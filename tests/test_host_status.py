"""Tests for vq host_status: administrative host up/down marking +
the `vq host` CLI group + fan-out skipping (v0.10.0 *Lampson's Hint*)."""
from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import cli, config, host_status, paths
from vq.cli import main


@pytest.fixture
def cfgdir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Isolated config dir with a two-host config (host_d + host_a)."""
    d = tmp_path / "cfg"
    d.mkdir()
    monkeypatch.setenv(paths.ENV_CONFIG_DIR, str(d))
    (d / "config.toml").write_text(
        '[hosts.host_d]\nssh = "host_d"\n\n[hosts.host_a]\nssh = "host_a"\n',
        encoding="utf-8",
    )
    return d


class TestDownListPrimitives:
    def test_empty_when_no_file(self, cfgdir: Path) -> None:
        assert host_status.load_down() == {}
        assert host_status.is_down("host_d") is None

    def test_mark_down_then_is_down(self, cfgdir: Path) -> None:
        entry = host_status.mark_down("host_d", "mobile link")
        assert entry.host == "host_d"
        assert entry.reason == "mobile link"
        assert entry.since  # non-empty ISO timestamp
        got = host_status.is_down("host_d")
        assert got is not None and got.reason == "mobile link"
        assert host_status.down_file().is_file()

    def test_mark_up_clears(self, cfgdir: Path) -> None:
        host_status.mark_down("host_d", "x")
        assert host_status.mark_up("host_d") is True
        assert host_status.is_down("host_d") is None
        # second up is a no-op
        assert host_status.mark_up("host_d") is False

    def test_mark_down_idempotent_updates_reason(self, cfgdir: Path) -> None:
        host_status.mark_down("host_a", "first")
        host_status.mark_down("host_a", "second")
        got = host_status.is_down("host_a")
        assert got is not None and got.reason == "second"
        assert list(host_status.load_down()) == ["host_a"]  # one entry

    def test_corrupt_file_yields_empty(self, cfgdir: Path) -> None:
        host_status.down_file().write_text("{not json", encoding="utf-8")
        assert host_status.load_down() == {}  # never raises

    def test_non_dict_file_yields_empty(self, cfgdir: Path) -> None:
        host_status.down_file().write_text("[1, 2, 3]", encoding="utf-8")
        assert host_status.load_down() == {}

    def test_describe(self, cfgdir: Path) -> None:
        e = host_status.DownEntry("h", "flaky", "2026-06-06T00:00:00+00:00")
        d = e.describe()
        assert "flaky" in d and "since" in d
        assert host_status.DownEntry("h", "", "").describe() == "(no reason given)"


class TestHostCli:
    def test_down_list_up_roundtrip(self, cfgdir: Path) -> None:
        r = CliRunner().invoke(
            main, ["host", "down", "host_d", "--reason", "mobile"]
        )
        assert r.exit_code == 0, r.output
        assert "DOWN" in r.output
        assert host_status.is_down("host_d") is not None

        r = CliRunner().invoke(main, ["host", "list"])
        assert r.exit_code == 0, r.output
        assert "host_d" in r.output and "DOWN" in r.output and "mobile" in r.output
        assert "host_a" in r.output  # host_a still up, also listed

        r = CliRunner().invoke(main, ["host", "up", "host_d"])
        assert r.exit_code == 0 and "UP" in r.output
        assert host_status.is_down("host_d") is None

    def test_up_when_not_down(self, cfgdir: Path) -> None:
        r = CliRunner().invoke(main, ["host", "up", "host_d"])
        assert r.exit_code == 0
        assert "was not marked down" in r.output


class TestFanoutSkip:
    def test_aggregate_skips_down_hosts(self, cfgdir: Path) -> None:
        """A down host must NOT be probed by the --all aggregator (no SSH),
        but must still appear in the output, marked down."""
        host_status.mark_down("host_d", "flaky link")
        cfg = config.load_config()
        probed: list[str] = []

        def per_host(h: str) -> str:
            probed.append(h)
            return f"OK {h}"

        out = cli._aggregate_per_host(cfg, per_host, parallel=False)
        assert "host_d" not in probed  # skipped — not probed
        assert "host_a" in probed  # live host probed
        assert "administratively down" in out
        assert "host_d" in out and "flaky link" in out

    def test_aggregate_json_skips_down_hosts(self, cfgdir: Path) -> None:
        host_status.mark_down("host_d", "flaky")
        cfg = config.load_config()
        probed: list[str] = []

        def per_host(h: str) -> str:
            probed.append(h)
            return '{"ok": true}'

        payload = cli._aggregate_per_host_json(cfg, per_host, parallel=False)
        assert "host_d" not in probed
        assert payload["host_d"]["admin_down"] == "flaky"
        assert payload["host_a"] == {"ok": True}
