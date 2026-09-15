"""A host's update-script wall cap is configuration, not an env var (#32).

host_e (6 cores) needs slightly more than the four-hour default for a cold
native rebuild of vibe-qc. On 2026-09-12 the cap reaped a healthy build, and
the retry with a raised cap finished in fifteen minutes. The only override was
`VQ_UPDATE_SCRIPT_TIMEOUT` in the invoking environment, which a planner-driven
roll never sets. The delegated forwarding itself is pinned in
`tests/test_admin_update_forwarding_contract.py`.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from vq import admin, cli, config


def _host_e(**extra: object) -> config.HostConfig:
    return config.HostConfig(ssh="host_e.example.invalid", **extra)


class TestTheKey:
    def test_a_positive_cap_is_accepted(self) -> None:
        assert _host_e(update_script_timeout_seconds=28800).update_script_timeout_seconds == 28800

    def test_it_is_optional(self) -> None:
        assert _host_e().update_script_timeout_seconds is None

    @pytest.mark.parametrize("bad", [0, -1, float("inf"), float("nan")])
    def test_a_non_positive_or_non_finite_cap_is_refused(self, bad: float) -> None:
        with pytest.raises(ValueError):
            _host_e(update_script_timeout_seconds=bad)


class TestDriverResolution:
    def test_the_host_key_replaces_the_default(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("VQ_UPDATE_SCRIPT_TIMEOUT", raising=False)

        env = cli._remote_admin_update_environment(
            _host_e(update_script_timeout_seconds=28800),
        )

        assert env["VQ_UPDATE_SCRIPT_TIMEOUT"] == "28800.0"

    def test_an_explicit_environment_value_still_wins(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("VQ_UPDATE_SCRIPT_TIMEOUT", "3600")

        env = cli._remote_admin_update_environment(
            _host_e(update_script_timeout_seconds=28800),
        )

        assert env["VQ_UPDATE_SCRIPT_TIMEOUT"] == "3600.0"

    def test_no_key_keeps_the_four_hour_default(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("VQ_UPDATE_SCRIPT_TIMEOUT", raising=False)

        env = cli._remote_admin_update_environment(_host_e())

        assert env["VQ_UPDATE_SCRIPT_TIMEOUT"] == str(
            float(admin.UPDATE_SCRIPT_TIMEOUT_SECONDS)
        )

    def test_the_ssh_observer_cap_grows_with_the_key(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("VQ_UPDATE_SCRIPT_TIMEOUT", raising=False)
        monkeypatch.delenv("VQ_REMOTE_ADMIN_UPDATE_TIMEOUT", raising=False)

        _env, outer = cli._remote_admin_update_contract(
            host_cfg=_host_e(update_script_timeout_seconds=28800),
        )

        assert outer is not None
        assert outer >= 28800 + cli._REMOTE_ADMIN_UPDATE_TIMEOUT_MARGIN_SECONDS


class TestLocalAndDetachedUpdates:
    def test_a_local_update_exports_the_host_key(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Registered with monkeypatch so the export is undone afterwards.
        monkeypatch.setenv("VQ_UPDATE_SCRIPT_TIMEOUT", "")
        cfg = config.Config(
            hosts={"host_e": _host_e(update_script_timeout_seconds=28800)},
        )

        cli._export_host_update_script_timeout(cfg, "host_e")

        assert os.environ["VQ_UPDATE_SCRIPT_TIMEOUT"] == "28800.0"
        assert admin._update_script_timeout() == 28800.0

    def test_an_explicit_value_is_not_overwritten(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("VQ_UPDATE_SCRIPT_TIMEOUT", "3600")
        cfg = config.Config(
            hosts={"host_e": _host_e(update_script_timeout_seconds=28800)},
        )

        cli._export_host_update_script_timeout(cfg, "host_e")

        assert os.environ["VQ_UPDATE_SCRIPT_TIMEOUT"] == "3600"

    def test_a_host_without_the_key_changes_nothing(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("VQ_UPDATE_SCRIPT_TIMEOUT", "")
        cfg = config.Config(hosts={"host_e": _host_e()})

        cli._export_host_update_script_timeout(cfg, "host_e")

        assert os.environ["VQ_UPDATE_SCRIPT_TIMEOUT"] == ""

    def test_a_detached_unit_receives_the_forwarded_cap(self) -> None:
        """A transient user unit starts from the user manager's environment,
        so the cap must survive the --setenv allowlist to take effect."""
        kept = admin._detached_systemd_environment(
            {"VQ_UPDATE_SCRIPT_TIMEOUT": "28800.0", "VQ_BUILD_STALL_TIMEOUT": "900.0"},
        )

        assert kept["VQ_UPDATE_SCRIPT_TIMEOUT"] == "28800.0"


@pytest.mark.no_autopatch_build_runner
class TestReapWording:
    """A wall-clock reap and a stall reap call for opposite responses."""

    def test_a_wall_cap_reap_is_not_called_wedged(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr("vq.admin._BUILD_POLL_INTERVAL_SECONDS", 0.1)
        script = tmp_path / "loud.sh"
        script.write_text("#!/bin/bash\nwhile true; do echo tick; sleep 0.05; done\n")
        emitted: list[str] = []

        result = admin._run_monitored_build(
            ["bash", str(script)],
            cwd=str(tmp_path),
            env=dict(os.environ),
            wall_timeout=1.0,
            stall_timeout=0.0,
            heartbeat_interval=0.0,
            log_label="test-loud",
            emit=emitted.append,
        )

        assert result.timed_out is True
        reaps = [line for line in emitted if "reaping" in line]
        assert reaps, emitted
        assert "wall-clock cap hit" in reaps[0]
        assert "wedged" not in reaps[0]
        assert "update_script_timeout_seconds" in reaps[0]

    def test_a_stall_reap_is_still_called_wedged(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr("vq.admin._BUILD_POLL_INTERVAL_SECONDS", 0.1)
        script = tmp_path / "silent.sh"
        script.write_text("#!/bin/bash\nsleep 120\n")
        emitted: list[str] = []

        result = admin._run_monitored_build(
            ["bash", str(script)],
            cwd=str(tmp_path),
            env=dict(os.environ),
            wall_timeout=30.0,
            stall_timeout=1.0,
            heartbeat_interval=0.0,
            log_label="test-silent",
            emit=emitted.append,
        )

        assert result.stalled is True
        reaps = [line for line in emitted if "reaping" in line]
        assert reaps and "wedged" in reaps[0] and "stall cap" in reaps[0]
