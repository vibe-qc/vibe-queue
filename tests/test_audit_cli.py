"""v0.8.9 *Cook's Hierarchy* — `vq audit` CLI verb tests.

Pins the v0.8.9 contract:

1. **Default text output** prints the trail with a stable column shape.
2. **`--json`** emits raw JSONL (one envelope per line) — stable schema
   matches the on-disk format.
3. **Filters compose** (`--since`, `--uid`, `--method`).
4. **`--method`** supports trailing `*` for prefix match.
5. **`--tail N`** caps output AFTER filters.
6. **Empty trail** prints a friendly message in text mode, nothing
   in JSON mode.
7. **`--all` and HOST are mutually exclusive.**
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import audit, paths
from vq import config as _config
from vq.cli import main


@pytest.fixture
def audit_state(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> Path:
    """Hermetic state dir + config dir. Pre-populates an audit log
    with deterministic entries the tests filter against."""
    state = tmp_path / "state"
    state.mkdir()
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(state))
    cfg = tmp_path / "cfg"
    cfg.mkdir()
    (cfg / "config.toml").write_text(
        "default_host = \"localhost\"\n"
        "[hosts.localhost]\nssh = \"localhost\"\n"
    )
    monkeypatch.setenv(_config.ENV_CONFIG_DIR, str(cfg))
    # Pre-populate audit log: three set_drain_state from uid=1000,
    # one set_throttle_state from uid=1001, one set_admin_status
    # from uid=1000, all timestamped over the last hour.
    now = datetime.now(UTC)
    entries = [
        {"ts": (now - timedelta(minutes=50)).isoformat(),
         "method": "set_drain_state", "uid": 1000, "ok": True,
         "args_summary": "set full"},
        {"ts": (now - timedelta(minutes=40)).isoformat(),
         "method": "set_drain_state", "uid": 1000, "ok": True,
         "args_summary": "clear"},
        {"ts": (now - timedelta(minutes=30)).isoformat(),
         "method": "set_throttle_state", "uid": 1001, "ok": True,
         "args_summary": "set weight=25"},
        {"ts": (now - timedelta(minutes=20)).isoformat(),
         "method": "set_admin_status", "uid": 1000, "ok": False,
         "args_summary": "env=vibeqc-dev",
         "error": "admin token required"},
        {"ts": (now - timedelta(minutes=10)).isoformat(),
         "method": "set_drain_state", "uid": 1000, "ok": True,
         "args_summary": "set full"},
    ]
    log_path = audit.audit_log_path()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
    return tmp_path


class TestDefaultOutput:
    def test_prints_all_entries_text(
        self, audit_state: Path,
    ) -> None:
        runner = CliRunner()
        result = runner.invoke(main, ["audit"])
        assert result.exit_code == 0, result.output
        # All 5 methods present somewhere.
        assert "set_drain_state" in result.output
        assert "set_throttle_state" in result.output
        assert "set_admin_status" in result.output

    def test_failed_entry_shows_error(
        self, audit_state: Path,
    ) -> None:
        runner = CliRunner()
        result = runner.invoke(main, ["audit"])
        assert "FAIL" in result.output
        assert "admin token required" in result.output


class TestJsonOutput:
    def test_json_emits_jsonl(
        self, audit_state: Path,
    ) -> None:
        runner = CliRunner()
        result = runner.invoke(main, ["audit", "--json"])
        assert result.exit_code == 0, result.output
        lines = [
            ln for ln in result.output.strip().splitlines() if ln
        ]
        assert len(lines) == 5
        for ln in lines:
            entry = json.loads(ln)
            assert "ts" in entry and "method" in entry

    def test_json_schema_preserved(
        self, audit_state: Path,
    ) -> None:
        runner = CliRunner()
        result = runner.invoke(main, ["audit", "--json"])
        # Last entry (set_drain_state at -10m) has stable fields.
        entries = [json.loads(ln) for ln in result.output.strip().splitlines()]
        last = entries[-1]
        assert last["method"] == "set_drain_state"
        assert last["uid"] == 1000
        assert last["ok"] is True
        assert last["args_summary"] == "set full"


class TestFilters:
    def test_uid_filter(self, audit_state: Path) -> None:
        runner = CliRunner()
        result = runner.invoke(main, ["audit", "--uid", "1001", "--json"])
        entries = [json.loads(ln) for ln in result.output.strip().splitlines()]
        assert len(entries) == 1
        assert entries[0]["method"] == "set_throttle_state"
        assert entries[0]["uid"] == 1001

    def test_method_exact_filter(self, audit_state: Path) -> None:
        runner = CliRunner()
        result = runner.invoke(
            main, ["audit", "--method", "set_drain_state", "--json"],
        )
        entries = [json.loads(ln) for ln in result.output.strip().splitlines()]
        assert len(entries) == 3
        for e in entries:
            assert e["method"] == "set_drain_state"

    def test_method_prefix_glob(self, audit_state: Path) -> None:
        """`set_*` matches every set_ method."""
        runner = CliRunner()
        result = runner.invoke(
            main, ["audit", "--method", "set_*", "--json"],
        )
        entries = [json.loads(ln) for ln in result.output.strip().splitlines()]
        # All 5 fixture entries are set_*.
        assert len(entries) == 5

    def test_since_filter(self, audit_state: Path) -> None:
        """`--since 15m` keeps only the most recent entry (the
        -10m one); the others are >15m ago."""
        runner = CliRunner()
        result = runner.invoke(
            main, ["audit", "--since", "15m", "--json"],
        )
        entries = [json.loads(ln) for ln in result.output.strip().splitlines()]
        assert len(entries) == 1
        # The most recent fixture entry.
        assert entries[0]["args_summary"] == "set full"
        assert entries[0]["method"] == "set_drain_state"

    def test_filters_compose(self, audit_state: Path) -> None:
        """`--method set_drain_state --uid 1000 --since 1h` matches
        all three drain entries from uid=1000."""
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "audit",
                "--method", "set_drain_state",
                "--uid", "1000",
                "--since", "1h",
                "--json",
            ],
        )
        entries = [json.loads(ln) for ln in result.output.strip().splitlines()]
        assert len(entries) == 3
        for e in entries:
            assert e["method"] == "set_drain_state"
            assert e["uid"] == 1000

    def test_since_rejects_invalid(self, audit_state: Path) -> None:
        runner = CliRunner()
        result = runner.invoke(main, ["audit", "--since", "not-a-duration"])
        assert result.exit_code != 0
        assert "--since" in result.output


class TestTail:
    def test_tail_caps_after_filters(
        self, audit_state: Path,
    ) -> None:
        runner = CliRunner()
        result = runner.invoke(
            main, ["audit", "--tail", "2", "--json"],
        )
        entries = [json.loads(ln) for ln in result.output.strip().splitlines()]
        assert len(entries) == 2
        # Last two fixture entries (set_admin_status fail + set_drain_state).
        assert entries[-1]["method"] == "set_drain_state"
        assert entries[-2]["method"] == "set_admin_status"

    def test_tail_default_100(
        self, audit_state: Path,
    ) -> None:
        """Default --tail=100 fits all 5 fixture entries."""
        runner = CliRunner()
        result = runner.invoke(main, ["audit", "--json"])
        entries = [json.loads(ln) for ln in result.output.strip().splitlines()]
        assert len(entries) == 5

    def test_tail_zero_rejected(self, audit_state: Path) -> None:
        """--tail must be >= 1 (IntRange enforces)."""
        runner = CliRunner()
        result = runner.invoke(main, ["audit", "--tail", "0"])
        assert result.exit_code != 0


class TestEmptyTrail:
    def test_empty_text_shows_friendly_message(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
    ) -> None:
        state = tmp_path / "state"
        state.mkdir()
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(state))
        cfg = tmp_path / "cfg"
        cfg.mkdir()
        (cfg / "config.toml").write_text(
            "default_host = \"localhost\"\n"
            "[hosts.localhost]\nssh = \"localhost\"\n"
        )
        monkeypatch.setenv(_config.ENV_CONFIG_DIR, str(cfg))
        runner = CliRunner()
        result = runner.invoke(main, ["audit"])
        assert result.exit_code == 0
        assert "no audit entries" in result.output

    def test_empty_json_emits_nothing(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
    ) -> None:
        state = tmp_path / "state"
        state.mkdir()
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(state))
        cfg = tmp_path / "cfg"
        cfg.mkdir()
        (cfg / "config.toml").write_text(
            "default_host = \"localhost\"\n"
            "[hosts.localhost]\nssh = \"localhost\"\n"
        )
        monkeypatch.setenv(_config.ENV_CONFIG_DIR, str(cfg))
        runner = CliRunner()
        result = runner.invoke(main, ["audit", "--json"])
        assert result.exit_code == 0
        # JSONL: zero matches → empty output (vs the friendly text
        # message). Scripting paths can detect with `wc -l`.
        assert result.output.strip() == ""


class TestAllHostsMutex:
    def test_all_and_host_mutually_exclusive(
        self, audit_state: Path,
    ) -> None:
        runner = CliRunner()
        result = runner.invoke(main, ["audit", "--all-hosts", "somehost"])
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output
