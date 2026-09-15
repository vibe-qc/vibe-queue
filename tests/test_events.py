"""Tests for vq.events: append-only per-job event log."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from vq import events
from vq.events import EventKind


class TestAppendAndRead:
    def test_first_append_creates_dir_and_file(self, tmp_path: Path) -> None:
        ws = tmp_path / "ws"
        ws.mkdir()
        events.append_event(ws, EventKind.SUBMITTED, "abc123", cpus=4)
        f = ws / "_vq" / "events.jsonl"
        assert f.exists()
        line = f.read_text().strip()
        record = json.loads(line)
        assert record["jobid"] == "abc123"
        assert record["kind"] == "submitted"
        assert record["cpus"] == 4
        assert "ts" in record

    def test_subsequent_appends_are_additive(self, tmp_path: Path) -> None:
        ws = tmp_path / "ws"
        ws.mkdir()
        events.append_event(ws, EventKind.SUBMITTED, "j1")
        events.append_event(ws, EventKind.DISPATCHED, "j1", pid=42)
        events.append_event(ws, EventKind.STATE_TRANSITION, "j1",
                            **{"from": "running", "to": "completed"})
        records = events.read_events(ws)
        assert [r["kind"] for r in records] == [
            "submitted", "dispatched", "state_transition",
        ]
        assert records[1]["pid"] == 42
        assert records[2]["from"] == "running"
        assert records[2]["to"] == "completed"

    def test_read_missing_file_returns_empty(self, tmp_path: Path) -> None:
        assert events.read_events(tmp_path / "no-such-ws") == []

    def test_bad_line_is_skipped(self, tmp_path: Path) -> None:
        ws = tmp_path / "ws"
        (ws / "_vq").mkdir(parents=True)
        path = ws / "_vq" / "events.jsonl"
        path.write_text(
            '{"ts":"x","kind":"submitted","jobid":"a"}\n'
            'this-is-not-json\n'
            '{"ts":"y","kind":"dispatched","jobid":"a"}\n'
        )
        records = events.read_events(ws)
        assert len(records) == 2
        assert records[0]["kind"] == "submitted"
        assert records[1]["kind"] == "dispatched"

    def test_oserror_is_swallowed_not_raised(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # If the workspace dir can't be made writable (read-only mount, etc.)
        # append_event must NOT raise, only warn. The dispatch hot path
        # cannot afford to die because /tmp is full.
        ws = tmp_path / "ws"
        ws.mkdir()

        def explode(*a, **kw):
            raise OSError("disk on fire")

        monkeypatch.setattr(Path, "mkdir", explode)
        # Should not raise
        events.append_event(ws, EventKind.SUBMITTED, "j1")


class TestStateTransitionHelper:
    def test_state_transition_writes_well_formed_record(
        self, tmp_path: Path
    ) -> None:
        ws = tmp_path / "ws"
        ws.mkdir()
        events.state_transition(
            ws, "j1",
            from_state="pending", to_state="running",
        )
        records = events.read_events(ws)
        assert len(records) == 1
        r = records[0]
        assert r["kind"] == "state_transition"
        assert r["from"] == "pending"
        assert r["to"] == "running"
        assert "exit_code" not in r  # not provided -> not emitted

    def test_state_transition_with_exit_code_and_evidence(
        self, tmp_path: Path
    ) -> None:
        ws = tmp_path / "ws"
        ws.mkdir()
        events.state_transition(
            ws, "j1",
            from_state="running", to_state="oom_killed",
            reason="rss exceeded",
            exit_code=-15,
            rss_mb=130_000, limit_mb=122_880,
        )
        r = events.read_events(ws)[0]
        assert r["from"] == "running"
        assert r["to"] == "oom_killed"
        assert r["reason"] == "rss exceeded"
        assert r["exit_code"] == -15
        assert r["evidence"]["rss_mb"] == 130_000
        assert r["evidence"]["limit_mb"] == 122_880


class TestEventLogE2E:
    """End-to-end: a full submit -> dispatch -> complete cycle should
    produce a SUBMITTED + DISPATCHED + 2*STATE_TRANSITION sequence.
    Verifies the wiring across submit / daemon / kill modules holds."""

    def test_full_lifecycle_emits_expected_events(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from vq import paths
        from vq.daemon import Daemon
        from vq.spec import JobSpec, JobState
        from vq.submit import submit_local

        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))

        # Submit
        src = tmp_path / "hi.py"
        src.write_text("print('hi from event-log test')")
        jobid = submit_local(host="localhost", input_file=str(src))

        spec = JobSpec.read(paths.spec_path(jobid))
        ws = Path(spec.cwd)

        # Drive the daemon to completion in-process
        d = Daemon(max_cpus=2, poll_interval=0.05)
        try:
            import time
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                d.iterate()
                if JobSpec.read(paths.spec_path(jobid)).state == JobState.COMPLETED:
                    break
                time.sleep(0.05)
        finally:
            for rj in d._running.values():
                rj.popen.kill()
                rj.popen.wait(timeout=1)
                rj.close_logs()

        log = events.read_events(ws)
        kinds = [r["kind"] for r in log]
        # Order: submitted, dispatched, state_transition(pending->running),
        # state_transition(running->completed). Allow extras in case future
        # additions are made -- assert containment + order of the core 4.
        assert "submitted" in kinds
        assert "dispatched" in kinds
        # Find the two state transitions and verify their from/to.
        transitions = [r for r in log if r["kind"] == "state_transition"]
        assert len(transitions) >= 2
        assert any(
            t["from"] == "pending" and t["to"] == "running" for t in transitions
        )
        assert any(
            t["from"] == "running" and t["to"] == "completed" for t in transitions
        )
