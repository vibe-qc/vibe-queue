"""Tests for kill_job."""
from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path

import pytest

import vq.kill as kill_module
from vq import events
from vq.kill import kill_job
from vq.spec import JobSpec, JobState


def _write(queue: Path, jobid: str, **fields: object) -> JobSpec:
    queue.mkdir(parents=True, exist_ok=True)
    base: dict[str, object] = {"id": jobid, "command": ["true"], "cwd": "/tmp", "cpus": 1}
    base.update(fields)
    spec = JobSpec(**base)
    spec.write(queue / f"{jobid}.json")
    return spec


class TestKillPending:
    def test_pending_job_marked_killed(self, tmp_path: Path) -> None:
        queue = tmp_path / "queue"
        _write(queue, "j1", state=JobState.PENDING)
        msg = kill_job("localhost", "j1", queue_dir=queue)
        assert "killed pending" in msg
        spec = JobSpec.read(queue / "j1.json")
        assert spec.state == JobState.KILLED
        assert spec.finished_at is not None
        assert spec.failure_reason == "killed by vq/operator request"

    def test_pending_job_records_operator_reason(self, tmp_path: Path) -> None:
        queue = tmp_path / "queue"
        _write(queue, "j1", state=JobState.PENDING)
        kill_job(
            "localhost",
            "j1",
            queue_dir=queue,
            reason="obsolete vibe-qc version; resubmit after fleet update",
        )
        spec = JobSpec.read(queue / "j1.json")
        assert spec.failure_reason == (
            "killed by vq/operator request: "
            "obsolete vibe-qc version; resubmit after fleet update"
        )


class TestKillRunning:
    def test_invalid_pgid_is_rejected_before_any_signal(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        queue = tmp_path / "queue"
        queue.mkdir()
        payload = JobSpec(
            id="unsafe-pgid",
            command=["true"],
            cwd="/tmp",
            cpus=1,
            state=JobState.RUNNING,
            pid=12345,
        ).model_dump(mode="json")
        payload["pgid"] = 0
        (queue / "unsafe-pgid.json").write_text(json.dumps(payload))
        signals: list[tuple[str, int, int]] = []
        monkeypatch.setattr(
            kill_module.os,
            "killpg",
            lambda process_id, sig: signals.append(("killpg", process_id, sig)),
        )
        monkeypatch.setattr(
            kill_module.os,
            "kill",
            lambda process_id, sig: signals.append(("kill", process_id, sig)),
        )

        with pytest.raises(ValueError, match="pgid"):
            kill_job("localhost", "unsafe-pgid", queue_dir=queue)

        assert signals == []

    def test_running_job_signaled_and_marked_killed(self, tmp_path: Path) -> None:
        queue = tmp_path / "queue"
        # Spawn a real process so the kill has something to signal.
        proc = subprocess.Popen(["sleep", "30"])
        try:
            _write(
                queue,
                "j1",
                state=JobState.RUNNING,
                pid=proc.pid,
                started_at="2026-01-01T00:00:00+00:00",
            )
            msg = kill_job("localhost", "j1", queue_dir=queue)
            assert "SIGTERM" in msg
            assert str(proc.pid) in msg
            # Process should die promptly
            for _ in range(50):
                if proc.poll() is not None:
                    break
                time.sleep(0.05)
            assert proc.poll() is not None
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()

        spec = JobSpec.read(queue / "j1.json")
        assert spec.state == JobState.KILLED
        assert spec.failure_reason == "killed by vq/operator request"

    def test_running_job_with_dead_pid_still_marks_killed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        queue = tmp_path / "queue"
        _write(queue, "j1", state=JobState.RUNNING, pid=99_999)

        def fake_kill(pid: int, sig: int) -> None:
            raise ProcessLookupError(f"fake: pid {pid} gone")

        monkeypatch.setattr("vq.kill.os.kill", fake_kill)
        msg = kill_job("localhost", "j1", queue_dir=queue)
        assert "not found" in msg
        spec = JobSpec.read(queue / "j1.json")
        assert spec.state == JobState.KILLED

    def test_running_job_with_unreapable_pgid_still_marks_killed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        queue = tmp_path / "queue"
        workspace = tmp_path / "job"
        workspace.mkdir()
        _write(
            queue,
            "j1",
            cwd=str(workspace),
            state=JobState.RUNNING,
            pid=99_999,
            pgid=99_999,
        )

        def fake_killpg(pgid: int, sig: int) -> None:
            raise PermissionError(f"fake: pgid {pgid} cannot be signaled")

        monkeypatch.setattr("vq.kill.os.killpg", fake_killpg)
        # A persistent EPERM is believed only after the teardown settle (#27).
        monkeypatch.setattr("vq.process_group.EXITING_GROUP_SETTLE_SECONDS", 0.05)
        msg = kill_job("localhost", "j1", queue_dir=queue)

        assert "permission denied" in msg
        spec = JobSpec.read(queue / "j1.json")
        assert spec.state == JobState.KILLED
        assert spec.finished_at is not None
        assert spec.failure_reason == "killed by vq/operator request"
        transitions = [
            event
            for event in events.read_events(workspace)
            if event.get("kind") == "state_transition"
        ]
        assert "permission denied" in transitions[-1]["reason"]

    def test_running_job_records_operator_reason(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        queue = tmp_path / "queue"
        _write(queue, "j1", state=JobState.RUNNING, pid=99_999)

        def fake_kill(pid: int, sig: int) -> None:
            raise ProcessLookupError(f"fake: pid {pid} gone")

        monkeypatch.setattr("vq.kill.os.kill", fake_kill)
        kill_job(
            "localhost",
            "j1",
            queue_dir=queue,
            reason="env refresh requested",
        )
        spec = JobSpec.read(queue / "j1.json")
        assert spec.failure_reason == (
            "killed by vq/operator request: env refresh requested"
        )


class TestErrors:
    def test_unknown_job_raises(self, tmp_path: Path) -> None:
        queue = tmp_path / "queue"
        queue.mkdir(parents=True)
        with pytest.raises(FileNotFoundError):
            kill_job("localhost", "nope", queue_dir=queue)

    @pytest.mark.parametrize(
        "state",
        [JobState.COMPLETED, JobState.FAILED, JobState.KILLED, JobState.INTERRUPTED],
    )
    def test_terminal_state_rejected(self, tmp_path: Path, state: JobState) -> None:
        queue = tmp_path / "queue"
        _write(queue, "j1", state=state)
        with pytest.raises(ValueError, match="terminal state"):
            kill_job("localhost", "j1", queue_dir=queue)

    def test_remote_host_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(NotImplementedError):
            kill_job("some.host", "j1")
