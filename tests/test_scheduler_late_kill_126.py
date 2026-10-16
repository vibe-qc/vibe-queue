"""A `vq kill` that reaches a scheduler job after it already finished (#126).

The kill marks the record KILLED from the last polled state, and the
scheduler cancel is a no-op on a finished job. When accounting then reports
the job's own end (COMPLETED or FAILED), the daemon keeps that outcome and
exit code instead of a self-contradictory killed/exit-0 record.
"""
from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.test_daemon_scheduler import _inject, _submit_scheduler
from tests.test_scheduler_abnormal_end_reconcile import _slurm_finished_mock
from vq import events, paths
from vq.daemon import Daemon
from vq.kill import _DEFAULT_KILL_REASON
from vq.scheduler_dialect import QstatDetail, SlurmDialect, TorqueDialect
from vq.spec import JobSpec, JobState


@pytest.fixture
def daemon(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Daemon]:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    d = Daemon(max_cpus=4, poll_interval=0.05, queue_dir=tmp_path / "queue",
               jobs_dir=tmp_path / "jobs")
    d.queue_dir.mkdir(parents=True, exist_ok=True)
    d.jobs_dir.mkdir(parents=True, exist_ok=True)
    yield d


def _killed_after_finish(daemon: Daemon, raw_state: str, rc: int,
                         *, state=JobState.KILLED, reason=_DEFAULT_KILL_REASON) -> JobSpec:
    detail = QstatDetail(raw_state=raw_state, exit_code=rc, walltime_used="00:00:29",
                         walltime_limit="01:00:00")
    _inject(daemon, _slurm_finished_mock(detail, rc=rc))
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)
    # What `vq kill` writes from the last polled (still running) state.
    path = daemon._spec_path("j1")
    killed = JobSpec.read(path)
    killed.state = state
    killed.failure_reason = reason
    killed.write(path)
    daemon._reconcile_scheduler()
    return JobSpec.read(path)


def test_a_job_the_scheduler_completed_keeps_completed(daemon: Daemon) -> None:
    on_disk = _killed_after_finish(daemon, "COMPLETED", 0)
    assert (on_disk.state, on_disk.exit_code, on_disk.failure_reason) == (
        JobState.COMPLETED, 0, None)
    reasons = [e.get("reason", "") for e in events.read_events(Path(on_disk.cwd))]
    assert any("before the kill request took effect" in r for r in reasons)
    assert "j1" not in daemon._scheduler_running


def test_a_job_the_scheduler_failed_keeps_failed_with_its_exit(daemon: Daemon) -> None:
    on_disk = _killed_after_finish(daemon, "FAILED", 3)
    assert (on_disk.state, on_disk.exit_code) == (JobState.FAILED, 3)
    assert "before the kill request took effect" in on_disk.failure_reason


def test_a_cancel_that_took_effect_stays_killed(daemon: Daemon) -> None:
    on_disk = _killed_after_finish(daemon, "CANCELLED by 1234", 143)
    assert on_disk.state == JobState.KILLED
    assert on_disk.failure_reason.startswith(_DEFAULT_KILL_REASON)


@pytest.mark.parametrize(
    ("state", "reason"),
    [(JobState.KILLED, "dependency failed"), (JobState.OOM_KILLED, "watchdog: memory")],
)
def test_only_an_operator_kill_is_reconsidered(daemon: Daemon, state, reason) -> None:
    on_disk = _killed_after_finish(daemon, "COMPLETED", 0, state=state, reason=reason)
    assert (on_disk.state, on_disk.failure_reason) == (state, reason)


def test_only_slurm_accounting_proves_a_job_ended_on_its_own() -> None:
    slurm = SlurmDialect()
    assert slurm.ended_on_its_own("COMPLETED") and slurm.ended_on_its_own("FAILED")
    assert not slurm.ended_on_its_own("CANCELLED by 1234")
    assert not slurm.ended_on_its_own("TIMEOUT") and not slurm.ended_on_its_own("  ")
    assert not TorqueDialect().ended_on_its_own("C")
