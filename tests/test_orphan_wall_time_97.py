"""A re-attached job keeps its wall-time budget across a daemon restart (#97).

The startup re-attach registered the orphan with the watchdog at "now", so
the elapsed-time budget restarted from zero on every daemon restart and a
job could run far past its submitted limit. The anchor is now the job's
recorded ``started_at``.
"""
from __future__ import annotations

import os
import subprocess
import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from vq.daemon import Daemon
from vq.spec import JobSpec, JobState
from vq.watchdog import WatchdogAction


@pytest.fixture
def orphan(tmp_path: Path) -> Iterator[tuple[Daemon, subprocess.Popen]]:
    daemon = Daemon(max_cpus=4, poll_interval=0.05,
                    queue_dir=tmp_path / "queue", jobs_dir=tmp_path / "jobs")
    daemon.queue_dir.mkdir(parents=True, exist_ok=True)
    daemon.jobs_dir.mkdir(parents=True, exist_ok=True)
    proc = subprocess.Popen(["sleep", "30"], start_new_session=True)
    try:
        yield daemon, proc
    finally:
        proc.kill()
        proc.wait(timeout=10)


def _reattach(daemon: Daemon, proc: subprocess.Popen, *, started_at: str | None,
              wall_time_seconds: int = 9000) -> JobSpec:
    ws = daemon.jobs_dir / "job97"
    ws.mkdir()
    spec = JobSpec(id="job97", command=["sleep", "30"], cwd=str(ws), cpus=1,
                   state=JobState.RUNNING, pid=proc.pid, pgid=os.getpgid(proc.pid),
                   started_at=started_at, wall_time_seconds=wall_time_seconds)
    spec.write(daemon._spec_path("job97"))
    daemon._reattach_or_interrupt_at_startup()
    assert "job97" in daemon._orphans
    return JobSpec.read(daemon._spec_path("job97"))


def _ago(seconds: float) -> str:
    return (datetime.now(UTC) - timedelta(seconds=seconds)).isoformat()


def test_the_budget_counts_from_the_recorded_start(orphan) -> None:
    daemon, proc = orphan

    _reattach(daemon, proc, started_at=_ago(8000))

    elapsed = time.monotonic() - daemon.watchdog._states["job97"].started_monotonic
    assert 8000 <= elapsed < 8060


def test_a_job_already_past_its_limit_is_stopped_after_the_restart(orphan) -> None:
    daemon, proc = orphan
    spec = _reattach(daemon, proc, started_at=_ago(9500))

    verdict = daemon.watchdog.evaluate("job97", proc.pid, spec.pgid, spec)

    assert verdict.action == WatchdogAction.SIGTERM
    assert verdict.terminal_state == JobState.TIME_EXCEEDED


@pytest.mark.parametrize("started_at", [None, "not-a-time", "2999-01-01T00:00:00+00:00"])
def test_a_missing_or_impossible_start_falls_back_to_now(orphan, started_at) -> None:
    daemon, proc = orphan

    _reattach(daemon, proc, started_at=started_at)

    elapsed = time.monotonic() - daemon.watchdog._states["job97"].started_monotonic
    assert 0 <= elapsed < 60
