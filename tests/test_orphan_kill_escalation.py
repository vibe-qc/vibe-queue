"""A kill after daemon reattachment must still have a bounded grace."""
from __future__ import annotations

import signal
import subprocess
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests import test_terminal_reaping
from tests.test_terminal_reaping import (
    _LIVENESS_SECONDS,
    _SIGTERM_IGNORING_LOCK_COMMAND,
    _command_exited,
    _dispatch_started,
    _kill_group_best_effort,
)
from vq import daemon as daemon_mod
from vq import kill as kill_mod
from vq.daemon import Daemon, _OrphanJob
from vq.spec import JobSpec, JobState

dispatching_daemon = test_terminal_reaping.dispatching_daemon


@pytest.fixture
def reattached_job(
    dispatching_daemon: Daemon,
) -> Iterator[tuple[Daemon, str, subprocess.Popen[bytes]]]:
    original = dispatching_daemon
    jobid = "orphankill01"
    spec = _dispatch_started(
        original, jobid, command=_SIGTERM_IGNORING_LOCK_COMMAND, cpus=original.max_cpus,
    )
    assert spec.pgid is not None
    child = original._running.pop(jobid)
    original._queue_lock_fd.close()
    restarted = None
    try:
        restarted = Daemon(
            max_cpus=original.max_cpus, poll_interval=0.05,
            queue_dir=original.queue_dir, jobs_dir=original.jobs_dir,
        )
        restarted._reattach_or_interrupt_at_startup()
        assert jobid in restarted._orphans
        assert jobid not in restarted._running
        yield restarted, jobid, child.popen
    finally:
        _kill_group_best_effort(Path(spec.cwd), spec.pgid)
        if child.popen.poll() is None:
            child.popen.kill()
        child.popen.wait(timeout=_LIVENESS_SECONDS)
        child.close_logs()
        if restarted is not None:
            restarted._queue_lock_fd.close()


@pytest.mark.parametrize("killer", ["cli", "watchdog"])
def test_reattached_sigterm_survivor_is_escalated(
    reattached_job, monkeypatch: pytest.MonkeyPatch, killer: str,
) -> None:
    daemon, jobid, wrapper = reattached_job
    monkeypatch.setattr(daemon_mod, "KILL_ESCALATION_GRACE_SECONDS", 0.0)
    spec_path = daemon._spec_path(jobid)
    if killer == "cli":
        kill_mod.kill_job("localhost", jobid, queue_dir=daemon.queue_dir)
        terminal = JobState.KILLED
    else:
        spec = JobSpec.read(spec_path)
        spec.wall_time_seconds = 1
        spec.write(spec_path)
        daemon.watchdog._states[jobid].started_monotonic -= 100.0
        daemon._watchdog_pass()
        terminal = JobState.TIME_EXCEEDED
    wrapper.wait(timeout=_LIVENESS_SECONDS)
    assert not _command_exited(daemon.jobs_dir / jobid, budget=0.0)

    daemon._reconcile_orphans()  # arm the grace
    assert jobid in daemon._orphans
    daemon._reconcile_orphans()  # expire the zero-length test grace
    assert jobid not in daemon._orphans, "the killed orphan must release its capacity"
    assert jobid not in daemon.watchdog._states
    assert _command_exited(daemon.jobs_dir / jobid, budget=_LIVENESS_SECONDS)
    assert JobSpec.read(spec_path).state == terminal


@pytest.fixture
def tracked_orphan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    daemon = Daemon(max_cpus=4, queue_dir=tmp_path / "queue", jobs_dir=tmp_path / "jobs")
    daemon.queue_dir.mkdir(parents=True, exist_ok=True)
    workspace = daemon.jobs_dir / "orphan01"
    workspace.mkdir(parents=True)
    spec = JobSpec(
        id="orphan01", command=["true"], cwd=str(workspace), state=JobState.KILLED,
        pgid=900001, pid=900001, cpus=4, mem_mb=128,
    )
    spec.write(daemon._spec_path(spec.id))
    orphan = _OrphanJob(pgid=900001, cpus=4, mem_mb=128, uid="1234")
    daemon._orphans[spec.id] = orphan
    daemon.watchdog.register(spec.id)
    clock = SimpleNamespace(now=100.0)
    monkeypatch.setattr(daemon_mod, "time", SimpleNamespace(monotonic=lambda: clock.now))
    monkeypatch.setattr(daemon_mod, "_pgroup_alive", lambda pgid: True)
    signals = []
    monkeypatch.setattr(daemon_mod, "killpg", lambda pgid, sig: signals.append((pgid, sig)))
    scopes = []
    monkeypatch.setattr(daemon, "_reap_scope", scopes.append)
    try:
        yield daemon, spec, orphan, clock, signals, scopes
    finally:
        daemon._queue_lock_fd.close()


def test_grace_holds_capacity_then_releases_even_if_group_still_answers(
    tracked_orphan, monkeypatch: pytest.MonkeyPatch,
) -> None:
    daemon, spec, orphan, clock, signals, scopes = tracked_orphan
    daemon._reconcile_orphans()
    deadline = clock.now + daemon_mod.KILL_ESCALATION_GRACE_SECONDS
    assert orphan.term_deadline == deadline
    assert not signals
    clock.now = deadline - 0.001
    daemon._reconcile_orphans()
    assert daemon._orphans[spec.id] is orphan
    assert not signals
    assert (orphan.cpus, orphan.mem_mb, orphan.uid) == (4, 128, "1234")

    waiting = JobSpec(id="waiting01", command=["true"], cwd=str(daemon.jobs_dir), cpus=4)
    waiting.write(daemon._spec_path(waiting.id))
    started = []
    monkeypatch.setattr(daemon, "_start_job", lambda spec: started.append(spec.id))
    daemon._dispatch_pending()
    assert not started

    # Zombies may keep the group probe positive after SIGKILL. The grace
    # still bounds the reservation, and a mutable spec cannot redirect it.
    spec.pgid = 900002
    spec.write(daemon._spec_path(spec.id))
    clock.now = deadline
    daemon._reconcile_orphans()
    assert signals == [(orphan.pgid, signal.SIGCONT), (orphan.pgid, signal.SIGKILL)]
    assert scopes == [spec.id]
    assert spec.id not in daemon._orphans
    assert spec.id not in daemon.watchdog._states
    daemon._dispatch_pending()
    assert started == [waiting.id]
    daemon._reconcile_orphans()
    assert len(signals) == 2


def test_group_exit_releases_before_grace_without_signals(tracked_orphan, monkeypatch):
    daemon, spec, orphan, clock, signals, scopes = tracked_orphan
    daemon._reconcile_orphans()
    monkeypatch.setattr(daemon_mod, "_pgroup_alive", lambda pgid: False)
    daemon._reconcile_orphans()
    assert spec.id not in daemon._orphans
    assert not signals
    assert not scopes
    assert JobSpec.read(daemon._spec_path(spec.id)).state == JobState.KILLED


def test_nonterminal_or_unreadable_spec_does_not_authorize_escalation(tracked_orphan):
    daemon, spec, orphan, clock, signals, scopes = tracked_orphan
    spec.state = JobState.RUNNING
    spec.write(daemon._spec_path(spec.id))
    daemon._reconcile_orphans()
    assert orphan.term_deadline is None
    spec.state = JobState.KILLED
    spec.write(daemon._spec_path(spec.id))
    daemon._reconcile_orphans()
    clock.now += daemon_mod.KILL_ESCALATION_GRACE_SECONDS
    daemon._spec_path(spec.id).write_text("invalid json")
    daemon._reconcile_orphans()
    assert spec.id in daemon._orphans
    assert not signals
    spec.state = JobState.RUNNING
    spec.write(daemon._spec_path(spec.id))
    daemon._reconcile_orphans()
    assert orphan.term_deadline is None
    assert not signals


def test_daemon_process_group_is_never_signalled(tracked_orphan, monkeypatch):
    daemon, spec, orphan, clock, signals, scopes = tracked_orphan
    monkeypatch.setattr(daemon_mod.os, "getpgrp", lambda: orphan.pgid)
    daemon._reconcile_orphans()
    clock.now += daemon_mod.KILL_ESCALATION_GRACE_SECONDS
    daemon._reconcile_orphans()
    assert orphan.term_deadline is None
    assert spec.id in daemon._orphans
    assert not signals
    assert not scopes
