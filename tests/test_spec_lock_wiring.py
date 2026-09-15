"""v0.8.11 *Dekker's Mutex* — interleave tests for the WIRED spec_lock.

``paths.spec_lock`` (the primitive) is tested in ``test_paths.py``. These
tests exercise the *call sites*: a real ``vq kill`` racing the daemon's real
``_record_finish`` / ``_record_orphan_finish`` on the same spec. Before the
wiring, both writers did read -> check -> write without mutual exclusion, so
the daemon could clobber a user's KILLED back to COMPLETED (the exact lost
update named in the ``spec_lock`` docstring) — ``vq kill`` reports success
while the spec lies as COMPLETED.

With both sides taking the per-spec lock, whichever acquires first writes its
terminal state atomically; the other re-reads under the lock, sees the
terminal state, and yields (``kill`` raises ``ValueError``; the daemon takes
its ``is_terminal`` branch and only stashes the exit code). Every trial is
therefore internally consistent — no lost update.
"""
from __future__ import annotations

import contextlib
import json
import subprocess
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from vq import cleanup
from vq.daemon import Daemon
from vq.kill import kill_job
from vq.spec import JobSpec, JobState
from vq.status import show_status

# Barrier-synchronized trials. The widened read->write window (below) makes
# the dangerous "both read RUNNING, daemon clobbers kill's KILLED back to
# COMPLETED" interleaving manifest on roughly half of un-locked trials, so a
# handful of trials reliably trips the regression if the lock is removed —
# while staying deterministically green WITH the lock (which serializes the
# whole read->write regardless of timing).
_TRIALS = 50

# Seconds of artificial delay injected AFTER each spec read, BEFORE the
# caller's subsequent write. Without the lock this guarantees both racers
# observe RUNNING before either writes (the lost-update precondition). With
# the lock the delay simply happens inside the held lock, so the racers can
# never both be mid-transition — the same widen-the-window technique the
# primitive test (test_paths.py) uses.
_WIDEN_S = 0.003


@pytest.fixture
def widen_read_window(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch ``JobSpec.read`` to sleep briefly after reading, widening the
    read->write window so the interleave is reproducible rather than relying
    on raw scheduler luck."""
    orig = JobSpec.read.__func__  # underlying function of the classmethod

    def _slow_read(cls: type[JobSpec], path: Path) -> JobSpec:
        spec = orig(cls, path)
        time.sleep(_WIDEN_S)
        return spec

    monkeypatch.setattr(JobSpec, "read", classmethod(_slow_read))


@pytest.fixture
def daemon(tmp_path: Path) -> Iterator[Daemon]:
    d = Daemon(
        max_cpus=8,
        poll_interval=0.05,
        queue_dir=tmp_path / "queue",
        jobs_dir=tmp_path / "jobs",
    )
    d.queue_dir.mkdir(parents=True, exist_ok=True)
    d.jobs_dir.mkdir(parents=True, exist_ok=True)
    yield d


def _running_spec(daemon: Daemon, jobid: str) -> None:
    """Write a RUNNING spec with no live pid/pgid, so ``vq kill`` records
    KILLED without signalling any real process (the lock, not signal
    delivery, is what's under test)."""
    workspace = daemon.jobs_dir / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(
        id=jobid,
        command=["echo", "hi"],
        cwd=str(workspace),
        cpus=1,
        state=JobState.RUNNING,
        pid=None,
        pgid=None,
        started_at="2026-05-14T00:00:00+00:00",
    )
    spec.write(daemon._spec_path(jobid))


def _kill_worker(
    daemon: Daemon, jobid: str, barrier: threading.Barrier, rejected: list[bool]
) -> None:
    barrier.wait()
    try:
        kill_job("localhost", jobid, queue_dir=daemon.queue_dir)
    except ValueError:
        # The daemon won the lock first; the spec was already terminal.
        rejected.append(True)


def _finish_worker(
    daemon: Daemon, jobid: str, barrier: threading.Barrier
) -> None:
    barrier.wait()
    daemon._record_finish(jobid, rc=0)


def _orphan_finish_worker(
    daemon: Daemon, jobid: str, barrier: threading.Barrier
) -> None:
    barrier.wait()
    spec = JobSpec.read(daemon._spec_path(jobid))
    daemon._record_orphan_finish(spec, rc=0, source="test-interleave")


class TestKillVsRecordFinish:
    def test_no_lost_update_under_interleave(
        self, daemon: Daemon, widen_read_window: None
    ) -> None:
        killed = completed = 0
        for i in range(_TRIALS):
            jobid = f"racefin{i:05d}"
            _running_spec(daemon, jobid)
            barrier = threading.Barrier(2)
            rejected: list[bool] = []
            tk = threading.Thread(
                target=_kill_worker, args=(daemon, jobid, barrier, rejected)
            )
            tf = threading.Thread(
                target=_finish_worker, args=(daemon, jobid, barrier)
            )
            tk.start()
            tf.start()
            tk.join()
            tf.join()

            final = JobSpec.read(daemon._spec_path(jobid))
            assert final.state in (JobState.KILLED, JobState.COMPLETED)
            if rejected:
                # kill saw a terminal spec under the lock => the daemon's
                # COMPLETED write landed first and was preserved.
                assert final.state == JobState.COMPLETED
                completed += 1
            else:
                # kill wrote KILLED first; _record_finish re-read under the
                # lock, saw is_terminal, preserved KILLED and only stashed
                # the exit code. Without the lock the daemon would clobber
                # KILLED back to COMPLETED here (the regression this guards).
                assert final.state == JobState.KILLED
                assert final.exit_code == 0
                killed += 1

        assert killed + completed == _TRIALS


class TestKillVsRecordOrphanFinish:
    def test_no_lost_update_under_interleave(
        self, daemon: Daemon, widen_read_window: None
    ) -> None:
        killed = completed = 0
        for i in range(_TRIALS):
            jobid = f"raceorf{i:05d}"
            _running_spec(daemon, jobid)
            barrier = threading.Barrier(2)
            rejected: list[bool] = []
            tk = threading.Thread(
                target=_kill_worker, args=(daemon, jobid, barrier, rejected)
            )
            to = threading.Thread(
                target=_orphan_finish_worker, args=(daemon, jobid, barrier)
            )
            tk.start()
            to.start()
            tk.join()
            to.join()

            final = JobSpec.read(daemon._spec_path(jobid))
            assert final.state in (JobState.KILLED, JobState.COMPLETED)
            if rejected:
                assert final.state == JobState.COMPLETED
                completed += 1
            else:
                assert final.state == JobState.KILLED
                assert final.exit_code == 0
                killed += 1

        assert killed + completed == _TRIALS


class TestKillDuringPopenWindow:
    """v0.8.12 *Peterson's Lock* — the _start_job dispatch window."""

    def test_kill_in_popen_window_is_honoured(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A `vq kill` landing in the post-Popen window — after the
        RUNNING-claim write, before the pid-fill write, while the spec
        carries pid=None/pgid=None — is preserved. The daemon reaps the
        just-spawned process group and does NOT clobber KILLED back to
        RUNNING (the pre-wiring bug, which would have overwritten the label
        and run an unkillable, untracked job).
        """
        jobid = "popenwin0001"
        workspace = daemon.jobs_dir / jobid
        workspace.mkdir(parents=True, exist_ok=True)
        spec = JobSpec(
            id=jobid,
            command=["sleep", "30"],
            cwd=str(workspace),
            cpus=1,
            state=JobState.PENDING,
        )
        spec.write(daemon._spec_path(jobid))

        real_popen = subprocess.Popen
        spawned: dict[str, subprocess.Popen] = {}

        def fake_popen(_cmd: object, **kwargs: object) -> subprocess.Popen:
            # Spawn a real, long-lived process so the reap has a target,
            # carrying the daemon's cwd/stdout/stderr/start_new_session/env.
            p = real_popen(["sleep", "30"], **kwargs)  # type: ignore[arg-type]
            spawned["p"] = p
            # The on-disk spec is RUNNING with pid/pgid None right now —
            # exactly the post-Popen window. Land a `vq kill` in it.
            kill_job("localhost", jobid, queue_dir=daemon.queue_dir)
            return p

        monkeypatch.setattr(subprocess, "Popen", fake_popen)
        try:
            started = daemon._start_job(spec)
            assert started is False, "dispatch must yield to the kill"
            assert jobid not in daemon._running, "killed job must not be tracked"
            final = JobSpec.read(daemon._spec_path(jobid))
            assert final.state == JobState.KILLED, "KILLED must not be clobbered"
            assert final.pid is None, "the pid-fill write must not have landed"
            assert spawned["p"].poll() is not None, "spawned process must be reaped"
        finally:
            p = spawned.get("p")
            if p is not None and p.poll() is None:
                p.kill()
                p.wait(timeout=2)


class TestDispatchFailureObservability:
    """EVENT-1 — dispatch-time FAILED transitions must be observable."""

    def test_popen_failure_emits_state_transition_event(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A dispatch that fails at Popen lands FAILED *and* emits a
        STATE_TRANSITION event (pre-v0.8.12 it was silent in events.jsonl),
        with a recorded failure_reason."""
        jobid = "evt1faildsp1"
        workspace = daemon.jobs_dir / jobid
        workspace.mkdir(parents=True, exist_ok=True)
        spec = JobSpec(
            id=jobid,
            command=["true"],
            cwd=str(workspace),
            cpus=1,
            state=JobState.PENDING,
        )
        spec.write(daemon._spec_path(jobid))

        def boom(*_a: object, **_k: object) -> subprocess.Popen:
            raise OSError("simulated Popen failure")

        monkeypatch.setattr(subprocess, "Popen", boom)
        started = daemon._start_job(spec)

        assert started is False
        final = JobSpec.read(daemon._spec_path(jobid))
        assert final.state == JobState.FAILED
        assert final.failure_reason, "dispatch failure must record a reason"

        events_file = workspace / "_vq" / "events.jsonl"
        lines = [json.loads(ln) for ln in events_file.read_text().splitlines()]
        failed_transitions = [
            e
            for e in lines
            if e.get("kind") == "state_transition" and e.get("to") == "failed"
        ]
        assert failed_transitions, (
            "dispatch-FAILED must emit a state_transition event (EVENT-1)"
        )


class TestStatusStampVsCleanupDelete:
    """v0.8.13 *Gray's Transaction* — the status-stamp / cleanup race."""

    def test_status_stamp_does_not_resurrect_a_deleted_spec(
        self, tmp_path: Path, widen_read_window: None
    ) -> None:
        """A `vq status` stamp racing `vq cleanup --delete` must not
        resurrect the deleted spec. Pre-wiring, status stamped its stale
        top-of-function read and re-wrote the file even after delete removed
        it; now status re-reads under the per-spec lock and finds it gone,
        so it doesn't write. After the race the spec must stay deleted.
        """
        queue = tmp_path / "queue"
        queue.mkdir(parents=True, exist_ok=True)
        jobid = "delrace00001"
        ws = tmp_path / "jobs" / jobid
        ws.mkdir(parents=True, exist_ok=True)
        (ws / "stdout.log").write_text("hi\n")
        (ws / "stderr.log").write_text("")
        spec = JobSpec(
            id=jobid,
            command=["true"],
            cwd=str(ws),
            cpus=1,
            state=JobState.COMPLETED,
            exit_code=0,
            finished_at="2026-05-14T00:00:00+00:00",
        )
        spec_file = queue / f"{jobid}.json"
        spec.write(spec_file)

        barrier = threading.Barrier(2)

        def do_status() -> None:
            barrier.wait()
            # status may legitimately error if the workspace is rmtree'd
            # mid-read; the resurrection check is what matters.
            with contextlib.suppress(Exception):
                show_status("localhost", jobid, queue_dir=queue)

        def do_delete() -> None:
            barrier.wait()
            cleanup.delete_job(spec, queue_dir=queue)

        ts = threading.Thread(target=do_status)
        td = threading.Thread(target=do_delete)
        ts.start()
        td.start()
        ts.join()
        td.join()

        assert not spec_file.exists(), (
            "vq status stamp resurrected a spec that cleanup --delete removed"
        )
