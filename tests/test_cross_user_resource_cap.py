"""v0.7.15 *Shannon's Entropy* — cross-user resource cap tripwire.

Pins the operator-stated invariant from 2026-05-28: "the resources
need to be managed by the queue. Jobs from different users shall
not run in parallel if that exceeds total available resource."

The dispatch loop's global cap (``used_cpus + spec.cpus >
effective_max_cpus`` in daemon.py's dispatch loop) is enforced
BEFORE the per-user quota gate, so jobs from different users
sum into the same ``used_cpus`` tally. Pre-v0.7.15 nothing
tested this explicitly — only per-user quota tests
(test_multi_user_quota_orphans_v0_6_34.py) and the orphan budget
test (test_daemon_orphan_budget_v0_6_29.py) exist, and neither
exercises the cross-user case.

This file ships the tripwire. If a future refactor relocates the
global cap inside the per-user branch (turning it into a per-
user-only enforcement) or breaks the order (per-user gate before
global gate, which would let a user's quota-loose allocation
bypass the host cap), these tests catch it.

No production code changes; the audit found the existing
behaviour correct.
"""

from __future__ import annotations

import contextlib
from pathlib import Path

from vq.daemon import Daemon, _OrphanJob
from vq.spec import JobSpec, JobState


def _make_daemon(
    tmp_path: Path, *, max_cpus: int, max_jobs: int = 99,
) -> Daemon:
    d = Daemon(
        max_cpus=max_cpus,
        max_jobs=max_jobs,
        poll_interval=0.05,
        queue_dir=tmp_path / "queue",
        jobs_dir=tmp_path / "jobs",
    )
    d.queue_dir.mkdir(parents=True, exist_ok=True)
    d.jobs_dir.mkdir(parents=True, exist_ok=True)
    return d


def _write_pending(
    d: Daemon,
    jobid: str,
    *,
    cpus: int = 1,
    submitter: str = "1000",
) -> None:
    ws = d.jobs_dir / jobid
    ws.mkdir(parents=True, exist_ok=True)
    JobSpec(
        id=jobid,
        command=["true"],
        cwd=str(ws),
        cpus=cpus,
        submitter=submitter,
        state=JobState.PENDING,
    ).write(d._spec_path(jobid))


def _kill_running(d: Daemon) -> None:
    """Helper for test teardown — kill any actually-spawned
    subprocess so tests don't leak ``true`` processes."""
    for rj in list(d._running.values()):
        with contextlib.suppress(Exception):
            rj.popen.kill()


# ----------------------------------------------------------------------
# Single-user baseline — cap is enforced regardless of submitter
# ----------------------------------------------------------------------


class TestGlobalCpuCapSingleUser:
    def test_two_specs_one_user_sum_into_global_cap(
        self, tmp_path: Path
    ) -> None:
        """The simplest case: one user submits 2 specs totalling
        6 CPUs against ``max_cpus=4``. Only one should dispatch."""
        d = _make_daemon(tmp_path, max_cpus=4)
        try:
            _write_pending(d, "userA-job1", cpus=3, submitter="1000")
            _write_pending(d, "userA-job2", cpus=3, submitter="1000")
            d._dispatch_pending()
            states = {
                jid: JobSpec.read(d._spec_path(jid)).state
                for jid in ("userA-job1", "userA-job2")
            }
            running = sum(1 for s in states.values() if s == JobState.RUNNING)
            pending = sum(1 for s in states.values() if s == JobState.PENDING)
            assert running == 1, states
            assert pending == 1, states
        finally:
            _kill_running(d)
            d._queue_lock_fd.close()


# ----------------------------------------------------------------------
# Cross-user invariant — the operator-stated requirement
# ----------------------------------------------------------------------


class TestGlobalCpuCapCrossUser:
    def test_two_users_two_specs_share_global_cap(
        self, tmp_path: Path
    ) -> None:
        """The operator-stated invariant: user A's 3-CPU job + user
        B's 3-CPU job against ``max_cpus=4`` cannot both run. The
        global cap sums across submitters."""
        d = _make_daemon(tmp_path, max_cpus=4)
        try:
            _write_pending(d, "userA-1234567", cpus=3, submitter="1000")
            _write_pending(d, "userB-1234567", cpus=3, submitter="2000")
            d._dispatch_pending()
            states = {
                jid: JobSpec.read(d._spec_path(jid)).state
                for jid in ("userA-1234567", "userB-1234567")
            }
            # Exactly one of the two ran. We don't care which — that's
            # priority + submitted_at order, not the invariant we're
            # pinning here.
            running = [
                jid for jid, s in states.items() if s == JobState.RUNNING
            ]
            pending = [
                jid for jid, s in states.items() if s == JobState.PENDING
            ]
            assert len(running) == 1, states
            assert len(pending) == 1, states
        finally:
            _kill_running(d)
            d._queue_lock_fd.close()

    def test_three_users_summed_against_global_cap(
        self, tmp_path: Path
    ) -> None:
        """3 users × 2 CPUs = 6 CPUs requested vs ``max_cpus=5``.
        Only 2 of the 3 specs should dispatch (whichever has earlier
        submitted_at; ties broken by jobid). The third sits PENDING
        because dispatching it would push global used_cpus to 6."""
        d = _make_daemon(tmp_path, max_cpus=5)
        try:
            _write_pending(d, "userA-jjjjjjj", cpus=2, submitter="1000")
            _write_pending(d, "userB-jjjjjjj", cpus=2, submitter="2000")
            _write_pending(d, "userC-jjjjjjj", cpus=2, submitter="3000")
            d._dispatch_pending()
            states = {
                jid: JobSpec.read(d._spec_path(jid)).state
                for jid in ("userA-jjjjjjj", "userB-jjjjjjj", "userC-jjjjjjj")
            }
            running = [
                jid for jid, s in states.items() if s == JobState.RUNNING
            ]
            assert len(running) == 2, states
        finally:
            _kill_running(d)
            d._queue_lock_fd.close()

    def test_one_users_giant_job_locks_out_others(
        self, tmp_path: Path
    ) -> None:
        """User A submits a single 16-CPU job; ``max_cpus=16`` —
        all CPUs consumed. User B's 1-CPU job that arrives next
        cannot dispatch. The single-job-saturation case is just as
        important as the multi-small-job case."""
        d = _make_daemon(tmp_path, max_cpus=16)
        try:
            _write_pending(d, "userA-bigjob", cpus=16, submitter="1000")
            _write_pending(d, "userB-tinyjb", cpus=1, submitter="2000")
            d._dispatch_pending()
            big = JobSpec.read(d._spec_path("userA-bigjob"))
            tiny = JobSpec.read(d._spec_path("userB-tinyjb"))
            assert big.state == JobState.RUNNING
            assert tiny.state == JobState.PENDING
        finally:
            _kill_running(d)
            d._queue_lock_fd.close()


# ----------------------------------------------------------------------
# Cross-user invariant under orphans (the v0.6.29 case + cross-user)
# ----------------------------------------------------------------------


class TestGlobalCpuCapWithOrphans:
    def test_orphan_from_one_user_blocks_another_users_dispatch(
        self, tmp_path: Path
    ) -> None:
        """An orphan (a job alive from a previous daemon) holding 6
        CPUs blocks ANY user's new dispatch when the spec would push
        used_cpus over the global cap. v0.6.29 verified this for
        same-user; v0.7.15 pins it cross-user too."""
        d = _make_daemon(tmp_path, max_cpus=8)
        try:
            # Orphan owned by user 1000 holding 6 CPUs.
            d._orphans["o1"] = _OrphanJob(
                pgid=900001, cpus=6, mem_mb=None, uid="1000",
            )
            # User 2000 submits a 3-CPU job — would push to 9 > 8.
            _write_pending(
                d, "userB-3cpu", cpus=3, submitter="2000",
            )
            d._dispatch_pending()
            spec = JobSpec.read(d._spec_path("userB-3cpu"))
            assert spec.state == JobState.PENDING, (
                "cross-user orphan must count against global cap; "
                "if this fires user B's job dispatched on top of "
                "user A's orphan and the host now runs 9 CPUs vs a "
                "cap of 8"
            )
        finally:
            _kill_running(d)
            d._queue_lock_fd.close()


# ----------------------------------------------------------------------
# Dispatch-order invariant — global gate runs BEFORE per-user gate
# ----------------------------------------------------------------------


class TestGlobalGateOrdersBeforePerUserGate:
    def test_global_cap_caught_before_per_user_quota_check(
        self, tmp_path: Path
    ) -> None:
        """Structural test: the global cap (line 1681 of daemon.py)
        runs ``continue`` BEFORE the per-user quota gate. So a
        spec rejected by the global cap NEVER consults per-user
        quota — preventing a future bug where per-user quota
        accounting could 'unlock' a spec the global cap should
        have rejected.

        Verified by inspecting that ``used_cpus`` (the variable
        feeding the global gate) is the global tally, not a
        per-user tally."""
        # Read the daemon source to confirm the line ordering at
        # the time of this test's authorship. If the surrounding
        # code is refactored in a way that moves the global gate
        # below the per-user gate, this string-grep fails and the
        # operator's attention is drawn to the change.
        from pathlib import Path as _P
        src = _P(__file__).parent.parent / "src" / "vq" / "daemon.py"
        text = src.read_text()
        # Find the dispatch loop's spec gates.
        loop_start = text.find("for spec in pending:")
        assert loop_start > 0, "dispatch loop signature changed"
        # v0.12.0: bound the excerpt to the dispatch METHOD body (up to the
        # next def at method indent), not a fixed char window. The loop grew
        # past the old 2000-char window (scheduler-target gate, build-as-job
        # skips), which pushed the per-user gate out of view and tripped this
        # check spuriously even though the ordering was correct. Method-
        # bounding keeps the ordering assertion valid as the loop keeps growing.
        loop_end = text.find("\n    def ", loop_start)
        if loop_end < 0:
            loop_end = len(text)
        loop_excerpt = text[loop_start:loop_end]
        idx_global = loop_excerpt.find(
            "if used_cpus + spec.cpus > effective_max_cpus"
        )
        idx_per_user = loop_excerpt.find("max_pending = cfg.quotas")
        assert idx_global > 0, "global cpus_total gate not found"
        assert idx_per_user > 0, "per-user quota gate not found"
        assert idx_global < idx_per_user, (
            "global cpus_total gate must run BEFORE per-user "
            "quota gate so a globally-over spec is rejected "
            "regardless of per-user quota state. If this fires, "
            "review the dispatch loop ordering."
        )
