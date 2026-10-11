"""v0.6.29: reattached orphans count against the dispatch budgets.

After a daemon restart, jobs still alive from the *previous* daemon
are reattached into ``_orphans`` (not ``_running``). Before this fix
the dispatch gate counted only ``_running`` — so a restart with N
live orphans dispatched a fresh ``max_jobs`` on top of them. The host
then ran N + max_jobs jobs, with N jobs' worth of CPU + memory budget
unaccounted.

This is the over-subscription observed on host_a after a v0.6.x
admin-update restart: 2 reattached orphans + 2 freshly dispatched
jobs = 4 running against ``--max-jobs 2``.

A ``_TerminalSurvivor`` (8eb94cc) is the same kind of holder arriving by
a different route -- a killed job whose group outlived the wrapper vq
reaped -- and ``_dispatch_pending`` counts it in the same ``local_active``
sum, so its job-slot contract is checked here beside the orphans' (#31).
"""
from __future__ import annotations

import contextlib
from pathlib import Path

from vq.daemon import Daemon, _OrphanJob, _TerminalSurvivor
from vq.spec import JobSpec, JobState


def _make_daemon(tmp_path: Path, *, max_cpus: int, max_jobs: int) -> Daemon:
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
    d: Daemon, jobid: str, *, cpus: int = 1, mem_mb: int | None = None
) -> None:
    ws = d.jobs_dir / jobid
    ws.mkdir(parents=True, exist_ok=True)
    JobSpec(
        id=jobid,
        command=["true"],
        cwd=str(ws),
        cpus=cpus,
        mem_mb=mem_mb,
        state=JobState.PENDING,
    ).write(d._spec_path(jobid))


def _survivor(d: Daemon, jobid: str, *, pgid: int) -> _TerminalSurvivor:
    """A killed job still holding its group, charged 1 cpu and no memory.

    ``deadline`` is infinite so the record cannot expire mid-test; no process
    exists behind ``pgid`` and none of the budget gates probes the group.
    """
    return _TerminalSurvivor(
        pgid=pgid,
        cpus=1,
        mem_mb=None,
        state=JobState.KILLED,
        workspace=d.jobs_dir / jobid,
        deadline=float("inf"),
    )


class TestOrphansCountTowardBudgets:
    def test_orphans_fill_max_jobs_block_dispatch(
        self, tmp_path: Path
    ) -> None:
        """2 reattached orphans fill --max-jobs 2 → a PENDING job must
        NOT dispatch (pre-fix it would, giving 4 jobs vs a cap of 2)."""
        d = _make_daemon(tmp_path, max_cpus=99, max_jobs=2)
        try:
            d._orphans["o1"] = _OrphanJob(pgid=900001, cpus=1, mem_mb=None)
            d._orphans["o2"] = _OrphanJob(pgid=900002, cpus=1, mem_mb=None)
            _write_pending(d, "pending-1")
            d._dispatch_pending()
            spec = JobSpec.read(d._spec_path("pending-1"))
            assert spec.state == JobState.PENDING
            assert "pending-1" not in d._running
        finally:
            d._queue_lock_fd.close()

    def test_orphan_cpus_fill_max_cpus_block_dispatch(
        self, tmp_path: Path
    ) -> None:
        """An orphan's cpus count against --max-cpus: a 4-cpu orphan
        saturates a 4-cpu budget, so even a 1-cpu PENDING job is
        held back."""
        d = _make_daemon(tmp_path, max_cpus=4, max_jobs=10)
        try:
            d._orphans["o1"] = _OrphanJob(pgid=900001, cpus=4, mem_mb=None)
            _write_pending(d, "pending-1", cpus=1)
            d._dispatch_pending()
            spec = JobSpec.read(d._spec_path("pending-1"))
            assert spec.state == JobState.PENDING
        finally:
            d._queue_lock_fd.close()

    def test_orphan_below_cap_still_allows_one_dispatch(
        self, tmp_path: Path
    ) -> None:
        """1 orphan against --max-jobs 2 leaves room for exactly one
        more — that one dispatches, a second stays PENDING."""
        d = _make_daemon(tmp_path, max_cpus=99, max_jobs=2)
        try:
            d._orphans["o1"] = _OrphanJob(pgid=900001, cpus=1, mem_mb=None)
            _write_pending(d, "pending-1")
            _write_pending(d, "pending-2")
            d._dispatch_pending()
            states = {
                jid: JobSpec.read(d._spec_path(jid)).state
                for jid in ("pending-1", "pending-2")
            }
            running = [j for j, s in states.items() if s == JobState.RUNNING]
            pending = [j for j, s in states.items() if s == JobState.PENDING]
            # orphan (1) + one fresh dispatch = 2 = the cap.
            assert len(running) == 1, states
            assert len(pending) == 1, states
        finally:
            for rj in d._running.values():
                with contextlib.suppress(Exception):
                    rj.popen.kill()
            d._queue_lock_fd.close()

    def test_survivors_fill_max_jobs_block_dispatch(
        self, tmp_path: Path
    ) -> None:
        """A killed job whose group outlived the wrapper vq reaped still
        occupies a job slot, so 2 survivors fill --max-jobs 2 and a PENDING
        job must not dispatch.

        max_cpus is 99 and neither survivor declares memory, so the job count
        is the only budget that can hold it — that is what pins the
        ``_terminal_survivors`` term in ``local_active`` specifically, rather
        than the cpu or memory sums beside it in ``_dispatch_pending``.
        """
        d = _make_daemon(tmp_path, max_cpus=99, max_jobs=2)
        try:
            d._terminal_survivors["s1"] = _survivor(d, "s1", pgid=900001)
            d._terminal_survivors["s2"] = _survivor(d, "s2", pgid=900002)
            _write_pending(d, "pending-1")
            d._dispatch_pending()
            assert (
                JobSpec.read(d._spec_path("pending-1")).state == JobState.PENDING
            )
            assert "pending-1" not in d._running

            # Once the escalation ends one group, the freed slot is usable —
            # so the block above was the survivor and not a stuck spec.
            del d._terminal_survivors["s2"]
            d._dispatch_pending()
            assert (
                JobSpec.read(d._spec_path("pending-1")).state == JobState.RUNNING
            )
        finally:
            for rj in d._running.values():
                with contextlib.suppress(Exception):
                    rj.popen.kill()
            d._queue_lock_fd.close()

    def test_no_orphans_dispatch_proceeds(self, tmp_path: Path) -> None:
        """Control: no orphans, budget free → the PENDING job DOES
        dispatch. Guards against the blocking tests passing for the
        wrong reason (e.g. a malformed spec that never dispatches)."""
        d = _make_daemon(tmp_path, max_cpus=99, max_jobs=2)
        try:
            _write_pending(d, "pending-1")
            d._dispatch_pending()
            spec = JobSpec.read(d._spec_path("pending-1"))
            assert spec.state == JobState.RUNNING
        finally:
            for rj in d._running.values():
                with contextlib.suppress(Exception):
                    rj.popen.kill()
            d._queue_lock_fd.close()
