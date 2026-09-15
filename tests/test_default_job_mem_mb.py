"""default_job_mem_mb: undeclared-memory jobs are charged a default so
they can no longer be dispatched unbounded (the 2026-06-21 AICCM
build-storm hardening).

With the default set, a job that declares no ``mem_mb`` counts its
default against the daemon memory budget at admission AND is cgroup-capped
at it, so it cannot oversubscribe the host or drive it into swap. With the
default ``None``, the legacy v0.3 leniency holds (undeclared == unbounded).
See ``daemon.py`` ``_eff_mem_mb`` / ``_dispatch_pending``.
"""
from __future__ import annotations

import contextlib
from pathlib import Path

from vq.daemon import Daemon, _TerminalSurvivor
from vq.spec import JobSpec, JobState


def _make_daemon(
    tmp_path: Path,
    *,
    max_mem_mb: int | None,
    default_job_mem_mb: int | None,
) -> Daemon:
    # max_cpus / max_jobs are set high so memory is the only gate exercised.
    d = Daemon(
        max_cpus=99,
        max_jobs=99,
        max_mem_mb=max_mem_mb,
        default_job_mem_mb=default_job_mem_mb,
        poll_interval=0.05,
        queue_dir=tmp_path / "queue",
        jobs_dir=tmp_path / "jobs",
    )
    d.queue_dir.mkdir(parents=True, exist_ok=True)
    d.jobs_dir.mkdir(parents=True, exist_ok=True)
    return d


def _write_pending(d: Daemon, jobid: str, *, mem_mb: int | None = None) -> None:
    ws = d.jobs_dir / jobid
    ws.mkdir(parents=True, exist_ok=True)
    JobSpec(
        id=jobid,
        command=["true"],
        cwd=str(ws),
        cpus=1,
        mem_mb=mem_mb,
        state=JobState.PENDING,
    ).write(d._spec_path(jobid))


def _states(d: Daemon, jobids: list[str]) -> dict[str, JobState]:
    return {jid: JobSpec.read(d._spec_path(jid)).state for jid in jobids}


def _cleanup(d: Daemon) -> None:
    for rj in d._running.values():
        with contextlib.suppress(Exception):
            rj.popen.kill()
    d._queue_lock_fd.close()


def test_eff_mem_uses_default_when_undeclared(tmp_path: Path) -> None:
    d = _make_daemon(tmp_path, max_mem_mb=10000, default_job_mem_mb=4000)
    try:
        assert d._eff_mem_mb(None) == 4000   # undeclared -> default
        assert d._eff_mem_mb(7000) == 7000   # declared -> declared
    finally:
        d._queue_lock_fd.close()


def test_eff_mem_none_when_no_default(tmp_path: Path) -> None:
    d = _make_daemon(tmp_path, max_mem_mb=10000, default_job_mem_mb=None)
    try:
        assert d._eff_mem_mb(None) is None   # legacy: undeclared unbounded
        assert d._eff_mem_mb(7000) == 7000
    finally:
        d._queue_lock_fd.close()


def test_undeclared_jobs_gated_by_default(tmp_path: Path) -> None:
    """Budget 10000, default 4000 -> only 2 undeclared jobs fit
    (2*4000=8000 ok; a 3rd would be 12000 > 10000). Pre-fix all 4
    dispatched because undeclared jobs bypassed the memory gate."""
    d = _make_daemon(tmp_path, max_mem_mb=10000, default_job_mem_mb=4000)
    try:
        ids = [f"u{i}" for i in range(4)]
        for jid in ids:
            _write_pending(d, jid)  # all undeclared
        d._dispatch_pending()
        st = _states(d, ids)
        running = [j for j, s in st.items() if s == JobState.RUNNING]
        pending = [j for j, s in st.items() if s == JobState.PENDING]
        assert len(running) == 2, st
        assert len(pending) == 2, st
    finally:
        _cleanup(d)


def test_no_default_undeclared_unbounded(tmp_path: Path) -> None:
    """Control: default None -> the old leniency. All 4 undeclared jobs
    dispatch despite the budget, since the gate is skipped. Guards the
    gated test against passing for the wrong reason."""
    d = _make_daemon(tmp_path, max_mem_mb=10000, default_job_mem_mb=None)
    try:
        ids = [f"u{i}" for i in range(4)]
        for jid in ids:
            _write_pending(d, jid)
        d._dispatch_pending()
        st = _states(d, ids)
        running = [j for j, s in st.items() if s == JobState.RUNNING]
        assert len(running) == 4, st
    finally:
        _cleanup(d)


def test_declared_mem_still_gates(tmp_path: Path) -> None:
    """A job that declares mem_mb is gated on its declared value even with
    a default set: budget 10000, one declared 9000 job runs, a second
    (would be 18000) stays PENDING."""
    d = _make_daemon(tmp_path, max_mem_mb=10000, default_job_mem_mb=2000)
    try:
        _write_pending(d, "big-1", mem_mb=9000)
        _write_pending(d, "big-2", mem_mb=9000)
        d._dispatch_pending()
        st = _states(d, ["big-1", "big-2"])
        assert set(st.values()) == {JobState.RUNNING, JobState.PENDING}, st
    finally:
        _cleanup(d)


def test_killed_job_survivor_still_holds_its_memory(tmp_path: Path) -> None:
    """A killed job whose process group outlived its reaped wrapper is still
    using its memory until the kill grace ends, so a job that would
    oversubscribe the budget stays PENDING. cpus and jobs have headroom, so
    memory is the only gate that can hold it; releasing the survivor lets the
    same job dispatch, which proves the survivor was what held it."""
    d = _make_daemon(tmp_path, max_mem_mb=10000, default_job_mem_mb=None)
    try:
        d._terminal_survivors["killed-1"] = _TerminalSurvivor(
            pgid=900003,
            cpus=1,
            mem_mb=9000,
            state=JobState.KILLED,
            workspace=d.jobs_dir / "killed-1",
            deadline=float("inf"),
        )
        _write_pending(d, "big-2", mem_mb=9000)
        d._dispatch_pending()
        assert _states(d, ["big-2"]) == {"big-2": JobState.PENDING}

        del d._terminal_survivors["killed-1"]
        d._dispatch_pending()
        assert _states(d, ["big-2"]) == {"big-2": JobState.RUNNING}
    finally:
        _cleanup(d)
