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

These tests exercise cross-user host accounting and the order of the
global and per-user gates through actual dispatch, independently of the
source layout of the dispatch loop.
"""

from __future__ import annotations

import contextlib
import os
from pathlib import Path

import pytest

from vq import cgroup, config, paths
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
    @pytest.mark.parametrize(
        "max_cpus,user_cpus,expect_quota,expect_start",
        [
            pytest.param(4, 100, False, False, id="global-refusal"),
            pytest.param(8, 1, True, False, id="per-user-refusal"),
            pytest.param(8, 4, True, True, id="both-gates-admit"),
        ],
    )
    def test_global_cap_caught_before_per_user_quota_check(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        max_cpus: int, user_cpus: int, expect_quota: bool,
        expect_start: bool,
    ) -> None:
        """Another user's usage counts globally, before this user's quota.

        Positive controls ensure that an unrelated early gate cannot make
        either refusal pass. Stub only process launch; exercise the real
        multi-user queue scan, resource accounting and quota checks.
        """
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "mu"))
        cfgdir = tmp_path / "cfg"
        cfgdir.mkdir()
        (cfgdir / "config.toml").write_text(
            "[multi_user]\nenabled = true\n\n[quotas]\n"
            f"default_max_concurrent_cpus = {user_cpus}\n"
        )
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfgdir))
        monkeypatch.setattr(cgroup, "systemd_run_on_path", lambda: True)
        d = Daemon(
            max_cpus=max_cpus, max_jobs=99, poll_interval=0.05,
            multi_user=True, queue_dir=tmp_path / "q", jobs_dir=tmp_path / "j",
        )
        uid = os.getuid()
        quota_calls: list[tuple[str, str]] = []
        starts: list[str] = []
        for name in ("effective_max_pending_jobs", "effective_max_concurrent_cpus"):
            original = getattr(config.QuotaConfig, name)

            def tracked(self, user, _name=name, _original=original):
                quota_calls.append((_name, str(user)))
                return _original(self, user)

            monkeypatch.setattr(config.QuotaConfig, name, tracked)

        def record_start(spec, **kwargs):
            starts.append(spec.id)
            return False  # No subprocess or running-job state is created.

        monkeypatch.setattr(d, "_start_job", record_start)
        try:
            d._orphans["other-user-orphan"] = _OrphanJob(
                pgid=900001, cpus=3, mem_mb=None, uid=str(uid + 1),
            )
            paths.provision_user_state(uid, os.getgid())
            jobid = "pending-gate-order"
            ws = paths.user_workspace_dir(uid, jobid)
            ws.mkdir(parents=True, exist_ok=True)
            JobSpec(
                id=jobid, command=["true"], cwd=str(ws), cpus=2,
                submitter=str(uid), state=JobState.PENDING,
            ).write(paths.user_spec_path(uid, jobid))
            d._dispatch_pending()
            assert quota_calls == (
                [("effective_max_pending_jobs", str(uid)),
                 ("effective_max_concurrent_cpus", str(uid))]
                if expect_quota else []
            )
            assert starts == ([jobid] if expect_start else [])
            assert JobSpec.read(paths.user_spec_path(uid, jobid)).state == JobState.PENDING
        finally:
            d._queue_lock_fd.close()
