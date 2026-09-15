"""v0.6.36: `_dispatch_pending` scans the queue dirs once per tick.

Pre-v0.6.36 the multi-user dispatch path called ``_iter_specs()``
three times per tick — once to sort PENDING, once to count
SUSPENDED jobs against per-user quota, once to re-sort PENDING
(``_iter_specs`` clears + repopulates ``_job_uid``, so the first
``pending`` list had to be rebuilt). v0.6.36 materialises the spec
list once and derives all three from it.

The behaviour these tests pin: the SUSPENDED-counts-toward-quota
path — which moved from a dedicated ``_iter_specs()`` scan to a
filter over the single materialised list — still works.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from vq import cgroup, config, paths
from vq.daemon import Daemon, _OrphanJob
from vq.spec import JobSpec, JobState


def _make_mu_daemon(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    quota_body: str,
) -> Daemon:
    """A multi-user Daemon with a tmp state root + a [quotas] config."""
    monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "mu"))
    cfgdir = tmp_path / "cfg"
    cfgdir.mkdir()
    (cfgdir / "config.toml").write_text(
        "[multi_user]\nenabled = true\n\n[quotas]\n" + quota_body
    )
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfgdir))
    monkeypatch.setattr(cgroup, "systemd_run_on_path", lambda: True)
    return Daemon(
        max_cpus=999,
        max_jobs=999,
        poll_interval=0.05,
        multi_user=True,
        queue_dir=tmp_path / "q",
        jobs_dir=tmp_path / "j",
    )


def _write_user_spec(
    uid: int, jobid: str, *, state: JobState, cpus: int = 1
) -> None:
    """Provision the user's state dir and drop a spec in it."""
    paths.provision_user_state(uid, os.getgid())
    ws = paths.user_workspace_dir(uid, jobid)
    ws.mkdir(parents=True, exist_ok=True)
    JobSpec(
        id=jobid,
        command=["true"],
        cwd=str(ws),
        cpus=cpus,
        state=state,
        submitter=str(uid),
    ).write(paths.user_spec_path(uid, jobid))


class TestSingleScanStillCountsSuspended:
    def test_suspended_job_counts_against_max_pending_jobs(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = _make_mu_daemon(
            tmp_path, monkeypatch,
            quota_body="default_max_pending_jobs = 1\n",
        )
        try:
            # One SUSPENDED job already fills the user's 1-job quota.
            _write_user_spec(uid, "susp-1", state=JobState.SUSPENDED)
            _write_user_spec(uid, "pending-1", state=JobState.PENDING)
            d._dispatch_pending()
            # The SUSPENDED job still holds quota → the PENDING job
            # is held (not dispatched). This is the path that moved
            # from a separate _iter_specs() scan to a filter over the
            # single materialised list.
            spec = JobSpec.read(paths.user_spec_path(uid, "pending-1"))
            assert spec.state == JobState.PENDING
        finally:
            d._queue_lock_fd.close()

    def test_suspended_cpus_count_against_concurrent_cpus(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = _make_mu_daemon(
            tmp_path, monkeypatch,
            quota_body="default_max_concurrent_cpus = 4\n",
        )
        try:
            # A 4-CPU SUSPENDED job uses the user's whole CPU quota.
            _write_user_spec(uid, "susp-1", state=JobState.SUSPENDED, cpus=4)
            _write_user_spec(uid, "pending-1", state=JobState.PENDING, cpus=1)
            d._dispatch_pending()
            spec = JobSpec.read(paths.user_spec_path(uid, "pending-1"))
            assert spec.state == JobState.PENDING
        finally:
            d._queue_lock_fd.close()

    def test_suspended_plus_orphan_both_count(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A SUSPENDED job (materialised-list path) and an orphan
        # (_orphans path) must BOTH count toward the same user's
        # quota in one dispatch pass.
        uid = os.getuid()
        d = _make_mu_daemon(
            tmp_path, monkeypatch,
            quota_body="default_max_pending_jobs = 2\n",
        )
        try:
            _write_user_spec(uid, "susp-1", state=JobState.SUSPENDED)
            d._orphans["orph-1"] = _OrphanJob(
                pgid=900001, cpus=1, mem_mb=None, uid=str(uid),
            )
            _write_user_spec(uid, "pending-1", state=JobState.PENDING)
            d._dispatch_pending()
            # SUSPENDED (1) + orphan (1) == the 2-job quota → held.
            spec = JobSpec.read(paths.user_spec_path(uid, "pending-1"))
            assert spec.state == JobState.PENDING
        finally:
            d._queue_lock_fd.close()
