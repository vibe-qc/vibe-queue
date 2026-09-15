"""v0.6.34: per-user [quotas] count reattached orphans.

v0.6.29 made the GLOBAL dispatch budgets (max_jobs / max_cpus /
max_mem) count reattached orphans, but deferred the per-user
`[quotas]` counting. Until this fix a daemon restart let a user
exceed their `max_pending_jobs` / `max_concurrent_cpus` by the
count + CPU footprint of their orphaned jobs — the per-user
analogue of the v0.6.29 gap.
"""
from __future__ import annotations

import contextlib
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


def _write_user_pending(uid: int, jobid: str, *, cpus: int) -> None:
    """Provision the user's state dir and drop a PENDING spec in it."""
    paths.provision_user_state(uid, os.getgid())
    ws = paths.user_workspace_dir(uid, jobid)
    ws.mkdir(parents=True, exist_ok=True)
    JobSpec(
        id=jobid,
        command=["true"],
        cwd=str(ws),
        cpus=cpus,
        state=JobState.PENDING,
        submitter=str(uid),
    ).write(paths.user_spec_path(uid, jobid))


class TestPerUserQuotaCountsOrphans:
    def test_orphan_cpus_count_against_max_concurrent_cpus(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = _make_mu_daemon(
            tmp_path, monkeypatch,
            quota_body="default_max_concurrent_cpus = 4\n",
        )
        try:
            # One orphan owned by the user, using all 4 quota CPUs.
            d._orphans["orph-1"] = _OrphanJob(
                pgid=900001, cpus=4, mem_mb=None, uid=str(uid),
            )
            _write_user_pending(uid, "pending-1", cpus=1)
            d._dispatch_pending()
            # The orphan fills the user's 4-CPU quota → the 1-CPU
            # pending job is held.
            spec = JobSpec.read(paths.user_spec_path(uid, "pending-1"))
            assert spec.state == JobState.PENDING
        finally:
            d._queue_lock_fd.close()

    def test_orphan_counts_against_max_pending_jobs(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = _make_mu_daemon(
            tmp_path, monkeypatch,
            quota_body="default_max_pending_jobs = 1\n",
        )
        try:
            d._orphans["orph-1"] = _OrphanJob(
                pgid=900001, cpus=1, mem_mb=None, uid=str(uid),
            )
            _write_user_pending(uid, "pending-1", cpus=1)
            d._dispatch_pending()
            # One orphan already fills the user's 1-job quota.
            spec = JobSpec.read(paths.user_spec_path(uid, "pending-1"))
            assert spec.state == JobState.PENDING
        finally:
            d._queue_lock_fd.close()

    def test_without_orphan_quota_gate_does_not_block(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Control: no orphan → the user's quota is free → the
        per-user gate does NOT hold the job (it leaves PENDING for
        dispatch). Guards against the quota tests passing for the
        wrong reason."""
        uid = os.getuid()
        d = _make_mu_daemon(
            tmp_path, monkeypatch,
            quota_body="default_max_concurrent_cpus = 4\n",
        )
        try:
            _write_user_pending(uid, "pending-1", cpus=1)
            d._dispatch_pending()
            # No orphan, quota free → the gate let the job through
            # (it leaves PENDING — whether the subsequent dispatch
            # then succeeds is not what this test checks).
            spec = JobSpec.read(paths.user_spec_path(uid, "pending-1"))
            assert spec.state != JobState.PENDING
        finally:
            for rj in d._running.values():
                with contextlib.suppress(Exception):
                    rj.popen.kill()
            d._queue_lock_fd.close()
