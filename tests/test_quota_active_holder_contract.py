"""Approved per-user quota contract for active resource holders.

``max_pending_jobs`` is an active-work dispatch cap despite its historical
name.  Job-count and CPU quota accounting must use the same deduplicated holder
set across daemon ticks: local children, scheduler handles, local orphans,
SUSPENDED jobs, and scheduler ``reattach_failed`` reservations.  PENDING specs
are candidates, not holders.

These tests are hermetic.  Dispatch is replaced with an in-memory state write;
no child process, scheduler, SSH connection, or daemon loop is started.
"""
from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from vq import cgroup, config, paths
from vq.daemon import (
    SCHEDULER_UNTRACKABLE_RESERVATION_SECONDS,
    Daemon,
    _OrphanJob,
    _SchedulerJob,
    _TerminalSurvivor,
)
from vq.scheduler_dispatch import SchedulerHandle
from vq.spec import JobSpec, JobState


@pytest.fixture
def daemon_factory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Callable[[str], Daemon]]:
    daemons: list[Daemon] = []

    def make(quota_body: str) -> Daemon:
        root = tmp_path / f"multi-user-{len(daemons)}"
        config_dir = tmp_path / f"config-{len(daemons)}"
        config_dir.mkdir()
        (config_dir / "config.toml").write_text(
            "[multi_user]\n"
            "enabled = true\n\n"
            "[quotas]\n"
            f"{quota_body}",
            encoding="utf-8",
        )
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(root))
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(config_dir))
        monkeypatch.setattr(cgroup, "systemd_run_on_path", lambda: True)
        daemon = Daemon(
            max_cpus=999,
            max_jobs=999,
            max_scheduler_jobs=999,
            poll_interval=0.05,
            multi_user=True,
            queue_dir=tmp_path / f"queue-{len(daemons)}",
            jobs_dir=tmp_path / f"jobs-{len(daemons)}",
        )
        daemons.append(daemon)
        return daemon

    yield make

    for daemon in daemons:
        daemon._queue_lock_fd.close()


def _write_spec(
    uid: str,
    job_id: str,
    *,
    state: JobState,
    cpus: int = 1,
    scheduler: bool = False,
    scheduler_state: str | None = None,
    scheduler_job_id: str | None = None,
    last_heartbeat_at: str | None = None,
) -> JobSpec:
    workspace = paths.user_workspace_dir(uid, job_id)
    workspace.mkdir(parents=True, exist_ok=True)
    spec_path = paths.user_spec_path(uid, job_id)
    spec_path.parent.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(
        id=job_id,
        command=["true"],
        cwd=str(workspace),
        cpus=cpus,
        state=state,
        submitter=uid,
        scheduler_target="host_f" if scheduler else None,
        scheduler_state=scheduler_state,
        scheduler_job_id=scheduler_job_id,
        started_at=(
            "2026-08-08T12:00:00+00:00"
            if state in {JobState.RUNNING, JobState.SUSPENDED}
            else None
        ),
        last_heartbeat_at=last_heartbeat_at,
    )
    spec.write(spec_path)
    return spec


def _scheduler_holder(spec: JobSpec) -> _SchedulerJob:
    return _SchedulerJob(
        handle=SchedulerHandle(
            job_id=spec.scheduler_job_id or f"pbs-{spec.id}",
            remote_workspace=f"/remote/{spec.id}",
        ),
        dispatcher=object(),  # type: ignore[arg-type]
        cpus=spec.cpus,
        mem_mb=spec.mem_mb,
    )


def _install_fake_start(
    daemon: Daemon,
    monkeypatch: pytest.MonkeyPatch,
    *,
    track_started_scheduler: bool = False,
) -> list[str]:
    started: list[str] = []

    def fake_start(spec: JobSpec) -> bool:
        spec.state = JobState.RUNNING
        if spec.scheduler_target is not None and track_started_scheduler:
            spec.scheduler_job_id = f"pbs-{spec.id}"
            daemon._scheduler_running[spec.id] = _scheduler_holder(spec)
        elif spec.scheduler_target is None:
            daemon._running[spec.id] = SimpleNamespace(
                cpus=spec.cpus,
                mem_mb=spec.mem_mb,
            )
        spec.write(daemon._spec_path(spec.id))
        started.append(spec.id)
        return True

    monkeypatch.setattr(daemon, "_start_job", fake_start)
    return started


@pytest.mark.parametrize(
    "quota_body",
    [
        "default_max_pending_jobs = 1\n",
        "default_max_concurrent_cpus = 4\n",
    ],
)
def test_running_scheduler_handle_holds_job_and_cpu_quota(
    daemon_factory,
    monkeypatch: pytest.MonkeyPatch,
    quota_body: str,
) -> None:
    uid = str(os.getuid())
    daemon = daemon_factory(quota_body)
    holder = _write_spec(
        uid,
        "scheduler-holder",
        state=JobState.RUNNING,
        cpus=4,
        scheduler=True,
        scheduler_job_id="501.cluster",
    )
    daemon._scheduler_running[holder.id] = _scheduler_holder(holder)
    _write_spec(uid, "pending-local", state=JobState.PENDING)
    started = _install_fake_start(daemon, monkeypatch)

    daemon._dispatch_pending()

    assert started == []


@pytest.mark.parametrize(
    "quota_body",
    [
        "default_max_pending_jobs = 2\n",
        "default_max_concurrent_cpus = 3\n",
    ],
)
def test_local_suspended_child_counts_once(
    daemon_factory,
    monkeypatch: pytest.MonkeyPatch,
    quota_body: str,
) -> None:
    uid = str(os.getuid())
    daemon = daemon_factory(quota_body)
    holder = _write_spec(
        uid,
        "local-suspended",
        state=JobState.SUSPENDED,
        cpus=2,
    )
    daemon._running[holder.id] = SimpleNamespace(cpus=2, mem_mb=None)
    _write_spec(uid, "pending-local", state=JobState.PENDING)
    started = _install_fake_start(daemon, monkeypatch)

    daemon._dispatch_pending()

    assert started == ["pending-local"]


@pytest.mark.parametrize(
    "quota_body",
    [
        "default_max_pending_jobs = 2\n",
        "default_max_concurrent_cpus = 3\n",
    ],
)
def test_scheduler_suspended_handle_counts_once(
    daemon_factory,
    monkeypatch: pytest.MonkeyPatch,
    quota_body: str,
) -> None:
    uid = str(os.getuid())
    daemon = daemon_factory(quota_body)
    holder = _write_spec(
        uid,
        "scheduler-suspended",
        state=JobState.SUSPENDED,
        cpus=2,
        scheduler=True,
        scheduler_job_id="502.cluster",
    )
    daemon._scheduler_running[holder.id] = _scheduler_holder(holder)
    _write_spec(uid, "pending-local", state=JobState.PENDING)
    started = _install_fake_start(daemon, monkeypatch)

    daemon._dispatch_pending()

    assert started == ["pending-local"]


@pytest.mark.parametrize(
    "quota_body",
    [
        "default_max_pending_jobs = 2\n",
        "default_max_concurrent_cpus = 3\n",
    ],
)
def test_suspended_orphan_counts_once(
    daemon_factory,
    monkeypatch: pytest.MonkeyPatch,
    quota_body: str,
) -> None:
    uid = str(os.getuid())
    daemon = daemon_factory(quota_body)
    holder = _write_spec(
        uid,
        "suspended-orphan",
        state=JobState.SUSPENDED,
        cpus=2,
    )
    daemon._orphans[holder.id] = _OrphanJob(
        pgid=900001,
        cpus=2,
        mem_mb=None,
        uid=uid,
    )
    _write_spec(uid, "pending-local", state=JobState.PENDING)
    started = _install_fake_start(daemon, monkeypatch)

    daemon._dispatch_pending()

    assert started == ["pending-local"]


@pytest.mark.parametrize(
    "quota_body",
    [
        "default_max_pending_jobs = 1\n",
        "default_max_concurrent_cpus = 4\n",
    ],
)
def test_killed_job_survivor_holds_job_and_cpu_quota(
    daemon_factory,
    monkeypatch: pytest.MonkeyPatch,
    quota_body: str,
) -> None:
    """A killed job whose process group outlived its reaped wrapper is
    still running on the host until the kill grace ends, so it keeps its
    owner's job and CPU quota. Its spec is already KILLED, so only the
    in-memory survivor record can charge it."""
    uid = str(os.getuid())
    daemon = daemon_factory(quota_body)
    holder = _write_spec(uid, "killed-survivor", state=JobState.KILLED, cpus=4)
    daemon._terminal_survivors[holder.id] = _TerminalSurvivor(
        pgid=900002,
        cpus=4,
        mem_mb=None,
        state=JobState.KILLED,
        workspace=Path(holder.cwd),
        deadline=float("inf"),
        owner_uid=uid,
    )
    _write_spec(uid, "pending-local", state=JobState.PENDING)
    started = _install_fake_start(daemon, monkeypatch)

    daemon._dispatch_pending()

    assert started == []


@pytest.mark.parametrize(
    "quota_body",
    [
        "default_max_pending_jobs = 1\n",
        "default_max_concurrent_cpus = 4\n",
    ],
)
def test_reattach_failed_reservation_holds_job_and_cpu_quota(
    daemon_factory,
    monkeypatch: pytest.MonkeyPatch,
    quota_body: str,
) -> None:
    uid = str(os.getuid())
    daemon = daemon_factory(quota_body)
    _write_spec(
        uid,
        "reattach-holder",
        state=JobState.RUNNING,
        cpus=4,
        scheduler=True,
        scheduler_state="reattach_failed",
        scheduler_job_id="503.cluster",
    )
    _write_spec(uid, "pending-local", state=JobState.PENDING)
    started = _install_fake_start(daemon, monkeypatch)

    daemon._dispatch_pending()

    assert started == []


def test_stale_untrackable_reattach_row_releases_quota(
    daemon_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    uid = str(os.getuid())
    daemon = daemon_factory("default_max_pending_jobs = 1\n")
    stale = datetime.now(UTC) - timedelta(
        seconds=SCHEDULER_UNTRACKABLE_RESERVATION_SECONDS + 60
    )
    _write_spec(
        uid,
        "stale-reattach",
        state=JobState.RUNNING,
        scheduler=True,
        scheduler_state="reattach_failed",
        last_heartbeat_at=stale.isoformat(),
    )
    _write_spec(uid, "pending-local", state=JobState.PENDING)
    started = _install_fake_start(daemon, monkeypatch)

    daemon._dispatch_pending()

    assert started == ["pending-local"]


def test_scheduler_holder_remains_counted_on_later_dispatch_ticks(
    daemon_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    uid = str(os.getuid())
    daemon = daemon_factory("default_max_pending_jobs = 1\n")
    _write_spec(
        uid,
        "scheduler-first",
        state=JobState.PENDING,
        scheduler=True,
    )
    _write_spec(
        uid,
        "scheduler-second",
        state=JobState.PENDING,
        scheduler=True,
    )
    started = _install_fake_start(
        daemon,
        monkeypatch,
        track_started_scheduler=True,
    )

    daemon._dispatch_pending()
    assert started == ["scheduler-first"]

    daemon._dispatch_pending()
    assert started == ["scheduler-first"]


def test_late_collision_keeps_active_holder_charged_to_admission_owner(
    daemon_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner_uid = str(os.getuid())
    other_uid = str(os.getuid() + 10_000)
    daemon = daemon_factory("default_max_pending_jobs = 1\n")
    holder = _write_spec(
        owner_uid,
        "late-collision-holder",
        state=JobState.RUNNING,
    )
    holder_path = paths.user_spec_path(owner_uid, holder.id)
    daemon._running[holder.id] = SimpleNamespace(
        cpus=holder.cpus,
        mem_mb=holder.mem_mb,
        owner_uid=owner_uid,
        spec_path=holder_path,
    )
    _write_spec(
        other_uid,
        holder.id,
        state=JobState.PENDING,
    )
    _write_spec(owner_uid, "owner-pending", state=JobState.PENDING)
    started = _install_fake_start(daemon, monkeypatch)

    daemon._dispatch_pending()

    assert started == []


@pytest.mark.parametrize(
    ("active_state", "scheduler_state", "scheduler_job_id"),
    [
        (JobState.RUNNING, "running", "504.cluster"),
        (
            JobState.SUBMIT_OUTCOME_UNKNOWN,
            "submit_outcome_unknown",
            None,
        ),
    ],
)
def test_colliding_untracked_scheduler_row_keeps_global_reservation(
    daemon_factory,
    monkeypatch: pytest.MonkeyPatch,
    active_state: JobState,
    scheduler_state: str,
    scheduler_job_id: str | None,
) -> None:
    owner_uid = str(os.getuid())
    other_uid = str(os.getuid() + 10_000)
    pending_uid = str(os.getuid() + 20_000)
    daemon = daemon_factory("")
    daemon.max_scheduler_jobs = 1
    collision_id = "colliding-reattach-global"
    _write_spec(
        owner_uid,
        collision_id,
        state=active_state,
        scheduler=True,
        scheduler_state=scheduler_state,
        scheduler_job_id=scheduler_job_id,
    )
    _write_spec(
        other_uid,
        collision_id,
        state=JobState.PENDING,
        scheduler=True,
    )
    _write_spec(
        pending_uid,
        "unrelated-scheduler-pending",
        state=JobState.PENDING,
        scheduler=True,
    )
    started = _install_fake_start(
        daemon,
        monkeypatch,
        track_started_scheduler=True,
    )

    daemon._dispatch_pending()

    assert started == []
    assert [
        (uid, spec.id)
        for uid, _path, spec in daemon._colliding_scheduler_reservations
    ] == [(owner_uid, collision_id)]


@pytest.mark.parametrize(
    "quota_body",
    [
        "default_max_pending_jobs = 1\n",
        "default_max_concurrent_cpus = 4\n",
    ],
)
def test_colliding_untracked_scheduler_row_keeps_owner_quota(
    daemon_factory,
    monkeypatch: pytest.MonkeyPatch,
    quota_body: str,
) -> None:
    owner_uid = str(os.getuid())
    other_uid = str(os.getuid() + 10_000)
    daemon = daemon_factory(quota_body)
    collision_id = "colliding-reattach-quota"
    _write_spec(
        owner_uid,
        collision_id,
        state=JobState.RUNNING,
        cpus=4,
        scheduler=True,
        scheduler_state="reattach_failed",
        scheduler_job_id="505.cluster",
    )
    _write_spec(
        other_uid,
        collision_id,
        state=JobState.PENDING,
        scheduler=True,
    )
    _write_spec(owner_uid, "owner-pending-after-collision", state=JobState.PENDING)
    started = _install_fake_start(daemon, monkeypatch)

    daemon._dispatch_pending()

    assert started == []
