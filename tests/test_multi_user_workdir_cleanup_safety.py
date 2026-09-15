"""Trusted-path contract for root multi-user terminal workdir cleanup.

The multi-user daemon owns one managed workdir at
``paths.user_workdir(owner_uid, job_id)``.  Immediate terminal cleanup must
derive that target from daemon-owned identity, never trust the mutable
``JobSpec.workdir`` string, and refuse mismatched, traversing, or symlinked
targets.  A refusal keeps the terminal lifecycle state and persists a status-
visible diagnostic.

All paths live below ``tmp_path``.  The tests invoke only the cleanup helper;
they do not require root and start no daemon or child process.
"""
from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace

import pytest

from vq import cgroup, config, paths
from vq.daemon import Daemon, _OrphanJob
from vq.spec import JobSpec, JobState


@pytest.fixture
def multi_user_daemon(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Daemon]:
    monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "multi-user"))
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "config.toml").write_text(
        "[multi_user]\n"
        "enabled = true\n",
        encoding="utf-8",
    )
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(config_dir))
    monkeypatch.setattr(cgroup, "systemd_run_on_path", lambda: True)
    monkeypatch.setattr(cgroup, "stop_scope", lambda *_args, **_kwargs: True)
    daemon = Daemon(
        max_cpus=4,
        max_jobs=4,
        poll_interval=0.05,
        multi_user=True,
        queue_dir=tmp_path / "unused-queue",
        jobs_dir=tmp_path / "unused-jobs",
    )
    try:
        yield daemon
    finally:
        daemon._queue_lock_fd.close()


def _write_terminal_spec(
    daemon: Daemon,
    uid: str,
    job_id: str,
    *,
    workdir: Path | None,
    state: JobState = JobState.COMPLETED,
    submitter: str | None = None,
    failure_reason: str | None = None,
) -> tuple[JobSpec, Path]:
    workspace = paths.user_workspace_dir(uid, job_id)
    workspace.mkdir(parents=True, exist_ok=True)
    spec_path = paths.user_spec_path(uid, job_id)
    spec_path.parent.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(
        id=job_id,
        command=["true"],
        cwd=str(workspace),
        cpus=1,
        state=state,
        submitted_at="2026-08-08T12:00:00+00:00",
        started_at="2026-08-08T12:01:00+00:00",
        finished_at="2026-08-08T12:02:00+00:00",
        exit_code=0 if state == JobState.COMPLETED else 1,
        submitter=submitter if submitter is not None else uid,
        workdir=str(workdir) if workdir is not None else None,
        clean_workdir_on_terminal=True,
        failure_reason=failure_reason,
    )
    daemon._job_uid[job_id] = uid
    spec.write(spec_path)
    return spec, spec_path


def _write_running_cleanup_spec(
    daemon: Daemon,
    uid: str,
    job_id: str,
    *,
    workdir: Path,
    pgid: int | None,
) -> tuple[JobSpec, Path]:
    workspace = paths.user_workspace_dir(uid, job_id)
    workspace.mkdir(parents=True, exist_ok=True)
    spec_path = paths.user_spec_path(uid, job_id)
    spec_path.parent.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(
        id=job_id,
        command=["true"],
        cwd=str(workspace),
        cpus=1,
        state=JobState.RUNNING,
        submitted_at="2026-08-08T12:00:00+00:00",
        started_at="2026-08-08T12:01:00+00:00",
        submitter=uid,
        pgid=pgid,
        workdir=str(workdir),
        clean_workdir_on_terminal=True,
    )
    daemon._job_uid[job_id] = uid
    spec.write(spec_path)
    return spec, spec_path


def _assert_cleanup_skip_diagnostic(
    spec_path: Path,
    *,
    state: JobState,
    prior_reason: str | None = None,
) -> JobSpec:
    persisted = JobSpec.read(spec_path)
    assert persisted.state == state
    assert persisted.failure_reason is not None
    assert "workdir cleanup" in persisted.failure_reason.lower()
    if prior_reason is not None:
        assert prior_reason in persisted.failure_reason
    return persisted


def test_managed_owner_job_path_is_deleted_even_with_forged_submitter(
    multi_user_daemon: Daemon,
) -> None:
    owner_uid = str(os.getuid())
    managed = paths.user_workdir(owner_uid, "managed-job")
    managed.mkdir(parents=True)
    (managed / "result.dat").write_text("result", encoding="utf-8")
    spec, spec_path = _write_terminal_spec(
        multi_user_daemon,
        owner_uid,
        "managed-job",
        workdir=managed,
        submitter="999999",
    )

    multi_user_daemon._maybe_cleanup_workdir(spec)

    assert not managed.exists()
    persisted = JobSpec.read(spec_path)
    assert persisted.state == JobState.COMPLETED
    assert persisted.failure_reason is None


def test_mutable_workdir_mismatch_cannot_delete_outside_target(
    multi_user_daemon: Daemon,
    tmp_path: Path,
) -> None:
    owner_uid = str(os.getuid())
    managed = paths.user_workdir(owner_uid, "mismatch-job")
    managed.mkdir(parents=True)
    (managed / "managed.dat").write_text("keep", encoding="utf-8")
    outside = tmp_path / "outside-victim"
    outside.mkdir()
    (outside / "valuable.dat").write_text("keep", encoding="utf-8")
    spec, spec_path = _write_terminal_spec(
        multi_user_daemon,
        owner_uid,
        "mismatch-job",
        workdir=outside,
        state=JobState.FAILED,
        failure_reason="payload exited nonzero",
    )

    multi_user_daemon._maybe_cleanup_workdir(spec)

    assert managed.is_dir()
    assert (managed / "managed.dat").is_file()
    assert outside.is_dir()
    assert (outside / "valuable.dat").is_file()
    _assert_cleanup_skip_diagnostic(
        spec_path,
        state=JobState.FAILED,
        prior_reason="payload exited nonzero",
    )


def test_traversing_workdir_string_cannot_escape_managed_root(
    multi_user_daemon: Daemon,
) -> None:
    owner_uid = str(os.getuid())
    managed = paths.user_workdir(owner_uid, "traversal-job")
    managed.mkdir(parents=True)
    victim = paths.user_workdir_root(owner_uid).parent / "traversal-victim"
    victim.mkdir()
    (victim / "valuable.dat").write_text("keep", encoding="utf-8")
    traversing = paths.user_workdir_root(owner_uid) / ".." / victim.name
    spec, spec_path = _write_terminal_spec(
        multi_user_daemon,
        owner_uid,
        "traversal-job",
        workdir=traversing,
    )

    multi_user_daemon._maybe_cleanup_workdir(spec)

    assert managed.is_dir()
    assert victim.is_dir()
    assert (victim / "valuable.dat").is_file()
    _assert_cleanup_skip_diagnostic(
        spec_path,
        state=JobState.COMPLETED,
    )


def test_symlinked_managed_workdir_is_preserved_with_diagnostic(
    multi_user_daemon: Daemon,
    tmp_path: Path,
) -> None:
    owner_uid = str(os.getuid())
    outside = tmp_path / "symlink-target"
    outside.mkdir()
    (outside / "valuable.dat").write_text("keep", encoding="utf-8")
    managed = paths.user_workdir(owner_uid, "symlink-job")
    managed.parent.mkdir(parents=True)
    managed.symlink_to(outside, target_is_directory=True)
    spec, spec_path = _write_terminal_spec(
        multi_user_daemon,
        owner_uid,
        "symlink-job",
        workdir=managed,
    )

    multi_user_daemon._maybe_cleanup_workdir(spec)

    assert managed.is_symlink()
    assert outside.is_dir()
    assert (outside / "valuable.dat").is_file()
    _assert_cleanup_skip_diagnostic(
        spec_path,
        state=JobState.COMPLETED,
    )


def test_symlinked_managed_workdir_root_cannot_redirect_deletion(
    multi_user_daemon: Daemon,
    tmp_path: Path,
) -> None:
    owner_uid = str(os.getuid())
    workdir_root = paths.user_workdir_root(owner_uid)
    workdir_root.parent.mkdir(parents=True)
    outside_root = tmp_path / "outside-workdir-root"
    outside_job = outside_root / "root-symlink-job"
    outside_job.mkdir(parents=True)
    (outside_job / "valuable.dat").write_text("keep", encoding="utf-8")
    workdir_root.symlink_to(outside_root, target_is_directory=True)
    managed = paths.user_workdir(owner_uid, "root-symlink-job")
    spec, spec_path = _write_terminal_spec(
        multi_user_daemon,
        owner_uid,
        "root-symlink-job",
        workdir=managed,
    )

    multi_user_daemon._maybe_cleanup_workdir(spec)

    assert workdir_root.is_symlink()
    assert outside_job.is_dir()
    assert (outside_job / "valuable.dat").is_file()
    _assert_cleanup_skip_diagnostic(
        spec_path,
        state=JobState.COMPLETED,
    )


def test_late_collision_does_not_redirect_active_owner_cleanup(
    multi_user_daemon: Daemon,
) -> None:
    owner_uid = str(os.getuid())
    other_uid = str(os.getuid() + 10_000)
    job_id = "late-collision-cleanup"
    owner_workdir = paths.user_workdir(owner_uid, job_id)
    owner_workdir.mkdir(parents=True)
    (owner_workdir / "scratch.dat").write_text("delete", encoding="utf-8")
    owner_spec, owner_spec_path = _write_terminal_spec(
        multi_user_daemon,
        owner_uid,
        job_id,
        workdir=owner_workdir,
    )

    other_workdir = paths.user_workdir(other_uid, job_id)
    other_workdir.mkdir(parents=True)
    (other_workdir / "scratch.dat").write_text("keep", encoding="utf-8")
    _other_spec, other_spec_path = _write_terminal_spec(
        multi_user_daemon,
        other_uid,
        job_id,
        workdir=other_workdir,
    )
    other_before = other_spec_path.read_bytes()
    assert list(multi_user_daemon._iter_specs()) == []
    assert job_id not in multi_user_daemon._job_uid

    multi_user_daemon._running[job_id] = SimpleNamespace(
        owner_uid=owner_uid,
        spec_path=owner_spec_path,
    )
    multi_user_daemon._maybe_cleanup_workdir(owner_spec)

    assert not owner_workdir.exists()
    assert other_workdir.is_dir()
    assert other_spec_path.read_bytes() == other_before


def test_scheduler_without_local_workdir_has_no_false_cleanup_diagnostic(
    multi_user_daemon: Daemon,
) -> None:
    owner_uid = str(os.getuid())
    spec, spec_path = _write_terminal_spec(
        multi_user_daemon,
        owner_uid,
        "scheduler-no-local-workdir",
        workdir=None,
    )
    spec.scheduler_target = "host_f"
    spec.write(spec_path)
    before = spec_path.read_bytes()

    multi_user_daemon._maybe_cleanup_workdir(
        spec,
        owner_uid=owner_uid,
        spec_path=spec_path,
    )

    assert spec_path.read_bytes() == before
    assert JobSpec.read(spec_path).failure_reason is None


@pytest.mark.parametrize("reaper", ["local", "orphan"])
def test_preexisting_terminal_label_cleans_only_after_actual_reap(
    multi_user_daemon: Daemon,
    reaper: str,
) -> None:
    owner_uid = str(os.getuid())
    job_id = f"preterminal-{reaper}"
    managed = paths.user_workdir(owner_uid, job_id)
    managed.mkdir(parents=True)
    (managed / "scratch.dat").write_text("delete", encoding="utf-8")
    spec, spec_path = _write_terminal_spec(
        multi_user_daemon,
        owner_uid,
        job_id,
        workdir=managed,
        state=JobState.KILLED,
    )

    if reaper == "local":
        multi_user_daemon._running[job_id] = SimpleNamespace(
            owner_uid=owner_uid,
            spec_path=spec_path,
        )
        multi_user_daemon._record_finish(job_id, rc=143)
    else:
        multi_user_daemon._orphans[job_id] = _OrphanJob(
            pgid=999_999,
            cpus=1,
            mem_mb=None,
            uid=owner_uid,
            spec_path=spec_path,
        )
        multi_user_daemon._record_orphan_finish(
            spec,
            rc=143,
            source="test actual exit",
        )

    assert not managed.exists()
    assert JobSpec.read(spec_path).state == JobState.KILLED


def test_startup_unknown_liveness_does_not_clean_workdir(
    multi_user_daemon: Daemon,
) -> None:
    owner_uid = str(os.getuid())
    job_id = "startup-popen-window"
    managed = paths.user_workdir(owner_uid, job_id)
    managed.mkdir(parents=True)
    (managed / "scratch.dat").write_text("still in use", encoding="utf-8")
    _spec, spec_path = _write_running_cleanup_spec(
        multi_user_daemon,
        owner_uid,
        job_id,
        workdir=managed,
        pgid=None,
    )

    # pgid=None is ambiguous: the old daemon may have died before Popen or
    # after spawning but before recording the process group.
    multi_user_daemon._reattach_or_interrupt_at_startup()

    persisted = JobSpec.read(spec_path)
    assert persisted.state == JobState.ABORTED_BY_QUEUE
    assert managed.is_dir()
    assert (managed / "scratch.dat").is_file()


def test_confirmed_abort_exit_cleans_workdir(
    multi_user_daemon: Daemon,
) -> None:
    owner_uid = str(os.getuid())
    job_id = "confirmed-abort-exit"
    managed = paths.user_workdir(owner_uid, job_id)
    managed.mkdir(parents=True)
    (managed / "scratch.dat").write_text("delete", encoding="utf-8")
    spec, spec_path = _write_running_cleanup_spec(
        multi_user_daemon,
        owner_uid,
        job_id,
        workdir=managed,
        pgid=999_999,
    )

    multi_user_daemon._mark_aborted_by_queue(
        spec,
        reason="process group is confirmed gone",
        process_exit_confirmed=True,
    )

    assert JobSpec.read(spec_path).state == JobState.ABORTED_BY_QUEUE
    assert not managed.exists()


def test_metadata_value_error_is_persisted_as_cleanup_refusal(
    multi_user_daemon: Daemon,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner_uid = str(os.getuid())
    job_id = "metadata-value-error"
    managed = paths.user_workdir(owner_uid, job_id)
    managed.mkdir(parents=True)
    spec, spec_path = _write_terminal_spec(
        multi_user_daemon,
        owner_uid,
        job_id,
        workdir=managed,
    )
    real_stat = os.stat

    def broken_metadata_stat(path, *args, **kwargs):
        if kwargs.get("dir_fd") is not None:
            raise ValueError("invalid descriptor-relative metadata")
        return real_stat(path, *args, **kwargs)

    monkeypatch.setattr(os, "stat", broken_metadata_stat)

    multi_user_daemon._maybe_cleanup_workdir(
        spec,
        owner_uid=owner_uid,
        spec_path=spec_path,
    )

    assert managed.is_dir()
    _assert_cleanup_skip_diagnostic(
        spec_path,
        state=JobState.COMPLETED,
    )
