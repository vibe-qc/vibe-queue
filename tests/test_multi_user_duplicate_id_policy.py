"""M12b: duplicate bare job IDs fail closed across multi-user state trees.

The public job ID remains bare for compatibility, but storage ownership is the
per-UID spec path.  A duplicate that exists before admission is quarantined for
dispatch and bare lookup without mutating either owner's record.  A duplicate
that appears after admission cannot redirect reconciliation away from the
owner-qualified path captured by the active local or scheduler record.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

import vq.daemon as daemon_module
from vq import cgroup, config, paths, throttle
from vq.daemon import Daemon
from vq.scheduler_dialect import QstatDetail, SchedulerPhase
from vq.scheduler_dispatch import SchedulerHandle
from vq.spec import JobSpec, JobState

_UID_A = "1000"
_UID_B = "2000"
_UID_UNIQUE = "3000"
_COLLIDING_ID = "deadbeef0000"
_UNIQUE_ID = "c0ffee123456"
_SUBMITTED_AT = "2026-08-08T12:00:00+00:00"


class _LocalHandle:
    pid = 999_999_999


class _LocalDispatcher:
    def __init__(self) -> None:
        self.return_code: int | None = None

    def launch(self, **_kwargs: object) -> _LocalHandle:
        return _LocalHandle()

    def poll(self, _handle: _LocalHandle) -> int | None:
        return self.return_code

    def terminate(self, _handle: _LocalHandle) -> None:
        pass

    def kill(self, _handle: _LocalHandle) -> None:
        pass

    def wait(self, _handle: _LocalHandle, timeout: float) -> bool:
        del timeout
        return True


class _SchedulerRecorder:
    def __init__(self) -> None:
        self.finished = False
        self.submitted: list[str] = []
        self.fetched_into: list[Path] = []

    def remote_workspace(self, job_id: str) -> str:
        return f"/remote/{job_id}"

    def submit(
        self,
        *,
        job_id: str,
        local_workspace: Path | None,
        **_kwargs: object,
    ) -> SchedulerHandle:
        assert local_workspace is not None
        self.submitted.append(job_id)
        return SchedulerHandle(
            job_id=f"cluster-{job_id}",
            remote_workspace=f"/remote/{job_id}",
        )

    def poll(
        self, handles: list[SchedulerHandle]
    ) -> dict[str, SchedulerPhase]:
        phase = SchedulerPhase.FINISHED if self.finished else SchedulerPhase.RUNNING
        return {handle.job_id: phase for handle in handles}

    def poll_detail(
        self, _handles: list[SchedulerHandle]
    ) -> dict[str, QstatDetail]:
        return {}

    def phase_from_detail(self, _detail: QstatDetail) -> SchedulerPhase:
        return SchedulerPhase.FINISHED

    def exit_marker_code(
        self,
        _handle: SchedulerHandle,
        *,
        array_index: int | None = None,
    ) -> int | None:
        del array_index
        return 0 if self.finished else None

    def fetch_results(self, _handle: SchedulerHandle, local_dir: Path) -> None:
        self.fetched_into.append(local_dir)

    def cancel(self, _handle: SchedulerHandle) -> None:
        pass


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
        "enabled = true\n\n"
        "[quotas]\n"
    )
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(config_dir))
    monkeypatch.setattr(cgroup, "systemd_run_on_path", lambda: True)
    monkeypatch.setattr(cgroup, "available", lambda: False)
    monkeypatch.setattr(cgroup, "scope_exists", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(cgroup, "stop_scope", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        cgroup,
        "wrap_command",
        lambda command, **_kwargs: list(command),
    )
    monkeypatch.setattr(daemon_module, "_gid_for_uid", lambda _uid: 1)
    monkeypatch.setattr(
        daemon_module,
        "_chown_tree",
        lambda _root, _uid, _gid: None,
    )
    monkeypatch.setattr(
        throttle,
        "apply_persistent_throttle_if_set",
        lambda *_args, **_kwargs: None,
    )
    daemon = Daemon(
        max_cpus=8,
        max_jobs=8,
        max_scheduler_jobs=8,
        max_mem_mb=100_000,
        poll_interval=0.05,
        multi_user=True,
        queue_dir=tmp_path / "unused-queue",
        jobs_dir=tmp_path / "unused-jobs",
    )
    try:
        yield daemon
    finally:
        daemon._close_running_logs()
        daemon._queue_lock_fd.close()


def _write_spec(
    uid: str,
    job_id: str,
    *,
    scheduler: bool,
) -> tuple[JobSpec, Path, Path]:
    workspace = paths.user_workspace_dir(uid, job_id)
    workspace.mkdir(parents=True, exist_ok=True)
    spec_path = paths.user_spec_path(uid, job_id)
    spec_path.parent.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(
        id=job_id,
        command=["true"],
        cwd=str(workspace),
        cpus=1,
        state=JobState.PENDING,
        submitter=uid,
        submitted_at=_SUBMITTED_AT,
        scheduler_target="host_f" if scheduler else None,
    )
    spec.write(spec_path)
    return spec, spec_path, workspace


def _assert_ambiguous_bare_lookup(job_id: str) -> None:
    try:
        resolved = paths.resolve_spec_path(job_id, multi_user=True)
    except Exception as exc:
        message = str(exc).lower()
        assert job_id in message
        assert "ambiguous" in message or "multiple" in message
    else:
        pytest.fail(
            f"bare lookup for duplicate {job_id!r} selected {resolved} instead "
            "of failing ambiguous"
        )


def test_unique_discovery_and_bare_lookup_remain_unchanged(
    multi_user_daemon: Daemon,
) -> None:
    spec, spec_path, _workspace = _write_spec(
        _UID_A,
        _UNIQUE_ID,
        scheduler=True,
    )

    assert list(multi_user_daemon._iter_specs()) == [spec]
    assert multi_user_daemon._job_uid == {_UNIQUE_ID: _UID_A}
    assert paths.resolve_spec_path(_UNIQUE_ID, multi_user=True) == spec_path
    assert paths.resolve_spec_path(
        _UNIQUE_ID,
        multi_user=True,
        uid=_UID_A,
    ) == spec_path


def test_discovery_quarantines_both_duplicates_and_keeps_unique(
    multi_user_daemon: Daemon,
) -> None:
    _spec_a, path_a, _workspace_a = _write_spec(
        _UID_A,
        _COLLIDING_ID,
        scheduler=True,
    )
    _spec_b, path_b, _workspace_b = _write_spec(
        _UID_B,
        _COLLIDING_ID,
        scheduler=True,
    )
    unique, _unique_path, _unique_workspace = _write_spec(
        _UID_UNIQUE,
        _UNIQUE_ID,
        scheduler=True,
    )
    before_a = path_a.read_bytes()
    before_b = path_b.read_bytes()

    discovered = list(multi_user_daemon._iter_specs())

    assert discovered == [unique]
    assert _COLLIDING_ID not in multi_user_daemon._job_uid
    assert multi_user_daemon._job_uid == {_UNIQUE_ID: _UID_UNIQUE}
    assert path_a.read_bytes() == before_a
    assert path_b.read_bytes() == before_b


def test_dispatch_freezes_both_duplicates_and_runs_unrelated_unique(
    multi_user_daemon: Daemon,
) -> None:
    _spec_a, path_a, _workspace_a = _write_spec(
        _UID_A,
        _COLLIDING_ID,
        scheduler=True,
    )
    _spec_b, path_b, _workspace_b = _write_spec(
        _UID_B,
        _COLLIDING_ID,
        scheduler=True,
    )
    _unique, unique_path, _unique_workspace = _write_spec(
        _UID_UNIQUE,
        _UNIQUE_ID,
        scheduler=True,
    )
    before_a = path_a.read_bytes()
    before_b = path_b.read_bytes()
    recorder = _SchedulerRecorder()
    multi_user_daemon._scheduler_dispatchers["host_f"] = recorder  # type: ignore[assignment]

    multi_user_daemon._dispatch_pending()

    assert recorder.submitted == [_UNIQUE_ID]
    assert set(multi_user_daemon._scheduler_running) == {_UNIQUE_ID}
    assert JobSpec.read(unique_path).state == JobState.RUNNING
    assert path_a.read_bytes() == before_a
    assert path_b.read_bytes() == before_b
    assert JobSpec.read(path_a).state == JobState.PENDING
    assert JobSpec.read(path_b).state == JobState.PENDING


def test_public_bare_lookup_fails_ambiguous_without_mutating_either_owner(
    multi_user_daemon: Daemon,
) -> None:
    _ = multi_user_daemon
    _spec_a, path_a, _workspace_a = _write_spec(
        _UID_A,
        _COLLIDING_ID,
        scheduler=False,
    )
    _spec_b, path_b, _workspace_b = _write_spec(
        _UID_B,
        _COLLIDING_ID,
        scheduler=False,
    )
    before_a = path_a.read_bytes()
    before_b = path_b.read_bytes()

    assert paths.resolve_spec_path(
        _COLLIDING_ID,
        multi_user=True,
        uid=_UID_A,
    ) == path_a
    assert paths.resolve_spec_path(
        _COLLIDING_ID,
        multi_user=True,
        uid=_UID_B,
    ) == path_b
    _assert_ambiguous_bare_lookup(_COLLIDING_ID)

    assert path_a.read_bytes() == before_a
    assert path_b.read_bytes() == before_b


def test_local_reconcile_keeps_admission_owner_after_late_collision(
    multi_user_daemon: Daemon,
) -> None:
    spec_a, path_a, _workspace_a = _write_spec(
        _UID_A,
        _COLLIDING_ID,
        scheduler=False,
    )
    assert list(multi_user_daemon._iter_specs()) == [spec_a]
    dispatcher = _LocalDispatcher()
    multi_user_daemon.dispatcher = dispatcher  # type: ignore[assignment]
    assert multi_user_daemon._start_job(spec_a) is True
    running = multi_user_daemon._running[_COLLIDING_ID]
    assert running.owner_uid == _UID_A
    assert running.spec_path == path_a
    assert JobSpec.read(path_a).state == JobState.RUNNING

    _spec_b, path_b, _workspace_b = _write_spec(
        _UID_B,
        _COLLIDING_ID,
        scheduler=False,
    )
    before_b = path_b.read_bytes()
    list(multi_user_daemon._iter_specs())
    dispatcher.return_code = 0

    multi_user_daemon._reconcile_running()

    finished_a = JobSpec.read(path_a)
    frozen_b = JobSpec.read(path_b)
    assert (
        finished_a.state,
        finished_a.exit_code,
        frozen_b.state,
        frozen_b.exit_code,
    ) == (JobState.COMPLETED, 0, JobState.PENDING, None)
    assert path_b.read_bytes() == before_b
    assert _COLLIDING_ID not in multi_user_daemon._running


def test_local_claim_rejects_raced_inner_id_change(
    multi_user_daemon: Daemon,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec, spec_path, _workspace = _write_spec(
        _UID_A,
        _COLLIDING_ID,
        scheduler=False,
    )
    assert list(multi_user_daemon._iter_specs()) == [spec]
    dispatcher = _LocalDispatcher()
    multi_user_daemon.dispatcher = dispatcher  # type: ignore[assignment]
    raced = False

    def rewrite_inner_id(_root: Path, _uid: int, _gid: int) -> None:
        nonlocal raced
        if raced:
            return
        raced = True
        mutable = JobSpec.read(spec_path)
        mutable.id = _UNIQUE_ID
        mutable.write(spec_path)

    # _chown_tree sits after the initial root-safety validation and before the
    # locked PENDING claim, reproducing a user rewrite during local setup.
    monkeypatch.setattr(daemon_module, "_chown_tree", rewrite_inner_id)

    assert multi_user_daemon._start_job(spec) is False

    persisted = JobSpec.read(spec_path)
    assert persisted.id == _COLLIDING_ID
    assert persisted.state == JobState.FAILED
    assert persisted.failure_reason is not None
    assert "does not match admitted job" in persisted.failure_reason
    assert multi_user_daemon._running == {}
    assert not paths.user_spec_path(_UID_A, _UNIQUE_ID).exists()


def test_scheduler_reconcile_keeps_admission_owner_after_late_collision(
    multi_user_daemon: Daemon,
) -> None:
    spec_a, path_a, workspace_a = _write_spec(
        _UID_A,
        _COLLIDING_ID,
        scheduler=True,
    )
    assert list(multi_user_daemon._iter_specs()) == [spec_a]
    recorder = _SchedulerRecorder()
    multi_user_daemon._scheduler_dispatchers["host_f"] = recorder  # type: ignore[assignment]
    assert multi_user_daemon._start_job(spec_a) is True
    running = multi_user_daemon._scheduler_running[_COLLIDING_ID]
    assert running.owner_uid == _UID_A
    assert running.spec_path == path_a
    assert JobSpec.read(path_a).state == JobState.RUNNING

    _spec_b, path_b, _workspace_b = _write_spec(
        _UID_B,
        _COLLIDING_ID,
        scheduler=True,
    )
    before_b = path_b.read_bytes()
    # A normal dispatch scan observes the late collision.  It must quarantine
    # the new record rather than submitting it under the already-active bare ID.
    multi_user_daemon._dispatch_pending()
    recorder.finished = True

    multi_user_daemon._reconcile_scheduler()

    finished_a = JobSpec.read(path_a)
    frozen_b = JobSpec.read(path_b)
    assert (
        finished_a.state,
        finished_a.exit_code,
        frozen_b.state,
        frozen_b.exit_code,
    ) == (JobState.COMPLETED, 0, JobState.PENDING, None)
    assert recorder.submitted == [_COLLIDING_ID]
    assert recorder.fetched_into == [workspace_a]
    assert path_b.read_bytes() == before_b
    assert _COLLIDING_ID not in multi_user_daemon._scheduler_running
