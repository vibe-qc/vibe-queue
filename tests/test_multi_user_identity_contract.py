"""Multi-user job identity remains separated across per-UID state trees."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from vq import cgroup, config, paths
from vq.daemon import Daemon
from vq.scheduler_dispatch import SchedulerHandle
from vq.spec import JobSpec, JobState

_UID_A = "1000"
_UID_B = "2000"
_JOB_A = "a1b2c3d4e5f6"
_JOB_B = "0f1e2d3c4b5a"


class _SchedulerRecorder:
    def __init__(self) -> None:
        self.submitted: list[tuple[str, Path]] = []

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
        self.submitted.append((job_id, local_workspace))
        return SchedulerHandle(
            job_id=f"pbs-{job_id}",
            remote_workspace=f"/remote/{job_id}",
        )


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
        "default_max_pending_jobs = 1\n"
    )
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(config_dir))
    monkeypatch.setattr(cgroup, "systemd_run_on_path", lambda: True)
    monkeypatch.setattr(cgroup, "available", lambda: False)
    daemon = Daemon(
        max_cpus=4,
        max_jobs=4,
        max_scheduler_jobs=4,
        poll_interval=0.05,
        multi_user=True,
        queue_dir=tmp_path / "queue",
        jobs_dir=tmp_path / "jobs",
    )
    try:
        yield daemon
    finally:
        daemon._queue_lock_fd.close()


def _write_scheduler_spec(uid: str, job_id: str) -> tuple[JobSpec, Path]:
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
        scheduler_target="host_f",
    )
    spec.write(spec_path)
    return spec, spec_path


def test_distinct_ids_bind_to_their_owning_uid_tree(
    multi_user_daemon: Daemon,
) -> None:
    spec_a, path_a = _write_scheduler_spec(_UID_A, _JOB_A)
    spec_b, path_b = _write_scheduler_spec(_UID_B, _JOB_B)

    discovered = {spec.id: spec for spec in multi_user_daemon._iter_specs()}

    assert discovered == {_JOB_A: spec_a, _JOB_B: spec_b}
    assert multi_user_daemon._job_uid == {_JOB_A: _UID_A, _JOB_B: _UID_B}
    assert multi_user_daemon._spec_path(_JOB_A) == path_a
    assert multi_user_daemon._spec_path(_JOB_B) == path_b
    assert paths.resolve_spec_path(_JOB_A, multi_user=True) == path_a
    assert paths.resolve_spec_path(_JOB_B, multi_user=True) == path_b
    assert multi_user_daemon._validate_multi_user_spec(discovered[_JOB_A]) is None
    assert multi_user_daemon._validate_multi_user_spec(discovered[_JOB_B]) is None


def test_distinct_ids_dispatch_and_track_per_uid(
    multi_user_daemon: Daemon,
) -> None:
    spec_a, path_a = _write_scheduler_spec(_UID_A, _JOB_A)
    spec_b, path_b = _write_scheduler_spec(_UID_B, _JOB_B)
    recorder = _SchedulerRecorder()
    multi_user_daemon._scheduler_dispatchers["host_f"] = recorder  # type: ignore[assignment]

    multi_user_daemon._dispatch_pending()

    assert set(recorder.submitted) == {
        (_JOB_A, Path(spec_a.cwd)),
        (_JOB_B, Path(spec_b.cwd)),
    }
    assert set(multi_user_daemon._scheduler_running) == {_JOB_A, _JOB_B}
    assert multi_user_daemon._job_uid == {_JOB_A: _UID_A, _JOB_B: _UID_B}
    on_disk_a = JobSpec.read(path_a)
    on_disk_b = JobSpec.read(path_b)
    assert (on_disk_a.state, on_disk_a.scheduler_job_id) == (
        JobState.RUNNING,
        f"pbs-{_JOB_A}",
    )
    assert (on_disk_b.state, on_disk_b.scheduler_job_id) == (
        JobState.RUNNING,
        f"pbs-{_JOB_B}",
    )
