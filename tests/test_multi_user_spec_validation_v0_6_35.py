"""v0.6.35: the multi-user daemon vets a spec before acting on it.

In multi-user mode each user **owns** their
``/var/lib/vq/users/<uid>/queue/`` directory, so they can drop a
hand-crafted spec JSON straight in — bypassing ``vq submit``
entirely. Before this fix ``_start_job`` (which runs as **root**)
trusted three attacker-controlled fields:

* ``submitter`` — used to pick the uid the job runs as. Forge
  ``"0"`` and the job runs as root: full privilege escalation.
* ``cwd`` — the workspace the daemon ``chown``s recursively. Point
  it at ``/etc`` and the daemon chowns it to the attacker.
* ``stdout_path`` / ``stderr_path`` — joined onto ``cwd`` and
  opened ``"ab"`` as root. An absolute / ``../`` path escapes and
  creates a root-owned file anywhere.

``_validate_multi_user_spec`` binds the dispatch to the **trusted**
uid -- the per-user state directory the spec was read from, which a
user cannot forge (``users/`` is root-owned) -- and rejects declared
workspace/log paths outside that user's real ``jobs/`` tree. Dispatch
still uses ordinary pathnames after validation; that later use is not
claimed to be race-free confinement.
"""
from __future__ import annotations

import contextlib
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from vq import cgroup, config, events, paths
from vq.daemon import MULTI_USER_SPEC_MAX_BYTES, Daemon
from vq.scheduler_dispatch import SchedulerHandle, SchedulerPhase
from vq.spec import JobSpec, JobState


def _make_mu_daemon(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Daemon:
    """A multi-user Daemon rooted at a tmp state tree."""
    monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "mu"))
    cfgdir = tmp_path / "cfg"
    cfgdir.mkdir()
    (cfgdir / "config.toml").write_text(
        "[multi_user]\nenabled = true\n\n[quotas]\n"
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


def _spec(
    uid: int,
    jobid: str,
    *,
    submitter: str | None,
    cwd: str,
    stdout_path: str = "stdout.log",
    stderr_path: str = "stderr.log",
) -> JobSpec:
    return JobSpec(
        id=jobid,
        command=["true"],
        cwd=cwd,
        cpus=1,
        state=JobState.PENDING,
        submitter=submitter,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
    )


def _honest_cwd(uid: int, jobid: str) -> str:
    """The workspace path an honest `vq submit` would record."""
    return str(paths.user_workspace_dir(uid, jobid))


class _SchedulerRecorder:
    def __init__(self) -> None:
        self.local_workspaces: list[Path] = []

    def remote_workspace(self, job_id: str) -> str:
        return f"/remote/{job_id}"

    def submit(
        self, *, job_id: str, local_workspace: Path | None, **_kwargs: object
    ) -> SchedulerHandle:
        assert local_workspace is not None
        self.local_workspaces.append(local_workspace)
        return SchedulerHandle(
            job_id="555.cluster", remote_workspace=f"/remote/{job_id}"
        )

    def poll(
        self, handles: list[SchedulerHandle]
    ) -> dict[str, SchedulerPhase]:
        return {handle.job_id: SchedulerPhase.FINISHED for handle in handles}


def _persist_honest_scheduler_spec(
    daemon: Daemon, uid: int, job_id: str
) -> tuple[JobSpec, Path, Path]:
    workspace = Path(_honest_cwd(uid, job_id))
    workspace.mkdir(parents=True)
    spec = _spec(
        uid,
        job_id,
        submitter=str(uid),
        cwd=str(workspace),
    )
    spec.scheduler_target = "host_f"
    daemon._job_uid[job_id] = str(uid)
    spec_path = daemon._spec_path(job_id)
    spec_path.parent.mkdir(parents=True, exist_ok=True)
    spec.write(spec_path)
    return spec, spec_path, workspace


class TestValidatorAcceptsHonestSpecs:
    def test_honest_spec_passes(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            paths.user_jobs_dir(uid).mkdir(parents=True)
            spec = _spec(
                uid, "job-1",
                submitter=str(uid), cwd=_honest_cwd(uid, "job-1"),
            )
            d._job_uid["job-1"] = str(uid)
            assert d._validate_multi_user_spec(spec) is None
        finally:
            d._queue_lock_fd.close()

    def test_resume_sibling_cwd_in_jobs_tree_passes(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # An auto-resume sibling keeps the PARENT's workspace, so its
        # cwd is jobs/<parent-jobid> while its own id differs. The
        # gate confines to the jobs tree, not an exact jobid, so this
        # legitimate case still passes.
        uid = os.getuid()
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            paths.user_jobs_dir(uid).mkdir(parents=True)
            spec = _spec(
                uid, "sibling-2",
                submitter=str(uid), cwd=_honest_cwd(uid, "parent-1"),
            )
            d._job_uid["sibling-2"] = str(uid)
            assert d._validate_multi_user_spec(spec) is None
        finally:
            d._queue_lock_fd.close()


class TestValidatorRejectsForgedSubmitter:
    def test_forged_submitter_root_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The spec lives in a non-root user's dir but claims
        # submitter=0 — the privilege-escalation payload.
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            spec = _spec(
                4242, "job-1",
                submitter="0", cwd=_honest_cwd(4242, "job-1"),
            )
            d._job_uid["job-1"] = "4242"
            reason = d._validate_multi_user_spec(spec)
            assert reason is not None
            assert "submitter=0" in reason and "4242" in reason
        finally:
            d._queue_lock_fd.close()

    def test_forged_submitter_other_user_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            spec = _spec(
                4242, "job-1",
                submitter="4343", cwd=_honest_cwd(4242, "job-1"),
            )
            d._job_uid["job-1"] = "4242"
            assert d._validate_multi_user_spec(spec) is not None
        finally:
            d._queue_lock_fd.close()

    def test_missing_submitter_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            spec = _spec(
                4242, "job-1",
                submitter=None, cwd=_honest_cwd(4242, "job-1"),
            )
            d._job_uid["job-1"] = "4242"
            assert d._validate_multi_user_spec(spec) is not None
        finally:
            d._queue_lock_fd.close()

    def test_unknown_owning_dir_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # No _job_uid entry → the daemon cannot prove who owns it.
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            spec = _spec(
                4242, "job-1",
                submitter="4242", cwd=_honest_cwd(4242, "job-1"),
            )
            assert d._validate_multi_user_spec(spec) is not None
        finally:
            d._queue_lock_fd.close()


class TestValidatorRejectsPathEscapes:
    def test_cwd_outside_jobs_tree_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # cwd=/etc — the daemon would chown -R /etc to the attacker.
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            spec = _spec(4242, "job-1", submitter="4242", cwd="/etc")
            d._job_uid["job-1"] = "4242"
            reason = d._validate_multi_user_spec(spec)
            assert reason is not None
            assert "outside" in reason
        finally:
            d._queue_lock_fd.close()

    def test_symlinked_jobs_root_cannot_redefine_the_trusted_boundary(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            outside = tmp_path / "outside-jobs"
            (outside / "job-1").mkdir(parents=True)
            jobs_root = paths.user_jobs_dir(uid)
            jobs_root.parent.mkdir(parents=True)
            jobs_root.symlink_to(outside, target_is_directory=True)
            spec = _spec(
                uid,
                "job-1",
                submitter=str(uid),
                cwd=str(outside / "job-1"),
            )
            d._job_uid[spec.id] = str(uid)

            reason = d._validate_multi_user_spec(spec)

            assert reason is not None
            assert "not a real directory" in reason
        finally:
            d._queue_lock_fd.close()

    def test_cwd_other_users_tree_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # uid 4242 points cwd into uid 4343's jobs tree.
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            spec = _spec(
                4242, "job-1",
                submitter="4242", cwd=_honest_cwd(4343, "job-1"),
            )
            d._job_uid["job-1"] = "4242"
            assert d._validate_multi_user_spec(spec) is not None
        finally:
            d._queue_lock_fd.close()

    def test_stdout_path_relative_escape_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            spec = _spec(
                4242, "job-1",
                submitter="4242", cwd=_honest_cwd(4242, "job-1"),
                stdout_path="../../../../../../etc/cron.d/vq-pwn",
            )
            d._job_uid["job-1"] = "4242"
            reason = d._validate_multi_user_spec(spec)
            assert reason is not None
            assert "stdout_path" in reason
        finally:
            d._queue_lock_fd.close()

    def test_stderr_path_absolute_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            spec = _spec(
                4242, "job-1",
                submitter="4242", cwd=_honest_cwd(4242, "job-1"),
                stderr_path="/etc/passwd",
            )
            d._job_uid["job-1"] = "4242"
            reason = d._validate_multi_user_spec(spec)
            assert reason is not None
            assert "stderr_path" in reason
        finally:
            d._queue_lock_fd.close()


class TestDiscoveryBindsFilenameToInnerId:
    def test_matching_legacy_safe_id_is_admitted_from_observed_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            jobid = "legacy-job_1.2"
            spec = _spec(
                uid,
                jobid,
                submitter=str(uid),
                cwd=_honest_cwd(uid, jobid),
            )
            spec_path = paths.user_queue_dir(uid) / f"{jobid}.json"
            spec_path.parent.mkdir(parents=True, exist_ok=True)
            spec.write(spec_path)

            assert list(d._iter_specs()) == [spec]
            assert d._spec_path(jobid) == spec_path
        finally:
            d._queue_lock_fd.close()

    def test_safe_inner_id_mismatch_is_quarantined_without_rewrite(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            stored_id = "stored-id"
            inner_id = "different-id"
            spec = _spec(
                uid,
                inner_id,
                submitter=str(uid),
                cwd=_honest_cwd(uid, inner_id),
            )
            stored_path = paths.user_queue_dir(uid) / f"{stored_id}.json"
            stored_path.parent.mkdir(parents=True, exist_ok=True)
            spec.write(stored_path)
            before = stored_path.read_bytes()
            reconstructed = paths.user_spec_path(uid, inner_id)

            assert list(d._iter_specs()) == []
            d._dispatch_pending()

            assert stored_path.read_bytes() == before
            assert not reconstructed.exists()
            assert inner_id not in d._job_uid
        finally:
            d._queue_lock_fd.close()


class TestDiscoveryRejectsUnsafeQueueEntries:
    def test_symlink_is_not_followed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            queue_dir = paths.user_queue_dir(uid)
            queue_dir.mkdir(parents=True)
            outside = tmp_path / "outside.json"
            outside_spec = _spec(
                uid,
                "linked",
                submitter=str(uid),
                cwd=_honest_cwd(uid, "linked"),
            )
            outside_spec.write(outside)
            (queue_dir / "linked.json").symlink_to(outside)

            assert list(d._iter_specs()) == []
            assert outside.read_bytes() == outside_spec.to_json().encode()
        finally:
            d._queue_lock_fd.close()

    def test_fifo_is_rejected_without_blocking(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            queue_dir = paths.user_queue_dir(uid)
            queue_dir.mkdir(parents=True)
            os.mkfifo(queue_dir / "fifo.json")

            assert list(d._iter_specs()) == []
        finally:
            d._queue_lock_fd.close()

    def test_oversized_regular_file_is_rejected_without_reading_payload(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            queue_dir = paths.user_queue_dir(uid)
            queue_dir.mkdir(parents=True)
            oversized = queue_dir / "oversized.json"
            oversized.touch()
            os.truncate(oversized, MULTI_USER_SPEC_MAX_BYTES + 1)

            assert list(d._iter_specs()) == []
        finally:
            d._queue_lock_fd.close()

    def test_invalid_utf8_is_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            queue_dir = paths.user_queue_dir(uid)
            queue_dir.mkdir(parents=True)
            (queue_dir / "invalid.json").write_bytes(b"\xff")

            assert list(d._iter_specs()) == []
        finally:
            d._queue_lock_fd.close()


class TestAdmissionGatePrecedesOrchestration:
    def test_forged_refresh_spec_cannot_create_build_or_emit_events(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            evil_cwd = tmp_path / "outside-jobs"
            spec = _spec(
                uid,
                "forged-refresh",
                submitter=str(uid + 1),
                cwd=str(evil_cwd),
            )
            spec.refresh_before = "vibeqc-dev"
            spec_path = paths.user_spec_path(uid, spec.id)
            spec_path.parent.mkdir(parents=True)
            spec.write(spec_path)
            orchestration_calls: list[str] = []
            monkeypatch.setattr(
                d,
                "_maybe_refresh_before_run",
                lambda *_args: orchestration_calls.append("refresh") or True,
            )
            monkeypatch.setattr(
                d,
                "_create_build_job",
                lambda *_args: orchestration_calls.append("build") or "build-id",
            )
            event_calls: list[Path] = []
            monkeypatch.setattr(
                events,
                "state_transition",
                lambda workspace, *_args, **_kwargs: event_calls.append(Path(workspace)),
            )
            monkeypatch.setattr(
                events,
                "append_event",
                lambda workspace, *_args, **_kwargs: event_calls.append(Path(workspace)),
            )

            d._dispatch_pending()

            rejected = JobSpec.read(spec_path)
            assert rejected.state == JobState.FAILED
            assert rejected.failure_reason is not None
            assert "multi-user spec gate" in rejected.failure_reason
            assert orchestration_calls == []
            assert event_calls == []
            assert not evil_cwd.exists()
        finally:
            d._queue_lock_fd.close()

    def test_forged_dependent_cwd_cannot_reach_cascade_event_writer(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            predecessor = _spec(
                uid,
                "failed-parent",
                submitter=str(uid),
                cwd=_honest_cwd(uid, "failed-parent"),
            )
            predecessor.state = JobState.FAILED
            Path(predecessor.cwd).mkdir(parents=True)
            evil_cwd = tmp_path / "cascade-events-outside"
            dependent = _spec(
                uid,
                "forged-dependent",
                submitter=str(uid),
                cwd=str(evil_cwd),
            )
            dependent.depends_on = [predecessor.id]
            queue_dir = paths.user_queue_dir(uid)
            queue_dir.mkdir(parents=True)
            predecessor.write(queue_dir / f"{predecessor.id}.json")
            dependent_path = queue_dir / f"{dependent.id}.json"
            dependent.write(dependent_path)
            event_calls: list[Path] = []
            monkeypatch.setattr(
                events,
                "state_transition",
                lambda workspace, *_args, **_kwargs: event_calls.append(Path(workspace)),
            )

            d._dispatch_pending()

            rejected = JobSpec.read(dependent_path)
            assert rejected.state == JobState.FAILED
            assert rejected.failure_reason is not None
            assert "multi-user spec gate" in rejected.failure_reason
            assert "predecessor" not in rejected.failure_reason
            assert event_calls == []
            assert not evil_cwd.exists()
        finally:
            d._queue_lock_fd.close()


class TestActiveSpecIdentityIsImmutable:
    def _persist_pair(
        self,
        daemon: Daemon,
        uid: int,
    ) -> tuple[JobSpec, Path, JobSpec, Path]:
        source = _spec(
            uid,
            "active-a",
            submitter=str(uid),
            cwd=_honest_cwd(uid, "active-a"),
        )
        source.state = JobState.RUNNING
        victim = _spec(
            uid,
            "victim-b",
            submitter=str(uid),
            cwd=_honest_cwd(uid, "victim-b"),
        )
        for item in (source, victim):
            Path(item.cwd).mkdir(parents=True)
            path = paths.user_spec_path(uid, item.id)
            path.parent.mkdir(parents=True, exist_ok=True)
            item.write(path)
        assert {item.id for item in daemon._iter_specs()} == {
            source.id,
            victim.id,
        }
        return (
            source,
            paths.user_spec_path(uid, source.id),
            victim,
            paths.user_spec_path(uid, victim.id),
        )

    def test_inner_id_edit_cannot_redirect_retry_write(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            source, source_path, victim, victim_path = self._persist_pair(d, uid)
            source.retry_max = 1
            source.id = victim.id
            source.write(source_path)
            victim_before = victim_path.read_bytes()
            d._running["active-a"] = SimpleNamespace(
                owner_uid=str(uid),
                spec_path=source_path,
            )

            d._record_finish("active-a", 1)

            retried = JobSpec.read(source_path)
            assert retried.id == "active-a"
            assert retried.state == JobState.PENDING
            assert retried.retry_count == 1
            assert victim_path.read_bytes() == victim_before
        finally:
            d._queue_lock_fd.close()

    def test_inner_id_edit_cannot_redirect_scope_or_terminal_write(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            source, source_path, victim, victim_path = self._persist_pair(d, uid)
            source.id = victim.id
            source.write(source_path)
            victim_before = victim_path.read_bytes()
            d._running["active-a"] = SimpleNamespace(
                owner_uid=str(uid),
                spec_path=source_path,
            )
            reaped: list[str] = []
            monkeypatch.setattr(d, "_reap_scope", reaped.append)

            d._record_finish("active-a", 0)

            finished = JobSpec.read(source_path)
            assert finished.id == "active-a"
            assert finished.state == JobState.COMPLETED
            assert reaped == ["active-a"]
            assert victim_path.read_bytes() == victim_before
        finally:
            d._queue_lock_fd.close()

    def test_active_symlink_replacement_is_not_followed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            _source, source_path, _victim, victim_path = self._persist_pair(d, uid)
            victim_before = victim_path.read_bytes()
            source_path.unlink()
            source_path.symlink_to(victim_path)
            runtime = SimpleNamespace(owner_uid=str(uid), spec_path=source_path)

            with pytest.raises(OSError):
                d._read_active_spec("active-a", runtime)

            assert victim_path.read_bytes() == victim_before
        finally:
            d._queue_lock_fd.close()

    def test_active_fifo_replacement_is_rejected_without_blocking(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            _source, source_path, _victim, _victim_path = self._persist_pair(d, uid)
            source_path.unlink()
            os.mkfifo(source_path)
            runtime = SimpleNamespace(owner_uid=str(uid), spec_path=source_path)

            with pytest.raises(ValueError, match="not a regular file"):
                d._read_active_spec("active-a", runtime)
        finally:
            d._queue_lock_fd.close()

    def test_startup_abort_inner_id_edit_cannot_delete_victim_workdir(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            source, source_path, victim, victim_path = self._persist_pair(d, uid)
            victim_workdir = paths.user_workdir(uid, victim.id)
            victim_workdir.mkdir(parents=True)
            sentinel = victim_workdir / "keep.txt"
            sentinel.write_text("keep", encoding="utf-8")
            victim_before = victim_path.read_bytes()
            attacker_record = source.model_copy(deep=True)
            attacker_record.id = victim.id
            attacker_record.clean_workdir_on_terminal = True
            attacker_record.workdir = str(victim_workdir)
            attacker_record.write(source_path)

            # Startup callers historically omit the explicit admitted jobid.
            d._mark_aborted_by_queue(
                source,
                reason="startup could not reattach",
                process_exit_confirmed=True,
            )

            aborted = JobSpec.read(source_path)
            assert aborted.id == source.id
            assert aborted.state == JobState.ABORTED_BY_QUEUE
            assert sentinel.read_text(encoding="utf-8") == "keep"
            assert victim_path.read_bytes() == victim_before
        finally:
            d._queue_lock_fd.close()


class TestStartJobEnforcesTheGate:
    def test_start_job_rejects_forged_spec_without_touching_fs(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """_start_job must reject a forged spec BEFORE its first
        filesystem op — mkdir/open/chown all run as root there."""
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            evil_target = tmp_path / "evil-target"
            spec = _spec(
                4242, "job-1",
                submitter="0", cwd=str(evil_target),
            )
            d._job_uid["job-1"] = "4242"
            started = d._start_job(spec)
            assert started is False
            assert spec.state == JobState.FAILED
            # The gate ran before workspace.mkdir — the forged cwd
            # was never created.
            assert not evil_target.exists()
            # Nothing landed in _running.
            assert "job-1" not in d._running
        finally:
            for rj in d._running.values():
                with contextlib.suppress(Exception):
                    rj.popen.kill()
            d._queue_lock_fd.close()


class TestSchedulerStartEnforcesTheGate:
    def test_honest_scheduler_spec_stages_validated_workspace(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            workspace = Path(_honest_cwd(uid, "sched-1"))
            workspace.mkdir(parents=True)
            spec = _spec(
                uid,
                "sched-1",
                submitter=str(uid),
                cwd=str(workspace),
            )
            spec.scheduler_target = "host_f"
            d._job_uid[spec.id] = str(uid)
            spec_path = d._spec_path(spec.id)
            spec_path.parent.mkdir(parents=True, exist_ok=True)
            spec.write(spec_path)
            recorder = _SchedulerRecorder()
            d._scheduler_dispatchers["host_f"] = recorder  # type: ignore[assignment]

            assert d._start_job(spec) is True

            assert recorder.local_workspaces == [workspace]
            on_disk = JobSpec.read(spec_path)
            assert on_disk.state == JobState.RUNNING
            assert on_disk.scheduler_job_id == "555.cluster"
        finally:
            d._queue_lock_fd.close()

    def test_initially_forged_scheduler_workspace_is_never_staged(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            evil_target = tmp_path / "outside-user-jobs"
            spec = _spec(
                uid,
                "sched-1",
                submitter=str(uid),
                cwd=str(evil_target),
            )
            spec.scheduler_target = "host_f"
            d._job_uid[spec.id] = str(uid)
            spec_path = d._spec_path(spec.id)
            spec_path.parent.mkdir(parents=True, exist_ok=True)
            spec.write(spec_path)
            recorder = _SchedulerRecorder()
            d._scheduler_dispatchers["host_f"] = recorder  # type: ignore[assignment]

            assert d._start_job(spec) is False

            assert recorder.local_workspaces == []
            assert not evil_target.exists()
            on_disk = JobSpec.read(spec_path)
            assert on_disk.state == JobState.FAILED
            assert on_disk.failure_reason is not None
            assert "multi-user spec gate" in on_disk.failure_reason
        finally:
            d._queue_lock_fd.close()

    def test_workspace_mutated_after_initial_gate_is_never_staged(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            workspace = Path(_honest_cwd(uid, "sched-1"))
            workspace.mkdir(parents=True)
            spec = _spec(
                uid,
                "sched-1",
                submitter=str(uid),
                cwd=str(workspace),
            )
            spec.scheduler_target = "host_f"
            d._job_uid[spec.id] = str(uid)
            spec_path = d._spec_path(spec.id)
            spec_path.parent.mkdir(parents=True, exist_ok=True)
            spec.write(spec_path)
            recorder = _SchedulerRecorder()
            evil_target = tmp_path / "outside-user-jobs"

            def mutate_before_claim(target: str) -> _SchedulerRecorder:
                assert target == "host_f"
                with paths.spec_lock(spec_path):
                    raced = JobSpec.read(spec_path)
                    raced.cwd = str(evil_target)
                    raced.write(spec_path)
                return recorder

            monkeypatch.setattr(
                d, "_scheduler_dispatcher_for", mutate_before_claim
            )

            assert d._start_job(spec) is False

            assert recorder.local_workspaces == []
            assert spec.id not in d._scheduler_running
            assert not evil_target.exists()
            on_disk = JobSpec.read(spec_path)
            assert on_disk.state == JobState.FAILED
            assert on_disk.scheduler_job_id is None
            assert on_disk.failure_reason is not None
            assert "multi-user spec gate" in on_disk.failure_reason
        finally:
            d._queue_lock_fd.close()

    def test_submitter_mutated_after_initial_gate_is_never_submitted(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            workspace = Path(_honest_cwd(uid, "sched-1"))
            workspace.mkdir(parents=True)
            spec = _spec(
                uid,
                "sched-1",
                submitter=str(uid),
                cwd=str(workspace),
            )
            spec.scheduler_target = "host_f"
            d._job_uid[spec.id] = str(uid)
            spec_path = d._spec_path(spec.id)
            spec_path.parent.mkdir(parents=True, exist_ok=True)
            spec.write(spec_path)
            recorder = _SchedulerRecorder()

            def mutate_before_claim(target: str) -> _SchedulerRecorder:
                assert target == "host_f"
                with paths.spec_lock(spec_path):
                    raced = JobSpec.read(spec_path)
                    raced.submitter = str(uid + 1)
                    raced.write(spec_path)
                return recorder

            monkeypatch.setattr(
                d, "_scheduler_dispatcher_for", mutate_before_claim
            )

            assert d._start_job(spec) is False

            assert recorder.local_workspaces == []
            assert spec.id not in d._scheduler_running
            on_disk = JobSpec.read(spec_path)
            assert on_disk.state == JobState.FAILED
            assert on_disk.scheduler_job_id is None
            assert on_disk.failure_reason is not None
            assert "multi-user spec gate" in on_disk.failure_reason
        finally:
            d._queue_lock_fd.close()

    def test_target_mutated_before_claim_is_deferred_to_next_scan(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            workspace = Path(_honest_cwd(uid, "sched-1"))
            workspace.mkdir(parents=True)
            spec = _spec(
                uid,
                "sched-1",
                submitter=str(uid),
                cwd=str(workspace),
            )
            spec.scheduler_target = "host_f"
            d._job_uid[spec.id] = str(uid)
            spec_path = d._spec_path(spec.id)
            spec_path.parent.mkdir(parents=True, exist_ok=True)
            spec.write(spec_path)
            recorder = _SchedulerRecorder()

            def mutate_before_claim(target: str) -> _SchedulerRecorder:
                assert target == "host_f"
                with paths.spec_lock(spec_path):
                    raced = JobSpec.read(spec_path)
                    raced.scheduler_target = "host_c"
                    raced.write(spec_path)
                return recorder

            monkeypatch.setattr(
                d, "_scheduler_dispatcher_for", mutate_before_claim
            )

            assert d._start_job(spec) is False

            assert recorder.local_workspaces == []
            assert spec.id not in d._scheduler_running
            on_disk = JobSpec.read(spec_path)
            assert on_disk.state == JobState.PENDING
            assert on_disk.scheduler_target == "host_c"
            assert on_disk.scheduler_job_id is None
        finally:
            d._queue_lock_fd.close()

    def test_id_mutated_after_initial_gate_cannot_redirect_locked_write(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            workspace = Path(_honest_cwd(uid, "sched-1"))
            workspace.mkdir(parents=True)
            spec = _spec(
                uid,
                "sched-1",
                submitter=str(uid),
                cwd=str(workspace),
            )
            spec.scheduler_target = "host_f"
            d._job_uid[spec.id] = str(uid)
            spec_path = d._spec_path(spec.id)
            spec_path.parent.mkdir(parents=True, exist_ok=True)
            spec.write(spec_path)

            victim_workspace = Path(_honest_cwd(uid, "sched-victim"))
            victim = _spec(
                uid,
                "sched-victim",
                submitter=str(uid),
                cwd=str(victim_workspace),
            )
            victim.scheduler_target = "host_f"
            d._job_uid[victim.id] = str(uid)
            victim_path = d._spec_path(victim.id)
            victim.write(victim_path)
            recorder = _SchedulerRecorder()
            recorder.max_wall_time_seconds = 28_800  # type: ignore[attr-defined]

            def mutate_before_claim(target: str) -> _SchedulerRecorder:
                assert target == "host_f"
                with paths.spec_lock(spec_path):
                    raced = JobSpec.read(spec_path)
                    raced.id = victim.id
                    raced.wall_time_seconds = 43_200
                    raced.write(spec_path)
                return recorder

            monkeypatch.setattr(
                d, "_scheduler_dispatcher_for", mutate_before_claim
            )

            assert d._start_job(spec) is False

            assert recorder.local_workspaces == []
            assert spec.id not in d._scheduler_running
            source_on_disk = JobSpec.read(spec_path)
            assert source_on_disk.id == spec.id
            assert source_on_disk.state == JobState.FAILED
            assert source_on_disk.scheduler_job_id is None
            assert source_on_disk.failure_reason is not None
            assert "multi-user spec gate" in source_on_disk.failure_reason
            assert "allows at most" not in source_on_disk.failure_reason
            assert events.read_events(workspace) == []
            victim_on_disk = JobSpec.read(victim_path)
            assert victim_on_disk.id == victim.id
            assert victim_on_disk.state == JobState.PENDING
            assert victim_on_disk.cwd == str(victim_workspace)
        finally:
            d._queue_lock_fd.close()

    def test_phase_two_restores_admitted_snapshot_and_stable_identity(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            spec, spec_path, workspace = _persist_honest_scheduler_spec(
                d, uid, "sched-1"
            )
            victim, victim_path, victim_workspace = (
                _persist_honest_scheduler_spec(d, uid, "sched-victim")
            )
            recorder = _SchedulerRecorder()
            d._scheduler_dispatchers["host_f"] = recorder  # type: ignore[assignment]
            evil_target = tmp_path / "outside-user-jobs"
            event_workspaces: list[Path] = []
            monkeypatch.setattr(
                events,
                "append_event",
                lambda event_workspace, *_args, **_kwargs: (
                    event_workspaces.append(Path(event_workspace))
                ),
            )
            monkeypatch.setattr(
                events,
                "state_transition",
                lambda event_workspace, *_args, **_kwargs: (
                    event_workspaces.append(Path(event_workspace))
                ),
            )
            original_submit = recorder.submit

            def mutate_during_submit(**kwargs: object) -> SchedulerHandle:
                handle = original_submit(**kwargs)  # type: ignore[arg-type]
                with paths.spec_lock(spec_path):
                    raced = JobSpec.read(spec_path)
                    raced.id = victim.id
                    raced.cwd = str(evil_target)
                    raced.scheduler_target = "host_c"
                    raced.state = JobState.PENDING
                    raced.command = ["false"]
                    raced.cpus = 7
                    raced.write(spec_path)
                return handle

            recorder.submit = mutate_during_submit  # type: ignore[assignment]

            assert d._start_job(spec) is True

            assert event_workspaces == [workspace, workspace]
            assert not evil_target.exists()
            assert set(d._scheduler_running) == {spec.id}
            on_disk = JobSpec.read(spec_path)
            assert on_disk.id == spec.id
            assert on_disk.cwd == str(workspace)
            assert on_disk.scheduler_target == "host_f"
            assert on_disk.state == JobState.RUNNING
            assert on_disk.command == ["true"]
            assert on_disk.cpus == 1
            assert on_disk.scheduler_job_id == "555.cluster"
            victim_on_disk = JobSpec.read(victim_path)
            assert victim_on_disk.id == victim.id
            assert victim_on_disk.state == JobState.PENDING
            assert victim_on_disk.cwd == str(victim_workspace)
        finally:
            d._queue_lock_fd.close()

    def test_phase_two_preserves_terminal_kill_without_redirecting_writes(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = _make_mu_daemon(tmp_path, monkeypatch)
        try:
            spec, spec_path, workspace = _persist_honest_scheduler_spec(
                d, uid, "sched-1"
            )
            victim, victim_path, victim_workspace = (
                _persist_honest_scheduler_spec(d, uid, "sched-victim")
            )
            recorder = _SchedulerRecorder()
            cancelled: list[str] = []
            recorder.cancel = (  # type: ignore[attr-defined]
                lambda handle: cancelled.append(handle.job_id)
            )
            d._scheduler_dispatchers["host_f"] = recorder  # type: ignore[assignment]
            evil_target = tmp_path / "outside-user-jobs"
            original_submit = recorder.submit
            finished_at = "2026-08-02T12:01:00+00:00"

            def mutate_during_submit(**kwargs: object) -> SchedulerHandle:
                handle = original_submit(**kwargs)  # type: ignore[arg-type]
                with paths.spec_lock(spec_path):
                    raced = JobSpec.read(spec_path)
                    raced.id = victim.id
                    raced.cwd = str(evil_target)
                    raced.scheduler_target = "host_c"
                    raced.state = JobState.KILLED
                    raced.finished_at = finished_at
                    raced.failure_reason = "operator killed during qsub"
                    raced.write(spec_path)
                return handle

            recorder.submit = mutate_during_submit  # type: ignore[assignment]

            assert d._start_job(spec) is False

            assert cancelled == ["555.cluster"]
            assert not evil_target.exists()
            assert spec.id not in d._scheduler_running
            on_disk = JobSpec.read(spec_path)
            assert on_disk.id == spec.id
            assert on_disk.cwd == str(workspace)
            assert on_disk.scheduler_target == "host_f"
            assert on_disk.state == JobState.KILLED
            assert on_disk.finished_at == finished_at
            assert on_disk.failure_reason == "operator killed during qsub"
            assert on_disk.scheduler_job_id == "555.cluster"
            victim_on_disk = JobSpec.read(victim_path)
            assert victim_on_disk.id == victim.id
            assert victim_on_disk.state == JobState.PENDING
            assert victim_on_disk.cwd == str(victim_workspace)
        finally:
            d._queue_lock_fd.close()
