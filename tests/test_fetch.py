"""Tests for vq.fetch: local copy, tar-emitter, remote streaming (mocked)."""
from __future__ import annotations

import io
import json
import os
import subprocess
import tarfile
from datetime import datetime
from pathlib import Path

import pytest

from vq import config, fetch, ownership, paths, transport
from vq.config import HostConfig
from vq.scheduler_dispatch import SchedulerHandle
from vq.spec import JobSpec, JobState


@pytest.fixture
def state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    return tmp_path


def _materialize_job(state_root: Path, jobid: str, files: dict[str, str]) -> JobSpec:
    """Helper: write a spec + workspace dir + files, return the spec."""
    queue = paths.queue_dir()
    jobs = paths.jobs_dir()
    queue.mkdir(parents=True, exist_ok=True)
    jobs.mkdir(parents=True, exist_ok=True)
    workspace = jobs / jobid
    workspace.mkdir()
    for name, content in files.items():
        (workspace / name).write_text(content)
    spec = JobSpec(
        id=jobid,
        command=["echo", "hi"],
        cwd=str(workspace),
        cpus=1,
        submitter="test_user@test",
    )
    spec.write(queue / f"{jobid}.json")
    return spec


def _materialize_multi_user_job(
    uid: str,
    jobid: str,
    *,
    workspace_files: dict[str, str],
    workdir_files: dict[str, str] | None = None,
    job_name: str | None = None,
) -> JobSpec:
    """Helper: write a multi-user spec under users/<uid>/queue."""
    queue = paths.user_queue_dir(uid)
    workspace = paths.user_workspace_dir(uid, jobid)
    queue.mkdir(parents=True, exist_ok=True)
    workspace.mkdir(parents=True, exist_ok=True)
    for name, content in workspace_files.items():
        path = workspace / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    workdir: Path | None = None
    if workdir_files is not None:
        workdir = paths.user_workdir(uid, jobid)
        workdir.mkdir(parents=True, exist_ok=True)
        for name, content in workdir_files.items():
            path = workdir / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
    spec = JobSpec(
        id=jobid,
        command=["echo", "hi"],
        cwd=str(workspace),
        cpus=1,
        submitter=uid,
        job_name=job_name,
        workdir=str(workdir) if workdir is not None else None,
    )
    spec.write(paths.user_spec_path(uid, jobid))
    return spec


def _enable_multi_user_config(
    state_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfgdir = state_root / "cfg"
    cfgdir.mkdir(parents=True, exist_ok=True)
    (cfgdir / "config.toml").write_text("[multi_user]\nenabled = true\n")
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfgdir))
    monkeypatch.setattr(
        config, "SYSTEM_CONFIG_PATH", state_root / "absent-system.toml"
    )


class _FakeSchedulerDispatcher:
    def __init__(self, filename: str = "live.out", content: str = "remote live\n") -> None:
        self.filename = filename
        self.content = content
        self.fetches: list[tuple[str, str, Path]] = []
        self.handles: list[SchedulerHandle] = []

    def remote_workspace(self, jobid: str) -> str:
        return f"/remote/{jobid}"

    def fetch_results(self, handle: SchedulerHandle, local_dir: Path) -> None:
        self.handles.append(handle)
        self.fetches.append((handle.job_id, handle.remote_workspace, local_dir))
        local_dir.mkdir(parents=True, exist_ok=True)
        (local_dir / self.filename).write_text(self.content)


def _scheduler_config() -> config.Config:
    return config.Config(
        hosts={
            "host_f": HostConfig(
                ssh="host_f.invalid",
                scheduler="pbs",
                scheduler_dialect="torque",
                scratch_root="/home/USER",
                scheduler_driver="localhost",
            )
        }
    )


class TestFetchLocal:
    def test_copies_workspace_to_output_dir(self, state: Path) -> None:
        _materialize_job(state, "deadbeef0123", {"out.txt": "hello", "data.npz": "x"})
        out = state / "fetched"
        dst = fetch.fetch_local("deadbeef0123", out)
        assert dst == out / "deadbeef0123"
        assert (dst / "out.txt").read_text() == "hello"
        assert (dst / "data.npz").read_text() == "x"

    def test_missing_jobid_raises(self, state: Path) -> None:
        with pytest.raises(FileNotFoundError, match="no such job"):
            fetch.fetch_local("nonexistent1", state / "out")

    def test_existing_destination_raises(self, state: Path) -> None:
        _materialize_job(state, "abcd00000000", {"x.txt": ""})
        out = state / "fetched"
        out.mkdir()
        (out / "abcd00000000").mkdir()
        with pytest.raises(FileExistsError, match="destination already exists"):
            fetch.fetch_local("abcd00000000", out)

    def test_same_job_destination_is_refreshed(self, state: Path) -> None:
        """(#114): a re-fetch REFRESHES the destination.

        This test previously asserted the opposite — that a second fetch keeps
        whatever is already in the destination. That contract is what made
        ``vq fetch`` print ``fetched -> ...`` over a frozen snapshot while the
        job kept running, nearly reporting a converged run as hung. A
        destination the operator hand-edited is not the case worth protecting;
        a destination that silently misreports the job's output is.
        """
        spec = _materialize_job(state, "repeat000001", {"out.txt": "original"})
        out = state / "fetched"
        first = fetch.fetch_local("repeat000001", out)
        (first / "out.txt").write_text("locally edited")
        (Path(spec.cwd) / "out.txt").write_text("advanced")

        second = fetch.fetch_local("repeat000001", out)

        assert second == first
        assert (second / "out.txt").read_text() == "advanced"

    def test_other_job_sidecar_does_not_make_destination_idempotent(
        self, state: Path
    ) -> None:
        _materialize_job(state, "repeat000002", {"out.txt": "source"})
        dst = state / "fetched" / "repeat000002"
        (dst / "_vq").mkdir(parents=True)
        (dst / fetch.TERMINAL_DIAGNOSIS_SIDECAR).write_text(
            json.dumps(
                {
                    "schema": "vq.terminal-diagnosis.v1",
                    "jobid": "different-job",
                }
            )
        )

        with pytest.raises(FileExistsError, match="destination already exists"):
            fetch.fetch_local("repeat000002", state / "fetched")

    def test_same_job_sidecar_behind_symlink_is_not_trusted(
        self, state: Path
    ) -> None:
        _materialize_job(state, "repeat000003", {"out.txt": "source"})
        target = state / "elsewhere"
        (target / "_vq").mkdir(parents=True)
        (target / fetch.TERMINAL_DIAGNOSIS_SIDECAR).write_text(
            json.dumps(
                {
                    "schema": "vq.terminal-diagnosis.v1",
                    "jobid": "repeat000003",
                }
            )
        )
        out = state / "fetched"
        out.mkdir()
        (out / "repeat000003").symlink_to(target, target_is_directory=True)

        with pytest.raises(FileExistsError, match="destination already exists"):
            fetch.fetch_local("repeat000003", out)

    def test_workspace_missing_on_disk_raises(self, state: Path) -> None:
        # Spec exists but cwd doesn't (e.g. workspace was rm'd manually)
        _materialize_job(state, "ffffffffffff", {"x.txt": ""})
        spec = JobSpec.read(paths.queue_dir() / "ffffffffffff.json")
        import shutil
        shutil.rmtree(spec.cwd)
        with pytest.raises(FileNotFoundError, match="workspace.*not found"):
            fetch.fetch_local("ffffffffffff", state / "out")

    def test_terminal_fetch_stamps_last_fetched_at(self, state: Path) -> None:
        from vq.spec import JobState
        _materialize_job(state, "term00000000", {"out.txt": "ok"})
        spec_path = paths.queue_dir() / "term00000000.json"
        spec = JobSpec.read(spec_path)
        spec.state = JobState.COMPLETED
        spec.finished_at = "2026-05-02T10:00:00+00:00"
        spec.exit_code = 0
        spec.write(spec_path)
        assert JobSpec.read(spec_path).last_fetched_at is None
        fetch.fetch_local("term00000000", state / "fetched")
        assert JobSpec.read(spec_path).last_fetched_at is not None

    def test_fetch_local_writes_terminal_diagnosis_sidecar(
        self, state: Path
    ) -> None:
        from vq.spec import JobState

        _materialize_job(state, "diagfetch001", {"out.txt": "partial\n"})
        spec_path = paths.queue_dir() / "diagfetch001.json"
        spec = JobSpec.read(spec_path)
        spec.state = JobState.FAILED
        spec.exit_code = 137
        spec.failure_reason = "Killed: 9"
        spec.write(spec_path)

        dst = fetch.fetch_local("diagfetch001", state / "fetched")
        payload = json.loads(
            (dst / "_vq" / "terminal-diagnosis.json").read_text()
        )

        assert payload["schema"] == "vq.terminal-diagnosis.v1"
        assert payload["jobid"] == "diagfetch001"
        assert payload["state"] == "failed"
        assert payload["exit_code"] == 137
        assert payload["terminal_diagnosis"]["category"] == "sigkill"
        assert (
            payload["terminal_diagnosis"]["action_hint"]
            == "increase_memory_or_check_external_kill"
        )

    def test_archived_job_fetched_via_archive_untar(self, state: Path) -> None:
        from vq.cleanup import archive_workspace
        from vq.spec import JobState
        _materialize_job(state, "arc000000000", {"out.txt": "archived\n"})
        spec_path = paths.queue_dir() / "arc000000000.json"
        spec = JobSpec.read(spec_path)
        spec.state = JobState.COMPLETED
        spec.finished_at = "2026-05-02T10:00:00+00:00"
        spec.exit_code = 0
        spec.write(spec_path)
        # Archive it; workspace dir is removed.
        archive_workspace(JobSpec.read(spec_path))
        assert not Path(spec.cwd).exists()

        dst = fetch.fetch_local("arc000000000", state / "fetched")

        assert (dst / "out.txt").read_text() == "archived\n"

    def test_live_scheduler_job_stages_remote_workspace_before_copy(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        spec = _materialize_job(state, "livefetch001", {"live.out": "stale\n"})
        spec.scheduler_target = "host_f"
        spec.scheduler_job_id = "555.cluster"
        spec.state = JobState.RUNNING
        spec.scheduler_state = "poll_failed"
        spec.array_index = 2
        spec.array_total = 5
        spec.array_group_id = "fetch-array"
        spec.write(paths.spec_path("livefetch001"))
        fake = _FakeSchedulerDispatcher()
        monkeypatch.setattr(fetch.config, "load_config", _scheduler_config)
        monkeypatch.setattr(fetch, "scheduler_dispatcher_for", lambda host_cfg: fake)

        dst = fetch.fetch_local("livefetch001", state / "fetched")

        assert (dst / "live.out").read_text() == "remote live\n"
        assert fake.fetches == [
            ("555.cluster", "/remote/livefetch001", Path(spec.cwd))
        ]
        assert [
            (handle.job_id, handle.remote_workspace, handle.array_size)
            for handle in fake.handles
        ] == [("555.cluster", "/remote/livefetch001", None)]
        manifest = fetch.read_fetch_manifest(dst)
        assert manifest is not None
        assert manifest["jobid"] == "livefetch001"
        assert manifest["source_host"] == "local"
        assert manifest["source_kind"] == "workspace"
        assert manifest["source_path"] == spec.cwd
        assert manifest["transport"] == "local-copy"
        assert manifest["stale"] is False
        assert JobSpec.read(paths.spec_path("livefetch001")).last_fetched_at is None

    def test_live_scheduler_refresh_with_no_id_skips_dispatcher(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        spec = _materialize_job(state, "livefetch002", {"live.out": "stale\n"})
        spec.scheduler_target = "host_f"
        spec.scheduler_job_id = None
        monkeypatch.setattr(
            fetch.config,
            "load_config",
            lambda: pytest.fail("missing scheduler id must skip dispatcher setup"),
        )

        fetch._refresh_live_scheduler_workspace(spec)

    def test_live_scheduler_refresh_preserves_empty_scheduler_id(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        spec = _materialize_job(state, "livefetch003", {"live.out": "stale\n"})
        spec.scheduler_target = "host_f"
        spec.scheduler_job_id = ""
        fake = _FakeSchedulerDispatcher()
        monkeypatch.setattr(fetch.config, "load_config", _scheduler_config)
        monkeypatch.setattr(fetch, "scheduler_dispatcher_for", lambda host_cfg: fake)

        fetch._refresh_live_scheduler_workspace(spec)

        assert [
            (handle.job_id, handle.remote_workspace, handle.array_size)
            for handle in fake.handles
        ] == [("", "/remote/livefetch003", None)]
        assert fake.fetches == [("", "/remote/livefetch003", Path(spec.cwd))]


class TestEmitWorkspaceTar:
    def test_writes_tar_to_stdout(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _materialize_job(state, "aaaaaaaaaaaa", {"out.txt": "tar me"})
        buf = io.BytesIO()
        # sys.stdout.buffer is what emit_workspace_tar writes to.
        # Replace sys.stdout with an object that exposes a .buffer attr.
        class _FakeStdout:
            buffer = buf
        import vq.fetch as fetch_module
        monkeypatch.setattr(fetch_module.sys, "stdout", _FakeStdout())

        fetch.emit_workspace_tar("aaaaaaaaaaaa")

        buf.seek(0)
        with tarfile.open(fileobj=buf, mode="r|") as tf:
            names = tf.getnames()
        # Top-level entry is the jobid; out.txt lives under it
        assert "aaaaaaaaaaaa" in names
        assert "aaaaaaaaaaaa/out.txt" in names

    def test_workspace_tar_includes_terminal_diagnosis_sidecar(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from vq.spec import JobState

        _materialize_job(state, "tarfailed001", {"out.txt": "partial\n"})
        spec_path = paths.queue_dir() / "tarfailed001.json"
        spec = JobSpec.read(spec_path)
        spec.state = JobState.TIME_EXCEEDED
        spec.scheduler_target = "host_f"
        spec.failure_reason = "PBS walltime exceeded"
        spec.write(spec_path)
        buf = io.BytesIO()

        class _FakeStdout:
            buffer = buf

        import vq.fetch as fetch_module

        monkeypatch.setattr(fetch_module.sys, "stdout", _FakeStdout())
        fetch.emit_workspace_tar("tarfailed001")

        buf.seek(0)
        with tarfile.open(fileobj=buf, mode="r:") as tf:
            member = tf.extractfile(
                "tarfailed001/_vq/terminal-diagnosis.json"
            )
            assert member is not None
            payload = json.loads(member.read().decode("utf-8"))
        assert payload["terminal_diagnosis"]["category"] == "scheduler_walltime"
        assert payload["terminal_diagnosis"]["action_hint"] == "increase_walltime"

    def test_missing_jobid_raises(self, state: Path) -> None:
        with pytest.raises(FileNotFoundError, match="no such job"):
            fetch.emit_workspace_tar("nope12345678")

    @pytest.mark.parametrize(
        "emitter",
        ["workspace", "artifact", "workdir", "workdir-artifact"],
    )
    def test_multi_user_emitters_refuse_foreign_job_before_tar_output(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
        emitter: str,
    ) -> None:
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(state / "multi-user"))
        _enable_multi_user_config(state, monkeypatch)
        _materialize_multi_user_job(
            "2002",
            "foreign-tar-1",
            workspace_files={"result.dat": "private result\n"},
            workdir_files={"scratch.dat": "private scratch\n"},
        )
        monkeypatch.setattr(ownership, "_caller_uid", lambda: 1001)
        monkeypatch.setattr(ownership, "_caller_is_admin", lambda _cfg: False)
        buf = io.BytesIO()

        class _FakeStdout:
            buffer = buf

        monkeypatch.setattr(fetch.sys, "stdout", _FakeStdout())

        with pytest.raises(ownership.OwnershipError):
            if emitter == "workspace":
                fetch.emit_workspace_tar("foreign-tar-1", multi_user=True)
            elif emitter in {"artifact", "workdir-artifact"}:
                fetch.emit_artifact_tar(
                    "foreign-tar-1", "result.dat", multi_user=True,
                    workdir=emitter == "workdir-artifact",
                )
            else:
                fetch.emit_workdir_tar("foreign-tar-1", multi_user=True)

        assert buf.getvalue() == b""

    def test_multi_user_submitter_numeric_streams_workspace(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(state / "multi-user"))
        _enable_multi_user_config(state, monkeypatch)
        _materialize_multi_user_job(
            str(os.geteuid()),
            "marsfetch001",
            workspace_files={"out.txt": "host_a stdout\n"},
            job_name="vibeview-p04-bands",
        )
        buf = io.BytesIO()

        class _FakeStdout:
            buffer = buf

        import vq.fetch as fetch_module

        monkeypatch.setattr(fetch_module.sys, "stdout", _FakeStdout())

        fetch.emit_workspace_tar("marsfetch001", multi_user=True)

        buf.seek(0)
        with tarfile.open(fileobj=buf, mode="r|") as tf:
            names = tf.getnames()
        assert "vibeview-p04-bands-marsfetch001" in names
        assert "vibeview-p04-bands-marsfetch001/out.txt" in names

    def test_archived_job_streams_archive_bytes_to_stdout(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """v0.5.10.1 fix: emit_workspace_tar must read from spec.archive_path
        when the spec is archived, NOT error out because spec.cwd is gone.

        Pre-fix, `vq fetch <archived-jobid>` worked locally (fetch_local
        is archive-aware) but FAILED remotely (fetch_remote pipes through
        the tar-workspace verb which calls this function, and this function
        only knew about live workspaces). Repro on host_d 2026-05-10:
        `vq fetch c99c97cad7c3 -o /tmp/...` errored with "workspace for
        job c99c97cad7c3 not found at /home/USER/.local/share/vq/jobs/...".
        """
        from vq.cleanup import archive_workspace
        from vq.spec import JobState
        _materialize_job(state, "ar2000000000", {"deep.txt": "value\n"})
        spec_path = paths.queue_dir() / "ar2000000000.json"
        spec = JobSpec.read(spec_path)
        spec.state = JobState.COMPLETED
        spec.finished_at = "2026-05-02T10:00:00+00:00"
        spec.exit_code = 0
        spec.write(spec_path)
        archive_workspace(JobSpec.read(spec_path))
        # Workspace gone, archive present.
        assert not Path(spec.cwd).exists()
        spec = JobSpec.read(spec_path)
        assert spec.is_archived
        assert spec.archive_path is not None
        assert Path(spec.archive_path).is_file()

        # Now exercise the tar-workspace verb path.
        buf = io.BytesIO()
        class _FakeStdout:
            buffer = buf
        import vq.fetch as fetch_module
        monkeypatch.setattr(fetch_module.sys, "stdout", _FakeStdout())

        fetch.emit_workspace_tar("ar2000000000")

        # The emitted stream keeps the archived members and appends a fresh
        # diagnosis sidecar for fetched-artifact triage.
        buf.seek(0)
        payload: dict[str, dict[str, object]] = {}
        with tarfile.open(fileobj=buf, mode="r|*") as tf:
            names: list[str] = []
            for member in tf:
                names.append(member.name)
                if member.name.endswith("_vq/terminal-diagnosis.json"):
                    f = tf.extractfile(member)
                    assert f is not None
                    payload["diagnosis"] = json.loads(f.read().decode("utf-8"))
        assert "ar2000000000" in names
        assert "ar2000000000/deep.txt" in names
        assert "ar2000000000/_vq/terminal-diagnosis.json" in names
        assert payload["diagnosis"]["jobid"] == "ar2000000000"
        assert payload["diagnosis"]["terminal_diagnosis"]["category"] == "completed"

    def test_archived_job_with_missing_archive_file_raises(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If the spec says archived_at but the .tar.bz2 is gone, surface
        that to the user clearly rather than silently emitting an empty
        tar that the remote receiver would error on with a tar-parse
        message."""
        from vq.cleanup import archive_workspace
        from vq.spec import JobState
        _materialize_job(state, "ar3000000000", {"x.txt": "x\n"})
        spec_path = paths.queue_dir() / "ar3000000000.json"
        spec = JobSpec.read(spec_path)
        spec.state = JobState.COMPLETED
        spec.finished_at = "2026-05-02T10:00:00+00:00"
        spec.exit_code = 0
        spec.write(spec_path)
        archive_workspace(JobSpec.read(spec_path))
        spec = JobSpec.read(spec_path)
        # Simulate the archive file being deleted out from under us.
        Path(spec.archive_path).unlink()

        with pytest.raises(FileNotFoundError, match="archive for job .* not found"):
            fetch.emit_workspace_tar("ar3000000000")

    def test_live_scheduler_job_stages_remote_workspace_before_streaming(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        spec = _materialize_job(state, "tarfetch0001", {"live.out": "stale\n"})
        spec.scheduler_target = "host_f"
        spec.scheduler_job_id = "777.cluster"
        spec.write(paths.spec_path("tarfetch0001"))
        fake = _FakeSchedulerDispatcher()
        monkeypatch.setattr(fetch.config, "load_config", _scheduler_config)
        monkeypatch.setattr(fetch, "scheduler_dispatcher_for", lambda host_cfg: fake)

        buf = io.BytesIO()

        class _FakeStdout:
            buffer = buf

        import vq.fetch as fetch_module
        monkeypatch.setattr(fetch_module.sys, "stdout", _FakeStdout())

        fetch.emit_workspace_tar("tarfetch0001")

        buf.seek(0)
        payload: dict[str, str] = {}
        with tarfile.open(fileobj=buf, mode="r|") as tf:
            for member in tf:
                if not member.isfile():
                    continue
                f = tf.extractfile(member)
                assert f is not None
                payload[member.name] = f.read().decode()
        assert payload["tarfetch0001/live.out"] == "remote live\n"
        assert fake.fetches == [
            ("777.cluster", "/remote/tarfetch0001", Path(spec.cwd))
        ]


class TestFetchRemote:
    @pytest.fixture
    def host_cfg(self) -> HostConfig:
        return HostConfig(ssh="host_d", remote_vq="vq")

    def _build_tar_bytes(
        self,
        files: dict[str, str],
        top_dir: str,
        *,
        diagnosis: dict[str, object] | None = None,
    ) -> bytes:
        """Build a tar archive with the given files under top_dir/, return bytes."""
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w|") as tf:
            for name, content in files.items():
                data = content.encode()
                info = tarfile.TarInfo(f"{top_dir}/{name}")
                info.size = len(data)
                tf.addfile(info, io.BytesIO(data))
            # Also emit a directory entry so extractall handles things cleanly
            d = tarfile.TarInfo(top_dir)
            d.type = tarfile.DIRTYPE
            tf.addfile(d)
            if diagnosis is not None:
                data = json.dumps(diagnosis).encode()
                info = tarfile.TarInfo(
                    f"{top_dir}/{fetch.TERMINAL_DIAGNOSIS_SIDECAR}"
                )
                info.size = len(data)
                tf.addfile(info, io.BytesIO(data))
        return buf.getvalue()

    @staticmethod
    def _successful_stream(tar_bytes: bytes):  # noqa: ANN205
        class FakeProc:
            def __init__(self) -> None:
                self.stdout = io.BytesIO(tar_bytes)
                self.stderr = io.BytesIO(b"")
                self.returncode: int | None = None

            def wait(self) -> int:
                self.returncode = 0
                return 0

            def kill(self) -> None:
                self.returncode = -9

        return FakeProc()

    def _install_snapshot(
        self,
        monkeypatch: pytest.MonkeyPatch,
        *,
        jobid: str,
        state_name: str,
        content: str,
    ) -> str:
        submitted_at = "2026-08-20T10:11:12+00:00"
        tar_bytes = self._build_tar_bytes(
            {"result.out": content},
            top_dir=jobid,
            diagnosis={
                "schema": "vq.terminal-diagnosis.v1",
                "jobid": jobid,
                "submitted_at": submitted_at,
                "state": state_name,
            },
        )
        monkeypatch.setattr(
            transport.subprocess,
            "Popen",
            lambda *args, **kwargs: self._successful_stream(tar_bytes),
        )
        return submitted_at

    def test_terminal_remote_fetch_marks_back_only_after_promotion(
        self,
        host_cfg: HostConfig,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        jobid = "markback0001"
        submitted_at = self._install_snapshot(
            monkeypatch,
            jobid=jobid,
            state_name="completed",
            content="complete\n",
        )
        out = tmp_path / "out"
        calls: list[tuple[tuple[str, ...], dict[str, object]]] = []

        def acknowledge(
            unused_host: HostConfig,
            *args: str,
            **kwargs: object,
        ) -> subprocess.CompletedProcess[str]:
            dst = out / jobid
            assert (dst / "result.out").read_text() == "complete\n"
            manifest = fetch.read_fetch_manifest(dst)
            assert manifest is not None
            assert manifest["stale"] is False
            # The destination is user-raceable after promotion.  Corrupt its
            # diagnosis before recording the call: mark-back must still use
            # the terminal identity captured from the private staging tree.
            (dst / fetch.TERMINAL_DIAGNOSIS_SIDECAR).write_text(
                json.dumps(
                    {
                        "schema": "vq.terminal-diagnosis.v1",
                        "jobid": jobid,
                        "submitted_at": "forged-after-promotion",
                        "state": "failed",
                    }
                )
            )
            calls.append((args, kwargs))
            return subprocess.CompletedProcess(
                [], 0, stdout="2026-08-20T10:12:13+00:00\n", stderr=""
            )

        monkeypatch.setattr(transport, "run_remote_vq", acknowledge)

        dst = fetch.fetch_remote(host_cfg, jobid, out)

        assert dst == out / jobid
        assert calls == [
            (
                (
                    "mark-fetched",
                    jobid,
                    "--submitted-at",
                    submitted_at,
                    "--state",
                    "completed",
                ),
                {
                    "timeout": fetch.REMOTE_FETCH_ACK_TIMEOUT_SECONDS,
                    "retry_transient": 0,
                    "owned_process_group": True,
                    "max_stdout_bytes": fetch.REMOTE_FETCH_ACK_STDOUT_MAX_BYTES,
                    "max_stderr_bytes": fetch.REMOTE_FETCH_ACK_STDERR_MAX_BYTES,
                },
            )
        ]

    @pytest.mark.parametrize(
        ("returncode", "stderr", "succeeds"),
        [
            (2, "Usage: vq [OPTIONS] COMMAND [ARGS]...\n\n"
             "Error: No such command 'mark-fetched'.\n", True),
            (1, "Error: No such command 'mark-fetched'.\n", False),
            (255, "Error: No such command 'mark-fetched'.\n", False),
            (2, "Error: No such command 'other'.\n", False),
            (2, "Error: No such option: --submitted-at\n", False),
            (2, "Error: fetch acknowledgement identity no longer matches\n", False),
            (2, "Error: No such command 'mark-fetched'.\nPermission denied\n", False),
        ],
    )
    def test_legacy_mark_back_command_compatibility(
        self,
        host_cfg: HostConfig,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
        returncode: int,
        stderr: str,
        succeeds: bool,
    ) -> None:
        jobid = "legacyack001"
        self._install_snapshot(
            monkeypatch, jobid=jobid, state_name="completed", content="complete\n"
        )
        calls = []

        def legacy_remote(cmd, **kwargs):  # noqa: ANN001, ANN003, ANN202
            calls.append(cmd)
            assert (tmp_path / "out" / jobid / "result.out").read_text() == "complete\n"
            return subprocess.CompletedProcess(cmd, returncode, stdout="", stderr=stderr)

        # Exercise the real transport classifier, stubbing only its SSH process.
        monkeypatch.setattr(transport, "run_owned_subprocess", legacy_remote)
        if succeeds:
            fetch.fetch_remote(host_cfg, jobid, tmp_path / "out")
            assert "last_fetched_at was not recorded" in caplog.text
            assert "update the remote vq" in caplog.text
        else:
            with pytest.raises(transport.RemoteError):
                fetch.fetch_remote(host_cfg, jobid, tmp_path / "out")
            assert "last_fetched_at was not recorded" not in caplog.text
        assert len(calls) == 1
        manifest = fetch.read_fetch_manifest(tmp_path / "out" / jobid)
        assert manifest is not None and manifest["stale"] is False

    def test_live_remote_fetch_does_not_mark_back(
        self,
        host_cfg: HostConfig,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        jobid = "markback0002"
        self._install_snapshot(
            monkeypatch,
            jobid=jobid,
            state_name="running",
            content="still running\n",
        )
        monkeypatch.setattr(
            transport,
            "run_remote_vq",
            lambda *args, **kwargs: pytest.fail(
                "a live snapshot must not acknowledge a later terminal state"
            ),
        )

        dst = fetch.fetch_remote(host_cfg, jobid, tmp_path / "out")

        assert (dst / "result.out").read_text() == "still running\n"

    @pytest.mark.parametrize(
        ("ack_result", "error_type", "message"),
        [
            ("failure", transport.RemoteError, "workspace landed.*mark-back"),
            (
                "unknown",
                transport.RemoteOutcomeUnknown,
                "outcome is unknown.*tree remains fresh",
            ),
            ("invalid-receipt", transport.RemoteError, "invalid receipt"),
        ],
    )
    def test_remote_mark_back_errors_leave_promoted_tree_fresh(
        self,
        host_cfg: HostConfig,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        ack_result: str,
        error_type: type[transport.RemoteError],
        message: str,
    ) -> None:
        jobid = "markback0003"
        self._install_snapshot(
            monkeypatch,
            jobid=jobid,
            state_name="completed",
            content="complete\n",
        )

        def acknowledge(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
            if ack_result == "failure":
                raise transport.RemoteError("mark-back unavailable")
            if ack_result == "unknown":
                raise transport.RemoteOutcomeUnknown("ssh observer timed out")
            return subprocess.CompletedProcess(
                [], 0, stdout="not-a-timestamp\n", stderr=""
            )

        monkeypatch.setattr(
            transport,
            "run_remote_vq",
            acknowledge,
        )
        out = tmp_path / "out"

        with pytest.raises(error_type, match=message):
            fetch.fetch_remote(host_cfg, jobid, out)

        assert (out / jobid / "result.out").read_text() == "complete\n"
        manifest = fetch.read_fetch_manifest(out / jobid)
        assert manifest is not None
        assert manifest["stale"] is False

    def test_failure_before_promotion_never_marks_back(
        self,
        host_cfg: HostConfig,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        jobid = "markback0006"
        self._install_snapshot(
            monkeypatch,
            jobid=jobid,
            state_name="completed",
            content="complete\n",
        )
        monkeypatch.setattr(
            fetch,
            "_replace_directory",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                OSError("promotion failed")
            ),
        )
        monkeypatch.setattr(
            transport,
            "run_remote_vq",
            lambda *args, **kwargs: pytest.fail(
                "an unpromoted workspace must not acknowledge the ledger"
            ),
        )
        out = tmp_path / "out"

        with pytest.raises(OSError, match="promotion failed"):
            fetch.fetch_remote(host_cfg, jobid, out)

        assert not (out / jobid).exists()
        assert list(out.glob(".vq-fetch-*")) == []

    def test_diagnosis_free_remote_tar_lands_but_fails_protocol(
        self,
        host_cfg: HostConfig,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        tar_bytes = self._build_tar_bytes(
            {"out.txt": "remote hello", "data.bin": "x"},
            top_dir="abc123def456",
        )

        class FakeProc:
            def __init__(self) -> None:
                self.stdout = io.BytesIO(tar_bytes)
                self.stderr = io.BytesIO(b"")
                self.returncode: int | None = None
                self.killed = False

            def wait(self) -> int:
                self.returncode = 0
                return 0

            def kill(self) -> None:
                self.killed = True

            def communicate(self) -> tuple[bytes, bytes]:
                return b"", b""

        captured: list[list[str]] = []

        def fake_popen(cmd: list[str], **kwargs) -> FakeProc:
            captured.append(cmd)
            return FakeProc()

        monkeypatch.setattr(transport.subprocess, "Popen", fake_popen)

        out = tmp_path / "out"
        with pytest.raises(
            transport.RemoteError,
            match="workspace landed.*diagnosis.*update the remote vq",
        ):
            fetch.fetch_remote(host_cfg, "abc123def456", out)
        dst = out / "abc123def456"
        assert dst == out / "abc123def456"
        assert (dst / "out.txt").read_text() == "remote hello"
        assert (dst / "data.bin").read_text() == "x"

        # v0.8.17: the transport now goes through transport.stream_remote_vq
        # (_ssh_base + shlex.join), so the argv is the hardened ssh prefix,
        # the host, then the remote command as ONE shell-quoted string (so
        # the remote shell can't re-tokenise / word-split it).
        assert len(captured) == 1
        argv = captured[0]
        assert argv[0] == "ssh"
        assert "BatchMode=yes" in argv  # the hardened _ssh_base prefix
        assert argv[-2:] == ["host_d", "vq tar-workspace abc123def456"]

    def test_remote_failure_raises(
        self,
        host_cfg: HostConfig,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        class FakeProc:
            def __init__(self) -> None:
                self.stdout = io.BytesIO(b"")  # empty stream -> tar fail
                self.stderr = io.BytesIO(b"no such job: abc")
                self.returncode: int | None = None

            def wait(self) -> int:
                self.returncode = 1
                return 1

            def kill(self) -> None: pass
            def communicate(self) -> tuple[bytes, bytes]:
                return b"", b"no such job: abc"

        monkeypatch.setattr(transport.subprocess, "Popen", lambda *a, **k: FakeProc())

        with pytest.raises(transport.RemoteError):
            fetch.fetch_remote(host_cfg, "abc123def456", tmp_path / "out")

    def test_existing_destination_raises_after_first_tar_member(
        self,
        host_cfg: HostConfig,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """v0.5.34 trade-off: pre-v0.5.34 fetch_remote could pre-check
        ``output_dir / jobid`` without any SSH call because the dest
        dirname was knowable locally (it was just the jobid). With
        ``--job-name`` the dest dirname is ``<name>-<jobid>`` and the
        name only lives in the remote spec, so we can't pre-check
        without either (a) an extra SSH round-trip on every fetch, or
        (b) starting the tar stream, reading the first member to learn
        the actual top-level, and only THEN checking existence.

        We chose (b) — the common-case fast path stays SSH-once, and
        the collision case pays one round-trip we'd have paid anyway.
        Test asserts the new shape: Popen IS invoked (we open the
        stream) and the FileExistsError fires only after we've seen
        the first tar member."""
        out = tmp_path / "out"
        out.mkdir()
        (out / "abc123def456").mkdir()

        # Build a tarball whose first member is the dest top-level
        # entry (mimicking what the remote ``vq tar-workspace`` writes).
        import io
        import tarfile
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tf:
            info = tarfile.TarInfo("abc123def456")
            info.type = tarfile.DIRTYPE
            tf.addfile(info)
        buf.seek(0)

        class FakeProc:
            def __init__(self) -> None:
                self.stdout = buf
                self.stderr = io.BytesIO(b"")
                self.returncode: int | None = None
            def wait(self) -> int:
                self.returncode = 0
                return 0
            def kill(self) -> None: pass
            def communicate(self) -> tuple[bytes, bytes]:
                return b"", b""

        popen_called = False
        def fake_popen(*a, **k):
            nonlocal popen_called
            popen_called = True
            return FakeProc()

        monkeypatch.setattr(transport.subprocess, "Popen", fake_popen)
        with pytest.raises(FileExistsError):
            fetch.fetch_remote(host_cfg, "abc123def456", out)
        assert popen_called, (
            "v0.5.34: Popen IS invoked — the dest pre-check happens on "
            "the first tar member, after the SSH stream opens"
        )

    def test_same_job_destination_is_replaced_by_the_streamed_snapshot(
        self,
        host_cfg: HostConfig,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """(#114): the stream runs to completion and the previous
        snapshot is replaced.

        This test previously asserted that finding a same-job destination
        aborted the stream (``proc.killed``) and returned the OLD bytes as a
        success. That is the defect: on the rp218 canary it fed a watcher a
        stdout frozen at +60540 s for ~7.7 h while the job was demonstrably
        alive.
        """
        tar_bytes = self._build_tar_bytes(
            {"out.txt": "new remote result"},
            top_dir="repeatremote1",
            diagnosis={
                "schema": "vq.terminal-diagnosis.v1",
                "jobid": "repeatremote1",
                "submitted_at": "2026-08-20T10:11:12+00:00",
                "state": "running",
            },
        )
        out = tmp_path / "out"
        dst = out / "repeatremote1"
        (dst / "_vq").mkdir(parents=True)
        (dst / "out.txt").write_text("existing complete result")
        (dst / fetch.TERMINAL_DIAGNOSIS_SIDECAR).write_text(
            json.dumps(
                {
                    "schema": "vq.terminal-diagnosis.v1",
                    "jobid": "repeatremote1",
                }
            )
        )

        class FakeProc:
            def __init__(self) -> None:
                self.stdout = io.BytesIO(tar_bytes)
                self.stderr = io.BytesIO(b"")
                self.returncode: int | None = None
                self.killed = False

            def wait(self) -> int:
                self.returncode = -9 if self.killed else 0
                return self.returncode

            def kill(self) -> None:
                self.killed = True

        proc = FakeProc()
        monkeypatch.setattr(transport.subprocess, "Popen", lambda *a, **k: proc)

        result = fetch.fetch_remote(host_cfg, "repeatremote1", out)

        assert result == dst
        assert not proc.killed
        assert (dst / "out.txt").read_text() == "new remote result"
        assert list(out.glob(".vq-fetch-*")) == []
        assert list(out.glob(".vq-stale-*")) == []

    def test_concurrent_same_job_promote_race_publishes_this_fetch(
        self,
        host_cfg: HostConfig,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A concurrent fetch of the same job can publish between our
        first-member pre-check and our promote (ENOTEMPTY on macOS).

        (#114): the recovery is to replace it with THIS fetch's
        snapshot, which is at least as fresh, rather than to accept the other
        directory's contents as our result. Either way the race must not fail
        the fetch; what changed is which bytes the caller is handed.
        """
        tar_bytes = self._build_tar_bytes(
            {"out.txt": "this fetch"},
            top_dir="promoterace01",
            diagnosis={
                "schema": "vq.terminal-diagnosis.v1",
                "jobid": "promoterace01",
                "submitted_at": "2026-08-20T10:11:12+00:00",
                "state": "running",
            },
        )

        class FakeProc:
            def __init__(self) -> None:
                self.stdout = io.BytesIO(tar_bytes)
                self.stderr = io.BytesIO(b"")
                self.returncode: int | None = None

            def wait(self) -> int:
                self.returncode = 0
                return 0

            def kill(self) -> None:
                self.returncode = -9

        monkeypatch.setattr(
            transport.subprocess, "Popen", lambda *a, **k: FakeProc()
        )
        out = tmp_path / "out"
        dst = out / "promoterace01"

        real_replace = os.replace
        raced = {"done": False}

        def publish_concurrent_fetch(src, target):  # noqa: ANN001, ANN202
            if raced["done"]:
                return real_replace(src, target)
            raced["done"] = True
            assert Path(src).name == "promoterace01"
            assert Path(target) == dst
            target = Path(target)
            (target / "_vq").mkdir(parents=True)
            (target / "out.txt").write_text("concurrent complete result")
            (target / fetch.TERMINAL_DIAGNOSIS_SIDECAR).write_text(
                json.dumps(
                    {
                        "schema": "vq.terminal-diagnosis.v1",
                        "jobid": "promoterace01",
                    }
                )
            )
            raise OSError(66, "Directory not empty")

        monkeypatch.setattr(fetch.os, "replace", publish_concurrent_fetch)

        result = fetch.fetch_remote(host_cfg, "promoterace01", out)

        assert result == dst
        assert (dst / "out.txt").read_text() == "this fetch"
        assert list(out.glob(".vq-fetch-*")) == []
        assert list(out.glob(".vq-stale-*")) == []

    def test_failed_extraction_leaves_no_partial_destination(
        self,
        host_cfg: HostConfig,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """REMOTE-4: a mid-stream failure (truncated tar) must leave NO
        half-written ``output_dir/<dest>/`` for the next fetch / restore to
        trip over. Extraction goes through a hidden staging dir promoted by an
        atomic os.replace only on full success; on failure the staging dir is
        rmtree'd and no destination is created."""
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w|") as tf:
            d = tarfile.TarInfo("destdir")
            d.type = tarfile.DIRTYPE
            tf.addfile(d)
            data = b"x" * 8192
            info = tarfile.TarInfo("destdir/big.bin")
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
        # Keep the dir header + file header + only 1 KiB of big.bin's 8 KiB
        # body, so extracting the file runs off the end of the stream.
        truncated = buf.getvalue()[: 512 + 512 + 1024]

        class FakeProc:
            def __init__(self) -> None:
                self.stdout = io.BytesIO(truncated)
                self.stderr = io.BytesIO(b"remote tar: write error")
                self.returncode: int | None = None

            def wait(self) -> int:
                self.returncode = 1
                return 1

            def kill(self) -> None:
                pass

        monkeypatch.setattr(transport.subprocess, "Popen", lambda *a, **k: FakeProc())
        monkeypatch.setattr(
            transport,
            "run_remote_vq",
            lambda *args, **kwargs: pytest.fail(
                "a partial remote fetch must not acknowledge the ledger"
            ),
        )

        out = tmp_path / "out"
        with pytest.raises(transport.RemoteError):
            fetch.fetch_remote(host_cfg, "destdir", out)

        # No partial destination, and the staging dir is gone.
        assert not (out / "destdir").exists()
        assert list(out.glob(".vq-fetch-*")) == []


class TestFetchMarkBack:
    def test_exact_terminal_identity_is_idempotent_and_preserves_fresh_fields(
        self, state: Path
    ) -> None:
        from vq.spec import JobState

        jobid = "markspec0001"
        _materialize_job(state, jobid, {"result.out": "done\n"})
        spec_path = paths.spec_path(jobid)
        spec = JobSpec.read(spec_path)
        spec.state = JobState.COMPLETED
        spec.finished_at = "2026-08-20T10:13:00+00:00"
        spec.failure_reason = "preserve me"
        spec.write(spec_path)

        first = fetch.mark_terminal_fetch(
            jobid,
            submitted_at=spec.submitted_at,
            state="completed",
        )
        second = fetch.mark_terminal_fetch(
            jobid,
            submitted_at=spec.submitted_at,
            state="completed",
        )

        fresh = JobSpec.read(spec_path)
        assert fresh.last_fetched_at == second
        assert second >= first
        assert fresh.failure_reason == "preserve me"

    @pytest.mark.parametrize(
        ("submitted_at", "state_name"),
        [
            ("2026-08-20T00:00:00+00:00", "completed"),
            (None, "failed"),
        ],
    )
    def test_reused_or_changed_job_identity_is_rejected(
        self,
        state: Path,
        submitted_at: str | None,
        state_name: str,
    ) -> None:
        from vq.spec import JobState

        jobid = "markspec0002"
        _materialize_job(state, jobid, {"result.out": "done\n"})
        spec_path = paths.spec_path(jobid)
        spec = JobSpec.read(spec_path)
        spec.state = JobState.COMPLETED
        spec.finished_at = "2026-08-20T10:13:00+00:00"
        spec.write(spec_path)

        with pytest.raises(ValueError, match="fetch acknowledgement identity"):
            fetch.mark_terminal_fetch(
                jobid,
                submitted_at=submitted_at or spec.submitted_at,
                state=state_name,
            )

        assert JobSpec.read(spec_path).last_fetched_at is None

    def test_legacy_spec_without_stable_submitted_at_fails_closed(
        self, state: Path
    ) -> None:
        from vq.spec import JobState

        jobid = "marklegacy01"
        _materialize_job(state, jobid, {"result.out": "done\n"})
        spec_path = paths.spec_path(jobid)
        spec = JobSpec.read(spec_path)
        spec.state = JobState.COMPLETED
        spec.finished_at = "2026-08-20T10:13:00+00:00"
        spec.write(spec_path)
        raw = json.loads(spec_path.read_text())
        raw.pop("submitted_at")
        spec_path.write_text(json.dumps(raw))
        dst = fetch.fetch_local(jobid, state / "fetched")

        assert (dst / "result.out").read_text() == "done\n"
        assert json.loads(spec_path.read_text()).get("last_fetched_at") is None

    def test_replaced_spec_symlink_is_not_followed(
        self, state: Path
    ) -> None:
        from vq.spec import JobState

        jobid = "marksymlink1"
        _materialize_job(state, jobid, {"result.out": "done\n"})
        spec_path = paths.spec_path(jobid)
        spec = JobSpec.read(spec_path)
        spec.state = JobState.COMPLETED
        spec.finished_at = "2026-08-20T10:13:00+00:00"
        spec.write(spec_path)
        outside = state / "outside-spec.json"
        spec.write(outside)
        spec_path.unlink()
        spec_path.symlink_to(outside)

        with pytest.raises(ValueError, match="not a regular file"):
            fetch.mark_terminal_fetch(
                jobid,
                submitted_at=spec.submitted_at,
                state="completed",
                expected_path=spec_path,
            )

        assert JobSpec.read(outside).last_fetched_at is None


class TestFetchLocalWithJobName:
    """v0.5.34: when the spec has ``job_name`` set, fetch_local lands
    the workspace at ``<output_dir>/<name>-<jobid>/`` instead of
    ``<output_dir>/<jobid>/``."""

    def test_fetch_uses_name_prefixed_destination(self, state: Path) -> None:
        spec = _materialize_job(
            state, "deadbeef0123", {"out.txt": "named hello"}
        )
        spec.job_name = "mgo-pbe"
        spec.write(paths.spec_path("deadbeef0123"))

        out = state / "fetched"
        dst = fetch.fetch_local("deadbeef0123", out)
        assert dst == out / "mgo-pbe-deadbeef0123"
        assert (dst / "out.txt").read_text() == "named hello"

    def test_unnamed_fetch_falls_back_to_jobid_only(self, state: Path) -> None:
        """Pre-v0.5.34 behaviour preserved when no name is set."""
        _materialize_job(state, "noname000000", {"out.txt": "plain"})
        out = state / "fetched"
        dst = fetch.fetch_local("noname000000", out)
        assert dst == out / "noname000000"

    def test_archived_named_job_extracts_to_named_dest(
        self, state: Path
    ) -> None:
        """End-to-end: archive a named job, then fetch it — the
        un-tarred dest dir must use the name prefix."""
        from vq import cleanup
        from vq.spec import JobState
        spec = _materialize_job(state, "archivedaaaa", {"x.txt": "y"})
        spec.state = JobState.COMPLETED
        spec.finished_at = "2026-05-15T00:00:00+00:00"
        spec.exit_code = 0
        spec.job_name = "tagged"
        spec.write(paths.spec_path("archivedaaaa"))

        cleanup.archive_workspace(spec)
        spec_after = JobSpec.read(paths.spec_path("archivedaaaa"))
        assert spec_after.is_archived

        out = state / "fetched"
        dst = fetch.fetch_local("archivedaaaa", out)
        assert dst == out / "tagged-archivedaaaa"
        assert (dst / "x.txt").read_text() == "y"


class TestEmitWorkspaceTarWithJobName:
    """v0.5.34: ``emit_workspace_tar`` (the internal tar-workspace verb
    used by ``vq fetch HOST JOBID``) uses ``dest_dirname`` as arcname
    so the receiver lands at ``<output_dir>/<name>-<jobid>/``."""

    def test_arcname_includes_job_name(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        spec = _materialize_job(state, "streamtest12", {"o.log": "x"})
        spec.job_name = "myjob"
        spec.write(paths.spec_path("streamtest12"))

        buf = io.BytesIO()

        class FakeStdout:
            buffer = buf

        monkeypatch.setattr("sys.stdout", FakeStdout())
        fetch.emit_workspace_tar("streamtest12")

        buf.seek(0)
        with tarfile.open(fileobj=buf, mode="r|") as tf:
            names = [m.name for m in tf]
        # Every entry should be under "myjob-streamtest12/"
        assert any(n == "myjob-streamtest12" for n in names)
        for n in names:
            assert n.split("/", 1)[0] == "myjob-streamtest12"

    def test_arcname_unchanged_when_no_job_name(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Pre-v0.5.34 shape preserved."""
        _materialize_job(state, "plaintarball", {"o.log": "x"})

        buf = io.BytesIO()

        class FakeStdout:
            buffer = buf

        monkeypatch.setattr("sys.stdout", FakeStdout())
        fetch.emit_workspace_tar("plaintarball")

        buf.seek(0)
        with tarfile.open(fileobj=buf, mode="r|") as tf:
            names = [m.name for m in tf]
        for n in names:
            assert n.split("/", 1)[0] == "plaintarball"


# ----------------------------------------------------------------------
# v0.7.7 *Cerf's Datagram* — fetch_workdir_local / fetch_workdir_remote
# / emit_workdir_tar
# ----------------------------------------------------------------------


def _materialize_job_with_workdir(
    state_root: Path,
    jobid: str,
    workspace_files: dict[str, str],
    workdir_files: dict[str, str],
    *,
    job_name: str | None = None,
    clean_on_terminal: bool = False,
) -> JobSpec:
    """Same as _materialize_job but also drops a separate workdir
    next to the workspace so the spec's ``workdir`` field points at
    a real directory the workdir fetchers can read."""
    queue = paths.queue_dir()
    jobs = paths.jobs_dir()
    queue.mkdir(parents=True, exist_ok=True)
    jobs.mkdir(parents=True, exist_ok=True)
    workspace = jobs / jobid
    workspace.mkdir()
    for name, content in workspace_files.items():
        (workspace / name).write_text(content)
    workdir = state_root / "workdirs" / jobid
    workdir.mkdir(parents=True)
    for name, content in workdir_files.items():
        (workdir / name).write_text(content)
    spec = JobSpec(
        id=jobid,
        command=["echo", "hi"],
        cwd=str(workspace),
        cpus=1,
        submitter="test_user@test",
        job_name=job_name,
        workdir=str(workdir),
        clean_workdir_on_terminal=clean_on_terminal,
    )
    spec.write(queue / f"{jobid}.json")
    return spec


class TestFetchWorkdirLocal:
    def test_copies_workdir_to_output_dir(self, state: Path) -> None:
        _materialize_job_with_workdir(
            state,
            "wdir00000001",
            workspace_files={"input.py": "print()"},
            workdir_files={
                "scratch.bin": "binary",
                "intermediate.h5": "hdf5",
            },
        )
        out = state / "fetched"
        dst = fetch.fetch_workdir_local("wdir00000001", out)
        # Destination has "-workdir" suffix so workspace + workdir
        # fetches don't collide.
        assert dst == out / "wdir00000001-workdir"
        assert (dst / "scratch.bin").read_text() == "binary"
        assert (dst / "intermediate.h5").read_text() == "hdf5"

    def test_workdir_fetch_writes_terminal_diagnosis_sidecar(
        self, state: Path
    ) -> None:
        from vq.spec import JobState

        _materialize_job_with_workdir(
            state,
            "wdirdiag001",
            workspace_files={"input.py": "print()"},
            workdir_files={"scratch.bin": "partial"},
        )
        spec_path = paths.queue_dir() / "wdirdiag001.json"
        spec = JobSpec.read(spec_path)
        spec.state = JobState.FAILED
        spec.exit_code = 137
        spec.write(spec_path)

        dst = fetch.fetch_workdir_local("wdirdiag001", state / "fetched")
        payload = json.loads(
            (dst / "_vq" / "terminal-diagnosis.json").read_text()
        )

        assert payload["jobid"] == "wdirdiag001"
        assert payload["terminal_diagnosis"]["category"] == "sigkill"

    def test_workdir_dest_uses_job_name_when_set(self, state: Path) -> None:
        _materialize_job_with_workdir(
            state,
            "wdir00000002",
            workspace_files={"foo.py": ""},
            workdir_files={"out.dat": "data"},
            job_name="my-experiment",
        )
        out = state / "fetched"
        dst = fetch.fetch_workdir_local("wdir00000002", out)
        assert dst == out / "my-experiment-wdir00000002-workdir"
        assert (dst / "out.dat").read_text() == "data"

    def test_missing_jobid_raises(self, state: Path) -> None:
        with pytest.raises(FileNotFoundError, match="no such job"):
            fetch.fetch_workdir_local("nonexistent2", state / "out")

    def test_spec_without_workdir_field_raises(
        self, state: Path
    ) -> None:
        # _materialize_job builds a spec WITHOUT a workdir.
        _materialize_job(state, "nowdir000001", {"input.py": ""})
        with pytest.raises(FileNotFoundError, match="no workdir"):
            fetch.fetch_workdir_local("nowdir000001", state / "out")

    def test_scheduler_spec_without_workdir_points_to_workspace_fetch(
        self, state: Path
    ) -> None:
        spec = _materialize_job(state, "schednowdir1", {"stdout.log": "done\n"})
        spec.scheduler_target = "host_f-itwin"
        spec.scheduler_job_id = "123.host_f"
        spec.write(paths.spec_path("schednowdir1"))

        with pytest.raises(FileNotFoundError) as excinfo:
            fetch.fetch_workdir_local("schednowdir1", state / "out")

        msg = str(excinfo.value)
        assert "scheduler jobs are workspace-only in vq" in msg
        assert "preserved scheduler workspace" in msg
        assert "stdout, stderr, _vq markers, and generated output files" in msg
        assert "vq fetch host_f-itwin schednowdir1 -o DIR" in msg

    def test_workdir_missing_on_disk_raises(self, state: Path) -> None:
        # Spec has a workdir field but the directory is gone.
        _materialize_job_with_workdir(
            state,
            "wdirgone0001",
            workspace_files={},
            workdir_files={"x": ""},
        )
        spec = JobSpec.read(paths.queue_dir() / "wdirgone0001.json")
        import shutil
        shutil.rmtree(spec.workdir)
        with pytest.raises(FileNotFoundError, match="workdir.*not found"):
            fetch.fetch_workdir_local("wdirgone0001", state / "out")

    def test_workdir_swept_on_terminal_carries_clear_hint(
        self, state: Path
    ) -> None:
        """When the workdir was swept because --clean-tmp was set at
        submit + job hit terminal, the error explains why."""
        from vq.spec import JobState
        _materialize_job_with_workdir(
            state,
            "wdirsweep001",
            workspace_files={},
            workdir_files={"x": ""},
            clean_on_terminal=True,
        )
        spec_path = paths.queue_dir() / "wdirsweep001.json"
        spec = JobSpec.read(spec_path)
        spec.state = JobState.COMPLETED
        spec.finished_at = "2026-05-27T10:00:00+00:00"
        spec.exit_code = 0
        spec.write(spec_path)
        # Simulate the daemon's terminal sweep removing the workdir.
        import shutil
        shutil.rmtree(spec.workdir)
        with pytest.raises(FileNotFoundError, match="--clean-tmp"):
            fetch.fetch_workdir_local("wdirsweep001", state / "out")

    def test_workdir_age_swept_carries_age_sweep_hint(self, state: Path) -> None:
        """CLEAN-3: when the workdir was removed by the auto-cleanup AGE-SWEEP
        (workdir_swept_at stamped, --clean-tmp NOT set), the hint names the
        age-sweep — not --clean-tmp, which the operator never passed."""
        from vq.spec import JobState
        _materialize_job_with_workdir(
            state, "wdirage00001", workspace_files={}, workdir_files={"x": ""},
            clean_on_terminal=False,  # NOT --clean-tmp
        )
        spec_path = paths.queue_dir() / "wdirage00001.json"
        spec = JobSpec.read(spec_path)
        spec.state = JobState.COMPLETED
        spec.finished_at = "2026-05-27T10:00:00+00:00"
        spec.workdir_swept_at = "2026-06-01T03:00:00+00:00"  # the age-sweep stamp
        spec.write(spec_path)
        import shutil
        shutil.rmtree(spec.workdir)
        with pytest.raises(FileNotFoundError, match="age-sweep") as ei:
            fetch.fetch_workdir_local("wdirage00001", state / "out")
        assert "--clean-tmp" not in str(ei.value)

    def test_existing_destination_raises(self, state: Path) -> None:
        _materialize_job_with_workdir(
            state,
            "wdirexist001",
            workspace_files={},
            workdir_files={"x": ""},
        )
        out = state / "fetched"
        out.mkdir()
        (out / "wdirexist001-workdir").mkdir()
        with pytest.raises(
            FileExistsError, match="destination already exists"
        ):
            fetch.fetch_workdir_local("wdirexist001", out)


class TestEmitWorkdirTar:
    def test_writes_tar_to_stdout(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _materialize_job_with_workdir(
            state,
            "wdirtar00001",
            workspace_files={},
            workdir_files={"data.bin": "raw bytes", "log.txt": "trace"},
        )
        buf = io.BytesIO()

        class FakeStdout:
            buffer = buf

        monkeypatch.setattr("sys.stdout", FakeStdout())
        fetch.emit_workdir_tar("wdirtar00001")

        buf.seek(0)
        with tarfile.open(fileobj=buf, mode="r|") as tf:
            members = list(tf)
        names = [m.name for m in members]
        # arcname carries the "-workdir" suffix so the receiver lands
        # the payload in <jobid>-workdir/.
        for n in names:
            assert n.startswith("wdirtar00001-workdir")
        # Both workdir files are in the tar.
        assert any(n.endswith("data.bin") for n in names)
        assert any(n.endswith("log.txt") for n in names)

    def test_workdir_tar_includes_terminal_diagnosis_sidecar(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from vq.spec import JobState

        _materialize_job_with_workdir(
            state,
            "wdirtardiag1",
            workspace_files={},
            workdir_files={"trace.txt": "partial"},
        )
        spec_path = paths.queue_dir() / "wdirtardiag1.json"
        spec = JobSpec.read(spec_path)
        spec.state = JobState.FAILED
        spec.exit_code = 137
        spec.write(spec_path)
        buf = io.BytesIO()

        class FakeStdout:
            buffer = buf

        monkeypatch.setattr("sys.stdout", FakeStdout())
        fetch.emit_workdir_tar("wdirtardiag1")

        buf.seek(0)
        with tarfile.open(fileobj=buf, mode="r:") as tf:
            member = tf.extractfile(
                "wdirtardiag1-workdir/_vq/terminal-diagnosis.json"
            )
            assert member is not None
            payload = json.loads(member.read().decode("utf-8"))
        assert payload["jobid"] == "wdirtardiag1"
        assert payload["terminal_diagnosis"]["category"] == "sigkill"

    def test_missing_jobid_raises(self, state: Path) -> None:
        with pytest.raises(FileNotFoundError, match="no such job"):
            fetch.emit_workdir_tar("nonexistent3")

    def test_multi_user_submitter_numeric_streams_workdir(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(state / "multi-user"))
        _enable_multi_user_config(state, monkeypatch)
        _materialize_multi_user_job(
            str(os.geteuid()),
            "marsfetch002",
            workspace_files={"input.py": "print('hi')\n"},
            workdir_files={"_vq/events.jsonl": '{"state":"failed"}\n'},
            job_name="vibeview-p04-bands",
        )
        buf = io.BytesIO()

        class _FakeStdout:
            buffer = buf

        monkeypatch.setattr("sys.stdout", _FakeStdout())

        fetch.emit_workdir_tar("marsfetch002", multi_user=True)

        buf.seek(0)
        with tarfile.open(fileobj=buf, mode="r|") as tf:
            names = tf.getnames()
        assert "vibeview-p04-bands-marsfetch002-workdir" in names
        assert (
            "vibeview-p04-bands-marsfetch002-workdir/_vq/events.jsonl"
            in names
        )

    def test_spec_without_workdir_field_raises(
        self, state: Path
    ) -> None:
        _materialize_job(state, "nowdir000002", {"x": ""})
        with pytest.raises(FileNotFoundError, match="no workdir"):
            fetch.emit_workdir_tar("nowdir000002")


class TestInternalTarCommandsMultiUser:
    def test_mark_fetched_is_hidden_and_autodetects_multi_user_owner(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from click.testing import CliRunner

        from vq.cli import main
        from vq.spec import JobState

        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(state / "multi-user"))
        _enable_multi_user_config(state, monkeypatch)
        uid = str(os.geteuid())
        spec = _materialize_multi_user_job(
            uid,
            "mark-owner-1",
            workspace_files={"result.out": "done\n"},
        )
        spec.state = JobState.COMPLETED
        spec.finished_at = "2026-08-20T10:13:00+00:00"
        spec.write(paths.user_spec_path(uid, spec.id))
        runner = CliRunner()

        help_result = runner.invoke(main, ["--help"])
        result = runner.invoke(
            main,
            [
                "mark-fetched",
                spec.id,
                "--submitted-at",
                spec.submitted_at,
                "--state",
                "completed",
            ],
        )

        assert "mark-fetched" not in help_result.output
        assert result.exit_code == 0, result.output
        assert len(result.stdout.strip().splitlines()) == 1
        assert datetime.fromisoformat(result.stdout.strip()).tzinfo is not None
        assert JobSpec.read(paths.user_spec_path(uid, spec.id)).last_fetched_at

    def test_mark_fetched_fails_closed_on_duplicate_multi_user_jobid(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from click.testing import CliRunner

        from vq.cli import main
        from vq.spec import JobState

        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(state / "multi-user"))
        _enable_multi_user_config(state, monkeypatch)
        first: JobSpec | None = None
        for uid in ("2001", "2002"):
            spec = _materialize_multi_user_job(
                uid,
                "mark-duplicate",
                workspace_files={"result.out": uid},
            )
            spec.state = JobState.COMPLETED
            spec.finished_at = "2026-08-20T10:13:00+00:00"
            spec.write(paths.user_spec_path(uid, spec.id))
            first = first or spec
        assert first is not None

        result = CliRunner().invoke(
            main,
            [
                "mark-fetched",
                first.id,
                "--submitted-at",
                first.submitted_at,
                "--state",
                "completed",
            ],
        )

        assert result.exit_code == 1
        assert result.stdout_bytes == b""
        assert result.stderr.strip().splitlines() == [
            "ambiguous job id 'mark-duplicate': multiple per-user specs exist"
        ]
        for uid in ("2001", "2002"):
            assert JobSpec.read(
                paths.user_spec_path(uid, first.id)
            ).last_fetched_at is None

    @pytest.mark.parametrize(
        "args",
        [
            ["tar-workspace", "../outside"],
            [
                "mark-fetched",
                "../outside",
                "--submitted-at",
                "2026-08-20T10:11:12+00:00",
                "--state",
                "completed",
            ],
        ],
    )
    def test_hidden_job_command_rejects_traversal_before_outside_read(
        self, state: Path, args: list[str]
    ) -> None:
        from click.testing import CliRunner

        from vq.cli import main

        queue = paths.queue_dir()
        queue.mkdir(parents=True)
        outside_workspace = state / "outside-workspace"
        outside_workspace.mkdir()
        (outside_workspace / "private.dat").write_text("must not stream\n")
        JobSpec(
            id="outside",
            command=["true"],
            cwd=str(outside_workspace),
            cpus=1,
        ).write(queue.parent / "outside.json")

        result = CliRunner().invoke(main, args)

        assert result.exit_code == 1
        assert result.stdout_bytes == b""
        assert "invalid job id '../outside'" in result.stderr
        assert "must not stream" not in result.stderr

    @pytest.mark.parametrize(
        "args",
        [
            ["tar-workspace", "foreign-cli-tar"],
            ["tar-artifact", "foreign-cli-tar", "result.dat"],
            ["tar-workdir", "foreign-cli-tar"],
            [
                "mark-fetched",
                "foreign-cli-tar",
                "--submitted-at",
                "SUBMITTED_AT",
                "--state",
                "completed",
            ],
        ],
    )
    def test_foreign_job_denial_is_one_line_stderr_and_zero_stdout(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
        args: list[str],
    ) -> None:
        from click.testing import CliRunner

        from vq.cli import main

        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(state / "multi-user"))
        _enable_multi_user_config(state, monkeypatch)
        spec = _materialize_multi_user_job(
            "2002",
            "foreign-cli-tar",
            workspace_files={"result.dat": "private result\n"},
            workdir_files={"scratch.dat": "private scratch\n"},
        )
        if args[0] == "mark-fetched":
            from vq.spec import JobState

            spec.state = JobState.COMPLETED
            spec.finished_at = "2026-08-20T10:13:00+00:00"
            spec.write(paths.user_spec_path("2002", spec.id))
            args = [spec.submitted_at if arg == "SUBMITTED_AT" else arg for arg in args]
        monkeypatch.setattr(ownership, "_caller_uid", lambda: 1001)
        monkeypatch.setattr(ownership, "_caller_is_admin", lambda _cfg: False)

        result = CliRunner().invoke(main, args)

        assert result.exit_code == 1
        assert result.stdout_bytes == b""
        assert result.stderr.strip().splitlines() == [
            "job foreign-cli-tar belongs to uid 2002; caller is uid 1001. "
            "Join the 'vq-admins' group for admin access, or run as root."
        ]

    @pytest.mark.parametrize(
        "args",
        [
            ["tar-workspace", "invalid-policy-tar"],
            ["tar-artifact", "invalid-policy-tar", "result.dat"],
            ["tar-workdir", "invalid-policy-tar"],
        ],
    )
    def test_invalid_system_policy_is_one_line_stderr_and_zero_stdout(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
        args: list[str],
    ) -> None:
        from click.testing import CliRunner

        from vq.cli import main

        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(state / "multi-user"))
        personal_dir = state / "empty-personal-config"
        personal_dir.mkdir()
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(personal_dir))
        system_path = state / "invalid-system.toml"
        system_path.write_text(
            'hosts = "not-a-table"\n'
            "[multi_user]\n"
            "enabled = true\n"
        )
        monkeypatch.setattr(config, "SYSTEM_CONFIG_PATH", system_path)
        _materialize_multi_user_job(
            str(os.geteuid()),
            "invalid-policy-tar",
            workspace_files={"result.dat": "must not stream\n"},
            workdir_files={"scratch.dat": "must not stream\n"},
        )

        result = CliRunner().invoke(main, args)

        assert result.exit_code == 1
        assert result.stdout_bytes == b""
        assert len(result.stderr.strip().splitlines()) == 1
        assert "invalid config in" in result.stderr
        assert "Traceback" not in result.stderr

    def test_tar_workspace_uses_multi_user_autodetect(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from click.testing import CliRunner

        from vq.cli import main

        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(state / "multi-user"))
        _enable_multi_user_config(state, monkeypatch)
        _materialize_multi_user_job(
            str(os.geteuid()),
            "marscli001",
            workspace_files={"stdout.log": "located\n"},
            job_name="vibeview-p04-bands",
        )

        result = CliRunner().invoke(
            main, ["tar-workspace", "marscli001"], standalone_mode=False
        )

        assert result.exit_code == 0, getattr(result, "output", "")
        raw = getattr(result, "stdout_bytes", None) or result.output.encode()
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r|") as tf:
            names = tf.getnames()
        assert "vibeview-p04-bands-marscli001/stdout.log" in names

    def test_tar_workdir_uses_multi_user_autodetect(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from click.testing import CliRunner

        from vq.cli import main

        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(state / "multi-user"))
        _enable_multi_user_config(state, monkeypatch)
        _materialize_multi_user_job(
            str(os.geteuid()),
            "marscli002",
            workspace_files={"input.py": "print('hi')\n"},
            workdir_files={"_vq/events.jsonl": '{"state":"failed"}\n'},
            job_name="vibeview-p04-bands",
        )

        result = CliRunner().invoke(
            main, ["tar-workdir", "marscli002"], standalone_mode=False
        )

        assert result.exit_code == 0, getattr(result, "output", "")
        raw = getattr(result, "stdout_bytes", None) or result.output.encode()
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r|") as tf:
            names = tf.getnames()
        assert (
            "vibeview-p04-bands-marscli002-workdir/_vq/events.jsonl"
            in names
        )


class TestFetchWorkdirRemote:
    @pytest.fixture
    def host_cfg(self) -> HostConfig:
        return HostConfig(ssh="host_d", remote_vq="vq")

    def _build_tar_bytes(self, files: dict[str, str], top_dir: str) -> bytes:
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w|") as tf:
            for name, content in files.items():
                data = content.encode()
                info = tarfile.TarInfo(f"{top_dir}/{name}")
                info.size = len(data)
                tf.addfile(info, io.BytesIO(data))
            d = tarfile.TarInfo(top_dir)
            d.type = tarfile.DIRTYPE
            tf.addfile(d)
        return buf.getvalue()

    def test_extracts_remote_workdir_tarball(
        self,
        host_cfg: HostConfig,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        diagnosis = json.dumps(
            {
                "schema": "vq.terminal-diagnosis.v1",
                "jobid": "abc999",
                "submitted_at": "2026-08-20T10:11:12+00:00",
                "state": "completed",
            }
        )
        tar_bytes = self._build_tar_bytes(
            {
                "scratch.bin": "binary blob",
                "checkpoint.h5": "h5",
                fetch.TERMINAL_DIAGNOSIS_SIDECAR: diagnosis,
            },
            top_dir="abc999-workdir",
        )

        class FakeProc:
            def __init__(self) -> None:
                self.stdout = io.BytesIO(tar_bytes)
                self.stderr = io.BytesIO(b"")
                self.returncode: int | None = None

            def wait(self) -> int:
                self.returncode = 0
                return 0

            def kill(self) -> None: ...

            def communicate(self) -> tuple[bytes, bytes]:
                return b"", b""

        captured: list[list[str]] = []

        def fake_popen(cmd: list[str], **kwargs) -> FakeProc:
            captured.append(cmd)
            return FakeProc()

        monkeypatch.setattr(transport.subprocess, "Popen", fake_popen)
        monkeypatch.setattr(
            transport,
            "run_remote_vq",
            lambda *args, **kwargs: pytest.fail(
                "workdir retrieval must not mark the workspace fetched"
            ),
        )

        out = tmp_path / "out"
        dst = fetch.fetch_workdir_remote(host_cfg, "abc999", out)
        assert dst == out / "abc999-workdir"
        assert (dst / "scratch.bin").read_text() == "binary blob"
        assert (dst / "checkpoint.h5").read_text() == "h5"

        # v0.8.17: hardened transport; the tar-workdir verb is shell-quoted
        # into one remote-command string after the host.
        assert len(captured) == 1
        argv = captured[0]
        assert argv[0] == "ssh"
        assert "BatchMode=yes" in argv
        assert argv[-2:] == ["host_d", "vq tar-workdir abc999"]

    def test_remote_failure_raises(
        self,
        host_cfg: HostConfig,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        class FakeProc:
            def __init__(self) -> None:
                self.stdout = io.BytesIO(b"")
                self.stderr = io.BytesIO(b"workdir for job X not found")
                self.returncode: int | None = None

            def wait(self) -> int:
                self.returncode = 1
                return 1

            def kill(self) -> None: ...

            def communicate(self) -> tuple[bytes, bytes]:
                return b"", b"workdir for job X not found"

        monkeypatch.setattr(
            transport.subprocess, "Popen", lambda *a, **k: FakeProc()
        )

        with pytest.raises(transport.RemoteError):
            fetch.fetch_workdir_remote(
                host_cfg, "abc999", tmp_path / "out"
            )


class TestSelectedWorkdirArtifact:
    @pytest.mark.parametrize("subdir", [None, "results", "results/accepted"])
    def test_cli_selects_workdir_without_copying_build_tree(
        self, state: Path, monkeypatch: pytest.MonkeyPatch, subdir: str | None,
    ) -> None:
        from click.testing import CliRunner

        from vq.cli import main

        spec = _materialize_job_with_workdir(
            state, "selectedwd01", {"kernel.json": "workspace"},
            {"kernel.json": "workdir"},
        )
        if subdir:
            nested = Path(spec.workdir) / subdir
            nested.mkdir(parents=True)
            (nested / "kernel.json").write_text("workdir")
            (Path(spec.workdir) / "kernel.json").write_text("wrong root")
        build = Path(spec.workdir) / "native-build"
        build.mkdir()
        (build / "large-object.o").write_bytes(b"unrelated" * 1024)

        def forbidden_refresh(*args: object, **kwargs: object) -> None:
            pytest.fail("selected workdir fetch must not refresh a scheduler workspace")

        monkeypatch.setattr(fetch, "_refresh_live_scheduler_workspace", forbidden_refresh)
        out = state / "selected"
        result = CliRunner().invoke(main, [
            "fetch", "localhost", spec.id, "--workdir", "--name", "kernel.json",
            "-o", str(out), "--json",
            *(["--subdir", subdir] if subdir else []),
        ])
        assert result.exit_code == 0, result.output
        assert (out / "kernel.json").read_text() == "workdir"
        assert [p.name for p in out.iterdir()] == ["kernel.json"]
        assert json.loads(result.output)["source_kind"] == "workdir"
        assert json.loads(result.output)["source_subdir"] == subdir

    @pytest.mark.parametrize("subdir", [None, "results", "results/accepted"])
    def test_remote_round_trip_transfers_only_selected_regular_file(
        self, state: Path, monkeypatch: pytest.MonkeyPatch, subdir: str | None,
    ) -> None:
        import contextlib
        from types import SimpleNamespace

        from click.testing import CliRunner

        from vq.cli import main

        spec = _materialize_job_with_workdir(
            state, "selectedwd02", {"kernel.json": "wrong workspace bytes"},
            {"kernel.json": "exact workdir bytes", "unrelated.o": "large build"},
        )
        if subdir:
            nested = Path(spec.workdir) / subdir
            nested.mkdir(parents=True)
            (nested / "kernel.json").write_text("exact workdir bytes")
            (Path(spec.workdir) / "kernel.json").write_text("wrong root")
        calls = []

        @contextlib.contextmanager
        def stream(host_cfg: HostConfig, *args: str):
            calls.append(args)
            result = CliRunner().invoke(main, list(args))
            assert result.exit_code == 0, result.stderr
            with tarfile.open(fileobj=io.BytesIO(result.stdout_bytes)) as tf:
                assert tf.getnames() == ["kernel.json"]
            yield SimpleNamespace(stdout=io.BytesIO(result.stdout_bytes), stderr_text="")

        monkeypatch.setattr(transport, "stream_remote_vq", stream)
        dst = fetch.fetch_artifact_remote(
            HostConfig(ssh="remote.invalid"), spec.id, "kernel.json", state / "remote-out",
            workdir=True, subdir=subdir,
        )
        assert dst.read_text() == "exact workdir bytes"
        assert calls == [("tar-artifact", spec.id, "kernel.json", "--workdir",
                          *(("--subdir", subdir) if subdir else ()))]

    @pytest.mark.parametrize("missing", ["unset", "swept", "file"])
    def test_missing_workdir_never_falls_back_to_workspace_or_archive(
        self, state: Path, missing: str,
    ) -> None:
        import shutil

        spec = _materialize_job_with_workdir(
            state, "selectedwd03", {"kernel.json": "wrong workspace bytes"},
            {"kernel.json": "workdir bytes"},
        )
        if missing == "unset":
            spec.workdir = None
        elif missing == "swept":
            shutil.rmtree(spec.workdir)
            spec.workdir_swept_at = "2026-09-07T12:00:00+00:00"
        else:
            (Path(spec.workdir) / "kernel.json").unlink()
        spec.archive_path = str(state / "must-not-open-workspace-archive.tar")
        spec.archived_at = "2026-09-07T11:00:00+00:00"
        spec.write(paths.spec_path(spec.id))
        out = state / "preserved"
        out.mkdir()
        (out / "kernel.json").write_text("previous accepted bytes")
        with pytest.raises(FileNotFoundError) as error:
            fetch.fetch_artifact_local(spec.id, "kernel.json", out, workdir=True)
        if missing == "swept":
            assert "age-sweep" in str(error.value)
        assert (out / "kernel.json").read_text() == "previous accepted bytes"
        assert len(list(out.iterdir())) == 1

    @pytest.mark.parametrize("kind", ["symlink", "fifo", "workdir-symlink"])
    def test_nonregular_sources_refused_without_reading_them(
        self, state: Path, kind: str,
    ) -> None:
        spec = _materialize_job_with_workdir(state, "selectedwd04", {}, {})
        source = Path(spec.workdir) / "kernel.json"
        if kind == "symlink":
            target = state / "private"
            target.write_text("private")
            source.symlink_to(target)
        elif kind == "fifo":
            os.mkfifo(source)
        else:
            target = Path(spec.workdir)
            source.write_text("redirected")
            link = state / "workdir-link"
            link.symlink_to(target, target_is_directory=True)
            spec.workdir = str(link)
            spec.write(paths.spec_path(spec.id))
        with pytest.raises((OSError, ValueError)):
            fetch.fetch_artifact_local(spec.id, "kernel.json", state / "out", workdir=True)
        assert not (state / "out" / "kernel.json").exists()

    @pytest.mark.parametrize("name", ["../kernel.json", "/kernel.json", ".", ""])
    def test_selected_workdir_refuses_traversal(self, state: Path, name: str) -> None:
        with pytest.raises(ValueError, match="basename"):
            fetch.fetch_artifact_local("selectedwd05", name, state / "out", workdir=True)
        assert not (state / "out").exists()

    def test_remote_failure_after_bytes_preserves_existing_artifact(
        self, state: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import contextlib
        from types import SimpleNamespace

        data = io.BytesIO()
        with tarfile.open(fileobj=data, mode="w") as tf:
            info = tarfile.TarInfo("kernel.json")
            info.size = 3
            tf.addfile(info, io.BytesIO(b"new"))

        @contextlib.contextmanager
        def fail_after_stream(*args: object):
            yield SimpleNamespace(stdout=io.BytesIO(data.getvalue()), stderr_text="failed")
            raise transport.RemoteError("remote command failed after output")

        monkeypatch.setattr(transport, "stream_remote_vq", fail_after_stream)
        out = state / "out"
        out.mkdir()
        (out / "kernel.json").write_text("old")
        with pytest.raises(transport.RemoteError, match="after output"):
            fetch.fetch_artifact_remote(
                HostConfig(ssh="remote.invalid"), "selectedwd06", "kernel.json", out,
                workdir=True,
            )
        assert (out / "kernel.json").read_text() == "old"
        assert len(list(out.iterdir())) == 1

    def test_old_remote_refusal_is_not_retried_as_workspace(
        self, state: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import contextlib

        calls = []

        @contextlib.contextmanager
        def old_remote(host_cfg: HostConfig, *args: str):
            calls.append(args)
            raise transport.RemoteError("No such option: --workdir")
            yield  # pragma: no cover - stream was never established

        monkeypatch.setattr(transport, "stream_remote_vq", old_remote)
        with pytest.raises(transport.RemoteError, match="No such option"):
            fetch.fetch_artifact_remote(
                HostConfig(ssh="old.invalid"), "selectedwd07", "kernel.json",
                state / "out", workdir=True,
            )
        assert calls == [("tar-artifact", "selectedwd07", "kernel.json", "--workdir")]
        assert not (state / "out" / "kernel.json").exists()

    def test_hidden_remote_cli_streams_workdir_selection(self, state: Path) -> None:
        from click.testing import CliRunner

        from vq.cli import main

        spec = _materialize_job_with_workdir(
            state, "selectedwd08", {"kernel.json": "workspace"},
            {"kernel.json": "workdir", "native.o": "unrelated"},
        )
        result = CliRunner().invoke(main, [
            "tar-artifact", spec.id, "kernel.json", "--workdir",
        ])
        assert result.exit_code == 0, result.stderr
        with tarfile.open(fileobj=io.BytesIO(result.stdout_bytes)) as tf:
            assert tf.getnames() == ["kernel.json"]
            assert tf.extractfile("kernel.json").read() == b"workdir"

    @pytest.mark.parametrize(
        "subdir", ["../results", "/results", ".", "", "results/..", "results//safe"],
    )
    def test_subdir_refuses_ambiguous_or_escaping_paths(self, state: Path, subdir: str) -> None:
        with pytest.raises(ValueError, match="relative directory"):
            fetch.fetch_artifact_local("selectedwd09", "kernel.json", state / "out",
                                       workdir=True, subdir=subdir)
        assert not (state / "out").exists()

    def test_subdir_symlink_refused(self, state: Path) -> None:
        spec = _materialize_job_with_workdir(state, "selectedwd10", {}, {})
        outside = state / "outside"
        outside.mkdir()
        (outside / "kernel.json").write_text("private")
        (Path(spec.workdir) / "results").symlink_to(outside, target_is_directory=True)
        with pytest.raises(OSError):
            fetch.fetch_artifact_local(spec.id, "kernel.json", state / "out",
                                       workdir=True, subdir="results")
        assert not (state / "out" / "kernel.json").exists()

    def test_subdir_replacement_cannot_redirect_open_file(
        self, state: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        spec = _materialize_job_with_workdir(state, "selectedwd11", {}, {})
        results = Path(spec.workdir) / "results"
        results.mkdir()
        (results / "kernel.json").write_text("original")
        outside = state / "outside"
        outside.mkdir()
        (outside / "kernel.json").write_text("redirected")
        original_open = os.open

        def replace_before_open(path, flags, *args, **kwargs):
            if path == "kernel.json" and kwargs.get("dir_fd") is not None:
                results.rename(Path(spec.workdir) / "old-results")
                results.symlink_to(outside, target_is_directory=True)
            return original_open(path, flags, *args, **kwargs)

        monkeypatch.setattr(os, "open", replace_before_open)
        fetched = fetch.fetch_artifact_local(spec.id, "kernel.json", state / "out",
                                             workdir=True, subdir="results")
        assert fetched.read_text() == "original"

    @pytest.mark.parametrize("options", [
        ["--subdir", "results", "--name", "kernel.json"],
        ["--subdir", "results", "--workdir"],
        ["--subdir", "results", "--workdir", "--name", "kernel.json", "--workspace"],
    ])
    def test_cli_rejects_incomplete_source_selection(self, state: Path, options: list[str]) -> None:
        from click.testing import CliRunner

        from vq.cli import main

        result = CliRunner().invoke(main, ["fetch", "localhost", "selectedwd12", *options])
        assert result.exit_code == 2
        assert "--subdir requires" in result.output or "mutually exclusive" in result.output
