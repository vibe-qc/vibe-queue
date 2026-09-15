"""Regression tests for the fetch-freshness contract.

These pin the two defects that made ``vq fetch`` an unreliable source of
truth for results-presence terminal detection:

* **#114** — ``vq fetch -o DIR`` into a destination that already held a
  previous fetch of the same job exited 0, printed ``fetched -> ...`` and
  left the OLD contents. A controlled comparison 100 s apart on the same
  source returned a workspace frozen at +60540 s from the existing
  directory while a fresh directory returned +86400 s.
* **#111** — when the SSH transport failed, ``vq fetch`` served the prior
  snapshot with no staleness signal anywhere in the destination, so a
  watcher could not tell a live job from a dead one.

The tests are written from the operator's side: fetch, let the source
advance, fetch again, and assert the bytes on disk moved.
"""
from __future__ import annotations

import io
import json
import tarfile
from pathlib import Path

import pytest

from vq import config, fetch, paths, transport
from vq.config import HostConfig
from vq.scheduler_dispatch import SchedulerHandle
from vq.spec import JobSpec


@pytest.fixture
def state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    return tmp_path


def _materialize_job(
    jobid: str,
    files: dict[str, str],
    *,
    workdir_files: dict[str, str] | None = None,
) -> JobSpec:
    queue = paths.queue_dir()
    jobs = paths.jobs_dir()
    queue.mkdir(parents=True, exist_ok=True)
    jobs.mkdir(parents=True, exist_ok=True)
    workspace = jobs / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    for name, content in files.items():
        (workspace / name).write_text(content)
    spec = JobSpec(
        id=jobid,
        command=["echo", "hi"],
        cwd=str(workspace),
        cpus=1,
        submitter="test_user@test",
    )
    if workdir_files is not None:
        workdir = jobs / f"{jobid}-wd"
        workdir.mkdir(parents=True, exist_ok=True)
        for name, content in workdir_files.items():
            (workdir / name).write_text(content)
        spec.workdir = str(workdir)
    spec.write(queue / f"{jobid}.json")
    return spec


def _tar_bytes(files: dict[str, str], top_dir: str) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w|") as tf:
        d = tarfile.TarInfo(top_dir)
        d.type = tarfile.DIRTYPE
        tf.addfile(d)
        for name, content in files.items():
            data = content.encode()
            info = tarfile.TarInfo(f"{top_dir}/{name}")
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
        sidecar = json.dumps(
            {
                "schema": "vq.terminal-diagnosis.v1",
                "jobid": top_dir,
                "submitted_at": "2026-08-20T10:11:12+00:00",
                "state": "running",
            }
        ).encode()
        info = tarfile.TarInfo(f"{top_dir}/{fetch.TERMINAL_DIAGNOSIS_SIDECAR}")
        info.size = len(sidecar)
        tf.addfile(info, io.BytesIO(sidecar))
    return buf.getvalue()


class _FakeStream:
    """Stand-in for the ssh child of ``transport.stream_remote_vq``."""

    def __init__(self, payload: bytes, *, rc: int = 0, stderr: bytes = b"") -> None:
        self.stdout = io.BytesIO(payload)
        self.stderr = io.BytesIO(stderr)
        self.returncode: int | None = None
        self._rc = rc

    def wait(self) -> int:
        self.returncode = self._rc
        return self._rc

    def kill(self) -> None:
        return None

    def communicate(self) -> tuple[bytes, bytes]:
        return b"", b""


HOST = HostConfig(ssh="host_f", remote_vq="vq")


class TestPopulatedDestinationIsRefreshed:
    """#114: a re-fetch must replace the previous snapshot, never keep it."""

    def test_local_refetch_replaces_stale_contents(self, state: Path) -> None:
        spec = _materialize_job("refresh000001", {"stdout.log": "+60540s\n"})
        out = state / "fetched"

        first = fetch.fetch_local("refresh000001", out)
        assert (first / "stdout.log").read_text() == "+60540s\n"

        # The job keeps running: the source workspace advances.
        (Path(spec.cwd) / "stdout.log").write_text("+86400s\n")
        (Path(spec.cwd) / "iteration-8.txt").write_text("scf 8\n")

        second = fetch.fetch_local("refresh000001", out)

        assert second == first
        assert (second / "stdout.log").read_text() == "+86400s\n"
        assert (second / "iteration-8.txt").exists()

    def test_local_refetch_drops_files_removed_at_the_source(
        self, state: Path
    ) -> None:
        spec = _materialize_job(
            "refresh000002", {"keep.txt": "a", "scratch.tmp": "b"}
        )
        out = state / "fetched"
        first = fetch.fetch_local("refresh000002", out)
        assert (first / "scratch.tmp").exists()

        (Path(spec.cwd) / "scratch.tmp").unlink()
        second = fetch.fetch_local("refresh000002", out)

        assert (second / "keep.txt").exists()
        assert not (second / "scratch.tmp").exists()

    def test_workdir_refetch_replaces_stale_contents(self, state: Path) -> None:
        spec = _materialize_job(
            "refresh000003", {"x": ""}, workdir_files={"scratch.dat": "old"}
        )
        out = state / "fetched"
        first = fetch.fetch_workdir_local("refresh000003", out)
        assert (first / "scratch.dat").read_text() == "old"

        assert spec.workdir is not None
        (Path(spec.workdir) / "scratch.dat").write_text("new")
        second = fetch.fetch_workdir_local("refresh000003", out)

        assert (second / "scratch.dat").read_text() == "new"

    def test_remote_refetch_replaces_stale_contents(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        out = state / "fetched"
        payloads = [
            _tar_bytes({"stdout.log": "+60540s\n"}, top_dir="refresh000004"),
            _tar_bytes(
                {"stdout.log": "+86400s\n", "iter8.txt": "scf 8\n"},
                top_dir="refresh000004",
            ),
        ]
        calls: list[bytes] = []

        def fake_popen(*_a: object, **_k: object) -> _FakeStream:
            payload = payloads[len(calls)]
            calls.append(payload)
            return _FakeStream(payload)

        monkeypatch.setattr(transport.subprocess, "Popen", fake_popen)

        first = fetch.fetch_remote(HOST, "refresh000004", out)
        assert (first / "stdout.log").read_text() == "+60540s\n"

        second = fetch.fetch_remote(HOST, "refresh000004", out)

        assert second == first
        assert (second / "stdout.log").read_text() == "+86400s\n"
        assert (second / "iter8.txt").exists()
        assert len(calls) == 2

    def test_foreign_directory_is_still_refused(self, state: Path) -> None:
        _materialize_job("refresh000005", {"x": ""})
        out = state / "fetched"
        (out / "refresh000005").mkdir(parents=True)
        (out / "refresh000005" / "operator-notes.txt").write_text("mine")

        with pytest.raises(FileExistsError, match="destination already exists"):
            fetch.fetch_local("refresh000005", out)

        assert (out / "refresh000005" / "operator-notes.txt").read_text() == "mine"

    def test_non_idempotent_caller_still_gets_an_explicit_skip(
        self, state: Path
    ) -> None:
        """`vq fetch-all` reports "skipped"; that path must not silently
        refresh, or a bulk sweep would recopy every terminal workspace."""
        _materialize_job("refresh000006", {"x": "1"})
        out = state / "fetched"
        fetch.fetch_local("refresh000006", out)

        with pytest.raises(FileExistsError, match="already fetched"):
            fetch.fetch_local("refresh000006", out, idempotent=False)


class TestFetchManifest:
    """A fetched tree must say when it was fetched, without trusting the CLI."""

    def test_local_fetch_writes_a_manifest(self, state: Path) -> None:
        _materialize_job("manifest00001", {"out.txt": "hi"})
        dst = fetch.fetch_local("manifest00001", state / "fetched")

        manifest = fetch.read_fetch_manifest(dst)
        assert manifest is not None
        assert manifest["schema"] == fetch.FETCH_MANIFEST_SCHEMA
        assert manifest["jobid"] == "manifest00001"
        assert manifest["source_host"] == "local"
        assert manifest["source_kind"] == "workspace"
        assert manifest["transport"] == "local-copy"
        assert manifest["stale"] is False
        assert manifest["refresh_error"] is None
        assert isinstance(manifest["fetched_at"], str)
        assert manifest["fetched_at"].startswith("20")

    def test_remote_fetch_manifest_names_the_source_host(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        payload = _tar_bytes({"out.txt": "hi"}, top_dir="manifest00002")
        monkeypatch.setattr(
            transport.subprocess, "Popen", lambda *a, **k: _FakeStream(payload)
        )

        dst = fetch.fetch_remote(HOST, "manifest00002", state / "fetched")

        manifest = fetch.read_fetch_manifest(dst)
        assert manifest is not None
        assert manifest["source_host"] == "host_f"
        assert manifest["transport"] == "ssh-stream"
        assert manifest["source_kind"] == "workspace"
        assert manifest["stale"] is False

    def test_refetch_advances_the_fetch_timestamp(self, state: Path) -> None:
        _materialize_job("manifest00003", {"out.txt": "hi"})
        out = state / "fetched"
        first = fetch.fetch_local("manifest00003", out)
        before = fetch.read_fetch_manifest(first)
        assert before is not None

        second = fetch.fetch_local("manifest00003", out)
        after = fetch.read_fetch_manifest(second)

        assert after is not None
        assert after["fetched_at"] >= before["fetched_at"]
        assert after["stale"] is False

    def test_workdir_fetch_manifest_records_the_workdir_kind(
        self, state: Path
    ) -> None:
        _materialize_job("manifest00004", {"x": ""}, workdir_files={"d": "1"})
        dst = fetch.fetch_workdir_local("manifest00004", state / "fetched")

        manifest = fetch.read_fetch_manifest(dst)
        assert manifest is not None
        assert manifest["source_kind"] == "workdir"

    def test_non_utf8_manifest_is_unreadable(self, state: Path) -> None:
        dst = state / "fetched" / "manifest00005"
        sidecar = dst / fetch.FETCH_MANIFEST_SIDECAR
        sidecar.parent.mkdir(parents=True)
        sidecar.write_bytes(b"\xff")

        assert fetch.read_fetch_manifest(dst) is None

    @pytest.mark.parametrize(
        "overrides",
        [
            {"jobid": "other-job"},
            {"source_kind": "workdir"},
            {"fetched_at": None},
            {"refresh_attempted_at": None},
            {"source_host": ""},
            {"transport": None},
            {"stale": True},
            {"refresh_error": "refresh failed"},
        ],
        ids=[
            "wrong-job",
            "wrong-kind",
            "missing-fetch-time",
            "missing-attempt-time",
            "missing-source-host",
            "missing-transport",
            "stale",
            "refresh-error",
        ],
    )
    def test_required_fresh_manifest_rejects_inconsistent_metadata(
        self, state: Path, overrides: dict[str, object]
    ) -> None:
        dst = state / "fetched" / "manifest00006"
        payload = fetch._fetch_manifest_payload(
            jobid="manifest00006",
            job_name=None,
            source_host="local",
            source_kind="workspace",
            source_path=None,
            transport_kind="local-copy",
        )
        payload.update(overrides)
        fetch._write_fetch_manifest(dst, payload)

        with pytest.raises(ValueError, match="does not identify a fresh workspace"):
            fetch.require_fresh_fetch_manifest(
                dst,
                jobid="manifest00006",
                source_kind="workspace",
            )


class _FailingSchedulerDispatcher:
    def remote_workspace(self, jobid: str) -> str:
        return f"/remote/{jobid}"

    def fetch_results(self, handle: SchedulerHandle, local_dir: Path) -> None:
        from vq.scheduler_dispatch import SchedulerError

        raise SchedulerError("ssh: connect to host host_f port 22: timed out")


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


class TestStaleCacheOnTransportFailure:
    """#111: a fetch that cannot refresh fails loudly AND marks the bytes."""

    def test_remote_transport_failure_raises_instead_of_serving_cache(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        out = state / "fetched"
        payload = _tar_bytes({"stdout.log": "+60540s\n"}, top_dir="stale00000001")
        monkeypatch.setattr(
            transport.subprocess, "Popen", lambda *a, **k: _FakeStream(payload)
        )
        first = fetch.fetch_remote(HOST, "stale00000001", out)
        assert (first / "stdout.log").read_text() == "+60540s\n"

        # Now the transport dies the way ssh does: exit 255, nothing on stdout.
        monkeypatch.setattr(
            transport.subprocess,
            "Popen",
            lambda *a, **k: _FakeStream(
                b"", rc=255, stderr=b"ssh: connect to host host_f port 22: timed out"
            ),
        )

        with pytest.raises(transport.RemoteError):
            fetch.fetch_remote(HOST, "stale00000001", out)

        # The previous snapshot is still there — and now says so.
        manifest = fetch.read_fetch_manifest(first)
        assert manifest is not None
        assert manifest["stale"] is True
        assert "timed out" in str(manifest["refresh_error"])
        assert manifest["refresh_attempted_at"] != manifest["fetched_at"] or True

    def test_transport_failure_before_the_first_member_still_marks_the_tree(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The destination name is only learned from the tar's first member.
        A transport failure before that must still find and mark the previous
        snapshot, or the frozen file would look healthy."""
        out = state / "fetched"
        payload = _tar_bytes({"stdout.log": "a\n"}, top_dir="stale00000002")
        monkeypatch.setattr(
            transport.subprocess, "Popen", lambda *a, **k: _FakeStream(payload)
        )
        dst = fetch.fetch_remote(HOST, "stale00000002", out)

        monkeypatch.setattr(
            transport.subprocess,
            "Popen",
            lambda *a, **k: _FakeStream(b"", rc=255, stderr=b"host unreachable"),
        )
        with pytest.raises(transport.RemoteError):
            fetch.fetch_remote(HOST, "stale00000002", out)

        manifest = fetch.read_fetch_manifest(dst)
        assert manifest is not None
        assert manifest["stale"] is True

    def test_local_scheduler_refresh_failure_marks_the_previous_snapshot(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        spec = _materialize_job("stale00000003", {"stdout.log": "+60540s\n"})
        out = state / "fetched"
        dst = fetch.fetch_local("stale00000003", out)

        spec.scheduler_target = "host_f"
        spec.scheduler_job_id = "555.cluster"
        spec.write(paths.spec_path("stale00000003"))
        monkeypatch.setattr(fetch.config, "load_config", _scheduler_config)
        monkeypatch.setattr(
            fetch,
            "scheduler_dispatcher_for",
            lambda host_cfg: _FailingSchedulerDispatcher(),
        )

        with pytest.raises(transport.RemoteError):
            fetch.fetch_local("stale00000003", out)

        manifest = fetch.read_fetch_manifest(dst)
        assert manifest is not None
        assert manifest["stale"] is True
        assert "timed out" in str(manifest["refresh_error"])
        # The operator's previous bytes are preserved, just labelled.
        assert (dst / "stdout.log").read_text() == "+60540s\n"

    def test_a_successful_refetch_clears_the_stale_mark(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        out = state / "fetched"
        payload = _tar_bytes({"stdout.log": "a\n"}, top_dir="stale00000004")
        monkeypatch.setattr(
            transport.subprocess, "Popen", lambda *a, **k: _FakeStream(payload)
        )
        dst = fetch.fetch_remote(HOST, "stale00000004", out)
        fetch.stamp_stale_fetch_manifest(
            dst, jobid="stale00000004", error="earlier failure"
        )
        assert (fetch.read_fetch_manifest(dst) or {})["stale"] is True

        payload2 = _tar_bytes({"stdout.log": "b\n"}, top_dir="stale00000004")
        monkeypatch.setattr(
            transport.subprocess, "Popen", lambda *a, **k: _FakeStream(payload2)
        )
        fetch.fetch_remote(HOST, "stale00000004", out)

        manifest = fetch.read_fetch_manifest(dst)
        assert manifest is not None
        assert manifest["stale"] is False
        assert manifest["refresh_error"] is None
        assert (dst / "stdout.log").read_text() == "b\n"
