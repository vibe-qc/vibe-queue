"""Partial fetch: a workspace entry vq cannot carry costs the ENTRY, not the job (#62).

Two mechanisms lost five of 428 jobs in one `vq fetch-all HOST -s completed`:

* the emitter's ``tf.add(src, arcname=...)`` walked the workspace and raised on
  the first unreadable file, so a job under a multi-user root with one
  unreadable ``.qvf`` produced no tarball at all; and
* the receiver's ``data`` extraction filter refused absolute symlinks — which a
  job may legitimately make — and one refused member failed the whole job.

Both now skip the entry and record it. The extraction filter is NOT relaxed
anywhere in this module's expectations: an unsafe member must still never be
written, it must merely stop costing the job. The tests that pin that are
:meth:`TestRemoteExtractionSkipsRefusedMembers.
test_the_refused_link_is_never_written_outside_the_destination` and
:meth:`TestRemoteExtractionSkipsRefusedMembers.test_a_truncated_stream_still_fails`.
"""
from __future__ import annotations

import io
import json
import os
import tarfile
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import config, fetch, paths
from vq.cli import main
from vq.config import HostConfig
from vq.spec import JobSpec, JobState

pytestmark = pytest.mark.skipif(
    os.geteuid() == 0,
    reason="an unreadable file is still readable by root, so mechanism 1 "
    "cannot be reproduced; the suite must not run as root anyway",
)


# ----------------------------------------------------------------------
# fixtures / helpers
# ----------------------------------------------------------------------


@pytest.fixture
def state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    return tmp_path


@pytest.fixture
def cli_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "config.toml").write_text('default_host = "localhost"\n')
    return tmp_path


def _job(
    jobid: str,
    files: dict[str, str],
    *,
    st: JobState = JobState.COMPLETED,
    name: str | None = None,
) -> JobSpec:
    """Spec + workspace in the live queue dir, terminal by default."""
    queue = paths.queue_dir()
    jobs = paths.jobs_dir()
    queue.mkdir(parents=True, exist_ok=True)
    jobs.mkdir(parents=True, exist_ok=True)
    workspace = jobs / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    for fname, content in files.items():
        target = workspace / fname
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)
    spec = JobSpec(
        id=jobid,
        command=["echo", "hi"],
        cwd=str(workspace),
        cpus=1,
        submitter="test_user@test",
        state=st,
        job_name=name,
    )
    if spec.is_terminal:
        spec.finished_at = "2026-09-17T10:00:00+00:00"
        spec.exit_code = 0 if st == JobState.COMPLETED else 1
    spec.write(queue / f"{jobid}.json")
    return spec


def _emit_workspace_tar_bytes(
    jobid: str, monkeypatch: pytest.MonkeyPatch
) -> bytes:
    buf = io.BytesIO()

    class _FakeStdout:
        buffer = buf

    monkeypatch.setattr(fetch.sys, "stdout", _FakeStdout())
    fetch.emit_workspace_tar(jobid, protocol_token="0" * 32)
    return buf.getvalue()


def _tar_index(raw: bytes) -> dict[str, tarfile.TarInfo]:
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r|") as tf:
        return {member.name: member for member in tf}


def _tar_payload(raw: bytes, name: str) -> bytes:
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r|") as tf:
        for member in tf:
            if member.name == name:
                handle = tf.extractfile(member)
                assert handle is not None
                return handle.read()
    raise AssertionError(f"{name!r} is not in the tarball")


def _exclusions_from_tar(raw: bytes, top_dir: str) -> dict[str, object]:
    payload = json.loads(
        _tar_payload(raw, f"{top_dir}/{fetch.FETCH_EXCLUSIONS_SIDECAR}").decode()
    )
    assert isinstance(payload, dict)
    return payload


def _excluded_paths(payload: dict[str, object]) -> set[str]:
    excluded = payload["excluded"]
    assert isinstance(excluded, list)
    return {str(item["path"]) for item in excluded}


def _reason_for(payload: dict[str, object], path: str) -> str:
    excluded = payload["excluded"]
    assert isinstance(excluded, list)
    for item in excluded:
        if item["path"] == path:
            return str(item["reason"])
    raise AssertionError(f"{path!r} was not excluded; got {_excluded_paths(payload)}")


class _FakeProc:
    """Enough of Popen for transport.stream_remote_vq's drain + wait."""

    def __init__(self, tar_bytes: bytes) -> None:
        self.stdout = io.BytesIO(tar_bytes)
        self.stderr = io.BytesIO(b"")
        self.returncode: int | None = None

    def wait(self) -> int:
        self.returncode = 0
        return 0

    def kill(self) -> None:  # pragma: no cover - only on a failure path
        self.returncode = -9


def _remote_tar(
    top_dir: str,
    *,
    files: dict[str, str] | None = None,
    symlinks: dict[str, str] | None = None,
    diagnosis: dict[str, object] | None = None,
    exclusions: dict[str, object] | None = None,
) -> bytes:
    """A remote `vq tar-workspace` stream, built member by member.

    Building it by hand is the point: it lets a test ship exactly the member
    an old remote vq would have shipped (an absolute symlink), which is what
    the receiving side has to survive.
    """
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w|",
                      pax_headers={"VQ.fetch-protocol.v1": "0" * 32}) as tf:
        root = tarfile.TarInfo(top_dir)
        root.type = tarfile.DIRTYPE
        root.mode = 0o755
        tf.addfile(root)
        for name, content in (files or {}).items():
            data = content.encode()
            info = tarfile.TarInfo(f"{top_dir}/{name}")
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
        for name, target in (symlinks or {}).items():
            info = tarfile.TarInfo(f"{top_dir}/{name}")
            info.type = tarfile.SYMTYPE
            info.linkname = target
            tf.addfile(info)
        for relpath, payload in (
            (fetch.TERMINAL_DIAGNOSIS_SIDECAR, diagnosis),
            (fetch.FETCH_EXCLUSIONS_SIDECAR, exclusions),
        ):
            if payload is None:
                continue
            data = json.dumps(payload).encode()
            info = tarfile.TarInfo(f"{top_dir}/{relpath}")
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
    return buf.getvalue()


# ----------------------------------------------------------------------
# Mechanism 1 — the emitter walks past what it cannot read.
# ----------------------------------------------------------------------


class TestEmitterSkipsUnreadableEntries:
    def test_an_unreadable_file_costs_the_file_not_the_job(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Pre-#62 this raised PermissionError and produced no tarball."""
        _job(
            "unreadable01",
            {"bulk_si.out": "energies\n", "bulk_si.qvf": "binary\n"},
            name="bulk-si",
        )
        secret = paths.jobs_dir() / "unreadable01" / "bulk_si.qvf"
        secret.chmod(0o000)

        raw = _emit_workspace_tar_bytes("unreadable01", monkeypatch)

        names = set(_tar_index(raw))
        assert "bulk-si-unreadable01/bulk_si.out" in names
        assert "bulk-si-unreadable01/bulk_si.qvf" not in names
        payload = _exclusions_from_tar(raw, "bulk-si-unreadable01")
        assert payload["excluded_count"] == 1
        assert (
            _reason_for(payload, "bulk-si-unreadable01/bulk_si.qvf") == "unreadable"
        )

    def test_an_unreadable_subdirectory_costs_that_subtree_only(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _job(
            "unreadable02",
            {"keep.out": "kept\n", "periodic/eos/scan.qvf": "scan\n"},
        )
        (paths.jobs_dir() / "unreadable02" / "periodic" / "eos").chmod(0o000)
        try:
            raw = _emit_workspace_tar_bytes("unreadable02", monkeypatch)
        finally:
            (paths.jobs_dir() / "unreadable02" / "periodic" / "eos").chmod(0o755)

        names = set(_tar_index(raw))
        assert "unreadable02/keep.out" in names
        assert "unreadable02/periodic" in names
        assert "unreadable02/periodic/eos/scan.qvf" not in names
        payload = _exclusions_from_tar(raw, "unreadable02")
        assert (
            _reason_for(payload, "unreadable02/periodic/eos") == "unreadable_directory"
        )

    def test_a_complete_workspace_states_positively_that_nothing_was_dropped(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An empty list is evidence; a missing sidecar is only silence."""
        _job("complete001", {"a.out": "a\n", "sub/b.out": "b\n"})

        raw = _emit_workspace_tar_bytes("complete001", monkeypatch)

        names = set(_tar_index(raw))
        assert {"complete001/a.out", "complete001/sub/b.out"} <= names
        payload = _exclusions_from_tar(raw, "complete001")
        assert payload["excluded_count"] == 0
        assert payload["excluded"] == []
        assert payload["jobid"] == "complete001"

    def test_the_readable_tree_is_archived_exactly_as_tf_add_did(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The hand-rolled walk must not change a fully readable workspace."""
        _job("identical01", {"a.out": "a\n", "sub/b.out": "b\n", "sub/c.out": "c\n"})
        src = paths.jobs_dir() / "identical01"

        raw = _emit_workspace_tar_bytes("identical01", monkeypatch)
        reference = io.BytesIO()
        with tarfile.open(fileobj=reference, mode="w|") as tf:
            tf.add(src, arcname="identical01")

        ours = [
            name
            for name in _tar_index(raw)
            if not name.startswith("identical01/_vq/")
        ]
        assert ours == list(_tar_index(reference.getvalue()))


# ----------------------------------------------------------------------
# Mechanism 2, emitter half — absolute symlinks.
# ----------------------------------------------------------------------


class TestEmitterHandlesAbsoluteSymlinks:
    def test_an_absolute_link_inside_the_workspace_is_stored_relative(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Same file, expressed so the extraction filter can accept it."""
        _job("abslink0001", {"real/data.out": "payload\n"})
        workspace = paths.jobs_dir() / "abslink0001"
        (workspace / "latest.out").symlink_to(workspace / "real" / "data.out")

        raw = _emit_workspace_tar_bytes("abslink0001", monkeypatch)

        member = _tar_index(raw)["abslink0001/latest.out"]
        assert member.issym()
        assert not os.path.isabs(member.linkname)
        assert member.linkname == os.path.join("real", "data.out")
        assert _exclusions_from_tar(raw, "abslink0001")["excluded_count"] == 0

    def test_a_relativized_link_survives_the_data_filter(
        self, state: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The rewrite is only worth anything if extraction now accepts it."""
        _job("abslink0002", {"real/data.out": "payload\n"})
        workspace = paths.jobs_dir() / "abslink0002"
        (workspace / "latest.out").symlink_to(workspace / "real" / "data.out")
        raw = _emit_workspace_tar_bytes("abslink0002", monkeypatch)

        unpacked = tmp_path / "unpacked"
        unpacked.mkdir()
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r|") as tf:
            tf.extractall(unpacked, filter="data")

        link = unpacked / "abslink0002" / "latest.out"
        assert link.is_symlink()
        assert link.read_text() == "payload\n"

    def test_an_absolute_link_out_of_the_workspace_is_skipped_and_listed(
        self, state: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The inventory-run shape: mirror/ links into a release checkout."""
        _job("abslink0003", {"inventory.json": "{}\n"}, name="r21c-inventory")
        workspace = paths.jobs_dir() / "abslink0003"
        elsewhere = tmp_path / "release-checkout"
        elsewhere.mkdir()
        (elsewhere / "python").write_text("interpreter\n")
        (workspace / "mirror").mkdir()
        (workspace / "mirror" / "python").symlink_to(elsewhere / "python")

        raw = _emit_workspace_tar_bytes("abslink0003", monkeypatch)

        top = "r21c-inventory-abslink0003"
        names = set(_tar_index(raw))
        assert f"{top}/inventory.json" in names
        assert f"{top}/mirror" in names
        assert f"{top}/mirror/python" not in names
        payload = _exclusions_from_tar(raw, top)
        assert _reason_for(payload, f"{top}/mirror/python") == "absolute_symlink"
        assert payload["excluded_count"] == 1

    def test_an_escaping_link_is_never_rewritten_into_the_tree(
        self, state: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Safety pin: no member may claim a target outside the workspace."""
        _job("abslink0004", {"keep.out": "kept\n"})
        workspace = paths.jobs_dir() / "abslink0004"
        (workspace / "escape").symlink_to(tmp_path / "outside")

        raw = _emit_workspace_tar_bytes("abslink0004", monkeypatch)

        for name, member in _tar_index(raw).items():
            if not member.issym():
                continue
            assert not os.path.isabs(member.linkname), name
            resolved = os.path.normpath(
                os.path.join(os.path.dirname(name), member.linkname)
            )
            assert not resolved.startswith(".."), name


# ----------------------------------------------------------------------
# Mechanism 2, receiver half — the extraction filter keeps its teeth.
# ----------------------------------------------------------------------


class TestRemoteExtractionSkipsRefusedMembers:
    host_cfg = HostConfig(ssh="host_d", remote_vq="vq")

    @staticmethod
    def _diagnosis(jobid: str) -> dict[str, object]:
        return {
            "schema": "vq.terminal-diagnosis.v1",
            "jobid": jobid,
            "submitted_at": "2026-09-17T10:11:12+00:00",
            "state": "completed",
        }

    def _stream(
        self, monkeypatch: pytest.MonkeyPatch, tar_bytes: bytes
    ) -> None:
        monkeypatch.setattr(
            fetch.transport.subprocess,
            "Popen",
            lambda *args, **kwargs: _FakeProc(tar_bytes),
        )
        monkeypatch.setattr(
            fetch,
            "_send_remote_fetch_acknowledgement",
            lambda *args, **kwargs: None,
        )

    def test_an_absolute_link_from_an_old_remote_costs_the_link_not_the_job(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Pre-#62 this raised 'is a link to an absolute path' for the job."""
        jobid = "oldremote001"
        self._stream(
            monkeypatch,
            _remote_tar(
                jobid,
                files={"inventory.json": "{}\n"},
                symlinks={"mirror-python": "/opt/release/python"},
                diagnosis=self._diagnosis(jobid),
            ),
        )
        out = tmp_path / "out"

        dst = fetch.fetch_remote(self.host_cfg, jobid, out)

        assert (dst / "inventory.json").read_text() == "{}\n"
        payload = fetch.read_fetch_exclusions(dst)
        assert payload is not None
        assert payload["excluded_count"] == 1
        assert _reason_for(payload, f"{jobid}/mirror-python") == "absolute_symlink"
        manifest = fetch.read_fetch_manifest(dst)
        assert manifest is not None
        assert manifest["partial"] is True
        assert manifest["excluded_count"] == 1
        # Partial is not stale: these bytes ARE this fetch's bytes.
        assert manifest["stale"] is False

    def test_the_refused_link_is_never_written_outside_the_destination(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The fix must not become 'trust the member'."""
        jobid = "traversal001"
        victim = tmp_path / "victim.txt"
        victim.write_text("untouched\n")
        self._stream(
            monkeypatch,
            _remote_tar(
                jobid,
                files={"keep.out": "kept\n"},
                symlinks={
                    "absolute": str(victim),
                    "escape": "../../victim.txt",
                },
                diagnosis=self._diagnosis(jobid),
            ),
        )
        out = tmp_path / "out"

        dst = fetch.fetch_remote(self.host_cfg, jobid, out)

        assert not (dst / "absolute").exists()
        assert not (dst / "absolute").is_symlink()
        assert not (dst / "escape").exists()
        assert not (dst / "escape").is_symlink()
        assert victim.read_text() == "untouched\n"
        payload = fetch.read_fetch_exclusions(dst)
        assert payload is not None
        assert _excluded_paths(payload) == {
            f"{jobid}/absolute",
            f"{jobid}/escape",
        }

    def test_a_truncated_stream_still_fails(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Only FilterError is survivable; a corrupt stream is still a failure."""
        jobid = "truncated001"
        whole = _remote_tar(
            jobid,
            files={"keep.out": "kept\n" * 500},
            diagnosis=self._diagnosis(jobid),
        )
        self._stream(monkeypatch, whole[: len(whole) // 3])

        with pytest.raises(fetch.transport.RemoteError, match="failed to extract"):
            fetch.fetch_remote(self.host_cfg, jobid, tmp_path / "out")

    def test_a_short_read_is_raised_not_recorded_as_an_exclusion(
        self, tmp_path: Path
    ) -> None:
        """Widening the catch to TarError would turn lost bytes into a note.

        A member whose data ran out is not a member vq chose to leave behind;
        it is evidence that the stream cannot be trusted. Recording it as an
        exclusion would report a corrupt transfer as a tidy partial one.
        """
        raw = _remote_tar("shortread01", files={"keep.out": "kept\n" * 500})
        exclusions: list[fetch.FetchExclusion] = []
        dest = tmp_path / "dest"
        dest.mkdir()

        with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as complete:
            cut = complete.getmember("shortread01/keep.out").offset_data + 1000
        with (
            tarfile.open(fileobj=io.BytesIO(raw[:cut]), mode="r|") as tf,
            pytest.raises(tarfile.ReadError),
        ):
            for member in tf:
                fetch._extract_member_or_record(tf, member, dest, exclusions)

        assert exclusions == []

    def test_the_remote_exclusion_list_is_merged_with_the_local_one(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A partially readable workspace AND a refused link: one manifest."""
        jobid = "merged000001"
        self._stream(
            monkeypatch,
            _remote_tar(
                jobid,
                files={"keep.out": "kept\n"},
                symlinks={"mirror": "/opt/release"},
                diagnosis=self._diagnosis(jobid),
                exclusions={
                    "schema": fetch.FETCH_EXCLUSIONS_SCHEMA,
                    "jobid": jobid,
                    "excluded_count": 1,
                    "excluded": [
                        {
                            "path": f"{jobid}/secret.qvf",
                            "stage": fetch.EXCLUSION_STAGE_ARCHIVE,
                            "reason": "unreadable",
                            "detail": "PermissionError: Permission denied",
                        }
                    ],
                },
            ),
        )

        dst = fetch.fetch_remote(self.host_cfg, jobid, tmp_path / "out")

        payload = fetch.read_fetch_exclusions(dst)
        assert payload is not None
        assert payload["excluded_count"] == 2
        assert _excluded_paths(payload) == {
            f"{jobid}/secret.qvf",
            f"{jobid}/mirror",
        }
        stages = {
            str(item["stage"]) for item in payload["excluded"]  # type: ignore[union-attr]
        }
        assert stages == {
            fetch.EXCLUSION_STAGE_ARCHIVE,
            fetch.EXCLUSION_STAGE_EXTRACT,
        }

    def test_a_malformed_remote_exclusion_list_is_ignored_not_trusted(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        jobid = "junklist0001"
        self._stream(
            monkeypatch,
            _remote_tar(
                jobid,
                files={"keep.out": "kept\n"},
                diagnosis=self._diagnosis(jobid),
                exclusions={
                    "schema": "not.our.schema",
                    "excluded": [{"path": 17}, "nonsense"],
                },
            ),
        )

        dst = fetch.fetch_remote(self.host_cfg, jobid, tmp_path / "out")

        payload = fetch.read_fetch_exclusions(dst)
        assert payload is not None
        assert payload["excluded_count"] == 0


# ----------------------------------------------------------------------
# Local fetch — the same contract without a wire.
# ----------------------------------------------------------------------


class TestLocalFetchRecordsExclusions:
    def test_an_unreadable_file_no_longer_aborts_a_local_fetch(
        self, state: Path, tmp_path: Path
    ) -> None:
        _job("localpart01", {"good.out": "good\n", "secret.qvf": "secret\n"})
        (paths.jobs_dir() / "localpart01" / "secret.qvf").chmod(0o000)
        out = tmp_path / "out"

        dst = fetch.fetch_local("localpart01", out)

        assert (dst / "good.out").read_text() == "good\n"
        payload = fetch.read_fetch_exclusions(dst)
        assert payload is not None
        assert payload["excluded_count"] == 1
        assert _excluded_paths(payload) == {"localpart01/secret.qvf"}
        manifest = fetch.read_fetch_manifest(dst)
        assert manifest is not None
        assert manifest["partial"] is True

    def test_a_complete_local_fetch_is_not_partial(
        self, state: Path, tmp_path: Path
    ) -> None:
        _job("localwhole1", {"good.out": "good\n"})

        dst = fetch.fetch_local("localwhole1", tmp_path / "out")

        manifest = fetch.read_fetch_manifest(dst)
        assert manifest is not None
        assert manifest["partial"] is False
        assert manifest["excluded_count"] == 0
        assert fetch.excluded_count_for_destination(dst) == 0


# ----------------------------------------------------------------------
# `vq fetch-all`: partial is loud, and the sweep leaves a manifest.
# ----------------------------------------------------------------------


class TestFetchAllReportsCoverage:
    def test_a_partial_job_is_fetched_flagged_and_still_exit_zero(
        self, cli_state: Path
    ) -> None:
        """The deliberate policy: a stated partial beats a lost job."""
        _job("sweeppart01", {"good.out": "good\n", "secret.qvf": "secret\n"})
        _job("sweepwhole1", {"good.out": "good\n"})
        (paths.jobs_dir() / "sweeppart01" / "secret.qvf").chmod(0o000)
        out = cli_state / "results"

        result = CliRunner().invoke(main, ["fetch-all", "localhost", "-o", str(out)])

        assert result.exit_code == 0, result.output
        assert "[PARTIAL: 1 excluded]" in result.output
        assert "2 fetched, 1 partial" in result.output
        assert "INCOMPLETE" in result.output
        assert (out / "sweeppart01" / "good.out").read_text() == "good\n"

    def test_the_sweep_writes_a_machine_readable_report(
        self, cli_state: Path
    ) -> None:
        """A count in a summary line is not a manifest (#62)."""
        _job("sweeppart02", {"good.out": "good\n", "secret.qvf": "secret\n"})
        _job("sweepwhole2", {"good.out": "good\n"}, name="eos-scan")
        (paths.jobs_dir() / "sweeppart02" / "secret.qvf").chmod(0o000)
        out = cli_state / "results"

        result = CliRunner().invoke(
            main, ["fetch-all", "localhost", "-o", str(out), "-s", "completed"]
        )

        assert result.exit_code == 0, result.output
        report_path = out / "_vq" / "fetch-all-report.json"
        assert f"report -> {report_path}" in result.output
        report = json.loads(report_path.read_text())
        assert report["schema"] == "vq.fetch-all-report.v1"
        assert report["states"] == ["completed"]
        assert report["totals"] == {
            "selected": 2,
            "fetched": 2,
            "partial": 1,
            "skipped": 0,
            "failed": 0,
        }
        by_id = {entry["jobid"]: entry for entry in report["jobs"]}
        assert by_id["sweepwhole2"]["partial"] is False
        assert by_id["sweepwhole2"]["job_name"] == "eos-scan"
        partial = by_id["sweeppart02"]
        assert partial["partial"] is True
        assert partial["outcome"] == "fetched"
        assert [item["path"] for item in partial["excluded"]] == [
            "sweeppart02/secret.qvf"
        ]

    def test_a_failed_job_is_named_in_the_report_not_only_counted(
        self, cli_state: Path
    ) -> None:
        _job("gonejob0001", {"good.out": "good\n"})
        # The workspace disappears between listing and fetch: the exact shape
        # that produced "5 failed" and no list of which five.
        import shutil

        shutil.rmtree(paths.jobs_dir() / "gonejob0001")
        out = cli_state / "results"

        result = CliRunner().invoke(main, ["fetch-all", "localhost", "-o", str(out)])

        assert result.exit_code != 0
        report = json.loads((out / "_vq" / "fetch-all-report.json").read_text())
        assert report["totals"]["failed"] == 1
        entry = report["jobs"][0]
        assert entry["jobid"] == "gonejob0001"
        assert entry["outcome"] == "error"
        assert "workspace for job gonejob0001 not found" in entry["detail"]

    def test_the_report_can_be_redirected_or_declined(
        self, cli_state: Path
    ) -> None:
        _job("reportopt01", {"good.out": "good\n"})
        out = cli_state / "results"
        elsewhere = cli_state / "coverage.json"

        redirected = CliRunner().invoke(
            main, ["fetch-all", "localhost", "-o", str(out), "--report", str(elsewhere)]
        )
        assert redirected.exit_code == 0, redirected.output
        assert json.loads(elsewhere.read_text())["totals"]["fetched"] == 1
        assert not (out / "_vq" / "fetch-all-report.json").exists()

        declined = CliRunner().invoke(
            main, ["fetch-all", "localhost", "-o", str(out), "--report", "none"]
        )
        assert declined.exit_code == 0, declined.output
        assert not (out / "_vq" / "fetch-all-report.json").exists()


class TestSingleFetchReportsPartial:
    def test_vq_fetch_says_so_when_the_tree_is_incomplete(
        self, cli_state: Path
    ) -> None:
        _job("cliparti0001", {"good.out": "good\n", "secret.qvf": "secret\n"})
        (paths.jobs_dir() / "cliparti0001" / "secret.qvf").chmod(0o000)
        out = cli_state / "results"

        result = CliRunner().invoke(
            main, ["fetch", "localhost", "cliparti0001", "-o", str(out), "--workspace"]
        )

        assert result.exit_code == 0, result.output
        assert "PARTIAL: 1 entry could not be fetched" in result.output
        assert fetch.FETCH_EXCLUSIONS_SIDECAR in result.output

    def test_vq_fetch_json_carries_the_partial_flag(self, cli_state: Path) -> None:
        _job("cliparti0002", {"good.out": "good\n", "secret.qvf": "secret\n"})
        (paths.jobs_dir() / "cliparti0002" / "secret.qvf").chmod(0o000)
        out = cli_state / "results"

        result = CliRunner().invoke(
            main,
            [
                "fetch",
                "localhost",
                "cliparti0002",
                "-o",
                str(out),
                "--workspace",
                "--json",
            ],
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["partial"] is True
        assert payload["excluded_count"] == 1


@pytest.fixture(autouse=True)
def negotiated_fetch_protocol():
    """Bind synthetic current-emitter tar streams to their fixture request."""
    with pytest.MonkeyPatch.context() as protocol_patch:
        protocol_patch.setattr(fetch, "_new_fetch_protocol_token", lambda: "0" * 32)
        yield
