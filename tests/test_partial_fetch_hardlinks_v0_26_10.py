"""A partial fetch must still produce a *valid* archive, and an honest count (#62).

Independent verification of `baff826` found two ways the partial-fetch
contract broke down on its own terms.

* **A hard link to an entry the emitter skipped.** A hard link member carries
  no data: it names an earlier member and the receiver resolves it there.
  Skipping the first of a hard-linked set — exactly what an unreadable
  ``.qvf`` under a multi-user root does — left the rest naming a member that
  is not in the archive. ``tarfile`` reports that as a bare ``KeyError``,
  which is neither ``FilterError`` nor ``TarError``, so it escaped every
  handler and took the whole fetch with it. Through ``vq fetch-all`` it took
  the rest of the sweep too, because ``bulk_fetch``'s per-job guard does not
  name ``KeyError``: the all-or-nothing loss #62 removed, reintroduced by its
  own fix and made worse.
* **The emitter's exclusion list is capped at
  ``FETCH_EXCLUSIONS_MAX_RECORDED``.** The receiver rebuilt only the records
  it could see, so "1500 lost, 1000 listed" was merged into an untruncated
  "1000 lost" — the receiving side publishing a smaller loss than the host it
  fetched from, in the one file that exists to state the loss.

The extraction filter is untouched here: nothing in this module makes a
refused member extractable.
"""
from __future__ import annotations

import io
import json
import os
import tarfile
from pathlib import Path

import pytest

from vq import fetch, paths
from vq.config import HostConfig
from vq.spec import JobSpec, JobState

pytestmark = pytest.mark.skipif(
    os.geteuid() == 0,
    reason="an unreadable file is still readable by root, so the mechanism "
    "cannot be reproduced; the suite must not run as root anyway",
)

HOST_CFG = HostConfig(ssh="host_h", remote_vq="vq")


@pytest.fixture
def state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    return tmp_path


def _job(jobid: str, files: dict[str, str]) -> JobSpec:
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
        state=JobState.COMPLETED,
    )
    spec.finished_at = "2026-09-17T10:00:00+00:00"
    spec.exit_code = 0
    spec.write(queue / f"{jobid}.json")
    return spec


def _hardlinked_partial_workspace(jobid: str) -> JobSpec:
    """A job whose unreadable output has a readable-looking hard-linked twin.

    The ``.qvf`` shape from #62, plus the hard link a mirrored or ``cp -al``
    tree brings with it. Both names are the same inode, so neither can be
    read: the archive must contain neither, and name both.
    """
    spec = _job(jobid, {"good.out": "good\n"})
    workspace = Path(spec.cwd)
    (workspace / "scan.qvf").write_text("payload\n")
    os.link(workspace / "scan.qvf", workspace / "scan-copy.qvf")
    (workspace / "scan.qvf").chmod(0o000)
    return spec


def _emit_workspace_tar_bytes(jobid: str, monkeypatch: pytest.MonkeyPatch) -> bytes:
    buf = io.BytesIO()

    class _FakeStdout:
        buffer = buf

    monkeypatch.setattr(fetch.sys, "stdout", _FakeStdout())
    fetch.emit_workspace_tar(jobid, protocol_token="0" * 32)
    raw = buf.getvalue()
    monkeypatch.undo()
    return raw


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


def _serve(monkeypatch: pytest.MonkeyPatch, tar_bytes: bytes) -> None:
    monkeypatch.setattr(
        fetch.transport.subprocess,
        "Popen",
        lambda *args, **kwargs: _FakeProc(tar_bytes),
    )
    monkeypatch.setattr(
        fetch, "_send_remote_fetch_acknowledgement", lambda *args, **kwargs: None
    )


def _members(raw: bytes) -> dict[str, tarfile.TarInfo]:
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r|") as tf:
        return {member.name: member for member in tf}


def _excluded(payload: dict[str, object]) -> dict[str, str]:
    excluded = payload["excluded"]
    assert isinstance(excluded, list)
    return {str(item["path"]): str(item["reason"]) for item in excluded}


# ----------------------------------------------------------------------
# The emitter must not ship a link it did not ship a target for.
# ----------------------------------------------------------------------


class TestEmitterNeverShipsADanglingHardLink:
    def test_every_link_member_names_a_member_that_is_in_the_archive(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The invariant. Pre-fix the archive named `scan*.qvf` as a link to
        a member the walk had skipped."""
        jobid = "hlemit000001"
        _hardlinked_partial_workspace(jobid)

        members = _members(_emit_workspace_tar_bytes(jobid, monkeypatch))

        for name, member in members.items():
            if member.islnk():
                assert member.linkname in members, (
                    f"{name} is a hard link to {member.linkname}, which is "
                    "not in the archive"
                )

    def test_both_names_of_the_unreadable_inode_are_listed(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A hole a consumer can see: neither name carries content."""
        jobid = "hlemit000002"
        _hardlinked_partial_workspace(jobid)

        raw = _emit_workspace_tar_bytes(jobid, monkeypatch)

        members = _members(raw)
        assert f"{jobid}/good.out" in members
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r|") as tf:
            payload = None
            for member in tf:
                if member.name == f"{jobid}/{fetch.FETCH_EXCLUSIONS_SIDECAR}":
                    handle = tf.extractfile(member)
                    assert handle is not None
                    payload = json.loads(handle.read().decode())
        assert payload is not None
        reasons = _excluded(payload)
        assert set(reasons) == {f"{jobid}/scan.qvf", f"{jobid}/scan-copy.qvf"}
        assert sorted(reasons.values()) == ["hardlink_target_excluded", "unreadable"]
        assert payload["excluded_count"] == 2

    def test_a_readable_hard_link_is_still_stored_as_a_link(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No regression: the fix must not turn hard links into copies."""
        jobid = "hlemit000003"
        spec = _job(jobid, {"a.out": "shared\n"})
        os.link(Path(spec.cwd) / "a.out", Path(spec.cwd) / "b.out")

        members = _members(_emit_workspace_tar_bytes(jobid, monkeypatch))

        kinds = {name: member.type for name, member in members.items()}
        assert kinds[f"{jobid}/a.out"] == tarfile.REGTYPE
        assert kinds[f"{jobid}/b.out"] == tarfile.LNKTYPE
        assert members[f"{jobid}/b.out"].linkname == f"{jobid}/a.out"


# ----------------------------------------------------------------------
# The receiver must survive one that was shipped anyway.
# ----------------------------------------------------------------------


class TestReceiverSurvivesAnUnresolvableLinkMember:
    def test_a_hardlinked_partial_workspace_fetches(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        jobid = "hlfetch00001"
        _hardlinked_partial_workspace(jobid)
        raw = _emit_workspace_tar_bytes(jobid, monkeypatch)
        _serve(monkeypatch, raw)

        dst = fetch.fetch_remote(HOST_CFG, jobid, state / "out")

        assert (dst / "good.out").read_text() == "good\n"
        payload = fetch.read_fetch_exclusions(dst)
        assert payload is not None
        assert payload["excluded_count"] == 2
        manifest = fetch.read_fetch_manifest(dst)
        assert manifest is not None
        assert manifest["partial"] is True

    def test_a_dangling_link_member_costs_the_member_not_the_archive(
        self, tmp_path: Path
    ) -> None:
        """A remote still running the emitter that shipped these (#62's own
        fix, before this one) must not cost the whole tree.

        ``tarfile`` reports an unresolvable link target as a bare
        ``KeyError``; pre-fix that escaped ``_extract_member_or_record``,
        ``_stream_extract_remote_tar``'s ``TarError`` handler and
        ``bulk_fetch``'s per-job guard alike.
        """
        archive = tmp_path / "workspace.tar"
        with tarfile.open(archive, mode="w") as tf:
            root = tarfile.TarInfo("job-x")
            root.type = tarfile.DIRTYPE
            root.mode = 0o755
            tf.addfile(root)
            link = tarfile.TarInfo("job-x/scan-copy.qvf")
            link.type = tarfile.LNKTYPE
            link.linkname = "job-x/scan.qvf"
            tf.addfile(link)
            data = b"good\n"
            info = tarfile.TarInfo("job-x/good.out")
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
        destination = tmp_path / "dest"
        destination.mkdir()
        exclusions: list[fetch.FetchExclusion] = []

        with tarfile.open(archive, mode="r:") as tf:
            for member in tf:
                fetch._extract_member_or_record(tf, member, destination, exclusions)

        assert (destination / "job-x" / "good.out").read_text() == "good\n"
        assert not (destination / "job-x" / "scan-copy.qvf").exists()
        assert [(item.path, item.reason) for item in exclusions] == [
            ("job-x/scan-copy.qvf", "unresolved_link_target")
        ]
        assert exclusions[0].stage == fetch.EXCLUSION_STAGE_EXTRACT

    def test_a_streamed_dangling_link_is_a_row_and_not_a_dead_sweep(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A host still emitting these stays one job's problem.

        On a non-seekable stream ``tarfile``'s own lookup can leave the
        iterator unable to continue, which is a ``TarError`` and so a per-job
        ``error`` row. What must never happen again is the sweep ending on
        the spot with the remaining jobs unfetched and no report written.
        """
        first = _job("hlstream0001", {"good.out": "good\n"})
        later = _job("hlstream0002", {"good.out": "good\n"})
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w|",
                      pax_headers={"VQ.fetch-protocol.v1": "0" * 32}) as tf:
            root = tarfile.TarInfo("hlstream0001")
            root.type = tarfile.DIRTYPE
            root.mode = 0o755
            tf.addfile(root)
            link = tarfile.TarInfo("hlstream0001/scan-copy.qvf")
            link.type = tarfile.LNKTYPE
            link.linkname = "hlstream0001/scan.qvf"
            tf.addfile(link)
            data = b"good\n"
            info = tarfile.TarInfo("hlstream0001/good.out")
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
        _serve(monkeypatch, buf.getvalue())
        out = state / "out"

        def fetch_one(jobid: str) -> Path:
            if jobid == "hlstream0001":
                return fetch.fetch_remote(HOST_CFG, jobid, out)
            (out / jobid).mkdir(parents=True, exist_ok=True)
            return out / jobid

        rows = fetch.bulk_fetch([first, later], states=None, fetch_one=fetch_one)

        assert [row.jobid for row in rows] == ["hlstream0001", "hlstream0002"]
        assert rows[1].outcome == "fetched"

    def test_the_sweep_moves_on_to_the_next_job(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`bulk_fetch` does not name KeyError, so pre-fix the whole sweep
        died on the first such job and the report was never written."""
        partial = _hardlinked_partial_workspace("hlsweep00001")
        later = _job("hlsweep00002", {"good.out": "good\n"})
        raw = _emit_workspace_tar_bytes("hlsweep00001", monkeypatch)
        _serve(monkeypatch, raw)
        out = state / "out"

        def fetch_one(jobid: str) -> Path:
            if jobid == "hlsweep00001":
                return fetch.fetch_remote(HOST_CFG, jobid, out)
            (out / jobid).mkdir(parents=True, exist_ok=True)
            return out / jobid

        rows = fetch.bulk_fetch([partial, later], states=None, fetch_one=fetch_one)

        assert [(row.jobid, row.outcome) for row in rows] == [
            ("hlsweep00001", "fetched"),
            ("hlsweep00002", "fetched"),
        ]
        assert rows[0].excluded_count == 2


# ----------------------------------------------------------------------
# The merged count must not shrink as it crosses the wire.
# ----------------------------------------------------------------------


def _remote_tar_with_exclusions(jobid: str, payload: dict[str, object]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w|",
                      pax_headers={"VQ.fetch-protocol.v1": "0" * 32}) as tf:
        root = tarfile.TarInfo(jobid)
        root.type = tarfile.DIRTYPE
        root.mode = 0o755
        tf.addfile(root)
        for relpath, data in (
            ("good.out", b"good\n"),
            (
                fetch.TERMINAL_DIAGNOSIS_SIDECAR,
                json.dumps(
                    {
                        "schema": "vq.terminal-diagnosis.v1",
                        "jobid": jobid,
                        "submitted_at": "2026-09-17T10:11:12+00:00",
                        "state": "completed",
                    }
                ).encode(),
            ),
            (fetch.FETCH_EXCLUSIONS_SIDECAR, json.dumps(payload).encode()),
        ):
            info = tarfile.TarInfo(f"{jobid}/{relpath}")
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
    return buf.getvalue()


class TestTheMergedCountKeepsTheRemoteTotal:
    def test_a_capped_remote_list_does_not_become_a_smaller_complete_one(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        jobid = "hlcount00001"
        over_cap = fetch.FETCH_EXCLUSIONS_MAX_RECORDED + 500
        remote = fetch._exclusions_payload(
            [
                fetch.FetchExclusion(
                    f"{jobid}/f{index}.qvf",
                    fetch.EXCLUSION_STAGE_ARCHIVE,
                    "unreadable",
                    "PermissionError: Permission denied",
                )
                for index in range(over_cap)
            ],
            jobid=jobid,
        )
        assert remote["excluded_count"] == over_cap
        assert remote["truncated"] is True
        _serve(monkeypatch, _remote_tar_with_exclusions(jobid, remote))

        dst = fetch.fetch_remote(HOST_CFG, jobid, state / "out")

        payload = fetch.read_fetch_exclusions(dst)
        assert payload is not None
        assert payload["excluded_count"] == over_cap
        assert payload["recorded_count"] == fetch.FETCH_EXCLUSIONS_MAX_RECORDED
        assert payload["truncated"] is True
        assert fetch.excluded_count_for_destination(dst) == over_cap
        manifest = fetch.read_fetch_manifest(dst)
        assert manifest is not None
        assert manifest["excluded_count"] == over_cap

    def test_a_remote_count_smaller_than_its_own_list_never_shrinks_the_total(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The count is a claim from another host: it may raise the total
        above what we can list, never lower it below what we did list."""
        jobid = "hlcount00002"
        _serve(
            monkeypatch,
            _remote_tar_with_exclusions(
                jobid,
                {
                    "schema": fetch.FETCH_EXCLUSIONS_SCHEMA,
                    "jobid": jobid,
                    "excluded_count": 0,
                    "excluded": [
                        {
                            "path": f"{jobid}/a.qvf",
                            "stage": fetch.EXCLUSION_STAGE_ARCHIVE,
                            "reason": "unreadable",
                            "detail": "PermissionError: Permission denied",
                        },
                        {
                            "path": f"{jobid}/b.qvf",
                            "stage": fetch.EXCLUSION_STAGE_ARCHIVE,
                            "reason": "unreadable",
                            "detail": "PermissionError: Permission denied",
                        },
                    ],
                },
            ),
        )

        dst = fetch.fetch_remote(HOST_CFG, jobid, state / "out")

        payload = fetch.read_fetch_exclusions(dst)
        assert payload is not None
        assert payload["excluded_count"] == 2
        assert payload["truncated"] is False

    def test_a_missing_remote_count_is_not_read_as_a_number(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        jobid = "hlcount00003"
        _serve(
            monkeypatch,
            _remote_tar_with_exclusions(
                jobid,
                {
                    "schema": fetch.FETCH_EXCLUSIONS_SCHEMA,
                    "jobid": jobid,
                    "excluded_count": "lots",
                    "excluded": [
                        {
                            "path": f"{jobid}/a.qvf",
                            "stage": fetch.EXCLUSION_STAGE_ARCHIVE,
                            "reason": "unreadable",
                            "detail": "PermissionError: Permission denied",
                        }
                    ],
                },
            ),
        )

        dst = fetch.fetch_remote(HOST_CFG, jobid, state / "out")

        payload = fetch.read_fetch_exclusions(dst)
        assert payload is not None
        assert payload["excluded_count"] == 1


@pytest.fixture(autouse=True)
def negotiated_fetch_protocol():
    """Bind synthetic current-emitter tar streams to their fixture request."""
    with pytest.MonkeyPatch.context() as protocol_patch:
        protocol_patch.setattr(fetch, "_new_fetch_protocol_token", lambda: "0" * 32)
        yield
