"""Fetch coverage needs a typed skip and an unambiguous metadata namespace (#80)."""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from vq import config, fetch, paths, transport
from vq.cli import main
from vq.spec import JobSpec, JobState


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "config"))
    (tmp_path / "config").mkdir()
    (tmp_path / "config/config.toml").write_text('default_host = "localhost"\n')
    return tmp_path


def job(jobid, root):
    workspace = root / jobid
    workspace.mkdir()
    (workspace / "result.out").write_text("retained result\n")
    spec = JobSpec(
        id=jobid,
        command=["true"],
        cwd=str(workspace),
        cpus=1,
        submitter="test_user@test",
        state=JobState.COMPLETED,
    )
    spec.finished_at = "2026-09-17T10:00:00+00:00"
    spec.exit_code = 0
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    spec.write(paths.spec_path(jobid))
    return spec


def snapshot(root):
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in root.rglob("*")
        if p.is_file()
    }


def put(root, relative, data=b"original user bytes\n"):
    target = root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)


def tar_bytes(root, members):
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w",
                      pax_headers={"VQ.fetch-protocol.v1": "0" * 32}) as tf:
        base = tarfile.TarInfo(root)
        base.type = tarfile.DIRTYPE
        tf.addfile(base)
        for name, kind, data in members:
            info = tarfile.TarInfo(root + "/" + name)
            if kind == "file":
                info.size = len(data)
                tf.addfile(info, io.BytesIO(data))
            else:
                info.type = tarfile.SYMTYPE if kind == "link" else tarfile.DIRTYPE
                if kind == "link":
                    info.linkname = data
                tf.addfile(info)
    return out.getvalue()


def remote_stream(monkeypatch, raw):
    @contextlib.contextmanager
    def stream(*args):
        yield SimpleNamespace(stdout=io.BytesIO(raw), stderr_text="")

    monkeypatch.setattr(transport, "stream_remote_vq", stream)
    monkeypatch.setattr(fetch, "_send_remote_fetch_acknowledgement", lambda *a, **k: None)
    return config.HostConfig(ssh="compute-a")


def test_bulk_collision_is_error_report_and_later_job_still_lands(isolated):
    bad = job("collision001", isolated)
    good = job("following002", isolated)
    put(Path(bad.cwd), "_vq")
    before = snapshot(Path(bad.cwd))
    out = isolated / "fetched"
    result = CliRunner().invoke(main, ["fetch-all", "localhost", "-o", str(out)])
    assert result.exit_code != 0, result.output
    report = json.loads((out / "_vq/fetch-all-report.json").read_bytes())
    rows = {row["jobid"]: row for row in report["jobs"]}
    assert report["totals"]["failed"] == 1 and report["totals"]["skipped"] == 0
    assert rows[bad.id]["outcome"] == "error" and "_vq" in rows[bad.id]["detail"]
    assert rows[good.id]["outcome"] == "fetched"
    assert not (out / bad.dest_dirname).exists()
    assert (out / good.dest_dirname / "result.out").read_text() == "retained result\n"
    assert snapshot(Path(bad.cwd)) == before


@pytest.mark.parametrize(
    "relative",
    ["_vq/terminal-diagnosis.json", "_vq/fetch-exclusions.json", "_vq/fetch-manifest.json"],
)
@pytest.mark.parametrize("workdir", [False, True])
def test_raw_control_collision_preserves_source_and_existing_destination(
    isolated,
    relative,
    workdir,
):
    spec = job("metadata0001", isolated)
    root = Path(spec.cwd)
    if workdir:
        spec.workdir = str(root)
        spec.write(paths.spec_path(spec.id))
    operation = fetch.fetch_workdir_local if workdir else fetch.fetch_local
    out = isolated / "fetched"
    dst = operation(spec.id, out)
    before_dst = snapshot(dst)
    put(root, relative)
    put(root, "result.out", b"new result\n")
    before_src = snapshot(root)
    with pytest.raises(ValueError, match="fetch metadata collision"):
        operation(spec.id, out)
    assert snapshot(root) == before_src and snapshot(dst) == before_dst
    assert not list(out.glob(".vq-fetch-*"))


def test_lifecycle_metadata_and_genuine_idempotent_skip_remain_valid(isolated):
    spec = job("lifecycle001", isolated)
    root = Path(spec.cwd)
    for rel in ["_vq/events.jsonl", "_vq/resource-usage.json", "_vq/exit-code"]:
        put(root, rel)
    out = isolated / "fetched"
    dst = fetch.fetch_local(spec.id, out)
    for rel in ["_vq/events.jsonl", "_vq/resource-usage.json", "_vq/exit-code"]:
        assert (dst / rel).read_bytes() == (root / rel).read_bytes()
    before = snapshot(dst)
    with pytest.raises(FileExistsError):
        fetch.fetch_local(spec.id, out, idempotent=False)
    rows = fetch.bulk_fetch(
        [spec], states=None, fetch_one=lambda jid: fetch.fetch_local(jid, out, idempotent=False)
    )
    assert rows[0].outcome == "skipped" and snapshot(dst) == before


def test_bare_destination_collision_is_not_a_completed_fetch(isolated):
    spec = job("bare00000001", isolated)
    out = isolated / "fetched"
    put(out / spec.dest_dirname, "private.txt", b"keep destination\n")
    before = snapshot(out)
    rows = fetch.bulk_fetch(
        [spec], states=None, fetch_one=lambda jid: fetch.fetch_local(jid, out, idempotent=False)
    )
    assert rows[0].outcome == "error" and snapshot(out) == before


@pytest.mark.parametrize(
    "error",
    [
        FileExistsError("incidental filesystem collision"),
        PermissionError("denied"),
        OSError("disk full"),
        tarfile.ReadError("invalid archive"),
    ],
)
def test_ordinary_io_and_tar_errors_do_not_abort_later_jobs(isolated, error):
    first = job("first0000001", isolated)
    second = job("second000002", isolated)
    out = isolated / "fetched"

    def one(jid):
        if jid == first.id:
            raise error
        return fetch.fetch_local(jid, out)

    rows = fetch.bulk_fetch([first, second], states=None, fetch_one=one)
    assert [row.outcome for row in rows] == ["error", "fetched"]
    assert str(error) in rows[0].detail


def test_programming_error_is_not_hidden_as_an_ordinary_job_error(isolated):
    spec = job("program00001", isolated)

    def broken(_jid):
        raise RuntimeError("programming defect")

    with pytest.raises(RuntimeError, match="programming defect"):
        fetch.bulk_fetch([spec], states=None, fetch_one=broken)


@pytest.mark.parametrize(
    "relative",
    ["_vq", "_vq/terminal-diagnosis.json", "_vq/fetch-exclusions.json", "_vq/fetch-manifest.json"],
)
def test_new_emitter_refuses_raw_namespace_collision(isolated, monkeypatch, relative):
    spec = job("emitter00001", isolated)
    put(Path(spec.cwd), relative)
    original = snapshot(Path(spec.cwd))
    output = io.BytesIO()
    monkeypatch.setattr(fetch.sys, "stdout", SimpleNamespace(buffer=output))
    with pytest.raises(ValueError, match="fetch metadata collision"):
        fetch.emit_workspace_tar(spec.id)
    assert snapshot(Path(spec.cwd)) == original


@pytest.mark.parametrize(
    "name,kind,payload",
    [
        ("_vq", "file", b"original"),
        ("_vq", "link", "payload"),
        ("_vq/fetch-manifest.json", "file", b"original"),
        ("_vq/fetch-exclusions.json", "link", "../result.out"),
        ("a/../_vq/fetch-exclusions.json", "file", b"original"),
        ("_vq/terminal-diagnosis.json", "dir", b""),
    ],
)
def test_remote_collisions_are_errors_before_promotion(
    isolated,
    monkeypatch,
    name,
    kind,
    payload,
):
    spec = job("remote000001", isolated)
    members = [("result.out", "file", b"payload"), (name, kind, payload)]
    host = remote_stream(monkeypatch, tar_bytes(spec.id, members))
    out = isolated / "fetched"
    with pytest.raises(ValueError, match="fetch metadata collision"):
        fetch.fetch_remote(host, spec.id, out)
    assert not (out / spec.id).exists() and not list(out.glob(".vq-fetch-*"))


@pytest.mark.parametrize("relative", ["_vq/terminal-diagnosis.json", "_vq/fetch-exclusions.json"])
def test_remote_duplicate_controls_cannot_overwrite_original_bytes(isolated, monkeypatch, relative):
    spec = job("duplicate001", isolated)
    raw = tar_bytes(
        spec.id,
        [
            ("result.out", "file", b"payload"),
            (relative, "file", b"original user bytes"),
            (
                relative,
                "file",
                fetch._diagnosis_sidecar_bytes(spec)
                if "diagnosis" in relative
                else fetch._exclusions_bytes(fetch._exclusions_payload([], jobid=spec.id)),
            ),
        ],
    )
    host = remote_stream(monkeypatch, raw)
    out = isolated / "fetched"
    with pytest.raises(ValueError, match="fetch metadata collision"):
        fetch.fetch_remote(host, spec.id, out)
    assert not (out / spec.id).exists()


def test_remote_alias_payload_cannot_be_overwritten_by_protocol_metadata(isolated, monkeypatch):
    spec = job("aliased00001", isolated)
    raw = tar_bytes(
        spec.id,
        [
            ("_vq", "dir", b""),
            ("alias", "link", "_vq"),
            ("alias/fetch-exclusions.json", "file", b"original user bytes"),
            ("_vq/terminal-diagnosis.json", "file", fetch._diagnosis_sidecar_bytes(spec)),
        ],
    )
    host = remote_stream(monkeypatch, raw)
    with pytest.raises(ValueError, match="fetch metadata collision"):
        fetch.fetch_remote(host, spec.id, isolated / "fetched")
    assert not (isolated / "fetched" / spec.id).exists()


def test_remote_protocol_sidecars_and_lifecycle_files_still_fetch(isolated, monkeypatch):
    spec = job("protocol0001", isolated)
    members = [
        ("result.out", "file", b"result"),
        ("_vq/events.jsonl", "file", b"event\n"),
        ("_vq/terminal-diagnosis.json", "file", fetch._diagnosis_sidecar_bytes(spec)),
        (
            "_vq/fetch-exclusions.json",
            "file",
            fetch._exclusions_bytes(fetch._exclusions_payload([], jobid=spec.id)),
        ),
    ]
    host = remote_stream(monkeypatch, tar_bytes(spec.id, members))
    dst = fetch.fetch_remote(host, spec.id, isolated / "fetched")
    assert (dst / "result.out").read_bytes() == b"result"
    assert (dst / "_vq/events.jsonl").read_bytes() == b"event\n"
    assert fetch.read_fetch_manifest(dst)["partial"] is False
    with pytest.raises(FileExistsError):
        fetch.fetch_remote(host, spec.id, isolated / "fetched", idempotent=False)


@pytest.mark.parametrize(
    "relative", ["_vq", "_vq/fetch-manifest.json", "_vq/terminal-diagnosis.json"]
)
def test_raw_archive_collision_preserves_archive_and_previous_fetch(isolated, relative):
    spec = job("archive00001", isolated)
    out = isolated / "fetched"
    dst = fetch.fetch_local(spec.id, out)
    before = snapshot(dst)
    archive = isolated / "payload.tar.bz2"
    with tarfile.open(archive, "w:bz2") as tf:
        raw = tar_bytes(
            spec.id,
            [("result.out", "file", b"new result"), (relative, "file", b"original archive bytes")],
        )
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as source:
            for member in source:
                tf.addfile(member, source.extractfile(member) if member.isfile() else None)
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    spec.archived_at = "2026-09-17T10:01:00+00:00"
    spec.archive_path = str(archive)
    spec.write(paths.spec_path(spec.id))
    with pytest.raises(ValueError, match="fetch metadata collision"):
        fetch.fetch_local(spec.id, out)
    assert snapshot(dst) == before
    assert hashlib.sha256(archive.read_bytes()).hexdigest() == digest


def test_staged_recheck_catches_content_arriving_during_copy(isolated, monkeypatch):
    spec = job("racing000001", isolated)
    original = fetch.shutil.copytree

    def copy(src, dst, *args, **kwargs):
        result = original(src, dst, *args, **kwargs)
        put(Path(dst), "_vq/fetch-manifest.json")
        return result

    monkeypatch.setattr(fetch.shutil, "copytree", copy)
    with pytest.raises(ValueError, match="fetch metadata collision"):
        fetch.fetch_local(spec.id, isolated / "fetched")
    assert not (isolated / "fetched" / spec.id).exists()


@pytest.mark.parametrize("workdir", [False, True])
def test_genuine_same_job_skip_uses_a_positive_signal(isolated, workdir):
    spec = job("idempotent01", isolated)
    if workdir:
        spec.workdir = spec.cwd
        spec.write(paths.spec_path(spec.id))
    operation = fetch.fetch_workdir_local if workdir else fetch.fetch_local
    out = isolated / "fetched"
    dst = operation(spec.id, out)
    before = snapshot(dst)
    rows = fetch.bulk_fetch(
        [spec], states=None, fetch_one=lambda jid: operation(jid, out, idempotent=False)
    )
    assert rows[0].outcome == "skipped" and snapshot(dst) == before


def test_corrupt_local_archive_is_reported_and_sweep_continues(isolated):
    broken = job("corrupt00001", isolated)
    good = job("intact000002", isolated)
    archive = isolated / "broken.tar.bz2"
    archive.write_bytes(b"retained corrupt archive")
    broken.archived_at = "2026-09-17T10:01:00+00:00"
    broken.archive_path = str(archive)
    broken.write(paths.spec_path(broken.id))
    out = isolated / "fetched"
    # The CLI deliberately omits archived jobs. A caller that explicitly
    # supplies one to bulk_fetch still gets per-job archive error isolation.
    rows = fetch.bulk_fetch(
        [broken, good],
        states=None,
        fetch_one=lambda jid: fetch.fetch_local(jid, out),
    )
    assert [row.outcome for row in rows] == ["error", "fetched"]
    assert (out / good.id / "result.out").read_text() == "retained result\n"
    assert archive.read_bytes() == b"retained corrupt archive"


@pytest.mark.parametrize(
    "relative",
    ["_vq", "_vq/terminal-diagnosis.json", "_vq/fetch-exclusions.json", "_vq/fetch-manifest.json"],
)
def test_archived_emitter_refuses_raw_sidecars_and_preserves_archive(
    isolated,
    monkeypatch,
    relative,
):
    spec = job("emitarc00001", isolated)
    archive = isolated / "original.tar"
    raw = tar_bytes(
        spec.id, [("result.out", "file", b"payload"), (relative, "file", b"original user bytes")]
    )
    archive.write_bytes(raw)
    spec.archived_at = "2026-09-17T10:01:00+00:00"
    spec.archive_path = str(archive)
    spec.write(paths.spec_path(spec.id))
    output = io.BytesIO()
    monkeypatch.setattr(fetch.sys, "stdout", SimpleNamespace(buffer=output))
    with pytest.raises(ValueError, match="fetch metadata collision"):
        fetch.emit_workspace_tar(spec.id)
    assert archive.read_bytes() == raw


def test_remote_collision_preserves_existing_destination_and_continues(isolated, monkeypatch):
    spec = job("remotekeep01", isolated)
    next_spec = job("remoteafter2", isolated)
    members = [
        ("result.out", "file", b"old result"),
        ("_vq/terminal-diagnosis.json", "file", fetch._diagnosis_sidecar_bytes(spec)),
    ]
    host = remote_stream(monkeypatch, tar_bytes(spec.id, members))
    out = isolated / "fetched"
    dst = fetch.fetch_remote(host, spec.id, out)
    before = snapshot(dst)
    collision = [
        ("result.out", "file", b"new result"),
        ("_vq/fetch-exclusions.json", "file", b"raw payload"),
        ("_vq/fetch-exclusions.json", "file", b"{}"),
        ("_vq/terminal-diagnosis.json", "file", fetch._diagnosis_sidecar_bytes(spec)),
    ]
    remote_stream(monkeypatch, tar_bytes(spec.id, collision))

    def one(jid):
        if jid == spec.id:
            return fetch.fetch_remote(host, jid, out)
        return fetch.fetch_local(jid, out)

    rows = fetch.bulk_fetch([spec, next_spec], states=None, fetch_one=one)
    assert [r.outcome for r in rows] == ["error", "fetched"]
    assert "_vq/fetch-exclusions.json" in rows[0].detail
    assert snapshot(dst) == before
    assert (out / next_spec.id / "result.out").read_text() == "retained result\n"


@pytest.fixture(autouse=True)
def negotiated_fetch_protocol():
    """Bind synthetic current-emitter tar streams to their fixture request."""
    with pytest.MonkeyPatch.context() as protocol_patch:
        protocol_patch.setattr(fetch, "_new_fetch_protocol_token", lambda: "0" * 32)
        yield
