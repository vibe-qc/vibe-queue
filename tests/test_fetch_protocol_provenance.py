"""Negotiation must distinguish generated metadata from raw archive payload."""

from __future__ import annotations

import contextlib
import io
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from tests import test_fetch_metadata_collisions as fixtures
from vq import config, fetch, paths, transport
from vq.cli import main

isolated = fixtures.isolated
job = fixtures.job
snapshot = fixtures.snapshot


@pytest.mark.parametrize("kind", ["workspace", "workdir", "archive"])
def test_negotiated_current_cli_roundtrip(isolated, monkeypatch, kind):
    spec = job("protocolnew1", isolated)
    operation = fetch.fetch_remote
    if kind == "workdir":
        spec.workdir = spec.cwd
        operation = fetch.fetch_workdir_remote
    if kind == "archive":
        archive = isolated / "raw.tar"
        with tarfile.open(archive, "w") as tf:
            tf.add(spec.cwd, arcname=spec.id)
        spec.archived_at = "2026-09-17T10:00:00+00:00"
        spec.archive_path = str(archive)
    spec.write(paths.spec_path(spec.id))
    before = snapshot(Path(spec.cwd))
    tokens = []

    @contextlib.contextmanager
    def stream(host, *args):
        assert args[-2] == "--fetch-protocol-token"
        token = args[-1]
        assert len(token) == 32 and all(x in "0123456789abcdef" for x in token)
        tokens.append(token)
        result = CliRunner().invoke(main, list(args))
        assert result.exit_code == 0, result.stderr
        yield SimpleNamespace(stdout=io.BytesIO(result.stdout_bytes), stderr_text=result.stderr)

    monkeypatch.setattr(transport, "stream_remote_vq", stream)
    monkeypatch.setattr(fetch, "_send_remote_fetch_acknowledgement", lambda *a, **kw: None)
    for index in range(2):
        dst = operation(config.HostConfig(ssh="compute-a"), spec.id, isolated / str(index))
        assert (dst / "result.out").read_text() == "retained result\n"
        assert fetch.read_fetch_manifest(dst)["partial"] is False
    assert tokens[0] != tokens[1]
    assert snapshot(Path(spec.cwd)) == before


def wire(spec, marker, *, control_marker=None):
    out = io.BytesIO()
    headers = {} if marker is None else {"VQ.fetch-protocol.v1": marker}
    with tarfile.open(fileobj=out, mode="w", pax_headers=headers) as tf:
        root = tarfile.TarInfo(spec.id)
        root.type = tarfile.DIRTYPE
        tf.addfile(root)
        raw = (
            b'{"schema":"vq.fetch-exclusions.v1","excluded":[],"excluded_count":0,'
            b'"raw_job_payload":"ORIGINAL SCIENCE BYTES"}'
        )
        for name, data in [
            ("result.out", b"new result"),
            ("_vq/fetch-exclusions.json", raw),
            ("_vq/terminal-diagnosis.json", fetch._diagnosis_sidecar_bytes(spec)),
        ]:
            info = tarfile.TarInfo(spec.id + "/" + name)
            info.size = len(data)
            if control_marker is not None and name.startswith("_vq/"):
                info.pax_headers = {"VQ.fetch-protocol.v1": control_marker}
            tf.addfile(info, io.BytesIO(data))
    return out.getvalue()


@pytest.mark.parametrize("marker", [None, "", "malformed", "b" * 32])
def test_missing_malformed_or_replayed_response_preserves_previous_tree(
    isolated,
    monkeypatch,
    marker,
):
    spec = job("protocolbad1", isolated)
    out = isolated / "fetched"
    dst = fetch.fetch_local(spec.id, out)
    before = snapshot(dst)
    original = snapshot(Path(spec.cwd))
    raw = wire(spec, marker)
    monkeypatch.setattr(fetch, "_new_fetch_protocol_token", lambda: "a" * 32)

    @contextlib.contextmanager
    def stream(*args):
        yield SimpleNamespace(stdout=io.BytesIO(raw), stderr_text="")

    monkeypatch.setattr(transport, "stream_remote_vq", stream)
    with pytest.raises(ValueError, match="update the remote vq"):
        fetch.fetch_remote(config.HostConfig(ssh="compute-a"), spec.id, out)
    assert snapshot(dst) == before and snapshot(Path(spec.cwd)) == original
    assert not list(out.glob(".vq-fetch-*"))


def test_replayed_member_marker_cannot_override_current_response(isolated, monkeypatch):
    spec = job("memberproof1", isolated)
    raw = wire(spec, "a" * 32, control_marker="b" * 32)
    monkeypatch.setattr(fetch, "_new_fetch_protocol_token", lambda: "a" * 32)

    @contextlib.contextmanager
    def stream(*args):
        yield SimpleNamespace(stdout=io.BytesIO(raw), stderr_text="")

    monkeypatch.setattr(transport, "stream_remote_vq", stream)
    with pytest.raises(ValueError, match="non-protocol fetch sidecar"):
        fetch.fetch_remote(config.HostConfig(ssh="compute-a"), spec.id, isolated / "out")
    assert not (isolated / "out" / spec.id).exists()


@pytest.mark.parametrize("early_error", [False, True])
def test_old_cli_option_rejection_does_not_change_previous_destination(
    isolated,
    monkeypatch,
    early_error,
):
    spec = job("oldcli000001", isolated)
    out = isolated / "out"
    dst = fetch.fetch_local(spec.id, out)
    before = snapshot(dst)

    @contextlib.contextmanager
    def stream(*args):
        error = "Error: No such option: --fetch-protocol-token"
        if early_error:
            raise transport.RemoteError(error)
        yield SimpleNamespace(stdout=io.BytesIO(b""), stderr_text=error)

    monkeypatch.setattr(transport, "stream_remote_vq", stream)
    with pytest.raises(ValueError, match="update the remote vq"):
        fetch.fetch_remote(config.HostConfig(ssh="compute-a"), spec.id, out)
    assert snapshot(dst) == before


@pytest.mark.parametrize("verb", ["tar-workspace", "tar-workdir"])
@pytest.mark.parametrize("token", ["", "x" * 32, "A" * 32, "1" * 31, "1" * 33])
def test_cli_rejects_invalid_protocol_request_before_emitting(isolated, verb, token):
    result = CliRunner().invoke(main, [verb, "missingjob01", "--fetch-protocol-token", token])
    assert result.exit_code != 0 and not result.stdout_bytes
    assert "32 lowercase hexadecimal" in result.stderr


@pytest.fixture(autouse=True)
def no_remote_acknowledgement(monkeypatch):
    """Also contain mutants that proceed past a response-provenance refusal."""
    monkeypatch.setattr(fetch, "_send_remote_fetch_acknowledgement", lambda *a, **kw: None)
    monkeypatch.setattr(
        transport, "run_remote_vq",
        lambda *a, **kw: pytest.fail("unexpected remote operation in provenance fixture"),
    )
