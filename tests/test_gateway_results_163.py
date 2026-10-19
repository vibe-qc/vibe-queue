"""Gateway slice 4 (#163): result collection, manifest, downloads and retention."""
from __future__ import annotations

import contextlib
import hashlib
import io
import json
import subprocess
import tarfile
from datetime import timedelta
from types import SimpleNamespace

import pytest

from tests.test_gateway_logs_cancel_163 import _advance, _submitted
from tests.test_gateway_submit_163 import _job, _project, _tar, _upload
from tests.test_gateway_submit_163 import gateway as gateway  # noqa: F401 — fixture
from vq import transport
from vq.gateway import results as gw_results
from vq.gateway import uploads as gw_uploads


def _workspace(files: dict[str, bytes], *, links=(), unsafe=()) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
        for name in links:
            info = tarfile.TarInfo(name)
            info.type = tarfile.SYMTYPE
            info.linkname = "/etc/passwd"
            archive.addfile(info)
        for name in unsafe:
            info = tarfile.TarInfo(name)
            info.size = 1
            archive.addfile(info, io.BytesIO(b"x"))
    return buffer.getvalue()


@pytest.fixture
def target(monkeypatch):
    state = {"status": "running", "tar": b"", "streams": 0}

    def run_vq(host_cfg, *argv, **kwargs):
        assert argv[0] == "status"
        return subprocess.CompletedProcess(argv, 0, stdout=json.dumps({"state": state["status"]}))

    @contextlib.contextmanager
    def stream_vq(host_cfg, *argv):
        assert argv[0] == "tar-workspace"
        state["streams"] += 1
        yield SimpleNamespace(stdout=io.BytesIO(state["tar"]))

    monkeypatch.setattr(transport, "run_remote_vq", run_vq)
    monkeypatch.setattr(transport, "stream_remote_vq", stream_vq)
    return state


def _placed(gateway_fixture, scopes=("submit", "read", "cancel", "download")):
    client, headers, job_id, root = _submitted(gateway_fixture, scopes=scopes)
    _advance(root, job_id, "placed")
    return client, headers, job_id


def test_results_wait_for_placement_and_completion(gateway, target) -> None:
    client, headers, job_id, _root = _submitted(gateway, scopes=("submit", "read", "download"))
    held = client.get(f"/v1/jobs/{job_id}/results", headers=headers).json()
    assert held["ready"] is False and held["state"] == "held" and target["streams"] == 0


def test_a_running_job_is_not_collected_yet(gateway, target) -> None:
    client, headers, job_id = _placed(gateway)
    running = client.get(f"/v1/jobs/{job_id}/results", headers=headers).json()
    assert running["ready"] is False and "not finished" in running["detail"]
    assert target["streams"] == 0


def test_a_finished_job_is_collected_once_and_downloaded_by_digest(gateway, target) -> None:
    client, headers, job_id = _placed(gateway)
    target["status"] = "completed"
    target["tar"] = _workspace({"out.txt": b"energy -76.0\n", "sub/data.bin": b"\x00\x01"},
                               links=["link"], unsafe=["../escape"])

    first = client.get(f"/v1/jobs/{job_id}/results", headers=headers)
    assert first.status_code == 200, first.text
    manifest = first.json()
    assert manifest["ready"] is True and manifest["target_state"] == "completed"
    assert {item["path"] for item in manifest["files"]} == {"out.txt", "sub/data.bin"}
    assert set(manifest["skipped"]) == {"link", "../escape"}
    again = client.get(f"/v1/jobs/{job_id}/results", headers=headers).json()
    assert again["collected_at"] == manifest["collected_at"] and target["streams"] == 1

    got = client.get(f"/v1/jobs/{job_id}/results/out.txt", headers=headers)
    assert got.content == b"energy -76.0\n"
    assert got.headers["x-content-sha256"] == hashlib.sha256(got.content).hexdigest()
    nested = client.get(f"/v1/jobs/{job_id}/results/sub/data.bin", headers=headers)
    assert nested.content == b"\x00\x01"
    for missing in ("nope.txt", "link", "../escape"):
        assert client.get(f"/v1/jobs/{job_id}/results/{missing}", headers=headers
                          ).status_code == 404


def test_outputs_beyond_the_quota_are_refused(gateway, target) -> None:
    client, headers, job_id = _placed(gateway)
    target["status"] = "completed"
    target["tar"] = _workspace({"big.bin": b"x" * (2 * 1024 * 1024)})
    refused = client.get(f"/v1/jobs/{job_id}/results", headers=headers)
    assert refused.status_code == 413
    assert client.get(f"/v1/jobs/{job_id}/results/big.bin", headers=headers).status_code == 409


def test_downloads_are_owned_and_need_the_download_scope(gateway, target) -> None:
    client, headers, job_id = _placed(gateway)
    target["status"] = "completed"
    target["tar"] = _workspace({"out.txt": b"ok"})
    assert client.get(f"/v1/jobs/{job_id}/results", headers=headers).status_code == 200
    _client, key, _state, _broker = gateway
    bob = key("bob", scopes=("read", "download"))
    assert client.get(f"/v1/jobs/{job_id}/results", headers=bob).status_code == 404
    assert client.get(f"/v1/jobs/{job_id}/results/out.txt", headers=bob).status_code == 404
    reader = key("alice", scopes=("read",))
    assert client.get(f"/v1/jobs/{job_id}/results", headers=reader).status_code == 403


def test_a_placed_job_counts_as_active_until_collected(gateway, target) -> None:
    client, key, state, _broker = gateway
    state["project"] = _project(max_active_jobs=1)
    client, headers, job_id = _placed(gateway)
    upload_id = _upload(client, headers, _tar())
    blocked = client.post("/v1/jobs", headers=headers,
                          json=_job(upload_id, idempotency_key="run-0002"))
    assert blocked.status_code == 429
    target["status"] = "completed"
    target["tar"] = _workspace({"out.txt": b"ok"})
    assert client.get(f"/v1/jobs/{job_id}/results", headers=headers).json()["ready"]
    allowed = client.post("/v1/jobs", headers=headers,
                          json=_job(upload_id, idempotency_key="run-0002"))
    assert allowed.status_code == 202, allowed.text


def test_retention_prunes_old_results_and_uploads(gateway, target, tmp_path, monkeypatch) -> None:
    client, headers, job_id = _placed(gateway)
    target["status"] = "completed"
    target["tar"] = _workspace({"out.txt": b"ok"})
    client.get(f"/v1/jobs/{job_id}/results", headers=headers)
    store = gw_results.ResultStore(tmp_path / "gateway")
    uploads = gw_uploads.UploadStore(tmp_path / "gateway")
    assert store.prune({job_id: 14}, dry_run=False) == []
    later = gw_results._now() + timedelta(days=15)
    monkeypatch.setattr(gw_results, "_now", lambda: later)
    monkeypatch.setattr(gw_uploads, "_now", lambda: later)
    assert store.prune({job_id: 14}, dry_run=True) == [job_id]
    assert store.manifest(job_id) is not None
    assert store.prune({job_id: 14}, dry_run=False) == [job_id]
    assert store.manifest(job_id) is None
    assert len(gw_uploads.prune_uploads(uploads, {"group-a": 14}, dry_run=False)) == 1


def test_prune_cli_reports_what_it_would_remove(tmp_path, monkeypatch) -> None:
    from click.testing import CliRunner

    from vq import config, paths
    from vq.cli import main as cli

    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setattr(config, "load_config", lambda: config.Config(
        gateway=config.GatewayConfig(projects={"group-a": _project()})))
    result = CliRunner().invoke(cli, ["gateway", "prune", "--dry-run"])
    assert result.exit_code == 0, result.output
    assert "would remove 0 result set(s) and 0 upload(s)" in result.output
