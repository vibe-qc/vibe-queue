"""Gateway slice 5 (#163): the client, end to end against the real app.

Acceptance criterion 1: a client with no fleet inventory or SSH key submits a
calculation, survives dropped responses, follows logs and retrieves
byte-verified outputs. Requests go through the real gateway app; only the
broker's SSH transport to the target is faked.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest
from click.testing import CliRunner

from tests.test_gateway_logs_cancel_163 import _advance
from tests.test_gateway_logs_cancel_163 import remote as remote  # noqa: F401 — fixture
from tests.test_gateway_results_163 import _workspace
from tests.test_gateway_results_163 import target as target  # noqa: F401 — fixture
from tests.test_gateway_submit_163 import gateway as gateway  # noqa: F401 — fixture
from vq import broker_store
from vq.gateway import client as gw_client

BASE = "https://gateway.example"


class Adapter:
    """Route client requests into the TestClient; optionally drop responses."""

    def __init__(self, test_client) -> None:
        self.test_client = test_client
        self.drop: list[tuple[str, str]] = []
        self.tamper: dict[str, bytes] = {}
        self.calls: list[tuple[str, str]] = []

    def __call__(self, method, url, headers, body):
        path = url.removeprefix(BASE)
        self.calls.append((method, path.split("?")[0]))
        response = self.test_client.request(method, path, headers=headers, content=body)
        for index, (drop_method, prefix) in enumerate(self.drop):
            if method == drop_method and path.startswith(prefix):
                del self.drop[index]
                raise ConnectionResetError("response lost after the gateway acted")
        data = self.tamper.get(path, response.content)
        return response.status_code, dict(response.headers), data


def _credentials(tmp_path: Path, secret: str, *, url: str = BASE, mode: int = 0o600) -> Path:
    path = tmp_path / "creds.toml"
    path.write_text(f'url = "{url}"\nkey = "{secret}"\n')
    os.chmod(path, mode)
    return path


@pytest.fixture
def connected(gateway, tmp_path, monkeypatch):
    test_client, key, _state, broker_root = gateway
    headers = key("alice", scopes=("submit", "read", "cancel", "download"))
    secret = headers["Authorization"].removeprefix("Bearer ")
    adapter = Adapter(test_client)
    creds = gw_client.load_credentials(_credentials(tmp_path, secret))
    client = gw_client.GatewayClient(creds, send=adapter, sleep=lambda _s: None)
    payload = tmp_path / "payload"
    payload.mkdir()
    (payload / "run.py").write_text("print('energy')\n" * 200)
    monkeypatch.setattr(gw_client, "CHUNK_BYTES", 1024)
    return client, adapter, payload, broker_root, secret


def _request(upload_id: str, key: str = "paper-run-0001") -> dict:
    return {"upload_id": upload_id, "command": ["python3", "run.py"],
            "program": "vibeqc-release", "cpus": 2, "mem_mb": 1024,
            "wall_time_seconds": 600, "idempotency_key": key}


@pytest.mark.parametrize("mode,url,match", [
    (0o640, BASE, "readable only by you"),
    (0o600, "http://gateway.example", "plain http"),
    (0o600, "ftp://x", "https://"),
])
def test_credentials_are_private_and_never_sent_in_clear(tmp_path, mode, url, match):
    with pytest.raises(gw_client.ClientError, match=match):
        gw_client.load_credentials(_credentials(tmp_path, "vqk_x", url=url, mode=mode))
    loopback = gw_client.load_credentials(
        _credentials(tmp_path, "vqk_x", url="http://127.0.0.1:8770"))
    assert loopback.url == "http://127.0.0.1:8770"


def test_submit_survives_dropped_responses_without_a_second_job(connected) -> None:
    client, adapter, payload, broker_root, _secret = connected
    archive, temporary = gw_client.pack(payload)
    adapter.drop = [("PUT", "/v1/uploads/"), ("POST", "/v1/jobs")]
    try:
        upload_id = client.upload(archive)
    finally:
        if temporary:
            archive.unlink()
    job = client.submit(_request(upload_id))
    assert job["replayed"] is True  # the first response was lost after admission
    again = client.submit(_request(upload_id))
    assert again["job_id"] == job["job_id"]
    with broker_store.BrokerStore.open(broker_root) as store:
        records, _problems = store.scan()
    assert [r.broker_job_id for r in records] == [job["job_id"]]
    assert ("GET", f"/v1/uploads/{upload_id}") in adapter.calls  # resumed from the gateway


def test_logs_and_byte_verified_fetch(connected, target, tmp_path) -> None:
    client, adapter, payload, broker_root, _secret = connected
    archive, _temporary = gw_client.pack(payload)
    job_id = client.submit(_request(client.upload(archive)))["job_id"]
    _advance(broker_root, job_id, "placed")
    target["status"] = "completed"
    target["tar"] = _workspace({"out.txt": b"E = -76.02\n", "sub/a.dat": b"\x01\x02"})

    dest = tmp_path / "results"
    assert sorted(client.fetch(job_id, dest)) == ["out.txt", "sub/a.dat"]
    assert (dest / "out.txt").read_bytes() == b"E = -76.02\n"
    with pytest.raises(gw_client.ClientError, match="exists"):
        client.fetch(job_id, dest)
    adapter.tamper[f"/v1/jobs/{job_id}/results/out.txt"] = b"forged"
    with pytest.raises(gw_client.ClientError, match="digest"):
        client.fetch(job_id, dest, force=True)


def test_logs_follow_until_results_are_ready(connected, target, remote, tmp_path,
                                             monkeypatch) -> None:
    client, _adapter, payload, broker_root, secret = connected
    archive, _temporary = gw_client.pack(payload)
    job_id = client.submit(_request(client.upload(archive)))["job_id"]
    _advance(broker_root, job_id, "placed")
    _calls, replies = remote
    replies["logs"] = {"--stdout": "step 1\nstep 2\n"}
    target["status"] = "completed"
    target["tar"] = _workspace({"out.txt": b"ok"})
    # The logs fixture replaced run_remote_vq; route status calls back to "completed".
    from vq import transport

    logs_run = transport.run_remote_vq

    def run_vq(host_cfg, *argv, **kwargs):
        if argv[0] == "status":
            import json
            import subprocess

            return subprocess.CompletedProcess(argv, 0, stdout=json.dumps({"state": "completed"}))
        return logs_run(host_cfg, *argv, **kwargs)

    monkeypatch.setattr(transport, "run_remote_vq", run_vq)
    monkeypatch.setattr(gw_client, "urllib_send", lambda creds: _adapter)
    creds = _credentials(tmp_path, secret)
    result = CliRunner().invoke(cli_main(), ["remote", "logs", job_id, "--follow",
                                             "--credentials", str(creds)])
    assert result.exit_code == 0, result.output
    assert "step 1\nstep 2\n" in result.output


def cli_main():
    from vq.cli import main

    return main


def test_cli_submit_status_cancel(connected, tmp_path, monkeypatch) -> None:
    _client, adapter, payload, _broker_root, secret = connected
    monkeypatch.setattr(gw_client, "urllib_send", lambda creds: adapter)
    creds = str(_credentials(tmp_path, secret))
    runner = CliRunner()
    submitted = runner.invoke(cli_main(), [
        "remote", "submit", str(payload), "--program", "vibeqc-release", "--cpus", "2",
        "--mem-mb", "1024", "--wall-time", "600", "--idempotency-key", "cli-0001",
        "--credentials", creds, "--", "python3", "run.py"])
    assert submitted.exit_code == 0, submitted.output
    job_id = submitted.output.strip().splitlines()[0]
    assert broker_store.BROKER_JOB_ID_PATTERN.fullmatch(job_id)
    status = runner.invoke(cli_main(), ["remote", "status", job_id, "--credentials", creds])
    assert status.exit_code == 0 and '"state": "held"' in status.output
    cancelled = runner.invoke(cli_main(), ["remote", "cancel", job_id, "--credentials", creds])
    assert cancelled.exit_code == 0 and "cancelled" in cancelled.output


def test_pack_refuses_links(tmp_path) -> None:
    payload = tmp_path / "p"
    payload.mkdir()
    (payload / "run.py").write_text("print(1)\n")
    (payload / "link").symlink_to("/etc/hosts")
    with pytest.raises(gw_client.ClientError, match="not a plain file"):
        gw_client.pack(payload)
