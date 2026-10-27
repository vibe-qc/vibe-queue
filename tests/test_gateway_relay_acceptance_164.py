"""#164 acceptance: outbound-only connector, connection loss and restarts.

Covers the issue's acceptance criteria:
- With a coordinator that may not listen, an outside client submits and
  fetches through the gateway alone. The connector opens only its outbound
  connection to the gateway.
- Interrupting before claim, after local acceptance before acknowledgement,
  during logs, during result upload and during cancel keeps one durable job
  identity with no second launch, after restarting each side.
- Backpressure from a full connector spool is visible to clients.
"""
from __future__ import annotations

import socket
import threading
import time
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from tests.test_gateway_logs_cancel_163 import _advance
from tests.test_gateway_relay_164 import BASE
from tests.test_gateway_results_163 import _workspace
from tests.test_gateway_results_163 import target as target  # noqa: F401 — fixture
from tests.test_gateway_submit_163 import _job, _project, _tar, _upload
from vq import broker_store, config, paths
from vq.gateway import client as gw_client
from vq.gateway import connector as gw_connector
from vq.gateway import keys as gw_keys
from vq.gateway import relay
from vq.gateway.app import create_gateway_app
from vq.gateway.client import Credentials, GatewayClient


class FaultyAdapter:
    """Route into the current app instance; inject outages and lost responses."""

    def __init__(self, world: World) -> None:
        self.world = world
        self.down = False
        self.lose: list[tuple[str, str]] = []

    def __call__(self, method, url, headers, body):
        if self.down:
            raise ConnectionRefusedError("gateway unreachable")
        path = url.removeprefix(BASE)
        response = self.world.app.request(method, path, headers=headers, content=body)
        for index, (lose_method, prefix) in enumerate(self.lose):
            if method == lose_method and path.startswith(prefix):
                del self.lose[index]
                raise ConnectionResetError("response lost after the gateway acted")
        return response.status_code, dict(response.headers), response.content


class World:
    def __init__(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, spool_mb: int = 64):
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        self.tmp = tmp_path
        self.gw_root = tmp_path / "gateway"
        self.broker_root = tmp_path / "broker"
        self.conn_root = tmp_path / "connector"
        self.projects = {"group-a": _project(storage_quota_mb=8)}
        self.connector_cfg = config.Config(
            hosts={"target": config.HostConfig(ssh="target.example")},
            broker=config.BrokerConfig(enabled=True, targets=["target"]),
            connector=config.ConnectorConfig(enabled=True, gateway_url=BASE, spool_mb=spool_mb),
        )
        self.restart_gateway()
        self.adapter = FaultyAdapter(self)
        _r, secret = gw_keys.KeyStore(self.gw_root).create(
            principal="alice", project="group-a",
            scopes=["submit", "read", "cancel", "download"], expires_days=30,
            projects=["group-a"])
        self.alice = {"Authorization": f"Bearer {secret}"}
        token = relay.ConnectorStore(self.gw_root).create_enrollment(
            "group-a", projects=["group-a"])
        gw_connector.enroll(BASE, token, send=self.adapter, root=self.conn_root)
        self.restart_connector()

    def gateway_cfg(self) -> config.Config:
        return config.Config(gateway=config.GatewayConfig(
            enabled=True, mode="relay", projects=self.projects))

    def restart_gateway(self) -> None:
        self.app = TestClient(create_gateway_app(self.gateway_cfg, root=self.gw_root),
                              raise_server_exceptions=False)

    def restart_connector(self) -> None:
        client = GatewayClient(gw_connector.load_credentials(self.conn_root),
                               send=self.adapter, sleep=lambda _s: None)
        self.connector = gw_connector.Connector(self.connector_cfg, client,
                                                broker_root=self.broker_root,
                                                root=self.conn_root)

    def submit(self, key: str = "run-0001") -> str:
        upload_id = _upload(self.app, self.alice, _tar())
        answer = self.app.post("/v1/jobs", headers=self.alice,
                               json=_job(upload_id, idempotency_key=key))
        assert answer.status_code in (200, 202), answer.text
        return answer.json()["job_id"]

    def record(self, job_id: str) -> dict[str, Any]:
        return relay.RelayStore(self.gw_root).read(job_id)

    def broker_records(self) -> list[broker_store.BrokerRecord]:
        with broker_store.BrokerStore.open(self.broker_root) as store:
            return store.scan()[0]

    def expire_leases(self) -> None:
        store = relay.RelayStore(self.gw_root)
        for record in store._all():
            record["lease_until"] = None if record["transfer"] == "queued" else \
                "2000-01-01T00:00:00+00:00"
            for part in ("cancel", "collect"):
                if record.get(part):
                    record[part]["lease_until"] = "2000-01-01T00:00:00+00:00"
            store._write(record)

    def admitted_and_placed(self) -> tuple[str, str]:
        job_id = self.submit()
        self.connector.run_once()
        broker_job_id = self.record(job_id)["broker_job_id"]
        _advance(self.broker_root, broker_job_id, "placed")
        return job_id, broker_job_id

    def show(self, job_id: str) -> dict[str, Any]:
        return self.app.get(f"/v1/jobs/{job_id}", headers=self.alice).json()


@pytest.fixture
def world(tmp_path, monkeypatch) -> World:
    return World(tmp_path, monkeypatch)


def _one_job(world: World, job_id: str, broker_job_id: str | None = None) -> None:
    records = world.broker_records()
    assert len(records) == 1, "a second launch"
    assert world.record(job_id)["broker_job_id"] == records[0].broker_job_id
    if broker_job_id is not None:
        assert records[0].broker_job_id == broker_job_id


def test_interrupted_before_claim(world) -> None:
    job_id = world.submit()
    world.adapter.down = True
    with pytest.raises(gw_client.ClientError):
        world.connector.run_once()
    world.adapter.down = False
    world.restart_gateway()
    world.restart_connector()
    assert world.connector.run_once()["accepted"] == [job_id]
    _one_job(world, job_id)


def test_interrupted_after_local_acceptance_before_ack(world, monkeypatch) -> None:
    job_id = world.submit()
    real_ack = gw_connector.Connector._ack

    def die(self, item, outcome, **kw):
        raise RuntimeError("connector died before acknowledging")

    monkeypatch.setattr(gw_connector.Connector, "_ack", die)
    with pytest.raises(RuntimeError):
        world.connector.run_once()
    monkeypatch.setattr(gw_connector.Connector, "_ack", real_ack)
    first_broker_id = world.broker_records()[0].broker_job_id
    world.restart_gateway()
    world.restart_connector()
    world.expire_leases()
    assert world.connector.run_once()["accepted"] == [job_id]
    _one_job(world, job_id, first_broker_id)
    assert world.show(job_id)["transfer"] == "accepted"


def test_interrupted_during_logs(world, monkeypatch) -> None:
    import subprocess

    from vq import transport

    output = "line 1\nline 2\n"
    monkeypatch.setattr(transport, "run_remote_vq", lambda h, *argv, **kw:
                        subprocess.CompletedProcess(argv, 0, stdout=output if "--stdout"
                                                    in argv else "", stderr=""))
    job_id, broker_job_id = world.admitted_and_placed()
    world.app.get(f"/v1/jobs/{job_id}/logs", headers=world.alice)
    world.adapter.lose = [("PUT", f"/v1/connector/jobs/{job_id}/logs/stdout")]
    world.connector.run_once()
    world.restart_gateway()
    world.restart_connector()
    world.connector.run_once()
    logs = world.app.get(f"/v1/jobs/{job_id}/logs", headers=world.alice).json()
    assert logs["data"] == output and logs["size"] == len(output)  # no duplicate bytes
    _one_job(world, job_id, broker_job_id)


def test_interrupted_during_result_upload(world, target) -> None:
    job_id, broker_job_id = world.admitted_and_placed()
    target["status"] = "completed"
    payload = bytes(range(256)) * 64
    target["tar"] = _workspace({"out.bin": payload})
    world.connector.RESULT_CHUNK = 4096
    world.app.get(f"/v1/jobs/{job_id}/results", headers=world.alice)
    world.adapter.lose = [("PUT", f"/v1/connector/jobs/{job_id}/results/out.bin")]
    world.connector.run_once()
    world.restart_gateway()
    ready = world.app.get(f"/v1/jobs/{job_id}/results", headers=world.alice).json()
    if not ready["ready"]:  # the lost response deferred it; a restarted connector resumes
        world.restart_connector()
        world.expire_leases()
        world.connector.run_once()
        ready = world.app.get(f"/v1/jobs/{job_id}/results", headers=world.alice).json()
    assert ready["ready"] is True
    got = world.app.get(f"/v1/jobs/{job_id}/results/out.bin", headers=world.alice)
    assert got.content == payload
    _one_job(world, job_id, broker_job_id)


def test_interrupted_during_cancel(world, monkeypatch) -> None:
    import subprocess

    from vq import transport

    kills: list[list[str]] = []

    def run_vq(host_cfg, *argv, **kwargs):
        kills.append(list(argv))
        out = "killed\n" if len(kills) == 1 else "already in terminal state\n"
        rc = 0 if len(kills) == 1 else 1
        return subprocess.CompletedProcess(argv, rc, stdout=out, stderr="")

    monkeypatch.setattr(transport, "run_remote_vq", run_vq)
    job_id, broker_job_id = world.admitted_and_placed()
    world.app.post(f"/v1/jobs/{job_id}/cancel", headers=world.alice)
    world.adapter.lose = [("POST", f"/v1/connector/items/cancel:{job_id}/ack")]
    world.connector.run_once()  # the kill ran; the retried ack repeats harmlessly
    world.restart_gateway()
    world.restart_connector()
    world.expire_leases()
    world.connector.run_once()
    assert world.show(job_id)["cancellation"]["state"] == "stopped"
    _one_job(world, job_id, broker_job_id)


def test_a_full_spool_is_visible_backpressure(tmp_path, monkeypatch) -> None:
    world = World(tmp_path, monkeypatch, spool_mb=1)
    (world.conn_root / "spool").mkdir(parents=True, exist_ok=True)
    (world.conn_root / "spool" / "busy").write_bytes(b"x" * (1024 * 1024))
    job_id = world.submit()
    assert world.connector.run_once()["claimed"] == 0
    shown = world.show(job_id)
    assert shown["transfer"] == "queued" and shown["connector_backpressure"] is True
    (world.conn_root / "spool" / "busy").unlink()
    assert world.connector.run_once()["accepted"] == [job_id]
    assert world.show(job_id)["connector_backpressure"] is False


def test_outbound_only_over_real_http(tmp_path, monkeypatch, target) -> None:
    """A real loopback server; the coordinator may not listen at all."""
    import uvicorn

    world = World(tmp_path, monkeypatch)
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    server = uvicorn.Server(uvicorn.Config(
        create_gateway_app(world.gateway_cfg, root=world.gw_root),
        host="127.0.0.1", port=port, log_level="error"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    for _ in range(100):
        if server.started:
            break
        time.sleep(0.05)
    assert server.started
    url = f"http://127.0.0.1:{port}"
    try:
        connector_creds = gw_connector.load_credentials(world.conn_root)
        connector = gw_connector.Connector(
            world.connector_cfg,
            GatewayClient(Credentials(url=url, key=connector_creds.key),
                          sleep=lambda _s: None),
            broker_root=world.broker_root, root=world.conn_root)
        outside = GatewayClient(
            Credentials(url=url, key=world.alice["Authorization"].removeprefix("Bearer ")),
            sleep=lambda _s: None)

        connections: list[Any] = []
        real_connect = socket.socket.connect

        def recording_connect(self, address):
            connections.append(address)
            return real_connect(self, address)

        def no_listen(self, *args):
            raise AssertionError("the coordinator must not accept inbound connections")

        payload = tmp_path / "payload"
        payload.mkdir()
        (payload / "run.py").write_text("print('energy')\n")
        archive, _tmp = gw_client.pack(payload)
        job = outside.submit({"upload_id": outside.upload(archive), "command":
                              ["python3", "run.py"], "program": "vibeqc-release",
                              "cpus": 2, "mem_mb": 1024, "wall_time_seconds": 600,
                              "idempotency_key": "net-0001"})

        with monkeypatch.context() as guard:
            guard.setattr(socket.socket, "connect", recording_connect)
            guard.setattr(socket.socket, "listen", no_listen)
            assert connector.run_once()["accepted"] == [job["job_id"]]
        broker_job_id = world.record(job["job_id"])["broker_job_id"]
        _advance(world.broker_root, broker_job_id, "placed")
        target["status"] = "completed"
        target["tar"] = _workspace({"out.txt": b"E = -76.0\n"})
        assert outside.results(job["job_id"])["ready"] is False
        with monkeypatch.context() as guard:
            guard.setattr(socket.socket, "connect", recording_connect)
            guard.setattr(socket.socket, "listen", no_listen)
            connector.run_once()
        dest = tmp_path / "results"
        assert outside.fetch(job["job_id"], dest) == ["out.txt"]
        assert (dest / "out.txt").read_bytes() == b"E = -76.0\n"
        assert connections and {tuple(a)[:2] for a in connections} == {("127.0.0.1", port)}
        assert len(world.broker_records()) == 1
    finally:
        server.should_exit = True
        thread.join(timeout=10)
