"""Relay mode and the outbound connector (#164, slice 1).

The gateway holds admitted jobs; a connector on the broker host claims them
over the gateway's own API, admits each to its local broker under the scoped
idempotency key and acknowledges. Every request the connector makes goes
through the real app; nothing reaches the connector from outside.
"""
from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner
from fastapi.testclient import TestClient

from tests.test_gateway_submit_163 import _job, _project, _tar, _upload
from vq import broker_store, config, paths
from vq.gateway import connector as gw_connector
from vq.gateway import keys as gw_keys
from vq.gateway import relay
from vq.gateway.app import create_gateway_app
from vq.gateway.client import Credentials, GatewayClient

BASE = "https://gateway.example"


class Adapter:
    def __init__(self, client: TestClient) -> None:
        self.client = client
        self.tamper: dict[str, bytes] = {}

    def __call__(self, method, url, headers, body):
        path = url.removeprefix(BASE)
        response = self.client.request(method, path, headers=headers, content=body)
        return response.status_code, dict(response.headers), self.tamper.get(
            path, response.content)


@pytest.fixture
def relay_world(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    projects = {"group-a": _project(), "group-b": _project()}
    state = {"mode": "relay"}

    def gateway_cfg() -> config.Config:  # the reachable host: no broker at all
        return config.Config(gateway=config.GatewayConfig(
            enabled=True, mode=state["mode"], projects=projects))

    connector_cfg = config.Config(  # the internal broker host
        hosts={"target": config.HostConfig(ssh="target.example")},
        broker=config.BrokerConfig(enabled=True, targets=["target"]),
        connector=config.ConnectorConfig(enabled=True, gateway_url=BASE),
    )
    gw_root = tmp_path / "gateway"
    app = TestClient(create_gateway_app(gateway_cfg, root=gw_root),
                     raise_server_exceptions=False)
    adapter = Adapter(app)
    keys = gw_keys.KeyStore(gw_root)
    connectors = relay.ConnectorStore(gw_root)

    def client_key(principal="alice", project="group-a") -> dict[str, str]:
        _r, secret = keys.create(principal=principal, project=project,
                                 scopes=["submit", "read", "cancel", "download"],
                                 expires_days=30, projects=list(projects))
        return {"Authorization": f"Bearer {secret}"}

    def make_connector(project="group-a", name="conn") -> gw_connector.Connector:
        token = connectors.create_enrollment(project, projects=list(projects))
        root = tmp_path / name
        gw_connector.enroll(BASE, token, send=adapter, root=root)
        client = GatewayClient(gw_connector.load_credentials(root), send=adapter,
                               sleep=lambda _s: None)
        return gw_connector.Connector(connector_cfg, client, broker_root=tmp_path / "broker",
                                      root=root)

    return SimpleWorld(app, adapter, client_key, make_connector, connectors, state,
                       tmp_path / "broker", gw_root)


class SimpleWorld:
    def __init__(self, app, adapter, client_key, make_connector, connectors, state,
                 broker_root, gw_root) -> None:
        self.app, self.adapter, self.client_key = app, adapter, client_key
        self.make_connector, self.connectors, self.state = make_connector, connectors, state
        self.broker_root, self.gw_root = broker_root, gw_root

    def submit(self, headers: dict[str, str], key: str = "run-0001") -> dict[str, Any]:
        upload_id = _upload(self.app, headers, _tar())
        answer = self.app.post("/v1/jobs", headers=headers,
                               json=_job(upload_id, idempotency_key=key))
        assert answer.status_code in (200, 202), answer.text
        return answer.json()

    def broker_records(self) -> list[broker_store.BrokerRecord]:
        with broker_store.BrokerStore.open(self.broker_root) as store:
            return store.scan()[0]

    def expire_leases(self) -> None:
        store = relay.RelayStore(self.gw_root)
        for record in store._all():
            if record["lease_until"]:
                record["lease_until"] = "2000-01-01T00:00:00+00:00"
                store._write(record)


def test_a_relay_job_reaches_the_broker_through_the_connector(relay_world) -> None:
    world = relay_world
    alice = world.client_key()
    connector = world.make_connector()
    job = world.submit(alice)
    assert job["job_id"].startswith("g-") and job["transfer"] == "queued"

    summary = connector.run_once()
    assert summary["accepted"] == [job["job_id"]]
    (record,) = world.broker_records()
    assert record.intent.command == ("python3", "run.py")

    shown = world.app.get(f"/v1/jobs/{job['job_id']}", headers=alice).json()
    assert shown["transfer"] == "accepted" and shown["state"] == "held"
    assert shown["connector_fresh"] is True and shown["status_as_of"]
    listed = world.app.get("/v1/jobs", headers=alice).json()["jobs"]
    assert [j["job_id"] for j in listed] == [job["job_id"]]


def test_a_replayed_submission_is_one_job(relay_world) -> None:
    world = relay_world
    alice = world.client_key()
    connector = world.make_connector()
    first = world.submit(alice)
    connector.run_once()
    again = world.submit(alice)
    assert again["job_id"] == first["job_id"] and again["replayed"] is True
    assert connector.run_once()["claimed"] == 0
    assert len(world.broker_records()) == 1


def test_a_crash_after_local_accept_before_ack_admits_once(relay_world, monkeypatch) -> None:
    world = relay_world
    alice = world.client_key()
    connector = world.make_connector()
    job = world.submit(alice)
    real_ack = gw_connector.Connector._ack

    def die(self, item, outcome, **kw):
        raise RuntimeError("connector died before acknowledging")

    monkeypatch.setattr(gw_connector.Connector, "_ack", die)
    with pytest.raises(RuntimeError):
        connector.run_once()
    assert len(world.broker_records()) == 1
    claimed = world.app.get(f"/v1/jobs/{job['job_id']}", headers=alice).json()
    assert claimed["transfer"] == "claimed"

    monkeypatch.setattr(gw_connector.Connector, "_ack", real_ack)
    world.expire_leases()
    restarted = gw_connector.Connector(connector.cfg, connector.client,
                                       broker_root=connector.broker_root, root=connector.root)
    assert restarted.run_once()["accepted"] == [job["job_id"]]
    records = world.broker_records()
    assert len(records) == 1
    record = relay.RelayStore(world.gw_root).read(job["job_id"])
    assert record["broker_job_id"] == records[0].broker_job_id and record["epoch"] == 2


def test_a_stale_epoch_is_refused(relay_world) -> None:
    world = relay_world
    world.submit(world.client_key())
    connector = world.make_connector()
    first = connector.client._json("POST", "/v1/connector/claim", payload={})["items"][0]
    world.expire_leases()
    second = connector.client._json("POST", "/v1/connector/claim", payload={})["items"][0]
    assert second["epoch"] == first["epoch"] + 1
    status, _h, body = connector.client._request(
        "POST", f"/v1/connector/items/{first['item_id']}/ack",
        payload={"epoch": first["epoch"], "outcome": "accepted", "broker_job_id": "b-x"})
    assert status == 409 and b"stale" in body


def test_a_revoked_connector_claims_nothing(relay_world) -> None:
    world = relay_world
    world.submit(world.client_key())
    connector = world.make_connector()
    connector_id = world.connectors.list()[0]["connector_id"]
    world.connectors.revoke(connector_id)
    status, _h, body = connector.client._request("POST", "/v1/connector/claim", payload={})
    assert status == 401 and b"revoked" in body
    assert world.broker_records() == []


def test_a_connector_sees_only_its_own_project(relay_world) -> None:
    world = relay_world
    world.submit(world.client_key("bob", "group-b"))
    connector = world.make_connector("group-a")
    assert connector.run_once()["claimed"] == 0
    record = relay.RelayStore(world.gw_root)._all()[0]
    status, _h, _b = connector.client._request(
        "GET", f"/v1/connector/uploads/{record['upload_id']}")
    assert status == 404


def test_enrollment_is_one_use_expiring_and_one_per_project(relay_world, monkeypatch) -> None:
    world = relay_world
    token = world.connectors.create_enrollment("group-a", projects=["group-a"])
    world.connectors.enroll(token)
    with pytest.raises(relay.RelayError, match="already used"):
        world.connectors.enroll(token)
    with pytest.raises(relay.RelayError, match="already has a connector"):
        world.connectors.create_enrollment("group-a", projects=["group-a"])
    late = world.connectors.create_enrollment("group-b", projects=["group-b"])
    later = relay._now() + timedelta(minutes=11)
    monkeypatch.setattr(relay, "_now", lambda: later)
    with pytest.raises(relay.RelayError, match="expired"):
        world.connectors.enroll(late)


def test_client_and_connector_credentials_never_cross(relay_world) -> None:
    world = relay_world
    alice = world.client_key()
    connector = world.make_connector()
    as_client = {"Authorization": f"Bearer {connector.client.credentials.key}"}
    assert world.app.get("/v1/jobs", headers=as_client).status_code == 401
    assert world.app.post("/v1/connector/claim", headers=alice, json={}).status_code == 401


def test_connector_routes_vanish_in_local_mode(relay_world):
    world = relay_world
    world.state["mode"] = "local"
    assert world.app.post("/v1/connector/claim", json={}).status_code == 404


def test_rotation_overlaps_briefly(relay_world, monkeypatch) -> None:
    world = relay_world
    connector = world.make_connector()
    old_key = connector.client.credentials.key
    connector.rotate()
    fresh = gw_connector.load_credentials(connector.root)
    assert fresh.key != old_key
    new_client = GatewayClient(fresh, send=world.adapter, sleep=lambda _s: None)
    old_client = GatewayClient(Credentials(url=BASE, key=old_key), send=world.adapter,
                               sleep=lambda _s: None)
    assert new_client._request("POST", "/v1/connector/claim", payload={})[0] == 200
    assert old_client._request("POST", "/v1/connector/claim", payload={})[0] == 200
    later = relay._now() + timedelta(minutes=11)
    monkeypatch.setattr(relay, "_now", lambda: later)
    assert old_client._request("POST", "/v1/connector/claim", payload={})[0] == 401
    assert new_client._request("POST", "/v1/connector/claim", payload={})[0] == 200


def test_a_payload_that_fails_its_digest_is_refused(relay_world) -> None:
    world = relay_world
    alice = world.client_key()
    connector = world.make_connector()
    job = world.submit(alice)
    upload_id = relay.RelayStore(world.gw_root).read(job["job_id"])["upload_id"]
    world.adapter.tamper[f"/v1/connector/uploads/{upload_id}"] = b"forged bytes"
    assert connector.run_once()["refused"] == [job["job_id"]]
    shown = world.app.get(f"/v1/jobs/{job['job_id']}", headers=alice).json()
    assert shown["transfer"] == "refused" and "digest" in shown["refused_reason"]
    assert world.broker_records() == []


def test_gateway_connector_enroll_cli(tmp_path, monkeypatch) -> None:
    from vq.cli import main

    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setattr(config, "load_config", lambda: config.Config(gateway=config.GatewayConfig(
        enabled=True, mode="relay", projects={"group-a": _project()})))
    result = CliRunner().invoke(main, ["gateway", "connector", "enroll", "--project", "group-a"])
    assert result.exit_code == 0, result.output
    assert relay.ENROLL_TOKEN_RE.fullmatch(result.output.splitlines()[0])
    listed = CliRunner().invoke(main, ["gateway", "connector", "list", "--json"])
    assert json.loads(listed.output) == []



# --- slice 2: the cancel exchange -------------------------------------------

def test_cancel_before_claim_never_delivers(relay_world) -> None:
    world = relay_world
    alice = world.client_key()
    connector = world.make_connector()
    job = world.submit(alice)
    done = world.app.post(f"/v1/jobs/{job['job_id']}/cancel", headers=alice).json()
    assert done["transfer"] == "cancelled" and done["stopped"] is True
    assert done["cancellation"]["state"] == "stopped"
    assert connector.run_once()["claimed"] == 0
    assert world.broker_records() == []


def test_cancel_of_an_admitted_job_is_a_request_until_acknowledged(relay_world) -> None:
    world = relay_world
    alice = world.client_key()
    connector = world.make_connector()
    job = world.submit(alice)
    connector.run_once()
    requested = world.app.post(f"/v1/jobs/{job['job_id']}/cancel", headers=alice).json()
    assert requested["cancellation"]["state"] == "requested" and requested["stopped"] is False
    again = world.app.post(f"/v1/jobs/{job['job_id']}/cancel", headers=alice).json()
    assert again["cancellation"]["requested_at"] == requested["cancellation"]["requested_at"]

    summary = connector.run_once()
    assert summary["cancelled"] == [job["job_id"]]
    (record,) = world.broker_records()
    assert record.state == broker_store.STATE_CANCELLED
    shown = world.app.get(f"/v1/jobs/{job['job_id']}", headers=alice).json()
    assert shown["cancellation"]["state"] == "stopped"
    assert shown["state"] == "cancelled"  # the connector's report after the kill
    assert connector.run_once()["claimed"] == 0


def test_a_cancel_whose_kill_fails_is_retried_not_dropped(relay_world, monkeypatch) -> None:
    from vq import broker_relay, transport

    world = relay_world
    alice = world.client_key()
    connector = world.make_connector()
    job = world.submit(alice)
    connector.run_once()
    world.app.post(f"/v1/jobs/{job['job_id']}/cancel", headers=alice)
    real_kill = broker_relay._kill

    def unreachable(*args, **kwargs):
        raise transport.RemoteError("target unreachable")

    monkeypatch.setattr(broker_relay, "_kill", unreachable)
    assert connector.run_once()["deferred"] == [f"cancel:{job['job_id']}"]
    shown = world.app.get(f"/v1/jobs/{job['job_id']}", headers=alice).json()
    assert shown["cancellation"]["state"] == "requested"
    monkeypatch.setattr(broker_relay, "_kill", real_kill)
    world.expire_leases()
    store = relay.RelayStore(world.gw_root)
    record = store.read(job["job_id"])
    record["cancel"]["lease_until"] = "2000-01-01T00:00:00+00:00"
    store._write(record)
    assert connector.run_once()["cancelled"] == [job["job_id"]]


def test_a_stale_cancel_epoch_is_refused(relay_world) -> None:
    world = relay_world
    alice = world.client_key()
    connector = world.make_connector()
    job = world.submit(alice)
    connector.run_once()
    world.app.post(f"/v1/jobs/{job['job_id']}/cancel", headers=alice)
    item = connector.client._json("POST", "/v1/connector/claim", payload={})["items"][0]
    assert item["kind"] == "cancel"
    status, _h, body = connector.client._request(
        "POST", f"/v1/connector/items/{item['item_id']}/ack",
        payload={"epoch": item["epoch"] - 1, "outcome": "stopped"})
    assert status == 409 and b"stale" in body


def test_a_refused_job_settles_its_pending_cancel(relay_world) -> None:
    world = relay_world
    alice = world.client_key()
    connector = world.make_connector()
    job = world.submit(alice)
    item = connector.client._json("POST", "/v1/connector/claim", payload={})["items"][0]
    world.app.post(f"/v1/jobs/{job['job_id']}/cancel", headers=alice)
    connector.client._json("POST", f"/v1/connector/items/{item['item_id']}/ack",
                           payload={"epoch": item["epoch"], "outcome": "refused",
                                    "reason": "test"})
    shown = world.app.get(f"/v1/jobs/{job['job_id']}", headers=alice).json()
    assert shown["transfer"] == "refused" and shown["cancellation"]["state"] == "stopped"


# --- slice 3: log push --------------------------------------------------------

@pytest.fixture
def target_logs(monkeypatch):
    import subprocess

    from vq import transport

    output = {"--stdout": "", "--stderr": ""}

    def run_vq(host_cfg, *argv, **kwargs):
        if argv[0] == "logs":
            stream = next(a for a in argv if a in output)
            return subprocess.CompletedProcess(argv, 0, stdout=output[stream], stderr="")
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    monkeypatch.setattr(transport, "run_remote_vq", run_vq)
    return output


def _placed_relay_job(world, alice):
    from tests.test_gateway_logs_cancel_163 import _advance

    connector = world.make_connector()
    job = world.submit(alice)
    connector.run_once()
    broker_job_id = relay.RelayStore(world.gw_root).read(job["job_id"])["broker_job_id"]
    _advance(world.broker_root, broker_job_id, "placed")
    return connector, job


def test_wanted_logs_are_pushed_by_offset(relay_world, target_logs) -> None:
    world = relay_world
    alice = world.client_key()
    connector, job = _placed_relay_job(world, alice)
    target_logs["--stdout"] = "step 1\nstep 2\n"
    first = world.app.get(f"/v1/jobs/{job['job_id']}/logs", headers=alice).json()
    assert first["data"] == "" and first["size"] == 0  # nothing pushed yet; now wanted

    assert connector.run_once()["logs_pushed"] == len("step 1\nstep 2\n")
    shown = world.app.get(f"/v1/jobs/{job['job_id']}/logs", headers=alice).json()
    assert shown["data"] == "step 1\nstep 2\n" and shown["as_of"]

    target_logs["--stdout"] += "step 3\n"
    assert connector.run_once()["logs_pushed"] == len("step 3\n")
    tail = world.app.get(f"/v1/jobs/{job['job_id']}/logs?offset={shown['next_offset']}",
                         headers=alice).json()
    assert tail["data"] == "step 3\n"


def test_logs_are_not_pushed_unless_wanted(relay_world, target_logs) -> None:
    world = relay_world
    connector, _job = _placed_relay_job(world, world.client_key())
    target_logs["--stdout"] = "quiet\n"
    assert connector.run_once()["logs_pushed"] == 0


def test_a_stale_push_offset_is_refused_and_tolerated(relay_world, target_logs) -> None:
    world = relay_world
    alice = world.client_key()
    connector, job = _placed_relay_job(world, alice)
    target_logs["--stdout"] = "abc\n"
    world.app.get(f"/v1/jobs/{job['job_id']}/logs", headers=alice)
    connector.run_once()
    status, _h, body = connector.client._request(
        "PUT", f"/v1/connector/jobs/{job['job_id']}/logs/stdout", body=b"abc\n",
        headers={"Content-Range": "bytes 0-3/4"})
    assert status == 409 and b"resume at byte 4" in body


def test_the_log_buffer_is_bounded(relay_world, target_logs, monkeypatch) -> None:
    world = relay_world
    alice = world.client_key()
    connector, job = _placed_relay_job(world, alice)
    monkeypatch.setattr(relay, "LOG_BUFFER_CAP", 8)
    monkeypatch.setattr(gw_connector.Connector, "LOG_CHUNK", 4)
    target_logs["--stdout"] = "0123456789abcdef"
    world.app.get(f"/v1/jobs/{job['job_id']}/logs", headers=alice)
    assert connector.run_once()["logs_pushed"] == 8
    assert world.app.get(f"/v1/jobs/{job['job_id']}/logs", headers=alice).json()["size"] == 8


def test_another_projects_connector_cannot_push(relay_world, target_logs) -> None:
    world = relay_world
    alice = world.client_key()
    _connector, job = _placed_relay_job(world, alice)
    other = world.make_connector("group-b", name="conn-b")
    status, _h, _b = other.client._request(
        "PUT", f"/v1/connector/jobs/{job['job_id']}/logs/stdout", body=b"x",
        headers={"Content-Range": "bytes 0-0/1"})
    assert status == 404


def test_a_claim_with_only_wanted_logs_returns_promptly(relay_world, target_logs,
                                                        monkeypatch) -> None:
    import time

    from vq.gateway import app as gw_app

    world = relay_world
    alice = world.client_key()
    connector, job = _placed_relay_job(world, alice)
    world.app.get(f"/v1/jobs/{job['job_id']}/logs", headers=alice)
    monkeypatch.setattr(gw_app, "LOG_PUSH_SECONDS", 0.2)
    started = time.monotonic()
    answer = connector.client._json("POST", "/v1/connector/claim",
                                    payload={"wait_seconds": 10})
    assert time.monotonic() - started < 5
    assert [w["job_id"] for w in answer["logs_wanted"]] == [job["job_id"]]


# --- slice 4: result transfer ---------------------------------------------------

from tests.test_gateway_results_163 import _workspace  # noqa: E402
from tests.test_gateway_results_163 import target as target  # noqa: E402, F401 — fixture


def test_results_are_collected_by_the_connector_and_verified(relay_world, target) -> None:
    import hashlib

    world = relay_world
    alice = world.client_key()
    connector, job = _placed_relay_job(world, alice)
    target["status"] = "completed"
    target["tar"] = _workspace({"out.txt": b"E = -76.02\n", "sub/a.dat": b"\x00\x01",
                                "empty.txt": b""}, links=["link"])
    asked = world.app.get(f"/v1/jobs/{job['job_id']}/results", headers=alice).json()
    assert asked["ready"] is False and asked["collection"]["state"] == "requested"

    summary = connector.run_once()
    assert summary["collected"] == [job["job_id"]]
    ready = world.app.get(f"/v1/jobs/{job['job_id']}/results", headers=alice).json()
    assert ready["ready"] is True
    assert {f["path"] for f in ready["files"]} == {"out.txt", "sub/a.dat", "empty.txt"}
    assert ready["skipped"] == ["link"]
    got = world.app.get(f"/v1/jobs/{job['job_id']}/results/out.txt", headers=alice)
    assert got.content == b"E = -76.02\n"
    assert got.headers["x-content-sha256"] == hashlib.sha256(got.content).hexdigest()
    assert not (connector.root / "results").exists() or not any(
        (connector.root / "results").iterdir())  # the local copy is gone
    assert connector.run_once()["claimed"] == 0


def test_a_running_job_reports_not_finished_and_can_be_asked_again(relay_world, target):
    world = relay_world
    alice = world.client_key()
    connector, job = _placed_relay_job(world, alice)
    world.app.get(f"/v1/jobs/{job['job_id']}/results", headers=alice)
    assert connector.run_once()["not-finished"] == [job["job_id"]]
    shown = world.app.get(f"/v1/jobs/{job['job_id']}", headers=alice).json()
    assert shown["collection"]["state"] == "not-finished"
    again = world.app.get(f"/v1/jobs/{job['job_id']}/results", headers=alice).json()
    assert again["collection"]["state"] == "requested"


def test_results_beyond_the_quota_fail_visibly(relay_world, target) -> None:
    world = relay_world
    alice = world.client_key()
    connector, job = _placed_relay_job(world, alice)
    target["status"] = "completed"
    target["tar"] = _workspace({"big.bin": b"x" * (2 * 1024 * 1024)})
    world.app.get(f"/v1/jobs/{job['job_id']}/results", headers=alice)
    connector.run_once()
    shown = world.app.get(f"/v1/jobs/{job['job_id']}", headers=alice).json()
    assert shown["collection"]["state"] == "failed"
    assert world.app.get(f"/v1/jobs/{job['job_id']}/results/big.bin", headers=alice
                         ).status_code == 409


def test_pushed_files_must_match_the_manifest(relay_world, target) -> None:
    world = relay_world
    alice = world.client_key()
    connector, job = _placed_relay_job(world, alice)
    world.app.get(f"/v1/jobs/{job['job_id']}/results", headers=alice)
    item = next(i for i in connector.client._json("POST", "/v1/connector/claim",
                                                    payload={})["items"]
                if i["kind"] == "collect")
    connector.client._json("PUT", f"/v1/connector/jobs/{job['job_id']}/results/out.txt",
                           body=b"forged", headers={"Content-Range": "bytes 0-5/6"})
    real = {"files": [{"path": "out.txt", "size": 2, "sha256": "0" * 64}], "skipped": [],
            "target_state": "completed"}
    status, _h, body = connector.client._request(
        "POST", f"/v1/connector/items/{item['item_id']}/ack",
        payload={"epoch": item["epoch"], "outcome": "collected", "manifest": real})
    assert status == 409 and b"manifest" in body
    stale, _h, _b = connector.client._request(
        "POST", f"/v1/connector/items/{item['item_id']}/ack",
        payload={"epoch": item["epoch"] - 1, "outcome": "failed"})
    assert stale == 409


def test_result_paths_are_confined() -> None:
    for path in ("../escape", "/etc/passwd", "a/../../b", ""):
        with pytest.raises(relay.RelayError):
            relay._safe_parts(path)
    assert relay._safe_parts("sub/./a.dat") == ("sub", "a.dat")


def test_a_collected_job_stops_counting_as_active(tmp_path, monkeypatch, target) -> None:
    world_projects = {"group-a": _project(max_active_jobs=1)}
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    gw_root = tmp_path / "gateway"
    app = TestClient(create_gateway_app(lambda: config.Config(gateway=config.GatewayConfig(
        enabled=True, mode="relay", projects=world_projects)), root=gw_root),
        raise_server_exceptions=False)
    adapter = Adapter(app)
    _r, secret = gw_keys.KeyStore(gw_root).create(
        principal="alice", project="group-a", scopes=["submit", "read", "download"],
        expires_days=30, projects=["group-a"])
    alice = {"Authorization": f"Bearer {secret}"}
    token = relay.ConnectorStore(gw_root).create_enrollment("group-a", projects=["group-a"])
    gw_connector.enroll(BASE, token, send=adapter, root=tmp_path / "conn")
    connector = gw_connector.Connector(
        config.Config(hosts={"target": config.HostConfig(ssh="target.example")},
                      broker=config.BrokerConfig(enabled=True, targets=["target"])),
        GatewayClient(gw_connector.load_credentials(tmp_path / "conn"), send=adapter,
                      sleep=lambda _s: None),
        broker_root=tmp_path / "broker", root=tmp_path / "conn")
    world = SimpleWorld(app, adapter, None, None, None, {}, tmp_path / "broker", gw_root)
    job = world.submit(alice)
    connector.run_once()
    from tests.test_gateway_logs_cancel_163 import _advance

    _advance(world.broker_root, relay.RelayStore(gw_root).read(job["job_id"])["broker_job_id"],
             "placed")
    upload_id = _upload(app, alice, _tar())
    blocked = app.post("/v1/jobs", headers=alice,
                       json=_job(upload_id, idempotency_key="run-0002"))
    assert blocked.status_code == 429
    target["status"] = "completed"
    target["tar"] = _workspace({"out.txt": b"ok"})
    app.get(f"/v1/jobs/{job['job_id']}/results", headers=alice)
    connector.run_once()
    allowed = app.post("/v1/jobs", headers=alice,
                       json=_job(upload_id, idempotency_key="run-0002"))
    assert allowed.status_code == 202, allowed.text
