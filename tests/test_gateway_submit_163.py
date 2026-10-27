"""Gateway slice 2 (#163): resumable uploads, broker submission, status and list."""
from __future__ import annotations

import hashlib
import io
import tarfile
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from vq import broker_store, config, paths
from vq.gateway import keys as gw_keys
from vq.gateway.app import create_gateway_app


def _tar(files: dict[str, bytes] | None = None, *, links: dict[str, str] | None = None,
         names: list[str] | None = None) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        for name, data in (files if files is not None else {"run.py": b"print(1)\n"}).items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
        for name, target in (links or {}).items():
            info = tarfile.TarInfo(name)
            info.type = tarfile.SYMTYPE
            info.linkname = target
            archive.addfile(info)
        for name in names or []:
            info = tarfile.TarInfo(name)
            info.size = 1
            archive.addfile(info, io.BytesIO(b"x"))
    return buffer.getvalue()


def _project(**changes: Any) -> config.GatewayProjectConfig:
    fields = dict(programs=["vibeqc-release"], max_cpus=8, max_mem_mb=8192,
                  max_wall_time_seconds=3600, storage_quota_mb=1)
    fields.update(changes)
    return config.GatewayProjectConfig(**fields)


@pytest.fixture
def gateway(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    state: dict[str, Any] = {"project": _project()}

    def cfg() -> config.Config:
        return config.Config(
            hosts={"target": config.HostConfig(ssh="target.example")},
            broker=config.BrokerConfig(enabled=True, targets=["target"]),
            gateway=config.GatewayConfig(enabled=True, projects={"group-a": state["project"]}),
        )

    root = tmp_path / "gateway"
    broker_root = tmp_path / "broker"
    client = TestClient(create_gateway_app(cfg, root=root, broker_root=broker_root),
                        raise_server_exceptions=False)
    store = gw_keys.KeyStore(root)

    def key(principal: str = "alice", scopes=("submit", "read")) -> dict[str, str]:
        _record, secret = store.create(principal=principal, project="group-a", scopes=scopes,
                                       expires_days=30, projects=["group-a"])
        return {"Authorization": f"Bearer {secret}"}

    return client, key, state, broker_root


def _upload(client: TestClient, headers: dict[str, str], data: bytes,
            *, chunk: int = 0) -> str:
    created = client.post("/v1/uploads", headers=headers,
                          json={"size": len(data), "sha256": hashlib.sha256(data).hexdigest()})
    assert created.status_code == 201, created.text
    upload_id = created.json()["upload_id"]
    step = chunk or len(data)
    for start in range(0, len(data), step):
        part = data[start:start + step]
        sent = client.put(f"/v1/uploads/{upload_id}", content=part, headers={
            **headers, "Content-Range": f"bytes {start}-{start + len(part) - 1}/{len(data)}"})
        assert sent.status_code == 200, sent.text
    final = client.post(f"/v1/uploads/{upload_id}/finalize", headers=headers)
    assert final.status_code == 200, final.text
    return upload_id


def _job(upload_id: str, **changes: Any) -> dict[str, Any]:
    body = {"upload_id": upload_id, "command": ["python3", "run.py"],
            "program": "vibeqc-release", "cpus": 2, "mem_mb": 1024,
            "wall_time_seconds": 600, "idempotency_key": "run-0001"}
    body.update(changes)
    return body


def test_upload_submit_status_list_and_replay(gateway) -> None:
    client, key, _state, broker_root = gateway
    alice = key()
    upload_id = _upload(client, alice, _tar(), chunk=512)

    first = client.post("/v1/jobs", headers=alice, json=_job(upload_id))
    assert first.status_code == 202, first.text
    job = first.json()
    assert job["state"] == broker_store.STATE_HELD and job["replayed"] is False
    assert "command" not in job and "target_host" not in job

    again = client.post("/v1/jobs", headers=alice, json=_job(upload_id))
    assert again.status_code == 200 and again.json()["job_id"] == job["job_id"]
    assert again.json()["replayed"] is True
    changed = client.post("/v1/jobs", headers=alice,
                          json=_job(upload_id, command=["python3", "run.py", "--x"]))
    assert changed.status_code == 409

    shown = client.get(f"/v1/jobs/{job['job_id']}", headers=alice)
    assert shown.status_code == 200 and shown.json()["job_id"] == job["job_id"]
    listed = client.get("/v1/jobs", headers=alice).json()
    assert [j["job_id"] for j in listed["jobs"]] == [job["job_id"]]
    with broker_store.BrokerStore.open(broker_root) as store:
        records, _problems = store.scan()
    assert len(records) == 1
    assert records[0].intent.command == ("python3", "run.py")


def test_a_chunk_must_resume_at_the_received_count(gateway) -> None:
    client, key, _state, _broker = gateway
    alice = key()
    data = _tar()
    upload_id = client.post("/v1/uploads", headers=alice, json={
        "size": len(data), "sha256": hashlib.sha256(data).hexdigest()}).json()["upload_id"]
    client.put(f"/v1/uploads/{upload_id}", content=data[:100],
               headers={**alice, "Content-Range": f"bytes 0-99/{len(data)}"})
    wrong = client.put(f"/v1/uploads/{upload_id}", content=data[200:300],
                       headers={**alice, "Content-Range": f"bytes 200-299/{len(data)}"})
    assert wrong.status_code == 409 and "resume at byte 100" in wrong.text
    assert client.get(f"/v1/uploads/{upload_id}", headers=alice).json()["received"] == 100
    missing = client.put(f"/v1/uploads/{upload_id}", content=b"x", headers=alice)
    assert missing.status_code == 400
    early = client.post(f"/v1/uploads/{upload_id}/finalize", headers=alice)
    assert early.status_code == 409


@pytest.mark.parametrize("data,match", [
    (_tar(names=["/etc/evil"]), "unsafe archive path"),
    (_tar(names=["../escape"]), "unsafe archive path"),
    (_tar(links={"link": "/etc/passwd"}), "not a plain file"),
    (b"not a tar archive at all" * 50, "not a valid"),
])
def test_unsafe_archives_are_refused_at_finalize(gateway, data, match) -> None:
    client, key, _state, _broker = gateway
    alice = key()
    upload_id = client.post("/v1/uploads", headers=alice, json={
        "size": len(data), "sha256": hashlib.sha256(data).hexdigest()}).json()["upload_id"]
    client.put(f"/v1/uploads/{upload_id}", content=data,
               headers={**alice, "Content-Range": f"bytes 0-{len(data) - 1}/{len(data)}"})
    final = client.post(f"/v1/uploads/{upload_id}/finalize", headers=alice)
    assert final.status_code == 422 and match in final.text


def test_digest_mismatch_and_quota_are_refused(gateway) -> None:
    client, key, _state, _broker = gateway
    alice = key()
    data = _tar()
    upload_id = client.post("/v1/uploads", headers=alice, json={
        "size": len(data), "sha256": "0" * 64}).json()["upload_id"]
    client.put(f"/v1/uploads/{upload_id}", content=data,
               headers={**alice, "Content-Range": f"bytes 0-{len(data) - 1}/{len(data)}"})
    assert client.post(f"/v1/uploads/{upload_id}/finalize", headers=alice).status_code == 422
    huge = client.post("/v1/uploads", headers=alice, json={"size": 2 * 1024 * 1024,
                                                            "sha256": "0" * 64})
    assert huge.status_code == 413


@pytest.mark.parametrize("changes,status", [
    ({"program": "other"}, 403),
    ({"cpus": 64}, 403),
    ({"wall_time_seconds": None}, 400),
    ({"uid": 0}, 400),
    ({"idempotency_key": ""}, 400),
    ({"command": []}, 400),
    ({"upload_id": "u-0000000000000000"}, 404),
])
def test_policy_and_schema_refusals_change_nothing(gateway, changes, status) -> None:
    client, key, _state, broker_root = gateway
    alice = key()
    upload_id = _upload(client, alice, _tar())
    body = {**_job(upload_id), **changes}
    refused = client.post("/v1/jobs", headers=alice, json=body)
    assert refused.status_code == status, refused.text
    with broker_store.BrokerStore.open(broker_root) as store:
        assert store.scan()[0] == []


def test_another_principal_sees_neither_uploads_nor_jobs(gateway) -> None:
    client, key, _state, _broker = gateway
    alice, bob = key("alice"), key("bob")
    upload_id = _upload(client, alice, _tar())
    job_id = client.post("/v1/jobs", headers=alice, json=_job(upload_id)).json()["job_id"]
    assert client.get(f"/v1/uploads/{upload_id}", headers=bob).status_code == 404
    assert client.post("/v1/jobs", headers=bob, json=_job(upload_id)).status_code == 404
    assert client.get(f"/v1/jobs/{job_id}", headers=bob).status_code == 404
    assert client.get("/v1/jobs", headers=bob).json()["jobs"] == []


def test_a_read_only_key_cannot_upload_or_submit(gateway) -> None:
    client, key, _state, _broker = gateway
    reader = key("carol", scopes=("read",))
    assert client.post("/v1/uploads", headers=reader,
                       json={"size": 10, "sha256": "0" * 64}).status_code == 403
    assert client.post("/v1/jobs", headers=reader, json=_job("u-1")).status_code == 403


def test_the_waiting_job_limit_applies_but_a_replay_passes(gateway) -> None:
    client, key, state, _broker = gateway
    state["project"] = _project(max_active_jobs=1)
    alice = key()
    upload_id = _upload(client, alice, _tar())
    assert client.post("/v1/jobs", headers=alice, json=_job(upload_id)).status_code == 202
    second = client.post("/v1/jobs", headers=alice,
                         json=_job(upload_id, idempotency_key="run-0002"))
    assert second.status_code == 429
    assert client.post("/v1/jobs", headers=alice, json=_job(upload_id)).status_code == 200


def test_a_lost_ownership_write_is_repaired_by_the_retry(gateway, monkeypatch) -> None:
    from vq.gateway import jobs as gw_jobs

    client, key, _state, broker_root = gateway
    alice = key()
    upload_id = _upload(client, alice, _tar())
    real = gw_jobs.JobIndex.record
    calls = {"n": 0}

    def crash_once(self, broker_job_id, owner):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("crash after broker admission")
        return real(self, broker_job_id, owner)

    monkeypatch.setattr(gw_jobs.JobIndex, "record", crash_once)
    assert client.post("/v1/jobs", headers=alice, json=_job(upload_id)).status_code == 500
    retry = client.post("/v1/jobs", headers=alice, json=_job(upload_id))
    assert retry.status_code == 202, retry.text
    with broker_store.BrokerStore.open(broker_root) as store:
        records, _problems = store.scan()
    assert [r.broker_job_id for r in records] == [retry.json()["job_id"]]
    assert client.get(f"/v1/jobs/{retry.json()['job_id']}", headers=alice).status_code == 200
