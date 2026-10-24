"""Gateway slice 3 (#163): paged logs and cancellation of owned jobs."""
from __future__ import annotations

import subprocess
from typing import Any

import pytest

from tests.test_broker_store_64 import _snapshot
from tests.test_gateway_submit_163 import _job, _tar, _upload
from tests.test_gateway_submit_163 import gateway as gateway  # noqa: F401 — fixture
from vq import broker_store, overview, transport

TARGET_JOB = "0123456789ab"


@pytest.fixture
def remote(monkeypatch):
    calls: list[list[str]] = []
    replies: dict[str, Any] = {}

    def run_vq(host_cfg, *argv, **kwargs):
        calls.append(list(argv))
        reply = replies.get(argv[0], "")
        if isinstance(reply, Exception):
            raise reply
        if isinstance(reply, dict):
            reply = reply.get(next(a for a in argv if a in ("--stdout", "--stderr")), "")
        return subprocess.CompletedProcess(argv, 0, stdout=reply, stderr="")

    monkeypatch.setattr(transport, "run_remote_vq", run_vq)
    return calls, replies


def _submitted(gateway_fixture, principal="alice", scopes=("submit", "read", "cancel")):
    client, key, _state, broker_root = gateway_fixture
    headers = key(principal, scopes=scopes)
    upload_id = _upload(client, headers, _tar())
    job_id = client.post("/v1/jobs", headers=headers, json=_job(upload_id)).json()["job_id"]
    return client, headers, job_id, broker_root


def _advance(broker_root, job_id: str, state: str) -> None:
    with broker_store.BrokerStore.open(broker_root) as store:
        record = store.get(job_id)
        decision = overview.place_broker_job(
            [_snapshot(host="target")], record.intent.placement_request(), now=105.0,
            max_age_seconds=1e9)
        record = store.reserve(job_id, record.revision, decision=decision,
                               snapshot=_snapshot(host="target"), target_state_root="/s",
                               now=106.0)
        record = store.begin_transfer(job_id, record.revision, now=107.0)
        if state == "placed":
            store.record_placed(job_id, record.revision,
                                request_fingerprint=record.current_attempt.request_fingerprint,
                                target_job_id=TARGET_JOB, now=108.0)


def test_logs_of_a_held_job_are_empty_and_touch_no_host(gateway, remote) -> None:
    client, headers, job_id, _root = _submitted(gateway)
    calls, _replies = remote
    shown = client.get(f"/v1/jobs/{job_id}/logs", headers=headers)
    assert shown.status_code == 200
    assert shown.json() == {"job_id": job_id, "state": "held", "placed": False,
                            "stream": "stdout", "offset": 0, "data": "", "next_offset": 0}
    assert calls == []


def test_logs_of_a_placed_job_page_by_byte_offset(gateway, remote) -> None:
    client, headers, job_id, root = _submitted(gateway)
    calls, replies = remote
    _advance(root, job_id, "placed")
    replies["logs"] = {"--stdout": "hello\nworld\n", "--stderr": "warn\n"}

    first = client.get(f"/v1/jobs/{job_id}/logs?limit=6", headers=headers).json()
    assert (first["data"], first["next_offset"], first["size"]) == ("hello\n", 6, 12)
    second = client.get(f"/v1/jobs/{job_id}/logs?offset=6&limit=6", headers=headers).json()
    assert second["data"] == "world\n" and second["next_offset"] == 12
    err = client.get(f"/v1/jobs/{job_id}/logs?stream=stderr", headers=headers).json()
    assert err["data"] == "warn\n"
    assert calls[0] == ["logs", "localhost", TARGET_JOB, "--stdout", "-n", "0"]
    assert client.get(f"/v1/jobs/{job_id}/logs?stream=env", headers=headers
                      ).status_code == 400


def test_an_unreachable_target_is_a_502_without_detail(gateway, remote) -> None:
    client, headers, job_id, root = _submitted(gateway)
    _calls, replies = remote
    _advance(root, job_id, "placed")
    replies["logs"] = transport.RemoteError("ssh to internal-host-7 failed")
    failed = client.get(f"/v1/jobs/{job_id}/logs", headers=headers)
    assert failed.status_code == 502 and "internal-host-7" not in failed.text


def test_another_principal_can_neither_read_nor_cancel(gateway, remote) -> None:
    client, _headers, job_id, _root = _submitted(gateway)
    _client, key, _state, _broker = gateway
    bob = key("bob", scopes=("read", "cancel"))
    assert client.get(f"/v1/jobs/{job_id}/logs", headers=bob).status_code == 404
    assert client.post(f"/v1/jobs/{job_id}/cancel", headers=bob).status_code == 404


def test_cancel_needs_the_cancel_scope(gateway, remote) -> None:
    client, _headers, job_id, _root = _submitted(gateway)
    _client, key, _state, _broker = gateway
    reader = key("alice", scopes=("read",))
    assert client.post(f"/v1/jobs/{job_id}/cancel", headers=reader).status_code == 403


def test_cancelling_a_held_job_stops_it_without_a_host_call(gateway, remote) -> None:
    client, headers, job_id, _root = _submitted(gateway)
    calls, _replies = remote
    done = client.post(f"/v1/jobs/{job_id}/cancel", headers=headers)
    assert done.status_code == 200, done.text
    assert done.json()["state"] == "cancelled" and done.json()["stopped"] is True
    again = client.post(f"/v1/jobs/{job_id}/cancel", headers=headers)
    assert again.status_code == 200 and again.json()["stopped"] is True
    assert calls == []


def test_a_job_a_broker_pass_left_held_can_be_shown_and_cancelled(gateway, remote) -> None:
    # A broker pass that finds no ready target records its blockers on the held
    # job as (host, reason) pairs; showing or cancelling it used to fail with 500.
    client, headers, job_id, root = _submitted(gateway)
    calls, _replies = remote
    with broker_store.BrokerStore.open(root) as store:
        record = store.get(job_id)
        store.record_held_blockers(
            job_id, record.revision, now=105.0,
            blockers=(("target", "daemon-not-ready: queue paused"), ("other", "unreachable")))
    shown = client.get(f"/v1/jobs/{job_id}", headers=headers)
    assert shown.status_code == 200, shown.text
    assert shown.json()["state"] == "held"
    assert shown.json()["waiting_reasons"] == ["daemon-not-ready", "unreachable"]
    done = client.post(f"/v1/jobs/{job_id}/cancel", headers=headers)
    assert done.status_code == 200, done.text
    assert done.json()["state"] == "cancelled" and done.json()["stopped"] is True
    assert calls == []


def test_cancelling_a_placed_job_kills_it_on_its_target(gateway, remote) -> None:
    client, headers, job_id, root = _submitted(gateway)
    calls, replies = remote
    _advance(root, job_id, "placed")
    replies["kill"] = "killed\n"
    done = client.post(f"/v1/jobs/{job_id}/cancel", headers=headers).json()
    assert done["stopped"] is True and done["cancellation"]["requested_at"]
    assert calls[0][:3] == ["kill", "localhost", TARGET_JOB]
    assert "gateway" in calls[0][calls[0].index("--reason") + 1]


def test_a_transfer_in_doubt_is_reported_as_not_stopped(gateway, remote) -> None:
    client, headers, job_id, root = _submitted(gateway)
    calls, _replies = remote
    _advance(root, job_id, "transferring")
    done = client.post(f"/v1/jobs/{job_id}/cancel", headers=headers).json()
    assert done["stopped"] is False and done["state"] == "transferring"
    assert calls == []
