"""`--idempotency-key` for broker submissions (#64, SPEC §4.8).

The key names the broker job id. The same key with the same content returns
the job it already created; different content under it is a hard conflict.
"""
from __future__ import annotations

import os

import pytest
from click.testing import CliRunner

from tests.test_broker_relay_64 import HOST, Remote, _request, broker, client  # noqa: F401
from tests.test_broker_store_64 import _archive, _entries, _intent
from vq import broker_relay, broker_store, transport
from vq.broker_store import BrokerStore
from vq.cli import main


@pytest.fixture
def root(tmp_path):
    return tmp_path / "broker"


def test_the_key_names_a_stable_id_and_is_validated():
    first = broker_store.broker_job_id_for_key("calc-42")
    assert first == broker_store.broker_job_id_for_key("calc-42")
    assert first != broker_store.broker_job_id_for_key("calc-43")
    assert broker_store.BROKER_JOB_ID_PATTERN.fullmatch(first)
    with pytest.raises(broker_store.BrokerStoreError, match="idempotency key"):
        broker_store.broker_job_id_for_key("-bad key")


def test_a_repeat_returns_the_same_job_and_stores_nothing_new(root, tmp_path):
    payload = _archive(tmp_path / "p.tar")
    with BrokerStore.open(root) as store:
        first = store.accept(payload, _intent(), now=100.0, idempotency_key="calc-42")
        again = store.accept(payload, _intent(), now=200.0, idempotency_key="calc-42")
        records, problems = store.scan()
    assert again == first and first.broker_job_id == broker_store.broker_job_id_for_key("calc-42")
    assert len(records) == 1 and problems == {}
    assert _entries(root)["staging"] == []


@pytest.mark.parametrize("change", ["intent", "payload"])
def test_other_content_under_the_key_is_a_conflict(root, tmp_path, change):
    payload = _archive(tmp_path / "p.tar")
    with BrokerStore.open(root) as store:
        store.accept(payload, _intent(), now=100.0, idempotency_key="calc-42")
        other_payload = (
            _archive(tmp_path / "q.tar", {"run.py": b"print('other')\n"})
            if change == "payload" else payload
        )
        other_intent = _intent(cpus=8) if change == "intent" else _intent()
        with pytest.raises(broker_store.BrokerIdempotencyConflict, match="different"):
            store.accept(other_payload, other_intent, now=200.0, idempotency_key="calc-42")
        assert len(store.scan()[0]) == 1
    assert _entries(root)["staging"] == []


def test_a_payload_orphaned_by_a_crash_is_adopted_only_when_identical(root, tmp_path):
    payload = _archive(tmp_path / "p.tar")
    job_id = broker_store.broker_job_id_for_key("calc-42")
    with BrokerStore.open(root):
        pass
    # A crash after the payload link and before the record: bytes, no record.
    os.link(payload, root / "payloads" / f"{job_id}.tar")
    with BrokerStore.open(root) as store:
        record = store.accept(payload, _intent(), now=100.0, idempotency_key="calc-42")
        assert record.broker_job_id == job_id
        store.verify_payload(record)

    other_id = broker_store.broker_job_id_for_key("calc-43")
    stray = _archive(tmp_path / "stray.tar", {"run.py": b"print('stray')\n"})
    os.link(stray, root / "payloads" / f"{other_id}.tar")
    with BrokerStore.open(root) as store:
        with pytest.raises(broker_store.BrokerStoreError, match="collides"):
            store.accept(payload, _intent(), now=100.0, idempotency_key="calc-43")
        with pytest.raises(broker_store.BrokerStoreError, match="no broker record"):
            store.get(other_id)
    assert (root / "payloads" / f"{other_id}.tar").read_bytes() == stray.read_bytes()


def test_an_unkeyed_accept_is_unchanged(root, tmp_path):
    payload = _archive(tmp_path / "p.tar")
    with BrokerStore.open(root) as store:
        first = store.accept(payload, _intent(), now=100.0)
        second = store.accept(payload, _intent(), now=100.0)
    assert first.broker_job_id != second.broker_job_id


def test_broker_accept_cli_is_idempotent_under_a_key(broker, tmp_path):  # noqa: F811
    payload = _archive(tmp_path / "upload.tar")
    argv = ["broker", "accept", str(payload), "--cpus", "4", "--mem-mb", "8000",
            "--idempotency-key", "calc-42", "--", "python3", "run.py"]
    first = CliRunner().invoke(main, argv)
    again = CliRunner().invoke(main, argv)
    assert first.exit_code == 0 and again.exit_code == 0, first.output + again.output
    assert first.output == again.output == broker_store.broker_job_id_for_key("calc-42") + "\n"
    conflict = CliRunner().invoke(main, [*argv[:4], "8", *argv[5:]])
    assert conflict.exit_code == 1 and "different" in conflict.output


def test_the_client_relays_the_key(client, tmp_path):  # noqa: F811
    source = tmp_path / "job"
    source.mkdir()
    result = CliRunner().invoke(main, ["submit", "fleet", "-d", str(source), "--cpus", "1",
                                       "--mem-mb", "100", "--idempotency-key", "calc-42",
                                       "--", "bash", "run.sh"])
    assert result.exit_code == 0, result.output
    ((_host_cfg, relay, _kw),) = client
    assert relay.idempotency_key == "calc-42"
    argv = broker_relay.accept_argv("/tmp/x.tar", relay)
    assert argv[argv.index("--idempotency-key") + 1] == "calc-42"
    assert argv[-3:] == ["--", "bash", "run.sh"]


def test_a_lost_answer_under_a_key_says_a_retry_is_safe(tmp_path, monkeypatch):
    remote = Remote(error=transport.RemoteOutcomeUnknown("timed out"))
    remote.install(monkeypatch)
    source = tmp_path / "job"
    source.mkdir()
    (source / "run.py").write_text("print('hi')\n")
    with pytest.raises(broker_relay.BrokerRelayUncertain) as caught:
        broker_relay.submit_via_broker(
            HOST, _request(idempotency_key="calc-42"), directory=str(source))
    message = str(caught.value)
    assert broker_store.broker_job_id_for_key("calc-42") in message
    assert "same --idempotency-key is safe" in message


def test_an_answer_naming_another_job_is_uncertain(tmp_path, monkeypatch):
    Remote(answer="b-0123456789ab").install(monkeypatch)
    source = tmp_path / "job"
    source.mkdir()
    (source / "run.py").write_text("print('hi')\n")
    with pytest.raises(broker_relay.BrokerRelayUncertain, match="names"):
        broker_relay.submit_via_broker(
            HOST, _request(idempotency_key="calc-42"), directory=str(source))
