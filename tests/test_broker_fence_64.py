"""Settle a broker transfer in doubt from its target's own fence (#64).

Target side: `fence_idempotency_key` answers "accepted" (with the job) when a
keyed submission landed, repairing a crash-gap claim, and otherwise writes a
fence so a late copy of the request can never be accepted. Broker side:
`vq broker reconcile` turns that proof into `placed` or back into `held`.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from click.testing import CliRunner

from tests.test_broker_relay_64 import broker  # noqa: F401  (fixture)
from vq import broker_service, broker_store, submit, transport
from vq.cli import main

KEY = "b-0123456789ab"


def _submit(tmp_path: Path, key: str) -> str:
    source = tmp_path / "input.py"
    source.write_text("print('once')\n")
    return submit.submit_local(host="localhost", input_file=str(source),
                               queue_dir=tmp_path / "queue", jobs_dir=tmp_path / "jobs",
                               idempotency_key=key)


def test_an_unseen_key_is_fenced_and_a_late_submit_is_refused(tmp_path: Path) -> None:
    queue = tmp_path / "queue"
    queue.mkdir()
    assert submit.fence_idempotency_key(queue, KEY) == {"state": "fenced", "job_id": None}
    assert submit.fence_idempotency_key(queue, KEY) == {"state": "fenced", "job_id": None}
    with pytest.raises(submit.IdempotencyFenced, match="fenced"):
        _submit(tmp_path, KEY)
    assert list(queue.glob("*.json")) == []


def test_a_landed_submission_is_reported_not_fenced(tmp_path: Path) -> None:
    job = _submit(tmp_path, KEY)
    assert submit.fence_idempotency_key(tmp_path / "queue", KEY) == {
        "state": "accepted", "job_id": job}
    assert _submit(tmp_path, KEY) == job  # still an ordinary replay


def test_a_crash_before_the_claim_is_repaired_as_accepted(tmp_path: Path) -> None:
    job = _submit(tmp_path, KEY)
    claims = list((tmp_path / "queue" / ".submit-idempotency").rglob("*.json"))
    assert len(claims) == 1
    claims[0].unlink()  # the spec was published, its claim not yet

    assert submit.fence_idempotency_key(tmp_path / "queue", KEY) == {
        "state": "accepted", "job_id": job}
    assert _submit(tmp_path, KEY) == job


def test_the_cli_answers_json_with_its_state_root(tmp_path: Path, monkeypatch) -> None:
    from vq import paths

    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    result = CliRunner().invoke(main, ["admin", "fence-idempotency-key", KEY])
    assert result.exit_code == 0, result.output
    answer = json.loads(result.output)
    assert answer == {"schema": "vq.idempotency-fence/1", "state": "fenced",
                      "job_id": None, "state_root": str(paths.state_root())}


# -- broker ------------------------------------------------------------------------


def _answer_fence(monkeypatch, *, state: str, job_id=None, state_root="/s") -> list:
    calls = []

    def run_vq(host_cfg, *argv, **kwargs):
        calls.append((host_cfg.ssh, list(argv)))
        return subprocess.CompletedProcess(argv, 0, stdout=json.dumps({
            "schema": "vq.idempotency-fence/1", "state": state,
            "job_id": job_id, "state_root": state_root}))

    monkeypatch.setattr(transport, "run_remote_vq", run_vq)
    return calls


def test_an_accepted_transfer_is_placed_on_that_target_job(broker, monkeypatch) -> None:  # noqa: F811
    record = broker.job("transferring")
    calls = _answer_fence(monkeypatch, state="accepted", job_id="0123456789ab")

    placed = broker_service.reconcile(broker.cfg, record.broker_job_id)

    assert (placed.state, placed.current_attempt.target_job_id) == ("placed", "0123456789ab")
    assert calls == [("alpha", ["admin", "fence-idempotency-key", record.broker_job_id])]


def test_a_fenced_transfer_holds_again_with_its_charge_released(broker, monkeypatch) -> None:  # noqa: F811
    record = broker.job("transferring")
    _answer_fence(monkeypatch, state="fenced")

    held = broker_service.reconcile(broker.cfg, record.broker_job_id)

    assert held.state == "held"
    attempt = held.attempts[-1]
    assert attempt.phase == "returned" and attempt.charge_released_at is not None
    assert attempt.return_reason.startswith(broker_store.FENCED_RETURN_PREFIX)
    with broker_store.BrokerStore.open() as store:
        assert store.get(record.broker_job_id) == held  # the loader accepts the proof
        assert store.outstanding_charges() == {}


def test_a_fenced_transfer_can_be_cancelled_instead(broker, monkeypatch) -> None:  # noqa: F811
    record = broker.job("transferring")
    _answer_fence(monkeypatch, state="fenced")
    result = CliRunner().invoke(main, ["broker", "reconcile", record.broker_job_id, "--cancel"])
    assert result.exit_code == 0, result.output
    assert result.output.strip() == f"{record.broker_job_id}: cancelled"


def test_a_fence_from_another_state_root_changes_nothing(broker, monkeypatch) -> None:  # noqa: F811
    record = broker.job("transferring")
    _answer_fence(monkeypatch, state="fenced", state_root="/elsewhere")
    with pytest.raises(broker_store.BrokerStoreError, match="state root"):
        broker_service.reconcile(broker.cfg, record.broker_job_id)
    with broker_store.BrokerStore.open() as store:
        assert store.get(record.broker_job_id).state == "transferring"


def test_only_a_transfer_in_doubt_is_reconciled(broker, monkeypatch) -> None:  # noqa: F811
    record = broker.job()
    calls = _answer_fence(monkeypatch, state="fenced")
    with pytest.raises(broker_store.BrokerStoreError, match="only a transfer in doubt"):
        broker_service.reconcile(broker.cfg, record.broker_job_id)
    assert calls == []

