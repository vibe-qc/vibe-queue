"""`vq wait` for a broker job (#64): poll the broker until the target job ends."""
from __future__ import annotations

import json
import subprocess

import pytest
from click.testing import CliRunner

from tests.test_broker_relay_64 import broker, client  # noqa: F401  (fixtures)
from vq import transport, wait
from vq.cli import main
from vq.spec import JobState

JOB = "b-0123456789ab"


class Clock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = 0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps += 1
        self.now += seconds


def _views(*views):
    """A read_view that returns each view in turn, raising the exceptions."""
    pending = list(views)

    def read():
        view = pending.pop(0) if len(pending) > 1 else pending[0]
        if isinstance(view, Exception):
            raise view
        return view

    return read


def _placed(target, **extra):
    return {"state": "placed", "target_host": "alpha", "target_job_id": "0123456789ab",
            "target_status": target, **extra}


def _wait(read, **kwargs):
    clock = Clock()
    kwargs.setdefault("poll_interval", 1.0)
    result = wait.wait_for_broker_job(JOB, read, broker_host="fleet",
                                      _now=clock.monotonic, _sleep=clock.sleep, **kwargs)
    return result, clock


def test_a_held_job_is_pending_until_its_target_job_completes():
    result, clock = _wait(_views(
        {"state": "held", "blockers": {"alpha": "busy"}},
        {"state": "transferring", "target_host": "alpha"},
        _placed({"state": "running"}),
        _placed({"state": "completed", "exit_code": 0,
                 "terminal_diagnosis": {"category": "completed"}}),
    ))
    assert (result.jobid, result.state, result.cli_exit_code) == (JOB, JobState.COMPLETED, 0)
    assert result.terminal_diagnosis == {"category": "completed"}
    assert result.queue_handle == {"job_id": JOB, "host": "fleet", "submitted_at": None}
    assert clock.sleeps == 3


def test_a_failed_target_job_passes_its_exit_code_through():
    result, _ = _wait(_views(_placed({"state": "failed", "exit_code": 3})))
    assert (result.state, result.cli_exit_code) == (JobState.FAILED, 3)


def test_a_job_cancelled_before_placement_ends_as_killed():
    result, _ = _wait(_views({"state": "cancelled",
                              "cancellation": {"reason": "no longer needed"}}))
    assert (result.state, result.exit_code, result.cli_exit_code) == (JobState.KILLED, None, 1)
    assert result.terminal_diagnosis["category"] == "manual_kill"
    assert result.terminal_diagnosis["reason"] == "no longer needed"


def test_an_unavailable_poll_is_retried():
    result, clock = _wait(_views(
        wait.BrokerViewUnavailable("ssh: connection reset"),
        _placed(None, target_status_error="target unreachable"),
        _placed({"state": "no-such-state"}),
        {"state": "unheard-of"},
        _placed({"state": "completed", "exit_code": 0}),
    ))
    assert result.state == JobState.COMPLETED and clock.sleeps == 4


def test_a_timeout_names_the_broker_state_blockers_and_operator_action():
    view = {"state": "transferring", "target_host": "alpha",
            "diagnostics": ["first", "ssh timed out"],
            "operator_action": "check alpha for job key b-0123456789ab"}
    with pytest.raises(wait.WaitTimeout) as caught:
        _wait(_views(view), timeout=3.0)
    assert caught.value.last_state == JobState.PENDING
    detail = caught.value.detail
    assert "broker_state=transferring" in detail and "target_host=alpha" in detail
    assert "diagnostic=ssh timed out" in detail and "OPERATOR ACTION: check alpha" in detail
    assert caught.value.queue_handle["host"] == "fleet"

    with pytest.raises(wait.WaitTimeout) as caught:
        _wait(_views({"state": "held", "blockers": {"beta": "mem", "alpha": "cpus"}}),
              timeout=1.0)
    assert "blockers=alpha: cpus, beta: mem" in caught.value.detail


def test_a_timeout_after_placement_names_the_target_job():
    with pytest.raises(wait.WaitTimeout) as caught:
        _wait(_views(_placed({"state": "running"})), timeout=1.0)
    assert caught.value.last_state == JobState.RUNNING
    assert caught.value.detail.startswith("placed on alpha as 0123456789ab")


# -- CLI ---------------------------------------------------------------------------


def test_a_client_waits_through_its_broker_entry(client, monkeypatch):  # noqa: F811
    answers = [{"state": "held"}, _placed({"state": "completed", "exit_code": 0})]
    polled = []

    def run_vq(host_cfg, *argv, **kwargs):
        polled.append((host_cfg.ssh, list(argv)))
        return subprocess.CompletedProcess(argv, 0, stdout=json.dumps(answers.pop(0)))

    monkeypatch.setattr(transport, "run_remote_vq", run_vq)
    result = CliRunner().invoke(main, ["wait", JOB, "--poll-interval", "0.1", "--json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert (payload["jobid"], payload["state"]) == (JOB, "completed")
    assert payload["queue_handle"]["host"] == "fleet"
    assert polled == [("coordinator", ["status", "localhost", JOB, "--json"])] * 2


def test_on_the_broker_a_placed_job_ends_with_its_target_state(broker, monkeypatch):  # noqa: F811
    record = broker.job("placed")

    def run_vq(host_cfg, *argv, **kwargs):
        broker.calls.append((host_cfg.ssh, list(argv)))
        return subprocess.CompletedProcess(
            argv, 0, stdout=json.dumps({"state": "failed", "exit_code": 7}))

    monkeypatch.setattr(transport, "run_remote_vq", run_vq)
    result = CliRunner().invoke(main, ["wait", "localhost", record.broker_job_id])
    assert result.exit_code == 7, result.output
    assert f"{record.broker_job_id}: failed" in result.output
    assert broker.calls == [("alpha", ["status", "localhost", "0123456789ab", "--json"])]


def test_on_the_broker_a_held_job_times_out_as_pending(broker):  # noqa: F811
    record = broker.job()
    result = CliRunner().invoke(main, ["wait", "localhost", record.broker_job_id,
                                       "--timeout", "0.1", "--poll-interval", "0.1", "--json"])
    assert result.exit_code == 124, result.output
    payload = json.loads(result.output)
    assert payload["state"] == "pending" and "broker_state=held" in payload["detail"]


def test_on_the_broker_a_cancelled_job_ends_as_killed(broker):  # noqa: F811
    record = broker.job()
    assert CliRunner().invoke(main, ["kill", "localhost", record.broker_job_id]).exit_code == 0
    result = CliRunner().invoke(main, ["wait", "localhost", record.broker_job_id, "--json"])
    assert result.exit_code == 1, result.output
    assert json.loads(result.output)["state"] == "killed"


def test_on_the_broker_an_unknown_broker_job_is_a_usage_error(broker):  # noqa: F811
    result = CliRunner().invoke(main, ["wait", "localhost", JOB])
    assert result.exit_code == 2 and f"no broker record {JOB}" in result.output
