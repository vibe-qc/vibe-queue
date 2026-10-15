"""A rollout can abandon a host whose started action failed verifiably (#144).

An obsolete rollout's helper update on a scheduler host executed and failed;
its plan-bound holds on the host's scheduler group then blocked every
rollout under the newer report. Superseding needs healthy lanes, and #96
abandonment needed never-started actions. Now an action whose every attempt
is harvested and whose durable operation completed with an executed nonzero
exit also qualifies. The failure fence stays for the next rollout to consume.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from tests.test_fleet_rollout import (
    _healthy_plan_bound_supersede_action,
    _plan_bound_hold,
    _report_at,
    _supported_drain_snapshot,
)
from tests.test_fleet_rollout_abandon_96 import Control
from vq import fleet_operation, fleet_rollout

ROLLOUT = "v0.17.11-0123456789ab"
HELPER = "helper:host_f"
LANES = (
    "scheduler-runtime:host_f:vibe-view",
    "scheduler-runtime:host_f:vibeqc-dev",
    "scheduler-runtime:host_f:vibeqc-release",
)
FAILED = {"status": "failed", "executed": True, "returncode": 1}


@pytest.fixture
def failed_helper(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Journal: helper:host_f failed rc 1 (harvested), its lanes never ran."""
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    report = _report_at(157, digest="d" * 64)
    action = _healthy_plan_bound_supersede_action(
        report=report, phase="helper", program="vibeqc-queue")
    identity = fleet_rollout._operation_identity(
        ROLLOUT, report.digest_sha256, action, attempt=1)
    state = {"result": dict(FAILED), "state": "completed", "raise": None}

    def observation() -> fleet_operation.OperationObservation:
        return fleet_operation.OperationObservation(
            operation_id=fleet_operation.operation_id(identity),
            identity=identity,
            request_sha256="c" * 64,
            state=state["state"],
            retry_safe=False,
            lease_busy=False,
            request={},
            ready={},
            authorization={},
            activation={},
            result=state["result"],
        )

    def observe(operation: str, *, recover: bool = False, **kwargs: Any):
        assert recover is False, "abandonment must only inspect the operation"
        assert operation == fleet_operation.operation_id(identity)
        if state["raise"] is not None:
            raise state["raise"]
        return observation()

    monkeypatch.setattr(fleet_operation, "observe_operation", observe)
    attempt = fleet_rollout._attempt_ref(
        observation(), identity, request_sha256="c" * 64, harvested=True)
    holds = {
        host: _plan_bound_hold(ROLLOUT, host=host) for host in ("host_f", "alias-a")
    }
    fleet_rollout.save_run(fleet_rollout.RolloutRun(
        rollout_id=ROLLOUT,
        report_digest_sha256=report.digest_sha256,
        report_source_path="releases/v0.17.11.json",
        actions={
            HELPER: {"status": "failed", "rc": 1, "operation_attempts": [attempt]},
            **{lane: {"status": "not-run"} for lane in LANES},
        },
        holds=holds,
    ))
    return holds, state, attempt


def _own_lease(hold: dict[str, Any], host: str) -> dict[str, Any]:
    return _supported_drain_snapshot(
        host=host, reason=hold["reason"], lease_owner=hold["lease_owner"])


def _abandon(host: str, control) -> fleet_rollout.AbandonedHost:
    return fleet_rollout.abandon_deferred_host(
        rollout_id_value=ROLLOUT, host=host, control_runner=control)


def test_a_verifiably_failed_host_is_abandoned_with_its_alias(failed_helper) -> None:
    holds, _state, attempt = failed_helper

    for host in ("host_f", "alias-a"):
        result = _abandon(host, Control(_own_lease(holds[host], host)))
        assert result.actions == (HELPER, *LANES)
        assert result.failed_actions == (HELPER,)

    run = fleet_rollout.load_run(ROLLOUT)
    assert run is not None
    for host in ("host_f", "alias-a"):
        hold = run.holds[host]
        assert hold["status"] == "released" and hold["abandoned_at"]
        assert hold["abandoned_failed_actions"] == [HELPER]
    # Nothing blocks a newer report's plan any more ...
    assert fleet_rollout._journaled_scheduler_holds_requiring_plan(run) == set()
    # ... and the failure fence is left for the next rollout to consume.
    (kept,) = fleet_rollout._operation_attempts(run.actions[HELPER])
    assert kept == attempt
    assert kept["failure_fence_consumed"] is False
    assert run.actions[HELPER]["status"] == "failed"

    replay = _abandon("host_f", lambda *a, **k: pytest.fail("replay touched the host"))
    assert replay.replayed and replay.failed_actions == (HELPER,)


@pytest.mark.parametrize(
    "tamper",
    ["not-harvested", "outcome-unknown", "not-executed", "exit-zero", "unreadable"],
)
def test_an_unverified_failure_still_refuses(failed_helper, tamper: str) -> None:
    holds, state, attempt = failed_helper
    if tamper == "not-harvested":
        run = fleet_rollout.load_run(ROLLOUT)
        assert run is not None
        run.actions[HELPER] = {
            **run.actions[HELPER],
            "operation_attempts": [{**attempt, "harvested": False}],
        }
        fleet_rollout.save_run(run)
    elif tamper == "outcome-unknown":
        state["state"], state["result"] = "outcome-unknown", None
    elif tamper == "not-executed":
        state["result"] = {"status": "failed", "executed": False, "returncode": None}
    elif tamper == "exit-zero":
        state["result"] = {"status": "success", "executed": True, "returncode": 0}
    else:
        state["raise"] = fleet_operation.OperationError("unreadable receipt")
    control = Control(_own_lease(holds["host_f"], "host_f"))

    with pytest.raises(fleet_rollout.FleetRolloutError, match="verified failure"):
        _abandon("host_f", control)

    run = fleet_rollout.load_run(ROLLOUT)
    assert run is not None and run.holds["host_f"]["status"] == "active"
    assert control.calls == []


def test_a_successful_action_still_refuses(failed_helper) -> None:
    holds, _state, attempt = failed_helper
    run = fleet_rollout.load_run(ROLLOUT)
    assert run is not None
    run.actions[LANES[0]] = {"status": "success", "operation_attempts": [attempt]}
    fleet_rollout.save_run(run)

    with pytest.raises(fleet_rollout.FleetRolloutError, match="already started"):
        _abandon("host_f", Control(_own_lease(holds["host_f"], "host_f")))


def test_finalizing_still_leaves_a_failed_host_alone(failed_helper) -> None:
    holds, _state, _attempt = failed_helper
    control = Control(_own_lease(holds["host_f"], "host_f"))
    run = fleet_rollout.load_run(ROLLOUT)
    assert run is not None

    finished = fleet_rollout.finalize_run(run, complete=False, control_runner=control)

    assert finished.holds["host_f"]["status"] == "active"
    assert control.calls == []


def test_cli_text_names_the_failed_actions(failed_helper, monkeypatch) -> None:
    from click.testing import CliRunner

    from vq.cli import main

    holds, _state, _attempt = failed_helper
    control = Control(_own_lease(holds["host_f"], "host_f"))
    real = fleet_rollout.abandon_deferred_host
    monkeypatch.setattr(fleet_rollout, "abandon_deferred_host",
                        lambda **kw: real(**kw, control_runner=control))

    done = CliRunner().invoke(main, ["admin", "rollout-latest", "--resume", ROLLOUT,
                                     "--abandon-host", "host_f"])

    assert done.exit_code == 0, done.output
    assert "(3 never-started, 1 verifiably failed action(s)); hold released" in done.output
    as_json = CliRunner().invoke(main, ["admin", "rollout-latest", "--resume", ROLLOUT,
                                        "--abandon-host", "host_f", "--json"])
    assert json.loads(as_json.output)["status"] == "already-abandoned"
