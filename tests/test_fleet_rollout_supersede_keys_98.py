"""The supersede health gate accepts the lane evidence build_plan produces (#98)."""
from __future__ import annotations

import dataclasses

import pytest

from tests.test_fleet_rollout import _plan_bound_supersede_plan, _report_at
from vq import fleet_rollout


def _with_lane_state_before(plan, *, probe_unavailable: bool = False):
    """Rebuild every action's ``before`` the way build_plan does: asdict(LaneState)."""
    actions = []
    for action in plan.actions:
        lane = fleet_rollout.LaneState(**action.before, probe_unavailable=probe_unavailable)
        actions.append(dataclasses.replace(action, before=fleet_rollout._before(lane)))
    return dataclasses.replace(plan, actions=actions)


def test_lane_state_evidence_passes_the_gate() -> None:
    report = _report_at(157, digest="c" * 64)
    plan = _with_lane_state_before(_plan_bound_supersede_plan(report))
    assert "probe_unavailable" in plan.actions[0].before

    evidence = fleet_rollout._healthy_scheduler_supersede_actions(
        plan, action_host="host_f", accepted_report=report,
    )

    assert evidence


def test_legacy_nine_key_evidence_still_passes() -> None:
    report = _report_at(157, digest="c" * 64)

    assert fleet_rollout._healthy_scheduler_supersede_actions(
        _plan_bound_supersede_plan(report), action_host="host_f", accepted_report=report,
    )


def test_an_unanswered_probe_is_still_not_a_verdict() -> None:
    report = _report_at(157, digest="c" * 64)
    plan = _with_lane_state_before(_plan_bound_supersede_plan(report), probe_unavailable=True)

    with pytest.raises(fleet_rollout.FleetProbeUnavailable):
        fleet_rollout._healthy_scheduler_supersede_actions(
            plan, action_host="host_f", accepted_report=report,
        )


def test_the_gate_names_every_lane_state_field() -> None:
    fields = {field.name for field in dataclasses.fields(fleet_rollout.LaneState)}

    assert fields == fleet_rollout._LANE_EVIDENCE_KEYS | {"probe_unavailable"}
