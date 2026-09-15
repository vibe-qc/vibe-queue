"""Canonical scheduler parity and the user-visible scoped verdict agree."""

from __future__ import annotations

import copy
import json
from contextlib import nullcontext

import pytest
from click.testing import CliRunner

from tests.test_fleet_rollout_cli import _action, _patch_discovery, _plan, _report
from vq import fleet_rollout as rollout
from vq.cli import main

TARGETS = (
    "host_f",
    "host_f-amd",
    "host_f-big",
    "host_f-big2",
    "host_f-big3",
    "host_f-inf",
    "host_f-itwin",
    "host_f-jtwin",
)


def case():
    report = _report()
    plan = _plan(
        report,
        [
            _action(action_id="driver", decision="skip"),
            _action(action_id="host_f-helper", phase="helper", host="host_f", decision="skip"),
        ],
        extra_hosts=(*TARGETS, "host_c", "host_f-unrelated"),
    )
    for target in TARGETS[1:]:
        plan.topology[target].update(role="alias", canonical_host="host_f")
    # A similar name does not establish a binding to the selected group.
    plan.topology["host_f-unrelated"].update(role="alias", canonical_host="host_c")
    plan._scheduler_hold_targets = {
        "host_f": tuple((name, "localhost") for name in TARGETS),
        "host_c": (("host_c", "localhost"), ("host_f-unrelated", "localhost")),
    }
    doctor = {name: {"ok": True, "checks": []} for name in plan.topology}
    run = rollout.RolloutRun(
        rollout_id=rollout.rollout_id(report),
        report_digest_sha256=report.digest_sha256,
        report_source_path=report.source_path,
    )
    return report, plan, doctor, run


def hold(host, *, status="active", owned=True):
    return {
        "host": host,
        "action_host": "host_f",
        "kind": "scheduler-target",
        "owned": owned,
        "status": status,
        "reason": "exact rollout claim",
    }


def result(plan, doctor, run=None, *, only="host_f", initial=None):
    return rollout.result_payload(
        initial=initial,
        verification=plan,
        doctor=doctor,
        run=run,
        selection=rollout.HostSelection(only=(only,)) if only else None,
    )


@pytest.mark.parametrize("missing", [False, True])
def test_selected_canonical_requires_every_bound_alias_doctor(missing):
    _, plan, doctor, _ = case()
    if missing:
        del doctor["host_f-amd"]
    else:
        doctor["host_f-amd"] = {
            "ok": False,
            "checks": [{"name": "daemon_rpc", "ok": False}],
        }
    payload = result(plan, doctor)
    assert payload["selection"]["verdict"] == "degraded"
    assert "host_f-amd" in payload["selection"]["degraded_hosts"]
    restricted = rollout.restrict_plan(plan, rollout.HostSelection(only=("host_f",)))
    assert {a.id for a in restricted.actions} == {"host_f-helper"}
    assert set(TARGETS) <= restricted.topology.keys()
    assert "host_c" not in restricted.topology


@pytest.mark.parametrize("status", ["active", "cleanup-failed"])
@pytest.mark.parametrize("host", ["host_f", "host_f-amd", "localhost"])
def test_unreleased_owned_hold_prevents_convergence_without_claiming_liveness(status, host):
    _, plan, doctor, run = case()
    run.holds[host] = hold(host, status=status)
    before = copy.deepcopy(run.as_dict())
    payload = result(plan, doctor, run)
    assert payload["status"] != "complete"
    assert payload["selection"]["verdict"] == "degraded"
    assert host in payload["selection"]["degraded_hosts"]
    assert "unconfirmed" in " ".join(payload["selection"]["degraded_hosts"][host])
    assert "drain_liveness" not in payload
    assert run.as_dict() == before


def test_original_group_hold_remains_visible_if_final_binding_shrinks():
    _, initial, doctor, run = case()
    final = copy.deepcopy(initial)
    del final.topology["host_f-amd"]
    final._scheduler_hold_targets["host_f"] = (("host_f", "localhost"),)
    run.holds["host_f-amd"] = hold("host_f-amd")
    payload = result(final, doctor, run, initial=initial)
    assert payload["selection"]["verdict"] == "degraded"
    assert "host_f-amd" in payload["selection"]["degraded_hosts"]


@pytest.mark.parametrize("kind", ["external", "released", "unrelated"])
def test_scoped_success_preserves_external_released_and_unrelated_hold_semantics(kind):
    _, plan, doctor, run = case()
    doctor["host_c"] = {"ok": False, "checks": []}
    doctor["host_f-unrelated"] = {"ok": False, "checks": []}
    if kind == "external":
        run.holds["host_f"] = hold("host_f", owned=False)
    elif kind == "released":
        run.holds["host_f"] = hold("host_f", status="released")
    else:
        run.holds["host_c"] = hold("host_c")
    payload = result(plan, doctor, run)
    assert payload["selection"]["verdict"] == "converged"
    assert payload["selection"]["degraded_hosts"] == {}
    assert payload["status"] == "blocked"
    assert "host_c" in payload["doctor_failures"]
    if kind == "external":
        assert payload["preserved_external_holds"]
        assert not payload["retained_rollout_holds"]


def test_healthy_unscoped_run_with_retained_owned_hold_is_not_complete():
    _, plan, doctor, run = case()
    run.holds["host_f-amd"] = hold("host_f-amd")
    payload = result(plan, doctor, run, only=None)
    assert payload["status"] != "complete"
    assert payload["selection"]["verdict"] == "degraded"


def test_pure_verification_includes_bound_alias_health():
    _, plan, doctor, _ = case()
    doctor["host_f-amd"] = {"ok": False, "checks": []}
    scoped = rollout.restrict_plan(plan, rollout.HostSelection(only=("host_f",)))
    assert "host_f-amd" in rollout.degraded_hosts(scoped, doctor)


@pytest.mark.parametrize("retained", [False, True])
def test_skipping_canonical_excludes_its_alias_failures_and_owned_holds(retained):
    _, plan, doctor, run = case()
    doctor["host_f-amd"] = {"ok": False, "checks": []}
    if retained:
        run.holds["host_f-amd"] = hold("host_f-amd")
    selection = rollout.HostSelection(skip=("host_f",))
    payload = rollout.result_payload(
        initial=plan,
        verification=plan,
        doctor=doctor,
        run=run,
        selection=selection,
    )
    assert payload["selection"]["verdict"] == "converged"
    assert payload["status"] == "blocked"
    assert "host_f-amd" in payload["doctor_failures"]
    restricted = rollout.restrict_plan(plan, selection)
    assert set(TARGETS).isdisjoint(restricted.topology)


def test_alias_legacy_hold_degrades_its_selected_canonical_scope():
    _, plan, doctor, _ = case()
    payload = rollout.result_payload(
        initial=plan,
        verification=plan,
        doctor=doctor,
        run=None,
        selection=rollout.HostSelection(only=("host_f",)),
        retained_legacy_holds=(("old-report", "host_f-amd", "exact cleanup pending"),),
    )
    assert payload["selection"]["verdict"] == "degraded"
    assert "host_f-amd" in payload["selection"]["degraded_hosts"]


@pytest.mark.parametrize("alias_bad,retained", [(True, False), (False, True), (True, True)])
def test_cli_scoped_result_exits_two_for_alias_or_unreleased_owned_hold(
    monkeypatch,
    alias_bad,
    retained,
):
    report, plan, doctor, run = case()
    if alias_bad:
        doctor["host_f-amd"] = {
            "ok": False,
            "checks": [{"name": "daemon_rpc", "ok": False}],
        }
    if retained:
        run.holds = {target: hold(target) for target in TARGETS}
    _patch_discovery(
        monkeypatch,
        report=report,
        plans=[plan, plan],
        extra_hosts=(*TARGETS, "host_c", "host_f-unrelated"),
        doctor=doctor,
    )
    monkeypatch.setattr(rollout, "rollout_execution_lock", lambda *a, **k: nullcontext())
    monkeypatch.setattr("vq.cli.admin_module.toolset_lifecycle_lock", lambda *a, **k: nullcontext())
    monkeypatch.setattr(
        rollout, "reconcile_durable_operations", lambda **k: rollout.OperationReconciliation()
    )
    monkeypatch.setattr(
        rollout, "abort_unselected_pre_authorization_operations", lambda *a, **k: None
    )
    monkeypatch.setattr(rollout, "execute_plan", lambda *a, **k: run)
    monkeypatch.setattr(rollout, "reconcile_scheduler_parity_holds", lambda *a, **k: run)
    monkeypatch.setattr(rollout, "finalize_run", lambda *a, **k: run)
    monkeypatch.setattr("vq.cli.subprocess.run", lambda *a, **k: pytest.fail("unexpected command"))
    response = CliRunner().invoke(main, ["admin", "rollout-latest", "--only", "host_f", "--json"])
    payload = json.loads(response.output)
    assert response.exit_code == 2, (response.output, response.exception)
    assert payload["selection"]["verdict"] == "degraded"
    if retained:
        assert len(payload["retained_rollout_holds"]) == 8
