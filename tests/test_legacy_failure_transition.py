"""Pure tests for the legacy failure host-group transition model."""

from __future__ import annotations

import ast
import copy
import json
import pickle
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import fields, replace
from pathlib import Path
from typing import Any, cast

import pytest

from vq import legacy_failure_transition as transition

HOST = "host_e"
NOW = "2026-08-25T20:30:00+00:00"
SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64
SHA_D = "d" * 64


def _report(name: str, digest: str) -> transition.ReportRef:
    return transition.ReportRef(
        source_path=f"reports/{name}.json",
        digest_sha256=digest,
        rollout_id=name,
    )


def _action(
    action_id: str,
    *,
    host: str = HOST,
    decision: str = "update",
    reason: str = "behind accepted pin",
    pin_name: str = "release",
    argv: tuple[str, ...] | None = None,
    before: Mapping[str, object] | None = None,
) -> transition.PlanActionRef:
    program = action_id.rsplit(":", 1)[-1]
    default_before: dict[str, object] = {
        "configured": True,
        "current_sha": "0" * 40,
        "current_version": "0.15.137",
        "current_tag": "v0.15.137",
        "dirty": False,
        "last_ok": True,
        "acknowledged": True,
        "detail": "authenticated pre-update lane state",
        "metrics": {"cache": {"hit": True, "ratio": 0.5}},
        "required": False,
    }
    return transition.PlanActionRef(
        action_id=action_id,
        phase="local-runtime",
        host=host,
        program=program,
        decision=decision,
        reason=reason,
        pin_name=pin_name,
        target_sha="1" * 40,
        target_version="0.15.142",
        target_tag="v0.15.142",
        argv=("admin", "update", program, host) if argv is None else argv,
        before=default_before if before is None else before,
    )


def _failure(
    report: transition.ReportRef,
    *,
    host: str = HOST,
    reason: str = "release failed rc=9",
    action_ids: tuple[str, ...] = ("local-runtime:host_e:vibeqc-release",),
    consumed: frozenset[str] = frozenset(),
    skip_states: tuple[tuple[str, transition.SkipState], ...] = (),
) -> transition.HostFailureGroup:
    return transition.HostFailureGroup(
        report=report,
        host=host,
        reason=reason,
        attempts=tuple(
            transition.FailureAttemptRef(
                action_id=action_id,
                operation_id=f"{index + 1:x}" * 64,
                consumed=action_id in consumed,
            )
            for index, action_id in enumerate(action_ids)
        ),
        skip_states=skip_states,
    )


def _settled_hold(
    host: str = HOST,
    *,
    outcome: str = "observed-inactive",
) -> dict[str, object]:
    return {
        "host": host,
        "kind": "full",
        "owned": True,
        "status": "released",
        "legacy_reconciliation": {"outcome": outcome},
    }


def _proof(
    report: transition.ReportRef,
    host: str = HOST,
    *,
    kind: transition.HoldProofKind = transition.HoldProofKind.OBSERVED_INACTIVE,
    outcome: str = "observed-inactive",
) -> transition.HoldProof:
    settled = _settled_hold(host, outcome=outcome)
    return transition.HoldProof(
        old_rollout_id=report.rollout_id,
        kind=kind,
        settled_hold=settled,
        settled_hold_sha256=transition.canonical_json_sha256(settled),
    )


def _existing_proof(
    report: transition.ReportRef,
    settled: dict[str, object] | None = None,
    host: str = HOST,
) -> transition.HoldProof:
    record = _settled_hold(host) if settled is None else settled
    return transition.HoldProof(
        old_rollout_id=report.rollout_id,
        kind=transition.HoldProofKind.EXISTING_BACKLINK,
        settled_hold=record,
        settled_hold_sha256=transition.canonical_json_sha256(record),
    )


def _token(report: transition.ReportRef, digest: str) -> transition.JournalToken:
    return transition.JournalToken(report.rollout_id, digest)


def _snapshot(
    report: transition.ReportRef,
    *,
    host: str = HOST,
    token_digest: str,
    records: Mapping[str, object] | None = None,
    hold: Mapping[str, object] | None = None,
    retained: Mapping[str, object] | None = None,
    failed_reason: str | None = None,
    complete: bool = False,
    intent: object | None = None,
) -> transition.JournalHostSnapshot:
    return transition.JournalHostSnapshot(
        report=report,
        host=host,
        token=_token(report, token_digest),
        relevant_action_records={} if records is None else records,
        hold=hold,
        retained_hold=retained,
        failed_reason=failed_reason,
        complete=complete,
        forward_intent=intent,
    )


def _active_hold(host: str = HOST) -> dict[str, object]:
    return {"host": host, "kind": "full", "owned": True, "status": "active"}


def _make_recovery(
    *,
    failures: tuple[transition.HostFailureGroup, ...] | None = None,
    old_snapshots: tuple[transition.JournalHostSnapshot, ...] | None = None,
    current_snapshot: transition.JournalHostSnapshot | None = None,
    proofs: tuple[transition.HoldProof, ...] | None = None,
    latest: transition.LatestReportRef | None = None,
    actions: tuple[transition.PlanActionRef, ...] | None = None,
    receipt_report: transition.ReceiptReportRef | None = None,
    host: str = HOST,
) -> transition.HostRecoveryInput:
    old_report = _report("v0.15.137-old", SHA_A)
    current_report = _report("v0.15.142-current", SHA_B)
    if latest is None:
        latest = transition.LatestReportRef(current_report, "latest-generation-7")
    failures = (_failure(old_report, host=host),) if failures is None else failures
    if old_snapshots is None:
        old_snapshots = tuple(
            _snapshot(
                item.report,
                host=host,
                token_digest=SHA_C,
                records={action_id: {"status": "failed"} for action_id in item.failed_action_ids},
                hold=_active_hold(host),
            )
            for item in failures
        )
    if current_snapshot is None:
        current_snapshot = _snapshot(
            current_report,
            host=host,
            token_digest=SHA_D,
        )
    proofs = tuple(_proof(item.report, host) for item in failures) if proofs is None else proofs
    if actions is None:
        actions = (
            _action(f"local-runtime:{host}:vibeqc-release", host=host),
            _action(
                f"local-runtime:{host}:vibeqc-dev",
                host=host,
                decision="skip",
                reason="already at target with LAST OK=true",
                pin_name="dev",
            ),
        )
    tokens = {current_snapshot.token.rollout_id: current_snapshot.token}
    tokens.update({item.token.rollout_id: item.token for item in old_snapshots})
    vector = transition.MutationVector(latest, tuple(tokens[key] for key in sorted(tokens)))
    compatibility: dict[str, object] = {}
    if receipt_report is not None and "receipt_plan_actions" in {
        item.name for item in fields(transition.HostRecoveryInput)
    }:
        compatibility["receipt_plan_actions"] = actions
    return transition.HostRecoveryInput(
        latest_report=latest,
        host=host,
        plan_actions=actions,
        failures=failures,
        old_snapshots=old_snapshots,
        current_snapshot=current_snapshot,
        hold_proofs=proofs,
        mutation_vector=vector,
        recorded_at=NOW,
        receipt_report=receipt_report,
        **compatibility,
    )


def _fresh_intent(
    recovery: transition.HostRecoveryInput | None = None,
) -> transition.FailureUpdateIntent:
    planned = transition.plan_host_transition(_make_recovery() if recovery is None else recovery)
    assert planned.phase is transition.TransitionPhase.PREVALIDATED
    effect = planned.effects[0]
    assert isinstance(effect, transition.WriteCurrentAckAndIntent)
    return effect.intent


def _current_with_intent(
    base: transition.HostRecoveryInput,
    intent: transition.FailureUpdateIntent,
    *,
    token_digest: str = SHA_D,
) -> transition.JournalHostSnapshot:
    rows = transition.canonical_no_launch_rows(
        intent.host,
        base.plan_actions,
        intent.failed_host_reason,
    )
    return _snapshot(
        intent_report(intent),
        host=intent.host,
        token_digest=token_digest,
        records=rows,
        failed_reason=intent.failed_host_reason,
        intent=intent.as_dict(),
    )


def intent_report(intent: transition.FailureUpdateIntent) -> transition.ReportRef:
    return transition.ReportRef(
        intent.current_report_path,
        intent.current_report_digest_sha256,
        intent.current_rollout_id,
    )


def _receipt(intent: transition.FailureUpdateIntent) -> transition.ReceiptReportRef:
    """Stand in for an adapter-authenticated persisted receipt capability."""
    receipt_fields = {item.name for item in fields(transition.ReceiptReportRef)}
    credential = (
        "adapter-authenticated-report-token"
        if "authentication_token" in receipt_fields
        else transition.canonical_json_sha256(intent.as_dict())
    )
    return transition.ReceiptReportRef(
        intent_report(intent),
        credential,
    )


def _receipt_for_report(
    intent: transition.FailureUpdateIntent,
    report: transition.ReportRef,
) -> transition.ReceiptReportRef:
    return replace(_receipt(intent), ref=report)


def _backlink(intent: transition.FailureUpdateIntent, index: int = 0) -> dict[str, object]:
    return transition.expand_failure_update_backlink(
        intent,
        intent.sources[index],
        _receipt(intent),
    ).as_dict()


def _recovery_with_intent(
    fresh: transition.HostRecoveryInput,
    intent: transition.FailureUpdateIntent,
    *,
    old_snapshots: tuple[transition.JournalHostSnapshot, ...] | None = None,
    failures: tuple[transition.HostFailureGroup, ...] | None = None,
    proofs: tuple[transition.HoldProof, ...] | None = None,
    receipt: transition.ReceiptReportRef | None = None,
) -> transition.HostRecoveryInput:
    return _make_recovery(
        failures=fresh.failures if failures is None else failures,
        old_snapshots=fresh.old_snapshots if old_snapshots is None else old_snapshots,
        current_snapshot=_current_with_intent(fresh, intent),
        proofs=fresh.hold_proofs if proofs is None else proofs,
        latest=fresh.latest_report,
        actions=fresh.plan_actions,
        receipt_report=_receipt(intent) if receipt is None else receipt,
    )


def _assert_reject(
    recovery: transition.HostRecoveryInput,
    match: str | None = None,
) -> transition.Reject:
    planned = transition.plan_host_transition(recovery)
    assert len(planned.effects) == 1
    effect = planned.effects[0]
    assert isinstance(effect, transition.Reject)
    if match is not None:
        assert match in effect.reason
    return effect


def test_intent_and_source_parsers_reject_every_field_mutation() -> None:
    raw = _fresh_intent().as_dict()
    intent_mutations: dict[str, object] = {
        "schema": "wrong",
        "host": "",
        "current_report_path": "",
        "current_report_digest_sha256": "A" * 64,
        "current_rollout_id": "",
        "current_action_ids": [],
        "current_plan_projection": {},
        "current_actions_sha256": "x",
        "current_ack_sha256": "x",
        "failed_host_reason": "",
        "sources": [],
        "recorded_at": "2026-08-25T20:30:00",
    }
    for field, bad_value in intent_mutations.items():
        candidate = copy.deepcopy(raw)
        candidate[field] = bad_value
        with pytest.raises(transition.TransitionError, match="intent|failure update"):
            transition.parse_failure_update_intent(candidate)

    source_mutations: dict[str, object] = {
        "historical_rollout_id": "",
        "historical_report_path": "",
        "historical_report_digest_sha256": "x",
        "failed_action_ids": [],
        "failed_operation_ids": ["x"],
        "failure_reason": "",
        "settled_hold_sha256": "x",
    }
    for field, bad_value in source_mutations.items():
        candidate = copy.deepcopy(raw)
        assert isinstance(candidate["sources"], list)
        candidate["sources"][0][field] = bad_value
        with pytest.raises(transition.TransitionError):
            transition.parse_failure_update_intent(candidate)


def test_v2_intent_persists_a_closed_canonical_host_plan_projection() -> None:
    recovery = _make_recovery()
    intent = _fresh_intent(recovery)
    raw = intent.as_dict()
    assert transition.FAILURE_UPDATE_INTENT_SCHEMA == ("vq.fleet.legacy_failure_update_intent/2")
    assert transition.FAILURE_UPDATE_ACTIONS_SCHEMA == ("vq.fleet.legacy_failure_update_actions/2")
    assert list(raw) == [
        "schema",
        "host",
        "current_report_path",
        "current_report_digest_sha256",
        "current_rollout_id",
        "current_action_ids",
        "current_plan_projection",
        "current_actions_sha256",
        "current_ack_sha256",
        "failed_host_reason",
        "sources",
        "recorded_at",
    ]
    projection = cast(dict[str, Any], raw["current_plan_projection"])
    assert projection == transition.project_host_actions(
        recovery.latest_report,
        recovery.host,
        recovery.plan_actions,
    )
    assert list(projection) == ["schema", "host", "actions"]
    assert transition.canonical_json_sha256(projection) == intent.current_actions_sha256
    action_rows = cast(list[dict[str, Any]], projection["actions"])
    assert [row["action_id"] for row in action_rows] == sorted(
        row["action_id"] for row in action_rows
    )
    assert len(action_rows) == len({row["action_id"] for row in action_rows})
    assert all(row["host"] == HOST for row in action_rows)
    assert list(intent.current_action_ids) == [
        row["action_id"] for row in action_rows if row["decision"] == "update"
    ]
    assert all(
        list(row)
        == [
            "action_id",
            "phase",
            "host",
            "program",
            "decision",
            "reason",
            "pin_name",
            "target_sha",
            "target_version",
            "target_tag",
            "argv",
            "before",
        ]
        and list(row["before"])
        == [
            "configured",
            "current_sha",
            "current_version",
            "current_tag",
            "dirty",
            "last_ok",
            "acknowledged",
            "detail",
            "metrics",
            "required",
        ]
        for row in action_rows
    )
    assert list(cast(list[dict[str, Any]], raw["sources"])[0]) == [
        "historical_rollout_id",
        "historical_report_path",
        "historical_report_digest_sha256",
        "failed_action_ids",
        "failed_operation_ids",
        "failure_reason",
        "settled_hold_sha256",
    ]
    assert all(
        all(
            type(row[name]) is str and row[name]
            for name in (
                "action_id",
                "phase",
                "host",
                "program",
                "decision",
                "reason",
                "pin_name",
                "target_sha",
            )
        )
        and (row["target_version"] is None or type(row["target_version"]) is str)
        and (row["target_tag"] is None or type(row["target_tag"]) is str)
        and type(row["argv"]) is list
        and all(type(item) is str and item for item in row["argv"])
        and type(row["before"]["configured"]) is bool
        and (row["before"]["current_sha"] is None or type(row["before"]["current_sha"]) is str)
        and (
            row["before"]["current_version"] is None
            or type(row["before"]["current_version"]) is str
        )
        and (row["before"]["current_tag"] is None or type(row["before"]["current_tag"]) is str)
        and (row["before"]["dirty"] is None or type(row["before"]["dirty"]) is bool)
        and type(row["before"]["last_ok"]) is bool
        and type(row["before"]["acknowledged"]) is bool
        and type(row["before"]["detail"]) is str
        and (row["before"]["metrics"] is None or type(row["before"]["metrics"]) is dict)
        and type(row["before"]["required"]) is bool
        for row in action_rows
    )

    malformed: list[dict[str, Any]] = []
    for field, value in (
        ("schema", "vq.fleet.legacy_failure_update_actions/1"),
        ("host", "host_a"),
        ("actions", {}),
    ):
        candidate = copy.deepcopy(projection)
        candidate[field] = value
        malformed.append(candidate)
    reversed_rows = copy.deepcopy(projection)
    reversed_rows["actions"].reverse()
    malformed.append(reversed_rows)
    duplicate_rows = copy.deepcopy(projection)
    duplicate_rows["actions"].append(copy.deepcopy(duplicate_rows["actions"][0]))
    malformed.append(duplicate_rows)
    for mutation in (
        lambda row: row.pop("program"),
        lambda row: row.__setitem__("extra", True),
        lambda row: row.__setitem__("action_id", 3),
        lambda row: row.__setitem__("phase", 3),
        lambda row: row.__setitem__("phase", "unknown-runtime"),
        lambda row: row.__setitem__("host", "host_a"),
        lambda row: row.__setitem__("program", 3),
        lambda row: row.__setitem__("decision", 3),
        lambda row: row.__setitem__("decision", "run"),
        lambda row: row.__setitem__("reason", 3),
        lambda row: row.__setitem__("pin_name", 3),
        lambda row: row.__setitem__("target_sha", 3),
        lambda row: row.__setitem__("target_sha", ""),
        lambda row: row.__setitem__("target_sha", "g" * 40),
        lambda row: row.__setitem__("target_sha", "1" * 39),
        lambda row: row.__setitem__("target_version", 3),
        lambda row: row.__setitem__("target_tag", 3),
        lambda row: row.__setitem__("argv", ["ok", 3]),
        lambda row: row.__setitem__("before", []),
        lambda row: row["before"].pop("configured"),
        lambda row: row["before"].__setitem__("extra", True),
        lambda row: row["before"].__setitem__("configured", 1),
        lambda row: row["before"].__setitem__("current_sha", 1),
        lambda row: row["before"].__setitem__("current_sha", ""),
        lambda row: row["before"].__setitem__("current_sha", "g" * 40),
        lambda row: row["before"].__setitem__("current_sha", "0" * 39),
        lambda row: row["before"].__setitem__("current_version", 1),
        lambda row: row["before"].__setitem__("current_tag", 1),
        lambda row: row["before"].__setitem__("dirty", "no"),
        lambda row: row["before"].__setitem__("last_ok", 1),
        lambda row: row["before"].__setitem__("acknowledged", 1),
        lambda row: row["before"].__setitem__("detail", 1),
        lambda row: row["before"].pop("required"),
        lambda row: row["before"].__setitem__("required", 1),
        lambda row: row["before"].__setitem__("metrics", []),
        lambda row: row["before"].__setitem__("metrics", {"ratio": float("nan")}),
    ):
        candidate = copy.deepcopy(projection)
        mutation(candidate["actions"][0])
        malformed.append(candidate)
    for bad_projection in malformed:
        candidate = copy.deepcopy(raw)
        candidate["current_plan_projection"] = bad_projection
        with pytest.raises(transition.TransitionError):
            candidate["current_actions_sha256"] = transition.canonical_json_sha256(bad_projection)
            transition.parse_failure_update_intent(candidate)

    legacy = copy.deepcopy(raw)
    legacy["schema"] = "vq.fleet.legacy_failure_update_intent/1"
    legacy.pop("current_plan_projection")
    with pytest.raises(transition.TransitionError):
        transition.parse_failure_update_intent(legacy)

    required_true = replace(
        recovery.plan_actions[0],
        before={
            **cast(dict[str, object], transition.thaw_json(recovery.plan_actions[0].before)),
            "required": True,
        },
    )
    required_false = replace(
        required_true,
        before={
            **cast(dict[str, object], transition.thaw_json(required_true.before)),
            "required": False,
        },
    )
    assert transition.current_actions_sha256(
        recovery.latest_report,
        recovery.host,
        (required_true, recovery.plan_actions[1]),
    ) != transition.current_actions_sha256(
        recovery.latest_report,
        recovery.host,
        (required_false, recovery.plan_actions[1]),
    )

    unconfigured = _action(
        "local-runtime:host_e:unconfigured",
        before={
            "configured": False,
            "current_sha": None,
            "current_version": None,
            "current_tag": None,
            "dirty": None,
            "last_ok": False,
            "acknowledged": False,
            "detail": "not configured",
            "metrics": None,
            "required": False,
        },
    )
    unconfigured_intent = _fresh_intent(_make_recovery(actions=(unconfigured,)))
    unconfigured_before = cast(
        dict[str, Any],
        unconfigured_intent.as_dict()["current_plan_projection"],
    )["actions"][0]["before"]
    assert unconfigured_before == transition.thaw_json(unconfigured.before)

    for allowed_phase in ("driver", "helper", "scheduler-runtime", "local-runtime"):
        phase_action = replace(recovery.plan_actions[0], phase=allowed_phase)
        _fresh_intent(_make_recovery(actions=(phase_action,)))
    for allowed_decision in ("update", "skip", "defer", "block"):
        decision_action = replace(
            recovery.plan_actions[1],
            action_id=f"local-runtime:host_e:{allowed_decision}",
            decision=allowed_decision,
        )
        _fresh_intent(_make_recovery(actions=(recovery.plan_actions[0], decision_action)))
    free_text = replace(
        recovery.plan_actions[0],
        program="adapter authenticated free text",
        pin_name="adapter/pin free text",
    )
    _fresh_intent(_make_recovery(actions=(free_text,)))


@pytest.mark.parametrize("bad", [None, [], "receipt"])
def test_strict_receipt_parsers_reject_non_mappings(bad: object) -> None:
    with pytest.raises(transition.TransitionError):
        transition.parse_failure_update_intent(bad)
    with pytest.raises(transition.TransitionError):
        transition.parse_failure_update_backlink(bad)


def test_backlink_parser_rejects_all_thirteen_field_mutations() -> None:
    raw = _backlink(_fresh_intent())
    mutations: dict[str, object] = {
        "schema": "wrong",
        "host": "",
        "failed_action_ids": [],
        "failed_operation_ids": ["x"],
        "historical_report_path": "",
        "historical_report_digest_sha256": "x",
        "settled_hold_sha256": "x",
        "current_report_path": "",
        "current_report_digest_sha256": "x",
        "current_rollout_id": "",
        "current_action_ids": [],
        "current_actions_sha256": "x",
        "recorded_at": "2026-08-25T20:30:00",
    }
    assert list(raw) == list(mutations)
    for field, bad_value in mutations.items():
        candidate = copy.deepcopy(raw)
        candidate[field] = bad_value
        with pytest.raises(transition.TransitionError):
            transition.parse_failure_update_backlink(candidate)


def test_closed_schemas_reject_missing_and_extra_fields() -> None:
    intent = _fresh_intent().as_dict()
    intent.pop("host")
    with pytest.raises(transition.TransitionError):
        transition.parse_failure_update_intent(intent)
    backlink = _backlink(_fresh_intent())
    backlink["extra"] = True
    with pytest.raises(transition.TransitionError):
        transition.parse_failure_update_backlink(backlink)


def test_receipt_capability_cannot_satisfy_latest_planning_api() -> None:
    recovery = _make_recovery()
    receipt = _receipt(_fresh_intent(recovery))
    with pytest.raises(TypeError, match="LatestReportRef"):
        transition.project_host_actions(receipt, HOST, recovery.plan_actions)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="LatestReportRef"):
        transition.MutationVector(receipt, ())  # type: ignore[arg-type]


def test_latest_and_receipt_wrappers_require_exact_report_ref() -> None:
    report = _report("wrapped", SHA_A)
    latest = transition.LatestReportRef(report, "generation")
    receipt = transition.ReceiptReportRef(report, SHA_B)
    with pytest.raises(TypeError, match="ReportRef"):
        transition.LatestReportRef(receipt, "generation")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="ReportRef"):
        transition.ReceiptReportRef(latest, SHA_B)  # type: ignore[arg-type]
    with pytest.raises((TypeError, transition.TransitionError)):
        transition.ReceiptReportRef(report, 3)  # type: ignore[arg-type]


def test_receipt_capability_is_never_self_minted_by_pure_module() -> None:
    assert not hasattr(transition, "receipt_report_from_intent")


def test_full_plan_projection_hash_covers_every_action_field_and_membership() -> None:
    recovery = _make_recovery()
    latest = recovery.latest_report
    actions = recovery.plan_actions
    baseline = transition.current_actions_sha256(latest, HOST, actions)
    assert transition.project_host_actions(latest, HOST, tuple(reversed(actions))) == (
        transition.project_host_actions(latest, HOST, actions)
    )
    variants = (
        actions + (_action("helper:host_e", host=HOST, decision="skip"),),
        (replace(actions[0], action_id="changed-id"), actions[1]),
        (replace(actions[0], phase="helper"), actions[1]),
        (replace(actions[0], host="host_a"), actions[1]),
        (replace(actions[0], program="changed-program"), actions[1]),
        (replace(actions[0], decision="skip"), actions[1]),
        (replace(actions[0], reason="different reason"), actions[1]),
        (replace(actions[0], pin_name="different-pin"), actions[1]),
        (replace(actions[0], target_sha="2" * 40), actions[1]),
        (replace(actions[0], target_version="0.15.999"), actions[1]),
        (replace(actions[0], target_tag="v0.15.999"), actions[1]),
        (replace(actions[0], argv=("different", "argv")), actions[1]),
        (
            replace(
                actions[0],
                before={
                    **cast(dict[str, object], transition.thaw_json(actions[0].before)),
                    "current_sha": "9" * 40,
                },
            ),
            actions[1],
        ),
    )
    assert all(
        transition.current_actions_sha256(latest, HOST, variant) != baseline for variant in variants
    )
    intent = _fresh_intent(recovery)
    assert intent.current_action_ids == (actions[0].action_id,)


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        ("phase", "unknown-runtime"),
        ("decision", "run"),
        ("target_sha", ""),
        ("target_sha", "g" * 40),
        ("target_sha", "1" * 39),
        ("before.current_sha", ""),
        ("before.current_sha", "g" * 40),
        ("before.current_sha", "0" * 39),
    ],
)
def test_plan_action_closed_phase_decision_and_sha_domains_reject(
    field: str,
    bad_value: str,
) -> None:
    action = _action("local-runtime:host_e:vibeqc-release")
    with pytest.raises(transition.TransitionError):
        if field == "before.current_sha":
            replace(
                action,
                before={
                    **cast(dict[str, object], transition.thaw_json(action.before)),
                    "current_sha": bad_value,
                },
            )
        else:
            replace(action, **cast(Any, {field: bad_value}))


def test_plan_action_closed_domains_accept_exact_enums_nulls_and_free_text() -> None:
    action = _action("local-runtime:host_e:vibeqc-release")
    for phase in ("driver", "helper", "scheduler-runtime", "local-runtime"):
        assert replace(action, phase=phase).phase == phase
    for decision in ("update", "skip", "defer", "block"):
        assert replace(action, decision=decision).decision == decision
    nullable_before = {
        **cast(dict[str, object], transition.thaw_json(action.before)),
        "current_sha": None,
        "current_version": None,
        "current_tag": None,
        "dirty": None,
        "metrics": None,
    }
    free_text = replace(
        action,
        program="adapter authenticated free text",
        pin_name="adapter/pin free text",
        before=nullable_before,
    )
    assert free_text.program == "adapter authenticated free text"
    assert free_text.pin_name == "adapter/pin free text"


def test_no_launch_rows_are_exact_executor_three_field_rows() -> None:
    recovery = _make_recovery()
    rows = transition.canonical_no_launch_rows(HOST, recovery.plan_actions, "boom")
    assert rows == {
        recovery.plan_actions[0].action_id: {
            "decision": "update",
            "reason": f"skipped: an earlier lane on {HOST} failed (boom)",
            "status": "not-run",
        }
    }


def test_deterministic_group_reason_preserves_sources_and_rejects_conflict() -> None:
    early = _failure(_report("a-rollout", SHA_A), reason="early reason")
    late = _failure(
        _report("z-rollout", SHA_B),
        reason="last reason",
        action_ids=("local-runtime:host_e:vibeqc-dev",),
    )
    assert transition.canonical_host_failure_reason((late, early)) == "last reason"
    assert transition.canonical_host_failure_reason((early, early, late)) == "last reason"
    conflicting = replace(early, reason="conflicting duplicate")
    with pytest.raises(transition.TransitionError, match="conflicting"):
        transition.canonical_host_failure_reason((early, conflicting))
    recovery = _make_recovery(failures=(late, early))
    intent = _fresh_intent(recovery)
    assert [item.failure_reason for item in intent.sources] == ["early reason", "last reason"]


def test_current_ack_absent_exact_conflict_and_ordinary_rows_without_intent() -> None:
    recovery = _make_recovery()
    intent = _fresh_intent(recovery)
    exact = _current_with_intent(recovery, intent)
    assert transition.classify_current_ack(recovery.current_snapshot, intent) is (
        transition.CurrentAckState.ABSENT
    )
    assert transition.classify_current_ack(exact, intent) is transition.CurrentAckState.EXACT
    ordinary = replace(exact, forward_intent=None)
    assert transition.classify_current_ack(ordinary, intent) is (
        transition.CurrentAckState.CONFLICT
    )
    arbitrary_no_intent = (
        replace(recovery.current_snapshot, relevant_action_records={"ordinary": {"status": "ok"}}),
        replace(
            recovery.current_snapshot, relevant_action_records={"attempt": {"status": "running"}}
        ),
        replace(recovery.current_snapshot, failed_reason="unrelated failure"),
        replace(recovery.current_snapshot, complete=True),
    )
    assert all(
        transition.classify_current_ack(snapshot, intent) is transition.CurrentAckState.CONFLICT
        for snapshot in arbitrary_no_intent
    )
    conflict = replace(exact, failed_reason="different")
    assert transition.classify_current_ack(conflict, intent) is (
        transition.CurrentAckState.CONFLICT
    )


def test_second_same_host_stale_skip_is_removed_atomically() -> None:
    old = _report("old-two", SHA_A)
    ids = (
        "local-runtime:host_e:vibeqc-release",
        "local-runtime:host_e:vibeqc-dev",
    )
    group = _failure(
        old,
        action_ids=ids,
        skip_states=tuple((item, transition.SkipState.STALE) for item in ids),
    )
    old_snapshot = _snapshot(
        old,
        token_digest=SHA_C,
        records={item: {"status": "failed", "legacy_failure_skip": {"old": True}} for item in ids},
        hold=_active_hold(),
    )
    recovery = _make_recovery(failures=(group,), old_snapshots=(old_snapshot,))
    intent = _fresh_intent(recovery)
    acked = _make_recovery(
        failures=(group,),
        old_snapshots=(old_snapshot,),
        current_snapshot=_current_with_intent(recovery, intent),
        proofs=recovery.hold_proofs,
        latest=recovery.latest_report,
        actions=recovery.plan_actions,
        receipt_report=_receipt(intent),
    )
    planned = transition.plan_host_transition(acked)
    assert planned.phase is transition.TransitionPhase.ACKED
    effect = planned.effects[0]
    assert isinstance(effect, transition.WriteOldBacklink)
    assert {edit.action_id for edit in effect.action_edits} == set(ids)
    assert all(edit.delete_keys == ("legacy_failure_skip",) for edit in effect.action_edits)
    backlink_digests = {
        transition.canonical_json_sha256(edit.set_items[0][1]) for edit in effect.action_edits
    }
    assert len(backlink_digests) == 1


@pytest.mark.parametrize("mode", ["malformed", "current", "overlap"])
def test_second_same_host_bad_skip_or_backlink_overlap_rejects(mode: str) -> None:
    old = _report("old-two", SHA_A)
    ids = (
        "local-runtime:host_e:vibeqc-release",
        "local-runtime:host_e:vibeqc-dev",
    )
    states = [(ids[0], transition.SkipState.ABSENT)]
    records: dict[str, object] = {ids[0]: {"status": "failed"}}
    second: dict[str, object] = {"status": "failed", "legacy_failure_skip": {"old": True}}
    if mode == "malformed":
        states.append((ids[1], transition.SkipState.ABSENT))
    elif mode == "current":
        states.append((ids[1], transition.SkipState.CURRENT))
    else:
        states.append((ids[1], transition.SkipState.STALE))
    records[ids[1]] = second
    group = _failure(old, action_ids=ids, skip_states=tuple(states))
    if mode == "overlap":
        second["legacy_failure_update_ack"] = _backlink(_fresh_intent())
    old_snapshot = _snapshot(
        old,
        token_digest=SHA_C,
        records=records,
        hold=_active_hold(),
    )
    recovery = _make_recovery(failures=(group,), old_snapshots=(old_snapshot,))
    planned = transition.plan_host_transition(recovery)
    assert isinstance(planned.effects[0], transition.Reject)


def test_sequential_replan_emits_one_full_vector_mutation_per_phase() -> None:
    old_a = _report("a-old", SHA_A)
    old_b = _report("b-old", SHA_B)
    group_a = _failure(old_a, reason="first")
    group_b = _failure(
        old_b,
        reason="second",
        action_ids=("local-runtime:host_e:vibeqc-dev",),
    )
    snap_a = _snapshot(
        old_a,
        token_digest=SHA_A,
        records={group_a.failed_action_ids[0]: {"status": "failed"}},
        hold=_active_hold(),
    )
    snap_b = _snapshot(
        old_b,
        token_digest=SHA_B,
        records={group_b.failed_action_ids[0]: {"status": "failed"}},
        hold=_active_hold(),
    )
    fresh = _make_recovery(
        failures=(group_b, group_a),
        old_snapshots=(snap_a, snap_b),
        proofs=(_proof(old_a), _proof(old_b)),
    )
    peer = _token(_report("peer-old", SHA_C), "3" * 64)

    def invocation_wide(recovery: transition.HostRecoveryInput) -> transition.HostRecoveryInput:
        return replace(
            recovery,
            mutation_vector=transition.MutationVector(
                recovery.latest_report,
                tuple(
                    sorted(
                        (*recovery.mutation_vector.journals, peer),
                        key=lambda item: item.rollout_id,
                    )
                ),
            ),
        )

    fresh = invocation_wide(fresh)
    first = transition.plan_host_transition(fresh)
    assert first.phase is transition.TransitionPhase.PREVALIDATED
    assert [type(item) for item in first.effects] == [transition.WriteCurrentAckAndIntent]
    intent = first.effects[0].intent  # type: ignore[union-attr]
    current = _current_with_intent(fresh, intent)

    ack_only = _make_recovery(
        failures=(group_a, group_b),
        old_snapshots=(snap_a, snap_b),
        current_snapshot=current,
        proofs=(_proof(old_a), _proof(old_b)),
        latest=fresh.latest_report,
        actions=fresh.plan_actions,
    )
    second = transition.plan_host_transition(
        replace(
            ack_only,
            receipt_report=_receipt(intent),
        )
    )
    assert second.phase is transition.TransitionPhase.ACKED
    assert len(second.effects) == 1
    assert isinstance(second.effects[0], transition.WriteOldBacklink)
    assert second.effects[0].old_report == old_a
    assert second.effects[0].expected_vector == ack_only.mutation_vector

    back_a = _backlink(intent, 0)
    linked_a = replace(
        snap_a,
        relevant_action_records={
            group_a.failed_action_ids[0]: {
                "status": "failed",
                "legacy_failure_update_ack": back_a,
            }
        },
        hold=_settled_hold(),
    )
    partial = _make_recovery(
        failures=(group_a, group_b),
        old_snapshots=(linked_a, snap_b),
        current_snapshot=current,
        proofs=(_existing_proof(old_a), _proof(old_b)),
        latest=fresh.latest_report,
        actions=fresh.plan_actions,
        receipt_report=_receipt(intent),
    )
    third = transition.plan_host_transition(partial)
    assert third.phase is transition.TransitionPhase.BACKLINKING
    assert len(third.effects) == 1
    assert third.effects[0].old_report == old_b  # type: ignore[union-attr]
    assert third.effects[0].expected_vector == partial.mutation_vector  # type: ignore[union-attr]

    back_b = _backlink(intent, 1)
    linked_b = replace(
        snap_b,
        relevant_action_records={
            group_b.failed_action_ids[0]: {
                "status": "failed",
                "legacy_failure_update_ack": back_b,
            }
        },
        hold=_settled_hold(),
    )
    linked = _make_recovery(
        failures=(group_a, group_b),
        old_snapshots=(linked_a, linked_b),
        current_snapshot=current,
        proofs=(_existing_proof(old_a), _existing_proof(old_b)),
        latest=fresh.latest_report,
        actions=fresh.plan_actions,
        receipt_report=_receipt(intent),
    )
    fourth = transition.plan_host_transition(linked)
    assert fourth.phase is transition.TransitionPhase.CONSUMING
    assert len(fourth.effects) == 1
    assert isinstance(fourth.effects[0], transition.ConsumeMember)
    assert fourth.effects[0].old_report == old_a
    assert fourth.effects[0].expected_vector == linked.mutation_vector
    assert fourth.effects[0].retry_authorized is False

    consumed_a = replace(
        group_a,
        attempts=tuple(replace(item, consumed=True) for item in group_a.attempts),
    )
    partly_consumed = _make_recovery(
        failures=(consumed_a, group_b),
        old_snapshots=(linked_a, linked_b),
        current_snapshot=current,
        proofs=(_existing_proof(old_a), _existing_proof(old_b)),
        latest=fresh.latest_report,
        actions=fresh.plan_actions,
        receipt_report=_receipt(intent),
    )
    fifth = transition.plan_host_transition(partly_consumed)
    assert fifth.phase is transition.TransitionPhase.CONSUMING
    assert len(fifth.effects) == 1
    assert fifth.effects[0].old_report == old_b  # type: ignore[union-attr]
    assert fifth.effects[0].expected_vector == partly_consumed.mutation_vector  # type: ignore[union-attr]

    consumed_b = replace(
        group_b,
        attempts=tuple(replace(item, consumed=True) for item in group_b.attempts),
    )
    newer = transition.LatestReportRef(_report("v0.15.143-latest", SHA_C), "generation-8")
    fully_consumed = _make_recovery(
        failures=(consumed_a, consumed_b),
        old_snapshots=(linked_a, linked_b),
        current_snapshot=current,
        proofs=(_existing_proof(old_a), _existing_proof(old_b)),
        latest=newer,
        actions=(),
        receipt_report=_receipt(intent),
    )
    sixth = transition.plan_host_transition(fully_consumed)
    assert sixth.phase is transition.TransitionPhase.COMPLETE
    assert [type(item) for item in sixth.effects] == [transition.ClearForwardIntent]
    assert sixth.effects[0].expected_vector == fully_consumed.mutation_vector  # type: ignore[union-attr]

    cleared_current = replace(current, forward_intent=None)
    after_clear = _make_recovery(
        failures=(consumed_a, consumed_b),
        old_snapshots=(linked_a, linked_b),
        current_snapshot=cleared_current,
        proofs=(_existing_proof(old_a), _existing_proof(old_b)),
        latest=newer,
        actions=(),
        receipt_report=_receipt(intent),
    )
    final = transition.plan_host_transition(after_clear)
    assert final.phase is transition.TransitionPhase.COMPLETE
    assert final.effects == ()


def test_released_only_without_intent_or_backlink_cannot_mint() -> None:
    recovery = _make_recovery()
    released = replace(recovery.old_snapshots[0], hold=_settled_hold())
    candidate = _make_recovery(
        failures=recovery.failures,
        old_snapshots=(released,),
        proofs=recovery.hold_proofs,
        latest=recovery.latest_report,
        actions=recovery.plan_actions,
    )
    planned = transition.plan_host_transition(candidate)
    assert isinstance(planned.effects[0], transition.Reject)
    assert "released-only" in planned.effects[0].reason


def test_input_cross_binds_snapshot_reports_and_rejects_rollout_collision() -> None:
    recovery = _make_recovery()
    wrong_old = replace(
        recovery.old_snapshots[0],
        report=_report(recovery.old_snapshots[0].report.rollout_id, SHA_B),
    )
    with pytest.raises(transition.TransitionError, match="snapshot.*report"):
        _make_recovery(
            failures=recovery.failures,
            old_snapshots=(wrong_old,),
            proofs=recovery.hold_proofs,
            latest=recovery.latest_report,
            actions=recovery.plan_actions,
        )
    collision_group = _failure(recovery.current_snapshot.report)
    collision_snapshot = _snapshot(
        recovery.current_snapshot.report,
        token_digest=SHA_C,
        records={collision_group.failed_action_ids[0]: {"status": "failed"}},
        hold=_active_hold(),
    )
    with pytest.raises(transition.TransitionError, match="current.*old|collision"):
        _make_recovery(
            failures=(collision_group,),
            old_snapshots=(collision_snapshot,),
            current_snapshot=recovery.current_snapshot,
            proofs=(_proof(collision_group.report),),
            latest=recovery.latest_report,
            actions=recovery.plan_actions,
        )


def test_none_journal_token_never_describes_a_present_snapshot() -> None:
    report = _report("absent", SHA_A)
    absent = transition.JournalToken(report.rollout_id, None)
    latest = transition.LatestReportRef(_report("latest", SHA_B), "generation")
    assert transition.MutationVector(latest, (absent,)).token_for(report.rollout_id) == absent
    with pytest.raises(transition.TransitionError, match="absent|digest"):
        transition.JournalHostSnapshot(
            report=report,
            host=HOST,
            token=absent,
            relevant_action_records={},
        )


def test_absent_current_journal_is_a_fenced_create_only_state() -> None:
    base = _make_recovery()
    current_missing = transition.JournalToken(base.latest_report.ref.rollout_id, None)
    peer = _token(_report("create-peer-old", SHA_C), "7" * 64)
    vector = transition.MutationVector(
        base.latest_report,
        tuple(
            sorted(
                (
                    current_missing,
                    *(snapshot.token for snapshot in base.old_snapshots),
                    peer,
                ),
                key=lambda item: item.rollout_id,
            )
        ),
    )
    absent = transition.HostRecoveryInput(
        latest_report=base.latest_report,
        host=base.host,
        plan_actions=base.plan_actions,
        failures=base.failures,
        old_snapshots=base.old_snapshots,
        current_snapshot=None,
        hold_proofs=base.hold_proofs,
        mutation_vector=vector,
        recorded_at=base.recorded_at,
    )
    planned = transition.plan_host_transition(absent)
    assert len(planned.effects) == 1
    effect = planned.effects[0]
    assert isinstance(effect, transition.WriteCurrentAckAndIntent)
    assert effect.report == base.latest_report.ref
    assert effect.expected_vector == vector
    assert effect.expected_vector.token_for(effect.report.rollout_id).digest_sha256 is None
    assert effect.expected_vector.token_for(peer.rollout_id) == peer

    with pytest.raises(transition.TransitionError, match="absent current.*missing token"):
        replace(absent, mutation_vector=base.mutation_vector)
    old_only_vector = transition.MutationVector(
        base.latest_report,
        tuple(
            sorted(
                (*(snapshot.token for snapshot in base.old_snapshots), peer),
                key=lambda item: item.rollout_id,
            )
        ),
    )
    with pytest.raises(transition.TransitionError, match="current.*token"):
        replace(absent, mutation_vector=old_only_vector)

    created_token = transition.JournalToken(base.latest_report.ref.rollout_id, "e" * 64)
    competing_create = transition.advance_mutation_vector(
        vector,
        current_missing,
        created_token,
    )
    with pytest.raises(transition.TransitionError, match="changed"):
        transition.advance_mutation_vector(competing_create, current_missing, created_token)
    with pytest.raises(transition.TransitionError, match="absent current.*missing token"):
        replace(absent, mutation_vector=competing_create)

    advanced_peer = transition.advance_mutation_vector(
        vector,
        peer,
        _token(_report("create-peer-old", SHA_C), "8" * 64),
    )
    with pytest.raises(transition.TransitionError, match="changed"):
        transition.advance_mutation_vector(
            advanced_peer,
            peer,
            _token(_report("create-peer-old", SHA_C), "9" * 64),
        )

    receipt = _receipt(effect.intent)
    with pytest.raises(transition.TransitionError, match="absent current.*receipt"):
        replace(
            absent,
            receipt_report=receipt,
        )

    consumed = replace(
        base.failures[0],
        attempts=tuple(replace(item, consumed=True) for item in base.failures[0].attempts),
    )
    linked_old = replace(
        base.old_snapshots[0],
        relevant_action_records={
            consumed.failed_action_ids[0]: {
                "status": "failed",
                "legacy_failure_update_ack": _backlink(effect.intent),
            }
        },
        hold=_settled_hold(),
        retained_hold=None,
    )
    consumed_absent = replace(
        absent,
        failures=(consumed,),
        old_snapshots=(linked_old,),
        hold_proofs=(_existing_proof(consumed.report),),
    )
    _assert_reject(consumed_absent, "absent current")
    backlinked_absent = replace(
        absent,
        old_snapshots=(linked_old,),
        hold_proofs=(_existing_proof(consumed.report),),
    )
    _assert_reject(backlinked_absent, "absent current")


def test_existing_intent_requires_opaque_report_identity_capability_only() -> None:
    fresh = _make_recovery()
    intent = _fresh_intent(fresh)
    current = _current_with_intent(fresh, intent)
    missing = _make_recovery(
        failures=fresh.failures,
        old_snapshots=fresh.old_snapshots,
        current_snapshot=current,
        proofs=fresh.hold_proofs,
        latest=fresh.latest_report,
        actions=fresh.plan_actions,
    )
    _assert_reject(missing, "receipt")
    wrong_report = _receipt_for_report(intent, _report("other", SHA_C))
    _assert_reject(_recovery_with_intent(fresh, intent, receipt=wrong_report), "receipt")


def test_existing_intent_uses_persisted_projection_and_ignores_rebuilt_live_plan() -> None:
    fresh = _make_recovery()
    intent = _fresh_intent(fresh)
    actions = fresh.plan_actions
    rebuilt_live_plan = (
        replace(
            actions[0],
            decision="skip",
            reason="rebuilt live decision changed after persisted ACK",
            target_sha="2" * 40,
            target_version="0.15.999",
            target_tag="v0.15.999",
            argv=("different", "argv"),
            before={
                **cast(dict[str, object], transition.thaw_json(actions[0].before)),
                "current_sha": "9" * 40,
                "required": True,
            },
        ),
        replace(actions[1], reason="another rebuilt reason"),
    )
    existing = replace(
        _recovery_with_intent(fresh, intent),
        plan_actions=rebuilt_live_plan,
    )
    accepted = transition.plan_host_transition(existing)
    assert isinstance(accepted.effects[0], transition.WriteOldBacklink)


def test_cold_restart_reconstructs_from_serialized_journals_and_report_bytes() -> None:
    fresh = _make_recovery()
    intent = _fresh_intent(fresh)
    current_rows = transition.canonical_no_launch_rows(
        intent.host,
        fresh.plan_actions,
        intent.failed_host_reason,
    )
    old_group = fresh.failures[0]
    old_snapshot = fresh.old_snapshots[0]
    backlink = _backlink(intent)
    current_journal_bytes = json.dumps(
        {
            "report": {
                "source_path": intent.current_report_path,
                "digest_sha256": intent.current_report_digest_sha256,
                "rollout_id": intent.current_rollout_id,
            },
            "token_digest": SHA_D,
            "records": current_rows,
            "failed_reason": intent.failed_host_reason,
            "complete": False,
            "forward_intent": intent.as_dict(),
        },
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    historical_report_bytes = json.dumps(
        {
            "report": {
                "source_path": old_group.report.source_path,
                "digest_sha256": old_group.report.digest_sha256,
                "rollout_id": old_group.report.rollout_id,
            },
            "host": old_group.host,
            "reason": old_group.reason,
            "attempts": [
                {
                    "action_id": item.action_id,
                    "operation_id": item.operation_id,
                    "consumed": item.consumed,
                }
                for item in old_group.attempts
            ],
        },
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    old_journal_bytes = json.dumps(
        {
            "token_digest": old_snapshot.token.digest_sha256,
            "records": {
                action_id: {
                    "status": "failed",
                    "legacy_failure_update_ack": backlink,
                }
                for action_id in old_group.failed_action_ids
            },
            "hold": _settled_hold(),
            "retained_hold": None,
            "failed_reason": old_snapshot.failed_reason,
            "complete": False,
        },
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    latest_generation = fresh.latest_report.discovery_token
    del (
        backlink,
        current_rows,
        fresh,
        intent,
        old_group,
        old_snapshot,
    )

    current_raw = json.loads(current_journal_bytes)
    historical_raw = json.loads(historical_report_bytes)
    old_raw = json.loads(old_journal_bytes)
    parsed_intent = transition.parse_failure_update_intent(current_raw["forward_intent"])
    current_report = transition.ReportRef(**current_raw["report"])
    latest = transition.LatestReportRef(current_report, latest_generation)
    receipt = _receipt_for_report(parsed_intent, current_report)
    current_snapshot = _snapshot(
        current_report,
        token_digest=current_raw["token_digest"],
        records=current_raw["records"],
        failed_reason=current_raw["failed_reason"],
        complete=current_raw["complete"],
        intent=current_raw["forward_intent"],
    )
    old_report = transition.ReportRef(**historical_raw["report"])
    reconstructed_group = transition.HostFailureGroup(
        report=old_report,
        host=historical_raw["host"],
        reason=historical_raw["reason"],
        attempts=tuple(transition.FailureAttemptRef(**item) for item in historical_raw["attempts"]),
    )
    reconstructed_old = _snapshot(
        old_report,
        token_digest=old_raw["token_digest"],
        records=old_raw["records"],
        hold=old_raw["hold"],
        retained=old_raw["retained_hold"],
        failed_reason=old_raw["failed_reason"],
        complete=old_raw["complete"],
    )
    proof = _existing_proof(old_report, settled=old_raw["hold"])
    vector = transition.MutationVector(
        latest,
        tuple(
            sorted(
                (current_snapshot.token, reconstructed_old.token),
                key=lambda item: item.rollout_id,
            )
        ),
    )
    restarted = transition.HostRecoveryInput(
        latest_report=latest,
        host=historical_raw["host"],
        plan_actions=(),
        failures=(reconstructed_group,),
        old_snapshots=(reconstructed_old,),
        current_snapshot=current_snapshot,
        hold_proofs=(proof,),
        mutation_vector=vector,
        recorded_at=parsed_intent.recorded_at,
        receipt_report=receipt,
    )
    planned = transition.plan_host_transition(restarted)
    assert len(planned.effects) == 1
    assert isinstance(planned.effects[0], transition.ConsumeMember)
    assert (
        transition.thaw_json(parsed_intent.current_plan_projection)
        == json.loads(current_journal_bytes)["forward_intent"]["current_plan_projection"]
    )


@pytest.mark.parametrize("next_phase", ["backlink", "consume"])
@pytest.mark.parametrize(
    "source_field",
    [
        "historical_rollout_id",
        "historical_report_path",
        "historical_report_digest_sha256",
        "failed_action_ids",
        "failed_operation_ids",
        "failure_reason",
        "settled_hold_sha256",
    ],
)
def test_cold_existing_intent_rejects_authenticated_historical_source_drift(
    next_phase: str,
    source_field: str,
) -> None:
    fresh = _make_recovery()
    original = _fresh_intent(fresh)

    def candidate_for(
        intent: transition.FailureUpdateIntent,
    ) -> transition.HostRecoveryInput:
        group = fresh.failures[0]
        snapshots = fresh.old_snapshots
        proofs = fresh.hold_proofs
        if next_phase == "consume":
            snapshots = (
                replace(
                    fresh.old_snapshots[0],
                    relevant_action_records={
                        group.failed_action_ids[0]: {
                            "status": "failed",
                            "legacy_failure_update_ack": _backlink(intent),
                        }
                    },
                    hold=_settled_hold(),
                    retained_hold=None,
                ),
            )
            proofs = (_existing_proof(group.report),)
        # On the future surface this is a true cold input: no plan actions survive.
        # The compatibility-only receipt plan remains solely to let the frozen v1
        # implementation reach the historical binding being tested.
        return replace(
            _recovery_with_intent(
                fresh,
                intent,
                old_snapshots=snapshots,
                proofs=proofs,
                receipt=_receipt(intent),
            ),
            plan_actions=(),
        )

    baseline = transition.plan_host_transition(candidate_for(original))
    expected_effect = (
        transition.WriteOldBacklink if next_phase == "backlink" else transition.ConsumeMember
    )
    assert isinstance(baseline.effects[0], expected_effect)

    raw = original.as_dict()
    source = cast(dict[str, Any], cast(list[object], raw["sources"])[0])
    replacements: dict[str, object] = {
        "historical_rollout_id": "tampered-historical-rollout",
        "historical_report_path": "reports/tampered-historical.json",
        "historical_report_digest_sha256": SHA_C,
        "failed_action_ids": ["local-runtime:host_e:tampered-program"],
        "failed_operation_ids": ["f" * 64],
        "failure_reason": "parser-valid but unauthenticated historical failure",
        "settled_hold_sha256": SHA_D,
    }
    source[source_field] = replacements[source_field]
    if source_field == "failure_reason":
        failed_reason = cast(str, source["failure_reason"])
        raw["failed_host_reason"] = failed_reason
        row_reason = f"skipped: an earlier lane on {HOST} failed ({failed_reason})"
        rows = {
            action_id: {
                "decision": "update",
                "reason": row_reason,
                "status": "not-run",
            }
            for action_id in cast(list[str], raw["current_action_ids"])
        }
        raw["current_ack_sha256"] = transition.canonical_json_sha256(
            {
                "complete": False,
                "failed_hosts": {HOST: failed_reason},
                "actions": rows,
            }
        )
    parsed = transition.parse_failure_update_intent(
        json.loads(json.dumps(raw, allow_nan=False, separators=(",", ":"), sort_keys=True))
    )
    rejected = transition.plan_host_transition(candidate_for(parsed))
    assert len(rejected.effects) == 1
    assert isinstance(rejected.effects[0], transition.Reject)
    assert not isinstance(rejected.effects[0], expected_effect)


def test_option1_trusts_internally_coherent_current_projection_before_any_old_backlink() -> None:
    fresh = _make_recovery()
    original = _fresh_intent(fresh)
    coherent = original.as_dict()
    coherent["current_plan_projection"]["actions"][0]["program"] = "coherent-preload-value"
    coherent["current_actions_sha256"] = transition.canonical_json_sha256(
        coherent["current_plan_projection"]
    )
    trusted = transition.parse_failure_update_intent(coherent)
    current = _snapshot(
        intent_report(trusted),
        token_digest=SHA_D,
        records=transition.canonical_no_launch_rows(
            trusted.host,
            fresh.plan_actions,
            trusted.failed_host_reason,
        ),
        failed_reason=trusted.failed_host_reason,
        intent=trusted.as_dict(),
    )
    cold = _make_recovery(
        failures=fresh.failures,
        old_snapshots=fresh.old_snapshots,
        current_snapshot=current,
        proofs=fresh.hold_proofs,
        latest=fresh.latest_report,
        actions=(),
        receipt_report=_receipt(trusted),
    )
    planned = transition.plan_host_transition(cold)
    assert isinstance(planned.effects[0], transition.WriteOldBacklink)


def test_intent_rejects_internally_inconsistent_ids_hash_and_canonical_reason() -> None:
    raw = _fresh_intent().as_dict()

    def recompute_ack(candidate: dict[str, Any]) -> None:
        reason = (
            f"skipped: an earlier lane on {candidate['host']} failed "
            f"({candidate['failed_host_reason']})"
        )
        rows = {
            action_id: {"decision": "update", "reason": reason, "status": "not-run"}
            for action_id in candidate["current_action_ids"]
        }
        candidate["current_ack_sha256"] = transition.canonical_json_sha256(
            {
                "complete": False,
                "failed_hosts": {candidate["host"]: candidate["failed_host_reason"]},
                "actions": rows,
            }
        )

    wrong_ids = copy.deepcopy(raw)
    wrong_ids["current_action_ids"] = ["fake-update-id"]
    recompute_ack(wrong_ids)
    wrong_reason = copy.deepcopy(raw)
    wrong_reason["failed_host_reason"] = "valid but wrong deterministic reason"
    recompute_ack(wrong_reason)
    stale_plan_hash = copy.deepcopy(raw)
    stale_plan_hash["current_actions_sha256"] = SHA_A
    for tampered in (wrong_ids, wrong_reason, stale_plan_hash):
        with pytest.raises(transition.TransitionError):
            transition.parse_failure_update_intent(tampered)


def test_fresh_current_ack_requires_strict_empty_state_without_migration_authority() -> None:
    fresh = _make_recovery()
    first = transition.plan_host_transition(fresh)
    effect = first.effects[0]
    assert isinstance(effect, transition.WriteCurrentAckAndIntent)
    assert effect.action_replacements
    intent = effect.intent
    exact_rows = transition.canonical_no_launch_rows(
        HOST,
        fresh.plan_actions,
        intent.failed_host_reason,
    )
    adoptable_snapshot = replace(
        fresh.current_snapshot,
        relevant_action_records=exact_rows,
        failed_reason=intent.failed_host_reason,
        complete=False,
    )
    unauthenticated_ack = _make_recovery(
        failures=fresh.failures,
        old_snapshots=fresh.old_snapshots,
        current_snapshot=adoptable_snapshot,
        proofs=fresh.hold_proofs,
        latest=fresh.latest_report,
        actions=fresh.plan_actions,
    )
    _assert_reject(unauthenticated_ack, "current acknowledgement")

    row_id = next(iter(exact_rows))
    conflicts = (
        replace(adoptable_snapshot, complete=True),
        replace(adoptable_snapshot, failed_reason="different"),
        replace(
            adoptable_snapshot,
            relevant_action_records={row_id: {**exact_rows[row_id], "extra": True}},
        ),
        replace(adoptable_snapshot, relevant_action_records={}, failed_reason="still nonempty"),
    )
    for snapshot in conflicts:
        _assert_reject(
            _make_recovery(
                failures=fresh.failures,
                old_snapshots=fresh.old_snapshots,
                current_snapshot=snapshot,
                proofs=fresh.hold_proofs,
                latest=fresh.latest_report,
                actions=fresh.plan_actions,
            )
        )


def test_all_old_rows_are_prevalidated_before_current_ack() -> None:
    old_a = _report("a-old", SHA_A)
    old_b = _report("b-old", SHA_B)
    group_a = _failure(old_a)
    group_b = _failure(old_b, action_ids=("local-runtime:host_e:vibeqc-dev",))
    good = _snapshot(
        old_a,
        token_digest=SHA_A,
        records={group_a.failed_action_ids[0]: {"status": "failed"}},
        hold=_active_hold(),
    )
    malformed = _snapshot(
        old_b,
        token_digest=SHA_B,
        records={},
        hold=_active_hold(),
    )
    candidate = _make_recovery(
        failures=(group_b, group_a),
        old_snapshots=(good, malformed),
        proofs=(_proof(old_a), _proof(old_b)),
    )
    _assert_reject(candidate, "missing")


def test_existing_intent_prevalidates_every_old_row_before_one_backlink_write() -> None:
    old_a = _report("a-old", SHA_A)
    old_b = _report("b-old", SHA_B)
    group_a = _failure(old_a)
    group_b = _failure(old_b, action_ids=("local-runtime:host_e:vibeqc-dev",))
    snap_a = _snapshot(
        old_a,
        token_digest=SHA_A,
        records={group_a.failed_action_ids[0]: {"status": "failed"}},
        hold=_active_hold(),
    )
    snap_b = _snapshot(
        old_b,
        token_digest=SHA_B,
        records={group_b.failed_action_ids[0]: {"status": "failed"}},
        hold=_active_hold(),
    )
    fresh = _make_recovery(
        failures=(group_a, group_b),
        old_snapshots=(snap_a, snap_b),
        proofs=(_proof(old_a), _proof(old_b)),
    )
    intent = _fresh_intent(fresh)
    corrupt_b = replace(
        snap_b,
        relevant_action_records={
            group_b.failed_action_ids[0]: {
                "status": "failed",
                "legacy_failure_update_ack": {**_backlink(intent, 1), "host": "host_a"},
            }
        },
    )
    candidate = _recovery_with_intent(
        fresh,
        intent,
        old_snapshots=(snap_a, corrupt_b),
    )
    _assert_reject(candidate, "conflicting backlink")


def test_no_intent_rejects_pending_backlink_consumed_missing_and_mixed_states() -> None:
    fresh = _make_recovery()
    intent = _fresh_intent(fresh)
    group = fresh.failures[0]
    pending_linked = replace(
        fresh.old_snapshots[0],
        relevant_action_records={
            group.failed_action_ids[0]: {
                "status": "failed",
                "legacy_failure_update_ack": _backlink(intent),
            }
        },
        hold=_settled_hold(),
    )
    _assert_reject(
        _make_recovery(
            failures=(group,),
            old_snapshots=(pending_linked,),
            proofs=(_existing_proof(group.report),),
            latest=fresh.latest_report,
            actions=fresh.plan_actions,
        ),
        "without forward intent",
    )

    consumed = replace(
        group,
        attempts=tuple(replace(item, consumed=True) for item in group.attempts),
    )
    _assert_reject(
        _make_recovery(
            failures=(consumed,),
            old_snapshots=fresh.old_snapshots,
            proofs=fresh.hold_proofs,
            latest=fresh.latest_report,
            actions=fresh.plan_actions,
        ),
        "consumed",
    )

    other = _failure(
        _report("other-old", SHA_C),
        action_ids=("local-runtime:host_e:vibeqc-dev",),
    )
    other_snapshot = _snapshot(
        other.report,
        token_digest=SHA_B,
        records={other.failed_action_ids[0]: {"status": "failed"}},
        hold=_active_hold(),
    )
    _assert_reject(
        _make_recovery(
            failures=(consumed, other),
            old_snapshots=(fresh.old_snapshots[0], other_snapshot),
            proofs=(_proof(group.report), _proof(other.report)),
            latest=fresh.latest_report,
            actions=fresh.plan_actions,
        ),
        "mixed",
    )


def test_fully_consumed_audit_selects_and_validates_receipt_local_current_journal() -> None:
    fresh = _make_recovery()
    intent = _fresh_intent(fresh)
    group = fresh.failures[0]
    consumed = replace(
        group,
        attempts=tuple(replace(item, consumed=True) for item in group.attempts),
    )
    linked = replace(
        fresh.old_snapshots[0],
        relevant_action_records={
            group.failed_action_ids[0]: {
                "status": "failed",
                "legacy_failure_update_ack": _backlink(intent),
            }
        },
        hold=_settled_hold(),
    )
    receipt_current = replace(_current_with_intent(fresh, intent), forward_intent=None)
    audit_latest = transition.LatestReportRef(_report("unrelated-latest", SHA_C), "generation")
    candidate = _make_recovery(
        failures=(consumed,),
        old_snapshots=(linked,),
        current_snapshot=receipt_current,
        proofs=(_existing_proof(group.report),),
        latest=audit_latest,
        actions=(),
        receipt_report=_receipt(intent),
    )
    planned = transition.plan_host_transition(candidate)
    assert planned.phase is transition.TransitionPhase.COMPLETE
    assert planned.effects == ()

    bad_current = (
        replace(receipt_current, complete=True),
        replace(receipt_current, failed_reason="tampered reason"),
        replace(receipt_current, relevant_action_records={}),
        replace(
            receipt_current,
            relevant_action_records={
                intent.current_action_ids[0]: {
                    **cast(
                        dict[str, object],
                        receipt_current.relevant_action_records[intent.current_action_ids[0]],
                    ),
                    "extra": True,
                }
            },
        ),
    )
    for snapshot in bad_current:
        _assert_reject(replace(candidate, current_snapshot=snapshot), "receipt")
    _assert_reject(replace(candidate, receipt_report=None), "receipt")
    _assert_reject(
        replace(
            candidate,
            receipt_report=_receipt_for_report(intent, audit_latest.ref),
        ),
        "receipt",
    )
    unrelated_snapshot = _snapshot(audit_latest.ref, token_digest="e" * 64)
    unrelated_vector = transition.MutationVector(
        audit_latest,
        tuple(sorted((unrelated_snapshot.token, linked.token), key=lambda item: item.rollout_id)),
    )
    _assert_reject(
        replace(
            candidate,
            current_snapshot=unrelated_snapshot,
            mutation_vector=unrelated_vector,
        ),
        "receipt",
    )
    for broken_old in (
        replace(linked, hold=None),
        replace(linked, hold=_active_hold()),
        replace(linked, retained_hold={"reason": "still retained"}),
    ):
        _assert_reject(
            _make_recovery(
                failures=(consumed,),
                old_snapshots=(broken_old,),
                current_snapshot=receipt_current,
                proofs=(_existing_proof(group.report),),
                latest=audit_latest,
                actions=(),
                receipt_report=_receipt(intent),
            ),
            "hold",
        )
    _assert_reject(
        _make_recovery(
            failures=(consumed,),
            old_snapshots=(replace(linked, complete=True),),
            current_snapshot=receipt_current,
            proofs=(_existing_proof(group.report),),
            latest=audit_latest,
            actions=(),
            receipt_report=_receipt(intent),
        ),
        "complete",
    )


@pytest.mark.parametrize(
    "row_case",
    ["decision", "reason", "status", "missing-expected", "extra-foreign"],
)
def test_terminal_receipt_current_requires_exact_three_field_ack_row_set(
    row_case: str,
) -> None:
    fresh = _make_recovery()
    intent = _fresh_intent(fresh)
    group = fresh.failures[0]
    consumed = replace(
        group,
        attempts=tuple(replace(item, consumed=True) for item in group.attempts),
    )
    linked = replace(
        fresh.old_snapshots[0],
        relevant_action_records={
            group.failed_action_ids[0]: {
                "status": "failed",
                "legacy_failure_update_ack": _backlink(intent),
            }
        },
        hold=_settled_hold(),
    )
    receipt_current = replace(_current_with_intent(fresh, intent), forward_intent=None)
    records = cast(
        dict[str, Any],
        transition.thaw_json(receipt_current.relevant_action_records),
    )
    expected_id = intent.current_action_ids[0]
    if row_case in {"decision", "reason", "status"}:
        records[expected_id][row_case] = f"tampered-{row_case}"
    elif row_case == "missing-expected":
        records.pop(expected_id)
    else:
        records["foreign-action"] = {
            "decision": "update",
            "reason": records[expected_id]["reason"],
            "status": "not-run",
        }
    audit_latest = transition.LatestReportRef(_report("terminal-latest", SHA_C), "generation")
    candidate = _make_recovery(
        failures=(consumed,),
        old_snapshots=(linked,),
        current_snapshot=replace(receipt_current, relevant_action_records=records),
        proofs=(_existing_proof(group.report),),
        latest=audit_latest,
        actions=(),
        receipt_report=_receipt(intent),
    )
    _assert_reject(candidate, "receipt")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("current_report_path", "reports/tampered-current.json"),
        ("current_report_digest_sha256", SHA_C),
        ("current_rollout_id", "tampered-current"),
        ("current_action_ids", ["tampered-current-action"]),
    ],
)
def test_terminal_single_source_backlink_must_match_authenticated_report_and_ack_identity(
    field: str,
    value: object,
) -> None:
    fresh = _make_recovery()
    intent = _fresh_intent(fresh)
    group = fresh.failures[0]
    consumed = replace(
        group,
        attempts=tuple(replace(item, consumed=True) for item in group.attempts),
    )
    tampered_backlink = _backlink(intent)
    tampered_backlink[field] = value
    transition.parse_failure_update_backlink(tampered_backlink)
    linked = replace(
        fresh.old_snapshots[0],
        relevant_action_records={
            group.failed_action_ids[0]: {
                "status": "failed",
                "legacy_failure_update_ack": tampered_backlink,
            }
        },
        hold=_settled_hold(),
    )
    receipt_current = replace(_current_with_intent(fresh, intent), forward_intent=None)
    audit_latest = transition.LatestReportRef(_report("terminal-latest", SHA_D), "generation")
    candidate = _make_recovery(
        failures=(consumed,),
        old_snapshots=(linked,),
        current_snapshot=receipt_current,
        proofs=(_existing_proof(group.report),),
        latest=audit_latest,
        actions=(),
        receipt_report=_receipt(intent),
    )
    _assert_reject(candidate, "receipt")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("current_actions_sha256", "f" * 64),
        ("recorded_at", "2026-08-25T20:31:00+00:00"),
    ],
)
def test_terminal_single_source_trusts_local_projection_hash_and_timestamp(
    field: str,
    value: object,
) -> None:
    """A lone backlink has no second durable anchor for these local facts."""
    fresh = _make_recovery()
    intent = _fresh_intent(fresh)
    group = fresh.failures[0]
    consumed = replace(
        group,
        attempts=tuple(replace(item, consumed=True) for item in group.attempts),
    )
    trusted_backlink = _backlink(intent)
    trusted_backlink[field] = value
    transition.parse_failure_update_backlink(trusted_backlink)
    if field == "current_actions_sha256":
        assert value not in {intent.current_actions_sha256, intent.current_ack_sha256}
    linked = replace(
        fresh.old_snapshots[0],
        relevant_action_records={
            group.failed_action_ids[0]: {
                "status": "failed",
                "legacy_failure_update_ack": trusted_backlink,
            }
        },
        hold=_settled_hold(),
    )
    receipt_current = replace(_current_with_intent(fresh, intent), forward_intent=None)
    audit_latest = transition.LatestReportRef(_report("terminal-latest", SHA_D), "generation")
    candidate = _make_recovery(
        failures=(consumed,),
        old_snapshots=(linked,),
        current_snapshot=receipt_current,
        proofs=(_existing_proof(group.report),),
        latest=audit_latest,
        actions=(),
        receipt_report=_receipt(intent),
    )
    planned = transition.plan_host_transition(candidate)
    assert planned.phase is transition.TransitionPhase.COMPLETE
    assert planned.effects == ()


def test_terminal_happy_path_accepts_invocation_wide_peer_vector() -> None:
    fresh = _make_recovery()
    intent = _fresh_intent(fresh)
    group = fresh.failures[0]
    consumed = replace(
        group,
        attempts=tuple(replace(item, consumed=True) for item in group.attempts),
    )
    linked = replace(
        fresh.old_snapshots[0],
        relevant_action_records={
            group.failed_action_ids[0]: {
                "status": "failed",
                "legacy_failure_update_ack": _backlink(intent),
            }
        },
        hold=_settled_hold(),
    )
    receipt_current = replace(_current_with_intent(fresh, intent), forward_intent=None)
    audit_latest = transition.LatestReportRef(_report("terminal-latest", SHA_C), "generation")
    candidate = _make_recovery(
        failures=(consumed,),
        old_snapshots=(linked,),
        current_snapshot=receipt_current,
        proofs=(_existing_proof(group.report),),
        latest=audit_latest,
        actions=(),
        receipt_report=_receipt(intent),
    )
    peer = _token(_report("terminal-peer", SHA_B), "6" * 64)
    candidate = replace(
        candidate,
        mutation_vector=transition.MutationVector(
            audit_latest,
            tuple(
                sorted(
                    (*candidate.mutation_vector.journals, peer),
                    key=lambda item: item.rollout_id,
                )
            ),
        ),
    )
    planned = transition.plan_host_transition(candidate)
    assert planned.phase is transition.TransitionPhase.COMPLETE
    assert planned.effects == ()


@pytest.mark.parametrize(
    ("kind", "outcome", "accepted"),
    [
        (transition.HoldProofKind.NONE, "observed-inactive", False),
        (transition.HoldProofKind.EXISTING_BACKLINK, "observed-inactive", False),
        (transition.HoldProofKind.OBSERVED_INACTIVE, "observed-inactive", True),
        (
            transition.HoldProofKind.CONDITIONAL_ABSENCE_CONFIRMED,
            "conditional-absence-confirmed",
            True,
        ),
    ],
)
def test_fresh_hold_proof_algebra(
    kind: transition.HoldProofKind,
    outcome: str,
    accepted: bool,
) -> None:
    fresh = _make_recovery()
    proof = _proof(fresh.failures[0].report, kind=kind, outcome=outcome)
    candidate = _make_recovery(
        failures=fresh.failures,
        old_snapshots=fresh.old_snapshots,
        proofs=(proof,),
        latest=fresh.latest_report,
        actions=fresh.plan_actions,
    )
    planned = transition.plan_host_transition(candidate)
    if accepted:
        assert isinstance(planned.effects[0], transition.WriteCurrentAckAndIntent)
    else:
        assert isinstance(planned.effects[0], transition.Reject)


def test_released_hold_requires_exact_full_kind_and_canonical_identity_fields() -> None:
    fresh = _make_recovery()
    report = fresh.failures[0].report
    for field, value in (
        ("kind", "partial"),
        ("kind", None),
        ("host", "host_a"),
        ("status", "cleanup-failed"),
        ("owned", False),
        ("legacy_reconciliation", {"outcome": "other"}),
    ):
        settled = _settled_hold()
        if value is None:
            settled.pop(field)
        else:
            settled[field] = value
        proof = transition.HoldProof(
            old_rollout_id=report.rollout_id,
            kind=transition.HoldProofKind.OBSERVED_INACTIVE,
            settled_hold=settled,
            settled_hold_sha256=transition.canonical_json_sha256(settled),
        )
        _assert_reject(
            _make_recovery(
                failures=fresh.failures,
                old_snapshots=fresh.old_snapshots,
                proofs=(proof,),
                latest=fresh.latest_report,
                actions=fresh.plan_actions,
            ),
            "canonical",
        )


def test_exact_backlink_requires_settled_hash_retained_absent_and_no_skip() -> None:
    fresh = _make_recovery()
    intent = _fresh_intent(fresh)
    group = fresh.failures[0]
    exact_record = {
        "status": "failed",
        "legacy_failure_update_ack": _backlink(intent),
    }
    exact = replace(
        fresh.old_snapshots[0],
        relevant_action_records={group.failed_action_ids[0]: exact_record},
        hold=_settled_hold(),
    )
    accepted = _recovery_with_intent(
        fresh,
        intent,
        old_snapshots=(exact,),
        proofs=(_existing_proof(group.report),),
    )
    assert isinstance(
        transition.plan_host_transition(accepted).effects[0], transition.ConsumeMember
    )

    bad_snapshots = (
        replace(exact, hold=_active_hold()),
        replace(exact, hold={**_settled_hold(), "extra": True}),
        replace(exact, retained_hold={"reason": "still live"}),
        replace(
            exact,
            relevant_action_records={
                group.failed_action_ids[0]: {
                    **exact_record,
                    "legacy_failure_skip": {"old": True},
                }
            },
        ),
    )
    for snapshot in bad_snapshots:
        _assert_reject(
            _recovery_with_intent(
                fresh,
                intent,
                old_snapshots=(snapshot,),
                proofs=(_existing_proof(group.report),),
            )
        )
    _assert_reject(
        _recovery_with_intent(
            fresh,
            intent,
            old_snapshots=(exact,),
            proofs=(_proof(group.report),),
        )
    )


@pytest.mark.parametrize("phase", ["existing", "terminal"])
@pytest.mark.parametrize("kind_mode", ["missing", "non-full"])
def test_hash_consistent_released_hold_still_requires_exact_full_kind(
    phase: str,
    kind_mode: str,
) -> None:
    fresh = _make_recovery()
    intent = _fresh_intent(fresh)
    group = fresh.failures[0]
    settled = _settled_hold()
    if kind_mode == "missing":
        settled.pop("kind")
    else:
        settled["kind"] = "partial"
    settled_sha = transition.canonical_json_sha256(settled)
    member = replace(intent.sources[0], settled_hold_sha256=settled_sha)
    bad_intent = replace(intent, sources=(member,))
    linked = replace(
        fresh.old_snapshots[0],
        relevant_action_records={
            group.failed_action_ids[0]: {
                "status": "failed",
                "legacy_failure_update_ack": _backlink(bad_intent),
            }
        },
        hold=settled,
        retained_hold=None,
    )
    proof = _existing_proof(group.report, settled=settled)
    if phase == "existing":
        candidate = _recovery_with_intent(
            fresh,
            bad_intent,
            old_snapshots=(linked,),
            proofs=(proof,),
        )
    else:
        consumed = replace(
            group,
            attempts=tuple(replace(item, consumed=True) for item in group.attempts),
        )
        candidate = _make_recovery(
            failures=(consumed,),
            old_snapshots=(linked,),
            current_snapshot=replace(
                _current_with_intent(fresh, bad_intent),
                forward_intent=None,
            ),
            proofs=(proof,),
            latest=fresh.latest_report,
            actions=(),
            receipt_report=_receipt(bad_intent),
        )
    _assert_reject(candidate, "kind")


def test_complete_old_snapshot_cannot_consume_or_reopen_fresh_recovery() -> None:
    fresh = _make_recovery()
    intent = _fresh_intent(fresh)
    group = fresh.failures[0]
    complete_linked = replace(
        fresh.old_snapshots[0],
        relevant_action_records={
            group.failed_action_ids[0]: {
                "status": "failed",
                "legacy_failure_update_ack": _backlink(intent),
            }
        },
        hold=_settled_hold(),
        retained_hold=None,
        complete=True,
    )
    existing = transition.plan_host_transition(
        _recovery_with_intent(
            fresh,
            intent,
            old_snapshots=(complete_linked,),
            proofs=(_existing_proof(group.report),),
        )
    )

    complete_unlinked = replace(fresh.old_snapshots[0], complete=True)
    reopening = transition.plan_host_transition(
        _make_recovery(
            failures=fresh.failures,
            old_snapshots=(complete_unlinked,),
            proofs=fresh.hold_proofs,
            latest=fresh.latest_report,
            actions=fresh.plan_actions,
        )
    )
    assert all(
        len(planned.effects) == 1 and isinstance(planned.effects[0], transition.Reject)
        for planned in (existing, reopening)
    )


def test_consumed_audit_groups_require_one_common_receipt_context() -> None:
    old_a = _report("a-old", SHA_A)
    old_b = _report("b-old", SHA_B)
    group_a = _failure(old_a, reason="first")
    group_b = _failure(
        old_b,
        reason="second",
        action_ids=("local-runtime:host_e:vibeqc-dev",),
    )
    snap_a = _snapshot(
        old_a,
        token_digest=SHA_A,
        records={group_a.failed_action_ids[0]: {"status": "failed"}},
        hold=_active_hold(),
    )
    snap_b = _snapshot(
        old_b,
        token_digest=SHA_B,
        records={group_b.failed_action_ids[0]: {"status": "failed"}},
        hold=_active_hold(),
    )
    fresh = _make_recovery(
        failures=(group_a, group_b),
        old_snapshots=(snap_a, snap_b),
        proofs=(_proof(old_a), _proof(old_b)),
    )
    intent = _fresh_intent(fresh)
    consumed_a = replace(
        group_a,
        attempts=tuple(replace(item, consumed=True) for item in group_a.attempts),
    )
    consumed_b = replace(
        group_b,
        attempts=tuple(replace(item, consumed=True) for item in group_b.attempts),
    )
    linked_a = replace(
        snap_a,
        relevant_action_records={
            group_a.failed_action_ids[0]: {
                "status": "failed",
                "legacy_failure_update_ack": _backlink(intent, 0),
            }
        },
        hold=_settled_hold(),
    )
    baseline_b = _backlink(intent, 1)
    divergent_common_fields: dict[str, object] = {
        "current_report_path": "reports/other-current.json",
        "current_report_digest_sha256": SHA_C,
        "current_rollout_id": "other-current",
        "current_action_ids": ["other-current-action"],
        "current_actions_sha256": SHA_C,
        "recorded_at": "2026-08-25T20:31:00+00:00",
    }
    audit_latest = transition.LatestReportRef(_report("audit-latest", SHA_C), "generation")
    receipt_current = replace(_current_with_intent(fresh, intent), forward_intent=None)
    linked_b_exact = replace(
        snap_b,
        relevant_action_records={
            group_b.failed_action_ids[0]: {
                "status": "failed",
                "legacy_failure_update_ack": baseline_b,
            }
        },
        hold=_settled_hold(),
    )
    exact = _make_recovery(
        failures=(consumed_a, consumed_b),
        old_snapshots=(linked_a, linked_b_exact),
        current_snapshot=receipt_current,
        proofs=(_existing_proof(old_a), _existing_proof(old_b)),
        latest=audit_latest,
        actions=(),
        receipt_report=_receipt(intent),
    )
    completed = transition.plan_host_transition(exact)
    assert completed.phase is transition.TransitionPhase.COMPLETE
    assert completed.effects == ()
    for field_name, different_value in divergent_common_fields.items():
        divergent_b = {**baseline_b, field_name: different_value}
        transition.parse_failure_update_backlink(divergent_b)
        linked_b = replace(
            snap_b,
            relevant_action_records={
                group_b.failed_action_ids[0]: {
                    "status": "failed",
                    "legacy_failure_update_ack": divergent_b,
                }
            },
            hold=_settled_hold(),
        )
        candidate = _make_recovery(
            failures=(consumed_a, consumed_b),
            old_snapshots=(linked_a, linked_b),
            current_snapshot=receipt_current,
            proofs=(_existing_proof(old_a), _existing_proof(old_b)),
            latest=audit_latest,
            actions=(),
            receipt_report=_receipt(intent),
        )
        _assert_reject(candidate, "receipt context")

    linked_b = replace(
        snap_b,
        relevant_action_records={
            group_b.failed_action_ids[0]: {
                "status": "failed",
                "legacy_failure_update_ack": baseline_b,
            }
        },
        hold={**_settled_hold(), "legacy_reconciliation": {"outcome": "observed-inactive"}},
    )
    wrong_hold_proof = transition.HoldProof(
        old_rollout_id=old_b.rollout_id,
        kind=transition.HoldProofKind.EXISTING_BACKLINK,
        settled_hold=_settled_hold(),
        settled_hold_sha256=transition.canonical_json_sha256(_settled_hold()),
    )
    tampered_hold = {**_settled_hold(), "extra": "hash divergence"}
    linked_b = replace(linked_b, hold=tampered_hold)
    _assert_reject(
        _make_recovery(
            failures=(consumed_a, consumed_b),
            old_snapshots=(linked_a, linked_b),
            current_snapshot=receipt_current,
            proofs=(_existing_proof(old_a), wrong_hold_proof),
            latest=audit_latest,
            actions=(),
            receipt_report=_receipt(intent),
        ),
        "hold",
    )


def test_existing_backlink_rejects_missing_hold_after_conditional_settlement() -> None:
    fresh = _make_recovery()
    conditional = _proof(
        fresh.failures[0].report,
        kind=transition.HoldProofKind.CONDITIONAL_ABSENCE_CONFIRMED,
        outcome="conditional-absence-confirmed",
    )
    conditional_fresh = _make_recovery(
        failures=fresh.failures,
        old_snapshots=fresh.old_snapshots,
        proofs=(conditional,),
        latest=fresh.latest_report,
        actions=fresh.plan_actions,
    )
    intent = _fresh_intent(conditional_fresh)
    group = fresh.failures[0]
    linked_absent = replace(
        fresh.old_snapshots[0],
        relevant_action_records={
            group.failed_action_ids[0]: {
                "status": "failed",
                "legacy_failure_update_ack": _backlink(intent),
            }
        },
        hold=None,
        retained_hold=None,
    )
    existing = transition.HoldProof(
        old_rollout_id=group.report.rollout_id,
        kind=transition.HoldProofKind.EXISTING_BACKLINK,
        settled_hold=conditional.settled_hold,
        settled_hold_sha256=conditional.settled_hold_sha256,
    )
    planned = transition.plan_host_transition(
        _recovery_with_intent(
            conditional_fresh,
            intent,
            old_snapshots=(linked_absent,),
            proofs=(existing,),
        )
    )
    assert len(planned.effects) == 1
    assert isinstance(planned.effects[0], transition.Reject)
    assert "hold" in planned.effects[0].reason


def test_consumed_before_full_backlink_is_never_healed() -> None:
    old = _report("two-actions", SHA_A)
    ids = ("local-runtime:host_e:vibeqc-release", "local-runtime:host_e:vibeqc-dev")
    group = _failure(old, action_ids=ids)
    snapshot = _snapshot(
        old,
        token_digest=SHA_C,
        records={action_id: {"status": "failed"} for action_id in ids},
        hold=_active_hold(),
    )
    fresh = _make_recovery(failures=(group,), old_snapshots=(snapshot,), proofs=(_proof(old),))
    intent = _fresh_intent(fresh)
    consumed_one = replace(
        group,
        attempts=(replace(group.attempts[0], consumed=True), group.attempts[1]),
    )
    _assert_reject(
        _recovery_with_intent(fresh, intent, failures=(consumed_one,)),
        "consumed",
    )


def test_valid_but_wrong_backlink_field_tampering_rejects() -> None:
    fresh = _make_recovery()
    intent = _fresh_intent(fresh)
    expected = _backlink(intent)
    mutations: dict[str, object] = {
        "schema": transition.FAILURE_UPDATE_BACKLINK_SCHEMA,
        "host": "host_a",
        "failed_action_ids": ["other-action"],
        "failed_operation_ids": [SHA_D],
        "historical_report_path": "reports/other.json",
        "historical_report_digest_sha256": SHA_C,
        "settled_hold_sha256": SHA_C,
        "current_report_path": "reports/other-current.json",
        "current_report_digest_sha256": SHA_C,
        "current_rollout_id": "other-current",
        "current_action_ids": ["other-current-action"],
        "current_actions_sha256": SHA_C,
        "recorded_at": "2026-08-25T20:31:00+00:00",
    }
    group = fresh.failures[0]
    for name, value in mutations.items():
        if name == "schema":
            continue
        raw = copy.deepcopy(expected)
        raw[name] = value
        transition.parse_failure_update_backlink(raw)
        snapshot = replace(
            fresh.old_snapshots[0],
            relevant_action_records={
                group.failed_action_ids[0]: {
                    "status": "failed",
                    "legacy_failure_update_ack": raw,
                }
            },
            hold=_settled_hold(),
        )
        _assert_reject(
            _recovery_with_intent(
                fresh,
                intent,
                old_snapshots=(snapshot,),
                proofs=(_existing_proof(group.report),),
            )
        )


def test_backlink_must_be_placed_on_all_and_only_failed_rows() -> None:
    old = _report("two-actions", SHA_A)
    ids = ("local-runtime:host_e:vibeqc-release", "local-runtime:host_e:vibeqc-dev")
    group = _failure(old, action_ids=ids)
    snapshot = _snapshot(
        old,
        token_digest=SHA_C,
        records={action_id: {"status": "failed"} for action_id in ids},
        hold=_active_hold(),
    )
    fresh = _make_recovery(failures=(group,), old_snapshots=(snapshot,), proofs=(_proof(old),))
    intent = _fresh_intent(fresh)
    backlink = _backlink(intent)
    partial = replace(
        snapshot,
        relevant_action_records={
            ids[0]: {"status": "failed", "legacy_failure_update_ack": backlink},
            ids[1]: {"status": "failed"},
        },
        hold=_settled_hold(),
    )
    _assert_reject(
        _recovery_with_intent(
            fresh,
            intent,
            old_snapshots=(partial,),
            proofs=(_existing_proof(old),),
        )
    )
    extra = replace(
        snapshot,
        relevant_action_records={
            **{
                action_id: {"status": "failed", "legacy_failure_update_ack": backlink}
                for action_id in ids
            },
            "unrelated": {"legacy_failure_update_ack": backlink},
        },
        hold=_settled_hold(),
    )
    _assert_reject(
        _recovery_with_intent(
            fresh,
            intent,
            old_snapshots=(extra,),
            proofs=(_existing_proof(old),),
        )
    )


def test_mapping_inputs_are_recursively_frozen_and_thawed_as_fresh_adapter_values() -> None:
    before: dict[str, Any] = {
        "configured": True,
        "current_sha": "0" * 40,
        "current_version": "0.15.137",
        "current_tag": "v0.15.137",
        "dirty": False,
        "last_ok": True,
        "acknowledged": True,
        "detail": "authenticated pre-update lane state",
        "metrics": {"nested": {"items": ["one", {"two": 2}]}},
        "required": False,
    }
    action = _action("local-runtime:host_e:vibeqc-release", before=before)
    before["metrics"]["nested"]["items"][1]["two"] = 999
    before["metrics"]["nested"]["items"].append("late")
    assert cast(dict[str, Any], transition.thaw_json(action.before))["metrics"] == {
        "nested": {"items": ["one", {"two": 2}]}
    }

    records: dict[str, Any] = {"action": {"status": "failed", "nested": [1, {"value": 2}]}}
    hold = _active_hold()
    retained: dict[str, Any] = {"reason": {"detail": ["live"]}}
    intent_raw: dict[str, Any] = {"outer": {"inner": ["intent"]}}
    report = _report("frozen", SHA_A)
    snapshot = _snapshot(
        report,
        token_digest=SHA_B,
        records=records,
        hold=hold,
        retained=retained,
        intent=intent_raw,
    )
    records["action"]["nested"][1]["value"] = 999
    hold["status"] = "changed"
    retained["reason"]["detail"].append("late")
    intent_raw["outer"]["inner"].append("late")
    assert transition.thaw_json(snapshot.relevant_action_records) == {
        "action": {"status": "failed", "nested": [1, {"value": 2}]}
    }
    assert cast(dict[str, Any], transition.thaw_json(snapshot.hold))["status"] == "active"
    assert transition.thaw_json(snapshot.retained_hold) == {"reason": {"detail": ["live"]}}
    assert transition.thaw_json(snapshot.forward_intent) == {"outer": {"inner": ["intent"]}}

    settled = _settled_hold()
    proof = transition.HoldProof(
        report.rollout_id,
        transition.HoldProofKind.OBSERVED_INACTIVE,
        settled,
        transition.canonical_json_sha256(settled),
    )
    settled["status"] = "changed"
    assert cast(dict[str, Any], transition.thaw_json(proof.settled_hold))["status"] == "released"

    nested_set: dict[str, Any] = {"nested": {"items": [1, 2]}}
    edit = transition.ActionEdit("action", set_items=(("receipt", nested_set),))
    nested_set["nested"]["items"].append(3)
    assert transition.thaw_json(edit.set_items[0][1]) == {"nested": {"items": [1, 2]}}

    thawed = cast(dict[str, Any], transition.thaw_json(snapshot.relevant_action_records))
    thawed["action"]["status"] = "adapter mutation"
    frozen_action = cast(Mapping[str, object], snapshot.relevant_action_records["action"])
    assert frozen_action["status"] == "failed"
    with pytest.raises(TypeError):
        snapshot.relevant_action_records["late"] = {}  # type: ignore[index]


def test_frozen_json_private_storage_cannot_be_rebound_or_mutated() -> None:
    action = _action("local-runtime:host_e:vibeqc-release")
    original = transition.thaw_json(action.before)
    with pytest.raises((AttributeError, TypeError)):
        action.before._items = (("configured", False),)  # type: ignore[attr-defined]
    with pytest.raises((AttributeError, TypeError)):
        object.__setattr__(action.before, "_items", (("configured", False),))
    metrics = action.before["metrics"]
    with pytest.raises((AttributeError, TypeError)):
        metrics._items = (("late", True),)  # type: ignore[attr-defined,union-attr]
    assert transition.thaw_json(action.before) == original


def test_effect_payload_mappings_remain_immutable_after_construction() -> None:
    fresh = _make_recovery()
    first = transition.plan_host_transition(fresh)
    current_effect = first.effects[0]
    assert isinstance(current_effect, transition.WriteCurrentAckAndIntent)
    current_rows = cast(
        dict[str, Any], transition.thaw_json(dict(current_effect.action_replacements))
    )
    current_rows[next(iter(current_rows))]["status"] = "changed"
    frozen_row = dict(current_effect.action_replacements)[next(iter(current_rows))]
    assert frozen_row["status"] == "not-run"

    intent = current_effect.intent
    existing = _recovery_with_intent(fresh, intent)
    old_effect = transition.plan_host_transition(existing).effects[0]
    assert isinstance(old_effect, transition.WriteOldBacklink)
    thawed_hold = cast(dict[str, Any], transition.thaw_json(old_effect.settled_hold))
    thawed_hold["status"] = "changed"
    assert old_effect.settled_hold["status"] == "released"
    thawed_receipt = cast(
        dict[str, Any],
        transition.thaw_json(old_effect.action_edits[0].set_items[0][1]),
    )
    thawed_receipt["host"] = "host_a"
    frozen_receipt = cast(Mapping[str, object], old_effect.action_edits[0].set_items[0][1])
    assert frozen_receipt["host"] == HOST


def test_retry_true_is_rejected_at_attempt_and_effect_boundaries() -> None:
    with pytest.raises(transition.TransitionError, match="retry"):
        transition.FailureAttemptRef("action", SHA_A, retry_authorized=True)
    attempt = transition.FailureAttemptRef("action", SHA_A)
    report = _report("old", SHA_B)
    latest = transition.LatestReportRef(_report("current", SHA_C), "generation")
    vector = transition.MutationVector(
        latest,
        tuple(
            sorted(
                (_token(report, SHA_A), _token(latest.ref, SHA_B)), key=lambda item: item.rollout_id
            )
        ),
    )
    with pytest.raises(transition.TransitionError, match="retry"):
        transition.ConsumeMember(
            expected_vector=vector,
            old_report=report,
            host=HOST,
            attempts=(attempt,),
            retry_authorized=True,
        )


def test_mutation_effect_identity_and_safety_flags_are_cross_bound() -> None:
    fresh = _make_recovery()
    current_effect = transition.plan_host_transition(fresh).effects[0]
    assert isinstance(current_effect, transition.WriteCurrentAckAndIntent)
    with pytest.raises(transition.TransitionError):
        replace(current_effect, host="host_a")
    with pytest.raises(transition.TransitionError):
        replace(current_effect, report=_report("other", SHA_C))
    with pytest.raises(transition.TransitionError, match="complete"):
        replace(current_effect, set_complete_false=False)

    intent = current_effect.intent
    old_effect = transition.plan_host_transition(_recovery_with_intent(fresh, intent)).effects[0]
    assert isinstance(old_effect, transition.WriteOldBacklink)
    with pytest.raises(transition.TransitionError):
        replace(old_effect, host="host_a")
    with pytest.raises(transition.TransitionError):
        replace(old_effect, old_report=_report("other", SHA_C))
    with pytest.raises(transition.TransitionError, match="flags"):
        replace(old_effect, delete_retained_hold=False)
    with pytest.raises(transition.TransitionError, match="flags"):
        replace(old_effect, set_complete_false=False)

    linked = replace(
        fresh.old_snapshots[0],
        relevant_action_records={
            fresh.failures[0].failed_action_ids[0]: {
                "status": "failed",
                "legacy_failure_update_ack": _backlink(intent),
            }
        },
        hold=_settled_hold(),
    )
    consuming = _recovery_with_intent(
        fresh,
        intent,
        old_snapshots=(linked,),
        proofs=(_existing_proof(fresh.failures[0].report),),
    )
    consume_effect = transition.plan_host_transition(consuming).effects[0]
    assert isinstance(consume_effect, transition.ConsumeMember)
    with pytest.raises(transition.TransitionError, match="deployment|retry"):
        replace(consume_effect, consumed=False)


def test_mutation_payloads_require_private_planner_provenance_to_enter_transition() -> None:
    fresh = _make_recovery()
    current_transition = transition.plan_host_transition(fresh)
    current_effect = current_transition.effects[0]
    assert isinstance(current_effect, transition.WriteCurrentAckAndIntent)

    intent = current_effect.intent
    old_transition = transition.plan_host_transition(_recovery_with_intent(fresh, intent))
    old_effect = old_transition.effects[0]
    assert isinstance(old_effect, transition.WriteOldBacklink)
    linked = replace(
        fresh.old_snapshots[0],
        relevant_action_records={
            fresh.failures[0].failed_action_ids[0]: {
                "status": "failed",
                "legacy_failure_update_ack": _backlink(intent),
            }
        },
        hold=_settled_hold(),
    )
    consuming = _recovery_with_intent(
        fresh,
        intent,
        old_snapshots=(linked,),
        proofs=(_existing_proof(fresh.failures[0].report),),
    )
    consume_transition = transition.plan_host_transition(consuming)
    consume_effect = consume_transition.effects[0]
    assert isinstance(consume_effect, transition.ConsumeMember)
    consumed_group = replace(
        fresh.failures[0],
        attempts=tuple(replace(item, consumed=True) for item in fresh.failures[0].attempts),
    )
    clearing = _recovery_with_intent(
        fresh,
        intent,
        old_snapshots=(linked,),
        failures=(consumed_group,),
        proofs=(_existing_proof(fresh.failures[0].report),),
    )
    clear_transition = transition.plan_host_transition(clearing)
    clear_effect = clear_transition.effects[0]
    assert isinstance(clear_effect, transition.ClearForwardIntent)

    authenticated = (
        (current_transition, current_effect),
        (old_transition, old_effect),
        (consume_transition, consume_effect),
        (clear_transition, clear_effect),
    )
    for planned, effect in authenticated:
        assert planned.authorized_effects() == (effect,)
        with pytest.raises(transition.TransitionError, match="planner|provenance|seal"):
            transition.HostTransition(planned.phase, HOST, (effect,))
        naked = type(effect)(
            **{item.name: getattr(effect, item.name) for item in fields(type(effect))}
        )
        for untrusted in (naked, replace(effect)):
            with pytest.raises(transition.TransitionError, match="planner|provenance|seal"):
                transition.HostTransition(planned.phase, HOST, (untrusted,))

    for changes in (
        {},
        {"phase": transition.TransitionPhase.ACKED},
        {"host": "host_a"},
        {"effects": ()},
    ):
        with pytest.raises(transition.TransitionError):
            replace(current_transition, **changes)

    with pytest.raises(transition.TransitionError, match="replacement|rows"):
        replace(current_effect, action_replacements=())

    extra_failed_id = "local-runtime:host_e:foreign-failure"
    partial_backlink = replace(
        old_effect.backlink,
        failed_action_ids=tuple(sorted((*old_effect.backlink.failed_action_ids, extra_failed_id))),
    )
    with pytest.raises(transition.TransitionError, match="action edits|failed actions"):
        replace(old_effect, backlink=partial_backlink)

    foreign_attempt = transition.FailureAttemptRef(
        "local-runtime:host_e:foreign-failure",
        "f" * 64,
    )
    foreign_consume = replace(consume_effect, attempts=(foreign_attempt,))
    with pytest.raises(transition.TransitionError, match="planner|provenance|seal"):
        transition.HostTransition(
            transition.TransitionPhase.CONSUMING,
            HOST,
            (foreign_consume,),
        )

    replaced_clear = replace(clear_effect, intent_sha256=SHA_A)
    with pytest.raises(transition.TransitionError, match="planner|provenance|seal"):
        transition.HostTransition(
            transition.TransitionPhase.COMPLETE,
            HOST,
            (replaced_clear,),
        )

    stale_digest = transition.plan_host_transition(fresh).effects[0]
    assert isinstance(stale_digest, transition.WriteCurrentAckAndIntent)
    object.__setattr__(stale_digest, "failed_reason", "post-plan payload mutation")
    with pytest.raises(transition.TransitionError, match="digest|provenance"):
        transition.HostTransition(
            transition.TransitionPhase.PREVALIDATED,
            HOST,
            (stale_digest,),
        )

    cleared_current = replace(clearing.current_snapshot, forward_intent=None)
    after_clear = _make_recovery(
        failures=(consumed_group,),
        old_snapshots=(linked,),
        current_snapshot=cleared_current,
        proofs=(_existing_proof(fresh.failures[0].report),),
        latest=fresh.latest_report,
        actions=(),
        receipt_report=_receipt(intent),
    )
    empty_complete = transition.plan_host_transition(after_clear)
    assert empty_complete.phase is transition.TransitionPhase.COMPLETE
    assert empty_complete.effects == ()
    with pytest.raises(transition.TransitionError, match="planner|provenance|seal"):
        transition.HostTransition(transition.TransitionPhase.COMPLETE, HOST, ())
    with pytest.raises(transition.TransitionError, match="planner|provenance|seal"):
        replace(empty_complete)

    payload_mutations = (
        (current_transition, current_effect, "action_replacements", ()),
        (old_transition, old_effect, "action_edits", ()),
        (consume_transition, consume_effect, "attempts", ()),
        (clear_transition, clear_effect, "intent_sha256", SHA_A),
    )
    for planned, effect, field_name, value in payload_mutations:
        object.__setattr__(effect, field_name, value)
        with pytest.raises(transition.TransitionError, match="digest|provenance"):
            planned.authorized_effects()

    for field_name, value in (
        ("phase", transition.TransitionPhase.ACKED),
        ("host", "host_a"),
        ("effects", ()),
    ):
        planned = transition.plan_host_transition(fresh)
        assert planned.authorized_effects()
        object.__setattr__(planned, field_name, value)
        with pytest.raises(transition.TransitionError, match="envelope|digest|provenance"):
            planned.authorized_effects()


def _planned_effects_by_phase() -> dict[str, transition.HostTransition]:
    fresh = _make_recovery()
    current = transition.plan_host_transition(fresh)
    current_effect = current.effects[0]
    assert isinstance(current_effect, transition.WriteCurrentAckAndIntent)
    intent = current_effect.intent
    old = transition.plan_host_transition(_recovery_with_intent(fresh, intent))
    linked = replace(
        fresh.old_snapshots[0],
        relevant_action_records={
            fresh.failures[0].failed_action_ids[0]: {
                "status": "failed",
                "legacy_failure_update_ack": _backlink(intent),
            }
        },
        hold=_settled_hold(),
    )
    consuming = _recovery_with_intent(
        fresh,
        intent,
        old_snapshots=(linked,),
        proofs=(_existing_proof(fresh.failures[0].report),),
    )
    consume = transition.plan_host_transition(consuming)
    consumed_group = replace(
        fresh.failures[0],
        attempts=tuple(replace(item, consumed=True) for item in fresh.failures[0].attempts),
    )
    clear = transition.plan_host_transition(
        _recovery_with_intent(
            fresh,
            intent,
            old_snapshots=(linked,),
            failures=(consumed_group,),
            proofs=(_existing_proof(fresh.failures[0].report),),
        )
    )
    return {"current": current, "old": old, "consume": consume, "clear": clear}


@pytest.mark.parametrize("phase", ("current", "old", "consume", "clear"))
def test_authorized_effects_rejects_same_value_runtime_shape_drift(phase: str) -> None:
    planned = _planned_effects_by_phase()[phase]
    effect = planned.effects[0]
    if phase == "current":
        assert isinstance(effect, transition.WriteCurrentAckAndIntent)
        object.__setattr__(effect, "action_replacements", list(effect.action_replacements))
    elif phase == "old":
        assert isinstance(effect, transition.WriteOldBacklink)
        object.__setattr__(effect, "settled_hold", transition.thaw_json(effect.settled_hold))
    elif phase == "consume":
        assert isinstance(effect, transition.ConsumeMember)
        object.__setattr__(effect, "attempts", list(effect.attempts))
    else:
        assert isinstance(effect, transition.ClearForwardIntent)
        object.__setattr__(
            effect.expected_vector,
            "journals",
            list(effect.expected_vector.journals),
        )
    with pytest.raises(transition.TransitionError):
        planned.authorized_effects()


@pytest.mark.parametrize(
    "mutation",
    (
        "effects-list",
        "host-subclass",
        "missing-transition-seal",
        "foreign-transition-seal",
        "missing-transition-digest",
        "missing-effect-seal",
        "missing-effect-digest",
        "frozen-row-dict",
    ),
)
def test_authorized_effects_rejects_runtime_provenance_drift(mutation: str) -> None:
    planned = transition.plan_host_transition(_make_recovery())
    effect = planned.effects[0]
    assert isinstance(effect, transition.WriteCurrentAckAndIntent)
    if mutation == "effects-list":
        object.__setattr__(planned, "effects", list(planned.effects))
    elif mutation == "host-subclass":

        class HostText(str):
            pass

        object.__setattr__(planned, "host", HostText(planned.host))
    elif mutation == "missing-transition-seal":
        object.__delattr__(planned, "_planner_seal")
    elif mutation == "foreign-transition-seal":
        object.__setattr__(planned, "_planner_seal", object())
    elif mutation == "missing-transition-digest":
        object.__delattr__(planned, "_planner_payload_sha256")
    elif mutation == "missing-effect-seal":
        object.__delattr__(effect, "_planner_seal")
    elif mutation == "missing-effect-digest":
        object.__delattr__(effect, "_planner_payload_sha256")
    else:
        action_id, row = effect.action_replacements[0]
        object.__setattr__(effect, "action_replacements", ((action_id, dict(row)),))
    with pytest.raises(transition.TransitionError):
        planned.authorized_effects()


def test_planner_constructor_capability_is_never_persisted_or_reusable() -> None:
    planned = transition.plan_host_transition(_make_recovery())
    effect = planned.effects[0]
    persisted = [
        getattr(record, name)
        for record in (planned, effect)
        for name in ("_planner_seal", "_planner_payload_sha256")
        if hasattr(record, name)
    ]
    assert persisted
    for leaked in persisted:
        with pytest.raises(transition.TransitionError, match="planner|provenance|seal"):
            transition.HostTransition(
                transition.TransitionPhase.COMPLETE,
                HOST,
                (),
                _planner_token=leaked,
            )


def test_authority_records_cannot_shadow_methods_or_accept_dynamic_attributes() -> None:
    planned_by_phase = _planned_effects_by_phase()
    planned = planned_by_phase["current"]
    effect = planned.effects[0]
    old_effect = planned_by_phase["old"].effects[0]
    consume_effect = planned_by_phase["consume"].effects[0]
    clear_effect = planned_by_phase["clear"].effects[0]
    assert isinstance(effect, transition.WriteCurrentAckAndIntent)
    assert isinstance(old_effect, transition.WriteOldBacklink)
    assert isinstance(consume_effect, transition.ConsumeMember)
    assert isinstance(clear_effect, transition.ClearForwardIntent)
    reachable = (
        planned,
        effect,
        old_effect,
        consume_effect,
        clear_effect,
        effect.intent,
        effect.expected_vector,
        effect.expected_vector.latest,
        effect.report,
        old_effect.backlink,
        old_effect.action_edits[0],
        consume_effect.attempts[0],
    )
    for record in reachable:
        with pytest.raises((AttributeError, TypeError)):
            object.__setattr__(record, "unexpected_authority_state", True)
    with pytest.raises((AttributeError, TypeError)):
        object.__setattr__(planned, "authorized_effects", lambda: planned.effects)
    with pytest.raises((AttributeError, TypeError)):
        object.__setattr__(effect.intent, "as_dict", lambda: {})


def test_nested_payload_method_shadow_cannot_hide_mutation() -> None:
    transitions = _planned_effects_by_phase()
    current = transitions["current"]
    current_effect = current.effects[0]
    assert isinstance(current_effect, transition.WriteCurrentAckAndIntent)
    original_reason = current_effect.intent.failed_host_reason
    original_as_dict = current_effect.intent.as_dict
    object.__setattr__(current_effect.intent, "failed_host_reason", "hidden mutation")
    with suppress(AttributeError, TypeError):
        object.__setattr__(
            current_effect.intent,
            "as_dict",
            lambda: {
                **original_as_dict(),
                "failed_host_reason": original_reason,
            },
        )
    with pytest.raises(transition.TransitionError):
        current.authorized_effects()

    old = transitions["old"]
    old_effect = old.effects[0]
    assert isinstance(old_effect, transition.WriteOldBacklink)
    object.__setattr__(
        old_effect.backlink,
        "current_report_path",
        "reports/hidden-backlink-mutation.json",
    )
    with suppress(AttributeError, TypeError):
        object.__setattr__(old_effect.backlink, "as_dict", lambda: {})
    with pytest.raises(transition.TransitionError):
        old.authorized_effects()


def test_reject_envelope_runtime_shape_is_authorized_exactly() -> None:
    fresh = _make_recovery()
    rejected = transition.plan_host_transition(
        _make_recovery(
            failures=fresh.failures,
            old_snapshots=(replace(fresh.old_snapshots[0], hold=_settled_hold()),),
            proofs=fresh.hold_proofs,
            latest=fresh.latest_report,
            actions=fresh.plan_actions,
        )
    )
    assert isinstance(rejected.effects[0], transition.Reject)
    assert type(rejected.authorized_effects()) is tuple
    object.__setattr__(rejected, "effects", list(rejected.effects))
    with pytest.raises(transition.TransitionError):
        rejected.authorized_effects()


def _planned_reject_transition() -> transition.HostTransition:
    fresh = _make_recovery()
    rejected = transition.plan_host_transition(
        _make_recovery(
            failures=fresh.failures,
            old_snapshots=(replace(fresh.old_snapshots[0], hold=_settled_hold()),),
            proofs=fresh.hold_proofs,
            latest=fresh.latest_report,
            actions=fresh.plan_actions,
        )
    )
    assert isinstance(rejected.effects[0], transition.Reject)
    return rejected


@pytest.mark.parametrize(
    "clone_kind",
    ("direct", "replace", "copy", "deepcopy", "pickle", "object-new"),
)
def test_reject_effect_requires_exact_registered_planner_identity(clone_kind: str) -> None:
    planned = _planned_reject_transition()
    genuine = planned.effects[0]
    assert isinstance(genuine, transition.Reject)
    assert planned.authorized_effects() == (genuine,)
    if clone_kind == "direct":
        clone = transition.Reject(genuine.host, genuine.reason)
    elif clone_kind == "replace":
        clone = replace(genuine)
    elif clone_kind == "copy":
        clone = copy.copy(genuine)
    elif clone_kind == "deepcopy":
        clone = copy.deepcopy(genuine)
    elif clone_kind == "pickle":
        clone = pickle.loads(pickle.dumps(genuine))
    else:
        clone = object.__new__(transition.Reject)
        for item in fields(transition.Reject):
            object.__setattr__(clone, item.name, getattr(genuine, item.name))
    assert clone is not genuine
    for name in ("_planner_seal", "_planner_payload_sha256"):
        if hasattr(genuine, name):
            object.__setattr__(clone, name, getattr(genuine, name))
    object.__setattr__(planned, "effects", (clone,))
    with pytest.raises(transition.TransitionError, match="planner|provenance|registered"):
        planned.authorized_effects()


def test_reject_effect_missing_registry_entry_is_fail_closed() -> None:
    planned = _planned_reject_transition()
    genuine = planned.effects[0]
    assert planned.authorized_effects() == (genuine,)
    assert transition._AUTHORITY_REGISTRY.pop(id(genuine), None) is not None
    with pytest.raises(transition.TransitionError, match="registered"):
        planned.authorized_effects()


@pytest.mark.parametrize("phase", ("current", "old", "consume", "clear"))
def test_copied_private_provenance_never_authorizes_replaced_effect(phase: str) -> None:
    planned = _planned_effects_by_phase()[phase]
    genuine = planned.effects[0]
    clone = replace(genuine)
    for name in ("_planner_seal", "_planner_payload_sha256"):
        object.__setattr__(clone, name, getattr(genuine, name))
    object.__setattr__(planned, "effects", (clone,))
    with pytest.raises(transition.TransitionError, match="planner|provenance|registered"):
        planned.authorized_effects()

    for copier in (copy.copy, copy.deepcopy):
        copied = copier(genuine)
        assert copied is not genuine
        object.__setattr__(planned, "effects", (copied,))
        with pytest.raises(transition.TransitionError, match="planner|provenance|registered"):
            planned.authorized_effects()

    restored = pickle.loads(pickle.dumps(genuine))
    assert restored is not genuine
    object.__setattr__(planned, "effects", (restored,))
    with pytest.raises(transition.TransitionError, match="planner|provenance|registered"):
        planned.authorized_effects()


def test_copied_transition_envelopes_never_inherit_runtime_authority() -> None:
    transitions = tuple(_planned_effects_by_phase().values())
    fresh = _make_recovery()
    current_effect = transition.plan_host_transition(fresh).effects[0]
    assert isinstance(current_effect, transition.WriteCurrentAckAndIntent)
    intent = current_effect.intent
    linked = replace(
        fresh.old_snapshots[0],
        relevant_action_records={
            fresh.failures[0].failed_action_ids[0]: {
                "status": "failed",
                "legacy_failure_update_ack": _backlink(intent),
            }
        },
        hold=_settled_hold(),
    )
    consumed_group = replace(
        fresh.failures[0],
        attempts=tuple(replace(item, consumed=True) for item in fresh.failures[0].attempts),
    )
    cleared_current = replace(_current_with_intent(fresh, intent), forward_intent=None)
    empty_complete = transition.plan_host_transition(
        _make_recovery(
            failures=(consumed_group,),
            old_snapshots=(linked,),
            current_snapshot=cleared_current,
            proofs=(_existing_proof(fresh.failures[0].report),),
            latest=fresh.latest_report,
            actions=(),
            receipt_report=_receipt(intent),
        )
    )
    assert empty_complete.effects == ()
    for genuine in (*transitions, empty_complete):
        clones = (copy.copy(genuine), copy.deepcopy(genuine))
        for clone in clones:
            assert clone is not genuine
            with pytest.raises(
                transition.TransitionError,
                match="planner|provenance|registered",
            ):
                clone.authorized_effects()
        restored = pickle.loads(pickle.dumps(genuine))
        with pytest.raises(transition.TransitionError, match="planner|provenance|registered"):
            restored.authorized_effects()
        manual = object.__new__(transition.HostTransition)
        for item in fields(transition.HostTransition):
            object.__setattr__(manual, item.name, getattr(genuine, item.name))
        for name in ("_planner_seal", "_planner_payload_sha256"):
            object.__setattr__(manual, name, getattr(genuine, name))
        with pytest.raises(transition.TransitionError, match="planner|provenance|registered"):
            manual.authorized_effects()


def test_registry_missing_and_stale_cleanup_are_fail_closed_by_identity() -> None:
    planned = transition.plan_host_transition(_make_recovery())
    digest = planned._planner_payload_sha256
    key = id(planned)
    old_entry = transition._AUTHORITY_REGISTRY[key]
    transition._register_authority(planned, digest)
    replacement_entry = transition._AUTHORITY_REGISTRY[key]
    assert replacement_entry[0] is not old_entry[0]
    callback = old_entry[0].__callback__
    assert callback is not None
    callback(old_entry[0])
    assert transition._AUTHORITY_REGISTRY[key][0] is replacement_entry[0]
    assert planned.authorized_effects()

    del transition._AUTHORITY_REGISTRY[key]
    with pytest.raises(transition.TransitionError, match="registered"):
        planned.authorized_effects()


def test_recovery_input_rejects_post_construction_skip_and_vector_mutation() -> None:
    fresh = _make_recovery()
    action_id = fresh.failures[0].failed_action_ids[0]
    current_group = replace(
        fresh.failures[0],
        skip_states=((action_id, transition.SkipState.CURRENT),),
    )
    recovery = _make_recovery(
        failures=(current_group,),
        old_snapshots=fresh.old_snapshots,
        proofs=fresh.hold_proofs,
        latest=fresh.latest_report,
        actions=fresh.plan_actions,
    )
    transition.plan_host_transition(recovery)
    object.__setattr__(
        recovery.failures[0],
        "skip_states",
        ((action_id, transition.SkipState.STALE),),
    )
    with pytest.raises(transition.TransitionError, match="input|authority|digest|registered"):
        transition.plan_host_transition(recovery)

    vector_recovery = _make_recovery()
    journals = vector_recovery.mutation_vector.journals
    object.__setattr__(
        vector_recovery.mutation_vector,
        "journals",
        (replace(journals[0], digest_sha256=SHA_A), *journals[1:]),
    )
    with pytest.raises(transition.TransitionError, match="input|authority|digest|registered"):
        transition.plan_host_transition(vector_recovery)

    shadow_recovery = _make_recovery()
    with pytest.raises((AttributeError, TypeError)):
        object.__setattr__(
            shadow_recovery.mutation_vector,
            "token_for",
            lambda _rollout_id: shadow_recovery.mutation_vector.journals[0],
        )


@pytest.mark.parametrize(
    "graph",
    (
        "report",
        "group",
        "attempt",
        "hold",
        "settled-hold",
        "old-snapshot",
        "current-snapshot",
        "member",
        "projection",
        "receipt-token",
    ),
)
def test_recovery_input_rejects_nested_capability_mutation(graph: str) -> None:
    fresh = _make_recovery()
    if graph in {"member", "projection", "receipt-token"}:
        intent = transition.plan_host_transition(fresh).effects[0].intent
        recovery = _recovery_with_intent(fresh, intent)
        if graph == "receipt-token":
            object.__setattr__(
                recovery.receipt_report,
                "authentication_token",
                "mutated receipt authentication",
            )
        else:
            raw_intent = transition.thaw_json(recovery.current_snapshot.forward_intent)
            assert isinstance(raw_intent, dict)
            if graph == "member":
                sources = cast(list[dict[str, object]], raw_intent["sources"])
                sources[0]["failure_reason"] = "mutated member reason"
            else:
                projection = cast(dict[str, object], raw_intent["current_plan_projection"])
                actions = cast(list[dict[str, object]], projection["actions"])
                before = cast(dict[str, object], actions[0]["before"])
                before["required"] = not before["required"]
            object.__setattr__(
                recovery.current_snapshot,
                "forward_intent",
                transition._freeze_json(raw_intent),
            )
    else:
        recovery = fresh
        if graph == "report":
            object.__setattr__(recovery.failures[0].report, "source_path", "reports/tamper.json")
        elif graph == "group":
            object.__setattr__(recovery.failures[0], "reason", "mutated group reason")
        elif graph == "attempt":
            object.__setattr__(
                recovery.failures[0],
                "attempts",
                (replace(recovery.failures[0].attempts[0], consumed=True),),
            )
        elif graph == "hold":
            object.__setattr__(recovery.hold_proofs[0], "settled_hold_sha256", SHA_A)
        elif graph == "settled-hold":
            settled = transition.thaw_json(recovery.hold_proofs[0].settled_hold)
            assert isinstance(settled, dict)
            settled["status"] = "mutated"
            object.__setattr__(
                recovery.hold_proofs[0],
                "settled_hold",
                transition._freeze_json(settled),
            )
        elif graph == "old-snapshot":
            object.__setattr__(recovery.old_snapshots[0], "complete", True)
        else:
            object.__setattr__(recovery.current_snapshot, "complete", True)
    with pytest.raises(transition.TransitionError, match="input|authority|digest|registered"):
        transition.plan_host_transition(recovery)


def test_recovery_input_digest_requires_exact_builtin_string() -> None:
    class EqualDigest(str):
        def __eq__(self, other: object) -> bool:
            return True

        def __ne__(self, other: object) -> bool:
            return False

    recovery = _make_recovery()
    assert hasattr(recovery, "_planner_payload_sha256")
    object.__setattr__(
        recovery,
        "_planner_payload_sha256",
        EqualDigest(recovery._planner_payload_sha256),
    )
    with pytest.raises(transition.TransitionError, match="input|authority|digest"):
        transition.plan_host_transition(recovery)


def test_copied_recovery_inputs_never_inherit_runtime_authority() -> None:
    genuine = _make_recovery()
    reconstructed = replace(genuine)
    assert transition.plan_host_transition(reconstructed)
    clones = (
        copy.copy(genuine),
        copy.deepcopy(genuine),
        pickle.loads(pickle.dumps(genuine)),
    )
    for clone in clones:
        assert clone is not genuine
        for name in ("_planner_seal", "_planner_payload_sha256"):
            if hasattr(genuine, name):
                object.__setattr__(clone, name, getattr(genuine, name))
        with pytest.raises(transition.TransitionError, match="input|authority|registered"):
            transition.plan_host_transition(clone)

    manual = object.__new__(transition.HostRecoveryInput)
    for item in fields(transition.HostRecoveryInput):
        object.__setattr__(manual, item.name, getattr(genuine, item.name))
    for name in ("_planner_seal", "_planner_payload_sha256"):
        if hasattr(genuine, name):
            object.__setattr__(manual, name, getattr(genuine, name))
    with pytest.raises(transition.TransitionError, match="input|authority|registered"):
        transition.plan_host_transition(manual)


def test_exact_dataclass_field_and_mutation_effect_surface() -> None:
    report = _report("opaque-token", SHA_A)
    with pytest.raises(transition.TransitionError):
        transition.ReceiptReportRef(report, "")
    with pytest.raises((TypeError, transition.TransitionError)):
        transition.ReceiptReportRef(report, 3)  # type: ignore[arg-type]
    if "authentication_token" in {item.name for item in fields(transition.ReceiptReportRef)}:
        assert transition.ReceiptReportRef(report, "opaque nonempty adapter token").ref == report

    expected_fields = {
        transition.ReportRef: {"source_path", "digest_sha256", "rollout_id"},
        transition.LatestReportRef: {"ref", "discovery_token"},
        transition.ReceiptReportRef: {"ref", "authentication_token"},
        transition.PlanActionRef: {
            "action_id",
            "phase",
            "host",
            "program",
            "decision",
            "reason",
            "pin_name",
            "target_sha",
            "target_version",
            "target_tag",
            "argv",
            "before",
        },
        transition.FailureAttemptRef: {
            "action_id",
            "operation_id",
            "consumed",
            "retry_authorized",
        },
        transition.HostFailureGroup: {"report", "host", "reason", "attempts", "skip_states"},
        transition.FailureUpdateIntent: {
            "schema",
            "host",
            "current_report_path",
            "current_report_digest_sha256",
            "current_rollout_id",
            "current_action_ids",
            "current_plan_projection",
            "current_actions_sha256",
            "current_ack_sha256",
            "failed_host_reason",
            "sources",
            "recorded_at",
        },
        transition.JournalToken: {"rollout_id", "digest_sha256"},
        transition.MutationVector: {"latest", "journals"},
        transition.JournalHostSnapshot: {
            "report",
            "host",
            "token",
            "relevant_action_records",
            "hold",
            "retained_hold",
            "failed_reason",
            "complete",
            "forward_intent",
        },
        transition.HoldProof: {"old_rollout_id", "kind", "settled_hold", "settled_hold_sha256"},
        transition.WriteCurrentAckAndIntent: {
            "expected_vector",
            "report",
            "host",
            "intent",
            "action_replacements",
            "failed_reason",
            "set_complete_false",
        },
        transition.WriteOldBacklink: {
            "expected_vector",
            "old_report",
            "host",
            "backlink",
            "settled_hold",
            "action_edits",
            "delete_retained_hold",
            "set_complete_false",
        },
        transition.ConsumeMember: {
            "expected_vector",
            "old_report",
            "host",
            "attempts",
            "consumed",
            "retry_authorized",
        },
        transition.ClearForwardIntent: {
            "expected_vector",
            "report",
            "host",
            "intent_sha256",
        },
        transition.Reject: {"host", "reason"},
        transition.HostRecoveryInput: {
            "latest_report",
            "host",
            "plan_actions",
            "receipt_report",
            "failures",
            "old_snapshots",
            "current_snapshot",
            "hold_proofs",
            "mutation_vector",
            "recorded_at",
        },
        transition.HostTransition: {"phase", "host", "effects"},
    }
    for data_type, expected in expected_fields.items():
        assert {item.name for item in fields(data_type)} == expected
    assert not hasattr(transition, "receipt_report_from_intent")

    reject = transition.Reject(HOST, "no")
    with pytest.raises(transition.TransitionError, match="at most one"):
        transition.HostTransition(
            transition.TransitionPhase.PREVALIDATED,
            HOST,
            (reject, reject),
        )


def test_current_old_a_old_b_consume_a_consume_b_clear_advance_and_replan() -> None:
    old_a = _report("a-old", SHA_A)
    old_b = _report("b-old", SHA_B)
    group_a = _failure(old_a, reason="first")
    group_b = _failure(old_b, reason="second", action_ids=("local-runtime:host_e:vibeqc-dev",))
    snap_a = _snapshot(
        old_a,
        token_digest=SHA_A,
        records={group_a.failed_action_ids[0]: {"status": "failed"}},
        hold=_active_hold(),
    )
    snap_b = _snapshot(
        old_b,
        token_digest=SHA_B,
        records={group_b.failed_action_ids[0]: {"status": "failed"}},
        hold=_active_hold(),
    )
    fresh = _make_recovery(
        failures=(group_b, group_a),
        old_snapshots=(snap_a, snap_b),
        proofs=(_proof(old_a), _proof(old_b)),
    )
    peer = _token(_report("all-phase-peer-old", SHA_C), "3" * 64)

    def invocation_wide(recovery: transition.HostRecoveryInput) -> transition.HostRecoveryInput:
        return replace(
            recovery,
            mutation_vector=transition.MutationVector(
                recovery.latest_report,
                tuple(
                    sorted(
                        (*recovery.mutation_vector.journals, peer),
                        key=lambda item: item.rollout_id,
                    )
                ),
            ),
        )

    fresh = invocation_wide(fresh)

    def one(recovery: transition.HostRecoveryInput, expected_type: type[object]) -> object:
        planned = transition.plan_host_transition(recovery)
        assert len(planned.effects) == 1
        effect = planned.effects[0]
        assert isinstance(effect, expected_type)
        assert effect.expected_vector == recovery.mutation_vector  # type: ignore[union-attr]
        assert len(effect.expected_vector.journals) == 4  # type: ignore[union-attr]
        assert effect.expected_vector.token_for(peer.rollout_id) == peer  # type: ignore[union-attr]
        newer_latest = transition.LatestReportRef(
            _report("advanced-report", SHA_C),
            "advanced-generation",
        )
        advanced_report_vector = replace(effect.expected_vector, latest=newer_latest)  # type: ignore[union-attr]
        assert advanced_report_vector != effect.expected_vector  # type: ignore[union-attr]
        with pytest.raises(transition.TransitionError, match="latest report"):
            replace(recovery, mutation_vector=advanced_report_vector)
        for index, token in enumerate(effect.expected_vector.journals):  # type: ignore[union-attr]
            changed_digest = f"{index + 4:x}" * 64
            changed = replace(token, digest_sha256=changed_digest)
            journals = tuple(
                changed if item.rollout_id == token.rollout_id else item
                for item in effect.expected_vector.journals  # type: ignore[union-attr]
            )
            changed_vector = replace(effect.expected_vector, journals=journals)  # type: ignore[union-attr]
            assert changed_vector != effect.expected_vector  # type: ignore[union-attr]
            if token == peer:
                with pytest.raises(transition.TransitionError, match="changed"):
                    transition.advance_mutation_vector(changed_vector, peer, changed)
            else:
                with pytest.raises(transition.TransitionError, match="snapshot token"):
                    replace(recovery, mutation_vector=changed_vector)
        return effect

    current_effect = one(fresh, transition.WriteCurrentAckAndIntent)
    assert isinstance(current_effect, transition.WriteCurrentAckAndIntent)
    intent = current_effect.intent
    current = _current_with_intent(fresh, intent, token_digest="e" * 64)

    acked = invocation_wide(
        _make_recovery(
            failures=(group_a, group_b),
            old_snapshots=(snap_a, snap_b),
            current_snapshot=current,
            proofs=(_proof(old_a), _proof(old_b)),
            latest=fresh.latest_report,
            actions=fresh.plan_actions,
            receipt_report=_receipt(intent),
        )
    )
    old_a_effect = one(acked, transition.WriteOldBacklink)
    assert isinstance(old_a_effect, transition.WriteOldBacklink)
    assert old_a_effect.old_report == old_a
    with pytest.raises(transition.TransitionError, match="changed"):
        transition.advance_mutation_vector(
            acked.mutation_vector,
            fresh.mutation_vector.token_for(current.report.rollout_id),
            _token(current.report, "f" * 64),
        )

    linked_a = replace(
        snap_a,
        token=_token(old_a, "f" * 64),
        relevant_action_records={
            group_a.failed_action_ids[0]: {
                "status": "failed",
                "legacy_failure_update_ack": _backlink(intent, 0),
            }
        },
        hold=_settled_hold(),
    )
    backlinking = invocation_wide(
        _make_recovery(
            failures=(group_a, group_b),
            old_snapshots=(linked_a, snap_b),
            current_snapshot=current,
            proofs=(_existing_proof(old_a), _proof(old_b)),
            latest=fresh.latest_report,
            actions=fresh.plan_actions,
            receipt_report=_receipt(intent),
        )
    )
    old_b_effect = one(backlinking, transition.WriteOldBacklink)
    assert isinstance(old_b_effect, transition.WriteOldBacklink)
    assert old_b_effect.old_report == old_b

    linked_b = replace(
        snap_b,
        token=_token(old_b, "7" * 64),
        relevant_action_records={
            group_b.failed_action_ids[0]: {
                "status": "failed",
                "legacy_failure_update_ack": _backlink(intent, 1),
            }
        },
        hold=_settled_hold(),
    )
    consuming_a = invocation_wide(
        _make_recovery(
            failures=(group_a, group_b),
            old_snapshots=(linked_a, linked_b),
            current_snapshot=current,
            proofs=(_existing_proof(old_a), _existing_proof(old_b)),
            latest=fresh.latest_report,
            actions=fresh.plan_actions,
            receipt_report=_receipt(intent),
        )
    )
    consume_a = one(consuming_a, transition.ConsumeMember)
    assert isinstance(consume_a, transition.ConsumeMember)
    assert consume_a.old_report == old_a

    consumed_group_a = replace(
        group_a,
        attempts=tuple(replace(item, consumed=True) for item in group_a.attempts),
    )
    consumed_snap_a = replace(linked_a, token=_token(old_a, "8" * 64))
    consuming_b = invocation_wide(
        _make_recovery(
            failures=(consumed_group_a, group_b),
            old_snapshots=(consumed_snap_a, linked_b),
            current_snapshot=current,
            proofs=(_existing_proof(old_a), _existing_proof(old_b)),
            latest=fresh.latest_report,
            actions=fresh.plan_actions,
            receipt_report=_receipt(intent),
        )
    )
    consume_b = one(consuming_b, transition.ConsumeMember)
    assert isinstance(consume_b, transition.ConsumeMember)
    assert consume_b.old_report == old_b

    consumed_group_b = replace(
        group_b,
        attempts=tuple(replace(item, consumed=True) for item in group_b.attempts),
    )
    consumed_snap_b = replace(linked_b, token=_token(old_b, "9" * 64))
    completing = invocation_wide(
        _make_recovery(
            failures=(consumed_group_a, consumed_group_b),
            old_snapshots=(consumed_snap_a, consumed_snap_b),
            current_snapshot=current,
            proofs=(_existing_proof(old_a), _existing_proof(old_b)),
            latest=fresh.latest_report,
            actions=fresh.plan_actions,
            receipt_report=_receipt(intent),
        )
    )
    clear = one(completing, transition.ClearForwardIntent)
    assert isinstance(clear, transition.ClearForwardIntent)


def test_two_hosts_sharing_old_journal_are_fenced_by_whole_journal_token() -> None:
    old = _report("shared-old", SHA_A)
    current = _report("current", SHA_B)
    latest = transition.LatestReportRef(current, "generation-9")
    shared_token = _token(old, SHA_C)
    vector = transition.MutationVector(
        latest,
        tuple(sorted((shared_token, _token(current, SHA_D)), key=lambda item: item.rollout_id)),
    )
    advanced = transition.advance_mutation_vector(vector, shared_token, _token(old, "e" * 64))
    assert advanced.token_for(old.rollout_id).digest_sha256 == "e" * 64
    with pytest.raises(transition.TransitionError, match="changed"):
        transition.advance_mutation_vector(advanced, shared_token, _token(old, "f" * 64))

    host_e = _failure(old)
    host_a = _failure(
        old,
        host="host_a",
        action_ids=("local-runtime:host_a:vibeqc-release",),
    )
    host_e_snapshot = _snapshot(
        old,
        token_digest=SHA_C,
        records={host_e.failed_action_ids[0]: {"status": "failed"}},
        hold=_active_hold(),
    )
    host_a_snapshot = _snapshot(
        old,
        host="host_a",
        token_digest=SHA_C,
        records={host_a.failed_action_ids[0]: {"status": "failed"}},
        hold=_active_hold("host_a"),
    )
    assert host_e_snapshot.token == host_a_snapshot.token

    current_host_e = _snapshot(current, token_digest=SHA_D)
    current_host_a = _snapshot(current, host="host_a", token_digest=SHA_D)
    host_e_recovery = _make_recovery(
        failures=(host_e,),
        old_snapshots=(host_e_snapshot,),
        current_snapshot=current_host_e,
        proofs=(_proof(old),),
        latest=latest,
        actions=(_action("local-runtime:host_e:vibeqc-release"),),
    )
    host_a_recovery = _make_recovery(
        failures=(host_a,),
        old_snapshots=(host_a_snapshot,),
        current_snapshot=current_host_a,
        proofs=(_proof(old, "host_a"),),
        latest=latest,
        actions=(_action("local-runtime:host_a:vibeqc-release", host="host_a"),),
        host="host_a",
    )
    host_e_effect = transition.plan_host_transition(host_e_recovery).effects[0]
    host_a_effect = transition.plan_host_transition(host_a_recovery).effects[0]
    assert isinstance(host_e_effect, transition.WriteCurrentAckAndIntent)
    assert isinstance(host_a_effect, transition.WriteCurrentAckAndIntent)
    assert host_e_effect.expected_vector.token_for(current.rollout_id) == (
        host_a_effect.expected_vector.token_for(current.rollout_id)
    )
    after_host_e = transition.advance_mutation_vector(
        host_e_effect.expected_vector,
        host_e_effect.expected_vector.token_for(current.rollout_id),
        _token(current, "9" * 64),
    )
    with pytest.raises(transition.TransitionError, match="changed"):
        transition.advance_mutation_vector(
            after_host_e,
            host_a_effect.expected_vector.token_for(current.rollout_id),
            _token(current, "8" * 64),
        )


def test_host_replan_preserves_invocation_wide_peer_old_tokens() -> None:
    host_e = _make_recovery()
    host_a_old = _report("host_a-old", SHA_A)
    host_a_token = _token(host_a_old, "8" * 64)
    invocation_vector = transition.MutationVector(
        host_e.latest_report,
        tuple(
            sorted(
                (*host_e.mutation_vector.journals, host_a_token),
                key=lambda item: item.rollout_id,
            )
        ),
    )
    with_peer = replace(host_e, mutation_vector=invocation_vector)
    first = transition.plan_host_transition(with_peer)
    assert len(first.effects) == 1
    first_effect = first.effects[0]
    assert isinstance(first_effect, transition.WriteCurrentAckAndIntent)
    assert first_effect.expected_vector == invocation_vector
    assert first_effect.expected_vector.token_for(host_a_old.rollout_id) == host_a_token

    advanced_peer = transition.advance_mutation_vector(
        invocation_vector,
        host_a_token,
        _token(host_a_old, "9" * 64),
    )
    replanned = transition.plan_host_transition(replace(host_e, mutation_vector=advanced_peer))
    assert len(replanned.effects) == 1
    replanned_effect = replanned.effects[0]
    assert isinstance(replanned_effect, transition.WriteCurrentAckAndIntent)
    assert replanned_effect.expected_vector == advanced_peer
    assert first_effect.expected_vector != replanned_effect.expected_vector
    with pytest.raises(transition.TransitionError, match="changed"):
        transition.advance_mutation_vector(
            advanced_peer,
            host_a_token,
            _token(host_a_old, "7" * 64),
        )


def test_mutation_vector_is_explicitly_preauthenticated_adapter_capability() -> None:
    assert "preauthenticated" in cast(str, transition.MutationVector.__doc__).lower()
    recovery = _make_recovery()
    peer = _token(_report("adapter-authenticated-peer", SHA_A), "8" * 64)
    supplied = replace(
        recovery,
        mutation_vector=transition.MutationVector(
            recovery.latest_report,
            tuple(
                sorted(
                    (*recovery.mutation_vector.journals, peer),
                    key=lambda item: item.rollout_id,
                )
            ),
        ),
    )
    effect = transition.plan_host_transition(supplied).effects[0]
    assert isinstance(effect, transition.WriteCurrentAckAndIntent)
    assert effect.expected_vector == supplied.mutation_vector
    advanced = transition.advance_mutation_vector(
        supplied.mutation_vector,
        peer,
        _token(_report("adapter-authenticated-peer", SHA_A), "9" * 64),
    )
    with pytest.raises(transition.TransitionError, match="changed"):
        transition.advance_mutation_vector(
            advanced,
            peer,
            _token(_report("adapter-authenticated-peer", SHA_A), "7" * 64),
        )


def test_transition_module_has_no_live_effect_or_runtime_fleet_import() -> None:
    module_path = Path(transition.__file__)
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    assert not {"subprocess", "os", "fleet_rollout", "fleet_release", "fleet_operation"} & imports
    effect_fields = {
        name
        for effect_type in (
            transition.WriteCurrentAckAndIntent,
            transition.WriteOldBacklink,
            transition.ConsumeMember,
            transition.ClearForwardIntent,
        )
        for name in effect_type.__dataclass_fields__
    }
    assert not {"runner", "launch", "deploy", "control", "retry_authorized_true"} & effect_fields


def test_effect_builders_are_planner_private_and_not_provenance_open_api() -> None:
    assert not hasattr(transition, "build_current_ack_effect")
    assert not hasattr(transition, "build_old_backlink_effect")
    assert not hasattr(transition, "build_consume_effect")
    assert not hasattr(transition, "build_clear_intent_effect")


def test_hash_is_canonical_and_rejects_non_json_values() -> None:
    assert transition.canonical_json_sha256({"b": 2, "a": 1}) == (
        transition.canonical_json_sha256({"a": 1, "b": 2})
    )
    with pytest.raises(transition.TransitionError):
        transition.canonical_json_sha256({"bad": object()})
