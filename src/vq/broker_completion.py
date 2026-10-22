"""The opt-in completion-time broker policy (#160): re-rank what B2 admits.

With ``[broker] policy = "completion-time"`` each placement still starts
from B2 (:func:`vq.overview.place_broker_job`) over the debited snapshots: a
host is a candidate only with open admission, a ready queue and the job's
CPUs, memory and walltime fitting now. This policy never adds a host. It
orders the admitted hosts by the completion-time planner
(:mod:`vq.placement_policy`) over ``[broker] placement_fleet``, the
operator's record of each daemon target, with this pass's evidence laid
over it, and refuses those the planner's hard requirements reject. A host
missing from that record, or an unreadable record, is a blocker: the policy
never falls back silently to the capacity ranking.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from vq import overview, placement_policy
from vq.broker_store import BrokerIntent, BrokerRecord

Reranker = Callable[
    [BrokerRecord, overview.BrokerPlacementDecision,
     Sequence[overview.BrokerCapacitySnapshot], float, float],
    overview.BrokerPlacementDecision,
]
"""``(record, decision, debited snapshots, now, max_age_seconds)`` to a decision."""

#: The planner's program for a job that names none: every target can run a
#: plain command, so each is given this stand-in runtime.
COMMAND_PROGRAM = "vq-command"
_COMMAND_RUNTIME = "0" * 64
_PREFIX = "completion-time: "


def _held(decision: overview.BrokerPlacementDecision,
          blockers: list[tuple[str, str]]) -> overview.BrokerPlacementDecision:
    return replace(decision, host=None, ranking=(), blockers=tuple(sorted(blockers)))


def completion_time_reranker(fleet_path: Path) -> Reranker:
    """The re-ranker for the placement fleet at ``fleet_path``, read every pass."""

    def rerank(
        record: BrokerRecord,
        decision: overview.BrokerPlacementDecision,
        snapshots: Sequence[overview.BrokerCapacitySnapshot],
        now: float,
        max_age_seconds: float,
    ) -> overview.BrokerPlacementDecision:
        try:
            base = placement_policy.load_fleet(fleet_path)
        except ValueError as exc:
            reason = " ".join(str(exc).split())[:300]
            return _held(decision, [*decision.blockers, *(
                (rank[-1], f"{_PREFIX}placement fleet unusable: {reason}")
                for rank in decision.ranking)])
        return rank_admitted(base, record.intent, decision, snapshots, now, max_age_seconds)

    return rerank


def rank_admitted(
    base: placement_policy.Fleet,
    intent: BrokerIntent,
    decision: overview.BrokerPlacementDecision,
    snapshots: Sequence[overview.BrokerCapacitySnapshot],
    now: float,
    max_age_seconds: float,
) -> overview.BrokerPlacementDecision:
    """``decision``'s admitted hosts ordered, or refused, by the planner over ``base``."""
    admitted = [rank[-1] for rank in decision.ranking]
    blockers = list(decision.blockers)
    records = {target.name: target for target in base.targets}
    evidence = {snapshot.host: snapshot for snapshot in snapshots}
    targets: list[dict[str, Any]] = []
    for host in admitted:
        target = records.get(host)
        if target is None or target.kind != "daemon":
            blockers.append((host, f"{_PREFIX}not a daemon target of the placement fleet"))
            continue
        snapshot = evidence[host]
        data = target.model_dump(mode="json")
        data["observed_at"] = datetime.fromtimestamp(snapshot.observed_at, UTC).isoformat()
        data["reachable"] = True
        data["free"] = {**(data.get("free") or {}),
                        "cpus": snapshot.free_cpus, "mem_mb": snapshot.free_mem_mb}
        if intent.program is None:
            data["programs"] = {**data["programs"], COMMAND_PROGRAM: [_COMMAND_RUNTIME]}
        targets.append(data)
    try:
        # No target left fails the fleet's own validation: held, reasons kept.
        fleet = placement_policy.Fleet.model_validate(
            {"schema_name": placement_policy.FLEET_SCHEMA, "targets": targets})
        request = placement_policy.Request.model_validate({
            "schema_name": placement_policy.REQUEST_SCHEMA,
            "program": intent.program or COMMAND_PROGRAM,
            "cpus": intent.cpus, "mem_mb_per_node": intent.mem_mb,
            **({"wall_time_seconds": intent.wall_time_seconds}
               if intent.wall_time_seconds is not None else {}),
        })
        explanation = placement_policy.explain(
            fleet, request, now=datetime.fromtimestamp(now, UTC),
            max_age_seconds=max_age_seconds)
    except ValueError as exc:
        reason = " ".join(str(exc).split())[:300]
        return _held(decision, [*blockers, *(
            (target["name"], f"{_PREFIX}{reason}") for target in targets)])
    for rejected in explanation["rejected"]:  # type: ignore[attr-defined]
        blockers.append((rejected["target"], _PREFIX + "; ".join(rejected["reasons"])))
    order = [candidate["target"]
             for candidate in explanation["candidates"]]  # type: ignore[attr-defined]
    ranks = {rank[-1]: rank for rank in decision.ranking}
    return replace(decision, host=order[0] if order else None,
                   ranking=tuple(ranks[host] for host in order),
                   blockers=tuple(sorted(blockers)))
