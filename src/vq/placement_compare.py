"""What ``[broker] policy = "completion-time"`` would change, on recorded jobs (#160).

``vq placement compare FLEET`` replays this host's recent finished local
jobs through both broker orderings over one set of capacity snapshots built
from FLEET's daemon targets: B2 (:func:`vq.overview.place_broker_job`), as
the broker calls it, and the broker's own completion-time re-ranker
(:func:`vq.broker_completion.rank_admitted`) applied to that decision. It
reads FLEET and this host's queue; it places nothing and contacts no host.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from vq import overview, placement_policy
from vq.broker_capacity import UNLIMITED_JOBS
from vq.broker_completion import rank_admitted
from vq.broker_store import BrokerIntent, BrokerStoreError

COMPARE_SCHEMA = "vq.placement-compare/1"


def snapshots_from_fleet(fleet: placement_policy.Fleet) -> list[overview.BrokerCapacitySnapshot]:
    """One broker capacity snapshot per daemon target, as its record states it."""
    snapshots = []
    for target in fleet.targets:
        if target.kind != "daemon":
            continue
        free = target.free or placement_policy.FreeCapacity()  # absent: unknown
        snapshots.append(overview.BrokerCapacitySnapshot(
            host=target.name,
            observed_at=placement_policy._parse_time(target.observed_at, "observed_at")
            .timestamp(),
            free_cpus=free.cpus,
            free_mem_mb=free.mem_mb,
            free_jobs=UNLIMITED_JOBS,
            # B2 refuses an unreachable host before it asks about health.
            reachable=target.reachable,
            daemon_healthy=True,
            state_root_agreement=True,
            admission_open=target.reachable and not target.maintenance,
            queue_ready=True,
            max_wall_time_seconds=target.max_walltime_seconds,
        ))
    return snapshots


def _workloads(specs: list[Any], limit: int) -> tuple[list[tuple[str, BrokerIntent]],
                                                      dict[str, int]]:
    finished = sorted((spec for spec in specs
                       if spec.is_terminal and spec.scheduler_target is None),
                      key=lambda spec: (spec.submitted_at, spec.id), reverse=True)[:limit]
    skipped: dict[str, int] = {}
    workloads = []
    for spec in finished:
        if spec.mem_mb is None:
            skipped["no declared mem_mb"] = skipped.get("no declared mem_mb", 0) + 1
            continue
        try:
            intent = BrokerIntent(command=tuple(spec.command) or ("true",), cpus=spec.cpus,
                                  mem_mb=spec.mem_mb, wall_time_seconds=spec.wall_time_seconds,
                                  program=spec.program)
        except BrokerStoreError:
            skipped["not a broker workload"] = skipped.get("not a broker workload", 0) + 1
            continue
        workloads.append((spec.id, intent))
    return workloads, skipped


def compare(fleet: placement_policy.Fleet, specs: list[Any], *, now: datetime,
            max_age_seconds: float, limit: int) -> dict[str, Any]:
    """Both orderings for each recorded workload, and how often they agree."""
    snapshots = snapshots_from_fleet(fleet)
    if not snapshots:
        raise placement_policy.PlacementError("the fleet has no daemon target to compare on")
    workloads, skipped = _workloads(specs, limit)
    clock = now.timestamp()
    rows = []
    for job_id, intent in workloads:
        decision = overview.place_broker_job(snapshots, intent.placement_request(),
                                             now=clock, max_age_seconds=max_age_seconds)
        ranked = (rank_admitted(fleet, intent, decision, snapshots, clock, max_age_seconds)
                  if decision.host is not None else decision)
        rows.append({
            "job_id": job_id, "cpus": intent.cpus, "mem_mb": intent.mem_mb,
            "wall_time_seconds": intent.wall_time_seconds, "program": intent.program,
            "capacity": decision.host, "completion_time": ranked.host,
            "same": decision.host == ranked.host,
            "capacity_blockers": dict(decision.blockers),
            "completion_time_blockers": dict(ranked.blockers),
        })
    return {
        "schema": COMPARE_SCHEMA, "now": now.isoformat(), "max_age_seconds": max_age_seconds,
        "jobs": rows,
        "totals": {
            "compared": len(rows),
            "same": sum(row["same"] for row in rows),
            "different": sum(not row["same"] for row in rows),
            "held_by_capacity": sum(row["capacity"] is None for row in rows),
            "held_by_completion_time": sum(row["completion_time"] is None for row in rows),
            "skipped": dict(sorted(skipped.items())),
        },
    }


def compare_text(report: dict[str, Any]) -> str:
    rows = [("JOB", "CPUS", "MEM MB", "CAPACITY", "COMPLETION-TIME", "")]
    for row in report["jobs"]:
        rows.append((row["job_id"], str(row["cpus"]), str(row["mem_mb"]),
                     row["capacity"] or "held", row["completion_time"] or "held",
                     "" if row["same"] else "differs"))
    widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
    lines = ["  ".join(r[i].ljust(widths[i]) for i in range(len(r))).rstrip() for r in rows]
    totals = report["totals"]
    lines += ["", f"{totals['compared']} compared: {totals['same']} same, "
                  f"{totals['different']} different; held by capacity "
                  f"{totals['held_by_capacity']}, by completion-time "
                  f"{totals['held_by_completion_time']}"]
    for reason, count in totals["skipped"].items():
        lines.append(f"skipped {count}: {reason}")
    return "\n".join(lines) + "\n"
