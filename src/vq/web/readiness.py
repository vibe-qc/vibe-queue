"""Execution readiness for the fleet view (#168).

Answers "can calculations run now, and where?" from the snapshot the
console already holds, without another probe. It keeps apart what the
host cards and their tiles blur together:

* configured targets (config keys) versus execution resources: two keys
  for one machine, and scheduler lanes logging in to one cluster, are one
  resource each;
* daemon health versus dispatch versus program runtime state: a
  replacement build in progress, or one that failed, leaves the current
  runtime eligible, and mixed vq versions say nothing about either;
* observed versus unknown: an unreachable or unread target is unknown and
  makes the totals partial. It is never reported healthy, blocked or idle.

Dispatch follows the daemon's own gates as far as the snapshot can prove
them: an operator's ``vq host down``, a drain, and an admin-update lease
whose scope holds the target. Anything else the daemon checks at dispatch
time (quotas, memory pressure, per-job runtime pins) is not visible here,
so ``open`` means "nothing in the snapshot holds it", not a promise that a
given job will start.

Pure: no network, no clock.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

from vq import admin
from vq.overview import AdminMarkerSnapshot, _admin_marker_snapshots
from vq.web import calculations

if TYPE_CHECKING:
    from vq.web.fleet import FleetSnapshot, HostSnapshot

Row = dict[str, object]

DAEMON_OK = "ok"
DAEMON_DEGRADED = "degraded"
DAEMON_UNKNOWN = "unknown"

DISPATCH_OPEN = "open"
DISPATCH_HELD = "held"
DISPATCH_UNKNOWN = "unknown"

PROGRAM_ELIGIBLE = "eligible"
PROGRAM_HELD = "held"
PROGRAM_REFUSED = "refused"
PROGRAM_UNKNOWN = "unknown"

#: What an admin-update lease means for the program it names. ``building``
#: is an update in flight; on a lease that does not hold dispatch it is a
#: replacement being built beside the current runtime.
UPDATE_BUILDING = "building"
UPDATE_FAILED = "update failed"
UPDATE_RECOVERY = "needs recovery"

_LEASE_STATES = {
    admin.ADMIN_UPDATE_MARKER_DIAG_RUNNING: UPDATE_BUILDING,
    admin.ADMIN_UPDATE_MARKER_DIAG_FAILED: UPDATE_FAILED,
    admin.ADMIN_UPDATE_MARKER_DIAG_STALE: UPDATE_RECOVERY,
}

#: The program label for a lease on a lane's own vq helper.
HELPER_PROGRAM = "vq helper"
#: The program label for a job that names no registered program.
COMMAND_PROGRAM = "(command)"


@dataclass
class _Lease:
    """One admin-update lease, read relative to one dispatch target."""

    snapshot: AdminMarkerSnapshot
    programs: list[str]
    holds: bool
    state: str | None


@dataclass
class _Target:
    host: str
    kind: str
    resource: str | None
    daemon: str
    dispatch: str
    reasons: list[str] = field(default_factory=list)
    leases: list[_Lease] = field(default_factory=list)
    runtimes: dict[str, str | None] = field(default_factory=dict)
    runtime_errors: dict[str, str] = field(default_factory=dict)


def _is_lane(host: HostSnapshot) -> bool:
    return (
        host.driver is not None
        or host.cluster is not None
        or host.overview.is_scheduler_host
    )


def _lease_programs(
    snapshot: AdminMarkerSnapshot, *, lane: str | None,
) -> list[str] | None:
    """Programs a lease names for one target, or None if it names another.

    ``lane`` is the scheduler lane's key, None for a daemon host's own
    local execution. Scheduler leases name their lane; a plain env or
    ``runtime-slot:`` lease is the daemon host's own.
    """
    marker = snapshot.marker
    names: list[str] = []
    relevant = False
    for env in marker.envs if marker is not None else []:
        parts = env.split(":")
        if parts[0] == "scheduler-runtime" and len(parts) == 3:
            if parts[1] == lane:
                relevant = True
                names.append(parts[2])
        elif parts[0] == "scheduler" and len(parts) == 2:
            if parts[1] == lane:
                relevant = True
                names.append(HELPER_PROGRAM)
        elif parts[0] == "runtime-slot" and len(parts) == 2:
            if lane is None:
                relevant = True
                names.append(parts[1])
        elif len(parts) == 1 and lane is None:
            relevant = True
            names.append(env)
    return names if relevant else None


def _read_lease(snapshot: AdminMarkerSnapshot, *, lane: str | None) -> _Lease | None:
    """How one lease bears on one target, or None when it does not.

    Mirrors the daemon: the lease's scope decides what it holds, an
    unreadable or unrecognised lease holds everything, and a stale ordinary
    lease holds nothing because the daemon reaps it.
    """
    marker = snapshot.marker
    try:
        scope = admin.admin_update_marker_scope(marker)
    except Exception:  # pragma: no cover - malformed remote marker
        scope = None
    programs = _lease_programs(snapshot, lane=lane)
    if programs is None and scope is not None:
        return None
    reaped = (
        marker is not None
        and snapshot.stale_reason is not None
        and marker.managed_transaction is None
        and not marker.owns_pause_scope
    )
    token = lane if lane is not None else admin.LOCAL_DISPATCH_SCOPE
    return _Lease(
        snapshot=snapshot,
        programs=programs or [],
        holds=not reaped and (scope is None or token in scope),
        state=_LEASE_STATES.get(snapshot.status or ""),
    )


def _lease_reason(lease: _Lease) -> str:
    what = ", ".join(lease.programs) or "an unreadable or unrecognised lease"
    summary = lease.snapshot.summary or "admin update in progress"
    return f"admin update holds dispatch ({what}): {summary}"


def _target(host: HostSnapshot, resource: str | None) -> _Target:
    ov = host.overview
    lane = _is_lane(host)
    kind = "scheduler" if lane else "daemon"
    if ov.admin_down:
        return _Target(
            host.host, kind, resource, DAEMON_UNKNOWN, DISPATCH_HELD,
            [f"administratively down: {ov.admin_down}"],
        )
    if not ov.reachable:
        return _Target(
            host.host, kind, resource, DAEMON_UNKNOWN, DISPATCH_UNKNOWN,
            [f"not observed: {ov.error or 'unreachable'}"],
        )
    health = ov.daemon_health
    daemon = (
        DAEMON_UNKNOWN if health is None
        else DAEMON_OK if getattr(health, "ok", False) else DAEMON_DEGRADED
    )
    target = _Target(host.host, kind, resource, daemon, DISPATCH_OPEN)
    leases = host.driver_leases if lane else _admin_marker_snapshots(ov)
    if lane and not leases:
        leases = _admin_marker_snapshots(ov)
    for snapshot in leases:
        lease = _read_lease(snapshot, lane=host.host if lane else None)
        if lease is not None:
            target.leases.append(lease)
    drain = ov.drain_state
    if drain is not None and drain.enabled and (
        drain.is_full_drain or (lane and drain.drains_scheduler_target(host.host))
    ):
        reason = f" ({drain.reason})" if drain.reason else ""
        target.reasons.append(f"drained{reason}")
    target.reasons.extend(_lease_reason(lease) for lease in target.leases if lease.holds)
    if target.reasons:
        target.dispatch = DISPATCH_HELD
    elif daemon == DAEMON_UNKNOWN:
        target.dispatch = DISPATCH_UNKNOWN
        target.reasons.append("daemon health not reported")
    elif daemon == DAEMON_DEGRADED:
        target.dispatch = DISPATCH_UNKNOWN
        findings = [
            str(f) for f in getattr(health, "findings", []) or []
            if str(f).startswith("FAIL")
        ]
        target.reasons.append(
            "daemon degraded" + (f": {findings[0]}" if findings else "")
        )
    if not lane:
        for env in ov.envs or []:
            name = getattr(env, "name", None)
            if not isinstance(name, str) or not name:
                continue
            error = getattr(env, "error", None)
            if isinstance(error, str) and error:
                target.runtime_errors[name] = error
            target.runtimes[name] = (
                getattr(env, "current_version", None)
                or getattr(env, "current_describe", None)
            )
    return target


def _resources(hosts: list[HostSnapshot]) -> dict[str, str | None]:
    """Each target's execution resource, None where it cannot be told.

    A daemon host is the machine that answered, by the name it reports for
    itself; one that did not answer may be a second key for a machine that
    did, so it stays unidentified. A scheduler lane is its cluster.
    """
    resources: dict[str, str | None] = {}
    for host in hosts:
        if _is_lane(host):
            resources[host.host] = f"cluster:{host.cluster or host.host}"
        elif host.overview.reachable and not host.overview.admin_down:
            name = host.overview.reported_hostname or host.host
            resources[host.host] = f"machine:{name}"
        else:
            resources[host.host] = None
    return resources


def _daemon_identity(
    owner: str, by_host: dict[str, HostSnapshot], resources: dict[str, str | None],
) -> str:
    resource = resources.get(owner)
    if resource is not None and owner in by_host and not _is_lane(by_host[owner]):
        return resource
    return f"key:{owner}"


def _refusal(row: Row) -> tuple[str, str, datetime | None] | None:
    """``(kind, reason, at)`` when the row records the queue refusing it.

    Three shapes, all already written by the daemon: a pending job whose
    request exceeds the target's configured caps; a job failed at dispatch
    before it ever started (exit code -1, the daemon's dispatch-refusal
    signature, which a command failure or a dependency cascade never
    carries); and a submission the scheduler rejected before assigning an
    ID.
    """
    state = row.get("state")
    if state == "pending" and row.get("pending_over_capacity") is True:
        overages = row.get("configured_capacity_overages")
        parts = [
            f"{o.get('resource')} {o.get('requested')} > limit {o.get('limit')}"
            for o in overages if isinstance(o, dict)
        ] if isinstance(overages, list) else []
        reason = "request exceeds the target's configured capacity"
        if parts:
            reason += ": " + ", ".join(parts)
        return "over capacity", reason, calculations._ts(row.get("submitted_at"))
    if state != "failed":
        return None
    failure = row.get("failure_reason")
    detail = failure if isinstance(failure, str) and failure else None
    diagnosis = row.get("terminal_diagnosis")
    if isinstance(diagnosis, dict) and diagnosis.get("category") == "scheduler_submit_failed":
        return (
            "scheduler rejected",
            detail or str(diagnosis.get("summary") or "scheduler rejected the submission"),
            calculations._ts(row.get("finished_at")),
        )
    if row.get("started_at") is None and row.get("exit_code") == -1 and detail:
        return "dispatch refused", detail, calculations._ts(row.get("finished_at"))
    return None


def _program(row: Row) -> str:
    program = row.get("program")
    return program if isinstance(program, str) and program else COMMAND_PROGRAM


def _admission_failures(rows: list[Row]) -> list[dict[str, object]]:
    """The latest refusal per target and program, unless a later job got in.

    A refusal followed by an admitted dispatch of the same program on the
    same target is history, not a readiness problem. A pending over-capacity
    job is current by definition.
    """
    latest: dict[tuple[str, str], tuple[Row, str, str, datetime | None]] = {}
    admitted: dict[tuple[str, str], datetime] = {}
    for row in rows:
        target = row.get("queue_host")
        if not isinstance(target, str) or not target:
            continue
        key = (target, _program(row))
        refusal = _refusal(row)
        if refusal is None:
            started = calculations._ts(row.get("started_at"))
            if started is not None and (key not in admitted or started > admitted[key]):
                admitted[key] = started
            continue
        kind, reason, at = refusal
        previous = latest.get(key)
        if previous is None or (
            at is not None and (previous[3] is None or at >= previous[3])
        ):
            latest[key] = (row, kind, reason, at)
    failures = []
    for key, (row, kind, reason, at) in sorted(latest.items()):
        later = admitted.get(key)
        if kind != "over capacity" and at is not None and later is not None and later > at:
            continue
        failures.append({
            "target": key[0],
            "program": key[1],
            "job": row.get("id"),
            "kind": kind,
            "reason": reason,
            "at": at.isoformat() if at is not None else None,
        })
    return failures


def _executions(
    rows: list[Row], by_host: dict[str, HostSnapshot], resources: dict[str, str | None],
) -> dict[str, object]:
    """Live calculations counted once per owning daemon and job ID.

    Job IDs are unique per daemon. Two config keys for one machine list the
    same jobs twice; folding on the daemon's identity counts them once.
    """
    seen: set[tuple[str, str]] = set()
    counts = dict.fromkeys(("running", "queued", "held", "unconfirmed"), 0)
    allocated = 0
    folded = 0
    for row in rows:
        owner = row.get("queue_owner") or row.get("queue_host")
        jobid = row.get("id")
        if not isinstance(owner, str) or not isinstance(jobid, str):
            continue
        key = (_daemon_identity(owner, by_host, resources), jobid)
        if key in seen:
            folded += 1
            continue
        seen.add(key)
        kind = calculations.activity(row)
        if kind not in counts:
            continue
        counts[kind] += 1
        if kind == "running":
            allocated += calculations._positive_int(row.get("cpus")) or 0
    return {"counts": counts, "allocated_cpus": allocated, "folded_rows": folded}


def _program_view(
    targets: list[_Target], failures: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Per program: where it is eligible, held, refused or unknown.

    A program appears on a target that reports its runtime, holds a lease
    naming it, or refused one of its jobs. Its replacement builds and failed
    updates are listed beside the current runtime's eligibility, never
    instead of it.
    """
    refused = {(str(f["target"]), str(f["program"])): f for f in failures}
    names: set[str] = {program for _, program in refused}
    for target in targets:
        names.update(target.runtimes)
        for lease in target.leases:
            names.update(lease.programs)
    programs: list[dict[str, object]] = []
    for name in sorted(names):
        places: dict[str, list[dict[str, object]]] = {
            PROGRAM_ELIGIBLE: [], PROGRAM_HELD: [], PROGRAM_REFUSED: [], PROGRAM_UNKNOWN: [],
        }
        updates: list[dict[str, object]] = []
        for target in targets:
            failure = refused.get((target.host, name))
            naming = [lease for lease in target.leases if name in lease.programs]
            if name not in target.runtimes and failure is None and not naming:
                continue
            updates.extend(
                {
                    "target": target.host,
                    "state": lease.state,
                    "holds_dispatch": lease.holds,
                    "summary": lease.snapshot.summary,
                    "action": lease.snapshot.action if lease.state != UPDATE_BUILDING else None,
                }
                for lease in naming if lease.state is not None
            )
            place: dict[str, object] = {
                "target": target.host, "runtime": target.runtimes.get(name), "reason": None,
            }
            if target.dispatch == DISPATCH_UNKNOWN:
                status, place["reason"] = PROGRAM_UNKNOWN, "; ".join(target.reasons)
            elif target.dispatch == DISPATCH_HELD:
                status, place["reason"] = PROGRAM_HELD, "; ".join(target.reasons)
            elif failure is not None:
                status, place["reason"] = PROGRAM_REFUSED, failure["reason"]
            elif name in target.runtime_errors:
                status = PROGRAM_UNKNOWN
                place["reason"] = f"runtime not readable: {target.runtime_errors[name]}"
            else:
                status = PROGRAM_ELIGIBLE
            places[status].append(place)
        entry: dict[str, object] = {"program": name, "updates": updates}
        entry.update(places)
        programs.append(entry)
    return programs


def fleet_readiness(snapshot: FleetSnapshot | None) -> dict[str, object] | None:
    """The readiness summary for the Fleet view, or None before a snapshot."""
    if snapshot is None:
        return None
    hosts = snapshot.hosts
    by_host = {host.host: host for host in hosts}
    resources = _resources(hosts)
    targets = [_target(host, resources[host.host]) for host in hosts]

    known = {r for r in resources.values() if r is not None}
    daemons: dict[str, str] = {}
    for host, target in zip(hosts, targets, strict=True):
        owner = host.driver if host.driver is not None else host.host
        if target.kind == "scheduler" and host.driver is None:
            continue
        identity = _daemon_identity(owner, by_host, resources)
        if target.daemon == DAEMON_UNKNOWN and identity in daemons:
            continue
        daemons[identity] = target.daemon

    # Targets whose calculations are missing from the snapshot.
    partial = []
    for host, target in zip(hosts, targets, strict=True):
        if host.overview.admin_down:
            reason = "administratively down, not probed"
        elif not host.overview.reachable:
            reason = target.reasons[0]
        elif host.jobs_error:
            reason = f"job listing failed: {host.jobs_error}"
        else:
            continue
        partial.append({"target": host.host, "reason": reason})

    rows = snapshot.jobs
    failures = _admission_failures(rows)

    def _count(values: list[str], *keys: str) -> dict[str, int]:
        return {key: values.count(key) for key in keys}

    return {
        "targets": {
            "configured": len(hosts),
            "daemon_hosts": sum(t.kind == "daemon" for t in targets),
            "scheduler_lanes": sum(t.kind == "scheduler" for t in targets),
        },
        "resources": {
            "known": len(known),
            "machines": sum(r.startswith("machine:") for r in known),
            "clusters": sum(r.startswith("cluster:") for r in known),
            "unidentified": sum(r is None for r in resources.values()),
        },
        "daemons": _count(list(daemons.values()), DAEMON_OK, DAEMON_DEGRADED, DAEMON_UNKNOWN),
        "dispatch": _count(
            [t.dispatch for t in targets], DISPATCH_OPEN, DISPATCH_HELD, DISPATCH_UNKNOWN,
        ),
        "by_target": {
            t.host: {
                "kind": t.kind,
                "resource": t.resource,
                "daemon": t.daemon,
                "dispatch": t.dispatch,
                "reasons": t.reasons,
            }
            for t in targets
        },
        "programs": _program_view(targets, failures),
        "admission_failures": failures,
        "executions": _executions(rows, by_host, resources),
        "partial": partial,
    }
