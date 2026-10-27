"""The broker role on the broker host (#64, B6): accept, pass, views.

Everything here refuses unless the local config has ``[broker] enabled =
true``. ``run_pass`` is one placement pass end to end: it collects capacity
evidence from the configured targets (``vq broker capacity`` over SSH), then
runs :meth:`vq.broker_placement.BrokerPlacer.run_once` with the keyed remote
submit. The broker's daemon starts it every ``[broker] interval_seconds``;
``vq broker pass`` runs one by hand.
"""

from __future__ import annotations

import dataclasses
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from vq import broker_capacity, broker_placement, broker_store
from vq.broker_completion import completion_time_reranker


class BrokerDisabled(broker_store.BrokerStoreError):
    """This host is not configured as a broker."""


def require_enabled(cfg: Any) -> None:
    from vq import config

    if not cfg.broker.enabled:
        raise BrokerDisabled(
            "this host is not a broker: set [broker] enabled = true and its targets"
        )
    if cfg.multi_user.enabled or config.system_multi_user_enabled():
        raise BrokerDisabled(
            "a multi-user host cannot be a broker in this contract: the role "
            "runs as the operator account"
        )


def _check_placeable(cfg: Any, intent: broker_store.BrokerIntent) -> None:
    """Refuse at accept a job no configured target could ever take.

    Under strict FIFO such a job would hold every later job forever.
    """
    if intent.pool is None:
        return
    pool = cfg.pools.get(intent.pool)
    if pool is None:
        raise broker_store.BrokerStoreError(f"unknown pool {intent.pool!r}")
    if not set(pool.hosts) & set(cfg.broker.targets):
        raise broker_store.BrokerStoreError(
            f"pool {intent.pool!r} contains no [broker] target"
        )


def accept(
    cfg: Any,
    payload: Path,
    intent: broker_store.BrokerIntent,
    *,
    root: Path | None = None,
    clock: Callable[[], float] = time.time,
    idempotency_key: str | None = None,
) -> broker_store.BrokerRecord:
    """Durably take one relayed submission into the held queue.

    With ``idempotency_key``, a repeat of the same submission returns the
    job the key already names, and different content under it is refused.
    """
    require_enabled(cfg)
    _check_placeable(cfg, intent)
    with broker_store.BrokerStore.open(root) as store:
        return store.accept(payload, intent, now=clock(), idempotency_key=idempotency_key)


def cancel(
    cfg: Any,
    broker_job_id: str,
    *,
    reason: str,
    root: Path | None = None,
    clock: Callable[[], float] = time.time,
) -> broker_store.BrokerRecord:
    """Record a cancellation; a held job is cancelled at once.

    A transfer in doubt becomes ``cancel-pending`` for an operator; a placed
    job keeps the cancellation until its target job is killed.
    """
    require_enabled(cfg)
    with broker_store.BrokerStore.open(root) as store:
        for _ in range(3):
            current = store.get(broker_job_id)
            if current.cancellation is not None or current.state == broker_store.STATE_CANCELLED:
                return current
            try:
                return store.request_cancel(
                    broker_job_id, current.revision, reason=reason, now=clock()
                )
            except broker_store.BrokerRecordConflict:
                continue  # a pass moved it on; cancel what it is now
        raise broker_store.BrokerStoreBusy(
            f"broker job {broker_job_id} kept changing; retry the cancellation"
        )


FENCE_SCHEMA = "vq.idempotency-fence/1"


def _fence_on_target(cfg: Any, attempt: broker_store.BrokerAttempt) -> dict[str, Any]:
    """Run the target's ``fence-idempotency-key`` for one attempt; strict answer."""
    import json

    from vq import broker_target

    host_cfg = broker_target.bound_route(cfg, attempt)
    proc = broker_target.run(
        host_cfg, "admin", "fence-idempotency-key", attempt.idempotency_key,
        timeout=120, owned_process_group=True,
        max_stdout_bytes=64 * 1024, max_stderr_bytes=64 * 1024,
    )
    try:
        answer = json.loads(proc.stdout or "")
    except ValueError as exc:
        raise broker_store.BrokerStoreError(
            f"{attempt.target_host} gave no readable fence answer"
        ) from exc
    if (
        not isinstance(answer, dict)
        or set(answer) != {"schema", "state", "job_id", "state_root"}
        or answer.get("schema") != FENCE_SCHEMA
        or answer.get("state") not in {"accepted", "fenced"}
    ):
        raise broker_store.BrokerStoreError(
            f"{attempt.target_host} gave an unrecognised fence answer"
        )
    if answer["state_root"] != attempt.target_state_root:
        # Idempotency is scoped to one state authority: a fence elsewhere
        # proves nothing about the store the request was sent to.
        raise broker_store.BrokerStoreError(
            f"{attempt.target_host} now answers for state root "
            f"{answer['state_root']!r}, not the recorded {attempt.target_state_root!r}; "
            "nothing was changed"
        )
    return answer


def reconcile(
    cfg: Any,
    broker_job_id: str,
    *,
    cancel: bool = False,
    root: Path | None = None,
    clock: Callable[[], float] = time.time,
) -> broker_store.BrokerRecord:
    """Settle a transfer in doubt from its target's own fence (#64).

    Under the pass lock, so no replay runs meanwhile, the attempt's target
    either proves the keyed submission was accepted (the job becomes
    ``placed`` with that target job) or fences the key so no copy of the
    request can ever be accepted there (the attempt returns, and the job
    holds again, or is cancelled with ``cancel`` or an earlier
    cancellation). The fence must come from the attempt's recorded state
    root. Never sends the job anywhere.
    """
    from vq.spec import validate_job_id

    require_enabled(cfg)
    with broker_store.BrokerStore.open(root) as store, store.pass_lock():
        record = store.get(broker_job_id)
        if record.state != broker_store.STATE_TRANSFERRING:
            raise broker_store.BrokerStoreError(
                f"{broker_job_id} is {record.state}; only a transfer in doubt "
                "is reconciled"
            )
        attempt = record.current_attempt
        assert attempt is not None and attempt.request_fingerprint is not None
        answer = _fence_on_target(cfg, attempt)
        if answer["state"] == "accepted":
            try:
                target_job_id = validate_job_id(str(answer["job_id"]))
            except ValueError as exc:
                raise broker_store.BrokerStoreError(
                    f"{attempt.target_host} named an invalid job: {exc}"
                ) from exc
            return store.record_placed(
                broker_job_id, record.revision,
                request_fingerprint=attempt.request_fingerprint,
                target_job_id=target_job_id, now=clock(),
            )
        record = store.fence_transfer(
            broker_job_id, record.revision,
            proof=(
                f"{attempt.target_host} fenced key {attempt.idempotency_key} "
                f"in {attempt.target_state_root}"
            ),
            now=clock(),
        )
        if cancel and record.cancellation is None:
            record = store.request_cancel(
                broker_job_id, record.revision,
                reason="cancelled after its transfer was fenced", now=clock(),
            )
        return store.resume_holding(broker_job_id, record.revision, now=clock())


def run_pass(
    cfg: Any,
    *,
    root: Path | None = None,
    clock: Callable[[], float] = time.time,
    collect: Callable[..., dict[str, broker_capacity.TargetEvidence]] | None = None,
    submit: broker_placement.Submitter | None = None,
) -> dict[str, Any]:
    """Collect evidence from every target, then run one strict-FIFO pass."""
    require_enabled(cfg)
    settings = cfg.broker
    collect = collect or broker_capacity.collect
    with broker_store.BrokerStore.open(root) as store, store.pass_lock():
        # The pass lock is taken first, so a second pass stops before its SSH.
        records, _problems = store.scan()
        # Reserved and returned jobs go back to held within this pass.
        queued_states = {
            broker_store.STATE_HELD, broker_store.STATE_RESERVED, broker_store.STATE_RETURNED,
        }
        programs = sorted({
            record.intent.program for record in records
            if record.state in queued_states and record.intent.program
        })
        started_at = clock()
        evidence = collect(
            cfg, list(settings.targets), clock=clock,
            timeout=settings.collect_timeout_seconds, programs=programs,
        )
        targets = set(settings.targets)
        base_refusal = broker_placement.remote_target_refusal(cfg)
        roots: dict[str, list[str]] = {}
        for host, found in evidence.items():
            if host in targets and found.state_root is not None:
                authority = found.snapshot.authority
                lane = (":" + host) if authority and authority.get("scheduler") != "local" else ""
                roots.setdefault(found.state_root + lane, []).append(host)
        shared = {
            host: sorted(hosts) for hosts in roots.values() if len(hosts) > 1 for host in hosts
        }

        def refuse_target(host: str) -> str | None:
            if host not in targets:
                return f"{host} is not a [broker] target"
            if host in shared:
                # Two names for one state authority: one machine's capacity
                # would be promised twice. Neither is used until fixed.
                return f"targets {', '.join(shared[host])} share one state root"
            return base_refusal(host)

        def state_root_of(host: str) -> str | None:
            found = evidence.get(host)
            return found.state_root if found is not None else None

        placer = broker_placement.BrokerPlacer(
            store,
            submit=submit or broker_placement.remote_submitter(cfg),
            state_root_of=state_root_of,
            refuse_target=refuse_target,
            job_refusal=broker_capacity.program_refusal(evidence),
            clock=clock,
            rerank=(
                completion_time_reranker(Path(settings.placement_fleet))
                if settings.policy == "completion-time" and settings.placement_fleet
                else None
            ),
        )
        report = placer.run_once_locked(
            [evidence[host].snapshot for host in settings.targets if host in evidence],
            max_age_seconds=settings.max_age_seconds,
            target_jobs={host: found.jobs for host, found in evidence.items()},
            retry_interval_seconds=settings.retry_interval_seconds,
        )
    return {
        "schema": "vq.broker-pass/1",
        "started_at": started_at,
        "targets": {
            host: {
                "error": found.error,
                "closed": list(found.reasons),
                "state_root": found.state_root,
            }
            for host, found in sorted(evidence.items())
        },
        **dataclasses.asdict(report),
    }


def record_view(record: broker_store.BrokerRecord) -> dict[str, Any]:
    """What an operator or client needs to know about one broker job."""
    attempt = record.current_attempt
    view: dict[str, Any] = {
        "broker_job_id": record.broker_job_id,
        "state": record.state,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
        "program": record.intent.program,
        "job_name": record.intent.job_name,
        "cpus": record.intent.cpus,
        "mem_mb": record.intent.mem_mb,
        "wall_time_seconds": record.intent.wall_time_seconds,
        # #159: the calibrated runtime the completion-time policy plans with.
        "runtime_seconds": (list(record.intent.runtime_seconds)
                            if record.intent.runtime_seconds is not None else None),
        "target_host": attempt.target_host if attempt else None,
        "target_job_id": attempt.target_job_id if attempt else None,
        "attempts": len(record.attempts),
        # The hosts the attempt's decision ranked, best first (#160); the
        # target is the first of them the placement walk could use.
        "candidates": (
            [rank[-1] for rank in attempt.ranking
             if isinstance(rank, list) and rank and isinstance(rank[-1], str)]
            if attempt else []
        ),
        "blockers": (
            dict(record.held_blockers) if record.held_blockers
            else dict(attempt.blockers) if attempt else {}
        ),
        "diagnostics": list(attempt.diagnostics) if attempt else [],
        "cancellation": (
            None if record.cancellation is None else {
                "requested_at": record.cancellation.requested_at,
                "reason": record.cancellation.reason,
                "completed": record.cancellation.completed_at is not None,
            }
        ),
    }
    if record.state == broker_store.STATE_TRANSFERRING and record.cancellation is not None:
        view["operator_action"] = (
            f"cancelled while its transfer was uncertain: check {view['target_host']} "
            f"for job key {record.broker_job_id} and kill it there if it exists"
        )
    return view


CONSOLE_FIELDS = (
    "broker_job_id", "state", "target_host", "target_job_id", "cpus", "mem_mb",
    "job_name", "created_at", "updated_at", "blockers", "operator_action", "candidates",
)
"""What the fleet console may show of a broker job (#64 B7, maintainer decision
2026-09-29): metadata only. Never the command, program, payload or free-text
diagnostics."""

_CONSOLE_BLOCKER_CODES = frozenset({
    "scheduler-unsupported", "unreachable", "daemon-not-ready", "state-root-unverified",
    "admission-blocked", "queue-not-ready", "snapshot-invalid", "snapshot-future",
    "snapshot-stale", "pool-mismatch", "capacity-invalid", "wall-time-unknown",
    "wall-time-exceeded", "capacity-unknown", "cpus-insufficient", "memory-insufficient",
    "job-slots-insufficient", "target-refused", "job-refused", "state-root-unknown",
    "completion-time",
})


def _console_blocker(reason: Any) -> str:
    # Refusal details can name a program or contain a remote exception. Keep
    # those in CLI diagnostics; the console exposes only known reason codes.
    code = reason.partition(":")[0] if isinstance(reason, str) else ""
    return code if code in _CONSOLE_BLOCKER_CODES else "placement-blocked"


def console_view(cfg: Any, root: Path | None = None) -> dict[str, Any] | None:
    """The broker's jobs for the fleet console, read-only; None off a broker.

    Never creates broker state: without an existing store it shows no jobs.
    An unreadable store is reported as ``error``, and records that cannot
    be loaded are counted as ``unreadable``, never shown in part.
    """
    from datetime import UTC, datetime

    if not cfg.broker.enabled:
        return None
    store_root = root if root is not None else broker_store.default_broker_root()
    if not store_root.is_dir():
        return {"jobs": [], "unreadable": 0, "error": None}
    try:
        views, problems = list_records(store_root)
    except (broker_store.BrokerStoreError, OSError) as exc:
        return {"jobs": [], "unreadable": 0, "error": str(exc)}

    def iso(value: Any) -> str | None:
        if not isinstance(value, (int, float)):
            return None
        return datetime.fromtimestamp(value, UTC).isoformat(timespec="seconds")

    jobs = []
    for view in views:
        row = {name: view.get(name) for name in CONSOLE_FIELDS}
        row["blockers"] = {
            host: _console_blocker(reason) for host, reason in view["blockers"].items()
        }
        row["created_at"] = iso(row["created_at"])
        row["updated_at"] = iso(row["updated_at"])
        jobs.append(row)
    return {"jobs": jobs, "unreadable": len(problems), "error": None}


def list_records(root: Path | None = None) -> tuple[list[dict[str, Any]], dict[str, str]]:
    with broker_store.BrokerStore.open(root) as store:
        records, problems = store.scan()
    records.sort(key=lambda record: (record.created_at, record.broker_job_id))
    return [record_view(record) for record in records], problems


def show_record(broker_job_id: str, root: Path | None = None) -> dict[str, Any]:
    with broker_store.BrokerStore.open(root) as store:
        return record_view(store.get(broker_job_id))
