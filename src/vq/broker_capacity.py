"""Capacity evidence for broker placement (#64, B5/B6).

B2 (:func:`vq.overview.place_broker_job`) needs *positive* admission evidence
per target: a healthy daemon, a matching state root, open admission, a queue
that would not make the job wait, and effective headroom. No existing remote
command reports that authoritatively, so each target computes it from its own
state with :func:`sample_local`, exposed as the read-only
``vq broker capacity --json``. The broker runs that over SSH
(:func:`collect`) and converts the answer with :func:`evidence_from_payload`.

Every gate is conservative: anything unknown closes admission rather than
opening it. A target running a vq without the command gives no evidence and is
never eligible. The broker stamps ``observed_at`` with **its own** clock at
the start of the SSH call, a lower bound of when the target sampled, so clock
skew between hosts can neither make stale evidence look fresh nor settle a
charge early.
"""

from __future__ import annotations

import json
import math
import re
import time
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from vq import overview

CAPACITY_SCHEMA = "vq.broker-capacity/1"
UNLIMITED_JOBS = 1 << 30
"""Job-slot headroom used for a target with no job cap (B2 needs an int)."""
_ACTIVE_STATES = frozenset({"running", "suspended"})


# --------------------------------------------------------------------------
# Target side


ADMISSION_SNAPSHOT_MAX_AGE_SECONDS = 30.0
"""Oldest daemon admission snapshot accepted as current (same host's clock)."""


def sample_local(
    cfg: Any,
    *,
    now: Callable[[], float] = time.time,
    programs: Sequence[str] | None = None,
    scheduler_target: str | None = None,
) -> dict[str, Any]:
    """This host's admission evidence, read-only, as the broker will judge it.

    The daemon itself is the authority for everything it decides dispatch
    with: its published admission snapshot (``get_admission_snapshot``) gives
    its holds, caps, local usage (orphans and surviving killed jobs included)
    and pending local jobs. This command adds what only it can: positive
    evidence that a running daemon uses this state root, the multi-user
    refusal, live free memory, the status of the named ``programs`` (all
    registered ones when ``None``) and every job id this host records.
    ``admission.reasons`` lists every closed gate; admission is open only
    when it is empty.
    """
    from vq import config, daemon_control, hostmem, rpc
    from vq.listing import list_jobs

    sampled_at = now()
    reasons: list[str] = []
    multi_user = bool(cfg.multi_user.enabled) or config.system_multi_user_enabled()
    if multi_user:
        reasons.append("multi-user host: not a broker target in this contract")

    _code, envelope = daemon_control.local_daemon_ping(
        5.0, multi_user=multi_user, verbose=True
    )
    daemon_ok = envelope.get("ok") is True
    if not daemon_ok:
        reasons.append("the daemon did not answer")
    roots = envelope.get("state_roots")
    client_root = roots.get("client") if isinstance(roots, Mapping) else None
    daemons = roots.get("daemons") if isinstance(roots, Mapping) else None
    agreement = (
        isinstance(client_root, str)
        and client_root.startswith("/")
        and isinstance(daemons, list)
        and any(
            isinstance(entry, Mapping) and entry.get("state_root") == client_root
            for entry in daemons
        )
    )
    if not agreement:
        reasons.append("no running daemon was shown to use this state root")

    snapshot: Mapping[str, Any] | None = None
    try:
        answer = rpc.call("get_admission_snapshot", multi_user=multi_user, timeout=5.0)
    except Exception as exc:  # noqa: BLE001 - an older or absent daemon proves nothing
        reasons.append(f"the daemon gave no admission snapshot: {exc}")
    else:
        snapshot = _valid_admission_snapshot(answer, sampled_at, reasons)
    scheduler = None
    if scheduler_target is not None:
        targets = snapshot.get("scheduler_targets", {}) if snapshot is not None else {}
        scheduler = targets.get(scheduler_target)
        if not isinstance(scheduler, Mapping):
            reasons.append("daemon has no scheduler admission evidence for this target")
            scheduler = None
        else:
            reasons.extend(scheduler["holds"])
            if scheduler.get("can_attempt") is not True:
                reasons.append("scheduler submission pacing or budget holds admission")
    elif snapshot is not None:
        reasons.extend(snapshot["holds"])

    free_cpus = free_mem = free_jobs = None
    local_pending = 0
    if snapshot is not None:
        local_pending = snapshot["local_pending"]
        free_cpus = snapshot["max_cpus"] - snapshot["used_cpus"]
        free_jobs = (
            UNLIMITED_JOBS if snapshot["max_jobs"] is None
            else snapshot["max_jobs"] - snapshot["local_active"]
        )
        bounds = []
        mem_available = hostmem.sample_host_mem().mem_available_mb
        if mem_available is not None:
            bounds.append(mem_available)
        if snapshot["max_mem_mb"] is not None:
            if snapshot["used_mem_mb"] is None:
                bounds = []  # an undeclared job's charge is unknown
            else:
                bounds.append(snapshot["max_mem_mb"] - snapshot["used_mem_mb"])
        free_mem = min(bounds) if bounds else None

    gpus, free_gpus = _gpu_figures(snapshot)
    specs = list_jobs("localhost", multi_user=multi_user)
    local = [spec for spec in specs if spec.scheduler_target is None]
    active = [spec for spec in local if str(spec.state) in _ACTIVE_STATES]
    pending = [spec for spec in local if str(spec.state) == "pending"]

    names = sorted(cfg.programs) if programs is None else sorted(set(programs))
    statuses = {}
    runtime_pins = {}
    for name in names:
        program = cfg.programs.get(name)
        if program is None:
            statuses[name] = "NOT_REGISTERED"
            continue
        try:
            if scheduler_target is not None:
                from vq import submit
                pin = submit._validate_scheduler_target_expected_sha(
                    cfg, scheduler_target, name, None,
                )
                statuses[name] = ("OK" if pin is not None and pin.expected_git_sha
                                  else "RUNTIME_UNVERIFIED")
                if pin is not None and pin.expected_git_sha:
                    runtime_pins[name] = pin.expected_git_sha
            else:
                statuses[name] = program.availability_status().status
        except Exception as exc:  # noqa: BLE001 - one bad program is not all
            statuses[name] = f"ERROR: {exc}"

    idle_seconds = None
    if not active:
        finished = []
        for spec in local:
            if spec.is_terminal and spec.finished_at:
                try:
                    finished.append(datetime.fromisoformat(spec.finished_at).timestamp())
                except ValueError:
                    continue
        if finished:
            idle_seconds = max(0, int(sampled_at - max(finished)))

    authority = None
    if scheduler_target is not None:
        from vq import broker_target
        _, authority = broker_target.route(cfg, scheduler_target)
        if scheduler is not None and scheduler.get("authority") != authority:
            reasons.append("scheduler configuration changed since daemon admission snapshot")
        free_cpus = free_mem = None
        free_jobs = scheduler.get("free_jobs") if scheduler else None
    return {
        "max_wall_time_seconds": (
            cfg.hosts[scheduler_target].scheduler_max_wall_time_seconds
            if scheduler_target is not None else None
        ),
        "max_request_cpus": (
            cfg.hosts[scheduler_target].scheduler_max_cpus
            if scheduler_target is not None else None
        ),
        "runtime_pins": runtime_pins,
        "scheduler_target": scheduler_target,
        "authority": authority,
        # #160: this host's definition of the lane, for a broker elsewhere.
        "lane_sha256": (broker_target.lane_digest(cfg.hosts[scheduler_target])
                        if scheduler_target is not None else None),
        "schema": CAPACITY_SCHEMA,
        "sampled_at": sampled_at,
        "state_root": client_root if agreement else None,
        "daemon_ok": daemon_ok,
        "state_root_agreement": agreement,
        "admission": {"open": not reasons, "reasons": reasons},
        # Both the daemon's count and the live spec list must show no pending
        # local job: the snapshot can be older than a job accepted since.
        "queue_ready": (
            scheduler is not None and scheduler.get("queue_ready") is True
            and not any(str(s.state) == "pending" and s.scheduler_target == scheduler_target
                        for s in specs)
        ) if scheduler_target is not None else (
            snapshot is not None and local_pending == 0 and not pending
        ),
        # How much older than this sample the daemon's figures are, on this
        # host's clock; the broker backdates observed_at by it.
        "snapshot_age_seconds": (
            max(0.0, sampled_at - snapshot["published_at"]) if snapshot is not None else None
        ),
        "pending_local_jobs": local_pending,
        "pending_cpus": sum(spec.cpus for spec in pending),
        "free_cpus": free_cpus,
        "free_mem_mb": free_mem,
        "free_jobs": free_jobs,
        # #168: the daemon's GPU devices and those not assigned now; null
        # from a daemon that does not report them.
        "gpus": gpus,
        "free_gpus": free_gpus,
        "idle_seconds": idle_seconds,
        "programs": statuses,
        "jobs": sorted(spec.id for spec in specs),
    }


def _gpu_figures(snapshot: Mapping[str, Any] | None) -> tuple[int | None, int | None]:
    """The daemon's GPU device count and free devices, or unknown (None, None).

    A daemon that lists no devices has none (0). One whose snapshot carries
    no GPU keys predates GPU admission, so its GPUs are unknown, not zero.
    """
    if snapshot is None or "gpu_devices" not in snapshot:
        return None, None
    devices, used = snapshot["gpu_devices"], snapshot.get("used_gpus")
    if devices is None:
        devices = []
    if (not isinstance(devices, list) or type(used) is not int
            or not 0 <= used <= len(devices)):
        return None, None
    return len(devices), len(devices) - used


_ADMISSION_KEYS = frozenset({
    "schema", "published_at", "holds", "local_pending", "max_cpus", "max_jobs",
    "max_mem_mb", "local_active", "used_cpus", "used_mem_mb",
})


def _valid_admission_snapshot(
    answer: object, sampled_at: float, reasons: list[str]
) -> Mapping[str, Any] | None:
    """Accept the daemon's snapshot only whole, well-typed and current."""
    if (
        not isinstance(answer, Mapping)
        or answer.get("schema") != "vq.daemon-admission/1"
        or not set(answer) >= _ADMISSION_KEYS
    ):
        reasons.append("the daemon's admission snapshot is missing or malformed")
        return None
    published = answer["published_at"]
    holds = answer["holds"]
    counts_ok = all(
        type(answer[key]) is int and answer[key] >= 0
        for key in ("local_pending", "local_active", "used_cpus")
    )
    caps_ok = (
        type(answer["max_cpus"]) is int and answer["max_cpus"] > 0
        and all(
            answer[key] is None or (type(answer[key]) is int and answer[key] > 0)
            for key in ("max_jobs", "max_mem_mb")
        )
        and (answer["used_mem_mb"] is None
             or (type(answer["used_mem_mb"]) is int and answer["used_mem_mb"] >= 0))
    )
    if (
        type(published) not in (int, float)
        or not math.isfinite(published)
        or not isinstance(holds, list)
        or not all(isinstance(hold, str) for hold in holds)
        or not counts_ok
        or not caps_ok
    ):
        reasons.append("the daemon's admission snapshot is missing or malformed")
        return None
    age = sampled_at - published
    if age < -5.0 or age > ADMISSION_SNAPSHOT_MAX_AGE_SECONDS:
        reasons.append(f"the daemon's admission snapshot is {age:.0f} s old")
        return None
    return answer


# --------------------------------------------------------------------------
# Broker side


@dataclass(frozen=True)
class TargetEvidence:
    """What one target said, converted for B2 and the placer."""

    host: str
    snapshot: overview.BrokerCapacitySnapshot
    state_root: str | None = None
    programs: Mapping[str, str] = field(default_factory=dict)
    jobs: frozenset[str] = frozenset()
    reasons: tuple[str, ...] = ()
    error: str | None = None


def _closed(host: str, observed_at: float, error: str) -> TargetEvidence:
    """No evidence: every gate closed, capacity unknown."""
    return TargetEvidence(
        host=host,
        snapshot=overview.BrokerCapacitySnapshot(
            host=host, observed_at=observed_at, free_cpus=None, free_mem_mb=None,
            free_jobs=None,
        ),
        error=error,
    )


def _int_or_none(value: object, name: str) -> int | None:
    if value is None:
        return None
    if type(value) is not int:
        raise ValueError(f"{name} must be an integer or null")
    return value


def evidence_from_payload(
    host: str,
    payload: object,
    *,
    observed_at: float,
    pools: frozenset[str] = frozenset(),
) -> TargetEvidence:
    """Convert a target's capacity answer; anything malformed is no evidence."""
    try:
        if not isinstance(payload, Mapping) or payload.get("schema") != CAPACITY_SCHEMA:
            raise ValueError("not a vq broker capacity answer")
        admission = payload["admission"]
        if not isinstance(admission, Mapping) or type(admission.get("open")) is not bool:
            raise ValueError("admission is malformed")
        reasons = admission.get("reasons")
        if not isinstance(reasons, list) or not all(isinstance(r, str) for r in reasons):
            raise ValueError("admission reasons are malformed")
        for name in ("daemon_ok", "state_root_agreement", "queue_ready"):
            if type(payload.get(name)) is not bool:
                raise ValueError(f"{name} must be a boolean")
        root = payload.get("state_root")
        if root is not None and (not isinstance(root, str) or not root.startswith("/")):
            raise ValueError("state_root must be an absolute path or null")
        programs = payload.get("programs")
        if not isinstance(programs, Mapping) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in programs.items()
        ):
            raise ValueError("programs are malformed")
        jobs = payload.get("jobs")
        if not isinstance(jobs, list) or not all(isinstance(j, str) for j in jobs):
            raise ValueError("jobs are malformed")
        scheduler_target = payload.get("scheduler_target")
        authority = payload.get("authority")
        if scheduler_target is not None and (
            scheduler_target != host or not isinstance(authority, dict)
            or authority.get("scheduler") not in {"slurm", "pbs"} or authority.get("target") != host
            or not all(isinstance(k, str) and isinstance(v, str) for k, v in authority.items())
        ):
            raise ValueError("scheduler authority is malformed")
        pins = payload.get("runtime_pins")
        if pins is not None and (
            not isinstance(pins, dict) or not all(
                isinstance(k, str) and isinstance(v, str)
                and re.fullmatch(r"[0-9a-f]{40}", v) is not None
                for k, v in pins.items()
            )
        ):
            raise ValueError("runtime pins are malformed")
        pending_cpus = _int_or_none(payload.get("pending_cpus"), "pending_cpus") or 0
        age = payload.get("snapshot_age_seconds")
        if age is not None and (
            type(age) not in (int, float) or not math.isfinite(age) or age < 0
        ):
            raise ValueError("snapshot_age_seconds must be finite and not negative")
        # The capacity figures describe the moment the daemon published them,
        # so that, not the call's start, is the bound on when they were
        # sampled. A charge must not settle on figures older than its job.
        observed_at = observed_at - (age or 0.0)
        agreement = payload["state_root_agreement"] is True and root is not None
        snapshot = overview.BrokerCapacitySnapshot(
            host=host,
            observed_at=observed_at,
            free_cpus=_int_or_none(payload.get("free_cpus"), "free_cpus"),
            free_mem_mb=_int_or_none(payload.get("free_mem_mb"), "free_mem_mb"),
            free_jobs=_int_or_none(payload.get("free_jobs"), "free_jobs"),
            reachable=True,
            daemon_healthy=payload["daemon_ok"] is True,
            state_root_agreement=agreement,
            # Open only when the target reported no closed gate at all.
            admission_open=admission["open"] is True and not reasons,
            queue_ready=payload["queue_ready"] is True,
            is_scheduler_host=payload.get("scheduler_target") is not None,
            scheduler_admission=payload.get("scheduler_target") == host,
            authority=payload.get("authority"),
            runtime_pins=payload.get("runtime_pins"),
            max_wall_time_seconds=payload.get("max_wall_time_seconds"),
            max_request_cpus=_int_or_none(payload.get("max_request_cpus"), "max_request_cpus"),
            pools=pools,
            pending_cpus=max(pending_cpus, 0),
            idle_seconds=_int_or_none(payload.get("idle_seconds"), "idle_seconds"),
        )
    except (KeyError, TypeError, ValueError) as exc:
        return _closed(host, observed_at, f"unusable capacity answer from {host}: {exc}")
    return TargetEvidence(
        host=host,
        snapshot=snapshot,
        state_root=root if agreement else None,
        programs=dict(programs),
        jobs=frozenset(jobs),
        reasons=tuple(reasons),
    )


def host_pools(cfg: Any, host: str) -> frozenset[str]:
    return frozenset(
        name for name, pool in cfg.pools.items() if host in getattr(pool, "hosts", ())
    )


def fetch_payload(
    cfg: Any,
    host: str,
    *,
    runner: Callable[..., Any] | None = None,
    timeout: float = 60.0,
    programs: Sequence[str] = (),
) -> object:
    """One target's raw ``vq broker capacity --json`` answer over SSH.

    Raises ValueError, naming the host but never its output, when it is
    not configured or gives no parseable answer.
    """
    from vq import broker_target
    host_cfg = cfg.hosts.get(host)
    if host_cfg is None:
        raise ValueError(f"{host} is not configured")
    binding = None
    scheduler_args = []
    if getattr(host_cfg, "scheduler", "local") != "local":
        host_cfg, binding = broker_target.route(cfg, host)
        scheduler_args = ["--scheduler-target", host]
    if runner is None:
        from vq import transport
        runner = broker_target.run if scheduler_args else transport.run_remote_vq
    try:
        program_args = [arg for name in sorted(set(programs))
                        for arg in ("--program", name)]
        proc = runner(
            host_cfg, "broker", "capacity", "--json", *program_args, *scheduler_args,
            timeout=timeout, owned_process_group=True,
            max_stdout_bytes=16 * 1024 * 1024, max_stderr_bytes=1024 * 1024,
        )
        payload = json.loads(proc.stdout)
        if scheduler_args and binding is not None and broker_target.is_remote_driver(host_cfg):
            return _remote_driver_payload(host, payload, binding)
        if scheduler_args and (payload.get("scheduler_target") != host
                               or payload.get("authority") != binding):
            raise ValueError("driver returned a different scheduler authority")
        return payload
    except Exception as exc:  # noqa: BLE001 - unreachable is no evidence
        text = " ".join(str(exc).split()) or type(exc).__name__
        raise ValueError(f"{host} gave no capacity evidence: {text}") from None


def _remote_driver_payload(host: str, payload: Any,
                           binding: dict[str, str]) -> dict[str, Any]:
    """A driver on another host's answer, checked by lane and bound to this broker.

    The driver reports the lane's authority from its own config, where it is
    its own local driver: it must name this lane and scheduler, drive the
    lane itself, and define the lane identically (:func:`lane_digest`). The
    evidence then carries this broker's binding, which recovery re-checks.
    """
    reported = payload.get("authority") if isinstance(payload, dict) else None
    if (not isinstance(reported, dict) or payload.get("scheduler_target") != host
            or reported.get("target") != host
            or reported.get("scheduler") != binding["scheduler"]):
        raise ValueError("driver returned a different scheduler authority")
    if reported.get("ssh") != "localhost":
        raise ValueError("the driver does not drive this lane itself")
    lane = payload.get("lane_sha256")
    if lane is None:
        raise ValueError("the driver's vq reports no lane digest; upgrade it for "
                         "remote-driver placement")
    if lane != binding["lane_sha256"]:
        raise ValueError("the driver defines this lane differently from the broker")
    return {**payload, "authority": binding}


def collect(
    cfg: Any,
    hosts: Sequence[str],
    *,
    clock: Callable[[], float] = time.time,
    runner: Callable[..., Any] | None = None,
    timeout: float = 60.0,
    workers: int = 8,
    programs: Sequence[str] = (),
) -> dict[str, TargetEvidence]:
    """Ask every target for its evidence over SSH, in parallel and bounded.

    ``observed_at`` is the broker's clock at the start of each call. Only
    ``programs`` (those the held jobs name) are checked on each target.
    """
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("collection timeout must be finite and positive")

    def one(host: str) -> TargetEvidence:
        started = clock()
        try:
            payload = fetch_payload(cfg, host, runner=runner, timeout=timeout,
                                    programs=programs)
        except ValueError as exc:
            return _closed(host, started, str(exc))
        return evidence_from_payload(
            host, payload, observed_at=started, pools=host_pools(cfg, host)
        )

    if not hosts:
        return {}
    with ThreadPoolExecutor(max_workers=max(1, min(workers, len(hosts)))) as pool:
        results = list(pool.map(one, hosts))
    return {evidence.host: evidence for evidence in results}


def program_refusal(
    evidence: Mapping[str, TargetEvidence],
) -> Callable[[Any, str], str | None]:
    """Per job: refuse a host whose registry does not report its program OK."""

    def refuse(record: Any, host: str) -> str | None:
        program = record.intent.program
        if program is None:
            return None
        status = evidence[host].programs.get(program) if host in evidence else None
        if status != "OK":
            return f"program {program} is {status or 'not registered'} on {host}"
        return None

    return refuse
