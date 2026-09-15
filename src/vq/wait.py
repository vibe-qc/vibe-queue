"""Block until a job reaches a terminal state (v0.6.14).

Used by:
  * `vq wait [HOST] JOBID` — standalone synchronous wait verb.
  * `vq submit ... --wait` — sugar that submits + waits in one call.

The poll loop is laptop-side regardless of host. For local jobs it
reads the spec file directly; for remote jobs it shells out to
`vq status HOST JOBID --json` and parses the state. We DO NOT push
the wait loop to the remote — keeping it laptop-side means:

  * Ctrl-C / SIGINT cleanly cancels the WAIT (not the job).
  * Network hiccups in remote-status fetches retry transparently.
  * The remote daemon doesn't get a long-lived ssh session per
    wait, which would limit concurrent waits to the laptop's
    MaxSessions.

The wait verb does NOT cancel the underlying job on Ctrl-C — the
job keeps running. Operator who wants to abort the job should
`vq kill JOBID` separately.
"""
from __future__ import annotations

import logging
import math
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from vq import config, rpc, transport
from vq.host import is_local_host
from vq.listing import queue_handle_for_spec
from vq.scheduler_dispatch import SchedulerError
from vq.spec import TERMINAL_STATES, JobSpec, JobState
from vq.spec_access import reread_authorized_spec, resolve_authorized_spec

log = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL_SECONDS = 5.0
"""Default cadence for the poll loop. 5s matches the watchdog
sampling rhythm and keeps idle wait-loops cheap on both ends."""


@dataclass(frozen=True)
class WaitResult:
    """Outcome of a wait. ``state`` is the spec's terminal state;
    ``exit_code`` is the job's exit_code (None for non-COMPLETED/FAILED
    terminal states where there's no underlying process exit). ``cli_exit_code``
    is what the CLI should exit with — mapping defined below."""

    jobid: str
    state: JobState
    exit_code: int | None
    terminal_diagnosis: Mapping[str, Any] | None = None
    queue_handle: Mapping[str, Any] | None = None

    @property
    def cli_exit_code(self) -> int:
        """Map the terminal state to a meaningful CLI exit code so a
        shell wrapper can `set -e` on `vq wait`. Conventions:

          * COMPLETED → the job's actual exit_code (0 normally).
          * FAILED → the job's exit_code (non-zero by definition).
            Fallback to 1 if exit_code is unknown.
          * KILLED / OOM_KILLED / STARVED / TIME_EXCEEDED → 1
            (operator + watchdog kills both indicate "not success").
          * ABORTED_BY_QUEUE → 1.
          * INTERRUPTED → 1.

        Special timeout case (see ``WaitTimeout`` below) doesn't
        return a WaitResult — the CLI converts that into exit 124
        directly (matches GNU coreutils ``timeout``).
        """
        if self.state == JobState.COMPLETED:
            # COMPLETED usually means exit_code=0, but it's possible
            # for a bash-wrap path to record an unusual exit code.
            # Trust spec.exit_code when present, default to 0.
            return self.exit_code if self.exit_code is not None else 0
        if self.state == JobState.FAILED:
            return self.exit_code if self.exit_code is not None else 1
        # Every other terminal state maps to 1.
        return 1

    def to_json_payload(self) -> dict[str, object]:
        """Machine-readable result emitted by ``vq wait --json``."""
        return {
            "jobid": self.jobid,
            "state": self.state.value,
            "exit_code": self.exit_code,
            "cli_exit_code": self.cli_exit_code,
            "queue_handle": self.queue_handle,
            "terminal_diagnosis": self.terminal_diagnosis,
        }


class WaitTimeout(Exception):
    """Raised when the configured timeout elapses before the job
    reaches a terminal state. Includes the last-seen state so the
    CLI can render a useful message."""

    def __init__(
        self,
        jobid: str,
        last_state: JobState | None,
        detail: str | None = None,
        queue_handle: Mapping[str, Any] | None = None,
    ) -> None:
        self.jobid = jobid
        self.last_state = last_state
        self.detail = detail
        self.queue_handle = queue_handle
        msg = (
            f"wait for {jobid} timed out; last state: "
            f"{last_state.value if last_state else '(unknown)'}"
        )
        if detail:
            msg = f"{msg}; {detail}"
        super().__init__(msg)


def _format_seconds(seconds: float) -> str:
    seconds_i = int(round(seconds))
    if seconds_i < 60:
        return f"{seconds_i}s"
    minutes, sec = divmod(seconds_i, 60)
    if minutes < 60:
        return f"{minutes}m{sec:02d}s"
    hours, minute = divmod(minutes, 60)
    return f"{hours}h{minute:02d}m{sec:02d}s"


def _parse_iso_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _current_pause_seconds(
    spec: JobSpec,
    *,
    monotonic_now: float | None = None,
) -> float | None:
    if spec.state != JobState.SUSPENDED:
        return None
    if spec.paused_monotonic_at is not None:
        now = time.monotonic() if monotonic_now is None else monotonic_now
        return round(max(0.0, now - spec.paused_monotonic_at), 3)
    paused_at = _parse_iso_utc(spec.paused_at)
    if paused_at is None:
        return None
    return round(max(0.0, (datetime.now(UTC) - paused_at).total_seconds()), 3)


def _active_elapsed_detail(
    started_at: str | None,
    wall_time_seconds: int | float | None,
    *,
    paused_effective_seconds: float = 0.0,
) -> str | None:
    started = _parse_iso_utc(started_at)
    if started is None:
        return None
    elapsed = max(0.0, (datetime.now(UTC) - started).total_seconds())
    active_elapsed = max(0.0, elapsed - max(0.0, paused_effective_seconds))
    if wall_time_seconds is None:
        return f"active_elapsed={_format_seconds(active_elapsed)}"
    wall = float(wall_time_seconds)
    if wall <= 0:
        return f"active_elapsed={_format_seconds(active_elapsed)}"
    pct = int(round(100 * active_elapsed / wall))
    return (
        f"active_elapsed={_format_seconds(active_elapsed)}/"
        f"wall={_format_seconds(wall)} ({pct}% used)"
    )


def _hms_to_seconds(value: str | None) -> int | None:
    if not value:
        return None
    parts = value.split(":")
    if len(parts) != 3:
        return None
    try:
        hours, minutes, seconds = (int(part) for part in parts)
    except ValueError:
        return None
    if minutes < 0 or minutes >= 60 or seconds < 0 or seconds >= 60:
        return None
    return hours * 3600 + minutes * 60 + seconds


def _scheduler_wall_detail(used: str | None, limit: str | None) -> str | None:
    if used is None and limit is None:
        return None
    if used is not None and limit is not None:
        used_seconds = _hms_to_seconds(used)
        limit_seconds = _hms_to_seconds(limit)
        suffix = ""
        if used_seconds is not None and limit_seconds and limit_seconds > 0:
            pct = int(round(100 * used_seconds / limit_seconds))
            suffix = f" ({pct}% used)"
        return f"scheduler_wall={used}/{limit}{suffix}"
    if used is not None:
        return f"scheduler_wall={used} used"
    return f"scheduler_wall={limit} limit"


def _payload_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if not isinstance(value, (int, float, str)):
        return None
    try:
        result = float(value)
    except (OverflowError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _payload_int(value: object) -> int | None:
    as_float = _payload_float(value)
    if as_float is None:
        return None
    return int(as_float)


def _timeout_detail_from_spec(
    spec: JobSpec,
    *,
    monotonic_now: float | None = None,
) -> str | None:
    parts: list[str] = []
    current_pause = _current_pause_seconds(spec, monotonic_now=monotonic_now)
    paused_effective = spec.paused_seconds_total + (current_pause or 0.0)
    if spec.paused_by:
        parts.append(f"paused_by={spec.paused_by}")
    if current_pause is not None:
        parts.append(f"paused_now={_format_seconds(current_pause)}")
    if paused_effective > 0:
        parts.append(f"paused_total={_format_seconds(paused_effective)}")
    active_detail = _active_elapsed_detail(
        spec.started_at,
        spec.wall_time_seconds,
        paused_effective_seconds=paused_effective,
    )
    if active_detail:
        parts.append(active_detail)
    if spec.scheduler_target:
        parts.append(f"scheduler_target={spec.scheduler_target}")
    if spec.scheduler_state:
        parts.append(f"scheduler_state={spec.scheduler_state}")
    wall_detail = _scheduler_wall_detail(
        spec.scheduler_walltime_used,
        spec.scheduler_walltime_limit,
    )
    if wall_detail:
        parts.append(wall_detail)
    if spec.failure_reason:
        parts.append(f"reason={spec.failure_reason}")
    return "; ".join(parts) or None


def _queue_handle_from_spec(spec: JobSpec) -> dict[str, str | None]:
    return queue_handle_for_spec(spec, "localhost")


def _payload_str(
    payload: Mapping[str, Any],
    key: str,
) -> str | None:
    value = payload.get(key)
    return value if isinstance(value, str) and value else None


def _timeout_detail_from_payload(payload: Mapping[str, Any]) -> str | None:
    parts: list[str] = []
    paused_by = _payload_str(payload, "paused_by")
    paused_current = _payload_float(payload.get("paused_current_seconds"))
    paused_effective = _payload_float(payload.get("paused_effective_seconds"))
    if paused_by:
        parts.append(f"paused_by={paused_by}")
    if paused_current is not None:
        parts.append(f"paused_now={_format_seconds(paused_current)}")
    if paused_effective is not None and paused_effective > 0:
        parts.append(f"paused_total={_format_seconds(paused_effective)}")
    active_detail = _active_elapsed_detail(
        _payload_str(payload, "started_at"),
        _payload_int(payload.get("wall_time_seconds")),
        paused_effective_seconds=paused_effective or 0.0,
    )
    if active_detail:
        parts.append(active_detail)
    for label, key in (
        ("pending_reason", "pending_admission_reason"),
        ("scheduler_phase", "pbs_state_label"),
        ("fetch_state", "fetch_state_label"),
        ("scheduler_target", "scheduler_target"),
        ("scheduler_state", "scheduler_state"),
    ):
        value = _payload_str(payload, key)
        if value:
            parts.append(f"{label}={value}")
    wall_detail = _scheduler_wall_detail(
        _payload_str(payload, "scheduler_walltime_used"),
        _payload_str(payload, "scheduler_walltime_limit"),
    )
    if wall_detail:
        parts.append(wall_detail)
    reason = _payload_str(payload, "failure_reason")
    if reason:
        parts.append(f"reason={reason}")
    return "; ".join(parts) or None


def wait_for_terminal_local(
    jobid: str,
    *,
    poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS,
    timeout: float | None = None,
    queue_dir: Path | None = None,
    multi_user: bool = False,
    _now: callable = time.monotonic,  # type: ignore[type-arg]
    _sleep: callable = time.sleep,  # type: ignore[type-arg]
) -> WaitResult:
    """Block until the local spec for ``jobid`` is terminal. The
    spec is re-read each tick (the daemon writes it atomically via
    tempfile-then-rename, so partial-write races are impossible).

    ``multi_user`` (v0.6.40): resolve the spec from the per-user
    state dirs under ``/var/lib/vq/users/<uid>/`` instead of the
    single-user queue dir.

    ``_now`` / ``_sleep`` are injectable for fast tests."""
    spec_path, spec = resolve_authorized_spec(
        jobid,
        multi_user=multi_user,
        queue_dir=queue_dir,
    )
    if not multi_user:
        # Pin the queue selected by the initial authorized lookup. Environment
        # drift during a long wait must not redirect later secure re-reads.
        queue_dir = spec_path.parent

    deadline = (_now() + timeout) if timeout is not None else None
    last_state: JobState | None = None
    last_detail: str | None = None
    last_queue_handle: Mapping[str, Any] | None = None

    def reread_after_boundary() -> JobSpec | None:
        try:
            return reread_authorized_spec(
                jobid,
                expected_path=spec_path,
                queue_dir=queue_dir,
                multi_user=multi_user,
            )
        except FileNotFoundError:
            raise FileNotFoundError(
                f"spec for {jobid} disappeared during wait (deleted by "
                f"`vq cleanup`?)"
            ) from None
        except (OSError, ValueError, config.ConfigError) as exc:
            log.warning(
                "wait: secure spec re-read failed for %s; retaining the last "
                "authorized snapshot: %s",
                jobid,
                exc,
            )
            return None

    while True:
        if (
            not spec.is_terminal
            and spec.scheduler_target is not None
            and spec.scheduler_job_id is not None
        ):
            from vq.status import refresh_scheduler_status

            now = _now()
            if deadline is not None and now >= deadline:
                raise WaitTimeout(
                    jobid,
                    spec.state,
                    _timeout_detail_from_spec(spec, monotonic_now=now),
                    _queue_handle_from_spec(spec),
                )
            refresh_timeout = min(
                poll_interval,
                rpc.SCHEDULER_STATUS_REFRESH_MAX_SECONDS,
            )
            if deadline is not None:
                refresh_timeout = min(
                    refresh_timeout,
                    deadline - now,
                )
            if refresh_timeout > 0:
                try:
                    scheduler_refresh = refresh_scheduler_status(
                        jobid,
                        queue_dir=queue_dir,
                        multi_user=multi_user,
                        timeout_seconds=refresh_timeout,
                        spec=spec,
                    )
                    if (
                        scheduler_refresh is not None
                        and scheduler_refresh["status"] != "fresh"
                    ):
                        log.warning(
                            "wait: scheduler refresh unavailable for %s "
                            "(reason=%s; will retry in %.1fs)",
                            jobid,
                            scheduler_refresh["reason"],
                            poll_interval,
                        )
                except SchedulerError as exc:
                    log.warning(
                        "wait: scheduler refresh failed for %s "
                        "(will retry in %.1fs): %s",
                        jobid,
                        poll_interval,
                        exc,
                    )
                refreshed = reread_after_boundary()
                if refreshed is not None:
                    spec = refreshed
        last_state = spec.state
        last_detail = _timeout_detail_from_spec(spec, monotonic_now=_now())
        last_queue_handle = _queue_handle_from_spec(spec)
        if spec.state in TERMINAL_STATES:
            from vq.status import terminal_diagnosis_for_spec

            return WaitResult(
                jobid=jobid,
                state=spec.state,
                exit_code=spec.exit_code,
                queue_handle=last_queue_handle,
                terminal_diagnosis=terminal_diagnosis_for_spec(spec),
            )
        if deadline is not None and _now() >= deadline:
            raise WaitTimeout(jobid, last_state, last_detail, last_queue_handle)
        _sleep(poll_interval)
        refreshed = reread_after_boundary()
        if refreshed is not None:
            spec = refreshed


def wait_for_terminal_remote(
    host_cfg: config.HostConfig,
    jobid: str,
    *,
    poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS,
    timeout: float | None = None,
    _now: callable = time.monotonic,  # type: ignore[type-arg]
    _sleep: callable = time.sleep,  # type: ignore[type-arg]
) -> WaitResult:
    """Block until the remote spec for ``jobid`` is terminal.

    Polls via ``ssh <host> vq status localhost JOBID --json``. The
    remote vq's --json output carries the spec; we extract state +
    exit_code from it.

    Transient SSH/network errors are logged + retried on the next
    poll tick — a 5s network blip shouldn't fail a multi-hour wait.
    Persistent errors (auth failure, host unreachable) will keep
    retrying until the timeout fires; operator's escape hatch is
    Ctrl-C.
    """
    import json as _json

    deadline = (_now() + timeout) if timeout is not None else None
    last_state: JobState | None = None
    last_detail: str | None = None
    last_queue_handle: Mapping[str, Any] | None = None
    while True:
        try:
            proc = transport.run_remote_vq(
                host_cfg, "status", "localhost", jobid, "--json"
            )
            payload = _json.loads(proc.stdout)
            state_str = payload.get("state")
            exit_code = payload.get("exit_code")
            if state_str is None:
                raise transport.RemoteError(
                    f"remote vq status didn't return a state field: "
                    f"{proc.stdout!r}"
                )
            last_state = JobState(state_str)
            last_detail = _timeout_detail_from_payload(payload)
            queue_handle = payload.get("queue_handle")
            if not isinstance(queue_handle, Mapping):
                submitted_at = payload.get("submitted_at")
                queue_handle = {
                    "job_id": jobid,
                    "host": host_cfg.ssh,
                    "submitted_at": (
                        submitted_at if isinstance(submitted_at, str) else None
                    ),
                }
            last_queue_handle = queue_handle
            if last_state in TERMINAL_STATES:
                terminal_diagnosis = payload.get("terminal_diagnosis")
                return WaitResult(
                    jobid=jobid,
                    state=last_state,
                    exit_code=exit_code if isinstance(exit_code, int) else None,
                    queue_handle=queue_handle,
                    terminal_diagnosis=(
                        terminal_diagnosis
                        if isinstance(terminal_diagnosis, Mapping)
                        else None
                    ),
                )
        except (transport.RemoteError, _json.JSONDecodeError, ValueError) as e:
            log.warning(
                "wait: transient error polling remote status for %s "
                "(will retry in %.1fs): %s",
                jobid, poll_interval, e,
            )
        if deadline is not None and _now() >= deadline:
            raise WaitTimeout(jobid, last_state, last_detail, last_queue_handle)
        _sleep(poll_interval)


def wait_for_terminal(
    host: str,
    jobid: str,
    *,
    host_cfg: config.HostConfig | None = None,
    poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS,
    timeout: float | None = None,
    multi_user: bool = False,
) -> WaitResult:
    """Local-or-remote dispatcher matching the rest of the CLI's
    host-resolution shape. ``host_cfg`` is required for non-local
    hosts (the caller passes it from cfg.host(host)).

    ``multi_user`` (v0.6.40) is forwarded to the local path; the
    remote path polls ``vq status`` over SSH and is mode-agnostic
    (the remote vq resolves its own state layout)."""
    if is_local_host(host):
        return wait_for_terminal_local(
            jobid, poll_interval=poll_interval, timeout=timeout,
            multi_user=multi_user,
        )
    if host_cfg is None:
        raise ValueError(
            f"wait_for_terminal(host={host!r}): host_cfg required for "
            f"non-local host"
        )
    return wait_for_terminal_remote(
        host_cfg, jobid, poll_interval=poll_interval, timeout=timeout
    )


@dataclass(frozen=True)
class SchedulerAcceptance:
    """Observed scheduler submission outcome, not a calculation verdict."""

    jobid: str
    outcome: str
    state: str | None = None
    scheduler_job_id: str | None = None
    detail: str | None = None

    @property
    def cli_exit_code(self) -> int:
        if self.outcome == "accepted":
            return 0
        return 124 if self.outcome == "timeout" else 1

    def to_json_payload(self) -> dict[str, object]:
        return {
            "jobid": self.jobid,
            "outcome": self.outcome,
            "state": self.state,
            "scheduler_job_id": self.scheduler_job_id,
            "detail": self.detail,
        }


def wait_for_scheduler_acceptance(
    host: str,
    jobids: list[str],
    *,
    scheduler_target: str,
    host_cfg: config.HostConfig | None = None,
    timeout: float = 60.0,
    poll_interval: float = 1.0,
    multi_user: bool = False,
    queue_dir: Path | None = None,
    _now: Callable[[], float] = time.monotonic,
    _sleep: Callable[[float], None] = time.sleep,
) -> list[SchedulerAcceptance]:
    """Wait for durable qsub/sbatch IDs within one budget for the whole batch.

    Poll the queue authority, not the execution host. A local RUNNING state is
    not acceptance: staging also uses it. Only an ID on the expected scheduler
    target proves submission. A terminal failure takes precedence over that ID.
    This only reads queue state (remote status may also refresh scheduler state).
    Timeout and Ctrl-C leave every queued job intact; submission is never replayed.
    """
    import json

    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("scheduler acceptance timeout must be finite and positive")
    if not math.isfinite(poll_interval) or poll_interval <= 0:
        raise ValueError("scheduler acceptance poll interval must be finite and positive")
    if not jobids or len(set(jobids)) != len(jobids):
        raise ValueError("scheduler acceptance requires distinct job IDs")
    local = is_local_host(host)
    if not local and host_cfg is None:
        raise ValueError("remote scheduler acceptance requires the driver host config")
    deadline = _now() + timeout
    resolved: dict[str, tuple[Path, Path | None]] = {}
    results: dict[str, SchedulerAcceptance] = {}
    snapshots: dict[str, SchedulerAcceptance] = {}
    while len(results) < len(jobids):
        for jobid in jobids:
            remaining = deadline - _now()
            if remaining <= 0:
                break
            if jobid in results:
                continue
            try:
                if local:
                    if jobid not in resolved:
                        path, spec = resolve_authorized_spec(
                            jobid, queue_dir=queue_dir, multi_user=multi_user,
                        )
                        resolved[jobid] = (path, None if multi_user else path.parent)
                    else:
                        path, pinned_queue = resolved[jobid]
                        spec = reread_authorized_spec(
                            jobid, expected_path=path, queue_dir=pinned_queue,
                            multi_user=multi_user,
                        )
                    payload = spec.model_dump(mode="json")
                else:
                    proc = transport.run_remote_vq(
                        host_cfg, "status", "localhost", jobid, "--json", "--tail", "1",
                        timeout=min(remaining, 10.0),
                        owned_process_group=True,
                        max_stdout_bytes=2 * 1024 * 1024,
                        max_stderr_bytes=64 * 1024,
                    )
                    payload = json.loads(proc.stdout)
                if not isinstance(payload, dict) or payload.get("id") != jobid:
                    raise ValueError("queue authority returned a different job identity")
                if payload.get("scheduler_target") != scheduler_target:
                    raise ValueError("queue authority returned a different scheduler target")
                state = JobState(payload.get("state"))
                scheduler_id = payload.get("scheduler_job_id")
                scheduler_id = (
                    scheduler_id.strip()
                    if isinstance(scheduler_id, str) and scheduler_id.strip() else None
                )
                detail = payload.get("failure_reason")
                detail = detail if isinstance(detail, str) else None
                outcome = "timeout"
                if state in TERMINAL_STATES and (
                    state != JobState.COMPLETED or payload.get("exit_code") not in (None, 0)
                ):
                    outcome = "failed"
                    detail = detail or f"job reached {state.value} during scheduler submission"
                elif scheduler_id is not None:
                    outcome = "accepted"
                elif state in TERMINAL_STATES:
                    outcome = "failed"
                    detail = "terminal job has no scheduler submission ID"
                observation = SchedulerAcceptance(
                    jobid, outcome, state.value, scheduler_id, detail,
                )
                snapshots[jobid] = observation
                if outcome != "timeout":
                    results[jobid] = observation
            except (OSError, ValueError, config.ConfigError, transport.RemoteError) as exc:
                # Never convert an unreadable/unauthorized snapshot into success.
                snapshots[jobid] = SchedulerAcceptance(
                    jobid, "timeout", detail=f"queue observation unavailable: {exc}",
                )
        remaining = deadline - _now()
        if remaining <= 0 or len(results) == len(jobids):
            break
        _sleep(min(poll_interval, remaining))
    return [
        results.get(jobid) or snapshots.get(jobid) or SchedulerAcceptance(
            jobid, "timeout", detail="acceptance deadline elapsed before observation",
        )
        for jobid in jobids
    ]
