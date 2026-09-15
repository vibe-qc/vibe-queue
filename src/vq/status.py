"""Show a single job's spec, status, and recent stdout/stderr."""
from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import json
import time
import zipfile
from pathlib import Path

from vq import admission, capacity, config, drain, events, rpc
from vq.daemon_control import is_daemon_serving
from vq.host import is_local_host
from vq.listing import (
    effective_queue_state,
    normalized_scheduler_phase,
    normalized_scheduler_target,
    pending_configured_capacity_known,
    pending_configured_capacity_overages,
    queue_handle_for_spec,
    scheduler_running_confirmed,
)
from vq.logs import read_system_progress
from vq.logs import tail_file as _tail_file
from vq.scheduler_dispatch import scheduler_dispatcher_for
from vq.spec import (
    JobSpec,
    JobState,
    describe_exit_code,
    signal_name_for_exit,
)
from vq.spec_access import (
    reread_authorized_spec,
    resolve_authorized_spec,
    stamp_terminal_status_read,
)

CHECKPOINT_QVF_FILENAME = "checkpoint.qvf"
SCHEDULER_STATUS_REFRESH_SECONDS = 5.0


class _CapacitySnapshotUnset:
    """Sentinel separating an unread snapshot from a proven unknown one."""


_CAPACITY_SNAPSHOT_UNSET = _CapacitySnapshotUnset()


def _pending_capacity_context(
    spec: JobSpec,
    *,
    multi_user: bool,
) -> tuple[
    capacity.DaemonCapacity | None,
    tuple[capacity.ConfiguredCapacityOverage, ...],
]:
    """Best-effort configured-cap snapshot and overages for one job."""
    if spec.state != JobState.PENDING:
        return None, ()
    try:
        snapshot = capacity.read_daemon_capacity(multi_user=multi_user)
    except Exception:
        return None, ()
    return snapshot, pending_configured_capacity_overages(spec, snapshot)


def refresh_scheduler_status(
    jobid: str,
    *,
    queue_dir: Path | None = None,
    multi_user: bool = False,
    timeout_seconds: float = SCHEDULER_STATUS_REFRESH_SECONDS,
    spec: JobSpec | None = None,
) -> dict[str, object] | None:
    """Refresh one active scheduler spec through the daemon main loop.

    Old/down daemons retain the established stale-status behavior.  A daemon
    that advertises the refresh contract must complete it before status is
    rendered; failure is explicit rather than silently presenting an old
    lifecycle state as current.
    """
    if spec is None:
        _spec_path, spec = resolve_authorized_spec(
            jobid,
            queue_dir=queue_dir,
            multi_user=multi_user,
        )
    if (
        spec.is_terminal
        or spec.scheduler_target is None
        or spec.scheduler_job_id is None
    ):
        return None
    try:
        result = rpc.request_scheduler_status_refresh(
            jobid,
            multi_user=multi_user,
            timeout_seconds=timeout_seconds,
        )
    except (ConnectionError, rpc.RPCError):
        return {
            "status": "unavailable",
            "observed_at": None,
            "reason": "rpc_error",
        }
    if result is None:
        return {
            "status": "unavailable",
            "observed_at": None,
            "reason": "daemon_unreachable_or_unsupported",
        }
    if result["completed"] is not True:
        reason = result.get("reason")
        if not isinstance(reason, str) or not reason:
            reason = "timeout"
        return {
            "status": "unavailable",
            "observed_at": None,
            "reason": reason,
        }
    return {
        "status": "fresh",
        "observed_at": result["observed_at"],
        "reason": None,
    }


def _refresh_and_reread_scheduler_spec(
    jobid: str,
    *,
    spec_path: Path,
    spec: JobSpec,
    queue_dir: Path | None,
    multi_user: bool,
) -> tuple[JobSpec, dict[str, object] | None]:
    """Refresh, then securely re-resolve and authorize the rendered spec."""
    scheduler_refresh = refresh_scheduler_status(
        jobid,
        queue_dir=queue_dir,
        multi_user=multi_user,
        spec=spec,
    )
    if scheduler_refresh is None:
        return spec, None
    try:
        refreshed_spec = reread_authorized_spec(
            jobid,
            expected_path=spec_path,
            queue_dir=queue_dir,
            multi_user=multi_user,
        )
    except (OSError, ValueError, config.ConfigError):
        return spec, {
            "status": "unavailable",
            "observed_at": None,
            "reason": "spec_reread_failed",
        }
    return refreshed_spec, scheduler_refresh


def show_status(
    host: str,
    jobid: str,
    *,
    tail: int | None = 50,
    queue_dir: Path | None = None,
    multi_user: bool = False,
    cfg: config.Config | None = None,
) -> str:
    """Format the spec and tail of stdout/stderr for `jobid`.

    `tail=None` means "show all output"; `tail=N` shows the last N lines per
    stream and prepends a "(K earlier lines)" hint if truncated.
    """
    if not is_local_host(host):
        raise NotImplementedError(f"remote status for {host!r} not implemented in v0.1")
    spec_path, spec = resolve_authorized_spec(
        jobid,
        queue_dir=queue_dir,
        multi_user=multi_user,
    )
    if not multi_user:
        # The old inline path resolved the default once, then reused that exact
        # queue for the pending census below.  Keep one coherent snapshot even
        # if the process environment changes during rendering.
        queue_dir = spec_path.parent
    spec, scheduler_refresh = _refresh_and_reread_scheduler_spec(
        jobid,
        spec_path=spec_path,
        spec=spec,
        queue_dir=queue_dir,
        multi_user=multi_user,
    )
    workspace = Path(spec.cwd)
    capacity_snapshot, configured_overages = _pending_capacity_context(
        spec,
        multi_user=multi_user,
    )

    state_label = _status_state_label(spec)
    if configured_overages:
        state_label = f"{state_label} (over cap)"
    if spec.is_archived:
        state_label = f"{state_label} (archived)"
    # STATUS-1: a non-terminal state (running / pending / suspended) only
    # means anything while the daemon is alive to advance it. If the daemon
    # is down, nothing reconciles this spec — a job whose process already
    # died still reads RUNNING here until the daemon restarts. Flag it so the
    # operator doesn't trust a stale label. (Terminal states are immutable,
    # so they're never stale; archived implies terminal.)
    if not spec.is_terminal and not is_daemon_serving(multi_user=multi_user):
        state_label = f"{state_label}  (⚠ daemon down — may be stale)"
    if (
        scheduler_refresh is not None
        and scheduler_refresh["status"] != "fresh"
    ):
        state_label = f"{state_label}  (scheduler refresh unavailable; may be stale)"

    lines = [
        f"id:           {spec.id}",
    ]
    # v0.5.34: show job_name prominently next to id when set. Conditional
    # so the common (no-name) status block is unchanged.
    if spec.job_name:
        lines.append(f"name:         {spec.job_name}")
    # v0.12.0: optional program-registry identity from `vq submit --program`.
    # It is job metadata, not a rewritten command, so keep it close to the
    # human label and omit it for old/no-program specs.
    if spec.program:
        lines.append(f"program:      {spec.program}")
    lines.extend(
        [
            f"state:        {state_label}",
            f"command:      {' '.join(spec.command)}",
            f"cwd:          {spec.cwd}",
            f"cpus:         {spec.cpus}",
            f"submitted:    {spec.submitted_at}",
        ]
    )
    if scheduler_refresh is not None:
        if scheduler_refresh["status"] == "fresh":
            lines.append(
                "scheduler refresh: fresh at "
                f"{scheduler_refresh['observed_at']}"
            )
        else:
            lines.append(
                "scheduler refresh: unavailable "
                f"({scheduler_refresh['reason']}; state may be stale)"
            )
    if spec.scheduler_tasks is not None:
        lines.append(f"sched_tasks:  {spec.scheduler_tasks}")
    # v0.9.2: queue position for a PENDING job — how many pending jobs sort
    # ahead of it in the daemon's dispatch order. Answers the documented
    # "my job sits in pending, where am I in line?" question. Best-effort:
    # a listing hiccup must never break `vq status`.
    if spec.state == JobState.PENDING:
        all_specs_for_pending: list[JobSpec] | None = None
        with contextlib.suppress(Exception):
            from vq.listing import list_jobs, pending_queue_position

            all_specs = list_jobs(host, queue_dir=queue_dir, multi_user=multi_user)
            all_specs_for_pending = all_specs
            same_target = [
                s
                for s in all_specs
                if s.scheduler_target == spec.scheduler_target
            ]
            pending = [s for s in same_target if s.state == JobState.PENDING]
            rank, total = pending_queue_position(spec, pending)
            scheduler_target_label = normalized_scheduler_target(spec)
            lane = (
                f"{scheduler_target_label} scheduler lane"
                if spec.scheduler_target is not None
                else "local lane"
            )
            lines.append(
                f"queue position: {rank} of {total} pending in {lane} "
                "(dispatch order: priority, then submit time)"
            )
            from vq.eta import (
                estimate_pending_wait,
                format_eta_duration,
                format_eta_sources,
            )

            if configured_overages:
                lines.append(
                    "queue eta:    unavailable (request exceeds configured "
                    "daemon caps; capacity must change or the job must be "
                    "resubmitted)"
                )
            else:
                eta = estimate_pending_wait(spec, pending, same_target)
                if eta is None:
                    lines.append(
                        "queue eta:    unavailable "
                        "(no retained completed-job history for jobs ahead)"
                    )
                elif eta.weak_sources:
                    weak_txt = format_eta_sources(eta.weak_sources)
                    if set(eta.source_counts) - set(eta.weak_sources):
                        lines.append(
                            f"queue eta:    ~{format_eta_duration(eta.seconds)} "
                            f"before dispatch turn ({eta.jobs_ahead} jobs ahead; "
                            f"history: {format_eta_sources(eta.source_counts)}; "
                            f"low confidence: {weak_txt})"
                        )
                    else:
                        lines.append(
                            "queue eta:    unknown (too few samples for a "
                            f"reliable estimate; history: {weak_txt})"
                        )
                else:
                    lines.append(
                        f"queue eta:    ~{format_eta_duration(eta.seconds)} "
                        f"before dispatch turn ({eta.jobs_ahead} jobs ahead; "
                        f"history: {format_eta_sources(eta.source_counts)})"
                    )
        if all_specs_for_pending is not None:
            blockers = pending_blockers(
                spec,
                all_specs_for_pending,
                multi_user=multi_user,
                capacity_snapshot=capacity_snapshot,
            )
            for i, blocker in enumerate(blockers):
                # One line per blocker: they stack, and clearing the first
                # leaves the job parked on the next.
                label = "pending:     " if i == 0 else "             "
                lines.append(f"{label} {blocker}")
    # v0.5.29: only show priority when it's non-default — keeps the
    # status block uncluttered for the common case.
    if spec.priority != 0:
        lines.append(f"priority:     {spec.priority}")
    # v0.6.6: show tags when any are set. Spec stores them deduped
    # + sorted, so the display order is stable across submits.
    if spec.tags:
        lines.append(f"tags:         {', '.join(spec.tags)}")
    # v0.5.30: auto-resume lineage. Both lines are conditional so the
    # common (no-auto-resume) status block is unchanged.
    if spec.recover_on_reboot:
        lines.append("auto-resume:  on (resubmits after a host reboot)")
    if spec.parent_jobid:
        lines.append(f"parent_jobid: {spec.parent_jobid} (auto-resumed from)")
    # v0.5.31: retry budget + backoff. Shown only when --retry was used.
    if spec.retry_max > 0:
        lines.append(
            f"retries:      {spec.retry_count}/{spec.retry_max} used"
        )
    if spec.not_before:
        # v0.6.12: the not_before field is shared by two paths —
        # v0.5.31 retry-backoff (sets it on FAIL-then-requeue with
        # retry_count > 0) and v0.6.12 scheduled-submit (sets it at
        # submit time with retry_count == 0). Use retry_count to
        # disambiguate the operator-facing label so a scheduled
        # submit doesn't get mislabeled as a retry.
        reason = "retry backoff" if spec.retry_count > 0 else "scheduled submit"
        lines.append(f"not_before:   {spec.not_before} ({reason})")
    # v0.6.22: show paused_by tag when set (operator can spot
    # "who paused this?" from the status line).
    if spec.paused_by:
        lines.append(f"paused_by:    {spec.paused_by}")
    current_pause = _current_pause_seconds(spec)
    if current_pause is not None:
        lines.append(f"paused_now:   {_format_seconds(current_pause)} since {spec.paused_at}")
    if spec.paused_seconds_total > 0:
        lines.append(
            f"paused_total: {_format_seconds(spec.paused_seconds_total)}"
        )
    # v0.6.54: show per-job workdir + cleanup mode when set. Useful
    # for chats / operators to know where the scratch space lives
    # (e.g. to ssh in and inspect basis-opt outputs). Conditional so
    # pre-v0.6.54 specs (workdir=None) don't show a phantom line.
    if spec.workdir:
        mode = (
            "clean-on-terminal" if spec.clean_workdir_on_terminal
            else "lingers until cleanup-sweep"
        )
        lines.append(f"workdir:      {spec.workdir} ({mode})")
    # v1.0 scheduler backend: split the local vq lifecycle from the scheduler
    # lifecycle. The vq state flips to RUNNING once the driver submits the job;
    # scheduler_state tells whether the cluster job is queued, executing, or in
    # the marker/fetch fence.
    if spec.scheduler_target is not None:
        scheduler_bits = [
            normalized_scheduler_target(spec) or "scheduler-target-unknown"
        ]
        if spec.scheduler_job_id:
            scheduler_bits.append(f"job={spec.scheduler_job_id}")
        lines.append(f"scheduler:    {' '.join(scheduler_bits)}")
        lines.append(f"sched_state:  {_scheduler_state_label(spec)}")
        lines.append(f"fetch_state:  {_scheduler_fetch_state_label(spec)}")
        if spec.scheduler_exec_host:
            lines.append(f"exec_host:    {spec.scheduler_exec_host}")
        walltime = _format_scheduler_walltime(
            spec.scheduler_walltime_used,
            spec.scheduler_walltime_limit,
        )
        if walltime:
            lines.append(f"sched_wall:   {walltime}")
        walltime_warning = _format_scheduler_walltime_warning(
            spec.scheduler_walltime_used,
            spec.scheduler_walltime_limit,
        )
        if walltime_warning:
            lines.append(f"warning:      {walltime_warning}")
    # v0.6.52: show array context when this spec is an element of
    # a `vq submit --array N` group. Conditional like priority/tags
    # so non-array submits keep their uncluttered status block.
    if spec.array_index is not None:
        lines.append(
            f"array:        {spec.array_index}/{spec.array_total} "
            f"(group={spec.array_group_id})"
        )
    # v0.6.51: show depends_on when set. Annotation tells the operator
    # at a glance whether the dispatch gate is what's holding this
    # job (waiting / ready / failed:JOBID).
    if spec.depends_on:
        # Probe each predecessor's state. The probe is best-effort
        # (multi-user lookup may not find a cross-user dep that was
        # submitted under a different uid; rare and surfaced as
        # "?"). Resolution scope matches the daemon's: search all
        # user dirs in multi-user mode, single queue dir otherwise.
        annot = _depends_on_annotation(spec.depends_on, multi_user=multi_user)
        lines.append(
            f"depends_on:   {', '.join(spec.depends_on)} ({annot})"
        )
    # v0.7.8: show depends_on_any (afterany semantics) when set.
    # Predecessor failure is fine here — only "did it reach
    # terminal yet" matters.
    if spec.depends_on_any:
        annot_any = _depends_on_any_annotation(
            spec.depends_on_any, multi_user=multi_user,
        )
        lines.append(
            f"depends_on_any: {', '.join(spec.depends_on_any)} "
            f"({annot_any})"
        )
    if spec.submitter:
        lines.append(f"submitter:    {spec.submitter}")
    if spec.workspace_source:
        lines.append(f"source:       {spec.workspace_source}")
    if spec.pid is not None:
        lines.append(f"pid:          {spec.pid}")
    if spec.started_at:
        started_note = _scheduler_started_note(spec)
        if started_note:
            lines.append(f"started:      {spec.started_at} ({started_note})")
        else:
            lines.append(f"started:      {spec.started_at}")
    if spec.finished_at:
        lines.append(f"finished:     {spec.finished_at}")
    if spec.exit_code is not None:
        lines.append(f"exit_code:    {describe_exit_code(spec.exit_code)}")
    # v0.24: live calculation progress from the .system manifest.
    if not spec.is_terminal:
        progress = _read_progress_from_system(spec, cfg=cfg)
        if progress is not None:
            lines.append(f"progress:     {_format_progress_line(progress)}")
    # v0.6.51: surface the daemon-attributed failure reason when set
    # (currently: depends_on cascade-fail). Conditional so the common
    # exit-code-says-it-all case stays uncluttered.
    if spec.failure_reason:
        lines.append(f"failure:      {spec.failure_reason}")
    # v0.12.0: crash feedback. The captured stderr tail tells the operator
    # WHY a calculation died (traceback / CRYSTAL or ORCA error) inline,
    # no fetch-and-grep. Indented so it reads as the cause of the failure.
    if spec.failure_tail:
        lines.append("stderr tail:")
        lines.extend(f"  {tl}" for tl in spec.failure_tail.splitlines())
    scheduler_missing_marker = _scheduler_missing_marker_event(workspace)
    if scheduler_missing_marker is not None:
        lines.extend(_format_scheduler_missing_marker_summary(scheduler_missing_marker))
    if spec.archive_path:
        lines.append(f"archive:      {spec.archive_path}")
    qvf = _qvf_lifecycle_payload(spec)
    if qvf is not None:
        lines.append(
            "qvf:          "
            f"{qvf.get('run_status') or 'unavailable'} "
            f"(sequence={qvf.get('sequence')}, "
            f"terminal_complete={str(qvf['terminal_complete']).lower()}, "
            f"outcome={qvf['outcome']})"
        )

    if spec.is_archived:
        # Workspace is gone; surface a hint instead of a stack trace from
        # ``_tail_file`` finding no stdout.log.
        lines.extend(
            [
                "",
                "--- stdout ---",
                "(archived; vq cleanup --restore <jobid> to un-tar)",
                "",
                "--- stderr ---",
                "(archived; vq cleanup --restore <jobid> to un-tar)",
            ]
        )
    else:
        lines.extend(
            [
                "",
                "--- stdout ---",
                _tail_file(workspace / spec.stdout_path, tail),
                "",
                "--- stderr ---",
                _tail_file(workspace / spec.stderr_path, tail),
            ]
        )

    # v0.5.10: stamp last_status_at on terminal specs so auto-cleanup
    # policy (v0.6.x) can spare jobs the user looked at recently. Only
    # touch terminal specs to avoid racing the daemon, which is the only
    # writer for non-terminal specs.
    if spec.is_terminal:
        stamp_terminal_status_read(
            spec_path,
            jobid=jobid,
            queue_dir=queue_dir,
            multi_user=multi_user,
        )

    return "\n".join(lines)


def show_status_json(
    host: str,
    jobid: str,
    *,
    tail: int | None = 50,
    queue_dir: Path | None = None,
    multi_user: bool = False,
    cfg: config.Config | None = None,
) -> str:
    """v0.6.14: emit the spec + tailed output as a single JSON object.

    Shape: every JobSpec field at the top level (state, exit_code,
    cwd, submitted_at, ...), plus a ``stdout`` / ``stderr`` pair of
    strings (the same tail as ``show_status``). Used by ``vq wait``
    for state polling and by any other tooling that wants a stable
    machine-readable view.

    Same last_status_at stamp side-effect as the text path so the
    two formats stay equivalent for cleanup-skip purposes.
    """
    if not is_local_host(host):
        raise NotImplementedError(
            f"remote status for {host!r} not implemented in show_status_json"
        )
    spec_path, spec = resolve_authorized_spec(
        jobid,
        queue_dir=queue_dir,
        multi_user=multi_user,
    )
    if not multi_user:
        # Keep the queue snapshot used to find the spec coherent with the
        # pending census below.
        queue_dir = spec_path.parent
    spec, scheduler_refresh = _refresh_and_reread_scheduler_spec(
        jobid,
        spec_path=spec_path,
        spec=spec,
        queue_dir=queue_dir,
        multi_user=multi_user,
    )
    workspace = Path(spec.cwd)
    capacity_snapshot, configured_overages = _pending_capacity_context(
        spec,
        multi_user=multi_user,
    )
    payload = spec.model_dump(mode="json")
    payload["pending_over_capacity"] = (
        True
        if configured_overages
        else (
            False
            if pending_configured_capacity_known(spec, capacity_snapshot)
            else None
        )
    )
    payload["configured_capacity_overages"] = [
        overage.to_payload() for overage in configured_overages
    ]
    if scheduler_refresh is not None:
        payload["scheduler_refresh"] = scheduler_refresh
    payload["effective_state"] = effective_queue_state(spec)
    payload["scheduler_running_confirmed"] = scheduler_running_confirmed(spec)
    payload["queue_handle"] = _queue_handle_payload(spec, host)
    payload.update(monitoring_payload_for_spec(spec))
    # Keep the established checkpoint-QVF ``progress`` field intact while
    # exposing the calculation manifest's live progress under an unambiguous
    # additive key. Text and JSON now consume the same normalized source.
    payload["calculation_progress"] = _read_progress_from_system(spec, cfg=cfg)
    current_pause = _current_pause_seconds(spec)
    payload["paused_current_seconds"] = current_pause
    paused_effective = round(spec.paused_seconds_total + (current_pause or 0.0), 3)
    payload["paused_effective_seconds"] = paused_effective
    payload.update(
        _runtime_accounting_payload(
            spec,
            paused_effective_seconds=paused_effective,
        )
    )
    scheduler_missing_marker = None
    if spec.scheduler_target is not None:
        # ``scheduler_job_id`` is the canonical JobSpec field. Keep the shorter
        # alias for campaign/dashboard clients that historically called it
        # ``scheduler_id``; both must always carry the same persisted handle.
        payload.update(scheduler_status_projection_for_spec(spec))
        scheduler_missing_marker = _scheduler_missing_marker_event(workspace)
        if scheduler_missing_marker is not None:
            payload["scheduler_missing_marker_event"] = scheduler_missing_marker
    payload["terminal_diagnosis"] = terminal_diagnosis_for_spec(
        spec,
        scheduler_missing_marker=scheduler_missing_marker,
    )
    if spec.state == JobState.PENDING:
        with contextlib.suppress(Exception):
            from vq.listing import list_jobs

            all_specs = list_jobs(host, queue_dir=queue_dir, multi_user=multi_user)
            blockers = pending_blockers(
                spec,
                all_specs,
                multi_user=multi_user,
                capacity_snapshot=capacity_snapshot,
            )
            if blockers:
                payload["pending_admission_reason"] = "; ".join(blockers)
            # The full list, since blockers stack. `pending_admission_reason`
            # stays a joined string for existing consumers (vq wait reads it).
            payload["pending_blockers"] = blockers
    if spec.is_archived:
        payload["stdout"] = (
            "(archived; vq cleanup --restore <jobid> to un-tar)"
        )
        payload["stderr"] = (
            "(archived; vq cleanup --restore <jobid> to un-tar)"
        )
    else:
        payload["stdout"] = _tail_file(workspace / spec.stdout_path, tail)
        payload["stderr"] = _tail_file(workspace / spec.stderr_path, tail)
    # v0.6.51: surface the resolved dispatch-readiness annotation
    # alongside the raw depends_on list. Same shape as the text
    # status's "(ready/waiting/failed/unresolved/?)" suffix.
    if spec.depends_on:
        payload["depends_on_status"] = _depends_on_annotation(
            spec.depends_on, multi_user=multi_user,
        )
    # v0.7.8: same shape for depends_on_any.
    if spec.depends_on_any:
        payload["depends_on_any_status"] = _depends_on_any_annotation(
            spec.depends_on_any, multi_user=multi_user,
        )
    if spec.is_terminal:
        stamp_terminal_status_read(
            spec_path,
            jobid=jobid,
            queue_dir=queue_dir,
            multi_user=multi_user,
        )
    return json.dumps(payload, indent=2, sort_keys=True, default=str)


# v0.6.50: the tail helper lives in vq.logs (shared between vq
# status and the new vq logs verb). The import at the top of this
# module aliases it to ``_tail_file`` so the long-standing usage
# below (lines that call ``_tail_file(...)``) keeps working.
# Re-exported here for any external caller that imports it from
# ``vq.status`` directly.
__all__ = [
    "show_status",
    "show_status_json",
    "monitoring_payload_for_spec",
    "scheduler_status_projection_for_spec",
    "terminal_diagnosis_for_spec",
    "_tail_file",
]


_LOCAL_ACTIVE_STATES = {JobState.RUNNING, JobState.SUSPENDED}
_SCHEDULER_ACTIVE_STATES = {
    JobState.RUNNING,
    JobState.SUSPENDED,
    JobState.SUBMITTING,
    JobState.SUBMIT_OUTCOME_UNKNOWN,
}


def _queue_handle_payload(spec: JobSpec, host: str) -> dict[str, str | None]:
    """Stable back-reference for cockpit clients and QVF metadata."""
    return queue_handle_for_spec(spec, host)


def _current_pause_seconds(spec: JobSpec) -> float | None:
    if spec.state != JobState.SUSPENDED:
        return None
    if spec.paused_monotonic_at is not None:
        return round(max(0.0, time.monotonic() - spec.paused_monotonic_at), 3)
    if not spec.paused_at:
        return None
    with contextlib.suppress(ValueError):
        paused = dt.datetime.fromisoformat(spec.paused_at.replace("Z", "+00:00"))
        if paused.tzinfo is None:
            paused = paused.replace(tzinfo=dt.UTC)
        now = dt.datetime.now(dt.UTC)
        return round(
            max(0.0, (now - paused.astimezone(dt.UTC)).total_seconds()),
            3,
        )
    return None


def _runtime_accounting_payload(
    spec: JobSpec,
    *,
    paused_effective_seconds: float,
) -> dict[str, float | int | None]:
    """Machine-readable elapsed-time fields for long-job monitors.

    ``wall_elapsed_seconds`` is clock time from ``started_at`` to
    ``finished_at`` or now. ``active_elapsed_seconds`` subtracts accumulated
    vq pause/admin-update time. Scheduler walltime fields are parsed directly
    from qstat-style HH:MM:SS strings so clients do not need to duplicate that
    parser.
    """
    wall_elapsed = _job_wall_elapsed_seconds(spec)
    active_elapsed = (
        round(max(0.0, wall_elapsed - paused_effective_seconds), 3)
        if wall_elapsed is not None
        else None
    )
    active_walltime_percent = (
        int(round(100 * active_elapsed / spec.wall_time_seconds))
        if active_elapsed is not None
        and spec.wall_time_seconds is not None
        and spec.wall_time_seconds > 0
        else None
    )
    scheduler_used = (
        _hms_to_seconds(spec.scheduler_walltime_used)
        if spec.scheduler_walltime_used
        else None
    )
    scheduler_limit = (
        _hms_to_seconds(spec.scheduler_walltime_limit)
        if spec.scheduler_walltime_limit
        else None
    )
    scheduler_percent = (
        int(round(100 * scheduler_used / scheduler_limit))
        if scheduler_used is not None
        and scheduler_limit is not None
        and scheduler_limit > 0
        else None
    )
    scheduler_remaining = (
        max(0, scheduler_limit - scheduler_used)
        if scheduler_used is not None and scheduler_limit is not None
        else None
    )
    return {
        "wall_elapsed_seconds": wall_elapsed,
        "active_elapsed_seconds": active_elapsed,
        "active_walltime_percent": active_walltime_percent,
        "scheduler_walltime_used_seconds": scheduler_used,
        "scheduler_walltime_limit_seconds": scheduler_limit,
        "scheduler_walltime_percent": scheduler_percent,
        "scheduler_walltime_remaining_seconds": scheduler_remaining,
    }


def _job_wall_elapsed_seconds(spec: JobSpec) -> float | None:
    started = _parse_iso_utc(spec.started_at)
    if started is None:
        return None
    finished = _parse_iso_utc(spec.finished_at) if spec.finished_at else None
    end = finished or dt.datetime.now(dt.UTC)
    return round(max(0.0, (end - started).total_seconds()), 3)


def _read_progress_from_system(
    spec: JobSpec,
    *,
    cfg: config.Config | None = None,
) -> dict[str, object] | None:
    """Read the ``[progress]`` section from ``{stem}.system`` in the
    selected local or live scheduler workspace. Returns ``None`` when the
    manifest is absent, unreadable, or has no progress section."""
    progress = read_system_progress(spec, cfg=cfg)
    if progress is None:
        return None
    normalized = dict(progress)
    for canonical, live_alias in (
        ("iteration", "iter"),
        ("energy_eh", "energy"),
        ("gradient_norm", "grad"),
        ("diis_subspace", "diis"),
    ):
        if canonical not in normalized and live_alias in normalized:
            normalized[canonical] = normalized[live_alias]
    return normalized


def _format_progress_line(progress: dict[str, object]) -> str:
    """Format a ``[progress]`` dict as a one-line status string."""
    parts: list[str] = []
    phase = progress.get("phase")
    if isinstance(phase, str):
        parts.append(str(phase))
    iteration = progress.get("iteration")
    if not isinstance(iteration, (int, float)):
        iteration = progress.get("iter")
    if isinstance(iteration, (int, float)):
        parts.append(f"iter {int(iteration)}")
    energy = progress.get("energy_eh")
    if not isinstance(energy, (int, float)):
        energy = progress.get("energy")
    if isinstance(energy, (int, float)):
        parts.append(f"E={float(energy):.10f} Ha")
    grad = progress.get("gradient_norm")
    if not isinstance(grad, (int, float)):
        grad = progress.get("grad")
    if isinstance(grad, (int, float)):
        parts.append(f"|grad|={float(grad):.2e}")
    return "  ".join(parts) if parts else "(no progress data)"


def _parse_iso_utc(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    with contextlib.suppress(ValueError):
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.UTC)
        return parsed.astimezone(dt.UTC)
    return None


def terminal_diagnosis_for_spec(
    spec: JobSpec,
    *,
    scheduler_missing_marker: dict[str, object] | None = None,
) -> dict[str, object] | None:
    """Structured interpretation of terminal state for automation.

    This does not create new lifecycle states; it packages the interpretation
    already documented for release-paper operators so clients can decide
    whether the next action is a resource change, queue diagnostic, or output
    inspection without scraping human status text.
    """
    if not spec.is_terminal:
        return None

    scheduler_submit_failed = (
        spec.state == JobState.FAILED
        and spec.scheduler_target is not None
        and _reason_mentions_scheduler_submit(spec.failure_reason)
    )
    signal_name = (
        None if scheduler_submit_failed else _terminal_signal_name(spec.exit_code)
    )
    payload: dict[str, object] = {
        "category": "unknown",
        "action_hint": "inspect_outputs",
        "summary": "Terminal state needs output inspection.",
    }
    if spec.exit_code is not None:
        payload["exit_code_description"] = describe_exit_code(spec.exit_code)
    if signal_name is not None:
        payload["signal"] = signal_name
    if spec.failure_reason:
        payload["reason"] = spec.failure_reason

    if spec.state == JobState.COMPLETED:
        payload.update(
            category="completed",
            action_hint="none",
            summary="Job completed normally.",
        )
    elif spec.state == JobState.TIME_EXCEEDED:
        payload.update(
            category=(
                "scheduler_walltime"
                if spec.scheduler_target is not None
                else "vq_walltime"
            ),
            action_hint="increase_walltime",
            summary=(
                "Scheduler walltime limit was reached."
                if spec.scheduler_target is not None
                else "vq walltime limit was reached."
            ),
        )
    elif spec.state == JobState.OOM_KILLED:
        payload.update(
            category="oom_killed",
            action_hint="increase_memory",
            summary="vq watchdog classified the job as out-of-memory killed.",
        )
    elif spec.state == JobState.STARVED:
        payload.update(
            category="resource_starved",
            action_hint="increase_resources_or_reduce_host_load",
            summary="vq watchdog classified the job as resource-starved.",
        )
    elif spec.state == JobState.KILLED:
        payload.update(
            category="manual_kill",
            action_hint="operator_killed",
            summary="Job was killed by vq/operator request.",
        )
    elif spec.state == JobState.INTERRUPTED:
        payload.update(
            category="interrupted",
            action_hint="queue_diagnostics",
            summary="Job was interrupted before vq could classify a normal exit.",
        )
    elif spec.state == JobState.ABORTED_BY_QUEUE:
        missing_marker = (
            scheduler_missing_marker is not None
            or _reason_mentions_exit_marker(spec.failure_reason)
        )
        payload.update(
            category=(
                "scheduler_missing_exit_marker"
                if spec.scheduler_target is not None and missing_marker
                else "queue_aborted"
            ),
            action_hint="queue_diagnostics",
            summary=(
                "Scheduler job finished but vq could not recover an exit marker."
                if spec.scheduler_target is not None and missing_marker
                else "vq aborted the job because queue/runtime evidence was incomplete."
            ),
        )
    elif spec.state == JobState.FAILED:
        if scheduler_submit_failed:
            payload.update(
                category="scheduler_submit_failed",
                action_hint="inspect_scheduler_request_and_host_config",
                summary=(
                    "The scheduler rejected or could not accept the job before "
                    "a scheduler job ID was assigned."
                ),
            )
        elif signal_name == "SIGKILL":
            payload.update(
                category="sigkill",
                action_hint="increase_memory_or_check_external_kill",
                summary=(
                    "Command died from SIGKILL; treat as resource or external "
                    "process-manager kill until logs prove otherwise."
                ),
            )
        elif signal_name is not None:
            payload.update(
                category="signal_exit",
                action_hint="inspect_outputs_and_host_logs",
                summary=f"Command died from {signal_name}.",
            )
        else:
            payload.update(
                category="command_failed",
                action_hint="inspect_outputs",
                summary="Command exited non-zero; inspect stdout/stderr.",
            )
    return payload


def _reason_mentions_exit_marker(reason: str | None) -> bool:
    return bool(reason and "exit-marker" in reason.lower())


def _reason_mentions_scheduler_submit(reason: str | None) -> bool:
    if not reason:
        return False
    lowered = reason.lower()
    return any(
        marker in lowered
        for marker in (
            "scheduler submit",
            "qsub to",
            "qsub failed",
            "sbatch failed",
        )
    )


def _terminal_signal_name(exit_code: int | None) -> str | None:
    encoded = signal_name_for_exit(exit_code)
    if encoded is not None:
        return encoded
    if exit_code is None or exit_code >= 0:
        return None
    return {
        1: "SIGHUP",
        2: "SIGINT",
        3: "SIGQUIT",
        4: "SIGILL",
        5: "SIGTRAP",
        6: "SIGABRT",
        8: "SIGFPE",
        9: "SIGKILL",
        11: "SIGSEGV",
        13: "SIGPIPE",
        14: "SIGALRM",
        15: "SIGTERM",
    }.get(abs(exit_code))


def _format_seconds(seconds: float) -> str:
    seconds_i = int(round(seconds))
    if seconds_i < 60:
        return f"{seconds_i}s"
    minutes, sec = divmod(seconds_i, 60)
    if minutes < 60:
        return f"{minutes}m{sec:02d}s"
    hours, minute = divmod(minutes, 60)
    return f"{hours}h{minute:02d}m{sec:02d}s"


def monitoring_payload_for_spec(spec: JobSpec) -> dict[str, object]:
    """Stable additive fields for cockpit-style live monitors.

    ``runtime_workdir`` means the value the job sees as ``$VQ_WORKDIR``.
    For local jobs that is the persisted ``spec.workdir``. For scheduler jobs
    the daemon injects the scheduler remote workspace instead, so derive it
    from the scheduler host config when available.
    """
    runtime_workdir = _runtime_workdir(spec)
    checkpoint_path = (
        f"{runtime_workdir.rstrip('/')}/{CHECKPOINT_QVF_FILENAME}"
        if runtime_workdir
        else None
    )
    checkpoint_exists: bool | None
    if spec.workdir and checkpoint_path:
        checkpoint_exists = Path(checkpoint_path).is_file()
    else:
        checkpoint_exists = None
    progress = (
        _progress_from_checkpoint_qvf(Path(checkpoint_path))
        if checkpoint_exists and checkpoint_path
        else None
    )
    return {
        "progress": progress,
        "qvf_lifecycle": _qvf_lifecycle_payload(spec),
        "runtime_workdir": runtime_workdir,
        "checkpoint_qvf_filename": CHECKPOINT_QVF_FILENAME,
        "checkpoint_qvf_path": checkpoint_path,
        "checkpoint_qvf_exists": checkpoint_exists,
    }


# Preserve the prior private import surface while consumers migrate to the
# public name.  Milestone 8 changes ownership visibility, not payload behavior.
_monitoring_payload = monitoring_payload_for_spec


def _qvf_lifecycle_payload(spec: JobSpec) -> dict[str, object] | None:
    """Inspect a first-class QVF payload without importing vibe-qc.

    A queue process is not a successful QVF result merely because it exited
    zero: a settled container must have a terminal provenance status and a
    complete, sequenced run.record containing both the exact input and the
    full (possibly empty) log.  Conversely, a complete ``failed`` container is
    a chemistry failure represented by the application protocol, not an
    opaque queue-process failure.
    """
    name = spec.qvf_artifact_name
    if name is None:
        return None
    payload: dict[str, object] = {
        "artifact_name": name,
        "exists": False,
        "run_status": None,
        "sequence": None,
        "run_record_complete": False,
        "terminal_complete": False,
        "queue_terminal": spec.is_terminal,
        "queue_process_failed": False,
        "chemistry_failed": False,
        "done": False,
        "outcome": "unavailable",
    }
    if spec.is_archived:
        payload["outcome"] = "archived_uninspected"
        return payload
    path = Path(spec.cwd) / name
    if path.is_symlink() or not path.is_file():
        payload["queue_process_failed"] = spec.is_terminal
        payload["outcome"] = (
            "queue_process_failed" if spec.is_terminal else "unavailable"
        )
        return payload
    payload["exists"] = True
    try:
        with zipfile.ZipFile(path) as zf:
            manifest = json.loads(zf.read("manifest.json"))
            provenance = manifest.get("provenance", {})
            run_status = provenance.get("run_status")
            if run_status in {"pending", "running", "converged", "failed"}:
                payload["run_status"] = run_status
            records = [
                section
                for section in manifest.get("sections", [])
                if isinstance(section, dict)
                and section.get("kind") == "run.record"
            ]
            sequenced = [
                record
                for record in records
                if isinstance(record.get("sequence"), int)
                and int(record["sequence"]) >= 0
            ]
            if sequenced:
                record = max(
                    sequenced, key=lambda item: int(item["sequence"])
                )
                payload["sequence"] = int(record["sequence"])
                members = record.get("members")
                complete = bool(record.get("program")) and isinstance(
                    members, dict
                )
                for role in ("input", "log"):
                    member = members.get(role) if isinstance(members, dict) else None
                    if not isinstance(member, dict):
                        complete = False
                        continue
                    member_path = member.get("path")
                    expected_sha = member.get("sha256")
                    if (
                        not isinstance(member_path, str)
                        or not isinstance(expected_sha, str)
                    ):
                        complete = False
                        continue
                    try:
                        digest = hashlib.sha256()
                        with zf.open(member_path) as member_file:
                            for chunk in iter(
                                lambda: member_file.read(1024 * 1024), b""
                            ):
                                digest.update(chunk)
                        actual_sha = digest.hexdigest()
                    except KeyError:
                        complete = False
                        continue
                    if actual_sha != expected_sha:
                        complete = False
                payload["run_record_complete"] = complete
    except (
        OSError,
        KeyError,
        TypeError,
        ValueError,
        UnicodeDecodeError,
        zipfile.BadZipFile,
    ) as exc:
        payload["error"] = f"{type(exc).__name__}: {exc}"

    terminal_status = payload["run_status"] in {"converged", "failed"}
    terminal_complete = bool(
        terminal_status and payload["run_record_complete"]
    )
    payload["terminal_complete"] = terminal_complete
    payload["chemistry_failed"] = bool(
        payload["run_status"] == "failed" and terminal_complete
    )
    payload["queue_process_failed"] = bool(
        spec.is_terminal
        and (
            spec.state != JobState.COMPLETED
            or not terminal_complete
        )
        and not payload["chemistry_failed"]
    )
    payload["done"] = bool(spec.is_terminal and terminal_complete)
    if payload["done"]:
        payload["outcome"] = (
            "chemistry_failed"
            if payload["chemistry_failed"]
            else "converged"
        )
    elif payload["queue_process_failed"]:
        payload["outcome"] = "queue_process_failed"
    elif payload["run_status"] in {"pending", "running"}:
        payload["outcome"] = payload["run_status"]
    elif terminal_status:
        payload["outcome"] = "terminal_incomplete"
    else:
        payload["outcome"] = "unavailable"
    return payload


def _progress_from_checkpoint_qvf(path: Path) -> dict[str, object] | None:
    """Read the small live-progress payload from a checkpoint QVF manifest.

    The QVF writer stores live checkpoint metadata under
    ``provenance.checkpoint``. Status polling must stay cheap and robust, so
    only ``manifest.json`` is read and any missing, malformed, or mid-replace
    archive simply reports no structured progress for this poll.
    """
    with contextlib.suppress(
        OSError,
        KeyError,
        TypeError,
        ValueError,
        UnicodeDecodeError,
        zipfile.BadZipFile,
    ):
        with zipfile.ZipFile(path) as zf:
            manifest = json.loads(zf.read("manifest.json"))
        if not isinstance(manifest, dict):
            return None
        provenance = manifest.get("provenance")
        if not isinstance(provenance, dict):
            return None
        checkpoint = provenance.get("checkpoint")
        if not isinstance(checkpoint, dict):
            return None

        progress: dict[str, object] = {"source": "checkpoint_qvf"}
        run_status = provenance.get("run_status")
        if isinstance(run_status, str):
            progress["run_status"] = run_status
        for key in (
            "seq",
            "wall_time_s",
            "written_at",
            "scf_iteration",
            "energy_eh",
        ):
            if key in checkpoint:
                progress[key] = checkpoint[key]
        return progress
    return None


def _runtime_workdir(spec: JobSpec) -> str | None:
    if spec.workdir:
        return spec.workdir
    if spec.scheduler_target is not None:
        return _scheduler_runtime_workdir(spec)
    return None


def _scheduler_runtime_workdir(spec: JobSpec) -> str | None:
    with contextlib.suppress(Exception):
        host_cfg = config.load_config().host(spec.scheduler_target or "")
        return scheduler_dispatcher_for(host_cfg).remote_workspace(spec.id)
    return None


def _status_state_label(spec: JobSpec) -> str:
    """Human label for the local vq lifecycle state.

    Scheduler-backed jobs enter the local RUNNING state when vq successfully
    hands them to qsub/sbatch. That is useful for lifecycle bookkeeping, but a
    bare "running" status is misleading while PBS still reports queued or has
    not been polled yet. Keep the raw state, and annotate the meaning.
    """
    label = spec.state.value
    note = _scheduler_running_state_note(spec)
    if note:
        return f"{label} ({note})"
    return label


def _scheduler_running_state_note(spec: JobSpec) -> str | None:
    if (
        spec.scheduler_target is None
        or spec.state != JobState.RUNNING
        or spec.is_terminal
    ):
        return None
    scheduler_state = effective_queue_state(spec)
    if scheduler_state == "running":
        return None
    if scheduler_state == "queued":
        return "submitted to scheduler; scheduler queued"
    if scheduler_state == "held":
        return "submitted to scheduler; scheduler held"
    if scheduler_state == "poll_failed":
        return "scheduler poll failed; daemon will retry"
    if scheduler_state == "finishing":
        return "scheduler finished; exit marker and final artifacts pending"
    if scheduler_state == "marker_probe_failed":
        return "scheduler exit-marker probe failed; daemon will retry"
    if scheduler_state == "fetch_failed":
        return "scheduler output fetch failed; daemon will retry"
    if scheduler_state == "reattach_failed":
        return "scheduler reattach failed; scheduler job untracked"
    if scheduler_state == "unpolled":
        return "submitted to scheduler; scheduler unpolled"
    if scheduler_state == "scheduler_unknown":
        return "scheduler phase unrecognized; inspect raw JSON"
    return f"submitted to scheduler; scheduler {scheduler_state}"


def _scheduler_started_note(spec: JobSpec) -> str | None:
    if _scheduler_running_state_note(spec):
        return "scheduler handoff time, not node start"
    return None


def _pending_admission_reason(
    spec: JobSpec,
    all_specs: list[JobSpec],
    *,
    multi_user: bool = False,
) -> str | None:
    """Best-effort explanation for a local pending job that cannot dispatch.

    This mirrors the daemon's dispatch gates using observable state: advertised
    daemon capacity, drain state, and visible RUNNING/SUSPENDED specs. It is
    intentionally conservative. If a gate cannot be proven from those files,
    return None rather than guessing.
    """
    reasons = pending_blockers(spec, all_specs, multi_user=multi_user)
    return "; ".join(reasons) if reasons else None


def pending_blockers(
    spec: JobSpec,
    all_specs: list[JobSpec],
    *,
    multi_user: bool = False,
    capacity_snapshot: (
        capacity.DaemonCapacity | None | _CapacitySnapshotUnset
    ) = _CAPACITY_SNAPSHOT_UNSET,
) -> list[str]:
    """Every dispatch gate we can PROVE is holding this pending job.

    The daemon has sixteen ways to leave a job PENDING; this reproduces the
    ones establishable from observable state (spec fields, drain.json, the
    admin-update marker, the advertised daemon capacity). It reports *all*
    of them rather than the first, because they genuinely stack — a job can be
    behind both a drain and its own dependencies, and fixing one leaves it
    parked on the other.

    Conservative by construction: a gate that cannot be proven is omitted
    rather than guessed. An empty list therefore means "nothing provable is
    holding it", NOT "nothing is holding it" — the daemon-internal gates
    (host-pressure, per-user quota under an unreadable queue dir) are invisible
    from here. Callers must not render an empty list as "ready to run".
    """
    if spec.state != JobState.PENDING:
        return []
    reasons: list[str] = []

    # --- gates that need no capacity file -------------------------------
    # A protected runtime is mid-rebuild. This is the single most confusing
    # PENDING for a submitting chat: nothing is wrong with the job, the daemon
    # is holding the affected dispatch target while that runtime is mutated.
    marker = _admin_update_hold_reason(spec.scheduler_target)
    if marker:
        reasons.append(marker)
    if spec.not_before and not _not_before_passed(spec.not_before):
        label = "retry backoff" if spec.retry_count > 0 else "scheduled submit"
        reasons.append(f"held until {spec.not_before} ({label})")
    if spec.refresh_before is not None:
        reasons.append(
            f"waiting on a rebuild of {spec.refresh_before!r} (--refresh); it "
            "dispatches once that build job completes"
        )
    unmet = _unmet_dependencies(spec, all_specs)
    if unmet:
        reasons.append(
            f"waiting on {len(unmet)} dependenc(y/ies): {', '.join(unmet)}"
        )
    building = sorted(
        {
            s.build_env
            for s in all_specs
            if s.build_env is not None and s.state == JobState.RUNNING
        }
    )
    if building:
        reasons.append(
            f"a build job is running for {', '.join(building)}; dispatch is "
            "held until it finishes so nothing runs against a half-built env"
        )

    legacy_drain, scheduler_leases, _lease_error, drain_state = (
        drain.read_effective_drain_snapshot()
    )
    if drain_state and drain_state.enabled and drain_state.is_full_drain:
        suffix = f": {drain_state.reason}" if drain_state.reason else ""
        if drain_state.update_mode == "deny" or drain_state.reject_submits:
            reasons.append(
                "paused for update; daemon is not dispatching and new "
                f"submissions are denied{suffix}"
            )
        elif drain_state.update_mode == "accept":
            reasons.append(
                "paused for update; daemon is not dispatching, but new "
                f"submissions are accepted for later{suffix}"
            )
        else:
            reasons.append(
                f"operator drain is active{suffix}; daemon is not dispatching"
            )

    if (
        spec.scheduler_target is not None
        and drain_state is not None
        and drain_state.drains_scheduler_target(spec.scheduler_target)
    ):
        target = spec.scheduler_target
        attributions: list[str] = []
        if (
            legacy_drain is not None
            and target in legacy_drain.scheduler_hosts
        ):
            attributions.append(
                legacy_drain.reason or "legacy hold"
            )
        for lease in scheduler_leases:
            if lease.scheduler_host != target:
                continue
            attributions.append(
                f"{lease.owner}: {lease.reason}"
                if lease.reason
                else f"owned by {lease.owner}"
            )
        suffix = f": {'; '.join(attributions)}" if attributions else ""
        reasons.append(
            "scheduler-target drain is active for "
            f"{normalized_scheduler_target(spec)}; daemon is not dispatching this "
            f"scheduler lane{suffix}"
        )

    if isinstance(capacity_snapshot, _CapacitySnapshotUnset):
        cap = capacity.read_daemon_capacity(multi_user=multi_user)
    else:
        cap = capacity_snapshot
    if cap is None:
        return reasons

    if spec.scheduler_target is not None:
        if cap.max_scheduler_jobs is None:
            return reasons
        active = [
            s
            for s in all_specs
            if s.scheduler_target is not None
            and s.state in _SCHEDULER_ACTIVE_STATES
        ]
        scheduler_active = len(active)
        if scheduler_active >= cap.max_scheduler_jobs:
            reasons.append(
                "scheduler job-count admission blocked: "
                f"{scheduler_active}/{cap.max_scheduler_jobs} "
                "scheduler jobs already active"
            )
        return reasons

    configured_overages = capacity.configured_capacity_overages(
        cpus=spec.cpus,
        mem_mb=spec.mem_mb,
        snapshot=cap,
    )
    overage_resources = {overage.resource for overage in configured_overages}
    for overage in configured_overages:
        if overage.resource == "cpus":
            reasons.append(
                "CPU request exceeds configured cap: "
                f"requested {overage.requested} CPUs, cap is {overage.limit}; "
                "cannot dispatch until the daemon cap changes or the job is "
                "resubmitted"
            )
        else:
            requested = (
                f"charged {overage.requested} MB (daemon default for an "
                "undeclared job)"
                if overage.uses_default
                else f"requested {overage.requested} MB"
            )
            reasons.append(
                "memory request exceeds configured cap: "
                f"{requested}, cap is {overage.limit} MB; cannot dispatch "
                "until the daemon cap changes or the job is resubmitted"
            )

    effective_max_jobs = cap.max_jobs
    effective_max_cpus = cap.max_cpus
    if drain_state and drain_state.enabled:
        effective_max_jobs = drain_state.effective_max_jobs(cap.max_jobs)
        effective_max_cpus = drain_state.effective_max_cpus(cap.max_cpus)

    active = [
        s
        for s in all_specs
        if s.scheduler_target is None and s.state in _LOCAL_ACTIVE_STATES
    ]
    local_active = len(active)
    if effective_max_jobs is not None and local_active >= effective_max_jobs:
        reasons.append(
            "local job-count admission blocked: "
            f"{local_active}/{effective_max_jobs} local jobs already active"
        )

    used_cpus = sum(s.cpus for s in active)
    if (
        "cpus" not in overage_resources
        and used_cpus + spec.cpus > effective_max_cpus
    ):
        remaining = max(0, effective_max_cpus - used_cpus)
        reasons.append(
            "CPU admission blocked: "
            f"requested {spec.cpus} CPUs, {remaining} CPUs available "
            f"of {effective_max_cpus} local cap"
        )

    # Mirror the daemon's _eff_mem_mb: an UNDECLARED job is charged the
    # daemon's default_job_mem_mb, not zero. Reading raw `mem_mb or 0` and
    # skipping specs with mem_mb=None under-counted the tally and dropped the
    # memory reason entirely for exactly the jobs it applies to. The default is
    # a daemon-CLI-only value, so it is now advertised in daemon_capacity.json.
    if cap.max_mem_mb is not None and "memory" not in overage_resources:
        charge = _effective_mem_mb(spec, cap)
        if charge is not None:
            used_mem = sum(_effective_mem_mb(s, cap) or 0 for s in active)
            if used_mem + charge > cap.max_mem_mb:
                remaining = max(0, cap.max_mem_mb - used_mem)
                declared = (
                    f"requested {charge} MB"
                    if spec.mem_mb is not None
                    else f"charged {charge} MB (daemon default for an "
                    "undeclared job)"
                )
                reasons.append(
                    "memory admission blocked: "
                    f"{declared}, {remaining} MB available "
                    f"of {cap.max_mem_mb} MB local cap"
                )
    return reasons


def _effective_mem_mb(spec: JobSpec, cap: capacity.DaemonCapacity) -> int | None:
    """What the daemon's memory gate charges this spec (``daemon._eff_mem_mb``)."""
    if spec.mem_mb is not None:
        return spec.mem_mb
    return cap.default_job_mem_mb


def _admin_update_hold_reason(scheduler_target: str | None) -> str | None:
    """Is an effective admin-update marker holding this dispatch target?

    Mirrors the daemon's gate, including its two subtleties: an *unreadable*
    marker also holds dispatch, and the daemon reaps only ordinary stale
    corpses. Stale durable transaction and pause receipts keep holding their
    scope until recovery. Recognised marker scopes use the same parser as the
    daemon so an update on one scheduler host is never reported against
    another host's pending job. Best-effort — this is a diagnostic, never a
    correctness path.
    """
    global_hold = (
        "an admin-update marker is present but unreadable or unrecognised; "
        "the daemon holds all dispatch until it is cleared "
        "(`vq admin clear-update-marker`)"
    )
    marker_present = False
    try:
        from vq import admin as admin_mod

        marker_present = admin_mod.admin_update_marker_exists()
        if not marker_present:
            return None
        dispatch_target = (
            scheduler_target
            if scheduler_target is not None
            else admin_mod.LOCAL_DISPATCH_SCOPE
        )
        applicable = []
        stale_applicable = []
        live_applicable = []
        for _, marker in admin_mod._admin_update_marker_entries():
            if marker is None:
                return global_hold
            stale = admin_mod.admin_update_marker_stale_reason(marker)
            if (
                stale is not None
                and marker.managed_transaction is None
                and not marker.owns_pause_scope
            ):
                # The daemon auto-reaps an ordinary corpse before computing
                # its effective hold scope.
                continue
            scope = admin_mod.admin_update_marker_scope(marker)
            if scope is None:
                return global_hold
            if dispatch_target in scope:
                applicable.append(marker)
                if stale is not None:
                    stale_applicable.append(marker)
                else:
                    live_applicable.append(marker)
        if not applicable:
            return None
        if stale_applicable:
            stale_envs = ", ".join(
                env for marker in stale_applicable for env in marker.envs
            )
            live_detail = ""
            if live_applicable:
                live_envs = ", ".join(
                    env for marker in live_applicable for env in marker.envs
                )
                live_detail = (
                    f"; an admin update is also in progress ({live_envs}) and "
                    "holds the same dispatch target"
                )
            return (
                "a stale durable admin-update recovery receipt is holding "
                f"affected dispatch ({stale_envs}); the updater is no longer "
                "running and explicit recovery is required (`vq admin status`)"
                f"{live_detail}"
            )
        envs = ", ".join(env for marker in applicable for env in marker.envs)
        states = ", ".join(sorted({str(marker.state) for marker in applicable}))
        return (
            f"an admin update is in progress ({envs}, states={states}); "
            "the daemon holds affected dispatch while environments are rebuilt "
            "(`vq admin status`)"
        )
    except Exception:  # noqa: BLE001 — a diagnostic must never break status
        return global_hold if marker_present else None


def _not_before_passed(not_before: str) -> bool:
    """Has a spec's ``not_before`` gate elapsed?

    Matches ``daemon._not_before_ready``: anything unparseable — including a
    naive, timezone-less timestamp — counts as ready now. Reporting a phantom
    block for a spec the daemon would happily dispatch is worse than silence.
    """
    try:
        parsed = dt.datetime.fromisoformat(not_before.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return True
        return parsed <= dt.datetime.now(dt.UTC)
    except (ValueError, TypeError):
        return True


def _unmet_dependencies(spec: JobSpec, all_specs: list[JobSpec]) -> list[str]:
    """Dependencies still holding this spec, per the daemon's two rules.

    ``depends_on`` needs every predecessor COMPLETED; ``depends_on_any`` only
    needs them terminal. An unknown jobid is treated as unmet — the daemon
    cannot dispatch against a predecessor it cannot see either.
    """
    by_id = {s.id: s for s in all_specs}
    return [
        f"{dependency_id}({state.value if state is not None else 'unknown'})"
        for dependency_id, state in admission.iter_unmet_dependencies(spec, by_id)
    ]


def _scheduler_state_label(spec: JobSpec) -> str:
    """Last scheduler phase, without implying terminal jobs are still live."""
    raw = normalized_scheduler_phase(spec)
    if spec.is_terminal:
        return f"{raw} (last observed before local {spec.state.value})"
    return raw


def _scheduler_fetch_state_label(spec: JobSpec) -> str:
    """Human fetch/reconcile phase for scheduler-backed workspaces."""
    if spec.state == JobState.SUBMITTING:
        return "scheduler submit in progress; automatic replay is disabled"
    if spec.state == JobState.SUBMIT_OUTCOME_UNKNOWN:
        return "scheduler submit outcome unknown; reconcile exact receipt before retry"
    if (
        not spec.is_terminal
        and effective_queue_state(spec) == "scheduler_unknown"
    ):
        return "scheduler phase or ownership unknown; remote workspace state unknown"
    if spec.scheduler_state == "poll_failed" and not spec.is_terminal:
        return "scheduler poll failed; remote workspace state unknown"
    if spec.scheduler_state == "finishing" and not spec.is_terminal:
        return "waiting for exit-marker/final artifact collection"
    if spec.scheduler_state == "marker_probe_failed" and not spec.is_terminal:
        return "exit-marker probe failed; daemon will retry"
    if spec.scheduler_state == "fetch_failed" and not spec.is_terminal:
        return "workspace fetch failed; daemon will retry"
    if spec.scheduler_state == "reattach_failed" and not spec.is_terminal:
        return "scheduler job untracked; repair config or scheduler_job_id"
    if spec.scheduler_state == "artifacts_unavailable" and not spec.is_terminal:
        return "scheduler artifacts unavailable; inspect event evidence"
    if (
        not spec.is_terminal
        and normalized_scheduler_phase(spec)
        in {
            "hold_outcome_unknown",
            "scheduler_reconciliation_quarantined",
            "release_outcome_unknown",
            "scheduler_unknown",
        }
    ):
        return "scheduler phase or ownership unknown; remote workspace state unknown"
    if spec.is_terminal:
        # Only last_fetched_at is evidence that a fetch ran. Falling back to
        # finished_at claimed "workspace staged locally" for any terminal spec
        # — including one `vq kill` had just stamped, where no fetch ever
        # happened. That reassured the submitter at exactly the moment they
        # most needed to know the remote workspace was never collected.
        if spec.last_fetched_at:
            return f"workspace staged locally at {spec.last_fetched_at}"
        if spec.scheduler_state == "reattach_failed":
            return (
                "terminal locally, but the scheduler job was never tracked and "
                "no fetch ran; the remote workspace was not collected"
            )
        return (
            f"terminal locally at {spec.finished_at}; no fetch recorded, so the "
            "remote workspace may not have been collected"
            if spec.finished_at
            else "terminal locally; fetch timestamp unknown"
        )
    return "live workspace on scheduler host"


def scheduler_status_projection_for_spec(spec: JobSpec) -> dict[str, object]:
    """Return every derived scheduler label from one bounded projection."""
    if spec.scheduler_target is None:
        return {}
    state_label = _scheduler_state_label(spec)
    return {
        "scheduler_id": spec.scheduler_job_id,
        "pbs_state_label": state_label,
        "fetch_state_label": _scheduler_fetch_state_label(spec),
        # Backward-compatible name for clients predating the text UI split.
        "scheduler_status_label": state_label,
    }


def _scheduler_missing_marker_event(workspace: Path) -> dict[str, object] | None:
    """Latest scheduler missing-marker abort event, if this workspace has one."""
    for record in reversed(events.read_events(workspace)):
        if (
            record.get("kind") != events.EventKind.STATE_TRANSITION.value
            or record.get("to") != JobState.ABORTED_BY_QUEUE.value
        ):
            continue
        evidence = record.get("evidence")
        if not isinstance(evidence, dict):
            continue
        reason = str(record.get("reason", ""))
        if (
            "remote_exit_marker" not in evidence
            and "missing_marker_grace_seconds" not in evidence
            and "without an exit-marker" not in reason
        ):
            continue
        return record
    return None


def _format_scheduler_missing_marker_summary(
    record: dict[str, object],
) -> list[str]:
    evidence = record.get("evidence")
    if not isinstance(evidence, dict):
        return []
    lines = ["scheduler diagnostics:"]
    remote_marker = evidence.get("remote_exit_marker")
    local_marker = evidence.get("local_exit_marker")
    if remote_marker or local_marker:
        parts = []
        if remote_marker:
            parts.append(f"remote {remote_marker}")
        if local_marker:
            parts.append(f"local {local_marker}")
        lines.append(f"  missing_marker: {'; '.join(parts)}")
    remote_workspace = evidence.get("remote_workspace")
    if remote_workspace:
        lines.append(f"  remote_workspace: {remote_workspace}")
    scheduler_job_id = evidence.get("scheduler_job_id")
    if scheduler_job_id:
        lines.append(f"  scheduler_job: {scheduler_job_id}")
    qstat_hint = _first_evidence_line(evidence.get("qstat_detail_stderr"))
    if qstat_hint:
        qstat_rc = evidence.get("qstat_detail_rc")
        suffix = f" (rc={qstat_rc})" if qstat_rc is not None else ""
        lines.append(f"  qstat_detail: {qstat_hint}{suffix}")
    remote_stdout = "remote_stdout_tail" in evidence
    remote_stderr = "remote_stderr_tail" in evidence
    local_stdout = "local_stdout_tail" in evidence
    local_stderr = "local_stderr_tail" in evidence
    if remote_stdout or remote_stderr or local_stdout or local_stderr:
        lines.append(
            "  captured_tails: "
            f"remote stdout={'yes' if remote_stdout else 'no'}, "
            f"stderr={'yes' if remote_stderr else 'no'}; "
            f"local stdout={'yes' if local_stdout else 'no'}, "
            f"stderr={'yes' if local_stderr else 'no'}"
        )
    lines.append("  full_evidence: `vq events <jobid>` (the lifecycle timeline)")
    return lines


def _first_evidence_line(value: object, *, limit: int = 160) -> str | None:
    if not isinstance(value, str):
        return None
    first = next((line.strip() for line in value.splitlines() if line.strip()), "")
    if not first:
        return None
    if len(first) > limit:
        return f"{first[: limit - 3]}..."
    return first


def _depends_on_annotation(
    dep_ids: list[str], *, multi_user: bool
) -> str:
    """v0.6.51: render a one-token summary of a depends_on list's
    readiness for the status output. Returns one of:

    * ``"ready"`` — every predecessor is COMPLETED (dispatch
      should fire on the next tick subject to budgets).
    * ``"failed: JOBID"`` — at least one predecessor is in a
      non-COMPLETED terminal state. The dependent should already
      be FAILED by the daemon's cascade pass; the annotation is
      informational for the operator looking at the new state.
    * ``"waiting: JOBID (STATE)"`` — at least one predecessor is
      non-terminal. Shows the first one to give the operator a
      handle.
    * ``"unresolved: JOBID"`` — a predecessor isn't present in
      any visible queue dir (cleanup'd, or cross-user in
      multi-user mode where this caller can't see the other
      user's tree).

    The annotation is best-effort: an OSError or invalid spec on
    disk surfaces as ``"?"`` rather than crashing the status
    output.
    """
    from vq import paths as _paths  # noqa: PLC0415 — avoid circular

    first_waiting: tuple[str, JobState] | None = None
    first_failed: str | None = None
    unresolved: str | None = None
    for did in dep_ids:
        try:
            if multi_user:
                try:
                    p = _paths.resolve_spec_path(did, multi_user=True)
                except FileNotFoundError:
                    if unresolved is None:
                        unresolved = did
                    continue
            else:
                p = _paths.queue_dir() / f"{did}.json"
                if not p.exists():
                    if unresolved is None:
                        unresolved = did
                    continue
            dep_spec = JobSpec.read(p)
        except (OSError, ValueError):
            return "?"
        if dep_spec.state == JobState.COMPLETED:
            continue
        if dep_spec.state in _terminal_states():
            if first_failed is None:
                first_failed = did
            continue
        if first_waiting is None:
            first_waiting = (did, dep_spec.state)
    if first_failed is not None:
        return f"failed: {first_failed}"
    if unresolved is not None:
        return f"unresolved: {unresolved}"
    if first_waiting is not None:
        did, dst = first_waiting
        return f"waiting: {did} ({dst.value})"
    return "ready"


def _terminal_states() -> frozenset[JobState]:
    """Imported lazily to keep the module-import surface small."""
    from vq.spec import TERMINAL_STATES  # noqa: PLC0415
    return TERMINAL_STATES


def _format_scheduler_walltime(used: str | None, limit: str | None) -> str | None:
    """Render qstat walltime detail, adding a percentage when parseable."""
    if used is None and limit is None:
        return None
    if used is not None and limit is not None:
        pct = _walltime_percent(used, limit)
        suffix = f" ({pct}% used)" if pct is not None else ""
        return f"{used} / {limit}{suffix}"
    if used is not None:
        return f"{used} used"
    assert limit is not None
    return f"{limit} limit"


def _format_scheduler_walltime_warning(
    used: str | None,
    limit: str | None,
) -> str | None:
    """Warn when a scheduler-backed run is close to its walltime kill."""
    if used is None or limit is None:
        return None
    used_seconds = _hms_to_seconds(used)
    limit_seconds = _hms_to_seconds(limit)
    if used_seconds is None or limit_seconds is None or limit_seconds <= 0:
        return None

    pct = int(round(100 * used_seconds / limit_seconds))
    remaining = max(0, limit_seconds - used_seconds)
    if used_seconds >= limit_seconds:
        return (
            f"scheduler walltime limit reached ({pct}% used); "
            "cluster may terminate this job imminently"
        )
    if pct < 90:
        return None
    return (
        f"scheduler walltime nearly exhausted ({pct}% used; "
        f"{_seconds_to_hms(remaining)} remaining)"
    )


def _walltime_percent(used: str, limit: str) -> int | None:
    """Percentage for scheduler walltime strings; None for unknown formats."""
    used_seconds = _hms_to_seconds(used)
    limit_seconds = _hms_to_seconds(limit)
    if used_seconds is None or limit_seconds is None or limit_seconds == 0:
        return None
    return int(round(100 * used_seconds / limit_seconds))


def _hms_to_seconds(value: str) -> int | None:
    days = 0
    time_part = value
    if "-" in value:
        day_part, time_part = value.split("-", 1)
        try:
            days = int(day_part)
        except ValueError:
            return None

    parts = time_part.split(":")
    if len(parts) != 3:
        return None
    try:
        hours, minutes, seconds = (int(part) for part in parts)
    except ValueError:
        return None
    if (
        days < 0
        or hours < 0
        or not (0 <= minutes < 60)
        or not (0 <= seconds < 60)
    ):
        return None
    return days * 86400 + hours * 3600 + minutes * 60 + seconds


def _seconds_to_hms(total_seconds: int) -> str:
    hours, rem = divmod(total_seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _depends_on_any_annotation(
    dep_ids: list[str], *, multi_user: bool
) -> str:
    """v0.7.8 *Knuth's Schedule*: sibling of
    :func:`_depends_on_annotation` for afterany semantics.

    Returns one of:

    * ``"ready"`` — every predecessor has reached a terminal state
      (regardless of WHICH terminal state; afterany doesn't care
      about success vs. failure).
    * ``"waiting: JOBID (STATE)"`` — at least one predecessor is
      non-terminal. Shows the first one as a handle.
    * ``"unresolved: JOBID"`` — predecessor not present in any
      visible queue dir.

    Crucially: NO ``"failed: …"`` variant. Predecessor failure is
    the expected case for afterany — that's the whole point. A
    failed predecessor in the depends_on_any list just means the
    dependent is one step closer to ready.
    """
    from vq import paths as _paths  # noqa: PLC0415 — avoid circular

    first_waiting: tuple[str, JobState] | None = None
    unresolved: str | None = None
    terminal = _terminal_states()
    for did in dep_ids:
        try:
            if multi_user:
                try:
                    p = _paths.resolve_spec_path(did, multi_user=True)
                except FileNotFoundError:
                    if unresolved is None:
                        unresolved = did
                    continue
            else:
                p = _paths.queue_dir() / f"{did}.json"
                if not p.exists():
                    if unresolved is None:
                        unresolved = did
                    continue
            dep_spec = JobSpec.read(p)
        except (OSError, ValueError):
            return "?"
        if dep_spec.state in terminal:
            continue
        if first_waiting is None:
            first_waiting = (did, dep_spec.state)
    if unresolved is not None:
        return f"unresolved: {unresolved}"
    if first_waiting is not None:
        did, dst = first_waiting
        return f"waiting: {did} ({dst.value})"
    return "ready"
