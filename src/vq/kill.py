"""Cancel a job by writing KILLED into its spec and (if running) signaling its PID.

The daemon's reconciliation step is defensive about externally-mutated specs: if
it sees a popen exit for a job whose spec is already in a terminal state, it
leaves the recorded state alone. So the kill flow here is safe to run from any
process that can read/write the queue dir, with no IPC to the daemon.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
import signal
import time
from pathlib import Path

from vq import events, paths, process_group
from vq.host import is_local_host
from vq.ownership import check_spec_path_owner
from vq.spec import JobSpec, JobState, utcnow_iso

log = logging.getLogger(__name__)

# v0.12.0: bound the spec-lock acquire so `vq kill` fails fast with a clear
# error instead of hanging forever behind a stuck or paused lock holder (the
# compute-b wedged-build incident). 15s is far longer than the daemon's
# sub-second hold of the lock, so this only trips on a genuinely stuck holder.
_KILL_LOCK_TIMEOUT = 15.0
_DEFAULT_KILL_REASON = "killed by vq/operator request"
_MAX_KILL_REASON_CHARS = 1000


def _format_kill_failure_reason(reason: str | None) -> str:
    cleaned = " ".join((reason or "").split())
    if not cleaned:
        return _DEFAULT_KILL_REASON
    if len(cleaned) > _MAX_KILL_REASON_CHARS:
        cleaned = cleaned[: _MAX_KILL_REASON_CHARS - 3].rstrip() + "..."
    return f"{_DEFAULT_KILL_REASON}: {cleaned}"


def _current_pause_seconds(spec: JobSpec) -> float:
    if spec.state != JobState.SUSPENDED:
        return 0.0
    if spec.paused_monotonic_at is not None:
        return max(0.0, time.monotonic() - spec.paused_monotonic_at)
    if spec.paused_at:
        try:
            paused = dt.datetime.fromisoformat(
                spec.paused_at.replace("Z", "+00:00")
            )
            now = dt.datetime.now(dt.UTC)
            if paused.tzinfo is None:
                paused = paused.replace(tzinfo=dt.UTC)
            return max(0.0, (now - paused.astimezone(dt.UTC)).total_seconds())
        except ValueError:
            return 0.0
    return 0.0



def _cancel_scheduler_job(spec: JobSpec) -> tuple[str, str]:
    """``qdel`` a scheduler job vq is marking terminal. Returns (note, event).

    **Why this lives in the kill path.** Marking a spec terminal is not, on its
    own, a kill for a scheduler job: the batch job runs on the cluster, and only
    ``qdel`` stops it. The daemon does escalate a terminal spec to ``qdel`` --
    but only for jobs in its in-memory ``_scheduler_running`` map, so a job it is
    not tracking is never cancelled. That happens routinely: the daemon is down
    (every ``vq admin update`` and ``systemctl restart`` opens that window, and
    it is exactly when the ``--restart-after-update`` flow runs), or a reattach
    failed at startup. Worse, once such a spec is killed it is terminal, so the
    deferred-reattach retry skips it forever and it can never become tracked --
    the leak is permanent. vq then reports KILLED while the job keeps burning
    allocation, and retention eventually ``rm -rf``s its remote workspace out
    from under it.

    So the kill issues its own ``qdel``. It is safe for both to fire:
    :meth:`SchedulerDispatcher.cancel` runs ``check=False`` and logs a nonzero
    rc (already gone) rather than raising.

    Degrades LOUDLY, never silently: if the dispatcher cannot be built or the
    ``qdel`` fails, the spec is still marked terminal (the operator asked for
    that) but the returned note says the cluster job may still be live and names
    the id to cancel by hand. A kill that reports plain success while leaking a
    running job is the failure mode this exists to prevent.
    """
    target = spec.scheduler_target
    if target is None:
        return "", ""
    if not spec.scheduler_job_id:
        note = (
            "; NO scheduler job id was recorded for this job, so vq could not "
            "cancel it — if a batch job was submitted it is still live, check "
            f"the scheduler on {target} by hand"
        )
        return note, "scheduler cancel skipped: no scheduler_job_id recorded"
    # Imported lazily: a purely local kill must not pull the config/transport
    # stack in (and the local-path unit tests rely on that).
    from vq.config import ConfigError, load_config  # noqa: PLC0415
    from vq.scheduler_dialect import DialectError  # noqa: PLC0415
    from vq.scheduler_dispatch import (  # noqa: PLC0415
        SchedulerError,
        scheduler_dispatcher_for,
        scheduler_handle_for_spec,
    )

    try:
        dispatcher = scheduler_dispatcher_for(load_config().host(target))
        dispatcher.cancel(
            scheduler_handle_for_spec(
                dispatcher,
                spec,
                job_id=spec.scheduler_job_id,
            )
        )
    except (ConfigError, SchedulerError, DialectError, OSError) as exc:
        log.warning(
            "job %s: could not qdel scheduler job %s on %s: %s",
            spec.id,
            spec.scheduler_job_id,
            target,
            exc,
        )
        note = (
            f"; WARNING could not cancel scheduler job {spec.scheduler_job_id} "
            f"on {target} ({exc}) — it may still be running; cancel it by hand"
        )
        return note, f"scheduler cancel FAILED for {spec.scheduler_job_id}: {exc}"
    return (
        f"; cancelled scheduler job {spec.scheduler_job_id} on {target}",
        f"scheduler cancel issued for {spec.scheduler_job_id}",
    )


def kill_job(
    host: str,
    jobid: str,
    *,
    queue_dir: Path | None = None,
    multi_user: bool = False,
    reason: str | None = None,
    via: str = "cli",
) -> str:
    """Mark `jobid` KILLED; if it was RUNNING, also send SIGTERM to its PID.

    ``reason`` is persisted as ``JobSpec.failure_reason`` so status, wait,
    fetch metadata, and API clients can tell the submitter why the queue or
    operator ended the job.

    Returns a one-line human-readable message describing what happened.
    Raises ``FileNotFoundError`` if no such job, ``ValueError`` if already
    in a terminal state.
    """
    if not is_local_host(host):
        raise NotImplementedError(f"remote kill for {host!r} not implemented in v0.1")
    if multi_user:
        spec_path = paths.resolve_spec_path(jobid, multi_user=True)
    else:
        queue_dir = queue_dir or paths.queue_dir()
        spec_path = queue_dir / f"{jobid}.json"
        if not spec_path.exists():
            raise FileNotFoundError(f"no such job: {jobid}")
    # v0.6.x: multi-user ownership check.
    check_spec_path_owner(spec_path, multi_user=multi_user)

    # v0.8.11 *Dekker's Mutex*: serialize the whole read -> mutate -> write
    # against the daemon's terminal-transition writers (_record_finish, the
    # watchdog, the depends_on cascade) and any other CLI verb. Without the
    # per-spec lock, `vq kill` could read RUNNING while the daemon, between
    # our read and our write, writes COMPLETED — and our KILLED would clobber
    # it (lost update). The signals below are fast syscalls, so holding the
    # lock across them is fine; we never hold it across a subprocess or network
    # call here. The one exception is bounded: a group that answers EPERM is
    # re-probed for up to process_group.EXITING_GROUP_SETTLE_SECONDS (1 s)
    # before the denial is believed, well inside _KILL_LOCK_TIMEOUT (#27).
    with paths.spec_lock(spec_path, timeout=_KILL_LOCK_TIMEOUT):
        spec = JobSpec.read(spec_path)
        if spec.is_terminal:
            raise ValueError(f"job {jobid} already in terminal state ({spec.state.value})")

        workspace = Path(spec.cwd)
        failure_reason = _format_kill_failure_reason(reason)
        events.append_event(
            workspace,
            events.EventKind.KILL_REQUESTED,
            jobid,
            via=via,
            reason=failure_reason,
        )

        if spec.state == JobState.PENDING:
            prev_state = spec.state
            spec.state = JobState.KILLED
            spec.finished_at = utcnow_iso()
            spec.failure_reason = failure_reason
            spec.write(spec_path)
            events.state_transition(
                workspace,
                jobid,
                from_state=prev_state.value,
                to_state=spec.state.value,
                reason=f"{failure_reason} (was pending)",
            )
            was_pending = True
        else:
            was_pending = False

            # state == RUNNING or SUSPENDED. For SUSPENDED, we have to SIGCONT
            # first or SIGTERM queues forever. Cleaner: target the process group
            # with SIGCONT then SIGTERM regardless -- on a RUNNING process the
            # SIGCONT is a no-op.
            pgid = spec.pgid
            pid = spec.pid
            sigterm_sent = False
            signal_denied = False
            if pgid is not None:
                # Through process_group, so a group that is only exiting
                # reads as gone rather than as another user's (#27).
                group_gone = False
                try:
                    process_group.signal_process_group(pgid, signal.SIGCONT)
                except ProcessLookupError:
                    group_gone = True
                except PermissionError as exc:
                    signal_denied = True
                    log.warning(
                        "kill: permission denied sending SIGCONT to pgid %s: %s",
                        pgid,
                        exc,
                    )
                # A group that refuses SIGCONT refuses SIGTERM too, and a
                # second settle would only repeat the same verdict.
                if not group_gone and not signal_denied:
                    try:
                        process_group.signal_process_group(pgid, signal.SIGTERM)
                        sigterm_sent = True
                    except ProcessLookupError:
                        pass  # already dead; daemon will reconcile
                    except PermissionError as exc:
                        signal_denied = True
                        log.warning(
                            "kill: permission denied sending SIGTERM to pgid %s: %s",
                            pgid,
                            exc,
                        )
            elif pid is not None:
                # Pre-v0.3 spec without pgid; fall back to per-pid kill (no
                # SIGCONT needed if state is RUNNING; the SIGTERM gets through).
                try:
                    os.kill(pid, signal.SIGTERM)
                    sigterm_sent = True
                except ProcessLookupError:
                    pass
                except PermissionError as exc:
                    signal_denied = True
                    log.warning(
                        "kill: permission denied sending SIGTERM to pid %s: %s",
                        pid,
                        exc,
                    )
            prev_state = spec.state
            paused_seconds = (
                _current_pause_seconds(spec)
                if prev_state == JobState.SUSPENDED
                else 0.0
            )
            spec.state = JobState.KILLED
            spec.finished_at = utcnow_iso()
            spec.failure_reason = failure_reason
            if prev_state == JobState.SUSPENDED:
                spec.paused_seconds_total = round(
                    spec.paused_seconds_total + paused_seconds, 3
                )
                spec.paused_at = None
                spec.paused_monotonic_at = None
            spec.write(spec_path)
            target = f"pgid {pgid}" if pgid is not None else f"pid {pid}"
            pause_note = (
                f"; accounted paused {paused_seconds:.1f}s this cycle"
                if paused_seconds
                else ""
            )
            events.state_transition(
                workspace,
                jobid,
                from_state=prev_state.value,
                to_state=spec.state.value,
                reason=(
                    f"{failure_reason}; SIGTERM to {target}{pause_note}"
                    if sigterm_sent
                    else (
                        f"{failure_reason}; could not signal {target}: "
                        f"permission denied{pause_note}"
                        if signal_denied
                        else f"{failure_reason}; {target} already dead{pause_note}"
                    )
                ),
            )
            state_word = "suspended" if prev_state == JobState.SUSPENDED else "running"
    # OUTSIDE the spec lock: qdel crosses the network, and the lock must never
    # be held across a network call (see the lock comment above). Both the
    # pending and the running path land here, because a PENDING spec can
    # already have been qsub'd — the daemon claims RUNNING in two phases, so a
    # crash between them leaves a submitted batch job on a spec that still
    # reads PENDING.
    scheduler_note, scheduler_event = _cancel_scheduler_job(spec)
    if scheduler_event:
        events.state_transition(
            workspace,
            jobid,
            from_state=prev_state.value,
            to_state=spec.state.value,
            reason=scheduler_event,
        )
    if was_pending:
        return f"killed pending job {jobid}{scheduler_note}"
    if spec.scheduler_target is not None:
        # A scheduler job has no local pid; reporting "pid None not found; was
        # already dead" told the submitter nothing about the cluster, which is
        # the only place the job actually lives.
        return f"killed {state_word} job {jobid}{scheduler_note}"
    if sigterm_sent:
        return f"killed {state_word} job {jobid} (SIGCONT+SIGTERM to {target})"
    if signal_denied:
        return (
            f"marked {state_word} job {jobid} killed "
            f"({target} could not be signaled: permission denied)"
        )
    return f"killed {state_word} job {jobid} ({target} not found; was already dead)"
