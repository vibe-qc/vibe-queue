"""Pause / resume running jobs via SIGSTOP / SIGCONT.

`vq pause <jobid>` SIGSTOPs the job's process group, marks the spec
SUSPENDED, and stamps ``paused_at``. The kernel freezes the process:
no CPU, no syscalls, but RAM stays allocated and file descriptors stay
open. The watchdog skips every kill check while a job is SUSPENDED.

`vq resume <jobid>` SIGCONTs the process group, flips the spec back to
RUNNING, and adds the elapsed pause to ``paused_seconds_total`` so
wall-time enforcement doesn't penalise paused intervals.

Limitations (worth documenting for the user):

* No checkpoint-to-disk. CRIU is the right tool for that and is not in
  scope. RAM stays allocated; under memory pressure the kernel may
  swap inactive pages out, but it is not guaranteed.
* No queue-level "pause everything" verb yet -- pause is per-jobid.
  An "everything" verb is straightforward to add (loop over running
  jobs) but no stated need yet.
* Pause works on whatever the daemon dispatched. With cgroup-v2
  enforcement active (v0.4+), the SIGSTOP targets the systemd-run scope
  via its pgid -- the kernel does the right thing.
"""
from __future__ import annotations

import contextlib
import logging
import os
import signal
import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from vq import events, ownership, paths, process_group, spec_access
from vq.config import ConfigError, HostConfig
from vq.host import is_local_host
from vq.scheduler_dialect import DialectError, SchedulerPhase
from vq.scheduler_dispatch import (
    SchedulerDispatcher,
    SchedulerError,
    SchedulerHandle,
    scheduler_dispatcher_for,
    scheduler_handle_for_spec,
)
from vq.spec import JobSpec, JobState, utcnow_iso

log = logging.getLogger(__name__)

_HOLD_OUTCOME_UNKNOWN = "hold_outcome_unknown"
_RELEASE_OUTCOME_UNKNOWN = "release_outcome_unknown"


class PauseError(RuntimeError):
    """Raised when a pause / resume request can't be honoured (wrong
    state, no pgid, process gone). The CLI translates these into
    user-visible UsageError / ClickException messages."""


def _clear_pause_intent(spec: JobSpec) -> None:
    """Clear the pre-SIGSTOP transaction record on an in-memory spec."""
    spec.pause_intent_at = None
    spec.pause_intent_monotonic_at = None
    spec.pause_intent_pgid = None
    spec.pause_intent_by = None


def _finish_pause_from_intent(spec: JobSpec) -> None:
    """Atomically-project a durable pause intent into SUSPENDED metadata."""
    if spec.pause_intent_at is None:
        raise PauseError(f"job {spec.id} has no durable pause intent")
    spec.state = JobState.SUSPENDED
    spec.paused_at = spec.pause_intent_at
    spec.paused_monotonic_at = spec.pause_intent_monotonic_at
    spec.paused_by = spec.pause_intent_by
    _clear_pause_intent(spec)


@dataclass(frozen=True)
class PauseIntentReconcileResult:
    """Typed result of completing durable pre-SIGSTOP job intents."""

    completed: tuple[str, ...]
    cleared_gone: tuple[str, ...]
    errors: tuple[tuple[str, str], ...]

    @property
    def success(self) -> bool:
        return not self.errors

    @property
    def summary(self) -> str:
        parts = [f"completed {len(self.completed)} pause intent(s)"]
        if self.cleared_gone:
            parts.append(f"cleared {len(self.cleared_gone)} gone process(es)")
        if self.errors:
            parts.append(f"{len(self.errors)} error(s)")
        return "; ".join(parts)


def _resolve_one_spec_path(
    jobid: str,
    queue_dir: Path | None,
    multi_user: bool,
    queue_root: Path | None = None,
) -> Path:
    """v0.6.38: resolve a single job's spec path. In multi-user mode
    the spec lives under ``/var/lib/vq/users/<uid>/queue/`` — search
    every per-user dir. Raises ``FileNotFoundError`` if not found."""
    if multi_user:
        if queue_root is None:
            return paths.resolve_spec_path(jobid, multi_user=True)
        safe_jobid = paths._safe_job_id(jobid)
        matches = [
            user_dir / "queue" / f"{safe_jobid}.json"
            for user_dir in paths._all_user_dirs(state_root=queue_root)
            if (user_dir / "queue" / f"{safe_jobid}.json").exists()
        ]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            raise ValueError(
                f"ambiguous job id {jobid!r}: multiple per-user specs exist"
            )
        raise FileNotFoundError(f"no such job: {jobid}")
    qd = queue_dir or paths.queue_dir()
    spec_path = qd / f"{jobid}.json"
    if not spec_path.exists():
        raise FileNotFoundError(f"no such job: {jobid}")
    return spec_path


def _all_user_spec_paths(queue_root: Path | None = None) -> list[Path]:
    """v0.6.38: every spec path across every per-user queue dir under
    ``/var/lib/vq/users/<uid>/`` — the multi-user analogue of
    ``queue_dir.glob("*.json")`` for the bulk pause/resume verbs."""
    out: list[Path] = []
    for user_dir in paths._all_user_dirs(state_root=queue_root):
        qd = user_dir / "queue"
        if qd.is_dir():
            out.extend(sorted(qd.glob("*.json")))
    return out


def _bulk_control_candidates(
    spec_paths: Sequence[Path],
    *,
    multi_user: bool,
    policy: ownership.AuthorizationPolicy | None = None,
) -> Iterator[Path]:
    """Omit terminal history before taking any per-job control locks.

    This is a negative hint, never authorization to signal a process. Every
    yielded row still goes through locked authorization and the single-job
    helper's final recheck. A terminal hint must pass read-side ownership;
    denied hints fall back to the locked error path. Do not report fields or
    counts from omitted rows. Retain rows with pause evidence,
    even when terminal, for the ordinary control/recovery path. Exact token
    admission and reconciliation proofs deliberately do not use this filter.

    ``policy`` is the caller's already-resolved authorization policy for this
    one operation. Every row is checked either way; passing it just stops each
    row re-reading and re-validating the config, which is what made this
    filter's cost grow with everything the queue retains (#22).
    """
    for spec_path in spec_paths:
        try:
            spec = spec_access.read_bounded_regular_spec(spec_path)
        except (OSError, ValueError):
            # Preserve the existing locked error/authorization behavior when
            # the hint cannot be read. Never treat unreadable data as terminal.
            yield spec_path
            continue
        if (
            spec.is_terminal
            and spec.paused_by is None
            and spec.pause_intent_at is None
            and spec.pause_intent_by is None
            and spec.pause_intent_pgid is None
            and spec.pause_intent_monotonic_at is None
        ):
            try:
                ownership.check_owner(
                    spec, multi_user=multi_user, policy=policy,
                )
            except ownership.OwnershipError:
                pass
            else:
                continue
        yield spec_path


def _pause_scope_spec_paths(
    *,
    queue_dir: Path | None,
    multi_user: bool,
    queue_root: Path | None = None,
) -> list[Path]:
    """Return the exact durable rows covered by a local bulk operation."""
    if multi_user:
        return _all_user_spec_paths(queue_root)
    resolved = queue_dir or paths.queue_dir()
    return sorted(resolved.glob("*.json")) if resolved.is_dir() else []


def _bind_proof_queue_root(
    queue_root: Path | None,
    *,
    queue_dir: Path | None,
    multi_user: bool,
) -> Path | None:
    """Bind a proof scan to the persisted queue-root identity.

    Admin receipts pass the root captured before SIGSTOP.  A root override can
    change independently of multi-user configuration, so a mismatch is an
    unresolved safety condition, never an empty queue.  Multi-user helpers use
    the path module's hardened per-user traversal after this identity check;
    single-user helpers receive the exact ``<root>/queue`` directory.
    """
    if queue_root is None:
        return queue_dir
    persisted = Path(queue_root).expanduser().resolve(strict=False)
    current = (
        paths.multi_user_root() if multi_user else paths.state_root()
    ).expanduser().resolve(strict=False)
    if persisted != current:
        raise PauseError(
            "persisted pause queue root no longer matches path policy "
            f"(receipt={persisted}, current={current}); restore the original "
            "queue-root configuration before proving pause/resume"
        )
    if multi_user:
        if queue_dir is not None:
            raise PauseError(
                "multi-user pause proof cannot combine queue_root with a "
                "single-user queue_dir"
            )
        return None
    persisted_queue = persisted / "queue"
    if (
        queue_dir is not None
        and queue_dir.expanduser().resolve(strict=False) != persisted_queue
    ):
        raise PauseError(
            "pause proof queue_dir does not belong to the persisted queue root"
        )
    return persisted_queue


@contextlib.contextmanager
def _locked_authorized_spec(
    spec_path: Path,
    *,
    multi_user: bool,
    policy: ownership.AuthorizationPolicy | None = None,
) -> Iterator[JobSpec]:
    """Load and authorize one spec inside its mutation lock.

    Pause/resume changes process or scheduler state, so a read-side ownership
    check before acquiring the lock would leave a gap before the mutation.
    Every caller classifies or mutates only the snapshot yielded here.  Bulk
    callers release this lock before invoking a single-job helper, which then
    repeats the same locked authorization against the final snapshot.

    ``policy`` is the caller's already-resolved authorization policy for this
    one operation; the snapshot is authorized against it exactly as it would be
    against a freshly loaded one.
    """
    with paths.spec_lock(spec_path):
        spec = JobSpec.read(spec_path)
        ownership.check_owner(spec, multi_user=multi_user, policy=policy)
        yield spec


def pause_job(
    host: str,
    jobid: str,
    *,
    paused_by: str | None = None,
    queue_dir: Path | None = None,
    queue_root: Path | None = None,
    multi_user: bool = False,
) -> str:
    """Send SIGSTOP to ``jobid``'s process group; mark spec SUSPENDED.

    v0.6.22: ``paused_by`` records who initiated the pause. The tag
    is stored on the spec and surfaces in ``vq status``; ``vq resume
    --paused-by TAG`` (and programmatic ``resume_all(
    paused_by_filter=TAG)``) only resumes jobs with a matching tag.
    Used by scripts that want to pause the queue for a build without
    risk of also-resuming operator-paused jobs.

    Returns a one-line human-readable message. Raises FileNotFoundError
    if the spec isn't there, PauseError if the spec isn't RUNNING or has
    no pgid, NotImplementedError for non-local host (the CLI dispatcher
    handles cross-machine routing).
    """
    if not is_local_host(host):
        raise NotImplementedError(
            f"pause for {host!r}: CLI must dispatch via SSH; "
            "this function is local-only"
        )
    spec_path = _resolve_one_spec_path(
        jobid, queue_dir, multi_user, queue_root,
    )

    # v0.8.13 *Gray's Transaction*: serialize the read -> mutate -> write
    # against the daemon's terminal-transition writers (a job exiting /
    # being watchdog-killed while we pause it) and other CLI verbs. The
    # read happens inside the lock, so the state checks below see the latest
    # state — if the job went terminal we refuse instead of clobbering it
    # with SUSPENDED. SIGSTOP is a fast syscall, safe to hold across.
    with _locked_authorized_spec(spec_path, multi_user=multi_user) as spec:
        if spec.pause_intent_at is not None:
            raise PauseError(
                f"job {jobid} already has a durable pause intent; the daemon "
                "or admin recovery must reconcile it before another pause"
            )
        if spec.state == JobState.SUSPENDED:
            raise PauseError(f"job {jobid} is already suspended")
        if spec.state != JobState.RUNNING:
            raise PauseError(
                f"job {jobid} is in state {spec.state.value}; only RUNNING jobs "
                "can be paused"
            )
        if spec.pgid is None:
            raise PauseError(
                f"job {jobid} has no pgid recorded; cannot SIGSTOP. "
                "(Pre-v0.3 spec or dispatch race.)"
            )
        if spec.pgid == os.getpgrp():
            # Self-SIGSTOP guard (2026-07-25): a `vq build-env` job runs
            # update_env, whose pause bracket swept up the build job's OWN
            # spec — killpg froze this process at the next line, BEFORE the
            # SUSPENDED write, the marker, or any event. The job then sat
            # RUNNING-but-stopped forever (compute-b e4c32b654b03, build-host
            # ca78bea5db13, localhost 8cd5ad2cd096, each ~9h past wall).
            # No caller may ever suspend the process group it runs in.
            raise PauseError(
                f"job {jobid}: refusing to SIGSTOP pgid {spec.pgid} — that "
                "is this process's own process group; a self-pause freezes "
                "the pauser before it can record anything"
            )

        # Persist the complete intent before the first process mutation.  If
        # this process is SIGKILLed before SIGSTOP, reconciliation finishes the
        # pause; if it dies after SIGSTOP but before the SUSPENDED write, the
        # same record prevents an untagged RUNNING-but-stopped orphan.
        spec.pause_intent_at = utcnow_iso()
        spec.pause_intent_monotonic_at = time.monotonic()
        spec.pause_intent_pgid = spec.pgid
        spec.pause_intent_by = paused_by
        spec.write(spec_path)

        try:
            process_group.signal_process_group(spec.pgid, signal.SIGSTOP)
        except ProcessLookupError:
            # The process is gone; the daemon's reconciler will catch it.
            # Disarm the durable intent so the daemon's ordinary exit path can
            # classify it.  If this write itself fails, retaining the intent is
            # conservative: the next reconciliation retries and observes the
            # same gone process group.
            _clear_pause_intent(spec)
            spec.write(spec_path)
            raise PauseError(
                f"job {jobid} pgid {spec.pgid} is gone; "
                "daemon reconciler will mark it shortly"
            ) from None
        except PermissionError:
            # v0.6.38: multi-user — the job's process group belongs to
            # another user. Only root (the daemon) or the job's owner can
            # SIGSTOP it. Surface a clear error instead of a raw traceback.
            _clear_pause_intent(spec)
            spec.write(spec_path)
            raise PauseError(
                f"job {jobid}: permission denied sending SIGSTOP to pgid "
                f"{spec.pgid} — it belongs to another user; only root or "
                f"the job's owner can pause it"
            ) from None

        _finish_pause_from_intent(spec)
        spec.write(spec_path)
        events.append_event(
            Path(spec.cwd), events.EventKind.STATE_TRANSITION, jobid,
            **{
                "from": JobState.RUNNING.value,
                "to": JobState.SUSPENDED.value,
                "reason": (
                    f"vq pause (SIGSTOP, paused_by={paused_by})"
                    if paused_by
                    else "vq pause (SIGSTOP)"
                ),
            },
        )
    paused_by_part = f" paused_by={paused_by}" if paused_by else ""
    return f"paused job {jobid} (SIGSTOP to pgid {spec.pgid}){paused_by_part}"


def _reconcile_pause_intent_path(
    spec_path: Path,
    *,
    multi_user: bool,
    omit_without_intent: bool = False,
) -> str | None:
    """Complete one fsynced pause intent under its exact spec lock.

    Returns ``"completed"`` when SUSPENDED was durably recorded,
    ``"cleared_gone"`` when the process disappeared before SIGSTOP, and
    ``None`` when no intent exists.  An unsafe/ambiguous intent is retained and
    raises :class:`PauseError`; dropping it would erase the only evidence that
    the corresponding process group may already be stopped.

    ``omit_without_intent`` reads the row before taking its lock and stops
    there when there is no intent to finish.  It is a negative hint about work
    to do, never authorization to signal a process: a row that carries an
    intent, and a row that cannot be read at all, still go through the locked
    and authorized path unchanged.  Specs are published by atomic replace, so
    the unlocked read always sees one whole record -- the snapshot the locked
    read would have taken a moment earlier.  An intent armed concurrently is
    therefore reconciled by the next sweep, exactly as one armed a moment
    later already is, so this belongs to a caller that sweeps repeatedly and
    not to one whose single sweep has to be exact.  Off by default for that
    reason; see :func:`reconcile_pause_intents`.
    """
    if omit_without_intent:
        try:
            if JobSpec.read(spec_path).pause_intent_at is None:
                return None
        except (OSError, ValueError):
            pass

    with _locked_authorized_spec(spec_path, multi_user=multi_user) as spec:
        if spec.pause_intent_at is None:
            return None
        intended_pgid = spec.pause_intent_pgid
        if (
            intended_pgid is None
            or intended_pgid <= 0
            or spec.pgid != intended_pgid
        ):
            raise PauseError(
                f"job {spec.id} has an unsafe pause intent: intended pgid "
                f"{intended_pgid!r}, current pgid {spec.pgid!r}"
            )
        if intended_pgid == os.getpgrp():
            raise PauseError(
                f"job {spec.id}: refusing to reconcile a self-pause intent "
                f"for this process group {intended_pgid}"
            )

        # A terminal/non-running transition after the intent was written must
        # not leave a possibly stopped process unable to consume its pending
        # SIGTERM/SIGKILL.  SIGCONT is idempotent and clearing the intent is
        # safe only after that inverse signal succeeds (or the group is gone).
        if spec.state not in {JobState.RUNNING, JobState.SUSPENDED}:
            try:
                process_group.signal_process_group(intended_pgid, signal.SIGCONT)
            except ProcessLookupError:
                pass
            except PermissionError:
                raise PauseError(
                    f"job {spec.id}: permission denied reconciling terminal "
                    f"pause intent for pgid {intended_pgid}"
                ) from None
            _clear_pause_intent(spec)
            spec.write(spec_path)
            return "cleared_gone"

        try:
            # Idempotent for the post-SIGSTOP/pre-spec-write crash window and
            # performs the missing signal for the pre-SIGSTOP window.
            process_group.signal_process_group(intended_pgid, signal.SIGSTOP)
        except ProcessLookupError:
            _clear_pause_intent(spec)
            spec.write(spec_path)
            return "cleared_gone"
        except PermissionError:
            raise PauseError(
                f"job {spec.id}: permission denied reconciling SIGSTOP for "
                f"pgid {intended_pgid}"
            ) from None

        _finish_pause_from_intent(spec)
        spec.write(spec_path)
        events.append_event(
            Path(spec.cwd),
            events.EventKind.STATE_TRANSITION,
            spec.id,
            **{
                "from": JobState.RUNNING.value,
                "to": JobState.SUSPENDED.value,
                "reason": (
                    "reconciled durable vq pause intent "
                    f"(SIGSTOP, paused_by={spec.paused_by})"
                    if spec.paused_by
                    else "reconciled durable vq pause intent (SIGSTOP)"
                ),
            },
        )
        return "completed"


def reconcile_pause_intents(
    host: str,
    *,
    queue_dir: Path | None = None,
    queue_root: Path | None = None,
    multi_user: bool = False,
    omit_rows_without_intent: bool = False,
) -> PauseIntentReconcileResult:
    """Finish every durable local pause intent visible to this caller.

    The daemon invokes this before lifecycle polling.  Admin recovery invokes
    it before token-scoped resume proof.  Per-job failures are returned rather
    than discarded so a caller that owns an update marker can keep that marker
    armed until every potential stopped process is accounted for.

    A queue retains its terminal jobs, so this sweep is sized by everything
    the host has ever run rather than by what is live (#22).  Locking and
    authorizing a job only to find it carries no intent costs three file opens
    apiece -- the lock, the spec, and the authorization config -- which the
    daemon then pays for every retained job on every tick.
    ``omit_rows_without_intent`` reads each row first and skips the ones with
    nothing to finish.  It is for a caller that sweeps repeatedly, so a row
    that arms an intent between the read and the lock is simply picked up next
    time.  ``pause_token_scope_with_proof`` leaves it off: an exact proof
    scans every row under its lock, including terminal ones, and must not rest
    on an unlocked hint.
    """
    if not is_local_host(host):
        raise NotImplementedError(
            f"pause-intent reconciliation for {host!r} is local-only"
    )
    if multi_user:
        spec_paths = _all_user_spec_paths(queue_root)
    else:
        queue_dir = queue_dir or paths.queue_dir()
        spec_paths = (
            sorted(queue_dir.glob("*.json")) if queue_dir.is_dir() else []
        )
    completed: list[str] = []
    cleared_gone: list[str] = []
    errors: list[tuple[str, str]] = []
    for spec_path in spec_paths:
        try:
            result = _reconcile_pause_intent_path(
                spec_path,
                multi_user=multi_user,
                omit_without_intent=omit_rows_without_intent,
            )
        except ConfigError:
            raise
        except (
            OSError,
            ValueError,
            PauseError,
            ownership.OwnershipError,
        ) as exc:
            errors.append((spec_path.stem, _failure_text(exc)))
            continue
        if result == "completed":
            completed.append(spec_path.stem)
        elif result == "cleared_gone":
            cleared_gone.append(spec_path.stem)
    return PauseIntentReconcileResult(
        tuple(completed), tuple(cleared_gone), tuple(errors),
    )


def resume_job(
    host: str,
    jobid: str,
    *,
    paused_by_filter: str | None = None,
    queue_dir: Path | None = None,
    queue_root: Path | None = None,
    multi_user: bool = False,
) -> str:
    """Send SIGCONT; mark spec RUNNING; accumulate paused interval.

    Returns a one-line human-readable message. Raises FileNotFoundError
    if no such job, PauseError if the spec isn't SUSPENDED, no pgid, or
    the process is gone.
    """
    if not is_local_host(host):
        raise NotImplementedError(
            f"resume for {host!r}: CLI must dispatch via SSH; "
            "this function is local-only"
        )
    spec_path = _resolve_one_spec_path(
        jobid, queue_dir, multi_user, queue_root,
    )

    # v0.8.13 *Gray's Transaction*: serialize the read -> mutate -> write,
    # so the SUSPENDED check + RUNNING write are atomic against a racing
    # daemon terminal write. SIGCONT is a fast syscall, safe under the lock.
    with _locked_authorized_spec(spec_path, multi_user=multi_user) as spec:
        if spec.state != JobState.SUSPENDED:
            raise PauseError(
                f"job {jobid} is in state {spec.state.value}; only SUSPENDED "
                "jobs can be resumed"
            )
        if paused_by_filter is not None and spec.paused_by != paused_by_filter:
            raise PauseError(
                f"job {jobid} paused_by={spec.paused_by!r} does not match "
                f"--paused-by {paused_by_filter!r}; refusing to resume. "
                "Drop --paused-by to resume unconditionally."
            )
        if spec.pgid is None:
            raise PauseError(
                f"job {jobid} has no pgid recorded; cannot SIGCONT"
            )

        try:
            process_group.signal_process_group(spec.pgid, signal.SIGCONT)
        except ProcessLookupError:
            raise PauseError(
                f"job {jobid} pgid {spec.pgid} is gone; nothing to resume"
            ) from None
        except PermissionError:
            # v0.6.38: multi-user — pgid belongs to another user.
            raise PauseError(
                f"job {jobid}: permission denied sending SIGCONT to pgid "
                f"{spec.pgid} — it belongs to another user; only root or "
                f"the job's owner can resume it"
            ) from None

        # Accumulate the pause interval. PA-1: prefer the monotonic anchor
        # stamped at SIGSTOP — it can't be distorted by a wall-clock step
        # (NTP slew / manual `date`) during a long pause, which would
        # otherwise mis-bill paused_seconds_total and so the wall-time
        # budget. Fall back to the wall-clock paused_at diff for a spec
        # paused before PA-1 shipped (no monotonic anchor on disk).
        paused_seconds = 0.0
        if spec.paused_monotonic_at is not None:
            paused_seconds = max(0.0, time.monotonic() - spec.paused_monotonic_at)
        elif spec.paused_at is not None:
            try:
                paused_dt = datetime.fromisoformat(spec.paused_at)
                now_dt = datetime.fromisoformat(utcnow_iso())
                paused_seconds = (now_dt - paused_dt).total_seconds()
            except ValueError:
                # Malformed paused_at; just zero the interval.
                paused_seconds = 0.0
        spec.state = JobState.RUNNING
        spec.paused_at = None
        spec.paused_monotonic_at = None
        # v0.6.22: clear paused_by on resume. The tag is meaningless
        # for a running job; clearing it now means the NEXT pause
        # (potentially by a different actor) gets fresh ownership.
        spec.paused_by = None
        spec.paused_seconds_total = round(spec.paused_seconds_total + paused_seconds, 3)
        spec.write(spec_path)
        events.append_event(
            Path(spec.cwd), events.EventKind.STATE_TRANSITION, jobid,
            **{
                "from": JobState.SUSPENDED.value,
                "to": JobState.RUNNING.value,
                "reason": f"vq resume (SIGCONT, paused {paused_seconds:.1f}s this cycle)",
            },
        )
    return (
        f"resumed job {jobid} (SIGCONT to pgid {spec.pgid}; "
        f"paused this cycle: {paused_seconds:.1f}s, total: "
        f"{spec.paused_seconds_total:.1f}s)"
    )


def _scheduler_handle(
    dispatcher: SchedulerDispatcher,
    spec: JobSpec,
) -> SchedulerHandle:
    """Build the persisted scheduler handle for a spec."""
    if spec.scheduler_job_id is None:
        raise PauseError(
            f"job {spec.id} has no scheduler_job_id recorded; cannot control it"
        )
    return scheduler_handle_for_spec(
        dispatcher,
        spec,
        job_id=spec.scheduler_job_id,
    )


def _failure_text(exc: BaseException) -> str:
    """Render one single-line failure for a compound pause error."""
    message = " ".join(str(exc).splitlines()).strip()
    return message or type(exc).__name__


def _restore_scheduler_spec_snapshot_locked(
    spec_path: Path,
    *,
    before: JobSpec,
    after: JobSpec,
    multi_user: bool,
) -> None:
    """Restore and byte-verify our exact pre-state under the caller's lock."""
    expected_before_bytes = before.to_json().encode("utf-8")
    expected_after_bytes = after.to_json().encode("utf-8")

    def read_current() -> tuple[JobSpec, bytes]:
        durable_bytes = spec_path.read_bytes()
        current = JobSpec.from_json(durable_bytes.decode("utf-8"))
        ownership.check_owner(current, multi_user=multi_user)
        return current, durable_bytes

    current, durable_bytes = read_current()
    if current == before and durable_bytes == expected_before_bytes:
        return
    if current != after or durable_bytes != expected_after_bytes:
        raise PauseError(
            f"job {before.id} spec is not the exact post-state before "
            "rollback; refusing to overwrite the newer or malformed record"
        )

    failures: list[str] = []
    for attempt in range(1, 3):
        try:
            before.write(spec_path)
        except BaseException as exc:
            failures.append(
                f"restore attempt {attempt} write failed: {_failure_text(exc)}"
            )

        try:
            current, durable_bytes = read_current()
        except BaseException as exc:
            failures.append(
                f"restore attempt {attempt} verification failed: "
                f"{_failure_text(exc)}"
            )
            raise PauseError(
                f"job {before.id} exact pre-state restore verification "
                f"failed after attempt {attempt}; retry refused: "
                f"{'; '.join(failures)}"
            ) from exc
        if current == before and durable_bytes == expected_before_bytes:
            # Includes atomic replace-then-raise: the durable bytes, not the
            # wrapper's return path, decide whether restoration succeeded.
            return
        if current != after or durable_bytes != expected_after_bytes:
            raise PauseError(
                f"job {before.id} restore attempt {attempt} verification "
                "found a non-exact post-state; retry refused rather than "
                "overwrite the newer or malformed record"
            )
        failures.append(
            f"restore attempt {attempt} left the exact stale post-state"
        )

    raise PauseError(
        f"job {before.id} exact pre-state restore was not durable after "
        f"2 attempts: {'; '.join(failures)}"
    )


def _persist_scheduler_outcome_unknown_locked(
    spec_path: Path,
    *,
    before: JobSpec,
    after: JobSpec,
    scheduler_state: str,
    multi_user: bool,
) -> None:
    """Persist an intended post-state when the exact inverse is unknown.

    The opposite ordinary verb is then the exact recovery route: a held-
    outcome marker is SUSPENDED and routes through resume/qrls; a released-
    outcome marker is RUNNING and routes through pause/qhold.
    """
    current = JobSpec.read(spec_path)
    ownership.check_owner(current, multi_user=multi_user)
    unknown = after.model_copy(deep=True)
    unknown.scheduler_state = scheduler_state
    if current == unknown:
        return
    if current not in (before, after):
        raise PauseError(
            f"job {before.id} spec changed before outcome-unknown marking; "
            "refusing to overwrite the newer record"
        )
    try:
        unknown.write(spec_path)
    except BaseException:
        # atomic_write_text may replace successfully and a wrapper may then
        # raise. Treat the durable exact marker as success in that case.
        if JobSpec.read(spec_path) == unknown:
            return
        raise
    if JobSpec.read(spec_path) != unknown:
        raise PauseError(
            f"job {before.id} outcome-unknown marker did not persist exactly"
        )


def _rollback_scheduler_mutation(
    dispatcher: SchedulerDispatcher,
    handle: SchedulerHandle,
    *,
    rollback_operation: str,
    spec_path: Path,
    before: JobSpec | None,
    after: JobSpec | None,
    local_write_attempted: bool,
    unknown_scheduler_state: str,
    multi_user: bool,
) -> tuple[list[str], bool]:
    """Undo one attempted qhold/qrls plus an optional exact local write.

    The caller keeps the spec lock across mutation, commit, and this rollback,
    so a lagging pause/resume caller cannot apply an inverse to another
    caller's committed scheduler state.
    """
    failures: list[str] = []
    inverse_succeeded = False
    unknown_marked = False
    try:
        if rollback_operation == "qrls":
            dispatcher.release(handle)
        elif rollback_operation == "qhold":
            dispatcher.hold(handle)
        else:  # pragma: no cover - closed internal call surface
            raise AssertionError(
                f"unsupported scheduler rollback {rollback_operation!r}"
            )
        inverse_succeeded = True
    except BaseException as exc:
        failures.append(
            f"{rollback_operation} rollback failed: {_failure_text(exc)}"
        )

    # A failed inverse leaves the scheduler outcome unknown. Keep an exact
    # post-mutation spec in place rather than actively creating the opposite
    # local/remote state pairing. Restore only after the inverse returned.
    if (
        inverse_succeeded
        and local_write_attempted
        and before is not None
        and after is not None
    ):
        try:
            _restore_scheduler_spec_snapshot_locked(
                spec_path,
                before=before,
                after=after,
                multi_user=multi_user,
            )
        except BaseException as exc:
            failures.append(f"spec rollback failed: {_failure_text(exc)}")
    elif not inverse_succeeded and before is not None and after is not None:
        try:
            _persist_scheduler_outcome_unknown_locked(
                spec_path,
                before=before,
                after=after,
                scheduler_state=unknown_scheduler_state,
                multi_user=multi_user,
            )
            unknown_marked = True
        except BaseException as exc:
            failures.append(
                "outcome-unknown marker persistence failed: "
                f"{_failure_text(exc)}"
            )
    return failures, unknown_marked


def pause_scheduler_job(
    scheduler_target: str,
    host_cfg: HostConfig,
    jobid: str,
    *,
    paused_by: str | None = None,
    queue_dir: Path | None = None,
    multi_user: bool = False,
) -> str:
    """Place a scheduler hold on a queued scheduler-backed job.

    This is intentionally narrower than local ``pause_job``. PBS/Torque
    ``qhold`` prevents a queued job from starting; it is not a live SIGSTOP for
    a job already executing on a compute node. The helper polls first and
    refuses RUNNING scheduler phases rather than pretending a running cluster
    calculation has been frozen.
    """
    spec_path = _resolve_one_spec_path(jobid, queue_dir, multi_user)
    try:
        dispatcher = scheduler_dispatcher_for(host_cfg)
    except (SchedulerError, DialectError) as exc:
        raise PauseError(str(exc)) from exc
    with _locked_authorized_spec(spec_path, multi_user=multi_user) as spec:
        if spec.scheduler_target != scheduler_target:
            raise PauseError(
                f"job {jobid} targets scheduler {spec.scheduler_target!r}, "
                f"not {scheduler_target!r}"
            )
        if spec.state == JobState.SUSPENDED:
            raise PauseError(f"job {jobid} is already suspended")
        if spec.state != JobState.RUNNING:
            raise PauseError(
                f"job {jobid} is in state {spec.state.value}; only RUNNING "
                "scheduler jobs can be held"
            )
        handle = _scheduler_handle(dispatcher, spec)
    phase = dispatcher.poll([handle]).get(handle.job_id, SchedulerPhase.FINISHED)
    if phase is SchedulerPhase.RUNNING:
        raise PauseError(
            f"job {jobid} is already running on scheduler {scheduler_target!r}; "
            "qhold only prevents queued jobs from starting and cannot suspend "
            "a live compute-node job"
        )
    if phase is SchedulerPhase.FINISHED:
        raise PauseError(
            f"job {jobid} is no longer listed by the scheduler; daemon "
            "reconciliation should mark it terminal shortly"
        )
    with _locked_authorized_spec(spec_path, multi_user=multi_user) as spec:
        final_handle = _scheduler_handle(dispatcher, spec)
        if (
            spec.scheduler_target != scheduler_target
            or final_handle != handle
            or spec.state != JobState.RUNNING
        ):
            raise PauseError(
                f"job {jobid} changed before scheduler hold; "
                "no scheduler mutation was attempted"
            )

        before_write = spec.model_copy(deep=True)
        after_write = before_write.model_copy(deep=True)
        after_write.state = JobState.SUSPENDED
        after_write.paused_at = utcnow_iso()
        after_write.paused_monotonic_at = time.monotonic()
        after_write.paused_by = paused_by
        after_write.scheduler_state = "held"
        mutation_attempted = False
        local_write_attempted = False
        try:
            # Set before the call: qhold may apply remotely and then raise.
            mutation_attempted = True
            dispatcher.hold(handle)

            # Policy and the exact on-disk snapshot must still agree before
            # the local commit. The sidecar lock serializes every compliant
            # pause/resume caller across qhold and this write.
            current = JobSpec.read(spec_path)
            ownership.check_owner(current, multi_user=multi_user)
            current_handle = _scheduler_handle(dispatcher, current)
            if (
                current.scheduler_target != scheduler_target
                or current_handle != handle
                or current.state != JobState.RUNNING
                or current != before_write
            ):
                raise PauseError(
                    f"job {jobid} changed while scheduler hold was being "
                    "applied; the spec was left unchanged"
                )
            local_write_attempted = True
            after_write.write(spec_path)
            spec = after_write
        except BaseException as exc:
            if not mutation_attempted:  # pragma: no cover - defensive
                raise
            rollback_failures, unknown_marked = _rollback_scheduler_mutation(
                dispatcher,
                handle,
                rollback_operation="qrls",
                spec_path=spec_path,
                before=before_write,
                after=after_write,
                local_write_attempted=local_write_attempted,
                unknown_scheduler_state=_HOLD_OUTCOME_UNKNOWN,
                multi_user=multi_user,
            )
            if rollback_failures:
                recovery = (
                    f"; durable spec marked {_HOLD_OUTCOME_UNKNOWN}; "
                    f"vq resume {scheduler_target} {jobid} is the exact "
                    "qrls recovery"
                    if unknown_marked
                    else ""
                )
                raise PauseError(
                    f"scheduler qhold for job {jobid} failed during remote "
                    f"mutation: {_failure_text(exc)}; "
                    f"{'; '.join(rollback_failures)}; outcome unknown"
                    f"{recovery}"
                ) from exc
            if isinstance(exc, (ConfigError, ownership.OwnershipError)):
                raise
            if not isinstance(exc, Exception):
                raise
            raise PauseError(
                f"{_failure_text(exc)}; scheduler rollback completed"
            ) from exc

    # Event recording is deliberately post-commit and best-effort. Once the
    # scheduler and spec agree, a reporting failure must not undo the job.
    try:
        events.append_event(
            Path(spec.cwd),
            events.EventKind.STATE_TRANSITION,
            jobid,
            **{
                "from": JobState.RUNNING.value,
                "to": JobState.SUSPENDED.value,
                "reason": (
                    f"vq pause (qhold, paused_by={paused_by})"
                    if paused_by
                    else "vq pause (qhold)"
                ),
            },
        )
    except Exception as exc:  # pragma: no cover - append_event is best-effort
        log.warning(
            "job %s: committed scheduler hold but event reporting failed: %s",
            jobid,
            _failure_text(exc),
        )
    paused_by_part = f" paused_by={paused_by}" if paused_by else ""
    return (
        f"held scheduler job {jobid} (qhold {handle.job_id} on "
        f"{scheduler_target}){paused_by_part}"
    )


def resume_scheduler_job(
    scheduler_target: str,
    host_cfg: HostConfig,
    jobid: str,
    *,
    paused_by_filter: str | None = None,
    queue_dir: Path | None = None,
    multi_user: bool = False,
) -> str:
    """Release a scheduler hold and restore the spec to RUNNING."""
    spec_path = _resolve_one_spec_path(jobid, queue_dir, multi_user)
    try:
        dispatcher = scheduler_dispatcher_for(host_cfg)
    except (SchedulerError, DialectError) as exc:
        raise PauseError(str(exc)) from exc
    with _locked_authorized_spec(spec_path, multi_user=multi_user) as spec:
        if spec.scheduler_target != scheduler_target:
            raise PauseError(
                f"job {jobid} targets scheduler {spec.scheduler_target!r}, "
                f"not {scheduler_target!r}"
            )
        if spec.state != JobState.SUSPENDED:
            raise PauseError(
                f"job {jobid} is in state {spec.state.value}; only SUSPENDED "
                "scheduler jobs can be released"
            )
        if paused_by_filter is not None and spec.paused_by != paused_by_filter:
            raise PauseError(
                f"job {jobid} paused_by={spec.paused_by!r} does not match "
                f"--paused-by {paused_by_filter!r}; refusing to resume"
            )
        handle = _scheduler_handle(dispatcher, spec)
    with _locked_authorized_spec(spec_path, multi_user=multi_user) as spec:
        final_handle = _scheduler_handle(dispatcher, spec)
        if (
            spec.scheduler_target != scheduler_target
            or final_handle != handle
            or spec.state != JobState.SUSPENDED
        ):
            raise PauseError(
                f"job {jobid} changed before scheduler release; "
                "no scheduler mutation was attempted"
            )
        if paused_by_filter is not None and spec.paused_by != paused_by_filter:
            raise PauseError(
                f"job {jobid} paused_by={spec.paused_by!r} does not match "
                f"--paused-by {paused_by_filter!r}; refusing to resume"
            )

        before_write = spec.model_copy(deep=True)
        paused_seconds = 0.0
        if before_write.paused_monotonic_at is not None:
            paused_seconds = max(
                0.0, time.monotonic() - before_write.paused_monotonic_at
            )
        elif before_write.paused_at is not None:
            try:
                paused_dt = datetime.fromisoformat(before_write.paused_at)
                now_dt = datetime.fromisoformat(utcnow_iso())
                paused_seconds = (now_dt - paused_dt).total_seconds()
            except ValueError:
                paused_seconds = 0.0
        after_write = before_write.model_copy(deep=True)
        after_write.state = JobState.RUNNING
        after_write.paused_at = None
        after_write.paused_monotonic_at = None
        after_write.paused_by = None
        after_write.paused_seconds_total = round(
            after_write.paused_seconds_total + paused_seconds, 3
        )
        after_write.scheduler_state = "queued"
        mutation_attempted = False
        local_write_attempted = False
        try:
            # Set before the call: qrls may apply remotely and then raise.
            mutation_attempted = True
            dispatcher.release(handle)

            current = JobSpec.read(spec_path)
            ownership.check_owner(current, multi_user=multi_user)
            current_handle = _scheduler_handle(dispatcher, current)
            if (
                current.scheduler_target != scheduler_target
                or current_handle != handle
                or current.state != JobState.SUSPENDED
            ):
                raise PauseError(
                    f"job {jobid} changed while scheduler hold was being "
                    "released; the spec was left unchanged"
                )
            if (
                paused_by_filter is not None
                and current.paused_by != paused_by_filter
            ):
                raise PauseError(
                    f"job {jobid} paused_by={current.paused_by!r} does not "
                    f"match --paused-by {paused_by_filter!r}; refusing to resume"
                )
            if current != before_write:
                raise PauseError(
                    f"job {jobid} changed while scheduler hold was being "
                    "released; the spec was left unchanged"
                )
            local_write_attempted = True
            after_write.write(spec_path)
            spec = after_write
        except BaseException as exc:
            if not mutation_attempted:  # pragma: no cover - defensive
                raise
            rollback_failures, unknown_marked = _rollback_scheduler_mutation(
                dispatcher,
                handle,
                rollback_operation="qhold",
                spec_path=spec_path,
                before=before_write,
                after=after_write,
                local_write_attempted=local_write_attempted,
                unknown_scheduler_state=_RELEASE_OUTCOME_UNKNOWN,
                multi_user=multi_user,
            )
            if rollback_failures:
                recovery = (
                    f"; durable spec marked {_RELEASE_OUTCOME_UNKNOWN}; "
                    f"vq pause {scheduler_target} {jobid} is the exact "
                    "qhold recovery"
                    if unknown_marked
                    else ""
                )
                raise PauseError(
                    f"scheduler qrls for job {jobid} failed during remote "
                    f"mutation: {_failure_text(exc)}; "
                    f"{'; '.join(rollback_failures)}; outcome unknown"
                    f"{recovery}"
                ) from exc
            if isinstance(exc, (ConfigError, ownership.OwnershipError)):
                raise
            if not isinstance(exc, Exception):
                raise
            raise PauseError(
                f"{_failure_text(exc)}; scheduler rollback completed"
            ) from exc

    try:
        events.append_event(
            Path(spec.cwd),
            events.EventKind.STATE_TRANSITION,
            jobid,
            **{
                "from": JobState.SUSPENDED.value,
                "to": JobState.RUNNING.value,
                "reason": (
                    f"vq resume (qrls, held {paused_seconds:.1f}s this cycle)"
                ),
            },
        )
    except Exception as exc:  # pragma: no cover - append_event is best-effort
        log.warning(
            "job %s: committed scheduler release but event reporting failed: %s",
            jobid,
            _failure_text(exc),
        )
    return (
        f"released scheduler job {jobid} (qrls {handle.job_id} on "
        f"{scheduler_target}; held this cycle: {paused_seconds:.1f}s, "
        f"total: {spec.paused_seconds_total:.1f}s)"
    )


def pause_scheduler_all(
    scheduler_target: str,
    host_cfg: HostConfig,
    *,
    paused_by: str | None = None,
    queue_dir: Path | None = None,
    multi_user: bool = False,
) -> str:
    """Hold every running scheduler-backed job for ``scheduler_target``."""
    if multi_user:
        spec_paths = _all_user_spec_paths()
    else:
        queue_dir = queue_dir or paths.queue_dir()
        if not queue_dir.exists():
            return "paused 0 jobs (queue dir empty)"
        spec_paths = sorted(queue_dir.glob("*.json"))

    paused: list[str] = []
    already_suspended: list[str] = []
    skipped: list[str] = []
    errors: list[tuple[str, str]] = []

    policy = (
        ownership.authorization_policy(multi_user=multi_user)
        if spec_paths
        else None
    )
    for spec_path in _bulk_control_candidates(
        spec_paths, multi_user=multi_user, policy=policy,
    ):
        try:
            with _locked_authorized_spec(
                spec_path, multi_user=multi_user, policy=policy,
            ) as spec:
                if spec.scheduler_target != scheduler_target:
                    continue
                if spec.state == JobState.SUSPENDED:
                    already_suspended.append(spec.id)
                    continue
                if spec.state != JobState.RUNNING:
                    skipped.append(spec.id)
                    continue
                jobid = spec.id
        except ownership.OwnershipError as exc:
            errors.append((spec_path.stem, str(exc)))
            continue
        except ConfigError:
            raise
        except Exception:
            continue
        try:
            pause_scheduler_job(
                scheduler_target,
                host_cfg,
                jobid,
                paused_by=paused_by,
                queue_dir=queue_dir,
                multi_user=multi_user,
            )
            paused.append(jobid)
        except (PauseError, FileNotFoundError, ownership.OwnershipError) as exc:
            errors.append((jobid, str(exc)))

    return _format_bulk_summary(
        verb="paused",
        primary=paused,
        already=already_suspended,
        already_word="already suspended",
        skipped=skipped,
        skipped_reason="not RUNNING",
        errors=errors,
    )


def resume_scheduler_all(
    scheduler_target: str,
    host_cfg: HostConfig,
    *,
    paused_by_filter: str | None = None,
    queue_dir: Path | None = None,
    multi_user: bool = False,
) -> str:
    """Release every held scheduler-backed job for ``scheduler_target``."""
    if multi_user:
        spec_paths = _all_user_spec_paths()
    else:
        queue_dir = queue_dir or paths.queue_dir()
        if not queue_dir.exists():
            return "resumed 0 jobs (queue dir empty)"
        spec_paths = sorted(queue_dir.glob("*.json"))

    resumed: list[str] = []
    already_running: list[str] = []
    skipped: list[str] = []
    paused_by_other: list[str] = []
    errors: list[tuple[str, str]] = []

    policy = (
        ownership.authorization_policy(multi_user=multi_user)
        if spec_paths
        else None
    )
    for spec_path in _bulk_control_candidates(
        spec_paths, multi_user=multi_user, policy=policy,
    ):
        try:
            with _locked_authorized_spec(
                spec_path, multi_user=multi_user, policy=policy,
            ) as spec:
                if spec.scheduler_target != scheduler_target:
                    continue
                if spec.state == JobState.RUNNING:
                    already_running.append(spec.id)
                    continue
                if spec.state != JobState.SUSPENDED:
                    skipped.append(spec.id)
                    continue
                if (
                    paused_by_filter is not None
                    and spec.paused_by != paused_by_filter
                ):
                    paused_by_other.append(spec.id)
                    continue
                jobid = spec.id
        except ownership.OwnershipError as exc:
            errors.append((spec_path.stem, str(exc)))
            continue
        except ConfigError:
            raise
        except Exception:
            continue
        try:
            resume_scheduler_job(
                scheduler_target,
                host_cfg,
                jobid,
                paused_by_filter=paused_by_filter,
                queue_dir=queue_dir,
                multi_user=multi_user,
            )
            resumed.append(jobid)
        except (PauseError, FileNotFoundError, ownership.OwnershipError) as exc:
            errors.append((jobid, str(exc)))

    summary = _format_bulk_summary(
        verb="resumed",
        primary=resumed,
        already=already_running,
        already_word="already running",
        skipped=skipped,
        skipped_reason="not SUSPENDED",
        errors=errors,
    )
    if paused_by_other:
        summary += (
            f" [{len(paused_by_other)} paused by other tag, "
            f"left paused]"
        )
    return summary


def pause_all(
    host: str,
    *,
    paused_by: str | None = None,
    queue_dir: Path | None = None,
    queue_root: Path | None = None,
    multi_user: bool = False,
    exclude_jobids: set[str] | None = None,
) -> str:
    """Pause every RUNNING job in the queue. Idempotent: jobs already
    SUSPENDED are left alone; non-RUNNING/SUSPENDED jobs are ignored.

    v0.6.22: ``paused_by`` records the actor tag on each newly-
    paused spec. Pair with ``resume_all(paused_by_filter=...)`` to
    resume only this batch — useful for scripts that want to pause
    the queue, do work, and resume without disturbing operator-
    paused jobs.

    Returns a summary line like "paused 4 jobs (3 already suspended,
    7 not in a pausable state)". Useful when the user wants to free
    CPU for something else without manually identifying every job.
    """
    if not is_local_host(host):
        raise NotImplementedError(
            f"pause --all for {host!r}: CLI must dispatch via SSH"
        )
    # v0.6.38: in multi-user mode sweep every per-user queue dir
    # under /var/lib/vq/users/<uid>/ instead of the single queue dir.
    if multi_user:
        spec_paths = _all_user_spec_paths(queue_root)
    else:
        queue_dir = queue_dir or paths.queue_dir()
        if not queue_dir.exists():
            return "paused 0 jobs (queue dir empty)"
        spec_paths = sorted(queue_dir.glob("*.json"))

    paused: list[str] = []
    already_suspended: list[str] = []
    skipped: list[str] = []
    errors: list[tuple[str, str]] = []

    policy = (
        ownership.authorization_policy(multi_user=multi_user)
        if spec_paths
        else None
    )
    for spec_path in _bulk_control_candidates(
        spec_paths, multi_user=multi_user, policy=policy,
    ):
        try:
            with _locked_authorized_spec(
                spec_path, multi_user=multi_user, policy=policy,
            ) as spec:
                if exclude_jobids and spec.id in exclude_jobids:
                    # The caller IS this job (a build-env job pausing the
                    # queue around its own rebuild). The single-job helper
                    # also refuses its own process group as defense in depth.
                    skipped.append(spec.id)
                    continue
                if spec.state == JobState.SUSPENDED:
                    already_suspended.append(spec.id)
                    continue
                if spec.state != JobState.RUNNING:
                    skipped.append(spec.id)
                    continue
                jobid = spec.id
        except ownership.OwnershipError as exc:
            errors.append((spec_path.stem, str(exc)))
            continue
        except ConfigError:
            raise
        except Exception:
            continue  # corrupt spec; skip
        try:
            pause_job(
                host, jobid, paused_by=paused_by,
                queue_dir=queue_dir, queue_root=queue_root,
                multi_user=multi_user,
            )
            paused.append(jobid)
        except (PauseError, FileNotFoundError, ownership.OwnershipError) as exc:
            # PauseError can fire if the job's pgid is gone (race
            # with daemon reaping); we just skip those.
            errors.append((jobid, str(exc)))

    return _format_bulk_summary(
        verb="paused",
        primary=paused,
        already=already_suspended, already_word="already suspended",
        skipped=skipped, skipped_reason="not RUNNING",
        errors=errors,
    )


def pause_provides_branches(
    host: str,
    branches: list[str],
    *,
    queue_dir: Path | None = None,
    queue_root: Path | None = None,
    multi_user: bool = False,
    exclude_jobids: set[str] | None = None,
    paused_by: str | None = None,
) -> tuple[str, list[str]]:
    """v0.5.47: pause every RUNNING job whose ``spec.branch`` is in
    ``branches``. Returns ``(summary_line, paused_jobids)`` so the
    caller (typically ``admin.update_env``) can call
    :func:`resume_jobs` on exactly the set it paused, even when other
    jobs were paused independently by an operator before/during the
    update.

    Why the explicit-list return: ``resume_all`` resumes *every*
    SUSPENDED job. If an operator had manually paused a release-branch
    job before an admin update of the dev env, ``resume_all`` would
    also resume that operator-paused release job — a surprise.
    ``resume_jobs`` against the paused list scoped to the update keeps
    the operator's other pauses intact.

    Jobs whose ``spec.branch`` is None (submitted without ``--branch``,
    e.g. via ``--python`` or default routing) are NOT paused by this
    function — surgical scoping is opt-in via the spec branch tag.
    The caller (``admin.update_env``) decides whether to fall back to
    :func:`pause_all` for the "no branch tag → unknown env coverage"
    case; this helper just does the filtered pause.

    Empty ``branches`` list returns ("paused 0 jobs (no branches to
    match)", []) — same shape as the other bulk helpers."""
    if not is_local_host(host):
        raise NotImplementedError(
            f"pause_provides_branches for {host!r}: "
            f"CLI must dispatch via SSH"
        )
    if not branches:
        return "paused 0 jobs (no branches to match)", []
    # v0.6.38: multi-user sweeps every per-user queue dir.
    if multi_user:
        spec_paths = _all_user_spec_paths(queue_root)
    else:
        queue_dir = queue_dir or paths.queue_dir()
        if not queue_dir.exists():
            return "paused 0 jobs (queue dir empty)", []
        spec_paths = sorted(queue_dir.glob("*.json"))

    branch_set = set(branches)
    paused: list[str] = []
    already_suspended: list[str] = []
    skipped: list[str] = []
    errors: list[tuple[str, str]] = []

    policy = (
        ownership.authorization_policy(multi_user=multi_user)
        if spec_paths
        else None
    )
    for spec_path in _bulk_control_candidates(
        spec_paths, multi_user=multi_user, policy=policy,
    ):
        try:
            with _locked_authorized_spec(
                spec_path, multi_user=multi_user, policy=policy,
            ) as spec:
                if spec.branch not in branch_set:
                    # Out of scope for this update -- leave it alone,
                    # including the explicit untagged case.
                    continue
                if exclude_jobids and spec.id in exclude_jobids:
                    # The job executing this update (see pause_all).
                    skipped.append(spec.id)
                    continue
                if spec.state == JobState.SUSPENDED:
                    already_suspended.append(spec.id)
                    continue
                if spec.state != JobState.RUNNING:
                    skipped.append(spec.id)
                    continue
                jobid = spec.id
        except ownership.OwnershipError as exc:
            errors.append((spec_path.stem, str(exc)))
            continue
        except ConfigError:
            raise
        except Exception:
            continue
        try:
            pause_job(
                host, jobid, paused_by=paused_by,
                queue_dir=queue_dir, queue_root=queue_root,
                multi_user=multi_user,
            )
            paused.append(jobid)
        except (PauseError, FileNotFoundError, ownership.OwnershipError) as exc:
            errors.append((jobid, str(exc)))

    summary = _format_bulk_summary(
        verb="paused",
        primary=paused,
        already=already_suspended, already_word="already suspended",
        skipped=skipped, skipped_reason="not RUNNING",
        errors=errors,
    )
    # Annotate the summary with the scope so the operator sees what
    # was targeted (vs the queue-wide pause_all output).
    summary += f" [scope: branches={sorted(branch_set)}]"
    return summary, paused


@dataclass(frozen=True)
class PauseAdmissionProof:
    """Durable quiescence verdict before an admin mutates an environment."""

    paused_by: str
    pause_summary: str
    eligible_jobids: tuple[str, ...]
    reconciliation: PauseIntentReconcileResult
    unresolved: tuple[tuple[str, str], ...]

    @property
    def proven_quiescent(self) -> bool:
        return not self.unresolved

    @property
    def summary(self) -> str:
        if self.proven_quiescent:
            return (
                f"{self.pause_summary}; durable admission proved "
                f"{len(self.eligible_jobids)} eligible job(s) quiescent"
            )
        return (
            f"{self.pause_summary}; durable admission NOT proved "
            f"({len(self.unresolved)} unresolved)"
        )

    def require_quiescent(self) -> None:
        if self.proven_quiescent:
            return
        details = "; ".join(
            f"{jobid}: {reason}" for jobid, reason in self.unresolved[:8]
        )
        if len(self.unresolved) > 8:
            details += f"; ... {len(self.unresolved) - 8} more"
        raise PauseError(
            f"paused_by={self.paused_by!r} admission is not durably proven: "
            f"{details}"
        )


def _process_group_alive(pgid: int | None) -> bool:
    """Conservative POSIX liveness probe used only for admission proof."""
    if pgid is None or pgid <= 0:
        return False
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Lack of signal authority is evidence the process exists, not that it
        # is quiescent.
        return True
    return True


def pause_token_scope_with_proof(
    host: str,
    paused_by: str,
    *,
    branches: Sequence[str] | None = None,
    queue_dir: Path | None = None,
    queue_root: Path | None = None,
    multi_user: bool = False,
    exclude_jobids: set[str] | None = None,
) -> PauseAdmissionProof:
    """Pause an admin scope and prove every eligible local job quiescent.

    The first locked scan captures all jobs already RUNNING before the pause.
    The second scan proves those exact rows SUSPENDED (or their process groups
    gone) and also catches a job that raced from PENDING to RUNNING after the
    first scan.  Scheduler-target jobs execute in their remote runtime and are
    outside a local checkout/venv mutation's admission scope.
    """
    if not paused_by:
        raise PauseError("pause admission proof requires a paused_by token")
    if not is_local_host(host):
        raise NotImplementedError(
            f"pause admission proof for {host!r} is local-only"
        )
    queue_dir = _bind_proof_queue_root(
        queue_root, queue_dir=queue_dir, multi_user=multi_user,
    )
    excluded = exclude_jobids or set()
    branch_set = set(branches) if branches is not None else None

    def in_scope(spec: JobSpec) -> bool:
        return (
            spec.scheduler_target is None
            and spec.id not in excluded
            and (branch_set is None or spec.branch in branch_set)
        )

    # Finish any older crash intent before deciding what is RUNNING now.
    reconcile_pause_intents(
        host,
        queue_dir=queue_dir,
        queue_root=queue_root,
        multi_user=multi_user,
    )
    initial_paths = _pause_scope_spec_paths(
        queue_dir=queue_dir,
        queue_root=queue_root,
        multi_user=multi_user,
    )
    captured: dict[Path, tuple[str, int | None]] = {}
    unresolved: list[tuple[str, str]] = []
    policy = (
        ownership.authorization_policy(multi_user=multi_user)
        if initial_paths
        else None
    )
    for spec_path in initial_paths:
        try:
            with _locked_authorized_spec(
                spec_path, multi_user=multi_user, policy=policy,
            ) as spec:
                if spec_path.stem != spec.id:
                    unresolved.append(
                        (spec_path.stem, "spec filename/job id mismatch")
                    )
                elif in_scope(spec) and spec.state == JobState.RUNNING:
                    captured[spec_path] = (spec.id, spec.pgid)
        except ConfigError:
            raise
        except (
            OSError,
            ValueError,
            ownership.OwnershipError,
        ) as exc:
            # The unreadable row could be an in-scope RUNNING job.
            unresolved.append(
                (spec_path.stem, f"initial spec cannot be proven: {_failure_text(exc)}")
            )

    if branches is None:
        pause_summary = pause_all(
            host,
            paused_by=paused_by,
            queue_dir=queue_dir,
            queue_root=queue_root,
            multi_user=multi_user,
            exclude_jobids=excluded,
        )
    else:
        pause_summary, _ = pause_provides_branches(
            host,
            list(branches),
            paused_by=paused_by,
            queue_dir=queue_dir,
            queue_root=queue_root,
            multi_user=multi_user,
            exclude_jobids=excluded,
        )

    reconciliation = reconcile_pause_intents(
        host,
        queue_dir=queue_dir,
        queue_root=queue_root,
        multi_user=multi_user,
    )
    final_paths = set(
        _pause_scope_spec_paths(
            queue_dir=queue_dir,
            queue_root=queue_root,
            multi_user=multi_user,
        )
    )
    for missing_path, (jobid, _pgid) in captured.items():
        if missing_path not in final_paths:
            unresolved.append(
                (jobid, "captured RUNNING spec disappeared before proof")
            )

    for spec_path in sorted(final_paths):
        captured_row = captured.get(spec_path)
        try:
            with _locked_authorized_spec(
                spec_path, multi_user=multi_user, policy=policy,
            ) as spec:
                currently_eligible_running = (
                    in_scope(spec) and spec.state == JobState.RUNNING
                )
                if captured_row is None and not currently_eligible_running:
                    continue
                jobid, captured_pgid = captured_row or (spec.id, spec.pgid)
                if spec.pause_intent_at is not None:
                    unresolved.append(
                        (jobid, "durable pause intent remains unresolved")
                    )
                    continue
                if spec.state == JobState.SUSPENDED:
                    continue
                if spec.state == JobState.RUNNING:
                    unresolved.append(
                        (jobid, "eligible job remains RUNNING after pause")
                    )
                    continue
                pgid = spec.pgid if spec.pgid is not None else captured_pgid
                if _process_group_alive(pgid):
                    unresolved.append(
                        (
                            jobid,
                            f"state {spec.state.value} but pgid {pgid} is alive",
                        )
                    )
                else:
                    continue
        except ConfigError:
            raise
        except (
            OSError,
            ValueError,
            ownership.OwnershipError,
        ) as exc:
            jobid = captured_row[0] if captured_row else spec_path.stem
            unresolved.append(
                (jobid, f"final spec cannot be proven: {_failure_text(exc)}")
            )

    eligible_ids = tuple(sorted({jobid for jobid, _ in captured.values()}))
    return PauseAdmissionProof(
        paused_by=paused_by,
        pause_summary=pause_summary,
        eligible_jobids=eligible_ids,
        reconciliation=reconciliation,
        unresolved=tuple(unresolved),
    )


def resume_jobs(
    host: str,
    jobids: list[str],
    *,
    queue_dir: Path | None = None,
    queue_root: Path | None = None,
    multi_user: bool = False,
) -> str:
    """v0.5.47: resume the specific list of jobids passed in (typically
    the list returned by :func:`pause_provides_branches`). Per-job
    failures (jobid gone, spec corrupt, not SUSPENDED) are tolerated
    and counted in the summary — same lenient stance as
    :func:`resume_all`.

    Distinct from :func:`resume_all` which resumes EVERY SUSPENDED
    job: this targets only the explicit list, so operator-paused jobs
    outside the admin-update scope stay suspended."""
    if not is_local_host(host):
        raise NotImplementedError(
            f"resume_jobs for {host!r}: CLI must dispatch via SSH"
        )
    if not jobids:
        return "resumed 0 jobs (nothing to resume)"
    if not multi_user:
        queue_dir = queue_dir or paths.queue_dir()
        if not queue_dir.exists():
            return "resumed 0 jobs (queue dir empty)"

    resumed: list[str] = []
    already_running: list[str] = []
    skipped: list[str] = []
    errors: list[tuple[str, str]] = []

    for jobid in jobids:
        # v0.6.38: multi-user resolves each jobid across the per-user
        # queue dirs; single-user uses the one queue dir.
        try:
            spec_path = _resolve_one_spec_path(
                jobid, queue_dir, multi_user, queue_root,
            )
        except FileNotFoundError:
            errors.append((jobid, "spec file gone"))
            continue
        try:
            with _locked_authorized_spec(
                spec_path, multi_user=multi_user
            ) as spec:
                if spec.state == JobState.RUNNING:
                    already_running.append(jobid)
                    continue
                if spec.state != JobState.SUSPENDED:
                    skipped.append(jobid)
                    continue
        except ownership.OwnershipError as exc:
            errors.append((jobid, str(exc)))
            continue
        except ConfigError:
            raise
        except Exception as exc:
            errors.append((jobid, f"spec read failed: {exc}"))
            continue
        try:
            resume_job(
                host,
                jobid,
                queue_dir=queue_dir,
                queue_root=queue_root,
                multi_user=multi_user,
            )
            resumed.append(jobid)
        except (PauseError, FileNotFoundError, ownership.OwnershipError) as exc:
            errors.append((jobid, str(exc)))

    return _format_bulk_summary(
        verb="resumed",
        primary=resumed,
        already=already_running, already_word="already running",
        skipped=skipped, skipped_reason="not SUSPENDED",
        errors=errors,
    )


def resume_all(
    host: str,
    *,
    paused_by_filter: str | None = None,
    queue_dir: Path | None = None,
    queue_root: Path | None = None,
    multi_user: bool = False,
) -> str:
    """Resume every SUSPENDED job in the queue. Idempotent: jobs that
    are already RUNNING are left alone; non-SUSPENDED jobs are ignored.

    v0.6.22: ``paused_by_filter`` scopes the resume to jobs whose
    ``spec.paused_by`` matches the given tag. When set, SUSPENDED
    jobs with a different (or missing) ``paused_by`` value are
    skipped — leaving operator-paused jobs paused. None = resume
    every SUSPENDED job (pre-v0.6.22 behavior).

    The filter intentionally treats SUSPENDED jobs with
    ``paused_by=None`` as "out of scope" when a filter is given:
    untagged pauses can't be claimed by a tagged resume. A script
    that wants to resume legacy/untagged pauses too has to call
    ``resume_all`` without a filter.
    """
    if not is_local_host(host):
        raise NotImplementedError(
            f"resume --all for {host!r}: CLI must dispatch via SSH"
        )
    # v0.6.38: multi-user sweeps every per-user queue dir.
    if multi_user:
        spec_paths = _all_user_spec_paths(queue_root)
    else:
        queue_dir = queue_dir or paths.queue_dir()
        if not queue_dir.exists():
            return "resumed 0 jobs (queue dir empty)"
        spec_paths = sorted(queue_dir.glob("*.json"))

    resumed: list[str] = []
    already_running: list[str] = []
    skipped: list[str] = []
    paused_by_other: list[str] = []  # v0.6.22: filter mismatches
    errors: list[tuple[str, str]] = []

    policy = (
        ownership.authorization_policy(multi_user=multi_user)
        if spec_paths
        else None
    )
    for spec_path in _bulk_control_candidates(
        spec_paths, multi_user=multi_user, policy=policy,
    ):
        try:
            with _locked_authorized_spec(
                spec_path, multi_user=multi_user, policy=policy,
            ) as spec:
                if spec.state == JobState.RUNNING:
                    already_running.append(spec.id)
                    continue
                if spec.state != JobState.SUSPENDED:
                    skipped.append(spec.id)
                    continue
                # Apply paused_by_filter only after authorization so a
                # foreign operator tag cannot leak through the bulk summary.
                if (
                    paused_by_filter is not None
                    and spec.paused_by != paused_by_filter
                ):
                    paused_by_other.append(spec.id)
                    continue
                jobid = spec.id
        except ownership.OwnershipError as exc:
            errors.append((spec_path.stem, str(exc)))
            continue
        except ConfigError:
            raise
        except Exception:
            continue
        try:
            resume_job(
                host,
                jobid,
                paused_by_filter=paused_by_filter,
                queue_dir=queue_dir,
                queue_root=queue_root,
                multi_user=multi_user,
            )
            resumed.append(jobid)
        except (PauseError, FileNotFoundError, ownership.OwnershipError) as exc:
            errors.append((jobid, str(exc)))

    # Same summary shape, with a v0.6.22 addition for paused-by
    # filter mismatches. _format_bulk_summary doesn't have a slot
    # for that yet — append it inline.
    summary = _format_bulk_summary(
        verb="resumed",
        primary=resumed,
        already=already_running, already_word="already running",
        skipped=skipped, skipped_reason="not SUSPENDED",
        errors=errors,
    )
    if paused_by_other:
        summary += (
            f" [{len(paused_by_other)} paused by other tag, "
            f"left paused]"
        )
    return summary


@dataclass(frozen=True)
class ResumeScopeProof:
    """Durable verdict for one token-scoped admin resume.

    A human summary from ``resume_all`` is deliberately not evidence: that
    API tolerates per-job failures for interactive use.  This object is proof
    based on a second, locked scan of every durable spec after pending pause
    intents have first been reconciled.
    """

    paused_by: str
    resume_summary: str
    reconciliation: PauseIntentReconcileResult
    unresolved: tuple[tuple[str, str], ...]

    @property
    def proven_clear(self) -> bool:
        return not self.unresolved

    @property
    def summary(self) -> str:
        if self.proven_clear:
            return f"{self.resume_summary}; durable token scope is clear"
        return (
            f"{self.resume_summary}; durable token scope NOT clear "
            f"({len(self.unresolved)} unresolved)"
        )

    def require_clear(self) -> None:
        """Raise with bounded exact detail unless the durable scan is clear."""
        if self.proven_clear:
            return
        details = "; ".join(
            f"{jobid}: {reason}" for jobid, reason in self.unresolved[:8]
        )
        if len(self.unresolved) > 8:
            details += f"; ... {len(self.unresolved) - 8} more"
        raise PauseError(
            f"paused_by={self.paused_by!r} resume is not durably proven: "
            f"{details}"
        )


def _prove_pause_token_absent(
    paused_by: str,
    *,
    queue_dir: Path | None,
    queue_root: Path | None,
    multi_user: bool,
) -> tuple[tuple[str, str], ...]:
    """Locked second pass proving no durable row still owns ``paused_by``."""
    unresolved: list[tuple[str, str]] = []
    scope_paths = _pause_scope_spec_paths(
        queue_dir=queue_dir,
        queue_root=queue_root,
        multi_user=multi_user,
    )
    policy = (
        ownership.authorization_policy(multi_user=multi_user)
        if scope_paths
        else None
    )
    for spec_path in scope_paths:
        try:
            with _locked_authorized_spec(
                spec_path, multi_user=multi_user, policy=policy,
            ) as spec:
                reasons: list[str] = []
                if (
                    spec.pause_intent_at is not None
                    and spec.pause_intent_by == paused_by
                ):
                    reasons.append(
                        f"pause intent remains for pgid {spec.pause_intent_pgid}"
                    )
                # A successful resume clears paused_by in the same atomic spec
                # write that records RUNNING.  Treat the token in *any* state
                # as unresolved: a terminal writer racing a stopped process is
                # not proof that SIGCONT occurred.
                if spec.paused_by == paused_by:
                    reasons.append(
                        f"paused_by token remains in state {spec.state.value}"
                    )
                if reasons:
                    unresolved.append((spec.id, ", ".join(reasons)))
        except ConfigError:
            raise
        except (
            OSError,
            ValueError,
            ownership.OwnershipError,
        ) as exc:
            # An unreadable row might be the stopped job carrying this token.
            # Exact proof therefore fails closed instead of copying the bulk
            # interactive helpers' historical "skip corrupt spec" stance.
            unresolved.append(
                (spec_path.stem, f"spec cannot be proven: {_failure_text(exc)}")
            )
    return tuple(unresolved)


def prove_pause_token_absent(
    host: str,
    paused_by: str,
    *,
    queue_dir: Path | None = None,
    queue_root: Path | None = None,
    multi_user: bool = False,
) -> ResumeScopeProof:
    """Prove one pause token absent without changing queue or process state.

    Orphan-receipt quarantine is intentionally narrower than ordinary update
    recovery: it may retire evidence only when the old pause transaction was
    already cleared.  Calling :func:`resume_token_scope_with_proof` here would
    reconcile intents, send resume signals, and rewrite job specs before the
    orphan admission is complete.  This helper performs only the locked scan
    used by that mutating primitive and therefore remains safe for dry-run.
    """
    if not paused_by:
        raise PauseError("pause absence proof requires a non-empty paused_by token")
    if not is_local_host(host):
        raise NotImplementedError(
            f"pause absence proof for {host!r} is local-only"
        )
    queue_dir = _bind_proof_queue_root(
        queue_root, queue_dir=queue_dir, multi_user=multi_user,
    )
    unresolved = _prove_pause_token_absent(
        paused_by,
        queue_dir=queue_dir,
        queue_root=queue_root,
        multi_user=multi_user,
    )
    return ResumeScopeProof(
        paused_by=paused_by,
        resume_summary="read-only pause-token scan",
        reconciliation=PauseIntentReconcileResult((), (), ()),
        unresolved=unresolved,
    )


def resume_token_scope_with_proof(
    host: str,
    paused_by: str,
    *,
    queue_dir: Path | None = None,
    queue_root: Path | None = None,
    multi_user: bool = False,
) -> ResumeScopeProof:
    """Resume one actor token and prove no stopped/intended row remains.

    This is the admin-update primitive.  Interactive ``resume_all`` retains
    its lenient string-returning API, while lifecycle code gets a typed verdict
    that must be clear before it removes its crash-recovery marker/receipt.
    """
    if not paused_by:
        raise PauseError("resume proof requires a non-empty paused_by token")
    queue_dir = _bind_proof_queue_root(
        queue_root, queue_dir=queue_dir, multi_user=multi_user,
    )
    reconciliation = reconcile_pause_intents(
        host,
        queue_dir=queue_dir,
        queue_root=queue_root,
        multi_user=multi_user,
    )
    resume_summary = resume_all(
        host,
        paused_by_filter=paused_by,
        queue_dir=queue_dir,
        queue_root=queue_root,
        multi_user=multi_user,
    )
    unresolved = list(
        _prove_pause_token_absent(
            paused_by,
            queue_dir=queue_dir,
            queue_root=queue_root,
            multi_user=multi_user,
        )
    )
    # Reconciliation errors can describe a row that a racing terminal writer
    # subsequently removed.  The second pass is authoritative when it read the
    # row cleanly; retain only errors whose path was not otherwise proven clear
    # because a missing/corrupt row remains ambiguous.
    unresolved_ids = {jobid for jobid, _ in unresolved}
    for jobid, detail in reconciliation.errors:
        if jobid not in unresolved_ids:
            try:
                spec_path = _resolve_one_spec_path(
                    jobid, queue_dir, multi_user, queue_root,
                )
                with _locked_authorized_spec(
                    spec_path, multi_user=multi_user,
                ) as spec:
                    if (
                        spec.pause_intent_by != paused_by
                        and spec.paused_by != paused_by
                    ):
                        continue
            except (OSError, ValueError, ownership.OwnershipError):
                pass
            unresolved.append((jobid, f"pause reconciliation failed: {detail}"))
    return ResumeScopeProof(
        paused_by=paused_by,
        resume_summary=resume_summary,
        reconciliation=reconciliation,
        unresolved=tuple(unresolved),
    )


def _format_bulk_summary(
    *,
    verb: str,
    primary: list[str],
    already: list[str],
    already_word: str,
    skipped: list[str],
    skipped_reason: str,
    errors: list[tuple[str, str]],
) -> str:
    """Common one-line summary format for pause_all / resume_all etc."""
    plural = "s" if len(primary) != 1 else ""
    head = f"{verb} {len(primary)} job{plural}"
    suffix_parts: list[str] = []
    if already:
        suffix_parts.append(f"{len(already)} {already_word}")
    if skipped:
        suffix_parts.append(f"{len(skipped)} {skipped_reason}")
    if errors:
        suffix_parts.append(f"{len(errors)} error(s)")
    if suffix_parts:
        return head + " (" + ", ".join(suffix_parts) + ")"
    return head
