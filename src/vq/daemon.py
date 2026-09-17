"""Daemon main loop: poll the queue dir, dispatch pending jobs, reconcile completion.

Single-process, single-host. Tracks running children's Popen objects in memory.

v0.3 adds a Watchdog: every iterate() pass samples each running job's RSS and
CPU time and may escalate via SIGTERM -> grace -> SIGKILL when memory caps,
wall-time, or CPU-starvation thresholds fire. Watchdog kills land in distinct
terminal states (OOM_KILLED, STARVED, TIME_EXCEEDED) so users can tell them
apart from manual `vq kill` (KILLED).

v0.4 refines daemon-restart recovery. The old behaviour ("any RUNNING spec
becomes INTERRUPTED at startup") was correct only when the daemon's process
exited because the host died. A daemon-only restart (systemd reload, crash +
restart) leaves child process groups alive (start_new_session=True puts each
child in its own session, parented to init after the daemon goes). v0.4 uses
the pgid stored in v0.3 specs to distinguish:

* process group alive at restart -> stay RUNNING, re-register with watchdog,
  reconcile via periodic killpg(pgid, 0) instead of popen.poll() (the popen
  is gone with the old daemon process). When the orphan eventually dies the
  daemon notices via the next reconcile pass and writes ABORTED_BY_QUEUE
  (reason ``daemon_restart_orphan``) -- unless the v0.5.9 exit-marker below
  recovers the real rc, in which case it writes COMPLETED / FAILED.
* process group gone at restart -> ABORTED_BY_QUEUE.
* spec has no pgid (pre-v0.3 spec on disk) -> ABORTED_BY_QUEUE unconditionally.
  We can't safely check liveness without a pgid.

(``INTERRUPTED`` was the v0.4 design's name for these; the shipped daemon
never writes it -- it survives only as a legacy state for reading pre-v0.4
specs. See ``vq.spec.JobState.INTERRUPTED`` and daemon ``_mark_aborted_by_queue``.)

v0.5.9 closes the cross-restart-completion gap. v0.4's orphan recovery was
correct ("we don't know the rc, mark ABORTED_BY_QUEUE") but pessimistic for
the case the v0.6 ``vq admin update vq`` flow needs: pause jobs, restart
daemon, resume jobs, let them finish. Resumed jobs would re-attach as
orphans (the new daemon doesn't have their Popen handle), and on graceful
exit get misclassified as ABORTED_BY_QUEUE. v0.5.9 wraps every dispatched
command with a helper that writes the user command's exit code to
``<workspace>/_vq/exit-code`` before exiting; the orphan reconciler reads that
marker when a pgid disappears and uses the recovered rc to mark COMPLETED /
FAILED. The helper now also writes terminal resource usage. ABORTED_BY_QUEUE
is reserved for the genuinely unknown-rc case (SIGKILL of the wrapper, host
crash, pre-v0.5.9 spec).
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import logging
import math
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import Enum, auto
from pathlib import Path
from types import FrameType
from typing import IO

import vq.watchdog as _vq_watchdog
from vq import (
    admin,
    admission,
    build_job,
    capacity,
    cgroup,
    dispatch,
    drain,
    events,
    notify,
    paths,
    resource_receipt,
    throttle,
)
from vq.config import (
    BinaryProgram,
    ConfigError,
    VenvProgram,
    _git_sha_matches,
    config_path,
    load_config,
    run_import_identity_probe,
)
from vq.scheduler_dialect import (
    DialectError,
    QstatDetail,
    SchedulerPhase,
    enforce_scheduler_wall_time_limit,
)
from vq.scheduler_dispatch import (
    SchedulerDispatcher,
    SchedulerError,
    SchedulerHandle,
    SchedulerPollEvidence,
    SchedulerSubmitOutcomeUnknown,
    scheduler_dispatcher_for,
    scheduler_handle_for_spec,
)
from vq.spec import (
    TERMINAL_STATES,
    JobSpec,
    JobState,
    ProgramRuntimePin,
    utcnow_iso,
    validate_job_id,
)
from vq.submit import ensure_idempotency_claim_for_spec, new_jobid
from vq.watchdog import (
    HostPressureAction,
    Watchdog,
    WatchdogAction,
    killpg,
)

log = logging.getLogger(__name__)

EXIT_MARKER_RELPATH = "_vq/exit-code"
_SCHEDULER_POLL_DIAGNOSTIC_LIMIT = 240
_SCHEDULER_POLL_SECRET = re.compile(
    r"(?i)\b(bearer|password|secret|token)\b(?:\s*[:=]?\s*)\S+"
)
MULTI_USER_SPEC_MAX_BYTES = 8 * 1024 * 1024
"""Maximum queue-record size admitted from a user-writable directory.

Eight MiB is far above normal JobSpec sizes while bounding root-daemon memory
use. The ordinary trusted ``JobSpec.read`` contract is deliberately unchanged.
"""


def _safe_scheduler_poll_diagnostic(value: object) -> str:
    """Return one bounded line suitable for specs, events, and daemon logs."""
    text = "".join(
        character if character.isprintable() else " " for character in str(value)
    )
    text = " ".join(text.split())
    text = _SCHEDULER_POLL_SECRET.sub(r"\1 <redacted>", text)
    return text[:_SCHEDULER_POLL_DIAGNOSTIC_LIMIT] or "scheduler poll unavailable"


def _read_untrusted_multi_user_spec(path: Path) -> JobSpec:
    """Read one untrusted queue record without following or blocking on it.

    Multi-user queue entries are controlled by the target uid. Open with
    ``O_NOFOLLOW`` and ``O_NONBLOCK``, require the opened object itself to be a
    regular file, bound bytes, reject a size change during the read, and decode
    UTF-8 strictly before model validation. This helper is intentionally used
    only for multi-user discovery, dispatch claims, and active rereads;
    trusted single-user reads retain their legacy behavior.
    """
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    fd = os.open(path, flags)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError(f"untrusted spec {path} is not a regular file")
        if before.st_size > MULTI_USER_SPEC_MAX_BYTES:
            raise ValueError(
                f"untrusted spec {path} is {before.st_size} bytes; maximum is "
                f"{MULTI_USER_SPEC_MAX_BYTES}"
            )

        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(
                fd,
                min(64 * 1024, MULTI_USER_SPEC_MAX_BYTES + 1 - total),
            )
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > MULTI_USER_SPEC_MAX_BYTES:
                raise ValueError(
                    f"untrusted spec {path} grew beyond "
                    f"{MULTI_USER_SPEC_MAX_BYTES} bytes while being read"
                )

        after = os.fstat(fd)
        if after.st_size != before.st_size or total != before.st_size:
            raise ValueError(f"untrusted spec {path} changed size while being read")
        text = b"".join(chunks).decode("utf-8")
    finally:
        os.close(fd)
    return JobSpec.from_json(text)

# v0.5.31: retry-on-failure exponential backoff. The Nth retry (1-indexed)
# waits RETRY_BACKOFF_BASE_SECONDS * 2^(N-1), capped at
# RETRY_BACKOFF_MAX_SECONDS. retry 1 -> 10s, 2 -> 20s, 3 -> 40s, 4 -> 80s,
# ... 7 -> 640s, 8+ -> 600s (capped). The backoff matters for transient
# failures (resource contention, a filesystem hiccup); a deterministic
# failure fails fast each time regardless, so the user just sees N quick
# attempts spaced by the backoff.
RETRY_BACKOFF_BASE_SECONDS = 10
RETRY_BACKOFF_MAX_SECONDS = 600

# STATE-3 (v0.8.14): grace between an external `vq kill`'s SIGTERM and the
# daemon's SIGKILL escalation for a SIGTERM-ignoring process. Mirrors the
# watchdog's SIGTERM->grace->SIGKILL grace (Watchdog.grace_seconds = 10.0).
KILL_ESCALATION_GRACE_SECONDS = 10.0

# §18: how often the driver rewrites a live scheduler job's spec to refresh the
# qstat detail (elapsed walltime, exec host). Bounded so a long run doesn't churn
# the spec every poll; a transition (queued->running) always writes immediately.
SCHEDULER_STATUS_REFRESH_SECONDS = 60.0
SCHEDULER_STATUS_RPC_MAX_SECONDS = 30.0
SCHEDULER_REATTACH_RETRY_SECONDS = 60.0
# A scheduler submit can cross several serial SSH/staging calls. Reconcile
# cluster state between bounded chunks of a large submit burst so a daemonless
# host's completed jobs release capacity without waiting for the whole burst.
# Yield the old pending snapshot too: new arrivals and admission holds must be
# observed before the next chunk, rather than waiting behind the entire burst.
# The bound is this quantum plus at most one in-flight submit attempt.
SCHEDULER_DISPATCH_RECONCILE_QUANTUM_SECONDS = 5.0

SCHEDULER_UNTRACKABLE_RESERVATION_SECONDS = 900.0
"""How long an untrackable scheduler spec keeps reserving a dispatch slot.

A ``reattach_failed`` spec reserves capacity against ``max_scheduler_jobs``
because it *may* still be live on the cluster. For a spec that still has its
``scheduler_job_id`` that reservation is open-ended and correct -- vq holds a
handle, so ``_reconcile_scheduler`` will eventually reap it.

A spec with **no** id is different: vq has nothing to poll, and the only
recovery is ``recorded_job_id``, which the retry tick attempts every
``SCHEDULER_REATTACH_RETRY_SECONDS``. A live job writes that record before it
runs anything, so surviving this window without one means vq will never recover
it -- and reserving for it forever is not caution, it is permanent starvation.

pbs-cluster, 2026-08-01: ``qstat`` showed exactly one of the user's jobs while vq
counted 121 active, so ~120 unrecoverable specs held the entire scheduler
budget and an almost-empty cluster dispatched nothing. Generous on purpose --
15 minutes is many retry ticks, so a merely slow or briefly unreachable
cluster never trips it.
"""

# pbs-cluster/Torque can drop a short job from qstat before the shared workspace has
# made the generated _vq/exit-code marker visible to the off-cluster driver.
# Treat the marker as the output-complete fence and wait briefly before deciding
# that a queue-finished scheduler job is genuinely missing its marker.
SCHEDULER_FINISHED_MARKER_GRACE_SECONDS = 120.0

SCHEDULER_FETCH_FAILURE_LIMIT = 3
"""Consecutive `fetch_results` failures tolerated after a job has left the
scheduler, before terminal classification proceeds without its artifacts.

Parking a job on a fetch failure encodes an assumption -- that a later attempt
can succeed. That holds for a transport hiccup and is false when the remote
workspace is gone, and vq cannot always tell the two apart from the error text.
pbs-cluster 2026-07-26: jobs 04b5d4b0b46c (rc 127) and 9e2f3dc15a78 (rc 0) had both
left PBS with durable, authoritative `_vq/exit-code` markers already read, yet
sat `running`/`fetch_failed` indefinitely because `tar -cf {ws}.result.tar -C
{ws} .` could not archive a workspace that no longer existed. An exit code vq
already holds must not be discarded because a *separate* operation failed.
"""

# Thread-pool libraries otherwise default to the host's full core count on many
# scientific stacks, which can make a local `--cpus N` job consume far more.
_THREAD_CAP_ENV_VARS = (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "BLIS_NUM_THREADS",
)

_UNREAD_RUNTIME_SHA = object()


def _vq_job_env(spec: JobSpec, *, workdir: str) -> dict[str, str]:
    """Environment variables vq promises to every job.

    Local dispatch merges these into the inherited process environment;
    scheduler dispatch passes them to the generated qsub script. Keep this as
    the single source for VQ_* metadata so pbs-cluster scheduler jobs see the same
    array/chain/rerun/program context as local jobs.
    """
    env = {
        "VQ_WORKDIR": workdir,
        "VQ_JOB_ID": spec.id,
        "VQ_CPUS": str(spec.cpus),
    }
    if spec.scheduler_tasks is not None:
        env["VQ_SCHEDULER_TASKS"] = str(spec.scheduler_tasks)
    if spec.mem_mb is not None:
        env["VQ_MEM_MB"] = str(spec.mem_mb)
    if spec.wall_time_seconds is not None:
        env["VQ_WALL_TIME_SECONDS"] = str(spec.wall_time_seconds)
    if spec.program is not None:
        env["VQ_PROGRAM"] = spec.program
    if (
        spec.program_runtime_pin is not None
        and spec.program_runtime_pin.resolved_git_sha is not None
    ):
        env["VQ_PROGRAM_GIT_SHA"] = (
            spec.program_runtime_pin.resolved_git_sha
        )
    if spec.array_index is not None:
        env["VQ_ARRAY_INDEX"] = str(spec.array_index)
        env["VQ_ARRAY_TOTAL"] = str(spec.array_total or 1)
        env["VQ_ARRAY_GROUP_ID"] = spec.array_group_id or ""
    if spec.chain_index is not None:
        env["VQ_CHAIN_INDEX"] = str(spec.chain_index)
        env["VQ_CHAIN_TOTAL"] = str(spec.chain_total or 1)
        env["VQ_CHAIN_GROUP_ID"] = spec.chain_group_id or ""
    if spec.rerun_until_file_exists:
        env["VQ_RERUN_COUNT"] = str(spec.rerun_count)
        env["VQ_RERUN_MAX"] = str(spec.rerun_max)
    return env


def _program_env(
    spec: JobSpec, *, include_host_paths: bool = True
) -> dict[str, str]:
    """Expose managed program paths to the job payload.

    ``--program NAME`` remains a validation/identity mechanism: vq does not
    rewrite the submitted command. These env vars give wrappers and directory
    payloads a stable way to call the configured program without hard-coding a
    host-specific path.

    * venv programs (local dispatch): ``VQ_PROGRAM_BIN`` (the directory
      holding the interpreter and console scripts), ``VQ_PROGRAM_PYTHON``,
      and ``VQ_PROGRAM_GIT_DIR``; ``VQ_PROGRAM_BRANCH`` is portable and also
      exported to scheduler jobs.
    * binary programs (local dispatch): ``VQ_PROGRAM_EXE`` (the validated
      executable). For venv programs ``VQ_PROGRAM_BIN`` is a directory, so
      binary programs get their own variable rather than overloading it.

    Scheduler jobs never receive driver-local paths: their runtime is
    selected by ``scheduler_program_hooks`` and may use an unrelated
    filesystem layout.
    """
    if spec.program is None:
        return {}
    try:
        cfg = load_config()
    except ConfigError:
        return {}
    prog = cfg.programs.get(spec.program)
    env: dict[str, str] = {}
    if isinstance(prog, VenvProgram):
        if include_host_paths:
            env.update(
                VQ_PROGRAM_BIN=str(Path(prog.python).parent),
                VQ_PROGRAM_GIT_DIR=prog.git_dir,
                VQ_PROGRAM_PYTHON=prog.python,
            )
        if prog.branch:
            env["VQ_PROGRAM_BRANCH"] = prog.branch
    elif isinstance(prog, BinaryProgram) and include_host_paths:
        env["VQ_PROGRAM_EXE"] = prog.binary
    return env


# v0.6.23 / HP-1 (v0.8.24): the `paused_by` tag the host-pressure auto-pause
# stamps on a spec. Distinguishes a watchdog pressure-pause from an operator
# `vq pause` so (a) `vq status` shows WHY a job is paused, and (b) the
# startup re-seed (HP-1) resumes only watchdog pauses, never operator ones.
HOST_PRESSURE_PAUSE_TAG = "watchdog_host_pressure"

# v0.12.0 build-as-job: a build job (``JobSpec.build_env`` set) dispatches at
# a reserved-high priority so that, on a host the --refresh drain just
# emptied, it is ``pending[0]`` and dispatches before any normal work, then
# holds the host exclusively until it finishes. Above any sane user priority.
# v0.12.x: canonical home moved to vq.build_job (shared with the auto-update
# submit front door); re-exported so vq.daemon.BUILD_JOB_PRIORITY and the
# tests that import it from here keep working.
BUILD_JOB_PRIORITY = build_job.BUILD_JOB_PRIORITY


def _retry_backoff_seconds(retry_count: int) -> int:
    """Exponential backoff (seconds) before the ``retry_count``-th retry.
    ``retry_count`` is 1-indexed (the first retry passes 1)."""
    delay = RETRY_BACKOFF_BASE_SECONDS * (2 ** (retry_count - 1))
    return min(delay, RETRY_BACKOFF_MAX_SECONDS)


def _not_before_ready(not_before: str | None, now: datetime) -> bool:
    """Whether a PENDING spec's ``not_before`` retry-backoff gate has passed.

    ``None`` (no constraint) and any unparseable/incompatible value are both
    treated as "ready now". Crucially this includes a *naive* (timezone-less)
    timestamp: ``datetime.fromisoformat`` happily parses it to a naive
    datetime, and ``naive <= aware`` raises ``TypeError`` — which pre-v0.8.9
    was uncaught here and aborted the entire ``_dispatch_pending`` tick via
    the ``sorted()`` key, stalling *all* new dispatch every poll while one
    poisoned (hand-edited / corrupt) spec sat PENDING. Per the documented
    intent (``JobSpec.not_before``): a bad timestamp must never permanently
    trap a job — and it must never wedge the queue for everyone else either.
    """
    if not_before is None:
        return True
    try:
        return datetime.fromisoformat(not_before) <= now
    except (ValueError, TypeError):
        return True


"""Workspace-relative path of the exit-code marker the v0.5.9 wrap writes.

Hard-coded rather than configurable: the marker is daemon-internal and
shares ``_vq/`` with events.jsonl / samples.jsonl, which are also under
fixed names."""


@dataclass
class _RunningJob:
    popen: subprocess.Popen[bytes]
    cpus: int
    mem_mb: int | None
    stdout_fh: IO[bytes]
    stderr_fh: IO[bytes]
    # STATE-3 (v0.8.14): monotonic deadline after which the daemon escalates
    # an external `vq kill` (spec terminal, process still alive) to SIGKILL;
    # None until the daemon first notices the terminal label. ``term_sigkilled``
    # flips once we've sent the SIGKILL so we don't re-fire / re-log every tick.
    term_deadline: float | None = None
    term_sigkilled: bool = False
    # Immutable admission identity. A later duplicate bare id must not redirect
    # reconciliation to another user's mutable queue record.
    owner_uid: str | None = None
    spec_path: Path | None = None

    def close_logs(self) -> None:
        for fh in (self.stdout_fh, self.stderr_fh):
            with contextlib.suppress(Exception):
                fh.close()


@dataclass
class _TerminalSurvivor:
    """A killed local job whose process group outlived the wrapper vq reaped.

    ``vq kill`` and the watchdog both SIGTERM the whole process group and
    write the terminal label, then owe the job a grace before SIGKILL. The
    group is led by the ``vq.resource_receipt`` wrapper, which installs no
    handlers and so dies to that SIGTERM at once -- while a command that
    ignores SIGTERM keeps running. ``_record_finish`` then reaps the wrapper
    and the job leaves ``_running`` -- which is the only place
    ``_escalate_if_killed`` looks for a deadline to arm, so the escalation it
    exists to perform could never fire, and the command was left running in
    neither ``_running`` nor ``_orphans``, its cpu/mem slot handed back to a
    host it still occupies.

    So the job keeps a record here from the wrapper's reap until its group is
    gone or its grace expires, and its capacity stays reserved for that long.
    This is the only state vq keeps for a job whose spec is already terminal
    and already reaped; it never revises that spec.
    """

    pgid: int
    cpus: int
    mem_mb: int | None
    state: JobState
    workspace: Path
    # Monotonic deadline for the SIGKILL, inherited from the ``_RunningJob``
    # when ``_escalate_if_killed`` had already armed one so a wrapper that
    # takes its time dying cannot restart the grace.
    deadline: float
    owner_uid: str | None = None


@dataclass
class _SchedulerJob:
    """A batch job dispatched to a scheduler host (Arch 2, design doc §17).

    The parallel of :class:`_RunningJob` for the v1.0 cluster backend. The job
    is NOT a local child: it runs on the cluster, observed via the
    :class:`~vq.scheduler_dispatch.SchedulerDispatcher` (``qstat``) and reaped
    from its exit-marker (staged home by ``fetch_results``). Kept in a separate
    ``_scheduler_running`` dict so the local ``Popen`` path — every reachable
    job today, since submit-routing is not yet wired — stays byte-for-byte
    unchanged. No ``pid``/``pgid``/file handles; the scheduler owns the process,
    so the ``/proc`` watchdog never touches these (telemetry-only, §10).
    """

    handle: SchedulerHandle
    dispatcher: SchedulerDispatcher
    cpus: int
    mem_mb: int | None
    # Armed once we notice an external `vq kill` (spec terminal, scheduler job
    # still listed) so we `qdel` exactly once rather than every reconcile tick.
    term_qdeled: bool = False
    # CLOCK_MONOTONIC of the last status write, so the live-detail refresh (§18)
    # rewrites the spec at most every SCHEDULER_STATUS_REFRESH_SECONDS rather
    # than every poll (the elapsed walltime changes continuously).
    last_status_write: float = 0.0
    # When qstat reports FINISHED before the exit-marker is visible, remember
    # the first miss and tolerate a short visibility grace before marking the
    # job ABORTED_BY_QUEUE.
    finished_without_marker_since: float | None = None
    finished_without_marker_misses: int = 0
    # Consecutive `fetch_results` failures after the job already left the
    # scheduler. Bounded, because parking on a fetch failure assumes a retry can
    # succeed -- and when the remote workspace is gone it never can. See
    # SCHEDULER_FETCH_FAILURE_LIMIT.
    fetch_failure_misses: int = 0
    # Kept for backwards-compatible in-memory shape. Best-effort scheduler
    # telemetry never gates terminal reconciliation.
    accounting_failure_misses: int = 0
    # Set after the scheduler affirmatively rejects this exact handle. The
    # daemon logs that attribution once while terminal marker/fetch
    # reconciliation proceeds; ambiguous poll failures never set it.
    explicit_absence_reported: bool = False
    # Immutable admission identity; see _RunningJob.owner_uid/spec_path.
    owner_uid: str | None = None
    spec_path: Path | None = None


@dataclass(frozen=True)
class _SchedulerPollObservation:
    attempted_at: str
    refresh_sequence: int
    phases: dict[str, SchedulerPhase]
    explicitly_absent_job_ids: frozenset[str]
    details: dict[str, QstatDetail]
    # Per-job "why is this still queued", for dialects that report it on the
    # coarse poll rather than on the detail record. Empty for Torque, whose
    # reason arrives on QstatDetail instead.
    queued_reasons: dict[str, str] = field(default_factory=dict)
    poll_error: (
        SchedulerError
        | DialectError
        | OSError
        | subprocess.SubprocessError
        | None
    ) = None
    accounting_error: (
        SchedulerError
        | DialectError
        | OSError
        | subprocess.SubprocessError
        | None
    ) = None


@dataclass
class _SchedulerPollFlight:
    """One read-only scheduler-host observation running off the main loop."""

    items: tuple[tuple[str, _SchedulerJob], ...]
    refresh_sequence: int
    done: threading.Event
    observation: _SchedulerPollObservation | None = None
    error: BaseException | None = None


_SCHEDULER_SUBMIT_BINDING_SCHEMA = "vq.scheduler-submit-binding/1"
_SCHEDULER_SUBMIT_BINDING_MAX_BYTES = MULTI_USER_SPEC_MAX_BYTES + 4096
_BINDING_JOB_ID_UNSET = object()


@dataclass(frozen=True)
class _SchedulerSubmitBinding:
    """Daemon-owned authority for a multi-user scheduler transaction."""

    owner_uid: str
    job_id: str
    scheduler_target: str
    cwd: str
    cpus: int
    admitted_spec: JobSpec
    scheduler_job_id: str | None = None
    transaction_state: str = "open"


def _scheduler_submit_binding_may_be_open(
    binding: _SchedulerSubmitBinding | None,
) -> bool:
    """Whether private authority still represents a possible allocation."""
    return binding is not None and binding.transaction_state == "open"


def _scheduler_submit_binding_directory() -> Path:
    """Return a private, daemon-owned binding directory."""
    paths.ensure_multi_user_root()
    directory = paths.multi_user_root() / "scheduler-submit-bindings"
    try:
        metadata = directory.lstat()
    except FileNotFoundError:
        with contextlib.suppress(FileExistsError):
            directory.mkdir(mode=0o700)
        metadata = directory.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise OSError("unsafe scheduler submit binding directory")
    return directory


def _scheduler_submit_binding_name(owner_uid: str, job_id: str) -> str:
    if not owner_uid.isascii() or not owner_uid.isdecimal():
        raise ValueError("scheduler submit binding owner must be a numeric uid")
    if str(int(owner_uid)) != owner_uid:
        raise ValueError("scheduler submit binding owner uid is not canonical")
    return f"{owner_uid}.{validate_job_id(job_id)}.json"


def _decode_scheduler_submit_binding(
    raw: bytes,
    *,
    owner_uid: str,
    job_id: str,
) -> _SchedulerSubmitBinding:
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeError, ValueError) as exc:
        raise ValueError("malformed scheduler submit binding") from exc
    if not isinstance(payload, dict) or set(payload) != {
        "schema",
        "owner_uid",
        "job_id",
        "scheduler_target",
        "cwd",
        "cpus",
        "admitted_spec",
        "scheduler_job_id",
        "transaction_state",
    }:
        raise ValueError("malformed scheduler submit binding")
    target = payload.get("scheduler_target")
    cwd = payload.get("cwd")
    cpus = payload.get("cpus")
    scheduler_job_id = payload.get("scheduler_job_id")
    transaction_state = payload.get("transaction_state")
    try:
        admitted_spec = JobSpec.model_validate(payload.get("admitted_spec"))
    except (TypeError, ValueError) as exc:
        raise ValueError("malformed scheduler submit binding") from exc
    if (
        payload.get("schema") != _SCHEDULER_SUBMIT_BINDING_SCHEMA
        or payload.get("owner_uid") != owner_uid
        or payload.get("job_id") != job_id
        or not isinstance(target, str)
        or not target
        or not isinstance(cwd, str)
        or not cwd
        or isinstance(cpus, bool)
        or not isinstance(cpus, int)
        or cpus < 1
        or admitted_spec.id != job_id
        or admitted_spec.cwd != cwd
        or admitted_spec.cpus != cpus
        or admitted_spec.scheduler_target != target
        or admitted_spec.submitter != owner_uid
        or transaction_state not in {"open", "closed"}
        or (
            scheduler_job_id is not None
            and not isinstance(scheduler_job_id, str)
        )
    ):
        raise ValueError("malformed scheduler submit binding")
    return _SchedulerSubmitBinding(
        owner_uid,
        job_id,
        target,
        cwd,
        cpus,
        admitted_spec,
        scheduler_job_id,
        transaction_state,
    )


def _read_scheduler_submit_binding(
    owner_uid: str,
    job_id: str,
) -> _SchedulerSubmitBinding | None:
    directory = _scheduler_submit_binding_directory()
    name = _scheduler_submit_binding_name(owner_uid, job_id)
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    file_flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    directory_fd = os.open(directory, directory_flags)
    file_fd: int | None = None
    try:
        try:
            file_fd = os.open(name, file_flags, dir_fd=directory_fd)
        except FileNotFoundError:
            return None
        metadata = os.fstat(file_fd)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_size > _SCHEDULER_SUBMIT_BINDING_MAX_BYTES
            or metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) & 0o077
        ):
            raise ValueError("unsafe scheduler submit binding file")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(
                file_fd,
                min(64 * 1024, _SCHEDULER_SUBMIT_BINDING_MAX_BYTES + 1 - total),
            )
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > _SCHEDULER_SUBMIT_BINDING_MAX_BYTES:
                raise ValueError("oversized scheduler submit binding")
        after = os.fstat(file_fd)
        if after.st_size != metadata.st_size or total != metadata.st_size:
            raise ValueError("scheduler submit binding changed while being read")
        raw = b"".join(chunks)
        return _decode_scheduler_submit_binding(
            raw,
            owner_uid=owner_uid,
            job_id=job_id,
        )
    finally:
        if file_fd is not None:
            os.close(file_fd)
        os.close(directory_fd)


def _ensure_scheduler_submit_binding(
    owner_uid: str,
    spec: JobSpec,
    scheduler_target: str,
) -> _SchedulerSubmitBinding:
    """Create-once the exact owner/job/target/workspace authority."""
    expected = _SchedulerSubmitBinding(
        owner_uid,
        spec.id,
        scheduler_target,
        spec.cwd,
        spec.cpus,
        spec.model_copy(deep=True),
    )
    existing = _read_scheduler_submit_binding(owner_uid, spec.id)
    if existing is not None:
        if (
            existing.owner_uid != expected.owner_uid
            or existing.job_id != expected.job_id
            or existing.scheduler_target != expected.scheduler_target
            or existing.cwd != expected.cwd
            or existing.cpus != expected.cpus
        ):
            raise ValueError("scheduler submit binding conflicts with this job")
        return existing
    directory = _scheduler_submit_binding_directory()
    name = _scheduler_submit_binding_name(owner_uid, spec.id)
    encoded = (
        json.dumps(
            {
                "schema": _SCHEDULER_SUBMIT_BINDING_SCHEMA,
                "owner_uid": owner_uid,
                "job_id": spec.id,
                "scheduler_target": scheduler_target,
                "cwd": spec.cwd,
                "cpus": spec.cpus,
                "admitted_spec": spec.model_dump(mode="json"),
                "scheduler_job_id": None,
                "transaction_state": "open",
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    file_flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
    directory_fd = os.open(directory, directory_flags)
    file_fd: int | None = None
    try:
        try:
            file_fd = os.open(name, file_flags, 0o600, dir_fd=directory_fd)
        except FileExistsError:
            existing = _read_scheduler_submit_binding(owner_uid, spec.id)
            if existing is None or (
                existing.owner_uid != expected.owner_uid
                or existing.job_id != expected.job_id
                or existing.scheduler_target != expected.scheduler_target
                or existing.cwd != expected.cwd
                or existing.cpus != expected.cpus
            ):
                raise ValueError(
                    "scheduler submit binding conflicts with this job"
                ) from None
            return existing
        with os.fdopen(file_fd, "wb") as stream:
            file_fd = None
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.fsync(directory_fd)
    finally:
        if file_fd is not None:
            os.close(file_fd)
        os.close(directory_fd)
    return expected


def _write_scheduler_submit_binding(binding: _SchedulerSubmitBinding) -> None:
    """Durably replace one daemon-owned binding with stricter evidence."""
    directory = _scheduler_submit_binding_directory()
    name = _scheduler_submit_binding_name(binding.owner_uid, binding.job_id)
    payload = {
        "schema": _SCHEDULER_SUBMIT_BINDING_SCHEMA,
        "owner_uid": binding.owner_uid,
        "job_id": binding.job_id,
        "scheduler_target": binding.scheduler_target,
        "cwd": binding.cwd,
        "cpus": binding.cpus,
        "admitted_spec": binding.admitted_spec.model_dump(mode="json"),
        "scheduler_job_id": binding.scheduler_job_id,
        "transaction_state": binding.transaction_state,
    }
    paths.atomic_write_text(
        directory / name,
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
    )
    os.chmod(directory / name, 0o600)


def _remove_scheduler_submit_binding(owner_uid: str, job_id: str) -> None:
    """Durably remove one exact closed multi-user scheduler transaction."""
    directory = _scheduler_submit_binding_directory()
    name = _scheduler_submit_binding_name(owner_uid, job_id)
    directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        try:
            os.unlink(name, dir_fd=directory_fd)
        except FileNotFoundError:
            return
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _replace_scheduler_submit_binding_spec(
    owner_uid: str,
    spec: JobSpec,
    *,
    scheduler_job_id: str | None | object = _BINDING_JOB_ID_UNSET,
    allow_clear_bound_id: bool = False,
    transaction_state: str | None = None,
) -> _SchedulerSubmitBinding:
    """Persist a trusted lifecycle snapshot without changing its authority."""
    binding = _read_scheduler_submit_binding(owner_uid, spec.id)
    if binding is None:
        raise ValueError("scheduler lifecycle has no submit binding")
    if (
        spec.scheduler_target != binding.scheduler_target
        or spec.cwd != binding.cwd
        or spec.cpus != binding.cpus
        or spec.submitter != owner_uid
    ):
        raise ValueError("scheduler lifecycle conflicts with its submit binding")
    bound_job_id = (
        binding.scheduler_job_id
        if scheduler_job_id is _BINDING_JOB_ID_UNSET
        else scheduler_job_id
    )
    if bound_job_id is not None and not isinstance(bound_job_id, str):
        raise ValueError("invalid bound scheduler job id")
    next_transaction_state = transaction_state or binding.transaction_state
    if next_transaction_state not in {"open", "closed"}:
        raise ValueError("invalid scheduler transaction state")
    if (
        binding.scheduler_job_id is not None
        and bound_job_id != binding.scheduler_job_id
        and not (allow_clear_bound_id and bound_job_id is None)
    ):
        raise ValueError("scheduler lifecycle conflicts with bound acceptance id")
    updated = _SchedulerSubmitBinding(
        binding.owner_uid,
        binding.job_id,
        binding.scheduler_target,
        binding.cwd,
        binding.cpus,
        spec.model_copy(deep=True),
        bound_job_id,
        next_transaction_state,
    )
    _write_scheduler_submit_binding(updated)
    return updated


def _iter_scheduler_submit_bindings() -> Iterator[_SchedulerSubmitBinding]:
    """Read every well-formed private scheduler transaction authority."""
    directory = _scheduler_submit_binding_directory()
    for entry in sorted(directory.iterdir()):
        name = entry.name
        if not name.endswith(".json"):
            continue
        owner_uid, separator, job_id = name[:-5].partition(".")
        if not separator:
            continue
        try:
            binding = _read_scheduler_submit_binding(owner_uid, job_id)
        except (OSError, ValueError):
            log.exception("failed to read scheduler submit binding %s", name)
            continue
        if binding is not None:
            yield binding


def _close_scheduler_submit_binding(owner_uid: str, spec: JobSpec) -> None:
    """Durably tombstone a proven-closed transaction before unlink cleanup."""
    _replace_scheduler_submit_binding_spec(
        owner_uid,
        spec,
        transaction_state="closed",
    )


def _bind_scheduler_job_id(
    owner_uid: str,
    job_id: str,
    scheduler_job_id: str,
) -> _SchedulerSubmitBinding:
    binding = _read_scheduler_submit_binding(owner_uid, job_id)
    if binding is None:
        raise ValueError("scheduler acceptance has no submit binding")
    if binding.scheduler_job_id not in {None, scheduler_job_id}:
        raise ValueError("scheduler acceptance conflicts with the bound scheduler id")
    if binding.scheduler_job_id == scheduler_job_id:
        return binding
    updated = _SchedulerSubmitBinding(
        binding.owner_uid,
        binding.job_id,
        binding.scheduler_target,
        binding.cwd,
        binding.cpus,
        binding.admitted_spec,
        scheduler_job_id,
        binding.transaction_state,
    )
    _write_scheduler_submit_binding(updated)
    return updated


class _SchedulerFetchOutcome(Enum):
    FETCHED = auto()
    RETRY = auto()
    UNAVAILABLE = auto()


@dataclass
class _SchedulerFetchFlight:
    """One bound artifact transfer; only its outcome crosses to the main loop."""

    job: _SchedulerJob
    workspace: Path
    done: threading.Event
    dispatcher: SchedulerDispatcher
    handle: SchedulerHandle
    telemetry_complete: bool = True
    error: BaseException | None = None


_SCHEDULER_FETCH_CONCURRENCY = 2


@dataclass
class _OrphanJob:
    """v0.6.x: a job still alive from a *previous* daemon, reattached
    by process-group id after a daemon restart. The daemon has no
    Popen handle for it, but it MUST still count against the dispatch
    budgets — so the resource footprint (cpus / mem_mb, read from the
    spec at reattach time) is carried here alongside the pgid.

    Before this carried cpus/mem, the dispatch gate counted only
    ``_running`` — a daemon restart with N live orphans would then
    dispatch a fresh ``max_jobs`` on top of them, over-subscribing
    the host by N jobs' worth of CPU + memory.

    ``uid`` (v0.6.x) is the job's submitter — the numeric-uid string
    in multi-user mode, ``None`` in single-user mode. The per-user
    quota counting needs it so a user's reattached orphans count
    against their ``[quotas]`` limits across a daemon restart."""

    pgid: int
    cpus: int
    mem_mb: int | None
    uid: str | None = None
    spec_path: Path | None = None
    # Armed when a kill is first observed after reattachment. The orphan
    # keeps its resource reservation until exit or SIGKILL escalation.
    term_deadline: float | None = None


def _pgroup_alive(pgid: int) -> bool:
    """Probe via killpg(pgid, 0). Signal 0 sends nothing; only the
    error tells us whether the group exists. Returns True if alive,
    False if gone or not signal-able by us."""
    return killpg(pgid, 0)


_VERSION_DRIFT_CHECK_INTERVAL_SECONDS = 60.0
"""v0.6.2: how often the daemon re-reads vq.__version__ from on-disk
source to detect drift. 60s is well below the cost of a useful
warning (operator notices in their next log scan) and well above the
cost of the file read (~microseconds)."""

_VERSION_RE = re.compile(r'__version__\s*=\s*["\']([^"\']+)["\']')


_UNSET = object()
"""Sentinel for "this cached dispatcher was not built from config"."""


def _config_fingerprint() -> str | None:
    """Cheap identity of the on-disk config, or None when it does not exist.

    ``load_config()`` reads exactly one file (``config.config_path()``).
    Hash its bytes so same-length rewrites inside one coarse filesystem clock
    tick cannot retain a stale scheduler dispatcher. That exact race appeared
    in container CI and can also happen on network filesystems. Config files
    are small, and this avoids parsing TOML on every dispatch pass.
    """
    path = config_path()
    try:
        payload = path.read_bytes()
    except OSError:
        return None
    return hashlib.blake2b(payload, digest_size=16).hexdigest()


def _read_vq_version_from_source() -> str | None:
    """v0.6.2: read ``vq/__init__.py`` from disk and parse out the
    ``__version__`` literal. Returns None on any failure (file gone,
    unreadable, no version line, no regex match). The daemon uses
    this for the periodic drift probe — failures are benign because
    the probe is best-effort."""
    try:
        from vq import __file__ as vq_file

        init_path = Path(vq_file)
        text = init_path.read_text(encoding="utf-8")
    except (OSError, ImportError):
        return None
    match = _VERSION_RE.search(text)
    return match.group(1) if match else None


def _read_pid_start_time(pid: int) -> int | None:
    """v0.5.50: read ``/proc/<pid>/stat`` field 22 (process start time
    in clock ticks since boot). Returns None on any failure (macOS, no
    /proc, process gone, malformed line). Used as the anti-recycling
    fingerprint: a PID recycled by the kernel to a different process
    after the daemon went down will have a different start time, so
    the startup recovery path can distinguish "our process is alive"
    from "PID points at someone else now".

    Format reminder (man 5 proc, /proc/[pid]/stat):
      pid (comm) state ppid pgrp session tty_nr tpgid flags minflt
      cminflt majflt cmajflt utime stime cutime cstime priority nice
      num_threads itrealvalue starttime ...
    starttime is field 22 (1-indexed). The catch: `comm` (field 2) is
    parenthesised and CAN CONTAIN SPACES AND PARENS. Splitting on
    whitespace breaks. Read raw, locate the trailing ')' of comm, then
    split the rest from there — field 22 of the full row is then index
    19 of the post-comm split (fields 3..22 → indices 0..19)."""
    try:
        with open(f"/proc/{pid}/stat", encoding="latin-1") as f:
            raw = f.read()
    except OSError:
        return None
    # comm ends at the last ')' since comm itself can contain ')'.
    rparen = raw.rfind(")")
    if rparen == -1:
        return None
    after = raw[rparen + 1 :].split()
    # Fields 3..N are after the comm; starttime is field 22 (1-indexed),
    # which is index 19 in the 0-indexed post-comm split.
    if len(after) < 20:
        return None
    try:
        return int(after[19])
    except ValueError:
        return None


def _pid_fingerprint_matches(spec: JobSpec) -> bool | None:
    """v0.5.50: True iff ``spec.pid_start_time`` matches the current
    ``/proc/<spec.pid>/stat`` field 22. None when we can't tell (no
    /proc on this host, spec from before v0.5.50 with no recorded
    start_time, process already gone). False means the PID was
    recycled to a different process — the kernel reused the slot.

    Callers should treat ``None`` as "no signal, fall back to pgid
    liveness" and ``False`` as "this is NOT our process, give up on
    the spec." ``True`` is the affirmative pass-through.
    """
    if spec.pid is None or spec.pid_start_time is None:
        return None
    current = _read_pid_start_time(spec.pid)
    if current is None:
        return None
    return current == spec.pid_start_time


def _exit_marker_path(workspace: Path, *, array_index: int | None = None) -> Path:
    marker = workspace / EXIT_MARKER_RELPATH
    if array_index is None:
        return marker
    return marker.with_name(f"{marker.name}.{array_index}")


def _read_exit_marker(workspace: Path, *, array_index: int | None = None) -> int | None:
    """Read the v0.5.9 exit-code marker the command wrapper writes, or None.

    The marker holds the inner command's shell-style integer rc (0..255 for
    normal exits, 128+sig for signaled exits). Missing / empty / unparseable ->
    None, signalling
    "fall back to ABORTED_BY_QUEUE; rc genuinely unrecoverable."
    Reads via ``read_text`` are best-effort.
    """
    marker = _exit_marker_path(workspace, array_index=array_index)
    try:
        text = marker.read_text().strip()
    except (OSError, FileNotFoundError):
        return None
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


_QSTAT_WALLTIME_USED_RE = re.compile(
    r"resources_used\.walltime\s*=\s*([0-9]+:[0-9]{2}:[0-9]{2})"
)
_QSTAT_WALLTIME_LIMIT_RE = re.compile(
    r"Resource_List\.walltime\s*=\s*([0-9]+:[0-9]{2}:[0-9]{2})"
)


def _scheduler_hms_to_seconds(value: str | None) -> int | None:
    if value is None:
        return None
    parts = value.split(":")
    if len(parts) != 3:
        return None
    try:
        hours, minutes, seconds = (int(part) for part in parts)
    except ValueError:
        return None
    if hours < 0 or not (0 <= minutes < 60) or not (0 <= seconds < 60):
        return None
    return hours * 3600 + minutes * 60 + seconds


def _scheduler_walltime_pair(
    spec: JobSpec,
    detail: QstatDetail | None,
    evidence: dict[str, object] | None = None,
) -> tuple[str | None, str | None]:
    used = detail.walltime_used if detail is not None else None
    limit = detail.walltime_limit if detail is not None else None
    if used is None:
        used = spec.scheduler_walltime_used
    if limit is None:
        limit = spec.scheduler_walltime_limit
    if evidence is not None and (used is None or limit is None):
        qstat_detail = evidence.get("qstat_detail")
        if isinstance(qstat_detail, str):
            if used is None:
                match = _QSTAT_WALLTIME_USED_RE.search(qstat_detail)
                if match is not None:
                    used = match.group(1)
            if limit is None:
                match = _QSTAT_WALLTIME_LIMIT_RE.search(qstat_detail)
                if match is not None:
                    limit = match.group(1)
    return used, limit


def _scheduler_walltime_exceeded(
    spec: JobSpec,
    detail: QstatDetail | None,
    evidence: dict[str, object] | None = None,
) -> tuple[bool, str | None, str | None]:
    used, limit = _scheduler_walltime_pair(spec, detail, evidence)
    used_seconds = _scheduler_hms_to_seconds(used)
    limit_seconds = _scheduler_hms_to_seconds(limit)
    if used_seconds is None or limit_seconds is None or limit_seconds <= 0:
        return False, used, limit
    return used_seconds >= limit_seconds, used, limit


def _scheduler_walltime_reason(
    *,
    used: str | None,
    limit: str | None,
    missing_marker: bool,
) -> str:
    marker_note = (
        "; exit-marker missing after final workspace fetch"
        if missing_marker
        else "; preserving recovered exit-marker"
    )
    if used is not None and limit is not None:
        return f"scheduler walltime limit reached ({used} >= {limit}){marker_note}"
    return f"scheduler walltime limit reached{marker_note}"


# #414: durable state for a scheduler-attributed abnormal termination, keyed
# by the dialect's normalized accounting state. Scheduler kills reuse the
# watchdog terminal vocabulary (the retry-with-more-memory semantics of
# OOM_KILLED apply regardless of which supervisor observed the kill);
# everything else the scheduler ended or invalidated (CANCELLED, NODE_FAIL,
# BOOT_FAIL, PREEMPTED, ...) is "the queue ended this", ABORTED_BY_QUEUE.
_SCHEDULER_ABNORMAL_END_STATES: dict[str, JobState] = {
    "OUT_OF_MEMORY": JobState.OOM_KILLED,
    "TIMEOUT": JobState.TIME_EXCEEDED,
    "DEADLINE": JobState.TIME_EXCEEDED,
}


def _scheduler_abnormal_end_reason(
    *,
    abnormal_state: str,
    raw_state: str,
    marker_rc: int | None,
) -> str:
    state_note = (
        f" (accounting state {raw_state!r})" if raw_state != abnormal_state else ""
    )
    marker_note = (
        "exit-marker missing"
        if marker_rc is None
        else f"exit-marker rc={marker_rc} recorded, but a marker cannot certify "
        "a run the scheduler terminated"
    )
    return (
        f"scheduler accounting reports {abnormal_state}{state_note}; {marker_note}"
    )


_FAILURE_TAIL_MAX_LINES = 20
_FAILURE_TAIL_MAX_BYTES = 4000
_MISSING_MARKER_LOCAL_LIST_LIMIT = 200
_MISSING_MARKER_LOCAL_TAIL_BYTES = 4096


def _read_stderr_tail(workspace: Path, stderr_relpath: str) -> str | None:
    """Tail of a job's stderr.log, for crash feedback in ``vq status``.

    Returns up to the last ``_FAILURE_TAIL_MAX_LINES`` lines of
    ``workspace / stderr_relpath`` (reading only the trailing
    ``_FAILURE_TAIL_MAX_BYTES`` so a multi-GB log costs nothing),
    lossy-decoded and stripped, or None when the log is empty / missing /
    unreadable. Best-effort by contract: ANY error returns None, because
    capturing crash feedback must never break the daemon's terminal
    recording (the spec lock is held when this runs).
    """
    try:
        path = workspace / stderr_relpath
        with path.open("rb") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            fh.seek(max(0, size - _FAILURE_TAIL_MAX_BYTES))
            data = fh.read()
    except (OSError, ValueError):
        return None
    if not data:
        return None
    lines = data.decode("utf-8", "replace").splitlines()[-_FAILURE_TAIL_MAX_LINES:]
    return "\n".join(lines).strip() or None


def _local_missing_marker_diagnostics(
    workspace: Path, spec: JobSpec, *, array_index: int | None = None
) -> dict[str, object]:
    """Local evidence after a final scheduler fetch still lacks a marker."""
    evidence: dict[str, object] = {
        "local_workspace": str(workspace),
        "local_exit_marker": str(_exit_marker_path(workspace, array_index=array_index)),
        "local_workspace_listing": _format_local_listing(workspace),
        "local_vq_listing": _format_local_listing(workspace / "_vq"),
        "local_file_sample": _format_local_file_sample(workspace),
    }
    stdout_tail = _read_local_tail(
        _array_log_path(workspace, spec.stdout_path, array_index=array_index)
    )
    if stdout_tail:
        evidence["local_stdout_tail"] = stdout_tail
    stderr_tail = _read_local_tail(
        _array_log_path(workspace, spec.stderr_path, array_index=array_index)
    )
    if stderr_tail:
        evidence["local_stderr_tail"] = stderr_tail
    return evidence


def _array_log_path(
    workspace: Path, relpath: str, *, array_index: int | None
) -> Path:
    path = workspace / relpath
    if array_index is None:
        return path
    return path.with_name(f"{path.name}.{array_index}")


def _format_local_listing(path: Path) -> str:
    try:
        entries = sorted(path.iterdir(), key=lambda p: p.name)
    except FileNotFoundError:
        return "(missing)"
    except OSError as exc:
        return f"(unreadable: {exc})"
    lines: list[str] = []
    for entry in entries[:_MISSING_MARKER_LOCAL_LIST_LIMIT]:
        try:
            st = entry.stat()
            kind = "d" if entry.is_dir() else "f" if entry.is_file() else "?"
            lines.append(f"{kind} {st.st_size:>12} {entry.name}")
        except OSError as exc:
            lines.append(f"? {'-':>12} {entry.name} ({exc})")
    if len(entries) > _MISSING_MARKER_LOCAL_LIST_LIMIT:
        lines.append(
            f"...<truncated after {_MISSING_MARKER_LOCAL_LIST_LIMIT} entries>..."
        )
    return "\n".join(lines) or "(empty)"


def _format_local_file_sample(workspace: Path) -> str:
    if not workspace.exists():
        return "(missing)"
    lines: list[str] = []
    try:
        for path in workspace.rglob("*"):
            if not path.is_file():
                continue
            try:
                lines.append(str(path.relative_to(workspace)))
            except ValueError:
                lines.append(str(path))
            if len(lines) >= _MISSING_MARKER_LOCAL_LIST_LIMIT:
                lines.append(
                    f"...<truncated after {_MISSING_MARKER_LOCAL_LIST_LIMIT} files>..."
                )
                break
    except OSError as exc:
        return f"(unreadable: {exc})"
    return "\n".join(lines) or "(empty)"


def _read_local_tail(path: Path) -> str | None:
    try:
        with path.open("rb") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            fh.seek(max(0, size - _MISSING_MARKER_LOCAL_TAIL_BYTES))
            data = fh.read()
    except (OSError, ValueError):
        return None
    if not data:
        return None
    text = data.decode("utf-8", "replace")
    if size > _MISSING_MARKER_LOCAL_TAIL_BYTES:
        text = (
            f"...<truncated to last {_MISSING_MARKER_LOCAL_TAIL_BYTES} bytes>...\n"
            f"{text}"
        )
    return text


def _build_wrapped_command(
    inner: list[str],
    exit_marker: Path,
    *,
    cgroup_scope_name: str | None = None,
) -> list[str]:
    """Wrap ``inner`` with direct terminal accounting and the exit marker.

    The collector launches the original argv exactly once, publishes
    ``resource-usage.json`` before the existing marker, and exits with the same
    shell-style return code. If the whole process group is killed, neither
    artifact is promised, preserving the orphan reconciler's fail-closed path.
    """
    receipt_path = exit_marker.with_name(resource_receipt.RESOURCE_USAGE_BASENAME)
    wrapped = [
        sys.executable,
        "-m",
        "vq.resource_receipt",
        "--receipt",
        str(receipt_path),
        "--exit-marker",
        str(exit_marker),
    ]
    if cgroup_scope_name is not None:
        wrapped.extend(["--cgroup-scope-name", cgroup_scope_name])
    return [*wrapped, "--", *inner]


def _gid_for_uid(uid: int) -> int | None:
    """v0.6.x: resolve a uid's primary gid for the multi-user
    privilege-drop. Returns None if the uid has no passwd entry —
    the caller treats that as "cannot drop safely" and fails the job
    rather than guessing a gid."""
    try:
        import pwd

        return pwd.getpwuid(uid).pw_gid
    except (KeyError, OSError):
        return None


def _chown_tree(root: Path, uid: int, gid: int) -> None:
    """v0.6.x: recursively chown ``root`` and everything under it to
    uid/gid. Used in multi-user mode so a job — which runs as its
    submitter, not as the root daemon — can write into its own
    workspace (exit-code marker, stdout/stderr logs, output files).

    Best-effort per entry: a chown failure on one path is logged and
    skipped rather than aborting the dispatch, since a partially
    chowned tree still lets the job run (the job's own writes create
    user-owned files); the daemon-created files are the ones that
    matter and they are chowned first."""
    try:
        os.chown(root, uid, gid)
    except OSError as e:  # pragma: no cover - defensive
        log.warning("chown %s -> %s:%s failed: %s", root, uid, gid, e)
    for dirpath, dirnames, filenames in os.walk(root):
        for name in dirnames + filenames:
            p = os.path.join(dirpath, name)
            try:
                os.chown(p, uid, gid, follow_symlinks=False)
            except OSError as e:  # pragma: no cover - defensive
                log.warning("chown %s -> %s:%s failed: %s", p, uid, gid, e)


def _host_total_mem_mb() -> int | None:
    """Best-effort read of host total memory in MB.

    Linux: parse /proc/meminfo. macOS (dev): None -- the daemon there
    runs without a memory-budget gate (matches "Linux first, macOS dev
    only" non-negotiable in SPEC.md sec 1.2).
    """
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) // 1024  # kB -> MB
    except (OSError, ValueError):
        return None
    return None


def _apply_default_thread_caps(env: dict[str, str], cpus: int) -> None:
    """Default unmanaged scientific thread pools to the job's CPU claim.

    vibe-qc parallelism is OpenMP-led. A pthreaded OpenBLAS using ``cpus``
    threads inside those OpenMP regions can trigger OpenBLAS' "Detect OpenMP
    Loop" hang warning, so the safe queue default is OpenMP=N, OpenBLAS=1.
    Operator-provided environment values still win via ``setdefault``.
    """
    threads = str(cpus)
    for name in _THREAD_CAP_ENV_VARS:
        default = "1" if name == "OPENBLAS_NUM_THREADS" else threads
        env.setdefault(name, default)


def _runtime_pin_snapshot_mismatches(
    prog: VenvProgram,
    pin: ProgramRuntimePin,
    *,
    actual_git_sha: str | None | object = _UNREAD_RUNTIME_SHA,
) -> list[str]:
    """Return live runtime mismatches against the spec's submitted pins."""
    mismatches: list[str] = []
    # An unenforced SHA is a record of what the runtime was at submit time, not
    # a requirement -- the job is meant to run on whatever is current. Skipping
    # the comparison is the whole point; the value stays on the spec for
    # provenance. See ProgramRuntimePin.enforce_git_sha.
    if pin.expected_git_sha and pin.enforce_git_sha:
        actual_sha = (
            prog.current_git_sha(full=len(pin.expected_git_sha) == 40)
            if actual_git_sha is _UNREAD_RUNTIME_SHA
            else actual_git_sha
        )
        if actual_sha is None:
            mismatches.append(
                f"git_sha expected {pin.expected_git_sha}, "
                "but current git SHA could not be read"
            )
        elif not _git_sha_matches(actual_sha, pin.expected_git_sha):
            mismatches.append(
                f"git_sha expected {pin.expected_git_sha}, got {actual_sha}"
            )
    if pin.expected_import_version:
        module = pin.import_check or prog.import_check
        if not module:
            mismatches.append(
                f"import version expected {pin.expected_import_version}, "
                "but no import_check is configured"
            )
        else:
            rc, _output, actual_version = run_import_identity_probe(
                prog.python,
                module,
                symbols=pin.import_symbols or prog.import_symbols,
            )
            label = f"import {module} version"
            if rc != 0 or actual_version is None:
                mismatches.append(
                    f"{label} expected {pin.expected_import_version}, "
                    "but current import version could not be read"
                )
            elif actual_version != pin.expected_import_version:
                mismatches.append(
                    f"{label} expected {pin.expected_import_version}, "
                    f"got {actual_version}"
                )
    return mismatches


@dataclass(frozen=True)
class _ProgramRuntimeResolution:
    failure: str | None = None
    resolved_git_sha: str | None = None


def _program_runtime_pin_dispatch_resolution(
    spec: JobSpec,
) -> _ProgramRuntimeResolution:
    """Resolve a dispatch-blocking mismatch and the runtime SHA actually used.

    ``vq submit --program NAME`` already validates configured venv runtime pins
    before queueing. For new specs, ``program_runtime_pin`` snapshots those
    configured pins at submit time; dispatch compares the live runtime to the
    snapshot so a later ``vq admin update`` cannot move both the checkout and
    config pin underneath a pending job. Old specs without a snapshot keep the
    pre-existing live-config recheck.
    """
    if spec.program is None:
        return _ProgramRuntimeResolution()
    pin = spec.program_runtime_pin
    if (
        spec.scheduler_target is not None
        and pin is not None
        and pin.scheduler_host is None
    ):
        return _ProgramRuntimeResolution(
            failure=(
                f"program {spec.program!r} carries only a driver-local runtime "
                f"observation for scheduler target {spec.scheduler_target!r}; "
                "refusing a scheduler result with unenforceable provenance"
            )
        )
    if (
        spec.scheduler_target is not None
        and pin is not None
        and pin.scheduler_host is not None
    ):
        if pin.scheduler_host != spec.scheduler_target:
            return _ProgramRuntimeResolution(
                failure=(
                    f"program {spec.program!r} runtime pin belongs to scheduler "
                    f"host {pin.scheduler_host!r}, but the job targets "
                    f"{spec.scheduler_target!r}"
                )
            )
        # BUG 101 scheduler pins identify the immutable target-side wrapper
        # selected at submit time.  The driver-local [programs] checkout is
        # neither executed nor an authority for that identity.
        if pin.expected_git_sha is None:
            return _ProgramRuntimeResolution(
                failure=(
                    f"program {spec.program!r} scheduler runtime pin has no "
                    "resolved git SHA for provenance"
                )
            )
        return _ProgramRuntimeResolution(
            resolved_git_sha=pin.expected_git_sha
        )
    try:
        cfg = load_config()
    except ConfigError as exc:
        return _ProgramRuntimeResolution(
            failure=(
                f"program {spec.program!r} runtime pin check could not read "
                f"config: {exc}"
            )
        )
    prog = cfg.programs.get(spec.program)
    if spec.program_runtime_pin is not None and not isinstance(prog, VenvProgram):
        return _ProgramRuntimeResolution(
            failure=(
                f"program {spec.program!r} runtime pin check could not resolve "
                "a venv program in the current config"
            )
        )
    if not isinstance(prog, VenvProgram):
        return _ProgramRuntimeResolution()
    if spec.program_runtime_pin is not None:
        if (
            pin is not None
            and pin.expected_git_sha is None
            and pin.expected_import_version is None
        ):
            # Legacy synthetic/empty snapshots carried no provenance claim.
            return _ProgramRuntimeResolution()
        actual_sha = prog.current_git_sha(full=True)
        mismatches = _runtime_pin_snapshot_mismatches(
            prog,
            spec.program_runtime_pin,
            actual_git_sha=actual_sha,
        )
    else:
        actual_sha = None
        mismatches = prog.runtime_pin_mismatches(include_import=True)
    if mismatches:
        return _ProgramRuntimeResolution(
            failure=(
                f"program {spec.program!r} runtime pin mismatch before dispatch: "
                + "; ".join(mismatches)
            )
        )
    if pin is None:
        # Pre-snapshot specs retain their legacy validation behavior. They do
        # not gain a provenance claim that was never part of their contract.
        return _ProgramRuntimeResolution()
    if actual_sha is None:
        return _ProgramRuntimeResolution(
            failure=(
                f"program {spec.program!r} resolved git SHA could not be read "
                "before dispatch; refusing an unattributed result"
            )
        )
    return _ProgramRuntimeResolution(resolved_git_sha=actual_sha)


def _program_runtime_pin_dispatch_failure(spec: JobSpec) -> str | None:
    """Compatibility projection of the dispatch runtime resolution."""
    return _program_runtime_pin_dispatch_resolution(spec).failure


def _runtime_pin_without_resolution(
    pin: ProgramRuntimePin | None,
) -> ProgramRuntimePin | None:
    if pin is None:
        return None
    return pin.model_copy(update={"resolved_git_sha": None}, deep=True)


class Daemon:
    def __init__(
        self,
        *,
        max_cpus: int | None = None,
        max_jobs: int | None = None,
        max_scheduler_jobs: int | None = None,
        max_mem_mb: int | None = None,
        default_job_mem_mb: int | None = None,
        poll_interval: float = 1.0,
        watchdog: Watchdog | None = None,
        queue_dir: Path | None = None,
        jobs_dir: Path | None = None,
        notify_webhook_url: str | None = None,
        notify_on_states: list[str] | None = None,
        multi_user: bool = False,
        loop_hook: Callable[[], None] | None = None,
    ) -> None:
        if max_cpus is None:
            max_cpus = os.cpu_count() or 1
        if max_cpus < 1:
            raise ValueError(f"max_cpus must be >= 1 (got {max_cpus})")
        if max_jobs is not None and max_jobs < 1:
            raise ValueError(f"max_jobs must be >= 1 or None (got {max_jobs})")
        if max_scheduler_jobs is not None and max_scheduler_jobs < 1:
            raise ValueError(
                "max_scheduler_jobs must be >= 1 or None "
                f"(got {max_scheduler_jobs})"
            )
        # max_mem_mb=None means "no memory-budget gate" (host total
        # not detectable, or admin chose to disable). Detection runs
        # only when the caller did not supply an explicit value.
        if max_mem_mb is None:
            max_mem_mb = _host_total_mem_mb()
        if max_mem_mb is not None and max_mem_mb < 1:
            raise ValueError(f"max_mem_mb must be >= 1 or None (got {max_mem_mb})")
        if not math.isfinite(poll_interval) or poll_interval <= 0:
            raise ValueError(
                "poll_interval must be finite and > 0 "
                f"(got {poll_interval})"
            )
        self.max_cpus = max_cpus
        self.max_jobs = max_jobs
        self.max_scheduler_jobs = max_scheduler_jobs
        self.max_mem_mb = max_mem_mb
        # Hardening (2026-06-21 AICCM build-storm): the assumed memory
        # footprint of a job that does NOT declare mem_mb. When set, an
        # undeclared job is charged this much RAM for BOTH the admission
        # budget gate and its cgroup MemoryMax cap, so a job that declares
        # nothing can no longer be dispatched unbounded or drive the host
        # into swap. None preserves the old v0.3 leniency (undeclared ==
        # unbounded). A job that genuinely needs more must declare
        # --mem-mb, which then packs correctly against the budget.
        if default_job_mem_mb is not None and default_job_mem_mb < 1:
            raise ValueError(
                f"default_job_mem_mb must be >= 1 or None (got {default_job_mem_mb})"
            )
        self.default_job_mem_mb = default_job_mem_mb
        self.poll_interval = poll_interval
        self._loop_hook = loop_hook
        # v0.6.x: multi-user mode. When true, the daemon iterates over
        # per-user state dirs under /var/lib/vq/users/<uid>/ instead of
        # a single queue_dir. Tracking maps (jobid→uid) are populated
        # during spec iteration and used by dispatch/kill/fetch paths
        # for ownership resolution.
        self._multi_user = multi_user
        # v0.6.x: multi-user dispatch drops privileges to each job's
        # submitter via `systemd-run --uid/--gid`. Without systemd-run
        # there is no safe way to run a job as anyone but root — fail
        # fast at construction (before the queue lock is claimed)
        # rather than silently running every job as root.
        if self._multi_user and not cgroup.systemd_run_on_path():
            raise RuntimeError(
                "multi-user mode requires systemd-run on PATH: jobs are "
                "dropped to their submitter's uid via `systemd-run "
                "--uid`. systemd-run was not found. Refusing to start — "
                "a multi-user daemon without privilege-drop would run "
                "every job as root."
            )
        # jobid → uid (string). Populated by _iter_specs in multi-user
        # mode; empty in single-user mode.
        self._job_uid: dict[str, str] = {}
        # jobid -> exact queue path observed during the same admission scan.
        # Never rebuild a root-daemon write target from a user-controlled inner
        # id; active runtime records retain this path after the next scan.
        self._job_spec_paths: dict[str, Path] = {}
        # cgroup detection is at-init: probe systemd-run --user once and
        # remember. If available, the kernel handles memory + CPU
        # enforcement; the watchdog runs in telemetry-only mode for memory.
        # Wall-time stays in the watchdog regardless (v0.5.8: systemd's
        # RuntimeMaxSec is not pause-aware and not runtime-mutable, so
        # cgroup wall-time enforcement was dropped). See
        # docs/wall_time_design.md.
        # v0.5.50: clear the lru_cache before probing so a daemon
        # restart re-tests cgroup availability from scratch. The
        # previous daemon's cached True is stale if user-systemd lost
        # its delegated controllers between restarts (e.g. user-systemd
        # died and was respawned without the Delegate=... drop-in, or
        # the OOM cascade we saw on 2026-05-17 perturbed the cgroup
        # hierarchy). Audit § 2j: cached True → systemd-run dispatch
        # fails at every _start_job → infinite re-dispatch loop.
        cgroup.reset_availability_cache()
        self.cgroup_enabled = cgroup.available()
        # Watchdog uses the same host_total_mem_mb the dispatcher uses for
        # its budget by default, so the per-job mem cap and the host-percent
        # ceiling agree on what "the host" is.
        self.watchdog = (
            watchdog
            if watchdog is not None
            else Watchdog(
                host_total_mem_mb=max_mem_mb,
                enforce_memory=not self.cgroup_enabled,
                enforce_wall_time=True,
            )
        )
        self.queue_dir = queue_dir or paths.queue_dir()
        self.jobs_dir = jobs_dir or paths.jobs_dir()
        # In multi-user mode, the lock lives at the system root.
        if self._multi_user:
            # Validate the real, daemon-owned final state-root component before
            # opening the root-daemon lock beneath it. ``run()`` later validates
            # the narrower ``users/`` structural boundary before queue scans.
            lock_dir = paths.ensure_multi_user_root()
        else:
            lock_dir = self.queue_dir
            lock_dir.mkdir(parents=True, exist_ok=True)
        self._queue_lock_path = lock_dir / ".vq-daemon.lock"
        # Lifetime-held: the flock below holds the queue lock for the daemon's
        # whole run, and a context manager would close the fd and release it.
        self._queue_lock_fd = open(self._queue_lock_path, "w")  # noqa: SIM115
        try:
            fcntl.flock(self._queue_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self._queue_lock_fd.close()
            raise RuntimeError(
                f"Another vq daemon is already using queue directory "
                f"{lock_dir}. Only one daemon per $VQ_STATE_DIR is "
                f"supported. Stop the other daemon before starting this one."
            ) from None
        self._running: dict[str, _RunningJob] = {}
        # v0.4: jobs that were RUNNING when the previous daemon exited but
        # whose process group is still alive (parented to init now). We
        # track them by pgid so we can poll liveness via killpg(pgid, 0)
        # and the watchdog can keep sampling /proc/<pid>.
        self._orphans: dict[str, _OrphanJob] = {}  # jobid -> _OrphanJob
        # Jobs whose spec is terminal and whose wrapper has been reaped, but
        # whose process group is still alive and still owes the SIGTERM its
        # killer sent a grace. See _TerminalSurvivor.
        self._terminal_survivors: dict[str, _TerminalSurvivor] = {}
        # The local job-execution mechanism (SPEC §8.1). LocalDispatcher owns
        # the in-memory Popen lifecycle; scheduler-target jobs take the
        # separate SchedulerDispatcher path below. See vq/dispatch.py and
        # vq/scheduler_dispatch.py.
        self.dispatcher: dispatch.Dispatcher[subprocess.Popen[bytes]] = (
            dispatch.LocalDispatcher()
        )
        # v1.0 cluster backend (design doc §17): jobs whose spec carries a
        # scheduler_target are dispatched to a cluster over SSH+qsub and tracked
        # here, separate from the local _running dict so the Popen path is
        # untouched. _scheduler_dispatchers caches one SchedulerDispatcher per
        # target host, built lazily from this daemon's config on first dispatch.
        self._scheduler_running: dict[str, _SchedulerJob] = {}
        self._scheduler_dispatchers: dict[str, SchedulerDispatcher] = {}
        self._scheduler_reattach_retry_last: dict[str, float] = {}
        # Active scheduler rows hidden from normal iteration by the multi-user
        # duplicate-id quarantine.  They are never dispatched or reconciled
        # while ambiguous, but each may still name live remote work and must
        # therefore continue to reserve scheduler capacity and owner quota.
        # The owner-qualified path keeps two colliding rows distinct without
        # reintroducing a bare-id lookup.
        self._colliding_scheduler_reservations: list[
            tuple[str, Path, JobSpec]
        ] = []
        # Specs whose dispatch-slot reservation we have already released, so
        # the explanation is logged once rather than every tick.
        self._untracked_reservation_released: set[str] = set()
        self._stop = False
        # Read-only scheduler status refresh requests arrive on the RPC
        # accept thread, while scheduler state remains owned by the daemon's
        # main loop.  A sequence number plus condition makes the handoff
        # explicit: a request is complete only after a reconciliation pass
        # that began after that request was published.
        self._scheduler_refresh_condition = threading.Condition()
        self._scheduler_refresh_wakeup = threading.Event()
        self._scheduler_refresh_requested = 0
        self._scheduler_refresh_requests: dict[int, str] = {}
        self._scheduler_refresh_results: dict[int, dict[str, object]] = {}
        # Unit-level reconciliation stays synchronous. ``run()`` flips this
        # before entering the production loop so slow SSH/scheduler observation
        # cannot hold local dispatch or another scheduler host hostage.
        self._background_scheduler_polling = False
        self._scheduler_poll_flights: dict[
            SchedulerDispatcher, _SchedulerPollFlight
        ] = {}
        self._scheduler_fetch_flights: dict[str, _SchedulerFetchFlight] = {}
        # Main-loop service order: actual admissions move a dispatcher behind
        # waiting groups. New arrivals cannot continually overtake a waiter.
        self._scheduler_fetch_order: dict[SchedulerDispatcher, None] = {}
        self._scheduler_fetch_admissions_left: int | None = None
        # v0.5.35: webhook URL for terminal-state notifications. None
        # (default) = notifications disabled; daemon-side callers
        # check this and fire ``notify.send_terminal_notification``
        # from the terminal paths that currently own notification.
        self.notify_webhook_url = notify_webhook_url
        # v0.7.17 *Postel's Robustness*: optional state filter so a
        # busy queue doesn't drown the operator's webhook channel
        # in every COMPLETED ping. Empty list = no filter (pre-
        # v0.7.17 behaviour, no filtering at notification call sites). Non-empty
        # = fire only when spec.state.value matches.
        self.notify_on_states: list[str] = list(notify_on_states or [])
        # v0.5.45: track whether the admin-update-in-progress marker
        # was present on the previous _dispatch_pending tick. Used to
        # log once per transition (marker appears / clears) rather
        # than every tick. Initial False means the FIRST tick logs
        # the present case — desirable when the daemon starts up
        # after a crash that left a marker.
        self._admin_update_marker_present: bool = False
        self._admin_update_hold_scope: frozenset[str] | None = None
        self._logged_stale_managed_marker_ids: set[str] = set()
        """Dispatch targets held by a live admin-update marker, per
        :func:`admin.admin_update_marker_scope`. ``None`` = hold everything
        (unreadable or unrecognised marker). Only meaningful while
        :attr:`_admin_update_marker_present` is True."""
        # v0.11.0: --refresh drain-tracking. Holds the jobid whose
        # ``refresh_before`` rebuild we are currently draining the host
        # for, so ``_maybe_refresh_before_run`` logs the "now draining"
        # transition exactly once instead of every poll tick for the
        # whole drain window. None when no refresh-drain is in flight.
        self._refresh_draining_for: str | None = None
        # v0.6.2: state for the periodic version-drift probe.
        # `_last_version_drift_check_monotonic` rate-limits the
        # check to once per _VERSION_DRIFT_CHECK_INTERVAL_SECONDS;
        # `_version_drift_seen_at_version` records the on-disk
        # version we last warned about so we don't spam the same
        # warning every interval (logs once per distinct drift
        # state — drift appears / drift changes / drift clears).
        self._last_version_drift_check_monotonic: float = 0.0
        self._version_drift_seen_at_version: str | None = None
        # v0.12.1: config staleness. `_scheduler_dispatchers` above snapshots
        # `scheduler_program_hooks` (and dialect / scratch / directives) at
        # build time and used to live for the daemon's whole life, so a config
        # fix landing on disk had NO effect until a full restart — `vq daemon
        # start` is removed and there was no SIGHUP path. On 2026-07-22 a
        # command_wrapper fix landed at ~17:50 and the daemon kept rendering
        # double-wrapped job scripts for every dispatch 17:57-18:26, chewing
        # through the released backlog with config nobody was running anymore.
        #
        # Fix: fingerprint the config file, and rebuild a cached dispatcher
        # whenever the fingerprint has moved since that dispatcher was built.
        # Cheap (one stat per dispatch pass) and needs no operator action. The
        # explicit reload path (SIGHUP / `vq daemon reload`) additionally
        # refreshes daemon-level config-derived state.
        self._config_fingerprint: str | None = _config_fingerprint()
        self._scheduler_dispatcher_fingerprints: dict[str, str | None] = {}
        self._config_reload_requested = False
        self._config_error_logged: str | None = None

    def _provision_admin_user_dirs(self) -> None:
        """Provision admins and harden every existing per-user state tree.

        Closes the submit-side bootstrap gap: ``/var/lib/vq/users/``
        is root-owned, so an unprivileged user cannot create their
        own ``<uid>/`` subtree — their first ``vq submit`` would
        fail with PermissionError. The daemon runs as root, so it
        can provision those dirs. New non-admin users still need the one-off
        provisioning step. Existing numeric trees are also passed through the
        same routine on every startup so deployments created before the
        root-owned structural-parent invariant migrate in place.

        An unsafe or unrepairable existing tree is fatal: continuing would let
        the root daemon scan paths whose structure it could not establish.
        Failure to create a brand-new admin tree remains isolated to that uid."""
        from vq import ownership

        try:
            cfg = load_config()
        except Exception as e:  # pragma: no cover - defensive
            log.warning("provision: config load failed: %s", e)
            return
        admin_uids = set(ownership.admin_group_uids(cfg))
        existing_uids = {int(path.name) for path in paths._all_user_dirs()}
        uids = sorted(admin_uids | existing_uids)
        provisioned = 0
        for uid in uids:
            gid = _gid_for_uid(uid)
            if gid is None:
                if uid in existing_uids:
                    raise paths.UnsafeMultiUserStateError(
                        f"cannot establish multi-user state ownership for uid {uid}: "
                        "no passwd entry or primary gid"
                    )
                continue
            try:
                paths.provision_user_state(uid, gid)
                provisioned += 1
            except paths.UnsafeMultiUserStateError:
                raise
            except OSError as e:
                if uid in existing_uids:
                    raise paths.UnsafeMultiUserStateError(
                        f"cannot harden existing multi-user state for uid {uid}: {e}"
                    ) from e
                log.warning(
                    "provision: user state dir for uid %s failed: %s", uid, e
                )
        if uids:
            log.info(
                "provisioned or hardened %d/%d multi-user state dir(s)",
                provisioned,
                len(uids),
            )

    def run(self) -> None:
        log.info(
            "daemon starting (max_cpus=%s, max_jobs=%s, max_scheduler_jobs=%s, "
            "max_mem_mb=%s, cgroup=%s, multi_user=%s, queue=%s)",
            self.max_cpus,
            "unlimited" if self.max_jobs is None else self.max_jobs,
            (
                "unlimited"
                if self.max_scheduler_jobs is None
                else self.max_scheduler_jobs
            ),
            "unset" if self.max_mem_mb is None else self.max_mem_mb,
            "enforced" if self.cgroup_enabled else "disabled",
            "enabled" if self._multi_user else "disabled",
            paths.multi_user_root() if self._multi_user else self.queue_dir,
        )
        if self._multi_user:
            paths.ensure_users_root()
            self._provision_admin_user_dirs()
        else:
            self.queue_dir.mkdir(parents=True, exist_ok=True)
            self.jobs_dir.mkdir(parents=True, exist_ok=True)
        # Publish one immutable daemon-start budget. RPC is authoritative while
        # this process is healthy; the mode-aware file is its daemon-down and
        # mixed-version fallback.
        advertised_capacity = capacity.write_daemon_capacity(
            self.max_cpus,
            self.max_jobs,
            self.max_mem_mb,
            self.max_scheduler_jobs,
            self.default_job_mem_mb,
            multi_user=self._multi_user,
        )
        # #53: install the stop handlers BEFORE the startup walk, not after
        # it. The walk reads every queued spec, which on a driver-sized queue
        # takes minutes -- a reported 21,683-spec queue needed more than 242 s
        # cold. Until v0.26.8 the handlers went on afterwards, so for that
        # whole window SIGTERM kept its default disposition and killed the
        # process outright: `vq daemon stop`, `systemctl --user stop` and
        # `launchctl bootout` all ended in an instant death, with no shutdown
        # and no line in the daemon log. That is the ungraceful removal a
        # self-update rollback recorded, and the window that most needs a
        # handler was the one window that had none.
        self._install_stop_handlers()
        self._reattach_or_interrupt_at_startup()
        if self._stop:
            # Stopped during the walk. Nothing below is worth doing for a
            # daemon that is leaving, and the RPC socket in particular must
            # not be published: an updater polling for readiness would take
            # it as proof this daemon came up.
            log.info("daemon stopped during startup before RPC came up")
            self._close_running_logs()
            return
        self._reseed_host_pressure_pauses_at_startup()  # HP-1
        # v0.11.0: reap a stale admin-update marker left by a prior
        # daemon life — a killed `vq admin update`, or one whose host
        # rebooted (the marker persists in the state dir). Without this a
        # corpse marker silently gates dispatch from the very first tick
        # (compute-b/build-host 2026-06-18). A marker that's actually live — an
        # update orchestrating THIS restart, pid still alive — is detected
        # as live and correctly keeps holding dispatch. Cheap: one file
        # read + an os.kill probe, once at startup.
        self._poll_admin_update_marker()
        # v0.8.0 *Dahl's Simula*: start the RPC server so CLI
        # clients can read/write admin-status through the
        # daemon's canonical view. Failure to start is logged
        # but not fatal — the daemon's primary job (dispatch) is
        # unaffected; CLIs fall back to direct file access.
        self._rpc_server: _rpc_module.RPCServer | None = None
        try:
            from vq import rpc as _rpc_module
            drain.prepare_read_only_drain_snapshot_locks(
                multi_user=self._multi_user,
            )
            rpc_source = Path(_rpc_module.__file__)
            self._rpc_server = _rpc_module.RPCServer(
                multi_user=self._multi_user,
                admin_group_gid=self._admin_group_gid_for_rpc(),
                source_sha_reader=lambda: admin.running_source_sha(
                    rpc_source,
                ),
                source_tree_sha256_reader=lambda: admin.running_source_tree_sha256(
                    rpc_source.resolve().parent,
                ),
            )
            _rpc_module.register_get_admin_status_method(
                self._rpc_server,
                lambda: admin.read_admin_status(via_rpc=False),
            )
            _rpc_module.register_set_admin_status_method(
                self._rpc_server,
                admin.replace_admin_status_record_from_mapping,
            )
            # v0.8.1 *Karp's Reduction*: drain.json + throttle.json get
            # the same daemon-mediated treatment as admin-status. Without
            # these registrations, CLI callers fall back to direct file
            # writes — correct in single-user, divergent in multi-user.
            _rpc_module.register_get_drain_state_method(
                self._rpc_server,
                lambda: drain.read_drain_state(
                    via_rpc=False,
                    multi_user=self._multi_user,
                ),
            )
            _rpc_module.register_get_drain_read_only_snapshot_method(
                self._rpc_server,
                lambda: drain.read_locked_drain_snapshot(
                    multi_user=self._multi_user,
                ),
            )
            _rpc_module.register_get_scheduler_drain_leases_method(
                self._rpc_server,
                lambda: drain.read_scheduler_drain_leases(
                    via_rpc=False,
                    multi_user=self._multi_user,
                ),
                schema_version=drain.SCHEDULER_DRAIN_LEASE_SCHEMA_VERSION,
            )
            _rpc_module.register_legacy_scheduler_drain_release_method(
                self._rpc_server,
                lambda host, expected_reason, expected_set_at: (
                    drain.release_legacy_scheduler_host(
                        host,
                        via_rpc=False,
                        expected_reason=expected_reason,
                        expected_set_at=expected_set_at,
                        multi_user=self._multi_user,
                    )
                ),
            )
            _rpc_module.register_owned_full_drain_release_method(
                self._rpc_server,
                lambda expected_reason, expected_set_at: (
                    drain.release_owned_full_drain(
                        expected_reason=expected_reason,
                        expected_set_at=expected_set_at,
                        via_rpc=False,
                        multi_user=self._multi_user,
                    )
                ),
            )
            _rpc_module.register_set_drain_state_method(
                self._rpc_server,
                clear_state=lambda: drain.clear_drain(
                    via_rpc=False,
                    multi_user=self._multi_user,
                ),
                replace_state=lambda state: (
                    drain.replace_drain_state_from_mapping(
                        state,
                        multi_user=self._multi_user,
                    )
                ),
            )
            _rpc_module.register_set_scheduler_drain_lease_method(
                self._rpc_server,
                lambda lease, release_id, release_host, release_owner, release_all: (
                    drain.apply_scheduler_drain_lease_mapping_mutation(
                        lease=lease,
                        release_id=release_id,
                        release_host=release_host,
                        release_owner=release_owner,
                        release_all=release_all,
                        multi_user=self._multi_user,
                    )
                ),
                schema_version=drain.SCHEDULER_DRAIN_LEASE_SCHEMA_VERSION,
            )
            _rpc_module.register_get_throttle_state_method(
                self._rpc_server,
                lambda: throttle.read_throttle_state(via_rpc=False),
            )
            _rpc_module.register_set_throttle_state_method(
                self._rpc_server,
                clear_state=lambda: throttle.clear_throttle_state(
                    via_rpc=False,
                ),
                replace_state=throttle.replace_throttle_state_from_mapping,
            )
            _rpc_module.register_capacity_methods(
                self._rpc_server,
                advertised_capacity,
            )
            _rpc_module.register_get_scheduler_status_refresh_method(
                self._rpc_server,
                self.request_scheduler_status_refresh,
            )
            # Needs the Daemon instance: a reload mutates in-memory caches,
            # not a state file.
            _rpc_module.register_reload_methods(self._rpc_server, self)
            self._rpc_server.start()
        except Exception:  # noqa: BLE001 — RPC must not block startup
            log.exception(
                "RPC server failed to start; daemon continuing "
                "without RPC. CLI clients will fall back to direct "
                "file access (multi-user mode: stale view risk).",
            )
            self._rpc_server = None
        self._background_scheduler_polling = True
        try:
            while not self._stop:
                # Clear before the pass. A request published during the pass
                # remains set and makes the following wait return immediately;
                # a request racing the next clear is already in the monotonic
                # request sequence and is observed by that next pass.
                self._scheduler_refresh_wakeup.clear()
                try:
                    self.iterate()
                    if self._loop_hook is not None:
                        self._loop_hook()
                except Exception:
                    log.exception("error in main loop iteration; continuing")
                self._scheduler_refresh_wakeup.wait(self.poll_interval)
        finally:
            self._background_scheduler_polling = False
            # Poll workers are daemon threads and mutate only their private
            # flight record. Dropping the registry prevents any late result
            # from being applied after shutdown; transport timeouts bound the
            # remaining read-only command.
            self._scheduler_poll_flights.clear()
            # No late transfer outcome may publish a lifecycle transition.
            # A detached transport can finish its unique scratch archive, but
            # a restarted daemon uses another archive and re-proves the job's
            # terminal evidence. Neither process removes the other's archive.
            self._scheduler_fetch_flights.clear()
            self._scheduler_fetch_order.clear()
            self._scheduler_fetch_admissions_left = None
            with self._scheduler_refresh_condition:
                self._scheduler_refresh_condition.notify_all()
            if self._rpc_server is not None:
                try:
                    self._rpc_server.stop()
                except Exception:
                    log.exception("RPC server stop failed")
            self._close_running_logs()
        log.info("daemon stopped")

    def _admin_group_gid_for_rpc(self) -> int | None:
        """Resolve the admin-group GID for the RPC socket's chown.
        Returns None in single-user mode (no chown needed) or if
        the multi-user config doesn't name an admin group (rare)."""
        if not self._multi_user:
            return None
        try:
            import grp

            from vq.config import load_config
            cfg = load_config()
            group_name = cfg.multi_user.admin_group
            if not group_name:
                return None
            return grp.getgrnam(group_name).gr_gid
        except Exception:  # noqa: BLE001 — best-effort
            return None

    def request_scheduler_status_refresh(
        self,
        jobid: str,
        timeout_seconds: float,
    ) -> dict[str, object]:
        """Wait for a new successful observation of one exact job.

        The RPC thread never touches scheduler dispatchers or specs.  It only
        publishes a sequence number and wakes the daemon loop; the main loop
        performs the usual batched reconcile and acknowledges that exact
        sequence afterwards.  A completed global pass is insufficient: the
        requested job must itself have a successful scheduler observation
        durably stamped during that pass.  Timeout or omission is not proof of
        any scheduler state.
        """
        jobid = validate_job_id(jobid)
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(float(timeout_seconds))
            or float(timeout_seconds) <= 0
            or float(timeout_seconds) > SCHEDULER_STATUS_RPC_MAX_SECONDS
        ):
            raise ValueError(
                "timeout_seconds must be finite, greater than zero, and at most "
                f"{SCHEDULER_STATUS_RPC_MAX_SECONDS:g}"
            )
        timeout = float(timeout_seconds)
        deadline = time.monotonic() + timeout
        with self._scheduler_refresh_condition:
            self._scheduler_refresh_requested += 1
            request_id = self._scheduler_refresh_requested
            self._scheduler_refresh_requests[request_id] = jobid
            self._scheduler_refresh_wakeup.set()
            while request_id not in self._scheduler_refresh_results and not self._stop:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._scheduler_refresh_requests.pop(request_id, None)
                    return {
                        "schema": "vq.scheduler.status_refresh/1",
                        "completed": False,
                        "observed_at": None,
                        "reason": "timeout",
                    }
                self._scheduler_refresh_condition.wait(remaining)
            result = self._scheduler_refresh_results.pop(request_id, None)
            self._scheduler_refresh_requests.pop(request_id, None)
            if result is not None:
                return result
            return {
                "schema": "vq.scheduler.status_refresh/1",
                "completed": False,
                "observed_at": None,
                "reason": "daemon_stopping",
            }

    def iterate(self) -> None:
        """One pass: reconcile running + orphans, sample watchdog, dispatch pending."""
        self._maybe_apply_config_reload()
        # A pauser fsyncs its intent before SIGSTOP.  Complete either crash
        # window before polling/watchdog work so a killed admin process cannot
        # leave a live child RUNNING-but-stopped and untagged in the durable
        # queue.  Import locally to keep daemon startup dependencies acyclic.
        from vq.pause_resume import reconcile_pause_intents

        pause_intents = reconcile_pause_intents(
            "localhost",
            queue_dir=self.queue_dir,
            multi_user=self._multi_user,
            # This runs every tick over every retained job, so it reads each
            # row before locking it and skips the ones with no intent to
            # finish (#22). A tick is a repeated sweep, not a proof: an intent
            # armed between that read and the lock is picked up next tick.
            omit_rows_without_intent=True,
        )
        for jobid, detail in pause_intents.errors:
            log.error(
                "job %s: durable pause-intent reconciliation failed: %s",
                jobid,
                detail,
            )
        self._reconcile_running()
        self._reconcile_terminal_survivors()
        self._retry_deferred_scheduler_reattach()
        self._reconcile_scheduler()
        self._reconcile_orphans()
        # v0.6.20: host-pressure pass — pause-all / resume-all under
        # global memory pressure. Runs BEFORE the per-job watchdog
        # so a pause-decision this tick is reflected in spec.state
        # by the time _watchdog_pass evaluates each job (watchdog
        # treats SUSPENDED as "skip kill paths").
        self._host_pressure_pass()
        self._watchdog_pass()
        self._dispatch_pending()
        # v0.5.17: opt-in auto-cleanup. Reads <state_root>/auto-cleanup.json
        # every iteration (cheap stat + json parse); if the policy says
        # the interval has elapsed since last_run_at, runs an archive +
        # delete sweep using the same primitives as the manual verb.
        # Cost when no policy is set: one path.exists() call.
        self._maybe_auto_cleanup()
        # v0.6.2: periodically re-read vq.__version__ from on-disk
        # source and warn if it has drifted from the running daemon's
        # version. Audit § 1c: pre-v0.6.2 the daemon had no way to
        # notice it was running stale code after a manual `git pull`
        # that didn't go through `vq admin update`. The auto-restart
        # path (v0.5.42+) handles the standard case; this probe is the
        # safety net for operator bypass.
        self._maybe_check_version_drift()

    def _maybe_auto_cleanup(self) -> None:
        """v0.5.17 hook: run an auto-cleanup sweep if the policy's
        interval has elapsed.

        Automatic retention is lower priority than live job lifecycle work.
        A large cleanup pass is synchronous, so starting one after dispatching
        a child can delay the next ``poll()`` / ``waitpid()`` long enough to
        leave an exited process as a zombie and its spec falsely RUNNING.
        Defer while any local, reattached, or scheduler job is active; the
        elapsed policy remains due and is retried once the daemon is quiescent.
        Operators can still invoke ``vq cleanup`` explicitly while jobs run.

        Defensive: any exception during the sweep is logged but doesn't
        propagate. A botched cleanup pass mustn't take down the daemon's
        dispatch loop. The error is visible in the daemon log + the
        policy's ``last_run_at`` is still stamped so we don't retry
        every iteration.
        """
        if (
            self._running
            or self._orphans
            or self._scheduler_running
            or self._terminal_survivors
        ):
            return
        try:
            from vq.cleanup import (  # local import to avoid cycle
                read_auto_cleanup_policy,
                run_auto_cleanup_pass,
                should_run_auto_cleanup,
            )

            policy = read_auto_cleanup_policy()
            if policy is None:
                return
            if not should_run_auto_cleanup(policy):
                return
            # v0.6.39: in multi-user mode the pass sweeps every
            # per-user state tree under /var/lib/vq/users/<uid>/.
            run_auto_cleanup_pass(
                policy,
                multi_user=self._multi_user,
                scheduler_workspace_reaper=self._cleanup_scheduler_remote_workspace,
            )
        except Exception:
            log.exception("auto-cleanup pass failed; will retry next interval")

    def _cleanup_scheduler_remote_workspace(self, spec: JobSpec) -> None:
        """Remove an old terminal scheduler job's remote workspace.

        The cleanup module owns retention policy and terminal-state guards; the
        daemon supplies the live scheduler dispatcher/transport needed to reach
        the cluster-side workspace.
        """
        if spec.scheduler_target is None:
            return
        dispatcher = self._scheduler_dispatcher_for(spec.scheduler_target)
        handle = scheduler_handle_for_spec(
            dispatcher,
            spec,
            job_id=spec.scheduler_job_id or spec.id,
        )
        dispatcher.cleanup_remote_workspace(handle)

    def _maybe_check_version_drift(self) -> None:
        """v0.6.2: every ``_VERSION_DRIFT_CHECK_INTERVAL_SECONDS``,
        re-read ``vq.__version__`` from on-disk source and warn if
        the running daemon's import-time version disagrees. Cheap
        (one file read + one regex) and rate-limited to once per
        minute via ``self._last_version_drift_check_monotonic`` to
        keep the dispatch loop's overhead negligible.

        Audit § 1c: ``operations.md`` documented "the running daemon
        version is whatever it imported at startup — not directly
        inspectable without restart" as a known gap. The v0.5.42+
        auto-restart path closes the standard case (``vq admin update
        vibeqc-queue`` restarts the daemon). This probe is the safety
        net for operator bypass — manual ``git pull`` + forgot to
        restart, hand-edited source under the daemon, etc.

        Logs WARNING once per transition (drift detected / drift
        cleared) so the daemon log doesn't fill with the same line
        every minute. State kept in
        ``self._version_drift_seen_at_version``; None means no drift
        has been logged yet, or the drift cleared (operator restarted
        the daemon, the new daemon's import-time version matches
        on-disk again).
        """
        now_mono = time.monotonic()
        if (
            now_mono - self._last_version_drift_check_monotonic
            < _VERSION_DRIFT_CHECK_INTERVAL_SECONDS
        ):
            return
        self._last_version_drift_check_monotonic = now_mono
        on_disk = _read_vq_version_from_source()
        if on_disk is None:
            return  # source file gone / unparseable; not actionable
        from vq import __version__ as running_version

        if on_disk == running_version:
            # Drift cleared (or never seen).
            if self._version_drift_seen_at_version is not None:
                log.info(
                    "vq version drift cleared: daemon and on-disk source both report %s",
                    running_version,
                )
                self._version_drift_seen_at_version = None
            return
        # Drift present. Log on the first observation of THIS specific
        # on-disk version (so the line fires when the operator pulls
        # a new version, and again if they pull a further one without
        # restarting).
        if self._version_drift_seen_at_version != on_disk:
            log.warning(
                "vq version drift: daemon running %s, on-disk source "
                "says %s — run `vq admin update vibeqc-queue` (or "
                "`systemctl --user restart vq-daemon`) to pick up the "
                "new code",
                running_version,
                on_disk,
            )
            self._version_drift_seen_at_version = on_disk

    def stop(self) -> None:
        with self._scheduler_refresh_condition:
            self._stop = True
            self._scheduler_refresh_condition.notify_all()
        self._scheduler_refresh_wakeup.set()

    def _maybe_reap_terminal_survivor(self, spec: JobSpec) -> None:
        """STATE-2 (v0.8.15): SIGKILL a terminal job's process group that
        survived a daemon restart.

        The startup reattach scan only handles RUNNING/SUSPENDED specs, so a
        spec that was already terminal (``vq kill`` / a watchdog kill) when
        the daemon went down — but whose process group is *still alive* — is
        never reaped. It leaks: untracked by ``_running`` / ``_orphans``,
        consuming CPU/RAM, while its spec reads KILLED. This is the
        restart-time companion to STATE-3 (the same condition during normal
        operation).

        The original killer already SIGTERM'd this process (``kill.py``
        signals before writing the terminal label; the watchdog SIGTERMs at
        the transition) and it ignored that SIGTERM for the entire daemon
        downtime — so the SIGTERM grace is long elapsed. We SIGKILL directly
        (SIGCONT first in case it was left stopped — SIGKILL kills a stopped
        process anyway, but the SIGCONT is explicit and harmless).

        Guarded by the pid-fingerprint cross-check so we never SIGKILL a pgid
        the kernel recycled to an unrelated process during the downtime:
        ``_pid_fingerprint_matches`` returning ``False`` means "not our
        process" → skip. ``None`` (macOS / pre-v0.5.50 spec) falls back to
        pgid-only liveness, matching the reattach path's handling.
        """
        if spec.pgid is None or not _pgroup_alive(spec.pgid):
            return
        if _pid_fingerprint_matches(spec) is False:
            log.warning(
                "job %s: terminal (%s) pgid %s is alive but the pid "
                "fingerprint says the PID was recycled; NOT reaping (the "
                "live process is not ours)",
                spec.id,
                spec.state.value,
                spec.pgid,
            )
            return
        log.warning(
            "job %s: terminal (%s) but pgid %s survived the daemon restart; "
            "SIGKILL-reaping the leaked process group (STATE-2)",
            spec.id,
            spec.state.value,
            spec.pgid,
        )
        with contextlib.suppress(ProcessLookupError):
            killpg(spec.pgid, signal.SIGCONT)
        with contextlib.suppress(ProcessLookupError):
            killpg(spec.pgid, signal.SIGKILL)
        self._reap_scope(spec.id)
        events.append_event(
            Path(spec.cwd),
            events.EventKind.STATE_TRANSITION,
            spec.id,
            **{
                "from": spec.state.value,
                "to": spec.state.value,
                "reason": (
                    "daemon restart; terminal job's leaked process group "
                    "SIGKILLed (STATE-2)"
                ),
            },
        )

    def _reattach_or_interrupt_at_startup(self) -> None:
        """For each spec in RUNNING (or SUSPENDED) state at daemon startup:

        * if pgid is alive -> stay in same state, track as orphan,
          register with watchdog so /proc sampling continues.
        * if pgid is gone AND a v0.5.9 exit-marker exists -> classify
          via :meth:`_record_orphan_finish` (COMPLETED / FAILED with
          recovered rc). This is the "job finished while the daemon
          was down" case -- common during ``systemctl --user restart
          vq-daemon``.
        * if pgid is gone AND no marker -> ABORTED_BY_QUEUE (v0.4.1
          path; "the queue ended this job"; clearer than INTERRUPTED).
        * if pgid is missing (pre-v0.3 spec) -> ABORTED_BY_QUEUE for
          the same reason.

        SUSPENDED jobs (v0.5.1) survive across daemon restarts: SIGSTOP
        keeps the process frozen but alive, so the pgid check returns
        True and the spec stays SUSPENDED. The user resumes via
        ``vq resume`` whenever they're ready.

        Abort cases preserve stdout.log / stderr.log so the submitter
        can read the script's output / traceback to figure out what
        happened (Python error vs. silent OOM vs. successful completion
        lost to the gap).
        """
        # v0.5.30: specs THIS startup pass moves to ABORTED_BY_QUEUE
        # from a RUNNING entry state. Tracked separately from "all
        # ABORTED_BY_QUEUE specs" so the auto-resume pass below only
        # resumes jobs that died in THIS reboot — scanning every
        # ABORTED_BY_QUEUE spec would resubmit-storm on each restart.
        # SUSPENDED-at-entry jobs are deliberately excluded: the user
        # paused those, and un-pausing via a resubmit contradicts that
        # intent.
        just_aborted_running: list[JobSpec] = []
        for spec in self._iter_specs():
            # #53: a stop that lands mid-walk is honoured at the next spec
            # boundary. `vq daemon stop` waits 10 s, launchd and systemd
            # escalate to SIGKILL on their own timeouts, so a daemon that
            # merely noted the signal and then finished a several-minute
            # scan would still be killed ungracefully. Each spec is
            # reconciled and persisted independently, and the walk exists
            # precisely to reconcile whatever is on disk at startup, so an
            # unvisited spec is simply reconciled by the next daemon life.
            if self._stop:
                log.info(
                    "stop requested during startup reattach; "
                    "leaving the remaining specs for the next daemon start",
                )
                break
            binding_fence = self._scheduler_binding_requires_fence(spec)
            transaction_may_exist = (
                self._scheduler_transaction_may_exist(spec)
                or binding_fence
            )
            if transaction_may_exist:
                spec, owner_uid, spec_path, expected_target = (
                    self._capture_scheduler_transaction_authority(spec)
                )
            else:
                owner_uid = self._job_uid.get(spec.id) if self._multi_user else None
                spec_path = (
                    self._job_spec_paths.get(spec.id) if self._multi_user else None
                )
                expected_target = spec.scheduler_target
            evidence_conflict = spec.scheduler_state in {
                "submit_evidence_conflict",
                "submit_evidence_conflict_after_terminal",
            }
            unconfirmed_submit = not evidence_conflict and (
                binding_fence
                or
                spec.state
                in {JobState.SUBMITTING, JobState.SUBMIT_OUTCOME_UNKNOWN}
                or spec.scheduler_state
                in {
                    "submitting",
                    "submit_outcome_unknown",
                    "submit_outcome_unknown_after_terminal",
                    "submit_reconciliation_quarantined_after_terminal",
                    "submit_cancel_pending_after_terminal",
                }
            )
            if unconfirmed_submit:
                if expected_target is None:
                    self._quarantine_unconfirmed_scheduler_spec(
                        spec.id,
                        owner_uid=owner_uid,
                        spec_path=spec_path,
                        reason="unconfirmed scheduler submit has no bound target",
                    )
                    continue
                if self._multi_user:
                    reason = self._validate_multi_user_spec(spec)
                    if owner_uid is None or spec_path is None:
                        reason = (
                            reason
                            or "cannot bind the unconfirmed submit to its owning path"
                        )
                    if reason is not None:
                        self._quarantine_unconfirmed_scheduler_spec(
                            spec.id,
                            owner_uid=owner_uid,
                            spec_path=spec_path,
                            reason=f"multi-user spec gate: {reason}",
                            expected_target=expected_target,
                        )
                        continue
                self._reconcile_unconfirmed_scheduler_submit(
                    spec,
                    owner_uid=owner_uid,
                    spec_path=spec_path,
                    expected_target=expected_target,
                )
                continue
            if spec.state not in (JobState.RUNNING, JobState.SUSPENDED):
                # STATE-2: a spec that was already terminal when the daemon
                # went down, whose process group SURVIVED the downtime, would
                # otherwise leak — the rest of this scan only handles
                # RUNNING/SUSPENDED, so nothing ever reaps it. SIGKILL it.
                if spec.is_terminal:
                    self._maybe_reap_terminal_survivor(spec)
                continue
            entry_state = spec.state  # RUNNING or SUSPENDED, pre-mutation
            workspace = Path(spec.cwd)
            if spec.scheduler_target is not None:
                # v1.0 cluster backend (§17): a job dispatched over SSH+qsub
                # keeps running on the cluster across a driver-daemon restart.
                # Rebuild its handle from the persisted scheduler_job_id and
                # resume tracking; _reconcile_scheduler then reaps it normally.
                # If the handle can't be rebuilt (host config gone, no id), do
                # NOT classify the job terminal at startup. A qsub may have
                # succeeded just before the driver daemon died, so aborting here
                # can turn a live PBS job into a false ABORTED_BY_QUEUE. Mark it
                # explicitly untracked; later daemon ticks retry reattach after
                # config/the spec is repaired.
                if self._reattach_scheduler_job(spec):
                    continue
                # A receipt/marker identity conflict is not an ordinary
                # transient reattach failure: it is durable ambiguity about
                # which remote allocation exists.  _reattach_scheduler_job
                # parks it as SUBMIT_OUTCOME_UNKNOWN; preserve that indefinite
                # reservation rather than rewriting it to the age-limited
                # reattach_failed state below.
                try:
                    after_reattach, _ = self._read_active_spec(
                        spec.id,
                        owner_uid=(
                            self._job_uid.get(spec.id) if self._multi_user else None
                        ),
                        spec_path=(
                            self._job_spec_paths.get(spec.id)
                            if self._multi_user
                            else None
                        ),
                    )
                except (OSError, ValueError):
                    after_reattach = None
                if (
                    after_reattach is not None
                    and (
                        after_reattach.state == JobState.SUBMIT_OUTCOME_UNKNOWN
                        or after_reattach.scheduler_state
                        == "scheduler_reconciliation_quarantined"
                    )
                ):
                    continue
                self._defer_scheduler_reattach(
                    spec, reason="scheduler job could not be reattached at startup"
                )
                continue
            if spec.pgid is None:
                self._mark_aborted_by_queue(spec, reason="no pgid recorded; cannot check liveness")
                if entry_state == JobState.RUNNING:
                    just_aborted_running.append(spec)
                continue
            if _pgroup_alive(spec.pgid):
                # v0.5.50: anti-recycle check. pgid liveness alone can
                # be fooled by the kernel reusing spec.pid for an
                # unrelated process after the daemon died — that
                # unrelated process can also re-use the recycled pgid.
                # Cross-check /proc/<pid>/stat field 22 (start time)
                # against the value we captured at dispatch. Mismatch
                # → declare ABORTED_BY_QUEUE rather than silently
                # re-attaching to someone else's process. None means
                # we can't tell (macOS, pre-v0.5.50 spec) → fall back
                # to the pgid-only liveness check.
                fp = _pid_fingerprint_matches(spec)
                if fp is False:
                    self._mark_aborted_by_queue(
                        spec,
                        reason=(
                            "pid_recycled: spec.pid="
                            f"{spec.pid} is alive but /proc/<pid>/stat "
                            f"start-time does not match the value "
                            f"recorded at dispatch — kernel reused the "
                            f"PID for a different process"
                        ),
                    )
                    if entry_state == JobState.RUNNING:
                        just_aborted_running.append(spec)
                    continue
                # v0.6.0: cgroup-scope MainPID cross-check (audit
                # § 4c.2). When cgroups are in play, systemd tracks
                # the scope unit's MainPID. If that PID disagrees
                # with spec.pid, the scope is detached / collected /
                # taken over by something else — we lost cgroup
                # ownership of this process. Mark ABORTED_BY_QUEUE
                # with a distinct reason so the operator sees the
                # different failure class. None from scope_main_pid
                # means "can't tell" (macOS, scope cleaned up
                # already, systemctl unreachable) → fall back to
                # the pgid liveness verdict.
                if self.cgroup_enabled and spec.pid is not None:
                    scope_pid = cgroup.scope_main_pid(
                        f"vq-job-{spec.id}",
                        multi_user=self._multi_user,
                    )
                    if scope_pid is not None and scope_pid != spec.pid:
                        self._mark_aborted_by_queue(
                            spec,
                            reason=(
                                f"cgroup_scope_mismatch: spec.pid="
                                f"{spec.pid} but vq-job-{spec.id}"
                                f".scope MainPID={scope_pid} — "
                                f"systemd no longer associates this "
                                f"job's pid with its scope unit"
                            ),
                        )
                        if entry_state == JobState.RUNNING:
                            just_aborted_running.append(spec)
                        continue
                self._orphans[spec.id] = _OrphanJob(
                    pgid=spec.pgid,
                    cpus=spec.cpus,
                    mem_mb=spec.mem_mb,
                    # v0.6.35: the trusted uid is the per-user state
                    # dir the spec was read from (_job_uid), NOT the
                    # user-writable spec.submitter field. None in
                    # single-user mode (_job_uid is empty there).
                    uid=self._job_uid.get(spec.id),
                    spec_path=self._spec_path(spec.id),
                )
                # Watchdog needs a wall-clock anchor. We don't know the
                # original started_at clock tick, so use "now" -- this
                # means wall_time_seconds resets on daemon restart, which
                # is the conservative choice (better to not falsely TIME_EXCEEDED
                # a freshly re-attached job).
                self.watchdog.register(spec.id)
                log.info(
                    "job %s pgid=%s still alive at startup; re-attached as orphan",
                    spec.id,
                    spec.pgid,
                )
                events.append_event(
                    workspace,
                    events.EventKind.STATE_TRANSITION,
                    spec.id,
                    **{
                        "from": JobState.RUNNING.value,
                        "to": JobState.RUNNING.value,
                        "reason": "daemon restart; orphan still alive, re-attached",
                    },
                )
            else:
                rc = _read_exit_marker(workspace)
                if rc is not None:
                    self._record_orphan_finish(spec, rc, source="exit-marker (startup)")
                else:
                    self._mark_aborted_by_queue(
                        spec,
                        reason="daemon restart; orphan pgid is gone (no marker)",
                        process_exit_confirmed=True,
                    )
                    if entry_state == JobState.RUNNING:
                        just_aborted_running.append(spec)

        # v0.5.30: auto-resume pass. A job that was RUNNING when the
        # daemon went down, whose process group is now gone (the
        # hard-reboot signature: kernel SIGKILL'd everything before the
        # command wrapper could write its exit marker), and that was submitted
        # with --auto-resume, gets a sibling resubmit. Runs after the
        # main loop so the just-written sibling specs don't perturb the
        # iteration.
        # #53: this pass still runs after an interrupted walk. It covers only
        # the specs THIS pass aborted, and a sibling is a durable PENDING spec
        # that the next daemon dispatches. Skipping it would lose those resumes
        # for good: their parents are ABORTED_BY_QUEUE now, so a later startup
        # no longer sees them enter from RUNNING.
        for spec in just_aborted_running:
            if spec.recover_on_reboot:
                self._auto_resume(spec)

    def _auto_resume(self, dead: JobSpec) -> None:
        """v0.5.30: emit a sibling resubmit of a job killed by a host
        reboot.

        ``dead`` is a spec this startup pass just moved to
        ABORTED_BY_QUEUE (RUNNING -> gone-pgid -> no marker) AND that
        carries ``recover_on_reboot=True``.

        The sibling: fresh jobid, SAME command + SAME workspace (so the
        job's own restart-from-disk logic — CRYSTAL GUESSP=fort.20,
        PySCF chkfile, ORCA .gbw — can pick up partial state),
        ``parent_jobid = dead.id``. ``recover_on_reboot`` propagates so
        a sibling that ALSO dies in a later reboot is itself resumed;
        the ``parent_jobid`` chain records the full lineage.

        The sibling shares the dead job's workspace, so it also shares
        ``events.jsonl`` — the SUBMITTED event appended here lands right
        after the dead job's abort event, giving one continuous history.

        Best-effort: a failed spec write logs but doesn't abort startup
        (the dead job stays ABORTED_BY_QUEUE, recoverable by hand)."""
        new_id = new_jobid()
        sibling = JobSpec(
            id=new_id,
            command=list(dead.command),
            cwd=dead.cwd,  # SAME workspace — restart-from-disk needs the partials
            cpus=dead.cpus,
            scheduler_tasks=dead.scheduler_tasks,
            mem_mb=dead.mem_mb,
            wall_time_seconds=dead.wall_time_seconds,
            priority=dead.priority,
            recover_on_reboot=True,  # keep resuming across successive reboots
            parent_jobid=dead.id,
            program=dead.program,
            program_runtime_pin=(
                dead.program_runtime_pin.model_copy(
                    update={"resolved_git_sha": None},
                    deep=True,
                )
                if dead.program_runtime_pin is not None
                else None
            ),
            # v0.5.31: carry the retry budget forward so --retry and
            # --auto-resume compose — a job that had spent 2 of 3
            # retries before a reboot resumes with 2/3 still spent.
            # not_before is intentionally NOT carried: the resume should
            # dispatch promptly, not sit in a stale backoff window.
            retry_max=dead.retry_max,
            retry_count=dead.retry_count,
            state=JobState.PENDING,
            submitter=dead.submitter,
            workspace_source=dead.workspace_source,
        )
        try:
            sibling.write(self._spec_path(new_id))
        except OSError as e:
            log.error(
                "auto-resume: failed to write sibling spec for %s: %s "
                "(%s stays ABORTED_BY_QUEUE; resubmit by hand)",
                dead.id,
                e,
                dead.id,
            )
            return
        log.info(
            "auto-resume: job %s killed by reboot -> resubmitted as %s (same workspace: %s)",
            dead.id,
            new_id,
            dead.cwd,
        )
        events.append_event(
            Path(dead.cwd),
            events.EventKind.SUBMITTED,
            new_id,
            command=sibling.command,
            cpus=sibling.cpus,
            parent_jobid=dead.id,
            program=sibling.program,
            reason="auto-resume after host reboot",
        )

    def _mark_aborted_by_queue(
        self,
        spec: JobSpec,
        *,
        reason: str,
        evidence: dict[str, object] | None = None,
        process_exit_confirmed: bool = False,
        jobid: str | None = None,
        owner_uid: str | None = None,
        spec_path: Path | None = None,
    ) -> None:
        """Move a spec to ABORTED_BY_QUEUE with a recorded reason. Used
        at startup (pgid gone / never recorded) and when an orphan we've
        been polling finally exits during the daemon's downtime.

        ABORTED_BY_QUEUE is distinct from INTERRUPTED (legacy v0.3
        bucket) and from KILLED (user requested) and from OOM_KILLED /
        TIME_EXCEEDED / STARVED (watchdog acted). It tells the submitter
        "the queue ended this for reasons specific to the queue's own
        lifecycle; check stdout.log / stderr.log to see if your job had
        already produced useful output."
        """
        admitted_jobid = jobid or spec.id
        active_runtime = (
            self._orphans.get(admitted_jobid)
            or self._running.get(admitted_jobid)
            or self._scheduler_running.get(admitted_jobid)
        )
        spec_path = self._active_spec_path(
            admitted_jobid,
            active_runtime,
            owner_uid=owner_uid,
            spec_path=spec_path,
        )
        # v0.8.11 *Dekker's Mutex*: lock + re-read so we don't clobber a
        # terminal label a racing writer (`vq kill`, an exit-marker reap)
        # set between the caller's read and now. All callers pass a
        # non-terminal spec, so the is_terminal bail only fires on a genuine
        # race — where preserving the real terminal state is the correct
        # outcome. The webhook fires after the lock releases.
        with paths.spec_lock(spec_path):
            try:
                fresh, _ = self._read_active_spec(
                    admitted_jobid,
                    active_runtime,
                    owner_uid=owner_uid,
                    spec_path=spec_path,
                )
            except (OSError, ValueError):
                return
            if fresh.is_terminal:
                log.info(
                    "job %s already terminal (%s); skipping ABORTED_BY_QUEUE",
                    admitted_jobid,
                    fresh.state,
                )
                return
            log.warning(
                "job %s -> ABORTED_BY_QUEUE (pid=%s, pgid=%s): %s",
                admitted_jobid,
                fresh.pid,
                fresh.pgid,
                reason,
            )
            prev_state = fresh.state
            fresh.state = JobState.ABORTED_BY_QUEUE
            fresh.finished_at = utcnow_iso()
            fresh.failure_reason = reason
            fresh.write(spec_path)
            events.state_transition(
                Path(fresh.cwd),
                admitted_jobid,
                from_state=prev_state.value,
                to_state=JobState.ABORTED_BY_QUEUE.value,
                reason=reason,
                **(evidence or {}),
            )
        # v0.5.35: queue-side terminal transition → fire webhook (outside lock).
        notify.send_terminal_notification(
            fresh, self.notify_webhook_url,
            notify_on_states=self.notify_on_states,
        )
        # A terminal label is not proof that the process is gone. In
        # particular, pgid=None is the crash-mid-Popen signature and a cgroup
        # mismatch can coexist with a live matching pgid. Deleting scratch in
        # either case races active work. Only callers that observed the local
        # process group or remote scheduler job exit may opt into cleanup.
        if process_exit_confirmed:
            self._maybe_cleanup_workdir(
                fresh,
                owner_uid=owner_uid,
                spec_path=spec_path,
            )

    def _mark_scheduler_time_exceeded(
        self,
        spec: JobSpec,
        *,
        reason: str,
        evidence: dict[str, object] | None = None,
        terminal_side_effects: bool = True,
        jobid: str | None = None,
        owner_uid: str | None = None,
        spec_path: Path | None = None,
    ) -> None:
        """Move a scheduler-backed spec to TIME_EXCEEDED with evidence.

        Used when scheduler accounting says the walltime limit fired, including
        cases where the PBS wrapper was killed before it could write an
        exit-marker. If a marker is later recovered, ``_record_finish`` preserves
        this terminal label and only fills in the exit code.
        """
        admitted_jobid = jobid or spec.id
        active_runtime = self._scheduler_running.get(admitted_jobid)
        spec_path = self._active_spec_path(
            admitted_jobid,
            active_runtime,
            owner_uid=owner_uid,
            spec_path=spec_path,
        )
        with paths.spec_lock(spec_path):
            try:
                fresh, _ = self._read_active_spec(
                    admitted_jobid,
                    active_runtime,
                    owner_uid=owner_uid,
                    spec_path=spec_path,
                )
            except (OSError, ValueError):
                return
            if fresh.is_terminal:
                log.info(
                    "job %s already terminal (%s); skipping scheduler TIME_EXCEEDED",
                    admitted_jobid,
                    fresh.state,
                )
                return
            log.warning("job %s -> TIME_EXCEEDED: %s", admitted_jobid, reason)
            prev_state = fresh.state
            fresh.state = JobState.TIME_EXCEEDED
            fresh.finished_at = utcnow_iso()
            fresh.failure_reason = reason
            if evidence is not None:
                used = evidence.get("scheduler_walltime_used")
                limit = evidence.get("scheduler_walltime_limit")
                if isinstance(used, str):
                    fresh.scheduler_walltime_used = used
                if isinstance(limit, str):
                    fresh.scheduler_walltime_limit = limit
            if fresh.failure_tail is None:
                tail = _read_stderr_tail(Path(fresh.cwd), fresh.stderr_path)
                if tail:
                    fresh.failure_tail = tail
            fresh.write(spec_path)
            events.state_transition(
                Path(fresh.cwd),
                admitted_jobid,
                from_state=prev_state.value,
                to_state=JobState.TIME_EXCEEDED.value,
                reason=reason,
                **(evidence or {}),
            )
        if terminal_side_effects:
            notify.send_terminal_notification(
                fresh,
                self.notify_webhook_url,
                notify_on_states=self.notify_on_states,
            )
            self._maybe_cleanup_workdir(
                fresh,
                owner_uid=owner_uid,
                spec_path=spec_path,
            )

    def _mark_scheduler_abnormal_end(
        self,
        spec: JobSpec,
        *,
        abnormal_state: str,
        detail: QstatDetail,
        marker_rc: int | None,
        jobid: str,
        owner_uid: str | None = None,
        spec_path: Path | None = None,
    ) -> None:
        """Record a scheduler-attributed abnormal termination on a spec (#414).

        ``abnormal_state`` is the dialect's normalized accounting verdict
        (``OUT_OF_MEMORY`` / ``CANCELLED`` / ``TIMEOUT`` / ...). Writes the
        matching failed-class terminal state with the scheduler reason so the
        following :meth:`_record_finish` preserves that label and only fills
        in the observed rc — the reconciliation the incident jobs lacked,
        where a marker rc of 0 minted ``completed`` for OOM-killed runs.

        Like the watchdog's own kills, a scheduler kill is deliberately
        terminal before ``_record_finish`` runs, so retry-on-failure does not
        resubmit a job the scheduler ended (``_maybe_retry``'s caller
        contract). Notification and workdir cleanup stay with
        ``_record_finish``, which every caller invokes right after.
        """
        admitted_jobid = jobid or spec.id
        active_runtime = self._scheduler_running.get(admitted_jobid)
        spec_path = self._active_spec_path(
            admitted_jobid,
            active_runtime,
            owner_uid=owner_uid,
            spec_path=spec_path,
        )
        terminal_state = _SCHEDULER_ABNORMAL_END_STATES.get(
            abnormal_state, JobState.ABORTED_BY_QUEUE
        )
        reason = _scheduler_abnormal_end_reason(
            abnormal_state=abnormal_state,
            raw_state=detail.raw_state,
            marker_rc=marker_rc,
        )
        evidence: dict[str, object] = {
            "scheduler_target": spec.scheduler_target,
            "scheduler_job_id": spec.scheduler_job_id,
            "scheduler_accounting_state": detail.raw_state,
            "scheduler_abnormal_state": abnormal_state,
            "exit_marker_rc": marker_rc,
        }
        if detail.exit_code is not None:
            evidence["scheduler_accounting_exit_code"] = detail.exit_code
        with paths.spec_lock(spec_path):
            try:
                fresh, _ = self._read_active_spec(
                    admitted_jobid,
                    active_runtime,
                    owner_uid=owner_uid,
                    spec_path=spec_path,
                )
            except (OSError, ValueError):
                return
            if fresh.is_terminal:
                # A racing kill / walltime classification won the spec; that
                # terminal label is already failed-class — never completed.
                log.info(
                    "job %s already terminal (%s); skipping scheduler "
                    "abnormal-end %s",
                    admitted_jobid,
                    fresh.state,
                    abnormal_state,
                )
                return
            log.warning(
                "job %s -> %s: %s", admitted_jobid, terminal_state.value, reason
            )
            prev_state = fresh.state
            fresh.state = terminal_state
            fresh.finished_at = utcnow_iso()
            fresh.failure_reason = reason
            # The last live stamp froze scheduler_state at "running"; record
            # the scheduler's own terminal verdict instead.
            fresh.scheduler_state = abnormal_state.lower()
            if detail.walltime_used:
                fresh.scheduler_walltime_used = detail.walltime_used
            if detail.walltime_limit:
                fresh.scheduler_walltime_limit = detail.walltime_limit
            if fresh.failure_tail is None:
                tail = _read_stderr_tail(Path(fresh.cwd), fresh.stderr_path)
                if tail:
                    fresh.failure_tail = tail
            fresh.write(spec_path)
            events.state_transition(
                Path(fresh.cwd),
                admitted_jobid,
                from_state=prev_state.value,
                to_state=terminal_state.value,
                reason=reason,
                **evidence,
            )

    def _reconcile_orphans(self) -> None:
        """Poll orphan pgids; finish exited groups and escalate killed ones.

        v0.4 always marked exiting orphans ABORTED_BY_QUEUE because init
        reaped them and the rc was unrecoverable. v0.5.9's command wrapper
        leaves the rc in ``<workspace>/_vq/exit-code`` before exit, so
        we can usually do better:

        * marker present & parseable -> COMPLETED (rc=0) or FAILED (rc!=0)
          via :meth:`_record_orphan_finish`. The wrapper also captures
          signal exits (rc=128+sig), so a SIGTERM'd-while-daemon-down
          inner command lands in FAILED with a meaningful exit_code.
        * marker absent / unparseable AND spec is non-terminal ->
          ABORTED_BY_QUEUE (the v0.4 path; genuinely unknown rc, e.g.
          the wrapper was SIGKILL'd or the host crashed mid-write).
        * spec already terminal (watchdog kill, vq kill) -> leave the
          state alone but stash the rc for forensics (mirrors
          :meth:`_record_finish`).

        A live group whose spec becomes terminal keeps its reservation until
        exit or the kill grace expires. Escalation releases it even if zombie
        members still answer the group probe.

        The submitter should still consult stdout.log / stderr.log to
        see what the inner command actually printed before exiting.
        """
        gone: list[str] = []
        for jobid, orphan in list(self._orphans.items()):
            if (
                not _pgroup_alive(orphan.pgid)
                or self._escalate_orphan_if_killed(jobid, orphan)
            ):
                gone.append(jobid)
        for jobid in gone:
            orphan = self._orphans[jobid]
            try:
                spec, spec_path = self._read_active_spec(jobid, orphan)
            except (OSError, ValueError):
                # Spec deleted or made unreadable; just clean up our state.
                pass
            else:
                rc = _read_exit_marker(Path(spec.cwd))
                if rc is not None:
                    self._record_orphan_finish(
                        spec,
                        rc,
                        source="exit-marker",
                        jobid=jobid,
                        orphan=orphan,
                    )
                elif not spec.is_terminal:
                    self._mark_aborted_by_queue(
                        spec,
                        reason="orphan process exited (no exit-code marker)",
                        process_exit_confirmed=True,
                        jobid=jobid,
                        owner_uid=orphan.uid,
                        spec_path=spec_path,
                    )
            del self._orphans[jobid]
            self.watchdog.unregister(jobid)

    def _escalate_orphan_if_killed(self, jobid: str, orphan: _OrphanJob) -> bool:
        """Bound a killed orphan's grace without waiting for zombies to vanish.

        Reattached jobs have no Popen handle, so the running-child escalation
        never sees them. Keep the orphan charged through its grace, then use
        its captured process group rather than a mutable spec's pgid.
        """
        try:
            spec, _ = self._read_active_spec(jobid, orphan)
        except (OSError, ValueError):
            return False
        if not spec.is_terminal:
            orphan.term_deadline = None
            return False
        if orphan.pgid == os.getpgrp():
            return False
        now = time.monotonic()
        if orphan.term_deadline is None:
            orphan.term_deadline = now + KILL_ESCALATION_GRACE_SECONDS
            return False
        if now < orphan.term_deadline:
            return False
        log.warning(
            "job %s: terminal orphan (%s) pgid %s outlived the kill grace; "
            "escalating to SIGKILL",
            jobid, spec.state.value, orphan.pgid,
        )
        with contextlib.suppress(ProcessLookupError):
            killpg(orphan.pgid, signal.SIGCONT)
        with contextlib.suppress(ProcessLookupError):
            killpg(orphan.pgid, signal.SIGKILL)
        self._reap_scope(jobid)
        events.state_transition(
            Path(spec.cwd), jobid,
            from_state=spec.state.value, to_state=spec.state.value,
            reason="reattached orphan outlived the kill grace; SIGKILLed",
        )
        return True

    def _record_orphan_finish(
        self,
        spec: JobSpec,
        rc: int,
        *,
        source: str,
        jobid: str | None = None,
        orphan: _OrphanJob | None = None,
    ) -> None:
        """Move an orphan to a terminal state using a recovered exit code.

        The orphan-side analogue of :meth:`_record_finish`. The
        important difference: we never had a Popen handle, so the rc
        came from somewhere outside the daemon (the v0.5.9 marker
        file). ``source`` is logged as part of the state-transition
        event so forensics can tell "rc came from a marker" apart from
        "rc came from popen.poll()".
        """
        admitted_jobid = jobid or spec.id
        if orphan is None:
            orphan = self._orphans.get(admitted_jobid)
        spec_path = self._active_spec_path(admitted_jobid, orphan)
        owner_uid = orphan.uid if orphan is not None else None
        fire_notify = False
        spawn_rerun = False
        # v0.8.11 *Dekker's Mutex*: serialize against `vq kill` / the
        # watchdog. The lock is held ONLY across the spec read and write;
        # the webhook / rerun spawn / workdir cleanup run after it releases.
        # ``_maybe_retry`` writes the same spec inside the lock and so must
        # never take it itself.
        with paths.spec_lock(spec_path):
            # STATE-5: re-read under the lock. The caller's ``spec`` was read
            # before the lock and may be stale — a racing `vq kill` could have
            # written a terminal label between the orphan-reaper's dir scan
            # and now. The re-read is the authoritative state we mutate.
            try:
                spec, _ = self._read_active_spec(
                    admitted_jobid,
                    orphan,
                    owner_uid=owner_uid,
                    spec_path=spec_path,
                )
            except (OSError, ValueError):
                # Spec vanished / corrupt under us; nothing safe to write.
                return
            if spec.is_terminal:
                # Same precedence rule as _record_finish: a terminal state
                # set by the watchdog or `vq kill` wins over the marker.
                # Stash the rc anyway -- it's useful forensic info.
                changed = False
                if spec.exit_code is None:
                    spec.exit_code = rc
                    changed = True
                # v0.12.0: crash feedback for watchdog-set terminals.
                if spec.state != JobState.COMPLETED and spec.failure_tail is None:
                    tail = _read_stderr_tail(Path(spec.cwd), spec.stderr_path)
                    if tail:
                        spec.failure_tail = tail
                        changed = True
                if changed:
                    spec.write(spec_path)
                log.info(
                    "orphan %s recovered rc=%d via %s (state preserved as %s)",
                    spec.id,
                    rc,
                    source,
                    spec.state,
                )
                # v0.5.35: notify even when the state was already terminal
                # (watchdog kill that the orphan-reaper is just confirming).
                fire_notify = True
            # v0.5.31: retry-on-failure. A non-zero rc on a not-yet-terminal
            # orphan is a retryable failure; if the job has budget left,
            # re-enqueue instead of going FAILED.
            elif rc != 0 and self._maybe_retry(
                spec,
                rc,
                jobid=admitted_jobid,
                spec_path=spec_path,
            ):
                return
            else:
                prev_state = spec.state
                spec.state = JobState.COMPLETED if rc == 0 else JobState.FAILED
                spec.exit_code = rc
                if spec.state == JobState.FAILED:
                    tail = _read_stderr_tail(Path(spec.cwd), spec.stderr_path)
                    if tail:
                        spec.failure_tail = tail
                spec.finished_at = utcnow_iso()
                spec.write(spec_path)
                log.info(
                    "orphan %s recovered rc=%d via %s -> %s",
                    spec.id,
                    rc,
                    source,
                    spec.state,
                )
                events.state_transition(
                    Path(spec.cwd),
                    spec.id,
                    from_state=prev_state.value,
                    to_state=spec.state.value,
                    reason=f"orphan exited; rc recovered from {source}",
                    exit_code=rc,
                )
                fire_notify = True
                spawn_rerun = True
        # --- lock released; side effects below must NOT hold it ---
        # v0.5.35: terminal transition → fire webhook (no-op if URL unset).
        if fire_notify:
            notify.send_terminal_notification(
                spec, self.notify_webhook_url,
                notify_on_states=self.notify_on_states,
            )
        if spawn_rerun:
            # v0.8.8 *Turing's Halt*: convergence-flag auto-resubmit
            # check. Runs BEFORE workdir cleanup so the rerun spawn can
            # still read the spec's workdir if --rerun-until points
            # into it. Cleanup runs after (idempotent — the rerun spawn
            # copies the cwd, not the workdir).
            self._maybe_spawn_rerun(spec)
        # Cleanup is tied to the observed process exit, not to whether this
        # reaper happened to author the terminal label. Watchdog/user-killed
        # orphans therefore clean up only after they are actually gone too.
        self._maybe_cleanup_workdir(
            spec,
            owner_uid=owner_uid,
            spec_path=spec_path,
        )

    def _maybe_spawn_rerun(self, spec: JobSpec) -> None:
        """v0.8.8 *Turing's Halt*: convergence-flag auto-resubmit.

        Called after a COMPLETED terminal transition. If the spec was
        submitted with ``--rerun-until PATH``, check the path; if
        the file is absent AND ``rerun_count < rerun_max``, spawn a
        fresh clone of the spec (rerun_count++, depends_on=[this
        jobid]) so the script gets another iteration.

        Conditions for spawn:

        * ``spec.rerun_until_file_exists`` is set.
        * ``spec.state == COMPLETED`` (FAILED jobs don't trigger
          reruns — failure is the wrong-signal, not "try again").
        * ``spec.rerun_count < spec.rerun_max``.
        * The path (with ``$VQ_WORKDIR`` substituted from the
          spec's actual workdir) does NOT exist.

        Best-effort: any failure (workspace copy fails, queue dir
        unwritable, ...) is logged at WARNING but doesn't propagate
        — the original spec's terminal record stands; the operator
        just doesn't get the auto-respawn.
        """
        if not spec.rerun_until_file_exists:
            return
        if spec.state != JobState.COMPLETED:
            return
        # Resolve $VQ_WORKDIR template against the spec's workdir.
        flag_path_str = spec.rerun_until_file_exists
        if spec.workdir:
            flag_path_str = flag_path_str.replace("$VQ_WORKDIR", spec.workdir)
        flag_path = Path(flag_path_str)
        if flag_path.exists():
            log.info(
                "job %s: rerun-until flag present (%s); chain done "
                "after %d iteration(s)",
                spec.id, flag_path, spec.rerun_count + 1,
            )
            return
        if spec.rerun_count >= spec.rerun_max:
            log.warning(
                "job %s: rerun-until flag MISSING (%s) but "
                "rerun_count=%d == rerun_max=%d; not respawning. "
                "Operator: bump --rerun-max and resubmit if more "
                "iterations are needed.",
                spec.id, flag_path,
                spec.rerun_count, spec.rerun_max,
            )
            return
        # Spawn a fresh clone.
        try:
            self._spawn_rerun_clone(spec)
        except Exception as e:  # noqa: BLE001 — best-effort
            log.warning(
                "job %s: rerun spawn failed (%s); operator can "
                "manually `vq resubmit %s` to continue.",
                spec.id, e, spec.id,
            )

    def _spawn_rerun_clone(self, spec: JobSpec) -> None:
        """Create a fresh JobSpec mirroring ``spec`` but with a new
        jobid, fresh workspace (copied from spec.cwd), incremented
        rerun_count, and ``depends_on=[spec.id]`` so it dispatches
        only after the predecessor has been recorded as COMPLETED.

        Workspace copy uses ``shutil.copytree`` — the same mechanism
        v0.6.8's ``resubmit_local`` uses. Keeps the rerun chain
        self-contained (each iteration has its own workspace +
        workdir, so the operator can inspect any iteration without
        them stomping each other).
        """
        from vq.submit import new_jobid as _new_jobid
        new_id = _new_jobid()
        # The owning user — single-user vs multi-user-per-user tree.
        if self._multi_user and spec.submitter:
            uid = spec.submitter  # numeric uid string
            new_queue_dir = paths.user_queue_dir(uid)
            new_jobs_dir = paths.user_jobs_dir(uid)
        else:
            new_queue_dir = self.queue_dir
            new_jobs_dir = self.jobs_dir
        new_workspace = new_jobs_dir / new_id
        # Fresh workspace: copy the source content from the original
        # spec's cwd. shutil.copytree creates the dst; it must not
        # already exist.
        shutil.copytree(spec.cwd, new_workspace)
        # Build the new spec.
        new_spec = JobSpec(
            id=new_id,
            command=list(spec.command),
            cwd=str(new_workspace.resolve()),
            cpus=spec.cpus,
            mem_mb=spec.mem_mb,
            wall_time_seconds=spec.wall_time_seconds,
            priority=spec.priority,
            recover_on_reboot=spec.recover_on_reboot,
            retry_max=spec.retry_max,
            job_name=spec.job_name,
            branch=spec.branch,
            program=spec.program,
            program_runtime_pin=(
                spec.program_runtime_pin.model_copy(
                    update={"resolved_git_sha": None},
                    deep=True,
                )
                if spec.program_runtime_pin is not None
                else None
            ),
            tags=list(spec.tags),
            depends_on=[spec.id],
            depends_on_any=list(spec.depends_on_any),
            # v0.8.8: preserve the rerun fields + bump the count.
            rerun_until_file_exists=spec.rerun_until_file_exists,
            rerun_max=spec.rerun_max,
            rerun_count=spec.rerun_count + 1,
            clean_workdir_on_terminal=spec.clean_workdir_on_terminal,
            submitter=spec.submitter,
            workspace_source=spec.workspace_source,
        )
        new_spec.write(new_queue_dir / f"{new_id}.json")
        log.info(
            "job %s: spawned rerun iteration %d/%d as %s "
            "(flag %s not present)",
            spec.id, spec.rerun_count + 1, spec.rerun_max,
            new_id, spec.rerun_until_file_exists,
        )

    def _record_workdir_cleanup_skip(
        self,
        spec: JobSpec,
        spec_path: Path,
        reason: str,
    ) -> None:
        """Persist a terminal, operator-visible cleanup refusal.

        Cleanup runs after the terminal-state write and outside its lock. Re-read
        under the per-spec lock so this diagnostic cannot restore a stale state
        or replace a failure reason written by another terminal actor.
        """
        diagnostic = f"workdir cleanup skipped: {reason}"
        log.warning("job %s: %s", spec.id, diagnostic)
        try:
            with paths.spec_lock(spec_path):
                fresh, _ = self._read_active_spec(
                    spec.id,
                    owner_uid=spec.submitter if self._multi_user else None,
                    spec_path=spec_path,
                )
                if not fresh.is_terminal:
                    log.warning(
                        "job %s: not persisting cleanup diagnostic because "
                        "the current state is %s",
                        spec.id,
                        fresh.state.value,
                    )
                    return
                if diagnostic in (fresh.failure_reason or ""):
                    return
                if fresh.failure_reason:
                    fresh.failure_reason = f"{fresh.failure_reason}; {diagnostic}"
                else:
                    fresh.failure_reason = diagnostic
                fresh.write(spec_path)
        except (OSError, ValueError) as exc:
            log.warning(
                "job %s: could not persist workdir cleanup diagnostic: %s",
                spec.id,
                exc,
            )

    def _maybe_cleanup_workdir(
        self,
        spec: JobSpec,
        *,
        owner_uid: str | None = None,
        spec_path: Path | None = None,
    ) -> None:
        """Opt-in immediate workdir cleanup at a terminal transition.

        Single-user mode retains the original mutable-spec path contract. In
        root multi-user mode the queued spec is user-writable, so the daemon
        derives the only permissible target from its trusted owner mapping and
        the job id. The recorded path is validation evidence only, never the
        deletion target. Refusals preserve the terminal state and are persisted
        for ``vq status`` rather than living only in the root daemon log.

        A missing directory is an idempotent success. Other cleanup failures do
        not propagate and therefore cannot undo terminal-state recording.
        """
        if not spec.clean_workdir_on_terminal:
            return
        # Scheduler jobs use remote scratch and do not receive a local workdir.
        # There is nothing to clean or diagnose in that valid case.
        if not spec.workdir:
            return
        if not self._multi_user:
            wd = Path(spec.workdir)
            try:
                if wd.exists():
                    shutil.rmtree(wd)
                    log.info(
                        "job %s: workdir cleaned "
                        "(clean_workdir_on_terminal): %s",
                        spec.id,
                        wd,
                    )
            except OSError as exc:
                log.warning(
                    "job %s: failed to clean workdir %s: %s",
                    spec.id,
                    wd,
                    exc,
                )
            return

        runtime = (
            self._running.get(spec.id)
            or self._scheduler_running.get(spec.id)
            or self._orphans.get(spec.id)
        )
        owner = owner_uid
        if owner is None and runtime is not None:
            owner = getattr(runtime, "owner_uid", None)
            if owner is None:
                owner = getattr(runtime, "uid", None)
        if owner is None:
            owner = self._job_uid.get(spec.id)
        if owner is None:
            # There is no safe path to either the workdir or spec without the
            # directory-derived owner. Log only; never fall back to submitter.
            log.warning(
                "job %s: workdir cleanup skipped: trusted owner is unavailable",
                spec.id,
            )
            return
        try:
            owner_uid = int(owner)
        except (TypeError, ValueError):
            log.warning(
                "job %s: workdir cleanup skipped: trusted owner is not numeric",
                spec.id,
            )
            return

        component = Path(spec.id)
        if (
            not spec.id
            or spec.id in {".", ".."}
            or component.is_absolute()
            or component.parts != (spec.id,)
        ):
            # An unsafe id cannot be joined to the trusted queue path safely
            # enough to persist a diagnostic. The multi-user admission gate
            # normally makes this unreachable, but deletion still fails closed.
            log.warning(
                "job %r: workdir cleanup skipped: job id is not one path component",
                spec.id,
            )
            return

        if spec_path is None and runtime is not None:
            spec_path = getattr(runtime, "spec_path", None)
        if spec_path is None:
            spec_path = paths.user_spec_path(owner, spec.id)
        workdir_root = paths.user_workdir_root(owner_uid)
        managed_workdir = paths.user_workdir(owner_uid, spec.id)
        if spec.workdir != str(managed_workdir):
            self._record_workdir_cleanup_skip(
                spec,
                spec_path,
                "recorded path does not match the managed owner/job path",
            )
            return

        open_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
        try:
            root_fd = os.open(workdir_root, open_flags)
        except FileNotFoundError:
            return
        except (OSError, ValueError) as exc:
            self._record_workdir_cleanup_skip(
                spec,
                spec_path,
                f"managed workdir root could not be opened safely: {exc}",
            )
            return
        try:
            try:
                entry = os.stat(spec.id, dir_fd=root_fd, follow_symlinks=False)
            except FileNotFoundError:
                return
            except (OSError, ValueError) as exc:
                self._record_workdir_cleanup_skip(
                    spec,
                    spec_path,
                    f"managed owner/job path metadata could not be read: {exc}",
                )
                return
            if stat.S_ISLNK(entry.st_mode):
                self._record_workdir_cleanup_skip(
                    spec,
                    spec_path,
                    "managed owner/job path is a symlink",
                )
                return
            if not stat.S_ISDIR(entry.st_mode):
                self._record_workdir_cleanup_skip(
                    spec,
                    spec_path,
                    "managed owner/job path is not a directory",
                )
                return
            try:
                shutil.rmtree(spec.id, dir_fd=root_fd)
            except (OSError, ValueError) as exc:
                self._record_workdir_cleanup_skip(
                    spec,
                    spec_path,
                    f"managed owner/job path could not be removed: {exc}",
                )
                return
        finally:
            with contextlib.suppress(OSError):
                os.close(root_fd)
        log.info(
            "job %s: workdir cleaned (clean_workdir_on_terminal): %s",
            spec.id,
            managed_workdir,
        )

    def _reconcile_running(self) -> None:
        finished: list[str] = []
        for jobid, rj in self._running.items():
            rc = self.dispatcher.poll(rj.popen)
            if rc is None:
                # STATE-3: still alive. If an external `vq kill` wrote a
                # terminal label but the process is ignoring the SIGTERM, the
                # job would otherwise sit here forever (popen.poll() never
                # returns), pinning its cpu/mem slot. Escalate to SIGKILL
                # once the grace elapses.
                self._escalate_if_killed(jobid, rj)
                continue
            self._record_finish(jobid, rc)
            rj.close_logs()
            finished.append(jobid)
        for jobid in finished:
            del self._running[jobid]

    def _escalate_if_killed(self, jobid: str, rj: _RunningJob) -> None:
        """STATE-3 (v0.8.14): force-reap a `vq kill`'d job whose process is
        ignoring SIGTERM.

        ``vq kill`` (kill.py) SIGTERMs the process group and writes a terminal
        label, but it runs in a separate process with no daemon IPC, so the
        daemon only learns of the kill by reading the spec. A SIGTERM-ignoring
        process then stays alive while its spec reads KILLED — and because
        ``popen.poll()`` never returns, the job never leaves ``_running`` and
        its cpu/mem claim is pinned forever. Once the spec is terminal and a
        grace period (mirroring the watchdog's SIGTERM->grace->SIGKILL) has
        elapsed, send SIGKILL; the next poll then reaps the dead process via
        ``_record_finish`` (which preserves the terminal label).

        This arms and fires only while ``popen`` itself survives the kill, and
        the ``vq.resource_receipt`` wrapper that leads a local job's group
        does not: it installs no handlers, so the killer's SIGTERM ends it at
        once and the daemon reaps it on the next pass. A command that ignores
        SIGTERM outlives it, and :meth:`_track_terminal_survivor` carries the
        escalation on from there — handing this method's deadline over when it
        had already armed one. What is left here is the narrower case of a
        ``popen`` that survives the SIGTERM: a job dispatched without the
        wrapper, and a wrapper that has not finished dying yet.
        """
        if rj.term_sigkilled:
            return  # already escalated; just waiting for popen.poll() to reap
        try:
            spec, _spec_path = self._read_active_spec(jobid, rj)
        except (OSError, ValueError):
            return
        if not spec.is_terminal:
            rj.term_deadline = None  # not killed; keep the clock un-armed
            return
        if spec.pgid is None:
            return
        now = time.monotonic()
        if rj.term_deadline is None:
            # First tick we've noticed the kill. The killer already sent
            # SIGTERM; give the process a grace window before SIGKILL. (The
            # job is in _running, so the daemon has been up continuously since
            # dispatch — "now" is within a poll interval of the actual kill.)
            rj.term_deadline = now + KILL_ESCALATION_GRACE_SECONDS
            return
        if now < rj.term_deadline:
            return
        log.warning(
            "job %s: spec is terminal (%s) but pgid %s is still alive ~%.0fs "
            "after kill; escalating to SIGKILL",
            jobid,
            spec.state.value,
            spec.pgid,
            KILL_ESCALATION_GRACE_SECONDS,
        )
        with contextlib.suppress(ProcessLookupError):
            killpg(spec.pgid, signal.SIGCONT)
        with contextlib.suppress(ProcessLookupError):
            killpg(spec.pgid, signal.SIGKILL)
        self._reap_scope(jobid)
        rj.term_sigkilled = True

    def _track_terminal_survivor(
        self, jobid: str, spec: JobSpec, rj: _RunningJob
    ) -> None:
        """Keep a killed job's still-live process group tracked once vq has
        reaped its wrapper, so the SIGTERM grace can still be escalated.

        Called from :meth:`_record_finish` for a local child whose spec is
        already terminal. The killer -- ``vq kill``, or the watchdog's
        OOM_KILLED / STARVED / TIME_EXCEEDED transition -- SIGTERMed the whole
        group; the wrapper leading it dies to that signal immediately, so the
        reap that brings us here says nothing about the command. When the
        group still answers, the job stays charged for its cpu and memory
        until :meth:`_reconcile_terminal_survivors` sees the group go or
        SIGKILLs what is left of it.

        Deliberately not an immediate SIGKILL: the grace the killer's SIGTERM
        opened is still running, and a command that is shutting down cleanly
        is entitled to the rest of it.

        A process group id is not reused while any member is alive, so a group
        answering here -- microseconds after the reap, as in
        :meth:`_reap_stranded_group` -- is still this job's. That identity
        holds only as long as the group does, so the reconcile pass drops the
        record the first time the group stops answering rather than waiting
        out the deadline against an id the kernel may by then have recycled.

        The converse, a group of nothing but unreaped zombies (they answer
        ``killpg(pgid, 0)``, and a container's PID 1 need not reap them),
        costs one grace window of reserved capacity and a SIGKILL that lands
        on no one. That is the bounded direction to be wrong in, and it is
        why nothing here waits for the group to go empty.
        """
        if rj.term_sigkilled:
            return  # _escalate_if_killed already fired against this group
        pgid = spec.pgid
        # Never the daemon's own group (cf. pause_job's self-pause guard).
        if pgid is None or pgid == os.getpgrp() or not _pgroup_alive(pgid):
            return
        deadline = (
            rj.term_deadline
            if rj.term_deadline is not None
            else time.monotonic() + KILL_ESCALATION_GRACE_SECONDS
        )
        self._terminal_survivors[jobid] = _TerminalSurvivor(
            pgid=pgid,
            cpus=rj.cpus,
            mem_mb=rj.mem_mb,
            state=spec.state,
            workspace=Path(spec.cwd),
            deadline=deadline,
            owner_uid=rj.owner_uid,
        )
        log.warning(
            "job %s: wrapper reaped on a terminal spec (%s) but pgid %s is "
            "still alive; holding its %d cpu(s) and escalating to SIGKILL in "
            "%.0fs if it does not exit first",
            jobid,
            spec.state.value,
            pgid,
            rj.cpus,
            max(0.0, deadline - time.monotonic()),
        )

    def _reconcile_terminal_survivors(self) -> None:
        """Release or SIGKILL each tracked survivor of a terminal job.

        The companion pass to :meth:`_track_terminal_survivor`, and the STATE-3
        escalation for every local job that runs under the wrapper. Exactly one
        pass ends a survivor's record: the group goes, or the grace expires and
        the group is SIGKILLed. Nothing here blocks on the group emptying --
        see :meth:`_track_terminal_survivor` on zombies -- so the SIGKILL is
        the last thing vq does for the job, and its capacity is released in
        the same pass that sends it.
        """
        for jobid, survivor in list(self._terminal_survivors.items()):
            if not _pgroup_alive(survivor.pgid):
                log.info(
                    "job %s: pgid %s exited within the kill grace; releasing "
                    "its %d cpu(s)",
                    jobid,
                    survivor.pgid,
                    survivor.cpus,
                )
                del self._terminal_survivors[jobid]
                continue
            if time.monotonic() < survivor.deadline:
                continue
            log.warning(
                "job %s: spec is terminal (%s) and its wrapper is reaped, but "
                "pgid %s is still alive ~%.0fs after the kill; escalating to "
                "SIGKILL",
                jobid,
                survivor.state.value,
                survivor.pgid,
                KILL_ESCALATION_GRACE_SECONDS,
            )
            with contextlib.suppress(ProcessLookupError):
                killpg(survivor.pgid, signal.SIGCONT)
            with contextlib.suppress(ProcessLookupError):
                killpg(survivor.pgid, signal.SIGKILL)
            self._reap_scope(jobid)
            events.append_event(
                survivor.workspace,
                events.EventKind.STATE_TRANSITION,
                jobid,
                **{
                    "from": survivor.state.value,
                    "to": survivor.state.value,
                    "reason": (
                        "killed job's process group outlived its reaped "
                        "wrapper; SIGKILLed after the kill grace"
                    ),
                },
            )
            del self._terminal_survivors[jobid]

    def _reap_scope(self, jobid: str) -> None:
        """Best-effort: stop a job's cgroup scope so descendants that
        escaped the process group (setsid / PR_SET_PGID children that
        ``killpg(2)`` cannot reach, but the scope's cgroup still holds)
        die with the rest of the job. No-op when cgroups are not in
        play, when ``--collect`` already removed the scope, or on macOS.
        ``cgroup.stop_scope`` is itself non-raising and idempotent."""
        if not (self.cgroup_enabled or self._multi_user):
            return
        cgroup.stop_scope(f"vq-job-{jobid}", multi_user=self._multi_user)

    def _reap_stranded_group(self, jobid: str, spec: JobSpec, rc: int) -> bool:
        """SIGKILL what is left of a local job's process group once its wrapper
        has been reaped without having waited for the command. Returns whether
        it signalled the group, so a terminal spec that still carried a pause
        intent is not then also tracked as a :class:`_TerminalSurvivor` owed a
        grace vq just spent.

        The ``vq.resource_receipt`` wrapper leads the job's group, forks the
        command into it and ``wait4``s for it before writing the exit marker
        and the resource receipt. Two ways of losing the wrapper strand the
        command where nothing else would end it: ``_reap_scope`` is a no-op
        without a cgroup scope, and a retry clears the pgid the STATE-2 restart
        sweep needs.

        * Paused. ``vq pause`` and the host-pressure pause SIGSTOP the whole
          group, so a wrapper SIGKILLed while stopped (a ``kill -9`` of the pid
          ``vq status`` shows, or an OOM kill) leaves the command stopped under
          init, holding its memory. This reap moves the spec out of SUSPENDED
          (to FAILED, or to PENDING for a retry), and ``resume_job`` refuses
          any other state. Keyed on the durable record, like STATE-2:
          SUSPENDED, or a pause intent a crashed pauser left behind.
        * Killed by a signal while running (``rc < 0`` on a nonterminal spec).
          The command's outcome can no longer be recorded, and the caller is
          about to release its cpu/mem slot and, with retries left, dispatch it
          again into the same workspace. Supervising the survivor as an orphan
          instead could only end it ABORTED_BY_QUEUE, since the marker's writer
          is gone, and would skip the retry.

        A wrapper that exits normally has waited for the command, so group
        members the command left in the background are not signalled. Nor is a
        terminal spec's group, beyond the pause case above: a SIGKILL here
        would cut short the grace the killer's SIGTERM opened, so
        :meth:`_track_terminal_survivor` carries that case on the killer's own
        clock instead.

        A group id is not reused while any member is alive, so a live group is
        still this job's; the leader a pid-fingerprint check would read is the
        process just reaped. SIGCONT first, as STATE-2 and STATE-3 do.
        """
        paused = spec.state == JobState.SUSPENDED or spec.pause_intent_at is not None
        if not paused and (rc >= 0 or spec.is_terminal):
            return False
        pgid = spec.pgid
        # Never the daemon's own group (cf. pause_job's self-pause guard).
        if pgid is None or pgid == os.getpgrp() or not _pgroup_alive(pgid):
            return False
        log.warning(
            "job %s: wrapper exited rc=%d while the job was %s, but pgid %s "
            "is still alive; SIGKILL-reaping the process group",
            jobid,
            rc,
            "paused" if paused else "running",
            pgid,
        )
        with contextlib.suppress(ProcessLookupError):
            killpg(pgid, signal.SIGCONT)
        with contextlib.suppress(ProcessLookupError):
            killpg(pgid, signal.SIGKILL)
        reason = (
            "paused job's wrapper exited; its stopped process group SIGKILLed"
            if paused
            else "wrapper killed by a signal; its surviving process group SIGKILLed"
        )
        events.append_event(
            Path(spec.cwd),
            events.EventKind.STATE_TRANSITION,
            jobid,
            **{"from": spec.state.value, "to": spec.state.value, "reason": reason},
        )
        return True

    def _record_finish(self, jobid: str, rc: int) -> None:
        # Tell the watchdog this job is done; drops its per-job state.
        # (Watchdog state is in-memory, unrelated to the spec file — do it
        # before taking the spec lock.)
        self.watchdog.unregister(jobid)
        runtime = self._running.get(jobid) or self._scheduler_running.get(jobid)
        spec_path = self._active_spec_path(jobid, runtime)
        owner_uid = getattr(runtime, "owner_uid", None) if runtime is not None else None
        # v0.8.11 *Dekker's Mutex*: serialize the read -> mutate -> write
        # against `vq kill`, the watchdog, and the depends_on cascade. The
        # lock is held ONLY across the spec read and write; the webhook
        # (network), the rerun spawn (workspace copy), and the workdir
        # cleanup (rmtree) run AFTER the lock releases — holding it across
        # them would block every other writer (and `vq status`) for their
        # whole duration. ``_maybe_retry`` writes the same spec and is called
        # *inside* the lock, so it must never take the lock itself (no nest).
        fire_notify = False
        spawn_rerun = False
        remove_scheduler_binding = False
        retried = False
        with paths.spec_lock(spec_path):
            spec, _ = self._read_active_spec(
                jobid,
                runtime,
                owner_uid=owner_uid,
                spec_path=spec_path,
            )
            workspace = Path(spec.cwd)
            # Signal before the branches, under the lock as pause_job does: a
            # retry clears ``spec.pgid``.
            stranded_group_reaped = False
            if isinstance(runtime, _RunningJob):
                stranded_group_reaped = self._reap_stranded_group(jobid, spec, rc)
            if spec.is_terminal:
                # External actor (e.g. `vq kill`) or the watchdog already wrote
                # a terminal state; don't overwrite it with COMPLETED/FAILED.
                # Just record the exit code so users can see what actually
                # happened, then move on.
                changed = False
                if spec.exit_code is None:
                    spec.exit_code = rc
                    changed = True
                # v0.12.0: crash feedback for watchdog-set terminals
                # (OOM_KILLED / STARVED / TIME_EXCEEDED, or a kill while
                # running). The state names the failure class; the tail shows
                # what the job printed before it was stopped.
                if spec.state != JobState.COMPLETED and spec.failure_tail is None:
                    tail = _read_stderr_tail(workspace, spec.stderr_path)
                    if tail:
                        spec.failure_tail = tail
                        changed = True
                if (
                    self._multi_user
                    and isinstance(runtime, _SchedulerJob)
                    and owner_uid is not None
                ):
                    try:
                        _close_scheduler_submit_binding(owner_uid, spec)
                    except (OSError, ValueError):
                        log.exception(
                            "job %s: could not durably close scheduler authority "
                            "before recording its terminal reap",
                            jobid,
                        )
                        return
                    remove_scheduler_binding = True
                if changed:
                    spec.write(spec_path)
                # The killer SIGTERMed the whole group and the wrapper leading
                # it dies to that at once, so this reap does not mean the
                # command is gone. Keep the job charged for its resources
                # while its group outlives the wrapper, and let the STATE-3
                # escalation reach it there.
                if isinstance(runtime, _RunningJob) and not stranded_group_reaped:
                    self._track_terminal_survivor(jobid, spec, runtime)
                log.info("job %s reaped rc=%d (state preserved as %s)", jobid, rc, spec.state)
                events.append_event(
                    workspace,
                    events.EventKind.STATE_TRANSITION,
                    jobid,
                    **{
                        "from": spec.state.value,
                        "to": spec.state.value,
                        "reason": "reaped (terminal state already set)",
                        "exit_code": rc,
                    },
                )
                # v0.5.35: the watchdog-kill / vq-kill case lands here AFTER
                # the killer wrote the terminal state. This is where the
                # notification fires for OOM_KILLED / STARVED /
                # TIME_EXCEEDED / KILLED (when killed-while-RUNNING) — the
                # killer doesn't fire the notification itself; this single
                # daemon-side hook covers every "process actually exited"
                # path uniformly. Fired below, after the lock releases.
                fire_notify = True
            # v0.5.31: retry-on-failure. A non-zero exit on a not-yet-terminal
            # spec is the only retryable failure; if the job has budget left,
            # re-enqueue instead of going FAILED. ``_maybe_retry`` writes
            # under the lock we already hold.
            elif rc != 0 and self._maybe_retry(
                spec,
                rc,
                jobid=jobid,
                spec_path=spec_path,
                scheduler_retry_owner_uid=(
                    owner_uid
                    if self._multi_user and isinstance(runtime, _SchedulerJob)
                    else None
                ),
            ):
                retried = True
            else:
                prev_state = spec.state
                spec.state = JobState.COMPLETED if rc == 0 else JobState.FAILED
                spec.exit_code = rc
                if spec.state == JobState.FAILED:
                    tail = _read_stderr_tail(workspace, spec.stderr_path)
                    if tail:
                        spec.failure_tail = tail
                spec.finished_at = utcnow_iso()
                if (
                    self._multi_user
                    and isinstance(runtime, _SchedulerJob)
                    and owner_uid is not None
                ):
                    try:
                        _close_scheduler_submit_binding(owner_uid, spec)
                    except (OSError, ValueError):
                        log.exception(
                            "job %s: could not durably close scheduler authority "
                            "before recording terminal state",
                            jobid,
                        )
                        return
                    remove_scheduler_binding = True
                spec.write(spec_path)
                log.info("job %s finished rc=%d -> %s", jobid, rc, spec.state)
                events.state_transition(
                    workspace,
                    jobid,
                    from_state=prev_state.value,
                    to_state=spec.state.value,
                    exit_code=rc,
                )
                fire_notify = True
                spawn_rerun = True
        # --- lock released; the side effects below must NOT hold it ---
        if retried:
            # The attempt is over even though the job is not. Stop its scope as
            # the terminal path below does, so processes that escaped the group
            # do not run on through the backoff outside the capacity count;
            # before, only the retry's own dispatch stopped a leftover scope.
            if isinstance(runtime, _RunningJob):
                self._reap_scope(jobid)
            return
        if remove_scheduler_binding and owner_uid is not None:
            try:
                _remove_scheduler_submit_binding(owner_uid, jobid)
            except (OSError, ValueError):
                log.exception(
                    "job %s: failed to remove closed scheduler submit binding",
                    jobid,
                )
        # The job's process has exited. Reap its transient cgroup scope so
        # descendants that escaped the process group (and thus outlived the
        # dispatcher's wait) are killed and the unit is collected, instead of
        # lingering with memory unaccounted to any job. Idempotent + a no-op
        # without cgroups or when --collect already removed the scope.
        self._reap_scope(jobid)
        # v0.12.x fix 2: a build job reaching a terminal state drives the
        # env's build-failure backoff. COMPLETED clears it; any non-
        # COMPLETED terminal (FAILED exit-1, atomic-build rollback, or a
        # watchdog TIME_EXCEEDED/OOM/STARVED kill of a wedged build) records
        # a failure so the next auto-update backs off instead of immediately
        # re-submitting the same wedging build. Placed here — the single
        # daemon-side reap hook — so the watchdog-SIGKILL path (which never
        # reaches `vq build-env`'s own exit handler) is covered too.
        if spec.build_env is not None and spec.is_terminal:
            if spec.state == JobState.COMPLETED:
                build_job.clear_build_backoff(spec.build_env)
            else:
                build_job.record_build_failure(spec.build_env)
        # v0.5.35: COMPLETED / FAILED / preserved-terminal → fire the webhook.
        # ``send_terminal_notification`` is a no-op when ``notify_webhook_url``
        # is None (the default).
        if fire_notify:
            notify.send_terminal_notification(
                spec, self.notify_webhook_url,
                notify_on_states=self.notify_on_states,
            )
        if spawn_rerun:
            # v0.8.8 *Turing's Halt*: convergence-flag auto-resubmit
            # check. Runs BEFORE workdir cleanup so the rerun spawn can
            # still read the spec's workdir if --rerun-until points
            # into it. Cleanup runs after (idempotent — the rerun spawn
            # copies the cwd, not the workdir).
            self._maybe_spawn_rerun(spec)
        # A terminal label may have been written before the child exited. Run
        # cleanup here after the actual reap regardless of who set that label.
        self._maybe_cleanup_workdir(
            spec,
            owner_uid=owner_uid,
            spec_path=spec_path,
        )

    def _maybe_retry(
        self,
        spec: JobSpec,
        rc: int,
        *,
        jobid: str | None = None,
        spec_path: Path | None = None,
        scheduler_retry_owner_uid: str | None = None,
    ) -> bool:
        """v0.5.31: retry-on-failure. If ``spec`` was submitted with
        ``--retry N`` and hasn't spent its budget, re-enqueue it
        (state -> PENDING, retry_count++, not_before = now + exponential
        backoff) instead of letting the caller mark it FAILED. Returns
        True if it re-enqueued, False if the job should proceed to FAILED.

        Caller contract: only invoke this for a non-zero ``rc`` on a
        spec that is NOT already terminal. Watchdog kills (OOM_KILLED /
        STARVED / TIME_EXCEEDED) and ``vq kill`` (KILLED) are caught by
        the ``is_terminal`` precedence check upstream of both call
        sites, so they never reach here — only a genuine non-zero
        command exit does.

        v0.8.11 *Dekker's Mutex* lock contract: this writes the spec
        (state -> PENDING) and is called ONLY from ``_record_finish`` /
        ``_record_orphan_finish``, each of which already holds
        ``paths.spec_lock(spec_path)`` across the read -> mutate -> write.
        It must therefore NOT take the spec lock itself — ``flock`` on a
        second fd for the same path from the same process would deadlock
        (the lock never nests, by design). If you add a new caller, that
        caller must hold the lock around this call.
        """
        if spec.retry_max <= 0:
            return False
        if spec.retry_count >= spec.retry_max:
            log.info(
                "job %s exhausted retry budget (%d/%d) -> FAILED",
                spec.id,
                spec.retry_count,
                spec.retry_max,
            )
            return False
        jobid = jobid or spec.id
        spec_path = spec_path or self._spec_path(jobid)
        try:
            ensure_idempotency_claim_for_spec(spec_path.parent, spec)
        except (OSError, ValueError) as exc:
            # Do not erase the spec-first repair evidence or requeue the
            # execution when its original queue-acceptance tombstone could not
            # be made durable. The caller will land the current attempt in its
            # ordinary terminal failure state; a later keyed client can still
            # repair the claim by scanning this bound spec.
            log.error(
                "job %s: cannot durably repair submit idempotency claim "
                "before retry; leaving the attempt terminal: %s",
                spec.id,
                exc,
            )
            return False
        spec.retry_count += 1
        backoff = _retry_backoff_seconds(spec.retry_count)
        spec.not_before = (datetime.now(UTC) + timedelta(seconds=backoff)).isoformat()
        spec.state = JobState.PENDING
        # Clear the run-instance fields so the next dispatch starts clean.
        # The workspace (and its events.jsonl / stdout.log) is reused —
        # _start_job opens the logs in append mode and unlinks any stale
        # exit marker, so a retry continues the same workspace's history.
        spec.pid = None
        spec.pgid = None
        spec.started_at = None
        spec.exit_code = None
        spec.scheduler_job_id = None
        if spec.program_runtime_pin is not None:
            spec.program_runtime_pin.resolved_git_sha = None
        # A retry is a new execution attempt, not another queue-authority
        # acceptance of the original keyed submit.  The durable claim remains
        # a tombstone for that original job ID, while the retried spec cannot
        # be rediscovered as a crash-gap claim candidate.
        spec.idempotency_key_hash = None
        spec.submission_intent_digest = None
        spec.submission_owner_hash = None
        spec.id = jobid
        # A proven-terminal scheduler attempt must be closed in daemon-owned
        # authority before exposing its reusable PENDING row.  The next phase
        # one rotates this tombstone into a fresh open attempt; ordinary same-id
        # reuse is rejected and can never inherit the old acceptance id.
        if scheduler_retry_owner_uid is not None:
            try:
                _replace_scheduler_submit_binding_spec(
                    scheduler_retry_owner_uid,
                    spec,
                    scheduler_job_id=None,
                    allow_clear_bound_id=True,
                    transaction_state="closed",
                )
            except (OSError, ValueError) as exc:
                log.error(
                    "job %s: cannot durably close scheduler attempt before "
                    "retry; leaving the attempt terminal: %s",
                    jobid,
                    exc,
                )
                return False
        spec.write(spec_path)
        log.info(
            "job %s failed rc=%d -> re-enqueued (retry %d/%d, ~%ds backoff, not_before %s)",
            jobid,
            rc,
            spec.retry_count,
            spec.retry_max,
            backoff,
            spec.not_before,
        )
        events.append_event(
            Path(spec.cwd),
            events.EventKind.STATE_TRANSITION,
            jobid,
            **{
                "from": JobState.RUNNING.value,
                "to": JobState.PENDING.value,
                "reason": (
                    f"retry {spec.retry_count}/{spec.retry_max} after rc={rc}; {backoff}s backoff"
                ),
                "exit_code": rc,
            },
        )
        return True

    def _reseed_host_pressure_pauses_at_startup(self) -> None:
        """HP-1 (v0.8.24): re-arm the watchdog's host-pressure resume tracking
        after a daemon restart.

        The watchdog auto-pauses running jobs under host memory pressure and
        SIGCONTs them once pressure recedes — but the record of WHICH jobs it
        paused lives only in the (now-dead) process. A restart therefore
        strands those jobs SUSPENDED forever: the resume tick only targets
        jobs it remembers pausing, and a fresh watchdog remembers none. Scan
        for SUSPENDED specs tagged ``HOST_PRESSURE_PAUSE_TAG`` and re-seed the
        watchdog so the next pressure-recedes tick resumes them.

        Operator-paused jobs (any other ``paused_by``, including None) are
        deliberately NOT re-seeded — the operator paused them on purpose; only
        a ``vq resume`` should bring them back.

        Skipped when host-pressure enforcement is off: ``check_host_pressure``
        would then never fire RESUME, so re-seeding ``active=True`` would gate
        dispatch (HP-2) forever. Instead we log the orphaned tagged jobs so the
        operator can resume them by hand.
        """
        tagged = [
            spec.id
            for spec in self._iter_specs()
            if spec.state == JobState.SUSPENDED
            and spec.paused_by == HOST_PRESSURE_PAUSE_TAG
        ]
        if not tagged:
            return
        if not self.watchdog.enforce_host_pressure_pause:
            log.warning(
                "host-pressure: %d job(s) are SUSPENDED with the '%s' tag but "
                "host-pressure enforcement is OFF; they will NOT auto-resume "
                "— `vq resume` them by hand: %s",
                len(tagged), HOST_PRESSURE_PAUSE_TAG, tagged,
            )
            return
        self.watchdog.reseed_host_pressure_paused(tagged)
        log.info(
            "host-pressure: re-seeded %d auto-paused job(s) at startup; they "
            "resume when pressure recedes below %.1f%%: %s",
            len(tagged), self.watchdog.host_pressure_resume_pct, tagged,
        )

    def _host_pressure_pass(self) -> None:
        """v0.6.20: pause running jobs when host memory pressure crosses
        ``host_pressure_pause_pct``; resume them once pressure recedes
        below ``host_pressure_resume_pct``.

        Why this matters (2026-05-18 workstation wedge post-mortem):
        the box ran a vq job + Steam + Nextcloud + GNOME shell
        concurrently; combined RAM crossed the 125 GB cliff; kernel
        OOM-killer cascaded through vq-daemon itself. Per-job cgroup
        MemoryMax was correct but didn't see the AGGREGATE pressure.
        This pass watches global pressure and reacts at 85% — before
        the kernel does — by SIGSTOPping running jobs to freeze
        their footprint and prefer non-vq cgroups for OOM-kill if it
        still happens.

        Best-effort: any failure (pressure unreadable, pause_job
        errors, etc.) logs but doesn't propagate. The daemon main
        loop must not die because of a partial pressure intervention.
        """
        try:
            # HP-3 (v0.8.24): the auto-pause candidate set is every job the
            # daemon tracks as alive — in-process (_running) AND reattached
            # orphans (_orphans, still alive from a previous daemon). Pre-fix
            # the list was _running only, so a reattached orphan was immune to
            # the host-pressure pause and kept loading a pressured host across
            # a daemon restart. We only auto-pause jobs whose spec.state is
            # RUNNING (not already SUSPENDED / in a transition); reading the
            # spec each tick is cheap on the dispatcher's idle path.
            candidate_jobids = [*self._running.keys(), *self._orphans.keys()]
            current_running: list[str] = []
            for jobid in candidate_jobids:
                runtime = self._running.get(jobid) or self._orphans.get(jobid)
                try:
                    spec, _ = self._read_active_spec(jobid, runtime)
                except Exception:
                    continue
                if spec.state == JobState.RUNNING:
                    current_running.append(jobid)

            # #563: resolve the pressure probe through the module at call
            # time. ``check_host_pressure``'s default argument binds
            # ``read_host_memory_pressure_pct`` when watchdog.py is imported,
            # so a test that pins ``vq.watchdog.read_host_memory_pressure_pct``
            # never reached this call and the daemon tests read the REAL
            # host: the v0.15.158 release gate went red at 85 % CI-host memory
            # in a test about marker scoping (pipeline 4998, job 10057).
            verdict = self.watchdog.check_host_pressure(
                current_running,
                _pressure_reader=_vq_watchdog.read_host_memory_pressure_pct,
            )

            if verdict.action == HostPressureAction.NO_OP:
                return

            if verdict.action == HostPressureAction.PAUSE:
                log.warning("host-pressure pause: %s", verdict.reason)
                for jid in verdict.jobids:
                    try:
                        # Import inline to avoid a top-level cycle
                        # (pause_resume imports nothing of daemon).
                        # v0.6.23: tag the pause as 'watchdog_host_pressure'
                        # so operators inspecting `vq status` see WHY a job
                        # is paused. Also keeps `vq resume --paused-by foo`
                        # (the v0.6.22 operator-tagged variant) from
                        # accidentally claiming watchdog pauses.
                        from vq.pause_resume import pause_job

                        pause_job(
                            "localhost",
                            jid,
                            paused_by=HOST_PRESSURE_PAUSE_TAG,
                            # v0.6.38: in multi-user mode the spec
                            # lives under /var/lib/vq/users/<uid>/ —
                            # without this the host-pressure pause
                            # silently no-ops (FileNotFoundError).
                            multi_user=self._multi_user,
                        )
                    except Exception as e:  # noqa: BLE001 — best-effort
                        log.error(
                            "host-pressure pause: failed to pause %s: %s",
                            jid,
                            e,
                        )
                return

            if verdict.action == HostPressureAction.RESUME:
                log.info("host-pressure resume: %s", verdict.reason)
                for jid in verdict.jobids:
                    try:
                        from vq.pause_resume import resume_job

                        resume_job(
                            "localhost", jid,
                            multi_user=self._multi_user,
                        )
                    except Exception as e:  # noqa: BLE001 — best-effort
                        log.error(
                            "host-pressure resume: failed to resume %s: %s",
                            jid,
                            e,
                        )
        except Exception:  # noqa: BLE001 — must not crash the dispatch loop
            log.exception("host-pressure pass crashed; dispatch loop continues")

    def _watchdog_pass(self) -> None:
        """Sample each running job (in-process and orphaned); act on verdicts.

        Called every iterate() pass. The watchdog itself rate-limits actual
        sampling to its own ``interval_seconds``, so this is cheap to call
        on every poll.
        """
        # In-process jobs first (popen available), then orphans (pgid only).
        # Bookkeeping: popen=None signals orphan in the kill paths below.
        targets: list[tuple[str, int, _RunningJob | None]] = []
        for jobid, rj in list(self._running.items()):
            targets.append((jobid, rj.popen.pid, rj))
        for jobid, orphan in list(self._orphans.items()):
            # We don't know the pid for orphans (it may have changed if the
            # original leader died and a child took over). Use pgid for /proc
            # sampling -- the pgid leader's pid in /proc/<pgid>/status is a
            # valid RSS sample for the group's primary process.
            targets.append((jobid, orphan.pgid, None))

        for jobid, sample_pid, rj in targets:
            runtime = rj or self._orphans.get(jobid)
            try:
                spec, spec_path = self._read_active_spec(jobid, runtime)
            except Exception:
                log.exception("watchdog: cannot read spec for %s; skipping", jobid)
                continue
            verdict = self.watchdog.evaluate(
                jobid=jobid,
                pid=sample_pid,
                pgid=spec.pgid,
                spec=spec,
                cgroup_unit_name=f"vq-job-{jobid}",
                cgroup_multi_user=self._multi_user,
            )
            if verdict.action == WatchdogAction.OK:
                continue
            if verdict.terminal_state is None:
                # Defensive: SIGTERM/SIGKILL verdicts must carry a state.
                log.error(
                    "watchdog: verdict %s for %s missing terminal_state", verdict.action, jobid
                )
                continue
            workspace = Path(spec.cwd)
            if verdict.action == WatchdogAction.SIGTERM:
                log.warning(
                    "watchdog: SIGTERM job %s (pgid=%s): %s",
                    jobid,
                    spec.pgid,
                    verdict.reason,
                )
                # Mark the spec to the terminal state immediately so a
                # racing `vq status` shows the watchdog kill, not RUNNING.
                # _record_finish later sees is_terminal and preserves it.
                #
                # CONC-3 (v0.8.9): the spec read at the top of this loop may
                # be stale — `vq kill` / host-pressure / a cascade can write a
                # terminal label from another process in between. Re-read
                # right before the write so the watchdog doesn't clobber (and
                # emit a contradictory event for) a label someone else set.
                # If it's already terminal, preserve that label and skip the
                # duplicate transition — but STILL deliver the SIGTERM below:
                # the watchdog's job is to make the misbehaving process die,
                # regardless of who recorded the terminal state.
                # v0.8.11 *Dekker's Mutex*: lock the re-read -> write so the
                # CONC-3 re-read above is atomic against the racing writer
                # (`vq kill` / cascade / host-pressure). The SIGTERM itself is
                # delivered after the lock releases — signalling doesn't touch
                # the spec file and must not block other writers.
                with paths.spec_lock(spec_path):
                    try:
                        fresh, _ = self._read_active_spec(
                            jobid,
                            runtime,
                            spec_path=spec_path,
                        )
                    except (OSError, ValueError):
                        fresh = None
                    if fresh is not None and not fresh.is_terminal:
                        prev_state = fresh.state
                        fresh.state = verdict.terminal_state
                        fresh.finished_at = utcnow_iso()
                        fresh.write(spec_path)
                        events.append_event(
                            workspace,
                            events.EventKind.WATCHDOG_KILL,
                            jobid,
                            signal="SIGTERM",
                            reason=verdict.reason,
                            target_state=verdict.terminal_state.value,
                        )
                        events.state_transition(
                            workspace,
                            jobid,
                            from_state=prev_state.value,
                            to_state=fresh.state.value,
                            reason=f"watchdog: {verdict.reason}",
                        )
                    elif fresh is not None:
                        log.info(
                            "watchdog: job %s already terminal (%s) before SIGTERM "
                            "mark; preserving label, still signalling pgid",
                            jobid,
                            fresh.state.value,
                        )
                if spec.pgid is not None:
                    with contextlib.suppress(ProcessLookupError):
                        killpg(spec.pgid, signal.SIGCONT)
                    killpg(spec.pgid, signal.SIGTERM)
                elif rj is not None:
                    self.dispatcher.terminate(rj.popen)
                # else: orphan with no pgid recorded -- nothing we can kill.
            elif verdict.action == WatchdogAction.SIGKILL:
                log.warning(
                    "watchdog: SIGKILL job %s (pgid=%s) after grace: %s",
                    jobid,
                    spec.pgid,
                    verdict.reason,
                )
                events.append_event(
                    workspace,
                    events.EventKind.WATCHDOG_KILL,
                    jobid,
                    signal="SIGKILL",
                    reason=verdict.reason,
                    target_state=spec.state.value,
                )
                if spec.pgid is not None:
                    with contextlib.suppress(ProcessLookupError):
                        killpg(spec.pgid, signal.SIGCONT)
                    killpg(spec.pgid, signal.SIGKILL)
                elif rj is not None:
                    self.dispatcher.kill(rj.popen)
                self._reap_scope(jobid)

    def _poll_admin_update_marker(self) -> bool:
        """v0.5.45 / v0.11.0: per-tick check for the
        admin-update-in-progress marker. Returns True when a **live**
        update marker is present (caller should skip new dispatches);
        False when there's no marker or the marker is a **stale corpse**
        (its writing `vq admin update` is gone), which is reaped here.

        v0.11.0: pre-this, ANY marker on disk paused dispatch forever —
        a marker left by a killed update, or one that outlived a reboot
        (it persists in the state dir), silently parked every job at
        `pending` while the host reported plain `up`/`OK`. compute-b and
        build-host both wedged this way on 2026-06-18. Now a stale marker is
        auto-reaped (deleted, with a loud warning) and dispatch resumes;
        only a genuinely in-flight update — pid alive, young, incl. the
        daemon-restart window where the update process survives the
        restart — still holds the queue. See
        :func:`admin.admin_update_marker_stale_reason`.

        Logs once per transition (live marker appears / clears) so the
        operator sees a clean blocked/unblocked pair rather than per-tick
        spam; a reap always logs loudly (it's a one-shot — the file is
        gone afterward). State is kept in
        :attr:`_admin_update_marker_present` across ticks."""
        markers = admin.read_admin_update_markers()
        present = bool(markers) or admin.admin_update_marker_exists()
        if not present:
            if self._admin_update_marker_present:
                log.info(
                    "admin-update-in-progress marker cleared — "
                    "resuming new dispatches.",
                )
            self._admin_update_marker_present = False
            self._logged_stale_managed_marker_ids.clear()
            return False

        # Marker present. A parseable marker whose writer is gone is a
        # stale corpse → reap it and let dispatch proceed. An unreadable
        # marker (corrupt JSON) can't be staleness-checked, so we stay
        # conservative and keep pausing on it (rare — the write is
        # atomic via os.replace; an operator clears it by hand).
        for marker in markers:
            stale_reason = admin.admin_update_marker_stale_reason(marker)
            if stale_reason is not None:
                if marker.managed_transaction is not None or marker.owns_pause_scope:
                    receipt_id = marker.marker_id or f"legacy:{marker.pid}"
                    if receipt_id not in self._logged_stale_managed_marker_ids:
                        log.error(
                            "STALE durable admin-update receipt retained — %s. "
                            "Dispatch remains scoped by the marker; recover exact "
                            "files/service with `vq admin recover-update`, or for a "
                            "pause-only receipt inspect the env then use "
                            "`vq admin clear-update-marker` (envs=%s, "
                            "host=%s, marker_id=%s).",
                            stale_reason,
                            marker.envs,
                            marker.host,
                            marker.marker_id or "legacy",
                        )
                        self._logged_stale_managed_marker_ids.add(receipt_id)
                else:
                    self._reap_stale_admin_update_marker(marker, stale_reason)
        markers = admin.read_admin_update_markers()
        live_ids = {
            marker.marker_id or f"legacy:{marker.pid}"
            for marker in markers
            if marker.managed_transaction is not None or marker.owns_pause_scope
        }
        self._logged_stale_managed_marker_ids.intersection_update(live_ids)
        if not admin.admin_update_marker_exists():
            self._admin_update_marker_present = False
            self._admin_update_hold_scope = frozenset()
            return False

        scope = admin.admin_update_markers_scope()
        if not self._admin_update_marker_present:
            if markers:
                log.info(
                    "admin-update-in-progress marker detected — %s active "
                    "lease(s); holding new dispatches for %s (scopes=%s); "
                    "running jobs "
                    "unaffected. "
                    "A stale ordinary marker is auto-reaped; a durable managed "
                    "receipt is retained for `vq admin recover-update`.",
                    len(markers),
                    "ALL targets" if scope is None else sorted(scope),
                    [
                        {"envs": marker.envs, "host": marker.host}
                        for marker in markers
                    ],
                )
            else:
                log.info(
                    "admin-update-in-progress marker detected but "
                    "unreadable (JSON parse failed) — pausing new "
                    "dispatches; running jobs unaffected. Clear by hand "
                    "via `vq admin clear-update-marker` if it persists.",
                )
        self._admin_update_marker_present = True
        self._admin_update_hold_scope = scope
        return True

    def _admin_update_holds_spec(self, spec: JobSpec) -> bool:
        """Re-attest the dispatch hold at the final durable RUNNING claim.

        The earlier pending sweep is only an optimization.  Marker admission
        can race the comparatively long workspace/cgroup setup, so the spec
        lock protecting PENDING -> RUNNING must repeat this check.  Whichever
        side acquires the spec lock first then has a complete outcome: either
        dispatch is held, or the updater's pause proof observes RUNNING and
        stops the child before any environment mutation.
        """
        if not self._poll_admin_update_marker():
            return False
        scope = self._admin_update_hold_scope
        if scope is None:
            return True
        if spec.scheduler_target is not None:
            return spec.scheduler_target in scope
        return admin.LOCAL_DISPATCH_SCOPE in scope

    def _reap_stale_admin_update_marker(
        self, marker: admin.AdminUpdateMarker | None, reason: str
    ) -> None:
        """v0.11.0: delete a stale admin-update marker and log loudly.

        Called from :meth:`_poll_admin_update_marker` (and the startup
        recovery pass) when the marker's writing process is provably
        gone. Deleting it does what an operator would have done by hand
        (`vq admin clear-update-marker`): it un-gates dispatch AND stops
        the corpse from refusing the next `vq admin update`. The loud
        warning + the (now-removed) marker's fields are the durable
        forensic record that an update died mid-flight — if the env's
        venv may be half-built, the operator re-runs
        `vq admin update <env> --force` after verifying it."""
        cleared = admin.clear_admin_update_marker(marker)
        m = cleared if cleared is not None else marker
        log.warning(
            "STALE admin-update-in-progress marker reaped — it was "
            "silently gating new dispatch but %s. Auto-cleared; resuming "
            "dispatch. If a `vq admin update` was interrupted the env's "
            "venv may be half-built — verify with `vq admin status` and "
            "re-run `vq admin update <env> --force` if needed. "
            "(envs=%s, host=%s, started=%s, pid=%s, vq_version=%s)",
            reason,
            m.envs if m else "?",
            m.host if m else "?",
            m.started_at if m else "?",
            m.pid if m else "?",
            m.vq_version if m else "?",
        )

    def _eff_mem_mb(self, mem_mb: int | None) -> int | None:
        """Effective memory footprint for the admission budget + the cgroup
        cap: the job's declared ``mem_mb``, or the daemon's
        ``default_job_mem_mb`` when the job declared nothing. Returns
        ``None`` only when neither is set (the legacy unbounded-undeclared
        mode), in which case the memory gate is skipped for that job."""
        return mem_mb if mem_mb is not None else self.default_job_mem_mb

    def _fail_impossible_spec(self, spec: JobSpec, why: str) -> None:
        """Terminal-fail a PENDING spec whose request can never fit this
        daemon's configured caps. Same locked re-read -> write shape as the
        depends_on cascade-fail so a racing `vq kill` cannot be clobbered."""
        jobid = spec.id
        spec_path = self._active_spec_path(jobid)
        with paths.spec_lock(spec_path):
            try:
                fresh, _ = self._read_active_spec(jobid, spec_path=spec_path)
            except (OSError, ValueError):
                return
            if fresh.state != JobState.PENDING:
                return
            fresh.state = JobState.FAILED
            fresh.finished_at = utcnow_iso()
            fresh.failure_reason = (
                f"impossible resource request: {why}. Resubmit with a "
                "request within the daemon's caps."
            )
            try:
                fresh.write(spec_path)
            except OSError as e:
                log.warning(
                    "impossible-spec fail: could not write FAILED for %s: %s",
                    jobid,
                    e,
                )
                return
            events.state_transition(
                Path(fresh.cwd),
                jobid,
                from_state=JobState.PENDING.value,
                to_state=JobState.FAILED.value,
                reason=fresh.failure_reason,
            )
        spec.state = JobState.FAILED
        log.warning("impossible resource request: %s -> FAILED (%s)",
                    spec.id, why)
        notify.send_terminal_notification(
            fresh, self.notify_webhook_url,
            notify_on_states=self.notify_on_states,
        )

    def _maybe_cascade_dependency_failure(
        self,
        spec: JobSpec,
        specs_by_id: dict[str, JobSpec],
    ) -> None:
        """Cascade one pending spec when a required predecessor failed."""
        if spec.state != JobState.PENDING or not spec.depends_on:
            # v0.7.8: ``depends_on_any`` never cascade-fails (its
            # whole point is "run regardless of predecessor
            # outcome"), so the cascade check scans only
            # ``depends_on``. A spec with empty ``depends_on`` is
            # unaffected even if ``depends_on_any`` is non-empty.
            return

        failed_pred_id: str | None = None
        failed_pred_state: JobState | None = None
        for pred_id in spec.depends_on:
            pred = specs_by_id.get(pred_id)
            if pred is None:
                continue  # missing -> treat as still-waiting
            if pred.state in TERMINAL_STATES and pred.state != JobState.COMPLETED:
                failed_pred_id = pred_id
                failed_pred_state = pred.state
                break
        if failed_pred_id is None:
            return

        # Cascade-fail. Re-read just before mutating to dodge a race against
        # `vq kill` / a manual edit between the directory scan and now.
        # v0.8.11 *Dekker's Mutex*: lock the re-read -> write so the PENDING
        # re-check and FAILED write are atomic against a racing `vq kill`.
        jobid = spec.id
        spec_path = self._active_spec_path(jobid)
        with paths.spec_lock(spec_path):
            try:
                fresh, _ = self._read_active_spec(jobid, spec_path=spec_path)
            except (OSError, ValueError):
                return
            if fresh.state != JobState.PENDING:
                return
            fresh.state = JobState.FAILED
            fresh.finished_at = utcnow_iso()
            fresh.failure_reason = (
                f"predecessor {failed_pred_id} failed "
                f"(state={failed_pred_state.value if failed_pred_state else '?'})"
            )
            try:
                fresh.write(spec_path)
            except OSError as e:
                log.warning(
                    "depends_on cascade: failed to write FAILED "
                    "transition for %s: %s", jobid, e,
                )
                return
            # EVENT-2 (v0.8.16): emit the STATE_TRANSITION event so a
            # cascade-fail is visible in events.jsonl. Inside the lock,
            # atomic with the write.
            events.state_transition(
                Path(fresh.cwd),
                jobid,
                from_state=JobState.PENDING.value,
                to_state=JobState.FAILED.value,
                reason=fresh.failure_reason,
            )

        # Reflect the new state in our in-memory snapshot so the rest of this
        # tick, including later dependents in scan order, sees the transition.
        spec.state = JobState.FAILED
        specs_by_id[spec.id] = fresh
        log.info(
            "depends_on cascade: %s -> FAILED (predecessor %s state=%s)",
            spec.id, failed_pred_id,
            failed_pred_state.value if failed_pred_state else "?",
        )
        # EVENT-2 (v0.8.16): fire the terminal webhook too (a no-op when no
        # webhook URL is configured). Keep the network call outside the lock.
        notify.send_terminal_notification(
            fresh, self.notify_webhook_url,
            notify_on_states=self.notify_on_states,
        )

    def _dispatch_pending(self) -> None:
        # Three independent caps: job count, cpu budget, memory budget.
        # All three must allow a dispatch; any one alone can block.
        #
        # v0.5.14: vq drain can temporarily lower max_jobs / max_cpus
        # below the daemon's configured value, or block all dispatches
        # entirely (full drain). Read the state file every iteration
        # so drain takes effect at the next admission pass (scheduler submit
        # bursts yield at the bounded quantum below).
        #
        # v0.5.45: also block when the admin-update-in-progress marker
        # is on disk. v0.5.44 added the marker but it only guarded the
        # `vq admin update` entry path; the daemon kept dispatching
        # into a possibly-half-installed venv whenever a prior update
        # was interrupted. Now the dispatch loop honors the marker
        # too. Running jobs continue (their process images are
        # already mapped; the venv mutation can't reach them);
        # reconcile / orphan / watchdog passes also continue. Only
        # NEW dispatches are gated. State-change logging (transition
        # in either direction) lets the operator see in the daemon
        # log when the marker was observed and when it cleared.
        # v0.14.x: the admin-update hold is scoped to what the marker
        # protects. A blanket return here made a pbs-cluster runtime rebuild park
        # slurm-cluster SLURM handoffs on an idle cluster (2026-07-25,
        # d446f1a62143) — same over-blocking class as the build-job gate
        # relaxed in v0.12.1 below. An unscoped (unreadable/unrecognised)
        # marker still holds everything, and running jobs are never touched.
        if self._poll_admin_update_marker():
            admin_held = self._admin_update_hold_scope
            if admin_held is None:
                return
        else:
            admin_held = frozenset()
        if self._config_unusable():
            return
        # v0.8.1 *Karp's Reduction*: the dispatch loop is the canonical
        # consumer of drain.json (the daemon owns the file). Skip the
        # RPC round-trip — it would just call into our own socket.
        drain_state = drain.read_effective_drain_state(
            via_rpc=False,
            multi_user=self._multi_user,
        )
        if drain_state is not None and drain_state.is_full_drain:
            # Full drain: no new dispatches at all. Running jobs continue.
            return
        # HP-2 (v0.8.24): if the watchdog has auto-paused running jobs for
        # host memory pressure, gate NEW dispatch — putting fresh jobs onto
        # the host we're actively relieving would defeat the pause. Like a
        # full drain, running / reattached jobs continue and only new dispatch
        # is held; the host-pressure pass un-gates (and resumes the paused
        # jobs) once pressure recedes. Composes with operator drain — neither
        # overrides the other; both are independent "hold new dispatch" gates.
        if self.watchdog.host_pressure_active:
            return
        if drain_state is not None:
            effective_max_jobs = drain_state.effective_max_jobs(self.max_jobs)
            effective_max_cpus = drain_state.effective_max_cpus(self.max_cpus)
        else:
            effective_max_jobs = self.max_jobs
            effective_max_cpus = self.max_cpus
        # v0.6.x: reattached orphans (jobs still alive from a previous
        # daemon, tracked in _orphans not _running) count against ALL
        # three budgets. Without this, a daemon restart with N live
        # orphans would dispatch a fresh max_jobs on top of them — the
        # host then runs N + max_jobs jobs, with N jobs' worth of CPU
        # + memory budget unaccounted (the over-subscription seen on
        # compute-a after a v0.6.x admin-update restart).
        # Local and scheduler jobs consume different resources. Local children
        # count against the driver's max_jobs / max_cpus / max_mem budget.
        # Scheduler jobs count only against max_scheduler_jobs: they run on the
        # cluster, and conflating the two caps let one local long job stall an
        # otherwise-empty PBS queue. Jobs preserved as reattach_failed after a
        # daemon restart are not in _scheduler_running, but they may still be
        # live in PBS; reserve scheduler slots for them until reattach succeeds
        # or an operator resolves the spec.
        # A killed job whose group outlived the wrapper vq reaped
        # (_TerminalSurvivor) is still on the host, so it counts against all
        # three local budgets until the escalation ends it. Without this, a
        # `vq kill` of a SIGTERM-ignoring command hands its slot to a fresh
        # job while the old one is still running.
        local_active = (
            len(self._running) + len(self._orphans) + len(self._terminal_survivors)
        )
        scheduler_active = len(self._scheduler_running)
        used_cpus = (
            sum(rj.cpus for rj in self._running.values())
            + sum(o.cpus for o in self._orphans.values())
            + sum(t.cpus for t in self._terminal_survivors.values())
        )
        used_mem = (
            sum((self._eff_mem_mb(rj.mem_mb) or 0) for rj in self._running.values())
            + sum((self._eff_mem_mb(o.mem_mb) or 0) for o in self._orphans.values())
            + sum(
                (self._eff_mem_mb(t.mem_mb) or 0)
                for t in self._terminal_survivors.values()
            )
        )
        # v0.5.29: dispatch order is (-priority, submitted_at). Higher
        # priority first; FIFO by submission within one priority level
        # (so the all-default-priority case is identical to the
        # pre-v0.5.29 pure-submitted_at ordering).
        # v0.5.31: a PENDING spec whose not_before is still in the future
        # is skipped — that's a job in retry-backoff. A corrupt
        # not_before is treated as "ready now" so a bad timestamp can't
        # permanently trap a job.
        now = datetime.now(UTC)

        def _backoff_ready(s: JobSpec) -> bool:
            return _not_before_ready(s.not_before, now)

        # v0.6.36: materialise the spec list ONCE per dispatch tick.
        # _iter_specs() globs + JSON-reads every queue dir and (in
        # multi-user mode) clears + repopulates _job_uid as a side
        # effect. The pre-v0.6.36 code called it 3× in this function
        # (pending sort, SUSPENDED-quota count, pending re-sort) — on
        # a busy multi-user host that tripled the per-tick stat+read
        # I/O for no behavioural gain. One materialised list leaves
        # _job_uid fully + stably populated for the rest of the pass.
        all_specs = list(self._iter_specs())
        if self._multi_user:
            admitted_specs: list[JobSpec] = []
            for spec in all_specs:
                owner_uid = self._job_uid.get(spec.id)
                spec_path = self._job_spec_paths.get(spec.id)
                transaction_may_exist = (
                    self._scheduler_transaction_may_exist(spec)
                    or self._scheduler_binding_requires_fence(spec)
                )
                if transaction_may_exist and spec.id not in self._scheduler_running:
                    try:
                        binding = self._scheduler_submit_binding_for(spec)
                        binding_error: str | None = None
                    except (OSError, ValueError) as exc:
                        binding = None
                        binding_error = str(exc)
                else:
                    binding = None
                    binding_error = None
                expected_target = (
                    binding.scheduler_target
                    if binding is not None
                    else spec.scheduler_target
                )
                binding_mismatch = (
                    binding is not None
                    and transaction_may_exist
                    and (
                        spec.scheduler_target != binding.scheduler_target
                        or spec.cwd != binding.cwd
                        or spec.cpus != binding.cpus
                    )
                )
                reason = self._validate_multi_user_spec(spec)
                if (
                    reason is None
                    and binding_error is None
                    and not binding_mismatch
                ):
                    admitted_specs.append(spec)
                    continue
                unsafe_reason = (
                    f"scheduler submit binding unreadable: {binding_error}"
                    if binding_error is not None
                    else "scheduler target/workspace changed after submit intent"
                    if binding_mismatch
                    else f"multi-user spec gate: {reason}"
                )
                if transaction_may_exist:
                    self._quarantine_unconfirmed_scheduler_spec(
                        spec.id,
                        owner_uid=owner_uid,
                        spec_path=spec_path,
                        reason=unsafe_reason,
                        expected_target=expected_target,
                    )
                    if spec_path is not None:
                        try:
                            quarantined = _read_untrusted_multi_user_spec(spec_path)
                        except (OSError, ValueError):
                            quarantined = None
                        if quarantined is not None:
                            admitted_specs.append(quarantined)
                    continue
                log.error(
                    "job %s: rejected by multi-user admission gate (%s); "
                    "marking FAILED",
                    spec.id,
                    reason,
                )
                # Dependency cascade, refresh/build orchestration, and
                # impossible-request failure can all write events or create
                # workspaces before _start_job's dispatch-time gate. Land the
                # invalid record through its captured queue path without using
                # any untrusted cwd/log field, then exclude it from this tick.
                self._fail_dispatch(
                    spec,
                    unsafe_reason,
                    emit_event=False,
                )
            all_specs = admitted_specs
        # Materialize durable scheduler reservations once. The predicate can
        # age out an unrecoverable no-id row and emits a one-time warning, so
        # global scheduler capacity and per-user quota must consume the same
        # result rather than evaluating it independently.
        untracked_scheduler_reservations: list[
            tuple[str | None, Path | None, JobSpec]
        ] = [
            (
                self._job_uid.get(spec.id) if self._multi_user else None,
                None,
                spec,
            )
            for spec in all_specs
            if spec.id not in self._scheduler_running
            and spec.scheduler_state == "reattach_failed"
            and spec.state in (JobState.RUNNING, JobState.SUSPENDED)
            and self._untracked_spec_still_reserves(spec)
        ]
        untracked_scheduler_reservations.extend(
            (
                self._job_uid.get(spec.id) if self._multi_user else None,
                None,
                spec,
            )
            for spec in all_specs
            if spec.id not in self._scheduler_running
            and spec.scheduler_state == "scheduler_reconciliation_quarantined"
            and spec.state in (JobState.RUNNING, JobState.SUSPENDED)
        )
        # An ambiguous qsub/sbatch is deliberately an indefinite reservation:
        # ageing it out and dispatching behind it would silently convert an
        # uncertainty fence into duplicate work. Only exact remote evidence or
        # an explicit operator terminal transition releases this slot.
        untracked_scheduler_reservations.extend(
            (
                self._job_uid.get(spec.id) if self._multi_user else None,
                None,
                spec,
            )
            for spec in all_specs
            if spec.id not in self._scheduler_running
            and spec.state
            in {JobState.SUBMITTING, JobState.SUBMIT_OUTCOME_UNKNOWN}
        )
        untracked_scheduler_reservations.extend(
            (
                self._job_uid.get(spec.id) if self._multi_user else None,
                None,
                spec,
            )
            for spec in all_specs
            if spec.id not in self._scheduler_running
            and spec.is_terminal
            and spec.scheduler_state
            in {
                "submitting",
                "submit_outcome_unknown",
                "submit_outcome_unknown_after_terminal",
                "submit_evidence_conflict_after_terminal",
                "submit_reconciliation_quarantined_after_terminal",
                "submit_cancel_pending_after_terminal",
            }
        )
        if self._multi_user:
            already_reserved = {
                (owner_uid, reserved.id)
                for owner_uid, _path, reserved in untracked_scheduler_reservations
            }
            for spec in all_specs:
                owner_uid = self._job_uid.get(spec.id)
                if (
                    spec.id in self._scheduler_running
                    or (owner_uid, spec.id) in already_reserved
                    or not self._scheduler_binding_requires_fence(spec)
                ):
                    continue
                untracked_scheduler_reservations.append((owner_uid, None, spec))
                already_reserved.add((owner_uid, spec.id))
        # A duplicate-id quarantine must freeze mutations, not erase resource
        # evidence. Each active scheduler row may still name a distinct remote
        # job. Keep it reserved indefinitely while ambiguous;
        # ageing out a no-id row would itself be an ownership decision we
        # cannot make safely until the collision is resolved. If one owner is
        # already represented by a live scheduler handle, deduplicate only that
        # exact owner/id pair and retain every other colliding owner-qualified
        # row.
        tracked_scheduler_owners = {
            (str(owner), jobid)
            for jobid, runtime in self._scheduler_running.items()
            if (
                owner := getattr(runtime, "owner_uid", None)
                or self._job_uid.get(jobid)
            )
            is not None
        }
        untracked_scheduler_reservations.extend(
            (uid, spec_path, spec)
            for uid, spec_path, spec in self._colliding_scheduler_reservations
            if (uid, spec.id) not in tracked_scheduler_owners
        )
        scheduler_active += len(untracked_scheduler_reservations)

        # v0.6.51: build a {jobid -> spec} index for the depends_on
        # dispatch gate. The gate has two effects on a PENDING spec
        # with a non-empty depends_on:
        #
        # * If ALL predecessors are COMPLETED → dispatch-eligible
        #   (subject to budgets / quotas / backoff like any PENDING
        #   spec).
        # * If ANY predecessor is in a non-COMPLETED terminal state
        #   (FAILED / KILLED / OOM_KILLED / STARVED / TIME_EXCEEDED /
        #   INTERRUPTED / ABORTED_BY_QUEUE) → cascade-fail the
        #   dependent: transition to FAILED with a ``failure_reason``
        #   naming the failing predecessor + its state, and persist
        #   the spec. This pass runs BEFORE the pending filter so
        #   cascade-failed jobs disappear from the dispatch queue
        #   on the same tick and surface to `vq status` / `vq queue`
        #   immediately.
        # * Otherwise (at least one predecessor still non-terminal,
        #   or a predecessor jobid that doesn't resolve in any user
        #   dir) → leave PENDING. Missing predecessor is treated
        #   conservatively as "still waiting" so an accidental
        #   `vq cleanup --delete` doesn't silently fail the dependent.
        specs_by_id: dict[str, JobSpec] = {s.id: s for s in all_specs}
        for spec in list(all_specs):
            self._maybe_cascade_dependency_failure(spec, specs_by_id)

        def _deps_ready(s: JobSpec) -> bool:
            """v0.6.51: a PENDING spec with depends_on is ready iff
            every predecessor is COMPLETED. Cascade-fail already
            consumed the predecessor-failure case above; this just
            asks 'are they all done-successfully yet?'

            v0.7.8 *Knuth's Schedule*: also gate on ``depends_on_any``
            — every predecessor in that list must be in a terminal
            state (any state, including failure variants). Combining
            additively with ``depends_on``: a spec carrying both
            lists waits for ``all(depends_on COMPLETED) AND
            all(depends_on_any TERMINAL)``.
            """
            return next(
                admission.iter_unmet_dependencies(s, specs_by_id),
                None,
            ) is None

        pending = sorted(
            (
                s
                for s in all_specs
                if s.state == JobState.PENDING
                and _backoff_ready(s)
                and _deps_ready(s)
            ),
            key=lambda s: (-s.priority, s.submitted_at),
        )
        # v0.11.0: --refresh hook. If the NEXT job that would dispatch
        # (``pending[0]`` in the priority/FIFO order just computed) asked
        # for a venv-env rebuild, drain the host and rebuild before it
        # runs. Placed here — after the full-drain / host-pressure "hold
        # all dispatch" gates above, and after the priority+deps sort, but
        # before the budget gates + ``_start_job`` below — so the refresh
        # (a) respects an operator drain / host-pressure pause, (b) acts on
        # the genuinely-next job rather than an arbitrary refresh job
        # ahead of higher-priority work, and (c) drains the WHOLE host
        # (ignoring per-job budgets, which don't apply to a host-wide
        # rebuild). Returns True when this tick must not dispatch anything
        # (drain still in progress, or the rebuild just ran / failed this
        # tick); the job itself dispatches on a later tick once the env is
        # fresh. See HANDOVER_VQ_REFRESH.md.
        if pending and self._maybe_refresh_before_run(pending[0], all_specs):
            return
        # v0.12.1: a build job is still exclusive for local work and for
        # scheduler-target jobs that depend on the mutating env, but it no
        # longer globally parks unrelated scheduler-target dispatch. slurm-cluster
        # exposed this: a local stopped build of vibeqc-dev held a tiny,
        # independent SLURM smoke on an idle cluster even though the smoke did
        # not consume or mutate the local build env.
        running_build_envs = {
            s.build_env
            for s in all_specs
            if s.build_env is not None and s.state == JobState.RUNNING
        }
        running_build_ids = {
            s.id
            for s in all_specs
            if s.build_env is not None and s.state == JobState.RUNNING
        }

        def _blocked_by_running_build(s: JobSpec) -> bool:
            if not running_build_envs:
                return False
            if s.scheduler_target is None:
                return True
            if any(dep in running_build_ids for dep in s.depends_on):
                return True
            if s.build_env is not None:
                return True
            if s.refresh_before in running_build_envs:
                return True
            return s.program in running_build_envs

        def _held_by_admin_update(s: JobSpec) -> bool:
            """Scoped admin-update hold: a spec is held only when its
            dispatch target is what the live marker protects."""
            if not admin_held:
                return False
            if s.scheduler_target is not None:
                return s.scheduler_target in admin_held
            return admin.LOCAL_DISPATCH_SCOPE in admin_held
        # Per-user quota enforcement. ``max_pending_jobs`` is the historical
        # config name, but its dispatch contract is an active-work cap: PENDING
        # specs are candidates and do not consume it. Count and CPU usage come
        # from one owner-qualified holder projection so a SUSPENDED job
        # represented in a runtime map and on disk is charged exactly once.
        per_user_active: dict[str, int] = {}  # uid -> count of active jobs
        per_user_cpus: dict[str, int] = {}  # uid -> used CPUs
        refresh_per_user_usage: Callable[[], None] | None = None
        if self._multi_user:
            cfg = load_config()

            def _runtime_owner(jobid: str, runtime: object) -> str:
                owner = getattr(runtime, "owner_uid", None)
                if owner is None:
                    owner = getattr(runtime, "uid", None)
                return owner or self._job_uid.get(jobid, "unknown")

            def _refresh_per_user_usage() -> None:
                quota_holders: dict[tuple[str, str, str], int] = {}
                for jid, running_job in self._running.items():
                    uid = _runtime_owner(jid, running_job)
                    quota_holders[("job", uid, jid)] = running_job.cpus
                for jid, scheduler_job in self._scheduler_running.items():
                    uid = _runtime_owner(jid, scheduler_job)
                    quota_holders.setdefault(
                        ("job", uid, jid),
                        scheduler_job.cpus,
                    )
                for jid, orphan in self._orphans.items():
                    uid = _runtime_owner(jid, orphan)
                    quota_holders.setdefault(
                        ("job", uid, jid),
                        orphan.cpus,
                    )
                for jid, survivor in self._terminal_survivors.items():
                    uid = _runtime_owner(jid, survivor)
                    quota_holders.setdefault(
                        ("job", uid, jid),
                        survivor.cpus,
                    )
                # A suspended spec can survive without a live in-memory handle.
                # setdefault keeps local children, scheduler handles, and orphans
                # from being charged again through their durable state.
                for durable_spec in all_specs:
                    if durable_spec.state == JobState.SUSPENDED:
                        uid = self._job_uid.get(durable_spec.id, "unknown")
                        quota_holders.setdefault(
                            ("job", uid, durable_spec.id),
                            self._scheduler_reservation_cpus(uid, durable_spec)
                            if durable_spec.scheduler_target is not None
                            else durable_spec.cpus,
                        )
                for reservation_uid, reservation_path, reserved_spec in (
                    untracked_scheduler_reservations
                ):
                    uid = reservation_uid or "unknown"
                    identity_kind = "quarantine" if reservation_path else "job"
                    identity = (
                        str(reservation_path)
                        if reservation_path is not None
                        else reserved_spec.id
                    )
                    quota_holders.setdefault(
                        (identity_kind, uid, identity),
                        self._scheduler_reservation_cpus(
                            reservation_uid,
                            reserved_spec,
                        ),
                    )
                per_user_active.clear()
                per_user_cpus.clear()
                for (_kind, uid, _identity), cpus in quota_holders.items():
                    per_user_active[uid] = per_user_active.get(uid, 0) + 1
                    per_user_cpus[uid] = per_user_cpus.get(uid, 0) + cpus

            refresh_per_user_usage = _refresh_per_user_usage
            refresh_per_user_usage()
        scheduler_reconcile_deadline = (
            time.monotonic() + SCHEDULER_DISPATCH_RECONCILE_QUANTUM_SECONDS
        )
        for spec in pending:
            if _blocked_by_running_build(spec):
                continue
            if _held_by_admin_update(spec):
                continue
            # v0.12.0 build-as-job: a spec still carrying refresh_before has
            # not been processed by _maybe_refresh_before_run yet (that runs
            # only on pending[0]). It must not dispatch until its env is
            # rebuilt, so skip it here. It dispatches once it reaches the
            # head, gets a build-job dependency, and that build completes.
            if spec.refresh_before is not None:
                continue
            if spec.scheduler_target is None:
                # 2026-07-25: fail-fast an AUTO-GENERATED build job that can
                # NEVER dispatch on this daemon — its request exceeds the
                # CONFIGURED caps (not the drain-lowered effective ones,
                # which recover). compute-a sat with a 16-CPU refresh build
                # pending forever against an 8-CPU daemon; eternal-pending
                # reads as "queued" while meaning "impossible". Deliberately
                # scoped to build jobs: an OPERATOR spec over the caps stays
                # pending on purpose — restarting the daemon with a higher
                # --max-cpus is a supported way to let it dispatch, and two
                # long-standing tests pin that semantic.
                if spec.build_env is not None:
                    impossible: str | None = None
                    if spec.cpus > self.max_cpus:
                        impossible = (
                            f"requests {spec.cpus} cpus but this daemon "
                            f"caps at --max-cpus {self.max_cpus}"
                        )
                    else:
                        eff_mem = self._eff_mem_mb(spec.mem_mb)
                        if (
                            self.max_mem_mb is not None
                            and eff_mem is not None
                            and eff_mem > self.max_mem_mb
                        ):
                            impossible = (
                                f"requests {eff_mem} MB but this daemon "
                                f"caps at --max-mem-mb {self.max_mem_mb}"
                            )
                    if impossible is not None:
                        self._fail_impossible_spec(spec, impossible)
                        continue
                # _orphans is fixed for this dispatch pass; _running grows as we
                # dispatch, so this stops local jobs once running + orphans hits
                # the local-process cap without blocking scheduler submissions
                # later in the priority order.
                if (
                    effective_max_jobs is not None
                    and local_active >= effective_max_jobs
                ):
                    continue
                if used_cpus + spec.cpus > effective_max_cpus:
                    continue
                # Memory gate. An undeclared job is charged the daemon's
                # default_job_mem_mb (when configured) via _eff_mem_mb, so it
                # contributes to the running tally instead of dispatching free
                # -- the v0.4 tightening of the old v0.3 leniency. When no
                # default is set AND the job declares nothing, eff_mem is None
                # and the gate is skipped (back-compat: undeclared unbounded).
                eff_mem = self._eff_mem_mb(spec.mem_mb)
                if (
                    self.max_mem_mb is not None
                    and eff_mem is not None
                    and used_mem + eff_mem > self.max_mem_mb
                ):
                    continue
            else:
                if (
                    drain_state is not None
                    and drain_state.drains_scheduler_target(spec.scheduler_target)
                ):
                    continue
                # Scheduler jobs (§17) skip the LOCAL cpu/mem gates: they run on
                # the cluster, so their cpus/mem are the scheduler's to honour via
                # #PBS directives. A separate scheduler cap bounds how many qsubs
                # the driver keeps active/queued at once.
                if (
                    self.max_scheduler_jobs is not None
                    and scheduler_active >= self.max_scheduler_jobs
                ):
                    continue
            # v0.6.x: per-user quota gate.
            if self._multi_user:
                uid = self._job_uid.get(spec.id, "unknown")
                max_pending = cfg.quotas.effective_max_pending_jobs(uid)
                max_cpus_quota = cfg.quotas.effective_max_concurrent_cpus(uid)
                if max_pending is not None and per_user_active.get(uid, 0) >= max_pending:
                    continue  # user at their max-pending cap
                if (
                    max_cpus_quota is not None
                    and per_user_cpus.get(uid, 0) + spec.cpus > max_cpus_quota
                ):
                    continue  # user at their CPU quota
            # Re-read just before dispatch in case `vq kill` raced us between
            # the directory scan and now.
            try:
                fresh, _ = self._read_active_spec(
                    spec.id,
                    owner_uid=self._job_uid.get(spec.id),
                    spec_path=self._job_spec_paths.get(spec.id),
                )
            except (OSError, ValueError):
                log.exception("cannot safely reread pending spec %s; skipping", spec.id)
                continue
            if fresh.state != JobState.PENDING:
                continue
            started = self._start_job(fresh)
            recovered_scheduler_runtime = (
                fresh.scheduler_target is not None
                and not started
                and fresh.id in self._scheduler_running
            )
            if (
                fresh.scheduler_target is not None
                and not started
                and fresh.id not in self._scheduler_running
            ):
                # qsub/sbatch ambiguity changes the durable row from PENDING to
                # SUBMIT_OUTCOME_UNKNOWN but intentionally returns False.  The
                # reservation projection above was materialized before that
                # transition, so add the exact owner-qualified holder now or a
                # second submit in this same scan can bypass both the global
                # scheduler cap and the first submitter's quota.
                try:
                    reserved, _reserved_path = self._read_active_spec(
                        fresh.id,
                        owner_uid=self._job_uid.get(fresh.id),
                        spec_path=self._job_spec_paths.get(fresh.id),
                    )
                except (OSError, ValueError):
                    reserved = None
                    _reserved_path = None
                    reservation_uid = (
                        self._job_uid.get(fresh.id) if self._multi_user else None
                    )
                    if reservation_uid is not None:
                        try:
                            binding = _read_scheduler_submit_binding(
                                reservation_uid,
                                fresh.id,
                            )
                        except (OSError, ValueError):
                            binding = None
                        if _scheduler_submit_binding_may_be_open(binding):
                            assert binding is not None
                            reserved = binding.admitted_spec.model_copy(deep=True)
                            if not reserved.is_terminal:
                                reserved.state = JobState.SUBMIT_OUTCOME_UNKNOWN
                                reserved.scheduler_state = "submit_outcome_unknown"
                if (
                    reserved is not None
                    and reserved.state
                    in {JobState.SUBMITTING, JobState.SUBMIT_OUTCOME_UNKNOWN}
                ):
                    reservation_uid = (
                        self._job_uid.get(fresh.id) if self._multi_user else None
                    )
                    # A unique admitted row is owner-qualified by uid + id.  A
                    # concrete path is reserved for duplicate-id quarantine
                    # entries, whose separate copies must not deduplicate.
                    reservation_path = None
                    reservation_key = (
                        reservation_uid,
                        reservation_path,
                        reserved.id,
                    )
                    existing_keys = {
                        (uid, path, held.id)
                        for uid, path, held in untracked_scheduler_reservations
                    }
                    if reservation_key not in existing_keys:
                        untracked_scheduler_reservations.append(
                            (reservation_uid, reservation_path, reserved)
                        )
                        scheduler_active += 1
                        if refresh_per_user_usage is not None:
                            refresh_per_user_usage()
            if fresh.scheduler_target is not None and self._running:
                # Scheduler submission crosses SSH and qsub/sbatch and can take
                # several seconds. A large pending batch previously kept this
                # loop inside serial network calls for minutes, leaving local
                # children that exited meanwhile as zombies until the whole
                # batch had been submitted. Poll local children after every
                # scheduler attempt so their terminal state and resource
                # release are not starved by unrelated cluster admission.
                self._reconcile_running()
            if started or recovered_scheduler_runtime:
                # Scheduler jobs consume no driver cpu/mem (§17), so they do not
                # add to the local running tally that gates further dispatch.
                if fresh.scheduler_target is None:
                    local_active += 1
                    used_cpus += fresh.cpus
                    used_mem += self._eff_mem_mb(fresh.mem_mb) or 0
                else:
                    scheduler_active += 1
                if self._multi_user:
                    uid = self._job_uid.get(fresh.id, "unknown")
                    per_user_active[uid] = per_user_active.get(uid, 0) + 1
                    per_user_cpus[uid] = per_user_cpus.get(uid, 0) + fresh.cpus
                if started and fresh.build_env is not None:
                    # v0.12.0 build-as-job: a build job runs exclusively. Stop
                    # dispatching this tick so nothing co-starts beside it, and
                    # the build-in-progress gate holds the subsequent ticks.
                    break
            if (
                fresh.scheduler_target is not None
                and time.monotonic() >= scheduler_reconcile_deadline
            ):
                self._reconcile_scheduler()
                # Re-enter full admission with a fresh pending order, config,
                # drain/update holds and owner-qualified reservation census.
                # Reconciliation alone left a newly submitted priority-0 job
                # behind hundreds of priority -1 rows in this snapshot.
                self._scheduler_refresh_wakeup.set()
                return

    def _find_build_job(self, env: str, all_specs: list[JobSpec]) -> str | None:
        """Return the jobid of a non-terminal build job for ``env`` (a spec
        with ``build_env == env`` still PENDING or RUNNING), or None.

        The deduplication primitive: a burst of N ``--refresh <env>`` jobs all
        attach (via ``depends_on``) to ONE build job instead of each draining
        the host and rebuilding. Callers pass the dispatch tick's materialised
        ``all_specs`` so this is a list scan, no extra disk I/O."""
        for s in all_specs:
            if s.build_env == env and not s.is_terminal:
                return s.id
        return None

    def _create_build_job(self, env: str, like_spec: JobSpec) -> str:
        """Create and persist a PENDING build job that rebuilds ``env`` as a
        first-class, cgroup-capped, exclusive job.

        Mirrors ``_spawn_rerun_clone`` for the id / workspace / multi-user-dir
        handling. The command is ``vq build-env <env>`` run under the daemon's
        own interpreter (so it resolves on the host). It is declared at
        ``BUILD_JOB_PRIORITY`` (so it is ``pending[0]`` on the drained host the
        moment it is created), with ``cpus`` = the full core budget (a fast
        rebuild, since it runs exclusively) and ``mem_mb`` = the daemon memory
        budget (so the cgroup wrap caps it). ``like_spec`` is the --refresh job
        that triggered it. Its submitter owns the build job in multi-user
        mode."""
        build_id = new_jobid()
        if self._multi_user and like_spec.submitter:
            uid = like_spec.submitter
            queue_dir = paths.user_queue_dir(uid)
            jobs_dir = paths.user_jobs_dir(uid)
        else:
            queue_dir = self.queue_dir
            jobs_dir = self.jobs_dir
        workspace = jobs_dir / build_id
        workspace.mkdir(parents=True, exist_ok=True)
        build_spec = JobSpec(
            id=build_id,
            command=[sys.executable, "-m", "vq", "build-env", env],
            cwd=str(workspace.resolve()),
            cpus=self.max_cpus or os.cpu_count() or 1,
            mem_mb=self.max_mem_mb,
            priority=BUILD_JOB_PRIORITY,
            build_env=env,
            # v0.12.x fix 1: a wall-time cap so the watchdog reaps a wedged
            # rebuild (SIGTERM->SIGKILL its whole group, releasing the CPUs)
            # even if it slips past the in-process stall guard. Sits above
            # the update_script's own cap so the loud in-process timeout
            # fires first and this is the last-resort backstop.
            wall_time_seconds=build_job.default_build_wall_time(),
            job_name=f"build-{env}",
            state=JobState.PENDING,
            submitter=like_spec.submitter,
        )
        build_spec.write(queue_dir / f"{build_id}.json")
        log.info(
            "build-as-job: created build job %s for env %r "
            "(cpus=%s, mem_mb=%s) on behalf of refresh job %s",
            build_id, env, build_spec.cpus, build_spec.mem_mb, like_spec.id,
        )
        return build_id

    def _maybe_refresh_before_run(
        self, next_spec: JobSpec, all_specs: list[JobSpec]
    ) -> bool:
        """v0.12.0 build-as-job: per-job ``--refresh`` orchestration. Called
        from ``_dispatch_pending`` with the job that would dispatch next
        (``pending[0]``) and the tick's materialised ``all_specs``.

        Returns ``True`` to hold ALL dispatch this tick, ``False`` to let
        dispatch proceed:

        * ``next_spec.refresh_before`` is None -> ``False`` (the common case).
        * A build job for the env is already in flight (found in
          ``all_specs``) -> attach ``next_spec`` to it via ``depends_on``,
          clear ``refresh_before``, return ``True``. No drain, no second
          build. This is the dedup: a burst of N same-env refresh jobs all
          share ONE build job.
        * No build in flight and the host is not yet drained -> drain (hold),
          return ``True``.
        * No build in flight and the host is drained -> create the build job
          (a first-class, cgroup-capped, exclusive ``vq build-env`` job),
          attach ``next_spec`` to it, return ``True``.

        After attaching, the existing ``depends_on`` gate (``_deps_ready``
        plus the cascade-fail pass in ``_dispatch_pending``) does the rest:
        it holds ``next_spec`` out of ``pending`` until the build is
        COMPLETED, and cascade-fails it if the build FAILED. So a failed
        rebuild can never run a job against a half-built env, and the queue
        can never wedge, there is no synchronous rebuild here, only a spec
        edit. The build itself runs through the normal job path, so its
        storm is cgroup-capped and it shows in ``vq status`` with its own
        events. ``_fail_refresh_job`` survives only for the create-failed
        edge below."""
        env = next_spec.refresh_before
        if env is None:
            return False  # no refresh requested, dispatch normally

        # Dedup: if a build job for this env is already PENDING/RUNNING,
        # attach to it (below) without draining or creating a second one.
        build_id = self._find_build_job(env, all_specs)
        if build_id is None:
            # No build in flight. Drain the host first (hold all dispatch
            # until every RUNNING + reattached-orphan job finishes), then
            # create the build job. SUSPENDED jobs are left alone: the
            # refresh waits behind a paused job the same way it waits behind
            # a running one.
            n_active = (
                len(self._running)
                + len(self._orphans)
                + len(self._terminal_survivors)
            )
            if n_active > 0:
                if self._refresh_draining_for != next_spec.id:
                    self._refresh_draining_for = next_spec.id
                    log.info(
                        "build-as-job: job %s wants env %r rebuilt; draining "
                        "host first (%d active: %d running + %d orphan + %d "
                        "killed-but-alive) before creating the build job",
                        next_spec.id, env, n_active,
                        len(self._running), len(self._orphans),
                        len(self._terminal_survivors),
                    )
                return True  # hold dispatch while the host drains
            self._refresh_draining_for = None
            try:
                build_id = self._create_build_job(env, next_spec)
            except Exception as e:  # noqa: BLE001 (never wedge on create failure)
                # Spec write / mkdir failed. Fail the refresh job rather than
                # re-trying every tick forever (it would stay pending[0]).
                return self._fail_refresh_job(
                    next_spec.id, env,
                    f"could not create build job for env {env!r}: {e}",
                )

        # Attach next_spec to the build job + clear the trigger. The
        # depends_on gate now holds it out of ``pending`` until the build is
        # COMPLETED (or cascade-fails it if the build FAILED). Lock-safe
        # read-modify-write. If the spec left PENDING under us (vq kill
        # raced), leave it.
        jobid = next_spec.id
        spec_path = self._active_spec_path(jobid)
        with paths.spec_lock(spec_path):
            try:
                fresh, _ = self._read_active_spec(jobid, spec_path=spec_path)
            except (OSError, ValueError):
                return False  # job vanished, let dispatch carry on
            if fresh.state != JobState.PENDING:
                return False  # no longer ours to dispatch
            if build_id not in fresh.depends_on:
                fresh.depends_on = [*fresh.depends_on, build_id]
            fresh.refresh_before = None
            fresh.write(spec_path)
        log.info(
            "build-as-job: refresh job %s now depends on build job %s "
            "for env %r", next_spec.id, build_id, env,
        )
        return True

    def _fail_refresh_job(self, jobid: str, env: str, reason: str) -> bool:
        """v0.11.0: transition a job to FAILED because its ``--refresh``
        rebuild could not be completed. Lock-safe read-modify-write
        (same pattern as the depends_on cascade-fail). Returns ``True``
        so the caller holds this dispatch tick; the FAILED job drops out
        of ``pending`` immediately so the queue is never wedged by a
        broken-env refresh. Idempotent against a racing terminal
        transition (only acts while the spec is still PENDING)."""
        log.error("refresh: failing job %s — %s", jobid, reason)
        spec_path = self._active_spec_path(jobid)
        with paths.spec_lock(spec_path):
            try:
                fresh, _ = self._read_active_spec(jobid, spec_path=spec_path)
            except (OSError, ValueError):
                return True
            if fresh.state != JobState.PENDING:
                return True
            fresh.state = JobState.FAILED
            fresh.finished_at = utcnow_iso()
            fresh.failure_reason = reason
            # Clear the trigger so, in the impossible event the spec is
            # somehow re-set to PENDING, we don't loop on the same failed
            # rebuild.
            fresh.refresh_before = None
            try:
                fresh.write(spec_path)
            except OSError as e:
                log.warning(
                    "refresh: failed to write FAILED transition for %s: %s",
                    jobid, e,
                )
                return True
            events.state_transition(
                Path(fresh.cwd),
                jobid,
                from_state=JobState.PENDING.value,
                to_state=JobState.FAILED.value,
                reason=reason,
            )
        # Terminal webhook (no-op when no URL configured), outside the
        # lock — it's a network call. Mirrors the depends_on cascade.
        notify.send_terminal_notification(
            fresh, self.notify_webhook_url,
            notify_on_states=self.notify_on_states,
        )
        return True

    def _validate_multi_user_spec(self, spec: JobSpec) -> str | None:
        """v0.6.35: vet a spec before the multi-user daemon acts on it
        as root. Returns a rejection reason, or ``None`` if safe.

        Threat model: in multi-user mode each user **owns** their
        ``/var/lib/vq/users/<uid>/queue/`` directory, so every field
        of a spec found there is attacker-controlled — a user can
        drop a hand-crafted JSON file straight in without going
        through ``vq submit``. The daemon then runs ``_start_job`` as
        **root**: it ``chown``s the workspace, opens stdout/stderr,
        and drops privileges to a uid. If any of those inputs are
        trusted blindly, a user can run a job as root (forge
        ``submitter``), chown an arbitrary path (forge ``cwd``), or
        create a root-owned file anywhere (forge ``stdout_path`` /
        ``stderr_path``).

        The only trustworthy uid is the directory the spec was read
        from: ``users/`` is root-owned, so a user cannot create or
        rename a ``<uid>/`` entry. ``_iter_specs`` records that as
        ``_job_uid``. This gate (1) binds ``submitter`` to that uid,
        and (2) confines every path the daemon touches as root to the
        submitter's own ``jobs/`` tree.

        Single-user mode never calls this — there is no privilege
        boundary and the queue dir is the operator's own.
        """
        trusted = self._job_uid.get(spec.id)
        if trusted is None:
            return "cannot determine the owning per-user state directory"
        try:
            trusted_uid = int(trusted)
        except ValueError:
            return f"owning state dir name {trusted!r} is not a numeric uid"
        # 1. submitter must match the directory the spec lives in.
        if spec.submitter is None:
            return "no submitter recorded (a multi-user spec must declare one)"
        try:
            claimed = int(spec.submitter)
        except ValueError:
            return f"submitter={spec.submitter!r} is not a numeric uid"
        if claimed != trusted_uid:
            return (
                f"submitter={claimed} does not match the owning state "
                f"directory uid={trusted_uid} — forged or misplaced spec"
            )
        # 2. cwd must resolve inside the submitter's own jobs tree.
        jobs_root_path = paths.user_jobs_dir(trusted_uid)
        try:
            jobs_root_stat = jobs_root_path.lstat()
        except OSError as e:
            return f"jobs root {jobs_root_path} could not be inspected: {e}"
        if not stat.S_ISDIR(jobs_root_stat.st_mode):
            return f"jobs root {jobs_root_path} is not a real directory"
        jobs_root = jobs_root_path.resolve()
        try:
            cwd = Path(spec.cwd).resolve()
        except (OSError, RuntimeError, ValueError) as e:
            return f"cwd={spec.cwd!r} could not be resolved: {e}"
        if cwd != jobs_root and jobs_root not in cwd.parents:
            return (
                f"cwd={spec.cwd!r} is outside the submitter's jobs tree "
                f"{jobs_root} — refusing to chown/run there as root"
            )
        # 3. stdout/stderr targets must stay inside the workspace
        #    (an absolute or ``../`` path would escape the join).
        for label, rel in (
            ("stdout_path", spec.stdout_path),
            ("stderr_path", spec.stderr_path),
        ):
            try:
                target = (cwd / rel).resolve()
            except (OSError, RuntimeError, ValueError) as e:
                return f"{label}={rel!r} could not be resolved: {e}"
            if target != cwd and cwd not in target.parents:
                return (
                    f"{label}={rel!r} escapes the job workspace — "
                    f"refusing to open it as root"
                )
        return None

    def _scheduler_submit_binding_for(
        self,
        spec: JobSpec,
    ) -> _SchedulerSubmitBinding | None:
        """Read the daemon-owned scheduler authority for one admitted owner."""
        if not self._multi_user:
            return None
        owner_uid = self._job_uid.get(spec.id)
        if owner_uid is None:
            return None
        return _read_scheduler_submit_binding(owner_uid, spec.id)

    def _scheduler_binding_requires_fence(self, spec: JobSpec) -> bool:
        """Treat missing/corrupt mutable state as open when private authority is."""
        if not self._multi_user:
            return False
        owner_uid = self._job_uid.get(spec.id)
        if owner_uid is None:
            return False
        try:
            binding = _read_scheduler_submit_binding(owner_uid, spec.id)
        except (OSError, ValueError):
            return True
        return _scheduler_submit_binding_may_be_open(binding)

    @staticmethod
    def _owner_scheduler_binding_requires_fence(
        owner_uid: str,
        job_id: str,
    ) -> bool:
        """Owner-qualified form used before duplicate bare ids are admitted."""
        try:
            binding = _read_scheduler_submit_binding(owner_uid, job_id)
        except (OSError, ValueError):
            return True
        return _scheduler_submit_binding_may_be_open(binding)

    @staticmethod
    def _scheduler_transaction_may_exist(spec: JobSpec) -> bool:
        """Whether terminalizing a mutable row could abandon remote work."""
        target_backed_state = spec.scheduler_target is not None and spec.state in {
            JobState.RUNNING,
            JobState.SUSPENDED,
            JobState.SUBMITTING,
            JobState.SUBMIT_OUTCOME_UNKNOWN,
        }
        return target_backed_state or spec.scheduler_state in {
            "submitting",
            "submit_outcome_unknown",
            "submit_outcome_unknown_after_terminal",
            "submit_evidence_conflict",
            "submit_evidence_conflict_after_terminal",
            "submit_reconciliation_quarantined",
            "submit_reconciliation_quarantined_after_terminal",
            "submit_cancel_pending_after_terminal",
            "scheduler_reconciliation_quarantined",
            "reattach_failed",
        }

    def _capture_scheduler_transaction_authority(
        self,
        spec: JobSpec,
    ) -> tuple[JobSpec, str | None, Path | None, str | None]:
        """Bind a mutable row to its daemon-owned owner/path/target authority."""
        owner_uid = self._job_uid.get(spec.id) if self._multi_user else None
        spec_path = (
            self._job_spec_paths.get(spec.id) if self._multi_user else None
        )
        if not self._multi_user:
            return spec, owner_uid, spec_path, spec.scheduler_target
        if owner_uid is None or spec_path is None:
            return spec, owner_uid, spec_path, None
        try:
            binding = _read_scheduler_submit_binding(owner_uid, spec.id)
        except (OSError, ValueError) as exc:
            self._quarantine_unconfirmed_scheduler_spec(
                spec.id,
                owner_uid=owner_uid,
                spec_path=spec_path,
                reason=f"scheduler submit binding unreadable: {exc}",
                expected_target=spec.scheduler_target,
            )
            return spec, owner_uid, spec_path, None
        if binding is None and self._scheduler_transaction_may_exist(spec):
            self._quarantine_unconfirmed_scheduler_spec(
                spec.id,
                owner_uid=owner_uid,
                spec_path=spec_path,
                reason="scheduler submit binding is missing after submit intent",
                expected_target=spec.scheduler_target,
            )
            return spec, owner_uid, spec_path, None
        expected_target = (
            binding.scheduler_target
            if binding is not None
            else spec.scheduler_target
        )
        if (
            binding is not None
            and self._scheduler_transaction_may_exist(spec)
            and (
                spec.scheduler_target != binding.scheduler_target
                or spec.cwd != binding.cwd
                or spec.cpus != binding.cpus
            )
        ):
            self._quarantine_unconfirmed_scheduler_spec(
                spec.id,
                owner_uid=owner_uid,
                spec_path=spec_path,
                reason="scheduler target/workspace changed after submit intent",
                expected_target=expected_target,
            )
            with contextlib.suppress(OSError, ValueError):
                spec = _read_untrusted_multi_user_spec(spec_path)
            return spec, owner_uid, spec_path, None
        return spec, owner_uid, spec_path, expected_target

    def _multi_user_scheduler_authority_error(
        self,
        spec: JobSpec,
        *,
        owner_uid: str | None,
        expected_target: str | None,
        expected_scheduler_job_id: str | None = None,
    ) -> str | None:
        """Revalidate every mutable field that can redirect a root action."""
        validation_error = self._validate_multi_user_spec(spec)
        if validation_error is not None:
            return f"multi-user spec gate: {validation_error}"
        if owner_uid is None:
            return "scheduler transaction has no captured owner"
        try:
            binding = _read_scheduler_submit_binding(owner_uid, spec.id)
        except (OSError, ValueError) as exc:
            return f"scheduler submit binding unreadable: {exc}"
        if binding is None:
            return "scheduler submit binding is missing after submit intent"
        if expected_target is None or binding.scheduler_target != expected_target:
            return "scheduler submit binding target changed during reconciliation"
        if spec.scheduler_target != binding.scheduler_target:
            return "scheduler target changed during submit reconciliation"
        if spec.cwd != binding.cwd:
            return "scheduler workspace changed during submit reconciliation"
        if spec.cpus != binding.cpus:
            return "scheduler CPU request changed during submit reconciliation"
        if (
            expected_scheduler_job_id is not None
            and binding.scheduler_job_id != expected_scheduler_job_id
        ):
            return "scheduler id does not match daemon-owned acceptance authority"
        if (
            binding.scheduler_job_id is not None
            and spec.scheduler_job_id is not None
            and spec.scheduler_job_id != binding.scheduler_job_id
        ):
            return "mutable scheduler id conflicts with bound acceptance"
        return None

    def _scheduler_reservation_cpus(
        self,
        owner_uid: str | None,
        spec: JobSpec,
    ) -> int:
        """Return the immutable admitted CPU weight for a possible allocation."""
        if not self._multi_user or owner_uid is None:
            return spec.cpus
        try:
            binding = _read_scheduler_submit_binding(owner_uid, spec.id)
        except (OSError, ValueError):
            # The exact admitted weight is unavailable. Charge an effectively
            # unbounded value so a finite per-owner CPU cap cannot be bypassed
            # by corrupting daemon authority plus lowering mutable spec.cpus.
            return sys.maxsize
        if binding is None and self._scheduler_transaction_may_exist(spec):
            return sys.maxsize
        return binding.cpus if binding is not None else spec.cpus

    def _fail_dispatch(
        self,
        spec: JobSpec,
        reason: str,
        *,
        emit_event: bool = True,
        exit_code: int | None = -1,
    ) -> bool:
        """Land a spec FAILED at dispatch time — lock-safe and observable.

        Used by ``_start_job``'s early-exit paths (multi-user gate reject,
        leaked cgroup scope, gid resolution, privilege-drop failure, Popen
        failure). Two jobs in one helper:

        * **EVENT-1**: the pre-v0.8.12 early-FAILED paths wrote FAILED with
          no ``STATE_TRANSITION`` event, so a dispatch failure was invisible
          in ``events.jsonl``. This emits the transition.
        * **v0.8.12 *Peterson's Lock***: takes the per-spec lock and re-reads,
          so a ``vq kill`` that raced the dispatch (writing KILLED to the
          still-PENDING / just-claimed spec) is preserved, not clobbered to
          FAILED.

        ``emit_event=False`` is used by the multi-user spec-gate reject: that
        path runs BEFORE ``workspace.mkdir`` and the spec's ``cwd`` is
        attacker-controlled and not yet validated, so writing an
        ``events.jsonl`` under it as root would be unsafe. The FAILED spec
        write (to the trusted queue dir) still happens.

        Always returns ``False`` so callers can ``return
        self._fail_dispatch(...)``.
        """
        jobid = spec.id
        spec_path = self._active_spec_path(jobid)
        with paths.spec_lock(spec_path):
            try:
                fresh, _ = self._read_active_spec(jobid, spec_path=spec_path)
            except (OSError, ValueError):
                fresh = spec
                fresh.id = jobid
            if fresh.is_terminal:
                log.info(
                    "job %s already terminal (%s); not marking FAILED at "
                    "dispatch (%s)",
                    fresh.id,
                    fresh.state,
                    reason,
                )
                return False
            prev_state = fresh.state
            fresh.state = JobState.FAILED
            fresh.finished_at = utcnow_iso()
            fresh.exit_code = exit_code
            fresh.failure_reason = reason
            fresh.write(spec_path)
            if emit_event:
                events.state_transition(
                    Path(fresh.cwd),
                    fresh.id,
                    from_state=prev_state.value,
                    to_state=JobState.FAILED.value,
                    reason=reason,
                    exit_code=exit_code,
                )
        return False

    def _persist_program_runtime_resolution(
        self,
        spec: JobSpec,
        resolved_git_sha: str | None,
    ) -> bool:
        """Attach one proven dispatch SHA without clobbering a raced spec."""
        if resolved_git_sha is None:
            return True
        spec_path = self._active_spec_path(
            spec.id,
            owner_uid=(self._job_uid.get(spec.id) if self._multi_user else None),
        )
        with paths.spec_lock(spec_path):
            try:
                fresh, _ = self._read_active_spec(
                    spec.id,
                    spec_path=spec_path,
                )
            except (OSError, ValueError):
                return False
            if (
                fresh.state != JobState.PENDING
                or fresh.program != spec.program
                or fresh.scheduler_target != spec.scheduler_target
                or _runtime_pin_without_resolution(fresh.program_runtime_pin)
                != _runtime_pin_without_resolution(spec.program_runtime_pin)
            ):
                return False
            assert fresh.program_runtime_pin is not None
            fresh.program_runtime_pin.resolved_git_sha = resolved_git_sha
            fresh.write(spec_path)
            spec.program_runtime_pin = fresh.program_runtime_pin.model_copy(
                deep=True
            )
        return True

    def _start_job(self, spec: JobSpec) -> bool:
        job_id = spec.id
        owner_uid = self._job_uid.get(job_id) if self._multi_user else None
        admitted_spec_path = self._active_spec_path(
            job_id,
            owner_uid=owner_uid,
        )
        # v0.6.35: in multi-user mode the per-user queue dir is
        # user-writable, so spec.submitter / cwd / std*_path are all
        # attacker-controlled. Vet the spec against the trusted
        # directory uid BEFORE any filesystem op below — mkdir, open,
        # and chown all run as root here. A spec that fails the gate
        # is landed FAILED rather than dispatched.
        if self._multi_user:
            reason = self._validate_multi_user_spec(spec)
            if reason is not None:
                log.error(
                    "job %s: rejected by multi-user spec gate (%s); "
                    "marking FAILED",
                    spec.id,
                    reason,
                )
                # emit_event=False: we're before workspace.mkdir and the
                # spec's cwd is attacker-controlled + unvalidated, so don't
                # write an events.jsonl under it as root.
                return self._fail_dispatch(
                    spec, f"multi-user spec gate: {reason}", emit_event=False
                )
        # The queue record, not a possibly stale scan object, owns program
        # metadata. Capture one current PENDING snapshot after the initial
        # multi-user safety gate but before resolving the runtime, so an
        # additive pin that landed after the scan cannot be skipped.
        resolution_spec_path = self._spec_path(job_id)
        with paths.spec_lock(resolution_spec_path):
            try:
                fresh, _ = self._read_active_spec(
                    job_id,
                    owner_uid=owner_uid,
                    spec_path=resolution_spec_path,
                )
            except (OSError, ValueError):
                return False
            if fresh.state != JobState.PENDING:
                return False
            spec = fresh
        runtime_resolution = _program_runtime_pin_dispatch_resolution(spec)
        if runtime_resolution.failure is not None:
            log.error(
                "job %s: rejected by program runtime pin gate (%s); "
                "marking FAILED",
                spec.id,
                runtime_resolution.failure,
            )
            return self._fail_dispatch(spec, runtime_resolution.failure)
        if not self._persist_program_runtime_resolution(
            spec,
            runtime_resolution.resolved_git_sha,
        ):
            log.info(
                "job %s changed while recording program runtime provenance; "
                "not starting",
                spec.id,
            )
            return False
        # v1.0 cluster backend (§17): a scheduler-target spec is not a local
        # Popen child. Route it to the SSH+qsub path, which skips every
        # local-process concern below (file handles, exit-marker shim, cgroup /
        # systemd-run priv-drop) -- the scheduler runs the job on the cluster.
        if spec.scheduler_target is not None:
            return self._start_scheduler_job(spec)
        workspace = Path(spec.cwd)
        workspace.mkdir(parents=True, exist_ok=True)
        stdout_fh = (workspace / spec.stdout_path).open("ab")
        stderr_fh = (workspace / spec.stderr_path).open("ab")
        # v0.5.9 exit-code marker plus direct terminal resource receipt. Wrap
        # the user command in a small POSIX collector that records the inner rc
        # to <workspace>/_vq/exit-code and resource accounting to the sibling
        # resource-usage.json on graceful exit. The marker is read by
        # _reconcile_orphans /
        # _reattach_or_interrupt_at_startup so a job that completes
        # while the daemon is down (cross-restart resume, the v0.6 admin
        # update flow) gets COMPLETED / FAILED instead of
        # ABORTED_BY_QUEUE.
        #
        # Stale marker: workspaces are jobid-keyed and freshly created
        # by submit (one job, one workspace), so a marker file can only
        # exist if the job was previously dispatched and is now being
        # restarted (resubmission semantics not yet supported but worth
        # being defensive about). Unlink before each dispatch to keep
        # the invariant "marker exists -> *this* dispatch wrote it."
        exit_marker = (workspace / EXIT_MARKER_RELPATH).resolve()
        exit_marker.parent.mkdir(parents=True, exist_ok=True)
        exit_marker.unlink(missing_ok=True)
        resource_usage = exit_marker.with_name(
            resource_receipt.RESOURCE_USAGE_BASENAME
        )
        resource_usage.unlink(missing_ok=True)
        scope_name = f"vq-job-{spec.id}"
        wrapped_inner = _build_wrapped_command(
            spec.command,
            exit_marker,
            cgroup_scope_name=(
                scope_name if self.cgroup_enabled or self._multi_user else None
            ),
        )
        # Wrap with systemd-run --user --scope when cgroup enforcement is
        # available + the spec declares any of mem_mb / cpus / wall_time.
        # No-op (returns the inner argv unchanged) when either side is
        # missing. The collector goes INSIDE systemd-run so it and the user
        # command are both accounted to the per-job cgroup.
        # MU-1 (v0.8.19): run the scope-collision pre-flight whenever a scope
        # will actually be created — i.e. when cgroup enforcement is on OR
        # we're in multi-user mode (where systemd-run --scope is MANDATORY
        # for the privilege drop, regardless of cgroup_enabled). Pre-fix the
        # gate was `cgroup_enabled` alone, so a multi-user daemon with cgroup
        # enforcement off skipped the check and then hit systemd-run's cryptic
        # "Unit already exists" on a leaked scope from a prior dispatch.
        if self.cgroup_enabled or self._multi_user:
            # v0.5.51: pre-flight scope-name collision check (audit
            # § 2e). A previous dispatch's scope unit may have leaked
            # past --collect cleanup; systemd-run would then fail
            # cryptically with "Unit already exists". Detect + try a
            # best-effort stop; if it persists, land the spec FAILED
            # with a clear reason instead of looping forever on the
            # subprocess error.
            collision = cgroup.scope_exists(
                scope_name, multi_user=self._multi_user
            )
            if collision is True:
                stopped = cgroup.stop_scope(
                    scope_name, multi_user=self._multi_user
                )
                log.warning(
                    "job %s: scope %s.scope already exists at dispatch; stop attempt %s",
                    spec.id,
                    scope_name,
                    "succeeded" if stopped else "FAILED",
                )
                if not stopped:  # noqa: SIM102 (nested form keeps the comment below clear)
                    # Re-check after the stop attempt; if STILL there,
                    # give up on this dispatch cleanly.
                    if cgroup.scope_exists(
                        scope_name, multi_user=self._multi_user
                    ) is True:
                        log.error(
                            "job %s: cannot start — scope %s.scope is "
                            "leaked and cannot be stopped; "
                            "spec marked FAILED. Operator may need to "
                            "`systemctl --user reset-failed %s.scope` "
                            "or `systemctl --user kill %s.scope` and "
                            "resubmit.",
                            spec.id,
                            scope_name,
                            scope_name,
                            scope_name,
                        )
                        stdout_fh.close()
                        stderr_fh.close()
                        return self._fail_dispatch(
                            spec,
                            f"cgroup scope {scope_name}.scope is leaked and "
                            "cannot be stopped",
                        )
        if self._multi_user:
            # v0.6.35 privilege drop: the systemd-run wrap runs the job
            # as its submitter, never as the root daemon. It is
            # MANDATORY in multi-user mode — there is no safe
            # un-wrapped path. The run-uid is the uid of the per-user
            # state directory the spec was read from (_job_uid) — the
            # trusted signal, already vetted against the user-writable
            # spec.submitter field by _validate_multi_user_spec above.
            # Only the gid lookup can still fail here.
            run_uid = int(self._job_uid[spec.id])
            run_gid = _gid_for_uid(run_uid)
            if run_gid is None:
                log.error(
                    "job %s: cannot resolve a primary gid for uid %d; "
                    "refusing to dispatch (would otherwise run as root)",
                    spec.id,
                    run_uid,
                )
                stdout_fh.close()
                stderr_fh.close()
                return self._fail_dispatch(
                    spec, f"cannot resolve a primary gid for uid {run_uid}"
                )
            # The job runs as run_uid — chown its workspace so it can
            # write the exit-code marker + output files into the tree
            # the root daemon just created.
            _chown_tree(workspace, run_uid, run_gid)
            try:
                run_command = cgroup.wrap_command(
                    wrapped_inner,
                    mem_mb=self._eff_mem_mb(spec.mem_mb),
                    cpus=spec.cpus,
                    unit_name=scope_name,
                    run_as_uid=run_uid,
                    run_as_gid=run_gid,
                )
            except RuntimeError as e:
                log.error("job %s: cannot drop privileges: %s", spec.id, e)
                stdout_fh.close()
                stderr_fh.close()
                return self._fail_dispatch(
                    spec, f"cannot drop privileges: {e}"
                )
        else:
            run_command = (
                cgroup.wrap_command(
                    wrapped_inner,
                    mem_mb=self._eff_mem_mb(spec.mem_mb),
                    cpus=spec.cpus,
                    unit_name=scope_name,
                )
                if self.cgroup_enabled
                else wrapped_inner
            )
        # v0.6.54: per-job workdir — scratch space distinct from the
        # workspace (cwd, which holds the submitted source). Lives at
        # <state>/workdirs/<jobid>/ (single-user) or
        # /var/lib/vq/users/<uid>/workdirs/<jobid>/ (multi-user).
        # Always created; the env var VQ_WORKDIR carries the path to
        # the job. Reasons for a separate directory tree:
        #
        # * Keeps long-lived chat-readable scratch (basis-opt
        #   convergence runs, vqfetch downloads, etc.) outside the
        #   per-user jobs/ tree the daemon manages aggressively.
        # * Cleanup policy can differ — workdirs are swept by an
        #   age-based pass (default 14 days), whereas workspaces are
        #   only ever archived/deleted by explicit operator action
        #   (`vq cleanup --archive` / --delete).
        # * `--clean-tmp` opt-in immediate cleanup deletes the
        #   workdir as soon as the job hits a terminal state — for
        #   jobs whose result is fully captured in stdout/stderr.
        if self._multi_user:
            workdir_path = paths.user_workdir(run_uid, spec.id)
        else:
            workdir_path = paths.workdir_for(spec.id)
        workdir_path.mkdir(parents=True, exist_ok=True)
        if self._multi_user:
            _chown_tree(workdir_path, run_uid, run_gid)
        spec.workdir = str(workdir_path.resolve())
        # v0.6.0 (audit § 2b): write spec.state=RUNNING with
        # pid=None/pgid=None BEFORE Popen so a daemon crash in the
        # narrow window between Popen.success and the post-Popen
        # spec.write doesn't leave a PENDING spec next to a running
        # process (which would cause the daemon's next dispatch loop
        # to re-dispatch the spec → DOUBLE DISPATCH). With this
        # ordering, the recovery path is:
        #   * crash before Popen: spec is RUNNING + pgid=None →
        #     existing recovery already marks ABORTED_BY_QUEUE via
        #     "no pgid recorded" branch.
        #   * crash mid-Popen-window (process exists, spec is
        #     RUNNING + pgid=None): same recovery branch, marks
        #     ABORTED_BY_QUEUE. The orphan process is unowned but
        #     within its cgroup scope; recovery logs a warning.
        #   * normal path: post-Popen spec.write fills pid/pgid/
        #     pid_start_time; orphan-reattach works as designed.
        #
        # STATE-1 (v0.8.9): re-assert PENDING immediately before the RUNNING
        # write. `vq kill` writes KILLED straight to the spec from a separate
        # process with no daemon IPC, and _dispatch_pending's pre-dispatch
        # re-read (its "in case vq kill raced us" check) happened BEFORE all
        # the setup above — workspace mkdir, log opens, exit-marker unlink,
        # cgroup-scope teardown, and (multi-user) a full-tree chown — a
        # multi-millisecond-to-seconds window. A kill landing in that window
        # used to be silently clobbered back to RUNNING here and the job was
        # launched anyway. Re-read and bail (without spawning) if it left
        # PENDING; _record_finish's is_terminal guard can't save us because
        # the KILLED would already be overwritten before Popen.
        # v0.8.12 *Peterson's Lock*: hold the per-spec lock across the
        # STATE-1 re-read and the RUNNING-claim write so a `vq kill` landing
        # in the dispatch-setup window can't slip between the re-read and the
        # write. Released before Popen below — we never hold the lock across a
        # subprocess spawn.
        with paths.spec_lock(admitted_spec_path):
            try:
                latest = (
                    _read_untrusted_multi_user_spec(admitted_spec_path)
                    if self._multi_user
                    else JobSpec.read(admitted_spec_path)
                )
            except (OSError, ValueError):
                latest = None
            if latest is None or latest.state != JobState.PENDING:
                log.info(
                    "job %s left PENDING (now %s) during dispatch setup; not starting",
                    job_id,
                    latest.state.value if latest is not None else "unreadable",
                )
                stdout_fh.close()
                stderr_fh.close()
                return False
            if self._admin_update_holds_spec(latest):
                log.info(
                    "job %s held by an admin-update marker at final dispatch "
                    "claim; leaving it PENDING",
                    job_id,
                )
                stdout_fh.close()
                stderr_fh.close()
                return False
            if self._multi_user:
                if latest.id != job_id:
                    reason = (
                        f"spec id={latest.id!r} does not match admitted job "
                        f"id={job_id!r}"
                    )
                else:
                    # The per-user record is writable while setup runs. Repeat
                    # the root-safety gate against the exact snapshot that is
                    # about to be claimed rather than trusting the earlier
                    # directory scan.
                    reason = self._validate_multi_user_spec(latest)
                if reason is not None:
                    log.error(
                        "job %s: rejected by multi-user spec gate during "
                        "local claim (%s); marking FAILED",
                        job_id,
                        reason,
                    )
                    # Keep storage identity canonical even when the raced
                    # document changed its inner id. No event is emitted:
                    # fields such as cwd are part of the untrusted reread.
                    latest.id = job_id
                    latest.state = JobState.FAILED
                    latest.finished_at = utcnow_iso()
                    latest.exit_code = -1
                    latest.failure_reason = f"multi-user spec gate: {reason}"
                    latest.write(admitted_spec_path)
                    stdout_fh.close()
                    stderr_fh.close()
                    return False
            if latest.program_runtime_pin != spec.program_runtime_pin:
                log.info(
                    "job %s changed program runtime provenance during local "
                    "dispatch setup; not starting",
                    job_id,
                )
                stdout_fh.close()
                stderr_fh.close()
                return False
            spec = latest
            spec.workdir = str(workdir_path.resolve())
            spec.state = JobState.RUNNING
            spec.pid = None
            spec.pgid = None
            spec.pid_start_time = None
            spec.started_at = utcnow_iso()
            spec.write(admitted_spec_path)
        # v0.6.52 + v0.6.54 + v0.8.7: child env injection.
        # * VQ_WORKDIR — always set (v0.6.54). Absolute path of the
        #   per-job workdir.
        # * VQ_ARRAY_INDEX / VQ_ARRAY_TOTAL / VQ_ARRAY_GROUP_ID
        #   (v0.6.52) — only for array elements.
        # * VQ_CHAIN_INDEX / VQ_CHAIN_TOTAL / VQ_CHAIN_GROUP_ID
        #   (v0.8.7) — only for chain links. NEB / DFT+U workflows
        #   read these to know "what iteration am I?" inside the
        #   script (e.g. branch on chain_index==0 to read the
        #   initial geometry vs reading the prev iter's output).
        # * VQ_PROGRAM_* (v0.12.0) — for managed venv programs, stable paths to
        #   the configured venv/bin, python, and checkout so directory payloads
        #   can call e.g. "$VQ_PROGRAM_BIN/vibe-view" without knowing each
        #   host's install layout.
        # systemd-run --scope inherits the caller's environment, so
        # all propagate through the privilege-drop wrap.
        child_env = dict(os.environ)
        _apply_default_thread_caps(child_env, spec.cpus)
        child_env.update(_vq_job_env(spec, workdir=spec.workdir))
        child_env.update(_program_env(spec))
        try:
            popen = self.dispatcher.launch(
                run_command=run_command,
                cwd=workspace,
                env=child_env,
                stdout_fh=stdout_fh,
                stderr_fh=stderr_fh,
            )
        except dispatch.DispatchError as exc:
            log.exception("failed to start job %s", spec.id)
            reason = f"failed to start: {exc}"
            # Use the already opened, admitted log descriptor. A full disk
            # must not prevent the durable FAILED transition and its reason.
            with contextlib.suppress(OSError):
                stderr_fh.write(f"vq: {reason}\n".encode("utf-8", "replace"))
                stderr_fh.flush()
            stdout_fh.close()
            stderr_fh.close()
            return self._fail_dispatch(spec, reason)
        # Capture the process identifiers OUTSIDE the lock (no spec I/O).
        pid = popen.pid
        # Capture pgid at dispatch time. start_new_session=True makes the
        # child the leader of its own process group, so pgid == pid here.
        # We store it explicitly because (a) the child may fork OMP/MPI
        # workers we want to reap together via os.killpg(pgid, ...) on
        # kill, and (b) the daemon recovery path in v0.4 will use pgid
        # to distinguish "process really gone" from "different process,
        # pid recycled."
        try:
            pgid: int | None = os.getpgid(pid)
        except ProcessLookupError:
            # Already exited between Popen and getpgid -- rare, but
            # leave pgid None and let reconciliation pick it up.
            pgid = None
        # v0.5.50: capture the PID start-time fingerprint so the
        # startup recovery path can distinguish "our process resumed"
        # from "PID was recycled to a different process". Best-effort:
        # macOS / non-Linux returns None and the fingerprint check
        # gets skipped at recovery time (falls back to pgid liveness).
        pid_start_time = _read_pid_start_time(pid)
        # v0.8.12 *Peterson's Lock*: close the post-Popen window. Between the
        # RUNNING-claim write above and this pid-fill write, the spec carried
        # pid=None / pgid=None, so a `vq kill` landing in that window wrote
        # KILLED but could NOT signal the process we had just spawned. Re-read
        # under the lock: if a terminal label was set during the window,
        # honour it — reap the just-spawned process group ourselves (SIGKILL:
        # the operator already chose to kill it, the process barely started,
        # and nothing would track it to escalate a SIGTERM later) and do NOT
        # clobber the label back to RUNNING. killpg covers the process
        # group, and _reap_scope stops the job's transient scope so any
        # descendant that escaped the group goes too (rather than waiting
        # for the next same-jobid dispatch's scope-collision pre-flight).
        with paths.spec_lock(admitted_spec_path):
            latest, _ = self._read_active_spec(
                job_id,
                owner_uid=owner_uid,
                spec_path=admitted_spec_path,
            )
            if latest.is_terminal:
                if latest.id != job_id:
                    # A racing terminal writer may also have changed the
                    # user-controlled inner id. Preserve its lifecycle verdict
                    # but never let that redirect the admitted storage/runtime
                    # identity.
                    latest.id = job_id
                    latest.write(admitted_spec_path)
                log.warning(
                    "job %s went %s during the Popen window; reaping the "
                    "just-spawned process group and preserving the label",
                    job_id,
                    latest.state.value,
                )
                if pgid is not None:
                    with contextlib.suppress(ProcessLookupError):
                        killpg(pgid, signal.SIGKILL)
                else:
                    self.dispatcher.kill(popen)
                self._reap_scope(job_id)
                if not self.dispatcher.wait(popen, timeout=5):
                    log.warning(
                        "job %s: spawned process did not exit promptly after "
                        "the Popen-window reap; leaving it for "
                        "reconciliation / scope cleanup",
                        job_id,
                    )
                stdout_fh.close()
                stderr_fh.close()
                return False
            spec.pid = pid
            spec.pgid = pgid
            spec.pid_start_time = pid_start_time
            spec.started_at = utcnow_iso()
            spec.write(admitted_spec_path)
        self._running[job_id] = _RunningJob(
            popen=popen,
            cpus=spec.cpus,
            mem_mb=spec.mem_mb,
            stdout_fh=stdout_fh,
            stderr_fh=stderr_fh,
            owner_uid=owner_uid,
            spec_path=admitted_spec_path,
        )
        # v0.6.x: track which user owns this job for quota/resolution.
        if owner_uid is not None:
            self._job_uid[job_id] = owner_uid
        # Watchdog needs a wall-clock reference for wall_time_seconds and
        # for elapsed-time stamping in samples.jsonl.
        self.watchdog.register(job_id, started_monotonic=time.monotonic())
        # v0.5.15: if persistent throttle is active, apply CPUWeight to
        # the freshly-created scope.
        # v0.5.21: also try on non-cgroup hosts via renice on the pgid;
        # apply_persistent_throttle_if_set handles both paths internally.
        # Best-effort -- a failed call leaves the new job at default
        # priority, which is the same as "no persistent throttle" behavior.
        applied = throttle.apply_persistent_throttle_if_set(
            spec.id,
            pgid=spec.pgid,
            multi_user=self._multi_user,
        )
        if applied is not None:
            log.info(
                "started job %s: applied persistent CPUWeight=%d (via %s)",
                spec.id,
                applied,
                "cgroup" if self.cgroup_enabled else "renice fallback",
            )
        log.info(
            "started job %s pid=%d pgid=%s cpus=%d mem_mb=%s",
            spec.id,
            popen.pid,
            spec.pgid,
            spec.cpus,
            "-" if spec.mem_mb is None else spec.mem_mb,
        )
        events.append_event(
            workspace,
            events.EventKind.DISPATCHED,
            spec.id,
            pid=popen.pid,
            pgid=spec.pgid,
            cpus=spec.cpus,
            mem_mb=spec.mem_mb,
        )
        events.state_transition(
            workspace,
            spec.id,
            from_state=JobState.PENDING.value,
            to_state=JobState.RUNNING.value,
        )
        return True

    # ------------------------------------------------------------------
    # v1.0 cluster backend (design doc §17): scheduler-target jobs.
    # Parallel to the local Popen path above; the local path is untouched.
    # ------------------------------------------------------------------

    def _scheduler_dispatcher_for(self, target: str) -> SchedulerDispatcher:
        """Get (build + cache) the SchedulerDispatcher for a scheduler host.

        Built lazily from THIS daemon's config the first time a job targets the
        host, so a daemon that never sees a scheduler job loads no extra state.
        Propagates ConfigError (unknown host) / SchedulerError / DialectError,
        which the caller turns into a dispatch failure.
        """
        current = _config_fingerprint()
        cached = self._scheduler_dispatchers.get(target)
        if cached is not None:
            built_at = self._scheduler_dispatcher_fingerprints.get(target, _UNSET)
            # `_UNSET` = the entry was injected rather than built from config
            # (the test seam, and any future programmatic override). Those are
            # pinned: a config change must not silently swap them out.
            if built_at is _UNSET or built_at == current:
                return cached
            log.info(
                "config changed on disk; rebuilding scheduler dispatcher for "
                "%s so new job scripts use the current config",
                target,
            )
        host_cfg = load_config().host(target)  # ConfigError if the host is unknown
        dispatcher = scheduler_dispatcher_for(host_cfg)
        self._scheduler_dispatchers[target] = dispatcher
        self._scheduler_dispatcher_fingerprints[target] = current
        return dispatcher

    def _start_scheduler_job(self, spec: JobSpec) -> bool:
        """Dispatch a scheduler-target job to its cluster via SSH+qsub (§17).

        The scheduler analogue of the local Popen path: a two-phase RUNNING
        claim (mirroring ``_start_job``) brackets the qsub so a ``vq kill``
        racing the dispatch is honoured (qdel the just-submitted job rather than
        leak it). No local process / cgroup / file handles -- the job runs on
        the cluster and is tracked in ``_scheduler_running``.
        """
        job_id = spec.id
        spec_path = self._spec_path(job_id)
        owner_uid = self._job_uid.get(job_id) if self._multi_user else None
        target = spec.scheduler_target
        assert target is not None  # the caller gates on this
        try:
            dispatcher = self._scheduler_dispatcher_for(target)
        except (ConfigError, SchedulerError, DialectError) as exc:
            if self._multi_user:
                return self._fail_scheduler_dispatch_transaction(
                    spec,
                    f"scheduler host {target!r}: {exc}",
                    owner_uid=owner_uid,
                    spec_path=self._active_spec_path(
                        job_id,
                        owner_uid=owner_uid,
                    ),
                    expected_target=target,
                    exit_code=-1,
                )
            return self._fail_dispatch(spec, f"scheduler host {target!r}: {exc}")
        if spec.retry_count > 0:
            # A retry reuses the vq id/workspace.  Clear the prior attempt's
            # proven-terminal receipt and markers while the mutable row still
            # says PENDING.  If the daemon dies after this call, another retry
            # can repeat it safely; it must never expose SUBMITTING first and
            # let startup misattribute stale acceptance evidence.
            with paths.spec_lock(spec_path):
                try:
                    retry_latest = (
                        _read_untrusted_multi_user_spec(spec_path)
                        if self._multi_user
                        else JobSpec.read(spec_path)
                    )
                except (OSError, ValueError):
                    return False
                if (
                    retry_latest.id != job_id
                    or retry_latest.state != JobState.PENDING
                    or retry_latest.scheduler_target != target
                    or retry_latest.retry_count <= 0
                ):
                    return False
                if self._multi_user:
                    reason = self._validate_multi_user_spec(retry_latest)
                    if reason is not None or owner_uid is None:
                        return False
                    try:
                        prior_binding = _read_scheduler_submit_binding(
                            owner_uid,
                            job_id,
                        )
                    except (OSError, ValueError):
                        return False
                    if _scheduler_submit_binding_may_be_open(prior_binding):
                        return False
                    if (
                        prior_binding is not None
                        and prior_binding.admitted_spec.model_dump(mode="json")
                        != retry_latest.model_dump(mode="json")
                    ):
                        return False
            try:
                dispatcher.prepare_retry_attempt(job_id)
            except (SchedulerError, DialectError) as exc:
                log.warning(
                    "job %s: could not rotate proven-terminal scheduler "
                    "evidence before retry claim: %s",
                    job_id,
                    exc,
                )
                return False
        # Phase 1: persist PENDING -> SUBMITTING before qsub. A daemon death or
        # ambiguous SSH outcome after this point must never put the spec back
        # through the PENDING dispatcher and create a duplicate scheduler job.
        with paths.spec_lock(spec_path):
            try:
                latest = (
                    _read_untrusted_multi_user_spec(spec_path)
                    if self._multi_user
                    else JobSpec.read(spec_path)
                )
            except (OSError, ValueError):
                latest = None
            if latest is None or latest.state != JobState.PENDING:
                log.info(
                    "job %s left PENDING during scheduler dispatch; not starting",
                    job_id,
                )
                return False
            if latest.scheduler_target != target:
                log.info(
                    "job %s changed scheduler target from %r to %r before "
                    "claim; deferring to the next dispatch scan",
                    job_id,
                    target,
                    latest.scheduler_target,
                )
                return False
            if latest.program_runtime_pin != spec.program_runtime_pin:
                log.info(
                    "job %s changed program runtime provenance before scheduler "
                    "claim; deferring to the next dispatch scan",
                    job_id,
                )
                return False
            if self._multi_user:
                if latest.id != job_id:
                    reason = (
                        f"spec id={latest.id!r} does not match queued job "
                        f"id={job_id!r}"
                    )
                else:
                    reason = self._validate_multi_user_spec(latest)
                if reason is not None:
                    log.error(
                        "job %s: rejected by multi-user spec gate during "
                        "scheduler claim (%s); marking FAILED",
                        job_id,
                        reason,
                    )
                    # This is the same no-event transition as _start_job's
                    # initial gate, but it must be written inline because the
                    # phase-one spec lock is already held. Canonicalize a
                    # raced inner id and always write the captured path whose
                    # lock protects this transition.
                    latest.id = job_id
                    latest.state = JobState.FAILED
                    latest.finished_at = utcnow_iso()
                    latest.exit_code = -1
                    latest.failure_reason = f"multi-user spec gate: {reason}"
                    latest.write(spec_path)
                    return False
            try:
                enforce_scheduler_wall_time_limit(
                    latest.wall_time_seconds,
                    getattr(dispatcher, "max_wall_time_seconds", None),
                    scheduler_host=target,
                    partition=getattr(dispatcher, "_queue", None),
                )
            except DialectError as exc:
                reason = str(exc)
                latest.state = JobState.FAILED
                latest.finished_at = utcnow_iso()
                latest.exit_code = None
                latest.failure_reason = reason
                latest.write(spec_path)
                # The multi-user gate above has now proved cwd/id safe. Mirror
                # _fail_dispatch's durable EVENT-1 transition while the same
                # spec lock still protects the PENDING -> FAILED write.
                events.state_transition(
                    Path(latest.cwd),
                    latest.id,
                    from_state=JobState.PENDING.value,
                    to_state=JobState.FAILED.value,
                    reason=reason,
                    exit_code=None,
                )
                return False
            spec = latest.model_copy(deep=True)
            spec.state = JobState.SUBMITTING
            spec.started_at = utcnow_iso()
            spec.scheduler_job_id = None
            spec.scheduler_state = "submitting"
            if self._multi_user:
                assert owner_uid is not None
                try:
                    existing_binding = _read_scheduler_submit_binding(
                        owner_uid,
                        spec.id,
                    )
                    if _scheduler_submit_binding_may_be_open(existing_binding):
                        self._quarantine_unconfirmed_scheduler_spec_locked(
                            spec,
                            spec_path,
                            reason=(
                                "daemon-owned scheduler submit authority already "
                                "records an open transaction"
                            ),
                            expected_target=(
                                existing_binding.scheduler_target
                                if existing_binding is not None
                                else target
                            ),
                        )
                        return False
                    if existing_binding is not None and spec.retry_count <= 0:
                        raise ValueError(
                            "closed scheduler submit authority blocks ordinary "
                            "same-id reuse"
                        )
                    if (
                        existing_binding is not None
                        and existing_binding.admitted_spec.model_dump(mode="json")
                        != latest.model_dump(mode="json")
                    ):
                        raise ValueError(
                            "closed scheduler submit authority does not match "
                            "the exact retry intent"
                        )
                    binding = _ensure_scheduler_submit_binding(
                        owner_uid,
                        spec,
                        target,
                    )
                    if spec.retry_count > 0 and binding.scheduler_job_id is not None:
                        raise ValueError(
                            "prior scheduler acceptance was not closed before retry"
                        )
                    _replace_scheduler_submit_binding_spec(
                        owner_uid,
                        spec,
                        scheduler_job_id=None,
                        allow_clear_bound_id=existing_binding is not None,
                        transaction_state="open",
                    )
                except (OSError, ValueError) as exc:
                    latest.state = JobState.FAILED
                    latest.finished_at = utcnow_iso()
                    latest.exit_code = None
                    latest.failure_reason = (
                        "scheduler submit binding could not be established: "
                        f"{exc}"
                    )
                    latest.write(spec_path)
                    return False
            spec.write(spec_path)
        # Stage + qsub OUTSIDE the lock (never hold a spec lock across SSH).
        env = _vq_job_env(spec, workdir=dispatcher.remote_workspace(spec.id))
        # Program registry paths belong to the driver host. Scheduler runtimes
        # are selected by scheduler_program_hooks and may use an unrelated
        # filesystem layout, so only portable program identity is exported.
        env.update(_program_env(spec, include_host_paths=False))
        _apply_default_thread_caps(env, spec.cpus)
        try:
            handle = dispatcher.submit(
                job_id=spec.id,
                command=spec.command,
                cpus=spec.cpus,
                scheduler_tasks=spec.scheduler_tasks,
                mem_mb=spec.mem_mb,
                wall_time_seconds=spec.wall_time_seconds,
                env=env,
                local_workspace=Path(spec.cwd),
                program=spec.program,
                retry_attempt=spec.retry_count > 0,
            )
        except SchedulerSubmitOutcomeUnknown as exc:
            log.warning(
                "job %s: scheduler submit outcome unknown on %s; preserving "
                "nonterminal reservation",
                spec.id,
                target,
            )
            self._mark_scheduler_submit_outcome_unknown(
                spec,
                reason=str(exc),
                owner_uid=owner_uid,
                spec_path=spec_path,
                expected_target=target,
            )
            try:
                parked, _ = self._read_active_spec(
                    spec.id,
                    owner_uid=owner_uid,
                    spec_path=spec_path,
                )
            except (OSError, ValueError):
                return False
            self._reconcile_unconfirmed_scheduler_submit(
                parked,
                owner_uid=owner_uid,
                spec_path=spec_path,
                expected_target=target,
            )
            return False
        except (SchedulerError, DialectError) as exc:
            log.exception("job %s: scheduler submit to %s failed", spec.id, target)
            return self._fail_scheduler_dispatch_transaction(
                spec,
                f"scheduler submit to {target} failed: {exc}",
                owner_uid=owner_uid,
                spec_path=spec_path,
                expected_target=target,
                exit_code=None,
            )
        # Phase 2: record the scheduler id; honour a kill that raced the qsub.
        # The phase-one snapshot is the authority for what was actually
        # admitted and submitted. The queue record is user-writable in
        # multi-user mode, so a fresh reread may contribute only concurrent
        # scheduler observations and a terminal lifecycle transition. It must
        # never redirect the spec write, tracker key, event path, target, or
        # submitted command/resources.
        if self._multi_user:
            assert owner_uid is not None
            try:
                _bind_scheduler_job_id(owner_uid, job_id, handle.job_id)
            except (OSError, ValueError) as exc:
                log.error(
                    "job %s: accepted scheduler allocation could not be bound "
                    "durably; retaining tracker for operator recovery: %s",
                    job_id,
                    exc,
                )
        # Direct acceptance is already authoritative. Install its immutable
        # runtime before any mutable queue or binding snapshot update so a
        # phase-two I/O failure cannot release capacity or orphan the accepted
        # allocation until a daemon restart.
        self._scheduler_running[job_id] = _SchedulerJob(
            handle=handle,
            dispatcher=dispatcher,
            cpus=spec.cpus,
            mem_mb=spec.mem_mb,
            owner_uid=owner_uid,
            spec_path=spec_path,
        )
        with paths.spec_lock(spec_path):
            try:
                latest, _ = self._read_active_spec(
                    job_id,
                    owner_uid=owner_uid,
                    spec_path=spec_path,
                )
            except (OSError, ValueError):
                # Acceptance is already proven. Reconstitute the captured
                # phase-one authority at its captured queue path so a user
                # cannot orphan the remote allocation by deleting/corrupting
                # mutable JSON during qsub.
                latest = spec.model_copy(deep=True)
                latest.failure_reason = (
                    "scheduler accepted while the queue record was unreadable; "
                    "restored from captured submit intent"
                )
            committed = spec.model_copy(deep=True)
            for field in (
                "scheduler_state",
                "scheduler_exec_host",
                "scheduler_walltime_used",
                "scheduler_walltime_limit",
                "last_heartbeat_at",
            ):
                setattr(committed, field, getattr(latest, field))
            if latest.is_terminal:
                for field in (
                    "state",
                    "finished_at",
                    "failure_reason",
                    "failure_tail",
                    "exit_code",
                    "paused_at",
                    "paused_monotonic_at",
                    "paused_seconds_total",
                    "paused_by",
                ):
                    setattr(committed, field, getattr(latest, field))
            committed.scheduler_job_id = handle.job_id
            if not committed.is_terminal:
                committed.state = JobState.RUNNING
                if committed.scheduler_state == "submitting":
                    committed.scheduler_state = None
            try:
                committed.write(spec_path)
                if self._multi_user and owner_uid is not None:
                    _replace_scheduler_submit_binding_spec(
                        owner_uid,
                        committed,
                        scheduler_job_id=handle.job_id,
                    )
            except (OSError, ValueError) as exc:
                log.error(
                    "job %s: scheduler acceptance phase-two persistence is "
                    "deferred while the accepted allocation stays tracked: %s",
                    job_id,
                    exc,
                )
                return True
            spec = committed
        if spec.is_terminal:
            log.warning(
                "job %s went %s during qsub; cancelling exact acceptance %s",
                job_id,
                spec.state.value,
                handle.job_id,
            )
            return self._cancel_recovered_terminal_scheduler_submit(
                spec,
                handle.job_id,
                dispatcher=dispatcher,
                owner_uid=owner_uid,
                spec_path=spec_path,
                expected_target=target,
            )
        workspace = Path(spec.cwd)
        log.info(
            "started job %s on scheduler %s as %s cpus=%d",
            job_id,
            target,
            handle.job_id,
            spec.cpus,
        )
        events.append_event(
            workspace,
            events.EventKind.DISPATCHED,
            job_id,
            scheduler_target=target,
            scheduler_job_id=handle.job_id,
            cpus=spec.cpus,
            scheduler_tasks=spec.scheduler_tasks,
        )
        events.state_transition(
            workspace,
            job_id,
            from_state=JobState.SUBMITTING.value,
            to_state=JobState.RUNNING.value,
        )
        return True

    def _fail_scheduler_dispatch_transaction(
        self,
        admitted: JobSpec,
        reason: str,
        *,
        owner_uid: str | None,
        spec_path: Path,
        expected_target: str,
        exit_code: int | None,
    ) -> bool:
        """Land a proven pre-acceptance failure through captured safe bytes."""
        preserved_terminal = False
        binding_closed = False
        binding_exists = False
        if self._multi_user and owner_uid is not None:
            try:
                binding_exists = (
                    _read_scheduler_submit_binding(owner_uid, admitted.id)
                    is not None
                )
            except (OSError, ValueError):
                log.exception(
                    "job %s: scheduler submit binding is unreadable while "
                    "landing a proven rejection",
                    admitted.id,
                )
                return False
        with paths.spec_lock(spec_path):
            try:
                fresh, _ = self._read_active_spec(
                    admitted.id,
                    owner_uid=owner_uid,
                    spec_path=spec_path,
                )
            except (OSError, ValueError):
                fresh = None
            if fresh is not None and fresh.is_terminal:
                preserved_terminal = True
                if binding_exists and owner_uid is not None:
                    try:
                        _close_scheduler_submit_binding(owner_uid, fresh)
                    except (OSError, ValueError):
                        log.exception(
                            "job %s: could not durably close rejected scheduler "
                            "transaction before exposing terminal state",
                            admitted.id,
                        )
                        return False
                    binding_closed = True
            else:
                committed = admitted.model_copy(deep=True)
                previous = committed.state
                committed.state = JobState.FAILED
                committed.scheduler_target = expected_target
                committed.scheduler_state = "submit_rejected"
                committed.scheduler_job_id = None
                committed.finished_at = utcnow_iso()
                committed.exit_code = exit_code
                committed.failure_reason = reason
                if binding_exists and owner_uid is not None:
                    try:
                        _close_scheduler_submit_binding(owner_uid, committed)
                    except (OSError, ValueError):
                        log.exception(
                            "job %s: could not durably close rejected scheduler "
                            "transaction before exposing terminal state",
                            admitted.id,
                        )
                        return False
                    binding_closed = True
                committed.write(spec_path)
                events.state_transition(
                    Path(admitted.cwd),
                    admitted.id,
                    from_state=previous.value,
                    to_state=JobState.FAILED.value,
                    reason=reason,
                    exit_code=exit_code,
                    scheduler_target=expected_target,
                )
        if binding_closed and owner_uid is not None:
            try:
                _remove_scheduler_submit_binding(owner_uid, admitted.id)
            except (OSError, ValueError):
                log.exception(
                    "job %s: failed to close/remove scheduler submit binding",
                    admitted.id,
                )
        if preserved_terminal:
            log.info(
                "job %s: scheduler rejection preserved a racing terminal state",
                admitted.id,
            )
        return False

    def _mark_scheduler_submit_outcome_unknown(
        self,
        spec: JobSpec,
        *,
        reason: str,
        owner_uid: str | None = None,
        spec_path: Path | None = None,
        expected_target: str | None = None,
        durable_state: str = "submit_outcome_unknown",
    ) -> None:
        """Durably park an ambiguous scheduler submit without replaying it."""
        if durable_state not in {
            "submit_outcome_unknown",
            "submit_evidence_conflict",
        }:
            raise ValueError("invalid scheduler ambiguity state")
        jobid = spec.id
        admitted_path = self._active_spec_path(
            jobid,
            owner_uid=owner_uid,
            spec_path=spec_path,
        )
        with paths.spec_lock(admitted_path):
            try:
                fresh, _ = self._read_active_spec(
                    jobid,
                    owner_uid=owner_uid,
                    spec_path=admitted_path,
                )
            except (OSError, ValueError):
                return
            if self._multi_user:
                authority_error = self._multi_user_scheduler_authority_error(
                    fresh,
                    owner_uid=owner_uid,
                    expected_target=expected_target,
                )
                if authority_error is not None:
                    self._quarantine_unconfirmed_scheduler_spec_locked(
                        fresh,
                        admitted_path,
                        reason=authority_error,
                        expected_target=expected_target,
                    )
                    return
            if fresh.scheduler_target is None:
                return
            if fresh.is_terminal:
                fresh.scheduler_state = (
                    "submit_evidence_conflict_after_terminal"
                    if durable_state == "submit_evidence_conflict"
                    else "submit_outcome_unknown_after_terminal"
                )
                fresh.last_heartbeat_at = utcnow_iso()
                fresh.failure_reason = reason
                fresh.write(admitted_path)
                if self._multi_user and owner_uid is not None:
                    _replace_scheduler_submit_binding_spec(owner_uid, fresh)
                return
            previous = fresh.state
            transitioned = (
                previous != JobState.SUBMIT_OUTCOME_UNKNOWN
                or fresh.scheduler_state != durable_state
            )
            fresh.state = JobState.SUBMIT_OUTCOME_UNKNOWN
            fresh.scheduler_state = durable_state
            fresh.last_heartbeat_at = utcnow_iso()
            fresh.failure_reason = reason
            fresh.write(admitted_path)
            if self._multi_user and owner_uid is not None:
                _replace_scheduler_submit_binding_spec(owner_uid, fresh)
            if transitioned:
                events.state_transition(
                    Path(fresh.cwd),
                    jobid,
                    from_state=previous.value,
                    to_state=JobState.SUBMIT_OUTCOME_UNKNOWN.value,
                    reason=(
                        "scheduler submit outcome is unknown; capacity remains "
                        "reserved and automatic replay is disabled"
                    ),
                    scheduler_target=fresh.scheduler_target,
                    submit_error=_safe_scheduler_poll_diagnostic(reason),
                )

    def _scheduler_fetch_capacity_available(self) -> bool:
        return self._scheduler_fetch_admissions_left != 0 and sum(
            not flight.done.is_set()
            for flight in self._scheduler_fetch_flights.values()
        ) < _SCHEDULER_FETCH_CONCURRENCY

    def _fetch_scheduler_results(
        self,
        jobid: str,
        sj: _SchedulerJob,
        spec: JobSpec,
        *,
        log_failure: Callable[[SchedulerError], None],
        log_unavailable: Callable[[int], None] | None = None,
    ) -> _SchedulerFetchOutcome:
        """Fetch one finished scheduler workspace with bounded retries.

        This is the shared effectful transaction for the two finished-job
        branches in :meth:`_reconcile_scheduler`. The remote/filesystem I/O is
        deliberately unlocked and uses the caller's existing ``spec.cwd``
        snapshot. In the production loop at most two transfer/telemetry workers
        perform that I/O; only this main-loop consumer changes counters or job
        state. Waiting for a worker is not a failed attempt. A successful fetch
        resets the in-memory consecutive-failure
        counter. A caught :class:`SchedulerError` increments it, stamps the
        durable retry state below the bound, or stamps artifacts unavailable
        at the bound. Unexpected exceptions still escape.

        The callbacks keep branch-specific logging, including its ordering
        relative to the status stamps, with the caller. Marker authority,
        grace, local marker reads, walltime precedence, and terminalization do
        not belong to this helper.
        """
        dispatcher, handle = sj.dispatcher, sj.handle

        def transfer() -> bool:
            dispatcher.fetch_results(handle, workspace)
            record_resource_sample = getattr(
                dispatcher,
                "record_terminal_resource_sample",
                None,
            )
            return (
                record_resource_sample(handle, workspace)
                if callable(record_resource_sample)
                else True
            )

        workspace = Path(spec.cwd)
        try:
            if self._background_scheduler_polling:
                flight = self._scheduler_fetch_flights.get(jobid)
                if flight is None:
                    if not self._scheduler_fetch_capacity_available():
                        return _SchedulerFetchOutcome.RETRY
                    flight = _SchedulerFetchFlight(
                        sj, workspace, threading.Event(), dispatcher, handle,
                    )
                    self._scheduler_fetch_flights[jobid] = flight

                    def collect() -> None:
                        try:
                            flight.telemetry_complete = transfer()
                        except BaseException as exc:  # noqa: BLE001 - consumed on main loop
                            flight.error = exc
                        finally:
                            flight.done.set()
                            self._scheduler_refresh_wakeup.set()

                    try:
                        threading.Thread(
                            target=collect, name="vq-scheduler-fetch", daemon=True,
                        ).start()
                    except BaseException:
                        self._scheduler_fetch_flights.pop(jobid, None)
                        raise
                    if self._scheduler_fetch_admissions_left is not None:
                        self._scheduler_fetch_admissions_left -= 1
                    self._scheduler_fetch_order.pop(dispatcher, None)
                    self._scheduler_fetch_order[dispatcher] = None
                    return _SchedulerFetchOutcome.RETRY
                if not flight.done.is_set():
                    return _SchedulerFetchOutcome.RETRY
                self._scheduler_fetch_flights.pop(jobid, None)
                if (
                    flight.job is not sj or flight.workspace != workspace
                    or flight.dispatcher is not dispatcher or flight.handle != handle
                ):
                    # A replacement runtime/workspace cannot inherit evidence
                    # from the old binding, even if it reused the same bare id.
                    return _SchedulerFetchOutcome.RETRY
                if flight.error is not None:
                    raise flight.error
                telemetry_complete = flight.telemetry_complete
            else:
                telemetry_complete = transfer()
        except SchedulerError as exc:
            log_failure(exc)
            sj.fetch_failure_misses += 1
            error = str(exc)
            if sj.fetch_failure_misses < SCHEDULER_FETCH_FAILURE_LIMIT:
                self._stamp_scheduler_fetch_failed(
                    jobid,
                    sj,
                    spec,
                    reason=error,
                )
                return _SchedulerFetchOutcome.RETRY
            if log_unavailable is not None:
                log_unavailable(sj.fetch_failure_misses)
            self._stamp_scheduler_artifacts_unavailable(jobid, reason=error)
            return _SchedulerFetchOutcome.UNAVAILABLE
        sj.fetch_failure_misses = 0
        if telemetry_complete is False:
            # Telemetry is intentionally downstream of the authoritative
            # terminal phase, durable exit marker, and successful artifact
            # fetch.  sacct can lag for minutes; keeping the job RUNNING until
            # it catches up regresses one-poll terminal recognition, reserves
            # capacity indefinitely across daemon restarts, and misstates the
            # scheduler lifecycle.  The sample writer has already persisted an
            # explicit partial/unavailable reason and remains idempotently
            # retryable by a later terminal-fenced collection.
            log.warning(
                "job %s: scheduler resource accounting is not yet complete; "
                "preserving explicit telemetry status without delaying the "
                "authoritative terminal transition",
                jobid,
            )
        sj.accounting_failure_misses = 0
        return _SchedulerFetchOutcome.FETCHED

    def _observe_scheduler_dispatcher(
        self,
        dispatcher: SchedulerDispatcher,
        items: tuple[tuple[str, _SchedulerJob], ...],
        *,
        refresh_sequence: int,
    ) -> _SchedulerPollObservation:
        """Perform read-only live/detail polls for one scheduler host."""
        handles = [sj.handle for _, sj in items]
        attempted_at = utcnow_iso()
        phases: dict[str, SchedulerPhase] = {}
        explicitly_absent_job_ids: frozenset[str] = frozenset()
        queued_reasons: dict[str, str] = {}
        poll_error: (
            SchedulerError
            | DialectError
            | OSError
            | subprocess.SubprocessError
            | None
        ) = None
        try:
            poll_with_evidence = getattr(dispatcher, "poll_with_evidence", None)
            if callable(poll_with_evidence):
                evidence: SchedulerPollEvidence = poll_with_evidence(handles)
                phases = dict(evidence.phases)
                explicitly_absent_job_ids = frozenset(
                    evidence.explicitly_absent_job_ids
                )
                queued_reasons = dict(getattr(evidence, "queued_reasons", {}))
            else:
                phases = dispatcher.poll(handles)
        except (
            SchedulerError,
            DialectError,
            OSError,
            subprocess.SubprocessError,
        ) as exc:
            poll_error = exc

        accounting_error: (
            SchedulerError
            | DialectError
            | OSError
            | subprocess.SubprocessError
            | None
        ) = None
        try:
            details = dispatcher.poll_detail(handles)
        except (
            SchedulerError,
            DialectError,
            OSError,
            subprocess.SubprocessError,
        ) as exc:
            accounting_error = exc
            details = {}
        return _SchedulerPollObservation(
            attempted_at=attempted_at,
            refresh_sequence=refresh_sequence,
            phases=phases,
            explicitly_absent_job_ids=explicitly_absent_job_ids,
            details=details,
            queued_reasons=queued_reasons,
            poll_error=poll_error,
            accounting_error=accounting_error,
        )

    def _start_scheduler_poll_flight(
        self,
        dispatcher: SchedulerDispatcher,
        items: list[tuple[str, _SchedulerJob]],
    ) -> None:
        """Start at most one bounded, read-only observation for one host."""
        if dispatcher in self._scheduler_poll_flights:
            return
        with self._scheduler_refresh_condition:
            refresh_sequence = self._scheduler_refresh_requested
        flight = _SchedulerPollFlight(
            items=tuple(items),
            refresh_sequence=refresh_sequence,
            done=threading.Event(),
        )
        self._scheduler_poll_flights[dispatcher] = flight

        def observe() -> None:
            try:
                flight.observation = self._observe_scheduler_dispatcher(
                    dispatcher,
                    flight.items,
                    refresh_sequence=flight.refresh_sequence,
                )
            except BaseException as exc:  # noqa: BLE001 - replay on main loop
                flight.error = exc
            finally:
                flight.done.set()
                self._scheduler_refresh_wakeup.set()

        try:
            threading.Thread(
                target=observe,
                name="vq-scheduler-poll",
                daemon=True,
            ).start()
        except BaseException:
            self._scheduler_poll_flights.pop(dispatcher, None)
            raise

    def _reconcile_scheduler(self) -> None:
        """Run one main-loop-owned scheduler poll and acknowledge refreshes."""
        with self._scheduler_refresh_condition:
            # A request that arrives after this snapshot must wait for the
            # next pass; otherwise a nearly-complete old poll could be
            # mislabeled as fresh evidence.
            refresh_target = self._scheduler_refresh_requested
        observed_jobs: dict[str, tuple[str, int]] = {}
        if self._background_scheduler_polling:
            # A worker can finish just after a waiting group's turn. Reserve
            # that newly free slot for the next pass, when the waiter is first,
            # instead of letting later groups take it repeatedly in this pass.
            self._scheduler_fetch_admissions_left = max(
                0, _SCHEDULER_FETCH_CONCURRENCY - sum(
                    not flight.done.is_set()
                    for flight in self._scheduler_fetch_flights.values()
                ),
            )
        try:
            observed_jobs = self._reconcile_scheduler_pass()
        finally:
            self._scheduler_fetch_admissions_left = None
            if refresh_target:
                with self._scheduler_refresh_condition:
                    ready = [
                        (request_id, jobid)
                        for request_id, jobid in self._scheduler_refresh_requests.items()
                        if request_id <= refresh_target
                    ]
                    for request_id, jobid in ready:
                        observation = observed_jobs.get(jobid)
                        observation_is_fresh = (
                            observation is not None
                            and observation[1] >= request_id
                        )
                        if (
                            not observation_is_fresh
                            and self._background_scheduler_polling
                            and jobid in self._scheduler_running
                        ):
                            # A production observation may still be in flight,
                            # or the completed flight began before this request.
                            # Leave the request pending for the next host-local
                            # flight; its own RPC deadline remains authoritative.
                            continue
                        if not observation_is_fresh:
                            result: dict[str, object] = {
                                "schema": "vq.scheduler.status_refresh/1",
                                "completed": False,
                                "observed_at": None,
                                "reason": "job_not_observed",
                            }
                        else:
                            assert observation is not None
                            observed_at = observation[0]
                            result = {
                                "schema": "vq.scheduler.status_refresh/1",
                                "completed": True,
                                "observed_at": observed_at,
                            }
                        self._scheduler_refresh_results[request_id] = result
                        del self._scheduler_refresh_requests[request_id]
                    self._scheduler_refresh_condition.notify_all()

    def _reconcile_scheduler_pass(self) -> dict[str, tuple[str, int]]:
        """Poll cluster jobs, reap terminals, honour external kills (§17).

        ONE batched live poll plus ONE batched accounting poll per target host
        (design doc §4, never per-job). On Slurm, absence from ``squeue`` is
        terminal only when ``sacct -X`` supplies a terminal record; an
        unavailable or ambiguous observation remains live/unknown. Terminal
        proof then enters the existing exit-marker and final-fetch fence. An
        external ``vq kill`` (spec terminal while the job is still listed) is
        escalated to ``qdel``.
        """
        observed_jobs: dict[str, tuple[str, int]] = {}
        for jobid, flight in list(self._scheduler_fetch_flights.items()):
            if (
                flight.done.is_set()
                and self._scheduler_running.get(jobid) is not flight.job
            ):
                self._scheduler_fetch_flights.pop(jobid, None)
        if not self._scheduler_running:
            self._scheduler_fetch_order.clear()
            for dispatcher, flight in list(self._scheduler_poll_flights.items()):
                if flight.done.is_set():
                    self._scheduler_poll_flights.pop(dispatcher, None)
            return observed_jobs
        by_dispatcher: dict[SchedulerDispatcher, list[tuple[str, _SchedulerJob]]] = {}
        for jobid, sj in self._scheduler_running.items():
            by_dispatcher.setdefault(sj.dispatcher, []).append((jobid, sj))
        finished: list[str] = []
        for dispatcher in set(self._scheduler_poll_flights) - set(by_dispatcher):
            flight = self._scheduler_poll_flights[dispatcher]
            if flight.done.is_set():
                self._scheduler_poll_flights.pop(dispatcher, None)
        if self._background_scheduler_polling:
            self._scheduler_fetch_order = {
                dispatcher: None for dispatcher in self._scheduler_fetch_order
                if dispatcher in by_dispatcher
            }
            for dispatcher in by_dispatcher:
                self._scheduler_fetch_order.setdefault(dispatcher, None)
            dispatchers = list(self._scheduler_fetch_order)
        else:
            dispatchers = list(by_dispatcher)
        for dispatcher in dispatchers:
            current_items = by_dispatcher[dispatcher]
            if self._background_scheduler_polling:
                flight = self._scheduler_poll_flights.get(dispatcher)
                if flight is None:
                    self._start_scheduler_poll_flight(dispatcher, current_items)
                    continue
                if not flight.done.is_set():
                    continue
                self._scheduler_poll_flights.pop(dispatcher, None)
                if flight.error is not None:
                    raise flight.error
                if flight.observation is None:
                    raise RuntimeError("scheduler poll flight completed without a result")
                observation = flight.observation
                items = list(flight.items)
            else:
                with self._scheduler_refresh_condition:
                    refresh_sequence = self._scheduler_refresh_requested
                items = current_items
                observation = self._observe_scheduler_dispatcher(
                    dispatcher,
                    tuple(items),
                    refresh_sequence=refresh_sequence,
                )
            attempted_at = observation.attempted_at
            phases = observation.phases
            explicitly_absent_job_ids = observation.explicitly_absent_job_ids
            details = observation.details
            queued_reasons = observation.queued_reasons
            poll_error = observation.poll_error
            accounting_error = observation.accounting_error
            if poll_error is not None:
                poll_reason = _safe_scheduler_poll_diagnostic(poll_error)
                log.warning(
                    "scheduler poll failed for %d job(s): %s",
                    len(items),
                    poll_reason,
                )
            if accounting_error is not None:
                accounting_reason = _safe_scheduler_poll_diagnostic(
                    accounting_error
                )
                log.debug("scheduler detail poll failed: %s", accounting_reason)
            # A failed live poll is an unknown observation even if accounting
            # happens to have a terminal row: without squeue, liveness has not
            # been disproved. Both batched calls still run so host telemetry is
            # prompt and consistent.
            if poll_error is not None:
                for jobid, sj in items:
                    self._stamp_scheduler_poll_failed(
                        jobid,
                        sj,
                        reason=poll_reason,
                        attempted_at=attempted_at,
                    )
                continue
            accounting_required = bool(
                getattr(dispatcher, "accounting_required_for_absent", False)
            )
            for jobid, sj in items:
                try:
                    spec, spec_path = self._read_active_spec(jobid, sj)
                except (OSError, ValueError):
                    continue
                # External `vq kill`: spec terminal but the job is still listed.
                if spec.is_terminal and not sj.term_qdeled:
                    if not self._multi_user:
                        # Persist ownership before qdel.  Scheduler acceptance
                        # of a cancellation request does not prove immediate
                        # disappearance, so this sentinel remains until a live
                        # poll observes the exact allocation terminal/absent.
                        with paths.spec_lock(spec_path):
                            try:
                                cancel_pending, _ = self._read_active_spec(
                                    jobid,
                                    sj,
                                    spec_path=spec_path,
                                )
                            except (OSError, ValueError):
                                continue
                            if not cancel_pending.is_terminal:
                                continue
                            cancel_pending.scheduler_job_id = sj.handle.job_id
                            cancel_pending.scheduler_state = (
                                "submit_cancel_pending_after_terminal"
                            )
                            cancel_pending.last_heartbeat_at = utcnow_iso()
                            cancel_pending.write(spec_path)
                            spec = cancel_pending
                    log.info(
                        "job %s: spec terminal (%s); qdel %s",
                        jobid,
                        spec.state.value,
                        sj.handle.job_id,
                    )
                    try:
                        dispatcher.cancel(sj.handle)
                    except (SchedulerError, DialectError):
                        log.exception(
                            "job %s: scheduler cancellation is not yet "
                            "confirmed; retaining durable ownership",
                            jobid,
                        )
                        continue
                    sj.term_qdeled = True
                detail = details.get(sj.handle.job_id)
                phase = phases.get(sj.handle.job_id, SchedulerPhase.FINISHED)
                explicitly_absent = (
                    sj.handle.job_id in explicitly_absent_job_ids
                )
                if explicitly_absent and not sj.explicit_absence_reported:
                    log.warning(
                        "job %s: scheduler explicitly reports handle %s "
                        "unknown; entering terminal marker/fetch reconciliation",
                        jobid,
                        sj.handle.job_id,
                    )
                    sj.explicit_absence_reported = True
                if phase is SchedulerPhase.FINISHED and accounting_required:
                    if detail is None and explicitly_absent:
                        # Slurm affirmatively rejected this exact missing
                        # handle. That is live-queue absence proof even when
                        # its accounting row has already aged out; the exit
                        # marker/fetch fence below still owns final status.
                        pass
                    elif accounting_error is not None:
                        self._stamp_scheduler_poll_failed(
                            jobid,
                            sj,
                            reason=accounting_reason,
                            attempted_at=attempted_at,
                        )
                        continue
                    elif detail is None:
                        self._stamp_scheduler_poll_failed(
                            jobid,
                            sj,
                            reason=(
                                "accounting record unavailable for scheduler job "
                                f"{sj.handle.job_id}"
                            ),
                            attempted_at=attempted_at,
                        )
                        continue
                    else:
                        try:
                            detail_phase = dispatcher.phase_from_detail(detail)
                        except (SchedulerError, DialectError) as exc:
                            log.warning(
                                "job %s: scheduler detail state %r could not be "
                                "mapped: %s",
                                jobid,
                                detail.raw_state,
                                exc,
                            )
                            self._stamp_scheduler_poll_failed(
                                jobid,
                                sj,
                                reason=str(exc),
                                attempted_at=attempted_at,
                            )
                            continue
                        if detail_phase is None:
                            self._stamp_scheduler_poll_failed(
                                jobid,
                                sj,
                                reason=(
                                    "accounting state unavailable for scheduler job "
                                    f"{sj.handle.job_id}"
                                ),
                                attempted_at=attempted_at,
                            )
                            continue
                        if detail_phase is not SchedulerPhase.FINISHED:
                            log.warning(
                                "job %s: coarse scheduler poll reported finished, "
                                "but accounting reports state %s; keeping job "
                                "live as %s",
                                jobid,
                                detail.raw_state,
                                detail_phase.name.lower(),
                            )
                            phase = detail_phase
                elif phase is SchedulerPhase.FINISHED and detail is not None:
                    # Torque compatibility: qstat absence remains terminal, but
                    # a surviving qstat -f live record wins when available.
                    try:
                        detail_phase = dispatcher.phase_from_detail(detail)
                    except (SchedulerError, DialectError) as exc:
                        log.warning(
                            "job %s: scheduler detail state %r could not be "
                            "mapped: %s",
                            jobid,
                            detail.raw_state,
                            exc,
                        )
                        detail_phase = None
                    if (
                        detail_phase is not None
                        and detail_phase is not SchedulerPhase.FINISHED
                    ):
                        phase = detail_phase
                observed_at = self._stamp_scheduler_poll_succeeded(
                    jobid,
                    sj,
                    attempted_at=attempted_at,
                )
                if observed_at is not None:
                    observed_jobs[jobid] = (
                        observed_at,
                        observation.refresh_sequence,
                    )
                if phase is not SchedulerPhase.FINISHED:
                    sj.explicit_absence_reported = False
                    sj.finished_without_marker_since = None
                    sj.finished_without_marker_misses = 0
                    self._stamp_scheduler_status(
                        jobid,
                        sj,
                        spec,
                        phase,
                        detail,
                        queued_reason=queued_reasons.get(sj.handle.job_id),
                    )
                    continue
                # Scheduler termination is known even when both artifact
                # workers are occupied. Keep ownership and all marker/fetch
                # fences, but do not leave an old running/unpolled phase on
                # display. A successful poll does not clear a fetch failure.
                if spec.scheduler_state not in {
                    "fetch_failed", "marker_probe_failed", "artifacts_unavailable",
                }:
                    self._stamp_scheduler_finishing(jobid, sj, spec)
                if self._background_scheduler_polling:
                    fetch = self._scheduler_fetch_flights.get(jobid)
                    if (
                        fetch is not None and not fetch.done.is_set()
                        or fetch is None
                        and not self._scheduler_fetch_capacity_available()
                    ):
                        # Do not serially re-read hundreds of remote exit
                        # markers while the bounded transfer workers are busy.
                        # The scheduler reservation remains held until the
                        # unchanged terminal evidence path consumes a result.
                        continue
                # qstat no longer lists the job. The generated scheduler wrapper
                # writes the exit-marker only after command exit and, for
                # node-local scratch, after copy-back to the shared workspace.
                # Use that marker as the fetch fence; otherwise a fast job can
                # be fetched while outputs are still appearing on the cluster FS.
                # vq arrays are independent scheduler jobs and workspaces.
                # Only a native scheduler-array handle owns suffixed artifacts.
                artifact_index = (
                    spec.array_index if sj.handle.array_size is not None else None
                )
                # #414: the terminal accounting verdict, resolved once per
                # reap. A scheduler-attributed abnormal end (OUT_OF_MEMORY /
                # CANCELLED / TIMEOUT / ...) must survive into the terminal
                # classification below no matter which rc source resolves —
                # the incident jobs were OOM-killed with a marker that read 0
                # and were reaped completed/exit-0.
                abnormal_state = (
                    dispatcher.abnormal_termination_from_detail(detail)
                    if detail is not None
                    else None
                )
                try:
                    rc = dispatcher.exit_marker_code(
                        sj.handle, array_index=artifact_index
                    )
                except SchedulerError as exc:
                    log.warning("job %s: exit-marker probe failed: %s", jobid, exc)
                    self._stamp_scheduler_marker_probe_failed(
                        jobid,
                        sj,
                        spec,
                        artifact_index=artifact_index,
                        reason=str(exc),
                    )
                    continue

                if rc is None:
                    if spec.is_terminal:
                        # External kill + no marker -> label already set; drop
                        # it once the scheduler agrees the job is gone.
                        finished.append(jobid)
                        continue
                    now = time.monotonic()
                    if sj.finished_without_marker_since is None:
                        sj.finished_without_marker_since = now
                    sj.finished_without_marker_misses += 1
                    elapsed = now - sj.finished_without_marker_since
                    self._stamp_scheduler_finishing(jobid, sj, spec)
                    if elapsed < SCHEDULER_FINISHED_MARKER_GRACE_SECONDS:
                        log.info(
                            "job %s: scheduler finished but exit-marker is not "
                            "visible yet; waiting %.1fs more (miss %d)",
                            jobid,
                            SCHEDULER_FINISHED_MARKER_GRACE_SECONDS - elapsed,
                            sj.finished_without_marker_misses,
                        )
                        continue
                    # Gone with no marker after the visibility grace. Before
                    # terminal classification, make one final whole-workspace
                    # fetch and read the marker locally. This catches PBS/NFS
                    # lag where `cat <remote marker>` missed a just-written
                    # marker, or the marker only became visible through the
                    # fetched tarball. If the marker is still absent, the user
                    # still gets every artifact the remote workspace held.
                    # Same bound as the marker-visible path below. Without it a
                    # gone workspace parks the job forever here too: the abort
                    # classification underneath is never reached, so the row
                    # stays non-terminal on a job the scheduler has already
                    # forgotten. A terminal Slurm accounting row can supply a
                    # secondary rc, but only after this grace + final-fetch
                    # fence has exhausted every marker source.
                    fetch_outcome = self._fetch_scheduler_results(
                        jobid,
                        sj,
                        spec,
                        log_failure=lambda exc, jobid=jobid: log.warning(
                            "job %s: final fetch before missing-marker abort "
                            "failed: %s",
                            jobid,
                            exc,
                        ),
                    )
                    if fetch_outcome is _SchedulerFetchOutcome.RETRY:
                        continue
                    local_rc = _read_exit_marker(
                        Path(spec.cwd), array_index=artifact_index
                    )
                    if local_rc is not None:
                        log.info(
                            "job %s: recovered exit-marker rc=%d after final "
                            "workspace fetch",
                            jobid,
                            local_rc,
                        )
                        if abnormal_state is not None and detail is not None:
                            self._mark_scheduler_abnormal_end(
                                spec,
                                abnormal_state=abnormal_state,
                                detail=detail,
                                marker_rc=local_rc,
                                jobid=jobid,
                                owner_uid=sj.owner_uid,
                                spec_path=spec_path,
                            )
                        self._record_finish(jobid, local_rc)
                        finished.append(jobid)
                        continue
                    accounting_rc = (
                        detail.exit_code
                        if accounting_required and detail is not None
                        else None
                    )
                    # Gone with no marker and no external terminal after the
                    # marker-visibility grace + final fetch: the queue ended it
                    # for reasons we can't see (SIGKILL, node loss, lost
                    # workspace), or the wrapper never reached marker write.
                    evidence: dict[str, object] = {
                        "scheduler_target": spec.scheduler_target,
                        "missing_marker_grace_seconds": (
                            SCHEDULER_FINISHED_MARKER_GRACE_SECONDS
                        ),
                        "missing_marker_misses": sj.finished_without_marker_misses,
                    }
                    if accounting_rc is not None:
                        evidence["scheduler_accounting_exit_code"] = accounting_rc
                    try:
                        evidence.update(
                            dispatcher.missing_marker_diagnostics(
                                sj.handle, array_index=artifact_index
                            )
                        )
                    except Exception as exc:  # noqa: BLE001 - diagnostic path
                        evidence["remote_diagnostics_error"] = str(exc)
                    evidence.update(
                        _local_missing_marker_diagnostics(
                            Path(spec.cwd), spec, array_index=artifact_index
                        )
                    )
                    walltime_exceeded, walltime_used, walltime_limit = (
                        _scheduler_walltime_exceeded(spec, detail, evidence)
                    )
                    if walltime_exceeded:
                        evidence["scheduler_walltime_exceeded"] = True
                        if walltime_used is not None:
                            evidence["scheduler_walltime_used"] = walltime_used
                        if walltime_limit is not None:
                            evidence["scheduler_walltime_limit"] = walltime_limit
                        self._mark_scheduler_time_exceeded(
                            spec,
                            reason=_scheduler_walltime_reason(
                                used=walltime_used,
                                limit=walltime_limit,
                                missing_marker=True,
                            ),
                            evidence=evidence,
                            terminal_side_effects=accounting_rc is None,
                            jobid=jobid,
                            owner_uid=sj.owner_uid,
                            spec_path=spec_path,
                        )
                        if accounting_rc is not None:
                            self._record_finish(jobid, accounting_rc)
                    elif accounting_rc is not None:
                        log.info(
                            "job %s: marker absent after final fetch; reaping "
                            "from terminal scheduler accounting rc=%d",
                            jobid,
                            accounting_rc,
                        )
                        if abnormal_state is not None and detail is not None:
                            self._mark_scheduler_abnormal_end(
                                spec,
                                abnormal_state=abnormal_state,
                                detail=detail,
                                marker_rc=None,
                                jobid=jobid,
                                owner_uid=sj.owner_uid,
                                spec_path=spec_path,
                            )
                        self._record_finish(jobid, accounting_rc)
                    else:
                        self._mark_aborted_by_queue(
                            spec,
                            reason=(
                                "scheduler job finished without an exit-marker "
                                f"after {SCHEDULER_FINISHED_MARKER_GRACE_SECONDS:.0f}s "
                                "marker-visibility grace and final workspace fetch"
                            ),
                            evidence=evidence,
                            process_exit_confirmed=True,
                            jobid=jobid,
                            owner_uid=sj.owner_uid,
                            spec_path=spec_path,
                        )
                    finished.append(jobid)
                    continue

                # Marker visible: bring the whole workspace home, then reap by
                # the remote marker rc. fetch_results is deliberately after the
                # marker read so the marker fences output copy-back on pbs-cluster.
                # The job has already left the scheduler and its rc is in hand.
                # Retry a bounded number of times for a transient transport
                # failure, then classify anyway: a workspace that is gone will
                # never archive, and parking forever throws away an authoritative
                # exit code over an unrelated failure. Terminal state and artifact
                # retrieval are independent facts and are no longer conflated.
                fetch_outcome = self._fetch_scheduler_results(
                    jobid,
                    sj,
                    spec,
                    log_failure=lambda exc, jobid=jobid: log.warning(
                        "job %s: fetch_results failed: %s",
                        jobid,
                        exc,
                    ),
                    log_unavailable=lambda misses, jobid=jobid, rc=rc: log.warning(
                        "job %s: %d consecutive fetch failures after the job "
                        "left the scheduler; reaping on the durable exit "
                        "marker rc=%d without its workspace",
                        jobid,
                        misses,
                        rc,
                    ),
                )
                if fetch_outcome is _SchedulerFetchOutcome.RETRY:
                    continue
                if rc is not None:
                    # A fetched workspace came home. When it did not, the row
                    # was just annotated
                    # `artifacts_unavailable`, and rewriting it to `finishing`
                    # here would erase the only durable record that this job's
                    # outputs are gone -- leaving a terminal row that looks like
                    # an ordinary completion with absent files.
                    if (
                        fetch_outcome is _SchedulerFetchOutcome.FETCHED
                        and spec.scheduler_state
                        in {
                            "fetch_failed",
                            "marker_probe_failed",
                            "poll_failed",
                        }
                    ):
                        self._stamp_scheduler_finishing(jobid, sj, spec)
                    walltime_exceeded, walltime_used, walltime_limit = (
                        _scheduler_walltime_exceeded(spec, detail)
                    )
                    if walltime_exceeded:
                        evidence: dict[str, object] = {
                            "scheduler_target": spec.scheduler_target,
                            "scheduler_job_id": spec.scheduler_job_id,
                            "remote_workspace": sj.handle.remote_workspace,
                            "scheduler_walltime_exceeded": True,
                        }
                        if walltime_used is not None:
                            evidence["scheduler_walltime_used"] = walltime_used
                        if walltime_limit is not None:
                            evidence["scheduler_walltime_limit"] = walltime_limit
                        self._mark_scheduler_time_exceeded(
                            spec,
                            reason=_scheduler_walltime_reason(
                                used=walltime_used,
                                limit=walltime_limit,
                                missing_marker=False,
                            ),
                            evidence=evidence,
                            terminal_side_effects=False,
                            jobid=jobid,
                            owner_uid=sj.owner_uid,
                            spec_path=spec_path,
                        )
                    # #414: reconcile the terminal label against scheduler
                    # accounting BEFORE recording the marker rc. An abnormal
                    # accounting verdict (OUT_OF_MEMORY / CANCELLED / TIMEOUT
                    # / ...) writes the failed-class terminal state with the
                    # scheduler reason; the marker rc then lands under that
                    # preserved label instead of minting completed/exit-0 for
                    # a job the scheduler killed. Ordered after the walltime
                    # branch so a duration-proven TIME_EXCEEDED keeps its
                    # richer walltime reason.
                    if abnormal_state is not None and detail is not None:
                        self._mark_scheduler_abnormal_end(
                            spec,
                            abnormal_state=abnormal_state,
                            detail=detail,
                            marker_rc=rc,
                            jobid=jobid,
                            owner_uid=sj.owner_uid,
                            spec_path=spec_path,
                        )
                    # COMPLETED/FAILED by rc, or rc recorded under a preserved
                    # terminal label (the vq-kill case lands here when the job
                    # trapped SIGTERM and wrote its marker before qdel).
                    self._record_finish(jobid, rc)
                    if walltime_exceeded:
                        try:
                            finished_spec, finished_path = self._read_active_spec(
                                jobid,
                                sj,
                                owner_uid=sj.owner_uid,
                                spec_path=spec_path,
                            )
                        except (OSError, ValueError):
                            pass
                        else:
                            self._maybe_cleanup_workdir(
                                finished_spec,
                                owner_uid=sj.owner_uid,
                                spec_path=finished_path,
                            )
                finished.append(jobid)
        for jobid in finished:
            self._scheduler_running.pop(jobid, None)
        return observed_jobs

    def _reattach_scheduler_job(self, spec: JobSpec) -> bool:
        """Re-track a scheduler job across a daemon restart (§17).

        A cluster job keeps running while the driver daemon is down. Rebuild its
        :class:`SchedulerDispatcher` from config and its :class:`SchedulerHandle`
        from the persisted ``scheduler_job_id`` plus the deterministic remote
        workspace, then re-add it to ``_scheduler_running`` so the next
        ``_reconcile_scheduler`` resumes polling / reaping it. Returns ``False``
        when the host config is gone or no scheduler id can be established;
        startup then leaves the spec non-terminal as
        ``scheduler_state=reattach_failed`` and later daemon ticks retry
        reattach.

        A spec with no persisted ``scheduler_job_id`` is not hopeless. It is the
        signature of a driver death inside ``_start_scheduler_job``'s dispatch
        window, where the qsub may well have succeeded -- so ask the cluster for
        what the driver failed to write down, via the id the job script records
        for itself. Before this, that retry could never succeed: the no-id case
        returned ``False`` immediately, so the case the deferral was written for
        was the one case it could not recover, and a live cluster job stayed
        permanently unnamed.
        """
        spec, owner_uid, captured_path, expected_target = (
            self._capture_scheduler_transaction_authority(spec)
        )
        if self._multi_user and expected_target is None:
            return False
        target = expected_target if self._multi_user else spec.scheduler_target
        if target is None:
            return False
        spec_path = self._active_spec_path(
            spec.id,
            owner_uid=owner_uid,
            spec_path=captured_path,
        )
        if self._multi_user:
            validation_error = self._validate_multi_user_spec(spec)
            if validation_error is not None or spec.scheduler_target != target:
                self._quarantine_unconfirmed_scheduler_spec(
                    spec.id,
                    owner_uid=owner_uid,
                    spec_path=spec_path,
                    reason=(
                        f"multi-user spec gate: {validation_error}"
                        if validation_error is not None
                        else "scheduler target changed during reattach"
                    ),
                    expected_target=target,
                )
                return False
        try:
            dispatcher = self._scheduler_dispatcher_for(target)
        except (ConfigError, SchedulerError, DialectError) as exc:
            log.warning(
                "job %s: cannot reattach to scheduler %s: %s", spec.id, target, exc
            )
            return False
        scheduler_job_id = spec.scheduler_job_id
        if self._multi_user:
            assert owner_uid is not None
            try:
                binding = _read_scheduler_submit_binding(owner_uid, spec.id)
            except (OSError, ValueError):
                return False
            if binding is None:
                return False
            if binding.scheduler_job_id is not None:
                if (
                    scheduler_job_id is not None
                    and scheduler_job_id != binding.scheduler_job_id
                ):
                    self._quarantine_unconfirmed_scheduler_spec(
                        spec.id,
                        owner_uid=owner_uid,
                        spec_path=spec_path,
                        reason="mutable scheduler id conflicts with bound acceptance",
                        expected_target=target,
                    )
                    return False
                scheduler_job_id = binding.scheduler_job_id
            elif scheduler_job_id is not None:
                # The user-writable queue field alone is not acceptance proof.
                scheduler_job_id = None
        if scheduler_job_id is None:
            receipt_reader = getattr(dispatcher, "submit_receipt", None)
            receipt = receipt_reader(spec.id) if callable(receipt_reader) else None
            receipt_job_id = (
                receipt.scheduler_job_id
                if receipt is not None and receipt.status == "accepted"
                else None
            )
            recorded_job_id = dispatcher.recorded_job_id(spec.id)
            if (
                receipt_job_id is not None
                and recorded_job_id is not None
                and receipt_job_id != recorded_job_id
            ):
                log.error(
                    "job %s: conflicting scheduler receipt id %s and "
                    "job-start marker id %s; refusing to choose",
                    spec.id,
                    receipt_job_id,
                    recorded_job_id,
                )
                self._mark_scheduler_submit_outcome_unknown(
                    spec,
                    reason="conflicting scheduler receipt and job-start marker ids",
                    owner_uid=owner_uid,
                    spec_path=spec_path,
                    expected_target=target,
                    durable_state="submit_evidence_conflict",
                )
                return False
            scheduler_job_id = recorded_job_id or receipt_job_id
            if scheduler_job_id is None:
                return False
            if not self._persist_recovered_scheduler_job_id(
                spec,
                scheduler_job_id,
                owner_uid=owner_uid,
                spec_path=spec_path,
                expected_target=target,
            ):
                return False
            log.warning(
                "job %s: recovered scheduler id %s from the job's own record; "
                "the driver died between qsub and recording it",
                spec.id,
                scheduler_job_id,
            )
        handle = scheduler_handle_for_spec(
            dispatcher,
            spec,
            job_id=scheduler_job_id,
        )
        self._scheduler_running[spec.id] = _SchedulerJob(
            handle=handle,
            dispatcher=dispatcher,
            cpus=spec.cpus,
            mem_mb=spec.mem_mb,
            owner_uid=owner_uid,
            spec_path=spec_path,
        )
        log.info(
            "job %s: reattached to scheduler %s as %s",
            spec.id,
            target,
            scheduler_job_id,
        )
        return True

    def _quarantine_unconfirmed_scheduler_spec_locked(
        self,
        spec: JobSpec,
        spec_path: Path,
        *,
        reason: str,
        expected_target: str | None = None,
    ) -> None:
        """Fence one unsafe unconfirmed row without touching its untrusted cwd.

        The scheduler mutation may already have happened, so even maliciously
        changed local metadata cannot justify a terminal state.  Keep the row
        nonterminal and capacity-reserving for explicit operator resolution.
        """
        if spec.is_terminal:
            spec.scheduler_state = "submit_reconciliation_quarantined_after_terminal"
        elif spec.state in {JobState.RUNNING, JobState.SUSPENDED}:
            spec.scheduler_state = "scheduler_reconciliation_quarantined"
        else:
            spec.state = JobState.SUBMIT_OUTCOME_UNKNOWN
            spec.scheduler_state = "submit_reconciliation_quarantined"
            spec.scheduler_job_id = None
            spec.finished_at = None
            spec.exit_code = None
        if expected_target is not None:
            spec.scheduler_target = expected_target
        spec.failure_reason = reason
        spec.write(spec_path)
        log.error(
            "job %s: quarantined unconfirmed scheduler submit (%s)",
            spec.id,
            reason,
        )

    def _quarantine_unconfirmed_scheduler_spec(
        self,
        jobid: str,
        *,
        owner_uid: str | None,
        spec_path: Path | None,
        reason: str,
        expected_target: str | None = None,
    ) -> None:
        """Quarantine through the captured owner-qualified path only."""
        if self._multi_user and (owner_uid is None or spec_path is None):
            log.error(
                "job %s: cannot quarantine unconfirmed submit without a captured "
                "owner-qualified path (%s)",
                jobid,
                reason,
            )
            return
        admitted_path = self._active_spec_path(
            jobid,
            owner_uid=owner_uid,
            spec_path=spec_path,
        )
        with paths.spec_lock(admitted_path):
            try:
                fresh, _ = self._read_active_spec(
                    jobid,
                    owner_uid=owner_uid,
                    spec_path=admitted_path,
                )
            except (OSError, ValueError):
                return
            self._quarantine_unconfirmed_scheduler_spec_locked(
                fresh,
                admitted_path,
                reason=reason,
                expected_target=expected_target,
            )

    def _reconcile_unconfirmed_scheduler_submit(
        self,
        spec: JobSpec,
        *,
        owner_uid: str | None = None,
        spec_path: Path | None = None,
        expected_target: str | None = None,
    ) -> bool:
        """Resolve SUBMITTING/unknown only from exact durable remote proof."""
        if spec.scheduler_state in {
            "submit_evidence_conflict",
            "submit_evidence_conflict_after_terminal",
        }:
            return False
        private_transaction_open = False
        if self._multi_user and owner_uid is not None:
            try:
                private_transaction_open = _scheduler_submit_binding_may_be_open(
                    _read_scheduler_submit_binding(owner_uid, spec.id)
                )
            except (OSError, ValueError):
                private_transaction_open = True
        target = expected_target or spec.scheduler_target
        terminal_ambiguity = spec.is_terminal and spec.scheduler_state in {
            "submitting",
            "submit_outcome_unknown",
            "submit_outcome_unknown_after_terminal",
            "submit_reconciliation_quarantined_after_terminal",
            "submit_cancel_pending_after_terminal",
        } or (spec.is_terminal and private_transaction_open)
        if target is None or (spec.is_terminal and not terminal_ambiguity):
            return False
        expected_target = target if expected_target is None else expected_target
        if self._multi_user:
            authority_error = self._multi_user_scheduler_authority_error(
                spec,
                owner_uid=owner_uid,
                expected_target=expected_target,
            )
            if authority_error is not None or spec_path is None:
                self._quarantine_unconfirmed_scheduler_spec(
                    spec.id,
                    owner_uid=owner_uid,
                    spec_path=spec_path,
                    reason=(
                        authority_error
                        if authority_error is not None
                        else "cannot bind the unconfirmed submit to its owning path"
                    ),
                    expected_target=expected_target,
                )
                return False
        if (
            spec.is_terminal
            and spec.scheduler_state == "submit_cancel_pending_after_terminal"
            and spec.scheduler_job_id is not None
        ):
            scheduler_job_id = spec.scheduler_job_id
            if self._multi_user:
                assert owner_uid is not None
                try:
                    binding = _read_scheduler_submit_binding(owner_uid, spec.id)
                except (OSError, ValueError):
                    binding = None
                if (
                    binding is None
                    or binding.transaction_state != "open"
                    or binding.scheduler_job_id is None
                    or scheduler_job_id != binding.scheduler_job_id
                ):
                    self._quarantine_unconfirmed_scheduler_spec(
                        spec.id,
                        owner_uid=owner_uid,
                        spec_path=spec_path,
                        reason=(
                            "cancel-pending scheduler id conflicts with "
                            "daemon-owned acceptance authority"
                        ),
                        expected_target=expected_target,
                    )
                    return False
                scheduler_job_id = binding.scheduler_job_id
            try:
                dispatcher = self._scheduler_dispatcher_for(target)
            except (ConfigError, SchedulerError, DialectError):
                return False
            return self._cancel_recovered_terminal_scheduler_submit(
                spec,
                scheduler_job_id,
                dispatcher=dispatcher,
                owner_uid=owner_uid,
                spec_path=spec_path,
                expected_target=expected_target,
            )
        try:
            dispatcher = self._scheduler_dispatcher_for(target)
        except (ConfigError, SchedulerError, DialectError):
            self._mark_scheduler_submit_outcome_unknown(
                spec,
                reason="scheduler submit receipt is currently unobservable",
                owner_uid=owner_uid,
                spec_path=spec_path,
                expected_target=expected_target,
            )
            return False
        bound_scheduler_job_id: str | None = None
        if self._multi_user:
            assert owner_uid is not None
            try:
                binding = _read_scheduler_submit_binding(owner_uid, spec.id)
            except (OSError, ValueError) as exc:
                self._quarantine_unconfirmed_scheduler_spec(
                    spec.id,
                    owner_uid=owner_uid,
                    spec_path=spec_path,
                    reason=f"scheduler submit binding unreadable: {exc}",
                    expected_target=expected_target,
                )
                return False
            if binding is None or binding.transaction_state != "open":
                self._quarantine_unconfirmed_scheduler_spec(
                    spec.id,
                    owner_uid=owner_uid,
                    spec_path=spec_path,
                    reason="scheduler submit binding is unavailable for reconciliation",
                    expected_target=expected_target,
                )
                return False
            bound_scheduler_job_id = binding.scheduler_job_id
            if bound_scheduler_job_id is None:
                # Scheduler workspaces and adjacent receipts are writable by
                # the configured remote Unix account, including the submitted
                # payload itself. Until direct acceptance is bound in this
                # root-owned authority, matching remote files cannot authorize
                # root to poll or cancel an arbitrary scheduler allocation.
                self._mark_scheduler_submit_outcome_unknown(
                    spec,
                    reason=(
                        "remote submit evidence is untrusted without a "
                        "daemon-bound scheduler id"
                    ),
                    owner_uid=owner_uid,
                    spec_path=spec_path,
                    expected_target=expected_target,
                )
                return False
        receipt_reader = getattr(dispatcher, "submit_receipt", None)
        receipt = receipt_reader(spec.id) if callable(receipt_reader) else None
        receipt_job_id = (
            receipt.scheduler_job_id
            if receipt is not None and receipt.status == "accepted"
            else None
        )
        # Always read the job-authored marker.  It is the closest evidence to
        # what actually started, and must never be silently shadowed by an
        # accepted receipt naming a different scheduler allocation.
        recorded_job_id = dispatcher.recorded_job_id(spec.id)
        acceptance_ids = {
            candidate
            for candidate in (
                bound_scheduler_job_id,
                receipt_job_id,
                recorded_job_id,
            )
            if candidate is not None
        }
        if len(acceptance_ids) > 1:
            self._mark_scheduler_submit_outcome_unknown(
                spec,
                reason=(
                    "conflicting daemon binding, scheduler receipt, and "
                    "job-start marker ids"
                ),
                owner_uid=owner_uid,
                spec_path=spec_path,
                expected_target=expected_target,
                durable_state="submit_evidence_conflict",
            )
            return False
        # A valid job-start marker outweighs rejection/unknown receipt states;
        # absent a marker, an accepted receipt remains sufficient proof.
        scheduler_job_id = (
            bound_scheduler_job_id or recorded_job_id or receipt_job_id
        )
        if (
            scheduler_job_id is None
            and receipt is not None
            and receipt.status == "rejected"
        ):
            reason = (
                "scheduler submit was durably rejected "
                f"(exit {receipt.scheduler_returncode})"
            )
            admitted_path = self._active_spec_path(
                spec.id,
                owner_uid=owner_uid,
                spec_path=spec_path,
            )
            with paths.spec_lock(admitted_path):
                try:
                    fresh, _ = self._read_active_spec(
                        spec.id,
                        owner_uid=owner_uid,
                        spec_path=admitted_path,
                    )
                except (OSError, ValueError):
                    return False
                if fresh.is_terminal:
                    if fresh.scheduler_state in {
                        "submitting",
                        "submit_outcome_unknown",
                        "submit_outcome_unknown_after_terminal",
                        "submit_reconciliation_quarantined_after_terminal",
                    }:
                        fresh.scheduler_state = "submit_rejected_after_terminal"
                        fresh.last_heartbeat_at = utcnow_iso()
                    if self._multi_user and owner_uid is not None:
                        try:
                            _close_scheduler_submit_binding(owner_uid, fresh)
                        except (OSError, ValueError):
                            log.exception(
                                "job %s: failed to close rejected scheduler "
                                "submit binding before terminal write",
                                fresh.id,
                            )
                            return False
                    fresh.write(admitted_path)
                    if self._multi_user and owner_uid is not None:
                        try:
                            _remove_scheduler_submit_binding(owner_uid, fresh.id)
                        except (OSError, ValueError):
                            log.exception(
                                "job %s: failed to remove closed rejected "
                                "scheduler submit binding",
                                fresh.id,
                            )
                    return False
                if self._multi_user:
                    authority_error = self._multi_user_scheduler_authority_error(
                        fresh,
                        owner_uid=owner_uid,
                        expected_target=expected_target,
                    )
                    if authority_error is not None:
                        self._quarantine_unconfirmed_scheduler_spec_locked(
                            fresh,
                            admitted_path,
                            reason=authority_error,
                            expected_target=expected_target,
                        )
                        return False
                previous = fresh.state
                fresh.state = JobState.FAILED
                fresh.scheduler_state = "submit_rejected"
                fresh.failure_reason = reason
                fresh.exit_code = None
                fresh.finished_at = utcnow_iso()
                if self._multi_user and owner_uid is not None:
                    try:
                        _close_scheduler_submit_binding(owner_uid, fresh)
                    except (OSError, ValueError):
                        log.exception(
                            "job %s: failed to close rejected scheduler submit "
                            "binding before terminal write",
                            fresh.id,
                        )
                        return False
                fresh.write(admitted_path)
                events.state_transition(
                    Path(fresh.cwd),
                    fresh.id,
                    from_state=previous.value,
                    to_state=JobState.FAILED.value,
                    reason=reason,
                    scheduler_target=target,
                )
            if self._multi_user and owner_uid is not None:
                try:
                    _remove_scheduler_submit_binding(owner_uid, spec.id)
                except (OSError, ValueError):
                    log.exception(
                        "job %s: failed to close/remove rejected scheduler "
                        "submit binding",
                        spec.id,
                    )
            return False
        if scheduler_job_id is None:
            self._mark_scheduler_submit_outcome_unknown(
                spec,
                reason="scheduler acceptance has no valid receipt or job-start marker",
                owner_uid=owner_uid,
                spec_path=spec_path,
                expected_target=expected_target,
            )
            return False
        if self._multi_user:
            assert owner_uid is not None
            try:
                _bind_scheduler_job_id(owner_uid, spec.id, scheduler_job_id)
            except (OSError, ValueError) as exc:
                self._quarantine_unconfirmed_scheduler_spec(
                    spec.id,
                    owner_uid=owner_uid,
                    spec_path=spec_path,
                    reason=f"accepted scheduler id could not be bound: {exc}",
                    expected_target=expected_target,
                )
                return False
        if spec.is_terminal:
            return self._cancel_recovered_terminal_scheduler_submit(
                spec,
                scheduler_job_id,
                dispatcher=dispatcher,
                owner_uid=owner_uid,
                spec_path=spec_path,
                expected_target=expected_target,
            )
        if not self._persist_recovered_scheduler_job_id(
            spec,
            scheduler_job_id,
            owner_uid=owner_uid,
            spec_path=spec_path,
            expected_target=expected_target,
        ):
            try:
                raced, _ = self._read_active_spec(
                    spec.id,
                    owner_uid=owner_uid,
                    spec_path=spec_path,
                )
            except (OSError, ValueError):
                return False
            if raced.is_terminal:
                return self._cancel_recovered_terminal_scheduler_submit(
                    raced,
                    scheduler_job_id,
                    dispatcher=dispatcher,
                    owner_uid=owner_uid,
                    spec_path=spec_path,
                    expected_target=expected_target,
                )
            return False
        admitted_path = self._active_spec_path(
            spec.id,
            owner_uid=owner_uid,
            spec_path=spec_path,
        )
        with paths.spec_lock(admitted_path):
            fresh, _ = self._read_active_spec(
                spec.id,
                owner_uid=owner_uid,
                spec_path=admitted_path,
            )
            cancel_terminal = fresh if fresh.is_terminal else None
            if cancel_terminal is None and self._multi_user:
                authority_error = self._multi_user_scheduler_authority_error(
                    fresh,
                    owner_uid=owner_uid,
                    expected_target=expected_target,
                    expected_scheduler_job_id=scheduler_job_id,
                )
                if authority_error is not None:
                    self._quarantine_unconfirmed_scheduler_spec_locked(
                        fresh,
                        admitted_path,
                        reason=authority_error,
                        expected_target=expected_target,
                    )
                    return False
            if cancel_terminal is not None:
                pass
            elif fresh.scheduler_job_id != scheduler_job_id:
                previous = fresh.state
                fresh.state = JobState.SUBMIT_OUTCOME_UNKNOWN
                fresh.scheduler_state = "submit_evidence_conflict"
                fresh.scheduler_job_id = None
                fresh.last_heartbeat_at = utcnow_iso()
                fresh.write(admitted_path)
                if self._multi_user and owner_uid is not None:
                    _replace_scheduler_submit_binding_spec(
                        owner_uid,
                        fresh,
                        scheduler_job_id=scheduler_job_id,
                    )
                events.state_transition(
                    Path(fresh.cwd),
                    fresh.id,
                    from_state=previous.value,
                    to_state=JobState.SUBMIT_OUTCOME_UNKNOWN.value,
                    reason="scheduler id changed during submit reconciliation",
                    scheduler_target=expected_target,
                )
                if self._multi_user and owner_uid is not None:
                    _replace_scheduler_submit_binding_spec(owner_uid, fresh)
                return False
            else:
                previous = fresh.state
                fresh.state = JobState.RUNNING
                fresh.scheduler_state = None
                fresh.scheduler_job_id = scheduler_job_id
                fresh.last_heartbeat_at = utcnow_iso()
                fresh.write(admitted_path)
                events.state_transition(
                    Path(fresh.cwd),
                    fresh.id,
                    from_state=previous.value,
                    to_state=JobState.RUNNING.value,
                    reason="scheduler acceptance recovered from exact remote evidence",
                    scheduler_target=target,
                    scheduler_job_id=scheduler_job_id,
                )
        if cancel_terminal is not None:
            return self._cancel_recovered_terminal_scheduler_submit(
                cancel_terminal,
                scheduler_job_id,
                dispatcher=dispatcher,
                owner_uid=owner_uid,
                spec_path=spec_path,
                expected_target=expected_target,
            )
        handle = scheduler_handle_for_spec(
            dispatcher,
            fresh,
            job_id=scheduler_job_id,
        )
        self._scheduler_running[spec.id] = _SchedulerJob(
            handle=handle,
            dispatcher=dispatcher,
            cpus=self._scheduler_reservation_cpus(owner_uid, fresh),
            mem_mb=fresh.mem_mb,
            owner_uid=owner_uid,
            spec_path=admitted_path,
        )
        return True

    def _cancel_recovered_terminal_scheduler_submit(
        self,
        spec: JobSpec,
        scheduler_job_id: str,
        *,
        dispatcher: SchedulerDispatcher,
        owner_uid: str | None,
        spec_path: Path | None,
        expected_target: str,
    ) -> bool:
        """Own and cancel acceptance discovered after a terminal kill race."""
        admitted_path = self._active_spec_path(
            spec.id,
            owner_uid=owner_uid,
            spec_path=spec_path,
        )
        with paths.spec_lock(admitted_path):
            try:
                fresh, _ = self._read_active_spec(
                    spec.id,
                    owner_uid=owner_uid,
                    spec_path=admitted_path,
                )
            except (OSError, ValueError):
                return False
            if not fresh.is_terminal:
                return False
            if self._multi_user:
                authority_error = self._multi_user_scheduler_authority_error(
                    fresh,
                    owner_uid=owner_uid,
                    expected_target=expected_target,
                    expected_scheduler_job_id=scheduler_job_id,
                )
                if authority_error is not None:
                    self._quarantine_unconfirmed_scheduler_spec_locked(
                        fresh,
                        admitted_path,
                        reason=authority_error,
                        expected_target=expected_target,
                    )
                    return False
            fresh.scheduler_job_id = scheduler_job_id
            fresh.scheduler_state = "submit_cancel_pending_after_terminal"
            fresh.last_heartbeat_at = utcnow_iso()
            fresh.write(admitted_path)
            if self._multi_user and owner_uid is not None:
                _replace_scheduler_submit_binding_spec(
                    owner_uid,
                    fresh,
                    scheduler_job_id=scheduler_job_id,
                )
        handle = scheduler_handle_for_spec(
            dispatcher,
            fresh,
            job_id=scheduler_job_id,
        )
        runtime = _SchedulerJob(
            handle=handle,
            dispatcher=dispatcher,
            cpus=self._scheduler_reservation_cpus(owner_uid, fresh),
            mem_mb=fresh.mem_mb,
            owner_uid=owner_uid,
            spec_path=admitted_path,
        )
        self._scheduler_running[spec.id] = runtime
        try:
            dispatcher.cancel(handle)
        except (SchedulerError, DialectError):
            log.exception(
                "job %s: exact scheduler acceptance recovered after a terminal "
                "race, but cancellation is not yet confirmed",
                spec.id,
            )
            return False
        runtime.term_qdeled = True
        try:
            cancelled_phase = dispatcher.poll([handle]).get(handle.job_id)
        except (SchedulerError, DialectError):
            log.exception(
                "job %s: cancellation was accepted but exact scheduler "
                "disappearance is not yet observable",
                spec.id,
            )
            return False
        if cancelled_phase is not SchedulerPhase.FINISHED:
            log.info(
                "job %s: cancellation was accepted but scheduler allocation "
                "%s is still visible; retaining cancel-pending ownership",
                spec.id,
                scheduler_job_id,
            )
            return False
        cancellation_closed = not self._multi_user
        with paths.spec_lock(admitted_path):
            try:
                final, _ = self._read_active_spec(
                    spec.id,
                    owner_uid=owner_uid,
                    spec_path=admitted_path,
                )
            except (OSError, ValueError):
                return False
            if final.is_terminal and final.scheduler_job_id == scheduler_job_id:
                final.scheduler_state = "submit_cancelled_after_terminal"
                final.last_heartbeat_at = utcnow_iso()
                if self._multi_user and owner_uid is not None:
                    try:
                        _close_scheduler_submit_binding(owner_uid, final)
                    except (OSError, ValueError):
                        log.exception(
                            "job %s: failed to close confirmed-cancel scheduler "
                            "binding before terminal write",
                            spec.id,
                        )
                        return False
                    cancellation_closed = True
                final.write(admitted_path)
        if not cancellation_closed:
            return False
        self._scheduler_running.pop(spec.id, None)
        if self._multi_user and owner_uid is not None:
            try:
                _remove_scheduler_submit_binding(owner_uid, spec.id)
            except (OSError, ValueError):
                log.exception(
                    "job %s: failed to close/remove cancelled scheduler submit binding",
                    spec.id,
                )
        return False

    def _untracked_spec_still_reserves(self, spec: JobSpec) -> bool:
        """Whether an untracked scheduler spec still holds a dispatch slot.

        See :data:`SCHEDULER_UNTRACKABLE_RESERVATION_SECONDS`. A spec that kept
        its ``scheduler_job_id`` reserves indefinitely -- vq holds a handle and
        ``_reconcile_scheduler`` will reap it. A spec with no id can only be
        recovered by ``recorded_job_id``, retried every
        ``SCHEDULER_REATTACH_RETRY_SECONDS``; once the window has passed
        without recovery there is nothing left to wait for, and continuing to
        reserve starves every pending job behind it.

        Releasing the slot deliberately does NOT reap the spec. vq cannot prove
        the job is dead -- only that it can no longer track it -- so the row
        stays visible for an operator (or ``vq cleanup``) to resolve. What ends
        is its claim on capacity it will never use.
        """
        if spec.scheduler_job_id is not None:
            return True
        marked_at = spec.last_heartbeat_at or spec.started_at
        if marked_at is None:
            # No timestamp to age against: keep the conservative reservation
            # rather than release a slot on a spec we know nothing about.
            return True
        try:
            marked = datetime.fromisoformat(marked_at)
        except ValueError:
            return True
        if marked.tzinfo is None:
            marked = marked.replace(tzinfo=UTC)
        age = (datetime.now(UTC) - marked).total_seconds()
        if age < SCHEDULER_UNTRACKABLE_RESERVATION_SECONDS:
            return True
        if spec.id not in self._untracked_reservation_released:
            self._untracked_reservation_released.add(spec.id)
            log.warning(
                "job %s: untracked on scheduler %s for %.0fs with no job id; "
                "releasing its dispatch slot. vq cannot recover this spec, so "
                "holding capacity for it only blocks pending work. The row "
                "stays non-terminal for an operator to resolve.",
                spec.id,
                spec.scheduler_target,
                age,
            )
        return False

    def _persist_recovered_scheduler_job_id(
        self,
        spec: JobSpec,
        job_id: str,
        *,
        owner_uid: str | None = None,
        spec_path: Path | None = None,
        expected_target: str | None = None,
    ) -> bool:
        """Write a recovered scheduler id onto the spec under its lock.

        Refuses if the spec went terminal meanwhile: an operator who killed the
        job while it was unnamed must not have it silently re-tracked.
        """
        admitted_jobid = spec.id
        admitted_path = self._active_spec_path(
            admitted_jobid,
            owner_uid=owner_uid,
            spec_path=spec_path,
        )
        with paths.spec_lock(admitted_path):
            try:
                fresh, _ = self._read_active_spec(
                    admitted_jobid,
                    owner_uid=owner_uid,
                    spec_path=admitted_path,
                )
            except (OSError, ValueError):
                return False
            if fresh.is_terminal:
                return False
            if self._multi_user:
                authority_error = self._multi_user_scheduler_authority_error(
                    fresh,
                    owner_uid=owner_uid,
                    expected_target=expected_target,
                )
                if authority_error is not None:
                    self._quarantine_unconfirmed_scheduler_spec_locked(
                        fresh,
                        admitted_path,
                        reason=authority_error,
                        expected_target=expected_target,
                    )
                    return False
            if fresh.scheduler_target is None:
                return False
            if fresh.scheduler_job_id is not None:
                return fresh.scheduler_job_id == job_id
            fresh.scheduler_job_id = job_id
            fresh.write(admitted_path)
            if self._multi_user and owner_uid is not None:
                _replace_scheduler_submit_binding_spec(
                    owner_uid,
                    fresh,
                    scheduler_job_id=job_id,
                )
        return True

    def _defer_scheduler_reattach(self, spec: JobSpec, *, reason: str) -> None:
        """Keep a scheduler job non-terminal when restart reattach is blocked."""
        admitted_jobid = spec.id
        spec_path = self._active_spec_path(admitted_jobid)
        with paths.spec_lock(spec_path):
            try:
                fresh, _ = self._read_active_spec(
                    admitted_jobid,
                    spec_path=spec_path,
                )
            except (OSError, ValueError):
                return
            if fresh.is_terminal or fresh.scheduler_target is None:
                return
            previous = fresh.state
            fresh.scheduler_state = "reattach_failed"
            fresh.last_heartbeat_at = utcnow_iso()
            fresh.write(spec_path)
            events.state_transition(
                Path(fresh.cwd),
                admitted_jobid,
                from_state=previous.value,
                to_state=previous.value,
                reason=reason,
                scheduler_target=fresh.scheduler_target,
                scheduler_job_id=fresh.scheduler_job_id,
            )

    def _mark_scheduler_reattached(self, spec: JobSpec) -> None:
        """Clear the deferred-reattach marker after a later successful reattach."""
        admitted_jobid = spec.id
        runtime = self._scheduler_running.get(admitted_jobid)
        spec_path = self._active_spec_path(admitted_jobid, runtime)
        with paths.spec_lock(spec_path):
            try:
                fresh, _ = self._read_active_spec(
                    admitted_jobid,
                    runtime,
                    spec_path=spec_path,
                )
            except (OSError, ValueError):
                return
            if fresh.is_terminal or fresh.scheduler_target is None:
                return
            previous = fresh.state
            fresh.scheduler_state = None
            fresh.last_heartbeat_at = utcnow_iso()
            fresh.write(spec_path)
            events.state_transition(
                Path(fresh.cwd),
                admitted_jobid,
                from_state=previous.value,
                to_state=previous.value,
                reason="scheduler job reattached after deferred startup recovery",
                scheduler_target=fresh.scheduler_target,
                scheduler_job_id=fresh.scheduler_job_id,
            )

    def _retry_deferred_scheduler_reattach(self) -> None:
        """Retry scheduler reattach for jobs preserved as untracked at startup."""
        now = time.monotonic()
        for spec in self._iter_specs():
            binding_fence = self._scheduler_binding_requires_fence(spec)
            transaction_may_exist = (
                self._scheduler_transaction_may_exist(spec)
                or binding_fence
            )
            if transaction_may_exist:
                spec, owner_uid, spec_path, expected_target = (
                    self._capture_scheduler_transaction_authority(spec)
                )
            else:
                owner_uid = self._job_uid.get(spec.id) if self._multi_user else None
                spec_path = (
                    self._job_spec_paths.get(spec.id) if self._multi_user else None
                )
                expected_target = spec.scheduler_target
            evidence_conflict = spec.scheduler_state in {
                "submit_evidence_conflict",
                "submit_evidence_conflict_after_terminal",
            }
            unconfirmed_submit = not evidence_conflict and (
                binding_fence
                or
                spec.state
                in {JobState.SUBMITTING, JobState.SUBMIT_OUTCOME_UNKNOWN}
                or spec.scheduler_state
                in {
                    "submitting",
                    "submit_outcome_unknown",
                    "submit_outcome_unknown_after_terminal",
                    "submit_reconciliation_quarantined_after_terminal",
                    "submit_cancel_pending_after_terminal",
                }
            )
            if (
                spec.id not in self._scheduler_running
                and expected_target is not None
                and unconfirmed_submit
            ):
                last = self._scheduler_reattach_retry_last.get(spec.id, 0.0)
                if now - last < SCHEDULER_REATTACH_RETRY_SECONDS:
                    continue
                self._scheduler_reattach_retry_last[spec.id] = now
                self._reconcile_unconfirmed_scheduler_submit(
                    spec,
                    owner_uid=owner_uid,
                    spec_path=spec_path,
                    expected_target=expected_target,
                )
                continue
            if (
                spec.id in self._scheduler_running
                or spec.scheduler_target is None
                or spec.scheduler_state != "reattach_failed"
                or spec.state not in (JobState.RUNNING, JobState.SUSPENDED)
            ):
                continue
            last = self._scheduler_reattach_retry_last.get(spec.id, 0.0)
            if now - last < SCHEDULER_REATTACH_RETRY_SECONDS:
                continue
            self._scheduler_reattach_retry_last[spec.id] = now
            if self._reattach_scheduler_job(spec):
                self._mark_scheduler_reattached(spec)

    def _stamp_scheduler_finishing(
        self, jobid: str, sj: _SchedulerJob, spec: JobSpec
    ) -> None:
        """Record scheduler termination pending marker and artifact collection."""
        cluster_state = "finishing"
        transitioned = spec.scheduler_state != cluster_state
        now = time.monotonic()
        if (
            not transitioned
            and (now - sj.last_status_write) < SCHEDULER_STATUS_REFRESH_SECONDS
        ):
            return
        spec_path = self._active_spec_path(jobid, sj)
        with paths.spec_lock(spec_path):
            try:
                fresh, _ = self._read_active_spec(
                    jobid,
                    sj,
                    spec_path=spec_path,
                )
            except (OSError, ValueError):
                return
            if fresh.is_terminal:
                return
            fresh.scheduler_state = cluster_state
            fresh.last_heartbeat_at = utcnow_iso()
            fresh.write(spec_path)
        sj.last_status_write = now

    def _stamp_scheduler_poll_succeeded(
        self,
        jobid: str,
        sj: _SchedulerJob,
        *,
        attempted_at: str,
    ) -> str | None:
        """Persist an unambiguous successful scheduler observation."""
        spec_path = self._active_spec_path(jobid, sj)
        with paths.spec_lock(spec_path):
            try:
                fresh, _ = self._read_active_spec(
                    jobid,
                    sj,
                    spec_path=spec_path,
                )
            except (OSError, ValueError):
                return None
            if fresh.is_terminal or fresh.scheduler_target is None:
                return None
            observed_at = utcnow_iso()
            fresh.scheduler_poll_last_attempted_at = attempted_at
            fresh.scheduler_poll_last_success_at = observed_at
            fresh.write(spec_path)
        return observed_at

    def _stamp_scheduler_poll_failed(
        self,
        jobid: str,
        sj: _SchedulerJob,
        *,
        reason: str,
        attempted_at: str,
    ) -> None:
        """Record that the batched scheduler poll failed and will retry."""
        reason = _safe_scheduler_poll_diagnostic(reason)
        cluster_state = "poll_failed"
        now = time.monotonic()
        spec_path = self._active_spec_path(jobid, sj)
        with paths.spec_lock(spec_path):
            try:
                fresh, _ = self._read_active_spec(
                    jobid,
                    sj,
                    spec_path=spec_path,
                )
            except (OSError, ValueError):
                return
            if fresh.is_terminal or fresh.scheduler_target is None:
                return
            transitioned = fresh.scheduler_state != cluster_state
            previous = fresh.state
            errored_at = utcnow_iso()
            fresh.scheduler_state = cluster_state
            fresh.scheduler_poll_last_attempted_at = attempted_at
            fresh.scheduler_poll_last_error_at = errored_at
            fresh.scheduler_poll_last_error = reason
            fresh.write(spec_path)
            if transitioned:
                log.warning(
                    "job %s: scheduler poll failed for handle %s: %s",
                    jobid,
                    sj.handle.job_id,
                    reason,
                )
                events.state_transition(
                    Path(fresh.cwd),
                    jobid,
                    from_state=previous.value,
                    to_state=previous.value,
                    reason="scheduler poll failed; daemon will retry",
                    scheduler_target=fresh.scheduler_target,
                    scheduler_job_id=fresh.scheduler_job_id,
                    remote_workspace=sj.handle.remote_workspace,
                    poll_error=reason,
                )
        sj.last_status_write = now

    def _stamp_scheduler_artifacts_unavailable(
        self, jobid: str, *, reason: str
    ) -> None:
        """Record that the workspace could not be staged home before reaping.

        The job is about to be classified terminal from its durable exit marker
        even though its artifacts are unreachable, so the reason is persisted on
        the spec: a `completed` row whose outputs are missing must be
        distinguishable from one whose outputs are simply not fetched yet, or an
        operator will chase a phantom fetch bug. Best-effort -- a failure to
        annotate must never block the reap the caller is about to perform.
        """
        sj = self._scheduler_running.get(jobid)
        spec_path = self._active_spec_path(jobid, sj)
        with paths.spec_lock(spec_path):
            try:
                fresh, _ = self._read_active_spec(
                    jobid,
                    sj,
                    spec_path=spec_path,
                )
            except (OSError, ValueError):
                return
            if fresh.is_terminal:
                return
            fresh.scheduler_state = "artifacts_unavailable"
            fresh.last_heartbeat_at = utcnow_iso()
            try:
                fresh.write(spec_path)
            except OSError as exc:  # pragma: no cover - annotation is advisory
                log.warning(
                    "job %s: could not record artifacts_unavailable: %s",
                    jobid,
                    exc,
                )

    def _stamp_scheduler_fetch_failed(
        self, jobid: str, sj: _SchedulerJob, spec: JobSpec, *, reason: str
    ) -> None:
        """Record that PBS finished but workspace staging failed and will retry."""
        cluster_state = "fetch_failed"
        transitioned = spec.scheduler_state != cluster_state
        now = time.monotonic()
        if (
            not transitioned
            and (now - sj.last_status_write) < SCHEDULER_STATUS_REFRESH_SECONDS
        ):
            return
        spec_path = self._active_spec_path(jobid, sj)
        with paths.spec_lock(spec_path):
            try:
                fresh, _ = self._read_active_spec(
                    jobid,
                    sj,
                    spec_path=spec_path,
                )
            except (OSError, ValueError):
                return
            if fresh.is_terminal:
                return
            transitioned = fresh.scheduler_state != cluster_state
            previous = fresh.state
            fresh.scheduler_state = cluster_state
            fresh.last_heartbeat_at = utcnow_iso()
            fresh.write(spec_path)
            if transitioned:
                events.state_transition(
                    Path(fresh.cwd),
                    jobid,
                    from_state=previous.value,
                    to_state=previous.value,
                    reason="scheduler workspace fetch failed; daemon will retry",
                    scheduler_target=fresh.scheduler_target,
                    scheduler_job_id=fresh.scheduler_job_id,
                    remote_workspace=sj.handle.remote_workspace,
                    fetch_error=reason,
                )
        sj.last_status_write = now

    def _stamp_scheduler_marker_probe_failed(
        self,
        jobid: str,
        sj: _SchedulerJob,
        spec: JobSpec,
        *,
        artifact_index: int | None,
        reason: str,
    ) -> None:
        """Record that PBS finished but marker probing failed and will retry."""
        cluster_state = "marker_probe_failed"
        transitioned = spec.scheduler_state != cluster_state
        now = time.monotonic()
        if (
            not transitioned
            and (now - sj.last_status_write) < SCHEDULER_STATUS_REFRESH_SECONDS
        ):
            return
        remote_marker = f"{sj.handle.remote_workspace}/{EXIT_MARKER_RELPATH}"
        if artifact_index is not None:
            remote_marker = f"{remote_marker}.{artifact_index}"
        spec_path = self._active_spec_path(jobid, sj)
        with paths.spec_lock(spec_path):
            try:
                fresh, _ = self._read_active_spec(
                    jobid,
                    sj,
                    spec_path=spec_path,
                )
            except (OSError, ValueError):
                return
            if fresh.is_terminal:
                return
            transitioned = fresh.scheduler_state != cluster_state
            previous = fresh.state
            fresh.scheduler_state = cluster_state
            fresh.last_heartbeat_at = utcnow_iso()
            fresh.write(spec_path)
            if transitioned:
                events.state_transition(
                    Path(fresh.cwd),
                    jobid,
                    from_state=previous.value,
                    to_state=previous.value,
                    reason="scheduler exit-marker probe failed; daemon will retry",
                    scheduler_target=fresh.scheduler_target,
                    scheduler_job_id=fresh.scheduler_job_id,
                    remote_workspace=sj.handle.remote_workspace,
                    remote_exit_marker=remote_marker,
                    marker_probe_error=reason,
                )
        sj.last_status_write = now

    def _stamp_scheduler_status(
        self,
        jobid: str,
        sj: _SchedulerJob,
        spec: JobSpec,
        phase: SchedulerPhase,
        detail: QstatDetail | None,
        *,
        queued_reason: str | None = None,
    ) -> None:
        """Record the cluster-side status + qstat detail on a live job (§18).

        Propagates qstat's coarse phase to ``spec.scheduler_state`` so a user
        (through coordinator, via ``vq status``) can tell "submitted but still
        queued behind other jobs" from "actually computing on a node" -- the vq
        ``state`` reads RUNNING from the moment we qsub, which hides that. When
        available, also stamps the exec host + walltime-used/limit. Writes on a
        queued->running transition *or* every ``SCHEDULER_STATUS_REFRESH_SECONDS``
        (so the live elapsed walltime stays fresh without rewriting the spec
        every poll), refreshing ``last_heartbeat_at`` as the scheduler-
        reconciliation liveness timestamp.
        """
        if detail is not None and detail.raw_state == "H":
            cluster_state = "held"
        elif phase is SchedulerPhase.RUNNING:
            cluster_state = "running"
        else:
            cluster_state = "queued"
        # Why the cluster has not started this job, in the scheduler's own
        # words: Slurm reports it on the coarse poll, Torque on the detail
        # record, so take whichever arrived. A job that is no longer pending
        # has no such answer, and keeping the last one would leave a running
        # job explaining why it is waiting.
        observed_reason = (
            (queued_reason or (detail.queued_reason if detail is not None else None))
            if phase is SchedulerPhase.PENDING
            else None
        )
        transitioned = spec.scheduler_state != cluster_state
        now = time.monotonic()
        # The reason deliberately does not force a write of its own: some
        # schedulers rewrite a queued job's comment on every cycle (an
        # estimated start time, a queue position), and honouring that would
        # cost one spec write per poll per job. It rides the same refresh
        # budget as the walltime below. Clearing is unaffected, because
        # leaving PENDING is itself a transition.
        if not transitioned and (now - sj.last_status_write) < SCHEDULER_STATUS_REFRESH_SECONDS:
            return  # unchanged + refreshed recently -> skip the write (low churn)
        spec_path = self._active_spec_path(jobid, sj)
        with paths.spec_lock(spec_path):
            try:
                fresh, _ = self._read_active_spec(
                    jobid,
                    sj,
                    spec_path=spec_path,
                )
            except (OSError, ValueError):
                return
            if fresh.is_terminal:
                return  # a racing kill won the spec; don't stamp over it
            fresh.scheduler_state = cluster_state
            fresh.last_heartbeat_at = utcnow_iso()
            # Unlike the fields below, this one is assigned rather than merged:
            # a stale explanation is worse than none, so an absent reason
            # clears it.
            fresh.scheduler_queued_reason = observed_reason
            if detail is not None:
                # Only overwrite with non-empty values (a queued job has no
                # exec_host / resources_used yet -- keep any prior reading).
                if detail.exec_host:
                    fresh.scheduler_exec_host = detail.exec_host
                if detail.walltime_used:
                    fresh.scheduler_walltime_used = detail.walltime_used
                if detail.walltime_limit:
                    fresh.scheduler_walltime_limit = detail.walltime_limit
            fresh.write(spec_path)
        sj.last_status_write = now

    def _iter_specs(self) -> Iterator[JobSpec]:
        """Iterate over all specs in the queue(s).

        In multi-user mode, scan every per-user queue before yielding. Bare job
        ids are compatibility identifiers rather than storage identities, so a
        duplicate across user trees is quarantined in full: neither record is
        yielded and neither is entered in ``_job_uid``. Unrelated unique jobs
        continue normally. In single-user mode, iterate over ``self.queue_dir``
        only.
        """
        self._colliding_scheduler_reservations = []
        if self._multi_user:
            self._job_uid.clear()
            self._job_spec_paths.clear()
            records: list[tuple[str, Path, JobSpec]] = []
            paths_by_jobid: dict[str, list[Path]] = {}
            readable_paths: set[Path] = set()
            for user_dir in paths._all_user_dirs():
                uid = user_dir.name
                qdir = paths.user_queue_dir(uid)
                if not qdir.is_dir():
                    continue
                for path in sorted(qdir.glob("*.json")):
                    try:
                        spec = _read_untrusted_multi_user_spec(path)
                    except Exception:
                        log.exception("failed to read %s; skipping", path)
                        continue
                    if path.name != f"{spec.id}.json":
                        log.error(
                            "spec filename %s does not match inner job id %r; "
                            "quarantining the record without mutation",
                            path,
                            spec.id,
                        )
                        continue
                    # A closed private binding is a durable lifecycle
                    # tombstone written before the corresponding mutable row.
                    # If the daemon died in that narrow window (or an owner
                    # replaced the row), the private final/retry snapshot wins.
                    try:
                        closed_binding = _read_scheduler_submit_binding(
                            uid,
                            spec.id,
                        )
                    except (OSError, ValueError):
                        closed_binding = None
                    if (
                        closed_binding is not None
                        and closed_binding.transaction_state == "closed"
                        and closed_binding.admitted_spec.model_dump(mode="json")
                        != spec.model_dump(mode="json")
                    ):
                        with paths.spec_lock(path):
                            spec = closed_binding.admitted_spec.model_copy(deep=True)
                            spec.write(path)
                    records.append((uid, path, spec))
                    readable_paths.add(path)
                    paths_by_jobid.setdefault(spec.id, []).append(path)
            # A user may delete or corrupt the mutable queue row while qsub is
            # in flight. The private binding is the durable authority created
            # before scheduler mutation; reconstruct its exact admitted row so
            # restart cannot forget a possible allocation, release capacity,
            # or replay the command. This never repairs an existing readable
            # row: ordinary admission below still validates/quarantines it.
            for binding in _iter_scheduler_submit_bindings():
                if not _scheduler_submit_binding_may_be_open(binding):
                    continue
                path = paths.user_spec_path(binding.owner_uid, binding.job_id)
                if path in readable_paths:
                    continue
                with paths.spec_lock(path):
                    try:
                        restored = _read_untrusted_multi_user_spec(path)
                    except (OSError, ValueError):
                        restored = None
                    if (
                        restored is None
                        or restored.id != binding.job_id
                        or restored.submitter != binding.owner_uid
                    ):
                        restored = binding.admitted_spec.model_copy(deep=True)
                        restored.id = binding.job_id
                        restored.submitter = binding.owner_uid
                        restored.scheduler_target = binding.scheduler_target
                        restored.cwd = binding.cwd
                        restored.cpus = binding.cpus
                        restored.scheduler_job_id = binding.scheduler_job_id
                        if not restored.is_terminal and restored.state not in {
                            JobState.SUBMITTING,
                            JobState.SUBMIT_OUTCOME_UNKNOWN,
                            JobState.RUNNING,
                            JobState.SUSPENDED,
                        }:
                            restored.state = JobState.SUBMIT_OUTCOME_UNKNOWN
                            restored.scheduler_state = "submit_outcome_unknown"
                            restored.finished_at = None
                            restored.exit_code = None
                        restored.failure_reason = (
                            "queue record restored from daemon-owned scheduler "
                            "submit authority"
                        )
                        restored.write(path)
                if path.name != f"{restored.id}.json":
                    continue
                records.append((binding.owner_uid, path, restored))
                readable_paths.add(path)
                paths_by_jobid.setdefault(restored.id, []).append(path)
            colliding = {
                jobid
                for jobid, spec_paths in paths_by_jobid.items()
                if len(spec_paths) > 1
            }
            for jobid in sorted(colliding):
                log.error(
                    "job id %s exists in multiple per-user queues; "
                    "quarantining every colliding record",
                    jobid,
                )
            self._colliding_scheduler_reservations = [
                (uid, path, spec)
                for uid, path, spec in records
                if spec.id in colliding
                and (
                    self._scheduler_transaction_may_exist(spec)
                    or self._owner_scheduler_binding_requires_fence(uid, spec.id)
                )
            ]
            for uid, path, spec in records:
                if spec.id in colliding:
                    continue
                self._job_uid[spec.id] = uid
                self._job_spec_paths[spec.id] = path
                yield spec
        else:
            for path in sorted(self.queue_dir.glob("*.json")):
                try:
                    yield JobSpec.read(path)
                except Exception:
                    log.exception("failed to read %s; skipping", path)

    def _spec_path(self, jobid: str) -> Path:
        """Resolve the spec path for ``jobid``.

        In multi-user mode, uses the ``_job_uid`` mapping.
        In single-user mode, returns the legacy queue-dir path.
        """
        if self._multi_user:
            # Once admitted, owner-qualified storage is immutable even if a
            # colliding bare id appears in another user's tree later.
            runtime = (
                self._running.get(jobid)
                or self._scheduler_running.get(jobid)
                or self._orphans.get(jobid)
            )
            if runtime is not None:
                captured = getattr(runtime, "spec_path", None)
                if captured is not None:
                    return captured
                owner = getattr(runtime, "owner_uid", None)
                if owner is None:
                    owner = getattr(runtime, "uid", None)
                if owner is not None:
                    return paths.user_spec_path(owner, jobid)
            captured = self._job_spec_paths.get(jobid)
            if captured is not None:
                return captured
            uid = self._job_uid.get(jobid)
            if uid is not None:
                return paths.user_spec_path(uid, jobid)
            # Fallback: search all user dirs.
            return paths.resolve_spec_path(jobid, multi_user=True)
        return self.queue_dir / f"{jobid}.json"

    def _active_spec_path(
        self,
        jobid: str,
        runtime: object | None = None,
        *,
        owner_uid: str | None = None,
        spec_path: Path | None = None,
    ) -> Path:
        """Resolve an active job through its immutable admission identity.

        The optional fallbacks preserve compatibility for tests and legacy
        in-memory records created before the identity fields existed. New
        admissions always carry both fields.
        """
        captured_path = spec_path
        if captured_path is None and runtime is not None:
            captured_path = getattr(runtime, "spec_path", None)
        if captured_path is not None:
            return captured_path
        captured_owner = owner_uid
        if captured_owner is None and runtime is not None:
            captured_owner = getattr(runtime, "owner_uid", None)
            if captured_owner is None:
                captured_owner = getattr(runtime, "uid", None)
        if self._multi_user and captured_owner is not None:
            return paths.user_spec_path(captured_owner, jobid)
        if self._multi_user:
            admitted_path = self._job_spec_paths.get(jobid)
            if admitted_path is not None:
                return admitted_path
        return self._spec_path(jobid)

    def _read_active_spec(
        self,
        jobid: str,
        runtime: object | None = None,
        *,
        owner_uid: str | None = None,
        spec_path: Path | None = None,
    ) -> tuple[JobSpec, Path]:
        """Read an active record through its immutable admission identity.

        A multi-user queue owner may edit the JSON while a job runs. The path,
        owner, and in-memory tracker key are immutable after admission; the
        inner ``id`` is not. Canonicalize a safe-but-mismatched reread in
        memory before any retry, cleanup, cgroup, event, or write sink can use
        it. Invalid ids are rejected by ``JobSpec`` before reaching this point.
        The exact admitted path is returned so callers can lock and write the
        same record without resolving from mutable fields.
        """
        admitted_path = self._active_spec_path(
            jobid,
            runtime,
            owner_uid=owner_uid,
            spec_path=spec_path,
        )
        if self._multi_user:
            spec = _read_untrusted_multi_user_spec(admitted_path)
        else:
            spec = JobSpec.read(admitted_path)
        if self._multi_user and spec.id != jobid:
            supplied_id = spec.id
            spec.id = jobid
            log.error(
                "active job %s record at %s changed inner id to %r; "
                "retaining immutable admitted identity",
                jobid,
                admitted_path,
                supplied_id,
            )
        return spec, admitted_path

    def _close_running_logs(self) -> None:
        for rj in self._running.values():
            rj.close_logs()

    def _install_stop_handlers(self) -> None:
        """Take over SIGTERM/SIGINT (and SIGHUP) before any long startup work.

        Called at the top of :meth:`run`, ahead of the startup walk, so that
        a stop arriving while the daemon is still scanning specs is recorded
        and logged instead of killing the process at its default disposition
        (#53). The handler only sets a flag; the walk and the main loop are
        what act on it.
        """
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)
        # SIGHUP = reload config, the POSIX convention. Guarded: SIGHUP does not
        # exist on Windows, and signal.signal raises off the main thread.
        with contextlib.suppress(ValueError, AttributeError, OSError):
            signal.signal(signal.SIGHUP, self._handle_reload_signal)

    def _handle_signal(self, signum: int, frame: FrameType | None) -> None:
        log.info("received signal %d; stopping after current iteration", signum)
        self._stop = True
        self._scheduler_refresh_wakeup.set()

    def _handle_reload_signal(self, signum: int, frame: FrameType | None) -> None:
        """SIGHUP: request a config reload on the next iteration.

        Sets a flag and returns — no config reading, no logging decisions, no
        locks inside a signal handler. :meth:`_maybe_apply_config_reload`
        consumes it at the top of the next ``iterate()``.
        """
        self._config_reload_requested = True

    def request_config_reload(self) -> None:
        """Queue a config reload. Safe to call from the RPC thread.

        The work happens on the main loop, so an RPC caller never waits on
        config parsing and the reload can never interleave with a dispatch.
        """
        self._config_reload_requested = True

    def _config_unusable(self) -> bool:
        """Park dispatch while the on-disk config does not parse.

        Only fires when the config has *changed* since the daemon read it and
        the new content is invalid. Dispatching under those conditions means
        every job that needs config either fails or silently uses state the
        operator has already replaced — the failure mode BUG 4 is about. Parking
        is loud and recoverable; the alternative is quiet and is not.

        A valid change needs no gate: :meth:`_scheduler_dispatcher_for` picks it
        up on the same pass via the fingerprint.
        """
        current = _config_fingerprint()
        if current == self._config_fingerprint:
            return False
        if current is None and self._config_fingerprint is not None:
            # The config file VANISHED under a running daemon (moved aside
            # mid-edit, an atomic replace we caught between unlink and rename,
            # a botched deploy). load_config() would happily return an empty
            # Config, and the dispatcher rebuild this change introduced would
            # then raise ConfigError("unknown host") and mark every PENDING
            # scheduler job terminally FAILED. Before the rebuild those jobs
            # rode the cached dispatcher and survived. Park instead: a missing
            # config is never a state to dispatch under, and parking is
            # recoverable where a terminal FAILED is not.
            if self._config_error_logged != "missing":
                log.error(
                    "config file %s has disappeared. HOLDING all dispatch "
                    "until it is back — restore it (the daemon picks it up "
                    "automatically) or run `vq daemon reload`.",
                    config_path(),
                )
                self._config_error_logged = "missing"
            return True
        try:
            load_config()
        except ConfigError as e:
            message = str(e)
            if self._config_error_logged != message:
                log.error(
                    "config on disk changed and no longer parses: %s. "
                    "HOLDING all dispatch until it is valid again — fix the "
                    "file (the daemon picks it up automatically), or run "
                    "`vq daemon reload` to retry now.",
                    message,
                )
                self._config_error_logged = message
            return True
        if self._config_error_logged is not None:
            log.info("config on disk parses again; resuming dispatch")
            self._config_error_logged = None
        self._config_fingerprint = current
        return False

    def _maybe_apply_config_reload(self) -> None:
        """Apply a requested reload, or refuse it if the config is broken.

        Called first in :meth:`iterate`. Adopting an invalid config would be
        strictly worse than staleness, so a ``ConfigError`` keeps the running
        state and logs loudly instead.

        ``multi_user`` is deliberately NOT reloaded: it selects the state-dir
        layout, the queue lock, the RPC socket path, the pidfile, and the log
        file. Changing it in place would leave the daemon half-migrated. A
        change there requires a restart and says so.
        """
        if not self._config_reload_requested:
            return
        self._config_reload_requested = False
        try:
            cfg = load_config()
        except ConfigError as e:
            log.error(
                "config reload REFUSED: %s — keeping the previously loaded "
                "config. Fix the file and reload again.",
                e,
            )
            return
        if cfg.multi_user.enabled != self._multi_user:
            log.warning(
                "config reload: multi_user changed (%s -> %s) but cannot be "
                "applied in place — it selects the state layout, queue lock, "
                "RPC socket, and pidfile. Restart the daemon to adopt it. "
                "Every other config change from this reload IS applied.",
                self._multi_user,
                cfg.multi_user.enabled,
            )
        self._scheduler_dispatchers.clear()
        self._scheduler_dispatcher_fingerprints.clear()
        self._config_fingerprint = _config_fingerprint()
        self._config_error_logged = None
        log.info(
            "config reloaded; scheduler dispatcher cache cleared (in-flight "
            "jobs keep the dispatcher they were submitted with)"
        )
