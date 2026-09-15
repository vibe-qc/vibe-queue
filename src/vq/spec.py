"""Job spec: on-disk record of a queued, running, or finished job.

One JSON file per job lives at <queue_dir>/<id>.json. The daemon reads, mutates,
and writes it as the job moves through its state machine; the client reads it
to render `vq queue`, `vq status`, etc.

Spec evolution: bump SPEC_VERSION when adding required fields or changing the
meaning of existing ones. Optional fields can be added without a version bump
because parsing tolerates unknown keys (forward-compat) and missing optional
keys (backward-compat through pydantic defaults).

Schema versions:
* v1 (v0.1.0 - v0.2.x): id, command, cwd, cpus, state, timestamps, pid,
  exit_code, log paths.
* v2 (v0.3.0+): adds mem_mb and wall_time_seconds (resource declarations),
  pgid (process group id, for clean kills + daemon recovery), and the optional
  last_heartbeat_at liveness timestamp. Adds OOM_KILLED, STARVED, and
  TIME_EXCEEDED terminal states.

Old v1 specs read into v2 cleanly: missing fields default to None.
A v1-only daemon refuses to load v2 specs (forward-compat is not free).
"""
from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator

from vq._storage import atomic_write_text

SPEC_VERSION = 2

# v0.5.34: charset for ``job_name``. Strict: alnum + ``-``, ``_``, ``.`` only.
# - Filesystem-safe on every OS (no ``/``, no spaces, no shell metacharacters)
# - No quoting concerns when the name flows into archive filenames + fetch
#   destination directories
# - Predictable: what the user typed is exactly what lands on disk
# Max length 50 chars: long enough for a readable label like
# ``mgo-pbe-rev2-2x2x2-supercell``, short enough that a NAME column in
# ``vq queue`` doesn't blow the terminal width.
JOB_NAME_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,50}$")
JOB_NAME_MAX_LEN = 50

# Job ids are generated as 12 lowercase hex characters, but durable v1/v2
# specs previously accepted arbitrary strings and also contain older
# human-readable ids. Keep safe legacy ids readable without inheriting the
# job-name UI's arbitrary 50-character display limit. The 160-character bound
# leaves room for suffixes below common 255-byte filename-component limits and
# matches the bounded identifier posture used elsewhere in the control plane.
JOB_ID_MAX_LEN = 160
JOB_ID_PATTERN = re.compile(rf"^[A-Za-z0-9._-]{{1,{JOB_ID_MAX_LEN}}}$")


def validate_job_id(value: str) -> str:
    """Return a filesystem/path-component-safe job id or raise ``ValueError``.

    This deliberately does not require the current 12-hex generated shape:
    safe legacy ids remain valid.  ``.`` and ``..`` need an explicit rejection
    because they match the otherwise-safe punctuation charset but are directory
    traversal components when used as workspace names.
    """
    if value in {".", ".."} or not JOB_ID_PATTERN.fullmatch(value):
        raise ValueError(
            f"invalid job id {value!r}: must match {JOB_ID_PATTERN.pattern}, "
            f"must not be '.' or '..', and must be 1-{JOB_ID_MAX_LEN} ASCII "
            "characters (alphanumerics, '-', '_', '.' only)"
        )
    return value


class JobState(StrEnum):
    PENDING = "pending"
    # Scheduler-only dispatch transaction states. SUBMITTING is persisted
    # before qsub/sbatch; SUBMIT_OUTCOME_UNKNOWN is the fail-closed state when
    # the observer loses a possibly accepted submit. Neither is terminal and
    # neither may re-enter the ordinary PENDING dispatch path.
    SUBMITTING = "submitting"
    SUBMIT_OUTCOME_UNKNOWN = "submit_outcome_unknown"
    RUNNING = "running"
    # v0.5.1: SIGSTOP'd by `vq pause`. Non-terminal -- the process is
    # frozen but its RAM and file descriptors remain allocated. The
    # daemon still counts a SUSPENDED job against its CPU / memory /
    # max-jobs budgets (it didn't actually free anything). `vq resume`
    # SIGCONT's it back to RUNNING.
    SUSPENDED = "suspended"
    COMPLETED = "completed"
    FAILED = "failed"
    KILLED = "killed"
    INTERRUPTED = "interrupted"
    # v0.3 watchdog-driven terminal states. Distinguished from KILLED so the
    # user can tell "I killed it" from "the daemon killed it because the
    # job was misbehaving."
    OOM_KILLED = "oom_killed"
    STARVED = "starved"
    TIME_EXCEEDED = "time_exceeded"
    # v0.4.1: the queue itself ended this job for non-watchdog, non-user
    # reasons. Most commonly: an orphan that exited while the daemon was
    # down (so we never saw the exit code), or a startup recovery path
    # that decided the job couldn't be reattached (e.g. pgid is gone or
    # was never recorded). Distinguished from INTERRUPTED so the
    # submitter can tell "queue ended it" from "job vanished without
    # explanation"; the submitter should resubmit and also check
    # stdout.log / stderr.log to see if there's a Python traceback,
    # OOM hint, or successful output already on disk.
    ABORTED_BY_QUEUE = "aborted_by_queue"


TERMINAL_STATES: frozenset[JobState] = frozenset(
    {
        JobState.COMPLETED,
        JobState.FAILED,
        JobState.KILLED,
        JobState.INTERRUPTED,
        JobState.OOM_KILLED,
        JobState.STARVED,
        JobState.TIME_EXCEEDED,
        JobState.ABORTED_BY_QUEUE,
    }
)

# Watchdog-attributed terminal states -- subset of TERMINAL_STATES. Useful
# for filtering "watchdog killed me" from "I exited normally" in tooling
# (e.g. retry policies that re-queue OOM_KILLED with double mem_mb).
WATCHDOG_TERMINAL_STATES: frozenset[JobState] = frozenset(
    {
        JobState.OOM_KILLED,
        JobState.STARVED,
        JobState.TIME_EXCEEDED,
    }
)


def utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()


class ProgramRuntimePin(BaseModel):
    """Submit-time snapshot of configured ``--program`` runtime pins.

    BUG 101 additions: for a scheduler-target submit the pin records the
    TARGET's verified runtime identity (from the driver's deployment
    records), never the driver-local program of the same name. All fields
    are additive and optional, so old specs read clean.
    """

    expected_git_sha: str | None = None
    enforce_git_sha: bool = True
    """Whether ``expected_git_sha`` is a REQUIREMENT or just a record.

    Three different things used to land in ``expected_git_sha``: an explicit
    ``--expected-sha``, a configured ``[programs.NAME] expected_git_sha``, and
    -- when neither was given -- whatever the runtime happened to be at submit
    time. The first two are intent and must be enforced. The third is an
    observation, and enforcing it means any job queued across a runtime roll
    dies at dispatch with a pin mismatch.

    On a continuously-rolling fleet that is fatal rather than merely annoying:
    2026-08-01, all 123 of the campaign's bundle jobs failed because the
    release runtime rolled between their submit and their dispatch tick, which
    is precisely what an unpinned submit exists to survive.

    ``False`` keeps the observed SHA in the spec for provenance and skips the
    equality check, so the job runs on whatever is current -- "the calculation
    that is next shall always take the latest version". Defaults ``True`` so
    an old spec, and every explicitly pinned submit, is unchanged.
    """
    resolved_git_sha: str | None = None
    """Full SHA observed or authenticated immediately before dispatch.

    Unlike ``expected_git_sha``, this is never submit-time intent. The daemon
    fills it only after the dispatch provenance gate succeeds, then exports the
    same value as ``VQ_PROGRAM_GIT_SHA``. Old and still-pending specs read it as
    ``None``.
    """
    expected_import_version: str | None = None
    import_check: str | None = None
    import_symbols: list[str] = Field(default_factory=list)
    scheduler_host: str | None = None
    """The scheduler target the operator named (e.g. ``slurm-cluster-campaign``),
    preserved even though the JobSpec itself is owned by the driver."""
    resolved_executable: str | None = None
    """The target-side active runtime path (the immutable per-SHA wrapper)."""
    program_kind: str | None = None
    """``scheduler-runtime`` for a managed immutable deployment."""
    program_version: str | None = None
    """Human version derived from the deployment tag (e.g. ``0.15.50``)."""
    artifact_identity: str | None = None
    """Immutable artifact identity — today the per-SHA wrapper path, which
    embeds version and SHA and is never mutated in place."""


class JobSpec(BaseModel):
    """On-disk record for a single job."""

    model_config = ConfigDict(extra="ignore", validate_assignment=True)

    spec_version: int = SPEC_VERSION
    id: str
    command: list[str] = Field(min_length=1)
    cwd: str
    cpus: int = Field(ge=1)
    # Scheduler-only task/rank count. For SLURM this renders as
    # ``--ntasks=N`` while ``cpus`` remains ``--cpus-per-task`` and vq's local
    # accounting/thread cap. None preserves the historical one-task scheduler
    # shape. Additive: old specs read clean.
    scheduler_tasks: int | None = Field(default=None, ge=1)
    # v0.3: resource declarations. Optional in v2 (kept None for old specs
    # and for jobs submitted without explicit budgets); will become required
    # in v0.4 per SPEC.md once the watchdog is the assumed default.
    mem_mb: int | None = Field(default=None, ge=1)
    wall_time_seconds: int | None = Field(default=None, ge=1)
    # v0.5.29: dispatch priority. The daemon orders the pending queue by
    # (-priority, submitted_at): higher priority dispatches first; within
    # one priority level, FIFO by submission time (so the all-default
    # case is unchanged from pre-v0.5.29 pure-FIFO behaviour). Negative
    # values are allowed and mean "run after the default-priority work."
    # Additive field with a default — v1/v2 specs read into v0.5.29
    # cleanly, no SPEC_VERSION bump.
    priority: int = 0
    # v0.11.0: refresh-before-run. When set to a venv-env name (a
    # [programs.<env>] of kind="venv", e.g. "vibeqc-dev"), the daemon —
    # once this job is next to dispatch — holds new dispatch until running
    # jobs drain, runs admin.update_env(<env>) (git pull + update_script),
    # then dispatches this job on the freshly-rebuilt venv. A failed rebuild
    # lands the job in FAILED (with failure_reason) rather than running it
    # against a broken env. None = no refresh (the default). Additive —
    # pre-v0.11.0 specs read clean. See HANDOVER_VQ_REFRESH.md.
    refresh_before: str | None = None
    # v0.12.0: build-job marker. When set to a venv-env name, THIS spec is
    # the build job that rebuilds that env. It runs the env update_script
    # (via `vq build-env`) inside its own cgroup-capped, full-host,
    # exclusive job scope, so the build is accounted and bounded like any
    # other job rather than an uncapped inline daemon subprocess. The
    # daemon creates one such job per drain window when a --refresh job
    # needs <env> built, then points the waiting --refresh jobs at it.
    # None means an ordinary job (the default). Additive: pre-v0.12.0
    # specs read clean, no SPEC_VERSION bump.
    build_env: str | None = None
    # v0.5.30: opt-in auto-resume after a host reboot. When the daemon
    # restarts and finds this spec was RUNNING but its process group is
    # gone with no exit marker (the signature of a hard reboot / power
    # loss — the kernel SIGKILLs everything before the command wrapper can
    # write its marker), it normally lands in ABORTED_BY_QUEUE and the
    # user resubmits by hand. With recover_on_reboot=True the daemon
    # instead emits a sibling resubmit: fresh jobid, SAME command + SAME
    # workspace (so the job's own restart-from-disk logic — CRYSTAL
    # GUESSP=fort.20, PySCF chkfile, ORCA .gbw — can pick up where it
    # left off), parent_jobid linking the chain. Opt-in by design:
    # silent auto-resume after a thermal trip or OOM cascade is exactly
    # the wrong thing. Additive field — pre-v0.5.30 specs read clean.
    recover_on_reboot: bool = False
    # v0.5.30: set on an auto-resume sibling to the jobid it was
    # resubmitted FROM. None on originally-submitted jobs. Lets
    # `vq status` show the lineage (A -> B -> C across successive
    # reboots) without a separate ledger.
    parent_jobid: str | None = None
    # v0.5.31: retry-on-failure. retry_max is the budget set at submit
    # time via `--retry N` (0 = no retry, the default). retry_count is
    # how many retries the daemon has spent. When a job's command exits
    # non-zero AND retry_count < retry_max, the daemon re-enqueues it
    # (state -> PENDING, retry_count++, not_before set for exponential
    # backoff) instead of letting it land in FAILED. Only the plain
    # non-zero-exit FAILED transition is retryable — watchdog kills
    # (OOM_KILLED / STARVED / TIME_EXCEEDED), `vq kill` (KILLED), and
    # ABORTED_BY_QUEUE are NOT retried (they're caught by is_terminal
    # precedence before the retry check). Additive — pre-v0.5.31 specs
    # read clean.
    retry_max: int = Field(default=0, ge=0)
    retry_count: int = Field(default=0, ge=0)
    # v0.5.31: earliest dispatch time, ISO-8601. Set by the daemon when
    # it re-enqueues a job for retry-backoff; the dispatch loop skips a
    # PENDING job whose not_before is still in the future. None = no
    # constraint (dispatch as soon as budgets allow). A corrupt value
    # is treated as "ready now" so a bad timestamp can't trap a job.
    not_before: str | None = None
    # v0.6.51: job dependencies. List of predecessor jobids that
    # must reach COMPLETED before this job is eligible for dispatch
    # (SLURM `afterok` semantics). Empty list = no constraint
    # (legacy behaviour).
    #
    # Daemon policy on predecessor failure: if ANY predecessor lands
    # in a non-COMPLETED terminal state (FAILED / CANCELLED /
    # TIMEOUT / KILLED / OOM_KILLED / STARVED / TIME_EXCEEDED /
    # ABORTED_BY_QUEUE), the dependent transitions to FAILED with a
    # ``failure_reason`` naming the failing predecessor + state.
    # Cascade is one-hop only — a chain A → B → C with B failing
    # cascades to C as "predecessor B failed", not "predecessor A
    # failed via B."
    #
    # Missing predecessor (jobid not present in any queue dir at
    # dispatch time — e.g. `vq cleanup --delete` ran on it) is
    # treated conservatively as "still waiting" so a misclick on
    # cleanup doesn't silently fail the dependent. Operator can
    # `vq kill` to unblock.
    #
    # Self-dependency rejected at submit time (would never
    # dispatch). Cycles are NOT detected — operator's
    # responsibility; they manifest as "both jobs stay PENDING
    # forever" and surface in `vq queue`.
    #
    # Additive field: pre-v0.6.51 specs read clean as an empty
    # list (Pydantic Field default).
    depends_on: list[str] = Field(default_factory=list)
    # v0.7.8 *Knuth's Schedule*: afterany semantics. List of
    # predecessor jobids that must reach a TERMINAL state (any of
    # COMPLETED / FAILED / KILLED / OOM_KILLED / STARVED /
    # TIME_EXCEEDED / TIMEOUT / ABORTED_BY_QUEUE / INTERRUPTED)
    # before this job is eligible for dispatch. SLURM ``afterany``
    # semantics — complements the ``depends_on`` field which carries
    # SLURM ``afterok`` semantics (predecessor must succeed).
    #
    # Critical difference vs. ``depends_on``: a predecessor failure
    # in ``depends_on_any`` does NOT cascade-fail the dependent. The
    # whole point is "run me regardless of the predecessor's
    # outcome." Useful for cleanup / post-processing jobs that should
    # collect artefacts whether the upstream job succeeded, failed,
    # or got killed.
    #
    # Combines additively with ``depends_on``: a spec can carry both
    # lists, in which case the dispatch gate requires
    # ``all(depends_on succeeded) AND all(depends_on_any terminated)``.
    # Same validation discipline as ``depends_on`` (self-dependency
    # rejected at submit; missing predecessor is conservatively
    # "still waiting"; cross-list dedup is up to the operator).
    #
    # Additive field: pre-v0.7.8 specs read clean as an empty list.
    depends_on_any: list[str] = Field(default_factory=list)
    # v0.6.51: human-readable explanation when the queue transitions
    # a job to a failure/terminal state for a reason the submitter can't
    # infer from exit_code alone. Initial use: depends_on cascade-fail
    # ("predecessor JOBID failed (state=FAILED)"). Also used for
    # queue/operator-attributed kills (e.g. killing obsolete-version jobs
    # before an environment update) so status/wait/API clients can show the
    # submitter why the queue ended the job. None for the common cases where
    # exit_code + state already say everything. Additive — pre-v0.6.51 specs
    # read clean.
    failure_reason: str | None = None
    # v0.12.0: crash feedback. On a non-COMPLETED terminal transition
    # (FAILED / OOM_KILLED / STARVED / TIME_EXCEEDED), the daemon captures
    # the tail of the job's stderr.log here, so `vq status` shows WHY a
    # calculation died (the Python traceback, the CRYSTAL error line, the
    # ORCA abort) without the operator having to `vq fetch` + grep. Capped
    # to the last ~20 lines / ~4 KB. Best-effort: None when the log is
    # empty/missing or the read fails -- capturing it must never break the
    # daemon's terminal recording. COMPLETED jobs never set it. Additive:
    # pre-v0.12.0 specs read clean.
    failure_tail: str | None = None
    # v0.6.52: array job context. SLURM-style array submissions
    # (`vq submit --array N input.py`) spawn N spec files sharing a
    # group id; each gets a sequential index 0..N-1 and the total N.
    # All three are None for a non-array submit (the common case);
    # all three are set together when array_index is set.
    #
    # The daemon injects these as VQ_ARRAY_INDEX / VQ_ARRAY_TOTAL /
    # VQ_ARRAY_GROUP_ID environment variables in the dispatched
    # process, so the job's script can branch on its index without
    # having to read its own spec file.
    #
    # Additive — pre-v0.6.52 specs read clean (all three default
    # None). No new dispatch semantics: an array element is an
    # ordinary independent spec; the daemon doesn't gang-schedule
    # or otherwise treat the group atomically.
    array_index: int | None = Field(default=None, ge=0)
    array_total: int | None = Field(default=None, ge=1)
    array_group_id: str | None = None
    # v0.8.7 *Hoare's Triple*: linear-chain submission. `vq submit
    # --chain N script.py` spawns N specs where spec[k]
    # depends_on=[spec[k-1].jobid]. Each gets a sequential
    # `chain_index` 0..N-1 and the total N; all share a
    # `chain_group_id`. The daemon injects VQ_CHAIN_INDEX /
    # VQ_CHAIN_TOTAL / VQ_CHAIN_GROUP_ID environment variables at
    # dispatch so the job's script can branch on its position
    # (e.g. NEB image-by-image, DFT+U self-consistency iteration).
    #
    # Distinct from array: array siblings are independent + parallel
    # (no chain dep). Chain elements are strictly sequential — only
    # one runs at a time, and a failure cascades to subsequent links
    # (the existing depends_on semantic). The CLI rejects --chain
    # combined with --array.
    #
    # Additive — pre-v0.8.7 specs read clean (all three default None).
    chain_index: int | None = Field(default=None, ge=0)
    chain_total: int | None = Field(default=None, ge=1)
    chain_group_id: str | None = None
    # v0.8.8 *Turing's Halt*: convergence-flag-driven auto-resubmit.
    # When ``rerun_until_file_exists`` is set, the daemon checks the
    # file path on every COMPLETED terminal transition. If the file is
    # missing AND ``rerun_count < rerun_max``, the daemon spawns a
    # fresh clone of this spec (incremented rerun_count, depends_on
    # this jobid) so the script gets another iteration. If the file
    # is present, the rerun loop is done. If ``rerun_count`` reaches
    # ``rerun_max``, the daemon stops respawning and logs a warning
    # (the chain is left COMPLETED — the operator decides whether to
    # bump the cap and resubmit).
    #
    # The path may contain ``$VQ_WORKDIR`` as a literal token; the
    # daemon substitutes the spec's actual workdir at check time so
    # the script can write its convergence flag next to its other
    # output.
    #
    # Use case: DFT+U U-self-consistency (script writes CONVERGED to
    # ``$VQ_WORKDIR/CONVERGED`` when ``|U_new - U_old| < tol``); NEB
    # CI (script writes NEB_CONVERGED when force-tol met).
    #
    # FAILED transitions do NOT trigger rerun — the failure is the
    # signal that something's wrong, not "try again." Operators who
    # want failure retries already have ``--retry N`` (v0.5.31).
    rerun_until_file_exists: str | None = None
    rerun_max: int = Field(default=10, ge=0)
    rerun_count: int = Field(default=0, ge=0)
    # v0.6.54: per-job scratch workdir, distinct from the workspace
    # (cwd, which holds the submitted source). The daemon creates the
    # workdir at dispatch, chowns it to the run-uid in multi-user
    # mode, and injects its absolute path as VQ_WORKDIR in the child
    # process's environment. Persistence reasons:
    #
    # * `vq status` and `vq fetch` need to know where the workdir
    #   lives so the operator can see / pull back what the job wrote.
    # * The cleanup pass needs to find the workdir from the spec
    #   (workdirs live OUTSIDE the per-user jobs/ tree by design —
    #   isolating "long-lived scratch the operator might want" from
    #   "ephemeral run artefacts the daemon owns").
    # * A daemon restart needs to be able to re-locate the workdir
    #   of an orphan job.
    #
    # None for pre-v0.6.54 specs (read clean) AND for jobs submitted
    # with `--no-workdir` (a future opt-out — v0.6.54 always creates
    # one).
    workdir: str | None = None
    # v0.6.54: when True, daemon rmtree's the workdir as soon as the
    # job hits a terminal state. Default False so the workdir lingers
    # for the operator / chat to read until the cleanup-pass sweeps
    # it (configurable max-age, default 14 days). Set by
    # `vq submit --clean-tmp` for jobs where the script writes its
    # result back via stdout / fetch / events and the workdir bytes
    # have no operator value (basis-opt convergence runs that emit
    # the final basis as a stdout JSON line, e.g.).
    clean_workdir_on_terminal: bool = False
    # CLEAN-3 (v0.8.23): set by the daemon's auto-cleanup age-sweep when it
    # removes this job's workdir (a terminal job whose workdir outlived the
    # configured workdir_max_age). A breadcrumb so a later `vq fetch
    # --workdir` can say "the age-sweep removed it" instead of mis-blaming
    # --clean-tmp. Distinct from clean_workdir_on_terminal (the opt-in
    # immediate cleanup); the sweep stamps this even for jobs that did NOT
    # pass --clean-tmp.
    workdir_swept_at: str | None = None
    state: JobState = JobState.PENDING
    submitted_at: str = Field(default_factory=utcnow_iso)
    submitter: str | None = None
    workspace_source: str | None = None
    # BUG 104: opaque queue-authority idempotency binding.  The raw operator
    # key is never durable; only fixed-size hashes and the canonical intent
    # digest are stored so a crash between spec and claim publication can be
    # repaired by scanning specs.  Fresh resubmits/reruns default back to None.
    idempotency_key_hash: str | None = Field(default=None, exclude=True)
    submission_intent_digest: str | None = Field(default=None, exclude=True)
    submission_owner_hash: str | None = Field(default=None, exclude=True)
    # Complete-calculation QVF payload.  When set, this workspace was created
    # from a single declarative QVF container and the named file is both the
    # input and the result artifact.  vq resolves the managed ``--program``
    # runtime to the managed ``vibeqc run <name>`` entry point at submit time;
    # it never
    # treats the ZIP container as a Python source file.  Additive so older
    # specs continue to load with None.
    qvf_artifact_name: str | None = None
    pid: StrictInt | None = Field(default=None, gt=0)
    # v0.3: process group id, captured at dispatch via os.getpgid(popen.pid).
    # Kill operations target the pgid (so OMP child threads / MPI workers
    # all die together) and daemon recovery uses it to detect "process
    # really gone vs just renumbered."
    pgid: StrictInt | None = Field(default=None, gt=0)
    # v1.0 cluster backend (docs/pbs_dispatcher_backend_design.md §17): when set,
    # this job is NOT a local Popen child but a batch job dispatched to a
    # scheduler host (the HostConfig key, e.g. "pbs-cluster") by a driver daemon over
    # SSH+qsub. The driver resolves the host's scheduler config from its own
    # config and dispatches via the SchedulerDispatcher; pid/pgid stay None.
    # None (the default) preserves the all-local meaning for every existing job.
    scheduler_target: str | None = None
    # The scheduler's own job id (e.g. "12345.cluster"), captured at qsub so the
    # daemon can reattach / qstat / qdel it. None for local jobs.
    scheduler_job_id: str | None = None
    # Cluster-side phase from qstat, stamped by the driver on each reconcile
    # poll: "queued" (the qsub job is waiting in the cluster queue) or "running"
    # (executing on a node). Distinct from the vq lifecycle `state` above, which
    # reads RUNNING from the moment the daemon dispatches the qsub -- this field
    # is what tells "submitted but still queued behind other jobs" from "actually
    # computing". None for local jobs (and until the first poll). The driver also
    # refreshes `last_heartbeat_at` on transitions and periodic status writes
    # (§17/§18).
    scheduler_state: str | None = None
    # Fail-closed scheduler observation telemetry. An attempted timestamp is
    # recorded for every daemon reconciliation batch; success and error are
    # separate so an unavailable squeue/sacct response can never look like a
    # successful observation that proved a job terminal. The last error is
    # retained after recovery for operator forensics; a newer success timestamp
    # shows that the scheduler became observable again.
    # These fields are durable but deliberately excluded from ordinary
    # ``model_dump`` projections: queue/status JSON is an established public
    # wire shape. ``to_json`` below adds them only to the on-disk spec record.
    scheduler_poll_last_attempted_at: str | None = Field(default=None, exclude=True)
    scheduler_poll_last_success_at: str | None = Field(default=None, exclude=True)
    scheduler_poll_last_error_at: str | None = Field(default=None, exclude=True)
    scheduler_poll_last_error: str | None = Field(default=None, exclude=True)
    # v0.15.x queue hardening: once a terminal scheduler job is old enough for
    # retention cleanup, the local daemon removes the remote scheduler workspace
    # after the result tree has already been copied back locally. This timestamp
    # records that remote-side reclaim so later archive/delete sweeps do not
    # keep issuing redundant SSH rm calls. None for local jobs, live scheduler
    # jobs, and older terminal scheduler specs not yet swept.
    scheduler_remote_workspace_cleaned_at: str | None = None
    # Richer cluster-side detail from `qstat -f`, refreshed by the driver on each
    # detail poll (§18 item 2): which node(s) the job landed on, and how much of
    # its walltime budget it has used vs requested. All None until the job runs
    # (a queued job has no exec_host / resources_used yet) and for local jobs.
    scheduler_exec_host: str | None = None
    scheduler_walltime_used: str | None = None
    scheduler_walltime_limit: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    # Optional liveness timestamp introduced with schema v2. Current scheduler
    # reconciliation refreshes it for nonlocal scheduler jobs; local watchdog
    # sampling does not mutate the JobSpec.
    last_heartbeat_at: str | None = None
    # v0.5.1: pause/resume accounting. paused_at is set when state goes
    # to SUSPENDED, cleared on resume. paused_seconds_total accumulates
    # across pause cycles so wall-time enforcement excludes paused
    # intervals (otherwise a multi-hour pause would consume the user's
    # wall_time_seconds budget).
    paused_at: str | None = None
    # PA-1 (v0.8.22): the CLOCK_MONOTONIC reading at SIGSTOP, used to measure
    # the pause interval at resume. ``paused_at`` (a wall-clock ISO string) is
    # still stamped for human display, but a wall-clock step (NTP slew, manual
    # `date`) over a long pause would mis-bill the wall-time budget; monotonic
    # is immune. System-wide on Linux/macOS, so it's comparable across the
    # separate `vq pause` / `vq resume` processes (a paused, SIGSTOP'd job
    # can't survive a reboot, so the two readings are always same-boot).
    paused_monotonic_at: float | None = Field(
        default=None,
        ge=0,
        allow_inf_nan=False,
    )
    paused_seconds_total: float = Field(
        default=0.0,
        ge=0,
        allow_inf_nan=False,
    )
    # v0.6.22: free-form tag identifying WHO paused the job. Set
    # when `vq pause` / `vq pause --all` is invoked with
    # `--paused-by TAG`; cleared on resume. Used by `vq resume
    # --paused-by TAG` (and programmatic `resume_all(
    # paused_by_filter=...)`) to scope a resume to jobs paused by
    # a specific actor — e.g. a script that paused the queue for
    # a build can resume only what IT paused, leaving operator-
    # paused jobs paused.
    #
    # First-pauser-wins: if a job is already SUSPENDED when a
    # second pause arrives, the new ``paused_by`` is NOT applied
    # (the existing one stays). Matches the idempotent-pause
    # behavior — re-pausing an already-suspended job is a no-op.
    #
    # Same charset as ``job_name`` (alnum + `-` `_` `.`, ≤50 chars)
    # so the tag flows into CLI argv / ssh-shipped argv / log
    # lines without quoting. Additive field; pre-v0.6.22 specs
    # read clean (default None = "paused without a tag", which
    # is the legacy behavior).
    paused_by: str | None = None
    # Crash-safe pause transaction intent.  ``pause_job`` durably writes all
    # four fields while the spec is still RUNNING, before it sends SIGSTOP.
    # It then atomically replaces that intent with the ordinary SUSPENDED
    # fields above.  A process death in either gap therefore leaves enough
    # information for the daemon or an admin recovery command to finish the
    # exact pause instead of stranding a stopped process whose spec still says
    # RUNNING.  ``pause_intent_at is not None`` is the presence bit because an
    # untagged operator pause legitimately has ``pause_intent_by=None``.
    pause_intent_at: str | None = None
    pause_intent_monotonic_at: float | None = Field(
        default=None,
        ge=0,
        allow_inf_nan=False,
    )
    pause_intent_pgid: StrictInt | None = Field(default=None, gt=0)
    pause_intent_by: str | None = None
    exit_code: int | None = None
    stdout_path: str = "stdout.log"
    stderr_path: str = "stderr.log"
    # v0.5.10 cleanup-tracking metadata. Optional with None defaults so v0.3+
    # specs read into v0.5.10 cleanly. ``last_status_at`` / ``last_fetched_at``
    # are stamped by ``vq status`` / ``vq fetch`` (terminal specs only, to
    # avoid racing the daemon's writes on non-terminal specs); auto-cleanup
    # policy uses them as a "skip jobs the user looked at recently" gate.
    # ``archived_at`` + ``archive_path`` are set by ``vq cleanup --archive``:
    # the workspace tarball lives at ``archive_path``, the spec stays in the
    # queue (so ``vq queue`` / ``vq status`` still see the job, annotated
    # "(archived)"), and ``vq cleanup --restore`` un-tars + clears them.
    last_status_at: str | None = None
    last_fetched_at: str | None = None
    archived_at: str | None = None
    archive_path: str | None = None
    # v0.5.34: optional human-readable label. Decorative — does NOT
    # replace ``id`` as the canonical addressing key (per SLURM/PBS/AWS
    # Batch convention: opaque jobid is primary, name is for humans).
    # When set:
    #   * shown in ``vq queue`` (new NAME column, conditional on at
    #     least one spec having a name set, same pattern as v0.5.29's
    #     PRI column)
    #   * shown in ``vq status`` header
    #   * prefixes the fetch destination dir: ``<output_dir>/<name>-<jobid>/``
    #   * prefixes the archive filename: ``<archive_dir>/<name>-<jobid>.tar.bz2``
    # When None: behaviour is identical to pre-v0.5.34 (jobid-only paths,
    # no NAME column). Additive — pre-v0.5.34 specs read clean.
    # Charset is strict (alnum + ``-_.``) so the name flows into
    # filesystem paths and ssh-shipped argv without ever needing quoting.
    # Non-unique by design: two jobs with the same name is fine (their
    # full dest dirs / archives are still disambiguated by the ``-jobid``
    # suffix).
    job_name: str | None = None
    # v0.5.47: branch name the job was submitted with via `vq submit
    # --branch X`. Stored verbatim (canonical name or alias —
    # whatever the user typed); resolution to a python interpreter
    # path is a separate, eager step done at the submit CLI boundary.
    # Used by `vq admin update`'s surgical pause scoping
    # (provides_branches): when set, only this branch's env-update
    # invalidates the in-memory module cache, so other-branch jobs
    # don't get paused unnecessarily. None on jobs submitted without
    # --branch (the default-python path or --python override). Additive
    # field — pre-v0.5.47 specs read clean.
    branch: str | None = None
    # v0.12.0: optional program-registry identity submitted via
    # ``vq submit --program NAME``. This is metadata for dispatchers and job
    # scripts, not command rewriting: the command stays exactly what the user
    # submitted, while the daemon exposes ``VQ_PROGRAM=NAME`` at runtime and
    # scheduler templates can use it as a future per-program hook key.
    # Additive field - pre-v0.12.0 specs read clean with ``None``.
    program: str | None = None
    # v0.12.0: submit-time snapshot of the configured runtime pins for
    # ``program``. The daemon uses this instead of the live registry's current
    # expected pins so a queued job submitted for SHA A cannot be silently
    # blessed after ``vq admin update`` moves both the checkout and config to
    # SHA B before dispatch. None for old specs and unpinned programs.
    program_runtime_pin: ProgramRuntimePin | None = None
    # v0.5.50: PID-fingerprint anti-recycle. /proc/<pid>/stat field 22
    # is the process start time in clock ticks since boot (CLK_TCK
    # units; jiffies on most Linux). Captured at dispatch alongside
    # pid/pgid; the daemon-startup recovery path cross-checks the
    # current /proc/<pid>/stat field 22 against this value before
    # declaring "the recorded pgid is alive, treat as orphan". If
    # the kernel recycled spec.pid to a different process, the
    # start-time field won't match and the spec lands in
    # ABORTED_BY_QUEUE with reason='pid_recycled' instead of being
    # incorrectly treated as the original job's resumed process.
    # macOS / non-Linux hosts: None (no /proc), and the cross-check
    # is skipped (falls back to pre-v0.5.50 pgid-only liveness check).
    # Additive field — pre-v0.5.50 specs read clean.
    pid_start_time: int | None = None
    # v0.6.6: free-form set of label strings the operator attaches
    # at submit time via `vq submit --tag X --tag Y` (repeatable
    # flag). Used by `vq queue --tag X` to filter the listing
    # (AND-semantics: a row shows iff it has ALL the requested
    # tags) and shown in `vq status` for the operator. Charset
    # matches job_name (alnum + - _ . ; ≤50 chars) so tags flow
    # into the CLI argv and config-toml round-trip without
    # quoting. Default empty list. Stored deduped + sorted at
    # validate time so two submits with `--tag foo --tag bar`
    # vs `--tag bar --tag foo` round-trip identically.
    tags: list[str] = Field(default_factory=list)
    # v0.6.14: vibe-qc output-module integration. When the submitted
    # job is a Python script that uses ``vibeqc.run_job``, vq's
    # submit pre-flight (opt-in via ``--vibeqc-preflight``) executes
    # the script once with ``VIBEQC_DRY_RUN=1`` to harvest the
    # OutputPlan, then reads the resulting ``{stem}.system`` and
    # populates the three fields below. None / empty when the
    # pre-flight didn't run or didn't produce a manifest.
    #
    # * ``expected_outputs`` — relative paths (within the workspace)
    #   that the job is declared to produce. Used by ``vq fetch
    #   --outputs-only`` to tar only the declared files instead of
    #   the whole workspace, and by future ``vq watch`` to inotify
    #   the workspace for missing-but-expected outputs.
    # * ``output_stem`` — bare basename of the job's output files
    #   (e.g. ``"output-h2o"``). Surfaced in ``vq status`` so the
    #   operator can correlate the vq job with the on-disk
    #   ``output-h2o.{out,system,molden,...}`` family. None when no
    #   stem was declared.
    # * ``last_output_status`` — value of ``[outputs].status`` from
    #   the most recent read of ``{stem}.system``: ``"running"``,
    #   ``"complete"``, ``"crashed"``, or ``"dry_run"``. Refreshed by
    #   ``vq status`` so the listing can show "5/8 outputs written"
    #   without re-parsing the manifest from disk on every refresh.
    #
    # Additive — pre-v0.6.14 specs read clean (defaults are empty
    # list / None). See vibeqc docs/design_output_module.md.
    expected_outputs: list[str] = Field(default_factory=list)
    output_stem: str | None = None
    last_output_status: str | None = None

    @field_validator("id")
    @classmethod
    def _check_id(cls, value: str) -> str:
        return validate_job_id(value)

    @field_validator("depends_on", "depends_on_any")
    @classmethod
    def _check_dependency_ids(cls, values: list[str]) -> list[str]:
        """Apply the durable job-id boundary to every predecessor."""
        return [validate_job_id(value) for value in values]

    @field_validator("spec_version")
    @classmethod
    def _check_version(cls, v: int) -> int:
        if v < 1:
            raise ValueError(f"spec version {v} is older than the oldest supported version (1)")
        if v > SPEC_VERSION:
            raise ValueError(
                f"spec version {v} is newer than this build supports "
                f"({SPEC_VERSION}); upgrade vq to read this file"
            )
        return v

    @field_validator("tags")
    @classmethod
    def _check_tags(cls, v: list[str]) -> list[str]:
        """v0.6.6: dedupe + sort tags at validate time so two submits
        with the same tags in different order round-trip to the same
        spec. Enforce the same strict charset as job_name so tags
        flow into CLI argv / config-toml / ssh argv without quoting,
        and bound each tag's length so a runaway --tag with a giant
        payload can't fill the spec file."""
        if not v:
            return []
        cleaned = sorted(set(v))
        for tag in cleaned:
            if not JOB_NAME_PATTERN.fullmatch(tag):
                raise ValueError(
                    f"invalid tag {tag!r}: must match "
                    f"{JOB_NAME_PATTERN.pattern} "
                    f"(alphanumerics, ``-``, ``_``, ``.`` only; "
                    f"1-{JOB_NAME_MAX_LEN} chars). Same strict charset "
                    f"as job_name — tags must flow into CLI argv + "
                    f"ssh-shipped argv without quoting."
                )
        return cleaned

    @field_validator("job_name")
    @classmethod
    def _check_job_name(cls, v: str | None) -> str | None:
        """Enforce strict charset for ``job_name`` (v0.5.34).

        Reject at spec construction so a bad name fails fast at submit
        time rather than producing a surprise filename later. Same
        regex applied by the CLI before we even build a JobSpec — this
        is the belt-and-braces backstop for direct JobSpec construction
        (in tests, in callers).
        """
        if v is None:
            return v
        if not JOB_NAME_PATTERN.fullmatch(v):
            raise ValueError(
                f"invalid job_name {v!r}: must match {JOB_NAME_PATTERN.pattern} "
                f"(alphanumerics, ``-``, ``_``, ``.`` only; "
                f"1-{JOB_NAME_MAX_LEN} chars). This keeps the name "
                "filesystem-safe across every OS and quoting-safe over ssh."
            )
        return v

    @field_validator("program")
    @classmethod
    def _check_program(cls, v: str | None) -> str | None:
        """Same strict identifier surface as ``job_name`` / tags.

        Program names are forwarded over SSH argv and exported as
        ``VQ_PROGRAM``. Keep them shell/filename friendly; existence in
        ``[programs.NAME]`` is a CLI-time config validation.
        """
        if v is None:
            return v
        if not JOB_NAME_PATTERN.fullmatch(v):
            raise ValueError(
                f"invalid program {v!r}: must match {JOB_NAME_PATTERN.pattern} "
                f"(alphanumerics, ``-``, ``_``, ``.`` only; "
                f"1-{JOB_NAME_MAX_LEN} chars). Program names are exported as "
                "VQ_PROGRAM and forwarded over SSH argv."
            )
        return v

    @field_validator(
        "idempotency_key_hash",
        "submission_intent_digest",
        "submission_owner_hash",
    )
    @classmethod
    def _check_submission_digest(cls, value: str | None) -> str | None:
        if value is not None and re.fullmatch(r"[0-9a-f]{64}", value) is None:
            raise ValueError(
                "submission idempotency digests must be 64 lowercase "
                "hexadecimal characters"
            )
        return value

    @field_validator("paused_by")
    @classmethod
    def _check_paused_by(cls, v: str | None) -> str | None:
        """v0.6.22: strict charset for the paused_by tag, identical
        to ``job_name`` / tags. Same rationale: the tag flows into
        ssh-shipped argv and log lines, so quoting-safety is the
        property we care about."""
        if v is None:
            return v
        if not JOB_NAME_PATTERN.fullmatch(v):
            raise ValueError(
                f"invalid paused_by {v!r}: must match "
                f"{JOB_NAME_PATTERN.pattern} (alphanumerics, ``-``, "
                f"``_``, ``.`` only; 1-{JOB_NAME_MAX_LEN} chars). Same "
                f"strict charset as job_name — paused_by must flow into "
                f"CLI argv + ssh-shipped argv without quoting."
            )
        return v

    @field_validator("pause_intent_by")
    @classmethod
    def _check_pause_intent_by(cls, v: str | None) -> str | None:
        """Pause intents use the same argv-safe actor-token contract."""
        if v is None:
            return v
        if not JOB_NAME_PATTERN.fullmatch(v):
            raise ValueError(
                f"invalid paused_by {v!r}: pause intent actor must match "
                f"{JOB_NAME_PATTERN.pattern} (alphanumerics, ``-``, "
                f"``_``, ``.`` only; 1-{JOB_NAME_MAX_LEN} chars)"
            )
        return v

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    @property
    def is_watchdog_killed(self) -> bool:
        """True iff the watchdog (not the user, not normal exit) ended this job."""
        return self.state in WATCHDOG_TERMINAL_STATES

    @property
    def is_archived(self) -> bool:
        """True iff the workspace has been tarred + removed by ``vq cleanup --archive``."""
        return self.archived_at is not None

    @property
    def dest_dirname(self) -> str:
        """v0.5.34: directory/file basename used for fetched workspaces and
        archive tarballs.

        Returns ``<job_name>-<jobid>`` when ``job_name`` is set, else
        ``<jobid>`` (pre-v0.5.34 behaviour). Used by:
        * ``cleanup.archive_workspace`` — both the outer ``.tar.bz2``
          filename and the tarball's internal top-level directory.
        * ``fetch_local`` / ``fetch_remote`` — the destination directory
          name under the user's ``-o output_dir``.
        * ``emit_workspace_tar`` — the streaming-tar ``arcname`` for
          non-archived jobs.

        The on-disk workspace (``<jobs_dir>/<jobid>/``) is unchanged —
        the daemon and watchdog still address jobs by jobid. ``job_name``
        only shapes user-visible artifacts.
        """
        if self.job_name:
            return f"{self.job_name}-{self.id}"
        return self.id

    def to_json(self) -> str:
        public = self.model_dump(mode="json")
        durable: dict[str, object] = {}
        for key, value in public.items():
            durable[key] = value
            if key == "workspace_source":
                durable.update(
                    {
                        "idempotency_key_hash": self.idempotency_key_hash,
                        "submission_intent_digest": self.submission_intent_digest,
                        "submission_owner_hash": self.submission_owner_hash,
                    }
                )
            if key == "scheduler_state":
                durable.update(
                    {
                        "scheduler_poll_last_attempted_at": (
                            self.scheduler_poll_last_attempted_at
                        ),
                        "scheduler_poll_last_success_at": (
                            self.scheduler_poll_last_success_at
                        ),
                        "scheduler_poll_last_error_at": (
                            self.scheduler_poll_last_error_at
                        ),
                        "scheduler_poll_last_error": self.scheduler_poll_last_error,
                    }
                )
        return json.dumps(durable, indent=2, ensure_ascii=False)

    @classmethod
    def from_json(cls, text: str) -> Self:
        return cls.model_validate_json(text)

    def write(self, path: Path) -> None:
        atomic_write_text(path, self.to_json())

    @classmethod
    def read(cls, path: Path) -> Self:
        return cls.from_json(path.read_text())


# Signal numbers identical across Linux and macOS (the POSIX core), so
# decoding stays correct no matter where the CLI renders versus where the job
# ran. Jobs run on Linux compute hosts, and the CLI may render on a macOS
# controller. Numbers that differ across platforms (SIGBUS, SIGUSR1/2,
# SIGCHLD, ...) are left to the "signal N" fallback rather than risk
# mislabeling them.
_PORTABLE_SIGNALS: dict[int, str] = {
    1: "SIGHUP", 2: "SIGINT", 3: "SIGQUIT", 4: "SIGILL", 5: "SIGTRAP",
    6: "SIGABRT", 8: "SIGFPE", 9: "SIGKILL", 11: "SIGSEGV", 13: "SIGPIPE",
    14: "SIGALRM", 15: "SIGTERM",
}
_SIGNAL_HINTS: dict[int, str] = {
    6: "abort, a failed assertion or uncaught C++ exception",
    8: "floating-point error",
    9: "hard kill, usually the OOM killer or a vq kill escalation",
    11: "segmentation fault",
    15: "graceful termination",
}


def signal_name_for_exit(exit_code: int | None) -> str | None:
    """The signal name when ``exit_code`` is bash's 128+sig for a portably-
    mapped signal (137 -> 'SIGKILL', 139 -> 'SIGSEGV'), else None. The short
    companion to describe_exit_code, for brief surfaces (notifications, the
    fetch suffix) where the full hint would be too long.
    """
    if exit_code is None or not (128 < exit_code <= 128 + 64):
        return None
    return _PORTABLE_SIGNALS.get(exit_code - 128)


def describe_exit_code(exit_code: int | None) -> str:
    """Render an exit code, decoding the shell-style 128+sig convention for a
    signaled termination (137 is SIGKILL, 139 is SIGSEGV). The daemon's wrapper
    reports a job killed by signal N as exit code 128+N (see daemon.py), which
    otherwise shows as an opaque number. The worst case is a hard SIGKILL or
    OOM, which also tends to leave no stderr to go on, so naming the signal is
    often the only crash clue the operator gets.
    """
    if exit_code is None:
        return "unknown"
    if 128 < exit_code <= 128 + 64:
        sig = exit_code - 128
        name = signal_name_for_exit(exit_code)
        if name is None:
            return f"{exit_code} (killed by signal {sig})"
        hint = _SIGNAL_HINTS.get(sig)
        inside = f"{name}: {hint}" if hint else name
        return f"{exit_code} (killed by {inside})"
    return str(exit_code)
