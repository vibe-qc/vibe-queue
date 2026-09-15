"""Queue listing: read all specs, format as a text table."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from vq import capacity, paths
from vq.host import LOCAL_HOST_ALIASES, is_local_host
from vq.spec import JobSpec, JobState, validate_job_id

# State display order for the queue listing: active states first, then terminal.
_STATE_ORDER: dict[JobState, int] = {
    JobState.RUNNING: 0,
    JobState.SUSPENDED: 1,
    JobState.SUBMITTING: 2,
    JobState.SUBMIT_OUTCOME_UNKNOWN: 3,
    JobState.PENDING: 4,
    JobState.INTERRUPTED: 5,
    JobState.FAILED: 6,
    JobState.KILLED: 7,
    JobState.OOM_KILLED: 8,
    JobState.STARVED: 9,
    JobState.TIME_EXCEEDED: 10,
    JobState.ABORTED_BY_QUEUE: 11,
    JobState.COMPLETED: 12,
}
_ACTIVE_CAPACITY_STATES: frozenset[JobState] = frozenset(
    {
        JobState.RUNNING,
        JobState.SUSPENDED,
        JobState.SUBMITTING,
        JobState.SUBMIT_OUTCOME_UNKNOWN,
    }
)
_DELEGATED_LOCAL_HANDLE_HOSTS: frozenset[str | None] = frozenset(
    {None, "", *LOCAL_HOST_ALIASES}
)

# Scheduler-backed specs deliberately retain JobState.RUNNING from successful
# qsub/sbatch handoff until vq has terminal proof.  That durable state is an
# ownership/reservation fact, not proof that a compute node is executing the
# job.  Queue and monitor projections therefore expose the scheduler's exact
# last phase while leaving the persisted lifecycle untouched.
SCHEDULER_QUEUE_PHASES: frozenset[str] = frozenset(
    {
        "queued",
        "held",
        "unpolled",
        "poll_failed",
        "finishing",
        "marker_probe_failed",
        "fetch_failed",
        "reattach_failed",
        "artifacts_unavailable",
        "scheduler_reconciliation_quarantined",
        "release_outcome_unknown",
        "scheduler_unknown",
    }
)
_SCHEDULER_DISPLAY_PHASES: frozenset[str] = frozenset(
    set(SCHEDULER_QUEUE_PHASES)
    | {
        "running",
        "submitting",
        "submit_outcome_unknown",
        "submit_outcome_unknown_after_terminal",
        "submit_evidence_conflict",
        "submit_evidence_conflict_after_terminal",
        "submit_rejected",
        "submit_rejected_after_terminal",
        "submit_cancel_pending_after_terminal",
        "submit_cancelled_after_terminal",
        "submit_reconciliation_quarantined",
        "submit_reconciliation_quarantined_after_terminal",
        "hold_outcome_unknown",
    }
)
QUEUE_FILTER_STATES: frozenset[str] = frozenset(
    {state.value for state in JobState} | set(SCHEDULER_QUEUE_PHASES)
)
_UNKNOWN_SCHEDULER_TARGET = "scheduler-target-unknown"


def _normalized_scheduler_target_value(target: str | None) -> str | None:
    if target is None:
        return None
    if (
        target
        and len(target) <= 128
        and target == target.strip()
        and target.isprintable()
    ):
        return target
    return _UNKNOWN_SCHEDULER_TARGET


def normalized_scheduler_target(spec: JobSpec) -> str | None:
    """Return a single-line bounded target label, preserving raw JSON only."""
    return _normalized_scheduler_target_value(spec.scheduler_target)


def scheduler_target_is_safe(target: object) -> bool:
    """Whether a raw target can be used as an actionable host label."""
    return (
        isinstance(target, str)
        and bool(target)
        and len(target) <= 128
        and target == target.strip()
        and target.isprintable()
    )


def queue_host_for_scheduler_target(target: object, fallback_host: str) -> str:
    """Return an actionable target host, never an unsafe diagnostic value."""
    if not scheduler_target_is_safe(target):
        return fallback_host
    assert isinstance(target, str)
    return target


def effective_queue_state(spec: JobSpec) -> str:
    """Return the monitor-facing state without changing durable ownership.

    Local and terminal jobs use their vq lifecycle state.  A live scheduler
    job whose durable state is RUNNING uses the exact last scheduler phase;
    no observation yet is explicitly ``unpolled``.  Consequently only an
    exact scheduler ``running`` observation is presented as running, while
    poll failures and copy-back fences remain visible and queryable.  A
    scheduler-backed SUSPENDED row is presented as ``held`` only when that
    exact remote phase was recorded; any other suspended-side observation is
    unknown rather than silently promoted to a confirmed hold.
    """
    if (
        spec.scheduler_target is not None
        and spec.state in {JobState.RUNNING, JobState.SUSPENDED}
        and not spec.is_terminal
    ):
        if not scheduler_target_is_safe(spec.scheduler_target):
            return "scheduler_unknown"
        phase = normalized_scheduler_phase(spec)
        if spec.state == JobState.RUNNING and (
            phase == JobState.RUNNING.value or phase in SCHEDULER_QUEUE_PHASES
        ):
            return phase
        if spec.state == JobState.SUSPENDED and phase == "held":
            return phase
        return "scheduler_unknown"
    return spec.state.value


def normalized_scheduler_phase(spec: JobSpec) -> str:
    """Return a bounded scheduler phase token for projection and display.

    Persisted and delegated specs are an input boundary. Keep their raw
    ``scheduler_state`` in JSON for diagnosis, but never interpolate an
    arbitrary value into terminal text or treat it as a supported filter.
    """
    phase = (
        "unpolled"
        if spec.scheduler_state is None
        else spec.scheduler_state
    )
    if phase in _SCHEDULER_DISPLAY_PHASES:
        return phase
    return "scheduler_unknown"


def scheduler_running_confirmed(spec: JobSpec) -> bool | None:
    """Whether a scheduler-owned RUNNING row has execution confirmation.

    ``None`` means this predicate does not apply (local, non-running, or
    terminal row).  ``False`` is deliberately not treated as idle: the
    scheduler observation may be queued, in a copy-back fence, or unavailable,
    while the raw lifecycle continues to reserve the accepted allocation.
    """
    if (
        spec.state != JobState.RUNNING
        or spec.is_terminal
    ):
        return None
    if spec.scheduler_target is None:
        return None
    if not scheduler_target_is_safe(spec.scheduler_target):
        return False
    return normalized_scheduler_phase(spec) == JobState.RUNNING.value


def matches_queue_state_filter(
    spec: JobSpec,
    selected: set[str],
    *,
    active: bool,
) -> bool:
    """Match lifecycle filters conservatively and scheduler phases exactly.

    Historical lifecycle filters retain their meaning: ``-s running`` includes
    every scheduler-owned raw RUNNING row so an unavailable observation cannot
    be mistaken for free capacity.  Exact scheduler phases are additional
    selectors (for example ``-s poll_failed``).  Per-row effective state and
    the table aggregate distinguish confirmation from reservation.
    """
    selected_phases = selected & SCHEDULER_QUEUE_PHASES
    return (
        (active and not spec.is_terminal)
        or spec.state.value in selected
        or effective_queue_state(spec) in selected
        or (
            spec.scheduler_target is not None
            and scheduler_target_is_safe(spec.scheduler_target)
            and not spec.is_terminal
            and spec.state in {JobState.RUNNING, JobState.SUSPENDED}
            and normalized_scheduler_phase(spec) in selected_phases
        )
    )


def queue_handle_for_spec(spec: JobSpec, host: str) -> dict[str, str]:
    """Return the stable queue back-reference for one persisted job."""
    return {
        "job_id": spec.id,
        "host": queue_host_for_scheduler_target(spec.scheduler_target, host),
        "submitted_at": spec.submitted_at,
    }


def queue_handle_without_spec(job_id: str, host: str) -> dict[str, str | None]:
    """Return the stable fallback when no persisted job can be read."""
    return {
        "job_id": job_id,
        "host": host,
        "submitted_at": None,
    }


def queue_handle_for_unvalidated_row(
    row: Mapping[str, object],
    host: str,
) -> dict[str, str | None] | None:
    """Build a non-misleading handle at an invalid delegated-row boundary.

    Once full ``JobSpec`` validation fails, an embedded handle cannot be
    trusted to name the same job as the diagnostic row.  Rebuild it only from
    a path-safe canonical top-level ``id``; otherwise expose no actionable
    back-reference.
    """
    raw_id = row.get("id")
    if not isinstance(raw_id, str):
        return None
    try:
        job_id = validate_job_id(raw_id)
    except ValueError:
        return None
    submitted_at = row.get("submitted_at")
    return {
        "job_id": job_id,
        "host": host,
        "submitted_at": (
            submitted_at if isinstance(submitted_at, str) else None
        ),
    }


def normalize_delegated_queue_handle_host(
    host: object,
    queue_host: str,
) -> object:
    """Rewrite only legacy local aliases in a delegated queue handle."""
    return queue_host if host in _DELEGATED_LOCAL_HANDLE_HOSTS else host


def list_jobs(
    host: str,
    queue_dir: Path | None = None,
    *,
    multi_user: bool = False,
) -> list[JobSpec]:
    """Return all specs in the queue dir(s).

    In multi-user mode, scans every per-user queue dir under
    ``<users_root>/<uid>/queue/``. In single-user mode, scans
    the single queue dir.

    Sorted with active states first, then by submission time
    (ascending so pending jobs appear in dispatch order).
    """
    if not is_local_host(host):
        raise NotImplementedError(f"remote queue listing for {host!r} not implemented in v0.1")
    specs: list[JobSpec] = []
    if multi_user:
        for user_dir in paths._all_user_dirs():
            qd = paths.user_queue_dir(user_dir.name)
            if not qd.is_dir():
                continue
            for path in qd.glob("*.json"):
                try:
                    specs.append(JobSpec.read(path))
                except Exception:
                    continue
    else:
        qd = queue_dir or paths.queue_dir()
        if not qd.exists():
            return []
        for path in qd.glob("*.json"):
            try:
                specs.append(JobSpec.read(path))
            except Exception:
                continue
    specs.sort(key=lambda s: (_STATE_ORDER.get(s.state, 99), s.submitted_at))
    return specs


def pending_queue_position(spec: JobSpec, pending: list[JobSpec]) -> tuple[int, int]:
    """v0.9.2: ``(rank, total)`` for ``spec`` among the ``pending`` jobs,
    ranked by the daemon's dispatch order — ``(-priority, submitted_at)``
    (higher priority first, then FIFO by submission time; this is exactly the
    key ``_dispatch_pending`` sorts by).

    Scheduler-backed jobs have independent target lanes: a pbs-cluster itwin pending
    job should not count unrelated localhost or pbs-cluster big jobs in its position.
    ``rank`` is 1-based and ``total`` is the number of pending jobs in the
    same scheduler-target lane as ``spec``; local jobs use the ``None`` lane.

    A ``rank`` of 1 means "first in line by sort order" — NOT a guaranteed
    next-to-run, since a job ahead may be held by an unmet dependency, a
    scheduled ``not_before`` start, or a resource budget. ``submitted_at`` is
    an ISO-8601 string, which sorts lexically in timestamp order.
    """
    def _key(s: JobSpec) -> tuple[int, str]:
        return (-s.priority, s.submitted_at)

    lane = [s for s in pending if s.scheduler_target == spec.scheduler_target]
    if all(s.id != spec.id for s in lane):
        lane.append(spec)
    my_key = _key(spec)
    ahead = sum(1 for s in lane if s.id != spec.id and _key(s) < my_key)
    return ahead + 1, len(lane)


def pending_configured_capacity_overages(
    spec: JobSpec,
    snapshot: capacity.DaemonCapacity | None,
) -> tuple[capacity.ConfiguredCapacityOverage, ...]:
    """Return configured-cap overages for an applicable pending local job.

    Scheduler jobs are admitted by the target scheduler, so the driver's
    local daemon caps never classify them. Missing capacity is unknown, not
    evidence that a request is impossible.
    """
    if (
        snapshot is None
        or spec.state != JobState.PENDING
        or spec.scheduler_target is not None
    ):
        return ()
    return capacity.configured_capacity_overages(
        cpus=spec.cpus,
        mem_mb=spec.mem_mb,
        snapshot=snapshot,
    )


def pending_configured_capacity_known(
    spec: JobSpec,
    snapshot: capacity.DaemonCapacity | None,
) -> bool:
    """Whether configured-cap classification applies and is available."""
    if (
        snapshot is None
        or spec.state != JobState.PENDING
        or spec.scheduler_target is not None
    ):
        return False
    # Mixed-version snapshots may predate advertisement of the daemon's
    # undeclared-memory charge. Distinguish an omitted field (unknown) from an
    # explicit null (the daemon applies no default charge).
    return not (
        spec.mem_mb is None
        and snapshot.max_mem_mb is not None
        and "default_job_mem_mb" not in snapshot.model_fields_set
    )


def _array_state_summary(elements: list[JobSpec]) -> str:
    """v0.7.10 *McCarthy's List*: render a compact state breakdown
    for an array group.

    Format: ``N/M done`` when every non-terminal element reached
    COMPLETED, ``N/M S`` for any single-state breakdown (e.g.
    ``30/30 R`` while all running), or a multi-state breakdown
    ``5P/3R/22C/30`` when the group has mixed states. The single-
    letter prefixes encode states for readability:

      P=PENDING, R=RUNNING, S=SUSPENDED, C=COMPLETED, F=FAILED,
      K=KILLED, O=OOM_KILLED, V=STARVED, T=TIME_EXCEEDED,
      I=INTERRUPTED, A=ABORTED_BY_QUEUE. Scheduler phases use
      U=SUBMITTING, Q=QUEUED, H=HELD, UP=UNPOLLED, PF=POLL_FAILED,
      FN=FINISHING,
      MP=MARKER_PROBE_FAILED, FF=FETCH_FAILED, RA=REATTACH_FAILED,
      AU=ARTIFACTS_UNAVAILABLE, RQ=RECONCILIATION_QUARANTINED,
      RO=RELEASE_OUTCOME_UNKNOWN, and SU=SCHEDULER_UNKNOWN.

    Trailing total ``/M`` keeps the eye anchored on "how many
    elements are there in this group?" — the most common question
    after submitting a large ``--array N``.
    """
    total = len(elements)
    counts: dict[str, int] = {}
    for s in elements:
        state = effective_queue_state(s)
        counts[state] = counts.get(state, 0) + 1
    # Single-state case → "N/M S".
    if len(counts) == 1:
        only_state = next(iter(counts.keys()))
        letter = _STATE_LETTER.get(only_state, "?")
        if only_state == JobState.COMPLETED.value:
            return f"{total}/{total} done"
        return f"{total}/{total} {letter}"
    # Mixed case: stable order so the line is the same after a
    # daemon tick that flips one element.
    parts = []
    for state in _STATE_RENDER_ORDER:
        n = counts.get(state, 0)
        if n:
            parts.append(f"{n}{_STATE_LETTER.get(state, '?')}")
    for state in sorted(key for key in counts if key not in _STATE_RENDER_ORDER):
        # A future scheduler phase must remain visible even before it receives
        # a compact token in the known-state table.
        parts.append(f"{counts[state]}[{state}]")
    return "/".join(parts) + f"/{total}"


_STATE_LETTER: dict[JobState | str, str] = {
    JobState.PENDING: "P",
    JobState.SUBMITTING: "U",
    JobState.SUBMIT_OUTCOME_UNKNOWN: "?",
    JobState.RUNNING: "R",
    JobState.SUSPENDED: "S",
    JobState.COMPLETED: "C",
    JobState.FAILED: "F",
    JobState.KILLED: "K",
    JobState.OOM_KILLED: "O",
    JobState.STARVED: "V",
    JobState.TIME_EXCEEDED: "T",
    JobState.INTERRUPTED: "I",
    JobState.ABORTED_BY_QUEUE: "A",
    "queued": "Q",
    "held": "H",
    "unpolled": "UP",
    "poll_failed": "PF",
    "finishing": "FN",
    "marker_probe_failed": "MP",
    "fetch_failed": "FF",
    "reattach_failed": "RA",
    "artifacts_unavailable": "AU",
    "scheduler_reconciliation_quarantined": "RQ",
    "release_outcome_unknown": "RO",
    "scheduler_unknown": "SU",
}

# Order the letters appear in a mixed-state summary. Pending +
# running first (operator-visible "still working" states), then
# success / failure terminals.
_STATE_RENDER_ORDER: list[JobState | str] = [
    JobState.PENDING,
    JobState.SUBMITTING,
    JobState.SUBMIT_OUTCOME_UNKNOWN,
    JobState.RUNNING,
    "queued",
    "held",
    "unpolled",
    "poll_failed",
    "finishing",
    "marker_probe_failed",
    "fetch_failed",
    "reattach_failed",
    "artifacts_unavailable",
    "scheduler_reconciliation_quarantined",
    "release_outcome_unknown",
    "scheduler_unknown",
    JobState.SUSPENDED,
    JobState.COMPLETED,
    JobState.FAILED,
    JobState.KILLED,
    JobState.OOM_KILLED,
    JobState.STARVED,
    JobState.TIME_EXCEEDED,
    JobState.INTERRUPTED,
    JobState.ABORTED_BY_QUEUE,
]


def format_table(
    specs: list[JobSpec],
    total_cpus: int | None = None,
    *,
    collapse_arrays: bool = False,
    capacity_snapshot: capacity.DaemonCapacity | None = None,
    additional_unconfirmed_scheduler_running: int = 0,
) -> str:
    """Format a list of specs as a fixed-width text table.

    Archived jobs (``vq cleanup --archive``) get a trailing
    ``(archived)`` next to the state so the user can tell at a glance
    that ``vq fetch`` will un-tar instead of copy a workspace dir.

    v0.5.29: a ``PRI`` column appears between STATE and CPUS *only when*
    at least one job in the listing has a non-zero ``priority``. The
    all-default-priority case (the overwhelming majority) sees no column
    at all — zero noise — but the moment a priority is in play it's
    visible without the user having to ask for it.

    v0.5.34: a ``NAME`` column appears between ID and STATE *only when*
    at least one job in the listing has ``job_name`` set. Same policy
    as PRI: zero noise when nobody's using names, full visibility once
    they are. Truncated to 20 chars (``…`` ellipsis) to keep total row
    width below typical 132-col terminal even with long names.

    v0.7.10 *McCarthy's List*: ``collapse_arrays=True`` folds
    every ``--array N`` group into a single ``ARRAY 5P/25C/30
    gid=...`` row keyed on ``array_group_id``. The synthetic row
    inherits CPUS, submitted_at, and command from the array's first
    element (they're identical across the group by design). Non-
    array specs render unchanged in the same table.
    """
    if not specs:
        lines = ["(no jobs)"]
        lines.extend(
            _scheduler_occupancy_lines(
                specs,
                additional_unconfirmed_scheduler_running,
            )
        )
        return "\n".join(lines)
    used = sum(s.cpus for s in specs if s.state in _ACTIVE_CAPACITY_STATES)

    # v0.7.10: partition into array groups (when requested) +
    # non-array specs. We preserve the input ordering's intent —
    # each array group's "representative" inherits the position of
    # the earliest-submitted element so the relative row order
    # stays stable across collapse / expand toggles.
    #
    # We track collapsed-array metadata in a sidecar dict keyed by
    # id() of the spec, since JobSpec is a Pydantic model and
    # stashing non-field attrs on it would fight validation.
    array_overrides: dict[int, tuple[str, str, str | None]] = {}
    if collapse_arrays:
        array_groups: dict[str, list[JobSpec]] = {}
        non_arrays: list[JobSpec] = []
        for s in specs:
            if s.array_group_id is not None:
                array_groups.setdefault(s.array_group_id, []).append(s)
            else:
                non_arrays.append(s)
        synthetic: list[JobSpec] = []
        for gid, elements in array_groups.items():
            elements.sort(key=lambda s: (s.array_index or 0))
            rep = elements[0]
            summary = _array_state_summary(elements)
            over_cap = sum(
                bool(
                    pending_configured_capacity_overages(
                        element,
                        capacity_snapshot,
                    )
                )
                for element in elements
            )
            state_label = f"ARRAY {summary}"
            if over_cap:
                state_label = f"{state_label} ({over_cap} over cap)"
            # Track the (id_label, state_label) override for this
            # synthetic row in the sidecar dict.
            cluster_label = _collapsed_array_scheduler_label(elements)
            array_overrides[id(rep)] = (gid, state_label, cluster_label)
            synthetic.append(rep)
        rendered = synthetic + non_arrays
        rendered.sort(
            key=lambda s: (_STATE_ORDER.get(s.state, 99), s.submitted_at),
        )
    else:
        rendered = specs

    # v0.5.29: only surface the PRI column when it carries information.
    show_priority = any(s.priority != 0 for s in rendered)
    # v0.5.34: same policy for the NAME column.
    show_name = any(s.job_name is not None for s in rendered)
    # v1.0 scheduler backend: show cluster-side queued/running only when at
    # least one row is a scheduler job. Local-only queues stay byte-for-byte
    # as compact as before.
    show_cluster = any(s.scheduler_target is not None for s in specs)
    # SLURM MPI jobs can request ranks separately from per-rank CPUs. Surface
    # that only when used so local/OpenMP listings stay compact.
    show_scheduler_tasks = any(s.scheduler_tasks is not None for s in rendered)

    # Header row composition. Order: ID [NAME] STATE [CLUSTER] [PRI] CPUS
    # SUBMITTED COMMAND.
    # NAME sits next to ID because they're both "what is this job"
    # identifiers from the user's perspective; PRI sits next to CPUS
    # because they're both resource-budget-related.
    header: list[str] = ["ID"]
    if show_name:
        header.append("NAME")
    header.append("STATE")
    if show_cluster:
        header.append("CLUSTER")
    if show_priority:
        header.append("PRI")
    header.append("CPUS")
    if show_scheduler_tasks:
        header.append("TASKS")
    header.extend(["SUBMITTED (UTC)", "COMMAND"])
    rows: list[tuple[str, ...]] = [tuple(header)]

    for s in rendered:
        # v0.7.10: a synthetic collapsed-array row has its ID +
        # state label pre-computed in the sidecar.
        override = array_overrides.get(id(s))
        if override is not None:
            id_label, state_label, cluster_label = override
        else:
            cluster_label = None
            id_label = s.id
            state_label = effective_queue_state(s)
            if s.is_archived:
                state_label = f"{state_label} (archived)"
            if s.state == JobState.SUSPENDED and s.paused_by:
                state_label = f"{state_label} (paused_by {s.paused_by})"
            if pending_configured_capacity_overages(s, capacity_snapshot):
                state_label = f"{state_label} (over cap)"
            # v0.5.31: annotate a job that has been retried at least once,
            # so a PENDING job sitting in retry-backoff is distinguishable
            # from a fresh PENDING (and a terminal job shows it burned
            # retries before giving up). Only shown once retry_count > 0 —
            # a job merely submitted with --retry but not yet failed adds
            # no noise.
            if s.retry_count > 0:
                state_label = f"{state_label} (retry {s.retry_count}/{s.retry_max})"
        row: list[str] = [id_label]
        if show_name:
            row.append(_truncate(s.job_name or "", 20))
        row.append(state_label)
        if show_cluster:
            row.append(cluster_label if cluster_label is not None else _scheduler_label(s))
        if show_priority:
            row.append(str(s.priority))
        row.append(str(s.cpus))
        if show_scheduler_tasks:
            row.append(str(s.scheduler_tasks or ""))
        row.extend(
            [
                _format_time(s.submitted_at),
                _truncate(" ".join(s.command), 60),
            ]
        )
        rows.append(tuple(row))
    widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
    lines = ["  ".join(c.ljust(w) for c, w in zip(row, widths, strict=True)) for row in rows]
    if total_cpus is not None:
        lines.append("")
        lines.append(f"active cpus: {used}/{total_cpus}")
    lines.extend(
        _scheduler_occupancy_lines(
            specs,
            additional_unconfirmed_scheduler_running,
        )
    )
    return "\n".join(lines)


def _scheduler_occupancy_lines(
    specs: list[JobSpec],
    additional_unconfirmed: int = 0,
) -> list[str]:
    """Qualify valid and target-attributed malformed scheduler reservations."""
    scheduler_running = [
        spec
        for spec in specs
        if scheduler_running_confirmed(spec) is not None
    ]
    unconfirmed_counts: dict[str, int] = {}
    for spec in scheduler_running:
        if not scheduler_running_confirmed(spec):
            state = effective_queue_state(spec)
            unconfirmed_counts[state] = unconfirmed_counts.get(state, 0) + 1
    if additional_unconfirmed:
        unconfirmed_counts["scheduler_unknown"] = (
            unconfirmed_counts.get("scheduler_unknown", 0)
            + additional_unconfirmed
        )
    total_running = len(scheduler_running) + additional_unconfirmed
    lines: list[str] = []
    if total_running:
        not_confirmed = sum(unconfirmed_counts.values())
        confirmed = total_running - not_confirmed
        detail = (
            "; phases: "
            + ", ".join(
                f"{state}={unconfirmed_counts[state]}"
                for state in sorted(unconfirmed_counts)
            )
            if unconfirmed_counts
            else ""
        )
        lines.append("")
        lines.append(
            f"scheduler occupancy: {confirmed}/{total_running} "
            "scheduler-owned lifecycle-RUNNING job(s) were last confirmed "
            "sched=running; "
            f"{not_confirmed} not confirmed running{detail}."
        )
        if unconfirmed_counts:
            lines.append(
                "Raw vq RUNNING retains scheduler ownership under uncertainty. "
                "Inspect with `vq status HOST JOBID`; when a scheduler job ID "
                "is present, `vq fetch HOST JOBID` attempts a best-effort live "
                "workspace snapshot without proving the job terminal."
            )
    return lines


def _format_time(iso: str) -> str:
    return iso[:19].replace("T", " ")


def _truncate(s: str, n: int) -> str:
    if len(s) <= n:
        return s
    return s[: n - 3] + "..."


def _scheduler_label(spec: JobSpec) -> str:
    """Compact cluster-side phase for the queue table."""
    target = normalized_scheduler_target(spec)
    if target is None:
        return ""
    phase = _scheduler_phase_for_label(spec)
    if spec.is_terminal:
        return (
            f"{target}:"
            f"vq={spec.state.value},sched_last={phase}"
        )
    return (
        f"{target}:"
        f"vq={spec.state.value},sched={phase}"
    )


def _scheduler_phase_for_label(spec: JobSpec) -> str:
    """Return the bounded phase represented in a CLUSTER cell."""
    if spec.state == JobState.RUNNING and not spec.is_terminal:
        return effective_queue_state(spec)
    return normalized_scheduler_phase(spec)


def _collapsed_array_scheduler_label(elements: list[JobSpec]) -> str | None:
    """Render scheduler phase truth for a collapsed array as a whole."""
    targets = {
        (
            scheduler_target_is_safe(spec.scheduler_target),
            normalized_scheduler_target(spec),
        )
        for spec in elements
    }
    if targets == {(False, None)}:
        return None
    if len(targets) != 1:
        return "mixed-targets:sched=unknown"
    _target_is_safe, target = next(iter(targets))
    assert target is not None
    phases: dict[str, int] = {}
    for spec in elements:
        phase = _scheduler_phase_for_label(spec)
        phases[phase] = phases.get(phase, 0) + 1
    if len(phases) == 1:
        labels = {_scheduler_label(spec) for spec in elements}
        if len(labels) == 1:
            return labels.pop()
        # The scheduler phase can be identical while the durable lifecycle
        # shape differs (for example one live poll_failed element plus one
        # terminal element whose last phase was also poll_failed). Never let
        # the representative element's vq=/sched= wording stand in for the
        # whole array.
        phase = next(iter(phases))
        return f"{target}:vq=mixed,sched={phase}"
    detail = ",".join(f"{phase}={phases[phase]}" for phase in sorted(phases))
    return f"{target}:sched=mixed({detail})"
