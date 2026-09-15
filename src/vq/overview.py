"""vq overview — fleet-summary verb (v0.6.21).

One command that aggregates per-host:
  * vq version
  * daemon health (lifecycle contract verdict, including memory
    pressure since v0.6.21)
  * queue counts by state
  * recent terminal counts (within a configurable time window)
  * env versions + drift status from `vq admin status`
  * admin-update marker if present (state machine in-flight)

Answers "what's the state of my fleet?" without juggling four
different verbs.

Output:
  * Text (default) — multi-section per-host blocks, human-readable.
  * --json — single JSON object with a hosts: [...] array.

Single-host mode (`vq overview HOST`) gathers just that host.
Without HOST, walks every host in ``~/.config/vq/config.toml`` —
unreachable hosts surface their error inline rather than aborting
the whole sweep.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from vq import admin, capacity, config, drain, hostmem, lifecycle, paths, throttle, transport
from vq.host import is_local_host
from vq.listing import (
    list_jobs,
    pending_configured_capacity_known,
    pending_configured_capacity_overages,
)
from vq.spec import TERMINAL_STATES, JobSpec, JobState

log = logging.getLogger(__name__)

DEFAULT_RECENT_WINDOW_HOURS = 24
"""Default window for the 'recent terminal counts' section. 24h
covers a typical workday for a fleet doing overnight runs;
operators wanting longer history can pass --since."""


def _format_duration(seconds: int) -> str:
    """v0.6.24: human-format an integer second count for the
    "idle: X" line in text output. Examples:
        12      -> "12s"
        90      -> "1m 30s"
        3725    -> "1h 2m"
        90000   -> "1d 1h"

    Picks the two highest-order non-zero units (d/h/m/s) so the
    line stays compact regardless of magnitude. Clamps negatives
    to "0s" (callers should already clamp, but this is a cheap
    second guard against printing "-3s" if a clock skew slips in).
    """
    if seconds <= 0:
        return "0s"
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes and len(parts) < 2:
        parts.append(f"{minutes}m")
    if secs and len(parts) < 2:
        parts.append(f"{secs}s")
    return " ".join(parts) if parts else "0s"


@dataclass
class AdminMarkerSnapshot:
    """One marker lease plus its host-side diagnosis.

    Marker PID liveness can only be judged on the driver that owns it.  The
    snapshot keeps that verdict beside each lease while an overview is carried
    through JSON and projected onto the scheduler lane the lease can hold.
    """

    marker: admin.AdminUpdateMarker | None
    stale_reason: str | None = None
    status: str | None = None
    summary: str | None = None
    action: str | None = None
    pid_status: str | None = None
    heartbeat_status: str | None = None
    heartbeat_age_seconds: float | None = None


@dataclass
class HostOverview:
    """One host's slice of the fleet overview."""

    host: str
    reachable: bool = True
    error: str | None = None
    # v0.10.0 *Lampson's Hint*: set (to a human description) when the host
    # was skipped because it's administratively down (`vq host down`).
    # Distinct from an unreachable host — we never even probed it.
    admin_down: str | None = None
    # Reachable-and-OK fields (None when unreachable).
    vq_version: str | None = None
    reported_hostname: str | None = None
    """The machine's OWN name for itself, as reported by the host.

    Every host name a user sees elsewhere in vq is the ``[hosts.<key>]``
    TOML table key -- an operator's label, not a fact. That is fine until
    two keys name one machine, or a key names a different machine on a
    different copy of the config, at which point the label is actively
    misleading and nothing on screen can tell you so.

    The 2026-08-05 fleet audit found both at once: a coordinator enrolled
    as both ``coordinator`` and ``localhost``, rendered as two cards, while
    the same key ``localhost`` in the *driver's* config meant a different
    machine entirely. Reporting what the far end calls itself makes that
    visible instead of inferable.

    Set from ``socket.gethostname()`` where it means something -- on the
    host itself -- and carried back through the existing JSON. None on a
    scheduler host (daemonless: it has no vq of its own to ask, and
    borrowing the driver's name is the mistake this field exists to
    expose) and None from a host running a vq too old to report it."""

    is_scheduler_host: bool = False
    """True for a daemonless scheduler host (PBS, SLURM, ...).

    Such a host has no vq of its own to report a version for, which is a
    different statement from "we could not find out". Without this the
    console rendered both as the same bare "unknown", so a cluster whose
    helper record simply had not been written yet was indistinguishable
    from a host that failed to answer."""

    self_reported: bool = False
    """True when this overview was gathered *in this process* rather than
    read from the host.

    Distinguishes "coordinator probed coordinator over SSH" from "the console
    read its own constants and labelled them coordinator". Those look
    identical on screen and are not the same claim: the second one made a
    console report its own stale version as ten different hosts' versions."""
    helper_source_sha: str | None = None
    """Verified SOURCE-SHA of a scheduler host's own vq helper.

    Scheduler hosts are daemonless, so they have no vq of their own to
    report a version for -- but they DO run a helper vq that polls,
    reconciles and dispatches, and whose skew from the driver has already
    caused an incident. Reporting the helper's recorded identity here, and
    leaving ``vq_version`` None, makes that skew visible instead of hiding
    it behind the driver's version number. None on a non-scheduler host,
    or before the helper has a canonical record."""
    daemon_health: lifecycle.ContractVerdict | None = None
    queue_counts: dict[str, int] = field(default_factory=dict)
    # Scheduler-backed hosts keep the vq lifecycle and scheduler lifecycle
    # separate. ``queue_counts`` is the effective user-facing load for overview
    # and placement; this map preserves the raw PBS/qstat phases.
    scheduler_queue_counts: dict[str, int] = field(default_factory=dict)
    recent_terminal_counts: dict[str, int] = field(default_factory=dict)
    envs: list[admin.EnvStatus] = field(default_factory=list)
    admin_marker: admin.AdminUpdateMarker | None = None
    admin_markers: list[AdminMarkerSnapshot] = field(default_factory=list)
    """Every marker lease and its driver-side diagnosis.

    ``admin_marker`` remains the first-lease compatibility view. Scheduler
    projection uses this list so a foreign first lease cannot hide a later
    applicable or unreadable/global hold.
    """
    # v0.11.0: set (to a human reason) when ``admin_marker`` is present
    # but STALE because its writing `vq admin update` process is gone.
    # Ordinary stale markers are auto-reaped and stop gating; durable
    # transaction/pause receipts remain scoped until explicit recovery.
    # Surfaced loudly so neither case reads as plain `up`/`OK`. None when
    # there's no marker, or the marker looks like a live in-flight update.
    # Computed host-side in gather_overview_local (the pid liveness probe
    # must run where the pid lives) and carried through the JSON.
    admin_marker_stale_reason: str | None = None
    admin_marker_status: str | None = None
    admin_marker_summary: str | None = None
    admin_marker_action: str | None = None
    admin_marker_pid_status: str | None = None
    admin_marker_heartbeat_status: str | None = None
    admin_marker_heartbeat_age_seconds: float | None = None
    # v0.6.23: dispatch-gating state. drain_state is present when
    # `vq drain` has been set and the drain.json hasn't expired;
    # throttle_state is present when `vq throttle --persist` has
    # been set and the throttle.json hasn't expired.
    drain_state: drain.DrainState | None = None
    throttle_state: throttle.ThrottleState | None = None
    # v0.6.24: idle time. Seconds since the most-recently-finished
    # terminal job, OR None if the host is currently busy (any confirmed
    # running jobs), has an unconfirmed scheduler reservation, or has no
    # terminal history. The semantic is
    # "how long has this queue been quiet" — useful for operator
    # decisions like "is it safe to update vibeqc-release on this
    # host now?". A busy host returns None (not 0); the consumer
    # distinguishes busy vs idle via the running count.
    idle_seconds: int | None = None
    # v0.7.18 *Kay's Object*: load fields for the --recommend flag.
    # ``running_cpus`` sums confirmed RUNNING work; ``pending_cpus`` sums
    # pending work plus scheduler reservations whose execution phase is not
    # confirmed. Together they give a conservative workload tally the
    # recommendation logic can rank on. This is not a true "remaining
    # capacity" measure (we don't know the daemon's ``cpus_total`` from the
    # overview path), but it is a useful proxy for "which host's queue is
    # shortest?".
    running_cpus: int = 0
    pending_cpus: int = 0
    unconfirmed_scheduler_jobs: int = 0
    """Scheduler reservations whose last phase is neither pending nor running.

    This is a subset of ``queue_counts['pending']`` rather than an additive
    lifecycle count. The matching CPUs remain inside ``pending_cpus`` so old
    and new placement consumers reserve the same total capacity.
    """
    unconfirmed_scheduler_cpus: int = 0
    """CPU subset belonging to ``unconfirmed_scheduler_jobs``.

    Informational only: these CPUs are already included in ``pending_cpus``.
    """
    over_capacity_pending_jobs: int | None = None
    """Pending local jobs that exceed the daemon's configured base caps.

    Kept separate from ``queue_counts['pending']`` for compatibility: these
    jobs remain durably PENDING, but cannot dispatch until the configuration
    changes or the jobs are resubmitted. None means the queue or configured
    capacity snapshot was unavailable, not that every pending job fits.
    """
    # v0.11.0: the daemon's configured budget, advertised via the
    # daemon_capacity.json state file (capacity.py) so placement can
    # compute *remaining* capacity (free_cpus = max_cpus - running_cpus),
    # not just the queue-length proxy above. None when unknown (daemon
    # down, or a pre-v0.11.0 host that never wrote the file).
    max_cpus: int | None = None
    quota_max_concurrent_cpus: int | None = None
    """Per-submitter concurrent-CPU quota (`[quotas]
    default_max_concurrent_cpus`). Reported because it, not core count and
    not the daemon's own `max_cpus`, is often what actually stops a job
    dispatching -- and the three can disagree. workstation on 2026-07-27: 32
    physical cores, daemon max_cpus 32, quota 16, and a pending 24-cpu job
    with an idle queue and nothing in overview explaining why. None =
    unlimited."""
    max_jobs: int | None = None
    max_mem_mb: int | None = None
    max_scheduler_jobs: int | None = None
    # v0.11.0: live free RAM, sampled fresh each overview gather (hostmem.py).
    # QC jobs are memory-bound, so this — not CPU load — is the signal
    # placement matches a job's estimated peak memory against (mem_total_mb
    # is for display). Distinct from ``max_mem_mb`` above (the daemon's
    # configured budget): these are what the OS reports *right now*. None off
    # Linux (macOS has no /proc/meminfo) or on a host that never sampled →
    # placement falls back to the core-count proxy.
    mem_total_mb: int | None = None
    mem_available_mb: int | None = None


def _snapshot_admin_marker(
    marker: admin.AdminUpdateMarker | None,
) -> AdminMarkerSnapshot:
    diag = admin.diagnose_admin_update_marker(marker)
    return AdminMarkerSnapshot(
        marker=marker,
        stale_reason=admin.admin_update_marker_stale_reason(marker),
        status=diag.marker_status,
        summary=diag.summary,
        action=diag.action,
        pid_status=diag.pid_status,
        heartbeat_status=diag.heartbeat_status,
        heartbeat_age_seconds=diag.heartbeat_age_seconds,
    )


def _apply_admin_marker_snapshot(
    overview: HostOverview,
    snapshot: AdminMarkerSnapshot,
) -> None:
    overview.admin_marker = snapshot.marker
    overview.admin_marker_stale_reason = snapshot.stale_reason
    overview.admin_marker_status = snapshot.status
    overview.admin_marker_summary = snapshot.summary
    overview.admin_marker_action = snapshot.action
    overview.admin_marker_pid_status = snapshot.pid_status
    overview.admin_marker_heartbeat_status = snapshot.heartbeat_status
    overview.admin_marker_heartbeat_age_seconds = snapshot.heartbeat_age_seconds


def _admin_marker_snapshot_from_compat(
    overview: HostOverview,
) -> AdminMarkerSnapshot | None:
    values = (
        overview.admin_marker,
        overview.admin_marker_stale_reason,
        overview.admin_marker_status,
        overview.admin_marker_summary,
        overview.admin_marker_action,
        overview.admin_marker_pid_status,
        overview.admin_marker_heartbeat_status,
        overview.admin_marker_heartbeat_age_seconds,
    )
    if not any(value is not None for value in values):
        return None
    return AdminMarkerSnapshot(
        marker=overview.admin_marker,
        stale_reason=overview.admin_marker_stale_reason,
        status=overview.admin_marker_status,
        summary=overview.admin_marker_summary,
        action=overview.admin_marker_action,
        pid_status=overview.admin_marker_pid_status,
        heartbeat_status=overview.admin_marker_heartbeat_status,
        heartbeat_age_seconds=overview.admin_marker_heartbeat_age_seconds,
    )


def _admin_marker_snapshots(overview: HostOverview) -> list[AdminMarkerSnapshot]:
    if overview.admin_markers:
        return overview.admin_markers
    compat = _admin_marker_snapshot_from_compat(overview)
    return [compat] if compat is not None else []


def _select_admin_marker_snapshot_for_target(
    overview: HostOverview,
    target: str,
) -> AdminMarkerSnapshot | None:
    """Choose a truthful marker view for one scheduler dispatch lane."""
    global_effective: list[AdminMarkerSnapshot] = []
    scoped_effective: list[AdminMarkerSnapshot] = []
    informational: list[AdminMarkerSnapshot] = []
    for snapshot in _admin_marker_snapshots(overview):
        marker = snapshot.marker
        try:
            scope = admin.admin_update_marker_scope(marker)
        except Exception:  # pragma: no cover - malformed remote marker
            scope = None
        if scope is not None and target not in scope:
            continue
        stale_ordinary = (
            marker is not None
            and snapshot.stale_reason is not None
            and marker.managed_transaction is None
            and not marker.owns_pause_scope
        )
        candidate = (
            snapshot
            if scope is not None or stale_ordinary
            else _snapshot_admin_marker(None)
        )
        if stale_ordinary:
            informational.append(candidate)
        elif scope is None:
            global_effective.append(candidate)
        else:
            scoped_effective.append(candidate)
    if global_effective:
        return global_effective[0]
    if scoped_effective:
        return scoped_effective[0]
    return informational[0] if informational else None


def _collect_env_statuses(cfg: config.Config) -> list[admin.EnvStatus]:
    """Return one ``EnvStatus`` per ``kind=venv`` program, sorted by
    name for stable output. Mirrors ``vq admin status`` but returns
    structured data instead of formatted text."""
    statuses: list[admin.EnvStatus] = []
    for name, prog in sorted(cfg.programs.items()):
        if isinstance(prog, config.VenvProgram):
            try:
                statuses.append(admin.query_env_status(name, prog))
            except Exception as e:  # pragma: no cover — defensive
                log.warning("overview: env-status for %s failed: %s", name, e)
    return statuses


def _count_specs(
    specs: list[JobSpec],
    *,
    recent_window: timedelta,
    now: datetime | None = None,
    scheduler_effective: bool = False,
) -> tuple[dict[str, int], dict[str, int], datetime | None, int, int]:
    """Partition the spec list into queue_counts (by current state),
    recent_terminal_counts (terminal specs whose finished_at is
    within ``recent_window`` of ``now``), last_terminal_at
    (the most-recently-finished terminal spec across ALL history,
    not just the window — used for idle-time computation), and
    v0.7.18 ``running_cpus`` / ``pending_cpus`` totals.

    Queue counts include every state. Recent-terminal counts only
    include TERMINAL_STATES specs with a parseable finished_at
    inside the window. Specs without finished_at (e.g. ABORTED_BY_QUEUE
    where the daemon never observed the exit) are not counted in
    'recent' since we can't bound them to the window.

    last_terminal_at intentionally walks the FULL history (not just
    the window) so that idle time is well-defined on a quiet host
    with no recent activity. Returns None if no terminal spec has a
    parseable finished_at.

    v0.7.18: ``running_cpus`` sums ``spec.cpus`` across effective RUNNING
    specs; ``pending_cpus`` sums across effective PENDING specs. Used by the
    ``vq overview --recommend`` ranking. For scheduler projections, every
    phase other than exact ``running`` is capacity-conservative: known
    queued/held/unpolled phases and unconfirmed failure/fence phases all stay
    in the pending CPU bucket.

    ``scheduler_effective`` makes scheduler-target specs count by the
    last observed scheduler phase while preserving the raw JobSpec state
    elsewhere. A driver-owned spec is ``running`` as soon as qsub succeeds,
    but a PBS ``queued`` or ``held`` job should read as pending in overview
    and placement until the scheduler reports actual execution.
    """
    if now is None:
        now = datetime.now(UTC)
    cutoff = now - recent_window
    queue_counts: dict[str, int] = {}
    recent_terminal_counts: dict[str, int] = {}
    last_terminal_at: datetime | None = None
    running_cpus = 0
    pending_cpus = 0
    for spec in specs:
        queue_state = (
            _scheduler_effective_queue_state(spec)
            if scheduler_effective
            else spec.state.value
        )
        queue_counts[queue_state] = queue_counts.get(queue_state, 0) + 1
        if queue_state == JobState.RUNNING.value:
            running_cpus += spec.cpus
        elif queue_state == JobState.PENDING.value:
            pending_cpus += spec.cpus
        if spec.state in TERMINAL_STATES and spec.finished_at:
            try:
                finished = datetime.fromisoformat(spec.finished_at)
            except ValueError:
                continue
            if finished >= cutoff:
                recent_terminal_counts[spec.state.value] = (
                    recent_terminal_counts.get(spec.state.value, 0) + 1
                )
            # v0.6.24: track the absolute most-recent finish across
            # all terminal history (NOT windowed) — feeds idle_seconds.
            if last_terminal_at is None or finished > last_terminal_at:
                last_terminal_at = finished
    return (
        queue_counts,
        recent_terminal_counts,
        last_terminal_at,
        running_cpus,
        pending_cpus,
    )


_SCHEDULER_PENDING_PHASES = {"queued", "held", "unpolled"}


def _is_unconfirmed_scheduler_reservation(spec: JobSpec) -> bool:
    if (
        not spec.scheduler_target
        or spec.state != JobState.RUNNING
        or spec.is_terminal
    ):
        return False
    scheduler_state = spec.scheduler_state or "unpolled"
    return (
        scheduler_state != JobState.RUNNING.value
        and scheduler_state not in _SCHEDULER_PENDING_PHASES
    )


def _scheduler_effective_queue_state(spec: JobSpec) -> str:
    if (
        not spec.scheduler_target
        or spec.state != JobState.RUNNING
        or spec.is_terminal
    ):
        return str(spec.state.value)
    scheduler_state = spec.scheduler_state or "unpolled"
    if scheduler_state == JobState.RUNNING.value:
        return "running"
    # Only an exact last scheduler observation of ``running`` confirms
    # execution. Queued/held/unpolled phases are ordinary pending load;
    # failures, fences, and unknown future phases are unconfirmed reservations.
    # All reserve capacity in the existing pending bucket so mixed-version
    # placement consumers remain fail-closed.
    return "pending"


def _scheduler_phase_counts(specs: list[JobSpec]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for spec in specs:
        if spec.scheduler_target and spec.state == JobState.RUNNING:
            phase = spec.scheduler_state or "unpolled"
            counts[phase] = counts.get(phase, 0) + 1
    return counts


def gather_overview_local(
    host: str,
    cfg: config.Config,
    *,
    recent_window: timedelta = timedelta(hours=DEFAULT_RECENT_WINDOW_HOURS),
    now: datetime | None = None,
    multi_user: bool = False,
    specs: list[JobSpec] | None = None,
) -> HostOverview:
    """Gather the local-host overview by calling internal helpers
    directly (no SSH). Used by the daemon-local code path and by
    the remote forwarder's "remote echoes overview --json" invocation.

    ``multi_user`` (v0.6.41): count jobs across the per-user state
    dirs under ``/var/lib/vq/users/<uid>/`` instead of the
    single-user queue dir — otherwise ``vq overview`` / ``vq
    summary`` report 0 jobs on a multi-user host. ``specs`` lets a fleet
    sweep reuse one driver observation for its overview and scheduler lanes."""
    overview = HostOverview(host=host)
    # Gathered in-process: the version below is this interpreter's, and
    # the hostname below is this machine's. Both are only facts about the
    # host being described because we are *on* it -- which is exactly
    # what `self_reported` records, and exactly what stopped being true
    # when a console gathered "localhost" and called it a fleet host.
    overview.self_reported = True
    try:
        from vq import __version__ as _vq_version

        overview.vq_version = _vq_version
    except Exception:  # pragma: no cover — defensive
        overview.vq_version = None

    try:
        import socket  # noqa: PLC0415 — stdlib, cheap

        overview.reported_hostname = socket.gethostname().split(".", 1)[0]
    except Exception:  # pragma: no cover — defensive
        overview.reported_hostname = None

    try:
        overview.daemon_health = lifecycle.verify_user_systemd_contract()
    except Exception as e:  # pragma: no cover — defensive
        log.warning("overview: daemon health probe failed: %s", e)
        overview.daemon_health = None

    queue_snapshot_available = False
    try:
        if specs is None:
            specs = list_jobs(host, queue_dir=paths.queue_dir(), multi_user=multi_user)
        queue_snapshot_available = True
        (
            overview.queue_counts,
            overview.recent_terminal_counts,
            last_terminal_at,
            overview.running_cpus,
            overview.pending_cpus,
        ) = _count_specs(
            specs,
            recent_window=recent_window,
            now=now,
        )
        # v0.6.24: idle_seconds = time since last finish, BUT only
        # if the host is currently quiet (no running jobs). A host
        # with running jobs is busy regardless of when the most-
        # recent prior job finished; we model that as None (not 0)
        # so consumers can distinguish "busy" from "just finished".
        running_now = overview.queue_counts.get("running", 0)
        if running_now == 0 and last_terminal_at is not None:
            now_dt = now or datetime.now(UTC)
            delta = now_dt - last_terminal_at
            # Clamp negative deltas (clock skew) to 0; defensive but
            # cheap. delta.total_seconds() can be very large for
            # long-quiet hosts — int() truncation is fine.
            overview.idle_seconds = max(0, int(delta.total_seconds()))
    except Exception as e:  # pragma: no cover — defensive
        log.warning("overview: queue listing failed: %s", e)

    try:
        overview.envs = _collect_env_statuses(cfg)
    except Exception as e:
        log.warning("overview: env-status collection failed: %s", e)

    try:
        if admin.admin_update_marker_exists():
            overview.admin_markers = [
                _snapshot_admin_marker(marker)
                for _, marker in admin._admin_update_marker_entries()
            ]
            if overview.admin_markers:
                _apply_admin_marker_snapshot(overview, overview.admin_markers[0])
    except Exception as e:  # pragma: no cover — defensive
        log.warning("overview: marker read failed: %s", e)

    # v0.6.23: dispatch-gating state — drain + persistent throttle.
    # Both readers silently expire stale state, so a TTL'd drain
    # ("kids gaming for 2h") past its window reads as None here.
    try:
        overview.drain_state = drain.read_effective_drain_state()
    except Exception as e:  # pragma: no cover — defensive
        log.warning("overview: drain read failed: %s", e)
    try:
        overview.throttle_state = throttle.read_throttle_state()
    except Exception as e:  # pragma: no cover — defensive
        log.warning("overview: throttle read failed: %s", e)

    # The daemon's advertised budget: live RPC first, mode-aware startup file
    # second. Absent means unknown capacity, so placement falls back to the
    # queue-length proxy. Never raises.
    try:
        cap = capacity.read_daemon_capacity(multi_user=multi_user)
        if cap is not None:
            overview.max_cpus = cap.max_cpus
            overview.max_jobs = cap.max_jobs
            overview.max_mem_mb = cap.max_mem_mb
            overview.max_scheduler_jobs = cap.max_scheduler_jobs
            if queue_snapshot_available and specs is not None:
                over_capacity = 0
                classification_unknown = False
                for spec in specs:
                    overages = pending_configured_capacity_overages(spec, cap)
                    if overages:
                        over_capacity += 1
                    elif (
                        spec.state == JobState.PENDING
                        and spec.scheduler_target is None
                        and not pending_configured_capacity_known(spec, cap)
                    ):
                        classification_unknown = True
                overview.over_capacity_pending_jobs = (
                    None if classification_unknown else over_capacity
                )
    except Exception as e:  # pragma: no cover — defensive
        log.warning("overview: capacity read failed: %s", e)

    # The per-submitter quota gates dispatch independently of the daemon
    # budget above, and on a multi-user host it is usually the tighter of
    # the two. Reading it here keeps it on the same host-local path as the
    # capacity read, so the remote forwarder carries it for free.
    try:
        overview.quota_max_concurrent_cpus = (
            config.load_config().quotas.default_max_concurrent_cpus
        )
    except Exception as e:  # pragma: no cover — defensive
        log.warning("overview: quota read failed: %s", e)

    # v0.11.0: live free-RAM sample (hostmem.py). Sampled here — this runs ON
    # the host (locally, or via the remote forwarder's `vq overview localhost
    # --json`) — so it reads *this box's* real free RAM, not the laptop's.
    # Fresh every gather; never cached. Never raises.
    try:
        hm = hostmem.sample_host_mem()
        overview.mem_total_mb = hm.mem_total_mb
        overview.mem_available_mb = hm.mem_available_mb
    except Exception as e:  # pragma: no cover — defensive
        log.warning("overview: host-mem sample failed: %s", e)

    return overview


def gather_overview_remote(
    host: str,
    host_cfg: config.HostConfig,
    *,
    recent_window: timedelta = timedelta(hours=DEFAULT_RECENT_WINDOW_HOURS),
) -> HostOverview:
    """Gather an overview from a remote host by running
    ``<remote_vq> overview localhost --json`` over SSH and decoding
    the response. Single SSH call; the remote-side overview gather
    does all the heavy lifting and emits one JSON blob.

    On any transport / parse failure, returns a HostOverview with
    ``reachable=False`` + the error string — never raises. The
    caller (CLI fleet aggregator) renders unreachable hosts inline."""
    hours = int(recent_window.total_seconds() // 3600)
    argv = ["overview", "localhost", "--json", "--since-hours", str(hours)]
    try:
        proc = transport.run_remote_vq(host_cfg, *argv)
    except transport.RemoteError as e:
        return HostOverview(host=host, reachable=False, error=str(e))
    try:
        payload = json.loads(proc.stdout)
        # The remote CLI always wraps single-host output in the
        # fleet format `{"hosts": [...]}` for schema consistency.
        # Unwrap to the inner dict.
        if "hosts" in payload and isinstance(payload["hosts"], list):
            if not payload["hosts"]:
                return HostOverview(
                    host=host,
                    reachable=False,
                    error="remote overview returned empty hosts array",
                )
            inner = payload["hosts"][0]
        else:
            inner = payload  # accept bare host dict for forward-compat
        return _overview_from_json(host, inner)
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        return HostOverview(
            host=host,
            reachable=False,
            error=f"failed to parse remote overview JSON: {e}",
        )



def _helper_source_sha(host: str) -> str | None:
    """Short verified SOURCE-SHA of ``host``'s scheduler vq helper, if recorded.

    Read from the canonical helper record rather than compared against the
    driver -- the point is precisely to show when the two disagree. Never
    raises: an absent record means "not recorded yet", which is a legitimate
    state for a helper deployed before canonical bookkeeping existed.
    """
    try:
        records = admin.load_scheduler_runtime_status()
    except Exception as e:  # pragma: no cover - defensive
        log.warning("overview: helper record read failed: %s", e)
        return None
    record = records.get(f"{host}:{admin.SCHEDULER_HELPER_RECORD_PROGRAM}")
    sha = getattr(record, "actual_sha", None) if record is not None else None
    if isinstance(sha, str) and len(sha) == 40:
        return sha[:12]
    return None


def gather_scheduler_overview(
    host: str,
    host_cfg: config.HostConfig,
    cfg: config.Config,
    *,
    recent_window: timedelta = timedelta(hours=DEFAULT_RECENT_WINDOW_HOURS),
    now: datetime | None = None,
    multi_user: bool = False,
) -> HostOverview:
    """Represent a daemonless scheduler host through its driver daemon.

    Scheduler hosts intentionally do not answer ``vq overview`` themselves:
    the fixed driver daemon owns their specs, tagged with
    ``scheduler_target=<host>``. For placement and fleet routing, synthesize a
    host overview from the driver's health/gating state plus only the specs for
    this scheduler target.
    """
    if host_cfg.scheduler == "local":
        return HostOverview(
            host=host,
            reachable=False,
            error="not a scheduler host",
        )
    driver = host_cfg.scheduler_driver
    if driver is None:
        return HostOverview(
            host=host,
            reachable=False,
            error="scheduler host has no scheduler_driver configured",
        )

    try:
        if is_local_host(driver):
            driver_overview = gather_overview_local(
                driver,
                cfg,
                recent_window=recent_window,
                now=now,
                multi_user=multi_user,
            )
            specs = [
                s
                for s in list_jobs(driver, multi_user=multi_user)
                if not s.is_archived
            ]
        else:
            driver_cfg = cfg.host(driver)
            driver_overview = gather_overview_remote(
                driver,
                driver_cfg,
                recent_window=recent_window,
            )
            if not driver_overview.reachable:
                return HostOverview(
                    host=host,
                    reachable=False,
                    error=f"scheduler driver {driver!r}: {driver_overview.error}",
                )
            proc = transport.run_remote_vq(
                driver_cfg,
                "queue",
                "localhost",
                "--json",
            )
            payload = json.loads(proc.stdout)
            specs = [JobSpec.model_validate(item) for item in payload]
    except config.ConfigError as exc:
        return HostOverview(
            host=host,
            reachable=False,
            error=f"scheduler driver {driver!r}: {exc}",
        )
    except transport.RemoteError as exc:
        return HostOverview(
            host=host,
            reachable=False,
            error=f"scheduler driver {driver!r}: {exc}",
        )
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        return HostOverview(
            host=host,
            reachable=False,
            error=f"scheduler driver {driver!r} queue JSON failed: {exc}",
        )

    return project_scheduler_overview(
        host, driver_overview, specs, recent_window=recent_window, now=now,
    )


def project_scheduler_overview(
    host: str,
    driver_overview: HostOverview,
    specs: list[JobSpec],
    *,
    recent_window: timedelta = timedelta(hours=DEFAULT_RECENT_WINDOW_HOURS),
    now: datetime | None = None,
) -> HostOverview:
    """Project one scheduler lane from an already observed driver queue."""
    if not driver_overview.reachable:
        return HostOverview(host=host, reachable=False, error=driver_overview.error,
                            is_scheduler_host=True)
    target_specs = [s for s in specs if s.scheduler_target == host]
    (
        queue_counts,
        recent_terminal_counts,
        last_terminal_at,
        running_cpus,
        pending_cpus,
    ) = _count_specs(
        target_specs,
        recent_window=recent_window,
        now=now,
        scheduler_effective=True,
    )

    overview = HostOverview(host=host)
    # NOT driver_overview.vq_version. A scheduler host is daemonless, so the
    # driver's number is not this host's -- and pbs-cluster/slurm-cluster each run their
    # own helper vq, whose skew from the driver was a whole incident. A
    # confident wrong version hid exactly the thing an operator needed.
    overview.is_scheduler_host = True
    overview.helper_source_sha = _helper_source_sha(host)
    overview.daemon_health = driver_overview.daemon_health
    overview.drain_state = driver_overview.drain_state
    overview.throttle_state = driver_overview.throttle_state
    marker_snapshot = _select_admin_marker_snapshot_for_target(
        driver_overview,
        host,
    )
    if marker_snapshot is not None:
        # A scheduler card shows only holds that can gate that lane. Unknown
        # and unreadable markers remain global/fail-closed; a driver's plain
        # local-runtime receipt must not masquerade as a cluster hold.
        overview.admin_markers = [marker_snapshot]
        _apply_admin_marker_snapshot(overview, marker_snapshot)
    overview.queue_counts = queue_counts
    overview.scheduler_queue_counts = _scheduler_phase_counts(target_specs)
    overview.recent_terminal_counts = recent_terminal_counts
    overview.running_cpus = running_cpus
    overview.pending_cpus = pending_cpus
    unconfirmed_specs = [
        spec for spec in target_specs if _is_unconfirmed_scheduler_reservation(spec)
    ]
    overview.unconfirmed_scheduler_jobs = len(unconfirmed_specs)
    overview.unconfirmed_scheduler_cpus = sum(
        spec.cpus for spec in unconfirmed_specs
    )
    if (
        overview.queue_counts.get("running", 0) == 0
        and overview.unconfirmed_scheduler_jobs == 0
        and last_terminal_at is not None
    ):
        now_dt = now or datetime.now(UTC)
        overview.idle_seconds = max(0, int((now_dt - last_terminal_at).total_seconds()))
    return overview


def _remote_admin_marker_shape_is_safe(marker: dict[str, Any]) -> bool:
    """Validate fields that overview renders or uses for recovery advice."""
    envs = marker.get("envs")
    marker_host = marker.get("host")
    paused_jobids = marker.get("paused_jobids", [])
    managed_transaction = marker.get("managed_transaction")
    pause_token = marker.get("pause_token")
    return (
        isinstance(envs, list)
        and all(isinstance(env, str) for env in envs)
        and isinstance(marker_host, str)
        and bool(marker_host)
        and "\r" not in marker_host
        and "\n" not in marker_host
        and isinstance(marker.get("started_at"), str)
        and isinstance(marker.get("pid"), int)
        and not isinstance(marker.get("pid"), bool)
        and isinstance(marker.get("vq_version"), str)
        and (
            managed_transaction is None
            or isinstance(managed_transaction, dict)
        )
        and (pause_token is None or isinstance(pause_token, str))
        and isinstance(paused_jobids, list)
        and all(isinstance(jobid, str) for jobid in paused_jobids)
        and (
            "surgical_pause" not in marker
            or type(marker["surgical_pause"]) is bool
        )
    )


def _admin_marker_from_payload(value: object) -> admin.AdminUpdateMarker | None:
    if not isinstance(value, dict) or not _remote_admin_marker_shape_is_safe(value):
        return None
    from dataclasses import fields as _dc_fields

    kept = {
        item.name: value.get(item.name)
        for item in _dc_fields(admin.AdminUpdateMarker)
        if item.name in value
    }
    try:
        return admin.AdminUpdateMarker(**kept)
    except (TypeError, ValueError):
        return None


def _admin_marker_snapshot_from_payload(
    value: object,
) -> AdminMarkerSnapshot | None:
    if not isinstance(value, dict):
        return None
    raw_marker = value.get("marker")
    marker = _admin_marker_from_payload(raw_marker)
    if raw_marker is not None and marker is None:
        # Do not trust diagnosis/action text paired with an unsafe marker.
        return _snapshot_admin_marker(None)
    snapshot = AdminMarkerSnapshot(marker=marker)
    for field_name in (
        "stale_reason",
        "status",
        "summary",
        "action",
        "pid_status",
        "heartbeat_status",
    ):
        field_value = value.get(field_name)
        setattr(
            snapshot,
            field_name,
            field_value
            if isinstance(field_value, str) and field_value.strip()
            else None,
        )
    age = value.get("heartbeat_age_seconds")
    if isinstance(age, (int, float)) and not isinstance(age, bool):
        snapshot.heartbeat_age_seconds = float(age)
    return snapshot


def _overview_from_json(host: str, payload: dict[str, Any]) -> HostOverview:
    """Rebuild a HostOverview from its JSON serialization (used by
    the remote-forwarding path). The schema mirrors
    :func:`format_overview_json`'s output."""
    overview = HostOverview(host=host)
    overview.vq_version = payload.get("vq_version")
    # The one line that turns a remote gather into verified identity. The
    # payload was produced by `vq overview localhost --json` running ON
    # the far host, so its hostname is that machine's own answer. This
    # function used to drop it, which is why nothing in vq could tell an
    # operator that two config keys pointed at one machine.
    reported = payload.get("reported_hostname")
    overview.reported_hostname = reported if isinstance(reported, str) else None
    overview.helper_source_sha = payload.get("helper_source_sha")
    overview.queue_counts = payload.get("queue_counts", {}) or {}
    overview.scheduler_queue_counts = payload.get("scheduler_queue_counts", {}) or {}
    overview.recent_terminal_counts = payload.get("recent_terminal_counts", {}) or {}

    health = payload.get("daemon_health")
    if health is not None:
        overview.daemon_health = lifecycle.ContractVerdict(
            ok=health.get("ok", False),
            manager_pid=health.get("manager_pid"),
            loginctl_state=health.get("loginctl_state"),
            loginctl_runtime_path=health.get("loginctl_runtime_path"),
            systemctl_user_reachable=health.get("systemctl_user_reachable", False),
            vq_daemon_state=health.get("vq_daemon_state"),
            vq_daemon_main_pid=health.get("vq_daemon_main_pid"),
            daemon_pidfile_pid=health.get("daemon_pidfile_pid"),
            daemon_process_alive=health.get("daemon_process_alive"),
            memory_pressure_pct=health.get("memory_pressure_pct"),
            findings=health.get("findings", []),
        )

    envs = payload.get("envs", []) or []
    overview.envs = []
    for e in envs:
        # Robust to schema drift — drop unknown fields, default
        # required ones. AdminUpdateRecord nested rebuild is
        # intentionally not done here; we render summary text only.
        try:
            overview.envs.append(
                admin.EnvStatus(
                    name=e["name"],
                    git_dir=e.get("git_dir", ""),
                    branch=e.get("branch"),
                    current_sha=e.get("current_sha"),
                    current_describe=e.get("current_describe"),
                    current_version=e.get("current_version"),
                    is_dirty=e.get("is_dirty"),
                    last_record=None,  # render-only path; drop nested
                    error=e.get("error"),
                )
            )
        except (KeyError, TypeError):
            continue

    overview.admin_marker = _admin_marker_from_payload(payload.get("admin_marker"))

    # v0.11.0: the host-side stale verdict rides along in the JSON (the
    # liveness probe ran on the owning host; the aggregator can't re-probe
    # a remote pid). Plain string field, robust to absence (older remote).
    marker_text_fields = (
        "admin_marker_stale_reason",
        "admin_marker_status",
        "admin_marker_summary",
        "admin_marker_action",
        "admin_marker_pid_status",
        "admin_marker_heartbeat_status",
    )
    for field_name in marker_text_fields:
        value = payload.get(field_name)
        setattr(
            overview,
            field_name,
            value if isinstance(value, str) and value.strip() else None,
        )
    age = payload.get("admin_marker_heartbeat_age_seconds")
    if isinstance(age, (int, float)):
        overview.admin_marker_heartbeat_age_seconds = float(age)

    compat = _admin_marker_snapshot_from_compat(overview)
    if "admin_markers" not in payload:
        if compat is not None:
            overview.admin_markers = [compat]
    else:
        marker_snapshots = payload.get("admin_markers")
        if not isinstance(marker_snapshots, list) or (
            not marker_snapshots and compat is not None
        ):
            overview.admin_markers = [_snapshot_admin_marker(None)]
        else:
            for item in marker_snapshots:
                snapshot = _admin_marker_snapshot_from_payload(item)
                overview.admin_markers.append(
                    snapshot if snapshot is not None else _snapshot_admin_marker(None)
                )

    # v0.6.23: drain + throttle state from remote payload. Both are
    # pydantic BaseModels with model_validate semantics — robust to
    # extra fields if a future remote adds them.
    drain_payload = payload.get("drain_state")
    if drain_payload is not None:
        try:
            overview.drain_state = drain.DrainState.model_validate(drain_payload)
        except Exception:
            overview.drain_state = None
    throttle_payload = payload.get("throttle_state")
    if throttle_payload is not None:
        try:
            overview.throttle_state = throttle.ThrottleState.model_validate(throttle_payload)
        except Exception:
            overview.throttle_state = None

    # v0.6.24: idle_seconds from remote payload. May be absent on
    # pre-v0.6.24 hosts (key missing) — treat as None (no idle data).
    idle = payload.get("idle_seconds")
    if isinstance(idle, int) and idle >= 0:
        overview.idle_seconds = idle

    # v0.7.18: running_cpus / pending_cpus from remote payload. May
    # be absent on pre-v0.7.18 hosts (key missing) — treat as 0 so
    # the recommend ranking still has something to compare on (a
    # pre-v0.7.18 host looks "empty" for ranking purposes, which is
    # the conservative direction — operator may submit to it
    # anyway).
    running_cpus = payload.get("running_cpus")
    if isinstance(running_cpus, int) and running_cpus >= 0:
        overview.running_cpus = running_cpus
    pending_cpus = payload.get("pending_cpus")
    if isinstance(pending_cpus, int) and pending_cpus >= 0:
        overview.pending_cpus = pending_cpus
    unconfirmed_scheduler_jobs = payload.get("unconfirmed_scheduler_jobs")
    if (
        isinstance(unconfirmed_scheduler_jobs, int)
        and not isinstance(unconfirmed_scheduler_jobs, bool)
        and unconfirmed_scheduler_jobs >= 0
    ):
        overview.unconfirmed_scheduler_jobs = unconfirmed_scheduler_jobs
    unconfirmed_scheduler_cpus = payload.get("unconfirmed_scheduler_cpus")
    if (
        isinstance(unconfirmed_scheduler_cpus, int)
        and not isinstance(unconfirmed_scheduler_cpus, bool)
        and unconfirmed_scheduler_cpus >= 0
    ):
        overview.unconfirmed_scheduler_cpus = unconfirmed_scheduler_cpus
    # A mixed-version or stale remote payload must not claim the host is idle
    # while also reporting scheduler work whose execution outcome is unknown.
    if overview.unconfirmed_scheduler_jobs:
        overview.idle_seconds = None
    over_capacity_pending_jobs = payload.get("over_capacity_pending_jobs")
    if (
        isinstance(over_capacity_pending_jobs, int)
        and over_capacity_pending_jobs >= 0
    ):
        overview.over_capacity_pending_jobs = over_capacity_pending_jobs

    # v0.11.0: the daemon's advertised budget from the remote payload.
    # Absent on pre-v0.11.0 hosts (key missing) → stays None (capacity
    # unknown; placement falls back to the queue-length proxy).
    max_cpus = payload.get("max_cpus")
    if isinstance(max_cpus, int) and max_cpus >= 1:
        overview.max_cpus = max_cpus
    quota_cpus = payload.get("quota_max_concurrent_cpus")
    if isinstance(quota_cpus, int) and quota_cpus >= 1:
        overview.quota_max_concurrent_cpus = quota_cpus
    max_jobs = payload.get("max_jobs")
    if isinstance(max_jobs, int) and max_jobs >= 1:
        overview.max_jobs = max_jobs
    max_mem_mb = payload.get("max_mem_mb")
    if isinstance(max_mem_mb, int) and max_mem_mb >= 1:
        overview.max_mem_mb = max_mem_mb
    max_scheduler_jobs = payload.get("max_scheduler_jobs")
    if isinstance(max_scheduler_jobs, int) and max_scheduler_jobs >= 1:
        overview.max_scheduler_jobs = max_scheduler_jobs

    # v0.11.0: live free-RAM sample from the remote payload. Absent on a
    # pre-v0.11.0 host (key missing) → stays None (no RAM signal;
    # recommend_host falls back to the core-count proxy).
    mem_total_mb = payload.get("mem_total_mb")
    if isinstance(mem_total_mb, int) and mem_total_mb >= 0:
        overview.mem_total_mb = mem_total_mb
    mem_available_mb = payload.get("mem_available_mb")
    if isinstance(mem_available_mb, int) and mem_available_mb >= 0:
        overview.mem_available_mb = mem_available_mb

    return overview


_ADMIN_ACTION_TARGET_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._-]*\Z")


def _admin_marker_action_target(overview: HostOverview) -> str:
    """Return the CLI target that owns an overview's marker receipt."""
    marker = overview.admin_marker
    scope = admin.admin_update_marker_scope(marker)
    if (
        marker is not None
        and isinstance(marker.host, str)
        and _ADMIN_ACTION_TARGET_RE.fullmatch(marker.host) is not None
        and marker.host != "localhost"
        and scope is not None
        and marker.host in scope
    ):
        return marker.host
    return overview.host


def format_overview_text(overview: HostOverview) -> str:
    """Multi-section text rendering of one host's overview.

    Sections (each conditional on having data):
      * Header (host + vq_version)
      * Daemon health (verdict + memory pressure)
      * Queue counts (running / pending / suspended / terminal …)
      * Recent terminal counts (within the configured window)
      * Env versions (via admin.format_env_status_rows for consistency)
      * Admin-update marker (when present)

    Unreachable hosts render as a single line with the error.
    """
    if overview.admin_down is not None:
        return f"==== {overview.host} ====\n  DOWN (administratively): {overview.admin_down}"
    if not overview.reachable:
        return f"==== {overview.host} ====\n  ERROR: {overview.error}"

    # A scheduler host has no vq of its own, so it shows its helper's identity
    # rather than borrowing the driver's version number.
    if overview.vq_version is None and overview.helper_source_sha is not None:
        _ident = f"vq helper {overview.helper_source_sha}"
    else:
        _ident = f"vq {overview.vq_version or '?'}"
    lines: list[str] = [f"==== {overview.host} ({_ident}) ===="]

    # Daemon health
    if overview.daemon_health is not None:
        v = overview.daemon_health
        verdict = "OK" if v.ok else "FAIL"
        if v.vq_daemon_main_pid is not None:
            pid_part = f", daemon pid={v.vq_daemon_main_pid}"
        elif v.daemon_pidfile_pid is not None:
            pid_part = f", daemon pid={v.daemon_pidfile_pid} (pidfile)"
        else:
            pid_part = ""
        lines.append(f"  daemon:        {verdict}{pid_part}")
        if v.memory_pressure_pct is not None:
            lines.append(f"  memory pressure: {v.memory_pressure_pct:.1f}%")

    # Queue counts (sorted in a useful order: active states first)
    if overview.queue_counts:
        lines.append("  queue:")
        ordered = [
            "running",
            "pending",
            "suspended",
            "completed",
            "failed",
            "killed",
            "oom_killed",
            "starved",
            "time_exceeded",
            "aborted_by_queue",
            "interrupted",
        ]
        for state in ordered:
            count = overview.queue_counts.get(state, 0)
            if count > 0:
                lines.append(f"    {state:18s} {count:4d}")
    if overview.over_capacity_pending_jobs:
        lines.append(
            "  ⚠ over-cap pending: "
            f"{overview.over_capacity_pending_jobs} job(s) cannot dispatch "
            "under the configured daemon caps"
        )

    if overview.unconfirmed_scheduler_jobs:
        lines.append(
            "  unconfirmed scheduler reservations: "
            f"{overview.unconfirmed_scheduler_jobs} job(s), "
            f"{overview.unconfirmed_scheduler_cpus} CPU(s) "
            "(included in pending; capacity reserved)"
        )

    if overview.scheduler_queue_counts:
        lines.append("  scheduler queue:")
        ordered_scheduler = [
            "running",
            "queued",
            "held",
            "unpolled",
            "finishing",
            "poll_failed",
            "marker_probe_failed",
            "fetch_failed",
            "reattach_failed",
        ]
        seen: set[str] = set()
        for state in ordered_scheduler:
            count = overview.scheduler_queue_counts.get(state, 0)
            if count > 0:
                lines.append(f"    {state:18s} {count:4d}")
                seen.add(state)
        for state in sorted(set(overview.scheduler_queue_counts) - seen):
            count = overview.scheduler_queue_counts[state]
            if count > 0:
                lines.append(f"    {state:18s} {count:4d}")

    # Recent terminal counts
    if overview.recent_terminal_counts:
        lines.append("  recent (terminal, in window):")
        for state in sorted(overview.recent_terminal_counts.keys()):
            count = overview.recent_terminal_counts[state]
            lines.append(f"    {state:18s} {count:4d}")

    # v0.6.24: idle time. Operators reading this want a fast answer
    # to "is this host quiet right now?". Distinguish three states
    # so the line is informative on its own:
    #   * idle (no running jobs, has terminal history): "idle: 3m 12s"
    #   * busy (running > 0): "running: N job(s)"
    #   * unknown (no terminal history yet): line omitted
    running_now = overview.queue_counts.get("running", 0)
    if running_now > 0:
        lines.append(f"  running:       {running_now} job(s)")
    elif (
        overview.idle_seconds is not None
        and overview.unconfirmed_scheduler_jobs == 0
    ):
        lines.append(f"  idle:          {_format_duration(overview.idle_seconds)}")

    # v0.11.0: live free RAM — the binding constraint for QC placement. Lets
    # an operator see at a glance whether a host has room for a big job.
    if overview.mem_available_mb is not None:
        avail_gib = overview.mem_available_mb / 1024
        if overview.mem_total_mb:
            total_gib = overview.mem_total_mb / 1024
            pct = 100.0 * overview.mem_available_mb / overview.mem_total_mb
            lines.append(f"  mem free:      {avail_gib:.1f} / {total_gib:.1f} GiB ({pct:.0f}%)")
        else:
            lines.append(f"  mem free:      {avail_gib:.1f} GiB")

    # Env versions
    if overview.envs:
        lines.append("  envs:")
        for env in overview.envs:
            describe = env.current_version or env.current_describe or "(?)"
            dirty = " [DIRTY]" if env.is_dirty else ""
            err = f"  ERROR: {env.error}" if env.error else ""
            lines.append(f"    {env.name:18s} {env.branch or '?':10s} {describe}{dirty}{err}")

    # Admin update marker
    marker_diagnosis_present = any(
        isinstance(value, str) and value.strip()
        for value in (
            overview.admin_marker_status,
            overview.admin_marker_summary,
            overview.admin_marker_pid_status,
            overview.admin_marker_heartbeat_status,
            overview.admin_marker_stale_reason,
        )
    )
    if overview.admin_marker is not None:
        m = overview.admin_marker
        state = getattr(m, "state", "?")
        envs_str = ", ".join(m.envs) if m.envs else "(none)"
        status = (
            f" marker_status={overview.admin_marker_status}"
            if overview.admin_marker_status
            else ""
        )
        lines.append(
            f"  admin update marker: state={state}{status} "
            f"envs={envs_str} started={m.started_at}"
        )
        if overview.admin_marker_pid_status:
            lines.append(f"    pid: {overview.admin_marker_pid_status}")
        if overview.admin_marker_heartbeat_status:
            lines.append(
                f"    heartbeat: {overview.admin_marker_heartbeat_status}"
            )
        if overview.admin_marker_summary:
            lines.append(f"    summary: {overview.admin_marker_summary}")
        failure = getattr(m, "failure_reason", None)
        if failure:
            lines.append(f"    failure: {failure}")
    elif marker_diagnosis_present:
        status = overview.admin_marker_status or "unknown"
        lines.append(
            f"  admin update marker: marker_status={status} "
            "details=unavailable"
        )
        if overview.admin_marker_pid_status:
            lines.append(f"    pid: {overview.admin_marker_pid_status}")
        if overview.admin_marker_heartbeat_status:
            lines.append(
                f"    heartbeat: {overview.admin_marker_heartbeat_status}"
            )
        if overview.admin_marker_summary:
            lines.append(f"    summary: {overview.admin_marker_summary}")

    stale_reason = overview.admin_marker_stale_reason
    if isinstance(stale_reason, str) and stale_reason.strip():
        marker = overview.admin_marker
        durable = marker is not None and (
            marker.managed_transaction is not None or marker.owns_pause_scope
        )
        action_target = _admin_marker_action_target(overview)
        if durable:
            if marker.managed_transaction is not None:
                action = (
                    f"Inspect `vq admin status {action_target} --verbose`, "
                    f"then run `vq admin recover-update {action_target}`. "
                    "Never use --force."
                )
            else:
                action = (
                    f"Inspect `vq admin status {action_target} --verbose`, "
                    "then run `vq admin clear-update-marker "
                    f"{action_target}`, which verifies the exact paused "
                    "scope resumed before clearing. Never use --force."
                )
            lines.append(
                f"    ⚠ STALE: {stale_reason}. "
                "Dispatch remains scoped by this durable marker; a running "
                f"daemon retains it. {action}"
            )
        elif marker is None:
            action = (
                f"Inspect `vq admin status {action_target} --verbose` before "
                "clearing anything. Never use --force."
            )
            lines.append(
                f"    ⚠ STALE: {stale_reason}. Marker "
                "details could not be reconstructed, so dispatch impact is "
                f"unknown. {action}"
            )
        else:
            lines.append(
                f"    ⚠ STALE: {stale_reason}. "
                "Not gating dispatch; a running daemon auto-reaps it. "
                f"Clear by hand: `vq admin clear-update-marker {action_target}`"
            )
    elif overview.admin_marker is None and marker_diagnosis_present:
        action_target = _admin_marker_action_target(overview)
        action = (
            f"Inspect `vq admin status {action_target} --verbose` before "
            "clearing anything. Never use --force."
        )
        lines.append(
            "    ⚠ MARKER DETAILS UNAVAILABLE: dispatch impact is unknown. "
            f"{action}"
        )

    # v0.6.23: dispatch-gating state. Surface drain + throttle so
    # operators can see "why isn't dispatch happening?" without a
    # second round-trip.
    if overview.drain_state is not None:
        d = overview.drain_state
        if d.scheduler_hosts:
            mode = "scheduler-target"
            target_part = f" targets={','.join(sorted(d.scheduler_hosts))}"
        else:
            mode = "full" if d.is_full_drain else "partial-cap"
            target_part = ""
        reason_part = f" reason={d.reason!r}" if d.reason else ""
        dur_part = f" duration={d.duration_seconds}s" if d.duration_seconds else ""
        lines.append(
            f"  drain:         {mode}{target_part}{reason_part}{dur_part} "
            f"(set {d.set_at})"
        )
    if overview.throttle_state is not None:
        t = overview.throttle_state
        reason_part = f" reason={t.reason!r}" if t.reason else ""
        dur_part = f" duration={t.duration_seconds}s" if t.duration_seconds else ""
        lines.append(f"  throttle:      CPUWeight={t.weight}{reason_part}{dur_part}")

    return "\n".join(lines)


def format_overview_json(overview: HostOverview) -> dict[str, Any]:
    """Single dict for the JSON output. Stable schema — every field
    appears with its documented type even when None. Mirrors
    :class:`HostOverview` but uses dict serialization for nested
    dataclasses (ContractVerdict, EnvStatusRow, AdminUpdateMarker).
    """
    from dataclasses import asdict

    payload: dict[str, Any] = {
        "host": overview.host,
        "reachable": overview.reachable,
        "error": overview.error,
        "admin_down": overview.admin_down,
        "vq_version": overview.vq_version,
        "reported_hostname": overview.reported_hostname,
        "queue_counts": overview.queue_counts,
        "scheduler_queue_counts": overview.scheduler_queue_counts,
        "recent_terminal_counts": overview.recent_terminal_counts,
        "daemon_health": (asdict(overview.daemon_health) if overview.daemon_health else None),
        "envs": [asdict(e) for e in overview.envs],
        "admin_marker": (asdict(overview.admin_marker) if overview.admin_marker else None),
        "admin_markers": [
            asdict(snapshot) for snapshot in _admin_marker_snapshots(overview)
        ],
        "admin_marker_stale_reason": overview.admin_marker_stale_reason,
        "admin_marker_status": overview.admin_marker_status,
        "admin_marker_summary": overview.admin_marker_summary,
        "admin_marker_action": overview.admin_marker_action,
        "admin_marker_pid_status": overview.admin_marker_pid_status,
        "admin_marker_heartbeat_status": overview.admin_marker_heartbeat_status,
        "admin_marker_heartbeat_age_seconds": (
            overview.admin_marker_heartbeat_age_seconds
        ),
        # v0.6.23: pydantic models → model_dump for JSON-safe dicts.
        "drain_state": (
            overview.drain_state.model_dump(mode="json")
            if overview.drain_state is not None
            else None
        ),
        "throttle_state": (
            overview.throttle_state.model_dump(mode="json")
            if overview.throttle_state is not None
            else None
        ),
        # v0.6.24: idle time in seconds. None when busy (running > 0), when
        # scheduler execution is unconfirmed, or when the host has no terminal
        # history. Consumers should render "idle: …" only when this is not None.
        "idle_seconds": (
            None
            if overview.unconfirmed_scheduler_jobs
            else overview.idle_seconds
        ),
        # v0.7.18 *Kay's Object*: load fields feeding the --recommend
        # ranking. ``running_cpus`` + ``pending_cpus`` summed across
        # specs in those states. Zero when the host has no workload.
        "running_cpus": overview.running_cpus,
        "pending_cpus": overview.pending_cpus,
        # Subsets of the effective pending bucket, not additive placement
        # totals. They explain when capacity is held without confirmed compute.
        "unconfirmed_scheduler_jobs": overview.unconfirmed_scheduler_jobs,
        "unconfirmed_scheduler_cpus": overview.unconfirmed_scheduler_cpus,
        "over_capacity_pending_jobs": overview.over_capacity_pending_jobs,
        # v0.11.0: the daemon's configured budget (from daemon_capacity.json),
        # so the remote overview path + auto-placement can compute remaining
        # capacity. None when the daemon hasn't advertised it.
        "helper_source_sha": overview.helper_source_sha,
        "max_cpus": overview.max_cpus,
        "quota_max_concurrent_cpus": overview.quota_max_concurrent_cpus,
        "max_jobs": overview.max_jobs,
        "max_mem_mb": overview.max_mem_mb,
        "max_scheduler_jobs": overview.max_scheduler_jobs,
        # v0.11.0: live free-RAM sample (hostmem.py), fresh per gather. Feeds
        # the RAM-fit recommend_host placement + the text "mem:" line.
        "mem_total_mb": overview.mem_total_mb,
        "mem_available_mb": overview.mem_available_mb,
    }
    return payload


def recommend_host(
    overviews: list[HostOverview],
    job_cpus: int | None = None,
    job_mem_mb: int | None = None,
) -> str | None:
    """Pick the best host for a new submission (v0.7.18 *Kay's Object*;
    memory-aware since v0.11.0).

    Filter — skip hosts that can't run the job:

    * not ``reachable``;
    * ``admin_down`` (marked via ``vq host down``);
    * a full ``drain_state`` (won't dispatch);
    * a ``daemon_health`` verdict that isn't ``ok`` (dead daemon).

    Rank the survivors. **Memory is the binding constraint for QC** — an
    idle-CPU box with too little free RAM will OOM a big calculation — so a
    job's estimated peak memory (``job_mem_mb``, from vibe-qc's estimator via
    the dry-run manifest) is matched against each host's live free RAM
    (``mem_available_mb``, hostmem.py) *first*. Core headroom
    (``free = max_cpus - running - pending``; the ``-committed`` proxy when a
    host never advertised ``max_cpus``) decides dispatch-now-vs-queue
    *second*. Order (each negated for the ascending sort):

    1. **RAM fits** — host free RAM ≥ ``job_mem_mb``. The OOM guard, and the
       primary signal. ``job_mem_mb=None`` (no estimate) or a host with no
       RAM reading imposes no constraint here, collapsing to the core
       ranking below.
    2. **Cores fit** — ``free`` covers ``job_cpus`` (dispatch now vs queue).
       ``job_cpus=None`` imposes no core-fit constraint.
    3. **Most free RAM** — spread memory load toward the roomiest host.
    4. **Most free cores**, then **fewest queued cpus**, then **most idle**,
       then host name (deterministic).

    Returns the recommended host name, or ``None`` when no host passes the
    filter. Composes in a shell — ``vq submit $(vq overview --recommend)
    my.py`` — or use ``vq submit auto`` which does the gather + estimate +
    this ranking + submit in one step.
    """
    candidates: list[HostOverview] = []
    for o in overviews:
        if not o.reachable:
            continue
        # Administratively-down hosts (vq host down) must not be
        # auto-selected even if otherwise reachable.
        if o.admin_down:
            continue
        # Drained hosts won't dispatch; skip.
        if o.drain_state is not None and o.drain_state.is_full_drain:
            continue
        # Dead daemons would queue but not run; skip.
        if o.daemon_health is not None and not o.daemon_health.ok:
            continue
        candidates.append(o)
    if not candidates:
        return None

    def _free_cpus(o: HostOverview) -> int:
        committed = o.running_cpus + o.pending_cpus
        if o.max_cpus is not None:
            return o.max_cpus - committed
        # Unknown budget: negative-committed proxy. A host with no advertised
        # cap ranks by least-committed and never out-ranks a known host with
        # real core headroom for the job.
        return -committed

    def _mem_fits(o: HostOverview) -> int:
        # No estimate, or a host with no RAM reading (off Linux / never
        # sampled) → impose no memory constraint (1); let the core ranking
        # decide. A host whose known free RAM can't hold the estimate → 0
        # (OOM risk), so it loses to any host that can.
        if job_mem_mb is None or o.mem_available_mb is None:
            return 1
        return 1 if o.mem_available_mb >= job_mem_mb else 0

    # Ascending sort; every "more is better" term negated. RAM-fit first (the
    # OOM guard — the binding constraint for QC), then core-fit (dispatch now
    # vs queue), then most free RAM (spread memory), then most free cores,
    # fewest queued cpus, most idle, host. Unknown free RAM maps to 0 headroom
    # — a host we can't confirm sorts below one we can, without being excluded.
    def _rank(o: HostOverview) -> tuple[int, int, int, int, int, int, str]:
        free_cpus = _free_cpus(o)
        cpu_fits = 1 if (job_cpus is None or free_cpus >= job_cpus) else 0
        idle = o.idle_seconds if o.idle_seconds is not None else -1
        mem_avail = o.mem_available_mb or 0
        return (
            -_mem_fits(o),
            -cpu_fits,
            -mem_avail,
            -free_cpus,
            o.pending_cpus,
            -idle,
            o.host,
        )

    candidates.sort(key=_rank)
    return candidates[0].host


def format_fleet_overview_text(overviews: list[HostOverview]) -> str:
    """Stack the per-host text outputs with a blank line separator."""
    return "\n\n".join(format_overview_text(o) for o in overviews)


def format_fleet_overview_json(overviews: list[HostOverview]) -> str:
    """Single JSON object with ``hosts: [...]``."""
    return json.dumps(
        {"hosts": [format_overview_json(o) for o in overviews]},
        indent=2,
        sort_keys=True,
        default=str,
    )
