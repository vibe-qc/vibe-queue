"""Calculation overview for the fleet jobs table (#168).

Pure projections over fleet snapshot rows. Each one keeps apart three
things that are easy to blur into a single column:

* what a job *declared*: ``cpus``, ``mem_mb``, ``wall_time_seconds``;
* what was *measured*: the watchdog's latest sample, or the walltime the
  scheduler reported;
* what is *unknown*, with the reason, instead of a zero or the declared
  value standing in for the measurement.

Nothing here reads the network or the clock; callers pass ``now``.
"""
from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Sequence
from datetime import datetime
from typing import TYPE_CHECKING

from markupsafe import Markup, escape

from vq.spec import TERMINAL_STATES, JobState
from vq.top import _STALE_SAMPLE_SECONDS
from vq.usage import _hms_to_seconds
from vq.web import fleet as fleet_mod

if TYPE_CHECKING:
    from vq.web.fleet import FleetSnapshot

#: A fleet snapshot row: a ``vq queue --json`` row plus the fields the
#: snapshot adds. Read defensively; a newer remote may send anything.
Row = dict[str, object]

_TERMINAL = frozenset(state.value for state in TERMINAL_STATES)
_QUEUED = frozenset({
    JobState.PENDING.value,
    JobState.SUBMITTING.value,
    "queued",
})
_HELD = frozenset({JobState.SUSPENDED.value, "held"})

#: Buckets for the summary, in display order. ``unconfirmed`` is a live
#: job whose scheduler phase is not an observed run, queue or hold
#: (unpolled, a failed poll, collecting results, an unknown submit), so
#: it is neither counted as running nor as waiting.
ACTIVITIES = ("running", "queued", "held", "unconfirmed", "finished")

USAGE_AGE_UNKNOWN = "age_unknown"

_OBSERVATION_LABELS = {
    fleet_mod.USAGE_OBSERVED: "measured",
    fleet_mod.USAGE_STALE: "stale",
    fleet_mod.USAGE_NO_SAMPLE: "no sample",
    fleet_mod.USAGE_SCHEDULER: "not sampled",
    fleet_mod.USAGE_SCHEDULER_REPORTED: "scheduler-reported",
    fleet_mod.USAGE_COLLECTOR_ERROR: "collector error",
    fleet_mod.USAGE_NOT_COLLECTED: "unknown",
    fleet_mod.USAGE_NOT_RUNNING: "",
    USAGE_AGE_UNKNOWN: "age unknown",
}

_OBSERVATION_DETAILS = {
    fleet_mod.USAGE_OBSERVED: "latest watchdog sample on the daemon host",
    fleet_mod.USAGE_STALE: (
        "the latest watchdog sample is over 30 s old or was already marked "
        "stale by its collector"
    ),
    fleet_mod.USAGE_NO_SAMPLE: (
        "no watchdog sample yet, or this host cannot sample (no /proc)"
    ),
    fleet_mod.USAGE_SCHEDULER: (
        "scheduler jobs are not sampled while they run; the scheduler's "
        "accounting arrives with the results"
    ),
    fleet_mod.USAGE_SCHEDULER_REPORTED: (
        "the scheduler's own accounting at its last poll: memory in use "
        "(Torque/PBS) or the largest task's peak (Slurm), and CPU time over "
        "walltime so far (an average, not a current reading); not added to "
        "the fresh-sample totals"
    ),
    fleet_mod.USAGE_COLLECTOR_ERROR: (
        "the driver's resource samples could not be read this sweep"
    ),
    fleet_mod.USAGE_NOT_COLLECTED: "no resource observation in this snapshot",
    fleet_mod.USAGE_NOT_RUNNING: "",
    USAGE_AGE_UNKNOWN: (
        "the sample timestamp is missing, invalid or ahead of the console "
        "clock; its freshness is unknown"
    ),
}


def _ts(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if value >= 0 else None


def _positive_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if value > 0 else None


def activity(row: Row) -> str:
    """Which of :data:`ACTIVITIES` a row belongs to.

    Reads ``effective_state``, so a scheduler job vq has handed off counts
    as running only once the scheduler reported it running.
    """
    state = row.get("state")
    if state in _TERMINAL:
        return "finished"
    effective = row.get("effective_state") or state
    if effective == JobState.RUNNING.value:
        return "running"
    if effective in _QUEUED:
        return "queued"
    if effective in _HELD:
        return "held"
    return "unconfirmed"


def format_duration(seconds: float | None) -> str:
    """``H:MM:SS``, with a day prefix past 24 h; ``-`` when unknown."""
    if seconds is None:
        return "-"
    total = max(0, int(seconds))
    days, rem = divmod(total, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    clock = f"{hours:d}:{minutes:02d}:{secs:02d}"
    return f"{days}d {clock}" if days else clock


def _paused_seconds(row: Row, end: datetime) -> float:
    paused = _number(row.get("paused_seconds_total")) or 0.0
    paused_at = _ts(row.get("paused_at"))
    if paused_at is not None:
        paused += max(0.0, (end - paused_at).total_seconds())
    return paused


def runtime_view(row: Row, *, now: datetime) -> dict[str, object]:
    """Execution time, queue wait and time limit of one row.

    A direct job's execution time runs from ``started_at`` and excludes
    paused time. A scheduler job's ``started_at`` is when vq handed it to
    the scheduler, which can include the cluster's queue wait, so its
    execution time is only the walltime the scheduler reported -- unknown
    until the scheduler reports one. Queue wait is kept separate and is
    never counted as runtime.
    """
    state = row.get("state")
    kind = activity(row)
    scheduler = row.get("scheduler_target") is not None
    submitted = _ts(row.get("submitted_at"))
    started = _ts(row.get("started_at"))
    finished = _ts(row.get("finished_at"))

    seconds: float | None = None
    source: str | None = None
    if scheduler:
        used = _hms_to_seconds(_text(row.get("scheduler_walltime_used")))
        if used is not None:
            seconds, source = float(used), "scheduler"
    elif started is not None:
        end = (
            finished if state in _TERMINAL
            else now if state in {JobState.RUNNING.value, JobState.SUSPENDED.value}
            else None
        )
        if end is not None:
            elapsed = (end - started).total_seconds() - _paused_seconds(row, end)
            seconds, source = max(0.0, elapsed), "vq"

    # A queued job's wait is still growing. A started direct job waited
    # from submission to dispatch. A started scheduler job's wait inside
    # the cluster queue is not recorded, so it stays unknown.
    waiting = False
    wait_seconds: float | None = None
    if kind == "queued" and submitted is not None:
        waiting = True
        wait_seconds = max(0.0, (now - submitted).total_seconds())
    elif not scheduler and started is not None and submitted is not None:
        wait_seconds = max(0.0, (started - submitted).total_seconds())

    limit = _hms_to_seconds(_text(row.get("scheduler_walltime_limit"))) if scheduler else None
    limit_source = "scheduler" if limit is not None else None
    if limit is None:
        limit = _positive_int(row.get("wall_time_seconds"))
        limit_source = "declared" if limit is not None else None

    return {
        "seconds": seconds,
        "source": source,
        "final": state in _TERMINAL,
        "started_at": row.get("started_at") if isinstance(row.get("started_at"), str) else None,
        "wait_seconds": wait_seconds,
        "waiting": waiting,
        "limit_seconds": limit,
        "limit_source": limit_source,
    }


def observation_view(row: Row, *, now: datetime) -> dict[str, object]:
    """Measured CPU and memory with freshness evaluated at display time.

    A cached collection verdict cannot keep an aging sample fresh. Preserve
    last-known numbers for stale or undated readings, but only timestamped,
    fresh samples may contribute to the measured summary.
    """
    raw = row.get("resource_observation")
    observation = raw if isinstance(raw, dict) else {}
    status = observation.get("status")
    if status not in _OBSERVATION_DETAILS:
        status = fleet_mod.USAGE_NOT_COLLECTED
    measured = status in {fleet_mod.USAGE_OBSERVED, fleet_mod.USAGE_STALE, USAGE_AGE_UNKNOWN,
                          fleet_mod.USAGE_SCHEDULER_REPORTED}
    cpu_percent = _number(observation.get("cpu_percent")) if measured else None
    rss_mb = _number(observation.get("rss_mb")) if measured else None
    peak_mb = _number(observation.get("rss_peak_mb")) if measured else None
    peak_since = _ts(observation.get("rss_peak_since")) if peak_mb is not None else None
    if peak_since is None:
        peak_mb = None
    sample_at = _ts(observation.get("sample_at")) if measured else None
    age = (now - sample_at).total_seconds() if sample_at is not None else None
    if age is not None and age < 0:
        age = None
    if status == fleet_mod.USAGE_OBSERVED:
        if age is None:
            status = USAGE_AGE_UNKNOWN
        elif age > _STALE_SAMPLE_SECONDS:
            status = fleet_mod.USAGE_STALE
    if measured:
        when = f"{format_duration(age)} ago" if age is not None else "age unknown"
        label = (f"stale, {when}" if status == fleet_mod.USAGE_STALE
                 else f"scheduler, {when}" if status == fleet_mod.USAGE_SCHEDULER_REPORTED
                 else when)
    else:
        label = _OBSERVATION_LABELS[status]
    return {
        "status": status,
        "label": label,
        "detail": _OBSERVATION_DETAILS[status],
        "cpu_cores": round(cpu_percent / 100.0, 2) if cpu_percent is not None else None,
        "rss_mb": round(rss_mb, 1) if rss_mb is not None else None,
        # #168: a sampled peak, never summed into a total: peaks of different
        # jobs happen at different times.
        "rss_peak_mb": round(peak_mb, 1) if peak_mb is not None else None,
        "rss_peak_since": observation.get("rss_peak_since") if peak_since is not None else None,
        "rss_peak_scope": observation.get("rss_peak_scope") if peak_since is not None else None,
        "sample_at": observation.get("sample_at") if sample_at is not None else None,
        "age_seconds": age,
    }


def location_view(row: Row) -> dict[str, object]:
    """Where a job executes, apart from which daemon owns it.

    A direct job runs on its queue host. A scheduler job runs on the
    compute node(s) the scheduler reported, which are unknown until it
    starts; its driver is named separately because it does not run
    there.
    """
    if row.get("scheduler_target") is None:
        host = row.get("queue_host")
        return {"nodes": host if isinstance(host, str) else None, "driver": None}
    exec_host = row.get("scheduler_exec_host")
    owner = row.get("queue_owner")
    return {
        "nodes": exec_host if isinstance(exec_host, str) and exec_host else None,
        "driver": owner if isinstance(owner, str) else None,
    }


def software_view(row: Row) -> str | None:
    """Program and its recorded version or build, when the job has them."""
    pin = row.get("program_runtime_pin")
    pin = pin if isinstance(pin, dict) else {}
    program = row.get("program") or pin.get("program_kind")
    if not isinstance(program, str) or not program:
        return None
    version = pin.get("program_version")
    if isinstance(version, str) and version:
        return f"{program} {version}"
    sha = pin.get("resolved_git_sha") or pin.get("expected_git_sha")
    if isinstance(sha, str) and sha:
        return f"{program} @{sha[:9]}"
    return program


def calculation_view(row: Row, *, now: datetime) -> dict[str, object]:
    return {
        "activity": activity(row),
        "runtime": runtime_view(row, now=now),
        "observation": observation_view(row, now=now),
        "location": location_view(row),
        "software": software_view(row),
    }


def calculation_summary(rows: Iterable[Row], *, now: datetime) -> dict[str, object]:
    """Counts, allocated resources and measured usage over ``rows``.

    Allocation and measurement are separate totals. Allocation sums what
    running jobs declared; measurement sums only fresh samples, and
    ``partial`` says when some running job contributed none -- its reason
    is in ``unmeasured``. RSS totals add each job's resident memory, so
    pages shared between jobs are counted once per job.
    """
    counts = dict.fromkeys(ACTIVITIES, 0)
    running: list[Row] = []
    for row in rows:
        kind = activity(row)
        counts[kind] += 1
        if kind == "running":
            running.append(row)
    allocated_cpus = sum(_positive_int(row.get("cpus")) or 0 for row in running)
    mem_limits = [_positive_int(row.get("mem_mb")) for row in running]
    measured_jobs = 0
    cores = 0.0
    rss = 0.0
    unmeasured: Counter[str] = Counter()
    for row in running:
        observation = observation_view(row, now=now)
        if observation["status"] != fleet_mod.USAGE_OBSERVED:
            unmeasured[str(observation["status"])] += 1
            continue
        measured_jobs += 1
        if isinstance(observation["cpu_cores"], float):
            cores += observation["cpu_cores"]
        if isinstance(observation["rss_mb"], float):
            rss += observation["rss_mb"]
    return {
        "counts": counts,
        "allocated_cpus": allocated_cpus,
        "allocated_mem_mb": sum(limit or 0 for limit in mem_limits),
        "unlimited_mem_jobs": sum(limit is None for limit in mem_limits),
        "measured_jobs": measured_jobs,
        "measured_cpu_cores": round(cores, 1),
        "measured_rss_mb": round(rss, 1),
        "partial": measured_jobs < len(running),
        "unmeasured": [
            {"status": status, "count": count, "label": _OBSERVATION_LABELS[status],
             "detail": _OBSERVATION_DETAILS[status]}
            for status, count in sorted(unmeasured.items())
        ],
    }


def coverage_view(
    snapshot: FleetSnapshot | None, *, host: str = "all",
) -> dict[str, object]:
    """Which hosts' jobs this snapshot is missing, and which it may double count.

    A host whose queue could not be read contributes no live rows, so every
    total on the page is partial while one is listed here. Its last-known
    rows, when the console still has them, are shown apart from the totals.
    """
    if snapshot is None:
        return {"unlisted": [], "usage_errors": [], "duplicates": []}
    hosts = [h for h in snapshot.hosts if host == "all" or h.host == host]
    unlisted = []
    for h in hosts:
        reason = h.overview.admin_down or h.jobs_error
        if reason:
            unlisted.append({"host": h.host, "reason": reason,
                             "last_known_count": len(h.last_known),
                             "last_known_at": h.last_known_at})
    return {
        "unlisted": unlisted,
        "usage_errors": [
            {"host": h.host, "reason": h.usage_error} for h in hosts if h.usage_error
        ],
        "duplicates": [
            {"hostname": name, "keys": keys}
            for name, keys in snapshot.duplicate_enrolments
            if host == "all" or host in keys
        ],
    }


# --- Trends and per-host load (#168) ---------------------------------------------


def trend_point(snapshot: FleetSnapshot) -> dict[str, object]:
    """One sweep's fleet totals, as the console's trend history keeps them.

    Measured usage counts only samples fresh at the sweep. ``partial`` marks a
    sweep whose totals miss something: a running job without a fresh sample,
    or a host whose queue was not listed.
    """
    now = _ts(snapshot.gathered_at)
    summary = calculation_summary(snapshot.jobs, now=now) if now is not None else None
    if summary is None:
        return {"at": snapshot.gathered_at, "running": None, "allocated_cpus": None,
                "measured_cores": None, "measured_rss_mb": None, "partial": True}
    counts = summary["counts"]
    assert isinstance(counts, dict)
    return {
        "at": snapshot.gathered_at,
        "running": counts["running"],
        "allocated_cpus": summary["allocated_cpus"],
        "measured_cores": summary["measured_cpu_cores"],
        "measured_rss_mb": summary["measured_rss_mb"],
        "partial": bool(summary["partial"]) or any(h.unlisted for h in snapshot.hosts),
    }


def sparkline_svg(
    series: Sequence[tuple[str, Sequence[float | None]]],
    *,
    marks: Sequence[bool] = (),
    title: str,
    width: int = 200,
    height: int = 40,
) -> Markup:
    """A small inline SVG of one or more series on a shared scale from zero.

    A ``None`` value breaks its line instead of reading as zero. ``marks``
    puts a dot under each point flagged partial. Colours come from CSS
    classes, so the page's theme applies.
    """
    pad = 2.0
    length = max((len(values) for _, values in series), default=0)
    peak = max((v for _, values in series for v in values if v is not None), default=0.0)
    scale = peak if peak > 0 else 1.0

    def x(index: int) -> float:
        return pad + (width - 2 * pad) * (index / (length - 1) if length > 1 else 0.5)

    def y(value: float) -> float:
        return height - pad - (height - 2 * pad) * value / scale

    parts = [f'<svg class="sparkline" viewBox="0 0 {width} {height}" width="{width}" '
             f'height="{height}" role="img" aria-label="{escape(title)}">'
             f"<title>{escape(title)}</title>"]
    for css, values in series:
        runs: list[list[str]] = [[]]
        for index, value in enumerate(values):
            if value is None:
                runs.append([])
                continue
            runs[-1].append(f"{x(index):.1f},{y(value):.1f}")
        for run in runs:
            if len(run) > 1:
                parts.append(f'<polyline class="{escape(css)}" points="{" ".join(run)}"/>')
            elif run:
                cx, cy = run[0].split(",")
                parts.append(f'<circle class="{escape(css)}" cx="{cx}" cy="{cy}" r="1.5"/>')
    for index, partial in enumerate(marks):
        if partial:
            parts.append(f'<circle class="trend-partial" cx="{x(index):.1f}" '
                         f'cy="{height - 1:.1f}" r="1"/>')
    parts.append("</svg>")
    return Markup("".join(parts))


def _float(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def trend_view(points: Sequence[dict[str, object]]) -> dict[str, object] | None:
    """Sparklines over the console's sweep history, or None before a sweep."""
    if not points:
        return None
    running = [_float(p.get("running")) for p in points]
    allocated = [_float(p.get("allocated_cpus")) for p in points]
    cores = [_float(p.get("measured_cores")) for p in points]
    rss = [None if (v := _float(p.get("measured_rss_mb"))) is None else v / 1024.0
           for p in points]
    marks = [p.get("partial") is True for p in points]
    latest = points[-1]

    def last(values: list[float | None]) -> float | None:
        return values[-1] if values else None

    return {
        "points": len(points),
        "since": points[0].get("at"),
        "latest_partial": latest.get("partial") is True,
        "charts": [
            {"label": "running jobs", "latest": last(running),
             "svg": sparkline_svg([("trend-running", running)], title="running jobs")},
            {"label": "cpu: cores used / allocated", "latest": last(cores),
             "latest_allocated": last(allocated),
             "svg": sparkline_svg([("trend-allocated", allocated), ("trend-used", cores)],
                                  marks=marks, title="cpu cores used and allocated")},
            {"label": "rss used, GiB", "latest": last(rss),
             "svg": sparkline_svg([("trend-used", rss)], marks=marks,
                                  title="measured rss in GiB")},
        ],
    }


def host_load_view(
    snapshot: FleetSnapshot | None, *, now: datetime, host: str = "all",
) -> list[dict[str, object]]:
    """Per host: running work, what it declared, what was measured, and the
    host's own budget. Never a zero standing in for an unlisted host or an
    unsampled scheduler lane."""
    if snapshot is None:
        return []
    rows = []
    for h in snapshot.hosts:
        if host != "all" and h.host != host:
            continue
        running = [row for row in h.jobs if activity(row) == "running"]
        summary = calculation_summary(running, now=now)
        overview = h.overview
        lane = h.driver is not None or h.cluster is not None or overview.is_scheduler_host
        rows.append({
            "host": h.host,
            "listed": not h.unlisted,
            "reason": (overview.admin_down or h.jobs_error) if h.unlisted else None,
            "lane": lane,
            "running": len(running),
            "allocated_cpus": summary["allocated_cpus"],
            "max_cpus": overview.max_cpus,
            "measured_cores": summary["measured_cpu_cores"] if summary["measured_jobs"] else None,
            "measured_rss_mb": summary["measured_rss_mb"] if summary["measured_jobs"] else None,
            "measured_jobs": summary["measured_jobs"],
            "partial": summary["partial"],
            "mem_total_mb": overview.mem_total_mb,
            "mem_available_mb": overview.mem_available_mb,
        })
    return rows
