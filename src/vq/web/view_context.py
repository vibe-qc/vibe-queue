"""Pure query and view projections shared by the vq web adapters."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from urllib.parse import urlencode

from vq.spec import JobSpec, JobState
from vq.status import terminal_diagnosis_for_spec

if TYPE_CHECKING:
    from vq.web.fleet import FleetSnapshot, HostSnapshot

SORT_FIELDS = {
    "id",
    "state",
    "host",
    "sched",
    "cpus",
    "mem",
    "wall",
    "submitted",
    "diagnosis",
    "submitter",
    "command",
}
ROW_LIMITS = (50, 100, 200, 500, 1000, 2000)
DEFAULT_ROW_LIMIT = 200


def parse_time_for_sort(value: str | None) -> datetime:
    if not value:
        return datetime.min.replace(tzinfo=UTC)
    raw = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return datetime.min.replace(tzinfo=UTC)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def queue_query_state(query_params: Mapping[str, str]) -> dict[str, str | bool]:
    sort = query_params.get("sort") or "submitted"
    if sort not in SORT_FIELDS:
        sort = "submitted"
    direction = query_params.get("dir") or "desc"
    if direction not in {"asc", "desc"}:
        direction = "desc"
    state = (query_params.get("state") or "all").strip().lower()
    host = (query_params.get("host") or "all").strip()
    query = (query_params.get("q") or "").strip()
    triage = (query_params.get("triage") or "").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    try:
        limit = int(query_params.get("limit") or DEFAULT_ROW_LIMIT)
    except ValueError:
        limit = DEFAULT_ROW_LIMIT
    if limit not in ROW_LIMITS:
        limit = DEFAULT_ROW_LIMIT
    return {
        "sort": sort,
        "dir": direction,
        "state": state,
        "host": host,
        "q": query,
        "triage": triage,
        "limit": str(limit),
    }


def queue_query_string(
    query_state: dict[str, str | bool],
    **updates: object,
) -> str:
    merged: dict[str, object] = dict(query_state)
    merged.update(updates)
    pairs: list[tuple[str, str]] = []
    for key in ("state", "host", "q", "sort", "dir", "triage", "limit"):
        value = merged.get(key)
        if value in (None, "", "all"):
            continue
        if isinstance(value, bool):
            if value:
                pairs.append((key, "1"))
            continue
        if key == "sort" and value == "submitted":
            continue
        if key == "dir" and value == "desc":
            continue
        if key == "limit" and str(value) == str(DEFAULT_ROW_LIMIT):
            continue
        pairs.append((key, str(value)))
    return urlencode(pairs)


def queue_table_href(
    query_state: dict[str, str | bool],
    **updates: object,
) -> str:
    query_string = queue_query_string(query_state, **updates)
    return (
        f"/queue/_table?{query_string}"
        if query_string
        else "/queue/_table"
    )


def queue_sort_href(
    query_state: dict[str, str | bool],
    field: str,
) -> str:
    current_sort = str(query_state["sort"])
    current_dir = str(query_state["dir"])
    direction = "asc"
    if current_sort == field and current_dir == "asc":
        direction = "desc"
    query_string = queue_query_string(
        query_state,
        sort=field,
        dir=direction,
    )
    return f"/queue?{query_string}" if query_string else "/queue"


def queue_sort_label(query_state: dict[str, str | bool], field: str) -> str:
    if query_state["sort"] != field:
        return ""
    return " v" if query_state["dir"] == "desc" else " ^"


def filter_and_sort_specs(
    specs: list[JobSpec],
    query_state: dict[str, str | bool],
) -> list[JobSpec]:
    state_filter = str(query_state["state"])
    host_filter = str(query_state["host"])
    query = str(query_state["q"]).lower()
    triage_only = bool(query_state["triage"])

    def matches(spec: JobSpec) -> bool:
        host = spec.scheduler_target or "localhost"
        if state_filter != "all" and spec.state.value != state_filter:
            return False
        if host_filter != "all" and host != host_filter:
            return False
        diagnosis = terminal_diagnosis_for_spec(spec)
        if (
            triage_only
            and (
                not isinstance(diagnosis, dict)
                or diagnosis.get("action_hint") == "none"
            )
        ):
            return False
        if not query:
            return True
        haystack = " ".join(
            [
                spec.id,
                spec.state.value,
                host,
                spec.scheduler_state or "",
                spec.submitter or "",
                spec.job_name or "",
                spec.failure_reason or "",
                " ".join(spec.command),
            ]
        ).lower()
        return query in haystack

    filtered = [spec for spec in specs if matches(spec)]
    sort = str(query_state["sort"])

    def key(spec: JobSpec) -> object:
        diagnosis = terminal_diagnosis_for_spec(spec)
        diagnosis_summary = (
            diagnosis.get("summary") if isinstance(diagnosis, dict) else ""
        )
        command = " ".join(spec.command)
        values: dict[str, object] = {
            "id": spec.id,
            "state": spec.state.value,
            "host": spec.scheduler_target or "localhost",
            "sched": spec.scheduler_state or "",
            "cpus": spec.cpus,
            "mem": spec.mem_mb or 0,
            "wall": spec.wall_time_seconds or 0,
            "submitted": parse_time_for_sort(spec.submitted_at),
            "diagnosis": diagnosis_summary or "",
            "submitter": spec.submitter or "",
            "command": command,
        }
        return values[sort]

    filtered.sort(key=key, reverse=query_state["dir"] == "desc")
    return filtered


def cockpit_summary(specs: list[JobSpec]) -> dict[str, object]:
    state_counts: dict[str, int] = {}
    scheduler_hosts: set[str] = set()
    declared_mem_mb = 0
    active_cpus = 0
    terminal_attention = 0
    for spec in specs:
        state = spec.state.value
        state_counts[state] = state_counts.get(state, 0) + 1
        if spec.scheduler_target:
            scheduler_hosts.add(spec.scheduler_target)
        if spec.mem_mb:
            declared_mem_mb += spec.mem_mb
        if spec.state in {
            JobState.RUNNING,
            JobState.SUSPENDED,
            JobState.SUBMITTING,
            JobState.SUBMIT_OUTCOME_UNKNOWN,
        }:
            active_cpus += spec.cpus
        diagnosis = terminal_diagnosis_for_spec(spec)
        if isinstance(diagnosis, dict) and diagnosis.get("action_hint") != "none":
            terminal_attention += 1
    return {
        "total": len(specs),
        "active": sum(
            state_counts.get(state.value, 0)
            for state in (
                JobState.RUNNING,
                JobState.SUSPENDED,
                JobState.SUBMITTING,
                JobState.SUBMIT_OUTCOME_UNKNOWN,
            )
        ),
        "pending": state_counts.get(JobState.PENDING.value, 0),
        "terminal_attention": terminal_attention,
        "active_cpus": active_cpus,
        "declared_mem_mb": declared_mem_mb,
        "scheduler_hosts": sorted(scheduler_hosts),
        "state_counts": state_counts,
    }


def safe_fleet_next(raw: str | None) -> str:
    """Return only a same-site absolute redirect path."""
    if raw and raw.startswith("/") and not raw.startswith("//"):
        return raw
    return "/fleet"


def format_idle(seconds: int | None) -> str | None:
    if seconds is None:
        return None
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        return f"{seconds // 3600}h{(seconds % 3600) // 60:02d}m"
    return f"{seconds // 86400}d{(seconds % 86400) // 3600}h"


def host_version_label(overview: object) -> str:
    """What to print where a host's vq version goes.

    Four different things used to arrive at one chip, and the chip was
    rendered only ``{% if card.vq_version %}`` — so a host with nothing to
    report showed *nothing*, and silence is indistinguishable from
    agreement. A scheduler host in particular is daemonless: it has no vq
    version of its own, but it does run a helper whose skew from the
    driver has caused an incident, and the CLI already prints that helper
    SHA. The web console printed a blank.

    So: a real version when the host reported one, the helper's short SHA
    when it is a daemonless scheduler host, and an explicit ``unknown``
    otherwise. Never an empty chip.
    """
    version = getattr(overview, "vq_version", None)
    if isinstance(version, str) and version:
        return f"vq {version}"
    helper = getattr(overview, "helper_source_sha", None)
    if isinstance(helper, str) and helper:
        return f"vq helper {helper[:9]}"
    if getattr(overview, "is_scheduler_host", False):
        # "This host has no vq of its own and nobody has recorded its
        # helper yet" is a different, and far more actionable, statement
        # than "we could not find out". The record is written by a
        # scheduler-runtime update; until one runs from this driver there
        # is nothing to show, and saying so points at the fix.
        return "daemonless · helper not recorded"
    return "vq version unknown"


def fleet_host_card(host_snapshot: HostSnapshot) -> dict[str, object]:
    overview = host_snapshot.overview
    daemon_ok: bool | None = None
    if overview.daemon_health is not None:
        daemon_ok = bool(getattr(overview.daemon_health, "ok", False))
    reported = getattr(overview, "reported_hostname", None)
    return {
        "host": host_snapshot.host,
        # Shown under the config key when the machine calls itself
        # something else -- which is how a duplicate enrolment or a
        # machine-relative key becomes visible instead of inferable.
        "reported_hostname": reported,
        "identity_mismatch": bool(reported) and reported != host_snapshot.host,
        "self_reported": getattr(overview, "self_reported", False),
        "reachable": overview.reachable,
        "admin_down": overview.admin_down,
        "error": overview.error,
        "jobs_error": host_snapshot.jobs_error,
        "vq_version": overview.vq_version,
        "version_label": host_version_label(overview),
        "helper_source_sha": overview.helper_source_sha,
        # The per-host program versions (branch / SHA / dirty) that the
        # CLI prints and the JSON API already carries. Dropping them here
        # made the dashboard strictly less informative than `vq overview`
        # and unable to answer "which vibe-qc is on workstation?" at all.
        "envs": [
            {
                "name": getattr(env, "name", None),
                # current_version is the project's own [project] version
                # and is the source of truth; current_describe is the
                # nearest annotated tag, kept only as a fallback because
                # it is misleading when the tag is stale.
                "version": getattr(env, "current_version", None)
                or getattr(env, "current_describe", None),
                "branch": getattr(env, "branch", None),
                "dirty": getattr(env, "is_dirty", None),
                "error": getattr(env, "error", None),
            }
            for env in (overview.envs or [])
        ],
        "recent_terminal_counts": overview.recent_terminal_counts,
        "daemon_ok": daemon_ok,
        "queue_counts": overview.queue_counts,
        "scheduler_queue_counts": overview.scheduler_queue_counts,
        "running": overview.queue_counts.get("running", 0),
        "pending": overview.queue_counts.get("pending", 0),
        "running_cpus": overview.running_cpus,
        "max_cpus": overview.max_cpus,
        "mem_available_mb": overview.mem_available_mb,
        "mem_total_mb": overview.mem_total_mb,
        "draining": overview.drain_state is not None,
        "throttled": overview.throttle_state is not None,
        "admin_marker_summary": overview.admin_marker_summary,
        "admin_marker_stale_reason": overview.admin_marker_stale_reason,
        "idle": format_idle(overview.idle_seconds),
        "job_count": len(host_snapshot.jobs),
    }


#: Sortable columns on the fleet jobs table, mapped to the row key each
#: one orders by. Every rendered column is here: a header that looks like
#: the others but does not sort reads as a broken control.
FLEET_SORT_FIELDS = {
    "id",
    "host",
    "state",
    "sched",
    "cpus",
    "mem",
    "submitted",
    "submitter",
    "name",
    "diagnosis",
    "command",
    "cwd",
}

#: The default ordering. Not a column: it ranks *active* work above
#: finished work and only then sorts by recency.
#:
#: Sorting by submission time alone buries what an operator opened the
#: page to see. A job that has been running for two days was submitted
#: two days ago, so it sinks below every job that has since completed --
#: on a busy host the only running job can be hundreds of rows down.
DEFAULT_FLEET_SORT = "status"

#: Rank for :data:`DEFAULT_FLEET_SORT`. Lower sorts first. Anything not
#: listed is terminal and shares the bottom rank, where recency decides.
_STATE_RANK = {
    "running": 0,
    "suspended": 1,
    "pending": 2,
}
_TERMINAL_RANK = 3


def fleet_query_state(query_params: Mapping[str, str]) -> dict[str, str]:
    try:
        limit = int(query_params.get("limit") or DEFAULT_ROW_LIMIT)
    except ValueError:
        limit = DEFAULT_ROW_LIMIT
    if limit not in ROW_LIMITS:
        limit = DEFAULT_ROW_LIMIT
    sort = (query_params.get("sort") or DEFAULT_FLEET_SORT).strip()
    if sort not in FLEET_SORT_FIELDS and sort != DEFAULT_FLEET_SORT:
        sort = DEFAULT_FLEET_SORT
    direction = (query_params.get("dir") or "desc").strip().lower()
    if direction not in {"asc", "desc"}:
        direction = "desc"
    return {
        "host": (query_params.get("host") or "all").strip(),
        "state": (query_params.get("state") or "all").strip().lower(),
        "tag": (query_params.get("tag") or "").strip(),
        "submitter": (query_params.get("submitter") or "").strip(),
        "q": (query_params.get("q") or "").strip(),
        "limit": str(limit),
        "sort": sort,
        "dir": direction,
    }


def fleet_sort_href(query: dict[str, str], field: str) -> str:
    """Link for a sortable column header.

    Clicking the active column flips direction; clicking another column
    starts it descending, which is what you want for every column an
    operator actually reaches for first (newest, biggest, most CPUs).
    """
    direction = "asc" if query.get("sort") == field and query.get("dir") == "desc" else "desc"
    merged = dict(query, sort=field, dir=direction)
    qs = fleet_query_string(merged)
    return f"/fleet/jobs?{qs}" if qs else "/fleet/jobs"


def fleet_sort_label(query: dict[str, str], field: str) -> str:
    """Direction marker appended to the active column's header."""
    if query.get("sort") != field:
        return ""
    return " v" if query.get("dir") == "desc" else " ^"


def fleet_query_string(query: dict[str, str]) -> str:
    pairs = [
        (key, value)
        for key, value in query.items()
        if value not in ("", "all")
        and not (key == "limit" and value == str(DEFAULT_ROW_LIMIT))
        # Defaults stay out of the URL so a shared link carries only what
        # the operator actually chose.
        and not (key == "sort" and value == DEFAULT_FLEET_SORT)
        and not (key == "dir" and value == "desc")
    ]
    return urlencode(pairs)


def fleet_row_matches(row: dict, query: dict[str, str]) -> bool:
    if query["host"] != "all" and row.get("queue_host") != query["host"]:
        return False
    if query["state"] != "all" and row.get("state") != query["state"]:
        return False
    if query["tag"]:
        tags = row.get("tags")
        if not isinstance(tags, list) or query["tag"] not in tags:
            return False
    if query["submitter"] and row.get("submitter") != query["submitter"]:
        return False
    needle = query["q"].lower()
    if needle:
        command = row.get("command")
        haystack = " ".join(
            str(part)
            for part in (
                row.get("id"),
                row.get("state"),
                row.get("queue_host"),
                row.get("scheduler_state"),
                row.get("submitter"),
                row.get("job_name"),
                row.get("failure_reason"),
                " ".join(command) if isinstance(command, list) else command,
            )
            if part
        ).lower()
        if needle not in haystack:
            return False
    return True


def fleet_row_view(row: dict) -> dict[str, object]:
    command = row.get("command")
    diagnosis = row.get("terminal_diagnosis")
    return {
        "id": row.get("id"),
        "host": row.get("queue_host"),
        "state": row.get("state"),
        "sched": row.get("scheduler_state") or "",
        "cpus": row.get("cpus"),
        "mem_mb": row.get("mem_mb"),
        "submitted_at": row.get("submitted_at"),
        "submitter": row.get("submitter") or "",
        "name": row.get("job_name") or "",
        "tags": row.get("tags") or [],
        # Where the job actually ran. Two jobs with the same command in
        # different trees are otherwise indistinguishable in this table,
        # which is the normal case on a fleet running the same script
        # against several checkouts.
        "cwd": row.get("cwd") or "",
        "command": (
            " ".join(command) if isinstance(command, list) else (command or "")
        ),
        "diagnosis": (
            diagnosis.get("summary") if isinstance(diagnosis, dict) else None
        ),
    }


def _fleet_submitted(row: dict) -> datetime:
    raw = row.get("submitted_at")
    return parse_time_for_sort(raw if isinstance(raw, str) else None)


def _sort_fleet_rows(rows: list[dict], query: dict[str, str]) -> None:
    """Order the fleet job rows in place, per the active sort."""
    field = query.get("sort") or DEFAULT_FLEET_SORT
    descending = query.get("dir", "desc") == "desc"

    if field == DEFAULT_FLEET_SORT:
        # Active work first, newest within each rank. Deliberately NOT
        # reversed by `dir`: this ordering exists so a running job is
        # visible without scrolling, and an "ascending" variant that
        # buries it under every completed job has no use.
        rows.sort(key=_fleet_submitted, reverse=True)
        rows.sort(key=lambda r: _STATE_RANK.get(str(r.get("state")), _TERMINAL_RANK))
        return

    def key(row: dict) -> object:
        if field == "submitted":
            return _fleet_submitted(row)
        if field in {"cpus", "mem"}:
            raw = row.get("cpus" if field == "cpus" else "mem_mb")
            return raw if isinstance(raw, int) else -1
        if field == "command":
            command = row.get("command")
            return (
                " ".join(command) if isinstance(command, list) else str(command or "")
            )
        if field == "diagnosis":
            diagnosis = row.get("terminal_diagnosis")
            return (
                str(diagnosis.get("summary") or "")
                if isinstance(diagnosis, dict)
                else ""
            )
        mapped = {
            "host": "queue_host",
            "sched": "scheduler_state",
            "name": "job_name",
            "mem": "mem_mb",
        }.get(field, field)
        return str(row.get(mapped) or "")

    rows.sort(key=_fleet_submitted, reverse=True)  # stable tiebreak
    rows.sort(key=key, reverse=descending)


def fleet_jobs_context(
    snapshot: FleetSnapshot | None,
    query_params: Mapping[str, str],
) -> dict[str, object]:
    query = fleet_query_state(query_params)
    rows: list[dict] = snapshot.jobs if snapshot is not None else []
    filtered = [row for row in rows if fleet_row_matches(row, query)]
    _sort_fleet_rows(filtered, query)
    limit = int(query["limit"])
    visible = filtered[:limit]
    hosts = sorted({row.get("queue_host") or "" for row in rows} - {""})
    states = sorted({str(row.get("state")) for row in rows if row.get("state")})
    query_string = fleet_query_string(query)
    return {
        "snapshot": snapshot,
        "query": query,
        "jobs": [fleet_row_view(row) for row in visible],
        "host_options": hosts,
        "state_options": states,
        "row_limit_options": ROW_LIMITS,
        "shown_count": len(visible),
        "filtered_count": len(filtered),
        "total_count": len(rows),
        "sort_href": lambda field: fleet_sort_href(query, field),
        "sort_label": lambda field: fleet_sort_label(query, field),
        "table_refresh_url": (
            f"/fleet/jobs/_table?{query_string}"
            if query_string
            else "/fleet/jobs/_table"
        ),
    }


def snapshot_age_seconds(
    snapshot: FleetSnapshot | None, *, now: datetime | None = None
) -> int | None:
    """Seconds since the snapshot was gathered, or None if unknowable."""
    if snapshot is None:
        return None
    gathered = parse_time_for_sort(snapshot.gathered_at)
    if gathered == datetime.min.replace(tzinfo=UTC):
        return None
    return max(0, int(((now or datetime.now(UTC)) - gathered).total_seconds()))


def fleet_summary(cards: list[dict[str, object]]) -> dict[str, object]:
    """Fleet-level totals — the numbers an operator scans first.

    The page had none: it led with ``sweep took 4.31 s``, which tells you
    about vq, and made you read thirteen cards to learn whether anything
    was broken. Meanwhile the *single-host* page has had six metric tiles
    since v0.5.
    """
    def _int(card: dict[str, object], key: str) -> int:
        value = card.get(key)
        return value if isinstance(value, int) else 0

    reachable = [c for c in cards if c.get("reachable")]
    versions = {
        str(c.get("vq_version"))
        for c in reachable
        if isinstance(c.get("vq_version"), str) and c.get("vq_version")
    }
    return {
        "hosts": len(cards),
        "unreachable": sum(
            1 for c in cards if not c.get("reachable") and not c.get("admin_down")
        ),
        "admin_down": sum(1 for c in cards if c.get("admin_down")),
        "running": sum(_int(c, "running") for c in cards),
        "pending": sum(_int(c, "pending") for c in cards),
        "running_cpus": sum(_int(c, "running_cpus") for c in cards),
        "degraded": sum(1 for c in reachable if c.get("daemon_ok") is False),
        "draining": sum(1 for c in reachable if c.get("draining")),
        "attention": sum(1 for c in cards if c.get("jobs_error")),
        # The number an operator running a rolling update actually
        # watches. One version = converged.
        "vq_versions": sorted(versions),
        "converged": len(versions) <= 1,
    }


def fleet_grid_context(
    snapshot: FleetSnapshot | None,
    *,
    interval_seconds: int = 30,
    now: datetime | None = None,
    refresh_state: Mapping[str, object] | None = None,
) -> dict[str, object]:
    cards = (
        [fleet_host_card(host_snapshot) for host_snapshot in snapshot.hosts]
        if snapshot is not None
        else []
    )
    age = snapshot_age_seconds(snapshot, now=now)
    # A dead poller and a healthy one looked identical: the sweep thread
    # swallows its exceptions and keeps serving the last good snapshot,
    # while the page re-polls every 10 s so it still *animates*. The only
    # cue was a raw ISO timestamp. Three missed sweeps is the threshold —
    # long enough that one slow SSH fan-out is not an alarm.
    # gathered_at is the observation start, so even a just-completed slow
    # sweep has age. The interval starts after completion, not after start.
    duration = int(snapshot.duration_seconds) if snapshot is not None else 0
    stale_after = duration + max(interval_seconds * 3, interval_seconds + 30)
    refresh = refresh_state or {}
    elapsed = refresh.get("elapsed_seconds")
    return {
        "snapshot": snapshot,
        "refreshing": bool(refresh.get("refreshing")),
        "refresh_elapsed": format_idle(elapsed if isinstance(elapsed, int) else None),
        "refresh_error": refresh.get("error"),
        "interval_seconds": interval_seconds,
        "cards": cards,
        "summary": fleet_summary(cards),
        "snapshot_age_seconds": age,
        "snapshot_age": format_idle(age),
        "snapshot_stale": age is not None and age > stale_after,
        "stale_after_seconds": stale_after,
        "duplicate_enrolments": (
            snapshot.duplicate_enrolments if snapshot is not None else []
        ),
    }


def doctor_context(state: Mapping[str, object]) -> dict[str, object]:
    results = state["results"]
    ok_count = bad_count = 0
    if isinstance(results, list):
        for result in results:
            if result.get("ok"):
                ok_count += 1
            else:
                bad_count += 1
    return {
        "results": results,
        "gathered_at": state["gathered_at"],
        "refreshing": state["refreshing"],
        "ok_count": ok_count,
        "bad_count": bad_count,
    }
