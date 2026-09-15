"""Usage accounting over the retained vq job specs."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime

from vq.spec import JobSpec, JobState

GROUP_BY_CHOICES = ("tag", "submitter", "host", "none")


@dataclass(frozen=True)
class UsageRow:
    """One grouped usage bucket."""

    group: str
    jobs: int
    wall_seconds: float
    cpu_seconds: float

    @property
    def wall_hours(self) -> float:
        return self.wall_seconds / 3600.0

    @property
    def cpu_hours(self) -> float:
        return self.cpu_seconds / 3600.0


@dataclass(frozen=True)
class UsageReport:
    """A usage summary for one rendered host view."""

    host: str
    group_by: str
    include_active: bool
    rows: list[UsageRow]
    total: UsageRow
    skipped_jobs: int


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


def _hms_to_seconds(value: str | None) -> int | None:
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


def _paused_seconds(spec: JobSpec, *, end: datetime) -> float:
    paused = spec.paused_seconds_total
    paused_at = _parse_iso(spec.paused_at)
    if paused_at is not None:
        paused += max(0.0, (end - paused_at).total_seconds())
    return paused


def _runtime_seconds(
    spec: JobSpec,
    *,
    now: datetime,
    include_active: bool,
) -> float | None:
    """Return billable wall seconds for one spec, or None when unknown.

    Scheduler jobs prefer the cluster-reported walltime because vq's
    ``started_at`` can include time spent queued by the batch scheduler.
    """
    if spec.scheduler_target is not None:
        scheduler_seconds = _hms_to_seconds(spec.scheduler_walltime_used)
        if scheduler_seconds is not None:
            return float(scheduler_seconds)

    if spec.is_terminal:
        end = _parse_iso(spec.finished_at)
    elif include_active and spec.state in {JobState.RUNNING, JobState.SUSPENDED}:
        end = now
    else:
        return None

    start = _parse_iso(spec.started_at)
    if start is None or end is None:
        return None
    elapsed = (end - start).total_seconds()
    return max(0.0, elapsed - _paused_seconds(spec, end=end))


def _groups_for_spec(spec: JobSpec, *, host: str, group_by: str) -> list[str]:
    if group_by == "tag":
        return spec.tags or ["(untagged)"]
    if group_by == "submitter":
        return [spec.submitter or "(unknown)"]
    if group_by == "host":
        return [spec.scheduler_target or host]
    if group_by == "none":
        return ["total"]
    raise ValueError(f"unknown group_by {group_by!r}")


def build_usage_report(
    specs: list[JobSpec],
    *,
    host: str,
    group_by: str = "tag",
    include_active: bool = False,
    now: datetime | None = None,
) -> UsageReport:
    """Summarise CPU-hours from retained specs.

    Jobs without enough timing data are skipped rather than guessed.
    Multi-tag jobs count once in each tag bucket; the total row always counts
    each usable job exactly once.
    """
    if group_by not in GROUP_BY_CHOICES:
        raise ValueError(f"unknown group_by {group_by!r}")
    now = now or datetime.now(UTC)
    buckets: dict[str, UsageRow] = {}
    total_jobs = 0
    total_wall = 0.0
    total_cpu = 0.0
    skipped_jobs = 0

    for spec in specs:
        runtime = _runtime_seconds(spec, now=now, include_active=include_active)
        if runtime is None:
            skipped_jobs += 1
            continue
        cpu_seconds = runtime * spec.cpus
        total_jobs += 1
        total_wall += runtime
        total_cpu += cpu_seconds
        for group in _groups_for_spec(spec, host=host, group_by=group_by):
            old = buckets.get(group)
            if old is None:
                buckets[group] = UsageRow(
                    group=group,
                    jobs=1,
                    wall_seconds=runtime,
                    cpu_seconds=cpu_seconds,
                )
            else:
                buckets[group] = UsageRow(
                    group=group,
                    jobs=old.jobs + 1,
                    wall_seconds=old.wall_seconds + runtime,
                    cpu_seconds=old.cpu_seconds + cpu_seconds,
                )

    rows = sorted(
        buckets.values(),
        key=lambda row: (-row.cpu_seconds, -row.wall_seconds, row.group),
    )
    return UsageReport(
        host=host,
        group_by=group_by,
        include_active=include_active,
        rows=rows,
        total=UsageRow(
            group="total",
            jobs=total_jobs,
            wall_seconds=total_wall,
            cpu_seconds=total_cpu,
        ),
        skipped_jobs=skipped_jobs,
    )


def _fmt_hours(seconds: float) -> str:
    return f"{seconds / 3600.0:.2f}"


def format_usage_table(report: UsageReport) -> str:
    """Render usage as a compact table."""
    if not report.rows:
        lines = ["(no usage records)"]
        if report.skipped_jobs:
            lines.append(f"skipped jobs without usable timing: {report.skipped_jobs}")
        return "\n".join(lines)

    header = ["GROUP", "JOBS", "CPU-HOURS", "WALL-HOURS"]
    body = [
        [
            row.group,
            str(row.jobs),
            _fmt_hours(row.cpu_seconds),
            _fmt_hours(row.wall_seconds),
        ]
        for row in report.rows
    ]
    if report.group_by != "none":
        body.append(
            [
                "TOTAL",
                str(report.total.jobs),
                _fmt_hours(report.total.cpu_seconds),
                _fmt_hours(report.total.wall_seconds),
            ]
        )
    widths = [
        max(len(header[i]), *(len(row[i]) for row in body))
        for i in range(len(header))
    ]

    def _fmt_line(cells: list[str]) -> str:
        return "  ".join(
            cells[i].ljust(widths[i]) if i == 0 else cells[i].rjust(widths[i])
            for i in range(len(cells))
        ).rstrip()

    lines = [_fmt_line(header), *(_fmt_line(row) for row in body)]
    if report.group_by == "tag":
        lines.append("note: multi-tag jobs are counted once in each tag bucket")
    if report.skipped_jobs:
        lines.append(f"skipped jobs without usable timing: {report.skipped_jobs}")
    return "\n".join(lines)


def _row_to_json(row: UsageRow) -> dict[str, float | int | str]:
    return {
        "group": row.group,
        "jobs": row.jobs,
        "wall_seconds": round(row.wall_seconds, 3),
        "wall_hours": round(row.wall_hours, 6),
        "cpu_seconds": round(row.cpu_seconds, 3),
        "cpu_hours": round(row.cpu_hours, 6),
    }


def format_usage_json(report: UsageReport) -> str:
    """Render usage as a stable JSON object."""
    return json.dumps(
        {
            "host": report.host,
            "group_by": report.group_by,
            "include_active": report.include_active,
            "rows": [_row_to_json(row) for row in report.rows],
            "total": _row_to_json(report.total),
            "skipped_jobs": report.skipped_jobs,
        },
        indent=2,
        sort_keys=True,
    )
