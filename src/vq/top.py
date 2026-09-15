"""``vq top`` — a top(1)-style live resource snapshot of the RUNNING jobs.

The watchdog (:mod:`vq.watchdog`) appends one JSON line per sample to
``<workspace>/_vq/samples.jsonl`` while a job runs; the *last* line is the
job's current CPU% and RSS. ``vq top`` reads that last line for every RUNNING
job and renders it against the job's declared ceilings — ``mem_mb`` and
``wall_time_seconds`` — so an operator can see at a glance whether a job is
saturating its cores, creeping toward its memory cap, or near its wall-time
deadline. It's the per-job complement to ``vq overview`` (host-level) and
``vq status`` (one job, no live resource curve).

CPU% is the whole process group's usage, so a job using N cores reads ~N*100%
(an 8-core CRYSTAL run saturating its cores shows ~800%). The watchdog only
samples on hosts where it can read ``/proc``; on a host without it (macOS) the
resource columns read ``-`` while ACTIVE / ELAPSED (derived from ``started_at``)
still show. ACTIVE subtracts ``paused_seconds_total`` so it matches the
watchdog's wall-time accounting; ELAPSED remains the wall-clock age since
dispatch.
"""
from __future__ import annotations

import json
import math
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from vq.host import is_local_host
from vq.listing import list_jobs, queue_handle_for_spec
from vq.spec import JobSpec, JobState

# A latest sample older than this is flagged stale (the daemon stopped
# sampling, or the job's process is wedged): the watchdog's default sample
# interval is 5 s, so ~6 missed samples.
_STALE_SAMPLE_SECONDS = 30.0


@dataclass
class TopRow:
    """One RUNNING job's live-resource snapshot."""

    jobid: str
    name: str | None
    queue_handle: dict[str, str | None]
    cpus: int
    cpu_percent: float | None
    rss_mb: float | None
    mem_mb: int | None
    mem_percent: float | None
    active_elapsed_seconds: float | None
    elapsed_seconds: float | None
    wall_time_seconds: int | None
    wall_percent: float | None
    sample_stale: bool


def _read_latest_sample(workspace: Path) -> dict | None:
    """Return the last JSON record of ``<workspace>/_vq/samples.jsonl``, or
    None when the file is absent / empty / unreadable."""
    p = workspace / "_vq" / "samples.jsonl"
    try:
        text = p.read_text(encoding="utf-8")
    except OSError:
        return None
    for line in reversed(text.splitlines()):
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if isinstance(rec, dict):
            return rec
    return None


def _parse_iso(ts: object) -> datetime | None:
    if not isinstance(ts, str) or not ts:
        return None
    try:
        parsed = datetime.fromisoformat(ts)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _sample_nonnegative_float(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        parsed = float(value)
    except (OverflowError, ValueError):
        return None
    if not math.isfinite(parsed) or parsed < 0:
        return None
    return parsed


def gather_top_rows(
    specs: list[JobSpec], *, host: str = "localhost", now: datetime | None = None
) -> list[TopRow]:
    """Build a :class:`TopRow` for every RUNNING spec, newest-resource-first.

    ELAPSED comes from ``started_at`` (always available for a running job).
    ACTIVE subtracts ``paused_seconds_total`` to mirror the watchdog's
    wall-time budget. CPU% / RSS come from the latest watchdog sample (``-``
    until the first sample lands). Sorted by CPU% descending so the busiest
    jobs are on top.
    """
    now = now or datetime.now(UTC)
    rows: list[TopRow] = []
    for spec in specs:
        if spec.state != JobState.RUNNING:
            continue
        sample = _read_latest_sample(Path(spec.cwd))
        cpu_percent: float | None = None
        rss_mb: float | None = None
        sample_stale = False
        if sample is not None:
            cpu_percent = _sample_nonnegative_float(sample.get("cpu_percent"))
            rss_mb = _sample_nonnegative_float(sample.get("rss_mb"))
            sample_dt = _parse_iso(sample.get("ts"))
            if sample_dt is not None:
                age = (now - sample_dt).total_seconds()
                sample_stale = age > _STALE_SAMPLE_SECONDS
        started = _parse_iso(spec.started_at)
        elapsed = (now - started).total_seconds() if started is not None else None
        active_elapsed = (
            max(0.0, elapsed - spec.paused_seconds_total)
            if elapsed is not None
            else None
        )
        mem_percent = (
            (rss_mb / spec.mem_mb) * 100.0
            if rss_mb is not None and spec.mem_mb is not None and spec.mem_mb > 0
            else None
        )
        wall_percent = (
            (active_elapsed / spec.wall_time_seconds) * 100.0
            if (
                active_elapsed is not None
                and spec.wall_time_seconds is not None
                and spec.wall_time_seconds > 0
            )
            else None
        )
        rows.append(
            TopRow(
                jobid=spec.id,
                name=spec.job_name,
                queue_handle=queue_handle_for_spec(spec, host),
                cpus=spec.cpus,
                cpu_percent=cpu_percent,
                rss_mb=rss_mb,
                mem_mb=spec.mem_mb,
                mem_percent=mem_percent,
                active_elapsed_seconds=active_elapsed,
                elapsed_seconds=elapsed,
                wall_time_seconds=spec.wall_time_seconds,
                wall_percent=wall_percent,
                sample_stale=sample_stale,
            )
        )
    rows.sort(key=lambda r: (r.cpu_percent if r.cpu_percent is not None else -1.0), reverse=True)
    return rows


def _fmt_pct(pct: float | None) -> str:
    return "-" if pct is None else f"{pct:.0f}%"


def _fmt_mb(mb: float | None) -> str:
    if mb is None:
        return "-"
    if mb >= 1024:
        return f"{mb / 1024:.1f}G"
    return f"{mb:.0f}M"


def _fmt_dur(seconds: float | None) -> str:
    if seconds is None:
        return "-"
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:d}:{m:02d}:{s:02d}"


def format_top_table(rows: list[TopRow]) -> str:
    """Render rows as a fixed-width table. ``rows`` is assumed pre-sorted."""
    if not rows:
        return "(no running jobs)"
    header = [
        "JOBID",
        "NAME",
        "CPU%",
        "RSS",
        "MEM",
        "MEM%",
        "ACTIVE",
        "ELAPSED",
        "WALL",
        "WALL%",
    ]
    body: list[list[str]] = []
    for r in rows:
        body.append(
            [
                r.jobid,
                (r.name or "-")[:20],
                _fmt_pct(r.cpu_percent) + ("*" if r.sample_stale else ""),
                _fmt_mb(r.rss_mb),
                _fmt_mb(r.mem_mb),
                _fmt_pct(r.mem_percent),
                _fmt_dur(r.active_elapsed_seconds),
                _fmt_dur(r.elapsed_seconds),
                _fmt_dur(r.wall_time_seconds),
                _fmt_pct(r.wall_percent),
            ]
        )
    widths = [
        max(len(header[i]), *(len(row[i]) for row in body))
        for i in range(len(header))
    ]
    # JOBID + NAME left-justified; numeric columns right-justified.
    left = {0, 1}

    def _fmt_line(cells: list[str]) -> str:
        return "  ".join(
            cells[i].ljust(widths[i]) if i in left else cells[i].rjust(widths[i])
            for i in range(len(cells))
        ).rstrip()

    lines = [_fmt_line(header), *(_fmt_line(row) for row in body)]
    if any(r.sample_stale for r in rows):
        lines.append("")
        lines.append("* resource sample is stale (>30s old) — daemon not sampling, or job wedged")
    return "\n".join(lines)


def format_top_json(rows: list[TopRow]) -> str:
    """Render rows as a JSON array, for `vq top --json` (dashboards / scripts)."""
    return json.dumps(
        [
            {
                "jobid": r.jobid,
                "name": r.name,
                "queue_handle": r.queue_handle,
                "cpus": r.cpus,
                "cpu_percent": r.cpu_percent,
                "rss_mb": r.rss_mb,
                "mem_mb": r.mem_mb,
                "mem_percent": r.mem_percent,
                "active_elapsed_seconds": r.active_elapsed_seconds,
                "elapsed_seconds": r.elapsed_seconds,
                "wall_time_seconds": r.wall_time_seconds,
                "wall_percent": r.wall_percent,
                "sample_stale": r.sample_stale,
            }
            for r in rows
        ],
        indent=2,
    )


def show_top_local(
    host: str,
    *,
    multi_user: bool = False,
    queue_dir: Path | None = None,
    as_json: bool = False,
) -> str:
    """Gather + render the local host's running-job resource snapshot."""
    if not is_local_host(host):
        raise NotImplementedError(f"remote top for {host!r} not implemented here")
    specs = list_jobs(host, queue_dir=queue_dir, multi_user=multi_user)
    rows = gather_top_rows(specs, host=host)
    return format_top_json(rows) if as_json else format_top_table(rows)


def watch_loop(
    render: Callable[[], str],
    *,
    interval: float,
    host: str,
    sleep: Callable[[float], None],
    write: Callable[[str], None],
    clock: Callable[[], str],
) -> None:
    """`vq top --watch`: re-render ``render()`` every ``interval`` seconds,
    clearing the screen between frames, until the user hits Ctrl-C.

    ``sleep`` / ``write`` / ``clock`` are injected so the loop is testable
    without real time or a real terminal — the CLI passes ``time.sleep``,
    ``click.echo``, and ``utcnow_iso``.
    """
    try:
        while True:
            body = render()
            # ESC[2J clear screen, ESC[3J clear scrollback, ESC[H home cursor.
            write(
                f"\033[2J\033[3J\033[H"
                f"vq top — {host} — every {interval:g}s — {clock()} — Ctrl-C to exit\n"
                f"{body}\n"
            )
            sleep(interval)
    except KeyboardInterrupt:
        pass
