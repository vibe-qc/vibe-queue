"""Queue ETA estimates from retained job history."""
from __future__ import annotations

import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime

from vq.spec import JobSpec, JobState

# Sources that key on command shape / cpu count rather than on the work the
# job performs (tags identify the work). With few samples these inherit
# whatever expensive job most recently shared the command shape, so they must
# not be presented with the same confidence as a tag-informed estimate.
_GENERIC_SOURCES = frozenset({"command+cpus", "command", "cpus", "all"})
MIN_RELIABLE_GENERIC_SAMPLES = 3


@dataclass(frozen=True)
class JobDurationEstimate:
    """Historical duration estimate for one pending job."""

    seconds: float
    samples: int
    source: str
    # Stable identities of the retained jobs behind ``samples``. Queue-wide
    # aggregation uses their union so reusing the same history for several
    # pending jobs cannot manufacture additional evidence.
    evidence_ids: frozenset[str] = field(default_factory=frozenset)


@dataclass(frozen=True)
class QueueEtaEstimate:
    """Estimated wait until a pending job reaches its dispatch-order turn."""

    seconds: float
    jobs_ahead: int
    source_counts: dict[str, int]
    sample_count: int
    # Generic command-shape sources whose total matched sample count is below
    # ``MIN_RELIABLE_GENERIC_SAMPLES``: low-confidence history that must not
    # be rendered as a confident point estimate.
    weak_sources: dict[str, int] = field(default_factory=dict)


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


def _duration_seconds(spec: JobSpec) -> float | None:
    if spec.state != JobState.COMPLETED:
        return None
    if spec.scheduler_target is not None:
        scheduler_seconds = _hms_to_seconds(spec.scheduler_walltime_used)
        if scheduler_seconds is not None:
            return float(scheduler_seconds)
    start = _parse_iso(spec.started_at)
    end = _parse_iso(spec.finished_at)
    if start is None or end is None:
        return None
    return max(0.0, (end - start).total_seconds() - spec.paused_seconds_total)


def _finished_at(spec: JobSpec) -> datetime:
    return _parse_iso(spec.finished_at) or datetime.min.replace(tzinfo=UTC)


def _command_key(spec: JobSpec) -> tuple[str, ...]:
    return tuple(spec.command)


def _shared_tags(a: JobSpec, b: JobSpec) -> bool:
    return bool(set(a.tags).intersection(b.tags))


def _candidate_history(
    history: list[JobSpec],
    target: JobSpec,
) -> list[tuple[JobSpec, float]]:
    usable: list[tuple[JobSpec, float]] = []
    for spec in history:
        duration = _duration_seconds(spec)
        if duration is not None:
            usable.append((spec, duration))
    usable.sort(key=lambda item: _finished_at(item[0]), reverse=True)
    return usable


def estimate_job_duration(
    target: JobSpec,
    history: list[JobSpec],
    *,
    max_samples: int = 20,
) -> JobDurationEstimate | None:
    """Estimate one job's runtime from retained completed jobs.

    Matching is intentionally conservative and inspectable: try tag + command +
    cpus first, then progressively broader tag/command/cpus fallbacks. The
    median of the most recent matching samples is used to avoid one long failed
    run dominating the ETA.
    """
    candidates = _candidate_history(history, target)
    if not candidates:
        return None

    command = _command_key(target)

    def _pick(label: str, predicate) -> JobDurationEstimate | None:
        if max_samples <= 0:
            return None
        matches: list[tuple[str, float]] = []
        seen_ids: set[str] = set()
        for spec, duration in candidates:
            if not predicate(spec) or spec.id in seen_ids:
                continue
            matches.append((spec.id, duration))
            seen_ids.add(spec.id)
            if len(matches) == max_samples:
                break
        if not matches:
            return None
        values = [duration for _, duration in matches]
        evidence_ids = frozenset(jobid for jobid, _ in matches)
        return JobDurationEstimate(
            seconds=float(statistics.median(values)),
            samples=len(evidence_ids),
            source=label,
            evidence_ids=evidence_ids,
        )

    tiers = []
    if target.tags:
        tiers.extend(
            [
                (
                    "tag+command+cpus",
                    lambda spec: (
                        _shared_tags(target, spec)
                        and _command_key(spec) == command
                        and spec.cpus == target.cpus
                    ),
                ),
                (
                    "tag+cpus",
                    lambda spec: _shared_tags(target, spec) and spec.cpus == target.cpus,
                ),
                ("tag", lambda spec: _shared_tags(target, spec)),
            ]
        )
    tiers.extend(
        [
            (
                "command+cpus",
                lambda spec: _command_key(spec) == command and spec.cpus == target.cpus,
            ),
            ("command", lambda spec: _command_key(spec) == command),
            ("cpus", lambda spec: spec.cpus == target.cpus),
            ("all", lambda spec: True),
        ]
    )

    for label, predicate in tiers:
        estimate = _pick(label, predicate)
        if estimate is not None:
            return estimate
    return None


def estimate_pending_wait(
    target: JobSpec,
    pending: list[JobSpec],
    history: list[JobSpec],
) -> QueueEtaEstimate | None:
    """Estimate wait until ``target`` reaches its pending dispatch turn."""
    def _key(spec: JobSpec) -> tuple[int, str]:
        return (-spec.priority, spec.submitted_at)

    ahead = [
        spec
        for spec in pending
        if spec.id != target.id and _key(spec) < _key(target)
    ]
    if not ahead:
        return QueueEtaEstimate(
            seconds=0.0,
            jobs_ahead=0,
            source_counts={},
            sample_count=0,
        )

    total = 0.0
    source_counts: Counter[str] = Counter()
    evidence_ids: set[str] = set()
    source_evidence: defaultdict[str, set[str]] = defaultdict(set)
    for spec in ahead:
        estimate = estimate_job_duration(spec, history)
        if estimate is None:
            return None
        total += estimate.seconds
        source_counts[estimate.source] += 1
        evidence_ids.update(estimate.evidence_ids)
        source_evidence[estimate.source].update(estimate.evidence_ids)
    weak_sources = {
        source: len(source_ids)
        for source, source_ids in source_evidence.items()
        if source in _GENERIC_SOURCES
        and len(source_ids) < MIN_RELIABLE_GENERIC_SAMPLES
    }
    return QueueEtaEstimate(
        seconds=total,
        jobs_ahead=len(ahead),
        source_counts=dict(sorted(source_counts.items())),
        sample_count=len(evidence_ids),
        weak_sources=dict(sorted(weak_sources.items())),
    )


def format_eta_duration(seconds: float) -> str:
    """Compact human duration for status output."""
    seconds_i = max(0, int(round(seconds)))
    hours, rem = divmod(seconds_i, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}h{minutes:02d}m"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def format_eta_sources(source_counts: dict[str, int]) -> str:
    if not source_counts:
        return "no jobs ahead"
    return ", ".join(f"{name}={count}" for name, count in source_counts.items())
