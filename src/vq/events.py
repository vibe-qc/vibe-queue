"""Per-job event log: best-effort timeline of lifecycle events.

Lives at ``<workspace>/_vq/events.jsonl``. The JobSpec is the canonical
mutable state; the event log is append-only observability history.

Why both?
* The spec tells you "where the job is now."
* The event log tells you "how it got there, and at what time."

The ``vq events`` command, single-host and fleet web job views, and a limited
scheduler diagnostic in ``vq status`` read events. The single-host versioned
job-detail API returns the JobSpec-derived payload without an event tail.

Format: JSON Lines. One record per line. Records carry at minimum
``ts``, ``kind``, ``jobid``; everything else is kind-specific.

Best-effort writes: filesystem errors are logged and swallowed. The
event log is observability, not a transaction journal -- a missing
event must not break dispatch or kill paths. Some exceptional or unsafe
workspace paths intentionally cannot append an event.
"""
from __future__ import annotations

import json
import logging
from enum import StrEnum
from pathlib import Path
from typing import Any

from vq.spec import utcnow_iso

log = logging.getLogger(__name__)


class EventKind(StrEnum):
    """Canonical event names. Adding kinds is fine; renaming is not.

    Tooling reads events by ``kind`` string, so a rename breaks readers
    silently. If a name turns out wrong, add the new kind and emit both
    for one minor version, then deprecate.
    """

    SUBMITTED = "submitted"
    """Job written to the queue dir. Carries: command, cpus, mem_mb,
    wall_time_seconds, submitter, workspace_source."""

    DISPATCHED = "dispatched"
    """Daemon spawned the child process. Carries: pid, pgid, cpus,
    mem_mb (for cross-checking against budgets later)."""

    STATE_TRANSITION = "state_transition"
    """Any state change. Carries: from, to, reason (optional),
    exit_code (optional), evidence (optional dict for forensics)."""

    KILL_REQUESTED = "kill_requested"
    """User invoked vq kill. Carries: actor (e.g. submitter), via
    (cli/api/web)."""

    WATCHDOG_KILL = "watchdog_kill"
    """Watchdog escalated to SIGTERM or SIGKILL. Carries: signal,
    reason, target_state (the OOM_KILLED / TIME_EXCEEDED / STARVED
    that the spec is being moved to)."""


def append_event(
    workspace: Path,
    kind: EventKind,
    jobid: str,
    **data: Any,
) -> None:
    """Append one event record to ``<workspace>/_vq/events.jsonl``.

    Best-effort: any IOError is logged and swallowed. Callers must not
    rely on the event having been recorded.
    """
    record: dict[str, Any] = {
        "ts": utcnow_iso(),
        "kind": str(kind),
        "jobid": jobid,
        **data,
    }
    try:
        vq_dir = workspace / "_vq"
        vq_dir.mkdir(exist_ok=True)
        with (vq_dir / "events.jsonl").open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, sort_keys=True) + "\n")
    except OSError as e:
        log.warning(
            "failed to append event %s for %s: %s", kind, jobid, e
        )


def read_events(workspace: Path) -> list[dict[str, Any]]:
    """Return the parsed event log (all records, in order) or [] if absent.

    Corrupt / partially-written lines are skipped with a warning logged;
    callers see only well-formed records.
    """
    path = workspace / "_vq" / "events.jsonl"
    if not path.exists():
        return []
    out: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as f:
        for lineno, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError as e:
                log.warning(
                    "events.jsonl line %d at %s malformed: %s",
                    lineno, path, e,
                )
    return out


def state_transition(
    workspace: Path,
    jobid: str,
    *,
    from_state: str,
    to_state: str,
    reason: str | None = None,
    exit_code: int | None = None,
    **evidence: Any,
) -> None:
    """Convenience wrapper: append a STATE_TRANSITION event."""
    payload: dict[str, Any] = {"from": from_state, "to": to_state}
    if reason is not None:
        payload["reason"] = reason
    if exit_code is not None:
        payload["exit_code"] = exit_code
    if evidence:
        payload["evidence"] = evidence
    append_event(workspace, EventKind.STATE_TRANSITION, jobid, **payload)
