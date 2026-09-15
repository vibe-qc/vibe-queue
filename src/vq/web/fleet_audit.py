"""Append-only audit trail for fleet-console actions (M2).

Every authenticated write action appends one JSON line — who, what,
which job, on which host, and how it went. Same jsonl discipline as
``rpc-audit.jsonl``; this is the seed of the compliance surface in the
design doc's M6 (tamper-evident export comes later, the append-only
record starts now).
"""
from __future__ import annotations

import json
import threading
from datetime import UTC, datetime
from pathlib import Path

from vq import paths

_LOCK = threading.Lock()


def audit_path() -> Path:
    return paths.state_root() / "fleet-audit.jsonl"


def append_audit(
    *,
    user: str | None,
    role: str | None,
    action: str,
    jobid: str | None,
    host: str | None,
    outcome: str,
    request_id: str | None = None,
) -> None:
    """Append one audit record, raising OSError if it cannot be written.

    Callers distinguish refusal before an action from a missing outcome
    after an action; an append failure cannot roll back the operation.
    """
    record = {
        "ts": datetime.now(UTC).isoformat(),
        "user": user,
        "role": role,
        "action": action,
        "jobid": jobid,
        "host": host,
        "outcome": outcome,
    }
    if request_id is not None:
        record["request_id"] = request_id
    line = json.dumps(record, sort_keys=True)
    path = audit_path()
    with _LOCK:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")


def read_audit(limit: int = 200) -> list[dict]:
    """The most recent ``limit`` records, newest first. Malformed lines
    are skipped."""
    path = audit_path()
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return []
    records: list[dict] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if isinstance(rec, dict):
            records.append(rec)
    records.reverse()
    return records[:limit]
