"""v0.8.6 *Codd's Audit* — RPC audit-trail for mutating calls.

Every ``set_*`` RPC call appended to ``rpc-audit.jsonl`` so multi-user
deployments can answer forensic questions like "who drained the
queue at 03:14?" or "when was the last admin-status update for
vibeqc-release?".

## What gets logged

Only **mutating** methods (anything starting with ``set_``). Reads
(``ping``, ``get_methods``, ``get_admin_status``, ``get_drain_state``,
``get_throttle_state``) are NOT logged — they're high-volume,
not sensitive, and would drown out the signal.

## File location

* single-user → ``<state_root>/rpc-audit.jsonl``
* multi-user → ``<multi_user_root>/rpc-audit.jsonl``

(Same shape as the rest of the daemon's state files.)

## Line schema

One JSON object per line. Stable fields:

* ``ts`` — ISO 8601 UTC timestamp.
* ``method`` — the RPC method name (e.g. ``"set_drain_state"``).
* ``uid`` — caller's Unix uid via ``SO_PEERCRED``, or ``null`` if
  the kernel doesn't support it (macOS dev boxes) or the lookup
  failed.
* ``ok`` — ``true`` if the handler succeeded, ``false`` otherwise.
* ``args_summary`` — a short rendered summary of the args (env
  name, set-vs-clear, weight value, etc.). Tokens + full state
  dicts are NOT included.
* ``error`` — present only when ``ok=false``; the error message.

## Rotation

None in v0.8.6 — append-only. Each entry is ~200 bytes; even at
1000 writes/day the file grows ~70KB/year. If a future deployment
genuinely needs rotation, v0.5.16's pattern (cron-driven archive
sweeps) applies.

## Why a separate module

Could live inside ``vq/rpc.py``, but factoring out keeps the RPC
module focused on the protocol surface and lets monitoring scripts
``from vq.audit import read_audit_log`` without pulling in the
socket layer.
"""

from __future__ import annotations

import contextlib
import json
import logging
import socket
import struct
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from vq import paths

log = logging.getLogger(__name__)

AUDIT_FILENAME = "rpc-audit.jsonl"

# struct ucred = {pid_t pid; uid_t uid; gid_t gid;} on Linux —
# three 4-byte ints.
_UCRED_STRUCT = "3i"
_UCRED_SIZE = struct.calcsize(_UCRED_STRUCT)


def audit_log_path(*, multi_user: bool = False) -> Path:
    """Resolve the audit log path. Mirrors the other state files'
    location semantics so the audit trail lives next to the data
    it's auditing."""
    if multi_user:
        return paths.multi_user_root() / AUDIT_FILENAME
    return paths.state_root() / AUDIT_FILENAME


def peer_uid(conn: socket.socket) -> int | None:
    """Best-effort peer-uid lookup via ``SO_PEERCRED``.

    Returns the caller's Unix uid on Linux, ``None`` on platforms
    without the option (macOS dev boxes), or ``None`` on any
    other error. Audit lines with ``uid=null`` still capture the
    method + args + outcome — the absence of a uid just means
    "couldn't tell who called". Better than refusing to audit.
    """
    if not hasattr(socket, "SO_PEERCRED"):
        return None
    try:
        raw = conn.getsockopt(
            socket.SOL_SOCKET, socket.SO_PEERCRED, _UCRED_SIZE,
        )
        _pid, uid, _gid = struct.unpack(_UCRED_STRUCT, raw)
        return int(uid)
    except (OSError, struct.error):
        return None


def summarise_args(method: str, args: dict[str, Any]) -> str:
    """Render args into a short human string for the audit line.
    Never includes tokens or full state dicts — just the
    operator-meaningful bits.

    Method-specific shortcuts keep the audit line readable:
    * ``set_admin_status`` → ``env=vibeqc-dev``
    * ``set_drain_state(state=None)`` → ``clear``
    * ``set_drain_state(state={...})`` → ``set max_jobs=X``
    * ``set_throttle_state(state=None)`` → ``clear``
    * ``set_throttle_state(state={weight: N})`` → ``set weight=N``
    * ``set_scheduler_drain_lease`` → owner/host/lease-scoped action
    * ``set_legacy_scheduler_drain_release`` → legacy host release
    """
    if method == "set_admin_status":
        env = args.get("env")
        return f"env={env}" if env else "(no env)"
    if method == "set_scheduler_drain_lease":
        lease = args.get("lease")
        if isinstance(lease, dict):
            return (
                f"acquire host={lease.get('scheduler_host')} "
                f"owner={lease.get('owner')} lease_id={lease.get('lease_id')}"
            )
        release_id = args.get("release_id")
        if release_id is not None:
            return f"release lease_id={release_id}"
        release_host = args.get("release_host")
        if release_host is not None:
            owner = args.get("release_owner")
            suffix = f" owner={owner}" if owner is not None else " all-owners"
            return f"release host={release_host}{suffix}"
        if args.get("release_all") is True:
            return "release all scheduler leases"
        return "invalid scheduler lease mutation"
    if method == "set_legacy_scheduler_drain_release":
        host = args.get("host")
        return f"release legacy host={host}" if host else "(no host)"
    if method in ("set_drain_state", "set_throttle_state"):
        state = args.get("state")
        if state is None:
            return "clear"
        if not isinstance(state, dict):
            return "set (?)"
        if method == "set_drain_state":
            parts = []
            if state.get("enabled") is False:
                parts.append("disabled")
            for key in ("max_jobs", "max_cpus", "duration_seconds"):
                val = state.get(key)
                if val is not None:
                    parts.append(f"{key}={val}")
            if not parts:
                parts.append("full")
            return "set " + " ".join(parts)
        # throttle
        parts = []
        for key in ("weight", "duration_seconds"):
            val = state.get(key)
            if val is not None:
                parts.append(f"{key}={val}")
        return "set " + (" ".join(parts) if parts else "(?)")
    # Unknown set_* method (future ship) — emit args.keys() so
    # future-self can decode without losing forensic value.
    keys = sorted(k for k in args if k != "token")
    return "args=" + ",".join(keys) if keys else "(no args)"


def append_audit_line(
    *,
    method: str,
    uid: int | None,
    ok: bool,
    args_summary: str,
    error: str | None = None,
    multi_user: bool = False,
) -> None:
    """Append one audit line. Best-effort: a write failure is
    logged at WARNING but doesn't propagate. The audit log is a
    forensic aid, not a correctness gate — the verb still
    completed/failed regardless of whether the line landed.
    """
    line = {
        "ts": datetime.now(UTC).isoformat(),
        "method": method,
        "uid": uid,
        "ok": ok,
        "args_summary": args_summary,
    }
    if error is not None:
        line["error"] = error
    path = audit_log_path(multi_user=multi_user)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        # Open in append mode so concurrent writes don't truncate.
        # JSON-Lines: one object per line, newline-terminated.
        with path.open("a") as f:
            f.write(json.dumps(line) + "\n")
    except OSError as e:
        log.warning("audit: failed to append to %s: %s", path, e)


def read_audit_log(*, multi_user: bool = False) -> list[dict[str, Any]]:
    """Read every audit line. Tolerates a missing file (returns []),
    skips malformed lines silently. Intended for tests + monitoring
    scripts that want to walk the trail."""
    path = audit_log_path(multi_user=multi_user)
    if not path.exists():
        return []
    out: list[dict[str, Any]] = []
    try:
        with path.open() as f:
            for raw in f:
                raw = raw.strip()
                if not raw:
                    continue
                with contextlib.suppress(json.JSONDecodeError):
                    out.append(json.loads(raw))
    except OSError:
        return []
    return out
