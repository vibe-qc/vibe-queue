"""A finished job's measured resource usage, read from its terminal receipt (#168).

Two collectors leave a ``resource-usage.json`` receipt in the job's
``_vq/`` directory when its command exits: the direct-job collector
(``vq.direct-resource-usage.v1``, POSIX ``wait4`` rusage) and the scheduler
job's GNU time wrapper (``vq.scheduler-resource-usage.v1``), which a
scheduler array writes once per element as ``resource-usage.json.<index>``.
This module reads whichever is there and reports one normalized summary:
measured wall time, CPU time and peak RSS, never a declared value standing
in for a missing measurement.

The receipt sits in a workspace the job's own user can write, so it is
read without following links, size-capped, and every value is checked; a
receipt that fails any check is reported as ``invalid`` with the reason
rather than shown.
"""

from __future__ import annotations

import json
import math
import os
import stat
from pathlib import Path
from typing import Any

from vq.resource_receipt import RESOURCE_USAGE_BASENAME
from vq.resource_receipt import RESOURCE_USAGE_SCHEMA as DIRECT_SCHEMA

SCHEDULER_SCHEMA = "vq.scheduler-resource-usage.v1"
MAX_RECEIPT_BYTES = 64 * 1024
MAX_ARRAY_RECEIPTS = 10_000

#: What each collector's peak RSS means. Both report the largest single
#: process's resident set, not a sum over a job's concurrent processes.
PEAK_RSS_SCOPE = {
    "posix-wait4": "largest single process (ru_maxrss)",
    "gnu-time": "largest single process (GNU time maximum resident set)",
}


class _Invalid(ValueError):
    pass


def _read(path: Path) -> dict[str, Any]:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise _Invalid(f"{path.name} cannot be opened: {exc.strerror or exc}") from None
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise _Invalid(f"{path.name} is not a regular file")
        data = os.read(fd, MAX_RECEIPT_BYTES + 1)
    finally:
        os.close(fd)
    if len(data) > MAX_RECEIPT_BYTES:
        raise _Invalid(f"{path.name} is larger than {MAX_RECEIPT_BYTES} bytes")
    try:
        payload = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise _Invalid(f"{path.name} is not JSON") from None
    if not isinstance(payload, dict) or payload.get("schema") not in {
            DIRECT_SCHEMA, SCHEDULER_SCHEMA}:
        raise _Invalid(f"{path.name} is not a vq resource-usage receipt")
    return payload


def _number(payload: dict[str, Any], key: str, name: str) -> float | None:
    value = payload.get(key)
    if value is None:
        return None
    if (isinstance(value, bool) or not isinstance(value, (int, float))
            or not math.isfinite(value) or value < 0):
        raise _Invalid(f"{name}: {key} is not a non-negative number")
    return float(value)


def _integer(payload: dict[str, Any], key: str, name: str) -> int | None:
    value = payload.get(key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise _Invalid(f"{name}: {key} is not an integer")
    return value


def _one(payload: dict[str, Any], name: str) -> dict[str, Any]:
    collector = payload.get("collector")
    if collector not in PEAK_RSS_SCOPE:
        raise _Invalid(f"{name}: unknown collector {collector!r}")
    if payload.get("status") == "error":
        error = payload.get("error")
        return {"status": "error", "collector": collector,
                "error": error if isinstance(error, str) else "the collector failed"}
    if payload.get("status") != "ok":
        raise _Invalid(f"{name}: status is neither ok nor error")
    count = _integer(payload, "process_count", name)
    if count is not None and count < 0:
        raise _Invalid(f"{name}: process_count is negative")
    return {
        "status": "ok",
        "collector": collector,
        "wall_seconds": _number(payload, "wall_seconds", name),
        "active_cpu_seconds": _number(payload, "active_cpu_seconds", name),
        "peak_rss_mb": _number(payload, "peak_rss_mb", name),
        "process_count": count,
        "command_exit_code": _integer(payload, "command_exit_code", name),
    }


def _receipt_paths(directory: Path) -> list[Path]:
    """The job's receipt, or a scheduler array's element receipts in index order."""
    single = directory / RESOURCE_USAGE_BASENAME
    if os.path.lexists(single):
        return [single]
    try:
        names = os.listdir(directory)
    except OSError:
        return []
    prefix = RESOURCE_USAGE_BASENAME + "."
    indexed = sorted(
        (int(name[len(prefix):]), name) for name in names
        if name.startswith(prefix) and name[len(prefix):].isdigit()
    )
    return [directory / name for _, name in indexed[:MAX_ARRAY_RECEIPTS + 1]]


def read_final_usage(workspace: Path) -> dict[str, Any] | None:
    """The measured usage a finished job's receipt records, or None without one.

    ``status`` is ``ok`` with the numbers, ``error`` when the collector ran
    but could not measure (its reason in ``error``), or ``invalid`` when the
    receipt cannot be trusted (the reason in ``error``). Over a scheduler
    array's elements, CPU time is summed, and wall time and peak RSS are
    the largest element's; ``receipts`` says how many were read.
    """
    paths = _receipt_paths(workspace / "_vq")
    if not paths:
        return None
    try:
        if len(paths) > MAX_ARRAY_RECEIPTS:
            raise _Invalid(f"more than {MAX_ARRAY_RECEIPTS} array receipts")
        parts = [_one(_read(path), path.name) for path in paths]
    except _Invalid as exc:
        return {"status": "invalid", "error": str(exc), "receipts": len(paths)}
    failed = [part for part in parts if part["status"] == "error"]
    if failed:
        return {"status": "error", "collector": failed[0]["collector"],
                "error": failed[0]["error"], "receipts": len(parts)}
    collector = parts[0]["collector"]
    if any(part["collector"] != collector for part in parts):
        return {"status": "invalid", "error": "array receipts disagree on their collector",
                "receipts": len(parts)}

    def values(key: str) -> list[Any]:
        return [part[key] for part in parts if part[key] is not None]

    walls, cpus, peaks = values("wall_seconds"), values("active_cpu_seconds"), \
        values("peak_rss_mb")
    complete = len(walls) == len(cpus) == len(parts)
    wall_sum = math.fsum(walls)
    counts = values("process_count")
    codes = values("command_exit_code")
    return {
        "status": "ok",
        "collector": collector,
        "receipts": len(parts),
        "wall_seconds": max(walls) if walls else None,
        "active_cpu_seconds": round(math.fsum(cpus), 6) if cpus else None,
        # Mean busy cores over the run: CPU time over wall time, per element.
        "average_cores": (round(math.fsum(cpus) / wall_sum, 3)
                          if complete and wall_sum > 0 else None),
        "peak_rss_mb": max(peaks) if peaks else None,
        "peak_rss_scope": PEAK_RSS_SCOPE[collector],
        "process_count": max(counts) if counts else None,
        "command_exit_code": codes[0] if len(parts) == 1 and codes else None,
    }


def format_final_usage(usage: dict[str, Any] | None) -> str | None:
    """One line for `vq status`, or None when there is no receipt."""
    if usage is None:
        return None
    if usage["status"] == "invalid":
        return f"not shown: {usage['error']}"
    if usage["status"] == "error":
        return f"not measured: {usage['error']}"

    def clock(seconds: float | None) -> str:
        if seconds is None:
            return "unknown"
        total = int(seconds)
        return f"{total // 3600}:{total % 3600 // 60:02d}:{total % 60:02d}"

    parts = [f"wall {clock(usage['wall_seconds'])}",
             f"cpu {clock(usage['active_cpu_seconds'])}"
             + (f" ({usage['average_cores']} cores avg)"
                if usage["average_cores"] is not None else "")]
    peak = usage["peak_rss_mb"]
    parts.append("peak rss " + (f"{peak:.0f} MB" if peak is not None else "unknown")
                 + f" ({usage['peak_rss_scope']})")
    if usage["receipts"] > 1:
        parts.append(f"over {usage['receipts']} array elements")
    return " · ".join(parts)
