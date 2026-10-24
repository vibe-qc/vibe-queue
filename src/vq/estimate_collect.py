"""Assemble a held-out evaluation set from this host's finished jobs (#159).

A job submitted with ``vq submit --estimate`` keeps the estimate receipt it
was checked against (``JobSpec.resource_estimate``). Once it has finished,
its workspace holds the collector's ``resource-usage.json``. This pairs the
two for every such job in the local queue and writes a
``vq.estimate-evaluation-set/1`` that ``vq estimate-evaluate`` reads:

* ``estimates/<job>.json``: the receipt, as the job recorded it;
* ``usage/<job>.json``: the usage receipt, copied without following links
  and size-capped, or no entry when the job left none (a kill);
* ``set.json``: one case per job, with its outcome from the job state and
  the operator's hardware class. A limit is included only where it decides a
  censored case: the memory limit for an OOM kill, the wall limit for a
  time-limited run.

Jobs in any other terminal state, and scheduler arrays (whose elements leave
one receipt each), are listed as skipped with the reason. Nothing is
submitted or changed; the output directory is written whole or not at all.
"""

from __future__ import annotations

import json
import os
import shutil
import stat
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from vq.estimate_evaluation import SET_SCHEMA, EvaluationSet
from vq.resource_receipt import RESOURCE_USAGE_BASENAME
from vq.spec import JobSpec, JobState

MAX_USAGE_BYTES = 1024 * 1024

#: Job states that are an evaluable outcome, by the set's outcome name.
OUTCOMES = {
    JobState.COMPLETED: "completed",
    JobState.FAILED: "failed",
    JobState.OOM_KILLED: "oom_killed",
    JobState.TIME_EXCEEDED: "time_exceeded",
    JobState.KILLED: "killed",
}


class CollectError(ValueError):
    """No set can be assembled as asked."""


def _usage(workspace: Path) -> bytes | None:
    """The job's single usage receipt, read without following a link."""
    path = workspace / "_vq" / RESOURCE_USAGE_BASENAME
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        fd = os.open(path, flags)
    except FileNotFoundError:
        return None
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise CollectError(f"{path.name} is not a regular file")
        data = os.read(fd, MAX_USAGE_BYTES + 1)
    finally:
        os.close(fd)
    if len(data) > MAX_USAGE_BYTES:
        raise CollectError(f"{path.name} is larger than {MAX_USAGE_BYTES} bytes")
    return data


def _array_receipts(workspace: Path) -> bool:
    prefix = RESOURCE_USAGE_BASENAME + "."
    try:
        return any(name.startswith(prefix) for name in os.listdir(workspace / "_vq"))
    except OSError:
        return False


def collect(specs: Iterable[JobSpec], out_dir: Path, *, hardware_class: str,
            set_id: str) -> dict[str, Any]:
    """Write the set for every finished job in ``specs`` that carries an estimate."""
    if out_dir.exists() and (not out_dir.is_dir() or any(out_dir.iterdir())):
        raise CollectError(f"{out_dir} exists and is not an empty directory")
    cases: list[dict[str, Any]] = []
    skipped: list[dict[str, str]] = []
    files: dict[str, bytes] = {}
    for spec in sorted(specs, key=lambda s: s.id):
        if spec.resource_estimate is None or not spec.is_terminal:
            continue
        outcome = OUTCOMES.get(spec.state)
        if outcome is None:
            skipped.append({"job": spec.id,
                            "reason": f"state {spec.state.value} is not an evaluable outcome"})
            continue
        workspace = Path(spec.cwd)
        if _array_receipts(workspace):
            skipped.append({"job": spec.id, "reason": "a scheduler array's per-element "
                                                      "receipts are not collected"})
            continue
        try:
            usage = _usage(workspace)
        except (OSError, CollectError) as exc:
            skipped.append({"job": spec.id, "reason": f"usage receipt unreadable: {exc}"})
            continue
        case: dict[str, Any] = {"case_id": spec.id, "estimate": f"estimates/{spec.id}.json",
                                "outcome": outcome, "hardware_class": hardware_class}
        files[case["estimate"]] = (json.dumps(spec.resource_estimate, indent=2, sort_keys=True)
                                   + "\n").encode()
        if usage is not None:
            case["usage"] = f"usage/{spec.id}.json"
            files[case["usage"]] = usage
        if outcome == "oom_killed" and spec.mem_mb is not None:
            case["memory_limit_bytes"] = spec.mem_mb * 1024 * 1024
        if outcome == "time_exceeded" and spec.wall_time_seconds is not None:
            case["wall_limit_seconds"] = spec.wall_time_seconds
        cases.append(case)
    if not cases:
        raise CollectError("no finished job in this queue carries an estimate "
                           "(submit with `vq submit --estimate RECEIPT`)")
    document = {"schema_name": SET_SCHEMA, "set_id": set_id,
                "description": f"{len(cases)} finished jobs collected from this host's queue",
                "cases": cases}
    try:
        EvaluationSet.model_validate(document)
    except ValueError as exc:
        raise CollectError(f"the collected set does not validate: {exc}") from None
    files["set.json"] = (json.dumps(document, indent=2, sort_keys=True) + "\n").encode()
    staging = out_dir.with_name(f".{out_dir.name}.collect-{os.getpid()}")
    shutil.rmtree(staging, ignore_errors=True)
    try:
        for relative, data in files.items():
            target = staging / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        if out_dir.exists():
            out_dir.rmdir()
        os.rename(staging, out_dir)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return {"set": str(out_dir / "set.json"), "cases": len(cases), "skipped": skipped}
