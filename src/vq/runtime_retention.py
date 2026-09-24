"""Reviewed retention for a program's runtime generations (#74).

A slot update builds ``<root>/releases/<sha>`` and flips ``current``; nothing
ever removes an older generation. This module plans and applies that removal
as an explicit operator transaction, separate from any update or activation:

* :func:`plan` is read-only. It lists every generation with its size and why it
  is kept or proposed, the liveness census behind that, any blockers, and a
  digest over everything that decides the outcome.
* :func:`apply` takes the reviewed digest, recomputes the plan under the admin
  and lifecycle ownership (which includes the runtime-root lock), refuses when
  anything changed, revalidates each candidate immediately before removing it,
  and writes a receipt of exactly what was removed.

Always kept: ``current``, ``previous``, every generation a live job, a service
definition, the running daemon or an operator declaration references, and the
``keep`` most recent other verified generations. A live job whose generation
cannot be proven keeps **every** generation. An unreadable authority is a
blocker, never an empty reference set. Incomplete generations are left to the
slot build's own recovery. Scheduler-host packed publications are out of scope.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from vq import admin, config, paths, runtime_slots
from vq.spec import JobState

SCHEMA = "vq.runtime-retention/1"
DEFAULT_KEEP = 2
_SHA40 = re.compile(r"[0-9a-f]{40}")


class RetentionError(ValueError):
    """Retention cannot proceed without risking a generation still in use."""


def _digest(value: object) -> str:
    return hashlib.sha256(
        (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
    ).hexdigest()


def _tree_bytes(path: Path) -> int:
    total = 0
    for directory, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += os.lstat(os.path.join(directory, name)).st_size
            except OSError:
                continue
    return total


def _shas_named(text: str, releases: Path) -> set[str]:
    prefix = re.escape(f"{releases}{os.sep}")
    return set(re.findall(prefix + r"([0-9a-f]{40})(?![0-9a-f])", text))


def _job_census(root: Path, releases: Path) -> dict[str, Any]:
    """Generations live jobs are proven to use, and jobs that prove nothing."""
    try:
        specs = admin._runtime_slot_spec_snapshot()
    except runtime_slots.RuntimeSlotError as exc:
        raise RetentionError(str(exc)) from exc
    proven: dict[str, list[str]] = {}
    unresolved: list[str] = []
    for spec in specs:
        if getattr(spec, "is_terminal", False):
            continue
        shas: set[str] = set()
        pin = getattr(spec, "program_runtime_pin", None)
        if pin is not None and pin.resolved_runtime_root == str(root):
            shas |= _shas_named(pin.resolved_runtime_source or "", releases)
        for argument in getattr(spec, "command", None) or ():
            if isinstance(argument, str):
                shas |= _shas_named(argument, releases)
        not_started = getattr(spec, "state", None) == JobState.PENDING
        elsewhere = getattr(spec, "scheduler_target", None) is not None
        if not shas and not (not_started or elsewhere):
            # A started local job may reach a stable launcher that resolved
            # `current` at exec time; after a later flip nothing records which
            # generation it holds (runtime_slots.slots_in_use). A job that has
            # not started resolves at dispatch, and a scheduler job runs on
            # its target host, so neither can hold a generation of this store.
            unresolved.append(spec.id)
        for sha in shas:
            proven.setdefault(sha, []).append(spec.id)
    return {"proven": {sha: sorted(ids) for sha, ids in proven.items()},
            "unresolved": sorted(unresolved)}


def _service_files() -> tuple[list[Path], list[str]]:
    """Service definitions that may name a generation, and why any is unknown."""
    found: list[Path] = []
    blockers: list[str] = []
    launchd = Path.home() / "Library" / "LaunchAgents" / f"{admin.LAUNCHD_DAEMON_LABEL}.plist"
    if launchd.is_file():
        found.append(launchd)
    for system_unit in (Path("/etc/systemd/system/vq-daemon.service"),):
        if system_unit.is_file():
            found.append(system_unit)
    if shutil.which("systemctl") is not None:
        try:
            proc = subprocess.run(
                ["systemctl", "--user", "show", "vq-daemon", "--property=FragmentPath",
                 "--property=DropInPaths", "--no-pager"],
                capture_output=True, text=True, timeout=30, stdin=subprocess.DEVNULL,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            blockers.append(f"systemd user unit authority is unavailable: {exc}")
        else:
            if proc.returncode == 0:
                for line in proc.stdout.splitlines():
                    key, _sep, value = line.partition("=")
                    if key in {"FragmentPath", "DropInPaths"}:
                        found.extend(Path(item) for item in value.split() if item)
    return sorted(set(found)), blockers


def _service_census(releases: Path) -> dict[str, Any]:
    files, blockers = _service_files()
    referenced: dict[str, list[str]] = {}
    for path in files:
        try:
            text = path.read_bytes().decode("utf-8", "replace")
        except OSError as exc:
            blockers.append(f"cannot read service definition {path}: {exc}")
            continue
        for sha in _shas_named(text, releases):
            referenced.setdefault(sha, []).append(str(path))
    daemon: str | None = None
    try:
        from vq import rpc

        identity = rpc.call("get_process_identity", timeout=5.0)
    except Exception:  # noqa: BLE001 - no reachable daemon names nothing
        identity = None
    if isinstance(identity, dict):
        daemon = str(identity.get("python_executable") or "")
        for sha in _shas_named(daemon, releases):
            referenced.setdefault(sha, []).append("running vq daemon")
    return {"referenced": {sha: sorted(set(v)) for sha, v in referenced.items()},
            "files": [str(p) for p in files], "blockers": blockers}


def _generation_state(root: Path, sha: str) -> str:
    try:
        return "verified" if runtime_slots._verified_markers_match(root, sha) else "incomplete"
    except (runtime_slots.RuntimeSlotError, OSError, ValueError):
        return "incomplete"


def plan(
    env: str,
    cfg: config.Config,
    *,
    keep: int = DEFAULT_KEEP,
    retain: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Read-only retention plan for ``env``'s runtime store."""
    if keep < 0:
        raise RetentionError("--keep must not be negative")
    prog = cfg.programs.get(env)
    if not isinstance(prog, config.VenvProgram) or prog.runtime_slot_root is None:
        raise RetentionError(
            f"{env!r} is not a registered venv program with runtime_slot_root"
        )
    declared = sorted({sha.strip().lower() for sha in retain})
    for sha in declared:
        if not _SHA40.fullmatch(sha):
            raise RetentionError(f"--retain needs full 40-hex SHAs, not {sha!r}")
    try:
        slot_layout = runtime_slots.layout(prog.runtime_slot_root)
        root = slot_layout.root
        current = runtime_slots.resolve_current(root)
        previous = runtime_slots._previous_sha(root)
        if slot_layout.releases.exists():
            runtime_slots._assert_releases_root(slot_layout)
        shas = runtime_slots.list_slots(root)
    except runtime_slots.RuntimeSlotError as exc:
        raise RetentionError(str(exc)) from exc
    jobs = _job_census(root, slot_layout.releases)
    services = _service_census(slot_layout.releases)
    blockers = list(services["blockers"])
    if admin.admin_update_marker_exists():
        blockers.append("an admin update or recovery marker must be resolved first")

    generations: list[dict[str, Any]] = []
    extra_kept = 0
    for sha in shas:  # newest-modified first
        reasons: list[str] = []
        state = _generation_state(root, sha)
        if sha == current:
            reasons.append("current")
        if sha == previous:
            reasons.append("previous")
        if sha in jobs["proven"]:
            reasons.append("live job " + ", ".join(jobs["proven"][sha]))
        if sha in services["referenced"]:
            reasons.append("service " + ", ".join(services["referenced"][sha]))
        if sha in declared:
            reasons.append("declared by operator")
        if state != "verified":
            reasons.append("incomplete; left to slot build recovery")
        if jobs["unresolved"] and not reasons:
            reasons.append(
                f"{len(jobs['unresolved'])} live job(s) have no provable generation"
            )
        if not reasons and extra_kept < keep:
            extra_kept += 1
            reasons.append(f"within --keep {keep}")
        generations.append({"sha": sha, "state": state, "retain": bool(reasons),
                            "reasons": reasons})
    body = {
        "schema": SCHEMA, "env": env, "root": str(root), "current": current,
        "previous": previous, "keep": keep, "declared": declared,
        "generations": generations,
        "census": {"unresolved_live_jobs": jobs["unresolved"],
                   "service_files": services["files"]},
        "blockers": blockers,
    }
    plan_id = _digest(body)
    for generation in generations:
        generation["bytes"] = _tree_bytes(slot_layout.releases / generation["sha"])
    candidates = [g for g in generations if not g["retain"]]
    return {**body, "plan_id": plan_id,
            "reclaimable_bytes": sum(g["bytes"] for g in candidates),
            "candidates": [g["sha"] for g in candidates]}


def apply(
    env: str,
    cfg: config.Config,
    expected_plan_id: str,
    *,
    keep: int = DEFAULT_KEEP,
    retain: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Remove exactly the reviewed plan's candidates, or refuse."""
    prog = cfg.programs.get(env)
    if not isinstance(prog, config.VenvProgram):
        raise RetentionError(f"{env!r} is not a registered venv program")
    with admin.admin_update_ownership(), admin.toolset_lifecycle_lock(
        [prog], action="vq-runtime-retention",
    ):
        fresh = plan(env, cfg, keep=keep, retain=retain)
        if fresh["plan_id"] != expected_plan_id:
            raise RetentionError(
                "the store, its references or the census changed since review; "
                "plan again"
            )
        if fresh["blockers"]:
            raise RetentionError("plan has blockers: " + "; ".join(fresh["blockers"]))
        slot_layout = runtime_slots.layout(fresh["root"])
        removed: list[str] = []
        failed: dict[str, str] = {}
        for sha in fresh["candidates"]:
            try:
                runtime_slots._assert_releases_root(slot_layout)
                if sha in {runtime_slots.resolve_current(slot_layout.root),
                           runtime_slots._previous_sha(slot_layout.root)}:
                    raise RetentionError("became current or previous")
                shutil.rmtree(slot_layout.releases / sha)
                runtime_slots._fsync_directory(slot_layout.releases)
            except (OSError, RetentionError, runtime_slots.RuntimeSlotError) as exc:
                failed[sha] = str(exc)
                continue
            removed.append(sha)
        receipt = {"schema": SCHEMA, "env": env, "root": fresh["root"],
                   "plan_id": expected_plan_id, "applied_at": datetime.now(UTC).isoformat(),
                   "removed": removed, "failed": failed,
                   "retained": [g["sha"] for g in fresh["generations"] if g["retain"]]}
        receipt_dir = paths.state_root() / "runtime-retention"
        receipt_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        path = receipt_dir / f"{stamp}-{re.sub(r'[^A-Za-z0-9_.-]', '_', env)}.json"
        path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
        return {**receipt, "receipt": str(path)}
