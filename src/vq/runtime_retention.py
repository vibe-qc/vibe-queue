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
definition, the running daemon, a live process or an operator declaration
references, and the ``keep`` most recent other verified generations. A live
job whose generation cannot be proven keeps **every** generation; on Linux a
started job is proven by inspecting its whole process group (see
:func:`_process_census`). A process group reached through the ``current`` or
``previous`` pointer names no generation, so it too keeps every generation
(#114). An unreadable authority is a blocker, never an
empty reference set. Incomplete generations are left to the
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
PROC_ROOT = Path("/proc")
_PROC_TEXT_LIMIT = 16 * 1024 * 1024
_SLOT_SHA_ENV = re.compile(rb"(?:^|\x00)VQ_RUNTIME_SLOT_SHA=([0-9a-f]{40})(?=\x00|$)")


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


def _pointer_pattern(root: Path) -> re.Pattern[str]:
    """Matches ``<root>/current`` or ``<root>/previous`` as a whole path component."""
    names = "|".join(
        re.escape(str(root / name))
        for name in (runtime_slots.CURRENT_LINK, runtime_slots.PREVIOUS_LINK)
    )
    return re.compile(rf"(?:{names})(?![^/\s])")


def _read_limited(path: Path) -> bytes:
    with path.open("rb") as stream:
        data = stream.read(_PROC_TEXT_LIMIT + 1)
    if len(data) > _PROC_TEXT_LIMIT:
        raise PermissionError(f"{path} is too large to inspect")
    return data


def _process_references(
    base: Path, releases: Path, pointers: re.Pattern[str],
) -> tuple[set[str], bool]:
    """Generations one process uses: exe, cwd, argv, environment, maps, fds.

    Also whether its argv, cwd or an open fd names the ``current`` or
    ``previous`` pointer: whatever generation that resolved to at exec time
    is recorded nowhere (#114).

    The kernel reports exe, cwd, maps and fd targets fully resolved. That
    matches ``releases`` because :func:`vq.runtime_slots.layout` refuses every
    symlinked root component and returns the canonical path.

    Raises ``FileNotFoundError`` when the process exited meanwhile and
    ``PermissionError`` (or another ``OSError``) when any source could not be
    read, so the caller never mistakes a partial view for a complete one.
    """
    texts: list[str] = []
    pointer_texts: list[str] = []
    shas: set[str] = set()
    for link in ("exe", "cwd"):
        try:
            texts.append(os.readlink(base / link))
        except FileNotFoundError:
            # A kernel thread or zombie has no exe; the process must still exist.
            os.stat(base / "stat")
            continue
        if link == "cwd":
            pointer_texts.append(texts[-1])
    for name in ("cmdline", "maps"):
        texts.append(_read_limited(base / name).replace(b"\x00", b" ").decode("utf-8", "replace"))
        if name == "cmdline":
            pointer_texts.append(texts[-1])
    environ = _read_limited(base / "environ")
    shas.update(match.decode() for match in _SLOT_SHA_ENV.findall(environ))
    texts.append(environ.replace(b"\x00", b" ").decode("utf-8", "replace"))
    fd_dir = base / "fd"
    for entry in os.listdir(fd_dir):
        try:
            texts.append(os.readlink(fd_dir / entry))
        except FileNotFoundError:
            continue  # closed between listing and reading; it names nothing now
        pointer_texts.append(texts[-1])
    for text in texts:
        shas |= _shas_named(text, releases)
    return shas, any(pointers.search(text) for text in pointer_texts)


def _process_group(base: Path) -> int:
    stat = (base / "stat").read_bytes()
    fields = stat[stat.rindex(b")") + 2:].split()
    return int(fields[2])


def _census_uid() -> int:
    return os.geteuid()


def _process_census(releases: Path, root: Path) -> dict[str, Any]:
    """Generations live processes use, and which process groups were seen whole.

    Linux only: every process visible under ``/proc`` is inspected. One owned
    by this user is read completely; any other only by its command line,
    which is readable without privilege. A group is ``complete`` only when
    every member was read completely, which is what lets a started job with
    no recorded generation be proven (see :func:`_job_census`). A group any
    member of which names ``<root>/current`` or ``<root>/previous`` in its
    argv, cwd or an open fd is ``unresolved`` (#114): it is incomplete, and
    :func:`plan` keeps every generation while it lives. Without ``/proc``
    the census is unavailable and proves nothing.
    """
    census: dict[str, Any] = {
        "available": False, "referenced": {}, "groups": {}, "incomplete_groups": set(),
        "unresolved_groups": set(),
    }
    pointers = _pointer_pattern(root)
    if not (PROC_ROOT / "self" / "stat").is_file():
        return census
    census["available"] = True
    uid = _census_uid()
    referenced: dict[str, set[int]] = {}
    groups: dict[int, set[str]] = {}
    incomplete: set[int] = set()
    unresolved: set[int] = set()
    try:
        entries = os.listdir(PROC_ROOT)
    except OSError:
        census["available"] = False
        return census
    for entry in entries:
        if not entry.isdigit():
            continue
        pid = int(entry)
        base = PROC_ROOT / entry
        try:
            owned = os.stat(base).st_uid == uid
            group = _process_group(base)
        except (OSError, ValueError, IndexError):
            continue  # exited, or not a process entry
        try:
            if owned:
                shas, via_pointer = _process_references(base, releases, pointers)
            else:
                text = _read_limited(base / "cmdline").replace(b"\x00", b" ").decode(
                    "utf-8", "replace"
                )
                shas = _shas_named(text, releases)
                via_pointer = bool(pointers.search(text))
                incomplete.add(group)
        except FileNotFoundError:
            continue  # exited while being read; it holds nothing now
        except OSError:
            incomplete.add(group)
            groups.setdefault(group, set())
            continue
        groups.setdefault(group, set()).update(shas)
        if via_pointer:
            unresolved.add(group)
            incomplete.add(group)
        for sha in shas:
            referenced.setdefault(sha, set()).add(pid)
    census["referenced"] = {sha: sorted(pids) for sha, pids in referenced.items()}
    census["groups"] = groups
    census["incomplete_groups"] = incomplete
    census["unresolved_groups"] = unresolved
    return census


def _job_census(
    root: Path, releases: Path, processes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Generations live jobs are proven to use, and jobs that prove nothing."""
    try:
        specs = admin._runtime_slot_spec_snapshot()
    except runtime_slots.RuntimeSlotError as exc:
        raise RetentionError(str(exc)) from exc
    processes = processes or {"available": False}
    proven: dict[str, list[str]] = {}
    unresolved: list[str] = []
    process_proven: list[str] = []
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
            group = getattr(spec, "pgid", None)
            if (
                processes.get("available")
                and isinstance(group, int)
                and group in processes["groups"]
                and group not in processes["incomplete_groups"]
            ):
                # Its whole process group is alive and was read completely, so
                # what those processes reference is what the job holds. A job
                # whose group is gone or partly unreadable proves nothing: its
                # processes may have left the group, or be hidden from us.
                shas = set(processes["groups"][group])
                process_proven.append(spec.id)
            else:
                unresolved.append(spec.id)
        for sha in shas:
            proven.setdefault(sha, []).append(spec.id)
    return {"proven": {sha: sorted(ids) for sha, ids in proven.items()},
            "unresolved": sorted(unresolved),
            "process_proven": sorted(process_proven)}


def _service_files() -> tuple[list[Path], list[str]]:
    """Service definitions that may name a generation, and why any is unknown."""
    found: list[Path] = []
    blockers: list[str] = []
    from vq.web import install

    directories = {
        Path.home() / "Library/LaunchAgents",
        Path(os.environ.get("XDG_CONFIG_HOME", str(Path.home() / ".config"))) / "systemd/user",
        Path("/etc/systemd/system"),
    }
    candidates = [
        directory / name for directory in directories
        for name in ("vq-daemon.service", "vq-web.service",
                     f"{admin.LAUNCHD_DAEMON_LABEL}.plist", "com.vq.web.plist")
    ]
    for kind in (install.DAEMON_SERVICE, install.CONSOLE_SERVICE):
        marker = install._install_marker_path(kind)
        if marker.exists() or marker.is_symlink():
            candidates.append(marker)
            directories.add(marker.parent)
            try:
                payload = json.loads(marker.read_text())
                unit = payload.get("unit_path")
                if not isinstance(unit, str) or not Path(unit).is_absolute():
                    raise ValueError("missing absolute unit_path")
                candidates.append(Path(unit))
                directories.add(Path(unit).parent)
            except (OSError, ValueError, AttributeError) as exc:
                blockers.append(f"cannot read service provenance {marker}: {exc}")
    if shutil.which("systemctl") is not None:
        try:
            proc = subprocess.run(
                ["systemctl", "--user", "show", "vq-daemon", "vq-web", "--property=FragmentPath",
                 "--property=DropInPaths", "--no-pager"],
                capture_output=True, text=True, timeout=30, stdin=subprocess.DEVNULL,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            blockers.append(f"systemd user unit authority is unavailable: {exc}")
        else:
            # A missing web unit may make the combined query nonzero while
            # still returning the daemon's real fragment and drop-ins.
            for line in proc.stdout.splitlines():
                key, _sep, value = line.partition("=")
                if key in {"FragmentPath", "DropInPaths"}:
                    found.extend(Path(item) for item in value.split() if item)
    candidates.extend(found)
    directories.update(path.parent for path in found)
    for candidate in candidates:
        if candidate.exists() or candidate.is_symlink():
            found.append(candidate)
    # Recovery retains these exact bytes beside the bound files. They remain
    # consumers until the operator retires the corresponding recovery route.
    for directory in directories:
        try:
            entries = list(directory.iterdir())
        except FileNotFoundError:
            continue
        except OSError as exc:
            blockers.append(f"cannot inspect service directory {directory}: {exc}")
            continue
        found.extend(path for path in entries if
                     (path.name.startswith(".vq-daemon-") and
                      path.name.endswith(("-rollback", "-forward"))) or
                     any(path.name.startswith(f".{candidate.name}.vq-install-backup-")
                         for candidate in candidates if candidate.parent == directory))
        for unit in candidates:
            if unit.parent != directory or unit.suffix != ".service":
                continue
            drop_ins = directory / f"{unit.name}.d"
            try:
                found.extend(p for p in drop_ins.iterdir() if p.suffix == ".conf")
            except FileNotFoundError:
                continue
            except OSError as exc:
                blockers.append(f"cannot inspect service drop-ins {drop_ins}: {exc}")
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
    processes = _process_census(slot_layout.releases, root)
    jobs = _job_census(root, slot_layout.releases, processes)
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
        if sha in processes["referenced"]:
            # No pids here: they churn between review and apply, and the plan
            # id must change only when the outcome would.
            reasons.append("live process")
        if sha in declared:
            reasons.append("declared by operator")
        if state != "verified":
            reasons.append("incomplete; left to slot build recovery")
        if jobs["unresolved"] and not reasons:
            reasons.append(
                f"{len(jobs['unresolved'])} live job(s) have no provable generation"
            )
        if processes["unresolved_groups"] and not reasons:
            # No count: pgids churn, and the plan id must change only when
            # the outcome would.
            reasons.append("a live process group runs through current or previous")
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
                   "process_proven_jobs": jobs["process_proven"],
                   "process_scan": processes["available"],
                   "service_files": services["files"]},
        "blockers": blockers,
    }
    plan_id = _digest(body)
    # Informational only, outside the digest for the same reason as the sizes.
    body["census"]["process_references"] = processes["referenced"]
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


# ---------------------------------------------------------------------------
# Opt-in scheduled retention (#74), run by the auto-cleanup sweep
# ---------------------------------------------------------------------------

def schedule_path() -> Path:
    """Scheduled programs and their ``keep``, beside the receipts.

    Kept out of the auto-cleanup policy file on purpose: that model forbids
    unknown keys, so an older vq reading a policy with a new key would treat
    auto-cleanup as disabled altogether.
    """
    return paths.state_root() / "runtime-retention" / "schedule.json"


def read_schedule() -> dict[str, int]:
    try:
        data = json.loads(schedule_path().read_text())
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as exc:
        raise RetentionError(f"unreadable retention schedule: {exc}") from exc
    if not isinstance(data, dict) or not all(
        isinstance(k, str) and isinstance(v, int) and not isinstance(v, bool) and v >= 0
        for k, v in data.items()
    ):
        raise RetentionError("malformed retention schedule")
    return data


def write_schedule(schedule: dict[str, int]) -> None:
    path = schedule_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(schedule, indent=2, sort_keys=True) + "\n")
    tmp.replace(path)


def run_scheduled(cfg: config.Config) -> dict[str, Any]:
    """Plan and apply retention for every scheduled program; never raises.

    Each program is planned and, when it has candidates and no blockers,
    applied with that plan's own id, so the usual revalidation under admin
    and lifecycle ownership applies. A held ownership (an update in flight)
    or any other refusal skips that program until the next sweep.
    """
    outcome: dict[str, Any] = {}
    try:
        schedule = read_schedule()
    except RetentionError as exc:
        return {"error": str(exc)}
    for env, keep in sorted(schedule.items()):
        try:
            planned = plan(env, cfg, keep=keep)
            if planned["blockers"]:
                outcome[env] = {"skipped": planned["blockers"]}
                continue
            if not planned["candidates"]:
                outcome[env] = {"removed": []}
                continue
            applied = apply(env, cfg, planned["plan_id"], keep=keep)
            outcome[env] = {"removed": applied["removed"], "failed": applied["failed"],
                            "receipt": applied["receipt"]}
        except Exception as exc:  # noqa: BLE001 - one program must not stop the rest
            outcome[env] = {"skipped": [str(exc)]}
    return outcome
