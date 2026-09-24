"""Local runtime binding transactions with reviewed bytes and durable recovery.

This is deliberately separate from runtime_slots: that module owns generations;
this one owns external registry/launcher files. It never deletes a generation or
moves a legacy checkout, and cannot coordinate remote authorities or services.
"""
from __future__ import annotations

import base64
import contextlib
import copy
import fcntl
import hashlib
import json
import os
import secrets
import shlex
import stat
import subprocess
import tomllib
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from vq import admin, config, paths, runtime_slots

SCHEMA = "vq.runtime-migration/1"
_LIMIT = 16 * 1024 * 1024
_DIR = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
_FILE = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK


class MigrationError(ValueError):
    """Migration cannot preserve the reviewed binding contract."""


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Registry(_Strict):
    path: str
    candidate: str


class Launcher(_Strict):
    path: str
    bootstrap: str
    arguments: list[str] = Field(default_factory=list)


class Manifest(_Strict):
    schema_version: Literal["vq.runtime-migration/1"] = SCHEMA
    component: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    program: str
    source_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    runtime_root: str
    registries: list[Registry] = Field(min_length=1)
    launchers: list[Launcher] = Field(default_factory=list)
    # Explicit discovery declaration. Entries cannot be silently treated as local.
    external_consumers: list[str]


def _json(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _path(value: str | Path) -> Path:
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts or str(path) == "/":
        raise MigrationError("explicit absolute paths without '..' are required")
    paths.require_test_path_within_sandbox(path, "runtime migration")
    return path


def _owned(info: os.stat_result) -> None:
    if info.st_uid != os.geteuid() or info.st_mode & 0o022:
        raise MigrationError("migration requires owner-controlled files and directories")


@contextlib.contextmanager
def _directory(path: Path) -> Iterator[int]:
    fd = os.open("/", _DIR)
    try:
        for part in path.parts[1:]:
            child = os.open(part, _DIR, dir_fd=fd)
            os.close(fd)
            fd = child
        _owned(os.fstat(fd))
        yield fd
    finally:
        os.close(fd)


def _identity(info: os.stat_result) -> list[int]:
    # ctime changes on rename. Everything below survives our prepared rename.
    return [info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_gid,
            info.st_size, info.st_mtime_ns]


def _entry(fd: int, name: str) -> dict[str, Any]:
    try:
        before = os.stat(name, dir_fd=fd, follow_symlinks=False)
    except FileNotFoundError:
        return {"kind": "absent"}
    if stat.S_ISLNK(before.st_mode):
        value = {"kind": "link", "target": os.readlink(name, dir_fd=fd)}
        if before.st_uid != os.geteuid():
            raise MigrationError("foreign-owned binding")
    elif stat.S_ISREG(before.st_mode):
        _owned(before)
        if before.st_nlink != 1 or before.st_size > _LIMIT:
            raise MigrationError("binding must be a bounded, single-link regular file")
        opened = os.open(name, _FILE, dir_fd=fd)
        try:
            if _identity(os.fstat(opened)) != _identity(before):
                raise MigrationError("binding raced before read")
            with os.fdopen(opened, "rb", closefd=False) as stream:
                data = stream.read(_LIMIT + 1)
            if len(data) > _LIMIT or _identity(os.fstat(opened)) != _identity(before):
                raise MigrationError("binding raced during read")
        finally:
            os.close(opened)
        value = {"kind": "file", "data": base64.b64encode(data).decode(),
                 "sha256": _digest(data), "mode": stat.S_IMODE(before.st_mode)}
    else:
        raise MigrationError("binding is not a regular file or managed pointer")
    if _identity(os.stat(name, dir_fd=fd, follow_symlinks=False)) != _identity(before):
        raise MigrationError("binding raced after read")
    return {**value, "identity": _identity(before)}


def _snapshot(path: Path) -> dict[str, Any]:
    with _directory(path.parent) as fd:
        parent = os.fstat(fd)
        return {"path": str(path), "parent": [parent.st_dev, parent.st_ino],
                "entry": _entry(fd, path.name)}


def _bytes(entry: dict[str, Any]) -> bytes:
    if entry["kind"] != "file":
        raise MigrationError("expected a regular file")
    data = base64.b64decode(entry["data"], validate=True)
    if _digest(data) != entry["sha256"]:
        raise MigrationError("binding bytes disagree with their digest")
    return data


def _file(path: str) -> tuple[dict[str, Any], bytes]:
    snapshot = _snapshot(_path(path))
    return snapshot, _bytes(snapshot["entry"])


def read_manifest(path: Path) -> Manifest:
    _snapshot_value, data = _file(str(path))
    return Manifest.model_validate_json(data)


def _model(data: bytes, path: str) -> tuple[dict[str, Any], config.Config]:
    raw = tomllib.loads(data.decode())
    return raw, config._validate_config_data(copy.deepcopy(raw), Path(path))


def _profile(prog: config.VenvProgram) -> dict[str, Any]:
    value = prog.model_dump(mode="json")
    for key in ("description", "runtime_slot_root", "runtime_slot_execution",
                "runtime_slot_build_input"):
        value.pop(key, None)
    return value


def _programs(
    manifest: Manifest,
) -> tuple[list[dict[str, Any]], config.VenvProgram, str]:
    records = []
    representative = None
    build_inputs: set[str] = set()
    for registry in manifest.registries:
        old, data = _file(registry.path)
        candidate, new_data = _file(registry.candidate)
        raw, cfg = _model(data, registry.path)
        new_raw, _new_cfg = _model(new_data, registry.candidate)
        prog = cfg.programs.get(manifest.program)
        if not isinstance(prog, config.VenvProgram):
            raise MigrationError("each local registry must name the component's venv program")
        if representative is None:
            representative = prog
        if _profile(prog) != _profile(representative):
            raise MigrationError("authority build profiles disagree")
        aliases = []
        expected = copy.deepcopy(raw)
        # The candidate may also move the component to building from its
        # active generation (#72). Take the selected program's choice; the
        # exact comparison below then requires every alias to make it too.
        requested = (new_raw.get("programs", {}).get(manifest.program, {})
                     .get("runtime_slot_build_input"))
        build_inputs.add(requested or "legacy")
        for name, alias in cfg.programs.items():
            if not isinstance(alias, config.VenvProgram):
                continue
            # A shared checkout OR interpreter requires one component decision.
            same = (Path(alias.git_dir).resolve() == Path(prog.git_dir).resolve()
                    or Path(alias.python).absolute() == Path(prog.python).absolute())
            if not same:
                continue
            if _profile(alias) != _profile(prog):
                raise MigrationError(
                    "aliases sharing a source/interpreter have conflicting profiles"
                )
            if alias.runtime_slot_execution:
                raise MigrationError("binding is already enabled; use its transaction receipt")
            if alias.runtime_slot_root not in (None, manifest.runtime_root):
                raise MigrationError("alias already names a different runtime store")
            expected["programs"][name]["runtime_slot_root"] = manifest.runtime_root
            expected["programs"][name]["runtime_slot_execution"] = True
            if requested is not None:
                expected["programs"][name]["runtime_slot_build_input"] = requested
            aliases.append(name)
        if expected != new_raw:
            raise MigrationError(
                "candidate must change exactly every alias's root/execution "
                "binding (and, optionally, every alias's build input)"
            )
        records.append({"before": old, "candidate": candidate, "aliases": sorted(aliases),
                        "after": {**candidate["entry"], "mode": old["entry"]["mode"],
                                  "gid": old["entry"]["identity"][4]}})
    assert representative is not None
    if len(build_inputs) != 1:
        raise MigrationError("authorities disagree about the component's build input")
    # Validate source-local venv and both source-owned script entries before build.
    runtime_slots.program_venv_subdir(representative)
    runtime_slots.slot_local_program(representative, manifest.runtime_root, manifest.source_sha)
    return records, representative, build_inputs.pop()


def _verified(manifest: Manifest) -> dict[str, Any] | None:
    root, sha = manifest.runtime_root, manifest.source_sha
    target = runtime_slots.slot_path(root, sha)
    if target.is_symlink():
        raise MigrationError("generation must not be a symlink")
    if not target.exists():
        return None
    if not runtime_slots._verified_markers_match(root, sha):
        # A failed build is retryable only through the slot primitive's own
        # exact transaction receipts and liveness gates, never by migration cleanup.
        marker = runtime_slots._read_exact_marker(
            target / runtime_slots.SLOT_STATE_MARKER,
            expected_kind="vq-runtime-slot-build", expected_id=sha,
            expected_states={"building"},
        )
        receipt = runtime_slots._read_exact_marker(
            runtime_slots._slot_build_receipt(root, sha),
            expected_kind="vq-runtime-slot-build", expected_id=sha,
            expected_states={"building"},
        )
        if marker != receipt:
            raise MigrationError("incomplete generation has mismatched build receipts")
        return None
    runtime_slots._assert_generation(runtime_slots.layout(root), sha, verify_content=True)
    if not runtime_slots._verified_markers_match(root, sha):
        raise MigrationError("generation has no exact verified markers")
    if runtime_slots._head_sha(runtime_slots.slot_source(root, sha), runner=subprocess.run) != sha:
        raise MigrationError("generation Git identity differs from its verified SHA")
    return {"source": str(runtime_slots.slot_source(root, sha)),
            "python": str(runtime_slots.slot_python(root, sha)), "sha": sha,
            "marker": _snapshot(target / runtime_slots.SLOT_STATE_MARKER)}


def _verify_execution(value: dict[str, Any]) -> None:
    manifest = Manifest.model_validate(value["manifest"])
    prog = config.VenvProgram.model_validate(value["program"])
    selected = runtime_slots.slot_local_program(prog, manifest.runtime_root, manifest.source_sha)
    if selected.import_check is None and selected.vibeqc_source_root() is not None:
        selected = selected.model_copy(update={"import_check": "vibeqc"})
    availability = selected.availability_status()
    if availability.status != config.PROGRAM_STATUS_OK:
        raise MigrationError(
            "prepared runtime failed its configured probes: " + availability.reason
        )


def _pointer(root: Path, name: str) -> dict[str, Any]:
    if not root.exists():
        return {"path": str(root / name), "parent": None, "entry": {"kind": "absent"}}
    snapshot = _snapshot(root / name)
    if snapshot["entry"]["kind"] not in {"absent", "link"}:
        raise MigrationError("runtime pointer is not a symlink")
    if snapshot["entry"]["kind"] == "link":
        runtime_slots._pointer_sha(runtime_slots.layout(root), root / name)
    return snapshot


def _service_blockers(prog: config.VenvProgram, manifest: Manifest) -> list[str]:
    """Recheck the whole component, including a service moved since planning."""
    candidates = [prog.model_copy(update={"runtime_slot_execution": False})]
    shas = set(runtime_slots.list_slots(manifest.runtime_root)) | {manifest.source_sha}
    for sha in sorted(shas):
        candidates.append(prog.model_copy(update={
            "git_dir": str(runtime_slots.slot_source(manifest.runtime_root, sha)),
            "python": str(runtime_slots.slot_python(manifest.runtime_root, sha)),
            "runtime_slot_root": None, "runtime_slot_execution": False,
        }))
    for candidate in candidates:
        probe = admin._detect_vq_self_update(candidate)
        if probe.is_self_update or not probe.manager_available:
            return ["serving-daemon lifecycle or unknown service identity requires #73"]
    return []


def plan(manifest: Manifest) -> dict[str, Any]:
    """Read-only inventory; no locks, directories, staging files or builds."""
    root = runtime_slots.layout(_path(manifest.runtime_root)).root
    if str(root) != manifest.runtime_root:
        raise MigrationError("runtime root must use its canonical spelling")
    registries, prog, build_input = _programs(manifest)
    source = _path(prog.git_dir)
    with _directory(source):
        pass
    if root.is_relative_to(source) or source.is_relative_to(root):
        raise MigrationError("runtime store and legacy source must be independent trees")
    for registry in manifest.registries:
        for location in (registry.path, registry.candidate):
            if _path(location).is_relative_to(source) or _path(location).is_relative_to(root):
                raise MigrationError("registry and candidate must be outside source/runtime trees")
    if not Path(prog.python).is_file() or not os.access(prog.python, os.X_OK):
        raise MigrationError("legacy interpreter must remain executable for rollback")
    source_sha = prog.current_git_sha(full=True)
    if source_sha is None:
        raise MigrationError("legacy source identity is unreadable")
    blockers = [f"external consumer requires a reviewed participant: {x}"
                for x in manifest.external_consumers]
    if admin.admin_update_marker_exists():
        blockers.append("existing admin update or recovery marker must be resolved first")
    if prog.expected_git_sha and not manifest.source_sha.startswith(prog.expected_git_sha):
        blockers.append("target generation disagrees with the configured fixed SHA")
    blockers.extend(_service_blockers(prog, manifest))
    launchers = []
    occupied = {r.path for r in manifest.registries} | {r.candidate for r in manifest.registries}
    for launcher in manifest.launchers:
        if launcher.path in occupied or launcher.bootstrap == launcher.path:
            raise MigrationError("binding paths overlap")
        occupied.add(launcher.path)
        before = _snapshot(_path(launcher.path))
        if before["entry"]["kind"] != "absent":
            raise MigrationError("stable launcher must be new; legacy paths are retained untouched")
        bootstrap, _data = _file(launcher.bootstrap)
        if not bootstrap["entry"]["mode"] & 0o111:
            raise MigrationError("launcher bootstrap is not executable")
        if _path(launcher.bootstrap).is_relative_to(root):
            raise MigrationError("bootstrap must be retained independently of the selected store")
        command = [launcher.bootstrap, "runtime-python", "--root", str(root), "--",
                   *launcher.arguments]
        data = ("#!/bin/sh\nexec " + shlex.join(command) + ' "$@"\n').encode()
        launchers.append({"before": before, "bootstrap": bootstrap,
                          "after": {"kind": "file", "data": base64.b64encode(data).decode(),
                                    "sha256": _digest(data), "mode": 0o700}})
    if len({r.path for r in manifest.registries}) != len(manifest.registries):
        raise MigrationError("duplicate registry authority")
    generation = _verified(manifest)
    preparation = []
    for artifact in (runtime_slots._slot_build_receipt(root, manifest.source_sha),
                     runtime_slots.slot_path(root, manifest.source_sha)
                     / runtime_slots.SLOT_STATE_MARKER):
        if artifact.exists() or artifact.is_symlink():
            preparation.append(_snapshot(artifact))
    # Liveness is conservative inventory only. Nothing here prunes or moves it.
    retained = sorted(admin._runtime_slot_in_use_snapshot(str(root))) if root.exists() else []
    value = {"schema": SCHEMA, "manifest": manifest.model_dump(mode="json"),
             "program": prog.model_dump(mode="json"), "legacy_source_sha": source_sha,
             "build_input": build_input,
             "state_root": str(paths.state_root()),
             "registries": registries, "launchers": launchers, "generation": generation,
             "preparation": preparation,
             "current": _pointer(root, "current"), "previous": _pointer(root, "previous"),
             "retained_job_generations": retained, "blockers": blockers,
             "status": "ready" if generation else "preparation_required"}
    return {**value, "plan_id": _digest(_json(value))}


def _check_id(value: dict[str, Any], expected: str) -> None:
    body = {k: v for k, v in value.items() if k != "plan_id"}
    if value.get("plan_id") != expected or _digest(_json(body)) != expected:
        raise MigrationError("plan is stale or its reviewed digest disagrees")
    if value["blockers"]:
        raise MigrationError("plan has unresolved blockers")


@contextlib.contextmanager
def _ownership(value: dict[str, Any]) -> Iterator[None]:
    prog = config.VenvProgram.model_validate(value["program"])
    if str(paths.state_root()) != value["state_root"]:
        raise MigrationError("recovery must use the original queue state authority")
    manifest = Manifest.model_validate(value["manifest"])
    bound = prog.model_copy(update={"runtime_slot_root": manifest.runtime_root})
    resources = admin._runtime_slot_lifecycle_resources(bound, expected_sha=manifest.source_sha)
    with admin.admin_update_ownership(), admin.toolset_lifecycle_lock(
        [prog], action="runtime-migration", extra_resources=resources,
    ):
        admin._guard_admin_update_marker(force=False)
        blockers = _service_blockers(prog, manifest)
        if blockers:
            raise MigrationError("; ".join(blockers))
        yield


def prepare(manifest: Manifest, expected_plan_id: str) -> dict[str, Any]:
    before = plan(manifest)
    _check_id(before, expected_plan_id)
    with _ownership(before):
        _check_id(plan(manifest), expected_plan_id)
        prog = config.VenvProgram.model_validate(before["program"]).model_copy(
            update={"runtime_slot_root": manifest.runtime_root},
        )
        result = admin._do_slot_update_work(
            manifest.program, prog, expected_sha=manifest.source_sha, prepare_only=True,
        )
        if not result.success:
            raise MigrationError("preparation failed; bindings unchanged: "
                                 + "; ".join(result.work_errors))
        after = plan(manifest)
        if after["generation"] is None:
            raise MigrationError("preparation did not produce a verified generation")
        return after


def _write_new(fd: int, name: str, entry: dict[str, Any]) -> dict[str, Any]:
    if entry["kind"] == "absent":
        return entry
    if entry["kind"] == "link":
        os.symlink(entry["target"], name, dir_fd=fd)
    elif entry["kind"] == "file":
        handle = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                         entry["mode"], dir_fd=fd)
        try:
            os.fchmod(handle, entry["mode"])
            gid = entry.get("gid", (entry.get("identity") or [0] * 5)[4])
            if "gid" in entry or "identity" in entry:
                os.fchown(handle, -1, gid)
            with os.fdopen(handle, "wb", closefd=False) as stream:
                stream.write(_bytes(entry))
                stream.flush()
                os.fsync(handle)
        finally:
            os.close(handle)
    else:
        raise MigrationError("unknown binding kind")
    os.fsync(fd)
    return _entry(fd, name)


def _journal(directory: Path, value: dict[str, Any]) -> None:
    data = _json(value)
    entry = {"kind": "file", "data": base64.b64encode(data).decode(),
             "sha256": _digest(data), "mode": 0o600}
    with _directory(directory) as fd:
        name = ".journal-" + secrets.token_hex(16)
        _write_new(fd, name, entry)
        os.replace(name, "transaction.json", src_dir_fd=fd, dst_dir_fd=fd)
        os.fsync(fd)


def _actions(value: dict[str, Any]) -> list[dict[str, Any]]:
    root = Path(value["manifest"]["runtime_root"])
    sha = value["manifest"]["source_sha"]
    current = value["current"]["entry"]
    target = str(Path("releases") / sha)
    actions = []
    if current.get("target") != target:
        if current["kind"] == "link":
            actions.append({"before": value["previous"],
                            "after": {"kind": "link", "target": current["target"]}})
        actions.append({"before": value["current"],
                        "after": {"kind": "link", "target": target}})
    actions.extend({"before": item["before"], "after": item["after"]}
                   for item in [*value["launchers"], *value["registries"]])
    if len({a["before"]["path"] for a in actions}) != len(actions):
        raise MigrationError("binding overlaps a runtime pointer")
    for action in actions:
        path = Path(action["before"]["path"])
        if path.is_relative_to(root) and path not in (root / "current", root / "previous"):
            raise MigrationError("external binding cannot live inside its runtime store")
    return actions


def apply(manifest: Manifest, expected_plan_id: str, receipt_dir: Path) -> dict[str, Any]:
    value = plan(manifest)
    _check_id(value, expected_plan_id)
    if value["status"] != "ready":
        raise MigrationError("prepare and review a verified generation before applying")
    directory = _path(receipt_dir)
    for retained in (Path(value["program"]["git_dir"]), Path(manifest.runtime_root)):
        if directory.is_relative_to(retained):
            raise MigrationError("receipt directory must be outside source and runtime trees")
    with _ownership(value):
        _check_id(plan(manifest), expected_plan_id)
        _verify_execution(value)
        actions = _actions(value)
        for action in actions:
            action["forward_stage"] = ".vq-migrate-" + secrets.token_hex(16)
            action["rollback_stage"] = ".vq-migrate-" + secrets.token_hex(16)
        with _directory(directory.parent) as fd:
            os.mkdir(directory.name, mode=0o700, dir_fd=fd)
            os.fsync(fd)
        state = {"schema": SCHEMA, "plan": value, "state": "initializing", "actions": actions}
        _journal(directory, state)
        for action in actions:
            path = Path(action["before"]["path"])
            with _directory(path.parent) as fd:
                if _snapshot(path) != action["before"]:
                    raise MigrationError("binding changed before staging; originals untouched")
                action["prepared_after"] = _write_new(fd, action["forward_stage"], action["after"])
                action["prepared_before"] = _write_new(
                    fd, action["rollback_stage"], action["before"]["entry"],
                )
        state["state"] = "prepared"
        _journal(directory, state)
        return _resume(directory, state, rollback=False)


def _matches(actual: dict[str, Any], expected: dict[str, Any]) -> bool:
    return actual == expected


def _resume(directory: Path, state: dict[str, Any], *, rollback: bool) -> dict[str, Any]:
    if state["state"] == "initializing":
        raise MigrationError("initialization interrupted; original bindings are intact; replan")
    if state["state"] == "rolled_back":
        if rollback:
            return state
        raise MigrationError("rolled-back transaction cannot be reapplied; make a new plan")
    if not rollback and state["state"] == "rolling_back":
        raise MigrationError("rollback has started; finish rollback before replanning")
    manifest = Manifest.model_validate(state["plan"]["manifest"])
    if not rollback:
        if _verified(manifest) is None:
            raise MigrationError("prepared generation is no longer verified")
        _verify_execution(state["plan"])
        # Bootstrap bytes and candidates are authority only before first binding;
        # the recorded bytes own recovery, but bootstrap must remain available.
        for launcher in state["plan"]["launchers"]:
            if _snapshot(Path(launcher["bootstrap"]["path"])) != launcher["bootstrap"]:
                raise MigrationError("retained launcher bootstrap changed")
    else:
        for name in ("current", "previous"):
            old = state["plan"][name]["entry"]
            if old["kind"] == "link":
                sha = Path(old["target"]).name
                runtime_slots._assert_generation(
                    runtime_slots.layout(manifest.runtime_root), sha, verify_content=True,
                )
    ordered = list(reversed(state["actions"])) if rollback else state["actions"]
    state["state"] = "rolling_back" if rollback else "applying"
    _journal(directory, state)
    # Preflight every participant, so a known foreign edit causes no further writes.
    for action in ordered:
        path = Path(action["before"]["path"])
        snapshot = _snapshot(path)
        if snapshot["parent"] != action["before"]["parent"]:
            raise MigrationError("binding parent was replaced")
        actual = snapshot["entry"]
        allowed = [action["before"]["entry"], action["prepared_after"]]
        if rollback:
            allowed.append(action["prepared_before"])
        if not any(_matches(actual, item) for item in allowed):
            raise MigrationError("binding has a foreign edit; recovery will not overwrite it")
    for action in ordered:
        path = Path(action["before"]["path"])
        desired = action["prepared_before"] if rollback else action["prepared_after"]
        stage = action["rollback_stage"] if rollback else action["forward_stage"]
        with _directory(path.parent) as fd:
            parent = os.fstat(fd)
            if [parent.st_dev, parent.st_ino] != action["before"]["parent"]:
                raise MigrationError("binding parent changed at replacement boundary")
            actual = _entry(fd, path.name)
            if _matches(actual, desired) or (rollback and actual == action["before"]["entry"]):
                continue
            expected = action["prepared_after"] if rollback else action["before"]["entry"]
            if not _matches(actual, expected):
                raise MigrationError("binding changed at replacement boundary")
            if desired["kind"] == "absent":
                os.unlink(path.name, dir_fd=fd)
            else:
                if _entry(fd, stage) != desired:
                    raise MigrationError("prepared binding is missing or changed")
                os.replace(stage, path.name, src_dir_fd=fd, dst_dir_fd=fd)
            os.fsync(fd)
        # Journal is durable before and after each independently atomic rename.
        _journal(directory, state)
    state["state"] = "rolled_back" if rollback else "applied"
    _journal(directory, state)
    return state


def recover(receipt_dir: Path, *, rollback: bool = False) -> dict[str, Any]:
    directory = _path(receipt_dir)
    # The receipt lock is independent of source paths, which may be unavailable.
    with _directory(directory) as fd:
        lock = os.open("lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600, dir_fd=fd)
        try:
            _owned(os.fstat(lock))
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            state = json.loads(_bytes(_entry(fd, "transaction.json")))
            if state.get("schema") != SCHEMA:
                raise MigrationError("unsupported migration receipt")
            _check_id(state["plan"], state["plan"]["plan_id"])
            with _ownership(state["plan"]):
                return _resume(directory, state, rollback=rollback)
        finally:
            os.close(lock)
