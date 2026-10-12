"""Reviewable repair of terminal records after a state-root alias migration.

Only the explicitly named old-root alias may be followed. All record writes
are relative to pinned, no-follow directory descriptors. Artifacts never move.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import stat
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from vq import dirfd, paths
from vq.spec import JobSpec, validate_job_id
from vq.spec_access import SPEC_READ_MAX_BYTES

FIELDS = ("cwd", "workdir", "archive_path")
SCHEMA = "vq.state-path-repair/1"
_DIR = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
_FILE = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK


class RepairError(ValueError):
    """The selected records or root mapping cannot be repaired safely."""


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _json(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, ensure_ascii=False, indent=2) + "\n").encode()


def _absolute(value: Path) -> Path:
    value = value.expanduser()
    if not value.is_absolute() or ".." in value.parts or str(value) == "/":
        raise RepairError("roots must be explicit absolute paths without '..'")
    paths.require_test_path_within_sandbox(value, "state-path repair root")
    return value


def _identity(info: os.stat_result) -> list[int]:
    return [
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_uid,
        info.st_gid,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    ]


def _owned(info: os.stat_result) -> None:
    if info.st_uid != os.geteuid() or info.st_mode & 0o022:
        raise RepairError("repair requires owner-controlled files and directories")


@contextlib.contextmanager
def _directory(path: Path, *, owner: bool = True) -> Iterator[int]:
    """Pin every ancestor without following a symlink, including the last."""
    fd = os.open("/", _DIR)
    try:
        for component in path.parts[1:]:
            child = os.open(component, _DIR, dir_fd=fd)
            os.close(fd)
            fd = child
        if owner:
            _owned(os.fstat(fd))
        yield fd
    finally:
        os.close(fd)


@contextlib.contextmanager
def _relative_directory(root_fd: int, relative: Path) -> Iterator[int]:
    fd = os.dup(root_fd)
    try:
        for component in relative.parts:
            if component in {".", "..", "/"}:
                raise RepairError("noncanonical relative directory")
            child = os.open(component, _DIR, dir_fd=fd)
            os.close(fd)
            fd = child
            _owned(os.fstat(fd))
        yield fd
    finally:
        os.close(fd)


def _read_at(directory_fd: int, name: str, *, limit: int) -> tuple[bytes, list[int]]:
    before_entry = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    fd = os.open(name, _FILE, dir_fd=directory_fd)
    try:
        before = os.fstat(fd)
        _owned(before)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            raise RepairError("record must be a single-link regular file")
        if _identity(before_entry) != _identity(before) or before.st_size > limit:
            raise RepairError("record changed or exceeds the bounded read size")
        data = bytearray()
        while chunk := os.read(fd, min(65536, limit + 1 - len(data))):
            data.extend(chunk)
            if len(data) > limit:
                raise RepairError("record exceeds the bounded read size")
        after_entry = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if _identity(before) != _identity(os.fstat(fd)) or (
            _identity(before) != _identity(after_entry)
        ):
            raise RepairError("record changed while being read")
        return bytes(data), _identity(before)
    finally:
        os.close(fd)


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise RepairError(f"duplicate JSON field: {key}")
        result[key] = value
    return result


def _decode(data: bytes, name: str) -> tuple[dict[str, Any], JobSpec]:
    def nonfinite(value: str) -> None:
        raise RepairError(f"nonfinite JSON value: {value}")

    raw = json.loads(data, object_pairs_hook=_pairs, parse_constant=nonfinite)
    spec = JobSpec.from_json(data.decode())
    if spec.id != validate_job_id(Path(name).stem):
        raise RepairError("record filename and job identity differ")
    return raw, spec


def _queue_paths(root_fd: int) -> list[Path]:
    result = [Path("queue")]
    if "users" in dirfd.listdir(root_fd):
        with _relative_directory(root_fd, Path("users")) as users_fd:
            for uid in sorted(dirfd.listdir(users_fd)):
                if not uid.isdecimal() or str(int(uid)) != uid:
                    raise RepairError("unexpected entry in users directory")
                # A user repair never crosses another user's queue authority.
                if uid != str(os.geteuid()):
                    raise RepairError("repair another user's records as their owner")
                result.append(Path("users") / uid / "queue")
    return result


def _suffix(value: str, old: Path, field: str, spec: JobSpec) -> Path | None:
    path = Path(value)
    if not path.is_relative_to(old):
        return None
    if not path.is_absolute() or ".." in path.parts or str(path) != value:
        raise RepairError("recorded path is not canonical")
    relative = path.relative_to(old)
    parts = relative.parts
    if len(parts) == 4 and parts[0] == "users":
        if parts[1] != str(os.geteuid()):
            raise RepairError("recorded artifact belongs to another user")
        parts = parts[2:]
    kind = {"cwd": "jobs", "workdir": "workdirs", "archive_path": "archive"}[field]
    if len(parts) != 2 or parts[0] != kind:
        raise RepairError(f"{field} is outside the supported managed layout")
    if field == "archive_path":
        valid = parts[1] == f"{spec.id}.tar.bz2" or parts[1].endswith(f"-{spec.id}.tar.bz2")
    else:
        valid = parts[1] == spec.id
    if not valid:
        raise RepairError(f"{field} does not bind this job's artifact")
    return relative


def _artifact(root_fd: int, relative: Path) -> dict[str, Any]:
    """Hash a stable no-follow snapshot; internal symlinks are data, not roots."""
    count = 0
    symlinks: list[dict[str, str]] = []

    def visit(parent: int, name: str, trail: str) -> dict[str, Any]:
        nonlocal count
        count += 1
        if count > 100000:
            raise RepairError("artifact exceeds the 100000-entry inventory bound")
        before = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if stat.S_ISLNK(before.st_mode):
            # A top-level symlink would authorize an arbitrary alternate root.
            target = os.readlink(name, dir_fd=parent)
            value: dict[str, Any] = {"kind": "symlink", "target": target}
            symlinks.append({"path": trail, "target": target})
        elif stat.S_ISDIR(before.st_mode):
            fd = os.open(name, _DIR, dir_fd=parent)
            try:
                _owned(os.fstat(fd))
                if _identity(before) != _identity(os.fstat(fd)):
                    raise RepairError("artifact directory changed before opening")
                names = sorted(dirfd.listdir(fd))
                children = {child: visit(fd, child, f"{trail}/{child}") for child in names}
                if (
                    names != sorted(dirfd.listdir(fd))
                    or _identity(before) != _identity(os.fstat(fd))
                ):
                    raise RepairError("artifact directory changed during inventory")
                value = {"kind": "directory", "sha256": _digest(_json(children))}
            finally:
                os.close(fd)
        elif stat.S_ISREG(before.st_mode):
            fd = os.open(name, _FILE, dir_fd=parent)
            try:
                if _identity(before) != _identity(os.fstat(fd)):
                    raise RepairError("artifact was replaced before opening")
                digest = hashlib.sha256()
                while chunk := os.read(fd, 1024 * 1024):
                    digest.update(chunk)
                if _identity(before) != _identity(os.fstat(fd)):
                    raise RepairError("artifact changed during inventory")
                value = {"kind": "file", "sha256": digest.hexdigest(), "bytes": before.st_size}
            finally:
                os.close(fd)
        else:
            raise RepairError("artifact contains a special file")
        if _identity(before) != _identity(os.stat(name, dir_fd=parent, follow_symlinks=False)):
            raise RepairError("artifact entry changed during inventory")
        return value

    try:
        with _relative_directory(root_fd, relative.parent) as parent:
            before = os.stat(relative.name, dir_fd=parent, follow_symlinks=False)
            if stat.S_ISLNK(before.st_mode):
                raise RepairError("artifact root is a symlink")
            value = visit(parent, relative.name, str(relative))
            value["symlinks"] = symlinks
            value["identity"] = _identity(before)
            return value
    except FileNotFoundError:
        return {"kind": "absent"}


@contextlib.contextmanager
def _old_alias(old: Path, new: Path, new_fd: int) -> Iterator[tuple[int, dict[str, Any]]]:
    with _directory(old.parent, owner=False) as parent:
        entry = os.stat(old.name, dir_fd=parent, follow_symlinks=False)
        if not stat.S_ISLNK(entry.st_mode):
            raise RepairError("old root must be the explicitly reviewed compatibility symlink")
        target = os.readlink(old.name, dir_fd=parent)
        if target != str(new):
            raise RepairError("old-root symlink does not directly name the selected new root")
        fd = os.open(old.name, os.O_RDONLY | os.O_DIRECTORY, dir_fd=parent)
        try:
            if _identity(os.fstat(fd))[:2] != _identity(os.fstat(new_fd))[:2]:
                raise RepairError("old and new roots do not name the same current directory")
            proof = {"entry": _identity(entry), "target": target}
            if _identity(entry) != _identity(
                os.stat(old.name, dir_fd=parent, follow_symlinks=False)
            ):
                raise RepairError("old-root alias changed while opening")
            yield fd, proof
            if _identity(entry) != _identity(
                os.stat(old.name, dir_fd=parent, follow_symlinks=False)
            ):
                raise RepairError("old-root alias changed during repair")
        finally:
            os.close(fd)


def _residue_paths(node: object, needle: str) -> list[str]:
    """Report JSON pointers with lexical old-root mentions, never rewrite them."""
    pending: list[tuple[object, str]] = [(node, "")]
    found: list[str] = []
    while pending:
        value, pointer = pending.pop()
        if isinstance(value, str) and needle in value:
            found.append(pointer)
        elif isinstance(value, dict):
            for key, child in value.items():
                token = str(key).replace("~", "~0").replace("/", "~1")
                pending.append((child, pointer + "/" + token))
        elif isinstance(value, list):
            pending.extend((child, f"{pointer}/{i}") for i, child in enumerate(value))
    return sorted(found)


def plan_repair(
    state_root: Path,
    old_root: Path,
    new_root: Path,
    *,
    jobs: tuple[str, ...] = (),
) -> dict[str, Any]:
    state, old, new = map(_absolute, (state_root, old_root, new_root))
    if old == new or old.is_relative_to(new) or new.is_relative_to(old):
        raise RepairError("old and new roots must be distinct, non-nested paths")
    selected = sorted({validate_job_id(job) for job in jobs})
    seen: set[str] = set()
    bindings: dict[str, dict[str, tuple[str, str]]] = {}
    plan: dict[str, Any] = {
        "schema": SCHEMA,
        "state_root": str(state),
        "old_root": str(old),
        "new_root": str(new),
        "jobs": selected,
        "records": [],
        "blockers": [],
        "inventory": [],
        "residue": [],
        "alias_dependencies": [],
        "historical_content_verified": False,
        "correspondence": "current referents only; deleted historical bytes are not proved",
    }
    scanned = 0
    with _directory(state) as state_fd, _directory(new) as new_fd:
        plan["state_identity"] = _identity(os.fstat(state_fd))[:5]
        plan["new_identity"] = _identity(os.fstat(new_fd))[:5]
        with contextlib.ExitStack() as stack:
            old_fd = None
            try:
                old_fd, plan["alias"] = stack.enter_context(_old_alias(old, new, new_fd))
            except (OSError, RepairError) as exc:
                plan["blockers"].append(f"old-root correspondence unproven: {exc}")
            for queue in _queue_paths(state_fd):
                try:
                    with _relative_directory(state_fd, queue) as queue_fd:
                        for name in sorted(dirfd.listdir(queue_fd)):
                            if not name.endswith(".json") or (
                                selected and Path(name).stem not in selected
                            ):
                                continue
                            scanned += 1
                            inventory = None
                            try:
                                seen.add(Path(name).stem)
                                data, identity = _read_at(queue_fd, name, limit=SPEC_READ_MAX_BYTES)
                                raw, spec = _decode(data, name)
                                residue = _residue_paths(
                                    {key: value for key, value in raw.items() if key not in FIELDS},
                                    str(old),
                                )
                                if residue:
                                    plan["residue"].append(
                                        {"path": str(queue / name), "jobid": spec.id,
                                         "pointers": residue}
                                    )
                                affected = [
                                    field for field in FIELDS
                                    if isinstance(raw.get(field), str)
                                    and Path(raw[field]).is_relative_to(old)
                                ]
                                if affected:
                                    inventory = {
                                        "path": str(queue / name), "jobid": spec.id,
                                        "state": spec.state.value, "fields": affected,
                                    }
                                    plan["inventory"].append(inventory)
                                fields: dict[str, Any] = {}
                                for field in FIELDS:
                                    value = raw.get(field)
                                    if not isinstance(value, str):
                                        continue
                                    relative = _suffix(value, old, field, spec)
                                    if relative is None:
                                        continue
                                    if not spec.is_terminal:
                                        raise RepairError("affected job is active, not terminal")
                                    current = _artifact(new_fd, relative)
                                    for link in current.get("symlinks", []):
                                        if Path(link["target"]).is_relative_to(old):
                                            dependency = {"jobid": spec.id, **link}
                                            if dependency not in plan["alias_dependencies"]:
                                                plan["alias_dependencies"].append(dependency)
                                    expected_kind = (
                                        "file" if field == "archive_path" else "directory"
                                    )
                                    if current["kind"] not in {expected_kind, "absent"}:
                                        raise RepairError(f"{field} has the wrong artifact type")
                                    prior = (
                                        _artifact(old_fd, relative) if old_fd is not None else None
                                    )
                                    if prior is not None and prior != current:
                                        raise RepairError(
                                            "old and new current artifact snapshots differ"
                                        )
                                    fields[field] = {
                                        "old": value,
                                        "new": str(new / relative),
                                        "artifact": current,
                                    }
                                if not fields:
                                    continue
                                binding = {
                                    field: (change["old"], change["new"])
                                    for field, change in fields.items()
                                }
                                if spec.id in bindings and bindings[spec.id] != binding:
                                    raise RepairError(
                                        "duplicate job records have ambiguous path bindings"
                                    )
                                bindings[spec.id] = binding
                                if not spec.is_terminal:
                                    raise RepairError("affected job is active, not terminal")
                                source_field = "archive_path" if spec.is_archived else "cwd"
                                if (
                                    source_field not in fields
                                    or fields[source_field]["artifact"]["kind"] == "absent"
                                ):
                                    raise RepairError(
                                        "current fetch artifact is absent or outside the mapping"
                                    )
                                plan["records"].append(
                                    {
                                        "path": str(queue / name),
                                        "jobid": spec.id,
                                        "sha256": _digest(data),
                                        "identity": identity,
                                        "queue_identity": _identity(os.fstat(queue_fd))[:5],
                                        "fields": fields,
                                    }
                                )
                            except (OSError, ValueError) as exc:
                                if inventory is not None:
                                    inventory["blocker"] = str(exc)
                                plan["blockers"].append(f"{queue / name}: {exc}")
                except OSError as exc:
                    plan["blockers"].append(f"{queue}: {exc}")
    for missing in sorted(set(selected) - seen):
        plan["blockers"].append(f"selected job not found: {missing}")
    plan["summary"] = {
        "scanned_records": scanned,
        "affected_records": len(plan["inventory"]),
        "candidate_records": len(plan["records"]),
        "applicable_records": 0 if plan["blockers"] else len(plan["records"]),
        "blockers": len(plan["blockers"]),
        "residue_records": len(plan["residue"]),
        "alias_dependent_links": len(plan["alias_dependencies"]),
    }
    plan["plan_id"] = _digest(_json(plan))
    return plan


def _write_new(
    directory_fd: int,
    name: str,
    data: bytes,
    *,
    mode: int = 0o600,
    gid: int | None = None,
) -> None:
    fd = os.open(
        name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode, dir_fd=directory_fd
    )
    try:
        if gid is not None and os.fstat(fd).st_gid != gid:
            os.fchown(fd, -1, gid)
        os.fchmod(fd, mode)
        with os.fdopen(fd, "wb", closefd=False) as stream:
            stream.write(data)
            stream.flush()
            os.fsync(fd)
    finally:
        os.close(fd)
    os.fsync(directory_fd)


def apply_repair(
    plan: dict[str, Any], *, expected_plan_id: str, receipt_dir: Path
) -> dict[str, Any]:
    """Revalidate the reviewed plan, preserve originals, then replace per record.

    Records are atomic individually. A failure after a replacement leaves a
    durable receipt and all originals; it never rolls back a concurrent writer.
    """
    fresh = plan_repair(
        Path(plan["state_root"]),
        Path(plan["old_root"]),
        Path(plan["new_root"]),
        jobs=tuple(plan["jobs"]),
    )
    if fresh["plan_id"] != expected_plan_id or plan != fresh:
        raise RepairError("plan changed; review a fresh dry run")
    if fresh["blockers"]:
        raise RepairError("plan has blockers; no records were changed")
    receipt = _absolute(receipt_dir)
    if any(
        receipt.is_relative_to(Path(fresh[key])) for key in ("state_root", "old_root", "new_root")
    ):
        raise RepairError("receipt directory must be outside record and artifact roots")
    state = Path(fresh["state_root"])
    originals: dict[str, bytes] = {}
    with _directory(state) as state_fd, _directory(receipt.parent) as parent_fd:
        if _identity(os.fstat(state_fd))[:5] != fresh["state_identity"]:
            raise RepairError("selected record root changed before preservation")
        # Preserve every original before any record mutation.
        for row in fresh["records"]:
            relative = Path(row["path"])
            with _relative_directory(state_fd, relative.parent) as queue_fd:
                if _identity(os.fstat(queue_fd))[:5] != row["queue_identity"]:
                    raise RepairError("queue directory changed before preservation")
                data, identity = _read_at(queue_fd, relative.name, limit=SPEC_READ_MAX_BYTES)
            if _digest(data) != row["sha256"] or identity != row["identity"]:
                raise RepairError("record changed before preservation")
            originals[row["path"]] = data
        os.mkdir(receipt.name, mode=0o700, dir_fd=parent_fd)
        os.fsync(parent_fd)
        with _relative_directory(parent_fd, Path(receipt.name)) as receipt_fd:
            _write_new(receipt_fd, "plan.json", _json(fresh))
            for original_path, data in originals.items():
                _write_new(receipt_fd, _digest(original_path.encode()) + ".original.json", data)
            completed: list[str] = []
            for row in fresh["records"]:
                relative = Path(row["path"])
                with _relative_directory(state_fd, relative.parent) as queue_fd:
                    if _identity(os.fstat(queue_fd))[:5] != row["queue_identity"]:
                        raise RepairError("queue directory changed before replacement")
                    lock_name = relative.name + ".lock"
                    lock_fd = os.open(
                        lock_name, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600, dir_fd=queue_fd
                    )
                    try:
                        _owned(os.fstat(lock_fd))
                        if (
                            not stat.S_ISREG(os.fstat(lock_fd).st_mode)
                            or os.fstat(lock_fd).st_nlink != 1
                        ):
                            raise RepairError("spec lock is not regular")
                        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        if _identity(os.fstat(lock_fd)) != _identity(
                            os.stat(lock_name, dir_fd=queue_fd, follow_symlinks=False)
                        ):
                            raise RepairError("spec lock was replaced during acquisition")
                        data, identity = _read_at(
                            queue_fd, relative.name, limit=SPEC_READ_MAX_BYTES
                        )
                        if data != originals[row["path"]] or identity != row["identity"]:
                            raise RepairError("record changed under lock; retained partial receipt")
                        raw, spec = _decode(data, relative.name)
                        if not spec.is_terminal:
                            raise RepairError("job became active before repair")
                        # Re-open both roots without granting write authority to the alias.
                        with (
                            _directory(Path(fresh["new_root"])) as new_fd,
                            _old_alias(
                                Path(fresh["old_root"]),
                                Path(fresh["new_root"]),
                                new_fd,
                            ) as (old_fd, alias),
                        ):
                            if alias != fresh["alias"]:
                                raise RepairError("old-root alias changed before replacement")
                            for field, change in row["fields"].items():
                                artifact = Path(change["new"]).relative_to(fresh["new_root"])
                                if _artifact(new_fd, artifact) != change["artifact"] or (
                                    _artifact(old_fd, artifact) != change["artifact"]
                                ):
                                    raise RepairError("artifact changed before replacement")
                                raw[field] = change["new"]
                            new_data = _json(raw)
                            temp = ".state-path-repair-" + uuid.uuid4().hex
                            _write_new(
                                queue_fd,
                                temp,
                                new_data,
                                mode=stat.S_IMODE(identity[2]),
                                gid=identity[4],
                            )
                            _write_new(
                                receipt_fd,
                                f"{len(completed) + 1:06d}.intent.json",
                                _json(
                                    {
                                        "path": row["path"],
                                        "before": row["sha256"],
                                        "after": _digest(new_data),
                                        "temporary": temp,
                                    }
                                ),
                            )
                            current_data, current_identity = _read_at(
                                queue_fd, relative.name, limit=SPEC_READ_MAX_BYTES
                            )
                            if current_data != data or current_identity != identity:
                                raise RepairError("record changed before atomic replacement")
                            os.replace(
                                temp, relative.name, src_dir_fd=queue_fd, dst_dir_fd=queue_fd
                            )
                            os.fsync(queue_fd)
                        completed.append(row["path"])
                        _write_new(
                            receipt_fd,
                            f"{len(completed):06d}.applied.json",
                            _json(
                                {
                                    "path": row["path"],
                                    "before": row["sha256"],
                                    "after": _digest(new_data),
                                }
                            ),
                        )
                    finally:
                        os.close(lock_fd)
            result = {
                "schema": SCHEMA,
                "plan_id": expected_plan_id,
                "status": "complete",
                "records_changed": len(completed),
            }
            _write_new(receipt_fd, "result.json", _json(result))
            return result
