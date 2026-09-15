#!/usr/bin/env python3
"""Durable filesystem transaction for direct vq virtualenv replacement.

This helper is deliberately isolated from the target virtualenv.  The shell
entry points execute it with the same trusted external Python that owns the
toolset lifecycle lock.  It never imports vq or executes candidate bytes.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import re
import secrets
import shutil
import stat
import sys
from pathlib import Path
from typing import Any

SCHEMA = 1
KIND = "vq-direct-venv-replacement"
PHASES = {"armed", "building", "target_committed"}
TX_RE = re.compile(r"[0-9a-f]{32}")


class TransactionError(RuntimeError):
    """A malformed or ambiguous transaction that must remain fenced."""


def _fsync_dir(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    fd = os.open(str(path), flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _canonical_checkout(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise TransactionError("checkout is not absolute")
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise TransactionError(f"cannot resolve checkout: {exc}") from exc
    if not resolved.is_dir() or resolved.is_symlink():
        raise TransactionError("checkout is not a real directory")
    return resolved


def _canonical_target(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute() or path.name in {"", ".", ".."}:
        raise TransactionError("target is not a canonical absolute path")
    if path.exists() or path.is_symlink():
        if path.is_symlink() or not path.is_dir():
            raise TransactionError("target is not a real directory")
        target = path.resolve(strict=True)
        _assert_not_immutable_runtime_target(target)
        return target
    parent = path.parent.resolve(strict=True)
    if not parent.is_dir() or parent.is_symlink():
        raise TransactionError("target parent is not a real directory")
    target = parent / path.name
    _assert_not_immutable_runtime_target(target)
    return target


def _assert_not_immutable_runtime_target(target: Path) -> None:
    marker = target / ".vq-immutable-runtime"
    if marker.exists() or marker.is_symlink():
        raise TransactionError(
            f"refusing durable replacement of immutable runtime generation: {target}"
        )
    parts = target.parts
    if len(parts) >= 4 and parts[-1] == ".venv" and parts[-2] == "source":
        sha = parts[-3]
        if parts[-4] == "releases" and re.fullmatch(r"[0-9a-f]{40}", sha):
            raise TransactionError(
                f"refusing durable replacement of runtime slot generation: {target}"
            )


def receipt_path(target: Path) -> Path:
    return target.parent / f".{target.name}.vq-venv-replacement.json"


def receipt_update_path(receipt: Path, transaction: str) -> Path:
    return receipt.parent / f".{receipt.name}.next-{transaction}"


def backup_path(target: Path, transaction: str) -> Path:
    return target.parent / f".{target.name}.vq-venv-backup-{transaction}"


def marker_path(target: Path) -> Path:
    return target / ".vq-venv-transaction"


def marker_temp_path(target: Path, transaction: str) -> Path:
    return target.parent / f".{target.name}.vq-venv-marker-{transaction}"


def candidate_staging_path(target: Path, transaction: str) -> Path:
    return target.parent / f".{target.name}.vq-venv-candidate-{transaction}"


def _validate_regular(path: Path, *, mode: int, owner: int) -> os.stat_result:
    try:
        info = path.lstat()
    except OSError as exc:
        raise TransactionError(f"cannot inspect {path}: {exc}") from exc
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != owner
        or info.st_nlink != 1
        or stat.S_IMODE(info.st_mode) != mode
    ):
        raise TransactionError(f"unsafe transaction file {path}")
    return info


def _read_receipt(
    path: Path,
    *,
    expected_checkout: Path,
    expected_target: Path,
) -> dict[str, Any]:
    owner = os.geteuid()
    _validate_regular(path, mode=0o600, owner=owner)
    try:
        raw = path.read_text(encoding="utf-8")
        value = json.loads(raw)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise TransactionError(f"cannot parse transaction receipt: {exc}") from exc
    expected_keys = {
        "schema",
        "kind",
        "transaction",
        "checkout",
        "target",
        "owner_uid",
        "had_original",
        "original_dev",
        "original_ino",
        "candidate_dev",
        "candidate_ino",
        "target_tree_sha256",
        "phase",
    }
    if not isinstance(value, dict) or set(value) != expected_keys:
        raise TransactionError("transaction receipt has an invalid schema")
    transaction = value.get("transaction")
    if (
        type(value.get("schema")) is not int
        or value.get("schema") != SCHEMA
        or value.get("kind") != KIND
        or not isinstance(transaction, str)
        or TX_RE.fullmatch(transaction) is None
        or value.get("checkout") != str(expected_checkout)
        or value.get("target") != str(expected_target)
        or type(value.get("owner_uid")) is not int
        or type(value.get("had_original")) is not bool
        or (
            value.get("had_original")
            and (
                type(value.get("original_dev")) is not int
                or type(value.get("original_ino")) is not int
            )
        )
        or (
            not value.get("had_original")
            and (
                value.get("original_dev") is not None
                or value.get("original_ino") is not None
            )
        )
        or (
            value.get("candidate_dev") is not None
            and type(value.get("candidate_dev")) is not int
        )
        or (
            value.get("candidate_ino") is not None
            and type(value.get("candidate_ino")) is not int
        )
        or ((value.get("candidate_dev") is None) != (value.get("candidate_ino") is None))
        or (
            value.get("target_tree_sha256") is not None
            and (
                not isinstance(value.get("target_tree_sha256"), str)
                or re.fullmatch(r"[0-9a-f]{64}", value["target_tree_sha256"])
                is None
            )
        )
        or value.get("phase") not in PHASES
    ):
        raise TransactionError("transaction receipt identity is invalid")
    if value["owner_uid"] != owner:
        raise TransactionError("transaction receipt owner changed")
    return value


def _write_receipt(path: Path, value: dict[str, Any], *, replace: bool) -> None:
    owner = value["owner_uid"]
    if os.geteuid() not in {0, owner}:
        raise TransactionError("current user does not own the transaction target")
    transaction = value.get("transaction")
    if not isinstance(transaction, str) or TX_RE.fullmatch(transaction) is None:
        raise TransactionError("transaction receipt identity is invalid")
    if replace:
        _validate_regular(path, mode=0o600, owner=owner)
        temp = receipt_update_path(path, transaction)
        if temp.exists() or temp.is_symlink():
            raise TransactionError(f"a receipt update is already pending: {temp}")
    elif path.exists() or path.is_symlink():
        raise TransactionError(f"a replacement receipt already exists: {path}")
    else:
        # The initial receipt is the admission fence. Write it at its final,
        # discoverable name so SIGKILL can leave only a fenced malformed
        # receipt, never an anonymous journal file that the next invocation
        # cannot find. No filesystem mutation has occurred at this boundary.
        temp = path
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(str(temp), flags, 0o600)
    try:
        if os.geteuid() == 0 and owner != 0:
            os.fchown(fd, owner, -1)
        os.fchmod(fd, 0o600)
        payload = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
        offset = 0
        while offset < len(payload):
            offset += os.write(fd, payload[offset:])
        os.fsync(fd)
    except BaseException:
        # A replacement update always has a valid older canonical receipt to
        # drive recovery, so its exact transaction-derived scratch file can be
        # removed on an ordinary exception. For the initial admission write,
        # retain a possibly partial canonical receipt as an explicit fence.
        if replace:
            with contextlib.suppress(OSError):
                temp.unlink()
        raise
    finally:
        os.close(fd)
    if replace:
        os.replace(temp, path)
    _fsync_dir(path.parent)


def _remove_pending_receipt_update(
    receipt: Path,
    value: dict[str, Any],
) -> None:
    pending = receipt_update_path(receipt, value["transaction"])
    if not pending.exists() and not pending.is_symlink():
        return
    _validate_regular(pending, mode=0o600, owner=value["owner_uid"])
    pending.unlink()
    _fsync_dir(pending.parent)


def _write_phase(
    path: Path,
    *,
    checkout: Path,
    target: Path,
    phase: str,
) -> dict[str, Any]:
    value = _read_receipt(
        path,
        expected_checkout=checkout,
        expected_target=target,
    )
    value["phase"] = phase
    _write_receipt(path, value, replace=True)
    return value


def _validate_owned_directory(path: Path, owner: int, *, label: str) -> None:
    try:
        info = path.lstat()
    except OSError as exc:
        raise TransactionError(f"cannot inspect {label}: {exc}") from exc
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != owner:
        raise TransactionError(f"unsafe {label}: {path}")


def _marker_matches(target: Path, transaction: str, owner: int) -> bool:
    marker = marker_path(target)
    if not marker.exists() and not marker.is_symlink():
        return False
    _validate_regular(marker, mode=0o600, owner=owner)
    try:
        return marker.read_text(encoding="utf-8") == transaction + "\n"
    except OSError as exc:
        raise TransactionError(f"cannot read candidate marker: {exc}") from exc


def _remove_pending_marker(target: Path, transaction: str, owner: int) -> None:
    pending = marker_temp_path(target, transaction)
    if not pending.exists() and not pending.is_symlink():
        return
    _validate_regular(pending, mode=0o600, owner=owner)
    # This exact derived sibling is scratch only: it becomes authoritative
    # solely through an atomic rename into the candidate directory. SIGKILL
    # during its write may leave any prefix, so recovery validates its inode,
    # owner, mode, and name but deliberately does not require complete content.
    pending.unlink()
    _fsync_dir(pending.parent)


def _remove_derived_backup(
    backup: Path,
    owner: int,
    *,
    expected_dev: int,
    expected_ino: int,
) -> None:
    _validate_owned_directory(backup, owner, label="derived rollback backup")
    info = backup.lstat()
    if (info.st_dev, info.st_ino) != (expected_dev, expected_ino):
        raise TransactionError("rollback backup identity changed")
    shutil.rmtree(backup)
    _fsync_dir(backup.parent)


def _clear_receipt(path: Path, owner: int) -> None:
    _validate_regular(path, mode=0o600, owner=owner)
    path.unlink()
    _fsync_dir(path.parent)


def _fsync_tree(root: Path, owner: int) -> None:
    _validate_owned_directory(root, owner, label="replacement target")
    directories: list[Path] = []
    for current, dirnames, filenames in os.walk(root, topdown=True, followlinks=False):
        current_path = Path(current)
        directories.append(current_path)
        for name in list(dirnames):
            child = current_path / name
            info = child.lstat()
            if stat.S_ISLNK(info.st_mode):
                dirnames.remove(name)
            elif not stat.S_ISDIR(info.st_mode):
                raise TransactionError(f"unexpected non-directory in venv tree: {child}")
        for name in filenames:
            child = current_path / name
            info = child.lstat()
            if stat.S_ISLNK(info.st_mode):
                continue
            if not stat.S_ISREG(info.st_mode):
                raise TransactionError(f"unexpected special file in venv tree: {child}")
            flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
            fd = os.open(str(child), flags)
            try:
                opened = os.fstat(fd)
                if (opened.st_dev, opened.st_ino) != (info.st_dev, info.st_ino):
                    raise TransactionError(f"venv file changed during fsync: {child}")
                os.fsync(fd)
            finally:
                os.close(fd)
    for directory in reversed(directories):
        _fsync_dir(directory)
    _fsync_dir(root.parent)


def _tree_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        relative = path.relative_to(root).as_posix()
        if relative == ".vq-venv-transaction":
            continue
        info = path.lstat()
        mode = stat.S_IMODE(info.st_mode)
        if stat.S_ISLNK(info.st_mode):
            kind = b"L"
            payload = os.readlink(path).encode("utf-8", "surrogateescape")
        elif stat.S_ISDIR(info.st_mode):
            kind = b"D"
            payload = b""
        elif stat.S_ISREG(info.st_mode):
            kind = b"F"
            payload = path.read_bytes()
        else:
            raise TransactionError(f"unexpected special file in venv tree: {path}")
        digest.update(kind)
        digest.update(relative.encode("utf-8", "surrogateescape"))
        digest.update(b"\0")
        digest.update(f"{mode:o}".encode())
        digest.update(b"\0")
        digest.update(payload)
        digest.update(b"\0")
    return digest.hexdigest()


def command_begin(args: argparse.Namespace) -> None:
    checkout = _canonical_checkout(args.checkout)
    target = _canonical_target(args.target)
    receipt = receipt_path(target)
    transaction = secrets.token_hex(16)
    backup = backup_path(target, transaction)
    candidate_stage = candidate_staging_path(target, transaction)
    if backup.exists() or backup.is_symlink():
        raise TransactionError(f"derived backup path already exists: {backup}")
    if candidate_stage.exists() or candidate_stage.is_symlink():
        raise TransactionError(
            f"derived candidate staging path already exists: {candidate_stage}"
        )
    had_original = target.exists()
    owner = target.lstat().st_uid if had_original else os.geteuid()
    if owner != os.geteuid():
        raise TransactionError("direct replacement target is not owned by this user")
    original = target.lstat() if had_original else None
    value: dict[str, Any] = {
        "schema": SCHEMA,
        "kind": KIND,
        "transaction": transaction,
        "checkout": str(checkout),
        "target": str(target),
        "owner_uid": owner,
        "had_original": had_original,
        "original_dev": original.st_dev if original is not None else None,
        "original_ino": original.st_ino if original is not None else None,
        "candidate_dev": None,
        "candidate_ino": None,
        "target_tree_sha256": None,
        "phase": "armed",
    }
    _write_receipt(receipt, value, replace=False)
    candidate_stage.mkdir(mode=0o700)
    candidate = candidate_stage.lstat()
    _fsync_dir(candidate_stage.parent)
    value["candidate_dev"] = candidate.st_dev
    value["candidate_ino"] = candidate.st_ino
    _write_receipt(receipt, value, replace=True)
    print(transaction)


def command_start(args: argparse.Namespace) -> None:
    checkout = _canonical_checkout(args.checkout)
    target = _canonical_target(args.target)
    receipt = receipt_path(target)
    value = _read_receipt(
        receipt,
        expected_checkout=checkout,
        expected_target=target,
    )
    _remove_pending_receipt_update(receipt, value)
    if value["phase"] != "armed":
        raise TransactionError("replacement is not armed")
    transaction = value["transaction"]
    backup = backup_path(target, transaction)
    candidate_stage = candidate_staging_path(target, transaction)
    owner = value["owner_uid"]
    if value["candidate_dev"] is None or value["candidate_ino"] is None:
        raise TransactionError("candidate staging identity is not durable")
    _validate_owned_directory(candidate_stage, owner, label="candidate staging directory")
    candidate_info = candidate_stage.lstat()
    if (candidate_info.st_dev, candidate_info.st_ino) != (
        value["candidate_dev"], value["candidate_ino"]
    ) or any(candidate_stage.iterdir()):
        raise TransactionError("candidate staging directory changed")
    pending_marker = marker_temp_path(target, transaction)
    if pending_marker.exists() or pending_marker.is_symlink():
        raise TransactionError("derived pending candidate marker already exists")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(str(pending_marker), flags, 0o600)
    try:
        os.fchmod(fd, 0o600)
        payload = (transaction + "\n").encode()
        offset = 0
        while offset < len(payload):
            offset += os.write(fd, payload[offset:])
        os.fsync(fd)
    finally:
        os.close(fd)
    _fsync_dir(pending_marker.parent)
    _write_phase(
        receipt,
        checkout=checkout,
        target=target,
        phase="building",
    )
    if value["had_original"]:
        _validate_owned_directory(target, owner, label="original virtualenv")
        original_now = target.lstat()
        if (original_now.st_dev, original_now.st_ino) != (
            value["original_dev"], value["original_ino"]
        ):
            raise TransactionError("original virtualenv identity changed before move")
        if backup.exists() or backup.is_symlink():
            raise TransactionError("derived rollback backup already exists")
        os.replace(target, backup)
        _fsync_dir(target.parent)
    elif target.exists() or target.is_symlink():
        raise TransactionError("new target appeared after transaction arm")
    os.replace(candidate_stage, target)
    _fsync_dir(target.parent)
    marker = marker_path(target)
    os.replace(pending_marker, marker)
    _fsync_dir(target)
    _fsync_dir(target.parent)


def _reconcile(
    receipt: Path,
    checkout: Path,
    target: Path,
    *,
    allow_commit: bool,
) -> str:
    value = _read_receipt(
        receipt,
        expected_checkout=checkout,
        expected_target=target,
    )
    _remove_pending_receipt_update(receipt, value)
    transaction = value["transaction"]
    phase = value["phase"]
    owner = value["owner_uid"]
    backup = backup_path(target, transaction)
    candidate_stage = candidate_staging_path(target, transaction)
    _remove_pending_marker(target, transaction, owner)
    if candidate_stage.exists() or candidate_stage.is_symlink():
        _validate_owned_directory(
            candidate_stage, owner, label="candidate staging directory"
        )
        candidate_info = candidate_stage.lstat()
        if value["candidate_dev"] is None:
            if any(candidate_stage.iterdir()):
                raise TransactionError("uncheckpointed candidate staging changed")
        elif (candidate_info.st_dev, candidate_info.st_ino) != (
            value["candidate_dev"], value["candidate_ino"]
        ) or any(candidate_stage.iterdir()):
            raise TransactionError("candidate staging directory changed")
        candidate_stage.rmdir()
        _fsync_dir(candidate_stage.parent)
    target_exists = target.exists() or target.is_symlink()
    backup_exists = backup.exists() or backup.is_symlink()

    def candidate_matches() -> bool:
        if not target_exists or target.is_symlink():
            return False
        info = target.lstat()
        return (
            value["candidate_dev"] is not None
            and (info.st_dev, info.st_ino)
            == (value["candidate_dev"], value["candidate_ino"])
        )

    if phase == "target_committed":
        if not allow_commit:
            raise TransactionError("commit recovery was not authorized")
        if not target_exists or target.is_symlink():
            raise TransactionError("committed target is missing or unsafe")
        if not candidate_matches():
            raise TransactionError("committed target identity changed")
        expected_tree = value.get("target_tree_sha256")
        if expected_tree is None or _tree_digest(target) != expected_tree:
            raise TransactionError("committed target content changed")
        marker = marker_path(target)
        if (marker.exists() or marker.is_symlink()) and not _marker_matches(
            target, transaction, owner
        ):
            raise TransactionError("committed target marker changed")
        if backup_exists:
            if not value["had_original"]:
                raise TransactionError("unexpected rollback backup for a new target")
            _remove_derived_backup(
                backup,
                owner,
                expected_dev=value["original_dev"],
                expected_ino=value["original_ino"],
            )
        if marker.exists():
            marker.unlink()
        # Repeat this barrier even when a previous process already unlinked
        # the marker. Its unlink may not have reached stable storage before a
        # crash, and the receipt is the last authority for that cleanup.
        _fsync_dir(target)
        _clear_receipt(receipt, owner)
        return "committed"

    if value["had_original"]:
        if backup_exists:
            _validate_owned_directory(backup, owner, label="derived rollback backup")
            backup_info = backup.lstat()
            if (backup_info.st_dev, backup_info.st_ino) != (
                value["original_dev"], value["original_ino"]
            ):
                raise TransactionError("rollback backup identity changed")
            if target_exists:
                if phase == "armed":
                    info = target.lstat()
                    if (info.st_dev, info.st_ino) == (
                        value["original_dev"],
                        value["original_ino"],
                    ):
                        raise TransactionError(
                            "rollback backup and original target both exist"
                        )
                if not candidate_matches():
                    raise TransactionError("replacement candidate identity changed")
                _validate_owned_directory(target, owner, label="replacement candidate")
                shutil.rmtree(target)
                _fsync_dir(target.parent)
            os.replace(backup, target)
            _fsync_dir(target.parent)
        elif target_exists:
            _validate_owned_directory(target, owner, label="original virtualenv")
            if _marker_matches(target, transaction, owner):
                raise TransactionError("candidate exists but rollback backup is missing")
            info = target.lstat()
            if (info.st_dev, info.st_ino) != (
                value["original_dev"],
                value["original_ino"],
            ):
                raise TransactionError("original target identity changed")
        else:
            raise TransactionError("both original target and rollback backup are missing")
    else:
        if backup_exists:
            raise TransactionError("unexpected rollback backup for a new target")
        if target_exists:
            if not candidate_matches():
                raise TransactionError("replacement candidate identity changed")
            _validate_owned_directory(target, owner, label="replacement candidate")
            shutil.rmtree(target)
            _fsync_dir(target.parent)
    _clear_receipt(receipt, owner)
    return "restored"


def command_recover(args: argparse.Namespace) -> None:
    checkout = _canonical_checkout(args.checkout)
    target = _canonical_target(args.target)
    result = _reconcile(
        receipt_path(target),
        checkout,
        target,
        allow_commit=True,
    )
    print(result)


def command_commit(args: argparse.Namespace) -> None:
    checkout = _canonical_checkout(args.checkout)
    target = _canonical_target(args.target)
    receipt = receipt_path(target)
    value = _read_receipt(
        receipt,
        expected_checkout=checkout,
        expected_target=target,
    )
    if value["phase"] != "building":
        raise TransactionError("replacement is not ready to commit")
    owner = value["owner_uid"]
    if not _marker_matches(target, value["transaction"], owner):
        raise TransactionError("replacement target marker is missing or changed")
    _fsync_tree(target, owner)
    value["target_tree_sha256"] = _tree_digest(target)
    _write_receipt(receipt, value, replace=True)
    _write_phase(
        receipt,
        checkout=checkout,
        target=target,
        phase="target_committed",
    )
    result = _reconcile(receipt, checkout, target, allow_commit=True)
    if result != "committed":
        raise TransactionError("replacement commit did not reach a terminal state")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name, function in (
        ("begin", command_begin),
        ("start", command_start),
        ("recover", command_recover),
        ("commit", command_commit),
    ):
        command = subparsers.add_parser(name, add_help=False)
        command.add_argument("checkout")
        command.add_argument("target")
        command.set_defaults(function=function)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        args.function(args)
    except (OSError, TransactionError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
