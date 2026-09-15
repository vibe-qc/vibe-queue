"""Per-SHA runtime slots for venv hosts.

The layout primitive behind "build in the background, never halt the queue".
Scheduler hosts (pbs-cluster, slurm-cluster) already work this way -- an immutable per-SHA
bundle plus an atomically flipped pointer -- which is why a runtime update there
needs no drain (see ``admin._update_scheduler_runtime_locked``). Venv hosts
still ``git pull`` and ``pip install -e`` **in place**, so a live interpreter's
own source is rewritten underneath it. A job paused across that update serves
already-imported modules from ``sys.modules`` while importing anything new off
the rewritten disk: two versions in one process, silently, with no error. That
was reproduced on 2026-07-26 and is why venv hosts still drain.

This module owns only the on-disk shape::

    <root>/
      releases/<40-hex-sha>/     one immutable runtime, built at this final path
      current  -> releases/<sha> the pointer a stable wrapper resolves
      previous -> releases/<sha> retained for rollback

Nothing here builds, installs, or talks to a host: those belong to the update
path that will call it. Keeping the layout separable is deliberate -- the
correctness risk is concentrated in the flip and in path handling, and both are
testable without a fleet.

See ``vibe-queue/docs/design_immutable_venv_runtimes.md``.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import re
import secrets
import shutil
import stat
import subprocess
import sys
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn

if TYPE_CHECKING:  # pragma: no cover - typing only
    from vq.config import VenvProgram

_SHA40 = re.compile(r"^[0-9a-f]{40}$")

CURRENT_LINK = "current"
PREVIOUS_LINK = "previous"
RELEASES_DIR = "releases"
IMMUTABLE_RUNTIME_MARKER = ".vq-immutable-runtime"
SLOT_STATE_MARKER = ".vq-runtime-slot-state"

_SLOT_BUILD_RECEIPT_SUFFIX = ".build.json"
_SLOT_STATE_KIND = "vq-runtime-slot"
_SLOT_BUILD_KIND = "vq-runtime-slot-build"
_MARKER_SCHEMA = 1
_TRANSACTION_ID = re.compile(r"^[0-9a-f]{32}$")


class RuntimeSlotError(RuntimeError):
    """A slot operation refused to proceed."""


@dataclass(frozen=True)
class SlotLayout:
    """Resolved paths for one program's slot root."""

    root: Path
    releases: Path
    current: Path
    previous: Path


def layout(root: str | Path) -> SlotLayout:
    """Resolve the slot paths under ``root``.

    ``root`` must be absolute: these paths are handed to a build that runs
    elsewhere, and a relative root would silently resolve against whatever
    working directory that build happened to inherit.
    """
    path = Path(root)
    if not path.is_absolute():
        raise RuntimeSlotError(f"slot root must be an absolute path, got {root!r}")
    path = _canonical_unlinked_path(path)
    if path.exists() or path.is_symlink():
        _assert_directory_not_symlink(path, "runtime-slot root")
        info = path.stat()
        if os.geteuid() != 0 and info.st_uid != os.geteuid():
            raise RuntimeSlotError(
                f"runtime-slot root is owned by uid {info.st_uid}, not the "
                f"caller uid {os.geteuid()}: {path}"
            )
        if stat.S_IMODE(info.st_mode) & 0o022:
            raise RuntimeSlotError(
                f"runtime-slot root is group/world writable: {path}"
            )
    paths = SlotLayout(
        root=path,
        releases=path / RELEASES_DIR,
        current=path / CURRENT_LINK,
        previous=path / PREVIOUS_LINK,
    )
    if paths.releases.exists() or paths.releases.is_symlink():
        _assert_releases_root(paths)
    return paths


def _canonical_unlinked_path(path: Path) -> Path:
    """Canonicalize an absolute path while refusing every symlink component."""
    normalized = Path(os.path.normpath(str(path)))
    current = Path(normalized.anchor)
    missing: list[str] = []
    parts = normalized.parts[1:]
    for index, component in enumerate(parts):
        candidate = current / component
        if candidate.exists() or candidate.is_symlink():
            try:
                info = candidate.lstat()
            except OSError as exc:
                raise RuntimeSlotError(
                    f"cannot inspect runtime-slot root component {candidate}"
                ) from exc
            if stat.S_ISLNK(info.st_mode):
                raise RuntimeSlotError(
                    f"refusing symlinked runtime-slot root component {candidate}"
                )
            if not stat.S_ISDIR(info.st_mode):
                raise RuntimeSlotError(
                    f"runtime-slot root component is not a directory: {candidate}"
                )
            current = candidate.resolve(strict=True)
            continue
        missing = list(parts[index:])
        if sys.platform == "darwin":
            # Match the cross-language lifecycle-lock canonicalizer.  APFS is
            # commonly case-insensitive, and missing components have no stored
            # spelling for realpath(3) to recover; lowercase aliases must not
            # select different lock or generation paths before creation.
            missing = [component.lower() for component in missing]
        break
    for component in missing:
        current /= component
    return current


def _assert_runtime_root(path: Path) -> None:
    _assert_directory_not_symlink(path, "runtime-slot root")
    info = path.stat()
    if os.geteuid() != 0 and info.st_uid != os.geteuid():
        raise RuntimeSlotError(
            f"runtime-slot root is owned by uid {info.st_uid}, not the caller "
            f"uid {os.geteuid()}: {path}"
        )
    if stat.S_IMODE(info.st_mode) & 0o022:
        raise RuntimeSlotError(f"runtime-slot root is group/world writable: {path}")


def _assert_releases_root(paths: SlotLayout) -> None:
    """Bind ``releases`` to the validated root before following generations."""
    _assert_runtime_root(paths.root)
    _assert_directory_not_symlink(paths.releases, "runtime-slot releases root")
    root_info = paths.root.lstat()
    releases_info = paths.releases.lstat()
    if releases_info.st_uid != root_info.st_uid:
        raise RuntimeSlotError(
            f"runtime-slot releases root has the wrong owner: {paths.releases}"
        )
    if stat.S_IMODE(releases_info.st_mode) & 0o022:
        raise RuntimeSlotError(
            f"runtime-slot releases root is group/world writable: {paths.releases}"
        )


def slot_path(root: str | Path, sha: str) -> Path:
    """Path of the slot for ``sha``.

    The SHA is validated rather than interpolated: it reaches here from a
    release report and from remote command output, and a value containing a
    path separator would otherwise place a "slot" anywhere on the host.
    """
    if not isinstance(sha, str) or not _SHA40.match(sha):
        raise RuntimeSlotError(
            f"runtime slot needs a full 40-character lowercase hex SHA, got {sha!r}"
        )
    return layout(root).releases / sha


def resolve_current(root: str | Path) -> str | None:
    """SHA the ``current`` pointer names, or None when unset.

    Returns None rather than raising for an absent pointer (a host that has not
    been migrated yet) but refuses a ``current`` that is a real directory: that
    is an in-place install, and treating it as a slot root would let a flip
    delete a live runtime.
    """
    paths = layout(root)
    if not paths.current.is_symlink():
        if paths.current.exists():
            raise RuntimeSlotError(
                f"{paths.current} is not a symlink; this looks like an in-place "
                "install rather than a slot root. Migrate it before enabling "
                "runtime slots."
            )
        return None
    return _pointer_sha(paths, paths.current)


def exec_current_python(
    root: str | Path,
    arguments: list[str],
    *,
    expected_sha: str | None = None,
) -> NoReturn:
    """Replace the launcher with one exact, activated slot's interpreter.

    Resolve the pointer once. A concurrent activation can select a different
    runtime for the next launch, but cannot retarget this command. In particular,
    do not realpath the interpreter: a normal venv symlinks its Python binary
    to the base installation, and executing that target loses the venv.

    Activation verifies the content seals. Launch checks that same generation's
    verified markers and structural paths, without hashing gigabytes of native
    dependencies for each job. This relies on the immutable-generation contract;
    it is not an integrity audit of owner-modified runtime bytes.
    """
    paths = layout(root)
    if expected_sha is not None:
        slot_path(paths.root, expected_sha)  # validate before reading the pointer
    sha = resolve_current(paths.root)
    if sha is None:
        raise RuntimeSlotError(f"no active runtime slot at {paths.root}")
    if expected_sha is not None and sha != expected_sha:
        raise RuntimeSlotError(
            f"active runtime SHA mismatch: expected {expected_sha}, got {sha}"
        )
    source = slot_source(paths.root, sha)
    python = slot_python(paths.root, sha)
    for directory in (source, python.parent.parent, python.parent):
        _assert_directory_not_symlink(directory, "runtime launch directory")
    if not python.is_file() or not os.access(python, os.X_OK):
        raise RuntimeSlotError(f"runtime-slot interpreter is not executable: {python}")
    env = dict(os.environ)
    env.update(
        VQ_RUNTIME_SLOT_SHA=sha,
        VQ_RUNTIME_SLOT_SOURCE=str(source),
        VQ_RUNTIME_SLOT_PYTHON=str(python),
    )
    os.execve(str(python), [str(python), *arguments], env)


def create_slot(root: str | Path, sha: str) -> Path:
    """Create (or reuse) the slot directory for ``sha`` and return it.

    Reuse is deliberate: a retried deployment for the same SHA should land in
    the same place rather than accumulate near-duplicates. The caller owns what
    goes inside; an existing slot is NOT cleared here, because a running job may
    be executing from it.
    """
    target = slot_path(root, sha)
    if target.is_dir() and _verified_markers_match(root, sha):
        raise RuntimeSlotError(
            f"refusing to reopen verified immutable runtime slot {sha[:12]}"
        )
    target.mkdir(parents=True, exist_ok=True)
    return target


def begin_slot_build(
    root: str | Path,
    sha: str,
    *,
    transaction_id: str,
    in_use: Callable[[], set[str] | frozenset[str]],
) -> bool:
    """Reserve the final absolute generation path for an immutable build.

    Returns ``True`` when the caller must build the slot and ``False`` when a
    previously verified slot can be reused without writing it.  A failed build
    is retryable only when its sibling receipt and in-slot state marker agree
    on the exact prior transaction, and only after ``current``, ``previous``,
    and every non-terminal job spec prove that the SHA is not live.

    The build happens at ``releases/<sha>`` rather than in a renameable staging
    directory because virtualenv shebangs and editable-install paths embed the
    absolute generation path.
    """
    if not _TRANSACTION_ID.fullmatch(transaction_id):
        raise RuntimeSlotError("runtime-slot transaction id is malformed")
    paths = layout(root)
    target = slot_path(root, sha)
    receipt = _slot_build_receipt(root, sha)

    paths.releases.mkdir(parents=True, exist_ok=True)
    _assert_releases_root(paths)

    if target.is_symlink():
        raise RuntimeSlotError(f"refusing symlinked runtime slot {target}")
    if target.exists() and not target.is_dir():
        raise RuntimeSlotError(f"runtime slot path is not a directory: {target}")

    if target.is_dir() and _verified_markers_match(root, sha):
        if receipt.exists() or receipt.is_symlink():
            old_receipt = _read_exact_marker(
                receipt,
                expected_kind=_SLOT_BUILD_KIND,
                expected_id=sha,
                expected_states={"building"},
            )
            verified = _read_verified_state(target / SLOT_STATE_MARKER, sha)
            if old_receipt["transaction"] != verified["transaction"]:
                raise RuntimeSlotError(
                    f"runtime slot {sha[:12]} has a stale mismatched build receipt"
                )
            _unlink_and_sync(receipt)
        return False

    if target.exists() or receipt.exists() or receipt.is_symlink():
        old_receipt = _read_exact_marker(
            receipt,
            expected_kind=_SLOT_BUILD_KIND,
            expected_id=sha,
            expected_states={"building"},
        )
        if target.is_dir():
            state_path = target / SLOT_STATE_MARKER
            if not state_path.exists() and not state_path.is_symlink():
                if any(target.iterdir()):
                    raise RuntimeSlotError(
                        f"runtime slot {sha[:12]} lacks its in-slot build receipt"
                    )
                state = old_receipt
            else:
                state = _read_exact_marker(
                    state_path,
                    expected_kind=_SLOT_BUILD_KIND,
                    expected_id=sha,
                    expected_states={"building"},
                )
            if state["transaction"] != old_receipt["transaction"]:
                raise RuntimeSlotError(
                    f"runtime slot {sha[:12]} has mismatched build receipts"
                )
            immutable = slot_python(root, sha).parent.parent / IMMUTABLE_RUNTIME_MARKER
            if immutable.exists() or immutable.is_symlink():
                if _immutable_marker_matches(immutable, sha):
                    seal_slot_build(
                        root,
                        sha,
                        transaction_id=str(state["transaction"]),
                    )
                    return False
                raise RuntimeSlotError(
                    f"runtime slot {sha[:12]} has an invalid immutable marker"
                )

            referenced = {
                value for value in (resolve_current(root), _previous_sha(root)) if value
            }
            try:
                referenced.update(in_use())
            except RuntimeSlotError:
                raise
            except Exception as exc:
                raise RuntimeSlotError(
                    f"cannot prove failed slot {sha[:12]} is unused: {exc}"
                ) from exc
            if sha in referenced:
                raise RuntimeSlotError(
                    f"refusing to clean failed slot {sha[:12]} because it is "
                    "current, previous, or referenced by a non-terminal job"
                )
            shutil.rmtree(target)
            _fsync_directory(paths.releases)
        _unlink_and_sync(receipt)

    receipt_payload = _build_marker_payload(sha, transaction_id)
    _write_json_exclusive(receipt, receipt_payload)
    try:
        target.mkdir()
        _fsync_directory(paths.releases)
        _write_json_exclusive(
            target / SLOT_STATE_MARKER,
            receipt_payload,
        )
        _fsync_directory(target)
    except BaseException:
        # Keep the sibling receipt durable.  A retry can identify and clean the
        # exact transaction even if this process died between mkdir and marker.
        raise
    return True


def seal_slot_build(
    root: str | Path,
    sha: str,
    *,
    transaction_id: str,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> None:
    """Verify and seal a completed slot before it may be activated."""
    receipt = _slot_build_receipt(root, sha)
    state = _read_exact_marker(
        slot_path(root, sha) / SLOT_STATE_MARKER,
        expected_kind=_SLOT_BUILD_KIND,
        expected_id=sha,
        expected_states={"building"},
    )
    build_receipt = _read_exact_marker(
        receipt,
        expected_kind=_SLOT_BUILD_KIND,
        expected_id=sha,
        expected_states={"building"},
    )
    if (
        state["transaction"] != transaction_id
        or build_receipt["transaction"] != transaction_id
    ):
        raise RuntimeSlotError(
            f"runtime slot {sha[:12]} does not belong to this build transaction"
        )
    if _head_sha(slot_source(root, sha), runner=runner) != sha:
        raise RuntimeSlotError(
            f"runtime slot {sha[:12]} source changed before publication"
        )
    python = slot_python(root, sha)
    if not python.is_file() or not os.access(python, os.X_OK):
        raise RuntimeSlotError(
            f"runtime slot {sha[:12]} has no executable verified interpreter"
        )

    content_sha256 = venv_content_sha256(root, sha)
    source_sha256 = source_content_sha256(root, sha)
    immutable = python.parent.parent / IMMUTABLE_RUNTIME_MARKER
    if immutable.exists() or immutable.is_symlink():
        if not _immutable_marker_matches(immutable, sha):
            raise RuntimeSlotError(
                f"runtime slot {sha[:12]} has an invalid immutable marker"
            )
    else:
        _write_json_exclusive(
            immutable,
            {"schema": _MARKER_SCHEMA, "kind": _SLOT_STATE_KIND, "id": sha},
        )
    _seal_state(
        root,
        sha,
        transaction_id,
        content_sha256=content_sha256,
        source_content_sha256=source_sha256,
    )
    _unlink_and_sync(receipt)


def activate(root: str | Path, sha: str) -> str | None:
    """Point ``current`` at ``sha``, retaining the prior target as ``previous``.

    The flip is a single ``rename(2)`` onto ``current`` via a temporary symlink,
    never unlink-then-symlink: the latter leaves a window in which ``current``
    does not exist, and a job dispatched into that window would fail to find an
    interpreter at all. Returns the SHA that was previously current, or None.

    A running job is unaffected either way -- it already resolved its
    interpreter and holds an open path into its own slot, which this never
    touches.
    """
    paths = layout(root)
    target = slot_path(root, sha)
    if not target.exists() and not target.is_symlink():
        raise RuntimeSlotError(
            f"refusing to activate {sha}: {target} does not exist. Build the "
            "slot before flipping to it."
        )
    _assert_generation(layout(root), sha, verify_content=True)
    if not _verified_markers_match(root, sha):
        raise RuntimeSlotError(
            f"refusing to activate {sha}: the slot lacks exact verified "
            "immutable-runtime markers"
        )
    prior = resolve_current(root)
    if prior == sha:
        # A previous activation may have reached rename(2) and then reported a
        # directory-fsync failure.  Retrying must persist that already-visible
        # pointer rather than returning before the durability barrier.
        _fsync_directory(paths.root)
        return prior

    if prior is not None:
        _atomic_symlink(Path(RELEASES_DIR) / prior, paths.previous)
    _atomic_symlink(Path(RELEASES_DIR) / sha, paths.current)
    return prior


def rollback(root: str | Path) -> str | None:
    """Flip ``current`` back to whatever ``previous`` names.

    Returns the SHA rolled back to, or None when there is no retained previous
    slot (nothing to roll back to is not an error -- it is the state of a host
    that has only ever deployed once).
    """
    paths = layout(root)
    if not paths.previous.is_symlink():
        return None
    try:
        prior = _pointer_sha(paths, paths.previous)
        _assert_generation(paths, prior)
    except RuntimeSlotError as exc:
        raise RuntimeSlotError(
            "refusing to roll back: the retained slot is gone. Retention "
            "must never reclaim a slot that `previous` still names."
        ) from exc
    activate(root, prior)
    return prior


def list_slots(root: str | Path) -> list[str]:
    """Every built slot SHA under ``root``, newest-modified first."""
    releases = layout(root).releases
    if not releases.is_dir():
        return []
    slots = [
        entry
        for entry in releases.iterdir()
        if entry.is_dir() and not entry.is_symlink() and _SHA40.match(entry.name)
    ]
    slots.sort(key=lambda p: (p.stat().st_mtime_ns, p.name), reverse=True)
    return [entry.name for entry in slots]


def reclaimable(
    root: str | Path,
    *,
    in_use: set[str] | frozenset[str],
) -> list[str]:
    """Slots that may be deleted: not current, not previous, not in use.

    ``in_use`` is the caller's set of SHAs that live jobs are executing from.
    This function decides nothing about liveness -- it only refuses to propose a
    slot that is pointed at or declared in use. Reclamation is separated from
    the flip on purpose: deleting a slot a running job holds turns a silent
    version mix into a hard ImportError, which is better but still a broken run.
    """
    keep = {sha for sha in (resolve_current(root), _previous_sha(root)) if sha}
    keep |= set(in_use)
    return [sha for sha in list_slots(root) if sha not in keep]


def reclaim(
    root: str | Path,
    *,
    in_use: set[str] | frozenset[str],
) -> list[str]:
    """Delete every reclaimable slot; return what was removed."""
    removed: list[str] = []
    paths = layout(root)
    releases = paths.releases
    for sha in reclaimable(root, in_use=in_use):
        # Revalidate immediately before each destructive operation.  The root
        # is caller-owned and non-writable by other users, but this closes the
        # ordinary path-swap window as tightly as the path-based API permits.
        _assert_releases_root(paths)
        shutil.rmtree(releases / sha)
        # A successful cleanup must make the directory-entry removal durable.
        # Without the parent barrier, a crash can resurrect the generation
        # after we reported it reclaimed.
        _fsync_directory(releases)
        removed.append(sha)
    return removed


def _previous_sha(root: str | Path) -> str | None:
    paths = layout(root)
    if not paths.previous.is_symlink():
        if paths.previous.exists():
            raise RuntimeSlotError(
                f"{paths.previous} is not a managed runtime-slot symlink"
            )
        return None
    return _pointer_sha(paths, paths.previous)


def _pointer_sha(paths: SlotLayout, link: Path) -> str:
    raw = os.readlink(link)
    target = Path(raw)
    if target.is_absolute() or len(target.parts) != 2:
        raise RuntimeSlotError(
            f"{link} does not name an exact relative managed slot: {raw!r}"
        )
    releases, sha = target.parts
    if releases != RELEASES_DIR or not _SHA40.fullmatch(sha):
        raise RuntimeSlotError(
            f"{link} does not name an exact relative managed slot: {raw!r}"
        )
    _assert_generation(paths, sha)
    return sha


def _assert_generation(
    paths: SlotLayout,
    sha: str,
    *,
    verify_content: bool = False,
) -> None:
    _assert_releases_root(paths)
    generation = paths.releases / sha
    try:
        generation_info = generation.lstat()
        root_info = paths.root.lstat()
    except OSError as exc:
        raise RuntimeSlotError(
            f"managed runtime-slot generation is unavailable: {generation}"
        ) from exc
    if not stat.S_ISDIR(generation_info.st_mode):
        raise RuntimeSlotError(
            f"managed runtime-slot generation is not a real directory: {generation}"
        )
    if generation_info.st_uid != root_info.st_uid:
        raise RuntimeSlotError(
            f"managed runtime-slot generation has the wrong owner: {generation}"
        )
    if stat.S_IMODE(generation_info.st_mode) & 0o022:
        raise RuntimeSlotError(
            f"managed runtime-slot generation is group/world writable: {generation}"
        )
    if not _verified_markers_match(paths.root, sha):
        raise RuntimeSlotError(
            f"managed runtime-slot generation is not verified: {generation}"
        )
    if verify_content:
        python = slot_python(paths.root, sha)
        try:
            python_info = python.stat()
        except OSError as exc:
            raise RuntimeSlotError(
                f"managed runtime-slot interpreter is unavailable: {python}"
            ) from exc
        if not stat.S_ISREG(python_info.st_mode) or not os.access(python, os.X_OK):
            raise RuntimeSlotError(
                f"managed runtime-slot interpreter is not executable: {python}"
            )
        state = _read_verified_state(generation / SLOT_STATE_MARKER, sha)
        actual = venv_content_sha256(paths.root, sha)
        if actual != state["content_sha256"]:
            raise RuntimeSlotError(
                f"managed runtime-slot generation content changed: {generation}"
            )
        source_actual = source_content_sha256(paths.root, sha)
        if source_actual != state["source_content_sha256"]:
            raise RuntimeSlotError(
                f"managed runtime-slot source content changed: {generation}"
            )


def _slot_build_receipt(root: str | Path, sha: str) -> Path:
    return layout(root).releases / f".{sha}{_SLOT_BUILD_RECEIPT_SUFFIX}"


def _build_marker_payload(sha: str, transaction_id: str) -> dict[str, object]:
    return {
        "schema": _MARKER_SCHEMA,
        "kind": _SLOT_BUILD_KIND,
        "id": sha,
        "transaction": transaction_id,
        "state": "building",
    }


def _seal_state(
    root: str | Path,
    sha: str,
    transaction_id: str,
    *,
    content_sha256: str,
    source_content_sha256: str,
) -> None:
    _write_json_replace(
        slot_path(root, sha) / SLOT_STATE_MARKER,
        {
            "schema": _MARKER_SCHEMA,
            "kind": _SLOT_STATE_KIND,
            "id": sha,
            "transaction": transaction_id,
            "state": "verified",
            "content_sha256": content_sha256,
            "source_content_sha256": source_content_sha256,
        },
    )


def _verified_markers_match(root: str | Path, sha: str) -> bool:
    state_path = slot_path(root, sha) / SLOT_STATE_MARKER
    immutable = slot_python(root, sha).parent.parent / IMMUTABLE_RUNTIME_MARKER
    if not state_path.exists() and not state_path.is_symlink():
        return False
    payload = _read_json_regular(state_path)
    if payload.get("kind") == _SLOT_BUILD_KIND and payload.get("state") == "building":
        _read_exact_marker(
            state_path,
            expected_kind=_SLOT_BUILD_KIND,
            expected_id=sha,
            expected_states={"building"},
        )
        return False
    _read_verified_state(state_path, sha)
    return _immutable_marker_matches(immutable, sha)


def _read_verified_state(path: Path, sha: str) -> dict[str, object]:
    payload = _read_json_regular(path)
    if set(payload) != {
        "schema",
        "kind",
        "id",
        "transaction",
        "state",
        "content_sha256",
        "source_content_sha256",
    }:
        raise RuntimeSlotError(f"runtime-slot verified marker has invalid fields: {path}")
    if (
        payload.get("schema") != _MARKER_SCHEMA
        or payload.get("kind") != _SLOT_STATE_KIND
        or payload.get("id") != sha
        or payload.get("state") != "verified"
        or not isinstance(payload.get("transaction"), str)
        or not _TRANSACTION_ID.fullmatch(str(payload["transaction"]))
        or not isinstance(payload.get("content_sha256"), str)
        or not re.fullmatch(r"[0-9a-f]{64}", str(payload["content_sha256"]))
        or not isinstance(payload.get("source_content_sha256"), str)
        or not re.fullmatch(
            r"[0-9a-f]{64}", str(payload["source_content_sha256"])
        )
    ):
        raise RuntimeSlotError(f"runtime-slot verified marker is invalid: {path}")
    return payload


def venv_content_sha256(root: str | Path, sha: str) -> str:
    """Digest immutable venv bytes and symlink identities for activation."""
    venv = slot_python(root, sha).parent.parent
    _assert_directory_not_symlink(venv, "runtime-slot virtualenv")
    return _content_tree_sha256(
        venv,
        excluded_names={IMMUTABLE_RUNTIME_MARKER},
    )


def source_content_sha256(root: str | Path, sha: str) -> str:
    """Digest all source/build bytes that an editable runtime may import."""
    source = slot_source(root, sha)
    _assert_directory_not_symlink(source, "runtime-slot source")
    return _content_tree_sha256(source, excluded_names={".git", VENV_SUBDIR})


def _content_tree_sha256(path_root: Path, *, excluded_names: set[str]) -> str:
    digest = hashlib.sha256()
    try:
        root_info = path_root.lstat()
    except OSError as exc:
        raise RuntimeSlotError(
            f"runtime-slot content tree is unavailable: {path_root}"
        ) from exc
    if not stat.S_ISDIR(root_info.st_mode):
        raise RuntimeSlotError(
            f"runtime-slot content tree is not a real directory: {path_root}"
        )
    digest.update(b".\0D\0")
    digest.update(stat.S_IMODE(root_info.st_mode).to_bytes(2, "big"))
    digest.update(b"\0")

    entries = sorted(
        path
        for path in path_root.rglob("*")
        if not any(part in excluded_names for part in path.relative_to(path_root).parts)
        and "__pycache__" not in path.parts
        and path.suffix not in {".pyc", ".pyo"}
    )
    content_count = 0
    for path in entries:
        relative = path.relative_to(path_root).as_posix().encode("utf-8")
        digest.update(relative)
        digest.update(b"\0")
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode):
            digest.update(b"L\0")
            digest.update(os.readlink(path).encode("utf-8"))
            content_count += 1
        elif stat.S_ISDIR(info.st_mode):
            digest.update(b"D\0")
            digest.update(stat.S_IMODE(info.st_mode).to_bytes(2, "big"))
        else:
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise RuntimeSlotError(
                    f"runtime-slot tree contains unsafe file identity: {path}"
                )
            digest.update(b"F\0")
            digest.update(stat.S_IMODE(info.st_mode).to_bytes(2, "big"))
            digest.update(b"\0")
            digest.update(path.read_bytes())
            content_count += 1
        digest.update(b"\0")
    if content_count == 0:
        raise RuntimeSlotError(
            f"runtime-slot content tree contains no files: {path_root}"
        )
    return digest.hexdigest()


def _immutable_marker_matches(path: Path, sha: str) -> bool:
    payload = _read_json_regular(path)
    return payload == {
        "schema": _MARKER_SCHEMA,
        "kind": _SLOT_STATE_KIND,
        "id": sha,
    }


def _read_exact_marker(
    path: Path,
    *,
    expected_kind: str,
    expected_id: str,
    expected_states: set[str],
) -> dict[str, object]:
    payload = _read_json_regular(path)
    if set(payload) != {"schema", "kind", "id", "transaction", "state"}:
        raise RuntimeSlotError(f"runtime-slot marker has invalid fields: {path}")
    if (
        payload.get("schema") != _MARKER_SCHEMA
        or payload.get("kind") != expected_kind
        or payload.get("id") != expected_id
        or payload.get("state") not in expected_states
        or not isinstance(payload.get("transaction"), str)
        or not _TRANSACTION_ID.fullmatch(str(payload["transaction"]))
    ):
        raise RuntimeSlotError(f"runtime-slot marker is invalid: {path}")
    return payload


def _read_json_regular(path: Path) -> dict[str, object]:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise RuntimeSlotError(f"runtime-slot marker is unavailable: {path}") from exc
    try:
        info = os.fstat(fd)
        named = os.stat(path, follow_symlinks=False)
        parent_uid = path.parent.lstat().st_uid
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_nlink != 1
            or (named.st_dev, named.st_ino) != (info.st_dev, info.st_ino)
            or info.st_uid != parent_uid
            or stat.S_IMODE(info.st_mode) & 0o022
        ):
            raise RuntimeSlotError(
                f"runtime-slot marker has unsafe identity, owner, or mode: {path}"
            )
        with os.fdopen(fd, "rb", closefd=True) as stream:
            raw = stream.read(8193)
            fd = -1
        if len(raw) > 8192:
            raise RuntimeSlotError(f"runtime-slot marker is too large: {path}")
        value = json.loads(raw.decode("utf-8"))
    except RuntimeSlotError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RuntimeSlotError(f"runtime-slot marker is malformed: {path}") from exc
    finally:
        if fd >= 0:
            os.close(fd)
    if not isinstance(value, dict):
        raise RuntimeSlotError(f"runtime-slot marker is malformed: {path}")
    return value


def _write_json_exclusive(path: Path, payload: dict[str, object]) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    data = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
    try:
        fd = os.open(path, flags, 0o600)
    except OSError as exc:
        raise RuntimeSlotError(f"refusing to replace runtime-slot marker {path}") from exc
    try:
        with os.fdopen(fd, "wb", closefd=True) as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        with contextlib.suppress(OSError):
            path.unlink()
        raise
    _fsync_directory(path.parent)


def _write_json_replace(path: Path, payload: dict[str, object]) -> None:
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}-{secrets.token_hex(8)}")
    _write_json_exclusive(tmp, payload)
    try:
        if path.is_symlink():
            raise RuntimeSlotError(f"refusing symlinked runtime-slot marker {path}")
        os.replace(tmp, path)
        _fsync_directory(path.parent)
    finally:
        with contextlib.suppress(OSError):
            tmp.unlink()


def _unlink_and_sync(path: Path) -> None:
    if path.is_symlink():
        raise RuntimeSlotError(f"refusing symlinked runtime-slot receipt {path}")
    with contextlib.suppress(FileNotFoundError):
        path.unlink()
        _fsync_directory(path.parent)


def _assert_directory_not_symlink(path: Path, label: str) -> None:
    try:
        info = path.lstat()
    except OSError as exc:
        raise RuntimeSlotError(f"cannot inspect {label}: {path}") from exc
    if not stat.S_ISDIR(info.st_mode):
        raise RuntimeSlotError(f"{label} is not a real directory: {path}")


def _fsync_directory(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError as exc:
        raise RuntimeSlotError(f"cannot open runtime-slot directory {path}") from exc
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _atomic_symlink(target: Path, link: Path) -> None:
    """Point ``link`` at ``target`` atomically.

    ``os.symlink`` cannot overwrite, so the swap goes through a uniquely named
    temporary in the same directory and one ``os.replace``. Same directory
    matters: ``rename(2)`` is only atomic within a filesystem.

    The target is stored RELATIVE to the link's directory so the whole slot root
    stays relocatable -- an absolute target would bake in a path that a
    differently-mounted host cannot resolve.
    """
    link.parent.mkdir(parents=True, exist_ok=True)
    tmp = link.parent / f".{link.name}.swap-{os.getpid()}"
    if tmp.is_symlink() or tmp.exists():
        tmp.unlink()
    os.symlink(target, tmp)
    os.replace(tmp, link)
    _fsync_directory(link.parent)


SOURCE_SUBDIR = "source"
VENV_SUBDIR = ".venv"


def slot_source(root: str | Path, sha: str) -> Path:
    """Checkout directory inside ``sha``'s slot."""
    return slot_path(root, sha) / SOURCE_SUBDIR


def slot_python(root: str | Path, sha: str) -> Path:
    """Interpreter inside ``sha``'s slot."""
    return slot_path(root, sha) / SOURCE_SUBDIR / VENV_SUBDIR / "bin" / "python"


def slot_local_program(
    prog: VenvProgram, root: str | Path, sha: str
) -> VenvProgram:
    """A copy of ``prog`` whose paths name ``sha``'s slot directly.

    This is what lets the ordinary update path run *inside* a slot: point a
    program's ``git_dir`` and ``python`` at the slot, and every existing step --
    fetch, checkout, tag verification, update script, import check -- works
    unchanged, on a tree no running job is importing from.

    **The paths name the slot, never the ``current`` symlink, and that is a
    correctness requirement rather than a preference.** A venv is bound by
    absolute path to the tree it was created in: its editable ``.pth`` holds a
    literal path. If that path resolved through ``current``, flipping ``current``
    would change a RUNNING job's source out from under it -- reintroducing
    exactly the in-place mutation slots exist to prevent, and silently. So the
    interpreter a job holds must be the slot's own, and ``current`` is resolved
    only by the launcher at exec time, when it picks which slot to run.

    For the same reason a slot cannot be seeded by copying or hardlinking
    another slot: the copy's venv would still import the original's source. Each
    slot creates its own venv in place. See
    ``docs/design_immutable_venv_runtimes.md`` §0.
    """
    source = slot_source(root, sha)
    python = slot_python(root, sha)
    for path in (source, python):
        if CURRENT_LINK in path.parts:
            raise RuntimeSlotError(
                f"refusing to derive a slot-local program through "
                f"{CURRENT_LINK!r} ({path}): a venv records an absolute path, so "
                "a runtime resolved through the pointer would follow a later "
                "flip and change a running job's source underneath it"
            )
    # `runtime_slot_root` is cleared: a slot-local program IS the slot, so
    # handing it back to the slot-aware update path would recurse forever.
    return prog.model_copy(
        update={
            "git_dir": str(source),
            "python": str(python),
            "runtime_slot_root": None,
        }
    )


def materialize_source(
    live_git_dir: str | Path,
    root: str | Path,
    sha: str,
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> Path:
    """Populate ``sha``'s slot with a checkout at exactly that commit.

    Reuses the host's existing objects first. If the requested commit is newer
    than that checkout, fetches it into the unpublished slot from the live
    checkout's configured origin. The live checkout and its refs stay untouched.
    Already available commits need no network access (offline hosts may use a
    push-fed mirror).

    ``--local`` hardlinks the object store, and here that is safe where it was
    NOT safe for a venv: git objects are immutable and content-addressed, never
    rewritten in place, and a hardlink keeps the slot's own reference alive even
    if the source repository later prunes or repacks. A venv, by contrast,
    records absolute paths and cannot be shared at all -- see
    :func:`slot_local_program`.

    Idempotent: a slot already checked out at ``sha`` is left alone, so a retried
    deployment does not re-clone. The checkout is verified against ``sha``
    afterwards and the function fails closed on any mismatch -- a slot holding
    the wrong commit would be published under a SHA it does not contain, which
    is the one failure this whole design cannot tolerate.

    Does NOT create the venv or build anything; that is the update script's job,
    run against :func:`slot_local_program`'s derived program.
    """
    source = slot_source(root, sha)
    if _head_sha(source, runner=runner) == sha:
        return source

    create_slot(root, sha)
    if not source.exists():
        proc = runner(
            [
                "git",
                "clone",
                "--local",
                "--no-checkout",
                str(Path(live_git_dir)),
                str(source),
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=1800,
        )
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "").strip()
            raise RuntimeSlotError(
                f"could not clone {live_git_dir} into slot {sha[:12]}: {detail}"
            )

    available = runner(
        ["git", "-C", str(source), "cat-file", "-e", f"{sha}^{{commit}}"],
        capture_output=True, text=True, check=False, timeout=60,
    )
    if available.returncode != 0:
        origin = runner(
            ["git", "-C", str(Path(live_git_dir)), "remote", "get-url", "origin"],
            capture_output=True, text=True, check=False, timeout=60,
        )
        origin_url = (origin.stdout or "").strip()
        if origin.returncode != 0 or not origin_url:
            raise RuntimeSlotError(
                f"could not check out {sha[:12]}: commit is absent locally and "
                "the live checkout has no readable configured origin"
            )
        # Relative filesystem remotes are relative to the live repository,
        # not to the new slot. Leave URL and SCP-style transports verbatim.
        if ":" not in origin_url:
            origin_path = Path(origin_url).expanduser()
            if not origin_path.is_absolute():
                origin_path = Path(live_git_dir) / origin_path
            origin_url = str(origin_path.resolve())
        configured = runner(
            ["git", "-C", str(source), "remote", "set-url", "origin", origin_url],
            capture_output=True, text=True, check=False, timeout=60,
        )
        if configured.returncode != 0:
            raise RuntimeSlotError("could not configure the unpublished slot's origin")
        fetched = runner(
            ["git", "-C", str(source), "-c", "gc.auto=0", "-c",
             "maintenance.auto=false", "fetch", "--no-tags",
             "--no-recurse-submodules", "origin", sha],
            capture_output=True, text=True, check=False, timeout=1800,
        )
        if fetched.returncode != 0:
            detail = (fetched.stderr or fetched.stdout or "").strip()
            raise RuntimeSlotError(
                f"could not fetch exact commit {sha[:12]} into unpublished "
                f"runtime slot: {detail}; live runtime unchanged"
            )

    proc = runner(
        ["git", "-C", str(source), "checkout", "--detach", sha],
        capture_output=True,
        text=True,
        check=False,
        timeout=600,
    )
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeSlotError(
            f"could not check out {sha[:12]} in unpublished slot: {detail}; "
            "live runtime unchanged"
        )

    landed = _head_sha(source, runner=runner)
    if landed != sha:
        raise RuntimeSlotError(
            f"slot {sha[:12]} checked out {landed or '(unknown)'} instead; "
            "refusing to publish a slot that does not contain its own commit"
        )
    return source


def _head_sha(
    source: Path,
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]],
) -> str | None:
    if not (source / ".git").exists():
        return None
    proc = runner(
        ["git", "-C", str(source), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    if proc.returncode != 0:
        return None
    head = (proc.stdout or "").strip()
    return head if _SHA40.match(head) else None


def slots_in_use(root: str | Path, specs: Iterable[Any]) -> set[str]:
    """Slot SHAs that non-terminal jobs are executing from.

    A command containing an exact ``<root>/releases/<sha>/`` path proves which
    generation it holds.  Normal submitted specs may instead retain the stable
    ``current`` or wrapper path that was resolved only at exec time.  After a
    later flip, that durable command cannot prove the resolved SHA.  In that
    case every existing generation is retained: leaking disk until all such
    jobs are terminal is safe, while guessing can delete an executing runtime.

    Terminal jobs are ignored because their process is gone.  Any non-terminal
    spec without an exact generation path is treated as potentially slot-backed
    rather than ignored.
    """
    releases_prefix = f"{layout(root).releases}{os.sep}"
    found: set[str] = set()
    has_unresolved_live_spec = False
    for spec in specs:
        if getattr(spec, "is_terminal", False):
            continue
        matched = False
        for arg in getattr(spec, "command", None) or ():
            if not isinstance(arg, str) or not arg.startswith(releases_prefix):
                continue
            tail = arg[len(releases_prefix) :]
            candidate = tail.split("/", 1)[0]
            if _SHA40.fullmatch(candidate):
                found.add(candidate)
                matched = True
        if not matched:
            has_unresolved_live_spec = True
    if has_unresolved_live_spec:
        return set(list_slots(root))
    return found
