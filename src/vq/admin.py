"""Admin verbs: queue-managed environment refresh.

v0.5.20 ships the minimal ``vq admin update <env>`` verb. It looks up
``<env>`` in the local host's ``[programs.X]`` registry (must be a
``kind = "venv"`` program), pauses every running job in the queue,
runs ``git pull`` in the env's ``git_dir``, runs the ``update_script``
if one is configured, then resumes any paused jobs.

Why pause first: a job in flight may have ``import vibeqc`` modules
already loaded from the old commit; a rebuild that swaps shared objects
underneath an active process is a recipe for segfaults. Pausing
guarantees no Python code is executing in the affected venv during the
rebuild. After resume, paused jobs continue with the OLD bytecode they
already imported — they're unaffected by the new build; only NEW jobs
get the freshly-built env. (This is also the recommended manual recipe;
the verb just makes it one command.)

Why try/finally: a SIGINT or a network failure mid-pull must not strand
jobs in SUSPENDED. ``resume_all`` runs in a ``finally`` block, so the
worst case is "you typed Ctrl-C, git pull didn't finish, but the queue
is back up." The user retries the update when ready.

v0.5.20 shipped the minimal verb (single host, single env). Followups
landed across v0.5.24–v0.5.44: ``--tag`` verification (v0.5.24),
``vq admin status`` (v0.5.25), ``--all`` multi-env (v0.5.28),
``--all-hosts`` fleet sweep (v0.5.37), vq self-update auto-restart
(v0.5.42 / v0.5.43 symlink fix), and the
admin-update-in-progress marker file (v0.5.44) — see the v0.6.0
design pin in docs/roadmap_history.md for the trail.
"""
from __future__ import annotations

import base64
import binascii
import codecs
import contextlib
import copy
import fcntl
import hashlib
import json
import logging
import math
import os
import plistlib
import posixpath
import re
import secrets
import shlex
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import uuid
import zipfile
import zipimport
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field, fields
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from urllib.parse import unquote, urlsplit

from vq import (
    admin_detached,
    config,
    drain,
    fleet_operation,
    output,
    paths,
    runtime_slots,
    transport,
)
from vq.pause_resume import (
    pause_token_scope_with_proof,
    prove_pause_token_absent,
    resume_token_scope_with_proof,
)
from vq.spec import JobSpec, JobState, utcnow_iso

log = logging.getLogger(__name__)


OUTCOME_OK = "ok"
OUTCOME_ALREADY_CURRENT = "already-current"
OUTCOME_LOCKED = "locked"
OUTCOME_MARKER_PRESENT = "marker-present"
OUTCOME_PRECONDITION_FAILED = "precondition-failed"
OUTCOME_FAILED = "failed"

ADMIN_OUTCOMES: tuple[str, ...] = (
    OUTCOME_OK,
    OUTCOME_ALREADY_CURRENT,
    OUTCOME_LOCKED,
    OUTCOME_MARKER_PRESENT,
    OUTCOME_PRECONDITION_FAILED,
    OUTCOME_FAILED,
)
"""The closed set of admin-verb classifications, and the stable half of this
contract.

An unattended orchestration has to choose between wait, skip, acknowledge and
stop. Before this it could only do that by matching on prose, and every chain
written during the 2026-09 fleet migration ended up with a line like::

    grep -q "local checkout mutation lock" "$log" && { sleep 90; continue; }

-- load-bearing infrastructure spelled as a substring match on a sentence.

That sentence *was* pinned, by exactly one assertion in
``tests/test_self_update.py``, so vq's own CI would have caught a reword. The
protection stopped at the repository boundary: an orchestration greping a log
gets no signal at all, and simply stops matching. So the hazard is not that a
reword goes unnoticed -- vq notices -- but that vq is the only thing that
does, and every caller outside it silently falls through.

These values are pinned by a test. The messages beside them are free to
change, and one was, in the release that added this.
"""

ADMIN_OUTCOME_EXIT_CODES: dict[str, int] = {
    OUTCOME_OK: 0,
    OUTCOME_ALREADY_CURRENT: 0,
    OUTCOME_LOCKED: 75,
    OUTCOME_MARKER_PRESENT: 76,
    OUTCOME_PRECONDITION_FAILED: 77,
    OUTCOME_FAILED: 1,
}
"""Exit code per outcome, so a caller can branch without ``--json``.

``locked`` is 75 (``EX_TEMPFAIL``), whose established meaning is exactly
"temporary failure, retry later"; 76 and 77 continue that block.

``ok`` and ``already-current`` deliberately **share** 0 rather than taking
distinct codes. Both mean "continue", and every ``set -e`` wrapper and
``vq admin update x y && next`` idiom in existence treats non-zero as stop --
making a benign no-op non-zero would break far more callers than it informs.
A caller that must tell them apart reads ``outcome``, which is what it is for.
"""


def admin_outcome_for_exit_code(code: int) -> str | None:
    """The outcome a vq reported by exiting with ``code``, when that is unambiguous.

    A delegated ``vq admin update`` runs on the driver (or on the target host)
    and answers over SSH with an exit code. That vq classified the failure
    when it chose the code; relaying it lets the classification survive the
    hop, so ``vq admin update pbs-cluster`` exits 75 on a laptop exactly as it does
    on pbs-cluster's driver instead of folding into "remote vq failed (exit 75)".

    Only the retry / acknowledge / stop codes are recognised. 0 is not a
    failure, and 1 is also click's exit for every unclassified error, so
    neither says which outcome it was.
    """
    if code in (0, ADMIN_OUTCOME_EXIT_CODES[OUTCOME_FAILED]):
        return None
    for outcome, exit_code in ADMIN_OUTCOME_EXIT_CODES.items():
        if exit_code == code:
            return outcome
    return None


class AdminError(RuntimeError):
    """Raised on user-facing admin-update failures (unknown env, wrong
    program kind, git_dir not a checkout). Distinct from a clean run
    that ends with a non-zero rc from ``git pull`` or the update
    script — those return an UpdateResult with success=False."""

    outcome: str = OUTCOME_FAILED
    """Classification for a caller. Subclasses narrow it; see
    :data:`ADMIN_OUTCOMES`."""


class _ManagedGitAdmissionError(AdminError):
    """A read-only Git admission proof failed before daemon mutation."""


class AdminPreconditionFailed(AdminError):
    """A safety gate refused. The refusal is correct; do not retry or force.

    Distinct from :class:`AdminError` because the two need opposite handling.
    "the build failed" invites a retry after fixing the build; "this host is
    not converged, so I will not supersede its hold" is a *correct* answer
    that a retry cannot change and a force would defeat. Both were exit 1 plus
    prose, so an orchestration could not tell them apart.
    """

    outcome: str = OUTCOME_PRECONDITION_FAILED


class AdminUpdateInProgress(AdminError):
    """Raised when an admin-update-in-progress marker is already on
    disk and the caller didn't pass ``force=True``.

    A *subclass* of :class:`AdminError` so every existing
    ``except AdminError`` (and the test suite's
    ``pytest.raises(AdminError)``) keeps catching it. The distinct type
    exists so the CLI layer can tell this runtime/state condition — the
    argv was perfectly valid, a *prior* update just left a marker — apart
    from the genuine input errors (unknown env, wrong kind). The CLI
    renders this as a plain ``click.ClickException`` (``Error: …``,
    exit 1) rather than a ``click.UsageError`` (which prints the
    ``Usage: vq admin update …`` banner + exit 2 and so misleads the
    operator into thinking they mistyped the command — e.g. that a
    delegated ``vq admin update --all localhost`` argv was malformed,
    the 2026-06-26 developer-host→build-host ``--all`` report).

    Classified :data:`OUTCOME_LOCKED` by default, which is right for the
    concurrency races most of its raise sites report ("marker changed while
    read", "marker appeared after admission began"): something else is
    operating, so retry. A *conflicting marker left by a previous run* is a
    different situation and raises :class:`AdminMarkerPresent`."""

    outcome: str = OUTCOME_LOCKED


class AdminMarkerPresent(AdminUpdateInProgress):
    """A previous operation left a marker that overlaps this request's scope.

    Nothing is running. The caller must acknowledge the marker and retry,
    which is neither "wait" nor "give up" -- and telling those apart without
    reading prose is the whole point of the distinction. developer-host's
    ``vibeview-dev`` marker sat for about five hours in the 2026-09 migration
    because an orchestration could see only exit 1 and a sentence, and a
    sentence that was neither a lock error nor a build error was treated as
    fatal.

    A subclass of :class:`AdminUpdateInProgress` so every existing
    ``except AdminUpdateInProgress`` keeps catching it."""

    outcome: str = OUTCOME_MARKER_PRESENT


GIT_PULL_TIMEOUT_SECONDS = 300
"""Max wall-clock for ``git pull``. 5 minutes is generous for a normal
fast-forward; if your network is slower than that you've got bigger
problems."""

SOURCE_SHA_MARKER_NAME = "SOURCE-SHA"
SOURCE_TREE_SHA256_NAME = "SOURCE-TREE-SHA256"
SOURCE_SHA_TREE_FIELD = "tree-sha256"
"""Marker field binding a ``SOURCE-SHA`` to the tree it was written against.

Optional second line, ``tree-sha256=<64-hex>``. Absent means a marker written
before this field existed: legacy, trusted, unbound (see
:func:`read_source_sha_marker`)."""
_FULL_SHA_RE = re.compile(r"[0-9a-fA-F]{40}")
_SHA256_RE = re.compile(r"[0-9a-fA-F]{64}")
_SCHEDULER_STAGE_GENERATION_RE = re.compile(
    r"[0-9a-fA-F]{40}-[0-9a-fA-F]{32}"
)
SCHEDULER_STAGE_GENERATIONS_TO_KEEP = 5

RUNTIME_SOURCE_STAGES_TO_KEEP = 3
"""How many runtime-source upload stages a program keeps on a build host.

Unlike a helper staging generation, one of these belongs to exactly one deploy:
the driver re-archives the exact SHA from git on demand, and nothing reads a
stage once the build has consumed it. So a successful deploy reclaims its own,
and this bounds what failed deploys leave behind for forensics. Before #61 a
slurm host held 235 of them, 26 GB, one per deploy since July, and the home
they share went over quota.

This is NOT the retention rule for ``STAGE_ROOT/generations`` helper stages,
which stay: another deployment, possibly on another host, may still be using an
older one. See docs/operations.md, "Managed helper updates retain staging
generations"."""


def source_sha_marker_path() -> Path:
    """Package-local immutable source marker for daemonless scheduler helpers."""
    import vq

    return Path(vq.__file__).resolve().parent / SOURCE_SHA_MARKER_NAME


@dataclass
class SourceShaMarkerStatus:
    """Why the installed marker did or did not yield a usable SHA.

    :func:`read_source_sha_marker` collapses every failure to ``None``, which is
    the right fail-closed contract for callers. Operators need the distinction:
    "no marker installed" and "the marker outlived its code" have different
    remedies, and telling them apart is what turns a two-hour hunt into a
    one-line fix.
    """

    path: Path
    present: bool
    sha: str | None = None
    recorded_sha: str | None = None
    recorded_tree_sha256: str | None = None
    actual_tree_sha256: str | None = None
    stale: bool = False


def _marker_tree_digest(marker: Path) -> str | None:
    """Digest the package directory the marker labels, or None if it cannot be
    computed. Never raises: an undigestible tree is not proof of anything."""
    try:
        return source_tree_sha256(marker.parent)
    except (AdminError, OSError):
        return None


def inspect_source_sha_marker(path: Path | None = None) -> SourceShaMarkerStatus:
    """Read the marker and say whether it still describes the code beside it."""
    marker = path or source_sha_marker_path()
    try:
        text = marker.read_text(encoding="utf-8")
    except (FileNotFoundError, OSError):
        return SourceShaMarkerStatus(path=marker, present=False)

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    first = lines[0] if lines else ""
    if not _FULL_SHA_RE.fullmatch(first):
        return SourceShaMarkerStatus(path=marker, present=True)
    recorded_sha = first.lower()

    recorded_tree: str | None = None
    for line in lines[1:]:
        field, _, value = line.partition("=")
        # Unknown fields are ignored, not rejected: a newer writer may record
        # more than this reader knows about.
        if field.strip() == SOURCE_SHA_TREE_FIELD and _SHA256_RE.fullmatch(value.strip()):
            recorded_tree = value.strip().lower()
            break

    if recorded_tree is None:
        # Legacy marker, written before the binding existed. Trusted as-is --
        # every helper deployed to date carries this shape, and `vq source-sha`
        # gates scheduler compatibility, so rejecting them would fail every
        # lane in the fleet at once.
        return SourceShaMarkerStatus(
            path=marker, present=True, sha=recorded_sha, recorded_sha=recorded_sha
        )

    actual_tree = _marker_tree_digest(marker)
    if actual_tree is not None and actual_tree != recorded_tree:
        return SourceShaMarkerStatus(
            path=marker,
            present=True,
            sha=None,
            recorded_sha=recorded_sha,
            recorded_tree_sha256=recorded_tree,
            actual_tree_sha256=actual_tree,
            stale=True,
        )
    return SourceShaMarkerStatus(
        path=marker,
        present=True,
        sha=recorded_sha,
        recorded_sha=recorded_sha,
        recorded_tree_sha256=recorded_tree,
        actual_tree_sha256=actual_tree,
    )


def read_source_sha_marker(path: Path | None = None) -> str | None:
    """Read the installed helper source marker, returning None when absent.

    The marker is deliberately separate from ``vq --version``. Scheduler helper
    compatibility needs the exact source commit that installed the helper, not
    just the public package version, because queue-dev changes can land multiple
    helper-affecting commits inside one vq version.

    Returns None when the marker records a tree digest that no longer matches
    the package beside it. The marker is written *after* ``pip install`` and so
    is absent from the wheel's ``RECORD``; pip removes only files it tracks, so
    an upgrade leaves the previous marker orphaned in the *new* package
    directory. Unbound, that stale file is indistinguishable from a correct one
    and the daemon reports the previous commit indefinitely.
    """
    return inspect_source_sha_marker(path).sha


def source_identity() -> dict[str, str | None]:
    """Answer version, package digest and SOURCE-SHA together.

    One remote round trip for ``vq doctor``'s scheduler-helper check, which
    otherwise asks ``source-tree-sha256`` and ``source-sha`` separately and
    pays a python start-up on the login node for each. Failures are fields,
    not exit codes: the point of the call is that the helper answers every
    question it can in one go, so a missing or stale marker must not hide
    the digest beside it. The wording of ``source_sha_error`` matches what
    ``vq source-sha`` prints for the same marker state.
    """
    from vq import __version__  # noqa: PLC0415 -- vq/__init__ is the version

    payload: dict[str, str | None] = {"version": __version__}
    try:
        payload["source_tree_sha256"] = source_tree_sha256()
        payload["source_tree_sha256_error"] = None
    except (AdminError, OSError) as exc:
        payload["source_tree_sha256"] = None
        payload["source_tree_sha256_error"] = str(exc)
    status = inspect_source_sha_marker()
    if status.stale:
        payload["source_sha"] = None
        payload["source_sha_error"] = (
            f"SOURCE-SHA marker at {status.path} claims {status.recorded_sha} "
            "but the package beside it has changed since (recorded tree "
            f"{status.recorded_tree_sha256}, actual {status.actual_tree_sha256})"
        )
    elif status.sha is None:
        payload["source_sha"] = None
        payload["source_sha_error"] = "no SOURCE-SHA marker installed"
    else:
        payload["source_sha"] = status.sha
        payload["source_sha_error"] = None
    return payload


def write_source_sha_marker(sha: str, path: Path | None = None) -> Path:
    """Atomically write the package-local scheduler-helper source marker.

    Records the digest of the package directory as it stands *now*, binding the
    claim to the code it describes. Write side strict, read side tolerant.
    """
    if not _FULL_SHA_RE.fullmatch(sha):
        raise AdminError("source SHA marker must be a full 40-character hex SHA")
    marker = path or source_sha_marker_path()
    marker.parent.mkdir(parents=True, exist_ok=True)
    # Digest before staging the temp file, which lands in this same directory
    # and would otherwise be hashed into a value the read side can never
    # reproduce. Both marker names are excluded from the digest itself
    # (`source_tree_sha256`), so writing the marker does not invalidate it.
    tree_digest = _marker_tree_digest(marker)
    body = sha.lower() + "\n"
    if tree_digest is not None:
        body += f"{SOURCE_SHA_TREE_FIELD}={tree_digest}\n"
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=marker.parent, delete=False
    ) as fh:
        tmp = Path(fh.name)
        fh.write(body)
    try:
        # NamedTemporaryFile creates at 0600 and os.replace preserves it. On a
        # root-written /opt/vq install that leaves the marker unreadable to the
        # operators who need it, and `read_source_sha_marker` reports the
        # resulting PermissionError as "no marker installed" -- a compat gate
        # failing closed on a lie. Match the world-readable package beside it.
        os.chmod(tmp, 0o644)
        os.replace(tmp, marker)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise
    return marker


def current_source_sha(
    start: Path | None = None, *, require_tracked: bool = False
) -> str | None:
    """Return the full git SHA for the currently running vq source tree.

    Git discovers a repository by walking *up* from the anchor, so a
    non-editable install that merely sits somewhere inside an unrelated work
    tree answers with that tree's HEAD -- a SHA describing code this process is
    not running. Pass ``require_tracked=True`` to demand that the anchor is
    actually tracked by the repository git found, which is the only thing that
    makes the answer a statement about *these* bytes.
    """
    anchor = (start or Path(__file__)).resolve()
    git_dir = anchor if anchor.is_dir() else anchor.parent
    try:
        proc = _mutating_git_run(
            ["git", "-C", str(git_dir), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (subprocess.TimeoutExpired, OSError):
        return None
    if proc.returncode != 0:
        return None
    sha = proc.stdout.strip().lower()
    if not _FULL_SHA_RE.fullmatch(sha):
        return None
    if require_tracked and not _anchor_is_tracked(git_dir, anchor):
        return None
    return sha


def _anchor_is_tracked(git_dir: Path, anchor: Path) -> bool:
    """Is ``anchor`` a path the repository at ``git_dir`` actually tracks?"""
    target = "." if anchor.is_dir() else anchor.name
    try:
        proc = subprocess.run(
            ["git", "-C", str(git_dir), "ls-files", "--error-unmatch", target],
            capture_output=True,
            text=True,
            timeout=30,
            stdin=subprocess.DEVNULL,
        )
    except (subprocess.TimeoutExpired, OSError):
        return False
    return proc.returncode == 0


def source_tree_sha256(root: Path | None = None) -> str:
    """Return a content-derived digest for the installed ``vq`` package.

    ``SOURCE-SHA`` is declarative provenance and can be copied or overwritten.
    This digest is computed from the package files that will actually execute,
    making it suitable for scheduler-helper post-update verification.
    Generated bytecode/cache files and the provenance markers themselves are
    excluded so editable and regular installs hash identically.
    """
    if root is None:
        import vq

        root = Path(vq.__file__).resolve().parent
    root = root.resolve()
    if not root.is_dir():
        raise AdminError(f"vq source tree is not a directory: {root}")
    digest = hashlib.sha256()
    files = sorted(
        path
        for path in root.rglob("*")
        if path.is_file()
        and "__pycache__" not in path.parts
        and path.suffix not in {".pyc", ".pyo"}
        and path.name not in {SOURCE_SHA_MARKER_NAME, SOURCE_TREE_SHA256_NAME}
    )
    if not files:
        raise AdminError(f"vq source tree contains no files: {root}")
    for path in files:
        relative = path.relative_to(root).as_posix().encode("utf-8")
        digest.update(relative)
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_driver_recovery_archive(target: Path) -> str:
    """Write a deterministic zip-importable copy of this exact vq package."""
    import vq

    root = Path(vq.__file__).resolve().parent
    files = sorted(
        path
        for path in root.rglob("*")
        if path.is_file()
        and "__pycache__" not in path.parts
        and path.suffix not in {".pyc", ".pyo"}
        and path.name not in {SOURCE_SHA_MARKER_NAME, SOURCE_TREE_SHA256_NAME}
    )
    if not files:
        raise AdminError(f"vq source tree contains no files: {root}")
    with zipfile.ZipFile(
        target,
        "w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=9,
    ) as archive:
        for path in files:
            if path.is_symlink():
                raise AdminError(f"vq source tree contains a symlink: {path}")
            relative = path.relative_to(root).as_posix()
            info = zipfile.ZipInfo(
                f"vq/{relative}",
                date_time=(1980, 1, 1, 0, 0, 0),
            )
            info.compress_type = zipfile.ZIP_DEFLATED
            info.create_system = 3
            info.external_attr = (stat.S_IFREG | 0o444) << 16
            archive.writestr(info, path.read_bytes())
    return _sha256_file(target)


def require_staged_driver_recovery_archive(expected_sha256: str) -> None:
    """Prove this process imported vq from the exact staged helper archive."""
    if (
        type(expected_sha256) is not str
        or _SHA256_RE.fullmatch(expected_sha256) is None
        or expected_sha256 != expected_sha256.lower()
    ):
        raise AdminError("staged driver archive SHA-256 must be lowercase 64-hex")
    import vq

    loader = getattr(vq, "__loader__", None)
    if type(loader) is not zipimport.zipimporter:
        raise AdminError(
            "staged driver recovery is valid only from an integrity-checked "
            "zip-imported vq runtime"
        )
    archive_name = getattr(loader, "archive", None)
    if type(archive_name) is not str or not archive_name:
        raise AdminError("staged driver recovery archive identity is unavailable")
    try:
        actual_sha256 = _sha256_file(Path(archive_name))
    except OSError as exc:
        raise AdminError(
            f"staged driver recovery archive is unreadable: {exc}"
        ) from exc
    if actual_sha256 != expected_sha256:
        raise AdminError(
            "staged driver recovery archive changed before receipt admission"
        )


def _validate_remote_recovery_auth(
    remote_auth_args: tuple[str, ...],
    stdin_data: str | None,
) -> None:
    if remote_auth_args == ():
        if stdin_data is not None:
            raise AdminError("remote recovery stdin requires --token-stdin")
        return
    if remote_auth_args == ("--token-stdin",):
        if (
            type(stdin_data) is not str
            or not stdin_data.endswith("\n")
            or "\n" in stdin_data[:-1]
        ):
            raise AdminError("remote recovery token stdin is malformed")
        return
    if (
        len(remote_auth_args) == 2
        and remote_auth_args[0] == "--token-file"
        and type(remote_auth_args[1]) is str
        and Path(remote_auth_args[1]).is_absolute()
        and stdin_data is None
    ):
        return
    raise AdminError("remote recovery authentication arguments are invalid")


def _remove_driver_recovery_stage(
    host_cfg: config.HostConfig,
    host: str,
    remote_archive: str,
    remote_stage: str,
) -> None:
    for argv, label in (
        (("rm", "-f", "--", remote_archive), "archive"),
        (("rmdir", "--", remote_stage), "directory"),
    ):
        try:
            cleaned = transport.run_remote_shell(
                host_cfg,
                *argv,
                check=False,
                timeout=transport.DEFAULT_REMOTE_SHELL_TIMEOUT_SECONDS,
            )
        except transport.RemoteError as exc:
            log.warning(
                "could not remove staged driver recovery %s on %s: %s",
                label,
                host,
                exc,
            )
            continue
        if cleaned.returncode != 0:
            log.warning(
                "could not remove staged driver recovery %s on %s",
                label,
                host,
            )


def recover_remote_managed_update_with_driver_runtime(
    cfg: config.Config,
    host: str,
    *,
    marker_id: str | None,
    remote_auth_args: tuple[str, ...],
    stdin_data: str | None,
    as_json: bool,
) -> str:
    """Run current receipt recovery through an older remote vq interpreter.

    The remote installation is not edited or replaced. A unique, temporary
    zip of the exact running package is uploaded and hashed before its path is
    placed first on ``PYTHONPATH`` for one recovery invocation. The hidden
    digest argument makes an old CLI fail before receipt mutation if the
    staged package is not actually the one imported.
    """
    try:
        host_cfg = cfg.host(host)
    except config.ConfigError as exc:
        raise AdminError(str(exc)) from exc
    if not posixpath.isabs(host_cfg.remote_vq):
        raise AdminError(
            "--with-driver-runtime requires an absolute configured remote_vq "
            "path so the old interpreter identity is explicit"
        )
    if marker_id is None or re.fullmatch(r"[0-9a-f]{32}", marker_id) is None:
        raise AdminError(
            "--with-driver-runtime requires an exact 32-lowercase-hex marker ID"
        )
    _validate_remote_recovery_auth(remote_auth_args, stdin_data)

    remote_stage = f"vqscratch/vq-driver-recovery/{uuid.uuid4().hex}"
    remote_archive = f"{remote_stage}/vq-driver.pyz"
    stage_created = False
    mutation_ambiguous = False
    with tempfile.TemporaryDirectory(prefix="vq-driver-recovery-") as raw:
        local_archive = Path(raw) / "vq-driver-recovery.pyz"
        archive_sha256 = _write_driver_recovery_archive(local_archive)
        try:
            mkdir = transport.run_remote_shell(
                host_cfg,
                "mkdir",
                "-p",
                "--",
                remote_stage,
                check=False,
                timeout=transport.DEFAULT_REMOTE_SHELL_TIMEOUT_SECONDS,
            )
            if mkdir.returncode != 0:
                detail = _combined_output(mkdir.stdout, mkdir.stderr).strip()
                raise AdminError(
                    "could not create remote driver recovery stage: "
                    + (detail or "mkdir failed")
                )
            stage_created = True
            transport.upload_file(
                host_cfg,
                local_archive,
                remote_archive,
                retry_transient=1,
            )
            verify = transport.run_remote_shell(
                host_cfg,
                "sha256sum",
                remote_archive,
                check=False,
                timeout=transport.DEFAULT_REMOTE_SHELL_TIMEOUT_SECONDS,
                retry_transient=1,
            )
            expected_line = f"{archive_sha256}  {remote_archive}"
            if verify.returncode != 0 or verify.stdout.strip() != expected_line:
                raise AdminError(
                    "staged driver recovery archive digest mismatch before "
                    "remote receipt mutation"
                )
            remote_args = [
                "/usr/bin/env",
                "PYTHONDONTWRITEBYTECODE=1",
                "PYTHONNOUSERSITE=1",
                f"PYTHONPATH={remote_archive}",
                host_cfg.remote_vq,
                "admin",
                "recover-update",
                "localhost",
                "--marker-id",
                marker_id,
                "--staged-driver-archive-sha256",
                archive_sha256,
            ]
            if as_json:
                remote_args.append("--json")
            remote_args.extend(remote_auth_args)
            try:
                recovered = transport.run_remote_shell(
                    host_cfg,
                    *remote_args,
                    stdin_data=stdin_data,
                    retry_transient=0,
                    timeout=transport.DEFAULT_REMOTE_ADMIN_UPDATE_TIMEOUT_SECONDS,
                )
            except transport.RemoteOutcomeUnknown as exc:
                mutation_ambiguous = True
                raise transport.RemoteOutcomeUnknown(
                    f"{exc}\nThe integrity-checked recovery archive was retained "
                    f"at {host}:{remote_archive}; reconcile the marker before "
                    "any retry."
                ) from exc
            except transport.RemoteError:
                raise
            except BaseException:
                mutation_ambiguous = True
                raise
            return recovered.stdout
        finally:
            if stage_created and not mutation_ambiguous:
                _remove_driver_recovery_stage(
                    host_cfg,
                    host,
                    remote_archive,
                    remote_stage,
                )


def update_remote_managed_env_with_driver_runtime(
    cfg: config.Config,
    host: str,
    *,
    env: str,
    expected_sha: str,
    expected_tag: str | None,
    remote_auth_args: tuple[str, ...],
    stdin_data: str | None,
    as_json: bool,
    update_script_args: tuple[str, ...],
    show_output: bool,
    remote_timeout_env: Mapping[str, str],
    timeout: float | None,
    self_update: bool = False,
) -> str:
    """Execute one pinned direct-host update through the current package.

    The installed console supplies its interpreter; the authenticated archive
    supplies vq. Ordinary update owns service, checkout and receipt transitions.
    A lost observer never retries the mutation or removes its executing code.
    """
    try:
        host_cfg = cfg.host(host)
    except config.ConfigError as exc:
        raise AdminError(str(exc)) from exc
    if not posixpath.isabs(host_cfg.remote_vq):
        raise AdminError(
            "--with-driver-runtime requires an absolute configured remote_vq "
            "path so the old interpreter identity is explicit"
        )
    # Snapshot routing and credential selection before any transport. A caller
    # may reload or mutate Config while the archive is being staged (#677).
    config_file = config.config_path()
    try:
        config_bytes = config_file.read_bytes() if config_file.exists() else None
    except OSError as exc:
        raise AdminError("cannot snapshot driver config before update") from exc
    target_snapshot = copy.deepcopy(host_cfg.model_dump(mode="json"))
    topology_snapshot = {name: value.model_dump(mode="json") for name, value in cfg.hosts.items()}
    from .host import is_local_host

    if is_local_host(host) or is_local_host(host_cfg.ssh):
        raise AdminError("--with-driver-runtime requires a non-local direct host")
    if host_cfg.scheduler != "local":
        raise AdminError("--with-driver-runtime is not a scheduler-runtime update")
    if self_update and (
        env != "vibeqc-queue" or expected_tag is not None
        or update_script_args or show_output
    ):
        raise AdminError("driver bootstrap accepts only an exact self-update SHA")
    for value in cfg.hosts.values():
        driver = value.scheduler_driver
        if not self_update and (driver == host or (
            driver in cfg.hosts and cfg.hosts[driver].ssh == host_cfg.ssh
        )):
            raise AdminError("--with-driver-runtime refuses a scheduler_driver")
    if host_cfg.fleet_role in {"alias", "excluded"}:
        raise AdminError("--with-driver-runtime requires a canonical admitted host")
    if host_cfg.fleet_role == "vq-only" and env != "vibeqc-queue":
        raise AdminError("a vq-only host may update only vibeqc-queue")
    if not env or env.startswith("-"):
        raise AdminError("--with-driver-runtime requires one managed environment")
    if type(expected_sha) is not str or _FULL_SHA_RE.fullmatch(expected_sha) is None:
        raise AdminError("--with-driver-runtime requires --expected-sha FULL_SHA")
    allowed_timeouts = {"VQ_UPDATE_SCRIPT_TIMEOUT", "VQ_BUILD_STALL_TIMEOUT"}
    if set(remote_timeout_env) - allowed_timeouts:
        raise AdminError("unsupported staged update timeout environment")
    for value in remote_timeout_env.values():
        try:
            valid = math.isfinite(float(value)) and float(value) > 0
        except (ValueError, TypeError):
            valid = False
        if not valid:
            raise AdminError("invalid staged update timeout")
    host_cfg = copy.deepcopy(host_cfg)
    _validate_remote_recovery_auth(remote_auth_args, stdin_data)

    remote_stage = f"vqscratch/vq-driver-recovery/{uuid.uuid4().hex}"
    remote_archive = f"{remote_stage}/vq-driver.pyz"
    stage_created = False
    mutation_ambiguous = False
    with tempfile.TemporaryDirectory(prefix="vq-driver-recovery-") as raw:
        local_archive = Path(raw) / "vq-driver-recovery.pyz"
        archive_sha256 = _write_driver_recovery_archive(local_archive)
        try:
            mkdir = transport.run_remote_shell(
                host_cfg,
                "mkdir",
                "-p",
                "--",
                remote_stage,
                check=False,
                timeout=transport.DEFAULT_REMOTE_SHELL_TIMEOUT_SECONDS,
            )
            if mkdir.returncode != 0:
                detail = _combined_output(mkdir.stdout, mkdir.stderr).strip()
                raise AdminError(
                    "could not create remote driver recovery stage: "
                    + (detail or "mkdir failed")
                )
            stage_created = True
            transport.upload_file(
                host_cfg,
                local_archive,
                remote_archive,
                retry_transient=1,
            )
            verify = transport.run_remote_shell(
                host_cfg,
                "sha256sum",
                remote_archive,
                check=False,
                timeout=transport.DEFAULT_REMOTE_SHELL_TIMEOUT_SECONDS,
                retry_transient=1,
            )
            expected_line = f"{archive_sha256}  {remote_archive}"
            if verify.returncode != 0 or verify.stdout.strip() != expected_line:
                raise AdminError(
                    "staged driver recovery archive digest mismatch before "
                    "remote update mutation"
                )
            try:
                current_config_bytes = config_file.read_bytes() if config_file.exists() else None
            except OSError as exc:
                raise AdminError("cannot recheck driver config before update") from exc
            if (
                current_config_bytes != config_bytes
                or cfg.host(host).model_dump(mode="json") != target_snapshot
                or {name: value.model_dump(mode="json") for name, value in cfg.hosts.items()}
                != topology_snapshot
            ):
                raise AdminError("target config changed while staging driver update")
            remote_args = [
                "/usr/bin/env",
                "PYTHONDONTWRITEBYTECODE=1",
                "PYTHONNOUSERSITE=1",
                f"PYTHONPATH={remote_archive}",
                *(f"{key}={value}" for key, value in sorted(remote_timeout_env.items())),
                host_cfg.remote_vq,
                *(["self-update"] if self_update else ["admin", "update", env, "localhost"]),
            ]
            if expected_tag is not None:
                remote_args.extend(["--tag", expected_tag])
            remote_args.extend([
                "--expected-sha", expected_sha,
                "--staged-driver-archive-sha256", archive_sha256,
            ])
            for flag in update_script_args:
                remote_args.extend(["--update-script-arg", flag])
            if show_output:
                remote_args.append("--show-output")
            if as_json:
                remote_args.append("--json")
            remote_args.extend(remote_auth_args)
            try:
                recovered = transport.run_remote_shell(
                    host_cfg,
                    *remote_args,
                    stdin_data=stdin_data,
                    retry_transient=0,
                    timeout=timeout,
                )
            except transport.RemoteOutcomeUnknown as exc:
                mutation_ambiguous = True
                raise transport.RemoteOutcomeUnknown(
                    f"{exc}\nThe integrity-checked recovery archive was retained "
                    f"at {host}:{remote_archive}; reconcile the marker before "
                    "any retry."
                ) from exc
            except transport.RemoteError:
                raise
            except BaseException:
                mutation_ambiguous = True
                log.error(
                    "Update interrupted; driver archive retained at %s:%s; "
                    "reconcile the marker before any retry", host, remote_archive,
                )
                raise
            return recovered.stdout
        finally:
            if stage_created and not mutation_ambiguous:
                _remove_driver_recovery_stage(
                    host_cfg,
                    host,
                    remote_archive,
                    remote_stage,
                )


def running_source_sha(anchor: Path) -> str | None:
    """Resolve the source SHA that a running process should advertise.

    A tracked checkout is strongest because the anchor and commit are the same
    object.  A package-local installed marker outranks an enclosing untracked
    checkout; the latter remains the last-resort compatibility answer.  Any
    provenance failure returns ``None`` so daemon health remains available.
    """
    try:
        tracked = current_source_sha(anchor, require_tracked=True)
        if tracked is not None:
            return tracked
        return read_source_sha_marker() or current_source_sha(anchor)
    except Exception:  # noqa: BLE001 - provenance must not block health
        return None


def running_source_tree_sha256(root: Path) -> str | None:
    """Return the running package digest, or ``None`` when unavailable.

    Callers capture this once at process-service construction.  Re-reading it
    after an editable checkout advances could make a stale process appear to
    have restarted successfully.
    """
    try:
        return source_tree_sha256(root)
    except Exception:  # noqa: BLE001 - provenance must not block health
        return None


@dataclass
class SchedulerStagePruneResult:
    """Bounded cleanup result for immutable scheduler-helper generations."""

    stage_root: str
    keep: int
    preserve: str | None
    removed: list[str] = field(default_factory=list)
    retained: list[str] = field(default_factory=list)
    skipped_unrecognized: list[str] = field(default_factory=list)


def prune_scheduler_stage_generations(
    stage_root: Path,
    *,
    keep: int = SCHEDULER_STAGE_GENERATIONS_TO_KEEP,
    preserve: Path | None = None,
) -> SchedulerStagePruneResult:
    """Remove old recognized stage generations without touching other files."""
    if not stage_root.is_absolute():
        raise AdminError("scheduler stage root must be an absolute path")
    if keep < 1:
        raise AdminError("scheduler stage retention must be at least 1")
    generations = stage_root / "generations"
    if preserve is not None and not preserve.is_absolute():
        raise AdminError("preserved scheduler stage must be an absolute path")
    preserve_path = preserve
    if preserve_path is not None and preserve_path.parent != generations:
        raise AdminError("preserved scheduler stage is outside the generations root")
    result = SchedulerStagePruneResult(
        stage_root=str(stage_root),
        keep=keep,
        preserve=str(preserve_path) if preserve_path is not None else None,
    )
    if generations.is_symlink():
        raise AdminError("scheduler generations root must not be a symlink")
    _prune_stage_directory(
        generations, keep=keep, preserve=preserve_path, result=result,
    )
    return result


def _prune_stage_directory(
    parent: Path,
    *,
    keep: int,
    preserve: Path | None,
    result: SchedulerStagePruneResult,
) -> None:
    """Keep the newest ``keep`` recognized stages directly under ``parent``.

    "Recognized" is the exact ``<40 hex>-<32 hex>`` directory name both stage
    layouts use. Everything else under ``parent`` -- other files, other
    directories, and any symlink whatever its name -- is reported as skipped and
    left alone.
    """
    if not parent.is_dir():
        return

    recognized: list[Path] = []
    for candidate in parent.iterdir():
        if (
            candidate.is_symlink()
            or not candidate.is_dir()
            or not _SCHEDULER_STAGE_GENERATION_RE.fullmatch(candidate.name)
        ):
            result.skipped_unrecognized.append(str(candidate))
            continue
        recognized.append(candidate)
    recognized.sort(
        key=lambda path: (path.stat().st_mtime_ns, path.name),
        reverse=True,
    )

    retained: set[Path] = set()
    if preserve is not None and preserve in recognized:
        retained.add(preserve)
    for candidate in recognized:
        if len(retained) >= keep:
            break
        retained.add(candidate)
    for candidate in recognized:
        if candidate in retained:
            result.retained.append(str(candidate))
            continue
        shutil.rmtree(candidate)
        result.removed.append(str(candidate))


RUNTIME_SOURCE_STAGE_DIR_NAME = "runtime-source"
"""Basename of the runtime-source stage root, under ``<scratch>/.vq-admin/``."""


def prune_runtime_source_stages(
    stage_root: Path,
    *,
    keep: int = RUNTIME_SOURCE_STAGES_TO_KEEP,
    preserve: Path | None = None,
) -> SchedulerStagePruneResult:
    """Prune runtime-source upload stages, which have no ``generations`` level.

    ``stage_root`` is the ``runtime-source`` directory itself
    (``<scratch_root>/.vq-admin/runtime-source``); its children are one
    directory per program, each holding that program's stages. That layout is
    why :func:`prune_scheduler_stage_generations` could not see these and
    reported ``removed=0`` for them (#61).

    A deploy that verifies now reclaims its own stage, so this is the supported
    way to reclaim what older vq left behind, or what failed deploys kept. It
    does not touch helper staging generations, which are retained deliberately.
    """
    if not stage_root.is_absolute():
        raise AdminError("runtime source stage root must be an absolute path")
    if stage_root.name != RUNTIME_SOURCE_STAGE_DIR_NAME:
        raise AdminError(
            "runtime source stage root must be the "
            f"{RUNTIME_SOURCE_STAGE_DIR_NAME!r} directory itself, "
            f"got {stage_root.name!r}"
        )
    if keep < 1:
        raise AdminError("runtime source stage retention must be at least 1")
    if preserve is not None and not preserve.is_absolute():
        raise AdminError("preserved runtime source stage must be an absolute path")
    if preserve is not None and preserve.parent.parent != stage_root:
        raise AdminError(
            "preserved runtime source stage is outside the stage root"
        )
    result = SchedulerStagePruneResult(
        stage_root=str(stage_root),
        keep=keep,
        preserve=str(preserve) if preserve is not None else None,
    )
    if stage_root.is_symlink():
        raise AdminError("runtime source stage root must not be a symlink")
    if not stage_root.is_dir():
        return result
    for program_dir in sorted(stage_root.iterdir()):
        if program_dir.is_symlink() or not program_dir.is_dir():
            result.skipped_unrecognized.append(str(program_dir))
            continue
        _prune_stage_directory(
            program_dir,
            keep=keep,
            preserve=preserve if (
                preserve is not None and preserve.parent == program_dir
            ) else None,
            result=result,
        )
    return result


UPDATE_SCRIPT_TIMEOUT_SECONDS = 14400
"""Max wall-clock for the env's update_script. Four hours leaves useful
headroom for a cold vibe-qc rebuild with the current high-angular-momentum
libint recipe plus the remaining native dependencies and extensions on slower
fleet hosts. Override in the process running the update via
``VQ_UPDATE_SCRIPT_TIMEOUT`` (see :func:`_update_script_timeout`) when a host
needs a different site-specific cap. Delegated updates read this in the remote
CLI process, not the daemon."""

BUILD_STALL_TIMEOUT_SECONDS = 3600
"""v0.12.x: max wall-clock the update_script may produce **no output at
all** before we declare it wedged and reap it. This is the fast
wedge-catcher behind fix 1 of the 2026-06-26 fleet incident: a stuck
``build-env`` ran 12h+ with completely empty stdout/stderr while holding
6 CPUs (compute-b job 699efea5a802). The current libint build can spend
well over 15 minutes inside one large generation or compilation step without
printing another line, so the default allows one hour of silence while still
reaping the historical multi-hour wedge before the four-hour wall cap.
Override per-daemon via ``VQ_BUILD_STALL_TIMEOUT`` (seconds); set to ``0``
to disable stall detection and rely on the wall cap alone."""

BUILD_HEARTBEAT_INTERVAL_SECONDS = 120
"""v0.12.x: how often a running update_script logs a progress heartbeat
(elapsed + seconds-since-last-output) to the daemon journal AND the job
stdout. Turns the pre-fix "empty stdout for hours" into a visible
"still running (Ns elapsed, Ms since last output)" trail. Override via
``VQ_BUILD_HEARTBEAT_INTERVAL``; ``0`` disables the heartbeat."""

_BUILD_KILL_GRACE_SECONDS = 10.0
"""Grace between SIGTERM and SIGKILL when reaping a timed-out/stalled
build's process group. Mirrors the watchdog's SIGTERM->grace->SIGKILL
window (``Watchdog.grace_seconds``)."""

_BUILD_POLL_INTERVAL_SECONDS = 0.5
"""How often :func:`_run_monitored_build` wakes to re-check the wall /
stall / heartbeat clocks. Small enough that a sub-second test stall
timeout reaps promptly; large enough not to busy-spin a long build."""

SCHEDULER_HELPER_READINESS_MAX_ATTEMPTS = 6
"""Maximum scheduler-helper executions after a shared-filesystem install."""

SCHEDULER_HELPER_READINESS_STABLE_SUCCESSES = 2
"""Consecutive successful helper executions required before provenance."""

SCHEDULER_HELPER_READINESS_INTERVAL_SECONDS = 1.0
"""Bounded pause between scheduler-helper readiness probes."""

SCHEDULER_HELPER_ACTIVATION_MAX_ATTEMPTS = 10
"""Digest reads allowed while waiting for the site script's atomic flip.

vq stages source and runs the site's update command; the *site* owns extract →
install → atomically activate. A zero return code from that command means the
build succeeded, NOT that the login node has observed the activation: on a
shared NFS home the attribute cache can keep serving the pre-flip symlink
target for several seconds. A pre-flip helper is a perfectly good older vq — it
answers ``vq --version`` with rc=0 — so :func:`_wait_for_scheduler_helper_ready`
cannot see the difference, and a one-shot digest read lands on the old tree.

That is BUG 1 of the 2026-07-22 slurm-cluster report: a successful deploy
(``published_sha=6e7cd383…``, scheduler update rc=0) was reported FAILED, and an
independent ``vq doctor slurm-cluster --json`` seconds later returned ok=true. The
doctor was not reading a different path — it was, in effect, a manual retry.
So retry here until the flip settles, rather than treating the first read as
final.
"""

SCHEDULER_HELPER_ACTIVATION_INTERVAL_SECONDS = 3.0
"""Pause between activation-settle digest reads. With the attempt cap above
this is a ~27 s window: far longer than an NFS attribute-cache lifetime, far
shorter than the build that precedes it."""


def _build_stall_timeout() -> float:
    """No-output stall cap, honoring ``VQ_BUILD_STALL_TIMEOUT`` (seconds,
    ``>= 0``; ``0`` disables). Read at call time. Falls back to
    :data:`BUILD_STALL_TIMEOUT_SECONDS`."""
    raw = os.environ.get("VQ_BUILD_STALL_TIMEOUT", "").strip()
    if raw:
        try:
            val = float(raw)
        except ValueError:
            return float(BUILD_STALL_TIMEOUT_SECONDS)
        if math.isfinite(val) and val >= 0:
            return val
    return float(BUILD_STALL_TIMEOUT_SECONDS)


def _build_heartbeat_interval() -> float:
    """Heartbeat cadence, honoring ``VQ_BUILD_HEARTBEAT_INTERVAL``
    (seconds, ``>= 0``; ``0`` disables). Falls back to
    :data:`BUILD_HEARTBEAT_INTERVAL_SECONDS`."""
    raw = os.environ.get("VQ_BUILD_HEARTBEAT_INTERVAL", "").strip()
    if raw:
        try:
            val = float(raw)
        except ValueError:
            return float(BUILD_HEARTBEAT_INTERVAL_SECONDS)
        if math.isfinite(val) and val >= 0:
            return val
    return float(BUILD_HEARTBEAT_INTERVAL_SECONDS)


def _update_script_timeout() -> float:
    """Wall-clock cap for the update_script, honoring the
    ``VQ_UPDATE_SCRIPT_TIMEOUT`` env override (seconds, must be > 0).

    The four-hour default accommodates the current cold libint
    rebuilds; the operator can set this in the executing CLI's environment
    when a host needs a different cap. A delegated SSH update therefore reads
    the remote CLI environment, not the initiator's or daemon's environment.
    Read at call time and fall back to
    :data:`UPDATE_SCRIPT_TIMEOUT_SECONDS`."""
    raw = os.environ.get("VQ_UPDATE_SCRIPT_TIMEOUT", "").strip()
    if raw:
        try:
            val = float(raw)
        except ValueError:
            val = 0.0
        if math.isfinite(val) and val > 0:
            return val
    return float(UPDATE_SCRIPT_TIMEOUT_SECONDS)


# Arch/Manjaro install perl scripts (pod2man, used by libecpint's bundled
# libcerf to generate man pages) under /usr/bin/{core,vendor,site}_perl, which
# are NOT on the systemd-user daemon's minimal PATH. A cold native-dep rebuild
# then dies with "pod2man: command not found". Prepend whichever of these dirs
# exist; a no-op on distros that put pod2man directly on PATH.
_BUILD_PATH_DIRS: tuple[str, ...] = (
    "/usr/bin/core_perl",
    "/usr/bin/vendor_perl",
    "/usr/bin/site_perl",
)


def _configured_build_path_dirs() -> tuple[str, ...]:
    """Extra build PATH directories this host's config asks for.

    Best-effort on purpose: a build must not fail because the config could
    not be read, and the caller already loaded it to get here. Falling back
    to the built-in list loses a directory, which is the same outcome as not
    having configured one.
    """
    try:
        return tuple(config.load_config().build_path_dirs)
    except Exception:
        return ()


def _augment_build_path(env: dict[str, str]) -> None:
    """Prepend the build's extra PATH directories to ``env['PATH']`` in place.

    This is vq's answer to "the build needs a tool that only a *login* shell
    puts on PATH", and it is a deliberate choice over the alternatives -- see
    ``docs/fleet_update_runbook.md``. A remote command runs under a non-login
    shell, so on Arch/Manjaro ``/usr/bin/core_perl`` is absent and libecpint's
    vendored libcerf dies generating man pages with ``pod2man``. vq puts the
    directory on PATH for the build rather than sourcing a login profile,
    because it needs one directory and not a host's whole login environment.

    :data:`_BUILD_PATH_DIRS` covers the Arch perl layout that has actually
    bitten. ``build_path_dirs`` in the config covers the next one without a
    vq release: configured directories come first, so an operator's answer
    beats the built-in guess. A directory that does not exist is skipped, so
    the same config is safe on every host in a mixed fleet.
    """
    seen: set[str] = set()
    extra: list[str] = []
    for directory in (*_configured_build_path_dirs(), *_BUILD_PATH_DIRS):
        if directory not in seen and os.path.isdir(directory):
            seen.add(directory)
            extra.append(directory)
    if not extra:
        return
    cur = env.get("PATH", "")
    env["PATH"] = os.pathsep.join([*extra, *([cur] if cur else [])])


@dataclass
class UpdateResult:
    """Structured outcome of one ``admin update`` invocation. Caller
    reads ``.success`` for the boolean verdict; ``.format()`` /
    :func:`format_update_result` give the human-readable summary."""

    env: str
    git_dir: str
    branch: str | None
    update_script: str | None

    operation: str = "update"
    """Which verb produced this result: ``"update"`` or ``"install"``.

    They share this shape because they answer the same questions -- which
    commit, which script, what it printed, did it work -- but a summary
    headed "admin update" for a `vq admin install` misreports what happened,
    and the script it ran is the install_script."""

    post_update_script: str | None = None
    post_update_script_rc: int | None = None
    post_update_script_output: str = ""

    # Pause/resume summaries (always populated; pause_all/resume_all
    # never raise on partial failure, just report it in the summary).
    paused_summary: str = ""
    resumed_summary: str = ""

    # git pull
    git_pull_rc: int | None = None
    git_pull_output: str = ""  # stdout+stderr combined

    # update_script
    update_script_rc: int | None = None
    update_script_output: str = ""
    update_script_seconds: float | None = None
    """Wall-clock seconds the update_script ran for, or None if it did not.

    The scheduler-runtime lane has reported per-phase durations and cache
    hit rates through ``VQ-DEPLOY-METRIC`` since it landed; this lane -- which
    is most of the fleet -- reported nothing at all, so "did that update take
    three minutes or seventy" had no answer short of reading timestamps out
    of a transcript. You cannot make a build faster that you cannot time."""
    post_update_script_seconds: float | None = None
    """Wall-clock seconds the post_update_script ran for, or None."""
    already_current: bool = False
    """True when the requested target was already deployed and nothing ran.

    Distinct from ``success`` on purpose: both mean "continue", but only one
    of them touched the host. See :attr:`outcome`."""
    metrics: dict[str, str] = field(default_factory=dict)
    """``VQ-DEPLOY-METRIC key=value`` facts parsed from this lane's script
    output, the same way the scheduler-runtime lane parses them. Empty when
    the program's scripts emit none -- which is itself worth seeing, because
    it means the dependency-cache and ccache decisions are unreported here."""

    # v0.5.24: --tag verification
    expected_tag: str | None = None
    """The tag the user passed via ``--tag``. ``None`` means
    verification was skipped."""
    actual_tag: str | None = None
    """Tag actually checked out after pull. None if no exact-match tag
    found (HEAD doesn't point at any tag's commit) or verification was
    skipped. The ``tag_matches`` property compares this to
    ``expected_tag``."""
    tag_check_rc: int | None = None
    """Return code from ``git describe --exact-match --tags HEAD``.
    1 means no tag matches HEAD (legitimate state); other non-zero means
    git itself failed (no .git, etc.)."""

    expected_sha: str | None = None
    """The full commit SHA the user passed via ``--expected-sha``. ``None``
    means commit verification was skipped."""
    actual_sha: str | None = None
    """Full ``HEAD`` SHA observed during SHA verification, or ``None`` when
    the rev-parse failed or verification was skipped."""
    sha_check_rc: int | None = None
    """Return code from ``git rev-parse HEAD`` during SHA verification.
    ``None`` when verification was skipped."""

    # v0.7.1 *Lamport's Clock*: post-pull branch validation. Catches the
    # silent-branch-drift class surfaced 2026-05-25 (workstation vibeqc-dev
    # silently on `release` despite ``config.toml`` saying ``main``;
    # root-caused to a vibe-qc-side argv loss in
    # ``scripts/_safe_build_env.sh`` fixed in vibe-qc ``ea195796``).
    # See ``docs/v0_7_1_lamports_clock_design.md`` § Item 1.
    actual_branch: str | None = None
    """Branch HEAD points to after the pull (``git rev-parse
    --abbrev-ref HEAD``). None when ``VenvProgram.branch`` is unset
    (legacy envs where the operator manages branch by hand) or when the
    rev-parse call itself failed (no .git, etc.) — the latter is
    indistinguishable in practice and surfaces as
    ``branch_matches=False`` for the safe-failure path."""
    branch_check_rc: int | None = None
    """Return code from ``git rev-parse --abbrev-ref HEAD``. 0 on
    success (``actual_branch`` is set); non-zero means the rev-parse
    failed (rare — implies a broken .git directory). None when the
    branch check wasn't attempted (no ``VenvProgram.branch`` configured
    OR ``git pull`` failed, in which case we don't bother checking
    against a tree we couldn't refresh)."""

    # v0.7.1 *Lamport's Clock* Item 5: post-update dirty-tree signal.
    # ``dirty_after_update`` records ``git status --porcelain``'s
    # verdict after the pull+build. ``fail_on_dirty_in_effect`` mirrors
    # ``VenvProgram.fail_on_dirty`` so the success property can decide
    # whether dirty is a failure mode for this env. Both ``None`` when
    # the env has no configured branch / the git_pull failed (we don't
    # check a tree we couldn't refresh).
    dirty_after_update: bool | None = None
    fail_on_dirty_in_effect: bool = False

    # v0.12.x fix 3: atomic build (snapshot/restore + import gate). Makes
    # the {Python tree, native .so} pair atomic so a killed/failed rebuild
    # never leaves a newer-Python-against-older-.so env that imports broken
    # (the 2026-06-26 build-host ImportError). Armed only when the env config
    # has BOTH an update_script and an effective import check; managed vibe-qc
    # source layouts receive that check even when legacy config omitted it.
    pre_update_sha: str | None = None
    """HEAD SHA captured BEFORE the pull, used as the rollback target.
    None when the atomic machinery is disarmed (no effective import check) or
    the pre-pull rev-parse failed."""
    pre_update_branch: str | None = None
    """Symbolic branch checked out before an immutable selector update.

    ``None`` means the checkout was detached.  Rollback restores both this
    attachment state and :attr:`pre_update_sha`; restoring only the commit
    silently changes an attached checkout into a detached one."""
    rolled_back: bool = False
    """True iff the build failed (or the post-build import probe failed)
    and the checkout was git-reset back to ``pre_update_sha`` + the
    snapshotted ``.so`` restored. A rollback always means the update did
    NOT land — ``success`` is False."""
    rollback_summary: str = ""
    """Human-readable trail of the rollback (reset rc, artifacts restored,
    post-rollback import verdict). Empty when no rollback happened."""
    import_check_rc: int | None = None
    """Return code of the post-build ``python -c "import <import_check>"``
    probe. 0 = importable; non-zero = ABI-broken. None = probe not run
    (no effective import check, or no build attempted)."""
    import_check_output: str = ""
    """Captured stdout+stderr of the post-build import probe (carries the
    ImportError traceback tail when it fails)."""

    # Non-update-script errors that happened in the work block
    # (e.g. timeout, FileNotFoundError on the script path).
    work_errors: list[str] = field(default_factory=list)

    # v0.5.42: vq self-update auto-restart of vq-daemon. Populated only
    # when ``update_env`` ran with ``restart_daemon=True`` AND the env
    # being updated is the venv the running vq-daemon was launched from.
    # See :func:`_detect_vq_self_update` / :func:`_restart_vq_daemon`.
    daemon_restart_attempted: bool = False
    """True iff we detected this update as a vq self-update AND tried
    to restart the daemon. False when detection said "not vq's venv"
    or the caller passed ``--no-restart-daemon`` (or the update itself
    failed — never restart onto a half-installed package)."""
    daemon_restart_succeeded: bool | None = None
    """True iff the selected service manager restarted the daemon and the
    post-restart RPC/source-provenance gate passed. ``None`` when
    ``daemon_restart_attempted`` is False."""
    daemon_restart_message: str = ""
    """Human-readable line for the formatter: PID transition on success,
    error + recovery-recipe pointer on failure."""
    daemon_service_manager: str | None = None
    """Selected user service manager (``systemd`` or ``launchd``)."""
    daemon_expected_source_sha: str | None = None
    """Checkout SHA the restarted daemon is required to report over RPC."""
    daemon_actual_source_sha: str | None = None
    """Last source SHA observed from the daemon RPC health probe."""
    daemon_expected_source_tree_sha256: str | None = None
    """Digest of the package the updated interpreter now imports.

    ``daemon_expected_source_sha`` is ``git rev-parse HEAD`` of the *checkout*
    while ``daemon_actual_source_sha`` is whatever the *installed package*
    declares. Those are different objects, so their equality is the thing being
    assumed rather than the thing being proved. This pair compares bytes to
    bytes."""
    daemon_actual_source_tree_sha256: str | None = None
    """Tree digest the restarted daemon reported. ``None`` from a daemon
    predating the ping key -- absence is "unknown", never a mismatch."""
    daemon_health_verified: bool | None = None
    """True only when post-restart RPC and exact source provenance pass."""

    run_log_path: str | None = None
    """Transcript of this update: phase narration, heartbeats, and the full
    build output. Retrieve with ``vq admin logs``. A plain attribute assignment
    was not enough — the dataclass has to declare it or ``asdict()`` (and every
    JSON consumer downstream) silently drops it."""

    @property
    def tag_verification_attempted(self) -> bool:
        """True if the caller passed ``--tag`` and we tried to verify."""
        return self.expected_tag is not None

    @property
    def tag_matches(self) -> bool | None:
        """v0.5.24: True iff verification was attempted AND actual_tag
        matches expected_tag. ``None`` if verification wasn't attempted.
        Used by ``.success`` and by the formatter."""
        if not self.tag_verification_attempted:
            return None
        return self.actual_tag == self.expected_tag

    @property
    def sha_verification_attempted(self) -> bool:
        """True if the caller passed ``--expected-sha``."""
        return self.expected_sha is not None

    @property
    def sha_matches(self) -> bool | None:
        """True iff verification was attempted AND HEAD matches the expected
        full commit SHA. ``None`` if verification was not requested."""
        if not self.sha_verification_attempted:
            return None
        return self.actual_sha == self.expected_sha

    @property
    def branch_verification_attempted(self) -> bool:
        """v0.7.1: True iff ``VenvProgram.branch`` was non-None at
        update time AND we ran the rev-parse check (we only run it
        when git pull succeeded — checking a tree we couldn't refresh
        is misleading). Used by ``.success`` and by the formatter."""
        return self.branch_check_rc is not None

    @property
    def branch_matches(self) -> bool | None:
        """v0.7.1: True iff verification was attempted AND
        ``actual_branch`` matches ``self.branch`` (the configured
        ``VenvProgram.branch``). ``None`` if verification was skipped.
        Detached HEAD shows as ``actual_branch="HEAD"`` and is
        therefore a mismatch unless ``self.branch == "HEAD"`` (which
        no one configures, by construction)."""
        if not self.branch_verification_attempted:
            return None
        return self.actual_branch == self.branch

    @property
    def outcome(self) -> str:
        """This result's classification, from the closed set.

        ``already-current`` is set by the caller that decided no work was
        needed; a result that ran reports ``ok`` or ``failed``. The lock,
        marker and precondition classes never reach here -- those are raised
        before a result exists.
        """
        if self.already_current:
            return OUTCOME_ALREADY_CURRENT
        return OUTCOME_OK if self.success else OUTCOME_FAILED

    @property
    def success(self) -> bool:
        """Return the complete on-disk plus daemon-lifecycle verdict.

        An ``already_current`` result is a success: the host is in the state
        that was asked for. It reached that state earlier rather than now,
        which is what :attr:`outcome` says and what ``success`` deliberately
        does not -- a batch that counts failures must not count a confirmed
        no-op as one.
        """
        if self.already_current:
            return True
        if not self.work_succeeded:
            return False
        if self.daemon_restart_attempted:
            return (
                self.daemon_restart_succeeded is True
                and self.daemon_health_verified is True
            )
        return True

    @property
    def work_succeeded(self) -> bool:
        """Return the on-disk transaction verdict before daemon lifecycle.

        Pause/resume summaries are deliberately not part of this verdict.  A
        managed self-update uses it before it arms the restart fields, avoiding
        a circular dependency on health verification that has not run yet.
        """
        if self.work_errors:
            return False
        if self.git_pull_rc != 0:
            return False
        # update_script is optional; an unset script counts as
        # success (nothing to fail).
        if (
            self.update_script is not None
            and self.update_script_rc != 0
        ):
            return False
        if (
            self.post_update_script is not None
            and self.post_update_script_rc != 0
        ):
            return False
        # v0.5.24: if --tag was given, the HEAD must point at that tag.
        if self.tag_verification_attempted and not self.tag_matches:
            return False
        # v0.12.x: a blessed-SHA update must remain on the requested commit.
        if self.sha_verification_attempted and not self.sha_matches:
            return False
        # v0.7.1: if the env config pins a branch, HEAD must be on it.
        # The check only ran when git_pull succeeded (see
        # _do_update_work), so by the time we read it here a False
        # means a real branch drift rather than "we didn't get to it".
        if self.branch_verification_attempted and not self.branch_matches:
            return False
        # v0.7.1 Item 5: dirty-tree gate. Only flips success to False
        # when the env explicitly opted in via fail_on_dirty=True. The
        # check itself runs unconditionally (so the JSON consumer sees
        # dirty_after_update either way), but the verdict is opt-in.
        if self.fail_on_dirty_in_effect and self.dirty_after_update is True:
            return False
        # v0.12.x fix 3: atomic-build verdict. A rollback means the update
        # reverted to the prior commit — it did NOT land, so never report
        # success. A failed post-build import probe means the freshly built
        # env is ABI-broken; surface FAILED loudly so the operator / auto-
        # update never treats an importable-but-broken env as current.
        if self.rolled_back:
            return False
        return self.import_check_rc is None or self.import_check_rc == 0


@dataclass
class SchedulerHelperReadinessAttempt:
    """One post-install scheduler-helper execution probe."""

    attempt: int
    returncode: int | None
    transient_etxtbsy: bool
    output: str


@dataclass
class SchedulerHostUpdateResult:
    """Structured result for ``vq admin update <scheduler-host>``.

    Scheduler hosts are not ``programs``: they run no vq daemon and are
    refreshed by executing a configured remote provisioning command on the
    scheduler login node. Keep this result separate from :class:`UpdateResult`
    so git/tag/build fields do not pretend to exist for the cluster path.
    """

    host: str
    ssh: str
    scheduler: str
    mode: str
    command: str
    command_ssh: str | None = None
    command_rc: int | None = None
    command_output: str = ""
    stage_root: str | None = None
    stage_path: str | None = None
    stage_uploaded: bool = False
    archive_sha256: str | None = None
    stage_source: str | None = None
    """Provenance of the staged helper tree: ``"report-pin"`` when the caller
    supplied an exact ``--expected-sha`` (fleet rollouts always do — the
    accepted release report is the sole source of deployed identity), or
    ``"driver-tree"`` for a manual update staging the driver's own HEAD."""
    expected_source_sha: str | None = None
    remote_source_sha: str | None = None
    expected_source_tree_sha256: str | None = None
    remote_source_tree_sha256: str | None = None
    source_tree_rc: int | None = None
    source_tree_output: str = ""
    helper_readiness_verified: bool = False
    helper_readiness_attempts: list[SchedulerHelperReadinessAttempt] = field(
        default_factory=list
    )
    activation_wait_attempts: int = 0
    """Digest reads needed before the site script's atomic activation was
    observable from the login node. ``1`` = the flip had already landed when
    the deploy command returned; ``>1`` = vq waited it out instead of reporting
    a successful update as failed."""
    source_marker_rc: int | None = None
    source_marker_output: str = ""
    maintenance_warnings: list[str] = field(default_factory=list)
    active_jobs: list[str] = field(default_factory=list)
    work_errors: list[str] = field(default_factory=list)
    marker_cleared: bool = True
    run_log_path: str | None = None
    """Transcript of this operation: phase narration, heartbeats, and the full
    build output. Retrieve with ``vq admin logs``."""
    drain_wait_seconds: float = 0.0
    """Drain-wait budget the operator asked for (0 = one-shot refusal)."""
    drain_waited_seconds: float = 0.0
    """How long this update actually waited for the target to go quiet."""
    drain_lane_held: bool = False
    """True when vq added the scheduler drain lane itself (and so released it
    afterwards). False when an operator's pre-existing lane was left alone."""
    drain_skipped_reason: str | None = None
    """Why this update did not wait for running work, when it did not."""
    configured_command: str | None = None
    """The site's configured deploy argv, WITHOUT the per-run environment vq
    prepends. ``command`` carries a fresh stage path (with a uuid) on every
    run, so it can never match across runs; this is the stable identity the
    activation proof is keyed on."""
    activation: str | None = None
    """``"atomic"`` when the site script proved it published an immutable
    per-SHA helper root and switched the stable path by rename. ``None`` means
    unproven, which is not the same as "mutating" -- see
    :func:`helper_activation_is_proven`."""
    active_path: str | None = None
    """The per-SHA helper root the stable path resolved to after activation."""
    metrics: dict[str, str] = field(default_factory=dict)
    """Machine-readable deploy metrics parsed from ``VQ-DEPLOY-METRIC`` lines
    in the site script's transcript (dependency-cache decision, ccache hit
    rate, phase durations, native-rebuild flag + reason)."""

    @property
    def success(self) -> bool:
        return self.command_rc == 0 and not self.active_jobs and not self.work_errors

    @property
    def outcome(self) -> str:
        """This result's classification, from the closed set.

        A helper lane either ran, and reports ``ok`` or ``failed``, or was
        refused before a result existed: the lock, marker and precondition
        classes are raised, never returned. There is no ``already-current``
        here; the rollout planner proves that from the LAST OK record and
        does not call this lane at all.
        """
        return OUTCOME_OK if self.success else OUTCOME_FAILED


SCHEDULER_RUNTIME_STATUS_FILENAME = "scheduler-runtime-status.json"

SCHEDULER_HELPER_RECORD_PROGRAM = "vq-helper"
"""Pseudo-program key for the scheduler helper's canonical LAST OK record in
the scheduler-runtime status store. The helper is not a chemistry runtime,
but its deployed identity must be recorded and verified the same canonical
way so ``rollout-latest`` can prove "already at the accepted pin" without
comparing against the live driver checkout."""

_DEPLOY_METRIC_RE = re.compile(
    r"^VQ-DEPLOY-METRIC\s+([A-Za-z0-9_.-]+)=(.*)$", re.MULTILINE
)


def parse_deploy_metrics(output: str) -> dict[str, str]:
    """Extract ``VQ-DEPLOY-METRIC key=value`` lines from a deploy transcript.

    Site deploy scripts (pbs-cluster/slurm-cluster) emit one line per reportable fact:
    dependency-cache decision (+ the exact incompatibility on a cold
    rebuild), ccache availability and hit rate, and per-phase durations.
    Later lines win so a script can refine a metric as the deploy
    progresses.
    """
    return {
        match.group(1): match.group(2).strip()
        for match in _DEPLOY_METRIC_RE.finditer(output or "")
    }


@dataclass
class SchedulerRuntimeUpdateResult:
    """Structured outcome for one scheduler-host runtime deployment."""

    host: str
    program: str
    mode: str
    command: str
    command_ssh: str
    verify_command: str
    verify_ssh: str
    expected_sha: str
    expected_tag: str | None = None
    command_rc: int | None = None
    command_output: str = ""
    verify_rc: int | None = None
    verify_output: str = ""
    actual_sha: str | None = None
    actual_tag: str | None = None
    healthy: bool = False
    activation: str | None = None
    active_path: str | None = None
    health_detail: str | None = None
    quiescent: bool = False
    updater_pid: int | str | None = None
    active_jobs: list[str] = field(default_factory=list)
    work_errors: list[str] = field(default_factory=list)
    marker_cleared: bool = True
    mirror_fed: bool = False
    """True when this update pushed the exact pin into the target's push-fed
    source mirror (``feed_source_mirror``), replacing the manual feed step."""
    prepare_rc: int | None = None
    """Exit code of the login-host ``prepare_command``, when configured."""
    prepare_output: str = ""
    """Captured stdout+stderr of the login-host ``prepare_command``.

    Carried on the result, not only in the run log, because a prepare failure
    happens *before* the deploy and verify commands run: their sections
    render ``(no output)`` and without this the operator's whole evidence is
    ``prepare command rc=2``. Both of the 2026-09-10 pbs-cluster failures were one
    line of stderr -- ``vibeqc-release requires --tag`` and ``fatal: 'origin'
    does not appear to be a git repository`` -- and diagnosing either meant
    shelling in and re-running the preparer by hand."""
    staged_source_archive: str | None = None
    staged_source_sha256: str | None = None
    staged_source_stage: str | None = None
    """Remote directory this deploy uploaded its source archive into.

    Set as soon as the directory exists, so a deploy that fails partway through
    the upload still names the stage its own cleanup has to bound (#61)."""
    staged_source_stage_reclaimed: bool = False
    """True when this deploy removed its own upload staging.

    Only a successful deploy does: a failed one keeps its stage for forensics,
    bounded by :data:`RUNTIME_SOURCE_STAGES_TO_KEEP`."""
    staged_source_stages_reclaimed: int = 0
    """Stages removed for this program on the build host, this one included."""
    staged_source_stages_retained: int = 0
    """Stages left behind for this program after the reclaim."""
    staged_source_reclaim_error: str | None = None
    """Why the reclaim did not run or did not finish.

    Never a ``work_error``: failing to reclaim disk does not undo a deploy that
    verified, and turning a good deploy into a failed one over cleanup would be
    a worse outcome than the disk it leaves."""
    run_log_path: str | None = None
    """Transcript of this operation: phase narration, heartbeats, and the full
    build output. Retrieve with ``vq admin logs``."""
    drain_wait_seconds: float = 0.0
    """Drain-wait budget the operator asked for (0 = one-shot refusal)."""
    drain_waited_seconds: float = 0.0
    """How long this deployment actually waited for the target to go quiet."""
    drain_lane_held: bool = False
    """True when vq added the scheduler drain lane itself (and so released it
    afterwards). False when an operator's pre-existing lane was left alone."""
    drain_skipped_reason: str | None = None
    """Why this deployment did not drain or wait for running work. Always set
    for a runtime deployment: its contract stages out-of-place and activates
    atomically, so a running job keeps its own bundle and there is nothing to
    wait for. Surfaced so an operator reading a transcript sees the wait was
    skipped deliberately, not silently dropped."""
    metrics: dict[str, str] = field(default_factory=dict)
    """Machine-readable deploy metrics parsed from ``VQ-DEPLOY-METRIC`` lines
    in the deploy transcript. See :func:`parse_deploy_metrics`."""

    @property
    def success(self) -> bool:
        return (
            self.command_rc == 0
            and self.verify_rc == 0
            and self.actual_sha == self.expected_sha
            and self.actual_tag == self.expected_tag
            and self.healthy
            and self.activation == "atomic"
            and self.quiescent
            and self.updater_pid is None
            and not self.active_jobs
            and not self.work_errors
        )

    @property
    def outcome(self) -> str:
        """This result's classification, from the closed set.

        See :attr:`SchedulerHostUpdateResult.outcome`: ``ok`` or ``failed``
        for a lane that ran; the refusals are raised before a result exists.
        """
        return OUTCOME_OK if self.success else OUTCOME_FAILED


@dataclass
class SchedulerRuntimeUpdateRecord:
    """Persistent LAST OK record for one scheduler-host runtime."""

    host: str
    program: str
    last_updated_at: str
    last_success: bool
    expected_sha: str
    actual_sha: str | None = None
    expected_tag: str | None = None
    actual_tag: str | None = None
    command_rc: int | None = None
    verify_rc: int | None = None
    healthy: bool = False
    activation: str | None = None
    active_path: str | None = None
    health_detail: str | None = None
    quiescent: bool = False
    updater_pid: int | str | None = None
    errors: list[str] = field(default_factory=list)
    last_ok_sha: str | None = None
    """Identity of the last VERIFIED deployment, carried forward across
    failures. A failed attempt must not erase the operator's rollback target:
    after the 2026-07-24 pbs-cluster verify failure the record showed only sha='-',
    and finding what to roll back to meant digging through old transcripts."""
    last_ok_tag: str | None = None
    last_ok_at: str | None = None
    last_ok_active_path: str | None = None
    activation_command: str | None = None
    """The site command whose run produced ``activation``. A helper lane skips
    its wait only when the command it is about to run is the same one that
    proved atomic activation last time, so repointing a host at a different
    (possibly mutating) deploy script fails closed instead of inheriting the
    old script's proof."""
    metrics: dict[str, str] = field(default_factory=dict)
    """Deploy metrics of the recorded attempt (see
    :func:`parse_deploy_metrics`): dependency-cache decision, ccache hit
    rate, phase durations, native-rebuild flag."""


def scheduler_runtime_status_path() -> Path:
    return paths.state_root() / SCHEDULER_RUNTIME_STATUS_FILENAME


def load_scheduler_runtime_status() -> dict[str, SchedulerRuntimeUpdateRecord]:
    path = scheduler_runtime_status_path()
    try:
        raw = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(raw, dict):
        return {}
    records: dict[str, SchedulerRuntimeUpdateRecord] = {}
    allowed = {item.name for item in fields(SchedulerRuntimeUpdateRecord)}
    for key, value in raw.items():
        if not isinstance(key, str) or not isinstance(value, dict):
            continue
        try:
            records[key] = SchedulerRuntimeUpdateRecord(
                **{name: item for name, item in value.items() if name in allowed}
            )
        except (TypeError, ValueError):
            continue
    return records


def record_scheduler_runtime_outcome(result: SchedulerRuntimeUpdateResult) -> None:
    records = load_scheduler_runtime_status()
    key = f"{result.host}:{result.program}"
    previous = records.get(key)
    if result.success:
        last_ok_sha = result.actual_sha
        last_ok_tag = result.actual_tag
        last_ok_at = utcnow_iso()
        last_ok_active_path = result.active_path
    elif previous is not None:
        # Carry the rollback target forward. Records written before these
        # fields existed have them as None; when such a record was itself a
        # success, its own identity IS the last-good one.
        fallback = previous.last_success
        last_ok_sha = previous.last_ok_sha or (
            previous.actual_sha if fallback else None
        )
        last_ok_tag = previous.last_ok_tag or (
            previous.actual_tag if fallback else None
        )
        last_ok_at = previous.last_ok_at or (
            previous.last_updated_at if fallback else None
        )
        last_ok_active_path = previous.last_ok_active_path or (
            previous.active_path if fallback else None
        )
    else:
        last_ok_sha = last_ok_tag = last_ok_at = last_ok_active_path = None
    records[key] = SchedulerRuntimeUpdateRecord(
        host=result.host,
        program=result.program,
        last_updated_at=utcnow_iso(),
        last_success=result.success,
        expected_sha=result.expected_sha,
        actual_sha=result.actual_sha,
        expected_tag=result.expected_tag,
        actual_tag=result.actual_tag,
        command_rc=result.command_rc,
        verify_rc=result.verify_rc,
        healthy=result.healthy,
        activation=result.activation,
        active_path=result.active_path,
        health_detail=result.health_detail,
        quiescent=result.quiescent,
        updater_pid=result.updater_pid,
        errors=list(result.work_errors),
        last_ok_sha=last_ok_sha,
        last_ok_tag=last_ok_tag,
        last_ok_at=last_ok_at,
        last_ok_active_path=last_ok_active_path,
        metrics=dict(result.metrics),
    )
    _write_scheduler_runtime_status(records)


def _write_scheduler_runtime_status(
    records: dict[str, SchedulerRuntimeUpdateRecord],
) -> None:
    path = scheduler_runtime_status_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    paths.atomic_write_text(
        path,
        json.dumps(
            {key: asdict(value) for key, value in sorted(records.items())},
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )


def apply_helper_activation_receipt(result: SchedulerHostUpdateResult) -> None:
    """Read the site script's activation receipt off its own transcript.

    The helper's deploy scripts publish an immutable per-SHA root and switch
    the stable path by rename (pbs-cluster's ``vq-update-scheduler-buildhost``,
    slurm-cluster's ``update-scheduler-vq-admin.sh``). They say so with
    ``VQ-DEPLOY-METRIC helper_activation=atomic`` plus the path and the SHA.

    The SHA is the part that matters. A stale copy of a deploy script cannot
    launder an old proof into a new update: the receipt is accepted only when
    it names the exact commit this update asked for.
    """
    if result.metrics.get("helper_activation") != "atomic":
        return
    if result.expected_source_sha is None:
        return
    if result.metrics.get("helper_activation_sha", "").lower() != (
        result.expected_source_sha.lower()
    ):
        result.maintenance_warnings.append(
            "ignoring an activation receipt for a different commit: script "
            f"reported {result.metrics.get('helper_activation_sha') or '(none)'}, "
            f"this update staged {result.expected_source_sha}"
        )
        return
    result.activation = "atomic"
    result.active_path = result.metrics.get("helper_active_path") or None


def helper_activation_is_proven(host: str, command: str) -> tuple[bool, str]:
    """Whether ``host``'s helper deploy has proven it activates atomically.

    Returns ``(proven, reason)``. Consulted BEFORE the deploy runs, so it can
    only read the *previous* run's receipt -- which is the right question
    anyway: "does this host's deploy script stage and flip?" is a property of
    the installed script, not of one run. Both scheduler hosts re-sync their
    deploy scripts from the freshly verified archive on every update, so the
    recorded receipt describes the script that is installed now.

    Fails closed in every direction that matters: no record, no receipt, or a
    different command than the one that earned the receipt.
    """
    record = load_scheduler_runtime_status().get(
        f"{host}:{SCHEDULER_HELPER_RECORD_PROGRAM}"
    )
    if record is None:
        return False, "no previous helper deployment is recorded for this host"
    if record.activation != "atomic":
        return False, "the last helper deployment did not prove atomic activation"
    if record.activation_command != command:
        return False, (
            "the configured helper command differs from the one that proved "
            "atomic activation"
        )
    return True, (
        "the site helper deploy publishes an immutable per-SHA root and "
        "switches the stable path by rename, so a vq invocation already "
        f"running holds its own root (last proven at {record.active_path})"
    )


def record_scheduler_helper_outcome(result: SchedulerHostUpdateResult) -> None:
    """Persist the helper deployment's canonical LAST OK record.

    The scheduler helper's identity used to live only in remote provenance
    markers, probed by ``vq doctor`` against the *driver's* live checkout.
    That made a helper standing exactly at the accepted report pin look
    "not deployed" the moment the driver tree moved ahead. Recording the
    verified helper identity here — same store, same shape as the runtime
    lanes — lets ``rollout-latest`` prove a lane is already at the accepted
    pin without ever consulting the driver checkout, so a second rollout is
    a true no-op.
    """
    if result.expected_source_sha is None:
        # Staging never established an identity (early refusal/failure):
        # nothing was deployed, so the previous record remains the truth.
        return
    records = load_scheduler_runtime_status()
    key = f"{result.host}:{SCHEDULER_HELPER_RECORD_PROGRAM}"
    previous = records.get(key)
    verified = (
        result.success
        and result.remote_source_sha == result.expected_source_sha
        and result.remote_source_tree_sha256 == result.expected_source_tree_sha256
    )
    if verified:
        last_ok_sha = result.remote_source_sha
        last_ok_at = utcnow_iso()
    elif previous is not None:
        last_ok_sha = previous.last_ok_sha or (
            previous.actual_sha if previous.last_success else None
        )
        last_ok_at = previous.last_ok_at or (
            previous.last_updated_at if previous.last_success else None
        )
    else:
        last_ok_sha = last_ok_at = None
    records[key] = SchedulerRuntimeUpdateRecord(
        host=result.host,
        program=SCHEDULER_HELPER_RECORD_PROGRAM,
        last_updated_at=utcnow_iso(),
        last_success=verified,
        expected_sha=result.expected_source_sha,
        actual_sha=result.remote_source_sha,
        healthy=verified,
        health_detail=(
            f"helper staged from {result.stage_source or 'driver-tree'}; "
            "provenance verified"
            if verified
            else "; ".join(result.work_errors) or "helper update did not verify"
        ),
        quiescent=True,
        errors=list(result.work_errors),
        last_ok_sha=last_ok_sha,
        last_ok_at=last_ok_at,
        activation=result.activation
        or (previous.activation if previous is not None else None),
        active_path=result.active_path
        or (previous.active_path if previous is not None else None),
        activation_command=(
            result.configured_command
            if result.activation
            else (previous.activation_command if previous is not None else None)
        ),
        metrics=dict(result.metrics),
    )
    _write_scheduler_runtime_status(records)


def _resolve_venv_program(env: str, cfg: config.Config) -> config.VenvProgram:
    """Validate that ``env`` is a registered ``kind="venv"`` program with
    a real git checkout. Raises :class:`AdminError` on any problem.
    Shared by ``update_env`` (single) and ``update_all`` (batch)."""
    prog = cfg.programs.get(env)
    if prog is None:
        raise AdminError(
            f"unknown env {env!r}: not in [programs.X] registry. "
            f"Run `vq programs` to list registered envs."
        )
    if not isinstance(prog, config.VenvProgram):
        raise AdminError(
            f"env {env!r} has kind={prog.kind!r}; only kind=\"venv\" envs "
            f"can be refreshed via admin update (binary and import "
            f"programs are not git-backed)."
        )
    git_dir = Path(prog.git_dir)
    if not git_dir.is_dir():
        raise AdminError(
            f"env {env!r}: git_dir {prog.git_dir!r} is not a directory"
        )
    if not (git_dir / ".git").exists():
        raise AdminError(
            f"env {env!r}: git_dir {prog.git_dir!r} is not a git checkout "
            f"(no .git subdirectory)"
        )
    return prog



def _do_slot_update_work(
    env: str,
    prog: config.VenvProgram,
    *,
    expected_tag: str | None = None,
    expected_sha: str | None = None,
    update_script_args: list[str] | None = None,
) -> UpdateResult:
    """Update a slot-enabled venv program without touching the live runtime.

    Builds a NEW per-SHA slot alongside the running one and flips the pointer
    only after the build and its import check pass. A job already running keeps
    the interpreter it started with, because that interpreter lives in its own
    slot and nothing here writes to it -- which is the whole point: an in-place
    update rewrites the very files a live process imports from, and a job paused
    across one can end up serving some modules from ``sys.modules`` and importing
    others off the rewritten disk.

    A failed build leaves ``current`` where it was. There is no rollback step
    because there is no forward step to undo: the flip is the last thing that
    happens, and a half-built slot is simply never published.
    """
    result = UpdateResult(
        env=env,
        git_dir=prog.git_dir,
        branch=prog.branch,
        update_script=prog.update_script,
        post_update_script=prog.post_update_script,
        expected_tag=expected_tag,
        expected_sha=expected_sha.lower() if expected_sha is not None else None,
        fail_on_dirty_in_effect=prog.fail_on_dirty,
    )
    if expected_sha is None:
        # Slots are keyed by commit, so the target must be known before the
        # build starts -- an untagged `git pull` only learns its SHA afterwards,
        # by which point there is nowhere to have put it. Every fleet update
        # already passes --expected-sha from the accepted release report, so
        # this costs nothing operationally.
        result.work_errors.append(
            f"{env}: runtime slots require --expected-sha (the slot is keyed by "
            "commit and must be chosen before the build)"
        )
        return result

    sha = expected_sha.lower()
    configured_root = prog.runtime_slot_root
    assert configured_root is not None  # caller checked; narrows the type
    try:
        root = str(runtime_slots.layout(configured_root).root)
        transaction_id = secrets.token_hex(16)
        build_required = runtime_slots.begin_slot_build(
            root,
            sha,
            transaction_id=transaction_id,
            in_use=lambda: _runtime_slot_in_use_snapshot(root),
        )
        if build_required:
            runtime_slots.materialize_source(
                prog.git_dir, root, sha, runner=_mutating_git_run,
            )
    except runtime_slots.RuntimeSlotError as exc:
        result.work_errors.append(f"{env}: {exc}")
        return result

    slot_prog = runtime_slots.slot_local_program(prog, root, sha)
    if not build_required:
        result.git_pull_rc = 0
        if prog.update_script is not None:
            result.update_script_rc = 0
        if prog.post_update_script is not None:
            result.post_update_script_rc = 0
        result.actual_sha = sha
        result.sha_check_rc = 0
        result.dirty_after_update = False
        if expected_tag is not None:
            result.tag_check_rc, result.actual_tag = _run_expected_git_tag_check(
                runtime_slots.slot_source(root, sha), expected_tag,
            )
            if result.tag_check_rc != 0 or result.actual_tag != expected_tag:
                result.work_errors.append(
                    f"{env}: verified slot {sha[:12]} does not carry required "
                    f"tag {expected_tag!r}"
                )
                return result
        slot_source_root = slot_prog.vibeqc_source_root()
        if (
            slot_source_root is not None
            and slot_prog.effective_import_check() == "vibeqc"
        ):
            result.import_check_rc, result.import_check_output = (
                _run_import_check(
                    slot_prog.python,
                    "vibeqc",
                    symbols=slot_prog.import_symbols,
                    source_root=slot_source_root,
                    native_within=slot_source_root,
                )
            )
            if result.import_check_rc != 0:
                detail = result.import_check_output.strip().splitlines()
                suffix = f": {detail[-1]}" if detail else ""
                result.work_errors.append(
                    f"{env}: verified slot {sha[:12]} failed structural "
                    f"vibeqc runtime revalidation{suffix}"
                )
                return result
        try:
            previous = runtime_slots.activate(root, sha)
        except runtime_slots.RuntimeSlotError as exc:
            result.work_errors.append(
                f"{env}: verified slot reuse failed: {exc}"
            )
            return result
        log.info(
            "env %s: reused verified runtime slot %s (previous=%s)",
            env,
            sha[:12],
            (previous or "none")[:12],
        )
        return result

    # Reuse the ordinary update path verbatim inside the slot: fetch, checkout,
    # tag verification, update script, import check. Forking that logic for
    # slots would be two code paths to keep honest instead of one.
    inner = _do_update_work(
        env,
        slot_prog,
        expected_tag=expected_tag,
        expected_sha=sha,
        update_script_args=update_script_args,
        _fresh_runtime=True,
    )
    if not inner.success:
        inner.work_errors.append(
            f"{env}: slot {sha[:12]} was built but not activated; the live "
            "runtime is unchanged"
        )
        return inner

    try:
        runtime_slots.seal_slot_build(
            root,
            sha,
            transaction_id=transaction_id,
            runner=_mutating_git_run,
        )
        previous = runtime_slots.activate(root, sha)
    except runtime_slots.RuntimeSlotError as exc:
        inner.work_errors.append(f"{env}: slot built but activation failed: {exc}")
        return inner
    log.info(
        "env %s: activated runtime slot %s (previous=%s)",
        env,
        sha[:12],
        (previous or "none")[:12],
    )
    return inner


def _runtime_slot_in_use_snapshot(root: str) -> set[str]:
    """Read a complete, lock-consistent spec census for destructive cleanup."""
    specs: list[JobSpec] = []
    multi_user = config.system_multi_user_enabled()
    snapshot_paths = _visible_spec_paths(multi_user=multi_user)
    for path in snapshot_paths:
        try:
            with paths.spec_lock(path, timeout=5.0):
                specs.append(JobSpec.read(path))
        except Exception as exc:
            raise runtime_slots.RuntimeSlotError(
                f"cannot prove runtime-slot liveness because {path} is "
                f"unreadable: {exc}"
            ) from exc
    if _visible_spec_paths(multi_user=multi_user) != snapshot_paths:
        raise runtime_slots.RuntimeSlotError(
            "cannot prove runtime-slot liveness because the visible spec set "
            "changed during its locked snapshot"
        )
    return runtime_slots.slots_in_use(root, specs)


def _do_update_work(
    env: str,
    prog: config.VenvProgram,
    *,
    expected_tag: str | None = None,
    expected_sha: str | None = None,
    update_script_args: list[str] | None = None,
    managed_daemon_restart: bool = False,
    _fresh_runtime: bool = False,
) -> UpdateResult:
    """Run the git refresh + optional tag check + optional update_script
    for ONE env. Untagged updates run ``git pull``; tagged updates fetch and
    check out the exact tag before verification. Does NOT pause/resume the
    queue and does NOT persist the outcome — those are the caller's
    responsibility (so ``update_all`` can pause/resume ONCE around a batch
    instead of per-env).

    ``prog`` must already be validated via :func:`_resolve_venv_program`.

    v0.7.1 *Lamport's Clock* Item 3: ``update_script_args`` is a
    list of extra flags appended to the ``bash <script>`` invocation
    (typically forwarded from ``vq admin update --update-script-arg
    X``). The flags execute as the user running the update, same
    trust boundary as the script itself.
    """
    if prog.runtime_slot_root is not None:
        return _do_slot_update_work(
            env,
            prog,
            expected_tag=expected_tag,
            expected_sha=expected_sha,
            update_script_args=update_script_args,
        )
    git_dir = Path(prog.git_dir)
    result = UpdateResult(
        env=env,
        git_dir=str(git_dir),
        branch=prog.branch,
        update_script=prog.update_script,
        post_update_script=prog.post_update_script,
        expected_tag=expected_tag,
        expected_sha=expected_sha.lower() if expected_sha is not None else None,
        # v0.7.1 Item 5: snapshot the config-side opt-in so the
        # success property has everything it needs without re-
        # consulting cfg at verdict time.
        fail_on_dirty_in_effect=prog.fail_on_dirty,
    )
    immutable_selector = bool(expected_tag is not None or expected_sha is not None)
    import_check = prog.effective_import_check()
    vibeqc_source_root = (
        prog.vibeqc_source_root() if import_check == "vibeqc" else None
    )
    managed_vibeqc = vibeqc_source_root is not None
    atomic = bool(prog.update_script and import_check)
    preserve_live_runtime = atomic and managed_vibeqc and not _fresh_runtime
    # Capture the rollback target BEFORE the detached-HEAD reattach below.
    #
    # The reattach runs `git checkout <branch>`, which moves the WORKING TREE.
    # Recording HEAD after it meant the rollback baseline was the local branch
    # ref's position -- and a pinned fleet never advances that ref, because
    # every `--expected-sha` update checks out a detached HEAD. So a failed
    # build did not restore what the environment had been serving; it restored
    # wherever the branch was last left.
    #
    # Localhost, 2026-08-01: a bare `vq admin update vibeqc-dev localhost` hit a
    # transient build failure and the rollback landed the checkout on
    # 95c141c790ca -- v0.15.48, months old -- from a live v0.15.106. Worse, the
    # `.dist-info` from the last successful editable install was untouched, so
    # `vibeqc.__version__` still read 0.15.106 while the source on disk was
    # months behind: silent wrong-version execution with every version-reporting
    # surface agreeing it was fine.
    #
    # Runtime identity and source timestamps must describe the checkout that is
    # serving *before* the reattach.  A detached checkout can differ from its
    # stale local branch in both editable-install mapping and native sources.
    # Artifact bytes are copied at the same point. This is deliberately before
    # *any* Git mutation: if import identity cannot identify a recoverable core,
    # the updater must preserve the serving checkout and refuse to proceed.
    if immutable_selector:
        baseline_sha, baseline_branch = _capture_checkout_state(git_dir)
        if baseline_sha is None:
            result.work_errors.append(
                "immutable update refused: could not capture the exact "
                "pre-update checkout for rollback"
            )
            return result
        status_rc, status_output = _run_git_status_porcelain(git_dir)
        if status_rc != 0:
            result.work_errors.append(
                "immutable update refused: could not inspect the working tree"
            )
            return result
        if status_output.strip():
            result.work_errors.append(
                "immutable update refused: working tree is dirty"
            )
            return result
        result.pre_update_sha = baseline_sha
        result.pre_update_branch = baseline_branch
    elif atomic:
        result.pre_update_sha = _git_head_sha(git_dir)
    if atomic and managed_vibeqc:
        operation_active, operation_detail = _git_operation_in_progress(git_dir)
        if operation_active is None:
            result.work_errors.append(
                "managed vibeqc update refused: could not inspect Git "
                f"operation state: {operation_detail}"
            )
            return result
        if operation_active:
            result.work_errors.append(
                "managed vibeqc update refused: a pre-existing Git operation "
                f"is in progress ({operation_detail})"
            )
            return result
    if atomic and managed_vibeqc and not immutable_selector:
        status_rc, status_output = _run_git_status_porcelain(git_dir)
        if status_rc != 0:
            result.work_errors.append(
                "managed vibeqc update refused: could not inspect the "
                "pre-update working tree"
            )
            return result
        if status_output.strip():
            result.work_errors.append(
                "managed vibeqc update refused: the pre-update working tree "
                "is dirty; preserve or reconcile operator changes first"
            )
            return result
    runtime_native_path: Path | None = None
    serving_core_path: Path | None = None
    rollback_source_mtimes: tuple[tuple[Path, int], ...] = ()
    probe_output = ""
    _probe_version: str | None = None
    if preserve_live_runtime:
        (
            _probe_rc,
            probe_output,
            _probe_version,
            _module_path,
            native_path,
        ) = config.run_import_runtime_identity_probe(
            prog.python,
            import_check,
        )
        if config.VIBEQC_UNSUPPORTED_CORE_ERROR in probe_output:
            result.work_errors.append(
                "managed vibeqc update refused: "
                f"{config.VIBEQC_UNSUPPORTED_CORE_ERROR}"
            )
            return result
        if _module_path is None:
            result.work_errors.append(
                "managed vibeqc update refused: the interpreter did not "
                "identify its vibeqc package source before mutation"
            )
            return result
        expected_package = (vibeqc_source_root / "python" / "vibeqc").resolve()
        try:
            imported_module = Path(_module_path).resolve(strict=True)
        except OSError as exc:
            result.work_errors.append(
                "managed vibeqc update refused: the interpreter's vibeqc "
                f"package source could not be read: {exc}"
            )
            return result
        if imported_module.parent != expected_package:
            result.work_errors.append(
                "managed vibeqc update refused: the registered interpreter "
                "resolves vibeqc from the wrong checkout: "
                f"{imported_module} (expected {expected_package})"
            )
            return result
        if native_path:
            runtime_native_path = Path(native_path)
        if runtime_native_path is None:
            detail = probe_output.strip().splitlines()
            suffix = f": {detail[-1]}" if detail else ""
            result.work_errors.append(
                "managed vibeqc update refused: the interpreter did not "
                "identify its serving compiled core for rollback"
                f"{suffix}"
            )
            return result
        try:
            serving_core_path = runtime_native_path.resolve(strict=True)
        except OSError as exc:
            result.work_errors.append(
                "managed vibeqc update refused: the serving compiled core "
                f"could not be read for rollback: {exc}"
            )
            return result
        if not serving_core_path.is_file():
            result.work_errors.append(
                "managed vibeqc update refused: the serving compiled core is "
                f"not a file: {serving_core_path}"
            )
            return result
        try:
            rollback_source_mtimes = config.vibeqc_native_source_mtimes(git_dir)
        except OSError as exc:
            result.work_errors.append(
                "managed vibeqc update refused: could not capture native "
                f"source freshness evidence: {exc}"
            )
            return result
        if result.pre_update_sha is None:
            result.work_errors.append(
                "managed vibeqc update refused: could not capture the exact "
                "pre-update checkout for rollback"
            )
            return result
        if not rollback_source_mtimes:
            result.work_errors.append(
                "managed vibeqc update refused: no native source freshness "
                "evidence was available before mutation"
            )
            return result
    # v0.12.x fix 3: arm the atomic build. Snapshot the serving native core
    # BEFORE reattachment or pull, so a failed build can restore the prior
    # {tree, core} pair. Managed vibe-qc updates fail closed when the actual
    # core cannot be recovered; generic explicit import checks retain their
    # established source-only rollback behavior when no native artifact exists.
    so_backup: _NativeArtifactSnapshot | None = None
    if atomic:
        try:
            so_backup = _snapshot_native_artifacts(
                git_dir,
                import_check,
                runtime_native_path=runtime_native_path,
                source_mtimes=rollback_source_mtimes,
                track_vibeqc_candidates=managed_vibeqc,
                runtime_version=_probe_version,
                capture_runtime_version=preserve_live_runtime,
            )
        except OSError as exc:
            result.work_errors.append(
                "atomic update refused: native rollback artifacts could not "
                f"be captured: {exc}"
            )
            return result
    serving_core_snapshotted = bool(
        so_backup is not None
        and serving_core_path is not None
        and any(
            target == serving_core_path
            for _copy, target in so_backup.entries
        )
    )
    if preserve_live_runtime and not serving_core_snapshotted:
        detail = probe_output.strip().splitlines()
        suffix = f": {detail[-1]}" if detail else ""
        result.work_errors.append(
            "managed vibeqc update refused: the serving compiled core could "
            f"not be located for rollback{suffix}"
        )
        if so_backup is not None:
            shutil.rmtree(so_backup.directory, ignore_errors=True)
        return result
    reattach = _DetachedReattachResult(output="")
    if prog.branch and not immutable_selector:
        reattach = _reattach_clean_detached_checkout_for_update(
            git_dir,
            prog.branch,
            work_errors=result.work_errors,
            capture_rollback_ref=atomic,
        )
    reattach_output = reattach.output
    if result.work_errors:
        result.git_pull_rc = None
        result.git_pull_output = reattach_output
        if (
            atomic
            and reattach.performed
            and import_check is not None
            and result.pre_update_sha is not None
        ):
            _rollback_atomic_state(
                result,
                prog,
                git_dir,
                so_backup,
                import_check=import_check,
                source_root=vibeqc_source_root,
                reattach=reattach,
                cause=(
                    "detached checkout repair failed after changing "
                    "checkout state"
                ),
            )
        if so_backup is not None:
            shutil.rmtree(so_backup.directory, ignore_errors=True)
        return result
    if expected_tag is not None:
        result.git_pull_rc, result.git_pull_output = _run_git_fetch_tag(
            git_dir, expected_tag,
        )
        if result.git_pull_rc == 0:
            resolved_rc, resolved_sha, resolved_output = _run_git_resolve_commit(
                git_dir, f"refs/tags/{expected_tag}^{{commit}}",
            )
            result.git_pull_output += resolved_output
            if resolved_rc != 0 or resolved_sha is None:
                result.work_errors.append(
                    f"tag resolution failed for {expected_tag!r}: "
                    f"git rev-parse rc={resolved_rc}"
                )
            elif (
                result.expected_sha is not None
                and resolved_sha != result.expected_sha
            ):
                result.actual_sha = resolved_sha
                result.sha_check_rc = 0
                result.work_errors.append(
                    f"tag {expected_tag!r} resolves to {resolved_sha}, not "
                    f"the required {result.expected_sha}; checkout refused"
                )
            else:
                checkout_rc, checkout_output = _run_git_checkout_detached(
                    git_dir, resolved_sha,
                )
                result.git_pull_output += checkout_output
                if checkout_rc != 0:
                    result.work_errors.append(
                        f"tag checkout failed for {expected_tag!r}: "
                        f"git checkout rc={checkout_rc}"
                    )
    elif result.expected_sha is not None:
        result.git_pull_rc, result.git_pull_output = _run_git_fetch_sha(
            git_dir, result.expected_sha, prog.branch,
        )
        if result.git_pull_rc == 0:
            checkout_rc, checkout_output = _run_git_checkout_detached(
                git_dir, result.expected_sha,
            )
            result.git_pull_output += checkout_output
            if checkout_rc != 0:
                result.work_errors.append(
                    "SHA checkout failed for "
                    f"{result.expected_sha}: git checkout rc={checkout_rc}"
                )
    else:
        result.git_pull_rc, result.git_pull_output = _run_git_pull(
            git_dir,
            work_errors=result.work_errors,
            branch=prog.branch if reattach.performed else None,
        )
    if reattach_output:
        result.git_pull_output = reattach_output + result.git_pull_output
    # v0.7.1 *Lamport's Clock*: post-pull branch verification.
    # Runs BEFORE the update_script for untagged updates: a wrong-branch
    # checkout means we'd be spending 10-30 min of build to produce a venv
    # against the wrong source tree. Tagged updates intentionally detach HEAD
    # at the immutable tag, so the exact tag check below is the authority.
    # Skipped when ``prog.branch`` is unset (legacy envs where the operator
    # manages branch by hand) or when git refresh itself failed.
    if (
        expected_tag is None
        and result.expected_sha is None
        and prog.branch is not None
        and result.git_pull_rc == 0
    ):
        (
            result.branch_check_rc,
            result.actual_branch,
        ) = _run_git_branch_check(git_dir)
    # v0.5.24: --tag verification BEFORE the update_script.
    # Why before: a failed verify means the checkout isn't where
    # we expected; running the build against it would consume
    # resources to produce a wrongly-tagged venv. Cheaper to bail.
    if (
        expected_tag is not None
        and result.git_pull_rc == 0
        and not result.work_errors
    ):
        # v0.6.1: transition into TAG_CHECKING so a stuck-at-tag-check
        # update is distinguishable from a stuck-at-pull update in the
        # status banner. No-op when no marker exists (e.g. direct
        # _do_update_work test invocation).
        transition_admin_update_state(ADMIN_UPDATE_STATE_TAG_CHECKING)
        (
            result.tag_check_rc,
            result.actual_tag,
        ) = _run_expected_git_tag_check(git_dir, expected_tag)
    if (
        result.expected_sha is not None
        and result.git_pull_rc == 0
        and not result.work_errors
    ):
        result.sha_check_rc, result.actual_sha = _run_git_sha_check(git_dir)
    # Only run update_script if git pull succeeded AND (no --tag
    # given OR tag matched) AND branch verification did NOT
    # explicitly fail. Without --tag, behaviour is identical to
    # v0.5.20. v0.7.1: the branch gate prevents burning a 10-30
    # min build cycle against a wrong-branch tree.
    #
    # The branch gate is phrased as ``branch_matches is not False``
    # (not ``branch_matches``) on purpose: ``branch_matches`` is
    # ``None`` when verification didn't run (no ``prog.branch``
    # configured OR git pull failed OR — in test rigs — the
    # conftest stub forces a "didn't run" return). ``None`` must
    # not block the build; only an explicit ``False`` (verification
    # ran and reported a mismatch) does.
    if (
        prog.update_script
        and result.git_pull_rc == 0
        and (expected_tag is None or result.tag_matches)
        and (result.expected_sha is None or result.sha_matches)
        and result.branch_matches is not False
    ):
        # v0.6.1: transition into BUILDING for the update_script step.
        # On a heavy vibe-qc rebuild this can run 10-30 min; the
        # operator sees "stuck at BUILDING for 18m" in `vq admin status`
        # rather than the misleading "PULLING".
        transition_admin_update_state(ADMIN_UPDATE_STATE_BUILDING)
        (
            result.update_script_rc,
            result.update_script_output,
            result.update_script_seconds,
        ) = _run_update_script(
            git_dir, prog.update_script,
            work_errors=result.work_errors,
            extra_args=_update_script_args_for_ref(
                update_script_args, expected_tag, result.expected_sha,
            ),
            strip_config_ref_args=(
                expected_tag is not None or result.expected_sha is not None
            ),
            managed_daemon_restart=managed_daemon_restart,
            lifecycle_target=Path(prog.python).parent.parent,
        )
        result.metrics.update(
            parse_deploy_metrics(result.update_script_output)
        )
        if result.update_script_rc == 0:
            _verify_expected_tag_after_script(result, git_dir)
            _verify_expected_sha_after_script(result, git_dir)
    if (
        prog.post_update_script
        and result.git_pull_rc == 0
        and (expected_tag is None or result.tag_matches)
        and (result.expected_sha is None or result.sha_matches)
        and result.branch_matches is not False
        and (
            result.update_script is None
            or result.update_script_rc == 0
        )
    ):
        (
            result.post_update_script_rc,
            result.post_update_script_output,
            result.post_update_script_seconds,
        ) = _run_update_script(
            git_dir,
            prog.post_update_script,
            work_errors=result.work_errors,
            label="post_update_script",
            lifecycle_target=Path(prog.python).parent.parent,
        )
        result.metrics.update(
            parse_deploy_metrics(result.post_update_script_output)
        )
        if result.post_update_script_rc == 0:
            _verify_expected_tag_after_script(result, git_dir)
            _verify_expected_sha_after_script(result, git_dir)
    # v0.7.1 Item 5: post-update dirty-tree check. Gated on
    # ``prog.fail_on_dirty`` — we only run ``git status --porcelain``
    # when the env explicitly opts in. Two reasons:
    #   1. Operators can already see live-current dirty state via
    #      ``vq admin status`` (which calls _query_git_dirty from
    #      query_env_status), so the no-opt-in case isn't losing
    #      visibility.
    #   2. Skipping the extra subprocess.run call when it can't
    #      affect the verdict keeps the test surface clean — pre-
    #      v0.7.1 tests with rigid side_effect=[pull, script]
    #      lists keep working without touching the dirty path.
    # Dev clones (vibeqc-dev with basissetdev artifacts) leave
    # ``fail_on_dirty=False`` and skip; queue/release clones opt
    # in via config.toml and get the check.
    if result.git_pull_rc == 0 and (prog.fail_on_dirty or immutable_selector):
        status_rc, status_output = _run_git_status_porcelain(git_dir)
        if status_rc != 0:
            result.dirty_after_update = None
            result.work_errors.append(
                "post-update working-tree verification failed"
            )
        else:
            result.dirty_after_update = bool(status_output.strip())
            if immutable_selector and result.dirty_after_update:
                result.work_errors.append(
                    "immutable update left the previously clean working tree dirty"
                )
    # v0.12.x fix 3: post-build ABI gate + atomic rollback. Runs last so
    # it sees the build's verdict; restores the prior {tree, .so} pair if
    # the build failed or the freshly built env doesn't import.
    try:
        _finalize_atomic_build(
            result,
            prog,
            git_dir,
            so_backup,
            import_check=import_check,
            source_root=vibeqc_source_root,
            reattach=reattach,
            native_within=(vibeqc_source_root if _fresh_runtime else None),
        )
        _finalize_detached_reattach(
            result,
            prog,
            git_dir,
            so_backup,
            import_check=import_check,
            source_root=vibeqc_source_root,
            reattach=reattach,
        )
        _finalize_managed_atomic_transaction(
            result,
            prog,
            git_dir,
            so_backup,
            import_check=import_check,
            source_root=vibeqc_source_root,
            reattach=reattach,
        )
        _finalize_immutable_checkout(result, prog, git_dir, so_backup)
    finally:
        if so_backup is not None:
            shutil.rmtree(so_backup.directory, ignore_errors=True)
    return result


def _update_script_args_for_ref(
    update_script_args: list[str] | None,
    expected_tag: str | None,
    expected_sha: str | None,
) -> list[str] | None:
    """Build forwarded update-script args for a ref-pinned update.

    The contract for managed immutable release updates is explicit: vq first
    checks out ``TAG``/``SHA`` itself, then invokes the configured update
    script with an explicit source selector appended after operator-supplied
    script args. vibe-qc's update script accepts ``--branch`` for branches/tags
    and ``--ref`` for exact commits; placing the selected ref last makes the
    queue's immutable-selection gate authoritative.
    """
    forwarded = _strip_update_script_ref_args(update_script_args or [])
    if expected_tag is not None:
        forwarded.extend(["--branch", expected_tag])
    elif expected_sha is not None:
        forwarded.extend(["--ref", expected_sha])
    return forwarded or None


_UPDATE_SCRIPT_REF_FLAGS = {"--dev", "--release"}
_UPDATE_SCRIPT_REF_FLAGS_WITH_VALUE = {"--branch", "--ref"}


def _strip_update_script_ref_args(args: list[str]) -> list[str]:
    """Remove update-script source selectors before appending a vq pin.

    Managed pinned updates make vq, not the profile's shell defaults, the
    authority for the selected source ref. Some deployments configure
    ``update_script = "scripts/update.sh --dev"`` for ordinary branch
    refreshes. When an operator requests ``--tag`` or ``--expected-sha`` we
    must drop that older selector before appending ``--branch REF``; otherwise
    strict update scripts reject the conflicting argv before rebuilding.
    """
    stripped: list[str] = []
    skip_next = False
    for arg in args:
        if skip_next:
            skip_next = False
            continue
        if arg in _UPDATE_SCRIPT_REF_FLAGS:
            continue
        if arg in _UPDATE_SCRIPT_REF_FLAGS_WITH_VALUE:
            skip_next = True
            continue
        if any(
            arg.startswith(f"{flag}=")
            for flag in _UPDATE_SCRIPT_REF_FLAGS_WITH_VALUE
        ):
            continue
        stripped.append(arg)
    return stripped


def _verify_expected_tag_after_script(
    result: UpdateResult, git_dir: Path,
) -> None:
    if result.expected_tag is None:
        return
    (
        result.tag_check_rc,
        result.actual_tag,
    ) = _run_expected_git_tag_check(git_dir, result.expected_tag)
    if not result.tag_matches:
        result.work_errors.append(
            "post-update tag verification failed: expected "
            f"{result.expected_tag!r}, got {result.actual_tag!r}"
        )


def _verify_expected_sha_after_script(
    result: UpdateResult, git_dir: Path,
) -> None:
    if result.expected_sha is None:
        return
    result.sha_check_rc, result.actual_sha = _run_git_sha_check(git_dir)
    if not result.sha_matches:
        result.work_errors.append(
            "post-update SHA verification failed: expected "
            f"{result.expected_sha}, got {result.actual_sha or '(missing)'}"
        )


def _warn_if_declared_extras_are_inert(env: str, prog: config.VenvProgram) -> None:
    """Say so when a declared capability is not this update's to apply.

    ``extras`` reaches pip only through the managed daemon transaction, which
    is the one path that has proved the updater is vq's own
    ``scripts/update.sh`` and that it may name the install target outright. An
    ordinary update runs whatever ``update_script`` the program configures --
    for vibe-qc and vibe-view programs, a script that has never heard of
    ``--extras`` -- so it cannot honour the declaration and must not pretend
    to.

    Silence here would rebuild exactly the belief this feature exists to
    correct: config that looks applied and is not.
    """
    if prog.extras:
        output.narrate(
            f"note: {env} declares extras {list(prog.extras)}, which only a "
            "managed daemon update applies. This update rebuilds the "
            "environment with the profile its .vq-install-metadata records."
        )


def _new_pause_token() -> str:
    """v0.11.1: a per-invocation tag for the admin-update queue pause,
    unique to THIS ``update_env`` / ``update_all`` call.

    Every pause/resume bracket stamps each job it suspends with this token
    and resumes only jobs carrying that exact token. Because the
    token is fresh per invocation, a *prior* update that was interrupted
    (SIGKILL / OOM / host reboot) before its ``finally`` resume ran —
    leaving jobs SUSPENDED under that earlier run's tag — can NOT be
    woken by a *later* update: the later run's filter doesn't match the
    earlier token.

    Incident this guards (2026-06-22, build-host): a ``vq admin update
    vibeqc-queue`` resumed 22 jobs that a previous *interrupted* update
    had left paused, spiking the box to load 88 on heavy btrfs+LUKS I/O.
    The pre-fix non-surgical path resumed with a blanket ``resume_all``
    (no filter), which wakes every SUSPENDED job regardless of who
    paused it — including a prior run's stragglers and operator-paused
    jobs. A per-invocation token (NOT a shared constant like the build
    script's ``update-script`` tag, which a later run would re-match and
    re-wake) confines the resume to exactly what this run suspended. See
    ``docs/operations.md`` § "Admin update pause/resume scoping".

    The surgical (``provides_branches``) path uses the same token: an exact
    job-id list cannot be returned if the pauser dies halfway through its
    scan, while the durable per-job intents retain the token in that window.

    Charset/length: ``admin-update-`` + 12 hex chars (25 total) fits the
    ``paused_by`` validator (``[A-Za-z0-9._-]{1,50}``) so it flows
    through CLI argv / ssh-shipped argv / multi-user signalling without
    quoting.
    """
    return f"admin-update-{uuid.uuid4().hex[:12]}"


_MANAGED_UPDATE_SOURCE_FLAGS = {"--dev", "--release"}
_MANAGED_UPDATE_SOURCE_VALUE_FLAGS = {"--branch", "--ref"}

_EXTRAS_PROFILES: tuple[tuple[str, frozenset[str]], ...] = (
    ("core", frozenset()),
    ("web", frozenset({"web"})),
    ("test", frozenset({"test"})),
    ("dev", frozenset({"dev", "test"})),
    ("all", frozenset({"web", "test", "dev"})),
)
"""``update.sh --extras`` profiles, smallest first, and what each installs.

Mirrors ``vq_extras_to_spec`` in ``scripts/_venv_helpers.sh``, which is the
half that pip actually sees. ``tests/test_program_extras.py`` runs that shell
function against this table so the two cannot drift -- a profile that meant
different things on the two sides of the handoff would rebuild a serving venv
with capabilities nobody asked for, or without the ones somebody did.
"""

_MANAGED_UPDATE_PROFILE_NAMES = frozenset(name for name, _ in _EXTRAS_PROFILES)


def _resolve_extras_profile(recorded: str, required: Sequence[str]) -> str:
    """The profile to rebuild with: the smallest that covers both inputs.

    ``recorded`` is what ``.vq-install-metadata`` says the venv was last built
    with; ``required`` is what :attr:`~vq.config.VenvProgram.extras` declares
    it is *for*. The result is the smallest profile containing both, so a
    declaration can only ever add capability.

    That asymmetry is deliberate. The freeze this function sits inside exists
    so a managed daemon update cannot quietly rebuild a serving venv with less
    than it had, and a declaration read from config is not a reason to relax
    it: ``extras = ["web"]`` on a ``dev`` environment resolves to ``all``, not
    to ``web``, because narrowing here would uninstall the test tooling on the
    next rebuild (the managed path always passes ``--recreate-venv``, so a
    dropped extra really does disappear) and the operator asked for uvicorn,
    not for that.

    The union is not always a profile of its own -- ``web`` plus ``test`` is
    not a published combination -- so the answer is the smallest profile that
    *contains* it, which is why that pair resolves to ``all``.
    """
    contents = dict(_EXTRAS_PROFILES)
    if recorded not in contents:  # pragma: no cover - the caller validates it
        raise AdminError(f"unknown recorded venv extras profile {recorded!r}")
    needed = frozenset(required) | contents[recorded]
    for name, installs in _EXTRAS_PROFILES:
        if needed <= installs:
            return name
    # Reachable only if vq publishes an extra that no profile installs, which
    # is a packaging bug rather than an operator one. Say which, and refuse:
    # rebuilding with the nearest profile would silently ignore a declaration.
    raise AdminError(
        "no vq extras profile installs "
        + ", ".join(sorted(needed))
        + "; declared program extras cannot be satisfied"
    )


def _prove_legacy_managed_install(venv: Path, project_root: Path) -> bool:
    """Read PEP 610 without executing an unowned environment (#577)."""
    for name in (".vq-install-metadata", ".vq-checkout-owner"):
        if os.path.lexists(venv / name):
            raise AdminError("legacy adoption requires an entirely unmarked venv")
    if venv.is_symlink() or not (venv / "pyvenv.cfg").is_file():
        raise AdminError("legacy adoption requires a real virtualenv")
    if (venv / "pyvenv.cfg").is_symlink():
        raise AdminError("legacy adoption refuses a symlinked pyvenv.cfg")
    matches = sorted(venv.glob("lib*/python*/site-packages/vq-*.dist-info/direct_url.json"))
    safe = []
    for candidate in matches:
        if not candidate.is_file():
            raise AdminError("legacy adoption found nonregular PEP 610 metadata")
        if not any(p.is_symlink() for p in (candidate, *candidate.parents)
                   if p != venv and venv in p.parents):
            safe.append(candidate)
    if len(safe) != 1 or any(not p.samefile(safe[0]) for p in matches):
        raise AdminError("legacy adoption requires one unambiguous PEP 610 record")
    try:
        payload = json.loads(safe[0].read_text(encoding="utf-8"))
        url = urlsplit(payload["url"])
        editable = payload.get("dir_info", {}).get("editable", False)
        valid = (
            url.scheme == "file" and not url.netloc and not url.query
            and not url.fragment and Path(unquote(url.path)).is_absolute()
            and type(editable) is bool
            and Path(unquote(url.path)).resolve(strict=True) == project_root.resolve(strict=True)
        )
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
        raise AdminError("legacy adoption cannot prove PEP 610 checkout identity") from exc
    if not valid:
        raise AdminError("legacy adoption PEP 610 origin does not match the checkout")
    return editable


def _read_managed_install_metadata(venv: Path) -> dict[str, str]:
    """Read the recorded profile/mode using the same strict policy everywhere."""
    metadata = venv / ".vq-install-metadata"
    try:
        raw_lines = metadata.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise AdminError(
            f"cannot preserve managed venv install metadata {metadata}: {exc}"
        ) from exc
    values: dict[str, str] = {}
    for line in raw_lines:
        key, separator, value = line.partition("=")
        if not separator or key in values:
            raise AdminError(f"malformed managed venv install metadata {metadata}")
        values[key] = value
    if set(values) != {"version", "extras", "editable"}:
        raise AdminError(f"incomplete managed venv install metadata {metadata}")
    if (values["version"] != "1"
            or values["extras"] not in _MANAGED_UPDATE_PROFILE_NAMES
            or values["editable"] not in {"0", "1"}):
        raise AdminError(f"invalid managed venv install metadata {metadata}")
    return values


def _managed_update_script_args(
    prog: config.VenvProgram,
    supplied: list[str] | None,
) -> list[str]:
    """Pre-attest the canonical updater and freeze its install target.

    A managed daemon update may not delegate failure atomicity to an arbitrary
    successful hook.  The outer transaction accepts only the repository's
    canonical ``scripts/update.sh``, preserves the proven environment's install
    mode, and allows an explicit extras change. It supplies an external base
    interpreter and the serving venv path. Source selectors remain allowed in the
    configured command; immutable callers strip/replace them later.

    Three things may decide the profile, and none of them is the configured
    command line. The venv's own metadata carries it forward; an operator's
    ``--update-script-arg --recreate-venv --update-script-arg --extras
    --update-script-arg PROFILE`` changes it once (#11); and
    :attr:`~vq.config.VenvProgram.extras` is a floor under both, re-applied on
    every rebuild. ``--extras`` in ``update_script`` stays rejected because
    that is free text deciding what lands in a serving environment, where the
    other three are either attested evidence or typed, validated input whose
    effect is bounded by :func:`_resolve_extras_profile`.

    The floor is checked against an operator request rather than silently
    raising it: a request that does not cover the declaration is two
    instructions contradicting each other, and vq should say so rather than
    install something nobody typed.
    """
    if not prog.update_script:
        raise AdminError(
            "managed daemon update requires the canonical update script"
        )
    parts = shlex.split(prog.update_script)
    if not parts:
        raise AdminError("managed daemon update script is empty")
    project_root = _vq_project_root_for_program(prog)
    canonical = (project_root / "scripts" / "update.sh").resolve(strict=True)
    configured = (Path(prog.git_dir) / parts[0]).resolve(strict=True)
    if configured != canonical:
        raise AdminError(
            "managed daemon update requires the checkout's canonical "
            f"scripts/update.sh, got {configured}"
        )

    index = 0
    configured_args = parts[1:]
    configured_modes: set[str] = set()
    while index < len(configured_args):
        arg = configured_args[index]
        if arg in {"--editable", "--copied"}:
            if arg in configured_modes:
                raise AdminError("managed updater repeats a configured install mode")
            configured_modes.add(arg)
            index += 1
            continue
        if arg in _MANAGED_UPDATE_SOURCE_FLAGS:
            index += 1
            continue
        if arg in _MANAGED_UPDATE_SOURCE_VALUE_FLAGS:
            if index + 1 >= len(configured_args):
                raise AdminError(
                    f"managed updater source flag {arg} has no value"
                )
            index += 2
            continue
        if arg == "--extras":
            # Two supported routes exist now; name both. The generic message
            # sent coordinator's console down a two-day dead end, because the
            # rejection was correct and the remedy it implied ("run pip
            # yourself") is forbidden on a fleet host.
            raise AdminError(
                "managed daemon update rejects '--extras' in update_script. "
                "Declare the capability durably with extras = [\"web\"] under "
                "this program in vq's config, or change this one environment "
                "with: vq admin update ENV --update-script-arg --recreate-venv "
                "--update-script-arg --extras --update-script-arg web"
            )
        raise AdminError(
            "managed daemon update rejects configured updater argument "
            f"{arg!r}; only source selectors and the recorded install mode are allowed"
        )
    venv = Path(prog.python).parent.parent
    if venv.is_symlink():
        raise AdminError("managed daemon update refuses a symlinked virtualenv")
    venv = venv.resolve(strict=True)
    if supplied and "--adopt-legacy" in supplied:
        # An explicit declaration is required: PEP 610 records provenance and
        # editable mode, but cannot recover which extras were requested.
        if (len(supplied) != 4 or supplied[:2] != ["--adopt-legacy", "--extras"]
                or supplied[2] not in _MANAGED_UPDATE_PROFILE_NAMES
                or supplied[3] not in {"--editable", "--copied"}):
            raise AdminError(
                "managed daemon update rejects operator update-script arguments; "
                "legacy adoption requires --adopt-legacy --extras PROFILE "
                "and --editable or --copied"
            )
        editable = _prove_legacy_managed_install(venv, project_root)
        if editable != (supplied[3] == "--editable"):
            raise AdminError("legacy adoption mode differs from PEP 610")
        values = {"version": "1", "extras": supplied[2], "editable": str(int(editable))}
    else:
        values = _read_managed_install_metadata(venv)
    mode = "--editable" if values["editable"] == "1" else "--copied"
    if configured_modes - {mode}:
        raise AdminError("configured updater mode differs from the recorded install mode")
    if supplied and "--adopt-legacy" not in supplied:
        # Keep the target/interpreter/mode frozen. A profile change uses the
        # same stopped-service backup/replace/rollback transaction as an update.
        requested_profile: str | None = None
        recreate = False
        index = 0
        while index < len(supplied):
            arg = supplied[index]
            if arg == "--recreate-venv" and not recreate:
                recreate = True
                index += 1
            elif (arg == "--extras" and requested_profile is None
                  and index + 1 < len(supplied)
                  and supplied[index + 1] in _MANAGED_UPDATE_PROFILE_NAMES):
                requested_profile = supplied[index + 1]
                index += 2
            else:
                raise AdminError(
                    "managed profile change requires --recreate-venv --extras PROFILE; "
                    f"unsupported or duplicate argument {arg!r}"
                )
        if not recreate:
            raise AdminError("managed profile change requires explicit --recreate-venv")
        if requested_profile is not None:
            # An operator asking for a profile that does not cover what the
            # program declares is a contradiction between two things vq was
            # told, not a floor to quietly raise: say which two.
            declared_missing = sorted(
                set(prog.extras) - dict(_EXTRAS_PROFILES)[requested_profile]
            )
            if declared_missing:
                raise AdminError(
                    f"requested profile {requested_profile!r} does not install "
                    + ", ".join(declared_missing)
                    + f", which {prog.extras} on this program declares. Ask for a "
                    "profile that covers it, or change the declaration"
                )
            values["extras"] = requested_profile
    # The declaration is a floor under whatever decided the profile above --
    # the venv's own metadata, a legacy adoption, or #11's operator request.
    # Only the last of those is an instruction, and it was just checked against
    # the declaration, so widening here can never contradict one.
    values["extras"] = _resolve_extras_profile(values["extras"], prog.extras)
    try:
        probe = subprocess.run(
            [
                prog.python,
                "-I",
                "-S",
                "-c",
                (
                    "import os,sys; "
                    "print(os.path.realpath(getattr(sys,'_base_executable',None) "
                    "or sys.executable))"
                ),
            ],
            capture_output=True,
            text=True,
            timeout=30,
            cwd="/",
            stdin=subprocess.DEVNULL,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise AdminError(
            f"cannot resolve managed venv base interpreter: {exc}"
        ) from exc
    base_python = Path((probe.stdout or "").strip())
    if (
        probe.returncode != 0
        or not base_python.is_absolute()
        or not base_python.is_file()
        or not os.access(base_python, os.X_OK)
        or venv == base_python
        or venv in base_python.parents
    ):
        raise AdminError(
            "managed venv did not report a usable external base interpreter"
        )
    return [
        "--venv", str(venv),
        "--python", str(base_python.resolve()),
        "--extras", values["extras"],
        # _run_update_script retains configured arguments. The strict shell
        # parser must receive the attested mode once across both argv sources.
        *([] if configured_modes else [mode]),
        "--recreate-venv",
    ]


def acknowledge_failed_markers(envs: list[str], host: str) -> list[str]:
    """Clear in-scope markers whose previous run **failed**, and say which.

    The unattended half of ``vq admin clear-update-marker``. Recovery today is
    read the marker, clear it (interactively), re-run -- three steps and a
    TTY, which an orchestration has none of. developer-host's ``vibeview-dev``
    marker sat for about five hours behind exactly that.

    Deliberately narrow, and none of it is ``--force``:

    * only a marker this request's scope actually conflicts with, so
      acknowledging one program never clears another's;
    * only ``state=failed``. A ``running`` marker whose writer is alive is a
      live update and raises; a ``stale`` one is an unexplained death, whose
      diagnosis an operator should read before anything is discarded.

    Returns the marker ids cleared, empty when there was nothing to
    acknowledge. Writes the same durable receipt the verb writes, because
    "who acknowledged this, and when" must not depend on which entry point
    did it.
    """
    acknowledged: list[str] = []
    for _path, marker in _admin_update_marker_entries():
        if marker is None:
            raise AdminPreconditionFailed(
                "an unreadable admin-update marker is present; inspect it "
                "with `vq admin status --verbose` before acknowledging "
                "anything"
            )
        if not admin_update_scopes_conflict(
            marker.envs, marker.host, envs or [], host,
        ):
            continue
        diagnosis = diagnose_admin_update_marker(marker)
        if diagnosis.marker_status != ADMIN_UPDATE_MARKER_DIAG_FAILED:
            raise AdminPreconditionFailed(
                "--acknowledge-failed-marker only acknowledges a marker whose "
                f"previous run failed; this one is {diagnosis.marker_status!r}: "
                f"{diagnosis.summary}. {diagnosis.action}"
            )
        cleared = clear_admin_update_marker(marker)
        if cleared is not None:
            acknowledged.append(cleared.marker_id or "(no id)")
            output.narrate(
                f"acknowledged failed marker for envs={cleared.envs} "
                f"(started {cleared.started_at})"
            )
    return acknowledged


def already_current(
    prog: config.VenvProgram,
    *,
    expected_sha: str | None,
    expected_tag: str | None,
) -> tuple[bool, str]:
    """Is the requested target already deployed **and healthy**? With a reason.

    Health is not optional here, and it is the whole reason this is a
    function rather than a SHA comparison. compute-b spent the 2026-09 migration
    sitting at the right commit with a venv that could not import: a check
    that compared SHAs alone would have called that converged and skipped it
    forever, which is the failure this codebase keeps re-learning. A lane is
    current when the commit matches *and* the program answers its own
    availability probe.

    Without ``expected_sha`` there is no target to compare, so the answer is
    always no: an untargeted `vq admin update` means "bring this to the tip",
    and only the update itself can know whether it already is.
    """
    if expected_sha is None:
        return False, "no --expected-sha: an untargeted update has no target"
    head = prog.current_git_sha(full=True)
    if head != expected_sha:
        return False, (
            f"checkout is at {head or '(unreadable)'}, not {expected_sha}"
        )
    if prog.current_git_dirty() is not False:
        return False, "checkout is dirty, or its state could not be read"
    if expected_tag is not None:
        _rc, actual_tag = _run_git_tag_check(Path(prog.git_dir))
        if actual_tag != expected_tag:
            return False, (
                f"HEAD is tagged {actual_tag or '(none)'}, not {expected_tag}"
            )
    healthy, reason = prog.availability()
    if not healthy:
        return False, f"at the target commit but not healthy: {reason}"
    return True, f"already at {expected_sha}"


def _shas_agree(first: str, second: str) -> bool:
    """Two commit ids name the same commit, allowing one to be abbreviated."""
    width = min(len(first), len(second))
    return first[:width].lower() == second[:width].lower()


def _refuse_atomic_update_from_uninstalled_checkout(
    env: str,
    prog: config.VenvProgram,
    *,
    expected_sha: str | None,
) -> None:
    """Refuse an atomic update whose serving pair is already inconsistent.

    The rollback baseline is whatever is serving when an update starts. An
    earlier update that died after moving the checkout -- compute-b, 2026-09-11,
    killed with its ssh session (#37) -- leaves the checkout at a commit vq
    never installed. The next attempt snapshotted that pair, the four-hour cap
    reaped its build, and it "rolled back" to a checkout the core was never
    built from (#44). A pair that was never consistent is not a baseline.

    The evidence is the durable record, which ``vq admin status --json``
    reports as ``installed_sha_matches_checkout``. Unknown is not
    disagreement: with no record or an unreadable checkout, the update arms
    as it always has.

    A rebuild pinned to the checkout's own commit is let through, because it
    is the remedy: it does not move the checkout, so its rollback restores
    exactly the state it started from.
    """
    if prog.runtime_slot_root is not None:
        return
    if not (prog.update_script and prog.effective_import_check()):
        return
    installed = getattr(read_admin_status().get(env), "last_installed_sha", None)
    checkout = prog.current_git_sha(full=True)
    if not installed or not checkout or _shas_agree(installed, checkout):
        return
    if expected_sha is not None and _shas_agree(expected_sha, checkout):
        output.narrate(
            f"{env}: the checkout {checkout[:12]} was never installed (last "
            f"installed {installed[:12]}); rebuilding at the checkout's own "
            "commit, so a failed build restores this same state"
        )
        return
    raise AdminPreconditionFailed(
        f"{env}: refusing an atomic update from a checkout vq never "
        f"installed: the checkout is at {checkout[:12]} but the runtime was "
        f"last installed from {installed[:12]}, so a failed build would roll "
        "back to a pair that was never consistent. Rebuild at the checkout's "
        f"own commit first (`vq admin update {env} --expected-sha "
        f"{checkout}`), then retry this update."
    )


DETACHED_ACTIVATION_TIMEOUT_SECONDS = 120.0
"""How long the ssh-attached parent waits for its updater to prove itself live.

The updater activates before it does any work, so this only has to cover an
interpreter start and vq's imports on a loaded host. It is not a bound on the
update: the whole point is that the parent returns while the build continues.
"""

_DETACHED_ACTIVATION_POLL_SECONDS = 0.05
_DETACHED_UNIT_PROBE_SECONDS = 1.0
_DETACHED_SYSTEMD_RUN_TIMEOUT_SECONDS = 60.0

DETACHED_MECHANISM_SYSTEMD = "systemd-run"
"""The updater runs as a transient systemd user service unit.

Required on hosts whose systemd-logind carries ``KillUserProcesses=yes`` --
build-host, compute-b and workstation on 2026-09-11. There, ending the last ssh session
stops that session's scope and kills everything in it. A new session or process
group does not leave the scope, which is why a ``setsid nohup`` build started
over ssh still died ten minutes in (``203184e``). A user unit lives under the
user manager instead, which ``Linger=yes`` keeps running after logout.
"""

DETACHED_MECHANISM_SESSION = "session"
"""The updater runs as a new session and process-group leader.

Enough where a session's end reaches its processes as a signal to their process
group rather than as a scope kill, and the only option on a host with no
reachable systemd user manager, such as macOS.
"""

_DETACHED_SYSTEMD_ENV_NAMES = frozenset(
    {
        "PATH",
        "HOME",
        "USER",
        "LOGNAME",
        "SHELL",
        "LANG",
        "TZ",
        "TMPDIR",
        "VIRTUAL_ENV",
        "PYTHONPATH",
    }
)
_DETACHED_SYSTEMD_ENV_PREFIXES = ("VQ_", "XDG_", "LC_", "CMAKE_")
_DETACHED_SYSTEMD_ENV_DENIED_PREFIXES = ("VQ_FLEET_OPERATION_",)
_DETACHED_SYSTEMD_ENV_SECRET_MARKERS = (
    "TOKEN",
    "SECRET",
    "PASSWORD",
    "PASSWD",
    "CREDENTIAL",
)
_DETACHED_SYSTEMD_ENV_REFERENCE_SUFFIXES = ("_FILE", "_PATH", "_DIR")


def _detached_systemd_environment(environ: Mapping[str, str]) -> dict[str, str]:
    """The environment a transient user unit must be given explicitly.

    A user unit does not inherit the ssh session's environment; it starts from
    the user manager's. Without the forwarded state roots and the wall/stall
    watchdogs the updater would silently use defaults, which is the mixed-fleet
    bug the forwarded timeout pair exists to prevent.

    It is an allowlist, because ``--setenv`` values are readable by anyone who
    can run ``systemctl --user show`` on the unit. Anything named like a secret
    is dropped even inside the allowlist, except a name that holds a *path* to
    one (``VQ_WEB_TOKEN_FILE``): the path is not the secret, and dropping it
    would send token verification to the default location. Values carrying
    control characters are dropped too, since systemd refuses the assignment
    and the whole launch would fail over one variable.
    """
    kept: dict[str, str] = {}
    for name, value in environ.items():
        if name.startswith(_DETACHED_SYSTEMD_ENV_DENIED_PREFIXES):
            continue
        if not (
            name in _DETACHED_SYSTEMD_ENV_NAMES
            or name.startswith(_DETACHED_SYSTEMD_ENV_PREFIXES)
        ):
            continue
        upper = name.upper()
        if any(marker in upper for marker in _DETACHED_SYSTEMD_ENV_SECRET_MARKERS) and not (
            upper.endswith(_DETACHED_SYSTEMD_ENV_REFERENCE_SUFFIXES)
        ):
            continue
        if any(ord(char) < 32 for char in value):
            continue
        kept[name] = value
    return kept


def detached_unit_name(run_id: str) -> str:
    """The transient user unit a systemd-spawned detached update runs as."""
    return f"vq-admin-update-{admin_detached.validate_run_id(run_id)}"


def _detached_systemd_run_argv(
    run_id: str,
    child_argv: list[str],
    *,
    child_log: Path,
    environ: Mapping[str, str],
    cwd: str,
) -> list[str]:
    """Wrap ``child_argv`` so it runs as a collected transient user unit.

    ``--collect`` removes the unit once it exits, even when it failed, so run
    ids do not accumulate as dead units. Its output is appended to the run's
    ``child.log`` rather than the journal, so a launch-time refusal is readable
    through the same path on every host.
    """
    argv = [
        "systemd-run",
        "--user",
        "--collect",
        "--quiet",
        f"--unit={detached_unit_name(run_id)}",
        f"--working-directory={cwd}",
        f"--property=StandardOutput=append:{child_log}",
        f"--property=StandardError=append:{child_log}",
    ]
    environment = _detached_systemd_environment(environ)
    argv.extend(f"--setenv={name}={environment[name]}" for name in sorted(environment))
    argv.append("--")
    argv.extend(child_argv)
    return argv


def _detached_spawn_mechanism() -> str:
    """Choose how to put the updater outside the session that launched it."""
    if (
        sys.platform.startswith("linux")
        and shutil.which("systemd-run") is not None
        and _systemctl_user_available()
    ):
        return DETACHED_MECHANISM_SYSTEMD
    return DETACHED_MECHANISM_SESSION


def _user_lingers() -> bool | None:
    """Whether logind keeps this user's manager running after logout.

    ``None`` when it cannot be determined. It matters because a transient user
    unit is only as durable as the user manager: without lingering, the manager
    stops with the last session and takes the build with it.
    """
    if shutil.which("loginctl") is None:
        return None
    try:
        proc = subprocess.run(
            ["loginctl", "show-user", str(os.getuid()), "--property=Linger", "--value"],
            capture_output=True,
            text=True,
            timeout=10,
            stdin=subprocess.DEVNULL,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if proc.returncode != 0:
        return None
    value = (proc.stdout or "").strip().lower()
    if value == "yes":
        return True
    if value == "no":
        return False
    return None


def _detached_unit_active(unit: str) -> bool | None:
    """Is the transient unit still running? ``None`` when that is unknowable."""
    try:
        proc = subprocess.run(
            ["systemctl", "--user", "is-active", unit],
            capture_output=True,
            text=True,
            timeout=10,
            stdin=subprocess.DEVNULL,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    state = (proc.stdout or "").strip()
    if state in {"active", "activating", "reloading", "deactivating"}:
        return True
    if state in {"inactive", "failed"}:
        return False
    return None


def _detached_mechanism_warning(mechanism: str) -> str | None:
    """Say so when the chosen mechanism cannot be trusted to outlive the session."""
    if mechanism == DETACHED_MECHANISM_SYSTEMD:
        if _user_lingers() is False:
            return (
                "user lingering is disabled for this account, so the user "
                "manager -- and this build's unit with it -- stops when the last "
                "session ends; enable it with `loginctl enable-linger`"
            )
        return None
    if sys.platform.startswith("linux"):
        return (
            "no reachable systemd user manager, so the build runs in its own "
            "session only; on a host whose logind has KillUserProcesses=yes it "
            "still dies when the ssh session ends"
        )
    return None


def _insert_before_host(argv: list[str], extra: list[str]) -> list[str]:
    """Insert options ahead of a trailing ``localhost`` HOST positional."""
    if argv and argv[-1] == "localhost":
        return [*argv[:-1], *extra, argv[-1]]
    return [*argv, *extra]


def _write_detached_token_file(run_dir: Path, token: str) -> Path:
    """Hand the bearer token to a unit that has no stdin to read it from.

    Owner-only, inside the owner-only run directory, created exclusively, and
    removed by the launcher as soon as the updater has activated -- by which
    point it has already read it. ``--token-file`` enforces the 0600 mode.
    """
    path = run_dir / "token"
    fd = os.open(
        str(path),
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_CLOEXEC", 0),
        0o600,
    )
    try:
        os.fchmod(fd, 0o600)
        os.write(fd, (token + "\n").encode("utf-8"))
    finally:
        os.close(fd)
    return path


def _detached_child_log_tail(run_dir: Path, limit: int = 2000) -> str:
    """Last bytes an updater wrote before dying, for a launch-time diagnosis."""
    try:
        data = (run_dir / "child.log").read_bytes()
    except OSError:
        return ""
    return data[-limit:].decode("utf-8", "replace").strip()


def launch_detached_update(
    *,
    run_id: str,
    target: str,
    child_argv: list[str],
    token: str | None,
    multi_user: bool = False,
    activation_timeout: float = DETACHED_ACTIVATION_TIMEOUT_SECONDS,
    mechanism: str | None = None,
) -> dict[str, object]:
    """Start the update outside the session that asked for it; wait for activation.

    This runs on the *target* host, inside the ssh session a delegated
    ``vq admin update`` opened. On 2026-09-11 that session's end killed three
    builds mid-rebuild: build-host, compute-b and workstation run systemd-logind with
    ``KillUserProcesses=yes``, which stops the session's scope and everything in
    it. So where a systemd user manager is reachable the updater becomes a
    transient user unit, which lives outside every session; elsewhere it becomes
    a new session and process-group leader. ``child_argv`` must not carry a
    token option: the credential travels on stdin or through an owner-only file,
    whichever the mechanism can use, and never on argv.

    Returns the activation receipt the driver needs to start polling, including
    the mechanism used and a warning when that mechanism cannot be trusted to
    survive the session. Raises :class:`AdminError` if the updater could not be
    started or died before activating -- at that point nothing has been mutated,
    so the caller may report a clean refusal rather than an unknown outcome.
    """
    admin_detached.validate_run_id(run_id)
    chosen = mechanism or _detached_spawn_mechanism()
    if chosen not in {DETACHED_MECHANISM_SYSTEMD, DETACHED_MECHANISM_SESSION}:
        raise AdminError(f"unknown detached spawn mechanism {chosen!r}")
    with contextlib.suppress(OSError):
        admin_detached.prune_detached_runs(multi_user=multi_user)
    run_dir = admin_detached.write_launch(
        run_id, target=target, argv=list(child_argv), multi_user=multi_user
    )
    child_log = run_dir / "child.log"
    # The updater's own stdout is only diagnostics -- the payload the driver
    # prints comes from the terminal receipt -- but an updater that dies during
    # argument parsing says why here, and DEVNULL would throw that away.
    log_fd = os.open(
        str(child_log),
        os.O_CREAT | os.O_WRONLY | os.O_TRUNC | getattr(os, "O_CLOEXEC", 0),
        0o600,
    )
    token_path: Path | None = None
    child: subprocess.Popen[bytes] | None = None
    unit: str | None = None
    try:
        if chosen == DETACHED_MECHANISM_SYSTEMD:
            os.close(log_fd)
            log_fd = -1
            argv = list(child_argv)
            if token:
                token_path = _write_detached_token_file(run_dir, token)
                argv = _insert_before_host(argv, ["--token-file", str(token_path)])
            unit = detached_unit_name(run_id)
            try:
                cwd = os.getcwd()
            except OSError:
                cwd = str(Path.home())
            spawn = _detached_systemd_run_argv(
                run_id, argv, child_log=child_log, environ=os.environ, cwd=cwd
            )
            try:
                started = subprocess.run(
                    spawn,
                    capture_output=True,
                    text=True,
                    timeout=_DETACHED_SYSTEMD_RUN_TIMEOUT_SECONDS,
                    stdin=subprocess.DEVNULL,
                )
            except (OSError, subprocess.TimeoutExpired) as exc:
                raise AdminError(
                    "detached admin update could not be started as a systemd "
                    f"user unit; no update was attempted: {exc}"
                ) from exc
            if started.returncode != 0:
                detail = (started.stderr or started.stdout or "").strip()
                raise AdminError(
                    "detached admin update could not be started as a systemd "
                    f"user unit (systemd-run rc={started.returncode}); no update "
                    "was attempted" + (f": {detail}" if detail else "")
                )
        else:
            argv = list(child_argv)
            if token:
                argv = _insert_before_host(argv, ["--token-stdin"])
            try:
                child = subprocess.Popen(
                    argv,
                    stdin=subprocess.PIPE if token else subprocess.DEVNULL,
                    stdout=log_fd,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                    close_fds=True,
                )
            except OSError as exc:
                raise AdminError(
                    f"detached admin update could not be started: {exc}"
                ) from exc
            finally:
                os.close(log_fd)
                log_fd = -1
            if token:
                # The bearer travels on the updater's stdin for the same reason
                # it travels on the remote vq's: an argv element is visible in
                # `ps -ef` to every user on the host.
                try:
                    assert child.stdin is not None
                    child.stdin.write((token + "\n").encode("utf-8"))
                    child.stdin.close()
                except OSError:
                    pass
        return _await_detached_activation(
            run_id,
            run_dir,
            target=target,
            multi_user=multi_user,
            activation_timeout=activation_timeout,
            child=child,
            unit=unit,
            mechanism=chosen,
        )
    finally:
        if log_fd >= 0:
            os.close(log_fd)
        if token_path is not None:
            with contextlib.suppress(OSError):
                token_path.unlink()


def _await_detached_activation(
    run_id: str,
    run_dir: Path,
    *,
    target: str,
    multi_user: bool,
    activation_timeout: float,
    child: subprocess.Popen[bytes] | None,
    unit: str | None,
    mechanism: str,
) -> dict[str, object]:
    """Wait until the updater has activated, or prove it never will."""
    deadline = time.monotonic() + activation_timeout
    next_unit_probe = time.monotonic()
    while True:
        observation = admin_detached.observe(
            run_id, max_bytes=1, multi_user=multi_user
        )
        if observation.state != admin_detached.STATE_LAUNCHING:
            receipt: dict[str, object] = {
                "schema": admin_detached.DETACHED_ACTIVATION_SCHEMA,
                "run_id": run_id,
                "target": target,
                "pid": observation.pid,
                "state": observation.state,
                "mechanism": mechanism,
                "unit": unit,
            }
            warning = _detached_mechanism_warning(mechanism)
            if warning is not None:
                receipt["warning"] = warning
            return receipt
        gone: str | None = None
        if child is not None:
            returncode = child.poll()
            if returncode is not None:
                gone = f"rc={returncode}"
        elif unit is not None and time.monotonic() >= next_unit_probe:
            next_unit_probe = time.monotonic() + _DETACHED_UNIT_PROBE_SECONDS
            if _detached_unit_active(unit) is False:
                gone = f"unit {unit} is no longer active"
        if gone is not None:
            # It may have activated and even finished in the gap since the
            # observation above; only a run that is still unactivated is a
            # refusal.
            if (
                admin_detached.observe(run_id, max_bytes=1, multi_user=multi_user).state
                != admin_detached.STATE_LAUNCHING
            ):
                continue
            detail = _detached_child_log_tail(run_dir)
            raise AdminError(
                f"detached admin update exited ({gone}) before it started work; "
                "no update was attempted" + (f": {detail}" if detail else "")
            )
        if time.monotonic() >= deadline:
            raise AdminError(
                "detached admin update did not activate within "
                f"{activation_timeout:g}s; retained run record: {run_dir}"
            )
        time.sleep(_DETACHED_ACTIVATION_POLL_SECONDS)


def run_detached_update_child(
    run_id: str,
    work: Callable[[], None],
    *,
    multi_user: bool = False,
) -> int:
    """Run one update as the detached child and publish its terminal receipt.

    ``work`` performs exactly what the attached command would have done; its
    stdout is captured so the driver can print the identical payload, and its
    outcome classification is preserved so the driver can exit with the
    identical status. Every exit path publishes a receipt, because a run that
    ends without one is indistinguishable from the killed build this replaces.
    """
    import io  # noqa: PLC0415 — only the detached child captures its stdout

    import click  # noqa: PLC0415 — the CLI's error classes, not its module

    # Under the session mechanism the ssh session's end can still arrive as a
    # SIGHUP to a process group this updater has already left; ignoring it
    # costs nothing. Under a user unit there is no such signal to ignore.
    with contextlib.suppress(ValueError, OSError, AttributeError):
        signal.signal(signal.SIGHUP, signal.SIG_IGN)
    set_detached_run_id(run_id)
    admin_detached.write_activation(
        run_id,
        pid=os.getpid(),
        pid_start_time=_pid_start_time(os.getpid()) or 0,
        multi_user=multi_user,
    )
    buffer = io.StringIO()
    outcome = OUTCOME_OK
    exit_code = 0
    error: str | None = None
    try:
        with contextlib.redirect_stdout(buffer):
            work()
    except click.ClickException as exc:
        # The CLI classifies its own failures. ``outcome`` is present on the
        # classified admin errors and absent on a plain usage/Click error, so
        # read it off the exception rather than re-deriving a verdict here --
        # re-deriving is how a caller ends up matching on sentences again.
        outcome = getattr(exc, "outcome", OUTCOME_FAILED)
        exit_code = exc.exit_code
        error = exc.format_message()
    except SystemExit as exc:
        code = exc.code
        exit_code = code if isinstance(code, int) else (0 if code is None else 1)
        outcome = OUTCOME_OK if exit_code == 0 else OUTCOME_FAILED
    except BaseException as exc:  # noqa: BLE001 — a receipt is mandatory
        outcome, exit_code = OUTCOME_FAILED, 1
        error = f"{type(exc).__name__}: {exc}"
        log.exception("detached admin update %s failed", run_id)
    finally:
        admin_detached.write_result(
            run_id,
            outcome=outcome,
            exit_code=exit_code,
            payload=buffer.getvalue(),
            error=error,
            multi_user=multi_user,
        )
    return exit_code


def update_env(
    env: str,
    cfg: config.Config,
    *,
    host: str,
    admin_token: str | None = None,
    expected_tag: str | None = None,
    expected_sha: str | None = None,
    restart_daemon: bool = True,
    require_self_update: bool = False,
    force: bool = False,
    update_script_args: list[str] | None = None,
    acknowledge_failed_marker: bool = False,
) -> UpdateResult:
    """Run one managed-environment update under exclusive local ownership."""
    with admin_update_ownership():
        prog = _resolve_venv_program(env, cfg)
        if acknowledge_failed_marker:
            # Before the idempotence check and before the marker is taken: a
            # failed marker from a previous run is the thing standing in the
            # way, and acknowledging it is what the caller asked for.
            acknowledge_failed_markers([env], host)
        extra_resources = _runtime_slot_lifecycle_resources(
            prog, expected_sha=expected_sha,
        )
        with toolset_lifecycle_lock(
            [prog],
            action="vq-admin-update",
            extra_resources=extra_resources,
        ):
            return _update_env_owned(
                env,
                cfg,
                host=host,
                admin_token=admin_token,
                expected_tag=expected_tag,
                expected_sha=expected_sha,
                restart_daemon=restart_daemon,
                require_self_update=require_self_update,
                force=force,
                update_script_args=update_script_args,
                resolved_prog=prog,
            )


def _update_env_owned(
    env: str,
    cfg: config.Config,
    *,
    host: str,
    admin_token: str | None = None,
    expected_tag: str | None = None,
    expected_sha: str | None = None,
    restart_daemon: bool = True,
    require_self_update: bool = False,
    force: bool = False,
    update_script_args: list[str] | None = None,
    resolved_prog: config.VenvProgram | None = None,
) -> UpdateResult:
    """Refresh ``<env>`` on the local host. ``host`` should be the
    locally-resolved name (e.g. "localhost" or the bare hostname);
    cross-machine dispatch lives in the CLI layer (it delegates via
    ``ssh <host> vq admin update <env> localhost``).

    ``expected_tag`` (v0.5.24): if non-None, after a successful pull
    the function runs ``git describe --exact-match --tags HEAD`` and
    asserts the output matches. Mismatch → ``UpdateResult.tag_matches=False``
    → ``success=False``. Use for the release-chat pattern:
    ``vq admin update vibeqc-release --tag v0.8.0`` catches the case
    where the pull succeeded but didn't land the expected tag (rebase,
    branch divergence, etc.).

    ``expected_sha`` (v0.12.x): if non-None, fetch the configured branch,
    detach at that exact commit, and verify ``git rev-parse HEAD`` before
    and after the update script. Use for fleet rollouts to a blessed main
    SHA without racing newer pushes.

    ``restart_daemon`` (v0.5.42): if True (default) AND the update
    succeeded AND ``env`` is the venv from which the running
    ``vq-daemon`` was launched, its user systemd service or generated
    ``com.vq.daemon`` launchd agent is restarted so the daemon picks up
    the freshly-installed vq code. RPC must then report the updated exact
    source SHA (vq is editable-installed; on-disk changes alone do not
    replace imported bytecode). When the env is not vq's venv, the daemon
    is left alone regardless. Pass False to opt out. See
    :func:`_detect_vq_self_update` / :func:`_maybe_restart_daemon`
    for the detection + restart wiring.

    ``force`` (v0.5.44): if False (default) and an
    admin-update-in-progress marker is present from a prior
    interrupted update, raise :class:`AdminError` with a recovery
    recipe instead of proceeding. Pass True to overwrite the marker
    and run anyway. See :func:`admin_update_marker_exists`.

    Raises :class:`AdminError` for unrecoverable input problems
    (unknown env, wrong kind, git_dir not a checkout). Returns
    UpdateResult for everything else, including clean runs with a
    non-zero rc from git or the script.
    """
    prog = resolved_prog or _resolve_venv_program(env, cfg)
    initial_self_update_probe = _detect_vq_self_update(prog)
    if not initial_self_update_probe.manager_available:
        raise AdminError(
            f"cannot authoritatively determine whether {env!r} serves the "
            "current vq daemon; refusing checkout/venv mutation until service "
            f"provenance is restored [{initial_self_update_probe.diagnostic}]"
        )
    if initial_self_update_probe.is_self_update and not restart_daemon:
        raise AdminError(
            f"{env!r} is serving the current vq daemon and cannot be updated "
            "with daemon restart suppressed; use `vq self-update "
            "--expected-sha FULL_SHA`"
        )
    if (
        prog.runtime_slot_root is not None
        and (require_self_update or initial_self_update_probe.is_self_update)
    ):
        raise AdminError(
            "a vq daemon self-update cannot activate a runtime slot: the "
            "service definition is bound to the currently serving virtualenv"
        )
    if require_self_update:
        if not restart_daemon:
            raise AdminError(
                "a required vq self-update cannot suppress the daemon restart"
            )
        if not initial_self_update_probe.is_self_update:
            raise AdminError(
                f"{env!r} is not the venv serving the current vq daemon "
                f"[{initial_self_update_probe.diagnostic}]"
            )
        if (
            not initial_self_update_probe.manager_available
            or initial_self_update_probe.service_manager is None
        ):
            raise AdminError(
                "the vq self-update target has no verified supported daemon "
                f"restart path [{initial_self_update_probe.diagnostic}]"
            )
    managed_daemon_restart = bool(
        restart_daemon
        and initial_self_update_probe.is_self_update
        and initial_self_update_probe.manager_available
        and initial_self_update_probe.service_manager is not None
    )
    managed_request_args = list(update_script_args) if update_script_args else None
    profile_change_needed = False
    if not managed_daemon_restart:
        _warn_if_declared_extras_are_inert(env, prog)
    if managed_daemon_restart:
        # Fast-path rejection before marker acquisition and queue pause. The
        # same proofs run again at the exact service-stop boundary to close the
        # ordinary check/use window; a late refusal is explicitly unwound by
        # _update_env_logged after its pause scope is proven clear.
        _guard_managed_git_environment()
        _guard_git_index_unlocked(Path(prog.git_dir))
        if prog.post_update_script:
            raise AdminError(
                "managed daemon update rejects post_update_script; all serving "
                "venv mutation must be performed by canonical update.sh before "
                "strict provenance verification"
            )
        update_script_args = _managed_update_script_args(
            prog, update_script_args,
        )
        if expected_sha is not None and not managed_request_args:
            recorded = _read_managed_install_metadata(Path(prog.python).parent.parent)
            profile_change_needed = (
                recorded["extras"]
                != update_script_args[update_script_args.index("--extras") + 1]
            )
    # Idempotence follows request/target/install-mode validation, under both
    # ownership locks, but still precedes the marker, pause and build. An exact
    # source SHA cannot satisfy an explicit rebuild/profile request or a newly
    # declared extras floor (#11). Generic script arguments likewise require
    # execution: only their updater can interpret the requested work.
    if not managed_request_args and not profile_change_needed:
        current, why = already_current(
            prog, expected_sha=expected_sha, expected_tag=expected_tag,
        )
        if current:
            output.narrate(f"{env}: {why}; nothing to do")
            return UpdateResult(
                env=env,
                git_dir=prog.git_dir,
                branch=prog.branch,
                update_script=prog.update_script,
                already_current=True,
                expected_sha=expected_sha,
                actual_sha=expected_sha,
                expected_tag=expected_tag,
                actual_tag=expected_tag,
            )
    _refuse_atomic_update_from_uninstalled_checkout(
        env, prog, expected_sha=expected_sha,
    )
    # v0.6.42: on a multi-user host `vq admin update` runs as root,
    # so the surgical pause/resume can signal every user's jobs.
    multi_user = cfg.multi_user.enabled or config.system_multi_user_enabled()
    with admin_run_log(
        env, what=f"admin update {env}", multi_user=multi_user
    ) as run_log:
        output.run_log_write(
            "# effective update timeouts: "
            f"wall={_update_script_timeout():g}s "
            f"stall={_build_stall_timeout():g}s"
        )
        result = _update_env_logged(
            env,
            cfg,
            prog=prog,
            multi_user=multi_user,
            host=host,
            admin_token=admin_token,
            expected_tag=expected_tag,
            expected_sha=expected_sha,
            force=force,
            restart_daemon=restart_daemon,
            require_self_update=require_self_update,
            managed_daemon_restart=managed_daemon_restart,
            initial_self_update_probe=initial_self_update_probe,
            update_script_args=update_script_args,
            managed_request_args=managed_request_args,
        )
        if run_log.available:
            result.run_log_path = str(run_log.path)
        return result


def _update_env_logged(
    env: str,
    cfg: config.Config,
    *,
    prog: config.VenvProgram,
    multi_user: bool,
    host: str,
    admin_token: str | None,
    expected_tag: str | None,
    expected_sha: str | None,
    force: bool,
    restart_daemon: bool,
    require_self_update: bool,
    managed_daemon_restart: bool,
    initial_self_update_probe: _SelfUpdateProbe,
    update_script_args: list[str] | None,
    managed_request_args: list[str] | None = None,
) -> UpdateResult:
    """The body of :func:`update_env`, run inside its transcript.

    Split out so the run log brackets the whole operation — pause, pull,
    build, resume, daemon restart — rather than only the build.
    """
    # v0.5.44: refuse to proceed if a prior update left its marker
    # behind. The check runs BEFORE pause_all so a blocked call
    # doesn't briefly pause the queue for no reason.
    # v0.5.50: the marker check + write are now ONE atomic op via
    # acquire_admin_update_marker (O_CREAT|O_EXCL), called below
    # after pause. The pre-pause guard call here is kept as the
    # fast-path "fail before pausing" check — but the authoritative
    # race-free claim is in acquire_admin_update_marker.
    _guard_admin_update_marker(force=force, envs=[env], host=host)

    # 1. Prove the pause scope quiescent FIRST so the rebuild doesn't
    # race a running job. v0.5.47: when prog.provides_branches is set
    # and non-empty, only pause jobs whose spec.branch is in that
    # list — release-branch jobs keep running while a dev env
    # rebuild churns. The admission helper captures eligible RUNNING
    # rows, performs the pause, reconciles crash intents, and fails closed
    # unless a second locked scan proves them quiescent. This is stronger
    # than the interactive bulk helpers' intentionally lenient summaries.
    surgical = bool(prog.provides_branches)
    # v0.11.1: per-invocation tag so the resume in the finally below
    # wakes ONLY the jobs THIS invocation suspended. Without it, the
    # non-surgical resume_all would also wake jobs a prior *interrupted*
    # update left paused — the 2026-06-22 build-host load-88 incident. See
    # :func:`_new_pause_token`. Surgical pauses also stamp this token so a
    # crash during their per-job loop remains durably recoverable.
    pause_token = _new_pause_token()
    pause_queue_root = _admin_pause_queue_root(multi_user=multi_user)
    # 2026-07-25 self-pause wedge: when this update runs INSIDE a build-env
    # job (`vq build-env` is dispatched as a normal job and calls
    # update_env), the pause bracket must not sweep up the job executing
    # it — SIGSTOPping our own process group freezes this very code before
    # it can write anything. The daemon exports VQ_JOB_ID to every job;
    # pause_job additionally refuses own-pgid targets as defense in depth.
    own_jobid = os.environ.get("VQ_JOB_ID")
    exclude = {own_jobid} if own_jobid else None
    # v0.5.50: atomic check-and-write of the marker. With force=False
    # (default), a concurrent admin update racing past our pre-pause
    # guard gets FileExistsError → AdminError here. With force=True
    # the pre-existing marker is unconditionally overwritten.
    # v0.6.0: acquire_admin_update_marker writes state=PAUSING; each
    # phase below transitions through the state machine via
    # transition_admin_update_state. Sticky FAILED on any failure
    # path; success removes the file entirely.
    # 2. Do the work inside try/finally — resume MUST run even if a
    # subprocess hangs and gets killed, or the user hits Ctrl-C.
    # `result` is pre-bound to None so the finally block can resume the
    # queue without masking the original exception: if _do_update_work
    # raises, result stays None, resume_all still runs, and the original
    # exception propagates cleanly (we never reach record/return).
    result: UpdateResult | None = None
    daemon_lifecycle: _ManagedDaemonUpdate | None = None
    managed_git_admission_failed = False
    marker_acquired = False
    paused_summary = "pause not started"
    try:
        acquire_admin_update_marker(envs=[env], host=host, force=force)
        marker_acquired = True
        _record_admin_update_pause_scope(
            pause_token=pause_token,
            paused_jobids=[],
            # Recovery uses the token even for branch-scoped pauses so a death
            # between two SIGSTOPs can resume exactly the completed subset.
            surgical=False,
            multi_user=multi_user,
        )
        pause_proof = pause_token_scope_with_proof(
            host,
            pause_token,
            branches=(prog.provides_branches or []) if surgical else None,
            multi_user=multi_user,
            queue_root=pause_queue_root,
            exclude_jobids=exclude,
        )
        pause_proof.require_quiescent()
        paused_summary = pause_proof.summary
        transition_admin_update_state(ADMIN_UPDATE_STATE_PAUSED)
        # v0.6.0: PULLING phase covers _do_update_work's git pull +
        # optional tag check + optional update_script. The
        # transition fires once at entry; the per-step phases
        # (TAG_CHECKING, BUILDING) would require splitting
        # _do_update_work, which is bigger than the v0.6.0 cut —
        # consumers see PULLING as the "doing on-disk work" state.
        if managed_daemon_restart:
            if (managed_request_args is not None
                    and _managed_update_script_args(prog, managed_request_args)
                    != update_script_args):
                raise AdminError("managed install declaration changed before service stop")
            try:
                daemon_lifecycle = _begin_managed_daemon_update(
                    prog, initial_self_update_probe, env=env,
                )
            except _ManagedGitAdmissionError:
                managed_git_admission_failed = True
                raise
        transition_admin_update_state(ADMIN_UPDATE_STATE_PULLING)
        effective_script_args = list(update_script_args or [])
        result = _do_update_work(
            env, prog,
            expected_tag=expected_tag,
            expected_sha=expected_sha,
            update_script_args=effective_script_args,
            managed_daemon_restart=managed_daemon_restart,
        )
        result.paused_summary = paused_summary
        if daemon_lifecycle is not None:
            _complete_managed_daemon_update(prog, result, daemon_lifecycle)
    finally:
        cleanup_error: BaseException | None = None
        # A service stopped by outer admin must be restored before jobs resume,
        # even if the work helper raised rather than returning a result.  The
        # normal returned-result path above already performed strict restart
        # and provenance verification.
        if daemon_lifecycle is not None and not getattr(
            daemon_lifecycle, "terminal_verified", False,
        ):
            try:
                started, detail = _recover_managed_daemon_after_exception(
                    prog, daemon_lifecycle
                )
                if not started:
                    output.run_log_write(
                        "# daemon recovery after update exception FAILED: "
                        + detail
                    )
            except BaseException as exc:  # resume remains mandatory
                cleanup_error = exc
        if marker_acquired:
            try:
                transition_admin_update_state(ADMIN_UPDATE_STATE_RESUMING)
            except BaseException as exc:  # resume remains mandatory
                cleanup_error = cleanup_error or exc
        try:
            # Every newly paused job, including a branch-scoped surgical
            # pause, carries this token. Resume by token so a mid-scan error or
            # SIGKILL cannot strand the subset paused before the exception.
            resume_proof = resume_token_scope_with_proof(
                host,
                pause_token,
                multi_user=multi_user,
                queue_root=pause_queue_root,
            )
            resume_proof.require_clear()
            resumed_summary = resume_proof.summary
            _disarm_proven_pause_scope_without_managed_receipt()
            if marker_acquired and managed_git_admission_failed:
                # The late Git recheck is before receipt/service/file mutation.
                # Once the exact pause token is proven clear there is no
                # durable recovery authority to retain; leaving RESUMING here
                # would turn a harmless admission refusal into a stale marker.
                _clear_completed_admin_update_marker()
            if result is not None:
                result.resumed_summary = resumed_summary
        except BaseException as exc:
            cleanup_error = cleanup_error or exc
        if cleanup_error is not None:
            raise cleanup_error

    if not managed_daemon_restart:
        _maybe_restart_daemon(
            prog,
            result,
            restart_daemon=restart_daemon,
            require_self_update=require_self_update,
        )
    transition_admin_update_state(ADMIN_UPDATE_STATE_VERIFYING)

    # Persist the verified outcome before releasing the update marker.
    # In multi-user mode this is part of the update's success contract:
    # the authenticated daemon RPC owns the canonical state under
    # /var/lib/vq, while a direct per-user fallback would make the command
    # print OK even though every later status sweep still sees LAST OK=false.
    try:
        record_update_outcome(
            env,
            result,
            admin_token=admin_token,
            multi_user=multi_user,
        )
    except AdminError as exc:
        result.work_errors.append(
            f"canonical admin status persistence failed: {exc}"
        )

    # v0.5.48 (Bug A fix): clear marker AFTER _maybe_restart_daemon
    # has had a chance to set daemon_restart_succeeded. result.success
    # incorporates a failed self-update restart via the property at the
    # top of UpdateResult; clearing here ensures a failed restart
    # leaves the marker on disk so the next `vq admin update` blocks
    # with the recovery recipe. Pre-v0.5.48 the clear ran in the
    # finally block above, BEFORE _maybe_restart_daemon, so a failed
    # restart silently cleared the marker while the daemon stayed on
    # stale code — see docs/audit_2026-05-17 § 2g.
    # v0.6.0: on failure, transition to FAILED (sticky) instead of
    # leaving the marker in its last-pre-failure state. Operator's
    # `vq admin status` then shows state=failed + failure_reason.
    if result is not None and result.success:
        _clear_completed_admin_update_marker()
    else:
        reason = "update did not complete cleanly"
        if result is not None:
            if result.git_pull_rc not in (0, None):
                reason = f"git pull rc={result.git_pull_rc}"
            elif (
                result.update_script
                and result.update_script_rc not in (0, None)
            ):
                reason = f"update_script rc={result.update_script_rc}"
            elif (
                result.post_update_script
                and result.post_update_script_rc not in (0, None)
            ):
                reason = (
                    f"post_update_script rc={result.post_update_script_rc}"
                )
            elif result.work_errors:
                reason = f"work_errors: {'; '.join(result.work_errors)}"
            elif (
                result.daemon_restart_attempted
                and result.daemon_restart_succeeded is False
            ):
                reason = (
                    f"daemon restart failed: "
                    f"{result.daemon_restart_message}"
                )
            elif result.expected_tag and not result.tag_matches:
                reason = (
                    f"tag mismatch: expected {result.expected_tag!r}, "
                    f"got {result.actual_tag!r}"
                )
        transition_admin_update_state(
            ADMIN_UPDATE_STATE_FAILED, failure_reason=reason,
        )

    return result


def _resolve_provisionable_program(
    env: str, cfg: config.Config,
) -> config.VenvProgram:
    """Validate ``env`` can be provisioned. Deliberately does NOT require the
    checkout to exist -- creating it is the point."""
    prog = cfg.programs.get(env)
    if prog is None:
        raise AdminError(
            f"unknown env {env!r}: not in [programs.X] registry. "
            f"Run `vq programs` to list registered envs."
        )
    if not isinstance(prog, config.VenvProgram):
        raise AdminError(
            f"env {env!r} has kind={prog.kind!r}; only kind=\"venv\" envs can "
            "be provisioned (binary and import programs are not git-backed)."
        )
    if not prog.upstream:
        raise AdminError(
            f"env {env!r} has no upstream configured; vq cannot clone it. "
            f"Add `upstream = \"...\"` to [programs.{env}]."
        )
    if not prog.install_script:
        raise AdminError(
            f"env {env!r} has no install_script configured; vq will not guess "
            f"how to build it. Add `install_script = \"...\"` to "
            f"[programs.{env}]."
        )
    return prog


def _refuse_occupied_git_dir(env: str, git_dir: Path) -> None:
    """Refuse to provision over anything already at ``git_dir``.

    An existing checkout is somebody's work, possibly with local commits, and
    a provision is not a repair: ``vq admin update`` is. An empty directory is
    fine -- an operator who pre-created the mount point has said nothing about
    its contents.
    """
    if git_dir.is_symlink():
        raise AdminError(
            f"env {env!r}: git_dir {str(git_dir)!r} is a symlink; refusing to "
            "provision through it"
        )
    if not git_dir.exists():
        return
    if not git_dir.is_dir():
        raise AdminError(
            f"env {env!r}: git_dir {str(git_dir)!r} exists and is not a "
            "directory"
        )
    try:
        occupied = any(git_dir.iterdir())
    except OSError as exc:
        raise AdminError(
            f"env {env!r}: cannot read git_dir {str(git_dir)!r}: {exc}"
        ) from exc
    if occupied:
        detail = (
            "it is already a git checkout; refresh it with `vq admin update`"
            if (git_dir / ".git").exists()
            else "it is not empty"
        )
        raise AdminError(
            f"env {env!r}: refusing to provision into {str(git_dir)!r} -- "
            f"{detail}"
        )


def _clone_at_exact_sha(
    prog: config.VenvProgram,
    git_dir: Path,
    *,
    expected_sha: str,
    result: UpdateResult,
) -> bool:
    """Clone ``upstream`` into ``git_dir`` and detach at ``expected_sha``.

    Tags come with the clone so a subsequent ``--tag`` assertion can be
    checked locally. HEAD is verified against the requested SHA afterwards
    rather than trusted: a clone that lands on something else must fail here,
    not during the first job.
    """
    try:
        git_dir.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        result.work_errors.append(f"cannot create {git_dir.parent}: {exc}")
        return False
    steps = (
        ("clone", ["git", "clone", "--tags", prog.upstream or "", str(git_dir)]),
        ("checkout", ["git", "-C", str(git_dir), "checkout", "--detach", expected_sha]),
    )
    for label, argv in steps:
        try:
            proc = _mutating_git_run(
                argv, capture_output=True, text=True, timeout=1800,
            )
        except subprocess.TimeoutExpired:
            result.work_errors.append(f"git {label} timed out")
            return False
        except OSError as exc:
            result.work_errors.append(f"git {label} failed to start: {exc}")
            return False
        output = _combined_output(proc.stdout, proc.stderr)
        _run_log_command_output(f"git {label}", proc.returncode, output)
        result.git_pull_output += output
        result.git_pull_rc = proc.returncode
        if proc.returncode != 0:
            detail = _last_output_line(output)
            result.work_errors.append(
                f"git {label} failed rc={proc.returncode}"
                + (f": {detail}" if detail else "")
            )
            return False
    result.sha_check_rc, actual = _run_git_sha_check(git_dir)
    result.actual_sha = actual
    if actual != expected_sha:
        result.work_errors.append(
            f"clone landed on {actual}, not the requested {expected_sha}"
        )
        return False
    return True


def provision_env(
    env: str,
    cfg: config.Config,
    *,
    host: str,
    expected_sha: str,
    expected_tag: str | None = None,
    force: bool = False,
    install_script_args: list[str] | None = None,
) -> UpdateResult:
    """Clone and install one managed environment that does not exist yet.

    A host without a program's ``git_dir`` could not be brought up through vq
    at all: ``_resolve_venv_program`` refuses, and nothing clones. Every host
    in the 2026-09 migration therefore needed a manual ``git clone`` plus
    ``scripts/install.sh`` first -- the one step that could not go through
    vibe-queue, and so the step most likely to be done inconsistently. It was.

    Provisioning runs under the same exclusive ownership and admin-update
    marker as an update, so a provision and an update cannot race, and the
    installer runs under the same monitored-build supervision: the wall-clock
    and stall caps, the parallelism cap that keeps a cold native build from
    OOM-killing the box, and the build PATH policy.

    It does **not** drain or pause the queue, and that is not an oversight: a
    program whose checkout does not exist yet has no jobs bound to it and no
    interpreter for a running job to be holding open. There is nothing to
    quiesce. Everything an update pauses for exists only once this has run.

    The lifecycle lock is taken after the clone rather than before, because it
    identifies a checkout by ``git rev-parse --show-toplevel`` and there is no
    checkout to identify until then. The window is covered by the marker,
    which is exclusive per env, and by the refusal to write into anything that
    already exists.
    """
    prog = _resolve_provisionable_program(env, cfg)
    git_dir = Path(prog.git_dir)
    result = UpdateResult(
        env=env,
        git_dir=str(git_dir),
        branch=prog.branch,
        update_script=prog.install_script,
        operation="install",
        expected_sha=expected_sha,
        expected_tag=expected_tag,
    )
    with admin_update_ownership():
        _refuse_occupied_git_dir(env, git_dir)
        acquire_admin_update_marker(envs=[env], host=host, force=force)
        try:
            transition_admin_update_state(ADMIN_UPDATE_STATE_PULLING)
            # Re-checked under the marker: the first check is a courtesy that
            # fails before anything is claimed, this one is the guarantee.
            #
            # It runs before any mutation, so a refusal here means nothing
            # happened -- and a marker left behind for an operation that did
            # nothing is one an operator has to clear by hand to learn that.
            # Clear it and let the refusal speak for itself.
            try:
                _refuse_occupied_git_dir(env, git_dir)
            except AdminError:
                clear_admin_update_marker()
                raise
            output.narrate(f"cloning {prog.upstream} into {git_dir}")
            if not _clone_at_exact_sha(
                prog, git_dir, expected_sha=expected_sha, result=result,
            ):
                return result
            if expected_tag is not None:
                result.tag_check_rc, result.actual_tag = _run_git_tag_check(
                    git_dir
                )
            with toolset_lifecycle_lock([prog], action="vq-admin-install"):
                transition_admin_update_state(ADMIN_UPDATE_STATE_BUILDING)
                output.narrate(f"running {prog.install_script} in {git_dir}")
                rc, script_output, seconds = _run_update_script(
                    git_dir,
                    prog.install_script or "",
                    work_errors=result.work_errors,
                    extra_args=list(install_script_args or []),
                    label="install_script",
                )
                result.update_script_rc = rc
                result.update_script_output = script_output
                result.update_script_seconds = seconds
                result.metrics.update(parse_deploy_metrics(script_output))
            return result
        finally:
            _finish_provision_marker(result)


def _finish_provision_marker(result: UpdateResult) -> None:
    """Record the provision's verdict and release its marker."""
    if result.success:
        _clear_completed_admin_update_marker()
        return
    reason = "provision failed"
    if result.work_errors:
        reason = f"work_errors: {'; '.join(result.work_errors)}"
    elif result.update_script_rc not in (None, 0):
        reason = f"install_script rc={result.update_script_rc}"
    elif result.expected_tag and not result.tag_matches:
        reason = (
            f"tag mismatch: expected {result.expected_tag!r}, "
            f"got {result.actual_tag!r}"
        )
    transition_admin_update_state(
        ADMIN_UPDATE_STATE_FAILED, failure_reason=reason,
    )


def update_all(
    cfg: config.Config,
    *,
    host: str,
    admin_token: str | None = None,
    restart_daemon: bool = True,
    force: bool = False,
    update_script_args: list[str] | None = None,
) -> list[UpdateResult]:
    """Run the managed-environment batch under exclusive local ownership."""
    with admin_update_ownership():
        resolved = [
            (name, _resolve_venv_program(name, cfg))
            for name, candidate in sorted(cfg.programs.items())
            if isinstance(candidate, config.VenvProgram)
        ]
        lock_context = (
            toolset_lifecycle_lock(
                [prog for _name, prog in resolved],
                action="vq-admin-update-all",
            )
            if resolved
            else contextlib.nullcontext()
        )
        with lock_context:
            return _update_all_owned(
                cfg,
                host=host,
                admin_token=admin_token,
                restart_daemon=restart_daemon,
                force=force,
                update_script_args=update_script_args,
                resolved_progs=resolved,
            )


def _update_all_owned(
    cfg: config.Config,
    *,
    host: str,
    admin_token: str | None = None,
    restart_daemon: bool = True,
    force: bool = False,
    update_script_args: list[str] | None = None,
    resolved_progs: list[tuple[str, config.VenvProgram]] | None = None,
) -> list[UpdateResult]:
    """v0.5.28: refresh EVERY ``kind="venv"`` program in the registry,
    in sorted-by-name order.

    Pause/resume bracket the WHOLE batch (not per-env): the queue is
    paused once, every env is pulled+built, then the queue resumes once.
    A job that dispatched mid-batch could otherwise see env A on the new
    commit but env B still on the old one — pausing the whole batch
    closes that window.

    ``--tag`` verification is intentionally NOT supported for ``--all``:
    different envs track different tags (vibeqc-dev=main, vibeqc-release
    =a release tag), so a single ``--tag`` value can't apply to all. The
    CLI rejects ``--all --tag`` before reaching here.

    Each env's outcome is recorded via ``record_update_outcome`` so
    ``vq admin status`` reflects the batch. A failure in one env does
    NOT abort the rest — every env is attempted, results collected, and
    the caller decides the batch verdict (``all(r.success ...)``).

    Returns the per-env results in the same sorted order. Raises
    :class:`AdminError` only if the registry has zero venv programs
    (nothing to do — surface that clearly rather than silently
    returning an empty list).
    """
    venv_envs = sorted(
        name for name, prog in cfg.programs.items()
        if isinstance(prog, config.VenvProgram)
    )
    if not venv_envs:
        raise AdminError(
            "no kind=\"venv\" programs registered; nothing for "
            "`vq admin update --all` to do. Run `vq programs` to "
            "inspect the registry."
        )
    # Validate every env BEFORE pausing the queue — a typo'd git_dir
    # shouldn't pause the queue and then bail.
    progs: list[tuple[str, config.VenvProgram]] = (
        resolved_progs
        if resolved_progs is not None
        else [(name, _resolve_venv_program(name, cfg)) for name in venv_envs]
    )
    # Probe before the first checkout/build mutation.  A batch can contain the
    # venv serving this process's daemon, and that environment's update script
    # must leave restart timing to the outer batch transaction.  The later
    # restart pass re-probes after installation for exact provenance.
    self_update_probes = {
        name: _detect_vq_self_update(prog) for name, prog in progs
    }
    unavailable_probes = [
        (name, probe.diagnostic)
        for name, probe in self_update_probes.items()
        if not probe.manager_available
    ]
    if unavailable_probes:
        details = "; ".join(
            f"{name}: {diagnostic}" for name, diagnostic in unavailable_probes
        )
        raise AdminError(
            "cannot prove batch targets are distinct from the serving vq "
            f"daemon; refusing before pause or mutation ({details})"
        )
    self_update_names = [
        name
        for name, probe in self_update_probes.items()
        if probe.is_self_update
    ]
    if len(self_update_names) > 1:
        raise AdminError(
            "multiple configured venvs in the update batch match the current "
            f"vq daemon: {', '.join(self_update_names)}"
        )
    for name in self_update_names:
        probe = self_update_probes[name]
        prog = next(candidate for candidate_name, candidate in progs if candidate_name == name)
        if not restart_daemon:
            raise AdminError(
                f"batch target {name!r} is the serving vq daemon environment; "
                "--no-restart-daemon would swap live code without lifecycle "
                "ownership"
            )
        if probe.service_manager not in {
            _DaemonServiceManager.SYSTEMD.value,
            _DaemonServiceManager.LAUNCHD.value,
        }:
            raise AdminError(
                f"batch target {name!r} has no supported exact service manager"
            )
        if prog.runtime_slot_root is not None:
            raise AdminError(
                f"batch target {name!r} is the serving daemon and uses "
                "runtime_slot_root; first-class self-update requires the "
                "canonical serving virtualenv transaction"
            )
    managed_daemon_restarts = {
        name: bool(
            restart_daemon
            and probe.is_self_update
            and probe.manager_available
            and probe.service_manager is not None
        )
        for name, probe in self_update_probes.items()
    }
    if update_script_args and "--adopt-legacy" in update_script_args:
        raise AdminError("legacy adoption requires a single managed environment update")
    managed_script_args_by_name: dict[str, list[str]] = {}
    for name, prog in progs:
        if not managed_daemon_restarts[name]:
            _warn_if_declared_extras_are_inert(name, prog)
            continue
        if prog.post_update_script:
            raise AdminError(
                f"batch self-update target {name!r} has post_update_script; "
                "managed daemon transactions require canonical update.sh only"
            )
        managed_script_args_by_name[name] = _managed_update_script_args(
            prog, update_script_args,
        )

    # A batch has no per-env target, so any armed env whose checkout vq never
    # installed refuses the whole batch before its marker and pause (#44).
    for name, prog in progs:
        _refuse_atomic_update_from_uninstalled_checkout(
            name, prog, expected_sha=None,
        )

    # v0.5.44: marker guard BEFORE pausing the queue, same as
    # update_env.
    _guard_admin_update_marker(
        force=force,
        envs=[name for name, _ in progs],
        host=host,
    )

    # v0.6.42: multi-user — `vq admin update` runs as root; the
    # batch-wide pause/resume reaches every user's jobs.
    multi_user = cfg.multi_user.enabled or config.system_multi_user_enabled()
    # v0.11.1: per-invocation tag (see :func:`_new_pause_token` /
    # update_env) so the batch resume in the finally wakes only what
    # THIS batch paused, never a prior interrupted update's stragglers.
    pause_token = _new_pause_token()
    pause_queue_root = _admin_pause_queue_root(multi_user=multi_user)
    paused_summary = "pause not started"
    # v0.5.44: marker records the full env list so a recovery
    # inspector sees the whole batch scope.
    # v0.5.50: atomic check-and-write via acquire (same rationale as
    # update_env: closes the concurrent-update race).
    # v0.6.0: state machine — acquire writes PAUSING; we transition
    # PAUSED → PULLING (covers the whole loop) → RESUMING → ...
    results: list[UpdateResult] = []
    daemon_lifecycle: _ManagedDaemonUpdate | None = None
    marker_acquired = False
    daemon_target: tuple[str, config.VenvProgram] | None = next(
        (
            (name, prog)
            for name, prog in progs
            if managed_daemon_restarts[name]
        ),
        None,
    )
    try:
        acquire_admin_update_marker(
            envs=[name for name, _ in progs], host=host, force=force,
        )
        marker_acquired = True
        _record_admin_update_pause_scope(
            pause_token=pause_token,
            paused_jobids=[],
            surgical=False,
            multi_user=multi_user,
        )
        pause_proof = pause_token_scope_with_proof(
            host,
            pause_token,
            multi_user=multi_user,
            queue_root=pause_queue_root,
        )
        pause_proof.require_quiescent()
        paused_summary = pause_proof.summary
        transition_admin_update_state(ADMIN_UPDATE_STATE_PAUSED)
        transition_admin_update_state(ADMIN_UPDATE_STATE_PULLING)
        for name, prog in progs:
            if managed_daemon_restarts[name]:
                if (update_script_args and _managed_update_script_args(prog, update_script_args)
                        != managed_script_args_by_name[name]):
                    raise AdminError("managed install declaration changed before service stop")
                daemon_lifecycle = _begin_managed_daemon_update(
                    prog, self_update_probes[name], env=name,
                )
            per_env_args = (
                list(managed_script_args_by_name[name])
                if managed_daemon_restarts[name]
                else list(update_script_args or [])
            )
            result = _do_update_work(
                name, prog,
                update_script_args=per_env_args,
                managed_daemon_restart=managed_daemon_restarts[name],
            )
            result.paused_summary = paused_summary
            results.append(result)
            if managed_daemon_restarts[name] and daemon_lifecycle is not None:
                _complete_managed_daemon_update(
                    prog, result, daemon_lifecycle,
                )
    finally:
        cleanup_error: BaseException | None = None
        if daemon_lifecycle is not None and not getattr(
            daemon_lifecycle, "terminal_verified", False,
        ):
            try:
                assert daemon_target is not None
                started, detail = _recover_managed_daemon_after_exception(
                    daemon_target[1], daemon_lifecycle
                )
                if not started:
                    output.run_log_write(
                        "# daemon recovery after batch exception FAILED: "
                        + detail
                    )
            except BaseException as exc:  # resume remains mandatory
                cleanup_error = exc
        if marker_acquired:
            try:
                transition_admin_update_state(ADMIN_UPDATE_STATE_RESUMING)
            except BaseException as exc:  # resume remains mandatory
                cleanup_error = cleanup_error or exc
        try:
            # Scope the batch resume to this invocation's token.
            resume_proof = resume_token_scope_with_proof(
                host,
                pause_token,
                multi_user=multi_user,
                queue_root=pause_queue_root,
            )
            resume_proof.require_clear()
            resumed_summary = resume_proof.summary
            _disarm_proven_pause_scope_without_managed_receipt()
            for result in results:
                result.resumed_summary = resumed_summary
        except BaseException as exc:
            cleanup_error = cleanup_error or exc
        if cleanup_error is not None:
            raise cleanup_error

    # v0.5.42: at most one env in the batch can be a vq self-update
    # (only one venv contains the running daemon). Walk results, and
    # the first env whose probe says "self-update" gets the restart
    # treatment; everything else is a quiet no-op. Restart happens
    # AFTER the whole batch finished + the queue resumed.
    # v0.6.1: same RESTARTING_DAEMON-only-when-restarting semantics
    # as update_env — the helper transitions internally.
    for name, prog in progs:
        restart_result = next((r for r in results if r.env == name), None)
        if restart_result is None:
            continue
        if not managed_daemon_restarts[name]:
            _maybe_restart_daemon(
                prog,
                restart_result,
                restart_daemon=restart_daemon,
            )
    transition_admin_update_state(ADMIN_UPDATE_STATE_VERIFYING)

    # Record every result before deciding whether the batch marker can be
    # cleared. A multi-user canonical-state write failure is itself a failed
    # env outcome; otherwise the CLI could report a clean batch while the
    # daemon still exposes stale LAST OK data.
    for result in results:
        try:
            record_update_outcome(
                result.env,
                result,
                admin_token=admin_token,
                multi_user=multi_user,
            )
        except AdminError as exc:
            result.work_errors.append(
                f"canonical admin status persistence failed: {exc}"
            )

    # v0.5.48 (Bug A fix): clear marker AFTER the _maybe_restart_daemon
    # loop so a failed self-update restart (which flips that env's
    # result.success to False) keeps the marker on disk. Pre-v0.5.48
    # the clear ran in the finally block above, before any restart
    # attempt — a failed restart cleared the marker while the daemon
    # stayed on stale code. See docs/audit_2026-05-17 § 2g.
    # v0.6.0: on partial-batch failure, transition to FAILED (sticky)
    # with a summary failure_reason instead of leaving the marker in
    # an in-flight state.
    if results and all(r.success for r in results):
        _clear_completed_admin_update_marker()
    else:
        failed = [r.env for r in results if not r.success]
        n_ok = sum(1 for r in results if r.success)
        reason = (
            f"batch incomplete: {n_ok}/{len(results)} ok; "
            f"failed envs: {', '.join(failed) or '(none recorded)'}"
        )
        transition_admin_update_state(
            ADMIN_UPDATE_STATE_FAILED, failure_reason=reason,
        )

    return results


def _validate_drain_wait_seconds(value: float) -> None:
    if not math.isfinite(value) or value < 0:
        raise AdminError(
            "drain_wait_seconds must be finite and >= 0 "
            f"(got {value})"
        )


def update_scheduler_host(
    host: str,
    cfg: config.Config,
    *,
    install: bool = False,
    force: bool = False,
    update_script_args: list[str] | None = None,
    drain_wait_seconds: float = 0.0,
    expected_sha: str | None = None,
    admin_token: str | None = None,
) -> SchedulerHostUpdateResult:
    """Refresh one scheduler helper under both Python and source fences."""
    _validate_drain_wait_seconds(drain_wait_seconds)
    project_root = _scheduler_helper_project_root()
    source_checkout = str(_canonical_lifecycle_checkout(project_root))
    with admin_update_ownership(), toolset_lifecycle_lock(
        [],
        action=f"update scheduler helper {host}",
        extra_resources=(("checkout", source_checkout),),
    ):
        return _update_scheduler_host_owned(
            host,
            cfg,
            install=install,
            force=force,
            update_script_args=update_script_args,
            drain_wait_seconds=drain_wait_seconds,
            expected_sha=expected_sha,
            admin_token=admin_token,
        )


def _update_scheduler_host_owned(
    host: str,
    cfg: config.Config,
    *,
    install: bool = False,
    force: bool = False,
    update_script_args: list[str] | None = None,
    drain_wait_seconds: float = 0.0,
    expected_sha: str | None = None,
    admin_token: str | None = None,
) -> SchedulerHostUpdateResult:
    """Refresh a scheduler-backed host through its remote provisioning command.

    This is the scheduler-host analogue of :func:`update_env`: it claims the
    admin-update marker so the daemon stops dispatching new jobs, refuses to run
    while any already-submitted scheduler job for this target is still active,
    then executes the configured command on the scheduler login host over SSH.

    ``drain_wait_seconds`` opts into the supported maintenance window: hold a
    drain lane for ``host`` so no new work is dispatched to it, then wait up to
    that many seconds for the jobs already on the cluster to finish. Zero (the
    default) keeps the historical one-shot refusal. This is the only supported
    way to satisfy the active-job guard on a node that is never idle; it does
    not weaken the guard, and it is not ``--force`` (which does not bypass this
    guard at all — it only overrides the marker).

    No program scoping here on purpose: ``scheduler_update_command`` rebuilds
    the cluster-managed checkouts that every job on the target executes from,
    so on this path every active job is a dependant.

    ``expected_sha`` pins the staged helper source to that exact commit from
    the managed runtime repository instead of the live driver checkout.
    Fleet rollouts always pass the accepted report's vq pin here: the report
    is the sole source of deployed identity, and a driver checkout that has
    moved ahead of the pin must never leak into a helper deployment.
    """
    if expected_sha is not None and not _FULL_SHA_RE.fullmatch(expected_sha):
        raise AdminError(
            "--expected-sha must be a full 40-character hex commit SHA"
        )
    host_cfg = _resolve_scheduler_update_host(host, cfg, require_managed=True)
    mode = "install" if install else "update"
    command = (
        host_cfg.scheduler_install_command
        if install
        else host_cfg.scheduler_update_command
    )
    if not command:
        field_name = (
            "scheduler_install_command" if install else "scheduler_update_command"
        )
        raise AdminError(
            f"scheduler host {host!r} has no {field_name} configured"
        )

    argv = _scheduler_update_argv(command, update_script_args)
    command_host_cfg = _scheduler_update_command_host_config(host_cfg)
    result = SchedulerHostUpdateResult(
        host=host,
        ssh=host_cfg.ssh,
        scheduler=host_cfg.scheduler,
        mode=mode,
        command=shlex.join(argv),
        command_ssh=command_host_cfg.ssh,
    )

    marker_env = f"scheduler:{host}"
    _guard_admin_update_marker(force=force, envs=[marker_env], host=host)
    multi_user_logs = cfg.multi_user.enabled or config.system_multi_user_enabled()
    with admin_run_log(
        host, what=f"admin update {host} (scheduler {mode})",
        multi_user=multi_user_logs,
    ) as run_log:
        if run_log.available:
            result.run_log_path = str(run_log.path)
        output.narrate(f"scheduler {mode} on {host} via {command_host_cfg.ssh}")
        return _update_scheduler_host_guarded(
            host,
            cfg,
            host_cfg=host_cfg,
            command_host_cfg=command_host_cfg,
            result=result,
            argv=argv,
            mode=mode,
            marker_env=marker_env,
            force=force,
            drain_wait_seconds=drain_wait_seconds,
            expected_sha=expected_sha,
            admin_token=admin_token,
        )


def _update_scheduler_host_guarded(
    host: str,
    cfg: config.Config,
    *,
    host_cfg: config.HostConfig,
    command_host_cfg: config.HostConfig,
    result: SchedulerHostUpdateResult,
    argv: list[str],
    mode: str,
    marker_env: str,
    force: bool,
    drain_wait_seconds: float,
    expected_sha: str | None = None,
    admin_token: str | None = None,
) -> SchedulerHostUpdateResult:
    """The marker-holding body of :func:`update_scheduler_host`.

    Split out only so the run log wraps the whole operation, including the
    marker acquire/release, without another level of indentation.
    """
    acquire_admin_update_marker(envs=[marker_env], host=host, force=force)
    transition_admin_update_state(ADMIN_UPDATE_STATE_PAUSED)
    command_started = False
    # Assigned the moment the lane is taken, BEFORE any waiting, so the
    # `finally` below releases it even if the wait is interrupted. See
    # _take_scheduler_drain_lane for the leak this shape prevents.
    drain_lease_id: str | None = None
    drain_lease_owner: str | None = None
    multi_user: bool | None = None
    try:
        transition_admin_update_state(ADMIN_UPDATE_STATE_VERIFYING)
        multi_user = cfg.multi_user.enabled or config.system_multi_user_enabled()
        # Whether this wait is load-bearing depends on the SITE, so it is asked
        # rather than assumed. An earlier revision of this comment asserted the
        # helper had "no per-SHA bundle and no atomic flip contract behind it"
        # and told the reader not to delete the wait. That was written before
        # pbs-cluster's and slurm-cluster's per-SHA wrapper scripts landed (908e5eceb,
        # 87621aad7) and was never rechecked against them; by 2026-08-01 both
        # hosts published an immutable per-SHA helper root and switched the
        # stable path by rename, and the claim was simply false.
        #
        # It was not a harmless stale comment. On 2026-08-01 a paper-critical
        # GPW job, four hours into a twelve-hour budget, was killed to satisfy
        # this wait -- for a rebuild that could not have touched it.
        #
        # What makes the wait unnecessary here is narrower than the runtime
        # lane's argument and worth stating exactly, because the helper is NOT
        # the runtime and the two are easy to conflate:
        #
        #   * the scheduler hosts run no vq daemon (scheduler_dispatch's
        #     "Arch 2 -- off-cluster, SSH-driven, stateless-on-cluster"), so
        #     there is no long-lived process to swap under;
        #   * dispatch and polling do not go through the helper at all --
        #     qsub/qstat/qdel are raw shell over SSH from the DRIVER, and all
        #     live job state is the driver's;
        #   * the helper is a short-lived per-invocation process, so an
        #     invocation in flight during the flip keeps its own root's inode.
        #
        # So a helper rebuild cannot lose an in-flight row: nothing on the
        # scheduler host is holding one.
        #
        # The proof is the site's own receipt, not this reasoning, and it fails
        # closed -- a host on the in-place rsync path (contrib/
        # update-scheduler-vq.sh, which rsyncs over the live tree and installs
        # editable) never emits one and keeps the wait.
        result.configured_command = shlex.join(argv)
        activation_proven, activation_reason = helper_activation_is_proven(
            host, result.configured_command
        )
        if activation_proven:
            result.drain_wait_seconds = drain_wait_seconds
            result.drain_waited_seconds = 0.0
            result.drain_lane_held = False
            result.drain_skipped_reason = (
                f"{activation_reason}; no drain is required "
                "(--drain-wait accepted and ignored)"
            )
            output.narrate(
                f"helper update on {host} proceeds without waiting for running "
                "work: the site deploy activates atomically"
            )
        else:
            # Pre-generate the durable ID before the RPC. If the daemon commits
            # the claim but its response is lost, the finally block can still
            # release the exact claim instead of leaking an unnameable hold.
            if drain_wait_seconds > 0:
                drain_lease_id = uuid.uuid4().hex
                drain_lease_owner = _scheduler_drain_lane_owner(
                    host, drain_lease_id
                )
                drain_lease_id = _take_scheduler_drain_lane(
                    host,
                    drain_wait_seconds=drain_wait_seconds,
                    lease_id=drain_lease_id,
                    admin_token=admin_token,
                    multi_user=multi_user,
                )
            drain_outcome = _await_scheduler_quiescence(
                host,
                host_cfg=host_cfg,
                multi_user=multi_user,
                deadline_seconds=drain_wait_seconds,
            )
            drain_outcome.lane_added = drain_lease_id is not None
            result.drain_wait_seconds = drain_wait_seconds
            result.drain_waited_seconds = drain_outcome.waited_seconds
            result.drain_lane_held = drain_lease_id is not None
            result.drain_skipped_reason = None
            if not drain_outcome.quiesced:
                result.active_jobs = drain_outcome.remaining
                result.work_errors.append(
                    _active_jobs_refusal(
                        host, drain_outcome, "rebuilding the cluster environment"
                    )
                )
                return result

        transition_admin_update_state(ADMIN_UPDATE_STATE_BUILDING)
        try:
            stage_path = _stage_scheduler_helper_source(
                host,
                host_cfg,
                command_host_cfg,
                result,
                expected_sha=expected_sha,
            )
        except (AdminError, transport.RemoteError, OSError) as exc:
            result.work_errors.append(f"scheduler helper staging failed: {exc}")
            return result
        argv = [
            "env",
            f"VQ_SCHEDULER_STAGE={stage_path}",
            f"VQ_SCHEDULER_EXPECTED_SOURCE_SHA={result.expected_source_sha}",
            "VQ_SCHEDULER_EXPECTED_TREE_SHA256="
            f"{result.expected_source_tree_sha256}",
            *argv,
        ]
        result.command = shlex.join(argv)
        command_started = True
        try:
            proc = _run_scheduler_update_command_with_heartbeat(
                command_host_cfg,
                argv,
                timeout=host_cfg.scheduler_update_timeout_seconds,
                mode=mode,
            )
        except transport.RemoteError as exc:
            result.work_errors.append(str(exc))
            return result
        result.command_rc = proc.returncode
        result.command_output = _combined_output(proc.stdout, proc.stderr)
        result.metrics.update(parse_deploy_metrics(result.command_output))
        apply_helper_activation_receipt(result)
        _run_log_command_output(
            f"scheduler {mode} command", result.command_rc, result.command_output
        )
        if result.command_rc == 0 and _wait_for_scheduler_helper_ready(
            host_cfg, result
        ):
            _verify_scheduler_helper_provenance(host_cfg, result)
            # Staging retention is a separate maintenance operation. Another
            # deployment may still be using an older generation, including on
            # a different host; verified activation is not a cleanup lease.
        return result
    finally:
        release_error = _release_scheduler_drain_lane(
            host,
            drain_lease_id,
            lease_owner=drain_lease_owner,
            admin_token=admin_token,
            recovery_driver=host_cfg.scheduler_driver,
            multi_user=multi_user,
        )
        if release_error is not None:
            result.work_errors.append(release_error)
        # Canonical LAST OK persistence is part of the update verdict: a
        # failed write lands in work_errors BEFORE the marker decision, so
        # a helper whose recorded state is unknown never reports success.
        try:
            record_scheduler_helper_outcome(result)
        except OSError as exc:
            result.work_errors.append(
                f"could not persist scheduler helper LAST OK state: {exc}"
            )
        transition_admin_update_state(ADMIN_UPDATE_STATE_VERIFYING)
        if result.success or not command_started:
            clear_admin_update_marker()
            result.marker_cleared = True
        else:
            result.marker_cleared = False
            # Prefer the actual work errors over the command rc. The rc-first
            # form recorded `failure_reason="scheduler update rc=0"` whenever a
            # post-command verification failed — literally naming success as
            # the cause, which is what the operator read off the poisoned
            # slurm-cluster marker. The runtime lane already gets this right; match it.
            reason = "; ".join(result.work_errors) or (
                f"scheduler {mode} rc={result.command_rc}"
                if result.command_rc is not None
                else f"scheduler {mode} failed"
            )
            transition_admin_update_state(
                ADMIN_UPDATE_STATE_FAILED, failure_reason=reason,
            )


def _slurm_allocation_argv(
    allocation: config.SchedulerBuildAllocation,
    deploy_argv: list[str],
    host_cfg: config.HostConfig,
    program: str,
) -> list[str]:
    """Wrap a deployment command as an ``sbatch --parsable --wait`` batch job.

    A daemonless SLURM login host has no fixed build node, so the compile is
    submitted to the scheduler from the login host. ``sbatch --wait`` runs the
    job server-side and exits with the job's own return code, so a multi-hour
    cold build survives an SSH blip that a foreground ``srun`` would not. Job
    stdout/stderr land in per-deployment log files under ``scratch_root`` that
    the wrapper streams back, so the build log survives on failure. The
    deployment command must be a valid batch script (shebang); ``sbatch``
    passes the identity args to it and the configured ``sbatch_args`` (account,
    partition, resources) select the allocation.
    """
    scratch_root = host_cfg.scratch_root or "/tmp"
    log_dir = f"{scratch_root}/.vq-admin/runtime-deploy/{program}"
    # No `set -e`: a failed sbatch must still let us capture rc and stream logs.
    script = (
        "set -uo pipefail\n"
        f"log_dir={shlex.quote(log_dir)}\n"
        'mkdir -p "$log_dir"\n'
        'out="$log_dir/deploy.out"\n'
        'err="$log_dir/deploy.err"\n'
        'rm -f "$out" "$err"\n'
        f"sbatch --parsable --wait {shlex.join(allocation.sbatch_args)} "
        '--output="$out" --error="$err" '
        f"{shlex.join(deploy_argv)}\n"
        "rc=$?\n"
        '[ -f "$out" ] && cat "$out"\n'
        '[ -f "$err" ] && cat "$err" >&2\n'
        'exit "$rc"\n'
    )
    return ["bash", "-c", script]


def _scheduler_verify_timeout(
    deployment: config.SchedulerRuntimeDeployment,
) -> float:
    """Transport timeout for the login-host runtime verify command.

    Default keeps the legacy one-minute cap for a cheap in-line import check
    (pbs-cluster). ``verify_timeout_seconds`` lifts it when the verify command must
    first acquire a compute-node allocation before it can run a runtime that
    refuses to execute on the login node (slurm-cluster), so the wait for a node plus
    the in-node import/banner check both fit inside the transport window.
    """
    override = deployment.verify_timeout_seconds
    if override is not None:
        return override
    return min(deployment.timeout_seconds or 60.0, 60.0)


def _scheduler_runtime_execution_context(
    *,
    host: str,
    program: str,
    expected_sha: str,
    expected_tag: str | None,
) -> fleet_operation.OperationExecutionContext | None:
    """Validate a nested rollout identity before any admin-side mutation."""
    try:
        context = fleet_operation.execution_context_from_environ()
        if context is None:
            return None
        identity = fleet_operation.validate_live_execution_context(context)
    except fleet_operation.OperationError as exc:
        raise AdminError(f"invalid fleet operation execution context: {exc}") from exc

    expected = {
        "phase": "scheduler-runtime",
        "action_id": f"scheduler-runtime:{host}:{program}",
        "host": host,
        "program": program,
        "target_sha": expected_sha,
        "target_tag": expected_tag,
    }
    for field_name, wanted in expected.items():
        actual = getattr(identity, field_name)
        if actual != wanted:
            label = {
                "target_sha": "target SHA",
                "target_tag": "target tag",
            }.get(field_name, field_name.replace("_", " "))
            raise AdminError(
                "invalid fleet operation execution context: "
                f"outer {label} is {actual!r}, expected {wanted!r}"
            )
    return context


def update_scheduler_runtime(
    host: str,
    program: str,
    cfg: config.Config,
    *,
    expected_sha: str,
    expected_tag: str | None = None,
    install: bool = False,
    force: bool = False,
    update_script_args: list[str] | None = None,
    drain_wait_seconds: float = 0.0,
) -> SchedulerRuntimeUpdateResult:
    """Update one scheduler runtime under Python and exact source fences."""
    source_resources: tuple[tuple[str, str], ...] = ()
    if cfg.vibeqc_source_repo is not None:
        source_checkout = str(
            _canonical_lifecycle_checkout(
                Path(cfg.vibeqc_source_repo).expanduser(),
            )
        )
        source_resources = (("checkout", source_checkout),)
    with admin_update_ownership(), toolset_lifecycle_lock(
        [],
        action=f"update scheduler runtime {host}:{program}",
        extra_resources=source_resources,
    ):
        return _update_scheduler_runtime_owned(
            host,
            program,
            cfg,
            expected_sha=expected_sha,
            expected_tag=expected_tag,
            install=install,
            force=force,
            update_script_args=update_script_args,
            drain_wait_seconds=drain_wait_seconds,
        )


def _update_scheduler_runtime_owned(
    host: str,
    program: str,
    cfg: config.Config,
    *,
    expected_sha: str,
    expected_tag: str | None = None,
    install: bool = False,
    force: bool = False,
    update_script_args: list[str] | None = None,
    drain_wait_seconds: float = 0.0,
) -> SchedulerRuntimeUpdateResult:
    """Build, atomically activate, and verify one scheduler-host runtime.

    The build command is a trusted host-maintainer hook.  vq owns the safety
    envelope around it: immutable identity arguments, driver-side marker,
    active scheduler-job refusal, bounded transport, independent login-host
    verification, exact receipt comparison, and persistent LAST OK state.
    """
    if not _FULL_SHA_RE.fullmatch(expected_sha):
        raise AdminError("--expected-sha must be an exact 40-hex commit SHA")
    expected_sha = expected_sha.lower()
    operation_context = _scheduler_runtime_execution_context(
        host=host,
        program=program,
        expected_sha=expected_sha,
        expected_tag=expected_tag,
    )
    host_cfg = _resolve_scheduler_update_host(host, cfg, require_managed=True)
    deployment = host_cfg.scheduler_runtime_deployments.get(program)
    if deployment is None:
        known = sorted(host_cfg.scheduler_runtime_deployments)
        raise AdminError(
            f"scheduler host {host!r} has no runtime deployment for "
            f"{program!r}; configured: {known or '(none)'}"
        )
    command = deployment.install_command if install else deployment.update_command
    if not command:
        raise AdminError(
            f"scheduler runtime {host}:{program} has no install_command configured"
        )

    identity_args = ["--program", program, "--expected-sha", expected_sha]
    if expected_tag is not None:
        identity_args.extend(["--tag", expected_tag])
    try:
        command_parts = shlex.split(command)
        verify_parts = shlex.split(deployment.verify_command)
    except ValueError as exc:
        raise AdminError(f"scheduler runtime command parse failed: {exc}") from exc
    # The deployment command (with operator + identity args) is the payload.
    deploy_argv = [
        *command_parts,
        *(update_script_args or []),
        *identity_args,
    ]
    verify_argv = [*verify_parts, *identity_args]
    if not deploy_argv or not verify_argv:
        raise AdminError("scheduler runtime command is empty after shlex.split")

    # Build host is either a fixed SSH target (update_host, pbs-cluster's dedicated
    # build node) or a per-deployment SLURM allocation (update_allocation, for a
    # daemonless SLURM login host with no fixed build node). The two are
    # mutually exclusive (enforced in config). The sbatch wrapping and any
    # source staging happen inside the guarded build phase below, so the
    # command_argv is finalized there.
    command_host_cfg = (
        host_cfg.model_copy(update={"ssh": deployment.update_host})
        if deployment.update_host is not None
        else host_cfg
    )
    mode = "install" if install else "update"
    result = SchedulerRuntimeUpdateResult(
        host=host,
        program=program,
        mode=mode,
        # Display the readable deploy command, not the sbatch wrapper argv.
        command=shlex.join(deploy_argv),
        command_ssh=command_host_cfg.ssh,
        verify_command=shlex.join(verify_argv),
        verify_ssh=host_cfg.ssh,
        expected_sha=expected_sha,
        expected_tag=expected_tag,
    )

    marker_env = f"scheduler-runtime:{host}:{program}"
    _guard_admin_update_marker(force=force, envs=[marker_env], host=host)
    multi_user_logs = cfg.multi_user.enabled or config.system_multi_user_enabled()
    with admin_run_log(
        f"{host}-{program}",
        what=f"admin update {program} {host} (runtime {mode})",
        multi_user=multi_user_logs,
    ) as run_log:
        if run_log.available:
            result.run_log_path = str(run_log.path)
        output.narrate(
            f"runtime {mode}: {program} on {host} at {expected_sha[:12]}"
            + (f" (tag {expected_tag})" if expected_tag else "")
        )
        output.narrate(f"build via {command_host_cfg.ssh}", output.Level.VERBOSE)
        return _update_scheduler_runtime_guarded(
            host,
            program,
            cfg,
            host_cfg=host_cfg,
            command_host_cfg=command_host_cfg,
            deployment=deployment,
            result=result,
            deploy_argv=deploy_argv,
            verify_argv=verify_argv,
            expected_sha=expected_sha,
            mode=mode,
            marker_env=marker_env,
            force=force,
            drain_wait_seconds=drain_wait_seconds,
            operation_context=operation_context,
        )


PROGRAM_REPO_SLUGS = {
    "vibe-view": "mpei/vibe-view",
    "vibeview-dev": "mpei/vibe-view",
    "vibeqc-queue": "mpei/vibe-queue",
}
"""Repository each managed program is built from, after the 2026-09-08 split.

Anything not listed comes from vibe-qc (``vibeqc-dev``, ``vibeqc-release``,
and the mace/skala variants). This mirrors the same derivation the cluster
site-provided source stagers make, so the driver
and the host agree on which upstream a program belongs to.
"""

DEFAULT_PROGRAM_REPO_SLUG = "mpei/vibe-qc"


def program_source_repo(cfg: config.Config, program: str) -> Path:
    """The driver-local checkout a program's source is staged from.

    Until the split every program lived in one tree, so a single
    ``scheduler_runtime_source_repo`` was sufficient. It is not any more: the
    viewer is its own repository, and staging it from a vibe-qc clone builds
    whatever viewer that clone happens to carry -- or nothing at all, once
    vibe-qc stops carrying one.

    Resolution order is per-program first, then the historical single path, so
    a driver that has not yet declared ``pin_source_repos`` keeps working for
    vibe-qc programs exactly as before. Both spellings of the vibe-qc checkout
    resolve through :attr:`config.Config.vibeqc_source_repo`, which is also
    where the two are required to agree.
    """
    slug = PROGRAM_REPO_SLUGS.get(program, DEFAULT_PROGRAM_REPO_SLUG)
    configured = cfg.pin_source_repos.get(slug)
    if configured:
        return Path(configured).expanduser()
    if slug == DEFAULT_PROGRAM_REPO_SLUG and cfg.vibeqc_source_repo:
        return Path(cfg.vibeqc_source_repo).expanduser()
    raise AdminError(
        f"program {program!r} is built from {slug!r}, but no local checkout "
        f"is configured for it. Add it under [pin_source_repos] in the driver "
        f'config, e.g.\n    "{slug}" = "/path/to/checkout"'
    )


_PUSH_FED_MIRROR_PROBE = r"""
set -u
d=$1
if [ ! -d "$d" ]; then printf 'state=missing\n'; exit 0; fi
if ! bare=$(git -C "$d" rev-parse --is-bare-repository 2>/dev/null); then
  printf 'state=not-a-repo\n'; exit 0
fi
abs=$(cd "$d" && pwd -P) || abs=$d
origin=$(git -C "$d" remote get-url origin 2>/dev/null) || origin=
origin_abs=$origin
if [ -n "$origin" ]; then
  # Resolve from inside the mirror, because that is what git does with a
  # relative remote -- and "." is the most natural way to write a self-
  # referential one. Resolving against this shell's cwd instead reported a
  # working push-fed mirror as fetch-fed. An absolute path is unaffected by
  # the extra cd, and a URL simply fails it and keeps its raw spelling.
  origin_abs=$(cd "$d" && cd "$origin" 2>/dev/null && pwd -P) || origin_abs=$origin
fi
printf 'state=present\nbare=%s\nabs=%s\norigin=%s\norigin_abs=%s\n' \
  "$bare" "$abs" "$origin" "$origin_abs"
"""
"""Report what a configured ``feed_source_mirror`` actually is, on the host.

Emits ``key=value`` lines rather than exiting non-zero, so a probe that ran
and found nothing is distinguishable from a probe that could not run.
"""


def _verify_push_fed_mirror(
    host_cfg: config.HostConfig,
    mirror: str,
    result: SchedulerRuntimeUpdateResult,
) -> bool:
    """Check the mirror is push-fed before feeding it, or say how to fix it.

    vq does not create this mirror and never has; the convention it must
    satisfy was undocumented and is not what ``git init --bare`` leaves
    behind. A bare repo created that way has **no** ``origin``, and the
    preparer identifies a push-fed mirror by whether ``origin`` points at the
    mirror's own path -- so it tries to fetch and dies with ``fatal: 'origin'
    does not appear to be a git repository``. That happens after the feed has
    already succeeded, several minutes into a deployment, and reads as a
    problem with the source rather than with the setup.

    Deliberately a check and not a repair. An ``origin`` pointing somewhere
    else is not damage: it is a *fetch-fed* mirror, a different and valid
    arrangement, and silently repointing it at itself would convert a working
    setup into a broken one. Creating a missing mirror is likewise left alone
    -- it would turn a typo in ``feed_source_mirror`` into a second, empty
    mirror that pushes cleanly while the preparer keeps reading the real one.
    """
    probe = transport.run_remote_shell(
        host_cfg, "sh", "-c", _PUSH_FED_MIRROR_PROBE, "vq-mirror-probe", mirror,
        check=False,
        timeout=transport.DEFAULT_REMOTE_SHELL_TIMEOUT_SECONDS,
    )
    where = f"{host_cfg.ssh}:{mirror}"
    if probe.returncode != 0:
        detail = _last_output_line(_combined_output(probe.stdout, probe.stderr))
        result.work_errors.append(
            f"mirror feed: could not inspect {where}: "
            f"{detail or f'rc={probe.returncode}'}"
        )
        return False
    fields = dict(
        line.split("=", 1)
        for line in probe.stdout.splitlines()
        if "=" in line
    )
    state = fields.get("state")
    abs_path = fields.get("abs") or mirror
    # Every remedy below is meant to be pasted. A path needing quoting would
    # otherwise produce a command that runs and does the wrong thing, which is
    # worse than printing no remedy at all.
    quoted_mirror = shlex.quote(mirror)
    quoted_abs = shlex.quote(abs_path)
    create = (
        f"    ssh {shlex.quote(host_cfg.ssh)} "
        + shlex.quote(
            f"git init --bare {quoted_mirror} && "
            f"git -C {quoted_mirror} remote add origin {quoted_mirror}"
        )
    )
    if state == "missing":
        result.work_errors.append(
            f"mirror feed: {where} does not exist. vq does not create the "
            f"mirror; create it push-fed, with an origin naming itself:\n"
            f"{create}"
        )
        return False
    if state != "present":
        result.work_errors.append(
            f"mirror feed: {where} is not a git repository. Recreate it "
            f"push-fed:\n{create}"
        )
        return False
    if fields.get("bare") != "true":
        result.work_errors.append(
            f"mirror feed: {where} is a working checkout, not a bare "
            f"repository; a push into its checked-out branch is refused. "
            f"Recreate it push-fed:\n{create}"
        )
        return False
    origin = fields.get("origin", "")
    if not origin:
        result.work_errors.append(
            f"mirror feed: {where} has no origin, so the preparer will read "
            f"it as fetch-fed and fail with \"fatal: 'origin' does not appear "
            f"to be a git repository\" after this feed succeeds. A push-fed "
            f"mirror names itself:\n"
            f"    ssh {shlex.quote(host_cfg.ssh)} "
            + shlex.quote(
                f"git -C {quoted_mirror} remote add origin {quoted_abs}"
            )
        )
        return False
    if fields.get("origin_abs") != abs_path:
        result.work_errors.append(
            f"mirror feed: {where} has origin {origin!r}, which is not the "
            f"mirror itself, so it is fetch-fed. feed_source_mirror pushes "
            f"into the mirror and the preparer expects a push-fed one. Drop "
            f"feed_source_mirror to keep fetching, or make it push-fed:\n"
            f"    ssh {shlex.quote(host_cfg.ssh)} "
            + shlex.quote(
                f"git -C {quoted_mirror} remote set-url origin {quoted_abs}"
            )
        )
        return False
    return True


def _feed_and_prepare_runtime_source(
    host: str,
    cfg: config.Config,
    *,
    host_cfg: config.HostConfig,
    deployment: config.SchedulerRuntimeDeployment,
    result: SchedulerRuntimeUpdateResult,
    program: str,
    expected_sha: str,
    expected_tag: str | None,
) -> bool:
    """Automate offline-build-node source feed and preparation.

    pbs-cluster's runbook required two manual commands before every runtime
    update: push the exact pin into the login host's push-fed mirror, then
    run the preparer. Both are mechanical and driver-executable, so when
    the deployment profile opts in they run here — after quiescence,
    before the build, failing closed with the current runtime intact.
    Returns True to proceed with the build, False after appending the
    failure to ``result.work_errors``.
    """
    if deployment.feed_source_mirror:
        try:
            repo = str(program_source_repo(cfg, program))
        except AdminError as exc:
            result.work_errors.append(str(exc))
            return False
        remote = f"{host_cfg.ssh}:{deployment.feed_source_mirror}"
        # Before the fetch, which is allowed 600s, and before the push that
        # would otherwise succeed into a mirror the preparer cannot use.
        if not _verify_push_fed_mirror(
            host_cfg, deployment.feed_source_mirror, result,
        ):
            return False
        output.narrate(
            f"feeding source mirror {remote} with {expected_sha[:12]}"
        )
        try:
            fetch = _mutating_git_run(
                ["git", "-C", repo, "fetch", "origin", "--tags", "--prune"],
                capture_output=True, text=True, timeout=600,
            )
        except subprocess.TimeoutExpired:
            result.work_errors.append("mirror feed: git fetch timed out")
            return False
        if fetch.returncode != 0:
            # Fail closed: feeding from a stale clone pins the wrong tree.
            _run_log_command_output(
                "mirror feed fetch", fetch.returncode,
                _combined_output(fetch.stdout, fetch.stderr),
            )
            result.work_errors.append(
                "mirror feed: git fetch failed: "
                + (fetch.stderr.strip().splitlines()[-1]
                   if fetch.stderr.strip() else f"rc={fetch.returncode}")
            )
            return False
        # Feed only the authority for this lane. A release feed must not move
        # main behind the accepted dev pin, and a branch-tip feed must not
        # move release. Each preparer fails closed unless both refs for its
        # lane agree exactly; tags carry the immutable release identity.
        if program == "vibeqc-release":
            refspecs = [
                f"+{expected_sha}:refs/heads/release",
                f"+{expected_sha}:refs/remotes/origin/release",
            ]
        else:
            refspecs = [
                f"+{expected_sha}:refs/heads/main",
                f"+{expected_sha}:refs/remotes/origin/main",
            ]
        refspecs.append("+refs/tags/*:refs/tags/*")
        try:
            push = _mutating_git_run(
                ["git", "-C", repo, "push", remote, *refspecs],
                capture_output=True, text=True, timeout=900,
            )
        except subprocess.TimeoutExpired:
            result.work_errors.append("mirror feed: git push timed out")
            return False
        _run_log_command_output(
            "mirror feed push", push.returncode,
            _combined_output(push.stdout, push.stderr),
        )
        if push.returncode != 0:
            result.work_errors.append(
                "mirror feed: git push failed: "
                + (push.stderr.strip().splitlines()[-1]
                   if push.stderr.strip() else f"rc={push.returncode}")
            )
            return False
        result.mirror_fed = True
    if deployment.prepare_command:
        argv = [
            *shlex.split(deployment.prepare_command),
            "--program", program,
            "--expected-sha", expected_sha,
        ]
        if expected_tag:
            argv += ["--tag", expected_tag]
        output.narrate(f"preparing source bundle on {host_cfg.ssh}")
        try:
            proc = _run_scheduler_update_command_with_heartbeat(
                host_cfg,
                argv,
                timeout=deployment.prepare_timeout_seconds,
                mode=f"prepare {program}",
            )
        except transport.RemoteError as exc:
            result.work_errors.append(f"prepare command failed: {exc}")
            return False
        result.prepare_rc = proc.returncode
        result.prepare_output = _combined_output(proc.stdout, proc.stderr)
        _run_log_command_output(
            "prepare command", proc.returncode, result.prepare_output,
        )
        if proc.returncode != 0:
            detail = _last_output_line(result.prepare_output)
            result.work_errors.append(
                f"prepare command rc={proc.returncode}"
                + (f": {detail}" if detail else "")
            )
            return False
    return True


def _update_scheduler_runtime_guarded(
    host: str,
    program: str,
    cfg: config.Config,
    *,
    host_cfg: config.HostConfig,
    command_host_cfg: config.HostConfig,
    deployment: config.SchedulerRuntimeDeployment,
    result: SchedulerRuntimeUpdateResult,
    deploy_argv: list[str],
    verify_argv: list[str],
    expected_sha: str,
    mode: str,
    marker_env: str,
    force: bool,
    drain_wait_seconds: float,
    operation_context: fleet_operation.OperationExecutionContext | None,
) -> SchedulerRuntimeUpdateResult:
    """The marker-holding body of :func:`update_scheduler_runtime`.

    Split out so the run log wraps the whole operation including marker
    acquire/release; see :func:`_update_scheduler_host_guarded`.
    """
    acquire_admin_update_marker(envs=[marker_env], host=host, force=force)
    transition_admin_update_state(ADMIN_UPDATE_STATE_PAUSED)
    command_started = False
    # Assigned the moment the lane is taken, BEFORE any waiting, so the
    # `finally` below releases it even if the wait is interrupted. See
    # _take_scheduler_drain_lane for the leak this shape prevents.
    drain_lease_id: str | None = None
    try:
        transition_admin_update_state(ADMIN_UPDATE_STATE_VERIFYING)
        # No drain, and no waiting for running work. A runtime deployment's
        # contract (config.SchedulerRuntimeDeployment) already REQUIRES the
        # command to "stage away from the active path and atomically switch it
        # only after its own build checks pass", and nothing reclaims a
        # per-SHA bundle once published, so a running job keeps the exact
        # bundle it started with for its whole life. There was never anything
        # for this wait to protect: it only delayed the release.
        #
        # The contract is still ENFORCED rather than assumed --
        # _apply_scheduler_runtime_receipt fails the update unless the receipt
        # reports activation='atomic' plus an active_path and quiescent=true.
        # A site whose command mutates the active path in place therefore
        # fails verification instead of corrupting a running job silently.
        #
        # `--drain-wait` is accepted and ignored here rather than removed:
        # existing fleet invocations and the rollout planner both still pass
        # it, and breaking them buys nothing. The helper path
        # (_update_scheduler_host_locked) keeps its wait -- see the comment
        # there for why that one is not dead weight.
        result.drain_wait_seconds = drain_wait_seconds
        result.drain_waited_seconds = 0.0
        result.drain_lane_held = False
        result.drain_skipped_reason = (
            "runtime deployments stage out-of-place and activate atomically, "
            "so a running job keeps its own bundle; no drain is required "
            "(--drain-wait accepted and ignored)"
        )

        # Air-gapped source automation (pbs-cluster): feed the push-fed mirror with
        # the exact pin from the driver's clone, then run the login-host
        # preparer. Both were manual per-release runbook steps; both run
        # before the build starts, so a failure leaves the runtime intact.
        if not _feed_and_prepare_runtime_source(
            host,
            cfg,
            host_cfg=host_cfg,
            deployment=deployment,
            result=result,
            program=program,
            expected_sha=expected_sha,
            expected_tag=result.expected_tag,
        ):
            return result

        # Stage vibe-qc source at the exact SHA from the driver, for a build
        # host that cannot itself reach the source (a daemonless SLURM login
        # host with no repo credentials). The staged archive path is handed to
        # the deployment command as `--source-archive`. This runs before the
        # build starts, so a staging failure leaves the current runtime intact.
        if deployment.stage_source:
            try:
                remote_archive = _stage_scheduler_runtime_source(
                    host, command_host_cfg, program, expected_sha, cfg, result
                )
            except AdminError as exc:
                result.work_errors.append(f"source staging failed: {exc}")
                return result
            deploy_argv = [*deploy_argv, "--source-archive", remote_archive]
            result.command = shlex.join(deploy_argv)

        # IID 288: a release runtime must carry its real tag when one exists at
        # the pinned SHA, even when the caller omitted --tag. Dev and view are
        # branch-tip lanes: rule-A release reports can pin them to the same
        # commit as the release without changing that identity contract. Do
        # not leak the co-located release tag into those commands (#535).
        # Resolve from the driver's source checkout; never synthesise "v" +
        # version. On success the tag rides both deploy and verify argv and
        # round-trips through the receipt comparison. When no release tag
        # resolves, keep the manifest's null and warn loudly.
        if result.expected_tag is None and program == "vibeqc-release":
            resolved_tag = _resolve_tag_for_deploy(cfg, expected_sha)
            if resolved_tag is not None:
                result.expected_tag = resolved_tag
                deploy_argv = [*deploy_argv, "--tag", resolved_tag]
                verify_argv = [*verify_argv, "--tag", resolved_tag]
                result.command = shlex.join(deploy_argv)
                result.verify_command = shlex.join(verify_argv)
            else:
                log.warning(
                    "scheduler runtime deploy for %s:%s at %s has no "
                    "resolvable tag; the build manifest will record tag=null "
                    "(never synthesised from version)",
                    host,
                    program,
                    expected_sha,
                )

        if deployment.update_allocation is not None:
            command_argv = _slurm_allocation_argv(
                deployment.update_allocation, deploy_argv, host_cfg, program
            )
        else:
            command_argv = deploy_argv

        transition_admin_update_state(ADMIN_UPDATE_STATE_BUILDING)
        command_started = True
        try:
            if deployment.detached_build:
                proc = _run_detached_scheduler_command(
                    command_host_cfg,
                    command_argv,
                    scratch_root=host_cfg.scratch_root or "/tmp",
                    program=program,
                    timeout=deployment.timeout_seconds,
                    mode=f"runtime {program} {mode}",
                    operation_context=operation_context,
                )
            else:
                proc = _run_scheduler_update_command_with_heartbeat(
                    command_host_cfg,
                    command_argv,
                    timeout=deployment.timeout_seconds,
                    mode=f"runtime {program} {mode}",
                )
        except transport.RemoteError as exc:
            result.work_errors.append(str(exc))
            return result
        result.command_rc = proc.returncode
        result.command_output = _combined_output(proc.stdout, proc.stderr)
        result.metrics.update(parse_deploy_metrics(result.command_output))
        if deployment.detached_build:
            # The detached poll loop already streamed the output into the
            # run log; a second bulk tee would duplicate megabytes.
            output.run_log_write(
                f"--- deployment command rc={result.command_rc} "
                "(output streamed above by the detached poll loop) ---"
            )
        else:
            _run_log_command_output(
                "deployment command", result.command_rc, result.command_output
            )
        if proc.returncode != 0:
            result.work_errors.append(f"deployment command rc={proc.returncode}")
            return result

        transition_admin_update_state(ADMIN_UPDATE_STATE_VERIFYING)
        try:
            verify = transport.run_remote_shell(
                host_cfg,
                *verify_argv,
                check=False,
                timeout=_scheduler_verify_timeout(deployment),
            )
        except transport.RemoteError as exc:
            result.work_errors.append(f"runtime verification failed: {exc}")
            return result
        result.verify_rc = verify.returncode
        result.verify_output = _combined_output(verify.stdout, verify.stderr)
        _run_log_command_output(
            "verification command", result.verify_rc, result.verify_output
        )
        if verify.returncode != 0:
            result.work_errors.append(f"verification command rc={verify.returncode}")
            return result
        _apply_scheduler_runtime_receipt(result, verify.stdout)
        return result
    finally:
        _release_scheduler_drain_lane(host, drain_lease_id)
        # Before the outcome is persisted, so the receipt records what was
        # reclaimed. A deploy that verified drops its own upload staging; one
        # that did not keeps it for forensics, and older stages are trimmed so
        # failures cannot accumulate without limit (#61).
        if deployment.stage_source:
            _reclaim_runtime_source_stage(
                host,
                command_host_cfg,
                result,
                remove_current=result.success,
            )
        try:
            record_scheduler_runtime_outcome(result)
        except OSError as exc:
            result.work_errors.append(
                f"could not persist scheduler runtime LAST OK state: {exc}"
            )
        transition_admin_update_state(ADMIN_UPDATE_STATE_VERIFYING)
        if result.success or not command_started:
            clear_admin_update_marker()
            result.marker_cleared = True
        else:
            result.marker_cleared = False
            reason = "; ".join(result.work_errors) or (
                f"scheduler runtime {program} {mode} failed"
            )
            transition_admin_update_state(
                ADMIN_UPDATE_STATE_FAILED, failure_reason=reason,
            )


def _apply_scheduler_runtime_receipt(
    result: SchedulerRuntimeUpdateResult, stdout: str
) -> None:
    """Parse and enforce the scheduler runtime verification receipt."""
    try:
        payload = json.loads(stdout.strip())
    except json.JSONDecodeError as exc:
        result.work_errors.append(f"verification receipt is not valid JSON: {exc}")
        return
    if not isinstance(payload, dict):
        result.work_errors.append("verification receipt must be a JSON object")
        return
    result.actual_sha = str(payload.get("source_sha", "")).lower() or None
    raw_tag = payload.get("tag")
    result.actual_tag = str(raw_tag) if raw_tag is not None else None
    result.healthy = payload.get("healthy") is True
    raw_activation = payload.get("activation")
    result.activation = str(raw_activation) if raw_activation is not None else None
    raw_path = payload.get("active_path")
    result.active_path = str(raw_path) if raw_path is not None else None
    raw_detail = payload.get("health_detail")
    result.health_detail = str(raw_detail) if raw_detail is not None else None
    result.quiescent = payload.get("quiescent") is True
    result.updater_pid = payload.get("updater_pid")

    if payload.get("program") != result.program:
        result.work_errors.append(
            f"receipt program mismatch: expected {result.program!r}, "
            f"got {payload.get('program')!r}"
        )
    if result.actual_sha != result.expected_sha:
        result.work_errors.append(
            f"receipt SHA mismatch: expected {result.expected_sha}, "
            f"got {result.actual_sha or '(missing)'}"
        )
    if result.actual_tag != result.expected_tag:
        result.work_errors.append(
            f"receipt tag mismatch: expected {result.expected_tag!r}, "
            f"got {result.actual_tag!r}"
        )
    if not result.healthy:
        result.work_errors.append("receipt did not report healthy=true")
    if result.activation != "atomic":
        result.work_errors.append(
            "receipt did not report activation='atomic'"
        )
    if not result.active_path:
        result.work_errors.append("receipt did not report active_path")
    if not result.quiescent:
        result.work_errors.append("receipt did not report quiescent=true")
    if result.updater_pid is not None:
        result.work_errors.append(
            "receipt reported an active updater_pid: "
            f"{result.updater_pid!r}"
        )


def _resolve_scheduler_update_host(
    host: str,
    cfg: config.Config,
    *,
    require_managed: bool = False,
) -> config.HostConfig:
    """Resolve a scheduler host, optionally enforcing mutable fleet scope."""
    try:
        host_cfg = cfg.host(host)
    except config.ConfigError as exc:
        raise AdminError(str(exc)) from None
    if host_cfg.scheduler == "local":
        raise AdminError(
            f"host {host!r} is not a scheduler host; use "
            "`vq admin update ENV [HOST]` for venv programs"
        )
    if require_managed and host_cfg.fleet_role != "managed":
        canonical = (
            f"; update canonical host {host_cfg.fleet_canonical_host!r} instead"
            if host_cfg.fleet_role == "alias"
            else ""
        )
        raise AdminError(
            f"scheduler update target {host!r} must have "
            f"fleet_role='managed'; resolved {host_cfg.fleet_role!r}"
            f"{canonical}"
        )
    return host_cfg


def _scheduler_update_command_host_config(
    host_cfg: config.HostConfig,
) -> config.HostConfig:
    """Return the SSH target used for scheduler update/install commands.

    Job submission and qstat stay attached to the scheduler host's normal
    ``ssh`` / ``remote_scheduler_host`` settings. This helper only redirects
    the maintenance shell command for sites where compilation must happen on a
    dedicated build node.
    """
    if host_cfg.scheduler_update_host is None:
        return host_cfg
    return host_cfg.model_copy(update={"ssh": host_cfg.scheduler_update_host})


def _scheduler_helper_project_root() -> Path:
    """Locate the source project that owns the running ``vq`` package."""
    anchor = Path(__file__).resolve()
    for candidate in anchor.parents:
        if (candidate / "pyproject.toml").is_file() and (
            candidate / "src" / "vq"
        ).is_dir():
            return candidate
    raise AdminError(
        "could not locate the vibe-queue project root for scheduler staging"
    )


def vq_project_root(root: Path, *, strict: bool = True) -> Path:
    """Locate the vq package root inside a checkout, in either layout.

    ``vibe-queue/`` was a directory in the monorepo and is its own repository
    since the 2026-09-08 split, so the package root is either one level down
    or the checkout itself. Detected rather than assumed: hardcoding the
    monorepo shape made rollout-latest look for ``<repo>/vibe-queue`` inside
    a checkout that already WAS vibe-queue.

    ``strict`` decides what an unrecognisable checkout means. A managed
    program must name a real vq tree, so that caller wants the error here.
    A rollout resolving a report's repository does not: git is the
    authoritative check a moment later and reports the specific failure, so
    returning the root keeps that diagnosis instead of pre-empting it with a
    layout complaint.
    """
    root = Path(root).resolve()
    for candidate in (root / "vibe-queue", root):
        if (candidate / "pyproject.toml").is_file() and (
            candidate / "src" / "vq"
        ).is_dir():
            return candidate
    if not strict:
        return root
    raise AdminError(
        f"vq checkout {root} contains neither vibe-queue/src/vq nor src/vq"
    )


def _vq_project_root_for_program(prog: config.VenvProgram) -> Path:
    """Bind vq package provenance to the managed program's own checkout."""
    return vq_project_root(Path(prog.git_dir))


def _scheduler_helper_git_root(project_root: Path) -> tuple[Path, str]:
    """Resolve the enclosing git root and the vibe-queue pathspec inside it."""
    try:
        root_proc = subprocess.run(
            ["git", "-C", str(project_root), "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            timeout=30,
            stdin=subprocess.DEVNULL,
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        raise AdminError(f"could not locate scheduler-helper git root: {exc}") from exc
    if root_proc.returncode != 0:
        raise AdminError(
            "could not locate scheduler-helper git root: "
            + (root_proc.stderr.strip() or "git rev-parse failed")
        )
    git_root = Path(root_proc.stdout.strip()).resolve()
    try:
        relative = project_root.resolve().relative_to(git_root)
    except ValueError as exc:
        raise AdminError(
            f"vibe-queue project {project_root} is outside git root {git_root}"
        ) from exc
    pathspec = "." if relative == Path(".") else relative.as_posix()
    return git_root, pathspec


def _resolve_helper_pin_commit(git_root: Path, expected_sha: str) -> None:
    """Ensure the pinned helper commit exists locally, fetching if needed."""

    def _known() -> bool:
        proc = _mutating_git_run(
            [
                "git",
                "-C",
                str(git_root),
                "cat-file",
                "-e",
                f"{expected_sha}^{{commit}}",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        return proc.returncode == 0

    if _known():
        return
    fetch = _mutating_git_run(
        ["git", "-C", str(git_root), "fetch", "origin", "main", "--tags", "--quiet"],
        capture_output=True,
        text=True,
        timeout=GIT_PULL_TIMEOUT_SECONDS,
    )
    if fetch.returncode != 0:
        raise AdminError(
            f"pinned helper commit {expected_sha} is not local and fetch "
            "failed: " + (fetch.stderr.strip() or "git fetch failed")
        )
    if not _known():
        raise AdminError(
            f"pinned helper commit {expected_sha} does not exist on the "
            "managed runtime repository after fetching origin/main; verify "
            "the accepted release report against this checkout"
        )


def _scheduler_helper_archive(
    project_root: Path,
    archive: Path,
    source_sha: str,
    *,
    require_clean_tree: bool = True,
) -> str:
    """Archive the exact git tree and return its SHA-256 digest.

    ``git archive <sha>`` reads immutable objects, so a pinned-SHA stage is
    correct regardless of worktree state; the dirty-tree refusal only guards
    the live-driver-tree path, where HEAD plus uncommitted edits would ship
    content that does not match the recorded SOURCE-SHA.
    """
    git_root, pathspec = _scheduler_helper_git_root(project_root)
    if require_clean_tree:
        dirty = _mutating_git_run(
            ["git", "-C", str(git_root), "status", "--porcelain", "--", pathspec],
            capture_output=True,
            text=True,
            timeout=30,
            env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"},
        )
        if dirty.returncode != 0:
            raise AdminError(
                "could not inspect scheduler-helper source tree: "
                + (dirty.stderr.strip() or "git status failed")
            )
        if dirty.stdout.strip():
            raise AdminError(
                "scheduler-helper source tree is dirty; commit or remove local "
                "changes before creating a deployment archive"
            )
    treeish = source_sha if pathspec == "." else f"{source_sha}:{pathspec}"
    archive_proc = _mutating_git_run(
        [
            "git",
            "--no-replace-objects",
            "-C",
            str(git_root),
            "archive",
            "--format=tar.gz",
            "--prefix=vibe-queue/",
            "-o",
            str(archive),
            treeish,
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if archive_proc.returncode != 0:
        raise AdminError(
            "could not archive scheduler-helper source: "
            + (archive_proc.stderr.strip() or "git archive failed")
        )
    return hashlib.sha256(archive.read_bytes()).hexdigest()


def _helper_archive_tree_digest(archive: Path) -> str:
    """Content digest of ``src/vq`` inside a staged helper archive.

    For a pinned stage the digest must describe the archived tree, not the
    driver's installed package — the two can legitimately differ whenever
    the driver runs ahead of the accepted report pin.
    """
    with tempfile.TemporaryDirectory(prefix="vq-helper-pin-") as raw:
        extract_root = Path(raw)
        with tarfile.open(archive, "r:gz") as tar:
            tar.extractall(extract_root, filter="data")
        tree = extract_root / "vibe-queue" / "src" / "vq"
        if not tree.is_dir():
            raise AdminError(
                "staged helper archive does not contain vibe-queue/src/vq"
            )
        return source_tree_sha256(tree)


def source_tree_sha256_at_git_commit(
    project_root: Path,
    source_sha: str,
) -> str:
    """Hash ``src/vq`` from one exact local commit object.

    Fleet convergence must compare a daemon with the accepted report's
    immutable package bytes, never with a dirty or ahead driver worktree.
    This deliberately performs no fetch, checkout, or status probe: report
    discovery is responsible for making the accepted commit available.
    """
    if not _FULL_SHA_RE.fullmatch(source_sha):
        raise AdminError(
            "accepted vq source SHA must be a full 40-character Git object ID"
        )
    normalized_sha = source_sha.lower()
    git_root, _pathspec = _scheduler_helper_git_root(project_root)
    try:
        type_probe = subprocess.run(
            [
                "git",
                "--no-replace-objects",
                "-C",
                str(git_root),
                "cat-file",
                "-t",
                normalized_sha,
            ],
            capture_output=True,
            text=True,
            timeout=30,
            stdin=subprocess.DEVNULL,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise AdminError(
            f"could not inspect accepted vq commit {normalized_sha}: {exc}"
        ) from exc
    if type_probe.returncode != 0 or type_probe.stdout.strip() != "commit":
        detail = type_probe.stderr.strip() or type_probe.stdout.strip()
        suffix = f": {detail}" if detail else ""
        raise AdminError(
            f"accepted vq source {normalized_sha} is not a local commit object"
            f"{suffix}"
        )
    try:
        with tempfile.TemporaryDirectory(prefix="vq-accepted-source-") as raw:
            archive = Path(raw) / "vibe-queue.tar.gz"
            _scheduler_helper_archive(
                project_root,
                archive,
                f"{normalized_sha}^{{commit}}",
                require_clean_tree=False,
            )
            return _helper_archive_tree_digest(archive)
    except AdminError:
        raise
    except (OSError, subprocess.TimeoutExpired, tarfile.TarError) as exc:
        raise AdminError(
            f"could not derive accepted vq package digest: {exc}"
        ) from exc


_RUNTIME_SOURCE_STAGE_SEGMENT = "/.vq-admin/runtime-source/"
"""The path segment every runtime-source stage root contains.

:func:`_reclaim_runtime_source_stage` refuses to delete anything under a root
without it. The root is built from ``scratch_root``, which is operator config,
and the reclaim is an ``rm -rf`` on a remote host: this keeps a mistyped or
hostile ``scratch_root`` from turning it into one somewhere else."""

_RUNTIME_SOURCE_RECLAIM_SCRIPT = """\
set -eu
root=$1
keep=$2
remove=$3
[ -d "$root" ] || { printf 'reclaimed=0 retained=0\\n'; exit 0; }
cd "$root"
names=$(ls -1t 2>/dev/null | grep -E '^[0-9a-fA-F]{40}-[0-9a-fA-F]{32}$' || true)
reclaimed=0
retained=0
for name in $names; do
    if [ -L "$name" ] || [ ! -d "$name" ]; then
        continue
    fi
    if [ -n "$remove" ] && [ "$name" = "$remove" ]; then
        rm -rf -- "$name"
        reclaimed=$((reclaimed + 1))
        continue
    fi
    if [ "$retained" -lt "$keep" ]; then
        retained=$((retained + 1))
    else
        rm -rf -- "$name"
        reclaimed=$((reclaimed + 1))
    fi
done
printf 'reclaimed=%s retained=%s\\n' "$reclaimed" "$retained"
"""
"""Reclaim one program's runtime-source stages on the build host.

``ls -1t`` orders by mtime, newest first, and the ``grep`` keeps only the exact
``<40 hex>-<32 hex>`` stage shape, so nothing else in the directory is
considered. An in-flight upload from a concurrent deploy is the newest entry
and is therefore inside ``keep``. Symlinks and non-directories are skipped
rather than followed."""

_RUNTIME_SOURCE_STAGE_ATTEMPTS = 6
"""Upload+verify passes for a runtime source stage before giving up.

Was 3 with no sleep at all, so all three completed in well under a second —
entirely inside a single transient blip. On 2026-07-22 that aborted a two-hour
slurm-cluster deploy with ``scp: Connection closed`` while a manual scp minutes later
moved 100 MB to the same host without complaint. Six attempts on the backoff
schedule below span ~2.5 minutes, which outlives a gateway cutover and is still
negligible against the deploy it protects.
"""

_RUNTIME_SOURCE_STAGE_BACKOFF_BASE_SECONDS = 5.0
"""First inter-attempt pause; doubles per attempt (5/10/20/40/80 s)."""

_RUNTIME_SOURCE_STAGE_BACKOFF_MAX_SECONDS = 120.0
"""Cap on the exponential staging backoff."""


def _clip_retry_detail(text: str, limit: int = 160) -> str:
    """One-line, bounded form of a failure for a marker heartbeat message."""
    flat = " ".join(text.split())
    return flat if len(flat) <= limit else f"{flat[:limit]}..."


def _stage_upload_retry_reporter(host: str, what: str):
    """Heartbeat callback for :func:`transport.upload_file` retries."""

    def _report(attempt: int, total: int, delay: float, reason: str) -> None:
        refresh_admin_update_marker_heartbeat(
            f"{what} scp retry {attempt}/{total} on {host} in {delay:.0f}s "
            f"(fresh connection): {_clip_retry_detail(reason)}"
        )

    return _report


def _stage_retry_sleep(
    attempt: int,
    attempts: int,
    host: str,
    what: str,
    detail: str,
) -> None:
    """Back off before the next staging attempt, keeping the marker alive.

    The admin-update marker is held (and the queue paused) for the whole
    staging window, so a silent multi-minute sleep would read as a hang to
    anyone running ``vq admin status``. Heartbeat first, then sleep.
    """
    delay = min(
        _RUNTIME_SOURCE_STAGE_BACKOFF_MAX_SECONDS,
        _RUNTIME_SOURCE_STAGE_BACKOFF_BASE_SECONDS * (2.0**attempt),
    )
    refresh_admin_update_marker_heartbeat(
        f"{what} retry {attempt + 1}/{attempts} on {host} in {delay:.0f}s: {detail}"
    )
    time.sleep(delay)


def _resolve_unique_tag_for_sha(repo_path: Path, sha: str) -> str | None:
    """Return the single git tag pointing exactly at ``sha``, else None.

    Reads real tags only -- never synthesises a tag from a version string
    (IID 288: a synthesised tag is worse than a missing one). Zero matching
    tags and ambiguous (multiple) tags are both reported as None so the
    caller's warning path names the provenance gap.
    """
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo_path), "tag", "--points-at", sha],
            capture_output=True,
            text=True,
            timeout=30,
            stdin=subprocess.DEVNULL,
        )
    except (subprocess.TimeoutExpired, OSError):
        return None
    if proc.returncode != 0:
        return None
    tags = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    return tags[0] if len(tags) == 1 else None


def _resolve_tag_for_deploy(cfg: config.Config, sha: str) -> str | None:
    """Resolve the deployment tag for ``sha`` from the driver source repo.

    ``None`` when no source repo is configured, or no unique tag points at
    ``sha`` -- both leave the manifest's tag field null (IID 288).
    """
    repo = cfg.vibeqc_source_repo
    if repo is None:
        return None
    return _resolve_unique_tag_for_sha(Path(repo).expanduser(), sha)


def _reclaim_runtime_source_stage(
    host: str,
    command_host_cfg: config.HostConfig,
    result: SchedulerRuntimeUpdateResult,
    *,
    remove_current: bool,
    keep: int = RUNTIME_SOURCE_STAGES_TO_KEEP,
) -> None:
    """Reclaim this deploy's upload staging and bound what the program keeps.

    A runtime-source stage is upload staging for exactly one deploy: the driver
    re-archives the same SHA from git on demand, and nothing reads a stage once
    the build has consumed the archive. So a deploy that verified removes its
    own, and older stages are trimmed to ``keep`` so failures cannot accumulate
    without limit. Before #61 nothing removed them at all.

    This is deliberately **not** the helper-generation rule. Those stay, because
    another deployment may still be using an older one; see docs/operations.md.
    Nothing here calls the ``source-stage-prune`` verb on the host either: it
    removes exactly the directory this deploy created, by name, plus that one
    program's own older stages.

    Cleanup never fails a deploy. Any problem is recorded on the result and
    logged, and the caller's outcome is untouched.
    """
    stage_path = result.staged_source_stage
    if stage_path is None:
        return
    root = posixpath.dirname(stage_path)
    current = posixpath.basename(stage_path)
    if (
        not stage_path.startswith("/")
        or _RUNTIME_SOURCE_STAGE_SEGMENT not in stage_path
        or not _SCHEDULER_STAGE_GENERATION_RE.fullmatch(current)
    ):
        # Fail closed: the root comes from operator config and the script runs
        # `rm -rf` on a remote host.
        result.staged_source_reclaim_error = (
            f"refusing to reclaim an unrecognized stage path: {stage_path}"
        )
        log.error(
            "scheduler runtime deploy on %s: %s",
            host,
            result.staged_source_reclaim_error,
        )
        return
    try:
        proc = transport.run_remote_shell(
            command_host_cfg,
            "sh", "-c", _RUNTIME_SOURCE_RECLAIM_SCRIPT, "vq-runtime-src-reclaim",
            root, str(keep), current if remove_current else "",
            check=False,
            timeout=transport.DEFAULT_REMOTE_SHELL_TIMEOUT_SECONDS,
        )
    except transport.RemoteError as exc:
        result.staged_source_reclaim_error = f"reclaim could not run: {exc}"
        log.warning(
            "scheduler runtime deploy on %s: could not reclaim source staging "
            "under %s: %s",
            host,
            root,
            exc,
        )
        return
    if proc.returncode != 0:
        detail = _combined_output(proc.stdout, proc.stderr).strip()
        result.staged_source_reclaim_error = (
            f"reclaim rc={proc.returncode}: {detail or '(no output)'}"
        )
        log.warning(
            "scheduler runtime deploy on %s: source staging under %s was not "
            "reclaimed (%s)",
            host,
            root,
            result.staged_source_reclaim_error,
        )
        return
    for token in proc.stdout.split():
        key, _, value = token.partition("=")
        if not value.isdigit():
            continue
        if key == "reclaimed":
            result.staged_source_stages_reclaimed = int(value)
        elif key == "retained":
            result.staged_source_stages_retained = int(value)
    result.staged_source_stage_reclaimed = remove_current
    output.run_log_write(
        f"--- runtime source staging: reclaimed "
        f"{result.staged_source_stages_reclaimed}, retained "
        f"{result.staged_source_stages_retained} under {root} ---"
    )


def _stage_scheduler_runtime_source(
    host: str,
    command_host_cfg: config.HostConfig,
    program: str,
    expected_sha: str,
    cfg: config.Config,
    result: SchedulerRuntimeUpdateResult,
) -> str:
    """Archive vibe-qc at ``expected_sha`` on the driver and upload it.

    A daemonless SLURM login host (slurm-cluster) has no repo credentials, so the
    build host cannot fetch the source itself. The driver (which does have
    access) archives the exact commit from
    that program's own source repository, uploads it to a per-deployment
    stage dir under the host's ``scratch_root``, verifies the SHA-256 remotely,
    and returns the remote archive path for ``--source-archive``.
    """
    # Per-program: the viewer is its own repository since the split, so one
    # global source path would stage a vibe-qc tree as the viewer.
    repo_path = program_source_repo(cfg, program)
    # Linked worktrees store a pointer in a regular ``.git`` file. The git
    # commands below are the authoritative checkout/commit validation, so
    # accept either checkout representation here.
    if not (repo_path / ".git").exists():
        raise AdminError(
            f"source repo for {program!r} is not a git checkout: {repo_path}"
        )

    def _has_commit() -> bool:
        proc = _mutating_git_run(
            ["git", "-C", str(repo_path), "cat-file", "-e", f"{expected_sha}^{{commit}}"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        return proc.returncode == 0

    if not _has_commit():
        # Best-effort fetch so an expected SHA newer than the local checkout is
        # available before we fail closed.
        _mutating_git_run(
            ["git", "-C", str(repo_path), "fetch", "--quiet", "--tags", "origin"],
            capture_output=True,
            text=True,
            timeout=300,
        )
    if not _has_commit():
        raise AdminError(
            f"commit {expected_sha} not found in {repo_path} (fetch did not "
            "make it available)"
        )

    stage_root = command_host_cfg.scratch_root or "/tmp"
    token = uuid.uuid4().hex
    stage_path = (
        f"{stage_root}/.vq-admin/runtime-source/{program}/{expected_sha}-{token}"
    )
    archive_name = f"vibeqc-{expected_sha[:12]}-source.tar.gz"
    with tempfile.TemporaryDirectory(prefix="vq-runtime-src-") as raw_tmp:
        tmp = Path(raw_tmp)
        archive = tmp / archive_name
        archive_proc = _mutating_git_run(
            [
                "git", "-C", str(repo_path), "archive",
                "--format=tar.gz", "-o", str(archive), expected_sha,
            ],
            capture_output=True,
            text=True,
            timeout=300,
        )
        if archive_proc.returncode != 0:
            raise AdminError(
                "could not archive runtime source: "
                + (archive_proc.stderr.strip() or "git archive failed")
            )
        digest = hashlib.sha256(archive.read_bytes()).hexdigest()
        result.staged_source_sha256 = digest
        (tmp / "SOURCE-SHA").write_text(expected_sha + "\n", encoding="utf-8")
        (tmp / "ARCHIVE-SHA256").write_text(
            f"{digest}  {archive_name}\n", encoding="utf-8"
        )

        mkdir = transport.run_remote_shell(
            command_host_cfg, "mkdir", "-p", stage_path,
            check=False, timeout=transport.DEFAULT_REMOTE_SHELL_TIMEOUT_SECONDS,
        )
        if mkdir.returncode != 0:
            raise AdminError(
                "could not create runtime source stage: "
                + (_combined_output(mkdir.stdout, mkdir.stderr).strip() or "mkdir failed")
            )
        # From here the directory exists on the host, so it is this deploy's to
        # clean up even if the upload below never finishes (#61).
        result.staged_source_stage = stage_path
        verify_script = (
            "set -eu\n"
            'stage=$1\n'
            'expected=$2\n'
            'test "$(head -n 1 "$stage/SOURCE-SHA")" = "$expected"\n'
            'cd "$stage"\n'
            "sha256sum -c ARCHIVE-SHA256\n"
        )
        # The source archive can be large (a full monorepo tree) and the
        # transport link (e.g. WireGuard) can drop a transfer, leaving a
        # truncated upload that fails the integrity check. Retry the whole
        # upload+verify a few times so a flaky transfer does not fail the whole
        # deploy; each verify re-hashes the uploaded archive on the host.
        last_error = "upload/verify did not run"
        for attempt in range(_RUNTIME_SOURCE_STAGE_ATTEMPTS):
            try:
                for name in (archive_name, "SOURCE-SHA", "ARCHIVE-SHA256"):
                    transport.upload_file(
                        command_host_cfg,
                        tmp / name,
                        f"{stage_path}/{name}",
                        retry_transient=1,
                        on_retry=_stage_upload_retry_reporter(host, "runtime source"),
                    )
            except transport.RemoteError as exc:
                last_error = f"upload failed: {exc}"
                if attempt + 1 < _RUNTIME_SOURCE_STAGE_ATTEMPTS:
                    _stage_retry_sleep(
                        attempt,
                        _RUNTIME_SOURCE_STAGE_ATTEMPTS,
                        host,
                        "runtime source stage upload",
                        _clip_retry_detail(str(exc)),
                    )
                continue
            verify = transport.run_remote_shell(
                command_host_cfg, "sh", "-c", verify_script, "vq-runtime-src-verify",
                stage_path, expected_sha,
                check=False, timeout=transport.DEFAULT_REMOTE_SHELL_TIMEOUT_SECONDS,
            )
            if verify.returncode == 0:
                break
            last_error = (
                _combined_output(verify.stdout, verify.stderr).strip() or "verify failed"
            )
            if attempt + 1 < _RUNTIME_SOURCE_STAGE_ATTEMPTS:
                _stage_retry_sleep(
                    attempt,
                    _RUNTIME_SOURCE_STAGE_ATTEMPTS,
                    host,
                    "runtime source stage verify",
                    _clip_retry_detail(last_error),
                )
        else:
            raise AdminError(
                "runtime source stage verification failed after "
                f"{_RUNTIME_SOURCE_STAGE_ATTEMPTS} attempts: {last_error}"
            )
    remote_archive = f"{stage_path}/{archive_name}"
    result.staged_source_archive = remote_archive
    return remote_archive


def _scheduler_update_stage_path(
    host: str,
    host_cfg: config.HostConfig,
    command_host_cfg: config.HostConfig,
) -> str:
    configured = host_cfg.scheduler_update_stage
    if configured is not None:
        if not Path(configured).is_absolute():
            raise AdminError("scheduler_update_stage must be an absolute path")
        return configured
    home_proc = transport.run_remote_shell(
        command_host_cfg,
        "sh",
        "-c",
        'printf "%s\\n" "$HOME"',
        check=False,
        timeout=transport.DEFAULT_REMOTE_SHELL_TIMEOUT_SECONDS,
    )
    remote_home = home_proc.stdout.strip()
    if home_proc.returncode != 0 or not Path(remote_home).is_absolute():
        detail = _combined_output(home_proc.stdout, home_proc.stderr).strip()
        raise AdminError(
            "could not resolve scheduler update host HOME: "
            f"{detail or f'rc={home_proc.returncode}'}"
        )
    return str(Path(remote_home) / ".cache" / "vq-admin" / host)


def _stage_scheduler_helper_source(
    host: str,
    host_cfg: config.HostConfig,
    command_host_cfg: config.HostConfig,
    result: SchedulerHostUpdateResult,
    *,
    expected_sha: str | None = None,
) -> str:
    """Refresh the scheduler helper stage.

    With ``expected_sha`` (the fleet-rollout path) the stage is built from
    that exact commit of the managed runtime repository — never from the
    live driver checkout, which may legitimately sit ahead of the accepted
    report pin. Without it (manual operator update) the historical
    driver-tree staging is kept, and the transcript names that provenance.
    """
    project_root = _scheduler_helper_project_root()
    if expected_sha is not None:
        source_sha = expected_sha.lower()
        git_root, _ = _scheduler_helper_git_root(project_root)
        _resolve_helper_pin_commit(git_root, source_sha)
        result.stage_source = "report-pin"
        output.narrate(
            f"staging helper for {host} from pinned commit {source_sha[:12]} "
            "(accepted report identity, not the live driver tree)"
        )
    else:
        source_sha = current_source_sha()
        if source_sha is None:
            raise AdminError("could not determine driver source SHA")
        result.stage_source = "driver-tree"
        output.narrate(
            f"staging helper for {host} from the live driver tree at "
            f"{source_sha[:12]} (no --expected-sha pin was supplied)"
        )
    stage_root = _scheduler_update_stage_path(host, host_cfg, command_host_cfg)
    token = uuid.uuid4().hex
    stage_path = f"{stage_root}/generations/{source_sha}-{token}"
    result.expected_source_sha = source_sha
    result.stage_root = stage_root
    result.stage_path = stage_path

    with tempfile.TemporaryDirectory(prefix="vq-scheduler-helper-") as raw_tmp:
        tmp = Path(raw_tmp)
        archive = tmp / "vibe-queue-src.tar.gz"
        archive_digest = _scheduler_helper_archive(
            project_root,
            archive,
            source_sha,
            require_clean_tree=expected_sha is None,
        )
        if expected_sha is not None:
            tree_digest = _helper_archive_tree_digest(archive)
        else:
            tree_digest = source_tree_sha256(project_root / "src" / "vq")
        result.expected_source_tree_sha256 = tree_digest
        (tmp / SOURCE_SHA_MARKER_NAME).write_text(source_sha + "\n", encoding="utf-8")
        (tmp / SOURCE_TREE_SHA256_NAME).write_text(
            tree_digest + "\n", encoding="utf-8"
        )
        (tmp / "ARCHIVE-SHA256").write_text(
            f"{archive_digest}  {archive.name}\n", encoding="utf-8"
        )
        result.archive_sha256 = archive_digest

        mkdir = transport.run_remote_shell(
            command_host_cfg,
            "mkdir",
            "-p",
            stage_path,
            check=False,
            timeout=transport.DEFAULT_REMOTE_SHELL_TIMEOUT_SECONDS,
        )
        if mkdir.returncode != 0:
            raise AdminError(
                "could not create scheduler helper stage: "
                + (_combined_output(mkdir.stdout, mkdir.stderr).strip() or "mkdir failed")
            )
        names = [
            archive.name,
            SOURCE_SHA_MARKER_NAME,
            "ARCHIVE-SHA256",
            SOURCE_TREE_SHA256_NAME,
        ]
        for name in names:
            # Same transient-blip exposure as the runtime stage below, and this
            # one had no retry at all: one dropped scp aborted the whole helper
            # alignment.
            transport.upload_file(
                command_host_cfg,
                tmp / name,
                f"{stage_path}/{name}",
                retry_transient=_RUNTIME_SOURCE_STAGE_ATTEMPTS - 1,
                on_retry=_stage_upload_retry_reporter(host, "scheduler helper"),
            )
        verify_script = """set -eu
stage=$1
expected_sha=$2
expected_tree=$3
test "$(head -n 1 "$stage/SOURCE-SHA")" = "$expected_sha"
test "$(head -n 1 "$stage/SOURCE-TREE-SHA256")" = "$expected_tree"
cd "$stage"
sha256sum -c ARCHIVE-SHA256
"""
        verify = transport.run_remote_shell(
            command_host_cfg,
            "sh",
            "-c",
            verify_script,
            "vq-stage-verify",
            stage_path,
            source_sha,
            tree_digest,
            check=False,
            timeout=transport.DEFAULT_REMOTE_SHELL_TIMEOUT_SECONDS,
        )
        if verify.returncode != 0:
            raise AdminError(
                "scheduler helper stage verification failed: "
                + (_combined_output(verify.stdout, verify.stderr).strip() or "verification failed")
            )
    result.stage_uploaded = True
    return stage_path


@dataclass
class _HelperDigestProbe:
    """One read of a provenance digest from the deployed scheduler helper."""

    returncode: int | None
    output: str
    value: str | None
    """The parsed digest, or None when the read failed or was unparseable."""
    matched: bool
    error: str | None = None
    """Set when the read itself failed (transport error, bad rc, no digest).
    A *mismatch* is not an error here: it is the flip-pending signature."""
    attempts: int = 1
    """How many reads it took. >1 means the activation had not settled when the
    deploy command returned — recorded so the operator can see the race that
    used to be reported as a failure."""


def _probe_scheduler_helper_digest(
    host_cfg: config.HostConfig,
    verb: str,
    pattern: re.Pattern[str],
    expected: str,
    label: str,
) -> _HelperDigestProbe:
    """Read one provenance digest from the remote helper and classify it.

    Output parsing matches the doctor check (``doctor.scheduler_remote_vq_check``):
    prefer stdout, take the first *line*. The pre-fix admin path took the first
    whitespace token of stdout+stderr concatenated, so a helper that wrote a
    warning to stderr and nothing to stdout had that warning's first word
    compared against a SHA.
    """
    try:
        proc = transport.run_remote_vq(
            host_cfg,
            verb,
            check=False,
            timeout=transport.DEFAULT_REMOTE_SHELL_TIMEOUT_SECONDS,
        )
    except transport.RemoteError as exc:
        return _HelperDigestProbe(
            returncode=None,
            output=str(exc),
            value=None,
            matched=False,
            error=f"helper {label} verification failed: {exc}",
        )
    output = _combined_output(proc.stdout, proc.stderr)
    preferred = (proc.stdout or proc.stderr or "").strip()
    first_line = preferred.splitlines()[0].strip() if preferred else ""
    token = first_line.split()[0] if first_line else ""
    if proc.returncode != 0 or not pattern.fullmatch(token):
        detail = output.strip() or f"rc={proc.returncode}"
        return _HelperDigestProbe(
            returncode=proc.returncode,
            output=output,
            value=None,
            matched=False,
            error=f"helper {label} verification returned no valid digest: {detail}",
        )
    value = token.lower()
    return _HelperDigestProbe(
        returncode=proc.returncode,
        output=output,
        value=value,
        matched=value == expected.lower(),
    )


def _await_scheduler_helper_digest(
    host_cfg: config.HostConfig,
    verb: str,
    pattern: re.Pattern[str],
    expected: str,
    label: str,
    host: str,
) -> _HelperDigestProbe:
    """Read a provenance digest, retrying while it disagrees with ``expected``.

    Retries **only** a well-formed digest that does not match — the signature of
    an activation the login node has not observed yet. A transport error, a
    non-zero rc, or unparseable output returns immediately: the readiness probe
    has already established that the helper executes, so those are real
    failures, not a flip in progress. See
    :data:`SCHEDULER_HELPER_ACTIVATION_MAX_ATTEMPTS`.
    """
    probe = _probe_scheduler_helper_digest(host_cfg, verb, pattern, expected, label)
    for attempt in range(1, SCHEDULER_HELPER_ACTIVATION_MAX_ATTEMPTS):
        if probe.matched or probe.error is not None:
            break
        refresh_admin_update_marker_heartbeat(
            f"waiting for {label} activation to settle on {host} "
            f"(attempt {attempt}/{SCHEDULER_HELPER_ACTIVATION_MAX_ATTEMPTS}; "
            f"read {probe.value}, expected {expected.lower()})"
        )
        time.sleep(SCHEDULER_HELPER_ACTIVATION_INTERVAL_SECONDS)
        probe = _probe_scheduler_helper_digest(
            host_cfg, verb, pattern, expected, label
        )
        probe.attempts = attempt + 1
    return probe


def _verify_scheduler_helper_provenance(
    host_cfg: config.HostConfig,
    result: SchedulerHostUpdateResult,
) -> None:
    """Verify deployed helper content first, then its provenance marker.

    Both reads poll until the site script's atomic activation is observable
    from the login node rather than trusting the deploy command's rc=0 as proof
    that the flip has landed — see
    :data:`SCHEDULER_HELPER_ACTIVATION_MAX_ATTEMPTS`.
    """
    expected = result.expected_source_sha
    expected_tree = result.expected_source_tree_sha256
    if expected is None or expected_tree is None:
        result.work_errors.append(
            "scheduler helper stage identity is missing; provenance was not verified"
        )
        return

    tree = _await_scheduler_helper_digest(
        host_cfg,
        "source-tree-sha256",
        _SHA256_RE,
        expected_tree,
        "source-tree",
        result.host,
    )
    result.source_tree_rc = tree.returncode
    result.source_tree_output = tree.output
    result.activation_wait_attempts = tree.attempts
    if tree.error is not None:
        result.work_errors.append(tree.error)
        return
    result.remote_source_tree_sha256 = tree.value
    if not tree.matched:
        result.work_errors.append(
            "helper source-tree digest mismatch after "
            f"{SCHEDULER_HELPER_ACTIVATION_MAX_ATTEMPTS} activation-settle "
            f"reads: expected {expected_tree.lower()}, got {tree.value}"
        )
        return

    marker = _await_scheduler_helper_digest(
        host_cfg,
        "source-sha",
        _FULL_SHA_RE,
        expected,
        "SOURCE-SHA",
        result.host,
    )
    result.source_marker_rc = marker.returncode
    result.source_marker_output = marker.output
    result.activation_wait_attempts = max(
        result.activation_wait_attempts, marker.attempts
    )
    if marker.error is not None:
        result.work_errors.append(marker.error)
        return
    result.remote_source_sha = marker.value
    if not marker.matched:
        result.work_errors.append(
            "helper SOURCE-SHA marker verification mismatch after "
            f"{SCHEDULER_HELPER_ACTIVATION_MAX_ATTEMPTS} activation-settle "
            f"reads: expected {expected.lower()}, got {marker.value}"
        )


def _wait_for_scheduler_helper_ready(
    host_cfg: config.HostConfig,
    result: SchedulerHostUpdateResult,
) -> bool:
    """Require stable helper execution after a shared-filesystem install.

    An editable reinstall on a build node can briefly leave the login node's
    shared venv interpreter returning ``ETXTBSY``. Retry only that diagnosed
    filesystem race under a fixed budget. Two consecutive successful helper
    executions are required before content and marker provenance are verified.
    """
    stable_successes = 0
    for attempt in range(1, SCHEDULER_HELPER_READINESS_MAX_ATTEMPTS + 1):
        try:
            proc = transport.run_remote_vq(
                host_cfg,
                "--version",
                check=False,
                timeout=transport.DEFAULT_REMOTE_SHELL_TIMEOUT_SECONDS,
            )
        except transport.RemoteError as exc:
            result.helper_readiness_attempts.append(
                SchedulerHelperReadinessAttempt(
                    attempt=attempt,
                    returncode=None,
                    transient_etxtbsy=False,
                    output=str(exc),
                )
            )
            result.work_errors.append(
                f"scheduler helper readiness probe failed: {exc}"
            )
            return False

        output = _combined_output(proc.stdout, proc.stderr).strip()
        transient_etxtbsy = (
            proc.returncode == 126 and "text file busy" in output.lower()
        )
        result.helper_readiness_attempts.append(
            SchedulerHelperReadinessAttempt(
                attempt=attempt,
                returncode=proc.returncode,
                transient_etxtbsy=transient_etxtbsy,
                output=output,
            )
        )

        if proc.returncode == 0:
            stable_successes += 1
            if stable_successes >= SCHEDULER_HELPER_READINESS_STABLE_SUCCESSES:
                result.helper_readiness_verified = True
                return True
        else:
            stable_successes = 0
            if not transient_etxtbsy:
                result.work_errors.append(
                    "scheduler helper readiness probe failed "
                    f"(rc={proc.returncode}): {output or '(no output)'}"
                )
                return False

        if attempt < SCHEDULER_HELPER_READINESS_MAX_ATTEMPTS:
            refresh_admin_update_marker_heartbeat(
                "scheduler helper readiness pending on "
                f"{host_cfg.ssh} (attempt {attempt}/"
                f"{SCHEDULER_HELPER_READINESS_MAX_ATTEMPTS})"
            )
            time.sleep(SCHEDULER_HELPER_READINESS_INTERVAL_SECONDS)

    last = result.helper_readiness_attempts[-1]
    result.work_errors.append(
        "scheduler helper readiness exhausted after "
        f"{SCHEDULER_HELPER_READINESS_MAX_ATTEMPTS} bounded probes; "
        f"last rc={last.returncode}: {last.output or '(no output)'}"
    )
    return False


def _scheduler_update_argv(
    command: str,
    extra_args: list[str] | None,
) -> list[str]:
    parts = shlex.split(command)
    if not parts:
        raise AdminError("scheduler update command is empty after shlex.split")
    return [*parts, *(extra_args or [])]


def _run_scheduler_update_command_with_heartbeat(
    host_cfg: config.HostConfig,
    argv: list[str],
    *,
    timeout: float | None,
    mode: str,
) -> subprocess.CompletedProcess[str]:
    """Run a scheduler provisioning command while refreshing the marker.

    ``transport.run_remote_shell`` remains the single SSH implementation for
    quoting, timeouts, and retry behavior. This helper runs it on a worker
    thread so the admin process can keep the live update marker fresh while a
    long cluster-side compile/provisioning command is in flight.
    """
    done = threading.Event()
    result: dict[str, object] = {}

    def _target() -> None:
        try:
            result["proc"] = transport.run_remote_shell(
                host_cfg,
                *argv,
                check=False,
                timeout=timeout,
            )
        except BaseException as exc:  # noqa: BLE001 - re-raised below
            result["exc"] = exc
        finally:
            done.set()

    label = f"scheduler {mode} command on {host_cfg.ssh}"
    refresh_admin_update_marker_heartbeat(f"{label} started")
    worker = threading.Thread(target=_target, daemon=True)
    worker.start()
    start = time.monotonic()
    last_heartbeat = start
    while not done.wait(timeout=_BUILD_POLL_INTERVAL_SECONDS):
        interval = _build_heartbeat_interval()
        if not interval:
            continue
        now = time.monotonic()
        if (now - last_heartbeat) < interval:
            continue
        last_heartbeat = now
        refresh_admin_update_marker_heartbeat(
            f"{label} still running ({now - start:.0f}s elapsed)"
        )
    worker.join()
    exc = result.get("exc")
    if exc is not None:
        if isinstance(exc, transport.RemoteError):
            raise exc
        raise exc
    proc = result.get("proc")
    assert isinstance(proc, subprocess.CompletedProcess)
    refresh_admin_update_marker_heartbeat(
        f"{label} finished rc={proc.returncode}"
    )
    return proc


DETACHED_BUILD_POLL_INTERVAL_SECONDS = 30.0
"""Seconds between observation polls of a detached build. Each poll is one
fresh SSH connection; 30 s keeps a 90-minute build under ~200 connections
while still teeing output into the transcript near-live."""

DETACHED_BUILD_OBSERVATION_GRACE_SECONDS = 1800.0
"""How long consecutive poll failures are tolerated before the driver gives
up observing. The build itself is not harmed by unobservability — that is the
point of detaching — so this is deliberately generous: a VPN that stays down
for half an hour is an operator problem, not a reason to abandon a build."""

_DETACHED_OUTPUT_MARKER = "----VQ-OUTPUT----"

_DETACHED_REQUEST_IDENTITY_SCHEMA = "vq.admin.detached_scheduler_request_identity/1"
_DETACHED_COMMAND_SCHEMA = "vq.admin.detached_scheduler_command/1"
_DETACHED_REQUEST_SCHEMA = "vq.admin.detached_scheduler_request/1"
_DETACHED_QUERY_SCHEMA = "vq.admin.detached_scheduler_observation_query/1"
_DETACHED_LAUNCH_SCHEMA = "vq.admin.detached_scheduler_launch/1"
_DETACHED_OBSERVATION_SCHEMA = "vq.admin.detached_scheduler_observation/1"
_DETACHED_LEASE_SCHEMA = "vq.admin.detached_scheduler_lease/1"
_DETACHED_ACTIVATION_SCHEMA = "vq.admin.detached_scheduler_activation/1"
_DETACHED_RESULT_SCHEMA = "vq.admin.detached_scheduler_result/1"
_DETACHED_OUTPUT_LIMIT = 4 * 1024 * 1024
_DETACHED_OBSERVATION_CHUNK = 64 * 1024
_DETACHED_REQUEST_LIMIT = 512 * 1024
_DETACHED_HELPER_INPUT_LIMIT = 1024 * 1024
_DETACHED_RESPONSE_LIMIT = 2 * 1024 * 1024
_DETACHED_HELPER_TIMEOUT = 60.0
_DETACHED_RUN_ID_RE = re.compile(r"[0-9a-f]{64}")
_DETACHED_PROTOCOL_ROOT = ".vq-admin-r4b1-"


_DETACHED_REMOTE_HELPER_SOURCE = r'''import base64
import datetime
import errno
import fcntl
import hashlib
import json
import math
import os
import re
import selectors
import signal
import stat
import subprocess
import sys
import time

BINDING_SCHEMA = "vq.fleet.rollout_detached_scheduler_binding/1"
IDENTITY_SCHEMA = "vq.admin.detached_scheduler_request_identity/1"
COMMAND_SCHEMA = "vq.admin.detached_scheduler_command/1"
REQUEST_SCHEMA = "vq.admin.detached_scheduler_request/1"
QUERY_SCHEMA = "vq.admin.detached_scheduler_observation_query/1"
LAUNCH_SCHEMA = "vq.admin.detached_scheduler_launch/1"
OBSERVATION_SCHEMA = "vq.admin.detached_scheduler_observation/1"
LEASE_SCHEMA = "vq.admin.detached_scheduler_lease/1"
ACTIVATION_SCHEMA = "vq.admin.detached_scheduler_activation/1"
RESULT_SCHEMA = "vq.admin.detached_scheduler_result/1"
MAX_JSON = 1024 * 1024
MAX_OUTPUT = 4 * 1024 * 1024
MAX_CHUNK = 64 * 1024
HEX64 = re.compile(r"[0-9a-f]{64}\Z")
RUN = re.compile(r"run-([0-9a-f]{64})\Z")
TEMP = re.compile(r"\.(request|lease|activation|result)\.json\.[0-9a-f]{32}\.tmp\Z")
DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
FILE_FLAGS = os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
ORIGINAL_UMASK = os.umask(0o077)


def canonical(value):
    return json.dumps(value, allow_nan=False, ensure_ascii=False,
                      separators=(",", ":"), sort_keys=True).encode("utf-8")


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def duplicate_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def nonfinite(value):
    raise ValueError("non-finite JSON number " + value)


def strict_load(data, label):
    if len(data) > MAX_JSON:
        raise ValueError(label + " is too large")
    value = json.loads(data.decode("utf-8"), object_pairs_hook=duplicate_pairs,
                       parse_constant=nonfinite)
    if not isinstance(value, dict):
        raise ValueError(label + " must be an object")
    return value


def stdin_object():
    data = sys.stdin.buffer.read(MAX_JSON + 1)
    return strict_load(data, "helper request")


def private_dir(fd, label):
    info = os.fstat(fd)
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != 0o700):
        raise ValueError("unsafe " + label)


def ancestry_dir(fd, label):
    info = os.fstat(fd)
    mode = stat.S_IMODE(info.st_mode)
    if not stat.S_ISDIR(info.st_mode) or info.st_uid not in (0, os.geteuid()):
        raise ValueError("unsafe " + label + " owner")
    if mode & 0o022 and not (info.st_uid == 0 and mode & stat.S_ISVTX):
        raise ValueError("unsafe writable " + label)


def open_scratch(path):
    if (not isinstance(path, str) or not path.startswith("/")
            or path.startswith("//") or path == "/"
            or os.path.normpath(path) != path or "\x00" in path):
        raise ValueError("scratch root is not canonical")
    fd = os.open("/", DIR_FLAGS)
    try:
        ancestry_dir(fd, "filesystem root")
        for component in path.split("/")[1:]:
            next_fd = os.open(component, DIR_FLAGS, dir_fd=fd)
            try:
                ancestry_dir(next_fd, "scratch ancestry")
            except BaseException:
                os.close(next_fd)
                raise
            os.close(fd)
            fd = next_fd
        return fd
    except BaseException:
        os.close(fd)
        raise


def managed_child(parent_fd, name, create):
    if create:
        try:
            os.mkdir(name, 0o700, dir_fd=parent_fd)
            os.fsync(parent_fd)
        except FileExistsError:
            pass
    fd = os.open(name, DIR_FLAGS, dir_fd=parent_fd)
    try:
        private_dir(fd, name)
    except BaseException:
        os.close(fd)
        raise
    return fd


def open_base(scratch_root, run_id, create):
    if not isinstance(run_id, str) or HEX64.fullmatch(run_id) is None:
        raise ValueError("invalid managed-root run id")
    scratch_fd = open_scratch(scratch_root)
    admin_fd = detached_fd = -1
    try:
        admin_fd = managed_child(scratch_fd, ".vq-admin-r4b1-" + run_id, create)
        detached_fd = managed_child(admin_fd, "detached", create)
        return detached_fd
    finally:
        if admin_fd >= 0:
            os.close(admin_fd)
        os.close(scratch_fd)


def open_run(base_fd, run_name):
    if RUN.fullmatch(run_name) is None:
        raise ValueError("invalid run name")
    fd = os.open(run_name, DIR_FLAGS, dir_fd=base_fd)
    try:
        private_dir(fd, "run directory")
    except BaseException:
        os.close(fd)
        raise
    return fd


def regular_info(dir_fd, name, size_limit, mutable=False):
    info = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1
            or info.st_size > size_limit):
        raise ValueError("unsafe " + name)
    if not mutable and info.st_size < 1:
        raise ValueError("empty " + name)
    return info


def secure_open(dir_fd, name, flags, size_limit, mutable=False):
    before = regular_info(dir_fd, name, size_limit, mutable)
    fd = os.open(name, flags | FILE_FLAGS, dir_fd=dir_fd)
    after = os.fstat(fd)
    if ((before.st_dev, before.st_ino) != (after.st_dev, after.st_ino)
            or not stat.S_ISREG(after.st_mode) or after.st_uid != os.geteuid()
            or stat.S_IMODE(after.st_mode) != 0o600 or after.st_nlink != 1):
        os.close(fd)
        raise ValueError("changed " + name)
    return fd


def read_bytes(dir_fd, name, size_limit):
    before = regular_info(dir_fd, name, size_limit)
    fd = secure_open(dir_fd, name, os.O_RDONLY, size_limit)
    try:
        data = b""
        while len(data) <= size_limit:
            chunk = os.read(fd, min(65536, size_limit + 1 - len(data)))
            if not chunk:
                break
            data += chunk
        after = os.fstat(fd)
    finally:
        os.close(fd)
    if len(data) > size_limit or after.st_size != before.st_size:
        raise ValueError("changed " + name)
    return data


def read_json(dir_fd, name):
    return strict_load(read_bytes(dir_fd, name, MAX_JSON), name)


def optional_json(dir_fd, name):
    try:
        info = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    # publish() links the complete temporary receipt before unlinking its
    # temporary name. Until that unlink, the final receipt has two links and
    # is not yet admissible to the strict single-link reader. Treat only this
    # exact, same-directory publication window as pending; never read through
    # it or relax the hardlink check for an unrelated alias.
    if (stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid()
            and stat.S_IMODE(info.st_mode) == 0o600 and info.st_nlink == 2
            and 0 < info.st_size <= MAX_JSON):
        for temporary in os.listdir(dir_fd):
            if (not temporary.startswith("." + name + ".")
                    or TEMP.fullmatch(temporary) is None):
                continue
            try:
                linked = os.stat(temporary, dir_fd=dir_fd, follow_symlinks=False)
            except FileNotFoundError:
                # The publisher completed; the ordinary reader below can
                # now validate the final single-link receipt.
                continue
            if (linked.st_dev, linked.st_ino) == (info.st_dev, info.st_ino):
                return None
    return read_json(dir_fd, name)


def publish(dir_fd, name, value):
    data = canonical(value) + b"\n"
    if len(data) > MAX_JSON:
        raise ValueError("receipt too large")
    temp = "." + name + "." + os.urandom(16).hex() + ".tmp"
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | FILE_FLAGS,
                 0o600, dir_fd=dir_fd)
    linked = False
    try:
        with os.fdopen(fd, "wb") as stream:
            fd = -1
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temp, name, src_dir_fd=dir_fd, dst_dir_fd=dir_fd,
                follow_symlinks=False)
        linked = True
        os.unlink(temp, dir_fd=dir_fd)
        os.fsync(dir_fd)
    finally:
        if fd >= 0:
            os.close(fd)
        if not linked:
            try:
                os.unlink(temp, dir_fd=dir_fd)
            except FileNotFoundError:
                pass


def create_empty(dir_fd, name):
    fd = os.open(name, os.O_RDWR | os.O_CREAT | os.O_EXCL | FILE_FLAGS,
                 0o600, dir_fd=dir_fd)
    os.fsync(fd)
    os.fsync(dir_fd)
    return fd


def validate_binding(binding):
    fields = {"schema", "operation_id", "request_sha256", "nonce", "target",
              "program", "mode", "protocol", "run_id", "bound_at", "remote_run_dir",
              "command_sha256", "remote_request_sha256"}
    if set(binding) != fields or binding.get("schema") != BINDING_SCHEMA:
        raise ValueError("invalid binding")
    for name in ("operation_id", "request_sha256", "nonce", "run_id",
                 "command_sha256", "remote_request_sha256"):
        if not isinstance(binding.get(name), str) or HEX64.fullmatch(binding[name]) is None:
            raise ValueError("invalid binding " + name)
    for name in ("target", "program", "mode", "bound_at", "remote_run_dir"):
        value = binding.get(name)
        if (not isinstance(value, str) or not value or len(value) > 65536
                or any(char in value for char in ("\x00", "\r", "\n"))):
            raise ValueError("invalid binding " + name)
    if binding.get("protocol") != "fixed-host-detached-build/1":
        raise ValueError("invalid binding protocol")
    try:
        bound_at = datetime.datetime.fromisoformat(binding["bound_at"])
    except ValueError:
        raise ValueError("invalid binding bound_at")
    if (bound_at.tzinfo is None
            or bound_at.utcoffset() != datetime.timedelta(0)):
        raise ValueError("binding bound_at is not UTC")
    remote_run_dir = binding["remote_run_dir"]
    if (not remote_run_dir.startswith("/") or remote_run_dir.startswith("//")
            or remote_run_dir == "/"
            or os.path.normpath(remote_run_dir) != remote_run_dir):
        raise ValueError("binding remote run directory is not canonical")
    if binding["remote_run_dir"].rsplit("/", 1)[-1] != "run-" + binding["run_id"]:
        raise ValueError("binding path does not match run id")


def validate_request(request):
    if (set(request) != {"schema", "binding", "request_identity"}
            or request.get("schema") != REQUEST_SCHEMA):
        raise ValueError("invalid request fields")
    binding = request.get("binding")
    identity = request.get("request_identity")
    if not isinstance(binding, dict) or not isinstance(identity, dict):
        raise ValueError("invalid request objects")
    validate_binding(binding)
    fields = {"schema", "operation_id", "request_sha256", "nonce", "target",
              "program", "mode", "protocol", "run_id", "bound_at", "remote_run_dir",
              "scratch_root", "argv", "timeout_seconds", "output_limit"}
    if set(identity) != fields or identity.get("schema") != IDENTITY_SCHEMA:
        raise ValueError("invalid request identity")
    for name in ("operation_id", "request_sha256", "nonce", "target", "program",
                 "mode", "protocol", "run_id", "bound_at", "remote_run_dir"):
        if identity.get(name) != binding.get(name):
            raise ValueError("request identity differs from binding")
    argv = identity.get("argv")
    if (not isinstance(argv, list) or not argv
            or any(not isinstance(arg, str) or "\x00" in arg for arg in argv)):
        raise ValueError("invalid action argv")
    timeout = identity.get("timeout_seconds")
    if timeout is not None and (not isinstance(timeout, (int, float))
            or isinstance(timeout, bool) or not math.isfinite(timeout) or timeout <= 0):
        raise ValueError("invalid timeout")
    if identity.get("output_limit") != MAX_OUTPUT:
        raise ValueError("invalid output limit")
    scratch = identity.get("scratch_root")
    expected_dir = (str(scratch).rstrip("/")
                    + "/.vq-admin-r4b1-" + binding["run_id"]
                    + "/detached/run-" + binding["run_id"])
    if binding["remote_run_dir"] != expected_dir:
        raise ValueError("request scratch root differs from run directory")
    command_sha = hashlib.sha256(canonical(
        {"schema": COMMAND_SCHEMA, "argv": argv})).hexdigest()
    request_sha = hashlib.sha256(canonical(identity)).hexdigest()
    if command_sha != binding["command_sha256"] or request_sha != binding["remote_request_sha256"]:
        raise ValueError("request digest mismatch")
    return binding, identity


def binding_sha(request):
    return hashlib.sha256(canonical(request["binding"])).hexdigest()


def validate_entries(run_fd):
    allowed = {"request.json", "lease.lock", "lease.json", "activation.json",
               "result.json", "output.log"}
    for name in os.listdir(run_fd):
        if name in allowed:
            continue
        if TEMP.fullmatch(name) is None:
            raise ValueError("unexpected run entry " + name)
        info = os.stat(name, dir_fd=run_fd, follow_symlinks=False)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink not in (1, 2)
                or info.st_size > MAX_JSON):
            raise ValueError("unsafe temporary receipt")


def open_request_run(request, create_base=False):
    binding, identity = validate_request(request)
    base_fd = open_base(identity["scratch_root"], binding["run_id"], create_base)
    try:
        run_fd = open_run(base_fd, "run-" + binding["run_id"])
    finally:
        os.close(base_fd)
    validate_entries(run_fd)
    return run_fd, binding, identity


def launch(request):
    binding, identity = validate_request(request)
    base_fd = open_base(identity["scratch_root"], binding["run_id"], True)
    run_fd = lease_fd = -1
    launch_entered = False
    try:
        run_name = "run-" + binding["run_id"]
        os.mkdir(run_name, 0o700, dir_fd=base_fd)
        os.fsync(base_fd)
        run_fd = open_run(base_fd, run_name)
        lease_fd = create_empty(run_fd, "lease.lock")
        fcntl.flock(lease_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        publish(run_fd, "request.json", request)
        read_fd, write_fd = os.pipe()
        environment = {name: value for name, value in os.environ.items()
                       if not name.startswith("VQ_FLEET_OPERATION_")}
        environment["VQ_DETACHED_LEASE_FD"] = str(lease_fd)
        environment["VQ_DETACHED_ORIGINAL_UMASK"] = format(ORIGINAL_UMASK, "03o")
        try:
            launch_entered = True
            child = subprocess.Popen(
                [sys.executable, "-c",
                 base64.b64decode(__loader_source_b64__).decode("utf-8"), "record"],
                stdin=read_fd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                start_new_session=True, close_fds=True, pass_fds=(lease_fd,),
                env=environment, shell=False,
            )
        finally:
            os.close(read_fd)
        try:
            with os.fdopen(write_fd, "wb") as stream:
                stream.write(canonical(request) + b"\n")
        except BaseException:
            try:
                os.close(write_fd)
            except OSError:
                pass
            raise
        os.close(lease_fd)
        lease_fd = -1
        deadline = time.monotonic() + 30.0
        while True:
            activation = optional_json(run_fd, "activation.json")
            if activation is not None:
                if (set(activation) != {"schema", "binding_sha256", "recorder_pid",
                                       "launch_intent", "activated_at"}
                        or activation.get("schema") != ACTIVATION_SCHEMA
                        or activation.get("binding_sha256") != binding_sha(request)
                        or activation.get("recorder_pid") != child.pid
                        or activation.get("launch_intent") is not True
                        or not isinstance(activation.get("activated_at"), str)):
                    raise ValueError("activation receipt mismatch")
                break
            if child.poll() is not None:
                raise ValueError("recorder exited before activation")
            if time.monotonic() >= deadline:
                raise ValueError("recorder activation timed out")
            time.sleep(0.01)
        return {"schema": LAUNCH_SCHEMA, "status": "activated",
                "remote_request_sha256": binding["remote_request_sha256"]}
    finally:
        if lease_fd >= 0:
            if not launch_entered:
                try:
                    fcntl.flock(lease_fd, fcntl.LOCK_UN)
                except OSError:
                    pass
            os.close(lease_fd)
        if run_fd >= 0:
            os.close(run_fd)
        os.close(base_fd)


def write_all(fd, data):
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        view = view[written:]


def record(request):
    run_fd, binding, identity = open_request_run(request)
    lease_fd = output_fd = -1
    try:
        if read_json(run_fd, "request.json") != request:
            raise ValueError("persisted request mismatch")
        raw_fd = os.environ.pop("VQ_DETACHED_LEASE_FD", "")
        if not raw_fd.isdigit():
            raise ValueError("missing inherited lease")
        raw_umask = os.environ.pop("VQ_DETACHED_ORIGINAL_UMASK", "")
        if re.fullmatch(r"[0-7]{3}", raw_umask) is None:
            raise ValueError("missing inherited action umask")
        action_umask = int(raw_umask, 8)
        lease_fd = int(raw_fd)
        expected = regular_info(run_fd, "lease.lock", 0, True)
        actual = os.fstat(lease_fd)
        if ((actual.st_dev, actual.st_ino) != (expected.st_dev, expected.st_ino)
                or actual.st_size != 0 or actual.st_nlink != 1
                or actual.st_uid != os.geteuid() or stat.S_IMODE(actual.st_mode) != 0o600):
            raise ValueError("inherited lease does not match")
        fcntl.flock(lease_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        pid = os.getpid()
        if os.getsid(0) != pid or os.getpgrp() != pid:
            raise ValueError("recorder is not an isolated session leader")
        digest = binding_sha(request)
        publish(run_fd, "lease.json", {"schema": LEASE_SCHEMA,
                "binding_sha256": digest, "recorder_pid": pid,
                "acquired_at": utc_now()})
        publish(run_fd, "activation.json", {"schema": ACTIVATION_SCHEMA,
                "binding_sha256": digest, "recorder_pid": pid,
                "launch_intent": True, "activated_at": utc_now()})
        output_fd = create_empty(run_fd, "output.log")
        environment = {name: value for name, value in os.environ.items()
                       if not name.startswith("VQ_FLEET_OPERATION_")
                       and name not in {"VQ_DETACHED_LEASE_FD",
                                        "VQ_DETACHED_ORIGINAL_UMASK"}}
        os.umask(action_umask)
        try:
            child = subprocess.Popen(
                identity["argv"], stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, start_new_session=True, close_fds=True,
                env=environment, shell=False,
            )
        finally:
            os.umask(0o077)
        pipe_fd = child.stdout.fileno()
        os.set_blocking(pipe_fd, False)
        selector = selectors.DefaultSelector()
        selector.register(pipe_fd, selectors.EVENT_READ)
        observed = stored = 0
        observed_hash = hashlib.sha256()
        stored_hash = hashlib.sha256()
        eof = False
        timed_out = False
        deadline = (time.monotonic() + identity["timeout_seconds"]
                    if identity["timeout_seconds"] is not None else None)
        kill_deadline = None
        post_kill_deadline = None
        while not eof or child.poll() is None or kill_deadline is not None:
            now = time.monotonic()
            if (deadline is not None and now >= deadline and not timed_out
                    and (child.poll() is None or not eof)):
                timed_out = True
                try:
                    os.killpg(child.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                kill_deadline = now + 1.0
            if kill_deadline is not None and now >= kill_deadline:
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                kill_deadline = None
                post_kill_deadline = now + 1.0
            if post_kill_deadline is not None and now >= post_kill_deadline and not eof:
                raise ValueError("timed-out action kept its output pipe open after SIGKILL")
            events = selector.select(0.1)
            if not events and child.poll() is not None and not eof:
                events = [(None, None)]
            for _key, _mask in events:
                while True:
                    try:
                        chunk = os.read(pipe_fd, 65536)
                    except BlockingIOError:
                        break
                    if not chunk:
                        if not eof:
                            selector.unregister(pipe_fd)
                        eof = True
                        break
                    observed += len(chunk)
                    observed_hash.update(chunk)
                    keep = chunk[:max(0, MAX_OUTPUT - stored)]
                    if keep:
                        write_all(output_fd, keep)
                        stored += len(keep)
                        stored_hash.update(keep)
                if eof:
                    break
        selector.close()
        child.stdout.close()
        returncode = child.wait()
        os.fsync(output_fd)
        status_value = ("timed-out" if timed_out else
                        ("success" if returncode == 0 else "failed"))
        publish(run_fd, "result.json", {
            "schema": RESULT_SCHEMA, "binding_sha256": digest,
            "status": status_value, "returncode": returncode,
            "timed_out": timed_out, "completed_at": utc_now(),
            "output_sha256": observed_hash.hexdigest(),
            "output_stored_sha256": stored_hash.hexdigest(),
            "output_observed_bytes": observed, "output_stored_bytes": stored,
            "output_truncated": observed > stored,
        })
    finally:
        if output_fd >= 0:
            os.close(output_fd)
        if lease_fd >= 0:
            os.close(lease_fd)
        os.close(run_fd)


def output_snapshot(run_fd, offset, max_bytes):
    try:
        fd = secure_open(run_fd, "output.log", os.O_RDONLY, MAX_OUTPUT, True)
    except FileNotFoundError:
        data = b""
    else:
        try:
            chunks = []
            total = 0
            while total <= MAX_OUTPUT:
                chunk = os.read(fd, min(65536, MAX_OUTPUT + 1 - total))
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
            data = b"".join(chunks)
            after = os.fstat(fd)
        finally:
            os.close(fd)
        if len(data) > MAX_OUTPUT or after.st_size < len(data) or after.st_size > MAX_OUTPUT:
            raise ValueError("unsafe growing output")
    if offset > len(data):
        raise ValueError("output offset beyond spool")
    chunk = data[offset:offset + max_bytes]
    return data, chunk


def observe(query):
    if (set(query) != {"schema", "request", "offset", "max_bytes"}
            or query.get("schema") != QUERY_SCHEMA):
        raise ValueError("invalid observation query")
    request = query.get("request")
    offset = query.get("offset")
    max_bytes = query.get("max_bytes")
    if (not isinstance(request, dict) or not isinstance(offset, int)
            or isinstance(offset, bool) or offset < 0 or offset > MAX_OUTPUT
            or not isinstance(max_bytes, int) or isinstance(max_bytes, bool)
            or max_bytes < 1 or max_bytes > MAX_CHUNK):
        raise ValueError("invalid observation bounds")
    run_fd, _binding, _identity = open_request_run(request)
    lease_fd = -1
    try:
        if read_json(run_fd, "request.json") != request:
            raise ValueError("persisted request mismatch")
        lease_fd = secure_open(run_fd, "lease.lock", os.O_RDWR, 0, True)
        try:
            fcntl.flock(lease_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno not in (errno.EACCES, errno.EAGAIN):
                raise
            busy = True
        else:
            busy = False
            fcntl.flock(lease_fd, fcntl.LOCK_UN)
        # Snapshot receipts in reverse publication order before the output
        # spool.  If a later receipt is visible, reading its prerequisites
        # afterward must see them too; a concurrent publication can therefore
        # only yield an older but internally consistent state.  A result is
        # published only after the spool is fsynced, so reading it before the
        # output also prevents pairing terminal evidence with a partial spool.
        result_receipt = optional_json(run_fd, "result.json")
        activation_receipt = optional_json(run_fd, "activation.json")
        lease_receipt = optional_json(run_fd, "lease.json")
        data, chunk = output_snapshot(run_fd, offset, max_bytes)
        return {
            "schema": OBSERVATION_SCHEMA, "request": request,
            "lease": lease_receipt,
            "activation": activation_receipt,
            "result": result_receipt,
            "lease_busy": busy, "output_offset": offset,
            "output_next_offset": offset + len(chunk),
            "output_stored_bytes": len(data),
            "output_stored_sha256": hashlib.sha256(data).hexdigest(),
            "output_base64": base64.b64encode(chunk).decode("ascii"),
        }
    finally:
        if lease_fd >= 0:
            os.close(lease_fd)
        os.close(run_fd)


def main():
    action = sys.argv[1] if len(sys.argv) == 2 else ""
    payload = stdin_object()
    if action == "launch":
        result = launch(payload)
    elif action == "record":
        record(payload)
        return
    elif action == "observe":
        result = observe(payload)
    else:
        raise ValueError("invalid helper action")
    sys.stdout.buffer.write(canonical(result) + b"\n")


__loader_source_b64__ = "__VQ_HELPER_SOURCE_B64__"

try:
    main()
except BaseException as exc:
    sys.stderr.write("detached scheduler helper: " + str(exc) + "\n")
    raise SystemExit(2)
'''

# The helper must be able to re-exec its exact embedded source for the recorder.
# Avoid interpolating request data into argv: only this fixed source literal is
# substituted, while the canonical request travels on stdin.
_DETACHED_REMOTE_HELPER_SOURCE = _DETACHED_REMOTE_HELPER_SOURCE.replace(
    "__VQ_HELPER_SOURCE_B64__",
    base64.b64encode(_DETACHED_REMOTE_HELPER_SOURCE.encode("utf-8")).decode("ascii"),
)


class _DetachedObservationUnavailable(transport.RemoteError):
    """A read-only observation transport failed without proving remote state."""


@dataclass(frozen=True)
class _DetachedSchedulerObservation:
    state: str
    returncode: int | None
    timed_out: bool
    output: bytes
    next_offset: int
    stored_bytes: int
    stored_sha256: str


def _canonical_detached_json(payload: Mapping[str, object]) -> bytes:
    return json.dumps(
        payload,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _detached_digest(payload: Mapping[str, object]) -> str:
    return hashlib.sha256(_canonical_detached_json(payload)).hexdigest()


def _detached_scratch_root(value: str) -> str:
    if (
        not isinstance(value, str)
        or not value.startswith("/")
        or value.startswith("//")
        or value == "/"
        or posixpath.normpath(value) != value
        or any(character in value for character in ("\x00", "\r", "\n"))
    ):
        raise transport.RemoteError(
            "detached scheduler command requires a normalized absolute scratch root"
        )
    return value


def _detached_request_for_binding(
    binding: fleet_operation.DetachedSchedulerCommandBinding,
    argv: list[str],
    *,
    scratch_root: str,
    timeout: float | None,
) -> dict[str, object]:
    if (
        not isinstance(argv, list)
        or not argv
        or any(not isinstance(arg, str) or "\x00" in arg for arg in argv)
    ):
        raise transport.RemoteError("detached scheduler command argv is invalid")
    if timeout is not None and (
        type(timeout) not in {int, float}
        or not math.isfinite(timeout)
        or timeout <= 0
    ):
        raise transport.RemoteError("detached scheduler command timeout is invalid")
    scratch_root = _detached_scratch_root(scratch_root)
    identity: dict[str, object] = {
        "schema": _DETACHED_REQUEST_IDENTITY_SCHEMA,
        "operation_id": binding.operation_id,
        "request_sha256": binding.request_sha256,
        "nonce": binding.nonce,
        "target": binding.target,
        "program": binding.program,
        "mode": binding.mode,
        "protocol": binding.protocol,
        "run_id": binding.run_id,
        "bound_at": binding.bound_at,
        "remote_run_dir": binding.remote_run_dir,
        "scratch_root": scratch_root,
        "argv": list(argv),
        "timeout_seconds": float(timeout) if timeout is not None else None,
        "output_limit": _DETACHED_OUTPUT_LIMIT,
    }
    if _detached_digest(
        {"schema": _DETACHED_COMMAND_SCHEMA, "argv": list(argv)}
    ) != binding.command_sha256:
        raise transport.RemoteError(
            "detached scheduler binding command digest does not match this invocation"
        )
    if _detached_digest(identity) != binding.remote_request_sha256:
        raise transport.RemoteError(
            "detached scheduler binding request digest does not match this invocation"
        )
    expected_dir = (
        f"{scratch_root.rstrip('/')}/{_DETACHED_PROTOCOL_ROOT}{binding.run_id}/"
        f"detached/run-{binding.run_id}"
    )
    if binding.remote_run_dir != expected_dir:
        raise transport.RemoteError(
            "detached scheduler binding run directory does not match scratch root"
        )
    request = {
        "schema": _DETACHED_REQUEST_SCHEMA,
        "binding": binding.as_dict(),
        "request_identity": identity,
    }
    if len(_canonical_detached_json(request)) > _DETACHED_REQUEST_LIMIT:
        raise transport.RemoteError(
            "detached scheduler canonical request exceeds the bounded helper limit"
        )
    return request


def _build_detached_binding_request(
    context: fleet_operation.OperationExecutionContext,
    *,
    host_cfg: config.HostConfig,
    argv: list[str],
    scratch_root: str,
    program: str,
    timeout: float | None,
    mode: str,
) -> tuple[
    fleet_operation.DetachedSchedulerCommandBinding,
    bool,
    dict[str, object],
]:
    scratch_root = _detached_scratch_root(scratch_root)
    if (
        not isinstance(argv, list)
        or not argv
        or any(not isinstance(arg, str) or "\x00" in arg for arg in argv)
    ):
        raise transport.RemoteError("detached scheduler command argv is invalid")
    if timeout is not None and (
        type(timeout) not in {int, float}
        or not math.isfinite(timeout)
        or timeout <= 0
    ):
        raise transport.RemoteError("detached scheduler command timeout is invalid")
    command_sha256 = _detached_digest(
        {"schema": _DETACHED_COMMAND_SCHEMA, "argv": list(argv)}
    )
    existing = fleet_operation.read_detached_scheduler_binding(
        context.operation_id,
    )
    if existing is None:
        run_id = secrets.token_hex(32)
        bound_at = datetime.now(UTC).isoformat()
        remote_run_dir = (
            f"{scratch_root.rstrip('/')}/{_DETACHED_PROTOCOL_ROOT}{run_id}/"
            f"detached/run-{run_id}"
        )
        provisional = fleet_operation.DetachedSchedulerCommandBinding(
            operation_id=context.operation_id,
            request_sha256=context.request_sha256,
            nonce=context.nonce,
            target=host_cfg.ssh,
            program=program,
            mode=mode,
            protocol=fleet_operation.DETACHED_SCHEDULER_PROTOCOL,
            run_id=run_id,
            bound_at=bound_at,
            remote_run_dir=remote_run_dir,
            command_sha256=command_sha256,
            remote_request_sha256="0" * 64,
        )
        identity = {
            "schema": _DETACHED_REQUEST_IDENTITY_SCHEMA,
            "operation_id": provisional.operation_id,
            "request_sha256": provisional.request_sha256,
            "nonce": provisional.nonce,
            "target": provisional.target,
            "program": provisional.program,
            "mode": provisional.mode,
            "protocol": provisional.protocol,
            "run_id": provisional.run_id,
            "bound_at": provisional.bound_at,
            "remote_run_dir": provisional.remote_run_dir,
            "scratch_root": scratch_root,
            "argv": list(argv),
            "timeout_seconds": float(timeout) if timeout is not None else None,
            "output_limit": _DETACHED_OUTPUT_LIMIT,
        }
        request_sha256 = _detached_digest(identity)
        desired = fleet_operation.DetachedSchedulerCommandBinding(
            **{
                **provisional.__dict__,
                "remote_request_sha256": request_sha256,
            }
        )
    else:
        desired = existing

    desired_request = _detached_request_for_binding(
        desired,
        argv,
        scratch_root=scratch_root,
        timeout=timeout,
    )

    try:
        binding, created = fleet_operation.bind_detached_scheduler_command(
            context,
            target=desired.target,
            program=desired.program,
            mode=desired.mode,
            protocol=desired.protocol,
            run_id=desired.run_id,
            bound_at=desired.bound_at,
            remote_run_dir=desired.remote_run_dir,
            command_sha256=desired.command_sha256,
            remote_request_sha256=desired.remote_request_sha256,
        )
    except fleet_operation.OperationError as exc:
        # A racing creator may have won with a different random run id. Reload
        # its immutable receipt and adopt only if every invocation field below
        # reconstructs the exact same request. Never launch after this branch.
        binding = fleet_operation.read_detached_scheduler_binding(
            context.operation_id,
        )
        if binding is None:
            raise transport.RemoteError(
                f"detached scheduler binding failed before launch: {exc}"
            ) from exc
        created = False

    if (
        binding.target != host_cfg.ssh
        or binding.program != program
        or binding.mode != mode
        or binding.protocol != fleet_operation.DETACHED_SCHEDULER_PROTOCOL
        or binding.command_sha256 != command_sha256
    ):
        raise transport.RemoteError(
            "detached scheduler binding differs from this invocation; "
            "remote outcome unknown and automatic replay is disabled"
        )
    request = (
        desired_request
        if binding == desired
        else _detached_request_for_binding(
            binding,
            argv,
            scratch_root=scratch_root,
            timeout=timeout,
        )
    )
    return binding, created, request


def _strict_detached_response(stdout: str, *, label: str) -> dict[str, object]:
    encoded = stdout.encode("utf-8", "surrogateescape")
    if len(encoded) > _DETACHED_RESPONSE_LIMIT:
        raise transport.RemoteError(f"{label} exceeded the bounded response limit")

    def duplicate_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key {key!r}")
            result[key] = value
        return result

    def nonfinite(value: str) -> object:
        raise ValueError(f"non-finite JSON number {value}")

    try:
        payload = json.loads(
            stdout,
            object_pairs_hook=duplicate_pairs,
            parse_constant=nonfinite,
        )
    except (ValueError, json.JSONDecodeError) as exc:
        raise transport.RemoteError(f"{label} is not strict JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise transport.RemoteError(f"{label} is not a JSON object")
    return payload


def _detached_helper_call(
    host_cfg: config.HostConfig,
    action: str,
    payload: Mapping[str, object],
) -> subprocess.CompletedProcess[str]:
    encoded = _canonical_detached_json(payload)
    if len(encoded) > _DETACHED_HELPER_INPUT_LIMIT:
        raise transport.RemoteError("detached scheduler helper input is too large")
    try:
        return transport.run_remote_shell(
            host_cfg,
            "python3",
            "-c",
            _DETACHED_REMOTE_HELPER_SOURCE,
            action,
            check=False,
            timeout=_DETACHED_HELPER_TIMEOUT,
            stdin_data=encoded.decode("utf-8") + "\n",
        )
    except transport.RemoteError as exc:
        raise _DetachedObservationUnavailable(str(exc)) from exc


def _validate_detached_receipt(
    payload: object,
    *,
    schema: str,
    fields: set[str],
    binding_sha256: str,
    label: str,
) -> dict[str, object] | None:
    if payload is None:
        return None
    if not isinstance(payload, dict) or set(payload) != fields:
        raise transport.RemoteError(f"{label} has invalid fields")
    if payload.get("schema") != schema:
        raise transport.RemoteError(f"{label} has an unsupported schema")
    if payload.get("binding_sha256") != binding_sha256:
        raise transport.RemoteError(f"{label} does not match the local binding")
    return payload


def _validate_detached_timestamp(value: object, *, label: str) -> datetime:
    if not isinstance(value, str):
        raise transport.RemoteError(f"{label} lacks a timestamp")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise transport.RemoteError(f"{label} has an invalid timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise transport.RemoteError(f"{label} timestamp is not UTC")
    return parsed


def _parse_detached_observation(
    payload: Mapping[str, object],
    *,
    request: Mapping[str, object],
    offset: int,
) -> _DetachedSchedulerObservation:
    expected_fields = {
        "schema",
        "request",
        "lease",
        "activation",
        "result",
        "lease_busy",
        "output_offset",
        "output_next_offset",
        "output_stored_bytes",
        "output_stored_sha256",
        "output_base64",
    }
    if set(payload) != expected_fields or payload.get("schema") != (
        _DETACHED_OBSERVATION_SCHEMA
    ):
        raise transport.RemoteError("detached observation has invalid schema or fields")
    if payload.get("request") != request:
        raise transport.RemoteError("remote request does not match the local binding")
    binding = request.get("binding")
    assert isinstance(binding, Mapping)
    binding_sha256 = _detached_digest(binding)
    lease = _validate_detached_receipt(
        payload.get("lease"),
        schema=_DETACHED_LEASE_SCHEMA,
        fields={"schema", "binding_sha256", "recorder_pid", "acquired_at"},
        binding_sha256=binding_sha256,
        label="detached lease receipt",
    )
    activation = _validate_detached_receipt(
        payload.get("activation"),
        schema=_DETACHED_ACTIVATION_SCHEMA,
        fields={
            "schema",
            "binding_sha256",
            "recorder_pid",
            "launch_intent",
            "activated_at",
        },
        binding_sha256=binding_sha256,
        label="detached activation receipt",
    )
    result = _validate_detached_receipt(
        payload.get("result"),
        schema=_DETACHED_RESULT_SCHEMA,
        fields={
            "schema",
            "binding_sha256",
            "status",
            "returncode",
            "timed_out",
            "completed_at",
            "output_sha256",
            "output_stored_sha256",
            "output_observed_bytes",
            "output_stored_bytes",
            "output_truncated",
        },
        binding_sha256=binding_sha256,
        label="detached result receipt",
    )
    for receipt, label in ((lease, "lease"), (activation, "activation")):
        if receipt is not None and (
            not isinstance(receipt.get("recorder_pid"), int)
            or isinstance(receipt.get("recorder_pid"), bool)
            or int(receipt["recorder_pid"]) <= 1
        ):
            raise transport.RemoteError(f"detached {label} receipt has invalid pid")
    lease_at = None
    if lease is not None:
        lease_at = _validate_detached_timestamp(
            lease.get("acquired_at"),
            label="detached lease receipt",
        )
    activation_at = None
    if activation is not None:
        activation_at = _validate_detached_timestamp(
            activation.get("activated_at"),
            label="detached activation receipt",
        )
    if activation is not None and (
        lease is None
        or activation.get("launch_intent") is not True
        or activation.get("recorder_pid") != lease.get("recorder_pid")
    ):
        raise transport.RemoteError("detached activation contradicts the lease")
    lease_busy = payload.get("lease_busy")
    if not isinstance(lease_busy, bool):
        raise transport.RemoteError("detached observation has invalid lease state")
    stored_bytes = payload.get("output_stored_bytes")
    next_offset = payload.get("output_next_offset")
    if (
        payload.get("output_offset") != offset
        or not isinstance(next_offset, int)
        or isinstance(next_offset, bool)
        or not isinstance(stored_bytes, int)
        or isinstance(stored_bytes, bool)
        or not offset <= next_offset <= stored_bytes <= _DETACHED_OUTPUT_LIMIT
    ):
        raise transport.RemoteError("detached observation has invalid output bounds")
    output_sha = payload.get("output_stored_sha256")
    if not isinstance(output_sha, str) or _SHA256_RE.fullmatch(output_sha) is None:
        raise transport.RemoteError("detached observation has invalid output digest")
    encoded = payload.get("output_base64")
    if not isinstance(encoded, str):
        raise transport.RemoteError("detached observation output is not base64 text")
    try:
        chunk = base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise transport.RemoteError("detached observation output is malformed") from exc
    if len(chunk) != next_offset - offset or len(chunk) > _DETACHED_OBSERVATION_CHUNK:
        raise transport.RemoteError("detached observation output length is inconsistent")

    if result is not None:
        returncode = result.get("returncode")
        timed_out = result.get("timed_out")
        status_value = result.get("status")
        result_stored = result.get("output_stored_bytes")
        observed_bytes = result.get("output_observed_bytes")
        truncated = result.get("output_truncated")
        if (
            not isinstance(returncode, int)
            or isinstance(returncode, bool)
            or not isinstance(timed_out, bool)
            or status_value not in {"success", "failed", "timed-out"}
            or status_value
            != ("timed-out" if timed_out else ("success" if returncode == 0 else "failed"))
            or not isinstance(result_stored, int)
            or isinstance(result_stored, bool)
            or not isinstance(observed_bytes, int)
            or isinstance(observed_bytes, bool)
            or not 0 <= result_stored <= observed_bytes
            or result_stored != min(observed_bytes, _DETACHED_OUTPUT_LIMIT)
            or truncated is not (observed_bytes > result_stored)
            or result_stored != stored_bytes
            or result.get("output_stored_sha256") != output_sha
            or not isinstance(result.get("output_sha256"), str)
            or _SHA256_RE.fullmatch(str(result["output_sha256"])) is None
            or (
                truncated is False
                and result.get("output_sha256")
                != result.get("output_stored_sha256")
            )
        ):
            raise transport.RemoteError("detached result has invalid output accounting")
        completed_at = _validate_detached_timestamp(
            result.get("completed_at"),
            label="detached result receipt",
        )
        if activation is None:
            raise transport.RemoteError("detached result lacks durable activation")
        assert activation_at is not None
        if completed_at < activation_at:
            raise transport.RemoteError("detached result predates activation")
    else:
        returncode = None
        timed_out = False

    if lease_at is not None and activation_at is not None and activation_at < lease_at:
        raise transport.RemoteError("detached activation predates lease acquisition")

    if result is not None and lease_busy:
        state = "finishing"
    elif result is not None:
        state = "completed"
    elif activation is not None and lease_busy:
        state = "running"
    elif activation is not None:
        state = "outcome-unknown"
    elif lease_busy:
        state = "starting"
    else:
        state = "preactivation-failed"
    return _DetachedSchedulerObservation(
        state=state,
        returncode=returncode,
        timed_out=timed_out,
        output=chunk,
        next_offset=next_offset,
        stored_bytes=stored_bytes,
        stored_sha256=output_sha,
    )


def _observe_detached_scheduler_command(
    host_cfg: config.HostConfig,
    request: Mapping[str, object],
    *,
    offset: int,
    tolerate_missing: bool = False,
) -> _DetachedSchedulerObservation:
    query = {
        "schema": _DETACHED_QUERY_SCHEMA,
        "request": dict(request),
        "offset": offset,
        "max_bytes": _DETACHED_OBSERVATION_CHUNK,
    }
    proc = _detached_helper_call(host_cfg, "observe", query)
    if proc.returncode == 255 or proc.returncode < 0:
        raise _DetachedObservationUnavailable(
            f"SSH observation returned rc={proc.returncode}: {proc.stderr.strip()}"
        )
    if proc.returncode != 0:
        if tolerate_missing:
            raise _DetachedObservationUnavailable(
                "read-only observation has not reached a valid retained run: "
                f"rc={proc.returncode}: {proc.stderr.strip()}"
            )
        raise transport.RemoteError(
            "detached scheduler remote outcome unknown; observation failed "
            f"rc={proc.returncode}: {proc.stderr.strip()}"
        )
    try:
        payload = _strict_detached_response(
            proc.stdout,
            label="detached scheduler observation",
        )
        return _parse_detached_observation(payload, request=request, offset=offset)
    except transport.RemoteError as exc:
        raise transport.RemoteError(
            f"detached scheduler remote outcome unknown: {exc}"
        ) from exc


def _run_bound_detached_scheduler_command(
    host_cfg: config.HostConfig,
    argv: list[str],
    *,
    scratch_root: str,
    program: str,
    timeout: float | None,
    mode: str,
    operation_context: fleet_operation.OperationExecutionContext,
) -> subprocess.CompletedProcess[str]:
    try:
        binding, created, request = _build_detached_binding_request(
            operation_context,
            host_cfg=host_cfg,
            argv=argv,
            scratch_root=scratch_root,
            program=program,
            timeout=timeout,
            mode=mode,
        )
    except fleet_operation.OperationError as exc:
        raise transport.RemoteError(
            f"detached scheduler binding failed before launch: {exc}"
        ) from exc

    label = f"scheduler {mode} command (detached) on {host_cfg.ssh}"
    ambiguous_launch = False
    transport_ambiguous_launch = False
    if created:
        try:
            launched = _detached_helper_call(host_cfg, "launch", request)
        except _DetachedObservationUnavailable as exc:
            ambiguous_launch = True
            transport_ambiguous_launch = True
            log.warning("detached launch response lost; adopting by observation: %s", exc)
        else:
            if launched.returncode == 255 or launched.returncode < 0:
                ambiguous_launch = True
                transport_ambiguous_launch = True
                log.warning(
                    "detached launch returned ambiguous rc=%s; adopting by observation",
                    launched.returncode,
                )
            elif launched.returncode != 0:
                ambiguous_launch = True
                log.warning(
                    "detached launch helper returned rc=%s after binding; "
                    "adopting by exact observation: %s",
                    launched.returncode,
                    launched.stderr.strip(),
                )
            else:
                try:
                    launch_payload = _strict_detached_response(
                        launched.stdout,
                        label="detached scheduler launch receipt",
                    )
                except transport.RemoteError as exc:
                    ambiguous_launch = True
                    log.warning(
                        "detached launch receipt invalid after binding; adopting by "
                        "exact observation: %s",
                        exc,
                    )
                if not ambiguous_launch and launch_payload != {
                    "schema": _DETACHED_LAUNCH_SCHEMA,
                    "status": "activated",
                    "remote_request_sha256": binding.remote_request_sha256,
                }:
                    ambiguous_launch = True
                    log.warning(
                        "detached launch receipt does not match the local binding; "
                        "adopting by exact observation"
                    )
        if ambiguous_launch:
            output.narrate(
                f"{label}: launch response ambiguous; observing retained binding "
                f"{binding.remote_run_dir}"
            )
        else:
            output.narrate(f"{label}: launched, run dir {binding.remote_run_dir}")
    else:
        output.narrate(
            f"{label}: adopting exact retained run {binding.remote_run_dir}"
        )
    refresh_admin_update_marker_heartbeat(f"{label} observation started")

    chunks: list[bytes] = []
    run_log_decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    accumulated_output_sha256 = hashlib.sha256()
    offset = 0
    last_snapshot_size = 0
    last_snapshot_sha256 = hashlib.sha256(b"").hexdigest()
    start = time.monotonic()
    last_observed = start
    result_deadline = (
        start + float(timeout) + max(30.0, 2 * DETACHED_BUILD_POLL_INTERVAL_SECONDS)
        if timeout is not None
        else None
    )
    poll_immediately = False
    while True:
        if not poll_immediately:
            time.sleep(DETACHED_BUILD_POLL_INTERVAL_SECONDS)
        poll_immediately = False
        now = time.monotonic()
        try:
            observed = _observe_detached_scheduler_command(
                host_cfg,
                request,
                offset=offset,
                tolerate_missing=transport_ambiguous_launch,
            )
        except _DetachedObservationUnavailable as exc:
            if (now - last_observed) >= DETACHED_BUILD_OBSERVATION_GRACE_SECONDS:
                raise transport.RemoteError(
                    f"detached {mode} command outcome unknown after "
                    f"{DETACHED_BUILD_OBSERVATION_GRACE_SECONDS:.0f}s without "
                    f"a valid observation ({exc}); retained run: "
                    f"{binding.remote_run_dir}"
                ) from exc
            log.warning("detached bound observation failed (tolerated): %s", exc)
            continue
        last_observed = now
        ambiguous_launch = False
        transport_ambiguous_launch = False
        if observed.stored_bytes < last_snapshot_size or (
            observed.stored_bytes == last_snapshot_size
            and observed.stored_sha256 != last_snapshot_sha256
        ):
            raise transport.RemoteError(
                "detached scheduler remote outcome unknown: retained output "
                "changed across read-only observations"
            )
        last_snapshot_size = observed.stored_bytes
        last_snapshot_sha256 = observed.stored_sha256
        if observed.output:
            accumulated_output_sha256.update(observed.output)
            chunks.append(observed.output)
            live_text = run_log_decoder.decode(observed.output, final=False)
            if live_text:
                output.run_log_write(live_text)
            offset = observed.next_offset
        elapsed = now - start
        refresh_admin_update_marker_heartbeat(
            f"{label} {observed.state} ({elapsed:.0f}s elapsed, "
            f"{offset} retained output bytes)"
        )
        if observed.state in {"preactivation-failed", "outcome-unknown"}:
            raise transport.RemoteError(
                f"detached scheduler remote outcome unknown ({observed.state}); "
                f"retained run: {binding.remote_run_dir}; automatic replay is disabled"
            )
        if offset < observed.stored_bytes:
            # The remote spool is already bounded. Drain an observed backlog
            # through immediate, bounded read-only calls instead of sleeping a
            # full build-poll interval per 64 KiB chunk. In particular, a
            # valid terminal receipt must not expire while only local output
            # harvesting remains.
            poll_immediately = True
            continue
        if observed.state in {"completed", "finishing"}:
            assert observed.returncode is not None
            final_live_text = run_log_decoder.decode(b"", final=True)
            if final_live_text:
                output.run_log_write(final_live_text)
            if accumulated_output_sha256.hexdigest() != observed.stored_sha256:
                raise transport.RemoteError(
                    "detached scheduler remote outcome unknown: streamed output "
                    "does not match the terminal retained-output digest"
                )
            if observed.timed_out:
                raise transport.RemoteError(
                    "detached scheduler command exceeded its remote self-timeout; "
                    f"terminal evidence is retained at {binding.remote_run_dir}"
                )
            refresh_admin_update_marker_heartbeat(
                f"{label} finished rc={observed.returncode}"
            )
            return subprocess.CompletedProcess(
                args=list(argv),
                returncode=observed.returncode,
                stdout=b"".join(chunks).decode("utf-8", "replace"),
                stderr="",
            )
        if result_deadline is not None and now >= result_deadline:
            raise transport.RemoteError(
                f"detached {mode} command outcome unknown after its remote "
                f"self-timeout window; no process was killed by the observer. "
                f"Retained run: {binding.remote_run_dir}"
            )


def _run_detached_scheduler_command(
    host_cfg: config.HostConfig,
    argv: list[str],
    *,
    scratch_root: str,
    program: str,
    timeout: float | None,
    mode: str,
    operation_context: fleet_operation.OperationExecutionContext | None = None,
) -> subprocess.CompletedProcess[str]:
    """Select the bound rollout protocol or the compatible manual path."""
    if operation_context is None:
        try:
            operation_context = fleet_operation.execution_context_from_environ()
        except fleet_operation.OperationError as exc:
            raise transport.RemoteError(
                f"invalid fleet operation execution context: {exc}"
            ) from exc
    if operation_context is not None:
        return _run_bound_detached_scheduler_command(
            host_cfg,
            argv,
            scratch_root=scratch_root,
            program=program,
            timeout=timeout,
            mode=mode,
            operation_context=operation_context,
        )
    return _run_legacy_detached_scheduler_command(
        host_cfg,
        argv,
        scratch_root=scratch_root,
        program=program,
        timeout=timeout,
        mode=mode,
    )


def _run_legacy_detached_scheduler_command(
    host_cfg: config.HostConfig,
    argv: list[str],
    *,
    scratch_root: str,
    program: str,
    timeout: float | None,
    mode: str,
) -> subprocess.CompletedProcess[str]:
    """Run a build command detached on the build host, observing via polls.

    The foreground form (:func:`_run_scheduler_update_command_with_heartbeat`)
    ties the remote command's life to one SSH connection: when the connection
    dies, so does a 90-minute compile (the 2026-07-24 pbs-cluster deploy, ssh
    rc=255). Here the command starts under ``setsid`` with its output, return
    code, and pid parked in a per-run directory on the build host; the driver
    then polls over fresh connections, streaming each output increment into
    the run log. Any single connection loss costs one poll, not the build.

    Returns a synthetic ``CompletedProcess`` whose stdout is the accumulated
    build output. Raises :class:`transport.RemoteError` when the wall timeout
    expires (the process group is killed first) or when observation fails for
    :data:`DETACHED_BUILD_OBSERVATION_GRACE_SECONDS` straight.
    """
    run_dir = (
        f"{scratch_root.rstrip('/')}/.vq-admin/detached/"
        f"{program}-{uuid.uuid4().hex[:12]}"
    )
    label = f"scheduler {mode} command (detached) on {host_cfg.ssh}"
    launcher = "\n".join(
        [
            "set -euo pipefail",
            f"RUNDIR={shlex.quote(run_dir)}",
            'mkdir -p "$RUNDIR"',
            'cat > "$RUNDIR/cmd.sh" <<\'VQCMD\'',
            shlex.join(argv),
            "VQCMD",
            # $0 carries RUNDIR into the inner shell, dodging a second level
            # of quoting. rc is written via tmp+mv so a poll can never read a
            # half-written file.
            "setsid bash -c 'bash \"$0/cmd.sh\" > \"$0/output.log\" 2>&1; "
            "echo $? > \"$0/rc.tmp\"; mv \"$0/rc.tmp\" \"$0/rc\"' \"$RUNDIR\" "
            "< /dev/null > /dev/null 2>&1 &",
            'echo "$!" > "$RUNDIR/pid"',
            'echo "detached pid $!"',
        ]
    )
    started = transport.run_remote_shell(
        host_cfg, "bash", "-s", check=False, stdin_data=launcher,
    )
    if started.returncode != 0:
        raise transport.RemoteError(
            f"could not launch detached {mode} command on {host_cfg.ssh}: "
            f"{_combined_output(started.stdout, started.stderr).strip()}"
        )
    output.narrate(f"{label}: launched, run dir {run_dir}")
    refresh_admin_update_marker_heartbeat(f"{label} started")

    chunks: list[str] = []
    offset = 0
    start = time.monotonic()
    last_observed = start
    while True:
        time.sleep(DETACHED_BUILD_POLL_INTERVAL_SECONDS)
        elapsed = time.monotonic() - start
        if timeout and elapsed >= timeout:
            with contextlib.suppress(transport.RemoteError):
                transport.run_remote_shell(
                    host_cfg, "bash", "-s", check=False,
                    stdin_data=(
                        f"RUNDIR={shlex.quote(run_dir)}\n"
                        'kill -- "-$(cat "$RUNDIR/pid")" 2>/dev/null || true\n'
                    ),
                )
            raise transport.RemoteError(
                f"detached {mode} command exceeded {timeout:.0f}s; process "
                f"group killed. Partial output is in the transcript; the run "
                f"dir {run_dir} is retained on {host_cfg.ssh} for forensics."
            )
        poll_script = "\n".join(
            [
                f"RUNDIR={shlex.quote(run_dir)}",
                f"OFF={offset}",
                'if [[ -f "$RUNDIR/rc" ]]; then echo "RC=$(cat "$RUNDIR/rc")";'
                ' else echo "RC=none"; fi',
                'SIZE=$(stat -c %s "$RUNDIR/output.log" 2>/dev/null || echo 0)',
                'echo "SIZE=$SIZE"',
                f"echo {_DETACHED_OUTPUT_MARKER}",
                'if [[ "$SIZE" -gt "$OFF" ]]; then tail -c +$((OFF + 1)) '
                '"$RUNDIR/output.log" | head -c $((SIZE - OFF)); fi',
            ]
        )
        try:
            poll = transport.run_remote_shell(
                host_cfg, "bash", "-s", check=False, stdin_data=poll_script,
            )
        except transport.RemoteError as exc:
            if (time.monotonic() - last_observed
                    ) >= DETACHED_BUILD_OBSERVATION_GRACE_SECONDS:
                raise transport.RemoteError(
                    f"detached {mode} command unobservable for "
                    f"{DETACHED_BUILD_OBSERVATION_GRACE_SECONDS:.0f}s "
                    f"({exc}). The build may still be running on "
                    f"{host_cfg.ssh}; run dir: {run_dir}"
                ) from exc
            log.warning("detached poll failed (tolerated): %s", exc)
            continue
        last_observed = time.monotonic()
        head, _, chunk = poll.stdout.partition(
            f"{_DETACHED_OUTPUT_MARKER}\n"
        )
        rc_line = size_line = ""
        for line in head.splitlines():
            if line.startswith("RC="):
                rc_line = line[3:].strip()
            elif line.startswith("SIZE="):
                size_line = line[5:].strip()
        if chunk:
            chunks.append(chunk)
            output.run_log_write(chunk)
            offset += len(chunk.encode("utf-8", "surrogateescape"))
        refresh_admin_update_marker_heartbeat(
            f"{label} still running ({elapsed:.0f}s elapsed, "
            f"{offset} output bytes)"
        )
        if rc_line and rc_line != "none":
            try:
                rc = int(rc_line)
            except ValueError:
                raise transport.RemoteError(
                    f"detached {mode} command wrote a malformed rc "
                    f"{rc_line!r} in {run_dir} on {host_cfg.ssh}"
                ) from None
            # One final drain in case output landed between the tail and the
            # rc read, then best-effort cleanup: the transcript holds the
            # authoritative copy of the output.
            _ = size_line
            with contextlib.suppress(transport.RemoteError):
                tail = transport.run_remote_shell(
                    host_cfg, "bash", "-s", check=False,
                    stdin_data=(
                        f"RUNDIR={shlex.quote(run_dir)}\n"
                        f"tail -c +{offset + 1} \"$RUNDIR/output.log\" "
                        "2>/dev/null || true\n"
                        'rm -rf "$RUNDIR"\n'
                    ),
                )
                if tail.stdout:
                    chunks.append(tail.stdout)
                    output.run_log_write(tail.stdout)
            refresh_admin_update_marker_heartbeat(f"{label} finished rc={rc}")
            return subprocess.CompletedProcess(
                args=list(argv), returncode=rc,
                stdout="".join(chunks), stderr="",
            )


def _active_scheduler_job_specs(target: str, *, multi_user: bool) -> list[JobSpec]:
    """Specs that still occupy ``target``'s scheduler, re-read from disk.

    Always re-enumerates: on a multi-user driver a user directory can appear
    mid-wait, and a cached first listing would let a drain-wait declare
    quiescence that is not real.
    """
    active: list[JobSpec] = []
    for spec_path in _visible_spec_paths(multi_user=multi_user):
        try:
            spec = JobSpec.read(spec_path)
        except (OSError, ValueError):
            continue
        if spec.scheduler_target != target:
            continue
        if spec.is_terminal:
            continue
        if spec.scheduler_job_id is None and spec.state == JobState.PENDING:
            continue
        if spec.scheduler_state == "reattach_failed" and spec.scheduler_job_id is None:
            # A ghost: vq explicitly failed to reattach this spec after a daemon
            # restart and holds no cluster handle for it, so there is no
            # scheduler job whose completion could ever clear it. Counting it
            # does not merely inflate the wait -- it makes the wait
            # unsatisfiable, because nothing on the cluster will ever finish on
            # its behalf.
            #
            # pbs-cluster, 2026-08-01: a drain-wait for a helper rebuild was blocked
            # behind a real 4-hour GPW job AND this shape (f53f1cc9bb6c,
            # reattach_failed, absent from qstat). The real job was killed to
            # unblock the rebuild; the ghost would have blocked it regardless.
            #
            # Deliberately narrow: only a spec vq has already given up on AND
            # that has no job id. A RUNNING spec with no id but no
            # reattach_failed marker may simply be mid-dispatch, and skipping
            # that would declare quiescence while a job is starting.
            continue
        active.append(spec)
    return active


def _format_active_scheduler_jobs(specs: list[JobSpec]) -> list[str]:
    return [f"{spec.id}({spec.state.value})" for spec in specs]


def _poll_scheduler_phases(
    host_cfg: config.HostConfig, specs: list[JobSpec]
) -> dict[str, object]:
    """One batched ``qstat``/``squeue`` for these specs' scheduler ids.

    A module-level seam on purpose: this is the only place the admin path
    crosses the SSH boundary to the *scheduler* rather than to a shell, and
    tests patch it here so the unit suite never opens a socket.
    """
    from vq.scheduler_dispatch import (  # noqa: PLC0415 — avoid an import cycle
        scheduler_dispatcher_for,
        scheduler_handle_for_spec,
    )

    dispatcher = scheduler_dispatcher_for(host_cfg)
    return dispatcher.poll(
        scheduler_handle_for_spec(
            dispatcher,
            spec,
            job_id=str(spec.scheduler_job_id),
        )
        for spec in specs
    )


@dataclass
class SchedulerActiveJobs:
    """Active-job census for one scheduler target, reconciled where possible."""

    blocking: list[JobSpec] = field(default_factory=list)
    """Specs that genuinely hold the target."""

    reaped: list[str] = field(default_factory=list)
    """Job ids the scheduler positively reports as gone. vq still tracked them
    as active; they no longer block."""

    unreconciled: list[str] = field(default_factory=list)
    """Specs that could not be checked against the scheduler because vq never
    recorded a scheduler job id for them (a daemon that died mid-``qsub``).
    These keep blocking — an untracked batch job may still be live — but they
    are named so an operator can clear them instead of guessing at a count."""

    probe_error: str | None = None
    """Set when the scheduler probe itself failed. Everything keeps blocking."""

    @property
    def display(self) -> list[str]:
        return _format_active_scheduler_jobs(self.blocking)


def _reconcile_active_scheduler_jobs(
    host: str,
    host_cfg: config.HostConfig,
    specs: list[JobSpec],
) -> SchedulerActiveJobs:
    """Drop specs the scheduler says are gone from the active-job census.

    vq's own job state is not a reliable statement about the cluster.
    ``_start_scheduler_job`` stamps ``state=RUNNING`` *before* the qsub even
    runs, and the scheduler's real phase lives in the separate
    ``scheduler_state`` field — so "vq says RUNNING, qstat says QUEUED" is by
    design, and "vq says RUNNING, qstat has never heard of it" is what a daemon
    death or a failed reattach leaves behind. On pbs-cluster that meant six jobs, four
    queued and two long gone, blocking every attempt at host maintenance while
    reading to a human as multi-day production runs.

    The guard those jobs feed is correct and stays. This only removes entries
    the scheduler **positively reports as finished**, which cannot weaken it:

    * A probe failure (SSH down, qstat unparseable, an unbuildable dispatcher)
      leaves every spec blocking. Fail closed — never let a broken probe be the
      thing that green-lights a rebuild under live work.
    * A spec with no recorded scheduler job id is unprobeable and keeps
      blocking, because an untracked batch job may still be running. It is
      reported by id so the operator can act on it.
    """
    census = SchedulerActiveJobs()
    if not specs:
        return census
    probeable = [s for s in specs if s.scheduler_job_id]
    census.unreconciled = [s.id for s in specs if not s.scheduler_job_id]
    if not probeable:
        census.blocking = list(specs)
        return census
    try:
        phases = _poll_scheduler_phases(host_cfg, probeable)
    except Exception as exc:  # noqa: BLE001 — any probe failure fails closed
        census.blocking = list(specs)
        census.probe_error = str(exc)
        log.warning(
            "could not reconcile %s's active scheduler jobs against the "
            "scheduler (%s); treating all %d as active",
            host,
            exc,
            len(specs),
        )
        return census

    from vq.scheduler_dialect import SchedulerPhase  # noqa: PLC0415

    for spec in specs:
        if not spec.scheduler_job_id:
            census.blocking.append(spec)
            continue
        phase = phases.get(str(spec.scheduler_job_id))
        if phase is SchedulerPhase.FINISHED:
            census.reaped.append(f"{spec.id}({spec.scheduler_job_id})")
            continue
        census.blocking.append(spec)
    if census.reaped:
        log.info(
            "%s: %d tracked scheduler job(s) are gone from the scheduler and "
            "no longer block maintenance: %s",
            host,
            len(census.reaped),
            ", ".join(census.reaped),
        )
    return census


SCHEDULER_DRAIN_WAIT_POLL_SECONDS = 30.0
"""How often a drain-and-wait maintenance window re-counts active jobs.

The count only moves when the driver daemon reconciles a finished scheduler
job to terminal, which happens on its own poll cadence — a tighter loop would
just burn heartbeats without converging sooner."""

_scheduler_drain_wait_sleep: Callable[[float], None] = time.sleep
"""Sleep seam for scheduler quiescence tests.

Patch this callable rather than ``time.sleep`` itself.  The ``time`` module is
process-global, and timed ``subprocess.run`` calls use it internally for their
POSIX wait backoff; replacing ``vq.admin.time.sleep`` therefore also changes
unrelated lifecycle-lock preflight subprocesses.
"""


@dataclass
class SchedulerDrainWaitOutcome:
    """Result of waiting for a scheduler target to go quiet."""

    quiesced: bool
    waited_seconds: float
    polls: int
    deadline_seconds: float = 0.0
    """The budget the operator asked for. Distinct from ``waited_seconds``:
    even a zero-budget check takes a measurable moment, so "was a wait
    requested?" must be answered from this, not from elapsed time."""
    remaining: list[str] = field(default_factory=list)
    lane_added: bool = False
    stalled_reason: str | None = None
    """Set when the wait ended for a reason other than the deadline — every
    remaining job being SUSPENDED (qhold'd), or every remaining job being one
    vq cannot reconcile against the scheduler. Neither ever converges."""
    reaped: list[str] = field(default_factory=list)
    """Jobs the scheduler reported gone, which vq had been counting as active."""
    unreconciled: list[str] = field(default_factory=list)
    """Jobs with no recorded scheduler id — unprobeable, still blocking."""
    probe_error: str | None = None


def _await_scheduler_quiescence(
    host: str,
    *,
    host_cfg: config.HostConfig,
    multi_user: bool,
    deadline_seconds: float,
    poll_seconds: float = SCHEDULER_DRAIN_WAIT_POLL_SECONDS,
) -> SchedulerDrainWaitOutcome:
    """Wait for ``host``'s already-submitted scheduler jobs to finish.

    The active-job guard this serves is correct — it is what protected live
    paper jobs on pbs-cluster — but on a shared production node that is rarely idle it
    was unsatisfiable without a hand-built drain window, which is why pbs-cluster sat
    digest-red. This is that window, supported: the caller holds a drain lane so
    no *new* work is dispatched to the target, and this loop waits out the work
    already on the cluster.

    The marker is held for the whole wait, so every poll refreshes its
    heartbeat — otherwise ``vq admin status`` would start recommending
    ``--force`` against a wait that is working exactly as intended.

    Returns without waiting when ``deadline_seconds <= 0``, which keeps the
    default (no ``--drain-wait``) behaviour byte-identical to the one-shot
    refusal.
    """
    started = time.monotonic()
    polls = 0
    while True:
        # Re-enumerate AND re-reconcile every poll. vq's own state is not a
        # statement about the cluster (see _reconcile_active_scheduler_jobs):
        # without the probe, a job the scheduler finished days ago keeps the
        # count above zero and the wait burns its whole deadline for nothing.
        census = _reconcile_active_scheduler_jobs(
            host, host_cfg, _active_scheduler_job_specs(host, multi_user=multi_user)
        )
        specs = census.blocking
        polls += 1
        waited = time.monotonic() - started
        if not specs:
            return SchedulerDrainWaitOutcome(
                quiesced=True,
                waited_seconds=waited,
                polls=polls,
                deadline_seconds=deadline_seconds,
                reaped=census.reaped,
            )
        remaining = census.display
        if deadline_seconds <= 0:
            return SchedulerDrainWaitOutcome(
                quiesced=False,
                waited_seconds=waited,
                polls=polls,
                deadline_seconds=deadline_seconds,
                remaining=remaining,
                reaped=census.reaped,
                unreconciled=census.unreconciled,
                probe_error=census.probe_error,
            )
        # A qhold'd job stays non-terminal and keeps its scheduler job id, so it
        # counts as active forever. Waiting out a queue that is entirely held
        # would burn the whole deadline and then report a timeout, hiding the
        # real answer: resume them or exclude them first.
        if all(spec.state == JobState.SUSPENDED for spec in specs):
            return SchedulerDrainWaitOutcome(
                quiesced=False,
                waited_seconds=waited,
                polls=polls,
                deadline_seconds=deadline_seconds,
                remaining=remaining,
                reaped=census.reaped,
                unreconciled=census.unreconciled,
                probe_error=census.probe_error,
                stalled_reason=(
                    f"all {len(specs)} remaining job(s) on {host} are SUSPENDED "
                    "(scheduler-held); a hold never completes, so this wait "
                    f"cannot converge — `vq resume {host} --all` or let them "
                    "finish before retrying"
                ),
            )
        # Same reasoning as the all-SUSPENDED case: a spec vq never recorded a
        # scheduler id for cannot be observed, so it will never be seen to
        # finish. Waiting is guaranteed futile — say so now instead of at the
        # end of a four-hour budget.
        if census.unreconciled and len(census.unreconciled) == len(specs):
            return SchedulerDrainWaitOutcome(
                quiesced=False,
                waited_seconds=waited,
                polls=polls,
                deadline_seconds=deadline_seconds,
                remaining=remaining,
                reaped=census.reaped,
                unreconciled=census.unreconciled,
                probe_error=census.probe_error,
                stalled_reason=(
                    f"the {len(specs)} remaining job(s) on {host} "
                    f"({', '.join(census.unreconciled)}) have no scheduler job "
                    "id recorded, so vq cannot observe them finishing and this "
                    "wait cannot converge. They are most likely corpses from a "
                    "daemon that died mid-submit — confirm with qstat/squeue on "
                    f"the cluster, then `vq kill {host} <jobid>` to clear them"
                ),
            )
        if waited >= deadline_seconds:
            return SchedulerDrainWaitOutcome(
                quiesced=False,
                waited_seconds=waited,
                polls=polls,
                deadline_seconds=deadline_seconds,
                remaining=remaining,
                reaped=census.reaped,
                unreconciled=census.unreconciled,
                probe_error=census.probe_error,
            )
        refresh_admin_update_marker_heartbeat(
            f"drain-wait on {host}: {len(specs)} active scheduler job(s) "
            f"after {waited:.0f}s of {deadline_seconds:.0f}s"
        )
        _scheduler_drain_wait_sleep(
            min(poll_seconds, max(1.0, deadline_seconds - waited))
        )


def _take_scheduler_drain_lane(
    host: str,
    *,
    drain_wait_seconds: float,
    lease_id: str,
    admin_token: str | None = None,
    multi_user: bool | None = None,
) -> str | None:
    """Hold ``host``'s drain lane and return this operation's lease ID.

    The lane stops the daemon dispatching *new* work to the target for the
    duration; without it the wait would race an actively-fed queue and could
    never converge on a busy node.

    **This must be called from inside the caller's ``try``, with the result
    assigned to a local the caller's ``finally`` reads.** The first cut carried
    the flag out on the returned outcome object, which meant a Ctrl-C during a
    multi-hour wait — or any OSError from the heartbeat — skipped the
    assignment, so the ``finally`` saw ``lane_added=False`` and left the lane
    held forever. Worse, a retry could not clear it: the second run's
    ``add_scheduler_host`` returned False (already held), so its release was
    skipped too, and vq's own "never lift an operator's hold" protection then
    read vq's leaked lane as deliberate. pbs-cluster dispatch would stop until someone
    ran ``vq drain --scheduler-host pbs-cluster --release`` by hand.
    """
    _validate_drain_wait_seconds(drain_wait_seconds)
    if drain_wait_seconds <= 0:
        return None
    try:
        lease, _changed = drain.acquire_scheduler_drain_lease(
            host,
            reason=f"vq admin update {host}",
            owner=_scheduler_drain_lane_owner(host, lease_id),
            owner_pid=os.getpid(),
            lease_id=lease_id,
            token=admin_token,
            multi_user=multi_user,
        )
        return lease.lease_id
    except (OSError, drain.SchedulerDrainLeaseError) as exc:
        raise AdminError(
            f"could not safely hold the scheduler drain lane for {host}: "
            f"{exc}. The update did not start."
        ) from exc


def _scheduler_drain_lane_owner(host: str, lease_id: str) -> str:
    """Return the exact recovery identity for one admin update claim."""
    return f"admin-update:{host}:{lease_id}"


def _release_scheduler_drain_lane(
    host: str,
    lease_id: str | None,
    *,
    lease_owner: str | None = None,
    admin_token: str | None = None,
    recovery_driver: str | None = None,
    multi_user: bool | None = None,
) -> str | None:
    """Drop exactly this update's claim; preserve every overlapping owner."""
    if lease_id is None:
        return None
    try:
        drain.release_scheduler_drain_lease(
            lease_id,
            token=admin_token,
            multi_user=multi_user,
        )
    except (OSError, drain.SchedulerDrainLeaseError) as exc:
        owner = lease_owner or _scheduler_drain_lane_owner(host, lease_id)
        recovery = shlex.join(
            [
                "vq",
                "drain",
                "--release",
                "--scheduler-host",
                host,
                "--lease-owner",
                owner,
                "localhost",
            ]
        )
        recovery_location = (
            f" on scheduler driver {recovery_driver!r}"
            if recovery_driver
            else " on the scheduler driver"
        )
        detail = (
            f"could not release scheduler drain lease {lease_id} for {host}: "
            f"{exc}; run{recovery_location}: `{recovery}` with admin "
            "authentication. Do not use "
            "a broad scheduler-host release because it would clear other "
            "owners' holds"
        )
        log.warning("%s", detail)
        return detail
    return None


def _active_jobs_refusal(
    host: str,
    outcome: SchedulerDrainWaitOutcome,
    what: str,
) -> str:
    """Operator-facing reason a scheduler update refused to run.

    The count alone was not actionable: on pbs-cluster it named six jobs of which four
    were merely queued and two had been gone for days, and nothing said which
    was which. Every qualifier below exists so the operator knows what to do
    next rather than re-deriving it from qstat by hand.
    """
    if outcome.stalled_reason is not None:
        return outcome.stalled_reason
    base = (
        "scheduler host has active submitted job(s); wait for them "
        f"to finish before {what}"
    )
    qualifiers: list[str] = []
    if outcome.probe_error is not None:
        qualifiers.append(
            "vq could NOT reconcile these against the scheduler "
            f"({outcome.probe_error}), so all tracked jobs are counted — "
            "fix the probe before trusting this count"
        )
    if outcome.reaped:
        qualifiers.append(
            f"{len(outcome.reaped)} stale entr(y/ies) the scheduler already "
            f"finished were discounted ({', '.join(outcome.reaped)})"
        )
    if outcome.unreconciled:
        qualifiers.append(
            f"{len(outcome.unreconciled)} have no scheduler job id recorded "
            f"and cannot be observed ({', '.join(outcome.unreconciled)}); "
            f"confirm on the cluster, then `vq kill {host} <jobid>`"
        )
    if outcome.deadline_seconds > 0:
        head = (
            f"{base} — {len(outcome.remaining)} still active after "
            f"{outcome.waited_seconds:.0f}s of drain-wait"
        )
    else:
        head = (
            f"{base}. On a node that is never idle, use "
            f"`vq admin update ... --drain-wait DUR` to hold a drain lane for "
            f"{host} and wait the running work out"
        )
    return head + ("; " + "; ".join(qualifiers) if qualifiers else "")


def _visible_spec_paths(*, multi_user: bool) -> list[Path]:
    if multi_user:
        spec_paths: list[Path] = []
        for user_dir in paths._all_user_dirs():
            queue_dir = paths.user_queue_dir(user_dir.name)
            if queue_dir.is_dir():
                spec_paths.extend(sorted(queue_dir.glob("*.json")))
        return spec_paths
    queue_dir = paths.queue_dir()
    if not queue_dir.is_dir():
        return []
    return sorted(queue_dir.glob("*.json"))


def _combined_output(stdout: str, stderr: str) -> str:
    if stdout and stderr:
        return f"{stdout.rstrip()}\n{stderr.rstrip()}\n"
    return stdout or stderr


def _last_output_line(text: str) -> str:
    """The last non-blank line of a captured command's output, or "".

    A failure summary gets one line, and the last one is the right one: a
    remote command that dies says why on its way out (``fatal: 'origin' does
    not appear to be a git repository``, ``vibeqc-release requires --tag``),
    after whatever progress it printed first.
    """
    for line in reversed(text.splitlines()):
        stripped = line.strip()
        if stripped:
            return stripped
    return ""


def _run_log_command_output(label: str, rc: int | None, text: str) -> None:
    """Tee a captured remote command's output into the update transcript.

    The venv build lane streams its build into the run log line-by-line as it
    happens; the scheduler lanes run their command through
    ``transport.run_remote_shell``, which buffers, so the output only exists
    once the command has returned. Written here as one delimited block —
    without it the transcript keeps the phases and heartbeats but drops the
    command output entirely, and a failed deploy reads back as a bare
    ``rc=1`` under ``vq admin logs``.
    """
    output.run_log_write(f"--- {label} output (rc={rc}) ---")
    output.run_log_write(text.rstrip() or "(no output)")
    output.run_log_write(f"--- end {label} output ---")


def _run_git_tag_check(git_dir: Path) -> tuple[int, str | None]:
    """v0.5.24: run ``git describe --exact-match --tags HEAD`` and
    return (rc, tag_or_none).

    ``git describe --exact-match`` returns rc=0 + the tag name on
    stdout when HEAD points exactly at a tag; rc=128 (and a "no tag
    exactly matches" stderr) when no tag points at the current commit.
    We treat rc=128 as a legitimate state (just means: no tag here);
    the caller compares ``actual_tag`` to ``expected_tag`` for the
    actual verdict."""
    try:
        proc = subprocess.run(
            ["git", "-C", str(git_dir), "describe",
             "--exact-match", "--tags", "HEAD"],
            capture_output=True,
            text=True,
            timeout=30,
            stdin=subprocess.DEVNULL,
        )
    except (subprocess.TimeoutExpired, OSError):
        # Both error paths leave us with no verdict; treat as
        # "no tag" (verification will fail comparison).
        return 1, None
    rc = proc.returncode
    tag = proc.stdout.strip() if rc == 0 else None
    return rc, tag


def _run_expected_git_tag_check(
    git_dir: Path, expected_tag: str,
) -> tuple[int, str | None]:
    """Verify one fully-qualified tag ref points at ``HEAD``.

    ``git describe --exact-match`` chooses one tag when several point at the
    same commit.  That choice is based on tag metadata, not the selected
    release identity, so a perfectly valid expected tag could be reported as
    a mismatch.  Resolve the named ref explicitly instead.
    """
    tag_rc, tag_sha, _ = _run_git_resolve_commit(
        git_dir, f"refs/tags/{expected_tag}^{{commit}}",
    )
    head_rc, head_sha = _run_git_sha_check(git_dir)
    if tag_rc != 0 or tag_sha is None:
        return tag_rc or 1, None
    if head_rc != 0 or head_sha is None:
        return head_rc or 1, None
    if tag_sha != head_sha:
        return 1, None
    return 0, expected_tag


def _run_git_branch_check(git_dir: Path) -> tuple[int, str | None]:
    """v0.7.1 *Lamport's Clock*: run ``git rev-parse --abbrev-ref HEAD``
    and return ``(rc, branch_or_none)``.

    The check exists to catch the silent-branch-drift class surfaced
    2026-05-25 (workstation vibeqc-dev silently on ``release`` despite
    config saying ``main`` — root cause was vibe-qc's
    ``scripts/_safe_build_env.sh`` losing argv across a niced re-exec,
    fixed in vibe-qc commit ``ea195796``). vq's defense-in-depth is
    this rev-parse after the pull: even if a future build helper
    re-introduces argv loss, the next ``vq admin update`` will fail
    loudly with ``branch_mismatch`` instead of silently switching the
    env onto the wrong branch.

    Output semantics from git:
      * Normal branch checkout → rc=0, stdout=branch name (e.g.
        ``main``, ``release``, ``feature/foo``).
      * Detached HEAD (e.g. ``git checkout <SHA>``) → rc=0,
        stdout=``HEAD``. We return ``"HEAD"`` here; the caller's
        ``branch_matches`` comparison turns this into a mismatch
        unless the configured branch is literally ``"HEAD"`` (which
        no one configures, by construction).
      * Broken .git, network/filesystem errors → non-zero rc. We
        return ``(rc, None)`` and the caller's ``branch_matches``
        evaluates to False (None != "main").

    See ``docs/v0_7_1_lamports_clock_design.md`` § Item 1 for the
    full incident postmortem + design rationale.
    """
    try:
        proc = subprocess.run(
            ["git", "-C", str(git_dir),
             "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            timeout=30,
            stdin=subprocess.DEVNULL,
        )
    except (subprocess.TimeoutExpired, OSError):
        return 1, None
    rc = proc.returncode
    branch = proc.stdout.strip() if rc == 0 else None
    return rc, branch


def _run_git_sha_check(git_dir: Path) -> tuple[int, str | None]:
    """Return ``(rc, HEAD_SHA)`` for exact-SHA verification."""
    try:
        proc = subprocess.run(
            ["git", "-C", str(git_dir), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=30,
            stdin=subprocess.DEVNULL,
        )
    except (subprocess.TimeoutExpired, OSError):
        return 1, None
    rc = proc.returncode
    sha = proc.stdout.strip().lower() if rc == 0 else None
    if sha is not None and not _FULL_SHA_RE.fullmatch(sha):
        return 1, None
    return rc, sha


def _active_lifecycle_pass_fds() -> tuple[int, ...]:
    """Descriptors mutating children inherit to retain lifecycle locks.

    A Python controller may be SIGKILLed while Git continues in the kernel or
    as an orphaned child. Without descriptor inheritance the parent's flock
    disappears immediately and another updater can enter while that Git child
    is still changing refs/worktree bytes.
    """
    active: _ToolsetLifecycleLock | None = getattr(
        _toolset_lifecycle_local, "active", None,
    )
    if active is None:
        return ()
    return tuple(fd for _scope, _resource, fd, _path in active.resources)


def _mutating_git_run(argv: list[str], **kwargs):
    pass_fds = _active_lifecycle_pass_fds()
    if pass_fds and os.name == "posix":
        kwargs["pass_fds"] = pass_fds
    # (#118): git must never read the operator's stdin — it would eat
    # a caller's read-loop input, and a credential prompt would hang. Only
    # default it, so a caller that genuinely pipes data still wins.
    if "input" not in kwargs:
        kwargs.setdefault("stdin", subprocess.DEVNULL)
    return subprocess.run(argv, **kwargs)


def _run_git_pull(
    git_dir: Path,
    *,
    work_errors: list[str],
    branch: str | None = None,
) -> tuple[int | None, str]:
    """Run ``git -C <dir> pull``. Returns (rc, combined_output).
    rc is None on timeout / OSError; work_errors gets the explanation."""
    argv = ["git", "-C", str(git_dir), "pull"]
    if branch is not None:
        argv.extend(["origin", branch])
    try:
        proc = _mutating_git_run(
            argv,
            capture_output=True,
            text=True,
            timeout=GIT_PULL_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as e:
        work_errors.append(
            f"git pull timed out after {GIT_PULL_TIMEOUT_SECONDS}s"
        )
        return None, str(e)
    except OSError as e:
        work_errors.append(f"git pull failed to start: {e}")
        return None, str(e)
    return proc.returncode, (proc.stdout or "") + (proc.stderr or "")


def _run_git_fetch_sha(
    git_dir: Path, sha: str, branch: str | None,
) -> tuple[int | None, str]:
    """Fetch refs likely to contain ``sha`` before detached checkout.

    Direct managed env SHA pins are normally branch-tip or recent-main
    commits. Fetch the configured branch when available so rollout can pin a
    blessed main SHA without updating to a newer moving tip; fall back to a
    plain origin fetch for legacy envs without a configured branch. A pin on
    another branch is then fetched by its exact object ID from canonical
    origin; neither checkout nor the configured branch is moved (#677).
    """
    if not isinstance(sha, str) or _FULL_SHA_RE.fullmatch(sha) is None:
        return 2, "exact source fetch requires a full 40-hex commit SHA"
    args = ["git", "-C", str(git_dir), "fetch", "origin"]
    if branch:
        args.append(f"+refs/heads/{branch}:refs/remotes/origin/{branch}")
    try:
        proc = _mutating_git_run(
            args,
            capture_output=True,
            text=True,
            timeout=GIT_PULL_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as e:
        return None, str(e)
    except OSError as e:
        return None, str(e)
    output = (proc.stdout or "") + (proc.stderr or "")
    if proc.returncode != 0:
        return proc.returncode, output

    def check_commit() -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", "-C", str(git_dir), "cat-file", "-e", f"{sha}^{{commit}}"],
            capture_output=True, text=True, timeout=30, stdin=subprocess.DEVNULL,
        )

    try:
        probe = check_commit()
        if probe.returncode != 0:
            exact = _mutating_git_run(
                ["git", "-C", str(git_dir), "fetch", "--no-tags", "origin", sha],
                capture_output=True, text=True, timeout=GIT_PULL_TIMEOUT_SECONDS,
            )
            output += (exact.stdout or "") + (exact.stderr or "")
            if exact.returncode != 0:
                return exact.returncode, output
            probe = check_commit()
    except (subprocess.TimeoutExpired, OSError) as e:
        return None, output + str(e)
    return probe.returncode, output + (probe.stdout or "") + (probe.stderr or "")


def _run_git_fetch_tag(git_dir: Path, tag: str) -> tuple[int | None, str]:
    """Fetch exactly ``tag`` from origin.

    ``git fetch origin tag <tag>`` fails for missing tags and refuses to
    clobber an existing local tag that moved upstream. That is exactly the
    immutable-release contract ``vq admin update --tag`` needs.
    """
    try:
        proc = _mutating_git_run(
            ["git", "-C", str(git_dir), "fetch", "origin", "tag", tag],
            capture_output=True,
            text=True,
            timeout=GIT_PULL_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as e:
        return None, str(e)
    except OSError as e:
        return None, str(e)
    return proc.returncode, (proc.stdout or "") + (proc.stderr or "")


def _run_git_checkout_detached(
    git_dir: Path, ref: str,
) -> tuple[int | None, str]:
    try:
        proc = _mutating_git_run(
            ["git", "-C", str(git_dir), "checkout", "--detach", ref],
            capture_output=True,
            text=True,
            timeout=60,
        )
    except (subprocess.TimeoutExpired, OSError) as e:
        return None, str(e)
    return proc.returncode, (proc.stdout or "") + (proc.stderr or "")


def _run_git_resolve_commit(
    git_dir: Path, ref: str,
) -> tuple[int | None, str | None, str]:
    """Resolve one explicit ref to its full commit identity.

    Callers pass fully qualified tag refs (including ``^{commit}``) so a
    same-named branch cannot influence an immutable deployment selector.
    """
    try:
        proc = subprocess.run(
            ["git", "-C", str(git_dir), "rev-parse", "--verify", ref],
            capture_output=True,
            text=True,
            timeout=30,
            stdin=subprocess.DEVNULL,
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        return None, None, str(exc)
    output = (proc.stdout or "") + (proc.stderr or "")
    sha = proc.stdout.strip().lower() if proc.returncode == 0 else None
    if sha is not None and not _FULL_SHA_RE.fullmatch(sha):
        return 1, None, output
    return proc.returncode, sha, output


_MANAGED_GIT_SELECTOR_ENV = (
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_INDEX_FILE",
    "GIT_COMMON_DIR",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
)


def _guard_managed_git_environment() -> None:
    """Reject ambient selectors that can retarget ``git -C`` commands."""
    active = sorted(key for key in _MANAGED_GIT_SELECTOR_ENV if os.environ.get(key))
    if active:
        raise _ManagedGitAdmissionError(
            "cannot begin vq self-update: inherited Git repository selector "
            f"environment is active ({', '.join(active)}); unset it and retry. "
            "The serving daemon was not stopped"
        )


def _git_index_lock_path(git_dir: Path) -> Path:
    """Resolve the lock Git would use for this checkout's active index.

    Asking Git for the index path handles ordinary clones and linked worktrees
    without guessing whether ``.git`` is a directory or a gitfile. The helper
    also follows an explicitly configured ``GIT_INDEX_FILE``; managed-update
    admission separately rejects inherited repository selectors. Resolution
    is an admission proof: an unavailable or ambiguous answer must stop a
    managed self-update before its serving daemon is touched.
    """
    argv = [
        "git",
        "-C",
        str(git_dir),
        "rev-parse",
        "--path-format=absolute",
        "--git-path",
        "index",
    ]
    try:
        proc = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            timeout=30,
            stdin=subprocess.DEVNULL,
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        raise _ManagedGitAdmissionError(
            "cannot begin vq self-update: could not resolve the managed "
            f"checkout's Git index lock ({exc}); the serving daemon was not "
            "stopped"
        ) from exc
    raw = (proc.stdout or "").rstrip("\n")
    if (
        proc.returncode != 0
        or not raw
        or "\n" in raw
        or not Path(raw).is_absolute()
    ):
        detail = ((proc.stderr or proc.stdout or "").strip() or "no detail")
        detail = " ".join(detail.split())[:300]
        raise _ManagedGitAdmissionError(
            "cannot begin vq self-update: could not resolve the managed "
            f"checkout's Git index lock ({detail}); the serving daemon was "
            "not stopped"
        )
    return Path(raw + ".lock")


def _guard_git_index_unlocked(git_dir: Path) -> None:
    """Refuse a managed self-update while Git's index lock exists.

    vq never removes the lock or infers that its owner is dead.  A live Git
    writer and a stale lock have the same safe admission result: keep the
    currently serving daemon in place and require an operator to reconcile
    the checkout before retrying.
    """
    lock_path = _git_index_lock_path(git_dir)
    try:
        lock_path.lstat()
    except FileNotFoundError:
        return
    except OSError as exc:
        raise _ManagedGitAdmissionError(
            "cannot begin vq self-update: Git index-lock absence could not be "
            f"proved at {lock_path} ({exc}); the serving daemon was not stopped"
        ) from exc
    raise _ManagedGitAdmissionError(
        "cannot begin vq self-update: Git index lock is present at "
        f"{lock_path}; the serving daemon was not stopped. Confirm that no "
        "Git process is using the checkout, remove the lock only if it is "
        "stale, then retry"
    )


def _run_git_status_porcelain(git_dir: Path) -> tuple[int | None, str]:
    """Return ``git status --porcelain`` output.

    ``None`` return code means timeout/start failure. Used before automatic
    detached-HEAD repair so ``admin update`` only reattaches clean managed
    checkouts.
    """
    try:
        proc = subprocess.run(
            ["git", "-C", str(git_dir), "status", "--porcelain"],
            capture_output=True,
            text=True,
            timeout=30,
            stdin=subprocess.DEVNULL,
            # A cleanliness observation must not refresh the index or create
            # the very index.lock that managed-update admission is proving
            # absent. Git documents this switch for background/read-only
            # callers; the lifecycle's later mutating commands do not inherit
            # it.
            env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"},
        )
    except (subprocess.TimeoutExpired, OSError) as e:
        return None, str(e)
    return proc.returncode, proc.stdout or proc.stderr or ""


def _git_operation_in_progress(
    git_dir: Path,
) -> tuple[bool | None, str]:
    """Report merge/rebase/sequencer state without guessing worktree Git paths."""
    try:
        proc = subprocess.run(
            ["git", "-C", str(git_dir), "rev-parse", "--absolute-git-dir"],
            capture_output=True,
            text=True,
            timeout=30,
            stdin=subprocess.DEVNULL,
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        return None, str(exc)
    if proc.returncode != 0:
        return None, (proc.stdout or "") + (proc.stderr or "")
    raw = (proc.stdout or "").strip()
    if not raw or "\n" in raw:
        return None, "git returned an invalid operation-state directory"
    state_root = Path(raw)
    markers = (
        "MERGE_HEAD",
        "CHERRY_PICK_HEAD",
        "REVERT_HEAD",
        "rebase-merge",
        "rebase-apply",
        "sequencer",
        "BISECT_START",
        "BISECT_LOG",
    )
    active: list[str] = []
    for marker in markers:
        try:
            (state_root / marker).lstat()
        except FileNotFoundError:
            continue
        except OSError as exc:
            return None, f"could not inspect {marker}: {exc}"
        active.append(marker)
    return bool(active), ", ".join(active)


def _run_git_fetch_origin(git_dir: Path) -> tuple[int | None, str]:
    try:
        proc = _mutating_git_run(
            ["git", "-C", str(git_dir), "fetch", "origin"],
            capture_output=True,
            text=True,
            timeout=GIT_PULL_TIMEOUT_SECONDS,
        )
    except (subprocess.TimeoutExpired, OSError) as e:
        return None, str(e)
    return proc.returncode, (proc.stdout or "") + (proc.stderr or "")


def _run_git_checkout_branch(
    git_dir: Path,
    branch: str,
    *,
    force_to_origin: bool = False,
    set_upstream: bool = True,
    local_branch_exists: bool | None = None,
) -> tuple[int | None, str]:
    """Check out ``branch`` and set its upstream to ``origin/branch``.

    ``force_to_origin=True`` is for the destructive ``reset-branch`` command:
    it recreates the local branch at ``origin/<branch>`` after the hard reset.
    ``False`` is for ``admin update`` detached-HEAD recovery: it first tries to
    check out an existing local branch and only creates it from origin when the
    branch is absent.
    """
    chunks: list[str] = []
    if force_to_origin:
        commands = [
            ["git", "-C", str(git_dir), "checkout", "-B", branch, f"origin/{branch}"],
        ]
    else:
        fallback = [
            "git", "-C", str(git_dir), "checkout",
        ]
        if not set_upstream:
            fallback.append("--no-track")
        fallback.extend(["-B", branch, f"origin/{branch}"])
        if local_branch_exists is True:
            commands = [["git", "-C", str(git_dir), "checkout", branch]]
        elif local_branch_exists is False:
            commands = [fallback]
        else:
            commands = [
                ["git", "-C", str(git_dir), "checkout", branch],
                fallback,
            ]

    checkout_rc: int | None = None
    for index, argv in enumerate(commands):
        try:
            proc = _mutating_git_run(
                argv,
                capture_output=True,
                text=True,
                timeout=60,
            )
            checkout_rc = proc.returncode
            chunks.append(
                "$ " + " ".join(argv[:3] + argv[3:]) + "\n"
                + (proc.stdout or "")
                + (proc.stderr or "")
            )
        except (subprocess.TimeoutExpired, OSError) as e:
            checkout_rc = None
            chunks.append("$ " + " ".join(argv) + f"\n(failed: {e})\n")
        if checkout_rc == 0:
            break
        if force_to_origin or index == len(commands) - 1:
            return checkout_rc, "".join(chunks)

    if not set_upstream:
        return checkout_rc, "".join(chunks)
    upstream_rc, upstream_output = _run_git_set_upstream(git_dir, branch)
    chunks.append(upstream_output)
    return upstream_rc, "".join(chunks)


def _run_git_set_upstream(
    git_dir: Path, branch: str,
) -> tuple[int | None, str]:
    """Bind an already-checked-out local branch to ``origin/<branch>``."""
    argv = [
        "git", "-C", str(git_dir),
        "branch", "--set-upstream-to", f"origin/{branch}", branch,
    ]
    try:
        upstream_proc = _mutating_git_run(
            argv,
            capture_output=True,
            text=True,
            timeout=30,
        )
        upstream_rc: int | None = upstream_proc.returncode
        output = (
            f"$ git branch --set-upstream-to origin/{branch} {branch}\n"
            + (upstream_proc.stdout or "")
            + (upstream_proc.stderr or "")
        )
    except (subprocess.TimeoutExpired, OSError) as e:
        upstream_rc = None
        output = (
            f"$ git branch --set-upstream-to origin/{branch} {branch}\n"
            f"(failed: {e})\n"
        )
    return upstream_rc, output


@dataclass(frozen=True)
class _DetachedReattachResult:
    """Pre-mutation local-ref state for a performed detached reattach."""

    output: str
    performed: bool = False
    branch_existed: bool = False
    branch_sha: str | None = None


def _capture_local_branch_tip(
    git_dir: Path, branch: str,
) -> tuple[int | None, str | None, str]:
    """Return whether ``refs/heads/<branch>`` exists and its exact SHA.

    rc=1 with no SHA is the ordinary absent-ref result. ``None`` denotes an
    execution failure or malformed successful output.
    """
    ref = f"refs/heads/{branch}"
    argv = [
        "git", "-C", str(git_dir),
        "rev-parse", "--verify", "--quiet", ref,
    ]
    try:
        proc = _mutating_git_run(
            argv,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        return None, None, str(exc)
    output = (proc.stdout or "") + (proc.stderr or "")
    if proc.returncode != 0:
        return proc.returncode, None, output
    sha = (proc.stdout or "").strip().lower()
    if not _FULL_SHA_RE.fullmatch(sha):
        return None, None, output + "malformed local branch SHA\n"
    return 0, sha, output


def _reattach_clean_detached_checkout_for_update(
    git_dir: Path,
    branch: str,
    *,
    work_errors: list[str],
    capture_rollback_ref: bool = False,
) -> _DetachedReattachResult:
    """Reattach a clean detached checkout to ``branch`` before pull.

    Managed release checkouts intentionally visit immutable tags, but the next
    ``git pull`` must run from the configured branch. This helper is deliberately
    conservative: it only does work when HEAD is detached, refuses dirty trees,
    fetches origin, then checks out/creates the configured local branch.
    """
    rc, current = _run_git_branch_check(git_dir)
    if rc != 0 or current != "HEAD":
        return _DetachedReattachResult(output="")

    detached_sha = _git_head_sha(git_dir)
    chunks = [
        "$ git rev-parse --abbrev-ref HEAD\nHEAD\n",
    ]
    branch_existed = False
    branch_sha: str | None = None
    if capture_rollback_ref:
        branch_rc, branch_sha, branch_output = _capture_local_branch_tip(
            git_dir, branch
        )
        chunks.append(
            f"$ git rev-parse --verify --quiet refs/heads/{branch}\n"
            + branch_output
        )
        if branch_rc not in (0, 1):
            work_errors.append(
                "detached checkout repair failed: could not capture the exact "
                f"pre-update local branch ref {branch!r}"
            )
            return _DetachedReattachResult(output="".join(chunks))
        branch_existed = branch_rc == 0
        operation_active, operation_detail = _git_operation_in_progress(git_dir)
        if operation_active is None:
            work_errors.append(
                "detached checkout repair failed: could not inspect Git "
                f"operation state: {operation_detail}"
            )
            return _DetachedReattachResult(output="".join(chunks))
        if operation_active:
            work_errors.append(
                "detached checkout repair refused: a pre-existing Git "
                f"operation is in progress ({operation_detail})"
            )
            return _DetachedReattachResult(output="".join(chunks))
    status_rc, status = _run_git_status_porcelain(git_dir)
    chunks.append("$ git status --porcelain\n" + status)
    if status_rc != 0:
        work_errors.append(
            "detached checkout repair failed: could not inspect git status"
        )
        return _DetachedReattachResult(output="".join(chunks))
    if status.strip():
        work_errors.append(
            "detached checkout repair refused: working tree is dirty"
        )
        return _DetachedReattachResult(output="".join(chunks))

    fetch_rc, fetch_output = _run_git_fetch_origin(git_dir)
    chunks.append("$ git fetch origin\n" + fetch_output)
    if fetch_rc != 0:
        work_errors.append(
            f"detached checkout repair failed: git fetch origin rc={fetch_rc}"
        )
        return _DetachedReattachResult(output="".join(chunks))

    checkout_rc, checkout_output = _run_git_checkout_branch(
        git_dir,
        branch,
        force_to_origin=False,
        set_upstream=not capture_rollback_ref,
        local_branch_exists=(
            branch_existed if capture_rollback_ref else None
        ),
    )
    chunks.append(checkout_output)
    if checkout_rc != 0:
        work_errors.append(
            "detached checkout repair failed: could not check out "
            f"configured branch {branch!r}"
        )
        mutation_observed = False
        if capture_rollback_ref:
            # A post-checkout hook may return non-zero after Git has already
            # switched HEAD and the worktree. Prove the pre-checkout state is
            # unchanged before allowing the caller to discard its snapshot;
            # uncertainty is treated as mutation and triggers exact rollback.
            current_sha, current_branch = _capture_checkout_state(git_dir)
            branch_rc, current_branch_sha, branch_output = (
                _capture_local_branch_tip(git_dir, branch)
            )
            chunks.append(
                f"$ git rev-parse --verify --quiet refs/heads/{branch}\n"
                + branch_output
            )
            branch_unchanged = (
                branch_rc == 0 and current_branch_sha == branch_sha
                if branch_existed
                else branch_rc == 1 and current_branch_sha is None
            )
            mutation_observed = not (
                detached_sha is not None
                and current_sha == detached_sha
                and current_branch is None
                and branch_unchanged
            )
        return _DetachedReattachResult(
            output="".join(chunks),
            performed=mutation_observed,
            branch_existed=branch_existed,
            branch_sha=branch_sha,
        )
    return _DetachedReattachResult(
        output="".join(chunks),
        performed=True,
        branch_existed=branch_existed,
        branch_sha=branch_sha,
    )


# ----------------------------------------------------------------------
# v0.12.x fix 3: atomic build (snapshot/restore + import gate)
#
# vibe-qc's editable venv lives INSIDE git_dir and the native ``.so`` is
# built in-place, so a literal directory swap would relocate the venv and
# break its baked-in absolute paths. The atomic guarantee is instead
# achieved at artifact granularity: snapshot {checkout state, native-source
# freshness evidence, the interpreter-reported core and package ``.so`` files}
# before mutation, and on a failed build (or a failed post-build ``import``
# probe) restore that exact state. Managed checkout-local vibe-qc venvs receive
# the import contract implicitly; other programs opt in with ``import_check``.
# ----------------------------------------------------------------------


def _git_head_sha(git_dir: Path) -> str | None:
    """Return the full HEAD SHA, or None on any failure. The rollback
    target captured before the pull."""
    try:
        proc = subprocess.run(
            ["git", "-C", str(git_dir), "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=30,
            stdin=subprocess.DEVNULL,
        )
    except (subprocess.TimeoutExpired, OSError):
        return None
    if proc.returncode != 0:
        return None
    sha = proc.stdout.strip()
    return sha or None


def _capture_checkout_state(git_dir: Path) -> tuple[str | None, str | None]:
    """Capture exact HEAD and symbolic attachment before selector mutation."""
    sha = _git_head_sha(git_dir)
    if sha is None or not _FULL_SHA_RE.fullmatch(sha.lower()):
        return None, None
    branch_rc, branch = _run_git_branch_check(git_dir)
    if branch_rc != 0 or not isinstance(branch, str):
        return None, None
    return sha.lower(), None if branch == "HEAD" else branch


def _restore_checkout_state(
    git_dir: Path,
    *,
    sha: str,
    branch: str | None,
    clean_untracked: bool = True,
) -> tuple[int | None, str]:
    """Restore a proven-clean baseline's exact commit and attachment.

    Immutable update admission proves the baseline clean before any selector or
    build mutation.  Rollback first detaches without touching the worktree,
    then resets the index/tree to the captured commit.  ``reset --hard``
    removes untracked paths that obstruct files tracked by that commit.
    Immutable selector admission additionally sets ``clean_untracked`` so
    build output is removed under the restored baseline's ignore rules.
    Ordinary atomic rollback leaves unrelated untracked host state untouched.
    """
    chunks: list[str] = []
    # ``checkout --detach`` normally updates only HEAD because the selector
    # transaction already has a clean index relative to its current commit.
    # Unlike deleting the HEAD symbolic ref directly, it is valid in both
    # attached and already-detached states and preserves a well-formed HEAD.
    before_detach_sha = _git_head_sha(git_dir)
    detach_argv = ["git", "-C", str(git_dir), "checkout", "--detach"]
    try:
        detach = _mutating_git_run(
            detach_argv,
            capture_output=True,
            text=True,
            timeout=30,
        )
        detach_rc: int | None = detach.returncode
        chunks.append(
            "$ " + " ".join(detach_argv) + "\n"
            + (detach.stdout or "")
            + (detach.stderr or "")
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        detach_rc = None
        chunks.append(
            "$ " + " ".join(detach_argv) + f"\n(failed: {exc})\n"
        )
    if detach_rc != 0:
        detached_sha, detached_branch = _capture_checkout_state(git_dir)
        if (
            before_detach_sha is not None
            and detached_sha == before_detach_sha
            and detached_branch is None
        ):
            chunks.append(
                "checkout returned non-zero after verified HEAD detachment\n"
            )
            detach_rc = 0
        else:
            return detach_rc, "".join(chunks)
    reset_rc, reset_out = _git_reset_hard(git_dir, sha)
    chunks.append(reset_out)
    if reset_rc != 0:
        return reset_rc, "".join(chunks)
    clean_rc: int | None = 0
    if clean_untracked:
        clean_rc, clean_out = _git_clean_untracked(git_dir)
        chunks.append(clean_out)
    if clean_rc != 0 or branch is None:
        return clean_rc, "".join(chunks)
    # Rollback restores local state, not deployment policy.  In particular,
    # it must not call ``_run_git_checkout_branch``: that update-oriented
    # helper rewrites the branch upstream to ``origin/<branch>`` and fails for
    # a perfectly valid local-only branch.  This local-only ``checkout -B``
    # also restores the branch ref itself if the failed updater moved it, while
    # retaining existing branch.<name> remote/merge configuration.
    attach_argv = [
        "git", "-C", str(git_dir), "checkout", "-B", branch, sha,
    ]
    try:
        attach = _mutating_git_run(
            attach_argv,
            capture_output=True,
            text=True,
            timeout=30,
        )
        attach_rc: int | None = attach.returncode
        chunks.append(
            "$ " + " ".join(attach_argv) + "\n"
            + (attach.stdout or "")
            + (attach.stderr or "")
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        attach_rc = None
        chunks.append(
            "$ " + " ".join(attach_argv) + f"\n(failed: {exc})\n"
        )
    if attach_rc != 0 and _checkout_state_matches(
        git_dir, sha=sha, branch=branch
    ):
        chunks.append(
            "checkout returned non-zero after verified branch attachment\n"
        )
        attach_rc = 0
    return attach_rc, "".join(chunks)


def _git_clean_untracked(git_dir: Path) -> tuple[int | None, str]:
    """Remove paths created after an immutable update's clean preflight."""
    try:
        proc = _mutating_git_run(
            ["git", "-C", str(git_dir), "clean", "-fd"],
            capture_output=True,
            text=True,
            timeout=120,
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        return None, str(exc)
    return proc.returncode, (proc.stdout or "") + (proc.stderr or "")


def _checkout_state_matches(
    git_dir: Path, *, sha: str, branch: str | None,
) -> bool:
    current_sha, current_branch = _capture_checkout_state(git_dir)
    return current_sha == sha and current_branch == branch


def _git_reset_hard(git_dir: Path, sha: str) -> tuple[int | None, str]:
    """``git reset --hard <sha>`` to restore the pre-update committed
    tree. Returns (rc, combined_output); rc None on timeout/OSError."""
    try:
        proc = _mutating_git_run(
            ["git", "-C", str(git_dir), "reset", "--hard", sha],
            capture_output=True, text=True, timeout=120,
        )
    except (subprocess.TimeoutExpired, OSError) as e:
        return None, str(e)
    return proc.returncode, (proc.stdout or "") + (proc.stderr or "")


def _restore_detached_atomic_checkout(
    git_dir: Path,
    *,
    sha: str,
    branch: str,
    reattach: _DetachedReattachResult,
) -> tuple[int | None, str]:
    """Undo a temporary detached-HEAD reattach without cleaning host files."""
    # A failed pull can leave an unmerged index, and Git refuses even a
    # no-worktree detached checkout while conflicts exist. Admission proved the
    # baseline clean, so tracked/index changes are transaction-owned: clear
    # them at the current HEAD before detaching. Unrelated untracked files stay.
    clear_rc, clear_output = _git_reset_hard(git_dir, "HEAD")
    if clear_rc != 0:
        return clear_rc, clear_output
    restore_rc, output = _restore_checkout_state(
        git_dir,
        sha=sha,
        branch=None,
        clean_untracked=False,
    )
    output = clear_output + output
    if restore_rc != 0:
        return restore_rc, output
    ref = f"refs/heads/{branch}"
    if reattach.branch_existed:
        if reattach.branch_sha is None:
            return 1, output + "missing captured local branch SHA\n"
        argv = [
            "git", "-C", str(git_dir), "update-ref", ref,
            reattach.branch_sha,
        ]
    else:
        argv = ["git", "-C", str(git_dir), "update-ref", "-d", ref]
    try:
        proc = _mutating_git_run(
            argv,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        return None, output + "$ " + " ".join(argv) + f"\n(failed: {exc})\n"
    output += (
        "$ " + " ".join(argv) + "\n"
        + (proc.stdout or "")
        + (proc.stderr or "")
    )
    if proc.returncode != 0:
        return proc.returncode, output
    head_sha, head_branch = _capture_checkout_state(git_dir)
    branch_rc, branch_sha, branch_output = _capture_local_branch_tip(
        git_dir, branch
    )
    output += branch_output
    branch_matches = (
        branch_rc == 0 and branch_sha == reattach.branch_sha
        if reattach.branch_existed
        else branch_rc == 1 and branch_sha is None
    )
    if head_sha != sha or head_branch is not None or not branch_matches:
        return 1, output + "detached atomic rollback verification failed\n"
    return 0, output


def _abort_in_progress_pull(git_dir: Path) -> str:
    """Clear merge/rebase transaction metadata before exact rollback."""
    chunks: list[str] = []
    for operation in (("rebase", "--abort"), ("merge", "--abort")):
        argv = ["git", "-C", str(git_dir), *operation]
        try:
            proc = _mutating_git_run(
                argv,
                capture_output=True,
                text=True,
                timeout=60,
            )
        except (subprocess.TimeoutExpired, OSError) as exc:
            chunks.append("$ " + " ".join(argv) + f"\n(failed: {exc})\n")
            continue
        # "No <operation> in progress" is the ordinary result for one of the
        # two mutually exclusive paths. Record only an abort that actually ran.
        if proc.returncode == 0:
            chunks.append(
                "$ " + " ".join(argv) + "\n"
                + (proc.stdout or "")
                + (proc.stderr or "")
            )
    return "".join(chunks)


def _native_pkg_dir(git_dir: Path, import_check: str) -> Path:
    """The directory holding the env's compiled extension modules. Uses
    vibe-qc's ``<git_dir>/python/<module>/`` layout (the coupling fix 3
    deliberately accepts to locate the .so to snapshot)."""
    return Path(git_dir) / "python" / import_check.split(".", 1)[0]


_NATIVE_RUNTIME_TREE_GLOB = "third_party/*/install"
"""Vendored native libraries the compiled core links against.

The core is only half of a native runtime. On 2026-09-11/12 a rollback that
restored ``_vibeqc_core`` alone left developer-host, compute-b and workstation importing a
core whose ``libint2`` and ``libecpint`` the failed build had already wiped
(``update.sh`` removes every ``third_party/*/install`` before a native-deps
rebuild), so "restored 1 native artifact(s)" described a lane that no longer
imported (#44).
"""

_NativeTreeManifest = tuple[tuple[str, int, int, int, int], ...]


@dataclass(frozen=True)
class _NativeArtifactSnapshot:
    """Recoverable copies of native files and their exact live targets."""

    directory: Path
    entries: tuple[tuple[Path, Path], ...]
    source_mtimes: tuple[tuple[Path, int], ...]
    vibeqc_candidate_roots: tuple[
        tuple[Path, tuple[tuple[Path, str | None], ...]], ...
    ]
    runtime_version: str | None = None
    runtime_version_captured: bool = False
    native_runtime_trees: tuple[tuple[Path, Path, _NativeTreeManifest], ...] = ()
    """(live tree, snapshot copy, manifest of the live tree when copied)."""


def _native_tree_manifest(root: Path) -> _NativeTreeManifest:
    """Every entry under ``root`` as (path, mode, size, mtime, inode).

    Links are not followed. A rebuild that wiped and reinstalled a library
    changes inodes even when sizes happen to match, so an equal manifest means
    the tree was not touched and a rollback can leave it alone.
    """
    entries: list[tuple[str, int, int, int, int]] = []
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        for name in (*dirnames, *filenames):
            path = Path(dirpath) / name
            st = path.lstat()
            entries.append((
                str(path.relative_to(root)),
                st.st_mode,
                st.st_size,
                st.st_mtime_ns,
                st.st_ino,
            ))
    return tuple(sorted(entries))


def _snapshot_native_runtime_trees(
    git_dir: Path, backup: Path,
) -> tuple[tuple[Path, Path, _NativeTreeManifest], ...]:
    trees: list[tuple[Path, Path, _NativeTreeManifest]] = []
    for live in sorted(Path(git_dir).glob(_NATIVE_RUNTIME_TREE_GLOB)):
        if live.is_symlink() or not live.is_dir():
            continue
        saved = backup / "native-runtime" / live.relative_to(git_dir)
        manifest = _native_tree_manifest(live)
        shutil.copytree(live, saved, symlinks=True)
        trees.append((live.absolute(), saved, manifest))
    return tuple(trees)


def _restore_native_runtime_trees(
    snapshot: _NativeArtifactSnapshot | None,
) -> int:
    """Put back every vendored native tree the transaction changed.

    Each tree is staged beside its live location and swapped in by rename, so
    a restore that fails part-way raises rather than leaving a mixture of the
    snapshot and the failed build under one ``install`` directory.
    """
    if snapshot is None or not snapshot.directory.is_dir():
        return 0
    restored = 0
    for live, saved, manifest in snapshot.native_runtime_trees:
        if (
            live.is_dir()
            and not live.is_symlink()
            and _native_tree_manifest(live) == manifest
        ):
            continue
        live.parent.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=".vq-restore-", dir=live.parent))
        try:
            staged = staging / live.name
            shutil.copytree(saved, staged, symlinks=True)
            if live.is_symlink() or live.is_file():
                live.unlink()
            elif live.exists():
                live.rename(staging / f"{live.name}.discarded")
            staged.rename(live)
        finally:
            shutil.rmtree(staging, ignore_errors=True)
        restored += 1
    return restored


def _vibeqc_package_core_candidates(package_root: Path) -> tuple[Path, ...]:
    """Return loader-visible source-tree core paths without resolving links."""
    candidates: list[Path] = []
    for candidate in package_root.iterdir():
        if not (
            candidate.name.startswith("_vibeqc_core")
            and candidate.name.endswith(".so")
        ):
            continue
        try:
            candidate.lstat()
        except FileNotFoundError:
            continue
        candidates.append(candidate.absolute())
    return tuple(sorted(candidates))


def _capture_vibeqc_candidate_root(
    root: Path,
) -> tuple[Path, tuple[tuple[Path, str | None], ...]]:
    states: list[tuple[Path, str | None]] = []
    for candidate in _vibeqc_package_core_candidates(root):
        link_target = os.readlink(candidate) if candidate.is_symlink() else None
        states.append((candidate, link_target))
    return root, tuple(states)


def _snapshot_native_artifacts(
    git_dir: Path,
    import_check: str | None,
    *,
    runtime_native_path: Path | None = None,
    source_mtimes: tuple[tuple[Path, int], ...] = (),
    track_vibeqc_candidates: bool = False,
    runtime_version: str | None = None,
    capture_runtime_version: bool = False,
) -> _NativeArtifactSnapshot | None:
    """Copy the package's ``*.so`` files to a temp backup dir (preserving
    their exact targets) so a failed build can restore the prior ABI.

    Editable builds can install the live extension in venv ``site-packages``
    rather than under ``<git_dir>/python``.  ``runtime_native_path`` is the
    interpreter-reported core location and is snapshotted alongside any
    source-tree artifacts. ``source_mtimes`` must be captured before any Git
    mutation so rollback restores freshness evidence for the serving commit.
    A structural managed vibe-qc transaction also records the source-package
    core inventory so rollback can remove only loader candidates created by
    the transaction. Returns ``None`` when there is nothing to copy.
    """
    if not import_check:
        return None
    pkg = _native_pkg_dir(git_dir, import_check)
    candidate_roots: list[Path] = []
    if track_vibeqc_candidates:
        candidate_roots.append(pkg.absolute())
        if runtime_native_path is not None:
            runtime_root = runtime_native_path.absolute().parent
            if runtime_root not in candidate_roots:
                candidate_roots.append(runtime_root)
    vibeqc_candidate_roots = tuple(
        _capture_vibeqc_candidate_root(root)
        for root in candidate_roots
    )
    candidates = list(pkg.rglob("*.so")) if pkg.is_dir() else []
    for _root, baseline_states in vibeqc_candidate_roots:
        candidates.extend(path for path, _link_target in baseline_states)
    if runtime_native_path is not None and runtime_native_path.is_file():
        candidates.append(runtime_native_path)
    artifacts: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        try:
            resolved = candidate.resolve(strict=True)
        except OSError:
            continue
        if resolved in seen or not resolved.is_file():
            continue
        seen.add(resolved)
        artifacts.append(resolved)
    has_runtime_trees = any(
        tree.is_dir() and not tree.is_symlink()
        for tree in Path(git_dir).glob(_NATIVE_RUNTIME_TREE_GLOB)
    )
    if not artifacts and not track_vibeqc_candidates and not has_runtime_trees:
        return None
    backup = Path(tempfile.mkdtemp(prefix="vq-build-snap-"))
    entries: list[tuple[Path, Path]] = []
    try:
        for index, artifact in enumerate(sorted(artifacts)):
            copy = backup / f"{index:04d}-{artifact.name}"
            shutil.copy2(artifact, copy)
            entries.append((copy, artifact))
        # The vendored libraries the core links, captured with it: a core
        # restored without them is not the runtime that was serving (#44).
        native_runtime_trees = _snapshot_native_runtime_trees(git_dir, backup)
    except OSError:
        shutil.rmtree(backup, ignore_errors=True)
        raise
    return _NativeArtifactSnapshot(
        backup,
        tuple(entries),
        source_mtimes,
        vibeqc_candidate_roots,
        runtime_version,
        capture_runtime_version,
        native_runtime_trees,
    )


def _remove_transaction_created_vibeqc_cores(
    snapshot: _NativeArtifactSnapshot | None,
) -> tuple[int, tuple[str, ...]]:
    """Remove only new source-package cores after Git rollback succeeds."""
    if snapshot is None or not snapshot.vibeqc_candidate_roots:
        return 0, ()
    removed = 0
    errors: list[str] = []
    for root, baseline_states in snapshot.vibeqc_candidate_roots:
        try:
            current = _vibeqc_package_core_candidates(root)
        except OSError as exc:
            errors.append(f"could not inspect rolled-back core inventory: {exc}")
            continue
        baseline = {path for path, _link_target in baseline_states}
        for candidate in current:
            if candidate in baseline:
                continue
            try:
                candidate.unlink()
            except OSError as exc:
                errors.append(
                    f"could not remove transaction-created {candidate}: {exc}"
                )
            else:
                removed += 1
        for candidate, link_target in baseline_states:
            try:
                current_is_link = candidate.is_symlink()
                if link_target is not None:
                    if current_is_link and os.readlink(candidate) == link_target:
                        continue
                    if current_is_link or candidate.exists():
                        if candidate.is_dir() and not current_is_link:
                            raise OSError("loader candidate became a directory")
                        candidate.unlink()
                    os.symlink(link_target, candidate)
                elif current_is_link:
                    candidate.unlink()
                elif candidate.exists() and candidate.is_dir():
                    raise OSError("loader candidate became a directory")
            except OSError as exc:
                errors.append(
                    f"could not restore loader identity for {candidate}: {exc}"
                )
    return removed, tuple(errors)


def _restore_native_artifacts(
    snapshot: _NativeArtifactSnapshot | None,
    *,
    restore_source_mtimes: bool = False,
) -> int:
    """Restore native files and, after Git rollback, source mtimes."""
    if snapshot is None or not snapshot.directory.is_dir():
        return 0
    n = 0
    for source, target in snapshot.entries:
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        n += 1
    if restore_source_mtimes:
        mtime_errors: list[str] = []
        for source, mtime_ns in snapshot.source_mtimes:
            try:
                stat_result = source.stat()
                os.utime(
                    source,
                    ns=(stat_result.st_atime_ns, mtime_ns),
                    follow_symlinks=False,
                )
            except OSError as exc:
                mtime_errors.append(f"{source}: {exc}")
        if mtime_errors:
            raise OSError(
                "could not restore native-source mtimes: "
                + "; ".join(mtime_errors)
            )
    return n


def _run_import_check(
    python: str,
    module: str,
    *,
    symbols: list[str] | None = None,
    source_root: Path | None = None,
    native_within: Path | None = None,
    expected_version: str | None = None,
    check_version: bool = False,
) -> tuple[int, str]:
    """Run ``<python> -c "import <module>"`` and return (rc, output). The
    ABI gate: rc=0 means the freshly built extension actually imports;
    non-zero carries the ImportError tail. Mirrors
    :meth:`ImportProgram.availability`'s probe."""
    if module == "vibeqc" and source_root is not None:
        rc, output, version, _module_path, native_core_path = (
            config.run_import_runtime_identity_probe(
                python,
                module,
                symbols=symbols,
                timeout=120,
                source_root=source_root,
            )
        )
        if rc == 0 and check_version and version != expected_version:
            return 1, (
                "post-rollback runtime version mismatch: expected "
                f"{expected_version or '(not reported)'}, got "
                f"{version or '(not reported)'}"
            )
        if rc == 0 and native_within is not None:
            try:
                required_root = native_within.resolve(strict=True)
                native_core = (
                    Path(native_core_path).resolve(strict=True)
                    if native_core_path is not None
                    else None
                )
            except OSError as exc:
                return 1, f"immutable runtime core could not be resolved: {exc}"
            if native_core is None or not native_core.is_relative_to(required_root):
                return 1, (
                    "immutable runtime core is outside the slot generation: "
                    f"{native_core_path or '(not reported)'}"
                )
            relative = native_core.relative_to(required_root)
            excluded = {
                ".git",
                "__pycache__",
                runtime_slots.IMMUTABLE_RUNTIME_MARKER,
            }
            if (
                not relative.parts
                or any(part in excluded for part in relative.parts)
                or (
                    ".venv" in relative.parts
                    and relative.parts[0] != ".venv"
                )
                or relative.suffix in {".pyc", ".pyo"}
            ):
                return 1, (
                    "immutable runtime core is outside the slot's hashed "
                    f"content: {native_core_path or '(not reported)'}"
                )
        return rc, output
    return config.run_import_probe(python, module, symbols=symbols, timeout=120)


def _rollback_atomic_state(
    result: UpdateResult,
    prog: config.VenvProgram,
    git_dir: Path,
    so_backup: _NativeArtifactSnapshot | None,
    *,
    import_check: str,
    source_root: Path | None,
    reattach: _DetachedReattachResult,
    cause: str,
    abort_pull_state: bool = False,
) -> None:
    """Restore the pre-update checkout/core pair after a transaction failure."""
    if result.pre_update_sha is None or result.rollback_summary:
        return
    abort_output = _abort_in_progress_pull(git_dir) if abort_pull_state else ""
    if reattach.performed and prog.branch is not None:
        reset_rc, reset_out = _restore_detached_atomic_checkout(
            git_dir,
            sha=result.pre_update_sha,
            branch=prog.branch,
            reattach=reattach,
        )
        checkout_action = "restored detached checkout"
    else:
        reset_rc, reset_out = _git_reset_hard(git_dir, result.pre_update_sha)
        checkout_action = "git reset --hard"
    reset_out = abort_output + reset_out
    removed = 0
    restored = 0
    restored_trees = 0
    native_errors: tuple[str, ...] = ()
    if reset_rc == 0:
        removed, native_errors = _remove_transaction_created_vibeqc_cores(
            so_backup
        )
        try:
            restored = _restore_native_artifacts(
                so_backup,
                restore_source_mtimes=True,
            )
        except OSError as exc:
            native_errors += (f"could not restore native artifacts: {exc}",)
        try:
            restored_trees = _restore_native_runtime_trees(so_backup)
        except OSError as exc:
            native_errors += (
                f"could not restore native runtime trees: {exc}",
            )
    rollback_ok = reset_rc == 0 and not native_errors
    result.rolled_back = rollback_ok
    trail = [
        f"{checkout_action} {result.pre_update_sha[:12]} rc={reset_rc}",
        f"restored {restored} native artifact(s)",
    ]
    if restored_trees:
        trail.append(f"restored {restored_trees} native runtime tree(s)")
    if removed:
        trail.append(f"removed {removed} transaction-created core(s)")
    if native_errors:
        trail.append("native restore failed: " + "; ".join(native_errors))
    if reset_rc != 0 and reset_out.strip():
        trail.append("checkout restore failed")
    if rollback_ok:
        rb_rc, rb_output = _run_import_check(
            prog.python,
            import_check,
            symbols=prog.import_symbols,
            source_root=source_root,
            expected_version=(
                so_backup.runtime_version if so_backup is not None else None
            ),
            check_version=bool(
                so_backup is not None
                and so_backup.runtime_version_captured
            ),
        )
        trail.append(
            "post-rollback import OK" if rb_rc == 0
            else f"post-rollback import STILL FAILING rc={rb_rc}"
        )
        if (
            rb_rc != 0
            and so_backup is not None
            and so_backup.runtime_version_captured
        ):
            if rb_output.strip():
                trail.append(rb_output.strip().splitlines()[-1])
            rollback_ok = False
            result.rolled_back = False
    elif reset_rc != 0:
        trail.append("post-rollback import skipped: checkout restore failed")
    else:
        trail.append("post-rollback import skipped: native restore failed")
    result.rollback_summary = "; ".join(trail)
    result.work_errors.append(
        f"atomic build rollback{' FAILED' if not rollback_ok else ''}: "
        f"{cause} — reverted "
        f"{result.pre_update_sha[:12]} ({result.rollback_summary})"
    )
    log.error(
        "atomic build: env=%s %s; rolled back to %s (%s)",
        result.env,
        cause,
        result.pre_update_sha[:12],
        result.rollback_summary,
    )


def _finalize_atomic_build(
    result: UpdateResult, prog: config.VenvProgram, git_dir: Path,
    so_backup: _NativeArtifactSnapshot | None,
    *,
    import_check: str | None,
    source_root: Path | None,
    reattach: _DetachedReattachResult,
    native_within: Path | None = None,
) -> None:
    """v0.12.x fix 3: post-build ABI gate + atomic rollback.

    Mutates ``result`` in place. No-op unless the env is armed (an
    update_script plus a configured or managed-layout import check), the pull
    advanced the tree, and a build was actually attempted (a build SKIPPED by
    the branch/tag gate is left to those gates' own failure handling). When
    the build failed — or it returned rc=0 but the env does not import — the
    checkout and any temporary detached-HEAD reattach are restored to the
    pre-update state together with the snapshotted core.
    """
    if not (prog.update_script and import_check):
        return  # atomic machinery disarmed for this env
    if result.pre_update_sha is None:
        return
    if result.git_pull_rc != 0:
        if source_root is not None and import_check == "vibeqc":
            _rollback_atomic_state(
                result,
                prog,
                git_dir,
                so_backup,
                import_check=import_check,
                source_root=source_root,
                reattach=reattach,
                cause="git pull failed after atomic update admission",
                abort_pull_state=True,
            )
        return
    build_ran = result.update_script_rc is not None
    build_stalled = any(
        e.startswith("update_script ") for e in result.work_errors
    )
    if not (build_ran or build_stalled):
        return  # build skipped by the branch/tag gate — not our case
    tag_drifted = (
        result.expected_tag is not None
        and result.update_script_rc == 0
        and not result.tag_matches
    )
    build_failed = (
        (build_ran and result.update_script_rc != 0)
        or build_stalled
        or tag_drifted
    )

    # Post-build ABI probe — an rc=0 update_script can still leave a
    # broken extension (the build-host skew). Always probe when armed + built.
    check_kwargs: dict[str, object] = {
        "symbols": prog.import_symbols,
        "source_root": source_root,
    }
    if native_within is not None:
        check_kwargs["native_within"] = native_within
    result.import_check_rc, result.import_check_output = _run_import_check(
        prog.python,
        import_check,
        **check_kwargs,
    )
    import_failed = (
        result.import_check_rc is not None and result.import_check_rc != 0
    )

    if not build_failed and not import_failed:
        return  # consistent + importable → keep the freshly built env

    if tag_drifted:
        cause = "post-build tag verification failed"
    else:
        cause = (
            "update_script failed"
            if build_failed
            else "post-build import probe failed"
        )
    _rollback_atomic_state(
        result,
        prog,
        git_dir,
        so_backup,
        import_check=import_check,
        source_root=source_root,
        reattach=reattach,
        cause=cause,
        abort_pull_state=result.git_pull_rc != 0,
    )


def _finalize_detached_reattach(
    result: UpdateResult,
    prog: config.VenvProgram,
    git_dir: Path,
    so_backup: _NativeArtifactSnapshot | None,
    *,
    import_check: str | None,
    source_root: Path | None,
    reattach: _DetachedReattachResult,
) -> None:
    """Commit or undo the temporary branch attachment as one transaction."""
    if (
        not reattach.performed
        or result.rolled_back
        or result.pre_update_sha is None
        or import_check is None
        or prog.branch is None
    ):
        return
    if result.work_succeeded:
        upstream_rc, upstream_output = _run_git_set_upstream(
            git_dir, prog.branch
        )
        result.git_pull_output += upstream_output
        if upstream_rc == 0:
            return
        result.work_errors.append(
            "detached checkout update failed: could not set the configured "
            f"branch upstream to origin/{prog.branch}"
        )
        cause = "post-update branch upstream setup failed"
    else:
        cause = "update transaction failed after detached checkout reattach"
    _rollback_atomic_state(
        result,
        prog,
        git_dir,
        so_backup,
        import_check=import_check,
        source_root=source_root,
        reattach=reattach,
        cause=cause,
        abort_pull_state=result.git_pull_rc != 0,
    )


def _finalize_managed_atomic_transaction(
    result: UpdateResult,
    prog: config.VenvProgram,
    git_dir: Path,
    so_backup: _NativeArtifactSnapshot | None,
    *,
    import_check: str | None,
    source_root: Path | None,
    reattach: _DetachedReattachResult,
) -> None:
    """Roll back any remaining failed untagged managed vibe-qc gate."""
    if (
        result.expected_tag is not None
        or result.expected_sha is not None
        or result.rolled_back
        or result.pre_update_sha is None
        or import_check != "vibeqc"
        or source_root is None
        or result.work_succeeded
    ):
        return
    _rollback_atomic_state(
        result,
        prog,
        git_dir,
        so_backup,
        import_check=import_check,
        source_root=source_root,
        reattach=reattach,
        cause="managed update failed before all transaction gates passed",
    )


def _finalize_immutable_checkout(
    result: UpdateResult,
    prog: config.VenvProgram,
    git_dir: Path,
    so_backup: _NativeArtifactSnapshot | None,
) -> None:
    """Restore a failed tag/SHA transaction to its exact checkout state.

    Fetching is allowed to update remote-tracking refs, but a failed immutable
    update may not leave HEAD, its symbolic attachment, or snapshotted native
    artifacts changed.  The working tree was proven clean before mutation, so
    this rollback never erases operator edits.
    """
    if result.expected_tag is None and result.expected_sha is None:
        return
    if result.pre_update_sha is None or result.success:
        return
    status_rc, status_output = _run_git_status_porcelain(git_dir)
    checkout_matches = _checkout_state_matches(
        git_dir,
        sha=result.pre_update_sha,
        branch=result.pre_update_branch,
    )
    checkout_needs_restore = not (
        checkout_matches and status_rc == 0 and not status_output.strip()
    )
    if not checkout_needs_restore and so_backup is None:
        return
    restore_rc: int | None = 0
    restore_out = ""
    if checkout_needs_restore:
        restore_rc, restore_out = _restore_checkout_state(
            git_dir,
            sha=result.pre_update_sha,
            branch=result.pre_update_branch,
        )
    final_status_rc, final_status = _run_git_status_porcelain(git_dir)
    if (
        restore_rc == 0
        and (
            final_status_rc != 0
            or final_status.strip()
            or not _checkout_state_matches(
                git_dir,
                sha=result.pre_update_sha,
                branch=result.pre_update_branch,
            )
        )
    ):
        restore_rc = 1
        restore_out += "\nrollback verification did not restore a clean checkout"
    removed = 0
    restored = 0
    restored_trees = 0
    native_errors: tuple[str, ...] = ()
    if restore_rc == 0:
        removed, native_errors = _remove_transaction_created_vibeqc_cores(
            so_backup
        )
        try:
            restored = _restore_native_artifacts(
                so_backup,
                restore_source_mtimes=True,
            )
        except OSError as exc:
            native_errors += (f"could not restore native artifacts: {exc}",)
        try:
            restored_trees = _restore_native_runtime_trees(so_backup)
        except OSError as exc:
            native_errors += (
                f"could not restore native runtime trees: {exc}",
            )
        if (
            not native_errors
            and so_backup is not None
            and so_backup.runtime_version_captured
        ):
            import_check = prog.effective_import_check()
            source_root = prog.vibeqc_source_root()
            if import_check == "vibeqc" and source_root is not None:
                identity_rc, identity_output = _run_import_check(
                    prog.python,
                    import_check,
                    symbols=prog.import_symbols,
                    source_root=source_root,
                    expected_version=so_backup.runtime_version,
                    check_version=True,
                )
                if identity_rc != 0:
                    detail = identity_output.strip().splitlines()
                    suffix = detail[-1] if detail else "identity probe failed"
                    native_errors += (
                        f"post-rollback runtime identity failed: {suffix}",
                    )
    rollback_ok = restore_rc == 0 and not native_errors
    result.rolled_back = rollback_ok
    attachment = result.pre_update_branch or "detached HEAD"
    result.rollback_summary = (
        f"restored {attachment} at {result.pre_update_sha[:12]} "
        f"rc={restore_rc}; "
        f"restored {restored} native artifact(s)"
    )
    if restored_trees:
        result.rollback_summary += (
            f"; restored {restored_trees} native runtime tree(s)"
        )
    if removed:
        result.rollback_summary += (
            f"; removed {removed} transaction-created core(s)"
        )
    if native_errors:
        result.rollback_summary += "; " + "; ".join(native_errors)
    if not rollback_ok:
        result.work_errors.append(
            "immutable update rollback FAILED: "
            f"{result.rollback_summary}; {restore_out[-1000:]}"
        )
    else:
        result.work_errors.append(
            f"immutable update rolled back: {result.rollback_summary}"
        )


def _safe_build_parallelism() -> int | None:
    """v0.5.40 + v0.5.41: compute a safe CMAKE_BUILD_PARALLEL_LEVEL for
    the update_script's build step, or None when host RAM is unknown.

    Heuristic: ``min(nproc, max(2, ram_mb // 15000), 6)`` — each cc1plus
    on vibe-qc's heaviest template-instantiation TUs (libint integral
    headers, ``periodic_*.cpp``, ``guess.cpp``, ``gradient.cpp``) can
    peak at **8–10 GB resident**. v0.5.40 budgeted 10 GB/worker with no
    hard cap, which still let workstation run 12 cc1plus instances in
    parallel — the box stayed up but became unresponsive (kernel
    scheduling thrashed against ~100 GB of resident compiler heap, and
    the shell froze for tens of seconds at a time). v0.5.41 budgets
    **15 GB/worker** with a hard cap of **6** to prefer
    responsiveness-during-build over wall-time. Concrete numbers for
    the current fleet:

    * workstation (32 threads, 125 GB RAM) → 6 workers (≤60 GB peak, ≥65 GB host headroom)
    * compute-a    (16 threads,  62 GB RAM) → 4 workers (≤40 GB peak, ≥22 GB host headroom)
    * macbook (10 threads,  32 GB RAM) → 2 workers (≤20 GB peak, ≥12 GB host headroom)

    Why a hard cap of 6: on monster machines (≥100 GB), the bottleneck
    stops being cc1plus memory and becomes ninja link/serialization +
    filesystem write contention. Above 6 workers, you spend more time
    in ld + buffered IO than in compile, and the only thing the extra
    parallelism buys is more pressure on the page cache. The vq
    daemon's job is to refresh the env safely, not to win a wall-time
    contest — let the system stay usable while it runs.

    Why this matters: the OOM-induced workstation hang on 2026-05-16
    happened during an interactive ``bash scripts/update-dev.sh`` run
    where ninja used all 32 threads on cc1plus and peaked beyond
    125 GB resident. The vq-cgroup-controlled job path is protected
    (per-job MemoryMax), but the daemon-side ``update_script`` runs
    outside that scope — this env cap is the watchdog for build
    parallelism specifically. v0.5.41 is the followup: 12 workers
    didn't crash the box but did wedge interactive use, which the user
    flagged as a regression vs v0.5.40's intent.

    macOS dev / hosts without /proc/meminfo: returns None; the caller
    leaves the env untouched so the system / user default wins.
    """
    try:
        with open("/proc/meminfo", encoding="utf-8") as f:
            mem_mb: int | None = None
            for line in f:
                if line.startswith("MemTotal:"):
                    mem_mb = int(line.split()[1]) // 1024  # kB → MB
                    break
    except (OSError, ValueError):
        return None
    if mem_mb is None:
        return None
    nproc = os.cpu_count() or 1
    return min(nproc, max(2, mem_mb // 15000), 6)


def _build_niceness_prefix() -> list[str]:
    """v0.5.41: argv prefix that deprioritizes the update_script's CPU +
    IO so an in-flight build can't starve the foreground shell.

    Returns ``["nice", "-n", "19", "ionice", "-c", "3"]`` (or any
    subset whose binaries are present), or ``[]`` if neither is
    available. ``nice -n 19`` is the lowest CPU priority POSIX exposes;
    ``ionice -c 3`` is Linux's idle-class IO scheduling (only runs IO
    when nothing else wants the disk). Both are non-fatal optimizations:
    if a binary isn't on PATH (e.g. macOS lacks ``ionice``) we skip it
    silently rather than break the update.

    The /proc/meminfo gate is intentional: this is the same "is this a
    Linux build host" signal :func:`_safe_build_parallelism` uses, so
    the two helpers agree on when the niceness machinery is worth
    activating at all. On macOS dev boxes you typically *want* the
    build to take whatever CPU it needs."""
    if not Path("/proc/meminfo").exists():
        return []
    prefix: list[str] = []
    if shutil.which("nice"):
        prefix += ["nice", "-n", "19"]
    if shutil.which("ionice"):
        prefix += ["ionice", "-c", "3"]
    return prefix


@dataclass
class _BuildRunResult:
    """Outcome of :func:`_run_monitored_build` — a build subprocess run
    under wall-clock + stall + heartbeat supervision."""

    rc: int | None
    """Process return code, or ``None`` when the process was reaped
    (``timed_out`` / ``stalled``) or never started (``error`` set)."""
    output: str
    """Merged stdout+stderr captured up to exit/reap."""
    timed_out: bool = False
    """True iff the wall-clock cap was hit and the process group reaped."""
    stalled: bool = False
    """True iff the no-output stall cap was hit and the group reaped."""
    error: str | None = None
    """Set iff the subprocess could not be started (OSError text)."""


def _kill_process_group(
    proc: subprocess.Popen[str], *, log_label: str,
    grace: float = _BUILD_KILL_GRACE_SECONDS,
) -> None:
    """SIGTERM -> grace -> SIGKILL the whole process group led by ``proc``.

    A timed-out/stalled build's direct child is ``nice``/``bash``, but the
    real CPU+RAM hog is its ninja/cc1plus *grandchildren*. ``subprocess``'s
    own timeout kill signals only the direct child, orphaning the
    grandchildren to keep burning cores — the 2026-06-26 "holding 6 CPUs"
    symptom. Because the child was started with ``start_new_session=True``
    it leads its own process group, so a single ``killpg`` reaps the entire
    build tree. SIGCONT first in case a stalled step was left stopped
    (SIGKILL alone won't dislodge a stopped, SIGTERM-ignoring process)."""
    try:
        pgid = os.getpgid(proc.pid)
    except (ProcessLookupError, OSError):
        return  # already gone

    def _sig(sig: int) -> bool:
        try:
            os.killpg(pgid, sig)
            return True
        except (ProcessLookupError, OSError):
            return False

    _sig(signal.SIGCONT)
    if not _sig(signal.SIGTERM):
        return
    try:
        proc.wait(timeout=grace)
        log.info("%s: process group %d reaped via SIGTERM", log_label, pgid)
        return
    except subprocess.TimeoutExpired:
        pass
    log.warning(
        "%s: process group %d ignored SIGTERM; sending SIGKILL",
        log_label, pgid,
    )
    _sig(signal.SIGKILL)
    with contextlib.suppress(subprocess.TimeoutExpired):
        proc.wait(timeout=grace)


def _run_monitored_build(
    argv: list[str], *, cwd: str, env: dict[str, str],
    wall_timeout: float, stall_timeout: float, heartbeat_interval: float,
    log_label: str, emit: Callable[[str], None] | None = None,
    pass_fds: tuple[int, ...] = (),
) -> _BuildRunResult:
    """Run ``argv`` as a supervised build subprocess; return a
    :class:`_BuildRunResult`.

    Supervision (fix 1 of the 2026-06-26 fleet incident, where a stuck
    ``build-env`` ran 12h+ with empty stdout/stderr while pinning 6 CPUs):

    * **Own process group** (``start_new_session=True``) so a reap kills
      the whole ninja/cc1plus tree, not just the ``bash`` wrapper.
    * **Wall-clock cap** (``wall_timeout`` s): hard upper bound.
    * **Stall cap** (``stall_timeout`` s, ``0`` disables): reap if the
      build emits NO output for that long — catches a true wedge long
      before the (possibly operator-raised) wall cap.
    * **Heartbeat** (``heartbeat_interval`` s, ``0`` disables): periodic
      progress line to the daemon log and the narration channel, so a
      running CLI build remains observable without corrupting JSON stdout.

    Output is drained on a daemon thread (stderr merged into stdout) so a
    full pipe can't deadlock the supervisor. ``emit`` defaults to
    :func:`output.narrate`, which retains the run log and uses the CLI's
    stderr policy. Library calls without a channel are silent. An explicit
    caller sink overrides narration.
    """
    if emit is None:
        emit = output.narrate

    try:
        proc = subprocess.Popen(
            argv, cwd=cwd, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, start_new_session=True, pass_fds=pass_fds,
            stdin=subprocess.DEVNULL,
        )
    except OSError as e:
        return _BuildRunResult(rc=None, output="", error=str(e))

    chunks: list[str] = []
    last_output_mono = [time.monotonic()]

    def _reader() -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            chunks.append(line)
            # Tee the full build stream to the run log. `chunks` is truncated
            # to its last 80 lines before anything durable is written, so
            # without this the other 99% of a two-hour compile is discarded
            # when the process exits — the single most expensive gap in
            # diagnosing the 2026-07-22 fleet incident. Deliberately run-log
            # only, never the terminal: compiler output would drown the
            # narration this is meant to make legible.
            output.run_log_write(line)
            last_output_mono[0] = time.monotonic()

    reader = threading.Thread(target=_reader, daemon=True)
    reader.start()

    start = time.monotonic()
    last_heartbeat = start
    timed_out = stalled = False
    try:
        while True:
            if proc.poll() is not None:
                break
            now = time.monotonic()
            elapsed = now - start
            idle = now - last_output_mono[0]
            if wall_timeout and elapsed >= wall_timeout:
                timed_out = True
                break
            if stall_timeout and idle >= stall_timeout:
                stalled = True
                break
            if heartbeat_interval and (now - last_heartbeat) >= heartbeat_interval:
                last_heartbeat = now
                msg = (
                    f"{log_label}: still running "
                    f"({elapsed:.0f}s elapsed, {idle:.0f}s since last output)"
                )
                log.info(msg)
                with contextlib.suppress(Exception):
                    refresh_admin_update_marker_heartbeat(msg)
                with contextlib.suppress(Exception):
                    emit(msg)
            time.sleep(_BUILD_POLL_INTERVAL_SECONDS)
    except BaseException:
        _kill_process_group(proc, log_label=log_label)
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=_BUILD_KILL_GRACE_SECONDS)
        reader.join(timeout=5.0)
        raise

    if timed_out or stalled:
        elapsed = time.monotonic() - start
        if timed_out:
            # Not "wedged": the stall detector did not fire, so the build was
            # still producing output. A slow host and a stuck build call for
            # opposite responses, and compute-b's reaped build finished in
            # fifteen minutes once the cap was raised (#32).
            msg = (
                f"{log_label}: reaping build — wall-clock cap hit after "
                f"{elapsed:.0f}s while it was still producing output; a slow "
                "host needs a higher cap ([hosts.X] "
                "update_script_timeout_seconds or VQ_UPDATE_SCRIPT_TIMEOUT)"
            )
        else:
            msg = (
                f"{log_label}: reaping wedged build — stall cap (no output) "
                f"hit after {elapsed:.0f}s"
            )
        log.error(msg)
        with contextlib.suppress(Exception):
            emit(msg)
        _kill_process_group(proc, log_label=log_label)

    with contextlib.suppress(subprocess.TimeoutExpired):
        proc.wait(timeout=_BUILD_KILL_GRACE_SECONDS)
    reader.join(timeout=5.0)
    return _BuildRunResult(
        rc=proc.returncode, output="".join(chunks),
        timed_out=timed_out, stalled=stalled,
    )


def _run_update_script(
    git_dir: Path, script_cmd: str, *,
    work_errors: list[str],
    extra_args: list[str] | None = None,
    label: str = "update_script",
    strip_config_ref_args: bool = False,
    managed_daemon_restart: bool = False,
    lifecycle_target: Path | None = None,
) -> tuple[int | None, str, float | None]:
    """Run ``bash <script> [args...]`` from ``git_dir``.

    v0.5.39: ``script_cmd`` is :func:`shlex.split`-parsed so it can carry
    args after the path. The first token is the script's relative path
    under ``git_dir``; everything after is forwarded to bash as argv to
    the script. Two examples that both work:

    * ``"scripts/update-dev.sh"`` — legacy single-path form (still
      valid; just no args).
    * ``"scripts/update.sh --dev"`` — preferred for vibe-qc's dev
      branch refresh now that ``scripts/update-dev.sh`` is gone (it
      was a thin shell wrapper around ``update.sh --dev``; one
      maintained script beats two).

    Empty / whitespace-only ``script_cmd`` is treated as "no script
    configured" (returns None with a work_errors entry).

    v0.5.40: ``CMAKE_BUILD_PARALLEL_LEVEL`` is injected into the
    subprocess env based on host RAM/CPU (see
    :func:`_safe_build_parallelism`). This caps ninja parallelism for
    vibe-qc-style template-heavy builds and prevents the global-OOM
    failure mode that hung workstation on 2026-05-16. If the caller's env
    already sets the var, it's preserved verbatim — explicit user
    intent wins.

    v0.5.41: argv is additionally prefixed with ``nice -n 19 ionice -c
    3`` on Linux (see :func:`_build_niceness_prefix`) so the in-flight
    build runs at idle CPU+IO priority. v0.5.40's parallelism cap
    prevents the OOM hang, but 12 cc1plus workers still made the box
    unresponsive — this prefix is the followup that keeps interactive
    use snappy while the rebuild churns. The cap stays as the hard
    correctness guarantee (memory cannot exceed); niceness is the
    soft latency guarantee (responsiveness cannot collapse).
    """
    parts = shlex.split(script_cmd)
    if not parts:
        work_errors.append(f"{label} is empty after shlex.split")
        return None, "", None
    script_rel, *script_args = parts
    if strip_config_ref_args:
        script_args = _strip_update_script_ref_args(script_args)
    script_path = git_dir / script_rel
    if not script_path.exists():
        work_errors.append(
            f"{label} not found: {script_path}"
        )
        return None, "", None
    # v0.5.40: parallelism cap. Honor pre-existing env override; only
    # set the default when the caller hasn't expressed an opinion.
    env = os.environ.copy()
    # The managed script's cleanliness checks are observations, not a reason
    # to refresh Git's index. Required checkout/reset locks still work with
    # this setting, while `git status`/`git diff` cannot become a source of a
    # stale optional index lock during the stopped-daemon window (#236).
    env["GIT_OPTIONAL_LOCKS"] = "0"
    # This is an invocation-scoped capability, never ambient configuration.
    # A stale parent-shell value must not make a direct lifecycle script leave
    # a daemon running across its own update. Only admin.update_env sets it,
    # and only when that outer transaction is committed to performing and
    # verifying the required restart after the script succeeds.
    env.pop("VQ_ADMIN_MANAGED_DAEMON_RESTART_PID", None)
    env.pop(fleet_operation.ENV_LIFECYCLE_HANDOFF, None)
    for key in list(env):
        if key.startswith((
            "VIBE_TOOLSET_ADMIN_LOCK_",
            "VIBE_TOOLSET_INHERITED_",
        )):
            env.pop(key, None)
    pass_fds: tuple[int, ...] = ()
    if lifecycle_target is not None and getattr(
        _toolset_lifecycle_local, "active", None,
    ) is not None:
        try:
            handoff_env, pass_fds = _current_toolset_lock_handoff(
                git_dir, lifecycle_target,
            )
        except AdminError as exc:
            work_errors.append(str(exc))
            return None, "", None
        env.update(handoff_env)
    if managed_daemon_restart:
        env["VQ_ADMIN_MANAGED_DAEMON_RESTART_PID"] = str(os.getpid())
        env.pop("VQ_VENV", None)
    if "CMAKE_BUILD_PARALLEL_LEVEL" not in env:
        cap = _safe_build_parallelism()
        if cap is not None:
            env["CMAKE_BUILD_PARALLEL_LEVEL"] = str(cap)
    # Cold-build robustness: put Arch/Manjaro perl dirs (pod2man) on PATH so
    # the libecpint/libcerf man-page step doesn't fail "command not found".
    _augment_build_path(env)
    # v0.5.41: idle CPU+IO priority so the build can't starve the
    # foreground shell. On macOS / missing-tool hosts the prefix is
    # empty and argv is unchanged.
    # v0.7.1 *Lamport's Clock* Item 3: append per-invocation flags
    # forwarded from ``vq admin update --update-script-arg X``.
    # Goes AFTER the config-side script_args so the caller's intent
    # wins on conflicting flags (last arg wins in most update.sh-
    # style parsers; we follow the same convention).
    forwarded = list(extra_args) if extra_args else []
    argv = [
        *_build_niceness_prefix(),
        "bash", str(script_path), *script_args, *forwarded,
    ]
    # v0.12.x fix 1: run the build under wall-clock + stall + heartbeat
    # supervision in its OWN process group, so a wedged rebuild fails
    # loudly and the whole ninja/cc1plus tree is reaped (not just the
    # bash wrapper, which left grandchildren pinning cores in the
    # 2026-06-26 fleet incident). See :func:`_run_monitored_build`.
    script_timeout = _update_script_timeout()
    stall_timeout = _build_stall_timeout()
    started_at = time.monotonic()
    run = _run_monitored_build(
        argv,
        cwd=str(git_dir),
        env=env,
        wall_timeout=script_timeout,
        stall_timeout=stall_timeout,
        heartbeat_interval=_build_heartbeat_interval(),
        log_label=(
            f"build-env {git_dir.name}"
            if label == "update_script"
            else f"post-update-env {git_dir.name}"
        ),
        pass_fds=pass_fds,
    )
    elapsed = time.monotonic() - started_at
    if run.error is not None:
        work_errors.append(f"{label} failed to start: {run.error}")
        return None, run.output, elapsed
    if run.timed_out:
        work_errors.append(
            f"{label} timed out after {script_timeout:g}s "
            f"(process group reaped)"
        )
        return None, run.output, elapsed
    if run.stalled:
        work_errors.append(
            f"{label} stalled: no output for {stall_timeout:g}s "
            f"(process group reaped)"
        )
        return None, run.output, elapsed
    return run.rc, run.output, elapsed


# ----------------------------------------------------------------------
# v0.5.42: vq self-update auto-restart of vq-daemon
#
# Problem: vq is editable-installed (`pip install -e .`). After
# `git pull && pip install -e .` lands new code on disk, the running
# `vq-daemon` process keeps the OLD module objects in memory until it
# restarts. There's no automatic restart and no diagnostic — you just
# silently run stale code. The 2026-05-16 incident (v0.5.40's parallelism
# cap on disk while the daemon ran v0.5.39 in memory for 30 minutes) is
# the canonical case. See `vibe-queue/docs/operations.md` § "Daemon
# running stale code after `pip install -e .`".
#
# Approach: at the end of a successful `vq admin update <env>`, detect
# whether <env> is the venv from which `vq-daemon` was launched. If yes,
# `systemctl --user restart vq-daemon`. If no, leave it alone. The
# detection uses `systemctl --user show vq-daemon -p ExecStart --value`
# and compares the daemon's executable path against the env's venv bin
# dir (derived from `prog.python`).
# ----------------------------------------------------------------------


_EXECSTART_PATH_RE = re.compile(r"path=([^\s;]+)")
_SYSTEMD_EXECSTART_DEFINITION_RE = re.compile(
    r"\A\{ path=(?P<path>[^\s;\r\n]+) ; "
    r"argv\[\]=(?P<argv>[^;\r\n]*\S[^;\r\n]*) ; "
    r"ignore_errors=(?P<ignore_errors>yes|no) ; "
    r"start_time=\[[^\]\r\n]*\] ; "
    r"stop_time=\[[^\]\r\n]*\] ; "
    r"pid=\d+ ; "
    r"code=(?:\([^)\r\n]+\)|[A-Za-z][A-Za-z_-]*) ; "
    r"status=-?\d+(?:/[A-Za-z0-9_+-]+)? \}\Z"
)


def _systemctl_user_available() -> bool:
    """True iff ``systemctl`` is on PATH AND the user manager is
    reachable. On macOS (no systemctl) returns False. On Linux hosts
    where the user-systemd is a zombie (see operations.md § "Failed to
    connect to user scope bus..."), the ``is-system-running`` probe
    returns non-zero — we treat that as "not available" so detection
    falls through to the sys.executable cross-check.
    """
    if not shutil.which("systemctl"):
        return False
    try:
        proc = subprocess.run(
            ["systemctl", "--user", "is-system-running"],
            capture_output=True,
            text=True,
            timeout=10,
            stdin=subprocess.DEVNULL,
        )
    except Exception:  # noqa: BLE001 — best-effort probe; any failure
                       # here = "systemctl can't help us right now",
                       # which is the safe verdict (caller falls back
                       # to the sys.executable cross-check).
        return False
    # ``is-system-running`` returns rc=0 for "running", rc!=0 for
    # "starting"/"degraded"/etc. but the manager is still reachable in
    # the degraded case — what we care about is whether the IPC works.
    # Connection-refused from a zombie manager prints to stderr and
    # exits rc=1 with output "offline" or "Failed to connect..." — the
    # heuristic: anything mentioning "Failed to connect" or "offline"
    # is unusable for our purposes.
    out = (proc.stdout or "").strip().lower()
    err = (proc.stderr or "").strip().lower()
    return not ("failed to connect" in err or "offline" in out)


def _query_daemon_execstart() -> tuple[int, str]:
    """Return (rc, raw_value) from
    ``systemctl --user show vq-daemon -p ExecStart --value``. The raw
    value looks like ``{ path=/...; argv[]=/... ; ... }`` (or empty
    when the unit isn't loaded). Caller parses the path out via
    :data:`_EXECSTART_PATH_RE`. rc=0 on success; non-zero when
    systemctl couldn't reach the user manager or the unit is missing.
    """
    try:
        proc = subprocess.run(
            ["systemctl", "--user", "show", "vq-daemon",
             "-p", "ExecStart", "--value"],
            capture_output=True,
            text=True,
            timeout=10,
            stdin=subprocess.DEVNULL,
        )
    except Exception as e:  # noqa: BLE001 — best-effort probe; see
                            # :func:`_systemctl_user_available`.
        return 1, str(e)
    return proc.returncode, (proc.stdout or "") + (proc.stderr or "")


def _parse_execstart_path(execstart: str) -> str | None:
    """Pull the ``path=<path>`` field out of an ExecStart raw value.
    Returns None if no path= field is present (e.g. unit not loaded,
    empty value, malformed output)."""
    match = _EXECSTART_PATH_RE.search(execstart)
    return match.group(1) if match else None


def _normalize_systemd_command_identity(
    identity: tuple[str, ...] | None,
) -> tuple[str, str, str] | None:
    """Reduce one real-shaped systemd ExecStart value to its definition.

    ``systemctl show`` renders path, argv and ignore-errors together with
    timestamps, PID and exit status from the current or last run.  The latter
    fields change on stop/restart and cannot identify the unit definition.
    Accept only the complete single-command shape that vq's service uses;
    malformed, multi-command and newline-bearing values fail closed.
    """
    if (
        identity is None
        or len(identity) != 2
        or identity[0] != "systemd-execstart"
        or "\r" in identity[1]
        or "\n" in identity[1]
    ):
        return None
    match = _SYSTEMD_EXECSTART_DEFINITION_RE.fullmatch(identity[1])
    if match is None:
        return None
    return (
        match.group("path"),
        match.group("argv"),
        match.group("ignore_errors"),
    )


def _systemd_command_identity_is_vq_daemon(
    identity: tuple[str, ...] | None,
) -> bool:
    """Whether systemctl's textual argv is vq's supported daemon command."""
    definition = _normalize_systemd_command_identity(identity)
    if definition is None:
        return False
    executable, argv, _ignore_errors = definition
    daemon_argv = f"{executable} daemon run"
    return argv == daemon_argv or (
        argv.startswith(f"{daemon_argv} ")
        and not argv.endswith((" ", "\t"))
    )


def _query_daemon_mainpid() -> int | None:
    """Return the daemon's current MainPID, or None if systemctl can't
    answer or the daemon isn't running (MainPID=0). Used to report the
    PID transition in the restart message."""
    try:
        proc = subprocess.run(
            ["systemctl", "--user", "show", "vq-daemon",
             "-p", "MainPID", "--value"],
            capture_output=True,
            text=True,
            timeout=10,
            stdin=subprocess.DEVNULL,
        )
    except Exception:  # noqa: BLE001 — best-effort probe
        return None
    if proc.returncode != 0:
        return None
    raw = (proc.stdout or "").strip()
    try:
        pid = int(raw)
    except ValueError:
        return None
    return pid if pid > 0 else None


@dataclass(frozen=True)
class _SelfUpdateProbe:
    """Outcome of asking "is updating ``prog`` a vq self-update?".

    * ``is_self_update`` — best-effort verdict. True when the daemon
      was launched from this env's venv bin dir.
    * ``daemon_running`` — True iff the selected service manager reports a
      live daemon PID. False when stopped; None when status is unknown.
    * ``service_manager`` — the deterministic manager selected for this host,
      or None when no supported manager is available.
    * ``manager_available`` — whether the manager query was successful enough
      to prove the loaded service belongs to the updated venv.
    * ``diagnostic`` — one short human-readable line for the formatter
      / logs.
    """

    is_self_update: bool
    daemon_running: bool | None
    service_manager: str | None
    manager_available: bool
    diagnostic: str


class _DaemonServiceManager(StrEnum):
    SYSTEMD = "systemd"
    LAUNCHD = "launchd"


def _service_command_identities_match(
    manager: _DaemonServiceManager,
    current: tuple[str, ...] | None,
    recorded: tuple[str, ...] | None,
) -> bool:
    """Bind a manager definition while ignoring only systemd run state."""
    if manager is not _DaemonServiceManager.SYSTEMD:
        return current == recorded
    current_definition = _normalize_systemd_command_identity(current)
    recorded_definition = _normalize_systemd_command_identity(recorded)
    return (
        current_definition is not None
        and current_definition == recorded_definition
    )


@dataclass(frozen=True)
class _DaemonServiceState:
    manager: _DaemonServiceManager
    running: bool | None
    pid: int | None
    executable: str | None
    diagnostic: str
    command_identity: tuple[str, ...] | None = None


@dataclass
class _ManagedDaemonUpdate:
    """Service state retained while outer admin owns an on-disk update."""

    manager: _DaemonServiceManager
    env: str
    pre_pid: int | None
    was_running: bool
    was_stopped: bool
    pre_source_sha: str | None
    pre_source_tree_sha256: str | None
    pre_checkout_branch: str | None
    venv_path: Path
    venv_backup: Path | None
    service_executable: str
    owner_uid: int = field(default_factory=os.geteuid)
    service_command: tuple[str, ...] = ()
    transaction_id: str = ""
    backup_moved: bool = False
    receipt_phase: str = "armed"
    target_source_sha: str | None = None
    target_source_tree_sha256: str | None = None
    terminal_verified: bool = False


LAUNCHD_DAEMON_LABEL = "com.vq.daemon"


def _select_daemon_service_manager() -> _DaemonServiceManager | None:
    """Select the host's supported user service manager deterministically."""
    if sys.platform == "darwin":
        return (
            _DaemonServiceManager.LAUNCHD
            if shutil.which("launchctl") is not None
            else None
        )
    if _systemctl_user_available():
        return _DaemonServiceManager.SYSTEMD
    return None


def _launchd_daemon_target() -> str:
    return f"gui/{os.getuid()}/{LAUNCHD_DAEMON_LABEL}"


def _query_launchd_daemon() -> tuple[int, str]:
    """Return ``launchctl print`` rc and combined diagnostic output."""
    try:
        proc = subprocess.run(
            ["launchctl", "print", _launchd_daemon_target()],
            capture_output=True,
            text=True,
            timeout=30,
            stdin=subprocess.DEVNULL,
        )
    except subprocess.TimeoutExpired:
        return 124, "launchctl print timed out after 30s"
    except OSError as exc:
        return 127, f"failed to invoke launchctl: {exc}"
    output = "\n".join(part for part in (proc.stdout, proc.stderr) if part).strip()
    return proc.returncode, output


def _parse_launchd_service_state(raw: str) -> tuple[int | None, str | None]:
    """Parse the stable ``pid =`` and ``program =`` launchctl fields."""
    pid: int | None = None
    executable: str | None = None
    for line in raw.splitlines():
        stripped = line.strip()
        if stripped.startswith("pid ="):
            value = stripped.partition("=")[2].strip()
            if value.isdigit() and int(value) > 0:
                pid = int(value)
        elif stripped.startswith("program ="):
            value = stripped.partition("=")[2].strip().strip('"')
            executable = value or None
    return pid, executable


def _query_daemon_service_state(
    manager: _DaemonServiceManager,
) -> _DaemonServiceState:
    if manager is _DaemonServiceManager.SYSTEMD:
        try:
            proc = subprocess.run(
                [
                    "systemctl", "--user", "show", "vq-daemon",
                    "--property=ExecStart",
                    "--property=ActiveState",
                    "--property=MainPID",
                    "--no-pager",
                ],
                capture_output=True,
                text=True,
                timeout=30,
                stdin=subprocess.DEVNULL,
            )
        except subprocess.TimeoutExpired:
            return _DaemonServiceState(
                manager, None, None, None,
                "systemctl show vq-daemon timed out",
            )
        except OSError as exc:
            return _DaemonServiceState(
                manager, None, None, None,
                f"failed to invoke systemctl: {exc}",
            )
        raw = (proc.stdout or "") + (proc.stderr or "")
        if proc.returncode != 0:
            return _DaemonServiceState(
                manager, None, None, None,
                f"systemctl show vq-daemon rc={proc.returncode}: {raw[:200]}",
            )
        values: dict[str, str] = {}
        expected_properties = {"ExecStart", "ActiveState", "MainPID"}
        for line in (proc.stdout or "").splitlines():
            if not line:
                continue
            key, separator, value = line.partition("=")
            if (
                not separator
                or key not in expected_properties
                or key in values
            ):
                return _DaemonServiceState(
                    manager,
                    None,
                    None,
                    None,
                    "systemctl show vq-daemon returned ambiguous properties",
                )
            values[key] = value
        if values.keys() != expected_properties:
            return _DaemonServiceState(
                manager,
                None,
                None,
                None,
                "systemctl show vq-daemon omitted required properties",
            )
        raw_execstart = values["ExecStart"]
        command_identity = ("systemd-execstart", raw_execstart)
        if (
            raw_execstart
            and _normalize_systemd_command_identity(command_identity) is None
        ):
            return _DaemonServiceState(
                manager,
                None,
                None,
                None,
                "systemctl show vq-daemon returned ambiguous ExecStart",
            )
        executable = _parse_execstart_path(raw_execstart)
        active_state = values.get("ActiveState")
        raw_pid = values.get("MainPID", "")
        pid = int(raw_pid) if raw_pid.isdigit() and int(raw_pid) > 0 else None
        if active_state in {"active", "activating", "reloading", "deactivating"}:
            running: bool | None = True
        elif active_state in {"inactive", "failed"} and raw_pid == "0":
            running = False
        else:
            running = None
        return _DaemonServiceState(
            manager,
            running,
            pid,
            executable,
            f"systemd ExecStart={executable!r}; ActiveState="
            f"{active_state or 'unknown'}; MainPID={raw_pid or 'unknown'}",
            command_identity,
        )

    rc, raw = _query_launchd_daemon()
    if rc != 0:
        lower = raw.lower()
        definitely_absent = (
            rc in {3, 113}
            or "could not find service" in lower
            or "not found" in lower
        )
        return _DaemonServiceState(
            manager, False if definitely_absent else None, None, None,
            f"launchctl print {_launchd_daemon_target()} rc={rc}: {raw[:200]}",
            tuple(_launchd_plist_argv() or ()),
        )
    pid, executable = _parse_launchd_service_state(raw)
    return _DaemonServiceState(
        manager,
        pid is not None,
        pid,
        executable,
        f"launchd program={executable!r}; pid={pid or 'inactive'}",
        tuple(_launchd_plist_argv() or ()),
    )


def _query_daemon_service_pid(manager: _DaemonServiceManager) -> int | None:
    if manager is _DaemonServiceManager.SYSTEMD:
        return _query_daemon_mainpid()
    rc, raw = _query_launchd_daemon()
    if rc != 0:
        return None
    pid, _ = _parse_launchd_service_state(raw)
    return pid


def _run_daemon_service_command(
    argv: list[str], *, display: str,
) -> tuple[bool, str]:
    try:
        proc = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            timeout=DAEMON_RESTART_TIMEOUT_SECONDS,
            stdin=subprocess.DEVNULL,
        )
    except subprocess.TimeoutExpired:
        return False, (
            f"{display} timed out after {DAEMON_RESTART_TIMEOUT_SECONDS}s"
        )
    except OSError as exc:
        return False, f"failed to invoke {argv[0]}: {exc}"
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        return False, f"{display} failed rc={proc.returncode}: {detail or '(no output)'}"
    return True, f"{display} ... done"


def _launchd_plist_argv() -> list[str] | None:
    plist = Path.home() / "Library" / "LaunchAgents" / (
        f"{LAUNCHD_DAEMON_LABEL}.plist"
    )
    try:
        info = plist.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_nlink != 1
        ):
            return None
        with plist.open("rb") as stream:
            payload = plistlib.load(stream)
    except (OSError, plistlib.InvalidFileException, ValueError):
        return None
    argv = payload.get("ProgramArguments") if isinstance(payload, dict) else None
    if (
        not isinstance(argv, list)
        or not argv
        or not all(isinstance(value, str) for value in argv)
    ):
        return None
    return argv


def _launchd_plist_executable() -> str | None:
    argv = _launchd_plist_argv()
    return argv[0] if argv else None


def _launchd_plist_matches_venv(venv_path: Path) -> bool:
    return _launchd_argv_matches_venv(_launchd_plist_argv(), (venv_path / "bin").resolve())


def _launchd_argv_matches_venv(argv: list[str] | None, venv_bin: Path) -> bool:
    if not argv:
        return False
    if _execstart_matches_vq(argv[0], venv_bin):
        return argv[1:3] == ["daemon", "run"]
    return (
        _execstart_matches_venv(argv[0], venv_bin)
        and argv[1:5] == ["-m", "vq", "daemon", "run"]
    )


def _launchd_executable_matches_venv(executable: str, venv_bin: Path) -> bool:
    """Bind the loaded program to the secure plist and its daemon command."""
    argv = _launchd_plist_argv()
    if not argv or not _launchd_argv_matches_venv(argv, venv_bin):
        return False
    if _execstart_matches_vq(executable, venv_bin):
        return _execstart_matches_vq(argv[0], venv_bin)
    # Do not resolve Python's final symlink: distinct venvs commonly share
    # the same base interpreter and must never become interchangeable.
    return (
        _execstart_matches_venv(executable, venv_bin)
        and Path(argv[0]).name == "python"
        and Path(argv[0]).parent.resolve() == venv_bin
    )


def _service_executable_matches(
    executable: str | None,
    prog: config.VenvProgram,
    *,
    manager: _DaemonServiceManager | None = None,
) -> bool:
    if executable is None:
        return False
    venv_bin = Path(prog.python).parent.resolve()
    if manager is _DaemonServiceManager.LAUNCHD:
        return _launchd_executable_matches_venv(executable, venv_bin)
    return _execstart_matches_vq(executable, venv_bin)


def _lifecycle_service_executable_matches(
    executable: str | None, lifecycle: _ManagedDaemonUpdate,
) -> bool:
    if executable is None:
        return False
    venv_bin = (lifecycle.venv_path / "bin").resolve()
    if lifecycle.manager is _DaemonServiceManager.LAUNCHD:
        return _launchd_executable_matches_venv(executable, venv_bin)
    return _execstart_matches_vq(executable, venv_bin)


def _prove_managed_daemon_quiescent(
    manager: _DaemonServiceManager,
    prog: config.VenvProgram,
) -> tuple[bool, str]:
    """Require authoritative inactive/unloaded state after manager stop."""
    if manager is _DaemonServiceManager.SYSTEMD:
        state = _query_daemon_service_state(manager)
        if not _service_executable_matches(
            state.executable, prog, manager=manager,
        ):
            return False, (
                "systemd service identity became unavailable or changed after stop: "
                + state.diagnostic
            )
        if state.running is False and state.pid is None:
            return True, state.diagnostic
        return False, "systemd service is still running after stop: " + state.diagnostic

    rc, raw = _query_launchd_daemon()
    if rc == 0:
        pid, executable = _parse_launchd_service_state(raw)
        if pid is None and _service_executable_matches(
            executable, prog, manager=manager,
        ):
            return True, "launchd service is loaded but inactive"
        return False, f"launchd service remains active or changed: {raw[:300]}"
    lower = raw.lower()
    if rc in {3, 113} or "could not find service" in lower or "not found" in lower:
        return True, "launchd service is unloaded"
    return False, f"launchd state is unknown after bootout rc={rc}: {raw[:300]}"


def _reattest_service_before_start(
    lifecycle: _ManagedDaemonUpdate,
) -> tuple[bool, str]:
    state = _query_daemon_service_state(lifecycle.manager)
    executable = (
        state.executable
        if lifecycle.manager is _DaemonServiceManager.SYSTEMD
        else _launchd_plist_executable()
    )
    if not _lifecycle_service_executable_matches(executable, lifecycle):
        return False, (
            f"{lifecycle.manager.value} service no longer names the managed "
            "venv"
        )
    if not _service_command_identities_match(
        lifecycle.manager,
        state.command_identity,
        lifecycle.service_command,
    ):
        return False, (
            f"{lifecycle.manager.value} service command changed during the "
            "managed update"
        )
    return True, state.diagnostic


def _wait_for_managed_daemon_quiescence(
    manager: _DaemonServiceManager,
    prog: config.VenvProgram,
) -> tuple[bool, str]:
    """Wait only on authoritative inactive/unloaded manager evidence."""
    deadline = time.monotonic() + DAEMON_RESTART_TIMEOUT_SECONDS
    detail = "daemon quiescence was not checked"
    while True:
        quiescent, detail = _prove_managed_daemon_quiescent(manager, prog)
        if quiescent:
            return True, detail
        if time.monotonic() >= deadline:
            return False, detail
        time.sleep(0.1)


def _verify_preupdate_daemon_identity(
    lifecycle: _ManagedDaemonUpdate,
) -> tuple[bool, str]:
    if (
        lifecycle.pre_source_sha is None
        or lifecycle.pre_source_tree_sha256 is None
    ):
        return False, "pre-update daemon identity is incomplete"
    provenance = _verify_restarted_daemon(
        lifecycle.pre_source_sha,
        expected_tree_sha256=lifecycle.pre_source_tree_sha256,
        require_exact_identity=True,
    )
    return provenance.verified, provenance.detail


def _stop_managed_daemon_for_restore(
    prog: config.VenvProgram,
    lifecycle: _ManagedDaemonUpdate,
) -> tuple[bool, str]:
    """Quiesce only the exact service definition owned by this transaction."""
    if lifecycle.manager is _DaemonServiceManager.SYSTEMD:
        state = _query_daemon_service_state(lifecycle.manager)
        if not _lifecycle_service_executable_matches(
            state.executable, lifecycle,
        ):
            return False, (
                "systemd service identity is unavailable or changed: "
                + state.diagnostic
            )
        if state.running is False and state.pid is None:
            return True, state.diagnostic
        argv = ["systemctl", "--user", "stop", "vq-daemon"]
        display = "systemctl --user stop vq-daemon"
    else:
        rc, raw = _query_launchd_daemon()
        lower = raw.lower()
        if rc in {3, 113} or "could not find service" in lower or "not found" in lower:
            if not _launchd_plist_matches_venv(lifecycle.venv_path):
                return False, "launchd plist no longer names the managed venv"
            return True, "launchd service is already unloaded"
        if rc != 0:
            return False, f"launchd service state is unknown rc={rc}: {raw[:300]}"
        _pid, executable = _parse_launchd_service_state(raw)
        if not _lifecycle_service_executable_matches(executable, lifecycle):
            return False, "launchd service identity changed before rollback"
        argv = ["launchctl", "bootout", _launchd_daemon_target()]
        display = " ".join(argv)
    ok, detail = _run_daemon_service_command(argv, display=display)
    if not ok:
        return False, detail
    quiescent, quiescence_detail = _wait_for_managed_daemon_quiescence(
        lifecycle.manager, prog,
    )
    return quiescent, f"{detail}; {quiescence_detail}"


def _restore_managed_update_files(
    prog: config.VenvProgram,
    lifecycle: _ManagedDaemonUpdate,
) -> tuple[bool, str]:
    """Restore the exact clean checkout and serving venv captured at begin."""
    if (
        lifecycle.pre_source_sha is None
        or lifecycle.pre_source_tree_sha256 is None
    ):
        return False, "pre-update checkout/package identity is incomplete"
    quiescent, quiescence_detail = _stop_managed_daemon_for_restore(
        prog, lifecycle,
    )
    if not quiescent:
        return False, "could not quiesce daemon for rollback: " + quiescence_detail

    lifecycle.receipt_phase = "restoring_old"
    try:
        _persist_managed_update_receipt(prog, lifecycle)
    except AdminError as exc:
        return False, f"could not persist old-restore intent: {exc}"

    backup = lifecycle.venv_backup
    if lifecycle.backup_moved:
        if backup is None:
            return False, "rollback receipt says the venv moved but has no path"
        try:
            backup_info = backup.lstat()
        except OSError as exc:
            return False, f"rollback venv backup is unavailable at {backup}: {exc}"
        if (
            not stat.S_ISDIR(backup_info.st_mode)
            or backup_info.st_uid != lifecycle.owner_uid
        ):
            return False, f"rollback venv backup is unsafe at {backup}"
        quarantine: Path | None = None
        try:
            if lifecycle.venv_path.exists():
                quarantine = backup.parent / (
                    f".{lifecycle.venv_path.name}.vq-admin-failed-"
                    f"{lifecycle.transaction_id}"
                )
                if quarantine.exists() or quarantine.is_symlink():
                    return False, (
                        "exact failed-target quarantine already exists: "
                        f"{quarantine}"
                    )
                os.replace(lifecycle.venv_path, quarantine)
            os.replace(backup, lifecycle.venv_path)
            _fsync_directory_path(lifecycle.venv_path.parent)
            if backup.parent != lifecycle.venv_path.parent:
                _fsync_directory_path(backup.parent)
            lifecycle.backup_moved = False
        except OSError as exc:
            if (
                quarantine is not None
                and quarantine.exists()
                and not lifecycle.venv_path.exists()
            ):
                with contextlib.suppress(OSError):
                    os.replace(quarantine, lifecycle.venv_path)
            return False, f"could not atomically restore rollback venv: {exc}"
    restore_rc, restore_output = _restore_checkout_state(
        Path(prog.git_dir),
        sha=lifecycle.pre_source_sha,
        branch=lifecycle.pre_checkout_branch,
    )
    status_rc, status_output = _run_git_status_porcelain(Path(prog.git_dir))
    if (
        restore_rc != 0
        or status_rc != 0
        or status_output.strip()
        or not _checkout_state_matches(
            Path(prog.git_dir),
            sha=lifecycle.pre_source_sha,
            branch=lifecycle.pre_checkout_branch,
        )
    ):
        return False, (
            "could not restore the exact clean pre-update checkout: "
            + restore_output[-1000:]
        )

    installed_tree = _installed_tree_digest(str(lifecycle.venv_path / "bin" / "python"))
    if installed_tree != lifecycle.pre_source_tree_sha256:
        return False, (
            "restored vq package tree does not match the pre-update digest: "
            f"got {installed_tree or 'missing'}, expected "
            f"{lifecycle.pre_source_tree_sha256}"
        )
    # The files are exact again, but the old daemon has not yet been started
    # and proven through its RPC identity.  Keep that distinction durable so
    # a SIGKILL here is replayed through the service start/verification path
    # instead of being mistaken for a terminal rollback.
    lifecycle.receipt_phase = "files_restored"
    lifecycle.backup_moved = False
    try:
        _persist_managed_update_receipt(prog, lifecycle)
    except AdminError as exc:
        return False, f"could not persist restored-files state: {exc}"
    quarantine_parent = (
        lifecycle.venv_backup.parent
        if lifecycle.venv_backup is not None
        else lifecycle.venv_path.parent
    )
    quarantine = quarantine_parent / (
        f".{lifecycle.venv_path.name}.vq-admin-failed-{lifecycle.transaction_id}"
    )
    if quarantine.exists() or quarantine.is_symlink():
        try:
            info = quarantine.lstat()
            if (
                not stat.S_ISDIR(info.st_mode)
                or stat.S_ISLNK(info.st_mode)
                or info.st_uid != lifecycle.owner_uid
            ):
                return False, f"unsafe failed-target quarantine {quarantine}"
            shutil.rmtree(quarantine)
            _fsync_directory_path(quarantine.parent)
        except OSError as exc:
            return False, (
                "old files restored but failed-target cleanup remains for "
                f"recover-update at {quarantine}: {exc}"
            )
    return True, "clean checkout and previous virtualenv restored"


def _fsync_directory_path(path: Path) -> None:
    fd = os.open(str(path), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _commit_managed_update_files(
    prog: config.VenvProgram,
    lifecycle: _ManagedDaemonUpdate,
) -> tuple[bool, str]:
    backup = lifecycle.venv_backup
    if not lifecycle.backup_moved:
        lifecycle.terminal_verified = True
        return True, "rollback virtualenv already committed"
    if backup is None:
        return False, "rollback receipt says the venv moved but has no path"
    try:
        info = backup.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != lifecycle.owner_uid:
            return False, f"refusing unsafe rollback virtualenv {backup}"
        lifecycle.receipt_phase = "target_committed"
        _persist_managed_update_receipt(prog, lifecycle)
        cleanup = backup.parent / (
            f".{lifecycle.venv_path.name}.vq-admin-committed-"
            f"{lifecycle.transaction_id}"
        )
        if cleanup.exists() or cleanup.is_symlink():
            return False, f"exact committed-backup cleanup path exists: {cleanup}"
        os.replace(backup, cleanup)
        _fsync_directory_path(backup.parent)
    except OSError as exc:
        return False, f"could not commit verified rollback virtualenv {backup}: {exc}"
    lifecycle.backup_moved = False
    lifecycle.terminal_verified = True
    lifecycle.receipt_phase = "target_cleanup_pending"
    try:
        _persist_managed_update_receipt(prog, lifecycle)
    except AdminError as exc:
        # The rollback tree is already atomically disarmed.  The preceding
        # target_committed receipt plus exact path absence is sufficient for
        # recovery to infer this boundary, so never attempt to put a possibly
        # partial backup over the verified target.
        return True, (
            "rollback virtualenv committed; durable cleanup checkpoint "
            f"failed and requires recover-update: {exc}"
        )
    try:
        shutil.rmtree(cleanup)
        _fsync_directory_path(cleanup.parent)
    except OSError as exc:
        # The verified new daemon is already serving exact target bytes and the
        # rollback candidate has been atomically disarmed. Cleanup is now a
        # recoverable disk-hygiene concern, never a reason to replace the good
        # environment with a possibly partially deleted backup.
        return True, (
            f"rollback virtualenv committed; cleanup retained at {cleanup} "
            f"for recover-update: {exc}"
        )
    lifecycle.receipt_phase = "target_committed"
    try:
        _persist_managed_update_receipt(prog, lifecycle)
    except AdminError as exc:
        return True, (
            "rollback virtualenv committed and removed; terminal receipt "
            f"checkpoint requires recover-update: {exc}"
        )
    return True, "rollback virtualenv committed and removed"


def _managed_update_backup_path(
    prog: config.VenvProgram,
    venv_path: Path,
    transaction_id: str,
) -> Path:
    """Derive the sole rollback path authorized for this transaction."""
    if re.fullmatch(r"[0-9a-f]{32}", transaction_id) is None:
        raise AdminError("managed transaction_id must be 32 lowercase hex characters")
    checkout_root = _canonical_lifecycle_checkout(Path(prog.git_dir))
    canonical_venv = _canonical_future_lifecycle_path(venv_path)
    try:
        canonical_venv.relative_to(checkout_root)
    except ValueError:
        backup_parent = canonical_venv.parent
    else:
        backup_parent = checkout_root.parent
    return backup_parent / (
        f".{canonical_venv.name}.vq-admin-backup-{transaction_id}"
    )


def _begin_managed_daemon_update(
    prog: config.VenvProgram,
    probe: _SelfUpdateProbe,
    *,
    env: str = "vibeqc-queue",
) -> _ManagedDaemonUpdate:
    """Stop the proven serving daemon before checkout/install mutation."""
    if not probe.is_self_update or not probe.manager_available:
        raise AdminError(
            "cannot begin managed daemon update without an exact supported "
            f"service identity [{probe.diagnostic}]"
        )
    try:
        manager = _DaemonServiceManager(str(probe.service_manager))
    except ValueError as exc:
        raise AdminError(
            f"unsupported daemon service manager {probe.service_manager!r}"
        ) from exc
    state = _query_daemon_service_state(manager)
    if not _service_executable_matches(
        state.executable, prog, manager=manager,
    ):
        raise AdminError(
            "serving daemon executable identity changed before update: "
            f"{state.diagnostic}"
        )
    if not state.command_identity:
        raise AdminError(
            "serving daemon command identity is unavailable before update: "
            f"{state.diagnostic}"
        )
    if (
        manager is _DaemonServiceManager.SYSTEMD
        and not _systemd_command_identity_is_vq_daemon(state.command_identity)
    ):
        raise AdminError(
            "serving daemon systemd command is not canonical before update"
        )
    pre_pid = state.pid
    was_running = bool(state.running or pre_pid is not None)
    pre_source_sha, pre_branch = _capture_checkout_state(Path(prog.git_dir))
    pre_tree = _installed_tree_digest(prog.python)
    if pre_source_sha is None or pre_tree is None:
        raise AdminError(
            "cannot begin vq self-update: exact pre-update checkout SHA and "
            "installed package digest are required before stopping the daemon"
        )
    expected_pre_tree = source_tree_sha256_at_git_commit(
        _vq_project_root_for_program(prog), pre_source_sha,
    )
    if pre_tree != expected_pre_tree:
        raise AdminError(
            "cannot begin vq self-update: installed package tree digest is not "
            "bound to the pre-update checkout identity"
        )
    venv_path = Path(prog.python).parent.parent
    direct_receipt = venv_path.parent / (
        f".{venv_path.name}.vq-venv-replacement.json"
    )
    if direct_receipt.exists() or direct_receipt.is_symlink():
        raise AdminError(
            "cannot begin managed self-update while a direct lifecycle "
            f"replacement needs recovery at {direct_receipt}; run the "
            "checkout's update.sh --skip-git --recreate-venv against this "
            "exact venv once to reconcile it, then retry"
        )
    status_rc, status_output = _run_git_status_porcelain(Path(prog.git_dir))
    if status_rc != 0 or status_output.strip():
        raise AdminError(
            "cannot begin vq self-update: the checkout must be provably clean "
            "before failure-atomic rollback is armed"
        )
    if venv_path.is_symlink() or not (venv_path / "pyvenv.cfg").is_file():
        raise AdminError(
            f"cannot snapshot unverified serving virtualenv {venv_path}"
        )
    try:
        venv_info = venv_path.lstat()
    except OSError as exc:
        raise AdminError(
            f"cannot stat serving virtualenv {venv_path}: {exc}"
        ) from exc
    if not stat.S_ISDIR(venv_info.st_mode):
        raise AdminError(f"serving virtualenv is not a real directory: {venv_path}")
    checkout_root = _canonical_lifecycle_checkout(Path(prog.git_dir))
    canonical_venv = _canonical_future_lifecycle_path(venv_path)
    try:
        canonical_venv.relative_to(checkout_root)
    except ValueError:
        backup_parent = canonical_venv.parent
    else:
        # Keep the rollback environment outside the Git worktree: immutable
        # selector cleanup uses ``git clean -fd`` and must never be able to
        # remove the only old serving environment.
        backup_parent = checkout_root.parent
    if backup_parent.stat().st_dev != venv_path.stat().st_dev:
        raise AdminError(
            "managed venv rollback path is not on the serving venv's filesystem"
        )
    transaction_id = uuid.uuid4().hex
    backup = _managed_update_backup_path(prog, venv_path, transaction_id)
    if backup.exists() or backup.is_symlink():
        raise AdminError(f"managed rollback path already exists: {backup}")
    # Git creates this lock before changing the index or worktree.  A stale
    # lock therefore makes both the forward checkout and rollback checkout
    # fail.  Reject it at the last read-only boundary, before the durable
    # receipt and service stop, so recovery never depends on the same blocked
    # Git mutation that caused the update to fail (#236).
    _guard_managed_git_environment()
    _guard_git_index_unlocked(Path(prog.git_dir))
    lifecycle = _ManagedDaemonUpdate(
        manager=manager,
        env=env,
        pre_pid=pre_pid,
        was_running=was_running,
        was_stopped=False,
        pre_source_sha=pre_source_sha,
        pre_source_tree_sha256=pre_tree,
        pre_checkout_branch=pre_branch,
        venv_path=venv_path,
        venv_backup=backup,
        service_executable=state.executable,
        owner_uid=venv_info.st_uid,
        service_command=state.command_identity,
        transaction_id=transaction_id,
    )

    # The durable receipt precedes the first service or filesystem mutation.
    # A SIGKILL after this point is recovered by exact paths/identities, never
    # by scanning for a plausible-looking backup.
    _persist_managed_update_receipt(prog, lifecycle)

    try:
        if manager is _DaemonServiceManager.SYSTEMD:
            argv = ["systemctl", "--user", "stop", "vq-daemon"]
            display = "systemctl --user stop vq-daemon"
        else:
            plist = Path.home() / "Library" / "LaunchAgents" / (
                f"{LAUNCHD_DAEMON_LABEL}.plist"
            )
            if not _launchd_plist_matches_venv(venv_path):
                raise AdminError(
                    "launchd owns the serving daemon but its secure plist no "
                    f"longer names the managed venv at {plist}"
                )
            argv = ["launchctl", "bootout", _launchd_daemon_target()]
            display = " ".join(argv)
        ok, detail = _run_daemon_service_command(argv, display=display)
        if not ok:
            raise AdminError(
                f"could not stop the serving vq daemon before update: {detail}"
            )
        lifecycle.was_stopped = True
        quiescent, quiescence_detail = _wait_for_managed_daemon_quiescence(
            manager, prog,
        )
        if not quiescent:
            raise AdminError(
                "serving vq daemon quiescence could not be proved after the "
                f"stop command: {quiescence_detail}"
            )
        state_after_stop = _query_daemon_service_state(manager)
        if manager is _DaemonServiceManager.SYSTEMD:
            executable_after_stop = state_after_stop.executable
        else:
            executable_after_stop = _launchd_plist_executable()
        if not _service_executable_matches(
            executable_after_stop, prog, manager=manager,
        ):
            raise AdminError(
                "serving daemon definition changed during stop; refusing to "
                "move its virtualenv"
            )
        if not _service_command_identities_match(
            manager,
            state_after_stop.command_identity,
            lifecycle.service_command,
        ):
            raise AdminError(
                "serving daemon command changed during stop; refusing to move "
                "its virtualenv"
            )

        # The service is now authoritatively quiescent.  A same-parent rename
        # is atomic and preserves the exact old environment for outer rollback;
        # the canonical update script will build a replacement at the original
        # absolute path so console-script shebangs remain valid.
        os.replace(venv_path, backup)
        _fsync_directory_path(venv_path.parent)
        if backup.parent != venv_path.parent:
            _fsync_directory_path(backup.parent)
        lifecycle.backup_moved = True
        lifecycle.receipt_phase = "backup_moved"
        _persist_managed_update_receipt(prog, lifecycle)
    except BaseException as exc:
        # A stop operation can take effect and still time out or return an
        # error.  Reconcile the old service here because the caller cannot own
        # ``lifecycle`` until this function returns.  The checkout and venv
        # have not been mutated yet, so a strict old-identity RPC is enough
        # when the service is still running; otherwise start it explicitly.
        current = _query_daemon_service_state(manager)
        recovered = False
        recovery_detail = current.diagnostic
        if lifecycle.backup_moved:
            recovered, recovery_detail = _recover_managed_daemon_after_exception(
                prog, lifecycle,
            )
        elif (
            _service_executable_matches(
                current.executable, prog, manager=manager,
            )
            and current.running is True
            and current.pid is not None
        ):
            recovered, recovery_detail = _verify_preupdate_daemon_identity(
                lifecycle,
            )
        else:
            started, start_detail = _start_managed_daemon_update(lifecycle)
            if started:
                recovered, verify_detail = _verify_preupdate_daemon_identity(
                    lifecycle,
                )
                recovery_detail = f"{start_detail}; {verify_detail}"
            else:
                recovery_detail = start_detail
        if recovered:
            lifecycle.terminal_verified = True
            # The caller has not yet resumed its durable pause scope. Keep the
            # terminal receipt until normal marker clear or recover-update.
        else:
            retained = lifecycle.venv_backup if lifecycle.backup_moved else venv_path
            recovery_detail += f"; rollback environment retained at {retained}"
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            output.run_log_write(
                "# managed daemon recovery after interrupted stop: "
                + recovery_detail
            )
            raise
        raise AdminError(f"{exc}; pre-update service recovery: {recovery_detail}") \
            from exc
    return lifecycle


def _start_managed_daemon_update(
    lifecycle: _ManagedDaemonUpdate,
) -> tuple[bool, str]:
    """Start a service stopped by :func:`_begin_managed_daemon_update`."""
    identity_ok, identity_detail = _reattest_service_before_start(lifecycle)
    if not identity_ok:
        return False, identity_detail
    commands: list[tuple[list[str], str]]
    if lifecycle.manager is _DaemonServiceManager.SYSTEMD:
        argv = ["systemctl", "--user", "start", "vq-daemon"]
        commands = [(argv, "systemctl --user start vq-daemon")]
    else:
        rc, raw = _query_launchd_daemon()
        lower = raw.lower()
        launchd_absent = (
            rc in {3, 113}
            or "could not find service" in lower
            or "not found" in lower
        )
        if rc != 0 and not launchd_absent:
            return False, (
                "cannot determine whether the launchd agent is loaded before "
                f"restart (rc={rc}): {raw[:300]}"
            )
    if (
        lifecycle.manager is _DaemonServiceManager.LAUNCHD
        and launchd_absent
    ):
        plist = Path.home() / "Library" / "LaunchAgents" / (
            f"{LAUNCHD_DAEMON_LABEL}.plist"
        )
        commands = [
            (
                ["launchctl", "bootstrap", f"gui/{os.getuid()}", str(plist)],
                f"launchctl bootstrap gui/{os.getuid()} {plist}",
            ),
            (
                ["launchctl", "enable", _launchd_daemon_target()],
                f"launchctl enable {_launchd_daemon_target()}",
            ),
            (
                ["launchctl", "kickstart", "-k", _launchd_daemon_target()],
                f"launchctl kickstart -k {_launchd_daemon_target()}",
            ),
        ]
    elif lifecycle.manager is _DaemonServiceManager.LAUNCHD:
        argv = ["launchctl", "kickstart", "-k", _launchd_daemon_target()]
        commands = [(argv, " ".join(argv))]
    messages: list[str] = []
    for argv, display in commands:
        ok, detail = _run_daemon_service_command(argv, display=display)
        messages.append(detail)
        if not ok:
            return False, "; ".join(messages)
    post_state = _query_daemon_service_state(lifecycle.manager)
    if not _lifecycle_service_executable_matches(
        post_state.executable, lifecycle,
    ):
        messages.append(
            "service executable identity changed after start: "
            + post_state.diagnostic
        )
        return False, "; ".join(messages)
    post_pid = post_state.pid
    messages.append(
        f"PID {lifecycle.pre_pid or '?'} -> {post_pid or '?'}"
    )
    return True, "; ".join(messages)


def _execstart_matches_vq(daemon_path: str, venv_bin: Path) -> bool:
    """Require the direct or symlinked ``<venv>/bin/vq`` executable."""
    daemon = Path(daemon_path)
    target = venv_bin / "vq"
    try:
        return daemon.resolve(strict=False) == target.resolve(strict=False)
    except OSError:
        return False


def _execstart_matches_venv(daemon_path: str, venv_bin: Path) -> bool:
    """Compatibility detector for supported systemd and launchd programs.

    Current services run ``bin/vq`` (possibly through a user-local symlink).
    Legacy launchd agents also use ``bin/python -m vq daemon run``. Mutation
    paths additionally validate the selected manager and launchd argv.
    """
    if _execstart_matches_vq(daemon_path, venv_bin):
        return True
    try:
        candidate = Path(daemon_path)
        return candidate.name == "python" and candidate.parent.resolve() == venv_bin
    except OSError:
        return False


def _detect_vq_self_update(prog: config.VenvProgram) -> _SelfUpdateProbe:
    """v0.5.42: probe whether ``prog`` is the venv vq-daemon was
    launched from.

    macOS selects launchd and Linux selects user systemd. The loaded service's
    executable is compared with the managed venv. If no manager can be queried,
    ``sys.executable`` is only a fail-closed self-update hint: it can trigger an
    actionable failure, but never authorizes a restart or successful update.
    """
    # v0.5.43: .parent BEFORE .resolve() — the venv's bin/python is a
    # symlink to the system interpreter; .resolve() on the file would
    # dereference it out of the venv and parent would be /usr/bin.
    # Taking .parent first keeps us in the venv's bin dir; .resolve()
    # then handles any directory-level symlinks normally.
    venv_bin = Path(prog.python).parent.resolve()
    manager = _select_daemon_service_manager()
    if manager is None:
        sys_bin = Path(sys.executable).parent.resolve()
        is_self = sys_bin == venv_bin
        return _SelfUpdateProbe(
            is_self_update=is_self,
            daemon_running=None,
            service_manager=None,
            manager_available=False,
            diagnostic=(
                f"no supported user service manager; fallback to "
                f"sys.executable={sys.executable!r} "
                f"vs venv_bin={str(venv_bin)!r} → "
                f"{'match' if is_self else 'no match'}"
            ),
        )
    state = _query_daemon_service_state(manager)
    if state.executable is None:
        sys_bin = Path(sys.executable).parent.resolve()
        is_self = sys_bin == venv_bin
        return _SelfUpdateProbe(
            is_self_update=is_self,
            daemon_running=state.running,
            service_manager=manager.value,
            manager_available=False,
            diagnostic=f"{state.diagnostic}; executable provenance unavailable",
        )
    is_self = _service_executable_matches(
        state.executable, prog, manager=manager,
    )
    return _SelfUpdateProbe(
        is_self_update=is_self,
        daemon_running=state.running,
        service_manager=manager.value,
        manager_available=True,
        diagnostic=(
            f"{state.diagnostic}; env venv_bin={str(venv_bin)!r}; "
            f"→ {'self-update' if is_self else 'different venv'}"
        ),
    )


def resolve_vq_self_update_target(
    cfg: config.Config,
) -> tuple[str, config.VenvProgram, _SelfUpdateProbe]:
    """Resolve the one configured venv serving this user's vq daemon.

    The first-class self-update command intentionally takes no environment or
    host argument. The loaded service-manager executable is the authority, so
    an operator cannot accidentally select a scheduler alias or a chemistry
    runtime that merely happens to share the monorepo checkout.
    """
    matches: list[tuple[str, config.VenvProgram, _SelfUpdateProbe]] = []
    diagnostics: list[str] = []
    for name, candidate in sorted(cfg.programs.items()):
        if not isinstance(candidate, config.VenvProgram):
            continue
        probe = _detect_vq_self_update(candidate)
        diagnostics.append(f"{name}: {probe.diagnostic}")
        if probe.is_self_update:
            matches.append((name, candidate, probe))
    if not matches:
        detail = "; ".join(diagnostics) or "no venv programs are configured"
        raise AdminError(
            "no configured venv matches the current vq daemon; " + detail
        )
    if len(matches) > 1:
        names = ", ".join(name for name, _prog, _probe in matches)
        raise AdminError(
            f"multiple configured venvs match the current vq daemon: {names}"
        )
    name, prog, probe = matches[0]
    if not probe.manager_available or probe.service_manager is None:
        raise AdminError(
            "the matching vq environment has no verified supported daemon "
            f"restart path [{probe.diagnostic}]"
        )
    if prog.runtime_slot_root is not None:
        raise AdminError(
            "the serving vq environment uses runtime_slot_root; first-class "
            "self-update requires the canonical serving virtualenv "
            "transaction and refuses before report fetch or ref mutation"
        )
    return name, prog, probe


DAEMON_RESTART_TIMEOUT_SECONDS = 120
DAEMON_HEALTH_TIMEOUT_SECONDS = 60
"""Base post-restart RPC readiness window. Enough for an idle daemon;
:func:`_daemon_health_timeout` scales it up with the state dir size."""

DAEMON_HEALTH_SECONDS_PER_SPEC = 0.025
"""Readiness allowance per queued job spec. The daemon's startup resume
pass reads every ``queue/*.json`` before RPC answers, so a flat window
that is generous for an idle host is far too short for a driver
carrying thousands of jobs. In #53 a 21,683-spec queue took more than
242 s cold and 210 s warm under load. The old 30 s + 10 ms/spec
allowance expired at 247 s and rolled back a valid install. A 60 s
base plus 25 ms/spec gives that queue the full 600 s ceiling, with
headroom over the measured lower bound rather than assuming a cold
scan runs at the warm rate. This is a bounded allowance, not a startup
duration guarantee; operators can still override it. The poll returns
as soon as exact readiness is verified."""

DAEMON_HEALTH_TIMEOUT_MAX_SECONDS = 600
"""Ceiling on the scaled readiness window so a corrupt or gigantic
state dir cannot stretch the verification poll unboundedly."""


def _daemon_health_timeout() -> float:
    """Post-restart RPC readiness window in seconds.

    Honors ``VQ_DAEMON_HEALTH_TIMEOUT`` (seconds, must be > 0) as an
    operator override, read at call time like the other admin timeout
    knobs. Otherwise scales the base window with the number of job
    specs in the single-user queue dir — the population the restarted
    daemon must scan before its RPC socket answers — capped at
    :data:`DAEMON_HEALTH_TIMEOUT_MAX_SECONDS`.
    """
    raw = os.environ.get("VQ_DAEMON_HEALTH_TIMEOUT", "").strip()
    if raw:
        try:
            val = float(raw)
        except ValueError:
            val = 0.0
        if math.isfinite(val) and val > 0:
            return val
    base = float(DAEMON_HEALTH_TIMEOUT_SECONDS)
    try:
        spec_count = sum(1 for _ in paths.queue_dir().glob("*.json"))
    except OSError:
        return base
    scaled = base + spec_count * DAEMON_HEALTH_SECONDS_PER_SPEC
    return min(scaled, float(DAEMON_HEALTH_TIMEOUT_MAX_SECONDS))


def _restart_vq_daemon(
    *, manager: _DaemonServiceManager = _DaemonServiceManager.SYSTEMD,
    pre_pid: int | None,
) -> tuple[bool, str]:
    """Restart vq-daemon through its selected user service manager."""
    if manager is _DaemonServiceManager.LAUNCHD:
        command = ["launchctl", "kickstart", "-k", _launchd_daemon_target()]
        display = " ".join(command)
    else:
        command = ["systemctl", "--user", "restart", "vq-daemon"]
        display = "systemctl --user restart vq-daemon"
    try:
        proc = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=DAEMON_RESTART_TIMEOUT_SECONDS,
            stdin=subprocess.DEVNULL,
        )
    except subprocess.TimeoutExpired:
        return False, (
            f"{display} timed out after {DAEMON_RESTART_TIMEOUT_SECONDS}s. "
            "See operations.md for service recovery."
        )
    except OSError as exc:
        return False, f"failed to invoke {manager.value}: {exc}"
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        return False, (
            f"{display} failed rc={proc.returncode}: "
            f"{stderr or '(no stderr)'}. See operations.md for service recovery."
        )
    post_pid = _query_daemon_service_pid(manager)
    if pre_pid is None and post_pid is None:
        transition = "PID unknown"
    elif pre_pid is None:
        transition = f"PID -> {post_pid}"
    elif post_pid is None:
        transition = f"PID {pre_pid} -> ? (daemon not reporting MainPID)"
    else:
        transition = f"PID {pre_pid} -> {post_pid}"
    return True, f"{display} ... done ({transition})"


def _installed_tree_digest(python: str) -> str | None:
    """Digest the ``vq`` package that ``python`` actually imports.

    Asked of the target interpreter rather than computed in this process,
    because on a multi-user host the admin CLI and the daemon are deliberately
    two different installs. Mirrors how the scheduler-helper lane asks the
    remote for its own ``vq source-tree-sha256``
    (:func:`_verify_scheduler_helper_provenance`).

    Returns ``None`` on any failure. A venv that cannot run ``vq`` mid-update is
    "no digest expectation available", never a verification failure -- the
    caller falls back to the SHA comparison.
    """
    try:
        proc = subprocess.run(
            [python, "-m", "vq", "source-tree-sha256"],
            capture_output=True,
            text=True,
            timeout=60,
            # `-m` prepends the CWD to sys.path. An admin update runs from
            # wherever the operator happened to be, and a `vq/` directory there
            # -- the checkout's own `src/`, most obviously -- would be imported
            # instead of the interpreter's install, digesting the wrong tree
            # and producing an expectation the daemon can never match.
            cwd="/",
            stdin=subprocess.DEVNULL,
        )
    except (subprocess.TimeoutExpired, OSError):
        return None
    if proc.returncode != 0:
        return None
    digest = proc.stdout.strip().lower()
    return digest if _SHA256_RE.fullmatch(digest) else None


@dataclass
class DaemonProvenance:
    """Outcome of the post-restart provenance probe."""

    verified: bool
    actual_sha: str | None
    actual_tree_sha256: str | None
    detail: str


def _verify_restarted_daemon(
    expected_source_sha: str,
    *,
    expected_tree_sha256: str | None = None,
    require_exact_identity: bool = False,
) -> DaemonProvenance:
    """Wait for daemon RPC and require exact startup source provenance.

    Always pings the PER-USER daemon, deliberately ignoring the host's
    multi-user mode: the restart lane above only ever restarts the user
    daemon (``systemctl --user`` / the launchd user domain), so the user
    daemon is the one whose provenance proves the restart landed. On a host
    with ``[multi_user] enabled=true`` in ``/etc/vq/config.toml`` (compute-a),
    routing this ping by the multi-user flag sent it to the root system
    daemon — a non-git install whose ping carries no ``source_sha`` — so
    verification could never pass and every update needed ``mark-ok``,
    while the daemon actually restarted was reporting the right SHA on the
    single-user socket the whole time.

    Passing ``multi_user=False`` was not enough to express that (workstation,
    2026-08-02). ``socket_path(multi_user=False)`` resolves through
    ``$VQ_STATE_DIR``, and admins on a multi-user host are documented to run
    admin verbs as ``VQ_STATE_DIR=/var/lib/vq vq admin ...`` — so the
    "single-user" socket became ``/var/lib/vq/daemon.sock``, the root daemon
    again. Unlike compute-a, workstation's root daemon *does* carry a ``source_sha``
    (a ``/opt/vq`` ``SOURCE-SHA`` marker), so the misroute did not fail as
    "missing" — it failed as a confident mismatch naming the root daemon's
    commit, which reads exactly like a user daemon frozen one release back.
    Hence :func:`rpc.ping_user_daemon`, plus the ``multi_user`` assertion
    below: a daemon that says it is the multi-user one can never be the
    daemon this lane restarted, whatever socket carried the answer.
    """
    from vq import rpc  # local import avoids the module-level admin/RPC cycle

    window = _daemon_health_timeout()
    deadline = time.monotonic() + window
    no_response = (
        f"daemon RPC did not respond within {window:.0f}s. A daemon on a "
        "large state dir scans every queued spec before RPC answers; "
        "raise VQ_DAEMON_HEALTH_TIMEOUT if this window is still too short"
    )
    last_detail = no_response
    actual_sha: str | None = None
    actual_tree: str | None = None
    expected_tree = (
        expected_tree_sha256.lower() if expected_tree_sha256 else None
    )
    while time.monotonic() < deadline:
        envelope = rpc.ping_user_daemon()
        if envelope is None:
            last_detail = no_response
        elif envelope.get("multi_user"):
            # Answered by the root system daemon, which this lane never
            # restarts. Its provenance says nothing about the user daemon, so
            # treat it as a routing fault rather than a provenance mismatch --
            # comparing SHAs here is what made workstation look frozen.
            last_detail = (
                f"provenance ping reached the multi-user daemon at "
                f"{rpc.user_socket_path()}, not the user daemon this update "
                "restarted. Re-run without VQ_STATE_DIR pointing at the "
                "multi-user root"
            )
        else:
            raw_sha = envelope.get("source_sha")
            actual_sha = str(raw_sha).lower() if raw_sha else None
            raw_tree = envelope.get("source_tree_sha256")
            actual_tree = str(raw_tree).lower() if raw_tree else None
            if require_exact_identity:
                if expected_tree is None:
                    last_detail = (
                        "strict daemon provenance has no independently derived "
                        "target tree digest"
                    )
                elif (
                    actual_sha == expected_source_sha.lower()
                    and actual_tree == expected_tree
                ):
                    return DaemonProvenance(
                        verified=True,
                        actual_sha=actual_sha,
                        actual_tree_sha256=actual_tree,
                        detail=(
                            f"RPC healthy; exact source SHA {actual_sha} and "
                            f"tree {actual_tree[:12]} verified"
                        ),
                    )
                else:
                    last_detail = (
                        f"RPC strict identity mismatch: source SHA "
                        f"{actual_sha or 'missing'} (expected "
                        f"{expected_source_sha.lower()}), source tree "
                        f"{actual_tree or 'missing'} (expected {expected_tree})"
                    )
                time.sleep(0.1)
                continue
            if actual_sha == expected_source_sha.lower():
                # Deliberately NOT gated on the digest as well. The digest can
                # rescue a verification the SHA comparison would have failed;
                # it cannot fail one the SHA passes. Making it authoritative
                # would tighten *availability*, not precedence -- any host
                # whose admin CLI and daemon resolve different installs would
                # start failing updates that are correct today, and on the
                # multi-user hosts that is the designed arrangement. See
                # docs/operations.md; the asymmetry is intentional.
                return DaemonProvenance(
                    verified=True,
                    actual_sha=actual_sha,
                    actual_tree_sha256=actual_tree,
                    detail=f"RPC healthy; source SHA {actual_sha} verified",
                )
            if (
                expected_tree is not None
                and actual_tree is not None
                and actual_tree == expected_tree
            ):
                # The daemon loaded exactly the bytes this update installed.
                # The SHA strings disagree because they are drawn from
                # different objects: the expectation is the checkout's
                # `git rev-parse HEAD`, while the daemon reports whatever its
                # installed package declares -- a SOURCE-SHA marker, or, for a
                # non-editable install sitting inside an unrelated work tree,
                # that tree's HEAD. Neither of those is a statement about these
                # bytes; the digest is. Verify on the digest and say plainly
                # why the commit names differ, instead of failing an update
                # whose code is provably correct.
                return DaemonProvenance(
                    verified=True,
                    actual_sha=actual_sha,
                    actual_tree_sha256=actual_tree,
                    detail=(
                        f"RPC healthy; source tree {actual_tree[:12]} verified "
                        "against the installed package, so the daemon is "
                        "running the expected code. Its declared source SHA "
                        f"{actual_sha or 'missing'} does not match the "
                        f"checkout's {expected_source_sha.lower()[:12]}: the "
                        "declaration is wrong, not the code. On a "
                        "non-editable install that means a stale SOURCE-SHA "
                        "marker -- restamp with `vq source-sha --write-marker "
                        f"{expected_source_sha.lower()}` as the installing "
                        "user. On an editable install there is no marker in "
                        "play and the checkout moved after the daemon started; "
                        "do NOT write a marker there, just re-run the update"
                    ),
                )
            last_detail = (
                f"RPC source SHA {actual_sha or 'missing'} does not match "
                f"expected {expected_source_sha.lower()}"
                + _tree_mismatch_detail(expected_tree, actual_tree)
            )
        time.sleep(0.1)
    return DaemonProvenance(
        verified=False,
        actual_sha=actual_sha,
        actual_tree_sha256=actual_tree,
        detail=last_detail,
    )


def _tree_mismatch_detail(expected: str | None, actual: str | None) -> str:
    """Say which of "stale code" or "stale marker" a failed probe found.

    Without this the operator sees two commit names and no way to tell whether
    the daemon is running old code or merely misdescribing current code. That
    ambiguity is what turned the workstation probe on 2026-08-02 into a handoff
    instead of a diagnosis: checkout, editable install and unit were all
    provably correct and the daemon PID was genuinely new.
    """
    if expected is None:
        return (
            "; no tree digest expectation available (the updated interpreter "
            "could not run `vq source-tree-sha256`), so this verdict rests on "
            "the declared SHA alone"
        )
    if actual is None:
        return (
            "; this daemon reports no tree digest, so its code could not be "
            "checked directly -- it predates the source_tree_sha256 ping key"
        )
    return (
        f"; the daemon's source tree {actual[:12]} also differs from the "
        f"installed {expected[:12]}, so this is stale CODE, not a stale marker"
    )


def _complete_managed_daemon_update(
    prog: config.VenvProgram,
    result: UpdateResult,
    lifecycle: _ManagedDaemonUpdate,
) -> None:
    """Restart and strictly verify a daemon stopped by outer admin.

    On a failed on-disk transaction, verification is against the pre-update
    package identity after rollback.  On success it is against an independent
    digest of ``src/vq`` at the selected commit, never against whatever the
    target interpreter happens to import after the install.
    """
    transition_admin_update_state(ADMIN_UPDATE_STATE_RESTARTING_DAEMON)
    result.daemon_service_manager = lifecycle.manager.value
    update_landed = result.work_succeeded
    result.daemon_restart_attempted = True

    def fail_and_restore(message: str) -> None:
        result.daemon_restart_succeeded = False
        result.daemon_health_verified = False
        if message not in result.work_errors:
            result.work_errors.append(message)
        recovered, recovery_detail = _recover_managed_daemon_after_exception(
            prog, lifecycle,
        )
        result.daemon_restart_message = (
            f"{message}; pre-update recovery "
            f"{'verified' if recovered else 'FAILED'}: {recovery_detail}"
        )

    if not update_landed:
        fail_and_restore("on-disk update did not complete; target daemon not started")
        return
    expected_sha = result.expected_sha or current_source_sha(Path(prog.git_dir))
    expected_tree: str | None = None
    if expected_sha is not None:
        try:
            expected_tree = source_tree_sha256_at_git_commit(
                _vq_project_root_for_program(prog), expected_sha,
            )
        except AdminError as exc:
            fail_and_restore(f"could not derive target vq package digest: {exc}")
            return
    result.daemon_expected_source_sha = expected_sha
    result.daemon_expected_source_tree_sha256 = expected_tree
    if expected_sha is None or expected_tree is None:
        fail_and_restore(
            "exact target SHA and independently derived tree digest are required"
        )
        return
    installed_tree = _installed_tree_digest(prog.python)
    if installed_tree != expected_tree:
        fail_and_restore(
            "installed vq tree "
            f"{installed_tree or 'missing'} does not match independently "
            f"derived target {expected_tree}"
        )
        return
    lifecycle.target_source_sha = expected_sha
    lifecycle.target_source_tree_sha256 = expected_tree
    lifecycle.receipt_phase = "target_ready"
    try:
        _persist_managed_update_receipt(prog, lifecycle)
    except AdminError as exc:
        fail_and_restore(f"could not persist target update receipt: {exc}")
        return
    started, start_message = _start_managed_daemon_update(lifecycle)
    if not started:
        fail_and_restore("target daemon start failed: " + start_message)
        return
    provenance = _verify_restarted_daemon(
        expected_sha,
        expected_tree_sha256=expected_tree,
        require_exact_identity=True,
    )
    result.daemon_actual_source_sha = provenance.actual_sha
    result.daemon_actual_source_tree_sha256 = provenance.actual_tree_sha256
    if not provenance.verified:
        fail_and_restore("target daemon provenance failed: " + provenance.detail)
        return
    service_ok, service_detail = _reattest_service_before_start(lifecycle)
    if not service_ok:
        fail_and_restore(
            "target daemon service definition changed during RPC verification: "
            + service_detail
        )
        return
    lifecycle.receipt_phase = "target_verified"
    try:
        _persist_managed_update_receipt(prog, lifecycle)
    except AdminError as exc:
        fail_and_restore(f"could not persist verified target receipt: {exc}")
        return
    committed, commit_detail = _commit_managed_update_files(prog, lifecycle)
    if not committed:
        fail_and_restore(commit_detail)
        return
    result.daemon_health_verified = True
    result.daemon_restart_succeeded = True
    if lifecycle.receipt_phase == "target_cleanup_pending":
        result.work_errors.append(commit_detail)
    result.daemon_restart_message = (
        f"{start_message}; {provenance.detail}; {service_detail}; {commit_detail}"
    )


def _recover_managed_daemon_after_exception(
    prog: config.VenvProgram,
    lifecycle: _ManagedDaemonUpdate,
    *,
    clear_receipt: bool = True,
) -> tuple[bool, str]:
    """Restore exact old files and strictly verify the old daemon identity."""
    if (
        lifecycle.pre_source_sha is None
        or lifecycle.pre_source_tree_sha256 is None
    ):
        return False, (
            "pre-update daemon identity was incomplete; refusing an "
            "unverifiable restart after the update exception"
        )
    restored, restore_detail = _restore_managed_update_files(prog, lifecycle)
    if not restored:
        return False, restore_detail
    started, detail = _start_managed_daemon_update(lifecycle)
    if not started:
        return False, f"{restore_detail}; {detail}"
    provenance = _verify_restarted_daemon(
        lifecycle.pre_source_sha,
        expected_tree_sha256=lifecycle.pre_source_tree_sha256,
        require_exact_identity=True,
    )
    if not provenance.verified:
        return False, f"{restore_detail}; {detail}; {provenance.detail}"
    service_ok, service_detail = _reattest_service_before_start(lifecycle)
    if not service_ok:
        return False, (
            f"{restore_detail}; {detail}; {provenance.detail}; "
            f"{service_detail}"
        )
    lifecycle.receipt_phase = "old_restored"
    lifecycle.backup_moved = False
    try:
        _persist_managed_update_receipt(prog, lifecycle)
    except AdminError as exc:
        return False, (
            f"{restore_detail}; {detail}; {provenance.detail}; "
            f"{service_detail}; could not persist verified old-service "
            f"state: {exc}"
        )
    lifecycle.terminal_verified = True
    # Keep the terminal receipt until the caller has resumed its exact pause
    # scope. A SIGKILL between daemon verification and resume is then recovered
    # idempotently by ``vq admin recover-update``.
    return True, (
        f"{restore_detail}; {detail}; {provenance.detail}; {service_detail}"
    )


def _maybe_restart_daemon(
    prog: config.VenvProgram,
    result: UpdateResult,
    *,
    restart_daemon: bool,
    require_self_update: bool = False,
) -> None:
    """v0.5.42: at end of a successful ``update_env`` (or each env in
    ``update_all``), decide whether this was a vq self-update and, if
    so, restart the daemon. Mutates ``result`` in place to record the
    outcome.

    Skip-conditions (no restart attempted, ``daemon_restart_attempted``
    stays False):
      * ``restart_daemon=False`` (the caller passed ``--no-restart-daemon``)
      * The update itself didn't succeed (``result.success`` is False
        before we get here — never restart onto a half-installed
        package).
      * The probe says it's not a vq self-update (daemon was launched
        from a different venv, or systemctl is reachable but the unit
        is missing).
    Fail-conditions (restart attempted, ``daemon_restart_succeeded``
    set to False; ``result.success`` therefore turns False too):
      * Probe believes it IS a self-update but systemctl is unreachable
        — surfaces the recovery recipe.
      * ``systemctl --user restart`` returns non-zero / times out.
    """
    if not restart_daemon:
        result.daemon_restart_message = (
            "skipped: --no-restart-daemon set by caller"
        )
        return
    if not result.success:
        # Pre-restart success check: never restart onto a half-installed
        # package. Restarting now would put the daemon on whatever was
        # half-built, which is worse than running stale code.
        result.daemon_restart_message = (
            "skipped: update did not succeed (would not restart onto "
            "a half-installed package)"
        )
        return
    probe = _detect_vq_self_update(prog)
    if not probe.is_self_update:
        # Not vq's venv — leave the daemon alone. Stay quiet in the
        # formatter (no message line) to avoid noise on every
        # vibeqc-dev / vibeqc-release update.
        result.daemon_restart_message = (
            f"not a vq self-update — daemon untouched [{probe.diagnostic}]"
        )
        if require_self_update:
            result.work_errors.append(
                "required vq self-update lost daemon target identity before "
                "restart; refusing to report the on-disk mutation as complete"
            )
        return
    # It IS a vq self-update. Three paths from here:
    result.daemon_service_manager = probe.service_manager
    if not probe.manager_available or probe.service_manager is None:
        result.daemon_restart_attempted = True
        result.daemon_restart_succeeded = False
        result.daemon_health_verified = False
        result.daemon_restart_message = (
            "vq self-update detected but no verified supported service-manager "
            "path is available. See operations.md and docs/lifecycle.md for "
            f"service recovery [{probe.diagnostic}]"
        )
        return
    # v0.6.1: transition into RESTARTING_DAEMON ONLY when we're
    # about to actually issue the systemctl restart. Pre-v0.6.1
    # the transition fired positionally in update_env regardless of
    # whether _maybe_restart_daemon did anything — non-self-update
    # envs (vibeqc-dev, vibeqc-release) misleadingly went through
    # RESTARTING_DAEMON in the state banner. Now the state machine
    # accurately reflects what the code did.
    transition_admin_update_state(ADMIN_UPDATE_STATE_RESTARTING_DAEMON)
    manager = _DaemonServiceManager(probe.service_manager)
    result.daemon_restart_attempted = True
    pre_pid = _query_daemon_service_pid(manager)
    expected_sha = current_source_sha(Path(prog.git_dir))
    result.daemon_expected_source_sha = expected_sha
    # Asked of the just-updated interpreter, not of this process: on a
    # multi-user host the admin CLI and the daemon are two different installs
    # on purpose. None means "no digest expectation", which downgrades the
    # verdict to the SHA comparison rather than failing it.
    expected_tree = _installed_tree_digest(prog.python)
    result.daemon_expected_source_tree_sha256 = expected_tree
    if expected_sha is None:
        result.daemon_restart_succeeded = False
        result.daemon_health_verified = False
        result.daemon_restart_message = (
            "refusing daemon restart: updated checkout source SHA could not be "
            "determined for post-restart provenance verification"
        )
        return
    restarted, restart_message = _restart_vq_daemon(
        manager=manager,
        pre_pid=pre_pid,
    )
    if not restarted:
        result.daemon_restart_succeeded = False
        result.daemon_health_verified = False
        result.daemon_restart_message = restart_message
        return
    provenance = _verify_restarted_daemon(
        expected_sha,
        expected_tree_sha256=expected_tree,
    )
    result.daemon_actual_source_sha = provenance.actual_sha
    result.daemon_actual_source_tree_sha256 = provenance.actual_tree_sha256
    result.daemon_health_verified = provenance.verified
    result.daemon_restart_succeeded = provenance.verified
    result.daemon_restart_message = f"{restart_message}; {provenance.detail}"


def format_scheduler_update_result(result: SchedulerHostUpdateResult) -> str:
    """Human-readable summary for ``vq admin update <scheduler-host>``."""
    lines = [
        f"== admin update {result.host} (scheduler {result.mode}) ==",
        f"   ssh:           {result.ssh}",
        f"   scheduler:     {result.scheduler}",
    ]
    if result.command_ssh and result.command_ssh != result.ssh:
        lines.append(f"   update ssh:    {result.command_ssh}")
    if result.stage_path:
        lines.append(f"   source stage:  {result.stage_path}")
    lines += [
        f"   command:       {result.command}",
        "   source tree:   "
        f"{result.remote_source_tree_sha256 or '(unverified)'}",
        f"   source SHA:    {result.remote_source_sha or '(unverified)'}",
        "   helper ready:  "
        f"{'yes' if result.helper_readiness_verified else 'no'} "
        f"({len(result.helper_readiness_attempts)} probe(s))",
        f"   marker:        {'cleared' if result.marker_cleared else 'kept'}",
        "",
        f"-- scheduler {result.mode} command (rc={result.command_rc}) --",
        result.command_output.rstrip() or "(no output)",
    ]
    if result.active_jobs:
        lines += [
            "",
            "-- active scheduler jobs --",
            *(f"   {job}" for job in result.active_jobs),
        ]
    if result.work_errors:
        lines += [
            "",
            "-- work errors --",
            *(f"   {err}" for err in result.work_errors),
        ]
    if result.maintenance_warnings:
        lines += [
            "",
            "-- maintenance warnings --",
            *(f"   {warning}" for warning in result.maintenance_warnings),
        ]
    lines += [
        "",
        f"== {'OK' if result.success else 'FAILED'} ==",
    ]
    if not result.success:
        reasons: list[str] = []
        if result.active_jobs:
            reasons.append(f"{len(result.active_jobs)} active scheduler job(s)")
        if result.command_rc not in (None, 0):
            reasons.append(f"command rc={result.command_rc}")
        if result.work_errors:
            reasons.append(f"{len(result.work_errors)} work error(s)")
        if reasons:
            lines.append("   reasons: " + "; ".join(reasons))
    return "\n".join(lines)


def format_scheduler_update_result_json(
    result: SchedulerHostUpdateResult,
) -> str:
    payload = asdict(result)
    payload["success"] = result.success
    # The classification, on the success payload as well as the failure
    # object, so a caller reads one field in one place for every lane of
    # ``vq admin update`` -- see ``format_update_result_json``.
    payload["outcome"] = result.outcome
    return json.dumps(payload, indent=2, sort_keys=True)


def format_scheduler_runtime_update_result(
    result: SchedulerRuntimeUpdateResult,
) -> str:
    lines = [
        f"== admin update {result.program} {result.host} "
        f"(scheduler runtime {result.mode}) ==",
        f"   build ssh:     {result.command_ssh}",
        f"   verify ssh:    {result.verify_ssh}",
        f"   expected SHA:  {result.expected_sha}",
        f"   expected tag:  {result.expected_tag or '(none)'}",
        f"   actual SHA:    {result.actual_sha or '(unverified)'}",
        f"   actual tag:    {result.actual_tag or '(none)'}",
        f"   activation:    {result.activation or '(unverified)'}",
        f"   healthy:       {result.healthy}",
        f"   quiescent:     {result.quiescent}",
        f"   updater PID:   {result.updater_pid or '(none)'}",
        f"   active path:   {result.active_path or '(unverified)'}",
        f"   marker:        {'cleared' if result.marker_cleared else 'kept'}",
    ]
    # Only when a prepare_command is configured and ran. Rendered first
    # because it runs first: when it fails, the deploy and verify sections
    # below are legitimately empty and this is the only evidence there is.
    if result.prepare_rc is not None or result.prepare_output:
        lines += [
            "",
            f"-- prepare command (rc={result.prepare_rc}) --",
            result.prepare_output.rstrip() or "(no output)",
        ]
    lines += [
        "",
        f"-- deploy command (rc={result.command_rc}) --",
        result.command_output.rstrip() or "(no output)",
        "",
        f"-- verification command (rc={result.verify_rc}) --",
        result.verify_output.rstrip() or "(no output)",
    ]
    if result.active_jobs:
        lines += ["", "-- active scheduler jobs --"]
        lines.extend(f"   {job}" for job in result.active_jobs)
    if result.work_errors:
        lines += ["", "-- work errors --"]
        lines.extend(f"   {error}" for error in result.work_errors)
    lines += ["", f"== {'OK' if result.success else 'FAILED'} =="]
    return "\n".join(lines)


def format_scheduler_runtime_update_result_json(
    result: SchedulerRuntimeUpdateResult,
) -> str:
    payload = asdict(result)
    payload["success"] = result.success
    payload["outcome"] = result.outcome
    return json.dumps(payload, indent=2, sort_keys=True)


def format_scheduler_runtime_status_json(
    host: str, cfg: config.Config
) -> str:
    host_cfg = _resolve_scheduler_update_host(host, cfg)
    records = load_scheduler_runtime_status()
    deployments: dict[str, object] = {}
    for program, profile in sorted(host_cfg.scheduler_runtime_deployments.items()):
        record = records.get(f"{host}:{program}")
        if profile.update_allocation is not None:
            build_target = (
                f"{profile.update_allocation.scheduler}-allocation on "
                f"{host_cfg.ssh}"
            )
        else:
            build_target = profile.update_host or host_cfg.ssh
        deployments[program] = {
            "configured": True,
            "update_host": build_target,
            "last": asdict(record) if record is not None else None,
        }
    helper_record = records.get(f"{host}:{SCHEDULER_HELPER_RECORD_PROGRAM}")
    return json.dumps(
        {
            "scheduler_host": True,
            "host": host,
            "scheduler": host_cfg.scheduler,
            "scheduler_dialect": host_cfg.scheduler_dialect,
            "driver": host_cfg.scheduler_driver,
            "message": (
                "cluster provisioning is managed through "
                f"`vq admin update {host}`"
            ),
            "deployments": deployments,
            # Canonical helper LAST OK record: the rollout planner proves
            # "helper already at the accepted pin" from this, never from a
            # comparison against the live driver checkout.
            "helper": {
                "configured": host_cfg.scheduler_update_command is not None,
                "last": (
                    asdict(helper_record) if helper_record is not None else None
                ),
            },
            "marker": _marker_to_json_block(),
            "markers": _markers_to_json_blocks(),
        },
        indent=2,
        sort_keys=True,
    )


def format_scheduler_runtime_status(host: str, cfg: config.Config) -> str:
    host_cfg = _resolve_scheduler_update_host(host, cfg)
    records = load_scheduler_runtime_status()
    # Step 1 of the documented marker recovery is "`vq admin status HOST` shows
    # the marker banner". For a scheduler host it showed nothing: the JSON
    # sibling carried the marker block, this text renderer did not — so the
    # operator could not see the marker they were being told to inspect.
    banner = _format_marker_banner()
    lines = [
        *([banner] if banner else []),
        "(daemonless scheduler host; "
        f"scheduler={host_cfg.scheduler}; driver={host_cfg.scheduler_driver}; "
        "cluster provisioning is managed through "
        f"`vq admin update {host}`)",
        f"Scheduler runtime deployments: {host}",
        f"scheduler={host_cfg.scheduler} driver={host_cfg.scheduler_driver}",
    ]
    if not host_cfg.scheduler_runtime_deployments:
        lines.append("(none configured)")
        return "\n".join(lines)
    lines.append(
        "PROGRAM                 LAST UPDATED                 LAST OK  "
        "SHA          TAG       LAST GOOD"
    )
    for program in sorted(host_cfg.scheduler_runtime_deployments):
        record = records.get(f"{host}:{program}")
        if record is None:
            lines.append(
                f"{program:<23} {'never':<28} {'-':<8} {'-':<12} {'-':<9} -"
            )
            continue
        # LAST GOOD survives a failed attempt: it names the identity an
        # operator can redeploy to roll back, instead of leaving only '-'.
        if record.last_ok_sha:
            last_good = record.last_ok_sha[:12] + (
                f" ({record.last_ok_tag})" if record.last_ok_tag else ""
            )
        else:
            last_good = "-"
        lines.append(
            f"{program:<23} {record.last_updated_at:<28} "
            f"{str(record.last_success):<8} "
            f"{(record.actual_sha or '-')[:12]:<12} "
            f"{record.actual_tag or '-':<9} "
            f"{last_good}"
        )
    return "\n".join(lines)


def _took(seconds: float | None) -> str:
    """`, took 12m03s` for a measured phase, or "" for one that did not run.

    Minutes and seconds rather than a float: the question this answers is
    "was that three minutes or seventy", and nobody reads 4223.7 as 70
    minutes at a glance.
    """
    if seconds is None:
        return ""
    if seconds < 60:
        return f", took {seconds:.1f}s"
    return f", took {int(seconds) // 60}m{int(seconds) % 60:02d}s"


def _format_metrics(metrics: dict[str, str]) -> list[str]:
    """Render parsed VQ-DEPLOY-METRIC facts, or nothing when there are none."""
    if not metrics:
        return []
    return ["", "-- deploy metrics --"] + [
        f"   {key}: {metrics[key]}" for key in sorted(metrics)
    ]


def format_update_result(result: UpdateResult) -> str:
    """Build the human-readable summary printed by ``vq admin update``.

    Layout:
       == admin update <env> ==
          git_dir, branch, update_script, paused summary

       -- git pull / git fetch tag (rc=N) --
       <captured output, or "(no output)">

       -- <update_script> (rc=N) --
       <captured output, or "(no output)">

          resumed summary

       == OK == | == FAILED == (with reason line if FAILED)
    """
    script_label = (
        "install_script" if result.operation == "install" else "update_script"
    )
    lines = [
        f"== admin {result.operation} {result.env} ==",
        f"   git_dir:       {result.git_dir}",
        f"   branch:        {result.branch or '(unspecified)'}",
        f"   {script_label}: {result.update_script or '(none)'}",
    ]
    if result.operation != "install":
        lines.append(
            f"   post_update:   {result.post_update_script or '(none)'}"
        )
    if result.tag_verification_attempted:
        lines.append(f"   expected_tag:  {result.expected_tag}")
    if result.sha_verification_attempted:
        lines.append(f"   expected_sha:  {result.expected_sha}")
    refresh_label = (
        "git clone" if result.operation == "install"
        else "git fetch tag" if result.expected_tag
        else "git fetch SHA" if result.expected_sha
        else "git pull"
    )
    lines += [
        f"   paused:        {result.paused_summary}",
        "",
        f"-- {refresh_label} (rc={result.git_pull_rc}) --",
        result.git_pull_output.rstrip() or "(no output)",
    ]
    # v0.7.1 *Lamport's Clock*: branch verification block, rendered
    # whenever it ran (i.e., ``VenvProgram.branch`` was configured
    # AND git pull succeeded). Renders BEFORE the tag block to match
    # the runtime order (branch check runs first; failure short-
    # circuits the tag check + build).
    if result.branch_verification_attempted:
        b_verdict = (
            "MATCH" if result.branch_matches
            else f"MISMATCH (got {result.actual_branch!r})"
        )
        lines += [
            "",
            f"-- branch verification (rc={result.branch_check_rc}) --",
            f"   expected: {result.branch!r}",
            f"   actual:   {result.actual_branch!r}",
            f"   verdict:  {b_verdict}",
        ]
    if result.tag_verification_attempted:
        verdict = (
            "MATCH" if result.tag_matches
            else f"MISMATCH (got {result.actual_tag!r})"
        )
        lines += [
            "",
            f"-- tag verification (rc={result.tag_check_rc}) --",
            f"   expected: {result.expected_tag!r}",
            f"   actual:   {result.actual_tag!r}",
            f"   verdict:  {verdict}",
        ]
    if result.sha_verification_attempted:
        verdict = (
            "MATCH" if result.sha_matches
            else f"MISMATCH (got {result.actual_sha!r})"
        )
        lines += [
            "",
            f"-- SHA verification (rc={result.sha_check_rc}) --",
            f"   expected: {result.expected_sha}",
            f"   actual:   {result.actual_sha or '(missing)'}",
            f"   verdict:  {verdict}",
        ]
    if result.update_script is not None:
        lines += [
            "",
            f"-- {result.update_script} (rc={result.update_script_rc}"
            f"{_took(result.update_script_seconds)}) --",
            result.update_script_output.rstrip() or "(no output)",
        ]
    if result.post_update_script is not None:
        lines += [
            "",
            (
                f"-- {result.post_update_script} "
                f"(post-update rc={result.post_update_script_rc}"
                f"{_took(result.post_update_script_seconds)}) --"
            ),
            result.post_update_script_output.rstrip() or "(no output)",
        ]
    if result.work_errors:
        lines += [
            "",
            "-- work errors --",
            *(f"   {e}" for e in result.work_errors),
        ]
    if result.rolled_back:
        lines += [
            "",
            "-- atomic rollback --",
            result.rollback_summary or "checkout/install state rolled back",
        ]
    lines += _format_metrics(result.metrics)
    lines += [
        "",
        f"   resumed:       {result.resumed_summary}",
    ]
    # v0.5.42: surface the daemon-restart outcome. Only print a banner
    # line when something happened — staying quiet on "not vq's venv"
    # avoids noise on every vibeqc-dev / vibeqc-release update.
    if result.daemon_restart_attempted:
        verb = (
            "vq self-update detected — restarting vq-daemon"
            if result.daemon_restart_succeeded
            else "vq self-update detected — daemon restart FAILED"
        )
        lines += [
            "",
            f"==> {verb}",
            f"   {result.daemon_restart_message}",
        ]
    lines += [
        "",
        "== "
        + (
            "ALREADY CURRENT"
            if result.already_current
            else ("OK" if result.success else "FAILED")
        )
        + " ==",
    ]
    if not result.success:
        reasons: list[str] = []
        if result.work_errors:
            reasons.append(f"{len(result.work_errors)} work error(s)")
        if result.git_pull_rc not in (None, 0):
            reasons.append(f"git pull rc={result.git_pull_rc}")
        if (
            result.update_script is not None
            and result.update_script_rc not in (None, 0)
        ):
            reasons.append(f"update_script rc={result.update_script_rc}")
        if (
            result.post_update_script is not None
            and result.post_update_script_rc not in (None, 0)
        ):
            reasons.append(
                f"post_update_script rc={result.post_update_script_rc}"
            )
        if result.tag_verification_attempted and not result.tag_matches:
            reasons.append(
                f"tag mismatch (expected {result.expected_tag!r}, "
                f"got {result.actual_tag!r})"
            )
        if result.sha_verification_attempted and not result.sha_matches:
            reasons.append(
                f"SHA mismatch (expected {result.expected_sha}, "
                f"got {result.actual_sha or '(missing)'})"
            )
        # v0.7.1: surface branch mismatch in the failure summary so
        # the operator sees ``branch mismatch (expected 'main', got
        # 'release')`` instead of having to read the verification
        # block above.
        if result.branch_verification_attempted and not result.branch_matches:
            reasons.append(
                f"branch mismatch (expected {result.branch!r}, "
                f"got {result.actual_branch!r})"
            )
        # v0.7.1 Item 5: dirty-tree gate (opt-in via VenvProgram
        # .fail_on_dirty=True). Reason line is explicit so the
        # operator knows it was the fail_on_dirty policy that
        # tipped the verdict, not the pull/script.
        if (
            result.fail_on_dirty_in_effect
            and result.dirty_after_update is True
        ):
            reasons.append(
                "dirty tree after update (fail_on_dirty=true)"
            )
        if result.rolled_back:
            reasons.append("update rolled back")
        if result.import_check_rc not in (None, 0):
            reasons.append(f"post-update import check rc={result.import_check_rc}")
        if (
            result.daemon_restart_attempted
            and (
                result.daemon_restart_succeeded is not True
                or result.daemon_health_verified is not True
            )
        ):
            reasons.append(
                "vq-daemon restart failed or provenance verification failed"
            )
        if reasons:
            lines.append("   reasons: " + "; ".join(reasons))
    return "\n".join(lines)


def format_update_result_json(result: UpdateResult) -> str:
    """v0.5.46: JSON-serialised view of :func:`format_update_result`.

    Returns a pretty-printed JSON object with every UpdateResult field
    plus computed ``success`` (the property, materialised), plus
    ``tag_matches`` (None when no --tag was given). Stable schema:
    optional fields are emitted as ``null`` so consumers always see
    the same keys.

    Use for `vq admin update --json <env>`. The shape mirrors the text
    formatter so a script can drive on the same fields the human eye
    reads."""
    payload = asdict(result)
    payload["success"] = result.success
    payload["tag_matches"] = result.tag_matches
    # v0.7.1 *Lamport's Clock*: materialise the branch_matches property
    # alongside tag_matches so scripts can read the verdict directly
    # without re-implementing the comparison.
    payload["branch_matches"] = result.branch_matches
    # v0.26.1: the classification, on every payload including the successful
    # ones, so a caller reads one field in one place rather than inferring
    # from `success` here and an exception's prose there.
    payload["outcome"] = result.outcome
    return json.dumps(payload, indent=2, sort_keys=True)


def format_update_all_results_json(results: list[UpdateResult]) -> str:
    """v0.5.46: JSON-serialised view of
    :func:`format_update_all_results`.

    Returns an object with ``results`` (list of per-env objects in the
    same shape as :func:`format_update_result_json`'s output), plus
    batch-summary fields: ``batch_success`` (bool — all envs ok),
    ``n_ok`` / ``n_total`` / ``failed_envs`` (list of names that
    failed). An empty input gives ``{"results": [], "n_total": 0,
    "n_ok": 0, "failed_envs": [], "batch_success": true}``."""
    per_env: list[dict[str, object]] = []
    for r in results:
        rec = asdict(r)
        rec["success"] = r.success
        rec["tag_matches"] = r.tag_matches
        per_env.append(rec)
    n_ok = sum(1 for r in results if r.success)
    n_total = len(results)
    failed = [r.env for r in results if not r.success]
    return json.dumps(
        {
            "results": per_env,
            "n_ok": n_ok,
            "n_total": n_total,
            "failed_envs": failed,
            "batch_success": n_ok == n_total,
        },
        indent=2,
        sort_keys=True,
    )


def format_update_all_results(results: list[UpdateResult]) -> str:
    """v0.5.28: human-readable summary for ``vq admin update --all``.

    Prints each env's full per-env block (reusing
    :func:`format_update_result`) separated by blank lines, then a
    batch verdict line: ``== BATCH OK: N/N succeeded ==`` or
    ``== BATCH FAILED: M/N succeeded ==`` with the failing env names.
    """
    if not results:
        return "== admin update --all: no venv envs to update =="
    blocks = [format_update_result(r) for r in results]
    n_ok = sum(1 for r in results if r.success)
    n_total = len(results)
    verdict_lines = ["", "=" * 60]
    if n_ok == n_total:
        verdict_lines.append(f"== BATCH OK: {n_ok}/{n_total} envs succeeded ==")
    else:
        failed = [r.env for r in results if not r.success]
        verdict_lines.append(
            f"== BATCH FAILED: {n_ok}/{n_total} envs succeeded "
            f"(failed: {', '.join(failed)}) =="
        )
    return "\n\n".join(blocks) + "\n" + "\n".join(verdict_lines)


# ----------------------------------------------------------------------
# v0.5.25: vq admin status — tip SHAs + last-update times per registered
# venv program
# ----------------------------------------------------------------------


# ----------------------------------------------------------------------
# v0.5.44: admin-update-in-progress marker file
#
# Problem: `vq admin update` pauses the queue, runs git pull +
# update_script, then resumes — all inside a try/finally so the resume
# always runs. But if the *update_script itself* is killed mid-flight
# (Ctrl-C, ssh dropped, SIGKILL, kernel OOM during a heavy build), the
# venv is left in a half-installed state. The queue resumes (because
# the finally fires), dispatches start hitting the half-built venv,
# and jobs fail or — worse — silently use mismatched module versions.
#
# Fix: write `<state_root>/admin-update-in-progress` BEFORE the work
# starts; clear it AFTER successful resume. A failed `vq admin update`
# (rc != 0 from git pull, tag mismatch, update_script failure,
# or process killed) leaves the marker on disk. The next
# `vq admin update` invocation reads the marker, refuses to proceed,
# and tells the operator to inspect via `vq admin status` and recover
# via `vq admin clear-update-marker` (or re-run with --force).
#
# The marker is the SAFETY mechanism for the "I'm pretty sure the venv
# is fine" question after an unexpected exit. Without it, the answer
# is silent-and-hopeful; with it, the answer is loud-and-explicit.
# ----------------------------------------------------------------------

ADMIN_UPDATE_MARKER_FILENAME = "admin-update-in-progress"
ADMIN_UPDATE_MARKER_DIRNAME = "admin-update-markers"
ADMIN_UPDATE_MARKER_LOCK_FILENAME = "admin-update-marker.lock"
ADMIN_UPDATE_OWNERSHIP_LOCK_FILENAME = "admin-update-ownership.lock"
ADMIN_UPDATE_QUARANTINE_DIRNAME = "admin-update-quarantine"
ADMIN_UPDATE_RUNTIME_LOCK_DIR = Path("/run/vq-admin-update")
ORPHAN_QUARANTINE_SCHEMA = "vq-admin-orphan-quarantine-v1"


# v0.6.0: phase strings stored in AdminUpdateMarker.state. Picked as
# string literals (not StrEnum) so the on-disk JSON is human-readable
# and the wire format is forwards-compatible with future phase
# additions (unknown phases parse as the raw string; consumers should
# treat any non-quiescent value as "blocked"). Sequence:
#
#   PAUSING -> PAUSED -> PULLING -> TAG_CHECKING -> BUILDING ->
#   RESUMING -> RESTARTING_DAEMON -> VERIFYING -> (file removed = IDLE)
#                                              \-> FAILED (sticky)
#
# Pre-v0.6.0 markers don't have a state field; reads default it to
# ADMIN_UPDATE_STATE_LEGACY so the daemon's guard still fires.
ADMIN_UPDATE_STATE_PAUSING = "pausing"
ADMIN_UPDATE_STATE_PAUSED = "paused"
ADMIN_UPDATE_STATE_PULLING = "pulling"
ADMIN_UPDATE_STATE_TAG_CHECKING = "tag_checking"
ADMIN_UPDATE_STATE_BUILDING = "building"
ADMIN_UPDATE_STATE_RESUMING = "resuming"
ADMIN_UPDATE_STATE_RESTARTING_DAEMON = "restarting_daemon"
ADMIN_UPDATE_STATE_VERIFYING = "verifying"
ADMIN_UPDATE_STATE_FAILED = "failed"
ADMIN_UPDATE_STATE_LEGACY = "legacy_in_progress"
"""Default state for pre-v0.6.0 markers that lack a state field."""

ADMIN_UPDATE_STATES_QUIESCENT = frozenset({ADMIN_UPDATE_STATE_FAILED})
"""States that signal "no update in flight; either ack-and-clear via
`vq admin clear-update-marker` (FAILED) or the file shouldn't exist
(IDLE, which is the absence of the file)."""

# v0.11.0: a marker older than this can't be a real in-flight update —
# the writing `vq admin update` process is gone (crashed, killed, or
# its host rebooted) and the marker is a corpse silently gating the
# daemon's dispatch loop. Used by :func:`admin_update_marker_stale_reason`
# as the backstop when pid liveness can't settle the question (no /proc,
# or a recycled pid we couldn't fingerprint). The longest real update is
# a cold native-dep rebuild (~90 min on a slow host); 24h is a >15x
# margin so this never reaps a slow-but-live build, only true corpses
# like the compute-b 2026-06-18 marker that survived a 5-day-old kill + a
# reboot. Module-level (not config) by design: it's a safety floor, not
# a tunable — see docs/operations.md § "Admin update stuck".
ADMIN_UPDATE_MARKER_MAX_AGE_SECONDS = 24 * 60 * 60

ADMIN_UPDATE_MARKER_DIAG_RUNNING = "running"
ADMIN_UPDATE_MARKER_DIAG_FAILED = "failed"
ADMIN_UPDATE_MARKER_DIAG_STALE = "stale"
ADMIN_UPDATE_MARKER_DIAG_UNREADABLE = "unreadable"


@dataclass
class AdminUpdateMarker:
    """v0.5.44 / v0.6.0: serialised contents of the
    admin-update-in-progress marker. Schema lives on disk as JSON;
    the dataclass is the parsed view. Fields are deliberately small
    — the marker exists to record enough context for an operator to
    decide whether to clear or investigate, not to be a full audit
    log."""

    envs: list[str]
    """Env names being updated (single-element for update_env, full
    list for update_all). Operator sees "which env was being touched."
    """
    host: str
    """Host the update was launched against (typically "localhost" for
    on-host invocations; SSH-delegated calls record the local hostname
    as resolved by the remote vq)."""
    started_at: str
    """UTC ISO-8601 timestamp when the marker was written (= just
    after pause_all, before the first git pull). Operator sees how
    long ago the incomplete update started."""
    pid: int
    """PID of the `vq admin update` process that wrote the marker.
    Lets an operator check `ps` to see if the original process is
    still alive (rare — the marker is mostly a post-mortem signal —
    but useful when diagnosing 'is this stale?')."""
    vq_version: str
    """vq version the writer was running (from `vq.__version__`).
    Helps when reading an old marker that survived a vq upgrade."""
    pid_start_time: int = 0
    """v0.11.0: ``/proc/<pid>/stat`` field 22 (process start time in
    clock ticks since boot) of the writer process, captured at marker
    write. The anti-recycling fingerprint: after a reboot or a long
    idle the kernel may reuse ``pid`` for an unrelated process, so a
    bare ``os.kill(pid, 0)`` liveness probe can read "alive" against a
    stranger. Comparing the live process's start time to this recorded
    value distinguishes "our update is still running" from "pid was
    recycled" — see :func:`admin_update_marker_stale_reason`. ``0``
    for pre-v0.11.0 markers and on platforms without ``/proc`` (macOS);
    the staleness check falls back to the age bound there."""
    state: str = ADMIN_UPDATE_STATE_LEGACY
    """v0.6.0: current update phase. Defaults to
    ADMIN_UPDATE_STATE_LEGACY for pre-v0.6.0 markers on disk that
    lack the field (the field absence is treated as "presence
    blocks; phase unknown"). Updated atomically as each phase
    completes via :func:`transition_admin_update_state`; sticky
    FAILED is the only non-removal terminal value."""
    phase_started_at: str = ""
    """v0.6.0: UTC ISO-8601 timestamp of the most recent state
    transition. Lets `vq admin status` show "stuck at <phase> for
    <duration>" — the audit's primary motivation for a state
    machine over a boolean marker. Empty string for pre-v0.6.0
    markers."""
    failure_reason: str | None = None
    """v0.6.0: populated when state == ADMIN_UPDATE_STATE_FAILED;
    one-line description of what went wrong (e.g. "git pull
    rc=128", "update_script timed out after 1800s", "daemon
    restart failed: systemctl --user is unreachable"). None for
    any non-FAILED state."""
    last_heartbeat_at: str = ""
    """v0.12.x: UTC ISO-8601 timestamp refreshed by long-running admin
    update work. Phase transitions stamp this field, and the monitored
    build loop refreshes it on each build heartbeat so `vq admin status`
    can distinguish an alive, recently-chatty updater from a quiet marker."""
    last_heartbeat_message: str | None = None
    """v0.12.x: short operator-facing message associated with
    last_heartbeat_at (for example the build-loop "still running" line).
    None when the marker predates this field or no detail is useful."""
    marker_id: str = ""
    """Stable owner token for scope-aware concurrent markers. Empty for
    pre-scope marker files; legacy markers remain readable and conservative."""
    pause_token: str | None = None
    """Owner token passed to non-surgical pause/resume for this invocation."""
    pause_multi_user: bool | None = None
    """Exact queue namespace used by this pause transaction.

    ``False`` selects the single-user ``queue/`` tree and ``True`` selects the
    system multi-user ``users/<uid>/queue/`` trees. ``None`` is reserved for
    markers written before this receipt field existed; a legacy marker that
    still owns a pause token must fail closed rather than guess from today's
    configuration.
    """
    pause_queue_root: str | None = None
    """Normalized state-root identity paired with :attr:`pause_multi_user`.

    The multi-user root has an independent environment override, so the mode
    bit alone is not enough: recovery must also prove it is scanning the same
    root that admission paused.
    """
    paused_jobids: list[str] = field(default_factory=list)
    """Exact job ids paused by a surgical branch-scoped update."""
    surgical_pause: bool = False
    """Whether recovery must resume :attr:`paused_jobids` instead of a token."""
    managed_transaction: dict[str, object] | None = None
    """Durable v1 receipt for a serving-daemon venv transaction.

    The receipt is written and fsynced before the daemon is stopped.  It binds
    the exact old checkout, virtualenv backup, service definition, pause scope,
    and (once known) target identity so an interrupted self-update can be
    recovered without guessing or clearing the safety marker.
    """
    detached_run_id: str | None = None
    """Run id of the detached updater that owns this marker, when there is one.

    Set only for an update spawned into its own session by a delegated
    ``vq admin update`` (see :mod:`vq.admin_detached`). It is what lets
    ``vq admin status`` point an operator at the run whose transcript and
    terminal receipt explain a marker that is legitimately still building
    hours after the SSH session that launched it went away. ``None`` for an
    ordinary attached update, which is every local invocation.
    """

    @property
    def owns_pause_scope(self) -> bool:
        """Whether clearing this marker could strand paused job processes."""
        return bool(self.pause_token or self.paused_jobids)


@dataclass(frozen=True)
class AdminUpdateMarkerDiagnosis:
    """Operator-facing classification of an admin-update marker.

    The marker state describes the update phase; this diagnosis answers
    the separate operational question "should I wait for a live updater, or
    clear/force a stale/failed marker?" It is computed on the host that owns
    the marker, because PID liveness is only meaningful there.
    """

    marker_status: str
    summary: str
    action: str
    pid_status: str | None = None
    stale_reason: str | None = None
    heartbeat_status: str | None = None
    heartbeat_age_seconds: float | None = None


def admin_update_marker_path() -> Path:
    """v0.5.44: location of the admin-update-in-progress marker
    file. Lives alongside admin-status.json under the state root."""
    return paths.state_root() / ADMIN_UPDATE_MARKER_FILENAME


def admin_update_marker_dir() -> Path:
    """Directory containing additional disjoint update-marker leases."""
    return paths.state_root() / ADMIN_UPDATE_MARKER_DIRNAME


def _admin_update_marker_lock_path() -> Path:
    return paths.state_root() / ADMIN_UPDATE_MARKER_LOCK_FILENAME


@dataclass(frozen=True)
class _AdminStateBinding:
    """One immutable physical state/lock namespace for an admin transaction."""

    state_root: Path
    state_root_key: tuple[int, int]
    lock_root: Path
    lock_root_key: tuple[int, int]

    @property
    def marker_path(self) -> Path:
        return self.state_root / ADMIN_UPDATE_MARKER_FILENAME

    @property
    def marker_dir(self) -> Path:
        return self.state_root / ADMIN_UPDATE_MARKER_DIRNAME

    @property
    def marker_lock_path(self) -> Path:
        return self.lock_root / ADMIN_UPDATE_MARKER_LOCK_FILENAME

    @property
    def ownership_lock_path(self) -> Path:
        return self.lock_root / ADMIN_UPDATE_OWNERSHIP_LOCK_FILENAME

    @property
    def status_path(self) -> Path:
        return self.state_root / ADMIN_STATUS_FILENAME

    @property
    def quarantine_root(self) -> Path:
        return self.state_root / ADMIN_UPDATE_QUARANTINE_DIRNAME


def _safe_admin_update_lock_directory() -> Path:
    """Return an owner-controlled directory whose entries cannot be swapped.

    Ordinary user state roots are suitable when their final component is
    owned by this process and has no group/world write.  The documented
    multi-user root is intentionally ``2775`` so trusted admins can write
    control records; a lock file there could be unlinked and replaced while
    held.  A root updater therefore uses a private volatile directory under
    ``/run`` for that deployment shape.  Non-root callers fail closed on an
    unsafe state root rather than claiming an ineffective lock.
    """
    root = paths.state_root()
    try:
        root.mkdir(parents=True, mode=0o700, exist_ok=True)
        info = os.lstat(root)
    except OSError as exc:
        raise AdminError(
            f"cannot prepare admin-update ownership directory {root}: {exc}"
        ) from exc
    if (
        stat.S_ISDIR(info.st_mode)
        and info.st_uid == os.geteuid()
        and stat.S_IMODE(info.st_mode) & 0o022 == 0
    ):
        return root
    if os.geteuid() != 0:
        raise AdminError(
            f"unsafe admin-update ownership directory {root}: expected an "
            "owner-controlled real directory without group/world write"
        )

    runtime_parent = ADMIN_UPDATE_RUNTIME_LOCK_DIR.parent
    try:
        parent_info = os.lstat(runtime_parent)
    except OSError as exc:
        raise AdminError(
            f"cannot validate admin-update runtime lock parent "
            f"{runtime_parent}: {exc}"
        ) from exc
    if (
        not stat.S_ISDIR(parent_info.st_mode)
        or parent_info.st_uid != 0
        or stat.S_IMODE(parent_info.st_mode) & 0o022
    ):
        raise AdminError(
            f"unsafe admin-update runtime lock parent {runtime_parent}"
        )
    try:
        ADMIN_UPDATE_RUNTIME_LOCK_DIR.mkdir(mode=0o700, exist_ok=True)
        runtime_info = os.lstat(ADMIN_UPDATE_RUNTIME_LOCK_DIR)
    except OSError as exc:
        raise AdminError(
            "cannot prepare private admin-update runtime lock directory: "
            f"{exc}"
        ) from exc
    if (
        not stat.S_ISDIR(runtime_info.st_mode)
        or runtime_info.st_uid != 0
        or stat.S_IMODE(runtime_info.st_mode) & 0o022
    ):
        raise AdminError(
            f"unsafe admin-update runtime lock directory "
            f"{ADMIN_UPDATE_RUNTIME_LOCK_DIR}"
        )
    if stat.S_IMODE(runtime_info.st_mode) != 0o700:
        try:
            os.chmod(ADMIN_UPDATE_RUNTIME_LOCK_DIR, 0o700)
        except OSError as exc:
            raise AdminError(
                "could not enforce mode 0700 on admin-update runtime lock "
                f"directory: {exc}"
            ) from exc
    return ADMIN_UPDATE_RUNTIME_LOCK_DIR


def _capture_admin_state_binding() -> _AdminStateBinding:
    """Capture and validate the physical state and secure lock directories.

    Environment changes and path aliases must not move an in-flight operation
    to another marker registry.  These locks live *inside* the selected
    directories rather than under a digest of their spelling, so resolving
    symlinks plus pinning the directory inodes is sufficient; unlike lifecycle
    resource keys this must not invoke the mockable Git/subprocess runner used
    by ordinary admin-update tests.
    """
    requested = paths.state_root().expanduser()
    try:
        requested.mkdir(parents=True, mode=0o700, exist_ok=True)
        state_root = requested.resolve(strict=True)
        state_info = os.lstat(state_root)
    except (OSError, AdminError) as exc:
        raise AdminError(f"cannot bind admin state root {requested}: {exc}") from exc
    if not stat.S_ISDIR(state_info.st_mode) or stat.S_ISLNK(state_info.st_mode):
        raise AdminError(f"admin state root is not a physical directory: {state_root}")
    lock_root = _safe_admin_update_lock_directory().resolve(strict=True)
    try:
        lock_info = os.lstat(lock_root)
    except OSError as exc:
        raise AdminError(f"cannot bind admin lock root {lock_root}: {exc}") from exc
    if not stat.S_ISDIR(lock_info.st_mode) or stat.S_ISLNK(lock_info.st_mode):
        raise AdminError(f"admin lock root is not a physical directory: {lock_root}")
    return _AdminStateBinding(
        state_root=state_root,
        state_root_key=(state_info.st_dev, state_info.st_ino),
        lock_root=lock_root,
        lock_root_key=(lock_info.st_dev, lock_info.st_ino),
    )


def _validate_admin_state_binding(binding: _AdminStateBinding) -> None:
    """Fail closed if paths or directory inodes changed after admission."""
    current = _capture_admin_state_binding()
    if current != binding:
        raise AdminUpdateInProgress(
            "admin state/lock binding changed during the transaction; refusing "
            f"to move from {binding.state_root} to {current.state_root}"
        )


_admin_fork_guard = threading.RLock()
_admin_lock_fds: set[int] = set()


def _register_admin_lock_fd(fd: int) -> None:
    _admin_lock_fds.add(fd)


def _close_admin_lock_fd(fd: int) -> None:
    with _admin_fork_guard:
        _admin_lock_fds.discard(fd)
        with contextlib.suppress(OSError):
            os.close(fd)


def _admin_before_fork() -> None:
    _admin_fork_guard.acquire()


def _admin_after_fork_parent() -> None:
    _admin_fork_guard.release()


def _admin_after_fork_child() -> None:
    """Close every inherited lock fd, including another thread's locks."""
    global _admin_update_ownership_process_lock
    global _admin_update_marker_process_lock
    global _admin_update_ownership_local
    global _admin_update_marker_local
    global _admin_update_marker_owner
    try:
        for fd in tuple(_admin_lock_fds):
            with contextlib.suppress(OSError):
                os.close(fd)
        _admin_lock_fds.clear()
        _admin_update_ownership_process_lock = threading.Lock()
        _admin_update_marker_process_lock = threading.Lock()
        _admin_update_ownership_local = threading.local()
        _admin_update_marker_local = threading.local()
        _admin_update_marker_owner = threading.local()
        # A no-argument clear is an owner convenience API.  The child must
        # not fall through to its legacy singleton fallback after losing the
        # parent's exact ownership identity at fork.
        _admin_update_marker_owner.forked_without_owner = True
    finally:
        _admin_fork_guard.release()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(
        before=_admin_before_fork,
        after_in_parent=_admin_after_fork_parent,
        after_in_child=_admin_after_fork_child,
    )


def _admin_update_ownership_lock_path() -> Path:
    return _safe_admin_update_lock_directory() / (
        ADMIN_UPDATE_OWNERSHIP_LOCK_FILENAME
    )


def _open_secure_admin_lock(path: Path, *, label: str) -> int:
    """Open one owner-only stable lock inode and register it fork-safely."""
    flags = (
        os.O_RDWR
        | os.O_CREAT
        | os.O_NOFOLLOW
        | getattr(os, "O_CLOEXEC", 0)
    )
    with _admin_fork_guard:
        try:
            fd = os.open(path, flags, 0o600)
        except OSError as exc:
            raise AdminError(f"unsafe {label} {path}: {exc}") from exc
        try:
            info = os.fstat(fd)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.geteuid()
                or info.st_nlink != 1
            ):
                raise AdminError(
                    f"unsafe {label} {path}: expected an owner-controlled "
                    "regular file with one link"
                )
            if stat.S_IMODE(info.st_mode) != 0o600:
                os.fchmod(fd, 0o600)
                info = os.fstat(fd)
                if stat.S_IMODE(info.st_mode) != 0o600:
                    raise AdminError(
                        f"unsafe {label} {path}: could not enforce mode 0600"
                    )
            current = os.stat(path, follow_symlinks=False)
            if (
                (current.st_dev, current.st_ino) != (info.st_dev, info.st_ino)
                or not stat.S_ISREG(current.st_mode)
                or current.st_uid != os.geteuid()
                or current.st_nlink != 1
            ):
                raise AdminError(
                    f"unsafe {label} {path}: pathname and opened inode disagree"
                )
            _register_admin_lock_fd(fd)
            return fd
        except BaseException:
            os.close(fd)
            raise


def _open_admin_update_ownership_lock(
    binding: _AdminStateBinding | None = None,
) -> tuple[int, Path]:
    """Securely open and validate the stable ownership-lock inode."""
    binding = binding or _capture_admin_state_binding()
    _validate_admin_state_binding(binding)
    path = binding.ownership_lock_path
    return _open_secure_admin_lock(
        path, label="admin-update ownership lock",
    ), path


_admin_update_ownership_local = threading.local()
_admin_update_ownership_process_lock = threading.Lock()


@dataclass
class _ToolsetLifecycleLock:
    """The same checkout/target fcntl locks used by lifecycle shell scripts."""

    resources: tuple[tuple[str, str, int, Path], ...]


_toolset_lifecycle_local = threading.local()
_FLEET_LIFECYCLE_HANDOFF_SCHEMA = "vq.toolset.lifecycle_handoff/1"
_MULTI_USER_MIGRATION_RECEIPT_NAME = "vq-multi-user-bootstrap.json"


def _canonical_lifecycle_checkout(git_dir: Path) -> Path:
    try:
        proc = subprocess.run(
            ["git", "-C", str(git_dir), "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            timeout=30,
            stdin=subprocess.DEVNULL,
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        raise AdminError(f"cannot resolve lifecycle checkout: {exc}") from exc
    if proc.returncode != 0:
        raise AdminError(
            "cannot resolve lifecycle checkout: "
            + ((proc.stderr or proc.stdout or "git failed").strip())
        )
    return _physical_lifecycle_directory(Path(proc.stdout.strip()))


def _physical_lifecycle_directory(path: Path) -> Path:
    """Return the kernel's physical spelling for an existing directory.

    ``Path.resolve()`` preserves caller-supplied case on the default
    case-insensitive macOS filesystem (for example ``/PRIVATE/TMP``), while
    the lifecycle shell helper uses ``pwd -P`` and reports ``/private/tmp``.
    Those different strings hash to different lock files.  Run the platform's
    fixed ``pwd`` binary with ``path`` as its cwd so Python and shell bind the
    same physical directory without changing this process's global cwd.
    """
    try:
        proc = subprocess.run(
            ["/bin/pwd", "-P"],
            cwd=path,
            capture_output=True,
            text=True,
            timeout=30,
            stdin=subprocess.DEVNULL,
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        raise AdminError(
            f"cannot resolve lifecycle directory {path}: {exc}"
        ) from exc
    resolved = proc.stdout.strip()
    if proc.returncode != 0 or not resolved.startswith("/"):
        raise AdminError(
            f"cannot resolve lifecycle directory {path}: "
            + ((proc.stderr or proc.stdout or "pwd failed").strip())
        )
    return Path(resolved)


def _darwin_future_component(component: str) -> str:
    """Use one ASCII lock spelling for a not-yet-created macOS path.

    Existing paths are always canonicalized through ``pwd -P``.  A missing
    final component has no stored spelling yet, so two callers using ``.venv``
    and ``.VENV`` could otherwise obtain distinct locks before creating the
    same path on a case-insensitive APFS volume.  Collapsing ASCII case on
    Darwin is intentionally conservative on a case-sensitive volume: it may
    serialize two distinct future paths, but it cannot permit overlap.
    """
    if sys.platform != "darwin":
        return component
    return component.translate(str.maketrans(
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"
    ))


def _canonical_lifecycle_target(target: Path) -> Path:
    if target.is_symlink():
        raise AdminError(f"refusing symlinked lifecycle target {target}")
    if target.exists():
        if not target.is_dir():
            raise AdminError(f"lifecycle target is not a directory: {target}")
        return _physical_lifecycle_directory(target)
    parent = _physical_lifecycle_directory(target.parent)
    return parent / _darwin_future_component(target.name)


def _multi_user_migration_receipt_for_checkout(checkout: str) -> Path:
    """Return the canonical, environment-independent migration admission."""
    return Path(checkout) / ".git" / _MULTI_USER_MIGRATION_RECEIPT_NAME


def _refuse_active_multi_user_migration(lock: _ToolsetLifecycleLock) -> None:
    """Fail closed while deploy-multi-user owns a serving user runtime.

    Call only after the exact checkout/target locks are held. A migration that
    was already armed keeps this checkout-metadata receipt across process death, so an
    admin/reset/self-update successor cannot replace the runtime used for
    migration recovery.
    """
    targets = [resource for scope, resource, _fd, _path in lock.resources
               if scope == "target"]
    for scope, resource, _fd, _path in lock.resources:
        if scope != "checkout":
            continue
        receipt = _multi_user_migration_receipt_for_checkout(resource)
        if os.path.lexists(receipt):
            raise AdminUpdateInProgress(
                "durable multi-user migration admission is active for "
                f"{', '.join(targets) or resource}; resume the exact deploy-multi-user.sh "
                "transaction before mutating this checkout or virtualenv"
            )


def _canonical_future_lifecycle_path(path: Path) -> Path:
    """Canonicalize a not-yet-created slot resource without following it.

    Runtime-slot source and virtualenv paths do not exist until the updater
    materializes the selected SHA.  Their lifecycle locks must nevertheless be
    held *before* that materialization begins.  Resolve the nearest existing
    ancestor, reject any existing symlink below it, and append the still-missing
    suffix lexically so the key is the same one the child updater will derive
    once the path exists.
    """
    if not path.is_absolute():
        raise AdminError(f"lifecycle resource must be absolute: {path}")
    missing: list[str] = []
    cursor = path
    while not cursor.exists() and not cursor.is_symlink():
        if cursor.parent == cursor:
            raise AdminError(f"cannot resolve lifecycle resource {path}")
        missing.append(cursor.name)
        cursor = cursor.parent
    if cursor.is_symlink():
        raise AdminError(f"refusing symlinked lifecycle resource {cursor}")
    if not cursor.is_dir():
        raise AdminError(f"lifecycle resource is not a directory: {cursor}")
    base = _physical_lifecycle_directory(cursor)
    for component in reversed(missing):
        base /= _darwin_future_component(component)
    return base


def _runtime_slot_lifecycle_resources(
    prog: config.VenvProgram,
    *,
    expected_sha: str | None,
) -> tuple[tuple[str, str], ...]:
    """Return the exact future checkout/venv locks for a selected slot.

    A slot update still holds the live checkout/venv locks because it clones
    from that checkout and flips the live slot pointer.  These additional keys
    fence the newly materialized checkout and its updater child from direct
    lifecycle scripts as well.
    """
    if prog.runtime_slot_root is None or expected_sha is None:
        return ()
    sha = expected_sha.strip().lower()
    if _FULL_SHA_RE.fullmatch(sha) is None:
        # The update path owns the user-facing selector validation.  Do not
        # interpolate an invalid value into a filesystem path in the meantime.
        return ()
    try:
        root = runtime_slots.layout(prog.runtime_slot_root).root
    except runtime_slots.RuntimeSlotError as exc:
        raise AdminError(str(exc)) from exc
    source = _canonical_future_lifecycle_path(
        runtime_slots.slot_source(root, sha),
    )
    target = _canonical_future_lifecycle_path(
        runtime_slots.slot_python(root, sha).parent.parent,
    )
    return (
        ("checkout", str(source)),
        ("target", str(target)),
    )


def _toolset_lock_identity(scope: str, resource: str) -> tuple[int, Path]:
    """Return the resource owner and sole canonical lock pathname."""
    # Root maintenance and the legitimate resource owner must contend on the
    # same inode, while unrelated local users must not be able to pre-plant or
    # replace its predictable digest path. Derive the namespace UID from the
    # resource (or its nearest existing ancestor for a future runtime slot).
    # This is an advisory coordination boundary, not an authorization boundary
    # against the owner, who can already mutate the resource bytes directly.
    resource_path = Path(resource)
    owner_probe = resource_path
    while not owner_probe.exists() and not owner_probe.is_symlink():
        if owner_probe.parent == owner_probe:
            raise AdminError(
                f"cannot derive lifecycle resource owner for {resource}"
            )
        owner_probe = owner_probe.parent
    if owner_probe.is_symlink():
        raise AdminError(f"refusing symlinked lifecycle resource {owner_probe}")
    try:
        resource_uid = owner_probe.stat().st_uid
    except OSError as exc:
        raise AdminError(
            f"cannot inspect lifecycle resource owner {owner_probe}: {exc}"
        ) from exc
    if os.geteuid() not in {0, resource_uid}:
        raise AdminError(
            f"lifecycle resource {resource} is owned by uid {resource_uid}; "
            f"caller uid {os.geteuid()} is neither owner nor root"
        )
    root = Path(f"/tmp/vibe-toolset-lifecycle-locks-{resource_uid}")
    if root.is_symlink():
        raise AdminError(f"unsafe symlinked lifecycle lock root {root}")
    created_root = False
    old_umask = os.umask(0o077)
    try:
        try:
            root.mkdir(mode=0o700)
            created_root = True
        except FileExistsError:
            pass
    finally:
        os.umask(old_umask)
    if created_root and os.geteuid() == 0 and resource_uid != 0:
        os.chown(root, resource_uid, -1)
    root_info = root.lstat()
    if (
        not stat.S_ISDIR(root_info.st_mode)
        or root_info.st_uid != resource_uid
        or stat.S_IMODE(root_info.st_mode) != 0o700
    ):
        raise AdminError(f"unsafe lifecycle lock root {root}")
    digest = hashlib.sha256(f"{scope}:{resource}".encode()).hexdigest()
    return resource_uid, root / f"{scope}-{digest}.lock"


def _open_toolset_lock(scope: str, resource: str) -> tuple[int, Path]:
    resource_uid, path = _toolset_lock_identity(scope, resource)
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    old_umask = os.umask(0o077)
    try:
        fd = os.open(path, flags, 0o600)
    finally:
        os.umask(old_umask)
    try:
        info = os.fstat(fd)
        if info.st_uid == 0 and os.geteuid() == 0 and resource_uid != 0:
            os.fchown(fd, resource_uid, -1)
            info = os.fstat(fd)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != resource_uid
            or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise AdminError(f"unsafe lifecycle lock file {path}")
        current = os.stat(path, follow_symlinks=False)
        if (
            (current.st_dev, current.st_ino) != (info.st_dev, info.st_ino)
            or current.st_nlink != 1
        ):
            raise AdminError(f"lifecycle lock pathname changed: {path}")
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd, path
    except BlockingIOError as exc:
        os.close(fd)
        raise AdminUpdateInProgress(
            f"another toolset lifecycle operation owns {scope} {resource}"
        ) from exc
    except BaseException:
        os.close(fd)
        raise


def _validate_inherited_toolset_lock(
    scope: str,
    resource: str,
    fd: int,
    path: Path,
) -> None:
    """Prove an inherited descriptor is the exact currently-held lock."""
    resource_uid, expected_path = _toolset_lock_identity(scope, resource)
    if path != expected_path:
        raise AdminError(
            f"fleet lifecycle handoff lock path does not match {scope} "
            f"{resource}"
        )
    try:
        info = os.fstat(fd)
        named = os.stat(path, follow_symlinks=False)
    except OSError as exc:
        raise AdminError(
            f"fleet lifecycle handoff lock is unavailable: {exc}"
        ) from exc
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != resource_uid
        or info.st_nlink != 1
        or stat.S_IMODE(info.st_mode) != 0o600
        or (named.st_dev, named.st_ino) != (info.st_dev, info.st_ino)
        or not stat.S_ISREG(named.st_mode)
        or named.st_uid != resource_uid
        or named.st_nlink != 1
    ):
        raise AdminError(
            f"fleet lifecycle handoff does not bind the exact {scope} lock"
        )
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        raise AdminError(
            f"fleet lifecycle handoff does not own the {scope} lock"
        ) from exc


def _active_toolset_lifecycle_handoff() -> tuple[str, tuple[int, ...]]:
    """Serialize active lock descriptors for one durable rollout child."""
    active: _ToolsetLifecycleLock | None = getattr(
        _toolset_lifecycle_local, "active", None,
    )
    if active is None:
        raise AdminError("fleet rollout controller has no lifecycle fence")
    locks = [
        {
            "scope": scope,
            "resource": resource,
            "fd": fd,
            "path": str(path),
        }
        for scope, resource, fd, path in active.resources
    ]
    payload = json.dumps(
        {
            "schema": _FLEET_LIFECYCLE_HANDOFF_SCHEMA,
            "locks": locks,
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    return payload, tuple(item["fd"] for item in locks)


def _active_toolset_lifecycle_resources() -> tuple[tuple[str, str], ...]:
    active: _ToolsetLifecycleLock | None = getattr(
        _toolset_lifecycle_local, "active", None,
    )
    if active is None:
        raise AdminError("fleet rollout controller has no lifecycle fence")
    return tuple(
        (scope, resource)
        for scope, resource, _fd, _path in active.resources
    )


def _parse_toolset_lifecycle_handoff(
    encoded: str,
    *,
    require_execution_context: bool,
) -> _ToolsetLifecycleLock:
    """Validate exact inherited lifecycle-lock OFDs.

    Durable action children must additionally prove their immutable operation
    execution context.  A rollout-controller successor is instead admitted by
    the separately authenticated global-rollout handoff, then calls this
    parser with ``require_execution_context=False`` and binds the returned
    resources to its configured checkout and venv before any ref access.
    """
    try:
        if require_execution_context:
            context = fleet_operation.execution_context_from_environ()
            if context is None:
                raise AdminError(
                    "fleet lifecycle handoff lacks an operation execution context"
                )
            fleet_operation.validate_live_execution_context(context)
        raw = json.loads(encoded)
    except fleet_operation.OperationError as exc:
        raise AdminError(
            f"invalid fleet lifecycle execution context: {exc}"
        ) from exc
    except json.JSONDecodeError as exc:
        raise AdminError("fleet lifecycle handoff is not valid JSON") from exc
    if (
        not isinstance(raw, dict)
        or set(raw) not in (
            {"schema", "locks"},
            {"schema", "locks", "rollout_lock"},
        )
        or raw.get("schema") != _FLEET_LIFECYCLE_HANDOFF_SCHEMA
        or not isinstance(raw.get("locks"), list)
        or not raw["locks"]
    ):
        raise AdminError("fleet lifecycle handoff has invalid fields")
    resources: list[tuple[str, str, int, Path]] = []
    seen: set[tuple[str, str]] = set()
    seen_fds: set[int] = set()
    for item in raw["locks"]:
        if not isinstance(item, dict) or set(item) != {
            "scope", "resource", "fd", "path",
        }:
            raise AdminError("fleet lifecycle handoff lock has invalid fields")
        scope = item["scope"]
        resource = item["resource"]
        fd = item["fd"]
        path = item["path"]
        if (
            scope not in {"checkout", "target"}
            or not isinstance(resource, str)
            or not resource.startswith("/")
            or not isinstance(fd, int)
            or isinstance(fd, bool)
            or fd < 3
            or not isinstance(path, str)
            or not path.startswith("/")
        ):
            raise AdminError("fleet lifecycle handoff lock is malformed")
        key = (scope, resource)
        if key in seen or fd in seen_fds:
            raise AdminError("fleet lifecycle handoff repeats a lock")
        lock_path = Path(path)
        _validate_inherited_toolset_lock(
            scope,
            resource,
            fd,
            lock_path,
        )
        seen.add(key)
        seen_fds.add(fd)
        resources.append((scope, resource, fd, lock_path))
    return _ToolsetLifecycleLock(tuple(resources))


def _inherited_toolset_lifecycle_lock() -> _ToolsetLifecycleLock | None:
    """Adopt the exact lock OFDs carried by an authenticated rollout action."""
    encoded = os.environ.get(fleet_operation.ENV_LIFECYCLE_HANDOFF)
    if encoded is None:
        return None
    return _parse_toolset_lifecycle_handoff(
        encoded,
        require_execution_context=True,
    )


@contextlib.contextmanager
def _adopt_rollout_toolset_lifecycle_handoff(
    encoded: str,
    *,
    expected_resources: tuple[tuple[str, str], ...],
):
    """Borrow a predecessor controller's exact lifecycle locks.

    The predecessor retains its descriptor copies until this fresh controller
    exits, so closing these borrowed duplicates never opens an admission gap.
    Explicit ``LOCK_UN`` is forbidden because all copies share one open-file
    description.
    """
    if getattr(_toolset_lifecycle_local, "active", None) is not None:
        raise AdminError("rollout lifecycle handoff cannot replace active locks")
    held = _parse_toolset_lifecycle_handoff(
        encoded,
        require_execution_context=False,
    )
    actual = tuple(
        sorted(
            (scope, resource)
            for scope, resource, _fd, _path in held.resources
        )
    )
    expected = tuple(sorted(expected_resources))
    if actual != expected:
        raise AdminError(
            "rollout lifecycle handoff does not match the configured "
            "controller resources"
        )
    previous = getattr(_toolset_lifecycle_local, "active", None)
    for _scope, _resource, fd, _path in held.resources:
        os.set_inheritable(fd, False)
    _toolset_lifecycle_local.active = held
    try:
        yield held
    finally:
        _toolset_lifecycle_local.active = previous
        for _scope, _resource, fd, _path in reversed(held.resources):
            with contextlib.suppress(OSError):
                os.close(fd)


@contextlib.contextmanager
def toolset_lifecycle_lock(
    progs: list[config.VenvProgram], *, action: str,
    extra_resources: tuple[tuple[str, str], ...] = (),
):
    """Fence checkout/venv mutation against direct toolset scripts.

    The lock is reentrant only when the nested resource set is already held.
    File descriptors are inherited by the monitored updater so a killed parent
    cannot expose a still-running install to another lifecycle operation.
    """
    requested: list[tuple[str, str]] = []
    for prog in progs:
        requested.append(
            ("checkout", str(_canonical_lifecycle_checkout(Path(prog.git_dir))))
        )
        requested.append(
            ("target", str(_canonical_lifecycle_target(Path(prog.python).parent.parent)))
        )
    requested.extend(extra_resources)
    keys = tuple(sorted(set(requested)))
    active: _ToolsetLifecycleLock | None = getattr(
        _toolset_lifecycle_local, "active", None,
    )
    inherited = False
    if active is None:
        active = _inherited_toolset_lifecycle_lock()
        inherited = active is not None
    if active is not None:
        active_keys = {(scope, resource) for scope, resource, _fd, _path in active.resources}
        missing = tuple(key for key in keys if key not in active_keys)
        if not missing:
            previous = getattr(_toolset_lifecycle_local, "active", None)
            if inherited:
                _toolset_lifecycle_local.active = active
            try:
                _refuse_active_multi_user_migration(active)
                yield active
            finally:
                if inherited:
                    _toolset_lifecycle_local.active = previous
            return
        # A decision phase may learn an immutable runtime-slot target only
        # after it already owns the live checkout/venv. Expand the fence
        # non-blockingly for that exact derived resource set; on contention no
        # mutation has occurred and the outer locks remain intact while the
        # error unwinds. The nested child sees the expanded handoff, and the
        # added descriptors are released when this nested scope ends.
        expanded_resources: list[tuple[str, str, int, Path]] = []
        previous = getattr(_toolset_lifecycle_local, "active", None)
        try:
            for scope, resource in missing:
                fd, path = _open_toolset_lock(scope, resource)
                expanded_resources.append((scope, resource, fd, path))
            expanded = _ToolsetLifecycleLock(
                active.resources + tuple(expanded_resources),
            )
            _toolset_lifecycle_local.active = expanded
            _refuse_active_multi_user_migration(expanded)
            yield expanded
        finally:
            _toolset_lifecycle_local.active = (
                previous if inherited else active
            )
            for _scope, _resource, fd, _path in reversed(expanded_resources):
                with contextlib.suppress(OSError):
                    os.close(fd)
        return

    opened: list[tuple[str, str, int, Path]] = []
    try:
        for scope, resource in keys:
            fd, path = _open_toolset_lock(scope, resource)
            opened.append((scope, resource, fd, path))
        held = _ToolsetLifecycleLock(tuple(opened))
        _toolset_lifecycle_local.active = held
        _refuse_active_multi_user_migration(held)
        yield held
    finally:
        _toolset_lifecycle_local.active = None
        for _scope, _resource, fd, _path in reversed(opened):
            with contextlib.suppress(OSError):
                os.close(fd)


def _current_toolset_lock_handoff(
    git_dir: Path, target: Path,
) -> tuple[dict[str, str], tuple[int, ...]]:
    active: _ToolsetLifecycleLock | None = getattr(
        _toolset_lifecycle_local, "active", None,
    )
    if active is None:
        raise AdminError("managed updater has no active toolset lifecycle lock")
    checkout = str(_canonical_lifecycle_checkout(git_dir))
    canonical_target = str(_canonical_lifecycle_target(target))
    selected = {
        scope: (resource, fd, path)
        for scope, resource, fd, path in active.resources
        if (scope == "checkout" and resource == checkout)
        or (scope == "target" and resource == canonical_target)
    }
    if set(selected) != {"checkout", "target"}:
        raise AdminError("managed updater lifecycle lock does not match its target")
    checkout_fd = selected["checkout"][1]
    target_fd = selected["target"][1]
    external_python: str | None = None
    for candidate in (
        "/usr/bin/python3",
        "/usr/local/bin/python3",
        "/opt/homebrew/bin/python3",
        "/opt/local/bin/python3",
    ):
        path = Path(candidate)
        if not path.is_file() or not os.access(path, os.X_OK):
            continue
        resolved = path.resolve()
        try:
            resolved.relative_to(Path(canonical_target))
        except ValueError:
            pass
        else:
            continue
        try:
            probe = subprocess.run(
                [str(resolved), "-I", "-S", "-c", "import os; print(os.getpid())"],
                capture_output=True,
                text=True,
                timeout=30,
                stdin=subprocess.DEVNULL,
            )
        except (subprocess.TimeoutExpired, OSError):
            continue
        if probe.returncode == 0:
            external_python = str(resolved)
            break
    if external_python is None:
        raise AdminError(
            "no trusted external Python is available for lifecycle-lock handoff"
        )
    env = {
        "VIBE_TOOLSET_ADMIN_LOCK_PID": str(os.getpid()),
        "VIBE_TOOLSET_ADMIN_LOCK_CHECKOUT": checkout,
        "VIBE_TOOLSET_ADMIN_LOCK_TARGET": canonical_target,
        "VIBE_TOOLSET_ADMIN_LOCK_CHECKOUT_PATH": str(selected["checkout"][2]),
        "VIBE_TOOLSET_ADMIN_LOCK_TARGET_PATH": str(selected["target"][2]),
        "VIBE_TOOLSET_ADMIN_LOCK_CHECKOUT_FD": str(checkout_fd),
        "VIBE_TOOLSET_ADMIN_LOCK_TARGET_FD": str(target_fd),
        "VIBE_TOOLSET_ADMIN_LOCK_PYTHON": external_python,
    }
    return env, (checkout_fd, target_fd)


@contextlib.contextmanager
def admin_update_ownership():
    """Hold the local ref/checkout mutation boundary for one update.

    This lock is deliberately broader than the durable update marker.  A
    first-class self-update must authenticate/fetch its accepted report before
    it can create that marker, while an ordinary admin update could otherwise
    mutate the same checkout concurrently.  The lock is process- and
    thread-exclusive, non-blocking (matching the marker's refusal semantics),
    and reentrant only for the owning thread so ``self-update`` can span report
    discovery and then call the ordinary ``update_env`` implementation.
    """
    binding = _capture_admin_state_binding()
    depth = getattr(_admin_update_ownership_local, "depth", 0)
    if depth:
        if (
            getattr(_admin_update_ownership_local, "pid", None) != os.getpid()
            or getattr(_admin_update_ownership_local, "thread", None)
            != threading.get_ident()
            or getattr(_admin_update_ownership_local, "binding", None) != binding
        ):
            raise AdminUpdateInProgress(
                "admin-update ownership cannot be inherited or rebound"
            )
        _admin_update_ownership_local.depth = depth + 1
        try:
            yield
        finally:
            _admin_update_ownership_local.depth -= 1
        return

    if not _admin_update_ownership_process_lock.acquire(blocking=False):
        raise AdminUpdateInProgress(
            "this checkout is being mutated by another admin operation; "
            "wait for it to finish, then retry"
        )
    fd = -1
    try:
        fd, path = _open_admin_update_ownership_lock(binding)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise AdminUpdateInProgress(
                "this checkout is being mutated by another admin operation; "
                "wait for it to finish, then retry"
            ) from exc
        locked_info = os.fstat(fd)
        if locked_info.st_nlink != 1:
            raise AdminError(
                f"unsafe admin-update ownership lock {path}: lock inode was "
                "unlinked during acquisition"
            )
        _validate_admin_state_binding(binding)
        _admin_update_ownership_local.pid = os.getpid()
        _admin_update_ownership_local.thread = threading.get_ident()
        _admin_update_ownership_local.binding = binding
        _admin_update_ownership_local.depth = 1
        try:
            yield
        finally:
            _admin_update_ownership_local.depth = 0
            _admin_update_ownership_local.pid = None
            _admin_update_ownership_local.thread = None
            _admin_update_ownership_local.binding = None
    finally:
        if fd >= 0:
            _close_admin_lock_fd(fd)
        _admin_update_ownership_process_lock.release()


_admin_update_marker_local = threading.local()
_admin_update_marker_process_lock = threading.Lock()


@contextlib.contextmanager
def _admin_update_marker_lock(
    binding: _AdminStateBinding | None = None,
):
    """Serialize every marker mutation in one physical registry.

    Cross-process writers retain the historical blocking ``flock`` contract.
    Another thread in this process is refused rather than deadlocking, while
    only the exact owning PID/thread/binding may re-enter.  Releasing by close
    (never explicit ``LOCK_UN``) makes inherited-descriptor cleanup harmless.
    """
    binding = binding or _capture_admin_state_binding()
    depth = getattr(_admin_update_marker_local, "depth", 0)
    if depth:
        if (
            getattr(_admin_update_marker_local, "pid", None) != os.getpid()
            or getattr(_admin_update_marker_local, "thread", None)
            != threading.get_ident()
            or getattr(_admin_update_marker_local, "binding", None) != binding
        ):
            raise AdminUpdateInProgress(
                "admin-update marker admission cannot be inherited or rebound"
            )
        _admin_update_marker_local.depth = depth + 1
        try:
            yield binding
        finally:
            _admin_update_marker_local.depth -= 1
        return

    if not _admin_update_marker_process_lock.acquire(blocking=False):
        raise AdminUpdateInProgress(
            "another thread owns admin-update marker admission"
        )
    fd = -1
    try:
        _validate_admin_state_binding(binding)
        fd = _open_secure_admin_lock(
            binding.marker_lock_path,
            label="admin-update marker lock",
        )
        fcntl.flock(fd, fcntl.LOCK_EX)
        info = os.fstat(fd)
        current = os.stat(binding.marker_lock_path, follow_symlinks=False)
        if (
            info.st_nlink != 1
            or (current.st_dev, current.st_ino) != (info.st_dev, info.st_ino)
        ):
            raise AdminError(
                "admin-update marker lock inode changed during acquisition"
            )
        _validate_admin_state_binding(binding)
        _admin_update_marker_local.pid = os.getpid()
        _admin_update_marker_local.thread = threading.get_ident()
        _admin_update_marker_local.binding = binding
        _admin_update_marker_local.depth = 1
        try:
            yield binding
        finally:
            _admin_update_marker_local.depth = 0
            _admin_update_marker_local.pid = None
            _admin_update_marker_local.thread = None
            _admin_update_marker_local.binding = None
    finally:
        if fd >= 0:
            _close_admin_lock_fd(fd)
        _admin_update_marker_process_lock.release()


_admin_update_marker_owner = threading.local()


def _active_admin_state_binding() -> _AdminStateBinding | None:
    binding = getattr(_admin_update_marker_local, "binding", None)
    return binding if isinstance(binding, _AdminStateBinding) else None


def _set_owned_admin_update_marker_path(path: Path | None) -> None:
    _admin_update_marker_owner.path = path
    _admin_update_marker_owner.state_root = (
        paths.state_root().expanduser().resolve(strict=False)
        if path is not None
        else None
    )
    if path is not None:
        _admin_update_marker_owner.forked_without_owner = False


def _owned_admin_update_marker_path() -> Path | None:
    path = getattr(_admin_update_marker_owner, "path", None)
    if not isinstance(path, Path):
        return None
    root = paths.state_root().expanduser().resolve(strict=False)
    owned_root = getattr(_admin_update_marker_owner, "state_root", None)
    resolved = path.expanduser().resolve(strict=False)
    if owned_root != root:
        raise AdminError(
            "owned admin-update marker path belongs to a previous state root: "
            f"cached root {owned_root} does not match {root}. Refusing to "
            f"reuse {resolved} after {paths.ENV_STATE_DIR} changed."
        )
    if resolved != root and root not in resolved.parents:
        raise AdminError(
            f"owned admin-update marker path {resolved} is outside its bound "
            f"state root {root}"
        )
    return path


def _admin_update_marker_paths(
    binding: _AdminStateBinding | None = None,
) -> list[Path]:
    result: list[Path] = []
    binding = binding or _active_admin_state_binding()
    legacy = binding.marker_path if binding is not None else admin_update_marker_path()
    if os.path.lexists(legacy):
        result.append(legacy)
    directory = binding.marker_dir if binding is not None else admin_update_marker_dir()
    if directory.is_dir():
        result.extend(sorted(directory.glob("*.json")))
    return result


def _read_admin_update_marker_path(path: Path) -> AdminUpdateMarker | None:
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    try:
        return AdminUpdateMarker(**data)
    except TypeError:
        return None


def _admin_update_marker_entries(
) -> list[tuple[Path, AdminUpdateMarker | None]]:
    return [
        (path, _read_admin_update_marker_path(path))
        for path in _admin_update_marker_paths()
    ]


def admin_update_marker_exists() -> bool:
    """v0.5.44: cheap stat-based existence check. Used by the
    update_env / update_all guards — they only need to know
    "is anything there?", not the contents. Avoids JSON-parse cost
    on the hot path."""
    return bool(_admin_update_marker_paths())


def read_admin_update_marker() -> AdminUpdateMarker | None:
    """v0.5.44: parsed view of the marker, or None when:
      * the marker file doesn't exist, or
      * the file exists but isn't readable / parseable.

    Conservative-on-malformed: returns None so callers fall back to
    the "marker present but unreadable" display path rather than
    raising. The guard path checks
    :func:`admin_update_marker_exists` instead, which fires on ANY
    file (parseable or not) — so a corrupt marker still blocks new
    updates."""
    entries = _admin_update_marker_entries()
    return entries[0][1] if entries else None


def read_admin_update_markers() -> list[AdminUpdateMarker]:
    """Return every parseable marker lease in stable path order."""
    return [
        marker
        for _, marker in _admin_update_marker_entries()
        if marker is not None
    ]


def _write_admin_update_marker_atomic(
    marker: AdminUpdateMarker, *, path: Path | None = None,
) -> None:
    """Persist ``marker`` via tmpfile + replace.

    The marker is read by the daemon while admin updates are running, so
    every rewrite must be atomic against partial JSON reads.
    """
    binding = _active_admin_state_binding() or _capture_admin_state_binding()
    with _admin_update_marker_lock(binding):
        _write_admin_update_marker_atomic_locked(marker, path=path, binding=binding)


def _write_admin_update_marker_atomic_locked(
    marker: AdminUpdateMarker,
    *,
    path: Path | None,
    binding: _AdminStateBinding,
) -> None:
    """Atomic marker rewrite with exact marker admission already held."""
    _validate_admin_state_binding(binding)
    path = path or _owned_admin_update_marker_path() or binding.marker_path
    physical_parent = path.parent.resolve(strict=False)
    if physical_parent not in {binding.state_root, binding.marker_dir}:
        raise AdminError(
            f"admin-update marker path {path} is outside its bound registry"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    payload = json.dumps(asdict(marker), indent=2, sort_keys=True).encode("utf-8")
    fd = -1
    try:
        fd = os.open(
            str(tmp),
            os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_CLOEXEC", 0),
            0o600,
        )
        os.write(fd, payload)
        os.fsync(fd)
        os.close(fd)
        fd = -1
        os.replace(tmp, path)
    finally:
        if fd >= 0:
            os.close(fd)
        tmp.unlink(missing_ok=True)
    directory_fd = os.open(
        str(path.parent), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    _validate_admin_state_binding(binding)


# ----------------------------------------------------------------------
# v0.11.0: stale-marker detection
#
# The marker is meant to gate dispatch *while an update is in flight* and
# to block the next `vq admin update` until a human acknowledges a
# possibly-half-built venv. The bug it grew: nothing ever checked that
# the writing process was still alive, so a marker left by a killed
# update (or one that outlived a reboot — the marker persists in the
# state dir) gated the daemon's dispatch loop forever. The daemon showed
# plain `up`/`OK` while silently parking every job at `pending`.
#
# These helpers let the daemon (dispatch gate + startup reap) and the
# reporting path (`vq overview`) agree on one question: "is this marker a
# live update, or a corpse?" Marker liveness is owned here and consumed
# through `admin_update_marker_stale_reason`. PID start-time parsing mirrors
# daemon._read_pid_start_time rather than importing the daemon; that live
# daemon helper serves orphan-job recovery, and this marker path stays
# decoupled from the load-bearing module.
# ----------------------------------------------------------------------


def _pid_liveness(pid: int) -> bool | None:
    """Probe ``os.kill(pid, 0)``. True if ``pid`` is in the process
    table, False if not, None when we can't tell (pid <= 0, or an
    unexpected OSError). PermissionError counts as True — the process
    exists, we just don't own it. This is the marker-staleness owner."""
    if pid is None or pid <= 0:
        return None
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return None
    return True


def _pid_start_time(pid: int) -> int | None:
    """Read ``/proc/<pid>/stat`` field 22 (start time in clock ticks
    since boot), or None on any failure (macOS / no /proc, process
    gone, malformed line). The anti-recycling fingerprint. Mirror of
    ``daemon._read_pid_start_time`` — see that docstring for the
    field-22 parsing rationale (comm can contain spaces and parens)."""
    if pid is None or pid <= 0:
        return None
    try:
        with open(f"/proc/{pid}/stat", encoding="latin-1") as f:
            raw = f.read()
    except OSError:
        return None
    rparen = raw.rfind(")")
    if rparen == -1:
        return None
    after = raw[rparen + 1 :].split()
    if len(after) < 20:
        return None
    try:
        return int(after[19])
    except ValueError:
        return None


def _iso_age_seconds(ts: str, *, now_iso: str | None = None) -> float | None:
    """Seconds elapsed since ISO timestamp ``ts``.

    Returns None when the timestamp is missing or unparseable. Tolerant
    of a trailing ``Z``. Negative deltas (clock skew / a timestamp stamped
    slightly in the future) clamp to 0.
    """
    if not ts:
        return None
    try:
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        started = datetime.fromisoformat(ts)
    except ValueError:
        return None
    if started.tzinfo is None:
        started = started.replace(tzinfo=UTC)
    now = (
        datetime.fromisoformat(now_iso)
        if now_iso is not None
        else datetime.now(UTC)
    )
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    return max(0.0, (now - started).total_seconds())


def _marker_age_seconds(
    marker: AdminUpdateMarker, *, now_iso: str | None = None
) -> float | None:
    """Seconds elapsed since ``marker.started_at``."""
    return _iso_age_seconds(marker.started_at, now_iso=now_iso)


def _format_marker_age(seconds: float) -> str:
    """Compact age for admin marker status lines."""
    if seconds < 90:
        return f"{seconds:.0f}s"
    minutes = seconds / 60.0
    if minutes < 90:
        return f"{minutes:.0f}m"
    hours = minutes / 60.0
    if hours < 48:
        return f"{hours:.1f}h"
    days = hours / 24.0
    return f"{days:.1f}d"


def _marker_heartbeat_status(
    marker: AdminUpdateMarker,
) -> tuple[str | None, float | None]:
    """Return human status text and age for the marker heartbeat."""
    if not marker.last_heartbeat_at:
        return None, None
    age = _iso_age_seconds(marker.last_heartbeat_at)
    if age is None:
        text = f"last heartbeat at {marker.last_heartbeat_at}"
    else:
        text = f"last heartbeat {_format_marker_age(age)} ago"
    if marker.last_heartbeat_message:
        text = f"{text}: {marker.last_heartbeat_message}"
    return text, age


def admin_update_marker_stale_reason(
    marker: AdminUpdateMarker | None,
    *,
    now_iso: str | None = None,
    max_age_seconds: float = ADMIN_UPDATE_MARKER_MAX_AGE_SECONDS,
) -> str | None:
    """v0.11.0: return a human-readable reason the marker is **stale**
    (its writing `vq admin update` is gone), or None if it looks like a
    live in-flight update. Staleness does not by itself decide dispatch:
    the daemon auto-reaps ordinary stale markers but retains durable
    managed-transaction and pause-scope receipts for explicit recovery.

    Conservative by construction — a live update (incl. the
    daemon-restart window, where the update process stays alive across
    the restart) must never be judged stale, or we'd race dispatch
    against a venv mutation. The signals, in order:

      1. **pid is gone** (``os.kill`` → ProcessLookupError). Definitive:
         the writer exited. This is the common live-host case (a killed
         or crashed update).
      2. **pid was recycled.** pid reads alive but its ``/proc`` start
         time differs from the one recorded at write — the kernel reused
         the slot for a stranger (classic after a reboot). Needs a
         recorded ``pid_start_time`` (v0.11.0+ markers) and ``/proc``.
      3. **age backstop.** Older than ``max_age_seconds`` — no real
         update runs this long. Catches recycled pids we couldn't
         fingerprint (pre-v0.11.0 marker, or no /proc) and indeterminate
         liveness.

    ``vq_version`` mismatch is deliberately *not* a trigger: during a
    normal self-update the marker is written by the old vq, the daemon
    restarts into the new vq, and the still-running update process then
    clears the marker — so a live marker legitimately shows an older
    ``vq_version`` than the running daemon. It's surfaced in the warning
    text for context, but acting on it would false-positive that window.
    """
    if marker is None:
        return None
    pid_alive = _pid_liveness(marker.pid)
    if pid_alive is False:
        return (
            f"the `vq admin update` process (pid={marker.pid}) that wrote "
            f"this marker is no longer running"
        )
    if pid_alive is True and marker.pid_start_time:
        live_start = _pid_start_time(marker.pid)
        if live_start is not None and live_start != marker.pid_start_time:
            return (
                f"pid={marker.pid} was recycled to a different process "
                f"(/proc start-time {live_start} != recorded "
                f"{marker.pid_start_time}); the original update is gone"
            )
    age = _marker_age_seconds(marker, now_iso=now_iso)
    if age is not None and age > max_age_seconds:
        hours = age / 3600.0
        return (
            f"marker is {hours:.1f}h old (> {max_age_seconds / 3600.0:.0f}h "
            f"bound); no in-flight admin update runs this long"
        )
    return None


_detached_run_local = threading.local()


def set_detached_run_id(run_id: str | None) -> None:
    """Declare that this process is the detached updater for ``run_id``.

    Called once by the detached child before it takes the marker, so the
    marker it writes names the run whose transcript and terminal receipt
    explain it. Deliberately an explicit call rather than an inherited
    environment variable: a stale value picked up from a parent shell would
    attach an unrelated update to somebody else's run record, and every other
    ambient capability in this module is popped for exactly that reason.
    """
    _detached_run_local.run_id = run_id


def current_detached_run_id() -> str | None:
    """The detached run this process owns, or ``None`` when attached."""
    return getattr(_detached_run_local, "run_id", None)


def write_admin_update_marker(
    envs: list[str], host: str
) -> AdminUpdateMarker:
    """v0.5.44: write the marker atomically. Unlike
    :func:`record_update_outcome`, this is NOT best-effort —
    a write failure raises :class:`OSError` and the caller bails
    before pausing the queue. Better to refuse the update than to
    run it without the safety net.

    Atomic-write pattern: tmpfile + ``os.replace``. Mirrors
    :func:`write_admin_status` so future schema-evolution work
    can share the same tooling."""
    from vq import __version__ as vq_version

    _refuse_live_per_user_state_under_pytest()

    now = utcnow_iso()
    marker = AdminUpdateMarker(
        envs=list(envs),
        host=host,
        started_at=now,
        pid=os.getpid(),
        vq_version=vq_version,
        pid_start_time=_pid_start_time(os.getpid()) or 0,
        state=ADMIN_UPDATE_STATE_PAUSING,
        phase_started_at=now,
        last_heartbeat_at=now,
        last_heartbeat_message=f"state={ADMIN_UPDATE_STATE_PAUSING}",
        marker_id=uuid.uuid4().hex,
        detached_run_id=current_detached_run_id(),
    )
    path = admin_update_marker_path()
    _write_admin_update_marker_atomic(marker, path=path)
    _set_owned_admin_update_marker_path(path)
    return marker


def _format_marker_detail(marker: AdminUpdateMarker | None) -> str:
    """Helper: produce the diagnostic detail used by AdminError when
    refusing to proceed past an existing marker. Centralised so the
    O_EXCL-collision message (acquire_admin_update_marker) and the
    pre-check error (_guard_admin_update_marker) read identically."""
    if marker is None:
        return "(unreadable — file present but JSON parse failed)"
    diag = diagnose_admin_update_marker(marker)
    pieces = [
        f"envs={marker.envs}, host={marker.host}, "
        f"started={marker.started_at}, pid={marker.pid}, "
        f"state={marker.state}, marker_status={diag.marker_status}, "
        f"vq_version={marker.vq_version}",
    ]
    if diag.pid_status:
        pieces.append(diag.pid_status)
    if diag.heartbeat_status:
        pieces.append(diag.heartbeat_status)
    if diag.stale_reason:
        pieces.append(f"stale_reason={diag.stale_reason}")
    if marker.failure_reason:
        pieces.append(f"failure={marker.failure_reason}")
    return "; ".join(pieces)


def diagnose_admin_update_marker(
    marker: AdminUpdateMarker | None,
) -> AdminUpdateMarkerDiagnosis:
    """Classify a marker for update/status reporting.

    A present marker is not always a failed update. The common delegated
    SSH case is that the first ``vq admin update`` is still compiling on the
    remote host, and a retry merely sees its live marker. Treat live markers
    as "wait/check status"; reserve the clear/force recipe for failed or
    stale markers.
    """
    if marker is None:
        return AdminUpdateMarkerDiagnosis(
            marker_status=ADMIN_UPDATE_MARKER_DIAG_UNREADABLE,
            summary="marker file is present but cannot be parsed",
            action=(
                "Inspect the marker file on this host, then run "
                "`vq admin clear-update-marker` only after confirming no "
                "admin update is still running."
            ),
        )
    heartbeat_status, heartbeat_age = _marker_heartbeat_status(marker)
    if marker.managed_transaction is not None or marker.owns_pause_scope:
        stale_reason = admin_update_marker_stale_reason(marker)
        pid_alive = _pid_liveness(marker.pid)
        status = (
            ADMIN_UPDATE_MARKER_DIAG_RUNNING
            if stale_reason is None
            else ADMIN_UPDATE_MARKER_DIAG_STALE
        )
        return AdminUpdateMarkerDiagnosis(
            marker_status=status,
            summary=(
                "durable admin update is still running"
                if stale_reason is None
                else f"durable admin update needs recovery: {stale_reason}"
            ),
            action=(
                "Wait for the live updater and inspect `vq admin status --verbose`."
                if stale_reason is None
                else (
                    "Run `vq admin recover-update` for a managed transaction. "
                    "For a pause-only receipt, inspect the env then run "
                    "`vq admin clear-update-marker`, which proves the exact "
                    "paused scope resumed before clearing. Never use --force."
                )
            ),
            pid_status=(
                f"pid={marker.pid} appears alive"
                if pid_alive is True
                else f"pid={marker.pid} is not running"
                if pid_alive is False
                else f"pid={marker.pid} liveness unknown"
            ),
            stale_reason=stale_reason,
            heartbeat_status=heartbeat_status,
            heartbeat_age_seconds=heartbeat_age,
        )
    if marker.state == ADMIN_UPDATE_STATE_FAILED:
        reason = marker.failure_reason or "admin update recorded failed"
        return AdminUpdateMarkerDiagnosis(
            marker_status=ADMIN_UPDATE_MARKER_DIAG_FAILED,
            summary=f"previous admin update failed: {reason}",
            action=(
                "Inspect `vq admin status --verbose`, then either run "
                "`vq admin clear-update-marker` to acknowledge the failed "
                "update or re-run `vq admin update --force` after verifying "
                "the checkout."
            ),
            stale_reason=reason,
            heartbeat_status=heartbeat_status,
            heartbeat_age_seconds=heartbeat_age,
        )
    stale_reason = admin_update_marker_stale_reason(marker)
    pid_alive = _pid_liveness(marker.pid)
    if pid_alive is True:
        pid_status = f"pid={marker.pid} appears alive"
    elif pid_alive is False:
        pid_status = f"pid={marker.pid} is not running"
    else:
        pid_status = f"pid={marker.pid} liveness unknown"
    if stale_reason is not None:
        return AdminUpdateMarkerDiagnosis(
            marker_status=ADMIN_UPDATE_MARKER_DIAG_STALE,
            summary=f"admin-update marker is stale: {stale_reason}",
            action=(
                "Inspect `vq admin status --verbose`, then run "
                "`vq admin clear-update-marker` or re-run "
                "`vq admin update --force` after verifying the checkout."
            ),
            pid_status=pid_status,
            stale_reason=stale_reason,
            heartbeat_status=heartbeat_status,
            heartbeat_age_seconds=heartbeat_age,
        )
    summary = (
        "admin update is already running on this host; this may be the "
        "remote updater from an earlier delegated command"
    )
    if heartbeat_status:
        summary = f"{summary} ({heartbeat_status})"
    return AdminUpdateMarkerDiagnosis(
        marker_status=ADMIN_UPDATE_MARKER_DIAG_RUNNING,
        summary=summary,
        action=(
            "Wait for it to finish, or check progress with "
            "`vq admin status --verbose` on this host "
            "(from a delegating machine: `vq admin status HOST --verbose`)."
        ),
        pid_status=pid_status,
        heartbeat_status=heartbeat_status,
        heartbeat_age_seconds=heartbeat_age,
    )


@contextlib.contextmanager
def admin_run_log(target: str, *, what: str, multi_user: bool = False):
    """Open a transcript for one admin operation and narrate into it.

    Every ``vq admin update`` path wraps its work in this. The transcript
    captures the phase narration, each heartbeat, and the *complete* build
    output — not the 80-line tail that was previously all that survived — so
    `vq admin logs` can answer "what actually happened at 17:57" without
    re-running a two-hour deploy.

    Best-effort throughout: an unwritable log directory degrades to narration
    only. Yields the :class:`~vq.output.RunLog` (whose ``.path`` the caller
    records in the result) or ``None`` when logging is disabled.
    """
    started = utcnow_iso()
    path = paths.admin_update_logfile(target, started, multi_user=multi_user)
    run_log = output.RunLog(
        path,
        header=(
            f"# vq {what}\n"
            f"# target:   {target}\n"
            f"# started:  {started}\n"
            f"# vq:       {_vq_version()}\n"
            f"# pid:      {os.getpid()}\n"
        ),
    )
    # A detached run publishes its transcript path the moment the path exists,
    # which is here. Without it the driver could poll state but not follow the
    # build, and "still compiling" with no output is the silence this whole
    # logging layer was added to end.
    detached_run = current_detached_run_id()
    if detached_run is not None and run_log.available:
        with contextlib.suppress(OSError, admin_detached.DetachedRunError):
            admin_detached.publish_transcript(
                detached_run, path, multi_user=multi_user
            )
    try:
        with output.channel(run_log=run_log):
            yield run_log
    finally:
        run_log.stamp(f"# finished: {utcnow_iso()}")
        run_log.close()
        # Prune here rather than in a cleanup sweep: the bound is part of the
        # feature, and this is the only moment we know the target name.
        with contextlib.suppress(OSError):
            paths.prune_admin_update_logs(target, multi_user=multi_user)


def _vq_version() -> str:
    from vq import __version__

    return __version__


def _owned_admin_update_marker_entry(
) -> tuple[Path, AdminUpdateMarker] | None:
    """Find the lease owned by this update process.

    The thread-local path is authoritative. The PID fallback preserves
    pre-scope tests and update processes that crossed a source reload.
    """
    owned = _owned_admin_update_marker_path()
    entries = _admin_update_marker_entries()
    current_paths = {path for path, _ in entries}
    if owned is not None and owned in current_paths:
        marker = _read_admin_update_marker_path(owned)
        if marker is not None and marker.pid == os.getpid():
            return owned, marker
    matches = [
        (path, marker)
        for path, marker in entries
        if marker is not None and marker.pid == os.getpid()
    ]
    if len(matches) == 1:
        _set_owned_admin_update_marker_path(matches[0][0])
        return matches[0][0], matches[0][1]
    return None


def refresh_admin_update_marker_heartbeat(
    message: str | None = None,
) -> AdminUpdateMarker | None:
    """Refresh the live admin-update marker heartbeat.

    Only the process that owns the marker may refresh it. This keeps an
    unrelated process from making a stale marker look fresh if it happens
    to call the helper while an old marker is present.
    """
    entry = _owned_admin_update_marker_entry()
    existing = entry[1] if entry is not None else None
    if existing is None or existing.pid != os.getpid():
        return existing
    existing.last_heartbeat_at = utcnow_iso()
    existing.last_heartbeat_message = message
    _write_admin_update_marker_atomic(existing, path=entry[0])
    # Every long-running admin step already heartbeats here (build progress,
    # staging retries, activation-settle waits, drain-wait polls), so this one
    # line gives all of them live narration and a run-log entry. Before it, the
    # marker was the ONLY place that progress existed: an operator had to run
    # `vq admin status` in another terminal to discover that a silent two-hour
    # command was in fact working.
    if message:
        output.narrate(message)
    return existing


def transition_admin_update_state(
    new_state: str, *, failure_reason: str | None = None,
) -> AdminUpdateMarker | None:
    """v0.6.0: rewrite the marker file with a new phase. Returns the
    updated marker (or None if the marker is gone — caller should
    treat as a no-op).

    Single-writer assumption: only the `vq admin update` process
    that wrote the initial marker via :func:`acquire_admin_update_marker`
    calls this. Concurrent updates are prevented at the acquire step
    by the O_CREAT|O_EXCL atomic check (v0.5.50). Each phase
    transition rewrites the file via tmpfile + os.replace — atomic
    against partial reads from the daemon's per-tick marker check.

    Used by :func:`update_env` and :func:`update_all` to drive the
    state machine through PAUSING → PAUSED → PULLING → ... →
    VERIFYING. Sticky FAILED is the only terminal in-file value;
    success removes the file entirely (via
    :func:`clear_admin_update_marker`)."""
    entry = _owned_admin_update_marker_entry()
    existing = entry[1] if entry is not None else None
    if existing is None:
        return None
    existing.state = new_state
    now = utcnow_iso()
    existing.phase_started_at = now
    existing.failure_reason = failure_reason
    existing.last_heartbeat_at = now
    if failure_reason:
        existing.last_heartbeat_message = f"state={new_state}: {failure_reason}"
    else:
        existing.last_heartbeat_message = f"state={new_state}"
    _write_admin_update_marker_atomic(existing, path=entry[0])
    # Phase narration: the operator-visible spine of a long update. Failures
    # are QUIET so they survive even `--quiet`; ordinary phase changes are
    # NORMAL.
    if failure_reason:
        output.narrate(f"[{new_state}] {failure_reason}", output.Level.QUIET)
    else:
        output.narrate(f"[{new_state}]")
    return existing


MANAGED_UPDATE_RECEIPT_SCHEMA = "vq-managed-daemon-update-v1"
MANAGED_UPDATE_RECEIPT_PHASES = frozenset(
    {
        "armed", "backup_moved", "target_ready", "target_verified",
        "restoring_old", "files_restored", "old_restored",
        "target_committed", "target_cleanup_pending",
    }
)


def _admin_pause_queue_root(*, multi_user: bool) -> Path:
    """Return the normalized root that uniquely identifies a queue namespace."""
    root = paths.multi_user_root() if multi_user else paths.state_root()
    return root.expanduser().resolve(strict=False)


def _require_admin_update_pause_queue_scope(
    marker: AdminUpdateMarker,
) -> tuple[bool, Path]:
    """Validate and return one marker's exact persisted queue namespace.

    Both fields are required whenever a marker owns a pause token.  Comparing
    the normalized root to the current path policy prevents a tampered receipt
    from redirecting recovery to an arbitrary tree, and prevents an environment
    override change from making an empty scan look like proof of resume.
    """
    if type(marker.pause_multi_user) is not bool:
        raise AdminError(
            "admin update marker has no durable multi-user queue namespace; "
            "refusing to guess from current configuration"
        )
    if not isinstance(marker.pause_queue_root, str) or not marker.pause_queue_root:
        raise AdminError(
            "admin update marker has no durable queue-root identity; refusing "
            "to scan or clear its paused-job scope"
        )
    stored = Path(marker.pause_queue_root).expanduser()
    if not stored.is_absolute():
        raise AdminError("admin update marker queue-root identity is not absolute")
    normalized = stored.resolve(strict=False)
    if str(normalized) != marker.pause_queue_root:
        raise AdminError(
            "admin update marker queue-root identity is not normalized"
        )
    expected = _admin_pause_queue_root(multi_user=marker.pause_multi_user)
    if normalized != expected:
        raise AdminError(
            "admin update marker queue-root identity no longer matches path "
            f"policy (receipt={normalized}, current={expected}); restore the "
            "original queue-root configuration before recovery"
        )
    return marker.pause_multi_user, normalized


def _record_admin_update_pause_scope(
    *,
    pause_token: str,
    paused_jobids: list[str],
    surgical: bool,
    multi_user: bool,
) -> None:
    """Durably bind this invocation's exact resume scope to its marker."""
    entry = _owned_admin_update_marker_entry()
    if entry is None or entry[1].pid != os.getpid():
        raise AdminError(
            "cannot persist admin-update pause scope without the owned marker"
        )
    marker = entry[1]
    marker.pause_token = pause_token
    marker.pause_multi_user = bool(multi_user)
    marker.pause_queue_root = str(
        _admin_pause_queue_root(multi_user=multi_user)
    )
    marker.paused_jobids = list(paused_jobids)
    marker.surgical_pause = bool(surgical)
    _write_admin_update_marker_atomic(marker, path=entry[0])


def _disarm_proven_pause_scope_without_managed_receipt() -> None:
    """Drop pause authority only after the caller proved the token clear.

    A managed receipt retains the token until files, service, and job scope
    are cleared together.  An ordinary failed update keeps its safety marker
    but no longer needs to advertise pause recovery once the locked second
    scan proved every tagged process resumed.
    """
    entry = _owned_admin_update_marker_entry()
    if entry is None or entry[1].pid != os.getpid():
        return
    marker = entry[1]
    if marker.managed_transaction is not None:
        return
    marker.pause_token = None
    marker.pause_multi_user = None
    marker.pause_queue_root = None
    marker.paused_jobids = []
    marker.surgical_pause = False
    _write_admin_update_marker_atomic(marker, path=entry[0])


def _clear_completed_admin_update_marker(
    marker: AdminUpdateMarker | None = None,
) -> AdminUpdateMarker | None:
    """Atomically remove an exactly completed receipt/pause lease.

    This deliberately bypasses the public clear refusal and is called only
    after strict service/file verification plus ``ResumeScopeProof``.  The
    on-disk marker remains fully armed until the unlink, so process death
    before this function cannot lose either recovery authority.
    """
    with _admin_update_marker_lock() as binding:
        entries = _admin_update_marker_entries()
        owned = _owned_admin_update_marker_path()
        target: tuple[Path, AdminUpdateMarker | None] | None = None
        if marker is not None:
            target = next(
                (
                    entry for entry in entries
                    if entry[1] is not None
                    and entry[1].marker_id == marker.marker_id
                ),
                None,
            )
        elif owned is not None:
            target = next((entry for entry in entries if entry[0] == owned), None)
        if target is None:
            raise AdminUpdateInProgress(
                "completed admin-update marker disappeared before terminal clear"
            )
        path, snapshot = target
        path.unlink()
        _fsync_directory_path(path.parent)
        if path == owned:
            _set_owned_admin_update_marker_path(None)
        with contextlib.suppress(OSError):
            binding.marker_dir.rmdir()
        return snapshot


def _managed_update_receipt_payload(
    lifecycle: _ManagedDaemonUpdate,
) -> dict[str, object]:
    if lifecycle.receipt_phase not in MANAGED_UPDATE_RECEIPT_PHASES:
        raise AdminError(
            f"invalid managed update receipt phase {lifecycle.receipt_phase!r}"
        )
    if lifecycle.venv_backup is None:
        raise AdminError("managed update receipt has no rollback virtualenv path")
    return {
        "schema": MANAGED_UPDATE_RECEIPT_SCHEMA,
        "transaction_id": lifecycle.transaction_id,
        "owner_uid": lifecycle.owner_uid,
        "env": lifecycle.env,
        "phase": lifecycle.receipt_phase,
        "manager": lifecycle.manager.value,
        "pre_pid": lifecycle.pre_pid,
        "was_running": lifecycle.was_running,
        "pre_source_sha": lifecycle.pre_source_sha,
        "pre_source_tree_sha256": lifecycle.pre_source_tree_sha256,
        "pre_checkout_branch": lifecycle.pre_checkout_branch,
        "git_dir": str(Path(lifecycle.venv_path).parent),
        "venv_path": str(lifecycle.venv_path),
        "venv_backup": str(lifecycle.venv_backup),
        "backup_moved": lifecycle.backup_moved,
        "service_executable": lifecycle.service_executable,
        "service_command": list(lifecycle.service_command),
        "target_source_sha": lifecycle.target_source_sha,
        "target_source_tree_sha256": lifecycle.target_source_tree_sha256,
        "updated_at": utcnow_iso(),
    }


def _persist_managed_update_receipt(
    prog: config.VenvProgram,
    lifecycle: _ManagedDaemonUpdate,
) -> None:
    """Fsync the exact serving-venv transaction before each unsafe boundary."""
    entry = _owned_admin_update_marker_entry()
    if entry is None or entry[1].pid != os.getpid():
        raise AdminError(
            "cannot arm a managed daemon update without the owned admin marker"
        )
    marker = entry[1]
    if lifecycle.env not in marker.envs:
        raise AdminError(
            f"managed transaction env {lifecycle.env!r} is outside marker scope"
        )
    payload = _managed_update_receipt_payload(lifecycle)
    payload["git_dir"] = str(_canonical_lifecycle_checkout(Path(prog.git_dir)))
    marker.managed_transaction = payload
    _write_admin_update_marker_atomic(marker, path=entry[0])


def _clear_managed_update_receipt() -> None:
    entry = _owned_admin_update_marker_entry()
    if entry is None or entry[1].pid != os.getpid():
        return
    marker = entry[1]
    if marker.managed_transaction is None:
        return
    marker.managed_transaction = None
    _write_admin_update_marker_atomic(marker, path=entry[0])


def _clear_managed_update_receipt_for_marker(
    marker_id: str,
    transaction_id: str,
) -> None:
    """Clear only the exact durable receipt owned by a recovery invocation."""
    for path, marker in _admin_update_marker_entries():
        if marker is None or marker.marker_id != marker_id:
            continue
        raw = marker.managed_transaction
        if not isinstance(raw, dict) or raw.get("transaction_id") != transaction_id:
            raise AdminUpdateInProgress(
                "managed update receipt changed before recovery completion"
            )
        marker.managed_transaction = None
        _write_admin_update_marker_atomic(marker, path=path)
        return
    raise AdminUpdateInProgress("managed update marker disappeared during recovery")


def _require_full_hex(value: object, *, length: int, field_name: str) -> str:
    if not isinstance(value, str):
        raise AdminError(f"managed receipt {field_name} must be a string")
    normal = value.lower()
    pattern = _FULL_SHA_RE if length == 40 else _SHA256_RE
    if pattern.fullmatch(normal) is None:
        raise AdminError(
            f"managed receipt {field_name} must be exactly {length} hex characters"
        )
    return normal


def _parse_managed_update_receipt(
    marker: AdminUpdateMarker,
    cfg: config.Config,
) -> tuple[config.VenvProgram, _ManagedDaemonUpdate]:
    raw = marker.managed_transaction
    if not isinstance(raw, dict) or raw.get("schema") != MANAGED_UPDATE_RECEIPT_SCHEMA:
        raise AdminError("admin marker has no recognized managed update receipt")
    _require_admin_update_pause_queue_scope(marker)
    required_strings = (
        "transaction_id", "env", "phase", "manager", "git_dir", "venv_path",
        "venv_backup", "service_executable",
    )
    for name in required_strings:
        value = raw.get(name)
        if not isinstance(value, str) or not value:
            raise AdminError(f"managed receipt {name} is missing or invalid")
    if raw["phase"] not in MANAGED_UPDATE_RECEIPT_PHASES:
        raise AdminError(f"managed receipt phase {raw['phase']!r} is unknown")
    transaction_id = str(raw["transaction_id"])
    if re.fullmatch(r"[0-9a-f]{32}", transaction_id) is None:
        raise AdminError(
            "managed receipt transaction_id must be 32 lowercase hex characters"
        )
    if (
        not isinstance(marker.pause_token, str)
        or re.fullmatch(r"admin-update-[0-9a-f]{12}", marker.pause_token) is None
        or type(marker.surgical_pause) is not bool
        or not isinstance(marker.paused_jobids, list)
        or not all(
            isinstance(jobid, str) and jobid for jobid in marker.paused_jobids
        )
    ):
        raise AdminError(
            "managed update marker has no valid durable pause/resume scope"
        )
    owner_uid = raw.get("owner_uid")
    if not isinstance(owner_uid, int) or owner_uid < 0:
        raise AdminError("managed receipt owner_uid is invalid")
    if os.geteuid() not in {0, owner_uid}:
        raise AdminError("managed receipt belongs to another OS user")
    env = str(raw["env"])
    if marker.envs != [env] and env not in marker.envs:
        raise AdminError("managed receipt env is outside its marker scope")
    prog = _resolve_venv_program(env, cfg)
    expected_git = _canonical_lifecycle_checkout(Path(prog.git_dir))
    receipt_git = _canonical_future_lifecycle_path(Path(str(raw["git_dir"])))
    if receipt_git != expected_git:
        raise AdminError("managed receipt checkout does not match configured env")
    venv_path = _canonical_future_lifecycle_path(Path(prog.python).parent.parent)
    if _canonical_future_lifecycle_path(Path(str(raw["venv_path"]))) != venv_path:
        raise AdminError("managed receipt virtualenv does not match configured env")
    expected_backup = _managed_update_backup_path(
        prog, venv_path, transaction_id,
    )
    backup = _canonical_future_lifecycle_path(Path(str(raw["venv_backup"])))
    if backup != expected_backup:
        raise AdminError(
            "managed receipt rollback path is not the exact derived transaction path"
        )
    if backup.parent.stat().st_dev != venv_path.parent.stat().st_dev:
        raise AdminError(
            "managed receipt rollback path is not on the virtualenv filesystem"
        )
    identity_path = venv_path if venv_path.is_dir() else backup
    try:
        identity_info = identity_path.lstat()
    except OSError as exc:
        raise AdminError(
            f"managed receipt identity directory is unavailable: {exc}"
        ) from exc
    if (
        not stat.S_ISDIR(identity_info.st_mode)
        or stat.S_ISLNK(identity_info.st_mode)
        or identity_info.st_uid != owner_uid
    ):
        raise AdminError(
            "managed receipt owner_uid does not match the exact venv/backup"
        )
    command = raw.get("service_command")
    if (
        not isinstance(command, list)
        or not command
        or not all(isinstance(item, str) and item for item in command)
    ):
        raise AdminError("managed receipt service command is invalid")
    try:
        manager = _DaemonServiceManager(str(raw["manager"]))
    except ValueError as exc:
        raise AdminError("managed receipt service manager is unsupported") from exc
    receipt_executable = str(raw["service_executable"])
    expected_executable = (
        venv_path / "bin" / "python"
        if manager is _DaemonServiceManager.LAUNCHD
        else venv_path / "bin" / "vq"
    )
    executable_path = Path(receipt_executable)
    if not executable_path.is_absolute():
        raise AdminError("managed receipt service executable is not absolute")
    if manager is _DaemonServiceManager.LAUNCHD:
        try:
            executable_matches = (
                executable_path.name in {"python", "vq"}
                and _canonical_future_lifecycle_path(executable_path.parent)
                == _canonical_future_lifecycle_path(expected_executable.parent)
            )
        except (OSError, AdminError):
            executable_matches = False
    else:
        # systemd may intentionally name a stable wrapper symlink while the
        # venv's console script may itself be a symlink. Authenticate the
        # resolved executable identity, then compare the definition fields so
        # a same-target command rewrite is detected without binding run state.
        executable_matches = _execstart_matches_vq(
            receipt_executable,
            expected_executable.parent,
        )
    if not executable_matches:
        raise AdminError(
            "managed receipt service executable is outside the configured venv"
        )
    command_tuple = tuple(command)
    if manager is _DaemonServiceManager.LAUNCHD:
        expected_prefix = (
            (receipt_executable, "daemon", "run")
            if executable_path.name == "vq"
            else (receipt_executable, "-m", "vq", "daemon", "run")
        )
        if command_tuple[:len(expected_prefix)] != expected_prefix:
            raise AdminError("managed receipt launchd command is not canonical")
    else:
        definition = _normalize_systemd_command_identity(command_tuple)
        command_executable = definition[0] if definition is not None else None
        if (
            command_executable != receipt_executable
            or not _execstart_matches_vq(
                command_executable,
                expected_executable.parent,
            )
            or not _systemd_command_identity_is_vq_daemon(command_tuple)
        ):
            raise AdminError("managed receipt systemd command is not canonical")
    live_service = _query_daemon_service_state(manager)
    if not _service_command_identities_match(
        manager,
        live_service.command_identity,
        command_tuple,
    ):
        raise AdminError(
            "managed receipt service command does not match the current "
            "authoritative manager definition"
        )
    backup_moved = raw.get("backup_moved")
    if type(backup_moved) is not bool:
        raise AdminError("managed receipt backup_moved must be boolean")
    if "pre_checkout_branch" not in raw or (
        raw["pre_checkout_branch"] is not None
        and not isinstance(raw["pre_checkout_branch"], str)
    ):
        raise AdminError(
            "managed receipt pre_checkout_branch must be string or null"
        )
    pre_checkout_branch = raw["pre_checkout_branch"]
    if isinstance(pre_checkout_branch, str) and (
        not pre_checkout_branch
        or pre_checkout_branch.startswith("-")
        or any(character in pre_checkout_branch for character in ("\x00", "\r", "\n"))
        or ".." in pre_checkout_branch.split("/")
    ):
        raise AdminError("managed receipt pre_checkout_branch is invalid")
    lifecycle = _ManagedDaemonUpdate(
        manager=manager,
        env=env,
        pre_pid=raw.get("pre_pid") if isinstance(raw.get("pre_pid"), int) else None,
        was_running=raw.get("was_running") is True,
        was_stopped=True,
        pre_source_sha=_require_full_hex(
            raw.get("pre_source_sha"), length=40, field_name="pre_source_sha",
        ),
        pre_source_tree_sha256=_require_full_hex(
            raw.get("pre_source_tree_sha256"),
            length=64,
            field_name="pre_source_tree_sha256",
        ),
        pre_checkout_branch=pre_checkout_branch,
        venv_path=venv_path,
        venv_backup=backup,
        service_executable=receipt_executable,
        owner_uid=owner_uid,
        service_command=command_tuple,
        transaction_id=transaction_id,
        backup_moved=backup_moved,
        receipt_phase=str(raw["phase"]),
        target_source_sha=(
            _require_full_hex(
                raw.get("target_source_sha"), length=40,
                field_name="target_source_sha",
            )
            if raw.get("target_source_sha") is not None else None
        ),
        target_source_tree_sha256=(
            _require_full_hex(
                raw.get("target_source_tree_sha256"), length=64,
                field_name="target_source_tree_sha256",
            )
            if raw.get("target_source_tree_sha256") is not None else None
        ),
    )
    venv_exists = venv_path.is_dir()
    backup_exists = backup.is_dir()
    if not venv_exists and backup_exists:
        # Crash after the atomic rename but before the phase rewrite.
        lifecycle.backup_moved = True
    elif venv_exists and not backup_exists:
        if lifecycle.receipt_phase in {
            "target_verified", "target_committed", "target_cleanup_pending",
            "old_restored", "files_restored",
        }:
            # Crash after the verified rollback candidate was atomically
            # disarmed. Recovery may retain the target only after re-proving it.
            lifecycle.backup_moved = False
        elif lifecycle.backup_moved:
            if lifecycle.receipt_phase == "restoring_old":
                # The fsynced intent precedes backup->venv. Exact path presence
                # therefore proves the old venv rename completed; checkout and
                # daemon identity still need full recovery below.
                lifecycle.backup_moved = False
            else:
                raise AdminError(
                    "managed receipt says rollback venv moved but its exact "
                    "backup is absent before target verification"
                )
    elif not venv_exists and not backup_exists:
        raise AdminError("both managed virtualenv and exact rollback backup are absent")
    elif not lifecycle.backup_moved and backup_exists:
        # A stale armed receipt plus two live trees is ambiguous: never pick one.
        raise AdminError(
            "managed receipt is armed but both target and rollback virtualenvs exist"
        )
    return prog, lifecycle


@dataclass(frozen=True)
class ManagedUpdateRecoveryResult:
    env: str
    recovered: bool
    detail: str
    resumed_summary: str


@dataclass(frozen=True)
class OrphanReceiptQuarantineResult:
    """Outcome of planning or committing one orphan-receipt quarantine."""

    marker_id: str
    marker_sha256: str
    env: str
    dry_run: bool
    quarantined: bool
    plan_sha256: str
    quarantine_path: str
    detail: str
    pause_summary: str


@dataclass(frozen=True)
class _OrphanReceiptDescriptor:
    env: str
    transaction_id: str
    owner_uid: int
    manager: _DaemonServiceManager
    git_dir: Path
    venv_path: Path
    venv_backup: Path
    service_executable: Path
    service_command: tuple[str, ...]
    target_source_sha: str
    target_source_tree_sha256: str

    @property
    def assets(self) -> tuple[Path, ...]:
        return (
            self.git_dir,
            self.venv_path,
            self.venv_backup,
            self.service_executable,
        )


@dataclass(frozen=True)
class _SecureBytes:
    path: Path
    payload: bytes
    stat_key: tuple[int, int]


_MANAGED_RECEIPT_FIELDS = frozenset(
    {
        "schema", "transaction_id", "owner_uid", "env", "phase",
        "manager", "pre_pid", "was_running", "pre_source_sha",
        "pre_source_tree_sha256", "pre_checkout_branch", "git_dir",
        "venv_path", "venv_backup", "backup_moved",
        "service_executable", "service_command", "target_source_sha",
        "target_source_tree_sha256", "updated_at",
    }
)
_ADMIN_MARKER_FIELDS = frozenset(field.name for field in fields(AdminUpdateMarker))
_QUARANTINE_FILE_NAMES = frozenset(
    {"marker.json", "admin-status.json", "quarantine-receipt.json"}
)
_QUARANTINE_PREPARED_NAMES = frozenset(
    {
        "admin-status.json",
        "quarantine-receipt.json",
        ".admin-status.json.tmp",
        ".quarantine-receipt.json.tmp",
    }
)
_QUARANTINE_PREPARED_PAIRS = (
    ("admin-status.json", ".admin-status.json.tmp"),
    ("quarantine-receipt.json", ".quarantine-receipt.json.tmp"),
)
_QUARANTINE_PREPARED_FINAL_NAMES = frozenset(
    final_name for final_name, _staging_name in _QUARANTINE_PREPARED_PAIRS
)


def _read_secure_owner_file(
    path: Path,
    *,
    label: str,
    max_bytes: int = 8 * 1024 * 1024,
) -> _SecureBytes:
    """Read an exact owner-only regular file without following links."""
    flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise AdminError(f"cannot read secure {label} {path}: {exc}") from exc
    try:
        info = os.fstat(fd)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise AdminError(
                f"unsafe {label} {path}: expected owner-only mode-0600 "
                "regular file with one link"
            )
        named = os.stat(path, follow_symlinks=False)
        if (
            (named.st_dev, named.st_ino) != (info.st_dev, info.st_ino)
            or not stat.S_ISREG(named.st_mode)
            or named.st_uid != info.st_uid
            or named.st_nlink != 1
        ):
            raise AdminError(f"unsafe {label} {path}: pathname changed")
        if info.st_size > max_bytes:
            raise AdminError(f"{label} {path} exceeds {max_bytes} bytes")
        chunks: list[bytes] = []
        remaining = max_bytes + 1
        while remaining > 0:
            chunk = os.read(fd, min(remaining, 1024 * 1024))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        if len(payload) > max_bytes:
            raise AdminError(f"{label} {path} exceeds {max_bytes} bytes")
        after = os.fstat(fd)
        renamed = os.stat(path, follow_symlinks=False)
        if (
            (after.st_dev, after.st_ino, after.st_size)
            != (info.st_dev, info.st_ino, info.st_size)
            or (renamed.st_dev, renamed.st_ino) != (info.st_dev, info.st_ino)
        ):
            raise AdminUpdateInProgress(f"{label} {path} changed while read")
        return _SecureBytes(path, payload, (info.st_dev, info.st_ino))
    finally:
        os.close(fd)


def _secure_marker_entries(
    binding: _AdminStateBinding,
) -> list[tuple[_SecureBytes, dict[str, object], AdminUpdateMarker]]:
    """Read every registry candidate securely; one bad entry blocks all."""
    paths_seen: list[Path] = []
    if os.path.lexists(binding.marker_path):
        paths_seen.append(binding.marker_path)
    if os.path.lexists(binding.marker_dir):
        try:
            directory = os.lstat(binding.marker_dir)
        except OSError as exc:
            raise AdminError(f"cannot inspect marker registry: {exc}") from exc
        if (
            not stat.S_ISDIR(directory.st_mode)
            or stat.S_ISLNK(directory.st_mode)
            or directory.st_uid != os.geteuid()
            or stat.S_IMODE(directory.st_mode) & 0o022
        ):
            raise AdminError(
                f"unsafe admin-update marker registry {binding.marker_dir}"
            )
        paths_seen.extend(sorted(binding.marker_dir.glob("*.json")))
    entries: list[tuple[_SecureBytes, dict[str, object], AdminUpdateMarker]] = []
    for path in paths_seen:
        secure = _read_secure_owner_file(path, label="admin-update marker")
        try:
            raw = json.loads(secure.payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AdminError(f"admin-update marker {path} is invalid JSON") from exc
        if not isinstance(raw, dict) or set(raw) != _ADMIN_MARKER_FIELDS:
            raise AdminError(
                f"admin-update marker {path} does not have the exact v1 fields"
            )
        try:
            marker = AdminUpdateMarker(**raw)
        except TypeError as exc:
            raise AdminError(f"admin-update marker {path} is malformed") from exc
        entries.append((secure, raw, marker))
    return entries


def _parse_terminal_orphan_receipt(
    raw_marker: dict[str, object],
    marker: AdminUpdateMarker,
) -> _OrphanReceiptDescriptor:
    """Parse only the approved terminal-v1, already-committed orphan shape."""
    raw = raw_marker.get("managed_transaction")
    if (
        not isinstance(raw, dict)
        or set(raw) != _MANAGED_RECEIPT_FIELDS
        or raw.get("schema") != MANAGED_UPDATE_RECEIPT_SCHEMA
    ):
        raise AdminError("marker has no exact terminal-v1 managed receipt")
    if raw.get("phase") != "target_committed":
        raise AdminError("orphan quarantine accepts only phase=target_committed")
    if raw.get("backup_moved") is not False:
        raise AdminError("orphan quarantine requires backup_moved=false")
    if raw.get("was_running") is not True:
        raise AdminError("orphan quarantine requires was_running=true")
    if marker.surgical_pause is not False or marker.paused_jobids != []:
        raise AdminError(
            "orphan quarantine accepts only a nonsurgical empty job-id scope"
        )
    if (
        not isinstance(marker.pause_token, str)
        or re.fullmatch(r"admin-update-[0-9a-f]{12}", marker.pause_token) is None
    ):
        raise AdminError("orphan marker has no exact durable pause token")
    _require_admin_update_pause_queue_scope(marker)

    transaction_id = raw.get("transaction_id")
    if (
        not isinstance(transaction_id, str)
        or re.fullmatch(r"[0-9a-f]{32}", transaction_id) is None
    ):
        raise AdminError("orphan receipt transaction_id is invalid")
    owner_uid = raw.get("owner_uid")
    if type(owner_uid) is not int or owner_uid < 0:
        raise AdminError("orphan receipt owner_uid is invalid")
    if os.geteuid() not in {0, owner_uid}:
        raise AdminError("orphan receipt belongs to another OS user")
    env = raw.get("env")
    if not isinstance(env, str) or not env or marker.envs != [env]:
        raise AdminError("orphan receipt env is not the marker's exact scope")
    try:
        manager = _DaemonServiceManager(raw.get("manager"))
    except (TypeError, ValueError) as exc:
        raise AdminError("orphan receipt service manager is unsupported") from exc
    if raw.get("pre_pid") is not None and (
        type(raw.get("pre_pid")) is not int or int(raw["pre_pid"]) <= 0
    ):
        raise AdminError("orphan receipt pre_pid is invalid")
    branch = raw.get("pre_checkout_branch")
    if branch is not None and (
        not isinstance(branch, str)
        or not branch
        or branch.startswith("-")
        or any(character in branch for character in ("\x00", "\r", "\n"))
        or ".." in branch.split("/")
    ):
        raise AdminError("orphan receipt pre_checkout_branch is invalid")
    if not isinstance(raw.get("updated_at"), str) or not raw["updated_at"]:
        raise AdminError("orphan receipt updated_at is invalid")
    for name, length in (
        ("pre_source_sha", 40),
        ("pre_source_tree_sha256", 64),
        ("target_source_sha", 40),
        ("target_source_tree_sha256", 64),
    ):
        _require_full_hex(raw.get(name), length=length, field_name=name)

    def exact_future_path(name: str) -> Path:
        value = raw.get(name)
        if not isinstance(value, str) or not value or not Path(value).is_absolute():
            raise AdminError(f"orphan receipt {name} is not an absolute path")
        return _canonical_future_lifecycle_path(Path(value))

    git_dir = exact_future_path("git_dir")
    venv_path = exact_future_path("venv_path")
    venv_backup = exact_future_path("venv_backup")
    service_executable = exact_future_path("service_executable")
    expected_backup = (
        git_dir.parent
        if venv_path == git_dir or git_dir in venv_path.parents
        else venv_path.parent
    ) / f".{venv_path.name}.vq-admin-backup-{transaction_id}"
    if venv_backup != _canonical_future_lifecycle_path(expected_backup):
        raise AdminError("orphan receipt backup path is not transaction-derived")
    command = raw.get("service_command")
    if (
        not isinstance(command, list)
        or not command
        or not all(isinstance(value, str) and value for value in command)
    ):
        raise AdminError("orphan receipt service_command is invalid")
    command_tuple = tuple(command)
    if manager is _DaemonServiceManager.SYSTEMD:
        definition = _normalize_systemd_command_identity(command_tuple)
        if (
            definition is None
            or Path(definition[0]) != service_executable
            or not _systemd_command_identity_is_vq_daemon(command_tuple)
        ):
            raise AdminError("orphan receipt systemd command is not canonical")
    elif command_tuple[:5] != (
        str(service_executable), "-m", "vq", "daemon", "run",
    ):
        raise AdminError("orphan receipt launchd command is not canonical")
    return _OrphanReceiptDescriptor(
        env=env,
        transaction_id=transaction_id,
        owner_uid=owner_uid,
        manager=manager,
        git_dir=git_dir,
        venv_path=venv_path,
        venv_backup=venv_backup,
        service_executable=service_executable,
        service_command=command_tuple,
        target_source_sha=str(raw["target_source_sha"]).lower(),
        target_source_tree_sha256=str(raw["target_source_tree_sha256"]).lower(),
    )


def _paths_overlap(left: Path, right: Path) -> bool:
    return left == right or left in right.parents or right in left.parents


def _validate_orphan_assets_absent(
    descriptor: _OrphanReceiptDescriptor,
    prog: config.VenvProgram,
) -> tuple[str, ...]:
    """Prove every receipt asset absent and disjoint from the live runtime."""
    current_git = _canonical_lifecycle_checkout(Path(prog.git_dir))
    current_venv = _canonical_lifecycle_target(Path(prog.python).parent.parent)
    current_assets = (current_git, current_venv, Path(prog.python).resolve())
    labels: list[str] = []
    for foreign in descriptor.assets:
        if os.path.lexists(foreign):
            raise AdminError(f"orphan receipt asset still exists: {foreign}")
        if any(_paths_overlap(foreign, current) for current in current_assets):
            raise AdminError(
                f"orphan receipt asset {foreign} overlaps current runtime"
            )
        labels.append(str(foreign))
    return tuple(labels)


def _read_secure_admin_status(
    binding: _AdminStateBinding,
    *,
    required_env: str,
) -> _SecureBytes:
    secure = _read_secure_owner_file(
        binding.status_path, label="admin status",
    )
    try:
        raw = json.loads(secure.payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdminError("admin status is not valid JSON") from exc
    if not isinstance(raw, dict) or required_env not in raw:
        raise AdminError(
            f"admin status has no record for current environment {required_env!r}"
        )
    known = {field.name for field in fields(AdminUpdateRecord)}
    for env, record in raw.items():
        if not isinstance(env, str) or not isinstance(record, dict):
            raise AdminError("admin status contains a malformed environment row")
        try:
            AdminUpdateRecord(**{
                key: value for key, value in record.items() if key in known
            })
        except TypeError as exc:
            raise AdminError(
                f"admin status row {env!r} is not valid"
            ) from exc
    return secure


def _current_runtime_identity(
    prog: config.VenvProgram,
    *,
    expected_source_sha: str,
) -> dict[str, object]:
    """Prove checkout, install, manager, RPC and responder are one runtime."""
    expected_source_sha = _require_full_hex(
        expected_source_sha,
        length=40,
        field_name="expected_current_source_sha",
    )
    git_dir = _canonical_lifecycle_checkout(Path(prog.git_dir))
    _guard_git_index_unlocked(git_dir)
    actual_sha = _git_head_sha(git_dir)
    if actual_sha is None or actual_sha.lower() != expected_source_sha:
        raise AdminError(
            f"current checkout SHA {actual_sha or 'unavailable'} does not match "
            f"accepted {expected_source_sha}"
        )
    status_rc, dirty = _run_git_status_porcelain(git_dir)
    if status_rc != 0 or dirty:
        raise AdminError("current configured checkout is dirty or unreadable")
    expected_tree = source_tree_sha256_at_git_commit(
        git_dir, expected_source_sha,
    )
    installed_tree = _installed_tree_digest(prog.python)
    if installed_tree != expected_tree:
        raise AdminError(
            "current installed vq tree does not match the accepted checkout"
        )
    probe = _detect_vq_self_update(prog)
    if (
        not probe.is_self_update
        or probe.daemon_running is not True
        or not probe.manager_available
        or probe.service_manager is None
    ):
        raise AdminError(
            "current configured runtime is not the healthy serving daemon: "
            + probe.diagnostic
        )
    manager = _DaemonServiceManager(probe.service_manager)
    service = _query_daemon_service_state(manager)
    if (
        service.running is not True
        or service.pid is None
        or not service.command_identity
        or not _service_executable_matches(
            service.executable, prog, manager=manager,
        )
    ):
        raise AdminError(
            "current service-manager identity is incomplete: "
            + service.diagnostic
        )
    if manager is _DaemonServiceManager.SYSTEMD:
        if not _systemd_command_identity_is_vq_daemon(service.command_identity):
            raise AdminError("current systemd daemon command is not canonical")
        definition = _normalize_systemd_command_identity(service.command_identity)
        assert definition is not None
        expected_argv = shlex.split(definition[1])
    else:
        expected_argv = list(service.command_identity)

    try:
        version_probe = subprocess.run(
            [
                prog.python,
                "-I",
                "-c",
                "import vq; print(vq.__version__)",
            ],
            capture_output=True,
            text=True,
            timeout=30,
            cwd="/",
            stdin=subprocess.DEVNULL,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise AdminError(f"current runtime version probe failed: {exc}") from exc
    version = version_probe.stdout.strip()
    if version_probe.returncode != 0 or not version:
        raise AdminError("current runtime version probe did not succeed")

    from vq import rpc

    socket = rpc.user_socket_path()
    try:
        ping = rpc.call("ping", socket_override=socket, timeout=10)
        methods = rpc.call("get_methods", socket_override=socket, timeout=10)
        identity = rpc.call(
            "get_process_identity", socket_override=socket, timeout=10,
        )
    except Exception as exc:  # noqa: BLE001 - converted to fail-closed proof
        raise AdminError(f"current daemon RPC identity is unavailable: {exc}") from exc
    advertised_methods = methods.get("methods") if isinstance(methods, dict) else None
    if (
        not isinstance(ping, dict)
        or not isinstance(methods, dict)
        or not isinstance(advertised_methods, list)
        or "get_process_identity" not in advertised_methods
        or not isinstance(identity, dict)
    ):
        raise AdminError("current daemon RPC identity response is malformed")
    exact = {
        "pid": service.pid,
        "euid": os.geteuid(),
        "version": version,
        "source_sha": expected_source_sha,
        "source_tree_sha256": expected_tree,
        "multi_user": False,
        "socket_path": str(socket),
    }
    for key, value in exact.items():
        if identity.get(key) != value:
            raise AdminError(
                f"current daemon process identity {key} does not match"
            )
    for key in ("version", "source_sha", "source_tree_sha256", "multi_user"):
        if ping.get(key) != exact[key]:
            raise AdminError(f"current daemon ping {key} does not match")
    try:
        process_python = Path(str(identity.get("python_executable"))).resolve()
        configured_python = Path(prog.python).resolve(strict=True)
    except OSError as exc:
        raise AdminError(f"current daemon Python identity is unavailable: {exc}") from exc
    if process_python != configured_python:
        raise AdminError("current daemon Python does not match configured runtime")
    if identity.get("argv") != expected_argv:
        raise AdminError("current daemon argv does not match service definition")
    return {
        "git_dir": str(git_dir),
        "venv_path": str(_canonical_lifecycle_target(
            Path(prog.python).parent.parent
        )),
        "source_sha": expected_source_sha,
        "source_tree_sha256": expected_tree,
        "installed_tree_sha256": installed_tree,
        "manager": manager.value,
        "service_executable": service.executable,
        "service_command": list(service.command_identity),
        "pid": service.pid,
        "euid": os.geteuid(),
        "version": version,
        "python_executable": str(process_python),
        "argv": list(expected_argv),
        "multi_user": False,
        "socket_path": str(socket),
    }


def _canonical_json_bytes(value: object) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _ensure_owner_directory(path: Path, *, create: bool) -> None:
    if create:
        old_umask = os.umask(0o077)
        try:
            path.mkdir(mode=0o700, parents=False, exist_ok=True)
        finally:
            os.umask(old_umask)
    try:
        info = os.lstat(path)
    except OSError as exc:
        raise AdminError(f"quarantine directory {path} is unavailable: {exc}") from exc
    if (
        not stat.S_ISDIR(info.st_mode)
        or stat.S_ISLNK(info.st_mode)
        or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) != 0o700
    ):
        raise AdminError(f"unsafe quarantine directory {path}")


def _write_or_validate_quarantine_file(path: Path, payload: bytes) -> _SecureBytes:
    """Atomically create one exact 0600 evidence file or validate a retry.

    The deterministic ``.tmp`` entry is staging, never accepted evidence.  A
    process may die after any short write without poisoning the final name:
    retry securely truncates and rewrites the staging inode, fsyncs it, then
    commits it by rename.  Only the final pathname is hash-bound evidence.
    """
    if os.path.lexists(path):
        existing = _read_secure_owner_file(path, label="quarantine evidence")
        if existing.payload != payload:
            raise AdminError(f"quarantine evidence conflicts at {path}")
        return existing
    staging = path.with_name(f".{path.name}.tmp")
    flags = os.O_WRONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    created = not os.path.lexists(staging)
    if created:
        flags |= os.O_CREAT | os.O_EXCL
    try:
        fd = os.open(staging, flags, 0o600)
    except OSError as exc:
        raise AdminError(
            f"cannot prepare quarantine evidence {path}: {exc}"
        ) from exc
    try:
        info = os.fstat(fd)
        named = os.stat(staging, follow_symlinks=False)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) != 0o600
            or (named.st_dev, named.st_ino) != (info.st_dev, info.st_ino)
        ):
            raise AdminError(f"unsafe quarantine staging file {staging}")
        os.ftruncate(fd, 0)
        remaining = memoryview(payload)
        while remaining:
            written = os.write(fd, remaining)
            if written <= 0:
                raise OSError("short quarantine evidence write")
            remaining = remaining[written:]
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(staging, path)
    _fsync_directory_path(path.parent)
    committed = _read_secure_owner_file(path, label="quarantine evidence")
    if committed.payload != payload:
        raise AdminError(f"quarantine evidence changed while committing {path}")
    return committed


def _require_quarantine_evidence_unchanged(
    expected: _SecureBytes,
    *,
    label: str,
) -> None:
    current = _read_secure_owner_file(expected.path, label=label)
    if current.stat_key != expected.stat_key or current.payload != expected.payload:
        raise AdminUpdateInProgress(f"{label} changed before marker commit")


def _validate_prepared_quarantine_entries(
    directory: Path,
    *,
    require_complete: bool = False,
) -> set[str]:
    """Reject ambiguous evidence and authenticate every staging pathname."""
    _ensure_owner_directory(directory, create=False)
    entries = {entry.name for entry in directory.iterdir()}
    if not entries <= _QUARANTINE_PREPARED_NAMES:
        raise AdminError(
            "prepared quarantine is ambiguous or has unexpected evidence"
        )
    for final_name, staging_name in _QUARANTINE_PREPARED_PAIRS:
        if final_name in entries and staging_name in entries:
            raise AdminError(
                "prepared quarantine has duplicate final and staging evidence "
                f"for {final_name}"
            )
        if staging_name in entries:
            _read_secure_owner_file(
                directory / staging_name,
                label="prepared quarantine staging evidence",
            )
    if require_complete and entries != _QUARANTINE_PREPARED_FINAL_NAMES:
        raise AdminError("prepared quarantine evidence is incomplete")
    return entries


def _validate_quarantine_receipt(
    directory: Path,
    *,
    marker_id: str,
    marker_sha256: str,
    expected_current_source_sha: str,
    reason: str,
) -> tuple[dict[str, object], _SecureBytes, _SecureBytes, _SecureBytes]:
    _ensure_owner_directory(directory, create=False)
    entries = {entry.name for entry in directory.iterdir()}
    if entries != _QUARANTINE_FILE_NAMES:
        raise AdminError(
            f"quarantine {directory} is incomplete or has unexpected entries"
        )
    marker = _read_secure_owner_file(
        directory / "marker.json", label="quarantined marker",
    )
    status = _read_secure_owner_file(
        directory / "admin-status.json", label="quarantined admin status",
    )
    receipt = _read_secure_owner_file(
        directory / "quarantine-receipt.json", label="quarantine receipt",
    )
    try:
        raw = json.loads(receipt.payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdminError("quarantine receipt is invalid JSON") from exc
    if not isinstance(raw, dict) or set(raw) != {
        "schema", "created_at", "plan", "plan_sha256", "retention",
    }:
        raise AdminError("quarantine receipt has invalid fields")
    plan = raw.get("plan")
    if (
        raw.get("schema") != ORPHAN_QUARANTINE_SCHEMA
        or raw.get("retention") != "manual-cleanup-only"
        or not isinstance(raw.get("created_at"), str)
        or not raw["created_at"]
        or not isinstance(plan, dict)
    ):
        raise AdminError("quarantine receipt has invalid identity")
    plan_bytes = _canonical_json_bytes(plan)
    plan_digest = hashlib.sha256(plan_bytes).hexdigest()
    if raw.get("plan_sha256") != plan_digest:
        raise AdminError("quarantine receipt plan digest is invalid")
    expected = {
        "marker_id": marker_id,
        "marker_sha256": marker_sha256,
        "accepted_current_source_sha": expected_current_source_sha,
        "reason": reason,
        "status_sha256": hashlib.sha256(status.payload).hexdigest(),
    }
    for key, value in expected.items():
        if plan.get(key) != value:
            raise AdminError(f"quarantine receipt invocation {key} differs")
    if hashlib.sha256(marker.payload).hexdigest() != marker_sha256:
        raise AdminError("quarantined marker bytes do not match expected hash")
    marker_inode = plan.get("marker_inode")
    if marker_inode != [marker.stat_key[0], marker.stat_key[1]]:
        raise AdminError("quarantined marker inode does not match its receipt")
    return raw, marker, status, receipt


def quarantine_orphaned_managed_receipt(
    cfg: config.Config,
    *,
    marker_id: str,
    expected_marker_sha256: str,
    expected_current_source_sha: str,
    reason: str,
    dry_run: bool = False,
) -> OrphanReceiptQuarantineResult:
    """Quarantine one exact terminal receipt after positive orphan proofs.

    This is deliberately not a recovery fallback.  It never resumes jobs,
    changes a service, adopts a receipt, or removes evidence.  The sole
    terminal mutation is a same-filesystem rename of the original marker into
    a retained, owner-only evidence directory after every proof succeeds.
    """
    if re.fullmatch(r"[0-9a-f]{32}", marker_id or "") is None:
        raise AdminError("--marker-id must be exactly 32 lowercase hex characters")
    expected_marker_sha256 = _require_full_hex(
        expected_marker_sha256,
        length=64,
        field_name="expected_marker_sha256",
    )
    expected_current_source_sha = _require_full_hex(
        expected_current_source_sha,
        length=40,
        field_name="expected_current_source_sha",
    )
    if (
        not isinstance(reason, str)
        or not reason.strip()
        or len(reason) > 500
        or any(character in reason for character in ("\x00", "\r", "\n"))
    ):
        raise AdminError("--reason must be one non-empty line of at most 500 chars")
    reason = reason.strip()
    binding = _capture_admin_state_binding()
    key = f"{marker_id}-{expected_marker_sha256}"
    pending = binding.quarantine_root / f".{key}.pending"
    final = binding.quarantine_root / key

    # Initial read discovers the foreign lifecycle resources.  It authorizes
    # nothing: the exact bytes/inode are re-read after ownership+lifecycle+
    # marker admission, in that order.
    initial_entries = _secure_marker_entries(binding)
    initial_matches = [
        entry for entry in initial_entries if entry[2].marker_id == marker_id
    ]
    descriptor: _OrphanReceiptDescriptor | None = None
    initial_secure: _SecureBytes | None = None
    if len(initial_matches) == 1:
        initial_secure, initial_raw, initial_marker = initial_matches[0]
        if hashlib.sha256(initial_secure.payload).hexdigest() != expected_marker_sha256:
            raise AdminError("admin-update marker SHA-256 does not match")
        descriptor = _parse_terminal_orphan_receipt(initial_raw, initial_marker)
    elif len(initial_matches) > 1:
        raise AdminError("multiple admin-update markers use the selected marker ID")

    # A post-move retry learns its environment from the retained receipt.
    if descriptor is None:
        retained = final if os.path.lexists(final) else pending
        if not os.path.lexists(retained):
            raise AdminError("no live or quarantined marker matches the selected ID")
        raw_receipt, _marker, _status, _receipt = _validate_quarantine_receipt(
            retained,
            marker_id=marker_id,
            marker_sha256=expected_marker_sha256,
            expected_current_source_sha=expected_current_source_sha,
            reason=reason,
        )
        plan = raw_receipt["plan"]
        assert isinstance(plan, dict)
        env = plan.get("env")
        if not isinstance(env, str) or not env:
            raise AdminError("quarantine receipt environment is invalid")
        prog = _resolve_venv_program(env, cfg)
        extra_resources: tuple[tuple[str, str], ...] = ()
    else:
        prog = _resolve_venv_program(descriptor.env, cfg)
        extra_resources = (
            ("checkout", str(descriptor.git_dir)),
            ("target", str(descriptor.venv_path)),
            ("target", str(descriptor.venv_backup)),
        )

    with admin_update_ownership(), toolset_lifecycle_lock(
        [prog],
        action="quarantine orphaned managed receipt",
        extra_resources=extra_resources,
    ), _admin_update_marker_lock(binding):
        _validate_admin_state_binding(binding)
        current_entries = _secure_marker_entries(binding)
        current_matches = [
            entry for entry in current_entries if entry[2].marker_id == marker_id
        ]
        source_present = len(current_matches) == 1
        if len(current_matches) > 1:
            raise AdminError("multiple markers use the selected marker ID")
        pending_present = os.path.lexists(pending)
        final_present = os.path.lexists(final)
        if final_present:
            if source_present or pending_present:
                raise AdminError("ambiguous duplicate live/pending/final marker evidence")
            raw_receipt, _marker, _status, _receipt = _validate_quarantine_receipt(
                final,
                marker_id=marker_id,
                marker_sha256=expected_marker_sha256,
                expected_current_source_sha=expected_current_source_sha,
                reason=reason,
            )
            plan = raw_receipt["plan"]
            assert isinstance(plan, dict)
            return OrphanReceiptQuarantineResult(
                marker_id, expected_marker_sha256, str(plan["env"]), dry_run,
                True, str(raw_receipt["plan_sha256"]), str(final),
                "quarantine already complete; retained evidence validated",
                str(plan["pause_summary"]),
            )
        if pending_present and not source_present:
            raw_receipt, _marker, _status, _receipt = _validate_quarantine_receipt(
                pending,
                marker_id=marker_id,
                marker_sha256=expected_marker_sha256,
                expected_current_source_sha=expected_current_source_sha,
                reason=reason,
            )
            plan = raw_receipt["plan"]
            assert isinstance(plan, dict)
            if dry_run:
                return OrphanReceiptQuarantineResult(
                    marker_id, expected_marker_sha256, str(plan["env"]), True,
                    False, str(raw_receipt["plan_sha256"]), str(final),
                    "committed pending evidence is valid; apply would finalize it",
                    str(plan["pause_summary"]),
                )
            os.replace(pending, final)
            _fsync_directory_path(binding.quarantine_root)
            return OrphanReceiptQuarantineResult(
                marker_id, expected_marker_sha256, str(plan["env"]), False,
                True, str(raw_receipt["plan_sha256"]), str(final),
                "committed pending evidence finalized",
                str(plan["pause_summary"]),
            )
        if not source_present:
            raise AdminError("source marker is absent and no committed evidence exists")
        if pending_present:
            _validate_prepared_quarantine_entries(pending)
        if initial_secure is None or descriptor is None:
            raise AdminUpdateInProgress("marker appeared after orphan admission began")
        current_secure, current_raw, current_marker = current_matches[0]
        if (
            current_secure.stat_key != initial_secure.stat_key
            or current_secure.payload != initial_secure.payload
        ):
            raise AdminUpdateInProgress("admin-update marker changed before admission")
        if hashlib.sha256(current_secure.payload).hexdigest() != expected_marker_sha256:
            raise AdminError("admin-update marker SHA-256 changed")
        current_descriptor = _parse_terminal_orphan_receipt(
            current_raw, current_marker,
        )
        if current_descriptor != descriptor:
            raise AdminUpdateInProgress("managed receipt changed before admission")
        diagnosis = diagnose_admin_update_marker(current_marker)
        if diagnosis.marker_status != ADMIN_UPDATE_MARKER_DIAG_STALE:
            raise AdminUpdateInProgress(
                "orphan quarantine requires positive stale-writer evidence; "
                + diagnosis.summary
            )
        absent_assets = _validate_orphan_assets_absent(descriptor, prog)
        status = _read_secure_admin_status(binding, required_env=descriptor.env)
        pause_multi_user, pause_queue_root = (
            _require_admin_update_pause_queue_scope(current_marker)
        )
        assert current_marker.pause_token is not None
        pause = prove_pause_token_absent(
            current_marker.host,
            current_marker.pause_token,
            multi_user=pause_multi_user,
            queue_root=pause_queue_root,
        )
        pause.require_clear()
        runtime = _current_runtime_identity(
            prog, expected_source_sha=expected_current_source_sha,
        )
        _validate_admin_state_binding(binding)
        plan: dict[str, object] = {
            "marker_id": marker_id,
            "marker_sha256": expected_marker_sha256,
            "marker_inode": list(current_secure.stat_key),
            "marker_source_path": str(current_secure.path),
            "status_source_path": str(status.path),
            "status_sha256": hashlib.sha256(status.payload).hexdigest(),
            "accepted_current_source_sha": expected_current_source_sha,
            "reason": reason,
            "env": descriptor.env,
            "transaction_id": descriptor.transaction_id,
            "foreign_assets": list(absent_assets),
            "runtime": runtime,
            "pause": {
                "token": current_marker.pause_token,
                "multi_user": pause_multi_user,
                "queue_root": str(pause_queue_root),
                "proven_clear": pause.proven_clear,
            },
            "pause_summary": pause.summary,
            "quarantine_path": str(final),
        }
        plan_sha256 = hashlib.sha256(_canonical_json_bytes(plan)).hexdigest()
        if dry_run:
            if pending_present:
                allowed = _validate_prepared_quarantine_entries(pending)
                if "admin-status.json" in allowed:
                    saved = _read_secure_owner_file(
                        pending / "admin-status.json",
                        label="prepared admin status",
                    )
                    if saved.payload != status.payload:
                        raise AdminError("prepared admin status conflicts")
                if "quarantine-receipt.json" in allowed:
                    receipt = _read_secure_owner_file(
                        pending / "quarantine-receipt.json",
                        label="prepared quarantine receipt",
                    )
                    try:
                        saved_receipt = json.loads(receipt.payload)
                    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                        raise AdminError("prepared quarantine receipt is invalid") from exc
                    if saved_receipt.get("plan") != plan:
                        raise AdminError("prepared quarantine plan conflicts")
            return OrphanReceiptQuarantineResult(
                marker_id, expected_marker_sha256, descriptor.env, True,
                False, plan_sha256, str(final),
                "all orphan proofs are green; apply would quarantine marker",
                pause.summary,
            )

        _ensure_owner_directory(binding.quarantine_root, create=True)
        _ensure_owner_directory(pending, create=True)
        receipt_path = pending / "quarantine-receipt.json"
        created_at = utcnow_iso()
        if os.path.lexists(receipt_path):
            existing_receipt = _read_secure_owner_file(
                receipt_path, label="prepared quarantine receipt",
            )
            try:
                existing_raw = json.loads(existing_receipt.payload)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise AdminError("prepared quarantine receipt is invalid") from exc
            if not isinstance(existing_raw, dict) or existing_raw.get("plan") != plan:
                raise AdminError("prepared quarantine receipt conflicts")
            created_at = str(existing_raw.get("created_at") or "")
            if not created_at:
                raise AdminError("prepared quarantine receipt has no creation time")
        receipt_payload = _canonical_json_bytes({
            "schema": ORPHAN_QUARANTINE_SCHEMA,
            "created_at": created_at,
            "plan": plan,
            "plan_sha256": plan_sha256,
            "retention": "manual-cleanup-only",
        })
        prepared_status = _write_or_validate_quarantine_file(
            pending / "admin-status.json", status.payload,
        )
        prepared_receipt = _write_or_validate_quarantine_file(
            receipt_path, receipt_payload,
        )
        _validate_prepared_quarantine_entries(pending, require_complete=True)
        _fsync_directory_path(pending)
        _fsync_directory_path(binding.quarantine_root)
        # Revalidate pathname, inode and bytes immediately before the sole
        # point-of-no-return. Source-controlled writers all hold this lock.
        last = _read_secure_owner_file(
            current_secure.path, label="admin-update marker",
        )
        if last.stat_key != current_secure.stat_key or last.payload != current_secure.payload:
            raise AdminUpdateInProgress("marker changed before quarantine commit")
        _require_quarantine_evidence_unchanged(
            prepared_status,
            label="prepared admin status",
        )
        _require_quarantine_evidence_unchanged(
            prepared_receipt,
            label="prepared quarantine receipt",
        )
        os.replace(current_secure.path, pending / "marker.json")
        _fsync_directory_path(current_secure.path.parent)
        _fsync_directory_path(pending)
        moved = _read_secure_owner_file(
            pending / "marker.json", label="quarantined marker",
        )
        if moved.stat_key != current_secure.stat_key or moved.payload != current_secure.payload:
            raise AdminError("terminal marker move did not preserve exact evidence")
        os.replace(pending, final)
        _fsync_directory_path(binding.quarantine_root)
        _validate_quarantine_receipt(
            final,
            marker_id=marker_id,
            marker_sha256=expected_marker_sha256,
            expected_current_source_sha=expected_current_source_sha,
            reason=reason,
        )
        with contextlib.suppress(OSError):
            binding.marker_dir.rmdir()
        return OrphanReceiptQuarantineResult(
            marker_id, expected_marker_sha256, descriptor.env, False, True,
            plan_sha256, str(final),
            "original marker moved byte-identically into retained quarantine",
            pause.summary,
        )


def recover_managed_update(
    cfg: config.Config,
    *,
    marker_id: str | None = None,
) -> ManagedUpdateRecoveryResult:
    """Recover one stale/failed serving-daemon transaction from its receipt.

    Recovery follows the receipt's exact durable phase.  Nonterminal phases
    restore and prove the old identity; terminal target-committed cleanup debt
    keeps and proves that exact committed target.  A target that merely may
    have landed is never promoted by inference; after rollback, the accepted
    report can be retried normally.
    """
    entries = [
        (path, marker)
        for path, marker in _admin_update_marker_entries()
        if marker is not None and marker.managed_transaction is not None
        and (marker_id is None or marker.marker_id == marker_id)
    ]
    if len(entries) != 1:
        raise AdminError(
            "recover-update requires exactly one matching durable managed "
            f"transaction receipt; found {len(entries)}"
        )
    path, marker = entries[0]
    diagnosis = diagnose_admin_update_marker(marker)
    if diagnosis.marker_status == ADMIN_UPDATE_MARKER_DIAG_RUNNING:
        raise AdminUpdateInProgress(
            "the managed updater still appears live; refusing recovery until "
            "its exact ownership locks and marker are stale"
        )
    raw = marker.managed_transaction
    env_hint = raw.get("env") if isinstance(raw, dict) else None
    if not isinstance(env_hint, str) or not env_hint:
        raise AdminError("managed update receipt has no valid environment")
    prog_hint = _resolve_venv_program(env_hint, cfg)
    with admin_update_ownership(), toolset_lifecycle_lock(
        [prog_hint], action="recover managed daemon update",
    ):
        # Re-read under both checkout/venv fences and bind the exact marker ID.
        current = _read_admin_update_marker_path(path)
        if (
            current is None
            or current.marker_id != marker.marker_id
            or current.managed_transaction != marker.managed_transaction
        ):
            raise AdminUpdateInProgress(
                "managed update receipt changed during recovery admission"
            )
        prog, lifecycle = _parse_managed_update_receipt(current, cfg)
        pause_multi_user, pause_queue_root = (
            _require_admin_update_pause_queue_scope(current)
        )
        # Adopt the exact stale lease only after both mutation fences and the
        # full untrusted-receipt validation succeed.  Subsequent phase writes
        # use the ordinary PID-bound persistence helpers; if recovery itself
        # dies, the marker simply becomes stale again with the same immutable
        # marker/transaction IDs.
        current.pid = os.getpid()
        current.pid_start_time = _pid_start_time(os.getpid()) or 0
        current.last_heartbeat_at = utcnow_iso()
        current.last_heartbeat_message = "recover-update adopted durable receipt"
        _write_admin_update_marker_atomic(current, path=path)
        _set_owned_admin_update_marker_path(path)
        if lifecycle.receipt_phase in {
            "old_restored", "target_committed", "target_cleanup_pending",
        }:
            expected_sha = (
                lifecycle.pre_source_sha
                if lifecycle.receipt_phase == "old_restored"
                else lifecycle.target_source_sha
            )
            expected_tree = (
                lifecycle.pre_source_tree_sha256
                if lifecycle.receipt_phase == "old_restored"
                else lifecycle.target_source_tree_sha256
            )
            if expected_sha is None or expected_tree is None:
                recovered, detail = False, "terminal receipt identity is incomplete"
            else:
                service_ok, service_detail = _reattest_service_before_start(lifecycle)
                provenance = _verify_restarted_daemon(
                    expected_sha,
                    expected_tree_sha256=expected_tree,
                    require_exact_identity=True,
                )
                recovered = service_ok and provenance.verified
                detail = f"{provenance.detail}; {service_detail}"
                if (
                    recovered
                    and lifecycle.receipt_phase == "target_committed"
                    and lifecycle.backup_moved
                ):
                    committed, commit_detail = _commit_managed_update_files(
                        prog, lifecycle,
                    )
                    recovered = committed
                    detail = f"{detail}; {commit_detail}"
                if recovered and lifecycle.receipt_phase in {
                    "target_committed", "target_cleanup_pending",
                }:
                    cleanup = lifecycle.venv_backup.parent / (
                        f".{lifecycle.venv_path.name}.vq-admin-committed-"
                        f"{lifecycle.transaction_id}"
                    )
                    if cleanup.exists() or cleanup.is_symlink():
                        try:
                            info = cleanup.lstat()
                            if (
                                not stat.S_ISDIR(info.st_mode)
                                or info.st_uid != lifecycle.owner_uid
                            ):
                                raise AdminError(
                                    f"unsafe committed-backup cleanup path {cleanup}"
                                )
                            shutil.rmtree(cleanup)
                            _fsync_directory_path(cleanup.parent)
                        except (OSError, AdminError) as exc:
                            recovered = False
                            detail = f"{detail}; committed backup cleanup failed: {exc}"
                    if recovered:
                        lifecycle.receipt_phase = "target_committed"
                        lifecycle.backup_moved = False
                        try:
                            _persist_managed_update_receipt(prog, lifecycle)
                        except AdminError as exc:
                            recovered = False
                            detail = (
                                f"{detail}; terminal cleanup checkpoint failed: {exc}"
                            )
        else:
            recovered, detail = _recover_managed_daemon_after_exception(
                prog, lifecycle, clear_receipt=False,
            )
        if not recovered:
            # Recovery helpers durably advance the receipt around destructive
            # swaps (for example backup_moved -> restoring_old ->
            # files_restored).  ``current`` is the snapshot read before those
            # checkpoints.  Writing it back here would roll the durable state
            # backward and can make a retry interpret an already-moved backup
            # as still present.  Persist the lifecycle's latest exact phase in
            # the same atomic FAILED update instead.
            current.managed_transaction = _managed_update_receipt_payload(
                lifecycle,
            )
            current.managed_transaction["git_dir"] = str(
                _canonical_lifecycle_checkout(Path(prog.git_dir))
            )
            current.state = ADMIN_UPDATE_STATE_FAILED
            current.failure_reason = "durable managed recovery failed: " + detail
            _write_admin_update_marker_atomic(current, path=path)
            return ManagedUpdateRecoveryResult(
                lifecycle.env, False, detail, "jobs remain scoped to the marker",
            )
        lifecycle.receipt_phase = (
            "target_committed"
            if lifecycle.receipt_phase in {
                "target_committed", "target_cleanup_pending",
            }
            else "old_restored"
        )
        lifecycle.backup_moved = False
        current.managed_transaction = _managed_update_receipt_payload(lifecycle)
        current.managed_transaction["git_dir"] = str(
            _canonical_lifecycle_checkout(Path(prog.git_dir))
        )
        _write_admin_update_marker_atomic(current, path=path)
        assert current.pause_token is not None  # parser validated exact format
        resume_proof = resume_token_scope_with_proof(
            current.host,
            current.pause_token,
            multi_user=pause_multi_user,
            queue_root=pause_queue_root,
        )
        try:
            resume_proof.require_clear()
        except BaseException as exc:
            current.state = ADMIN_UPDATE_STATE_FAILED
            current.failure_reason = (
                "managed files/service recovered but paused jobs remain: "
                f"{exc}"
            )
            _write_admin_update_marker_atomic(current, path=path)
            return ManagedUpdateRecoveryResult(
                lifecycle.env, False, detail, resume_proof.summary,
            )
        resumed = resume_proof.summary
        _clear_completed_admin_update_marker(current)
        return ManagedUpdateRecoveryResult(
            lifecycle.env, True, detail, resumed,
        )


def _refuse_live_per_user_state_under_pytest() -> None:
    """Refuse to create the admin-update marker in live user state (#438).

    The marker is real state: it wedges the host's admin lane, and neither
    ``clear-update-marker`` nor ``recover-update`` can retire one whose receipt
    points at a deleted pytest temp directory. Forty-five test call sites create
    a real marker on purpose, and their only protection is the autouse
    ``_isolate_vq_state_env`` fixture in ``vibe-queue/tests/conftest.py``. That
    fixture is bypassed by any conftest-less invocation -- ``--noconftest`` is
    the documented way to run this repo's bugctl lanes -- and the write then
    lands in ``~/.local/share/vq`` while the test still passes.

    So the fixture cannot be the only guard. :mod:`vq.paths` now requires every
    pytest-controlled persistent root to be explicit and contained beneath
    ``VQ_TEST_SANDBOX_ROOT``. This marker-specific check remains as defence in
    depth against a caller that deliberately points its contained state override
    at its XDG fallback. Outside pytest both guards are inert, so a real
    ``vq admin update`` is never affected.
    """
    if "PYTEST_CURRENT_TEST" not in os.environ:
        return
    resolved = paths.state_root().expanduser().resolve(strict=False)
    if resolved != paths.xdg_state_root().expanduser().resolve(strict=False):
        return
    raise AdminError(
        f"refusing to create {ADMIN_UPDATE_MARKER_FILENAME} under the live "
        f"per-user state root {resolved} while running under pytest: this "
        "would wedge the host's admin lane with an artifact no sanctioned "
        f"command can retire. Set {paths.ENV_STATE_DIR} to a temporary "
        "directory for this test (the autouse _isolate_vq_state_env fixture "
        "does it; a conftest-less run such as --noconftest does not)."
    )


def acquire_admin_update_marker(
    envs: list[str], host: str, *, force: bool = False,
) -> AdminUpdateMarker:
    """v0.5.50: atomic check-and-write of the admin-update-in-progress
    marker. Closes the concurrent-admin-update race the audit flagged
    in § 2c: pre-v0.5.50 the guard
    (:func:`_guard_admin_update_marker`) and the writer
    (:func:`write_admin_update_marker`) were two separate calls, so
    two ``vq admin update`` invocations from different terminals
    could both pass the existence check before either wrote the
    marker — the second write would silently overwrite the first.

    With ``force=False`` (default) this uses ``os.open`` with
    ``O_CREAT|O_EXCL`` so the check-and-create is one atomic kernel
    call. ``FileExistsError`` from the open is converted to
    :class:`AdminError` with the same recovery-recipe message
    :func:`_guard_admin_update_marker` produces.

    With ``force=True`` the marker is unconditionally overwritten
    (matching pre-v0.5.50 ``write_admin_update_marker`` semantics)
    so the ``vq admin update --force`` flag's documented behaviour
    is preserved.

    Returns the marker dataclass on success, same shape as
    :func:`write_admin_update_marker`."""
    from vq import __version__ as vq_version

    _refuse_live_per_user_state_under_pytest()

    now = utcnow_iso()
    marker = AdminUpdateMarker(
        envs=list(envs),
        host=host,
        started_at=now,
        pid=os.getpid(),
        vq_version=vq_version,
        # v0.11.0: anti-recycling fingerprint — see AdminUpdateMarker
        # and admin_update_marker_stale_reason. This is the production
        # writer, so without it real markers would miss the recycled-pid
        # signal and lean on the age backstop alone.
        pid_start_time=_pid_start_time(os.getpid()) or 0,
        # v0.6.0: initial state = PAUSING. Caller drives the rest of
        # the state machine via transition_admin_update_state.
        state=ADMIN_UPDATE_STATE_PAUSING,
        phase_started_at=now,
        last_heartbeat_at=now,
        last_heartbeat_message=f"state={ADMIN_UPDATE_STATE_PAUSING}",
        marker_id=uuid.uuid4().hex,
        detached_run_id=current_detached_run_id(),
    )
    serialised = json.dumps(asdict(marker), indent=2, sort_keys=True)
    with _admin_update_marker_lock() as binding:
        entries = _admin_update_marker_entries()
        conflicts = [
            (path, existing)
            for path, existing in entries
            if existing is None
            or admin_update_scopes_conflict(
                existing.envs,
                existing.host,
                envs,
                host,
            )
        ]
        if conflicts and not force:
            existing = conflicts[0][1]
            detail = _format_marker_detail(existing)
            diag = diagnose_admin_update_marker(existing)
            raise AdminMarkerPresent(
                f"admin-update-in-progress marker present and conflicts with "
                f"envs={envs}, host={host}: {detail}. "
                f"{diag.summary}. {diag.action}"
            )
        if force:
            # Preserve the historical explicit-override contract: force
            # acknowledges and replaces every marker in this state root.
            # Normal (non-force) updates retain independent leases.
            receipts = [
                existing
                for _path, existing in entries
                if existing is not None and (
                    existing.managed_transaction is not None
                    or existing.owns_pause_scope
                )
            ]
            if receipts:
                raise AdminUpdateInProgress(
                    "refusing --force while a durable update/pause receipt is "
                    "present; run `vq admin recover-update` so files, service, "
                    "and the exact paused-job scope are reconciled first"
                )
            for path, _ in entries:
                path.unlink(missing_ok=True)
            entries = []
        if not entries and not os.path.lexists(binding.marker_path):
            path = binding.marker_path
        else:
            path = binding.marker_dir / f"{marker.marker_id}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(
                str(path),
                os.O_CREAT | os.O_EXCL | os.O_WRONLY
                | getattr(os, "O_CLOEXEC", 0),
                0o600,
            )
        except FileExistsError as exc:
            raise AdminUpdateInProgress(
                "admin-update marker lease collision; retry after inspecting "
                "`vq admin status --verbose`"
            ) from exc
        try:
            os.write(fd, serialised.encode("utf-8"))
            os.fsync(fd)
        finally:
            os.close(fd)
        directory_fd = os.open(
            str(path.parent), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    _set_owned_admin_update_marker_path(path)
    return marker


def clear_admin_update_marker(
    marker: AdminUpdateMarker | None = None,
) -> AdminUpdateMarker | None:
    """v0.5.44: remove the marker file. Returns the parsed marker
    that was cleared (so callers can display "cleared marker:
    env=X pid=Y started=Z"), or None if no marker was present (in
    which case the operation is a quiet no-op — idempotent)."""
    with _admin_update_marker_lock() as binding:
        entries = _admin_update_marker_entries()
        target: tuple[Path, AdminUpdateMarker | None] | None = None
        if marker is not None:
            for entry in entries:
                candidate = entry[1]
                if candidate is marker or (
                    candidate is not None
                    and marker.marker_id
                    and candidate.marker_id == marker.marker_id
                ) or (
                    candidate is not None
                    and not marker.marker_id
                    and candidate.pid == marker.pid
                    and candidate.started_at == marker.started_at
                    and candidate.envs == marker.envs
                    and candidate.host == marker.host
                ):
                    target = entry
                    break
        else:
            if getattr(
                _admin_update_marker_owner, "forked_without_owner", False,
            ):
                raise AdminUpdateInProgress(
                    "forked child has no admin-update marker ownership; "
                    "refusing an implicit clear"
                )
            owned = _owned_admin_update_marker_path()
            if owned is not None:
                target = next(
                    (entry for entry in entries if entry[0] == owned),
                    None,
                )
            if target is None and len(entries) == 1:
                target = entries[0]
            elif target is None and len(entries) > 1:
                raise AdminError(
                    "multiple admin-update markers are present; name a "
                    "specific scope or let each owning updater clear its lease"
                )
        if target is None:
            return None
        path, snapshot = target
        if snapshot is not None and (
            snapshot.managed_transaction is not None
            or snapshot.owns_pause_scope
        ):
            raise AdminError(
                "refusing to clear an admin-update marker that owns a durable "
                "managed-daemon or paused-job transaction; run `vq admin "
                "recover-update` "
                "instead"
            )
        path.unlink(missing_ok=True)
        _fsync_directory_path(path.parent)
        if path == _owned_admin_update_marker_path():
            _set_owned_admin_update_marker_path(None)
        directory = binding.marker_dir
        with contextlib.suppress(OSError):
            directory.rmdir()
        return snapshot


def recover_pause_scope_and_clear_marker(
    marker: AdminUpdateMarker,
) -> tuple[AdminUpdateMarker, str]:
    """Explicitly acknowledge a pause-only stale marker without stranding jobs.

    Managed virtualenv receipts must use :func:`recover_managed_update`.
    Ordinary interrupted updates have no failure-atomic file receipt, so the
    CLI asks the operator to inspect the env before calling this helper.  It
    nevertheless reconciles and proves the exact token scope before the one
    terminal unlink.
    """
    if marker.managed_transaction is not None:
        raise AdminError(
            "managed-daemon receipts require `vq admin recover-update`"
        )
    if (
        not isinstance(marker.pause_token, str)
        or re.fullmatch(r"admin-update-[0-9a-f]{12}", marker.pause_token) is None
    ):
        raise AdminError("marker has no valid durable pause token to recover")
    pause_multi_user, pause_queue_root = (
        _require_admin_update_pause_queue_scope(marker)
    )
    with admin_update_ownership():
        current = next(
            (
                candidate for candidate in read_admin_update_markers()
                if candidate.marker_id == marker.marker_id
            ),
            None,
        )
        if (
            current is None
            or current.pause_token != marker.pause_token
            or current.pause_multi_user != marker.pause_multi_user
            or current.pause_queue_root != marker.pause_queue_root
        ):
            raise AdminUpdateInProgress(
                "admin-update pause receipt changed during clear admission"
            )
        # Repeat validation under ownership so a root-override change or marker
        # replacement cannot redirect the exact scan admitted above.
        pause_multi_user, pause_queue_root = (
            _require_admin_update_pause_queue_scope(current)
        )
        proof = resume_token_scope_with_proof(
            current.host,
            current.pause_token,
            multi_user=pause_multi_user,
            queue_root=pause_queue_root,
        )
        proof.require_clear()
        _clear_completed_admin_update_marker(current)
        return current, proof.summary


LOCAL_DISPATCH_SCOPE = "@local"
"""Sentinel in :func:`admin_update_marker_scope` results meaning "jobs that
execute on this daemon's own host" (specs with no ``scheduler_target``)."""


def _admin_update_resources(
    envs: list[str], host: str,
) -> frozenset[tuple[str, str, str]] | None:
    """Parse update identities into conflict resources.

    ``None`` is global/fail-closed. Scheduler helpers overlap every runtime on
    their host; scheduler runtimes overlap only the same program (plus their
    helper); local managed envs overlap only the same host/component.
    """
    if not envs or not isinstance(host, str) or not host:
        return None
    resources: set[tuple[str, str, str]] = set()
    for env in envs:
        if not isinstance(env, str) or not env:
            return None
        if env.startswith("scheduler-runtime:"):
            parts = env.split(":")
            if (
                len(parts) != 3
                or not parts[1]
                or not parts[2]
                or parts[1] != host
            ):
                return None
            resources.add(("scheduler-runtime", parts[1], parts[2]))
        elif env.startswith("scheduler:"):
            parts = env.split(":")
            if len(parts) != 2 or not parts[1] or parts[1] != host:
                return None
            resources.add(("scheduler-helper", parts[1], ""))
        elif ":" in env:
            return None
        else:
            # Plain env markers live in one daemon state root. The recorded
            # host is diagnostic and legacy callers did not normalize it, so
            # the component name is the complete conflict identity here.
            resources.add(("local-runtime", LOCAL_DISPATCH_SCOPE, env))
    return frozenset(resources)


def admin_update_scopes_conflict(
    existing_envs: list[str],
    existing_host: str,
    requested_envs: list[str],
    requested_host: str,
) -> bool:
    """Whether two admin-update scopes touch the same mutable resource."""
    existing = _admin_update_resources(existing_envs, existing_host)
    requested = _admin_update_resources(requested_envs, requested_host)
    if existing is None or requested is None:
        return True
    for left in existing:
        for right in requested:
            if left == right:
                return True
            left_kind, left_host, _ = left
            right_kind, right_host, _ = right
            if left_host != right_host:
                continue
            if {
                left_kind,
                right_kind,
            } == {"scheduler-helper", "scheduler-runtime"}:
                return True
    return False


def admin_update_marker_scope(
    marker: AdminUpdateMarker | None,
) -> frozenset[str] | None:
    """Which dispatch targets a live admin-update marker holds.

    Returns a frozenset of scheduler host names (plus
    :data:`LOCAL_DISPATCH_SCOPE` for local execution), or ``None`` meaning
    **hold everything** — the pre-scoping behavior, kept as the fail-safe for
    an unreadable marker or an env entry this vq does not recognise.

    The blanket hold was correct where the marker was born (a venv host
    rebuilding its own env must not dispatch into it) but over-blocks on a
    driver that manages several scheduler hosts: a pbs-cluster runtime rebuild
    (``scheduler-runtime:pbs-cluster:vibeqc-dev``) held slurm-cluster SLURM handoffs on an
    idle cluster (2026-07-25, job d446f1a62143). The marker's ``envs``
    already name exactly what is being protected; this parses them:

    * ``scheduler:<host>`` (helper update) → holds ``<host>``
    * ``scheduler-runtime:<host>:<program>`` → holds ``<host>``
    * a plain env name (venv update on this daemon's host) → holds local
      execution only
    """
    if marker is None or not marker.envs:
        return None
    held: set[str] = set()
    for env in marker.envs:
        if not isinstance(env, str) or not env:
            return None
        if env.startswith("scheduler-runtime:"):
            parts = env.split(":")
            if len(parts) != 3 or not parts[1]:
                return None
            held.add(parts[1])
        elif env.startswith("scheduler:"):
            host = env.split(":", 1)[1]
            if not host:
                return None
            held.add(host)
        elif ":" in env:
            # A scoped form this vq does not know (written by a newer or
            # older vq). Guessing narrow could dispatch into an update.
            return None
        else:
            held.add(LOCAL_DISPATCH_SCOPE)
    return frozenset(held)


def admin_update_markers_scope() -> frozenset[str] | None:
    """Union dispatch holds from every marker lease.

    Any unreadable or unrecognised lease keeps the historical global hold.
    """
    entries = _admin_update_marker_entries()
    if not entries:
        return frozenset()
    held: set[str] = set()
    for _, marker in entries:
        scope = admin_update_marker_scope(marker)
        if scope is None:
            return None
        held.update(scope)
    return frozenset(held)


def _guard_admin_update_marker(
    *,
    force: bool,
    envs: list[str] | None = None,
    host: str = "",
) -> None:
    """v0.5.44: raise :class:`AdminError` if a marker is present and
    the caller hasn't passed ``force=True``. Called at the entry of
    :func:`update_env` and :func:`update_all` before any pause/work,
    so a blocked call doesn't briefly pause the queue.

    Uses :func:`admin_update_marker_exists` (cheap stat) for the
    decision so a corrupt-but-present marker still blocks. Falls
    back to :func:`read_admin_update_marker` for the diagnostic
    message — when the file exists but parses to None, the message
    says ``(unreadable)`` rather than pretending the marker is
    absent."""
    if force or not admin_update_marker_exists():
        return
    requested_envs = envs or []
    for _, marker in _admin_update_marker_entries():
        if marker is not None and not admin_update_scopes_conflict(
            marker.envs,
            marker.host,
            requested_envs,
            host,
        ):
            continue
        detail = _format_marker_detail(marker)
        diag = diagnose_admin_update_marker(marker)
        raise AdminMarkerPresent(
            f"admin-update-in-progress marker present and conflicts with "
            f"envs={requested_envs}, host={host}: {detail}. "
            f"{diag.summary}. {diag.action}"
        )


ADMIN_STATUS_FILENAME = "admin-status.json"
"""Persisted record of the most recent ``vq admin update`` per env,
keyed by env name. Lives at ``<state_root>/admin-status.json``. Each
entry: {last_updated_at, last_success, last_sha, last_tag,
last_expected_tag, last_expected_sha, last_git_pull_rc, last_update_script_rc,
last_branch_expected, last_branch_actual,
last_update_script_output}. v0.7.1 added the two ``last_branch_*``
fields and ``last_update_script_output``; older record files read
transparently because ``read_admin_status`` defaults the missing
keys + strips unknown keys."""


VQ_ADMIN_UPDATE_OUTPUT_LINES = "VQ_ADMIN_UPDATE_OUTPUT_LINES"
"""v0.7.1 *Lamport's Clock*: env var to override the number of
tail lines persisted in ``AdminUpdateRecord.last_update_script_output``.
Default 80 (= ~6 KB at 80-char lines, comfortably bounded), set in
:func:`_tail_lines`. Set to 0 to disable capture entirely. Higher
values trade admin-status.json size for failure-mode richness."""


_DEFAULT_OUTPUT_TAIL_LINES = 80


def _tail_lines(text: str, n: int | None = None) -> str | None:
    """v0.7.1: return the last ``n`` lines of ``text``, where ``n``
    defaults to the env var ``VQ_ADMIN_UPDATE_OUTPUT_LINES`` (or
    80 if unset). Returns ``None`` when text is empty (callers
    serialize empty as ``None`` to keep admin-status.json clean)
    or when ``n=0`` (capture disabled).

    Why tail-truncate rather than head-truncate: build failures
    are almost always on the LAST line(s) — the failing command's
    stderr is what the operator needs to see. Keeping the head
    would surface "Successfully compiled foo.o" for the 80 cleanly-
    built files and miss the cc1plus segfault on file 81."""
    if not text:
        return None
    if n is None:
        try:
            n = int(os.environ.get(
                VQ_ADMIN_UPDATE_OUTPUT_LINES,
                _DEFAULT_OUTPUT_TAIL_LINES,
            ))
        except ValueError:
            n = _DEFAULT_OUTPUT_TAIL_LINES
    if n <= 0:
        return None
    lines = text.splitlines(keepends=True)
    if len(lines) <= n:
        return text
    return "".join(lines[-n:])


@dataclass
class AdminUpdateRecord:
    """One-row history entry, written by ``update_env`` at end of run.
    All fields optional so we can write partial records when something
    fails before we got data."""

    last_updated_at: str
    last_success: bool
    last_sha: str | None = None
    last_tag: str | None = None
    last_expected_tag: str | None = None
    last_expected_sha: str | None = None
    last_installed_sha: str | None = None
    """Commit the venv was last successfully INSTALLED from.

    Distinct from :attr:`last_sha`, which is where the checkout ended up. They
    agree after a successful update and diverge after a rolled-back one: the
    rollback resets the tree but never re-runs the editable install, so the
    `.dist-info` keeps describing the previous commit. Localhost, 2026-08-01:
    a failed build left the checkout months behind while
    `vibeqc.__version__` still read the newer version, and nothing on any
    surface distinguished that from a healthy env.

    Carried forward unchanged by a failed update -- the install did not move,
    so neither does this. `vq admin status` compares it against the live
    checkout and reports a mismatch."""
    last_git_pull_rc: int | None = None
    last_update_script_rc: int | None = None
    # v0.7.1 *Lamport's Clock*: post-pull branch verification snapshot.
    # ``last_branch_expected`` = ``VenvProgram.branch`` at update time
    # (the contract); ``last_branch_actual`` = ``git rev-parse
    # --abbrev-ref HEAD`` after the pull (reality). Equal ⇒ env on the
    # right branch; unequal ⇒ silent drift (see incident postmortem in
    # ``docs/v0_7_1_lamports_clock_design.md``). Both ``None`` when
    # ``VenvProgram.branch`` was unset (legacy env, operator manages
    # branch by hand).
    last_branch_expected: str | None = None
    last_branch_actual: str | None = None
    # v0.7.1 *Lamport's Clock* Item 5: post-update dirty-tree flag.
    # ``True`` ⇒ the post-update tree had uncommitted changes;
    # ``False`` ⇒ clean; ``None`` ⇒ check didn't run (legacy env or
    # git-status query failed). Independent of LAST OK — operators
    # need to know dirty-after-update regardless of whether the
    # update itself succeeded.
    last_dirty_after_update: bool | None = None
    # v0.12.x: machine-readable/admin-visible diagnosis for failures that
    # are not captured by git/script rc alone, especially vq self-update
    # restart/provenance failures.
    last_failure_reason: str | None = None
    last_daemon_restart_attempted: bool | None = None
    last_daemon_restart_succeeded: bool | None = None
    last_daemon_health_verified: bool | None = None
    last_daemon_expected_source_sha: str | None = None
    last_daemon_actual_source_sha: str | None = None
    # Content-derived counterparts to the two SHA fields above. The SHAs are a
    # checkout commit compared against an installed package's declaration; these
    # compare bytes to bytes, which is what makes a stale-marker failure
    # distinguishable from a stale-code one without SSHing to the host.
    last_daemon_expected_source_tree_sha256: str | None = None
    last_daemon_actual_source_tree_sha256: str | None = None
    last_daemon_restart_message: str | None = None
    # v0.7.1 *Lamport's Clock* Item 2: tail of the update_script's
    # combined stdout+stderr. Truncated to N lines via
    # :func:`_tail_lines` (default 80, env override
    # ``VQ_ADMIN_UPDATE_OUTPUT_LINES``). ``None`` when the script
    # didn't run OR produced no output. Captures the "why did the
    # update fail?" signal so the operator doesn't have to SSH to
    # the host and grep log files — the 2026-05-25 incident's
    # biggest time sink (three workstation cycles, each costing a
    # round-trip just to discover the failure mode).
    last_update_script_output: str | None = None
    # v0.7.1 *Lamport's Clock* Item 4: ``vq admin mark-ok ENV
    # --note "reason"`` operator escape hatch — when set,
    # ``last_success`` was flipped True out-of-band (operator
    # verified the env via other means; the surgical Python edit
    # we did on 2026-05-25 is the canonical case). Both fields
    # set as a pair: a marked-ok record is identifiable by
    # ``last_marked_ok_at is not None``. ``vq admin status``
    # shows a ``*`` next to ``LAST OK`` to flag the asterisk
    # source; --verbose shows the note. Cleared (reset to None
    # on both) by the next real ``vq admin update`` that records
    # an outcome via :func:`record_update_outcome` — so a real
    # success / failure always supersedes a mark-ok.
    last_marked_ok_at: str | None = None
    last_marked_ok_note: str | None = None


def admin_status_path() -> Path:
    return paths.state_root() / ADMIN_STATUS_FILENAME


def _update_failure_reason(result: UpdateResult) -> str | None:
    """Concise persisted reason for a failed admin update."""
    if result.success:
        return None
    if result.git_pull_rc not in (0, None):
        return f"git pull rc={result.git_pull_rc}"
    if result.update_script and result.update_script_rc not in (0, None):
        return f"update_script rc={result.update_script_rc}"
    if (
        result.post_update_script
        and result.post_update_script_rc not in (0, None)
    ):
        return f"post_update_script rc={result.post_update_script_rc}"
    if result.work_errors:
        return f"work_errors: {'; '.join(result.work_errors)}"
    if result.rolled_back:
        return "rolled back after failed build/import verification"
    if result.import_check_rc is not None and result.import_check_rc != 0:
        return f"import_check rc={result.import_check_rc}"
    if (
        result.daemon_restart_attempted
        and result.daemon_restart_succeeded is False
    ):
        return f"daemon restart failed: {result.daemon_restart_message}"
    if result.expected_tag and not result.tag_matches:
        return (
            f"tag mismatch: expected {result.expected_tag!r}, "
            f"got {result.actual_tag!r}"
        )
    if result.expected_sha and not result.sha_matches:
        return (
            f"SHA mismatch: expected {result.expected_sha}, "
            f"got {result.actual_sha}"
        )
    if result.branch_verification_attempted and not result.branch_matches:
        return (
            f"branch mismatch: expected {result.branch!r}, "
            f"got {result.actual_branch!r}"
        )
    if result.fail_on_dirty_in_effect and result.dirty_after_update is True:
        return "dirty tree after update"
    return "update did not complete cleanly"


def read_admin_status(
    *, via_rpc: bool = True,
) -> dict[str, AdminUpdateRecord]:
    """Load the per-env last-update records. Corrupt file = empty dict
    (same conservative read-side stance as drain/throttle).

    v0.7.1 made the per-record load forward-compatible: unknown keys
    (e.g. a record written by a newer vq that added fields the local
    client doesn't know about) are silently stripped before the
    dataclass constructor runs. Pre-v0.7.1 the constructor's
    TypeError caused the whole entry to be dropped — meaning a newer
    daemon could write a record that an older client would treat as
    "no last update at all". The strip-then-construct path preserves
    everything the local client *does* know about.

    v0.8.0 *Dahl's Simula*: when ``via_rpc=True`` (the default for CLI
    callers), the read is routed through the daemon's RPC socket — so
    the user-XDG vs daemon-XDG split (v0.7.12 footgun) goes away. On
    RPC failure (daemon down, socket missing) the function falls back
    to the direct file read documented above; multi-user mode logs a
    WARNING on fallback because the local file is then a stale view.
    Internal callers (the daemon, the RPC server handler itself) pass
    ``via_rpc=False`` to short-circuit the loop.
    """
    if via_rpc:
        from vq import rpc as _rpc  # noqa: PLC0415 — circular if top-level
        from vq.config import load_config

        try:
            mu = load_config().multi_user.enabled
        except Exception:  # noqa: BLE001 — config load must not block read
            mu = False
        result = _rpc.try_rpc_or_fallback(
            "get_admin_status",
            multi_user=mu,
            fallback=lambda: read_admin_status(via_rpc=False),
        )
        # RPC response is already a dict[env, dict]; convert back to
        # the dataclass shape. Fallback path returns the right shape
        # already, so detect by type.
        if isinstance(result, dict) and result and not isinstance(
            next(iter(result.values())), AdminUpdateRecord,
        ):
            known_fields = {f.name for f in fields(AdminUpdateRecord)}
            out: dict[str, AdminUpdateRecord] = {}
            for env_name, rec in result.items():
                if not isinstance(rec, dict):
                    continue
                filtered = {
                    k: v for k, v in rec.items() if k in known_fields
                }
                try:
                    out[env_name] = AdminUpdateRecord(**filtered)
                except TypeError:
                    continue
            return out
        return result or {}
    path = admin_status_path()
    if not path.exists():
        return {}
    try:
        with path.open() as f:
            data = json.load(f)
    except (OSError, ValueError):
        return {}
    known_fields = {f.name for f in fields(AdminUpdateRecord)}
    out2: dict[str, AdminUpdateRecord] = {}
    for env_name, rec in data.items():
        if not isinstance(rec, dict):
            continue
        # v0.7.1: strip unknown keys before construction so a record
        # written by a newer vq loads cleanly under an older client.
        filtered = {k: v for k, v in rec.items() if k in known_fields}
        try:
            out2[env_name] = AdminUpdateRecord(**filtered)
        except TypeError:
            # Truly corrupt — missing required positional args, etc.
            # Skip rather than crash the whole read.
            continue
    return out2


def write_admin_status(
    records: dict[str, AdminUpdateRecord],
    *,
    via_rpc: bool = False,
    admin_token: str | None = None,
    require_rpc: bool = False,
    multi_user: bool | None = None,
) -> None:
    """Atomic tmpfile-rename write.

    v0.8.0: ``via_rpc=True`` routes through the daemon's RPC for
    each env entry (one ``set_admin_status`` per record). Internal
    callers (the daemon, RPC handlers) use ``via_rpc=False``.

    ``admin_token`` carries a token already authenticated by the CLI.
    It stays in process memory rather than being rediscovered from an
    argv or environment variable. ``require_rpc`` makes a failed RPC
    write fail closed instead of falling back to the caller's local
    state file; record_update_outcome uses that mode on multi-user
    deployments because only the daemon-owned file is canonical there.
    Single-user callers retain the historical direct-file fallback.
    """
    if require_rpc and not via_rpc:
        raise AdminError("require_rpc=True requires via_rpc=True")
    if via_rpc:
        from vq import rpc as _rpc  # noqa: PLC0415
        from vq.config import load_config

        if multi_user is None:
            try:
                cfg = load_config()
                mu = (
                    cfg.multi_user.enabled
                    or config.system_multi_user_enabled()
                )
            except Exception:  # noqa: BLE001
                mu = False
        else:
            mu = multi_user
        token = admin_token
        if mu and token is None:
            from vq import auth as _auth  # noqa: PLC0415
            token = _auth.resolve_token(None)
        # Fire one set_admin_status per env. Failure on ANY env
        # raises; caller decides whether to fall back.
        try:
            for env_name, rec in records.items():
                _rpc.call(
                    "set_admin_status",
                    {
                        "env": env_name,
                        "record": asdict(rec),
                        "token": token,
                    },
                    multi_user=mu,
                )
            return
        except (_rpc.RPCError, ConnectionError) as e:
            if require_rpc:
                raise AdminError(
                    "daemon RPC set_admin_status failed; canonical "
                    f"admin state was not updated ({e})"
                ) from e
            if mu:
                log.warning(
                    "RPC set_admin_status failed (%s); falling back "
                    "to direct file write — multi-user mode may now "
                    "diverge from the daemon's canonical view.", e,
                )
            # Fall through to the direct-write path.
    path = admin_status_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    serialised = {name: asdict(rec) for name, rec in records.items()}
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(serialised, indent=2, sort_keys=True))
    tmp.replace(path)


def replace_admin_status_record_from_mapping(
    env: str,
    record: dict[str, object],
) -> None:
    """Validate and directly replace one daemon-owned admin-status record.

    Unknown fields are stripped for mixed-version compatibility before the
    existing dataclass constructor validates required fields.  The direct
    read/write calls preserve the RPC handler's canonical, recursion-free
    persistence path.
    """
    known = {item.name for item in fields(AdminUpdateRecord)}
    filtered = {key: value for key, value in record.items() if key in known}
    new_record = AdminUpdateRecord(**filtered)
    records = read_admin_status(via_rpc=False)
    records[env] = new_record
    write_admin_status(records, via_rpc=False)


def record_update_outcome(
    env: str,
    result: UpdateResult,
    *,
    admin_token: str | None = None,
    multi_user: bool | None = None,
) -> None:
    """v0.5.25: called at the end of ``update_env`` to persist the
    outcome for later inspection via ``vq admin status``.

    Single-user persistence remains best-effort: RPC failure falls back to
    the user's direct state file, and a final filesystem error does not turn
    a healthy runtime into a failed update. Multi-user persistence is an
    authenticated canonical-state gate: RPC failure raises ``AdminError`` so
    the caller cannot report success while only a per-user fallback file was
    changed.
    """
    if multi_user is None:
        try:
            multi_user = (
                config.load_config().multi_user.enabled
                or config.system_multi_user_enabled()
            )
        except Exception:  # noqa: BLE001 - default safely to single-user
            multi_user = False
    records = read_admin_status()
    # Snapshot the post-update SHA so status can answer "what commit is
    # this env on right now?" without re-running git on every status
    # call. If the rev-parse fails (no git, dir gone), we record None.
    sha = _query_git_sha(Path(result.git_dir))
    # A successful update installed from wherever the checkout ended up. A
    # FAILED one did not touch the install at all -- the rollback resets the
    # tree and never re-runs the editable install -- so the previous value
    # carries forward. That divergence is exactly what status now reports.
    prior_record = records.get(result.env)
    installed_sha = (
        sha
        if result.success
        else getattr(prior_record, "last_installed_sha", None)
    )
    records[env] = AdminUpdateRecord(
        last_updated_at=utcnow_iso(),
        last_success=result.success,
        last_sha=sha,
        last_tag=result.actual_tag,
        last_expected_tag=result.expected_tag,
        last_expected_sha=result.expected_sha,
        last_installed_sha=installed_sha,
        last_git_pull_rc=result.git_pull_rc,
        last_update_script_rc=result.update_script_rc,
        # v0.7.1: branch verification snapshot. ``result.branch`` is
        # the configured ``VenvProgram.branch`` (the contract);
        # ``result.actual_branch`` is what HEAD pointed at after the
        # pull (reality). Both are ``None`` when the env has no
        # configured branch — record_update_outcome serializes them
        # transparently and old admin-status.json files read fine
        # because ``read_admin_status`` lets missing keys default.
        last_branch_expected=result.branch,
        last_branch_actual=result.actual_branch,
        # v0.7.1 Item 2: tail-truncated update_script output for the
        # "WHY did it fail?" question. _tail_lines returns None for
        # empty output or when disabled via VQ_ADMIN_UPDATE_OUTPUT_LINES=0,
        # which keeps successful-build noise out of admin-status.json.
        last_update_script_output=_tail_lines(result.update_script_output),
        # v0.7.1 Item 5: post-update dirty-tree snapshot.
        last_dirty_after_update=result.dirty_after_update,
        last_failure_reason=_update_failure_reason(result),
        last_daemon_restart_attempted=result.daemon_restart_attempted,
        last_daemon_restart_succeeded=result.daemon_restart_succeeded,
        last_daemon_health_verified=result.daemon_health_verified,
        last_daemon_expected_source_sha=result.daemon_expected_source_sha,
        last_daemon_actual_source_sha=result.daemon_actual_source_sha,
        last_daemon_expected_source_tree_sha256=(
            result.daemon_expected_source_tree_sha256
        ),
        last_daemon_actual_source_tree_sha256=(
            result.daemon_actual_source_tree_sha256
        ),
        last_daemon_restart_message=(
            result.daemon_restart_message or None
        ),
    )
    if multi_user:
        write_admin_status(
            records,
            via_rpc=True,
            admin_token=admin_token,
            require_rpc=True,
            multi_user=True,
        )
    else:
        with contextlib.suppress(OSError):
            write_admin_status(
                records,
                via_rpc=True,
                admin_token=admin_token,
            )


def mark_env_ok(env: str, *, note: str, cfg: config.Config) -> AdminUpdateRecord:
    """v0.7.1 *Lamport's Clock* Item 4: operator escape hatch.

    Flip ``last_success=True`` for ``env`` after the operator has
    verified the env is healthy out-of-band — typically because a
    manual rebuild (or the surgical Python edit we used on
    2026-05-25) succeeded but didn't go through ``vq admin update``,
    so the persisted record still shows ``last_success=False`` even
    though the env is fine.

    ``note`` is required — the rationale lands in
    ``last_marked_ok_note`` so a later operator can audit "why is
    this marked True?" without git-archaeology. Empty/whitespace-
    only notes raise ``AdminError`` (no silent acknowledgement).

    Records ``last_marked_ok_at`` so ``vq admin status`` can show
    the asterisk-source distinction (real update vs operator
    acknowledge). The next ``vq admin update`` that runs
    ``record_update_outcome`` will overwrite both fields (a real
    outcome always supersedes a mark-ok).

    Raises :class:`AdminError` for unknown env (mirror
    :func:`update_env`'s validation) or empty note.
    """
    # Reuse the standard env validation so unknown envs / wrong
    # kinds fail with the same error as `vq admin update <env>`.
    _resolve_venv_program(env, cfg)
    if not note or not note.strip():
        raise AdminError(
            f"mark-ok requires a --note explaining the manual "
            f"acknowledgement (got {note!r}). The note lands in "
            f"the audit record so a later operator can answer "
            f"'why was {env} marked OK without going through "
            f"vq admin update?' without git-archaeology."
        )
    records = read_admin_status()
    prior = records.get(env)
    sha = _query_git_sha(Path(_resolve_venv_program(env, cfg).git_dir))
    now = utcnow_iso()
    records[env] = AdminUpdateRecord(
        last_updated_at=now,
        last_success=True,
        last_sha=sha,
        # Preserve whatever the last real update knew about tags +
        # branch + script rc, so the mark-ok doesn't blow away
        # historical context. None when there's no prior record.
        last_tag=prior.last_tag if prior else None,
        last_expected_tag=prior.last_expected_tag if prior else None,
        last_expected_sha=prior.last_expected_sha if prior else None,
        last_git_pull_rc=prior.last_git_pull_rc if prior else None,
        last_update_script_rc=prior.last_update_script_rc if prior else None,
        last_branch_expected=prior.last_branch_expected if prior else None,
        last_branch_actual=prior.last_branch_actual if prior else None,
        last_update_script_output=(
            prior.last_update_script_output if prior else None
        ),
        # The mark-ok-specific fields:
        last_marked_ok_at=now,
        last_marked_ok_note=note.strip(),
    )
    write_admin_status(records)
    return records[env]


@dataclass
class ResetBranchResult:
    """v0.7.9 *Liskov's Substitution*: outcome of a
    ``vq admin reset-branch`` invocation.

    Captures enough state for the CLI to render a meaningful
    summary (prior SHA → new SHA, branch, fetch + reset return
    codes) and for the admin-status record to reflect what
    happened so a later operator can audit "why did this env
    suddenly jump SHAs?".
    """
    env: str
    branch: str
    prior_sha: str | None
    new_sha: str | None
    fetch_rc: int | None
    reset_rc: int | None
    output: str
    """Combined stdout+stderr of fetch + reset (truncated to
    ~4 KB for sanity)."""
    checkout_rc: int | None = None

    @property
    def success(self) -> bool:
        return (
            self.fetch_rc == 0
            and self.reset_rc == 0
            and self.checkout_rc == 0
            and self.new_sha is not None
        )


def reset_branch_env(
    env: str, *, cfg: config.Config,
) -> ResetBranchResult:
    """Reset one non-serving environment under update/lifecycle ownership."""
    with admin_update_ownership():
        prog = _resolve_venv_program(env, cfg)
        with toolset_lifecycle_lock([prog], action="vq-admin-reset-branch"):
            probe = _detect_vq_self_update(prog)
            if probe.is_self_update or not probe.manager_available:
                raise AdminError(
                    "reset-branch cannot authoritatively prove this environment "
                    "is different from the serving vq daemon; use the exact "
                    "self-update lifecycle or restore the service-manager "
                    "provenance first"
                )
            host = "localhost"
            multi_user = (
                cfg.multi_user.enabled or config.system_multi_user_enabled()
            )
            _guard_admin_update_marker(force=False, envs=[env], host=host)
            pause_token = _new_pause_token()
            pause_queue_root = _admin_pause_queue_root(
                multi_user=multi_user,
            )
            marker_acquired = False
            result: ResetBranchResult | None = None
            primary_error: BaseException | None = None
            try:
                acquire_admin_update_marker(envs=[env], host=host)
                marker_acquired = True
                _record_admin_update_pause_scope(
                    pause_token=pause_token,
                    paused_jobids=[],
                    surgical=False,
                    multi_user=multi_user,
                )
                pause_proof = pause_token_scope_with_proof(
                    host,
                    pause_token,
                    branches=prog.provides_branches or None,
                    multi_user=multi_user,
                    queue_root=pause_queue_root,
                )
                pause_proof.require_quiescent()
                transition_admin_update_state(ADMIN_UPDATE_STATE_PAUSED)
                transition_admin_update_state(ADMIN_UPDATE_STATE_PULLING)
                result = _reset_branch_env_owned(env, prog=prog)
            except BaseException as exc:
                primary_error = exc
            finally:
                resume_error: BaseException | None = None
                if marker_acquired:
                    with contextlib.suppress(BaseException):
                        transition_admin_update_state(ADMIN_UPDATE_STATE_RESUMING)
                    try:
                        resume_proof = resume_token_scope_with_proof(
                            host,
                            pause_token,
                            multi_user=multi_user,
                            queue_root=pause_queue_root,
                        )
                        resume_proof.require_clear()
                        _disarm_proven_pause_scope_without_managed_receipt()
                    except BaseException as exc:
                        resume_error = exc
                if resume_error is not None:
                    primary_error = resume_error
            if primary_error is not None:
                if marker_acquired:
                    with contextlib.suppress(BaseException):
                        transition_admin_update_state(
                            ADMIN_UPDATE_STATE_FAILED,
                            failure_reason=f"reset-branch failed: {primary_error}",
                        )
                raise primary_error
            assert result is not None
            if result.success:
                transition_admin_update_state(ADMIN_UPDATE_STATE_VERIFYING)
                _clear_completed_admin_update_marker()
            else:
                transition_admin_update_state(
                    ADMIN_UPDATE_STATE_FAILED,
                    failure_reason=(
                        f"reset-branch incomplete: fetch={result.fetch_rc}, "
                        f"reset={result.reset_rc}, checkout={result.checkout_rc}"
                    ),
                )
            return result


def _reset_branch_env_owned(
    env: str,
    *,
    prog: config.VenvProgram,
) -> ResetBranchResult:
    """v0.7.9 *Liskov's Substitution*: align ``env``'s working tree
    with the canonical ``origin/<configured-branch>``.

    Useful when v0.7.1's post-update branch validation surfaces a
    drifted env and the operator wants to snap back without
    spelunking through git by hand. Sequence:

    1. ``git -C <git_dir> fetch origin`` — refresh the remote ref.
    2. ``git -C <git_dir> reset --hard origin/<branch>`` — align
       HEAD + working tree.
    3. Capture the post-reset SHA + branch.
    4. Persist enough into ``last_*`` admin-status fields so the
       next ``vq admin status`` shows the change explicitly
       (last_sha + last_branch_actual). ``last_success`` is
       conservatively NOT flipped to True by reset-branch alone —
       a reset-branch fixes the *branch* but doesn't itself prove
       the build is healthy. Operator follows with
       ``vq admin update`` (or ``vq admin mark-ok`` if they've
       independently verified) to flip ``last_success``.

    The reset is destructive: any uncommitted local changes are
    discarded. This is exactly the intent (operators run
    ``reset-branch`` to throw away stray hand-edits); the CLI
    verb prints a clear "discarded N modified files" hint when
    that happens.

    Raises ``AdminError`` for unknown env (mirrors
    ``mark_env_ok``'s validation) or env without a configured
    branch (the reset target is undefined; the operator wants
    ``vq admin update`` instead, or to add a ``branch =`` line
    to the env's config).
    """
    if not prog.branch:
        raise AdminError(
            f"reset-branch requires {env!r} to have a configured "
            f"``branch =`` in [programs.venv.{env}]. Without a "
            f"branch the reset target is undefined; add the "
            f"config line (e.g. branch = 'main') or use "
            f"``vq admin update {env}`` instead."
        )
    git_dir = Path(prog.git_dir)
    branch = prog.branch

    # Step 1: capture prior SHA so the operator can see the jump.
    prior_sha = _query_git_sha(git_dir)

    # Step 2: fetch.
    chunks: list[str] = []
    fetch_rc, fetch_output = _run_git_fetch_origin(git_dir)
    chunks.append("$ git fetch origin\n" + fetch_output)

    # Step 3: reset --hard, only if fetch succeeded. A failed
    # fetch could mean we'd reset to a stale local ref, which is
    # worse than just bailing out — operator can re-try.
    if fetch_rc == 0:
        try:
            reset_proc = _mutating_git_run(
                [
                    "git", "-C", str(git_dir),
                    "reset", "--hard", f"origin/{branch}",
                ],
                capture_output=True,
                text=True,
                timeout=60,
            )
            reset_rc: int | None = reset_proc.returncode
            chunks.append(
                f"$ git reset --hard origin/{branch}\n"
                + (reset_proc.stdout or "")
                + (reset_proc.stderr or "")
            )
        except (subprocess.TimeoutExpired, OSError) as e:
            reset_rc = None
            chunks.append(
                f"$ git reset --hard origin/{branch}\n"
                f"(failed to start: {e})\n"
            )
    else:
        reset_rc = None
        chunks.append(
            f"$ git reset --hard origin/{branch}\n"
            "(skipped; fetch did not succeed)\n"
        )

    if reset_rc == 0:
        checkout_rc, checkout_output = _run_git_checkout_branch(
            git_dir, branch, force_to_origin=True,
        )
        chunks.append(checkout_output)
    else:
        checkout_rc = None
        chunks.append(
            f"$ git checkout -B {branch} origin/{branch}\n"
            "(skipped; reset did not succeed)\n"
        )

    new_sha = _query_git_sha(git_dir) if checkout_rc == 0 else None
    output = "\n".join(chunks)
    # Cap at ~4 KB so the persisted output doesn't bloat the
    # admin-status file. Same shape as v0.7.1 Item 2's
    # update_script_output tail.
    if len(output) > 4096:
        output = output[-4096:]

    # Update admin status so `vq admin status` shows the new SHA
    # + branch without having to wait for the next real update.
    # Conservative: do NOT touch last_success — a reset-branch
    # fixes the branch but doesn't prove the build is healthy.
    # The next `vq admin update` (or operator-explicit
    # `vq admin mark-ok`) flips last_success.
    records = read_admin_status()
    prior = records.get(env)
    records[env] = AdminUpdateRecord(
        # Preserve last_updated_at + last_success as-is so the
        # operator-visible "last successful update" timestamp
        # stays anchored to the real update event. Only the
        # SHA + branch_actual snapshot gets the reset.
        last_updated_at=prior.last_updated_at if prior else utcnow_iso(),
        last_success=prior.last_success if prior else False,
        last_sha=new_sha if new_sha is not None else (prior.last_sha if prior else None),
        last_tag=prior.last_tag if prior else None,
        last_expected_tag=prior.last_expected_tag if prior else None,
        last_expected_sha=prior.last_expected_sha if prior else None,
        last_git_pull_rc=prior.last_git_pull_rc if prior else None,
        last_update_script_rc=(
            prior.last_update_script_rc if prior else None
        ),
        last_branch_expected=branch,
        last_branch_actual=branch if new_sha is not None else (
            prior.last_branch_actual if prior else None
        ),
        # A reset-branch always produces a clean tree by definition
        # (``git reset --hard`` discards uncommitted changes).
        last_dirty_after_update=False if new_sha is not None else (
            prior.last_dirty_after_update if prior else None
        ),
        last_update_script_output=(
            prior.last_update_script_output if prior else None
        ),
        last_marked_ok_at=prior.last_marked_ok_at if prior else None,
        last_marked_ok_note=prior.last_marked_ok_note if prior else None,
    )
    write_admin_status(records)

    return ResetBranchResult(
        env=env,
        branch=branch,
        prior_sha=prior_sha,
        new_sha=new_sha,
        fetch_rc=fetch_rc,
        reset_rc=reset_rc,
        checkout_rc=checkout_rc,
        output=output,
    )


def _query_git_sha(git_dir: Path) -> str | None:
    """Run ``git -C <dir> rev-parse --short=12 HEAD``. Returns the SHA
    string or None on any failure (no git, dir gone, detached state
    that can't resolve, etc.). Called from inside ``record_update_outcome``
    which is best-effort + must never raise.

    12 hex chars matches what ``git log --oneline`` uses by default and
    is enough to be unambiguous in practice (collision probability is
    O(N^2 / 16^12)). Roomy enough for the foreseeable future of a
    research codebase."""
    try:
        proc = subprocess.run(
            ["git", "-C", str(git_dir), "rev-parse", "--short=12", "HEAD"],
            capture_output=True,
            text=True,
            timeout=10,
            stdin=subprocess.DEVNULL,
        )
    except Exception:  # noqa: BLE001 -- best-effort; status query must
                       # never raise (called from finally-block-equivalent).
        return None
    if proc.returncode != 0:
        return None
    return proc.stdout.strip() or None


def _query_git_describe(git_dir: Path) -> str | None:
    """Run ``git -C <dir> describe --tags --always``. Returns the tag
    name if HEAD is exactly at a tag, otherwise something like
    "v0.7.3-12-gabc1234" (last tag + ahead count + short SHA). None on
    failure. Best-effort: catches every exception."""
    try:
        proc = subprocess.run(
            ["git", "-C", str(git_dir), "describe",
             "--tags", "--always"],
            capture_output=True,
            text=True,
            timeout=10,
            stdin=subprocess.DEVNULL,
        )
    except Exception:  # noqa: BLE001 -- best-effort
        return None
    if proc.returncode != 0:
        return None
    return proc.stdout.strip() or None


def _query_pyproject_version(git_dir: Path) -> str | None:
    """v0.7.2 *Engelbart's Demo*: read ``[project] version`` from
    ``<git_dir>/pyproject.toml``. Returns the version string on
    success, ``None`` on any failure (missing file, malformed TOML,
    no version field, version not a string).

    Why: ``git describe --tags --always`` walks back to the nearest
    annotated tag, which lies when the codebase has moved several
    minor versions since the last *annotated* tag the helper can
    see. Concrete case 2026-05-25: vibe-qc main is at
    ``0.9.2.dev0`` (per ``pyproject.toml``), but ``git describe``
    returns ``v0.7.5-983-g11b4f7af`` because v0.7.5 is the most
    recent annotated tag git finds on this lineage — the v0.8.x and
    v0.9.x release tags are either lightweight or off-lineage. The
    pyproject ``version`` field is the canonical source of truth
    that vq should surface, not the misleading describe output.

    Tolerates everything missing — best-effort, never raises."""
    pyproject = git_dir / "pyproject.toml"
    if not pyproject.is_file():
        return None
    try:
        import tomllib
        with pyproject.open("rb") as f:
            data = tomllib.load(f)
    except (OSError, tomllib.TOMLDecodeError):
        return None
    project = data.get("project")
    if not isinstance(project, dict):
        return None
    version = project.get("version")
    if not isinstance(version, str):
        return None
    return version


def _query_git_dirty(git_dir: Path) -> bool | None:
    """Run ``git -C <dir> status --porcelain`` and return True iff there
    are uncommitted changes. None on failure to query.
    Best-effort: catches every exception."""
    try:
        proc = subprocess.run(
            ["git", "-C", str(git_dir), "status", "--porcelain"],
            capture_output=True,
            text=True,
            timeout=10,
            stdin=subprocess.DEVNULL,
            env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"},
        )
    except Exception:  # noqa: BLE001 -- best-effort
        return None
    if proc.returncode != 0:
        return None
    return bool(proc.stdout.strip())


@dataclass
class EnvStatus:
    """Live snapshot of one venv env for ``vq admin status``."""

    name: str
    git_dir: str
    branch: str | None  # from config
    current_sha: str | None
    current_describe: str | None
    is_dirty: bool | None
    last_record: AdminUpdateRecord | None
    error: str | None = None  # set when git_dir is unavailable etc.
    # v0.7.2 *Engelbart's Demo*: the project's ``[project] version``
    # from ``<git_dir>/pyproject.toml``. Source of truth for "what
    # version of vibe-qc is on the machine" — the ``current_describe``
    # field above is preserved for back-compat but is misleading when
    # the most recent annotated git tag is stale (the v0.7.5 vs
    # 0.9.2.dev0 case on 2026-05-25).
    current_version: str | None = None


def query_env_status(name: str, prog: config.VenvProgram) -> EnvStatus:
    """Live-query one venv env's git state, cross-reference with the
    persisted admin-update record."""
    records = read_admin_status()
    last = records.get(name)
    git_dir = Path(prog.git_dir)
    if not git_dir.is_dir():
        return EnvStatus(
            name=name,
            git_dir=str(git_dir),
            branch=prog.branch,
            current_sha=None,
            current_describe=None,
            is_dirty=None,
            last_record=last,
            error="git_dir not a directory",
        )
    if not (git_dir / ".git").exists():
        return EnvStatus(
            name=name,
            git_dir=str(git_dir),
            branch=prog.branch,
            current_sha=None,
            current_describe=None,
            is_dirty=None,
            last_record=last,
            error="not a git checkout",
        )
    return EnvStatus(
        name=name,
        git_dir=str(git_dir),
        branch=prog.branch,
        current_sha=_query_git_sha(git_dir),
        current_describe=_query_git_describe(git_dir),
        current_version=_query_pyproject_version(git_dir),
        is_dirty=_query_git_dirty(git_dir),
        last_record=last,
    )


def _format_marker_banner() -> str:
    """v0.5.44 / v0.6.0: build the warning banner shown by
    :func:`format_admin_status` when an admin-update-in-progress
    marker is on disk. Empty string when no marker — caller
    concatenates unconditionally. ASCII-only (project style).

    v0.6.0: includes the state machine's current phase + how long
    we've been stuck there (so an operator running `vq admin status`
    sees "stuck at PULLING for 47m" rather than just "in progress").
    """
    if not admin_update_marker_exists():
        return ""
    marker = read_admin_update_marker()
    if marker is None:
        body = "  (marker file present but unreadable — JSON parse failed)"
        diag = diagnose_admin_update_marker(None)
    else:
        diag = diagnose_admin_update_marker(marker)
        body_lines = [
            f"  marker_status: {diag.marker_status}",
            f"  state:       {marker.state}",
            f"  envs:        {', '.join(marker.envs)}",
            f"  host:        {marker.host}",
            f"  started_at:  {marker.started_at}",
        ]
        if marker.phase_started_at:
            body_lines.append(
                f"  phase_started: {marker.phase_started_at}"
            )
        if marker.last_heartbeat_at:
            body_lines.append(
                f"  last_heartbeat: {marker.last_heartbeat_at}"
            )
        body_lines.extend([
            f"  pid:         {marker.pid}",
            f"  vq_version:  {marker.vq_version}",
        ])
        if diag.pid_status:
            body_lines.append(f"  pid_status:  {diag.pid_status}")
        if diag.heartbeat_status:
            body_lines.append(f"  heartbeat:   {diag.heartbeat_status}")
        if diag.stale_reason and marker.state != ADMIN_UPDATE_STATE_FAILED:
            body_lines.append(f"  stale:       {diag.stale_reason}")
        if marker.failure_reason:
            body_lines.append(
                f"  failure:     {marker.failure_reason}"
            )
        body = "\n".join(body_lines)
    return (
        "!! admin-update-in-progress marker present !!\n"
        f"{body}\n"
        f"   {diag.summary}.\n"
        f"   {diag.action}\n\n"
    )


def _marker_entry_to_json_block(
    marker: AdminUpdateMarker | None,
) -> dict[str, object]:
    """Render one marker lease for machine-readable status."""
    if marker is None:
        diag = diagnose_admin_update_marker(None)
        return {
            "readable": False,
            "marker_status": diag.marker_status,
            "summary": diag.summary,
            "action": diag.action,
        }
    diag = diagnose_admin_update_marker(marker)
    return {
        **asdict(marker),
        "readable": True,
        "marker_status": diag.marker_status,
        "summary": diag.summary,
        "action": diag.action,
        "pid_status": diag.pid_status,
        "stale_reason": diag.stale_reason,
        "heartbeat_status": diag.heartbeat_status,
        "heartbeat_age_seconds": diag.heartbeat_age_seconds,
    }


def _markers_to_json_blocks() -> list[dict[str, object]]:
    return [
        _marker_entry_to_json_block(marker)
        for _, marker in _admin_update_marker_entries()
    ]


def _marker_to_json_block() -> dict[str, object] | None:
    """Compatibility view of the first scope-aware marker lease.

    Returns:
      * ``None`` when no marker file exists.
      * A dict with the parsed marker fields plus ``"readable": True``
        when the file parses cleanly.
      * A dict with ``{"readable": False}`` (no other fields) when the
        file is present but unparseable. The ``readable`` field lets
        machine consumers distinguish "no marker" (None) from
        "marker exists but corrupt" (readable=False) without ambiguity.
    """
    blocks = _markers_to_json_blocks()
    return blocks[0] if blocks else None



def _installed_matches(
    rec: object | None, st: object
) -> bool | None:
    """Whether the installed commit and the checkout on disk agree.

    None when either side is unknown -- an env that has never been updated by
    vq, or a checkout whose SHA could not be read. Absence of evidence is not
    reported as agreement, because the whole point is to stop a silent
    disagreement looking healthy.
    """
    installed = getattr(rec, "last_installed_sha", None) if rec is not None else None
    current = getattr(st, "current_sha", None)
    if not installed or not current:
        return None
    # current_sha is short-form in some paths; compare on the common prefix.
    # The update gate reads the same evidence the same way (#44).
    return _shas_agree(installed, current)


def admin_update_in_flight() -> bool:
    """Is an admin operation running on this host right now?

    True only for a marker whose writer is demonstrably alive. A failed or
    stale marker is emphatically *not* in flight: it is something to
    acknowledge, and conflating the two is what sends a caller to ``ps``.
    """
    return any(
        diagnose_admin_update_marker(marker).marker_status
        == ADMIN_UPDATE_MARKER_DIAG_RUNNING
        for _path, marker in _admin_update_marker_entries()
    )


def _last_outcome_fields(envs_out: list[dict[str, object]]) -> dict[str, object]:
    """Classify the most recently *recorded* admin update on this host.

    ``last_outcome`` is one of :data:`ADMIN_OUTCOMES`, or None when nothing
    has been recorded. It is a record, with everything that implies -- it says
    how the last operation ended, not whether the environment is healthy now.
    compute-b's ``LAST OK True`` beside a venv that could not import is the same
    field telling the same true-but-insufficient story, so a caller that needs
    the present tense reads ``vq programs`` (which probes) rather than this.

    ``already-current`` never appears here: a no-op records nothing, because
    nothing happened.
    """
    latest_at: str | None = None
    latest_success: object = None
    for env in envs_out:
        at = env.get("last_updated_at")
        if not isinstance(at, str):
            continue
        if latest_at is None or at > latest_at:
            latest_at = at
            latest_success = env.get("last_success")
    if latest_at is None:
        return {"last_outcome": None, "last_outcome_at": None}
    outcome = (
        OUTCOME_OK
        if latest_success is True
        else OUTCOME_FAILED
        if latest_success is False
        else None
    )
    return {"last_outcome": outcome, "last_outcome_at": latest_at}


def format_admin_status_json(cfg: config.Config) -> str:
    """v0.5.46: JSON-serialised view of :func:`format_admin_status`.

    Output shape::

        {
          "marker": {<AdminUpdateMarker fields>, "readable": true} | null,
          "envs":   [{<EnvStatus + AdminUpdateRecord fields>}, ...]
        }

    The per-env dict flattens the live ``EnvStatus`` (name, git_dir,
    branch, current_sha, current_describe, is_dirty, error) and the
    persisted ``AdminUpdateRecord`` (last_* fields) into one record per
    env, mirroring what the text formatter does row-by-row. Missing
    fields are explicitly serialised as ``null`` rather than omitted —
    a stable schema is more useful to scripts than a compact one.

    Use this when you want to parse `vq admin status` output rather
    than scrape the text. Pretty-printed (indent=2, sorted keys) so
    a human reading the JSON still gets a readable result."""
    envs_out: list[dict[str, object]] = []
    for name, prog in sorted(cfg.programs.items()):
        if not isinstance(prog, config.VenvProgram):
            continue
        st = query_env_status(name, prog)
        rec = st.last_record
        envs_out.append({
            "name": st.name,
            "git_dir": st.git_dir,
            "branch": st.branch,
            "current_sha": st.current_sha,
            "current_describe": st.current_describe,
            # v0.7.2 *Engelbart's Demo*: canonical semver from the
            # project's ``[project] version`` in pyproject.toml.
            # Null when no pyproject / unreadable / no version
            # field (the case where the text formatter's VERSION
            # column falls back to current_describe).
            "current_version": st.current_version,
            "is_dirty": st.is_dirty,
            "error": st.error,
            "last_updated_at":
                rec.last_updated_at if rec is not None else None,
            "last_success":
                rec.last_success if rec is not None else None,
            "last_sha": rec.last_sha if rec is not None else None,
            "last_installed_sha":
                rec.last_installed_sha if rec is not None else None,
            # True when the venv's installed code and the checkout on disk
            # are different commits -- the shape a rolled-back build leaves,
            # where `import vibeqc` serves one version and every
            # version-reporting surface describes another. None when either
            # side is unknown, since that is not evidence of agreement.
            "installed_sha_matches_checkout": _installed_matches(rec, st),
            "last_tag": rec.last_tag if rec is not None else None,
            "last_expected_tag":
                rec.last_expected_tag if rec is not None else None,
            "last_expected_sha":
                rec.last_expected_sha if rec is not None else None,
            "last_git_pull_rc":
                rec.last_git_pull_rc if rec is not None else None,
            "last_update_script_rc":
                rec.last_update_script_rc if rec is not None else None,
            # v0.7.1: branch verification snapshot — always present in
            # JSON output (null when verification was skipped) so
            # machine consumers get a stable schema.
            "last_branch_expected":
                rec.last_branch_expected if rec is not None else None,
            "last_branch_actual":
                rec.last_branch_actual if rec is not None else None,
            # v0.7.1 Item 2: persisted update_script output tail.
            # ``None`` when no script ran or output was empty;
            # otherwise the last N lines (default 80, env override
            # VQ_ADMIN_UPDATE_OUTPUT_LINES). Always present in JSON
            # for stable machine schema.
            "last_update_script_output":
                rec.last_update_script_output if rec is not None else None,
            # v0.7.1 Item 4: mark-ok audit fields. Both null when the
            # last_success came from a real update; both set when an
            # operator ran `vq admin mark-ok`. Always present in JSON
            # so consumers can distinguish operator-ack from real OK.
            "last_marked_ok_at":
                rec.last_marked_ok_at if rec is not None else None,
            "last_marked_ok_note":
                rec.last_marked_ok_note if rec is not None else None,
            # v0.7.1 Item 5: dirty-tree-after-update snapshot.
            # True = dirty, False = clean, null = unknown / not run.
            # Independent of LAST OK (operator wants the dirty signal
            # even when the update otherwise succeeded).
            "last_dirty_after_update":
                rec.last_dirty_after_update if rec is not None else None,
            "last_failure_reason":
                rec.last_failure_reason if rec is not None else None,
            "last_daemon_restart_attempted":
                rec.last_daemon_restart_attempted if rec is not None else None,
            "last_daemon_restart_succeeded":
                rec.last_daemon_restart_succeeded if rec is not None else None,
            "last_daemon_health_verified":
                rec.last_daemon_health_verified if rec is not None else None,
            "last_daemon_expected_source_sha":
                (
                    rec.last_daemon_expected_source_sha
                    if rec is not None else None
                ),
            "last_daemon_actual_source_sha":
                (
                    rec.last_daemon_actual_source_sha
                    if rec is not None else None
                ),
            "last_daemon_expected_source_tree_sha256":
                (
                    rec.last_daemon_expected_source_tree_sha256
                    if rec is not None else None
                ),
            "last_daemon_actual_source_tree_sha256":
                (
                    rec.last_daemon_actual_source_tree_sha256
                    if rec is not None else None
                ),
            "last_daemon_restart_message":
                rec.last_daemon_restart_message if rec is not None else None,
        })
    payload = {
        "marker": _marker_to_json_block(),
        "markers": _markers_to_json_blocks(),
        "envs": envs_out,
        # v0.26.1: the two questions a caller sequencing work actually has,
        # answered at the top level instead of inferred across nested blocks.
        # Their absence is why an orchestration ended up shelling out to
        # `ps -eo command | grep -c "[n]inja"` to decide whether a build had
        # finished -- and that loop, whose `grep -c` exits 1 on a zero count,
        # spun for six hours after the build was already done.
        "in_flight": admin_update_in_flight(),
        **_last_outcome_fields(envs_out),
    }
    return json.dumps(payload, indent=2, sort_keys=True)


def format_admin_status(
    cfg: config.Config, *, verbose: bool = False,
) -> str:
    """Build the ``vq admin status`` table.

    Columns: NAME / BRANCH / CURRENT SHA / DESCRIBE / DIRTY /
    LAST_UPDATED_AT / LAST OK. Only ``kind = "venv"`` programs appear
    (binary/import aren't git-backed).

    v0.5.44: prepends a marker banner if an
    admin-update-in-progress marker is on disk.

    v0.7.1 *Lamport's Clock* Item 2: ``verbose=True`` appends, after
    the main table, a per-failing-env block with the persisted
    ``last_update_script_output`` tail. Only emitted for rows where
    ``last_success=False`` AND we actually captured output — keeps
    the default rendering compact for the happy path while putting
    the failure detail one ``--verbose`` away.

    For machine consumption, see :func:`format_admin_status_json`."""
    banner = _format_marker_banner()
    venv_progs = [
        (name, prog) for name, prog in sorted(cfg.programs.items())
        if isinstance(prog, config.VenvProgram)
    ]
    if not venv_progs:
        return banner + (
            "no venv programs registered. Add [programs.X] sections "
            "with kind=\"venv\" to track them via `vq admin status`."
        )
    # v0.7.2 *Engelbart's Demo*: column header is VERSION (was
    # DESCRIBE pre-v0.7.2). The cell prefers ``pyproject.toml``'s
    # ``[project] version`` (canonical semver of the checked-out
    # codebase) and falls back to ``git describe`` only when no
    # pyproject.toml is found / parseable. Pre-v0.7.2 the column
    # always showed ``git describe`` — which was misleading on
    # codebases where the most recent annotated tag was many minor
    # versions old (vibe-qc on 2026-05-25: 0.9.2.dev0 displayed as
    # ``v0.7.5-983-g11b4f7af``).
    rows: list[tuple[str, str, str, str, str, str, str]] = [
        ("NAME", "BRANCH", "SHA", "VERSION", "DIRTY",
         "LAST_UPDATED_AT", "LAST OK"),
    ]
    for name, prog in venv_progs:
        st = query_env_status(name, prog)
        if st.error:
            rows.append((
                name, prog.branch or "-", "-", "-", "-",
                "-", f"ERROR: {st.error}",
            ))
            continue
        last_at = (
            st.last_record.last_updated_at if st.last_record else "never"
        )
        last_ok_str = (
            str(st.last_record.last_success) if st.last_record else "-"
        )
        # v0.7.1 Item 4: append ``*`` when LAST OK came from a
        # mark-ok rather than a real update. The asterisk is a
        # visual flag — the operator should reach for --verbose
        # to see the audit note.
        if (
            st.last_record is not None
            and st.last_record.last_marked_ok_at is not None
        ):
            last_ok_str = last_ok_str + "*"
        last_ok = last_ok_str
        # v0.7.1 *Lamport's Clock*: when the last update detected a
        # branch drift, render the BRANCH column as
        # ``<config> → <actual>`` so the operator sees the drift
        # without parsing JSON. Pre-v0.7.1 records have
        # ``last_branch_actual=None`` and fall through to the
        # plain config-branch rendering.
        config_branch = st.branch or "-"
        actual_branch = (
            st.last_record.last_branch_actual
            if st.last_record is not None
            else None
        )
        if (
            actual_branch is not None
            and st.branch is not None
            and actual_branch != st.branch
        ):
            branch_cell = f"{config_branch} -> {actual_branch}"
        else:
            branch_cell = config_branch
        # v0.7.2: VERSION cell prefers pyproject; falls back to
        # git describe if pyproject is unreadable / missing /
        # lacks a version field.
        version_cell = st.current_version or st.current_describe or "-"
        rows.append((
            name,
            branch_cell,
            st.current_sha or "-",
            version_cell,
            "yes" if st.is_dirty else ("no" if st.is_dirty is False else "-"),
            last_at,
            last_ok,
        ))
    widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
    lines = [
        "  ".join(col.ljust(w) for col, w in zip(row, widths, strict=True))
        for row in rows
    ]
    body = banner + "\n".join(lines)
    if not verbose:
        return body
    # v0.7.1 Item 2: append per-failing-env captured output tails.
    # Each block is delimited and prefixed with the env name so
    # multi-env failures stay disambiguated.
    # v0.7.1 Item 4: also append mark-ok audit notes (any env,
    # success or fail) so the operator sees WHY a row carries
    # the LAST OK=True* asterisk.
    tail_blocks: list[str] = []
    for name, prog in venv_progs:
        st = query_env_status(name, prog)
        rec = st.last_record
        if rec is None:
            continue
        # Failure tail (Item 2)
        if not rec.last_success and rec.last_failure_reason:
            tail_blocks.append(
                f"\n\n-- {name}: failure reason --\n"
                f"   {rec.last_failure_reason}"
            )
        if (
            not rec.last_success
            and rec.last_daemon_restart_message
            and rec.last_daemon_restart_message
            not in (rec.last_failure_reason or "")
        ):
            tail_blocks.append(
                f"\n\n-- {name}: daemon restart detail --\n"
                f"   {rec.last_daemon_restart_message}"
            )
        if not rec.last_success and rec.last_update_script_output:
            tail_blocks.append(
                f"\n\n-- {name}: update_script output tail "
                f"(rc={rec.last_update_script_rc}) --\n"
                f"{rec.last_update_script_output.rstrip()}"
            )
        # Mark-ok audit note (Item 4)
        if rec.last_marked_ok_at is not None:
            tail_blocks.append(
                f"\n\n-- {name}: marked OK by operator "
                f"({rec.last_marked_ok_at}) --\n"
                f"   note: {rec.last_marked_ok_note}"
            )
    return body + "".join(tail_blocks)
