"""Shared read-side access transactions for persisted job specs."""

from __future__ import annotations

import contextlib
import os
import stat
from pathlib import Path

from vq import config, ownership, paths
from vq.spec import JobSpec, utcnow_iso, validate_job_id

SPEC_READ_MAX_BYTES = 8 * 1024 * 1024
"""Maximum size of a spec accepted by a post-refresh read."""


def read_bounded_regular_spec(path: Path) -> JobSpec:
    """Read one exact file descriptor without following or blocking on it."""
    entry_before = os.lstat(path)
    if not stat.S_ISREG(entry_before.st_mode):
        raise ValueError(f"spec {path} is not a regular file")
    flags = os.O_RDONLY
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    fd = os.open(path, flags)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError(f"spec {path} is not a regular file")
        if (entry_before.st_dev, entry_before.st_ino) != (
            before.st_dev,
            before.st_ino,
        ):
            raise ValueError(f"spec {path} changed before it could be opened")
        if before.st_size > SPEC_READ_MAX_BYTES:
            raise ValueError(
                f"spec {path} is {before.st_size} bytes; maximum is "
                f"{SPEC_READ_MAX_BYTES}"
            )

        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(fd, min(64 * 1024, SPEC_READ_MAX_BYTES + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > SPEC_READ_MAX_BYTES:
                raise ValueError(
                    f"spec {path} grew beyond {SPEC_READ_MAX_BYTES} bytes"
                )

        after = os.fstat(fd)
        before_identity = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        )
        after_identity = (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        )
        if before_identity != after_identity or total != before.st_size:
            raise ValueError(f"spec {path} changed while being read")
        entry_after = os.lstat(path)
        if not stat.S_ISREG(entry_after.st_mode) or (
            entry_after.st_dev,
            entry_after.st_ino,
        ) != (after.st_dev, after.st_ino):
            raise ValueError(f"spec {path} was replaced while being read")
        text = b"".join(chunks).decode("utf-8")
    finally:
        os.close(fd)
    return JobSpec.from_json(text)


def read_bounded_regular_spec_at(
    directory_fd: int,
    name: str,
    *,
    display_directory: Path,
) -> JobSpec:
    """Read a stable spec relative to an already-trusted directory fd."""
    path = display_directory / name
    entry_before = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    if not stat.S_ISREG(entry_before.st_mode):
        raise ValueError(f"spec {path} is not a regular file")
    flags = os.O_RDONLY
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    fd = os.open(name, flags, dir_fd=directory_fd)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError(f"spec {path} is not a regular file")
        if (entry_before.st_dev, entry_before.st_ino) != (
            before.st_dev,
            before.st_ino,
        ):
            raise ValueError(f"spec {path} changed before it could be opened")
        if before.st_size > SPEC_READ_MAX_BYTES:
            raise ValueError(
                f"spec {path} is {before.st_size} bytes; maximum is "
                f"{SPEC_READ_MAX_BYTES}"
            )
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(fd, min(64 * 1024, SPEC_READ_MAX_BYTES + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > SPEC_READ_MAX_BYTES:
                raise ValueError(
                    f"spec {path} grew beyond {SPEC_READ_MAX_BYTES} bytes"
                )
        after = os.fstat(fd)
        identity_before = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        )
        identity_after = (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        )
        if identity_before != identity_after or total != before.st_size:
            raise ValueError(f"spec {path} changed while being read")
        entry_after = os.stat(
            name,
            dir_fd=directory_fd,
            follow_symlinks=False,
        )
        if not stat.S_ISREG(entry_after.st_mode) or (
            entry_after.st_dev,
            entry_after.st_ino,
        ) != (after.st_dev, after.st_ino):
            raise ValueError(f"spec {path} was replaced while being read")
        text = b"".join(chunks).decode("utf-8")
    finally:
        os.close(fd)
    return JobSpec.from_json(text)


# Compatibility for the original private call site while new queue-authority
# scans use the public bounded/no-follow primitive.
_read_bounded_regular_spec = read_bounded_regular_spec


def reread_authorized_spec(
    jobid: str,
    *,
    expected_path: Path,
    multi_user: bool,
    queue_dir: Path | None = None,
) -> JobSpec:
    """Re-resolve and securely read the spec selected by an earlier read.

    This transaction is for reads performed after an RPC or polling boundary,
    where a path may have been replaced since the caller's initial authorized
    snapshot.  It re-resolves the job, requires the same path, opens the final
    component without following or blocking, validates a bounded regular-file
    snapshot, and authorizes that exact parsed snapshot.
    """
    jobid = validate_job_id(jobid)
    if multi_user:
        resolved = paths.resolve_spec_path(jobid, multi_user=True)
    else:
        resolved = (queue_dir or paths.queue_dir()) / f"{jobid}.json"
    if resolved != expected_path:
        raise ValueError(
            f"spec path changed during read: expected {expected_path}, got {resolved}"
        )
    spec = read_bounded_regular_spec(resolved)
    if spec.id != jobid:
        raise ValueError(
            f"spec at {resolved} has id {spec.id!r}; expected {jobid!r}"
        )
    ownership.check_owner(spec, multi_user=multi_user)
    return spec


def resolve_authorized_spec(
    jobid: str,
    *,
    multi_user: bool,
    queue_dir: Path | None = None,
) -> tuple[Path, JobSpec]:
    """Resolve, authorize, and load one job spec in the established order.

    Host locality remains the caller's responsibility.  Multi-user lookup
    ignores ``queue_dir`` and searches the per-user trees; single-user lookup
    honors an injected queue before the process default.  Keep the ownership
    path check before the final load, including its historical extra read when
    multi-user authorization is enabled.
    """
    jobid = validate_job_id(jobid)
    if multi_user:
        spec_path = paths.resolve_spec_path(jobid, multi_user=True)
    else:
        queue_dir = queue_dir or paths.queue_dir()
        spec_path = queue_dir / f"{jobid}.json"
        if not spec_path.exists():
            raise FileNotFoundError(f"no such job: {jobid}")
    ownership.check_spec_path_owner(spec_path, multi_user=multi_user)
    return spec_path, JobSpec.read(spec_path)


def stamp_terminal_status_read(
    spec_path: Path,
    *,
    jobid: str | None = None,
    multi_user: bool = False,
    queue_dir: Path | None = None,
) -> None:
    """Best-effort terminal read stamp without clobbering fresh fields.

    The caller decides eligibility from the spec snapshot it rendered.  Once
    called, re-read under the per-spec lock and re-check terminality so a
    concurrent fetch, cleanup, or state write survives intact.  Keep the
    timestamp generation inside the lock and suppress filesystem, validation,
    authorization-policy, and locking failures: the visible read is primary.
    """
    with contextlib.suppress(
        OSError,
        ValueError,
        config.ConfigError,
    ), paths.spec_lock(spec_path):
        if jobid is None:
            fresh = JobSpec.read(spec_path)
        else:
            fresh = reread_authorized_spec(
                jobid,
                expected_path=spec_path,
                multi_user=multi_user,
                queue_dir=queue_dir,
            )
        if fresh.is_terminal:
            fresh.last_status_at = utcnow_iso()
            fresh.write(spec_path)
