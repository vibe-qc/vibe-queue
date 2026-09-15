"""Config-free filesystem persistence primitives.

This private module must stay independent of vq's path policy and data models.
Public callers continue to use the compatibility exports from :mod:`vq.paths`.
"""
from __future__ import annotations

import contextlib
import os
import tempfile
from pathlib import Path


def atomic_write_text(path: Path, text: str) -> None:
    """Write ``text`` to ``path`` atomically *and* durably.

    **Atomic** — write to a *uniquely named* temp file in the same directory,
    ``fsync`` it, then ``os.replace`` it onto the target. A reader never sees a
    partial file (the rename is atomic), and — critically — two writers racing
    on the *same* path no longer collide: pre-v0.8.9 the temp name was a fixed
    ``<name>.tmp`` shared by every writer of that path, so a second writer's
    ``replace`` could land on a temp the first writer had already renamed away
    (``FileNotFoundError``) or clobber its in-flight bytes. The spec dir has
    many concurrent writers — the daemon loop, its in-process RPC thread, and
    every CLI verb (``vq kill`` / ``status`` / ``fetch`` / ``pause`` /
    ``cleanup``) — so the unique temp is a correctness fix, not a nicety.

    **Durable** — ``fsync`` the temp file's data before the rename and
    ``fsync`` the parent directory after, so a crash or power-loss cannot
    leave a zero-length or absent spec. Without the data ``fsync``, a
    rename-then-crash on a delayed-allocation filesystem (ext4 ``data=writeback``)
    can persist the directory entry while the data blocks are still unwritten,
    yielding a zero-length spec — which every reader then *silently skips*
    (``JobSpec.read`` raises, callers ``continue``), so the job vanishes from
    both scheduling and ``vq queue``. The spec dir is the source of truth for
    every job record; a lost write loses a job. (SPEC.md §5.3 already promised
    this ``fsync`` behavior; this brings the code in line with the contract.)

    Mode is preserved on overwrite (and defaults to 0644 on create) so this
    primitive changes durability/atomicity only, not file permissions.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    data = text.encode("utf-8")
    # Match the destination's mode on overwrite; mkstemp would otherwise force
    # 0600 and silently tighten existing files. New files default to 0644
    # (the umask-022 result) — we avoid the os.umask(0) probe because the
    # daemon's RPC thread makes that process-global toggle racy.
    try:
        dest_mode = os.stat(path).st_mode & 0o777
    except FileNotFoundError:
        dest_mode = 0o644
    tmp_fd, tmp_name = tempfile.mkstemp(
        dir=str(path.parent), prefix=path.name + ".", suffix=".tmp"
    )
    try:
        with os.fdopen(tmp_fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.chmod(tmp_name, dest_mode)
        os.replace(tmp_name, path)
    except BaseException:
        # Failed before/at the rename — don't leak the temp file.
        with contextlib.suppress(OSError):
            os.unlink(tmp_name)
        raise
    # Make the rename itself durable. Best-effort: some filesystems don't
    # support directory fsync, and a failure here doesn't risk a torn spec
    # (the data was already fsync'd onto the temp inode before the rename).
    with contextlib.suppress(OSError):
        dir_fd = os.open(str(path.parent), os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
