"""List a directory through a held descriptor as it is now (#139).

On btrfs (seen on Linux 7.1), readdir through one open file description
stops at the last entry that existed when the description was opened.
``os.listdir(fd)`` reads through a duplicate of ``fd``, which shares that
description. So a directory opened while empty and then populated lists as
empty through its own descriptor, however many entries it has. tmpfs, ext4
and APFS list the current entries.

Opening ``"."`` relative to the held descriptor creates a new description of
the same inode without any pathname lookup outside it, so the inode binding
the callers rely on is kept.
"""

from __future__ import annotations

import os


def listdir(fd: int) -> list[str]:
    """Return the names in the directory ``fd`` refers to, as of now."""
    try:
        fresh = os.open(
            ".",
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_CLOEXEC", 0),
            dir_fd=fd,
        )
    except FileNotFoundError:
        # A removed directory has no "." to open; it lists as empty anyway.
        return os.listdir(fd)
    try:
        held = os.fstat(fd)
        opened = os.fstat(fresh)
        if (held.st_dev, held.st_ino) != (opened.st_dev, opened.st_ino):
            raise OSError(f"directory descriptor {fd} did not reopen as itself")
        return os.listdir(fresh)
    finally:
        os.close(fresh)
