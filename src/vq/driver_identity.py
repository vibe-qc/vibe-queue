"""An opaque, stable identity for the vq state that submits scheduler jobs.

Several drivers can stage into one cluster jobs root. Each new scheduler
workspace records this identity in ``_vq/driver-id`` so orphan cleanup can
tell this driver's abandoned workspaces from another driver's live ones
(#91). The value is 32 random hex characters generated once per state root:
it names no host, user or path.
"""
from __future__ import annotations

import os
import re
import secrets

from vq import paths

DRIVER_ID_BASENAME = "driver-id"
"""File name both in the state root and in a workspace's ``_vq/``."""

_DRIVER_ID = re.compile(r"[0-9a-f]{32}")


def is_driver_id(value: str) -> bool:
    return _DRIVER_ID.fullmatch(value) is not None


def driver_id() -> str:
    """Return this state root's identity, creating it on first use."""
    path = paths.state_root() / DRIVER_ID_BASENAME
    try:
        existing = path.read_text().strip()
    except FileNotFoundError:
        existing = ""
    if is_driver_id(existing):
        return existing
    path.parent.mkdir(parents=True, exist_ok=True)
    candidate = secrets.token_hex(16)
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        # Another process created it first; its value is the identity.
        existing = path.read_text().strip()
        if is_driver_id(existing):
            return existing
        raise OSError(f"{path} does not hold a driver id") from None
    with os.fdopen(fd, "w") as stream:
        stream.write(candidate + "\n")
    return candidate
