"""Signal a process group without mistaking its teardown for another owner's.

On macOS a process group whose last member is exiting, or is a zombie waiting
to be reaped, answers ``killpg(2)`` with ``EPERM`` rather than ``ESRCH``. vq read
that ``EPERM`` as "another user's group", so ``vq kill`` reported "permission
denied" for a job that was simply exiting and pause and resume blamed another
user (#27).

``EPERM`` still genuinely means another owner's live group in multi-user mode,
so it cannot be mapped to "gone". The two differ over time. A dying group
settles to ``ESRCH`` within milliseconds: a probe on 2026-09-13 at load 86
measured a median of 3 ms and a maximum of 77 ms, and the notes on #35 saw up
to 0.35 s at higher load. Another owner's live group keeps answering ``EPERM``.
So on ``EPERM`` this module re-probes for a bounded interval before believing
it.

A resource probe cannot make this distinction: a dying member still holds its
files when ``EPERM`` begins.
"""
from __future__ import annotations

import errno
import os
import time

EXITING_GROUP_SETTLE_SECONDS = 1.0
"""How long an ``EPERM`` is re-probed before it is believed."""


def signal_process_group(
    pgid: int, sig: int, *, settle_seconds: float | None = None,
) -> None:
    """``os.killpg(pgid, sig)``, re-probing an ``EPERM`` before believing it.

    Raises :class:`ProcessLookupError` when the group is gone, including a
    group that was still tearing down when it was first signalled. Re-raises
    the original :class:`PermissionError` when ``EPERM`` persists for
    ``settle_seconds``: that group has a live member this process may not
    signal. If the group answers normally while being re-probed, ``sig`` is
    sent again and its result stands.
    """
    try:
        os.killpg(pgid, sig)
        return
    except PermissionError as exc:
        denied = exc
    budget = (
        EXITING_GROUP_SETTLE_SECONDS if settle_seconds is None else settle_seconds
    )
    deadline = time.monotonic() + budget
    delay = 0.002
    while True:
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            raise ProcessLookupError(
                errno.ESRCH, f"process group {pgid} finished exiting",
            ) from None
        except PermissionError:
            pass
        else:
            os.killpg(pgid, sig)
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise denied
        time.sleep(min(delay, remaining))
        delay = min(delay * 2, 0.05)
