"""The local dispatcher seam for a JobSpec launched as a child process.

SPEC §8.1 requires that ``subprocess.Popen`` semantics stay isolated in a
single dispatch component. This module provides that boundary for the daemon's
local-process path.

Over time that isolation eroded: the local-process mechanism
(``subprocess.Popen``, ``popen.poll()``, ``popen.terminate()``/``.kill()``)
spread across the daemon's dispatch, reconcile, and watchdog paths. This
module re-establishes the seam.

A :class:`Dispatcher` encapsulates the local launch and ``Popen`` lifecycle
behind an opaque, dispatcher-defined *handle*. :class:`LocalDispatcher` is the
original v0.1 mechanism: a local child process whose handle is its
:class:`subprocess.Popen`.

Scheduler-target jobs now use the separate
:class:`vq.scheduler_dispatch.SchedulerDispatcher` path. Its SSH staging,
batched polling, result-fetch, and cancellation contract does not implement
this local ``Dispatcher`` protocol.

Current scope: this module owns the platform-agnostic process
*lifecycle* primitives that can be unit-tested on any OS — launch, poll,
terminate, kill, wait. Command construction (the exit-marker shim and the
Linux-only cgroup / ``systemd-run`` privilege-drop wrap) and the
pgid-mediated kill / liveness / reattach paths remain in the daemon. Scheduler
submission and reconciliation live in ``scheduler_dispatch.py`` and the
daemon's parallel scheduler path.
"""

from __future__ import annotations

import contextlib
import os
import subprocess
from typing import IO, Protocol, TypeVar

HandleT = TypeVar("HandleT")


class DispatchError(RuntimeError):
    """A job could not be launched.

    Raised by :meth:`Dispatcher.launch`. The daemon catches it and lands the
    spec FAILED through its normal dispatch-failure bookkeeping; the
    dispatcher itself performs no spec I/O.
    """


class Dispatcher(Protocol[HandleT]):
    """The local job-execution mechanism (SPEC §8.1).

    ``HandleT`` is the dispatcher's opaque handle for a launched local job; it
    is a :class:`subprocess.Popen` for :class:`LocalDispatcher`.
    """

    def launch(
        self,
        *,
        run_command: list[str],
        cwd: os.PathLike[str] | str,
        env: dict[str, str],
        stdout_fh: IO[bytes],
        stderr_fh: IO[bytes],
    ) -> HandleT:
        """Start the job and return its handle.

        Raise :class:`DispatchError` if the job cannot be started.
        """
        ...

    def poll(self, handle: HandleT) -> int | None:
        """Return the job's exit code if it has finished, else ``None``."""
        ...

    def terminate(self, handle: HandleT) -> None:
        """Request graceful termination (SIGTERM-equivalent).

        A no-op if the job is already gone.
        """
        ...

    def kill(self, handle: HandleT) -> None:
        """Force termination (SIGKILL-equivalent).

        A no-op if the job is already gone.
        """
        ...

    def wait(self, handle: HandleT, timeout: float) -> bool:
        """Block up to ``timeout`` seconds for the job to exit.

        Return ``True`` if it exited, ``False`` if the timeout elapsed first.
        """
        ...


class LocalDispatcher:
    """Run jobs as local child processes — the original v0.1 mechanism.

    Implements :class:`Dispatcher` with a :class:`subprocess.Popen` handle.
    Each job is spawned in its own session (``start_new_session=True``) so it
    leads a fresh process group; the daemon records that pgid on the spec and
    reaps the whole group on kill. The pgid-mediated signalling stays in the
    daemon (see the module docstring) — this class owns only the
    in-memory ``Popen`` lifecycle.
    """

    def launch(
        self,
        *,
        run_command: list[str],
        cwd: os.PathLike[str] | str,
        env: dict[str, str],
        stdout_fh: IO[bytes],
        stderr_fh: IO[bytes],
    ) -> subprocess.Popen[bytes]:
        try:
            proc: subprocess.Popen[bytes] = subprocess.Popen(
                run_command,
                cwd=cwd,
                stdout=stdout_fh,
                stderr=stderr_fh,
                start_new_session=True,
                env=env,
                stdin=subprocess.DEVNULL,
            )
        except Exception as exc:
            raise DispatchError(f"failed to spawn local process: {exc}") from exc
        return proc

    def poll(self, handle: subprocess.Popen[bytes]) -> int | None:
        return handle.poll()

    def terminate(self, handle: subprocess.Popen[bytes]) -> None:
        with contextlib.suppress(ProcessLookupError):
            handle.terminate()

    def kill(self, handle: subprocess.Popen[bytes]) -> None:
        with contextlib.suppress(ProcessLookupError):
            handle.kill()

    def wait(self, handle: subprocess.Popen[bytes], timeout: float) -> bool:
        try:
            handle.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            return False
        return True
