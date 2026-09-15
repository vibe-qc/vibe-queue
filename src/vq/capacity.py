"""Daemon capacity advertisement.

The daemon records its configured resource budget (``max_cpus`` /
``max_jobs`` / ``max_mem_mb``) to a small state file at startup and exposes
the same immutable snapshot over RPC.  ``vq status`` and ``vq overview`` use
the RPC view while the daemon is healthy, with the file as a daemon-down and
mixed-version fallback.

Mirrors the drain/throttle state-file pattern: a single-user daemon writes
``<state_root>/daemon_capacity.json`` and a multi-user daemon writes
``<multi_user_root>/daemon_capacity.json``. Readers parse it gracefully - a
missing or corrupt file reads as ``None``, so an old daemon that predates this
file (or a host whose daemon is down) simply reports *unknown* capacity rather
than raising.

The budget is static for a daemon run (set from ``--max-cpus`` /
``--max-jobs`` / ``--max-mem-mb`` at start), so one startup write suffices;
it is refreshed each time the daemon (re)starts. A stale file left by a
dead daemon is harmless: placement skips hosts whose ``daemon_health`` is
not live, so the orphaned budget is never consulted.
"""
from __future__ import annotations

import contextlib
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from vq import paths
from vq.spec import utcnow_iso

CAPACITY_FILENAME = "daemon_capacity.json"
"""Daemon-written state filename. Not a user-edited config file."""

CAPACITY_MAX_BYTES = 64 * 1024
"""Maximum accepted fallback snapshot size."""

_PositiveCapacityLimit = Annotated[int, Field(strict=True, gt=0)]


def _read_bounded_regular_capacity(path: Path) -> bytes | None:
    """Read one small regular snapshot without following or blocking."""
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        return None
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            return None
        if metadata.st_size > CAPACITY_MAX_BYTES:
            return None
        chunks: list[bytes] = []
        remaining = CAPACITY_MAX_BYTES + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(remaining, 16 * 1024))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        if len(raw) > CAPACITY_MAX_BYTES:
            return None
        return raw
    except OSError:
        return None
    finally:
        os.close(descriptor)


class DaemonCapacity(BaseModel):
    """The daemon's configured resource budget, advertised for load-aware
    placement. ``max_jobs`` / ``max_mem_mb`` are ``None`` when the daemon
    runs with no such gate (``max_cpus`` always has a value — it defaults
    to ``os.cpu_count()``)."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    max_cpus: _PositiveCapacityLimit
    max_jobs: _PositiveCapacityLimit | None = None
    max_mem_mb: _PositiveCapacityLimit | None = None
    max_scheduler_jobs: _PositiveCapacityLimit | None = None
    default_job_mem_mb: _PositiveCapacityLimit | None = None
    """The daemon's ``--default-job-mem-mb``: what an *undeclared* job is
    charged against the memory gate.

    Advertised because it is otherwise a daemon-CLI-only value, invisible to
    every reader. Without it a client cannot reproduce the daemon's memory
    admission decision at all: it can neither charge a ``mem_mb=None`` spec nor
    tally the memory already in use by undeclared running jobs, so
    ``vq status`` silently under-counted and dropped the memory reason on the
    floor for exactly the jobs it applies to."""
    written_at: str
    """``utcnow_iso()`` at the daemon-startup write."""


@dataclass(frozen=True)
class ConfiguredCapacityOverage:
    """One resource request that exceeds a daemon's configured base cap.

    This is deliberately narrower than an admission blocker: current usage,
    drains, quotas, and live free memory can delay a job that otherwise fits.
    An overage here means the request cannot dispatch under this daemon's
    advertised base configuration, even on an otherwise idle host.
    """

    resource: Literal["cpus", "memory"]
    requested: int
    limit: int
    uses_default: bool = False

    def to_payload(self) -> dict[str, str | int | bool]:
        """Return the stable machine-readable representation."""
        return {
            "resource": self.resource,
            "requested": self.requested,
            "limit": self.limit,
            "uses_default": self.uses_default,
        }


def configured_capacity_overages(
    *,
    cpus: int,
    mem_mb: int | None,
    snapshot: DaemonCapacity,
) -> tuple[ConfiguredCapacityOverage, ...]:
    """Return requests that cannot fit ``snapshot``'s configured caps.

    Memory mirrors the daemon's admission charge: an undeclared request uses
    ``default_job_mem_mb``. A missing memory cap or missing default leaves that
    dimension unconstrained/unknown. The helper intentionally ignores live
    usage, temporary drain reductions, per-user quotas, and host free RAM.
    """
    overages: list[ConfiguredCapacityOverage] = []
    if cpus > snapshot.max_cpus:
        overages.append(
            ConfiguredCapacityOverage(
                resource="cpus",
                requested=cpus,
                limit=snapshot.max_cpus,
            )
        )

    effective_mem_mb = (
        mem_mb if mem_mb is not None else snapshot.default_job_mem_mb
    )
    if (
        effective_mem_mb is not None
        and snapshot.max_mem_mb is not None
        and effective_mem_mb > snapshot.max_mem_mb
    ):
        overages.append(
            ConfiguredCapacityOverage(
                resource="memory",
                requested=effective_mem_mb,
                limit=snapshot.max_mem_mb,
                uses_default=mem_mb is None,
            )
        )
    return tuple(overages)


def _capacity_multi_user() -> bool:
    """Resolve which daemon a client should ask for capacity."""
    from vq import config as _config  # noqa: PLC0415

    return (
        _config.load_config().multi_user.enabled
        or _config.system_multi_user_enabled()
    )


def capacity_path(*, multi_user: bool | None = None) -> Path:
    """Return the canonical capacity advertisement path for one daemon mode."""
    if paths.is_multi_user() if multi_user is None else multi_user:
        return paths.multi_user_root() / CAPACITY_FILENAME
    return paths.state_root() / CAPACITY_FILENAME


def write_daemon_capacity(
    max_cpus: int,
    max_jobs: int | None,
    max_mem_mb: int | None,
    max_scheduler_jobs: int | None = None,
    default_job_mem_mb: int | None = None,
    *,
    multi_user: bool | None = None,
) -> DaemonCapacity:
    """Create the daemon-start snapshot and write its fallback file.

    The file write is best-effort: the returned snapshot remains available to
    the daemon's RPC handler even when a read-only filesystem or transient I/O
    error prevents persistence.
    """
    state = DaemonCapacity(
        max_cpus=max_cpus,
        max_jobs=max_jobs,
        max_mem_mb=max_mem_mb,
        max_scheduler_jobs=max_scheduler_jobs,
        default_job_mem_mb=default_job_mem_mb,
        written_at=utcnow_iso(),
    )
    p = capacity_path(multi_user=multi_user)
    with contextlib.suppress(OSError):
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.parent / (p.name + ".tmp")
        tmp.write_text(state.model_dump_json())
        tmp.replace(p)  # atomic rename on POSIX
    return state


def read_daemon_capacity(
    *,
    via_rpc: bool = True,
    multi_user: bool | None = None,
) -> DaemonCapacity | None:
    """Read the advertised budget from live RPC, then its file fallback.

    Returns ``None`` if neither source provides a parseable snapshot. Never
    raises - placement degrades to "unknown capacity for this host".
    """
    if via_rpc:
        from vq import rpc as _rpc  # noqa: PLC0415

        mu = _capacity_multi_user() if multi_user is None else multi_user
        result = _rpc.try_rpc_or_fallback(
            "get_daemon_capacity",
            multi_user=mu,
            fallback=lambda: read_daemon_capacity(
                via_rpc=False,
                multi_user=mu,
            ),
            # The fallback is explicitly mode-aware and therefore reads the
            # same canonical file, not the caller's unrelated XDG store.
            fallback_warns_in_multi_user=False,
        )
        if result is None or isinstance(result, DaemonCapacity):
            return result
        if isinstance(result, dict):
            known = set(DaemonCapacity.model_fields)
            filtered = {key: value for key, value in result.items() if key in known}
            try:
                return DaemonCapacity.model_validate(filtered)
            except ValueError:
                return read_daemon_capacity(via_rpc=False, multi_user=mu)
        return read_daemon_capacity(via_rpc=False, multi_user=mu)

    raw = _read_bounded_regular_capacity(
        capacity_path(multi_user=multi_user)
    )
    if raw is None:
        return None
    try:
        return DaemonCapacity.model_validate_json(raw)
    except Exception:  # pragma: no cover — forward-compat / corruption
        return None
