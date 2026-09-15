"""Daemon dispatch gate — ``vq drain`` puts new dispatches on hold.

The daemon writes nothing during a drain; ``vq drain`` writes a tiny
state file (``<state_root>/drain.json``) that the daemon's main loop
consults at the top of every ``_dispatch_pending`` call. Three modes:

* **Full drain** (``vq drain``) — no new jobs dispatched at all.
  Running jobs continue to completion. Useful when the user wants the
  box for something else for a bounded period without killing live
  jobs.
* **Partial drain** (``vq drain --max-jobs N`` / ``--max-cpus N``) —
  temporarily lowers the effective concurrent-job or CPU cap below the
  daemon's configured value. Lets small ``--cpus 1`` work through
  while blocking big jobs.
* **Scheduler-target drain** (``vq drain --scheduler-host HOST``) —
  blocks new qsub/sbatch dispatch for that scheduler target only, while
  unrelated scheduler targets and local jobs can still dispatch.
* **Released** (``vq drain --release``) — removes the drain state
  file; daemon goes back to its configured caps.

Survives daemon restarts because the state is on disk. Survives host
reboots for the same reason. The daemon checks drain state every
dispatch iteration (cheap stat + json parse), so drain takes effect
within one ``poll_interval`` (default 1 s).

Drain does NOT affect:

* **Already-running jobs** — they continue. Use ``vq pause --all`` or
  ``vq throttle --all`` for that.
* **The watchdog** — RSS / wall-time / starvation kills still fire.
  Drain only gates the dispatch of new pending jobs.
* **`vq submit`** by default — submissions still land as PENDING
  specs; they just don't dispatch until drain releases or
  partial-drain caps allow. Operators can opt into submission
  rejection for maintenance windows via ``reject_submits``.

Design notes:

* The drain state file is daemon-internal; the user-facing API is
  the ``vq drain`` CLI. We don't expose ``drain.json`` as a "config
  file" the user edits by hand.
* Effective caps are ``min(daemon.max_X, drain.max_X)`` when both
  are set. ``drain.max_X = None`` means "use the daemon's cap"; the
  daemon's value is already there from CLI flags at startup.
* Full-drain mode is ``enabled=True, max_jobs=None, max_cpus=None``.
  Partial-drain is ``enabled=True, max_jobs=N or max_cpus=N``. The
  ``enabled`` flag is the gate; cap fields are overrides.
"""
from __future__ import annotations

import contextlib
import fcntl
import json
import math
import os
import secrets
import shlex
import stat
import threading
from collections.abc import Iterator, Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt

from vq import paths
from vq.spec import utcnow_iso

DRAIN_FILENAME = "drain.json"
"""State filename under state_root(). Daemon-internal; the user-facing
API is the ``vq drain`` CLI."""

SCHEDULER_DRAIN_LEASES_FILENAME = "scheduler-drain-leases.json"
LEGACY_DRAIN_LOCK_FILENAME = ".drain.lock"
SCHEDULER_DRAIN_LEASES_LOCK_FILENAME = ".scheduler-drain-leases.lock"
SCHEDULER_DRAIN_LEASE_RPC_METHOD = "set_scheduler_drain_lease"
SCHEDULER_DRAIN_LEASES_RPC_METHOD = "get_scheduler_drain_leases"
DRAIN_READ_ONLY_SNAPSHOT_RPC_METHOD = "get_drain_read_only_snapshot"
DRAIN_READ_ONLY_SNAPSHOT_SCHEMA = "vq.drain.read_only_snapshot/1"
DRAIN_READ_ONLY_STATUS_SCHEMA = "vq.drain.read_only_status/1"
LEGACY_SCHEDULER_DRAIN_RELEASE_RPC_METHOD = "set_legacy_scheduler_drain_release"
OWNED_FULL_DRAIN_RELEASE_RPC_METHOD = "set_owned_full_drain_release"
SCHEDULER_DRAIN_LEASE_SCHEMA_VERSION = 1
_DRAIN_SNAPSHOT_FILE_LIMIT = 1024 * 1024
_DRAIN_SNAPSHOT_MAX_DEPTH = 64
_DRAIN_SNAPSHOT_TEXT_LIMIT = 1024


class SchedulerDrainLeaseError(RuntimeError):
    """A scheduler drain claim could not be read or changed safely."""


class SchedulerDrainCapabilityError(SchedulerDrainLeaseError):
    """The running daemon cannot perform owner-scoped lease mutations."""


class OwnedFullDrainReleaseError(RuntimeError):
    """An owned full drain could not be released without guessing."""


class DrainSnapshotError(RuntimeError):
    """A strictly read-only drain snapshot was unavailable or invalid."""


class SchedulerDrainLease(BaseModel):
    """One independently owned, non-expiring scheduler dispatch hold.

    Claims deliberately live outside :class:`DrainState`. Older clients write
    that model as one whole object through ``set_drain_state``; embedding
    claims there would let an old writer silently erase every newer claim.
    The separate store makes mixed-version legacy writes harmless.
    """

    # Store reads preserve unknown fields so a current read/mutate/write does
    # not erase metadata written by a future daemon. RPC writes are validated
    # strictly at the direct mapping-mutation boundary before reaching storage.
    model_config = ConfigDict(extra="allow")

    lease_id: str = Field(pattern=r"^[A-Za-z0-9._:-]{1,160}$")
    scheduler_host: str = Field(min_length=1, max_length=255)
    owner: str = Field(min_length=1, max_length=512)
    set_at: str = Field(default_factory=utcnow_iso)
    reason: str | None = None
    # POSIX pid_t is a positive signed integer on supported hosts. Values
    # outside this range either address process groups (<= 0) or make
    # ``os.kill(pid, 0)`` raise OverflowError during status rendering.
    owner_pid: int | None = Field(default=None, gt=0, le=2_147_483_647)
    owner_pid_start_time: int = 0


class _SchedulerDrainLeaseStore(BaseModel):
    """Versioned on-disk envelope for scheduler leases."""

    model_config = ConfigDict(extra="allow")

    schema_version: Literal[1] = 1
    leases: list[SchedulerDrainLease] = Field(default_factory=list)


class DrainState(BaseModel):
    """Persisted drain state. Written by ``vq drain``, read by the
    daemon at dispatch time.

    Unknown keys are IGNORED, not rejected, and that is a safety property
    rather than laxness. This state file is shared across a fleet that is
    routinely mid-rolling-upgrade, so a file written by a newer vq is read by
    older ones. Combined with :func:`read_drain_state`'s deliberate
    treat-corrupt-as-no-drain behaviour, ``extra="forbid"`` turned a single
    added field into "this drained host is not drained" on every host still
    running the older build -- i.e. the daemon would resume dispatching into a
    host an operator had held. Verified 2026-07-27: a state file carrying one
    unknown key parsed to ``None``, and a held ``scheduler_hosts=["pbs-cluster"]`` lane
    vanished with it.

    NOTE: this does not retroactively protect readers at vq <= 0.20.0, which
    still forbid extras. Do not add a field to this model until the fleet is
    past the release carrying this change, or those hosts will un-drain.
    """

    model_config = ConfigDict(extra="ignore")

    enabled: bool = True
    """Master flag. If False, the state file shouldn't exist at all
    (``vq drain --release`` deletes the file). Kept here for symmetry
    with future state extensions."""

    max_jobs: int | None = None
    """If set, override the daemon's concurrent-job cap with this
    value. None means 'use daemon's configured max_jobs'. Combined
    with ``max_jobs=None enabled=True`` = full drain."""

    max_cpus: int | None = None
    """If set, override the daemon's concurrent-CPU cap with this
    value. Same semantics as ``max_jobs``."""

    set_at: str = Field(default_factory=utcnow_iso)
    """ISO timestamp when drain was set. Surfaced via ``vq drain --status``."""

    reason: str | None = None
    """Optional free-text label for why drain is set
    (``vq drain --reason 'kids gaming'``). Surfaced via --status."""

    owner_pid: int | None = None
    """PID that took a vq-owned lane, when one did.

    A leaked lane is otherwise indistinguishable from a deliberate operator
    hold, which is exactly what makes recovery manual: `vq admin update
    <scheduler-host> --drain-wait` killed mid-wait leaves its lane behind, and
    vq's own "never lift an operator's hold" rule then reads the corpse as
    intentional. pbs-cluster, 2026-07-26 -- the host sat `accept_pending`, taking
    submissions and dispatching nothing, until someone released it by hand.

    Recording the owner lets :func:`orphaned_lane_reason` say so. It never
    auto-lifts: an operator's deliberate hold and a leaked one look identical
    on disk precisely because vq must not guess, and guessing wrong drops a
    hold someone is relying on. None = an operator hold, or a lane predating
    this field."""

    owner_pid_start_time: int = 0
    """`/proc` start time of :attr:`owner_pid` at the moment the lane was taken.

    The anti-recycling fingerprint, mirroring the admin-update marker's: after a
    reboot or a long idle the kernel may reuse a pid, so a bare liveness probe
    can read "alive" against a stranger. 0 where `/proc` is unavailable (macOS),
    in which case liveness alone is the signal."""

    scheduler_hosts: list[str] = Field(default_factory=list)
    """Scheduler targets whose pending rows are held from qsub/sbatch.

    This is a daemon-enforced lane drain: existing PENDING specs are left
    untouched, but the dispatch loop skips matching ``scheduler_target`` rows
    until the target is released.
    """

    full_dispatch: bool = False
    """Explicit global dispatch hold that can coexist with scheduler lanes.

    Older drain states encoded "full drain" as enabled with no max_jobs/max_cpus
    and no scheduler_hosts. This flag is the newer overlap-safe form: the daemon
    can hold all dispatch while also persisting named scheduler lanes for a later
    atomic handoff.
    """

    reject_submits: bool = False
    """When True, ``vq submit`` fails fast instead of writing a PENDING spec.

    Normal drain remains an accept-for-later dispatch pause. This flag is for
    fleet updates where accepting more stale-runtime work would be misleading.
    """

    update_mode: Literal["accept", "deny"] | None = None
    """Optional operator-facing maintenance label.

    ``"accept"`` means "paused for update but accepting jobs for later";
    ``"deny"`` means "paused for update and denying new jobs".
    """

    duration_seconds: StrictInt | None = Field(default=None, gt=0)
    """v0.5.16: optional auto-release after this many seconds elapsed
    from ``set_at``. ``None`` means "drain until manually released."
    Checked inside ``read_drain_state()`` — expired drain is silently
    cleared on the next read, so callers always see "expired = no
    drain." Useful for bounded windows ("kids gaming for 2 hours")
    where the user might otherwise forget to release."""

    @property
    def is_full_drain(self) -> bool:
        """True iff drain blocks ALL new dispatches (no partial-cap overrides)."""
        return (
            self.enabled
            and (
                self.full_dispatch
                or (
                    self.max_jobs is None
                    and self.max_cpus is None
                    and not self.scheduler_hosts
                )
            )
        )

    @property
    def is_scheduler_target_drain(self) -> bool:
        """True iff this drain holds one or more scheduler targets only."""
        return self.enabled and bool(self.scheduler_hosts)

    def drains_scheduler_target(self, target: str | None) -> bool:
        """Return True when ``target`` is held by this drain state."""
        return bool(target and target in self.scheduler_hosts)

    def effective_max_jobs(self, daemon_max_jobs: int | None) -> int | None:
        """Compute the effective concurrent-job cap during drain.

        Returns ``min(daemon_max_jobs, self.max_jobs)`` when both are
        set, else whichever is set. Full drain (both None +
        ``enabled=True``) is handled by the dispatch loop checking
        ``is_full_drain`` first.
        """
        if self.max_jobs is None:
            return daemon_max_jobs
        if daemon_max_jobs is None:
            return self.max_jobs
        return min(self.max_jobs, daemon_max_jobs)

    def effective_max_cpus(self, daemon_max_cpus: int) -> int:
        """Same as effective_max_jobs but for CPU cap. Daemon's
        max_cpus is non-None by construction (defaults to
        ``os.cpu_count()``)."""
        if self.max_cpus is None:
            return daemon_max_cpus
        return min(self.max_cpus, daemon_max_cpus)


def _validate_drain_state_for_read(data: Mapping[str, object]) -> DrainState:
    """Preserve a hold when only its legacy expiry value is invalid."""
    duration = data.get("duration_seconds")
    if duration is not None and (type(duration) is not int or duration <= 0):
        data = {**data, "duration_seconds": None}
    return DrainState.model_validate(data)


def _drain_multi_user() -> bool:
    """Resolve the daemon scope for a client choosing which RPC to call."""
    from vq import config as _config  # noqa: PLC0415

    return (
        _config.load_config().multi_user.enabled
        or _config.system_multi_user_enabled()
    )


def drain_state_path(*, multi_user: bool | None = None) -> Path:
    """Path to the drain state file (does not require it to exist).

    v0.8.1 *Karp's Reduction*: in multi-user mode the drain file is a
    daemon-wide setting shared by the root daemon and admin-group CLI
    callers. Mirrors :func:`throttle.throttle_state_path` (which got the
    same fix in v0.6.37) — pre-v0.8.1, ``vq drain`` from a user in
    multi-user mode wrote ``~/.local/share/vq/drain.json`` while the
    root daemon read ``/var/lib/vq/drain.json``: two unrelated views,
    user's drain never reached the dispatch loop. The RPC routing
    introduced in this version is the long-term fix; the multi-user
    path mapping here is the direct-write fallback's correctness gate.
    """
    # Direct daemon reads must use the mode of this process. A non-root
    # personal daemon can coexist with a system multi-user daemon on the same
    # host; system-config discovery is only appropriate for a client choosing
    # an RPC socket. Client-side RPC fallbacks pass that choice explicitly.
    if paths.is_multi_user() if multi_user is None else multi_user:
        return paths.multi_user_root() / DRAIN_FILENAME
    return paths.state_root() / DRAIN_FILENAME


_legacy_drain_lock_local = threading.local()


@contextlib.contextmanager
def _legacy_drain_state_lock(path: Path) -> Iterator[None]:
    """Serialize direct legacy-state reads that can mutate with RPC writes."""
    lock_path = path.with_name(LEGACY_DRAIN_LOCK_FILENAME)
    key = str(lock_path)
    held = getattr(_legacy_drain_lock_local, "paths", set())
    if key in held:
        yield
        return
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        # An already-running root daemon may have created this stable lock as
        # 0644. Status fallback must still be able to participate as an
        # unprivileged admin-group reader; flock does not require a writable
        # descriptor on supported POSIX hosts.
        fd = os.open(str(lock_path), os.O_RDONLY)
    except FileNotFoundError:
        fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o666)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        previous = held
        _legacy_drain_lock_local.paths = {*held, key}
        try:
            yield
        finally:
            _legacy_drain_lock_local.paths = previous
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def scheduler_drain_leases_path(*, multi_user: bool | None = None) -> Path:
    """Path of the mixed-version-safe scheduler lease store."""
    return drain_state_path(multi_user=multi_user).with_name(
        SCHEDULER_DRAIN_LEASES_FILENAME
    )


def _scheduler_drain_lease_paths(
    *,
    multi_user: bool | None = None,
) -> tuple[Path, Path]:
    """Resolve the data and stable lock path once for one transaction."""
    store_path = scheduler_drain_leases_path(multi_user=multi_user)
    return (
        store_path,
        store_path.with_name(SCHEDULER_DRAIN_LEASES_LOCK_FILENAME),
    )


@contextlib.contextmanager
def _scheduler_drain_leases_lock(lock_path: Path) -> Iterator[None]:
    """Serialize one lease-store read/mutate/atomic-write transaction."""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o666)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def prepare_read_only_drain_snapshot_locks(
    *,
    multi_user: bool | None = None,
) -> None:
    """Create the stable dual-snapshot locks during daemon startup.

    The observation RPC itself is forbidden from creating filesystem entries.
    Preparing the pair before the RPC server starts gives every later snapshot
    a fixed, non-writing legacy-then-lease transaction boundary.
    """
    legacy_path = drain_state_path(multi_user=multi_user)
    _store_path, lease_lock_path = _scheduler_drain_lease_paths(
        multi_user=multi_user
    )
    lock_paths = (
        legacy_path.with_name(LEGACY_DRAIN_LOCK_FILENAME),
        lease_lock_path,
    )
    legacy_path.parent.mkdir(parents=True, exist_ok=True)
    for lock_path in lock_paths:
        fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o666)
        os.close(fd)


def _open_existing_snapshot_lock(path: Path) -> int:
    flags = os.O_RDONLY
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    try:
        fd = os.open(str(path), flags)
    except OSError as exc:
        raise DrainSnapshotError(
            f"stable snapshot lock is unavailable: {path.name}"
        ) from exc
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise DrainSnapshotError(
                f"stable snapshot lock is not a regular file: {path.name}"
            )
        return fd
    except BaseException:
        os.close(fd)
        raise


@contextlib.contextmanager
def _read_only_drain_snapshot_locks(
    legacy_lock_path: Path,
    lease_lock_path: Path,
) -> Iterator[None]:
    """Hold both existing locks in the sole dual-lock order."""
    legacy_fd = _open_existing_snapshot_lock(legacy_lock_path)
    lease_fd: int | None = None
    try:
        try:
            fcntl.flock(legacy_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise DrainSnapshotError("legacy snapshot lock is busy") from exc
        lease_fd = _open_existing_snapshot_lock(lease_lock_path)
        try:
            try:
                fcntl.flock(lease_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                raise DrainSnapshotError(
                    "scheduler snapshot lock is busy"
                ) from exc
            yield
        finally:
            if lease_fd is not None:
                with contextlib.suppress(OSError):
                    fcntl.flock(lease_fd, fcntl.LOCK_UN)
                os.close(lease_fd)
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(legacy_fd, fcntl.LOCK_UN)
        os.close(legacy_fd)


def _strict_snapshot_json(path: Path) -> dict[str, object] | None:
    """Read one bounded JSON object without following its final symlink."""
    flags = os.O_RDONLY
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    try:
        fd = os.open(str(path), flags)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise DrainSnapshotError(f"{path.name} is unreadable") from exc
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > _DRAIN_SNAPSHOT_FILE_LIMIT:
            raise DrainSnapshotError(f"{path.name} is not a bounded regular file")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(fd, 65536)
            if not chunk:
                break
            total += len(chunk)
            if total > _DRAIN_SNAPSHOT_FILE_LIMIT:
                raise DrainSnapshotError(f"{path.name} exceeds the snapshot limit")
            chunks.append(chunk)
    finally:
        os.close(fd)
    try:
        text = b"".join(chunks).decode("utf-8")

        def reject_duplicates(
            pairs: list[tuple[str, object]],
        ) -> dict[str, object]:
            value: dict[str, object] = {}
            for key, item in pairs:
                if key in value:
                    raise ValueError(f"duplicate JSON key {key!r}")
                value[key] = item
            return value

        def reject_constant(value: str) -> object:
            raise ValueError(f"nonstandard JSON constant {value!r}")

        decoded = json.loads(
            text,
            object_pairs_hook=reject_duplicates,
            parse_constant=reject_constant,
        )
        pending: list[tuple[object, int]] = [(decoded, 0)]
        while pending:
            item, depth = pending.pop()
            if depth > _DRAIN_SNAPSHOT_MAX_DEPTH:
                raise ValueError("JSON is too deeply nested")
            if isinstance(item, dict):
                pending.extend((child, depth + 1) for child in item.values())
            elif isinstance(item, list):
                pending.extend((child, depth + 1) for child in item)
            elif isinstance(item, float) and not math.isfinite(item):
                raise ValueError("JSON contains a non-finite number")
        if not isinstance(decoded, dict):
            raise ValueError("JSON root is not an object")
        return decoded
    except (RecursionError, UnicodeError, ValueError) as exc:
        raise DrainSnapshotError(f"{path.name} is invalid") from exc


def _strict_snapshot_lease_store(path: Path) -> list[SchedulerDrainLease]:
    data = _strict_snapshot_json(path)
    if data is None:
        return []
    schema_version = data.get("schema_version")
    if (
        type(schema_version) is not int
        or schema_version != SCHEDULER_DRAIN_LEASE_SCHEMA_VERSION
        or not isinstance(data.get("leases"), list)
    ):
        raise DrainSnapshotError("scheduler drain lease schema is unsupported")
    try:
        store = _SchedulerDrainLeaseStore.model_validate(data, strict=True)
    except ValueError as exc:
        raise DrainSnapshotError("scheduler drain lease store is invalid") from exc
    ids = [lease.lease_id for lease in store.leases]
    owners = [(lease.scheduler_host, lease.owner) for lease in store.leases]
    if len(ids) != len(set(ids)) or len(owners) != len(set(owners)):
        raise DrainSnapshotError("scheduler drain lease identities are duplicated")
    return list(store.leases)


def read_locked_drain_snapshot(
    *,
    multi_user: bool | None = None,
) -> dict[str, object]:
    """Copy legacy and lease state under both locks without any write."""
    legacy_path = drain_state_path(multi_user=multi_user)
    lease_path, lease_lock_path = _scheduler_drain_lease_paths(
        multi_user=multi_user
    )
    legacy_lock_path = legacy_path.with_name(LEGACY_DRAIN_LOCK_FILENAME)
    legacy: DrainState | None = None
    leases: list[SchedulerDrainLease] = []
    legacy_error: str | None = None
    leases_error: str | None = None
    observed_at: str | None = None
    with _read_only_drain_snapshot_locks(legacy_lock_path, lease_lock_path):
        try:
            raw_legacy = _strict_snapshot_json(legacy_path)
            if raw_legacy is not None:
                legacy = DrainState.model_validate(raw_legacy, strict=True)
        except (DrainSnapshotError, ValueError):
            legacy_error = "unreadable"
        try:
            leases = _strict_snapshot_lease_store(lease_path)
        except DrainSnapshotError:
            leases_error = "unreadable"
        # Bind the timestamp to the bytes while both writer locks remain held.
        observed_at = utcnow_iso()
    assert observed_at is not None
    return {
        "observed_at": observed_at,
        "legacy_state": (
            legacy.model_dump(mode="json") if legacy is not None else None
        ),
        "legacy_error": legacy_error,
        "scheduler_leases": [
            lease.model_dump(mode="json") for lease in leases
        ],
        "scheduler_leases_error": leases_error,
    }


def _read_scheduler_drain_lease_store(path: Path) -> _SchedulerDrainLeaseStore:
    try:
        data = _strict_snapshot_json(path)
        if data is None:
            return _SchedulerDrainLeaseStore()
        schema_version = data.get("schema_version")
        if (
            type(schema_version) is not int
            or schema_version != SCHEDULER_DRAIN_LEASE_SCHEMA_VERSION
        ):
            raise ValueError(
                f"unsupported scheduler drain lease schema {schema_version!r}"
            )
        if not isinstance(data.get("leases"), list):
            raise ValueError("store leases must be an explicit list")
        store = _SchedulerDrainLeaseStore.model_validate(data, strict=True)
    except (DrainSnapshotError, OSError, RecursionError, ValueError) as exc:
        raise SchedulerDrainLeaseError(
            f"scheduler drain lease store {path} is unreadable: {exc}"
        ) from exc
    ids = [lease.lease_id for lease in store.leases]
    if len(ids) != len(set(ids)):
        raise SchedulerDrainLeaseError(
            f"scheduler drain lease store {path} contains duplicate lease IDs"
        )
    owners = [
        (lease.scheduler_host, lease.owner) for lease in store.leases
    ]
    if len(owners) != len(set(owners)):
        raise SchedulerDrainLeaseError(
            f"scheduler drain lease store {path} contains duplicate "
            "scheduler-host/owner claims"
        )
    return store


def _write_scheduler_drain_lease_store(
    store: _SchedulerDrainLeaseStore,
    path: Path,
) -> None:
    encoded = store.model_dump_json(indent=2) + "\n"
    if len(encoded.encode("utf-8")) > _DRAIN_SNAPSHOT_FILE_LIMIT:
        raise SchedulerDrainLeaseError(
            "scheduler drain lease store exceeds the supported size limit"
        )
    paths.atomic_write_text(
        path,
        encoded,
    )


def apply_scheduler_drain_lease_mutation(
    *,
    acquire: SchedulerDrainLease | None = None,
    release_id: str | None = None,
    release_host: str | None = None,
    release_owner: str | None = None,
    release_all: bool = False,
    multi_user: bool | None = None,
) -> tuple[list[SchedulerDrainLease], SchedulerDrainLease | None, bool]:
    """Apply exactly one locked lease mutation.

    Returns ``(all_leases, acquired_lease, changed)``. Acquisition is
    idempotent for one ``(scheduler_host, owner)`` pair so a resumed rollout
    finds its original claim rather than stacking duplicates.
    """
    if type(release_all) is not bool:
        raise SchedulerDrainLeaseError("release_all must be a boolean")
    for name, value in (
        ("release_id", release_id),
        ("release_host", release_host),
        ("release_owner", release_owner),
    ):
        if value is not None and (
            not isinstance(value, str) or not value.strip()
        ):
            raise SchedulerDrainLeaseError(f"{name} must be a non-empty string")
    actions = (
        acquire is not None
        and release_id is None
        and release_host is None
        and release_owner is None
        and not release_all,
        acquire is None
        and release_id is not None
        and release_host is None
        and release_owner is None
        and not release_all,
        acquire is None
        and release_id is None
        and release_host is not None
        and not release_all,
        acquire is None
        and release_id is None
        and release_host is None
        and release_owner is None
        and release_all,
    )
    if sum(actions) != 1:
        raise SchedulerDrainLeaseError("exactly one lease mutation is required")
    store_path, lock_path = _scheduler_drain_lease_paths(
        multi_user=multi_user
    )
    with _scheduler_drain_leases_lock(lock_path):
        store = _read_scheduler_drain_lease_store(store_path)
        acquired: SchedulerDrainLease | None = None
        changed = False
        if acquire is not None:
            for existing in store.leases:
                if existing.lease_id == acquire.lease_id:
                    if (
                        existing.scheduler_host != acquire.scheduler_host
                        or existing.owner != acquire.owner
                    ):
                        raise SchedulerDrainLeaseError(
                            f"lease ID {acquire.lease_id!r} already names a "
                            "different scheduler drain claim"
                        )
                    # A response-loss retry constructs fresh timestamps and
                    # process metadata. The durable first claim wins; identity
                    # is the exact ID plus its host/owner binding.
                    acquired = existing
                    break
                if (
                    existing.scheduler_host == acquire.scheduler_host
                    and existing.owner == acquire.owner
                ):
                    acquired = existing
                    break
            else:
                store.leases.append(acquire)
                acquired = acquire
                changed = True
        elif release_id is not None:
            retained = [
                lease for lease in store.leases if lease.lease_id != release_id
            ]
            changed = len(retained) != len(store.leases)
            store.leases = retained
        elif release_host is not None:
            retained = [
                lease
                for lease in store.leases
                if not (
                    lease.scheduler_host == release_host
                    and (release_owner is None or lease.owner == release_owner)
                )
            ]
            changed = len(retained) != len(store.leases)
            store.leases = retained
        else:
            changed = bool(store.leases)
            store.leases = []
        if changed:
            store.leases.sort(
                key=lambda lease: (
                    lease.scheduler_host,
                    lease.owner,
                    lease.lease_id,
                )
            )
            _write_scheduler_drain_lease_store(store, store_path)
        return list(store.leases), acquired, changed


def apply_scheduler_drain_lease_mapping_mutation(
    *,
    lease: dict[str, object] | None = None,
    release_id: str | None = None,
    release_host: str | None = None,
    release_owner: str | None = None,
    release_all: bool = False,
    multi_user: bool | None = None,
) -> tuple[list[SchedulerDrainLease], SchedulerDrainLease | None, bool]:
    """Validate a raw lease payload, then apply one locked mutation."""
    parsed = None
    if lease is not None:
        unknown = set(lease) - set(SchedulerDrainLease.model_fields)
        if unknown:
            raise ValueError(
                "unknown scheduler drain lease fields: "
                + ", ".join(sorted(unknown))
            )
        parsed = SchedulerDrainLease.model_validate(lease, strict=True)
    return apply_scheduler_drain_lease_mutation(
        acquire=parsed,
        release_id=release_id,
        release_host=release_host,
        release_owner=release_owner,
        release_all=release_all,
        multi_user=multi_user,
    )


def _rpc_lease_context(
    token: str | None = None,
    *,
    multi_user: bool | None = None,
) -> tuple[bool, str | None]:
    if multi_user is None:
        try:
            multi_user = _drain_multi_user()
        except Exception as exc:  # noqa: BLE001 - mutation must fail closed
            raise SchedulerDrainCapabilityError(
                f"cannot determine which daemon owns scheduler drains: {exc}"
            ) from exc
    resolved_token = token
    if multi_user and resolved_token is None:
        from vq import auth as _auth  # noqa: PLC0415

        resolved_token = _auth.resolve_token(None)
    return multi_user, resolved_token


def _require_scheduler_lease_rpc(*, multi_user: bool) -> None:
    from vq import rpc as _rpc  # noqa: PLC0415

    try:
        result = _rpc.call("get_methods", multi_user=multi_user)
    except (_rpc.RPCError, ConnectionError) as exc:
        raise SchedulerDrainCapabilityError(
            "cannot change an owner-scoped scheduler drain because the "
            f"daemon RPC capability probe failed: {exc}"
        ) from exc
    methods = result.get("methods") if isinstance(result, dict) else None
    if not isinstance(methods, list) or SCHEDULER_DRAIN_LEASE_RPC_METHOD not in methods:
        raise SchedulerDrainCapabilityError(
            "the running daemon does not advertise owner-scoped scheduler "
            "drain leases; update and restart the daemon before using this "
            "client"
        )


def read_scheduler_drain_leases(
    *,
    via_rpc: bool = True,
    multi_user: bool | None = None,
) -> list[SchedulerDrainLease]:
    """Read all scheduler leases in stable order.

    Reads may fall back to the separate file. Mutations never do: a new client
    must prove that the running daemon owns the lease transaction before it can
    introduce a claim that an older daemon would ignore.
    """
    if via_rpc:
        from vq import rpc as _rpc  # noqa: PLC0415

        if multi_user is None:
            multi_user, _token = _rpc_lease_context()
        try:
            result = _rpc.call(
                SCHEDULER_DRAIN_LEASES_RPC_METHOD,
                multi_user=multi_user,
            )
        except (_rpc.RPCError, ConnectionError):
            return read_scheduler_drain_leases(
                via_rpc=False,
                multi_user=multi_user,
            )
        if (
            not isinstance(result, dict)
            or type(result.get("schema_version")) is not int
            or result["schema_version"]
            != SCHEDULER_DRAIN_LEASE_SCHEMA_VERSION
            or not isinstance(result.get("leases"), list)
        ):
            raise SchedulerDrainLeaseError(
                "daemon returned an invalid or unsupported scheduler drain "
                "lease-list envelope"
            )
        try:
            leases = [
                SchedulerDrainLease.model_validate(item, strict=True)
                for item in result["leases"]
            ]
        except ValueError as exc:
            raise SchedulerDrainLeaseError(
                f"daemon returned an invalid scheduler drain lease: {exc}"
            ) from exc
        ids = [lease.lease_id for lease in leases]
        owners = [
            (lease.scheduler_host, lease.owner) for lease in leases
        ]
        if len(ids) != len(set(ids)) or len(owners) != len(set(owners)):
            raise SchedulerDrainLeaseError(
                "daemon returned duplicate scheduler drain leases"
            )
        return leases
    store_path = scheduler_drain_leases_path(multi_user=multi_user)
    return list(_read_scheduler_drain_lease_store(store_path).leases)


def acquire_scheduler_drain_lease(
    scheduler_host: str,
    *,
    owner: str,
    reason: str | None = None,
    owner_pid: int | None = None,
    lease_id: str | None = None,
    via_rpc: bool = True,
    token: str | None = None,
    multi_user: bool | None = None,
) -> tuple[SchedulerDrainLease, bool]:
    """Acquire an independently releasable, non-expiring scheduler hold."""
    host = scheduler_host.strip()
    if not host:
        raise SchedulerDrainLeaseError("scheduler host must not be empty")
    try:
        lease = SchedulerDrainLease(
            lease_id=lease_id or secrets.token_hex(16),
            scheduler_host=host,
            owner=owner,
            reason=reason,
            owner_pid=owner_pid,
            owner_pid_start_time=(
                (_pid_start_time(owner_pid) or 0)
                if owner_pid is not None
                else 0
            ),
        )
    except ValueError as exc:
        # Keep the public mutation boundary uniform. CLI transactions catch
        # SchedulerDrainLeaseError to roll back earlier claims; leaking a raw
        # Pydantic ValidationError here bypasses that cleanup path.
        raise SchedulerDrainLeaseError(
            f"invalid scheduler drain lease: {exc}"
        ) from exc
    if not via_rpc:
        _leases, acquired, changed = apply_scheduler_drain_lease_mutation(
            acquire=lease,
            multi_user=multi_user,
        )
        assert acquired is not None
        return acquired, changed

    from vq import rpc as _rpc  # noqa: PLC0415

    multi_user, token = _rpc_lease_context(
        token,
        multi_user=multi_user,
    )
    _require_scheduler_lease_rpc(multi_user=multi_user)
    try:
        result = _rpc.call(
            SCHEDULER_DRAIN_LEASE_RPC_METHOD,
            {
                "schema_version": SCHEDULER_DRAIN_LEASE_SCHEMA_VERSION,
                "lease": lease.model_dump(mode="json"),
                "token": token,
            },
            multi_user=multi_user,
        )
    except (_rpc.RPCError, ConnectionError) as exc:
        raise SchedulerDrainCapabilityError(
            "owner-scoped scheduler drain acquisition failed; refusing an "
            f"unsafe direct-file fallback: {exc}"
        ) from exc
    if (
        not isinstance(result, dict)
        or type(result.get("schema_version")) is not int
        or result["schema_version"] != SCHEDULER_DRAIN_LEASE_SCHEMA_VERSION
        or type(result.get("changed")) is not bool
        or not isinstance(result.get("lease"), dict)
    ):
        raise SchedulerDrainLeaseError(
            "daemon returned an invalid scheduler drain acquisition result"
        )
    try:
        acquired = SchedulerDrainLease.model_validate(
            result["lease"],
            strict=True,
        )
    except ValueError as exc:
        raise SchedulerDrainLeaseError(
            f"daemon returned an invalid scheduler drain lease: {exc}"
        ) from exc
    if acquired.scheduler_host != host or acquired.owner != owner:
        raise SchedulerDrainLeaseError(
            "daemon echoed a scheduler drain lease for the wrong host or owner"
        )
    if result["changed"] and acquired.lease_id != lease.lease_id:
        raise SchedulerDrainLeaseError(
            "daemon reported a newly created scheduler drain lease with a "
            "different ID than the requested rollback identity"
        )
    return acquired, result["changed"]


def release_scheduler_drain_lease(
    lease_id: str,
    *,
    via_rpc: bool = True,
    token: str | None = None,
    multi_user: bool | None = None,
) -> bool:
    """Release exactly one owner-scoped scheduler lease by ID."""
    if not via_rpc:
        _leases, _acquired, changed = apply_scheduler_drain_lease_mutation(
            release_id=lease_id,
            multi_user=multi_user,
        )
        return changed
    from vq import rpc as _rpc  # noqa: PLC0415

    multi_user, token = _rpc_lease_context(
        token,
        multi_user=multi_user,
    )
    _require_scheduler_lease_rpc(multi_user=multi_user)
    try:
        result = _rpc.call(
            SCHEDULER_DRAIN_LEASE_RPC_METHOD,
            {
                "schema_version": SCHEDULER_DRAIN_LEASE_SCHEMA_VERSION,
                "release_id": lease_id,
                "token": token,
            },
            multi_user=multi_user,
        )
    except (_rpc.RPCError, ConnectionError) as exc:
        raise SchedulerDrainCapabilityError(
            "owner-scoped scheduler drain release failed; refusing an unsafe "
            f"direct-file fallback: {exc}"
        ) from exc
    if (
        not isinstance(result, dict)
        or type(result.get("schema_version")) is not int
        or result["schema_version"] != SCHEDULER_DRAIN_LEASE_SCHEMA_VERSION
        or type(result.get("changed")) is not bool
    ):
        raise SchedulerDrainLeaseError(
            "daemon returned an invalid scheduler drain release result"
        )
    return result["changed"]


def release_scheduler_drain_leases(
    scheduler_host: str,
    *,
    owner: str | None = None,
    via_rpc: bool = True,
    token: str | None = None,
    multi_user: bool | None = None,
) -> bool:
    """Release claims for ``scheduler_host``, optionally for one owner only."""
    if not via_rpc:
        _leases, _acquired, changed = apply_scheduler_drain_lease_mutation(
            release_host=scheduler_host,
            release_owner=owner,
            multi_user=multi_user,
        )
        return changed
    from vq import rpc as _rpc  # noqa: PLC0415

    multi_user, token = _rpc_lease_context(
        token,
        multi_user=multi_user,
    )
    _require_scheduler_lease_rpc(multi_user=multi_user)
    try:
        result = _rpc.call(
            SCHEDULER_DRAIN_LEASE_RPC_METHOD,
            {
                "schema_version": SCHEDULER_DRAIN_LEASE_SCHEMA_VERSION,
                "release_host": scheduler_host,
                "release_owner": owner,
                "token": token,
            },
            multi_user=multi_user,
        )
    except (_rpc.RPCError, ConnectionError) as exc:
        raise SchedulerDrainCapabilityError(
            "owner-scoped scheduler drain release failed; refusing an unsafe "
            f"direct-file fallback: {exc}"
        ) from exc
    if (
        not isinstance(result, dict)
        or type(result.get("schema_version")) is not int
        or result["schema_version"] != SCHEDULER_DRAIN_LEASE_SCHEMA_VERSION
        or type(result.get("changed")) is not bool
    ):
        raise SchedulerDrainLeaseError(
            "daemon returned an invalid scheduler drain release result"
        )
    return result["changed"]


def clear_scheduler_drain_leases(
    *,
    via_rpc: bool = True,
    token: str | None = None,
    multi_user: bool | None = None,
) -> bool:
    """Release every scheduler lease as an explicit operator override."""
    if not via_rpc:
        _leases, _acquired, changed = apply_scheduler_drain_lease_mutation(
            release_all=True,
            multi_user=multi_user,
        )
        return changed
    from vq import rpc as _rpc  # noqa: PLC0415

    multi_user, token = _rpc_lease_context(
        token,
        multi_user=multi_user,
    )
    _require_scheduler_lease_rpc(multi_user=multi_user)
    try:
        result = _rpc.call(
            SCHEDULER_DRAIN_LEASE_RPC_METHOD,
            {
                "schema_version": SCHEDULER_DRAIN_LEASE_SCHEMA_VERSION,
                "release_all": True,
                "token": token,
            },
            multi_user=multi_user,
        )
    except (_rpc.RPCError, ConnectionError) as exc:
        raise SchedulerDrainCapabilityError(
            "scheduler drain release failed; refusing an unsafe direct-file "
            f"fallback: {exc}"
        ) from exc
    if (
        not isinstance(result, dict)
        or type(result.get("schema_version")) is not int
        or result["schema_version"] != SCHEDULER_DRAIN_LEASE_SCHEMA_VERSION
        or type(result.get("changed")) is not bool
    ):
        raise SchedulerDrainLeaseError(
            "daemon returned an invalid scheduler drain release-all result"
        )
    return result["changed"]


def _compose_effective_drain_state(
    legacy: DrainState | None,
    leases: list[SchedulerDrainLease],
) -> DrainState | None:
    if legacy is not None and not legacy.enabled:
        legacy = None
    if not leases:
        return legacy
    lease_hosts = [lease.scheduler_host for lease in leases]
    if legacy is None:
        effective = DrainState(
            scheduler_hosts=list(dict.fromkeys(lease_hosts)),
            full_dispatch=False,
            set_at=min(lease.set_at for lease in leases),
        )
        if len(leases) == 1:
            effective.reason = leases[0].reason
            effective.owner_pid = leases[0].owner_pid
            effective.owner_pid_start_time = leases[0].owner_pid_start_time
        return effective
    legacy_was_full = legacy.is_full_drain
    effective = legacy.model_copy(deep=True)
    # A sidecar lease is an active dispatch policy. Only an enabled legacy
    # document contributes additional policy; disabled stale fields must not
    # be reactivated merely because a lease exists.
    effective.enabled = True
    effective.scheduler_hosts = list(
        dict.fromkeys([*legacy.scheduler_hosts, *lease_hosts])
    )
    if legacy_was_full:
        # The legacy encoding used "enabled with no caps and no hosts" as an
        # implicit full hold. Adding a computed lease host would otherwise
        # change is_full_drain to False and silently resume local dispatch.
        effective.full_dispatch = True
    return effective


def read_effective_drain_snapshot(
    *,
    via_rpc: bool = True,
    multi_user: bool | None = None,
) -> tuple[
    DrainState | None,
    list[SchedulerDrainLease],
    str | None,
    DrainState | None,
]:
    """Read one coherent legacy/lease snapshot and its effective policy."""
    if via_rpc and multi_user is None:
        # One snapshot must come from one daemon tree. Config discovery can
        # change while a client is running; resolving separately for the
        # legacy and lease reads could compose personal and system state.
        multi_user = _drain_multi_user()
    raw_legacy = read_drain_state(
        via_rpc=via_rpc,
        multi_user=multi_user,
    )
    legacy = (
        raw_legacy
        if raw_legacy is not None and raw_legacy.enabled
        else None
    )
    try:
        leases = read_scheduler_drain_leases(
            via_rpc=via_rpc,
            multi_user=multi_user,
        )
    except SchedulerDrainLeaseError as exc:
        # Losing the set of held scheduler lanes is unsafe. The only sound
        # response to an unreadable store is to stop new dispatch entirely and
        # make the reason visible to operators. Preserve every legacy field:
        # submit admission reads that same record separately, so replacing it
        # with a fresh DrainState would make status claim ``accept_pending``
        # while the live submit path still enforces ``deny``. Reusing the
        # original ``set_at`` also keeps repeated status reads stable.
        detail = str(exc)
        error_reason = f"scheduler drain leases unreadable: {detail}"
        state = legacy.model_copy(deep=True) if legacy is not None else DrainState(
            set_at=(raw_legacy.set_at if raw_legacy is not None else utcnow_iso())
        )
        state.enabled = True
        state.full_dispatch = True
        # Expiry can clear only the legacy file. It cannot repair an unreadable
        # sidecar, so the synthetic full hold must not promise auto-release.
        state.duration_seconds = None
        state.reason = (
            f"{legacy.reason}; {error_reason}"
            if legacy is not None and legacy.reason
            else error_reason
        )
        return legacy, [], detail, state
    return legacy, leases, None, _compose_effective_drain_state(legacy, leases)


def read_effective_drain_state(
    *,
    via_rpc: bool = True,
    multi_user: bool | None = None,
) -> DrainState | None:
    """Compose legacy drain policy with the union of independent leases."""
    return read_effective_drain_snapshot(
        via_rpc=via_rpc,
        multi_user=multi_user,
    )[3]


def _project_read_only_legacy_expiry(
    state: DrainState | None,
    *,
    observed_at: str,
) -> DrainState | None:
    """Apply the legacy lazy-expiry policy to a copy, never to disk."""
    if state is None or not state.enabled:
        return None
    projected = state.model_copy(deep=True)
    if projected.duration_seconds is None:
        return projected
    try:
        set_dt = datetime.fromisoformat(projected.set_at)
        now = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
        if set_dt.tzinfo is None or now.tzinfo is None:
            raise ValueError("snapshot timestamps must be timezone-aware")
        now = now.astimezone(set_dt.tzinfo)
        expired = (now - set_dt).total_seconds() >= projected.duration_seconds
    except (TypeError, ValueError):
        return projected
    if not expired:
        return projected
    if not projected.scheduler_hosts:
        return None
    projected.full_dispatch = False
    projected.update_mode = None
    projected.reject_submits = False
    projected.duration_seconds = None
    return projected


def _snapshot_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    return value[:_DRAIN_SNAPSHOT_TEXT_LIMIT]


def _snapshot_hex(value: object, length: int) -> bool:
    return (
        isinstance(value, str)
        and len(value) == length
        and all(char in "0123456789abcdef" for char in value)
    )


def _validate_read_only_snapshot_envelope(
    result: object,
    *,
    expected_multi_user: bool,
) -> tuple[
    DrainState | None,
    list[SchedulerDrainLease],
    dict[str, bool],
    dict[str, object],
    str,
]:
    if not isinstance(result, Mapping):
        raise DrainSnapshotError("daemon returned a malformed read-only snapshot")
    try:
        encoded_size = len(json.dumps(result, default=str).encode("utf-8"))
    except (TypeError, ValueError) as exc:
        raise DrainSnapshotError("daemon returned a malformed read-only snapshot") from exc
    if encoded_size > _DRAIN_SNAPSHOT_FILE_LIMIT * 2:
        raise DrainSnapshotError("daemon returned an oversized read-only snapshot")
    if result.get("schema") != DRAIN_READ_ONLY_SNAPSHOT_SCHEMA:
        raise DrainSnapshotError("daemon returned an unsupported read-only snapshot")
    observed_at = result.get("observed_at")
    if not isinstance(observed_at, str):
        raise DrainSnapshotError("daemon snapshot is missing its observation time")
    try:
        observed_dt = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DrainSnapshotError("daemon snapshot observation time is invalid") from exc
    if observed_dt.tzinfo is None or observed_dt.utcoffset() != timedelta(0):
        raise DrainSnapshotError("daemon snapshot observation time is not UTC")
    provenance = result.get("provenance")
    if not isinstance(provenance, Mapping) or set(provenance) != {
        "method",
        "version",
        "source_sha",
        "source_tree_sha256",
        "multi_user",
    }:
        raise DrainSnapshotError("daemon snapshot provenance is malformed")
    if (
        provenance.get("method") != DRAIN_READ_ONLY_SNAPSHOT_RPC_METHOD
        or not isinstance(provenance.get("version"), str)
        or not provenance["version"]
        or not _snapshot_hex(provenance.get("source_sha"), 40)
        or not _snapshot_hex(provenance.get("source_tree_sha256"), 64)
        or type(provenance.get("multi_user")) is not bool
        or provenance.get("multi_user") is not expected_multi_user
    ):
        raise DrainSnapshotError("daemon snapshot provenance is unsupported")
    coverage_raw = result.get("coverage")
    if not isinstance(coverage_raw, Mapping) or set(coverage_raw) != {
        "legacy_state",
        "scheduler_leases",
    }:
        raise DrainSnapshotError("daemon snapshot coverage is malformed")
    if any(type(value) is not bool for value in coverage_raw.values()):
        raise DrainSnapshotError("daemon snapshot coverage is malformed")
    coverage = {
        "legacy_state": coverage_raw["legacy_state"],
        "scheduler_leases": coverage_raw["scheduler_leases"],
    }

    legacy: DrainState | None = None
    raw_legacy = result.get("legacy_state")
    if coverage["legacy_state"]:
        if raw_legacy is not None:
            if not isinstance(raw_legacy, Mapping):
                raise DrainSnapshotError("daemon legacy snapshot is malformed")
            try:
                legacy = DrainState.model_validate(raw_legacy, strict=True)
            except ValueError as exc:
                raise DrainSnapshotError("daemon legacy snapshot is malformed") from exc
            try:
                legacy_set_at = datetime.fromisoformat(
                    legacy.set_at.replace("Z", "+00:00")
                )
            except ValueError as exc:
                raise DrainSnapshotError("daemon legacy snapshot time is malformed") from exc
            if legacy_set_at.tzinfo is None:
                raise DrainSnapshotError("daemon legacy snapshot time is malformed")
    elif raw_legacy is not None:
        raise DrainSnapshotError("daemon legacy snapshot contradicts its coverage")

    leases: list[SchedulerDrainLease] = []
    raw_store = result.get("scheduler_lease_store")
    if coverage["scheduler_leases"]:
        if not isinstance(raw_store, Mapping):
            raise DrainSnapshotError("daemon lease snapshot is malformed")
        if (
            type(raw_store.get("schema_version")) is not int
            or raw_store["schema_version"] != SCHEDULER_DRAIN_LEASE_SCHEMA_VERSION
            or not isinstance(raw_store.get("leases"), list)
        ):
            raise DrainSnapshotError("daemon lease snapshot schema is unsupported")
        try:
            leases = [
                SchedulerDrainLease.model_validate(item, strict=True)
                for item in raw_store["leases"]
            ]
        except ValueError as exc:
            raise DrainSnapshotError("daemon lease snapshot is malformed") from exc
        ids = [lease.lease_id for lease in leases]
        owners = [(lease.scheduler_host, lease.owner) for lease in leases]
        if len(ids) != len(set(ids)) or len(owners) != len(set(owners)):
            raise DrainSnapshotError("daemon lease snapshot identities are duplicated")
        for lease in leases:
            try:
                lease_set_at = datetime.fromisoformat(
                    lease.set_at.replace("Z", "+00:00")
                )
            except ValueError as exc:
                raise DrainSnapshotError("daemon lease snapshot time is malformed") from exc
            if lease_set_at.tzinfo is None:
                raise DrainSnapshotError("daemon lease snapshot time is malformed")
    elif raw_store is not None:
        raise DrainSnapshotError("daemon lease snapshot contradicts its coverage")
    return legacy, leases, coverage, dict(provenance), observed_at


def read_only_status_payload(
    *,
    multi_user: bool | None = None,
) -> dict[str, object]:
    """Return one supported, non-writing, provenance-bound drain view.

    Unlike ordinary status this never falls back to direct files: an older or
    unreachable daemon is unknown, not authoritative evidence of inactivity.
    """
    from vq import rpc as _rpc  # noqa: PLC0415

    try:
        if multi_user is None:
            multi_user = _drain_multi_user()
        elif type(multi_user) is not bool:
            raise ValueError("multi_user must be a boolean")
        result = _rpc.call(
            DRAIN_READ_ONLY_SNAPSHOT_RPC_METHOD,
            multi_user=multi_user,
        )
    except (ConnectionError, _rpc.RPCError, OSError, ValueError) as exc:
        raise DrainSnapshotError(
            "supported read-only drain snapshot RPC is unavailable"
        ) from exc
    legacy, leases, coverage, provenance, observed_at = (
        _validate_read_only_snapshot_envelope(
            result,
            expected_multi_user=multi_user,
        )
    )
    projected_legacy = (
        _project_read_only_legacy_expiry(legacy, observed_at=observed_at)
        if coverage["legacy_state"]
        else None
    )
    safety_fail_closed = not coverage["scheduler_leases"]
    if safety_fail_closed:
        effective = (
            projected_legacy.model_copy(deep=True)
            if projected_legacy is not None
            else DrainState()
        )
        effective.enabled = True
        effective.full_dispatch = True
        effective.duration_seconds = None
        effective.reason = "scheduler drain inventory unreadable; dispatch held safe"
        if projected_legacy is None:
            effective.set_at = observed_at
    else:
        effective = _compose_effective_drain_state(projected_legacy, leases)
    if effective is None and not all(coverage.values()):
        raise DrainSnapshotError(
            "read-only drain snapshot is incomplete without an active policy"
        )

    policy: dict[str, object] | None = None
    if effective is not None:
        policy = {
            "active": True,
            "is_full_drain": effective.is_full_drain,
            "max_jobs": effective.max_jobs,
            "max_cpus": effective.max_cpus,
            "reason": _snapshot_text(effective.reason),
            "set_at": _snapshot_text(effective.set_at),
            "scheduler_hosts": sorted(set(effective.scheduler_hosts)),
            "legacy_scheduler_hosts": (
                sorted(set(projected_legacy.scheduler_hosts))
                if projected_legacy is not None
                else []
            ),
            "full_dispatch": effective.full_dispatch,
            "reject_submits": effective.reject_submits,
            "update_mode": effective.update_mode,
            "duration_seconds": effective.duration_seconds,
        }
    return {
        "schema": DRAIN_READ_ONLY_STATUS_SCHEMA,
        "observed_at": observed_at,
        "provenance": provenance,
        "coverage": coverage,
        "active": effective is not None,
        "policy": policy,
        "scheduler_leases": [
            {
                "lease_id": lease.lease_id,
                "scheduler_host": lease.scheduler_host,
                "owner": lease.owner,
                "set_at": _snapshot_text(lease.set_at),
                "reason": _snapshot_text(lease.reason),
            }
            for lease in leases
        ],
        "safety_fail_closed": safety_fail_closed,
    }


def _fallback_after_uninterpretable_rpc(
    detail: str,
    *,
    multi_user: bool,
) -> DrainState | None:
    """Read the drain file after an RPC answer the client cannot read.

    An answer we cannot interpret is a **failure**, not evidence that nothing
    is draining, and the difference is not cosmetic. The daemon's own dispatch
    loop reads the file directly (``via_rpc=False``), so it keeps blocking
    dispatch on whatever the file says. A client that reported "no drain" here
    would make ``vq drain --status`` deny a hold that is actively parking the
    fleet, and make ``--release`` / ``--release-full`` a silent no-op, because
    there is apparently nothing to release. That is the VQ-DRAIN-RPC symptom:
    the status view and the file disagree, and the file is the one with teeth.

    So treat it exactly as :func:`vq.rpc.try_rpc_or_fallback` treats a dead
    socket -- fall back to the file -- and say so loudly, because a client and
    daemon that disagree about the shape of this record is itself a defect
    worth an operator's attention.
    """
    import logging as _logging  # noqa: PLC0415 — module-local, matches this file

    _logging.getLogger(__name__).warning(
        "drain RPC returned an answer this vq cannot interpret (%s); falling "
        "back to reading %s directly. Reporting 'no drain' here would hide a "
        "hold the daemon is still enforcing. Client and daemon versions "
        "probably disagree -- restart the daemon so both run the same vq.",
        detail,
        drain_state_path(multi_user=multi_user),
    )
    return read_drain_state(via_rpc=False, multi_user=multi_user)


def read_drain_state(
    *,
    via_rpc: bool = True,
    multi_user: bool | None = None,
) -> DrainState | None:
    """Return the current DrainState or None if drain isn't active.

    Tolerates a missing or corrupt file: a corrupt drain.json is treated
    as "no drain" rather than crashing the daemon's dispatch loop. The
    daemon log notes the corruption so the user knows to investigate.

    v0.5.16: auto-release check. If the state has ``duration_seconds``
    set and ``set_at + duration_seconds`` has elapsed, clear the full
    dispatch gate in passing. If scheduler-target lanes are also present,
    they are preserved and only the global/full hold expires. Centralising the
    expiry check here means every caller (daemon dispatch loop, CLI --status,
    anything else) gets the same view.

    v0.8.1 *Karp's Reduction*: when ``via_rpc=True`` (the CLI default),
    the read is routed through the daemon's RPC socket — so the
    user-XDG vs daemon-XDG split (v0.7.12 footgun, same shape as
    admin-status got fixed for in v0.8.0) goes away. On RPC failure
    (daemon down, socket missing) the function falls back to the
    direct file read documented above; multi-user mode logs a WARNING
    on fallback because the local file may be a stale view. The
    daemon's dispatch loop passes ``via_rpc=False`` to short-circuit
    the loop (it's the source of truth, not a client of itself).
    """
    if via_rpc:
        from vq import rpc as _rpc  # noqa: PLC0415 — circular if top-level
        mu = _drain_multi_user() if multi_user is None else multi_user
        result = _rpc.try_rpc_or_fallback(
            "get_drain_state",
            multi_user=mu,
            fallback=lambda: read_drain_state(
                via_rpc=False,
                multi_user=mu,
            ),
        )
        if result is None:
            # The daemon answered, and its answer is "nothing is draining".
            # This is the ONLY path that may report absence.
            return None
        # RPC response is a plain dict; fallback already returns a
        # DrainState. Detect by type.
        if isinstance(result, DrainState):
            return result
        if isinstance(result, dict):
            # Strip unknown keys defensively in case the daemon is newer.
            known = set(DrainState.model_fields.keys())
            filtered = {k: v for k, v in result.items() if k in known}
            try:
                return _validate_drain_state_for_read(filtered)
            except ValueError as exc:
                return _fallback_after_uninterpretable_rpc(
                    f"could not validate the daemon's drain state: {exc}",
                    multi_user=mu,
                )
        return _fallback_after_uninterpretable_rpc(
            f"the daemon returned {type(result).__name__}, expected an object",
            multi_user=mu,
        )
    path = drain_state_path(multi_user=multi_user)
    with _legacy_drain_state_lock(path):
        if not path.exists():
            return None
        try:
            with path.open() as f:
                data = json.load(f)
            state = _validate_drain_state_for_read(data)
        except (OSError, ValueError):
            # Corrupt or partially-written drain file. Be conservative:
            # don't block dispatches over a parser error. The CLI surfaces
            # this via vq drain --status when the user next looks.
            return None
        # Auto-expire check. Keep the lock across the read/modify/write so an
        # RPC mutation cannot be resurrected from this earlier snapshot.
        if state.duration_seconds is not None:
            try:
                set_dt = datetime.fromisoformat(state.set_at)
                now = datetime.now(set_dt.tzinfo)
                if (now - set_dt).total_seconds() >= state.duration_seconds:
                    if state.scheduler_hosts:
                        state.full_dispatch = False
                        state.update_mode = None
                        state.reject_submits = False
                        state.duration_seconds = None
                        write_drain_state(
                            state,
                            via_rpc=False,
                            multi_user=multi_user,
                        )
                        return state
                    clear_drain(via_rpc=False, multi_user=multi_user)
                    return None
            except (ValueError, TypeError):
                # Bad set_at timestamp; treat as no-expire (better to keep
                # drain than silently clear it).
                pass
        return state


def write_drain_state(
    state: DrainState,
    *,
    via_rpc: bool = True,
    token: str | None = None,
    multi_user: bool | None = None,
) -> None:
    """Atomic write via tmpfile-then-rename.

    v0.8.1: ``via_rpc=True`` (the CLI default) routes the write through
    the daemon's RPC — multi-user mode then lands on the daemon's
    canonical file regardless of which admin-group user invoked the
    CLI. Internal callers (the daemon, RPC handlers) pass
    ``via_rpc=False`` to write the file directly.
    """
    if via_rpc:
        from vq import rpc as _rpc  # noqa: PLC0415
        mu = _drain_multi_user() if multi_user is None else multi_user
        if mu and token is None:
            from vq import auth as _auth  # noqa: PLC0415
            token = _auth.resolve_token(None)
        try:
            _rpc.call(
                "set_drain_state",
                {"state": state.model_dump(mode="json"), "token": token},
                multi_user=mu,
            )
            return
        except (_rpc.RPCError, ConnectionError) as e:
            if mu:
                raise SchedulerDrainCapabilityError(
                    "set_drain_state could not reach the system multi-user "
                    f"daemon; refusing a direct state-file write: {e}"
                ) from e
            # Fall through to the direct-write path.
        multi_user = mu
    path = drain_state_path(multi_user=multi_user)
    with _legacy_drain_state_lock(path):
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(state.model_dump_json(indent=2))
        tmp.replace(path)


def replace_drain_state_from_mapping(
    state: dict[str, object],
    *,
    multi_user: bool | None = None,
) -> bool:
    """Validate and directly replace one daemon-owned drain state.

    Unknown fields are stripped for mixed-version compatibility before the
    ordinary direct writer performs its locked atomic replace.  The return
    value is the persisted ``enabled`` flag used by transport adapters.
    """
    known = set(DrainState.model_fields.keys())
    filtered = {key: value for key, value in state.items() if key in known}
    new_state = DrainState.model_validate(filtered)
    write_drain_state(
        new_state,
        via_rpc=False,
        multi_user=multi_user,
    )
    return new_state.enabled


def clear_drain(
    *,
    via_rpc: bool = True,
    token: str | None = None,
    multi_user: bool | None = None,
) -> bool:
    """Remove the drain state file. Returns True if a file was
    removed, False if there was nothing to remove (idempotent).

    v0.8.1: ``via_rpc=True`` (the CLI default) routes the clear through
    the daemon's RPC (``set_drain_state(state=None)``). Internal
    callers (the daemon's own expiry path, RPC handlers) pass
    ``via_rpc=False`` to unlink the file directly.
    """
    if via_rpc:
        from vq import rpc as _rpc  # noqa: PLC0415
        mu = _drain_multi_user() if multi_user is None else multi_user
        if mu and token is None:
            from vq import auth as _auth  # noqa: PLC0415
            token = _auth.resolve_token(None)
        try:
            result = _rpc.call(
                "set_drain_state",
                {"state": None, "token": token},
                multi_user=mu,
            )
            if isinstance(result, dict):
                return bool(result.get("cleared", False))
            return False
        except (_rpc.RPCError, ConnectionError) as e:
            if mu:
                raise SchedulerDrainCapabilityError(
                    "clear_drain could not reach the system multi-user daemon; "
                    f"refusing a direct state-file unlink: {e}"
                ) from e
            # Fall through to direct unlink.
        multi_user = mu
    path = drain_state_path(multi_user=multi_user)
    with _legacy_drain_state_lock(path):
        if not path.exists():
            return False
        with contextlib.suppress(FileNotFoundError):
            path.unlink()
        return True


def release_scheduler_host(
    host: str,
    *,
    via_rpc: bool = True,
    token: str | None = None,
    multi_user: bool | None = None,
) -> bool:
    """Remove one scheduler-host lane from the persistent drain state.

    Returns True when the target was present. If no other drain mode remains,
    the drain state file is removed. This lets operators clear
    ``vq drain --scheduler-host pbs-cluster`` without lifting unrelated global or
    partial drains.
    """
    if via_rpc and multi_user is None:
        # One operator action must stay on one daemon tree even if config is
        # edited while the owner and legacy mutations are in flight.
        multi_user = _drain_multi_user()
    released_lease = False
    if not via_rpc:
        released_lease = release_scheduler_drain_leases(
            host,
            via_rpc=False,
            multi_user=multi_user,
        )
    else:
        # Explicit host release remains the operator's recovery override and
        # clears both legacy and owner-scoped holds. An old daemon cannot have
        # accepted a new lease; preserve its legacy release behavior when the
        # capability probe says the method is absent.
        try:
            released_lease = release_scheduler_drain_leases(
                host,
                token=token,
                multi_user=multi_user,
            )
        except SchedulerDrainCapabilityError:
            if read_scheduler_drain_leases(
                via_rpc=False,
                multi_user=multi_user,
            ):
                raise
    released_legacy = release_legacy_scheduler_host(
        host,
        via_rpc=via_rpc,
        token=token,
        multi_user=multi_user,
    )
    return released_lease or released_legacy


def release_legacy_scheduler_host(
    host: str,
    *,
    via_rpc: bool = True,
    token: str | None = None,
    expected_reason: str | None = None,
    expected_set_at: str | None = None,
    multi_user: bool | None = None,
) -> bool:
    """Remove only the pre-lease scheduler lane from ``drain.json``.

    This narrow migration primitive exists so a resumed rollout can acquire a
    durable owner-scoped lease first, then retire its own recognized legacy
    lane without deleting another owner's new lease.
    """
    if via_rpc:
        from vq import rpc as _rpc  # noqa: PLC0415

        multi_user, token = _rpc_lease_context(
            token,
            multi_user=multi_user,
        )
        try:
            result = _rpc.call(
                LEGACY_SCHEDULER_DRAIN_RELEASE_RPC_METHOD,
                {
                    "host": host,
                    "expected_reason": expected_reason,
                    "expected_set_at": expected_set_at,
                    "token": token,
                },
                multi_user=multi_user,
            )
        except (_rpc.RPCError, ConnectionError) as exc:
            raise SchedulerDrainCapabilityError(
                "atomic legacy scheduler-drain release failed; refusing a "
                f"client-side read/replace fallback: {exc}"
            ) from exc
        if (
            not isinstance(result, dict)
            or type(result.get("changed")) is not bool
        ):
            raise SchedulerDrainLeaseError(
                "daemon returned an invalid legacy scheduler-drain release "
                "result"
            )
        return result["changed"]
    path = drain_state_path(multi_user=multi_user)
    with _legacy_drain_state_lock(path):
        state = read_drain_state(
            via_rpc=False,
            multi_user=multi_user,
        )
        if state is None or host not in state.scheduler_hosts:
            return False
        if expected_set_at is not None and (
            state.set_at != expected_set_at or state.reason != expected_reason
        ):
            raise SchedulerDrainLeaseError(
                "legacy scheduler drain changed after inspection; refusing "
                f"to release {host!r} from a newer operator policy"
            )
        state.scheduler_hosts = [h for h in state.scheduler_hosts if h != host]
        if (
            not state.scheduler_hosts
            and not state.full_dispatch
            and state.max_jobs is None
            and state.max_cpus is None
            and not state.reject_submits
            and state.update_mode is None
        ):
            clear_drain(via_rpc=False, multi_user=multi_user)
        else:
            write_drain_state(
                state,
                via_rpc=False,
                multi_user=multi_user,
            )
        return True


def add_scheduler_host(
    host: str,
    *,
    reason: str | None = None,
    owner_pid: int | None = None,
    via_rpc: bool = True,
    token: str | None = None,
    multi_user: bool | None = None,
) -> bool:
    """Hold one scheduler target's dispatch lane, preserving every other mode.

    Returns True when this call actually added the lane, False when it was
    already held. That distinction is load-bearing for the maintenance flow in
    :func:`vq.admin.update_scheduler_host`: it must release only a lane it
    added itself, never an operator's pre-existing deliberate hold (pbs-cluster has
    carried one for days at a time).

    Merge semantics mirror ``vq drain --scheduler-host HOST``: an existing full
    drain is re-encoded as the overlap-safe ``full_dispatch`` flag so the global
    hold survives alongside the named lane.
    """
    if owner_pid is not None:
        # Compatibility adapter for vq-owned callers predating the explicit
        # lease API. Process-owned holds must never re-enter the legacy whole
        # document, where an old writer can erase their attribution.
        start_time = _pid_start_time(owner_pid) or 0
        _lease, changed = acquire_scheduler_drain_lease(
            host,
            owner=f"process:{owner_pid}:{start_time}:{host}",
            reason=reason,
            owner_pid=owner_pid,
            via_rpc=via_rpc,
            token=token,
            multi_user=multi_user,
        )
        return changed
    state = read_drain_state(
        via_rpc=via_rpc,
        multi_user=multi_user,
    )
    if state is not None and host in state.scheduler_hosts:
        return False
    if state is None:
        state = DrainState(
            enabled=True,
            reason=reason,
            scheduler_hosts=[host],
            full_dispatch=False,
        )
        # Stamp the owner only when vq itself is taking the lane. An operator's
        # `vq drain --scheduler-host` passes nothing, stays unstamped, and is
        # therefore never reported as an orphan -- a deliberate hold has no
        # process to outlive.
        if owner_pid is not None:
            state.owner_pid = owner_pid
            state.owner_pid_start_time = _pid_start_time(owner_pid) or 0
    else:
        if state.is_full_drain:
            state.full_dispatch = True
        state.scheduler_hosts = [*state.scheduler_hosts, host]
        if reason is not None and not state.reason:
            state.reason = reason
    write_drain_state(
        state,
        via_rpc=via_rpc,
        token=token,
        multi_user=multi_user,
    )
    return True


def release_full_drain(
    *,
    via_rpc: bool = True,
    token: str | None = None,
    multi_user: bool | None = None,
) -> bool:
    """Release only the global/full dispatch hold, preserving scheduler lanes.

    This is the safe handoff primitive for cluster outages:

    1. keep a full drain active;
    2. add one or more ``scheduler_hosts`` lanes;
    3. call this function to clear only the global gate.

    If no scheduler lanes or other partial caps remain, the drain file is
    removed. Returns True when a full/global gate was present.
    """
    state = read_drain_state(
        via_rpc=via_rpc,
        multi_user=multi_user,
    )
    if state is None:
        return False
    had_full = state.is_full_drain
    if not had_full:
        return False
    state.full_dispatch = False
    state.update_mode = None
    state.reject_submits = False
    state.duration_seconds = None
    if (
        not state.scheduler_hosts
        and state.max_jobs is None
        and state.max_cpus is None
    ):
        clear_drain(
            via_rpc=via_rpc,
            token=token,
            multi_user=multi_user,
        )
    else:
        write_drain_state(
            state,
            via_rpc=via_rpc,
            token=token,
            multi_user=multi_user,
        )
    return True


def release_owned_full_drain(
    *,
    expected_reason: str,
    expected_set_at: str,
    via_rpc: bool = True,
    token: str | None = None,
    multi_user: bool | None = None,
) -> bool:
    """Release a full drain only when its exact owner identity still matches.

    Fleet rollout recovery must not lift a drain that an operator replaced
    after the rollout recorded it.  ``reason`` alone is reusable and
    ``set_at`` alone does not identify intent, so both values are mandatory
    and compared under the same legacy-state lock as the mutation.

    The default client path is one fixed daemon RPC transaction.  It never
    performs a client-side read/replace fallback: only the daemon can bind the
    comparison to the state root selected at startup.  Internal daemon and
    test callers pass ``via_rpc=False`` to execute that locked transaction
    directly.

    Returns ``False`` when the state is missing or no full hold remains.  A
    present full hold with a different identity raises instead of silently
    treating another owner's drain as already released.
    """
    if not isinstance(expected_reason, str) or not expected_reason.strip():
        raise ValueError("expected_reason must be a non-empty string")
    if not isinstance(expected_set_at, str) or not expected_set_at.strip():
        raise ValueError("expected_set_at must be a non-empty string")

    if via_rpc:
        from vq import rpc as _rpc  # noqa: PLC0415

        if multi_user is None:
            try:
                multi_user = _drain_multi_user()
            except Exception as exc:  # noqa: BLE001 - mutation fails closed
                raise OwnedFullDrainReleaseError(
                    "cannot determine which daemon owns the full drain; "
                    f"refusing release: {exc}"
                ) from exc
        if multi_user and token is None:
            from vq import auth as _auth  # noqa: PLC0415

            token = _auth.resolve_token(None)
        try:
            result = _rpc.call(
                OWNED_FULL_DRAIN_RELEASE_RPC_METHOD,
                {
                    "expected_reason": expected_reason,
                    "expected_set_at": expected_set_at,
                    "token": token,
                },
                multi_user=multi_user,
            )
        except (_rpc.RPCError, ConnectionError) as exc:
            raise OwnedFullDrainReleaseError(
                "atomic owned full-drain release failed; refusing a "
                f"client-side read/replace fallback: {exc}"
            ) from exc
        if (
            not isinstance(result, dict)
            or type(result.get("changed")) is not bool
        ):
            raise OwnedFullDrainReleaseError(
                "daemon returned an invalid owned full-drain release result"
            )
        return result["changed"]

    path = drain_state_path(multi_user=multi_user)
    with _legacy_drain_state_lock(path):
        state = read_drain_state(
            via_rpc=False,
            multi_user=multi_user,
        )
        if state is None or not state.is_full_drain:
            return False
        if (
            state.reason != expected_reason
            or state.set_at != expected_set_at
        ):
            raise OwnedFullDrainReleaseError(
                "full drain changed after ownership was recorded; refusing "
                "to release a newer operator policy"
            )
        state.full_dispatch = False
        state.update_mode = None
        state.reject_submits = False
        state.duration_seconds = None
        if (
            not state.scheduler_hosts
            and state.max_jobs is None
            and state.max_cpus is None
        ):
            clear_drain(via_rpc=False, multi_user=multi_user)
        else:
            write_drain_state(
                state,
                via_rpc=False,
                multi_user=multi_user,
            )
        return True


def is_drained() -> bool:
    """Convenience: True if drain is active (any mode)."""
    return read_effective_drain_state() is not None


def _remaining_seconds(state: DrainState) -> int | None:
    if state.duration_seconds is None:
        return None
    try:
        set_dt = datetime.fromisoformat(state.set_at)
        now = datetime.now(set_dt.tzinfo)
        elapsed = (now - set_dt).total_seconds()
        return max(0, int(state.duration_seconds - elapsed))
    except (ValueError, TypeError):
        return None


def _mode_label(state: DrainState) -> str:
    if state.is_full_drain and state.scheduler_hosts:
        return "full+scheduler-target"
    if state.scheduler_hosts and (
        state.max_jobs is not None or state.max_cpus is not None
    ):
        return "partial+scheduler-target"
    if state.scheduler_hosts:
        return "scheduler-target"
    if state.is_full_drain:
        return "full"
    return "partial"


def status_payload() -> dict[str, object]:
    """Machine-readable summary for ``vq drain --status --json``."""
    legacy, leases, lease_error, state = read_effective_drain_snapshot()
    if state is None:
        return {
            "active": False,
            "mode": "inactive",
            "is_full_drain": False,
            "is_scheduler_target_drain": False,
            "scheduler_hosts": [],
            "scheduler_leases": [],
            "orphaned_scheduler_leases": [],
            "scheduler_leases_error": None,
            "legacy_scheduler_hosts": [],
            "submit_policy": "accept_pending",
            "state": None,
        }
    payload = state.model_dump(mode="json")
    return {
        **payload,
        "active": True,
        "mode": _mode_label(state),
        "is_full_drain": state.is_full_drain,
        "is_scheduler_target_drain": state.is_scheduler_target_drain,
        "scheduler_hosts": sorted(state.scheduler_hosts),
        "scheduler_leases": [
            lease.model_dump(mode="json") for lease in leases
        ],
        "orphaned_scheduler_leases": [
            {
                "lease_id": lease.lease_id,
                "scheduler_host": lease.scheduler_host,
                "owner": lease.owner,
                "reason": reason,
            }
            for lease in leases
            if (reason := scheduler_lease_orphaned_reason(lease)) is not None
        ],
        "scheduler_leases_error": lease_error,
        "legacy_scheduler_hosts": (
            sorted(legacy.scheduler_hosts) if legacy is not None else []
        ),
        "submit_policy": "deny" if state.reject_submits else "accept_pending",
        "remaining_seconds": _remaining_seconds(state),
        # False when the timer will leave a scheduler lane behind, so a machine
        # reader is not misled the way the text status was.
        "duration_releases_everything": (
            lease_error is None and not state.scheduler_hosts
        ),
        # None unless a vq-owned lane has outlived its process. Reported so
        # an operator mid-incident can tell an abandoned lane from a
        # deliberate hold without inferring it; vq never lifts either.
        "orphaned_lane_reason": orphaned_lane_reason(legacy),
        "state": payload,
    }


def format_status() -> str:
    """Human-readable summary for ``vq drain --status``."""
    legacy, leases, lease_error, state = read_effective_drain_snapshot()
    if state is None:
        return "drain: inactive (daemon dispatches normally)"
    if state.update_mode == "deny" or state.reject_submits:
        headline = "drain: PAUSED FOR UPDATE - denying new submissions"
    elif state.update_mode == "accept":
        headline = "drain: PAUSED FOR UPDATE - accepting jobs for later"
    else:
        headline = "drain: ACTIVE"
    parts = [f"{headline} since {state.set_at}"]
    if state.is_full_drain and state.scheduler_hosts:
        parts.append(
            "mode: full + scheduler-target (held: "
            f"{', '.join(sorted(state.scheduler_hosts))})"
        )
    elif state.scheduler_hosts and (
        state.max_jobs is not None or state.max_cpus is not None
    ):
        cap_parts = []
        if state.max_jobs is not None:
            cap_parts.append(f"max_jobs={state.max_jobs}")
        if state.max_cpus is not None:
            cap_parts.append(f"max_cpus={state.max_cpus}")
        parts.append(
            "mode: partial + scheduler-target (held: "
            f"{', '.join(sorted(state.scheduler_hosts))}; "
            f"{', '.join(cap_parts)})"
        )
    elif state.scheduler_hosts:
        parts.append(
            "mode: scheduler-target (held: "
            f"{', '.join(sorted(state.scheduler_hosts))})"
        )
    elif state.is_full_drain:
        parts.append("mode: full (no new dispatches)")
    else:
        cap_parts = []
        if state.max_jobs is not None:
            cap_parts.append(f"max_jobs={state.max_jobs}")
        if state.max_cpus is not None:
            cap_parts.append(f"max_cpus={state.max_cpus}")
        parts.append(f"mode: partial ({', '.join(cap_parts)})")
    if state.duration_seconds is not None:
        remaining = _remaining_seconds(state)
        if remaining is None:
            parts.append(f"duration={state.duration_seconds}s (bad set_at)")
        elif state.scheduler_hosts:
            # The timer releases the GLOBAL hold only. At expiry the
            # scheduler lanes are preserved and duration_seconds is nulled, so
            # the lane becomes permanently unbounded exactly when its countdown
            # runs out. Advertising a bare "auto-release in Ns" here told an
            # operator to wait for something that will never happen -- the same
            # shape that let the 2026-07-26 pbs-cluster lane leak sit unnoticed.
            parts.append(
                f"auto-release in {remaining}s (GLOBAL hold only; the "
                f"scheduler lane for {', '.join(sorted(state.scheduler_hosts))} "
                "persists and must be released by hand)"
            )
        else:
            parts.append(f"auto-release in {remaining}s")
    if state.reject_submits:
        parts.append("submit_policy: deny")
    else:
        parts.append("submit_policy: accept_pending")
    if state.reason:
        parts.append(f"reason: {state.reason}")
    orphaned = orphaned_lane_reason(legacy)
    if orphaned is not None:
        # Loud, and paired with the command that clears it. The operator reading
        # this is mid-incident: the host is `accept_pending`, taking submissions
        # and dispatching nothing, and the lane looks exactly like a deliberate
        # hold until something says otherwise.
        assert legacy is not None
        recovery_commands = []
        for host in sorted(legacy.scheduler_hosts):
            argv = [
                "vq",
                "drain",
                "--release",
                "--scheduler-host",
                host,
                "--release-legacy-only",
                "--expected-legacy-set-at",
                legacy.set_at,
            ]
            if legacy.reason is not None:
                argv.extend(["--expected-legacy-reason", legacy.reason])
            argv.append("localhost")
            recovery_commands.append(f"`{shlex.join(argv)}`")
        parts.append(
            f"ORPHANED LEGACY LANE: {orphaned}. Run on this status's daemon "
            "host; exact legacy-only recovery: "
            + "; ".join(recovery_commands)
        )
    for lease in leases:
        lease_orphaned = scheduler_lease_orphaned_reason(lease)
        label = (
            "ORPHANED SCHEDULER LEASE"
            if lease_orphaned is not None
            else "SCHEDULER LEASE"
        )
        detail = f": {lease_orphaned}." if lease_orphaned is not None else "."
        parts.append(
            f"{label}: host={lease.scheduler_host} owner={lease.owner} "
            f"lease_id={lease.lease_id}{detail} On this status's daemon host, "
            "release only this owner with `"
            + shlex.join(
                [
                    "vq",
                    "drain",
                    "--release",
                    "--scheduler-host",
                    lease.scheduler_host,
                    "--lease-owner",
                    lease.owner,
                    "localhost",
                ]
            )
            + "`"
        )
    if lease_error is not None:
        parts.append(f"SCHEDULER LEASE ERROR: {lease_error}")
    return " | ".join(parts)


def _pid_alive(pid: int) -> bool:
    if pid <= 0 or pid > 2_147_483_647:
        # Old legacy DrainState files did not constrain this field. Treat an
        # invalid value as indeterminate/alive rather than invoking process-
        # group semantics or crashing status on an OverflowError.
        return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, owned by someone else
    except (OSError, OverflowError):
        return True  # indeterminate -> assume alive, never call a live lane dead
    return True


def _pid_start_time(pid: int) -> int | None:
    """`/proc` field 22, or None off Linux."""
    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8") as fh:
            fields = fh.read().rsplit(")", 1)[-1].split()
        return int(fields[19])
    except (OSError, IndexError, ValueError):
        return None


def orphaned_lane_reason(state: DrainState | None) -> str | None:
    """Why a vq-owned drain lane looks abandoned, or None.

    Reports; never lifts. An operator's deliberate hold and a leaked one are
    identical on disk, and vq must not guess between them -- dropping a hold
    someone is relying on is worse than leaving a stale one visible. What this
    removes is the ambiguity: an operator seeing "its owner is gone" knows
    immediately that a release is safe, instead of inferring it.

    Conservative in the same direction as the admin-update marker's staleness
    check: indeterminate liveness reads as alive, and a lane with no recorded
    owner is treated as an operator hold rather than an orphan.
    """
    if state is None or not state.enabled or not state.scheduler_hosts:
        return None
    pid = state.owner_pid
    if pid is None:
        return None
    if not _pid_alive(pid):
        return (
            f"the `vq admin update` process (pid={pid}) that took this lane is "
            "no longer running"
        )
    recorded = state.owner_pid_start_time
    if recorded:
        live = _pid_start_time(pid)
        if live is not None and live != recorded:
            return (
                f"pid {pid} is alive but was recycled (start time {live} != "
                f"{recorded} recorded when the lane was taken), so the process "
                "holding this lane is a stranger"
            )
    return None


def scheduler_lease_orphaned_reason(
    lease: SchedulerDrainLease,
) -> str | None:
    """Return an owner-liveness diagnosis for one independent lease."""
    return orphaned_lane_reason(
        DrainState(
            scheduler_hosts=[lease.scheduler_host],
            reason=lease.reason,
            owner_pid=lease.owner_pid,
            owner_pid_start_time=lease.owner_pid_start_time,
        )
    )
