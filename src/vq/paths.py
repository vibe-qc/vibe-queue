"""Filesystem layout for vq daemon state, job workspaces, and config.

State root contains:

  queue/<jobid>.json   -- one spec file per job, regardless of state
  jobs/<jobid>/        -- per-job workspace (inputs unpacked, outputs written)
  daemon.pid           -- pidfile while the daemon is running
  daemon.log           -- daemon's own log

Override the state root with $VQ_STATE_DIR; otherwise XDG_DATA_HOME/vq, falling
back to ~/.local/share/vq. Likewise $VQ_CONFIG_DIR for the config root.

--- Multi-user mode (v0.6.x+) ---

When the daemon runs as root with multi-user enabled, state is laid out under
a system-wide root (default ``/var/lib/vq``) with per-user subdirectories:

  /var/lib/vq/
    users/
      <uid>/           -- one directory per Unix user
        queue/         -- that user's job specs
        jobs/          -- that user's workspaces
        archive/       -- that user's archives
        daemon.log     -- per-user daemon log (symlink or separate)
    daemon.pid         -- system-level pidfile
    daemon.log         -- system-level daemon log

Env vars: ``$VQ_MULTI_USER_ROOT`` overrides the system root (default
``/var/lib/vq``). The single-user ``$VQ_STATE_DIR`` env var is ignored in
multi-user mode (the daemon's state is always the multi-user root).
"""

from __future__ import annotations

import contextlib
import fcntl
import os
import re
import stat
import time
from collections.abc import Iterator
from pathlib import Path

from vq._storage import atomic_write_text as atomic_write_text
from vq.spec import validate_job_id

ENV_STATE_DIR = "VQ_STATE_DIR"
ENV_CONFIG_DIR = "VQ_CONFIG_DIR"
ENV_ARCHIVE_DIR = "VQ_ARCHIVE_DIR"
"""v0.5.22: override the default cleanup-archive location
(``<state_root>/archive/``). Useful when the user wants archives on a
different volume from the daemon state — common on small ``~`` partitions
with a big secondary disk. Also overridable per-policy via
``AutoCleanupPolicy.archive_dir`` (which trumps the env var for the
auto-cleanup pass)."""
ENV_MULTI_USER_ROOT = "VQ_MULTI_USER_ROOT"
"""v0.6.x: override the multi-user system root (default ``/var/lib/vq``).
Ignored in single-user mode."""
ENV_TEST_SANDBOX_ROOT = "VQ_TEST_SANDBOX_ROOT"
"""Internal pytest capability: every persistent root must resolve below it.

Production code never sets or needs this variable.  The vq conftest,
repository gate runner, and CI job set it before tests execute so even a
conftest-less child cannot inherit an operator's live path overrides.
"""

# Default system-level root for multi-user deployments.
_DEFAULT_MULTI_USER_ROOT = "/var/lib/vq"
_CANONICAL_UID_DIR_PATTERN = re.compile(r"^(?:0|[1-9][0-9]*)$")


class UnsafeImplicitTestPathError(RuntimeError):
    """Pytest attempted to resolve persistent state without an override.

    Test isolation is a process boundary, not a fixture convention. Pytest
    8.2+ publishes ``PYTEST_VERSION`` before collection and its children
    inherit that marker, so refusing unsafe roots here prevents every state
    consumer (including RPC clients) from reaching live daemon state.
    """


def _running_under_pytest() -> bool:
    """Return whether pytest owns this process or its process ancestry."""
    # PYTEST_CURRENT_TEST deliberately follows ordinary subprocesses.  Those
    # children do not import pytest, but they are still test-controlled code
    # and must not regain access to an implicit persistent root.  Tests that
    # intentionally exercise production default-path behaviour run children
    # with this marker removed and disposable HOME/XDG roots.
    return any(
        marker in os.environ
        for marker in ("PYTEST_CURRENT_TEST", "PYTEST_VERSION")
    )


def require_explicit_test_path(env_name: str, path_kind: str) -> None:
    """Require a test path to be explicit and contained by its sandbox."""
    if not _running_under_pytest():
        return
    value = os.environ.get(env_name)
    if not value:
        raise UnsafeImplicitTestPathError(
            f"refusing implicit {path_kind} while running under pytest: "
            f"set {env_name} to a temporary directory before Python starts. "
            "The vq test conftest and sanctioned gate runners do this; "
            "--noconftest bypasses fixtures and must never select live state."
        )
    require_test_path_within_sandbox(value, path_kind)


def require_test_path_within_sandbox(path: str | Path, path_kind: str) -> None:
    """Refuse a pytest-controlled persistent path outside its sandbox."""
    if not _running_under_pytest():
        return
    sandbox_value = os.environ.get(ENV_TEST_SANDBOX_ROOT)
    if not sandbox_value:
        raise UnsafeImplicitTestPathError(
            f"refusing test-controlled {path_kind} at {str(path)!r}: "
            f"{ENV_TEST_SANDBOX_ROOT} is missing. Run through the vq test "
            "conftest or a sanctioned gate runner."
        )
    sandbox = Path(sandbox_value).expanduser().resolve(strict=False)
    candidate = Path(path).expanduser().resolve(strict=False)
    if candidate != sandbox and sandbox not in candidate.parents:
        raise UnsafeImplicitTestPathError(
            f"refusing test-controlled {path_kind} outside the declared "
            f"sandbox: {candidate} is not beneath {sandbox} "
            f"({ENV_TEST_SANDBOX_ROOT})."
        )


def _is_root() -> bool:
    """True when the current process has effective UID 0 (root).
    Used to determine whether the daemon is running in multi-user mode."""
    return os.geteuid() == 0


def is_multi_user() -> bool:
    """Return True when the process is running as root and multi-user
    mode has been enabled via the config file ``[multi_user] enabled = true``.

    Callers that need the config-validated answer should use this after
    config has been loaded; callers that only care about path resolution
    can use :func:`_is_root` directly.
    """
    if not _is_root():
        return False
    # Lazy import to avoid circular dependency at module level.
    from vq.config import load_config

    cfg = load_config()
    return cfg.multi_user.enabled


def multi_user_root() -> Path:
    """System-level root for multi-user deployments.

    Resolution: ``$VQ_MULTI_USER_ROOT`` env var, else ``/var/lib/vq``.
    """
    require_explicit_test_path(ENV_MULTI_USER_ROOT, "vq multi-user state root")
    if env := os.environ.get(ENV_MULTI_USER_ROOT):
        return Path(env).expanduser()
    return Path(_DEFAULT_MULTI_USER_ROOT)


def users_root() -> Path:
    """Top-level directory holding per-user state dirs."""
    return multi_user_root() / "users"


def user_dir(uid: int | str) -> Path:
    """Per-user state directory: ``<users_root>/<uid>/``."""
    return users_root() / str(uid)


def _safe_job_id(jobid: str) -> str:
    """Validate a job id before it becomes any local path component."""
    return validate_job_id(jobid)


def user_queue_dir(uid: int | str) -> Path:
    """Queue dir for a specific user in multi-user mode."""
    return user_dir(uid) / "queue"


def user_jobs_dir(uid: int | str) -> Path:
    """Jobs dir for a specific user in multi-user mode."""
    return user_dir(uid) / "jobs"


def user_archive_dir(uid: int | str) -> Path:
    """Archive dir for a specific user in multi-user mode."""
    return user_dir(uid) / "archive"


def user_spec_path(uid: int | str, jobid: str) -> Path:
    """Spec path for a user's job in multi-user mode."""
    return user_queue_dir(uid) / f"{_safe_job_id(jobid)}.json"


def user_workspace_dir(uid: int | str, jobid: str) -> Path:
    """Workspace path for a user's job in multi-user mode."""
    return user_jobs_dir(uid) / _safe_job_id(jobid)


def user_workdir_root(uid: int | str) -> Path:
    """v0.6.54: per-job scratch workdir root for a user in multi-user
    mode (``<users_root>/<uid>/workdirs/``).

    Distinct from ``user_jobs_dir`` (which holds the submitted
    workspace = ``cwd``). Workdirs are operator-visible scratch:
    long-lived enough that the chat can read results back, but
    swept by the v0.6.54 cleanup pass after a configurable max-age."""
    return user_dir(uid) / "workdirs"


def user_workdir(uid: int | str, jobid: str) -> Path:
    """Per-job workdir for a user in multi-user mode."""
    return user_workdir_root(uid) / _safe_job_id(jobid)


class UnsafeMultiUserStateError(RuntimeError):
    """A managed multi-user state path violates a structural invariant."""


def ensure_multi_user_root() -> Path:
    """Create or validate the multi-user control/state root.

    The documented deployment uses root:vq-admins mode 2775 so trusted admins
    can write control-plane records. Preserve that group-write/setgid policy,
    but require a real daemon-owned final component and remove world write.
    Operator-selected ancestors remain part of the deployment trust boundary.
    """
    state_root = multi_user_root()
    try:
        state_stat = os.lstat(state_root)
    except FileNotFoundError:
        state_root.mkdir(parents=True, exist_ok=False)
        state_stat = os.lstat(state_root)
    if not stat.S_ISDIR(state_stat.st_mode):
        raise UnsafeMultiUserStateError(
            f"unsafe multi-user state root {state_root}: expected a real directory"
        )
    if state_stat.st_uid != os.geteuid():
        raise UnsafeMultiUserStateError(
            f"unsafe multi-user state root {state_root}: owner uid "
            f"{state_stat.st_uid} does not match provisioning uid {os.geteuid()}"
        )
    state_mode = stat.S_IMODE(state_stat.st_mode)
    hardened_state_mode = state_mode & ~0o002
    if hardened_state_mode != state_mode:
        os.chmod(state_root, hardened_state_mode)
    return state_root


def ensure_users_root() -> Path:
    """Create or validate the trusted root of every per-user state tree.

    The group-writable state/control root belongs to the explicitly trusted
    admin boundary. Its ``users`` child is a narrower structural boundary: it
    must be a real daemon-owned directory with no group/world write. This
    prevents daemon startup and provisioning from following a pre-positioned
    symlink at either managed final component.
    """
    state_root = ensure_multi_user_root()
    state_stat = os.lstat(state_root)

    root = users_root()
    try:
        root_stat = os.lstat(root)
    except FileNotFoundError:
        root.mkdir(exist_ok=False)
        root_stat = os.lstat(root)
    if not stat.S_ISDIR(root_stat.st_mode):
        raise UnsafeMultiUserStateError(
            f"unsafe multi-user users root {root}: expected a real directory"
        )
    if root_stat.st_uid != state_stat.st_uid:
        raise UnsafeMultiUserStateError(
            f"unsafe multi-user users root {root}: owner uid {root_stat.st_uid} "
            f"does not match state-root owner uid {state_stat.st_uid}"
        )
    root_mode = stat.S_IMODE(root_stat.st_mode)
    hardened_root_mode = root_mode & ~0o022
    if hardened_root_mode != root_mode:
        os.chmod(root, hardened_root_mode)
    return root


def provision_user_state(uid: int, gid: int) -> None:
    """v0.6.x: create a user's multi-user state tree
    (``<users_root>/<uid>/`` plus ``queue/`` ``jobs/`` ``archive/``).
    The structural ``<uid>/`` parent stays owned by the provisioning process
    (root in production) with the target user's primary group, while its
    writable children belong to ``uid:gid``.

    The caller must be root — the parent ``users/`` dir is
    root-owned, so an unprivileged user cannot create their own
    subtree (the v0.6.x submit-side bootstrap gap). The multi-user
    daemon calls this at startup for each admin-group member so an
    admin's first ``vq submit`` lands in a writable, correctly-owned
    state dir instead of hitting a PermissionError.

    Idempotent: existing real directories are retained and ownership is
    re-applied.  A symlink or non-directory managed entry fails closed before
    any chown can follow it.  Root-owning the structural parent first prevents
    the user from swapping a checked child before its ownership update."""
    ensure_users_root()
    structural = user_dir(uid)
    try:
        structural_stat = os.lstat(structural)
    except FileNotFoundError:
        structural.mkdir(exist_ok=False)
        structural_stat = os.lstat(structural)
    if not stat.S_ISDIR(structural_stat.st_mode):
        raise UnsafeMultiUserStateError(
            f"unsafe multi-user state root {structural}: expected a real directory"
        )

    # In production this is root:<target gid>.  Using the provisioning process
    # identity rather than a literal uid keeps direct unit tests unprivileged.
    os.chown(
        structural,
        os.geteuid(),
        gid,
        follow_symlinks=False,
    )
    # The target user must retain search permission after losing ownership of
    # the structural parent without exposing the numeric tree to other users.
    os.chmod(structural, 0o750)

    writable_children = [
        user_queue_dir(uid),
        user_jobs_dir(uid),
        user_archive_dir(uid),
        user_workdir_root(uid),  # v0.6.54
    ]
    for child in writable_children:
        try:
            child_stat = os.lstat(child)
        except FileNotFoundError:
            child.mkdir(exist_ok=False)
            child_stat = os.lstat(child)
        if not stat.S_ISDIR(child_stat.st_mode):
            raise UnsafeMultiUserStateError(
                f"unsafe managed multi-user path {child}: expected a real directory"
            )
        os.chown(child, uid, gid, follow_symlinks=False)
        # The uid directory is the authority for which user supplied a spec.
        # A group/world-writable managed child would let a peer inject a record
        # under that authority. Keep every other existing permission bit.
        child_mode = stat.S_IMODE(child_stat.st_mode)
        hardened_mode = child_mode & ~0o022
        if hardened_mode != child_mode:
            os.chmod(child, hardened_mode)


def xdg_state_root() -> Path:
    """The per-user XDG state root, ignoring ``$VQ_STATE_DIR``.

    Split out from :func:`state_root` for the one caller that must reach a
    *specific* user daemon rather than "whatever this process is pointed at":
    post-restart provenance verification. Admins on a multi-user host are
    documented to run admin verbs as ``VQ_STATE_DIR=/var/lib/vq vq admin ...``
    (``docs/state_file_audit.md``), which makes ``state_root()`` the multi-user
    root — so a "single-user" socket derived from it is in fact the root
    daemon's. Everything else should keep using :func:`state_root`, which
    honours the override on purpose.
    """
    require_explicit_test_path("XDG_DATA_HOME", "per-user XDG state root")
    xdg = os.environ.get("XDG_DATA_HOME", "~/.local/share")
    return Path(xdg).expanduser() / "vq"


def state_root() -> Path:
    require_explicit_test_path(ENV_STATE_DIR, "per-user vq state root")
    if env := os.environ.get(ENV_STATE_DIR):
        return Path(env).expanduser()
    return xdg_state_root()


def config_dir() -> Path:
    require_explicit_test_path(ENV_CONFIG_DIR, "per-user vq config root")
    if env := os.environ.get(ENV_CONFIG_DIR):
        return Path(env).expanduser()
    xdg = os.environ.get("XDG_CONFIG_HOME", "~/.config")
    return Path(xdg).expanduser() / "vq"


def queue_dir() -> Path:
    return state_root() / "queue"


def jobs_dir() -> Path:
    return state_root() / "jobs"


def workdir_root() -> Path:
    """v0.6.54: single-user workdir root (``<state_root>/workdirs/``)."""
    return state_root() / "workdirs"


def workdir_for(jobid: str) -> Path:
    """v0.6.54: per-job workdir (single-user)."""
    return workdir_root() / _safe_job_id(jobid)


def archive_dir() -> Path:
    """Default location for ``vq cleanup --archive`` tarballs.

    Resolution order (first match wins):
      1. ``$VQ_ARCHIVE_DIR`` env var (v0.5.22)
      2. ``<state_root>/archive`` (default; ``$VQ_STATE_DIR`` overrides
         this transitively)

    Keep the env var override at the function level so all archive
    paths — manual ``vq cleanup --archive``, auto-cleanup sweeps,
    archive-aware ``vq fetch``, the ``vq queue (archived)`` annotation —
    pick up the same location. ``AutoCleanupPolicy.archive_dir`` is a
    per-policy override consulted by the auto-cleanup pass and trumps
    both this default and the env var (so a policy author can park
    that env's archives on a different volume from another env's).
    """
    if env := os.environ.get(ENV_ARCHIVE_DIR):
        require_explicit_test_path(ENV_ARCHIVE_DIR, "vq archive root")
        return Path(env).expanduser()
    return state_root() / "archive"


def spec_path(jobid: str) -> Path:
    return queue_dir() / f"{_safe_job_id(jobid)}.json"


def workspace_dir(jobid: str) -> Path:
    return jobs_dir() / _safe_job_id(jobid)


def archive_path(jobid: str) -> Path:
    """Default tarball path for an archived job's workspace."""
    return archive_dir() / f"{_safe_job_id(jobid)}.tar.bz2"


def _all_user_dirs(*, state_root: Path | None = None) -> list[Path]:
    """List every per-user directory under ``users_root()``.

    Returns an empty list when ``users_root()`` doesn't exist
    (no users have submitted jobs yet). Sorted by uid for
    deterministic iteration order. ``state_root`` binds crash recovery to an
    explicitly persisted multi-user namespace instead of re-resolving a
    possibly changed ``VQ_MULTI_USER_ROOT`` override.
    """
    state_root = state_root or multi_user_root()
    try:
        state_stat = os.lstat(state_root)
    except FileNotFoundError:
        return []
    if not stat.S_ISDIR(state_stat.st_mode):
        raise UnsafeMultiUserStateError(
            f"unsafe multi-user state root {state_root}: expected a real directory"
        )
    if stat.S_IMODE(state_stat.st_mode) & 0o002:
        raise UnsafeMultiUserStateError(
            f"unsafe multi-user state root {state_root}: world writable"
        )

    root = state_root / "users"
    try:
        root_stat = os.lstat(root)
    except FileNotFoundError:
        return []
    if not stat.S_ISDIR(root_stat.st_mode):
        raise UnsafeMultiUserStateError(
            f"unsafe multi-user users root {root}: expected a real directory"
        )
    if root_stat.st_uid != state_stat.st_uid:
        raise UnsafeMultiUserStateError(
            f"unsafe multi-user users root {root}: owner uid {root_stat.st_uid} "
            f"does not match state-root owner uid {state_stat.st_uid}"
        )
    if stat.S_IMODE(root_stat.st_mode) & 0o022:
        raise UnsafeMultiUserStateError(
            f"unsafe multi-user users root {root}: group/world writable"
        )

    dirs = []
    for entry in sorted(root.iterdir()):
        if not _CANONICAL_UID_DIR_PATTERN.fullmatch(entry.name):
            if entry.name.isdigit():
                raise UnsafeMultiUserStateError(
                    f"unsafe multi-user uid entry {entry}: expected canonical "
                    "ASCII decimal spelling"
                )
            continue
        entry_stat = entry.stat(follow_symlinks=False)
        if not stat.S_ISDIR(entry_stat.st_mode):
            raise UnsafeMultiUserStateError(
                f"unsafe multi-user uid entry {entry}: expected a real directory"
            )
        dirs.append(entry)
    return dirs


def resolve_spec_path(
    jobid: str, *, multi_user: bool = False, uid: int | str | None = None
) -> Path:
    """Resolve the spec path for ``jobid``.

    In multi-user mode with a known uid, returns the per-user path.
    In multi-user mode without a known uid, searches all user dirs
    and raises FileNotFoundError if the job isn't found.
    In single-user mode, returns the legacy queue-dir path.
    """
    if multi_user:
        if uid is not None:
            return user_spec_path(uid, jobid)
        # Search all user dirs for the job. A bare id is a compatibility
        # convenience, not an ownership key: if more than one user has the
        # same id, selecting either record would let iteration order redirect
        # a read or mutation to the wrong owner.
        matches: list[Path] = []
        for user_dir_path in _all_user_dirs():
            candidate = user_spec_path(user_dir_path.name, jobid)
            if candidate.exists():
                matches.append(candidate)
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            raise ValueError(
                f"ambiguous job id {jobid!r}: multiple per-user specs exist"
            )
        raise FileNotFoundError(f"no such job: {jobid}")
    return spec_path(jobid)


def daemon_pidfile(*, multi_user: bool = False) -> Path:
    """Pidfile path. In multi-user mode, lives at the system root."""
    if multi_user:
        return multi_user_root() / "daemon.pid"
    return state_root() / "daemon.pid"


def daemon_logfile(*, multi_user: bool = False) -> Path:
    """Daemon log path. In multi-user mode, lives at the system root."""
    if multi_user:
        return multi_user_root() / "daemon.log"
    return state_root() / "daemon.log"


ADMIN_UPDATE_LOG_DIRNAME = "admin-updates"
"""Directory of per-update transcripts under the state root.

Before this existed, the full output of an update lived only in memory: it was
truncated to its last 80 lines into ``admin-status.json`` and the rest was
dropped when the process exited. For the scheduler lanes not even that — pbs-cluster
and slurm-cluster deploy output was unrecoverable the moment ``vq admin update``
returned. Diagnosing the 2026-07-22 incident meant re-running two-hour builds
to see what they had said."""

ADMIN_UPDATE_LOGS_TO_KEEP = 20
"""Transcripts retained per target before the oldest are pruned.

A bound is part of the feature, not a follow-up: ``daemon.log`` already grows
without limit on these hosts, and adding a second unbounded surface would be a
regression dressed as an improvement. Twenty covers well over a fleet release
cycle per target."""


def admin_update_log_dir(*, multi_user: bool = False) -> Path:
    """Directory holding per-update transcripts.

    Resolved through the same multi-user branch as :func:`daemon_logfile`:
    ``vq admin update`` runs as root on the multi-user hosts, so a naive
    ``state_root()`` would put the transcript in root's XDG directory instead
    of the shared state root — the same split that bit ``admin-status.json``.
    """
    root = multi_user_root() if multi_user else state_root()
    return root / ADMIN_UPDATE_LOG_DIRNAME


def _safe_log_component(value: str) -> str:
    """Filename-safe form of a config-supplied name.

    Every separator collapses to ``-``, so the result is always a single path
    component and a crafted target can never escape the log directory.
    """
    return "".join(c if (c.isalnum() or c in "-_.") else "-" for c in value) or "-"


def admin_update_log_target_dir(target: str, *, multi_user: bool = False) -> Path:
    """Directory holding one target's transcripts.

    Transcripts are namespaced by target **directory** rather than encoded into
    the filename. The first cut used ``<stamp>-<target>.log`` and matched it
    with a ``*-<target>.log`` glob, which is a suffix match: looking up
    ``vibeqc-dev`` also matched ``pbs-cluster-vibeqc-dev.log``, so
    ``vq admin logs vibeqc-dev`` could hand back a different target's
    transcript — and, far worse, the retention sweep for one target counted and
    **deleted** another's. A directory per target makes the match exact by
    construction.
    """
    return admin_update_log_dir(multi_user=multi_user) / _safe_log_component(target)


def admin_update_logfile(
    target: str, started_at: str, *, multi_user: bool = False
) -> Path:
    """Path for one update transcript.

    ``target`` is the env name, ``<host>`` for a scheduler helper update, or
    ``<host>-<program>`` for a runtime deployment; ``started_at`` is a UTC
    ISO-8601 timestamp.
    """
    stamp = _safe_log_component(started_at)
    return admin_update_log_target_dir(target, multi_user=multi_user) / f"{stamp}.log"


def prune_admin_update_logs(
    target: str,
    *,
    keep: int = ADMIN_UPDATE_LOGS_TO_KEEP,
    multi_user: bool = False,
) -> list[Path]:
    """Drop all but the newest ``keep`` transcripts for ``target``.

    Best-effort: returns the paths actually removed. Filenames are a sortable
    UTC timestamp, so lexical order is chronological order and no stat() is
    needed. Scoped to the target's own directory, so it can only ever delete
    that target's transcripts.
    """
    directory = admin_update_log_target_dir(target, multi_user=multi_user)
    try:
        matches = sorted(directory.glob("*.log"))
    except OSError:
        return []
    removed: list[Path] = []
    for path in matches[: max(0, len(matches) - keep)]:
        try:
            path.unlink()
        except OSError:
            continue
        removed.append(path)
    return removed


def config_file() -> Path:
    return config_dir() / "config.toml"


def spec_lock_path(spec_path: Path) -> Path:
    """Path of the sidecar advisory-lock file used by :func:`spec_lock`.

    Exposed so a caller that *deletes* a spec (``vq cleanup --delete``) can
    remove the ``<spec>.lock`` sidecar too, rather than leaking one empty
    lock file per deleted job.
    """
    return spec_path.parent / (spec_path.name + ".lock")


class SpecLockTimeout(TimeoutError):
    """Raised by :func:`spec_lock` when a ``timeout`` is given and the lock
    cannot be taken in time. flock auto-releases on the holder's process
    death, so a timeout means a live-but-wedged or SIGSTOPped writer is
    holding it, not a crashed one."""


@contextlib.contextmanager
def spec_lock(spec_path: Path, *, timeout: float | None = None) -> Iterator[None]:
    """Exclusive advisory lock for a read-modify-write of one job spec.

    vq's spec store is one JSON file per job with many concurrent writers: the
    daemon loop, its in-process RPC thread, and every CLI verb (each a
    separate process). :func:`atomic_write_text` makes each individual write
    atomic against *readers*, but two writers each doing read -> mutate ->
    write still lose updates (last writer wins — e.g. ``vq kill`` writing
    KILLED while the daemon writes COMPLETED). Wrap the whole read -> mutate ->
    write in this lock so the sequence is atomic against other writers too::

        with paths.spec_lock(p):
            spec = JobSpec.read(p)
            if spec.is_terminal:
                return
            spec.state = JobState.KILLED
            spec.write(p)

    Implementation notes:

    * The lock is a *sidecar* file ``<spec>.lock`` with a stable inode.
      Locking the spec file itself would not work: :func:`atomic_write_text`
      replaces the spec inode on every write (tmp + rename), so writers would
      each hold ``flock`` on a different inode and never exclude each other.
    * ``flock`` is advisory and released on close / process death, so a
      crashed or kill -9'd holder never wedges the lock.
    * Hold it only across the read -> mutate -> write, never across a
      subprocess / Popen / network call — other writers (and ``vq status``)
      block on it.
    * Multi-user: the daemon runs as root and a job's owner runs the CLI;
      both must be able to take the lock. We create the lock file world-RW-
      openable (subject to umask) and fall back to an ``O_RDONLY`` open —
      which is sufficient for ``flock(LOCK_EX)`` — when another principal
      created it without giving us write permission.

    Never nests (no writer holds two spec locks at once), so there is no lock
    ordering and no deadlock against the daemon-singleton lock.

    ``timeout`` (v0.12.0, seconds): ``None`` (default) keeps the blocking
    ``flock(LOCK_EX)`` the daemon's hot-path writers rely on. A float bounds
    the acquire with a ``LOCK_NB`` poll and raises :class:`SpecLockTimeout`
    on expiry, so an interactive verb fails fast instead of hanging behind a
    wedged holder.
    """
    lock_path = spec_lock_path(spec_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o666)
    except PermissionError:
        # Another principal created the lock without granting us write; an
        # O_RDONLY fd is enough to take an exclusive flock on it.
        fd = os.open(str(lock_path), os.O_RDONLY)
    try:
        if timeout is None:
            fcntl.flock(fd, fcntl.LOCK_EX)
        else:
            # Bounded acquire: poll LOCK_NB to a monotonic deadline so a
            # stuck or paused holder raises instead of wedging us forever.
            _deadline = time.monotonic() + timeout
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= _deadline:
                        raise SpecLockTimeout(
                            f"could not acquire the spec lock {lock_path.name} "
                            f"within {timeout:g}s. Another process is holding "
                            f"it (a stuck or paused writer), and flock would "
                            f"otherwise block forever."
                        ) from None
                    time.sleep(0.05)
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
