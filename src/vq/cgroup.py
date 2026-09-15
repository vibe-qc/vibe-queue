"""cgroups v2 enforcement via ``systemd-run --user --scope``.

This is the v0.4 piece that turns the v0.3 watchdog from "monitoring +
SIGTERM" into kernel-level enforcement for memory + CPU. When delegation
is configured on the user manager and ``systemd-run`` is on PATH, the
daemon wraps each dispatched job in a transient scope:

  systemd-run --user --scope --collect \\
      --property=MemoryMax=16384M    \\
      --property=MemoryHigh=14745M   \\
      --property=CPUQuota=400%       \\
      -- /path/to/python script.py

The kernel enforces in-cgroup: a memory overshoot kills the offender
in its own cgroup (not the host's OOM-killer free-for-all), and CPU is
shared per-quota.

**Wall-time is NOT enforced here** -- it lives in the Python watchdog
(see ``vq.watchdog``). v0.4..v0.5.7 attempted to use systemd's
``RuntimeMaxSec`` for wall-time, which broke pause/resume because
``RuntimeMaxSec`` is a wall-clock timer measured from scope-active and
cannot be mutated at runtime via ``systemctl --user set-property``
(systemd's set-property table only covers cgroup knobs in
``systemd.resource-control(5)``; time-based properties are not
runtime-mutable). v0.5.8 drops ``RuntimeMaxSec`` entirely so pause-aware
wall-time accounting can live in one place. See
``docs/wall_time_design.md`` for the longer story + future architecture
options.

Setup prerequisite: ``Delegate=cpu cpuset io memory pids`` on the user
manager. On most modern distros this means dropping a file at
``/etc/systemd/system/user@.service.d/delegate.conf`` (root-owned).
On hosts without that, ``available()`` returns False and the daemon
falls back to v0.3 ``/proc`` polling + signal kills for the resource
caps too.
"""
from __future__ import annotations

import logging
import shutil
import subprocess
from functools import lru_cache

log = logging.getLogger(__name__)


def _systemd_run_path() -> str | None:
    return shutil.which("systemd-run")


def systemd_run_on_path() -> bool:
    """v0.6.x: True iff the ``systemd-run`` binary is on PATH.

    Distinct from :func:`available` — that probes ``--user`` scope
    creation (memory delegation on the user manager). Multi-user
    dispatch does NOT use ``--user`` scopes; it uses system-mode
    ``--uid`` scopes, which only need the binary present and the
    daemon running as root. The multi-user startup guard uses this."""
    return _systemd_run_path() is not None


@lru_cache(maxsize=1)
def available() -> bool:
    """True iff cgroups v2 enforcement via ``systemd-run --user --scope``
    works on this host. Cached for the daemon's lifetime; restart to
    re-detect.

    The probe runs ``systemd-run --user --scope --quiet --collect
    --property=MemoryMax=10M -- /bin/true`` and checks the exit code.
    A successful run with no PermissionError / "Failed to set unit
    properties" means the user manager has memory delegation.
    """
    binpath = _systemd_run_path()
    if binpath is None:
        return False
    try:
        proc = subprocess.run(
            [
                binpath,
                "--user", "--scope", "--quiet", "--collect",
                "--property=MemoryMax=10M",
                "--",
                "/bin/true",
            ],
            capture_output=True,
            text=True,
            timeout=5.0,
            stdin=subprocess.DEVNULL,
        )
    except (subprocess.TimeoutExpired, OSError) as e:
        log.debug("cgroup probe failed: %s", e)
        return False
    if proc.returncode != 0:
        log.debug(
            "cgroup probe exit %d (stderr: %s)",
            proc.returncode, proc.stderr.strip(),
        )
        return False
    return True


def wrap_command(
    cmd: list[str],
    *,
    mem_mb: int | None = None,
    cpus: int | None = None,
    unit_name: str | None = None,
    run_as_uid: int | None = None,
    run_as_gid: int | None = None,
) -> list[str]:
    """Return a systemd-run-wrapped argv that runs ``cmd`` in a transient
    scope with the given memory + CPU caps, or ``cmd`` unchanged if
    any of: systemd-run not on PATH; delegation not configured; both
    caps are None **and** no ``unit_name`` is set (a non-job command).

    A job carries a ``unit_name`` (``vq-job-<id>``) and is scoped even
    with no caps: an accounting-only scope still gives the kernel cgroup
    hierarchy that captures setsid/PR_SET_PGID descendants the pgid walk
    misses, and lets the daemon reap the whole tree by stopping the unit.

    Properties applied:

    * ``mem_mb`` -> ``MemoryMax=<N>M`` (hard cap, kernel kills offender)
                  + ``MemoryHigh=<int(N*0.9)>M`` (soft, triggers reclaim
                  before MemoryMax fires; smoother behaviour for jobs
                  that sometimes spike near the cap)
    * ``cpus`` -> ``CPUQuota=<N*100>%``
    * ``unit_name`` without caps -> create a named scope for complete
      descendant accounting and cleanup, with no resource limit properties.

    Wall-time enforcement is intentionally NOT here -- the Python
    watchdog handles it (and is pause-aware). See module docstring.

    The caller passes ``cmd`` as the full argv (interpreter + script +
    args). The wrap is purely a prefix of length ~6 to ~10 args.

    v0.6.x multi-user privilege drop: when ``run_as_uid`` is set, the
    wrap runs the job in a **system-mode** transient scope owned by
    that uid/gid (``systemd-run --scope --uid=U --gid=G``, *without*
    ``--user``). A root multi-user daemon uses this to spawn every job
    as its submitter — never as root. In this mode the wrap is
    **mandatory**: it is applied even when no resource caps are set,
    and a missing ``systemd-run`` raises ``RuntimeError`` rather than
    silently returning a command that would run as root.
    """
    if run_as_uid is not None:
        # Multi-user: the wrap is the privilege-drop and is mandatory.
        # System-mode scope (no --user) + --uid/--gid; only root can
        # create these, which is exactly the multi-user daemon's case.
        binpath = _systemd_run_path()
        if binpath is None:
            raise RuntimeError(
                f"multi-user job dispatch requires systemd-run on PATH "
                f"to drop privileges to uid={run_as_uid}; it was not "
                f"found. Refusing to run the job as root."
            )
        args: list[str] = [
            binpath, "--scope", "--quiet", "--collect",
            f"--uid={run_as_uid}",
        ]
        if run_as_gid is not None:
            args.append(f"--gid={run_as_gid}")
        if unit_name:
            args.extend(["--unit", unit_name])
        if mem_mb is not None:
            args.append(f"--property=MemoryMax={mem_mb}M")
            args.append(f"--property=MemoryHigh={int(mem_mb * 0.9)}M")
        if cpus is not None:
            args.append(f"--property=CPUQuota={cpus * 100}%")
        args.append("--")
        args.extend(cmd)
        return args

    # A named job is scoped even capless (accounting-only) so the cgroup
    # captures pgid-escaping descendants. Only a capless, nameless
    # command runs unwrapped.
    if not (mem_mb or cpus or unit_name):
        return cmd
    if not available():
        return cmd
    binpath = _systemd_run_path()
    assert binpath is not None  # available() said yes

    args = [binpath, "--user", "--scope", "--quiet", "--collect"]
    if unit_name:
        args.extend(["--unit", unit_name])
    if mem_mb is not None:
        args.append(f"--property=MemoryMax={mem_mb}M")
        # Soft limit triggers reclaim ~10% before the hard kill.
        args.append(f"--property=MemoryHigh={int(mem_mb * 0.9)}M")
    if cpus is not None:
        # CPUQuota is given as percent of one CPU. 4 CPUs -> 400%.
        args.append(f"--property=CPUQuota={cpus * 100}%")

    args.append("--")
    args.extend(cmd)
    return args


def reset_availability_cache() -> None:
    """Clear the cached ``available()`` result. Called at daemon
    startup (v0.5.50) so a restart re-tests cgroup availability,
    and historically used by tests.

    Tolerant: tests may monkeypatch ``available`` with a plain
    function/lambda that has no ``cache_clear`` attribute. In that
    case this is a no-op rather than an AttributeError — the
    caller's intent ("forget any previously-cached availability")
    is already satisfied by the monkeypatch."""
    clear = getattr(available, "cache_clear", None)
    if clear is not None:
        clear()


# v0.5.38: cgroup-v2 cpu/memory readers for the watchdog.
#
# Background: until v0.5.37 the watchdog computed CPU% and RSS by
# summing /proc/<pid>/stat across processes whose pgid matches the
# job's dispatch pgid (``read_cputime_seconds_pgid`` in watchdog.py).
# That works for OMP / fork-without-exec children that inherit the
# pgid, but it MISSES descendants that escape via setsid /
# PR_SET_PGID — which ninja, mpirun, and several other build / launch
# tools do as a matter of course. In particular, a vibe-qc rebuild
# via ``pip install -e .`` sees ninja put every cc1plus in its own
# session; from the parent pgid's vantage CPU activity is silent for
# the entire 2-3 minute build, and the watchdog STARVED-kills the
# job at the 5-minute starve window even though 32 cc1plus instances
# are saturating the box.
#
# The fix: when the job runs under a cgroup-v2 scope (which it does
# whenever ``cgroup.available()`` is True — every modern vq host),
# read CPU + RSS straight from the scope's ``cpu.stat`` and
# ``memory.current`` files. The cgroup contains ALL descendants by
# kernel-managed hierarchy, so pgid escapes are invisible — the
# accounting is authoritative and immune to setsid tricks. The
# pgid-walk readers remain as a fallback for macOS dev hosts and
# Linux hosts without cgroup-v2 delegation.

CGROUP_FS_ROOT = "/sys/fs/cgroup"


def _cgroup_fs_path(raw: str) -> str | None:
    """Convert a cgroup-v2 path from procfs/systemd into a filesystem path."""
    rel = raw.strip()
    if not rel or rel == "-":
        return None
    if rel == "/":
        return CGROUP_FS_ROOT
    if rel.startswith("/"):
        return CGROUP_FS_ROOT + rel
    return f"{CGROUP_FS_ROOT}/{rel}"


def cgroup_path_for_pid(pid: int) -> str | None:
    """Return the absolute filesystem path of the cgroup containing
    ``pid``, or ``None`` if the lookup fails (process gone, /proc
    unreadable, non-cgroup-v2 host).

    Reads ``/proc/<pid>/cgroup`` (cgroup-v2 single-line format:
    ``0::/user.slice/user-1000.slice/.../vq-job-<id>.scope``),
    extracts the path part, and prepends ``/sys/fs/cgroup``. Cheap —
    one short text-file read.

    Notes:

    * cgroup-v1 puts multiple lines (one per controller) here; we
      only support v2, so any line with ``0::`` is what we want.
    * Containers / kubernetes pods can show a cgroup path like
      ``/kubepods.slice/...``; the underlying fs is the same so the
      returned path is still readable.
    * macOS has no ``/proc``; returns None there.
    """
    try:
        with open(f"/proc/{pid}/cgroup", encoding="utf-8") as f:
            for line in f:
                # cgroup-v2 line: "0::<path>"
                if line.startswith("0::"):
                    return _cgroup_fs_path(line[3:])
    except OSError:
        return None
    return None


def cgroup_path_for_scope(
    unit_name: str, *, multi_user: bool = False
) -> str | None:
    """Return the cgroup-v2 filesystem path for a transient scope unit.

    The watchdog normally wants the ``vq-job-<id>.scope`` ControlGroup,
    not merely the cgroup of the local ``Popen`` PID. With
    ``systemd-run --scope`` the Popen handle may be the ``systemd-run``
    client/waiter while the payload is tracked by systemd under the
    named scope. Querying ``ControlGroup`` makes the accounting anchor
    explicit and avoids sampling the wrapper instead of the job.
    """
    if not unit_name.endswith(".scope"):
        unit_name = unit_name + ".scope"
    binpath = _systemctl_path()
    if binpath is None:
        return None
    try:
        proc = subprocess.run(
            [
                *_systemctl_scope_argv(binpath, multi_user),
                "show",
                unit_name,
                "-p",
                "ControlGroup",
                "--value",
            ],
            capture_output=True,
            text=True,
            timeout=5,
            stdin=subprocess.DEVNULL,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0:
        return None
    return _cgroup_fs_path(proc.stdout or "")


def read_cpu_usage_seconds(cgroup_path: str) -> float | None:
    """Read the total CPU time in seconds for every task in
    ``cgroup_path`` (including all descendants), or ``None`` if the
    cgroup's ``cpu.stat`` is missing / unreadable.

    Reads the ``usage_usec`` line from ``<cgroup_path>/cpu.stat``;
    converts to seconds. This number is monotonic since cgroup
    creation — the watchdog's delta-of-delta math turns it into CPU%
    the same way it does for the pgid-walk sampler.

    Crucially, this aggregates across the full cgroup membership
    regardless of pgid / session — that's why this reader replaces
    the pgid-walk for the watchdog's CPU-activity heuristic. See the
    "v0.5.38" comment block above.
    """
    try:
        with open(f"{cgroup_path}/cpu.stat", encoding="utf-8") as f:
            for line in f:
                if line.startswith("usage_usec "):
                    try:
                        usec = int(line.split()[1])
                    except (IndexError, ValueError):
                        return None
                    return usec / 1_000_000.0
    except OSError:
        return None
    return None


def read_memory_current_mb(cgroup_path: str) -> int | None:
    """Read current memory usage in MB across every task in
    ``cgroup_path`` (or ``None`` if the cgroup's ``memory.current``
    is missing). Returns an integer megabytes value to match the
    units the watchdog uses for ``spec.mem_mb`` comparisons.

    Like ``cpu.stat``, ``memory.current`` aggregates across the whole
    cgroup membership and (usefully) avoids the shared-page double-
    count problem of the pgid-walk pid-sum. The two-line
    "sum-of-VmRSS" comment in ``read_rss_mb_pgid`` notes the
    double-count is fine for ceiling enforcement; the cgroup value
    is just better.
    """
    try:
        with open(f"{cgroup_path}/memory.current", encoding="utf-8") as f:
            raw = f.read().strip()
    except OSError:
        return None
    try:
        bytes_ = int(raw)
    except ValueError:
        return None
    # Round to whole MB to match the watchdog's mem_mb-vs-cap math.
    return bytes_ // (1024 * 1024)


def _systemctl_path() -> str | None:
    return shutil.which("systemctl")


def _systemctl_scope_argv(binpath: str, multi_user: bool) -> list[str]:
    """v0.6.x: base systemctl argv for managing a vq job's transient
    scope, picking the manager that actually owns the scope.

    Single-user job scopes live in the *user* manager
    (``systemd-run --user --scope``). Multi-user job scopes are
    *system* scopes (``systemd-run --scope --uid``, no ``--user``) —
    the root daemon creates them in the system manager. A
    ``systemctl --user`` call cannot see or stop a system scope, so
    the managing call must match the dispatch mode."""
    return [binpath] if multi_user else [binpath, "--user"]


def scope_main_pid(unit_name: str, *, multi_user: bool = False) -> int | None:
    """v0.6.0: query ``systemctl show <scope> -p MainPID --value``
    and return the int PID. ``multi_user`` (v0.6.x) selects the
    system manager — multi-user job scopes are system scopes, not
    ``--user`` scopes. Returns None when:
      * systemctl is unavailable or unreachable
      * the unit doesn't exist
      * MainPID is 0 (unit inactive)
      * the value isn't a parseable int

    Used by ``Daemon._reattach_or_interrupt_at_startup`` (v0.6.0) as
    a cgroup-anchored cross-check complementing the v0.5.50
    PID-fingerprint. If our spec.pid matches systemd's MainPID for
    the scope unit, we own the cgroup and the recorded pid IS the
    actual process. If not, the scope unit is detached/stale and
    the recorded PID may be unrelated."""
    if not unit_name.endswith(".scope"):
        unit_name = unit_name + ".scope"
    binpath = _systemctl_path()
    if binpath is None:
        return None
    try:
        proc = subprocess.run(
            [*_systemctl_scope_argv(binpath, multi_user), "show", unit_name,
             "-p", "MainPID", "--value"],
            capture_output=True,
            text=True,
            timeout=5,
            stdin=subprocess.DEVNULL,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0:
        return None
    raw = (proc.stdout or "").strip()
    if not raw:
        return None
    try:
        pid = int(raw)
    except ValueError:
        return None
    return pid if pid > 0 else None


def scope_exists(unit_name: str, *, multi_user: bool = False) -> bool | None:
    """v0.5.51: probe whether a transient scope unit already exists.
    ``multi_user`` (v0.6.x) targets the system manager — multi-user
    job scopes are system scopes, invisible to ``systemctl --user``.
    Returns:
      * True  — the scope is known to systemd (active OR failed-but-
        not-yet-collected).
      * False — systemctl is reachable and reports the unit doesn't
        exist (rc != 0 with stderr matching "not loaded" /
        "not-found" / "could not be found").
      * None  — systemctl is unavailable / unreachable / output
        unexpected; caller should treat as "can't tell" and proceed
        cautiously.

    Used by :class:`Daemon._start_job` to pre-flight against the
    "Unit already exists" failure mode (audit § 2e): a previous
    job's cgroup scope wasn't collected by --collect, so
    ``systemd-run --unit=vq-job-<id>.scope`` fails cryptically
    instead of producing a clean dispatch error.

    Suffix-tolerant: the helper appends ``.scope`` to ``unit_name``
    only if it isn't already there, so callers can pass either form.
    """
    if not unit_name.endswith(".scope"):
        unit_name = unit_name + ".scope"
    binpath = _systemctl_path()
    if binpath is None:
        return None
    try:
        proc = subprocess.run(
            [*_systemctl_scope_argv(binpath, multi_user), "show", unit_name,
             "-p", "LoadState", "--value"],
            capture_output=True,
            text=True,
            timeout=5,
            stdin=subprocess.DEVNULL,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0:
        return None
    raw = (proc.stdout or "").strip()
    # systemd LoadState values: loaded / not-found / error /
    # masked / stub. "not-found" is the canonical "no such unit"
    # signal; anything else implies the unit is at least known.
    if raw == "not-found":
        return False
    if raw == "":
        return None
    return True


def stop_scope(unit_name: str, *, multi_user: bool = False) -> bool:
    """v0.5.51: best-effort ``systemctl stop <scope>`` for cleaning
    up a leaked-but-known scope before re-dispatching the same
    jobid. ``multi_user`` (v0.6.x) targets the system manager —
    a multi-user job scope is a system scope and ``systemctl
    --user stop`` cannot reach it. Returns True on rc=0 (scope
    stopped or wasn't actually loaded), False otherwise.

    Conservative: not a replacement for ``--collect`` on the
    systemd-run call — just a recovery path for the
    "scope unit leaked and now collides on retry" edge case the
    :func:`scope_exists` pre-flight detects. Caller decides whether
    to retry the dispatch or land the spec FAILED."""
    if not unit_name.endswith(".scope"):
        unit_name = unit_name + ".scope"
    binpath = _systemctl_path()
    if binpath is None:
        return False
    try:
        proc = subprocess.run(
            [*_systemctl_scope_argv(binpath, multi_user), "stop", unit_name],
            capture_output=True,
            text=True,
            timeout=10,
            stdin=subprocess.DEVNULL,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return proc.returncode == 0


def set_cpu_weight(
    scope_name: str, weight: int, *, multi_user: bool = False
) -> bool:
    """Set ``CPUWeight`` on a live transient scope at runtime.

    Used by ``vq throttle`` to soft-deprioritise a running job's cgroup
    without killing or pausing it. ``CPUWeight`` IS runtime-mutable per
    ``systemd.resource-control(5)`` (unlike ``RuntimeMaxSec``, which
    bit v0.5.7 — see ``docs/wall_time_design.md``).

    Default systemd value is 100. Range 1-10_000. Lower means less CPU
    under contention; when no other process wants CPU, the scope still
    uses everything available — that's the "soft" in soft throttle vs.
    pause's hard freeze.

    Returns True on success, False on any failure (no systemctl, scope
    is gone, set-property rejected). Caller logs but does not raise --
    throttle is best-effort; a failed call leaves the previous weight
    intact, which is acceptable degraded behavior.

    ``multi_user`` (v0.6.37): a multi-user job runs in a *system*
    scope, managed by the system manager (``systemctl``, no
    ``--user``) and writable only by root. ``available()`` probes
    ``--user`` scope delegation, which is irrelevant to a system
    scope — so the gate is skipped in multi-user mode, and the
    set-property call targets the system manager. In single-user
    mode this is still a no-op when ``available()`` is False (macOS
    dev box, or Linux without delegation).
    """
    if not multi_user and not available():
        return False
    binpath = _systemctl_path()
    if binpath is None:
        return False
    try:
        proc = subprocess.run(
            [
                *_systemctl_scope_argv(binpath, multi_user),
                "set-property",
                scope_name,
                f"CPUWeight={weight}",
            ],
            capture_output=True,
            text=True,
            timeout=5.0,
            stdin=subprocess.DEVNULL,
        )
    except (subprocess.TimeoutExpired, OSError) as e:
        log.warning("set-property %s CPUWeight failed: %s", scope_name, e)
        return False
    if proc.returncode != 0:
        log.warning(
            "set-property %s CPUWeight=%d exit %d (stderr: %s)",
            scope_name, weight, proc.returncode, proc.stderr.strip(),
        )
        return False
    return True
