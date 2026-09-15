"""Soft CPU-priority control via cgroup ``CPUWeight``.

``vq throttle <jobid> --weight 20`` lowers the CPU priority of a running
job's systemd-run scope without killing or pausing it. Under contention
with a default-priority process (the kids' game, an interactive
workload, an urgent job), vq's share of the disputed cores is roughly
``weight / (weight + 100)``. When nothing else wants CPU, the
throttled job still uses everything available — that's the "soft" in
soft throttle vs. ``vq pause``'s hard freeze.

``--restore`` resets CPUWeight to systemd's default (100).
``--all`` applies to every RUNNING job (SUSPENDED jobs are skipped --
they're already not using CPU; restore them via ``vq resume``).

Key design decisions vs the v0.5.13 entry in docs/roadmap_history.md:

* **Doesn't persist across new dispatches.** A throttle applied via
  ``vq throttle --all --weight 20`` affects only the jobs that were
  running at the time. New jobs dispatched after the throttle call
  start at CPUWeight=100 again. Persistent throttle would need a
  daemon-level "current throttle level" state; deferred to v0.6.x
  if/when the use case shows up.

* **cgroup-only in v0.5.13.** The fallback ``renice -n N -g pgid``
  for non-cgroup hosts is sketched in that entry but not implemented
  here -- the only production target (workstation) is cgroup-enforced,
  and macOS doesn't need throttle (single-user dev box). When a
  second non-cgroup target appears, we add the renice path.

* **No state on the spec.** Unlike pause, throttle doesn't introduce
  a new job-state value or stamp the spec with ``throttled_at``. The
  cgroup property is the source of truth; ``systemctl --user show``
  reveals current CPUWeight if you really need to inspect.

The error model mirrors ``vq.pause_resume``: ``ThrottleError`` for any
non-fatal user-visible issue (wrong state, no pgid, cgroup
unavailable). The CLI translates these to ``click.UsageError`` /
``click.ClickException``.
"""
from __future__ import annotations

import contextlib
import json
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

from vq import cgroup, events, paths
from vq.host import is_local_host
from vq.spec import JobSpec, JobState, utcnow_iso

# Default CPUWeight per systemd.resource-control(5).
DEFAULT_CPU_WEIGHT = 100

# Range that systemd accepts. Anything outside trips set-property's own
# validator with a non-zero exit; we pre-validate so the user gets a
# clearer error message.
MIN_CPU_WEIGHT = 1
MAX_CPU_WEIGHT = 10_000

# v0.5.21: renice fallback for non-cgroup hosts. The Linux nice range is
# [-20, 19]; positive nice = lower priority. Unprivileged users can
# raise their own nice (lower priority) freely; lowering nice (higher
# priority) requires CAP_SYS_NICE / root, so negative-nice attempts may
# return rc=1 with "Permission denied".
NICE_MIN = -20
NICE_MAX = 19


class ThrottleError(RuntimeError):
    """Raised for any user-facing throttle failure. The CLI translates
    these into ``click.UsageError``."""


def _scope_name(jobid: str) -> str:
    """Match what ``daemon._start_job`` passed to ``cgroup.wrap_command``."""
    return f"vq-job-{jobid}.scope"


def _weight_to_nice(weight: int) -> int:
    """Heuristic CPUWeight (1-10000) -> renice (-20..19) mapping for the
    v0.5.21 non-cgroup fallback path.

    cgroup ``CPUWeight`` and POSIX ``nice`` aren't directly convertible
    (the former is a CFS cgroup share; the latter is a per-process
    priority bias). The fallback covers the common cases:

      weight=100 (default)  -> nice=0   (default priority)
      weight in [50, 99]    -> nice=5   (gentle de-prioritize)
      weight in [20, 49]    -> nice=10  (typical "step aside" target)
      weight in [10, 19]    -> nice=15  (deep de-prioritize)
      weight in [1, 9]      -> nice=19  (lowest legal priority)
      weight in [101, 199]  -> nice=-2  (slight boost; may need root)
      weight in [200, 499]  -> nice=-5  (boost; usually needs root)
      weight >= 500         -> nice=-10 (deeper boost; needs root)

    Deep precision should run under cgroup mode; the renice fallback is
    a "something rather than nothing" coverage for hosts without
    ``systemd-run --user --scope`` delegation."""
    if weight == DEFAULT_CPU_WEIGHT:
        return 0
    if weight < DEFAULT_CPU_WEIGHT:
        if weight >= 50:
            return 5
        if weight >= 20:
            return 10
        if weight >= 10:
            return 15
        return NICE_MAX
    # weight > 100: boost path
    if weight >= 500:
        return -10
    if weight >= 200:
        return -5
    return -2


def _renice_pgid(pgid: int, nice_level: int) -> tuple[bool, str]:
    """Run ``renice -n <nice> -g <pgid>``. Returns (ok, message).

    On success the message is a one-line confirmation; on failure it's
    the renice stderr (which usually tells you whether the issue is
    permission, missing pgid, or a malformed arg)."""
    if not (NICE_MIN <= nice_level <= NICE_MAX):
        return False, f"nice level {nice_level} out of range [{NICE_MIN}, {NICE_MAX}]"
    try:
        proc = subprocess.run(
            ["renice", "-n", str(nice_level), "-g", str(pgid)],
            capture_output=True,
            text=True,
            timeout=10,
            stdin=subprocess.DEVNULL,
        )
    except (subprocess.TimeoutExpired, OSError) as e:
        return False, f"renice invocation failed: {e}"
    if proc.returncode != 0:
        msg = (proc.stderr or proc.stdout or "").strip()
        return False, msg or f"renice exit {proc.returncode}"
    return True, f"renice -n {nice_level} -g {pgid}: ok"


def _apply_throttle(
    scope: str, pgid: int | None, weight: int, *, multi_user: bool = False,
) -> tuple[str, str]:
    """Try cgroup first; fall back to renice on non-cgroup hosts.
    Returns (path, detail) where path is "cgroup" or "renice".

    Raises ThrottleError if both paths are unavailable (cgroup off AND
    no pgid) or if the chosen path fails. Pure utility: caller is
    responsible for events/messaging.

    ``multi_user`` (v0.6.37): the job runs in a root-owned *system*
    scope. There is no ``--user`` delegation to probe and no renice
    fallback (a multi-user host runs systemd by construction) — go
    straight to the system-manager set-property."""
    if multi_user:
        if cgroup.set_cpu_weight(scope, weight, multi_user=True):
            return "cgroup", f"CPUWeight={weight}"
        raise ThrottleError(
            f"failed to set CPUWeight={weight} on {scope}; the job "
            "scope is a root-owned system scope — see the systemctl "
            "stderr in the log"
        )
    if cgroup.available():
        if cgroup.set_cpu_weight(scope, weight):
            return "cgroup", f"CPUWeight={weight}"
        raise ThrottleError(
            f"failed to set CPUWeight={weight} on {scope}; "
            "see daemon log for systemctl stderr"
        )
    # Non-cgroup fallback path.
    if pgid is None:
        raise ThrottleError(
            "cgroup enforcement unavailable AND no pgid recorded on "
            "the spec; cannot fall back to renice without a process "
            "group (pre-v0.3 spec or daemon-internal job?)"
        )
    nice = _weight_to_nice(weight)
    ok, msg = _renice_pgid(pgid, nice)
    if not ok:
        raise ThrottleError(
            f"cgroup unavailable; renice fallback failed: {msg}"
        )
    return "renice", f"nice={nice} (weight={weight} mapped)"


# ----------------------------------------------------------------------
# v0.5.15: persistent throttle across new dispatches
#
# Without persistence, ``vq throttle --all --weight 20`` only affects
# jobs that were running at the time of the call. A new job submitted
# afterwards starts at CPUWeight=100 — which is wrong for the
# kids-gaming-for-2-hours use case (the user expects throttle to apply
# to NEW submissions during that window too).
#
# State file lives at ``<state_root>/throttle.json``. Daemon's
# ``_start_job`` reads it after the systemd-run scope is created and
# applies CPUWeight to the new scope when present. Opt-in via the CLI's
# ``--persist`` flag (default is the v0.5.13 non-persistent behavior).
# ----------------------------------------------------------------------

THROTTLE_FILENAME = "throttle.json"

_PositiveDurationSeconds = Annotated[int, Field(strict=True, gt=0)]


class ThrottleState(BaseModel):
    """Persisted throttle state: the daemon applies this CPUWeight to
    every newly-dispatched scope while the file exists."""

    model_config = ConfigDict(extra="forbid")

    weight: int = Field(ge=MIN_CPU_WEIGHT, le=MAX_CPU_WEIGHT)
    """CPUWeight to apply to new scopes. Must be in
    [MIN_CPU_WEIGHT, MAX_CPU_WEIGHT]."""

    set_at: str = Field(default_factory=utcnow_iso)
    """ISO timestamp when persistence was set."""

    reason: str | None = None
    """Optional free-text label, surfaced via ``vq throttle --status``."""

    duration_seconds: _PositiveDurationSeconds | None = None
    """v0.5.16: optional auto-release after this many seconds elapsed
    from ``set_at``. Mirrors ``DrainState.duration_seconds``; same
    centralised expiry check inside the read function."""


def _throttle_state_from_read_mapping(
    state: dict[str, object],
    *,
    ignore_unknown: bool,
) -> ThrottleState | None:
    """Parse one stored/read throttle without dropping a valid throttle.

    New writers must provide a strict positive duration, but older or
    externally edited records may carry a value that the historical model
    coerced.  Duration is only an expiry hint, so neutralize that one invalid
    field to an unbounded throttle.  Every other validation failure retains
    the established corrupt-record result of ``None``.
    """
    if ignore_unknown:
        known = set(ThrottleState.model_fields)
        candidate = {key: value for key, value in state.items() if key in known}
    else:
        candidate = state
    try:
        return ThrottleState.model_validate(candidate)
    except ValueError:
        if "duration_seconds" not in candidate:
            return None
        without_expiry = dict(candidate)
        without_expiry["duration_seconds"] = None
        try:
            return ThrottleState.model_validate(without_expiry)
        except ValueError:
            return None


def throttle_state_path() -> Path:
    # v0.6.37: in multi-user mode the persistent-throttle file is a
    # daemon-wide setting shared by the root daemon and the (root)
    # `vq throttle --persist` CLI. It lives at the system root,
    # alongside daemon.pid / daemon.log — not under a single user's
    # ~/.local/share/vq.
    if paths.is_multi_user():
        return paths.multi_user_root() / THROTTLE_FILENAME
    return paths.state_root() / THROTTLE_FILENAME


def read_throttle_state(*, via_rpc: bool = True) -> ThrottleState | None:
    """Return the current persistent ThrottleState or None.

    Corrupt files are treated as no-persistence — the daemon should
    keep dispatching at default weight rather than crash on a parser
    error, same conservative policy as drain.read_drain_state(). An
    otherwise-valid legacy record with only a bad expiry remains active as an
    unbounded throttle instead of silently restoring full CPU weight.

    v0.5.16: auto-release check. Expired throttle.json is silently
    cleared on read.

    v0.8.1 *Karp's Reduction*: when ``via_rpc=True`` (the CLI default),
    the read is routed through the daemon's RPC socket — multi-user
    callers see the daemon's canonical view without needing root.
    Internal callers (daemon dispatch, RPC handlers, the
    ``apply_persistent_throttle_if_set`` hook called inside
    ``_start_job``) pass ``via_rpc=False`` to short-circuit the loop.
    """
    if via_rpc:
        from vq import rpc as _rpc  # noqa: PLC0415
        from vq.config import load_config

        try:
            mu = load_config().multi_user.enabled
        except Exception:  # noqa: BLE001
            mu = False
        result = _rpc.try_rpc_or_fallback(
            "get_throttle_state",
            multi_user=mu,
            fallback=lambda: read_throttle_state(via_rpc=False),
        )
        if result is None:
            return None
        if isinstance(result, ThrottleState):
            return result
        if isinstance(result, dict):
            return _throttle_state_from_read_mapping(
                result,
                ignore_unknown=True,
            )
        return None
    path = throttle_state_path()
    if not path.exists():
        return None
    try:
        with path.open() as f:
            data = json.load(f)
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    state = _throttle_state_from_read_mapping(data, ignore_unknown=False)
    if state is None:
        return None
    # Auto-expire check.
    if state.duration_seconds is not None:
        try:
            set_dt = datetime.fromisoformat(state.set_at)
            now = datetime.now(set_dt.tzinfo)
            if (now - set_dt).total_seconds() >= state.duration_seconds:
                clear_throttle_state(via_rpc=False)
                return None
        except (ValueError, TypeError):
            pass
    return state


def write_throttle_state(
    state: ThrottleState, *, via_rpc: bool = True,
) -> None:
    """Atomic write via tmpfile-then-rename. Mirrors drain.write_drain_state.

    v0.8.1: ``via_rpc=True`` (the CLI default) routes through the
    daemon's RPC. Multi-user mode then no longer requires the CLI to
    run as root — the daemon's RPC accept-loop is the writer, and
    the admin token gates access. Internal callers pass
    ``via_rpc=False`` for the direct write path.
    """
    if via_rpc:
        from vq import rpc as _rpc  # noqa: PLC0415
        from vq.config import load_config

        try:
            cfg = load_config()
            mu = cfg.multi_user.enabled
        except Exception:  # noqa: BLE001
            mu = False
        token: str | None = None
        if mu:
            from vq import auth as _auth  # noqa: PLC0415
            token = _auth.resolve_token(None)
        try:
            _rpc.call(
                "set_throttle_state",
                {"state": state.model_dump(mode="json"), "token": token},
                multi_user=mu,
            )
            return
        except (_rpc.RPCError, ConnectionError) as e:
            if mu:
                import logging as _logging
                _logging.getLogger(__name__).warning(
                    "RPC set_throttle_state failed (%s); falling back "
                    "to direct file write — multi-user mode may now "
                    "diverge from the daemon's canonical view.", e,
                )
    path = throttle_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(state.model_dump_json(indent=2))
    tmp.replace(path)


def replace_throttle_state_from_mapping(state: dict[str, object]) -> int:
    """Validate and directly replace one daemon-owned throttle state.

    Unknown fields are stripped for mixed-version compatibility before the
    ordinary direct writer performs its atomic replace. Weight retains its
    existing Pydantic coercion behavior; duration is a strict positive integer.
    The return value is the persisted weight used by the transport response.
    """
    known = set(ThrottleState.model_fields.keys())
    filtered = {key: value for key, value in state.items() if key in known}
    new_state = ThrottleState.model_validate(filtered)
    write_throttle_state(new_state, via_rpc=False)
    return new_state.weight


def clear_throttle_state(*, via_rpc: bool = True) -> bool:
    """Remove the throttle state file. Idempotent: returns True if a
    file was removed, False if there was nothing to remove.

    v0.8.1: ``via_rpc=True`` (the CLI default) routes the clear through
    the daemon's RPC (``set_throttle_state(state=None)``). Internal
    callers (daemon's own auto-expire, RPC handlers) pass
    ``via_rpc=False`` for direct unlink.
    """
    if via_rpc:
        from vq import rpc as _rpc  # noqa: PLC0415
        from vq.config import load_config

        try:
            cfg = load_config()
            mu = cfg.multi_user.enabled
        except Exception:  # noqa: BLE001
            mu = False
        token: str | None = None
        if mu:
            from vq import auth as _auth  # noqa: PLC0415
            token = _auth.resolve_token(None)
        try:
            result = _rpc.call(
                "set_throttle_state",
                {"state": None, "token": token},
                multi_user=mu,
            )
            if isinstance(result, dict):
                return bool(result.get("cleared", False))
            return False
        except (_rpc.RPCError, ConnectionError) as e:
            if mu:
                import logging as _logging
                _logging.getLogger(__name__).warning(
                    "RPC clear_throttle_state failed (%s); falling "
                    "back to direct file unlink.", e,
                )
    path = throttle_state_path()
    if not path.exists():
        return False
    with contextlib.suppress(FileNotFoundError):
        path.unlink()
    return True


def format_throttle_status() -> str:
    """Human-readable summary for ``vq throttle --status``."""
    state = read_throttle_state()
    if state is None:
        return (
            f"persistent throttle: inactive (new jobs dispatch at default "
            f"CPUWeight={DEFAULT_CPU_WEIGHT})"
        )
    parts = [
        f"persistent throttle: ACTIVE (CPUWeight={state.weight}) "
        f"since {state.set_at}"
    ]
    if state.duration_seconds is not None:
        try:
            set_dt = datetime.fromisoformat(state.set_at)
            now = datetime.now(set_dt.tzinfo)
            elapsed = (now - set_dt).total_seconds()
            remaining = max(0, int(state.duration_seconds - elapsed))
            parts.append(f"auto-release in {remaining}s")
        except (ValueError, TypeError):
            parts.append(f"duration={state.duration_seconds}s (bad set_at)")
    if state.reason:
        parts.append(f"reason: {state.reason}")
    return " | ".join(parts)


def apply_persistent_throttle_if_set(
    jobid: str, *, pgid: int | None = None, multi_user: bool = False,
) -> int | None:
    """Called by daemon._start_job after the job has launched. If
    persistent throttle is active, apply it via cgroup CPUWeight (when
    available) or via renice on the pgid (v0.5.21 non-cgroup fallback).
    Returns the applied weight (or None if no persistent state, or if
    the apply failed).

    ``pgid`` is the process group id of the freshly-started job — the
    daemon has it in hand by the time it calls us. Passing it in (vs
    reading the spec again) keeps this best-effort path fast and avoids
    a race where the apply runs before the spec write commits.

    ``multi_user`` (v0.6.37): the new job runs in a root-owned system
    scope — set CPUWeight on the system manager, no ``--user`` probe
    and no renice fallback.

    Best-effort: a failed apply doesn't fail the dispatch — the job
    runs at default weight and the user sees the dispatch event in
    events.jsonl as normal. The daemon log notes the failure.

    v0.8.1: ``via_rpc=False`` on the read — the daemon is calling its
    own state file directly (and per the v0.8.0 admin-status pattern,
    RPC-via-self would recurse into our own accept loop)."""
    state = read_throttle_state(via_rpc=False)
    if state is None:
        return None
    if multi_user:
        if cgroup.set_cpu_weight(_scope_name(jobid), state.weight, multi_user=True):
            return state.weight
        return None
    if cgroup.available():
        if cgroup.set_cpu_weight(_scope_name(jobid), state.weight):
            return state.weight
        return None
    # Non-cgroup fallback: renice the pgid.
    if pgid is None:
        return None
    nice = _weight_to_nice(state.weight)
    ok, _ = _renice_pgid(pgid, nice)
    if ok:
        return state.weight
    return None


def _require_root_for_multi_user(multi_user: bool) -> None:
    """v0.6.37: a multi-user job runs in a root-owned system scope, so
    adjusting its CPUWeight (or writing the daemon-wide persistent
    throttle file) requires root. Raise a clear ThrottleError instead
    of letting `systemctl set-property` fail with a raw permission
    error deeper down."""
    if multi_user and os.geteuid() != 0:
        raise ThrottleError(
            "vq throttle on a multi-user host adjusts root-owned "
            "system scopes — it must run as root (try: sudo vq "
            "throttle ...)"
        )


def throttle_job(
    host: str,
    jobid: str,
    weight: int,
    *,
    queue_dir: Path | None = None,
    multi_user: bool = False,
) -> str:
    """Set CPUWeight on the running job's scope to ``weight``.

    Returns a one-line human-readable message. Raises FileNotFoundError
    if the spec isn't there; ThrottleError for any non-RUNNING state,
    out-of-range weight, no pgid, or cgroup unavailable;
    NotImplementedError for non-local host (the CLI dispatcher handles
    cross-machine routing).

    ``multi_user`` (v0.6.37): resolve the spec from the per-user state
    dirs under ``/var/lib/vq/users/<uid>/`` and drive the job's
    root-owned system scope. Requires root.
    """
    if not is_local_host(host):
        raise NotImplementedError(
            f"throttle for {host!r}: CLI must dispatch via SSH; "
            "this function is local-only"
        )
    if not (MIN_CPU_WEIGHT <= weight <= MAX_CPU_WEIGHT):
        raise ThrottleError(
            f"CPUWeight {weight} out of range "
            f"[{MIN_CPU_WEIGHT}, {MAX_CPU_WEIGHT}]"
        )
    _require_root_for_multi_user(multi_user)
    if multi_user:
        # Searches every per-user queue dir; raises FileNotFoundError
        # if the job isn't found in any of them.
        spec_path = paths.resolve_spec_path(jobid, multi_user=True)
    else:
        queue_dir = queue_dir or paths.queue_dir()
        spec_path = queue_dir / f"{jobid}.json"
        if not spec_path.exists():
            raise FileNotFoundError(f"no such job: {jobid}")
    spec = JobSpec.read(spec_path)
    if spec.state != JobState.RUNNING:
        raise ThrottleError(
            f"job {jobid} is in state {spec.state.value}; only RUNNING jobs "
            "can be throttled (SUSPENDED jobs already use 0% CPU; "
            "use `vq resume` if you want them back)"
        )
    # v0.5.21: cgroup path with renice fallback. _apply_throttle handles
    # both; raises ThrottleError if both paths are unavailable.
    scope = _scope_name(jobid)
    path, detail = _apply_throttle(scope, spec.pgid, weight, multi_user=multi_user)

    events.append_event(
        Path(spec.cwd), events.EventKind.STATE_TRANSITION, jobid,
        **{
            "from": spec.state.value,
            "to": spec.state.value,
            "reason": f"vq throttle (CPUWeight={weight}, path={path})",
        },
    )
    qualifier = " (default)" if weight == DEFAULT_CPU_WEIGHT else ""
    if path == "cgroup":
        return f"throttled job {jobid} to CPUWeight={weight}{qualifier}"
    return (
        f"throttled job {jobid} via renice ({detail}); "
        f"cgroup unavailable on this host"
    )


def restore_job(
    host: str,
    jobid: str,
    *,
    queue_dir: Path | None = None,
    multi_user: bool = False,
) -> str:
    """Reset CPUWeight to systemd's default (100). Convenience wrapper
    around ``throttle_job(... weight=100)`` with a friendlier message."""
    throttle_job(
        host, jobid, DEFAULT_CPU_WEIGHT,
        queue_dir=queue_dir, multi_user=multi_user,
    )
    # Replace the throttle wording with restore wording for clarity.
    return f"restored job {jobid} to default CPUWeight={DEFAULT_CPU_WEIGHT}"


def throttle_all(
    host: str,
    weight: int,
    *,
    queue_dir: Path | None = None,
    multi_user: bool = False,
) -> str:
    """Apply ``weight`` to every RUNNING job's scope. Idempotent;
    SUSPENDED and terminal-state jobs are silently skipped.

    Returns a summary line like "throttled 4 jobs to CPUWeight=20
    (1 SUSPENDED skipped, 6 not running)". Useful when the user wants
    the whole queue to step aside for something else (a game, an
    interactive workload, an urgent job).

    ``multi_user`` (v0.6.37): sweep every per-user queue dir under
    ``/var/lib/vq/users/<uid>/`` — root throttles the whole host's
    jobs across all users.
    """
    if not is_local_host(host):
        raise NotImplementedError(
            f"throttle --all for {host!r}: CLI must dispatch via SSH"
        )
    if not (MIN_CPU_WEIGHT <= weight <= MAX_CPU_WEIGHT):
        raise ThrottleError(
            f"CPUWeight {weight} out of range "
            f"[{MIN_CPU_WEIGHT}, {MAX_CPU_WEIGHT}]"
        )
    _require_root_for_multi_user(multi_user)
    if multi_user:
        spec_paths: list[Path] = []
        for user_dir in paths._all_user_dirs():
            qd = paths.user_queue_dir(user_dir.name)
            if qd.is_dir():
                spec_paths.extend(sorted(qd.glob("*.json")))
    else:
        queue_dir = queue_dir or paths.queue_dir()
        if not queue_dir.exists():
            return f"throttled 0 jobs to CPUWeight={weight} (queue dir empty)"
        spec_paths = sorted(queue_dir.glob("*.json"))

    throttled: list[str] = []
    suspended: list[str] = []
    skipped: list[str] = []
    errors: list[tuple[str, str]] = []

    for spec_path in spec_paths:
        try:
            spec = JobSpec.read(spec_path)
        except Exception:
            continue
        if spec.state == JobState.SUSPENDED:
            suspended.append(spec.id)
            continue
        if spec.state != JobState.RUNNING:
            skipped.append(spec.id)
            continue
        try:
            throttle_job(
                host, spec.id, weight,
                queue_dir=queue_dir, multi_user=multi_user,
            )
            throttled.append(spec.id)
        except (ThrottleError, FileNotFoundError) as e:
            errors.append((spec.id, str(e)))

    return _format_throttle_summary(
        weight=weight,
        throttled=throttled,
        suspended=suspended,
        skipped=skipped,
        errors=errors,
    )


def restore_all(
    host: str,
    *,
    queue_dir: Path | None = None,
    multi_user: bool = False,
) -> str:
    """Reset CPUWeight=100 on every RUNNING job. Friendlier wrapper."""
    msg = throttle_all(
        host, DEFAULT_CPU_WEIGHT, queue_dir=queue_dir, multi_user=multi_user,
    )
    # Swap "throttled" wording for "restored" so the summary reads
    # naturally for the common "kids are done gaming, give me my queue
    # back" use case.
    return msg.replace(
        "throttled ", "restored "
    ).replace(
        f" to CPUWeight={DEFAULT_CPU_WEIGHT}",
        " to default CPUWeight",
    )


def _format_throttle_summary(
    *,
    weight: int,
    throttled: list[str],
    suspended: list[str],
    skipped: list[str],
    errors: list[tuple[str, str]],
) -> str:
    """One-line summary mirroring pause_resume._format_bulk_summary."""
    plural = "s" if len(throttled) != 1 else ""
    head = f"throttled {len(throttled)} job{plural} to CPUWeight={weight}"
    parts: list[str] = []
    if suspended:
        parts.append(f"{len(suspended)} SUSPENDED skipped")
    if skipped:
        parts.append(f"{len(skipped)} not running")
    if errors:
        plural2 = "s" if len(errors) != 1 else ""
        parts.append(f"{len(errors)} error{plural2}")
    if parts:
        return f"{head} ({'; '.join(parts)})"
    return head
