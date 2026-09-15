"""v0.12.x build-as-job submission + dedup + failure backoff (fix 2).

Fix 2 of the 2026-06-26 fleet incident: the dev-HEAD auto-update timer
submitted ``vq build-env`` jobs that piled up — a wedged build held the
host (compute-b job 699efea5a802 ran 12h+) while a duplicate
(475ff2429d52) stacked behind it ``starved``, and a just-failed build was
re-submitted on the next tick. This module is the single front door for
submitting a build-env job:

  * **dedup** — skip if a non-terminal build job for the env already
    exists (one live build per env);
  * **backoff** — skip, with a window that doubles per consecutive
    failure, if the env's last build failed;
  * **cap** — stamp the job with a ``wall_time_seconds`` the watchdog
    enforces, so even a build that slips past the in-process stall guard
    of fix 1 is reaped.

The daemon's ``--refresh`` path (:meth:`Daemon._create_build_job`) shares
the priority + wall-time + backoff record/clear hooks, so both routes
converge on the same cgroup-capped, watchdog-reaped, deduplicated build
job. The auto-update timer reaches this module via
:func:`vq.auto_update.auto_update_env`.
"""
from __future__ import annotations

import contextlib
import json
import logging
import os
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from vq import admin, capacity, config, paths
from vq.spec import JobSpec, JobState, utcnow_iso

log = logging.getLogger(__name__)

BUILD_JOB_PRIORITY = 1_000_000
"""Reserved-high priority so a freshly created build job is ``pending[0]``
on the drained host the moment it is created. Canonical home; the daemon
re-exports it (``from vq.build_job import BUILD_JOB_PRIORITY``) for
back-compat with ``vq.daemon.BUILD_JOB_PRIORITY``."""

BUILD_BACKOFF_BASE_SECONDS = 900
"""First post-failure backoff window (15 min). Doubles per consecutive
failure up to :data:`BUILD_BACKOFF_MAX_SECONDS`."""

BUILD_BACKOFF_MAX_SECONDS = 21600
"""Backoff cap (6 h) — long enough to stop a wedge-retry storm, short
enough that a transient breakage self-heals within a day's timer fires."""


def _new_jobid() -> str:
    """12 hex chars from a UUID4 — same shape as ``submit.new_jobid``
    (inlined to keep this module's import surface to admin/config/paths/
    spec only, no submit/cli coupling)."""
    return uuid.uuid4().hex[:12]


def default_build_wall_time() -> int:
    """Wall-clock cap stamped on a build job so the watchdog reaps a
    wedged rebuild even if fix 1's in-process stall guard is somehow
    missed. Sits ABOVE the update_script's own cap (+ git pull + slack)
    so the loud in-process stall/timeout fires first and the watchdog is
    the last-resort backstop. Tracks ``VQ_UPDATE_SCRIPT_TIMEOUT`` (via
    :func:`admin._update_script_timeout`) so raising the build cap for a
    cold-build box lifts the watchdog cap with it."""
    return (
        int(admin._update_script_timeout())
        + admin.GIT_PULL_TIMEOUT_SECONDS
        + 600
    )


# ----------------------------------------------------------------------
# dedup — one live build per env
# ----------------------------------------------------------------------


def _candidate_queue_dirs() -> list[Path]:
    """Queue dirs to scan for an in-flight build. Single-user: the one
    local queue. Multi-user: every per-user queue (best-effort — dirs we
    can't read are skipped by the caller's try/except)."""
    dirs: list[Path] = []
    if paths.is_multi_user():
        users_root = paths.users_root()
        if users_root.is_dir():
            for ud in sorted(users_root.iterdir()):
                qd = ud / "queue"
                if qd.is_dir():
                    dirs.append(qd)
    else:
        qd = paths.queue_dir()
        if qd.is_dir():
            dirs.append(qd)
    return dirs


def find_inflight_build_job(
    env: str, *, queue_dirs: list[Path] | None = None,
) -> str | None:
    """Return the jobid of a non-terminal build job for ``env`` (a spec
    with ``build_env == env`` still PENDING/RUNNING/SUSPENDED), or None.

    The dedup primitive behind fix 2: never submit a second build while
    one is already live for the env."""
    if queue_dirs is None:
        queue_dirs = _candidate_queue_dirs()
    for qd in queue_dirs:
        if not qd.is_dir():
            continue
        for spec_file in sorted(qd.glob("*.json")):
            try:
                spec = JobSpec.read(spec_file)
            except (OSError, ValueError):
                continue
            if spec.build_env == env and not spec.is_terminal:
                return spec.id
    return None


# ----------------------------------------------------------------------
# backoff — don't immediately rebuild after a failure
# ----------------------------------------------------------------------


def _backoff_dir() -> Path:
    return paths.state_root() / "build-backoff"


def build_backoff_path(env: str) -> Path:
    # env names are TOML table keys (alnum + dash); strip path separators
    # defensively so a malformed name can't escape the backoff dir.
    safe = env.replace("/", "_").replace(os.sep, "_")
    return _backoff_dir() / f"{safe}.json"


def _read_backoff(env: str) -> dict | None:
    try:
        return json.loads(build_backoff_path(env).read_text())
    except (FileNotFoundError, ValueError, OSError):
        return None


def record_build_failure(env: str, *, now: str | None = None) -> int:
    """Record (or escalate) the env's build-failure backoff. Returns the
    new consecutive-failure count. Best-effort; never raises. Called from
    the daemon's terminal-reap of a build job (so a watchdog SIGKILL is
    recorded too, not just a clean exit-1)."""
    path = build_backoff_path(env)
    prev = _read_backoff(env)
    try:
        count = (int(prev.get("count", 0)) + 1) if prev else 1
    except (TypeError, ValueError):
        count = 1
    data = {"env": env, "failed_at": now or utcnow_iso(), "count": count}
    with contextlib.suppress(OSError):
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(data))
        tmp.replace(path)
    return count


def clear_build_backoff(env: str) -> None:
    """Clear the env's backoff after a successful build. Best-effort."""
    with contextlib.suppress(FileNotFoundError, OSError):
        build_backoff_path(env).unlink()


def _backoff_window(count: int) -> float:
    return min(
        BUILD_BACKOFF_BASE_SECONDS * (2 ** max(0, count - 1)),
        BUILD_BACKOFF_MAX_SECONDS,
    )


def build_backoff_remaining(
    env: str, *, now: datetime | None = None,
) -> tuple[float, dict | None]:
    """Seconds remaining in the env's build-failure backoff window (0.0
    when not backed off, or when the record is unparseable), plus the
    backoff record (or None). ``now`` is injectable for tests."""
    rec = _read_backoff(env)
    if not rec:
        return 0.0, None
    try:
        failed_at = datetime.fromisoformat(rec["failed_at"])
        count = int(rec.get("count", 1))
    except (KeyError, ValueError, TypeError):
        return 0.0, rec
    window = _backoff_window(count)
    elapsed = ((now or datetime.now(UTC)) - failed_at).total_seconds()
    return max(0.0, window - elapsed), rec


# ----------------------------------------------------------------------
# submit
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class BuildSubmitOutcome:
    """Outcome of :func:`submit_build_env_job`.

    * ``submitted`` — a new build job was written (``jobid`` set).
    * ``deduped`` — a live build already exists (``jobid`` = the existing
      one); nothing submitted.
    * ``backed_off`` — the env is in its post-failure backoff window;
      nothing submitted.
    * ``error`` — submission failed (``reason`` carries the error).
    """

    action: Literal["submitted", "deduped", "backed_off", "error"]
    env: str
    jobid: str | None = None
    reason: str = ""


def build_job_cpus(cfg: config.Config) -> int:
    """CPU slots to request for an auto-generated build job.

    Never ask for more than the local daemon will actually admit. The count is
    a build-parallelism hint, not a requirement, but a spec above the daemon's
    cap can never be admitted at all: it used to sit PENDING forever and now
    terminal-fails with a named reason, and either way ``--refresh`` and the
    auto-update timer are dead on any host whose daemon caps CPUs below the
    physical core count. compute-a is the case that surfaced it -- 16 physical cores
    against an 8-CPU cap, so every generated build was born unadmittable
    (2026-07-25, jobs c621e42ce2a2 / 68e6cd5297d0).

    Authority order, most authoritative first:

    1. the daemon's advertised effective budget (``daemon_capacity.json``),
       which is written at daemon startup *after* CLI flags and config are
       resolved, so it is the only source that reflects a CLI-only ``--max-cpus``;
    2. the durable ``[daemon] max_cpus`` config section, for a daemon that has
       not advertised (never restarted since the feature landed);
    3. the physical core count, which is also what the daemon itself defaults
       ``max_cpus`` to -- so this branch is behaviour-preserving.
    """
    physical = os.cpu_count() or 1
    advertised = capacity.read_daemon_capacity()
    if advertised is not None:
        return max(1, min(physical, advertised.max_cpus))
    configured = cfg.daemon.max_cpus
    if configured is not None:
        return max(1, min(physical, configured))
    return physical



BUILD_MEM_FRACTION = 0.5
"""Share of the host's memory cap an auto-generated build job requests.

Half leaves room for the work already running: a build is maintenance, not the
point of the machine. It also fits by construction, which is the property that
matters -- the alternative was inheriting the daemon's ``default_job_mem_mb``,
which on compute-a EXCEEDS ``max_mem_mb``, so every undeclared job there -- builds
included -- was unadmittable forever.
"""


def build_job_mem_mb(cfg: config.Config) -> int | None:
    """Memory to request for an auto-generated build job, or None if uncapped.

    Declaring a figure that fits, rather than clamping one that does not, is the
    deliberate choice here. Clamping CPUs costs a build wall time; clamping
    memory gets it OOM-killed halfway through, which is worse than not starting.

    Same authority order as :func:`build_job_cpus`: the daemon's advertised
    effective budget first (it is the only source reflecting a CLI-only
    ``--max-mem-mb``), then the durable ``[daemon]`` config, then None -- an
    uncapped host has nothing to fit inside, so the spec declares nothing and
    behaves exactly as it always did.
    """
    cap: int | None = None
    advertised = capacity.read_daemon_capacity()
    if advertised is not None:
        cap = advertised.max_mem_mb
    if cap is None:
        cap = cfg.daemon.max_mem_mb
    if cap is None or cap < 1:
        return None
    return max(1, int(cap * BUILD_MEM_FRACTION))

def submit_build_env_job(
    env: str, cfg: config.Config, *, host: str,
    baseline_sha: str | None = None,
    target_sha: str | None = None,
    wall_time_seconds: int | None = None,
    queue_dir: Path | None = None,
    jobs_dir: Path | None = None,
) -> BuildSubmitOutcome:
    """Submit a deduped, backed-off, wall-time-capped ``vq build-env
    <env>`` job to the local daemon — the single front door fix 2 routes
    the dev-HEAD auto-update through (instead of an uncapped inline
    rebuild in the timer process).

    ``baseline_sha`` and ``target_sha`` bind an automatic branch decision to
    the queued command.  The worker revalidates both under the same lifecycle
    lock immediately before mutation, so a delayed job cannot downgrade a
    checkout that advanced after submission. ``queue_dir`` / ``jobs_dir``
    override the resolved local dirs (tests).
    """
    if (baseline_sha is None) != (target_sha is None):
        return BuildSubmitOutcome(
            "error", env,
            reason="build job needs both baseline_sha and target_sha",
        )
    for label, value in (
        ("baseline_sha", baseline_sha),
        ("target_sha", target_sha),
    ):
        if value is not None and re.fullmatch(r"[0-9a-f]{40}", value) is None:
            return BuildSubmitOutcome(
                "error", env, reason=f"{label} must be a full lowercase SHA",
            )
    # 1. dedup — one live build per env.
    qdirs = [queue_dir] if queue_dir is not None else None
    existing = find_inflight_build_job(env, queue_dirs=qdirs)
    if existing is not None:
        return BuildSubmitOutcome(
            "deduped", env, jobid=existing,
            reason=(
                f"build job {existing} already in flight for {env!r}; "
                f"skipping duplicate submission"
            ),
        )
    # 2. backoff — don't immediately rebuild after a failure.
    remaining, rec = build_backoff_remaining(env)
    if remaining > 0:
        count = int(rec.get("count", 1)) if rec else 1
        return BuildSubmitOutcome(
            "backed_off", env,
            reason=(
                f"backing off after {count} failed build(s); "
                f"~{remaining / 60:.0f} min until retry"
            ),
        )
    # 3. submit a first-class, cgroup-capped, exclusive build job.
    multi_user = config.system_multi_user_enabled() or cfg.multi_user.enabled
    qd = queue_dir or (
        paths.user_queue_dir(os.geteuid()) if multi_user else paths.queue_dir()
    )
    jd = jobs_dir or (
        paths.user_jobs_dir(os.geteuid()) if multi_user else paths.jobs_dir()
    )
    try:
        qd.mkdir(parents=True, exist_ok=True)
        jd.mkdir(parents=True, exist_ok=True)
        jobid = _new_jobid()
        workspace = jd / jobid
        workspace.mkdir(parents=True, exist_ok=True)
        cap = wall_time_seconds or default_build_wall_time()
        command = [sys.executable, "-m", "vq", "build-env", env]
        if baseline_sha is not None and target_sha is not None:
            command.extend(
                [
                    "--baseline-sha", baseline_sha,
                    "--expected-sha", target_sha,
                ]
            )
        spec = JobSpec(
            id=jobid,
            command=command,
            cwd=str(workspace.resolve()),
            cpus=build_job_cpus(cfg),
            mem_mb=build_job_mem_mb(cfg),
            priority=BUILD_JOB_PRIORITY,
            build_env=env,
            wall_time_seconds=cap,
            job_name=f"build-{env}",
            tags=["build-env", "auto-update"],
            state=JobState.PENDING,
            submitter=str(os.geteuid()) if multi_user else None,
        )
        spec.write(qd / f"{jobid}.json")
    except OSError as e:
        return BuildSubmitOutcome(
            "error", env, reason=f"could not submit build job: {e}",
        )
    log.info(
        "auto-update: submitted build job %s for env %r (wall_time=%ss)",
        jobid, env, cap,
    )
    return BuildSubmitOutcome(
        "submitted", env, jobid=jobid,
        reason=f"submitted build job {jobid} for {env!r} (wall_time={cap}s)",
    )
