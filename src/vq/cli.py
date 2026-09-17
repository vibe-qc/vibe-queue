"""vq -- cross-machine job queue CLI."""

from __future__ import annotations

import base64
import codecs
import contextlib
import json
import logging
import math
import os
import re
import secrets
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Any

import click

from vq import (
    __version__,
    admin_detached,
    auth,
    config,
    fleet_release,
    fleet_rollout,
    host_status,
    lifecycle,
    ownership,
    paths,
    provision,
    ssh_probe,
    transport,
)
from vq import (
    admin as admin_module,
)
from vq import (
    capacity as capacity_module,
)
from vq import (
    doctor as doctor_module,
)
from vq import (
    drain as drain_module,
)
from vq import (
    output as output_mod,
)
from vq import (
    submit as submit_module,
)
from vq import (
    throttle as throttle_module,
)
from vq.cleanup import (
    archive_workspace,
    cleanup_scheduler_remote_workspace,
    delete_job,
    find_candidates,
    parse_age,
    restore_workspace,
)
from vq.cleanup import (
    format_table as cleanup_table,
)
from vq.codename import format_version
from vq.daemon import Daemon
from vq.daemon_control import (
    is_daemon_serving,
    read_pidfile,
    remove_pidfile,
    stop_daemon,
    write_pidfile,
)
from vq.daemon_control import (
    local_daemon_ping as _probe_local_daemon,
)
from vq.doctor import (
    payload_hit_transport_failure as _doctor_payload_hit_transport_failure,
)
from vq.doctor import (
    scheduler_command_wrapper_check as _doctor_scheduler_command_wrapper_check,  # noqa: F401
)
from vq.doctor import (
    scheduler_liveness_check_from_payload as _scheduler_liveness_check_from_payload,  # noqa: F401
)
from vq.doctor import (
    scheduler_probe_check_from_payload as _scheduler_probe_check_from_payload,  # noqa: F401
)
from vq.fetch import (
    BulkFetchResult,
    bulk_fetch,
    emit_artifact_tar,
    emit_workdir_tar,
    emit_workspace_tar,
    fetch_artifact_local,
    fetch_artifact_remote,
    fetch_local,
    fetch_remote,
    fetch_workdir_local,
    fetch_workdir_remote,
    mark_terminal_fetch,
    prepare_remote_bulk_fetch,
    require_fresh_fetch_manifest,
    require_workspace_artifact_selection,
)
from vq.host import is_local_host
from vq.kill import kill_job
from vq.listing import (
    QUEUE_FILTER_STATES,
    effective_queue_state,
    format_table,
    list_jobs,
    matches_queue_state_filter,
    normalize_delegated_queue_handle_host,
    pending_configured_capacity_known,
    pending_configured_capacity_overages,
    queue_handle_for_spec,
    queue_handle_for_unvalidated_row,
    queue_handle_without_spec,
    queue_host_for_scheduler_target,
    scheduler_running_confirmed,
    scheduler_target_is_safe,
)
from vq.log import setup_cli_logging, setup_daemon_logging
from vq.logs import (
    follow_logs,
    follow_output,
    follow_progress,
    show_logs,
    show_logs_json,
    show_output,
    show_output_json,
    show_progress,
    tail_file,
)
from vq.pause_resume import (
    PauseError,
    pause_all,
    pause_job,
    pause_scheduler_all,
    pause_scheduler_job,
    resume_all,
    resume_job,
    resume_scheduler_all,
    resume_scheduler_job,
)
from vq.resubmit import (
    ResubmitOverrides,
    resubmit_local,
    resubmit_remote,
    resubmit_state,
    resubmit_state_remote,
)
from vq.scheduler_dispatch import (
    SchedulerDispatcher,
    SchedulerError,
    SchedulerHandle,
    scheduler_dispatcher_for,
    scheduler_handle_for_spec,
)
from vq.spec import (
    JOB_NAME_MAX_LEN,
    JOB_NAME_PATTERN,
    TERMINAL_STATES,
    JobSpec,
    JobState,
    ProgramRuntimePin,
    signal_name_for_exit,
)
from vq.spec_access import resolve_authorized_spec
from vq.status import (
    scheduler_status_projection_for_spec,
    show_status,
    show_status_json,
    terminal_diagnosis_for_spec,
)
from vq.throttle import (
    ThrottleError,
)
from vq.wait import (
    DEFAULT_POLL_INTERVAL_SECONDS,
    WaitTimeout,
    wait_for_scheduler_acceptance,
    wait_for_terminal,
)

log = logging.getLogger(__name__)

_UNSAFE_JOB_NAME_RUN = re.compile(r"[^A-Za-z0-9._-]+")

# These lifecycle selectors have unchanged source-side meaning on pre-#304
# remotes. Monitor-sensitive or newer selectors are applied to unfiltered JSON
# by the current client so a rolling deployment cannot reject or flatten them.
_ROLLING_REMOTE_SOURCE_FILTER_STATES: frozenset[str] = frozenset(
    {
        "pending",
        "completed",
        "failed",
        "killed",
        "interrupted",
        "oom_killed",
        "starved",
        "time_exceeded",
        "aborted_by_queue",
    }
)


def _parse_time_limit(value: str) -> int:
    """v0.7.16 *Codd's Tuple*: parse a SLURM-style time-limit
    string into seconds.

    Accepted forms (most-specific first):

    * ``HH:MM:SS`` — e.g. ``01:30:00`` → 5400
    * ``MM:SS`` — e.g. ``90:00`` → 5400
    * plain integer (string) — e.g. ``5400`` → 5400

    Rationale: SLURM operators have muscle memory for the colon-
    delimited form; HPC training material universally documents
    walltime as ``HH:MM:SS``. The plain-integer form is here
    because it composes with shell ``$(date +%s)`` arithmetic
    and matches the existing ``--wall-time-seconds`` semantics.

    Raises ``click.UsageError`` with a precise reason on any
    parse failure so the operator hears about a malformed value
    immediately, not after the job hits walltime.
    """
    value = value.strip()
    if not value:
        raise click.UsageError(
            "--time-limit / --time: empty value (expected HH:MM:SS, MM:SS, or integer seconds)"
        )
    if ":" in value:
        parts = value.split(":")
        if len(parts) not in (2, 3):
            raise click.UsageError(
                f"--time-limit / --time {value!r}: expected "
                f"HH:MM:SS or MM:SS (got {len(parts)} colon-"
                f"separated parts)"
            )
        try:
            ints = [int(p) for p in parts]
        except ValueError:
            raise click.UsageError(
                f"--time-limit / --time {value!r}: non-integer "
                f"component (expected digits only between colons)"
            ) from None
        if any(i < 0 for i in ints):
            raise click.UsageError(
                f"--time-limit / --time {value!r}: negative "
                f"component (expected non-negative integers)"
            )
        if len(parts) == 3:
            hours, minutes, seconds = ints
        else:
            hours = 0
            minutes, seconds = ints
        # Allow MM > 60 in the MM:SS form (SLURM accepts e.g.
        # 90:00 = 1h30m), but cap individual components in the
        # HH:MM:SS form for sanity (no 99:99:99 — operators
        # should write 100:0:0 or 4d12h or use seconds).
        if len(parts) == 3 and (minutes >= 60 or seconds >= 60):
            raise click.UsageError(
                f"--time-limit / --time {value!r}: in HH:MM:SS "
                f"form, MM and SS must be < 60 (got "
                f"{minutes:02d}:{seconds:02d}). Use MM:SS "
                f"form for over-60 minutes."
            )
        if len(parts) == 2 and seconds >= 60:
            raise click.UsageError(
                f"--time-limit / --time {value!r}: in MM:SS "
                f"form, SS must be < 60 (got {seconds:02d})"
            )
        total = hours * 3600 + minutes * 60 + seconds
    else:
        try:
            total = int(value)
        except ValueError:
            raise click.UsageError(
                f"--time-limit / --time {value!r}: expected HH:MM:SS, MM:SS, or integer seconds"
            ) from None
        if total < 0:
            raise click.UsageError(f"--time-limit / --time {value!r}: negative seconds")
    if total < 1:
        raise click.UsageError(
            f"--time-limit / --time {value!r}: must be >= 1 second (got {total})"
        )
    return total


def _normalize_job_name_option(job_name: str | None) -> str | None:
    """Return a spec-safe ``--job-name`` value, warning when coerced."""
    if job_name is None or JOB_NAME_PATTERN.fullmatch(job_name):
        return job_name

    sanitized = _UNSAFE_JOB_NAME_RUN.sub("-", job_name.strip()).strip("-")
    if len(sanitized) > JOB_NAME_MAX_LEN:
        sanitized = sanitized[:JOB_NAME_MAX_LEN].rstrip("-")
    if not sanitized:
        raise click.UsageError(
            f"--job-name {job_name!r} cannot be sanitized to a non-empty "
            f"filesystem-safe name (allowed: alphanumerics, '-', '_', '.', "
            f"1-{JOB_NAME_MAX_LEN} chars)."
        )
    if not JOB_NAME_PATTERN.fullmatch(sanitized):
        raise click.UsageError(
            f"--job-name {job_name!r} cannot be sanitized to a valid name "
            f"(allowed: alphanumerics, '-', '_', '.', 1-{JOB_NAME_MAX_LEN} chars)."
        )
    click.echo(
        f"vq: sanitized --job-name {job_name!r} -> {sanitized!r}",
        err=True,
    )
    return sanitized


def _validate_program_for_submit(
    cfg: config.Config, program_name: str | None, *, local_spec: bool
) -> None:
    """Render domain validation failures as Click usage errors."""
    try:
        submit_module._validate_program_for_submit(
            cfg,
            program_name,
            local_spec=local_spec,
        )
    except ValueError as e:
        raise click.UsageError(str(e)) from None


EXPECTED_SHA_MIN_PREFIX_LEN = submit_module.EXPECTED_SHA_MIN_PREFIX_LEN


def _validate_expected_sha_for_submit(
    cfg: config.Config,
    program_name: str | None,
    expected_sha: str | None,
    *,
    local_spec: bool,
) -> str | None:
    """Render domain SHA validation failures as Click usage errors."""
    try:
        return submit_module._validate_expected_sha_for_submit(
            cfg,
            program_name,
            expected_sha,
            local_spec=local_spec,
        )
    except ValueError as e:
        raise click.UsageError(str(e)) from None


def _validate_scheduler_target_expected_sha(
    cfg: config.Config,
    scheduler_host: str,
    program_name: str | None,
    expected_sha: str | None,
    *,
    command_candidates: list[str] | None = None,
) -> ProgramRuntimePin | None:
    """Render scheduler-runtime resolution failures as Click usage errors."""
    try:
        return submit_module._validate_scheduler_target_expected_sha(
            cfg,
            scheduler_host,
            program_name,
            expected_sha,
            command_candidates=command_candidates,
        )
    except ValueError as e:
        raise click.UsageError(str(e)) from None


def _program_runtime_pin_for_submit(
    cfg: config.Config,
    program_name: str | None,
    *,
    expected_sha: str | None = None,
) -> ProgramRuntimePin | None:
    """Compatibility adapter for local runtime-pin construction."""
    return submit_module._program_runtime_pin_for_submit(
        cfg,
        program_name,
        expected_sha=expected_sha,
    )


def _multi_user_active(cfg: config.Config | None = None) -> bool:
    """v0.6.x: True when multi-user mode is configured and the daemon
    state root should be the system-level path.

    The submit/kill/queue/status/fetch CLI verbs use this to decide
    whether to use per-user paths under /var/lib/vq/users/<uid>/
    instead of the legacy single-user paths under ~/.local/share/vq/.

    Resolution (v0.6.x): multi-user is active when EITHER the loaded
    config has ``[multi_user] enabled = true``, OR a system-wide
    ``/etc/vq/config.toml`` does. The system-wide fallback means a
    user on a multi-user host auto-detects the mode without having to
    mirror ``[multi_user]`` into their own ``~/.config/vq/config.toml``
    — the client and the root daemon then agree on where job state
    lives.
    """
    if cfg is None:
        try:
            cfg = config.load_config()
        except config.ConfigError:
            cfg = None
    if cfg is not None and cfg.multi_user.enabled:
        return True
    return config.system_multi_user_enabled()


def _submit_receipt(
    cfg: config.Config,
    host: str,
    jobids: list[str],
    *,
    runtime_pin: ProgramRuntimePin | None = None,
    capacity_warnings: Sequence[str] = (),
) -> dict[str, object]:
    """Context a submitter needs to answer "did this land, and what next?".

    A bare jobid could not distinguish a job that will dispatch in a second
    from one parked behind a drain, an in-flight admin update, or a dead
    daemon — an agent had to make three follow-up calls to find out. This
    collects that in one place at submit time.

    Deliberately cheap and best-effort: a config lookup plus a drain-state
    read, no SSH round-trip. A submit must never fail, or get slower, because
    of its own receipt.
    """
    receipt: dict[str, object] = {
        "jobids": list(jobids),
        "host": host,
        "acceptance_scope": "queue",
        "execution_status": "not_observed",
        "capacity_warnings": list(capacity_warnings),
    }
    if runtime_pin is not None and runtime_pin.scheduler_host is not None:
        # BUG 101: a pinned scheduler-target submit reports the RESOLVED
        # target identity — host, executable, version, full SHA — so the
        # submitter can audit what will actually run without a round-trip.
        receipt["program_runtime_pin"] = {
            "scheduler_host": runtime_pin.scheduler_host,
            "resolved_executable": runtime_pin.resolved_executable,
            "program_kind": runtime_pin.program_kind,
            "program_version": runtime_pin.program_version,
            "expected_git_sha": runtime_pin.expected_git_sha,
            "artifact_identity": runtime_pin.artifact_identity,
        }
    holds: list[str] = []
    try:
        try:
            host_cfg = cfg.host(host)
        except config.ConfigError:
            host_cfg = None
        if host_cfg is not None and host_cfg.scheduler != "local":
            receipt["scheduler"] = host_cfg.scheduler
            receipt["scheduler_driver"] = host_cfg.scheduler_driver
        # Name what will keep the job PENDING, at submit time, rather than
        # leaving the submitter to discover it by polling.
        if is_local_host(host) or (
            host_cfg is not None and host_cfg.scheduler != "local"
        ):
            state = drain_module.read_effective_drain_state()
            if state is not None:
                if state.is_full_drain:
                    holds.append("a full drain is active (`vq drain --status`)")
                elif state.drains_scheduler_target(host):
                    holds.append(
                        f"a scheduler drain lane holds {host} "
                        "(`vq drain --status`)"
                    )
            if admin_module.admin_update_marker_exists():
                # Scoped like the daemon's dispatch gate: only name the
                # marker as a hold when it actually holds THIS job's
                # target, else a slurm-cluster submit during a pbs-cluster rebuild
                # reads "held" while it dispatches normally.
                scope = admin_module.admin_update_markers_scope()
                target = (
                    host
                    if host_cfg is not None and host_cfg.scheduler != "local"
                    else admin_module.LOCAL_DISPATCH_SCOPE
                )
                if scope is None or target in scope:
                    holds.append(
                        "an admin update is in progress (`vq admin status`)"
                    )
    except BaseException:
        # Submission already returned authoritative job IDs. Receipt
        # enrichment is advisory; even an interrupt or broken logging handler
        # must not make accepted work outlive the client's only receipt.
        with contextlib.suppress(BaseException):
            log.debug("submit receipt failed", exc_info=True)
    receipt["dispatch_holds"] = holds
    receipt["next"] = [
        f"vq status {host} {jobids[0]}",
        f"vq logs {host} {jobids[0]} -f",
    ]
    return receipt


def _narrate_submit_receipt(receipt: dict[str, object]) -> None:
    """Print the receipt for a human, on stderr, only at an interactive TTY.

    Not unconditional, on purpose. stdout is the bare-jobid contract every
    wrapper in the fleet parses, and plenty of callers — including click's own
    test runner and several agent harnesses — merge stderr into stdout. Adding
    surprise chatter to a captured stream would break them for a courtesy
    line. A non-interactive caller that wants this asks for `--json`.
    """
    if not sys.stderr.isatty():
        return
    jobids = receipt["jobids"]
    assert isinstance(jobids, list)
    what = f"{len(jobids)} jobs" if len(jobids) > 1 else jobids[0]
    line = f"  submitted {what} -> {receipt['host']}"
    if receipt.get("scheduler"):
        line += (
            f"; scheduler={receipt['scheduler']} via driver "
            f"{receipt.get('scheduler_driver')}"
        )
    click.echo(line, err=True)
    holds = receipt.get("dispatch_holds") or []
    assert isinstance(holds, list)
    for hold in holds:
        click.echo(f"  note: dispatch is held — {hold}", err=True)
    nxt = receipt["next"]
    assert isinstance(nxt, list)
    click.echo(f"  next: {' | '.join(nxt)}", err=True)


def _resolve_host(cfg: config.Config, host: str | None) -> str:
    """Translate optional positional host into a resolved host string.

    Wraps :meth:`Config.resolve_host` so the CLI raises ``click.UsageError``
    (which Click renders as a clean error message + exit 2) instead of a
    bare ConfigError traceback.
    """
    try:
        return cfg.resolve_host(host)
    except config.ConfigError as e:
        raise click.UsageError(str(e)) from None


# ---------------------------------------------------------------------------
# v0.11.0 *Baran's Detour* — route a no-host per-job lookup around a
# default_host that is administratively down.
#
# The per-job verbs (status / logs / output / progress / events / tail / kill /
# wait / fetch / pause / resume / throttle / resubmit) take ``JOBID`` or
# ``HOST JOBID``. The no-host form has always resolved to ``default_host`` and
# delegated there. When
# ``default_host`` is the box you've marked ``vq host down`` (v0.10.0
# *Lampson's Hint*) — i.e. unreachable from here — that single delegate dies
# on SSH (exit 255) and the whole command hard-errors, even though the job
# lives on a reachable host. (``vq status compute-a JOBID`` worked; ``vq status
# JOBID`` did not.)
#
# Baran's Detour makes the no-host resolution skip a down ``default_host``
# and *locate* the job across the remaining up hosts — the rest are skipped /
# soft-warned, never aborted on — mirroring how ``vq overview`` / ``vq submit
# auto`` already route around down boxes. The fan-out is a read-only probe
# that finds the single owning host; the verb then runs on exactly that host
# through its normal local/remote branch, so a mutating verb (``vq kill``)
# never broadcasts — it acts only on the one host that owns the jobid.
#
# Issue #484 adds the other remote-default guard without changing ownership:
# when an inferred, unmarked remote default cannot supply one trusted durable
# queue listing, a non-status verb refuses before invoking its action.  A
# readable listing leaves that default selected even when the job is absent.
# ---------------------------------------------------------------------------

# A locate probe reads the durable queue listing.  It must not call ``status``:
# status itself uses this locator, and recursive status probing also refreshes
# scheduler state before ownership has been established.  Cap the read so one
# slow-but-reachable box cannot stall the whole fan-out.
_LOCATE_PROBE_TIMEOUT_SECONDS = 30.0


def _queue_daemon_key(host: str) -> str:
    """Collapse every local spelling to one physical queue authority."""
    return "localhost" if is_local_host(host) else host


def _job_queue_authority(
    cfg: config.Config,
    host: str,
) -> tuple[str, str | None] | None:
    """Return ``(daemon_host, scheduler_lane)`` for a public host handle.

    Local/remote daemon hosts own their lane directly.  A daemonless scheduler
    host is a logical lane in its configured driver's durable queue, so the
    persisted ``scheduler_target`` remains part of the ownership identity.
    Ordinary fleet aliases collapse to their canonical daemon; scheduler
    aliases do not, because each alias can name a distinct persisted lane.
    """
    try:
        host_cfg = cfg.host(host)
    except config.ConfigError:
        return None
    if host_cfg.scheduler != "local":
        driver = host_cfg.scheduler_driver
        if driver is None:
            return None
        if is_local_host(driver):
            # Local aliases resolve without a [hosts.*] enrollment.  The
            # scheduler row is still discoverable in this process's queue;
            # no plain local row is attributed to the scheduler lane.
            return _queue_daemon_key(driver), host
        try:
            driver_cfg = cfg.host(driver)
        except config.ConfigError:
            return None
        if driver_cfg.scheduler != "local":
            return None
        if driver_cfg.fleet_role == "alias":
            canonical_driver = driver_cfg.fleet_canonical_host
            if canonical_driver is None:
                return None
            driver = canonical_driver
        return _queue_daemon_key(driver), host
    if host_cfg.fleet_role == "alias":
        canonical_host = host_cfg.fleet_canonical_host
        if canonical_host is None:
            return None
        return _queue_daemon_key(canonical_host), None
    return _queue_daemon_key(host), None


def _queue_rows_hold_job(
    rows: object,
    jobid: str,
    *,
    scheduler_lane: str | None,
) -> str:
    """Classify one trusted queue JSON value for ``jobid`` and its lane."""
    target_counts = _queue_job_target_counts(rows, jobid)
    if target_counts is None:
        return "unreachable"
    matches = target_counts.get(scheduler_lane, 0)
    if matches > 1:
        return "ambiguous"
    return "found" if matches == 1 else "absent"


def _queue_job_target_counts(
    rows: object,
    jobid: str,
) -> dict[str | None, int] | None:
    """Validate one queue snapshot and count ``jobid`` rows by exact lane."""
    if not isinstance(rows, list):
        return None
    counts: dict[str | None, int] = {}
    for row in rows:
        if not isinstance(row, dict):
            return None
        row_id = row.get("id")
        if not isinstance(row_id, str) or not row_id.strip():
            return None
        target = row.get("scheduler_target")
        if target is not None and not isinstance(target, str):
            return None
        if row_id != jobid:
            continue
        counts[target] = counts.get(target, 0) + 1
    return counts


def _queue_rows_for_authority(
    cfg: config.Config,
    daemon_host: str,
    *,
    multi_user: bool,
) -> object | None:
    """Read one physical daemon queue once, or return no trusted snapshot."""
    if is_local_host(daemon_host):
        try:
            return [
                {
                    "id": spec.id,
                    "scheduler_target": spec.scheduler_target,
                }
                for spec in list_jobs(daemon_host, multi_user=multi_user)
            ]
        except OSError:
            return None
    try:
        host_cfg = cfg.host(daemon_host)
    except config.ConfigError:
        return None
    try:
        proc = transport.run_remote_vq(
            host_cfg,
            "queue",
            "localhost",
            "--show-archived",
            "--json",
            check=False,
            timeout=_LOCATE_PROBE_TIMEOUT_SECONDS,
            owned_process_group=True,
        )
    except transport.RemoteError:
        # A timeout, launch failure, or other transport error supplies no
        # trusted queue snapshot.
        return None
    if proc.returncode != 0:
        # A queue listing has no job-not-found exit: any non-zero result means
        # no trusted durable snapshot arrived, regardless of whether SSH or
        # the remote vq process supplied the error.
        return None
    if not proc.stdout:
        return None
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError:
        return None


def _host_has_job(cfg: config.Config, host: str, jobid: str, *, multi_user: bool) -> str:
    """Read-only probe: does ``host`` hold ``jobid``?

    Returns ``"found"`` / ``"absent"`` / ``"ambiguous"`` /
    ``"unreachable"``. Never raises.  Ownership comes from persisted queue
    rows, including archived records, rather than from a status call or a
    default host.  Scheduler targets read their driver's queue and match only
    their own ``scheduler_target`` lane; this keeps aliases sharing a driver
    distinct without counting the same spec as a driver-local job too.
    """
    authority = _job_queue_authority(cfg, host)
    if authority is None:
        return "unreachable"
    daemon_host, scheduler_lane = authority
    rows = _queue_rows_for_authority(
        cfg,
        daemon_host,
        multi_user=multi_user,
    )
    return _queue_rows_hold_job(rows, jobid, scheduler_lane=scheduler_lane)


def _require_inferred_default_queue_snapshot(
    cfg: config.Config,
    default_host: str,
    jobid: str,
    *,
    multi_user: bool,
) -> None:
    """Require one trusted queue listing from the default action endpoint.

    This is a reachability preflight, not ownership discovery.  Fleet aliases
    deliberately stay uncollapsed because the requested verb will use that
    exact endpoint.  A scheduler target is invoked through its exact configured
    driver, so that driver is the queue endpoint checked here.
    """
    scheduler_driver = _scheduler_driver_host(cfg, default_host)
    queue_host = scheduler_driver or default_host
    if is_local_host(queue_host):
        return

    # The caller already established that default_host itself is not marked
    # down.  Check a distinct scheduler driver too, so a marked-down action
    # endpoint is never contacted merely because its public lane is unmarked.
    rows: object | None = None
    if queue_host == default_host or host_status.is_down(queue_host) is None:
        rows = _queue_rows_for_authority(
            cfg,
            queue_host,
            multi_user=multi_user,
        )
    if _queue_job_target_counts(rows, jobid) is not None:
        return

    authority = (
        f" (queue authority {queue_host!r})"
        if queue_host != default_host
        else ""
    )
    raise click.ClickException(
        f"configured default_host {default_host!r}{authority} is unavailable "
        "or did not return a valid queue listing; name a host explicitly "
        "or update default_host"
    )


def _read_queue_authority_snapshots(
    cfg: config.Config,
    daemon_hosts: list[str],
    *,
    multi_user: bool,
) -> dict[str, object | None]:
    """Read distinct physical queues in parallel without rescanning aliases."""

    def read_one(daemon_host: str) -> object | None:
        try:
            return _queue_rows_for_authority(
                cfg,
                daemon_host,
                multi_user=multi_user,
            )
        except Exception:  # noqa: BLE001 - one queue cannot sink fleet discovery
            return None

    if len(daemon_hosts) <= 1 or _fanout_serial_requested():
        return {host: read_one(host) for host in daemon_hosts}

    from concurrent.futures import ThreadPoolExecutor, as_completed

    snapshots: dict[str, object | None] = {}
    with ThreadPoolExecutor(
        max_workers=_fanout_max_workers(len(daemon_hosts))
    ) as executor:
        futures = {executor.submit(read_one, host): host for host in daemon_hosts}
        for future in as_completed(futures):
            host = futures[future]
            try:
                snapshots[host] = future.result()
            except Exception:  # noqa: BLE001 - defence-in-depth
                snapshots[host] = None
    return snapshots


def _locate_job_host(
    cfg: config.Config,
    jobid: str,
    *,
    multi_user: bool,
    exclude: frozenset[str] = frozenset(),
) -> str:
    """Find the single up host that owns ``jobid`` and return its name.

    Reads every configured queue authority except admin-down ones
    (``vq host down``) and ``exclude``, in parallel (reusing the v0.7.6
    *Tanenbaum's Mailbox* fan-out). Scheduler handles are filtered lanes in
    their driver's listing; ordinary fleet aliases collapse to their canonical
    daemon. Raises ``click.ClickException`` when:

    * **no up host owns it** — the message lists which hosts were skipped
      (down) or unreachable, so the operator knows the search wasn't
      exhaustive (the job may be on the down box); or
    * **more than one host owns it** — a jobid present on multiple hosts;
      refuse to guess and ask for an explicit ``HOST``.
    """
    down = host_status.load_down()
    candidates: list[str] = []
    authorities: dict[str, tuple[str, str | None] | None] = {}
    for host in sorted(cfg.hosts):
        if host in down or host in exclude:
            continue
        host_cfg = cfg.hosts[host]
        # An ordinary fleet alias is the same daemon queue as its canonical
        # host.  Scheduler aliases remain candidates because their persisted
        # scheduler_target values are distinct lanes on the shared driver.
        if host_cfg.scheduler == "local" and host_cfg.fleet_role == "alias":
            continue
        driver = host_cfg.scheduler_driver
        if driver is not None and (driver in down or driver in exclude):
            continue
        authority = _job_queue_authority(cfg, host)
        if authority is not None and (
            authority[0] in down or authority[0] in exclude
        ):
            continue
        candidates.append(host)
        authorities[host] = authority
    if not candidates:
        skipped = sorted(set(down) | set(exclude))
        raise click.ClickException(
            f"cannot locate job {jobid}: no reachable host left to search "
            f"(skipped: {', '.join(skipped) or '(none)'}). Pass an explicit "
            f"host, or clear a down mark with `vq host up HOST`."
        )
    # Several public spellings can identify the exact same logical queue
    # handle.  Built-in local aliases are the important compatibility case:
    # ``localhost`` and ``127.0.0.1`` share one direct lane even when an old
    # config did not mark either as a fleet alias.  Deduplicate by the complete
    # authority tuple, not only by physical daemon, so scheduler lanes sharing
    # a driver remain distinct.  Prefer the daemon's canonical public spelling
    # when it is configured; otherwise the already-sorted candidate order is
    # deterministic.
    authority_handles: dict[tuple[str, str | None], list[str]] = {}
    results: dict[str, str] = {}
    for host in candidates:
        authority = authorities[host]
        if authority is None:
            results[host] = "unreachable"
            continue
        authority_handles.setdefault(authority, []).append(host)
    if (
        ("localhost", None) not in authority_handles
        and any(daemon_host == "localhost" for daemon_host, _ in authority_handles)
    ):
        # A scheduler lane may legally use an unenrolled localhost driver.
        # Once that physical queue is a trusted authority, its direct lane is
        # ownership evidence too: otherwise a matching local spec could be
        # ignored in favour of a duplicate job id found on a remote daemon.
        authority_handles[("localhost", None)] = ["localhost"]
    grouped: dict[str, list[tuple[str, str | None]]] = {}
    for (daemon_host, scheduler_lane), public_hosts in authority_handles.items():
        public_host = min(
            public_hosts,
            key=lambda candidate: (candidate != daemon_host, candidate),
        )
        grouped.setdefault(daemon_host, []).append((public_host, scheduler_lane))

    # A scheduler row is owned by the driver's physical queue, but only an
    # exact configured scheduler_target gives us a safe public handle for the
    # final status/refresh call.  Build this map from the complete config (not
    # only current candidates) so an administratively down or explicitly
    # excluded lane stays a known-but-unreachable owner rather than looking
    # like stale configuration.
    configured_scheduler_lanes: dict[str, set[str]] = {}
    for configured_host in sorted(cfg.hosts):
        authority = _job_queue_authority(cfg, configured_host)
        if authority is None:
            continue
        daemon_host, scheduler_lane = authority
        if scheduler_lane is not None:
            configured_scheduler_lanes.setdefault(daemon_host, set()).add(
                scheduler_lane
            )
    snapshots = _read_queue_authority_snapshots(
        cfg,
        sorted(grouped),
        multi_user=multi_user,
    )
    skipped_lanes = set(down) | set(exclude)
    unconfigured_targets: list[tuple[str, str]] = []
    for daemon_host, handles in grouped.items():
        rows = snapshots.get(daemon_host)
        target_counts = _queue_job_target_counts(rows, jobid)
        if target_counts is not None:
            known_lanes = configured_scheduler_lanes.get(daemon_host, set())
            unconfigured_targets.extend(
                (daemon_host, target)
                for target in target_counts
                if target is not None
                and target not in known_lanes
                and target not in skipped_lanes
            )
        for host, scheduler_lane in handles:
            if target_counts is None:
                results[host] = "unreachable"
                continue
            matches = target_counts.get(scheduler_lane, 0)
            if matches > 1:
                results[host] = "ambiguous"
            else:
                results[host] = "found" if matches == 1 else "absent"
    if unconfigured_targets:
        detail = ", ".join(
            f"{daemon_host}:{target}"
            for daemon_host, target in sorted(set(unconfigured_targets))
        )
        raise click.ClickException(
            f"job {jobid} has an unconfigured scheduler_target in durable "
            f"queue ownership ({detail}); refusing to route to a different "
            "owner. Restore that scheduler host in config or pass an explicit "
            "owner-qualified host."
        )
    found = sorted(h for h, r in results.items() if r == "found")
    ambiguous = sorted(h for h, r in results.items() if r == "ambiguous")
    if ambiguous:
        raise click.ClickException(
            f"job {jobid} has multiple durable records in "
            f"{', '.join(ambiguous)}; pass an explicit owner-qualified host."
        )
    if len(found) == 1:
        return found[0]
    if len(found) > 1:
        raise click.ClickException(
            f"job {jobid} is present on multiple hosts ({', '.join(found)}); "
            f"pass an explicit host, e.g. `vq status {found[0]} {jobid}`."
        )
    # Zero owners. Distinguish "searched and absent" from "couldn't search".
    unreachable = sorted(h for h, r in results.items() if r not in ("found", "absent"))
    skipped_down = sorted(h for h in down if h not in exclude)
    detail: list[str] = []
    if unreachable:
        detail.append(f"unreachable: {', '.join(unreachable)}")
    if skipped_down:
        detail.append(f"skipped (vq host down): {', '.join(skipped_down)}")
    suffix = f" ({'; '.join(detail)})" if detail else ""
    raise click.ClickException(f"job {jobid} not found on any reachable host{suffix}.")


def _note_searched_host(
    message: str,
    *,
    verb: str,
    jobid: str,
    searched_host: str,
    inferred: bool,
) -> str:
    """Append the searched host to a "no such job" error when the host was a guess.

    A bare ``vq status JOBID`` resolves to ``default_host`` and searches only
    there. When the job lives elsewhere the error was a flat "no such job:
    JOBID" that never said the search had been scoped to one host — so a
    polling loop could report a healthy job as missing indefinitely (the
    2026-07 compute-a/slurm-cluster case: 890 s of blank status for two live jobs).

    Only fires for the inferred-host form and only on a genuine not-found, so
    an explicit ``vq status HOST JOBID`` and every other error are untouched.
    The literal ``no such job`` substring is preserved — ``vq fetch``'s
    locate-and-retry keys on it, as do several tests.
    """
    if not inferred or "no such job" not in message.lower():
        return message
    return (
        f"{message}\n"
        f"  searched only {searched_host!r} (default_host); no host was given. "
        f"If the job is on another host, name it — `vq {verb} HOST {jobid}` — "
        "or run `vq queue --all` to find which host holds it."
    )


def _resolve_job_host(
    cfg: config.Config,
    explicit_host: str | None,
    jobid: str,
    *,
    multi_user: bool | None = None,
    locate_when_default_up: bool = False,
) -> str:
    """Resolve the host for a per-job verb's ``JOBID`` / ``HOST JOBID`` form.

    * ``explicit_host`` given → returned unchanged (the operator named the
      target; we never second-guess it, even a down one).
    * no host, local ``default_host`` **not** marked down → ``default_host``.
    * no host, remote ``default_host`` **not** marked down → require one
      readable durable queue snapshot before returning ``default_host``.
      ``locate_when_default_up`` instead requests full ownership discovery;
      status uses that mode so a stale default cannot own a lookup.
    * no host, ``default_host`` **is** marked ``vq host down`` → it's
      unreachable from here, so locate ``jobid`` on the other up hosts
      (v0.11.0 *Baran's Detour*) and return the owner.

    ``multi_user`` is computed from ``cfg`` when omitted (injectable for
    tests); it is consulted only when a remote queue must be read.
    """
    if explicit_host is not None:
        return explicit_host
    try:
        default = cfg.resolve_host(None)
    except config.ConfigError as e:
        raise click.UsageError(str(e)) from None
    entry = host_status.is_down(default)
    if entry is None and is_local_host(default):
        return default  # This process is the local queue authority.
    if multi_user is None:
        multi_user = _multi_user_active(cfg)
    if entry is None:
        if locate_when_default_up:
            return _locate_job_host(cfg, jobid, multi_user=multi_user)
        # A readable queue establishes reachability, not ownership.  Keep the
        # established single-default routing even when the job is absent or
        # duplicated; only refuse to send the requested action into an
        # authority whose durable queue cannot be read at all.
        _require_inferred_default_queue_snapshot(
            cfg,
            default,
            jobid,
            multi_user=multi_user,
        )
        return default
    if entry is not None:
        click.echo(
            f"vq: default_host {default!r} is marked down ({entry.describe()}); "
            f"locating {jobid} on other hosts...",
            err=True,
        )
    return _locate_job_host(cfg, jobid, multi_user=multi_user)


def _pick_auto_host(
    cfg: config.Config,
    job_cpus: int,
    pool: str | None = None,
    job_mem_mb: int | None = None,
) -> str:
    """v0.11.0: ``vq submit auto`` placement — gather the candidate hosts'
    overview (``pool``'s hosts if given, else ``default_pool``, else every
    configured host; admin-down boxes skipped without an SSH probe, like
    ``vq overview --all``) and return the memory-aware pick from
    :func:`vq.overview.recommend_host`. ``job_mem_mb`` (from ``--mem-mb``,
    or the automatic vibe-qc estimate) is the RAM-fit target — the binding
    constraint for QC; ``None`` falls back to core-fit. Raises
    ``ClickException`` if no host qualifies, naming the diagnostic command."""
    from vq import host_status
    from vq import overview as _overview_module

    overviews: list[_overview_module.HostOverview] = []
    host_names = cfg.resolve_pool_hosts(pool) or ["localhost"]
    down = host_status.load_down()
    for h in host_names:
        entry = down.get(h)
        if entry is not None:
            overviews.append(
                _overview_module.HostOverview(host=h, reachable=False, admin_down=entry.describe())
            )
            continue
        if is_local_host(h):
            overviews.append(
                _overview_module.gather_overview_local(h, cfg, multi_user=_multi_user_active(cfg))
            )
            continue
        try:
            host_cfg = cfg.host(h)
        except config.ConfigError as e:
            overviews.append(_overview_module.HostOverview(host=h, reachable=False, error=str(e)))
            continue
        if host_cfg.scheduler != "local":
            overviews.append(
                _overview_module.gather_scheduler_overview(
                    h,
                    host_cfg,
                    cfg,
                    multi_user=_multi_user_active(cfg),
                )
            )
            continue
        overviews.append(_overview_module.gather_overview_remote(h, host_cfg))

    chosen = _overview_module.recommend_host(overviews, job_cpus=job_cpus, job_mem_mb=job_mem_mb)
    if chosen is None:
        raise click.ClickException(
            "vq submit auto: no host qualifies — every configured host is "
            "unreachable / admin-down / drained / has a dead daemon. "
            "Inspect with `vq overview --all`."
        )
    return chosen


def _auto_estimate_job_mem_mb(cfg: config.Config, rest: list[str]) -> int | None:
    """v0.11.0: best-effort peak-memory estimate (MiB) for `vq submit auto`
    when no explicit ``--mem-mb`` was given. Runs the job's vibe-qc dry-run
    once with the configured ``estimate_python`` (which sets
    ``VIBEQC_DRY_RUN_ESTIMATE``) in an isolated temp copy of the script, then
    reads ``[memory].estimate_bytes`` from the manifest. Returns ``None`` — so
    the picker falls back to core-fit — when ``estimate_python`` is unset, the
    submit isn't a single ``.py`` file, vibe-qc isn't present / emits no
    estimate, or anything fails. Never raises; never writes into the user's
    source tree (the dry-run runs on a temp copy)."""
    if not cfg.estimate_python:
        return None
    # Single-file vibe-qc submit only: ``rest`` is exactly one local ``.py``.
    # Single-file submits are self-contained (the real run also gets only the
    # script), so a temp copy reproduces the run environment exactly.
    if len(rest) != 1:
        return None
    script = Path(rest[0])
    if script.suffix != ".py" or not script.is_file():
        return None
    from vq.vibeqc_preflight import vibeqc_dry_run_preflight

    try:
        with tempfile.TemporaryDirectory(prefix="vq-estimate-") as td:
            shutil.copy2(script, Path(td) / script.name)
            pre = vibeqc_dry_run_preflight(
                Path(td), [cfg.estimate_python, script.name], with_estimate=True
            )
    except Exception:  # best-effort — must never block the submit
        return None
    if pre is None or pre.estimate_bytes is None:
        return None
    # bytes → MiB, rounded up; clamp ≥1 so even a tiny job registers a target.
    return max(1, (pre.estimate_bytes + (1 << 20) - 1) >> 20)


_DELEGATE_TIMEOUT_UNSET = object()
_REMOTE_ADMIN_UPDATE_TIMEOUT_ENV = "VQ_REMOTE_ADMIN_UPDATE_TIMEOUT"
_REMOTE_ADMIN_UPDATE_TIMEOUT_MARGIN_SECONDS = 600.0
_FORWARDED_ADMIN_TIMEOUT_ENV_RULES = {
    "VQ_UPDATE_SCRIPT_TIMEOUT": (
        float(admin_module.UPDATE_SCRIPT_TIMEOUT_SECONDS),
        False,
    ),
    "VQ_BUILD_STALL_TIMEOUT": (
        float(admin_module.BUILD_STALL_TIMEOUT_SECONDS),
        True,
    ),
}


def _validated_timeout_environment_value(
    name: str,
    *,
    default: float,
    zero_allowed: bool,
) -> float:
    """Resolve one delegated timeout without the local helpers' fallback.

    The local admin helpers deliberately preserve their historical behavior of
    treating malformed values as unset. At an SSH boundary that would silently
    send a different deadline than the operator requested, so direct-remote
    updates reject malformed or out-of-domain values before any transport.
    """
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        value = math.nan
    valid = math.isfinite(value) and (value >= 0 if zero_allowed else value > 0)
    if not valid:
        relation = "greater than or equal to 0" if zero_allowed else "greater than 0"
        raise click.UsageError(
            f"{name} must be a finite number {relation} seconds for a "
            "delegated admin update"
        )
    return value


def _remote_admin_update_environment(
    host_cfg: config.HostConfig | None = None,
) -> dict[str, str]:
    """Return the complete, canonical wall/stall pair for a remote updater.

    Defaults are sent too. That is load-bearing for mixed-version fleets: an
    older remote vq may still default to the incident-era 1800/900-second caps,
    so forwarding only explicitly-set initiator values would preserve the bug.

    ``host_cfg`` supplies the target's ``update_script_timeout_seconds``, which
    replaces the built-in wall default; an explicitly set
    ``VQ_UPDATE_SCRIPT_TIMEOUT`` still wins (#32).
    """
    host_wall = (
        host_cfg.update_script_timeout_seconds if host_cfg is not None else None
    )
    values = {
        name: _validated_timeout_environment_value(
            name,
            default=(
                float(host_wall)
                if name == "VQ_UPDATE_SCRIPT_TIMEOUT" and host_wall is not None
                else default
            ),
            zero_allowed=zero_allowed,
        )
        for name, (default, zero_allowed) in _FORWARDED_ADMIN_TIMEOUT_ENV_RULES.items()
    }
    return {name: str(value) for name, value in values.items()}


def _validated_remote_admin_outer_timeout_override() -> float | None:
    """Return a finite positive local observer override, if explicitly set."""
    raw = os.environ.get(_REMOTE_ADMIN_UPDATE_TIMEOUT_ENV, "").strip()
    if not raw:
        return None
    try:
        override = float(raw)
    except ValueError:
        override = math.nan
    if not math.isfinite(override) or override <= 0:
        raise click.UsageError(
            f"{_REMOTE_ADMIN_UPDATE_TIMEOUT_ENV} must be a finite number "
            "greater than 0 seconds for a delegated admin update"
        )
    return override


def _remote_admin_update_timeout(
    command_timeout: float | None | object = _DELEGATE_TIMEOUT_UNSET,
    *,
    effective_remote_wall: float | None = None,
) -> float | None:
    """Outer SSH cap for delegated ``vq admin update`` commands.

    The command running on the remote host already owns the actual update
    timeout and marker heartbeat. This client-side cap only needs to be long
    enough not to mislabel an active remote compile as a stuck SSH command.
    Operators can override it with ``VQ_REMOTE_ADMIN_UPDATE_TIMEOUT``. An
    explicit value below the effective wall timeout plus the cleanup margin
    is rejected before SSH; the 15000-second default is only an unset floor.
    """
    if command_timeout is _DELEGATE_TIMEOUT_UNSET:
        if effective_remote_wall is None:
            raise ValueError(
                "effective_remote_wall is required for a delegated venv update"
            )
        base = effective_remote_wall
    elif command_timeout is None:
        # An unbounded inner command cannot safely be observed by a finite
        # outer subprocess timeout. SSH server-alive probes still detect a dead
        # transport. The command's own watchdogs remain authoritative.
        return None
    else:
        base = float(command_timeout)
    required = base + _REMOTE_ADMIN_UPDATE_TIMEOUT_MARGIN_SECONDS
    override = _validated_remote_admin_outer_timeout_override()
    if override is None:
        return max(
            transport.DEFAULT_REMOTE_ADMIN_UPDATE_TIMEOUT_SECONDS,
            required,
        )
    if override < required:
        raise click.UsageError(
            f"{_REMOTE_ADMIN_UPDATE_TIMEOUT_ENV} must be at least {required:g} "
            "seconds to cover the effective remote command timeout "
            f"({base:g} seconds) plus the "
            f"{_REMOTE_ADMIN_UPDATE_TIMEOUT_MARGIN_SECONDS:g}-second cleanup margin"
        )
    return override


def _host_cfg_or_none(cfg: config.Config, host: str) -> config.HostConfig | None:
    """The configured host, or None; the caller's own lookup reports errors."""
    try:
        return cfg.host(host)
    except config.ConfigError:
        return None


def _export_host_update_script_timeout(cfg: config.Config, host: str) -> None:
    """Make ``[hosts.HOST] update_script_timeout_seconds`` a local update's cap.

    The local updater, a detached child and that child's transient systemd unit
    all read ``VQ_UPDATE_SCRIPT_TIMEOUT`` from this process's environment, so
    exporting the key here reaches every one of them (#32). An explicitly set
    variable, including one a delegating driver forwarded, still wins.
    """
    if os.environ.get("VQ_UPDATE_SCRIPT_TIMEOUT", "").strip():
        return
    host_cfg = cfg.hosts.get(host)
    if host_cfg is None or host_cfg.update_script_timeout_seconds is None:
        return
    os.environ["VQ_UPDATE_SCRIPT_TIMEOUT"] = str(
        float(host_cfg.update_script_timeout_seconds)
    )


def _remote_admin_update_contract(
    *,
    aggregate: bool = False,
    host_cfg: config.HostConfig | None = None,
) -> tuple[dict[str, str], float | None]:
    """Capture one coherent timeout contract for a delegated venv update."""
    remote_env = _remote_admin_update_environment(host_cfg)
    if aggregate:
        # A remote registry can contain N sequential venv builds. One env's
        # wall cap plus margin is not a safe aggregate observer. Still reject
        # a malformed explicit observer override before any fan-out work.
        _validated_remote_admin_outer_timeout_override()
        outer_timeout = None
    else:
        outer_timeout = _remote_admin_update_timeout(
            effective_remote_wall=float(
                remote_env["VQ_UPDATE_SCRIPT_TIMEOUT"]
            )
        )
    return remote_env, outer_timeout


def _remote_admin_timeout_with_drain_wait(
    command_timeout: float | None,
    drain_wait_seconds: float,
) -> float | None:
    """Add a drain window without converting an unbounded cap to a TypeError."""
    outer = _remote_admin_update_timeout(command_timeout)
    return None if outer is None else outer + drain_wait_seconds


def _emit_remote_admin_timeout_summary(
    remote_env: Mapping[str, str],
    outer_timeout: float | None,
) -> None:
    """Report the effective delegated-vq deadlines without touching stdout."""
    wall = float(remote_env["VQ_UPDATE_SCRIPT_TIMEOUT"])
    stall = float(remote_env["VQ_BUILD_STALL_TIMEOUT"])
    outer = (
        "unbounded (remote --all batch)"
        if outer_timeout is None
        else f"{outer_timeout:g}s"
    )
    click.echo(
        "delegated venv update timeouts: "
        f"remote wall={wall:g}s; remote stall={stall:g}s; outer={outer}",
        err=True,
    )


def _delegate_to_remote(
    host: str,
    cfg: config.Config,
    *vq_args: str,
    stdin_data: str | None = None,
    timeout: float | None | object = _DELEGATE_TIMEOUT_UNSET,
    remote_env: Mapping[str, str] | None = None,
    mutating_admin: bool = False,
    reconcile_target: str | None = None,
) -> str:
    """Run ``<remote_vq> <vq_args>`` on the remote host and return stdout.

    Used by queue / status / kill, all of which simply delegate to the
    remote vq and print its output.

    ``stdin_data`` (v0.6.46): forwarded to the remote process's stdin.
    Used by the admin-update token-forwarding path so the bearer token
    can travel as ``--token-stdin`` rather than an argv element — keeps
    it out of the local ``ps -ef`` and the remote sh command line.

    CLI delegation deliberately makes one transport attempt. Reviewed
    replay-safe reads and staged transfers may opt into retry only at their
    lower-level transport call sites. ``mutating_admin`` adds fail-closed
    reconciliation advice for ambiguous write outcomes without changing
    read-only error rendering.
    """
    try:
        host_cfg = cfg.host(host)
    except config.ConfigError as e:
        raise click.UsageError(str(e)) from None
    extra: dict = {}
    if timeout is not _DELEGATE_TIMEOUT_UNSET:
        extra["timeout"] = timeout
    if remote_env is not None:
        extra["remote_env"] = remote_env
    try:
        proc = transport.run_remote_vq(host_cfg, *vq_args, stdin_data=stdin_data, **extra)
    except transport.RemoteOutcomeUnknown as e:
        if mutating_admin:
            raise click.ClickException(
                _remote_admin_unknown_outcome_message(
                    str(e), reconcile_target or host
                )
            ) from None
        raise click.ClickException(str(e)) from None
    except transport.RemoteLaunchError as e:
        if mutating_admin:
            raise click.ClickException(
                f"Remote admin command was not started; no remote mutation "
                f"was attempted. {e}"
            ) from None
        raise click.ClickException(str(e)) from None
    except transport.RemoteCommandError as e:
        # Scoped to the admin verbs, which are the only ones that classify an
        # outcome. Nothing else in vq exits 75/76/77 today, so this changes
        # no behaviour -- it keeps a verb that starts using one of those codes
        # for its own reasons from silently inheriting an admin meaning.
        outcome = (
            admin_module.admin_outcome_for_exit_code(e.returncode)
            if vq_args and vq_args[0] == "admin"
            else None
        )
        if outcome is None:
            raise click.ClickException(str(e)) from None
        # The remote vq classified this failure when it chose that exit
        # code. Relay it rather than folding it into "remote vq failed
        # (exit 75)" and exit 1, so a sweep driving updates from a laptop
        # branches exactly as one running on the driver does.
        raise _relayed_admin_outcome(
            outcome, e, as_json="--json" in vq_args,
        ) from None
    except transport.RemoteError as e:
        raise click.ClickException(str(e)) from None
    return proc.stdout


def _resolve_admin_token(
    cfg: config.Config,
    *,
    command_label: str,
    cli_token: str | None,
    token_stdin: bool,
    token_file: str | None,
    multi_user: bool | None = None,
) -> str | None:
    """Resolve and locally verify credentials for one admin write command."""
    from vq import auth as auth_module

    input_flag_count = sum([bool(cli_token), bool(token_stdin), bool(token_file)])
    if input_flag_count > 1:
        raise click.UsageError(
            "--token, --token-stdin, and --token-file are mutually "
            "exclusive; choose one (or fall back to $VQ_TOKEN)."
        )
    if cli_token:
        auth_module.warn_argv_token_exposure()
    token = auth_module.resolve_token(
        cli_token,
        token_stdin=token_stdin,
        token_file=token_file,
    )
    if multi_user is None:
        multi_user = _multi_user_active(cfg)
    if multi_user and not auth_module.verify_admin_token(token or ""):
        raise click.ClickException(
            f"{command_label}: token required in multi-user mode. "
            "Generate with `vq web init-token`, then pass via "
            "$VQ_TOKEN env var, --token-stdin, --token-file PATH, "
            "or (discouraged) --token TOKEN."
        )
    return token


def _remote_admin_auth(
    host: str,
    cfg: config.Config,
    token: str | None,
) -> tuple[list[str], str | None]:
    """Build the authentication tail for one delegated admin write.

    A host-local ``admin_token_file`` wins over a token resolved on the
    driver: multi-user hosts commonly have distinct tokens, and the remote
    process can read its own protected file without sending bearer bytes
    through the driver or SSH. Hosts without that setting retain the existing
    ``--token-stdin`` forwarding path.
    """
    try:
        remote_token_file = cfg.host(host).admin_token_file
    except config.ConfigError as e:
        raise click.UsageError(str(e)) from None
    if remote_token_file:
        return ["--token-file", remote_token_file], None
    if token:
        return ["--token-stdin"], token + "\n"
    return [], None


def _forward_admin_command(
    host: str,
    cfg: config.Config,
    remote_args: Sequence[str],
    *,
    token: str | None,
    append_localhost: bool,
    timeout: float | None | object = _DELEGATE_TIMEOUT_UNSET,
    remote_env: Mapping[str, str] | None = None,
    mutation_possible: bool = True,
    reconcile_target: str | None = None,
) -> str:
    """Delegate one admin write once, with target-specific authentication."""
    forwarded_args = list(remote_args)
    auth_args, remote_stdin = _remote_admin_auth(host, cfg, token)
    forwarded_args.extend(auth_args)
    if append_localhost:
        forwarded_args.append("localhost")

    delegate_kwargs: dict[str, object] = {}
    if timeout is not _DELEGATE_TIMEOUT_UNSET:
        delegate_kwargs["timeout"] = timeout
    if remote_env is not None:
        delegate_kwargs["remote_env"] = remote_env
    return _delegate_to_remote(
        host,
        cfg,
        *forwarded_args,
        stdin_data=remote_stdin,
        mutating_admin=mutation_possible,
        reconcile_target=(reconcile_target or host) if mutation_possible else None,
        **delegate_kwargs,
    )


_ADMIN_NO_DETACH_ENV = "VQ_ADMIN_NO_DETACH"
"""Escape hatch back to the attached delegated update.

Set it to run the remote updater as a child of the SSH session again. It
exists for bisecting a transport problem against the old behaviour, not as a
supported mode: the attached shape is the one that lost three hosts on
2026-09-11.
"""

_DETACHED_LAUNCH_TIMEOUT_SECONDS = 600.0
"""SSH cap on the launch call, which only spawns and waits for activation."""

_DETACHED_OBSERVE_TIMEOUT_SECONDS = 120.0
"""SSH cap on one read-only poll."""

_DETACHED_POLL_INTERVAL_SECONDS = 15.0
"""Gap between polls once the transcript backlog is drained."""

_DETACHED_OBSERVATION_GRACE_SECONDS = 1800.0
"""How long the driver keeps re-attaching before it calls the outcome unknown.

A build survives a transport outage; the *driver* is what has to wait it out.
Half an hour covers a gateway flap, a suspended laptop, or a sshd restart
without ever asking the operator to decide whether a running build is lost.
"""

_DETACHED_UNCONFIRMED_GRACE_SECONDS = 120.0
"""How long to keep looking for a run nothing has ever confirmed exists.

The long grace above protects a build we have *seen*. Before the first
successful observation there may be no build at all -- an ambiguous launch to
a host that then went unreachable is exactly the case the attached path
answered immediately with "outcome unknown". Waiting half an hour to say the
same thing would be a regression in every reachable-host failure, so an
unconfirmed run gets a short window and the old answer.
"""

_DETACHED_PROTOCOL_FAILURE_BUDGET = 3
"""Consecutive unparseable responses tolerated before the poll gives up.

A dropped connection is worth waiting out; a reply that *arrives* and is not
an observation is a disagreement about the protocol, and waiting cannot
resolve it. Without this bound such a reply is retried for the whole grace
window -- thirty minutes of a command that looks like a running build and is
really a misunderstanding.
"""

_DETACHED_NARRATION_RE = re.compile(
    r"\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+00:00 {2}"
)
"""Matches a transcript line written by ``RunLog.stamp`` (phase narration).

The transcript interleaves narration with the raw build stream. The attached
command only ever showed the operator the narration, so that is what the
driver echoes; the full build output stays on the host and comes back through
``vq admin logs``, exactly as before.
"""


class _DetachedUpdateUnsupported(RuntimeError):
    """The target's vq predates detached updates, so the legacy path applies."""


def _remote_admin_detach_unsupported(exc: transport.RemoteCommandError) -> bool:
    """Did the remote reject ``--detach`` as an unknown option?

    Narrow on purpose. A remote that never parsed the flag exited before it
    could pause a queue or touch a checkout, so falling back to the attached
    path is safe. Any other exit-2 usage error (an unknown env, say) is a real
    answer and must not be retried as something else.
    """
    if exc.returncode != 2:
        return False
    text = (exc.stderr or "").lower()
    return "no such option" in text and "--detach" in text


def _remote_admin_unknown_outcome_message(detail: str, target: str) -> str:
    """The advice for a write whose outcome SSH could not observe.

    One spelling, used by both the attached and the detached paths: an
    orchestration that learned this sentence should not have to learn a
    second one, and a detached run that truly went missing is the same
    problem the attached path always had.
    """
    return (
        f"{detail}\nRemote admin outcome is unknown: the command may still "
        "be running or may have completed. Do not retry it yet. "
        f"Run `vq admin status {target} --json`, inspect the update "
        "marker/LAST OK/log, and verify the exact live SHA or tag "
        "before deciding whether another update is safe."
    )


def _detached_update_child_argv(
    run_id: str,
    *,
    env: str | None,
    all_envs: bool,
    expected_tag: str | None,
    expected_sha: str | None,
    no_restart_daemon: bool,
    force: bool,
    as_json: bool,
    acknowledge_failed_marker: bool,
    update_script_args: Sequence[str],
    show_output: bool,
) -> list[str]:
    """Rebuild this invocation as the argv of its own detached child.

    ``sys.executable -m vq`` rather than whatever name invoked us: the child
    must be the same vq that received the delegated command, not whichever
    ``vq`` a PATH lookup would find. The bearer token is deliberately absent:
    the launcher adds ``--token-stdin`` or an owner-only ``--token-file``,
    whichever its spawn mechanism can deliver, and never puts a credential on
    argv, where every user on the host can read it.
    """
    argv = [sys.executable, "-m", "vq", "admin", "update"]
    if env is not None:
        argv.append(env)
    if all_envs:
        argv.append("--all")
    if acknowledge_failed_marker:
        argv.append("--acknowledge-failed-marker")
    if expected_tag is not None:
        argv.extend(["--tag", expected_tag])
    if expected_sha is not None:
        argv.extend(["--expected-sha", expected_sha])
    if no_restart_daemon:
        argv.append("--no-restart-daemon")
    if force:
        argv.append("--force")
    if as_json:
        argv.append("--json")
    for flag in update_script_args:
        argv.extend(["--update-script-arg", flag])
    if show_output:
        argv.append("--show-output")
    argv.extend(["--detach-child", "--detach-run-id", run_id])
    argv.append("localhost")
    return argv


def _detached_auto_update_child_argv(
    run_id: str,
    *,
    env: str | None,
    all_envs: bool,
    dry_run: bool,
    as_json: bool,
) -> list[str]:
    """Rebuild an ``admin auto-update`` invocation as its detached child's argv.

    The auto-update pbs-cluster of :func:`_detached_update_child_argv`, under the same
    two rules: ``sys.executable -m vq`` so the child is the vq that received
    the delegated command, and no bearer token on argv. The launcher adds
    ``--token-stdin`` or an owner-only ``--token-file``, whichever its spawn
    mechanism can deliver.
    """
    argv = [sys.executable, "-m", "vq", "admin", "auto-update"]
    if env is not None:
        argv.append(env)
    if all_envs:
        argv.append("--all")
    if dry_run:
        argv.append("--dry-run")
    if as_json:
        argv.append("--json")
    argv.extend(["--detach-child", "--detach-run-id", run_id])
    argv.append("localhost")
    return argv


def _forward_detached_admin_update(
    host: str,
    cfg: config.Config,
    remote_args: Sequence[str],
    *,
    token: str | None,
    timeout: float | None | object = _DELEGATE_TIMEOUT_UNSET,
    remote_env: Mapping[str, str] | None = None,
    reconcile_target: str | None = None,
) -> str:
    """Start the update on ``host`` detached, then follow it to its outcome.

    The launch call returns in seconds, so the SSH session that carries it is
    no longer the thing a two-hour rebuild depends on. What follows is a
    sequence of short read-only polls, each of which may fail and be retried
    without ambiguity, because observing a run mutates nothing.

    Raises :class:`_DetachedUpdateUnsupported` when the target's vq does not
    know the flag, leaving the caller free to use the attached path.

    ``timeout`` is accepted and deliberately unused: it is the outer cap for a
    session that carries a build, and this one carries a handshake. It is
    still validated upstream, and the attached fallback still honours it.
    """
    try:
        host_cfg = cfg.host(host)
    except config.ConfigError as e:
        raise click.UsageError(str(e)) from None
    target = reconcile_target or host
    run_id = admin_detached.new_run_id()
    auth_args, remote_stdin = _remote_admin_auth(host, cfg, token)
    launch_args = [
        *remote_args,
        "--detach",
        "--detach-run-id",
        run_id,
        *auth_args,
        "localhost",
    ]
    adopted = False
    try:
        launch_extra: dict[str, Any] = {}
        if remote_env is not None:
            launch_extra["remote_env"] = remote_env
        launched = transport.run_remote_vq(
            host_cfg,
            *launch_args,
            stdin_data=remote_stdin,
            timeout=_DETACHED_LAUNCH_TIMEOUT_SECONDS,
            **launch_extra,
        )
    except transport.RemoteOutcomeUnknown as e:
        # The launch response went missing, but the run id did not: it was
        # chosen here, before the call. So this is adoptable rather than
        # unknown -- observation decides whether anything started.
        adopted = True
        click.echo(
            f"delegated update launch response lost ({e}); adopting run "
            f"{run_id} on {host} by observation",
            err=True,
        )
    except transport.RemoteLaunchError as e:
        raise click.ClickException(
            "Remote admin command was not started; no remote mutation "
            f"was attempted. {e}"
        ) from None
    except transport.RemoteCommandError as e:
        if _remote_admin_detach_unsupported(e):
            raise _DetachedUpdateUnsupported(str(e)) from None
        # The target refused before detaching anything, and classified the
        # refusal when it chose its exit code. Relay it exactly as the attached
        # path does, rather than folding a lock, a marker or a gate into exit 1
        # and leaving a sweep to read a retry as a stop.
        outcome = admin_module.admin_outcome_for_exit_code(e.returncode)
        if outcome is not None:
            raise _relayed_admin_outcome(
                outcome, e, as_json="--json" in launch_args,
            ) from None
        raise click.ClickException(str(e)) from None
    except transport.RemoteError as e:
        raise click.ClickException(str(e)) from None
    else:
        _relay_detached_launch_receipt(host, run_id, launched.stdout)
    return _poll_detached_admin_update(
        host_cfg,
        run_id=run_id,
        target=target,
        adopted=adopted,
    )


def _relay_detached_launch_receipt(host: str, run_id: str, stdout: object) -> None:
    """Tell the operator how the build was detached, and any doubt about it.

    The warning is computed on the target, where lingering and the user manager
    can actually be probed; left there it would protect nobody. An unreadable
    receipt is not an error: observation, not this receipt, is the authority on
    the run.
    """
    try:
        receipt = json.loads(stdout) if isinstance(stdout, str) else None
    except ValueError:
        receipt = None
    details = receipt if isinstance(receipt, dict) else {}
    mechanism = details.get("mechanism")
    unit = details.get("unit")
    warning = details.get("warning")
    how = f" via {mechanism}" if isinstance(mechanism, str) else ""
    if isinstance(unit, str):
        how += f" unit {unit}"
    click.echo(
        f"delegated update running detached on {host} as {run_id}{how}", err=True
    )
    if isinstance(warning, str) and warning:
        click.echo(f"warning: {host}: {warning}", err=True)


def _poll_detached_admin_update(
    host_cfg: config.HostConfig,
    *,
    run_id: str,
    target: str,
    adopted: bool,
) -> str:
    """Follow one detached run to its terminal receipt and return its payload.

    Transport failures here are survivable by construction: the run is not a
    child of any of these connections, so a poll that fails costs one poll.
    Only an outage that outlasts the grace window, or a run whose process died
    without publishing a receipt, is reported as an unknown outcome.
    """
    offset = 0
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    started = time.monotonic()
    last_observed = started
    protocol_failures = 0
    confirmed = False
    reported_outage = False
    # Look once before sleeping: an update that refuses immediately (a marker
    # already present, a lane already converged) should answer as fast as the
    # attached command did, not one poll interval later.
    poll_immediately = True
    while True:
        if not poll_immediately:
            time.sleep(_DETACHED_POLL_INTERVAL_SECONDS)
        poll_immediately = False
        now = time.monotonic()
        try:
            proc = transport.run_remote_vq(
                host_cfg,
                "admin",
                "observe-update",
                run_id,
                # Explicit, for the same reason the delegated update appends a
                # `localhost` positional: the run lives on the host we are
                # talking to, and a target whose own default_host is another
                # machine would otherwise forward the observation there and
                # report a run that host has never heard of.
                "--host",
                "localhost",
                "--offset",
                str(offset),
                "--json",
                timeout=_DETACHED_OBSERVE_TIMEOUT_SECONDS,
                # No transport-level retry: this loop is already the retry,
                # and it backs off on its own schedule. Stacking the two would
                # double the delay while hiding half of it from the grace
                # accounting that decides when to call an outcome unknown.
            )
            observed = admin_detached.parse_observation(json.loads(proc.stdout))
        except transport.RemoteError as exc:
            # A run this driver has seen alive is worth waiting out; one that
            # has never been confirmed may not exist at all, and the operator
            # should get the old immediate answer rather than a long silence.
            grace = (
                _DETACHED_OBSERVATION_GRACE_SECONDS
                if confirmed
                else _DETACHED_UNCONFIRMED_GRACE_SECONDS
            )
            if (now - last_observed) >= grace:
                raise click.ClickException(
                    _remote_admin_unknown_outcome_message(
                        f"lost contact with detached update {run_id} on "
                        f"{host_cfg.ssh} for {grace:.0f}s ({exc})",
                        target,
                    )
                ) from None
            if not reported_outage:
                # Silence during an outage is what makes an operator reach for
                # a second update. Say once that the build is not the thing
                # that is broken, and that we are still watching it.
                reported_outage = True
                click.echo(
                    f"lost contact with {host_cfg.ssh} ({exc}); the detached "
                    f"update keeps running there. Re-attaching to {run_id} "
                    f"for up to {grace:.0f}s",
                    err=True,
                )
            log.debug("detached update poll failed (tolerated): %s", exc)
            continue
        except (admin_detached.DetachedRunError, ValueError) as exc:
            # The reply arrived and was not an observation. Retrying cannot
            # turn it into one, so spend a small budget on a transient oddity
            # and then say plainly what went wrong.
            protocol_failures += 1
            if protocol_failures >= _DETACHED_PROTOCOL_FAILURE_BUDGET:
                raise click.ClickException(
                    _remote_admin_unknown_outcome_message(
                        f"{host_cfg.ssh} answered {protocol_failures} "
                        f"observations of detached update {run_id} with "
                        f"something that is not an observation ({exc})",
                        target,
                    )
                ) from None
            log.debug("detached update observation was unreadable: %s", exc)
            continue
        last_observed = now
        protocol_failures = 0
        if reported_outage:
            click.echo(f"re-attached to {run_id} on {host_cfg.ssh}", err=True)
            reported_outage = False
        if observed.state != admin_detached.STATE_MISSING:
            confirmed = True

        chunk = base64.b64decode(observed.transcript_base64 or "")
        if chunk:
            text = decoder.decode(chunk)
            for line in text.splitlines():
                if _DETACHED_NARRATION_RE.match(line):
                    click.echo(line, err=True)
            offset = observed.transcript_next_offset
            # The spool is already bounded per response; drain a backlog with
            # back-to-back polls rather than one sleep per chunk, so a
            # terminal receipt is never held up by output harvesting.
            poll_immediately = True
            continue

        if observed.state == admin_detached.STATE_COMPLETED:
            payload = observed.payload or ""
            exit_code = observed.exit_code or 0
            if exit_code == 0:
                # Returned, never echoed: the per-host fan-out composes these
                # into one JSON document, and a direct write would land
                # outside it.
                return payload
            outcome = observed.outcome or admin_module.OUTCOME_FAILED
            if outcome not in admin_module.ADMIN_OUTCOMES:
                outcome = admin_module.OUTCOME_FAILED
            # Carried, never written here: under --all-hosts --json the fan-out
            # composes one document from every host, and a failing host's
            # result printed directly would land outside it. The caller that
            # owns stdout decides.
            raise _DetachedRunFailed(
                outcome,
                observed.error
                or f"admin update on {target} did not complete cleanly",
                payload=payload,
            )
        if observed.state == admin_detached.STATE_LOST:
            raise click.ClickException(
                _remote_admin_unknown_outcome_message(
                    f"detached update {run_id} on {host_cfg.ssh} is gone "
                    f"without a terminal receipt ({observed.detail}); its "
                    f"transcript is retained at "
                    f"{observed.transcript or '(none recorded)'}",
                    target,
                )
            )
        if observed.state == admin_detached.STATE_MISSING:
            # The host is answering and says it has no such run. Every way of
            # arriving here is terminal: waiting longer cannot conjure a record
            # the host says it does not have, and a loop with no exit would
            # hang the driver on a build that may well have finished.
            if confirmed:
                detail = (
                    f"the run record for {run_id} disappeared from "
                    f"{host_cfg.ssh} while it was being followed"
                )
            elif (now - started) >= _DETACHED_UNCONFIRMED_GRACE_SECONDS:
                detail = (
                    f"the {'adopted ' if adopted else ''}launch of {run_id} on "
                    f"{host_cfg.ssh} never recorded a run on the target"
                )
            else:
                continue
            # Close to proof that nothing ran, but not proof, so this keeps the
            # unknown-outcome advice rather than inviting a retry.
            raise click.ClickException(
                _remote_admin_unknown_outcome_message(detail, target)
            )


def _admin_detach_disabled() -> bool:
    """Whether the operator asked for the legacy attached delegation."""
    return os.environ.get(_ADMIN_NO_DETACH_ENV, "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _forward_venv_admin_update(
    host: str,
    cfg: config.Config,
    remote_args: Sequence[str],
    *,
    token: str | None,
    timeout: float | None | object = _DELEGATE_TIMEOUT_UNSET,
    remote_env: Mapping[str, str] | None = None,
    reconcile_target: str | None = None,
) -> str:
    """Delegate one managed-env update, detached wherever the target allows it.

    Detaching is the default because the alternative is the 2026-09-11
    failure: the build is a child of the SSH session, and the session is the
    least reliable part of the arrangement. A target whose vq is too old to
    detach still gets the attached path, because refusing to update a host
    until it has been updated is not a workable migration for the tool that
    performs the updates.
    """
    if not _admin_detach_disabled():
        try:
            return _forward_detached_admin_update(
                host,
                cfg,
                remote_args,
                token=token,
                timeout=timeout,
                remote_env=remote_env,
                reconcile_target=reconcile_target,
            )
        except _DetachedUpdateUnsupported as exc:
            click.echo(
                f"{host}: remote vq does not support detached updates; using "
                f"the attached path, where a dropped session kills the build "
                f"({exc})",
                err=True,
            )
    return _forward_admin_command(
        host,
        cfg,
        remote_args,
        token=token,
        append_localhost=True,
        timeout=timeout,
        remote_env=remote_env,
        reconcile_target=reconcile_target,
    )


def _delegate_job_lookup(
    cfg: config.Config,
    host: str,
    *remote_args: str,
    verb: str,
    jobid: str,
    searched_host: str,
    inferred: bool,
) -> str:
    """Delegate a per-job verb, enriching a remote "no such job" with the host.

    The 2026-07 incident went through this path: ``default_host`` was compute-a, the
    job was on slurm-cluster, and the remote compute-a returned a flat "no such job" that
    never said only compute-a had been searched. Wraps :func:`_delegate_to_remote`
    so the same host provenance the local path gets is added to the remote
    error too.
    """
    try:
        return _delegate_to_remote(host, cfg, *remote_args)
    except click.ClickException as exc:
        original = exc.format_message()
        enriched = _note_searched_host(
            original,
            verb=verb,
            jobid=jobid,
            searched_host=searched_host,
            inferred=inferred,
        )
        if enriched != original:
            raise click.ClickException(enriched) from None
        raise


def _stream_delegated_job_lookup(
    cfg: config.Config,
    host: str,
    *remote_args: str,
    verb: str,
    jobid: str,
    searched_host: str,
    inferred: bool,
) -> None:
    """Stream a delegated per-job verb while preserving host-scoped errors."""
    try:
        try:
            host_cfg = cfg.host(host)
        except config.ConfigError as e:
            raise click.UsageError(str(e)) from None
        try:
            with transport.stream_remote_vq(host_cfg, *remote_args) as stream:
                stdout = click.get_binary_stream("stdout")
                for chunk in iter(lambda: stream.stdout.read(65536), b""):
                    stdout.write(chunk)
                    stdout.flush()
        except transport.RemoteError as e:
            raise click.ClickException(str(e)) from None
    except click.ClickException as exc:
        original = exc.format_message()
        enriched = _note_searched_host(
            original,
            verb=verb,
            jobid=jobid,
            searched_host=searched_host,
            inferred=inferred,
        )
        if enriched != original:
            raise click.ClickException(enriched) from None
        raise


def _click_exception_is_remote_transport_failure(exc: click.ClickException) -> bool:
    """Best-effort test for SSH transport failures wrapped for Click."""
    text = exc.format_message().lower()
    return (
        "remote vq failed (exit 255)" in text
        or "ssh:" in text
        or "timed out" in text
        or "network is unreachable" in text
    )


def _scheduler_driver_host(cfg: config.Config, host: str) -> str | None:
    """Return the driver for an explicit scheduler host, if ``host`` is one."""
    if is_local_host(host):
        return None
    try:
        host_cfg = cfg.host(host)
    except config.ConfigError as e:
        raise click.UsageError(str(e)) from None
    if host_cfg.scheduler == "local":
        return None
    driver = host_cfg.scheduler_driver
    if driver is None:
        raise click.UsageError(
            f"scheduler host {host!r} has no scheduler_driver configured"
        )
    # Local aliases do not require a redundant [hosts.localhost] enrollment.
    # Validate remote drivers now so the eventual delegate error names the
    # scheduler config problem, not an opaque missing host at transport time.
    if is_local_host(driver):
        return driver
    try:
        cfg.host(driver)
    except config.ConfigError as e:
        raise click.UsageError(
            f"scheduler host {host!r} names driver {driver!r}, which "
            f"is not in config: {e}"
        ) from None
    return driver


def _local_host_label(host: str, scheduler_driver: str | None) -> str:
    """Describe the machine a local marker operation actually touched."""
    if scheduler_driver is not None:
        return f"this host (scheduler driver for {host})"
    return "this host"


def _scheduler_host_notice(cfg: config.Config, host: str, purpose: str) -> str | None:
    """Return a short daemonless-host notice, or ``None`` for normal hosts."""
    if is_local_host(host):
        return None
    try:
        host_cfg = cfg.host(host)
    except config.ConfigError as e:
        raise click.UsageError(str(e)) from None
    if host_cfg.scheduler == "local":
        return None
    driver = host_cfg.scheduler_driver or "(missing scheduler_driver)"
    return (
        f"(daemonless scheduler host; scheduler={host_cfg.scheduler}; "
        f"driver={driver}; {purpose})"
    )


def _scheduler_host_notice_json(
    cfg: config.Config,
    host: str,
    purpose: str,
) -> dict[str, object] | None:
    """JSON sibling of :func:`_scheduler_host_notice`."""
    if is_local_host(host):
        return None
    try:
        host_cfg = cfg.host(host)
    except config.ConfigError as e:
        raise click.UsageError(str(e)) from None
    if host_cfg.scheduler == "local":
        return None
    payload: dict[str, object] = {
        "scheduler_host": True,
        "scheduler": host_cfg.scheduler,
        "scheduler_dialect": host_cfg.scheduler_dialect,
        "driver": host_cfg.scheduler_driver,
        "message": purpose,
    }
    lane = host_cfg.scheduler_lane_metadata()
    if lane is not None:
        payload["scheduler_lane"] = lane
    return payload


def _programs_json_with_scheduler_lane(
    raw: str,
    host_cfg: config.HostConfig,
) -> str:
    """Project client-owned lane metadata onto delegated program records."""
    import json as _json

    try:
        decoded = _json.loads(raw)
    except (TypeError, ValueError):
        return raw
    if not isinstance(decoded, list):
        return raw
    lane = host_cfg.scheduler_lane_metadata()
    if lane is None:
        return raw
    projected: list[object] = []
    for item in decoded:
        if not isinstance(item, dict):
            projected.append(item)
            continue
        record = dict(item)
        record["scheduler_lane"] = lane
        projected.append(record)
    return _json.dumps(projected, indent=2, sort_keys=True)


def _admin_fanout_scheduler_skip(
    cfg: config.Config,
    host: str,
    *,
    as_json: bool,
) -> str | None:
    """Skip non-runtime and daemonless hosts in venv-update fan-outs.

    ``vq admin update ENV --all-hosts`` and ``vq admin auto-update ENV
    --all-hosts`` refresh venv programs on hosts that run a vq daemon.
    Fleet roles without managed runtime lanes (``vq-only``, ``alias``, and
    ``excluded``) must not inherit the driver's global ``[programs.*]``
    paths: those paths describe the driver's filesystem, not the remote
    coordinator or alias.

    Scheduler-only targets such as pbs-cluster do not have remote ``vq`` daemons; they
    have their own explicit ``vq admin update HOST`` cluster-maintenance path.
    """
    try:
        host_cfg = cfg.host(host)
    except config.ConfigError as e:
        raise click.UsageError(str(e)) from None
    if host_cfg.fleet_role in {"vq-only", "alias", "excluded"}:
        notice: dict[str, object] = {
            "skipped": True,
            "fleet_role": host_cfg.fleet_role,
            "reason": "host has no managed runtime lanes",
        }
        if as_json:
            import json as _json

            return _json.dumps(notice, sort_keys=True)
        return (
            f"skipped: fleet_role={host_cfg.fleet_role}; "
            "host has no managed runtime lanes."
        )

    notice = _scheduler_host_notice_json(
        cfg,
        host,
        "skipped by venv admin-update fan-out; use "
        f"`vq admin update {host}` for scheduler_update_command",
    )
    if notice is None:
        return None
    notice["skipped"] = True
    notice["reason"] = "daemonless scheduler host"
    notice["next_command"] = f"vq admin update {host}"
    if as_json:
        import json as _json

        return _json.dumps(notice, sort_keys=True)
    driver = notice.get("driver") or "(missing scheduler_driver)"
    scheduler = notice.get("scheduler") or "(unknown scheduler)"
    return (
        "skipped: daemonless scheduler host "
        f"(scheduler={scheduler}; driver={driver}). "
        f"Use `vq admin update {host}` for scheduler_update_command."
    )


def _scheduler_throttle_notice(cfg: config.Config, host: str) -> str | None:
    """Explain why CPUWeight throttling is not a scheduler-host control."""
    return _scheduler_host_notice(
        cfg,
        host,
        "CPUWeight/cgroup throttling applies only to local driver processes; "
        "scheduler jobs are controlled by the batch scheduler",
    )


def _reject_scheduler_throttle(cfg: config.Config, host: str) -> None:
    """Fail clearly for throttle operations against daemonless scheduler jobs."""
    driver = _scheduler_driver_host(cfg, host)
    if driver is None:
        return
    raise click.UsageError(
        f"vq throttle: scheduler host {host!r} is daemonless "
        f"(driver={driver!r}); CPUWeight/cgroup throttling does not control "
        "scheduler jobs. Use scheduler resource requests, or `vq kill "
        f"{host} JOBID` and resubmit with different resources."
    )


def _admin_update_scheduler_target(
    cfg: config.Config,
    candidate: str | None,
) -> str | None:
    """Return a scheduler host named by ``candidate`` for admin-update.

    Program names win over host names to preserve the long-standing
    ``vq admin update ENV [HOST]`` interpretation.
    """
    if candidate is None or candidate in cfg.programs:
        return None
    try:
        host_cfg = cfg.host(candidate)
    except config.ConfigError:
        return None
    if host_cfg.scheduler == "local":
        return None
    return candidate


def _require_managed_scheduler_update_target(
    host: str,
    host_cfg: config.HostConfig,
) -> None:
    """Reject aliases and unresolved/non-managed fleet roles before routing."""
    if host_cfg.fleet_role == "managed":
        return
    canonical = (
        f"; update canonical host {host_cfg.fleet_canonical_host!r} instead"
        if host_cfg.fleet_role == "alias"
        else ""
    )
    raise click.UsageError(
        f"scheduler update target {host!r} must have fleet_role='managed'; "
        f"resolved {host_cfg.fleet_role!r}{canonical}"
    )


def _classify_admin_update_target(
    cfg: config.Config,
    *,
    env: str | None,
    host: str | None,
    all_envs: bool,
    expected_sha: str | None,
) -> tuple[str | None, str | None]:
    """Return mutually exclusive scheduler-runtime and helper targets."""
    if env is not None and host is not None:
        try:
            explicit_host_cfg = cfg.host(host)
        except config.ConfigError:
            explicit_host_cfg = None
        runtime_is_configured = (
            explicit_host_cfg is not None
            and env in explicit_host_cfg.scheduler_runtime_deployments
        )
        if (
            explicit_host_cfg is not None
            and explicit_host_cfg.scheduler != "local"
            and (expected_sha is not None or runtime_is_configured)
        ):
            return host, None

    scheduler_helper_target = (
        _admin_update_scheduler_target(cfg, env)
        if not all_envs and host is None
        else None
    )
    return None, scheduler_helper_target


def _fanout_serial_requested() -> bool:
    """v0.7.6 *Tanenbaum's Mailbox*: check ``VQ_FANOUT_SERIAL`` escape
    hatch.

    Returns True when the operator has explicitly opted out of
    parallel per-host fan-out (e.g. for debug / log-ordering / a
    pathological host that confuses the thread pool). Truthy values:
    ``1``, ``true``, ``yes`` (case-insensitive).
    """
    val = os.environ.get("VQ_FANOUT_SERIAL", "").strip().lower()
    return val in ("1", "true", "yes", "on")


def _fanout_max_workers(n_hosts: int) -> int:
    """v0.7.6: pick the ThreadPoolExecutor ``max_workers`` for a
    per-host fan-out of ``n_hosts`` hosts.

    Honours ``VQ_FANOUT_WORKERS=N`` for operators that need to cap
    concurrency (e.g. tiny fleet vs. corporate SSH gateway rate
    limit). Default cap is 8 — enough to cover realistic
    multi-machine fleets, low enough not to swamp the local ssh
    multiplexer on a 20-host config.
    """
    env_val = os.environ.get("VQ_FANOUT_WORKERS", "").strip()
    if env_val:
        try:
            n = int(env_val)
            if n > 0:
                return min(n, max(1, n_hosts))
        except ValueError:
            pass
    return min(8, max(1, n_hosts))


def _safe_per_host(host_name: str, per_host_fn: Callable[[str], str]) -> str:
    """v0.7.6: catch per-host failures so one bad host can't take
    down the rest of the fan-out.

    Catches ``ClickException`` (the normal "host unreachable / config
    error" surface) and ``Exception`` (the catch-all for unexpected
    transport / parse errors). Returns a rendered error line in the
    same shape the pre-v0.7.6 serial implementation used, so the
    text output is unchanged for failing hosts.
    """
    try:
        return per_host_fn(host_name).rstrip()
    except click.ClickException as e:
        return f"(error querying {host_name}: {e.message})"
    except Exception as e:  # noqa: BLE001 — per-host isolation
        return f"(error querying {host_name}: {type(e).__name__}: {e})"


def _run_per_host(
    host_names: list[str],
    per_host_fn: Callable[[str], str],
    *,
    parallel: bool | None = None,
) -> dict[str, str]:
    """v0.7.6: call ``per_host_fn`` for each host, in parallel by
    default.

    Returns a dict keyed by host name so callers can render in
    whatever order they want — typically alphabetical for stability.
    With ``BatchMode=yes`` (v0.7.3) on every ssh/scp call, unreachable
    hosts fail fast (ConnectTimeout) instead of hanging on a password
    prompt, so concurrent fan-out is finally safe.

    ``parallel`` override: pass ``False`` to force serial (used by
    unit tests for deterministic ordering); leave ``None`` to honour
    ``VQ_FANOUT_SERIAL``. A single-host list is always serial.
    """
    if len(host_names) <= 1:
        return {h: _safe_per_host(h, per_host_fn) for h in host_names}

    use_serial = parallel is False or (parallel is None and _fanout_serial_requested())
    if use_serial:
        return {h: _safe_per_host(h, per_host_fn) for h in host_names}

    from concurrent.futures import ThreadPoolExecutor, as_completed

    results: dict[str, str] = {}
    max_workers = _fanout_max_workers(len(host_names))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_safe_per_host, h, per_host_fn): h for h in host_names}
        for fut in as_completed(futures):
            h = futures[fut]
            try:
                results[h] = fut.result()
            except Exception as e:  # noqa: BLE001 — defence-in-depth
                # _safe_per_host already catches everything; this is
                # belt-and-braces for an unexpected ThreadPool failure.
                results[h] = f"(error querying {h}: {type(e).__name__}: {e})"
    return results


def _aggregate_per_host_json(
    cfg: config.Config,
    per_host_fn: Callable[[str], str],
    *,
    parallel: bool | None = None,
) -> dict[str, object]:
    """v0.7.6 *Tanenbaum's Mailbox*: JSON sibling of
    :func:`_aggregate_per_host`.

    ``per_host_fn(host)`` must return a JSON string (the same shape
    the verb's local path emits to stdout). Returns a top-level
    object keyed by host name. Per-host failures land as
    ``{"error": "<message>"}`` so one bad host doesn't break the
    aggregate; the caller is responsible for raising a
    ``ClickException`` if any failures should exit non-zero.

    Parallel by default (same envelope as the text-mode aggregator);
    honours ``VQ_FANOUT_SERIAL=1`` and the ``parallel`` kwarg.
    """
    import json as _json

    if not cfg.hosts:
        return {}

    def _one(name: str) -> str:
        # Wrap per_host_fn so failures land as a JSON-shaped error
        # blob rather than propagating out of the executor.
        try:
            return per_host_fn(name)
        except click.ClickException as e:
            return _json.dumps({"error": str(e.message)})
        except Exception as e:  # noqa: BLE001 — per-host isolation
            return _json.dumps({"error": str(e)})

    all_names = sorted(cfg.hosts.keys())
    # v0.10.0 *Lampson's Hint*: skip probing administratively-down hosts.
    down = host_status.load_down()
    live = [h for h in all_names if h not in down]
    raws = _run_per_host(live, _one, parallel=parallel)
    payload: dict[str, object] = {}
    for h in all_names:
        if h in down:
            payload[h] = {"admin_down": down[h].reason, "since": down[h].since}
            continue
        raw = raws[h]
        try:
            payload[h] = _json.loads(raw)
        except _json.JSONDecodeError as e:
            payload[h] = {"error": f"remote returned invalid JSON: {e}"}
    return payload


def _aggregate_per_host(
    cfg: config.Config,
    per_host_fn: Callable[[str], str],
    *,
    parallel: bool | None = None,
) -> str:
    """v0.5.36: render one stacked per-host output block for ``--all``.

    Iterates configured hosts in sorted order, calls ``per_host_fn(host)``
    for each, and stacks the outputs under ``==== <host> ====`` banners.
    Per-host failures (SSH timeout, host down, config error) are caught
    and rendered inline as an error line under the host's banner so one
    bad host doesn't break the whole listing — the operator still sees
    the healthy hosts' state.

    Used by ``vq queue --all``, ``vq programs --all``,
    ``vq admin status --all``. Each command supplies a ``per_host_fn``
    closure that knows how to query / format a single host; this helper
    owns only the iteration + stacking shape.

    Empty config (no ``[hosts.X]`` blocks) returns an explanation rather
    than silent empty output — matches the pattern of other "no rows"
    paths elsewhere in the CLI.

    v0.7.6 *Tanenbaum's Mailbox*: per-host calls run in parallel via
    a ThreadPoolExecutor by default. Output ordering is deterministic
    alphabetical regardless of completion order — operators see the
    same stable banner sequence they did pre-v0.7.6. Escape hatches:
    ``VQ_FANOUT_SERIAL=1`` (env) or ``parallel=False`` (callers /
    tests).
    """
    if not cfg.hosts:
        return f"(no hosts configured — add [hosts.X] sections to {config.config_path()})"
    all_names = sorted(cfg.hosts.keys())
    # v0.10.0 *Lampson's Hint*: don't probe administratively-down hosts
    # (`vq host down`) — surface them inline as DOWN instead of paying an
    # SSH ConnectTimeout on every sweep.
    down = host_status.load_down()
    live = [h for h in all_names if h not in down]
    results = _run_per_host(live, per_host_fn, parallel=parallel)
    parts: list[str] = []
    for host_name in all_names:
        parts.append(f"==== {host_name} ====")
        if host_name in down:
            parts.append(f"(administratively down: {down[host_name].describe()})")
        else:
            parts.append(results[host_name])
        parts.append("")
    return "\n".join(parts).rstrip()


def _format_doctor_text(payload: dict[str, object]) -> str:
    header = f"== vq doctor: {payload['host']} =="
    if payload.get("as_driver") is not None:
        header = (
            f"== vq doctor: {payload['host']} "
            f"(as-driver {payload['as_driver']}) =="
        )
    lines = [header]
    checks = payload["checks"]
    assert isinstance(checks, list)
    for check in checks:
        assert isinstance(check, dict)
        label = "OK" if check.get("ok") else "FAIL"
        message = str(check["message"])
        first, *rest = message.splitlines()
        lines.append(f"{label} {check['name']}: {first}")
        lines.extend(f"  {line}" for line in rest)
    lines.append(f"verdict: {'ok' if payload.get('ok') else 'failed'}")
    return "\n".join(lines)


def _setup_cli_invocation_logging() -> None:
    """Install the ordinary client log after any command-specific guard."""
    # v0.6.16: configure client-side logging once per invocation.
    # Lives at <state_root>/client.log; rotates at 10 MB × 3
    # backups. VQ_LOG_LEVEL overrides level (default INFO);
    # VQ_LOG_DISABLED=1 skips setup entirely (test fixture
    # convenience). All logging.getLogger() calls scattered
    # across the codebase land here automatically.
    try:
        setup_cli_logging(paths.state_root() / "client.log")
        # One line per invocation captures the argv shape +
        # gives crash-debug a "what was the user doing?"
        # entry without needing the operator to enable
        # DEBUG manually.
        log.info(
            "cli invocation: argv=%s vq_version=%s",
            auth.redact_token_args(list(sys.argv)),
            __version__,
        )
    except Exception:  # pragma: no cover
        # setup_cli_logging already swallows OSError; any
        # other failure is a code bug we want to surface in
        # the next interactive run, not in a fatal CLI
        # invocation. Best-effort: skip.
        pass


class _ConfigErrorGroup(click.Group):
    """The root group, with :class:`config.ConfigError` rendered as an error.

    A broken ``config.toml`` is a file the user can fix, not a vq bug, so it
    belongs on stderr as one ``Error:`` message and a non-zero exit. About
    twenty call sites already convert it by hand (``_resolve_host``, ``vq
    doctor``, ``vq fetch``, ...), but a verb that loads config without a
    guard -- ``vq programs``, ``vq submit``, ``vq admin update`` -- printed a
    raw traceback and a pydantic dump instead. The rendering was therefore
    inconsistent per verb rather than absent, which is the worse failure: it
    reads as a crash, and the reader cannot tell which half of the output
    matters.

    Catching it once here covers every verb, including ones added later, and
    every nested group: click builds the subcommand's context and invokes it
    inside this frame (``Group.invoke``), so ``vq admin update`` and a
    parameter callback that loads config are both in scope.

    The hand-written guards are untouched. They convert the exception before
    it reaches this frame, so every verb that already rendered cleanly keeps
    its exact wording and exit code; this is the floor beneath them, not a
    replacement for them.
    """

    def invoke(self, ctx: click.Context) -> Any:
        try:
            return super().invoke(ctx)
        except config.ConfigError as exc:
            # ClickException (exit 1), not UsageError (exit 2): the command
            # line was well-formed, the config file is not, so printing the
            # verb's usage block would point at the wrong thing.
            #
            # ``from None`` matches the per-verb guards: the chained
            # pydantic/tomllib cause is already quoted in the message, and
            # re-printing it as a traceback is what this exists to stop.
            raise click.ClickException(str(exc)) from None


@click.group(
    cls=_ConfigErrorGroup, context_settings={"help_option_names": ["-h", "--help"]}
)
@click.version_option(
    version=__version__,
    prog_name="vq",
    # Codename after the number, matching vibe-qc's banner and the format rule
    # in docs/version_compatibility.md. A version with no registered codename
    # renders bare rather than with empty quotes.
    #
    # The ``, version `` infix is click's default and is LOAD-BEARING: a
    # scheduler host's helper is identified by parsing this line
    # (``doctor._scheduler_remote_vq_check``, ``\bversion\s+([^\s,;]+)``), and
    # a fleet is routinely mixed-version, so an older driver parses a newer
    # helper's output. Dropping the infix made helper_version None on every
    # host. Keep the prefix byte-identical to click's default and append.
    message=f"%(prog)s, version {format_version(__version__)}",
)
@click.pass_context
def main(ctx: click.Context) -> None:
    """vq: submit and manage queued jobs on a remote machine."""
    if ctx.invoked_subcommand not in (
        "daemon",
        "tar-workspace",
        "tar-workdir",
        "tar-artifact",
        "runtime-python",
    ):
        # The tar-* commands are streaming-tar internal verbs that write
        # binary to stdout. The daemon group delays setup so EUID-0 `run` can
        # validate its privilege mode before creating a client log.
        _setup_cli_invocation_logging()


@main.command("runtime-python")
@click.option("--root", required=True, type=click.Path(path_type=Path))
@click.option("--expected-sha", help="Require this full activated commit before exec.")
@click.argument("arguments", nargs=-1, type=click.UNPROCESSED)
def runtime_python(root: Path, expected_sha: str | None, arguments: tuple[str, ...]) -> None:
    """Execute Python from the current immutable runtime slot.

    Stable wrappers can use: vq runtime-python --root /absolute/runtime/root
    -- "$@". Put Python flags after the -- separator. The launcher preserves
    cwd, arguments, streams, process identity and the interpreter's exit status.
    Missing or unverified current slots fail; there is no legacy fallback.

    This launches an already activated runtime. It does not install a wrapper,
    migrate a program registration, or make an in-place environment immutable.
    """
    from vq.runtime_slots import RuntimeSlotError, exec_current_python

    try:
        exec_current_python(root, list(arguments), expected_sha=expected_sha)
    except (RuntimeSlotError, OSError) as exc:
        raise click.ClickException(str(exc)) from None


@main.command("self-update")
@click.option("--staged-driver-archive-sha256", default=None, hidden=True)
@click.option(
    "--expected-sha",
    metavar="FULL_SHA",
    help="Install exactly this full 40-hex vq source commit.",
)
@click.option(
    "--accepted-report",
    metavar="vX.Y.Z",
    help=(
        "Select this explicit accepted fleet report and install its exact vq "
        "pin. The report is verified from origin/main."
    ),
)
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON.")
@click.option(
    "--token",
    "cli_token",
    default=None,
    metavar="TOKEN",
    help="Bearer token for the admin gate in multi-user mode. Prefer stdin/file.",
)
@click.option(
    "--token-stdin",
    is_flag=True,
    default=False,
    help="Read the admin bearer token from one line on stdin.",
)
@click.option(
    "--token-file",
    type=click.Path(dir_okay=False),
    default=None,
    metavar="PATH",
    help="Read the admin bearer token from an owner-only file.",
)
def self_update(
    staged_driver_archive_sha256: str | None,
    expected_sha: str | None,
    accepted_report: str | None,
    as_json: bool,
    cli_token: str | None,
    token_stdin: bool,
    token_file: str | None,
) -> None:
    """Update the local vq install through its managed admin lifecycle.

    Exactly one immutable selector is required. This command deliberately has
    no host, environment, force, or no-restart option: service provenance
    selects the local vq environment, concurrent rollout/update ownership is
    refused, and a verified daemon restart is part of success.
    """
    if (expected_sha is None) == (accepted_report is None):
        raise click.UsageError(
            "choose exactly one of --expected-sha FULL_SHA or "
            "--accepted-report vX.Y.Z"
        )
    report: fleet_release.FleetReleaseReport | None = None
    if expected_sha is not None:
        expected_sha = expected_sha.strip().lower()
        if re.fullmatch(r"[0-9a-f]{40}", expected_sha) is None:
            raise click.UsageError(
                "--expected-sha must be a full 40-character hex commit SHA"
            )
    if staged_driver_archive_sha256 is not None:
        try:
            admin_module.require_staged_driver_recovery_archive(staged_driver_archive_sha256)
        except admin_module.AdminError as exc:
            raise click.UsageError(str(exc)) from None
    cfg = config.load_config()
    token = _resolve_admin_token(
        cfg,
        command_label="vq self-update",
        cli_token=cli_token,
        token_stdin=token_stdin,
        token_file=token_file,
    )
    try:
        env, prog, _probe = admin_module.resolve_vq_self_update_target(cfg)
        # rollout_execution_lock uses one fleet-wide lock path for every ID.
        # Reusing it here fences self-update against both another self-update
        # and rollout-latest without inventing a second ownership mechanism.
        with (
            fleet_rollout.rollout_execution_lock("self-update"),
            admin_module.admin_update_ownership(),
            admin_module.toolset_lifecycle_lock(
                [prog], action="vq-self-update",
            ),
        ):
            # The ownership lock closes both sides of the marker gap: a
            # pre-existing marker blocks before report fetch, and an ordinary
            # admin update cannot acquire/modify refs between this check,
            # accepted-report authentication, and marker acquisition inside
            # update_env.
            admin_module._guard_admin_update_marker(
                force=False,
                envs=[env],
                host="localhost",
            )
            if accepted_report is not None:
                reports = fleet_release.report_repo(cfg)
                with admin_module.toolset_lifecycle_lock(
                    [], action="vq-self-update-report",
                    extra_resources=((
                        "checkout", str(admin_module._canonical_lifecycle_checkout(reports)),
                    ),),
                ):
                    report = fleet_release.discover_report(
                        accepted_report, repo=reports,
                        runner=admin_module._mutating_git_run,
                    )
                expected_sha = report.pins["vq"].sha
            assert expected_sha is not None
            repo = Path(prog.git_dir)
            fetch_rc, fetch_output = admin_module._run_git_fetch_sha(
                repo, expected_sha, prog.branch,
            )
            if fetch_rc != 0:
                raise admin_module.AdminError(
                    "could not fetch the exact self-update target before "
                    f"ancestry validation: {fetch_output[-1000:]}"
                )
            current_sha = admin_module.current_source_sha(repo)
            if current_sha is None:
                raise admin_module.AdminError(
                    "could not resolve the current self-update checkout SHA"
                )
            if current_sha != expected_sha:
                current_before_target = fleet_release.git_is_ancestor(
                    repo, current_sha, expected_sha,
                )
                target_before_current = fleet_release.git_is_ancestor(
                    repo, expected_sha, current_sha,
                )
                if current_before_target is True:
                    pass
                elif target_before_current is True:
                    raise admin_module.AdminError(
                        "self-update target is older than the currently "
                        "installed checkout; this managed interface only "
                        "permits forward, exact-descendant updates"
                    )
                else:
                    raise admin_module.AdminError(
                        "self-update target and current checkout are divergent "
                        "or ancestry could not be proved; refusing the move"
                    )
            result = admin_module.update_env(
                env,
                cfg,
                host="localhost",
                admin_token=token,
                expected_sha=expected_sha,
                restart_daemon=True,
                require_self_update=True,
                force=False,
            )
    except admin_module.AdminUpdateInProgress as exc:
        raise click.ClickException(str(exc)) from None
    except (
        admin_module.AdminError,
        fleet_release.FleetReleaseError,
        fleet_rollout.FleetRolloutError,
    ) as exc:
        raise click.ClickException(str(exc)) from None

    if as_json:
        payload = json.loads(admin_module.format_update_result_json(result))
        payload["self_update_selector"] = (
            {"kind": "accepted-report", "identity": accepted_report}
            if report is not None
            else {"kind": "expected-sha", "identity": expected_sha}
        )
        if report is not None:
            payload["accepted_report_path"] = report.source_path
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
    else:
        selector = (
            f"accepted report {accepted_report}"
            if report is not None
            else f"exact SHA {expected_sha}"
        )
        click.echo(f"== vq self-update: {selector} ==")
        click.echo(admin_module.format_update_result(result))
    if not result.success:
        raise SystemExit(1)


@main.command("source-sha")
@click.option(
    "--write-marker",
    metavar="SHA",
    help=(
        "Write the package-local SOURCE-SHA marker. Intended for scheduler "
        "helper install/update commands."
    ),
)
def source_sha(write_marker: str | None) -> None:
    """Print or write the exact source SHA marker for this vq helper."""
    if write_marker is not None:
        try:
            marker = admin_module.write_source_sha_marker(write_marker)
        except admin_module.AdminError as exc:
            raise click.ClickException(str(exc)) from None
        click.echo(f"{write_marker.lower()} {marker}")
        return
    status = admin_module.inspect_source_sha_marker()
    if status.stale:
        raise click.ClickException(
            f"SOURCE-SHA marker at {status.path} claims "
            f"{status.recorded_sha} but the package beside it has changed since "
            f"(recorded tree {status.recorded_tree_sha256}, actual "
            f"{status.actual_tree_sha256}). The marker is written after `pip "
            "install` and is not tracked by the wheel, so an upgrade leaves the "
            "old one behind. Re-run `vq source-sha --write-marker "
            "<full-git-sha>` as the installing user to restamp it."
        )
    if status.sha is None:
        raise click.ClickException(
            "no SOURCE-SHA marker installed; run "
            "`vq source-sha --write-marker <full-git-sha>` during helper install"
        )
    click.echo(status.sha)


@main.command("source-tree-sha256")
def source_tree_sha256() -> None:
    """Print a content-derived SHA-256 of the installed vq package."""
    try:
        digest = admin_module.source_tree_sha256()
    except admin_module.AdminError as exc:
        raise click.ClickException(str(exc)) from None
    click.echo(digest)


@main.command("source-identity")
def source_identity() -> None:
    """Print version, package digest and SOURCE-SHA as one JSON object.

    One remote round trip for `vq doctor`'s scheduler-helper check; a helper
    older than this command is asked `source-tree-sha256` and `source-sha`
    separately. Exits 0 whenever the answer was produced: a missing or stale
    marker is reported in `source_sha_error`, not as an exit code.
    """
    click.echo(json.dumps(admin_module.source_identity(), sort_keys=True))


@main.command("source-stage-prune", hidden=True)
@click.argument("stage_root", type=click.Path(path_type=Path))
@click.option(
    "--keep",
    type=click.IntRange(min=1),
    default=None,
    help=(
        "Stages to keep. Default: "
        f"{admin_module.SCHEDULER_STAGE_GENERATIONS_TO_KEEP} for helper "
        f"generations, {admin_module.RUNTIME_SOURCE_STAGES_TO_KEEP} with "
        "--runtime-source."
    ),
)
@click.option(
    "--runtime-source",
    is_flag=True,
    help=(
        "STAGE_ROOT is a 'runtime-source' upload staging root "
        "(<scratch_root>/.vq-admin/runtime-source), whose stages sit one per "
        "program with no 'generations' level."
    ),
)
@click.option("--preserve", type=click.Path(path_type=Path))
@click.option("--json", "as_json", is_flag=True, help="Emit structured JSON.")
def source_stage_prune(
    stage_root: Path,
    keep: int | None,
    runtime_source: bool,
    preserve: Path | None,
    as_json: bool,
) -> None:
    """Prune recognized scheduler stage directories.

    Without --runtime-source this prunes immutable scheduler-helper stage
    generations, which a deploy never prunes on its own: another deployment
    may still be using an older one.

    With --runtime-source it prunes source-upload staging instead. Those belong
    to exactly one deploy each, so a deploy that verifies now reclaims its own;
    this reclaims what older vq left behind and what failed deploys kept.
    """
    try:
        if runtime_source:
            result = admin_module.prune_runtime_source_stages(
                stage_root,
                keep=(
                    admin_module.RUNTIME_SOURCE_STAGES_TO_KEEP
                    if keep is None
                    else keep
                ),
                preserve=preserve,
            )
        else:
            result = admin_module.prune_scheduler_stage_generations(
                stage_root,
                keep=(
                    admin_module.SCHEDULER_STAGE_GENERATIONS_TO_KEEP
                    if keep is None
                    else keep
                ),
                preserve=preserve,
            )
    except admin_module.AdminError as exc:
        raise click.ClickException(str(exc)) from None
    payload = asdict(result)
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    click.echo(
        f"removed={len(result.removed)} retained={len(result.retained)} "
        f"skipped={len(result.skipped_unrecognized)}"
    )


def _finite_positive_seconds(
    _ctx: click.Context,
    _param: click.Parameter,
    value: float | None,
) -> float | None:
    """Reject unusable positive intervals before command work starts."""
    if value is None:
        return None
    if not math.isfinite(value) or value <= 0:
        raise click.BadParameter("must be a finite number greater than zero")
    return value


_MAX_TOP_WATCH_INTERVAL_SECONDS = 86_400.0


def _top_watch_interval(
    ctx: click.Context,
    param: click.Parameter,
    value: float | None,
) -> float | None:
    """Keep top refreshes inside the portable platform-sleep envelope."""
    value = _finite_positive_seconds(ctx, param, value)
    if value is not None and value > _MAX_TOP_WATCH_INTERVAL_SECONDS:
        raise click.BadParameter(
            f"must not exceed {_MAX_TOP_WATCH_INTERVAL_SECONDS:g} seconds"
        )
    return value


@main.command("doctor")
@click.argument("host", required=False)
@click.option(
    "--all",
    "all_hosts",
    is_flag=True,
    default=False,
    help="Diagnose every configured host. Mutually exclusive with HOST.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Emit the stable machine-readable preflight envelope.",
)
@click.option(
    "--timeout",
    type=float,
    callback=_finite_positive_seconds,
    default=2.0,
    show_default=True,
    help="Daemon RPC timeout in seconds for the ping probe.",
)
@click.option(
    "--check-timeout",
    type=float,
    callback=_finite_positive_seconds,
    default=doctor_module.DEFAULT_CHECK_TIMEOUT_SECONDS,
    show_default=True,
    help=(
        "Per-check outer deadline in seconds for doctor-owned remote "
        "subprocesses."
    ),
)
@click.option(
    "--admin-update",
    "admin_update",
    is_flag=True,
    default=False,
    help=(
        "Also check scheduler-host admin-update readiness "
        "(scheduler_update_command / scheduler_install_command)."
    ),
)
@click.option(
    "--as-driver",
    "as_driver",
    metavar="CANDIDATE",
    default=None,
    help=(
        "Rehearse a driver migration: run every driver-side check (daemon "
        "health, delegated scheduler-probe) against CANDIDATE instead of the "
        "configured scheduler_driver. Proves the candidate's own config, ssh "
        "routing, and scheduler clients can drive the host BEFORE any "
        "scheduler_driver is repointed. Scheduler hosts only."
    ),
)
def doctor(
    host: str | None,
    all_hosts: bool,
    as_json: bool,
    timeout: float,
    check_timeout: float,
    admin_update: bool,
    as_driver: str | None,
) -> None:
    """Client-side config + connectivity + daemon preflight."""
    import json as _json

    if all_hosts and host is not None:
        raise click.UsageError("--all is mutually exclusive with HOST")
    try:
        cfg = config.load_config()
    except config.ConfigError as exc:
        raise click.UsageError(str(exc)) from None

    if as_driver is not None:
        # Fail the whole invocation, not per-host: a mistyped candidate name
        # rendering as N per-host FAILs would read like a fleet problem.
        try:
            cfg.host(as_driver)
        except config.ConfigError as exc:
            raise click.UsageError(str(exc)) from None

    if all_hosts:
        # One cache per invocation: a fleet behind a shared bastion probes that
        # bastion once, not once per host.
        probe_cache: dict[tuple[str, int], ssh_probe.TcpProbe] = {}
        host_names = sorted(cfg.hosts)

        def _doctor_payload(host_name: str) -> dict[str, object]:
            return doctor_module.diagnose_host(
                cfg,
                host_name,
                timeout=timeout,
                check_timeout=check_timeout,
                admin_update=admin_update,
                probe_cache=probe_cache,
                as_driver=as_driver,
            )

        # Read-only per-host probes fan out in a bounded pool (v0.7.6
        # envelope: max 8 workers, VQ_FANOUT_SERIAL opt-out). The serial
        # sweep dominated `rollout-latest --dry-run` wall clock — every
        # scheduler host costs three remote-vq round trips on top of the
        # TCP probe, and none of them mutate state.
        if len(host_names) <= 1 or _fanout_serial_requested():
            payload = {name: _doctor_payload(name) for name in host_names}
        else:
            from concurrent.futures import ThreadPoolExecutor

            with ThreadPoolExecutor(
                max_workers=_fanout_max_workers(len(host_names))
            ) as pool:
                payload = dict(
                    zip(
                        host_names,
                        pool.map(_doctor_payload, host_names),
                        strict=True,
                    )
                )
            # Re-probe a transport failure once, serially, before reporting the
            # host as failed. Up to eight concurrent ssh sessions is enough to
            # push a host on a slow route past its probe timeout, and a
            # timed-out or exit-255 probe becomes `remote_vq: ok=False`, which
            # makes the whole host `ok: false`. That is indistinguishable in the
            # output from a real fault: coordinator reported `verdict: failed`
            # from one `--all` run on 2026-07-27 while answering 5/5 ok on its
            # own moments later.
            #
            # This is not cosmetic. `fleet_rollout.doctor_failures` turns any
            # not-ok in-scope host into `status="blocked"`, so a lost race here
            # blocks an entire rollout.
            #
            # Only transport failures are retried, and only once. A genuinely
            # unreachable host still fails -- it must, because a host that
            # cannot be probed must not be silently skipped by a rollout -- it
            # just now costs one extra serial attempt to say so.
            contended = [
                name
                for name, host_payload in payload.items()
                if not host_payload.get("ok")
                and _doctor_payload_hit_transport_failure(host_payload)
            ]
            for name in contended:
                payload[name] = _doctor_payload(name)
        if as_json:
            click.echo(_json.dumps(payload, indent=2, sort_keys=True))
        elif payload:
            click.echo(
                "\n\n".join(
                    _format_doctor_text(host_payload)
                    for host_payload in payload.values()
                )
            )
        else:
            click.echo(f"(no [hosts.X] configured in {config.config_path()})")
        if not all(bool(host_payload["ok"]) for host_payload in payload.values()):
            raise SystemExit(1)
        return

    target = host or cfg.default_host or "localhost"
    if as_driver is not None:
        try:
            target_cfg = cfg.host(target)
        except config.ConfigError:
            target_cfg = None
        if target_cfg is None or target_cfg.scheduler == "local":
            raise click.UsageError(
                f"--as-driver applies to scheduler hosts; {target!r} is not one"
            )
    payload = doctor_module.diagnose_host(
        cfg,
        target,
        timeout=timeout,
        check_timeout=check_timeout,
        admin_update=admin_update,
        as_driver=as_driver,
    )
    if as_json:
        click.echo(_json.dumps(payload, indent=2, sort_keys=True))
    else:
        click.echo(_format_doctor_text(payload))
    if not payload["ok"]:
        raise SystemExit(1)


def _scheduler_workspace_reaper(
    cfg: config.Config,
) -> Callable[[JobSpec], None]:
    dispatchers: dict[str, SchedulerDispatcher] = {}

    def _reap(spec: JobSpec) -> None:
        if spec.scheduler_target is None:
            return
        dispatcher = dispatchers.get(spec.scheduler_target)
        if dispatcher is None:
            dispatcher = scheduler_dispatcher_for(cfg.host(spec.scheduler_target))
            dispatchers[spec.scheduler_target] = dispatcher
        handle = scheduler_handle_for_spec(
            dispatcher,
            spec,
            job_id=spec.scheduler_job_id or spec.id,
        )
        dispatcher.cleanup_remote_workspace(handle)

    return _reap


@main.command()
@click.argument("host", required=False)
@click.option(
    "--json",
    "json_output",
    is_flag=True,
    default=False,
    help="Emit one JSON record per program (stable schema, see below) "
    "instead of the human-readable table. Use this from scripts that "
    "need the absolute paths (e.g. tests/integration_smoke.py looks up "
    "binaries here so the daemon's PATH doesn't matter).",
)
@click.option(
    "--all",
    "all_hosts",
    is_flag=True,
    default=False,
    help="v0.5.36: list programs on EVERY host in ~/.config/vq/config.toml. "
    "Output is stacked per-host (banner + table for each), so a quick "
    "visual scan shows which engines are installed where. Mutually "
    "exclusive with positional HOST. Composes with --json: emits one "
    "top-level JSON object keyed by host, matching other fleet JSON "
    "commands. Unreachable hosts show the error inline or as a per-host "
    "JSON error; healthy hosts still render.",
)
@click.option(
    "--require",
    "required_programs",
    multiple=True,
    metavar="NAME",
    help="Fail with exit 1 if program NAME is absent or not status=OK on "
    "the selected host. Repeat for several programs. With --all, checks "
    "every configured host. JSON stdout stays parseable; failures are "
    "reported on stderr.",
)
@click.option(
    "--require-any",
    "required_program_groups",
    multiple=True,
    metavar="NAME[,NAME...]",
    help="Fail with exit 1 unless at least one comma-separated NAME in the "
    "group is status=OK on the selected host. Repeat for independent "
    "alternative groups. Useful during managed-program renames, e.g. "
    "`--require-any vibeview-dev,vibe-view`.",
)
@click.option(
    "--require-companion",
    "companion_requirement_specs",
    multiple=True,
    metavar="PRIMARY=COMPANION",
    help="For each host that registers PRIMARY, fail with exit 1 unless "
    "PRIMARY and COMPANION both report status=OK. Hosts without PRIMARY "
    "are ignored. Repeat for several companion relationships.",
)
@click.option(
    "--require-clean",
    "clean_programs",
    multiple=True,
    metavar="NAME",
    help="Fail with exit 1 if program NAME has a dirty or unreadable git "
    "checkout. Repeat for several programs. Combine with --require NAME "
    "when the program must also be present and status=OK.",
)
@click.option(
    "--require-branch",
    "branch_requirement_specs",
    multiple=True,
    metavar="NAME=BRANCH",
    help="Fail with exit 1 if program NAME is not on BRANCH or its branch "
    "cannot be read. Repeat for several programs.",
)
@click.option(
    "--require-sha",
    "sha_requirement_specs",
    multiple=True,
    metavar="NAME=SHA",
    help="Fail with exit 1 if program NAME is not at SHA. Short SHA prefixes "
    "are accepted. Repeat for several programs.",
)
@click.option(
    "--require-version",
    "version_requirement_specs",
    multiple=True,
    metavar="NAME=VERSION",
    help="Fail with exit 1 if program NAME does not report import VERSION. "
    "Repeat for several programs.",
)
def programs(
    host: str | None,
    json_output: bool,
    all_hosts: bool,
    required_programs: tuple[str, ...],
    required_program_groups: tuple[str, ...],
    companion_requirement_specs: tuple[str, ...],
    clean_programs: tuple[str, ...],
    branch_requirement_specs: tuple[str, ...],
    sha_requirement_specs: tuple[str, ...],
    version_requirement_specs: tuple[str, ...],
) -> None:
    """List registered programs on HOST and check whether each is available.

    \b
    Forms:
      vq programs                 (default_host)
      vq programs HOST
      vq programs --json          (machine-readable; same fields, JSON array)
      vq programs --all --require vibeview-dev
      vq programs --all --require-any vibeview-dev,vibe-view
      vq programs --all --require-companion orca=orca_2mkl
      vq programs --all --require vibeqc-dev --require-clean vibeqc-dev
      vq programs --all --require-branch vibeqc-dev=main
      vq programs --all --require-sha vibeqc-dev=12dbff58
      vq programs --all --require-version vibeqc-release=0.15.28

    A program is anything chats can submit jobs against: CRYSTAL,
    Pcrystal, ORCA, vibeqc-dev, vibeqc-release, PySCF, Psi4, ...
    The registry lives in ~/.config/vq/config.toml under [programs.X];
    see `vq submit --help` for the [hosts.X.branches] cousin that
    handles named-interpreter routing for vibe-qc venvs.

    Human output columns:
      NAME      registry name (what you'd refer to in commands)
      KIND      binary | venv | import
      STATUS    OK, MISSING or UNHEALTHY
      DETAIL    path / git_dir / module being checked, or the reason

    MISSING means work dispatched here will fail: the binary, interpreter
    or checkout is absent, a pin does not match, or the runtime does not
    import. UNHEALTHY (venv programs) means the runtime loads but its
    healthcheck_command did not pass; healthcheck_status says whether the
    healthcheck could not start (a configuration problem), timed out, or
    ran and failed. Both are not OK, so --require treats them alike.

    \b
    --json output schema (one object per program, in a JSON array;
    common fields plus kind-specific fields):
      common:    name, kind, status, reason, description
      binary:    binary       (absolute path)
      venv:      python, git_dir, branch, update_script,
                 healthcheck_command, healthcheck_status,
                 import_check, import_symbols, import_version,
                 current_git_sha, current_git_describe,
                 current_git_branch, current_git_dirty
      import:    python, import_check, import_symbols, import_version
    Empty registry -> empty array `[]`. status is "OK", "MISSING" or
    "UNHEALTHY". healthcheck_status is "ok", "not-configured", "not-run"
    (an earlier check failed), "could-not-start", "timed-out" or "failed".
    Stable schema; v0.6.0's `vq admin update` will consume it too.

    v0.5.18 just lists + checks. v0.6.0's `vq admin update` will
    use the same [programs.X] table to refresh venvs.

    v0.5.36: ``--all`` walks every configured host and stacks the
    per-host outputs under host banners. With ``--json`` it emits a
    single top-level object keyed by host name, so fleet monitors can
    parse the whole response without scraping banners.

    ``--require NAME`` turns the listing into a health gate: after
    rendering, vq exits non-zero if any selected host lacks NAME or
    reports it as not OK. This is the recommended quick check for
    queue-wide managed tools such as ``vibeview-dev``.

    ``--require-any A,B`` is the migration-friendly sibling: each
    selected host must expose at least one OK program from the comma
    separated group. Prefer standardizing the registry name after the
    transition rather than leaving permanent aliases.

    ``--require-companion PRIMARY=COMPANION`` checks a conditional
    relationship. A host that registers PRIMARY must report it healthy and
    must also register a healthy COMPANION; a host without PRIMARY is outside
    that relationship and passes. Unavailable host inventories fail closed
    because the relationship cannot be verified.

    ``--require-clean NAME`` fails if NAME has local checkout changes
    or if its dirty state cannot be read from the program record.

    ``--require-branch NAME=BRANCH`` fails if NAME is not currently on
    BRANCH, using the ``current_git_branch`` field from the program record.

    ``--require-sha NAME=SHA`` fails if NAME's current Git SHA does not
    match SHA. A 40-character requirement needs exact full-SHA evidence;
    shorter SHA prefixes are accepted.

    ``--require-version NAME=VERSION`` fails if NAME's ``import_version``
    does not exactly match VERSION.
    """
    cfg = config.load_config()
    required_any_groups = _parse_required_program_groups(required_program_groups)
    companion_requirements = _parse_program_value_requirements(
        companion_requirement_specs,
        "--require-companion",
    )
    branch_requirements = _parse_program_value_requirements(
        branch_requirement_specs,
        "--require-branch",
    )
    sha_requirements = _parse_program_value_requirements(
        sha_requirement_specs,
        "--require-sha",
    )
    version_requirements = _parse_program_value_requirements(
        version_requirement_specs,
        "--require-version",
    )
    has_requirements = bool(
        required_programs
        or required_any_groups
        or companion_requirements
        or clean_programs
        or branch_requirements
        or sha_requirements
        or version_requirements
    )

    if all_hosts and host is not None:
        raise click.UsageError(
            "--all and HOST are mutually exclusive; --all walks every "
            "configured host, so naming one in addition is contradictory"
        )

    def _query_one(h: str, *, as_json: bool) -> str:
        """v0.5.36: per-host programs rendering. Local hosts read the
        config in-process; remote hosts delegate via SSH (the registry
        lives on the host that actually has the binaries -- see the
        chat-onboarding doc + docs/hosts.md). JSON is forwarded so
        the aggregate can preserve each host's machine-readable payload."""
        if is_local_host(h):
            if as_json:
                return _format_programs_json(cfg)
            return _format_programs_table(cfg)
        try:
            host_cfg = cfg.host(h)
        except config.ConfigError as e:
            raise click.UsageError(str(e)) from None
        if host_cfg.scheduler != "local" and host_cfg.remote_vq == "vq":
            notice = _scheduler_host_notice(
                cfg,
                h,
                "no remote vq daemon; use `vq scheduler-probe "
                f"{h}` or `vq doctor {h}` for scheduler health",
            )
            if notice is not None:
                if as_json:
                    import json as _json

                    payload = _scheduler_host_notice_json(
                        cfg,
                        h,
                        "program registry is managed by the scheduler driver/cluster setup",
                    )
                    return _json.dumps(payload, indent=2, sort_keys=True)
                return notice
        remote_args: list[str] = ["programs", "localhost"]
        if as_json:
            remote_args.append("--json")
        delegated = _delegate_to_remote(h, cfg, *remote_args).rstrip()
        if as_json and host_cfg.scheduler != "local":
            return _programs_json_with_scheduler_lane(delegated, host_cfg)
        return delegated

    if all_hosts:
        payload: dict[str, object] | None = None
        if json_output:
            payload = _aggregate_per_host_json(
                cfg,
                lambda h: _query_one(h, as_json=True),
            )
            click.echo(json.dumps(payload, indent=2, sort_keys=True))
        else:
            click.echo(
                _aggregate_per_host(
                    cfg,
                    lambda h: _query_one(h, as_json=False),
                )
            )
        if has_requirements:
            if payload is None:
                payload = _aggregate_per_host_json(
                    cfg,
                    lambda h: _query_one(h, as_json=True),
                )
            _exit_if_program_requirements_fail(
                payload,
                required_programs,
                required_any_groups,
                clean_programs,
                branch_requirements,
                sha_requirements,
                version_requirements,
                companion_requirements,
            )
        return

    explicit_host = host is not None
    host = _resolve_host(cfg, host)
    fallback_to_localhost = (
        not explicit_host
        and not json_output
        and not has_requirements
        and not is_local_host(host)
    )
    down_entry = host_status.is_down(host) if fallback_to_localhost else None
    if down_entry is not None:
        click.echo(
            f"vq: default_host {host!r} is marked down ({down_entry.describe()}); "
            "showing localhost programs instead.",
            err=True,
        )
        host = "localhost"
    try:
        click.echo(_query_one(host, as_json=json_output))
    except click.ClickException as exc:
        if fallback_to_localhost and _click_exception_is_remote_transport_failure(exc):
            click.echo(
                f"vq: default_host {host!r} is unreachable; showing localhost "
                f"programs instead. Use `vq programs {host}` to retry that host, "
                f"or `vq host down {host} --reason REASON` to mark it down.",
                err=True,
            )
            host = "localhost"
            click.echo(_query_one(host, as_json=False))
            return
        raise
    if has_requirements:
        payload = {
            host: json.loads(_query_one(host, as_json=True)),
        }
        _exit_if_program_requirements_fail(
            payload,
            required_programs,
            required_any_groups,
            clean_programs,
            branch_requirements,
            sha_requirements,
            version_requirements,
            companion_requirements,
        )


def _parse_required_program_groups(
    raw_groups: Sequence[str],
) -> tuple[tuple[str, ...], ...]:
    groups: list[tuple[str, ...]] = []
    for raw in raw_groups:
        names = tuple(name.strip() for name in raw.split(",") if name.strip())
        if not names:
            raise click.UsageError("--require-any requires at least one NAME")
        groups.append(names)
    return tuple(groups)


def _parse_program_value_requirements(
    raw_specs: Sequence[str],
    option_name: str,
) -> tuple[tuple[str, str], ...]:
    pairs: list[tuple[str, str]] = []
    for raw in raw_specs:
        if "=" not in raw:
            raise click.UsageError(f"{option_name} expects NAME=VALUE")
        name, value = (part.strip() for part in raw.split("=", 1))
        if not name or not value:
            raise click.UsageError(f"{option_name} expects NAME=VALUE")
        pairs.append((name, value))
    return tuple(pairs)


def _sha_matches(actual: object, expected: str) -> bool:
    actual_text = str(actual or "").strip().lower()
    expected_text = expected.strip().lower()
    return bool(
        actual_text
        and expected_text
        and (
            actual_text == expected_text
            or actual_text.startswith(expected_text)
        )
    )


def _program_requirement_failures(
    payload: Mapping[str, object],
    required_programs: Sequence[str],
    required_any_groups: Sequence[Sequence[str]] = (),
    clean_programs: Sequence[str] = (),
    branch_requirements: Sequence[tuple[str, str]] = (),
    sha_requirements: Sequence[tuple[str, str]] = (),
    version_requirements: Sequence[tuple[str, str]] = (),
    companion_requirements: Sequence[tuple[str, str]] = (),
) -> list[str]:
    """Return human-readable failures for ``vq programs --require``."""
    failures: list[str] = []
    if not payload:
        exact = [
            f"(no hosts): {name} unavailable (no hosts configured)"
            for name in required_programs
        ]
        grouped = [
            "(no hosts): one of "
            f"{', '.join(group)} unavailable (no hosts configured)"
            for group in required_any_groups
        ]
        clean = [
            f"(no hosts): {name} cleanliness unavailable (no hosts configured)"
            for name in clean_programs
        ]
        branches = [
            f"(no hosts): {name} branch {branch} unavailable "
            "(no hosts configured)"
            for name, branch in branch_requirements
        ]
        shas = [
            f"(no hosts): {name} sha {sha} unavailable (no hosts configured)"
            for name, sha in sha_requirements
        ]
        versions = [
            f"(no hosts): {name} version {version} unavailable "
            "(no hosts configured)"
            for name, version in version_requirements
        ]
        return exact + grouped + clean + branches + shas + versions
    for host, host_payload in sorted(payload.items()):
        if isinstance(host_payload, list):
            records = {
                str(record.get("name")): record
                for record in host_payload
                if isinstance(record, dict) and record.get("name") is not None
            }
            for name in required_programs:
                record = records.get(name)
                if record is None:
                    failures.append(f"{host}: {name} is not registered")
                    continue
                status = str(record.get("status") or "MISSING")
                if status != "OK":
                    reason = str(record.get("reason") or status)
                    failures.append(f"{host}: {name} is {status} ({reason})")
            for group in required_any_groups:
                ok = [
                    name
                    for name in group
                    if (
                        (record := records.get(name)) is not None
                        and record.get("status") == "OK"
                    )
                ]
                if ok:
                    continue
                details: list[str] = []
                for name in group:
                    record = records.get(name)
                    if record is None:
                        details.append(f"{name}=unregistered")
                        continue
                    status = str(record.get("status") or "MISSING")
                    reason = str(record.get("reason") or status)
                    details.append(f"{name}={status} ({reason})")
                failures.append(
                    f"{host}: none of {', '.join(group)} is OK "
                    f"({'; '.join(details)})"
                )
            for primary, companion in companion_requirements:
                primary_record = records.get(primary)
                if primary_record is None:
                    continue
                primary_status = str(primary_record.get("status") or "MISSING")
                if primary_status != "OK":
                    reason = str(primary_record.get("reason") or primary_status)
                    failures.append(
                        f"{host}: primary {primary} is "
                        f"{primary_status} ({reason})"
                    )
                record = records.get(companion)
                if record is None:
                    failures.append(
                        f"{host}: {primary} is registered but companion "
                        f"{companion} is not registered"
                    )
                    continue
                status = str(record.get("status") or "MISSING")
                if status != "OK":
                    reason = str(record.get("reason") or status)
                    failures.append(
                        f"{host}: {primary} companion {companion} is "
                        f"{status} ({reason})"
                    )
            for name in clean_programs:
                record = records.get(name)
                if record is None:
                    failures.append(f"{host}: {name} is not registered")
                    continue
                dirty = record.get("current_git_dirty")
                if dirty is False:
                    continue
                if dirty is True:
                    failures.append(f"{host}: {name} checkout is dirty")
                else:
                    failures.append(f"{host}: {name} dirty state is unknown")
            for name, expected_branch in branch_requirements:
                record = records.get(name)
                if record is None:
                    failures.append(f"{host}: {name} is not registered")
                    continue
                branch = record.get("current_git_branch")
                if branch == expected_branch:
                    continue
                if not branch:
                    failures.append(
                        f"{host}: {name} branch state is unknown "
                        f"(expected {expected_branch})"
                    )
                else:
                    failures.append(
                        f"{host}: {name} branch is {branch} "
                        f"(expected {expected_branch})"
                    )
            for name, expected_sha in sha_requirements:
                record = records.get(name)
                if record is None:
                    failures.append(f"{host}: {name} is not registered")
                    continue
                expected_sha_text = expected_sha.strip()
                full_sha = record.get("current_git_sha_full")
                actual_sha = (
                    full_sha
                    if full_sha
                    else (
                        None
                        if len(expected_sha_text) == 40
                        else record.get("current_git_sha")
                    )
                )
                if _sha_matches(actual_sha, expected_sha):
                    continue
                if not actual_sha:
                    failures.append(
                        f"{host}: {name} git SHA is unknown "
                        f"(expected {expected_sha})"
                    )
                else:
                    failures.append(
                        f"{host}: {name} git SHA is {actual_sha} "
                        f"(expected {expected_sha})"
                    )
            for name, expected_version in version_requirements:
                record = records.get(name)
                if record is None:
                    failures.append(f"{host}: {name} is not registered")
                    continue
                actual_version = record.get("import_version")
                if actual_version == expected_version:
                    continue
                if not actual_version:
                    failures.append(
                        f"{host}: {name} import version is unknown "
                        f"(expected {expected_version})"
                    )
                else:
                    failures.append(
                        f"{host}: {name} import version is {actual_version} "
                        f"(expected {expected_version})"
                    )
            continue

        if isinstance(host_payload, dict):
            detail = (
                host_payload.get("error")
                or host_payload.get("admin_down")
                or host_payload.get("notice")
                or "no program records"
            )
            for name in required_programs:
                failures.append(f"{host}: {name} unavailable ({detail})")
            for group in required_any_groups:
                failures.append(
                    f"{host}: one of {', '.join(group)} unavailable ({detail})"
                )
            for primary, companion in companion_requirements:
                failures.append(
                    f"{host}: companion check {primary}={companion} "
                    f"unavailable ({detail})"
                )
            for name in clean_programs:
                failures.append(f"{host}: {name} cleanliness unavailable ({detail})")
            for name, expected_branch in branch_requirements:
                failures.append(
                    f"{host}: {name} branch {expected_branch} unavailable ({detail})"
                )
            for name, expected_sha in sha_requirements:
                failures.append(
                    f"{host}: {name} sha {expected_sha} unavailable ({detail})"
                )
            for name, expected_version in version_requirements:
                failures.append(
                    f"{host}: {name} version {expected_version} unavailable "
                    f"({detail})"
                )
            continue

        for name in required_programs:
            failures.append(f"{host}: {name} unavailable (invalid program payload)")
        for group in required_any_groups:
            failures.append(
                f"{host}: one of {', '.join(group)} unavailable "
                "(invalid program payload)"
            )
        for primary, companion in companion_requirements:
            failures.append(
                f"{host}: companion check {primary}={companion} unavailable "
                "(invalid program payload)"
            )
        for name in clean_programs:
            failures.append(
                f"{host}: {name} cleanliness unavailable "
                "(invalid program payload)"
            )
        for name, expected_branch in branch_requirements:
            failures.append(
                f"{host}: {name} branch {expected_branch} unavailable "
                "(invalid program payload)"
            )
        for name, expected_sha in sha_requirements:
            failures.append(
                f"{host}: {name} sha {expected_sha} unavailable "
                "(invalid program payload)"
            )
        for name, expected_version in version_requirements:
            failures.append(
                f"{host}: {name} version {expected_version} unavailable "
                "(invalid program payload)"
            )
    return failures


def _exit_if_program_requirements_fail(
    payload: Mapping[str, object],
    required_programs: Sequence[str],
    required_any_groups: Sequence[Sequence[str]] = (),
    clean_programs: Sequence[str] = (),
    branch_requirements: Sequence[tuple[str, str]] = (),
    sha_requirements: Sequence[tuple[str, str]] = (),
    version_requirements: Sequence[tuple[str, str]] = (),
    companion_requirements: Sequence[tuple[str, str]] = (),
) -> None:
    failures = _program_requirement_failures(
        payload,
        required_programs,
        required_any_groups,
        clean_programs,
        branch_requirements,
        sha_requirements,
        version_requirements,
        companion_requirements,
    )
    if not failures:
        return
    click.echo("required program check failed:", err=True)
    for failure in failures:
        click.echo(f"  - {failure}", err=True)
    raise SystemExit(1)


@main.command(name="scheduler-probe")
@click.argument("host")
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Emit the raw probe result as JSON for `vq doctor` and monitors.",
)
def scheduler_probe(host: str, as_json: bool) -> None:
    """Detect the batch scheduler on HOST (Torque / PBS Pro / SGE / SLURM).

    SSHes in and reads scheduler version/liveness plus which client binaries
    are present, then prints the detected dialect and the ``scheduler`` /
    ``scheduler_dialect`` config to set for the v1.0 cluster backend
    (docs/pbs_dispatcher_backend_design.md §8). Read-only: it submits no job
    and changes nothing on HOST, so it is safe to run against a shared cluster.

    \b
      vq scheduler-probe pbs-cluster

    Turns an unknown scheduler into a detected one for the per-host config,
    instead of guessing the dialect at build time.
    """
    from vq import scheduler_probe as probe_mod  # noqa: PLC0415

    cfg = config.load_config()
    host_cfg = cfg.host(host)
    try:
        result = probe_mod.probe_host(host_cfg)
    except (SchedulerError, transport.RemoteError) as exc:
        raise click.ClickException(str(exc)) from exc
    if as_json:
        import json as _json

        click.echo(_json.dumps(probe_mod.to_json_dict(result), indent=2, sort_keys=True))
    else:
        click.echo(probe_mod.format_report(result, host))


def _program_record(name: str, prog: config.ProgramConfig) -> dict[str, object]:
    """Build the JSON record for one program. Schema is documented in
    the `vq programs` docstring; keep them in sync."""
    available = prog.availability_status()
    record: dict[str, object] = {
        "name": name,
        "kind": prog.kind,
        "status": available.status,
        "reason": available.reason,
        "description": prog.description,
    }
    if isinstance(prog, config.BinaryProgram):
        record["binary"] = prog.binary
    elif isinstance(prog, config.VenvProgram):
        record["python"] = prog.python
        record["git_dir"] = prog.git_dir
        record["branch"] = prog.branch
        record["update_script"] = prog.update_script
        record["healthcheck_command"] = prog.healthcheck_command
        record["healthcheck_status"] = available.healthcheck_status
        record["import_check"] = prog.effective_import_check()
        record["import_symbols"] = prog.import_symbols
        record["import_version"] = prog.import_version()
        record["current_git_sha"] = prog.current_git_sha()
        record["current_git_sha_full"] = prog.current_git_sha(full=True)
        record["current_git_describe"] = prog.current_git_describe()
        record["current_git_branch"] = prog.current_git_branch()
        record["current_git_dirty"] = prog.current_git_dirty()
        record["expected_git_sha"] = prog.expected_git_sha
        record["expected_import_version"] = prog.expected_import_version
    elif isinstance(prog, config.ImportProgram):
        record["python"] = prog.python
        record["import_check"] = prog.import_check
        record["import_symbols"] = prog.import_symbols
        record["import_version"] = prog.import_version()
    return record


def _format_programs_json(cfg: config.Config) -> str:
    """JSON-array output for `vq programs --json`. Empty registry -> `[]`.

    Sorted by program name so the stream is deterministic (handy for
    scripts that diff successive snapshots)."""
    import json

    records = [_program_record(name, prog) for name, prog in sorted(cfg.programs.items())]
    return json.dumps(records, indent=2)


def _format_programs_table(cfg: config.Config) -> str:
    """Build the table for `vq programs` output."""
    if not cfg.programs:
        return (
            "no programs registered. Add [programs.NAME] sections to "
            f"{config.config_path()} (see vq programs --help)."
        )
    rows: list[tuple[str, str, str, str]] = [
        ("NAME", "KIND", "STATUS", "DETAIL"),
    ]
    for name, prog in sorted(cfg.programs.items()):
        available = prog.availability_status()
        rows.append((name, prog.kind, available.status, available.reason))
    widths = [max(len(r[i]) for r in rows) for i in range(4)]
    lines = ["  ".join(col.ljust(w) for col, w in zip(row, widths, strict=True)) for row in rows]
    return "\n".join(lines)


@main.command()
@click.argument("host", required=False)
@click.option(
    "-s",
    "--state",
    "states",
    multiple=True,
    metavar="STATE",
    help="Filter by durable lifecycle or exact scheduler phase (repeatable). "
    "`-s running` conservatively retains scheduler-owned RUNNING rows and "
    "reports how many were actually confirmed sched=running; phases such as "
    "queued, poll_failed, and finishing are independently queryable. E.g. "
    "`-s running -s pending` "
    "for both. Common shortcut: --active (every non-terminal job). "
    "Without any filter the listing includes all states "
    "(pre-v0.5.27 behaviour). Valid scheduler phase names also include "
    "queued, held, unpolled, poll_failed, finishing, marker_probe_failed, "
    "fetch_failed, reattach_failed, artifacts_unavailable, "
    "scheduler_reconciliation_quarantined, release_outcome_unknown, and "
    "scheduler_unknown.",
)
@click.option(
    "--active",
    is_flag=True,
    default=False,
    help="Show every non-terminal lifecycle state, including scheduler "
    "queued, poll_failed, and finishing rows. Composes with explicit -s flags: "
    "`--active -s completed` shows every non-terminal job PLUS completed.",
)
@click.option(
    "--show-archived",
    "show_archived",
    is_flag=True,
    default=False,
    help="v0.5.33: include archived jobs (those tarred away by "
    "`vq cleanup --archive`) in the listing. By default they're "
    "hidden — once you've archived a job you usually don't want "
    "it cluttering the day-to-day queue view. The job's spec is "
    "still on disk: `vq status JOBID` and `vq fetch JOBID` still "
    "work; only `vq queue` filters it out.",
)
@click.option(
    "--all",
    "all_hosts",
    is_flag=True,
    default=False,
    help="v0.5.36: aggregate the queue listing across EVERY host in "
    "~/.config/vq/config.toml. Output is stacked per-host (banner "
    "+ table for each), so the existing column-conditional logic "
    "(NAME, PRI) applies independently per host. Filters compose: "
    "`vq queue --all --active -s failed` runs the same filter on "
    "every host. Mutually exclusive with positional HOST. A host "
    "that's down or unreachable shows its error inline; healthy "
    "hosts still render.",
)
@click.option(
    "--tag",
    "tag_filters",
    multiple=True,
    type=str,
    metavar="TAG",
    help="v0.6.6: filter the listing to jobs that have ALL of the "
    "specified tags (AND-semantics; repeatable). "
    "`vq queue --tag experiment-12` shows just that experiment; "
    "`vq queue --tag experiment-12 --tag basisset-dev` shows "
    "their intersection. Composes with --active / -s / "
    "--show-archived / --all. Filter runs on the host with the "
    "specs (forwarded over SSH for remote hosts), so the wire "
    "doesn't carry rows that get dropped anyway.",
)
@click.option(
    "--array-group",
    "array_group",
    default=None,
    type=str,
    metavar="GID",
    help="v0.6.53: filter the listing to elements of the given "
    "`vq submit --array N` group (the 8-hex `array_group_id` "
    "the daemon recorded on each element's spec). After a "
    "30-element array submit, `vq queue --array-group <gid>` "
    "is the natural way to see just those rows without the "
    "rest of the queue scrolling past. Composes with -s / "
    "--active / --tag / --show-archived / --all (e.g. "
    "`vq queue --array-group <gid> -s failed` to find which "
    "elements failed). Forwarded over SSH for remote hosts.",
)
@click.option(
    "--collapse-arrays",
    "collapse_arrays",
    is_flag=True,
    default=False,
    help="v0.7.10 *McCarthy's List*: fold every ``--array N`` group "
    "into a single ``ARRAY 5P/25C/30`` row keyed on the array's "
    "group id, rather than rendering one row per element. The "
    "state column carries a compact per-state breakdown (P=PENDING, "
    "R=RUNNING, C=COMPLETED, F=FAILED, ...). Composes with the "
    "other filters; the per-group fold runs AFTER -s / --tag, so "
    "``vq queue --collapse-arrays -s failed`` shows only the array "
    "groups that have at least one failed element (and the "
    "breakdown reflects just those filtered elements). Non-array "
    "specs render unchanged in the same table.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="v0.6.21: emit a JSON array of JobSpec records instead of "
    "the text table. Same filters (-s, --active, --show-archived, "
    "--tag) apply. Used by `vq overview` and any other automation "
    "that wants the queue contents in machine-readable form. "
    "Forwarded over SSH for remote hosts. During rolling deployments, new "
    "scheduler-phase and --active JSON semantics are applied by the client "
    "to unfiltered remote JSON.",
)
def queue(
    host: str | None,
    states: tuple[str, ...],
    active: bool,
    show_archived: bool,
    all_hosts: bool,
    tag_filters: tuple[str, ...],
    array_group: str | None,
    collapse_arrays: bool,
    as_json: bool,
) -> None:
    """List jobs on HOST, optionally filtered by state.

    \b
    Forms:
      vq queue                            # all states, archived hidden
      vq queue HOST                       # all states on HOST
      vq queue --all                      # stacked listing across every host (v0.5.36)
      vq queue --all --json               # host-keyed JSON object
      vq queue --active                   # every non-terminal lifecycle state
      vq queue -s running                 # lifecycle-running reservations
      vq queue -s running -s pending      # explicit two-state filter
      vq queue --show-archived            # include archived jobs too
      vq queue --all --active             # active jobs on all hosts (mix is common)
      vq queue HOST --active              # explicit host + filter

    HOST is optional if ``default_host`` is set in ~/.config/vq/config.toml.

    v0.5.33: archived jobs (``vq cleanup --archive``) are hidden by
    default so the listing reflects the live queue. Pass
    ``--show-archived`` to see them too (annotated ``(archived)``).

    v0.5.36: ``--all`` walks every configured host. Per-host failures
    (unreachable, config error, SSH timeout) are caught and shown
    inline so one broken host doesn't hide the rest.
    """
    cfg = config.load_config()

    # v0.5.36: --all is mutually exclusive with a positional HOST.
    if all_hosts and host is not None:
        raise click.UsageError(
            "--all and HOST are mutually exclusive; --all walks every "
            "configured host, so naming one in addition is contradictory"
        )

    # Explicit filters use the monitor-facing projection.  --active remains a
    # distinct raw-nonterminal predicate so an unavailable scheduler never
    # makes a live owned job disappear from the ordinary watch view.
    selected: set[str] = set(states)
    # Validate state names locally so a typo errors clearly instead of
    # silently returning an empty listing.
    if selected:
        unknown = selected - QUEUE_FILTER_STATES
        if unknown:
            raise click.UsageError(
                f"unknown state(s): {sorted(unknown)}. "
                f"Valid: {sorted(QUEUE_FILTER_STATES)}"
            )

    # v0.6.6: --tag filter — AND-semantics. A spec passes iff every
    # requested tag is in spec.tags. Empty filter = no-op.
    required_tags = set(tag_filters)

    def _query_one(h: str) -> str:
        """v0.5.36: per-host queue rendering. Used both for the
        single-host path (h = resolved host) and the --all aggregation
        (called once per configured host). Keeps local/remote dispatch
        and filter forwarding in one place."""
        scheduler_target = h if _scheduler_driver_host(cfg, h) is not None else None
        if scheduler_target is not None:
            driver = _scheduler_driver_host(cfg, h)
            assert driver is not None
            h = driver
        if is_local_host(h):
            multi_user = _multi_user_active(cfg)
            specs = list_jobs(h, multi_user=multi_user)
            capacity_snapshot = None
            with contextlib.suppress(Exception):
                capacity_snapshot = capacity_module.read_daemon_capacity(
                    multi_user=multi_user,
                )
            if scheduler_target is not None:
                specs = [s for s in specs if s.scheduler_target == scheduler_target]
            if not show_archived:
                specs = [s for s in specs if not s.is_archived]
            if selected or active:
                specs = [
                    s
                    for s in specs
                    if matches_queue_state_filter(s, selected, active=active)
                ]
            if required_tags:
                specs = [s for s in specs if required_tags.issubset(set(s.tags))]
            if array_group is not None:
                specs = [s for s in specs if s.array_group_id == array_group]
            if as_json:
                import json as _json

                return _json.dumps(
                    [
                        _queue_json_row(
                            s,
                            h,
                            capacity_snapshot=capacity_snapshot,
                        )
                        for s in specs
                    ],
                    indent=2,
                    sort_keys=True,
                    default=str,
                )
            table = format_table(
                specs,
                collapse_arrays=collapse_arrays,
                capacity_snapshot=capacity_snapshot,
            )
            # STATUS-1: if the daemon is down but some rows still claim a
            # non-terminal state, those states are stale (nothing reconciles
            # them until the daemon restarts). Warn above the table.
            if any(not s.is_terminal for s in specs) and not is_daemon_serving(
                multi_user=_multi_user_active(cfg)
            ):
                # NB: deliberately avoid naming any job-state word here —
                # the listing-filter tests assert on the presence/absence of
                # state names in the output, and a warning that mentioned them
                # would collide.
                table = (
                    "⚠ vq daemon is down on this host — the states below "
                    "may be stale until it restarts and reconciles them.\n\n" + table
                )
            return table
        local_remote_state_filter = (
            scheduler_target is None
            and as_json
            and (
                active
                or not selected.issubset(_ROLLING_REMOTE_SOURCE_FILTER_STATES)
            )
        )
        remote_args: list[str] = ["queue", "localhost"]
        # Ordinary remote text stays source-rendered so daemon-staleness and
        # capacity diagnostics are preserved. JSON monitor filters can be
        # applied to unfiltered rows by this client during a rolling deploy.
        # Scheduler aliases always return driver JSON and are lane-filtered
        # below because raw reservation semantics must survive old drivers.
        if scheduler_target is None and not local_remote_state_filter:
            for st in sorted(selected):
                remote_args.extend(["-s", st])
            if active:
                remote_args.append("--active")
        if show_archived:
            remote_args.append("--show-archived")
        for tag in sorted(required_tags):
            remote_args.extend(["--tag", tag])
        if array_group is not None:
            remote_args.extend(["--array-group", array_group])
        # v0.7.10: forward --collapse-arrays so the per-group fold
        # runs on the host with the specs (the remote's listing
        # carries the full per-element table; we want the wire to
        # carry the already-folded summary).
        if collapse_arrays:
            remote_args.append("--collapse-arrays")
        if as_json or scheduler_target is not None or local_remote_state_filter:
            remote_args.append("--json")
        remote_output = _delegate_to_remote(h, cfg, *remote_args).rstrip()
        if scheduler_target is None:
            if as_json or local_remote_state_filter:
                normalized = _json_queue_rows_with_requested_handle_host(
                    remote_output,
                    h,
                )
                if local_remote_state_filter:
                    return _locally_filter_remote_queue_rows_json(
                        normalized,
                        host=h,
                        selected=selected,
                        active=active,
                    )
                return normalized
            return remote_output

        import json as _json

        try:
            raw_specs = _json.loads(remote_output or "[]")
        except _json.JSONDecodeError as exc:
            raise click.ClickException(
                f"scheduler driver {h!r} returned invalid queue JSON: {exc}"
            ) from exc
        if not isinstance(raw_specs, list):
            raise click.ClickException(
                f"scheduler driver {h!r} returned malformed queue JSON "
                "(expected a row array)"
            )

        specs: list[JobSpec] = []
        projected_rows: list[JobSpec | dict[str, object]] = []
        malformed_count = 0
        malformed_running_count = 0
        for raw in raw_specs:
            # One driver may own several scheduler aliases. Rows that cannot
            # even identify a target cannot safely be attributed to this
            # requested lane; a target-matching malformed reservation is kept
            # fail-closed below instead of aborting or disappearing.
            if (
                not isinstance(raw, dict)
                or raw.get("scheduler_target") != scheduler_target
            ):
                continue
            try:
                spec = JobSpec.model_validate(raw)
            except (TypeError, ValueError):
                if (
                    (selected or active)
                    and not _malformed_scheduler_row_matches_filter(
                        raw,
                        selected,
                        active=active,
                    )
                ):
                    continue
                malformed_count += 1
                if raw.get("state") == JobState.RUNNING.value:
                    malformed_running_count += 1
                projected_rows.append(
                    _fail_closed_scheduler_queue_row(
                        raw,
                        scheduler_target,
                        fallback_host=h,
                    )
                )
                continue
            # Reapply the monitor-facing predicate after reconstructing the
            # requested scheduler lane. This keeps text/JSON paths aligned
            # during a rolling deployment even when the driver is older.
            if (selected or active) and not matches_queue_state_filter(
                spec,
                selected,
                active=active,
            ):
                continue
            specs.append(spec)
            projected_rows.append(spec)
        if as_json:
            return _json.dumps(
                [
                    _queue_json_row(row, h)
                    if isinstance(row, JobSpec)
                    else row
                    for row in projected_rows
                ],
                indent=2,
                sort_keys=True,
                default=str,
            )
        table = format_table(
            specs,
            collapse_arrays=collapse_arrays,
            additional_unconfirmed_scheduler_running=malformed_running_count,
        )
        if malformed_count:
            warning = (
                f"warning: scheduler driver returned {malformed_count} "
                f"malformed reservation row(s) for {scheduler_target}; "
                "they are not confirmed running and cannot be rendered in "
                "the table. Inspect this lane with --json; raw scheduler "
                "ownership may still be active."
            )
            return f"{warning}\n\n{table}"
        return table

    if all_hosts:
        if as_json:
            payload = _aggregate_per_host_json(cfg, _query_one)
            click.echo(json.dumps(payload, indent=2, sort_keys=True))
            return
        click.echo(_aggregate_per_host(cfg, _query_one))
        return

    explicit_host = host
    host = _resolve_host(cfg, host)
    down_entry = host_status.is_down(host) if explicit_host is None else None
    if (
        down_entry is not None
        and not as_json
        and not is_local_host(host)
    ):
        click.echo(
            f"vq: default_host {host!r} is marked down ({down_entry.describe()}); "
            "showing localhost instead.",
            err=True,
        )
        click.echo(_query_one("localhost"))
        return
    try:
        click.echo(_query_one(host))
    except click.ClickException as exc:
        if (
            explicit_host is None
            and not as_json
            and not is_local_host(host)
            and _click_exception_is_remote_transport_failure(exc)
        ):
            click.echo(
                f"vq: default_host {host!r} is unreachable; showing localhost "
                f"instead. Use `vq list {host}` to retry that host, or "
                f"`vq host down {host} --reason REASON` to mark it down.",
                err=True,
            )
            click.echo(_query_one("localhost"))
            return
        raise


def _queue_json_row(
    spec: JobSpec,
    host: str,
    *,
    capacity_snapshot: capacity_module.DaemonCapacity | None = None,
) -> dict[str, object]:
    """Add queue-monitor derived fields to a JobSpec JSON row."""
    row = spec.model_dump(mode="json")
    row["effective_state"] = effective_queue_state(spec)
    row["scheduler_running_confirmed"] = scheduler_running_confirmed(spec)
    row["queue_handle"] = queue_handle_for_spec(spec, host)
    row["terminal_diagnosis"] = terminal_diagnosis_for_spec(spec)
    overages = pending_configured_capacity_overages(spec, capacity_snapshot)
    row["pending_over_capacity"] = (
        True
        if overages
        else (
            False
            if pending_configured_capacity_known(spec, capacity_snapshot)
            else None
        )
    )
    row["configured_capacity_overages"] = [
        overage.to_payload() for overage in overages
    ]
    return row


def _malformed_scheduler_row_matches_filter(
    row: Mapping[str, object],
    selected: set[str],
    *,
    active: bool,
) -> bool:
    """Fail-closed filter matching for a target-attributed invalid row.

    A malformed row cannot safely claim an exact scheduler phase. Raw
    lifecycle selectors still retain reservations, ``scheduler_unknown`` is
    the explicit diagnosis selector, and ``--active`` excludes only a known
    terminal lifecycle. An absent or invalid lifecycle is therefore retained
    as potentially active rather than treated as free capacity.
    """
    raw_state = row.get("state")
    terminal_values = {state.value for state in TERMINAL_STATES}
    return (
        (
            active
            and (
                not isinstance(raw_state, str)
                or raw_state not in terminal_values
            )
        )
        or (isinstance(raw_state, str) and raw_state in selected)
        or "scheduler_unknown" in selected
    )


def _fail_closed_scheduler_queue_row(
    row: Mapping[str, object],
    scheduler_target: str,
    *,
    fallback_host: str,
) -> dict[str, object]:
    """Preserve one invalid delegated row without trusting its projection."""
    projected = dict(row)
    projected["effective_state"] = "scheduler_unknown"
    projected["scheduler_running_confirmed"] = (
        False if row.get("state") == JobState.RUNNING.value else None
    )
    handle_host = queue_host_for_scheduler_target(
        scheduler_target,
        fallback_host,
    )
    projected["queue_handle"] = queue_handle_for_unvalidated_row(
        row,
        handle_host,
    )
    return projected


def _json_rows_with_requested_queue_handle_host(
    text: str, requested_host: str
) -> str:
    """Patch delegated JSON row arrays with the user's remote host alias."""
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return text
    if not isinstance(payload, list):
        return text
    for row in payload:
        if not isinstance(row, dict):
            continue
        fallback_host = row.get("scheduler_target")
        handle_host = queue_host_for_scheduler_target(
            fallback_host,
            requested_host,
        )
        unsafe_target = (
            fallback_host is not None
            and not scheduler_target_is_safe(fallback_host)
        )
        handle = row.get("queue_handle")
        if isinstance(handle, dict):
            handle["host"] = (
                handle_host
                if unsafe_target
                else normalize_delegated_queue_handle_host(
                    handle.get("host"),
                    handle_host,
                )
            )
        else:
            job_id = row.get("id")
            if not isinstance(job_id, str):
                job_id = row.get("jobid")
            row["queue_handle"] = {
                "job_id": job_id,
                "host": handle_host,
                "submitted_at": row.get("submitted_at"),
            }
    return json.dumps(payload, indent=2, sort_keys=True, default=str)


def _json_queue_rows_with_requested_handle_host(text: str, requested_host: str) -> str:
    """Normalize delegated queue JSON for host aliases and state projection."""
    normalized = _json_rows_with_requested_queue_handle_host(text, requested_host)
    try:
        payload = json.loads(normalized)
    except json.JSONDecodeError:
        return normalized
    if not isinstance(payload, list):
        return normalized
    for row in payload:
        if not isinstance(row, dict):
            continue
        try:
            spec = JobSpec.model_validate(row)
        except (TypeError, ValueError):
            target = row.get("scheduler_target")
            row["effective_state"] = "scheduler_unknown"
            row["scheduler_running_confirmed"] = (
                False
                if row.get("state") == "running" and target is not None
                else None
            )
            row["queue_handle"] = queue_handle_for_unvalidated_row(
                row,
                requested_host,
            )
            continue
        # Override, rather than setdefault: an older remote may emit the old
        # projection that flattened poll_failed/fence phases to running.
        row["effective_state"] = effective_queue_state(spec)
        row["scheduler_running_confirmed"] = scheduler_running_confirmed(spec)
    return json.dumps(payload, indent=2, sort_keys=True, default=str)


def _locally_filter_remote_queue_rows_json(
    text: str,
    *,
    host: str,
    selected: set[str],
    active: bool,
) -> str:
    """Apply new monitor filters to unfiltered rolling-deployment JSON."""
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise click.ClickException(
            f"remote host {host!r} returned invalid queue JSON: {exc}"
        ) from exc
    if not isinstance(payload, list):
        raise click.ClickException(
            f"remote host {host!r} returned malformed queue JSON "
            "(expected a row array)"
        )

    projected_rows: list[object] = []
    for row in payload:
        if not isinstance(row, dict):
            if not (active or "scheduler_unknown" in selected):
                continue
            projected_rows.append(
                {
                    "raw_row": row,
                    "effective_state": "scheduler_unknown",
                    "scheduler_running_confirmed": None,
                    "queue_handle": {
                        "job_id": None,
                        "host": host,
                        "submitted_at": None,
                    },
                }
            )
            continue
        try:
            spec = JobSpec.model_validate(row)
        except (TypeError, ValueError):
            if not _malformed_scheduler_row_matches_filter(
                row,
                selected,
                active=active,
            ):
                continue
            projected_rows.append(row)
            continue
        if not matches_queue_state_filter(spec, selected, active=active):
            continue
        projected_rows.append(row)

    return json.dumps(
        projected_rows,
        indent=2,
        sort_keys=True,
        default=str,
    )


def _json_with_requested_queue_handle_host(text: str, requested_host: str) -> str:
    """Patch delegated remote JSON with the user's host alias.

    Remote `vq status/logs localhost JOBID --json` runs on the remote daemon,
    so an older/newer remote may report its local alias as `localhost`. The
    client knows the host the operator asked for; keep scheduler targets
    intact, but rewrite ordinary localhost handles for cockpit back-references.
    """
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return text
    if not isinstance(payload, dict):
        return text

    fallback_host = payload.get("scheduler_target")
    handle_host = queue_host_for_scheduler_target(
        fallback_host,
        requested_host,
    )
    unsafe_target = (
        fallback_host is not None
        and not scheduler_target_is_safe(fallback_host)
    )
    payload["host"] = (
        handle_host
        if unsafe_target
        else normalize_delegated_queue_handle_host(
            payload.get("host"),
            handle_host,
        )
    )
    handle = payload.get("queue_handle")
    if isinstance(handle, dict):
        handle["host"] = (
            handle_host
            if unsafe_target
            else normalize_delegated_queue_handle_host(
                handle.get("host"),
                handle_host,
            )
        )
    else:
        job_id = payload.get("id")
        submitted_at = payload.get("submitted_at")
        if isinstance(job_id, str):
            payload["queue_handle"] = {
                "job_id": job_id,
                "host": handle_host,
                "submitted_at": submitted_at if isinstance(submitted_at, str) else None,
            }
    return json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n"


def _json_status_with_requested_handle_host(
    text: str,
    requested_host: str,
) -> str:
    """Normalize delegated status JSON across rolling vq deployments."""
    normalized = _json_with_requested_queue_handle_host(text, requested_host)
    try:
        payload = json.loads(normalized)
    except json.JSONDecodeError:
        return normalized
    if not isinstance(payload, dict):
        return normalized
    scheduler_projection_keys = {
        "scheduler_id",
        "pbs_state_label",
        "fetch_state_label",
        "scheduler_status_label",
    }
    for key in scheduler_projection_keys:
        payload.pop(key, None)
    try:
        spec = JobSpec.model_validate(payload)
    except (TypeError, ValueError):
        payload["effective_state"] = "scheduler_unknown"
        payload["scheduler_running_confirmed"] = (
            False
            if (
                payload.get("state") == JobState.RUNNING.value
                and payload.get("scheduler_target") is not None
            )
            else None
        )
        payload["host"] = requested_host
        payload["queue_handle"] = queue_handle_for_unvalidated_row(
            payload,
            requested_host,
        )
        if payload.get("scheduler_target") is not None:
            payload.update(
                {
                    "pbs_state_label": "scheduler_unknown",
                    "scheduler_status_label": "scheduler_unknown",
                    "fetch_state_label": (
                        "scheduler phase or ownership unknown; "
                        "remote workspace state unknown"
                    ),
                }
            )
    else:
        # Override stale fields emitted by an older remote exactly as the
        # delegated queue JSON path does.
        payload["effective_state"] = effective_queue_state(spec)
        payload["scheduler_running_confirmed"] = scheduler_running_confirmed(spec)
        payload.update(scheduler_status_projection_for_spec(spec))
    return json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n"


@main.command()
@click.argument("host_or_jobid")
@click.argument("jobid_if_host", required=False)
@click.option(
    "-n",
    "--tail",
    type=click.IntRange(min=0),
    default=50,
    show_default=True,
    help="Show last N lines of stdout/stderr (0 = full output).",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="v0.6.14: emit JSON instead of text. Top-level keys mirror "
    "JobSpec fields plus `stdout` / `stderr` (tailed to -n). "
    "Used by `vq wait` and any other automation that wants a "
    "stable machine-readable view.",
)
def status(
    host_or_jobid: str,
    jobid_if_host: str | None,
    tail: int,
    as_json: bool,
) -> None:
    """Show status, metadata, and recent stdout/stderr of JOBID on HOST.

    \b
    Forms:
      vq status HOST JOBID
      vq status JOBID            (locates the durable queue owner)
      vq status JOBID --json     (machine-readable)

    When no HOST is given, a remote default is not treated as ownership
    evidence: vq reads the configured queues and routes to the one durable
    owner. Unreachable listings do not hide an owner found elsewhere. The
    local default remains authoritative without a fleet fan-out.
    """
    cfg = config.load_config()
    inferred_default_host = False
    if jobid_if_host is None:
        # Single positional -> jobid.  Status always locates from durable
        # queue rows: an unmarked dead/stale default host has no ownership
        # authority merely because it remains configured as the default.
        jobid = host_or_jobid
        default_host = _resolve_host(cfg, None)
        host = _resolve_job_host(
            cfg,
            None,
            jobid,
            locate_when_default_up=True,
        )
        # Only the authoritative local fast path is still a default-host
        # inference.  A remote host returned by the durable locator is an
        # observed owner, even when it happens to equal default_host.  If its
        # row disappears before status reads it, do not mislabel it as the
        # configured default in the race error hint.
        inferred_default_host = (
            is_local_host(default_host) and host == default_host
        )
    else:
        host = host_or_jobid
        jobid = jobid_if_host
    requested_host = host
    scheduler_driver = _scheduler_driver_host(cfg, host)
    if scheduler_driver is not None:
        host = scheduler_driver

    if is_local_host(host):
        try:
            mu = _multi_user_active(cfg)
            if as_json:
                text = show_status_json(
                    host,
                    jobid,
                    tail=None if tail == 0 else tail,
                    multi_user=mu,
                    cfg=cfg,
                )
            else:
                text = show_status(
                    host,
                    jobid,
                    tail=None if tail == 0 else tail,
                    multi_user=mu,
                    cfg=cfg,
                )
        except FileNotFoundError as e:
            raise click.UsageError(
                _note_searched_host(
                    str(e),
                    verb="status",
                    jobid=jobid,
                    searched_host=requested_host,
                    inferred=inferred_default_host,
                )
            ) from None
        except (NotImplementedError, SchedulerError) as e:
            raise click.UsageError(str(e)) from None
        click.echo(text)
    else:
        remote_args = ["status", "localhost", jobid]
        if tail != 50:
            remote_args.extend(["-n", str(tail)])
        if as_json:
            remote_args.append("--json")
        text = _delegate_job_lookup(
            cfg,
            host,
            *remote_args,
            verb="status",
            jobid=jobid,
            searched_host=requested_host,
            inferred=inferred_default_host,
        )
        if as_json:
            text = _json_status_with_requested_handle_host(text, requested_host)
        click.echo(text, nl=False)


@main.command("logs",
           help="Show stdout/stderr for a job without typing the workspace path.")
@click.argument("host_or_jobid")
@click.argument("jobid_if_host", required=False)
@click.option(
    "-n",
    "--tail",
    type=click.IntRange(min=0),
    default=100,
    show_default=True,
    help="Show last N lines per stream (0 = full output). Default "
    "100 is larger than `vq status`'s 50 since operators reaching "
    "for `vq logs` want output, not metadata.",
)
@click.option(
    "-f",
    "--follow",
    is_flag=True,
    default=False,
    help="Stream new output as files grow. Unlike `vq tail -f`, this "
    "is spec-aware: stops automatically once the job hits a terminal "
    "state (COMPLETED / FAILED / CANCELLED / TIMEOUT) and the log "
    "files go idle for two consecutive polls — no Ctrl-C needed for "
    "synchronous shell-script chaining.",
)
@click.option(
    "--stdout",
    "only_stdout",
    is_flag=True,
    default=False,
    help="Show stdout only (no banner, suitable for `vq logs JOBID "
    "--stdout | grep ...`). Mutually exclusive with --stderr.",
)
@click.option(
    "--stderr",
    "only_stderr",
    is_flag=True,
    default=False,
    help="Show stderr only (no banner). Mutually exclusive with --stdout.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Emit a JSON object with jobid, state, the resolved stdout/"
    "stderr paths, and the tailed text per requested stream. "
    "--follow + --json is rejected (no agreed streaming JSON shape).",
)
def logs_cmd(
    host_or_jobid: str,
    jobid_if_host: str | None,
    tail: int,
    follow: bool,
    only_stdout: bool,
    only_stderr: bool,
    as_json: bool,
) -> None:
    """Show stdout / stderr for JOBID without typing the workspace path.

    \b
    Forms:
      vq logs JOBID                            # both streams, last 100 lines each
      vq logs HOST JOBID                       # explicit host
      vq logs JOBID --stdout | grep ...        # stdout only, no banner
      vq logs JOBID --tail 0                   # whole output
      vq logs JOBID -f                         # follow until terminal
      vq logs JOBID --json                     # machine-readable

    Companion to `vq status` (which shows metadata + a small log
    tail) and `vq tail` (which exec's into `tail(1)` on an
    arbitrary file in the workspace). `vq logs` is spec-aware:
    knows about both streams, terminates `-f` automatically on
    terminal-and-idle, and emits structured JSON.

    Like `vq status`, when no HOST is given and `default_host` is
    marked `vq host down`, the lookup skips it and locates the job
    across the other up hosts (Baran's Detour). An unmarked remote
    default must also supply one trusted durable queue listing before
    logs are requested; that reachability probe does not locate another
    owner.
    """
    if only_stdout and only_stderr:
        raise click.UsageError(
            "--stdout and --stderr are mutually exclusive (use neither "
            "to see both, banner-separated)"
        )
    if follow and as_json:
        raise click.UsageError(
            "--follow with --json is not supported (no agreed streaming "
            "JSON shape); use --json for a one-shot snapshot or --follow "
            "for live text output"
        )

    cfg = config.load_config()
    if jobid_if_host is None:
        jobid = host_or_jobid
        host = _resolve_job_host(cfg, None, jobid)
    else:
        host = host_or_jobid
        jobid = jobid_if_host
    requested_host = host
    scheduler_driver = _scheduler_driver_host(cfg, host)
    if scheduler_driver is not None:
        host = scheduler_driver

    stream: str = "both"
    if only_stdout:
        stream = "stdout"
    elif only_stderr:
        stream = "stderr"

    if is_local_host(host):
        try:
            mu = _multi_user_active(cfg)
            if follow:
                for chunk in follow_logs(
                    host,
                    jobid,
                    stream=stream,  # type: ignore[arg-type]
                    multi_user=mu,
                    initial_tail=None if tail == 0 else tail,
                    cfg=cfg,
                ):
                    click.echo(chunk, nl=False)
                return
            if as_json:
                text = show_logs_json(
                    host,
                    jobid,
                    tail=None if tail == 0 else tail,
                    stream=stream,  # type: ignore[arg-type]
                    multi_user=mu,
                    cfg=cfg,
                )
            else:
                text = show_logs(
                    host,
                    jobid,
                    tail=None if tail == 0 else tail,
                    stream=stream,  # type: ignore[arg-type]
                    multi_user=mu,
                    cfg=cfg,
                )
        except FileNotFoundError as e:
            raise click.UsageError(
                _note_searched_host(
                    str(e),
                    verb="logs",
                    jobid=jobid,
                    searched_host=requested_host,
                    inferred=jobid_if_host is None,
                )
            ) from None
        except (NotImplementedError, SchedulerError) as e:
            raise click.UsageError(str(e)) from None
        click.echo(text)
        return

    # Remote: delegate. The follow path consumes the SSH stdout pipe as the
    # remote vq emits bytes; one-shot snapshots retain captured delegation.
    remote_args: list[str] = ["logs", "localhost", jobid]
    if tail != 100:
        remote_args.extend(["-n", str(tail)])
    if follow:
        remote_args.append("-f")
    if only_stdout:
        remote_args.append("--stdout")
    elif only_stderr:
        remote_args.append("--stderr")
    if as_json:
        remote_args.append("--json")
    if follow:
        _stream_delegated_job_lookup(
            cfg,
            host,
            *remote_args,
            verb="logs",
            jobid=jobid,
            searched_host=requested_host,
            inferred=jobid_if_host is None,
        )
        return
    text = _delegate_job_lookup(
        cfg,
        host,
        *remote_args,
        verb="logs",
        jobid=jobid,
        searched_host=requested_host,
        inferred=jobid_if_host is None,
    )
    if as_json:
        text = _json_with_requested_queue_handle_host(text, requested_host)
    click.echo(text, nl=False)


@main.command("output",
           help="Stream the vibe-qc calculation .out file (SCF trace, energies, properties).")
@click.argument("host_or_jobid")
@click.argument("jobid_if_host", required=False)
@click.option(
    "-n",
    "--tail",
    type=click.IntRange(min=0),
    default=100,
    show_default=True,
    help="Show last N lines of the calculation .out file "
    "(0 = full output).",
)
@click.option(
    "-f",
    "--follow",
    is_flag=True,
    default=False,
    help="Stream the .out file as it grows. Stops automatically "
    "when the job reaches a terminal state and the file goes "
    "idle for two consecutive polls — no Ctrl-C needed.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Emit a JSON object with jobid, state, out_path, and the "
    "tailed output text.  --follow + --json is rejected (no agreed "
    "streaming JSON shape).",
)
def output_cmd(
    host_or_jobid: str,
    jobid_if_host: str | None,
    tail: int,
    follow: bool,
    as_json: bool,
) -> None:
    """Stream the vibe-qc calculation output (.out file).

    \b
    Forms:
      vq output JOBID              # last 100 lines
      vq output HOST JOBID         # explicit host
      vq output JOBID -n 50        # last 50 lines
      vq output JOBID -n 0         # whole output so far
      vq output JOBID -f           # follow live until job finishes
      vq output JOBID --json       # machine-readable snapshot

    The .out file is vibe-qc's canonical calculation record — the
    full SCF trace, orbital tables, properties, and (at verbose/debug)
    live C++ diagnostics.  It is written line-buffered so ``-f``
    shows each line as the SCF emits it.

    Unlike ``vq logs`` (which tails process stdout/stderr), ``vq
    output`` tails the structured calculation output that vibe-qc
    writes through its ``OutputChannel`` — the same file you get
    when running a calculation locally.

    On a scheduler target configured with ``node_scratch_dir``, relative
    calculation artifacts stay on the compute node until normal copy-back;
    use ``vq logs -f`` for stdout/stderr that remain live in the shared
    workspace.

    Like ``vq status``, when no HOST is given and ``default_host``
    is marked ``vq host down``, the lookup skips it and locates the
    job across the other up hosts (Baran's Detour).
    """
    if follow and as_json:
        raise click.UsageError(
            "--follow with --json is not supported (no agreed streaming "
            "JSON shape); use --json for a one-shot snapshot or --follow "
            "for live text output"
        )

    cfg = config.load_config()
    if jobid_if_host is None:
        jobid = host_or_jobid
        host = _resolve_job_host(cfg, None, jobid)
    else:
        host = host_or_jobid
        jobid = jobid_if_host
    requested_host = host
    scheduler_driver = _scheduler_driver_host(cfg, host)
    if scheduler_driver is not None:
        host = scheduler_driver

    if is_local_host(host):
        try:
            mu = _multi_user_active(cfg)
            if follow:
                for chunk in follow_output(
                    host,
                    jobid,
                    multi_user=mu,
                    initial_tail=tail if tail > 0 else 20,
                    cfg=cfg,
                ):
                    click.echo(chunk, nl=False)
                return
            if as_json:
                text = show_output_json(
                    host,
                    jobid,
                    tail=None if tail == 0 else tail,
                    multi_user=mu,
                    cfg=cfg,
                )
            else:
                text = show_output(
                    host,
                    jobid,
                    tail=None if tail == 0 else tail,
                    multi_user=mu,
                    cfg=cfg,
                )
        except FileNotFoundError as e:
            raise click.UsageError(
                _note_searched_host(
                    str(e),
                    verb="output",
                    jobid=jobid,
                    searched_host=requested_host,
                    inferred=jobid_if_host is None,
                )
            ) from None
        except (NotImplementedError, SchedulerError) as e:
            raise click.UsageError(str(e)) from None
        click.echo(text)
        return

    # Remote: delegate.
    remote_args: list[str] = ["output", "localhost", jobid]
    if tail != 100:
        remote_args.extend(["-n", str(tail)])
    if follow:
        remote_args.append("-f")
    if as_json:
        remote_args.append("--json")
    if follow:
        _stream_delegated_job_lookup(
            cfg,
            host,
            *remote_args,
            verb="output",
            jobid=jobid,
            searched_host=requested_host,
            inferred=jobid_if_host is None,
        )
        return
    text = _delegate_job_lookup(
        cfg,
        host,
        *remote_args,
        verb="output",
        jobid=jobid,
        searched_host=requested_host,
        inferred=jobid_if_host is None,
    )
    click.echo(text, nl=False)


@main.command(
    "progress",
    help="Show SCF iteration records (energy, gradient, DIIS).",
)
@click.argument("host_or_jobid")
@click.argument("jobid_if_host", required=False)
@click.option(
    "-n",
    "--tail",
    type=click.IntRange(min=0),
    default=50,
    show_default=True,
    help="Show last N iterations (0 = all).",
)
@click.option(
    "-f",
    "--follow",
    is_flag=True,
    default=False,
    help="Stream new iterations as the structured log writes them. "
    "Stops automatically "
    "when the job reaches a terminal state and the log goes idle.",
)
def progress_cmd(
    host_or_jobid: str,
    jobid_if_host: str | None,
    tail: int,
    follow: bool,
) -> None:
    """Show SCF iteration records for a running or completed job.

    \b
    Forms:
      vq progress JOBID              # last 50 iterations
      vq progress HOST JOBID         # explicit host
      vq progress JOBID -n 20        # last 20 iterations
      vq progress JOBID -n 0         # all iterations so far
      vq progress JOBID -f           # follow live until job finishes

    Reads the auto-enabled .scf.jsonl structured log and renders a
    compact table with iteration number, total energy, energy change,
    orbital gradient norm, and DIIS subspace size — the same columns
    as the .out file's SCF trace.

    Requires vibe-qc >= v0.24 (which auto-enables structured logging
    when running under vq). ``--follow`` displays records as the
    calculation writes them; some calculation backends publish their
    trace only after a blocking phase returns. Use ``vq status`` for
    the latest callback-backed calculation-progress snapshot.

    On a scheduler target configured with ``node_scratch_dir``, the
    structured log stays on the compute node until normal copy-back. Use
    ``vq logs -f`` for live stdout/stderr in the shared workspace.
    """
    cfg = config.load_config()
    if jobid_if_host is None:
        jobid = host_or_jobid
        host = _resolve_job_host(cfg, None, jobid)
    else:
        host = host_or_jobid
        jobid = jobid_if_host
    requested_host = host
    scheduler_driver = _scheduler_driver_host(cfg, host)
    if scheduler_driver is not None:
        host = scheduler_driver

    if is_local_host(host):
        try:
            mu = _multi_user_active(cfg)
            if follow:
                for chunk in follow_progress(
                    host, jobid, multi_user=mu,
                    initial_tail=tail if tail > 0 else 20,
                    cfg=cfg,
                ):
                    click.echo(chunk, nl=False)
                return
            text = show_progress(
                host, jobid,
                tail=None if tail == 0 else tail,
                multi_user=mu,
                cfg=cfg,
            )
        except FileNotFoundError as e:
            raise click.UsageError(
                _note_searched_host(
                    str(e), verb="progress", jobid=jobid,
                    searched_host=requested_host,
                    inferred=jobid_if_host is None,
                )
            ) from None
        except (NotImplementedError, SchedulerError) as e:
            raise click.UsageError(str(e)) from None
        click.echo(text)
        return

    remote_args: list[str] = ["progress", "localhost", jobid]
    if tail != 50:
        remote_args.extend(["-n", str(tail)])
    if follow:
        remote_args.append("-f")
        _stream_delegated_job_lookup(
            cfg,
            host,
            *remote_args,
            verb="progress",
            jobid=jobid,
            searched_host=requested_host,
            inferred=jobid_if_host is None,
        )
        return
    text = _delegate_job_lookup(
        cfg, host, *remote_args,
        verb="progress", jobid=jobid,
        searched_host=requested_host,
        inferred=jobid_if_host is None,
    )
    click.echo(text, nl=False)


@main.command("events")
@click.argument("host_or_jobid")
@click.argument("jobid_if_host", required=False)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Emit the raw event records as a JSON object.",
)
def events_cmd(
    host_or_jobid: str,
    jobid_if_host: str | None,
    as_json: bool,
) -> None:
    """Show a job's lifecycle event timeline.

    \b
    Forms:
      vq events JOBID                # timeline, one line per event
      vq events HOST JOBID           # explicit host
      vq events JOBID --json         # raw records

    The daemon records a structured event per lifecycle transition —
    submitted, dispatched, every state change, kill requests, watchdog
    kills, scheduler cancellations. This is the answer to "what actually
    happened to my job, and when?"; `vq status` shows the current state,
    `vq logs` shows the job's own output, and this shows the daemon's
    view of the job's history.

    Routed like `vq logs`: a scheduler job's events live on the driver,
    where its workspace is, so an explicit HOST or `default_host`
    resolves the same way.
    """
    from vq.logs import show_events, show_events_json

    cfg = config.load_config()
    if jobid_if_host is None:
        jobid = host_or_jobid
        host = _resolve_job_host(cfg, None, jobid)
    else:
        host = host_or_jobid
        jobid = jobid_if_host
    scheduler_driver = _scheduler_driver_host(cfg, host)
    if scheduler_driver is not None:
        host = scheduler_driver

    if is_local_host(host):
        try:
            mu = _multi_user_active(cfg)
            if as_json:
                text = show_events_json(host, jobid, multi_user=mu)
            else:
                text = show_events(host, jobid, multi_user=mu)
        except FileNotFoundError as e:
            raise click.UsageError(str(e)) from None
        except NotImplementedError as e:
            raise click.UsageError(str(e)) from None
        click.echo(text)
        return

    remote_args: list[str] = ["events", "localhost", jobid]
    if as_json:
        remote_args.append("--json")
    click.echo(_delegate_to_remote(host, cfg, *remote_args), nl=False)


@main.command("tail")
@click.argument("host_or_jobid")
@click.argument("jobid_if_host", required=False)
@click.option(
    "-f",
    "--follow",
    is_flag=True,
    default=False,
    help="Stream new output as it's written (like `tail -f`). Press "
    "Ctrl-C to stop. The file MUST exist when the verb starts — "
    "if a job hasn't written its log yet, re-run after a moment.",
)
@click.option(
    "-n",
    "--lines",
    type=click.IntRange(min=0),
    default=50,
    show_default=True,
    help="Number of lines to print before following (or just print, without -f). 0 = whole file.",
)
@click.option(
    "--name",
    "filename",
    default="stdout.log",
    show_default=True,
    metavar="FILENAME",
    help="File in the job's workspace to tail. Defaults to stdout.log "
    "(what the daemon captures from the process's stdout). For "
    "vibe-qc jobs that configure a Python logger to write to a "
    "custom file (e.g. inside the script: "
    "`logging.basicConfig(filename='vibeqc.log')`), pass that "
    "filename here: `vq tail JOBID --name vibeqc.log -f`. Common "
    "alternatives: 'stderr.log' (daemon stderr capture), the "
    "engine's own output file like 'mgo.out' for CRYSTAL or "
    "'h2.out' for ORCA / Psi4.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help=(
        "Emit a machine-readable one-shot tail payload. "
        "Mutually exclusive with -f."
    ),
)
def tail_cmd(
    host_or_jobid: str,
    jobid_if_host: str | None,
    follow: bool,
    lines: int,
    filename: str,
    as_json: bool,
) -> None:
    """Print or live-tail a file in JOBID's workspace.

    \b
    Forms:
      vq tail JOBID                        # last 50 lines of stdout.log
      vq tail JOBID -f                     # follow stdout.log live
      vq tail JOBID -n 200                 # last 200 lines
      vq tail JOBID --name vibeqc.log -f   # follow vibe-qc's logger file
      vq tail JOBID --name calc.out --json # machine-readable one-shot tail
      vq tail HOST JOBID                   # explicit host (vs default_host)

    Why a separate verb (not just `vq status -n N`):
      * `vq status` is a one-shot snapshot of metadata + recent log
        lines. It doesn't follow.
      * Real chat workflow is "watch the SCF converge in real time" —
        that needs `-f`. This verb is the equivalent of
        `ssh HOST tail -f /workspace/.../stdout.log`, just without you
        needing to know the workspace path.

    Implementation: this verb ``exec``s `tail` directly for ordinary local
    and remote daemon hosts. Live scheduler-host follow mode polls through
    the scheduler driver instead, because the file lives on the cluster
    workspace while the driver keeps the local spec.

    File-must-exist caveat: `tail -f` errors out if the file isn't there
    when the verb starts. If your job just dispatched and the logger
    hasn't flushed yet, `vq status JOBID` confirms it's running, then
    re-run `vq tail` a moment later.
    """
    cfg = config.load_config()
    if as_json and follow:
        raise click.UsageError(
            "`vq tail --json` cannot be combined with --follow; poll without -f "
            "or use `vq logs --json` for stdout/stderr tails"
        )
    if jobid_if_host is None:
        # Single positional -> jobid; route around a down default_host
        # (v0.11.0 Baran's Detour: locate the job on the up hosts).
        jobid = host_or_jobid
        host = _resolve_job_host(cfg, None, jobid)
    else:
        host = host_or_jobid
        jobid = jobid_if_host

    # Basic input hygiene: reject obvious path-traversal / absolute
    # paths in the filename (the workspace lookup is supposed to scope
    # to the job's dir only).
    if filename.startswith("/") or ".." in filename.split("/"):
        raise click.UsageError(
            f"--name {filename!r} must be a relative path inside the "
            f"workspace; absolute paths and '..' components rejected"
        )

    scheduler_driver = _scheduler_driver_host(cfg, host)
    if scheduler_driver is not None:
        if is_local_host(scheduler_driver):
            _tail_scheduler_job(
                host,
                cfg,
                jobid,
                filename,
                follow,
                lines,
                as_json,
                multi_user=_multi_user_active(cfg),
            )
        else:
            remote_args = [
                "tail",
                host,
                jobid,
                "--name",
                filename,
                "-n",
                str(lines),
            ]
            if follow:
                remote_args.append("-f")
            if as_json:
                remote_args.append("--json")
            text = _delegate_to_remote(scheduler_driver, cfg, *remote_args)
            if as_json:
                text = _json_with_requested_queue_handle_host(text, host)
            click.echo(text, nl=False)
        return

    if is_local_host(host):
        multi_user = _multi_user_active(cfg)
        if as_json:
            click.echo(
                _tail_local_json(
                    jobid,
                    filename,
                    lines,
                    requested_host=host,
                    multi_user=multi_user,
                )
            )
        else:
            _exec_tail_local(
                jobid,
                filename,
                follow,
                lines,
                multi_user=multi_user,
            )
    else:
        _exec_tail_remote(host, cfg, jobid, filename, follow, lines, as_json)


def _build_tail_argv(file_path: str, follow: bool, lines: int) -> list[str]:
    """``tail`` argv that works on both GNU and BSD `tail`."""
    line_arg = "+1" if lines == 0 else str(lines)
    argv = ["tail", f"-n{line_arg}"]
    if follow:
        # -f exists on both GNU and BSD tail. -F (retry on rotation)
        # is GNU-only; we don't depend on it.
        argv.append("-f")
    argv.append(file_path)
    return argv


def _exec_tail_local(
    jobid: str,
    filename: str,
    follow: bool,
    lines: int,
    *,
    multi_user: bool = False,
    spec: JobSpec | None = None,
) -> None:
    """Local-host tail: open the workspace file directly. ``execvp``
    replaces the current Python process with `tail` so SIGINT, output
    streaming, and exit-code propagation all go straight through."""
    target = _resolve_tail_target(
        jobid,
        filename,
        multi_user=multi_user,
        spec=spec,
    )
    argv = _build_tail_argv(str(target), follow, lines)
    os.execvp("tail", argv)


def _resolve_tail_target(
    jobid: str,
    filename: str,
    *,
    multi_user: bool = False,
    spec: JobSpec | None = None,
) -> Path:
    if spec is None and multi_user:
        _, spec = resolve_authorized_spec(jobid, multi_user=True)
    workspace = Path(spec.cwd) if spec is not None else paths.workspace_dir(jobid)
    if not workspace.is_dir():
        raise click.UsageError(
            f"no workspace for jobid {jobid!r} on this host "
            f"(expected {workspace}); the jobid may not exist, or "
            f"`vq cleanup --archive` may have moved it."
        )
    target = workspace / filename
    if not target.exists():
        # Hint about common alternatives so the user doesn't have to
        # guess what they wanted.
        siblings = sorted(p.name for p in workspace.iterdir() if p.is_file())
        hint = (
            f"  files in workspace: {', '.join(siblings[:10])}"
            if siblings
            else "  (workspace is empty — job may not have started)"
        )
        raise click.UsageError(
            f"file {filename!r} not found in workspace for {jobid}.\n{hint}"
        )
    return target


def _tail_local_json(
    jobid: str,
    filename: str,
    lines: int,
    *,
    requested_host: str,
    spec: JobSpec | None = None,
    multi_user: bool = False,
) -> str:
    if spec is None:
        if multi_user:
            _, spec = resolve_authorized_spec(jobid, multi_user=True)
        else:
            with contextlib.suppress(FileNotFoundError, ValueError, OSError):
                spec = JobSpec.read(paths.spec_path(jobid))
    target = _resolve_tail_target(
        jobid,
        filename,
        multi_user=multi_user,
        spec=spec,
    )
    return _tail_json_payload(
        jobid=jobid,
        requested_host=requested_host,
        filename=filename,
        lines=lines,
        text=tail_file(target, None if lines == 0 else lines),
        path=str(target),
        spec=spec,
    )


def _tail_json_payload(
    *,
    jobid: str,
    requested_host: str,
    filename: str,
    lines: int,
    text: str,
    path: str,
    spec: JobSpec | None,
    remote_workspace: str | None = None,
) -> str:
    queue_handle = (
        queue_handle_for_spec(spec, requested_host)
        if spec is not None
        else queue_handle_without_spec(jobid, requested_host)
    )
    payload: dict[str, object] = {
        "jobid": jobid,
        "host": (
            spec.scheduler_target
            if spec and spec.scheduler_target
            else requested_host
        ),
        "state": spec.state.value if spec else None,
        "filename": filename,
        "tail": None if lines == 0 else lines,
        "path": path,
        "text": text,
        "queue_handle": queue_handle,
    }
    if spec and spec.scheduler_target:
        payload["scheduler_target"] = spec.scheduler_target
        payload["scheduler_job_id"] = spec.scheduler_job_id
    if remote_workspace is not None:
        payload["remote_workspace"] = remote_workspace
        payload["live_scheduler_workspace"] = True
    return json.dumps(payload, indent=2, sort_keys=True, default=str)


def _tail_scheduler_job(
    scheduler_host: str,
    cfg: config.Config,
    jobid: str,
    filename: str,
    follow: bool,
    lines: int,
    as_json: bool,
    *,
    multi_user: bool,
) -> None:
    """Tail a scheduler-backed job through the local driver daemon.

    Live scheduler jobs have their newest files on the cluster workspace, not
    necessarily in the driver's staged-back copy. Terminal jobs have already
    been fetched home by daemon reconciliation, so the ordinary local tail path
    remains correct for those.
    """
    try:
        if multi_user:
            spec_path, spec = resolve_authorized_spec(jobid, multi_user=True)
        else:
            spec_path = paths.spec_path(jobid)
            spec = JobSpec.read(spec_path)
    except ownership.OwnershipError:
        raise
    except (FileNotFoundError, ValueError, OSError) as exc:
        raise click.UsageError(f"no such scheduler job on driver: {jobid}") from exc

    if spec.scheduler_target != scheduler_host:
        actual = spec.scheduler_target or "local"
        raise click.UsageError(
            f"job {jobid} is not owned by scheduler host {scheduler_host!r} "
            f"(scheduler_target={actual!r})"
        )

    if spec.is_terminal or spec.scheduler_job_id is None:
        if as_json:
            click.echo(
                _tail_local_json(
                    jobid,
                    filename,
                    lines,
                    requested_host=scheduler_host,
                    spec=spec,
                    multi_user=multi_user,
                )
            )
        else:
            _exec_tail_local(
                jobid,
                filename,
                follow,
                lines,
                multi_user=multi_user,
                spec=spec,
            )
        return

    try:
        host_cfg = cfg.host(scheduler_host)
        dispatcher = scheduler_dispatcher_for(host_cfg)
        handle = scheduler_handle_for_spec(
            dispatcher,
            spec,
            job_id=spec.scheduler_job_id,
        )
        if follow:
            _follow_scheduler_file(
                spec_path,
                dispatcher,
                handle,
                filename=filename,
                lines=lines,
            )
            return
        text = dispatcher.tail_file(
            handle,
            filename=filename,
            lines=None if lines == 0 else lines,
        )
    except (config.ConfigError, SchedulerError) as exc:
        raise click.UsageError(str(exc)) from None
    if as_json:
        click.echo(
            _tail_json_payload(
                jobid=spec.id,
                requested_host=scheduler_host,
                filename=filename,
                lines=lines,
                text=text.rstrip("\n") if text else "(no output)",
                path=f"{handle.remote_workspace}/{filename}",
                spec=spec,
                remote_workspace=handle.remote_workspace,
            )
        )
        return
    click.echo(text.rstrip("\n") if text else "(no output)")


def _follow_scheduler_file(
    spec_path: Path,
    dispatcher: SchedulerDispatcher,
    handle: SchedulerHandle,
    *,
    filename: str,
    lines: int,
    poll_interval: float = 1.0,
    idle_ticks_after_terminal: int = 2,
    sleep_fn: Callable[[float], None] | None = None,
) -> None:
    """Follow an arbitrary live scheduler workspace file via incremental tails."""
    sleep = sleep_fn or time.sleep
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    snapshot = dispatcher.tail_file_since(
        handle,
        filename=filename,
        byte_offset=0,
    )
    if snapshot is None:
        previous_size = 0
        initial_text = ""
        seen_file = False
    else:
        previous_size = snapshot.end_offset
        initial_text = decoder.decode(snapshot.data, final=False)
        seen_file = True
    visible = _tail_text(initial_text, lines)
    if visible:
        click.echo(visible, nl=False)
    else:
        click.echo("(no output)")
    idle_after_terminal = 0

    while True:
        sleep(poll_interval)
        try:
            spec = JobSpec.read(spec_path)
        except FileNotFoundError:
            return

        snapshot = dispatcher.tail_file_since(
            handle,
            filename=filename,
            byte_offset=previous_size,
        )
        if snapshot is None:
            chunk = ""
        else:
            if snapshot.reset:
                decoder = codecs.getincrementaldecoder("utf-8")(
                    errors="replace"
                )
            previous_size = snapshot.end_offset
            chunk = decoder.decode(snapshot.data, final=False)
            if not seen_file:
                chunk = _tail_text(chunk, lines)
                seen_file = True

        if chunk:
            click.echo(chunk, nl=False)
            idle_after_terminal = 0
            continue

        if spec.is_terminal:
            idle_after_terminal += 1
            if idle_after_terminal >= idle_ticks_after_terminal:
                final_text = decoder.decode(b"", final=True)
                if final_text:
                    click.echo(final_text, nl=False)
                return


def _tail_text(text: str, lines: int) -> str:
    if lines == 0:
        return text
    return "".join(text.splitlines(keepends=True)[-lines:])


def _exec_tail_remote(
    host: str,
    cfg: config.Config,
    jobid: str,
    filename: str,
    follow: bool,
    lines: int,
    as_json: bool = False,
) -> None:
    """Remote-host tail: ssh to the host and run ``vq tail localhost
    JOBID --name F``; the remote vq then `_exec_tail_local`s into
    `tail` and the output streams back through ssh.

    We delegate through `vq tail localhost` (rather than ssh-ing
    `tail` directly) so the remote workspace-path resolution uses the
    daemon's actual `$VQ_STATE_DIR`, not a guess from the laptop side.
    """
    try:
        host_cfg = cfg.host(host)
    except config.ConfigError as e:
        raise click.UsageError(str(e)) from None
    remote_argv = [
        host_cfg.remote_vq,
        "tail",
        "localhost",
        jobid,
        "--name",
        filename,
        "-n",
        str(lines),
    ]
    if follow:
        remote_argv.append("-f")
    if as_json:
        remote_argv.append("--json")
        text = _delegate_to_remote(host, cfg, *remote_argv[1:])
        click.echo(_json_with_requested_queue_handle_host(text, host), nl=False)
        return
    # v0.5.32: shlex-join the remote argv into one shell-safe string
    # before handing to ssh. Same reasoning as transport.run_remote_vq
    # — ssh always pipes the post-host string through a shell on the
    # remote, which would re-parse unquoted metacharacters. `filename`
    # in particular is upstream-validated against absolute paths and
    # `..`, but a name like `my report.log` (with a space) would still
    # have broken without quoting.
    # v0.12.1: reuse the same SSH option prefix as the bounded transport
    # helpers so direct-host tails fail fast on dead routes/auth prompts too.
    # `ssh -t` would allocate a pty (useful for interactive Ctrl-C
    # handling), but it also mangles output line-buffering. Plain ssh
    # is fine; Ctrl-C on the laptop sends SIGINT to the local ssh
    # process which terminates the remote tail cleanly.
    cmd = [*transport._ssh_base(host_cfg), shlex.join(remote_argv)]
    os.execvp("ssh", cmd)


@main.command(context_settings={"ignore_unknown_options": True})
@click.option(
    "-d",
    "--dir",
    "directory",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    help="Submit a directory's contents as the workspace.",
)
@click.option(
    "-c",
    "--compressed",
    "archive",
    type=click.Path(exists=True, dir_okay=False),
    help="Submit a tarball; extracted into the workspace on the daemon host.",
)
@click.option(
    "--cpus",
    default=1,
    show_default=True,
    type=click.IntRange(min=1),
    help="CPU slots this job claims against the daemon's max_cpus budget.",
)
@click.option(
    "--scheduler-tasks",
    "--ntasks",
    "scheduler_tasks",
    default=None,
    type=click.IntRange(min=1),
    metavar="N",
    help="Scheduler task/rank count for daemonless scheduler hosts. On SLURM "
    "this renders as --ntasks=N, while --cpus remains --cpus-per-task and "
    "the vq thread/accounting cap. Use for MPI jobs such as ORCA %pal nprocs.",
)
@click.option(
    "--mem-mb",
    "mem_mb",
    default=None,
    type=click.IntRange(min=1),
    metavar="N",
    help="Memory budget claimed in MB. Bookkeeping in v0.3 (daemon won't "
    "dispatch if running RSS budgets + N > --max-mem-mb); enforcement "
    "via cgroups v2 in v0.4. Defaults to host policy when omitted. "
    "v0.11.0: also the RAM-fit target for `vq submit auto` — picks a host "
    "with at least N MB free (overriding the automatic vibe-qc estimate).",
)
@click.option(
    "--wall-time-seconds",
    "wall_time_seconds",
    default=None,
    type=click.IntRange(min=1),
    metavar="SECONDS",
    help="Hard wall-clock limit. The watchdog SIGTERMs at this many seconds, "
    "then SIGKILLs after the grace period. Job state becomes "
    "TIME_EXCEEDED. Required from v0.4; optional in v0.3. "
    "v0.7.16: also expressible via --time-limit HH:MM:SS (SLURM "
    "ergonomic). Mutually exclusive with --time-limit.",
)
@click.option(
    "--time-limit",
    "--time",
    "time_limit",
    default=None,
    type=str,
    metavar="HH:MM:SS",
    help="v0.7.16 *Codd's Tuple*: SLURM-style wall-clock limit. "
    "Accepts HH:MM:SS (e.g. 01:30:00 = 5400 s), MM:SS "
    "(e.g. 90:00 = 5400 s), or plain integer seconds "
    "(e.g. 5400). Sets the same spec field as "
    "--wall-time-seconds; the two flags are mutually "
    "exclusive — use whichever your fingers prefer.",
)
@click.option(
    "--priority",
    "priority",
    default=0,
    show_default=True,
    type=int,
    metavar="N",
    help="v0.5.29: dispatch priority. The daemon orders the PENDING queue "
    "by (priority desc, submitted_at asc): higher priority dispatches "
    "first; within one priority level it stays FIFO. Negative values "
    "mean 'run after the default-priority work'. Use a positive "
    "priority for a critical-path job that must jump ahead of jobs "
    "already queued (it does NOT preempt RUNNING jobs — only reorders "
    "what dispatches next). Default 0 = unchanged FIFO behaviour.",
)
@click.option(
    "--auto-resume",
    "auto_resume",
    is_flag=True,
    default=False,
    help="v0.5.30: opt-in auto-resume after a host reboot. If the queue "
    "host reboots (power loss, crash, scheduled) while this job is "
    "RUNNING, the kernel kills it — normally it lands in "
    "ABORTED_BY_QUEUE and you resubmit by hand. With --auto-resume "
    "the daemon, on its next startup, emits a sibling resubmit: "
    "fresh jobid, SAME command + SAME workspace, parent_jobid "
    "linking the chain. YOUR job must be able to pick up from "
    "partial state on disk (CRYSTAL GUESSP=fort.20, PySCF chkfile, "
    "ORCA .gbw) — vq re-runs the same command, it does not "
    "checkpoint. Off by default: silent auto-resume after a thermal "
    "trip is the wrong thing unless you asked for it.",
)
@click.option(
    "--retry",
    "retry",
    default=0,
    show_default=True,
    type=click.IntRange(min=0),
    metavar="N",
    help="v0.5.31: retry-on-failure budget. If the job's command exits "
    "non-zero, the daemon re-enqueues it (back to PENDING) up to N "
    "times, with exponential backoff (10s, 20s, 40s, ... capped at "
    "10 min) between attempts. Only the plain non-zero-exit FAILED "
    "case is retried — watchdog kills (OOM_KILLED / STARVED / "
    "TIME_EXCEEDED) and `vq kill` are NOT retried (a job the "
    "watchdog or you killed shouldn't silently come back). Retry is "
    "for TRANSIENT failures (a flaky import, a filesystem hiccup, "
    "resource contention); a deterministic failure — bad input, SCF "
    "non-convergence — will just burn all N attempts. Same workspace "
    "is reused across attempts. Default 0 = no retry.",
)
@click.option(
    "--job-name",
    "job_name",
    default=None,
    type=str,
    metavar="NAME",
    help="v0.5.34: optional human-readable label for the job. Charset is "
    "strict on disk (alnum + `-` + `_` + `.`, max 50 chars, filesystem-safe); "
    "the CLI sanitizes friendlier names and warns when it changes them. "
    "When set, the name appears in `vq queue` (new NAME column) and "
    "`vq status`, AND prefixes the fetch destination dir (lands at "
    "`<output_dir>/<name>-<jobid>/` instead of `<output_dir>/<jobid>/`) "
    "AND the archive filename (`<archive_dir>/<name>-<jobid>.tar.bz2` "
    "instead of `<archive_dir>/<jobid>.tar.bz2`). Decorative only: "
    "the canonical addressing key is still the 12-hex jobid — `vq "
    "status NAME` does NOT resolve the name. Two jobs may share a "
    "name; the `-<jobid>` suffix keeps their paths unique.",
)
@click.option(
    "--program",
    "program_name",
    default=None,
    type=str,
    metavar="NAME",
    help="Attach a [programs.NAME] registry identity to the job. The daemon "
    "exports it as VQ_PROGRAM for local and scheduler jobs; future scheduler "
    "templates can use it for per-program setup. Does not rewrite the command.",
)
@click.option(
    "--expected-sha",
    "expected_sha",
    default=None,
    type=str,
    metavar="SHA",
    help="Require the submitted --program venv checkout to be at this git SHA "
    "before queueing, then snapshot the host-validated canonical SHA so "
    "dispatch fails if it drifts before the job runs. Accepts hex prefixes "
    f"of at least {EXPECTED_SHA_MIN_PREFIX_LEN} characters. Works for "
    "single-file, --dir, and --compressed payloads; forwarded to remote and "
    "scheduler-driver submits.",
)
@click.option(
    "--idempotency-key",
    "idempotency_key",
    default=None,
    type=str,
    metavar="KEY",
    help="Make a single logical submit replay-safe at the queue authority. "
    "The same owner, key, and canonical intent return the original job ID; "
    "reusing a key for different input or options is a hard conflict. "
    "Initially unsupported with --array or --chain.",
)
@click.option(
    "--qvf-force",
    "qvf_force",
    is_flag=True,
    default=False,
    help="Allow a settled single-file QVF container to run another sequence. "
    "Only valid for a .qvf input; the default refuses settled archives.",
)
@click.option(
    "--python",
    "python_path",
    default=None,
    type=str,
    metavar="PATH",
    help="Python interpreter for single-file submit (default: vq's own "
    "interpreter locally; host_cfg.remote_python remotely). For remote "
    "hosts the path is interpreted on the *remote* machine. "
    "Rejected with --dir/--compressed -- put the interpreter in the "
    "explicit command instead. Mutually exclusive with --branch.",
)
@click.option(
    "--branch",
    "branch_name",
    default=None,
    type=str,
    metavar="NAME",
    help="Route to a named Python interpreter via the host's "
    "[hosts.X.branches] table (e.g. --branch release vs --branch "
    "main when the host has both a release-tagged and a dev clone "
    "of vibe-qc, each in its own venv). Aliases are resolved "
    "through [hosts.X.branch_aliases]. Single-file submit only; "
    "mutually exclusive with --python. v0.5.47: the branch name "
    "is also stamped on the JobSpec as ``spec.branch`` so "
    "``vq admin update <env>`` can do surgical pause scoping "
    "via ``provides_branches``.",
)
@click.option(
    "--branch-name",
    "branch_name_passthrough",
    default=None,
    type=str,
    hidden=True,
    metavar="NAME",
    help="v0.5.47: hidden, used only by `submit_remote` when "
    "forwarding a laptop-side --branch to the remote vq. Sets "
    "spec.branch *without* triggering python resolution (the "
    "python interpreter has already been resolved on the laptop "
    "side and passed via --python). Operators should use "
    "--branch instead.",
)
@click.option(
    "--refresh",
    "refresh_before",
    default=None,
    type=str,
    metavar="ENV",
    help="v0.11.0: rebuild a named venv-env before this job runs. "
    'ENV is a [programs.X] entry of kind="venv" (the same name '
    "`vq admin update ENV` takes, e.g. `vibeqc-dev`). When this "
    "job is next to dispatch, the daemon drains the host (lets "
    "RUNNING jobs finish, holds new dispatch), runs `git pull` + "
    "the env's update_script (the `vq admin update ENV` work), "
    "then dispatches this job against the freshly-rebuilt venv. "
    "If the rebuild fails, the job lands in FAILED (with a "
    "failure_reason naming the env) rather than running against a "
    "broken env. Use when you've just pushed to main and want the "
    "fleet to rebuild before running your job — the refresh is "
    "per-job and explicitly opted into, so you own the choice. "
    "Single-file submit only; does not apply to --array / --chain "
    "in v1.",
)
@click.option(
    "--pool",
    "pool_opt",
    default=None,
    type=str,
    metavar="POOL",
    help="v0.11.0: with `vq submit auto`, restrict memory-aware placement "
    "to the hosts in a `[pools.POOL]` group (e.g. a 'compute' pool that "
    "excludes gaming / daily-driver boxes). Without --pool, auto uses "
    "`default_pool` if configured, else every host. Only valid with the "
    "`auto` host.",
)
@click.option(
    "--tag",
    "tags",
    multiple=True,
    type=str,
    metavar="TAG",
    help="v0.6.6: attach a free-form label to the job (repeatable: "
    "`--tag experiment-12 --tag basisset-dev` gives two tags). "
    "Charset matches --job-name (alnum + `-` `_` `.`, max 50 "
    "chars; same filesystem-safe rule). Tags are stored as a "
    "deduped + sorted set on the JobSpec; shown in `vq status`. "
    "Filter the queue listing via `vq queue --tag X` (AND-"
    "semantics: rows shown iff they have every requested tag). "
    "Pure operator metadata — does not affect dispatch order, "
    "scheduling, or resource accounting.",
)
@click.option(
    "--at",
    "at_iso",
    default=None,
    type=str,
    metavar="ISO8601",
    help="v0.6.12: schedule the job to dispatch no earlier than the "
    "given timestamp. The daemon's dispatch loop already gates "
    "PENDING specs on `spec.not_before` (v0.5.31's retry-backoff "
    "uses the same field); --at sets that field at submit time. "
    "Format: ISO 8601 with explicit timezone — `2026-05-20T22:00:00Z` "
    "(UTC) or `2026-05-20T17:00:00-05:00`. NAIVE timestamps (no "
    "tz suffix) are REJECTED at the CLI boundary to dodge the "
    "laptop-vs-server timezone-confusion footgun. Past timestamps "
    "are accepted (the dispatch loop treats them as ready-now). "
    "Composable with --retry / --priority — at-or-after AND the "
    "priority ordering both apply.",
)
@click.option(
    "--clean-tmp",
    "clean_tmp",
    is_flag=True,
    default=False,
    help="v0.6.54: delete the per-job workdir as soon as the job hits "
    "a terminal state. By default the workdir lingers (so the chat "
    "/ operator can read results back) until the daemon's auto-"
    "cleanup sweep ages it out (configurable, recommended 14d). "
    "Pass this when the job's result is fully captured in stdout / "
    "stderr / events and the workdir bytes have no operator value "
    "(typical for short basis-opt convergence runs that emit the "
    "final basis as a stdout JSON line). The workdir path is "
    "available to the job via the `$VQ_WORKDIR` env var the "
    "daemon injects.",
)
@click.option(
    "--array",
    "array",
    type=click.IntRange(min=1),
    default=1,
    show_default=False,
    metavar="N",
    help="v0.6.52: spawn N near-identical jobs sharing a group id "
    "(SLURM-array analogue). Each element gets a unique jobid + "
    "workspace; the daemon injects VQ_ARRAY_INDEX (0..N-1) / "
    "VQ_ARRAY_TOTAL (N) / VQ_ARRAY_GROUP_ID environment "
    "variables so the script can branch on its index. All N "
    "jobids are printed to stdout, one per line. No gang "
    "scheduling — each element dispatches independently subject "
    "to budgets / quotas. N=1 is silently treated as a regular "
    "submit (single jobid, no array fields set).",
)
@click.option(
    "--rerun-until",
    "rerun_until_file_exists",
    type=str,
    default=None,
    metavar="PATH",
    help="v0.8.8 *Turing's Halt*: convergence-flag auto-resubmit. "
    "On every COMPLETED terminal transition, the daemon checks "
    "PATH; if absent, spawns a fresh clone of this job (capped "
    "at --rerun-max). $VQ_WORKDIR is substituted at check time "
    "so scripts can write the flag next to their other output: "
    "`vq submit --rerun-until '$VQ_WORKDIR/CONVERGED' dft_u.py`. "
    "Use case: DFT+U self-consistency (script writes CONVERGED "
    "when |U_new - U_old| < tol); NEB CI (NEB_CONVERGED when "
    "force-tol met). FAILED jobs DON'T trigger reruns — failure "
    "is the wrong-signal, not 'try again' (operators wanting "
    "failure retries use --retry). The respawned spec gets "
    "rerun_count++, depends_on=[this jobid], and a fresh "
    "workspace copy; VQ_RERUN_COUNT is injected so the script "
    "can see its iteration number.",
)
@click.option(
    "--rerun-max",
    "rerun_max",
    type=click.IntRange(min=0),
    default=10,
    show_default=True,
    metavar="N",
    help="v0.8.8: cap the rerun chain spawned by --rerun-until. "
    "Default 10 is enough for typical DFT+U self-consistency "
    "loops. N=0 disables reruns (the flag must already be "
    "satisfied on the first run). When the cap is hit the "
    "daemon stops respawning and logs a warning; the operator "
    "decides whether to bump the cap and resubmit.",
)
@click.option(
    "--chain",
    "chain",
    type=click.IntRange(min=1),
    default=1,
    show_default=False,
    metavar="N",
    help="v0.8.7 *Hoare's Triple*: spawn N near-identical jobs in "
    "strict sequence. Element k starts only after element k-1 "
    "has succeeded (`depends_on`-linked). Each element gets a "
    "unique jobid + workspace; the daemon injects "
    "VQ_CHAIN_INDEX (0..N-1) / VQ_CHAIN_TOTAL (N) / "
    "VQ_CHAIN_GROUP_ID environment variables so the script can "
    "branch on its iteration. Use case: NEB image-by-image "
    "(image k initialises from image k-1's geometry); DFT+U "
    "self-consistency (iterate U until converged). Distinct "
    "from --array (siblings, independent + parallel); mutually "
    "exclusive with --array. Cascade-fail: if chain[k] fails, "
    "chain[k+1..] are marked FAILED without dispatching "
    "(inherits depends_on semantic). All N jobids printed to "
    "stdout, one per line. N=1 → regular submit.",
)
@click.option(
    "--depends-on",
    "depends_on",
    multiple=True,
    type=str,
    metavar="JOBID",
    help="v0.6.51: gate dispatch on a predecessor's success. The "
    "daemon holds this job PENDING until JOBID reaches COMPLETED; "
    "if JOBID lands in any failure terminal state (FAILED / "
    "KILLED / OOM_KILLED / TIME_EXCEEDED / STARVED / TIMEOUT / "
    "ABORTED_BY_QUEUE), this job is marked FAILED with reason "
    "`predecessor JOBID failed`. SLURM `afterok` semantics. "
    "Repeatable: `--depends-on JOB1 --depends-on JOB2` waits for "
    "BOTH (AND). Predecessor must exist in this user's queue at "
    "submit time (typo / cross-user dep → fast error). Compose "
    "with --wait for synchronous A-then-B chains: "
    "`B=$(vq submit --depends-on $A --wait B.py)`.",
)
@click.option(
    "--depends-on-any",
    "depends_on_any",
    multiple=True,
    type=str,
    metavar="JOBID",
    help="v0.7.8 *Knuth's Schedule*: gate dispatch on a "
    "predecessor reaching any terminal state. SLURM `afterany` "
    "semantics — complements --depends-on (`afterok`). The daemon "
    "holds this job PENDING until JOBID reaches a terminal state "
    "(COMPLETED / FAILED / KILLED / OOM_KILLED / TIME_EXCEEDED / "
    "STARVED / TIMEOUT / ABORTED_BY_QUEUE / INTERRUPTED) and then "
    "dispatches regardless of the predecessor's outcome. Critically, "
    "a predecessor failure does NOT cascade-fail this job (the whole "
    "point — useful for cleanup / post-processing jobs that should "
    "run whether the upstream succeeded or not). Repeatable; combines "
    "additively with --depends-on (`--depends-on A --depends-on-any "
    "B` waits for A=COMPLETED AND B=TERMINAL).",
)
@click.option(
    "--wait-submitted/--enqueue-only",
    default=False,
    help="Wait for durable scheduler acceptance, without waiting for the calculation. "
    "Staging failure exits nonzero; timeout exits 124 and retains the queued job. "
    "Only scheduler targets support --wait-submitted. --enqueue-only returns "
    "immediately after queue acceptance (the default).",
)
@click.option(
    "--submission-timeout",
    type=click.FloatRange(min=0, min_open=True),
    default=60.0,
    show_default=True,
    metavar="SECONDS",
    help="One scheduler-acceptance wait budget for all submitted array/chain jobs. "
    "Timeout never cancels or resubmits jobs.",
)
@click.option(
    "--wait",
    "wait_for_done",
    is_flag=True,
    default=False,
    help="v0.6.14: after submit, block until the job reaches a "
    "terminal state. Exit code reflects the job's outcome "
    "(see `vq wait --help`). Implementation is laptop-side "
    "polling: Ctrl-C cancels the WAIT and leaves the job "
    "running (use `vq kill JOBID` to abort). For a non-"
    "default poll cadence or a wait timeout, use the "
    "standalone `vq wait JOBID` verb instead.",
)
@click.option(
    "--fetch-on-done",
    "fetch_on_done",
    is_flag=True,
    default=False,
    help="v0.12.0: imply --wait, then fetch each finished job's "
    "workspace into the current directory (one <jobname>-<jobid>/ "
    "per job), so a one-shot submit returns its output without a "
    "separate `vq fetch`. Blocks the client until the job is "
    "terminal, so for a long calc background it or use "
    "`vq fetch-all HOST` later instead.",
)
@click.option(
    "--host",
    "host_opt",
    default=None,
    type=str,
    metavar="HOST",
    help="Target host. Overrides any positional host. Use this when the "
    "host name is not in config and would be misread as a file.",
)
@click.option(
    "--vibeqc-preflight",
    "vibeqc_preflight",
    is_flag=True,
    default=False,
    help="v0.6.14: run the submitted script once with "
    "VIBEQC_DRY_RUN=1 before queueing, harvest the OutputPlan "
    "from the resulting *.system manifest, and populate "
    "JobSpec.expected_outputs / output_stem. Only meaningful "
    "when the script uses ``vibeqc.run_job`` (other scripts "
    "are no-ops — failures are non-fatal). Opt-in because the "
    "pre-flight executes the user's Python file at submit "
    "time. Local submits only in v0.6.14; remote pre-flight "
    "is a follow-up.",
)
@click.option(
    "--scheduler-target",
    "scheduler_target",
    default=None,
    type=str,
    hidden=True,
    help="Internal (v1.0 cluster backend, design doc section 17): tag this spec "
    "for SSH+qsub dispatch to the named scheduler host. Set automatically when "
    "the CLI forwards a `vq submit --host <cluster>` to the cluster's driver "
    "daemon; users target the cluster with --host, not this flag.",
)
@click.option(
    "--json",
    "submit_json",
    is_flag=True,
    default=False,
    help="Emit a JSON submit receipt on stdout instead of the bare jobid(s): "
    "jobids, host, scheduler routing, anything currently holding dispatch "
    "(a drain, an in-flight admin update), configured-capacity warnings, "
    "and the follow-up commands. Use this from a script or an agent — a "
    "bare id cannot distinguish a job about to run from one parked behind "
    "a drain or over the daemon's configured caps.",
)
@click.argument("positional", nargs=-1, type=click.UNPROCESSED)
def submit(
    directory: str | None,
    archive: str | None,
    cpus: int,
    scheduler_tasks: int | None,
    mem_mb: int | None,
    wall_time_seconds: int | None,
    time_limit: str | None,
    priority: int,
    auto_resume: bool,
    retry: int,
    job_name: str | None,
    program_name: str | None,
    expected_sha: str | None,
    idempotency_key: str | None,
    qvf_force: bool,
    python_path: str | None,
    branch_name: str | None,
    branch_name_passthrough: str | None,
    refresh_before: str | None,
    pool_opt: str | None,
    tags: tuple[str, ...],
    at_iso: str | None,
    clean_tmp: bool,
    array: int,
    chain: int,
    rerun_until_file_exists: str | None,
    rerun_max: int,
    depends_on: tuple[str, ...],
    depends_on_any: tuple[str, ...],
    wait_submitted: bool,
    submission_timeout: float,
    wait_for_done: bool,
    fetch_on_done: bool,
    host_opt: str | None,
    vibeqc_preflight: bool,
    scheduler_target: str | None,
    submit_json: bool,
    positional: tuple[str, ...],
) -> None:
    """Submit a job to HOST.

    \b
    Host resolution:
      1. --host flag wins.
      2. Else first positional iff it's a known config host, "localhost",
         or this machine's hostname.
      3. Else default_host from ~/.config/vq/config.toml.

    \b
    Forms:
      vq submit HOST input.py                       (implicit "python input.py")
      vq submit HOST job.qvf --program NAME
      vq submit HOST --python /path/to/py input.py  (single-file, override interp)
      vq submit HOST -d DIR -- python run.py
      vq submit HOST -c ARCHIVE -- bash run.sh
      vq submit input.py                            (uses default_host)

    Put all vq options (including --idempotency-key) before `--`, then the
    executable and its arguments. The separator is required when payload
    arguments could be parsed as vq options. A leading option is not an
    executable; vq refuses it instead of queueing a malformed command.

    \b
    Watchdog notes (v0.5.38):
      The CPU-activity heuristic that drives STARVED kills samples the
      job's cgroup-v2 ``cpu.stat`` when available (every modern Linux
      host with systemd-user delegation), which sees ALL descendants
      regardless of pgid / session escapes. On macOS / hosts without
      cgroup-v2 the watchdog falls back to a pgid walk that can miss
      build tools that ``setsid`` / ``PR_SET_PGID`` their workers
      (ninja, some mpirun configurations, ...). If you target such a
      host AND your job's top-level command buffers stdout for long
      stretches (e.g. ``... 2>&1 | tail -3`` around a multi-minute
      ``pip install -e .``), the pgid-walk fallback can mistakenly
      STARVED-kill the job. Two workarounds: (a) let stdout stream
      live (drop the ``| tail`` pipe), (b) submit only to hosts with
      cgroup-v2.
    """
    try:
        submit_module._validate_submit_source_choice(
            directory=directory,
            archive=archive,
        )
    except ValueError as e:
        raise click.UsageError(str(e)) from None

    # v0.7.16: --time-limit / --time HH:MM:SS parses to the same
    # spec field as --wall-time-seconds. The two flags are mutually
    # exclusive — operators pick whichever ergonomic suits them.
    if time_limit is not None:
        if wall_time_seconds is not None:
            raise click.UsageError(
                "--time-limit / --time and --wall-time-seconds are "
                "mutually exclusive; both set the same spec field "
                "(JobSpec.wall_time_seconds)."
            )
        wall_time_seconds = _parse_time_limit(time_limit)

    cfg = config.load_config()
    try:
        requested_target = submit_module._resolve_requested_submit_target(
            cfg,
            host_opt=host_opt,
            positional=positional,
            pool=pool_opt,
            cpus=cpus,
            mem_mb=mem_mb,
            directory=directory,
            archive=archive,
            estimate_job_mem_mb=_auto_estimate_job_mem_mb,
            pick_auto_host=_pick_auto_host,
        )
    except ValueError as e:
        raise click.UsageError(str(e)) from None
    host = requested_target.requested_host
    receipt_host_for_errors = host
    rest = list(requested_target.remaining_args)
    if requested_target.auto_selected:
        estimated = requested_target.estimated_mem_mb
        estimate_note = (
            f" (≈{estimated} MB est.)" if estimated is not None else ""
        )
        click.echo(f"vq submit auto → {host}{estimate_note}", err=True)

    # v0.10.0 *Lampson's Hint*: refuse submits to an administratively-down
    # host (`vq host down`). The down mark is the submitter's "skip this box
    # for now" — queuing to it is almost always a mistake. `vq host up HOST`
    # clears it. localhost is never blocked (you can't be unreachable to
    # yourself).
    _down_entry = host_status.is_down(host)
    if _down_entry is not None and not is_local_host(host):
        raise click.ClickException(
            f"host {host!r} is marked administratively down "
            f"({_down_entry.describe()}). Run `vq host up {host}` to clear "
            f"it, or submit to a different host."
        )

    # Branch lookup is against the requested host, before a scheduler target
    # is rewritten to its driver.
    try:
        resolved_branch = submit_module._resolve_submit_branch(
            cfg,
            requested_host=host,
            branch_name=branch_name,
            branch_name_passthrough=branch_name_passthrough,
            python_path=python_path,
        )
    except ValueError as e:
        raise click.UsageError(str(e)) from None
    python_path = resolved_branch.python
    branch_for_spec = resolved_branch.branch_for_spec

    try:
        payload = submit_module._resolve_submit_payload(
            directory=directory,
            archive=archive,
            args=rest,
            qvf_force=qvf_force,
            array=array,
            chain=chain,
        )
    except ValueError as e:
        raise click.UsageError(str(e)) from None
    input_file = payload.input_file
    directory = payload.directory
    archive = payload.archive
    command = payload.command
    is_qvf_input = payload.is_qvf_input

    job_name = _normalize_job_name_option(job_name)
    if expected_sha is not None:
        expected_sha = expected_sha.strip()
    _validate_program_for_submit(cfg, program_name, local_spec=False)
    expected_sha = _validate_expected_sha_for_submit(
        cfg,
        program_name,
        expected_sha,
        local_spec=False,
    )

    # v0.6.12: parse --at and normalize to a canonical ISO 8601 string.
    # We REJECT naive timestamps (no tzinfo) at the CLI boundary so
    # the laptop-vs-server timezone-confusion footgun can't bite —
    # the operator gets a clear error rather than a job that fires
    # eight hours off when the daemon sits in a different TZ.
    not_before_iso: str | None = None
    if at_iso is not None:
        from datetime import datetime as _datetime

        try:
            parsed = _datetime.fromisoformat(at_iso)
        except ValueError as e:
            raise click.UsageError(
                f"--at {at_iso!r} is not a valid ISO 8601 timestamp: {e}. "
                f"Expected forms: `2026-05-20T22:00:00Z` (UTC), "
                f"`2026-05-20T17:00:00-05:00` (explicit offset)."
            ) from None
        if parsed.tzinfo is None:
            raise click.UsageError(
                f"--at {at_iso!r} has no timezone — naive timestamps "
                f"are rejected to avoid laptop-vs-server timezone "
                f"confusion. Append `Z` for UTC or an explicit offset "
                f"like `-05:00`."
            )
        # Re-emit in canonical form so the daemon sees a consistent
        # representation regardless of input variant
        # (`Z` vs `+00:00`, trailing fractional seconds, ...).
        not_before_iso = parsed.isoformat()

    try:
        submit_module._validate_submit_variants(
            array=array,
            chain=chain,
            refresh_before=refresh_before,
            idempotency_key=idempotency_key,
        )
    except ValueError as e:
        raise click.UsageError(str(e)) from None

    scheduler_submit_host = False
    if not is_local_host(host):
        try:
            scheduler_submit_host = cfg.host(host).scheduler != "local"
        except config.ConfigError:
            scheduler_submit_host = False

    # Scheduler hosts are forwarded to a driver that can mint these variants;
    # an ordinary remote daemon cannot receive their linkage or rerun policy.
    try:
        submit_module._validate_remote_submit_variants(
            requested_host=host,
            is_local=is_local_host(host),
            scheduler_submit_host=scheduler_submit_host,
            chain=chain,
            rerun_until_file_exists=rerun_until_file_exists,
        )
    except ValueError as e:
        raise click.UsageError(str(e)) from None

    # v0.6.52 + v0.8.7: --array N spawns N independent specs;
    # --chain N spawns N depends_on-linked specs. List-of-jobids
    # return; CLI echoes each on its own line so shell capture
    # (`jobids=$(vq submit --array N ...)` → `for j in $jobids;
    # do …; done`) works without further parsing. N=1 stays on
    # the legacy single-spec path so no array/chain fields are
    # written for the common single-submit case.
    submit_request = submit_module._NormalizedSubmitRequest(
        input_file=input_file,
        directory=directory,
        archive=archive,
        command=command,
        is_qvf_input=is_qvf_input,
        python=python_path,
        cpus=cpus,
        scheduler_tasks=scheduler_tasks,
        mem_mb=mem_mb,
        wall_time_seconds=wall_time_seconds,
        priority=priority,
        auto_resume=auto_resume,
        retry=retry,
        job_name=job_name,
        branch=branch_for_spec,
        program=program_name,
        expected_sha=expected_sha,
        tags=list(tags) if tags else None,
        not_before=not_before_iso,
        depends_on=list(depends_on) if depends_on else None,
        depends_on_any=list(depends_on_any) if depends_on_any else None,
        rerun_until_file_exists=rerun_until_file_exists,
        rerun_max=rerun_max,
        clean_workdir_on_terminal=clean_tmp,
        vibeqc_preflight=vibeqc_preflight,
        array=array,
        chain=chain,
        refresh_before=refresh_before,
        qvf_force=qvf_force,
        idempotency_key=idempotency_key,
    )
    try:
        classified_target = submit_module._classify_submit_target(
            cfg,
            requested_host=host,
            delegated_scheduler_target=scheduler_target,
        )
    except config.ConfigError as e:
        raise click.UsageError(str(e)) from None
    if not math.isfinite(submission_timeout):
        raise click.UsageError("--submission-timeout must be finite")
    if wait_submitted and classified_target.scheduler_target is None:
        raise click.UsageError("--wait-submitted requires a scheduler target")
    capacity_warnings: list[str] = []

    def _record_submit_warning(message: str) -> None:
        capacity_warnings.append(message)
        click.echo(
            f"{submit_module._SUBMIT_WARNING_PREFIX}{message}",
            err=True,
        )

    try:
        submit_plan = submit_module._resolve_submit_plan(
            cfg,
            target=classified_target,
            request=submit_request,
        )
        host = submit_plan.target.execution_host
        multi_user = _multi_user_active(cfg) if is_local_host(host) else False
        jobids = submit_module._execute_submit_plan(
            submit_plan,
            multi_user=multi_user,
            warning_sink=_record_submit_warning,
        )
    except NotImplementedError as e:
        raise click.UsageError(str(e)) from None
    except (ValueError, FileNotFoundError) as e:
        raise click.UsageError(str(e)) from None
    except transport.RemoteOutcomeUnknown as e:
        if submit_json:
            import json as _json

            click.echo(
                _json.dumps(
                    {
                        "error": {
                            "message": str(e),
                            "retry_safe": False,
                            "type": "remote_outcome_unknown",
                        },
                        "host": receipt_host_for_errors,
                        "jobids": [],
                        "outcome": "unknown",
                    },
                    sort_keys=True,
                )
            )
            raise click.exceptions.Exit(1) from None
        raise click.ClickException(
            f"{e}\nRemote submit outcome is unknown: the job may have been "
            "accepted. Do not replay this submit automatically; reconcile "
            "the queue authority and retained staging first."
        ) from None
    except transport.RemoteError as e:
        raise click.ClickException(str(e)) from None

    # `host` has been rewritten to the driver for a scheduler target; the
    # submitter cares about the cluster they named, not the machine that
    # dispatches to it.
    receipt = _submit_receipt(
        cfg,
        submit_plan.target.receipt_host,
        jobids,
        runtime_pin=submit_plan.receipt_runtime_pin,
        capacity_warnings=capacity_warnings,
    )
    acceptance_exit_code = 0
    if wait_submitted:
        # The queue authority may be a remote driver, distinct from the named
        # scheduler host. Do not delegate another submit or wait on the cluster.
        try:
            observations = wait_for_scheduler_acceptance(
                host, jobids,
                scheduler_target=submit_plan.target.scheduler_target,
                host_cfg=submit_plan.target.execution_host_config,
                timeout=submission_timeout,
                multi_user=multi_user,
            )
            receipt["scheduler_acceptance"] = [
                item.to_json_payload() for item in observations
            ]
            if any(item.outcome == "failed" for item in observations):
                acceptance_exit_code = 1
            elif any(item.cli_exit_code for item in observations):
                acceptance_exit_code = 124
            receipt["execution_status"] = (
                "scheduler_accepted" if acceptance_exit_code == 0 else "acceptance_incomplete"
            )
            if acceptance_exit_code == 0:
                receipt["acceptance_scope"] = "scheduler"
            for item in observations:
                if item.cli_exit_code:
                    click.echo(
                        f"scheduler acceptance for {item.jobid}: {item.outcome}"
                        f" ({item.detail or item.state or 'not observed'})",
                        err=True,
                    )
        except KeyboardInterrupt:
            acceptance_exit_code = 130
            receipt["execution_status"] = "acceptance_interrupted"
        receipt["cli_exit_code"] = acceptance_exit_code
        receipt["retry_safe"] = False
        if acceptance_exit_code:
            click.echo(
                "Acceptance wait did not succeed; queued jobs are retained. "
                "Inspect these job IDs; do not automatically resubmit.", err=True,
            )
    if submit_json:
        import json as _json

        click.echo(_json.dumps(receipt, indent=2, sort_keys=True))
    else:
        # stdout stays exactly the bare jobid(s) — the contract every wrapper
        # script and agent in the fleet parses. Context goes to stderr, and
        # only at a TTY; see _narrate_submit_receipt.
        for jid in jobids:
            click.echo(jid)
        _narrate_submit_receipt(receipt)
    if acceptance_exit_code:
        raise click.exceptions.Exit(acceptance_exit_code)
    # Legacy single-jobid variable for the --wait / SystemExit path
    # below (which still waits on one job; --wait + --array iterates
    # in sequence).
    jobid = jobids[0]

    # v0.6.14: --wait makes submit synchronous. Same dispatch-via-
    # host as the standalone `vq wait` verb. We've already echoed
    # the jobid to stdout, so shell wrappers can still capture it
    # even when --wait fails (timeout, ctrl-c) — only the exit
    # code carries the wait verdict.
    # v0.12.0 *Hollerith's Return*: --fetch-on-done implies --wait
    # (there is nothing to fetch until the job is terminal).
    if fetch_on_done:
        wait_for_done = True
    if wait_for_done:
        host_cfg_for_wait = None
        if not is_local_host(host):
            try:
                host_cfg_for_wait = cfg.host(host)
            except config.ConfigError as e:
                raise click.UsageError(str(e)) from None
        # v0.6.52: --wait + --array waits on every element in
        # submission order. Exit code is the WORST per-element
        # cli_exit_code (so a 3/30 success → non-zero exit).
        # Sequential not parallel — keeps the implementation
        # trivial; operator who wants parallel polling can drop
        # --wait and use `for j in $jobids; do vq wait $j & done`.
        try:
            results = []
            for jid in jobids:
                results.append(
                    wait_for_terminal(
                        host,
                        jid,
                        host_cfg=host_cfg_for_wait,
                        poll_interval=DEFAULT_POLL_INTERVAL_SECONDS,
                        timeout=None,
                        multi_user=_multi_user_active(cfg),
                    )
                )
            # Legacy single-result variable for the echo below.
            result = (
                results[0]
                if len(results) == 1
                else max(
                    results,
                    key=lambda r: r.cli_exit_code,
                )
            )
        except KeyboardInterrupt:
            click.echo(
                f"wait for {jobid} interrupted; job continues to run",
                err=True,
            )
            raise SystemExit(130) from None
        except transport.RemoteError as e:
            raise click.ClickException(str(e)) from None
        # v0.12.0 *Hollerith's Return*: each job is terminal now, so pull
        # its workspace back to the submitting dir. Best-effort per job: a
        # fetch failure (workspace cleaned up, dest already present from a
        # prior fetch) is reported but never changes the wait's exit code.
        if fetch_on_done:
            for jid in jobids:
                try:
                    if is_local_host(host):
                        _dst = fetch_local(jid, Path("."), multi_user=_multi_user_active(cfg))
                    else:
                        _dst = fetch_remote(host_cfg_for_wait, jid, Path("."))
                    require_fresh_fetch_manifest(
                        _dst,
                        jobid=jid,
                        source_kind="workspace",
                    )
                    click.echo(f"fetched {jid} -> {_dst}", err=True)
                except (
                    FileNotFoundError,
                    FileExistsError,
                    ValueError,
                    transport.RemoteError,
                ) as e:
                    click.echo(f"fetch of {jid} failed: {e}", err=True)
        click.echo(
            f"{jobid}: {result.state.value}" + _exit_suffix(result.exit_code),
            err=True,
        )
        raise SystemExit(result.cli_exit_code)


@main.command()
@click.option(
    "--all",
    "all_jobs",
    is_flag=True,
    default=False,
    help="Pause every RUNNING job in the queue. Mutually exclusive "
    "with passing a JOBID. Useful when you want to free the box "
    "for something else (e.g. a video game) without manually "
    "listing every running job.",
)
@click.option(
    "--paused-by",
    "paused_by",
    default=None,
    type=str,
    metavar="TAG",
    help="v0.6.22: record an actor tag on the paused spec(s). "
    "Pair with `vq resume --paused-by TAG` to scope a resume "
    "to exactly the jobs this tag paused — useful for scripts "
    "that pause the queue for a build and want to resume only "
    "what they paused (operator-paused jobs stay paused). "
    "Charset: alnum + `-` `_` `.`, ≤50 chars. First-pauser-"
    "wins: re-pausing an already-SUSPENDED job is still a "
    "no-op; the existing paused_by stays unchanged.",
)
@click.argument("host_or_jobid", required=False)
@click.argument("jobid_if_host", required=False)
def pause(
    all_jobs: bool,
    paused_by: str | None,
    host_or_jobid: str | None,
    jobid_if_host: str | None,
) -> None:
    """SIGSTOP a RUNNING job's process group; mark it SUSPENDED.

    \b
    Forms:
      vq pause HOST JOBID
      vq pause JOBID            (uses default_host from config)
      vq pause --all            (every RUNNING job, default_host)
      vq pause HOST --all       (every RUNNING job on HOST)
      vq pause --all --paused-by update-script  (mark batch v0.6.22+)

    The job's RAM stays allocated (no checkpoint-to-disk). Use this
    when you want to free CPU temporarily; if you also need RAM back,
    you'll have to kill the job and resubmit later.
    """
    cfg = config.load_config()
    # v0.6.38: pause/resume resolve specs from the per-user state dirs
    # on a multi-user host.
    mu = _multi_user_active(cfg)

    if paused_by is not None and not JOB_NAME_PATTERN.fullmatch(paused_by):
        raise click.UsageError(
            f"--paused-by {paused_by!r} is invalid: must be "
            f"alphanumerics + '-', '_', '.' only "
            f"(1-{JOB_NAME_MAX_LEN} chars)."
        )

    if all_jobs:
        if jobid_if_host is not None:
            raise click.UsageError("--all is mutually exclusive with a JOBID argument")
        # Forms: `vq pause --all` (host = default_host)
        #        `vq pause HOST --all` (host = positional)
        host = host_or_jobid if host_or_jobid is not None else _resolve_host(cfg, None)
        scheduler_driver = _scheduler_driver_host(cfg, host)
        if scheduler_driver is not None:
            if is_local_host(scheduler_driver):
                try:
                    msg = pause_scheduler_all(
                        host,
                        cfg.host(host),
                        paused_by=paused_by,
                        multi_user=mu,
                    )
                except (
                    config.ConfigError,
                    ownership.OwnershipError,
                    PauseError,
                ) as e:
                    raise click.UsageError(str(e)) from None
                click.echo(msg)
            else:
                remote_args = ["pause", host, "--all"]
                if paused_by is not None:
                    remote_args.extend(["--paused-by", paused_by])
                click.echo(
                    _delegate_to_remote(scheduler_driver, cfg, *remote_args),
                    nl=False,
                )
            return
        if is_local_host(host):
            try:
                msg = pause_all(host, paused_by=paused_by, multi_user=mu)
            except (
                config.ConfigError,
                ownership.OwnershipError,
                NotImplementedError,
            ) as e:
                raise click.UsageError(str(e)) from None
            click.echo(msg)
        else:
            remote_args = ["pause", "--all"]
            if paused_by is not None:
                remote_args.extend(["--paused-by", paused_by])
            click.echo(
                _delegate_to_remote(host, cfg, *remote_args),
                nl=False,
            )
        return

    if host_or_jobid is None:
        raise click.UsageError(
            "missing JOBID. Use `vq pause <jobid>`, `vq pause HOST <jobid>`, "
            "or `vq pause --all` to pause every running job."
        )

    if jobid_if_host is None:
        jobid = host_or_jobid
        host = _resolve_job_host(cfg, None, jobid)
    else:
        host = host_or_jobid
        jobid = jobid_if_host

    scheduler_driver = _scheduler_driver_host(cfg, host)
    if scheduler_driver is not None:
        if is_local_host(scheduler_driver):
            try:
                msg = pause_scheduler_job(
                    host,
                    cfg.host(host),
                    jobid,
                    paused_by=paused_by,
                    multi_user=mu,
                )
            except (
                config.ConfigError,
                FileNotFoundError,
                ownership.OwnershipError,
                PauseError,
            ) as e:
                raise click.UsageError(str(e)) from None
            click.echo(msg)
        else:
            remote_args = ["pause", host, jobid]
            if paused_by is not None:
                remote_args.extend(["--paused-by", paused_by])
            click.echo(
                _delegate_to_remote(scheduler_driver, cfg, *remote_args),
                nl=False,
            )
        return

    if is_local_host(host):
        try:
            msg = pause_job(host, jobid, paused_by=paused_by, multi_user=mu)
        except (
            config.ConfigError,
            FileNotFoundError,
            ownership.OwnershipError,
            PauseError,
            NotImplementedError,
        ) as e:
            raise click.UsageError(str(e)) from None
        click.echo(msg)
    else:
        remote_args = ["pause", "localhost", jobid]
        if paused_by is not None:
            remote_args.extend(["--paused-by", paused_by])
        click.echo(_delegate_to_remote(host, cfg, *remote_args), nl=False)


@main.command()
@click.option(
    "--all",
    "all_jobs",
    is_flag=True,
    default=False,
    help="Resume every SUSPENDED job in the queue. Mutually exclusive with passing a JOBID.",
)
@click.option(
    "--paused-by",
    "paused_by",
    default=None,
    type=str,
    metavar="TAG",
    help="v0.6.22: only resume jobs whose ``spec.paused_by`` matches "
    "the given tag. Pairs with `vq pause --paused-by TAG`: a "
    "script that paused the queue with `--paused-by my-script` "
    "calls `vq resume --all --paused-by my-script` to resume "
    "ONLY what it paused, leaving operator-paused (untagged) "
    "jobs paused. Implies `--all` if no JOBID is given. When "
    "applied to a single JOBID, it's enforced: a mismatch "
    "errors instead of silently leaving the job paused.",
)
@click.argument("host_or_jobid", required=False)
@click.argument("jobid_if_host", required=False)
def resume(
    all_jobs: bool,
    paused_by: str | None,
    host_or_jobid: str | None,
    jobid_if_host: str | None,
) -> None:
    """SIGCONT a SUSPENDED job; mark it RUNNING; bank the paused interval.

    \b
    Forms:
      vq resume HOST JOBID
      vq resume JOBID           (uses default_host from config)
      vq resume --all           (every SUSPENDED job, default_host)
      vq resume HOST --all      (every SUSPENDED job on HOST)

    Paused-while-SUSPENDED time is excluded from the job's
    wall_time_seconds budget, so a long pause won't cause a spurious
    TIME_EXCEEDED later.
    """
    cfg = config.load_config()
    # v0.6.38: pause/resume resolve specs from the per-user state dirs
    # on a multi-user host.
    mu = _multi_user_active(cfg)

    if paused_by is not None and not JOB_NAME_PATTERN.fullmatch(paused_by):
        raise click.UsageError(
            f"--paused-by {paused_by!r} is invalid: must be "
            f"alphanumerics + '-', '_', '.' only "
            f"(1-{JOB_NAME_MAX_LEN} chars)."
        )

    if all_jobs:
        if jobid_if_host is not None:
            raise click.UsageError("--all is mutually exclusive with a JOBID argument")
        host = host_or_jobid if host_or_jobid is not None else _resolve_host(cfg, None)
        scheduler_driver = _scheduler_driver_host(cfg, host)
        if scheduler_driver is not None:
            if is_local_host(scheduler_driver):
                try:
                    msg = resume_scheduler_all(
                        host,
                        cfg.host(host),
                        paused_by_filter=paused_by,
                        multi_user=mu,
                    )
                except (
                    config.ConfigError,
                    ownership.OwnershipError,
                    PauseError,
                ) as e:
                    raise click.UsageError(str(e)) from None
                click.echo(msg)
            else:
                remote_args = ["resume", host, "--all"]
                if paused_by is not None:
                    remote_args.extend(["--paused-by", paused_by])
                click.echo(
                    _delegate_to_remote(scheduler_driver, cfg, *remote_args),
                    nl=False,
                )
            return
        if is_local_host(host):
            try:
                msg = resume_all(host, paused_by_filter=paused_by, multi_user=mu)
            except (
                config.ConfigError,
                ownership.OwnershipError,
                NotImplementedError,
            ) as e:
                raise click.UsageError(str(e)) from None
            click.echo(msg)
        else:
            remote_args = ["resume", "--all"]
            if paused_by is not None:
                remote_args.extend(["--paused-by", paused_by])
            click.echo(
                _delegate_to_remote(host, cfg, *remote_args),
                nl=False,
            )
        return

    if host_or_jobid is None:
        raise click.UsageError(
            "missing JOBID. Use `vq resume <jobid>`, `vq resume HOST <jobid>`, "
            "or `vq resume --all` to resume every suspended job."
        )

    if jobid_if_host is None:
        jobid = host_or_jobid
        host = _resolve_job_host(cfg, None, jobid)
    else:
        host = host_or_jobid
        jobid = jobid_if_host

    scheduler_driver = _scheduler_driver_host(cfg, host)
    if scheduler_driver is not None:
        if is_local_host(scheduler_driver):
            try:
                msg = resume_scheduler_job(
                    host,
                    cfg.host(host),
                    jobid,
                    paused_by_filter=paused_by,
                    multi_user=mu,
                )
            except (
                config.ConfigError,
                FileNotFoundError,
                ownership.OwnershipError,
                PauseError,
            ) as e:
                raise click.UsageError(str(e)) from None
            click.echo(msg)
        else:
            remote_args = ["resume", host, jobid]
            if paused_by is not None:
                remote_args.extend(["--paused-by", paused_by])
            click.echo(
                _delegate_to_remote(scheduler_driver, cfg, *remote_args),
                nl=False,
            )
        return

    if is_local_host(host):
        try:
            # The paused-by filter belongs in the same locked, authorized
            # transaction as the state check and SIGCONT. Reading the legacy
            # single-user path here leaked foreign metadata and missed
            # multi-user specs entirely.
            msg = resume_job(
                host,
                jobid,
                paused_by_filter=paused_by,
                multi_user=mu,
            )
        except (
            config.ConfigError,
            FileNotFoundError,
            ownership.OwnershipError,
            PauseError,
            NotImplementedError,
        ) as e:
            raise click.UsageError(str(e)) from None
        click.echo(msg)
    else:
        remote_args = ["resume", "localhost", jobid]
        if paused_by is not None:
            remote_args.extend(["--paused-by", paused_by])
        click.echo(_delegate_to_remote(host, cfg, *remote_args), nl=False)


@main.command()
@click.option(
    "--weight",
    "weight",
    type=click.IntRange(min=1, max=10_000),
    default=None,
    metavar="N",
    help="Set CPUWeight to N. Range 1-10000; default systemd value is 100. "
    "Lower N means the job's cgroup gets less CPU under contention "
    "(e.g. --weight 20 → ~17%% of disputed cores vs a default 100). "
    "When nothing else wants CPU, the job still uses all available. "
    "Mutually exclusive with --restore.",
)
@click.option(
    "--restore",
    "restore",
    is_flag=True,
    default=False,
    help="Reset CPUWeight to systemd's default (100). Equivalent to "
    "--weight 100 but reads more naturally for the common "
    "kids-are-done-gaming use case.",
)
@click.option(
    "--all",
    "all_jobs",
    is_flag=True,
    default=False,
    help="Apply to every RUNNING job in the queue. Mutually exclusive "
    "with passing a JOBID. SUSPENDED jobs are skipped (they're "
    "already not using CPU).",
)
@click.option(
    "--persist",
    "persist",
    is_flag=True,
    default=False,
    help="ALSO write throttle.json so newly-dispatched jobs inherit "
    "the CPUWeight. Requires --all. Cleared by --restore --all "
    "or by --release-persist. Useful for kids-gaming-for-2-hours "
    "windows where new submissions during the window should also "
    "be deprioritised. v0.5.15+.",
)
@click.option(
    "--release-persist",
    "release_persist",
    is_flag=True,
    default=False,
    help="Clear ONLY the persistent throttle.json state (running jobs "
    "untouched). Use this if you set --persist but already "
    "manually restored running jobs.",
)
@click.option(
    "--status",
    "status_only",
    is_flag=True,
    default=False,
    help="Print the persistent throttle state and exit. Doesn't mutate anything.",
)
@click.option(
    "--reason",
    "reason",
    type=str,
    default=None,
    metavar="TEXT",
    help="Optional free-text label for the persistent throttle "
    "(e.g. 'kids gaming Sat afternoon'). Surfaced via --status. "
    "Only meaningful with --persist.",
)
@click.option(
    "--duration",
    "duration",
    type=str,
    default=None,
    metavar="DUR",
    help="Auto-release the persistent throttle after DUR elapses. "
    "Format: integer + unit ('2h', '30m', '600s', '1d', '1w'). "
    "Only meaningful with --persist. Daemon clears the state "
    "file the next time anyone reads it after the deadline, "
    "so the auto-release is effectively immediate from the "
    "perspective of new dispatches.",
)
@click.option(
    "--all-hosts",
    "all_hosts",
    is_flag=True,
    default=False,
    help="v0.8.5 *Knuth's Concrete*: apply the persistent-throttle "
    "operation (--status / --persist + weight / --release-persist) "
    "to EVERY host in parallel. Per-host failures land inline. "
    "Only valid with --status, --release-persist, or "
    "--persist (per-job ops can't fan out because jobids are "
    "host-local). Distinct from --all (which means 'every "
    "running job on ONE host').",
)
@click.argument("host_or_jobid", required=False)
@click.argument("jobid_if_host", required=False)
def throttle(
    weight: int | None,
    restore: bool,
    all_jobs: bool,
    persist: bool,
    release_persist: bool,
    status_only: bool,
    reason: str | None,
    duration: str | None,
    all_hosts: bool,
    host_or_jobid: str | None,
    jobid_if_host: str | None,
) -> None:
    """Soft CPU-priority adjustment via cgroup CPUWeight on a RUNNING job.

    \b
    Forms:
      vq throttle JOBID --weight N            (single job, default_host)
      vq throttle HOST JOBID --weight N
      vq throttle JOBID --restore             (back to CPUWeight=100)
      vq throttle --all --weight N            (every running job, default_host)
      vq throttle HOST --all --weight N
      vq throttle --all --restore             (everyone back to default)

    Soft-throttle vs `vq pause`: throttle keeps the job running but
    deprioritises it under contention (the kids' game gets ~83%% of
    disputed cores at --weight 20). pause hard-freezes the process at
    0%% CPU. Use throttle when the box has headroom and you want
    background progress; use pause when you want full priority.

    v0.5.13 default behavior: doesn't persist. A job submitted after
    `vq throttle --all --weight 20` starts at default CPUWeight=100.

    v0.5.15+: --persist (requires --all) writes throttle.json so the
    daemon applies the weight to newly-dispatched scopes. Cleared by
    --restore --all (which also restores running scopes) or by
    --release-persist (state file only, running scopes untouched).
    --status shows the current persistent state.

    cgroup-only: hosts without `systemd-run --user --scope` delegation
    get a ThrottleError. The renice fallback for non-cgroup hosts is
    on the v0.6.x followup list.

    v0.6.37: on a multi-user host a job's scope is a root-owned
    system scope, so `vq throttle` there must run as root
    (`sudo vq throttle ...`); an admin can throttle any user's job.
    """
    cfg = config.load_config()
    # v0.6.37: on a multi-user host the job's cgroup scope is a
    # root-owned system scope; throttle_module checks for root and
    # resolves specs from the per-user state dirs accordingly.
    mu = _multi_user_active(cfg)

    # v0.8.5: --all-hosts gates. Fan-out only applies to
    # persistent-throttle ops; per-job ops need a host-local jobid.
    if all_hosts:
        # Position arg shouldn't be a host_or_jobid in --all-hosts mode.
        if host_or_jobid is not None or jobid_if_host is not None:
            raise click.UsageError(
                "--all-hosts and HOST/JOBID are mutually exclusive; "
                "--all-hosts walks every configured host",
            )
        if not (status_only or release_persist or persist):
            raise click.UsageError(
                "--all-hosts is only valid with --status, "
                "--release-persist, or --persist (per-job throttle "
                "ops can't fan out across hosts because jobids are "
                "host-local)",
            )
        if not cfg.hosts:
            click.echo("(no [hosts.X] configured)")
            return
        # Build the per-host args; the local arm calls module
        # primitives directly so we don't roundtrip through `vq`.
        if status_only:
            # "localhost" so each remote acts on its own daemon, not its
            # default_host (same guard as drain --all).
            per_host_args = ["throttle", "--status", "localhost"]

            def _throttle_one(h: str) -> str:
                notice = _scheduler_throttle_notice(cfg, h)
                if notice is not None:
                    return notice
                if is_local_host(h):
                    return throttle_module.format_throttle_status()
                return _delegate_to_remote(
                    h,
                    cfg,
                    *per_host_args,
                ).rstrip()
        elif release_persist:
            # "localhost" so each remote acts on its own daemon, not its
            # default_host (same guard as drain --all).
            per_host_args = ["throttle", "--release-persist", "localhost"]

            def _throttle_one(h: str) -> str:  # noqa: F811
                notice = _scheduler_throttle_notice(cfg, h)
                if notice is not None:
                    return notice
                if is_local_host(h):
                    removed = throttle_module.clear_throttle_state()
                    return (
                        "persistent throttle cleared (running jobs untouched)"
                        if removed
                        else "persistent throttle was not set (no-op)"
                    )
                return _delegate_to_remote(
                    h,
                    cfg,
                    *per_host_args,
                ).rstrip()
        else:  # persist + --weight
            if weight is None:
                raise click.UsageError(
                    "--persist --all-hosts requires --weight N",
                )
            # "localhost" so each remote acts on its own daemon, not its
            # default_host (same guard as drain --all and the two throttle
            # --all-hosts branches above).
            per_host_args = [
                "throttle",
                "--all",
                "--persist",
                "--weight",
                str(weight),
                "localhost",
            ]
            if reason is not None:
                per_host_args.extend(["--reason", reason])
            if duration is not None:
                per_host_args.extend(["--duration", duration])

            def _throttle_one(h: str) -> str:  # noqa: F811
                notice = _scheduler_throttle_notice(cfg, h)
                if notice is not None:
                    return notice
                if is_local_host(h):
                    # Persist state only — running jobs on the LOCAL
                    # box would also need wiring via --all, but for
                    # fleet-wide --all-hosts we keep the local arm
                    # to "set persistent state" so the behaviour is
                    # uniform across hosts (no host gets its running
                    # jobs throttled differently). Operators wanting
                    # to throttle running-jobs across the fleet do
                    # it host-by-host today; v0.8.6 might revisit.
                    duration_seconds: int | None = None
                    if duration is not None:
                        try:
                            duration_seconds = int(
                                parse_age(duration).total_seconds(),
                            )
                        except ValueError as e:
                            raise click.UsageError(
                                f"--duration: {e}",
                            ) from None
                    state = throttle_module.ThrottleState(
                        weight=weight,
                        reason=reason,
                        duration_seconds=duration_seconds,
                    )
                    throttle_module.write_throttle_state(state)
                    return f"persistent throttle set (CPUWeight={weight})"
                return _delegate_to_remote(
                    h,
                    cfg,
                    *per_host_args,
                ).rstrip()

        click.echo(_aggregate_per_host(cfg, _throttle_one))
        return

    # v0.5.15: --status reads the persistent throttle file and returns.
    if status_only:
        host = host_or_jobid if host_or_jobid is not None else _resolve_host(cfg, None)
        notice = _scheduler_throttle_notice(cfg, host)
        if notice is not None:
            click.echo(notice)
            return
        if is_local_host(host):
            click.echo(throttle_module.format_throttle_status())
        else:
            click.echo(
                _delegate_to_remote(host, cfg, "throttle", "--status"),
                nl=False,
            )
        return

    # v0.5.15: --release-persist clears throttle.json without touching
    # running scopes. Useful when the user already restored running jobs
    # manually and just wants to stop persisting for new dispatches.
    if release_persist:
        host = host_or_jobid if host_or_jobid is not None else _resolve_host(cfg, None)
        _reject_scheduler_throttle(cfg, host)
        if is_local_host(host):
            removed = throttle_module.clear_throttle_state()
            if removed:
                click.echo("persistent throttle cleared (running jobs untouched)")
            else:
                click.echo("persistent throttle was not set (no-op)")
        else:
            click.echo(
                _delegate_to_remote(host, cfg, "throttle", "--release-persist"),
                nl=False,
            )
        return

    if weight is None and not restore:
        raise click.UsageError("must specify --weight N, --restore, --status, or --release-persist")
    if weight is not None and restore:
        raise click.UsageError("--weight and --restore are mutually exclusive")
    if persist and not all_jobs:
        raise click.UsageError(
            "--persist requires --all (persistence is queue-wide; "
            "per-job persistence isn't a thing -- jobs aren't re-dispatched)"
        )
    if persist and restore:
        raise click.UsageError(
            "--persist and --restore are mutually exclusive; "
            "use --restore --all to clear both running and persistent state"
        )

    effective_weight = throttle_module.DEFAULT_CPU_WEIGHT if restore else weight
    assert effective_weight is not None  # mypy

    if all_jobs:
        if jobid_if_host is not None:
            raise click.UsageError("--all is mutually exclusive with a JOBID argument")
        host = host_or_jobid if host_or_jobid is not None else _resolve_host(cfg, None)
        _reject_scheduler_throttle(cfg, host)
        if is_local_host(host):
            try:
                if restore:
                    msg = throttle_module.restore_all(host, multi_user=mu)
                    # --restore --all ALSO clears the persistent state
                    # so newly-dispatched jobs go back to default weight.
                    cleared = throttle_module.clear_throttle_state()
                    if cleared:
                        msg += " (persistent state cleared)"
                else:
                    msg = throttle_module.throttle_all(host, effective_weight, multi_user=mu)
                    if persist:
                        # v0.5.16: --duration is parsed via cleanup.parse_age
                        # (same "30d"/"2h"/etc. format used by --older-than).
                        duration_seconds: int | None = None
                        if duration is not None:
                            try:
                                duration_seconds = int(parse_age(duration).total_seconds())
                            except ValueError as e:
                                raise click.UsageError(f"--duration: {e}") from None
                        state = throttle_module.ThrottleState(
                            weight=effective_weight,
                            reason=reason,
                            duration_seconds=duration_seconds,
                        )
                        throttle_module.write_throttle_state(state)
                        msg += (
                            f" (persistent: new jobs will also start at "
                            f"CPUWeight={effective_weight}"
                        )
                        if duration_seconds is not None:
                            msg += f"; auto-release after {duration_seconds}s"
                        msg += ")"
            except NotImplementedError as e:
                raise click.UsageError(str(e)) from None
            except ThrottleError as e:
                raise click.UsageError(str(e)) from None
            click.echo(msg)
        else:
            args = ["throttle", "--all"]
            if restore:
                args.append("--restore")
            else:
                args.extend(["--weight", str(effective_weight)])
                if persist:
                    args.append("--persist")
                    if reason is not None:
                        args.extend(["--reason", reason])
                    if duration is not None:
                        args.extend(["--duration", duration])
            click.echo(_delegate_to_remote(host, cfg, *args), nl=False)
        return

    if host_or_jobid is None:
        raise click.UsageError(
            "missing JOBID. Use `vq throttle <jobid> --weight N`, "
            "`vq throttle HOST <jobid> --weight N`, or "
            "`vq throttle --all --weight N` to apply to every running job."
        )

    if jobid_if_host is None:
        jobid = host_or_jobid
        host = _resolve_job_host(cfg, None, jobid)
    else:
        host = host_or_jobid
        jobid = jobid_if_host

    _reject_scheduler_throttle(cfg, host)

    if is_local_host(host):
        try:
            if restore:
                msg = throttle_module.restore_job(host, jobid, multi_user=mu)
            else:
                msg = throttle_module.throttle_job(host, jobid, effective_weight, multi_user=mu)
        except FileNotFoundError as e:
            raise click.UsageError(str(e)) from None
        except ThrottleError as e:
            raise click.UsageError(str(e)) from None
        except NotImplementedError as e:
            raise click.UsageError(str(e)) from None
        click.echo(msg)
    else:
        args = ["throttle", "localhost", jobid]
        if restore:
            args.append("--restore")
        else:
            args.extend(["--weight", str(effective_weight)])
        click.echo(_delegate_to_remote(host, cfg, *args), nl=False)


@main.command("drain")
@click.option(
    "--max-jobs",
    "max_jobs",
    type=click.IntRange(min=0),
    default=None,
    metavar="N",
    help="Partial drain: lower the daemon's concurrent-jobs cap to N. "
    "Running jobs continue; new dispatches stop when N are already "
    "running. N=0 has the same effect as full drain.",
)
@click.option(
    "--max-cpus",
    "max_cpus",
    type=click.IntRange(min=0),
    default=None,
    metavar="N",
    help="Partial drain: lower the effective CPU budget to N. Small "
    "jobs (--cpus 1) keep flowing; big jobs (--cpus > N) block "
    "until released.",
)
@click.option(
    "--release",
    "release",
    is_flag=True,
    default=False,
    help="Remove the drain state; daemon goes back to its configured "
    "caps. Idempotent (no error if drain wasn't set).",
)
@click.option(
    "--release-full",
    "release_full",
    is_flag=True,
    default=False,
    help=(
        "Release only the global/full dispatch hold, preserving any "
        "--scheduler-host lanes. This is the safe handoff after adding a "
        "scheduler-target gate under a full drain."
    ),
)
@click.option(
    "--status",
    "status_only",
    is_flag=True,
    default=False,
    help="Print current drain state and exit. Doesn't mutate anything.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="With --status, emit a machine-readable drain-state payload.",
)
@click.option(
    "--reason",
    "reason",
    type=str,
    default=None,
    metavar="TEXT",
    help="Optional free-text label for why drain is set "
    "(e.g. 'kids gaming', 'maintenance window'). Surfaced via --status.",
)
@click.option(
    "--duration",
    "duration",
    type=str,
    default=None,
    metavar="DUR",
    help="Auto-release drain after DUR elapses. Format: integer + "
    "unit ('2h', '30m', '600s', '1d', '1w'). The state file "
    "self-clears on the next read after the deadline, so the "
    "drain is effectively lifted within one daemon poll_interval "
    "of the deadline. Useful for bounded windows where forgetting "
    "to release would be the dominant failure mode (kids gaming, "
    "maintenance windows).",
)
@click.option(
    "--update-mode",
    "update_mode",
    type=click.Choice(["accept", "deny"]),
    default=None,
    metavar="MODE",
    help=(
        "Mark the drain as an update pause. MODE=accept keeps accepting new "
        "submissions as pending work for later; MODE=deny rejects new "
        "submissions while the drain is active."
    ),
)
@click.option(
    "--scheduler-host",
    "scheduler_hosts",
    multiple=True,
    metavar="HOST",
    help=(
        "Drain only this scheduler target: matching pending scheduler rows stay "
        "PENDING and are not qsub/sbatch-dispatched, while other scheduler "
        "targets can still dispatch. May be repeated. Combine with --release "
        "to clear one scheduler-target drain."
    ),
)
@click.option(
    "--token",
    "cli_token",
    default=None,
    metavar="TOKEN",
    help="Admin bearer token. Discouraged: visible in shell history and ps; "
    "prefer $VQ_TOKEN, --token-stdin, or --token-file.",
)
@click.option(
    "--token-stdin",
    "token_stdin",
    is_flag=True,
    default=False,
    help="Read the admin bearer token from stdin (one line).",
)
@click.option(
    "--token-file",
    "token_file",
    type=click.Path(dir_okay=False),
    default=None,
    metavar="PATH",
    help="Read the admin bearer token from a 0600-mode file.",
)
@click.option(
    "--lease-owner",
    "lease_owner",
    type=str,
    default=None,
    hidden=True,
)
@click.option(
    "--release-legacy-only",
    "release_legacy_only",
    is_flag=True,
    default=False,
    hidden=True,
)
@click.option(
    "--expected-legacy-reason",
    "expected_legacy_reason",
    type=str,
    default=None,
    hidden=True,
)
@click.option(
    "--expected-legacy-set-at",
    "expected_legacy_set_at",
    type=str,
    default=None,
    hidden=True,
)
@click.option(
    "--expected-full-reason",
    "expected_full_reason",
    type=str,
    default=None,
    hidden=True,
)
@click.option(
    "--expected-full-set-at",
    "expected_full_set_at",
    type=str,
    default=None,
    hidden=True,
)
@click.option(
    "--read-only-snapshot",
    "read_only_snapshot",
    is_flag=True,
    default=False,
    hidden=True,
)
@click.option(
    "--all",
    "all_hosts",
    is_flag=True,
    default=False,
    help="v0.8.5 *Knuth's Concrete*: apply the operation to EVERY "
    "host in ~/.config/vq/config.toml in parallel. The same "
    "verb runs against each host (set / release / status / "
    "partial-cap). Per-host failures land inline as one-line "
    "FAIL banners so one bad host doesn't hide the rest. "
    "Mutually exclusive with positional HOST. Useful for "
    "fleet-wide maintenance windows ('vq drain --all --reason "
    "datacenter cooling').",
)
@click.argument("host", required=False)
def drain_cmd(
    max_jobs: int | None,
    max_cpus: int | None,
    release: bool,
    release_full: bool,
    status_only: bool,
    as_json: bool,
    reason: str | None,
    duration: str | None,
    update_mode: str | None,
    scheduler_hosts: tuple[str, ...],
    cli_token: str | None,
    token_stdin: bool,
    token_file: str | None,
    lease_owner: str | None,
    release_legacy_only: bool,
    expected_legacy_reason: str | None,
    expected_legacy_set_at: str | None,
    expected_full_reason: str | None,
    expected_full_set_at: str | None,
    read_only_snapshot: bool,
    all_hosts: bool,
    host: str | None,
) -> None:
    """Daemon-level dispatch gate: stop or limit new job dispatches.

    \b
    Forms:
      vq drain                   (full drain: no new dispatches)
      vq drain --max-jobs N      (partial: lower concurrent-jobs cap)
      vq drain --max-cpus N      (partial: lower CPU budget)
      vq drain --release         (clear drain; return to daemon caps)
      vq drain --release-full    (clear full drain, keep scheduler lanes)
      vq drain --status          (show current drain state, read-only)
      vq drain --status --json   (machine-readable drain state)
      vq drain --update-mode accept  (pause for update, accept jobs)
      vq drain --update-mode deny    (pause for update, reject jobs)
      vq drain --scheduler-host HOST (hold only that scheduler target)
      vq drain HOST [...]        (any of the above against HOST)

    Drain is daemon-level state in <state_root>/drain.json — survives
    daemon restarts and host reboots. It only affects new dispatches;
    already-running jobs continue. To pause running work too, use
    `vq pause --all` or `vq throttle --all --weight 20`.

    Use cases:
    * Pre-maintenance window: drain so no new job spawns, running ones
      finish naturally, then run your maintenance.
    * "I'm using the box for something else for the next hour."
    * Partial drain via --max-cpus N: let small --cpus 1 jobs through,
      block big --cpus 14 ones until released.

    Flag precedence: --status > --release > --max-jobs/--max-cpus > no-flags.

    v0.8.5 *Knuth's Concrete*: ``--all`` walks every configured
    host in parallel (uses the v0.7.6 fan-out). Per-host failures
    surface inline so partial-fleet outcomes are visible. Most
    useful for ``vq drain --all --reason 'maintenance'`` and the
    matching ``vq drain --all --release``.
    """
    cfg = config.load_config()
    scheduler_hosts = tuple(dict.fromkeys(h.strip() for h in scheduler_hosts if h.strip()))
    if release and release_full:
        raise click.UsageError("--release and --release-full are mutually exclusive")
    if as_json and not status_only:
        raise click.UsageError("--json is only supported with --status")
    if read_only_snapshot and (not status_only or not as_json):
        raise click.UsageError(
            "--read-only-snapshot requires --status and --json"
        )
    if read_only_snapshot and (
        all_hosts
        or release
        or release_full
        or max_jobs is not None
        or max_cpus is not None
        or reason is not None
        or duration is not None
        or update_mode is not None
        or scheduler_hosts
        or lease_owner is not None
    ):
        raise click.UsageError(
            "--read-only-snapshot is a single-control read and cannot be "
            "combined with drain mutations or --all"
        )
    if update_mode is not None and (max_jobs is not None or max_cpus is not None):
        raise click.UsageError(
            "--update-mode is a full maintenance drain; do not combine it "
            "with --max-jobs or --max-cpus",
        )
    if scheduler_hosts and (max_jobs is not None or max_cpus is not None):
        raise click.UsageError(
            "--scheduler-host is a scheduler-target drain; do not combine it "
            "with --max-jobs or --max-cpus",
        )
    if scheduler_hosts and update_mode is not None:
        raise click.UsageError(
            "--scheduler-host and --update-mode describe different drain "
            "policies; use one at a time",
        )
    if scheduler_hosts and all_hosts:
        raise click.UsageError(
            "--scheduler-host applies to this daemon's scheduler-target lanes; "
            "do not combine it with --all",
        )
    if lease_owner is not None and not scheduler_hosts:
        raise click.UsageError("--lease-owner requires --scheduler-host")
    if release_legacy_only and (not release or not scheduler_hosts):
        raise click.UsageError(
            "--release-legacy-only requires --release and --scheduler-host"
        )
    if release_legacy_only and expected_legacy_set_at is None:
        raise click.UsageError(
            "--release-legacy-only requires --expected-legacy-set-at"
        )
    if (expected_full_reason is None) != (expected_full_set_at is None):
        raise click.UsageError(
            "--expected-full-reason and --expected-full-set-at must be "
            "provided together"
        )
    if expected_full_reason is not None and not release_full:
        raise click.UsageError(
            "--expected-full-reason/--expected-full-set-at require "
            "--release-full"
        )
    if (
        expected_full_reason is not None
        and (
            not expected_full_reason.strip()
            or not (expected_full_set_at or "").strip()
        )
    ):
        raise click.UsageError(
            "--expected-full-reason and --expected-full-set-at must be "
            "non-empty"
        )
    if release_full and all_hosts:
        raise click.UsageError(
            "--release-full applies to this daemon's drain state; do not "
            "combine it with --all",
        )

    # v0.8.5: --all and HOST are mutually exclusive — naming one in
    # addition is contradictory.
    if all_hosts and host is not None:
        raise click.UsageError(
            "--all and HOST are mutually exclusive; --all walks "
            "every configured host, so naming one in addition is "
            "contradictory",
        )

    # Read-only drain status deliberately stays open.  Every mutation resolves
    # the same credential inputs as the other admin-write commands, including
    # system multi-user mode discovered outside the per-user config object.
    # Pin the daemon/state-tree scope once for the whole command. A config edit
    # between a multi-host acquire and its rollback must not move cleanup onto
    # the other daemon tree and strand a non-expiring claim.
    drain_scope = _multi_user_active(cfg)
    resolved_admin_token: str | None = None
    if not status_only:
        resolved_admin_token = _resolve_admin_token(
            cfg,
            command_label="drain",
            cli_token=cli_token,
            token_stdin=token_stdin,
            token_file=token_file,
            multi_user=drain_scope,
        )
    drain_auth_kwargs: dict[str, object] = {"multi_user": drain_scope}
    if resolved_admin_token is not None:
        drain_auth_kwargs["token"] = resolved_admin_token

    def _drain_args(*, target_localhost: bool) -> list[str]:
        args: list[str] = ["drain"]
        if status_only:
            args.append("--status")
            if as_json:
                args.append("--json")
            if read_only_snapshot:
                args.append("--read-only-snapshot")
        elif release_full:
            args.append("--release-full")
            if expected_full_reason is not None:
                args.extend(
                    ["--expected-full-reason", expected_full_reason]
                )
                args.extend(
                    ["--expected-full-set-at", expected_full_set_at or ""]
                )
            for scheduler_host in scheduler_hosts:
                args.extend(["--scheduler-host", scheduler_host])
            if lease_owner is not None:
                args.extend(["--lease-owner", lease_owner])
        elif release:
            args.append("--release")
            for scheduler_host in scheduler_hosts:
                args.extend(["--scheduler-host", scheduler_host])
            if lease_owner is not None:
                args.extend(["--lease-owner", lease_owner])
            if release_legacy_only:
                args.append("--release-legacy-only")
                args.extend(
                    ["--expected-legacy-set-at", expected_legacy_set_at or ""]
                )
                if expected_legacy_reason is not None:
                    args.extend(
                        ["--expected-legacy-reason", expected_legacy_reason]
                    )
        else:
            if max_jobs is not None:
                args.extend(["--max-jobs", str(max_jobs)])
            if max_cpus is not None:
                args.extend(["--max-cpus", str(max_cpus)])
            if reason is not None:
                args.extend(["--reason", reason])
            if duration is not None:
                args.extend(["--duration", duration])
            if update_mode is not None:
                args.extend(["--update-mode", update_mode])
            for scheduler_host in scheduler_hosts:
                args.extend(["--scheduler-host", scheduler_host])
            if lease_owner is not None:
                args.extend(["--lease-owner", lease_owner])
        if target_localhost:
            args.append("localhost")
        return args

    def _delegate_drain(target_host: str, *, target_localhost: bool) -> str:
        args = _drain_args(target_localhost=False)
        remote_stdin: str | None = None
        if not status_only:
            auth_args, remote_stdin = _remote_admin_auth(
                target_host,
                cfg,
                resolved_admin_token,
            )
            args.extend(auth_args)
        if target_localhost:
            args.append("localhost")
        return _delegate_to_remote(
            target_host,
            cfg,
            *args,
            stdin_data=remote_stdin,
        )

    def _acquire_scheduler_leases() -> None:
        # Allocate every identity before the first write.  If a later RPC
        # fails after committing but before returning its response, the
        # attempted ID lets us safely remove that exact claim as well as each
        # earlier claim that this invocation knows it created.
        lease_plan = [
            (scheduler_host, secrets.token_hex(16))
            for scheduler_host in scheduler_hosts
        ]
        changed_claims: list[tuple[str, str]] = []
        for scheduler_host, lease_id in lease_plan:
            try:
                _lease, changed = drain_module.acquire_scheduler_drain_lease(
                    scheduler_host,
                    owner=lease_owner or "operator",
                    reason=reason,
                    lease_id=lease_id,
                    **drain_auth_kwargs,
                )
            except BaseException as exc:
                rollback_failures: list[str] = []
                rollback_plan = [
                    (scheduler_host, lease_id),
                    *reversed(changed_claims),
                ]
                for rollback_host, rollback_id in rollback_plan:
                    try:
                        drain_module.release_scheduler_drain_lease(
                            rollback_id,
                            **drain_auth_kwargs,
                        )
                    except Exception as rollback_exc:
                        rollback_failures.append(
                            f"{rollback_host} ({rollback_id}): {rollback_exc}"
                        )
                rollback_detail = ""
                if rollback_failures:
                    rollback_detail = (
                        "; exact rollback could not be confirmed for "
                        + ", ".join(rollback_failures)
                    )
                if isinstance(exc, drain_module.SchedulerDrainLeaseError):
                    raise click.ClickException(
                        "scheduler drain acquisition failed for "
                        f"{scheduler_host}: {exc}{rollback_detail}"
                    ) from exc
                if rollback_detail and hasattr(exc, "add_note"):
                    exc.add_note(rollback_detail.lstrip("; "))
                raise
            if changed:
                changed_claims.append((scheduler_host, lease_id))

    def _local_drain_action_impl() -> str:
        if status_only:
            if as_json:
                if read_only_snapshot:
                    try:
                        payload = drain_module.read_only_status_payload(
                            multi_user=drain_scope,
                        )
                    except drain_module.DrainSnapshotError as exc:
                        raise click.ClickException(str(exc)) from exc
                    return json.dumps(
                        payload,
                        indent=2,
                        sort_keys=True,
                        default=str,
                    )
                return json.dumps(
                    drain_module.status_payload(),
                    indent=2,
                    sort_keys=True,
                    default=str,
                )
            return drain_module.format_status()
        if release_full:
            if scheduler_hosts:
                _acquire_scheduler_leases()
            try:
                if expected_full_reason is None:
                    released = drain_module.release_full_drain(
                        **drain_auth_kwargs
                    )
                else:
                    released = drain_module.release_owned_full_drain(
                        expected_reason=expected_full_reason,
                        expected_set_at=expected_full_set_at or "",
                        **drain_auth_kwargs,
                    )
            except drain_module.OwnedFullDrainReleaseError as exc:
                raise click.ClickException(str(exc)) from exc
            return (
                "full drain released; scheduler-target lanes retained"
                if released
                else "full drain was not set (no-op)"
            )
        if release:
            if scheduler_hosts:
                released: list[str] = []
                missing: list[str] = []
                for scheduler_host in scheduler_hosts:
                    try:
                        if release_legacy_only:
                            did_release = (
                                drain_module.release_legacy_scheduler_host(
                                    scheduler_host,
                                    expected_reason=expected_legacy_reason,
                                    expected_set_at=expected_legacy_set_at,
                                    **drain_auth_kwargs,
                                )
                            )
                        elif lease_owner is None:
                            did_release = drain_module.release_scheduler_host(
                                scheduler_host,
                                **drain_auth_kwargs,
                            )
                        else:
                            did_release = (
                                drain_module.release_scheduler_drain_leases(
                                    scheduler_host,
                                    owner=lease_owner,
                                    **drain_auth_kwargs,
                                )
                            )
                    except drain_module.SchedulerDrainLeaseError as exc:
                        partial = ""
                        if released:
                            partial = (
                                "; earlier scheduler drains were already "
                                f"released for {', '.join(released)}"
                            )
                        raise click.ClickException(
                            f"scheduler drain release failed for "
                            f"{scheduler_host}: {exc}{partial}"
                        ) from exc
                    if did_release:
                        released.append(scheduler_host)
                    else:
                        missing.append(scheduler_host)
                parts: list[str] = []
                if released:
                    parts.append(
                        "scheduler drain released for "
                        f"{', '.join(released)}"
                    )
                if missing:
                    # A scheduler-target lane is held on that host's DRIVER,
                    # not on the scheduler host itself, so a bare
                    # `--scheduler-host X --release` can resolve against X's
                    # own (inactive) drain state and report "not set" while the
                    # driver-side lane stays held. The operator reading this is
                    # mid-incident -- a leaked lane leaves the target
                    # accept_pending, taking submissions and dispatching
                    # nothing -- so name the command that actually clears it
                    # rather than letting them conclude there is nothing to
                    # clear. pbs-cluster, 2026-07-26.
                    detail: list[str] = []
                    for scheduler_host in missing:
                        driver = None
                        with contextlib.suppress(Exception):
                            driver = _scheduler_driver_host(cfg, scheduler_host)
                        if driver is None:
                            detail.append(scheduler_host)
                            continue
                        detail.append(
                            f"{scheduler_host} (a scheduler-target lane for "
                            f"{scheduler_host} is held on its driver {driver!r}, "
                            f"not on {scheduler_host} itself; if dispatch is "
                            f"stalled try `vq drain {driver} --scheduler-host "
                            f"{scheduler_host} --release`)"
                        )
                    parts.append(
                        "scheduler drain was not set for " + ", ".join(detail)
                    )
                return "; ".join(parts) if parts else "scheduler drain was not set"
            leases_removed = False
            try:
                leases_removed = drain_module.clear_scheduler_drain_leases(
                    **drain_auth_kwargs
                )
            except drain_module.SchedulerDrainCapabilityError as exc:
                try:
                    local_leases = drain_module.read_scheduler_drain_leases(
                        via_rpc=False,
                        multi_user=drain_scope,
                    )
                except drain_module.SchedulerDrainLeaseError as probe_exc:
                    raise click.ClickException(str(probe_exc)) from probe_exc
                if local_leases:
                    raise click.ClickException(str(exc)) from exc
            except drain_module.SchedulerDrainLeaseError as exc:
                raise click.ClickException(str(exc)) from exc
            try:
                removed = drain_module.clear_drain(
                    **drain_auth_kwargs
                )
            except drain_module.SchedulerDrainLeaseError as exc:
                raise click.ClickException(
                    "scheduler lease release completed, but legacy drain "
                    f"release failed: {exc}"
                ) from exc
            return (
                "drain released; daemon back to configured caps"
                if removed or leases_removed
                else "drain was not set (no-op)"
            )
        duration_seconds: int | None = None
        if duration is not None:
            try:
                duration_seconds = int(parse_age(duration).total_seconds())
            except ValueError as e:
                raise click.UsageError(f"--duration: {e}") from None
        existing = drain_module.read_drain_state(multi_user=drain_scope)
        if scheduler_hosts:
            _acquire_scheduler_leases()
            state = drain_module.read_effective_drain_state(
                multi_user=drain_scope
            )
            assert state is not None
            if state.is_full_drain:
                tail = (
                    " (full drain + scheduler-target drain; held: "
                    f"{', '.join(scheduler_hosts)})"
                )
            else:
                tail = (
                    " (scheduler-target drain; held: "
                    f"{', '.join(scheduler_hosts)})"
                )
            suffix_parts = []
            if duration_seconds is not None:
                suffix_parts.append(
                    "scheduler leases do not expire; --duration applies only "
                    "to a separate global hold"
                )
            if reason:
                suffix_parts.append(reason)
            suffix = f" — {'; '.join(suffix_parts)}" if suffix_parts else ""
            return f"drain set{tail}{suffix}"
        preserved_scheduler_hosts = (
            list(existing.scheduler_hosts)
            if existing is not None and existing.enabled
            else []
        )
        merged_scheduler_hosts = list(
            dict.fromkeys([*preserved_scheduler_hosts, *scheduler_hosts])
        )
        if scheduler_hosts and existing is not None:
            state = existing
            if existing.is_full_drain:
                state.full_dispatch = True
            state.scheduler_hosts = merged_scheduler_hosts
            if reason is not None:
                state.reason = reason
            if duration_seconds is not None:
                state.duration_seconds = duration_seconds
        else:
            state = drain_module.DrainState(
                enabled=True,
                max_jobs=max_jobs,
                max_cpus=max_cpus,
                reason=reason,
                duration_seconds=duration_seconds,
                reject_submits=(update_mode == "deny"),
                update_mode=update_mode,
                scheduler_hosts=merged_scheduler_hosts,
                full_dispatch=(
                    max_jobs is None and max_cpus is None and not scheduler_hosts
                ),
            )
        drain_module.write_drain_state(state, **drain_auth_kwargs)
        if state.is_full_drain and state.scheduler_hosts:
            tail = (
                " (full drain + scheduler-target drain; held: "
                f"{', '.join(state.scheduler_hosts)})"
            )
        elif scheduler_hosts:
            tail = (
                " (scheduler-target drain; held: "
                f"{', '.join(scheduler_hosts)})"
            )
        elif update_mode == "deny":
            tail = " (paused for update; denying new submissions)"
        elif update_mode == "accept":
            tail = " (paused for update; accepting jobs for later)"
        elif state.is_full_drain:
            tail = " (full drain; no new dispatches)"
        else:
            cap_parts = []
            if max_jobs is not None:
                cap_parts.append(f"max_jobs={max_jobs}")
            if max_cpus is not None:
                cap_parts.append(f"max_cpus={max_cpus}")
            tail = f" (partial: {', '.join(cap_parts)})"
        suffix_parts: list[str] = []
        if duration_seconds is not None:
            suffix_parts.append(f"auto-release in {duration_seconds}s")
        if reason:
            suffix_parts.append(reason)
        suffix = f" — {'; '.join(suffix_parts)}" if suffix_parts else ""
        return f"drain set{tail}{suffix}"

    def _local_drain_action() -> str:
        return _local_drain_action_impl()

    def _scheduler_drain_notice(h: str) -> str | None:
        return _scheduler_host_notice(
            cfg,
            h,
            "driver-level drain state; affects all dispatch owned by driver",
        )

    def _scheduler_drain_action(h: str) -> str | None:
        driver = _scheduler_driver_host(cfg, h)
        if driver is None:
            return None
        notice = _scheduler_drain_notice(h)
        assert notice is not None
        if is_local_host(driver):
            body = _local_drain_action()
        else:
            body = _delegate_drain(driver, target_localhost=True).rstrip()
        if as_json:
            try:
                payload = json.loads(body)
            except json.JSONDecodeError:
                payload = {"error": "driver returned invalid drain JSON", "raw": body}
            if isinstance(payload, dict):
                payload.setdefault("requested_host", h)
                payload.setdefault("driver_host", driver)
                payload.setdefault("scheduler_host", True)
            return json.dumps(payload, indent=2, sort_keys=True, default=str)
        return f"{notice}\n{body}"

    if all_hosts:
        if not cfg.hosts:
            click.echo("(no [hosts.X] configured)")
            return
        # Target each remote's OWN daemon explicitly. Without a host
        # token the delegated `vq drain ...` resolves through the
        # remote's default_host, so a host that has default_host set
        # would mis-route the command to that other host instead of
        # draining itself. "localhost" forces the is_local_host
        # short-circuit on each remote. (The local host is handled
        # in-process below, before per_host_args is used, so this only
        # affects remote delegations.)
        def _drain_one(h: str) -> str:
            scheduler_action = _scheduler_drain_action(h)
            if scheduler_action is not None:
                return scheduler_action
            if is_local_host(h):
                # Local — call drain primitives directly so the
                # in-process state writes don't round-trip through
                # `vq` subprocess.
                return _local_drain_action()
            return _delegate_drain(h, target_localhost=True).rstrip()

        if as_json:
            click.echo(
                json.dumps(
                    _aggregate_per_host_json(cfg, _drain_one),
                    indent=2,
                    sort_keys=True,
                    default=str,
                )
            )
        else:
            click.echo(_aggregate_per_host(cfg, _drain_one))
        return

    host = host if host is not None else _resolve_host(cfg, None)

    scheduler_action = _scheduler_drain_action(host)
    if scheduler_action is not None:
        click.echo(scheduler_action)
    elif is_local_host(host):
        click.echo(_local_drain_action())
    else:
        # Mutate/query the named host's own daemon. A remote config may point
        # default_host elsewhere; omitting this positional target would make
        # an explicit `vq drain REMOTE` hop again from REMOTE.
        click.echo(_delegate_drain(host, target_localhost=True), nl=False)


@main.command()
@click.option(
    "--reason",
    "reason",
    metavar="TEXT",
    help=(
        "Human-readable explanation recorded on the killed job and shown by "
        "`vq status`/`vq wait` JSON. Example: obsolete vibe-qc version; "
        "resubmit after fleet update."
    ),
)
@click.option(
    "--resubmit",
    "--restart-after-update",
    "restart_after_update",
    is_flag=True,
    default=False,
    help=(
        "After recording the kill, immediately create a fresh pending "
        "resubmission from the killed job. Intended for fleet upgrades: "
        "kill obsolete-runtime work with --reason, then restart it on the "
        "updated runtime."
    ),
)
@click.argument("host_or_jobid")
@click.argument("jobid_if_host", required=False)
def kill(
    reason: str | None,
    restart_after_update: bool,
    host_or_jobid: str,
    jobid_if_host: str | None,
) -> None:
    """Cancel JOBID on HOST. Pending jobs are marked killed; running jobs receive SIGTERM.

    \b
    Forms:
      vq kill [--reason TEXT] [--resubmit] HOST JOBID
      vq kill [--reason TEXT] [--resubmit] JOBID  (uses default_host)
    """
    cfg = config.load_config()
    if jobid_if_host is None:
        jobid = host_or_jobid
        host = _resolve_job_host(cfg, None, jobid)
    else:
        host = host_or_jobid
        jobid = jobid_if_host

    scheduler_driver = _scheduler_driver_host(cfg, host)
    if scheduler_driver is not None:
        host = scheduler_driver

    if is_local_host(host):
        try:
            msg = kill_job(
                host,
                jobid,
                multi_user=_multi_user_active(cfg),
                reason=reason,
            )
            if restart_after_update:
                new_id = resubmit_local(
                    jobid,
                    multi_user=_multi_user_active(cfg),
                )
                msg = f"{msg}\nresubmitted {jobid} -> {new_id}"
        except FileNotFoundError as e:
            raise click.UsageError(str(e)) from None
        except (FileExistsError, ValueError) as e:
            raise click.UsageError(str(e)) from None
        except NotImplementedError as e:
            raise click.UsageError(str(e)) from None
        except paths.SpecLockTimeout as e:
            # v0.12.0: a wedged lock holder would otherwise hang the CLI.
            raise click.ClickException(str(e)) from None
        click.echo(msg)
    else:
        remote_args = ["kill"]
        if reason:
            remote_args.extend(["--reason", reason])
        if restart_after_update:
            remote_args.append("--resubmit")
        remote_args.extend(["localhost", jobid])
        click.echo(_delegate_to_remote(host, cfg, *remote_args), nl=False)


@main.command(help="Block until a job reaches a terminal state, with optional timeout.")
@click.argument("host_or_jobid")
@click.argument("jobid_if_host", required=False)
@click.option(
    "--poll-interval",
    "poll_interval",
    default=DEFAULT_POLL_INTERVAL_SECONDS,
    type=click.FloatRange(min=0.1),
    callback=_finite_positive_seconds,
    show_default=True,
    metavar="SECONDS",
    help="How often to re-read the spec (or re-query remote status). "
    "Must be finite; default 5s matches the watchdog sampling rhythm.",
)
@click.option(
    "--timeout",
    "wait_timeout",
    default=None,
    type=click.FloatRange(min=0.1),
    callback=_finite_positive_seconds,
    metavar="SECONDS",
    help="Finite max wait. If the job hasn't reached a terminal state within "
    "this many seconds, exit 124 (matches GNU `timeout`) and "
    "leave the job running. The timeout message includes the last "
    "pause/scheduler/walltime detail seen when available. Default: wait forever.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    help="Emit a machine-readable terminal outcome on stdout.",
)
def wait(
    host_or_jobid: str,
    jobid_if_host: str | None,
    poll_interval: float,
    wait_timeout: float | None,
    as_json: bool,
) -> None:
    """Block until JOBID reaches a terminal state.

    \b
    Forms:
      vq wait HOST JOBID
      vq wait JOBID                       (uses default_host)
      vq wait JOBID --timeout 3600        (1-hour cap)
      vq wait JOBID --poll-interval 1     (tight loop for tests)
      vq wait JOBID --json                (machine-readable outcome)

    Exit code maps to the job's outcome:

      * 0 — job COMPLETED (the spec's exit_code, normally 0).
      * non-zero — any other terminal state (FAILED, KILLED,
        OOM_KILLED, STARVED, TIME_EXCEEDED, ABORTED_BY_QUEUE,
        INTERRUPTED). For FAILED, the spec's actual exit_code
        is propagated when known; otherwise 1.
      * 124 — --timeout elapsed before the job became terminal
        (matches GNU coreutils `timeout(1)`). The job keeps
        running; only the wait is canceled. When available, the
        message includes last-seen pause/scheduler/walltime detail.
      * 130 — SIGINT (operator Ctrl-C). Same: the job keeps
        running.

    The wait loop polls; it does NOT push the wait into the
    daemon. Ctrl-C cancels the WAIT only — `vq kill JOBID`
    if you want to abort the job.

    Composable with the rest of the queue:

      JID=$(vq submit foo.py)
      vq wait $JID && vq fetch $JID -o ./out
    """
    cfg = config.load_config()
    if jobid_if_host is None:
        jobid = host_or_jobid
        host = _resolve_job_host(cfg, None, jobid)
    else:
        host = host_or_jobid
        jobid = jobid_if_host

    scheduler_driver = _scheduler_driver_host(cfg, host)
    if scheduler_driver is not None:
        host = scheduler_driver

    host_cfg = None
    if not is_local_host(host):
        try:
            host_cfg = cfg.host(host)
        except config.ConfigError as e:
            raise click.UsageError(str(e)) from None

    try:
        result = wait_for_terminal(
            host,
            jobid,
            host_cfg=host_cfg,
            poll_interval=poll_interval,
            timeout=wait_timeout,
            multi_user=_multi_user_active(cfg),
        )
    except (FileNotFoundError, ValueError) as e:
        raise click.UsageError(str(e)) from None
    except WaitTimeout as e:
        if as_json:
            click.echo(
                json.dumps(
                    {
                        "jobid": e.jobid,
                        "state": (
                            e.last_state.value
                            if e.last_state is not None
                            else None
                        ),
                        "timed_out": True,
                        "detail": e.detail,
                        "queue_handle": e.queue_handle,
                        "cli_exit_code": 124,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            raise SystemExit(124) from None
        click.echo(str(e), err=True)
        raise SystemExit(124) from None
    except KeyboardInterrupt:
        if as_json:
            click.echo(
                json.dumps(
                    {
                        "jobid": jobid,
                        "state": None,
                        "interrupted": True,
                        "message": "wait interrupted; job continues to run",
                        "cli_exit_code": 130,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            raise SystemExit(130) from None
        click.echo(f"wait for {jobid} interrupted; job continues to run", err=True)
        raise SystemExit(130) from None
    except transport.RemoteError as e:
        raise click.ClickException(str(e)) from None

    if as_json:
        click.echo(json.dumps(result.to_json_payload(), indent=2, sort_keys=True))
        raise SystemExit(result.cli_exit_code)

    # One-line summary on stderr so `vq wait JID && ...` chains
    # see no extraneous stdout.
    click.echo(
        f"{jobid}: {result.state.value}" + _exit_suffix(result.exit_code),
        err=True,
    )
    raise SystemExit(result.cli_exit_code)


@main.command()
@click.argument("host_or_jobid", required=False)
@click.argument("jobid_if_host", required=False)
@click.option(
    "--state",
    "states",
    multiple=True,
    type=click.Choice(
        [s.value for s in JobState if s.value not in {"running", "pending", "suspended"}],
        case_sensitive=False,
    ),
    metavar="STATE",
    help="v0.6.10: bulk mode. Resubmit every job whose state matches. "
    "Repeatable. Only terminal states accepted (refuses running/"
    "pending/suspended). When --state is given, the positional "
    "JOBID is rejected; the optional HOST positional still works "
    "(`vq resubmit workstation --state aborted_by_queue`).",
)
@click.option(
    "--cpus",
    default=None,
    type=click.IntRange(min=1),
    metavar="N",
    help="Override --cpus on the new job; inherits from source if omitted.",
)
@click.option(
    "--scheduler-tasks",
    "--ntasks",
    "scheduler_tasks",
    default=None,
    type=click.IntRange(min=1),
    metavar="N",
    help="Override scheduler task/rank count on the new job; inherits if omitted.",
)
@click.option(
    "--mem-mb",
    "mem_mb",
    default=None,
    type=click.IntRange(min=1),
    metavar="N",
    help="Override --mem-mb on the new job; inherits from source if omitted.",
)
@click.option(
    "--wall-time-seconds",
    "wall_time_seconds",
    default=None,
    type=click.IntRange(min=1),
    metavar="SECONDS",
    help="Override --wall-time-seconds on the new job; inherits if omitted.",
)
@click.option(
    "--priority",
    default=None,
    type=int,
    metavar="N",
    help="Override --priority on the new job; inherits if omitted.",
)
@click.option(
    "--retry",
    "retry_max",
    default=None,
    type=click.IntRange(min=0),
    metavar="N",
    help="Override --retry budget on the new job; inherits if omitted. "
    "retry_count always resets to 0 on the new job (fresh budget).",
)
@click.option(
    "--job-name",
    "job_name",
    default=None,
    type=str,
    metavar="NAME",
    help="Override --job-name on the new job; inherits if omitted. The CLI "
    "sanitizes friendlier names and warns when it changes them.",
)
@click.option(
    "--tag",
    "tags",
    multiple=True,
    type=str,
    metavar="TAG",
    help="Replace tags on the new job (repeatable). Mutually exclusive "
    "with --clear-tags. Omit both to inherit the source's tags.",
)
@click.option(
    "--clear-tags",
    "clear_tags",
    is_flag=True,
    default=False,
    help="Clear tags on the new job. Mutually exclusive with --tag.",
)
def resubmit(
    host_or_jobid: str | None,
    jobid_if_host: str | None,
    states: tuple[str, ...],
    cpus: int | None,
    scheduler_tasks: int | None,
    mem_mb: int | None,
    wall_time_seconds: int | None,
    priority: int | None,
    retry_max: int | None,
    job_name: str | None,
    tags: tuple[str, ...],
    clear_tags: bool,
) -> None:
    """Rerun terminal-state jobs in a fresh workspace.

    \b
    Single-job mode:
      vq resubmit HOST JOBID
      vq resubmit JOBID                          (uses default_host)

    \b
    Bulk mode (v0.6.10):
      vq resubmit --state STATE [--state STATE2 ...]    (default_host)
      vq resubmit HOST --state STATE
      vq resubmit --state aborted_by_queue              (post-reboot recovery)

    Reads each source job's spec, copies its workspace into a new
    jobs_dir entry (or extracts the archive tarball when archived),
    writes a new spec with a fresh jobid + parent_jobid pointing at
    the source, and dispatches the new job through the normal pending
    queue. Source specs are unchanged.

    Bulk mode prints one new jobid per line on stdout (scriptable —
    e.g. `vq resubmit --state aborted_by_queue | xargs -I{} vq tail
    {} -f`); a summary line goes to stderr. Failures on individual
    source jobs (workspace gone, spec corrupt) are reported on
    stderr but don't abort the rest of the batch.

    Refuses non-terminal source states (RUNNING / PENDING / SUSPENDED)
    in both modes — use `vq kill` first if you need to abort and
    resubmit a still-running job.

    Per-flag overrides (--cpus, --scheduler-tasks, --mem-mb,
    --wall-time-seconds,
    --priority, --retry, --job-name, --tag) replace the inherited
    value on each new spec; the source spec keeps its original
    fields. --clear-tags wipes inherited tags. retry_count always
    resets to 0 on the new job (fresh budget); recover_on_reboot and
    branch carry over from the source.
    """
    if clear_tags and tags:
        raise click.UsageError(
            "--tag and --clear-tags are mutually exclusive "
            "(--tag replaces with the given set; --clear-tags wipes)"
        )
    job_name = _normalize_job_name_option(job_name)

    cfg = config.load_config()

    # Tag override semantics: same in both single + bulk modes.
    tag_override: list[str] | None
    if clear_tags:
        tag_override = []
    elif tags:
        tag_override = list(tags)
    else:
        tag_override = None

    overrides = ResubmitOverrides(
        cpus=cpus,
        scheduler_tasks=scheduler_tasks,
        mem_mb=mem_mb,
        wall_time_seconds=wall_time_seconds,
        priority=priority,
        retry_max=retry_max,
        tags=tag_override,
        job_name=job_name,
    )

    if states:
        # Bulk mode. HOST is the first positional (if present);
        # JOBID positional is rejected.
        if jobid_if_host is not None:
            raise click.UsageError(
                "--state is bulk mode; a positional JOBID is not "
                "accepted. Either use single-job mode "
                "(`vq resubmit HOST JOBID`) or bulk mode "
                "(`vq resubmit HOST --state STATE`), not both."
            )
        if host_or_jobid is not None:
            # Could be a hostname or stray jobid; only accept hostnames
            # so a typo doesn't silently get treated as a host.
            if host_or_jobid not in cfg.hosts and not is_local_host(host_or_jobid):
                raise click.UsageError(
                    f"unknown host {host_or_jobid!r}; either drop the "
                    f"positional to use default_host, or pass a host "
                    f"that's in {config.config_path()}"
                )
            host = host_or_jobid
        else:
            host = _resolve_host(cfg, None)

        scheduler_target = host if _scheduler_driver_host(cfg, host) is not None else None
        if scheduler_target is not None:
            driver = _scheduler_driver_host(cfg, host)
            assert driver is not None
            host = driver

        state_enums = [JobState(s.lower()) for s in states]

        try:
            if is_local_host(host):
                result = resubmit_state(
                    state_enums,
                    overrides=overrides,
                    multi_user=_multi_user_active(cfg),
                    scheduler_target=scheduler_target,
                )
                # stdout: one new jobid per line (scriptable)
                for _src, new_id in result.pairs:
                    click.echo(new_id)
                # stderr: per-source mapping + summary
                for src, new_id in result.pairs:
                    click.echo(f"  {src} -> {new_id}", err=True)
                for src, errmsg in result.errors:
                    click.echo(f"  {src} FAILED: {errmsg}", err=True)
                summary = (
                    f"resubmitted {len(result.pairs)} job"
                    f"{'s' if len(result.pairs) != 1 else ''}"
                    f" ({len(result.errors)} error"
                    f"{'s' if len(result.errors) != 1 else ''})"
                )
                click.echo(summary, err=True)
                if result.errors and not result.pairs:
                    # All-fail batch is a non-zero exit so scripts notice.
                    raise click.ClickException(summary)
            else:
                try:
                    host_cfg = cfg.host(host)
                except config.ConfigError as e:
                    raise click.UsageError(str(e)) from None
                new_ids, remote_stderr = resubmit_state_remote(
                    host_cfg,
                    state_enums,
                    overrides=overrides,
                    target_host=scheduler_target or "localhost",
                )
                for new_id in new_ids:
                    click.echo(new_id)
                # Forward the remote's stderr verbatim (it already
                # carries the per-source mapping + summary).
                if remote_stderr:
                    click.echo(remote_stderr, nl=False, err=True)
        except transport.RemoteError as e:
            raise click.ClickException(str(e)) from None
        return

    # Single-job mode (existing behavior).
    if host_or_jobid is None:
        raise click.UsageError("missing JOBID (or pass --state STATE for bulk mode)")
    if jobid_if_host is None:
        jobid = host_or_jobid
        host = _resolve_job_host(cfg, None, jobid)
    else:
        host = host_or_jobid
        jobid = jobid_if_host

    scheduler_target = host if _scheduler_driver_host(cfg, host) is not None else None
    if scheduler_target is not None:
        driver = _scheduler_driver_host(cfg, host)
        assert driver is not None
        host = driver

    try:
        if is_local_host(host):
            new_id = resubmit_local(
                jobid,
                overrides=overrides,
                multi_user=_multi_user_active(cfg),
            )
        else:
            try:
                host_cfg = cfg.host(host)
            except config.ConfigError as e:
                raise click.UsageError(str(e)) from None
            new_id = resubmit_remote(
                host_cfg,
                jobid,
                overrides=overrides,
                target_host=scheduler_target or "localhost",
            )
    except (FileNotFoundError, FileExistsError, ValueError) as e:
        raise click.UsageError(str(e)) from None
    except transport.RemoteError as e:
        raise click.ClickException(str(e)) from None
    click.echo(new_id)


@main.command()
@click.argument("host_or_jobid")
@click.argument("jobid_if_host", required=False)
@click.option(
    "-o",
    "--output",
    "output_dir",
    type=click.Path(file_okay=False, dir_okay=True, path_type=Path),
    default=Path("."),
    show_default=True,
    help="Parent directory; the workspace lands under "
    "<output_dir>/<jobname>-<jobid>/ when the job was submitted with "
    "`--job-name` (v0.5.34), else <output_dir>/<jobid>/. With "
    "--workdir, the workdir lands under "
    "<output_dir>/<jobname>-<jobid>-workdir/ (v0.7.7) so a "
    "workspace fetch and a workdir fetch of the same job can sit "
    "side-by-side without a name collision.",
)
@click.option(
    "--workdir",
    "fetch_workdir",
    is_flag=True,
    default=False,
    help="v0.7.7 *Cerf's Datagram*: fetch the per-job scratch "
    'workdir ("$VQ_WORKDIR", v0.6.54) instead of the workspace. '
    "Workdirs typically hold intermediate / large artefacts the "
    "job writes per the agent-protocol convention. Errors if the "
    "spec has no workdir (pre-v0.6.54) or the workdir was swept "
    "(`--clean-tmp` at submit + terminal state). Scheduler directory "
    "jobs usually have no separate workdir; fetch their workspace "
    "without this flag.",
)
@click.option(
    "--name",
    "artifact_name",
    default=None,
    metavar="BASENAME",
    help="Fetch exactly one workspace artifact into -o DIR, without queue "
    "logs, metadata, or diagnosis sidecars. The name must be one basename "
    "(a file such as job.qvf or a directory such as run.trexio). "
    "Combine with --workdir to select "
    "from the recorded scratch workdir instead of the workspace.",
)
@click.option(
    "--subdir", "artifact_subdir", default=None, metavar="RELATIVE_DIR",
    help="With --workdir --name, select the artifact from this workdir-relative "
    "directory (for example results). Rejects symlinks, absolute paths, . and .. "
    "components. The fetched file lands directly in -o DIR.",
)
@click.option(
    "--workspace",
    "explicit_workspace",
    is_flag=True,
    help="Explicitly fetch only the submitted workspace, even when a local "
    "daemon stores output artifacts in a separate --workdir. Mutually "
    "exclusive with --workdir and --name.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Emit a machine-readable fetch result with destination path and "
    "queue_handle instead of the human one-line summary.",
)
def fetch(
    host_or_jobid: str,
    jobid_if_host: str | None,
    output_dir: Path,
    fetch_workdir: bool,
    artifact_name: str | None,
    artifact_subdir: str | None,
    explicit_workspace: bool,
    as_json: bool,
) -> None:
    """Copy a job's workspace (or workdir) to a local directory.

    \b
    Forms:
      vq fetch HOST JOBID [-o DIR]
      vq fetch JOBID [-o DIR]                (uses default_host from config)
      vq fetch HOST JOBID --workdir [-o DIR] (v0.7.7: scratch workdir)
      vq fetch JOBID --workdir [-o DIR]      (v0.7.7)
      vq fetch HOST JOBID --name job.qvf [-o DIR]

    For a remote host the payload is streamed via ssh + tar; for a local
    host it's a plain directory copy. v0.5.34: the destination directory
    is <output_dir>/<jobname>-<jobid>/ when the job has a name (set at
    submit via `vq submit --job-name NAME`), else <output_dir>/<jobid>/.
    With ``--workdir`` (v0.7.7) the destination has ``-workdir``
    appended so a workspace fetch and a workdir fetch of the same job
    can coexist under one ``-o DIR``.

    Local daemon outputs may live in a separate workdir. An implicit
    workspace fetch then exits with a --workdir hint after copying the
    workspace. Use --workspace when only submitted inputs and queue logs
    are wanted. Older terminal local-job receipts without workdir metadata
    also require an explicit choice. Scheduler workspace fetches are unchanged.

    Re-fetching REFRESHES: a destination that already holds a previous
    fetch of this same job is replaced with the current snapshot, and
    the printed ``fetched_at`` advances. Every fetched tree carries
    ``_vq/fetch-manifest.json`` recording that timestamp plus a
    ``stale`` flag, so a poller can age-check the payload without
    trusting this command's exit code; a fetch that cannot refresh
    exits non-zero AND stamps the previous tree stale. An unrelated or
    different-job destination is still refused.
    """
    if artifact_subdir is not None and (not fetch_workdir or artifact_name is None):
        raise click.UsageError("--subdir requires --workdir and --name")
    if explicit_workspace and (fetch_workdir or artifact_name is not None):
        raise click.UsageError("--workspace, --workdir and --name are mutually exclusive")
    cfg = config.load_config()
    inferred_host = jobid_if_host is None
    if jobid_if_host is None:
        jobid = host_or_jobid
        host = _resolve_job_host(cfg, None, jobid)
    else:
        host = host_or_jobid
        jobid = jobid_if_host
    requested_queue_host = host
    scheduler_driver = _scheduler_driver_host(cfg, host)
    if scheduler_driver is not None:
        host = scheduler_driver

    def _queue_handle_for(resolved_host: str) -> dict[str, str | None]:
        handle = queue_handle_without_spec(jobid, requested_queue_host)
        if is_local_host(resolved_host):
            with contextlib.suppress(FileNotFoundError, ValueError, OSError):
                spec_path = (
                    paths.resolve_spec_path(jobid, multi_user=True)
                    if _multi_user_active(cfg)
                    else paths.spec_path(jobid)
                )
                spec = JobSpec.read(spec_path)
                handle["host"] = queue_host_for_scheduler_target(
                    spec.scheduler_target,
                    requested_queue_host,
                )
                handle["submitted_at"] = spec.submitted_at
        return handle

    def _fetch_from(resolved_host: str) -> tuple[Path, dict[str, str | None]]:
        if is_local_host(resolved_host):
            if artifact_name is not None:
                dst = fetch_artifact_local(
                    jobid,
                    artifact_name,
                    output_dir,
                    multi_user=_multi_user_active(cfg),
                    **({"workdir": True, "subdir": artifact_subdir} if fetch_workdir else {}),
                )
            elif fetch_workdir:
                dst = fetch_workdir_local(
                    jobid,
                    output_dir,
                    multi_user=_multi_user_active(cfg),
                )
            else:
                dst = fetch_local(
                    jobid,
                    output_dir,
                    multi_user=_multi_user_active(cfg),
                )
            return dst, _queue_handle_for(resolved_host)
        try:
            host_cfg = cfg.host(resolved_host)
        except config.ConfigError as e:
            raise click.UsageError(str(e)) from None
        if artifact_name is not None:
            dst = fetch_artifact_remote(
                host_cfg, jobid, artifact_name, output_dir,
                **({"workdir": True, "subdir": artifact_subdir} if fetch_workdir else {}),
            )
        elif fetch_workdir:
            dst = fetch_workdir_remote(host_cfg, jobid, output_dir)
        else:
            dst = fetch_remote(host_cfg, jobid, output_dir)
        return dst, _queue_handle_for(resolved_host)

    try:
        try:
            dst, queue_handle = _fetch_from(host)
        except (FileNotFoundError, transport.RemoteError) as e:
            if not inferred_host or "no such job" not in str(e).lower():
                raise
            owner = _locate_job_host(
                cfg,
                jobid,
                multi_user=_multi_user_active(cfg),
                exclude=frozenset({host}),
            )
            click.echo(
                f"vq: job {jobid} was not on default host {host!r}; "
                f"retrying fetch from {owner!r}.",
                err=True,
            )
            requested_queue_host = owner
            dst, queue_handle = _fetch_from(owner)
    except FileNotFoundError as e:
        raise click.UsageError(str(e)) from None
    except FileExistsError as e:
        raise click.UsageError(str(e)) from None
    except transport.RemoteError as e:
        raise click.ClickException(str(e)) from None
    except (ValueError, OSError, ownership.OwnershipError, config.ConfigError) as e:
        raise click.ClickException(str(e)) from None
    label = (
        f"fetched artifact {artifact_name}"
        if artifact_name is not None
        else ("fetched workdir" if fetch_workdir else "fetched")
    )
    # (#111): report WHEN these bytes were fetched, so an operator or
    # a poller reading the line does not have to take "fetched" on faith. A
    # single-artifact fetch writes no sidecar, so it has no manifest.
    manifest = None
    if artifact_name is None:
        try:
            manifest = require_fresh_fetch_manifest(
                dst,
                jobid=jobid,
                source_kind="workdir" if fetch_workdir else "workspace",
            )
            if not fetch_workdir and not explicit_workspace:
                require_workspace_artifact_selection(dst)
        except ValueError as exc:
            raise click.ClickException(
                f"fetched tree at {dst}, but {exc}; refusing to report success"
            ) from None
    fetched_at = manifest.get("fetched_at") if manifest else None
    stale = bool(manifest.get("stale")) if manifest else False
    if as_json:
        click.echo(
            json.dumps(
                {
                    "jobid": jobid,
                    "kind": (
                        "artifact"
                        if artifact_name is not None
                        else ("workdir" if fetch_workdir else "workspace")
                    ),
                    "name": artifact_name,
                    "source_kind": "workdir" if fetch_workdir else "workspace",
                    "source_subdir": artifact_subdir,
                    "destination": str(dst),
                    "fetched_at": fetched_at,
                    "stale": stale,
                    "queue_handle": queue_handle,
                },
                indent=2,
                sort_keys=True,
            )
        )
    else:
        suffix = f" (fetched_at={fetched_at})" if fetched_at else ""
        click.echo(f"{label} -> {dst}{suffix}")


def _exit_suffix(exit_code: int | None) -> str:
    """The ` (exit_code=N[, SIGNAME])` suffix for a fetch result line, naming
    the signal for a 128+sig exit (137 -> SIGKILL) so a hard-killed fetched
    job is legible, matching `vq status` and the fetch-all summary."""
    if exit_code is None:
        return ""
    sig = signal_name_for_exit(exit_code)
    tail = f", {sig}" if sig else ""
    return f" (exit_code={exit_code}{tail})"


def _print_fetch_all_results(label: str, results: list[BulkFetchResult]) -> bool:
    """Print per-job lines + a one-line summary for a fetch-all run on
    ``label`` (a host name). A fetched job that died shows its terminal
    state + crash-tail hint inline (closing the loop with the v0.12.0
    crash-feedback field), so the sweep doubles as a failure report.
    Returns True if any job errored.
    """
    fetched = [r for r in results if r.outcome == "fetched"]
    skipped = [r for r in results if r.outcome == "skipped"]
    errored = [r for r in results if r.outcome == "error"]
    for r in fetched:
        name = f"{r.job_name} " if r.job_name else ""
        tag = ""
        if r.state and r.state != "completed":
            tag = f"  [{r.state.upper()}"
            tag += f": {r.failure_hint}]" if r.failure_hint else "]"
        click.echo(f"fetched {name}{r.jobid} -> {r.detail}{tag}")
    for r in errored:
        click.echo(f"ERROR   {r.jobid}: {r.detail}", err=True)
    parts = [f"{len(fetched)} fetched"]
    if skipped:
        parts.append(f"{len(skipped)} skipped (already present)")
    if errored:
        parts.append(f"{len(errored)} failed")
    click.echo(f"{label}: {', '.join(parts)}")
    return bool(errored)


@main.command(name="fetch-all")
@click.argument("host", required=False)
@click.option(
    "-o",
    "--output",
    "output_dir",
    type=click.Path(file_okay=False, dir_okay=True, path_type=Path),
    default=Path("."),
    show_default=True,
    help="Parent directory; each job's workspace lands under "
    "<output_dir>/<jobname>-<jobid>/ (or <output_dir>/<jobid>/ when "
    "unnamed). Run this from your submitting folder to pull a whole "
    "batch back into it.",
)
@click.option(
    "-s",
    "--state",
    "states",
    multiple=True,
    help="Restrict to specific terminal state(s), repeatable "
    "(e.g. -s completed -s failed). Default: every terminal state. A "
    "non-terminal or unknown state is rejected, since a fetch needs a "
    "finished job.",
)
@click.option(
    "--all-hosts",
    "all_hosts",
    is_flag=True,
    default=False,
    help="Sweep every configured host instead of one (mutually "
    "exclusive with HOST). A host that is unreachable or errors is "
    "reported and skipped so one bad host does not abort the rest.",
)
def fetch_all(
    host: str | None,
    output_dir: Path,
    states: tuple[str, ...],
    all_hosts: bool,
) -> None:
    """Fetch ALL terminal jobs' workspaces from a host (or every host).

    \b
    Forms:
      vq fetch-all                    # default_host, every terminal job -> ./
      vq fetch-all HOST               # every terminal job on HOST
      vq fetch-all HOST -o results    # ... into ./results/
      vq fetch-all HOST -s completed  # only COMPLETED jobs
      vq fetch-all --all-hosts        # sweep every configured host

    The bulk companion to ``vq fetch HOST JOBID``: one command returns
    every finished job's output (vibe-qc ``.out`` / ``.system``, CRYSTAL,
    ORCA, anything the job wrote into its workspace) to your directory,
    instead of fetching them one at a time. A fetched job that FAILED is
    flagged in the summary with the first line of its crash tail, so the
    sweep also tells you what died and why.

    Idempotent: an exact terminal receipt is reconciled and skipped. A prior
    live, stale, or changed-generation same-job snapshot is refreshed; an
    untrusted existing destination remains a collision. A per-job failure (a
    cleaned-up workspace) and, under ``--all-hosts``, a whole unreachable host
    are reported inline and do not abort the rest, and the command exits
    non-zero if anything errored. Archived jobs are not included.
    """
    cfg = config.load_config()

    # --all-hosts mirrors `vq queue --all`: it is mutually exclusive with a
    # positional HOST (naming one host while asking for all contradicts).
    if all_hosts and host is not None:
        raise click.UsageError(
            "--all-hosts and HOST are mutually exclusive. --all-hosts "
            "walks every configured host, so naming one is contradictory."
        )

    # Validate the state filter against the TERMINAL set. A fetch needs a
    # finished job, so -s running (or a typo) is a usage error rather than
    # a silently-empty sweep. bulk_fetch would drop every non-terminal
    # spec anyway, so catching it here gives a precise message instead.
    selected: set[str] | None = None
    if states:
        valid = {s.value for s in TERMINAL_STATES}
        unknown = set(states) - valid
        if unknown:
            raise click.UsageError(
                f"non-terminal or unknown state(s): {sorted(unknown)}. "
                f"vq fetch-all only fetches finished jobs. Valid: {sorted(valid)}"
            )
        selected = set(states)

    multi_user = _multi_user_active(cfg)

    def _one_host(h: str) -> list[BulkFetchResult]:
        """Enumerate every terminal job on ``h`` and fetch each to
        output_dir, mirroring `vq queue HOST` (local list_jobs, remote
        `vq queue localhost --json` over SSH). Raises on a host-level
        failure (bad config, unreachable, unparseable listing)."""
        scheduler_target = h if _scheduler_driver_host(cfg, h) is not None else None
        if scheduler_target is not None:
            driver = _scheduler_driver_host(cfg, h)
            assert driver is not None
            h = driver
        host_cfg = None
        if is_local_host(h):
            specs = [s for s in list_jobs(h, multi_user=multi_user) if not s.is_archived]
        else:
            host_cfg = cfg.host(h)
            import json

            raw = _delegate_to_remote(h, cfg, "queue", "localhost", "--json")
            specs = [JobSpec.model_validate(d) for d in json.loads(raw)]
        if scheduler_target is not None:
            specs = [s for s in specs if s.scheduler_target == scheduler_target]
        specs_by_id = {spec.id: spec for spec in specs}

        def fetch_one(jid: str) -> Path:
            if host_cfg is None:
                dst = fetch_local(
                    jid,
                    output_dir,
                    multi_user=multi_user,
                    idempotent=False,
                )
            else:
                refresh_existing = prepare_remote_bulk_fetch(
                    host_cfg,
                    specs_by_id[jid],
                    output_dir,
                )
                dst = fetch_remote(
                    host_cfg,
                    jid,
                    output_dir,
                    idempotent=refresh_existing,
                )
            require_fresh_fetch_manifest(
                dst,
                jobid=jid,
                source_kind="workspace",
            )
            return dst

        return bulk_fetch(specs, states=selected, fetch_one=fetch_one)

    if all_hosts:
        hosts = sorted(cfg.hosts)
        if not hosts:
            raise click.UsageError("--all-hosts: no hosts configured in ~/.config/vq/config.toml")
    else:
        hosts = [_resolve_host(cfg, host)]

    # Run each host. Under --all-hosts a host-level failure (unreachable,
    # bad config) is reported and skipped so one bad host never sinks the
    # sweep. For a single explicit host the error propagates as usual.
    aggregate: list[BulkFetchResult] = []
    any_error = False
    for h in hosts:
        try:
            res = _one_host(h)
        except (
            config.ConfigError,
            transport.RemoteError,
            ValueError,
            TypeError,
        ) as e:
            if not all_hosts:
                raise click.ClickException(str(e)) from None
            click.echo(f"{h}: SKIPPED ({e})", err=True)
            any_error = True
            continue
        any_error |= _print_fetch_all_results(h, res)
        aggregate.extend(res)

    if all_hosts and len(hosts) > 1:
        tf = sum(1 for r in aggregate if r.outcome == "fetched")
        ts = sum(1 for r in aggregate if r.outcome == "skipped")
        te = sum(1 for r in aggregate if r.outcome == "error")
        tot = [f"{tf} fetched"]
        if ts:
            tot.append(f"{ts} skipped")
        if te:
            tot.append(f"{te} failed")
        click.echo(f"all-hosts total: {', '.join(tot)}")

    if any_error:
        raise SystemExit(1)


@main.command()
@click.argument("host", required=False)
@click.option(
    "--archive",
    "mode_archive",
    is_flag=True,
    help="Archive eligible jobs: tar.bz2 the workspace, remove it, "
    "stamp the spec with archived_at + archive_path. Spec stays on "
    "disk (hidden from `vq queue` by default — pass `--show-archived` "
    "to include; annotated '(archived)' there). Requires --older-than.",
)
@click.option(
    "--delete",
    "mode_delete",
    is_flag=True,
    help="Delete eligible jobs: remove spec + workspace + archive (if any). "
    "Job vanishes from `vq queue`. Requires --older-than.",
)
@click.option(
    "--restore",
    "mode_restore",
    metavar="JOBID",
    default=None,
    help="Un-tar the archive for JOBID back into its workspace, clear "
    "archived_at + archive_path. Mutually exclusive with --archive / --delete.",
)
@click.option(
    "--older-than",
    "older_than_str",
    metavar="DURATION",
    default=None,
    help="Filter eligibility by finished_at age. Forms: '30d', '4h', "
    "'1w', '600s' (integer + unit s/m/h/d/w). Required for --archive "
    "and --delete; ignored for --restore.",
)
@click.option(
    "-x",
    "--execute",
    is_flag=True,
    help="Actually perform the action. Without -x, the verb is dry-run "
    "and only prints what would happen — a typo in --older-than can't "
    "nuke six months of work.",
)
@click.option(
    "--auto-enable",
    "auto_enable",
    is_flag=True,
    help="v0.5.17: enable daemon-side auto-cleanup. Writes a policy "
    "file the daemon reads each iteration. Combine with "
    "--archive-after, --delete-after, --interval, --reason. "
    "Mutually exclusive with --archive / --delete / --restore.",
)
@click.option(
    "--auto-disable",
    "auto_disable",
    is_flag=True,
    help="v0.5.17: disable daemon-side auto-cleanup (removes the "
    "policy file; manual `vq cleanup` is unaffected).",
)
@click.option(
    "--auto-status",
    "auto_status",
    is_flag=True,
    help="v0.5.17: print current auto-cleanup policy and exit.",
)
@click.option(
    "--archive-after",
    "archive_after_str",
    metavar="DUR",
    default=None,
    help="Auto-cleanup: archive terminal jobs older than DUR. "
    "Format: '30d' / '4h' / '600s' etc. Only meaningful with "
    "--auto-enable.",
)
@click.option(
    "--delete-after",
    "delete_after_str",
    metavar="DUR",
    default=None,
    help="Auto-cleanup: delete terminal jobs older than DUR. Typically "
    "larger than --archive-after so jobs get archived first.",
)
@click.option(
    "--interval",
    "interval_str",
    metavar="DUR",
    default=None,
    help="Auto-cleanup: minimum gap between sweeps. Default 24h. "
    "Only meaningful with --auto-enable.",
)
@click.option(
    "--workdir-max-age",
    "workdir_max_age_str",
    metavar="DUR",
    default=None,
    help="Auto-cleanup: remove terminal-job workdirs older than DUR. "
    "Format: '14d' / '4h' / '600s' etc. Only meaningful with "
    "--auto-enable.",
)
@click.option(
    "--reason",
    "reason",
    metavar="TEXT",
    default=None,
    help="Optional free-text label on the auto-cleanup policy, surfaced via --auto-status.",
)
@click.option(
    "--archive-dir",
    "archive_dir_str",
    metavar="DIR",
    default=None,
    help="v0.5.22: override where archive tarballs go. Default is "
    "<state_root>/archive/ (and ``$VQ_ARCHIVE_DIR`` overrides "
    "globally). With --auto-enable: stored in the policy and used "
    "for every sweep. With --archive: used for this invocation "
    "only. Useful when ~ is small but a secondary disk is roomy.",
)
@click.option(
    "--archive-after-state",
    "archive_after_state",
    metavar="STATE:DUR",
    multiple=True,
    help="v0.5.23: per-state archive threshold override. Repeatable; "
    "format STATE:DUR (e.g. 'failed:7d', 'completed:90d'). "
    "Per-state value wins over --archive-after for that state; "
    "states not specified fall back to --archive-after. Useful "
    "when failed-job forensics matter longer than the success "
    "record. Only meaningful with --auto-enable. Valid states: "
    "completed, failed, killed, interrupted, oom_killed, starved, "
    "time_exceeded, aborted_by_queue.",
)
@click.option(
    "--delete-after-state",
    "delete_after_state",
    metavar="STATE:DUR",
    multiple=True,
    help="v0.5.23: per-state delete threshold override. Symmetric to --archive-after-state.",
)
@click.option(
    "--jobid",
    "jobids",
    metavar="JOBID",
    multiple=True,
    help="v0.6.23: act on specific job(s) instead of filtering by "
    "age. Repeatable. Mutually exclusive with --older-than. "
    "Works with --archive / --delete; for --restore the "
    "positional JOBID is already explicit. Each jobid must be "
    "in a terminal state — non-terminal / unknown / wrong-"
    "archive-state jobs surface per-jobid errors and the rest "
    "of the batch proceeds. Useful when you know exactly which "
    "smoke-test or one-off you want gone.",
)
def cleanup(
    host: str | None,
    mode_archive: bool,
    mode_delete: bool,
    mode_restore: str | None,
    older_than_str: str | None,
    execute: bool,
    auto_enable: bool,
    auto_disable: bool,
    auto_status: bool,
    archive_after_str: str | None,
    delete_after_str: str | None,
    interval_str: str | None,
    workdir_max_age_str: str | None,
    reason: str | None,
    archive_dir_str: str | None,
    archive_after_state: tuple[str, ...],
    delete_after_state: tuple[str, ...],
    jobids: tuple[str, ...],
) -> None:
    """Archive, delete, or restore terminal-state job workspaces.

    \b
    Forms:
      vq cleanup [HOST]                                      # list terminal jobs (read-only)
      vq cleanup [HOST] --archive --older-than 30d           # dry-run preview
      vq cleanup [HOST] --archive --older-than 30d -x        # actually archive
      vq cleanup [HOST] --delete  --older-than 90d           # dry-run preview
      vq cleanup [HOST] --delete  --older-than 90d -x        # actually delete
      vq cleanup [HOST] --restore JOBID                      # dry-run preview
      vq cleanup [HOST] --restore JOBID -x                   # actually restore

    Default mode (no flag) lists terminal-state jobs with their finished_at
    age and on-disk size — useful before deciding what `--older-than`
    threshold to apply.
    """
    # v0.5.17: --auto-* flags are mutually exclusive with the
    # one-shot --archive/--delete/--restore modes.
    auto_count = sum([auto_enable, auto_disable, auto_status])
    if auto_count > 1:
        raise click.UsageError(
            "--auto-enable / --auto-disable / --auto-status are mutually exclusive"
        )
    if auto_count > 0 and (mode_archive or mode_delete or mode_restore):
        raise click.UsageError(
            "--auto-* flags are mutually exclusive with --archive / "
            "--delete / --restore (the auto policy uses those primitives "
            "internally; mixing them in one invocation is ambiguous)"
        )

    # Mode mutex: exactly zero or one of {archive, delete, restore} may be set.
    mode_count = sum([mode_archive, mode_delete, bool(mode_restore)])
    if mode_count > 1:
        raise click.UsageError("--archive / --delete / --restore are mutually exclusive")
    # v0.6.23: --jobid X (repeatable) bypasses the --older-than
    # requirement for --archive / --delete. Mutually exclusive with
    # --older-than to avoid confusion about which filter wins.
    if jobids and older_than_str:
        raise click.UsageError(
            "--jobid and --older-than are mutually exclusive "
            "(--jobid picks specific jobs; --older-than filters by age)"
        )
    if (mode_archive or mode_delete) and not older_than_str and not jobids:
        raise click.UsageError(
            "--older-than or --jobid is required for --archive and --delete "
            "(e.g. --older-than 30d, or --jobid <jobid>)"
        )
    if jobids and not (mode_archive or mode_delete):
        raise click.UsageError(
            "--jobid only applies to --archive / --delete; --restore "
            "takes its jobid positionally (`--restore JOBID`)"
        )

    cfg = config.load_config()
    host = _resolve_host(cfg, host)
    # v0.6.43: on a multi-user host vq cleanup resolves terminal
    # jobs from the per-user state dirs under /var/lib/vq/users/.
    mu = _multi_user_active(cfg)

    # v0.5.17: auto-* branches short-circuit before the mode logic.
    if auto_status or auto_enable or auto_disable:
        from vq.cleanup import (
            AutoCleanupPolicy,
            clear_auto_cleanup_policy,
            format_auto_cleanup_status,
            write_auto_cleanup_policy,
        )

        if not is_local_host(host):
            # Pass through verbatim to remote vq.
            remote_args: list[str] = ["cleanup", "localhost"]
            if auto_status:
                remote_args.append("--auto-status")
            elif auto_disable:
                remote_args.append("--auto-disable")
            else:  # auto_enable
                remote_args.append("--auto-enable")
                if archive_after_str:
                    remote_args.extend(["--archive-after", archive_after_str])
                if delete_after_str:
                    remote_args.extend(["--delete-after", delete_after_str])
                if interval_str:
                    remote_args.extend(["--interval", interval_str])
                if workdir_max_age_str:
                    remote_args.extend(["--workdir-max-age", workdir_max_age_str])
                if reason:
                    remote_args.extend(["--reason", reason])
                if archive_dir_str:
                    remote_args.extend(["--archive-dir", archive_dir_str])
                # v0.5.23: per-state overrides (repeatable).
                for entry in archive_after_state:
                    remote_args.extend(["--archive-after-state", entry])
                for entry in delete_after_state:
                    remote_args.extend(["--delete-after-state", entry])
            click.echo(_delegate_to_remote(host, cfg, *remote_args), nl=False)
            return
        if auto_status:
            click.echo(format_auto_cleanup_status())
            return
        if auto_disable:
            removed = clear_auto_cleanup_policy()
            if removed:
                click.echo("auto-cleanup disabled (policy file cleared)")
            else:
                click.echo("auto-cleanup was not enabled (no-op)")
            return
        # auto_enable
        # v0.5.23/v0.6.54: at least one cleanup action threshold must be set.
        if not (
            archive_after_str
            or delete_after_str
            or workdir_max_age_str
            or archive_after_state
            or delete_after_state
        ):
            raise click.UsageError(
                "--auto-enable requires at least one of --archive-after, "
                "--delete-after, --workdir-max-age, --archive-after-state, or "
                "--delete-after-state (otherwise the auto-policy has "
                "nothing to do)"
            )
        try:
            archive_seconds = (
                int(parse_age(archive_after_str).total_seconds()) if archive_after_str else None
            )
            delete_seconds = (
                int(parse_age(delete_after_str).total_seconds()) if delete_after_str else None
            )
            interval_seconds = (
                int(parse_age(interval_str).total_seconds()) if interval_str else 24 * 60 * 60
            )
            workdir_max_age_seconds = (
                int(parse_age(workdir_max_age_str).total_seconds())
                if workdir_max_age_str
                else None
            )
            # v0.5.23: per-state overrides.
            from vq.cleanup import parse_state_age

            archive_after_by_state: dict[str, int] = {}
            for entry in archive_after_state:
                st, td = parse_state_age(entry)
                archive_after_by_state[st] = int(td.total_seconds())
            delete_after_by_state: dict[str, int] = {}
            for entry in delete_after_state:
                st, td = parse_state_age(entry)
                delete_after_by_state[st] = int(td.total_seconds())
        except ValueError as e:
            raise click.UsageError(str(e)) from None
        # v0.5.22: normalise the optional archive_dir override.
        # Expand the user shorthand ("~/...") here so the persisted
        # policy on disk is unambiguous about which directory it means.
        archive_dir_value = str(Path(archive_dir_str).expanduser()) if archive_dir_str else None
        policy = AutoCleanupPolicy(
            enabled=True,
            archive_after_seconds=archive_seconds,
            delete_after_seconds=delete_seconds,
            interval_seconds=interval_seconds,
            reason=reason,
            archive_dir=archive_dir_value,
            archive_after_by_state=archive_after_by_state,
            delete_after_by_state=delete_after_by_state,
            workdir_max_age_seconds=workdir_max_age_seconds,
        )
        write_auto_cleanup_policy(policy)
        parts = ["auto-cleanup enabled"]
        if archive_seconds is not None:
            parts.append(f"archive_after={archive_seconds}s")
        if delete_seconds is not None:
            parts.append(f"delete_after={delete_seconds}s")
        if workdir_max_age_seconds is not None:
            parts.append(f"workdir_max_age={workdir_max_age_seconds}s")
        parts.append(f"interval={interval_seconds}s")
        if archive_dir_value:
            parts.append(f"archive_dir={archive_dir_value}")
        # v0.5.23: surface per-state overrides in the confirmation line
        # so the user can spot a typo in the STATE name before the next
        # sweep runs.
        for st, secs in sorted(archive_after_by_state.items()):
            parts.append(f"archive_after[{st}]={secs}s")
        for st, secs in sorted(delete_after_by_state.items()):
            parts.append(f"delete_after[{st}]={secs}s")
        click.echo(f"{parts[0]} ({', '.join(parts[1:])})")
        return

    if not is_local_host(host):
        # Pass through every flag the user gave us so the remote vq runs
        # the same plan.
        remote_args: list[str] = ["cleanup", "localhost"]
        if mode_archive:
            remote_args.append("--archive")
        if mode_delete:
            remote_args.append("--delete")
        if mode_restore:
            remote_args.extend(["--restore", mode_restore])
        if older_than_str:
            remote_args.extend(["--older-than", older_than_str])
        if execute:
            remote_args.append("--execute")
        if archive_dir_str:
            remote_args.extend(["--archive-dir", archive_dir_str])
        # v0.6.23: forward --jobid (repeatable) to the remote.
        for jid in jobids:
            remote_args.extend(["--jobid", jid])
        click.echo(_delegate_to_remote(host, cfg, *remote_args), nl=False)
        return

    older_than = None
    if older_than_str:
        try:
            older_than = parse_age(older_than_str)
        except ValueError as e:
            raise click.UsageError(str(e)) from None

    # --restore branch: single jobid, no age filter.
    if mode_restore:
        # v0.6.43: resolve from the per-user dirs in multi-user mode.
        restore_uid: str | None = None
        if mu:
            try:
                restore_spec_path = paths.resolve_spec_path(mode_restore, multi_user=True)
            except FileNotFoundError:
                raise click.UsageError(f"no such job: {mode_restore}") from None
            restore_uid = restore_spec_path.parent.parent.name
        else:
            restore_spec_path = paths.spec_path(mode_restore)
        try:
            spec = JobSpec.read(restore_spec_path)
        except FileNotFoundError:
            raise click.UsageError(f"no such job: {mode_restore}") from None
        if not spec.is_archived:
            raise click.UsageError(f"job {mode_restore} is not archived; nothing to restore")
        candidates = find_candidates(require_archived=True, multi_user=mu)
        candidates = [c for c in candidates if c.spec.id == mode_restore]
        click.echo(cleanup_table(candidates, action="restore", dry_run=not execute))
        if execute:
            try:
                workspace = restore_workspace(
                    spec,
                    queue_dir=(
                        paths.user_queue_dir(restore_uid) if restore_uid is not None else None
                    ),
                )
            except (FileNotFoundError, FileExistsError, ValueError) as e:
                raise click.ClickException(str(e)) from None
            click.echo(f"restored -> {workspace}")
        return

    # --archive / --delete / no-action: scan candidates.
    # v0.6.23: --jobid X bypasses the age filter — operator names
    # specific jobs to act on. Per-jobid errors surface inline so
    # the operator sees exactly what was rejected and why.
    jobid_errors: list[tuple[str, str]] = []
    if mode_archive:
        if jobids:
            from vq.cleanup import find_candidates_by_jobid

            candidates, jobid_errors = find_candidates_by_jobid(
                list(jobids),
                require_archived=False,
                multi_user=mu,
            )
        else:
            candidates = find_candidates(
                older_than=older_than,
                require_archived=False,
                multi_user=mu,
            )
        action = "archive"
    elif mode_delete:
        if jobids:
            from vq.cleanup import find_candidates_by_jobid

            candidates, jobid_errors = find_candidates_by_jobid(
                list(jobids),
                multi_user=mu,
            )
        else:
            candidates = find_candidates(older_than=older_than, multi_user=mu)
        action = "delete"
    else:
        # No-action listing: every terminal job, archived or not.
        candidates = find_candidates(older_than=older_than, multi_user=mu)
        action = "list"

    # Surface per-jobid rejections (only set when --jobid is used).
    if jobid_errors:
        for jid, reason in jobid_errors:
            click.echo(f"  --jobid {jid}: {reason}", err=True)

    click.echo(cleanup_table(candidates, action=action, dry_run=not execute))
    if not execute or action == "list":
        return

    # v0.5.22: --archive-dir override for the one-shot --archive path.
    one_shot_archive_dir = Path(archive_dir_str).expanduser() if archive_dir_str else None
    scheduler_reaper = _scheduler_workspace_reaper(cfg)

    failures: list[tuple[str, str]] = []
    for c in candidates:
        # v0.6.43: in multi-user mode each candidate carries its
        # owning uid — archive/delete using that user's own queue +
        # archive dirs. An explicit --archive-dir still overrides.
        c_queue_dir = paths.user_queue_dir(c.uid) if c.uid else None
        c_archive_dir = (
            one_shot_archive_dir
            if one_shot_archive_dir is not None
            else (paths.user_archive_dir(c.uid) if c.uid else None)
        )
        try:
            if mode_archive:
                cleanup_scheduler_remote_workspace(
                    c.spec,
                    scheduler_reaper,
                    queue_dir=c_queue_dir,
                )
                archive_workspace(
                    c.spec,
                    queue_dir=c_queue_dir,
                    archive_dir=c_archive_dir,
                )
            elif mode_delete:
                cleanup_scheduler_remote_workspace(
                    c.spec,
                    scheduler_reaper,
                    queue_dir=c_queue_dir,
                )
                delete_job(c.spec, queue_dir=c_queue_dir)
        except Exception as e:  # noqa: BLE001 -- collect per-job, keep going
            failures.append((c.spec.id, str(e)))
    if failures:
        click.echo("")
        click.echo(f"failures: {len(failures)}")
        for jobid, msg in failures:
            click.echo(f"  {jobid}: {msg}")
        raise click.ClickException(f"{len(failures)} of {len(candidates)} job(s) failed")


def _hidden_tar_error_message(error: Exception) -> str:
    """Keep strict config failures on the one-line SSH helper protocol."""
    text = str(error)
    if isinstance(error, config.ConfigError):
        return text.splitlines()[0]
    return text


@main.command("mark-fetched", hidden=True)
@click.argument("jobid")
@click.option("--submitted-at", required=True)
@click.option(
    "--state",
    "state_name",
    type=click.Choice(sorted(state.value for state in TERMINAL_STATES)),
    required=True,
)
def mark_fetched(jobid: str, submitted_at: str, state_name: str) -> None:
    """Internal: acknowledge one exact terminal workspace fetch."""
    multi_user = _multi_user_active()
    try:
        marked_at = mark_terminal_fetch(
            jobid,
            submitted_at=submitted_at,
            state=state_name,
            multi_user=multi_user,
        )
    except (
        config.ConfigError,
        FileNotFoundError,
        OSError,
        ValueError,
        ownership.OwnershipError,
    ) as e:
        click.echo(_hidden_tar_error_message(e), err=True)
        raise SystemExit(1) from None
    click.echo(marked_at)


@main.command("tar-workspace", hidden=True)
@click.argument("jobid")
def tar_workspace(jobid: str) -> None:
    """Internal: emit the local workspace tarball for JOBID on stdout.

    Used by ``vq fetch <remote-host> JOBID``: the local side ssh's into
    the remote and runs this verb, piping the resulting tar stream
    through tarfile to extract it locally.
    """
    multi_user = _multi_user_active()
    try:
        emit_workspace_tar(jobid, multi_user=multi_user)
    except (
        config.ConfigError,
        FileNotFoundError,
        ValueError,
        ownership.OwnershipError,
        transport.RemoteError,
    ) as e:
        # Send the message to stderr so the local side's RemoteError can
        # surface it; exit non-zero so ssh exit reflects failure.
        click.echo(_hidden_tar_error_message(e), err=True)
        raise SystemExit(1) from None


@main.command("tar-workdir", hidden=True)
@click.argument("jobid")
def tar_workdir(jobid: str) -> None:
    """v0.7.7: emit the local workdir tarball for JOBID on stdout.

    Used by ``vq fetch <remote-host> JOBID --workdir``: the local side
    ssh's into the remote and runs this verb, piping the resulting tar
    stream through tarfile to extract it locally. Mirrors
    ``tar-workspace`` but tars ``spec.workdir`` (v0.6.54's per-job
    scratch directory) instead of ``spec.cwd``.
    """
    multi_user = _multi_user_active()
    try:
        emit_workdir_tar(jobid, multi_user=multi_user)
    except (
        config.ConfigError,
        FileNotFoundError,
        ValueError,
        ownership.OwnershipError,
    ) as e:
        click.echo(_hidden_tar_error_message(e), err=True)
        raise SystemExit(1) from None


@main.command("tar-artifact", hidden=True)
@click.argument("jobid")
@click.argument("name")
@click.option("--workdir", is_flag=True)
@click.option("--subdir", default=None)
def tar_artifact(jobid: str, name: str, workdir: bool, subdir: str | None) -> None:
    """Internal: emit one named artifact from the explicit source directory."""
    multi_user = _multi_user_active()
    try:
        emit_artifact_tar(jobid, name, multi_user=multi_user, workdir=workdir, subdir=subdir)
    except (
        config.ConfigError,
        FileNotFoundError,
        ValueError,
        ownership.OwnershipError,
        transport.RemoteError,
        OSError,
    ) as e:
        click.echo(_hidden_tar_error_message(e), err=True)
        raise SystemExit(1) from None


@main.command("build-env", hidden=True)
@click.argument("env")
@click.option("--baseline-sha", default=None, hidden=True)
@click.option("--expected-sha", default=None, hidden=True)
def build_env(
    env: str,
    baseline_sha: str | None,
    expected_sha: str | None,
) -> None:
    """Internal (v0.12.0 build-as-job): rebuild registered venv ENV on this
    host. This is the job-command form of the daemon's --refresh rebuild.

    It runs admin.update_env(ENV) for localhost (git pull + update_script)
    and exits 0 on success, 1 on a failed build, 2 if the rebuild could
    not even start. The daemon dispatches this as a normal job, so the
    cgroup scope, events, and dependents govern the build instead of an
    uncapped inline subprocess. Operators use `vq admin update`, not this.
    """
    cfg = config.load_config()
    try:
        if baseline_sha is None or expected_sha is None:
            raise admin_module.AdminError(
                "queued build-env command lacks its exact baseline/target "
                "receipt; allow the auto-update timer to submit a new job"
            )
        baseline_sha = baseline_sha.strip().lower()
        expected_sha = expected_sha.strip().lower()
        if (
            re.fullmatch(r"[0-9a-f]{40}", baseline_sha) is None
            or re.fullmatch(r"[0-9a-f]{40}", expected_sha) is None
        ):
            raise admin_module.AdminError(
                "queued build-env baseline and target must be full SHAs"
            )
        prog = admin_module._resolve_venv_program(env, cfg)
        from vq import auto_update as auto_update_module

        with (
            admin_module.admin_update_ownership(),
            admin_module.toolset_lifecycle_lock(
                [prog], action="vq-build-env",
            ),
        ):
            probe = admin_module._detect_vq_self_update(prog)
            if probe.is_self_update or not probe.manager_available:
                raise admin_module.AdminError(
                    "queued branch update cannot authoritatively prove this "
                    "environment is different from the serving vq daemon"
                )
            current_sha = admin_module.current_source_sha(Path(prog.git_dir))
            if current_sha != baseline_sha:
                raise admin_module.AdminError(
                    "queued branch update baseline is stale: current HEAD is "
                    f"{current_sha or 'unknown'}, receipt expected {baseline_sha}"
                )
            fetch_rc, fetch_detail = auto_update_module._fetch_origin(
                Path(prog.git_dir),
            )
            if fetch_rc != 0:
                raise admin_module.AdminError(
                    f"queued branch target refresh failed: {fetch_detail}"
                )
            if not prog.branch:
                raise admin_module.AdminError(
                    "queued branch update target has no configured branch"
                )
            observed_target = auto_update_module._rev_parse(
                Path(prog.git_dir), f"origin/{prog.branch}",
            )
            if observed_target != expected_sha:
                raise admin_module.AdminError(
                    "queued branch target is stale: origin/"
                    f"{prog.branch} is {observed_target or 'unknown'}, receipt "
                    f"expected {expected_sha}"
                )
            if auto_update_module._is_ancestor(
                Path(prog.git_dir), baseline_sha, expected_sha,
            ) is not True:
                raise admin_module.AdminError(
                    "queued branch target is not a proven descendant of its "
                    "baseline"
                )
            result = admin_module.update_env(
                env,
                cfg,
                host="localhost",
                expected_sha=expected_sha,
                restart_daemon=False,
            )
    except admin_module.AdminError as e:
        click.echo(f"build-env: {env!r} could not start: {e}", err=True)
        raise SystemExit(2) from None
    if not result.success:
        click.echo(f"build-env: rebuild of {env!r} failed", err=True)
        raise SystemExit(1)
    click.echo(f"build-env: {env} rebuilt OK")


@main.group()
def admin() -> None:
    """Queue admin verbs: refresh registered envs, etc. (v0.5.20+)."""


def _rollout_update_runner(
    argv: Sequence[str],
    **kwargs: object,
) -> subprocess.CompletedProcess[str]:
    """Stream child admin-update narration to stderr, preserving JSON stdout."""
    kwargs.pop("capture_output", None)
    # (#118): never inherit the operator's stdin — see
    # vq.transport._subprocess_session_kwargs.
    if "input" not in kwargs:
        kwargs.setdefault("stdin", subprocess.DEVNULL)
    return subprocess.run(
        argv,
        **kwargs,
        stdout=sys.stderr,
        stderr=sys.stderr,
    )


def _fleet_snapshots(
    cfg: config.Config,
) -> tuple[Mapping[str, Any], Mapping[str, Any], Mapping[str, Any]]:
    """The three read-only sweeps, with this fleet's doctor budget if it set one.

    Calls `collect_snapshots()` with no arguments when nothing is configured,
    so the default path is byte-identical to what it always was -- a fleet
    that never hit pbs-cluster's slow login node should see no change at all, and
    neither should the fakes in the test suite.
    """
    if cfg.fleet.check_timeout_seconds is None:
        return fleet_rollout.collect_snapshots()
    return fleet_rollout.collect_snapshots(
        check_timeout_seconds=cfg.fleet.check_timeout_seconds,
    )


def _emit_rollout_error(
    message: str, *, as_json: bool, outcome: str | None = None,
) -> None:
    """Render a rollout failure, carrying its classification when it has one.

    ``outcome`` is omitted for the failures that have always been exit 1, so
    the payload a consumer already parses is unchanged; it appears only where
    the exit code is something other than 1 and a caller therefore needs to
    know why.
    """
    if as_json:
        payload: dict[str, object] = {
            "schema": "vq.fleet.rollout_result/2",
            "status": "error",
            "error": message,
        }
        if outcome is not None:
            payload["outcome"] = outcome
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
    else:
        prefix = f"Error [{outcome}]" if outcome is not None else "Error"
        click.echo(f"{prefix}: {message}", err=True)


def _attest_local_fleet_driver(cfg: config.Config) -> str:
    """Require one stable scheduler-driver hostname naming this machine.

    ``localhost`` is a transport direction, not a fleet identity: a copied
    config on any workstation would otherwise authorize mutations in that
    workstation's local rollout/status tree.  The driver key or its SSH
    endpoint must therefore equal this OS hostname (short or FQDN) before the
    first report fetch or journal reconciliation.
    """
    drivers = {
        host.scheduler_driver
        for host in cfg.hosts.values()
        if host.scheduler != "local" and host.scheduler_driver is not None
    }
    if len(drivers) != 1:
        raise fleet_rollout.FleetRolloutError(
            "fleet rollout requires exactly one configured scheduler_driver; "
            f"found {sorted(drivers)}"
        )
    driver = next(iter(drivers))
    driver_cfg = cfg.host(driver)
    hostname = socket.gethostname()
    identities = {hostname, hostname.split(".", 1)[0]}
    if driver not in identities and driver_cfg.ssh not in identities:
        raise fleet_rollout.FleetRolloutError(
            "scheduler_driver must be a stable hostname for this machine "
            f"before rollout mutation; configured driver={driver!r}, "
            f"ssh={driver_cfg.ssh!r}, local={hostname!r}. Loopback aliases "
            "such as 'localhost' cannot attest the fleet driver"
        )
    return driver


def _echo_plan_hold_supersede(
    payloads: list[dict[str, object]],
    *,
    as_json: bool,
) -> None:
    """Report every recorded plan-hold supersession.

    A single pair keeps the exact ``/1`` object it has always emitted, so an
    existing ``--json`` consumer is unaffected; several are wrapped in a batch
    envelope rather than folded into one.
    """
    if as_json:
        if len(payloads) == 1:
            click.echo(json.dumps(payloads[0], indent=2, sort_keys=True))
            return
        click.echo(json.dumps(
            {
                "schema": "vq.fleet.plan_hold_supersede_batch/1",
                "results": payloads,
                "n_recorded": sum(
                    1 for item in payloads if item["replayed"] is False
                ),
                "n_replayed": sum(
                    1 for item in payloads if item["replayed"] is True
                ),
            },
            indent=2,
            sort_keys=True,
        ))
        return
    for payload in payloads:
        click.echo(
            f"plan-bound hold supersede {payload['status']}: "
            f"{payload['rollout_id']} {payload['host']} "
            f"(current {payload['current_rollout_id']})"
        )


def _echo_legacy_reconciliation_preview(
    inventory: fleet_rollout.LegacyRolloutInventory,
    *,
    as_json: bool,
) -> None:
    """Print what ``--reconcile-legacy`` would rewrite, having rewritten none.

    The mutating form writes durable supersession evidence fleet-wide and
    marks historical action outcomes permanently unknown. That is a bad thing
    to authorise blind, and until this existed the operator had no way to see
    it first.
    """
    if as_json:
        click.echo(json.dumps(inventory.as_dict(), indent=2, sort_keys=True))
        return
    click.echo("== --reconcile-legacy preview (nothing was written) ==")
    if inventory.empty:
        click.echo("   no legacy rollout state to reconcile")
        return
    sections = (
        (
            "journal actions whose outcome would become unknown",
            [
                f"{rollout_id} {action_id}"
                for rollout_id, action_id in inventory.running_actions
            ],
        ),
        (
            "obsolete scheduler claims that would be covered",
            [
                f"{rollout_id} {host}"
                for rollout_id, host in inventory.scheduler_holds
            ],
        ),
        (
            "retained holds that would be re-observed",
            [
                f"{rollout_id} {host}"
                for rollout_id, host in inventory.hold_retries
            ],
        ),
        (
            "failure transactions that would be settled",
            [
                f"{rollout_id} {host}: {reason}"
                for rollout_id, host, reason
                in inventory.pending_failure_transitions
            ],
        ),
    )
    for title, rows in sections:
        if not rows:
            continue
        click.echo(f"\n-- {title} --")
        for row in rows:
            click.echo(f"   {row}")
    click.echo(
        "\nRun the same command without --dry-run to write this. It is "
        "durable and fleet-wide."
    )


@admin.command("rollout-latest")
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Discover the latest accepted report and print the exact serial plan "
    "without changing any host.",
)
@click.option(
    "--verify-only",
    "verify_only",
    is_flag=True,
    default=False,
    help="v0.24.x: change nothing; emit ONE convergence verdict "
    "for the modeled managed, required vq-user, and read-only provenance "
    "lanes (converged | degraded(hosts...)) from "
    "the same planner a rollout uses. Coverage exclusions remain explicit; "
    "this does not assert whole-fleet convergence. Exit 0 modeled lanes "
    "converged, 2 degraded. Mutually exclusive with --dry-run.",
)
@click.option(
    "--only",
    "only_hosts",
    metavar="HOST",
    multiple=True,
    help="v0.24.x: act on HOST only. Repeatable. Keeps single-host recovery "
    "inside the accepted report's pinned path instead of a hand-typed "
    "`vq admin update <env> <host> --expected-sha <40-hex>`. The driver's own "
    "vq lane is always evaluated. Selection is host-granular, so a host's "
    "lanes stay bracketed by one drain exactly as in an unscoped run. "
    "Mutually exclusive with --skip.",
)
@click.option(
    "--skip",
    "skip_hosts",
    metavar="HOST",
    multiple=True,
    help="v0.24.x: act on every host EXCEPT HOST. Repeatable. Excluded lanes "
    "are reported as deferred, never as converged. The driver's own vq lane is "
    "always evaluated. Mutually exclusive with --only.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Emit the plan/result as machine-readable JSON. Update transcripts "
    "continue on stderr.",
)
@click.option("--resume", hidden=True, default=None)
@click.option(
    "--supersede-plan-hold",
    "supersede_plan_hold",
    nargs=2,
    multiple=True,
    metavar="ROLLOUT HOST",
    help="Recovery-only: permanently cover one exact obsolete plan-bound "
    "scheduler hold after a strictly newer accepted plan proves every "
    "scheduler lane healthy at its exact SHA and a supported read-only drain "
    "snapshot proves the target inactive. Writes one idempotent journal "
    "receipt, executes no rollout action, and exits. Repeatable: one obsolete "
    "rollout can retain a hold per scheduler lane (pbs-cluster retains six), and "
    "each pair is proved and recorded exactly as it is on its own. Stops at "
    "the first pair that fails its preconditions, having recorded the ones "
    "before it; the receipts are idempotent, so re-run after fixing it.",
)
@click.option(
    "--reconcile-legacy",
    "reconcile_legacy",
    is_flag=True,
    default=False,
    help="Explicitly reconcile authenticated pre-recorder rollout journals "
    "and obsolete pre-owner scheduler claims from a fresh healthy "
    "accepted-report plan plus supported read-only drain evidence. "
    "Historical action outcomes remain unknown. This recovery-only command "
    "writes durable supersession/retention evidence and exits before any "
    "rollout action; run an ordinary --dry-run next. Add --dry-run to this "
    "invocation to list exactly what it would reconcile, writing nothing.",
)
def admin_rollout_latest(
    dry_run: bool,
    verify_only: bool,
    only_hosts: tuple[str, ...],
    skip_hosts: tuple[str, ...],
    as_json: bool,
    resume: str | None,
    supersede_plan_hold: tuple[tuple[str, str], ...],
    reconcile_legacy: bool,
) -> None:
    """Roll the newest accepted release report across the configured fleet.

    The operator supplies no version, tag, or SHA. The command discovers the
    newest accepted machine report from the managed driver's runtime checkout,
    applies its exact component pins without downgrade, updates the driver's vq
    first, then re-enters through that fresh interpreter for scheduler helpers
    and runtime lanes. Re-running is an idempotent live-state reconciliation.

    \b
    Forms:
      vq admin rollout-latest                     roll the whole fleet
      vq admin rollout-latest --dry-run           print the exact serial plan
      vq admin rollout-latest --verify-only       one convergence verdict
      vq admin rollout-latest --only workstation      recover one host, still pinned
      vq admin rollout-latest --skip compute-a         roll everything but one host
      vq admin rollout-latest --supersede-plan-hold ROLLOUT HOST
      vq admin rollout-latest --reconcile-legacy --dry-run   preview it
      vq admin rollout-latest --reconcile-legacy             write it
    """
    if dry_run and verify_only:
        raise click.UsageError("--dry-run and --verify-only are mutually exclusive")
    if reconcile_legacy and verify_only:
        raise click.UsageError(
            "--reconcile-legacy is a mutating recovery option and cannot be "
            "combined with --verify-only, which answers a different question "
            "(has the fleet converged). Use --dry-run to preview what it "
            "would reconcile."
        )
    if resume is not None and (dry_run or verify_only):
        raise click.UsageError(
            "driver re-entry acknowledgement is mutating and cannot be "
            "combined with --dry-run or --verify-only"
        )
    if reconcile_legacy and (only_hosts or skip_hosts):
        raise click.UsageError(
            "--reconcile-legacy is fleet-global recovery and cannot be "
            "combined with --only or --skip"
        )
    if supersede_plan_hold and (
        dry_run
        or verify_only
        or reconcile_legacy
        or only_hosts
        or skip_hosts
        or resume is not None
    ):
        raise click.UsageError(
            "--supersede-plan-hold is an exact recovery-only operation and "
            "cannot be combined with rollout, read-only, scoped, resume, or "
            "legacy-reconciliation options"
        )
    if len(set(supersede_plan_hold)) != len(supersede_plan_hold):
        # Each receipt is idempotent, so a repeat is harmless -- but it is
        # also certainly a copy-paste slip in a command whose whole job is to
        # name exact pairs, and this command should not absorb those quietly.
        raise click.UsageError(
            "--supersede-plan-hold names the same ROLLOUT HOST pair twice"
        )
    if not verify_only:
        # Validate the direct-venv timeout contract before discovery performs
        # its first snapshot SSH. Dry-run validates the executable plan too;
        # verify-only can never delegate an update and remains independent of
        # update-only settings. Driver actions and fresh-interpreter re-entry
        # inherit the same process environment.
        _remote_admin_update_contract()
    rollout_fence: contextlib.ExitStack | None = None
    lifecycle_fence: contextlib.ExitStack | None = None
    inherited_reentry_fence: contextlib.ExitStack | None = None
    try:
        cfg = config.load_config()
        selection = fleet_rollout.resolve_selection(
            cfg,
            only=only_hosts,
            skip=skip_hosts,
        )
        configured_driver_hosts = {
            host.scheduler_driver
            for host in cfg.hosts.values()
            if host.scheduler != "local" and host.scheduler_driver is not None
        }
        out_of_scope_receipt_hosts = frozenset(
            host
            for host in cfg.hosts
            if selection.scoped
            and not selection.includes(host)
            and host not in configured_driver_hosts
        )
        repo = fleet_release.runtime_repo()
        driver_prog = cfg.programs.get("vibeqc-queue")
        if not isinstance(driver_prog, config.VenvProgram):
            raise fleet_rollout.FleetRolloutError(
                "rollout-latest requires a configured local "
                "[programs.vibeqc-queue] lifecycle target"
            )
        try:
            configured_repo = admin_module._canonical_lifecycle_checkout(
                Path(driver_prog.git_dir)
            )
            runtime_checkout = admin_module._canonical_lifecycle_checkout(repo)
        except admin_module.AdminError as exc:
            raise fleet_rollout.FleetRolloutError(
                f"could not bind rollout controller checkout: {exc}"
            ) from exc
        if configured_repo != runtime_checkout:
            raise fleet_rollout.FleetRolloutError(
                "configured vibeqc-queue lifecycle checkout does not match "
                "the running controller checkout; refusing report discovery "
                f"({configured_repo} != {runtime_checkout})"
            )
        configured_target = admin_module._canonical_lifecycle_target(
            Path(driver_prog.python).parent.parent
        )
        running_target = admin_module._canonical_lifecycle_target(
            Path(sys.executable).parent.parent
        )
        if configured_target != running_target:
            raise fleet_rollout.FleetRolloutError(
                "configured vibeqc-queue lifecycle target does not match the "
                "running controller interpreter; refusing report discovery "
                f"({configured_target} != {running_target})"
            )

        reports = fleet_release.report_repo(cfg)
        report_checkout = admin_module._canonical_lifecycle_checkout(reports)

        inherited_reentry_fence = contextlib.ExitStack()
        inherited_reentry = inherited_reentry_fence.enter_context(
            fleet_rollout.adopt_rollout_reentry_handoff(
                expected_rollout_id=resume,
                expected_lifecycle_resources=tuple(sorted({
                    ("checkout", str(configured_repo)),
                    ("target", str(configured_target)),
                    ("checkout", str(report_checkout)),
                })),
            )
        )

        if not (dry_run or verify_only):
            # Lock order is global rollout -> checkout/venv everywhere (the
            # same order used by ``vq self-update``).  The rollout lock path is
            # fleet-global regardless of this neutral admission label, so it
            # can safely precede report discovery without knowing the report
            # ID and cannot deadlock an opposing self-update admission.
            rollout_fence = contextlib.ExitStack()
            rollout_fence.enter_context(
                fleet_rollout.rollout_execution_lock("discover-latest")
            )

        # Discovery fetches origin/main + tags, so even dry-run/verify-only is
        # a ref-mutating controller. Hold the same checkout/venv lock used by
        # direct update.sh across discovery and planning, not only around the
        # fetch subprocess. Mutating action children need that exact lock, so
        # the controller deliberately releases it immediately before launch
        # and re-acquires it for final report/ancestry verification.
        def acquire_lifecycle_fence() -> contextlib.ExitStack:
            stack = contextlib.ExitStack()
            stack.enter_context(
                admin_module.toolset_lifecycle_lock(
                    [driver_prog], action="vq-rollout-controller",
                    extra_resources=(("checkout", str(report_checkout)),),
                )
            )
            return stack

        lifecycle_fence = acquire_lifecycle_fence()
        controller_lifecycle_handoff, _controller_lifecycle_fds = (
            admin_module._active_toolset_lifecycle_handoff()
        )
        controller_lifecycle_resources = (
            admin_module._active_toolset_lifecycle_resources()
        )

        def discover_current_report() -> fleet_release.FleetReleaseReport:
            with admin_module.toolset_lifecycle_lock(
                [driver_prog], action="vq-rollout-report-discovery",
            ):
                discovered = fleet_release.discover_latest_report(
                    reports,
                    pin_repos=cfg.pin_source_repos,
                    runner=admin_module._mutating_git_run,
                )
                if not dry_run:
                    fleet_release.require_latest_report(discovered)
                return discovered

        def current_report_digest() -> str:
            return discover_current_report().digest_sha256

        report = discover_current_report()
        discovery_warning = fleet_release.discovery_warning(report)
        if discovery_warning is not None:
            click.echo(f"WARNING: {discovery_warning}", err=True)

        def ancestry(pin_name: str, older: str, newer: str) -> bool | None:
            return fleet_release.git_is_pin_ancestor(
                report, pin_name, repo, older, newer,
                pin_repos=cfg.pin_source_repos,
            )

        try:
            target_vq_tree_sha256 = (
                admin_module.source_tree_sha256_at_git_commit(
                    # Either layout: vibe-queue/ inside the monorepo, or the
                    # runtime checkout itself since the split.
                    admin_module.vq_project_root(repo, strict=False),
                    report.pins["vq"].sha,
                )
            )
        except admin_module.AdminError as exc:
            raise fleet_rollout.FleetRolloutError(
                f"could not derive accepted vq package digest: {exc}"
            ) from exc
        current_rollout_id = fleet_rollout.rollout_id(report)
        if supersede_plan_hold:
            admin_snapshot, program_snapshot, doctor_snapshot = (
                _fleet_snapshots(cfg)
            )

            plan = fleet_rollout.build_plan(
                cfg,
                report,
                admin_status=admin_snapshot,
                programs=program_snapshot,
                doctor=doctor_snapshot,
                ancestry=ancestry,
                target_vq_tree_sha256=target_vq_tree_sha256,
            )
            driver_ssh = cfg.host(plan.driver).ssh
            if not is_local_host(driver_ssh):
                raise fleet_rollout.FleetRolloutError(
                    f"this command must run on scheduler_driver {plan.driver!r} "
                    f"(ssh={driver_ssh!r})"
                )
            # One plan, proved once, then each pair proved and recorded on
            # its own terms. The per-host evidence requirements are unchanged
            # -- they are what caught "pbs-cluster is not converged yet" -- and what
            # is gone is six near-identical invocations of a recovery-only
            # command to clear one obsolete rollout's holds.
            payloads: list[dict[str, object]] = []
            try:
                for hold_rollout_id, hold_host in supersede_plan_hold:
                    result = fleet_rollout.supersede_obsolete_plan_bound_hold(
                        rollout_id_value=hold_rollout_id,
                        host=hold_host,
                        accepted_report=report,
                        plan=plan,
                        repo=reports,
                        current_report_digest_resolver=current_report_digest,
                        control_runner=subprocess.run,
                    )
                    payloads.append({
                        "schema": "vq.fleet.plan_hold_supersede_result/1",
                        "status": (
                            "already-recorded" if result.replayed else "recorded"
                        ),
                        "rollout_id": result.rollout_id,
                        "host": result.host,
                        "current_rollout_id": result.current_rollout_id,
                        "replayed": result.replayed,
                    })
            finally:
                # Whatever was recorded before a refusal is durable and the
                # operator has to know it, so this prints on both paths. The
                # receipts are idempotent: re-running after the cause is fixed
                # replays them and continues.
                _echo_plan_hold_supersede(payloads, as_json=as_json)
            return
        if reconcile_legacy and dry_run:
            # Read-only preview, and it returns HERE rather than threading a
            # flag through the mutating path below: everything between this
            # point and the reconciliation writes, and a preview that has to
            # be trusted to skip each of those is not a preview.
            inventory = fleet_rollout.inventory_legacy_rollout_state(
                current_report_digest_sha256=report.digest_sha256,
                current_report_source_path=report.source_path,
                allowed_retention_identity_mismatch_hosts=(
                    out_of_scope_receipt_hosts
                ),
                configured_retention_hosts=frozenset(cfg.hosts),
                retired_hosts=cfg.fleet.retired_hosts,
                report_repo=reports,
            )
            _echo_legacy_reconciliation_preview(inventory, as_json=as_json)
            return

        # Reconcile every report and host before the first fleet snapshot.
        # Selection is intentionally not available to this pass, so a scoped
        # recovery cannot hide an orphaned detached mutation elsewhere.
        reconciliation = fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256=report.digest_sha256,
            current_report_source_path=report.source_path,
            inspect_only=dry_run or verify_only,
            allow_legacy_reconciliation=reconcile_legacy,
            allowed_retention_identity_mismatch_hosts=(
                out_of_scope_receipt_hosts
            ),
            configured_retention_hosts=frozenset(cfg.hosts),
            retired_hosts=cfg.fleet.retired_hosts,
            report_repo=reports,
            acknowledge_driver_reentry=resume,
            authenticated_driver_reentry=inherited_reentry,
            control_runner=subprocess.run,
            stream=sys.stderr,
            lifecycle_handoff=controller_lifecycle_handoff,
        )
        if reconciliation.driver_reentry_required:
            retained_driver = next(
                (
                    (rollout_id, reason)
                    for rollout_id, host, reason
                    in reconciliation.retained_legacy_holds
                    if host == reconciliation.driver_reentry_host
                ),
                None,
            )
            if retained_driver is not None:
                retained_rollout, retained_reason = retained_driver
                raise fleet_rollout.FleetRolloutError(
                    "driver fresh-interpreter re-entry is fenced by retained "
                    f"legacy hold {retained_rollout} on "
                    f"{reconciliation.driver_reentry_host}: "
                    f"{retained_reason}"
                )
            assert rollout_fence is not None
            if (
                reconciliation.driver_reentry_operation_id is None
                or reconciliation.driver_reentry_request_sha256 is None
                or reconciliation.driver_reentry_report_digest_sha256 is None
            ):
                raise fleet_rollout.FleetRolloutError(
                    "driver fresh-interpreter re-entry lacks its exact durable receipt"
                )
            reentry_capability = fleet_rollout.RolloutReentryCapability(
                rollout_id=str(reconciliation.driver_reentry_rollout_id),
                operation_id=reconciliation.driver_reentry_operation_id,
                request_sha256=reconciliation.driver_reentry_request_sha256,
                report_digest_sha256=(
                    reconciliation.driver_reentry_report_digest_sha256
                ),
            )
            reentry_handoff, reentry_fds = (
                fleet_rollout._active_rollout_reentry_handoff(
                    reentry_capability,
                    lifecycle_handoff=controller_lifecycle_handoff,
                )
            )
            rc = fleet_rollout.reenter_after_driver(
                str(reconciliation.driver_reentry_rollout_id),
                python=driver_prog.python,
                reentry_handoff=reentry_handoff,
                pass_fds=reentry_fds,
                as_json=as_json,
                selection=selection,
                reconcile_legacy=reconcile_legacy,
            )
            raise SystemExit(rc)
        reconciled_failed_hosts = {
            host: reason
            for _rollout_id, host, reason
            in reconciliation.failed_operation_hosts
        }
        if resume is not None and resume != current_rollout_id:
            raise fleet_rollout.FleetRolloutError(
                "accepted report changed during driver self-update: "
                f"expected rollout {resume}, discovered {current_rollout_id}; "
                "the exact recovered driver receipt was acknowledged; re-run "
                "rollout-latest to reconcile the newer report"
            )
        admin_snapshot, program_snapshot, doctor_snapshot = (
            _fleet_snapshots(cfg)
        )
        plan = fleet_rollout.build_plan(
            cfg,
            report,
            admin_status=admin_snapshot,
            programs=program_snapshot,
            doctor=doctor_snapshot,
            ancestry=ancestry,
            target_vq_tree_sha256=target_vq_tree_sha256,
        )
        driver_ssh = cfg.host(plan.driver).ssh
        if not is_local_host(driver_ssh):
            raise fleet_rollout.FleetRolloutError(
                f"this command must run on scheduler_driver {plan.driver!r} "
                f"(ssh={driver_ssh!r})"
            )

        fleet_rollout.assert_selection_in_plan(plan, selection)

        # Legacy journal supersession is itself a local state mutation.  It
        # therefore belongs behind both the actual-driver check and selection
        # validation, just like every later fleet mutation.  An invocation on
        # a workstation that merely has a copy of the driver's config must
        # never reconcile that workstation's similarly named state tree.
        legacy_reconciliation = fleet_rollout.LegacyRolloutReconciliation()
        failure_recovery_rows = tuple(
            dict.fromkeys(
                (
                    *reconciliation.failed_operation_hosts,
                    *reconciliation.pending_failure_transitions,
                )
            )
        )
        if (
            reconciliation.legacy_running_actions
            or reconciliation.legacy_scheduler_holds
            or reconciliation.legacy_hold_retries
            or reconciliation.pending_failure_transitions
            or (reconcile_legacy and reconciliation.failed_operation_hosts)
        ):
            if not reconcile_legacy:
                # Defensive: normal reconciliation cannot return these, but
                # keep the explicit authorization boundary at the caller too.
                raise fleet_rollout.FleetRolloutError(
                    "legacy failure transition or running rollout state "
                    "requires an explicit "
                    "--reconcile-legacy invocation"
                )
            failure_groups: list[tuple[tuple[str, str, str], ...]] = []
            failure_keys = {
                (rollout_id_value, host)
                for rollout_id_value, host, _reason in failure_recovery_rows
            }
            if failure_recovery_rows:
                for failure_host in sorted(
                    {host for _rollout_id, host, _reason in failure_recovery_rows}
                ):
                    failure_groups.append(
                        tuple(
                            row
                            for row in failure_recovery_rows
                            if row[1] == failure_host
                        )
                    )
            legacy_results = []
            for failure_group in failure_groups:
                group_keys = {
                    (rollout_id_value, host)
                    for rollout_id_value, host, _reason in failure_group
                }
                legacy_results.append(
                    fleet_rollout.reconcile_legacy_rollout_state(
                        legacy_running_actions=(),
                        legacy_scheduler_holds=(),
                        legacy_hold_retries=tuple(
                            item
                            for item in reconciliation.legacy_hold_retries
                            if item in group_keys
                        ),
                        failed_operation_hosts=failure_group,
                        accepted_report=report,
                        plan=plan,
                        admin_status=admin_snapshot,
                        repo=reports,
                        current_report_digest_resolver=current_report_digest,
                        control_runner=subprocess.run,
                    )
                )
            ordinary_hold_retries = tuple(
                item
                for item in reconciliation.legacy_hold_retries
                if item not in failure_keys
            )
            if (
                reconciliation.legacy_running_actions
                or reconciliation.legacy_scheduler_holds
                or ordinary_hold_retries
            ):
                legacy_results.append(
                    fleet_rollout.reconcile_legacy_rollout_state(
                        legacy_running_actions=(
                            reconciliation.legacy_running_actions
                        ),
                        legacy_scheduler_holds=(
                            reconciliation.legacy_scheduler_holds
                        ),
                        legacy_hold_retries=ordinary_hold_retries,
                        failed_operation_hosts=(),
                        accepted_report=report,
                        plan=plan,
                        admin_status=admin_snapshot,
                        repo=reports,
                        current_report_digest_resolver=current_report_digest,
                        control_runner=subprocess.run,
                    )
                )
            legacy_reconciliation = fleet_rollout.LegacyRolloutReconciliation(
                superseded_actions=tuple(
                    dict.fromkeys(
                        item
                        for result in legacy_results
                        for item in result.superseded_actions
                    )
                ),
                retained_actions=tuple(
                    dict.fromkeys(
                        item
                        for result in legacy_results
                        for item in result.retained_actions
                    )
                ),
                settled_inactive_holds=tuple(
                    dict.fromkeys(
                        item
                        for result in legacy_results
                        for item in result.settled_inactive_holds
                    )
                ),
                released_live_holds=tuple(
                    dict.fromkeys(
                        item
                        for result in legacy_results
                        for item in result.released_live_holds
                    )
                ),
                retained_holds=tuple(
                    dict.fromkeys(
                        item
                        for result in legacy_results
                        for item in result.retained_holds
                    )
                ),
                live_hold_state_changed=any(
                    result.live_hold_state_changed for result in legacy_results
                ),
                live_hold_refresh_required=any(
                    result.live_hold_refresh_required for result in legacy_results
                ),
            )
            if (
                legacy_reconciliation.live_hold_state_changed
                or legacy_reconciliation.live_hold_refresh_required
            ):
                if current_report_digest() != report.digest_sha256:
                    raise fleet_rollout.FleetRolloutError(
                        "accepted report changed during legacy hold "
                        "reconciliation; re-run rollout-latest from a fresh "
                        "plan"
                    )
                admin_snapshot, program_snapshot, doctor_snapshot = (
                    _fleet_snapshots(cfg)
                )
                plan = fleet_rollout.build_plan(
                    cfg,
                    report,
                    admin_status=admin_snapshot,
                    programs=program_snapshot,
                    doctor=doctor_snapshot,
                    ancestry=ancestry,
                    target_vq_tree_sha256=target_vq_tree_sha256,
                )
                driver_ssh = cfg.host(plan.driver).ssh
                if not is_local_host(driver_ssh):
                    raise fleet_rollout.FleetRolloutError(
                        "legacy hold reconciliation changed the plan driver; "
                        f"{plan.driver!r} is not local"
                    )
                fleet_rollout.assert_selection_in_plan(plan, selection)

        retained_legacy_holds = tuple(
            dict.fromkeys(
                (
                    *reconciliation.retained_legacy_holds,
                    *legacy_reconciliation.retained_holds,
                )
            )
        )
        retained_legacy_action_fences = tuple(
            (
                rollout_id,
                host,
                f"retained legacy action {action_id}: {reason}",
            )
            for rollout_id, action_id, host, reason
            in legacy_reconciliation.retained_actions
        )
        retained_legacy_fences = tuple(
            dict.fromkeys(
                (*retained_legacy_holds, *retained_legacy_action_fences)
            )
        )
        plan = fleet_rollout.fence_retained_legacy_holds(
            plan,
            retained_legacy_fences,
        )

        if reconcile_legacy:
            recovery_payload = {
                "schema": "vq.fleet.legacy_reconciliation/1",
                "status": (
                    "retained" if retained_legacy_fences else "reconciled"
                ),
                "rollout_id": current_rollout_id,
                "report": plan.report,
                "driver": plan.driver,
                "legacy_reconciliation": legacy_reconciliation.as_dict(),
                "retained_host_fences": [
                    {
                        "rollout_id": rollout_id,
                        "host": host,
                        "reason": reason,
                    }
                    for rollout_id, host, reason in retained_legacy_fences
                ],
                "next_step": (
                    "run `vq admin rollout-latest --dry-run` before an "
                    "ordinary rollout"
                ),
            }
            if as_json:
                click.echo(
                    json.dumps(recovery_payload, indent=2, sort_keys=True)
                )
            else:
                click.echo(
                    "rollout-latest legacy reconciliation "
                    f"{recovery_payload['status']}: "
                    f"{plan.report['release_tag']} "
                    f"(report {str(plan.report['digest_sha256'])[:12]})"
                )
                for item in legacy_reconciliation.retained_actions:
                    rollout_id_value, action_id, host, reason = item
                    click.echo(
                        "LEGACY ACTION RETAINED "
                        f"{rollout_id_value} {action_id} {host}: {reason}"
                    )
                for rollout_id_value, action_id in (
                    legacy_reconciliation.superseded_actions
                ):
                    click.echo(
                        "LEGACY ACTION SUPERSEDED "
                        f"{rollout_id_value} {action_id} "
                        "observed_outcome=unknown"
                    )
                for rollout_id_value, host in (
                    legacy_reconciliation.settled_inactive_holds
                ):
                    click.echo(
                        "LEGACY HOLD SETTLED INACTIVE "
                        f"{rollout_id_value} {host} "
                        "(authoritative absence confirmed)"
                    )
                for rollout_id_value, host in (
                    legacy_reconciliation.released_live_holds
                ):
                    click.echo(
                        "LEGACY LIVE HOLD RELEASED "
                        f"{rollout_id_value} {host} "
                        "(exact conditional owner identity)"
                    )
                for rollout_id_value, host, reason in (
                    legacy_reconciliation.retained_holds
                ):
                    click.echo(
                        "LEGACY HOLD RETAINED "
                        f"{rollout_id_value} {host}: {reason}"
                    )
                click.echo(str(recovery_payload["next_step"]))
            if retained_legacy_fences:
                raise SystemExit(2)
            return

        if verify_only:
            restricted = fleet_rollout.restrict_plan(plan, selection)
            # Probe each in-scope host's web console. Nothing in the
            # rollout knew the console existed, so `--verify-only` could
            # and did report `converged` while the coordinator served
            # 1081-commit-stale pages (2026-08-05). Hosts with no console
            # and hosts whose vq predates `vq web status` contribute
            # nothing to the verdict.
            consoles = fleet_rollout.collect_console_states(
                cfg,
                [
                    host
                    for host, resolved in restricted.topology.items()
                    if resolved.get("role") not in {"excluded", "unresolved"}
                ],
            )
            payload = fleet_rollout.verify_payload(
                plan=restricted,
                doctor=doctor_snapshot,
                rollout_id=current_rollout_id,
                selection=selection,
                consoles=consoles,
            )
            if as_json:
                click.echo(json.dumps(payload, indent=2, sort_keys=True))
            else:
                click.echo(fleet_rollout.render_verify_text(payload))
            if payload["verdict"] != "converged":
                raise SystemExit(2)
            return

        # Narrowing happens AFTER the plan is built from the accepted report, so
        # every surviving action still carries the report's exact pins and the
        # argv it would have had in an unscoped run. Selection never rewrites an
        # action, which is what keeps a single-host recovery on the pinned path.
        plan = fleet_rollout.select_hosts(plan, selection)
        driver_action = next(
            action for action in plan.actions if action.phase == "driver"
        )
        pending_driver_failure = (
            driver_action.host in reconciled_failed_hosts
        )
        retained_driver_hold = any(
            host == driver_action.host
            for _rollout_id, host, _reason in retained_legacy_fences
        )
        if not dry_run:
            fleet_rollout.abort_unselected_pre_authorization_operations(
                plan,
                failed_hosts=frozenset(reconciled_failed_hosts),
                abort_all=pending_driver_failure or retained_driver_hold,
                control_runner=subprocess.run,
                stream=sys.stderr,
                lifecycle_resources=controller_lifecycle_resources,
            )
        # Blocks are judged over the SELECTED hosts. A block on a host this
        # invocation was told to leave alone must not abort the run -- routing
        # around a blocked host is the main reason `--skip` exists, and a
        # blocked host that still aborts everything makes the flag useless
        # exactly when it is needed.
        in_scope = fleet_rollout.restrict_plan(
            plan,
            selection,
            keep_phases=fleet_rollout.SCOPE_EXEMPT_PHASES,
        )

        if dry_run:
            if as_json:
                payload = {
                    "schema": "vq.fleet.rollout_plan/2",
                    "rollout_id": current_rollout_id,
                    "selection": selection.as_dict(),
                    **plan.as_dict(),
                }
                if discovery_warning is not None:
                    payload["discovery_warnings"] = [discovery_warning]
                click.echo(json.dumps(payload, indent=2, sort_keys=True))
            else:
                click.echo(
                    fleet_rollout.render_plan_text(
                        plan,
                        title="rollout-latest dry run",
                    )
                )
            if in_scope.has_blocks:
                raise SystemExit(1)
            return

        if in_scope.has_blocks:
            details = "; ".join(in_scope.topology_errors)
            blocked = [
                f"{action.id}: {action.reason}"
                for action in in_scope.actions
                if action.decision == "block"
            ]
            raise fleet_rollout.FleetRolloutError(
                "rollout plan is blocked: "
                + "; ".join([part for part in (details, *blocked) if part])
            )

        if driver_action.decision == "update":
            recovered_driver_failure = reconciled_failed_hosts.get(
                driver_action.host
            )
            if recovered_driver_failure is not None:
                driver_failures = tuple(
                    failure
                    for failure in reconciliation.failed_operation_hosts
                    if failure[1] == driver_action.host
                )
                fleet_rollout.consume_reconciled_failure_fences(
                    driver_failures,
                    driver_block_host=driver_action.host,
                )
                raise fleet_rollout.FleetOperationFailed(
                    "the recovered durable driver update failed; refusing "
                    "same-invocation replay before a fresh controller run: "
                    f"{recovered_driver_failure}"
                )
            if resume is not None:
                raise fleet_rollout.FleetRolloutError(
                    "driver self-update returned without a verified target "
                    "identity; inspect `vq admin logs vibeqc-queue "
                    f"--host {plan.driver}` before retrying"
                )
            fleet_rollout.execute_one(
                plan,
                driver_action,
                rollout_id=current_rollout_id,
                report_digest_resolver=current_report_digest,
                durable_reconciled=True,
                lifecycle_handoff=controller_lifecycle_handoff,
                lifecycle_resources=controller_lifecycle_resources,
            )
            run = fleet_rollout.load_run(current_rollout_id)
            if run is None:
                raise fleet_rollout.FleetRolloutError(
                    "driver update completed without its durable rollout journal"
                )
            reentry_capability = fleet_rollout.driver_reentry_capability(
                run,
                action_id=driver_action.id,
            )
            reentry_handoff, reentry_fds = (
                fleet_rollout._active_rollout_reentry_handoff(
                    reentry_capability,
                    lifecycle_handoff=controller_lifecycle_handoff,
                )
            )
            rc = fleet_rollout.reenter_after_driver(
                current_rollout_id,
                python=driver_prog.python,
                reentry_handoff=reentry_handoff,
                pass_fds=reentry_fds,
                as_json=as_json,
                selection=selection,
                reconcile_legacy=reconcile_legacy,
            )
            raise SystemExit(rc)

        run = fleet_rollout.execute_plan(
            plan,
            rollout_id=current_rollout_id,
            control_runner=subprocess.run,
            scoped=selection.scoped,
            report_digest_resolver=current_report_digest,
            durable_reconciled=True,
            reconciled_failed_hosts=reconciled_failed_hosts,
            reconciled_failures=reconciliation.failed_operation_hosts,
            retain_scheduler_holds=True,
            lifecycle_handoff=controller_lifecycle_handoff,
            lifecycle_resources=controller_lifecycle_resources,
        )
        final_admin, final_programs, final_doctor = (
            _fleet_snapshots(cfg)
        )
        verification = fleet_rollout.build_plan(
            cfg,
            report,
            admin_status=final_admin,
            programs=final_programs,
            doctor=final_doctor,
            ancestry=ancestry,
            target_vq_tree_sha256=target_vq_tree_sha256,
        )
        verification = fleet_rollout.fence_retained_legacy_holds(
            verification,
            retained_legacy_fences,
        )
        run = fleet_rollout.reconcile_scheduler_parity_holds(
            plan,
            verification,
            doctor=final_doctor,
            run=run,
            runner=subprocess.run,
            selection=selection,
            report_digest_resolver=current_report_digest,
        )
        result = fleet_rollout.result_payload(
            initial=plan,
            verification=verification,
            doctor=final_doctor,
            run=run,
            selection=selection,
            retained_legacy_holds=retained_legacy_holds,
        )
        # A scoped invocation never claims fleet convergence. `complete`
        # means the entire fleet stands at the accepted report.
        requested_complete = (
            result["status"] == "complete" and not selection.scoped
        )
        run = fleet_rollout.finalize_run(
            run,
            complete=requested_complete,
            report_digest_resolver=current_report_digest,
        )
        journal_failed = bool(run.failed_hosts) or any(
            isinstance(value, dict)
            and value.get("status") in {"failed", "running"}
            for value in run.actions.values()
        )
        finalization_mismatch = requested_complete and not run.complete
        result = fleet_rollout.result_payload(
            initial=plan,
            verification=verification,
            doctor=final_doctor,
            run=run,
            selection=selection,
            retained_legacy_holds=retained_legacy_holds,
        )
        result["journal"] = run.as_dict()
        if reconcile_legacy:
            result["legacy_reconciliation"] = (
                legacy_reconciliation.as_dict()
            )
        try:
            result["drain_liveness"] = (
                fleet_rollout.collect_final_drain_liveness(
                    cfg,
                    topology=verification.topology,
                    run=run,
                    runner=subprocess.run,
                )
            )
        except Exception:  # noqa: BLE001 - final observation is additive only
            # The rollout result and its durable journal are already final.
            # Observation failure must not replace that primary outcome or leak
            # raw remote output into JSON/text; every host remains explicitly
            # unknown instead.
            result["drain_liveness"] = {
                "status": "unavailable",
                "observed_at": fleet_rollout.utcnow_iso(),
                "observed_hosts": [],
                "inactive_hosts": [],
                "active_holds": [],
                "unknown_hosts": [
                    {
                        "host": host,
                        "control_host": "unknown",
                        "reason": "final read-only drain sweep failed",
                    }
                    for host in sorted(cfg.hosts)
                ],
            }
        if as_json:
            click.echo(json.dumps(result, indent=2, sort_keys=True))
        else:
            click.echo(fleet_rollout.render_result_text(result))
        # A known terminal host failure still has a valid final live snapshot
        # and journal. Emit that one complete result before preserving the
        # established exit-1 execution verdict. Routing through the generic
        # exception envelope would discard failed_hosts and retained holds;
        # printing both would also make JSON stdout invalid. Live or
        # outcome-unknown operations fail before this final-snapshot boundary
        # and intentionally retain the error-envelope path above.
        # An unscoped attempt with an unreleased owned hold has not finalized
        # its execution. Preserve exit 1 even though the honest fleet status
        # now defers completion before finalize_run is asked to mark complete.
        owned_cleanup_pending = (
            not selection.scoped and bool(result["retained_rollout_holds"])
        )
        if journal_failed or finalization_mismatch or owned_cleanup_pending:
            raise SystemExit(1)
        # An unscoped run is judged on the fleet. A scoped run is judged on the
        # hosts it was asked about — otherwise a successful `--only workstation`
        # recovery would exit 2 because some other host is still behind, and the
        # operator could not tell that from workstation itself having failed. The
        # fleet-wide `status` stays in the payload either way.
        converged = (
            result["selection"]["verdict"] == "converged"
            if selection.scoped
            else result["status"] == "complete"
        )
        if not converged:
            raise SystemExit(2)
    except fleet_rollout.FleetProbeUnavailable as exc:
        # A gate that could not gather its evidence is a retry, not a
        # verdict, and it exits with the code that says so. Caught before the
        # general clause because it is a subclass of it.
        _emit_rollout_error(str(exc), as_json=as_json, outcome=exc.outcome)
        raise SystemExit(
            admin_module.ADMIN_OUTCOME_EXIT_CODES[exc.outcome]
        ) from None
    except (
        config.ConfigError,
        fleet_release.FleetReleaseError,
        fleet_rollout.FleetRolloutError,
    ) as exc:
        _emit_rollout_error(str(exc), as_json=as_json)
        raise SystemExit(1) from None
    finally:
        if lifecycle_fence is not None:
            lifecycle_fence.close()
        if rollout_fence is not None:
            rollout_fence.close()
        if inherited_reentry_fence is not None:
            inherited_reentry_fence.close()


class AdminOutcomeError(click.ClickException):
    """An admin verb's failure, classified for a caller.

    Carries one of :data:`admin_module.ADMIN_OUTCOMES` and exits with that
    outcome's code, so an orchestration can branch on the exit status alone
    and read ``outcome`` from ``--json`` when it needs the reason. Neither
    requires matching on the message, which is what every chain written
    during the 2026-09 migration had to do.
    """

    def __init__(self, outcome: str, message: str, *, as_json: bool) -> None:
        super().__init__(message)
        if outcome not in admin_module.ADMIN_OUTCOMES:
            raise ValueError(f"unknown admin outcome {outcome!r}")
        self.outcome = outcome
        self.exit_code = admin_module.ADMIN_OUTCOME_EXIT_CODES[outcome]
        self._as_json = as_json

    def show(self, file: object | None = None) -> None:
        if self._as_json:
            # stdout, so a --json caller parses one object whether the verb
            # succeeded or not.
            click.echo(
                json.dumps(
                    {"outcome": self.outcome, "error": self.format_message()},
                    indent=2,
                    sort_keys=True,
                )
            )
            return
        click.echo(f"Error [{self.outcome}]: {self.format_message()}", err=True)


def _classified_admin_error(
    exc: admin_module.AdminError, *, as_json: bool,
) -> AdminOutcomeError:
    """Re-raise an admin failure as its classification.

    The mapping lives on the exception types, so a new gate classifies itself
    by choosing a base class rather than by being added to a table here.
    """
    return AdminOutcomeError(exc.outcome, str(exc), as_json=as_json)


class _DetachedRunFailed(AdminOutcomeError):
    """A detached run finished with a failing outcome, carrying its recorded stdout.

    Raised rather than printed so the caller that owns stdout decides: a single
    host prints the result before the classification, while an ``--all-hosts
    --json`` fan-out must keep it out of the one document it composes.
    """

    def __init__(self, outcome: str, message: str, *, payload: str) -> None:
        super().__init__(outcome, message, as_json=False)
        self.payload = payload


def _relayed_admin_outcome(
    outcome: str, exc: transport.RemoteCommandError, *, as_json: bool,
) -> AdminOutcomeError:
    """A remote vq's classified failure, re-raised here with the same code.

    Under ``--json`` the remote printed ``{"outcome": ..., "error": ...}`` on
    its stdout; its ``error`` is relayed so the local object reads the same
    as a direct one would. Otherwise the transport's own message is kept,
    because it names the host and the command the way every other delegated
    failure does.
    """
    message = str(exc)
    if as_json:
        try:
            payload = json.loads(exc.stdout)
        except ValueError:
            payload = None
        if isinstance(payload, dict) and isinstance(payload.get("error"), str):
            message = payload["error"]
    return AdminOutcomeError(outcome, message, as_json=as_json)


def _pin_deploy_identity(
    program: str,
    *,
    expected_sha: str | None,
    expected_tag: str | None,
) -> tuple[str, str | None, str, str]:
    """Resolve one program's pinned deploy identity from the accepted report.

    The report already records the exact argv tail for every pin --
    ``release`` carries ``--tag vX.Y.Z --expected-sha SHA``, the others carry
    ``--expected-sha SHA`` -- and ``rollout-latest`` deploys straight from it.
    A single-host ``vq admin update`` made the operator retype the same thing,
    where omitting ``--tag`` for ``vibeqc-release`` fails inside the preparer
    rather than at the CLI.

    Returns ``(sha, tag, report version, report digest)``. Reads the pin's
    structured fields rather than re-parsing ``deploy_flags``, which the
    report loader has already required to be exactly those flags for that SHA
    and tag -- so this is the same argv, derived instead of typed.
    """
    pin_name = fleet_rollout.PROGRAM_PINS.get(program)
    if pin_name is None:
        raise click.UsageError(
            f"--from-report does not know a pin for {program!r}; the accepted "
            "report pins "
            + ", ".join(sorted(fleet_rollout.PROGRAM_PINS))
        )
    try:
        repo = fleet_release.report_repo()
        with admin_module.toolset_lifecycle_lock(
            [], action="vq-from-report",
            extra_resources=((
                "checkout", str(admin_module._canonical_lifecycle_checkout(repo)),
            ),),
        ):
            report = fleet_release.discover_latest_report(
                repo, runner=admin_module._mutating_git_run,
            )
        fleet_release.require_latest_report(report)
    except (fleet_release.FleetReleaseError, admin_module.AdminError) as exc:
        raise click.UsageError(f"--from-report: {exc}") from None
    pin = report.pins[pin_name]
    # An explicit flag that disagrees with the report is the mistake this
    # option exists to prevent, so it is refused rather than silently
    # overridden in either direction.
    if expected_sha is not None and expected_sha.strip().lower() != pin.sha:
        raise click.UsageError(
            f"--expected-sha {expected_sha} disagrees with the accepted "
            f"report, which pins {program} at {pin.sha}; drop the flag or "
            "drop --from-report"
        )
    if expected_tag is not None and expected_tag != pin.tag:
        raise click.UsageError(
            f"--tag {expected_tag} disagrees with the accepted report, which "
            f"pins {program} at {pin.tag or '(no tag)'}; drop the flag or "
            "drop --from-report"
        )
    version = ".".join(str(part) for part in report.release_version)
    return pin.sha, pin.tag, version, report.digest_sha256


@admin.command("install")
@click.argument("env", required=True)
@click.argument("host_arg", required=False)
@click.option(
    "--expected-sha",
    "expected_sha",
    default=None,
    metavar="SHA",
    help="Full 40-hex commit to clone at. Required, unless --from-report "
    "supplies it: a fleet host is provisioned at a pin, never at whatever a "
    "branch tip happens to be.",
)
@click.option(
    "--tag",
    "expected_tag",
    default=None,
    metavar="TAG",
    help="Assert the cloned commit is exactly this tag.",
)
@click.option(
    "--from-report",
    "from_report",
    is_flag=True,
    default=False,
    help="Take --expected-sha (and --tag) from the newest accepted fleet "
    "release report, as `vq admin update --from-report` does.",
)
@click.option(
    "--install-script-arg",
    "install_script_args",
    multiple=True,
    metavar="FLAG",
    help="Extra flag appended to the install_script invocation. Repeatable.",
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Overwrite an ordinary admin-update marker left by a dead updater. "
    "Never overwrites an existing checkout -- that is refused regardless.",
)
@click.option(
    "--json", "as_json", is_flag=True, default=False,
    help="Emit the UpdateResult-shaped object instead of the text summary.",
)
@click.option(
    "--show-output", "show_output", is_flag=True, default=False,
    help="On failure, print the captured install_script output tail to "
    "stderr in addition to the summary.",
)
@click.option("--token", "cli_token", default=None, metavar="TOKEN", hidden=True)
@click.option("--token-stdin", "token_stdin", is_flag=True, default=False)
@click.option(
    "--token-file", "token_file",
    type=click.Path(dir_okay=False), default=None, metavar="PATH",
)
def admin_install(
    env: str,
    host_arg: str | None,
    expected_sha: str | None,
    expected_tag: str | None,
    from_report: bool,
    install_script_args: tuple[str, ...],
    force: bool,
    as_json: bool,
    show_output: bool,
    cli_token: str | None,
    token_stdin: bool,
    token_file: str | None,
) -> None:
    """Clone ENV's upstream at a pinned commit and run its own installer.

    \b
      vq admin install vibeqc-dev build-host --expected-sha SHA
      vq admin install vibeqc-release pbs-cluster --from-report

    A host that does not yet have a program's ``git_dir`` could not be brought
    up through vq at all -- ``vq admin update`` refuses, and nothing clones. So
    every host had to be prepared by hand first, which is the step most likely
    to be done inconsistently.

    ENV must be a registered ``kind="venv"`` program carrying ``upstream`` and
    ``install_script``. HOST defaults to ``default_host``.

    It refuses to write into an existing non-empty ``git_dir``: an existing
    checkout is somebody's work, and refreshing one is what ``vq admin
    update`` is for. It does not drain or pause the queue, because a program
    whose checkout does not exist has no jobs bound to it and no interpreter
    for a running job to hold open.
    """
    try:
        cfg = config.load_config()
    except config.ConfigError as e:
        raise click.UsageError(str(e)) from None
    if from_report:
        expected_sha, expected_tag, report_version, report_digest = (
            _pin_deploy_identity(
                env, expected_sha=expected_sha, expected_tag=expected_tag,
            )
        )
        click.echo(
            f"from accepted report v{report_version} "
            f"({report_digest[:12]}): {env} "
            + (f"--tag {expected_tag} " if expected_tag else "")
            + f"--expected-sha {expected_sha}",
            err=True,
        )
    if expected_sha is None:
        raise click.UsageError(
            "--expected-sha is required (or --from-report): a fleet host is "
            "provisioned at a pinned commit, never at a moving branch tip"
        )
    expected_sha = expected_sha.strip().lower()
    if not re.fullmatch(r"[0-9a-f]{40}", expected_sha):
        raise click.UsageError("--expected-sha must be a full 40-hex SHA")
    host = _resolve_host(cfg, host_arg)
    token = _resolve_admin_token(
        cfg,
        command_label="admin install",
        cli_token=cli_token,
        token_stdin=token_stdin,
        token_file=token_file,
    )
    output_mod.enable_terminal_narration()
    if not is_local_host(host):
        remote_args = ["admin", "install", env, "--expected-sha", expected_sha]
        if expected_tag is not None:
            remote_args.extend(["--tag", expected_tag])
        for flag in install_script_args:
            remote_args.extend(["--install-script-arg", flag])
        if force:
            remote_args.append("--force")
        if as_json:
            remote_args.append("--json")
        if show_output:
            remote_args.append("--show-output")
        # --from-report is deliberately NOT forwarded: the report and its
        # runtime checkout live on the driver, and the flags above are what
        # it resolved to.
        click.echo(
            _forward_admin_command(
                host, cfg, remote_args, token=token, append_localhost=True,
            ),
            nl=False,
        )
        return
    try:
        result = admin_module.provision_env(
            env,
            cfg,
            host=host,
            expected_sha=expected_sha,
            expected_tag=expected_tag,
            force=force,
            install_script_args=list(install_script_args),
        )
    except admin_module.AdminError as e:
        raise click.UsageError(str(e)) from None
    if as_json:
        click.echo(admin_module.format_update_result_json(result))
    else:
        click.echo(admin_module.format_update_result(result))
    if show_output and not result.success and result.update_script_output:
        click.echo(result.update_script_output.rstrip(), err=True)
    if not result.success:
        raise click.ClickException(
            f"admin install {env} {host}: did not complete cleanly"
        )


@admin.command("update")
@click.argument("env_or_host", required=False)
@click.argument("host_if_env", required=False)
@click.option(
    "--all",
    "all_envs",
    is_flag=True,
    default=False,
    help='v0.5.28: refresh EVERY kind="venv" program in the registry '
    "(sorted by name), not just one. Mutually exclusive with a "
    "positional ENV and with --tag. Pause/resume bracket the whole "
    "batch — the queue is paused once, every env is pulled+built, "
    "then the queue resumes once. A failure in one env doesn't "
    "abort the rest; the batch verdict is all-or-nothing.",
)
@click.option(
    "--tag",
    "expected_tag",
    metavar="TAG",
    default=None,
    help="v0.5.24/v0.25.0: fetch exactly this named tag, resolve "
    "``refs/tags/TAG^{commit}``, and check out that peeled commit "
    "detached. vq verifies that the named ref still resolves to HEAD "
    "before rebuilding. The update script "
    "receives `--branch TAG` after any --update-script-arg flags, and "
    "vq verifies the exact tag again after the script returns. Missing, "
    "moved, or post-build-drifted tags fail closed. TAG is usually "
    "shaped like v0.X.Y. Use for the release-chat pattern: `vq admin "
    "update vibeqc-release --tag v0.8.0`. Not allowed with --all "
    "(different envs track different tags).",
)
@click.option(
    "--expected-sha",
    metavar="FULL_SHA",
    default=None,
    help="Pin a single managed venv update to this exact 40-hex commit, "
    "require this exact commit for scheduler-runtime deployment, or — "
    "with a scheduler-host argument — stage the scheduler vq HELPER from "
    "this exact commit of the managed runtime repository instead of the "
    "live driver checkout. Fleet rollouts always pin the helper to the "
    "accepted report's vq identity. "
    "The checkout is verified before and after the update script.",
)
@click.option(
    "--all-hosts",
    "all_hosts",
    is_flag=True,
    default=False,
    help="v0.5.37: run this admin update on EVERY host in "
    "~/.config/vq/config.toml. Sequential per-host (each host "
    "pauses its own jobs, updates, resumes — no cross-host "
    "interactions). Mutually exclusive with positional HOST. "
    "Composes with --all (every env on every host), with ENV "
    "(one env on every host), and with --tag (same tag verified "
    "on every host). Failure on one host doesn't abort the rest; "
    "the batch exit code is non-zero if ANY host failed.",
)
@click.option(
    "--no-restart-daemon",
    "no_restart_daemon",
    is_flag=True,
    default=False,
    help="v0.5.42: suppress daemon restart only for an environment that "
    "the service manager proves is NOT serving the current vq daemon. "
    "A serving self-target is rejected before pause, fetch, or checkout "
    "mutation; use `vq self-update`, whose success contract includes "
    "the required service restart and exact RPC/source provenance. Has "
    "no effect for an environment unrelated to vq's daemon.",
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="v0.5.44: overwrite ordinary admin-update markers and proceed "
    "only after independently verifying the previous updater is gone "
    "and the environment is safe to replace. Durable managed-daemon "
    "or paused-job receipts are never overwritten by --force; recover "
    "them with `vq admin recover-update` or the diagnosed pause-only "
    "clear flow.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="v0.5.46: emit JSON instead of the text summary. Single-env "
    "form returns one UpdateResult-shaped object (with the "
    "computed `success` field); --all returns a `results` array "
    "plus `batch_success` / `n_ok` / `n_total` / `failed_envs` "
    "summary. With --all-hosts, the output is a top-level object "
    "keyed by host; per-host failures appear as "
    '`{"error": "..."}` so one bad host doesn\'t break parsing.',
)
@click.option(
    "--token",
    "cli_token",
    default=None,
    metavar="TOKEN",
    help="v0.6.x: bearer token for admin operations in multi-user "
    "mode. DISCOURAGED — the token leaks into shell history and "
    "`ps -ef` argv. Prefer $VQ_TOKEN env var, --token-stdin, or "
    "--token-file. Generate with `vq web init-token`.",
)
@click.option(
    "--token-stdin",
    "token_stdin",
    is_flag=True,
    default=False,
    help="v0.6.46: read the bearer token from a single line on stdin. "
    "Use to keep the token out of `ps -ef` and shell history: "
    "`printf '%s\\n' \"$tok\" | vq admin update --token-stdin ...`. "
    "Mutually exclusive with --token / --token-file.",
)
@click.option(
    "--token-file",
    "token_file",
    type=click.Path(dir_okay=False),
    default=None,
    metavar="PATH",
    help="v0.6.46: read the bearer token from a 0600-mode file. "
    "Same perm enforcement as ~/.config/vq/web-token. Mutually "
    "exclusive with --token / --token-stdin.",
)
@click.option(
    "--update-script-arg",
    "update_script_args",
    multiple=True,
    metavar="FLAG",
    help="v0.7.1 *Lamport's Clock*: extra flag appended to the "
    "update_script's bash invocation. Repeatable. Examples: "
    "`--update-script-arg --recreate-venv --update-script-arg "
    "--dev`. The forwarded flags follow the script's "
    "config-side args, so the script's argv-parser sees them "
    "last (most parsers' 'last wins' rule applies on conflict). "
    "Lets operators ask for a clean venv rebuild without an SSH "
    "+ heredoc dance — the 2026-05-25 incident hit this three "
    "times on workstation. Also appended to a scheduler host's configured "
    "scheduler_update_command / scheduler_install_command. Forwarded "
    "transparently to remote hosts via --all-hosts. For a marked serving "
    "vq environment, profile changes require --recreate-venv and --extras "
    "PROFILE; the managed transaction preserves its target and install mode.",
)
@click.option(
    "--cluster-install",
    "cluster_install",
    is_flag=True,
    default=False,
    help="With a scheduler-host argument (e.g. `vq admin update pbs-cluster`), "
    "run scheduler_install_command instead of scheduler_update_command. "
    "Use for first provisioning; normal refreshes use the update command.",
)
@click.option(
    "--acknowledge-failed-marker",
    "acknowledge_failed_marker",
    is_flag=True,
    default=False,
    help="Acknowledge an in-scope marker whose previous run FAILED, then "
    "proceed -- the unattended form of `vq admin clear-update-marker`, "
    "writing the same durable receipt. Refuses a live or stale marker, and "
    "is not --force: it never overwrites a marker whose writer is alive and "
    "never touches a marker outside this update's scope.",
)
@click.option(
    "--from-report",
    "from_report",
    is_flag=True,
    default=False,
    help="Take --expected-sha (and --tag) for ENV from the newest accepted "
    "fleet release report, instead of retyping them. The report records the "
    "exact argv tail per pin and `rollout-latest` already deploys from it; "
    "this is the single-host form. ENV must be a pinned program "
    "(vibeqc-queue, vibeqc-release, vibeqc-dev, vibe-view). An explicit "
    "--expected-sha / --tag that disagrees with the report is refused rather "
    "than silently overridden.",
)
@click.option(
    "--show-output",
    "show_output",
    is_flag=True,
    default=False,
    help="v0.7.1 *Lamport's Clock* Item 6: on failure, print the "
    "captured update_script output tail to stderr immediately "
    "(in addition to the standard summary). Saves the round-trip "
    "of `vq admin status --verbose` after a failed update. "
    "No-op on success and under --json (the field is always in "
    "JSON output anyway).",
)
@click.option(
    "--serial/--parallel",
    "serial",
    default=True,
    show_default=True,
    help="With --all-hosts: update hosts one at a time (--serial, the "
    "default) or concurrently (--parallel). Serial is safer for rebuilds "
    "— a build break or a flaky link on one host won't fan out to all at "
    "once, and per-host output isn't interleaved. No effect without "
    "--all-hosts.",
)
@click.option(
    "--drain-wait",
    "drain_wait",
    metavar="DUR",
    default=None,
    help="SCHEDULER HOSTS ONLY: hold a drain lane for the target and wait "
    "up to DUR (e.g. 45m, 4h) for its already-submitted jobs to finish, "
    "instead of refusing immediately. The active-job guard is what protects "
    "live jobs from a mid-flight rebuild, but on a shared production node "
    "that is never idle it was unsatisfiable without a hand-built drain "
    "window. This is that window, supported. It does NOT weaken the guard: "
    "the rebuild still runs only once the target is genuinely quiet.",
)
@click.option(
    "--with-driver-runtime", is_flag=True, default=False,
    help="Run one pinned direct-host update using an integrity-checked copy "
    "of this driver. Requires --expected-sha; excludes scheduler drivers.",
)
@click.option(
    "--detach",
    "detach",
    is_flag=True,
    default=False,
    help="Run this update in its own session and return once it has started, "
    "printing a JSON receipt naming the run. The update's progress and exact "
    "outcome are then read with `vq admin observe-update RUN_ID`. A delegated "
    "`vq admin update ENV HOST` adds this to the command it sends, so a "
    "dropped SSH session can no longer kill a build mid-rebuild; you rarely "
    "need to pass it by hand. Only valid for an update on the host running it.",
)
@click.option("--detach-run-id", "detach_run_id", default=None, hidden=True)
@click.option("--detach-child", "detach_child", is_flag=True, default=False, hidden=True)
@click.option("--staged-driver-archive-sha256", default=None, hidden=True)
def admin_update(
    env_or_host: str | None,
    host_if_env: str | None,
    all_envs: bool,
    expected_tag: str | None,
    expected_sha: str | None,
    all_hosts: bool,
    no_restart_daemon: bool,
    force: bool,
    as_json: bool,
    cli_token: str | None,
    token_stdin: bool,
    token_file: str | None,
    update_script_args: tuple[str, ...],
    cluster_install: bool,
    acknowledge_failed_marker: bool,
    from_report: bool,
    show_output: bool,
    serial: bool,
    drain_wait: str | None,
    with_driver_runtime: bool,
    detach: bool,
    detach_run_id: str | None,
    detach_child: bool,
    staged_driver_archive_sha256: str | None,
) -> None:
    """Refresh ENV (or --all envs): pause queue, refresh Git, run update_script, resume.

    \b
    Forms:
      vq admin update ENV                       (default_host)
      vq admin update ENV HOST                  (explicit host)
      vq admin update ENV --tag v0.X.Y          (fetch + verify exact named tag)
      vq admin update --all                     (every venv env, default_host)
      vq admin update --all HOST                (every venv env, explicit host)
      vq admin update ENV --all-hosts           (one env, every host — v0.5.37)
      vq admin update --all --all-hosts         (every env, every host — v0.5.37)
      vq admin update SCHEDULER_HOST            (cluster provisioning command)
      vq admin update SCHEDULER_HOST --cluster-install
      vq admin update SCHEDULER_HOST --drain-wait 4h   (busy-node window)
      vq admin update ENV HOST --from-report     (pins from the report)

    ENV is the registry name of a ``kind = "venv"`` program (run
    ``vq programs`` to list registered envs). HOST defaults to
    ``default_host``.

    Typical chat workflow: commit + push from the laptop, then ask the
    queue host to refresh the env, then submit a job against the new
    commit:

    \b
        git push
        vq admin update vibeqc-dev
        vq submit my_test.py --branch main

    Release-chat workflow with ``--tag``:

    \b
        git push --tags
        vq admin update vibeqc-release --tag v0.8.0
        vq submit smoke_test.py --branch release

    Refresh everything (e.g. after a release that touches both the dev
    and release clones, or just to get a clean slate):

    \b
        vq admin update --all

    \b
    What it does, in order:
      1. Acquire an update marker and, on an in-place update path, record
         pause intent before pausing the exact affected running jobs. For
         ``--all`` the pause scope brackets the WHOLE batch.
      2. Refresh git (untagged: ``git pull``; tagged: fetch and check
         out the exact requested tag).
      3. v0.5.24/v0.25.0: if ``--tag`` was given, resolve the explicitly
         named ``refs/tags/TAG^{commit}`` and require that commit to equal
         HEAD. Mismatch = FAILED, update_script is skipped.
      4. If the env has an ``update_script`` configured (and Git refresh +
         tag check passed), run ``bash <git_dir>/<update_script>``.
         Tagged updates append ``--branch TAG`` last and re-verify
         the tag after the script returns.
      5. Resume and prove the exact token-scoped jobs paused by this
         invocation. A normal failure attempts that cleanup; a hard
         interruption retains the durable receipt and dispatch hold for
         explicit recovery rather than claiming that the queue resumed.

    DANGER -- pausing across a legacy in-place update is NOT safe on a venv
    host. A
    resumed job serves already-imported modules from ``sys.modules`` (old
    code) while any module it imports for the FIRST time after resume is
    read off disk (new code). That is two versions inside one process,
    with no error and no warning. Editable installs resolve directly into
    the checkout, and an in-place copied-package reinstall can likewise
    replace files that a resumed interpreter has not imported yet. Reproduced
    2026-07-26; see
    ``vibe-queue/docs/design_immutable_venv_runtimes.md`` §1.

    It is reachable, not theoretical: vibe-qc reaches its post-SCF
    property, population, QVF and output modules only after the SCF
    converges, which is the window a long job sits paused in. Affected
    rows are indistinguishable from good ones, so do NOT treat a
    paused-and-resumed job as evidence if an update crossed its lifetime.

    Configure ``runtime_slot_root`` for opt-in immutable per-SHA venv
    generations, or drain before using the legacy in-place path. Scheduler
    runtimes are immutable per-SHA bundles behind an atomically flipped
    pointer, so a running job keeps the bundle it started with.

    Only NEW dispatches after this call see the fresh build.

    \b
    Limitations:
      * Only ``kind = "venv"`` envs supported. ``binary`` programs
        (CRYSTAL, ORCA, Psi4) and ``import`` programs (PySCF) raise
        an error — they aren't git-backed.

    v0.5.44/v0.25.0: writes an admin-update marker before work and clears it
    only after terminal proof. An ordinary stale marker can be acknowledged
    with ``vq admin clear-update-marker`` or, after independent inspection,
    replaced by ``--force``. A pause-only receipt is cleared only after that
    command proves its exact pause scope resumed. A managed-daemon receipt
    retains the old checkout, virtualenv, service, and pause scope and must be
    reconciled with ``vq admin recover-update``; neither clear nor force can
    discard it.

    Exit code 0 only if every gate (Git refresh/checkout, tag check if --tag,
    update_script if configured) finished cleanly for every env. Non-zero
    exit prints a "FAILED" line with reasons.

    v0.5.37: ``--all-hosts`` extends the same per-host update across
    every host in config. Defaults to ``--serial`` (one host fully
    updated before the next) so a build break or a flaky link on one
    host doesn't fan out to all at once and per-host output isn't
    interleaved; pass ``--parallel`` to update hosts concurrently. Each
    host's queue is paused only during its own update window. Failure on
    one host doesn't abort the rest; the batch exit code is non-zero if
    any host failed.
    """
    driver_mode = with_driver_runtime or staged_driver_archive_sha256 is not None
    if driver_mode:
        if with_driver_runtime and staged_driver_archive_sha256 is not None:
            raise click.UsageError("internal staged-driver identity cannot be combined")
        if all_hosts:
            raise click.UsageError("--with-driver-runtime targets exactly one host")
        if all_envs:
            raise click.UsageError("--with-driver-runtime targets one managed environment")
        for flag, enabled in (
            ("--force", force), ("--no-restart-daemon", no_restart_daemon),
            ("--cluster-install", cluster_install), ("--drain-wait", drain_wait is not None),
        ):
            if enabled:
                raise click.UsageError(f"--with-driver-runtime cannot be combined with {flag}")
        if expected_sha is None:
            raise click.UsageError("--with-driver-runtime requires --expected-sha FULL_SHA")
        if staged_driver_archive_sha256 is not None:
            try:
                admin_module.require_staged_driver_recovery_archive(staged_driver_archive_sha256)
            except admin_module.AdminError as exc:
                raise click.UsageError(str(exc)) from None
    cfg = config.load_config()

    # Reject the detach combinations that later branches would silently drop.
    # A flag that is quietly ignored on some routes is worse than one that is
    # missing: the operator believes the build is protected when it is not.
    if detach or detach_child:
        if all_hosts:
            raise click.UsageError(
                "--detach runs one update on the host it is invoked on; "
                "--all-hosts walks every configured host from here"
            )
        if driver_mode:
            raise click.UsageError(
                "--detach cannot be combined with --with-driver-runtime, "
                "which carries its own integrity-checked driver copy"
            )

    # v0.5.37: --all-hosts is mutually exclusive with positional HOST.
    if all_hosts and host_if_env is not None:
        raise click.UsageError(
            "--all-hosts and HOST are mutually exclusive; --all-hosts "
            "walks every configured host, naming one in addition "
            "is contradictory"
        )

    # v0.5.28: disambiguate the positionals. With --all, the first
    # positional (if any) is HOST. Without --all, it's ENV [HOST].
    #
    # v0.5.37: with --all-hosts, the host positional is forbidden (above);
    # the first positional (if any) is ENV unless --all is also set.
    if all_envs:
        if expected_sha is not None:
            raise click.UsageError(
                "--all is mutually exclusive with --expected-sha "
                "(one SHA cannot apply to every managed env)"
            )
        if expected_tag is not None:
            raise click.UsageError(
                "--all is mutually exclusive with --tag (different envs "
                "track different tags; a single --tag can't apply to all)"
            )
        if host_if_env is not None:
            raise click.UsageError(
                "with --all, pass at most a HOST positional "
                "(`vq admin update --all [HOST]`), not `ENV HOST`"
            )
        env = None
        # With --all-hosts, the positional (if any) would be HOST,
        # but --all-hosts forbids that. So env_or_host must be None.
        if all_hosts and env_or_host is not None:
            raise click.UsageError(
                "with --all --all-hosts, no positional argument is "
                "allowed (every env on every host)"
            )
        host = env_or_host  # may be None → default_host (unused if --all-hosts)
    else:
        if env_or_host is None:
            raise click.UsageError(
                'ENV is required (the registry name of a kind="venv" '
                "program), or pass --all to refresh every venv env"
            )
        env = env_or_host
        host = host_if_env

    if from_report:
        if all_envs or all_hosts:
            raise click.UsageError(
                "--from-report deploys one pinned program to one host; it "
                "cannot be combined with --all or --all-hosts. Use "
                "`vq admin rollout-latest` to roll the whole fleet from the "
                "same report."
            )
        assert env is not None  # ENV is required on this branch
        expected_sha, expected_tag, report_version, report_digest = (
            _pin_deploy_identity(
                env, expected_sha=expected_sha, expected_tag=expected_tag,
            )
        )
        click.echo(
            f"from accepted report v{report_version} "
            f"({report_digest[:12]}): {env} "
            + (f"--tag {expected_tag} " if expected_tag else "")
            + f"--expected-sha {expected_sha}",
            err=True,
        )

    # Resolve before --all-hosts so every per-host closure sees the same local
    # credential; remote host token-file selection remains per target below.
    token = _resolve_admin_token(
        cfg,
        command_label="admin update",
        cli_token=cli_token,
        token_stdin=token_stdin,
        token_file=token_file,
    )

    if expected_sha is not None:
        expected_sha = expected_sha.strip().lower()
        if not re.fullmatch(r"[0-9a-f]{40}", expected_sha):
            raise click.UsageError("--expected-sha must be a full 40-hex SHA")

    # Live progress for the one verb family whose silence was the problem. An
    # admin update can run for hours; before this it printed nothing at all
    # until it finished. Narration goes to stderr, so `--json` stdout stays
    # machine-readable and pipes are unaffected. Band via VQ_OUTPUT_LEVEL.
    output_mod.enable_terminal_narration()
    drain_wait_seconds = 0.0
    if drain_wait is not None:
        try:
            drain_wait_seconds = parse_age(drain_wait).total_seconds()
        except ValueError as e:
            raise click.UsageError(f"--drain-wait: {e}") from None
        if drain_wait_seconds <= 0:
            raise click.UsageError("--drain-wait: duration must be positive")

    scheduler_runtime_target, scheduler_update_target = (
        _classify_admin_update_target(
            cfg,
            env=env,
            host=host,
            all_envs=all_envs,
            expected_sha=expected_sha,
        )
    )

    if driver_mode:
        if scheduler_runtime_target is not None:
            raise click.UsageError("--with-driver-runtime is not a scheduler-runtime update")
        if scheduler_update_target is not None:
            raise click.UsageError("--with-driver-runtime is not a scheduler-helper update")
        target = _resolve_host(cfg, host)
        if with_driver_runtime:
            if is_local_host(target):
                raise click.UsageError("--with-driver-runtime requires a non-local direct host")
            if any(value.scheduler_driver == target for value in cfg.hosts.values()):
                raise click.UsageError("--with-driver-runtime refuses a scheduler_driver")
            auth_args, remote_stdin = _remote_admin_auth(target, cfg, token)
            remote_env, remote_timeout = _remote_admin_update_contract(
                aggregate=False, host_cfg=_host_cfg_or_none(cfg, target),
            )
            _emit_remote_admin_timeout_summary(remote_env, remote_timeout)
            try:
                result = admin_module.update_remote_managed_env_with_driver_runtime(
                    cfg, target, env=env, expected_sha=expected_sha,
                    expected_tag=expected_tag, remote_auth_args=tuple(auth_args),
                    stdin_data=remote_stdin, as_json=as_json,
                    update_script_args=update_script_args, show_output=show_output,
                    remote_timeout_env=remote_env, timeout=remote_timeout,
                )
            except admin_module.AdminError as exc:
                raise click.UsageError(str(exc)) from None
            except transport.RemoteError as exc:
                raise click.ClickException(str(exc)) from None
            click.echo(result, nl=False)
            return
        if not is_local_host(target):
            raise click.UsageError("internal staged-driver identity requires localhost")
        if any(
            value.scheduler_driver is not None
            and (
                is_local_host(value.scheduler_driver)
                or value.scheduler_driver in cfg.hosts
                and is_local_host(cfg.hosts[value.scheduler_driver].ssh)
            )
            for value in cfg.hosts.values()
        ):
            raise click.UsageError("internal staged-driver update refuses a scheduler_driver")

    if (detach or detach_child) and (
        scheduler_runtime_target is not None or scheduler_update_target is not None
    ):
        raise click.UsageError(
            "--detach applies to a managed venv update; a scheduler lane "
            "already runs its command detached under its own protocol"
        )

    if scheduler_runtime_target is not None:
        if all_hosts or all_envs:
            raise click.UsageError(
                "scheduler runtime deployment targets one PROGRAM and one HOST"
            )
        if expected_sha is None:
            raise click.UsageError(
                "scheduler runtime deployment requires --expected-sha FULL_SHA"
            )
        if no_restart_daemon:
            raise click.UsageError(
                "--no-restart-daemon does not apply to scheduler runtimes"
            )
        target_cfg = cfg.host(scheduler_runtime_target)
        _require_managed_scheduler_update_target(
            scheduler_runtime_target, target_cfg,
        )
        driver = target_cfg.scheduler_driver
        if driver is None:
            raise click.UsageError(
                f"scheduler host {scheduler_runtime_target!r} has no "
                "scheduler_driver configured"
            )
        try:
            cfg.host(driver)
        except config.ConfigError as e:
            raise click.UsageError(
                f"scheduler host {scheduler_runtime_target!r} names driver "
                f"{driver!r}, which is not in config: {e}"
            ) from None

        if not is_local_host(driver):
            remote_args = [
                "admin",
                "update",
                env,
                scheduler_runtime_target,
                "--expected-sha",
                expected_sha,
            ]
            if expected_tag is not None:
                remote_args.extend(["--tag", expected_tag])
            if cluster_install:
                remote_args.append("--cluster-install")
            if force:
                remote_args.append("--force")
            if as_json:
                remote_args.append("--json")
            for sflag in update_script_args:
                remote_args.extend(["--update-script-arg", sflag])
            if show_output:
                remote_args.append("--show-output")
            if drain_wait is not None:
                remote_args.extend(["--drain-wait", drain_wait])
            click.echo(
                _forward_admin_command(
                    driver,
                    cfg,
                    remote_args,
                    token=token,
                    append_localhost=False,
                    reconcile_target=scheduler_runtime_target,
                    # The driver may now legitimately sit in a drain-wait before
                    # it starts building, so the outer SSH cap has to cover the
                    # wait as well as the build. Without this the operator would
                    # see an SSH timeout while the driver was waiting exactly as
                    # instructed — indistinguishable from a hang.
                    timeout=_remote_admin_timeout_with_drain_wait(
                        target_cfg.scheduler_runtime_deployments.get(
                            env
                        ).timeout_seconds
                        if env in target_cfg.scheduler_runtime_deployments
                        else target_cfg.scheduler_update_timeout_seconds,
                        drain_wait_seconds,
                    ),
                ),
                nl=False,
            )
            return

        try:
            runtime_result = admin_module.update_scheduler_runtime(
                scheduler_runtime_target,
                env,
                cfg,
                expected_sha=expected_sha,
                expected_tag=expected_tag,
                install=cluster_install,
                force=force,
                update_script_args=list(update_script_args),
                drain_wait_seconds=drain_wait_seconds,
            )
        except (
            admin_module.AdminUpdateInProgress,
            admin_module.AdminPreconditionFailed,
        ) as e:
            # Same classification as the env lane: the argv was valid and a
            # lock, a marker or a gate refused. Until this, the two pbs-cluster and
            # slurm-cluster helper lanes launched beside a developer-host build on
            # 2026-09-11 answered the held ownership lock with exit 1 and a
            # sentence, and the sweep stopped where it should have waited.
            raise _classified_admin_error(e, as_json=as_json) from None
        except admin_module.AdminError as e:
            raise click.UsageError(str(e)) from None
        if as_json:
            click.echo(
                admin_module.format_scheduler_runtime_update_result_json(
                    runtime_result
                )
            )
        else:
            click.echo(
                admin_module.format_scheduler_runtime_update_result(runtime_result)
            )
        if show_output and as_json and not runtime_result.success:
            # Prepare first, and unconditionally: a prepare failure is exactly
            # the case where the other two are empty.
            if runtime_result.prepare_output.strip():
                click.echo(runtime_result.prepare_output.rstrip(), err=True)
            click.echo(runtime_result.command_output.rstrip(), err=True)
            click.echo(runtime_result.verify_output.rstrip(), err=True)
        if not runtime_result.success:
            raise AdminOutcomeError(
                admin_module.OUTCOME_FAILED,
                f"admin update {env} {scheduler_runtime_target}: "
                "did not complete cleanly",
                # The result payload is already on stdout and carries
                # `outcome`; a second JSON object would trip a parser.
                as_json=False,
            )
        return

    if scheduler_update_target is not None:
        if all_hosts:
            raise click.UsageError(
                "--all-hosts updates venv programs across hosts; for a "
                "scheduler host update, omit --all-hosts"
            )
        if expected_tag is not None:
            raise click.UsageError(
                "--tag applies to venv program updates, not scheduler host updates"
            )
        if no_restart_daemon:
            raise click.UsageError(
                "--no-restart-daemon applies to venv self-updates, not "
                "scheduler host updates"
            )

        target_cfg = cfg.host(scheduler_update_target)
        _require_managed_scheduler_update_target(
            scheduler_update_target, target_cfg,
        )
        driver = target_cfg.scheduler_driver
        if driver is None:
            raise click.UsageError(
                f"scheduler host {scheduler_update_target!r} has no "
                "scheduler_driver configured"
            )
        try:
            cfg.host(driver)
        except config.ConfigError as e:
            raise click.UsageError(
                f"scheduler host {scheduler_update_target!r} names driver "
                f"{driver!r}, which is not in config: {e}"
            ) from None

        if not is_local_host(driver):
            remote_args = ["admin", "update", scheduler_update_target]
            if cluster_install:
                remote_args.append("--cluster-install")
            if expected_sha is not None:
                remote_args.extend(["--expected-sha", expected_sha])
            if force:
                remote_args.append("--force")
            if as_json:
                remote_args.append("--json")
            for sflag in update_script_args:
                remote_args.extend(["--update-script-arg", sflag])
            if show_output:
                remote_args.append("--show-output")
            if drain_wait is not None:
                remote_args.extend(["--drain-wait", drain_wait])
            click.echo(
                _forward_admin_command(
                    driver,
                    cfg,
                    remote_args,
                    token=token,
                    append_localhost=False,
                    reconcile_target=scheduler_update_target,
                    # See the runtime path above: the SSH cap must cover the
                    # drain-wait too, or a working wait looks like a hang.
                    timeout=_remote_admin_timeout_with_drain_wait(
                        target_cfg.scheduler_update_timeout_seconds,
                        drain_wait_seconds,
                    ),
                ),
                nl=False,
            )
            return

        try:
            sched_result = admin_module.update_scheduler_host(
                scheduler_update_target,
                cfg,
                install=cluster_install,
                force=force,
                update_script_args=list(update_script_args),
                drain_wait_seconds=drain_wait_seconds,
                expected_sha=expected_sha,
                admin_token=token,
            )
        except (
            admin_module.AdminUpdateInProgress,
            admin_module.AdminPreconditionFailed,
        ) as e:
            # See the runtime lane above.
            raise _classified_admin_error(e, as_json=as_json) from None
        except admin_module.AdminError as e:
            raise click.UsageError(str(e)) from None
        if as_json:
            click.echo(admin_module.format_scheduler_update_result_json(sched_result))
        else:
            click.echo(admin_module.format_scheduler_update_result(sched_result))
        if show_output and as_json and not sched_result.success and sched_result.command_output:
            click.echo(
                f"\n-- {sched_result.host}: scheduler {sched_result.mode} "
                f"output tail (rc={sched_result.command_rc}) --",
                err=True,
            )
            click.echo(sched_result.command_output.rstrip(), err=True)
        if not sched_result.success:
            raise AdminOutcomeError(
                admin_module.OUTCOME_FAILED,
                f"admin update {scheduler_update_target}: did not complete cleanly",
                # As above: the payload on stdout already carries `outcome`.
                as_json=False,
            )
        return
    if cluster_install:
        raise click.UsageError(
            "--cluster-install requires a scheduler host argument "
            "(for example: `vq admin update pbs-cluster --cluster-install`)"
        )
    if drain_wait is not None:
        # A venv env's update pauses the local queue and resumes it; there is no
        # scheduler lane to hold and no submitted cluster work to wait out.
        raise click.UsageError(
            "--drain-wait requires a scheduler host argument (for example: "
            "`vq admin update pbs-cluster --drain-wait 4h`). A venv env update pauses "
            "the local queue instead; see `vq drain --help` for local windows."
        )

    # v0.5.37: --all-hosts dispatch — sequential per-host update with
    # the same ENV / --all / --tag selection applied uniformly. Reuses
    # the existing single-host logic per iteration; output is stacked
    # under `==== HOST ====` banners via the same _aggregate_per_host
    # helper used by the v0.5.36 read commands. Failures are tracked
    # in a closure-captured list so the batch exits non-zero if any
    # host failed.
    if all_hosts:
        # Resolve once before fan-out. In --serial mode a local host can sort
        # before a remote one; in --parallel mode workers race. Per-worker
        # parsing could therefore mutate one host or start one SSH command
        # before another worker notices a malformed override.
        has_remote_update_target = any(
            not is_local_host(candidate)
            and _admin_fanout_scheduler_skip(
                cfg,
                candidate,
                as_json=as_json,
            )
            is None
            for candidate in cfg.hosts
        )
        if has_remote_update_target:
            fanout_remote_env, fanout_remote_timeout = (
                _remote_admin_update_contract(aggregate=all_envs)
            )
        else:
            fanout_remote_env = None
            fanout_remote_timeout = _DELEGATE_TIMEOUT_UNSET
        if fanout_remote_env is not None:
            assert fanout_remote_timeout is not _DELEGATE_TIMEOUT_UNSET
            _emit_remote_admin_timeout_summary(
                fanout_remote_env,
                fanout_remote_timeout,
            )
        failures: list[str] = []

        def _update_one(h: str) -> str:
            skipped = _admin_fanout_scheduler_skip(cfg, h, as_json=as_json)
            if skipped is not None:
                return skipped
            if is_local_host(h):
                if all_envs:
                    try:
                        results = admin_module.update_all(
                            cfg,
                            host=h,
                            admin_token=token,
                            restart_daemon=not no_restart_daemon,
                            force=force,
                            update_script_args=list(update_script_args),
                        )
                    except admin_module.AdminError as e:
                        failures.append(h)
                        raise click.ClickException(str(e)) from None
                    if as_json:
                        if not all(r.success for r in results):
                            failures.append(h)
                        return admin_module.format_update_all_results_json(results)
                    output = admin_module.format_update_all_results(results)
                    if not all(r.success for r in results):
                        failures.append(h)
                        n_ok = sum(1 for r in results if r.success)
                        return f"{output}\n\nFAILED: {n_ok}/{len(results)} envs OK on {h}"
                    return output
                assert env is not None
                try:
                    result = admin_module.update_env(
                        env,
                        cfg,
                        host=h,
                        admin_token=token,
                        expected_tag=expected_tag,
                        expected_sha=expected_sha,
                        restart_daemon=not no_restart_daemon,
                        force=force,
                        update_script_args=list(update_script_args),
                    )
                except admin_module.AdminError as e:
                    failures.append(h)
                    raise click.ClickException(str(e)) from None
                if as_json:
                    if not result.success:
                        failures.append(h)
                    return admin_module.format_update_result_json(result)
                output = admin_module.format_update_result(result)
                if not result.success:
                    failures.append(h)
                    return f"{output}\n\nFAILED on {h}"
                return output
            # Remote: delegate to the host's own vq.
            remote_args: list[str] = ["admin", "update"]
            if env is not None:
                remote_args.append(env)
            if all_envs:
                remote_args.append("--all")
            if expected_tag is not None:
                remote_args.extend(["--tag", expected_tag])
            if expected_sha is not None:
                remote_args.extend(["--expected-sha", expected_sha])
            if no_restart_daemon:
                remote_args.append("--no-restart-daemon")
            if force:
                remote_args.append("--force")
            if as_json:
                remote_args.append("--json")
            # v0.7.1: forward each --update-script-arg flag verbatim.
            # Each FLAG becomes one ``--update-script-arg FLAG`` pair
            # on the remote argv, preserving order so the script's
            # parser sees them in the same sequence the operator
            # typed locally.
            for sflag in update_script_args:
                remote_args.extend(["--update-script-arg", sflag])
            # v0.7.1 Item 6: forward --show-output so the remote vq
            # surfaces the failure tail in its local output (which
            # we capture + relay back via _delegate_to_remote).
            if show_output:
                remote_args.append("--show-output")
            # v0.6.46: forward the token via stdin, not argv. Pre-v0.6.46
            # this passed --token TOKEN as a remote argv element, so the
            # token surfaced in the local `ps -ef` (the ssh command line)
            # and in the remote sh -c command line (which is exactly the
            # argv exposure audit #3 flagged). --token-stdin keeps it
            # off both argv surfaces — only the in-memory pipe carries
            # it across the SSH tunnel.
            try:
                # A lost SSH response is ambiguous after this write starts.
                # One attempt only; status/live-state reconciliation decides
                # whether another update is safe.
                # The fan-out validated the overrides once, above; each host
                # still gets its own configured wall cap (#32).
                host_remote_env, host_remote_timeout = _remote_admin_update_contract(
                    aggregate=all_envs, host_cfg=_host_cfg_or_none(cfg, h),
                )
                if host_remote_env != fanout_remote_env:
                    _emit_remote_admin_timeout_summary(
                        host_remote_env, host_remote_timeout,
                    )
                return _forward_venv_admin_update(
                    h,
                    cfg,
                    remote_args,
                    token=token,
                    timeout=host_remote_timeout,
                    remote_env=host_remote_env,
                ).rstrip()
            except click.UsageError:
                # Auth/config lookup historically happened outside this try.
                # Let the fanout envelope render it without reclassifying the
                # host as a failed remote update.
                raise
            except click.ClickException:
                failures.append(h)
                raise

        if as_json:
            # v0.5.46: aggregate per-host JSON into one top-level
            # object keyed by host (same shape as `admin status --all
            # --json`). Per-host failures surface as {"error": ...}
            # so one bad host doesn't break parsing.
            # v0.7.6: fan-out in parallel via _aggregate_per_host_json.
            # The closure-captured ``failures`` list still tracks which
            # hosts failed (list.append is thread-safe; the post-loop
            # set check below tolerates non-deterministic append order).
            import json as _json

            payload = _aggregate_per_host_json(cfg, _update_one, parallel=not serial)
            click.echo(_json.dumps(payload, indent=2, sort_keys=True))
            if failures:
                raise click.ClickException(
                    f"admin update --all-hosts: {len(failures)} host(s) "
                    f"failed ({', '.join(sorted(failures))})"
                )
            return
        click.echo(_aggregate_per_host(cfg, _update_one, parallel=not serial))
        if failures:
            raise click.ClickException(
                f"admin update --all-hosts: {len(failures)} host(s) "
                f"failed ({', '.join(sorted(failures))})"
            )
        return

    host = _resolve_host(cfg, host)

    def _maybe_show_output_tail(results: list) -> None:
        """v0.7.1 Item 6: when --show-output is set and an update
        failed, print a clearly-flagged tail header + the captured
        script output to stderr.

        Works in both text and --json mode. In text mode, the
        standard format_update_result block already prints the
        output once; this adds a second tail-header copy below so
        the operator's eye lands on it. In --json mode, the output
        lives only in the JSON field on stdout (machine-readable);
        --show-output emits the human-readable tail to stderr so
        an interactive operator gets immediate failure context
        without having to parse the JSON.

        No-op on success or on envs that captured no output."""
        if not show_output:
            return
        for r in results:
            if r.success or not r.update_script_output:
                continue
            click.echo(
                f"\n-- {r.env}: update_script output tail (rc={r.update_script_rc}) --",
                err=True,
            )
            click.echo(r.update_script_output.rstrip(), err=True)

    def _run_local_update() -> None:
        """Perform the update here, exactly as an attached invocation would.

        Named rather than inlined so the detached child can run the identical
        work and capture its stdout and outcome: the driver then prints the
        same payload and exits with the same status it always did, which is
        the only way detaching stays invisible to every existing caller.
        """
        if all_envs:
            try:
                results = admin_module.update_all(
                    cfg,
                    host=host,
                    admin_token=token,
                    restart_daemon=not no_restart_daemon,
                    force=force,
                    update_script_args=list(update_script_args),
                )
            except (
                admin_module.AdminUpdateInProgress,
                admin_module.AdminPreconditionFailed,
            ) as e:
                # A prior update left its marker, or a gate refused — the
                # argv was valid either way, so this is a runtime/state
                # condition and NOT a usage error. It carries its own
                # outcome and exit code, so a batch driven by a script
                # branches the same way a single-env one does rather than
                # reading the misleading `Usage: vq admin update …` banner.
                raise _classified_admin_error(e, as_json=as_json) from None
            except admin_module.AdminError as e:
                raise click.UsageError(str(e)) from None
            if as_json:
                click.echo(admin_module.format_update_all_results_json(results))
            else:
                click.echo(admin_module.format_update_all_results(results))
            _maybe_show_output_tail(results)
            if not all(r.success for r in results):
                n_ok = sum(1 for r in results if r.success)
                raise AdminOutcomeError(
                    admin_module.OUTCOME_FAILED,
                    f"admin update --all: {n_ok}/{len(results)} envs "
                    "completed cleanly",
                    # See above: the results payload is already on stdout.
                    as_json=False,
                )
        else:
            assert env is not None  # guaranteed by the branch above
            try:
                result = admin_module.update_env(
                    env,
                    cfg,
                    host=host,
                    admin_token=token,
                    expected_tag=expected_tag,
                    expected_sha=expected_sha,
                    restart_daemon=not no_restart_daemon,
                    force=force,
                    update_script_args=list(update_script_args),
                    acknowledge_failed_marker=acknowledge_failed_marker,
                )
            except (
                admin_module.AdminUpdateInProgress,
                admin_module.AdminPreconditionFailed,
            ) as e:
                # The operational classes carry their own outcome, so an
                # orchestration branches on the exit code (or `outcome` under
                # --json) rather than on the sentence.
                raise _classified_admin_error(e, as_json=as_json) from None
            except admin_module.AdminError as e:
                # A plain AdminError here is a wrong argv -- unknown env,
                # wrong program kind. That is not one of the outcomes: the
                # operation never started and no host state is implied. Click's
                # UsageError (exit 2) is the universal spelling for it, and
                # callers already know it.
                raise click.UsageError(str(e)) from None
            if as_json:
                click.echo(admin_module.format_update_result_json(result))
            else:
                click.echo(admin_module.format_update_result(result))
            _maybe_show_output_tail([result])
            if not result.success:
                raise AdminOutcomeError(
                    admin_module.OUTCOME_FAILED,
                    f"admin update {env}: did not complete cleanly",
                    # Prose on stderr, never a second JSON object: the result
                    # payload is already on stdout and already carries
                    # `outcome`, so emitting another document here would leave
                    # two JSON values on one stream for a caller to trip over.
                    as_json=False,
                )

    if is_local_host(host):
        _export_host_update_script_timeout(cfg, host)
    if detach or detach_child:
        if not is_local_host(host):
            raise click.UsageError(
                "--detach runs the update on the host it is invoked on; the "
                "delegating driver adds it to the command it sends, so it "
                "cannot also name a remote HOST"
            )
        multi_user = _multi_user_active(cfg)
        if detach_child:
            # This process IS the detached updater. The launcher started it
            # outside the ssh session -- a transient user unit, or a session of
            # its own -- so that session ending does not end this build.
            if detach_run_id is None:
                raise click.UsageError(
                    "--detach-child requires the --detach-run-id it must publish under"
                )
            try:
                admin_detached.validate_run_id(detach_run_id)
            except admin_detached.DetachedRunError as e:
                raise click.UsageError(str(e)) from None
            raise SystemExit(
                admin_module.run_detached_update_child(
                    detach_run_id,
                    _run_local_update,
                    multi_user=multi_user,
                )
            )
        run_id = detach_run_id or admin_detached.new_run_id()
        try:
            admin_detached.validate_run_id(run_id)
        except admin_detached.DetachedRunError as e:
            raise click.UsageError(str(e)) from None
        try:
            receipt = admin_module.launch_detached_update(
                run_id=run_id,
                target=env or "all-envs",
                child_argv=_detached_update_child_argv(
                    run_id,
                    env=env,
                    all_envs=all_envs,
                    expected_tag=expected_tag,
                    expected_sha=expected_sha,
                    no_restart_daemon=no_restart_daemon,
                    force=force,
                    as_json=as_json,
                    acknowledge_failed_marker=acknowledge_failed_marker,
                    update_script_args=update_script_args,
                    show_output=show_output,
                ),
                token=token,
                multi_user=multi_user,
            )
        except admin_module.AdminError as e:
            raise click.ClickException(str(e)) from None
        click.echo(json.dumps(receipt, indent=2, sort_keys=True))
        return

    if is_local_host(host):
        _run_local_update()
    else:
        # Delegate to the remote vq, which will read its own
        # [programs.X] (the registry lives where the binaries do) and
        # run pause/resume against its own queue.
        remote_timeout_env, remote_outer_timeout = (
            _remote_admin_update_contract(
                aggregate=all_envs, host_cfg=_host_cfg_or_none(cfg, host),
            )
        )
        if all_envs:
            # As above, a remote --all batch has no locally knowable N-build
            # total. Keep the SSH observer unbounded and rely on each remote
            # script's forwarded watchdogs plus SSH server-alive detection.
            remote_args: list[str] = ["admin", "update", "--all"]
        else:
            assert env is not None
            remote_args = ["admin", "update", env]
            if acknowledge_failed_marker:
                remote_args.append("--acknowledge-failed-marker")
            if expected_tag is not None:
                remote_args.extend(["--tag", expected_tag])
            if expected_sha is not None:
                remote_args.extend(["--expected-sha", expected_sha])
        _emit_remote_admin_timeout_summary(
            remote_timeout_env,
            remote_outer_timeout,
        )
        if no_restart_daemon:
            remote_args.append("--no-restart-daemon")
        if force:
            remote_args.append("--force")
        if as_json:
            remote_args.append("--json")
        # v0.7.1: forward each --update-script-arg verbatim. Same
        # contract as the --all-hosts forwarder above (the host
        # receives one --update-script-arg FLAG pair per local
        # invocation, in order).
        for sflag in update_script_args:
            remote_args.extend(["--update-script-arg", sflag])
        # v0.7.1 Item 6: forward --show-output to the remote.
        if show_output:
            remote_args.append("--show-output")
        # The remote updater runs in its own session and publishes its own
        # outcome, so a dropped transport costs a poll rather than a build.
        # A launch response that still goes missing is adoptable by run id;
        # only a run that vanishes without a receipt is reported as unknown.
        try:
            delegated = _forward_venv_admin_update(
                host,
                cfg,
                remote_args,
                token=token,
                timeout=remote_outer_timeout,
                remote_env=remote_timeout_env,
            )
        except _DetachedRunFailed as failed:
            # One host owns this stdout, so print the recorded result before
            # the classification, exactly as a local update does.
            if failed.payload:
                click.echo(failed.payload, nl=False)
            raise
        click.echo(delegated, nl=False)


@admin.command("status")
@click.argument("host", required=False)
@click.option(
    "--all",
    "--all-hosts",
    "all_hosts",
    is_flag=True,
    default=False,
    help="v0.5.36: aggregate admin status across EVERY host in "
    "~/.config/vq/config.toml. Output is stacked per-host (banner "
    "+ table for each). Useful for the 'is everything at the "
    "commit I just pushed?' question across the whole fleet. "
    "Mutually exclusive with positional HOST. v0.12.1: "
    "``--all-hosts`` is accepted as an alias. Read verbs (doctor, "
    "programs, queue, admin status) spell this ``--all``; write "
    "fan-outs (admin update, admin auto-update, drain, fetch-all) "
    "spell it ``--all-hosts``. Mid-rollout that distinction is a "
    "trap, so the read verb tolerates the write verb's spelling "
    "rather than failing a verification sweep on a flag name.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="v0.5.46: emit JSON instead of the text table. Per-env "
    "records flatten EnvStatus + AdminUpdateRecord fields; "
    "admin-update-in-progress marker (when present) is included "
    "as a top-level `marker` block (null otherwise). With --all, "
    "the output is a top-level object keyed by host name; "
    'per-host failures appear as `{"error": "..."}` so one '
    "bad host doesn't break parsing of the rest.",
)
@click.option(
    "--verbose",
    "-v",
    "verbose",
    is_flag=True,
    default=False,
    help="v0.7.1 *Lamport's Clock*: append per-failing-env update-"
    "script output tails after the main table. The tails are the "
    "last 80 lines (env-tunable via VQ_ADMIN_UPDATE_OUTPUT_LINES) "
    "of the failed update's stdout+stderr, persisted in admin-"
    "status.json at update time. Use when LAST OK=False and you "
    "want the 'why' without SSHing to the host. No-op for envs "
    "that succeeded or didn't capture output. Ignored with --json "
    "(the field is always present in JSON output).",
)
def admin_status(
    host: str | None,
    all_hosts: bool,
    as_json: bool,
    verbose: bool,
) -> None:
    """Show tip SHAs + last-update times for every registered venv env.

    \b
    Forms:
      vq admin status              (default_host)
      vq admin status HOST         (explicit host)
      vq admin status --all        (every configured host, v0.5.36)
      vq admin status --all-hosts  (alias for --all, v0.12.1)

    \b
    Output columns:
      NAME             registry name (matches [programs.X])
      BRANCH           branch the env tracks (from config; "-" if unset)
      SHA              short SHA of HEAD (12 hex chars; live `git rev-parse`)
      VERSION          pyproject [project] version when available; otherwise
                       git describe --tags --always
      DIRTY            yes/no: uncommitted changes in the working tree
      LAST_UPDATED_AT  ISO timestamp of the most recent `vq admin update`
                       on this env ("never" if none seen)
      LAST OK          True/False: success of that most recent update

    Useful for the "is workstation at the commit I just pushed?" question:
    SHA should match `git rev-parse --short=12 HEAD` on the laptop.
    Mismatch means somebody (or something) hasn't run
    `vq admin update <env>` since your push.

    Only ``kind = "venv"`` programs appear; binary and import programs
    aren't git-backed.

    v0.5.36: ``--all`` walks every configured host. Per-host failures
    (unreachable, config error, SSH timeout) show inline so one bad
    host doesn't hide the rest of the fleet.
    """
    cfg = config.load_config()

    if all_hosts and host is not None:
        raise click.UsageError(
            "--all/--all-hosts and HOST are mutually exclusive; --all walks "
            "every configured host, so naming one in addition is contradictory"
        )

    def _query_one(h: str) -> str:
        """v0.5.36: per-host admin-status rendering (text).
        v0.7.1: forwards ``--verbose`` to remote hosts so multi-host
        aggregate output also surfaces failure tails when asked."""
        scheduler_driver = _scheduler_driver_host(cfg, h)
        if scheduler_driver is not None:
            if is_local_host(scheduler_driver):
                return admin_module.format_scheduler_runtime_status(h, cfg)
            return _delegate_to_remote(
                scheduler_driver, cfg, "admin", "status", h
            ).rstrip()
        if is_local_host(h):
            return admin_module.format_admin_status(cfg, verbose=verbose)
        remote_args = ["admin", "status"]
        if verbose:
            remote_args.append("--verbose")
        remote_args.append("localhost")
        return _delegate_to_remote(h, cfg, *remote_args).rstrip()

    def _query_one_json(h: str) -> str:
        """v0.5.46: per-host admin-status JSON. Returns the raw JSON
        string (so the caller can either emit it directly for the
        single-host case or parse it for --all aggregation)."""
        scheduler_driver = _scheduler_driver_host(cfg, h)
        if scheduler_driver is not None:
            if is_local_host(scheduler_driver):
                return admin_module.format_scheduler_runtime_status_json(h, cfg)
            return _delegate_to_remote(
                scheduler_driver, cfg, "admin", "status", h, "--json"
            ).rstrip()
        if is_local_host(h):
            return admin_module.format_admin_status_json(cfg)
        return _delegate_to_remote(
            h,
            cfg,
            "admin",
            "status",
            "--json",
            "localhost",
        ).rstrip()

    if all_hosts:
        if as_json:
            # v0.5.46: aggregate per-host JSON into one top-level
            # object keyed by host. Per-host failures surface as
            # {"error": "..."} so the rest of the fleet still parses.
            # Read-only queries fan out in the bounded v0.7.6 pool
            # (max 8 workers, VQ_FANOUT_SERIAL opt-out) — the serial
            # sweep was a major cost of `rollout-latest --dry-run`.
            import json as _json

            def _status_payload(h: str) -> object:
                try:
                    return _json.loads(_query_one_json(h))
                except click.ClickException as e:
                    return {"error": str(e.message)}
                except _json.JSONDecodeError as e:
                    return {"error": f"remote returned invalid JSON: {e}"}
                except Exception as e:  # noqa: BLE001 — per-host isolation
                    return {"error": str(e)}

            host_names = sorted(cfg.hosts.keys())
            if len(host_names) <= 1 or _fanout_serial_requested():
                payload = {h: _status_payload(h) for h in host_names}
            else:
                from concurrent.futures import ThreadPoolExecutor

                with ThreadPoolExecutor(
                    max_workers=_fanout_max_workers(len(host_names))
                ) as pool:
                    payload = dict(
                        zip(
                            host_names,
                            pool.map(_status_payload, host_names),
                            strict=True,
                        )
                    )
            click.echo(_json.dumps(payload, indent=2, sort_keys=True))
            return
        click.echo(_aggregate_per_host(cfg, _query_one))
        return

    host = _resolve_host(cfg, host)
    if is_local_host(host):
        if as_json:
            click.echo(admin_module.format_admin_status_json(cfg))
        else:
            click.echo(admin_module.format_admin_status(cfg, verbose=verbose))
    else:
        scheduler_driver = _scheduler_driver_host(cfg, host)
        if scheduler_driver is not None:
            if is_local_host(scheduler_driver):
                if as_json:
                    click.echo(
                        admin_module.format_scheduler_runtime_status_json(host, cfg)
                    )
                else:
                    click.echo(admin_module.format_scheduler_runtime_status(host, cfg))
                return
            delegate_args = ["admin", "status", host]
            if as_json:
                delegate_args.append("--json")
            click.echo(
                _delegate_to_remote(scheduler_driver, cfg, *delegate_args),
                nl=False,
            )
            return
        delegate_args = ["admin", "status"]
        if as_json:
            delegate_args.append("--json")
        if verbose:
            # v0.7.1: forward --verbose to the remote so the
            # surfaced output tails come from the host that
            # actually owns the failed env's record.
            delegate_args.append("--verbose")
        delegate_args.append("localhost")
        click.echo(
            _delegate_to_remote(host, cfg, *delegate_args),
            nl=False,
        )


@admin.command("logs")
@click.argument("target", required=False)
@click.option(
    "--host",
    "host",
    default=None,
    metavar="HOST",
    help="Host whose transcripts to read (default: default_host). A scheduler "
    "host routes to its driver, where the transcript actually lives.",
)
@click.option(
    "--list",
    "list_only",
    is_flag=True,
    default=False,
    help="List available transcripts instead of printing one.",
)
@click.option(
    "--tail",
    "tail_lines",
    type=click.IntRange(min=1),
    default=None,
    metavar="N",
    help="Print only the last N lines. Default: the whole transcript.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Emit {path, lines, text} (or the listing) as JSON.",
)
def admin_logs(
    target: str | None,
    host: str | None,
    list_only: bool,
    tail_lines: int | None,
    as_json: bool,
) -> None:
    """Read the transcript of a `vq admin update`.

    \b
    Forms:
      vq admin logs                      (newest transcript, this host)
      vq admin logs vibeqc-dev           (newest for that env)
      vq admin logs vibeqc-dev compute-a      (on another host, via SSH)
      vq admin logs pbs-cluster pbs-cluster            (a scheduler helper update)
      vq admin logs --list               (what is available)
      vq admin logs vibeqc-dev --tail 80

    TARGET is the env name for a venv update, `<host>` for a scheduler
    helper update, or `<host>-<program>` for a scheduler runtime
    deployment — the same string the update reported as its
    `run_log_path`.

    \b
    Why this exists:
      An update's full output used to live only in memory. It was cut
      to its last 80 lines into admin-status.json and the rest was
      dropped when the process exited; the scheduler lanes kept
      nothing at all. Diagnosing a failed two-hour pbs-cluster or slurm-cluster
      deploy meant re-running it. Each update now writes a timestamped
      transcript — phase narration, every heartbeat, and the complete
      build output — and this is how you get it back, including from
      a host you are not logged into.

    Transcripts live under `<state_root>/admin-updates/` and are
    pruned to the newest 20 per target.
    """
    cfg = config.load_config()
    resolved = _resolve_host(cfg, host)
    scheduler_driver = _scheduler_driver_host(cfg, resolved)
    # Same routing rule as clear-update-marker: a scheduler host's update runs
    # on its driver, so its transcript is written there.
    log_owner = scheduler_driver if scheduler_driver is not None else resolved
    if not is_local_host(log_owner):
        # HOST is an OPTION, not a second positional, precisely so this
        # delegation is unambiguous. With two positionals, a call that omitted
        # TARGET sent "localhost" into the TARGET slot, so a host whose
        # default_host is remote always reported zero transcripts.
        remote_args = ["admin", "logs"]
        if target is not None:
            remote_args.append(target)
        remote_args.extend(["--host", "localhost"])
        if list_only:
            remote_args.append("--list")
        if tail_lines is not None:
            remote_args.extend(["--tail", str(tail_lines)])
        if as_json:
            remote_args.append("--json")
        click.echo(_delegate_to_remote(log_owner, cfg, *remote_args), nl=False)
        return

    multi_user = _multi_user_active(cfg)
    log_dir = paths.admin_update_log_dir(multi_user=multi_user)
    # Transcripts are namespaced by target DIRECTORY, so a lookup is an exact
    # match rather than a suffix glob that could return (or prune) a different
    # target's transcripts.
    try:
        if target is not None:
            target_dir = paths.admin_update_log_target_dir(
                target, multi_user=multi_user
            )
            matches = sorted(target_dir.glob("*.log"))
        else:
            matches = sorted(log_dir.glob("*/*.log"), key=lambda p: p.name)
    except OSError as e:
        raise click.ClickException(f"could not read {log_dir}: {e}") from None

    if list_only:
        payload = [
            {
                "path": str(p),
                "target": p.parent.name,
                "started_at": p.stem,
                "bytes": p.stat().st_size,
            }
            for p in reversed(matches)
        ]
        if as_json:
            import json as _json

            click.echo(_json.dumps(payload, indent=2, sort_keys=True))
            return
        if not payload:
            click.echo(f"no admin update transcripts under {log_dir}")
            return
        for row in payload:
            click.echo(f"{row['bytes']:>10}  {row['target']:<24}  {row['path']}")
        return

    if not matches:
        hint = f" for {target!r}" if target else ""
        raise click.ClickException(
            f"no admin update transcript{hint} under {log_dir}. "
            "Transcripts are written from v0.12.1 onward; an update that ran "
            "before that (or with logging disabled) left none."
        )
    newest = matches[-1]
    try:
        text = newest.read_text(encoding="utf-8", errors="replace")
    except OSError as e:
        raise click.ClickException(f"could not read {newest}: {e}") from None
    if tail_lines is not None:
        text = "".join(text.splitlines(keepends=True)[-tail_lines:])
    if as_json:
        import json as _json

        click.echo(
            _json.dumps(
                {
                    "path": str(newest),
                    "lines": len(text.splitlines()),
                    "text": text,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return
    click.echo(f"== {newest} ==")
    click.echo(text, nl=False)


@admin.command("observe-update")
@click.argument("run_id")
@click.option(
    "--host",
    "host",
    default=None,
    metavar="HOST",
    help="Host the run lives on (default: default_host). The run record and "
    "its transcript stay on the machine that performed the update.",
)
@click.option(
    "--offset",
    type=click.IntRange(min=0),
    default=0,
    show_default=True,
    help="Byte offset to read the update transcript from. A follower passes "
    "the previous response's `transcript_next_offset` to get only new output.",
)
@click.option(
    "--max-bytes",
    type=click.IntRange(min=1, max=admin_detached.MAX_TRANSCRIPT_CHUNK),
    default=admin_detached.MAX_TRANSCRIPT_CHUNK,
    help="Largest transcript slice to return in one response.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Emit the observation as JSON. This is the shape a delegating driver "
    "polls; the text form is for reading by hand.",
)
def admin_observe_update(
    run_id: str,
    host: str | None,
    offset: int,
    max_bytes: int,
    as_json: bool,
) -> None:
    """Report the state of a detached `vq admin update` or `auto-update` run.

    \b
    Forms:
      vq admin observe-update RUN_ID
      vq admin observe-update RUN_ID --host build-host
      vq admin observe-update RUN_ID --json --offset 4096

    RUN_ID comes from the receipt `vq admin update --detach` or
    `vq admin auto-update --detach` printed, or from the `detached_run_id`
    field of the marker in `vq admin status --json`.

    \b
    States:
      launching   intent recorded; the updater has not activated yet
      running     the updater is alive and owns the work
      completed   a terminal receipt exists; outcome and exit code are exact
      lost        activated, not terminal, and its process is gone
      missing     no run record on this host

    Read-only: observing a run never signals it, never clears a marker, and
    never mutates the record. That is what makes it safe for a driver to
    retry after a dropped connection, which is the whole point of detaching
    the build from the session that launched it.
    """
    cfg = config.load_config()
    resolved = _resolve_host(cfg, host)
    if not is_local_host(resolved):
        remote_args = [
            "admin",
            "observe-update",
            run_id,
            # Explicit, for the same reason the delegated update appends a
            # `localhost` positional and the driver's own poller passes this
            # flag: the run lives on the host we are talking to. A target
            # whose own default_host is another machine would otherwise
            # forward the observation there and answer `missing` for a run
            # that host has never heard of.
            "--host",
            "localhost",
            "--offset",
            str(offset),
            "--max-bytes",
            str(max_bytes),
        ]
        if as_json:
            remote_args.append("--json")
        click.echo(_delegate_to_remote(resolved, cfg, *remote_args), nl=False)
        return

    try:
        observed = admin_detached.observe(
            run_id,
            offset=offset,
            max_bytes=max_bytes,
            multi_user=_multi_user_active(cfg),
        )
    except admin_detached.DetachedRunError as e:
        raise click.UsageError(str(e)) from None

    if as_json:
        click.echo(json.dumps(observed.to_json(), indent=2, sort_keys=True))
        return

    click.echo(f"run:        {observed.run_id}")
    click.echo(f"state:      {observed.state}")
    click.echo(f"detail:     {observed.detail}")
    click.echo(f"target:     {observed.target or '-'}")
    click.echo(f"pid:        {observed.pid if observed.pid else '-'}")
    click.echo(f"transcript: {observed.transcript or '-'}")
    if observed.state == admin_detached.STATE_COMPLETED:
        click.echo(f"outcome:    {observed.outcome or '-'}")
        click.echo(f"exit code:  {observed.exit_code}")
        if observed.error:
            click.echo(f"error:      {observed.error}")
    chunk = base64.b64decode(observed.transcript_base64 or "")
    if chunk:
        click.echo(
            f"-- transcript bytes {observed.transcript_offset}.."
            f"{observed.transcript_next_offset} of {observed.transcript_size} --"
        )
        click.echo(chunk.decode("utf-8", "replace"), nl=False)
    if observed.payload:
        click.echo("-- recorded result --")
        click.echo(observed.payload, nl=False)


@admin.command("clear-update-marker")
@click.argument("host", required=False)
@click.option(
    "-y",
    "--yes",
    is_flag=True,
    default=False,
    help="Skip the interactive confirmation prompt. Useful in "
    "scripts; on a TTY the default is to show the marker "
    "contents and ask for explicit confirmation before "
    "deleting.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="v0.5.46: emit JSON instead of the text summary. Output "
    'shape: `{"cleared": bool, "marker": {...} | null, '
    '"readable": bool}` plus marker diagnosis fields when a marker '
    'was present. `cleared` is False when no marker '
    "was present (quiet no-op case). Implies --yes (no prompt "
    "in scripting mode — the absence of a TTY is itself signal).",
)
@click.option(
    "--force-live",
    is_flag=True,
    default=False,
    help="Allow clearing a marker whose writer PID still appears alive. "
    "Without this, clear-update-marker refuses live markers even with --yes "
    "or --json so an active remote update is not accidentally un-gated. It "
    "never permits discarding a durable managed-daemon receipt or bypassing "
    "a pause-only receipt's exact resume proof.",
)
def admin_clear_update_marker(
    host: str | None,
    yes: bool,
    as_json: bool,
    force_live: bool,
) -> None:
    """v0.5.44: clear the admin-update-in-progress marker.

    \b
    Forms:
      vq admin clear-update-marker           (default_host)
      vq admin clear-update-marker HOST      (explicit host)
      vq admin clear-update-marker CLUSTER   (routes to its scheduler_driver)
      vq admin clear-update-marker -y        (skip confirmation)
      vq admin clear-update-marker --force-live

    The marker is written at the start of `vq admin update` and cleared only
    after terminal proof. If the update is interrupted, it persists and
    subsequent conflicting updates refuse to proceed. A durable managed-daemon
    receipt must be reconciled with `vq admin recover-update`; this command
    refuses to discard it. A pause-only receipt is cleared only after this
    command proves its exact token scope resumed.

    HOST names the machine whose update you are recovering, not
    necessarily the machine holding the file. For a SCHEDULER host
    (pbs-cluster, slurm-cluster) the update runs on that host's `scheduler_driver`
    and the marker lives there, so this command follows the driver
    the same way `vq admin status HOST` does. Passing the cluster
    name is correct and does the right thing.

    \b
    Ordinary-marker recovery flow:
      1. `vq admin status HOST` shows the marker banner with envs +
         pid + started_at.
      2. Manually verify the env is healthy (re-run the build,
         check git state, run smoke tests).
      3. `vq admin clear-update-marker HOST` removes the marker
         after the prompt confirms.

    For an ordinary marker only, `vq admin update <env> HOST --force`
    overwrites it and runs a new update after independent inspection. It
    refuses durable managed-daemon and paused-job receipts; never use force in
    place of `vq admin recover-update`.

    No marker present = quiet no-op (exit 0; "no marker present"
    message). Idempotent."""
    cfg = config.load_config()
    explicit_host = host is not None
    host = _resolve_host(cfg, host)
    # A SCHEDULER host's marker does not live on the cluster login node. Both
    # `update_scheduler_host` and `update_scheduler_runtime` run on the host's
    # `scheduler_driver` and write the marker into the DRIVER's state root, with
    # envs=['scheduler:<host>'] / ['scheduler-runtime:<host>:<program>'] and
    # `host` naming the cluster. So route the clear to the driver, exactly as
    # `vq admin status HOST` already does. Without this,
    # `vq admin clear-update-marker slurm-cluster` SSHes to slurm-cluster, finds no marker
    # there, prints "no marker present" and exits 0 — indistinguishable from
    # success — while the real marker keeps blocking every admin update on the
    # driver, leaving `--force` as the only way through. That is BUG 2 of the
    # 2026-07-22 report: the documented recovery path was a dead end.
    scheduler_driver = _scheduler_driver_host(cfg, host)
    marker_owner = scheduler_driver if scheduler_driver is not None else host
    # The hostless form resolves to `default_host`, which is usually some other
    # machine. Refuse rather than silently clearing the wrong host's marker
    # while a local one sits there blocking.
    if (
        not explicit_host
        and not is_local_host(marker_owner)
        and admin_module.admin_update_marker_exists()
    ):
        raise click.UsageError(
            "an admin-update marker is present on THIS machine, but no HOST "
            f"was given so the command would target {marker_owner!r} "
            "(default_host). Name the host explicitly: "
            "`vq admin clear-update-marker localhost` for the local marker, "
            f"or `vq admin clear-update-marker {marker_owner}` for that host's."
        )
    if is_local_host(marker_owner):
        markers = admin_module.read_admin_update_markers()
        if scheduler_driver is not None:
            candidates = [
                item for item in markers if item.host == host
            ]
        else:
            candidates = [
                item
                for item in markers
                if all(":" not in env for env in item.envs)
            ]
        marker = candidates[0] if candidates else None
        # An unreadable marker cannot be targeted safely, so preserve the
        # historical visible/refusal path only when no parseable lease exists.
        exists = bool(candidates) or (
            not markers and admin_module.admin_update_marker_exists()
        )

        def _diagnosed_marker_payload(marker_obj):
            diag = admin_module.diagnose_admin_update_marker(marker_obj)
            if marker_obj is None:
                return None, diag
            from dataclasses import asdict as _asdict

            return (
                {
                    **_asdict(marker_obj),
                    "marker_status": diag.marker_status,
                    "summary": diag.summary,
                    "action": diag.action,
                    "pid_status": diag.pid_status,
                    "stale_reason": diag.stale_reason,
                    "heartbeat_status": diag.heartbeat_status,
                    "heartbeat_age_seconds": diag.heartbeat_age_seconds,
                },
                diag,
            )

        def _live_marker_refusal_payload(marker_payload, diag):
            return {
                "cleared": False,
                "marker": marker_payload,
                "readable": marker_payload is not None,
                "marker_status": diag.marker_status,
                "summary": diag.summary,
                "action": diag.action,
                "pid_status": diag.pid_status,
                "stale_reason": diag.stale_reason,
                "heartbeat_status": diag.heartbeat_status,
                "heartbeat_age_seconds": diag.heartbeat_age_seconds,
                "error": (
                    "refusing to clear live admin-update marker without "
                    "--force-live"
                ),
            }

        def _clear_selected_marker():
            if (
                marker is not None
                and marker.managed_transaction is None
                and marker.owns_pause_scope
            ):
                try:
                    recovered_marker, _summary = (
                        admin_module.recover_pause_scope_and_clear_marker(
                            marker,
                        )
                    )
                    return recovered_marker
                except (
                    admin_module.AdminUpdateInProgress,
                    admin_module.AdminPreconditionFailed,
                ) as exc:
                    # Recovering a pause scope takes the checkout-mutation
                    # lock. Another admin operation holding it is the same
                    # `locked` condition `admin update` reports, and the
                    # recovery step the orchestration contract points at
                    # must not answer it with a bare exit 1.
                    raise _classified_admin_error(exc, as_json=as_json) from None
                except admin_module.AdminError as exc:
                    raise click.ClickException(str(exc)) from None
            try:
                return admin_module.clear_admin_update_marker(marker)
            except (
                admin_module.AdminUpdateInProgress,
                admin_module.AdminPreconditionFailed,
            ) as exc:
                raise _classified_admin_error(exc, as_json=as_json) from None
            except admin_module.AdminError as exc:
                raise click.ClickException(str(exc)) from None

        if as_json:
            import json as _json

            if not exists:
                click.echo(
                    _json.dumps(
                        {"cleared": False, "marker": None, "readable": False},
                        indent=2,
                        sort_keys=True,
                    )
                )
                return
            # Capture before unlink so the response reflects what was
            # cleared. --json implies --yes (no prompt in script mode).
            marker_payload, diag = _diagnosed_marker_payload(marker)
            if (
                diag.marker_status == admin_module.ADMIN_UPDATE_MARKER_DIAG_RUNNING
                and not force_live
            ):
                click.echo(
                    _json.dumps(
                        _live_marker_refusal_payload(marker_payload, diag),
                        indent=2,
                        sort_keys=True,
                    )
                )
                raise click.exceptions.Exit(1)
            snapshot = _clear_selected_marker()
            marker_payload, diag = _diagnosed_marker_payload(snapshot)
            click.echo(
                _json.dumps(
                    {
                        "cleared": True,
                        "marker": marker_payload,
                        "readable": snapshot is not None,
                        "marker_status": diag.marker_status,
                        "summary": diag.summary,
                        "action": diag.action,
                        "pid_status": diag.pid_status,
                        "stale_reason": diag.stale_reason,
                        "heartbeat_status": diag.heartbeat_status,
                        "heartbeat_age_seconds": diag.heartbeat_age_seconds,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            return
        if not exists:
            # Name where we looked. "no marker present" alone reads identically
            # whether the marker was cleared or the command was pointed at the
            # wrong machine.
            click.echo(
                "no admin-update-in-progress marker present on "
                f"{_local_host_label(host, scheduler_driver)}"
            )
            return
        if marker is not None:
            click.echo(
                f"marker:\n"
                f"  envs:        {', '.join(marker.envs)}\n"
                f"  host:        {marker.host}\n"
                f"  started_at:  {marker.started_at}\n"
                f"  pid:         {marker.pid}\n"
                f"  vq_version:  {marker.vq_version}"
            )
        else:
            click.echo("marker file present but unreadable (JSON parse failed)")
        diag = admin_module.diagnose_admin_update_marker(marker)
        click.echo(f"  marker_status: {diag.marker_status}")
        if diag.pid_status:
            click.echo(f"  pid_status:    {diag.pid_status}")
        if marker is not None and marker.last_heartbeat_at:
            click.echo(f"  last_heartbeat: {marker.last_heartbeat_at}")
        if diag.heartbeat_status:
            click.echo(f"  heartbeat:     {diag.heartbeat_status}")
        if diag.stale_reason:
            click.echo(f"  stale:         {diag.stale_reason}")
        click.echo(f"  summary:       {diag.summary}")
        click.echo(f"  action:        {diag.action}")
        if (
            diag.marker_status == admin_module.ADMIN_UPDATE_MARKER_DIAG_RUNNING
            and not force_live
        ):
            raise click.ClickException(
                "refusing to clear live admin-update marker without "
                "--force-live; wait for it to finish or inspect "
                "`vq admin status --verbose` first"
            )
        if not yes:
            try:
                click.confirm("clear it?", abort=True)
            except click.Abort:
                if sys.stdin.isatty():
                    # A human read the marker and said no. That is an answer.
                    raise
                # No TTY and nothing on stdin: a delegated
                # `vq admin clear-update-marker HOST` reaches the far side
                # exactly like this, where click aborts with a bare
                # "Aborted!" naming neither cause nor remedy. The 2026-09
                # migration worked around it by ssh'ing to the host and
                # piping `yes`. A piped answer still works and still counts.
                raise click.UsageError(
                    "clearing a marker needs confirmation and stdin is not a "
                    "terminal. Re-run with --yes, or with --json, which "
                    "implies it."
                ) from None
        _clear_selected_marker()
        click.echo("marker cleared")
        return
    # SSH delegate.
    remote_args = ["admin", "clear-update-marker"]
    if yes:
        remote_args.append("--yes")
    if as_json:
        remote_args.append("--json")
    if force_live:
        remote_args.append("--force-live")
    # For a scheduler host, hand the driver the CLUSTER name (mirroring
    # `admin status`): the driver re-resolves it, sees itself as the driver, and
    # clears its own marker. "localhost" would work too on the driver, but the
    # cluster name keeps the delegated argv self-documenting in the audit log.
    remote_args.append(host if scheduler_driver is not None else "localhost")
    click.echo(_delegate_to_remote(marker_owner, cfg, *remote_args), nl=False)


@admin.command("bootstrap-self-update")
@click.argument("host")
@click.option("--expected-sha", required=True, metavar="FULL_SHA")
@click.option("--json", "as_json", is_flag=True)
@click.option("--token", "cli_token", default=None)
@click.option("--token-stdin", is_flag=True)
@click.option("--token-file", type=click.Path(dir_okay=False), default=None)
def admin_bootstrap_self_update(
    host: str,
    expected_sha: str,
    as_json: bool,
    cli_token: str | None,
    token_stdin: bool,
    token_file: str | None,
) -> None:
    """Bootstrap an older remote driver through authenticated current code.

    HOST must be a canonical direct host. Its service manager selects the
    installation; the remote self-update owns the global rollout lock, exact
    forward ancestry check, durable receipt and verified daemon restart.
    No environment override or force mode is available.
    """
    expected_sha = expected_sha.strip().lower()
    if re.fullmatch(r"[0-9a-f]{40}", expected_sha) is None:
        raise click.UsageError("--expected-sha must be a full 40-character hex commit SHA")
    cfg = config.load_config()
    token = _resolve_admin_token(
        cfg, command_label="vq admin bootstrap-self-update",
        cli_token=cli_token, token_stdin=token_stdin, token_file=token_file,
    )
    target = _resolve_host(cfg, host)
    auth_args, remote_stdin = _remote_admin_auth(target, cfg, token)
    remote_env, remote_timeout = _remote_admin_update_contract(
        aggregate=False, host_cfg=_host_cfg_or_none(cfg, target),
    )
    _emit_remote_admin_timeout_summary(remote_env, remote_timeout)
    try:
        result = admin_module.update_remote_managed_env_with_driver_runtime(
            cfg, target, env="vibeqc-queue", expected_sha=expected_sha,
            expected_tag=None, remote_auth_args=tuple(auth_args),
            stdin_data=remote_stdin, as_json=as_json, update_script_args=(),
            show_output=False, remote_timeout_env=remote_env,
            timeout=remote_timeout, self_update=True,
        )
    except admin_module.AdminError as exc:
        raise click.UsageError(str(exc)) from None
    except transport.RemoteError as exc:
        raise click.ClickException(str(exc)) from None
    click.echo(result, nl=False)


@admin.command("recover-update")
@click.argument("host", required=False)
@click.option(
    "--marker-id", default=None, metavar="ID",
    help="Recover the one durable managed transaction with this marker ID.",
)
@click.option(
    "--quarantine-orphaned-receipt",
    is_flag=True,
    default=False,
    help="Quarantine one positively proven terminal-v1 orphan receipt.",
)
@click.option(
    "--expected-marker-sha256",
    default=None,
    metavar="SHA256",
    help="Full SHA-256 of the exact orphan marker bytes.",
)
@click.option(
    "--expected-current-source-sha",
    default=None,
    metavar="SHA",
    help="Accepted full SHA of the current healthy serving vq runtime.",
)
@click.option(
    "--reason",
    default=None,
    metavar="TEXT",
    help="One-line operator reason retained in quarantine evidence.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Run every proof but do not create or move quarantine evidence.",
)
@click.option(
    "--with-driver-runtime",
    is_flag=True,
    default=False,
    help=(
        "For an older remote vq only, stage this driver's exact vq package "
        "temporarily and use its recovery parser without installing it."
    ),
)
@click.option(
    "--staged-driver-archive-sha256",
    default=None,
    metavar="SHA256",
    hidden=True,
)
@click.option("--json", "as_json", is_flag=True, default=False)
@click.option("--token", "cli_token", default=None, metavar="TOKEN", hidden=True)
@click.option("--token-stdin", is_flag=True, default=False)
@click.option("--token-file", type=click.Path(dir_okay=False), default=None)
def admin_recover_update(
    host: str | None,
    marker_id: str | None,
    quarantine_orphaned_receipt: bool,
    expected_marker_sha256: str | None,
    expected_current_source_sha: str | None,
    reason: str | None,
    dry_run: bool,
    with_driver_runtime: bool,
    staged_driver_archive_sha256: str | None,
    as_json: bool,
    cli_token: str | None,
    token_stdin: bool,
    token_file: str | None,
) -> None:
    """Recover an interrupted serving-daemon update from its durable receipt.

    This command never guesses that an unverified target landed. It reconciles
    the receipt's exact durable phase: an interrupted, uncommitted target rolls
    back to the pre-update checkout and virtualenv; a receipt that already
    records a committed target is re-attested at that exact target and cleanup
    is completed. It verifies the selected daemon SHA and tree, resumes only
    the jobs named by the marker's pause scope, then clears that marker. A live
    updater or malformed/mismatched receipt remains blocked.

    ``--quarantine-orphaned-receipt`` is a separate break-glass path for one
    exact terminal-v1 receipt whose checkout, virtualenv, backup, and service
    executable are all gone. It requires the marker ID and byte hash, a full
    accepted SHA for the unrelated healthy current runtime, and a retained
    reason. It never resumes jobs or changes the service; it proves the pause
    token already clear and moves the original marker bytes into owner-only
    retained evidence.

    ``--with-driver-runtime`` is an explicit compatibility path for a remote
    vq whose own recovery parser predates a landed receipt fix. It uploads an
    integrity-checked temporary archive of the current driver package, runs
    the ordinary remote recovery state machine through that package exactly
    once, and never installs or replaces the remote vq environment.
    """
    if staged_driver_archive_sha256 is not None:
        if with_driver_runtime or quarantine_orphaned_receipt:
            raise click.UsageError(
                "the internal staged-driver identity cannot be combined with "
                "another recovery mode"
            )
        try:
            admin_module.require_staged_driver_recovery_archive(
                staged_driver_archive_sha256,
            )
        except admin_module.AdminError as exc:
            raise click.UsageError(str(exc)) from None
    quarantine_values = {
        "--expected-marker-sha256": expected_marker_sha256,
        "--expected-current-source-sha": expected_current_source_sha,
        "--reason": reason,
    }
    if quarantine_orphaned_receipt:
        if with_driver_runtime:
            raise click.UsageError(
                "--with-driver-runtime cannot be combined with orphan quarantine"
            )
        missing = [
            name for name, value in quarantine_values.items() if not value
        ]
        if marker_id is None:
            missing.insert(0, "--marker-id")
        if missing:
            raise click.UsageError(
                "--quarantine-orphaned-receipt requires " + ", ".join(missing)
            )
    elif dry_run or any(value is not None for value in quarantine_values.values()):
        raise click.UsageError(
            "--dry-run, --expected-marker-sha256, "
            "--expected-current-source-sha, and --reason are valid only with "
            "--quarantine-orphaned-receipt"
        )
    cfg = config.load_config()
    host = _resolve_host(cfg, host)
    scheduler_driver = _scheduler_driver_host(cfg, host)
    owner = scheduler_driver if scheduler_driver is not None else host
    if staged_driver_archive_sha256 is not None and not is_local_host(owner):
        raise click.UsageError(
            "the internal staged-driver identity requires localhost recovery"
        )
    token = _resolve_admin_token(
        cfg,
        command_label="admin recover-update",
        cli_token=cli_token,
        token_stdin=token_stdin,
        token_file=token_file,
    )
    if is_local_host(owner):
        if with_driver_runtime:
            raise click.UsageError(
                "--with-driver-runtime requires a non-local direct host"
            )
        try:
            if quarantine_orphaned_receipt:
                assert marker_id is not None
                assert expected_marker_sha256 is not None
                assert expected_current_source_sha is not None
                assert reason is not None
                recovered = admin_module.quarantine_orphaned_managed_receipt(
                    cfg,
                    marker_id=marker_id,
                    expected_marker_sha256=expected_marker_sha256,
                    expected_current_source_sha=expected_current_source_sha,
                    reason=reason,
                    dry_run=dry_run,
                )
            else:
                recovered = admin_module.recover_managed_update(
                    cfg, marker_id=marker_id,
                )
        except admin_module.AdminUpdateInProgress as exc:
            raise click.ClickException(str(exc)) from None
        except admin_module.AdminError as exc:
            raise click.UsageError(str(exc)) from None
        payload = asdict(recovered)
        if as_json:
            click.echo(json.dumps(payload, indent=2, sort_keys=True))
        elif quarantine_orphaned_receipt:
            click.echo(
                f"== admin recover-update quarantine {recovered.env} ==\n"
                f"   dry-run:     {recovered.dry_run}\n"
                f"   quarantined: {recovered.quarantined}\n"
                f"   marker:      {recovered.marker_id}\n"
                f"   plan:        {recovered.plan_sha256}\n"
                f"   evidence:    {recovered.quarantine_path}\n"
                f"   detail:      {recovered.detail}\n"
                f"   pause:       {recovered.pause_summary}"
            )
        else:
            click.echo(
                f"== admin recover-update {recovered.env} ==\n"
                f"   recovered: {recovered.recovered}\n"
                f"   detail:    {recovered.detail}\n"
                f"   resumed:   {recovered.resumed_summary}"
            )
        if not quarantine_orphaned_receipt and not recovered.recovered:
            raise click.exceptions.Exit(1)
        return
    if with_driver_runtime:
        if scheduler_driver is not None:
            raise click.UsageError(
                "--with-driver-runtime is only supported for direct hosts"
            )
        auth_args, remote_stdin = _remote_admin_auth(owner, cfg, token)
        try:
            output = admin_module.recover_remote_managed_update_with_driver_runtime(
                cfg,
                owner,
                marker_id=marker_id,
                remote_auth_args=tuple(auth_args),
                stdin_data=remote_stdin,
                as_json=as_json,
            )
        except admin_module.AdminError as exc:
            raise click.UsageError(str(exc)) from None
        except transport.RemoteOutcomeUnknown as exc:
            raise click.ClickException(str(exc)) from None
        except transport.RemoteError as exc:
            raise click.ClickException(str(exc)) from None
        click.echo(output, nl=False)
        return
    remote_args = ["admin", "recover-update", "localhost"]
    if marker_id:
        remote_args.extend(["--marker-id", marker_id])
    if quarantine_orphaned_receipt:
        remote_args.append("--quarantine-orphaned-receipt")
        remote_args.extend([
            "--expected-marker-sha256", str(expected_marker_sha256),
            "--expected-current-source-sha", str(expected_current_source_sha),
            "--reason", str(reason),
        ])
        if dry_run:
            remote_args.append("--dry-run")
    if as_json:
        remote_args.append("--json")
    auth_args, remote_stdin = _remote_admin_auth(owner, cfg, token)
    remote_args.extend(auth_args)
    click.echo(
        _delegate_to_remote(
            owner, cfg, *remote_args, stdin_data=remote_stdin,
        ),
        nl=False,
    )


@admin.command("mark-ok")
@click.argument("env")
@click.argument("host", required=False)
@click.option(
    "--note",
    required=True,
    metavar="REASON",
    help="v0.7.1: required audit note explaining WHY the env is "
    "being marked OK out-of-band. Lands in the admin-status "
    "record's last_marked_ok_note so a later operator can answer "
    "'why is this True?' without git-archaeology.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="v0.7.1: emit JSON instead of the text summary. Output "
    "is the post-write AdminUpdateRecord shape with "
    "`last_marked_ok_at` and `last_marked_ok_note` populated.",
)
@click.option(
    "--token",
    "cli_token",
    default=None,
    metavar="TOKEN",
    help="v0.7.1: bearer token for multi-user mode. DISCOURAGED — "
    "the token leaks into shell history and `ps -ef` argv. Prefer "
    "$VQ_TOKEN env var, --token-stdin, or --token-file.",
)
@click.option(
    "--token-stdin",
    "token_stdin",
    is_flag=True,
    default=False,
    help="v0.7.1: read the bearer token from a single line on "
    "stdin. Use to keep the token out of `ps -ef` and shell "
    "history. Mutually exclusive with --token / --token-file.",
)
@click.option(
    "--token-file",
    "token_file",
    type=click.Path(dir_okay=False),
    default=None,
    metavar="PATH",
    help="v0.7.1: read the bearer token from a 0600-mode file. "
    "Same perm enforcement as ~/.config/vq/web-token. Mutually "
    "exclusive with --token / --token-stdin.",
)
def admin_mark_ok(
    env: str,
    host: str | None,
    note: str,
    as_json: bool,
    cli_token: str | None,
    token_stdin: bool,
    token_file: str | None,
) -> None:
    """v0.7.1 *Lamport's Clock*: flip LAST OK=True after operator
    verifies an env out-of-band.

    \b
    Forms:
      vq admin mark-ok ENV --note "REASON"
      vq admin mark-ok ENV HOST --note "REASON"

    When to use: an env is healthy but ``vq admin status`` shows
    ``LAST OK=False`` because the last ``vq admin update``
    encountered something subtle (argv loss, venv-hybrid state,
    network hiccup) that the operator already fixed manually. The
    surgical Python-edit of admin-status.json we used on
    2026-05-25 is the canonical case — this verb does the same
    thing cleanly + with an audit trail.

    The asterisk: after mark-ok, ``vq admin status`` renders
    ``LAST OK=True*`` (note the trailing ``*``) to flag that the
    True came from operator acknowledge rather than a real
    update. ``vq admin status --verbose`` shows the audit note.

    Lifetime: the mark-ok is overwritten by the next real
    ``vq admin update`` for the env (whether success or failure).
    Use it as a tactical "I know this is fine, stop flagging it",
    not a permanent fixture.
    """
    cfg = config.load_config()
    host = _resolve_host(cfg, host)
    token = _resolve_admin_token(
        cfg,
        command_label="admin mark-ok",
        cli_token=cli_token,
        token_stdin=token_stdin,
        token_file=token_file,
    )

    if is_local_host(host):
        try:
            rec = admin_module.mark_env_ok(env, note=note, cfg=cfg)
        except admin_module.AdminError as e:
            raise click.UsageError(str(e)) from None
        if as_json:
            import json as _json
            from dataclasses import asdict as _asdict

            click.echo(_json.dumps(_asdict(rec), indent=2, sort_keys=True))
        else:
            click.echo(
                f"== admin mark-ok {env} ==\n"
                f"   marked_ok_at: {rec.last_marked_ok_at}\n"
                f"   note:         {rec.last_marked_ok_note}\n"
                f"   sha:          {rec.last_sha}\n"
                f"   == OK =="
            )
    else:
        # Remote delegation — forward note + token via stdin.
        remote_args = ["admin", "mark-ok", env, "--note", note]
        if as_json:
            remote_args.append("--json")
        auth_args, remote_stdin = _remote_admin_auth(host, cfg, token)
        remote_args.extend(auth_args)
        remote_args.append("localhost")
        click.echo(
            _delegate_to_remote(
                host,
                cfg,
                *remote_args,
                stdin_data=remote_stdin,
            ),
            nl=False,
        )


@admin.command("reset-branch")
@click.argument("env")
@click.argument("host", required=False)
@click.option(
    "--yes",
    "confirm_yes",
    is_flag=True,
    default=False,
    help="Required confirmation that the operator understands "
    "``git reset --hard`` is destructive (any uncommitted local "
    "changes in the env's working tree are discarded). Without "
    "--yes the verb prints the planned reset and exits 1 so a "
    "muscle-memory invocation can't accidentally throw away work.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="v0.7.9: emit JSON instead of the text summary. Shape "
    "is the dataclass-serialized ``ResetBranchResult`` (env, "
    "branch, prior_sha, new_sha, fetch_rc, reset_rc, output, "
    "success).",
)
@click.option(
    "--token",
    "cli_token",
    default=None,
    metavar="TOKEN",
    help="v0.7.9: bearer token for multi-user mode (discouraged; "
    "prefer $VQ_TOKEN / --token-stdin / --token-file).",
)
@click.option(
    "--token-stdin",
    "token_stdin",
    is_flag=True,
    default=False,
    help="v0.7.9: read bearer token from a single line on stdin.",
)
@click.option(
    "--token-file",
    "token_file",
    type=click.Path(dir_okay=False),
    default=None,
    metavar="PATH",
    help="v0.7.9: read bearer token from a 0600-mode file.",
)
def admin_reset_branch(
    env: str,
    host: str | None,
    confirm_yes: bool,
    as_json: bool,
    cli_token: str | None,
    token_stdin: bool,
    token_file: str | None,
) -> None:
    """v0.7.9 *Liskov's Substitution*: snap an env's working tree
    to ``origin/<configured-branch>``.

    \b
    Forms:
      vq admin reset-branch ENV --yes
      vq admin reset-branch ENV HOST --yes
      vq admin reset-branch ENV HOST --yes --json

    When to use: v0.7.1's post-update branch validation has
    surfaced silent branch drift (``vq admin status`` shows
    ``BRANCH ACTUAL`` differing from the configured branch), and
    the operator wants to snap back without spelunking through
    git by hand. The verb runs:

    \b
      git fetch origin
      git reset --hard origin/<configured-branch>

    on the env's ``git_dir``. Any uncommitted local changes are
    discarded — that's exactly the intent (operators run this to
    throw away stray hand-edits on workstation / compute-a after a
    cross-chat collision).

    Safety: ``--yes`` is required because the reset is
    destructive. Without ``--yes`` the verb prints the planned
    operation + exits non-zero so muscle-memory invocations
    can't accidentally clobber work.

    Post-reset bookkeeping: ``last_sha`` + ``last_branch_actual``
    update to reflect reality so the next ``vq admin status``
    shows the snap immediately. ``last_success`` is NOT flipped
    True — a reset-branch fixes the branch but doesn't prove the
    build is healthy. Follow with ``vq admin update ENV`` (to
    rebuild) or ``vq admin mark-ok ENV --note "..."`` (if you've
    independently verified) to flip ``last_success``.
    """
    cfg = config.load_config()
    host = _resolve_host(cfg, host)
    token = _resolve_admin_token(
        cfg,
        command_label="admin reset-branch",
        cli_token=cli_token,
        token_stdin=token_stdin,
        token_file=token_file,
    )

    if not confirm_yes:
        # Print the planned operation + exit non-zero so the
        # operator has to consciously add --yes. This is the
        # "you have entered a destructive command" guardrail.
        try:
            prog = cfg.host(host) if not is_local_host(host) else None
            del prog  # only used for the host-exists check
        except config.ConfigError as e:
            raise click.UsageError(str(e)) from None
        click.echo(
            f"== admin reset-branch {env} ==\n"
            f"   Planned: git fetch origin && git reset --hard "
            f"origin/<configured-branch> on {host}.\n"
            f"   This is DESTRUCTIVE (uncommitted local changes "
            f"in the env's working tree will be discarded).\n"
            f"   Re-run with --yes to confirm."
        )
        raise click.ClickException("reset-branch: --yes not supplied")

    if is_local_host(host):
        try:
            result = admin_module.reset_branch_env(env, cfg=cfg)
        except admin_module.AdminError as e:
            raise click.UsageError(str(e)) from None
        if as_json:
            import json as _json
            from dataclasses import asdict as _asdict

            click.echo(
                _json.dumps(
                    {**_asdict(result), "success": result.success},
                    indent=2,
                    sort_keys=True,
                )
            )
        else:
            verdict = "OK" if result.success else "FAILED"
            click.echo(
                f"== admin reset-branch {env} ==\n"
                f"   branch:       {result.branch}\n"
                f"   prior_sha:    {result.prior_sha}\n"
                f"   new_sha:      {result.new_sha}\n"
                f"   fetch_rc:     {result.fetch_rc}\n"
                f"   reset_rc:     {result.reset_rc}\n"
                f"   checkout_rc:  {result.checkout_rc}\n"
                f"   == {verdict} =="
            )
            if not result.success and result.output:
                click.echo(
                    "\n--- git output (tail) ---\n" + result.output,
                    err=True,
                )
        if not result.success:
            raise click.ClickException(
                f"reset-branch failed on {env}: fetch_rc="
                f"{result.fetch_rc} reset_rc={result.reset_rc}"
            )
    else:
        # Remote delegation.
        remote_args = ["admin", "reset-branch", env, "--yes"]
        if as_json:
            remote_args.append("--json")
        auth_args, remote_stdin = _remote_admin_auth(host, cfg, token)
        remote_args.extend(auth_args)
        remote_args.append("localhost")
        click.echo(
            _delegate_to_remote(
                host,
                cfg,
                *remote_args,
                stdin_data=remote_stdin,
            ),
            nl=False,
        )


@admin.command("audit-recovery")
@click.argument("host", required=False)
@click.option(
    "--all",
    "all_hosts",
    is_flag=True,
    default=False,
    help="Audit every configured host (alphabetised). Mutually exclusive with a positional HOST.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Emit JSON instead of the text table.",
)
def admin_audit_recovery(
    host: str | None,
    all_hosts: bool,
    as_json: bool,
) -> None:
    """v0.7.5 *Hopper's Compiler*: probe the 3-tier recovery
    channels contract for HOST.

    \b
    Forms:
      vq admin audit-recovery HOST
      vq admin audit-recovery --all
      vq admin audit-recovery HOST --json

    Tests each of: Tier 1 (BMC / IPMI — informational, from
    config), Tier 2 (Cockpit web admin on :9090), Tier 3
    (Recovery sshd on :22222 with the recovery key).

    For each tier, answers ONE question: "if my primary SSH
    broke right now, could I reach the host via this channel?"
    The probes are independent of vq's normal SSH transport on
    purpose — they have to work even when the primary key
    trust is broken, otherwise they'd be testing the wrong
    thing.

    Overall verdict (green / yellow / red):
      green  — at least 2 of the 3 tiers reachable
               (typically Cockpit + Recovery SSH; BMC counts
               as informational)
      yellow — exactly 1 tier reachable
      red    — no tier reachable; host is unrecoverable from
               this laptop if the primary path breaks. Run
               contrib/setup-recovery-channels.sh on the host
               before trusting it to the fleet.

    See docs/host_recovery_channels.md for the full contract.
    """
    from vq import recovery_audit as _audit

    cfg = config.load_config()

    if all_hosts and host is not None:
        raise click.UsageError(
            "--all and HOST are mutually exclusive; --all walks every configured host."
        )

    targets: list[str]
    if all_hosts:
        targets = sorted(cfg.hosts.keys())
        if not targets:
            raise click.UsageError(
                f"no hosts configured — add [hosts.X] sections to {config.config_path()}"
            )
    else:
        host = _resolve_host(cfg, host)
        targets = [host]

    # Resolve all host configs up-front (fail fast on bad config
    # before we spawn probes).
    host_cfgs: dict[str, config.HostConfig] = {}
    for name in targets:
        try:
            host_cfgs[name] = cfg.host(name)
        except config.ConfigError as e:
            raise click.UsageError(str(e)) from None

    # v0.7.6 *Tanenbaum's Mailbox*: probe hosts concurrently. Each
    # audit hits up to 3 network endpoints with 5 s timeouts, so the
    # serial cost was up to ``15 × n_hosts`` seconds; parallel fan-out
    # cuts the wall clock to ~15 s regardless of fleet size.
    def _audit_one(name: str) -> _audit.AuditReport:
        return _audit.audit_host(name, host_cfgs[name])

    if len(targets) > 1 and not _fanout_serial_requested():
        from concurrent.futures import ThreadPoolExecutor, as_completed

        report_by_host: dict[str, _audit.AuditReport] = {}
        with ThreadPoolExecutor(max_workers=_fanout_max_workers(len(targets))) as ex:
            futures = {ex.submit(_audit_one, n): n for n in targets}
            for fut in as_completed(futures):
                n = futures[fut]
                report_by_host[n] = fut.result()
        reports = [report_by_host[n] for n in targets]
    else:
        reports = [_audit_one(n) for n in targets]

    if as_json:
        import json as _json

        payload = {
            "reports": [_audit.format_audit_json(r) for r in reports],
            "n_total": len(reports),
            "n_green": sum(1 for r in reports if r.overall == "green"),
            "n_yellow": sum(1 for r in reports if r.overall == "yellow"),
            "n_red": sum(1 for r in reports if r.overall == "red"),
        }
        click.echo(_json.dumps(payload, indent=2, sort_keys=True))
    else:
        click.echo("\n\n".join(_audit.format_audit_text(r) for r in reports))

    # Exit non-zero if any host is red — caller can wire this
    # into CI / monitoring without parsing the output.
    if any(r.overall == "red" for r in reports):
        raise click.ClickException(
            f"audit-recovery: {sum(1 for r in reports if r.overall == 'red')}"
            f" host(s) RED — not recoverable from this laptop"
        )


@admin.command("provision")
@click.argument("host", required=False)
@click.option(
    "--all",
    "all_hosts",
    is_flag=True,
    default=False,
    help="Verify every configured host. Mutually exclusive with HOST.",
)
@click.option(
    "--check",
    "check_only",
    is_flag=True,
    default=False,
    help="Contractually read-only: verify and report, and do not print the "
    "remediation plan. Use this from scripts and from a rehearsal such as "
    "coordinator's driver migration, where the point is to confirm the "
    "preconditions rather than to fix them.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Emit the stable vq.admin.provision_check/1 envelope for one host, "
    "or vq.admin.provision_check_fleet/1 (per-host payloads under `hosts`) "
    "with --all. The shape follows the form, not the host count.",
)
def admin_provision(
    host: str | None,
    all_hosts: bool,
    check_only: bool,
    as_json: bool,
) -> None:
    """v0.24.x: verify a host's vq provisioning preconditions.

    \b
    Forms:
      vq admin provision HOST            verify + print the remediation plan
      vq admin provision HOST --check    verify only, no remediation prose
      vq admin provision --all --json    machine-readable, whole fleet

    Checks the conditions ``vq doctor`` does not: that ``remote_vq`` points at
    a wrapper rather than straight at the venv vq, that ``admin_token_file``
    exists at mode 0600, that ``/var/lib/vq`` is ``root:vq-admins`` 2775, that
    the root-owned ``/opt/vq`` install and its refresh helper are in place,
    that privilege delegation is available, and that programs are registered.
    Each of those has cost hours of a fleet sweep while doctor reported the
    host green.

    Both forms are read-only. Repairing any of these needs root on the target,
    so the failures are reported with the exact command that fixes them rather
    than executed -- see ``docs/multi_user_deployment.md``. Exit 0 when every
    check passes, 1 when any fails.
    """
    if all_hosts and host is not None:
        raise click.UsageError("--all is mutually exclusive with HOST")
    try:
        cfg = config.load_config()
    except config.ConfigError as exc:
        raise click.UsageError(str(exc)) from None

    if all_hosts and not cfg.hosts:
        # `all(...)` over an empty list is True, so without this an empty
        # config would report the whole fleet provisioned and exit 0.
        raise click.UsageError(
            f"no [hosts.X] configured in {config.config_path()}"
        )
    names = sorted(cfg.hosts) if all_hosts else [host or cfg.default_host or "localhost"]
    probe_cache: dict[tuple[str, int], object] = {}
    payloads: list[dict[str, object]] = []
    for name in names:
        try:
            payloads.append(
                provision.diagnose_host(cfg, name, probe_cache=probe_cache)
            )
        except provision.ProvisionError as exc:
            raise click.UsageError(str(exc)) from None

    if as_json:
        # Shape is chosen by the FORM, never by how many hosts happen to be
        # configured: a --all consumer that got a bare host payload back on a
        # one-host fleet would break the day a second host is added.
        payload: dict[str, object] = (
            {
                "schema": provision.FLEET_SCHEMA,
                "hosts": {str(item["host"]): item for item in payloads},
                "ok": all(bool(item["ok"]) for item in payloads),
            }
            if all_hosts
            else payloads[0]
        )
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
    else:
        click.echo(
            "\n\n".join(provision.render_text(item) for item in payloads)
        )
        if not check_only:
            for item in payloads:
                fixes = provision.remediations(item)
                if not fixes:
                    continue
                click.echo(
                    f"\n-- remediation plan for {item['host']} "
                    "(each step needs root on that host) --"
                )
                for index, fix in enumerate(fixes, start=1):
                    click.echo(f"{index}. {fix}")

    if not all(bool(item["ok"]) for item in payloads):
        raise SystemExit(1)


@admin.command("provision-user")
@click.argument("user")
def admin_provision_user(user: str) -> None:
    """v0.6.x: create a user's multi-user state directory.

    \b
      vq admin provision-user UID         (by numeric uid)
      vq admin provision-user USERNAME    (by name)

    On a multi-user host per-user job state lives under
    ``/var/lib/vq/users/<uid>/`` and that tree is root-owned — an
    unprivileged user cannot create their own subdir, so their
    first ``vq submit`` fails with PermissionError.

    The multi-user daemon auto-provisions a state dir for every
    ``admin_group`` member at startup. This verb does the same for
    a user who is NOT in the admin group: run it once, as root, and
    that user's ``vq submit`` works thereafter.

    Idempotent — safe to re-run (existing dirs are kept; ownership
    is re-applied). Must run as root: the chown to the target uid
    requires it. Multi-user mode only — a no-op concept in
    single-user mode, where state lives under ``~/.local/share/vq/``.
    """
    import pwd  # noqa: PLC0415

    cfg = config.load_config()
    if not _multi_user_active(cfg):
        raise click.UsageError(
            "`vq admin provision-user` applies only on a multi-user "
            "host. This host is single-user (no /etc/vq/config.toml "
            "with [multi_user] enabled) — state lives under "
            "~/.local/share/vq/, which `vq submit` creates itself."
        )
    if os.geteuid() != 0:
        raise click.UsageError(
            "must run as root — provisioning chowns the new state "
            "dir to the target user, which requires root. Re-run:\n"
            f"  sudo vq admin provision-user {user}"
        )
    try:
        pw = pwd.getpwuid(int(user)) if user.isdigit() else pwd.getpwnam(user)
    except KeyError:
        raise click.UsageError(f"no such user: {user!r} (not in the passwd database)") from None
    try:
        paths.provision_user_state(pw.pw_uid, pw.pw_gid)
    except OSError as e:
        raise click.ClickException(f"failed to provision state dir for {pw.pw_name}: {e}") from None
    click.echo(
        f"provisioned {paths.user_dir(pw.pw_uid)} "
        f"for {pw.pw_name} (uid={pw.pw_uid} gid={pw.pw_gid})"
    )


@admin.command("auto-update")
@click.argument("env_or_host", required=False)
@click.argument("host_if_env", required=False)
@click.option(
    "--all",
    "all_envs",
    is_flag=True,
    default=False,
    help='v0.6.49: drift-check + apply for EVERY `kind="venv"` '
    "program in the registry. Combines with --all-hosts for "
    "fleet-wide refresh. Per-env failure isolated — one env's "
    "ls-remote error doesn't abort the sweep. Mutually exclusive "
    "with an explicit ENV positional.",
)
@click.option(
    "--all-hosts",
    "all_hosts",
    is_flag=True,
    default=False,
    help="v0.6.49: sequentially delegate the verb to every host in "
    "`[hosts.*]`. Combines with --all for the full env × host "
    "matrix. Per-host failure isolated; non-zero exit if any "
    "host failed.",
)
@click.option(
    "--dry-run",
    "dry_run",
    is_flag=True,
    default=False,
    help="Report what would happen but don't apply. Tag policy validates "
    "the strict SemVer remote-tag inventory plus the local named refs and "
    "exact SHAs; branch policy compares HEAD with origin/<branch>. This is "
    "the cheapest way to ask `is there drift?`.",
)
@click.option(
    "--scheduler-runtimes",
    "scheduler_runtimes",
    is_flag=True,
    default=False,
    help="Retired, fail-closed compatibility flag. Scheduler runtimes are "
    "updated only by `vq admin rollout-latest` from an accepted release "
    "report.",
)
@click.option(
    "--scheduler-driver-reentry",
    "scheduler_driver_reentry",
    default=None,
    hidden=True,
    metavar="DRIVER",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="v0.6.11: emit JSON instead of the text summary. Output carries "
    "decision (env, action, reason, policy, current_tag, target_tag, "
    "current_sha, target_sha) plus optional update_result or branch-mode "
    "build_submit. Implies machine-readable mode — exit code still "
    "reflects success/failure for shell wrappers. v0.6.49: --all "
    "emits a JSON array of per-env objects; --all-hosts emits a "
    "top-level object keyed by host.",
)
@click.option(
    "--token",
    "cli_token",
    default=None,
    metavar="TOKEN",
    help="v0.6.48: bearer token for the admin gate in multi-user "
    "mode. DISCOURAGED — the token leaks into shell history and "
    "`ps -ef` argv. Prefer $VQ_TOKEN env var, --token-stdin, or "
    "--token-file. Generate with `vq web init-token`.",
)
@click.option(
    "--token-stdin",
    "token_stdin",
    is_flag=True,
    default=False,
    help="v0.6.48: read the bearer token from a single line on stdin. "
    "Use to keep the token out of `ps -ef` and shell history. "
    "Mutually exclusive with --token / --token-file.",
)
@click.option(
    "--token-file",
    "token_file",
    type=click.Path(dir_okay=False),
    default=None,
    metavar="PATH",
    help="v0.6.48: read the bearer token from a 0600-mode file. "
    "Same perm enforcement as ~/.config/vq/web-token. Mutually "
    "exclusive with --token / --token-stdin.",
)
@click.option(
    "--detach",
    "detach",
    is_flag=True,
    default=False,
    help="Run this auto-update in its own session and return once it has "
    "started, printing a JSON receipt naming the run. Its progress and exact "
    "outcome are then read with `vq admin observe-update RUN_ID`. A delegated "
    "`vq admin auto-update ENV HOST` adds this to the command it sends, so a "
    "dropped SSH session can no longer kill the rebuild it applies; you rarely "
    "need to pass it by hand. Only valid for an auto-update on the host "
    "running it.",
)
@click.option("--detach-run-id", "detach_run_id", default=None, hidden=True)
@click.option("--detach-child", "detach_child", is_flag=True, default=False, hidden=True)
def admin_auto_update(
    env_or_host: str | None,
    host_if_env: str | None,
    all_envs: bool,
    all_hosts: bool,
    dry_run: bool,
    scheduler_runtimes: bool,
    scheduler_driver_reentry: str | None,
    as_json: bool,
    cli_token: str | None,
    token_stdin: bool,
    token_file: str | None,
    detach: bool,
    detach_run_id: str | None,
    detach_child: bool,
) -> None:
    """Check ENV for drift under its configured auto-update policy; apply if needed.

    v0.6.49: --all / --all-hosts extend the verb to fleet-wide refresh.

    \b
    Forms:
      vq admin auto-update ENV                  (default_host)
      vq admin auto-update ENV HOST             (explicit host)
      vq admin auto-update ENV --dry-run        (probe only, no apply)
      vq admin auto-update --all                (every venv env, default_host)
      vq admin auto-update --all HOST           (every venv env, explicit host)
      vq admin auto-update ENV --all-hosts      (one env, every host — v0.6.49)
      vq admin auto-update --all --all-hosts    (every env, every host — v0.6.49)

    \b
    Decision logic is selected by ENV's ``auto_update_policy``. Tag mode
    resolves the newest SemVer tag (with proper prerelease precedence),
    refuses downgrades, and delegates to ``update_env`` with the exact tag.
    Branch mode fetches and compares ``HEAD`` with ``origin/<branch>`` and
    submits a capped, deduplicated ``build-env`` job. Branch-mode vq daemon
    self-targets are rejected before submission; use ``vq self-update`` with
    an exact SHA or accepted report so restart and provenance are required.

    In tag mode, non-SemVer tags on the remote are ignored. If you have an
    irregular tag naming scheme, run `vq admin update ENV --tag X` manually.

    Per-env / per-host failure isolation: --all keeps sweeping after
    one env's ls-remote / apply failure; --all-hosts keeps sweeping
    after one host's SSH / config failure. Exit code non-zero if any
    env or host hit an error or apply-failure; the operator gets the
    full per-env / per-host report regardless.

    Exit code:
      * 0 — every env / host returned skip or successful apply
      * non-zero — at least one decision="error" or apply=FAILED

    --dry-run never applies; exit code follows the decisions.
    """
    from vq import auto_update as auto_update_module

    # Standalone scheduler-runtime tag discovery was not bound to one accepted
    # fleet report, so it could mix release lanes. Retain the spelling only as
    # a fail-closed compatibility flag and reject it before config/token reads,
    # driver routing, ref discovery, status inspection, or mutation.
    if scheduler_runtimes:
        raise click.UsageError(
            auto_update_module.SCHEDULER_RUNTIME_AUTO_UPDATE_DISABLED_REASON
        )
    # v0.6.49: --all changes the positional shape. Without --all the
    # form is ENV [HOST]; with --all the single positional (if any)
    # is HOST.
    env: str | None
    host: str | None
    if all_envs:
        if host_if_env is not None:
            raise click.UsageError(
                "with --all, pass at most a HOST positional "
                "(`vq admin auto-update --all [HOST]`), not `ENV HOST`"
            )
        env = None
        host = env_or_host  # may be None → default_host
        if all_hosts and env_or_host is not None:
            raise click.UsageError(
                "with --all --all-hosts, no positional argument is "
                "allowed (every env on every host)"
            )
    else:
        if env_or_host is None:
            raise click.UsageError(
                'ENV is required (the registry name of a kind="venv" '
                "program), or pass --all to refresh every venv env"
            )
        env = env_or_host
        host = host_if_env

    # Reject the detach combination a later branch would silently drop, as
    # `admin update` does: a flag quietly ignored on some route leaves the
    # operator believing the rebuild is protected when it is not.
    if (detach or detach_child) and all_hosts:
        raise click.UsageError(
            "--detach runs one auto-update on the host it is invoked on; "
            "--all-hosts walks every configured host from here"
        )

    cfg = config.load_config()

    # Resolve before --all-hosts so every per-host closure receives the same
    # local credential; target token-file selection remains per host.
    token = _resolve_admin_token(
        cfg,
        command_label="admin auto-update",
        cli_token=cli_token,
        token_stdin=token_stdin,
        token_file=token_file,
    )

    # ----- helpers shared by the local + remote (--all-hosts) paths -----

    def _format_outcome_text(o: auto_update_module.AutoUpdateOutcome) -> str:
        """Render one outcome in the v0.6.11 multi-line text shape."""
        lines = [
            f"env:          {o.decision.env_name}",
            f"action:       {o.decision.action}",
            f"reason:       {o.decision.reason}",
            f"current_tag:  {o.decision.current_tag or '(none)'}",
            f"target_tag:   {o.decision.target_tag or '(none)'}",
        ]
        if dry_run:
            lines.append("(dry-run: no apply attempted)")
        elif o.build_submit is not None:
            # v0.12.x fix 2: branch-mode drift routed through a capped
            # build-env JOB on the local daemon (vs an inline rebuild).
            lines.append(f"apply:        build job {o.build_submit.action}")
            lines.append(f"  - {o.build_submit.reason}")
        elif o.update_result is not None:
            if o.update_result.success:
                lines.append("apply:        OK")
            else:
                lines.append("apply:        FAILED")
                # v0.6.49: pre-existing code path read `.errors` on
                # the dataclass, which doesn't exist — only worked by
                # accident because the old body echoed FAILED before
                # entering the for-loop that AttributeError'd. The
                # actual field is `.work_errors`.
                for err in o.update_result.work_errors:
                    lines.append(f"  - {err}")
        return "\n".join(lines)

    def _outcome_to_json(o: auto_update_module.AutoUpdateOutcome) -> dict[str, object]:
        from dataclasses import asdict as _asdict

        return {
            "decision": _asdict(o.decision),
            "dry_run": dry_run,
            "update_result": (_asdict(o.update_result) if o.update_result is not None else None),
            "build_submit": (_asdict(o.build_submit) if o.build_submit is not None else None),
        }

    def _outcome_failed(o: auto_update_module.AutoUpdateOutcome) -> bool:
        if o.decision.action == "error":
            return True
        # v0.12.x: a build-job submit only "fails" the verb when the submit
        # itself errored. submitted / deduped / backed_off are healthy
        # outcomes — the build runs async and reports its own verdict.
        if o.build_submit is not None:
            return o.build_submit.action == "error"
        return bool(o.update_result is not None and not o.update_result.success)

    def _run_local(h: str) -> tuple[str, bool]:
        """Run the verb on a local host (ENV or --all). Returns
        (rendered_output, any_failed)."""
        if all_envs:
            try:
                outcomes = auto_update_module.auto_update_all(
                    cfg,
                    host=h,
                    dry_run=dry_run,
                    admin_token=token,
                )
            except admin_module.AdminUpdateInProgress as e:
                # Marker-present (a prior update's leftover) is a
                # runtime/state condition, not a usage error — render as
                # `Error: …` (exit 1), not the `Usage:` banner.
                raise click.ClickException(str(e)) from None
            except admin_module.AdminError as e:
                raise click.UsageError(str(e)) from None
            any_failed = any(_outcome_failed(o) for o in outcomes)
            if as_json:
                import json as _json

                payload = [_outcome_to_json(o) for o in outcomes]
                return _json.dumps(payload, indent=2, sort_keys=True, default=str), any_failed
            # Text mode: stack per-env blocks under banners.
            chunks: list[str] = []
            for o in outcomes:
                chunks.append(f"---- {o.decision.env_name} ----")
                chunks.append(_format_outcome_text(o))
                chunks.append("")
            n_total = len(outcomes)
            n_ok = sum(1 for o in outcomes if not _outcome_failed(o))
            chunks.append(f"auto-update --all: {n_ok}/{n_total} envs OK")
            return "\n".join(chunks), any_failed
        # Single env.
        assert env is not None
        try:
            outcome = auto_update_module.auto_update_env(
                env,
                cfg,
                host=h,
                dry_run=dry_run,
                admin_token=token,
            )
        except admin_module.AdminUpdateInProgress as e:
            # Marker-present is a runtime/state condition, not a usage
            # error — see the --all branch above.
            raise click.ClickException(str(e)) from None
        except admin_module.AdminError as e:
            raise click.UsageError(str(e)) from None
        any_failed = _outcome_failed(outcome)
        if as_json:
            import json as _json

            return _json.dumps(
                _outcome_to_json(outcome), indent=2, sort_keys=True, default=str
            ), any_failed
        return _format_outcome_text(outcome), any_failed

    # ----- --all-hosts: sequential per-host delegation -----

    if all_hosts:
        failures: list[str] = []

        def _one_host(h: str) -> str:
            skipped = _admin_fanout_scheduler_skip(cfg, h, as_json=as_json)
            if skipped is not None:
                return skipped
            if is_local_host(h):
                output, any_failed = _run_local(h)
                if any_failed:
                    failures.append(h)
                return output
            # Remote delegate. Build the equivalent verb for the
            # target host; forward the token via --token-stdin so it
            # never lands on argv (carries the v0.6.48 fix).
            remote_args: list[str] = ["admin", "auto-update"]
            if all_envs:
                remote_args.append("--all")
            else:
                assert env is not None
                remote_args.append(env)
            if dry_run:
                remote_args.append("--dry-run")
            if as_json:
                remote_args.append("--json")
            try:
                if dry_run:
                    # A probe mutates nothing, so there is no rebuild for a
                    # dropped session to cut off and nothing to detach.
                    return _forward_admin_command(
                        h,
                        cfg,
                        remote_args,
                        token=token,
                        append_localhost=True,
                        mutation_possible=False,
                    ).rstrip()
                # A drift apply is a real rebuild on the target, so it runs
                # detached exactly like a delegated `admin update`.
                return _forward_venv_admin_update(
                    h,
                    cfg,
                    remote_args,
                    token=token,
                ).rstrip()
            except click.UsageError:
                # Preserve the pre-helper boundary: auth/config lookup errors
                # are rendered per host but are not remote-update failures.
                raise
            except click.ClickException:
                failures.append(h)
                raise

        if as_json:
            # v0.7.6: fan-out in parallel via _aggregate_per_host_json
            # (same shape as `admin update --all-hosts --json`).
            import json as _json

            payload = _aggregate_per_host_json(cfg, _one_host)
            click.echo(_json.dumps(payload, indent=2, sort_keys=True))
            if failures:
                raise click.ClickException(
                    f"admin auto-update --all-hosts: {len(failures)} "
                    f"host(s) failed ({', '.join(sorted(failures))})"
                )
            return
        click.echo(_aggregate_per_host(cfg, _one_host))
        if failures:
            raise click.ClickException(
                f"admin auto-update --all-hosts: {len(failures)} "
                f"host(s) failed ({', '.join(sorted(failures))})"
            )
        return

    # ----- single-host path -----

    host = _resolve_host(cfg, host)

    def _run_local_auto_update() -> None:
        """Perform the auto-update here, exactly as an attached invocation would.

        Named rather than inlined so the detached child runs the identical
        work: its stdout becomes the receipt's payload and its failure the
        receipt's exit status, so the driver prints and exits as this did.
        """
        assert host is not None
        output, any_failed = _run_local(host)
        click.echo(output)
        if any_failed:
            if all_envs:
                raise click.ClickException(
                    "admin auto-update --all: at least one env hit an "
                    "error or apply-failure (see per-env detail above)"
                )
            raise click.ClickException("auto-update apply step failed")

    if detach or detach_child:
        if not is_local_host(host):
            raise click.UsageError(
                "--detach runs the auto-update on the host it is invoked on; "
                "the delegating driver adds it to the command it sends, so it "
                "cannot also name a remote HOST"
            )
        multi_user = _multi_user_active(cfg)
        if detach_child:
            # This process IS the detached auto-update. It already lives in
            # its own session, so a dropped transport cannot signal the
            # rebuild it applies.
            if detach_run_id is None:
                raise click.UsageError(
                    "--detach-child requires the --detach-run-id it must publish under"
                )
            try:
                admin_detached.validate_run_id(detach_run_id)
            except admin_detached.DetachedRunError as e:
                raise click.UsageError(str(e)) from None
            raise SystemExit(
                admin_module.run_detached_update_child(
                    detach_run_id,
                    _run_local_auto_update,
                    multi_user=multi_user,
                )
            )
        run_id = detach_run_id or admin_detached.new_run_id()
        try:
            admin_detached.validate_run_id(run_id)
        except admin_detached.DetachedRunError as e:
            raise click.UsageError(str(e)) from None
        try:
            receipt = admin_module.launch_detached_update(
                run_id=run_id,
                target=env or "all-envs",
                child_argv=_detached_auto_update_child_argv(
                    run_id,
                    env=env,
                    all_envs=all_envs,
                    dry_run=dry_run,
                    as_json=as_json,
                ),
                token=token,
                multi_user=multi_user,
            )
        except admin_module.AdminError as e:
            raise click.ClickException(str(e)) from None
        click.echo(json.dumps(receipt, indent=2, sort_keys=True))
        return

    if is_local_host(host):
        _run_local_auto_update()
        return

    # Remote single-host delegate. v0.6.48: forward the token via
    # --token-stdin (matches v0.6.46's `admin update` forwarding
    # pattern). v0.6.49: same for --all (the verb shape doesn't
    # change; just the leaf positional vs --all).
    remote_args = ["admin", "auto-update"]
    if all_envs:
        remote_args.append("--all")
    else:
        assert env is not None
        remote_args.append(env)
    if dry_run:
        remote_args.append("--dry-run")
    if as_json:
        remote_args.append("--json")
    if dry_run:
        # A probe mutates nothing: it stays attached, and a lost response is
        # not reported as an ambiguous write.
        click.echo(
            _forward_admin_command(
                host,
                cfg,
                remote_args,
                token=token,
                append_localhost=True,
                mutation_possible=False,
            ),
            nl=False,
        )
        return
    # The drift apply is a real rebuild on the target, and an attached one
    # dies with the SSH session that carries it. It runs detached like a
    # delegated `admin update`: a dropped transport costs a poll, a lost launch
    # response is adopted by run id, and only a run that cannot be observed is
    # still reported as an unknown outcome.
    try:
        delegated = _forward_venv_admin_update(
            host,
            cfg,
            remote_args,
            token=token,
        )
    except _DetachedRunFailed as failed:
        # One host owns this stdout, so print the recorded report before the
        # classification, exactly as a local auto-update and a single-host
        # `admin update` do. The --all-hosts fan-out does not: its JSON must
        # stay one document.
        if failed.payload:
            click.echo(failed.payload, nl=False)
        raise
    click.echo(delegated, nl=False)


@main.command(help="Fleet-wide summary: running, queued, and health status across all hosts.")
@click.argument("host", required=False)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Emit structured JSON instead of the text overview.",
)
@click.option(
    "--since-hours",
    "since_hours",
    default=24,
    type=click.IntRange(min=1),
    metavar="N",
    show_default=True,
    help="v0.6.21: window for the 'recent terminal counts' section. "
    "Default 24h covers a typical workday for a fleet doing "
    "overnight runs; raise for longer history. Doesn't affect "
    "the queue-counts section (those are current state, not "
    "windowed).",
)
@click.option(
    "--recommend",
    "recommend",
    is_flag=True,
    default=False,
    help="v0.7.18 *Kay's Object*: instead of the formatted fleet "
    "overview, print just the host name of the best-loaded "
    "candidate for a new submission. Ranks healthy reachable "
    "non-drained hosts by `running_cpus + pending_cpus` ascending "
    "(least loaded wins), tiebreaks by pending_cpus then by "
    "idle_seconds. Exits non-zero if no host qualifies (every "
    "host unreachable / drained / dead daemon). Composes with "
    "shell: `vq submit $(vq overview --recommend) my.py`.",
)
def overview(
    host: str | None,
    as_json: bool,
    since_hours: int,
    recommend: bool,
) -> None:
    """Fleet summary — running / queued / health / versions.

    \b
    Forms:
      vq overview                  # fleet summary across every host
      vq overview HOST             # just that host
      vq overview --json           # machine-readable
      vq overview --since-hours 72 # widen the recent-window

    Per host, the summary shows:

    \b
    * vq version
    * daemon health (verdict from `vq daemon health` + memory pressure)
    * queue counts by state (running / pending / suspended / completed / …)
    * recent terminal counts (within --since-hours window)
    * env versions (from `vq admin status`) + dirty flag
    * admin-update marker if present (state machine in-flight)

    Single-host mode runs ONE SSH call per host (remote-side
    overview does all the heavy lifting and emits one JSON blob).
    Fleet mode iterates configured hosts; unreachable hosts surface
    their error inline rather than aborting the whole sweep.
    """
    from datetime import timedelta as _td

    from vq import overview as _overview_module

    cfg = config.load_config()
    recent_window = _td(hours=since_hours)

    def _gather_one(h: str) -> _overview_module.HostOverview:
        if is_local_host(h):
            return _overview_module.gather_overview_local(
                h,
                cfg,
                recent_window=recent_window,
                multi_user=_multi_user_active(cfg),
            )
        try:
            host_cfg = cfg.host(h)
        except config.ConfigError as e:
            return _overview_module.HostOverview(
                host=h,
                reachable=False,
                error=str(e),
            )
        if host_cfg.scheduler != "local":
            return _overview_module.gather_scheduler_overview(
                h,
                host_cfg,
                cfg,
                recent_window=recent_window,
                multi_user=_multi_user_active(cfg),
            )
        return _overview_module.gather_overview_remote(
            h,
            host_cfg,
            recent_window=recent_window,
        )

    overviews: list[_overview_module.HostOverview] = []
    if host is not None:
        # Single-host mode (positional given)
        h = (
            host
            if host == "localhost" or is_local_host(host) or host in cfg.hosts
            else _resolve_host(cfg, host)
        )
        overviews.append(_gather_one(h))
    else:
        # Fleet mode — iterate every host in config (plus localhost
        # if not already represented). Default behavior matches
        # `vq queue --all` / `vq admin status --all` (walk every
        # configured host).
        host_names = list(cfg.hosts.keys())
        if not host_names:
            # No config — fall back to localhost
            host_names = ["localhost"]
        # v0.10.0 *Lampson's Hint*: skip administratively-down hosts
        # (`vq host down`) — surface them as DOWN without an SSH probe so the
        # sweep doesn't hang on an unreachable box. (Single-host mode above
        # is NOT filtered: an explicit `vq overview HOST` still probes, so
        # you can check whether a down host has come back.)
        down = host_status.load_down()
        for h in host_names:
            entry = down.get(h)
            if entry is not None:
                overviews.append(
                    _overview_module.HostOverview(
                        host=h, reachable=False, admin_down=entry.describe()
                    )
                )
                continue
            overviews.append(_gather_one(h))

    if recommend:
        # v0.7.18: short-circuit to the recommend ranking. --json
        # and --recommend are mutually exclusive in spirit (the
        # output is a single host name on stdout); we don't error
        # on the combo, just let --recommend win.
        chosen = _overview_module.recommend_host(overviews)
        if chosen is None:
            raise click.ClickException(
                "no host qualifies for recommendation "
                "(every host unreachable / drained / dead daemon)"
            )
        click.echo(chosen)
        return
    if as_json:
        click.echo(_overview_module.format_fleet_overview_json(overviews))
    else:
        click.echo(_overview_module.format_fleet_overview_text(overviews))


@main.group("host")
def host_group() -> None:
    """Mark hosts administratively up / down (client-side).

    A DOWN host is skipped by fan-out probes — `vq overview` / `summary`,
    and the `--all` form of `vq queue` / `vq programs` / `vq admin status`
    — and refuses new submits, so a fleet sweep doesn't hang on an
    unreachable or deliberately-offline box. This is your local view only;
    it never touches the remote daemon. Temporary by design: `vq host up
    HOST` clears it.

    \b
      vq host down workstation --reason "mobile link, probe later"
      vq host up workstation
      vq host list
    """


@host_group.command("retirement-audit")
@click.argument("hostname")
def host_retirement_audit_cmd(hostname: str) -> None:
    """Print retained-evidence digests for an explicit host retirement.

    This reads local journals only. A digest is not retirement authorization;
    record the dated maintainer decision in [fleet.retired_hosts.HOST].
    """
    from vq import fleet_rollout

    try:
        bindings = {}
        for run in fleet_rollout._all_rollout_runs():
            if (hostname in run.legacy_retained_holds or any(
                isinstance(raw, dict) and raw.get("host") == hostname
                for raw in run.legacy_retained_actions.values()
            )):
                bindings[run.rollout_id] = fleet_rollout.retained_host_audit_digest(run, hostname)
        if not bindings:
            raise click.ClickException(f"no retained rollout evidence for {hostname!r}")
        click.echo(json.dumps({"host": hostname, "retained_receipts": bindings}, indent=2))
    except fleet_rollout.FleetRolloutError as exc:
        raise click.ClickException(str(exc)) from exc


@host_group.command("down")
@click.argument("hostname")
@click.option(
    "--reason",
    default="",
    help="Why it's down — shown in `vq host list` and fleet sweeps.",
)
def host_down_cmd(hostname: str, reason: str) -> None:
    """Mark HOSTNAME administratively down (skip probing + refuse submits)."""
    cfg = config.load_config()
    if hostname not in cfg.hosts and not is_local_host(hostname):
        known = ", ".join(sorted(cfg.hosts)) or "(none configured)"
        click.echo(
            f"warning: {hostname!r} is not a configured host "
            f"(known: {known}) — marking it down anyway.",
            err=True,
        )
    entry = host_status.mark_down(hostname, reason)
    suffix = f" — {entry.reason}" if entry.reason else ""
    click.echo(f"{hostname}: marked DOWN{suffix}")


@host_group.command("up")
@click.argument("hostname")
def host_up_cmd(hostname: str) -> None:
    """Clear HOSTNAME's administrative-down mark."""
    if host_status.mark_up(hostname):
        click.echo(f"{hostname}: marked UP")
    else:
        click.echo(f"{hostname}: was not marked down (nothing to clear)")


@host_group.command("list")
def host_list_cmd() -> None:
    """Show every configured host and its up / down status."""
    cfg = config.load_config()
    down = host_status.load_down()
    names = sorted(set(cfg.hosts) | set(down))
    if not names:
        click.echo(f"(no hosts configured — add [hosts.X] sections to {config.config_path()})")
        return
    width = max(len(n) for n in names)
    for name in names:
        entry = down.get(name)
        if entry is not None:
            click.echo(f"{name.ljust(width)}  DOWN  {entry.describe()}")
        elif name in cfg.hosts:
            click.echo(f"{name.ljust(width)}  up")
        else:
            click.echo(f"{name.ljust(width)}  up    (not in config)")


@main.command()
@click.argument("host", required=False)
@click.option(
    "--by",
    "group_by",
    type=click.Choice(["tag", "submitter", "host", "none"]),
    default="tag",
    show_default=True,
    help="Group usage rows by tag, submitter, host, or a single total row.",
)
@click.option(
    "--include-active",
    is_flag=True,
    default=False,
    help="Include currently RUNNING/SUSPENDED jobs using elapsed active time so far.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Emit a stable JSON object instead of the text table.",
)
def usage(
    host: str | None,
    group_by: str,
    include_active: bool,
    as_json: bool,
) -> None:
    """Summarise CPU-hours from retained job specs.

    \b
    Forms:
      vq usage HOST
      vq usage HOST --by submitter
      vq usage HOST --include-active
      vq usage HOST --json

    Terminal jobs need parseable ``started_at`` and ``finished_at``. Active jobs
    are included only with ``--include-active``. Scheduler-backed jobs prefer the
    scheduler-reported walltime when available, so pbs-cluster usage avoids charging
    time a qsub job spent queued before compute started.
    """
    from vq import usage as _usage_module

    cfg = config.load_config()
    explicit_host = host is not None
    h = _resolve_host(cfg, host)
    down_entry = host_status.is_down(h) if not explicit_host else None
    if down_entry is not None and not as_json and not is_local_host(h):
        click.echo(
            f"vq: default_host {h!r} is marked down ({down_entry.describe()}); "
            "showing localhost usage instead.",
            err=True,
        )
        h = "localhost"

    def _render_one(target: str) -> str:
        scheduler_target = (
            target if _scheduler_driver_host(cfg, target) is not None else None
        )
        if scheduler_target is not None:
            driver = _scheduler_driver_host(cfg, target)
            assert driver is not None
            if is_local_host(driver):
                specs = [
                    spec
                    for spec in list_jobs(driver, multi_user=_multi_user_active(cfg))
                    if spec.scheduler_target == scheduler_target
                ]
                report = _usage_module.build_usage_report(
                    specs,
                    host=scheduler_target,
                    group_by=group_by,
                    include_active=include_active,
                )
                return (
                    _usage_module.format_usage_json(report)
                    if as_json
                    else _usage_module.format_usage_table(report)
                )
            remote_args = ["usage", scheduler_target, "--by", group_by]
            if include_active:
                remote_args.append("--include-active")
            if as_json:
                remote_args.append("--json")
            return _delegate_to_remote(driver, cfg, *remote_args).rstrip("\n")

        if is_local_host(target):
            specs = list_jobs(target, multi_user=_multi_user_active(cfg))
            report = _usage_module.build_usage_report(
                specs,
                host=target,
                group_by=group_by,
                include_active=include_active,
            )
            return (
                _usage_module.format_usage_json(report)
                if as_json
                else _usage_module.format_usage_table(report)
            )

        remote_args = ["usage", "localhost", "--by", group_by]
        if include_active:
            remote_args.append("--include-active")
        if as_json:
            remote_args.append("--json")
        return _delegate_to_remote(target, cfg, *remote_args).rstrip("\n")

    try:
        click.echo(_render_one(h))
    except click.ClickException as exc:
        if (
            not explicit_host
            and not as_json
            and not is_local_host(h)
            and _click_exception_is_remote_transport_failure(exc)
        ):
            click.echo(
                f"vq: default_host {h!r} is unreachable; showing localhost "
                f"usage instead. Use `vq usage {h}` to retry that host, or "
                f"`vq host down {h} --reason REASON` to mark it down.",
                err=True,
            )
            h = "localhost"
            click.echo(_render_one(h))
            return
        raise


@main.command()
@click.argument("host", required=False)
@click.option(
    "-w",
    "--watch",
    "watch_interval",
    is_flag=False,
    flag_value=2.0,
    default=None,
    type=float,
    callback=_top_watch_interval,
    help=(
        "Auto-refresh every N seconds (finite, greater than zero, and at most "
        "86400; default 2) until Ctrl-C."
    ),
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    help="Emit rows as a JSON array instead of a table (for scripts/dashboards).",
)
def top(host: str | None, watch_interval: float | None, as_json: bool) -> None:
    """Live per-job resource snapshot (CPU% / RSS / wall-time) of RUNNING jobs.

    \b
    Forms:
      vq top              # running jobs on default_host
      vq top HOST         # running jobs on HOST
      vq top --watch      # live, auto-refresh every 2s (Ctrl-C to exit)
      vq top -w 5         # live, every 5s
      vq top --json       # machine-readable rows

    A top(1)-style table read from the watchdog's per-job samples:

    \b
    * CPU%    — whole process group; an 8-core job saturating its cores
                reads ~800%. ``*`` flags a stale sample (>30s old).
    * RSS/MEM/MEM% — current resident memory vs the job's ``--mem-mb`` ceiling.
    * ACTIVE  — wall-clock since dispatch minus paused time.
    * ELAPSED/WALL/WALL% — wall-clock and active runtime vs wall-time ceiling.

    Complements ``vq overview`` (host-level) and ``vq status`` (one job).
    HOST is optional when ``default_host`` is set in the config.
    """
    import time

    from vq import top as _top_module
    from vq.spec import utcnow_iso

    cfg = config.load_config()
    h = host or cfg.default_host
    if h is None:
        raise click.UsageError(
            "no HOST given and no default_host in config; pass a host or set default_host"
        )
    if watch_interval is not None and as_json:
        raise click.UsageError("--watch and --json are mutually exclusive")
    explicit_host = host
    down_entry = host_status.is_down(h) if explicit_host is None else None
    if (
        down_entry is not None
        and not as_json
        and not is_local_host(h)
    ):
        click.echo(
            f"vq: default_host {h!r} is marked down ({down_entry.describe()}); "
            "showing localhost instead.",
            err=True,
        )
        h = "localhost"

    def _render() -> str:
        scheduler_target = h if _scheduler_driver_host(cfg, h) is not None else None
        if scheduler_target is not None:
            driver = _scheduler_driver_host(cfg, h)
            assert driver is not None
            if is_local_host(driver):
                specs = [
                    spec
                    for spec in list_jobs(driver, multi_user=_multi_user_active(cfg))
                    if spec.scheduler_target == scheduler_target
                ]
                rows = _top_module.gather_top_rows(specs, host=scheduler_target)
                return (
                    _top_module.format_top_json(rows)
                    if as_json
                    else _top_module.format_top_table(rows)
                )
            remote_args = ["top", scheduler_target, *(["--json"] if as_json else [])]
            text = _delegate_to_remote(driver, cfg, *remote_args).rstrip("\n")
            if as_json:
                return _json_rows_with_requested_queue_handle_host(text, h)
            return text
        if is_local_host(h):
            return _top_module.show_top_local(
                h, multi_user=_multi_user_active(cfg), as_json=as_json
            )
        # Delegate `vq top localhost [--json]` to the remote and reuse its
        # rendering (a fresh SSH per refresh in --watch mode).
        remote_args = ["top", "localhost", *(["--json"] if as_json else [])]
        text = _delegate_to_remote(h, cfg, *remote_args).rstrip("\n")
        if as_json:
            return _json_rows_with_requested_queue_handle_host(text, h)
        return text

    if watch_interval is None:
        try:
            click.echo(_render())
        except click.ClickException as exc:
            if (
                explicit_host is None
                and not as_json
                and not is_local_host(h)
                and _click_exception_is_remote_transport_failure(exc)
            ):
                click.echo(
                    f"vq: default_host {h!r} is unreachable; showing localhost "
                    f"instead. Use `vq top {h}` to retry that host, or "
                    f"`vq host down {h} --reason REASON` to mark it down.",
                    err=True,
                )
                h = "localhost"
                click.echo(_render())
                return
            raise
        return
    _top_module.watch_loop(
        _render,
        interval=watch_interval,
        host=h,
        sleep=time.sleep,
        write=lambda s: click.echo(s, nl=False),
        clock=utcnow_iso,
    )


@main.command(help="Print the RPC audit trail — every admin action across the fleet.")
@click.option(
    "--since",
    "since",
    type=str,
    default=None,
    metavar="DUR",
    help="v0.8.9 *Cook's Hierarchy*: filter to entries newer than "
    "DUR ago (e.g. '1h', '30m', '7d'). Same format as "
    "`--duration` elsewhere. Default: no time filter.",
)
@click.option(
    "--uid",
    "uid_filter",
    type=int,
    default=None,
    metavar="N",
    help="Filter to entries from caller uid=N. In single-user "
    "mode SO_PEERCRED isn't always available (macOS); those "
    "entries have uid=null and won't match any --uid filter.",
)
@click.option(
    "--method",
    "method_filter",
    type=str,
    default=None,
    metavar="NAME",
    help="Filter to entries for method NAME. Accepts a trailing "
    "'*' for prefix match — `--method 'set_*'` matches every "
    "set_admin_status / set_drain_state / set_throttle_state "
    "call (the only methods that get audited, but useful when "
    "future v0.8.x ships add more set_*).",
)
@click.option(
    "--tail",
    "tail_n",
    type=click.IntRange(min=1),
    default=100,
    show_default=True,
    metavar="N",
    help="Print the last N entries (after other filters). Default "
    "100 — enough to read the recent operator activity at a "
    "glance without paging.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Emit raw JSONL (one envelope per line) instead of the "
    "human-formatted table. Stable schema matches the on-disk "
    "format: {ts, method, uid, ok, args_summary, error?}.",
)
@click.option(
    "--all-hosts",
    "all_hosts",
    is_flag=True,
    default=False,
    help="Aggregate the audit trail across every host in "
    "~/.config/vq/config.toml (parallel fan-out, v0.7.6 "
    "*Tanenbaum's Mailbox*). Per-host failures land inline. "
    "Mutually exclusive with positional HOST.",
)
@click.argument("host", required=False)
def audit(
    since: str | None,
    uid_filter: int | None,
    method_filter: str | None,
    tail_n: int,
    as_json: bool,
    all_hosts: bool,
    host: str | None,
) -> None:
    """Print the RPC audit trail — every admin action across the fleet.

    \b
    Forms:
      vq audit                        (local, last 100 entries)
      vq audit --since 1h             (last hour's entries)
      vq audit --method set_drain_state
      vq audit --uid 1001             (only entries from uid=1001)
      vq audit HOST                   (delegate via SSH)
      vq audit --all                  (parallel fan-out)
      vq audit --json                 (raw JSONL for scripting)

    Surfaces the audit log written by every ``set_*`` RPC call
    (v0.8.6 *Codd's Audit*). Schema per line: ``ts``, ``method``,
    ``uid``, ``ok``, ``args_summary``, optionally ``error``.

    Filters compose: ``--since 6h --method 'set_drain_state' --uid
    1001`` answers "what drains did user 1001 do in the last six
    hours?" Useful forensics:

    * "Who drained the queue at 03:14?" — ``vq audit --since 4h
      --method set_drain_state``
    * "Who's been clearing throttle most often?" — ``vq audit
      --since 7d --method set_throttle_state --json | jq
      'select(.args_summary == \"clear\") | .uid' | sort | uniq -c``

    Tokens are never in the audit summary (the v0.8.6 redaction
    contract is preserved on the read path too).
    """
    import json as _json
    from datetime import UTC, datetime

    from vq import audit as _audit

    if all_hosts and host is not None:
        raise click.UsageError(
            "--all and HOST are mutually exclusive; --all walks "
            "every configured host, so naming one in addition is "
            "contradictory",
        )

    cfg = config.load_config()

    # Parse --since once.
    since_cutoff: datetime | None = None
    if since is not None:
        try:
            delta = parse_age(since)
        except ValueError as e:
            raise click.UsageError(f"--since: {e}") from None
        since_cutoff = datetime.now(UTC) - delta

    # Compile --method into a predicate (support prefix glob).
    if method_filter is not None and method_filter.endswith("*"):
        method_prefix = method_filter[:-1]
        method_pred = lambda m: isinstance(m, str) and m.startswith(method_prefix)  # noqa: E731
    elif method_filter is not None:
        method_pred = lambda m: m == method_filter  # noqa: E731
    else:
        method_pred = None

    def _filter_and_render_local() -> str:
        from vq.config import load_config as _load_cfg

        try:
            mu = _load_cfg().multi_user.enabled
        except Exception:  # noqa: BLE001
            mu = False
        lines = _audit.read_audit_log(multi_user=mu)
        filtered: list[dict] = []
        for line in lines:
            if since_cutoff is not None:
                try:
                    line_ts = datetime.fromisoformat(line.get("ts", ""))
                    if line_ts < since_cutoff:
                        continue
                except (ValueError, TypeError):
                    # Malformed ts → keep it (don't silently drop a
                    # forensic record over a parse error).
                    pass
            if uid_filter is not None and line.get("uid") != uid_filter:
                continue
            if method_pred is not None and not method_pred(line.get("method")):
                continue
            filtered.append(line)
        # --tail applied after filters.
        filtered = filtered[-tail_n:]
        if as_json:
            return "\n".join(_json.dumps(entry) for entry in filtered)
        if not filtered:
            return "(no audit entries match the filter)"
        # Human table: ts | method | uid | ok | args_summary [| error]
        rows = []
        for entry in filtered:
            ts = entry.get("ts", "?")
            method = entry.get("method", "?")
            uid = entry.get("uid")
            uid_str = str(uid) if uid is not None else "-"
            ok = "ok" if entry.get("ok") else "FAIL"
            summary = entry.get("args_summary", "")
            row = f"{ts}  {ok:4}  uid={uid_str:6}  {method:24}  {summary}"
            if not entry.get("ok") and entry.get("error"):
                row += f"  | error: {entry['error']}"
            rows.append(row)
        return "\n".join(rows)

    def _query_one_remote(h: str) -> str:
        delegate_args = ["audit"]
        if since is not None:
            delegate_args.extend(["--since", since])
        if uid_filter is not None:
            delegate_args.extend(["--uid", str(uid_filter)])
        if method_filter is not None:
            delegate_args.extend(["--method", method_filter])
        delegate_args.extend(["--tail", str(tail_n)])
        if as_json:
            delegate_args.append("--json")
        delegate_args.append("localhost")
        return _delegate_to_remote(h, cfg, *delegate_args).rstrip()

    def _query_one_text(h: str) -> str:
        if is_local_host(h):
            return _filter_and_render_local()
        return _query_one_remote(h)

    if all_hosts:
        if not cfg.hosts:
            click.echo("(no [hosts.X] configured)")
            return
        click.echo(_aggregate_per_host(cfg, _query_one_text))
        return

    if host is not None and not is_local_host(host):
        host = _resolve_host(cfg, host)
        click.echo(_query_one_remote(host))
        return

    click.echo(_filter_and_render_local())


@main.group()
@click.pass_context
def daemon(ctx: click.Context) -> None:
    """Daemon control."""
    if ctx.invoked_subcommand != "run":
        _setup_cli_invocation_logging()


@daemon.command("start")
@click.option(
    "--max-cpus",
    type=click.IntRange(min=1),
    default=None,
    help="Concurrent CPU budget. Defaults to os.cpu_count().",
)
@click.option(
    "--max-jobs",
    type=click.IntRange(min=1),
    default=None,
    help="Maximum number of jobs running concurrently. Default: unlimited "
    "(controlled solely by --max-cpus). Set to 1 for strict serial "
    "execution where each job may use the full --max-cpus budget.",
)
@click.option(
    "--max-mem-mb",
    type=click.IntRange(min=1),
    default=None,
    help="Memory budget (MB) for concurrent jobs. Defaults to host total "
    "from /proc/meminfo (Linux only); macOS dev daemons run without "
    "this gate. Bookkeeping in v0.3 (only fires when both this AND "
    "the per-job --mem-mb are set); enforcement via cgroups v2 in v0.4.",
)
@click.option(
    "--poll-interval",
    type=click.FloatRange(min=0.01),
    default=1.0,
    show_default=True,
    help="Seconds between dispatch/reconcile passes.",
)
def daemon_start(
    max_cpus: int | None,
    max_jobs: int | None,
    max_mem_mb: int | None,
    poll_interval: float,
) -> None:
    """REMOVED in v0.6.0 — use the systemd-user unit instead.

    \b
    Recovery:
      cp contrib/vq-daemon.service ~/.config/systemd/user/
      systemctl --user daemon-reload
      systemctl --user enable --now vq-daemon
      vq daemon health        # verify

    See docs/lifecycle.md § "What vq requires" for the full
    contract. Pre-v0.6.0 this command spawned a daemon as a
    detached background process and wrote ``<state_root>/daemon.pid``
    as a competing identity source against systemd's MainPID —
    these could disagree, and `vq daemon health` (v0.5.49+) would
    report FAIL when they did. Removing the second identity source
    closes that whole class of failure mode.
    """
    raise click.UsageError(
        "`vq daemon start` was removed in v0.6.0. Use the systemd-user "
        "unit (contrib/vq-daemon.service) instead. Quick recipe:\n"
        "  cp contrib/vq-daemon.service ~/.config/systemd/user/\n"
        "  systemctl --user daemon-reload\n"
        "  systemctl --user enable --now vq-daemon\n"
        "  vq daemon health        # verify\n"
        "See docs/lifecycle.md for the full contract."
    )


@daemon.command("reload")
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Emit the RPC result as JSON instead of a text line.",
)
def daemon_reload(as_json: bool) -> None:
    """Make the running daemon re-read config.toml.

    \b
    Why this exists:
      The daemon caches config-derived state — most importantly one
      SchedulerDispatcher per scheduler host, which snapshots that
      host's `scheduler_program_hooks` when it is first built. Before
      this verb there was no way to make a running daemon notice a
      config fix: `vq daemon start` is removed and nothing handled
      SIGHUP, so a full restart was the only path. On 2026-07-22 that
      meant a command_wrapper fix sat on disk while the daemon
      dispatched an entire released backlog with the old config.

    \b
    What a reload does:
      * clears the scheduler-dispatcher cache, so the next dispatch
        renders job scripts from the config now on disk;
      * refuses (loudly, in the daemon log) if the file does not parse,
        keeping the previously loaded config rather than adopting a
        broken one;
      * does NOT change `multi_user` in place — that selects the state
        layout, queue lock, RPC socket, and pidfile. Restart for that.

    In-flight jobs keep the dispatcher they were submitted with.

    `kill -HUP <daemon-pid>` does the same thing. A plain edit to
    config.toml is also picked up automatically at the next dispatch;
    this verb is the immediate, confirmable form.
    """
    from vq import rpc as _rpc
    try:
        cfg = config.load_config()
    except Exception:  # noqa: BLE001 — a broken config is exactly when you reload
        cfg = None
    multi_user = _multi_user_active(cfg)
    # set_config_reload is token-gated in multi-user mode like every other
    # mutating RPC. Without forwarding a token the verb could never succeed on
    # a multi-user daemon — it would always come back PermissionError.
    from vq import auth as _auth_mod

    call_args: dict[str, object] = {}
    if multi_user:
        token = _auth_mod.resolve_token(None)
        if not token:
            raise click.ClickException(
                "daemon reload: an admin token is required in multi-user mode. "
                "Provide it via $VQ_TOKEN or ~/.config/vq/web-token."
            )
        call_args["token"] = token
    try:
        result = _rpc.call("set_config_reload", call_args, multi_user=multi_user)
    except ConnectionError as e:
        raise click.ClickException(
            f"could not reach the daemon's RPC socket: {e}\n"
            "  hint: the daemon may not be running, or predates this verb.\n"
            "  next: `vq daemon status`, then either "
            "`kill -HUP <daemon-pid>` or "
            "`systemctl --user restart vq-daemon`."
        ) from None
    except _rpc.RPCError as e:
        raise click.ClickException(f"daemon refused the reload: {e}") from None
    if as_json:
        import json as _json

        click.echo(_json.dumps(result, indent=2, sort_keys=True))
        return
    click.echo(
        "config reload queued; the daemon applies it on its next "
        "iteration (see the daemon log for the outcome)"
    )


@daemon.command("stop")
def daemon_stop() -> None:
    """Send SIGTERM to the running daemon and wait for it to exit."""
    try:
        pid = stop_daemon()
    except TimeoutError as e:
        raise click.ClickException(str(e)) from None
    if pid is None:
        click.echo("daemon: not running")
    else:
        click.echo(f"daemon stopped (pid {pid})")


def _launchd_daemon_plist(
    *,
    label: str,
    python_path: Path,
    working_directory: Path,
    max_cpus: int | None,
    max_jobs: int | None,
    max_mem_mb: int | None,
    web_host: str,
    web_port: int,
) -> bytes:
    """Build a macOS launchd plist for ``vq daemon run --web``."""
    import plistlib  # noqa: PLC0415

    argv = [
        str(python_path),
        "-m",
        "vq",
        "daemon",
        "run",
    ]
    if max_cpus is not None:
        argv.extend(["--max-cpus", str(max_cpus)])
    if max_jobs is not None:
        argv.extend(["--max-jobs", str(max_jobs)])
    if max_mem_mb is not None:
        argv.extend(["--max-mem-mb", str(max_mem_mb)])
    argv.extend(
        [
            "--web",
            "--web-host",
            web_host,
            "--web-port",
            str(web_port),
        ]
    )
    data: dict[str, object] = {
        "Label": label,
        "ProgramArguments": argv,
        "RunAtLoad": True,
        "KeepAlive": {"SuccessfulExit": False},
        "WorkingDirectory": str(working_directory),
        "StandardOutPath": str(paths.state_root() / "daemon-launchd.out"),
        "StandardErrorPath": str(paths.state_root() / "daemon-launchd.err"),
    }
    return plistlib.dumps(data, sort_keys=False)


@daemon.command("install")
@click.option(
    "--manager",
    default=None,
    type=click.Choice(["systemd-user", "systemd-system", "launchd"]),
    help="Service manager [default: detected].",
)
@click.option(
    "--unit-name",
    default=None,
    help="Service name [default: vq-daemon].",
)
@click.option(
    "--service-user",
    default=None,
    help="Unprivileged account for --manager systemd-system (required).",
)
@click.option(
    "--no-start",
    is_flag=True,
    default=False,
    help="Install and enable, but do not start it now.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Print every file and command, change nothing.",
)
@click.option(
    "--allow-dropping-unit-flags",
    is_flag=True,
    default=False,
    help="Proceed even though the existing unit's ExecStart carries capacity "
    "caps that [daemon] does not. Only after you have decided those caps are "
    "not wanted -- the generated unit will not carry them.",
)
def daemon_install(
    manager: str | None,
    unit_name: str | None,
    service_user: str | None,
    no_start: bool,
    dry_run: bool,
    allow_dropping_unit_flags: bool,
) -> None:
    """Install the daemon as a supervised service on this host.

    The counterpart of ``vq web install``, and it exists for the same reason.
    Across the reference fleet the daemon's units disagreed -- one referenced
    ``%h/.local/bin/vq``, two hardcoded a pre-split venv path -- because
    nothing owned them. Repointing the symlink moved one host and left the
    other two running the old vq after a restart, with their config and their
    symlink both looking correct; only ``vq doctor``'s ``daemon_rpc`` version
    showed it.

    The unit points at the vq you ran this with, and records that it does, so
    "which vq owns this daemon" has one answer and it is on disk.

    It carries no capacity caps. Those belong in ``[daemon]`` in this host's
    config, which is what makes them survive a unit rewrite -- unit rewrites
    are exactly what has dropped them before. If the unit being replaced
    hardcodes caps that ``[daemon]`` does not have, this refuses and prints
    the section to add.
    """
    from vq.web import install as service_install  # noqa: PLC0415

    kind = service_install.DAEMON_SERVICE
    try:
        chosen = manager or service_install.detect_manager()
        name = unit_name or kind.unit_name
        plan = service_install.build_plan(
            manager=chosen,
            unit_name=name,
            start=not no_start,
            service_user=service_user,
            kind=kind,
        )
        try:
            cfg = config.load_config()
        except config.ConfigError as exc:
            raise click.UsageError(str(exc)) from None
        unit_write = next(
            (w for w in plan.writes if w.purpose == "service unit"), None,
        )
        dropped = (
            service_install.daemon_caps_not_in_config(unit_write.path, cfg.daemon)
            if unit_write is not None
            else []
        )
        if dropped and not allow_dropping_unit_flags:
            fields = "\n".join(
                f"{service_install.DAEMON_CAP_FLAGS[flag]} = <value>"
                for flag in dropped
            )
            raise service_install.InstallError(
                f"the existing unit {unit_write.path} runs the daemon with "
                f"{', '.join(dropped)}, and this host's [daemon] config has "
                "no equivalent. A generated unit carries no caps, so "
                "installing now would silently drop them.\n\n"
                f"Add them to {config.config_path()}:\n\n"
                f"[daemon]\n{fields}\n\n"
                "then re-run. Or pass --allow-dropping-unit-flags if the caps "
                "are not wanted."
            )
        if dry_run:
            click.echo(plan.render())
            click.echo("\n(dry run — nothing was written)")
            return
        if chosen == "systemd-system" and os.geteuid() != 0:
            raise service_install.InstallError(
                "systemd-system installation must run as root (normally via "
                "sudo); the generated service itself runs as --service-user"
            )
        for line in service_install.apply_plan(plan):
            click.echo(line)
        for note in plan.notes:
            click.echo(f"note: {note}")
        if dropped:
            click.echo(
                "note: dropped hardcoded caps "
                f"{', '.join(dropped)} from the previous unit",
                err=True,
            )
    except service_install.InstallError as e:
        raise click.ClickException(str(e)) from None


@daemon.command("launchd-plist")
@click.option(
    "--label",
    default="com.vq.daemon",
    show_default=True,
    help="launchd service label.",
)
@click.option(
    "--python",
    "python_path",
    type=click.Path(path_type=Path, dir_okay=False),
    default=Path(sys.executable),
    show_default=True,
    help="Absolute Python executable to run `python -m vq daemon run`.",
)
@click.option(
    "--working-directory",
    type=click.Path(path_type=Path, file_okay=False),
    default=Path.cwd,
    show_default="current directory",
    help="WorkingDirectory for launchd.",
)
@click.option("--max-cpus", type=click.IntRange(min=1), default=None)
@click.option("--max-jobs", type=click.IntRange(min=1), default=None)
@click.option("--max-mem-mb", type=click.IntRange(min=1), default=None)
@click.option("--web-host", default="127.0.0.1", show_default=True)
@click.option("--web-port", type=click.IntRange(min=1, max=65535), default=8768, show_default=True)
@click.option(
    "--output",
    type=click.Path(path_type=Path, dir_okay=False),
    default=None,
    help="Write the plist to this path instead of stdout.",
)
def daemon_launchd_plist(
    label: str,
    python_path: Path,
    working_directory: Path,
    max_cpus: int | None,
    max_jobs: int | None,
    max_mem_mb: int | None,
    web_host: str,
    web_port: int,
    output: Path | None,
) -> None:
    """Render a macOS launchd plist for a daemon-owned dashboard.

    The command does not register the service. Inspect the generated plist,
    then install it with ``launchctl bootstrap`` when ready.
    """
    if not python_path.is_absolute():
        raise click.UsageError("--python must be an absolute path")
    if not working_directory.is_absolute():
        raise click.UsageError("--working-directory must be an absolute path")
    body = _launchd_daemon_plist(
        label=label,
        python_path=python_path,
        working_directory=working_directory,
        max_cpus=max_cpus,
        max_jobs=max_jobs,
        max_mem_mb=max_mem_mb,
        web_host=web_host,
        web_port=web_port,
    )
    if output is None:
        click.echo(body.decode("utf-8"), nl=False)
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(body)
    click.echo(f"wrote launchd plist to {output}")


def _local_daemon_ping(
    timeout: float,
    *,
    verbose: bool = False,
) -> tuple[int, dict]:
    """v0.8.2 helper: execute one ping against the local daemon socket.
    Returns (exit_code, envelope_dict). Used by both the local CLI
    path and the JSON-aggregation path under ``--all``.

    v0.8.3 split out from the click verb so the same shape can be
    re-used by the remote / --all paths via subprocess delegation
    (which parses the JSON envelope back out).

    v0.8.4: with ``verbose=True`` also calls ``get_methods`` after
    the ping succeeds and adds a ``methods`` field to the envelope.
    A get_methods failure on a successful ping (daemon is too old to
    know the method) is non-fatal — ``methods`` lands as ``None``,
    the rest of the envelope is unchanged.
    """
    try:
        cfg = config.load_config()
    except Exception:  # noqa: BLE001 — config load mustn't block ping
        cfg = None
    mu = _multi_user_active(cfg)
    return _probe_local_daemon(timeout, multi_user=mu, verbose=verbose)


def _format_ping_text(envelope: dict) -> str:
    """Render a single-host ping envelope as one human line. Matches
    the v0.8.2 output shape so existing tests + operator muscle
    memory survive the v0.8.3 refactor.

    v0.8.4: when ``methods`` is present (verbose ping), appends
    ``| methods=[...]`` to the ok line. Methods list is comma-joined
    for terminal readability; the JSON envelope keeps the array.
    """
    if envelope["ok"]:
        line = (
            f"daemon RPC: ok | version={envelope['version']} "
            f"| source_sha={envelope['source_sha'] or '?'} "
            f"| multi_user={envelope['multi_user']} "
            f"| socket={envelope['socket_path']} "
            f"| latency={envelope['latency_ms']}ms"
        )
        if envelope.get("methods") is not None:
            methods = envelope["methods"]
            line += f" | methods=[{', '.join(methods)}]"
        return line
    return (
        f"daemon RPC: FAIL | socket={envelope['socket_path']} "
        f"| latency={envelope['latency_ms']}ms "
        f"| error={envelope['error']}"
    )


@daemon.command("ping")
@click.argument("host", required=False)
@click.option(
    "--all",
    "all_hosts",
    is_flag=True,
    default=False,
    help="v0.8.3: ping EVERY host in ~/.config/vq/config.toml in "
    "parallel. Per-host SSH failures land inline as "
    '``{"error": "..."}`` (JSON) or a one-line FAIL banner '
    "(text) so one bad host doesn't hide the rest. Mutually "
    "exclusive with positional HOST.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Emit JSON {ok, version, multi_user, socket_path, latency_ms, "
    "error} instead of the human line. With --all, the output is "
    "a top-level dict keyed by host. Stable schema for monitoring.",
)
@click.option(
    "--timeout",
    type=float,
    callback=_finite_positive_seconds,
    default=2.0,
    show_default=True,
    help="RPC connect/read timeout in seconds. Short by design — a "
    "hung daemon should fail loud, not block the caller. Only "
    "affects the LOCAL daemon socket call; remote-host ping uses "
    "the SSH transport's own timeouts.",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="v0.8.4: include the daemon's RPC method list in the "
    "envelope (calls ``get_methods`` after ``ping``). Useful for "
    "client-server version-skew debugging and for monitoring "
    "scripts that gate on method availability before calling. "
    "Methods appear as ``methods=[...]`` in text output and "
    '``"methods": [...]`` in JSON. JSON also includes strict, read-only '
    "evidence for the target system multi-user config, the capability-probed "
    "responding process, and the fixed multi-user systemd unit. Auxiliary "
    "identity failures remain metadata and do not change daemon liveness.",
)
def daemon_ping(
    host: str | None,
    all_hosts: bool,
    as_json: bool,
    timeout: float,
    verbose: bool,
) -> None:
    """v0.8.2 *Lamport's Logical* — probe the daemon via its RPC
    socket. Returns the daemon's version + multi-user flag + the
    socket round-trip latency.

    v0.8.3 *Dijkstra's Shortest* — extended to remote hosts and
    parallel fleet-wide via ``--all``.

    \b
    Forms:
      vq daemon ping               (default_host — local socket)
      vq daemon ping HOST          (SSH-delegate to HOST)
      vq daemon ping --all         (parallel fan-out over every host)
      vq daemon ping --json        (machine-readable envelope)

    Distinct from ``vq daemon status`` (which checks the pidfile —
    can lie if the daemon crashed mid-process) and ``vq daemon
    health`` (which does the full lifecycle contract verification
    — heavier, multi-second). ``ping`` is the smallest possible
    "is the daemon actually responsive?" check, useful for:

    * CI / monitoring scripts that need a quick liveness probe.
    * Confirming a daemon restart finished before issuing follow-up
      commands.
    * ``vq daemon ping --all`` for "is the entire fleet alive?"
      from one shell.

    Exit codes (single-host path only — ``--all`` always exits 0
    so per-host failures don't hide the rest):

    * 0 — daemon reachable; RPC responded.
    * 1 — socket missing (daemon down or not started yet).
    * 2 — socket present but the daemon didn't respond cleanly
          (protocol error, timeout, crash mid-handshake).
    """
    import json as _json

    if all_hosts and host is not None:
        raise click.UsageError(
            "--all and HOST are mutually exclusive; --all walks "
            "every configured host, so naming one in addition is "
            "contradictory",
        )

    cfg = config.load_config()

    def _scheduler_ping_envelope(h: str) -> dict[str, object] | None:
        driver = _scheduler_driver_host(cfg, h)
        if driver is None:
            return None
        if is_local_host(driver):
            _, driver_env = _local_daemon_ping(timeout, verbose=verbose)
        else:
            raw = _delegate_to_remote(
                driver,
                cfg,
                "daemon",
                "ping",
                "--json",
                "--timeout",
                str(timeout),
                *(["--verbose"] if verbose else []),
                "localhost",
            ).rstrip()
            driver_env = _json.loads(raw or "{}")
        return {
            "ok": bool(driver_env.get("ok")),
            "scheduler_host": True,
            "scheduler": cfg.host(h).scheduler,
            "driver": driver,
            "message": "daemonless scheduler host; driver daemon owns dispatch",
            "driver_ping": driver_env,
        }

    def _query_one_text(h: str) -> str:
        scheduler_env = _scheduler_ping_envelope(h)
        if scheduler_env is not None:
            driver_ping = scheduler_env["driver_ping"]
            assert isinstance(driver_ping, dict)
            notice = _scheduler_host_notice(
                cfg,
                h,
                "driver daemon owns scheduler dispatch",
            )
            assert notice is not None
            return f"{notice}\n{_format_ping_text(driver_ping)}"
        if is_local_host(h):
            _, env = _local_daemon_ping(timeout, verbose=verbose)
            return _format_ping_text(env)
        delegate_args = ["daemon", "ping", "--timeout", str(timeout)]
        if verbose:
            delegate_args.append("--verbose")
        delegate_args.append("localhost")
        return _delegate_to_remote(h, cfg, *delegate_args).rstrip()

    def _query_one_json(h: str) -> str:
        scheduler_env = _scheduler_ping_envelope(h)
        if scheduler_env is not None:
            return _json.dumps(scheduler_env)
        if is_local_host(h):
            _, env = _local_daemon_ping(timeout, verbose=verbose)
            return _json.dumps(env)
        delegate_args = ["daemon", "ping", "--json", "--timeout", str(timeout)]
        if verbose:
            delegate_args.append("--verbose")
        delegate_args.append("localhost")
        return _delegate_to_remote(h, cfg, *delegate_args).rstrip()

    # --all path: parallel fan-out, exits 0 regardless of per-host
    # outcome so monitoring scripts can parse the aggregate.
    if all_hosts:
        if as_json:
            payload = _aggregate_per_host_json(cfg, _query_one_json)
            click.echo(_json.dumps(payload, indent=2, sort_keys=True))
            return
        if not cfg.hosts:
            click.echo("(no [hosts.X] configured)")
            return
        click.echo(_aggregate_per_host(cfg, _query_one_text))
        return

    # Single-host path: local or one remote.
    if host is not None and not is_local_host(host):
        # Remote single-host: delegate via SSH, exit code propagates
        # by parsing the envelope's ok flag (text path) or the JSON
        # ok field.
        try:
            host = _resolve_host(cfg, host)
        except click.UsageError:
            raise
        if as_json:
            raw = _query_one_json(host)
            click.echo(raw)
            try:
                env = _json.loads(raw)
                if not env.get("ok"):
                    raise SystemExit(1)
            except _json.JSONDecodeError:
                raise SystemExit(2) from None
        else:
            click.echo(_query_one_text(host))
        return

    # Local path (no host or host=='localhost').
    exit_code, envelope = _local_daemon_ping(timeout, verbose=verbose)
    if as_json:
        click.echo(_json.dumps(envelope, indent=2))
    elif exit_code == 0:
        click.echo(_format_ping_text(envelope))
    else:
        click.echo(_format_ping_text(envelope), err=True)
    if exit_code != 0:
        raise SystemExit(exit_code)


@daemon.command("status")
def daemon_status() -> None:
    """Show daemon status (running/not, pid, log location, unit provenance)."""
    pid = read_pidfile()
    if is_daemon_serving():
        if pid is None:
            click.echo("daemon: running (RPC healthy; no pidfile)")
        else:
            click.echo(f"daemon: running (pid {pid})")
    else:
        click.echo("daemon: not running")
    click.echo(f"  pidfile: {paths.daemon_pidfile()}")
    click.echo(f"  log:     {paths.daemon_logfile()}")
    click.echo(f"  queue:   {paths.queue_dir()}")
    click.echo(f"  jobs:    {paths.jobs_dir()}")
    _echo_daemon_service_provenance()


def _echo_daemon_service_provenance() -> None:
    """Say which vq installed this host's daemon unit, and whether it drifted.

    The drift this answers took three hosts and a fleet migration to notice:
    two units named a venv path that outlived the vq it pointed at, so
    repointing `~/.local/bin/vq` moved one host and left the others serving
    old code after a restart, with config and symlink both looking correct.
    ``vq doctor``'s ``daemon_rpc`` version eventually revealed it; this puts
    the same answer where somebody checking on the daemon already looks.
    """
    from vq.web import install as service_install  # noqa: PLC0415

    try:
        status = service_install.console_service_status(
            service_install.DAEMON_SERVICE
        )
    except Exception:  # noqa: BLE001 - provenance is additive to the status
        return
    if not status.installed:
        click.echo("  unit:    not installed by vq (`vq daemon install`)")
        return
    click.echo(f"  unit:    {status.unit_path} ({status.manager})")
    if status.drifted:
        click.echo(
            f"  ⚠ unit installed by vq {status.installed_by_version}, "
            f"this vq is {status.running_version} — re-run "
            "`vq daemon install` to re-point and restart",
            err=True,
        )
    else:
        click.echo(f"  owner:   vq {status.installed_by_version}")


@daemon.command("health")
@click.argument("host", required=False)
@click.option(
    "--all",
    "all_hosts",
    is_flag=True,
    default=False,
    help="v0.5.49: aggregate health across EVERY host in "
    "~/.config/vq/config.toml. Per-host failures (unreachable, "
    "ssh timeout) show inline; one bad host doesn't hide the "
    "rest. Mutually exclusive with positional HOST.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="v0.5.49: emit JSON instead of the text findings list. "
    "Field schema matches lifecycle.ContractVerdict. With --all, "
    "the output is a top-level dict keyed by host; per-host "
    'failures appear as `{"error": "..."}` so one bad host '
    "doesn't break parsing.",
)
def daemon_health(
    host: str | None,
    all_hosts: bool,
    as_json: bool,
) -> None:
    """v0.5.49: cross-check the four daemon-lifecycle sources of truth.

    \b
    Forms:
      vq daemon health              (default_host)
      vq daemon health HOST         (explicit host)
      vq daemon health --all        (every configured host)
      vq daemon health --json       (machine-readable)

    Sources cross-checked:
      * loginctl show-user $USER (login-layer state + runtime path)
      * pgrep 'systemd --user' (live user-manager PID, if any)
      * systemctl --user show vq-daemon.service (systemd's view +
        MainPID)
      * <state_root>/daemon.pid (pidfile written by `vq daemon start`)

    Diagnoses the 2026-05-17 workstation incident class: user-systemd
    detached from its service unit, systemctl unreachable, daemon
    actually gone — all in one verdict. Use this BEFORE running
    `vq admin update` if a host has been quiet for a while; it
    surfaces "user-systemd is broken" up-front instead of letting
    the update get half-way and fail at the daemon-restart step.

    Strictly read-only — never restarts, kills, or reconfigures.
    Recovery is the operator's call (see operations.md for recipes).
    """
    import json

    cfg = config.load_config()

    if all_hosts and host is not None:
        raise click.UsageError(
            "--all and HOST are mutually exclusive; --all walks every "
            "configured host, so naming one in addition is contradictory"
        )

    def _scheduler_health_json(h: str) -> str | None:
        driver = _scheduler_driver_host(cfg, h)
        if driver is None:
            return None
        if is_local_host(driver):
            verdict = lifecycle.verify_user_systemd_contract()
            driver_payload = json.loads(lifecycle.format_contract_verdict_json(verdict))
        else:
            raw = _delegate_to_remote(
                driver,
                cfg,
                "daemon",
                "health",
                "--json",
                "localhost",
            ).rstrip()
            driver_payload = json.loads(raw or "{}")
        payload = {
            "ok": bool(driver_payload.get("ok")),
            "scheduler_host": True,
            "scheduler": cfg.host(h).scheduler,
            "driver": driver,
            "message": "daemonless scheduler host; driver daemon owns dispatch",
            "driver_health": driver_payload,
        }
        return json.dumps(payload, indent=2, sort_keys=True)

    def _scheduler_health_text(h: str) -> str | None:
        driver = _scheduler_driver_host(cfg, h)
        if driver is None:
            return None
        notice = _scheduler_host_notice(
            cfg,
            h,
            "driver daemon owns scheduler dispatch",
        )
        assert notice is not None
        if is_local_host(driver):
            verdict = lifecycle.verify_user_systemd_contract()
            driver_text = lifecycle.format_contract_verdict(verdict)
        else:
            driver_text = _delegate_to_remote(
                driver,
                cfg,
                "daemon",
                "health",
                "localhost",
            ).rstrip()
        return f"{notice}\n{driver_text}"

    def _query_one_text(h: str) -> str:
        scheduler_text = _scheduler_health_text(h)
        if scheduler_text is not None:
            return scheduler_text
        if is_local_host(h):
            v = lifecycle.verify_user_systemd_contract()
            return lifecycle.format_contract_verdict(v)
        return _delegate_to_remote(
            h,
            cfg,
            "daemon",
            "health",
            "localhost",
        ).rstrip()

    def _query_one_json(h: str) -> str:
        scheduler_json = _scheduler_health_json(h)
        if scheduler_json is not None:
            return scheduler_json
        if is_local_host(h):
            v = lifecycle.verify_user_systemd_contract()
            return lifecycle.format_contract_verdict_json(v)
        return _delegate_to_remote(
            h,
            cfg,
            "daemon",
            "health",
            "--json",
            "localhost",
        ).rstrip()

    if all_hosts:
        if as_json:
            import json as _json

            payload: dict[str, object] = {}
            for h in sorted(cfg.hosts.keys()):
                try:
                    payload[h] = _json.loads(_query_one_json(h))
                except click.ClickException as e:
                    payload[h] = {"error": str(e.message)}
                except _json.JSONDecodeError as e:
                    payload[h] = {
                        "error": f"remote returned invalid JSON: {e}",
                    }
                except Exception as e:  # noqa: BLE001 — per-host isolation
                    payload[h] = {"error": str(e)}
            click.echo(_json.dumps(payload, indent=2, sort_keys=True))
            return
        click.echo(_aggregate_per_host(cfg, _query_one_text))
        return

    host = _resolve_host(cfg, host)
    scheduler_json = _scheduler_health_json(host) if as_json else None
    scheduler_text = _scheduler_health_text(host) if not as_json else None
    if scheduler_json is not None:
        click.echo(scheduler_json)
        try:
            payload = json.loads(scheduler_json)
        except json.JSONDecodeError:
            raise SystemExit(2) from None
        if not payload.get("ok"):
            raise click.ClickException(f"daemon health: verdict FAILED on {host}")
    elif scheduler_text is not None:
        click.echo(scheduler_text)
    elif is_local_host(host):
        if as_json:
            v = lifecycle.verify_user_systemd_contract()
            click.echo(lifecycle.format_contract_verdict_json(v))
        else:
            v = lifecycle.verify_user_systemd_contract()
            click.echo(lifecycle.format_contract_verdict(v))
            if not v.ok:
                raise click.ClickException(f"daemon health: verdict FAILED on {host}")
    else:
        delegate_args = ["daemon", "health"]
        if as_json:
            delegate_args.append("--json")
        delegate_args.append("localhost")
        click.echo(
            _delegate_to_remote(host, cfg, *delegate_args),
            nl=False,
        )


@main.group()
def web() -> None:
    """Read-only web dashboard control (v0.5+)."""


@web.command("run")
@click.option(
    "--host",
    "host",
    default=None,
    help="Interface to bind [default: 127.0.0.1, or [web] bind]. "
    "Localhost-only by default. Read-only HTML pages and the OpenAPI "
    "doc are UNAUTHENTICATED unless fleet accounts exist: any "
    "non-loopback bind exposes every job's name, cwd, stdout/stderr "
    "tails, host metadata, and queue state to anyone who can reach the "
    "port. Only use 0.0.0.0 / a LAN IP behind a TLS reverse proxy with "
    "its own access control. Write actions remain token-gated either way.",
)
@click.option(
    "--port",
    "port",
    default=None,
    type=click.IntRange(min=1, max=65535),
    help="TCP port to bind [default: 8765, or [web] port].",
)
@click.option(
    "--log-level",
    "log_level",
    default=None,
    type=click.Choice(["critical", "error", "warning", "info", "debug", "trace"]),
    help="uvicorn log level [default: info, or [web] log_level].",
)
@click.option(
    "--i-understand-public-bind",
    "public_bind_ack",
    is_flag=True,
    default=None,
    help="Acknowledge the read-only exposure risk and suppress the "
    "non-loopback startup warning. No-op for loopback binds. Persist it "
    "with [web] public_bind_ack = true.",
)
@click.option(
    "--fleet/--no-fleet",
    "fleet",
    default=None,
    help="Fleet-console mode (docs/fleet_console.md): adds the /fleet "
    "host grid, the /fleet/jobs cross-host table, and the /api/v1/fleet "
    "JSON surface, backed by a background SSH sweep of every configured "
    "host (vq overview + queue listings). Run this on the coordinator "
    "host with SSH reach to the fleet — not on every sidecar. "
    "[default: off, or [web] fleet]. Sweep interval: [web] "
    "fleet_interval_seconds (default 30).",
)
def web_run(
    host: str | None,
    port: int | None,
    log_level: str | None,
    public_bind_ack: bool | None,
    fleet: bool | None,
) -> None:
    """Run the web dashboard in the foreground.

    Read endpoints (queue, job detail, health, /docs) are UNAUTHENTICATED
    and leak operational metadata if bound to a non-loopback interface
    without a fronting reverse proxy. Default bind is localhost-only.

    Write endpoints (POST /api/v1/jobs/<id>/kill / pause / resume) require
    a bearer token; configure with ``vq web init-token``.

    Every flag has a persistent equivalent in the ``[web]`` config
    section, so a deployment does not have to carry its whole
    configuration in one unit-file ExecStart line. Precedence is
    flag > environment > ``[web]`` > built-in default; ``vq web config``
    prints the resolved values with the layer each came from.

    For production, let ``vq web install`` write and enable the service
    unit — it records which vq installed it, so the console can never
    silently drift from the vq that owns it.
    """
    try:
        import uvicorn  # noqa: PLC0415 - optional dep, import inside the verb

        from vq.web import create_app  # noqa: PLC0415
        from vq.web import settings as web_settings  # noqa: PLC0415
    except ImportError as e:
        raise click.UsageError(
            f"vq web requires the 'web' extra ({e.name} is missing). "
            "Install with: pip install -e '.[web]'"
        ) from None

    resolved = web_settings.resolve_settings(
        bind=host,
        port=port,
        fleet=fleet,
        log_level=log_level,
        public_bind_ack=public_bind_ack,
    )
    load_failure = web_settings.config_load_failure()
    if load_failure is not None:
        # Starting anyway is deliberate; starting silently was not (#40).
        click.echo(
            "⚠️  vq web: the config file could not be loaded, so [web] is "
            "ignored and the console\n"
            "    runs on the environment and built-in defaults.\n"
            f"    {load_failure}",
            err=True,
        )
    # Fleet mode is an app-construction decision (routes are registered
    # at create_app() time), so hand the resolved settings straight to
    # the factory rather than trusting the module-level instance, which
    # may have been constructed under different settings.
    app = create_app(resolved)

    # v0.6.45: loud startup warning on any non-loopback bind. The
    # read-only pages have no auth (by design — single-user-laptop
    # dashboards don't need it), so exposing them on a LAN or public
    # interface leaks operational metadata. Localhost binds are silent.
    if resolved.needs_public_bind_warning:
        click.echo(
            f"⚠️  vq web binding to non-loopback interface {resolved.bind!r}:\n"
            "    Read-only HTML pages and /docs are UNAUTHENTICATED.\n"
            "    Anyone who can reach this port sees every job's name,\n"
            "    cwd, stdout/stderr tail, host metadata, and queue state.\n"
            "    Only safe behind a TLS reverse proxy with its own ACL.\n"
            "    Pass --i-understand-public-bind to silence this warning.",
            err=True,
        )

    # Logged, not echoed: a loopback start must stay silent on stderr
    # (pinned by tests/test_web_bind_warning_v0_6_45.py), and under a
    # service unit this is the line that lands in the journal and tells
    # you which console just came up.
    log.info(
        "vq console %r starting on %s (fleet=%s)",
        resolved.title,
        resolved.url(),
        resolved.fleet,
    )
    uvicorn.run(
        app,
        host=resolved.bind,
        port=resolved.port,
        log_level=resolved.log_level,
    )


def _is_loopback_bind(host: str) -> bool:
    """True if ``host`` is a loopback interface bind.

    Accepts the literal name ``"localhost"`` plus any address in the
    127.0.0.0/8 IPv4 loopback range or ``::1`` IPv6 loopback. Anything
    else (LAN IPs, ``0.0.0.0``, ``::``, public IPs, hostnames) is
    treated as non-loopback and triggers the audit warning.

    v0.25.0: the rule lives on :class:`vq.web.settings.WebSettings` so
    the CLI, the app factory and the installer cannot drift apart on what
    counts as "exposed". This wrapper is the CLI's stable entry point.

    An earlier revision of this docstring claimed the import "needs no
    web extra". That was false when written: ``vq.web.settings`` is
    itself dependency-free, but importing it executed ``vq/web/__init__``,
    which imported FastAPI at module level. It is true now only because
    that import block was deferred — see the note at the top of
    ``vq/web/__init__.py``.
    """
    from vq.web.settings import WebSettings  # noqa: PLC0415

    return WebSettings(bind=host).is_loopback_bind


@web.command("config")
@click.option("--json", "as_json", is_flag=True, default=False)
def web_config(as_json: bool) -> None:
    """Show the resolved console configuration and where each value came from.

    The question this answers is the one an operator actually has when a
    console does something unexpected: *which layer set this?* Values are
    resolved exactly as ``vq web run`` resolves them, minus command-line
    flags — those are per-invocation and by definition not part of the
    persistent configuration.
    """
    from vq.web import settings as web_settings  # noqa: PLC0415

    rows = web_settings.describe_sources()
    # The resolver falls back to defaults on a broken config, deliberately;
    # this verb must not present that fallback as the configuration (#40).
    load_failure = web_settings.config_load_failure()
    if as_json:
        click.echo(
            json.dumps({"settings": rows, "config_error": load_failure}, indent=2)
        )
        return

    resolved = web_settings.resolve_settings()
    width = max(len(str(row["name"])) for row in rows)
    click.echo("vq web — resolved configuration")
    click.echo("  (precedence: CLI flag > env > [web] in config > default)")
    if load_failure is not None:
        click.echo(
            "  ⚠️  the config file could not be loaded, so [web] was ignored: "
            "every value below\n"
            "      comes from the environment or a built-in default.\n"
            f"      {load_failure}",
            err=True,
        )
    click.echo("")
    for row in rows:
        name = str(row["name"]).ljust(width)
        click.echo(f"  {name}  {str(row['value']):<24} {row['source']}")
    click.echo("")
    click.echo(f"  URL: {resolved.url()}")
    if resolved.needs_public_bind_warning:
        click.echo(
            "  ⚠️  non-loopback bind without public_bind_ack — "
            "vq web run will warn at startup.",
            err=True,
        )


@web.command("install")
@click.option("--bind", default=None, help="Persist [web] bind.")
@click.option(
    "--port", default=None, type=click.IntRange(1, 65535), help="Persist [web] port."
)
@click.option(
    "--fleet/--no-fleet", "fleet", default=None, help="Persist [web] fleet."
)
@click.option(
    "--interval",
    "interval",
    default=None,
    type=click.IntRange(min=5),
    help="Persist [web] fleet_interval_seconds.",
)
@click.option("--title", default=None, help="Persist [web] title (header brand).")
@click.option(
    "--i-understand-public-bind",
    "public_bind_ack",
    is_flag=True,
    default=None,
    help="Persist [web] public_bind_ack for a deliberate non-loopback bind.",
)
@click.option(
    "--manager",
    default=None,
    help="Service manager: systemd-user, systemd-system, launchd-user. "
    "Default: auto-detected.",
)
@click.option(
    "--unit-name",
    default=None,
    help="Service name [default: vq-web]. Change it to run two consoles "
    "on one host.",
)
@click.option(
    "--service-user",
    default=None,
    help="Unprivileged account for --manager systemd-system (required).",
)
@click.option(
    "--no-start",
    is_flag=True,
    default=False,
    help="Install and enable, but do not start it now.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Print every file and command, change nothing.",
)
def web_install(
    bind: str | None,
    port: int | None,
    fleet: bool | None,
    interval: int | None,
    title: str | None,
    public_bind_ack: bool | None,
    manager: str | None,
    unit_name: str | None,
    service_user: str | None,
    no_start: bool,
    dry_run: bool,
) -> None:
    """Install the console as a supervised service on this host.

    Writes a service unit that points at the vq you ran this with, records
    which vq installed it, and enables + starts it. Settings you pass are
    persisted to the ``[web]`` config section rather than baked into the
    unit's command line, so changing the port later is a config edit and
    not a service-file rewrite.

    Run it again after upgrading vq to re-point and restart the console —
    ``vq web status`` tells you when that is due.
    """
    from vq.web import install as web_install_mod  # noqa: PLC0415

    try:
        # Ahead of everything, --dry-run included: a console unit pointed at a
        # vq that cannot import uvicorn starts, fails, and gets restarted
        # forever, and the only evidence is in the journal.
        web_install_mod.require_console_runtime()
        chosen = manager or web_install_mod.detect_manager()
        name = unit_name or web_install_mod.DEFAULT_UNIT_NAME

        wants_config = any(
            value is not None
            for value in (bind, port, fleet, interval, title, public_bind_ack)
        )
        section = ""
        if wants_config:
            section = web_install_mod.render_web_section(
                bind=bind,
                port=port,
                fleet=fleet,
                fleet_interval_seconds=interval,
                title=title,
                public_bind_ack=public_bind_ack,
            )

        plan = web_install_mod.build_plan(
            manager=chosen,
            unit_name=name,
            start=not no_start,
            service_user=service_user,
        )

        if dry_run:
            click.echo(plan.render())
            if section:
                click.echo("")
                if web_install_mod.config_has_web_section():
                    click.echo(
                        "config already has a [web] section — it would be "
                        "left alone. Merge these by hand:"
                    )
                else:
                    click.echo(f"--- append to {config.config_path()}:")
                click.echo(section.rstrip("\n"))
            click.echo("\n(dry run — nothing was written)")
            return

        if chosen == "systemd-system" and os.geteuid() != 0:
            raise web_install_mod.InstallError(
                "systemd-system installation must run as root (normally via sudo); "
                "the generated service itself runs as --service-user"
            )

        if section:
            if web_install_mod.config_has_web_section():
                click.echo(
                    "config already has a [web] section; leaving it alone.\n"
                    "Merge these settings by hand if you want them:",
                    err=True,
                )
                click.echo(section.rstrip("\n"), err=True)
            else:
                written = web_install_mod.append_web_section(section)
                click.echo(f"wrote [web] section to {written}")

        for line in web_install_mod.apply_plan(plan):
            click.echo(line)
        for note in plan.notes:
            click.echo(f"note: {note}")
    except web_install_mod.InstallError as e:
        raise click.ClickException(str(e)) from None

    from vq.web import settings as web_settings  # noqa: PLC0415

    resolved = web_settings.resolve_settings()
    click.echo("")
    click.echo(f"console will serve {resolved.url()}")
    if resolved.needs_public_bind_warning:
        click.echo(
            "⚠️  non-loopback bind without public_bind_ack. The read "
            "surface is unauthenticated until you create an account:\n"
            "      vq web user add <name> --role admin",
            err=True,
        )


@web.command("uninstall")
@click.option("--manager", default=None, help="Override the recorded manager.")
@click.option("--unit-name", default=None, help="Override the recorded name.")
@click.option(
    "--purge",
    is_flag=True,
    default=False,
    help="Also remove the provenance marker. Off by default so a "
    "reinstall can still see what the previous install did.",
)
@click.option("--dry-run", is_flag=True, default=False)
def web_uninstall(
    manager: str | None, unit_name: str | None, purge: bool, dry_run: bool
) -> None:
    """Stop, disable and remove the console service.

    Leaves the ``[web]`` config section and any accounts alone — this
    removes the service, not the configuration.
    """
    from vq.web import install as web_install_mod  # noqa: PLC0415

    marker = web_install_mod.read_install_marker() or {}
    chosen = manager or str(marker.get("manager") or "")
    name = unit_name or str(marker.get("unit_name") or web_install_mod.DEFAULT_UNIT_NAME)
    if not chosen:
        raise click.ClickException(
            "no recorded install on this host, and no --manager given. "
            "Pass --manager systemd-user (or systemd-system / launchd-user)."
        )
    try:
        plan = web_install_mod.build_uninstall_plan(
            manager=chosen, unit_name=name, purge=purge
        )
    except web_install_mod.InstallError as e:
        raise click.ClickException(str(e)) from None

    if dry_run:
        click.echo(plan.render())
        click.echo("\n(dry run — nothing was changed)")
        return

    if chosen == "systemd-system" and os.geteuid() != 0:
        raise click.ClickException(
            "systemd-system uninstall must run as root (normally via sudo)"
        )
    try:
        for line in web_install_mod.apply_uninstall_plan(plan):
            click.echo(line)
    except web_install_mod.InstallError as e:
        raise click.ClickException(str(e)) from None


@web.command("status")
@click.option("--json", "as_json", is_flag=True, default=False)
def web_status(as_json: bool) -> None:
    """Report the installed console service and whether it has drifted.

    Drift means the vq that installed the service is not the vq you are
    running now — so the console is serving older code than is installed.
    A console cannot notice this on its own: every page it renders looks
    completely normal.
    """
    from vq.web import install as web_install_mod  # noqa: PLC0415

    status = web_install_mod.console_service_status()
    if as_json:
        click.echo(json.dumps(asdict(status), indent=2))
        return

    if not status.installed:
        click.echo(status.detail or "no console service installed")
        click.echo("\nInstall one with:  vq web install")
        return

    click.echo(f"service:      {status.unit_name} ({status.manager})")
    click.echo(f"unit:         {status.unit_path}")
    click.echo(f"installed by: vq {status.installed_by_version}")
    click.echo(f"installed at: {status.installed_at}")
    click.echo(f"running vq:   {status.running_version}")
    if status.active is None:
        click.echo("active:       unknown")
    else:
        click.echo(f"active:       {'yes' if status.active else 'no'}")
    # Whether the unit's own interpreter can run the console at all. A unit
    # crash-looping on a missing extra can still read "active" between
    # restarts, so this is not implied by the line above (#28).
    if status.runtime_ok is None:
        if status.runtime_detail:
            click.echo(f"runtime:      unknown ({status.runtime_detail})")
    else:
        click.echo(f"runtime:      {'ok' if status.runtime_ok else 'BROKEN'}")
    if status.runtime_ok is False:
        click.echo("")
        click.echo(
            f"⚠️  the installed unit's vq cannot serve the console "
            f"({status.runtime_detail}), so it fails on every start.\n"
            f"{status.runtime_remedy or ''}",
            err=True,
        )
    if status.drifted:
        click.echo("")
        click.echo(
            f"⚠️  drift: this console service was installed by vq "
            f"{status.installed_by_version}, but vq "
            f"{status.running_version} is installed now. The running "
            f"console is serving the older code.\n"
            f"    Fix with:  vq web install",
            err=True,
        )


@web.group("user")
def web_user() -> None:
    """Manage fleet-console accounts (M2, docs/fleet_dashboard_design.md).

    With no accounts, fleet mode is open (tunnel posture) and write
    actions are disabled. The first `vq web user add` turns login on
    for every fleet page.
    """


@web_user.command("add")
@click.argument("username")
@click.option(
    "--role",
    type=click.Choice(["viewer", "operator", "admin"]),
    required=True,
    help="viewer = read only; operator = job kill/pause/resume; "
    "admin = operator + audit trail.",
)
@click.option(
    "--password-stdin",
    is_flag=True,
    default=False,
    help="Read the password from stdin (first line) instead of an "
    "interactive hidden prompt. For scripted provisioning; keeps the "
    "credential off argv.",
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Replace an existing account (rotates its password/role).",
)
def web_user_add(
    username: str, role: str, password_stdin: bool, force: bool
) -> None:
    """Create (or with --force replace) a fleet-console account."""
    from vq.web import authn  # noqa: PLC0415 — needs the web extra

    if password_stdin:
        password = sys.stdin.readline().rstrip("\n")
    else:
        password = click.prompt(
            f"password for {username}", hide_input=True, confirmation_prompt=True
        )
    if not password:
        raise click.UsageError("empty password")
    try:
        path = authn.add_user(username, password, role, force=force)
    except (FileExistsError, ValueError) as e:
        raise click.UsageError(str(e)) from None
    click.echo(f"wrote {username} (role {role}) to {path} (mode 0600)")


@web_user.command("list")
def web_user_list() -> None:
    """List fleet-console accounts (names + roles, never hashes)."""
    from vq.web import authn  # noqa: PLC0415

    users = authn.load_users()
    if not users:
        click.echo("no accounts configured (fleet console is open)")
        return
    for name in sorted(users):
        click.echo(f"{name}  {users[name].get('role', '?')}")


@web_user.command("remove")
@click.argument("username")
def web_user_remove(username: str) -> None:
    """Remove a fleet-console account."""
    from vq.web import authn  # noqa: PLC0415

    try:
        authn.remove_user(username)
    except FileNotFoundError as e:
        raise click.UsageError(str(e)) from None
    click.echo(f"removed {username}")


@web.command("init-token")
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Overwrite an existing token. Invalidates anyone holding the old one.",
)
@click.option(
    "--quiet",
    is_flag=True,
    default=False,
    help="v0.6.x: write the token file WITHOUT echoing the token to "
    "stdout. For scripted/automated setup (e.g. the multi-user deploy "
    "script) — keeps the credential out of terminal scrollback and "
    "logs. Retrieve it later with `sudo cat <token-file>`.",
)
def web_init_token(force: bool, quiet: bool) -> None:
    """Generate a fresh random bearer token and write it to the canonical
    config path (``~/.config/vq/web-token`` by default; mode 0600).

    Without this, web write endpoints return 503 ("auth not configured").
    Read-only endpoints work fine without a token.

    By default the token is echoed to stdout so an operator can copy it
    for API use. Pass ``--quiet`` to suppress that — the token is still
    written to the 0600 file; only the stdout echo is skipped. Scripted
    callers should always use ``--quiet`` so the credential does not
    land in terminal scrollback, CI logs, or pasted output.
    """
    from vq import auth  # noqa: PLC0415

    token = auth.generate_token()
    try:
        path = auth.write_token(token, force=force)
    except FileExistsError as e:
        raise click.UsageError(str(e)) from None
    if quiet:
        click.echo(f"wrote token to {path} (mode 0600) — retrieve with `cat`")
        return
    click.echo(f"wrote token to {path} (mode 0600)")
    click.echo()
    click.echo("Use this header in API calls:")
    click.echo(f"  Authorization: Bearer {token}")
    click.echo()
    click.echo("Example (kill a job):")
    click.echo(
        f"  curl -X POST -H 'Authorization: Bearer {token}' "
        f"http://localhost:8765/api/v1/jobs/<jobid>/kill"
    )


def _start_web_sidecar(
    *,
    host: str,
    port: int,
    log_level: str,
    acknowledge_public_bind: bool,
) -> subprocess.Popen[bytes]:
    """Start ``vq web run`` as a daemon-owned sidecar process.

    Used by ``vq daemon run --web`` so the browser dashboard survives
    beyond an interactive shell/Codex session and shuts down with the
    daemon. The child uses the same Python environment as the daemon.
    """
    argv = [
        sys.executable,
        "-m",
        "vq.cli",
        "web",
        "run",
        "--host",
        host,
        "--port",
        str(port),
        "--log-level",
        log_level,
    ]
    if acknowledge_public_bind:
        argv.append("--i-understand-public-bind")
    proc = subprocess.Popen(argv, stdin=subprocess.DEVNULL)
    # Catch immediate import/bind failures (missing web extra, occupied port)
    # at daemon startup instead of leaving the operator with a dead dashboard.
    time.sleep(0.5)
    rc = proc.poll()
    if rc is not None:
        raise click.ClickException(
            "daemon web sidecar exited during startup "
            f"(rc={rc}); try `vq web run --host {host} --port {port}` "
            "for the full error"
        )
    log.info("web dashboard sidecar listening on http://%s:%s", host, port)
    return proc


def _stop_web_sidecar(proc: subprocess.Popen[bytes]) -> None:
    """Terminate the daemon-owned web sidecar with bounded escalation."""
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5.0)
    except subprocess.TimeoutExpired:
        log.warning("web dashboard sidecar ignored SIGTERM; killing it")
        proc.kill()
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=5.0)


def _ensure_web_sidecar_running(
    proc: subprocess.Popen[bytes],
    *,
    host: str,
    port: int,
    log_level: str,
    acknowledge_public_bind: bool,
) -> subprocess.Popen[bytes]:
    """Restart the daemon-owned web sidecar if it exits mid-daemon run."""
    rc = proc.poll()
    if rc is None:
        return proc
    log.warning(
        "web dashboard sidecar exited unexpectedly (rc=%s); restarting",
        rc,
    )
    return _start_web_sidecar(
        host=host,
        port=port,
        log_level=log_level,
        acknowledge_public_bind=acknowledge_public_bind,
    )


@daemon.command("run")
@click.option("--max-cpus", type=click.IntRange(min=1), default=None)
@click.option("--max-jobs", type=click.IntRange(min=1), default=None)
@click.option(
    "--max-scheduler-jobs",
    type=click.IntRange(min=1),
    default=None,
    help="Maximum active scheduler-backed jobs this driver daemon may keep "
    "submitted/queued/running at once. Scheduler jobs do not consume the "
    "local --max-jobs cap because they run on the cluster, not this host. "
    "Default: unlimited.",
)
@click.option("--max-mem-mb", type=click.IntRange(min=1), default=None)
@click.option(
    "--default-job-mem-mb",
    type=click.IntRange(min=1),
    default=None,
    help="Assumed memory (MB) for a job that declares no --mem-mb. When set, "
    "an undeclared job is charged this against the memory budget AND capped "
    "at it via cgroup, so an undeclared job can no longer oversubscribe the "
    "host or drive it into swap. Default: None (undeclared jobs unbounded, "
    "the legacy behaviour).",
)
@click.option(
    "--poll-interval",
    type=click.FloatRange(min=0.01),
    callback=_finite_positive_seconds,
    default=1.0,
)
@click.option(
    "--web",
    "run_web",
    is_flag=True,
    default=False,
    help="Also run the vq web dashboard as a daemon-owned sidecar. The "
    "dashboard starts before the daemon loop and is stopped when the daemon "
    "exits. Default bind is loopback-only.",
)
@click.option(
    "--web-host",
    default="127.0.0.1",
    show_default=True,
    help="Bind address for --web. Read-only dashboard pages are "
    "unauthenticated, so keep this loopback-only unless a reverse proxy owns "
    "access control.",
)
@click.option(
    "--web-port",
    default=8765,
    show_default=True,
    type=click.IntRange(min=1, max=65535),
    help="TCP port for --web.",
)
@click.option(
    "--web-log-level",
    default="info",
    type=click.Choice(["critical", "error", "warning", "info", "debug", "trace"]),
    show_default=True,
    help="Uvicorn log level for the --web sidecar.",
)
@click.option(
    "--web-i-understand-public-bind",
    is_flag=True,
    default=False,
    help="Forward the non-loopback bind acknowledgement to `vq web run` when "
    "using --web-host with a public or LAN interface.",
)
def daemon_run(
    max_cpus: int | None,
    max_jobs: int | None,
    max_scheduler_jobs: int | None,
    max_mem_mb: int | None,
    default_job_mem_mb: int | None,
    poll_interval: float,
    run_web: bool,
    web_host: str,
    web_port: int,
    web_log_level: str,
    web_i_understand_public_bind: bool,
) -> None:
    """Run the daemon in the foreground (used by systemd and `vq daemon start`)."""
    # v0.6.x: detect multi-user mode BEFORE writing the pidfile so
    # it lands at the correct path.
    running_as_root = os.geteuid() == 0
    multi_user = False
    notify_webhook_url: str | None = None
    notify_on_states: list[str] = []
    try:
        cfg = config.load_config()
        if running_as_root and not cfg.multi_user.enabled:
            raise click.ClickException(
                "refusing to run the daemon as root without a valid "
                "[multi_user] configuration with enabled = true; there is "
                "no run-as-root single-user fallback"
            )
        _setup_cli_invocation_logging()
        # v0.16.0: [daemon] config fills any capacity cap the CLI left
        # unset (an explicit flag always wins). Keeps per-host caps in
        # the config file instead of hand-edited ExecStart lines, which
        # unit rewrites have repeatedly dropped.
        filled = {}
        if max_cpus is None and cfg.daemon.max_cpus is not None:
            max_cpus = filled["max_cpus"] = cfg.daemon.max_cpus
        if max_jobs is None and cfg.daemon.max_jobs is not None:
            max_jobs = filled["max_jobs"] = cfg.daemon.max_jobs
        if max_scheduler_jobs is None and cfg.daemon.max_scheduler_jobs is not None:
            max_scheduler_jobs = filled["max_scheduler_jobs"] = (
                cfg.daemon.max_scheduler_jobs
            )
        if max_mem_mb is None and cfg.daemon.max_mem_mb is not None:
            max_mem_mb = filled["max_mem_mb"] = cfg.daemon.max_mem_mb
        if default_job_mem_mb is None and cfg.daemon.default_job_mem_mb is not None:
            default_job_mem_mb = filled["default_job_mem_mb"] = (
                cfg.daemon.default_job_mem_mb
            )
        if filled:
            log.info("[daemon] config filled caps: %s", filled)
        notify_webhook_url = cfg.notifications.webhook_url
        notify_on_states = list(cfg.notifications.notify_on_states)
        if notify_webhook_url:
            if notify_on_states:
                log.info(
                    "notifications: webhook configured (filter: %s)",
                    notify_on_states,
                )
            else:
                log.info(
                    "notifications: webhook configured (no state filter)",
                )
        else:
            log.info("notifications: disabled (no webhook_url in config)")
        multi_user = cfg.multi_user.enabled and running_as_root
    except config.ConfigError as e:
        if running_as_root:
            raise click.ClickException(
                "refusing to run the daemon as root because its "
                "configuration is invalid; root daemon startup requires "
                "[multi_user] enabled = true and has no single-user fallback"
            ) from e
        _setup_cli_invocation_logging()
        log.warning(
            "failed to load notification config (%s); notifications disabled for this run",
            e,
        )
    setup_daemon_logging(paths.daemon_logfile(multi_user=multi_user))
    write_pidfile(multi_user=multi_user)
    web_proc: subprocess.Popen[bytes] | None = None
    try:
        if run_web:
            web_proc = _start_web_sidecar(
                host=web_host,
                port=web_port,
                log_level=web_log_level,
                acknowledge_public_bind=web_i_understand_public_bind,
            )

        def _web_loop_hook() -> None:
            nonlocal web_proc
            if web_proc is None:
                return
            web_proc = _ensure_web_sidecar_running(
                web_proc,
                host=web_host,
                port=web_port,
                log_level=web_log_level,
                acknowledge_public_bind=web_i_understand_public_bind,
            )

        Daemon(
            max_cpus=max_cpus,
            max_jobs=max_jobs,
            max_scheduler_jobs=max_scheduler_jobs,
            max_mem_mb=max_mem_mb,
            default_job_mem_mb=default_job_mem_mb,
            poll_interval=poll_interval,
            notify_webhook_url=notify_webhook_url,
            notify_on_states=notify_on_states,
            multi_user=multi_user,
            loop_hook=_web_loop_hook if run_web else None,
        ).run()
    finally:
        if web_proc is not None:
            _stop_web_sidecar(web_proc)
        remove_pidfile(multi_user=multi_user)


# ----------------------------------------------------------------------
# v0.6.15: SLURM-style verb aliases.
#
# These are NAME-LEVEL aliases — registering the canonical command
# under a second name via Click's add_command. The flags are unchanged
# (still vq-style: --cpus, --mem-mb, --wall-time-seconds, etc.); the
# alias only lets operators with SLURM muscle memory type the verb
# name they're used to.
#
# Verb mapping:
#   vq sbatch  → vq submit   (script/job submission)
#   vq squeue  → vq queue    (list jobs)
#   vq scancel → vq kill     (cancel/terminate a job)
#   vq sacct   → vq status   (job accounting / inspection)
#
# Flag translation (SLURM `--mem=16G` → vq `--mem-mb=16000`,
# `--time=01:30:00` → `--wall-time-seconds=5400`, etc.) is OUT of
# scope for this ship — too much surface area for a sugar layer, and
# the partial translation would mislead operators about which SLURM
# semantics actually work. Operators see `--help` on the alias, get
# pointed at the canonical verb's flags, and use those.
main.add_command(submit, name="sbatch")
main.add_command(queue, name="squeue")
main.add_command(kill, name="scancel")
main.add_command(status, name="sacct")

# v0.6.24: `vq summary` is a name-level alias for `vq overview`.
# Same code path, same flags — only the verb name differs. Operators
# tend to reach for "summary" when they want a fleet rundown, while
# "overview" describes what the command produces. Both names work.
main.add_command(overview, name="summary")

# v0.12.0: `vq list` is a name-level alias for `vq queue`. Same code path
# and flags. Operators habitually reach for "list" to see what is queued,
# and `vq list` previously errored with "No such command". The canonical
# verb stays `queue`, with `squeue` as the Slurm-compat name.
main.add_command(queue, name="list")


if __name__ == "__main__":
    main()
