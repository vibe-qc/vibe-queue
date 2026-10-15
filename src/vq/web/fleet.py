"""Fleet aggregation for the vq web dashboard (fleet mode).

The fleet console (``vq web run --fleet``, see
``docs/fleet_dashboard_design.md``) needs one thing the single-host
dashboard does not have: a fleet-wide snapshot. This module provides it
as a cached, background-refreshed pull over the exact surfaces the CLI
already exposes — ``vq overview`` gathers per host, plus the per-host
job listings (local ``list_jobs``, remote ``vq queue localhost --json``,
scheduler hosts read on their ``scheduler_driver``).

No daemon changes, no new network protocol: everything here is a
consumer of existing state, fanned out over SSH from the host running
the web app (normally the coordinator).
"""
from __future__ import annotations

import json
import logging
import subprocess
import sys
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic

from vq import capacity, config, host_status, overview, transport
from vq import top as top_mod
from vq.host import is_local_host
from vq.listing import (
    effective_queue_state,
    list_jobs,
    normalize_delegated_queue_handle_host,
    pending_configured_capacity_known,
    pending_configured_capacity_overages,
    queue_handle_for_spec,
    queue_handle_for_unvalidated_row,
    queue_host_for_scheduler_target,
    scheduler_running_confirmed,
    scheduler_target_is_safe,
)
from vq.overview import AdminMarkerSnapshot
from vq.spec import JobSpec, JobState
from vq.status import terminal_diagnosis_for_spec

log = logging.getLogger(__name__)

#: Default seconds between background refreshes. An SSH fan-out across
#: a ~8-host fleet takes a few seconds; 30 s keeps the console fresh
#: without hammering the fleet.
DEFAULT_INTERVAL_SECONDS = 30

#: How long the console keeps showing a host's last-known jobs while its
#: queue cannot be listed (#168). Past this they are dropped, so a host
#: gone for good stops posing as a source of calculations.
LAST_KNOWN_MAX_AGE = timedelta(hours=24)

#: How many sweeps of fleet totals the console keeps for its trend plots
#: (#168): two hours at the default 30 s interval. In memory only.
TREND_POINTS = 240

#: Cap the SSH fan-out parallelism (same spirit as the CLI's
#: ``_fanout_max_workers``): enough to overlap the slow hosts, not
#: enough to open a connection storm.
_MAX_WORKERS = 8


def fleet_enabled() -> bool:
    """True when the web app should register the fleet surface.

    Resolved by :mod:`vq.web.settings`: ``--fleet`` beats ``VQ_WEB_FLEET``
    beats ``[web] fleet`` in the config file. Off by default — most hosts
    run the web sidecar for their *own* queue and must not start SSH
    fan-outs.
    """
    from vq.web import settings as _settings  # noqa: PLC0415 — cycle

    return _settings.resolve_settings().fleet


def fleet_interval_seconds() -> int:
    """Seconds between background fleet sweeps.

    ``VQ_WEB_FLEET_INTERVAL`` beats ``[web] fleet_interval_seconds``
    beats the built-in default. Values below the 5 s floor are ignored in
    favour of the default rather than honoured, so a typo cannot turn the
    poller into a connection storm.
    """
    from vq.web import settings as _settings  # noqa: PLC0415 — cycle

    return _settings.resolve_settings().fleet_interval_seconds


@dataclass
class HostSnapshot:
    """One host's slice of the fleet snapshot."""

    host: str
    overview: overview.HostOverview
    #: Job rows shaped like the CLI's ``vq queue --json`` rows (JobSpec
    #: dump + ``queue_handle`` + ``terminal_diagnosis``), each with an
    #: injected ``queue_host`` display field. Kept as plain dicts so a
    #: newer remote vq's extra spec fields survive untouched.
    jobs: list[dict] = field(default_factory=list)
    #: Job-listing failure, if any. Independent of the overview's own
    #: ``reachable``/``error`` state: an overview can succeed while the
    #: queue listing fails (and vice versa).
    jobs_error: str | None = None
    #: Resource-sample collection failure on the owning driver, if any
    #: (#168). The rows are still listed; their measurements read unknown.
    usage_error: str | None = None
    #: Scheduler lanes only (#168 readiness). The driver daemon that owns
    #: this lane's queue, and every admin-update lease on that driver: the
    #: projected overview keeps only the leases that hold this lane, so a
    #: background runtime build for it would otherwise be invisible here.
    driver: str | None = None
    driver_leases: list[AdminMarkerSnapshot] = field(default_factory=list)
    #: Scheduler lanes only: the first config key among the lanes that log
    #: in through the same SSH destination, i.e. the one cluster they share.
    #: A label, never the destination itself.
    cluster: str | None = None
    #: #168: while this host's queue cannot be listed, the rows of its last
    #: successful listing and when that listing was observed. Kept apart from
    #: :attr:`jobs`, so no live count or total includes them; see
    #: :func:`carry_last_known`.
    last_known: list[dict] = field(default_factory=list)
    last_known_at: str | None = None

    @property
    def unlisted(self) -> bool:
        """No row of this host's queue is in the snapshot: not probed, or
        the listing failed outright (a listing with some invalid rows still
        lists the rest)."""
        return bool(self.overview.admin_down) or (
            self.jobs_error is not None and not self.jobs
        )


@dataclass
class FleetSnapshot:
    """A point-in-time view of every configured host."""

    gathered_at: str
    duration_seconds: float
    hosts: list[HostSnapshot] = field(default_factory=list)

    @property
    def jobs(self) -> list[dict]:
        """All job rows across the fleet, flattened."""
        return [row for h in self.hosts for row in h.jobs]

    @property
    def duplicate_enrolments(self) -> list[tuple[str, list[str]]]:
        """Config keys that turned out to name the same physical machine.

        ``[(reported_hostname, [key, key, ...]), ...]``, only for machines
        named by more than one key.

        This is a real defect, not a cosmetic one: both keys are gathered,
        both are rendered as cards, and — because :attr:`jobs` flattens
        per-host rows — every job on that machine is **counted twice** in
        the cross-host table, in its totals, and in the JSON API. The
        driver/scheduler double-count already has explicit handling in
        :func:`_gather_host_jobs`; the same machine under two daemon-host
        keys did not.

        Surfaced rather than deduped on purpose. vq cannot know which key
        the operator meant to keep, and silently collapsing them would
        hide the config error that produced them — which is how the
        reference fleet ran for two weeks with its coordinator enrolled
        as both ``coordinator`` and ``localhost``.

        Keyed on :attr:`~vq.overview.HostOverview.reported_hostname`, so
        it needs the far end to have answered. Scheduler aliases (several
        keys deliberately sharing one SSH target) report no hostname and
        so never trip it, which is correct: they are distinct queues, not
        a duplicated machine.
        """
        by_name: dict[str, list[str]] = {}
        for host in self.hosts:
            name = host.overview.reported_hostname
            if not name:
                continue
            by_name.setdefault(name, []).append(host.host)
        return [
            (name, keys) for name, keys in sorted(by_name.items()) if len(keys) > 1
        ]

    def to_json(self) -> dict[str, object]:
        return {
            "gathered_at": self.gathered_at,
            "duration_seconds": self.duration_seconds,
            "hosts": [
                {
                    "host": h.host,
                    "overview": overview.format_overview_json(h.overview),
                    "job_count": len(h.jobs),
                    "jobs_error": h.jobs_error,
                    "usage_error": h.usage_error,
                    "last_known_count": len(h.last_known),
                    "last_known_at": h.last_known_at,
                }
                for h in self.hosts
            ],
        }


def carry_last_known(
    previous: FleetSnapshot | None,
    current: FleetSnapshot,
    *,
    max_age: timedelta = LAST_KNOWN_MAX_AGE,
) -> FleetSnapshot:
    """Give each unlisted host of ``current`` its last-known rows (#168).

    The rows come from ``previous``: the host's own listing if it had one,
    otherwise what ``previous`` was already carrying, with the time it was
    observed then rather than now. A host that lists again carries
    nothing, and rows older than ``max_age`` are dropped. Kept in this
    console's memory only; nothing is read from or written to a host.
    """
    if previous is None:
        return current
    now = _parse_time(current.gathered_at)
    before = {h.host: h for h in previous.hosts}
    for host in current.hosts:
        old = before.get(host.host)
        if not host.unlisted or old is None:
            continue
        if not old.unlisted:
            rows, observed_at = old.jobs, previous.gathered_at
        else:
            rows, observed_at = old.last_known, old.last_known_at
        observed = _parse_time(observed_at)
        if not rows or now is None or observed is None or now - observed > max_age:
            continue
        host.last_known, host.last_known_at = rows, observed_at
    return current


def _parse_time(value: str | None) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value) if value else None
    except ValueError:
        return None
    return parsed if parsed is not None and parsed.tzinfo is not None else None


def _job_row(
    spec: JobSpec,
    queue_host: str,
    capacity_snapshot: capacity.DaemonCapacity | None = None,
) -> dict[str, object]:
    """Shape a local JobSpec like the CLI's ``_queue_json_row``."""
    row: dict[str, object] = spec.model_dump(mode="json")
    row["effective_state"] = effective_queue_state(spec)
    row["scheduler_running_confirmed"] = scheduler_running_confirmed(spec)
    row["queue_handle"] = queue_handle_for_spec(spec, queue_host)
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


def _normalize_rows(
    rows: list[dict], queue_host: str,
    *, validated_specs: list[JobSpec | None] | None = None,
) -> list[dict]:
    """Inject the display ``queue_host`` and patch localhost handles.

    Remote listings answer as ``localhost`` from their own point of
    view; the fleet table wants the fleet-config host name.
    """
    for index, row in enumerate(rows):
        target = row.get("scheduler_target")
        row_valid = True
        try:
            spec = (JobSpec.model_validate(row) if validated_specs is None
                    else validated_specs[index])
            if spec is None:
                raise ValueError("invalid queue row")
        except (TypeError, ValueError):
            row_valid = False
            row_queue_host = queue_host
            row["effective_state"] = "scheduler_unknown"
            row["scheduler_running_confirmed"] = (
                False
                if row.get("state") == "running" and target is not None
                else None
            )
        else:
            row_queue_host = queue_host_for_scheduler_target(target, queue_host)
            # Override stale derived values from an older remote driver.
            row["effective_state"] = effective_queue_state(spec)
            row["scheduler_running_confirmed"] = scheduler_running_confirmed(spec)
        row["queue_host"] = row_queue_host
        unsafe_target = target is not None and not scheduler_target_is_safe(target)
        handle = row.get("queue_handle")
        if isinstance(handle, dict):
            if row_valid and not unsafe_target:
                handle["host"] = normalize_delegated_queue_handle_host(
                    handle.get("host"),
                    row_queue_host,
                )
            else:
                row["queue_handle"] = queue_handle_for_unvalidated_row(
                    row,
                    row_queue_host,
                )
        elif not row_valid:
            row["queue_handle"] = queue_handle_for_unvalidated_row(
                row,
                row_queue_host,
            )
    return rows


@dataclass
class _QueueObservation:
    rows: list[dict] = field(default_factory=list)
    row_specs: list[JobSpec | None] = field(default_factory=list)
    error: str | None = None

    @property
    def specs(self) -> list[JobSpec]:
        return [spec for spec in self.row_specs if spec is not None]


def _observe_queue(owner: str, cfg: config.Config, *, multi_user: bool) -> _QueueObservation:
    """Read and convert one driver's history once, without per-job requests."""
    try:
        if is_local_host(owner):
            specs = [s for s in list_jobs(owner, multi_user=multi_user) if not s.is_archived]
            cap = None
            try:
                cap = capacity.read_daemon_capacity(multi_user=multi_user)
            except Exception as exc:
                log.warning("fleet: capacity read failed for %s: %s", owner, exc)
            return _QueueObservation(
                rows=[_job_row(s, owner, capacity_snapshot=cap) for s in specs],
                row_specs=list(specs),
            )
        proc = transport.run_remote_vq(cfg.host(owner), "queue", "localhost", "--json")
        raw = json.loads(proc.stdout or "[]")
        if not isinstance(raw, list):
            raise ValueError("queue response must be an array")
        rows = []
        row_specs: list[JobSpec | None] = []
        invalid = 0
        for row in raw:
            if not isinstance(row, dict):
                invalid += 1
                continue
            try:
                spec = JobSpec.model_validate(row)
            except (TypeError, ValueError):
                spec = None
            if spec is not None and spec.is_archived:
                continue
            rows.append(row)
            row_specs.append(spec)
        return _QueueObservation(
            rows, row_specs, f"{invalid} invalid queue rows" if invalid else None,
        )
    except (transport.RemoteError, config.ConfigError, ValueError, OSError) as exc:
        return _QueueObservation(error=f"{type(exc).__name__}: {exc}")


def _gather_host_jobs(
    host: str,
    cfg: config.Config,
    *,
    multi_user: bool,
    scheduler_host_names: set[str],
    observation: _QueueObservation | None = None,
) -> tuple[list[dict], str | None]:
    """Project a host's rows from its driver; retain distinct queue identities."""
    host_cfg = cfg.hosts.get(host)
    scheduler = host_cfg is not None and host_cfg.scheduler != "local"
    owner = host_cfg.scheduler_driver if scheduler else host
    if owner is None:
        return [], "scheduler host has no scheduler_driver configured"
    if observation is None:
        observation = _observe_queue(owner, cfg, multi_user=multi_user)
    selected = [
        (row, spec) for row, spec in zip(observation.rows, observation.row_specs, strict=True)
        if (row.get("scheduler_target") == host if scheduler
            else (not isinstance(row.get("scheduler_target"), str)
                  or row.get("scheduler_target") not in scheduler_host_names))
    ]
    invalid = sum(spec is None for _, spec in selected)
    error = observation.error or (f"{invalid} invalid queue rows" if invalid else None)
    rows = _normalize_rows(
        [row for row, _ in selected], host,
        validated_specs=[spec for _, spec in selected],
    )
    for row in rows:
        # The daemon that owns the spec: the host itself, or the scheduler
        # driver for a cluster target. The console labels it separately
        # from where the job executes.
        row["queue_owner"] = owner
    return rows, error


#: Statuses of a row's ``resource_observation`` (#168). Only ``observed``
#: and ``stale`` carry numbers; every other status says why there are none,
#: so a missing measurement can never read as zero usage.
USAGE_OBSERVED = "observed"
USAGE_STALE = "stale"
USAGE_NO_SAMPLE = "no_sample"
USAGE_SCHEDULER = "scheduler"
#: A Torque/PBS job whose scheduler reports its memory and CPU time (#168).
USAGE_SCHEDULER_REPORTED = "scheduler_reported"
USAGE_NOT_RUNNING = "not_running"
USAGE_COLLECTOR_ERROR = "collector_error"
USAGE_NOT_COLLECTED = "not_collected"


@dataclass
class _UsageObservation:
    """One driver's latest watchdog samples, read once per sweep."""

    #: ``vq top --json`` rows keyed by job id. Job ids are unique per
    #: driver, and each driver has its own observation.
    by_job: dict[str, dict[str, object]] = field(default_factory=dict)
    collected_at: str | None = None
    error: str | None = None


def _observe_usage(
    owner: str,
    cfg: config.Config,
    queue: _QueueObservation,
    *,
    now: datetime,
) -> _UsageObservation:
    """Read the latest resource sample of every running direct job, in one go.

    The same rows ``vq top --json`` prints: one local read, or one remote
    ``vq top localhost --json`` per driver -- never one request per job.
    Skipped entirely when the driver runs no direct job, so a fleet of
    scheduler targets or idle hosts pays nothing for it. Scheduler jobs
    are not sampled: the watchdog runs on the daemon host, not on the
    compute node.
    """
    wanted = {
        spec.id for spec in queue.specs
        if spec.state == JobState.RUNNING and spec.scheduler_target is None
    }
    if not wanted:
        return _UsageObservation()
    collected_at = now.isoformat()
    try:
        if is_local_host(owner):
            rows = [
                top_mod.top_row_payload(row)
                for row in top_mod.gather_top_rows(
                    [spec for spec in queue.specs if spec.id in wanted],
                    host=owner, now=now,
                )
            ]
        else:
            proc = transport.run_remote_vq(cfg.host(owner), "top", "localhost", "--json")
            raw = json.loads(proc.stdout or "[]")
            if not isinstance(raw, list):
                raise ValueError("top response must be an array")
            rows = [row for row in raw if isinstance(row, dict)]
    except (transport.RemoteError, config.ConfigError, ValueError, OSError) as exc:
        return _UsageObservation(
            collected_at=collected_at, error=f"{type(exc).__name__}: {exc}",
        )
    by_job = {
        row["jobid"]: row for row in rows
        if isinstance(row.get("jobid"), str) and row["jobid"] in wanted
    }
    return _UsageObservation(by_job=by_job, collected_at=collected_at)


def _sample_time(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed.isoformat() if parsed.tzinfo is not None else None


def _resource_observation(
    row: dict[str, object], usage: _UsageObservation,
) -> dict[str, object]:
    """What was measured for one row this sweep, or why nothing was.

    ``cpu_percent`` is the job's whole process group (an 8-core job at
    full use reads ~800); ``rss_mb`` is the resident memory of that
    group's processes. Both come from the watchdog's latest sample and
    are never filled in from the job's declared ``cpus`` or ``mem_mb``.
    """
    observation: dict[str, object] = {
        "status": USAGE_NOT_COLLECTED,
        "source": None,
        "cpu_percent": None,
        "rss_mb": None,
        "rss_peak_mb": None,
        "rss_peak_since": None,
        # "job" for the watchdog's whole process group; "largest task" for
        # Slurm's sstat, which reports only its largest task's peak.
        "rss_peak_scope": None,
        "sample_at": None,
        "collected_at": usage.collected_at,
    }
    if row.get("state") != JobState.RUNNING.value:
        observation["status"] = USAGE_NOT_RUNNING
    elif row.get("scheduler_target") is not None:
        observation.update(_scheduler_reported(row) or {"status": USAGE_SCHEDULER})
    elif usage.error is not None:
        observation["status"] = USAGE_COLLECTOR_ERROR
    elif usage.collected_at is not None:
        sample = usage.by_job.get(str(row.get("id")))
        cpu = top_mod.sample_nonnegative_float(
            sample.get("cpu_percent") if sample else None
        )
        rss = top_mod.sample_nonnegative_float(
            sample.get("rss_mb") if sample else None
        )
        if sample is None or (cpu is None and rss is None):
            observation["status"] = USAGE_NO_SAMPLE
        else:
            # A peak without the time its tracking began cannot be read
            # honestly, so the pair is kept or dropped together.
            peak_since = _sample_time(sample.get("rss_peak_since"))
            peak = top_mod.sample_nonnegative_float(sample.get("rss_peak_mb"))
            observation.update(
                status=USAGE_STALE if sample.get("sample_stale") is True else USAGE_OBSERVED,
                source="watchdog",
                cpu_percent=cpu,
                rss_mb=rss,
                rss_peak_mb=peak if peak_since is not None else None,
                rss_peak_since=peak_since if peak is not None else None,
                rss_peak_scope="job" if peak is not None and peak_since is not None else None,
                sample_at=_sample_time(sample.get("sample_at")),
            )
    return observation


def _scheduler_reported(row: dict[str, object]) -> dict[str, object] | None:
    """What a scheduler says a running job uses, at its last poll (#168).

    Torque/PBS ``resources_used``: memory in use, and CPU time over the
    walltime so far, which is an average number of busy cores, not a current
    reading. Slurm's ``sstat``: the same CPU average, and the peak RSS of the
    largest task, which is a peak since the job started and not the job's
    memory. None when the scheduler reported none of them (an older vq).
    """
    from vq.scheduler_dialect import torque_memory_mb
    from vq.usage import _hms_to_seconds

    def text(key: str) -> str | None:
        value = row.get(key)
        return value if isinstance(value, str) and value else None

    mem = torque_memory_mb(text("scheduler_mem_used"))
    cpu = _hms_to_seconds(text("scheduler_cpu_used"))
    wall = _hms_to_seconds(text("scheduler_walltime_used"))
    task_peak = torque_memory_mb(text("scheduler_task_peak_rss"))
    peak_since = _sample_time(row.get("started_at")) if task_peak is not None else None
    if mem is None and cpu is None and peak_since is None:
        return None
    return {
        "status": USAGE_SCHEDULER_REPORTED,
        "source": "scheduler",
        "cpu_percent": round(100.0 * cpu / wall, 1) if cpu is not None and wall else None,
        "rss_mb": mem,
        "rss_peak_mb": task_peak if peak_since is not None else None,
        "rss_peak_since": peak_since,
        "rss_peak_scope": "largest task" if peak_since is not None else None,
        "sample_at": _sample_time(row.get("last_heartbeat_at")),
    }


def _gather_host_overview(
    host: str,
    cfg: config.Config,
    *,
    recent_window: timedelta,
    multi_user: bool,
) -> overview.HostOverview:
    """Mirror of the CLI overview's per-host dispatch. Never raises."""
    try:
        if is_local_host(host):
            return overview.gather_overview_local(
                host, cfg, recent_window=recent_window, multi_user=multi_user
            )
        host_cfg = cfg.host(host)
        if host_cfg.scheduler != "local":
            return overview.gather_scheduler_overview(
                host,
                host_cfg,
                cfg,
                recent_window=recent_window,
                multi_user=multi_user,
            )
        return overview.gather_overview_remote(
            host, host_cfg, recent_window=recent_window
        )
    except config.ConfigError as e:
        return overview.HostOverview(host=host, reachable=False, error=str(e))
    except Exception as e:  # pragma: no cover — defensive fan-out guard
        log.warning("fleet: overview gather failed for %s: %s", host, e)
        return overview.HostOverview(host=host, reachable=False, error=str(e))


def _multi_user_active(cfg: config.Config) -> bool:
    if config.system_multi_user_enabled():
        return True
    try:
        return bool(cfg.multi_user.enabled)
    except Exception:  # pragma: no cover — defensive
        return False


def gather_fleet_snapshot(
    cfg: config.Config | None = None,
    *,
    recent_window: timedelta = timedelta(hours=24),
) -> FleetSnapshot:
    """One full fleet sweep: every configured host (plus localhost when
    the config names none), administratively-down hosts included as
    unprobed rows — matching ``vq overview`` semantics."""
    started = monotonic()
    if cfg is None:
        cfg = config.load_config()
    multi_user = _multi_user_active(cfg)

    host_names = list(cfg.hosts.keys()) or ["localhost"]
    scheduler_host_names = {
        name for name, hc in cfg.hosts.items() if hc.scheduler != "local"
    }
    lanes_by_login: dict[str, list[str]] = {}
    for name in sorted(scheduler_host_names):
        lanes_by_login.setdefault(cfg.hosts[name].ssh, []).append(name)
    cluster_of = {
        name: lanes[0] for lanes in lanes_by_login.values() for name in lanes
    }
    down = host_status.load_down()

    observed_at = datetime.now(UTC)
    owners = {
        host: resolve_owner_host(cfg, host) for host in host_names if host not in down
    }

    def _observe(
        owner: str,
    ) -> tuple[_QueueObservation, overview.HostOverview, _UsageObservation]:
        if owner in down:
            detail = down[owner].describe()
            return _QueueObservation(error=detail), overview.HostOverview(
                host=owner, reachable=False, admin_down=detail, error=detail,
            ), _UsageObservation()
        queue = _observe_queue(owner, cfg, multi_user=multi_user)
        if is_local_host(owner):
            ov = overview.gather_overview_local(
                owner, cfg, recent_window=recent_window, multi_user=multi_user,
                now=observed_at, specs=queue.specs,
            )
        else:
            ov = _gather_host_overview(
                owner, cfg, recent_window=recent_window, multi_user=multi_user,
            )
        usage = _observe_usage(owner, cfg, queue, now=datetime.now(UTC))
        return queue, ov, usage

    unique_owners = list(dict.fromkeys(owner for owner in owners.values() if owner is not None))
    workers = min(_MAX_WORKERS, max(1, len(unique_owners)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        observations = dict(zip(unique_owners, pool.map(_observe, unique_owners), strict=True))

    snapshots = []
    for host in host_names:
        lane = host in scheduler_host_names
        if host in down:
            snapshots.append(HostSnapshot(host=host, overview=overview.HostOverview(
                host=host, reachable=False, admin_down=down[host].describe(),
            ), cluster=cluster_of.get(host)))
            continue
        owner = owners[host]
        if owner is None:
            snapshots.append(HostSnapshot(host=host, overview=overview.HostOverview(
                host=host, reachable=False,
                error="scheduler host has no scheduler_driver configured",
            ), jobs_error="scheduler host has no scheduler_driver configured",
                cluster=cluster_of.get(host)))
            continue
        queue, driver_overview, usage = observations[owner]
        if lane:
            ov = overview.project_scheduler_overview(
                host, driver_overview, queue.specs,
                recent_window=recent_window, now=observed_at,
            )
        else:
            ov = replace(driver_overview, host=host)
            # Reservations on this driver are not physical execution here,
            # including targets missing from the current fleet configuration.
            physical = [s for s in queue.specs if s.scheduler_target is None]
            (ov.queue_counts, ov.recent_terminal_counts, last_terminal,
             ov.running_cpus, ov.pending_cpus) = overview._count_specs(
                physical, recent_window=recent_window, now=observed_at,
            )
            ov.idle_seconds = (
                max(0, int((observed_at - last_terminal).total_seconds()))
                if not ov.queue_counts.get("running") and last_terminal is not None else None
            )
        jobs, jobs_error = _gather_host_jobs(
            host, cfg, multi_user=multi_user, scheduler_host_names=scheduler_host_names,
            observation=queue,
        )
        for row in jobs:
            row["resource_observation"] = _resource_observation(row, usage)
        snapshots.append(HostSnapshot(
            host=host, overview=ov, jobs=jobs, jobs_error=jobs_error,
            # A scheduler alias shares its driver's observation but has no
            # direct jobs to sample, so the failure is reported once, on
            # the driver's own host.
            usage_error=None if lane else usage.error,
            driver=owner if lane else None,
            driver_leases=(
                list(overview._admin_marker_snapshots(driver_overview)) if lane else []
            ),
            cluster=cluster_of.get(host),
        ))

    return FleetSnapshot(
        gathered_at=observed_at.isoformat(),
        duration_seconds=round(monotonic() - started, 2),
        hosts=snapshots,
    )


#: How many trailing watchdog samples the detail page shows. The
#: watchdog samples every ~5 s, so 60 records ≈ the last five minutes.
_SAMPLE_TAIL = 60


def _parse_sample_lines(text: str, limit: int = _SAMPLE_TAIL) -> list[dict]:
    """Decode the last ``limit`` well-formed records of a samples.jsonl
    body. Malformed lines (mid-write tail, truncation) are skipped."""
    records: list[dict] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if isinstance(rec, dict):
            records.append(rec)
    return records[-limit:]


def fetch_job_samples(
    cfg: config.Config,
    owner: str,
    workspace: str,
    *,
    limit: int = _SAMPLE_TAIL,
) -> list[dict]:
    """Recent watchdog resource samples for a job's workspace, read on
    the owning host. Best-effort: any failure (no watchdog on the host,
    workspace cleaned, transport error) yields ``[]``, never an error —
    the detail page renders fine without a resource curve."""
    path = f"{workspace.rstrip('/')}/_vq/samples.jsonl"
    try:
        if is_local_host(owner):
            text = (Path(workspace) / "_vq" / "samples.jsonl").read_text(
                encoding="utf-8"
            )
        else:
            proc = transport.run_remote_shell(
                cfg.host(owner),
                "tail",
                "-n",
                str(limit),
                path,
                check=False,
            )
            if proc.returncode != 0:
                return []
            text = proc.stdout
    except (OSError, config.ConfigError, transport.RemoteError):
        return []
    return _parse_sample_lines(text, limit)


def resolve_owner_host(cfg: config.Config, queue_host: str) -> str | None:
    """The daemon host that owns ``queue_host``'s specs.

    A daemon host owns its own specs; a scheduler host's specs live on
    its ``scheduler_driver``. Returns None when a scheduler host has no
    driver configured.
    """
    host_cfg = cfg.hosts.get(queue_host)
    if host_cfg is not None and host_cfg.scheduler != "local":
        return host_cfg.scheduler_driver
    return queue_host


PREVIEW_TAIL_LINES = 2000
"""Stdout lines a preview reads: the same status tail the drill-down shows, deeper."""


def fetch_job_preview(
    cfg: config.Config,
    queue_host: str,
    jobid: str,
    *,
    multi_user: bool = False,
) -> tuple[dict[str, object] | None, str | None]:
    """Calculation preview for one job (#65, V1); ``(payload, error)``.

    Reads only what the drill-down already reads, ``vq status --json`` with a
    stdout tail, on the owning host, and parses it with :mod:`vq.previews`.
    It opens no other file and walks no workdir. ``preview`` is ``None`` when
    no parser knows the job's program.
    """
    from vq import previews
    from vq.status import show_status_json

    owner = resolve_owner_host(cfg, queue_host)
    if owner is None:
        return None, f"scheduler host {queue_host!r} has no scheduler_driver configured"
    try:
        if is_local_host(owner):
            status_payload = json.loads(show_status_json(
                owner, jobid, tail=PREVIEW_TAIL_LINES, multi_user=multi_user,
            ))
        else:
            proc = transport.run_remote_vq(
                cfg.host(owner), "status", "localhost", jobid, "--json",
                "-n", str(PREVIEW_TAIL_LINES),
            )
            status_payload = json.loads(proc.stdout)
    except FileNotFoundError as e:
        return None, str(e)
    except transport.RemoteError as e:
        return None, str(e)
    except (config.ConfigError, json.JSONDecodeError, ValueError, OSError) as e:
        return None, f"{type(e).__name__}: {e}"
    program = status_payload.get("program")
    stdout = status_payload.get("stdout")
    text = stdout[-previews.MAX_TAIL_BYTES:] if isinstance(stdout, str) else ""
    source = "stdout"
    output_name = (
        previews.orca_output_name(status_payload.get("command"))
        if previews.parser_for(program if isinstance(program, str) else None) == "orca"
        else None
    )
    if output_name is not None:
        # Maintainer decision (2026-09-25): an ORCA job run through
        # run-orca.sh writes to one named output file; read exactly that
        # file's bounded tail through vq tail, never a directory walk.
        tail_args = ("tail", queue_host, jobid, "--name", output_name, "--json",
                     "-n", str(PREVIEW_TAIL_LINES))
        try:
            if is_local_host(owner):
                proc = subprocess.run(
                    [sys.executable, "-m", "vq.cli", *tail_args],
                    stdin=subprocess.DEVNULL, capture_output=True, text=True,
                    timeout=60, check=False,
                )
                tail_text = proc.stdout if proc.returncode == 0 else ""
            else:
                tail_text = transport.run_remote_vq(cfg.host(owner), *tail_args).stdout
            tailed = json.loads(tail_text).get("text") if tail_text else None
        except (transport.RemoteError, json.JSONDecodeError, OSError,
                subprocess.SubprocessError, AttributeError):
            tailed = None
        if isinstance(tailed, str):
            text = tailed[-previews.MAX_TAIL_BYTES:]
            source = output_name
    return {
        "source": source,
        "jobid": jobid,
        "queue_host": queue_host,
        "owner_host": owner,
        "state": status_payload.get("state"),
        "program": program if isinstance(program, str) else None,
        "preview": previews.preview(program if isinstance(program, str) else None, text),
    }, None


def fetch_job_detail(
    cfg: config.Config,
    queue_host: str,
    jobid: str,
    *,
    multi_user: bool = False,
    tail: int = 200,
) -> tuple[dict[str, object] | None, str | None]:
    """On-demand job drill-down for the fleet console (M1).

    Returns ``(payload, error)``; exactly one is None. The payload is
    ``{"status": <vq status --json shape>, "events": [...]}``, read on
    the host that owns the spec (driver-aware): locally through the
    same helpers the CLI uses, remotely through
    ``vq status localhost JOBID --json`` + ``vq events localhost JOBID
    --json`` over the existing transport. Unlike the snapshot this is
    fetched live per request — logs and events are exactly current.
    """
    from vq.logs import show_events_json
    from vq.status import show_status_json

    owner = resolve_owner_host(cfg, queue_host)
    if owner is None:
        return None, (
            f"scheduler host {queue_host!r} has no scheduler_driver configured"
        )
    try:
        if is_local_host(owner):
            status_payload = json.loads(
                show_status_json(owner, jobid, tail=tail, multi_user=multi_user)
            )
            events_payload = json.loads(
                show_events_json(owner, jobid, multi_user=multi_user)
            )
        else:
            owner_cfg = cfg.host(owner)
            proc = transport.run_remote_vq(
                owner_cfg, "status", "localhost", jobid, "--json", "-n", str(tail)
            )
            status_payload = json.loads(proc.stdout)
            proc = transport.run_remote_vq(
                owner_cfg, "events", "localhost", jobid, "--json"
            )
            events_payload = json.loads(proc.stdout)
    except FileNotFoundError as e:
        return None, str(e)
    except transport.RemoteError as e:
        return None, str(e)
    except (config.ConfigError, json.JSONDecodeError, ValueError, OSError) as e:
        return None, f"{type(e).__name__}: {e}"
    events = events_payload.get("events")
    workspace = status_payload.get("cwd")
    samples = (
        fetch_job_samples(cfg, owner, workspace)
        if isinstance(workspace, str) and workspace
        else []
    )
    return {
        "status": status_payload,
        "events": events if isinstance(events, list) else [],
        "samples": samples,
        "owner_host": owner,
    }, None


def gather_doctor_results(timeout: float = 10.0) -> list[dict]:
    """One ``vq doctor``-equivalent sweep across every configured host.

    Reuses the public per-host doctor service so the board and the terminal
    agree check-for-check. Administratively-down hosts short-circuit
    inside ``diagnose_host`` exactly like the CLI fan-out.
    """
    from vq import doctor as doctor_service  # noqa: PLC0415

    cfg = config.load_config()
    host_names = list(cfg.hosts.keys()) or ["localhost"]
    probe_cache: dict = {}

    def _one(host: str) -> dict:
        try:
            return doctor_service.diagnose_host(
                cfg, host, timeout=timeout, probe_cache=probe_cache
            )
        except Exception as e:  # pragma: no cover — defensive sweep guard
            log.warning("fleet: doctor sweep failed for %s: %s", host, e)
            return {
                "host": host,
                "ok": False,
                "scheduler": None,
                "driver": None,
                "checks": [
                    {"name": "doctor", "ok": False, "message": str(e)}
                ],
            }

    workers = min(_MAX_WORKERS, max(1, len(host_names)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(_one, host_names))


class DoctorCache:
    """On-demand doctor-board results.

    A doctor sweep dials SSH first hops and runs remote checks on every
    host — seconds per host — so unlike the fleet snapshot it never
    runs on a timer. The board renders the last cached verdicts with
    their age; the refresh button starts a debounced background sweep.
    """

    #: Debounce between operator-triggered sweeps.
    MIN_REFRESH_SECONDS = 30

    def __init__(self, timeout: float = 10.0) -> None:
        self.timeout = timeout
        self._lock = threading.Lock()
        self._results: list[dict] | None = None
        self._gathered_at: str | None = None
        self._last_start = 0.0
        self._refreshing = False

    def state(self) -> dict[str, object]:
        with self._lock:
            return {
                "results": self._results,
                "gathered_at": self._gathered_at,
                "refreshing": self._refreshing,
            }

    def set_results(self, results: list[dict]) -> None:
        """Inject results (tests)."""
        with self._lock:
            self._results = results
            self._gathered_at = datetime.now(UTC).isoformat()

    def refresh_async(self) -> bool:
        """Start a background sweep unless one is running or the last
        one started less than :attr:`MIN_REFRESH_SECONDS` ago. Returns
        whether a sweep was started."""
        with self._lock:
            if self._refreshing:
                return False
            if monotonic() - self._last_start < self.MIN_REFRESH_SECONDS:
                return False
            self._refreshing = True
            self._last_start = monotonic()
        threading.Thread(
            target=self._run, name="vq-web-doctor-sweep", daemon=True
        ).start()
        return True

    def _run(self) -> None:
        try:
            results = gather_doctor_results(timeout=self.timeout)
            with self._lock:
                self._results = results
                self._gathered_at = datetime.now(UTC).isoformat()
        except Exception as e:  # pragma: no cover — keep the board alive
            log.warning("fleet: doctor sweep failed: %s", e)
        finally:
            with self._lock:
                self._refreshing = False


class FleetCache:
    """Background-refreshed fleet snapshot with thread-safe reads.

    The web app starts one of these in fleet mode. Route handlers call
    :meth:`snapshot` (non-blocking; ``None`` until the first sweep
    lands) so a slow or hung SSH fan-out can never block a request.
    """

    #: Debounce between operator-triggered "refresh now" sweeps, on top
    #: of the regular interval poller.
    MIN_MANUAL_REFRESH_SECONDS = 15

    def __init__(self, interval_seconds: int | None = None) -> None:
        self.interval_seconds = interval_seconds or fleet_interval_seconds()
        self._lock = threading.Lock()
        self._snapshot: FleetSnapshot | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._manual_refreshing = False
        self._refreshing = False
        self._refresh_started_at: str | None = None
        self._refresh_started: float | None = None
        self._last_refresh_finished_at: str | None = None
        self._last_refresh_error: str | None = None
        self._last_manual = 0.0
        self._trend: deque[dict[str, object]] = deque(maxlen=TREND_POINTS)

    def snapshot(self) -> FleetSnapshot | None:
        with self._lock:
            return self._snapshot

    def set_snapshot(self, snapshot: FleetSnapshot) -> None:
        """Inject a snapshot (tests, or a synchronous first fill)."""
        with self._lock:
            self._snapshot = snapshot

    def record_trend(self, snapshot: FleetSnapshot) -> None:
        """Add one sweep's totals to the bounded trend history (#168)."""
        from vq.web import calculations  # noqa: PLC0415 -- calculations imports this module

        point = calculations.trend_point(snapshot)
        with self._lock:
            self._trend.append(point)

    def trend(self) -> list[dict[str, object]]:
        with self._lock:
            return list(self._trend)

    def refresh_state(self) -> dict[str, object]:
        with self._lock:
            return {
                "refreshing": self._refreshing,
                "started_at": self._refresh_started_at,
                "elapsed_seconds": (
                    max(0, int(monotonic() - self._refresh_started))
                    if self._refreshing and self._refresh_started is not None else None
                ),
                "finished_at": self._last_refresh_finished_at,
                "error": self._last_refresh_error,
            }

    def _claim_refresh(self, *, manual: bool) -> bool:
        with self._lock:
            if self._refreshing:
                return False
            now = monotonic()
            if manual and now - self._last_manual < self.MIN_MANUAL_REFRESH_SECONDS:
                return False
            self._refreshing = True
            self._manual_refreshing = manual
            self._refresh_started = now
            self._refresh_started_at = datetime.now(UTC).isoformat()
            self._last_refresh_error = None
            if manual:
                self._last_manual = now
            return True

    def _run_refresh(self) -> FleetSnapshot:
        try:
            snapshot = carry_last_known(self.snapshot(), gather_fleet_snapshot())
            self.set_snapshot(snapshot)
            self.record_trend(snapshot)
            return snapshot
        except Exception as exc:
            with self._lock:
                self._last_refresh_error = str(exc)
            raise
        finally:
            with self._lock:
                self._refreshing = False
                self._manual_refreshing = False
                self._last_refresh_finished_at = datetime.now(UTC).isoformat()

    def refresh(self) -> FleetSnapshot | None:
        """Sweep synchronously, or reuse the cache when a sweep is already active."""
        if not self._claim_refresh(manual=False):
            return self.snapshot()
        return self._run_refresh()

    def refresh_async(self) -> bool:
        """Start a debounced manual sweep, sharing admission with the poller."""
        if not self._claim_refresh(manual=True):
            return False

        def _run() -> None:
            try:
                self._run_refresh()
            except Exception as exc:
                log.warning("fleet: manual sweep failed: %s", exc)

        threading.Thread(
            target=_run, name="vq-web-fleet-manual-sweep", daemon=True
        ).start()
        return True

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, name="vq-web-fleet-poller", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.refresh()
            except Exception as e:  # pragma: no cover — keep the poller alive
                log.warning("fleet: sweep failed: %s", e)
            self._stop.wait(self.interval_seconds)
