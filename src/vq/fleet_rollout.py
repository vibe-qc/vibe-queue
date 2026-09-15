"""Plan and execute a reproducible fleet rollout from an accepted report."""

from __future__ import annotations

import codecs
import copy
import errno
import fcntl
import hashlib
import json
import math
import os
import re
import selectors
import shlex
import signal
import stat
import subprocess
import sys
import threading
import time
import unicodedata
from collections.abc import Callable, Iterator, Mapping, Sequence, Set
from contextlib import ExitStack, contextmanager, suppress
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime
from functools import partial
from pathlib import Path, PurePosixPath
from typing import Any, Literal

from vq import admin as admin_module
from vq import (
    config,
    fleet_operation,
    fleet_release,
    legacy_failure_transition,
    paths,
    transport,
)
from vq.host import is_local_host
from vq.spec import utcnow_iso

PROGRAM_PINS = {
    "vibeqc-queue": "vq",
    "vibeqc-release": "release",
    "vibeqc-dev": "dev",
    "vibe-view": "vibe_view",
}
SCHEDULER_PROGRAM_ORDER = ("vibeqc-release", "vibeqc-dev", "vibe-view")
LOCAL_PROGRAM_ORDER = (
    "vibeqc-queue",
    "vibeqc-release",
    "vibeqc-dev",
    "vibe-view",
)
DEFAULT_DRAIN_WAIT = "4h"
ROLLOUT_HOLD_MIN_DURATION_SECONDS = 6 * 3600
ROLLOUT_HOLD_CONTROL_MARGIN_SECONDS = 3600
_DELEGATED_UPDATE_CLEANUP_MARGIN_SECONDS = 600
_HELPER_SHA = re.compile(r"(?:SOURCE-SHA (?:mismatch: helper )?)([0-9a-f]{40})")
_SOURCE_SHA = re.compile(r"[0-9a-f]{40}")
_ADMIN_SHA = re.compile(r"[0-9a-f]{12,40}")
_SOURCE_TREE_SHA256 = re.compile(r"[0-9a-f]{64}")
_DRAIN_LEASE_ID = re.compile(r"[A-Za-z0-9._:-]{1,160}")
_ROOT_PYTHON_NAME = re.compile(r"python(?:3(?:\.\d+)?)?")
_ROOT_DAEMON_SOCKET = "/var/lib/vq/daemon.sock"
_ROOT_VQ_EXECUTABLE = "/opt/vq/venv/bin/vq"
_ROOT_DAEMON_ARGV = [_ROOT_VQ_EXECUTABLE, "daemon", "run"]
DURABLE_SUPERVISOR_ACQUIRE_TIMEOUT_SECONDS = 10.0
DURABLE_SUPERVISOR_REAP_TIMEOUT_SECONDS = 1.0
_DRAIN_LIVENESS_OUTPUT_LIMIT = 2 * 1024 * 1024
_DRAIN_LIVENESS_TEXT_LIMIT = 512
_DRAIN_LIVENESS_IDENTITY_TEXT_LIMIT = 1024
_DRAIN_READ_ONLY_STATUS_SCHEMA = "vq.drain.read_only_status/1"
_DRAIN_LIVENESS_CONTROL_TIMEOUT_SECONDS = 15.0
_DRAIN_LIVENESS_MAX_WORKERS = 8
_DRAIN_LIVENESS_REAP_GRACE_SECONDS = 0.25
ENV_ROLLOUT_REENTRY_HANDOFF = "VQ_ROLLOUT_REENTRY_HANDOFF"
_ROLLOUT_REENTRY_HANDOFF_SCHEMA = "vq.rollout.reentry_handoff/1"
_LEGACY_RETAINED_ACTION_SCHEMA = "vq.fleet.legacy_retained_action/1"
_LEGACY_RETAINED_HOLD_SCHEMA = "vq.fleet.legacy_retained_hold/1"
_PLAN_BOUND_HOLD_SUPERSEDE_SCHEMA = (
    "vq.fleet.plan_bound_hold_supersede/1"
)
_LEGACY_FAILURE_SKIP_SCHEMA = "vq.fleet.legacy_failure_skip/1"
_FAILURE_JOURNAL_DIGEST_ATTR = "_failure_journal_digest_sha256"

Decision = Literal["update", "skip", "defer", "block"]
Phase = Literal["driver", "helper", "scheduler-runtime", "local-runtime"]
ProvenanceDecision = Literal["skip", "defer", "block"]
SCOPE_EXEMPT_PHASES: frozenset[str] = frozenset({"driver"})
"""Phases that ``--only`` / ``--skip`` never narrow. See :func:`select_hosts`."""
Runner = Callable[..., subprocess.CompletedProcess[str]]
Ancestry = Callable[[str, str, str], bool | None]
"""Authenticated report pin name, older SHA, newer SHA -> ancestry."""
ReportDigestResolver = Callable[[], str]


class FleetRolloutError(RuntimeError):
    """Fleet rollout planning or execution failed closed."""


class FleetProbeUnavailable(FleetRolloutError):
    """A gate could not decide because the evidence never arrived.

    Distinct from :class:`FleetRolloutError` because the two need opposite
    handling. "This host is not converged" is a verdict a retry cannot
    change. "The probe timed out" is the absence of a measurement, and a
    retry is precisely the right response -- the same distinction the admin
    verbs draw between ``precondition-failed`` and ``locked``, which is why
    this classifies as the latter.

    A subclass, so every existing ``except FleetRolloutError`` keeps catching
    it and no caller has to learn about it to stay correct.
    """

    outcome: str = "locked"


class FleetOperationFailed(FleetRolloutError):
    """A durable mutating child ran and returned a nonzero status."""


@dataclass(frozen=True)
class OperationReconciliation:
    """Controller action required after the fleet-global receipt pass."""

    driver_reentry_rollout_id: str | None = None
    driver_reentry_host: str | None = None
    driver_reentry_operation_id: str | None = None
    driver_reentry_request_sha256: str | None = None
    driver_reentry_report_digest_sha256: str | None = None
    failed_operation_hosts: tuple[tuple[str, str, str], ...] = ()
    legacy_running_actions: tuple[tuple[str, str], ...] = ()
    legacy_scheduler_holds: tuple[tuple[str, str], ...] = ()
    legacy_hold_retries: tuple[tuple[str, str], ...] = ()
    retained_legacy_holds: tuple[tuple[str, str, str], ...] = ()
    pending_failure_transitions: tuple[tuple[str, str, str], ...] = ()

    @property
    def driver_reentry_required(self) -> bool:
        return self.driver_reentry_rollout_id is not None


@dataclass(frozen=True)
class RolloutReentryCapability:
    """Exact harvested driver receipt carried across one fresh re-entry."""

    rollout_id: str
    operation_id: str
    request_sha256: str
    report_digest_sha256: str


@dataclass(frozen=True)
class LegacyRolloutReconciliation:
    """Evidence recorded while retiring pre-recorder rollout state.

    A superseded action's historical subprocess outcome remains unknown.  The
    separate hold lists distinguish a journal-only inactive observation from
    an exact conditional release and from a host whose live identity could not
    be established.
    """

    superseded_actions: tuple[tuple[str, str], ...] = ()
    retained_actions: tuple[tuple[str, str, str, str], ...] = ()
    settled_inactive_holds: tuple[tuple[str, str], ...] = ()
    released_live_holds: tuple[tuple[str, str], ...] = ()
    retained_holds: tuple[tuple[str, str, str], ...] = ()
    live_hold_state_changed: bool = False
    live_hold_refresh_required: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "superseded_actions": [
                {"rollout_id": rollout_id, "action_id": action_id}
                for rollout_id, action_id in self.superseded_actions
            ],
            "retained_actions": [
                {
                    "rollout_id": rollout_id,
                    "action_id": action_id,
                    "host": host,
                    "reason": reason,
                }
                for rollout_id, action_id, host, reason in self.retained_actions
            ],
            "settled_inactive_holds": [
                {"rollout_id": rollout_id, "host": host}
                for rollout_id, host in self.settled_inactive_holds
            ],
            "released_live_holds": [
                {"rollout_id": rollout_id, "host": host}
                for rollout_id, host in self.released_live_holds
            ],
            "retained_holds": [
                {
                    "rollout_id": rollout_id,
                    "host": host,
                    "reason": reason,
                }
                for rollout_id, host, reason in self.retained_holds
            ],
            "live_hold_state_changed": self.live_hold_state_changed,
            "live_hold_refresh_required": self.live_hold_refresh_required,
        }


@dataclass(frozen=True)
class PlanBoundHoldSupersession:
    """Durable result of one exact obsolete scheduler-hold retirement."""

    rollout_id: str
    host: str
    current_rollout_id: str
    replayed: bool


@dataclass(frozen=True)
class ResolvedHost:
    name: str
    role: str
    canonical_host: str | None
    reason: str


@dataclass(frozen=True)
class LaneState:
    configured: bool
    current_sha: str | None
    current_version: str | None
    current_tag: str | None
    dirty: bool | None
    last_ok: bool
    acknowledged: bool
    detail: str
    metrics: Mapping[str, Any] | None = None
    """Deploy metrics of the lane's last recorded update (dependency-cache
    decision, ccache hit rate, phase durations, native-rebuild flag)."""
    probe_unavailable: bool = False
    """True when this lane's live evidence could not be gathered at all.

    ``last_ok`` is a verdict and has only two values, so a lane whose probe
    did not answer has to be recorded as ``False`` -- indistinguishable from
    a lane that answered "no". That collapse cost the 2026-09-10 migration
    five supersede refusals on pbs-cluster reading "lacks strictly healthy
    exact-target evidence" while nothing was wrong: three remote vq calls
    share one 10 s doctor budget, pbs-cluster's login node needs 1.6-2.5 s each, and
    ``source-sha`` intermittently ran out.

    This carries the difference alongside the verdict rather than widening
    ``last_ok`` to three states, which every reader of it would have to learn.
    A caller that must not act on absent evidence -- the supersede gate --
    checks this; everything else keeps treating the lane as not converged,
    which is the safe reading either way."""


@dataclass
class RolloutAction:
    id: str
    phase: Phase
    host: str
    program: str
    pin_name: str
    target_sha: str
    target_version: str | None
    target_tag: str | None
    argv: list[str]
    decision: Decision
    reason: str
    before: dict[str, Any]
    rc: int | None = None
    outcome: str | None = None
    output_tail: str | None = None


@dataclass(frozen=True)
class ProvenanceLane:
    """Read-only fleet identity which the update executor cannot run.

    Root daemons deliberately start here rather than as ``RolloutAction``
    objects. Until the privilege boundary and strict PID/systemd verification
    are implemented, inventory must prevent a false convergence verdict while
    remaining mechanically incapable of invoking sudo or an update helper.
    """

    id: str
    host: str
    component: str
    kind: str
    target_sha: str
    target_version: str | None
    target_source_tree_sha256: str | None
    current_sha: str | None
    current_version: str | None
    current_source_tree_sha256: str | None
    decision: ProvenanceDecision
    reason: str
    applicable: bool | None
    evidence: dict[str, Any]


@dataclass
class RolloutPlan:
    driver: str
    report: dict[str, Any]
    topology: dict[str, dict[str, Any]]
    actions: list[RolloutAction]
    provenance_lanes: list[ProvenanceLane] = field(default_factory=list)
    topology_errors: list[str] = field(default_factory=list)
    retained_legacy_fences: list[dict[str, str]] = field(default_factory=list)
    created_at: str | None = None
    _scheduler_hold_targets: dict[
        str, tuple[tuple[str, str], ...]
    ] = field(default_factory=dict, repr=False)
    """Canonical scheduler action host to exact target/control pairs.

    This is an execution-only projection of the same config/topology snapshot
    that produced ``actions``.  It is deliberately absent from ``as_dict`` so
    the additive safety binding does not change the rollout plan wire schema.
    """

    @property
    def has_blocks(self) -> bool:
        return bool(self.topology_errors) or any(
            action.decision == "block" for action in self.actions
        ) or any(
            lane.decision == "block" for lane in self.provenance_lanes
        ) or any(
            fence.get("host") == self.driver
            for fence in self.retained_legacy_fences
        )

    @property
    def has_deferred(self) -> bool:
        return any(
            action.decision == "defer" for action in self.actions
        ) or any(
            lane.decision == "defer" for lane in self.provenance_lanes
        ) or any(
            fence.get("host") != self.driver
            for fence in self.retained_legacy_fences
        )

    @property
    def updates(self) -> list[RolloutAction]:
        return [action for action in self.actions if action.decision == "update"]

    def as_dict(self) -> dict[str, Any]:
        return {
            "driver": self.driver,
            "report": self.report,
            "topology": self.topology,
            "topology_errors": self.topology_errors,
            "retained_legacy_fences": [
                dict(fence) for fence in self.retained_legacy_fences
            ],
            "actions": [asdict(action) for action in self.actions],
            "provenance_lanes": [
                asdict(lane) for lane in self.provenance_lanes
            ],
            "coverage": _coverage_payload(self),
            "summary": {
                decision: sum(
                    action.decision == decision for action in self.actions
                )
                for decision in ("update", "skip", "defer", "block")
            },
        }


@dataclass
class RolloutRun:
    rollout_id: str
    report_digest_sha256: str
    report_source_path: str
    actions: dict[str, dict[str, Any]] = field(default_factory=dict)
    holds: dict[str, dict[str, Any]] = field(default_factory=dict)
    complete: bool = False
    failed_hosts: dict[str, str] = field(default_factory=dict)
    """Hosts whose lane failed this invocation, mapped to the failure.

    A failure is terminal for its own host and no further, so the run reports a
    degraded set rather than stopping the fleet at the first bad host. Empty on a
    clean run; a non-empty value is why ``complete`` is False."""
    legacy_retained_actions: dict[str, dict[str, Any]] = field(
        default_factory=dict
    )
    legacy_retained_holds: dict[str, dict[str, Any]] = field(
        default_factory=dict
    )
    legacy_failure_update_intent: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        if not self.legacy_retained_actions:
            payload.pop("legacy_retained_actions")
        if not self.legacy_retained_holds:
            payload.pop("legacy_retained_holds")
        if self.legacy_failure_update_intent is None:
            payload.pop("legacy_failure_update_intent")
        return payload


_FailureJournalSnapshot = tuple[
    Path, RolloutRun, legacy_failure_transition.JournalToken
]


@dataclass(frozen=True, slots=True)
class _FailureReportEpoch:
    """One immutable local ``origin/main`` accepted-report observation."""

    origin_main_commit: str
    report_source_path: str
    report_digest_sha256: str
    rollout_id: str

    def __post_init__(self) -> None:
        if (
            type(self.origin_main_commit) is not str
            or _SOURCE_SHA.fullmatch(self.origin_main_commit) is None
            or type(self.report_source_path) is not str
            or not self.report_source_path
            or type(self.report_digest_sha256) is not str
            or _SOURCE_TREE_SHA256.fullmatch(self.report_digest_sha256) is None
            or type(self.rollout_id) is not str
            or not self.rollout_id
        ):
            raise FleetRolloutError("local accepted-report epoch is malformed")


def _coverage_payload(plan: RolloutPlan) -> dict[str, Any]:
    """Describe exactly what a rollout verdict does and does not prove.

    The accepted-report planner models configured managed runtime lanes, the
    required vq user install on ``vq-only`` coordinators, and separately
    inventories read-only provenance. It does not yet mutate a privileged root
    daemon or manage the operator-side vibe-basisopt client. Keep those gaps
    beside every count so a clean verdict cannot be mistaken for whole-host or
    whole-toolset convergence.
    """
    exclusions: list[dict[str, Any]] = [
        {
            "kind": "operator-managed",
            "host": None,
            "component": "vibe-basisopt",
            "reason": "operator-managed client has no rollout lane",
        },
    ]
    action_hosts = {action.host for action in plan.actions}
    for host, raw in sorted(plan.topology.items()):
        role = raw.get("role") if isinstance(raw, Mapping) else None
        if role == "vq-only":
            if host not in action_hosts:
                exclusions.append(
                    {
                        "kind": "topology-role",
                        "host": host,
                        "component": "vibeqc-queue",
                        "reason": (
                            "vq-only user install has no executable local "
                            "scheduler lane"
                        ),
                    }
                )
            for component in ("vibeqc-release", "vibeqc-dev", "vibe-view"):
                exclusions.append(
                    {
                        "kind": "topology-role",
                        "host": host,
                        "component": component,
                        "reason": (
                            f"vq-only host intentionally has no {component} lane"
                        ),
                    }
                )
        elif role == "excluded":
            exclusions.append(
                {
                    "kind": "topology-role",
                    "host": host,
                    "component": "host",
                    "reason": "host is explicitly excluded from rollout",
                }
            )
    managed = [
        action
        for action in plan.actions
        if plan.topology.get(action.host, {}).get("role") == "managed"
        if action.before.get("configured") is True
    ]
    vq_user = [
        action
        for action in plan.actions
        if plan.topology.get(action.host, {}).get("role") == "vq-only"
        and action.program == "vibeqc-queue"
    ]
    provenance = plan.provenance_lanes
    return {
        "scope": "managed-lanes+vq-user-lanes+read-only-provenance",
        "whole_fleet_convergence_asserted": False,
        "managed_lanes": {
            "total": len(managed),
            "converged": sum(
                action.decision == "skip" for action in managed
            ),
        },
        "vq_user_lanes": {
            "total": len(vq_user),
            "configured": sum(
                action.before.get("configured") is True for action in vq_user
            ),
            "converged": sum(action.decision == "skip" for action in vq_user),
            "deferred": sum(action.decision == "defer" for action in vq_user),
            "blocked": sum(action.decision == "block" for action in vq_user),
        },
        "provenance_lanes": {
            "total": len(provenance),
            "converged": sum(
                lane.decision == "skip" and lane.applicable is True
                for lane in provenance
            ),
            "not_applicable": sum(
                lane.applicable is False for lane in provenance
            ),
            "deferred": sum(lane.decision == "defer" for lane in provenance),
            "blocked": sum(lane.decision == "block" for lane in provenance),
        },
        "exclusions": exclusions,
    }


def rollout_state_dir() -> Path:
    return paths.state_root() / "rollouts"


def rollout_state_path(rollout_id: str) -> Path:
    safe = re.sub(r"[^A-Za-z0-9_.-]", "-", rollout_id)
    return rollout_state_dir() / f"{safe}.json"


def rollout_lock_path(rollout_id: str) -> Path:
    """Return the fleet-wide coordinator lock shared by every report."""
    del rollout_id
    return rollout_state_dir() / ".fleet-rollout.lock"


_rollout_lock_local = threading.local()


def _inherited_rollout_action_lock(
    rollout_id: str,
) -> tuple[int, Path] | None:
    """Adopt the controller's global lock inside one durable action child."""
    encoded = os.environ.get(fleet_operation.ENV_LIFECYCLE_HANDOFF)
    if encoded is None:
        return None
    try:
        raw = json.loads(encoded)
    except json.JSONDecodeError as exc:
        raise FleetRolloutError(
            "fleet action lifecycle handoff is not valid JSON"
        ) from exc
    if not isinstance(raw, dict):
        raise FleetRolloutError("fleet action lifecycle handoff is malformed")
    inherited = raw.get("rollout_lock")
    if inherited is None:
        return None
    try:
        context = fleet_operation.execution_context_from_environ()
        if context is None:
            raise FleetRolloutError(
                "fleet action rollout lock lacks an execution context"
            )
        identity = fleet_operation.validate_live_execution_context(context)
    except fleet_operation.OperationError as exc:
        raise FleetRolloutError(
            f"invalid fleet action execution context: {exc}"
        ) from exc
    if identity.rollout_id != rollout_id:
        raise FleetRolloutError(
            "fleet action rollout lock belongs to another rollout"
        )
    if not isinstance(inherited, dict) or set(inherited) != {
        "rollout_id", "fd", "path",
    }:
        raise FleetRolloutError("fleet action rollout lock has invalid fields")
    fd = inherited.get("fd")
    path = inherited.get("path")
    if (
        inherited.get("rollout_id") != rollout_id
        or not isinstance(fd, int)
        or isinstance(fd, bool)
        or fd < 3
        or not isinstance(path, str)
        or not path.startswith("/")
    ):
        raise FleetRolloutError("fleet action rollout lock is malformed")
    lock_path = Path(path)
    _validate_inherited_rollout_lock(fd, lock_path)
    # Consume only the global capability. The subsequent admin lifecycle
    # context still needs the checkout/target descriptors from this payload.
    replacement = dict(raw)
    replacement.pop("rollout_lock", None)
    os.environ[fleet_operation.ENV_LIFECYCLE_HANDOFF] = json.dumps(
        replacement,
        separators=(",", ":"),
        sort_keys=True,
    )
    os.set_inheritable(fd, False)
    return fd, lock_path


@contextmanager
def _rollout_execution_lock(rollout_id: str) -> Iterator[None]:
    """Fence concurrent fleet mutations, including different reports.

    The lock is reentrant only within the current thread so ``execute_plan``
    can call the public ``execute_one`` seam without deadlocking. A different
    process or thread fails immediately instead of sharing the deterministic
    lease owner and letting one invocation release another's protection.
    """
    path = rollout_lock_path(rollout_id)
    key = str(path)
    held = getattr(_rollout_lock_local, "paths", set())
    if key in held:
        yield
        return
    inherited = _inherited_rollout_action_lock(rollout_id)
    if inherited is not None:
        inherited_fd, inherited_path = inherited
        previous_active = getattr(_rollout_lock_local, "active", {})
        _rollout_lock_local.paths = {*held, key}
        _rollout_lock_local.active = {
            **previous_active,
            key: (inherited_fd, inherited_path),
        }
        try:
            yield
        finally:
            _rollout_lock_local.paths = held
            _rollout_lock_local.active = previous_active
            with suppress(OSError):
                os.close(inherited_fd)
        return
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    parent_info = path.parent.lstat()
    if (
        not stat.S_ISDIR(parent_info.st_mode)
        or parent_info.st_uid != os.geteuid()
        or stat.S_IMODE(parent_info.st_mode) & 0o022
    ):
        raise FleetRolloutError(
            f"unsafe fleet rollout lock directory {path.parent}: expected an "
            "owner-controlled non-writable real directory"
        )
    flags = os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    try:
        fd = os.open(path, flags, 0o600)
    except OSError as exc:
        raise FleetRolloutError(
            f"unsafe fleet rollout lock {path}: could not open without following links"
        ) from exc
    try:
        info = os.fstat(fd)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_nlink != 1
        ):
            raise FleetRolloutError(
                f"unsafe fleet rollout lock {path}: expected an owner-only "
                "regular file with one link"
            )
        if stat.S_IMODE(info.st_mode) != 0o600:
            # Earlier vq releases created this same permanent owner-controlled
            # file through the process umask (normally 0644). Tighten that
            # exact inode in place so upgraded coordinators do not all fail on
            # their first rollout, then validate the opened descriptor again.
            os.fchmod(fd, 0o600)
            info = os.fstat(fd)
            if stat.S_IMODE(info.st_mode) != 0o600:
                raise FleetRolloutError(
                    f"unsafe fleet rollout lock {path}: could not enforce mode 0600"
                )
        if os.get_inheritable(fd):
            os.set_inheritable(fd, False)
        handle = os.fdopen(fd, "r+", encoding="utf-8")
        fd = -1
    except BaseException:
        if fd >= 0:
            os.close(fd)
        raise
    with handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno not in {errno.EACCES, errno.EAGAIN}:
                raise
            raise FleetRolloutError(
                "another fleet rollout is already executing; wait for the "
                "active coordinator before retrying"
            ) from exc
        previous = held
        previous_active = getattr(_rollout_lock_local, "active", {})
        _rollout_lock_local.paths = {*held, key}
        _rollout_lock_local.active = {
            **previous_active,
            key: (handle.fileno(), path),
        }
        try:
            yield
        finally:
            _rollout_lock_local.paths = previous
            _rollout_lock_local.active = previous_active
            # Descriptors may have been inherited by a detached durable action.
            # Close-only last-owner semantics retain exclusion until that child
            # is terminal; an explicit LOCK_UN here would release every OFD copy.


def attach_active_rollout_lock(
    lifecycle_handoff: str,
    *,
    rollout_id: str,
) -> str:
    """Bind the active global rollout OFD into a durable action handoff."""
    key = str(rollout_lock_path(rollout_id))
    active = getattr(_rollout_lock_local, "active", {})
    item = active.get(key)
    if item is None:
        raise FleetRolloutError("durable action has no active global rollout fence")
    fd, path = item
    try:
        raw = json.loads(lifecycle_handoff)
    except json.JSONDecodeError as exc:
        raise FleetRolloutError("lifecycle handoff is not valid JSON") from exc
    if (
        not isinstance(raw, dict)
        or set(raw) != {"schema", "locks"}
        or raw.get("schema") != "vq.toolset.lifecycle_handoff/1"
    ):
        raise FleetRolloutError("lifecycle handoff has invalid fields")
    enriched = dict(raw)
    enriched["rollout_lock"] = {
        "rollout_id": rollout_id,
        "fd": fd,
        "path": str(path),
    }
    return json.dumps(enriched, separators=(",", ":"), sort_keys=True)


def _active_rollout_reentry_handoff(
    capability: RolloutReentryCapability,
    *,
    lifecycle_handoff: str,
) -> tuple[str, tuple[int, ...]]:
    """Serialize both controller fences for an atomic fresh re-entry."""
    rollout_id = capability.rollout_id
    key = str(rollout_lock_path(rollout_id))
    active = getattr(_rollout_lock_local, "active", {})
    item = active.get(key)
    if item is None:
        raise FleetRolloutError("rollout re-entry has no active global fence")
    rollout_fd, rollout_path = item
    try:
        lifecycle = json.loads(lifecycle_handoff)
    except json.JSONDecodeError as exc:
        raise FleetRolloutError(
            "rollout re-entry lifecycle handoff is not valid JSON"
        ) from exc
    if (
        not isinstance(lifecycle, dict)
        or set(lifecycle) != {"schema", "locks"}
        or lifecycle.get("schema") != "vq.toolset.lifecycle_handoff/1"
        or not isinstance(lifecycle.get("locks"), list)
        or not lifecycle["locks"]
    ):
        raise FleetRolloutError(
            "rollout re-entry lifecycle handoff has invalid fields"
        )
    lifecycle_fds: list[int] = []
    for lock in lifecycle["locks"]:
        if not isinstance(lock, dict):
            raise FleetRolloutError("rollout re-entry lifecycle lock is malformed")
        fd = lock.get("fd")
        if not isinstance(fd, int) or isinstance(fd, bool) or fd < 3:
            raise FleetRolloutError("rollout re-entry lifecycle fd is malformed")
        lifecycle_fds.append(fd)
    pass_fds = (rollout_fd, *lifecycle_fds)
    if len(set(pass_fds)) != len(pass_fds):
        raise FleetRolloutError("rollout re-entry repeats a lock descriptor")
    payload = {
        "schema": _ROLLOUT_REENTRY_HANDOFF_SCHEMA,
        "rollout_id": rollout_id,
        "operation_id": capability.operation_id,
        "request_sha256": capability.request_sha256,
        "report_digest_sha256": capability.report_digest_sha256,
        "parent_pid": os.getpid(),
        "parent_start_ticks": fleet_operation._pid_start_ticks(os.getpid()),
        "parent_boot_id": fleet_operation._boot_id(),
        "rollout_lock": {
            "fd": rollout_fd,
            "path": str(rollout_path),
        },
        "lifecycle": lifecycle,
    }
    return (
        json.dumps(payload, separators=(",", ":"), sort_keys=True),
        pass_fds,
    )


def _validate_inherited_rollout_lock(fd: int, path: Path) -> None:
    expected = rollout_lock_path("inherited")
    if path != expected:
        raise FleetRolloutError(
            "rollout re-entry handoff does not bind the global lock path"
        )
    try:
        info = os.fstat(fd)
        named = os.stat(path, follow_symlinks=False)
        parent = path.parent.lstat()
    except OSError as exc:
        raise FleetRolloutError(
            f"rollout re-entry lock is unavailable: {exc}"
        ) from exc
    if (
        not stat.S_ISDIR(parent.st_mode)
        or parent.st_uid != os.geteuid()
        or stat.S_IMODE(parent.st_mode) & 0o022
        or not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.geteuid()
        or info.st_nlink != 1
        or stat.S_IMODE(info.st_mode) != 0o600
        or not stat.S_ISREG(named.st_mode)
        or named.st_uid != os.geteuid()
        or named.st_nlink != 1
        or (named.st_dev, named.st_ino) != (info.st_dev, info.st_ino)
    ):
        raise FleetRolloutError(
            "rollout re-entry handoff does not bind the exact global lock"
        )
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        raise FleetRolloutError(
            "rollout re-entry handoff does not own the global lock"
        ) from exc


@contextmanager
def adopt_rollout_reentry_handoff(
    *,
    expected_rollout_id: str | None,
    expected_lifecycle_resources: tuple[tuple[str, str], ...],
) -> Iterator[RolloutReentryCapability | None]:
    """Adopt predecessor-held locks before a fresh controller touches refs."""
    encoded = os.environ.pop(ENV_ROLLOUT_REENTRY_HANDOFF, None)
    if encoded is None:
        yield None
        return
    try:
        raw = json.loads(encoded)
    except json.JSONDecodeError as exc:
        raise FleetRolloutError(
            "rollout re-entry handoff is not valid JSON"
        ) from exc
    expected_fields = {
        "schema",
        "rollout_id",
        "operation_id",
        "request_sha256",
        "report_digest_sha256",
        "parent_pid",
        "parent_start_ticks",
        "parent_boot_id",
        "rollout_lock",
        "lifecycle",
    }
    if not isinstance(raw, dict) or set(raw) != expected_fields:
        raise FleetRolloutError("rollout re-entry handoff has invalid fields")
    rollout_id = raw.get("rollout_id")
    operation_id = raw.get("operation_id")
    request_sha256 = raw.get("request_sha256")
    report_digest_sha256 = raw.get("report_digest_sha256")
    parent_pid = raw.get("parent_pid")
    parent_ticks = raw.get("parent_start_ticks")
    parent_boot_id = raw.get("parent_boot_id")
    lock = raw.get("rollout_lock")
    lifecycle = raw.get("lifecycle")
    if (
        raw.get("schema") != _ROLLOUT_REENTRY_HANDOFF_SCHEMA
        or not isinstance(rollout_id, str)
        or not rollout_id
        or rollout_id != expected_rollout_id
        or not isinstance(operation_id, str)
        or _SOURCE_TREE_SHA256.fullmatch(operation_id) is None
        or not isinstance(request_sha256, str)
        or _SOURCE_TREE_SHA256.fullmatch(request_sha256) is None
        or not isinstance(report_digest_sha256, str)
        or _SOURCE_TREE_SHA256.fullmatch(report_digest_sha256) is None
        or not isinstance(parent_pid, int)
        or isinstance(parent_pid, bool)
        or parent_pid != os.getppid()
        or (
            parent_ticks is not None
            and (
                not isinstance(parent_ticks, int)
                or isinstance(parent_ticks, bool)
                or parent_ticks < 0
            )
        )
        or (parent_boot_id is not None and not isinstance(parent_boot_id, str))
        or not isinstance(lock, dict)
        or set(lock) != {"fd", "path"}
        or not isinstance(lifecycle, dict)
    ):
        raise FleetRolloutError("rollout re-entry handoff is malformed")
    live_ticks = fleet_operation._pid_start_ticks(parent_pid)
    if parent_ticks is not None and live_ticks != parent_ticks:
        raise FleetRolloutError("rollout re-entry parent identity changed")
    live_boot_id = fleet_operation._boot_id()
    if parent_boot_id is not None and live_boot_id != parent_boot_id:
        raise FleetRolloutError("rollout re-entry boot identity changed")
    rollout_fd = lock.get("fd")
    rollout_path = lock.get("path")
    if (
        not isinstance(rollout_fd, int)
        or isinstance(rollout_fd, bool)
        or rollout_fd < 3
        or not isinstance(rollout_path, str)
        or not rollout_path.startswith("/")
    ):
        raise FleetRolloutError("rollout re-entry lock is malformed")
    lifecycle_locks = lifecycle.get("locks")
    if not isinstance(lifecycle_locks, list):
        raise FleetRolloutError("rollout re-entry lifecycle locks are malformed")
    lifecycle_fds = [
        item.get("fd") for item in lifecycle_locks if isinstance(item, dict)
    ]
    if (
        len(lifecycle_fds) != len(lifecycle_locks)
        or rollout_fd in lifecycle_fds
        or len(set(lifecycle_fds)) != len(lifecycle_fds)
    ):
        raise FleetRolloutError("rollout re-entry lock descriptors are invalid")
    _validate_inherited_rollout_lock(rollout_fd, Path(rollout_path))
    key = str(rollout_lock_path(rollout_id))
    held = getattr(_rollout_lock_local, "paths", set())
    active = getattr(_rollout_lock_local, "active", {})
    if key in held or key in active:
        raise FleetRolloutError("rollout re-entry cannot replace an active lock")
    os.set_inheritable(rollout_fd, False)
    _rollout_lock_local.paths = {*held, key}
    _rollout_lock_local.active = {
        **active,
        key: (rollout_fd, Path(rollout_path)),
    }
    lifecycle_encoded = json.dumps(
        lifecycle,
        separators=(",", ":"),
        sort_keys=True,
    )
    try:
        with ExitStack() as lifecycle_stack:
            try:
                lifecycle_stack.enter_context(
                    admin_module._adopt_rollout_toolset_lifecycle_handoff(
                        lifecycle_encoded,
                        expected_resources=expected_lifecycle_resources,
                    )
                )
            except admin_module.AdminError as exc:
                raise FleetRolloutError(
                    f"invalid rollout re-entry lifecycle handoff: {exc}"
                ) from exc
            yield RolloutReentryCapability(
                rollout_id=rollout_id,
                operation_id=operation_id,
                request_sha256=request_sha256,
                report_digest_sha256=report_digest_sha256,
            )
    finally:
        _rollout_lock_local.paths = held
        _rollout_lock_local.active = active
        with suppress(OSError):
            os.close(rollout_fd)


def _operation_attempts(record: object) -> list[dict[str, Any]]:
    """Return a strict defensive copy of durable refs from one action record."""
    if not isinstance(record, Mapping):
        return []
    raw = record.get("operation_attempts")
    if raw is None:
        return []
    if not isinstance(raw, list) or not all(isinstance(item, dict) for item in raw):
        raise FleetRolloutError("invalid operation_attempts rollout journal field")
    expected_keys = {
        "operation_id",
        "request_sha256",
        "attempt",
        "report_digest_sha256",
        "identity",
        "state",
        "retry_safe",
        "harvested",
        "failure_fence_consumed",
        "failure_retry_authorized",
    }
    valid_states = {
        "prepared",
        "starting",
        "running-pre-authorization",
        "running-authorized",
        "authorized-unactivated",
        "abandoned-pre-authorization",
        "outcome-unknown",
        "completed",
    }
    result: list[dict[str, Any]] = []
    seen_operations: set[str] = set()
    seen_attempts: set[int] = set()
    prior_attempt = 0
    for item in raw:
        if set(item) != expected_keys:
            raise FleetRolloutError(
                "invalid operation_attempts entry fields in rollout journal"
            )
        operation = item.get("operation_id")
        request_digest = item.get("request_sha256")
        report_digest = item.get("report_digest_sha256")
        attempt = item.get("attempt")
        identity_raw = item.get("identity")
        state = item.get("state")
        if (
            not isinstance(operation, str)
            or _SOURCE_TREE_SHA256.fullmatch(operation) is None
            or not isinstance(request_digest, str)
            or _SOURCE_TREE_SHA256.fullmatch(request_digest) is None
            or not isinstance(report_digest, str)
            or _SOURCE_TREE_SHA256.fullmatch(report_digest) is None
            or not isinstance(attempt, int)
            or isinstance(attempt, bool)
            or attempt <= 0
            or not isinstance(identity_raw, Mapping)
            or state not in valid_states
            or not isinstance(item.get("retry_safe"), bool)
            or not isinstance(item.get("harvested"), bool)
            or not isinstance(item.get("failure_fence_consumed"), bool)
            or not isinstance(item.get("failure_retry_authorized"), bool)
            or (
                item.get("failure_fence_consumed") is True
                and item.get("harvested") is not True
            )
            or (
                item.get("failure_retry_authorized") is True
                and item.get("failure_fence_consumed") is not True
            )
        ):
            raise FleetRolloutError(
                "invalid operation_attempts entry values in rollout journal"
            )
        try:
            identity = fleet_operation.OperationIdentity.from_dict(identity_raw)
        except fleet_operation.OperationError as exc:
            raise FleetRolloutError(
                f"invalid operation_attempts identity in rollout journal: {exc}"
            ) from exc
        if (
            identity.attempt != attempt
            or identity.report_digest_sha256 != report_digest
            or fleet_operation.operation_id(identity) != operation
            or operation in seen_operations
            or attempt in seen_attempts
            or attempt <= prior_attempt
        ):
            raise FleetRolloutError(
                "operation_attempts entries must have unique, increasing identities"
            )
        seen_operations.add(operation)
        seen_attempts.add(attempt)
        prior_attempt = attempt
        result.append(dict(item))
    return result


def driver_reentry_capability(
    run: RolloutRun,
    *,
    action_id: str,
) -> RolloutReentryCapability:
    """Return the sole exact successful harvested driver receipt."""
    record = run.actions.get(action_id)
    if (
        not isinstance(record, Mapping)
        or record.get("driver_reentry_required") is not True
        or record.get("status") != "success"
    ):
        raise FleetRolloutError(
            "driver update lacks a terminal fresh-reentry receipt"
        )
    attempts = _operation_attempts(record)
    if not attempts or attempts[-1].get("harvested") is not True:
        raise FleetRolloutError(
            "driver update has no latest harvested operation receipt"
        )
    item = attempts[-1]
    operation_id = item["operation_id"]
    try:
        observed = fleet_operation.observe_operation(operation_id)
    except fleet_operation.OperationError as exc:
        raise FleetRolloutError(
            f"driver update receipt is unavailable: {exc}"
        ) from exc
    identity = _identity_from_observation(observed)
    result = observed.result or {}
    if (
        observed.state != "completed"
        or identity.phase != "driver"
        or identity.rollout_id != run.rollout_id
        or identity.action_id != action_id
        or identity.report_digest_sha256 != run.report_digest_sha256
        or item["request_sha256"] != _request_sha256(observed)
        or result.get("status") != "success"
        or result.get("executed") is not True
        or result.get("returncode") != 0
    ):
        raise FleetRolloutError(
            "driver update receipt does not prove one exact successful action"
        )
    return RolloutReentryCapability(
        rollout_id=run.rollout_id,
        operation_id=operation_id,
        request_sha256=item["request_sha256"],
        report_digest_sha256=identity.report_digest_sha256,
    )


def _replace_action_record(
    run: RolloutRun,
    action_id: str,
    record: Mapping[str, Any],
) -> None:
    """Replace mutable action state without ever deleting durable attempts."""
    attempts = _operation_attempts(run.actions.get(action_id))
    replacement = dict(record)
    supplied = _operation_attempts(replacement)
    if supplied:
        by_id = {item["operation_id"]: item for item in attempts}
        order = [item["operation_id"] for item in attempts]
        for item in supplied:
            operation = item["operation_id"]
            if operation not in by_id:
                order.append(operation)
            by_id[operation] = item
        attempts = [by_id[operation] for operation in order]
    if attempts:
        replacement["operation_attempts"] = attempts
    run.actions[action_id] = replacement


@contextmanager
def rollout_execution_lock(rollout_id: str) -> Iterator[None]:
    """Public fleet coordinator fence for execution through verification."""
    with _rollout_execution_lock(rollout_id):
        yield


def save_run(run: RolloutRun) -> Path:
    """Atomically persist resumable orchestration state."""
    path = rollout_state_path(run.rollout_id)
    paths.atomic_write_text(
        path,
        json.dumps(run.as_dict(), indent=2, sort_keys=True) + "\n",
    )
    return path


def _load_run_with_digest(
    rollout_id: str,
) -> tuple[RolloutRun | None, str | None]:
    path = rollout_state_path(rollout_id)
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None, None
    except OSError as exc:
        raise FleetRolloutError(f"invalid rollout state {path}: {exc}") from exc
    try:
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise TypeError("journal root is not an object")
        run = RolloutRun(**payload)
        if run.rollout_id != rollout_id:
            raise ValueError("journal rollout_id does not match its path")
        digest = legacy_failure_transition.canonical_json_sha256(payload)
    except (
        TypeError,
        ValueError,
        json.JSONDecodeError,
        legacy_failure_transition.TransitionError,
    ) as exc:
        raise FleetRolloutError(f"invalid rollout state {path}: {exc}") from exc
    object.__setattr__(run, _FAILURE_JOURNAL_DIGEST_ATTR, digest)
    return run, digest


def load_run(rollout_id: str) -> RolloutRun | None:
    run, _digest = _load_run_with_digest(rollout_id)
    return run


def finalize_run(
    run: RolloutRun,
    *,
    complete: bool,
    report_digest_resolver: ReportDigestResolver | None = None,
) -> RolloutRun:
    """Finalize from the newest journal while fenced against another run.

    The CLI keeps the fleet-wide coordinator lock across execution, live
    verification, and this finalization. Reload the newest journal anyway so a
    direct caller cannot write an earlier in-memory snapshot over a newer
    action or hold record.
    """
    with _rollout_execution_lock(run.rollout_id):
        if report_digest_resolver is not None:
            current_digest = report_digest_resolver()
            if current_digest != run.report_digest_sha256:
                raise FleetRolloutError(
                    "accepted fleet report changed before finalization; "
                    "refusing to mark stale report "
                    f"{run.report_digest_sha256[:12]} complete now that "
                    f"{current_digest[:12]} is current"
                )
        current = load_run(run.rollout_id) or run
        if current.report_digest_sha256 != run.report_digest_sha256:
            raise FleetRolloutError(
                "rollout journal changed to a different accepted report "
                "during final verification"
            )
        unsettled_hold = any(
            isinstance(value, dict)
            and value.get("owned") is True
            and value.get("status") in {"active", "cleanup-failed"}
            for value in current.holds.values()
        )
        unsettled_failure = bool(current.failed_hosts) or any(
            isinstance(value, dict)
            and value.get("status") in {"failed", "running"}
            for value in current.actions.values()
        )
        current.complete = (
            complete and not unsettled_hold and not unsettled_failure
        )
        save_run(current)
        return current


def _program_map(
    programs: Mapping[str, Any],
    host: str,
) -> dict[str, Mapping[str, Any]]:
    entries = programs.get(host)
    if not isinstance(entries, list):
        return {}
    return {
        str(entry.get("name")): entry
        for entry in entries
        if isinstance(entry, dict) and isinstance(entry.get("name"), str)
    }


def _admin_host(
    admin_status: Mapping[str, Any],
    host: str,
) -> Mapping[str, Any]:
    payload = admin_status.get(host)
    return payload if isinstance(payload, dict) else {}


def _local_lane_state(
    admin_status: Mapping[str, Any],
    programs: Mapping[str, Any],
    *,
    host: str,
    program: str,
) -> LaneState:
    admin_host = _admin_host(admin_status, host)
    envs = admin_host.get("envs")
    env = next(
        (
            item
            for item in envs
            if isinstance(item, dict) and item.get("name") == program
        ),
        None,
    ) if isinstance(envs, list) else None
    program_entry = _program_map(programs, host).get(program)
    if env is None or env.get("error") == "git_dir not a directory":
        return LaneState(
            configured=False,
            current_sha=None,
            current_version=None,
            current_tag=None,
            dirty=None,
            last_ok=False,
            acknowledged=False,
            detail="no managed local venv lane",
        )
    current_sha = None
    current_version = None
    dirty = env.get("is_dirty")
    detail = str(env.get("error") or "")
    if program_entry is not None:
        candidate = program_entry.get("current_git_sha_full")
        if isinstance(candidate, str) and len(candidate) == 40:
            current_sha = candidate
        version = program_entry.get("import_version")
        if isinstance(version, str):
            current_version = version
        entry_dirty = program_entry.get("current_git_dirty")
        if isinstance(entry_dirty, bool):
            dirty = entry_dirty
        if program_entry.get("status") != "OK":
            detail = str(program_entry.get("reason") or "program unhealthy")
    if current_sha is None:
        expected = env.get("last_expected_sha")
        if isinstance(expected, str) and len(expected) == 40:
            current_sha = expected
    if current_version is None and isinstance(env.get("current_version"), str):
        current_version = str(env["current_version"])
    current_tag = next(
        (
            candidate
            for candidate in (
                (
                    program_entry.get("current_git_describe")
                    if program_entry is not None
                    else None
                ),
                env.get("current_describe"),
            )
            if isinstance(candidate, str)
            and fleet_release.semver_from_text(candidate) is not None
        ),
        None,
    )
    # ``last_tag`` is the requested tag from the last transaction, not an
    # observation of the current checkout.  It remains a valid compatibility
    # fallback only after that transaction succeeded; a failed atomic update
    # may have restored an older checkout while retaining the requested tag.
    if (
        current_tag is None
        and env.get("last_success") is True
        and isinstance(env.get("last_tag"), str)
        and fleet_release.semver_from_text(str(env["last_tag"])) is not None
    ):
        current_tag = str(env["last_tag"])
    acknowledged = env.get("last_marked_ok_at") is not None
    installed_matches_checkout = env.get("installed_sha_matches_checkout")
    last_ok = (
        env.get("last_success") is True
        and not acknowledged
        and dirty is False
        # A failed atomic update can roll the checkout back without
        # reinstalling the editable package. The live SHA may then be a
        # perfectly valid descendant of the accepted pin while the venv still
        # serves different code. Missing evidence remains compatible with
        # status payloads from older vq versions, but an explicit mismatch is
        # never a verified deployment.
        and installed_matches_checkout is not False
        and (program_entry is None or program_entry.get("status") == "OK")
    )
    return LaneState(
        configured=True,
        current_sha=current_sha,
        current_version=current_version,
        current_tag=current_tag,
        dirty=dirty if isinstance(dirty, bool) else None,
        last_ok=last_ok,
        acknowledged=acknowledged,
        detail=detail or "managed local venv",
    )


def _vq_user_lane_state(
    admin_status: Mapping[str, Any],
    programs: Mapping[str, Any],
    doctor: Mapping[str, Any],
    *,
    host: str,
    target_sha: str,
    target_version: str | None,
    target_tree_sha256: str | None,
) -> tuple[LaneState, str | None, str | None]:
    """Return the required user-vq lane and its fail-closed diagnostics.

    A ``vq-only`` host is not an optional chemistry target. Its target-local
    ``[programs.vibeqc-queue]`` registration is the update primitive, so two
    successful discovery surfaces that both omit it are a configuration block.
    A failed or contradictory sweep is transient/ambiguous and therefore
    defers. Exact convergence is intentionally stricter than the mixed-version
    local-runtime reader: the live program, installed checkout, canonical LAST
    OK record, and freshly pinged user daemon must all identify the accepted
    vq source and package tree.
    """
    empty = LaneState(
        configured=False,
        current_sha=None,
        current_version=None,
        current_tag=None,
        dirty=None,
        last_ok=False,
        acknowledged=False,
        detail="required vq-only user install is not registered",
    )
    if host not in admin_status:
        return empty, "admin status probe omitted the required host", None
    raw_admin_host = admin_status.get(host)
    if not isinstance(raw_admin_host, Mapping):
        return empty, "admin status probe returned a malformed host record", None
    admin_host = raw_admin_host
    admin_error = admin_host.get("error")
    if isinstance(admin_error, str) and admin_error.strip():
        return empty, f"admin status probe failed: {admin_error}", None

    if host not in programs:
        return empty, "programs probe omitted the required host", None
    program_payload = programs.get(host)
    if isinstance(program_payload, Mapping):
        program_error = program_payload.get("error")
        if isinstance(program_error, str) and program_error.strip():
            return empty, f"programs probe failed: {program_error}", None
        return empty, "programs probe returned a malformed host record", None
    if not isinstance(program_payload, list):
        return empty, "programs probe returned a malformed host record", None

    if host not in doctor:
        return empty, "doctor probe omitted the required host", None
    raw_doctor_host = doctor.get(host)
    if not isinstance(raw_doctor_host, Mapping):
        return empty, "doctor probe returned a malformed host record", None
    doctor_error = raw_doctor_host.get("error")
    if doctor_error is not None:
        if isinstance(doctor_error, str) and doctor_error.strip():
            return empty, f"doctor probe failed: {doctor_error}", None
        return empty, "doctor probe returned a malformed host record", None
    if not isinstance(raw_doctor_host.get("checks"), list):
        return empty, "doctor probe returned no check list", None

    envs = admin_host.get("envs")
    if not isinstance(envs, list):
        return empty, "admin status probe returned no env list", None
    env = next(
        (
            item
            for item in envs
            if isinstance(item, Mapping) and item.get("name") == "vibeqc-queue"
        ),
        None,
    )
    program = _program_map(programs, host).get("vibeqc-queue")

    if isinstance(program, Mapping) and program.get("kind") not in (None, "venv"):
        return (
            empty,
            None,
            "required vq-only user install is not updateable: target "
            "[programs.vibeqc-queue] must use kind=\"venv\"",
        )
    if env is None and program is None:
        return (
            empty,
            None,
            "required vq-only user install is missing; register "
            "[programs.vibeqc-queue] as a valid kind=\"venv\" checkout "
            "in the target host config",
        )
    if env is None or program is None:
        return (
            empty,
            "admin status and programs status snapshots disagree about the "
            "required vibeqc-queue lane",
            None,
        )
    env_error = env.get("error")
    if isinstance(env_error, str) and env_error:
        return (
            empty,
            None,
            "required vq-only user install is not updateable: " + env_error,
        )

    candidate = program.get("current_git_sha_full")
    current_sha = (
        candidate
        if isinstance(candidate, str) and _SOURCE_SHA.fullmatch(candidate)
        else None
    )
    version = program.get("import_version")
    current_version = version if isinstance(version, str) else None
    dirty_value = program.get("current_git_dirty")
    if not isinstance(dirty_value, bool):
        dirty_value = env.get("is_dirty")
    dirty = dirty_value if isinstance(dirty_value, bool) else None
    acknowledged = env.get("last_marked_ok_at") is not None

    def _admin_sha_matches(
        value: object,
        reference: str,
        *,
        exact: bool,
    ) -> bool:
        if not isinstance(value, str):
            return False
        if exact:
            return value == reference
        return _ADMIN_SHA.fullmatch(value) is not None and reference.startswith(
            value
        )

    admin_identities = (
        ("current_sha", env.get("current_sha"), False),
        ("last_sha", env.get("last_sha"), False),
        ("last_expected_sha", env.get("last_expected_sha"), True),
        ("last_installed_sha", env.get("last_installed_sha"), False),
    )
    identity_disagreements = [
        name
        for name, value, exact in admin_identities
        if value is not None
        and current_sha is not None
        and not _admin_sha_matches(value, current_sha, exact=exact)
    ]
    discovery_error = None
    if (
        env.get("last_success") is True
        and env.get("installed_sha_matches_checkout") is True
        and identity_disagreements
    ):
        discovery_error = (
            "admin and program snapshots disagree about the deployed SHA: "
            + ", ".join(identity_disagreements)
        )

    proof_errors: list[str] = []
    if program.get("status") != "OK":
        proof_errors.append("program.status")
    if current_sha != target_sha:
        proof_errors.append("program.source_sha")
    if current_version != target_version:
        proof_errors.append("program.version")
    if dirty is not False:
        proof_errors.append("program.dirty")
    if env.get("last_success") is not True:
        proof_errors.append("admin.last_success")
    if acknowledged:
        proof_errors.append("admin.last_marked_ok_at")
    if env.get("installed_sha_matches_checkout") is not True:
        proof_errors.append("admin.installed_sha_matches_checkout")
    for name, value, exact in admin_identities:
        if not _admin_sha_matches(value, target_sha, exact=exact):
            proof_errors.append(f"admin.{name}")

    daemon = _doctor_checks(doctor, host).get("daemon_rpc") or {}
    if daemon.get("ok") is not True:
        proof_errors.append("daemon.status")
    if daemon.get("version") != target_version:
        proof_errors.append("daemon.version")
    if daemon.get("source_sha") != target_sha:
        proof_errors.append("daemon.source_sha")
    if target_tree_sha256 is None:
        proof_errors.append("target.source_tree_sha256")
    elif daemon.get("source_tree_sha256") != target_tree_sha256:
        proof_errors.append("daemon.source_tree_sha256")
    if daemon.get("multi_user") is not False:
        proof_errors.append("daemon.multi_user")

    return (
        LaneState(
            configured=True,
            current_sha=current_sha,
            current_version=current_version,
            current_tag=(
                str(env["last_tag"])
                if isinstance(env.get("last_tag"), str)
                else None
            ),
            dirty=dirty,
            last_ok=not proof_errors,
            acknowledged=acknowledged,
            detail=(
                "exact managed vq-only user install"
                if not proof_errors
                else "required vq-only proof incomplete: "
                + ", ".join(proof_errors)
            ),
        ),
        discovery_error,
        None,
    )


def _scheduler_lane_state(
    admin_status: Mapping[str, Any],
    *,
    host: str,
    program: str,
) -> LaneState:
    deployments = _admin_host(admin_status, host).get("deployments")
    deployment = (
        deployments.get(program)
        if isinstance(deployments, dict)
        else None
    )
    if not isinstance(deployment, dict) or deployment.get("configured") is not True:
        return LaneState(
            configured=False,
            current_sha=None,
            current_version=None,
            current_tag=None,
            dirty=None,
            last_ok=False,
            acknowledged=False,
            detail="no managed scheduler runtime lane",
        )
    last = deployment.get("last")
    if not isinstance(last, dict):
        return LaneState(
            configured=True,
            current_sha=None,
            current_version=None,
            current_tag=None,
            dirty=None,
            last_ok=False,
            acknowledged=False,
            detail="managed scheduler runtime has never deployed",
        )
    return LaneState(
        configured=True,
        current_sha=(
            str(last["actual_sha"])
            if isinstance(last.get("actual_sha"), str)
            else None
        ),
        current_version=None,
        current_tag=(
            str(last["actual_tag"])
            if isinstance(last.get("actual_tag"), str)
            else None
        ),
        dirty=False,
        last_ok=last.get("last_success") is True and last.get("healthy") is True,
        acknowledged=False,
        detail=str(last.get("health_detail") or "managed scheduler runtime"),
        metrics=(
            last["metrics"] if isinstance(last.get("metrics"), dict) else None
        ),
    )


def _helper_lane_state(
    admin_status: Mapping[str, Any],
    doctor: Mapping[str, Any],
    *,
    host: str,
    configured: bool,
) -> LaneState:
    """Canonical scheduler-helper lane state.

    The helper's identity is proven by its canonical LAST OK record plus the
    live provenance probe — never by comparing against the live driver
    checkout. A driver tree sitting ahead of the accepted report pin must
    not make a helper standing exactly at that pin look "not deployed".
    """
    helper_block = _admin_host(admin_status, host).get("helper")
    record = (
        helper_block.get("last") if isinstance(helper_block, dict) else None
    )
    record = record if isinstance(record, dict) else None
    recorded_sha = (
        str(record["actual_sha"]).lower()
        if record is not None
        and isinstance(record.get("actual_sha"), str)
        and len(str(record["actual_sha"])) == 40
        else None
    )
    record_ok = record is not None and record.get("last_success") is True
    live_sha = _helper_sha(doctor, host)
    check = _doctor_checks(doctor, host).get("scheduler_remote_vq")
    timed_out = bool(check is not None and check.get("timed_out") is True)
    # "We did not get an answer", computed once rather than in one branch:
    # a probe that timed out is unavailable whatever else is known, and a
    # missing live SHA beside a real record is the same thing said quietly.
    probe_unavailable = live_sha is None and (timed_out or recorded_sha is not None)
    current_sha = live_sha or recorded_sha
    if live_sha is not None and recorded_sha is not None:
        last_ok = record_ok and live_sha == recorded_sha
        detail = (
            "canonical helper record matches live provenance probe"
            if last_ok
            else "canonical helper record and live provenance probe disagree"
            if record_ok
            else "helper record is not a verified success"
        )
    elif live_sha is not None:
        # Pre-record helper (deployed before canonical helper bookkeeping):
        # only the legacy driver-comparison doctor verdict is available.
        last_ok = bool(check is not None and check.get("ok") is True)
        detail = "scheduler-side vq helper (no canonical record yet)"
    elif recorded_sha is not None:
        last_ok = False
        detail = _helper_probe_unavailable_detail(
            check,
            otherwise="live helper provenance probe unavailable",
        )
    else:
        last_ok = False
        detail = _helper_probe_unavailable_detail(
            check,
            otherwise="scheduler-side vq helper",
        )
    return LaneState(
        configured=configured,
        current_sha=current_sha,
        current_version=None,
        current_tag=None,
        dirty=False,
        last_ok=last_ok,
        acknowledged=False,
        detail=detail,
        metrics=(
            record["metrics"]
            if record is not None and isinstance(record.get("metrics"), dict)
            else None
        ),
        probe_unavailable=probe_unavailable,
    )


def _helper_probe_unavailable_detail(
    check: Mapping[str, Any] | None,
    *,
    otherwise: str,
) -> str:
    """Say why the live helper probe yielded no SHA, naming a timeout as one.

    A timed-out probe and a wrong SHA both leave the lane without live
    evidence, and both keep LAST OK false: the supersede gate wants the
    helper's identity observed in THIS sweep, not inferred from its canonical
    record. They are different findings, though -- ``probe_unavailable``
    carries that beside the verdict, and this is the same distinction in the
    operator-facing line. A wrong SHA is a fact about the host; a timeout is
    a fact about the probe budget, and its remedy is a retry and then a wider
    ``[fleet] check_timeout_seconds``, not a redeploy.
    """
    if check is None or check.get("timed_out") is not True:
        return otherwise
    qualifiers: list[str] = []
    subprobe = check.get("subprobe")
    if isinstance(subprobe, str) and subprobe:
        qualifiers.append(subprobe)
    elapsed = check.get("elapsed_seconds")
    limit = check.get("timeout_seconds")
    if type(elapsed) in (int, float) and type(limit) in (int, float):
        qualifiers.append(f"after {elapsed:.3g}s of a {limit:.3g}s check budget")
    detail = "live helper provenance probe timed out"
    if qualifiers:
        detail += f" ({' '.join(qualifiers)})"
    return (
        f"{detail}; retry, and if it persists widen "
        "`[fleet] check_timeout_seconds`"
    )


def _doctor_host(
    doctor: Mapping[str, Any],
    host: str,
) -> Mapping[str, Any]:
    payload = doctor.get(host)
    return payload if isinstance(payload, dict) else {}


def _doctor_checks(
    doctor: Mapping[str, Any],
    host: str,
) -> dict[str, Mapping[str, Any]]:
    checks = _doctor_host(doctor, host).get("checks")
    if not isinstance(checks, list):
        return {}
    return {
        str(check.get("name")): check
        for check in checks
        if isinstance(check, dict) and isinstance(check.get("name"), str)
    }


def _helper_sha(doctor: Mapping[str, Any], host: str) -> str | None:
    check = _doctor_checks(doctor, host).get("scheduler_remote_vq")
    if check is None:
        return None
    direct = check.get("source_sha")
    if isinstance(direct, str) and len(direct) == 40:
        return direct
    message = check.get("message")
    if not isinstance(message, str):
        return None
    match = _HELPER_SHA.search(message)
    return match.group(1) if match is not None else None


def _positive_int(value: object) -> bool:
    return type(value) is int and value > 0


def _root_python_executable(value: object) -> bool:
    if not isinstance(value, str):
        return False
    path = PurePosixPath(value)
    return (
        path.is_absolute()
        and path.parent == PurePosixPath("/opt/vq/venv/bin")
        and _ROOT_PYTHON_NAME.fullmatch(path.name) is not None
    )


def _active_system_service(value: object) -> bool:
    return (
        isinstance(value, Mapping)
        and value.get("status") == "ok"
        and value.get("active_state") == "active"
        and _positive_int(value.get("main_pid"))
    )


def _service_proves_not_active(value: object) -> bool:
    if not isinstance(value, Mapping):
        return False
    if value.get("status") == "unsupported":
        error = value.get("error")
        return (
            isinstance(error, str)
            and bool(error)
            and all(
                value.get(key) is None
                for key in (
                    "active_state",
                    "argv",
                    "exec_start",
                    "executable",
                    "id",
                    "load_state",
                    "main_pid",
                    "source",
                    "sub_state",
                    "user",
                )
            )
        )
    return (
        value.get("status") == "ok"
        and value.get("error") is None
        and value.get("source") in ("/usr/bin/systemctl", "/bin/systemctl")
        and value.get("id") == "vq-daemon-multi-user.service"
        and value.get("load_state") in ("loaded", "not-found")
        and value.get("active_state") == "inactive"
        and value.get("sub_state") == "dead"
        and type(value.get("main_pid")) is int
        and value.get("main_pid") == 0
    )


def _root_daemon_identity_errors(
    check: Mapping[str, Any],
    *,
    target_sha: str,
    target_version: str | None,
    target_tree_sha256: str | None,
) -> list[str]:
    """Return stable comparison codes for the exact accepted root daemon."""
    errors: list[str] = []
    system = check.get("system_multi_user")
    system = system if isinstance(system, Mapping) else {}
    process = check.get("process_identity")
    process = process if isinstance(process, Mapping) else {}
    service = check.get("system_service")
    service = service if isinstance(service, Mapping) else {}

    if _SOURCE_SHA.fullmatch(target_sha) is None:
        errors.append("target.source_sha.invalid")
    if not isinstance(target_version, str) or not target_version:
        errors.append("target.version.missing")
    if target_tree_sha256 is None:
        errors.append("target.source_tree_sha256.missing")
    elif _SOURCE_TREE_SHA256.fullmatch(target_tree_sha256) is None:
        errors.append("target.source_tree_sha256.invalid")

    if system.get("source") != str(config.SYSTEM_CONFIG_PATH):
        errors.append("system_multi_user.source.mismatch")
    if system.get("status") != "enabled":
        errors.append("system_multi_user.status.mismatch")
    if system.get("enabled") is not True:
        errors.append("system_multi_user.enabled.mismatch")
    if system.get("error") is not None:
        errors.append("system_multi_user.error.present")

    if check.get("ok") is not True:
        errors.append("daemon.status.unavailable")
    if check.get("version") != target_version:
        errors.append("daemon.version.mismatch")
    if check.get("source_sha") != target_sha:
        errors.append("daemon.source_sha.mismatch")
    if check.get("source_tree_sha256") != target_tree_sha256:
        errors.append("daemon.source_tree_sha256.mismatch")
    if check.get("multi_user") is not True:
        errors.append("daemon.multi_user.mismatch")
    if check.get("socket_path") != _ROOT_DAEMON_SOCKET:
        errors.append("daemon.socket_path.mismatch")

    process_status = process.get("status")
    if process_status != "ok":
        suffix = (
            process_status
            if isinstance(process_status, str)
            and process_status in {"unknown", "unsupported"}
            else "unavailable"
        )
        errors.append(f"process_identity.status.{suffix}")
    if process.get("error") is not None:
        errors.append("process_identity.error.present")
    process_pid = process.get("pid")
    if not _positive_int(process_pid):
        errors.append("process_identity.pid.invalid")
    if process.get("euid") != 0 or type(process.get("euid")) is not int:
        errors.append("process_identity.euid.mismatch")
    if not _root_python_executable(process.get("python_executable")):
        errors.append("process_identity.python_executable.mismatch")
    if process.get("argv") != _ROOT_DAEMON_ARGV:
        errors.append("process_identity.argv.mismatch")
    if process.get("version") != target_version:
        errors.append("process_identity.version.mismatch")
    if process.get("source_sha") != target_sha:
        errors.append("process_identity.source_sha.mismatch")
    if process.get("source_tree_sha256") != target_tree_sha256:
        errors.append("process_identity.source_tree_sha256.mismatch")
    if process.get("multi_user") is not True:
        errors.append("process_identity.multi_user.mismatch")
    if process.get("socket_path") != _ROOT_DAEMON_SOCKET:
        errors.append("process_identity.socket_path.mismatch")

    if service.get("status") != "ok":
        errors.append("system_service.status.unavailable")
    if service.get("error") is not None:
        errors.append("system_service.error.present")
    if service.get("source") not in ("/usr/bin/systemctl", "/bin/systemctl"):
        errors.append("system_service.source.mismatch")
    if service.get("id") != "vq-daemon-multi-user.service":
        errors.append("system_service.id.mismatch")
    if service.get("load_state") != "loaded":
        errors.append("system_service.load_state.mismatch")
    if service.get("active_state") != "active":
        errors.append("system_service.active_state.mismatch")
    if service.get("sub_state") != "running":
        errors.append("system_service.sub_state.mismatch")
    if service.get("user") != "root":
        errors.append("system_service.user.mismatch")
    service_pid = service.get("main_pid")
    if not _positive_int(service_pid):
        errors.append("system_service.main_pid.invalid")
    elif _positive_int(process_pid) and service_pid != process_pid:
        errors.append("system_service.main_pid.mismatch")
    exec_start = service.get("exec_start")
    if not (
        isinstance(exec_start, str)
        and bool(exec_start)
        and all(character.isprintable() for character in exec_start)
    ):
        errors.append("system_service.exec_start.invalid")
    if service.get("executable") != _ROOT_VQ_EXECUTABLE:
        errors.append("system_service.executable.mismatch")
    if service.get("argv") != _ROOT_DAEMON_ARGV:
        errors.append("system_service.argv.mismatch")
    return errors


def _root_daemon_provenance_lane(
    doctor: Mapping[str, Any],
    *,
    host: str,
    target_sha: str,
    target_version: str | None,
    target_tree_sha256: str | None,
) -> ProvenanceLane:
    """Inventory one possible system multi-user daemon without mutating it.

    Applicability comes only from the target's strict verbose-ping evidence.
    A pre-R1 client, invalid system config, wrong socket, or failed ping is
    unknown and therefore deferred. Explicit absence/disablement plus complete
    negative service evidence is the sole not-applicable case. An enabled
    daemon converges only when its atomic process provenance and fixed systemd
    identity both match the immutable accepted package. Privileged refresh is
    deliberately left for a later, separately reviewed milestone.
    """
    check = _doctor_checks(doctor, host).get("daemon_rpc") or {}
    system = check.get("system_multi_user")
    system = system if isinstance(system, Mapping) else {}
    authoritative = system.get("source") == str(config.SYSTEM_CONFIG_PATH)
    status = system.get("status")
    enabled = system.get("enabled")
    runtime_multi_user = (
        check.get("ok") is True and check.get("multi_user") is True
    )
    service = check.get("system_service")
    service_active = _active_system_service(service)
    errors = _root_daemon_identity_errors(
        check,
        target_sha=target_sha,
        target_version=target_version,
        target_tree_sha256=target_tree_sha256,
    )
    if (
        authoritative
        and enabled is False
        and status in ("absent", "disabled")
        and not runtime_multi_user
        and not service_active
        and _service_proves_not_active(service)
    ):
        applicable: bool | None = False
        decision: ProvenanceDecision = "skip"
        reason = (
            "target /etc/vq/config.toml confirms system multi-user mode is "
            "not enabled; root daemon is not applicable"
        )
        comparison = {"status": "not-applicable", "errors": []}
    elif authoritative and enabled is True and status == "enabled":
        applicable = True
        if errors:
            decision = "defer"
            reason = (
                "system multi-user root daemon does not match the exact "
                "accepted identity: " + ", ".join(errors)
            )
            comparison = {"status": "mismatch", "errors": errors}
        else:
            decision = "skip"
            reason = "exact accepted root daemon is already running"
            comparison = {"status": "exact", "errors": []}
    elif service_active:
        applicable = True
        decision = "defer"
        reason = (
            "system service is active, but target /etc/vq/config.toml does "
            "not authoritatively confirm enabled multi-user mode; root "
            "daemon provenance requires reconciliation"
        )
        comparison = {"status": "mismatch", "errors": errors}
    elif runtime_multi_user:
        applicable = True
        decision = "defer"
        reason = (
            "daemon RPC reports a live multi-user daemon, but target "
            "/etc/vq/config.toml does not authoritatively confirm that mode; "
            "root daemon provenance requires reconciliation"
        )
        comparison = {"status": "mismatch", "errors": errors}
    else:
        applicable = None
        decision = "defer"
        detail = system.get("error")
        suffix = f": {' '.join(str(detail).split())}" if detail else ""
        reason = (
            "target /etc/vq/config.toml multi-user applicability could not "
            f"be verified; root daemon provenance remains unknown{suffix}"
        )
        comparison = {"status": "unknown", "errors": errors}
    evidence = {
        key: check.get(key)
        for key in (
            "ok",
            "message",
            "version",
            "source_sha",
            "source_tree_sha256",
            "multi_user",
            "process_identity",
            "socket_path",
            "system_service",
            "system_multi_user",
        )
    }
    evidence["comparison"] = comparison
    current_sha = check.get("source_sha")
    current_version = check.get("version")
    current_tree_sha256 = check.get("source_tree_sha256")
    return ProvenanceLane(
        id=f"root-daemon:{host}:vibeqc-queue",
        host=host,
        component="root-vq-daemon",
        kind="root-daemon",
        target_sha=target_sha,
        target_version=target_version,
        target_source_tree_sha256=target_tree_sha256,
        current_sha=(
            str(current_sha)
            if isinstance(current_sha, str) and len(current_sha) == 40
            else None
        ),
        current_version=(
            str(current_version) if isinstance(current_version, str) else None
        ),
        current_source_tree_sha256=(
            str(current_tree_sha256)
            if isinstance(current_tree_sha256, str)
            and _SOURCE_TREE_SHA256.fullmatch(current_tree_sha256) is not None
            else None
        ),
        decision=decision,
        reason=reason,
        applicable=applicable,
        evidence=evidence,
    )


def _host_hold_reason(doctor: Mapping[str, Any], host: str) -> str | None:
    payload = _doctor_host(doctor, host)
    checks = _doctor_checks(doctor, host)
    admin_down = checks.get("admin_down")
    if admin_down is not None:
        return str(admin_down.get("message") or "administratively down")
    liveness = checks.get("scheduler_liveness")
    if liveness is not None and liveness.get("ok") is False:
        dispatch = liveness.get("scheduler_dispatch")
        if isinstance(dispatch, dict) and dispatch.get("dispatching_new_jobs") is False:
            return str(liveness.get("message") or "external scheduler hold")
    if payload and payload.get("ok") is False:
        nonrecoverable = [
            str(check.get("name"))
            for check in checks.values()
            if check.get("ok") is False
            and check.get("name") not in {"scheduler_remote_vq"}
        ]
        if nonrecoverable:
            return "doctor failed: " + ", ".join(sorted(nonrecoverable))
    return None


def _probe_error(admin_status: Mapping[str, Any], host: str) -> str | None:
    """The read-only status sweep's own failure for ``host``, if it failed.

    ``vq admin status --all --json`` isolates per-host failures as
    ``{"error": "..."}`` so one unreachable host cannot break the sweep. Every
    lane-state reader below looks for ``envs``/``deployments`` and finds
    neither, so without this an unreachable host renders as "no managed lane"
    — indistinguishable from a host that genuinely has none, and silently
    skipped by the planner. build-host did exactly that on 2026-07-26 (SSH first hop
    dead, ``vq admin status build-host`` exit 255): all four of its lanes planned as
    ``skip`` and ``--dry-run`` exited 0 without naming it.
    """
    payload = _admin_host(admin_status, host)
    error = payload.get("error")
    if isinstance(error, str) and error.strip():
        return " ".join(error.split())
    return None


def _marker_reason(
    admin_status: Mapping[str, Any],
    host: str,
    *,
    requested_envs: list[str],
    requested_host: str,
) -> str | None:
    """Why one action defers behind an overlapping admin-update marker.

    ``host`` is where the marker was *observed*, which is not always where it
    was taken. A scheduler host has no daemon of its own, so
    ``format_scheduler_runtime_status_json`` embeds the **driver's** marker in
    its payload; a local-scheduler host delegates remotely and returns its own.
    Attributing every marker to the observed host produced
    "active update marker on pbs-cluster (envs=scheduler-runtime:slurm-cluster:vibeqc-dev)"
    on 2026-07-26 — a slurm-cluster update, reported against pbs-cluster, which reads exactly
    like a scope leak and cost real time to tell apart from one.

    Marker storage is driver-global, but marker *leases* are scope-aware.
    Independent host/component actions continue; helper/runtime overlap and
    same-resource updates still fail closed.
    """
    payload = _admin_host(admin_status, host)
    raw_markers = payload.get("markers")
    if isinstance(raw_markers, list):
        markers = [item for item in raw_markers if isinstance(item, dict)]
    else:
        marker = payload.get("marker")
        markers = [marker] if isinstance(marker, dict) else []
    marker = next(
        (
            item
            for item in markers
            if item.get("readable") is not False
            and admin_module.admin_update_scopes_conflict(
                item.get("envs") if isinstance(item.get("envs"), list) else [],
                str(item.get("host") or ""),
                requested_envs,
                requested_host,
            )
        ),
        None,
    )
    unreadable = next(
        (item for item in markers if item.get("readable") is False),
        None,
    )
    if marker is None:
        marker = unreadable
    if marker is None:
        return None
    envs = marker.get("envs")
    scope = ", ".join(str(item) for item in envs) if isinstance(envs, list) else "all"
    holder = marker.get("host")
    where = (
        f"on {host}"
        if not isinstance(holder, str) or not holder or holder == host
        else f"held by {holder}"
    )
    # A stale marker defers too — a marker file deliberately blocks until a
    # human acknowledges it — but the operator needs to know which it is,
    # because the recovery differs (wait vs. clear).
    stale = marker.get("stale_reason")
    suffix = (
        f"; marker is stale ({' '.join(str(stale).split())})"
        if isinstance(stale, str) and stale.strip()
        else ""
    )
    return f"active update marker {where} (envs={scope}){suffix}"


def resolve_topology(
    cfg: config.Config,
    admin_status: Mapping[str, Any],
) -> dict[str, ResolvedHost]:
    """Resolve explicit roles and safe legacy ``auto`` migrations."""
    resolved: dict[str, ResolvedHost] = {}
    scheduler_candidates: dict[str, list[str]] = {}
    for name, host in cfg.hosts.items():
        if (
            host.scheduler != "local"
            and (
                host.scheduler_update_command is not None
                or bool(host.scheduler_runtime_deployments)
            )
        ):
            scheduler_candidates.setdefault(host.ssh, []).append(name)

    scheduler_drivers = {
        host.scheduler_driver
        for host in cfg.hosts.values()
        if host.scheduler != "local" and host.scheduler_driver is not None
    }
    for name, host in cfg.hosts.items():
        if host.fleet_role != "auto":
            resolved[name] = ResolvedHost(
                name=name,
                role=host.fleet_role,
                canonical_host=host.fleet_canonical_host,
                reason="explicit config",
            )
            continue
        if host.scheduler != "local":
            canonical = scheduler_candidates.get(host.ssh, [])
            if name in canonical and len(canonical) == 1:
                resolved[name] = ResolvedHost(
                    name=name,
                    role="managed",
                    canonical_host=None,
                    reason="auto: owns scheduler helper/runtime deployment config",
                )
            elif len(canonical) == 1:
                resolved[name] = ResolvedHost(
                    name=name,
                    role="alias",
                    canonical_host=canonical[0],
                    reason=f"auto: shares scheduler endpoint with {canonical[0]}",
                )
            else:
                resolved[name] = ResolvedHost(
                    name=name,
                    role="unresolved",
                    canonical_host=None,
                    reason="auto: scheduler alias/canonical target is ambiguous",
                )
            continue
        envs = _admin_host(admin_status, name).get("envs")
        managed_env = (
            any(
                isinstance(item, dict)
                and item.get("name") in PROGRAM_PINS
                and item.get("error") != "git_dir not a directory"
                for item in envs
            )
            if isinstance(envs, list)
            else False
        )
        if name in scheduler_drivers or managed_env:
            resolved[name] = ResolvedHost(
                name=name,
                role="managed",
                canonical_host=None,
                reason="auto: driver or managed venv lane present",
            )
        else:
            resolved[name] = ResolvedHost(
                name=name,
                role="unresolved",
                canonical_host=None,
                reason="auto: no managed lane found; declare vq-only/excluded",
            )
    return resolved


def derive_driver(
    cfg: config.Config,
    topology: Mapping[str, ResolvedHost],
) -> str:
    drivers = {
        host.scheduler_driver
        for name, host in cfg.hosts.items()
        if host.scheduler != "local"
        and topology[name].role == "managed"
        and host.scheduler_driver is not None
    }
    if len(drivers) != 1:
        raise FleetRolloutError(
            "fleet rollout requires exactly one scheduler_driver; "
            f"resolved {sorted(drivers)}"
        )
    driver = next(iter(drivers))
    if driver not in cfg.hosts:
        raise FleetRolloutError(f"scheduler_driver {driver!r} is not configured")
    if topology[driver].role not in {"managed", "vq-only"}:
        raise FleetRolloutError(
            f"scheduler_driver {driver!r} has fleet role "
            f"{topology[driver].role!r}"
        )
    return driver


def _scheduler_hold_target_map(
    cfg: config.Config,
    topology: Mapping[str, ResolvedHost],
    *,
    ordered: Sequence[str],
    scheduler_hosts: Sequence[str],
) -> dict[str, tuple[tuple[str, str], ...]]:
    """Bind each canonical scheduler lane to every exact dispatch alias.

    Alias jobs retain the config key the submitter named, and daemon drains
    compare that exact string.  A canonical-only lease therefore cannot cover
    an actionless queue/campaign alias.  Capture each target's own daemon
    control here; explicit aliases are allowed to name a different driver.

    Excluded and unresolved hosts are intentionally not inferred.  They carry
    no trustworthy canonical mapping, while non-local explicit and safely
    auto-resolved ``alias`` roles do.
    """
    alias_action_hosts: dict[str, str | None] = {}
    for name, resolved in topology.items():
        if resolved.role != "alias":
            continue
        trail: list[str] = []
        cursor = name
        while topology[cursor].role == "alias":
            if cursor in trail:
                cycle = trail[trail.index(cursor):] + [cursor]
                raise FleetRolloutError(
                    "scheduler alias cycle in resolved topology: "
                    + " -> ".join(cycle)
                )
            trail.append(cursor)
            parent = topology[cursor].canonical_host
            if not isinstance(parent, str) or parent not in topology:
                raise FleetRolloutError(
                    f"resolved scheduler alias {cursor!r} has no configured "
                    "canonical host"
                )
            cursor = parent
        terminal = topology[cursor]
        action_host = cursor if terminal.role == "managed" else None
        for alias in trail:
            alias_action_hosts[alias] = action_host

    result: dict[str, tuple[tuple[str, str], ...]] = {}
    for canonical in scheduler_hosts:
        targets = [
            canonical,
            *(
                name
                for name in ordered
                if name != canonical
                and topology[name].role == "alias"
                and alias_action_hosts[name] == canonical
                and cfg.hosts[name].scheduler != "local"
            ),
        ]
        bound: list[tuple[str, str]] = []
        for target in targets:
            control = cfg.hosts[target].scheduler_driver
            if not isinstance(control, str) or not control:
                raise FleetRolloutError(
                    f"scheduler hold target {target!r} has no control driver"
                )
            bound.append((target, control))
        result[canonical] = tuple(bound)
    return result


def _relation(
    current_sha: str | None,
    pin: fleet_release.FleetPin,
    *,
    ancestry: Ancestry,
) -> str:
    if current_sha is None:
        return "missing"
    if current_sha == pin.sha:
        return "equal"
    current_before_target = ancestry(pin.name, current_sha, pin.sha)
    if current_before_target is True:
        return "behind"
    target_before_current = ancestry(pin.name, pin.sha, current_sha)
    if target_before_current is True:
        return "ahead"
    if current_before_target is None or target_before_current is None:
        return "unknown"
    return "diverged"


def _preserve_descendant_repair_argv(
    argv: Sequence[str],
    *,
    pin: fleet_release.FleetPin,
    lane: LaneState,
) -> list[str]:
    """Retarget a repair from the accepted pin to the proven descendant.

    A no-downgrade lane can still need work when its checkout and installed
    provenance disagree. Re-running the report pin would repair by silently
    moving a chemistry checkout backwards. Instead, rebuild the exact live SHA
    whose descent from the pin the planner just proved. Release lanes retain
    their live tag when one was recorded; every other lane needs only the SHA.
    """
    current_sha = lane.current_sha
    if current_sha is None:
        raise FleetRolloutError(
            "cannot repair an ahead deployment without its current SHA"
        )
    planned = list(argv)
    old_flags = list(pin.deploy_flags)
    start = next(
        (
            index
            for index in range(len(planned) - len(old_flags) + 1)
            if planned[index:index + len(old_flags)] == old_flags
        ),
        None,
    )
    if start is None:
        raise FleetRolloutError(
            "internal rollout plan is missing the accepted pin deploy flags"
        )
    repair_flags: list[str] = []
    if pin.name == "release" and lane.current_tag is not None:
        repair_flags.extend(["--tag", lane.current_tag])
    repair_flags.extend(["--expected-sha", current_sha])
    return (
        planned[:start]
        + repair_flags
        + planned[start + len(old_flags):]
    )


def _lane_decision(
    lane: LaneState,
    *,
    pin: fleet_release.FleetPin,
    ancestry: Ancestry,
    ahead_is_release_drift: bool = False,
    redeploy_hint: str | None = None,
) -> tuple[Decision, str]:
    if not lane.configured:
        return "skip", lane.detail
    if lane.dirty is True:
        return "block", "managed checkout/runtime is dirty"
    if pin.name == "release":
        current_version = fleet_release.semver_from_text(
            lane.current_tag or lane.current_version
        )
        target_version = fleet_release.semver_from_text(pin.version)
        if (
            current_version is not None
            and target_version is not None
            and current_version > target_version
            and lane.last_ok
        ):
            return "skip", "newer release already deployed; no downgrade"
        if (
            current_version == target_version
            and lane.current_sha is not None
            and lane.current_sha != pin.sha
        ):
            return "block", "same release version has a different SHA"
    relation = _relation(lane.current_sha, pin, ancestry=ancestry)
    if relation == "equal":
        if lane.last_ok:
            return "skip", "already at target with LAST OK=true"
        if lane.acknowledged:
            return "update", "target was human-acknowledged, not verified"
        return "update", "target identity present but LAST OK is not true"
    if relation == "ahead":
        if ahead_is_release_drift:
            # The accepted report is the sole source of deployed vq
            # identity. A vq lane ahead of the pin is release drift, and
            # "no downgrade" must never silently coexist with staging
            # other lanes: fail closed with the exact recovery.
            current = (lane.current_sha or "")[:12]
            hint = f", or redeploy the pin with `{redeploy_hint}`" if (
                redeploy_hint
            ) else ""
            return (
                "block",
                f"release drift: deployed vq {current} is ahead of the "
                f"accepted report pin {pin.sha[:12]}; cut the next release "
                "so the accepted report pins the deployed vq"
                f"{hint}",
            )
        if lane.last_ok:
            return "skip", "newer descendant already deployed; no downgrade"
        if lane.acknowledged:
            return (
                "update",
                "newer descendant was human-acknowledged, not verified; "
                "repair its current deployment without downgrade",
            )
        return (
            "update",
            "newer descendant lacks verified deployment provenance; repair "
            "its current deployment without downgrade",
        )
    if relation in {"unknown", "diverged"}:
        return "block", f"cannot prove safe ancestry ({relation})"
    return "update", "target is newer" if relation == "behind" else "not deployed"


def _before(lane: LaneState) -> dict[str, Any]:
    return asdict(lane)


def _skip_proves_live_success(action: RolloutAction) -> bool:
    """Whether a fresh skip verifies a previously successful action.

    Exact-pin and healthy-ahead states are both converged. Treating only the
    former as journal verification would erase an ahead repair's actual target
    identity on the first resumable re-run.
    """
    return action.decision == "skip" and (
        action.reason == "already at target with LAST OK=true"
        or action.reason.endswith("already deployed; no downgrade")
    )


def _final_action_proves_scheduler_parity(
    expected: RolloutAction,
    final: RolloutAction | None,
) -> bool:
    """Bind a final healthy lane to the action that was actually attempted."""
    if (
        final is None
        or final.host != expected.host
        or final.phase != expected.phase
        or final.program != expected.program
        or not _skip_proves_live_success(final)
    ):
        return False
    if (
        final.target_sha == expected.target_sha
        and final.target_version == expected.target_version
        and final.target_tag == expected.target_tag
    ):
        return True
    # An ahead repair deliberately executes the live descendant while the
    # final planner continues to describe the accepted report floor. Permit
    # that one target-field transition only when the fresh lane snapshot
    # proves the exact descendant SHA that the initial action rebuilt.
    return (
        final.reason.endswith("already deployed; no downgrade")
        and final.before.get("current_sha") == expected.target_sha
    )


def _action(
    *,
    action_id: str,
    phase: Phase,
    host: str,
    program: str,
    pin: fleet_release.FleetPin,
    argv: Sequence[str],
    lane: LaneState,
    ancestry: Ancestry,
    hold_reason: str | None,
    marker_reason: str | None,
    probe_error: str | None = None,
    ahead_is_release_drift: bool = False,
    redeploy_hint: str | None = None,
) -> RolloutAction:
    action_argv = list(argv)
    action_target_sha = pin.sha
    action_target_version: str | None = pin.version
    action_target_tag = pin.tag
    if probe_error is not None:
        # Checked before the lane decision: a failed probe yields an empty
        # payload, which _lane_decision would read as "not configured" and
        # skip. Defer rather than block — a single unreachable host must not
        # stop the independent hosts (the serial rollout order contract), and
        # the next rollout retries it from a fresh live snapshot.
        decision: Decision = "defer"
        reason = f"host status probe failed: {probe_error}"
    else:
        decision, reason = _lane_decision(
            lane,
            pin=pin,
            ancestry=ancestry,
            ahead_is_release_drift=ahead_is_release_drift,
            redeploy_hint=redeploy_hint,
        )
        if decision == "update" and hold_reason is not None:
            decision, reason = "defer", hold_reason
        if decision == "update" and marker_reason is not None:
            decision, reason = "defer", marker_reason
        if (
            decision == "update"
            and not ahead_is_release_drift
            and _relation(lane.current_sha, pin, ancestry=ancestry) == "ahead"
        ):
            action_argv = _preserve_descendant_repair_argv(
                argv,
                pin=pin,
                lane=lane,
            )
            assert lane.current_sha is not None
            action_target_sha = lane.current_sha
            if pin.name == "release":
                action_target_tag = lane.current_tag
                action_target_version = (
                    lane.current_tag.removeprefix("v")
                    if lane.current_tag is not None
                    else lane.current_version
                )
            else:
                # The live import version may describe the stale installed
                # package that caused this repair, not the descendant checkout
                # being rebuilt. Keep the unknown explicit until verification.
                action_target_tag = None
                action_target_version = None
            current = lane.current_sha[:12]
            provenance = (
                "was human-acknowledged, not verified"
                if lane.acknowledged
                else "lacks verified deployment provenance"
            )
            reason = (
                f"newer descendant {current} {provenance}; repair that "
                "descendant without downgrade"
            )
    return RolloutAction(
        id=action_id,
        phase=phase,
        host=host,
        program=program,
        pin_name=pin.name,
        # The accepted floor remains independently auditable in
        # plan.report["pins"][pin_name]. Ahead repairs target the proven live
        # descendant instead, so these fields must describe the argv that will
        # actually execute rather than repeat the older accepted pin.
        target_sha=action_target_sha,
        target_version=action_target_version,
        target_tag=action_target_tag,
        argv=action_argv,
        decision=decision,
        reason=reason,
        before=_before(lane),
    )


def build_plan(
    cfg: config.Config,
    report: fleet_release.FleetReleaseReport,
    *,
    admin_status: Mapping[str, Any],
    programs: Mapping[str, Any],
    doctor: Mapping[str, Any],
    ancestry: Ancestry,
    target_vq_tree_sha256: str | None = None,
) -> RolloutPlan:
    """Create the complete serial plan without changing fleet state."""
    topology = resolve_topology(cfg, admin_status)
    driver = derive_driver(cfg, topology)
    topology_errors = [
        f"{name}: {resolved.reason}"
        for name, resolved in topology.items()
        if resolved.role == "unresolved"
    ]
    topology_errors.extend(
        f"{name}: vq-only role requires scheduler=local for the managed "
        "user-vq update lane"
        for name, resolved in topology.items()
        if resolved.role == "vq-only" and cfg.hosts[name].scheduler != "local"
    )
    ordered = fleet_release.ordered_hosts(
        list(cfg.hosts),
        cfg.fleet_rollout_order,
    )
    canonical = [
        name
        for name in ordered
        if topology[name].role == "managed"
    ]
    actions: list[RolloutAction] = []

    driver_pin = report.pins["vq"]
    driver_required = topology[driver].role == "vq-only"
    if driver_required:
        driver_lane, driver_probe, driver_configuration_error = (
            _vq_user_lane_state(
                admin_status,
                programs,
                doctor,
                host=driver,
                target_sha=driver_pin.sha,
                target_version=driver_pin.version,
                target_tree_sha256=target_vq_tree_sha256,
            )
        )
    else:
        driver_lane = _local_lane_state(
            admin_status,
            programs,
            host=driver,
            program="vibeqc-queue",
        )
        driver_probe = _probe_error(admin_status, driver)
        driver_configuration_error = None
    driver_action = _action(
        action_id=f"driver:{driver}:vibeqc-queue",
        phase="driver",
        host=driver,
        program="vibeqc-queue",
        pin=driver_pin,
        argv=[
            "admin",
            "update",
            "vibeqc-queue",
            driver,
            *driver_pin.deploy_flags,
        ],
        lane=driver_lane,
        ancestry=ancestry,
        hold_reason=_host_hold_reason(doctor, driver),
        marker_reason=_marker_reason(
            admin_status,
            driver,
            requested_envs=["vibeqc-queue"],
            requested_host=driver,
        ),
        probe_error=driver_probe,
        ahead_is_release_drift=True,
        redeploy_hint=(
            f"vq admin update vibeqc-queue {driver} "
            f"--expected-sha {driver_pin.sha}"
        ),
    )
    if driver_required:
        driver_action.before["required"] = True
        if driver_configuration_error is not None and driver_probe is None:
            driver_action.decision = "block"
            driver_action.reason = driver_configuration_error
    actions.append(driver_action)

    # A vq-only coordinator is a first-class user-vq target, not a chemistry
    # host. Update it immediately after the driver re-entry and before helper
    # or runtime work, using the same report-pinned remote admin primitive as
    # managed local vq lanes. A missing target-side program registration is a
    # deterministic configuration block; a failed/torn discovery sweep defers.
    vq_only_hosts = [
        name
        for name in ordered
        if topology[name].role == "vq-only"
        and cfg.hosts[name].scheduler == "local"
        and name != driver
    ]
    for host in vq_only_hosts:
        lane, discovery_error, configuration_error = _vq_user_lane_state(
            admin_status,
            programs,
            doctor,
            host=host,
            target_sha=driver_pin.sha,
            target_version=driver_pin.version,
            target_tree_sha256=target_vq_tree_sha256,
        )
        action = _action(
            action_id=f"local-runtime:{host}:vibeqc-queue",
            phase="local-runtime",
            host=host,
            program="vibeqc-queue",
            pin=driver_pin,
            argv=[
                "admin",
                "update",
                "vibeqc-queue",
                host,
                *driver_pin.deploy_flags,
            ],
            lane=lane,
            ancestry=ancestry,
            hold_reason=_host_hold_reason(doctor, host),
            marker_reason=_marker_reason(
                admin_status,
                host,
                requested_envs=["vibeqc-queue"],
                requested_host=host,
            ),
            probe_error=discovery_error,
            ahead_is_release_drift=True,
            redeploy_hint=(
                f"vq admin update vibeqc-queue {host} "
                f"--expected-sha {driver_pin.sha}"
            ),
        )
        action.before["required"] = True
        if configuration_error is not None and discovery_error is None:
            action.decision = "block"
            action.reason = configuration_error
        actions.append(action)

    scheduler_hosts = [
        name for name in canonical if cfg.hosts[name].scheduler != "local"
    ]
    scheduler_hold_targets = _scheduler_hold_target_map(
        cfg,
        topology,
        ordered=ordered,
        scheduler_hosts=scheduler_hosts,
    )
    for host in scheduler_hosts:
        helper_lane = _helper_lane_state(
            admin_status,
            doctor,
            host=host,
            configured=cfg.hosts[host].scheduler_update_command is not None,
        )
        actions.append(
            _action(
                action_id=f"helper:{host}",
                phase="helper",
                host=host,
                program="vibeqc-queue",
                pin=driver_pin,
                argv=[
                    "admin",
                    "update",
                    host,
                    # The helper is staged from the accepted report's exact
                    # vq pin — never from the live driver checkout.
                    "--expected-sha",
                    driver_pin.sha,
                    "--drain-wait",
                    DEFAULT_DRAIN_WAIT,
                ],
                lane=helper_lane,
                ancestry=ancestry,
                hold_reason=_host_hold_reason(doctor, host),
                marker_reason=_marker_reason(
                    admin_status,
                    host,
                    requested_envs=[f"scheduler:{host}"],
                    requested_host=host,
                ),
                probe_error=_probe_error(admin_status, host),
                ahead_is_release_drift=True,
                redeploy_hint=(
                    f"vq admin update {host} --expected-sha {driver_pin.sha}"
                ),
            )
        )

    for host in scheduler_hosts:
        hold = _host_hold_reason(doctor, host)
        probe = _probe_error(admin_status, host)
        for program in SCHEDULER_PROGRAM_ORDER:
            marker = _marker_reason(
                admin_status,
                host,
                requested_envs=[f"scheduler-runtime:{host}:{program}"],
                requested_host=host,
            )
            pin = report.pins[PROGRAM_PINS[program]]
            lane = _scheduler_lane_state(
                admin_status,
                host=host,
                program=program,
            )
            actions.append(
                _action(
                    action_id=f"scheduler-runtime:{host}:{program}",
                    phase="scheduler-runtime",
                    host=host,
                    program=program,
                    pin=pin,
                    argv=[
                        "admin",
                        "update",
                        program,
                        host,
                        *pin.deploy_flags,
                        "--drain-wait",
                        DEFAULT_DRAIN_WAIT,
                    ],
                    lane=lane,
                    ancestry=ancestry,
                    hold_reason=hold,
                    marker_reason=marker,
                    probe_error=probe,
                )
            )

    local_hosts = [
        name for name in canonical if cfg.hosts[name].scheduler == "local"
    ]
    for host in local_hosts:
        hold = _host_hold_reason(doctor, host)
        probe = _probe_error(admin_status, host)
        for program in LOCAL_PROGRAM_ORDER:
            if host == driver and program == "vibeqc-queue":
                continue
            pin = report.pins[PROGRAM_PINS[program]]
            marker = _marker_reason(
                admin_status,
                host,
                requested_envs=[program],
                requested_host=host,
            )
            lane = _local_lane_state(
                admin_status,
                programs,
                host=host,
                program=program,
            )
            actions.append(
                _action(
                    action_id=f"local-runtime:{host}:{program}",
                    phase="local-runtime",
                    host=host,
                    program=program,
                    pin=pin,
                    argv=[
                        "admin",
                        "update",
                        program,
                        host,
                        *pin.deploy_flags,
                    ],
                    lane=lane,
                    ancestry=ancestry,
                    hold_reason=hold,
                    marker_reason=marker,
                    probe_error=probe,
                    # vq identity everywhere comes from the accepted report;
                    # chemistry lanes keep the plain no-downgrade skip.
                    ahead_is_release_drift=(program == "vibeqc-queue"),
                    redeploy_hint=(
                        f"vq admin update vibeqc-queue {host} "
                        f"--expected-sha {pin.sha}"
                        if program == "vibeqc-queue"
                        else None
                    ),
                )
            )

    provenance_lanes = [
        _root_daemon_provenance_lane(
            doctor,
            host=host,
            target_sha=driver_pin.sha,
            target_version=driver_pin.version,
            target_tree_sha256=target_vq_tree_sha256,
        )
        for host in ordered
        if topology[host].role in {"managed", "vq-only"}
        and cfg.hosts[host].scheduler == "local"
    ]
    return RolloutPlan(
        driver=driver,
        report=fleet_release.report_summary(report),
        topology={
            name: asdict(resolved)
            for name, resolved in topology.items()
        },
        actions=actions,
        _scheduler_hold_targets=scheduler_hold_targets,
        provenance_lanes=provenance_lanes,
        topology_errors=topology_errors,
    )


SCOPE_EXCLUDED_REASON = "out of rollout scope"
"""Why an in-plan action is not executed by a host-scoped invocation.

Deliberately a ``defer`` and not a ``skip``. ``skip`` is the planner saying "no
work is needed here"; a scoped run has said nothing at all about the lanes it
did not look at, and ``run.complete`` keys off ``has_deferred``. Recording the
narrowing as a skip would let ``--only workstation`` mark the whole fleet converged.
"""


@dataclass(frozen=True)
class HostSelection:
    """Which hosts one ``rollout-latest`` invocation is allowed to change.

    Selection narrows *execution*, never the plan and never the report. The
    accepted report still pins every identity, the argv of every surviving
    action is byte-identical to the unscoped one, and end-of-run verification
    still rebuilds the whole fleet. That is the entire point: recovering one
    host after a terminal lane failure used to mean leaving the report-pinned
    path for a hand-typed ``vq admin update <env> <host> --expected-sha
    <40-hex>``, which is where SHA-transcription errors live.
    """

    only: tuple[str, ...] = ()
    skip: tuple[str, ...] = ()

    @property
    def scoped(self) -> bool:
        return bool(self.only or self.skip)

    def includes(self, host: str) -> bool:
        if self.only:
            return host in self.only
        return host not in self.skip

    def as_dict(self) -> dict[str, Any]:
        return {
            "only": list(self.only),
            "skip": list(self.skip),
            "scoped": self.scoped,
        }


def resolve_selection(
    cfg: config.Config,
    *,
    only: Sequence[str] = (),
    skip: Sequence[str] = (),
) -> HostSelection:
    """Validate operator host selection against the configured fleet.

    Fails closed on an unknown name rather than silently selecting nothing: a
    typo in ``--only planetxx`` that quietly became "update no hosts" would
    report a clean exit for a rollout that did nothing at all.
    """
    only_names = tuple(dict.fromkeys(only))
    skip_names = tuple(dict.fromkeys(skip))
    if only_names and skip_names:
        raise FleetRolloutError(
            "--only and --skip are mutually exclusive; use one or the other"
        )
    unknown = sorted(
        {name for name in (*only_names, *skip_names) if name not in cfg.hosts}
    )
    if unknown:
        raise FleetRolloutError(
            f"unknown host(s) {', '.join(unknown)}; configured hosts are "
            + ", ".join(sorted(cfg.hosts))
        )
    return HostSelection(only=only_names, skip=skip_names)


def select_hosts(plan: RolloutPlan, selection: HostSelection) -> RolloutPlan:
    """Return ``plan`` with out-of-scope update actions deferred.

    The driver's own vq lane is deliberately exempt. A report-pinned rollout is
    driven *by* the pinned vq — the driver stages every remote lane, derives
    every ancestry answer, and re-enters through its own fresh interpreter — so
    running a scoped recovery from a driver that is behind the pin would verify
    the fleet against the wrong code. ``--skip <driver>`` therefore narrows that
    host's helper/runtime/chemistry lanes and leaves its vq lane alone.
    """
    if not selection.scoped:
        return plan
    actions = [replace(action) for action in plan.actions]
    for action in actions:
        if action.phase in SCOPE_EXEMPT_PHASES:
            continue
        if selection.includes(action.host):
            continue
        if action.decision == "update":
            action.decision = "defer"
            action.reason = (
                f"{SCOPE_EXCLUDED_REASON}: {_selection_phrase(selection)}"
            )
    return RolloutPlan(
        driver=plan.driver,
        report=plan.report,
        topology=plan.topology,
        actions=actions,
        _scheduler_hold_targets=dict(plan._scheduler_hold_targets),
        provenance_lanes=list(plan.provenance_lanes),
        topology_errors=list(plan.topology_errors),
        retained_legacy_fences=[
            dict(fence) for fence in plan.retained_legacy_fences
        ],
        created_at=plan.created_at,
    )


def fence_retained_legacy_holds(
    plan: RolloutPlan,
    retained_holds: Sequence[tuple[str, str, str]],
) -> RolloutPlan:
    """Fence mutations on hosts whose pre-recorder state remains unknown.

    A coherent legacy-action supersession allows an unreachable pairless hold
    to stop blocking *other* hosts.  It never authorizes work on the retained
    host itself.  Convert every such host's managed lane to a defer, while a
    retained driver is a hard block because no later phase may run through an
    unfenced interpreter transition.  Reapply this projection to the final
    verification plan so a fresh snapshot cannot race the hold observation
    and manufacture a convergence claim.
    """
    reasons_by_host: dict[str, list[str]] = {}
    for rollout_id, host, reason in retained_holds:
        detail = f"{rollout_id}: {reason}"
        bucket = reasons_by_host.setdefault(host, [])
        if detail not in bucket:
            bucket.append(detail)
    if not reasons_by_host:
        return plan

    retained_fences: list[dict[str, str]] = [
        dict(fence) for fence in plan.retained_legacy_fences
    ]
    retained_keys = {
        (
            fence.get("rollout_id"),
            fence.get("host"),
            fence.get("reason"),
        )
        for fence in retained_fences
    }

    topology_errors: list[str] = []
    for error in plan.topology_errors:
        host = error.partition(":")[0]
        details = reasons_by_host.get(host)
        if details is None or host == plan.driver:
            topology_errors.append(error)
            continue
        topology_detail = f"topology evidence: {error}"
        if topology_detail not in details:
            details.append(topology_detail)

    def fence_reason(host: str) -> str:
        details = reasons_by_host[host]
        return (
            "retained legacy rollout state; mutation fenced until explicit "
            "evidence proves the historical action/hold safe to reconcile "
            f"({'; '.join(details)})"
        )

    for rollout_id, host, reason in retained_holds:
        item = {
            "rollout_id": rollout_id,
            "host": host,
            "reason": reason,
        }
        key = (item["rollout_id"], item["host"], item["reason"])
        if key not in retained_keys:
            retained_fences.append(item)
            retained_keys.add(key)

    actions = [replace(action) for action in plan.actions]
    for action in actions:
        details = reasons_by_host.get(action.host)
        if details is None:
            continue
        if action.phase == "driver":
            action.decision = "block"
        else:
            action.decision = "defer"
        action.reason = fence_reason(action.host)

    provenance_lanes = [
        replace(
            lane,
            decision=("block" if lane.host == plan.driver else "defer"),
            reason=fence_reason(lane.host),
        )
        if lane.host in reasons_by_host
        else lane
        for lane in plan.provenance_lanes
    ]

    return RolloutPlan(
        driver=plan.driver,
        report=plan.report,
        topology=plan.topology,
        actions=actions,
        _scheduler_hold_targets=dict(plan._scheduler_hold_targets),
        provenance_lanes=provenance_lanes,
        topology_errors=topology_errors,
        retained_legacy_fences=retained_fences,
        created_at=plan.created_at,
    )


def assert_selection_in_plan(plan: RolloutPlan, selection: HostSelection) -> None:
    """Refuse a selection whose named hosts have no lane in this plan.

    Both directions matter, and for the same reason. ``--only`` a
    configured-but-unplanned host — an ``alias``, an ``excluded`` one, a name
    that resolves to no managed lane — would run to completion having done
    nothing and exit 0 saying so. ``--skip`` a lane-less host is a silent no-op:
    the operator believes a host was excluded and it never was, which is worse
    than the typo, because the run then does exactly what they were trying to
    prevent. A scheduler alias sharing its canonical host's SSH endpoint is the
    realistic case — skipping the alias name does not skip the canonical lanes.
    """
    if not selection.scoped:
        return
    # Read-only root provenance is not a managed update lane. In particular,
    # an exact root daemon on a ``vq-only`` host must not disguise the missing
    # user-install lane and turn a zero-action ``--only`` run green.
    planned_hosts = {action.host for action in plan.actions}
    planned = sorted(planned_hosts)
    named = selection.only or selection.skip
    absent = sorted(name for name in named if name not in planned)
    if absent:
        raise FleetRolloutError(
            f"{_selection_phrase(selection)} names host(s) with no lane in "
            f"this plan: {', '.join(absent)}. Hosts with lanes are "
            + ", ".join(planned)
        )
    if not any(
        selection.includes(host)
        for host in planned_hosts
    ):
        raise FleetRolloutError(
            f"{_selection_phrase(selection)} leaves no lane to act on; "
            "hosts with lanes in this plan are " + ", ".join(planned)
        )


def _diagnostic_scope_hosts(
    plans: Sequence[RolloutPlan],
    selection: HostSelection,
    keep_phases: frozenset[str],
    *,
    extra_hosts: Sequence[str] = (),
) -> set[str]:
    """Include exact scheduler protection targets without selecting more actions.

    Final parity requires every bound alias to be healthy, even when only its
    canonical action host was selected. Use the plan's captured target binding;
    shared SSH/control endpoints and similar names do not establish membership.
    """
    names = set(extra_hosts)
    exempt: set[str] = set()
    bound: set[str] = set()
    selected_targets: set[str] = set()
    for plan in plans:
        names.update(plan.topology)
        names.update(error.partition(":")[0] for error in plan.topology_errors)
        names.update(action.host for action in plan.actions)
        exempt.update(action.host for action in plan.actions if action.phase in keep_phases)
        for action_host, targets in plan._scheduler_hold_targets.items():
            bound.update(target for target, _control in targets)
            if selection.includes(action_host):
                selected_targets.update(target for target, _control in targets)
    # --skip canonical also excludes its bound aliases from diagnostics.
    # They do not become selected merely because their literal names differ.
    return {
        name for name in names - bound if selection.includes(name)
    } | selected_targets | exempt


def restrict_plan(
    plan: RolloutPlan,
    selection: HostSelection,
    *,
    keep_phases: frozenset[str] = frozenset(),
) -> RolloutPlan:
    """Return ``plan`` reduced to the selected hosts, for read-only verdicts.

    The execution path defers out-of-scope lanes (:func:`select_hosts`) because
    a narrowed *rollout* has not proven anything about the hosts it left alone.
    A narrowed *verdict* is the opposite question — "is workstation converged?" —
    so here the out-of-scope lanes, their doctor results, and their topology
    errors are removed rather than counted against the answer. For a pure
    verdict the driver lane is not exempt either: ``--only workstation`` asks about
    workstation.

    ``keep_phases`` re-admits phases the caller must not drop. The execution
    result passes :data:`SCOPE_EXEMPT_PHASES`, because :func:`select_hosts`
    never narrows the driver's own vq lane away — omitting it from the scoped
    verdict would exit 0 for a run whose driver was itself deferred, i.e. a
    rollout staged by a vq that is not the one the report pins.
    """
    if not selection.scoped:
        return plan
    kept_hosts = _diagnostic_scope_hosts((plan,), selection, keep_phases)

    def _in_scope(host: str) -> bool:
        return host in kept_hosts

    return RolloutPlan(
        driver=plan.driver,
        report=plan.report,
        topology={
            name: entry
            for name, entry in plan.topology.items()
            if _in_scope(name)
        },
        actions=[
            action
            for action in plan.actions
            if selection.includes(action.host) or action.phase in keep_phases
        ],
        _scheduler_hold_targets=dict(plan._scheduler_hold_targets),
        provenance_lanes=[
            lane
            for lane in plan.provenance_lanes
            if selection.includes(lane.host)
        ],
        topology_errors=[
            error
            for error in plan.topology_errors
            if _in_scope(error.partition(":")[0])
        ],
        retained_legacy_fences=[
            dict(fence)
            for fence in plan.retained_legacy_fences
            if _in_scope(str(fence.get("host") or ""))
        ],
        created_at=plan.created_at,
    )


def _selection_phrase(selection: HostSelection) -> str:
    if selection.only:
        return "--only " + ", ".join(selection.only)
    return "--skip " + ", ".join(selection.skip)


def collect_json(
    argv: Sequence[str],
    *,
    runner: Runner = subprocess.run,
) -> Mapping[str, Any]:
    """Run one read-only vq JSON command and parse output even on exit 1."""
    proc = runner(
        [sys.executable, "-m", "vq", *argv],
        capture_output=True,
        text=True,
        check=False,
        timeout=600,
    )
    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        detail = (proc.stderr or proc.stdout or "(no output)").strip()
        raise FleetRolloutError(
            f"vq {' '.join(argv)} returned invalid JSON: {detail}"
        ) from exc
    if not isinstance(payload, dict):
        raise FleetRolloutError(f"vq {' '.join(argv)} JSON root is not an object")
    return payload


def collect_snapshots(
    *,
    runner: Runner = subprocess.run,
    check_timeout_seconds: float | None = None,
) -> tuple[Mapping[str, Any], Mapping[str, Any], Mapping[str, Any]]:
    """Collect the three read-only fleet sweeps concurrently.

    Each sweep already fans out per host in a bounded pool; running the
    three sweeps themselves in parallel keeps dry-run discovery inside
    the minute-scale budget. Deployment stays strictly serial — this
    concurrency is read-only.
    """
    doctor_argv = ["doctor", "--all", "--json"]
    if check_timeout_seconds is not None:
        # The sweep could not pass this at all until v0.26.1, so every fleet
        # got the 10 s default whatever its login node was like. See
        # `FleetConfig.check_timeout_seconds`.
        doctor_argv += ["--check-timeout", f"{check_timeout_seconds:g}"]
    argvs = (
        ["admin", "status", "--all", "--json"],
        ["programs", "--all", "--json"],
        doctor_argv,
    )
    if runner is not subprocess.run:
        # Test runners are frequently stateful fakes; keep them serial and
        # deterministic.
        results = [collect_json(argv, runner=runner) for argv in argvs]
        return tuple(results)
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=len(argvs)) as pool:
        futures = [
            pool.submit(collect_json, argv, runner=runner) for argv in argvs
        ]
        return tuple(future.result() for future in futures)


def execute_action(
    action: RolloutAction,
    *,
    runner: Runner = subprocess.run,
) -> subprocess.CompletedProcess[str]:
    """Execute one planned update, streaming output into the parent terminal."""
    capture = runner is not subprocess.run
    return runner(
        [sys.executable, "-m", "vq", *action.argv],
        capture_output=capture,
        text=True,
        check=False,
    )


def _run_control(
    argv: Sequence[str],
    *,
    runner: Runner,
    timeout: float | None = None,
) -> subprocess.CompletedProcess[str]:
    kwargs: dict[str, Any] = {
        "capture_output": True,
        "text": True,
        "check": False,
    }
    if timeout is not None:
        kwargs["timeout"] = timeout
    return runner(
        [sys.executable, "-m", "vq", *argv],
        **kwargs,
    )


def _run_bounded_observation_argv(
    argv: Sequence[str],
    *,
    timeout: float,
) -> subprocess.CompletedProcess[str]:
    """Run one read-only observer with bounded output and group lifetime."""
    proc = subprocess.Popen(
        list(argv),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    selector: selectors.BaseSelector | None = None
    stdout_chunks: list[bytes] = []
    stdout_stored = 0
    output_observed = 0
    output_exceeded = False
    returncode: int | None = None
    deadline = time.monotonic() + timeout

    def signal_group(sig: signal.Signals) -> None:
        with suppress(ProcessLookupError, PermissionError):
            os.killpg(proc.pid, sig)

    def reap_group() -> None:
        # Do not reap the leader before signalling the entire owned group: its
        # PID is also the PGID and must not become reusable before killpg.
        signal_group(signal.SIGKILL)
        with suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=_DRAIN_LIVENESS_REAP_GRACE_SECONDS)

    try:
        assert proc.stdout is not None
        assert proc.stderr is not None
        selector = selectors.DefaultSelector()
        selector.register(proc.stdout, selectors.EVENT_READ, "stdout")
        selector.register(proc.stderr, selectors.EVENT_READ, "stderr")
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(list(argv), timeout)
            for key, _events in selector.select(min(remaining, 0.1)):
                try:
                    chunk = os.read(key.fileobj.fileno(), 65536)
                except BlockingIOError:
                    continue
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                output_observed += len(chunk)
                if output_observed > _DRAIN_LIVENESS_OUTPUT_LIMIT:
                    output_exceeded = True
                if key.data == "stdout" and (
                    stdout_stored < _DRAIN_LIVENESS_OUTPUT_LIMIT
                ):
                    retained = chunk[
                        : _DRAIN_LIVENESS_OUTPUT_LIMIT - stdout_stored
                    ]
                    stdout_chunks.append(retained)
                    stdout_stored += len(retained)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise subprocess.TimeoutExpired(list(argv), timeout)
        try:
            returncode = proc.wait(timeout=remaining)
        except subprocess.TimeoutExpired as exc:
            raise subprocess.TimeoutExpired(list(argv), timeout) from exc
    except BaseException:
        reap_group()
        raise
    finally:
        if selector is not None:
            selector.close()
        with suppress(OSError):
            if proc.stdout is not None:
                proc.stdout.close()
        with suppress(OSError):
            if proc.stderr is not None:
                proc.stderr.close()

    if output_exceeded:
        raise ValueError("snapshot command output exceeds the size limit")
    assert returncode is not None
    return subprocess.CompletedProcess(
        list(argv),
        returncode,
        stdout=b"".join(stdout_chunks).decode("utf-8", errors="strict"),
        stderr="",
    )


def _drain_observation_argv(cfg: config.Config, control: str) -> list[str]:
    remote_args = [
        "drain",
        "--status",
        "--json",
        "--read-only-snapshot",
        "localhost",
    ]
    if is_local_host(control):
        return [sys.executable, "-m", "vq", *remote_args]
    host_cfg = cfg.host(control)
    if is_local_host(host_cfg.ssh):
        return [sys.executable, "-m", "vq", *remote_args]
    remote_command = shlex.join([host_cfg.remote_vq, *remote_args])
    return [*transport._ssh_base(host_cfg), remote_command]


def _drain_control_endpoint(cfg: config.Config, control: str) -> str:
    """Collapse logical controls that address this process's local daemon."""
    if is_local_host(control):
        return "localhost"
    try:
        host_cfg = cfg.host(control)
    except config.ConfigError:
        return control
    return "localhost" if is_local_host(host_cfg.ssh) else control


def _control_json(
    argv: Sequence[str],
    *,
    runner: Runner,
) -> Mapping[str, Any]:
    proc = _run_control(argv, runner=runner)
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "(no output)").strip()
        raise FleetRolloutError(
            f"vq {' '.join(argv)} failed with exit {proc.returncode}: {detail}"
        )
    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        detail = (proc.stderr or proc.stdout or "(no output)").strip()
        raise FleetRolloutError(
            f"vq {' '.join(argv)} returned invalid JSON: {detail}"
        ) from exc
    if not isinstance(payload, dict):
        raise FleetRolloutError(f"vq {' '.join(argv)} JSON root is not an object")
    return payload


def _liveness_text(value: object, *, default: str | None = None) -> str | None:
    if not isinstance(value, str):
        return default
    neutral = "".join(
        " " if unicodedata.category(char).startswith("C") else char
        for char in value
    )
    normalized = " ".join(neutral.split())
    if not normalized:
        return default
    return normalized[:_DRAIN_LIVENESS_TEXT_LIMIT]


def _liveness_timestamp(value: object) -> str:
    if (
        not isinstance(value, str)
        or len(value) > _DRAIN_LIVENESS_IDENTITY_TEXT_LIMIT
    ):
        raise ValueError("timestamp is missing or oversized")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp is not timezone-aware")
    return value


def _strict_liveness_json(text: object) -> Mapping[str, Any]:
    if not isinstance(text, str):
        raise ValueError("snapshot output is not text")
    if len(text.encode("utf-8")) > _DRAIN_LIVENESS_OUTPUT_LIMIT:
        raise ValueError("snapshot output exceeds the size limit")

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

    try:
        payload = json.loads(
            text,
            object_pairs_hook=reject_duplicates,
            parse_constant=reject_constant,
        )
    except RecursionError as exc:
        raise ValueError("snapshot JSON is too deeply nested") from exc
    pending: list[tuple[object, int]] = [(payload, 0)]
    while pending:
        item, depth = pending.pop()
        if depth > 32:
            raise ValueError("snapshot JSON is too deeply nested")
        if isinstance(item, dict):
            pending.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            pending.extend((child, depth + 1) for child in item)
        elif isinstance(item, float) and not math.isfinite(item):
            raise ValueError("snapshot JSON contains a non-finite number")
    if not isinstance(payload, dict):
        raise ValueError("snapshot JSON root is not an object")
    return payload


def _validate_final_drain_status(payload: Mapping[str, Any]) -> dict[str, Any]:
    if payload.get("schema") != _DRAIN_READ_ONLY_STATUS_SCHEMA:
        raise ValueError("unsupported snapshot schema")
    observed_at = _liveness_timestamp(payload.get("observed_at"))
    provenance = payload.get("provenance")
    if not isinstance(provenance, Mapping) or set(provenance) != {
        "method",
        "version",
        "source_sha",
        "source_tree_sha256",
        "multi_user",
    }:
        raise ValueError("snapshot provenance is malformed")
    source_sha = provenance.get("source_sha")
    source_tree = provenance.get("source_tree_sha256")
    if (
        provenance.get("method") != "get_drain_read_only_snapshot"
        or not isinstance(provenance.get("version"), str)
        or not provenance["version"]
        or not isinstance(source_sha, str)
        or _SOURCE_SHA.fullmatch(source_sha) is None
        or not isinstance(source_tree, str)
        or _SOURCE_TREE_SHA256.fullmatch(source_tree) is None
        or type(provenance.get("multi_user")) is not bool
    ):
        raise ValueError("snapshot provenance is unsupported")
    coverage = payload.get("coverage")
    if not isinstance(coverage, Mapping) or set(coverage) != {
        "legacy_state",
        "scheduler_leases",
    }:
        raise ValueError("snapshot coverage is malformed")
    if any(type(value) is not bool for value in coverage.values()):
        raise ValueError("snapshot coverage is malformed")
    active = payload.get("active")
    safety = payload.get("safety_fail_closed")
    if type(active) is not bool or type(safety) is not bool:
        raise ValueError("snapshot activity flags are malformed")
    if safety is not (coverage["scheduler_leases"] is False):
        raise ValueError("snapshot safety gate contradicts lease coverage")

    raw_leases = payload.get("scheduler_leases")
    if not isinstance(raw_leases, list):
        raise ValueError("snapshot scheduler leases are malformed")
    leases: list[dict[str, str | None]] = []
    ids: set[str] = set()
    owners: set[tuple[str, str]] = set()
    for raw in raw_leases:
        if not isinstance(raw, Mapping):
            raise ValueError("snapshot scheduler lease is malformed")
        lease_id = raw.get("lease_id")
        host = raw.get("scheduler_host")
        owner = raw.get("owner")
        reason = raw.get("reason")
        if (
            not isinstance(lease_id, str)
            or _DRAIN_LEASE_ID.fullmatch(lease_id) is None
            or not isinstance(host, str)
            or not host
            or len(host) > 255
            or not isinstance(owner, str)
            or not owner
            or len(owner) > 512
            or (reason is not None and not isinstance(reason, str))
            or (
                isinstance(reason, str)
                and len(reason) > _DRAIN_LIVENESS_IDENTITY_TEXT_LIMIT
            )
        ):
            raise ValueError("snapshot scheduler lease is malformed")
        set_at = _liveness_timestamp(raw.get("set_at"))
        if lease_id in ids or (host, owner) in owners:
            raise ValueError("snapshot scheduler lease identity is duplicated")
        ids.add(lease_id)
        owners.add((host, owner))
        leases.append(
            {
                "lease_id": lease_id,
                "scheduler_host": host,
                "owner": owner,
                "set_at": set_at,
                "reason": _liveness_text(reason),
                "identity_reason": reason,
            }
        )
    if not coverage["scheduler_leases"] and leases:
        raise ValueError("snapshot leases contradict incomplete coverage")

    raw_policy = payload.get("policy")
    policy: dict[str, Any] | None = None
    if raw_policy is not None:
        if not isinstance(raw_policy, Mapping):
            raise ValueError("snapshot policy is malformed")
        required = {
            "active",
            "is_full_drain",
            "max_jobs",
            "max_cpus",
            "reason",
            "set_at",
            "scheduler_hosts",
            "legacy_scheduler_hosts",
            "full_dispatch",
            "reject_submits",
            "update_mode",
            "duration_seconds",
        }
        if not required.issubset(raw_policy):
            raise ValueError("snapshot policy is incomplete")
        if any(
            type(raw_policy.get(key)) is not bool
            for key in (
                "active",
                "is_full_drain",
                "full_dispatch",
                "reject_submits",
            )
        ) or raw_policy.get("active") is not True:
            raise ValueError("snapshot policy flags are malformed")
        for cap in ("max_jobs", "max_cpus", "duration_seconds"):
            value = raw_policy.get(cap)
            if value is not None and (
                type(value) is not int or value < 0
            ):
                raise ValueError("snapshot policy cap is malformed")
        update_mode = raw_policy.get("update_mode")
        if update_mode not in {None, "accept", "deny"}:
            raise ValueError("snapshot update mode is malformed")
        reason = raw_policy.get("reason")
        if reason is not None and (
            not isinstance(reason, str)
            or len(reason) > _DRAIN_LIVENESS_IDENTITY_TEXT_LIMIT
        ):
            raise ValueError("snapshot reason is malformed")
        set_at = _liveness_timestamp(raw_policy.get("set_at"))

        def host_list(name: str) -> list[str]:
            raw_hosts = raw_policy.get(name)
            if (
                not isinstance(raw_hosts, list)
                or any(
                    not isinstance(host, str)
                    or not host
                    or len(host) > 255
                    for host in raw_hosts
                )
                or len(set(raw_hosts)) != len(raw_hosts)
            ):
                raise ValueError(f"snapshot {name} is malformed")
            return list(raw_hosts)

        scheduler_hosts = host_list("scheduler_hosts")
        legacy_hosts = host_list("legacy_scheduler_hosts")
        if not coverage["legacy_state"] and legacy_hosts:
            raise ValueError("legacy targets contradict incomplete coverage")
        component_hosts = set(legacy_hosts) | {
            str(lease["scheduler_host"]) for lease in leases
        }
        if not safety and set(scheduler_hosts) != component_hosts:
            raise ValueError("effective scheduler targets contradict components")
        computed_full = bool(raw_policy["full_dispatch"]) or (
            raw_policy.get("max_jobs") is None
            and raw_policy.get("max_cpus") is None
            and not scheduler_hosts
        )
        if raw_policy["is_full_drain"] is not computed_full:
            raise ValueError("snapshot full-drain flag contradicts its policy")
        if not coverage["legacy_state"] and coverage["scheduler_leases"]:
            lease_hosts = sorted(
                {str(lease["scheduler_host"]) for lease in leases}
            )
            expected_reason = (
                leases[0]["identity_reason"] if len(leases) == 1 else None
            )
            expected_set_at = (
                min(str(lease["set_at"]) for lease in leases)
                if leases
                else None
            )
            if (
                not leases
                or legacy_hosts
                or scheduler_hosts != lease_hosts
                or reason != expected_reason
                or set_at != expected_set_at
                or raw_policy.get("max_jobs") is not None
                or raw_policy.get("max_cpus") is not None
                or raw_policy["full_dispatch"] is not False
                or raw_policy["reject_submits"] is not False
                or update_mode is not None
                or raw_policy.get("duration_seconds") is not None
                or raw_policy["is_full_drain"] is not False
            ):
                raise ValueError(
                    "snapshot policy contradicts unavailable legacy coverage"
                )
        if safety and not coverage["legacy_state"] and (
            scheduler_hosts
            or legacy_hosts
            or leases
            or reason
            != "scheduler drain inventory unreadable; dispatch held safe"
            or set_at != observed_at
            or raw_policy.get("max_jobs") is not None
            or raw_policy.get("max_cpus") is not None
            or raw_policy["full_dispatch"] is not True
            or raw_policy["reject_submits"] is not False
            or update_mode is not None
            or raw_policy.get("duration_seconds") is not None
            or raw_policy["is_full_drain"] is not True
        ):
            raise ValueError("snapshot safety policy is not synthetic-exact")
        policy = {
            "active": True,
            "is_full_drain": raw_policy["is_full_drain"],
            "max_jobs": raw_policy.get("max_jobs"),
            "max_cpus": raw_policy.get("max_cpus"),
            "reason": _liveness_text(reason),
            "identity_reason": reason,
            "set_at": set_at,
            "scheduler_hosts": scheduler_hosts,
            "legacy_scheduler_hosts": legacy_hosts,
            "full_dispatch": raw_policy["full_dispatch"],
            "reject_submits": raw_policy["reject_submits"],
            "update_mode": update_mode,
            "duration_seconds": raw_policy.get("duration_seconds"),
        }
    if active is not (policy is not None):
        raise ValueError("snapshot active flag contradicts its policy")
    if not active and (
        not all(coverage.values()) or leases or safety
    ):
        raise ValueError("inactive snapshot lacks complete authoritative coverage")
    if safety and (policy is None or policy["is_full_drain"] is not True):
        raise ValueError("snapshot safety gate lacks a full policy")
    return {
        "observed_at": observed_at,
        "coverage": dict(coverage),
        "policy": policy,
        "scheduler_leases": leases,
        "safety_fail_closed": safety,
    }


def _canonical_drain_control(
    cfg: config.Config,
    topology: Mapping[str, Mapping[str, Any]],
    host: str,
) -> str:
    try:
        host_cfg = cfg.host(host)
    except config.ConfigError as exc:
        raise ValueError("drain target is not configured") from exc
    if host_cfg.scheduler != "local":
        driver = host_cfg.scheduler_driver
        if not isinstance(driver, str) or not driver:
            raise ValueError("scheduler control driver is missing")
        current = driver
    else:
        current = host

    if is_local_host(current) and current not in cfg.hosts:
        return "localhost"

    seen: set[str] = set()
    while True:
        if current in seen:
            raise ValueError("control mapping is cyclic")
        seen.add(current)
        entry = topology.get(current)
        if not isinstance(entry, Mapping):
            raise ValueError("control topology is missing")
        if entry.get("role") != "alias":
            break
        canonical = entry.get("canonical_host")
        if not isinstance(canonical, str) or not canonical:
            raise ValueError("alias control mapping is ambiguous")
        current = canonical

    try:
        control_cfg = cfg.host(current)
    except config.ConfigError as exc:
        raise ValueError("canonical control host is not configured") from exc
    if control_cfg.scheduler != "local":
        raise ValueError("drain control is not a daemon host")
    return current


def _live_hold_owner_class(
    run: RolloutRun | None,
    *,
    host: str,
    control_host: str,
    kind: str,
    owner: str | None,
    reason: str | None,
    set_at: str | None,
) -> str:
    if kind == "safety-fail-closed":
        return "safety-fail-closed"
    if run is None:
        return "external" if owner is not None or kind != "scheduler-target" else "legacy"
    raw = run.holds.get(host)
    if not isinstance(raw, Mapping):
        return "external" if owner is not None or kind != "scheduler-target" else "legacy"
    exact_base = (
        raw.get("host") == host
        and raw.get("owned") is True
        and raw.get("control_host") == control_host
        and raw.get("reason") == reason
        and raw.get("status") in {"active", "cleanup-failed"}
    )
    if kind == "scheduler-target" and owner is not None:
        if (
            exact_base
            and raw.get("kind") == "scheduler-target"
            and raw.get("lease_owner") == owner
        ):
            return "rollout"
        return "external"
    if kind == "scheduler-target":
        if (
            exact_base
            and raw.get("kind") == "scheduler-target"
            and raw.get("legacy_expected_set_at") == set_at
        ):
            return "rollout"
        return "legacy"
    if kind == "full" and (
        exact_base
        and raw.get("kind") == "full"
        and raw.get("set_at") == set_at
    ):
        return "rollout"
    return "external"


def _final_liveness_hold(
    run: RolloutRun | None,
    *,
    host: str,
    target: str,
    control_host: str,
    remote_observed_at: str,
    controller_observed_at: str,
    kind: str,
    owner: str | None = None,
    lease_id: str | None = None,
    reason: str | None = None,
    identity_reason: str | None = None,
    set_at: str | None = None,
    constraints: Mapping[str, int | None] | None = None,
) -> dict[str, Any]:
    ownership_host = control_host if kind == "full" else target
    item: dict[str, Any] = {
        "host": host,
        "control_host": control_host,
        "kind": kind,
        "owner_class": _live_hold_owner_class(
            run,
            host=ownership_host,
            control_host=control_host,
            kind=kind,
            owner=owner,
            reason=identity_reason,
            set_at=set_at,
        ),
        "owner": _liveness_text(owner),
        "lease_id": _liveness_text(lease_id),
        "reason": _liveness_text(reason),
        "set_at": _liveness_text(set_at),
        "remote_observed_at": _liveness_text(
            remote_observed_at,
            default="unknown",
        ),
        "controller_observed_at": _liveness_text(
            controller_observed_at,
            default="unknown",
        ),
    }
    if constraints is not None:
        item["constraints"] = dict(constraints)
    return item


def collect_final_drain_liveness(
    cfg: config.Config,
    *,
    topology: Mapping[str, Mapping[str, Any]],
    run: RolloutRun | None,
    runner: Runner = subprocess.run,
) -> dict[str, Any]:
    """Observe every configured host through each canonical daemon once."""
    mappings: dict[str, tuple[str, str, str]] = {}
    unknown: list[dict[str, str]] = []
    for host in sorted(cfg.hosts):
        try:
            control = _canonical_drain_control(cfg, topology, host)
        except ValueError:
            unknown.append(
                {
                    "host": host,
                    "control_host": host,
                    "reason": "missing or ambiguous drain control mapping",
                }
            )
            continue
        # The requested scheduler target remains exact. Alias canonicalization
        # selects only the daemon to query; it must not invent alias hold scope.
        mappings[host] = (
            control,
            host,
            _drain_control_endpoint(cfg, control),
        )

    snapshots: dict[str, dict[str, Any]] = {}
    control_times: dict[str, str] = {}
    controls = sorted({endpoint for _control, _target, endpoint in mappings.values()})

    def observe(control: str) -> tuple[str, dict[str, Any] | None, str]:
        try:
            if runner is subprocess.run:
                proc = _run_bounded_observation_argv(
                    _drain_observation_argv(cfg, control),
                    timeout=_DRAIN_LIVENESS_CONTROL_TIMEOUT_SECONDS,
                )
            else:
                proc = _run_control(
                    [
                        "drain",
                        "--status",
                        "--json",
                        "--read-only-snapshot",
                        control,
                    ],
                    runner=runner,
                    timeout=_DRAIN_LIVENESS_CONTROL_TIMEOUT_SECONDS,
                )
            if proc.returncode != 0:
                raise ValueError("snapshot command failed")
            snapshot = _validate_final_drain_status(
                _strict_liveness_json(proc.stdout)
            )
            return control, snapshot, utcnow_iso()
        except (
            OSError,
            RecursionError,
            subprocess.SubprocessError,
            subprocess.TimeoutExpired,
            TypeError,
            ValueError,
        ):
            return control, None, utcnow_iso()

    if runner is subprocess.run and controls:
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(
            max_workers=min(len(controls), _DRAIN_LIVENESS_MAX_WORKERS)
        ) as pool:
            observations = list(pool.map(observe, controls))
    else:
        observations = [observe(control) for control in controls]
    for control, snapshot, controller_at in observations:
        if snapshot is None:
            continue
        snapshots[control] = snapshot
        control_times[control] = controller_at

    observed_hosts: list[dict[str, str]] = []
    inactive_hosts: list[str] = []
    active_holds: list[dict[str, Any]] = []
    already_unknown = {item["host"] for item in unknown}
    for host, (control, target, endpoint) in sorted(mappings.items()):
        snapshot = snapshots.get(endpoint)
        if snapshot is None:
            if host not in already_unknown:
                unknown.append(
                    {
                        "host": host,
                        "control_host": control,
                        "reason": "unsupported or malformed read-only drain snapshot",
                    }
                )
                already_unknown.add(host)
            continue
        remote_at = str(snapshot["observed_at"])
        controller_at = control_times[endpoint]
        observed_hosts.append(
            {
                "host": host,
                "control_host": control,
                "remote_observed_at": _liveness_text(
                    remote_at,
                    default="unknown",
                ),
                "controller_observed_at": _liveness_text(
                    controller_at,
                    default="unknown",
                ),
            }
        )
        coverage = snapshot["coverage"]
        if not all(coverage.values()) and host not in already_unknown:
            unknown.append(
                {
                    "host": host,
                    "control_host": control,
                    "reason": "read-only drain snapshot coverage is incomplete",
                }
            )
            already_unknown.add(host)
        policy = snapshot["policy"]
        host_holds: list[dict[str, Any]] = []
        make_hold = partial(
            _final_liveness_hold,
            run,
            host=host,
            target=target,
            control_host=control,
            remote_observed_at=remote_at,
            controller_observed_at=controller_at,
        )

        if isinstance(policy, Mapping) and not snapshot["safety_fail_closed"]:
            reason = policy.get("reason")
            identity_reason = policy.get("identity_reason")
            set_at = policy.get("set_at")
            if policy.get("is_full_drain") is True:
                host_holds.append(
                    make_hold(
                        kind="full",
                        reason=reason,
                        identity_reason=identity_reason,
                        set_at=set_at,
                    )
                )
            if (
                cfg.host(target).scheduler == "local"
                and (
                    policy.get("max_jobs") is not None
                    or policy.get("max_cpus") is not None
                )
            ):
                host_holds.append(
                    make_hold(
                        kind="partial",
                        reason=reason,
                        identity_reason=identity_reason,
                        set_at=set_at,
                        constraints={
                            "max_jobs": policy.get("max_jobs"),
                            "max_cpus": policy.get("max_cpus"),
                        },
                    )
                )
            if policy.get("reject_submits") is True:
                host_holds.append(
                    make_hold(
                        kind="submit-deny",
                        reason=reason,
                        identity_reason=identity_reason,
                        set_at=set_at,
                    )
                )
            if target in policy.get("legacy_scheduler_hosts", []):
                host_holds.append(
                    make_hold(
                        kind="scheduler-target",
                        reason=reason,
                        identity_reason=identity_reason,
                        set_at=set_at,
                    )
                )
        for lease in snapshot["scheduler_leases"]:
            if lease.get("scheduler_host") != target:
                continue
            host_holds.append(
                make_hold(
                    kind="scheduler-target",
                    owner=lease.get("owner"),
                    lease_id=lease.get("lease_id"),
                    reason=lease.get("reason"),
                    identity_reason=lease.get("identity_reason"),
                    set_at=lease.get("set_at"),
                )
            )
        if snapshot["safety_fail_closed"]:
            host_holds.append(
                make_hold(
                    kind="safety-fail-closed",
                    reason=(
                        "scheduler drain inventory unreadable; dispatch held safe"
                    ),
                    identity_reason=None,
                    set_at=remote_at,
                )
            )
        if not host_holds and all(coverage.values()):
            inactive_hosts.append(host)
        active_holds.extend(host_holds)

    kind_order = {
        "full": 0,
        "partial": 1,
        "submit-deny": 2,
        "scheduler-target": 3,
        "safety-fail-closed": 4,
    }
    active_holds.sort(
        key=lambda item: (
            str(item["host"]),
            kind_order.get(str(item["kind"]), 99),
            str(item.get("lease_id") or ""),
        )
    )
    unknown.sort(key=lambda item: (item["host"], item["control_host"]))
    observed_hosts.sort(key=lambda item: item["host"])
    inactive_hosts.sort()
    status = (
        "unavailable"
        if not observed_hosts
        else "partial" if unknown else "complete"
    )
    return {
        "status": status,
        "observed_at": utcnow_iso(),
        "observed_hosts": observed_hosts,
        "inactive_hosts": inactive_hosts,
        "active_holds": active_holds,
        "unknown_hosts": unknown,
    }


def _hold_is_scheduler(actions: Sequence[RolloutAction]) -> bool:
    return any(
        action.phase in {"helper", "scheduler-runtime"} for action in actions
    )


def _scheduler_hold_targets_for(
    plan: RolloutPlan,
    action_host: str,
) -> tuple[tuple[str, str], ...]:
    """Return the exact scheduler targets protected by one action host.

    Hand-built/legacy plans predate the private mapping and retain the
    canonical target on the plan driver.  Built plans always carry the full
    resolved alias set from their config snapshot.
    """
    targets = plan._scheduler_hold_targets.get(action_host)
    if targets:
        return targets
    return ((action_host, plan.driver),)


def _journaled_hold_action_host(
    target: str,
    hold: Mapping[str, Any],
) -> str:
    """Return the canonical operation host protected by an exact hold.

    Old journals have no ``action_host`` and are canonical by construction, so
    their key remains the relationship.  A malformed new binding must not be
    guessed: doing so could release an alias while its canonical durable child
    is still unsettled.
    """
    value = hold.get("action_host")
    if value is None:
        return target
    if not isinstance(value, str) or not value.strip():
        raise FleetRolloutError(
            f"rollout hold for {target} has invalid action_host binding"
        )
    return value


def _journaled_hold_control_host(
    target: str,
    hold: Mapping[str, Any],
    *,
    legacy_default: str | None = None,
) -> str:
    """Return a trusted exact control binding, with canonical compatibility."""
    value = hold.get("control_host")
    if value is None:
        if hold.get("action_host") is None and legacy_default is not None:
            return legacy_default
        raise FleetRolloutError(
            f"rollout hold for {target} has invalid control_host binding"
        )
    if not isinstance(value, str) or not value or value != value.strip():
        raise FleetRolloutError(
            f"rollout hold for {target} has invalid control_host binding"
        )
    return value


def _validate_scheduler_hold_bindings(
    plan: RolloutPlan,
    run: RolloutRun,
) -> None:
    """Fence durable target bindings before any rollout control mutation."""
    legacy_targets = _journaled_legacy_owned_hosts(run)
    scheduler_actions: dict[str, list[RolloutAction]] = {}
    for action in plan.actions:
        if action.phase != "driver":
            scheduler_actions.setdefault(action.host, []).append(action)
    for target, hold in run.holds.items():
        if not isinstance(hold, Mapping):
            continue
        if hold.get("kind") != "scheduler-target" or hold.get("status") not in {
            "active",
            "cleanup-failed",
        }:
            continue
        if hold.get("host") != target:
            raise FleetRolloutError(
                f"durable scheduler hold binding for {target} has a "
                "different exact target"
            )
        owner = hold.get("lease_owner")
        exact_owner = (
            hold.get("owned") is True
            and owner == _hold_lease_owner(run.rollout_id, target)
        )
        legacy_owner = (
            hold.get("owned") is True
            and owner is None
            and hold.get("action_host") is None
            and hold.get("reason") == _hold_reason(run.rollout_id, target)
        )
        if not exact_owner and not legacy_owner:
            raise FleetRolloutError(
                f"durable scheduler hold ownership for {target} is neither "
                "the deterministic exact owner nor a recognized historical "
                "legacy claim"
            )
        action_host = _journaled_hold_action_host(target, hold)
        current = plan._scheduler_hold_targets.get(action_host)
        if current is None:
            actions = scheduler_actions.get(action_host, ())
            if not _hold_is_scheduler(actions):
                if target in legacy_targets:
                    # The compatibility migration loop below owns this
                    # actionable legacy error and preserves its established
                    # recovery guidance without issuing a control mutation.
                    continue
                raise FleetRolloutError(
                    f"durable scheduler hold binding for {target} references "
                    f"action host {action_host!r} with no current scheduler lane"
                )
            current = _scheduler_hold_targets_for(plan, action_host)
        controls = dict(current)
        expected_control = controls.get(target)
        if expected_control is None:
            raise FleetRolloutError(
                f"durable scheduler hold binding for {target} is no longer "
                f"part of action host {action_host!r}; it was removed or "
                "reparented"
            )
        stored_control = _journaled_hold_control_host(
            target,
            hold,
            legacy_default=(
                expected_control if action_host == target else None
            ),
        )
        if stored_control != expected_control:
            raise FleetRolloutError(
                f"durable scheduler hold binding for {target} names control "
                f"host {stored_control!r}, not current {expected_control!r}"
            )


def _hold_reason(rollout_id: str, host: str) -> str:
    del host
    # One driver drain document can carry several scheduler-target lanes, so
    # every lane in the same rollout must use the same ownership token.
    return f"vq rollout-latest {rollout_id}"


def _hold_lease_owner(rollout_id: str, host: str) -> str:
    """Return the deterministic owner key for one rollout host lane."""
    return f"fleet-rollout:{rollout_id}:{host}"


def _positive_env_seconds(name: str) -> float | None:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return None
    try:
        value = float(raw)
    except ValueError:
        return None
    return value if value > 0 else None


def _full_rollout_hold_duration_seconds(
    actions: Sequence[RolloutAction],
) -> int:
    """Return a bounded full-drain horizon for serialized local actions.

    Each delegated update has an outer SSH cap, while a localhost update has
    the update-script cap plus unfenced activation and verification work.  A
    host's full drain must cover the sum, not just one cold build.  Keep one
    final control-hour for journal writes and exact release, and retain the
    historical six-hour floor so a process death still has a predictable
    automatic recovery bound.
    """
    update_count = max(
        1,
        sum(action.decision == "update" for action in actions),
    )
    script_timeout = admin_module._update_script_timeout()
    delegated_timeout = _positive_env_seconds(
        "VQ_REMOTE_ADMIN_UPDATE_TIMEOUT"
    )
    if delegated_timeout is None:
        delegated_timeout = max(
            transport.DEFAULT_REMOTE_ADMIN_UPDATE_TIMEOUT_SECONDS,
            script_timeout + _DELEGATED_UPDATE_CLEANUP_MARGIN_SECONDS,
        )
    action_budget = max(script_timeout, delegated_timeout)
    serialized_budget = math.ceil(
        update_count * action_budget + ROLLOUT_HOLD_CONTROL_MARGIN_SECONDS
    )
    return max(ROLLOUT_HOLD_MIN_DURATION_SECONDS, serialized_budget)


def _journaled_exact_owner_holds(
    run: RolloutRun,
) -> dict[str, dict[str, Any]]:
    """Recover scheduler and local claims releasable by exact identity.

    A persisted ``active`` claim can survive process death, while a
    ``cleanup-failed`` claim may mean that the release response was lost.  In
    both cases the deterministic owner key makes another release safe and
    idempotent.  Do not infer ownership for legacy or malformed records.
    """
    recovered: dict[str, dict[str, Any]] = {}
    for host, value in run.holds.items():
        if not isinstance(value, dict):
            continue
        if value.get("owned") is not True:
            continue
        if value.get("status") not in {"active", "cleanup-failed"}:
            continue
        if value.get("host") != host:
            continue
        if value.get("kind") == "scheduler-target":
            if value.get("lease_owner") != _hold_lease_owner(
                run.rollout_id, host
            ):
                continue
        elif value.get("kind") == "full":
            if (
                value.get("acquire_unconfirmed") is True
                or value.get("reason") != _hold_reason(run.rollout_id, host)
                or not isinstance(value.get("set_at"), str)
            ):
                continue
        else:
            continue
        recovered[host] = dict(value)
    return recovered


def _journaled_legacy_owned_hosts(run: RolloutRun) -> set[str]:
    """Return pre-lease scheduler holds that this rollout must migrate."""
    recovered: set[str] = set()
    for host, value in run.holds.items():
        if not isinstance(value, dict):
            continue
        if value.get("kind") != "scheduler-target":
            continue
        if value.get("owned") is not True:
            continue
        if value.get("status") not in {"active", "cleanup-failed"}:
            continue
        if value.get("host") != host:
            continue
        if value.get("lease_owner") is not None:
            continue
        if value.get("action_host") is not None:
            continue
        if value.get("reason") != _hold_reason(run.rollout_id, host):
            continue
        recovered.add(host)
    return recovered


def _journaled_scheduler_holds_missing_control_host(
    run: RolloutRun,
) -> set[str]:
    """Return exact pre-R4a leases that still need a trusted driver binding."""
    return {
        host
        for host, value in _journaled_exact_owner_holds(run).items()
        if value.get("kind") == "scheduler-target"
        and (
            not isinstance(value.get("control_host"), str)
            or not str(value["control_host"]).strip()
        )
    }


def _journaled_scheduler_holds_requiring_plan(
    run: RolloutRun,
) -> set[str]:
    """Return explicit scheduler bindings only a current plan can validate."""
    return {
        host
        for host, value in run.holds.items()
        if isinstance(value, Mapping)
        and value.get("kind") == "scheduler-target"
        and value.get("status") in {"active", "cleanup-failed"}
        and "action_host" in value
    }


def _journaled_owner_hosts_requiring_validation(run: RolloutRun) -> set[str]:
    """Return exact owners that must be live-validated before cleanup.

    An unconfirmed acquire may or may not have committed. A confirmed owner
    with a pending legacy migration may have been removed independently after
    the journal was written. In both cases, reacquiring the deterministic
    owner is idempotent and ensures the legacy lane is never removed without a
    replacement hold being live.
    """
    recovered: set[str] = set()
    for host, value in run.holds.items():
        if not isinstance(value, dict):
            continue
        if value.get("kind") != "scheduler-target":
            continue
        if value.get("owned") is not True:
            continue
        if value.get("status") not in {"active", "cleanup-failed"}:
            continue
        if value.get("host") != host:
            continue
        if value.get("lease_owner") != _hold_lease_owner(run.rollout_id, host):
            continue
        if (
            value.get("acquire_unconfirmed") is not True
            and value.get("legacy_migration_pending") is not True
        ):
            continue
        recovered.add(host)
    return recovered


def _external_scheduler_holds(
    status: Mapping[str, Any],
    *,
    host: str,
    rollout_owner: str,
    rollout_owns_legacy: bool,
) -> list[dict[str, Any]]:
    """Snapshot independently owned claims that this rollout will preserve."""
    external: list[dict[str, Any]] = []
    raw_leases = status.get("scheduler_leases")
    leases = raw_leases if isinstance(raw_leases, list) else []
    for item in leases:
        if not isinstance(item, Mapping):
            continue
        if item.get("scheduler_host") != host:
            continue
        if item.get("owner") == rollout_owner:
            continue
        external.append(
            {
                "host": host,
                "kind": "scheduler-target",
                "lease_id": item.get("lease_id"),
                "owner": item.get("owner"),
                "reason": item.get("reason"),
            }
        )

    state = status.get("state")
    state_map = state if isinstance(state, Mapping) else {}
    legacy_hosts = {
        str(value)
        for value in status.get("legacy_scheduler_hosts", [])
        if isinstance(value, str)
    }
    if host in legacy_hosts and not rollout_owns_legacy:
        external.append(
            {
                "host": host,
                "kind": "scheduler-target",
                "lease_id": None,
                "owner": "legacy",
                "reason": state_map.get("reason"),
            }
        )

    if bool(status.get("is_full_drain")):
        external.append(
            {
                "host": host,
                "kind": "full",
                "lease_id": None,
                "owner": None,
                "reason": state_map.get("reason"),
            }
        )

    scheduler_hosts = {
        str(value)
        for value in status.get("scheduler_hosts", [])
        if isinstance(value, str)
    }
    if (
        host in scheduler_hosts
        and not any(
            item.get("kind") == "scheduler-target" for item in external
        )
        and not rollout_owns_legacy
        and not any(
            isinstance(item, Mapping)
            and item.get("scheduler_host") == host
            and item.get("owner") == rollout_owner
            for item in leases
        )
    ):
        # Mixed-version status may expose only the effective target union.
        # Preserve the fact and its best available reason rather than silently
        # dropping it from the final report.
        external.append(
            {
                "host": host,
                "kind": "scheduler-target",
                "lease_id": None,
                "owner": None,
                "reason": state_map.get("reason"),
            }
        )
    return external


def _status_has_only_scheduler_sidecar_policy(
    status: Mapping[str, Any],
    state: Mapping[str, Any],
) -> bool:
    """Return whether active dispatch policy comes only from lease sidecars.

    A sidecar scheduler-target lease can coexist with a separately acquired
    full-host rollout hold.  Require the complete current status shape before
    making that distinction; mixed-version or malformed status remains an
    operator partial drain and therefore fails closed.
    """
    required_state = {
        "enabled",
        "max_jobs",
        "max_cpus",
        "scheduler_hosts",
        "full_dispatch",
        "reject_submits",
        "update_mode",
    }
    if (
        status.get("active") is not True
        or status.get("mode") != "scheduler-target"
        or status.get("is_full_drain") is not False
        or status.get("is_scheduler_target_drain") is not True
        or status.get("submit_policy") != "accept_pending"
        or "scheduler_leases_error" not in status
        or status.get("scheduler_leases_error") is not None
        or not required_state.issubset(state)
        or state.get("enabled") is not True
        or state.get("max_jobs") is not None
        or state.get("max_cpus") is not None
        or state.get("full_dispatch") is not False
        or state.get("reject_submits") is not False
        or state.get("update_mode") is not None
    ):
        return False

    raw_hosts = status.get("scheduler_hosts")
    state_hosts = state.get("scheduler_hosts")
    legacy_hosts = status.get("legacy_scheduler_hosts")
    leases = status.get("scheduler_leases")
    if (
        not isinstance(raw_hosts, list)
        or not raw_hosts
        or not isinstance(state_hosts, list)
        or not isinstance(legacy_hosts, list)
        or legacy_hosts
        or not isinstance(leases, list)
        or not leases
    ):
        return False
    if any(
        not isinstance(host, str) or not host or host != host.strip()
        for host in [*raw_hosts, *state_hosts]
    ):
        return False
    hosts = set(raw_hosts)
    if len(hosts) != len(raw_hosts) or hosts != set(state_hosts):
        return False

    lease_ids: set[str] = set()
    lease_owners: set[tuple[str, str]] = set()
    lease_hosts: set[str] = set()
    for lease in leases:
        if not isinstance(lease, Mapping):
            return False
        lease_id = lease.get("lease_id")
        scheduler_host = lease.get("scheduler_host")
        owner = lease.get("owner")
        set_at = lease.get("set_at")
        if any(
            not isinstance(value, str)
            or not value
            or value != value.strip()
            for value in (lease_id, scheduler_host, owner, set_at)
        ):
            return False
        identity = (scheduler_host, owner)
        if lease_id in lease_ids or identity in lease_owners:
            return False
        lease_ids.add(lease_id)
        lease_owners.add(identity)
        lease_hosts.add(scheduler_host)
    return hosts == lease_hosts


def acquire_rollout_hold(
    plan: RolloutPlan,
    run: RolloutRun,
    *,
    host: str,
    actions: Sequence[RolloutAction],
    runner: Runner,
    action_host: str | None = None,
    control_host: str | None = None,
) -> dict[str, Any]:
    """Hold one canonical host across all of its update actions.

    Per-action admin markers intentionally clear after each helper/runtime
    update. The outer daemon drain closes that handoff window without
    weakening the live-updater guard or disturbing already-running jobs.
    """
    scheduler = _hold_is_scheduler(actions)
    protected_host = action_host or host
    status_host = control_host or (plan.driver if scheduler else host)
    status = _control_json(
        ["drain", "--status", "--json", status_host],
        runner=runner,
    )
    state = status.get("state")
    state_map = state if isinstance(state, dict) else {}
    legacy_expected_reason = state_map.get("reason")
    legacy_expected_set_at = state_map.get("set_at")
    scheduler_hosts = {
        str(value)
        for value in status.get("scheduler_hosts", [])
        if isinstance(value, str)
    }
    legacy_scheduler_hosts = {
        str(value)
        for value in status.get("legacy_scheduler_hosts", [])
        if isinstance(value, str)
    }
    reason = _hold_reason(run.rollout_id, host)
    lease_owner = _hold_lease_owner(run.rollout_id, host)
    scheduler_leases = status.get("scheduler_leases", [])
    exact_scheduler_lease = any(
        isinstance(item, dict)
        and item.get("scheduler_host") == host
        and item.get("owner") == lease_owner
        for item in scheduler_leases
    ) if isinstance(scheduler_leases, list) else False
    previous = run.holds.get(host)
    previously_owned = bool(
        isinstance(previous, dict) and previous.get("owned") is True
    )
    legacy_owned = bool(
        scheduler
        and previously_owned
        and isinstance(previous, dict)
        and (
            previous.get("lease_owner") is None
            or previous.get("legacy_migration_pending") is True
        )
        and host in legacy_scheduler_hosts
        and state_map.get("reason") == reason
        and isinstance(legacy_expected_set_at, str)
    )
    external_holds: list[dict[str, Any]] = []
    external_reason: str | None = None

    if scheduler:
        external_holds = _external_scheduler_holds(
            status,
            host=host,
            rollout_owner=lease_owner,
            rollout_owns_legacy=legacy_owned,
        )
        already_held = bool(status.get("is_full_drain")) or host in scheduler_hosts
        owned = exact_scheduler_lease
        kind = "scheduler-target"
        # Journal the deterministic owner before the mutation. A kill or lost
        # SSH response after the remote daemon commits can otherwise leave an
        # owner-scoped lease that no resumed process knows it owns. The
        # unconfirmed marker prevents cleanup from removing a legacy lane until
        # the replacement lease has been observed or acquired successfully.
        provisional = {
            "host": host,
            "action_host": protected_host,
            "kind": "scheduler-target",
            "owned": True,
            "status": "active",
            "reason": reason,
            "preexisting": already_held,
            "lease_owner": lease_owner,
            "control_host": status_host,
            "legacy_migrated": False,
            "legacy_migration_pending": legacy_owned,
        }
        if legacy_owned:
            provisional["legacy_expected_reason"] = legacy_expected_reason
            provisional["legacy_expected_set_at"] = legacy_expected_set_at
        if not owned:
            provisional["acquire_unconfirmed"] = True
        run.holds[host] = provisional
        save_run(run)
        if not owned:
            proc = _run_control(
                [
                    "drain",
                    "--scheduler-host",
                    host,
                    "--reason",
                    reason,
                    "--lease-owner",
                    lease_owner,
                    status_host,
                ],
                runner=runner,
            )
            if proc.returncode != 0:
                detail = (proc.stderr or proc.stdout or "(no output)").strip()
                raise FleetRolloutError(
                    f"could not acquire rollout hold for {host}: {detail}"
                )
            owned = True
            provisional.pop("acquire_unconfirmed", None)
            run.holds[host] = provisional
            save_run(run)
        # Persist ownership before any legacy cleanup. If the new claim
        # commits and the following migration step fails, a resumed process
        # still has the exact owner key needed to release it safely.
        run.holds[host] = dict(provisional)
        save_run(run)
        if legacy_owned:
            migration_argv = [
                "drain",
                "--release",
                "--scheduler-host",
                host,
                "--release-legacy-only",
                "--expected-legacy-set-at",
                str(legacy_expected_set_at),
            ]
            if legacy_expected_reason is not None:
                migration_argv.extend(
                    ["--expected-legacy-reason", str(legacy_expected_reason)]
                )
            migration_argv.append(status_host)
            proc = _run_control(
                migration_argv,
                runner=runner,
            )
            if proc.returncode != 0:
                detail = (proc.stderr or proc.stdout or "(no output)").strip()
                raise FleetRolloutError(
                    f"could not migrate legacy rollout hold for {host}: {detail}"
                )
    else:
        active = bool(status.get("active"))
        full = bool(status.get("is_full_drain"))
        scheduler_sidecar_only = _status_has_only_scheduler_sidecar_policy(
            status,
            state_map,
        )
        full_active = active and full
        existing_reason = state_map.get("reason")
        existing_set_at = state_map.get("set_at")
        owned = (
            previously_owned
            and full_active
            and existing_reason == reason
            and isinstance(existing_set_at, str)
        )
        kind = "full"
        full_hold_duration_seconds = _full_rollout_hold_duration_seconds(
            actions
        )
        if active and not full and not scheduler_sidecar_only:
            raise FleetRolloutError(
                f"{host} has a pre-existing partial drain; refusing to "
                "overwrite it for rollout-latest"
            )
        recovering_unconfirmed = bool(
            owned
            and isinstance(previous, Mapping)
            and previous.get("acquire_unconfirmed") is True
        )
        if recovering_unconfirmed:
            # A lost acquire response is resolved by the exact live pair. Do
            # not refresh the drain and create a second set_at identity.
            provisional = dict(previous)
            provisional.pop("acquire_unconfirmed", None)
            provisional["set_at"] = existing_set_at
            provisional["control_host"] = status_host
            run.holds[host] = provisional
            save_run(run)
        elif not full_active or owned:
            # Rewriting our own active hold refreshes its countdown on resume.
            # A different full drain is borrowed and never overwritten.
            provisional = {
                "host": host,
                "kind": kind,
                "owned": True,
                "status": "active",
                "reason": reason,
                "preexisting": full_active,
                "duration_seconds": full_hold_duration_seconds,
                "acquire_unconfirmed": True,
                "control_host": status_host,
            }
            run.holds[host] = provisional
            save_run(run)
            proc = _run_control(
                [
                    "drain",
                    "--update-mode",
                    "accept",
                    "--reason",
                    reason,
                    "--duration",
                    f"{full_hold_duration_seconds}s",
                    host,
                ],
                runner=runner,
            )
            if proc.returncode != 0:
                detail = (proc.stderr or proc.stdout or "(no output)").strip()
                raise FleetRolloutError(
                    f"could not acquire rollout hold for {host}: {detail}"
                )
            confirmed = _control_json(
                ["drain", "--status", "--json", status_host],
                runner=runner,
            )
            confirmed_state = confirmed.get("state")
            confirmed_map = (
                confirmed_state if isinstance(confirmed_state, dict) else {}
            )
            confirmed_set_at = confirmed_map.get("set_at")
            if (
                not bool(confirmed.get("active"))
                or not bool(confirmed.get("is_full_drain"))
                or confirmed_map.get("reason") != reason
                or not isinstance(confirmed_set_at, str)
            ):
                raise FleetRolloutError(
                    f"could not confirm exact rollout hold identity for {host}; "
                    "leaving the provisional hold active for reconciliation"
                )
            owned = True
            existing_set_at = confirmed_set_at
            provisional.pop("acquire_unconfirmed", None)
            provisional["set_at"] = confirmed_set_at
            run.holds[host] = provisional
            save_run(run)
        elif full_active:
            external_reason = (
                existing_reason if isinstance(existing_reason, str) else None
            )
            external_holds = [
                {
                    "host": host,
                    "kind": "full",
                    "lease_id": None,
                    "owner": None,
                    "reason": external_reason,
                }
            ]

    record = {
        "host": host,
        "kind": kind,
        "owned": owned,
        "status": "active",
        "reason": reason,
        "preexisting": already_held if scheduler else not owned,
    }
    if scheduler:
        record["lease_owner"] = lease_owner
        record["control_host"] = status_host
        record["legacy_migrated"] = legacy_owned
        record["legacy_migration_pending"] = False
        record["action_host"] = protected_host
    elif owned:
        record["duration_seconds"] = full_hold_duration_seconds
        record["set_at"] = existing_set_at
        record["control_host"] = status_host
    if external_holds:
        record["external_holds"] = external_holds
    if external_reason is not None:
        record["external_reason"] = external_reason
    run.holds[host] = record
    save_run(run)
    return record


def _scheduler_hold_control_for(
    plan: RolloutPlan,
    *,
    action_host: str,
    target: str,
    journaled: Mapping[str, Any] | None = None,
) -> str:
    if journaled is not None:
        control = journaled.get("control_host")
        if isinstance(control, str) and control:
            return control
    for candidate, control in _scheduler_hold_targets_for(plan, action_host):
        if candidate == target:
            return control
    if target == action_host:
        return plan.driver
    raise FleetRolloutError(
        f"journaled scheduler hold target {target!r} has no current-plan "
        f"alias binding for action host {action_host!r}"
    )


def _journal_scheduler_rollout_hold_group(
    plan: RolloutPlan,
    run: RolloutRun,
    *,
    action_host: str,
) -> set[str]:
    """Atomically journal every missing exact owner before group mutation."""
    seeded: set[str] = set()
    for target, control_host in _scheduler_hold_targets_for(plan, action_host):
        existing = run.holds.get(target)
        if (
            isinstance(existing, Mapping)
            and existing.get("kind") == "scheduler-target"
            and existing.get("status") in {"active", "cleanup-failed"}
        ):
            continue
        run.holds[target] = {
            "host": target,
            "action_host": action_host,
            "kind": "scheduler-target",
            "owned": True,
            "status": "active",
            "reason": _hold_reason(run.rollout_id, target),
            "preexisting": False,
            "lease_owner": _hold_lease_owner(run.rollout_id, target),
            "control_host": control_host,
            "legacy_migrated": False,
            "legacy_migration_pending": False,
            "acquire_unconfirmed": True,
        }
        seeded.add(target)
    if seeded:
        # One atomic journal replacement makes the complete immutable group
        # recoverable even if the controller dies after member one commits and
        # before member two is inspected or acquired.
        save_run(run)
    return seeded


def _acquire_scheduler_rollout_hold_group(
    plan: RolloutPlan,
    run: RolloutRun,
    *,
    action_host: str,
    actions: Sequence[RolloutAction],
    runner: Runner,
    held: dict[str, dict[str, Any]],
    validated: set[str],
) -> None:
    """Confirm every exact target before a canonical scheduler action runs."""
    _journal_scheduler_rollout_hold_group(
        plan,
        run,
        action_host=action_host,
    )
    held.update(_journaled_exact_owner_holds(run))
    for target, control_host in _scheduler_hold_targets_for(plan, action_host):
        if target in validated:
            continue
        held[target] = acquire_rollout_hold(
            plan,
            run,
            host=target,
            action_host=action_host,
            control_host=control_host,
            actions=actions,
            runner=runner,
        )
        validated.add(target)


def _held_targets_for_action(
    held: Mapping[str, Mapping[str, Any]],
    action_host: str,
) -> list[str]:
    return [
        target
        for target, hold in held.items()
        if _journaled_hold_action_host(target, hold) == action_host
    ]


def _release_rollout_hold_group(
    plan: RolloutPlan,
    run: RolloutRun,
    *,
    action_host: str,
    held: dict[str, dict[str, Any]],
    runner: Runner,
) -> dict[str, str]:
    """Release every exact target for one action lane independently."""
    errors: dict[str, str] = {}
    for target in reversed(_held_targets_for_action(held, action_host)):
        try:
            release_rollout_hold(
                plan,
                run,
                hold=held[target],
                runner=runner,
            )
            held.pop(target)
        except FleetRolloutError as exc:
            errors[target] = str(exc)
    return errors


def release_rollout_hold(
    plan: RolloutPlan,
    run: RolloutRun,
    *,
    hold: Mapping[str, Any],
    runner: Runner,
    save_state: bool = True,
) -> None:
    """Release only a hold acquired by this rollout.

    Borrowed administrator drains are deliberately left unchanged.
    """
    host = str(hold["host"])
    record = dict(hold)
    if not hold.get("owned"):
        record["status"] = "preserved"
        run.holds[host] = record
        if save_state:
            save_run(run)
        return
    if hold.get("kind") == "scheduler-target":
        control_host = _journaled_hold_control_host(
            host,
            hold,
            legacy_default=plan.driver,
        )
        if hold.get("acquire_unconfirmed") is True:
            # The mutation may have committed even though its response never
            # reached us. Exact-owner release is safe in both cases. Never
            # clear a legacy lane on this path: until the replacement was
            # confirmed, that legacy record remains the only certain hold.
            argv = [
                "drain",
                "--release",
                "--scheduler-host",
                host,
                "--lease-owner",
                str(hold["lease_owner"]),
                control_host,
            ]
            proc = _run_control(argv, runner=runner)
            if proc.returncode != 0:
                detail = (proc.stderr or proc.stdout or "(no output)").strip()
                record["status"] = "cleanup-failed"
                record["cleanup_error"] = detail
                run.holds[host] = record
                if save_state:
                    save_run(run)
                raise FleetRolloutError(
                    f"could not release rollout hold for {host}: {detail}"
                )
            record.pop("acquire_unconfirmed", None)
            record.pop("cleanup_error", None)
            if hold.get("legacy_migration_pending") is True:
                # Restore the pre-lease journal shape so a later resume can
                # retry the acquire-then-migrate sequence. The legacy lane is
                # still present and still owns dispatch safety.
                record.pop("lease_owner", None)
                record.pop("legacy_migration_pending", None)
                record["legacy_migrated"] = False
                record["status"] = "active"
            else:
                record["status"] = "released"
            run.holds[host] = record
            if save_state:
                save_run(run)
            return
        if hold.get("legacy_migration_pending") is True:
            expected_set_at = hold.get("legacy_expected_set_at")
            if not isinstance(expected_set_at, str):
                raise FleetRolloutError(
                    f"cannot migrate legacy rollout hold for {host}: "
                    "the inspected legacy set_at identity is missing"
                )
            migration_argv = [
                "drain",
                "--release",
                "--scheduler-host",
                host,
                "--release-legacy-only",
                "--expected-legacy-set-at",
                expected_set_at,
            ]
            expected_reason = hold.get("legacy_expected_reason")
            if expected_reason is not None:
                migration_argv.extend(
                    ["--expected-legacy-reason", str(expected_reason)]
                )
            migration_argv.append(control_host)
            migration = _run_control(migration_argv, runner=runner)
            if migration.returncode != 0:
                detail = (
                    migration.stderr
                    or migration.stdout
                    or "(no output)"
                ).strip()
                record["status"] = "cleanup-failed"
                record["cleanup_error"] = detail
                run.holds[host] = record
                if save_state:
                    save_run(run)
                raise FleetRolloutError(
                    f"could not migrate legacy rollout hold for {host}: "
                    f"{detail}"
                )
            # Make the migration durable while the exact owner lease is still
            # held. A process death after this save can safely resume with only
            # the owner-scoped release remaining.
            record["status"] = "active"
            record["legacy_migrated"] = True
            record["legacy_migration_pending"] = False
            record.pop("cleanup_error", None)
            run.holds[host] = record
            if save_state:
                save_run(run)
        argv = [
            "drain",
            "--release",
            "--scheduler-host",
            host,
            "--lease-owner",
            str(hold["lease_owner"]),
            control_host,
        ]
    else:
        expected_reason = hold.get("reason")
        expected_set_at = hold.get("set_at")
        if hold.get("acquire_unconfirmed") is True:
            record["status"] = "active"
            run.holds[host] = record
            if save_state:
                save_run(run)
            raise FleetRolloutError(
                f"cannot exact-release unconfirmed rollout hold for {host}; "
                "reconcile its live reason and set_at first"
            )
        if not isinstance(expected_reason, str) or not isinstance(
            expected_set_at, str
        ):
            record["status"] = "active"
            run.holds[host] = record
            if save_state:
                save_run(run)
            raise FleetRolloutError(
                f"cannot exact-release rollout hold for {host}: reason/set_at "
                "identity is missing"
            )
        argv = [
            "drain",
            "--release-full",
            "--expected-full-reason",
            expected_reason,
            "--expected-full-set-at",
            expected_set_at,
            host,
        ]
    proc = _run_control(argv, runner=runner)
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "(no output)").strip()
        record["status"] = "cleanup-failed"
        record["cleanup_error"] = detail
        run.holds[host] = record
        if save_state:
            save_run(run)
        raise FleetRolloutError(
            f"could not release rollout hold for {host}: {detail}"
        )
    record["status"] = "released"
    run.holds[host] = record
    if save_state:
        save_run(run)


def rollout_id(report: fleet_release.FleetReleaseReport) -> str:
    """Stable journal key for one immutable accepted report."""
    return f"{report.release.tag}-{report.digest_sha256[:12]}"


def _load_or_create_run(plan: RolloutPlan, rollout_id: str) -> RolloutRun:
    run = load_run(rollout_id) or RolloutRun(
        rollout_id=rollout_id,
        report_digest_sha256=str(plan.report["digest_sha256"]),
        report_source_path=str(plan.report["source_path"]),
    )
    if run.report_digest_sha256 != plan.report["digest_sha256"]:
        raise FleetRolloutError(
            "resume report digest differs from persisted rollout state"
        )
    return run


def _validate_current_report(
    plan: RolloutPlan,
    resolver: ReportDigestResolver | None,
) -> None:
    """Reject a plan whose accepted report changed while it waited to run."""
    if resolver is None:
        return
    current_digest = resolver()
    planned_digest = str(plan.report["digest_sha256"])
    if current_digest != planned_digest:
        raise FleetRolloutError(
            "accepted fleet report changed after planning; refusing to "
            f"execute stale report {planned_digest[:12]} now that "
            f"{current_digest[:12]} is current"
        )


def _operation_identity(
    rollout_id: str,
    report_digest_sha256: str,
    action: RolloutAction,
    *,
    attempt: int,
    lifecycle_resources: tuple[tuple[str, str], ...] = (),
    require_rollout_lock: bool = False,
) -> fleet_operation.OperationIdentity:
    return fleet_operation.OperationIdentity(
        rollout_id=rollout_id,
        report_digest_sha256=report_digest_sha256,
        attempt=attempt,
        action_id=action.id,
        phase=action.phase,
        host=action.host,
        program=action.program,
        pin_name=action.pin_name,
        target_sha=action.target_sha,
        target_version=action.target_version,
        target_tag=action.target_tag,
        argv=tuple(action.argv),
        lifecycle_resources=lifecycle_resources,
        rollout_lock_path=(
            str(rollout_lock_path(rollout_id))
            if require_rollout_lock else None
        ),
    )


def _identity_from_observation(
    observed: fleet_operation.OperationObservation,
) -> fleet_operation.OperationIdentity:
    return observed.identity


def _attempt_ref(
    observed: fleet_operation.OperationObservation,
    identity: fleet_operation.OperationIdentity,
    *,
    request_sha256: str,
    harvested: bool = False,
    failure_fence_consumed: bool = False,
    failure_retry_authorized: bool = False,
) -> dict[str, Any]:
    return {
        "operation_id": observed.operation_id,
        "request_sha256": request_sha256,
        "attempt": identity.attempt,
        "report_digest_sha256": identity.report_digest_sha256,
        "identity": identity.as_dict(),
        "state": observed.state,
        "retry_safe": observed.retry_safe,
        "harvested": harvested,
        "failure_fence_consumed": failure_fence_consumed,
        "failure_retry_authorized": failure_retry_authorized,
    }


def _request_sha256(observed: fleet_operation.OperationObservation) -> str:
    return observed.request_sha256


def _upsert_operation_ref(
    run: RolloutRun,
    identity: fleet_operation.OperationIdentity,
    observed: fleet_operation.OperationObservation,
    *,
    request_sha256: str,
    harvested: bool | None = None,
) -> dict[str, Any]:
    current = run.actions.get(identity.action_id)
    attempts = _operation_attempts(current)
    existing: dict[str, Any] | None = None
    for item in attempts:
        if item.get("operation_id") == observed.operation_id:
            existing = item
            break
    value = _attempt_ref(
        observed,
        identity,
        request_sha256=request_sha256,
        harvested=(
            bool(existing and existing.get("harvested"))
            if harvested is None
            else harvested
        ),
        failure_fence_consumed=bool(
            existing and existing.get("failure_fence_consumed")
        ),
        failure_retry_authorized=bool(
            existing and existing.get("failure_retry_authorized")
        ),
    )
    if existing is None:
        attempts.append(value)
    else:
        existing.clear()
        existing.update(value)
    base = dict(current) if isinstance(current, Mapping) else {
        "decision": "update",
        "reason": "recovered durable operation",
        "status": "running",
        "argv": list(identity.argv),
        "target_sha": identity.target_sha,
        "target_version": identity.target_version,
        "target_tag": identity.target_tag,
    }
    base["operation_attempts"] = attempts
    _replace_action_record(run, identity.action_id, base)
    return value


def _set_failure_fence_state(
    run: RolloutRun,
    *,
    action_id: str,
    operation_id: str,
    consumed: bool,
    retry_authorized: bool,
    block_reason: str | None = None,
) -> bool:
    """Update one strict attempt marker through the sole replacement helper."""
    current = run.actions.get(action_id)
    attempts = _operation_attempts(current)
    changed = False
    found = False
    for item in attempts:
        if item["operation_id"] != operation_id:
            continue
        found = True
        if (
            item["failure_fence_consumed"] != consumed
            or item["failure_retry_authorized"] != retry_authorized
        ):
            item["failure_fence_consumed"] = consumed
            item["failure_retry_authorized"] = retry_authorized
            changed = True
        break
    if not found:
        raise FleetRolloutError(
            f"durable failure fence references missing operation {operation_id}"
        )
    if changed or block_reason is not None:
        base = dict(current) if isinstance(current, Mapping) else {}
        base["operation_attempts"] = attempts
        if block_reason is not None:
            base["reconciled_failure"] = block_reason
        _replace_action_record(run, action_id, base)
    return changed


def _verified_failed_attempt(
    run: RolloutRun,
    *,
    action_id: str,
    attempt: Mapping[str, Any],
    expected_host: str,
) -> bool:
    """Prove a referenced attempt is the exact terminal executed failure."""
    operation = str(attempt["operation_id"])
    try:
        observed = fleet_operation.observe_operation(operation)
    except fleet_operation.OperationError as exc:
        raise FleetRolloutError(
            f"cannot consume durable failure fence {operation}: {exc}"
        ) from exc
    identity = _identity_from_observation(observed)
    if (
        identity.rollout_id != run.rollout_id
        or identity.action_id != action_id
        or identity.host != expected_host
        or identity.as_dict() != attempt["identity"]
        or _request_sha256(observed) != attempt["request_sha256"]
        or observed.state != "completed"
        or attempt["harvested"] is not True
    ):
        raise FleetRolloutError(
            f"durable failure fence {operation} no longer binds its verified "
            "terminal receipt"
        )
    result = observed.result or {}
    return bool(
        result.get("executed") is True and result.get("returncode") != 0
    )


def consume_reconciled_failure_fences(
    failures: Sequence[tuple[str, str, str]],
    *,
    current_run: RolloutRun | None = None,
    driver_block_host: str | None = None,
    mutation_guard: Callable[[], None] | None = None,
) -> RolloutRun | None:
    """Durably consume host failure fences only after a fresh-plan skip.

    Cross-report failures live in their original rollout journal. The caller
    first persists the current plan's host-local skip, then calls here. A death
    between those saves repeats the skip; it can never lose the fence.
    """
    grouped: dict[str, dict[str, str]] = {}
    for failed_rollout, host, reason in failures:
        grouped.setdefault(failed_rollout, {})[host] = reason
    returned = current_run
    for failed_rollout, host_reasons in grouped.items():
        run = (
            current_run
            if current_run is not None and current_run.rollout_id == failed_rollout
            else load_run(failed_rollout)
        )
        if run is None:
            raise FleetRolloutError(
                f"cannot consume failure fence for missing rollout "
                f"{failed_rollout}"
            )
        matched: set[str] = set()
        for action_id, raw_record in list(run.actions.items()):
            if not isinstance(raw_record, Mapping):
                continue
            for attempt in _operation_attempts(raw_record):
                identity = fleet_operation.OperationIdentity.from_dict(
                    attempt["identity"]
                )
                reason = host_reasons.get(identity.host)
                if reason is None or attempt["failure_fence_consumed"] is True:
                    continue
                if not _verified_failed_attempt(
                    run,
                    action_id=action_id,
                    attempt=attempt,
                    expected_host=identity.host,
                ):
                    continue
                if mutation_guard is not None:
                    mutation_guard()
                _set_failure_fence_state(
                    run,
                    action_id=action_id,
                    operation_id=attempt["operation_id"],
                    consumed=True,
                    retry_authorized=False,
                    block_reason=(
                        reason if identity.host == driver_block_host else None
                    ),
                )
                matched.add(identity.host)
        missing = set(host_reasons) - matched
        if missing:
            raise FleetRolloutError(
                f"could not find pending durable failure fence(s) for "
                f"{failed_rollout}: {', '.join(sorted(missing))}"
            )
        if mutation_guard is not None:
            mutation_guard()
        save_run(run)
        if current_run is not None and run.rollout_id == current_run.rollout_id:
            returned = run
    return returned


def _run_for_operation(
    identity: fleet_operation.OperationIdentity,
) -> RolloutRun:
    run = load_run(identity.rollout_id)
    if run is None:
        run = RolloutRun(
            rollout_id=identity.rollout_id,
            report_digest_sha256=identity.report_digest_sha256,
            report_source_path="(recovered durable operation)",
        )
    if run.report_digest_sha256 != identity.report_digest_sha256:
        raise FleetRolloutError(
            f"durable operation for {identity.rollout_id} disagrees with its "
            "rollout journal report digest"
        )
    return run


def _all_rollout_run_snapshots() -> tuple[_FailureJournalSnapshot, ...]:
    """Load every journal from one path-bound byte snapshot."""
    root = rollout_state_dir()
    if not root.is_dir():
        return ()
    snapshots: list[_FailureJournalSnapshot] = []
    paths = tuple(sorted(root.glob("*.json")))
    for path in paths:
        rollout_id_value = path.stem
        run = load_run(rollout_id_value)
        if run is None:
            raise FleetRolloutError(
                f"invalid rollout state {path}: journal disappeared during inventory"
            )
        if path != rollout_state_path(run.rollout_id):
            raise FleetRolloutError(
                f"invalid rollout state {path}: journal rollout_id does not match its path"
            )
        snapshots.append((path, run, _failure_token_for_loaded_run(run.rollout_id, run)))
    if tuple(sorted(root.glob("*.json"))) != paths:
        raise FleetRolloutError("rollout journal membership changed during inventory")
    return tuple(snapshots)


def _pending_failure_transition_rows(
    snapshots: Sequence[_FailureJournalSnapshot],
) -> tuple[tuple[str, str, str], ...]:
    """Strictly inventory every durable failure transaction still in flight."""
    runs = {run.rollout_id: run for _path, run, _token in snapshots}
    host_currents: dict[str, str] = {}
    source_currents: dict[tuple[str, str], str] = {}
    pending: list[tuple[str, str, str]] = []
    for _path, current, _token in snapshots:
        raw_intent = current.legacy_failure_update_intent
        if raw_intent is None:
            continue
        try:
            intent = legacy_failure_transition.parse_failure_update_intent(
                raw_intent
            )
        except legacy_failure_transition.TransitionError as exc:
            raise FleetRolloutError(
                "outstanding legacy failure transition intent is malformed"
            ) from exc
        if (
            current.rollout_id != intent.current_rollout_id
            or not fleet_release.same_report(current.report_source_path, intent.current_report_path)
            or current.report_digest_sha256
            != intent.current_report_digest_sha256
            or current.complete is not False
        ):
            raise FleetRolloutError(
                "outstanding legacy failure transition conflicts with its "
                "current journal"
            )
        prior_current = host_currents.setdefault(intent.host, current.rollout_id)
        if prior_current != current.rollout_id:
            raise FleetRolloutError(
                f"multiple outstanding legacy failure transitions name {intent.host}"
            )
        _validate_journal_records(
            current,
            recognized_action_ids=frozenset(intent.current_action_ids),
        )
        _validate_failure_current_host_state(
            current,
            host=intent.host,
            allowed_action_ids=frozenset(intent.current_action_ids),
        )
        expected_reason = (
            f"skipped: an earlier lane on {intent.host} failed "
            f"({intent.failed_host_reason})"
        )
        expected_rows = {
            action_id: {
                "decision": "update",
                "reason": expected_reason,
                "status": "not-run",
            }
            for action_id in intent.current_action_ids
        }
        if (
            {
                action_id: current.actions.get(action_id)
                for action_id in intent.current_action_ids
            }
            != expected_rows
            or current.failed_hosts.get(intent.host)
            != intent.failed_host_reason
        ):
            raise FleetRolloutError(
                "outstanding legacy failure transition current acknowledgement "
                "is inconsistent"
            )
        for member in intent.sources:
            source_key = (member.historical_rollout_id, intent.host)
            prior_source_current = source_currents.setdefault(
                source_key,
                current.rollout_id,
            )
            if prior_source_current != current.rollout_id:
                raise FleetRolloutError(
                    "legacy failure source belongs to multiple forward intents"
                )
            source = runs.get(member.historical_rollout_id)
            if source is None:
                raise FleetRolloutError(
                    f"outstanding legacy failure transition source "
                    f"{member.historical_rollout_id} is missing"
                )
            if (
                not fleet_release.same_report(
                    source.report_source_path, member.historical_report_path
                )
                or source.report_digest_sha256
                != member.historical_report_digest_sha256
                or source.complete is not False
            ):
                raise FleetRolloutError(
                    "outstanding legacy failure transition source identity changed"
                )
            _validate_journal_records(source)
            expected_operations = set(member.failed_operation_ids)
            observed_actions: set[str] = set()
            observed_operations: set[str] = set()
            backlinks: list[
                legacy_failure_transition.FailureUpdateBacklink
            ] = []
            for action_id, record in sorted(source.actions.items()):
                if not isinstance(record, Mapping):
                    continue
                raw_backlink = record.get("legacy_failure_update_ack")
                if record.get("status") != "failed" and raw_backlink is None:
                    continue
                try:
                    action_host = _failure_action_host(action_id)
                except FleetRolloutError as exc:
                    raise FleetRolloutError(
                        "outstanding legacy failure transition action identity "
                        "is malformed"
                    ) from exc
                parsed_backlink = None
                if raw_backlink is not None:
                    try:
                        parsed_backlink = (
                            legacy_failure_transition.parse_failure_update_backlink(
                                raw_backlink
                            )
                        )
                    except legacy_failure_transition.TransitionError as exc:
                        raise FleetRolloutError(
                            "outstanding legacy failure transition backlink is malformed"
                        ) from exc
                    if (
                        parsed_backlink.host != action_host
                        or action_id not in parsed_backlink.failed_action_ids
                        or not fleet_release.same_report(
                            parsed_backlink.historical_report_path, source.report_source_path
                        )
                        or parsed_backlink.historical_report_digest_sha256
                        != source.report_digest_sha256
                    ):
                        raise FleetRolloutError(
                            "outstanding legacy failure transition backlink "
                            "does not bind its source action"
                        )
                if record.get("status") != "failed":
                    if action_host == intent.host:
                        raise FleetRolloutError(
                            "outstanding legacy failure transition has an extra "
                            "same-host backlink placement"
                        )
                    continue
                row_attempts = _operation_attempts(record)
                if not row_attempts:
                    raise FleetRolloutError(
                        "outstanding legacy failure transition failed action lacks "
                        "a durable attempt"
                    )
                row_operations: set[str] = set()
                for attempt in row_attempts:
                    operation_id = attempt.get("operation_id")
                    try:
                        identity = fleet_operation.OperationIdentity.from_dict(
                            attempt["identity"]
                        )
                    except fleet_operation.OperationError as exc:
                        raise FleetRolloutError(
                            "outstanding legacy failure transition attempt "
                            "identity is malformed"
                        ) from exc
                    if (
                        identity.rollout_id != source.rollout_id
                        or identity.report_digest_sha256
                        != source.report_digest_sha256
                        or identity.action_id != action_id
                        or identity.host != action_host
                    ):
                        raise FleetRolloutError(
                            "outstanding legacy failure transition attempt "
                            "identity disagrees with its source"
                        )
                    assert isinstance(operation_id, str)
                    row_operations.add(operation_id)
                if parsed_backlink is not None and not row_operations.issubset(
                    parsed_backlink.failed_operation_ids
                ):
                    raise FleetRolloutError(
                        "outstanding legacy failure transition backlink "
                        "does not bind its source operations"
                    )
                if action_host != intent.host:
                    continue
                if any(
                    attempt.get("failure_retry_authorized") is not False
                    for attempt in row_attempts
                ):
                    raise FleetRolloutError(
                        "outstanding legacy failure transition authorized retry"
                    )
                observed_actions.add(action_id)
                observed_operations.update(row_operations)
                if parsed_backlink is not None:
                    backlinks.append(parsed_backlink)
            if observed_actions != set(member.failed_action_ids):
                raise FleetRolloutError(
                    "outstanding legacy failure transition failed actions changed"
                )
            if observed_operations != expected_operations:
                raise FleetRolloutError(
                    "outstanding legacy failure transition operation membership changed"
                )
            if backlinks and len(backlinks) != len(member.failed_action_ids):
                raise FleetRolloutError(
                    "outstanding legacy failure transition has partial backlinks"
                )
            for backlink in backlinks:
                if (
                    backlink.host != intent.host
                    or backlink.failed_action_ids != member.failed_action_ids
                    or backlink.failed_operation_ids
                    != member.failed_operation_ids
                    or not fleet_release.same_report(
                        backlink.historical_report_path, member.historical_report_path
                    )
                    or backlink.historical_report_digest_sha256
                    != member.historical_report_digest_sha256
                    or backlink.settled_hold_sha256
                    != member.settled_hold_sha256
                    or not fleet_release.same_report(
                        backlink.current_report_path, intent.current_report_path
                    )
                    or backlink.current_report_digest_sha256
                    != intent.current_report_digest_sha256
                    or backlink.current_rollout_id != intent.current_rollout_id
                    or backlink.current_action_ids != intent.current_action_ids
                    or backlink.current_actions_sha256
                    != intent.current_actions_sha256
                    or backlink.recorded_at != intent.recorded_at
                ):
                    raise FleetRolloutError(
                        "outstanding legacy failure transition backlink disagrees"
                    )
            if backlinks:
                settled_hold = source.holds.get(intent.host)
                if (
                    not isinstance(settled_hold, Mapping)
                    or settled_hold.get("host") != intent.host
                    or settled_hold.get("kind") != "full"
                    or settled_hold.get("owned") is not True
                    or settled_hold.get("status") != "released"
                    or legacy_failure_transition.canonical_json_sha256(
                        settled_hold
                    )
                    != member.settled_hold_sha256
                    or intent.host in source.legacy_retained_holds
                ):
                    raise FleetRolloutError(
                        "outstanding legacy failure transition settled hold changed"
                    )
            pending.append(
                (
                    member.historical_rollout_id,
                    intent.host,
                    member.failure_reason,
                )
            )
    return tuple(sorted(pending))


def _all_rollout_runs() -> list[RolloutRun]:
    return [run for _path, run, _token in _all_rollout_run_snapshots()]


def _validate_journal_records(
    run: RolloutRun,
    *,
    recognized_action_ids: frozenset[str] = frozenset(),
) -> None:
    """Reject corrupt action/hold containers before weakening protection."""
    if type(run.complete) is not bool:
        raise FleetRolloutError(
            f"rollout {run.rollout_id} complete flag is not a boolean"
        )
    if not isinstance(run.failed_hosts, Mapping) or any(
        not isinstance(host, str) or not isinstance(reason, str)
        for host, reason in run.failed_hosts.items()
    ):
        raise FleetRolloutError(
            f"rollout {run.rollout_id} failed_hosts record is not a string map"
        )
    if not isinstance(run.actions, Mapping):
        raise FleetRolloutError(
            f"rollout {run.rollout_id} actions record is not an object"
        )
    for action_id, record in run.actions.items():
        if not isinstance(action_id, str) or not isinstance(record, Mapping):
            raise FleetRolloutError(
                f"rollout {run.rollout_id} has malformed action record for "
                f"{action_id!r}"
            )
        status = record.get("status")
        if not isinstance(status, str) or status not in {
            "running",
            "success",
            "failed",
            "not-run",
            "superseded",
        }:
            raise FleetRolloutError(
                f"rollout {run.rollout_id} has unknown action status for "
                f"{action_id!r}: {status!r}"
            )
        attempts = _operation_attempts(record)
        recognized_action_id = action_id in recognized_action_ids
        if not recognized_action_id:
            try:
                _legacy_action_parts(action_id)
                recognized_action_id = True
            except FleetRolloutError:
                pass
        if (
            not recognized_action_id
            and not attempts
            and any(
                isinstance(candidate, Mapping)
                and candidate.get("kind") == "full"
                and candidate.get("owned") is True
                and candidate.get("status") in {"active", "cleanup-failed"}
                for candidate in (
                    run.holds.values()
                    if isinstance(run.holds, Mapping)
                    else ()
                )
            )
        ):
            raise FleetRolloutError(
                f"rollout {run.rollout_id} has unrecognized action identity "
                f"{action_id!r} protecting an active full hold"
            )
        if status == "superseded" and not _coherent_superseded_legacy_record(
            run, action_id, record
        ):
            raise FleetRolloutError(
                f"rollout {run.rollout_id} has incoherent superseded action "
                f"record for {action_id!r}"
            )
    if not isinstance(run.holds, Mapping):
        raise FleetRolloutError(
            f"rollout {run.rollout_id} holds record is not an object"
        )
    for host, hold in run.holds.items():
        if not isinstance(host, str) or not isinstance(hold, Mapping):
            raise FleetRolloutError(
                f"rollout {run.rollout_id} has malformed hold record for "
                f"{host!r}"
            )
        if (
            hold.get("host") != host
            or hold.get("kind") not in {"full", "scheduler-target"}
            or type(hold.get("owned")) is not bool
            or hold.get("status")
            not in {"active", "cleanup-failed", "preserved", "released"}
        ):
            raise FleetRolloutError(
                f"rollout {run.rollout_id} has incoherent hold record for "
                f"{host!r}"
            )


def _legacy_subtree_sha256(value: Any) -> str:
    """Digest one JSON journal subtree without weakening its exact shape."""
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _accepted_report_version(value: object) -> tuple[int, int, int] | None:
    # Both split-transition prefixes are accepted; see REPORT_DIRECTORIES.
    prefixes = "|".join(
        re.escape(directory) for directory in fleet_release.REPORT_DIRECTORIES
    )
    pattern = rf"(?:{prefixes})/" r"v([0-9]+)\.([0-9]+)\.([0-9]+)\.json"
    match = re.fullmatch(pattern, value) if isinstance(value, str) else None
    if match is None:
        return None
    major, minor, patch = match.groups()
    return int(major), int(minor), int(patch)


def _is_accepted_report_path(value: object) -> bool:
    return _accepted_report_version(value) is not None


@dataclass(frozen=True)
class _ValidatedLegacyRetention:
    """Strict durable receipts that replace a fleet-global legacy block."""

    retired_hosts: frozenset[str] = frozenset()
    action_ids: frozenset[str] = frozenset()
    hold_hosts: frozenset[str] = frozenset()
    fences: tuple[tuple[str, str, str], ...] = ()
    superseded_plan_hold_hosts: frozenset[str] = frozenset()


def _validate_plan_bound_hold_supersede_receipt(
    run: RolloutRun,
    *,
    host: str,
    hold: Mapping[str, Any],
    raw: Mapping[str, Any],
    current_report: fleet_release.FleetReleaseReport | None = None,
) -> None:
    """Validate one permanent, exact plan-bound hold coverage receipt.

    Unlike an ordinary retention receipt this record is not a host fence and
    is not rebound to every later accepted report.  It records why the
    historical journal-only hold is permanently safe to ignore: a strictly
    newer accepted plan found every current scheduler lane healthy at its
    exact target, and a supported read-only drain snapshot found the exact
    target inactive.  The historical hold remains byte-bound for audit.
    """
    fields = {
        "schema",
        "source",
        "rollout_id",
        "host",
        "action_host",
        "kind",
        "control_host",
        "historical_report_path",
        "historical_report_digest_sha256",
        "hold_record_sha256",
        "current_report_path",
        "current_report_digest_sha256",
        "current_rollout_id",
        "current_actions",
        "live_drain_inactive",
        "observed_at",
        "recorded_at",
    }
    actions = raw.get("current_actions")
    historical_version = _accepted_report_version(run.report_source_path)
    current_version = _accepted_report_version(raw.get("current_report_path"))
    current_digest = raw.get("current_report_digest_sha256")
    action_host = hold.get("action_host")
    control_host = hold.get("control_host")
    if (
        set(raw) != fields
        or raw.get("schema") != _PLAN_BOUND_HOLD_SUPERSEDE_SCHEMA
        or raw.get("source") != "plan-bound-supersede"
        or raw.get("rollout_id") != run.rollout_id
        or raw.get("host") != host
        or raw.get("action_host") != action_host
        or raw.get("kind") != "scheduler-target"
        or raw.get("control_host") != control_host
        or not fleet_release.same_report(raw.get("historical_report_path"), run.report_source_path)
        or raw.get("historical_report_digest_sha256")
        != run.report_digest_sha256
        or raw.get("hold_record_sha256") != _legacy_subtree_sha256(hold)
        or historical_version is None
        or current_version is None
        or current_version <= historical_version
        or not isinstance(current_digest, str)
        or _SOURCE_TREE_SHA256.fullmatch(current_digest) is None
        or raw.get("current_rollout_id")
        != (
            f"v{current_version[0]}.{current_version[1]}."
            f"{current_version[2]}-{current_digest[:12]}"
        )
        or raw.get("live_drain_inactive") is not True
        or not isinstance(actions, list)
        or not actions
    ):
        raise FleetRolloutError(
            f"rollout {run.rollout_id} has incoherent plan-bound hold "
            f"supersede receipt for {host!r}"
        )
    try:
        _liveness_timestamp(raw.get("observed_at"))
        _liveness_timestamp(raw.get("recorded_at"))
    except (TypeError, ValueError) as exc:
        raise FleetRolloutError(
            f"rollout {run.rollout_id} has incoherent plan-bound hold "
            f"supersede receipt for {host!r}: {exc}"
        ) from exc
    expected_actions = {
        f"helper:{action_host}": ("helper", "vibeqc-queue", "vq"),
        **{
            f"scheduler-runtime:{action_host}:{program}": (
                "scheduler-runtime",
                program,
                PROGRAM_PINS[program],
            )
            for program in SCHEDULER_PROGRAM_ORDER
        },
    }
    action_ids: list[str] = []
    for item in actions:
        expected = (
            expected_actions.get(str(item.get("action_id")))
            if isinstance(item, Mapping)
            else None
        )
        if (
            not isinstance(item, Mapping)
            or set(item)
            != {
                "action_id",
                "phase",
                "host",
                "program",
                "expected_sha",
                "observed_sha",
            }
            or not isinstance(item.get("action_id"), str)
            or expected is None
            or item.get("phase") != expected[0]
            or item.get("host") != action_host
            or item.get("program") != expected[1]
            or not isinstance(item.get("expected_sha"), str)
            or _SOURCE_SHA.fullmatch(str(item["expected_sha"])) is None
            or item.get("observed_sha") != item.get("expected_sha")
        ):
            raise FleetRolloutError(
                f"rollout {run.rollout_id} has incoherent plan-bound hold "
                f"supersede action evidence for {host!r}"
            )
        action_ids.append(str(item["action_id"]))
    if action_ids != sorted(expected_actions):
        raise FleetRolloutError(
            f"rollout {run.rollout_id} plan-bound hold supersede action "
            f"evidence is not the exact sorted scheduler set for {host!r}"
        )
    if current_report is not None:
        if (
            type(current_report) is not fleet_release.FleetReleaseReport
            or not fleet_release.same_report(current_report.source_path, raw["current_report_path"])
            or current_report.digest_sha256
            != raw["current_report_digest_sha256"]
            or rollout_id(current_report) != raw["current_rollout_id"]
        ):
            raise FleetRolloutError(
                f"rollout {run.rollout_id} plan-bound hold supersede report "
                f"identity is not authenticated for {host!r}"
            )
        for item in actions:
            assert isinstance(item, Mapping)
            expected = expected_actions[str(item["action_id"])]
            try:
                pin = current_report.pins[expected[2]]
            except KeyError as exc:
                raise FleetRolloutError(
                    f"rollout {run.rollout_id} plan-bound hold supersede "
                    f"report lacks pin {expected[2]!r} for {host!r}"
                ) from exc
            if item["expected_sha"] != pin.sha:
                raise FleetRolloutError(
                    f"rollout {run.rollout_id} plan-bound hold supersede "
                    f"action evidence is not report-pinned for {host!r}"
                )


def retained_host_audit_digest(run: RolloutRun, host: str) -> str:
    """Bind an explicit retirement to all retained evidence for one host.

    Computing a digest is read-only and does not validate or retire anything.
    The normal receipt and committed-report guards still validate this data
    before a matching operator declaration can permit an unconfigured host.
    """
    actions = {
        key: value for key, value in run.legacy_retained_actions.items()
        if isinstance(value, Mapping) and value.get("host") == host
    }
    hold = run.legacy_retained_holds.get(host)
    if not actions and hold is None:
        raise FleetRolloutError(f"rollout {run.rollout_id} has no retained evidence for {host!r}")
    return _legacy_subtree_sha256({
        "rollout_id": run.rollout_id,
        "host": host,
        "historical_report_path": run.report_source_path,
        "historical_report_digest_sha256": run.report_digest_sha256,
        "actions": {key: run.actions.get(key) for key in actions},
        "hold": run.holds.get(host),
        "retained_actions": actions,
        "retained_hold": hold,
    })


def _authenticate_host_retirement(
    run: RolloutRun, host: str, declaration: config.HostRetirement,
    *, report_repo: Path | None,
) -> None:
    expected = declaration.retained_receipts.get(run.rollout_id)
    if expected != retained_host_audit_digest(run, host):
        raise FleetRolloutError(
            f"rollout {run.rollout_id} retirement evidence changed or is missing for {host!r}"
        )
    if report_repo is None:
        raise FleetRolloutError("host retirement requires the authenticated report repository")
    _authenticate_journal_report(
        run, repo=report_repo, cache={}, subject=f"retired host {host!r}",
        binding_failure="retirement journal does not bind its committed historical report",
    )
    receipts = [raw for raw in run.legacy_retained_actions.values()
                if isinstance(raw, Mapping) and raw.get("host") == host]
    if host in run.legacy_retained_holds:
        receipts.append(run.legacy_retained_holds[host])
    for raw in receipts:
        try:
            receipt_report = fleet_release.discover_historical_report_by_digest(
                str(raw["current_report_path"]), str(raw["current_report_digest_sha256"]),
                report_repo, fetch=False,
            )
        except (KeyError, fleet_release.FleetReleaseError) as exc:
            raise FleetRolloutError("retirement receipt report could not be authenticated") from exc
        if (not fleet_release.same_report(receipt_report.source_path, raw["current_report_path"])
                or receipt_report.digest_sha256 != raw["current_report_digest_sha256"]):
            raise FleetRolloutError("retirement receipt report identity does not match")


def _validate_legacy_retention_receipts(
    run: RolloutRun,
    *,
    current_report_source_path: str | None,
    current_report_digest_sha256: str,
    allow_current_identity_mismatch: bool,
    allowed_current_identity_mismatch_hosts: frozenset[str] = frozenset(),
    configured_hosts: frozenset[str] | None = None,
    retired_hosts: Mapping[str, config.HostRetirement] | None = None,
    report_repo: Path | None = None,
) -> _ValidatedLegacyRetention:
    """Validate top-level receipts before they can narrow a global fence.

    The historical action and hold subtrees deliberately remain untouched.
    Their canonical hashes make a later edit or partial receipt a hard failure.
    A receipt is reusable only for the exact accepted report that supplied its
    current-plan observation. Explicit legacy reconciliation may reauthenticate
    and replace an older receipt. A scoped caller may instead keep an exact
    stale receipt as a host-local fence, but only for a host it explicitly
    excludes; the receipt never authorizes work or loses its historical hash.
    """
    raw_actions = run.legacy_retained_actions
    raw_holds = run.legacy_retained_holds
    if not isinstance(raw_actions, Mapping) or not isinstance(
        raw_holds, Mapping
    ):
        raise FleetRolloutError(
            f"rollout {run.rollout_id} has malformed legacy retention receipts"
        )
    if not raw_actions and not raw_holds:
        return _ValidatedLegacyRetention()
    if run.complete is not False:
        raise FleetRolloutError(
            f"rollout {run.rollout_id} has retention receipts but is marked "
            "complete"
        )
    current_report_version = _accepted_report_version(
        current_report_source_path
    )
    if current_report_version is None:
        raise FleetRolloutError(
            f"rollout {run.rollout_id} legacy retention receipts require the "
            "exact current accepted-report path"
        )

    retired: set[str] = set()
    for host, declaration in (retired_hosts or {}).items():
        if configured_hosts is not None and host in configured_hosts:
            raise FleetRolloutError(f"retired host {host!r} is still configured as active")
        relevant = host in raw_holds or any(
            isinstance(raw, Mapping) and raw.get("host") == host for raw in raw_actions.values()
        )
        if not relevant:
            continue
        # Revalidate callers that constructed a model without normal validation.
        declaration = config.HostRetirement.model_validate(declaration.model_dump())
        _authenticate_host_retirement(run, host, declaration, report_repo=report_repo)
        retired.add(host)

    action_fields = {
        "schema",
        "action_id",
        "phase",
        "host",
        "program",
        "historical_report_path",
        "historical_report_digest_sha256",
        "action_record_sha256",
        "argv_sha256",
        "durable_operation_present",
        "observed_outcome",
        "current_report_path",
        "current_report_digest_sha256",
        "current_action_reason",
        "retained_at",
        "reason",
    }
    action_ids: set[str] = set()
    action_hosts: dict[str, str] = {}
    action_fences: list[tuple[str, str, str]] = []
    identity_mismatch_hosts: set[str] = set()
    receipt_contexts: set[tuple[str, str, str]] = set()

    def record_identity_mismatch(
        raw: Mapping[str, Any],
        *,
        host: str,
        receipt_kind: str,
    ) -> None:
        path_changed = not fleet_release.same_report(
            raw["current_report_path"], current_report_source_path
        )
        digest_changed = (
            raw["current_report_digest_sha256"]
            != current_report_digest_sha256
        )
        if path_changed != digest_changed:
            raise FleetRolloutError(
                f"rollout {run.rollout_id} retained legacy {receipt_kind} "
                f"receipt has incoherent current report identity for {host!r}"
            )
        if not path_changed:
            return
        receipt_version = _accepted_report_version(raw["current_report_path"])
        assert receipt_version is not None
        if (
            not allow_current_identity_mismatch
            and receipt_version >= current_report_version
        ):
            raise FleetRolloutError(
                f"rollout {run.rollout_id} retained legacy {receipt_kind} "
                f"receipt is not from an older accepted report for {host!r}"
            )
        identity_mismatch_hosts.add(host)
    for action_id, raw in raw_actions.items():
        if not isinstance(action_id, str) or not isinstance(raw, Mapping):
            raise FleetRolloutError(
                f"rollout {run.rollout_id} has malformed retained legacy "
                f"action receipt for {action_id!r}"
            )
        record = run.actions.get(action_id)
        try:
            phase, host, program = _legacy_action_parts(action_id)
        except FleetRolloutError as exc:
            raise FleetRolloutError(
                f"rollout {run.rollout_id} retained legacy action receipt "
                f"has an unrecognized lane {action_id!r}"
            ) from exc
        if (
            set(raw) != action_fields
            or not isinstance(record, Mapping)
            or record.get("status") != "running"
            or bool(_operation_attempts(record))
            or raw.get("schema") != _LEGACY_RETAINED_ACTION_SCHEMA
            or raw.get("action_id") != action_id
            or raw.get("phase") != phase
            or raw.get("host") != host
            or raw.get("program") != program
            or not fleet_release.same_report(
                raw.get("historical_report_path"), run.report_source_path
            )
            or raw.get("historical_report_digest_sha256")
            != run.report_digest_sha256
            or raw.get("action_record_sha256")
            != _legacy_subtree_sha256(record)
            or raw.get("argv_sha256")
            != _legacy_subtree_sha256(record.get("argv"))
            or raw.get("durable_operation_present") is not False
            or raw.get("observed_outcome") != "unknown"
            or not _is_accepted_report_path(raw.get("current_report_path"))
            or not isinstance(raw.get("current_report_digest_sha256"), str)
            or _SOURCE_TREE_SHA256.fullmatch(
                str(raw.get("current_report_digest_sha256"))
            )
            is None
            or not isinstance(raw.get("current_action_reason"), str)
            or not str(raw.get("current_action_reason")).strip()
            or not isinstance(raw.get("retained_at"), str)
            or not str(raw.get("retained_at")).strip()
            or not isinstance(raw.get("reason"), str)
            or not str(raw.get("reason")).strip()
        ):
            raise FleetRolloutError(
                f"rollout {run.rollout_id} has incoherent retained legacy "
                f"action receipt for {action_id!r}"
            )
        if (configured_hosts is not None and host not in configured_hosts
                    and host not in retired):
            raise FleetRolloutError(
                f"rollout {run.rollout_id} retained legacy action receipt "
                f"names unconfigured host {host!r}"
            )
        receipt_contexts.add(
            (
                str(raw["current_report_path"]),
                str(raw["current_report_digest_sha256"]),
                str(raw["retained_at"]),
            )
        )
        record_identity_mismatch(raw, host=host, receipt_kind="action")
        action_ids.add(action_id)
        action_hosts[action_id] = host
        action_fences.append(
            (run.rollout_id, host, f"{action_id}: {raw['reason']}")
        )

    hold_fields = {
        "schema",
        "host",
        "kind",
        "source",
        "historical_report_path",
        "historical_report_digest_sha256",
        "hold_record_sha256",
        "current_report_path",
        "current_report_digest_sha256",
        "control_host",
        "observed_outcome",
        "retained_at",
        "reason",
    }
    hold_hosts: set[str] = set()
    superseded_plan_hold_hosts: set[str] = set()
    hold_fences: list[tuple[str, str, str]] = []
    for host, raw in raw_holds.items():
        if not isinstance(host, str) or not isinstance(raw, Mapping):
            raise FleetRolloutError(
                f"rollout {run.rollout_id} has malformed retained legacy "
                f"hold receipt for {host!r}"
            )
        hold = run.holds.get(host)
        if raw.get("schema") == _PLAN_BOUND_HOLD_SUPERSEDE_SCHEMA:
            if not isinstance(hold, Mapping):
                raise FleetRolloutError(
                    f"rollout {run.rollout_id} plan-bound hold supersede "
                    f"receipt has no historical hold for {host!r}"
                )
            _validate_plan_bound_hold_supersede_receipt(
                run,
                host=host,
                hold=hold,
                raw=raw,
            )
            if report_repo is None:
                raise FleetRolloutError(
                    f"rollout {run.rollout_id} plan-bound hold supersede "
                    "receipt requires its authenticated report repository"
                )
            try:
                receipt_report = (
                    fleet_release.discover_historical_report_by_digest(
                        str(raw["current_report_path"]),
                        str(raw["current_report_digest_sha256"]),
                        report_repo,
                        fetch=False,
                    )
                )
            except (KeyError, fleet_release.FleetReleaseError) as exc:
                raise FleetRolloutError(
                    f"rollout {run.rollout_id} plan-bound hold supersede "
                    f"receipt report could not be authenticated for {host!r}"
                ) from exc
            _validate_plan_bound_hold_supersede_receipt(
                run,
                host=host,
                hold=hold,
                raw=raw,
                current_report=receipt_report,
            )
            if (configured_hosts is not None and host not in configured_hosts
                    and host not in retired):
                raise FleetRolloutError(
                    f"rollout {run.rollout_id} plan-bound hold supersede "
                    f"receipt names unconfigured host {host!r}"
                )
            hold_hosts.add(host)
            superseded_plan_hold_hosts.add(host)
            continue
        source = raw.get("source")
        control_host = raw.get("control_host")
        if (
            set(raw) != hold_fields
            or not isinstance(hold, Mapping)
            or hold.get("status") not in {"active", "cleanup-failed"}
            or hold.get("owned") is not True
            or raw.get("schema") != _LEGACY_RETAINED_HOLD_SCHEMA
            or raw.get("host") != host
            or raw.get("kind") != hold.get("kind")
            or source
            not in {
                "retained-action-group",
                "scheduler-observation",
                "hold-observation",
            }
            or not fleet_release.same_report(
                raw.get("historical_report_path"), run.report_source_path
            )
            or raw.get("historical_report_digest_sha256")
            != run.report_digest_sha256
            or raw.get("hold_record_sha256")
            != _legacy_subtree_sha256(hold)
            or not _is_accepted_report_path(raw.get("current_report_path"))
            or not isinstance(raw.get("current_report_digest_sha256"), str)
            or _SOURCE_TREE_SHA256.fullmatch(
                str(raw.get("current_report_digest_sha256"))
            )
            is None
            or raw.get("observed_outcome") != "unknown"
            or not isinstance(raw.get("retained_at"), str)
            or not str(raw.get("retained_at")).strip()
            or not isinstance(raw.get("reason"), str)
            or not str(raw.get("reason")).strip()
            or (
                source == "retained-action-group"
                and (not raw_actions or control_host is not None)
            )
            or (
                source == "scheduler-observation"
                and (
                    hold.get("kind") != "scheduler-target"
                    or not isinstance(control_host, str)
                    or not control_host.strip()
                )
            )
            or (
                source == "hold-observation"
                and (
                    not isinstance(control_host, str)
                    or not control_host.strip()
                )
            )
        ):
            raise FleetRolloutError(
                f"rollout {run.rollout_id} has incoherent retained legacy "
                f"hold receipt for {host!r}"
            )
        if (configured_hosts is not None and host not in configured_hosts
                    and host not in retired):
            raise FleetRolloutError(
                f"rollout {run.rollout_id} retained legacy hold receipt "
                f"names unconfigured host {host!r}"
            )
        if (
            source == "hold-observation"
            and hold.get("kind") == "full"
            and control_host != host
        ) or (
            control_host is not None
            and configured_hosts is not None
            and control_host not in configured_hosts
            and not (host in retired and control_host == host)
        ):
            raise FleetRolloutError(
                f"rollout {run.rollout_id} retained legacy hold receipt "
                f"has incoherent control host for {host!r}"
            )
        receipt_contexts.add(
            (
                str(raw["current_report_path"]),
                str(raw["current_report_digest_sha256"]),
                str(raw["retained_at"]),
            )
        )
        record_identity_mismatch(raw, host=host, receipt_kind="hold")
        hold_hosts.add(host)
        hold_fences.append((run.rollout_id, host, str(raw["reason"])))

    running_legacy_ids = {
        action_id
        for action_id, record in run.actions.items()
        if isinstance(record, Mapping)
        and record.get("status") == "running"
        and not _operation_attempts(record)
    }
    if action_ids and action_ids != running_legacy_ids:
        raise FleetRolloutError(
            f"rollout {run.rollout_id} retained action receipt set is partial"
        )
    action_group_hold_hosts = {
        host
        for host, hold in run.holds.items()
        if isinstance(hold, Mapping)
        and hold.get("owned") is True
        and hold.get("status") in {"active", "cleanup-failed"}
    }
    if action_ids and not action_group_hold_hosts.issubset(hold_hosts):
        raise FleetRolloutError(
            f"rollout {run.rollout_id} retained action group does not bind "
            "every active owned hold"
        )
    if action_ids and (
        len(receipt_contexts) != 1
        or any(
            receipt.get("source") != "retained-action-group"
            for receipt in raw_holds.values()
        )
    ):
        raise FleetRolloutError(
            f"rollout {run.rollout_id} retained legacy action group has "
            "incoherent receipt context"
        )
    disallowed_mismatch_hosts = (
        identity_mismatch_hosts - allowed_current_identity_mismatch_hosts - retired
    )
    if disallowed_mismatch_hosts and not allow_current_identity_mismatch:
        raise FleetRolloutError(
            f"rollout {run.rollout_id} legacy retention receipts belong to a "
            "different accepted report; run --reconcile-legacy explicitly"
        )
    return _ValidatedLegacyRetention(
        retired_hosts=frozenset(retired),
        action_ids=frozenset(action_ids),
        hold_hosts=frozenset(hold_hosts),
        fences=tuple(dict.fromkeys((*action_fences, *hold_fences))),
        superseded_plan_hold_hosts=frozenset(superseded_plan_hold_hosts),
    )


@dataclass(frozen=True)
class _LegacyJournalInventory:
    retired_hold_refs: frozenset[tuple[str, str]] = frozenset()
    running_actions: tuple[tuple[str, str], ...] = ()
    retained_fences: tuple[tuple[str, str, str], ...] = ()
    retained_hold_refs: frozenset[tuple[str, str]] = frozenset()
    superseded_plan_hold_refs: frozenset[tuple[str, str]] = frozenset()


def _validate_running_journals(
    observations: Mapping[str, fleet_operation.OperationObservation],
    *,
    current_report_source_path: str | None,
    current_report_digest_sha256: str,
    allow_legacy_reconciliation: bool = False,
    allowed_retention_identity_mismatch_hosts: frozenset[str] = frozenset(),
    configured_retention_hosts: frozenset[str] | None = None,
    retired_hosts: Mapping[str, config.HostRetirement] | None = None,
    report_repo: Path | None = None,
) -> _LegacyJournalInventory:
    """Validate durable references and inventory pre-recorder actions.

    A normal or read-only invocation still rejects a pre-R4 ``running`` row.
    The explicit legacy-reconciliation path may carry the row forward to a
    fresh-plan proof, but this function never changes it and never treats it as
    completed.  An unreferenced durable operation with the same immutable
    rollout/action identity is not legacy evidence and remains a hard block.
    """
    observed_identities = {
        operation: _identity_from_observation(observed)
        for operation, observed in observations.items()
    }
    legacy: list[tuple[str, str]] = []
    retained_fences: list[tuple[str, str, str]] = []
    retained_hold_refs: set[tuple[str, str]] = set()
    retired_hold_refs: set[tuple[str, str]] = set()
    superseded_plan_hold_refs: set[tuple[str, str]] = set()
    for run in _all_rollout_runs():
        _validate_journal_records(run)
        retention = _validate_legacy_retention_receipts(
            run,
            current_report_source_path=current_report_source_path,
            current_report_digest_sha256=current_report_digest_sha256,
            allow_current_identity_mismatch=allow_legacy_reconciliation,
            allowed_current_identity_mismatch_hosts=(
                allowed_retention_identity_mismatch_hosts
            ),
            configured_hosts=configured_retention_hosts,
            retired_hosts=retired_hosts,
            report_repo=report_repo,
        )
        retired_hold_refs.update(
            (run.rollout_id, host) for host in retention.hold_hosts & retention.retired_hosts
        )
        retained_hold_refs.update(
            (run.rollout_id, host) for host in retention.hold_hosts
        )
        superseded_plan_hold_refs.update(
            (run.rollout_id, host)
            for host in retention.superseded_plan_hold_hosts
        )
        unreceipted_full_holds = [
            host
            for host, hold in run.holds.items()
            if isinstance(hold, Mapping)
            and hold.get("kind") == "full"
            and hold.get("owned") is True
            and hold.get("status") in {"active", "cleanup-failed"}
            and host not in retention.hold_hosts
        ]
        obsolete_recoverable_full_holds = [
            host
            for host in unreceipted_full_holds
            if run.report_digest_sha256 != current_report_digest_sha256
            and _is_recoverable_legacy_full_hold(
                run,
                host=host,
                hold=run.holds[host],
            )
        ]
        if allow_legacy_reconciliation:
            # Explicit recovery authenticates these historical reports and
            # observes each host later, behind the fresh-plan proof. Do not
            # let the fleet-global durable pass probe the first old full hold:
            # an offline peer must not prevent another host's authoritative
            # inactive settlement from becoming durable (#356).
            retained_hold_refs.update(
                (run.rollout_id, host)
                for host in obsolete_recoverable_full_holds
            )
        if unreceipted_full_holds and _run_has_superseded_legacy_action(run):
            for host in unreceipted_full_holds:
                hold = run.holds[host]
                assert isinstance(hold, Mapping)
                _validate_recoverable_legacy_full_hold_shape(
                    run,
                    host=host,
                    hold=hold,
                )
            if not allow_legacy_reconciliation:
                raise FleetRolloutError(
                    f"rollout {run.rollout_id} has superseded legacy action "
                    "evidence beside an unreceipted active full hold on "
                    f"{', '.join(sorted(unreceipted_full_holds))}; run "
                    "--reconcile-legacy explicitly"
                )
            retained_hold_refs.update(
                (run.rollout_id, host) for host in unreceipted_full_holds
            )
        retained_fences.extend(
            fence for fence in retention.fences
            if not allow_legacy_reconciliation or fence[1] in retention.retired_hosts
        )
        for action_id, record in run.actions.items():
            assert isinstance(record, Mapping)
            attempts = _operation_attempts(record)
            if record.get("status") == "running" and not attempts:
                orphan = next(
                    (
                        operation
                        for operation, identity in observed_identities.items()
                        if identity.rollout_id == run.rollout_id
                        and identity.action_id == action_id
                    ),
                    None,
                )
                if orphan is not None:
                    raise FleetRolloutError(
                        f"legacy running rollout action {action_id} in "
                        f"{run.rollout_id} has unreferenced durable operation "
                        f"{orphan}; refusing legacy supersession"
                    )
                if (
                    not allow_legacy_reconciliation
                    and action_id not in retention.action_ids
                ):
                    raise FleetRolloutError(
                        f"legacy running rollout action {action_id} in "
                        f"{run.rollout_id} has no durable operation reference"
                    )
                retired_action = bool(retention.retired_hosts) and (
                    _legacy_action_parts(action_id)[1] in retention.retired_hosts
                )
                if retired_action and action_id not in retention.action_ids:
                    raise FleetRolloutError(
                        f"retirement does not cover running legacy action {action_id!r}"
                    )
                if (allow_legacy_reconciliation and retention.retired_hosts
                        and not retired_action):
                    raise FleetRolloutError(
                        "cannot rewrite a mixed live/retired legacy action group; "
                        "its retained evidence must remain intact"
                    )
                if allow_legacy_reconciliation and not retired_action:
                    legacy.append((run.rollout_id, action_id))
            for attempt in attempts:
                operation = attempt["operation_id"]
                observed = observations.get(operation)
                if observed is None:
                    raise FleetRolloutError(
                        f"rollout action {action_id} references missing "
                        f"durable operation {operation}"
                    )
                identity = _identity_from_observation(observed)
                if (
                    identity.rollout_id != run.rollout_id
                    or identity.report_digest_sha256
                    != run.report_digest_sha256
                    or identity.action_id != action_id
                    or attempt["request_sha256"]
                    != _request_sha256(observed)
                    or attempt["attempt"] != identity.attempt
                    or attempt["report_digest_sha256"]
                    != identity.report_digest_sha256
                    or attempt["identity"] != identity.as_dict()
                ):
                    raise FleetRolloutError(
                        f"rollout action {action_id} has a mismatched durable "
                        f"operation reference {operation}"
                    )
    return _LegacyJournalInventory(
        retired_hold_refs=frozenset(retired_hold_refs),
        running_actions=tuple(legacy),
        retained_fences=tuple(dict.fromkeys(retained_fences)),
        retained_hold_refs=frozenset(retained_hold_refs),
        superseded_plan_hold_refs=frozenset(
            superseded_plan_hold_refs
        ),
    )


def _stream_operation_until_terminal(
    operation: str,
    *,
    stream: Any = None,
    resume_authorized: bool = False,
    launched_supervisor: subprocess.Popen[bytes] | None = None,
    lifecycle_handoff: str | None = None,
) -> fleet_operation.OperationObservation:
    """Adopt a live child and copy its bounded spool only to stderr."""
    destination = sys.stderr if stream is None else stream
    offset = 0
    decoder = codecs.getincrementaldecoder("utf-8")("replace")
    supervisor = launched_supervisor
    launch_attempted = supervisor is not None
    activation_deadline: float | None = None
    pending_observation: fleet_operation.OperationObservation | None = None
    last_launcher_returncode: int | None = None

    def authorized_without_activation(
        observed: fleet_operation.OperationObservation,
    ) -> bool:
        return (
            observed.state
            in {
                "authorized-unactivated",
                "running-authorized",
            }
            and observed.activation is None
        )

    try:
        while True:
            observed = pending_observation or fleet_operation.observe_operation(
                operation
            )
            pending_observation = None
            if observed.ready is not None:
                while True:
                    try:
                        chunk = fleet_operation.read_output_since(
                            operation,
                            offset,
                            max_bytes=64 * 1024,
                        )
                    except fleet_operation.OperationStateError:
                        chunk = None
                    if chunk is None:
                        break
                    if chunk.data:
                        destination.write(decoder.decode(chunk.data))
                        destination.flush()
                    offset = chunk.next_offset
                    if chunk.at_end:
                        break
            if observed.state == "completed":
                final_text = decoder.decode(b"", final=True)
                if final_text:
                    destination.write(final_text)
                    destination.flush()
                if supervisor is not None:
                    owned_supervisor = supervisor
                    supervisor = None
                    _reap_durable_supervisor(
                        owned_supervisor,
                        terminate=False,
                    )
                return observed
            if observed.state == "outcome-unknown":
                if supervisor is not None:
                    owned_supervisor = supervisor
                    supervisor = None
                    _reap_durable_supervisor(
                        owned_supervisor,
                        terminate=True,
                    )
                raise FleetRolloutError(
                    f"durable operation {operation} has launch intent but no "
                    "terminal receipt; outcome is unknown and it will not be replayed"
                )
            if observed.state == "abandoned-pre-authorization":
                if supervisor is not None:
                    owned_supervisor = supervisor
                    supervisor = None
                    _reap_durable_supervisor(
                        owned_supervisor,
                        terminate=False,
                    )
                return observed

            preactivation = authorized_without_activation(observed)
            if preactivation:
                # A busy lease is only process liveness.  Keep one bounded
                # deadline until the recorder publishes durable activation;
                # otherwise a supervisor stuck between flock and recorder
                # launch would be adopted forever.
                if activation_deadline is None:
                    activation_deadline = (
                        time.monotonic()
                        + DURABLE_SUPERVISOR_ACQUIRE_TIMEOUT_SECONDS
                    )
                if observed.state == "authorized-unactivated":
                    if not resume_authorized:
                        raise FleetRolloutError(
                            f"durable operation {operation} is authorized but has no "
                            "launch intent; explicit same-operation resumption is "
                            "required"
                        )
                    if supervisor is None:
                        if launch_attempted:
                            raise FleetRolloutError(
                                f"durable supervisor for {operation} lost the lease "
                                "before activation; refusing an unbounded relaunch "
                                "loop"
                            )
                        supervisor = fleet_operation.launch_supervisor(
                            operation,
                            lifecycle_handoff=lifecycle_handoff,
                        )
                        launch_attempted = True

                if supervisor is not None:
                    returncode = supervisor.poll()
                    if returncode is not None:
                        last_launcher_returncode = returncode
                        supervisor = None
                        refreshed = fleet_operation.observe_operation(operation)
                        if not authorized_without_activation(refreshed):
                            pending_observation = refreshed
                            continue
                        if not refreshed.lease_busy:
                            raise FleetRolloutError(
                                f"durable supervisor for {operation} exited with "
                                f"{returncode} before durable activation"
                            )
                        observed = refreshed

                assert activation_deadline is not None
                if time.monotonic() >= activation_deadline:
                    refreshed = fleet_operation.observe_operation(operation)
                    if not authorized_without_activation(refreshed):
                        pending_observation = refreshed
                        continue
                    owned_at_timeout = supervisor is not None
                    if supervisor is not None:
                        owned_supervisor = supervisor
                        supervisor = None
                        _reap_durable_supervisor(
                            owned_supervisor,
                            terminate=True,
                        )
                        # Terminating the launcher cannot terminate the
                        # start-new-session recorder.  Recheck its receipts to
                        # close the activation/result race before reporting the
                        # bounded failure.
                        refreshed = fleet_operation.observe_operation(operation)
                        if not authorized_without_activation(refreshed):
                            pending_observation = refreshed
                            continue
                    if owned_at_timeout:
                        lease_detail = (
                            "a detached recorder still owns the lifetime lease and "
                            "cannot be safely signalled"
                            if refreshed.lease_busy
                            else "the lifetime lease is now free for explicit "
                            "same-operation resumption"
                        )
                        raise FleetRolloutError(
                            f"durable supervisor for {operation} did not publish "
                            "durable activation within "
                            f"{DURABLE_SUPERVISOR_ACQUIRE_TIMEOUT_SECONDS:g} seconds; "
                            "the owned launcher was terminated and reaped; "
                            f"{lease_detail}"
                        )
                    exit_detail = (
                        f" after launcher exit {last_launcher_returncode}"
                        if last_launcher_returncode is not None
                        else ""
                    )
                    if refreshed.lease_busy:
                        raise FleetRolloutError(
                            f"durable operation {operation} did not publish durable "
                            "activation within "
                            f"{DURABLE_SUPERVISOR_ACQUIRE_TIMEOUT_SECONDS:g} seconds"
                            f"{exit_detail}; an adopted process still owns the "
                            "lifetime lease and cannot be safely identified"
                        )
                    raise FleetRolloutError(
                        f"durable operation {operation} did not publish durable "
                        "activation within "
                        f"{DURABLE_SUPERVISOR_ACQUIRE_TIMEOUT_SECONDS:g} seconds"
                        f"{exit_detail}; the lifetime lease is now free for explicit "
                        "same-operation resumption"
                    )
            else:
                activation_deadline = None
            time.sleep(fleet_operation.DEFAULT_POLL_INTERVAL_SECONDS)
    except BaseException as exc:
        if supervisor is not None:
            owned_supervisor = supervisor
            supervisor = None
            try:
                # The recorder is a separate start-new-session process and
                # retains the lifetime lease.  This reaps only the launcher
                # PID whose handle this process owns.
                _reap_durable_supervisor(
                    owned_supervisor,
                    terminate=True,
                )
            except FleetRolloutError as cleanup_exc:
                exc.add_note(f"durable supervisor cleanup also failed: {cleanup_exc}")
        raise


def _reap_durable_supervisor(
    supervisor: subprocess.Popen[bytes],
    *,
    terminate: bool,
) -> int:
    """Bound cleanup of a launcher without touching its detached recorder."""

    def bounded_wait() -> int | None:
        try:
            return supervisor.wait(timeout=DURABLE_SUPERVISOR_REAP_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            return None

    returncode = supervisor.poll()
    if returncode is not None:
        return returncode
    if not terminate:
        returncode = bounded_wait()
        if returncode is not None:
            return returncode
    try:
        supervisor.terminate()
    except ProcessLookupError:
        # The child exited after poll().  wait() below still owns the reap.
        pass
    except OSError as exc:
        raise FleetRolloutError(
            "durable supervisor could not be terminated"
        ) from exc
    returncode = bounded_wait()
    if returncode is not None:
        return returncode
    try:
        supervisor.kill()
    except ProcessLookupError:
        pass
    except OSError as exc:
        raise FleetRolloutError("durable supervisor could not be killed") from exc
    returncode = bounded_wait()
    if returncode is not None:
        return returncode
    raise FleetRolloutError(
        "durable supervisor could not be reaped after termination"
    )


def _harvest_operation(
    run: RolloutRun,
    observed: fleet_operation.OperationObservation,
    identity: fleet_operation.OperationIdentity,
    *,
    request_sha256: str,
) -> tuple[RolloutRun, bool]:
    """Fold one verified terminal receipt into its journal exactly once."""
    current = run.actions.get(identity.action_id)
    attempts = _operation_attempts(current)
    prior = next(
        (
            item
            for item in attempts
            if item.get("operation_id") == observed.operation_id
        ),
        None,
    )
    if prior is not None and prior.get("harvested") is True:
        return run, False
    if observed.state != "completed" or observed.result is None:
        raise FleetRolloutError(
            f"durable operation {observed.operation_id} is not terminal"
        )
    result = observed.result
    tail = fleet_operation.read_output_tail(observed.operation_id)
    status = str(result.get("status"))
    executed = result.get("executed") is True
    returncode = result.get("returncode")
    base = dict(current) if isinstance(current, Mapping) else {}
    base.update(
        {
            "decision": base.get("decision", "update"),
            "reason": base.get("reason", "recovered durable operation"),
            "status": (
                "success"
                if status == "success"
                else "failed" if executed else "not-run"
            ),
            "argv": list(identity.argv),
            "target_sha": identity.target_sha,
            "target_version": identity.target_version,
            "target_tag": identity.target_tag,
            "rc": returncode,
            "duration_seconds": result.get("duration_seconds"),
            "output_tail": tail.text or None,
        }
    )
    if identity.phase == "driver" and status == "success":
        base["driver_reentry_required"] = True
    _replace_action_record(run, identity.action_id, base)
    _upsert_operation_ref(
        run,
        identity,
        observed,
        request_sha256=request_sha256,
        harvested=True,
    )
    save_run(run)
    return run, True


def _recover_local_hold_identity(
    run: RolloutRun,
    *,
    host: str,
    hold: Mapping[str, Any],
    runner: Runner,
    save_state: bool = True,
    force_observe: bool = False,
) -> dict[str, Any] | None:
    """Recover an old or response-lost local hold by its live exact pair."""
    if hold.get("kind") != "full":
        return dict(hold)
    if (
        not force_observe
        and
        hold.get("acquire_unconfirmed") is not True
        and isinstance(hold.get("set_at"), str)
    ):
        return dict(hold)
    control_host = str(hold.get("control_host") or host)
    status = _control_json(
        ["drain", "--status", "--json", control_host],
        runner=runner,
    )
    required_status = {
        "active",
        "mode",
        "is_full_drain",
        "is_scheduler_target_drain",
        "scheduler_hosts",
        "scheduler_leases",
        "orphaned_scheduler_leases",
        "scheduler_leases_error",
        "state",
        "submit_policy",
    }
    if not required_status.issubset(status):
        raise FleetRolloutError(
            f"cannot reconcile response-lost rollout hold for {host}: "
            "drain status is incomplete"
        )
    if (
        type(status.get("active")) is not bool
        or type(status.get("is_full_drain")) is not bool
        or type(status.get("is_scheduler_target_drain")) is not bool
        or not isinstance(status.get("scheduler_hosts"), list)
        or not isinstance(status.get("scheduler_leases"), list)
        or not isinstance(status.get("orphaned_scheduler_leases"), list)
        or status.get("scheduler_leases_error") is not None
        or status.get("submit_policy")
        not in {"accept_pending", "deny"}
    ):
        raise FleetRolloutError(
            f"cannot reconcile response-lost rollout hold for {host}: "
            "drain status has malformed or non-authoritative fields"
        )
    if status["active"] is False:
        if (
            status.get("mode") != "inactive"
            or status["is_full_drain"] is not False
            or status["is_scheduler_target_drain"] is not False
            or status["scheduler_hosts"]
            or status["scheduler_leases"]
            or status["orphaned_scheduler_leases"]
            or status.get("state") is not None
            or status.get("submit_policy") != "accept_pending"
        ):
            raise FleetRolloutError(
                f"cannot reconcile response-lost rollout hold for {host}: "
                "inactive drain status is internally contradictory"
            )
        record = dict(hold)
        record["status"] = "released"
        record.pop("acquire_unconfirmed", None)
        run.holds[host] = record
        if save_state:
            save_run(run)
        return None
    state = status.get("state")
    state_map = state if isinstance(state, Mapping) else {}
    set_at = state_map.get("set_at")
    if (
        status["is_full_drain"] is not True
        or state_map.get("reason") != hold.get("reason")
        or not isinstance(set_at, str)
    ):
        raise FleetRolloutError(
            f"cannot reconcile response-lost rollout hold for {host}: live "
            "reason/set_at does not prove ownership"
        )
    journaled_set_at = hold.get("set_at")
    if isinstance(journaled_set_at, str) and set_at != journaled_set_at:
        raise FleetRolloutError(
            f"cannot reconcile response-lost rollout hold for {host}: live "
            "set_at does not match the journaled exact hold identity"
        )
    record = dict(hold)
    record["set_at"] = set_at
    record.pop("acquire_unconfirmed", None)
    run.holds[host] = record
    if save_state:
        save_run(run)
    return record


def _release_reconciled_holds(
    run: RolloutRun,
    observations: Mapping[str, fleet_operation.OperationObservation],
    *,
    runner: Runner,
    retain_hosts: frozenset[str] = frozenset(),
    defer_unobservable_legacy_full_holds: bool = False,
) -> list[tuple[str, str, str]]:
    """Release exact owned holds only after all related children are terminal."""
    retained: list[tuple[str, str, str]] = []
    _validate_journal_records(run)
    identities = {
        operation: _identity_from_observation(observed)
        for operation, observed in observations.items()
    }
    for host, raw_hold in list(run.holds.items()):
        assert isinstance(raw_hold, Mapping)
        if raw_hold.get("status") not in {"active", "cleanup-failed"}:
            # A released/preserved journal row is terminal history. Probing it
            # again could mistake a later operator-owned drain for this old
            # rollout's hold and turn unrelated live state into a global block.
            continue
        action_host = _journaled_hold_action_host(host, raw_hold)
        if (
            raw_hold.get("kind") == "scheduler-target"
            and "action_host" in raw_hold
        ):
            # New scheduler journals bind an exact target to a canonical plan
            # lane and configured control key. Global operation reconciliation
            # has no current topology snapshot with which to validate either
            # relationship, so even a syntactically plausible corrupt binding
            # must be deferred to execute_plan's semantic preflight.
            continue
        if action_host in retain_hosts:
            continue
        related = [
            (operation, observed, identities[operation])
            for operation, observed in observations.items()
            if identities[operation].rollout_id == run.rollout_id
            and identities[operation].host == action_host
        ]
        unsettled = False
        for operation, observed, identity in related:
            attempts = _operation_attempts(run.actions.get(identity.action_id))
            harvested = any(
                item.get("operation_id") == operation
                and item.get("harvested") is True
                for item in attempts
            )
            if observed.state != "completed" or not harvested:
                unsettled = True
                break
        if unsettled:
            continue
        # Pre-owner scheduler journals are deliberately migrated by
        # ``execute_plan`` after the current live plan proves this is still a
        # scheduler lane. They do not yet carry the exact lease owner required
        # for a safe release, so global reconciliation must preserve them
        # instead of pre-empting the compatibility migration below.
        if (
            host in _journaled_legacy_owned_hosts(run)
            or host in _journaled_scheduler_holds_missing_control_host(run)
        ):
            continue
        legacy_full_identity_missing = bool(
            raw_hold.get("kind") == "full"
            and raw_hold.get("owned") is True
            and raw_hold.get("status") in {"active", "cleanup-failed"}
            and not isinstance(raw_hold.get("set_at"), str)
        )
        try:
            hold = _recover_local_hold_identity(
                run,
                host=host,
                hold=raw_hold,
                runner=runner,
            )
        except FleetRolloutError as exc:
            if (
                defer_unobservable_legacy_full_holds
                and legacy_full_identity_missing
            ):
                retained.append((run.rollout_id, host, str(exc)))
                continue
            raise
        if hold is None:
            continue
        exact = _journaled_exact_owner_holds(run).get(host)
        if exact is None:
            if hold.get("owned") is True and hold.get("status") in {
                "active",
                "cleanup-failed",
            }:
                raise FleetRolloutError(
                    f"rollout hold for {host} lacks an exact release identity"
                )
            continue
        synthetic = RolloutPlan(
            driver=str(exact.get("control_host") or host),
            report={},
            topology={},
            actions=[],
        )
        release_rollout_hold(
            synthetic,
            run,
            hold=exact,
            runner=runner,
        )
    return retained


def _coherent_superseded_legacy_record(
    run: RolloutRun,
    action_id: str,
    raw: Mapping[str, Any],
) -> bool:
    """Validate persisted evidence before it weakens a fleet-global block."""
    try:
        phase, host, program = _legacy_action_parts(action_id)
    except FleetRolloutError:
        return False
    evidence = raw.get("legacy_reconciliation")
    if not isinstance(evidence, Mapping):
        return False
    current_path = evidence.get("current_report_path")
    current_digest = evidence.get("current_report_digest_sha256")
    current_reason = evidence.get("current_action_reason")
    current_relation = evidence.get("current_identity_relation")
    target_sha = evidence.get("current_target_sha")
    observed_sha = evidence.get("observed_current_sha")
    identity_relation_is_coherent = bool(
        (
            current_relation == "equal"
            and current_reason == "already at target with LAST OK=true"
            and observed_sha == target_sha
        )
        or (
            current_relation == "ahead"
            and isinstance(current_reason, str)
            and current_reason.endswith("already deployed; no downgrade")
            and observed_sha != target_sha
        )
    )
    return bool(
        raw.get("status") == "superseded"
        and raw.get("observed_outcome") == "unknown"
        and isinstance(raw.get("superseded_at"), str)
        and raw.get("superseded_at")
        and evidence.get("schema")
        == "vq.fleet.legacy_action_reconciliation/1"
        and evidence.get("action_id") == action_id
        and evidence.get("phase") == phase
        and evidence.get("host") == host
        and evidence.get("program") == program
        and fleet_release.same_report(
            evidence.get("historical_report_path"), run.report_source_path
        )
        and evidence.get("historical_report_digest_sha256")
        == run.report_digest_sha256
        and evidence.get("durable_operation_present") is False
        and evidence.get("overlapping_admin_update_marker") is False
        and fleet_release.is_report_path(current_path)
        and isinstance(current_digest, str)
        and re.fullmatch(r"[0-9a-f]{64}", current_digest)
        and isinstance(current_reason, str)
        and current_reason
        and isinstance(target_sha, str)
        and re.fullmatch(r"[0-9a-f]{40}", target_sha)
        and isinstance(observed_sha, str)
        and re.fullmatch(r"[0-9a-f]{40}", observed_sha)
        and identity_relation_is_coherent
        and evidence.get("outcome_claim") == "not-observed"
    )


def _run_has_superseded_legacy_action(run: RolloutRun) -> bool:
    """Whether exact fresh-plan evidence authorized legacy hold isolation."""
    for action_id, raw in run.actions.items():
        if not isinstance(raw, Mapping):
            continue
        if _coherent_superseded_legacy_record(run, action_id, raw):
            return True
    return False


def _inventory_legacy_scheduler_holds(
    runs: Sequence[RolloutRun],
    *,
    current_report_digest_sha256: str,
    allow_legacy_reconciliation: bool,
    retained_hold_refs: Set[tuple[str, str]],
    retired_hold_refs: Set[tuple[str, str]] = frozenset(),
) -> tuple[tuple[str, str], ...]:
    """Classify obsolete scheduler claims without issuing live controls."""
    legacy_scheduler_holds: list[tuple[str, str]] = []
    for run in runs:
        legacy_hosts = _journaled_legacy_owned_hosts(run)
        missing_control_hosts = (
            _journaled_scheduler_holds_missing_control_host(run)
        )
        plan_bound_hosts = _journaled_scheduler_holds_requiring_plan(run)
        obsolete = run.report_digest_sha256 != current_report_digest_sha256
        covered = {
            host
            for receipt_rollout, host in retained_hold_refs
            if receipt_rollout == run.rollout_id
        }
        uncovered_legacy_hosts = legacy_hosts - covered
        uncovered_missing_control = missing_control_hosts - covered
        uncovered_plan_bound = plan_bound_hosts - covered
        if legacy_hosts and obsolete:
            if allow_legacy_reconciliation:
                legacy_scheduler_holds.extend(
                    (run.rollout_id, host)
                    for host in sorted(legacy_hosts)
                    if (run.rollout_id, host) not in retired_hold_refs
                )
            elif uncovered_legacy_hosts:
                raise FleetRolloutError(
                    f"obsolete rollout {run.rollout_id} retains scheduler "
                    "hold(s) without a complete current-plan release "
                    f"identity on {', '.join(sorted(uncovered_legacy_hosts))}; "
                    "they cannot be plan-aware reconciled under the current "
                    "accepted report; inspect the live drain and release or "
                    "migrate them explicitly"
                )
        if uncovered_missing_control and obsolete:
            raise FleetRolloutError(
                f"obsolete rollout {run.rollout_id} retains scheduler "
                "hold(s) without a complete current-plan release identity on "
                f"{', '.join(sorted(uncovered_missing_control))}; they cannot be "
                "plan-aware reconciled under the current accepted report; "
                "inspect the live drain and release or migrate them explicitly"
            )
        if uncovered_plan_bound and obsolete:
            raise FleetRolloutError(
                f"obsolete rollout {run.rollout_id} retains plan-bound "
                "scheduler hold(s) on "
                f"{', '.join(sorted(uncovered_plan_bound))}; restore and resume "
                "that rollout with its matching report/config, or explicitly "
                "reconcile those exact holds before continuing"
            )
    return tuple(legacy_scheduler_holds)


@dataclass(frozen=True)
class LegacyRolloutInventory:
    """What ``--reconcile-legacy`` would act on, established without writing."""

    running_actions: tuple[tuple[str, str], ...] = ()
    """(rollout_id, action_id) journal rows whose outcome would become
    permanently unknown."""
    scheduler_holds: tuple[tuple[str, str], ...] = ()
    """(rollout_id, host) obsolete pre-owner scheduler claims."""
    hold_retries: tuple[tuple[str, str], ...] = ()
    """(rollout_id, host) retained holds a reconciliation would re-observe."""
    pending_failure_transitions: tuple[tuple[str, str, str], ...] = ()
    """(rollout_id, host, reason) durable failure transactions still in
    flight, which a reconciliation would settle."""

    @property
    def empty(self) -> bool:
        return not (
            self.running_actions
            or self.scheduler_holds
            or self.hold_retries
            or self.pending_failure_transitions
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "running_actions": [
                {"rollout_id": rollout_id, "action_id": action_id}
                for rollout_id, action_id in self.running_actions
            ],
            "scheduler_holds": [
                {"rollout_id": rollout_id, "host": host}
                for rollout_id, host in self.scheduler_holds
            ],
            "hold_retries": [
                {"rollout_id": rollout_id, "host": host}
                for rollout_id, host in self.hold_retries
            ],
            "pending_failure_transitions": [
                {"rollout_id": rollout_id, "host": host, "reason": reason}
                for rollout_id, host, reason in self.pending_failure_transitions
            ],
            "empty": self.empty,
        }


def inventory_legacy_rollout_state(
    *,
    current_report_digest_sha256: str,
    current_report_source_path: str | None = None,
    allowed_retention_identity_mismatch_hosts: frozenset[str] = frozenset(),
    configured_retention_hosts: frozenset[str] | None = None,
    retired_hosts: Mapping[str, config.HostRetirement] | None = None,
    report_repo: Path | None = None,
) -> LegacyRolloutInventory:
    """List what ``--reconcile-legacy`` would reconcile. Writes nothing.

    Deliberately NOT :func:`reconcile_durable_operations` with a flag. That
    function's ``inspect_only`` branches exist to make a read-only command
    *refuse* legacy state rather than tolerate it, and threading a preview
    through them would mean loosening the guards that a plain ``--dry-run``
    depends on. This reaches the same three inventory primitives directly,
    every one of which classifies journals and issues no control:
    :func:`_validate_running_journals`,
    :func:`_pending_failure_transition_rows` and
    :func:`_inventory_legacy_scheduler_holds`. Every journal it would list is
    then authenticated and bound through the same
    :func:`_authenticate_journal_report` the mutating pass uses -- a
    read-only git lookup -- so the preview refuses exactly what that pass
    refuses, and says so in the same words.

    It observes durable operations with ``recover=False`` for the same reason
    -- recovery is a write -- which is also why an orphaned or mismatched
    durable reference still raises here: those are the conditions under which
    the mutating run would refuse, and a preview that hid them would be
    worse than no preview.

    A durable, fleet-wide recovery that marks historical action outcomes
    unknown had to be authorised blind before this existed: the operator
    could not see what it would rewrite.
    """
    try:
        operation_ids = fleet_operation.list_operation_ids(recover=False)
        observations = {
            operation: fleet_operation.observe_operation(operation, recover=False)
            for operation in operation_ids
        }
    except fleet_operation.OperationError as exc:
        raise FleetRolloutError(
            f"legacy reconciliation inventory failed: {exc}"
        ) from exc
    snapshots = _all_rollout_run_snapshots()
    pending_failure_transitions = _pending_failure_transition_rows(snapshots)
    legacy_inventory = _validate_running_journals(
        observations,
        current_report_source_path=current_report_source_path,
        current_report_digest_sha256=current_report_digest_sha256,
        allow_legacy_reconciliation=True,
        allowed_retention_identity_mismatch_hosts=(
            allowed_retention_identity_mismatch_hosts
        ),
        configured_retention_hosts=configured_retention_hosts,
        retired_hosts=retired_hosts,
        report_repo=report_repo,
    )
    # Unconditional, matching the mutating pass: it computes this over every
    # run, and only its early failure-transition return happens to compute it
    # under that condition. Gating here made the preview disagree with the
    # thing it previews, which a test caught.
    scheduler_holds = _inventory_legacy_scheduler_holds(
        tuple(run for _path, run, _token in snapshots),
        current_report_digest_sha256=current_report_digest_sha256,
        allow_legacy_reconciliation=True,
        retained_hold_refs=legacy_inventory.retained_hold_refs,
        retired_hold_refs=legacy_inventory.retired_hold_refs,
    )
    hold_retries = tuple(
        sorted(
            legacy_inventory.retained_hold_refs
            - legacy_inventory.superseded_plan_hold_refs
            - legacy_inventory.retired_hold_refs
        )
    )
    # Bind every row this would list exactly as the mutating pass does, row
    # type by row type in its order and with its refusals. A preview is the
    # operator's only look at a durable, fleet-wide recovery before they
    # authorise it, so one that lists a receipt the real run then refuses is
    # worse than none -- and that is precisely what the migration hit.
    runs_by_id = {run.rollout_id: run for _path, run, _token in snapshots}
    historical_reports: dict[str, fleet_release.FleetReleaseReport] = {}

    def bind(rollout_id_value: str, binding_failure: str) -> None:
        run = runs_by_id.get(rollout_id_value)
        if run is None:
            raise FleetRolloutError(
                f"legacy rollout journal {rollout_id_value} disappeared"
            )
        _authenticate_journal_report(
            run,
            repo=report_repo,
            cache=historical_reports,
            subject=f"legacy rollout {rollout_id_value}",
            binding_failure=binding_failure,
        )

    for rollout_id_value, _action_id in legacy_inventory.running_actions:
        bind(
            rollout_id_value,
            f"legacy rollout {rollout_id_value} journal/report identity "
            "does not match the committed accepted report",
        )
    for rollout_id_value, host in scheduler_holds:
        bind(
            rollout_id_value,
            f"obsolete legacy scheduler claim for {host} in "
            f"{rollout_id_value} does not bind its exact committed "
            "accepted report and deterministic rollout ID",
        )
    for rollout_id_value, _host in hold_retries:
        bind(
            rollout_id_value,
            f"legacy rollout {rollout_id_value} retained hold does not "
            "bind its committed accepted report",
        )
    return LegacyRolloutInventory(
        running_actions=legacy_inventory.running_actions,
        scheduler_holds=scheduler_holds,
        hold_retries=hold_retries,
        pending_failure_transitions=pending_failure_transitions,
    )


def reconcile_durable_operations(
    *,
    current_report_digest_sha256: str,
    current_report_source_path: str | None = None,
    inspect_only: bool,
    allow_legacy_reconciliation: bool = False,
    allowed_retention_identity_mismatch_hosts: frozenset[str] = frozenset(),
    configured_retention_hosts: frozenset[str] | None = None,
    retired_hosts: Mapping[str, config.HostRetirement] | None = None,
    report_repo: Path | None = None,
    acknowledge_driver_reentry: str | None = None,
    authenticated_driver_reentry: RolloutReentryCapability | None = None,
    control_runner: Runner = subprocess.run,
    stream: Any = None,
    lifecycle_handoff: str | None = None,
) -> OperationReconciliation:
    """Reconcile every report's durable children before fleet discovery.

    This is intentionally fleet-global. A scoped selection is applied only
    after this pass, so ``--only`` cannot hide malformed or orphaned evidence
    from another host. A structurally exact stale-report retention receipt may
    remain as a host-local fence only when the caller explicitly excluded that
    host; selected and unscoped hosts still require exact current-report
    identity. Read-only commands validate but never authorize, abort, follow,
    journal, or harvest.
    """
    if (
        acknowledge_driver_reentry is not None
        and authenticated_driver_reentry is None
    ):
        raise FleetRolloutError(
            "driver re-entry acknowledgement requires the inherited rollout "
            "and lifecycle lock capability"
        )
    if authenticated_driver_reentry is not None and acknowledge_driver_reentry is None:
        raise FleetRolloutError(
            "authenticated driver re-entry is missing its exact rollout ID"
        )
    try:
        operation_ids = fleet_operation.list_operation_ids(recover=not inspect_only)
        observations = {
            operation: fleet_operation.observe_operation(
                operation,
                recover=not inspect_only,
            )
            for operation in operation_ids
        }
    except fleet_operation.OperationError as exc:
        raise FleetRolloutError(f"durable operation reconciliation failed: {exc}") from exc
    failure_snapshots = _all_rollout_run_snapshots()
    pending_failure_transitions = _pending_failure_transition_rows(
        failure_snapshots
    )
    pending_failure_sources = {
        (rollout_id_value, host): reason
        for rollout_id_value, host, reason in pending_failure_transitions
    }
    legacy_inventory = _validate_running_journals(
        observations,
        current_report_source_path=current_report_source_path,
        current_report_digest_sha256=current_report_digest_sha256,
        allow_legacy_reconciliation=(
            allow_legacy_reconciliation and not inspect_only
        ),
        allowed_retention_identity_mismatch_hosts=(
            allowed_retention_identity_mismatch_hosts
        ),
        configured_retention_hosts=configured_retention_hosts,
        retired_hosts=retired_hosts,
        report_repo=report_repo,
    )
    legacy_running_actions = legacy_inventory.running_actions
    pending_scheduler_holds = (
        _inventory_legacy_scheduler_holds(
            tuple(run for _path, run, _token in failure_snapshots),
            current_report_digest_sha256=current_report_digest_sha256,
            allow_legacy_reconciliation=(
                allow_legacy_reconciliation and not inspect_only
            ),
            retained_hold_refs=legacy_inventory.retained_hold_refs,
            retired_hold_refs=legacy_inventory.retired_hold_refs,
        )
        if pending_failure_transitions
        else ()
    )
    if inspect_only and pending_failure_transitions:
        raise FleetRolloutError(
            "an outstanding legacy failure transition requires an explicit "
            "mutating reconciliation"
        )
    if pending_failure_transitions:
        # The failure transaction owns automatic retry and launch authority
        # until its forward intent is durably cleared.  Return its exact
        # handoff without adopting, harvesting, or resuming any operation.
        return OperationReconciliation(
            pending_failure_transitions=pending_failure_transitions,
            legacy_running_actions=legacy_running_actions,
            legacy_scheduler_holds=pending_scheduler_holds,
            legacy_hold_retries=tuple(
                sorted(
                    legacy_inventory.retained_hold_refs
                    - legacy_inventory.superseded_plan_hold_refs
                    - legacy_inventory.retired_hold_refs
                )
            ),
            retained_legacy_holds=legacy_inventory.retained_fences,
        )
    if not inspect_only:
        unknown_operations = [
            operation
            for operation, observed in observations.items()
            if observed.state == "outcome-unknown"
        ]
        if unknown_operations:
            raise FleetRolloutError(
                f"durable operation {unknown_operations[0]} has outcome "
                "unknown; refusing replay"
            )
    if inspect_only:
        for run in _all_rollout_runs():
            for host, hold in run.holds.items():
                if (
                    isinstance(hold, Mapping)
                    and hold.get("owned") is True
                    and hold.get("status") in {"active", "cleanup-failed"}
                    and (run.rollout_id, host)
                    not in legacy_inventory.retained_hold_refs
                ):
                    raise FleetRolloutError(
                        f"rollout {run.rollout_id} retains an owned hold on "
                        f"{host}; dry-run/verify-only will not release or "
                        "reconcile it"
                    )
    reentry_rollouts: dict[str, tuple[str, RolloutReentryCapability]] = {}
    authenticated_reentry_consumed = False
    touched_runs: dict[str, RolloutRun] = {}
    failed_hosts: dict[tuple[str, str], str] = {}
    for operation in operation_ids:
        observed = observations[operation]
        identity = _identity_from_observation(observed)
        run = _run_for_operation(identity)
        touched_runs[run.rollout_id] = run
        request_digest = _request_sha256(observed)
        attempts = _operation_attempts(run.actions.get(identity.action_id))
        referenced_attempt = next(
            (
                item
                for item in attempts
                if item.get("operation_id") == operation
            ),
            None,
        )
        harvested = bool(
            referenced_attempt
            and referenced_attempt.get("harvested") is True
        )
        if inspect_only:
            action_record = run.actions.get(identity.action_id)
            if (
                isinstance(action_record, Mapping)
                and action_record.get("driver_reentry_required") is True
            ):
                raise FleetRolloutError(
                    "a completed driver update still requires fresh-interpreter "
                    "re-entry; dry-run/verify-only will not acknowledge it"
                )
            if observed.state != "completed" or not harvested:
                raise FleetRolloutError(
                    f"durable operation {operation} is {observed.state}; "
                    "dry-run/verify-only will not reconcile it, run an explicit "
                    "rollout invocation first"
                )
            result = observed.result or {}
            if (
                result.get("executed") is True
                and result.get("returncode") != 0
                and referenced_attempt is not None
                and referenced_attempt["failure_fence_consumed"] is not True
                and referenced_attempt["failure_retry_authorized"] is not True
            ):
                raise FleetRolloutError(
                    f"durable operation {operation} has an unacknowledged "
                    "executed failure; dry-run/verify-only will not consume "
                    "its host-local failure fence"
                )
            continue
        _upsert_operation_ref(
            run,
            identity,
            observed,
            request_sha256=request_digest,
        )
        save_run(run)
        if observed.state == "outcome-unknown":
            raise FleetRolloutError(
                f"durable operation {operation} has outcome unknown; refusing replay"
            )
        if observed.state in {
            "authorized-unactivated",
            "running-authorized",
        }:
            # Authorization is immutable. The stream adopter resumes only
            # this operation if its live supervisor disappears before
            # activation, and bounds evidence that the replacement acquired
            # the transferred lifetime lease.
            if identity.rollout_lock_path is None:
                raise FleetRolloutError(
                    f"authorized durable operation {operation} predates the "
                    "continuous rollout/lifecycle fence; it remains fenced and "
                    "cannot be replayed automatically by this release"
                )
            if lifecycle_handoff is None:
                raise FleetRolloutError(
                    f"authorized durable operation {operation} requires the "
                    "controller lifecycle fence"
                )
            operation_handoff = attach_active_rollout_lock(
                lifecycle_handoff,
                rollout_id=identity.rollout_id,
            )
            observed = _stream_operation_until_terminal(
                operation,
                stream=stream,
                resume_authorized=True,
                lifecycle_handoff=operation_handoff,
            )
        elif (
            identity.report_digest_sha256 != current_report_digest_sha256
            and observed.state
            in {
                "prepared",
                "starting",
                "running-pre-authorization",
                "abandoned-pre-authorization",
            }
        ):
            try:
                fleet_operation.abort_operation(
                    operation,
                    expected_request_sha256=request_digest,
                )
            except fleet_operation.OperationError as exc:
                raise FleetRolloutError(
                    f"could not abort obsolete pre-authorization operation "
                    f"{operation}: {exc}"
                ) from exc
            observed = _stream_operation_until_terminal(operation, stream=stream)
        elif observed.state == "abandoned-pre-authorization":
            fleet_operation.abort_operation(
                operation,
                expected_request_sha256=request_digest,
            )
            observed = fleet_operation.observe_operation(operation)
        observations[operation] = observed
        if observed.state == "completed":
            run, _ = _harvest_operation(
                run,
                observed,
                identity,
                request_sha256=request_digest,
            )
            result = observed.result or {}
            action_record = run.actions.get(identity.action_id)
            if (
                identity.phase == "driver"
                and result.get("status") == "success"
                and isinstance(action_record, dict)
                and action_record.get("driver_reentry_required") is True
            ):
                expected_capability = RolloutReentryCapability(
                    rollout_id=identity.rollout_id,
                    operation_id=operation,
                    request_sha256=request_digest,
                    report_digest_sha256=identity.report_digest_sha256,
                )
                if (
                    authenticated_driver_reentry == expected_capability
                    and acknowledge_driver_reentry == identity.rollout_id
                ):
                    action_record.pop("driver_reentry_required", None)
                    _replace_action_record(run, identity.action_id, action_record)
                    save_run(run)
                    authenticated_reentry_consumed = True
                else:
                    reentry_rollouts[identity.rollout_id] = (
                        identity.host,
                        expected_capability,
                    )
            if result.get("executed") is True and result.get("returncode") != 0:
                failure_attempt = next(
                    item
                    for item in _operation_attempts(
                        run.actions.get(identity.action_id)
                    )
                    if item["operation_id"] == operation
                )
                pending_reason = pending_failure_sources.get(
                    (run.rollout_id, identity.host)
                )
                if pending_reason is not None:
                    if failure_attempt["failure_retry_authorized"] is not False:
                        raise FleetRolloutError(
                            "outstanding legacy failure transition already "
                            "authorized retry"
                        )
                elif failure_attempt["failure_fence_consumed"] is not True:
                    failed_hosts[(run.rollout_id, identity.host)] = (
                        f"{identity.action_id} executed and failed with exit "
                        f"{result.get('returncode')}; it is not retry-safe"
                    )
                elif failure_attempt["failure_retry_authorized"] is not True:
                    _set_failure_fence_state(
                        run,
                        action_id=identity.action_id,
                        operation_id=operation,
                        consumed=True,
                        retry_authorized=True,
                    )
                    save_run(run)
        touched_runs[run.rollout_id] = run
    if inspect_only:
        return OperationReconciliation(
            retained_legacy_holds=legacy_inventory.retained_fences
        )
    legacy_hold_retries: list[tuple[str, str]] = []
    if allow_legacy_reconciliation:
        legacy_hold_retries.extend(
            sorted(
                legacy_inventory.retained_hold_refs
                - legacy_inventory.superseded_plan_hold_refs
                - legacy_inventory.retired_hold_refs
            )
        )
        # A pending terminal failure fence deliberately prevents the global
        # durable pass from releasing this host's exact hold.  If the hold is
        # obsolete, carry it into explicit host-local recovery anyway: an
        # authoritative inactive snapshot can settle only the journal without
        # consuming the failure fence or issuing a release.  Without this
        # bridge the failed-host retain set hides the unreceipted hold forever.
        for failed_rollout, host in sorted(failed_hosts):
            failed_run = touched_runs[failed_rollout]
            raw_hold = failed_run.holds.get(host)
            if (
                failed_run.report_digest_sha256
                == current_report_digest_sha256
                or not isinstance(raw_hold, Mapping)
                or raw_hold.get("kind") != "full"
                or raw_hold.get("owned") is not True
                or raw_hold.get("status")
                not in {"active", "cleanup-failed"}
            ):
                continue
            if _is_recoverable_legacy_full_hold(
                    failed_run,
                    host=host,
                    hold=raw_hold,
                ):
                # The legacy inventory already carries this closed historical
                # shape, including recovered set_at identities. Do not
                # reinterpret it as a modern writer record merely because the
                # permissive exact-owner inventory can also see it.
                continue
            _validate_exact_modern_full_hold_shape(
                failed_run,
                host=host,
                hold=raw_hold,
            )
            legacy_hold_retries.append((failed_rollout, host))
    inventory_runs = _all_rollout_runs()
    legacy_scheduler_holds = list(
        _inventory_legacy_scheduler_holds(
            inventory_runs,
            current_report_digest_sha256=current_report_digest_sha256,
            allow_legacy_reconciliation=allow_legacy_reconciliation,
            retained_hold_refs=legacy_inventory.retained_hold_refs,
            retired_hold_refs=legacy_inventory.retired_hold_refs,
        )
    )
    for run in inventory_runs:
        if run.holds:
            touched_runs.setdefault(run.rollout_id, run)
    retained_legacy_holds: list[tuple[str, str, str]] = list(
        legacy_inventory.retained_fences
    )
    legacy_rollout_ids = {
        rollout_id for rollout_id, _action_id in legacy_running_actions
    }
    for run_id, run in touched_runs.items():
        if run_id in legacy_rollout_ids:
            # The explicit fresh-plan pass owns both the action proof and the
            # pre-recorder holds it protected.  Releasing a hold before that
            # action is safely superseded would separate the two halves of the
            # recovery transaction.
            continue
        retained_legacy_holds.extend(_release_reconciled_holds(
            run,
            observations,
            runner=control_runner,
            retain_hosts=frozenset(
                {
                    host
                    for failed_run, host in failed_hosts
                    if failed_run == run_id
                }
                | {
                    host
                    for pending_run, host in pending_failure_sources
                    if pending_run == run_id
                }
                | {
                    host
                    for receipt_run, host
                    in legacy_inventory.retained_hold_refs
                    if receipt_run == run_id
                }
            ),
            # Only an explicit, authenticated action supersession authorizes
            # an unreachable pre-recorder hold to be isolated to its host.
            # An arbitrary old journal with a pairless full hold remains a
            # global block exactly as before.
            defer_unobservable_legacy_full_holds=(
                _run_has_superseded_legacy_action(run)
            ),
        ))
    if len(reentry_rollouts) > 1:
        raise FleetRolloutError(
            "multiple completed driver operations require re-entry; refusing "
            "to acknowledge an ambiguous interpreter transition"
        )
    if authenticated_driver_reentry is not None and not authenticated_reentry_consumed:
        raise FleetRolloutError(
            "authenticated driver re-entry does not match the exact terminal receipt"
        )
    reentry = next(iter(reentry_rollouts.values()), None)
    capability = reentry[1] if reentry is not None else None
    return OperationReconciliation(
        driver_reentry_rollout_id=next(iter(reentry_rollouts), None),
        driver_reentry_host=(reentry[0] if reentry is not None else None),
        driver_reentry_operation_id=(
            capability.operation_id if capability is not None else None
        ),
        driver_reentry_request_sha256=(
            capability.request_sha256 if capability is not None else None
        ),
        driver_reentry_report_digest_sha256=(
            capability.report_digest_sha256 if capability is not None else None
        ),
        failed_operation_hosts=tuple(
            (rollout_id, host, reason)
            for (rollout_id, host), reason in sorted(failed_hosts.items())
        ),
        pending_failure_transitions=pending_failure_transitions,
        legacy_running_actions=legacy_running_actions,
        legacy_scheduler_holds=tuple(legacy_scheduler_holds),
        legacy_hold_retries=tuple(dict.fromkeys(legacy_hold_retries)),
        retained_legacy_holds=tuple(dict.fromkeys(retained_legacy_holds)),
    )


def _legacy_action_parts(action_id: str) -> tuple[Phase, str, str]:
    """Parse the closed set of action IDs written before durable recorders."""
    parts = action_id.split(":")
    if len(parts) == 3 and all(parts):
        phase, host, program = parts
        valid = bool(
            (phase == "driver" and program == "vibeqc-queue")
            or (phase == "local-runtime" and program in LOCAL_PROGRAM_ORDER)
            or (
                phase == "scheduler-runtime"
                and program in SCHEDULER_PROGRAM_ORDER
            )
        )
        if valid:
            return phase, host, program  # type: ignore[return-value]
    if len(parts) == 2 and parts[0] == "helper" and parts[1]:
        return "helper", parts[1], "vibeqc-queue"
    raise FleetRolloutError(
        f"legacy rollout action {action_id!r} has no recognized exact lane"
    )


def _legacy_action_expected_argv(
    action_id: str,
    report: fleet_release.FleetReleaseReport,
) -> list[str]:
    phase, host, program = _legacy_action_parts(action_id)
    try:
        pin = report.pins[PROGRAM_PINS[program]]
    except KeyError as exc:
        raise FleetRolloutError(
            f"legacy rollout action {action_id} names an unsupported program"
        ) from exc
    if phase == "helper":
        return [
            "admin",
            "update",
            host,
            "--expected-sha",
            pin.sha,
            "--drain-wait",
            DEFAULT_DRAIN_WAIT,
        ]
    argv = ["admin", "update", program, host, *pin.deploy_flags]
    if phase == "scheduler-runtime":
        argv.extend(["--drain-wait", DEFAULT_DRAIN_WAIT])
    return argv


def _legacy_action_marker_reason(
    admin_status: Mapping[str, Any],
    action: RolloutAction,
) -> str | None:
    if action.phase == "scheduler-runtime":
        requested_envs = [
            f"scheduler-runtime:{action.host}:{action.program}"
        ]
    elif action.phase == "helper":
        requested_envs = [f"scheduler:{action.host}"]
    else:
        requested_envs = [action.program]
    return _marker_reason(
        admin_status,
        action.host,
        requested_envs=requested_envs,
        requested_host=action.host,
    )


@dataclass(frozen=True)
class _LegacyActionRecovery:
    """Authenticated disposition for one pre-recorder running action."""

    rollout_id: str
    action_id: str
    record: dict[str, Any]
    phase: Phase
    host: str
    program: str
    current: RolloutAction | None
    disposition: Literal["supersede", "retain"]
    detail: str | None = None


@dataclass(frozen=True)
class _LegacyFullHoldRecoveryCandidate:
    """Immutable identity for one fully prevalidated full-hold retry."""

    rollout_id: str
    host: str
    shape: Literal["recovered-legacy", "exact-modern"]
    hold_sha256: str


@dataclass(frozen=True)
class _LegacyFailureRecoveryCandidate:
    """Globally authenticated terminal-failure recovery identity."""

    rollout_id: str
    host: str
    reason: str
    report_source_path: str
    report_digest_sha256: str
    host_hold_sha256: str | None
    action_ids: tuple[str, ...]
    operation_ids: tuple[str, ...]
    action_records_sha256: str


def _legacy_retained_action_receipt(
    recovery: _LegacyActionRecovery,
    *,
    run: RolloutRun,
    plan: RolloutPlan,
    retained_at: str,
    reason: str,
) -> dict[str, Any]:
    """Persist one retained pre-recorder action.

    The report identity written here is the *journal's*, not the discovered
    report's, because that is what every reader of this receipt validates
    against -- and the two spellings legitimately differ on a split checkout.
    The binding check upstream has already proven they are the same report.
    """
    current_reason = (
        recovery.current.reason
        if recovery.current is not None
        else "current accepted plan has no matching lane"
    )
    return {
        "schema": _LEGACY_RETAINED_ACTION_SCHEMA,
        "action_id": recovery.action_id,
        "phase": recovery.phase,
        "host": recovery.host,
        "program": recovery.program,
        "historical_report_path": run.report_source_path,
        "historical_report_digest_sha256": run.report_digest_sha256,
        "action_record_sha256": _legacy_subtree_sha256(recovery.record),
        "argv_sha256": _legacy_subtree_sha256(recovery.record.get("argv")),
        "durable_operation_present": False,
        "observed_outcome": "unknown",
        "current_report_path": plan.report.get("source_path"),
        "current_report_digest_sha256": plan.report.get("digest_sha256"),
        "current_action_reason": current_reason,
        "retained_at": retained_at,
        "reason": reason,
    }


def _legacy_retained_hold_receipt(
    run: RolloutRun,
    *,
    host: str,
    hold: Mapping[str, Any],
    plan: RolloutPlan,
    source: Literal[
        "retained-action-group",
        "scheduler-observation",
        "hold-observation",
    ],
    control_host: str | None,
    retained_at: str,
    reason: str,
) -> dict[str, Any]:
    return {
        "schema": _LEGACY_RETAINED_HOLD_SCHEMA,
        "host": host,
        "kind": hold.get("kind"),
        "source": source,
        "historical_report_path": run.report_source_path,
        "historical_report_digest_sha256": run.report_digest_sha256,
        "hold_record_sha256": _legacy_subtree_sha256(hold),
        "current_report_path": plan.report.get("source_path"),
        "current_report_digest_sha256": plan.report.get("digest_sha256"),
        "control_host": control_host,
        "observed_outcome": "unknown",
        "retained_at": retained_at,
        "reason": reason,
    }


def _validate_preowner_full_hold_shape(
    run: RolloutRun,
    *,
    host: str,
    hold: Mapping[str, Any],
) -> None:
    """Recognize only the closed full-hold row written by the legacy era."""
    allowed = {
        "host",
        "kind",
        "owned",
        "preexisting",
        "reason",
        "status",
        "cleanup_error",
    }
    if (
        set(hold) - allowed
        or hold.get("host") != host
        or hold.get("kind") != "full"
        or hold.get("owned") is not True
        or hold.get("preexisting") is not False
        or hold.get("reason") != _hold_reason(run.rollout_id, host)
        or hold.get("status") not in {"active", "cleanup-failed"}
        or (
            hold.get("status") == "active" and "cleanup_error" in hold
        )
        or (
            hold.get("status") == "cleanup-failed"
            and (
                not isinstance(hold.get("cleanup_error"), str)
                or not str(hold["cleanup_error"]).strip()
            )
        )
    ):
        raise FleetRolloutError(
            f"legacy rollout {run.rollout_id} has malformed or ambiguous "
            f"pre-owner full hold on {host}"
        )


_RECOVERABLE_LEGACY_FULL_HOLD_FIELDS = frozenset(
    {
        "host",
        "kind",
        "owned",
        "preexisting",
        "reason",
        "status",
        "cleanup_error",
        "set_at",
    }
)

_EXACT_MODERN_FULL_HOLD_FIELDS = frozenset(
    {
        "host",
        "kind",
        "owned",
        "preexisting",
        "reason",
        "status",
        "duration_seconds",
        "set_at",
        "control_host",
    }
)


def _validate_exact_modern_full_hold_shape(
    run: RolloutRun,
    *,
    host: str,
    hold: Mapping[str, Any],
) -> None:
    """Validate the closed exact-owner full-hold shape used before #356."""
    expected_fields = set(_EXACT_MODERN_FULL_HOLD_FIELDS)
    if hold.get("status") == "cleanup-failed":
        expected_fields.add("cleanup_error")
    if (
        set(hold) != expected_fields
        or hold.get("host") != host
        or hold.get("kind") != "full"
        or hold.get("owned") is not True
        or hold.get("preexisting") is not False
        or hold.get("reason") != _hold_reason(run.rollout_id, host)
        or hold.get("status") not in {"active", "cleanup-failed"}
        or type(hold.get("duration_seconds")) is not int
        or int(hold["duration_seconds"])
        < ROLLOUT_HOLD_MIN_DURATION_SECONDS
        or not isinstance(hold.get("set_at"), str)
        or not str(hold["set_at"]).strip()
        or hold.get("control_host") != host
        or (
            hold.get("status") == "cleanup-failed"
            and (
                not isinstance(hold.get("cleanup_error"), str)
                or not str(hold["cleanup_error"]).strip()
            )
        )
    ):
        raise FleetRolloutError(
            f"obsolete rollout {run.rollout_id} has malformed or ambiguous "
            f"exact full hold on {host}"
        )


def _validate_recoverable_legacy_full_hold_shape(
    run: RolloutRun,
    *,
    host: str,
    hold: Mapping[str, Any],
) -> None:
    """Validate an old pairless full hold, including recovered ``set_at``.

    The pre-recorder journal wrote the closed shape accepted by
    :func:`_validate_preowner_full_hold_shape`.  An older recovery may have
    durably added only the exact live ``set_at`` before crashing.  Accept that
    one additive identity field, but no modern acquisition metadata or
    arbitrary subtree: this seam exists solely to make that historical crash
    state explicitly recoverable.
    """
    if "set_at" not in hold:
        _validate_preowner_full_hold_shape(run, host=host, hold=hold)
        return
    if (
        set(hold) - _RECOVERABLE_LEGACY_FULL_HOLD_FIELDS
        or hold.get("host") != host
        or hold.get("kind") != "full"
        or hold.get("owned") is not True
        or hold.get("preexisting") is not False
        or hold.get("reason") != _hold_reason(run.rollout_id, host)
        or hold.get("status") not in {"active", "cleanup-failed"}
        or not isinstance(hold.get("set_at"), str)
        or not str(hold["set_at"]).strip()
        or (hold.get("status") == "active" and "cleanup_error" in hold)
        or (
            hold.get("status") == "cleanup-failed"
            and (
                not isinstance(hold.get("cleanup_error"), str)
                or not str(hold["cleanup_error"]).strip()
            )
        )
    ):
        raise FleetRolloutError(
            f"legacy rollout {run.rollout_id} has malformed or ambiguous "
            f"recoverable full hold on {host}"
        )


def _is_recoverable_legacy_full_hold(
    run: RolloutRun,
    *,
    host: str,
    hold: Mapping[str, Any],
) -> bool:
    """Whether one closed-shape pre-owner full hold belongs to recovery.

    Modern full holds add control/duration/acquisition identity and continue
    through ordinary durable reconciliation. This predicate only recognizes
    the historical closed field set; a record in that set is validated
    strictly so malformed old state cannot be reclassified as recoverable.
    """
    if (
        hold.get("kind") != "full"
        or hold.get("owned") is not True
        or hold.get("status") not in {"active", "cleanup-failed"}
        or "preexisting" not in hold
        or set(hold) - _RECOVERABLE_LEGACY_FULL_HOLD_FIELDS
    ):
        return False
    _validate_recoverable_legacy_full_hold_shape(run, host=host, hold=hold)
    return True


@dataclass(frozen=True)
class _LegacySchedulerHoldRecovery:
    """Prevalidated disposition for one obsolete pre-owner target claim."""

    rollout_id: str
    host: str
    hold: dict[str, Any]
    control_host: str
    disposition: Literal["inactive", "release", "retain"]
    observed_at: str | None = None
    expected_reason: str | None = None
    expected_set_at: str | None = None
    detail: str | None = None


def _legacy_scheduler_hold_control(
    plan: RolloutPlan,
    *,
    target: str,
) -> str:
    """Bind a pre-owner canonical target to one current control daemon."""
    matches: list[tuple[str, str]] = []
    for action_host, targets in plan._scheduler_hold_targets.items():
        for candidate, control in targets:
            if candidate == target:
                matches.append((action_host, control))
    if (
        len(matches) != 1
        or matches[0][0] != target
        or not isinstance(matches[0][1], str)
        or not matches[0][1]
        or matches[0][1] != matches[0][1].strip()
    ):
        raise FleetRolloutError(
            f"obsolete legacy scheduler claim for {target} has no unique "
            "canonical current-plan control binding"
        )
    return matches[0][1]


def _legacy_scheduler_live_release_block(
    plan: RolloutPlan,
    *,
    host: str,
    admin_status: Mapping[str, Any],
) -> str | None:
    """Return why an active old lane cannot be lifted from this plan."""
    actions = [
        action
        for action in plan.actions
        if action.host == host
        and action.phase in {"helper", "scheduler-runtime"}
    ]
    if not actions:
        raise FleetRolloutError(
            f"obsolete legacy scheduler claim for {host} has no genuine "
            "current scheduler lane"
        )
    for action in actions:
        marker = _legacy_action_marker_reason(admin_status, action)
        if marker is not None:
            return f"overlapping admin-update marker: {marker}"
    if not all(
        _skip_proves_live_success(action)
        and action.before.get("configured") is True
        and action.before.get("last_ok") is True
        for action in actions
    ):
        return (
            "current scheduler lane is not a fresh healthy skip; retaining "
            "the legacy dispatch fence"
        )
    return None


def _validate_obsolete_legacy_scheduler_hold(
    run: RolloutRun,
    *,
    host: str,
    hold: Mapping[str, Any],
    plan: RolloutPlan,
    legacy_action_refs: frozenset[tuple[str, str]],
) -> str:
    """Validate the closed historical claim shape before any mutation."""
    allowed_fields = {
        "host",
        "kind",
        "owned",
        "preexisting",
        "reason",
        "status",
        "cleanup_error",
    }
    if (
        set(hold) - allowed_fields
        or hold.get("host") != host
        or hold.get("kind") != "scheduler-target"
        or hold.get("owned") is not True
        or hold.get("preexisting") is not False
        or hold.get("status") not in {"active", "cleanup-failed"}
        or hold.get("reason") != _hold_reason(run.rollout_id, host)
        or (
            "cleanup_error" in hold
            and not isinstance(hold.get("cleanup_error"), str)
        )
        or (
            hold.get("status") == "active"
            and "cleanup_error" in hold
        )
        or (
            hold.get("status") == "cleanup-failed"
            and (
                not isinstance(hold.get("cleanup_error"), str)
                or not str(hold["cleanup_error"]).strip()
            )
        )
        or bool(run.failed_hosts)
    ):
        raise FleetRolloutError(
            f"obsolete legacy scheduler claim for {host} in "
            f"{run.rollout_id} is malformed or has ambiguous ownership"
        )
    if run.report_digest_sha256 == plan.report.get("digest_sha256"):
        raise FleetRolloutError(
            f"legacy scheduler claim for {host} in {run.rollout_id} is not "
            "obsolete; resume its matching accepted-report plan"
        )
    for action_id, raw in run.actions.items():
        assert isinstance(raw, Mapping)
        attempts = _operation_attempts(raw)
        if (
            (run.rollout_id, action_id) in legacy_action_refs
            and raw.get("status") == "running"
            and not attempts
        ):
            continue
        terminal = not attempts and raw.get("status") in {
            "success",
            "not-run",
        }
        if not terminal and not (
            not attempts
            and _coherent_superseded_legacy_record(run, action_id, raw)
        ):
            raise FleetRolloutError(
                f"obsolete legacy scheduler claim for {host} in "
                f"{run.rollout_id} has unresolved or mixed action state at "
                f"{action_id}"
            )
    return _legacy_scheduler_hold_control(plan, target=host)


def _validate_exact_plan_bound_scheduler_hold(
    run: RolloutRun,
    *,
    host: str,
    hold: Mapping[str, Any],
    plan: RolloutPlan,
) -> tuple[str, str]:
    """Validate the complete writer shape and current alias binding."""
    allowed = {
        "host",
        "action_host",
        "kind",
        "owned",
        "status",
        "reason",
        "preexisting",
        "lease_owner",
        "control_host",
        "legacy_migrated",
        "legacy_migration_pending",
        "legacy_expected_reason",
        "legacy_expected_set_at",
        "acquire_unconfirmed",
        "external_holds",
        "cleanup_error",
    }
    action_host = hold.get("action_host")
    control_host = hold.get("control_host")
    pending = hold.get("legacy_migration_pending")
    migrated = hold.get("legacy_migrated")
    status = hold.get("status")
    has_acquire_unconfirmed = "acquire_unconfirmed" in hold
    has_external_holds = "external_holds" in hold
    has_expected_reason = "legacy_expected_reason" in hold
    has_expected_set_at = "legacy_expected_set_at" in hold
    has_expected_pair = has_expected_reason and has_expected_set_at
    if (
        set(hold) - allowed
        or hold.get("host") != host
        or hold.get("kind") != "scheduler-target"
        or hold.get("owned") is not True
        or status not in {"active", "cleanup-failed"}
        or hold.get("reason") != _hold_reason(run.rollout_id, host)
        or type(hold.get("preexisting")) is not bool
        or hold.get("lease_owner")
        != _hold_lease_owner(run.rollout_id, host)
        or not isinstance(action_host, str)
        or not action_host
        or action_host != action_host.strip()
        or not isinstance(control_host, str)
        or not control_host
        or control_host != control_host.strip()
        or type(migrated) is not bool
        or type(pending) is not bool
        or (
            has_acquire_unconfirmed
            and hold.get("acquire_unconfirmed") is not True
        )
        or (migrated is True and pending is True)
        or (migrated is True and has_acquire_unconfirmed)
        or (has_expected_reason != has_expected_set_at)
        or (status == "active" and "cleanup_error" in hold)
        or (
            status == "cleanup-failed"
            and (
                not isinstance(hold.get("cleanup_error"), str)
                or not str(hold["cleanup_error"]).strip()
            )
        )
        or (
            pending is True
            and (
                not has_expected_pair
                or
                not isinstance(hold.get("legacy_expected_set_at"), str)
                or not str(hold["legacy_expected_set_at"]).strip()
                or hold.get("legacy_expected_reason") != hold.get("reason")
            )
        )
        or (
            pending is False
            and migrated is False
            and has_expected_pair
        )
        or (
            pending is False
            and migrated is True
            and has_expected_pair
            and (
                not isinstance(hold.get("legacy_expected_set_at"), str)
                or not str(hold["legacy_expected_set_at"]).strip()
                or hold.get("legacy_expected_reason") != hold.get("reason")
                or has_acquire_unconfirmed
                or has_external_holds
            )
        )
        or (
            has_external_holds
            and (
                has_acquire_unconfirmed
                or pending is True
            )
        )
    ):
        raise FleetRolloutError(
            f"obsolete rollout {run.rollout_id} has malformed or ambiguous "
            f"plan-bound scheduler hold on {host}"
        )
    external = hold.get("external_holds", [])
    if not isinstance(external, list) or (
        has_external_holds and not external
    ):
        raise FleetRolloutError(
            f"obsolete rollout {run.rollout_id} has malformed or ambiguous "
            f"plan-bound scheduler hold on {host}"
        )
    for item in external:
        if (
            not isinstance(item, Mapping)
            or set(item) != {"host", "kind", "lease_id", "owner", "reason"}
            or item.get("host") != host
            or item.get("kind") not in {"full", "scheduler-target"}
            or item.get("lease_id") is not None
            and not isinstance(item.get("lease_id"), str)
            or item.get("owner") is not None
            and not isinstance(item.get("owner"), str)
            or item.get("reason") is not None
            and not isinstance(item.get("reason"), str)
        ):
            raise FleetRolloutError(
                f"obsolete rollout {run.rollout_id} has malformed external "
                f"hold evidence for {host}"
            )
    targets = _scheduler_hold_targets_for(plan, action_host)
    exact_controls = [
        candidate_control
        for candidate, candidate_control in targets
        if candidate == host
    ]
    if exact_controls != [control_host]:
        raise FleetRolloutError(
            f"obsolete rollout {run.rollout_id} plan-bound scheduler hold "
            f"for {host} does not match one exact current-plan target/control "
            "binding"
        )
    return action_host, control_host


def _healthy_scheduler_supersede_actions(
    plan: RolloutPlan,
    *,
    action_host: str,
    accepted_report: fleet_release.FleetReleaseReport,
) -> list[dict[str, str]]:
    """Return exact healthy current-lane evidence or fail closed."""
    actions = sorted(
        (
            action
            for action in plan.actions
            if action.host == action_host
            and action.phase in {"helper", "scheduler-runtime"}
        ),
        key=lambda action: action.id,
    )
    expected_action_ids = {
        f"helper:{action_host}",
        *(
            f"scheduler-runtime:{action_host}:{program}"
            for program in SCHEDULER_PROGRAM_ORDER
        ),
    }
    if (
        len(actions) != len(expected_action_ids)
        or {action.id for action in actions} != expected_action_ids
    ):
        raise FleetRolloutError(
            f"plan-bound scheduler hold action host {action_host!r} does not "
            "have the exact complete current scheduler action set"
        )
    evidence: list[dict[str, str]] = []
    for action in actions:
        try:
            phase, parsed_host, program = _legacy_action_parts(action.id)
            expected_pin_name = PROGRAM_PINS[program]
            pin = accepted_report.pins[expected_pin_name]
            expected_argv = _legacy_action_expected_argv(
                action.id,
                accepted_report,
            )
        except (FleetRolloutError, KeyError) as exc:
            raise FleetRolloutError(
                f"plan-bound scheduler hold action {action.id!r} is not "
                "bound to an accepted-report scheduler lane"
            ) from exc
        observed = action.before.get("current_sha")
        if (
            type(action) is not RolloutAction
            or phase != action.phase
            or parsed_host != action_host
            or program != action.program
            or action.pin_name != expected_pin_name
            or action.target_sha != pin.sha
            or action.target_version != pin.version
            or action.target_tag != pin.tag
            or action.argv != expected_argv
            or action.rc is not None
            or action.outcome is not None
            or action.output_tail is not None
            or set(action.before)
            != {
                "configured",
                "current_sha",
                "current_version",
                "current_tag",
                "dirty",
                "last_ok",
                "acknowledged",
                "detail",
                "metrics",
            }
            or action.decision != "skip"
            or action.reason != "already at target with LAST OK=true"
            or action.before.get("configured") is not True
            or action.before.get("last_ok") is not True
            or action.before.get("dirty") is not False
            or action.before.get("acknowledged") is not False
            or (
                action.before.get("current_version") is not None
                and not isinstance(action.before.get("current_version"), str)
            )
            or (
                action.before.get("current_tag") is not None
                and not isinstance(action.before.get("current_tag"), str)
            )
            or not isinstance(action.before.get("detail"), str)
            or not str(action.before["detail"]).strip()
            or (
                action.before.get("metrics") is not None
                and not isinstance(action.before.get("metrics"), Mapping)
            )
            or not isinstance(observed, str)
            or _SOURCE_SHA.fullmatch(observed) is None
            or observed != pin.sha
        ):
            if action.before.get("probe_unavailable") is True:
                # Not a verdict about the host: the live probe did not answer,
                # so nothing is known. Retrying is exactly right, and refusing
                # permanently here is what made pbs-cluster look non-converged five
                # times in a row while it was fine.
                raise FleetProbeUnavailable(
                    f"plan-bound scheduler hold action host {action_host!r} "
                    f"could not be proved at {action.id}: its live probe did "
                    "not answer, so this is not a verdict about the host. "
                    "Retry; if it persists, raise the doctor budget with "
                    "[fleet] check_timeout_seconds"
                )
            raise FleetRolloutError(
                f"plan-bound scheduler hold action host {action_host!r} "
                f"lacks strictly healthy exact-target evidence at {action.id}"
            )
        evidence.append(
            {
                "action_id": action.id,
                "phase": action.phase,
                "host": action.host,
                "program": action.program,
                "expected_sha": action.target_sha,
                "observed_sha": observed,
            }
        )
    return evidence


def _observe_obsolete_legacy_scheduler_hold(
    run: RolloutRun,
    *,
    host: str,
    hold: Mapping[str, Any],
    control_host: str,
    live_release_block: str | None,
    accept_legacy_absence: bool = False,
    runner: Runner,
) -> _LegacySchedulerHoldRecovery:
    """Classify one target from a provenance-bound non-writing snapshot."""
    argv = [
        "drain",
        "--status",
        "--json",
        "--read-only-snapshot",
        control_host,
    ]
    try:
        proc = _run_control(
            argv,
            runner=runner,
            timeout=_DRAIN_LIVENESS_CONTROL_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return _LegacySchedulerHoldRecovery(
            run.rollout_id,
            host,
            dict(hold),
            control_host,
            "retain",
            detail=f"supported read-only drain snapshot unavailable: {exc}",
        )
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "(no output)").strip()
        return _LegacySchedulerHoldRecovery(
            run.rollout_id,
            host,
            dict(hold),
            control_host,
            "retain",
            detail=(
                "supported read-only drain snapshot unavailable: "
                f"exit {proc.returncode}: {detail}"
            ),
        )
    try:
        snapshot = _validate_final_drain_status(
            _strict_liveness_json(proc.stdout)
        )
    except (RecursionError, TypeError, ValueError) as exc:
        raise FleetRolloutError(
            f"obsolete legacy scheduler claim for {host} in "
            f"{run.rollout_id} received a malformed or ambiguous supported "
            f"read-only drain snapshot: {exc}"
        ) from exc
    observed_at = str(snapshot["observed_at"])
    coverage = snapshot["coverage"]
    if (
        not all(coverage.values())
        or snapshot["safety_fail_closed"] is True
    ):
        return _LegacySchedulerHoldRecovery(
            run.rollout_id,
            host,
            dict(hold),
            control_host,
            "retain",
            observed_at=observed_at,
            detail="supported read-only drain snapshot coverage is incomplete",
        )
    policy = snapshot["policy"]
    if policy is None:
        return _LegacySchedulerHoldRecovery(
            run.rollout_id,
            host,
            dict(hold),
            control_host,
            "inactive",
            observed_at=observed_at,
        )
    scheduler_hosts = set(policy["scheduler_hosts"])
    legacy_hosts = set(policy["legacy_scheduler_hosts"])
    leases = [
        lease
        for lease in snapshot["scheduler_leases"]
        if lease["scheduler_host"] == host
    ]
    if host not in scheduler_hosts:
        return _LegacySchedulerHoldRecovery(
            run.rollout_id,
            host,
            dict(hold),
            control_host,
            "inactive",
            observed_at=observed_at,
        )
    if host not in legacy_hosts:
        if accept_legacy_absence:
            return _LegacySchedulerHoldRecovery(
                run.rollout_id,
                host,
                dict(hold),
                control_host,
                "inactive",
                observed_at=observed_at,
            )
        owners = ", ".join(
            sorted(str(lease["owner"]) for lease in leases)
        )
        owner_detail = f" ({owners})" if owners else ""
        return _LegacySchedulerHoldRecovery(
            run.rollout_id,
            host,
            dict(hold),
            control_host,
            "retain",
            observed_at=observed_at,
            detail=(
                "live target is held by an owner-scoped or external claim"
                f"{owner_detail}, not the journaled legacy component"
            ),
        )
    expected_reason = hold.get("reason")
    live_reason = policy.get("identity_reason")
    live_set_at = policy.get("set_at")
    if live_reason != expected_reason or not isinstance(live_set_at, str):
        return _LegacySchedulerHoldRecovery(
            run.rollout_id,
            host,
            dict(hold),
            control_host,
            "retain",
            observed_at=observed_at,
            detail=(
                "live legacy target reason/set_at does not match the exact "
                "historical rollout claim"
            ),
        )
    if live_release_block is not None:
        return _LegacySchedulerHoldRecovery(
            run.rollout_id,
            host,
            dict(hold),
            control_host,
            "retain",
            observed_at=observed_at,
            detail=live_release_block,
        )
    return _LegacySchedulerHoldRecovery(
        run.rollout_id,
        host,
        dict(hold),
        control_host,
        "release",
        observed_at=observed_at,
        expected_reason=str(expected_reason),
        expected_set_at=live_set_at,
    )


def _historical_report_binds_run(
    historical: fleet_release.FleetReleaseReport,
    run: RolloutRun,
) -> bool:
    """Is this authenticated historical report the one this rollout ran?

    The digest and the deterministic rollout id bind identity. The recorded
    path is only *where the report was written*, and both layouts are
    legitimate for the same report across the 2026-09-08 split: a journal
    persisted before it says ``vibe-queue/releases/<tag>.json``, while
    vibe-queue's own repository holds the byte-identical blob at
    ``releases/<tag>.json``.

    `b2aef94` fixed exactly this, on the supersede path, and could only ever
    be a partial fix: the same comparison was written out four more times.
    That is why compute-a's v0.15.118 retained hold authenticated perfectly and was
    then rejected as not binding its own rollout, blocking `--reconcile-legacy`,
    `--verify-only` and `--dry-run` alike with no operator workaround, because
    the comparison happens inside vq.

    One function rather than five copies, so the next sibling cannot be missed,
    and a test asserts that no sixth copy appears.
    """
    historical_tag = Path(historical.source_path).stem
    return (
        historical.source_path in fleet_release.report_paths_for(historical_tag)
        and historical.digest_sha256 == run.report_digest_sha256
        and rollout_id(historical) == run.rollout_id
    )


def _authenticate_journal_report(
    run: RolloutRun,
    *,
    repo: Path | None,
    cache: dict[str, fleet_release.FleetReleaseReport],
    subject: str,
    binding_failure: str,
) -> fleet_release.FleetReleaseReport:
    """Authenticate the committed report one journal records, and bind it.

    Every reconcile path and the ``--dry-run`` preview call this, so the
    preview cannot drift from the thing it previews: it refuses the same
    journals, in the same order, with the same words. Until it did, the
    preview listed compute-a's retained v0.15.118 hold as one that "would be
    re-observed" and the real run then refused that very receipt -- the
    operator's only read-only look at a durable, fleet-wide recovery was
    telling them the opposite of what would happen.

    ``cache`` is keyed by the recorded path and digest, and the binding check
    still runs on a hit, because the rollout id is per journal.
    """
    key = f"{run.report_source_path}@{run.report_digest_sha256}"
    historical = cache.get(key)
    if historical is None:
        try:
            historical = fleet_release.discover_historical_report_by_digest(
                run.report_source_path,
                run.report_digest_sha256,
                repo,
                fetch=False,
            )
        except fleet_release.FleetReleaseError as exc:
            raise FleetRolloutError(
                f"{subject} historical report could not be authenticated: "
                f"{exc}"
            ) from exc
        cache[key] = historical
    if not _historical_report_binds_run(historical, run):
        raise FleetRolloutError(binding_failure)
    return historical


def supersede_obsolete_plan_bound_hold(
    *,
    rollout_id_value: str,
    host: str,
    accepted_report: fleet_release.FleetReleaseReport,
    plan: RolloutPlan,
    repo: Path,
    current_report_digest_resolver: ReportDigestResolver,
    control_runner: Runner = subprocess.run,
) -> PlanBoundHoldSupersession:
    """Permanently cover one obsolete journal-only scheduler hold.

    This is the narrow recovery authorized by issue #431.  It never releases a
    drain or executes an update.  The only control call is the supported
    read-only drain snapshot, and the only write is one receipt in the named
    historical rollout journal after the report and whole journal are
    revalidated.
    """
    if (
        not isinstance(rollout_id_value, str)
        or not rollout_id_value
        or rollout_id_value != rollout_id_value.strip()
        or not isinstance(host, str)
        or not host
        or host != host.strip()
    ):
        raise FleetRolloutError(
            "plan-bound hold supersession requires exact non-empty rollout "
            "and host identities"
        )
    current_summary = fleet_release.report_summary(accepted_report)
    if plan.report != current_summary:
        raise FleetRolloutError(
            "plan-bound hold supersession plan does not bind the exact "
            "accepted report"
        )
    current_rollout_id = rollout_id(accepted_report)
    if current_report_digest_resolver() != accepted_report.digest_sha256:
        raise FleetRolloutError(
            "accepted report changed before plan-bound hold supersession"
        )

    with _rollout_execution_lock(rollout_id_value):
        run, expected_journal_digest = _load_run_with_digest(rollout_id_value)
        if run is None or expected_journal_digest is None:
            raise FleetRolloutError(
                f"obsolete rollout journal {rollout_id_value!r} does not exist"
            )
        _validate_journal_records(run)
        hold = run.holds.get(host)
        if not isinstance(hold, Mapping):
            raise FleetRolloutError(
                f"obsolete rollout {rollout_id_value} has no hold for "
                f"exact host {host!r}"
            )

        existing = run.legacy_retained_holds.get(host)
        if existing is not None:
            if not isinstance(existing, Mapping):
                raise FleetRolloutError(
                    f"obsolete rollout {rollout_id_value} has malformed "
                    f"hold receipt for {host!r}"
                )
            if existing.get("schema") != _PLAN_BOUND_HOLD_SUPERSEDE_SCHEMA:
                raise FleetRolloutError(
                    f"obsolete rollout {rollout_id_value} already has a "
                    f"different hold receipt for {host!r}"
                )
            _validate_plan_bound_hold_supersede_receipt(
                run,
                host=host,
                hold=hold,
                raw=existing,
            )
            try:
                receipt_report = (
                    fleet_release.discover_historical_report_by_digest(
                        str(existing["current_report_path"]),
                        str(existing["current_report_digest_sha256"]),
                        repo,
                        fetch=False,
                    )
                )
            except fleet_release.FleetReleaseError as exc:
                raise FleetRolloutError(
                    "plan-bound hold supersede receipt report could not be "
                    f"authenticated: {exc}"
                ) from exc
            if rollout_id(receipt_report) != existing["current_rollout_id"]:
                raise FleetRolloutError(
                    "plan-bound hold supersede receipt report identity changed"
                )
            _validate_plan_bound_hold_supersede_receipt(
                run,
                host=host,
                hold=hold,
                raw=existing,
                current_report=receipt_report,
            )
            return PlanBoundHoldSupersession(
                rollout_id=rollout_id_value,
                host=host,
                current_rollout_id=str(existing["current_rollout_id"]),
                replayed=True,
            )

        if run.legacy_retained_actions:
            raise FleetRolloutError(
                f"obsolete rollout {rollout_id_value} retains an unproven "
                "legacy action group; reconcile that group before one hold"
            )
        historical_version = _accepted_report_version(run.report_source_path)
        current_version = _accepted_report_version(accepted_report.source_path)
        if (
            run.report_digest_sha256 == accepted_report.digest_sha256
            or historical_version is None
            or current_version is None
            or current_version <= historical_version
        ):
            raise FleetRolloutError(
                f"rollout {rollout_id_value} is not strictly older than the "
                "current accepted report"
            )
        try:
            historical = fleet_release.discover_historical_report_by_digest(
                run.report_source_path,
                run.report_digest_sha256,
                repo,
                fetch=False,
            )
        except fleet_release.FleetReleaseError as exc:
            raise FleetRolloutError(
                f"obsolete rollout {rollout_id_value} historical report "
                f"could not be authenticated: {exc}"
            ) from exc
        if not _historical_report_binds_run(historical, run):
            raise FleetRolloutError(
                f"obsolete rollout {rollout_id_value} does not bind its "
                "exact accepted report"
            )
        action_host, control_host = (
            _validate_exact_plan_bound_scheduler_hold(
                run,
                host=host,
                hold=hold,
                plan=plan,
            )
        )
        action_evidence = _healthy_scheduler_supersede_actions(
            plan,
            action_host=action_host,
            accepted_report=accepted_report,
        )
        observed = _observe_obsolete_legacy_scheduler_hold(
            run,
            host=host,
            hold=hold,
            control_host=control_host,
            live_release_block=None,
            runner=control_runner,
        )
        if observed.disposition != "inactive" or observed.observed_at is None:
            raise FleetRolloutError(
                f"plan-bound scheduler hold for {host} in {rollout_id_value} "
                "is not proven inactive by a complete supported read-only "
                f"drain snapshot: {observed.detail or observed.disposition}"
            )
        if current_report_digest_resolver() != accepted_report.digest_sha256:
            raise FleetRolloutError(
                "accepted report changed during plan-bound hold supersession"
            )
        current, current_journal_digest = _load_run_with_digest(
            rollout_id_value
        )
        if (
            current is None
            or current_journal_digest != expected_journal_digest
            or current.as_dict() != run.as_dict()
        ):
            raise FleetRolloutError(
                f"obsolete rollout journal {rollout_id_value} changed during "
                "plan-bound hold observation"
            )
        receipt = {
            "schema": _PLAN_BOUND_HOLD_SUPERSEDE_SCHEMA,
            "source": "plan-bound-supersede",
            "rollout_id": run.rollout_id,
            "host": host,
            "action_host": action_host,
            "kind": "scheduler-target",
            "control_host": control_host,
            "historical_report_path": run.report_source_path,
            "historical_report_digest_sha256": run.report_digest_sha256,
            "hold_record_sha256": _legacy_subtree_sha256(hold),
            "current_report_path": accepted_report.source_path,
            "current_report_digest_sha256": accepted_report.digest_sha256,
            "current_rollout_id": current_rollout_id,
            "current_actions": action_evidence,
            "live_drain_inactive": True,
            "observed_at": observed.observed_at,
            "recorded_at": utcnow_iso(),
        }
        _validate_plan_bound_hold_supersede_receipt(
            current,
            host=host,
            hold=hold,
            raw=receipt,
            current_report=accepted_report,
        )
        current.legacy_retained_holds[host] = receipt
        current.complete = False
        save_run(current)
        persisted = load_run(rollout_id_value)
        if persisted is None:
            raise FleetRolloutError(
                "plan-bound hold supersede receipt disappeared after save"
            )
        persisted_hold = persisted.holds.get(host)
        persisted_receipt = persisted.legacy_retained_holds.get(host)
        if not isinstance(persisted_hold, Mapping) or not isinstance(
            persisted_receipt, Mapping
        ):
            raise FleetRolloutError(
                "plan-bound hold supersede receipt was not durably persisted"
            )
        _validate_plan_bound_hold_supersede_receipt(
            persisted,
            host=host,
            hold=persisted_hold,
            raw=persisted_receipt,
            current_report=accepted_report,
        )
        return PlanBoundHoldSupersession(
            rollout_id=rollout_id_value,
            host=host,
            current_rollout_id=current_rollout_id,
            replayed=False,
        )


def _failure_report_ref(
    report: fleet_release.FleetReleaseReport,
) -> legacy_failure_transition.ReportRef:
    return legacy_failure_transition.ReportRef(
        source_path=report.source_path,
        digest_sha256=report.digest_sha256,
        rollout_id=rollout_id(report),
    )


def _observe_local_failure_report_epoch(repo: Path) -> _FailureReportEpoch:
    """Observe the selected report and local ref without fetching.

    The rollout controller holds its checkout lifecycle lock across this
    observation. Compliant local fetch/update paths therefore cannot move the
    ref while the token is used; a remote push after the fetched epoch belongs
    to the next invocation because the remote hold has no joint lease with Git.
    """
    try:
        report = fleet_release.discover_latest_report(repo, fetch=False)
        fleet_release.require_latest_report(report)
        proc = subprocess.run(
            [
                "git",
                "-C",
                str(repo.resolve()),
                "rev-parse",
                "--verify",
                "origin/main^{commit}",
            ],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
            stdin=subprocess.DEVNULL,
        )
    except (OSError, subprocess.SubprocessError, fleet_release.FleetReleaseError) as exc:
        raise FleetRolloutError(
            f"could not observe the local accepted-report epoch: {exc}"
        ) from exc
    origin_main_commit = proc.stdout.strip()
    if proc.returncode != 0 or _SOURCE_SHA.fullmatch(origin_main_commit) is None:
        detail = (proc.stderr or proc.stdout or "(no output)").strip()
        raise FleetRolloutError(
            f"could not observe the local origin/main epoch: {detail}"
        )
    return _FailureReportEpoch(
        origin_main_commit=origin_main_commit,
        report_source_path=report.source_path,
        report_digest_sha256=report.digest_sha256,
        rollout_id=rollout_id(report),
    )


def _failure_token_for_loaded_run(
    rollout_id_value: str,
    run: RolloutRun | None,
) -> legacy_failure_transition.JournalToken:
    if run is None:
        return legacy_failure_transition.JournalToken(rollout_id_value, None)
    try:
        digest = object.__getattribute__(run, _FAILURE_JOURNAL_DIGEST_ATTR)
    except AttributeError as exc:
        raise FleetRolloutError(
            f"failure recovery journal {rollout_id_value} lacks its read-bound token"
        ) from exc
    if type(digest) is not str or _SOURCE_TREE_SHA256.fullmatch(digest) is None:
        raise FleetRolloutError(
            f"failure recovery journal {rollout_id_value} has an invalid read-bound token"
        )
    return legacy_failure_transition.JournalToken(
        rollout_id_value,
        digest,
    )


def _load_failure_run_and_token(
    rollout_id_value: str,
) -> tuple[RolloutRun | None, legacy_failure_transition.JournalToken]:
    run, digest = _load_run_with_digest(rollout_id_value)
    return run, legacy_failure_transition.JournalToken(rollout_id_value, digest)


def _revalidate_failure_context(
    *,
    expected_vector: legacy_failure_transition.MutationVector,
    expected_context_token: tuple[tuple[str, str, str], ...],
    expected_current_identity: tuple[str, str, str],
    expected_source_refs: frozenset[tuple[str, str, str]],
    old_rollout_ids: Sequence[str],
    latest_report: fleet_release.FleetReleaseReport,
    host: str,
) -> None:
    """Recheck context membership and the full journal vector from one inventory."""
    snapshots = _all_rollout_run_snapshots()
    snapshot_runs = {run.rollout_id: run for _path, run, _token in snapshots}
    snapshot_tokens = {token.rollout_id: token for _path, _run, token in snapshots}
    try:
        old_runs = [snapshot_runs[rollout] for rollout in old_rollout_ids]
    except KeyError as exc:
        raise FleetRolloutError(
            f"failure recovery journal {exc.args[0]} disappeared before guarded CAS"
        ) from exc
    current_id, current_path, current_digest, context_token = _failure_current_identity(
        latest=latest_report,
        host=host,
        old_runs=old_runs,
        source_refs=expected_source_refs,
        context_snapshots=snapshots,
    )
    if (
        current_id,
        current_path,
        current_digest,
    ) != expected_current_identity or context_token != expected_context_token:
        raise FleetRolloutError("failure-transition receipt context changed before guarded CAS")
    observed = tuple(
        snapshot_tokens[token.rollout_id]
        if token.rollout_id in snapshot_tokens
        else _load_failure_run_and_token(token.rollout_id)[1]
        for token in expected_vector.journals
    )
    if observed != expected_vector.journals:
        raise FleetRolloutError("failure-transition journal vector changed before guarded CAS")


def _revalidate_failure_hold_absence(
    checks: Sequence[tuple[RolloutRun, str, Mapping[str, Any]]],
    *,
    runner: Runner,
) -> None:
    """Reauthenticate each proved OLD hold after all earlier callbacks."""
    for run, host, hold in checks:
        outcome, _active = _failure_status_observation(
            run,
            host=host,
            hold=hold,
            runner=runner,
        )
        if outcome != "inactive":
            raise FleetRolloutError(
                f"failure hold for {host} is no longer absent before guarded CAS"
            )


def _revalidate_failure_joint_freshness(
    *,
    repo: Path,
    plan: RolloutPlan,
    current_report_digest_resolver: ReportDigestResolver,
    hold_absence_checks: Sequence[
        tuple[RolloutRun, str, Mapping[str, Any]]
    ],
    control_runner: Runner,
    expected_vector: legacy_failure_transition.MutationVector,
    expected_context_token: tuple[tuple[str, str, str], ...],
    expected_current_identity: tuple[str, str, str],
    expected_source_refs: frozenset[tuple[str, str, str]],
    old_rollout_ids: Sequence[str],
    latest_report: fleet_release.FleetReleaseReport,
    host: str,
) -> None:
    """Linearize the fetched report, final hold status, and local CAS."""
    _validate_current_report(plan, current_report_digest_resolver)
    expected_epoch = _observe_local_failure_report_epoch(repo)
    accepted_identity = (
        latest_report.source_path,
        latest_report.digest_sha256,
        rollout_id(latest_report),
    )
    if (
        type(expected_epoch) is not _FailureReportEpoch
        or (
            expected_epoch.report_source_path,
            expected_epoch.report_digest_sha256,
            expected_epoch.rollout_id,
        )
        != accepted_identity
    ):
        raise FleetRolloutError(
            "local accepted-report epoch differs from the fetched report: "
            f"{expected_epoch!r} != {accepted_identity!r}"
        )
    _revalidate_failure_hold_absence(
        hold_absence_checks,
        runner=control_runner,
    )
    observed_epoch = _observe_local_failure_report_epoch(repo)
    if type(observed_epoch) is not _FailureReportEpoch or observed_epoch != expected_epoch:
        raise FleetRolloutError(
            "local accepted-report epoch changed after final hold status"
        )
    _revalidate_failure_context(
        expected_vector=expected_vector,
        expected_context_token=expected_context_token,
        expected_current_identity=expected_current_identity,
        expected_source_refs=expected_source_refs,
        old_rollout_ids=old_rollout_ids,
        latest_report=latest_report,
        host=host,
    )


def _guarded_save_run(
    target_rollout_id: str,
    *,
    authenticated_runs: Mapping[str, RolloutRun],
    expected_vector: legacy_failure_transition.MutationVector,
    expected_context_token: tuple[tuple[str, str, str], ...],
    expected_current_identity: tuple[str, str, str],
    expected_source_refs: frozenset[tuple[str, str, str]],
    old_rollout_ids: Sequence[str],
    latest_report: fleet_release.FleetReleaseReport,
    repo: Path,
    plan: RolloutPlan,
    current_report_digest_resolver: ReportDigestResolver,
    hold_absence_checks: Sequence[
        tuple[RolloutRun, str, Mapping[str, Any]]
    ],
    control_runner: Runner,
    transition: legacy_failure_transition.HostTransition,
) -> tuple[RolloutRun, legacy_failure_transition.RecoveryEffect]:
    """Persist one planned effect after the final invocation-wide CAS."""
    path = rollout_state_path(target_rollout_id)
    _revalidate_failure_joint_freshness(
        repo=repo,
        plan=plan,
        current_report_digest_resolver=current_report_digest_resolver,
        hold_absence_checks=hold_absence_checks,
        control_runner=control_runner,
        expected_vector=expected_vector,
        expected_context_token=expected_context_token,
        expected_current_identity=expected_current_identity,
        expected_source_refs=expected_source_refs,
        old_rollout_ids=old_rollout_ids,
        latest_report=latest_report,
        host=transition.host,
    )
    try:
        authorized = transition.authorized_effects()
    except legacy_failure_transition.TransitionError as exc:
        raise FleetRolloutError(f"failure transition authorization failed: {exc}") from exc
    if len(authorized) != 1 or isinstance(
        authorized[0], legacy_failure_transition.Reject
    ):
        if len(authorized) == 1 and isinstance(
            authorized[0], legacy_failure_transition.Reject
        ):
            raise FleetRolloutError(
                f"failure transition rejected: {authorized[0].reason}"
            )
        raise FleetRolloutError("guarded journal CAS lacks one mutation effect")
    effect = authorized[0]
    if effect.expected_vector is not expected_vector:
        raise FleetRolloutError("guarded journal CAS has another mutation vector")
    if isinstance(
        effect,
        legacy_failure_transition.WriteCurrentAckAndIntent
        | legacy_failure_transition.ClearForwardIntent,
    ):
        effect_rollout_id = effect.report.rollout_id
    else:
        assert isinstance(
            effect,
            legacy_failure_transition.WriteOldBacklink
            | legacy_failure_transition.ConsumeMember,
        )
        effect_rollout_id = effect.old_report.rollout_id
    if effect_rollout_id != target_rollout_id:
        raise FleetRolloutError("guarded journal CAS candidate differs from effect")
    candidate = _apply_failure_effect(
        effect,
        authenticated_runs=authenticated_runs,
    )
    encoded = json.dumps(candidate.as_dict(), indent=2, sort_keys=True) + "\n"
    paths.atomic_write_text(path, encoded)
    return candidate, effect


def _authenticate_failure_report(
    *,
    source_path: str,
    digest_sha256: str,
    rollout_id_value: str,
    repo: Path,
) -> fleet_release.FleetReleaseReport:
    try:
        report = fleet_release.discover_historical_report_by_digest(
            source_path,
            digest_sha256,
            repo,
            fetch=False,
        )
    except (KeyError, fleet_release.FleetReleaseError) as exc:
        raise FleetRolloutError(
            f"historical report could not be authenticated for {rollout_id_value}"
        ) from exc
    if (
        type(report) is not fleet_release.FleetReleaseReport
        or not fleet_release.same_report(report.source_path, source_path)
        or report.digest_sha256 != digest_sha256
        or rollout_id(report) != rollout_id_value
    ):
        raise FleetRolloutError(
            f"historical report identity conflicts with journal {rollout_id_value}"
        )
    # Carry the report under the spelling its journal recorded. Discovery
    # returns whichever location it found the blob at, and every ReportRef,
    # intent member and backlink built from here on is compared against
    # journals -- so letting the discovered spelling through would persist a
    # record that disagrees with its own source. Identity is already proven
    # above, and nothing downstream locates the blob by this path.
    return replace(report, source_path=source_path)


def _failure_plan_actions(
    plan: RolloutPlan,
    *,
    host: str,
) -> tuple[legacy_failure_transition.PlanActionRef, ...]:
    return tuple(
        legacy_failure_transition.PlanActionRef(
            action_id=action.id,
            phase=action.phase,
            host=action.host,
            program=action.program,
            decision=action.decision,
            reason=action.reason,
            pin_name=action.pin_name,
            target_sha=action.target_sha,
            target_version=action.target_version,
            target_tag=action.target_tag,
            argv=tuple(action.argv),
            before=action.before,
        )
        for action in plan.actions
        if action.host == host
    )


def _failure_action_host(action_id: str) -> str:
    """Classify one exact action ID without trusting allowed membership."""
    parts = action_id.split(":")
    if (
        len(parts) == 3
        and parts[0] == "helper"
        and parts[1]
        and parts[2]
    ):
        return parts[1]
    _phase, host, _program = _legacy_action_parts(action_id)
    return host


def _validate_failure_current_host_state(
    run: RolloutRun,
    *,
    host: str,
    allowed_action_ids: frozenset[str],
) -> None:
    """Close the recovering host while preserving proved sibling state."""
    for action_id in run.actions:
        try:
            action_host = _failure_action_host(action_id)
        except FleetRolloutError as exc:
            raise FleetRolloutError(
                f"receipt current journal has unclassifiable action {action_id!r}"
            ) from exc
        if action_id in allowed_action_ids:
            if action_host != host:
                raise FleetRolloutError(
                    "receipt current journal allowed action names another host"
                )
            continue
        if action_host == host:
            raise FleetRolloutError(
                f"receipt current journal has unrelated {host} action {action_id!r}"
            )
    if host in run.holds:
        raise FleetRolloutError(
            f"receipt current journal retains a hold for recovering host {host}"
        )
    if not isinstance(run.legacy_retained_holds, Mapping):
        raise FleetRolloutError(
            "receipt current journal retained hold state is not an object"
        )
    for retained_host, receipt in run.legacy_retained_holds.items():
        if (
            not isinstance(retained_host, str)
            or not isinstance(receipt, Mapping)
            or receipt.get("host") != retained_host
        ):
            raise FleetRolloutError(
                "receipt current journal has unclassifiable retained hold state"
            )
        if retained_host == host:
            raise FleetRolloutError(
                f"receipt current journal retains hold evidence for {host}"
            )
    if not isinstance(run.legacy_retained_actions, Mapping):
        raise FleetRolloutError(
            "receipt current journal retained action state is not an object"
        )
    for action_id, receipt in run.legacy_retained_actions.items():
        if not isinstance(action_id, str) or not isinstance(receipt, Mapping):
            raise FleetRolloutError(
                "receipt current journal has unclassifiable retained action state"
            )
        try:
            _phase, action_host, _program = _legacy_action_parts(action_id)
        except FleetRolloutError as exc:
            raise FleetRolloutError(
                "receipt current journal has unclassifiable retained action state"
            ) from exc
        if (
            receipt.get("action_id") not in {None, action_id}
            or receipt.get("host") != action_host
        ):
            raise FleetRolloutError(
                "receipt current journal retained action identity disagrees"
            )
        if action_host == host:
            raise FleetRolloutError(
                f"receipt current journal retains action evidence for {host}"
            )


def _failure_current_identity(
    *,
    latest: fleet_release.FleetReleaseReport,
    host: str,
    old_runs: Sequence[RolloutRun],
    source_refs: frozenset[tuple[str, str, str]],
    context_snapshots: Sequence[_FailureJournalSnapshot],
) -> tuple[str, str, str, tuple[tuple[str, str, str], ...]]:
    """Select the durable receipt journal, if one already exists."""
    contexts: set[tuple[str, str, str]] = set()
    for run in old_runs:
        for action_id, raw in run.actions.items():
            if not isinstance(raw, Mapping):
                continue
            backlink = raw.get("legacy_failure_update_ack")
            if backlink is None:
                continue
            try:
                parsed = legacy_failure_transition.parse_failure_update_backlink(backlink)
            except legacy_failure_transition.TransitionError as exc:
                raise FleetRolloutError("historical failure backlink is malformed") from exc
            try:
                action_host = _failure_action_host(action_id)
            except FleetRolloutError as exc:
                raise FleetRolloutError(
                    "historical failure backlink action identity is malformed"
                ) from exc
            if (
                parsed.host != action_host
                or action_id not in parsed.failed_action_ids
                or not fleet_release.same_report(
                    parsed.historical_report_path, run.report_source_path
                )
                or parsed.historical_report_digest_sha256
                != run.report_digest_sha256
            ):
                raise FleetRolloutError(
                    "historical failure backlink does not bind its source action"
                )
            if parsed.host != host:
                continue
            contexts.add(
                (
                    parsed.current_rollout_id,
                    parsed.current_report_path,
                    parsed.current_report_digest_sha256,
                )
            )

    observed_source_refs = {
        (run.rollout_id, run.report_source_path, run.report_digest_sha256)
        for run in old_runs
    }
    if observed_source_refs != source_refs:
        raise FleetRolloutError("failure source journal report identity changed")
    source_ids = {item[0] for item in source_refs}
    source_paths = {item[1] for item in source_refs}
    source_digests = {item[2] for item in source_refs}
    context_tokens: list[tuple[str, str, str]] = []
    for path, candidate, token in context_snapshots:
        raw = candidate.legacy_failure_update_intent
        if raw is None:
            continue
        if not isinstance(raw, Mapping):
            raise FleetRolloutError("failure forward intent is malformed")
        raw_sources = raw.get("sources")
        source_candidates = (
            (raw_sources,)
            if isinstance(raw_sources, Mapping)
            else raw_sources
            if isinstance(raw_sources, list)
            else ()
        )
        overlaps_source = any(
            isinstance(item, Mapping)
            and (
                item.get("historical_rollout_id") in source_ids
                or item.get("historical_report_path") in source_paths
                or item.get("historical_report_digest_sha256") in source_digests
            )
            for item in source_candidates
        )
        raw_host = raw.get("host")
        if type(raw_host) is str and raw_host != host and not overlaps_source:
            continue
        try:
            intent = legacy_failure_transition.parse_failure_update_intent(raw)
        except legacy_failure_transition.TransitionError as exc:
            raise FleetRolloutError("failure forward intent is malformed") from exc
        if intent.host != host:
            if overlaps_source:
                raise FleetRolloutError(
                    "failure forward intent source membership names another host"
                )
            continue
        assert token.digest_sha256 is not None
        context_tokens.append((path.name, token.rollout_id, token.digest_sha256))
        if (
            candidate.rollout_id != intent.current_rollout_id
            or not fleet_release.same_report(
                candidate.report_source_path, intent.current_report_path
            )
            or candidate.report_digest_sha256 != intent.current_report_digest_sha256
        ):
            raise FleetRolloutError("failure forward intent conflicts with its journal")
        intent_sources = {
            (
                member.historical_rollout_id,
                member.historical_report_path,
                member.historical_report_digest_sha256,
            )
            for member in intent.sources
        }
        if intent_sources != source_refs:
            raise FleetRolloutError("failure forward intent source membership disagrees")
        contexts.add(
            (
                intent.current_rollout_id,
                intent.current_report_path,
                intent.current_report_digest_sha256,
            )
        )
    if len(contexts) > 1:
        raise FleetRolloutError("historical failure receipt contexts disagree")
    if contexts:
        current = next(iter(contexts))
    else:
        current = (rollout_id(latest), latest.source_path, latest.digest_sha256)
    return (*current, tuple(context_tokens))


def _failure_mutation_target(
    transition: legacy_failure_transition.HostTransition,
    recovery: legacy_failure_transition.HostRecoveryInput,
) -> str:
    """Derive the next journal target without inspecting an effect payload."""
    if transition.host != recovery.host:
        raise FleetRolloutError("failure transition host changed before persistence")
    if transition.phase is legacy_failure_transition.TransitionPhase.PREVALIDATED:
        return recovery.latest_report.ref.rollout_id
    if transition.phase in {
        legacy_failure_transition.TransitionPhase.ACKED,
        legacy_failure_transition.TransitionPhase.BACKLINKING,
    }:
        snapshots = {
            snapshot.report.rollout_id: snapshot
            for snapshot in recovery.old_snapshots
        }
        for group in sorted(
            recovery.failures,
            key=lambda item: (item.report.rollout_id, item.host),
        ):
            snapshot = snapshots[group.report.rollout_id]
            if any(
                "legacy_failure_update_ack"
                not in snapshot.relevant_action_records[action_id]
                for action_id in group.failed_action_ids
            ):
                return group.report.rollout_id
    if transition.phase is legacy_failure_transition.TransitionPhase.CONSUMING:
        for group in sorted(
            recovery.failures,
            key=lambda item: (item.report.rollout_id, item.host),
        ):
            if group.pending_attempts:
                return group.report.rollout_id
    if transition.phase is legacy_failure_transition.TransitionPhase.COMPLETE:
        if recovery.current_snapshot is None:
            raise FleetRolloutError("complete failure transition lacks a current journal")
        return recovery.current_snapshot.report.rollout_id
    raise FleetRolloutError("failure transition phase has no mutation target")


def _failure_status_observation(
    run: RolloutRun,
    *,
    host: str,
    hold: Mapping[str, Any],
    runner: Runner,
) -> tuple[Literal["inactive", "active", "unavailable"], dict[str, Any] | None]:
    candidate = copy.deepcopy(run)
    try:
        active = _recover_local_hold_identity(
            candidate,
            host=host,
            hold=hold,
            runner=runner,
            save_state=False,
            force_observe=True,
        )
    except (FleetRolloutError, OSError, subprocess.SubprocessError):
        return "unavailable", None
    return ("inactive", None) if active is None else ("active", active)


def _settled_failure_hold(
    *,
    run: RolloutRun,
    host: str,
    hold: Mapping[str, Any],
    runner: Runner,
) -> tuple[
    legacy_failure_transition.HoldProofKind,
    dict[str, Any],
    bool,
]:
    outcome, active = _failure_status_observation(
        run,
        host=host,
        hold=hold,
        runner=runner,
    )
    if outcome == "unavailable":
        raise FleetRolloutError(f"exact full hold for {host} could not be authenticated")
    changed = False
    proof_kind = legacy_failure_transition.HoldProofKind.OBSERVED_INACTIVE
    proof_outcome = "observed-inactive"
    if outcome == "active":
        assert active is not None
        expected_reason = active.get("reason")
        expected_set_at = active.get("set_at")
        if not isinstance(expected_reason, str) or not isinstance(expected_set_at, str):
            raise FleetRolloutError(f"exact full hold for {host} lacks reason/set_at identity")
        argv = [
            "drain",
            "--release",
            "--scheduler-host",
            host,
            "--release-legacy-only",
            "--expected-legacy-set-at",
            expected_set_at,
            "--expected-legacy-reason",
            expected_reason,
            str(active.get("control_host") or host),
        ]
        try:
            released = _run_control(argv, runner=runner)
        except (OSError, subprocess.SubprocessError) as exc:
            raise FleetRolloutError(
                f"exact conditional legacy release unavailable for {host}: {exc}"
            ) from exc
        if released.returncode != 0:
            detail = (released.stderr or released.stdout or "(no output)").strip()
            raise FleetRolloutError(f"exact conditional legacy release failed for {host}: {detail}")
        confirmation, _unused = _failure_status_observation(
            run,
            host=host,
            hold=hold,
            runner=runner,
        )
        if confirmation != "inactive":
            raise FleetRolloutError(f"post-release absence could not be authenticated for {host}")
        proof_kind = legacy_failure_transition.HoldProofKind.CONDITIONAL_ABSENCE_CONFIRMED
        proof_outcome = "conditional-absence-confirmed"
        changed = True
    settled = {
        "host": host,
        "kind": "full",
        "owned": True,
        "status": "released",
        "legacy_reconciliation": {"outcome": proof_outcome},
    }
    return proof_kind, settled, changed


def _apply_failure_effect(
    effect: legacy_failure_transition.RecoveryEffect,
    *,
    authenticated_runs: Mapping[str, RolloutRun],
) -> RolloutRun:
    if isinstance(effect, legacy_failure_transition.WriteCurrentAckAndIntent):
        existing = authenticated_runs.get(effect.report.rollout_id)
        run = (
            copy.deepcopy(existing)
            if existing is not None
            else RolloutRun(
                rollout_id=effect.report.rollout_id,
                report_digest_sha256=effect.report.digest_sha256,
                report_source_path=effect.report.source_path,
            )
        )
        for action_id, row in effect.action_replacements:
            _replace_action_record(
                run,
                action_id,
                legacy_failure_transition.thaw_json(row),
            )
        run.failed_hosts[effect.host] = effect.failed_reason
        run.legacy_failure_update_intent = effect.intent.as_dict()
        run.complete = False
        return run
    if isinstance(effect, legacy_failure_transition.WriteOldBacklink):
        existing = authenticated_runs.get(effect.old_report.rollout_id)
        run = copy.deepcopy(existing) if existing is not None else None
        if run is None:
            raise FleetRolloutError("historical journal disappeared before backlink")
        run.holds[effect.host] = legacy_failure_transition.thaw_json(effect.settled_hold)
        run.legacy_retained_holds.pop(effect.host, None)
        for edit in effect.action_edits:
            raw = run.actions.get(edit.action_id)
            if not isinstance(raw, Mapping):
                raise FleetRolloutError("historical action disappeared before backlink")
            row = copy.deepcopy(dict(raw))
            for key in edit.delete_keys:
                row.pop(key, None)
            for key, value in edit.set_items:
                row[key] = legacy_failure_transition.thaw_json(value)
            _replace_action_record(run, edit.action_id, row)
        run.complete = False
        return run
    if isinstance(effect, legacy_failure_transition.ConsumeMember):
        existing = authenticated_runs.get(effect.old_report.rollout_id)
        run = copy.deepcopy(existing) if existing is not None else None
        if run is None:
            raise FleetRolloutError("historical journal disappeared before consume")
        for attempt_ref in effect.attempts:
            raw = run.actions.get(attempt_ref.action_id)
            if not isinstance(raw, Mapping):
                raise FleetRolloutError("historical action disappeared before consume")
            row = copy.deepcopy(dict(raw))
            attempts = _operation_attempts(row)
            matched = 0
            for attempt in attempts:
                if attempt["operation_id"] == attempt_ref.operation_id:
                    attempt["failure_fence_consumed"] = True
                    attempt["failure_retry_authorized"] = False
                    matched += 1
            if matched != 1:
                raise FleetRolloutError("exact historical failure fence disappeared")
            row["operation_attempts"] = attempts
            _replace_action_record(run, attempt_ref.action_id, row)
        return run
    if isinstance(effect, legacy_failure_transition.ClearForwardIntent):
        existing = authenticated_runs.get(effect.report.rollout_id)
        run = copy.deepcopy(existing) if existing is not None else None
        if run is None or run.legacy_failure_update_intent is None:
            raise FleetRolloutError("forward intent disappeared before clear")
        if (
            legacy_failure_transition.canonical_json_sha256(run.legacy_failure_update_intent)
            != effect.intent_sha256
        ):
            raise FleetRolloutError("forward intent changed before clear")
        run.legacy_failure_update_intent = None
        return run
    raise FleetRolloutError("failure transition did not yield a mutation effect")


def _validate_failure_hold_receipt(
    run: RolloutRun,
    *,
    host: str,
    hold: Mapping[str, Any],
) -> None:
    retained = run.legacy_retained_holds.get(host)
    if retained is None:
        return
    if (
        not isinstance(retained, Mapping)
        or retained.get("host") != host
        or retained.get("kind") != "full"
        or not fleet_release.same_report(
            retained.get("historical_report_path"), run.report_source_path
        )
        or retained.get("historical_report_digest_sha256") != run.report_digest_sha256
        or retained.get("hold_record_sha256") != _legacy_subtree_sha256(hold)
    ):
        raise FleetRolloutError(
            f"rollout {run.rollout_id} has incoherent retained full-hold receipt"
        )


def _persisted_failure_skip_state(
    run: RolloutRun,
    *,
    host: str,
    action_id: str,
    attempts: Sequence[Mapping[str, Any]],
    current: RolloutAction | None,
    current_report_path: str,
    current_report_digest_sha256: str,
) -> legacy_failure_transition.SkipState:
    """Strictly classify one closed persisted failure-skip receipt."""
    raw_record = run.actions[action_id]
    if "legacy_failure_skip" not in raw_record:
        return legacy_failure_transition.SkipState.ABSENT
    evidence = raw_record.get("legacy_failure_skip")
    expected_fields = {
        "schema",
        "action_id",
        "host",
        "failed_rollout_id",
        "operation_ids",
        "historical_report_path",
        "historical_report_digest_sha256",
        "current_report_path",
        "current_report_digest_sha256",
        "current_target_sha",
        "current_action_reason",
        "observed_current_sha",
        "recorded_at",
    }
    expected_operations = sorted(str(item["operation_id"]) for item in attempts)
    if not isinstance(evidence, Mapping):
        raise FleetRolloutError(
            f"terminal failure fence for {host} has malformed or tampered "
            "persisted skip evidence"
        )
    current_path = evidence.get("current_report_path")
    current_digest = evidence.get("current_report_digest_sha256")
    current_target = evidence.get("current_target_sha")
    current_reason = evidence.get("current_action_reason")
    observed_current = evidence.get("observed_current_sha")
    intrinsic_skip_is_coherent = bool(
        (
            current_reason == "already at target with LAST OK=true"
            and observed_current == current_target
        )
        or (
            isinstance(current_reason, str)
            and current_reason.endswith("already deployed; no downgrade")
            and observed_current != current_target
        )
    )
    immutable_valid = bool(
        set(evidence) == expected_fields
        and evidence.get("schema") == _LEGACY_FAILURE_SKIP_SCHEMA
        and evidence.get("action_id") == action_id
        and evidence.get("host") == host
        and evidence.get("failed_rollout_id") == run.rollout_id
        and evidence.get("operation_ids") == expected_operations
        and fleet_release.same_report(
            evidence.get("historical_report_path"), run.report_source_path
        )
        and evidence.get("historical_report_digest_sha256")
        == run.report_digest_sha256
        and fleet_release.is_report_path(current_path)
        and isinstance(current_digest, str)
        and _SOURCE_TREE_SHA256.fullmatch(current_digest) is not None
        and isinstance(current_target, str)
        and _SOURCE_SHA.fullmatch(current_target) is not None
        and isinstance(current_reason, str)
        and current_reason.strip()
        and isinstance(observed_current, str)
        and _SOURCE_SHA.fullmatch(observed_current) is not None
        and intrinsic_skip_is_coherent
        and isinstance(evidence.get("recorded_at"), str)
        and str(evidence.get("recorded_at")).strip()
    )
    if not immutable_valid:
        raise FleetRolloutError(
            f"terminal failure fence for {host} has malformed or tampered "
            "persisted skip evidence"
        )
    if (
        not fleet_release.same_report(current_path, current_report_path)
        or current_digest != current_report_digest_sha256
    ):
        return legacy_failure_transition.SkipState.STALE
    if (
        current is None
        or current.host != host
        or not _skip_proves_live_success(current)
        or current.before.get("configured") is not True
        or current.before.get("last_ok") is not True
    ):
        return legacy_failure_transition.SkipState.STALE
    if (
        evidence.get("current_target_sha") != current.target_sha
        or evidence.get("current_action_reason") != current.reason
        or evidence.get("observed_current_sha") != current.before.get("current_sha")
    ):
        raise FleetRolloutError(
            f"terminal failure fence for {host} has tampered current-report skip evidence"
        )
    return legacy_failure_transition.SkipState.CURRENT


def _failure_group_from_run(
    run: RolloutRun,
    *,
    host: str,
    reason: str,
    report: fleet_release.FleetReleaseReport,
    operation_ids: frozenset[str],
    current_actions: Mapping[str, RolloutAction],
    current_report_path: str,
    current_report_digest_sha256: str,
) -> legacy_failure_transition.HostFailureGroup:
    attempts: list[legacy_failure_transition.FailureAttemptRef] = []
    skip_states: list[tuple[str, legacy_failure_transition.SkipState]] = []
    for action_id, raw in sorted(run.actions.items()):
        if not isinstance(raw, Mapping) or raw.get("status") != "failed":
            continue
        try:
            action_host = _failure_action_host(action_id)
        except FleetRolloutError as exc:
            raise FleetRolloutError(
                f"failed action {action_id} has no exact host identity"
            ) from exc
        row_attempts = _operation_attempts(raw)
        if not row_attempts:
            raise FleetRolloutError(f"failed action {action_id} lacks a durable operation receipt")
        for attempt in row_attempts:
            operation_id_value = str(attempt["operation_id"])
            if operation_id_value not in operation_ids:
                raise FleetRolloutError(
                    f"failed operation {operation_id_value} is absent from inventory"
                )
            try:
                observed = fleet_operation.observe_operation(
                    operation_id_value,
                    recover=True,
                )
            except fleet_operation.OperationError as exc:
                raise FleetRolloutError(
                    f"failed operation {operation_id_value} could not be observed"
                ) from exc
            identity = _identity_from_observation(observed)
            result = observed.result or {}
            if (
                identity.rollout_id != run.rollout_id
                or identity.action_id != action_id
                or identity.host != action_host
                or identity.as_dict() != attempt.get("identity")
                or _request_sha256(observed) != attempt.get("request_sha256")
                or observed.state != "completed"
                or attempt.get("harvested") is not True
                or result.get("executed") is not True
                or result.get("returncode") in {None, 0}
                or type(attempt.get("failure_fence_consumed")) is not bool
            ):
                raise FleetRolloutError(
                    f"failed operation {operation_id_value} no longer binds "
                    "its authenticated terminal receipt"
                )
            if action_host == host:
                if attempt.get("failure_retry_authorized") is not False:
                    raise FleetRolloutError(
                        f"failed operation {operation_id_value} authorized retry"
                    )
                attempts.append(
                    legacy_failure_transition.FailureAttemptRef(
                        action_id=action_id,
                        operation_id=operation_id_value,
                        consumed=bool(attempt["failure_fence_consumed"]),
                        retry_authorized=False,
                    )
                )
        skip_state = _persisted_failure_skip_state(
            run,
            host=action_host,
            action_id=action_id,
            attempts=row_attempts,
            current=current_actions.get(action_id),
            current_report_path=current_report_path,
            current_report_digest_sha256=current_report_digest_sha256,
        )
        if (
            action_host == host
            and skip_state is not legacy_failure_transition.SkipState.ABSENT
        ):
            skip_states.append((action_id, skip_state))
    if not attempts:
        raise FleetRolloutError(
            f"rollout {run.rollout_id} has no authenticated failed operations for {host}"
        )
    return legacy_failure_transition.HostFailureGroup(
        report=_failure_report_ref(report),
        host=host,
        reason=reason,
        attempts=tuple(attempts),
        skip_states=tuple(skip_states),
    )


def _reconcile_failure_transition_v2(
    *,
    accepted_report: fleet_release.FleetReleaseReport,
    legacy_hold_retries: Sequence[tuple[str, str]],
    failed_operation_hosts: Sequence[tuple[str, str, str]],
    plan: RolloutPlan,
    repo: Path,
    current_report_digest_resolver: ReportDigestResolver,
    control_runner: Runner,
) -> LegacyRolloutReconciliation:
    latest_path = plan.report.get("source_path")
    latest_digest = plan.report.get("digest_sha256")
    if (
        type(accepted_report) is not fleet_release.FleetReleaseReport
        or not fleet_release.same_report(latest_path, accepted_report.source_path)
        or latest_digest != accepted_report.digest_sha256
    ):
        raise FleetRolloutError("accepted report capability does not match the planned report")
    _validate_current_report(plan, current_report_digest_resolver)
    failure_rows = tuple(dict.fromkeys(failed_operation_hosts))
    failure_keys = {(rollout, host) for rollout, host, _reason in failure_rows}
    if len(failure_keys) != len(failure_rows):
        raise FleetRolloutError("failure recovery contains duplicate source rows")
    hosts = {host for _rollout, host, _reason in failure_rows}
    if len(hosts) != 1:
        raise FleetRolloutError("failure recovery must cover exactly one host")
    host = next(iter(hosts))
    current_actions = {action.id: action for action in plan.actions}
    participant_rows = tuple(
        dict.fromkeys(
            (
                *legacy_hold_retries,
                *((rollout, item_host) for rollout, item_host, _ in failure_rows),
            )
        )
    )
    participant_ids = {rollout for rollout, _item_host in participant_rows}
    released_live: set[tuple[str, str]] = set()
    settled_inactive: set[tuple[str, str]] = set()
    retained: set[tuple[str, str, str]] = set()
    conditional_proofs: dict[
        str,
        tuple[str, str, str, str, dict[str, Any]],
    ] = {}
    invocation_tokens: dict[str, str | None] | None = None

    for _phase in range(64):
        context_snapshots = _all_rollout_run_snapshots()
        snapshot_runs = {run.rollout_id: run for _path, run, _token in context_snapshots}
        snapshot_tokens = {token.rollout_id: token for _path, _run, token in context_snapshots}
        authenticated_runs: dict[str, RolloutRun] = {}
        loaded_tokens: dict[str, legacy_failure_transition.JournalToken] = {}
        for rollout_id_value, item_host in participant_rows:
            run = snapshot_runs.get(rollout_id_value)
            if run is None:
                raise FleetRolloutError(f"failure recovery journal {rollout_id_value} disappeared")
            loaded_tokens[rollout_id_value] = snapshot_tokens[rollout_id_value]
            _validate_journal_records(run)
            raw_hold = run.holds.get(item_host)
            if not isinstance(raw_hold, Mapping):
                raise FleetRolloutError(
                    f"failure recovery candidate lacks a full hold for {item_host}"
                )
            _validate_failure_hold_receipt(
                run,
                host=item_host,
                hold=raw_hold,
            )
            has_backlink = any(
                isinstance(raw, Mapping)
                and "legacy_failure_update_ack" in raw
                and _failure_action_host(action_id) == item_host
                for action_id, raw in run.actions.items()
            )
            if (rollout_id_value, item_host) in failure_keys and not has_backlink:
                _validate_exact_modern_full_hold_shape(
                    run,
                    host=item_host,
                    hold=raw_hold,
                )
            elif raw_hold.get("status") in {"active", "cleanup-failed"}:
                if _is_recoverable_legacy_full_hold(
                    run,
                    host=item_host,
                    hold=raw_hold,
                ):
                    _validate_recoverable_legacy_full_hold_shape(
                        run,
                        host=item_host,
                        hold=raw_hold,
                    )
                else:
                    _validate_exact_modern_full_hold_shape(
                        run,
                        host=item_host,
                        hold=raw_hold,
                    )
            authenticated_runs[rollout_id_value] = run

        old_runs = [authenticated_runs[rollout] for rollout, _host, _ in failure_rows]
        source_refs = frozenset(
            (run.rollout_id, run.report_source_path, run.report_digest_sha256) for run in old_runs
        )
        current_id, current_path, current_digest, context_token = _failure_current_identity(
            latest=accepted_report,
            host=host,
            old_runs=old_runs,
            source_refs=source_refs,
            context_snapshots=context_snapshots,
        )
        current_identity = (current_id, current_path, current_digest)
        current_run = snapshot_runs.get(current_id)
        loaded_tokens[current_id] = snapshot_tokens.get(
            current_id,
            legacy_failure_transition.JournalToken(current_id, None),
        )
        if current_run is not None and (
            not fleet_release.same_report(current_run.report_source_path, current_path)
            or current_run.report_digest_sha256 != current_digest
        ):
            raise FleetRolloutError("receipt current journal report changed")
        persisted_current_intent = None
        if current_run is not None:
            if current_run.legacy_failure_update_intent is None:
                backlink_action_ids: set[tuple[str, ...]] = set()
                for old_run in old_runs:
                    for raw_record in old_run.actions.values():
                        if not isinstance(raw_record, Mapping):
                            continue
                        raw_backlink = raw_record.get("legacy_failure_update_ack")
                        if raw_backlink is None:
                            continue
                        try:
                            backlink = (
                                legacy_failure_transition.parse_failure_update_backlink(
                                    raw_backlink
                                )
                            )
                        except legacy_failure_transition.TransitionError as exc:
                            raise FleetRolloutError(
                                "historical failure backlink is malformed"
                            ) from exc
                        if backlink.host == host:
                            backlink_action_ids.add(backlink.current_action_ids)
                if len(backlink_action_ids) > 1:
                    raise FleetRolloutError(
                        "historical failure backlink action contexts disagree"
                    )
                allowed_current_action_ids = (
                    frozenset(next(iter(backlink_action_ids)))
                    if backlink_action_ids
                    else frozenset(
                        action.id for action in plan.actions if action.host == host
                    )
                )
            else:
                try:
                    persisted_current_intent = (
                        legacy_failure_transition.parse_failure_update_intent(
                            current_run.legacy_failure_update_intent
                        )
                    )
                except legacy_failure_transition.TransitionError as exc:
                    raise FleetRolloutError(
                        "receipt current forward intent is malformed"
                    ) from exc
                allowed_current_action_ids = frozenset(
                    persisted_current_intent.current_action_ids
                )
            _validate_journal_records(
                current_run,
                recognized_action_ids=allowed_current_action_ids,
            )
            _validate_failure_current_host_state(
                current_run,
                host=host,
                allowed_action_ids=allowed_current_action_ids,
            )
        vector_ids = sorted({current_id, *participant_ids})
        tokens = tuple(loaded_tokens[item] for item in vector_ids)
        observed_tokens = {token.rollout_id: token.digest_sha256 for token in tokens}
        if invocation_tokens is None:
            invocation_tokens = dict(observed_tokens)
        elif observed_tokens != invocation_tokens:
            raise FleetRolloutError("failure-transition invocation vector changed between phases")
        latest = legacy_failure_transition.LatestReportRef(
            ref=_failure_report_ref(accepted_report),
            discovery_token=(
                f"accepted:{accepted_report.source_path}@{accepted_report.digest_sha256}"
            ),
        )
        vector = legacy_failure_transition.MutationVector(
            latest=latest,
            journals=tokens,
        )

        authenticated_reports: dict[str, fleet_release.FleetReleaseReport] = {}
        for rollout_id_value, run in authenticated_runs.items():
            authenticated_reports[rollout_id_value] = _authenticate_failure_report(
                source_path=run.report_source_path,
                digest_sha256=run.report_digest_sha256,
                rollout_id_value=rollout_id_value,
                repo=repo,
            )
        receipt_report = _authenticate_failure_report(
            source_path=current_path,
            digest_sha256=current_digest,
            rollout_id_value=current_id,
            repo=repo,
        )
        if current_run is not None:
            authenticated_runs[current_id] = current_run
        elif vector.token_for(current_id).digest_sha256 is not None:
            raise FleetRolloutError("receipt current journal changed during authentication")

        listed_operations = frozenset(fleet_operation.list_operation_ids(recover=True))
        failure_groups: list[legacy_failure_transition.HostFailureGroup] = []
        old_snapshots: list[legacy_failure_transition.JournalHostSnapshot] = []
        proofs: list[legacy_failure_transition.HoldProof] = []
        hold_absence_checks: list[
            tuple[RolloutRun, str, Mapping[str, Any]]
        ] = []
        sorted_failure_rows = tuple(sorted(failure_rows))
        for rollout_id_value, item_host, reason in sorted_failure_rows:
            run = authenticated_runs[rollout_id_value]
            report = authenticated_reports[rollout_id_value]
            failure_groups.append(
                _failure_group_from_run(
                    run,
                    host=item_host,
                    reason=reason,
                    report=report,
                    operation_ids=listed_operations,
                    current_actions=current_actions,
                    current_report_path=str(plan.report["source_path"]),
                    current_report_digest_sha256=str(plan.report["digest_sha256"]),
                )
            )
        if any(
            state is legacy_failure_transition.SkipState.CURRENT
            for group in failure_groups
            for _action_id, state in group.skip_states
        ):
            raise FleetRolloutError(
                "failure transition rejected: old failure action has a current skip"
            )
        for rollout_id_value, item_host in participant_rows:
            if (rollout_id_value, item_host) in failure_keys:
                continue
            run = authenticated_runs[rollout_id_value]
            hold = run.holds[item_host]
            outcome, _active = _failure_status_observation(
                run,
                host=item_host,
                hold=hold,
                runner=control_runner,
            )
            if outcome == "unavailable":
                retained.add(
                    (
                        rollout_id_value,
                        item_host,
                        "live full-hold identity remains unavailable",
                    )
                )
        recorded_at = utcnow_iso()
        _validate_current_report(plan, current_report_digest_resolver)
        for (rollout_id_value, item_host, _reason), group in zip(
            sorted_failure_rows,
            failure_groups,
            strict=True,
        ):
            run = authenticated_runs[rollout_id_value]
            hold = run.holds[item_host]
            has_backlink = any(
                isinstance(run.actions.get(action_id), Mapping)
                and "legacy_failure_update_ack" in run.actions[action_id]
                for action_id in group.failed_action_ids
            )
            if has_backlink:
                outcome, _active = _failure_status_observation(
                    run,
                    host=item_host,
                    hold=hold,
                    runner=control_runner,
                )
                if outcome != "inactive":
                    raise FleetRolloutError(
                        f"released failure hold for {item_host} is no longer absent"
                    )
                settled = copy.deepcopy(dict(hold))
                cached = conditional_proofs.get(rollout_id_value)
                if cached is not None and settled != cached[4]:
                    raise FleetRolloutError(
                        "persisted conditional hold differs from authenticated proof"
                    )
                proof_kind = legacy_failure_transition.HoldProofKind.EXISTING_BACKLINK
            else:
                cached_conditional = conditional_proofs.get(rollout_id_value)
                persisted_proof: tuple[
                    legacy_failure_transition.HoldProofKind,
                    dict[str, Any],
                ] | None = None
                if current_run is not None and (
                    current_run.legacy_failure_update_intent is not None
                ):
                    persisted_intent = (
                        legacy_failure_transition.parse_failure_update_intent(
                            current_run.legacy_failure_update_intent
                        )
                    )
                    persisted_member = next(
                        (
                            item
                            for item in persisted_intent.sources
                            if item.historical_rollout_id == rollout_id_value
                        ),
                        None,
                    )
                    if persisted_member is None:
                        raise FleetRolloutError(
                            "persisted failure intent lacks its OLD hold proof"
                        )
                    proof_candidates = (
                        (
                            legacy_failure_transition.HoldProofKind.OBSERVED_INACTIVE,
                            "observed-inactive",
                        ),
                        (
                            legacy_failure_transition.HoldProofKind.CONDITIONAL_ABSENCE_CONFIRMED,
                            "conditional-absence-confirmed",
                        ),
                    )
                    for candidate_kind, candidate_outcome in proof_candidates:
                        candidate = {
                            "host": item_host,
                            "kind": "full",
                            "owned": True,
                            "status": "released",
                            "legacy_reconciliation": {
                                "outcome": candidate_outcome
                            },
                        }
                        if (
                            legacy_failure_transition.canonical_json_sha256(
                                candidate
                            )
                            == persisted_member.settled_hold_sha256
                        ):
                            persisted_proof = (candidate_kind, candidate)
                            break
                    if persisted_proof is None:
                        raise FleetRolloutError(
                            "persisted failure intent hold proof is not canonical"
                        )
                if cached_conditional is not None or persisted_proof is not None:
                    if cached_conditional is not None:
                        proof_kind = (
                            legacy_failure_transition.HoldProofKind.CONDITIONAL_ABSENCE_CONFIRMED
                        )
                        settled = copy.deepcopy(cached_conditional[4])
                        cached_identity = cached_conditional[:4]
                        observed_identity = (
                            item_host,
                            run.report_source_path,
                            run.report_digest_sha256,
                            _legacy_subtree_sha256(hold),
                        )
                        if observed_identity != cached_identity:
                            raise FleetRolloutError(
                                "conditional hold identity changed before backlink"
                            )
                        if persisted_proof is not None and (
                            persisted_proof[0] is not proof_kind
                            or persisted_proof[1] != settled
                        ):
                            raise FleetRolloutError(
                                "persisted failure hold proof changed"
                            )
                    else:
                        assert persisted_proof is not None
                        proof_kind, persisted_settled = persisted_proof
                        settled = copy.deepcopy(persisted_settled)
                    outcome, _active = _failure_status_observation(
                        run,
                        host=item_host,
                        hold=hold,
                        runner=control_runner,
                    )
                    if outcome != "inactive":
                        raise FleetRolloutError(
                            f"settled failure hold for {item_host} is no longer absent"
                        )
                    changed = False
                else:
                    proof_kind, settled, changed = _settled_failure_hold(
                        run=run,
                        host=item_host,
                        hold=hold,
                        runner=control_runner,
                    )
                    if changed:
                        conditional_proofs[rollout_id_value] = (
                            item_host,
                            run.report_source_path,
                            run.report_digest_sha256,
                            _legacy_subtree_sha256(hold),
                            copy.deepcopy(settled),
                        )
                settled_inactive.add((rollout_id_value, item_host))
                if changed:
                    released_live.add((rollout_id_value, item_host))
            proofs.append(
                legacy_failure_transition.HoldProof(
                    old_rollout_id=rollout_id_value,
                    kind=proof_kind,
                    settled_hold=settled,
                    settled_hold_sha256=(legacy_failure_transition.canonical_json_sha256(settled)),
                )
            )
            hold_absence_checks.append((run, item_host, hold))
            old_snapshots.append(
                legacy_failure_transition.JournalHostSnapshot(
                    report=group.report,
                    host=item_host,
                    token=vector.token_for(rollout_id_value),
                    relevant_action_records={
                        action_id: record
                        for action_id, record in run.actions.items()
                        if action_id in group.failed_action_ids
                        or (
                            isinstance(record, Mapping)
                            and "legacy_failure_update_ack" in record
                            and _failure_action_host(action_id) == item_host
                        )
                    },
                    hold=hold,
                    retained_hold=run.legacy_retained_holds.get(item_host),
                    failed_reason=run.failed_hosts.get(item_host),
                    complete=run.complete,
                )
            )

        current_snapshot = None
        receipt_capability = None
        if current_run is not None:
            current_ref = _failure_report_ref(receipt_report)
            relevant_actions = {
                action_id: row
                for action_id, row in current_run.actions.items()
                if action_id in allowed_current_action_ids
            }
            current_snapshot = legacy_failure_transition.JournalHostSnapshot(
                report=current_ref,
                host=host,
                token=vector.token_for(current_id),
                relevant_action_records=relevant_actions,
                hold=current_run.holds.get(host),
                retained_hold=current_run.legacy_retained_holds.get(host),
                failed_reason=current_run.failed_hosts.get(host),
                complete=current_run.complete,
                forward_intent=current_run.legacy_failure_update_intent,
            )
            receipt_capability = legacy_failure_transition.ReceiptReportRef(
                ref=current_ref,
                authentication_token=(f"authenticated:{current_path}@{current_digest}"),
            )
        has_intent = bool(
            current_run is not None and current_run.legacy_failure_update_intent is not None
        )
        all_consumed = all(
            attempt.consumed for group in failure_groups for attempt in group.attempts
        )
        plan_actions = () if has_intent or all_consumed else _failure_plan_actions(plan, host=host)
        try:
            recovery = legacy_failure_transition.HostRecoveryInput(
                latest_report=latest,
                host=host,
                plan_actions=plan_actions,
                failures=tuple(failure_groups),
                old_snapshots=tuple(old_snapshots),
                current_snapshot=current_snapshot,
                hold_proofs=tuple(proofs),
                mutation_vector=vector,
                recorded_at=recorded_at,
                receipt_report=receipt_capability,
            )
            transition = legacy_failure_transition.plan_host_transition(recovery)
        except legacy_failure_transition.TransitionError as exc:
            raise FleetRolloutError(f"failure transition rejected: {exc}") from exc
        if (
            transition.phase is legacy_failure_transition.TransitionPhase.COMPLETE
            and (
                recovery.current_snapshot is None
                or recovery.current_snapshot.forward_intent is None
            )
        ):
            _revalidate_failure_joint_freshness(
                repo=repo,
                plan=plan,
                current_report_digest_resolver=current_report_digest_resolver,
                hold_absence_checks=hold_absence_checks,
                control_runner=control_runner,
                expected_vector=vector,
                expected_context_token=context_token,
                expected_current_identity=current_identity,
                expected_source_refs=source_refs,
                old_rollout_ids=tuple(run.rollout_id for run in old_runs),
                latest_report=accepted_report,
                host=host,
            )
            try:
                terminal_effects = transition.authorized_effects()
            except legacy_failure_transition.TransitionError as exc:
                raise FleetRolloutError(
                    f"failure transition authorization failed: {exc}"
                ) from exc
            if terminal_effects:
                raise FleetRolloutError(
                    "terminal failure transition unexpectedly yielded an effect"
                )
            return LegacyRolloutReconciliation(
                settled_inactive_holds=tuple(sorted(settled_inactive)),
                released_live_holds=tuple(sorted(released_live)),
                retained_holds=tuple(sorted(retained)),
                live_hold_state_changed=bool(released_live),
                live_hold_refresh_required=bool(released_live),
            )
        target_rollout_id = _failure_mutation_target(transition, recovery)
        candidate, effect = _guarded_save_run(
            target_rollout_id,
            authenticated_runs=authenticated_runs,
            expected_vector=vector,
            expected_context_token=context_token,
            expected_current_identity=current_identity,
            expected_source_refs=source_refs,
            old_rollout_ids=tuple(run.rollout_id for run in old_runs),
            latest_report=accepted_report,
            repo=repo,
            plan=plan,
            current_report_digest_resolver=current_report_digest_resolver,
            hold_absence_checks=hold_absence_checks,
            control_runner=control_runner,
            transition=transition,
        )
        assert invocation_tokens is not None
        invocation_tokens[candidate.rollout_id] = legacy_failure_transition.canonical_json_sha256(
            candidate.as_dict()
        )
        if isinstance(effect, legacy_failure_transition.ClearForwardIntent):
            return LegacyRolloutReconciliation(
                settled_inactive_holds=tuple(sorted(settled_inactive)),
                released_live_holds=tuple(sorted(released_live)),
                retained_holds=tuple(sorted(retained)),
                live_hold_state_changed=bool(released_live),
                live_hold_refresh_required=bool(released_live),
            )
    raise FleetRolloutError("failure transition exceeded its bounded phase count")


def reconcile_legacy_rollout_state(
    *,
    legacy_running_actions: Sequence[tuple[str, str]],
    legacy_scheduler_holds: Sequence[tuple[str, str]] = (),
    legacy_hold_retries: Sequence[tuple[str, str]] = (),
    failed_operation_hosts: Sequence[tuple[str, str, str]] = (),
    plan: RolloutPlan,
    admin_status: Mapping[str, Any],
    repo: Path,
    current_report_digest_resolver: ReportDigestResolver,
    control_runner: Runner = subprocess.run,
    accepted_report: fleet_release.FleetReleaseReport | None = None,
) -> LegacyRolloutReconciliation:
    """Safely supersede pre-recorder actions from fresh exact-lane evidence.

    This function is called only by the explicit, mutating
    ``--reconcile-legacy`` rollout path while the fleet-wide coordinator lock
    is held.  It never invents a subprocess result.  The historical action is
    authenticated against its committed accepted report, then a fresh current
    plan must prove that the same exact lane is healthy and no admin-update
    marker overlaps it.  Only then is ``running`` replaced by
    ``superseded``/``unknown``.

    Pre-recorder full holds lack the ``set_at`` identity required for a blind
    release.  An inactive live snapshot settles only the journal.  An active
    snapshot is releasable only after its exact reason and ``set_at`` have
    been recovered; unreachable or contradictory hosts remain retained.
    """
    refs = tuple(dict.fromkeys(legacy_running_actions))
    scheduler_refs = tuple(dict.fromkeys(legacy_scheduler_holds))
    hold_retry_refs = tuple(dict.fromkeys(legacy_hold_retries))
    failure_refs = tuple(dict.fromkeys(failed_operation_hosts))
    if accepted_report is not None and failure_refs:
        if refs or scheduler_refs:
            raise FleetRolloutError(
                "failure transition cannot mix legacy action or scheduler-hold references"
            )
        return _reconcile_failure_transition_v2(
            accepted_report=accepted_report,
            legacy_hold_retries=hold_retry_refs,
            failed_operation_hosts=failure_refs,
            plan=plan,
            repo=repo,
            current_report_digest_resolver=current_report_digest_resolver,
            control_runner=control_runner,
        )
    if not refs and not scheduler_refs and not hold_retry_refs and not failure_refs:
        return LegacyRolloutReconciliation()
    legacy_ref_set = frozenset(refs)
    _validate_current_report(plan, current_report_digest_resolver)
    if (
        not isinstance(plan.report.get("source_path"), str)
        or not str(plan.report["source_path"]).strip()
        or not isinstance(plan.report.get("digest_sha256"), str)
        or _SOURCE_TREE_SHA256.fullmatch(
            str(plan.report["digest_sha256"])
        )
        is None
    ):
        raise FleetRolloutError(
            "legacy reconciliation requires the exact current accepted-report "
            "path and digest"
        )
    current_actions = {action.id: action for action in plan.actions}
    skip_report_path = str(plan.report["source_path"])
    skip_report_digest = str(plan.report["digest_sha256"])
    failure_reasons = {
        (rollout_id_value, host): reason
        for rollout_id_value, host, reason in failure_refs
    }
    historical_reports: dict[str, fleet_release.FleetReleaseReport] = {}
    prepared: list[_LegacyActionRecovery] = []
    authenticated_runs: dict[str, RolloutRun] = {}

    def pending_failed_attempts(
        run: RolloutRun,
        *,
        host: str,
    ) -> dict[str, list[dict[str, Any]]]:
        pending: dict[str, list[dict[str, Any]]] = {}
        for action_id, raw_record in run.actions.items():
            if not isinstance(raw_record, Mapping):
                continue
            for attempt in _operation_attempts(raw_record):
                identity = fleet_operation.OperationIdentity.from_dict(
                    attempt["identity"]
                )
                if (
                    identity.host == host
                    and attempt["harvested"] is True
                    and attempt["failure_fence_consumed"] is False
                    and _verified_failed_attempt(
                        run,
                        action_id=action_id,
                        attempt=attempt,
                        expected_host=host,
                    )
                ):
                    pending.setdefault(action_id, []).append(attempt)
        return pending

    def current_failure_skip_detail(
        run: RolloutRun,
        *,
        host: str,
        require_persisted: bool,
    ) -> str | None:
        pending = pending_failed_attempts(run, host=host)
        if not pending:
            return "terminal failure fence has no pending exact attempt"
        for action_id, attempts in pending.items():
            current = current_actions.get(action_id)
            if (
                current is None
                or current.host != host
                or not _skip_proves_live_success(current)
                or current.before.get("configured") is not True
                or current.before.get("last_ok") is not True
            ):
                return (
                    "terminal failure fence has no matching healthy current-"
                    "plan host-local skip"
                )
            marker = _legacy_action_marker_reason(admin_status, current)
            if marker is not None:
                return f"terminal failure fence overlaps admin marker: {marker}"
            for attempt in attempts:
                if not _verified_failed_attempt(
                    run,
                    action_id=action_id,
                    attempt=attempt,
                    expected_host=host,
                ):
                    return "terminal failure fence is not an executed failure"
            if not require_persisted:
                continue
            if _persisted_failure_skip_state(
                run,
                host=host,
                action_id=action_id,
                attempts=attempts,
                current=current,
                current_report_path=skip_report_path,
                current_report_digest_sha256=skip_report_digest,
            ) is not legacy_failure_transition.SkipState.CURRENT:
                return (
                    "terminal failure fence lacks its exact persisted current-"
                    "plan host-local skip"
                )
        return None

    def persist_current_failure_skip(
        run: RolloutRun,
        *,
        host: str,
    ) -> str | None:
        detail = current_failure_skip_detail(
            run,
            host=host,
            require_persisted=False,
        )
        if detail is not None:
            return detail
        recorded_at = utcnow_iso()
        for action_id, attempts in pending_failed_attempts(
            run, host=host
        ).items():
            current = current_actions[action_id]
            state = _persisted_failure_skip_state(
                run,
                host=host,
                action_id=action_id,
                attempts=attempts,
                current=current,
                current_report_path=skip_report_path,
                current_report_digest_sha256=skip_report_digest,
            )
            if state is legacy_failure_transition.SkipState.CURRENT:
                continue
            raw_record = run.actions[action_id]
            assert isinstance(raw_record, Mapping)
            _replace_action_record(
                run,
                action_id,
                {
                    **raw_record,
                    "legacy_failure_skip": {
                        "schema": _LEGACY_FAILURE_SKIP_SCHEMA,
                        "action_id": action_id,
                        "host": host,
                        "failed_rollout_id": run.rollout_id,
                        "operation_ids": sorted(
                            str(attempt["operation_id"])
                            for attempt in attempts
                        ),
                        "historical_report_path": run.report_source_path,
                        "historical_report_digest_sha256": (
                            run.report_digest_sha256
                        ),
                        "current_report_path": plan.report.get(
                            "source_path"
                        ),
                        "current_report_digest_sha256": plan.report.get(
                            "digest_sha256"
                        ),
                        "current_target_sha": current.target_sha,
                        "current_action_reason": current.reason,
                        "observed_current_sha": current.before.get(
                            "current_sha"
                        ),
                        "recorded_at": recorded_at,
                    },
                },
            )
        return current_failure_skip_detail(
            run,
            host=host,
            require_persisted=True,
        )

    failure_candidates: dict[
        tuple[str, str], _LegacyFailureRecoveryCandidate
    ] = {}
    for rollout_id_value, host, reason in failure_refs:
        run = load_run(rollout_id_value)
        if run is None:
            raise FleetRolloutError(
                f"failed rollout journal {rollout_id_value} disappeared "
                "during global prevalidation"
            )
        _validate_journal_records(run)
        known_run = authenticated_runs.get(rollout_id_value)
        if known_run is not None and known_run.as_dict() != run.as_dict():
            raise FleetRolloutError(
                f"failed rollout journal {rollout_id_value} changed during "
                "global prevalidation"
            )
        authenticated_runs[rollout_id_value] = run
        historical = _authenticate_journal_report(
            run,
            repo=repo,
            cache=historical_reports,
            subject=f"failed rollout {rollout_id_value}",
            binding_failure=(
                f"failed rollout {rollout_id_value} does not bind its exact "
                "committed accepted report"
            ),
        )
        pending = pending_failed_attempts(run, host=host)
        if not pending:
            raise FleetRolloutError(
                f"cannot prevalidate terminal failure fence for {host} in "
                f"{rollout_id_value}: no pending exact executed failure"
            )
        for action_id, attempts in pending.items():
            raw_record = run.actions[action_id]
            if "legacy_failure_skip" not in raw_record:
                continue
            _persisted_failure_skip_state(
                run,
                host=host,
                action_id=action_id,
                attempts=attempts,
                current=current_actions.get(action_id),
                current_report_path=skip_report_path,
                current_report_digest_sha256=skip_report_digest,
            )
        action_ids = tuple(sorted(pending))
        operation_ids = tuple(
            sorted(
                str(attempt["operation_id"])
                for attempts in pending.values()
                for attempt in attempts
            )
        )
        action_records = {
            action_id: run.actions[action_id] for action_id in action_ids
        }
        failure_candidates[(rollout_id_value, host)] = (
            _LegacyFailureRecoveryCandidate(
                rollout_id=rollout_id_value,
                host=host,
                reason=reason,
                report_source_path=run.report_source_path,
                report_digest_sha256=run.report_digest_sha256,
                host_hold_sha256=(
                    _legacy_subtree_sha256(run.holds[host])
                    if host in run.holds
                    else None
                ),
                action_ids=action_ids,
                operation_ids=operation_ids,
                action_records_sha256=_legacy_subtree_sha256(action_records),
            )
        )

    # Establish every action proof before the first journal mutation.  A
    # malformed second row therefore cannot leave a partially authorized
    # batch merely because it sorted after a valid first row.
    for rollout_id_value, action_id in refs:
        run = load_run(rollout_id_value)
        if run is None:
            raise FleetRolloutError(
                f"legacy rollout journal {rollout_id_value} disappeared"
            )
        _validate_journal_records(run)
        known_run = authenticated_runs.get(rollout_id_value)
        if known_run is not None and known_run.as_dict() != run.as_dict():
            raise FleetRolloutError(
                f"legacy rollout journal {rollout_id_value} changed during "
                "global prevalidation"
            )
        authenticated_runs[rollout_id_value] = run
        record_raw = run.actions.get(action_id)
        if not isinstance(record_raw, Mapping):
            raise FleetRolloutError(
                f"legacy rollout action {action_id} in {rollout_id_value} "
                "is missing or malformed"
            )
        record = dict(record_raw)
        if record.get("status") != "running" or _operation_attempts(record):
            raise FleetRolloutError(
                f"legacy rollout action {action_id} in {rollout_id_value} "
                "changed before reconciliation"
            )
        # A transitional journal that mixes a pre-recorder row with durable or
        # failed sibling work is not safely reducible to the legacy model. In
        # particular, its sibling host may retain a failure fence whose hold
        # must not be released by this recovery. Require that every non-legacy
        # sibling is terminal pre-recorder state with no operation attempts.
        for sibling_id, sibling_raw in run.actions.items():
            if sibling_id == action_id:
                continue
            if not isinstance(sibling_raw, Mapping):
                raise FleetRolloutError(
                    f"legacy rollout {rollout_id_value} has malformed sibling "
                    f"state at {sibling_id}; refusing action supersession or "
                    "hold release"
                )
            sibling_attempts = _operation_attempts(sibling_raw)
            sibling_is_legacy = (
                (rollout_id_value, sibling_id) in legacy_ref_set
                and sibling_raw.get("status") == "running"
                and not sibling_attempts
            )
            if sibling_is_legacy:
                continue
            terminal_legacy = (
                not sibling_attempts
                and sibling_raw.get("status") in {"success", "not-run"}
            )
            coherent_superseded = (
                not sibling_attempts
                and _coherent_superseded_legacy_record(
                    run, sibling_id, sibling_raw
                )
            )
            if not terminal_legacy and not coherent_superseded:
                raise FleetRolloutError(
                    f"legacy rollout {rollout_id_value} has mixed sibling "
                    f"state at {sibling_id}; refusing action supersession or "
                    "hold release"
                )
        for hold_host, hold_raw in run.holds.items():
            if (
                isinstance(hold_raw, Mapping)
                and hold_raw.get("kind") == "full"
                and hold_raw.get("owned") is True
                and hold_raw.get("status")
                in {"active", "cleanup-failed"}
            ):
                _validate_preowner_full_hold_shape(
                    run,
                    host=hold_host,
                    hold=hold_raw,
                )
        historical = _authenticate_journal_report(
            run,
            repo=repo,
            cache=historical_reports,
            subject=f"legacy rollout {rollout_id_value}",
            binding_failure=(
                f"legacy rollout {rollout_id_value} journal/report identity "
                "does not match the committed accepted report"
            ),
        )
        expected_argv = _legacy_action_expected_argv(action_id, historical)
        if (
            set(record) != {"argv", "decision", "reason", "status"}
            or record.get("decision") != "update"
            or record.get("reason") != "target is newer"
            or not isinstance(record.get("argv"), list)
            or not all(isinstance(value, str) for value in record["argv"])
            or record.get("argv") != expected_argv
        ):
            raise FleetRolloutError(
                f"legacy rollout action {action_id} in {rollout_id_value} "
                "does not match its historical accepted-report lane"
            )
        phase, host, program = _legacy_action_parts(action_id)
        current = current_actions.get(action_id)
        detail: str | None = None
        if run.failed_hosts:
            detail = "historical rollout records one or more failed hosts"
        elif current is None:
            detail = "current accepted plan has no matching lane"
        elif (
            current.phase != phase
            or current.host != host
            or current.program != program
        ):
            detail = "current accepted plan lane identity does not match"
        elif (
            not _skip_proves_live_success(current)
            or current.before.get("configured") is not True
            or current.before.get("last_ok") is not True
        ):
            detail = "current plan does not prove a healthy superseding lane"
        else:
            marker = _legacy_action_marker_reason(admin_status, current)
            if marker is not None:
                detail = f"overlapping admin-update marker: {marker}"
        prepared.append(
            _LegacyActionRecovery(
                rollout_id=rollout_id_value,
                action_id=action_id,
                record=record,
                phase=phase,
                host=host,
                program=program,
                current=current,
                disposition="retain" if detail is not None else "supersede",
                detail=detail,
            )
        )

    retained_rollout_ids = {
        recovery.rollout_id
        for recovery in prepared
        if recovery.disposition == "retain"
    }
    prepared_scheduler: list[_LegacySchedulerHoldRecovery] = []
    scheduler_probes: list[
        tuple[RolloutRun, str, dict[str, Any], str, str | None]
    ] = []
    # Authenticate and bind every claim before the first live observation.
    # A bad second row therefore cannot follow a valid first row into either a
    # control mutation or a journal write.
    for rollout_id_value, host in scheduler_refs:
        run = load_run(rollout_id_value)
        if run is None:
            raise FleetRolloutError(
                f"legacy rollout journal {rollout_id_value} disappeared"
            )
        _validate_journal_records(run)
        known_run = authenticated_runs.get(rollout_id_value)
        if known_run is not None and known_run.as_dict() != run.as_dict():
            raise FleetRolloutError(
                f"legacy rollout journal {rollout_id_value} changed during "
                "global prevalidation"
            )
        authenticated_runs[rollout_id_value] = run
        raw_hold = run.holds.get(host)
        if not isinstance(raw_hold, Mapping):
            raise FleetRolloutError(
                f"obsolete legacy scheduler claim for {host} in "
                f"{rollout_id_value} is missing or malformed"
            )
        historical = _authenticate_journal_report(
            run,
            repo=repo,
            cache=historical_reports,
            subject=f"legacy rollout {rollout_id_value}",
            binding_failure=(
                f"obsolete legacy scheduler claim for {host} in "
                f"{rollout_id_value} does not bind its exact committed "
                "accepted report and deterministic rollout ID"
            ),
        )
        control_host = _validate_obsolete_legacy_scheduler_hold(
            run,
            host=host,
            hold=raw_hold,
            plan=plan,
            legacy_action_refs=legacy_ref_set,
        )
        live_release_block = _legacy_scheduler_live_release_block(
            plan,
            host=host,
            admin_status=admin_status,
        )
        if rollout_id_value not in retained_rollout_ids:
            scheduler_probes.append(
                (
                    run,
                    host,
                    dict(raw_hold),
                    control_host,
                    live_release_block,
                )
            )

    full_retry_candidates: list[_LegacyFullHoldRecoveryCandidate] = []
    scheduler_ref_set = frozenset(scheduler_refs)
    for rollout_id_value, host in hold_retry_refs:
        if (rollout_id_value, host) in scheduler_ref_set:
            continue
        run = load_run(rollout_id_value)
        if run is None:
            raise FleetRolloutError(
                f"legacy rollout journal {rollout_id_value} disappeared"
            )
        _validate_journal_records(run)
        retention = _validate_legacy_retention_receipts(
            run,
            current_report_source_path=str(plan.report.get("source_path")),
            current_report_digest_sha256=str(
                plan.report.get("digest_sha256")
            ),
            allow_current_identity_mismatch=True,
        )
        raw_hold = run.holds.get(host)
        raw_receipt = run.legacy_retained_holds.get(host)
        exact_full = _journaled_exact_owner_holds(run).get(host)
        recoverable_legacy_full = bool(
            isinstance(raw_hold, Mapping)
            and _is_recoverable_legacy_full_hold(
                run,
                host=host,
                hold=raw_hold,
            )
        )
        full_shape: Literal["recovered-legacy", "exact-modern"] | None = None
        if isinstance(raw_hold, Mapping) and raw_hold.get("kind") == "full":
            if recoverable_legacy_full:
                _validate_recoverable_legacy_full_hold_shape(
                    run,
                    host=host,
                    hold=raw_hold,
                )
                full_shape = "recovered-legacy"
            elif exact_full == dict(raw_hold):
                _validate_exact_modern_full_hold_shape(
                    run,
                    host=host,
                    hold=raw_hold,
                )
                full_shape = "exact-modern"
            else:
                raise FleetRolloutError(
                    f"legacy retained hold for {host} in {rollout_id_value} "
                    "has malformed or ambiguous full-hold identity"
                )
        unreceipted_full = bool(
            isinstance(raw_hold, Mapping)
            and raw_hold.get("kind") == "full"
            and raw_receipt is None
            and host not in retention.hold_hosts
            and (
                exact_full == dict(raw_hold)
                or recoverable_legacy_full
            )
        )
        if not isinstance(raw_hold, Mapping) or (
            not isinstance(raw_receipt, Mapping) and not unreceipted_full
        ):
            raise FleetRolloutError(
                f"legacy retained hold for {host} in {rollout_id_value} "
                "is missing or malformed"
            )
        historical = _authenticate_journal_report(
            run,
            repo=repo,
            cache=historical_reports,
            subject=f"legacy rollout {rollout_id_value}",
            binding_failure=(
                f"legacy rollout {rollout_id_value} retained hold does not "
                "bind its committed accepted report"
            ),
        )
        known_run = authenticated_runs.get(rollout_id_value)
        if known_run is not None and known_run.as_dict() != run.as_dict():
            raise FleetRolloutError(
                f"legacy rollout journal {rollout_id_value} changed during "
                "global prevalidation"
            )
        authenticated_runs[rollout_id_value] = run
        if (
            raw_hold.get("kind") == "full"
            and rollout_id_value not in retained_rollout_ids
        ):
            assert full_shape is not None
            full_retry_candidates.append(
                _LegacyFullHoldRecoveryCandidate(
                    rollout_id=rollout_id_value,
                    host=host,
                    shape=full_shape,
                    hold_sha256=_legacy_subtree_sha256(raw_hold),
                )
            )

    # Read every provenance-bound snapshot before the first mutation. A
    # malformed or ambiguous answer leaves the entire batch byte-identical.
    for run, host, hold, control_host, live_release_block in scheduler_probes:
        prepared_scheduler.append(
            _observe_obsolete_legacy_scheduler_hold(
                run,
                host=host,
                hold=hold,
                control_host=control_host,
                live_release_block=live_release_block,
                runner=control_runner,
            )
        )

    # The global lock is the concurrency fence, but repeat the exact local
    # comparison once after every remote read and before the first write. This
    # keeps a malformed/changed last journal from following an earlier valid
    # journal into a partial recovery batch.
    for rollout_id_value, expected in authenticated_runs.items():
        current_run = load_run(rollout_id_value)
        if current_run is None or current_run.as_dict() != expected.as_dict():
            raise FleetRolloutError(
                f"legacy rollout journal {rollout_id_value} changed before "
                "the recovery batch could be written"
            )

    superseded: list[tuple[str, str]] = []
    retained_actions: list[tuple[str, str, str, str]] = []
    settled_inactive: list[tuple[str, str]] = []
    released_live: list[tuple[str, str]] = []
    retained: list[tuple[str, str, str]] = []
    affected_full_holds = {
        (candidate.rollout_id, candidate.host): candidate
        for candidate in full_retry_candidates
    }
    prepared_by_rollout: dict[str, list[_LegacyActionRecovery]] = {}
    for recovery in prepared:
        prepared_by_rollout.setdefault(recovery.rollout_id, []).append(recovery)

    for rollout_id_value, recoveries in prepared_by_rollout.items():
        _validate_current_report(plan, current_report_digest_resolver)
        run = load_run(rollout_id_value)
        expected = authenticated_runs[rollout_id_value]
        if run is None or run.as_dict() != expected.as_dict():
            raise FleetRolloutError(
                f"legacy rollout journal {rollout_id_value} changed before "
                "its atomic reconciliation write"
            )
        if rollout_id_value in retained_rollout_ids:
            retained_at = utcnow_iso()
            receipts: dict[str, dict[str, Any]] = {}
            for recovery in recoveries:
                reason = recovery.detail or (
                    "same historical rollout contains another unproven "
                    "pre-recorder action; retaining the rollout as one group"
                )
                receipts[recovery.action_id] = (
                    _legacy_retained_action_receipt(
                        recovery,
                        run=run,
                        plan=plan,
                        retained_at=retained_at,
                        reason=reason,
                    )
                )
                retained_actions.append(
                    (
                        rollout_id_value,
                        recovery.action_id,
                        recovery.host,
                        reason,
                    )
                )
            hold_receipts: dict[str, dict[str, Any]] = {}
            for host, raw_hold in run.holds.items():
                if (
                    not isinstance(raw_hold, Mapping)
                    or raw_hold.get("owned") is not True
                    or raw_hold.get("status")
                    not in {"active", "cleanup-failed"}
                ):
                    continue
                reason = (
                    "same historical rollout contains an unproven "
                    "pre-recorder action; hold left untouched"
                )
                hold_receipts[host] = _legacy_retained_hold_receipt(
                    run,
                    host=host,
                    hold=raw_hold,
                    plan=plan,
                    source="retained-action-group",
                    control_host=None,
                    retained_at=retained_at,
                    reason=reason,
                )
                retained.append((rollout_id_value, host, reason))
            run.legacy_retained_actions = receipts
            run.legacy_retained_holds = hold_receipts
            run.complete = False
            save_run(run)
            continue

        for recovery in recoveries:
            current = recovery.current
            assert current is not None
            replacement = {
                **recovery.record,
                "status": "superseded",
                "observed_outcome": "unknown",
                "superseded_at": utcnow_iso(),
                "legacy_reconciliation": {
                    "schema": "vq.fleet.legacy_action_reconciliation/1",
                    "action_id": recovery.action_id,
                    "phase": current.phase,
                    "host": current.host,
                    "program": current.program,
                    "historical_report_path": run.report_source_path,
                    "historical_report_digest_sha256": (
                        run.report_digest_sha256
                    ),
                    "durable_operation_present": False,
                    "overlapping_admin_update_marker": False,
                    "current_report_path": plan.report.get("source_path"),
                    "current_report_digest_sha256": plan.report.get(
                        "digest_sha256"
                    ),
                    "current_action_reason": current.reason,
                    "current_identity_relation": (
                        "equal"
                        if current.reason == "already at target with LAST OK=true"
                        else "ahead"
                    ),
                    "current_target_sha": current.target_sha,
                    "observed_current_sha": current.before.get("current_sha"),
                    "outcome_claim": "not-observed",
                },
            }
            if not _coherent_superseded_legacy_record(
                run, recovery.action_id, replacement
            ):
                raise FleetRolloutError(
                    f"current plan evidence for legacy action "
                    f"{recovery.action_id} in {rollout_id_value} cannot form "
                    "a coherent persisted supersession record"
                )
            _replace_action_record(run, recovery.action_id, replacement)
            superseded.append((rollout_id_value, recovery.action_id))
        run.legacy_retained_actions = {}
        pending_hold_receipts = {
            host: receipt
            for host, receipt in run.legacy_retained_holds.items()
            if receipt.get("source") != "retained-action-group"
        }
        pending_at = utcnow_iso()
        for host, raw_hold in run.holds.items():
            if (
                isinstance(raw_hold, Mapping)
                and raw_hold.get("owned") is True
                and raw_hold.get("status")
                in {"active", "cleanup-failed"}
            ):
                pending_hold_receipts[host] = _legacy_retained_hold_receipt(
                    run,
                    host=host,
                    hold=raw_hold,
                    plan=plan,
                    source="hold-observation",
                    control_host=str(raw_hold.get("control_host") or host),
                    retained_at=pending_at,
                    reason=(
                        "legacy action superseded; exact hold cleanup is "
                        "pending supported observation"
                    ),
                )
        run.legacy_retained_holds = pending_hold_receipts
        run.complete = False
        save_run(run)
        for host, raw_hold in run.holds.items():
            if (
                isinstance(raw_hold, Mapping)
                and raw_hold.get("kind") == "full"
                and raw_hold.get("owned") is True
                and raw_hold.get("status") in {"active", "cleanup-failed"}
            ):
                recoverable_legacy_full = _is_recoverable_legacy_full_hold(
                    run,
                    host=host,
                    hold=raw_hold,
                )
                if recoverable_legacy_full:
                    _validate_recoverable_legacy_full_hold_shape(
                        run,
                        host=host,
                        hold=raw_hold,
                    )
                    shape: Literal[
                        "recovered-legacy", "exact-modern"
                    ] = "recovered-legacy"
                elif _journaled_exact_owner_holds(run).get(host) == dict(
                    raw_hold
                ):
                    _validate_exact_modern_full_hold_shape(
                        run,
                        host=host,
                        hold=raw_hold,
                    )
                    shape = "exact-modern"
                else:
                    raise FleetRolloutError(
                        f"legacy full hold for {host} in {rollout_id_value} "
                        "has malformed or ambiguous recovery identity"
                    )
                affected_full_holds.setdefault(
                    (rollout_id_value, host),
                    _LegacyFullHoldRecoveryCandidate(
                        rollout_id=rollout_id_value,
                        host=host,
                        shape=shape,
                        hold_sha256=_legacy_subtree_sha256(raw_hold),
                    ),
                )

    live_changed = False
    live_refresh_required = False

    def retain_scheduler_recovery(
        recovery: _LegacySchedulerHoldRecovery,
        reason: str,
    ) -> None:
        _validate_current_report(plan, current_report_digest_resolver)
        run = load_run(recovery.rollout_id)
        if run is None or run.holds.get(recovery.host) != recovery.hold:
            raise FleetRolloutError(
                f"obsolete legacy scheduler claim for {recovery.host} in "
                f"{recovery.rollout_id} changed before retention"
            )
        run.legacy_retained_holds[recovery.host] = (
            _legacy_retained_hold_receipt(
                run,
                host=recovery.host,
                hold=recovery.hold,
                plan=plan,
                source="scheduler-observation",
                control_host=recovery.control_host,
                retained_at=utcnow_iso(),
                reason=reason,
            )
        )
        run.complete = False
        save_run(run)
        retained.append((recovery.rollout_id, recovery.host, reason))

    for recovery in prepared_scheduler:
        _validate_current_report(plan, current_report_digest_resolver)
        run = load_run(recovery.rollout_id)
        if run is None or run.holds.get(recovery.host) != recovery.hold:
            raise FleetRolloutError(
                f"obsolete legacy scheduler claim for {recovery.host} in "
                f"{recovery.rollout_id} changed before reconciliation"
        )
        if recovery.disposition == "retain":
            retain_scheduler_recovery(
                recovery,
                recovery.detail or "live scheduler claim remains unknown",
            )
            continue
        outcome = "observed-inactive"
        settled_observed_at = recovery.observed_at
        if recovery.disposition == "release":
            assert recovery.expected_reason is not None
            assert recovery.expected_set_at is not None
            argv = [
                "drain",
                "--release",
                "--scheduler-host",
                recovery.host,
                "--release-legacy-only",
                "--expected-legacy-set-at",
                recovery.expected_set_at,
                "--expected-legacy-reason",
                recovery.expected_reason,
                recovery.control_host,
            ]
            try:
                proc = _run_control(argv, runner=control_runner)
            except (OSError, subprocess.SubprocessError) as exc:
                retain_scheduler_recovery(
                    recovery,
                    f"exact conditional legacy release unavailable: {exc}",
                )
                continue
            if proc.returncode != 0:
                detail = (
                    proc.stderr or proc.stdout or "(no output)"
                ).strip()
                retain_scheduler_recovery(
                    recovery,
                    "exact conditional legacy release failed: "
                    f"exit {proc.returncode}: {detail}",
                )
                continue
            # Exit zero includes the safe no-op where another actor removed
            # the exact legacy component between observation and mutation.
            # Re-observe through the same provenance-bound interface and only
            # settle the journal once absence is authoritative. Never report
            # that this process changed live state merely from an exit code.
            live_refresh_required = True
            try:
                confirmation = _observe_obsolete_legacy_scheduler_hold(
                    run,
                    host=recovery.host,
                    hold=recovery.hold,
                    control_host=recovery.control_host,
                    live_release_block=None,
                    accept_legacy_absence=True,
                    runner=control_runner,
                )
            except FleetRolloutError as exc:
                retain_scheduler_recovery(
                    recovery,
                    "post-release legacy absence could not be proven: "
                    f"{exc}",
                )
                continue
            if confirmation.disposition != "inactive":
                retain_scheduler_recovery(
                    recovery,
                    "post-release legacy absence could not be proven: "
                    + (
                        confirmation.detail
                        or "the legacy component remains observable"
                    ),
                )
                continue
            outcome = "conditional-absence-confirmed"
            settled_observed_at = confirmation.observed_at
        settled_inactive.append((recovery.rollout_id, recovery.host))
        record = dict(recovery.hold)
        record["status"] = "released"
        record.pop("cleanup_error", None)
        record["legacy_reconciliation"] = {
            "schema": "vq.fleet.legacy_scheduler_hold_reconciliation/1",
            "outcome": outcome,
            "control_host": recovery.control_host,
            "observed_at": settled_observed_at,
            "historical_report_path": run.report_source_path,
            "historical_report_digest_sha256": run.report_digest_sha256,
            "current_report_path": plan.report.get("source_path"),
            "current_report_digest_sha256": plan.report.get(
                "digest_sha256"
            ),
            "expected_legacy_reason": recovery.expected_reason,
            "expected_legacy_set_at": recovery.expected_set_at,
        }
        _validate_current_report(plan, current_report_digest_resolver)
        run.holds[recovery.host] = record
        run.legacy_retained_holds.pop(recovery.host, None)
        save_run(run)

    def require_journal_sha(
        rollout_id_value: str,
        *,
        expected_sha256: str,
    ) -> RolloutRun:
        current = load_run(rollout_id_value)
        if (
            current is None
            or _legacy_subtree_sha256(current.as_dict()) != expected_sha256
        ):
            raise FleetRolloutError(
                f"legacy rollout journal {rollout_id_value} changed during "
                "host-local recovery"
            )
        return current

    def guarded_journal_mutation(
        rollout_id_value: str,
        *,
        expected_sha256: str,
    ) -> None:
        _validate_current_report(plan, current_report_digest_resolver)
        require_journal_sha(
            rollout_id_value,
            expected_sha256=expected_sha256,
        )

    def require_failure_binding(
        failure_candidate: _LegacyFailureRecoveryCandidate,
        *,
        run: RolloutRun | None = None,
    ) -> RolloutRun:
        current = run or load_run(failure_candidate.rollout_id)
        action_records = (
            {
                action_id: current.actions.get(action_id)
                for action_id in failure_candidate.action_ids
            }
            if current is not None
            else {}
        )
        operation_ids = tuple(
            sorted(
                str(attempt["operation_id"])
                for raw_record in action_records.values()
                for attempt in _operation_attempts(raw_record)
                if attempt["operation_id"]
                in failure_candidate.operation_ids
            )
        )
        current_hold_sha256 = (
            _legacy_subtree_sha256(current.holds[failure_candidate.host])
            if current is not None
            and failure_candidate.host in current.holds
            else None
        )
        if (
            current is None
            or not fleet_release.same_report(
                current.report_source_path, failure_candidate.report_source_path
            )
            or current.report_digest_sha256
            != failure_candidate.report_digest_sha256
            or current_hold_sha256 != failure_candidate.host_hold_sha256
            or operation_ids != failure_candidate.operation_ids
            or _legacy_subtree_sha256(action_records)
            != failure_candidate.action_records_sha256
        ):
            raise FleetRolloutError(
                f"terminal failure fence for {failure_candidate.host} in "
                f"{failure_candidate.rollout_id} changed after global "
                "prevalidation"
            )
        return current

    expected_full_journal_sha: dict[str, str] = {}
    for recovery_candidate in affected_full_holds.values():
        current = load_run(recovery_candidate.rollout_id)
        raw_hold = (
            current.holds.get(recovery_candidate.host)
            if current is not None
            else None
        )
        if (
            current is None
            or not isinstance(raw_hold, Mapping)
            or _legacy_subtree_sha256(raw_hold)
            != recovery_candidate.hold_sha256
        ):
            raise FleetRolloutError(
                f"legacy full hold for {recovery_candidate.host} in "
                f"{recovery_candidate.rollout_id} changed after global "
                "prevalidation"
            )
        current_sha256 = _legacy_subtree_sha256(current.as_dict())
        prior_sha256 = expected_full_journal_sha.setdefault(
            recovery_candidate.rollout_id,
            current_sha256,
        )
        if prior_sha256 != current_sha256:
            raise FleetRolloutError(
                f"legacy rollout journal {recovery_candidate.rollout_id} "
                "changed while establishing host-local recovery candidates"
            )

    def retain_full_hold(
        retained_run: RolloutRun,
        host: str,
        reason: str,
    ) -> None:
        rollout_id_value = retained_run.rollout_id
        retained_hold = retained_run.holds.get(host)
        if (
            not isinstance(retained_hold, Mapping)
            or retained_hold.get("owned") is not True
            or retained_hold.get("status")
            not in {"active", "cleanup-failed"}
        ):
            raise FleetRolloutError(
                f"legacy full hold for {host} in {rollout_id_value} changed "
                "before retention"
            )
        exact = _journaled_exact_owner_holds(retained_run).get(host)
        control_host = str(
            (exact or {}).get("control_host") or host
        )
        retained_run.legacy_retained_holds[host] = (
            _legacy_retained_hold_receipt(
                retained_run,
                host=host,
                hold=retained_hold,
                plan=plan,
                source="hold-observation",
                control_host=control_host,
                retained_at=utcnow_iso(),
                reason=reason,
            )
        )
        retained_run.complete = False
        guarded_journal_mutation(
            rollout_id_value,
            expected_sha256=expected_full_journal_sha[rollout_id_value],
        )
        save_run(retained_run)
        expected_full_journal_sha[rollout_id_value] = (
            _legacy_subtree_sha256(retained_run.as_dict())
        )
        retained.append((rollout_id_value, host, reason))

    def same_host_action_authorizes_release(
        run: RolloutRun,
        *,
        host: str,
    ) -> bool:
        """Whether persisted exact proof authorizes this host's live release."""
        for action_id, raw_record in run.actions.items():
            if not isinstance(raw_record, Mapping):
                continue
            phase, action_host, program = _legacy_action_parts(action_id)
            if action_host != host or not _coherent_superseded_legacy_record(
                run,
                action_id,
                raw_record,
            ):
                continue
            evidence = raw_record.get("legacy_reconciliation")
            assert isinstance(evidence, Mapping)
            current = current_actions.get(action_id)
            if (
                current is not None
                and current.phase == phase
                and current.host == action_host
                and current.program == program
                and _skip_proves_live_success(current)
                and current.before.get("configured") is True
                and current.before.get("last_ok") is True
                and _legacy_action_marker_reason(admin_status, current)
                is None
                and evidence.get("current_target_sha")
                == current.target_sha
                and fleet_release.same_report(
                    evidence.get("current_report_path"), plan.report.get("source_path")
                )
                and evidence.get("current_report_digest_sha256")
                == plan.report.get("digest_sha256")
            ):
                return True
        return False

    handled_failure_hosts: set[tuple[str, str]] = set()
    deferred_failure_hosts: set[tuple[str, str]] = set()

    def settle_full_hold_candidate(
        original_run: RolloutRun,
        candidate_run: RolloutRun,
        *,
        host: str,
    ) -> bool:
        rollout_id_value = original_run.rollout_id
        failure_reason = failure_reasons.get((rollout_id_value, host))
        skip_ready = False
        if failure_reason is not None:
            skip_detail = persist_current_failure_skip(
                candidate_run,
                host=host,
            )
            skip_ready = skip_detail is None
        candidate_run.legacy_retained_holds.pop(host, None)
        candidate_run.complete = False
        guarded_journal_mutation(
            rollout_id_value,
            expected_sha256=expected_full_journal_sha[rollout_id_value],
        )
        save_run(candidate_run)
        expected_full_journal_sha[rollout_id_value] = (
            _legacy_subtree_sha256(candidate_run.as_dict())
        )
        settled_inactive.append((rollout_id_value, host))
        if failure_reason is None:
            return True
        if not skip_ready:
            deferred_failure_hosts.add((rollout_id_value, host))
            return True
        handled_failure_hosts.add((rollout_id_value, host))
        skip_sha256 = expected_full_journal_sha[rollout_id_value]
        consume_reconciled_failure_fences(
            ((rollout_id_value, host, failure_reason),),
            current_run=candidate_run,
            mutation_guard=partial(
                guarded_journal_mutation,
                rollout_id_value,
                expected_sha256=skip_sha256,
            ),
        )
        expected_full_journal_sha[rollout_id_value] = (
            _legacy_subtree_sha256(candidate_run.as_dict())
        )
        return True

    for (rollout_id_value, host), recovery_candidate in (
        affected_full_holds.items()
    ):
        original_run = require_journal_sha(
            rollout_id_value,
            expected_sha256=expected_full_journal_sha[rollout_id_value],
        )
        failure_candidate = failure_candidates.get((rollout_id_value, host))
        if failure_candidate is not None:
            require_failure_binding(
                failure_candidate,
                run=original_run,
            )
        candidate_hold = original_run.holds.get(host)
        if (
            not isinstance(candidate_hold, Mapping)
            or _legacy_subtree_sha256(candidate_hold)
            != recovery_candidate.hold_sha256
        ):
            raise FleetRolloutError(
                f"legacy full hold for {host} in {rollout_id_value} changed "
                "after global prevalidation"
            )
        candidate_run = copy.deepcopy(original_run)
        raw_hold = candidate_run.holds.get(host)
        if (
            not isinstance(raw_hold, Mapping)
            or raw_hold.get("kind") != "full"
            or raw_hold.get("owned") is not True
            or raw_hold.get("status")
            not in {"active", "cleanup-failed"}
        ):
            raise FleetRolloutError(
                f"legacy full hold for {host} in {rollout_id_value} changed "
                "before reconciliation"
            )
        _validate_current_report(plan, current_report_digest_resolver)
        try:
            hold = _recover_local_hold_identity(
                candidate_run,
                host=host,
                hold=raw_hold,
                runner=control_runner,
                save_state=False,
                force_observe=True,
            )
        except (
            FleetRolloutError,
            OSError,
            subprocess.SubprocessError,
        ) as exc:
            retain_full_hold(
                original_run,
                host,
                str(exc),
            )
            continue
        require_journal_sha(
            rollout_id_value,
            expected_sha256=expected_full_journal_sha[rollout_id_value],
        )
        if hold is None:
            settle_full_hold_candidate(
                original_run,
                candidate_run,
                host=host,
            )
            continue
        if not same_host_action_authorizes_release(candidate_run, host=host):
            retain_full_hold(
                original_run,
                host,
                "active legacy full hold has no matching authenticated "
                "same-host action supersession; hold left untouched",
            )
            continue
        exact = _journaled_exact_owner_holds(candidate_run).get(host)
        if exact is None:
            retain_full_hold(
                original_run,
                host,
                "live hold did not yield an exact conditional release "
                "identity",
            )
            continue
        synthetic = RolloutPlan(
            driver=str(exact.get("control_host") or host),
            report={},
            topology={},
            actions=[],
        )
        pre_release_hold = dict(exact)
        guarded_journal_mutation(
            rollout_id_value,
            expected_sha256=expected_full_journal_sha[rollout_id_value],
        )
        try:
            release_rollout_hold(
                synthetic,
                candidate_run,
                hold=exact,
                runner=control_runner,
                save_state=False,
            )
        except (
            FleetRolloutError,
            OSError,
            subprocess.SubprocessError,
        ) as exc:
            retain_full_hold(
                original_run,
                host,
                str(exc),
            )
            continue
        # A conditional release returns zero both when it removed the exact
        # pair and when that pair disappeared before the command. It may also
        # be a safe no-op if a competing state change kept a live hold in
        # place. Re-observe through the authoritative status contract before
        # rewriting the journal. Keep the old active identity in memory until
        # that proof so an unavailable snapshot can be durably retained with
        # a coherent receipt.
        candidate_run.holds[host] = pre_release_hold
        live_refresh_required = True
        try:
            confirmation = _recover_local_hold_identity(
                candidate_run,
                host=host,
                hold=pre_release_hold,
                runner=control_runner,
                save_state=False,
                force_observe=True,
            )
        except (
            FleetRolloutError,
            OSError,
            subprocess.SubprocessError,
        ) as exc:
            retain_full_hold(
                original_run,
                host,
                "post-release full-hold absence could not be proven: "
                f"{exc}",
            )
            continue
        require_journal_sha(
            rollout_id_value,
            expected_sha256=expected_full_journal_sha[rollout_id_value],
        )
        if confirmation is not None:
            retain_full_hold(
                original_run,
                host,
                "post-release full-hold absence could not be proven: "
                "the exact hold remains observable",
            )
            continue
        settle_full_hold_candidate(
            original_run,
            candidate_run,
            host=host,
        )

    for failure_candidate in failure_candidates.values():
        rollout_id_value = failure_candidate.rollout_id
        host = failure_candidate.host
        reason = failure_candidate.reason
        if (rollout_id_value, host) in (
            handled_failure_hosts | deferred_failure_hosts
        ):
            continue
        run = require_failure_binding(failure_candidate)
        raw_hold = run.holds.get(host)
        if (
            isinstance(raw_hold, Mapping)
            and raw_hold.get("owned") is True
            and raw_hold.get("status") in {"active", "cleanup-failed"}
        ):
            continue
        expected_sha256 = _legacy_subtree_sha256(run.as_dict())
        candidate_run = copy.deepcopy(run)
        skip_detail = persist_current_failure_skip(candidate_run, host=host)
        if skip_detail is not None:
            continue
        guarded_journal_mutation(
            rollout_id_value,
            expected_sha256=expected_sha256,
        )
        save_run(candidate_run)
        consume_reconciled_failure_fences(
            ((rollout_id_value, host, reason),),
            current_run=candidate_run,
            mutation_guard=partial(
                guarded_journal_mutation,
                rollout_id_value,
                expected_sha256=_legacy_subtree_sha256(
                    candidate_run.as_dict()
                ),
            ),
        )

    return LegacyRolloutReconciliation(
        superseded_actions=tuple(superseded),
        retained_actions=tuple(retained_actions),
        settled_inactive_holds=tuple(settled_inactive),
        released_live_holds=tuple(released_live),
        retained_holds=tuple(retained),
        live_hold_state_changed=live_changed,
        live_hold_refresh_required=live_refresh_required,
    )


def abort_unselected_pre_authorization_operations(
    plan: RolloutPlan,
    *,
    failed_hosts: frozenset[str] = frozenset(),
    abort_all: bool = False,
    control_runner: Runner = subprocess.run,
    stream: Any = None,
    lifecycle_resources: tuple[tuple[str, str], ...] = (),
) -> None:
    """Cancel current-report prepared work the fresh plan will not execute."""
    digest = str(plan.report["digest_sha256"])
    by_id = {action.id: action for action in plan.actions}
    try:
        operation_ids = fleet_operation.list_operation_ids(recover=True)
        observations = {
            operation: fleet_operation.observe_operation(operation, recover=True)
            for operation in operation_ids
        }
    except fleet_operation.OperationError as exc:
        raise FleetRolloutError(f"durable operation plan fence failed: {exc}") from exc
    touched: dict[str, RolloutRun] = {}
    for operation, observed in observations.items():
        identity = _identity_from_observation(observed)
        if identity.report_digest_sha256 != digest or observed.state not in {
            "prepared",
            "starting",
            "running-pre-authorization",
            "abandoned-pre-authorization",
        }:
            continue
        action = by_id.get(identity.action_id)
        exact = False
        if action is not None and action.decision == "update":
            desired = _operation_identity(
                identity.rollout_id,
                digest,
                action,
                attempt=identity.attempt,
                lifecycle_resources=lifecycle_resources,
                require_rollout_lock=bool(lifecycle_resources),
            )
            exact = desired.as_dict() == identity.as_dict()
        if exact and not abort_all and identity.host not in failed_hosts:
            continue
        run = _run_for_operation(identity)
        request_digest = _request_sha256(observed)
        _upsert_operation_ref(
            run,
            identity,
            observed,
            request_sha256=request_digest,
        )
        save_run(run)
        try:
            fleet_operation.abort_operation(
                operation,
                expected_request_sha256=request_digest,
            )
            observed = _stream_operation_until_terminal(operation, stream=stream)
        except fleet_operation.OperationError as exc:
            raise FleetRolloutError(
                f"could not abort unselected pre-authorization operation "
                f"{operation}: {exc}"
            ) from exc
        observations[operation] = observed
        if observed.state == "completed":
            run, _ = _harvest_operation(
                run,
                observed,
                identity,
                request_sha256=request_digest,
            )
        touched[run.rollout_id] = run
    for run in touched.values():
        _release_reconciled_holds(
            run,
            observations,
            runner=control_runner,
            retain_hosts=failed_hosts,
        )


def _durable_execute_one(
    plan: RolloutPlan,
    action: RolloutAction,
    *,
    rollout_id: str,
    report_digest_resolver: ReportDigestResolver | None,
    allow_reconciled_terminal_retry: bool,
    stream: Any = None,
    lifecycle_handoff: str | None = None,
    lifecycle_resources: tuple[tuple[str, str], ...] = (),
) -> RolloutRun:
    _validate_current_report(plan, report_digest_resolver)
    run = _load_or_create_run(plan, rollout_id)
    attempts = _operation_attempts(run.actions.get(action.id))
    next_attempt = max(
        (
            int(item.get("attempt", 0))
            for item in attempts
            if isinstance(item.get("attempt"), int)
        ),
        default=0,
    ) + 1
    desired = _operation_identity(
        rollout_id,
        str(plan.report["digest_sha256"]),
        action,
        attempt=next_attempt,
        lifecycle_resources=lifecycle_resources,
        require_rollout_lock=bool(lifecycle_resources),
    )
    observed: fleet_operation.OperationObservation | None = None
    identity = desired
    operation = ""
    request_digest = ""
    if attempts:
        item = attempts[-1]
        candidate = item["operation_id"]
        reuse_candidate = True
        try:
            candidate_observation = fleet_operation.observe_operation(candidate)
        except fleet_operation.OperationError as exc:
            raise FleetRolloutError(
                f"referenced durable operation {candidate} is invalid: {exc}"
            ) from exc
        candidate_identity = _identity_from_observation(candidate_observation)
        if (
            candidate_identity.constructor_fields()
            | {"attempt": desired.attempt}
            != desired.constructor_fields()
            or item["request_sha256"] != _request_sha256(candidate_observation)
            or item["attempt"] != candidate_identity.attempt
            or item["identity"] != candidate_identity.as_dict()
        ):
            raise FleetRolloutError(
                f"latest durable attempt for {action.id} does not bind the "
                "current exact action identity"
            )
        if candidate_observation.state == "outcome-unknown":
            raise FleetRolloutError(
                f"latest durable attempt {candidate} has outcome unknown; "
                "refusing replay"
            )
        if candidate_observation.state == "completed":
            result = candidate_observation.result or {}
            if item["harvested"] is not True:
                run, _ = _harvest_operation(
                    run,
                    candidate_observation,
                    candidate_identity,
                    request_sha256=item["request_sha256"],
                )
            if (
                result.get("executed") is True
                and result.get("returncode") != 0
            ):
                if not (
                    item["harvested"] is True
                    and item["failure_fence_consumed"] is True
                    and item["failure_retry_authorized"] is True
                    and allow_reconciled_terminal_retry
                ):
                    raise FleetOperationFailed(
                        f"{action.id} executed and failed with exit "
                        f"{result.get('returncode')}; its host-local failure "
                        "fence has not completed a fresh-plan reconciliation"
                    )
                reuse_candidate = False
            if not (
                item["harvested"] is True and allow_reconciled_terminal_retry
            ):
                raise FleetRolloutError(
                    f"latest durable attempt for {action.id} is already terminal; "
                    "collect and explicitly attest a fresh live plan before "
                    "creating another attempt"
                )
            reuse_candidate = False
        if candidate_observation.state == "abandoned-pre-authorization":
            raise FleetRolloutError(
                f"latest durable attempt for {action.id} was abandoned safely; "
                "reconcile it before creating another attempt"
            )
        if reuse_candidate:
            observed = candidate_observation
            identity = candidate_identity
            operation = candidate
            request_digest = item["request_sha256"]
    if observed is None:
        try:
            handle = fleet_operation.prepare_operation(desired)
            operation = handle.operation_id
            request_digest = handle.request_sha256
            observed = fleet_operation.observe_operation(operation)
        except fleet_operation.OperationError as exc:
            raise FleetRolloutError(
                f"could not prepare durable rollout action: {exc}"
            ) from exc
    assert observed is not None
    operation_handoff = lifecycle_handoff
    if identity.rollout_lock_path is not None:
        if lifecycle_handoff is None:
            raise FleetRolloutError(
                f"durable rollout action {action.id} requires the controller "
                "lifecycle fence"
            )
        operation_handoff = attach_active_rollout_lock(
            lifecycle_handoff,
            rollout_id=identity.rollout_id,
        )
    run.complete = False
    run.failed_hosts = {}
    _replace_action_record(
        run,
        action.id,
        {
            "decision": action.decision,
            "reason": action.reason,
            "status": "running",
            "argv": action.argv,
            "target_sha": action.target_sha,
            "target_version": action.target_version,
            "target_tag": action.target_tag,
            "operation_attempts": [
                _attempt_ref(
                    observed,
                    identity,
                    request_sha256=request_digest,
                )
            ],
        },
    )
    save_run(run)
    launched_supervisor: subprocess.Popen[bytes] | None = None
    try:
        _validate_current_report(plan, report_digest_resolver)
        if observed.state == "prepared":
            launched_supervisor = fleet_operation.launch_supervisor(
                operation,
                lifecycle_handoff=operation_handoff,
            )
        # An already authorized operation bypasses this block: its relaunched
        # supervisor resumes the immutable attempt without a second decision.
        if observed.state not in {
            "authorized-unactivated",
            "running-authorized",
        }:
            observed = fleet_operation.wait_for_ready(operation)
            _validate_current_report(plan, report_digest_resolver)
            fleet_operation.authorize_operation(
                operation,
                expected_report_digest_sha256=identity.report_digest_sha256,
            )
        observed = _stream_operation_until_terminal(
            operation,
            stream=stream,
            resume_authorized=True,
            launched_supervisor=launched_supervisor,
            lifecycle_handoff=operation_handoff,
        )
    except BaseException as exc:
        cleanup_error: FleetRolloutError | None = None
        if launched_supervisor is not None:
            owned_supervisor = launched_supervisor
            launched_supervisor = None
            try:
                _reap_durable_supervisor(
                    owned_supervisor,
                    terminate=True,
                )
            except FleetRolloutError as reap_exc:
                cleanup_error = reap_exc

        # A stale report is safe only while launch has not been authorized.
        # Reap our launcher handle first so a failing diagnostic observation
        # cannot strand it.  An already detached recorder is a separate
        # session and continues to own the durable action and lifetime lease.
        latest: fleet_operation.OperationObservation | None = None
        observation_error: BaseException | None = None
        try:
            latest = fleet_operation.observe_operation(operation)
        except BaseException as latest_exc:
            observation_error = latest_exc

        if latest is not None and latest.authorization is None:
            with suppress(fleet_operation.OperationError):
                fleet_operation.abort_operation(
                    operation,
                    expected_request_sha256=request_digest,
                )
        if observation_error is not None:
            exc.add_note(
                "durable operation cleanup observation also failed: "
                f"{observation_error}"
            )
        if cleanup_error is not None:
            exc.add_note(f"durable supervisor cleanup also failed: {cleanup_error}")
        if not isinstance(exc, Exception):
            raise
        raise FleetRolloutError(
            f"durable rollout operation {operation} did not reach a "
            f"verified terminal receipt: {exc}"
        ) from exc
    run = load_run(rollout_id) or run
    run, _ = _harvest_operation(
        run,
        observed,
        identity,
        request_sha256=request_digest,
    )
    result = observed.result or {}
    action.rc = result.get("returncode") if isinstance(result.get("returncode"), int) else None
    action.output_tail = run.actions[action.id].get("output_tail")
    if result.get("executed") is True and result.get("returncode") != 0:
        action.outcome = "failed"
        raise FleetOperationFailed(
            f"{action.id} executed and failed with exit {result.get('returncode')}; "
            "the durable attempt is not retry-safe"
        )
    if result.get("status") != "success":
        raise FleetRolloutError(
            f"{action.id} did not execute ({result.get('status')}); retry only "
            "after this terminal receipt has been reconciled"
        )
    action.outcome = "success"
    return run


def _host_has_unsettled_operation(
    run: RolloutRun,
    actions: Sequence[RolloutAction],
) -> bool:
    for action in actions:
        attempts = _operation_attempts(run.actions.get(action.id))
        for item in attempts:
            operation = item.get("operation_id")
            if not isinstance(operation, str):
                return True
            try:
                observed = fleet_operation.observe_operation(operation)
            except fleet_operation.OperationError:
                return True
            if observed.state != "completed" or item.get("harvested") is not True:
                return True
            if (
                observed.result is not None
                and observed.result.get("executed") is True
                and observed.result.get("returncode") != 0
                and item.get("failure_retry_authorized") is not True
            ):
                # A failed mutation is terminal but the host remains in a
                # deliberately conservative maintenance state until the next
                # explicit reconciliation proves what should run next.
                return True
    return False


def execute_one(
    plan: RolloutPlan,
    action: RolloutAction,
    *,
    rollout_id: str,
    runner: Runner | None = None,
    report_digest_resolver: ReportDigestResolver | None = None,
    durable_reconciled: bool = False,
    lifecycle_handoff: str | None = None,
    lifecycle_resources: tuple[tuple[str, str], ...] = (),
) -> RolloutRun:
    with _rollout_execution_lock(rollout_id):
        return _execute_one_locked(
            plan,
            action,
            rollout_id=rollout_id,
            runner=runner,
            report_digest_resolver=report_digest_resolver,
            durable_reconciled=durable_reconciled,
            lifecycle_handoff=lifecycle_handoff,
            lifecycle_resources=lifecycle_resources,
        )


def _execute_one_locked(
    plan: RolloutPlan,
    action: RolloutAction,
    *,
    rollout_id: str,
    runner: Runner | None,
    report_digest_resolver: ReportDigestResolver | None = None,
    durable_reconciled: bool = False,
    lifecycle_handoff: str | None = None,
    lifecycle_resources: tuple[tuple[str, str], ...] = (),
) -> RolloutRun:
    """Execute and journal one update action.

    The caller must pass an action from the current live plan. This is used for
    the driver self-update, after which the command re-enters through the newly
    deployed interpreter before touching any remote host.
    """
    if action.decision != "update":
        raise FleetRolloutError(
            f"{action.id} is {action.decision}, not executable"
        )
    if runner is None:
        return _durable_execute_one(
            plan,
            action,
            rollout_id=rollout_id,
            report_digest_resolver=report_digest_resolver,
            allow_reconciled_terminal_retry=durable_reconciled,
            lifecycle_handoff=lifecycle_handoff,
            lifecycle_resources=lifecycle_resources,
        )
    _validate_current_report(plan, report_digest_resolver)
    run = _load_or_create_run(plan, rollout_id)
    # A persisted completion describes the prior attempt only. Clear it in
    # the same durable transition that marks this action running so a crash or
    # failure cannot leave an actively changing rollout labelled complete.
    run.complete = False
    run.failed_hosts = {}
    _replace_action_record(run, action.id, {
        "decision": action.decision,
        "reason": action.reason,
        "status": "running",
        "argv": action.argv,
        "target_sha": action.target_sha,
        "target_version": action.target_version,
        "target_tag": action.target_tag,
    })
    save_run(run)
    # Report publication does not share this lock. Check again at the last
    # local boundary before the external update mutation so a newer accepted
    # report that appeared while journaling stops the stale action.
    _validate_current_report(plan, report_digest_resolver)
    started = time.monotonic()
    proc = execute_action(action, runner=runner)
    duration_seconds = round(time.monotonic() - started, 3)
    output = (proc.stdout or "") + (proc.stderr or "")
    action.rc = proc.returncode
    action.output_tail = "\n".join(output.splitlines()[-80:]) or None
    if proc.returncode != 0:
        action.outcome = "failed"
        _replace_action_record(run, action.id, {
            "decision": action.decision,
            "reason": action.reason,
            "status": "failed",
            "argv": action.argv,
            "target_sha": action.target_sha,
            "target_version": action.target_version,
            "target_tag": action.target_tag,
            "rc": proc.returncode,
            "duration_seconds": duration_seconds,
            "output_tail": action.output_tail,
        })
        save_run(run)
        log_target = action.host if action.phase == "helper" else action.program
        raise FleetRolloutError(
            f"{action.id} failed with exit {proc.returncode}; "
            f"read `vq admin logs {log_target} --host {action.host}` "
            "before retrying"
        )
    action.outcome = "success"
    _replace_action_record(run, action.id, {
        "decision": action.decision,
        "reason": action.reason,
        "status": "success",
        "argv": action.argv,
        "target_sha": action.target_sha,
        "target_version": action.target_version,
        "target_tag": action.target_tag,
        "rc": 0,
        "duration_seconds": duration_seconds,
    })
    save_run(run)
    return run


def execute_plan(
    plan: RolloutPlan,
    *,
    rollout_id: str,
    runner: Runner | None = None,
    control_runner: Runner | None = None,
    stop_after: int | None = None,
    scoped: bool = False,
    report_digest_resolver: ReportDigestResolver | None = None,
    durable_reconciled: bool = False,
    reconciled_failed_hosts: Mapping[str, str] | None = None,
    reconciled_failures: Sequence[tuple[str, str, str]] = (),
    retain_scheduler_holds: bool = False,
    lifecycle_handoff: str | None = None,
    lifecycle_resources: tuple[tuple[str, str], ...] = (),
) -> RolloutRun:
    with _rollout_execution_lock(rollout_id):
        return _execute_plan_locked(
            plan,
            rollout_id=rollout_id,
            runner=runner,
            control_runner=control_runner,
            stop_after=stop_after,
            scoped=scoped,
            report_digest_resolver=report_digest_resolver,
            durable_reconciled=durable_reconciled,
            reconciled_failed_hosts=reconciled_failed_hosts,
            reconciled_failures=reconciled_failures,
            retain_scheduler_holds=retain_scheduler_holds,
            lifecycle_handoff=lifecycle_handoff,
            lifecycle_resources=lifecycle_resources,
        )


def _execute_plan_locked(
    plan: RolloutPlan,
    *,
    rollout_id: str,
    runner: Runner | None,
    control_runner: Runner | None,
    stop_after: int | None,
    scoped: bool,
    report_digest_resolver: ReportDigestResolver | None,
    durable_reconciled: bool,
    reconciled_failed_hosts: Mapping[str, str] | None,
    reconciled_failures: Sequence[tuple[str, str, str]],
    retain_scheduler_holds: bool,
    lifecycle_handoff: str | None,
    lifecycle_resources: tuple[tuple[str, str], ...],
) -> RolloutRun:
    """Execute update actions serially and persist after every transition.

    ``stop_after`` is an integration-test interruption seam. Production leaves
    it ``None``.
    """
    del scoped  # completion is promoted only by fenced live finalization
    _validate_current_report(plan, report_digest_resolver)
    run = _load_or_create_run(plan, rollout_id)
    # Beginning a new attempt invalidates the previous convergence verdict.
    # Persist that before acquiring or migrating any non-expiring hold; a
    # process death during the first control mutation must not leave the
    # journal marked complete. Failures are recomputed from this attempt.
    run.complete = False
    run.failed_hosts = {}
    save_run(run)
    control = control_runner or runner or subprocess.run
    updates_by_host: dict[str, list[RolloutAction]] = {}
    actions_by_host: dict[str, list[RolloutAction]] = {}
    last_update_index: dict[str, int] = {}
    for index, action in enumerate(plan.actions):
        if action.phase != "driver":
            actions_by_host.setdefault(action.host, []).append(action)
        if action.decision == "update" and action.phase != "driver":
            updates_by_host.setdefault(action.host, []).append(action)
            last_update_index[action.host] = index
    # A previous process may have died with a claim active, or may have lost
    # the response to a successful release.  Carry exact-owner claims into
    # this invocation so its normal cleanup path reconciles them even when
    # the refreshed plan contains only skip actions.
    held = _journaled_exact_owner_holds(run)
    validated_held: set[str] = set()
    seeded_held: set[str] = set()
    completed_now = 0
    cleanup_errors: dict[str, str] = {}
    # Hosts whose lane failed this invocation. A failure is terminal for ITS
    # host and no further, so one host cannot stop the fleet from converging.
    #
    # It used to abort the whole run. That made the tool unusable on this fleet:
    # the workstation/compute-a multi-user `/opt/vq` gap (runbook §3b) failed by design
    # every release until a maintainer ran its sudo step, so `rollout-latest`
    # was guaranteed to halt on every release, leaving every host ordered behind
    # workstation unverified. (That step is now one command,
    # `sudo /opt/vq/bin/vq-multi-user-refresh`, but it is still a maintainer
    # action on a privileged path, so the lane still fails first and is still
    # terminal for its host only.) The updater hit it twice during the v0.15.75 sweep and
    # finished the remaining venv lanes by hand. Worse, the manual
    # `--all-hosts` commands already continue past a single host's failure, so
    # the automated path was the strict regression -- same fleet, same failure,
    # two behaviours.
    #
    # Per-host and not blind continue-everything: within one host a failed lane
    # still stops its siblings, because a host whose release lane just failed is
    # not a host to keep installing onto.
    failure_refs = list(reconciled_failures) or [
        (rollout_id, host, reason)
        for host, reason in (reconciled_failed_hosts or {}).items()
    ]
    failed_hosts: dict[str, str] = {
        host: reason for _failed_rollout, host, reason in failure_refs
    }
    failed_hosts.update(reconciled_failed_hosts or {})
    planned_hosts = {action.host for action in plan.actions}
    unplanned_failure_hosts = set(failed_hosts) - planned_hosts
    if unplanned_failure_hosts:
        run.failed_hosts = {
            host: failed_hosts[host] for host in sorted(unplanned_failure_hosts)
        }
        save_run(run)
        raise FleetRolloutError(
            "pending durable failure fence has no current-plan host lane for "
            f"{', '.join(sorted(unplanned_failure_hosts))}; the fence and its "
            "hold remain active, restore the host lane or reconcile it "
            "explicitly before retrying"
        )
    _validate_scheduler_hold_bindings(plan, run)
    try:
        for action_host in sorted(updates_by_host):
            host_actions = updates_by_host[action_host]
            if len(host_actions) > 1 and _hold_is_scheduler(host_actions):
                seeded_held.update(
                    _journal_scheduler_rollout_hold_group(
                        plan,
                        run,
                        action_host=action_host,
                    )
                )
        if seeded_held:
            held.update(_journaled_exact_owner_holds(run))
        # A journal written before owner-scoped leases can outlive the update
        # actions it protected. Migrate that recognized legacy hold even when
        # the refreshed plan is skip-only or has only one update left; the old
        # len(host_actions) > 1 gate would otherwise never revisit it.
        for target in sorted(_journaled_legacy_owned_hosts(run)):
            journaled = run.holds.get(target)
            if not isinstance(journaled, Mapping):
                continue
            action_host = _journaled_hold_action_host(target, journaled)
            host_actions = actions_by_host.get(action_host, [])
            if not _hold_is_scheduler(host_actions):
                raise FleetRolloutError(
                    f"legacy scheduler rollout hold for {target} has no "
                    "matching scheduler lane in the current plan; inspect "
                    "the live drain and release it explicitly before retrying"
                )
            try:
                _validate_current_report(plan, report_digest_resolver)
                held[target] = acquire_rollout_hold(
                    plan,
                    run,
                    host=target,
                    action_host=action_host,
                    control_host=_scheduler_hold_control_for(
                        plan,
                        action_host=action_host,
                        target=target,
                        journaled=journaled,
                    ),
                    actions=host_actions,
                    runner=control,
                )
                validated_held.add(target)
            except FleetRolloutError:
                # Acquisition is journaled before legacy cleanup. Recover that
                # exact owner so the finally block can retry or leave a durable
                # cleanup-failed record instead of stranding an unnamed lease.
                recovered = _journaled_exact_owner_holds(run).get(target)
                if recovered is not None:
                    held[target] = recovered
                raise
        # A prior process may have died after journaling the deterministic
        # owner and before seeing the acquire response. Re-run acquisition for
        # scheduler actions even when the refreshed plan is skip-only or has a
        # single remaining update. The status probe makes this idempotent when
        # the first RPC did commit.
        for target in sorted(
            _journaled_owner_hosts_requiring_validation(run)
        ):
            if target in validated_held or target in seeded_held:
                continue
            journaled = run.holds.get(target)
            if not isinstance(journaled, Mapping):
                continue
            action_host = _journaled_hold_action_host(target, journaled)
            host_actions = actions_by_host.get(action_host, [])
            if not _hold_is_scheduler(host_actions):
                continue
            try:
                _validate_current_report(plan, report_digest_resolver)
                held[target] = acquire_rollout_hold(
                    plan,
                    run,
                    host=target,
                    action_host=action_host,
                    control_host=_scheduler_hold_control_for(
                        plan,
                        action_host=action_host,
                        target=target,
                        journaled=journaled,
                    ),
                    actions=host_actions,
                    runner=control,
                )
                validated_held.add(target)
            except FleetRolloutError:
                recovered = _journaled_exact_owner_holds(run).get(target)
                if recovered is not None:
                    held[target] = recovered
                raise
        for index, action in enumerate(plan.actions):
            if action.decision == "update" and action.host in failed_hosts:
                previous = run.actions.get(action.id)
                if (
                    isinstance(previous, Mapping)
                    and previous.get("status") == "failed"
                ):
                    _replace_action_record(
                        run,
                        action.id,
                        {
                            **previous,
                            "reconciled_failure": failed_hosts[action.host],
                        },
                    )
                else:
                    _replace_action_record(run, action.id, {
                        "decision": action.decision,
                        "reason": (
                            f"skipped: an earlier lane on {action.host} failed "
                            f"({failed_hosts[action.host]})"
                        ),
                        "status": "not-run",
                    })
                save_run(run)
                continue
            if action.decision != "update":
                previous = run.actions.get(action.id)
                if (
                    isinstance(previous, dict)
                    and previous.get("status") == "success"
                    and _skip_proves_live_success(action)
                ):
                    _replace_action_record(run, action.id, {
                        **previous,
                        "verified_reason": action.reason,
                    })
                elif (
                    isinstance(previous, dict)
                    and previous.get("status") == "success"
                    and SCOPE_EXCLUDED_REASON in action.reason
                ):
                    # A host-scoped invocation shares this rollout's journal
                    # with the full run. Overwriting an out-of-scope host's
                    # recorded success with `not-run` would erase the only
                    # record of a deploy that did happen under this rollout id
                    # -- including its rc, duration and output tail.
                    _replace_action_record(run, action.id, {
                        **previous,
                        "out_of_scope_reason": action.reason,
                    })
                else:
                    _replace_action_record(run, action.id, {
                        "decision": action.decision,
                        "reason": action.reason,
                        "status": "not-run",
                    })
                save_run(run)
                continue
            host_actions = updates_by_host.get(action.host, [])
            if action.phase != "driver" and len(host_actions) > 1:
                _validate_current_report(plan, report_digest_resolver)
                if _hold_is_scheduler(host_actions):
                    _acquire_scheduler_rollout_hold_group(
                        plan,
                        run,
                        action_host=action.host,
                        actions=host_actions,
                        runner=control,
                        held=held,
                        validated=validated_held,
                    )
                elif action.host not in validated_held:
                    held[action.host] = acquire_rollout_hold(
                        plan,
                        run,
                        host=action.host,
                        actions=host_actions,
                        runner=control,
                    )
                    validated_held.add(action.host)
            # The newly collected live snapshot is authoritative. A prior
            # journal success is not permission to skip an action that still
            # fails the target identity/LAST OK gate.
            try:
                execute_kwargs: dict[str, Any] = {
                    "rollout_id": rollout_id,
                    "runner": runner,
                    "report_digest_resolver": report_digest_resolver,
                }
                if durable_reconciled:
                    execute_kwargs["durable_reconciled"] = True
                if lifecycle_handoff is not None:
                    execute_kwargs["lifecycle_handoff"] = lifecycle_handoff
                    execute_kwargs["lifecycle_resources"] = lifecycle_resources
                run = execute_one(plan, action, **execute_kwargs)
            except FleetRolloutError as exc:
                if runner is None and not isinstance(exc, FleetOperationFailed):
                    raise
                # Record the host as failed and move on to the next host. The
                # journal already carries this action's rc and output tail from
                # execute_one, and the end-of-run verification rebuilds the plan
                # from live state, so the degraded set is reported rather than
                # inferred from where the run stopped.
                failed_hosts[action.host] = str(exc)
                run = _load_or_create_run(plan, rollout_id)
                if runner is None:
                    if isinstance(exc, FleetOperationFailed):
                        failure = (rollout_id, action.host, str(exc))
                        if failure not in failure_refs:
                            failure_refs.append(failure)
                    # The verified child result is terminal, but a failed
                    # mutation keeps this host's exact outer drain until the
                    # next explicit reconciliation. Other hosts remain
                    # independent and may continue under their own holds.
                    continue
                released = _release_rollout_hold_group(
                    plan,
                    run,
                    action_host=action.host,
                    held=held,
                    runner=control,
                )
                for target in _scheduler_hold_targets_for(
                    plan, action.host
                ):
                    cleanup_errors.pop(target[0], None)
                cleanup_errors.update(released)
                continue
            completed_now += 1
            if stop_after is not None and completed_now >= stop_after:
                raise InterruptedError("simulated rollout interruption")
            if (
                last_update_index.get(action.host) == index
                and not (
                    retain_scheduler_holds
                    and _hold_is_scheduler(host_actions)
                )
            ):
                release_errors = _release_rollout_hold_group(
                    plan,
                    run,
                    action_host=action.host,
                    held=held,
                    runner=control,
                )
                for target, _control_host in _scheduler_hold_targets_for(
                    plan, action.host
                ):
                    cleanup_errors.pop(target, None)
                cleanup_errors.update(release_errors)
                if release_errors:
                    raise FleetRolloutError(
                        "; ".join(release_errors.values())
                    )
        consumable = tuple(
            failure
            for failure in failure_refs
            if failure[1] in planned_hosts
        )
        if consumable:
            run = (
                consume_reconciled_failure_fences(
                    consumable,
                    current_run=run,
                )
                or run
            )
    finally:
        # SIGINT, a failed lane, and the integration interruption seam are
        # terminal for this invocation. Never strand a rollout-owned drain;
        # the journal and live-state planner make the next invocation resume.
        # Acquisition journals its deterministic owner before the remote
        # mutation. If that call raises before assignment to ``held`` (for
        # example Ctrl-C after a committed RPC), merge the newest durable
        # owner records here so this same invocation still exact-releases its
        # uncertain claim.
        durable_run = load_run(rollout_id)
        if durable_run is not None:
            run = durable_run
            held.update(_journaled_exact_owner_holds(durable_run))
        completed_without_exception = sys.exc_info()[0] is None
        for target in reversed(list(held)):
            try:
                # A failed release may still have completed an earlier
                # migration step. Retry from the newest durable record so a
                # response loss does not repeat already-journaled work.
                recovered = _journaled_exact_owner_holds(run).get(target)
                if recovered is not None:
                    held[target] = recovered
                action_host = _journaled_hold_action_host(
                    target, held[target]
                )
                if (
                    retain_scheduler_holds
                    and completed_without_exception
                    and held[target].get("kind") == "scheduler-target"
                ):
                    # The live update command is only one piece of the proof.
                    # Keep exact scheduler protection until the fenced caller
                    # has rebuilt the plan from fresh program/doctor evidence.
                    # Direct execute_plan callers retain the historical
                    # cleanup contract because the option defaults false.
                    continue
                if runner is None and (
                    action_host in failed_hosts
                    or _host_has_unsettled_operation(
                        run,
                        actions_by_host.get(action_host, ()),
                    )
                ):
                    # A detached child still owns, or may have owned, the
                    # mutation. Keep the exact outer drain until a later
                    # invocation reconciles a trustworthy terminal receipt.
                    continue
                release_rollout_hold(
                    plan,
                    run,
                    hold=held[target],
                    runner=control,
                )
                held.pop(target)
                cleanup_errors.pop(target, None)
            except FleetRolloutError as exc:
                cleanup_errors[target] = str(exc)
        if cleanup_errors and sys.exc_info()[0] is None:
            raise FleetRolloutError("; ".join(cleanup_errors.values()))
    # Command success is not fleet convergence. The fenced caller must still
    # collect fresh lane, doctor, and console evidence; only ``finalize_run``
    # may promote this journal after that live verdict. Keeping this false also
    # makes a death during verification fail closed.
    run.complete = False
    run.failed_hosts = dict(failed_hosts)
    save_run(run)
    return run


def reconcile_scheduler_parity_holds(
    initial: RolloutPlan,
    verification: RolloutPlan,
    *,
    doctor: Mapping[str, Any],
    run: RolloutRun,
    runner: Runner,
    selection: HostSelection | None = None,
    report_digest_resolver: ReportDigestResolver | None = None,
) -> RolloutRun:
    """Retain affected scheduler lanes until final parity is live-proven.

    The caller owns the fleet rollout fence across execution, the fresh
    snapshot, and this reconciliation.  An action's successful subprocess is
    not proof that its installed helper/runtime matches the accepted report.
    Every in-scope scheduler action host is therefore released only when all
    of its final actions are trustworthy converged skips and every exact
    dispatch target in the group has a healthy doctor result.  A final drift,
    missing action, or unhealthy target acquires/reconfirms the whole exact
    alias group instead, including for a one-action rollout that did not need
    an outer hold during deployment.
    """
    current = load_run(run.rollout_id) or run
    if current.report_digest_sha256 != run.report_digest_sha256:
        raise FleetRolloutError(
            "rollout journal changed to a different accepted report "
            "during scheduler parity reconciliation"
        )
    initial_digest = str(initial.report.get("digest_sha256", ""))
    verification_digest = str(verification.report.get("digest_sha256", ""))
    if (
        initial_digest != current.report_digest_sha256
        or verification_digest != current.report_digest_sha256
    ):
        raise FleetRolloutError(
            "scheduler parity plans do not bind the current rollout report"
        )
    _validate_current_report(verification, report_digest_resolver)
    initial_by_host: dict[str, list[RolloutAction]] = {}
    for action in initial.actions:
        if action.phase not in {"helper", "scheduler-runtime"}:
            continue
        if (
            selection is not None
            and selection.scoped
            and not selection.includes(action.host)
        ):
            continue
        if SCOPE_EXCLUDED_REASON in action.reason:
            continue
        initial_by_host.setdefault(action.host, []).append(action)
    if not initial_by_host:
        return current

    safety_targets = dict(verification._scheduler_hold_targets)
    bindings_stable: dict[str, bool] = {}
    for action_host in initial_by_host:
        initial_targets = _scheduler_hold_targets_for(initial, action_host)
        final_targets = _scheduler_hold_targets_for(verification, action_host)
        initial_controls = dict(initial_targets)
        final_controls = dict(final_targets)
        conflicting = sorted(
            target
            for target in initial_controls.keys() & final_controls.keys()
            if initial_controls[target] != final_controls[target]
        )
        if conflicting:
            raise FleetRolloutError(
                "scheduler parity control binding changed for "
                f"{', '.join(conflicting)}; retaining any existing exact "
                "rollout hold"
            )
        combined = list(initial_targets)
        combined.extend(
            item for item in final_targets if item[0] not in initial_controls
        )
        safety_targets[action_host] = tuple(combined)
        bindings_stable[action_host] = initial_controls == final_controls
    safety_plan = replace(
        verification,
        _scheduler_hold_targets=safety_targets,
    )

    _validate_scheduler_hold_bindings(safety_plan, current)

    verification_by_id = {action.id: action for action in verification.actions}
    verification_by_host: dict[str, list[RolloutAction]] = {}
    for action in verification.actions:
        if action.phase in {"helper", "scheduler-runtime"}:
            verification_by_host.setdefault(action.host, []).append(action)

    held = _journaled_exact_owner_holds(current)
    validated: set[str] = set()
    errors: dict[str, str] = {}
    for action_host in sorted(initial_by_host):
        _validate_current_report(verification, report_digest_resolver)
        expected_actions = initial_by_host[action_host]
        host_actions = verification_by_host.get(action_host, [])
        final_actions = [
            verification_by_id.get(action.id) for action in expected_actions
        ]
        targets = _scheduler_hold_targets_for(safety_plan, action_host)
        actions_converged = (
            bindings_stable[action_host]
            and {action.id for action in expected_actions}
            == {action.id for action in host_actions}
            and all(
                _final_action_proves_scheduler_parity(expected, final)
                for expected, final in zip(
                    expected_actions, final_actions, strict=True
                )
            )
        )
        targets_healthy = all(
            _doctor_host(doctor, target).get("ok") is True
            for target, _control in targets
        )
        if actions_converged and targets_healthy:
            errors.update(
                _release_rollout_hold_group(
                    safety_plan,
                    current,
                    action_host=action_host,
                    held=held,
                    runner=runner,
                )
            )
            continue

        _acquire_scheduler_rollout_hold_group(
            safety_plan,
            current,
            action_host=action_host,
            actions=host_actions or expected_actions,
            runner=runner,
            held=held,
            validated=validated,
        )

    if errors:
        raise FleetRolloutError("; ".join(errors.values()))
    return load_run(run.rollout_id) or current


def reenter_after_driver(
    rollout_id: str,
    *,
    python: str | None = None,
    reentry_handoff: str | None = None,
    pass_fds: tuple[int, ...] = (),
    as_json: bool,
    selection: HostSelection | None = None,
    reconcile_legacy: bool = False,
    runner: Runner = subprocess.run,
) -> int:
    """Resume orchestration in a fresh interpreter after updating driver vq.

    Host selection must survive the re-entry. The driver's own vq lane is exempt
    from narrowing (:func:`select_hosts`), so a scoped run can and does reach
    this branch — and a re-entry that dropped ``--only`` would silently widen a
    single-host recovery into a full-fleet rollout.
    """
    executable = python or sys.executable
    argv = [
        executable,
        "-I",
        "-m",
        "vq",
        "admin",
        "rollout-latest",
        "--resume",
        rollout_id,
    ]
    for host in (selection or HostSelection()).only:
        argv += ["--only", host]
    for host in (selection or HostSelection()).skip:
        argv += ["--skip", host]
    if reconcile_legacy:
        argv.append("--reconcile-legacy")
    if as_json:
        argv.append("--json")
    environment = dict(os.environ)
    for name in (
        "PYTHONHOME",
        "PYTHONPATH",
        "PYTHONSTARTUP",
        ENV_ROLLOUT_REENTRY_HANDOFF,
    ):
        environment.pop(name, None)
    if reentry_handoff is not None:
        if os.name != "posix" or not pass_fds:
            raise FleetRolloutError(
                "fresh driver re-entry requires inherited POSIX lock descriptors"
            )
        environment[ENV_ROLLOUT_REENTRY_HANDOFF] = reentry_handoff
    elif pass_fds:
        raise FleetRolloutError(
            "fresh driver re-entry descriptors lack their authenticated payload"
        )
    kwargs: dict[str, Any] = {
        "text": True,
        "check": False,
        "env": environment,
    }
    if pass_fds:
        kwargs["pass_fds"] = pass_fds
        kwargs["close_fds"] = True
    try:
        return runner(argv, **kwargs).returncode
    except OSError as exc:
        raise FleetRolloutError(
            f"could not start the fresh configured driver interpreter: {exc}"
        ) from exc


def doctor_failures(
    doctor: Mapping[str, Any],
    topology: Mapping[str, Mapping[str, Any]],
) -> dict[str, list[str]]:
    """Return failed doctor checks for every in-scope fleet identity."""
    failures: dict[str, list[str]] = {}
    for host, resolved in topology.items():
        if resolved.get("role") in {"excluded", "unresolved"}:
            continue
        payload = _doctor_host(doctor, host)
        if payload.get("ok") is True:
            continue
        checks = _doctor_checks(doctor, host)
        failed = sorted(
            name for name, check in checks.items() if check.get("ok") is False
        )
        failures[host] = failed or ["doctor result missing or unhealthy"]
    return failures


#: A console probe is one SSH round trip per host. Short, because a host
#: that cannot answer in this long is reported as unknown rather than
#: holding up the verdict.
CONSOLE_PROBE_TIMEOUT_SECONDS = 45.0


@dataclass(frozen=True)
class ConsoleState:
    """One host's answer to "is a web console installed here, and current?".

    Three outcomes, and keeping them distinct is the entire point:

    * **not applicable** -- no console installed. The correct, expected
      state for every host that is not the coordinator.
    * **ok** -- installed, running, and installed by the vq that is now
      on the host.
    * **degraded** -- installed but stale or stopped. This is the state
      that went unnoticed for two weeks on the reference fleet while
      ``--verify-only`` reported the fleet ``converged``.

    A fourth, ``unknown``, exists for hosts whose vq predates
    ``vq web status``. It must never degrade a host: rolling this check
    out would otherwise mark the entire fleet degraded until every host
    had been upgraded, which is precisely backwards.
    """

    host: str
    installed: bool
    ok: bool
    unknown: bool
    reason: str | None = None

    @property
    def degrades(self) -> bool:
        return self.installed and not self.ok and not self.unknown


def probe_console_state(cfg: Any, host: str) -> ConsoleState:
    """Ask one host about its console. Never raises.

    Reads ``vq web status --json`` on the host. That verb exits 0 whether
    or not a console is installed and whether or not it has drifted, so
    the payload is the verdict and the exit status carries nothing --
    a non-zero exit means the verb itself could not run.
    """
    from vq import transport  # noqa: PLC0415 — avoid an import cycle

    try:
        host_cfg = cfg.host(host)
    except Exception as exc:  # pragma: no cover — defensive
        return ConsoleState(host, False, False, True, f"config error: {exc}")

    try:
        proc = transport.run_remote_vq(
            host_cfg,
            "web",
            "status",
            "--json",
            check=False,
            timeout=CONSOLE_PROBE_TIMEOUT_SECONDS,
        )
    except Exception as exc:
        return ConsoleState(host, False, False, True, f"probe failed: {exc}")

    if proc.returncode != 0:
        # Overwhelmingly: a vq too old to have the verb. Unknown, not bad.
        detail = (proc.stderr or proc.stdout or "").strip().splitlines()
        return ConsoleState(
            host, False, False, True,
            f"vq web status unavailable: {detail[0] if detail else 'exit ' + str(proc.returncode)}",
        )

    try:
        payload = json.loads(proc.stdout or "{}")
    except json.JSONDecodeError:
        return ConsoleState(host, False, False, True, "vq web status returned invalid JSON")
    if not isinstance(payload, dict):
        return ConsoleState(host, False, False, True, "vq web status returned no object")

    if not payload.get("installed"):
        return ConsoleState(host, False, True, False, None)

    reasons: list[str] = []
    if payload.get("drifted"):
        reasons.append(
            f"console installed by vq {payload.get('installed_by_version')}, "
            f"host now runs {payload.get('running_version')}"
        )
    if payload.get("active") is False:
        reasons.append("console service is not running")

    if reasons:
        return ConsoleState(host, True, False, False, "; ".join(reasons))
    return ConsoleState(host, True, True, False, None)


def collect_console_states(
    cfg: Any, hosts: Sequence[str], *, max_workers: int = 8
) -> dict[str, ConsoleState]:
    """Probe every host's console concurrently. Never raises."""
    from concurrent.futures import ThreadPoolExecutor  # noqa: PLC0415

    names = list(hosts)
    if not names:
        return {}
    workers = min(max_workers, max(1, len(names)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        states = pool.map(lambda h: probe_console_state(cfg, h), names)
    return {state.host: state for state in states}


def console_failures(
    consoles: Mapping[str, ConsoleState],
    topology: Mapping[str, Mapping[str, Any]],
) -> dict[str, list[str]]:
    """Hosts whose console is installed but stale or stopped.

    Closes the hole that let a console serve 1081-commit-stale code for
    two weeks while ``vq admin rollout-latest --verify-only`` reported
    the fleet ``converged``: nothing in the rollout knew the console
    existed. Hosts with no console, and hosts too old to answer, are
    absent from the result and so do not degrade the fleet.
    """
    failures: dict[str, list[str]] = {}
    for host, resolved in topology.items():
        if resolved.get("role") in {"excluded", "unresolved"}:
            continue
        state = consoles.get(host)
        if state is None or not state.degrades:
            continue
        failures[host] = [state.reason or "console degraded"]
    return failures


def _preserved_external_holds(run: RolloutRun | None) -> list[dict[str, Any]]:
    """Project journaled claims the rollout deliberately did not release.

    This is preservation evidence, not a fresh liveness probe. An independent
    owner may release its claim after the rollout observed it, and a timed
    legacy full drain may expire. The final text therefore says ``PRESERVED``
    rather than asserting a later observation the journal cannot prove.
    """
    if run is None:
        return []
    projected: list[dict[str, Any]] = []
    seen: set[str] = set()
    for host, raw in sorted(run.holds.items()):
        if not isinstance(raw, Mapping):
            continue
        candidates = raw.get("external_holds")
        external = candidates if isinstance(candidates, list) else []
        if not external and raw.get("owned") is False:
            external = [
                {
                    "host": host,
                    "kind": raw.get("kind"),
                    "lease_id": None,
                    "owner": None,
                    # Old journals used ``reason`` for the rollout's own
                    # deterministic token even when the drain was borrowed.
                    # Never relabel that value as an external operator reason.
                    "reason": raw.get("external_reason"),
                }
            ]
        for item in external:
            if not isinstance(item, Mapping):
                continue
            record = {
                "host": str(item.get("host") or host),
                "kind": str(item.get("kind") or raw.get("kind") or "drain"),
                "lease_id": item.get("lease_id"),
                "owner": item.get("owner"),
                "reason": item.get("reason"),
                "status": "preserved",
            }
            identity = json.dumps(record, sort_keys=True, default=str)
            if identity in seen:
                continue
            seen.add(identity)
            projected.append(record)
    return projected


def _retained_rollout_holds(run: RolloutRun | None) -> list[dict[str, Any]]:
    """Project owned claims whose journal does not record exact release.

    ``active`` and ``cleanup-failed`` are persisted journal states, not a
    fresh drain probe.  A response-lost release may already have committed,
    while an interrupted acquire may still need reconciliation.  The result
    therefore says ``retained`` and exposes the exact recorded state without
    asserting that the hold is currently live.
    """
    if run is None:
        return []
    projected: list[dict[str, Any]] = []
    for host, raw in sorted(run.holds.items()):
        if not isinstance(raw, Mapping):
            continue
        if raw.get("owned") is not True:
            continue
        status = raw.get("status")
        if not isinstance(status, str) or status not in {
            "active",
            "cleanup-failed",
        }:
            continue
        projected.append(
            {
                "host": str(raw.get("host") or host),
                "kind": str(raw.get("kind") or "drain"),
                "reason": raw.get("reason"),
                "status": status,
            }
        )
    return projected


def result_payload(
    *,
    initial: RolloutPlan | None,
    verification: RolloutPlan,
    doctor: Mapping[str, Any],
    run: RolloutRun | None,
    selection: HostSelection | None = None,
    retained_legacy_holds: Sequence[tuple[str, str, str]] = (),
) -> dict[str, Any]:
    """Build the stable machine report for dry runs and completed attempts.

    ``initial=None`` means "nothing was executed": every lane reports
    ``changed: false``. Passing the verification plan as its own ``initial``
    would instead report every pending lane as changed, which is exactly
    backwards for a read-only verdict.
    """
    initial_by_id = (
        {action.id: action for action in initial.actions}
        if initial is not None
        else {}
    )
    journal_actions = run.actions if run is not None else {}
    lanes: list[dict[str, Any]] = []
    for action in verification.actions:
        before = action.before
        original = initial_by_id.get(action.id)
        reported_target = (
            original
            if original is not None and original.decision == "update"
            else action
        )
        current_sha = before.get("current_sha")
        version = before.get("current_version") or before.get("current_tag")
        if (
            current_sha == reported_target.target_sha
            and reported_target.target_version is not None
        ):
            version = reported_target.target_version
        journal_entry = journal_actions.get(action.id)
        duration = (
            journal_entry.get("duration_seconds")
            if isinstance(journal_entry, dict)
            else None
        )
        lanes.append(
            {
                "host": action.host,
                "module": action.program,
                "phase": action.phase,
                "version": version,
                "sha": current_sha,
                # For completed attempts, report the identity the initial
                # action actually targeted. Verification may legitimately
                # reduce a repaired descendant to a healthy-ahead skip whose
                # comparison floor is the older accepted report pin.
                "target_version": reported_target.target_version,
                "target_tag": reported_target.target_tag,
                "target_sha": reported_target.target_sha,
                "decision": action.decision,
                "reason": action.reason,
                "changed": bool(original and original.decision == "update"),
                "configured": before.get("configured"),
                "required": before.get("required", False),
                "last_ok": before.get("last_ok"),
                "acknowledged": before.get("acknowledged"),
                # Per-action evidence: dependency-cache decision, ccache hit
                # rate, phase durations, and the native-rebuild flag come
                # from the lane's canonical deploy record; the wall-clock
                # duration comes from this rollout's journal.
                "metrics": before.get("metrics"),
                "duration_seconds": duration,
            }
        )
    failed_doctor = doctor_failures(doctor, verification.topology)
    retained_owned = _retained_rollout_holds(run)
    if verification.has_blocks or failed_doctor:
        status = "blocked"
    elif verification.has_deferred or retained_legacy_holds or retained_owned:
        status = "deferred"
    elif verification.updates:
        status = "incomplete"
    else:
        status = "complete"
    # `status` stays fleet-wide even for a scoped run: hiding another host's
    # doctor failure because this invocation was not asked to touch it would be
    # the same class of lie the whole planner exists to prevent. The scoped
    # verdict lives beside it so a `--only workstation` recovery can still be judged
    # on workstation, which is what the operator asked about.
    scope = selection or HostSelection()
    scoped_view = restrict_plan(
        verification,
        scope,
        keep_phases=SCOPE_EXEMPT_PHASES,
    )
    scoped_degraded = degraded_hosts(scoped_view, doctor)
    # Parity reconciliation retains the union when bindings change. A removed
    # final alias must not disappear from the hold verdict either.
    scope_hosts = _diagnostic_scope_hosts(
        (verification, initial) if initial is not None else (verification,),
        scope,
        SCOPE_EXEMPT_PHASES,
        extra_hosts=[
            *(host for _rollout_id, host, _reason in retained_legacy_holds),
            *(hold["host"] for hold in retained_owned),
        ],
    )
    for rollout_id, host, reason in retained_legacy_holds:
        if scope.scoped and host not in scope_hosts:
            continue
        detail = f"retained legacy rollout hold {rollout_id}: {reason}"
        if detail not in scoped_degraded.setdefault(host, []):
            scoped_degraded[host].append(detail)
    for hold in retained_owned:
        host = hold["host"]
        if scope.scoped and host not in scope_hosts:
            continue
        scoped_degraded.setdefault(host, []).append(
            f"retained rollout hold: {hold['kind']} "
            f"(journal status {hold['status']}; exact release unconfirmed)"
        )
    selection_block = {
        **scope.as_dict(),
        "verdict": "degraded" if scoped_degraded else "converged",
        "degraded_hosts": scoped_degraded,
    }
    return {
        "schema": "vq.fleet.rollout_result/2",
        "status": status,
        "driver": verification.driver,
        "report": verification.report,
        "topology": verification.topology,
        "topology_errors": verification.topology_errors,
        "doctor_failures": failed_doctor,
        # Hosts whose lane failed this invocation. A failure is terminal for its
        # own host only, so the operator needs the degraded set named rather
        # than inferring it from where the run happened to stop.
        "failed_hosts": dict(run.failed_hosts) if run is not None else {},
        # Which hosts this invocation was allowed to change, and whether those
        # hosts converged. A reader that finds `incomplete` above needs to know
        # whether the remaining lanes were left alone on purpose.
        "selection": selection_block,
        "coverage": _coverage_payload(verification),
        "preserved_external_holds": _preserved_external_holds(run),
        "retained_rollout_holds": retained_owned,
        "retained_legacy_holds": [
            {"rollout_id": rollout_id, "host": host, "reason": reason}
            for rollout_id, host, reason in retained_legacy_holds
        ],
        "retained_legacy_fences": [
            dict(fence) for fence in verification.retained_legacy_fences
        ],
        "lanes": lanes,
        "provenance_lanes": [
            asdict(lane) for lane in verification.provenance_lanes
        ],
        "journal": run.as_dict() if run is not None else None,
        "summary": verification.as_dict()["summary"],
    }


def degraded_hosts(
    plan: RolloutPlan,
    doctor: Mapping[str, Any],
    consoles: Mapping[str, ConsoleState] | None = None,
) -> dict[str, list[str]]:
    """Every in-scope host that is not standing exactly at the accepted report.

    One host, one entry, every reason it is not converged: lanes the planner
    would still act on, plus its failed doctor checks, plus any topology error
    naming it. A host absent from this mapping is proven converged by the same
    planner that would otherwise have updated it.
    """
    degraded: dict[str, list[str]] = {}
    for action in plan.actions:
        if action.decision == "skip":
            continue
        degraded.setdefault(action.host, []).append(
            f"{action.id}: {action.decision} ({action.reason})"
        )
    for lane in plan.provenance_lanes:
        if lane.decision == "skip":
            continue
        degraded.setdefault(lane.host, []).append(
            f"{lane.id}: {lane.decision} ({lane.reason})"
        )
    for host, checks in doctor_failures(doctor, plan.topology).items():
        degraded.setdefault(host, []).append(
            "doctor: " + ", ".join(checks)
        )
    # A stale web console is a real convergence failure: it serves pages
    # about the fleet from code the fleet has moved past. Absent probes
    # (older vq, no console) contribute nothing -- see console_failures.
    for host, reasons in console_failures(consoles or {}, plan.topology).items():
        degraded.setdefault(host, []).append("console: " + ", ".join(reasons))
    for error in plan.topology_errors:
        host, _, detail = error.partition(": ")
        degraded.setdefault(host, []).append(f"topology: {detail or error}")
    for fence in plan.retained_legacy_fences:
        host = str(fence.get("host") or "unknown")
        rollout_id = str(fence.get("rollout_id") or "unknown")
        reason = str(fence.get("reason") or "identity unknown")
        detail = f"retained legacy rollout {rollout_id}: {reason}"
        if detail not in degraded.setdefault(host, []):
            degraded[host].append(detail)
    return {host: reasons for host, reasons in sorted(degraded.items())}


def verify_payload(
    *,
    plan: RolloutPlan,
    doctor: Mapping[str, Any],
    rollout_id: str,
    selection: HostSelection | None = None,
    consoles: Mapping[str, ConsoleState] | None = None,
) -> dict[str, Any]:
    """One machine-checkable convergence verdict for the accepted report.

    Judging convergence used to be four commands and an operator's judgement
    call: ``admin status --all --json``, ``programs --all``, ``doctor --all``,
    then a second zero-action dry run to confirm the planner agreed. All four
    read the same live state this function reads, and the planner is already the
    thing that decides whether a lane needs work — so the verdict is that
    planner's own answer, with nothing left to interpret.

    ``verdict`` is exactly ``converged`` or ``degraded``. ``status`` carries the
    finer-grained ``complete``/``incomplete``/``deferred``/``blocked`` word so a
    caller that wants the distinction still has it.
    """
    result = result_payload(
        initial=None,
        verification=plan,
        doctor=doctor,
        run=None,
        selection=selection,
    )
    degraded = degraded_hosts(plan, doctor, consoles)
    return {
        # /3: adds read-only provenance lanes and folds them into the verdict.
        "schema": "vq.fleet.rollout_verify/3",
        "verdict": "converged" if not degraded else "degraded",
        "status": result["status"],
        "rollout_id": rollout_id,
        "driver": result["driver"],
        "report": result["report"],
        "selection": result["selection"],
        "coverage": result["coverage"],
        "degraded_hosts": degraded,
        "doctor_failures": result["doctor_failures"],
        "console_failures": console_failures(consoles or {}, plan.topology),
        "topology_errors": result["topology_errors"],
        "retained_legacy_fences": result["retained_legacy_fences"],
        "summary": result["summary"],
        "lanes": result["lanes"],
        "provenance_lanes": result["provenance_lanes"],
    }


def _single_line(value: Any) -> str:
    """Render persisted/operator text without allowing multiline output."""
    neutral = "".join(
        " " if unicodedata.category(char).startswith("C") else char
        for char in str(value)
    )
    return " ".join(neutral.split())


def _render_coverage_lines(raw: Any) -> list[str]:
    if not isinstance(raw, Mapping):
        return []
    lines = [
        "coverage: managed lanes, vq user lanes, plus read-only provenance; "
        "whole-fleet convergence not asserted"
    ]
    managed = raw.get("managed_lanes")
    vq_user = raw.get("vq_user_lanes")
    provenance = raw.get("provenance_lanes")
    if all(isinstance(item, Mapping) for item in (managed, vq_user, provenance)):
        lines.append(
            "coverage counts: "
            f"managed={managed.get('converged', 0)}/{managed.get('total', 0)}; "
            f"vq-user={vq_user.get('converged', 0)}/{vq_user.get('total', 0)} "
            f"(deferred={vq_user.get('deferred', 0)}, "
            f"blocked={vq_user.get('blocked', 0)}); "
            f"provenance={provenance.get('converged', 0)}/"
            f"{provenance.get('total', 0)} "
            f"(not-applicable={provenance.get('not_applicable', 0)}, "
            f"deferred={provenance.get('deferred', 0)}, "
            f"blocked={provenance.get('blocked', 0)})"
        )
    exclusions = raw.get("exclusions")
    if not isinstance(exclusions, list):
        return lines
    for item in exclusions:
        if not isinstance(item, Mapping):
            continue
        label = item.get("host") or item.get("component") or "unknown"
        component = item.get("component")
        component_suffix = (
            f" component={_single_line(component)}"
            if item.get("host") is not None and component is not None
            else ""
        )
        lines.append(
            f"EXCLUDED {_single_line(label)}{component_suffix}: "
            f"{_single_line(item.get('reason') or 'no reason recorded')}"
        )
    return lines


def render_verify_text(payload: Mapping[str, Any]) -> str:
    """Human-readable one-verdict convergence report."""
    report = payload["report"]
    degraded = payload.get("degraded_hosts") or {}
    verdict = str(payload["verdict"])
    headline = (
        verdict
        if verdict == "converged"
        else f"{verdict}({', '.join(sorted(degraded))})"
    )
    lines = [
        f"rollout-latest verify: modeled lanes {headline}",
        f"report: {report['release_tag']} "
        f"(report {str(report['digest_sha256'])[:12]})",
        f"driver: {payload['driver']}",
    ]
    lines.extend(_render_coverage_lines(payload.get("coverage")))
    selection = payload.get("selection")
    if isinstance(selection, Mapping) and selection.get("scoped"):
        lines.append(
            "scope:  "
            + _selection_phrase(
                HostSelection(
                    only=tuple(selection.get("only") or ()),
                    skip=tuple(selection.get("skip") or ()),
                )
            )
        )
    for host, reasons in degraded.items():
        for reason in reasons:
            lines.append(f"DEGRADED {host:16} {reason}")
    return "\n".join(lines)


def render_plan_text(plan: RolloutPlan, *, title: str) -> str:
    """Human-readable dry-run/final plan with exact identities."""
    lines = [
        f"{title}: {plan.report['release_tag']} "
        f"(report {str(plan.report['digest_sha256'])[:12]})",
        f"driver: {plan.driver}",
    ]
    lines.extend(_render_coverage_lines(_coverage_payload(plan)))
    for error in plan.topology_errors:
        lines.append(f"BLOCK topology {error}")
    for fence in plan.retained_legacy_fences:
        decision = (
            "BLOCK" if fence.get("host") == plan.driver else "DEFER"
        )
        lines.append(
            f"{decision:6} {_single_line(fence.get('host') or 'unknown'):16} "
            "legacy-fence     -            -  "
            f"{_single_line(fence.get('rollout_id') or 'unknown')}: "
            f"{_single_line(fence.get('reason') or 'identity unknown')}"
        )
    for action in plan.actions:
        version = action.target_tag or action.target_version or "-"
        lines.append(
            f"{action.decision.upper():6} {action.host:16} "
            f"{action.program:16} {version:12} "
            f"{action.target_sha[:12]}  {action.reason}"
        )
    for lane in plan.provenance_lanes:
        current = lane.current_sha or "-"
        lines.append(
            f"{lane.decision.upper():6} {lane.host:16} "
            f"{lane.component:16} provenance   "
            f"{current[:12]}  {_single_line(lane.reason)}"
        )
    return "\n".join(lines)


def render_result_text(payload: Mapping[str, Any]) -> str:
    """Human-readable final per-host/module deployment report."""
    report = payload["report"]
    lines = [
        f"rollout-latest modeled lanes {payload['status']}: "
        f"{report['release_tag']} "
        f"(report {str(report['digest_sha256'])[:12]})",
        f"driver: {payload['driver']}",
    ]
    lines.extend(_render_coverage_lines(payload.get("coverage")))
    legacy = payload.get("legacy_reconciliation")
    if isinstance(legacy, Mapping):
        superseded = legacy.get("superseded_actions")
        if isinstance(superseded, list):
            for item in superseded:
                if not isinstance(item, Mapping):
                    continue
                lines.append(
                    "LEGACY ACTION SUPERSEDED "
                    f"{_single_line(item.get('rollout_id') or 'unknown')} "
                    f"{_single_line(item.get('action_id') or 'unknown')} "
                    "observed_outcome=unknown"
                )
        settled = legacy.get("settled_inactive_holds")
        if isinstance(settled, list):
            for item in settled:
                if not isinstance(item, Mapping):
                    continue
                lines.append(
                    "LEGACY HOLD SETTLED INACTIVE "
                    f"{_single_line(item.get('rollout_id') or 'unknown')} "
                    f"{_single_line(item.get('host') or 'unknown')} "
                    "(journal-only; no live release issued)"
                )
        released_live = legacy.get("released_live_holds")
        if isinstance(released_live, list):
            for item in released_live:
                if not isinstance(item, Mapping):
                    continue
                lines.append(
                    "LEGACY LIVE HOLD RELEASED "
                    f"{_single_line(item.get('rollout_id') or 'unknown')} "
                    f"{_single_line(item.get('host') or 'unknown')} "
                    "(exact conditional owner identity)"
                )
        legacy_retained = legacy.get("retained_holds")
        if isinstance(legacy_retained, list):
            for item in legacy_retained:
                if not isinstance(item, Mapping):
                    continue
                lines.append(
                    "LEGACY HOLD RETAINED "
                    f"{_single_line(item.get('rollout_id') or 'unknown')} "
                    f"{_single_line(item.get('host') or 'unknown')}: "
                    f"{_single_line(item.get('reason') or 'identity unknown')}"
                )
    failed_hosts = payload.get("failed_hosts")
    if isinstance(failed_hosts, Mapping):
        ordered_failures = sorted(
            failed_hosts.items(),
            key=lambda item: str(item[0]),
        )
        for host, reason in ordered_failures:
            lines.append(
                f"FAILED HOST {_single_line(host)}: {_single_line(reason)}"
            )
    retained_legacy = payload.get("retained_legacy_holds")
    if isinstance(retained_legacy, list):
        for hold in retained_legacy:
            if not isinstance(hold, Mapping):
                continue
            lines.append(
                "RETAINED LEGACY HOLD FENCE "
                f"{_single_line(hold.get('rollout_id') or 'unknown')} "
                f"{_single_line(hold.get('host') or 'unknown')}: "
                f"{_single_line(hold.get('reason') or 'identity unknown')}"
            )
    retained_fences = payload.get("retained_legacy_fences")
    if isinstance(retained_fences, list):
        for fence in retained_fences:
            if not isinstance(fence, Mapping):
                continue
            lines.append(
                "RETAINED LEGACY PLAN FENCE "
                f"{_single_line(fence.get('rollout_id') or 'unknown')} "
                f"{_single_line(fence.get('host') or 'unknown')}: "
                f"{_single_line(fence.get('reason') or 'identity unknown')}"
            )
    retained = payload.get("retained_rollout_holds")
    if isinstance(retained, list):
        for hold in retained:
            if not isinstance(hold, Mapping):
                continue
            host = _single_line(hold.get("host") or "unknown")
            kind = _single_line(hold.get("kind") or "drain")
            status = _single_line(hold.get("status") or "unknown")
            reason = _single_line(hold.get("reason") or "no reason recorded")
            lines.append(
                f"RETAINED ROLLOUT HOLD {host} {kind} "
                f"journal_status={status} reason={reason} "
                "(journal evidence; current liveness not asserted)"
            )
    preserved = payload.get("preserved_external_holds")
    if isinstance(preserved, list):
        for hold in preserved:
            if not isinstance(hold, Mapping):
                continue
            host = _single_line(hold.get("host") or "unknown")
            kind = _single_line(hold.get("kind") or "drain")
            owner = _single_line(hold.get("owner") or "external")
            reason = _single_line(hold.get("reason") or "no reason recorded")
            lines.append(
                f"PRESERVED EXTERNAL HOLD {host} {kind} "
                f"owner={owner} reason={reason}"
            )
    liveness = payload.get("drain_liveness")
    if isinstance(liveness, Mapping):
        active_holds = liveness.get("active_holds")
        if isinstance(active_holds, list):
            for hold in active_holds:
                if not isinstance(hold, Mapping):
                    continue
                owner_class = _liveness_text(
                    hold.get("owner_class"),
                    default="unknown",
                )
                if owner_class == "rollout":
                    label = "OBSERVED ACTIVE ROLLOUT HOLD"
                elif owner_class in {"external", "legacy"}:
                    label = "OBSERVED ACTIVE EXTERNAL HOLD"
                elif owner_class == "safety-fail-closed":
                    label = "OBSERVED ACTIVE SAFETY HOLD"
                else:
                    label = "OBSERVED ACTIVE HOLD"
                host = _liveness_text(hold.get("host"), default="unknown")
                kind = _liveness_text(hold.get("kind"), default="drain")
                owner = _liveness_text(
                    hold.get("owner"),
                    default=owner_class or "unknown",
                )
                reason = _liveness_text(
                    hold.get("reason"),
                    default="no reason recorded",
                )
                observed = _liveness_text(
                    hold.get("remote_observed_at"),
                    default="unknown time",
                )
                lines.append(
                    f"{label} {host} {kind} owner_class={owner_class} "
                    f"owner={owner} reason={reason} "
                    f"(final sweep as of {observed})"
                )
        unknown_hosts = liveness.get("unknown_hosts")
        if isinstance(unknown_hosts, list):
            for item in unknown_hosts:
                if not isinstance(item, Mapping):
                    continue
                host = _liveness_text(item.get("host"), default="unknown")
                reason = _liveness_text(
                    item.get("reason"),
                    default="read-only observation unavailable",
                )
                lines.append(f"DRAIN LIVENESS UNKNOWN {host}: {reason}")
    topology_errors = payload.get("topology_errors")
    if isinstance(topology_errors, list):
        lines.extend(f"BLOCK topology {error}" for error in topology_errors)
    failures = payload.get("doctor_failures")
    if isinstance(failures, dict):
        for host, checks in failures.items():
            lines.append(f"DOCTOR FAIL {host}: {', '.join(checks)}")
    lanes = payload.get("lanes")
    if isinstance(lanes, list):
        for lane in lanes:
            changed = "changed" if lane.get("changed") else "skipped"
            lines.append(
                f"{str(lane['decision']).upper():6} {str(lane['host']):16} "
                f"{str(lane['module']):16} "
                f"{str(lane.get('version') or '-'):12} "
                f"{str(lane.get('sha') or '-')[:12]}  "
                f"{changed}: {lane['reason']}"
            )
    provenance = payload.get("provenance_lanes")
    if isinstance(provenance, list):
        for lane in provenance:
            if not isinstance(lane, Mapping):
                continue
            lines.append(
                f"{str(lane.get('decision') or 'defer').upper():6} "
                f"{str(lane.get('host') or 'unknown'):16} "
                f"{str(lane.get('component') or 'provenance'):16} "
                f"{str(lane.get('current_version') or '-'):12} "
                f"{str(lane.get('current_sha') or '-')[:12]}  "
                "provenance: "
                f"{_single_line(lane.get('reason') or 'no reason recorded')}"
            )
    return "\n".join(lines)
