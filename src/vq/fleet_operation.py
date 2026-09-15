"""Durable local supervision for one mutating fleet rollout action.

The rollout controller and the process which performs an update have different
failure domains.  This module gives them a small, config-free protocol: a
controller prepares an immutable request, starts this module in a detached
interpreter, waits for its nonce-bearing ready receipt, revalidates the
accepted report, and only then writes authorization.  The supervisor holds a
lifetime ``flock`` while it launches and waits for exactly one ``python -m vq``
child.

The lock, rather than a PID probe, is the cross-platform liveness authority.
Linux process fingerprints are additional evidence only.  Once the lock is
free, an authorization without a terminal result is deliberately classified
as outcome-unknown and is never permission to launch the same operation again.
"""

from __future__ import annotations

import argparse
import contextlib
import errno
import fcntl
import hashlib
import json
import math
import os
import re
import secrets
import stat
import subprocess
import sys
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from vq import paths

IDENTITY_SCHEMA = "vq.fleet.rollout_operation_identity/1"
REQUEST_SCHEMA = "vq.fleet.rollout_operation_request/1"
READY_SCHEMA = "vq.fleet.rollout_operation_ready/1"
AUTHORIZATION_SCHEMA = "vq.fleet.rollout_operation_authorization/1"
ACTIVATION_SCHEMA = "vq.fleet.rollout_operation_activation/1"
RESULT_SCHEMA = "vq.fleet.rollout_operation_result/1"
DETACHED_SCHEDULER_BINDING_SCHEMA = (
    "vq.fleet.rollout_detached_scheduler_binding/1"
)
DETACHED_SCHEDULER_PROTOCOL = "fixed-host-detached-build/1"

ENV_OPERATION_ID = "VQ_FLEET_OPERATION_ID"
ENV_OPERATION_REQUEST_SHA256 = "VQ_FLEET_OPERATION_REQUEST_SHA256"
ENV_OPERATION_NONCE = "VQ_FLEET_OPERATION_NONCE"
ENV_LIFECYCLE_HANDOFF = "VQ_TOOLSET_LIFECYCLE_HANDOFF"
_OPERATION_ENV_PREFIX = "VQ_FLEET_OPERATION_"
_OPERATION_ENV_NAMES = frozenset(
    {
        ENV_OPERATION_ID,
        ENV_OPERATION_REQUEST_SHA256,
        ENV_OPERATION_NONCE,
    }
)

MAX_RECEIPT_BYTES = 1024 * 1024
MAX_OUTPUT_BYTES = 4 * 1024 * 1024
MAX_OUTPUT_TAIL_BYTES = 1024 * 1024
DEFAULT_AUTHORIZATION_TIMEOUT_SECONDS = 300.0
DEFAULT_POLL_INTERVAL_SECONDS = 0.05
SUPERVISOR_NOT_AUTHORIZED_EXIT_CODE = 75
SUPERVISOR_INVALID_REQUEST_EXIT_CODE = 76
SUPERVISOR_INTERNAL_ERROR_EXIT_CODE = 70

_OPERATION_ID = re.compile(r"[0-9a-f]{64}\Z")
_REPORT_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_SOURCE_SHA = re.compile(r"[0-9a-f]{40}\Z")
_NONCE = re.compile(r"[0-9a-f]{64}\Z")
_PHASES = frozenset({"driver", "helper", "scheduler-runtime", "local-runtime"})
_CREDENTIAL_OPTIONS = frozenset(
    {
        "--admin-token",
        "--credential",
        "--passphrase",
        "--password",
        "--secret",
        "--token",
        "--token-file",
        "--token-stdin",
    }
)
_CREDENTIAL_ASSIGNMENT = re.compile(r"(?i)^(?:VQ_TOKEN|[^=]*(?:PASSWORD|PASSPHRASE|SECRET|TOKEN))=")
_OPERATION_FILES = frozenset(
    {
        "request.json",
        "lease.lock",
        "decision.lock",
        "output.log",
        "ready.json",
        "authorization.json",
        "activation.json",
        "result.json",
        "scheduler-command.json",
    }
)
_RECEIPT_TEMP = re.compile(
    r"^\.(request|ready|authorization|activation|result|scheduler-command)\.json\."
    r"[0-9a-f]{32}\.tmp\Z"
)

ObservationState = Literal[
    "prepared",
    "starting",
    "running-pre-authorization",
    "running-authorized",
    "authorized-unactivated",
    "abandoned-pre-authorization",
    "outcome-unknown",
    "completed",
]
ChildPopen = Callable[..., subprocess.Popen[bytes]]


class OperationError(RuntimeError):
    """Base class for a durable fleet-operation failure."""


class OperationSecurityError(OperationError):
    """An identity or managed path violates the persistence boundary."""


class OperationStateError(OperationError):
    """Receipts are missing, malformed, inconsistent, or unsafe to replay."""


class OperationBusyError(OperationError):
    """Another supervisor owns the operation's lifetime lease."""


@dataclass(frozen=True)
class OperationExecutionContext:
    """Validated immutable identity exported only to an activated child."""

    operation_id: str
    request_sha256: str
    nonce: str

    def __post_init__(self) -> None:
        for label, value, pattern in (
            ("operation id", self.operation_id, _OPERATION_ID),
            ("operation request digest", self.request_sha256, _REPORT_DIGEST),
            ("operation nonce", self.nonce, _NONCE),
        ):
            if not isinstance(value, str) or pattern.fullmatch(value) is None:
                raise OperationSecurityError(f"{label} must be 64 lowercase hex characters")


@dataclass(frozen=True)
class DetachedSchedulerCommandBinding:
    """Create-once local authority for one fixed-host detached command."""

    operation_id: str
    request_sha256: str
    nonce: str
    target: str
    program: str
    mode: str
    protocol: str
    run_id: str
    bound_at: str
    remote_run_dir: str
    command_sha256: str
    remote_request_sha256: str

    def __post_init__(self) -> None:
        OperationExecutionContext(
            operation_id=self.operation_id,
            request_sha256=self.request_sha256,
            nonce=self.nonce,
        )
        target = _validate_text("detached scheduler target", self.target)
        assert target is not None
        program = _validate_text("detached scheduler program", self.program)
        assert program is not None
        mode = _validate_text("detached scheduler mode", self.mode)
        assert mode is not None
        if self.protocol != DETACHED_SCHEDULER_PROTOCOL:
            raise OperationSecurityError("unsupported detached scheduler protocol")
        if not isinstance(self.run_id, str) or _NONCE.fullmatch(self.run_id) is None:
            raise OperationSecurityError(
                "detached scheduler run id must be 64 lowercase hex characters"
            )
        try:
            bound_at = datetime.fromisoformat(self.bound_at)
        except (TypeError, ValueError) as exc:
            raise OperationSecurityError(
                "detached scheduler bound-at timestamp is invalid"
            ) from exc
        if bound_at.tzinfo is None or bound_at.utcoffset() != UTC.utcoffset(bound_at):
            raise OperationSecurityError(
                "detached scheduler bound-at timestamp must be UTC"
            )
        run_dir = _validate_text("detached scheduler remote run directory", self.remote_run_dir)
        assert run_dir is not None
        if (
            not run_dir.startswith("/")
            or run_dir.startswith("//")
            or run_dir == "/"
            or os.path.normpath(run_dir) != run_dir
        ):
            raise OperationSecurityError(
                "detached scheduler remote run directory must be a normalized absolute path"
            )
        if run_dir.rsplit("/", 1)[-1] != f"run-{self.run_id}":
            raise OperationSecurityError(
                "detached scheduler remote run directory does not match run id"
            )
        for label, value in (
            ("command", self.command_sha256),
            ("remote request", self.remote_request_sha256),
        ):
            if not isinstance(value, str) or not _REPORT_DIGEST.fullmatch(value):
                raise OperationSecurityError(
                    f"detached scheduler {label} digest must be 64 lowercase hex characters"
                )

    def as_dict(self) -> dict[str, object]:
        return {
            "schema": DETACHED_SCHEDULER_BINDING_SCHEMA,
            "operation_id": self.operation_id,
            "request_sha256": self.request_sha256,
            "nonce": self.nonce,
            "target": self.target,
            "program": self.program,
            "mode": self.mode,
            "protocol": self.protocol,
            "run_id": self.run_id,
            "bound_at": self.bound_at,
            "remote_run_dir": self.remote_run_dir,
            "command_sha256": self.command_sha256,
            "remote_request_sha256": self.remote_request_sha256,
        }

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, object],
    ) -> DetachedSchedulerCommandBinding:
        expected = {
            "schema",
            "operation_id",
            "request_sha256",
            "nonce",
            "target",
            "program",
            "mode",
            "protocol",
            "run_id",
            "bound_at",
            "remote_run_dir",
            "command_sha256",
            "remote_request_sha256",
        }
        if set(payload) != expected or payload.get("schema") != (
            DETACHED_SCHEDULER_BINDING_SCHEMA
        ):
            raise OperationStateError(
                "detached scheduler binding has invalid schema or fields"
            )
        try:
            return cls(
                operation_id=payload["operation_id"],  # type: ignore[arg-type]
                request_sha256=payload["request_sha256"],  # type: ignore[arg-type]
                nonce=payload["nonce"],  # type: ignore[arg-type]
                target=payload["target"],  # type: ignore[arg-type]
                program=payload["program"],  # type: ignore[arg-type]
                mode=payload["mode"],  # type: ignore[arg-type]
                protocol=payload["protocol"],  # type: ignore[arg-type]
                run_id=payload["run_id"],  # type: ignore[arg-type]
                bound_at=payload["bound_at"],  # type: ignore[arg-type]
                remote_run_dir=payload["remote_run_dir"],  # type: ignore[arg-type]
                command_sha256=payload["command_sha256"],  # type: ignore[arg-type]
                remote_request_sha256=payload["remote_request_sha256"],  # type: ignore[arg-type]
            )
        except OperationSecurityError as exc:
            raise OperationStateError(f"invalid detached scheduler binding: {exc}") from exc


def _validate_text(name: str, value: object, *, optional: bool = False) -> str | None:
    if optional and value is None:
        return None
    if not isinstance(value, str) or not value:
        raise OperationSecurityError(f"{name} must be a non-empty string")
    if len(value) > 65_536:
        raise OperationSecurityError(f"{name} is too long")
    if any(character in value for character in ("\x00", "\r", "\n")):
        raise OperationSecurityError(f"{name} contains a control character")
    return value


def execution_context_from_environ(
    environ: Mapping[str, str] | None = None,
) -> OperationExecutionContext | None:
    """Read the exact all-or-none context emitted by the execution recorder.

    An ordinary/manual ``vq`` child has none of these variables and keeps its
    historical behavior.  A partial or malformed tuple is never downgraded to
    that legacy path: callers must fail before remote mutation instead.
    """
    source = os.environ if environ is None else environ
    unexpected = sorted(
        name
        for name in source
        if name.startswith(_OPERATION_ENV_PREFIX) and name not in _OPERATION_ENV_NAMES
    )
    if unexpected:
        raise OperationSecurityError(
            "fleet operation execution context contains unknown reserved variables: "
            + ", ".join(unexpected)
        )
    present = {name for name in _OPERATION_ENV_NAMES if name in source}
    if not present:
        return None
    if present != _OPERATION_ENV_NAMES:
        raise OperationSecurityError(
            "fleet operation execution context must contain exactly operation id, "
            "request digest, and nonce"
        )
    return OperationExecutionContext(
        operation_id=source[ENV_OPERATION_ID],
        request_sha256=source[ENV_OPERATION_REQUEST_SHA256],
        nonce=source[ENV_OPERATION_NONCE],
    )


def _scrubbed_operation_environment() -> dict[str, str]:
    """Return ambient state without any spoofable operation context."""
    return {
        name: value
        for name, value in os.environ.items()
        if not name.startswith(_OPERATION_ENV_PREFIX)
        and name != ENV_LIFECYCLE_HANDOFF
    }


def _activated_child_environment(
    handle: OperationHandle,
    *,
    nonce: str,
    lifecycle_handoff: str | None = None,
) -> dict[str, str]:
    """Replace every ambient fleet-operation variable with the exact tuple."""
    environment = _scrubbed_operation_environment()
    environment[paths.ENV_STATE_DIR] = str(handle.state_root)
    environment.update(
        {
            ENV_OPERATION_ID: handle.operation_id,
            ENV_OPERATION_REQUEST_SHA256: handle.request_sha256,
            ENV_OPERATION_NONCE: nonce,
        }
    )
    if lifecycle_handoff is not None:
        environment[ENV_LIFECYCLE_HANDOFF] = lifecycle_handoff
    return environment


def _lifecycle_handoff_fds(
    payload: str | None,
    *,
    expected_resources: tuple[tuple[str, str], ...],
    expected_rollout_id: str,
    expected_rollout_lock_path: str | None,
) -> tuple[int, ...]:
    """Return the exact open descriptors named by a controller handoff.

    The admin child performs the resource/path/inode/flock validation before
    treating these descriptors as lifecycle ownership.  The durable supervisor
    only needs to retain the numbered descriptors across both exec boundaries
    so a controller death cannot release its checkout fence while the action
    is still running.
    """
    if payload is None:
        if expected_resources:
            raise OperationSecurityError(
                "operation requires a lifecycle handoff"
            )
        return ()
    try:
        raw = json.loads(payload)
    except (TypeError, json.JSONDecodeError) as exc:
        raise OperationSecurityError(
            "lifecycle handoff is not valid JSON"
        ) from exc
    if (
        not isinstance(raw, dict)
        or set(raw) not in (
            {"schema", "locks"},
            {"schema", "locks", "rollout_lock"},
        )
    ):
        raise OperationSecurityError("lifecycle handoff has invalid fields")
    if raw.get("schema") != "vq.toolset.lifecycle_handoff/1":
        raise OperationSecurityError("lifecycle handoff has an invalid schema")
    locks = raw.get("locks")
    if not isinstance(locks, list) or not locks:
        raise OperationSecurityError("lifecycle handoff has no locks")
    result: list[int] = []
    resources: list[tuple[str, str]] = []
    for item in locks:
        if not isinstance(item, dict) or set(item) != {
            "scope", "resource", "fd", "path",
        }:
            raise OperationSecurityError(
                "lifecycle handoff lock has invalid fields"
            )
        fd = item.get("fd")
        scope = item.get("scope")
        resource = item.get("resource")
        path = item.get("path")
        if (
            scope not in {"checkout", "target"}
            or not isinstance(resource, str)
            or not resource.startswith("/")
            or not isinstance(path, str)
            or not path.startswith("/")
        ):
            raise OperationSecurityError(
                "lifecycle handoff lock identity is malformed"
            )
        if not isinstance(fd, int) or isinstance(fd, bool) or fd < 3:
            raise OperationSecurityError(
                "lifecycle handoff fd must be an inherited descriptor"
            )
        try:
            os.fstat(fd)
        except OSError as exc:
            raise OperationSecurityError(
                "lifecycle handoff descriptor is not open"
            ) from exc
        result.append(fd)
        resources.append((scope, resource))
    rollout_lock = raw.get("rollout_lock")
    if expected_rollout_lock_path is None and rollout_lock is not None:
        raise OperationSecurityError(
            "operation does not authorize a rollout-lock handoff"
        )
    if expected_rollout_lock_path is not None and rollout_lock is None:
        raise OperationSecurityError(
            "operation requires the global rollout-lock handoff"
        )
    if rollout_lock is not None:
        if not isinstance(rollout_lock, dict) or set(rollout_lock) != {
            "rollout_id", "fd", "path",
        }:
            raise OperationSecurityError(
                "lifecycle handoff rollout lock has invalid fields"
            )
        rollout_fd = rollout_lock.get("fd")
        rollout_id = rollout_lock.get("rollout_id")
        rollout_path = rollout_lock.get("path")
        if (
            rollout_id != expected_rollout_id
            or not isinstance(rollout_fd, int)
            or isinstance(rollout_fd, bool)
            or rollout_fd < 3
            or not isinstance(rollout_path, str)
            or rollout_path != expected_rollout_lock_path
        ):
            raise OperationSecurityError(
                "lifecycle handoff rollout lock is malformed"
            )
        try:
            info = os.fstat(rollout_fd)
            named = os.stat(rollout_path, follow_symlinks=False)
            parent = Path(rollout_path).parent.lstat()
        except OSError as exc:
            raise OperationSecurityError(
                "lifecycle handoff rollout descriptor is not open"
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
            raise OperationSecurityError(
                "lifecycle handoff does not bind the exact rollout lock"
            )
        try:
            fcntl.flock(rollout_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise OperationSecurityError(
                "lifecycle handoff does not own the rollout lock"
            ) from exc
        result.append(rollout_fd)
    if len(set(result)) != len(result):
        raise OperationSecurityError(
            "lifecycle handoff repeats a descriptor"
        )
    if tuple(sorted(resources)) != tuple(sorted(expected_resources)):
        raise OperationSecurityError(
            "lifecycle handoff resources do not match the immutable operation"
        )
    return tuple(result)


def _validate_argv(argv: object) -> tuple[str, ...]:
    if not isinstance(argv, (list, tuple)) or not argv:
        raise OperationSecurityError("argv must be a non-empty string sequence")
    result: list[str] = []
    consume_as_credential = False
    for index, raw in enumerate(argv):
        value = _validate_text(f"argv[{index}]", raw)
        assert value is not None
        option = value.partition("=")[0].lower()
        if consume_as_credential or option in _CREDENTIAL_OPTIONS:
            raise OperationSecurityError(
                "credential-bearing options are forbidden in durable operation argv"
            )
        if _CREDENTIAL_ASSIGNMENT.match(value):
            raise OperationSecurityError(
                "credential-bearing assignments are forbidden in durable operation argv"
            )
        # Kept explicit for readability if an option with a separate value is
        # added above: the option itself is rejected before its value is seen.
        consume_as_credential = False
        result.append(value)
    return tuple(result)


@dataclass(frozen=True)
class OperationIdentity:
    """Immutable fields which select exactly one rollout action attempt."""

    rollout_id: str
    report_digest_sha256: str
    attempt: int
    action_id: str
    phase: str
    host: str
    program: str
    pin_name: str
    target_sha: str
    target_version: str | None
    target_tag: str | None
    argv: tuple[str, ...]
    lifecycle_resources: tuple[tuple[str, str], ...] = ()
    rollout_lock_path: str | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "rollout_id",
            "action_id",
            "host",
            "program",
            "pin_name",
        ):
            _validate_text(field_name, getattr(self, field_name))
        if not isinstance(self.attempt, int) or isinstance(self.attempt, bool) or self.attempt < 1:
            raise OperationSecurityError("attempt must be a positive integer")
        if not isinstance(self.report_digest_sha256, str) or not _REPORT_DIGEST.fullmatch(
            self.report_digest_sha256
        ):
            raise OperationSecurityError(
                "report_digest_sha256 must be 64 lowercase hexadecimal characters"
            )
        if self.phase not in _PHASES:
            raise OperationSecurityError(f"unsupported rollout phase {self.phase!r}")
        if not isinstance(self.target_sha, str) or not _SOURCE_SHA.fullmatch(self.target_sha):
            raise OperationSecurityError("target_sha must be 40 lowercase hexadecimal characters")
        _validate_text("target_version", self.target_version, optional=True)
        _validate_text("target_tag", self.target_tag, optional=True)
        object.__setattr__(self, "argv", _validate_argv(self.argv))
        resources: list[tuple[str, str]] = []
        for item in self.lifecycle_resources:
            if (
                not isinstance(item, (list, tuple))
                or len(item) != 2
                or item[0] not in {"checkout", "target"}
                or not isinstance(item[1], str)
                or not item[1].startswith("/")
                or "\x00" in item[1]
            ):
                raise OperationSecurityError(
                    "lifecycle resources must be exact checkout/target "
                    "absolute paths"
                )
            resources.append((item[0], item[1]))
        if len(set(resources)) != len(resources):
            raise OperationSecurityError("lifecycle resources must be unique")
        object.__setattr__(
            self,
            "lifecycle_resources",
            tuple(sorted(resources)),
        )
        if self.rollout_lock_path is not None:
            if (
                not isinstance(self.rollout_lock_path, str)
                or not self.rollout_lock_path.startswith("/")
                or "\x00" in self.rollout_lock_path
            ):
                raise OperationSecurityError(
                    "rollout_lock_path must be an exact absolute path"
                )
            if not self.lifecycle_resources:
                raise OperationSecurityError(
                    "a rollout lock requires exact lifecycle resources"
                )

    def constructor_fields(self) -> dict[str, object]:
        """Return the strict constructor shape, excluding the schema tag."""
        payload = {
            "rollout_id": self.rollout_id,
            "report_digest_sha256": self.report_digest_sha256,
            "attempt": self.attempt,
            "action_id": self.action_id,
            "phase": self.phase,
            "host": self.host,
            "program": self.program,
            "pin_name": self.pin_name,
            "target_sha": self.target_sha,
            "target_version": self.target_version,
            "target_tag": self.target_tag,
            "argv": self.argv,
        }
        if self.lifecycle_resources:
            payload["lifecycle_resources"] = self.lifecycle_resources
        if self.rollout_lock_path is not None:
            payload["rollout_lock_path"] = self.rollout_lock_path
        return payload

    def as_dict(self) -> dict[str, object]:
        payload = self.constructor_fields()
        payload["argv"] = list(self.argv)
        if self.lifecycle_resources:
            payload["lifecycle_resources"] = [
                list(item) for item in self.lifecycle_resources
            ]
        return {"schema": IDENTITY_SCHEMA, **payload}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> OperationIdentity:
        expected = {
            "schema",
            "rollout_id",
            "report_digest_sha256",
            "attempt",
            "action_id",
            "phase",
            "host",
            "program",
            "pin_name",
            "target_sha",
            "target_version",
            "target_tag",
            "argv",
        }
        allowed_shapes = {
            frozenset(expected | subset)
            for subset in (
                set(),
                {"lifecycle_resources"},
                {"lifecycle_resources", "rollout_lock_path"},
            )
        }
        if (
            frozenset(payload) not in allowed_shapes
            or payload.get("schema") != IDENTITY_SCHEMA
        ):
            raise OperationStateError("operation identity has an invalid schema or fields")
        try:
            return cls(
                rollout_id=payload["rollout_id"],  # type: ignore[arg-type]
                report_digest_sha256=payload["report_digest_sha256"],  # type: ignore[arg-type]
                attempt=payload["attempt"],  # type: ignore[arg-type]
                action_id=payload["action_id"],  # type: ignore[arg-type]
                phase=payload["phase"],  # type: ignore[arg-type]
                host=payload["host"],  # type: ignore[arg-type]
                program=payload["program"],  # type: ignore[arg-type]
                pin_name=payload["pin_name"],  # type: ignore[arg-type]
                target_sha=payload["target_sha"],  # type: ignore[arg-type]
                target_version=payload["target_version"],  # type: ignore[arg-type]
                target_tag=payload["target_tag"],  # type: ignore[arg-type]
                argv=payload["argv"],  # type: ignore[arg-type]
                lifecycle_resources=payload.get("lifecycle_resources", ()),  # type: ignore[arg-type]
                rollout_lock_path=payload.get("rollout_lock_path"),  # type: ignore[arg-type]
            )
        except OperationSecurityError as exc:
            raise OperationStateError(f"invalid persisted operation identity: {exc}") from exc


@dataclass(frozen=True)
class OperationHandle:
    """Validated path and identity for one prepared operation."""

    operation_id: str
    state_root: Path
    directory: Path
    identity: OperationIdentity
    request_sha256: str


@dataclass(frozen=True)
class OperationObservation:
    """Conservative reconciliation result for a prepared operation."""

    operation_id: str
    identity: OperationIdentity
    request_sha256: str
    state: ObservationState
    retry_safe: bool
    lease_busy: bool
    request: Mapping[str, object]
    ready: Mapping[str, object] | None
    authorization: Mapping[str, object] | None
    activation: Mapping[str, object] | None
    result: Mapping[str, object] | None


@dataclass(frozen=True)
class OperationOutputTail:
    """Bounded terminal output returned only after digest verification."""

    text: str
    truncated: bool
    bytes_read: int
    stored_bytes: int
    total_bytes: int
    output_sha256: str


@dataclass(frozen=True)
class OperationOutputChunk:
    """One bounded, offset-pinned read of the protected live spool."""

    data: bytes
    offset: int
    next_offset: int
    stored_bytes: int
    at_end: bool
    spool_full: bool


def _canonical_json(payload: Mapping[str, object]) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def operation_id(identity: OperationIdentity) -> str:
    """Return the canonical SHA-256 key for an immutable identity."""
    return hashlib.sha256(_canonical_json(identity.as_dict())).hexdigest()


def _state_root(state_root: Path | None) -> Path:
    selected = state_root if state_root is not None else paths.state_root()
    return selected.expanduser().absolute()


def operations_root(*, state_root: Path | None = None) -> Path:
    return _state_root(state_root) / "rollouts" / "operations"


def _validate_operation_id(value: str) -> str:
    if not isinstance(value, str) or not _OPERATION_ID.fullmatch(value):
        raise OperationSecurityError(
            "operation id must be exactly 64 lowercase hexadecimal characters"
        )
    return value


def _directory_flags() -> int:
    required = ("O_DIRECTORY", "O_NOFOLLOW")
    if any(not hasattr(os, name) for name in required):
        raise OperationSecurityError("platform lacks required no-follow directory opens")
    return os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)


def _validate_directory_fd(fd: int, *, private: bool, label: str) -> os.stat_result:
    info = os.fstat(fd)
    if not stat.S_ISDIR(info.st_mode):
        raise OperationSecurityError(f"unsafe {label}: expected a real directory")
    if info.st_uid != os.geteuid():
        raise OperationSecurityError(f"unsafe {label}: wrong owner")
    mode = stat.S_IMODE(info.st_mode)
    if private and mode != 0o700:
        raise OperationSecurityError(f"unsafe {label}: expected mode 0700")
    if not private and mode & 0o022:
        raise OperationSecurityError(f"unsafe {label}: group/world writable")
    if os.get_inheritable(fd):
        os.set_inheritable(fd, False)
    return info


def _open_root_fd(state_root: Path, *, create: bool) -> int:
    if create:
        with contextlib.suppress(FileExistsError):
            state_root.mkdir(mode=0o700, parents=True)
    try:
        fd = os.open(state_root, _directory_flags())
    except FileNotFoundError as exc:
        raise OperationStateError(f"missing state root {state_root}") from exc
    except OSError as exc:
        raise OperationSecurityError(f"could not safely open state root {state_root}") from exc
    try:
        _validate_directory_fd(fd, private=False, label="state root")
    except Exception:
        os.close(fd)
        raise
    return fd


def _open_directory_at(
    parent_fd: int,
    name: str,
    *,
    private: bool,
    label: str,
    create: bool,
) -> int:
    if "/" in name or name in {"", ".", ".."}:
        raise OperationSecurityError(f"unsafe managed directory name {name!r}")
    try:
        fd = os.open(name, _directory_flags(), dir_fd=parent_fd)
    except FileNotFoundError:
        if not create:
            raise OperationStateError(f"missing {label}") from None
        try:
            os.mkdir(name, mode=0o700, dir_fd=parent_fd)
            os.fsync(parent_fd)
        except FileExistsError:
            pass
        try:
            fd = os.open(name, _directory_flags(), dir_fd=parent_fd)
        except OSError as exc:
            raise OperationSecurityError(f"could not safely open {label}") from exc
    except OSError as exc:
        raise OperationSecurityError(f"could not safely open {label}") from exc
    try:
        _validate_directory_fd(fd, private=private, label=label)
    except Exception:
        os.close(fd)
        raise
    return fd


@contextlib.contextmanager
def _open_operations_fd(state_root: Path, *, create: bool) -> Any:
    root_fd = _open_root_fd(state_root, create=create)
    rollout_fd = -1
    operation_root_fd = -1
    try:
        rollout_fd = _open_directory_at(
            root_fd,
            "rollouts",
            private=False,
            label="rollout root",
            create=create,
        )
        operation_root_fd = _open_directory_at(
            rollout_fd,
            "operations",
            private=True,
            label="operation root",
            create=create,
        )
        yield operation_root_fd
    finally:
        if operation_root_fd >= 0:
            os.close(operation_root_fd)
        if rollout_fd >= 0:
            os.close(rollout_fd)
        os.close(root_fd)


@contextlib.contextmanager
def _open_operation_fd(
    operation: str,
    *,
    state_root: Path,
    create: bool,
) -> Any:
    operation = _validate_operation_id(operation)
    with _open_operations_fd(state_root, create=create) as operation_root_fd:
        operation_fd = _open_directory_at(
            operation_root_fd,
            operation,
            private=True,
            label="operation directory",
            create=create,
        )
        try:
            yield operation_fd
        finally:
            os.close(operation_fd)


def _validate_regular_at(
    directory_fd: int,
    name: str,
    *,
    label: str,
    size_limit: int | None,
) -> os.stat_result:
    if "/" in name or name in {"", ".", ".."}:
        raise OperationSecurityError(f"unsafe managed file name {name!r}")
    try:
        info = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError as exc:
        raise OperationStateError(f"missing {label}") from exc
    if not stat.S_ISREG(info.st_mode):
        raise OperationSecurityError(f"unsafe {label}: expected a regular file")
    if info.st_uid != os.geteuid():
        raise OperationSecurityError(f"unsafe {label}: wrong owner")
    if stat.S_IMODE(info.st_mode) != 0o600:
        raise OperationSecurityError(f"unsafe {label}: expected mode 0600")
    if info.st_nlink != 1:
        raise OperationSecurityError(f"unsafe {label}: unexpected hard links")
    if size_limit is not None and info.st_size > size_limit:
        raise OperationSecurityError(f"unsafe {label}: file is too large")
    return info


def _secure_open_at(directory_fd: int, name: str, *, flags: int, label: str) -> int:
    expected = _validate_regular_at(directory_fd, name, label=label, size_limit=None)
    try:
        fd = os.open(
            name,
            flags | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
            dir_fd=directory_fd,
        )
    except OSError as exc:
        raise OperationSecurityError(f"could not safely open {label}") from exc
    actual = os.fstat(fd)
    if (actual.st_dev, actual.st_ino) != (expected.st_dev, expected.st_ino):
        os.close(fd)
        raise OperationSecurityError(f"unsafe {label}: file changed while opening")
    if (
        not stat.S_ISREG(actual.st_mode)
        or stat.S_IMODE(actual.st_mode) != 0o600
        or actual.st_uid != os.geteuid()
        or actual.st_nlink != 1
    ):
        os.close(fd)
        raise OperationSecurityError(f"unsafe {label}: invalid opened file")
    if os.get_inheritable(fd):
        os.set_inheritable(fd, False)
    return fd


def _create_private_at(directory_fd: int, name: str, *, label: str) -> int:
    try:
        fd = os.open(
            name,
            os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
            0o600,
            dir_fd=directory_fd,
        )
    except FileExistsError:
        return _secure_open_at(directory_fd, name, flags=os.O_RDWR, label=label)
    except OSError as exc:
        raise OperationSecurityError(f"could not create {label}") from exc
    try:
        if os.get_inheritable(fd):
            os.set_inheritable(fd, False)
        os.fsync(fd)
        os.fsync(directory_fd)
    except Exception:
        os.close(fd)
        raise
    return fd


def _contains_nul(value: object) -> bool:
    if isinstance(value, str):
        return "\x00" in value
    if isinstance(value, Mapping):
        return any(_contains_nul(key) or _contains_nul(item) for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return any(_contains_nul(item) for item in value)
    return False


def _write_receipt_at(
    directory_fd: int,
    name: str,
    payload: Mapping[str, object],
) -> dict[str, object]:
    """Create one immutable, durable 0600 JSON receipt without replacement."""
    if name not in {
        "request.json",
        "ready.json",
        "authorization.json",
        "activation.json",
        "result.json",
        "scheduler-command.json",
    }:
        raise OperationSecurityError(f"unsupported receipt name {name!r}")
    if _contains_nul(payload):
        raise OperationSecurityError("receipt payload contains NUL")
    try:
        encoded = json.dumps(
            payload,
            allow_nan=False,
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise OperationSecurityError("receipt payload is not strict JSON") from exc
    encoded += b"\n"
    if len(encoded) > MAX_RECEIPT_BYTES:
        raise OperationSecurityError("receipt payload is too large")
    try:
        _validate_regular_at(
            directory_fd,
            name,
            label="existing receipt",
            size_limit=MAX_RECEIPT_BYTES,
        )
    except OperationStateError:
        pass
    else:
        raise OperationStateError(f"receipt already exists: {name}")

    temporary = f".{name}.{secrets.token_hex(16)}.tmp"
    fd = -1
    linked = False
    try:
        fd = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
            0o600,
            dir_fd=directory_fd,
        )
        if os.get_inheritable(fd):
            os.set_inheritable(fd, False)
        with os.fdopen(fd, "wb") as stream:
            fd = -1
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(
                temporary,
                name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )
            linked = True
        except FileExistsError as exc:
            _validate_regular_at(
                directory_fd,
                name,
                label="racing receipt",
                size_limit=MAX_RECEIPT_BYTES,
            )
            raise OperationStateError(f"receipt already exists: {name}") from exc
        os.unlink(temporary, dir_fd=directory_fd)
        os.fsync(directory_fd)
    finally:
        if fd >= 0:
            os.close(fd)
        if not linked:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(temporary, dir_fd=directory_fd)
    return dict(payload)


def _reject_duplicate_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _reject_nonfinite(value: str) -> object:
    raise ValueError(f"non-finite JSON number {value}")


def _read_receipt_at(directory_fd: int, name: str, *, label: str) -> dict[str, object]:
    expected = _validate_regular_at(
        directory_fd,
        name,
        label=label,
        size_limit=MAX_RECEIPT_BYTES,
    )
    fd = _secure_open_at(directory_fd, name, flags=os.O_RDONLY, label=label)
    try:
        actual = os.fstat(fd)
        if actual.st_size != expected.st_size or actual.st_size > MAX_RECEIPT_BYTES:
            raise OperationSecurityError(f"unsafe {label}: file changed while reading")
        chunks: list[bytes] = []
        remaining = MAX_RECEIPT_BYTES + 1
        while remaining > 0:
            chunk = os.read(fd, min(65_536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
    finally:
        os.close(fd)
    if len(data) > MAX_RECEIPT_BYTES:
        raise OperationSecurityError(f"unsafe {label}: file is too large")
    try:
        payload = json.loads(
            data.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_pairs,
            parse_constant=_reject_nonfinite,
        )
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise OperationStateError(f"malformed {label}") from exc
    if not isinstance(payload, dict):
        raise OperationStateError(f"malformed {label}: expected a JSON object")
    if _contains_nul(payload):
        raise OperationSecurityError(f"unsafe {label}: decoded NUL")
    return payload


def _read_optional_receipt_at(
    directory_fd: int,
    name: str,
    *,
    label: str,
) -> dict[str, object] | None:
    try:
        os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    return _read_receipt_at(directory_fd, name, label=label)


def _request_payload(identity: OperationIdentity) -> dict[str, object]:
    return {
        "schema": REQUEST_SCHEMA,
        "operation_id": operation_id(identity),
        "identity": identity.as_dict(),
    }


def _request_sha256(handle: OperationHandle) -> str:
    """Digest the canonical immutable request, independent of JSON layout."""
    return handle.request_sha256


def _request_digest(identity: OperationIdentity) -> str:
    return hashlib.sha256(_canonical_json(_request_payload(identity))).hexdigest()


def _load_handle_from_fd(
    operation: str,
    *,
    state_root: Path,
    operation_fd: int,
) -> OperationHandle:
    for name, label in (("lease.lock", "lease lock"), ("decision.lock", "decision lock")):
        _validate_regular_at(operation_fd, name, label=label, size_limit=0)
    request = _read_receipt_at(operation_fd, "request.json", label="operation request")
    if set(request) != {"schema", "operation_id", "identity"}:
        raise OperationStateError("operation request has unexpected fields")
    if request.get("schema") != REQUEST_SCHEMA or request.get("operation_id") != operation:
        raise OperationStateError("operation request identity is inconsistent")
    raw_identity = request.get("identity")
    if not isinstance(raw_identity, dict):
        raise OperationStateError("operation request identity is not an object")
    identity = OperationIdentity.from_dict(raw_identity)
    if operation_id(identity) != operation:
        raise OperationStateError("operation directory does not match canonical identity")
    return OperationHandle(
        operation,
        state_root,
        operations_root(state_root=state_root) / operation,
        identity,
        _request_digest(identity),
    )


def _validate_operation_entries_at(
    operation_fd: int,
    *,
    recover: bool,
) -> None:
    """Validate interrupted create-once publishes and optionally recover them.

    The caller must hold ``decision.lock``.  A temp-only inode is before the
    create-once hardlink publish point and is safe to discard.  A target plus
    same-inode temp is after publish and needs only the interrupted unlink.
    Every other pairing is contradictory and fails closed.  Read-only callers
    must pass ``recover=False``: recoverable debris then raises without changing
    bytes or metadata, so dry-run and verification remain observational.
    """
    recovered_target_names: set[str] = set()
    changed = False
    for name in sorted(os.listdir(operation_fd)):
        match = _RECEIPT_TEMP.fullmatch(name)
        if match is None:
            continue
        target = f"{match.group(1)}.json"
        if target in recovered_target_names:
            raise OperationSecurityError(f"multiple interrupted receipt publishes target {target}")
        recovered_target_names.add(target)
        temporary = os.stat(name, dir_fd=operation_fd, follow_symlinks=False)
        if (
            not stat.S_ISREG(temporary.st_mode)
            or temporary.st_uid != os.geteuid()
            or stat.S_IMODE(temporary.st_mode) != 0o600
            or temporary.st_size > MAX_RECEIPT_BYTES
            or temporary.st_nlink not in {1, 2}
        ):
            raise OperationSecurityError(f"unsafe interrupted receipt temporary {name}")
        try:
            published = os.stat(
                target,
                dir_fd=operation_fd,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            if temporary.st_nlink != 1:
                raise OperationSecurityError(
                    f"unpublished receipt temporary {name} has extra links"
                ) from None
        else:
            if (
                not stat.S_ISREG(published.st_mode)
                or published.st_uid != os.geteuid()
                or stat.S_IMODE(published.st_mode) != 0o600
                or published.st_size > MAX_RECEIPT_BYTES
                or (published.st_dev, published.st_ino) != (temporary.st_dev, temporary.st_ino)
                or published.st_nlink != 2
                or temporary.st_nlink != 2
            ):
                raise OperationSecurityError(f"interrupted receipt {name} does not match {target}")
        if recover:
            os.unlink(name, dir_fd=operation_fd)
            changed = True
    if recovered_target_names and not recover:
        raise OperationStateError("interrupted receipt publish requires explicit mutating recovery")
    if changed:
        os.fsync(operation_fd)
    unknown = sorted(set(os.listdir(operation_fd)) - _OPERATION_FILES)
    if unknown:
        raise OperationSecurityError(
            f"unexpected entries in operation directory: {', '.join(unknown)}"
        )


def prepare_operation(
    identity: OperationIdentity,
    *,
    state_root: Path | None = None,
) -> OperationHandle:
    """Create or validate the immutable private record for ``identity``."""
    root = _state_root(state_root)
    operation = operation_id(identity)
    handle = OperationHandle(
        operation,
        root,
        operations_root(state_root=root) / operation,
        identity,
        _request_digest(identity),
    )
    with _open_operation_fd(operation, state_root=root, create=True) as operation_fd:
        decision_fd = _create_private_at(operation_fd, "decision.lock", label="decision lock")
        try:
            fcntl.flock(decision_fd, fcntl.LOCK_EX)
            lease_fd = _create_private_at(operation_fd, "lease.lock", label="lease lock")
            os.close(lease_fd)
            _validate_operation_entries_at(operation_fd, recover=True)
            existing = _read_optional_receipt_at(
                operation_fd,
                "request.json",
                label="operation request",
            )
            expected = _request_payload(identity)
            if existing is None:
                _write_receipt_at(operation_fd, "request.json", expected)
            elif existing != expected:
                raise OperationStateError("operation request does not match canonical identity")
        finally:
            fcntl.flock(decision_fd, fcntl.LOCK_UN)
            os.close(decision_fd)
    return handle


def _load_handle(operation: str, *, state_root: Path | None = None) -> OperationHandle:
    operation = _validate_operation_id(operation)
    root = _state_root(state_root)
    with _open_operation_fd(operation, state_root=root, create=False) as operation_fd:
        decision_fd = _secure_open_at(
            operation_fd,
            "decision.lock",
            flags=os.O_RDWR,
            label="decision lock",
        )
        try:
            fcntl.flock(decision_fd, fcntl.LOCK_EX)
            _validate_operation_entries_at(operation_fd, recover=True)
            return _load_handle_from_fd(
                operation,
                state_root=root,
                operation_fd=operation_fd,
            )
        finally:
            fcntl.flock(decision_fd, fcntl.LOCK_UN)
            os.close(decision_fd)


def _write_operation_receipt(
    handle: OperationHandle,
    name: str,
    payload: Mapping[str, object],
) -> dict[str, object]:
    with _open_operation_fd(
        handle.operation_id,
        state_root=handle.state_root,
        create=False,
    ) as operation_fd:
        decision_fd = _secure_open_at(
            operation_fd,
            "decision.lock",
            flags=os.O_RDWR,
            label="decision lock",
        )
        try:
            fcntl.flock(decision_fd, fcntl.LOCK_EX)
            _validate_operation_entries_at(operation_fd, recover=True)
            current = _load_handle_from_fd(
                handle.operation_id,
                state_root=handle.state_root,
                operation_fd=operation_fd,
            )
            if current.request_sha256 != handle.request_sha256:
                raise OperationStateError("operation request changed before receipt write")
            return _write_receipt_at(operation_fd, name, payload)
        finally:
            fcntl.flock(decision_fd, fcntl.LOCK_UN)
            os.close(decision_fd)


def _write_decided_receipt_at(
    operation_fd: int,
    name: str,
    payload: Mapping[str, object],
) -> dict[str, object]:
    decision_fd = _secure_open_at(
        operation_fd,
        "decision.lock",
        flags=os.O_RDWR,
        label="decision lock",
    )
    try:
        fcntl.flock(decision_fd, fcntl.LOCK_EX)
        _validate_operation_entries_at(operation_fd, recover=True)
        return _write_receipt_at(operation_fd, name, payload)
    finally:
        fcntl.flock(decision_fd, fcntl.LOCK_UN)
        os.close(decision_fd)


def _read_optional_decided_receipt_at(
    operation_fd: int,
    name: str,
    *,
    label: str,
) -> dict[str, object] | None:
    decision_fd = _secure_open_at(
        operation_fd,
        "decision.lock",
        flags=os.O_RDWR,
        label="decision lock",
    )
    try:
        fcntl.flock(decision_fd, fcntl.LOCK_EX)
        _validate_operation_entries_at(operation_fd, recover=True)
        return _read_optional_receipt_at(operation_fd, name, label=label)
    finally:
        fcntl.flock(decision_fd, fcntl.LOCK_UN)
        os.close(decision_fd)


def list_operation_ids(
    *,
    state_root: Path | None = None,
    recover: bool = False,
) -> list[str]:
    """List fully validated operations, optionally repairing publish debris.

    ``recover=False`` is strictly observational.  A mutating reconciliation
    pass may opt into recovery; an unpublished prepare stub containing only
    the two permanent locks is then ignored until preparation is retried.
    """
    if not isinstance(recover, bool):
        raise ValueError("recover must be a boolean")
    root = _state_root(state_root)
    try:
        root.lstat()
    except FileNotFoundError:
        return []
    try:
        with _open_operations_fd(root, create=False) as operation_root_fd:
            result: list[str] = []
            for name in sorted(os.listdir(operation_root_fd)):
                if not _OPERATION_ID.fullmatch(name):
                    raise OperationSecurityError(f"unexpected entry in operation root: {name!r}")
                operation_fd = _open_directory_at(
                    operation_root_fd,
                    name,
                    private=True,
                    label="operation directory",
                    create=False,
                )
                try:
                    loaded = _load_observation_from_fd(
                        name,
                        state_root=root,
                        operation_fd=operation_fd,
                        recover=recover,
                        allow_unpublished=True,
                    )
                finally:
                    os.close(operation_fd)
                if loaded is None:
                    continue
                result.append(name)
            return result
    except OperationStateError as exc:
        if "missing rollout root" in str(exc) or "missing operation root" in str(exc):
            return []
        raise


def _timestamp() -> str:
    return datetime.now(UTC).isoformat()


def _pid_start_ticks(pid: int) -> int | None:
    try:
        raw = Path(f"/proc/{pid}/stat").read_text(encoding="latin-1")
    except OSError:
        return None
    closing = raw.rfind(")")
    if closing < 0:
        return None
    fields = raw[closing + 1 :].split()
    if len(fields) < 20:
        return None
    try:
        return int(fields[19])
    except ValueError:
        return None


def _boot_id() -> str | None:
    try:
        value = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
    except OSError:
        return None
    return value if value and "\x00" not in value else None


def _process_identity(pid: int) -> dict[str, object]:
    return {
        "pid": pid,
        "pid_start_ticks": _pid_start_ticks(pid),
        "boot_id": _boot_id(),
        "platform": sys.platform,
    }


def _validate_process_identity(value: object, *, label: str) -> Mapping[str, object]:
    if not isinstance(value, dict) or set(value) != {
        "pid",
        "pid_start_ticks",
        "boot_id",
        "platform",
    }:
        raise OperationStateError(f"{label} has an invalid process identity")
    pid = value.get("pid")
    ticks = value.get("pid_start_ticks")
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        raise OperationStateError(f"{label} has an invalid pid")
    if ticks is not None and (not isinstance(ticks, int) or isinstance(ticks, bool) or ticks < 0):
        raise OperationStateError(f"{label} has invalid process start ticks")
    _validate_text(f"{label}.boot_id", value.get("boot_id"), optional=True)
    _validate_text(f"{label}.platform", value.get("platform"))
    return value


def _ready_payload(handle: OperationHandle, *, nonce: str) -> dict[str, object]:
    if not _NONCE.fullmatch(nonce):
        raise OperationSecurityError("ready nonce must be 64 lowercase hexadecimal characters")
    return {
        "schema": READY_SCHEMA,
        "operation_id": handle.operation_id,
        "request_sha256": _request_sha256(handle),
        "nonce": nonce,
        "ready_at": _timestamp(),
        "supervisor": _process_identity(os.getpid()),
    }


def _authorization_payload(
    handle: OperationHandle,
    *,
    nonce: str,
    decision: Literal["run", "abort"] = "run",
) -> dict[str, object]:
    if not _NONCE.fullmatch(nonce):
        raise OperationSecurityError(
            "authorization nonce must be 64 lowercase hexadecimal characters"
        )
    return {
        "schema": AUTHORIZATION_SCHEMA,
        "operation_id": handle.operation_id,
        "request_sha256": _request_sha256(handle),
        "nonce": nonce,
        "decision": decision,
        "report_digest_sha256": handle.identity.report_digest_sha256,
        "authorized_at": _timestamp(),
    }


def _activation_payload(
    handle: OperationHandle,
    *,
    nonce: str,
) -> dict[str, object]:
    command_identity = {
        "python_module": "vq",
        "argv": list(handle.identity.argv),
    }
    return {
        "schema": ACTIVATION_SCHEMA,
        "operation_id": handle.operation_id,
        "request_sha256": _request_sha256(handle),
        "nonce": nonce,
        "activated_at": _timestamp(),
        "launch_intent": True,
        "command_sha256": hashlib.sha256(_canonical_json(command_identity)).hexdigest(),
    }


def _result_payload(
    handle: OperationHandle,
    *,
    nonce: str,
    status: str,
    executed: bool,
    returncode: int | None,
    retry_safe: bool,
    child: Mapping[str, object] | None,
    output_sha256: str,
    output_stored_bytes: int,
    output_observed_bytes: int,
    output_truncated: bool,
    duration_seconds: float,
) -> dict[str, object]:
    return {
        "schema": RESULT_SCHEMA,
        "operation_id": handle.operation_id,
        "request_sha256": _request_sha256(handle),
        "nonce": nonce,
        "completed_at": _timestamp(),
        "status": status,
        "executed": executed,
        "returncode": returncode,
        "retry_safe": retry_safe,
        "child": dict(child) if child is not None else None,
        "output_sha256": output_sha256,
        "output_stored_bytes": output_stored_bytes,
        "output_observed_bytes": output_observed_bytes,
        "output_truncated": output_truncated,
        "output_truncation_guidance": (
            "read canonical vq admin logs for complete output" if output_truncated else None
        ),
        "duration_seconds": round(duration_seconds, 6),
    }


def _expect_receipt_fields(
    payload: Mapping[str, object],
    *,
    schema: str,
    fields: set[str],
    operation: str,
    label: str,
) -> None:
    if set(payload) != fields or payload.get("schema") != schema:
        raise OperationStateError(f"{label} has invalid schema or fields")
    if payload.get("operation_id") != operation:
        raise OperationStateError(f"{label} belongs to another operation")


def _validate_request_binding(
    payload: Mapping[str, object],
    handle: OperationHandle,
    *,
    label: str,
) -> None:
    if payload.get("request_sha256") != _request_sha256(handle):
        raise OperationStateError(f"{label} does not bind the immutable request")


def _validate_ready(payload: Mapping[str, object], handle: OperationHandle) -> str:
    _expect_receipt_fields(
        payload,
        schema=READY_SCHEMA,
        fields={
            "schema",
            "operation_id",
            "request_sha256",
            "nonce",
            "ready_at",
            "supervisor",
        },
        operation=handle.operation_id,
        label="ready receipt",
    )
    _validate_request_binding(payload, handle, label="ready receipt")
    nonce = payload.get("nonce")
    if not isinstance(nonce, str) or not _NONCE.fullmatch(nonce):
        raise OperationStateError("ready receipt has an invalid nonce")
    _validate_text("ready receipt timestamp", payload.get("ready_at"))
    _validate_process_identity(payload.get("supervisor"), label="ready receipt")
    return nonce


def _validate_authorization(
    payload: Mapping[str, object],
    handle: OperationHandle,
    *,
    ready_nonce: str | None,
) -> tuple[str, str]:
    _expect_receipt_fields(
        payload,
        schema=AUTHORIZATION_SCHEMA,
        fields={
            "schema",
            "operation_id",
            "request_sha256",
            "nonce",
            "decision",
            "report_digest_sha256",
            "authorized_at",
        },
        operation=handle.operation_id,
        label="authorization receipt",
    )
    _validate_request_binding(payload, handle, label="authorization receipt")
    authorization_nonce = payload.get("nonce")
    if not isinstance(authorization_nonce, str) or not _NONCE.fullmatch(authorization_nonce):
        raise OperationStateError("authorization has an invalid nonce")
    decision = payload.get("decision")
    if decision not in {"run", "abort"}:
        raise OperationStateError("authorization has an invalid decision")
    if ready_nonce is not None and authorization_nonce != ready_nonce:
        raise OperationStateError("authorization nonce does not match ready receipt")
    if decision == "run" and ready_nonce is None:
        raise OperationStateError("run authorization exists without a ready receipt")
    if payload.get("report_digest_sha256") != handle.identity.report_digest_sha256:
        raise OperationStateError("authorization report digest does not match request")
    _validate_text("authorization timestamp", payload.get("authorized_at"))
    return decision, authorization_nonce


def _validate_activation(
    payload: Mapping[str, object],
    handle: OperationHandle,
    *,
    nonce: str,
) -> None:
    _expect_receipt_fields(
        payload,
        schema=ACTIVATION_SCHEMA,
        fields={
            "schema",
            "operation_id",
            "request_sha256",
            "nonce",
            "activated_at",
            "launch_intent",
            "command_sha256",
        },
        operation=handle.operation_id,
        label="activation receipt",
    )
    _validate_request_binding(payload, handle, label="activation receipt")
    if payload.get("nonce") != nonce:
        raise OperationStateError("activation nonce does not match ready receipt")
    _validate_text("activation timestamp", payload.get("activated_at"))
    if payload.get("launch_intent") is not True:
        raise OperationStateError("activation lacks durable launch intent")
    command_sha = payload.get("command_sha256")
    if not isinstance(command_sha, str) or not _REPORT_DIGEST.fullmatch(command_sha):
        raise OperationStateError("activation has an invalid command digest")
    expected = _activation_payload(handle, nonce=nonce)["command_sha256"]
    if command_sha != expected:
        raise OperationStateError("activation command digest does not match request")


def _validate_execution_context_at(
    handle: OperationHandle,
    operation_fd: int,
    context: OperationExecutionContext,
) -> None:
    if context.operation_id != handle.operation_id:
        raise OperationStateError("execution context belongs to another operation")
    if context.request_sha256 != handle.request_sha256:
        raise OperationStateError("execution context request digest does not match")
    ready = _read_optional_receipt_at(
        operation_fd,
        "ready.json",
        label="ready receipt",
    )
    authorization = _read_optional_receipt_at(
        operation_fd,
        "authorization.json",
        label="authorization receipt",
    )
    activation = _read_optional_receipt_at(
        operation_fd,
        "activation.json",
        label="activation receipt",
    )
    if ready is None or authorization is None or activation is None:
        raise OperationStateError(
            "detached scheduler binding requires an activated outer operation"
        )
    nonce = _validate_ready(ready, handle)
    decision, authorization_nonce = _validate_authorization(
        authorization,
        handle,
        ready_nonce=nonce,
    )
    if decision != "run" or authorization_nonce != nonce or context.nonce != nonce:
        raise OperationStateError(
            "execution context does not match the authorized outer operation"
        )
    _validate_activation(activation, handle, nonce=nonce)


def validate_live_execution_context(
    context: OperationExecutionContext,
    *,
    state_root: Path | None = None,
) -> OperationIdentity:
    """Prove that an exported context still belongs to its live action.

    This is the read-only pre-mutation gate for nested operations.  It reloads
    the owner-only request and authorization chain, rejects terminal actions,
    and requires the recorder's lifetime lease to remain held.  Immutable
    binding observation deliberately uses a separate path so terminal evidence
    remains readable after that lease is released.
    """
    if not isinstance(context, OperationExecutionContext):
        raise OperationSecurityError("invalid fleet operation execution context")
    root = _state_root(state_root)
    with _open_operation_fd(
        context.operation_id,
        state_root=root,
        create=False,
    ) as operation_fd:
        decision_fd = _secure_open_at(
            operation_fd,
            "decision.lock",
            flags=os.O_RDWR,
            label="decision lock",
        )
        try:
            fcntl.flock(decision_fd, fcntl.LOCK_SH)
            _validate_operation_entries_at(operation_fd, recover=False)
            handle = _load_handle_from_fd(
                context.operation_id,
                state_root=root,
                operation_fd=operation_fd,
            )
            _validate_execution_context_at(handle, operation_fd, context)
            if _read_optional_receipt_at(
                operation_fd,
                "result.json",
                label="operation result",
            ) is not None:
                raise OperationStateError("fleet operation is already terminal")
            if not _lease_busy_at(operation_fd):
                raise OperationStateError(
                    "fleet operation no longer owns its lifetime lease"
                )
            return handle.identity
        finally:
            fcntl.flock(decision_fd, fcntl.LOCK_UN)
            os.close(decision_fd)


def bind_detached_scheduler_command(
    context: OperationExecutionContext,
    *,
    target: str,
    program: str,
    mode: str,
    protocol: str,
    run_id: str,
    bound_at: str,
    remote_run_dir: str,
    command_sha256: str,
    remote_request_sha256: str,
    state_root: Path | None = None,
) -> tuple[DetachedSchedulerCommandBinding, bool]:
    """Create or verify one local fixed-host command binding before SSH.

    The receipt is protected by the outer operation's decision lock.  An
    existing exact receipt is observation authority, never permission to
    launch the remote command again; callers use the boolean to distinguish
    that fail-closed adoption case from the sole create-and-launch path.
    """
    if not isinstance(context, OperationExecutionContext):
        raise OperationSecurityError("invalid fleet operation execution context")
    desired = DetachedSchedulerCommandBinding(
        operation_id=context.operation_id,
        request_sha256=context.request_sha256,
        nonce=context.nonce,
        target=target,
        program=program,
        mode=mode,
        protocol=protocol,
        run_id=run_id,
        bound_at=bound_at,
        remote_run_dir=remote_run_dir,
        command_sha256=command_sha256,
        remote_request_sha256=remote_request_sha256,
    )
    root = _state_root(state_root)
    with _open_operation_fd(
        context.operation_id,
        state_root=root,
        create=False,
    ) as operation_fd:
        decision_fd = _secure_open_at(
            operation_fd,
            "decision.lock",
            flags=os.O_RDWR,
            label="decision lock",
        )
        try:
            fcntl.flock(decision_fd, fcntl.LOCK_EX)
            _validate_operation_entries_at(operation_fd, recover=True)
            handle = _load_handle_from_fd(
                context.operation_id,
                state_root=root,
                operation_fd=operation_fd,
            )
            _validate_execution_context_at(handle, operation_fd, context)
            existing_payload = _read_optional_receipt_at(
                operation_fd,
                "scheduler-command.json",
                label="detached scheduler binding",
            )
            if existing_payload is not None:
                existing = DetachedSchedulerCommandBinding.from_dict(existing_payload)
                if existing != desired:
                    raise OperationStateError(
                        "detached scheduler binding already exists with another identity"
                    )
                return existing, False
            if _read_optional_receipt_at(
                operation_fd,
                "result.json",
                label="operation result",
            ) is not None:
                raise OperationStateError(
                    "detached scheduler binding cannot be created after terminal result"
                )
            if not _lease_busy_at(operation_fd):
                raise OperationStateError(
                    "detached scheduler binding requires the active outer lifetime lease"
                )
            _write_receipt_at(
                operation_fd,
                "scheduler-command.json",
                desired.as_dict(),
            )
            return desired, True
        finally:
            fcntl.flock(decision_fd, fcntl.LOCK_UN)
            os.close(decision_fd)


def read_detached_scheduler_binding(
    operation: str,
    *,
    state_root: Path | None = None,
) -> DetachedSchedulerCommandBinding | None:
    """Read and verify the optional local binding without recovery writes."""
    operation = _validate_operation_id(operation)
    root = _state_root(state_root)
    with _open_operation_fd(operation, state_root=root, create=False) as operation_fd:
        decision_fd = _secure_open_at(
            operation_fd,
            "decision.lock",
            flags=os.O_RDWR,
            label="decision lock",
        )
        try:
            fcntl.flock(decision_fd, fcntl.LOCK_SH)
            _validate_operation_entries_at(operation_fd, recover=False)
            handle = _load_handle_from_fd(
                operation,
                state_root=root,
                operation_fd=operation_fd,
            )
            payload = _read_optional_receipt_at(
                operation_fd,
                "scheduler-command.json",
                label="detached scheduler binding",
            )
            if payload is None:
                return None
            binding = DetachedSchedulerCommandBinding.from_dict(payload)
            _validate_execution_context_at(
                handle,
                operation_fd,
                OperationExecutionContext(
                    operation_id=binding.operation_id,
                    request_sha256=binding.request_sha256,
                    nonce=binding.nonce,
                ),
            )
            return binding
        finally:
            fcntl.flock(decision_fd, fcntl.LOCK_UN)
            os.close(decision_fd)


def _output_digest_at(operation_fd: int) -> tuple[str, int]:
    _validate_regular_at(
        operation_fd,
        "output.log",
        label="operation output",
        size_limit=MAX_OUTPUT_BYTES,
    )
    fd = _secure_open_at(
        operation_fd,
        "output.log",
        flags=os.O_RDONLY,
        label="operation output",
    )
    digest = hashlib.sha256()
    size = 0
    try:
        while True:
            chunk = os.read(fd, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
            if size > MAX_OUTPUT_BYTES:
                raise OperationSecurityError("operation output exceeds bounded spool")
    finally:
        os.close(fd)
    return digest.hexdigest(), size


def _validate_result(
    payload: Mapping[str, object],
    handle: OperationHandle,
    *,
    nonce: str,
    activation: Mapping[str, object] | None,
    authorization_decision: str,
    operation_fd: int,
) -> bool:
    _expect_receipt_fields(
        payload,
        schema=RESULT_SCHEMA,
        fields={
            "schema",
            "operation_id",
            "request_sha256",
            "nonce",
            "completed_at",
            "status",
            "executed",
            "returncode",
            "retry_safe",
            "child",
            "output_sha256",
            "output_stored_bytes",
            "output_observed_bytes",
            "output_truncated",
            "output_truncation_guidance",
            "duration_seconds",
        },
        operation=handle.operation_id,
        label="result receipt",
    )
    _validate_request_binding(payload, handle, label="result receipt")
    if payload.get("nonce") != nonce:
        raise OperationStateError("result nonce does not match ready receipt")
    _validate_text("result timestamp", payload.get("completed_at"))
    status_value = payload.get("status")
    executed = payload.get("executed")
    returncode = payload.get("returncode")
    retry_safe = payload.get("retry_safe")
    child = payload.get("child")
    if status_value not in {"success", "failed", "aborted"}:
        raise OperationStateError("result has an invalid status")
    if not isinstance(executed, bool) or not isinstance(retry_safe, bool):
        raise OperationStateError("result has invalid boolean fields")
    if executed:
        if activation is None or authorization_decision != "run":
            raise OperationStateError("executed result lacks an activation receipt")
        if not isinstance(returncode, int) or isinstance(returncode, bool):
            raise OperationStateError("executed result lacks an integer returncode")
        if retry_safe:
            raise OperationStateError("an executed mutating child cannot be retry-safe")
        expected_status = "success" if returncode == 0 else "failed"
        if status_value != expected_status:
            raise OperationStateError("result status disagrees with child returncode")
        _validate_process_identity(child, label="result receipt")
    elif status_value != "aborted" or returncode is not None or not retry_safe or child is not None:
        raise OperationStateError("non-executed result is not a safe terminal decision")
    elif status_value == "aborted" and (
        activation is not None or authorization_decision != "abort"
    ):
        raise OperationStateError("aborted result contradicts the durable decision")
    output_sha = payload.get("output_sha256")
    output_stored_bytes = payload.get("output_stored_bytes")
    output_observed_bytes = payload.get("output_observed_bytes")
    output_truncated = payload.get("output_truncated")
    truncation_guidance = payload.get("output_truncation_guidance")
    duration = payload.get("duration_seconds")
    if not isinstance(output_sha, str) or not _REPORT_DIGEST.fullmatch(output_sha):
        raise OperationStateError("result has an invalid output digest")
    if (
        not isinstance(output_stored_bytes, int)
        or isinstance(output_stored_bytes, bool)
        or not 0 <= output_stored_bytes <= MAX_OUTPUT_BYTES
        or not isinstance(output_observed_bytes, int)
        or isinstance(output_observed_bytes, bool)
        or output_observed_bytes < output_stored_bytes
        or not isinstance(output_truncated, bool)
    ):
        raise OperationStateError("result has invalid bounded-output accounting")
    if output_truncated != (output_observed_bytes > output_stored_bytes):
        raise OperationStateError("result output truncation contradicts byte accounting")
    expected_guidance = (
        "read canonical vq admin logs for complete output" if output_truncated else None
    )
    if truncation_guidance != expected_guidance:
        raise OperationStateError("result has invalid output truncation guidance")
    if (
        not isinstance(duration, (int, float))
        or isinstance(duration, bool)
        or not math.isfinite(duration)
        or duration < 0
    ):
        raise OperationStateError("result has an invalid duration")
    actual_sha, actual_size = _output_digest_at(operation_fd)
    if (actual_sha, actual_size) != (output_sha, output_stored_bytes):
        raise OperationStateError("operation output does not match terminal result")
    return retry_safe


def _lease_busy_at(operation_fd: int) -> bool:
    fd = _secure_open_at(operation_fd, "lease.lock", flags=os.O_RDWR, label="lease lock")
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno not in {errno.EACCES, errno.EAGAIN}:
                raise
            return True
        fcntl.flock(fd, fcntl.LOCK_UN)
        return False
    finally:
        os.close(fd)


def _observe_locked_from_fd(
    handle: OperationHandle,
    operation_fd: int,
) -> OperationObservation:
    request = _read_receipt_at(operation_fd, "request.json", label="operation request")
    ready = _read_optional_receipt_at(operation_fd, "ready.json", label="ready receipt")
    authorization = _read_optional_receipt_at(
        operation_fd,
        "authorization.json",
        label="authorization receipt",
    )
    activation = _read_optional_receipt_at(
        operation_fd,
        "activation.json",
        label="activation receipt",
    )
    result = _read_optional_receipt_at(operation_fd, "result.json", label="result receipt")
    try:
        _validate_regular_at(
            operation_fd,
            "output.log",
            label="operation output",
            size_limit=MAX_OUTPUT_BYTES,
        )
    except OperationStateError:
        output_exists = False
    else:
        output_exists = True

    ready_nonce: str | None = None
    if ready is not None:
        ready_nonce = _validate_ready(ready, handle)
    authorization_decision: str | None = None
    authorization_nonce: str | None = None
    if authorization is not None:
        authorization_decision, authorization_nonce = _validate_authorization(
            authorization,
            handle,
            ready_nonce=ready_nonce,
        )
    if activation is not None:
        if authorization_decision != "run" or ready_nonce is None:
            raise OperationStateError("activation exists without run authorization")
        _validate_activation(activation, handle, nonce=ready_nonce)
    terminal_retry_safe: bool | None = None
    if result is not None:
        if authorization_decision is None or authorization_nonce is None:
            raise OperationStateError("result exists without authorization")
        terminal_retry_safe = _validate_result(
            result,
            handle,
            nonce=authorization_nonce,
            activation=activation,
            authorization_decision=authorization_decision,
            operation_fd=operation_fd,
        )
    if ready is not None and not output_exists:
        raise OperationStateError("ready receipt exists without operation output")
    if result is not None and not output_exists:
        raise OperationStateError("terminal result exists without operation output")

    busy = _lease_busy_at(operation_fd)
    if result is not None:
        assert terminal_retry_safe is not None
        state: ObservationState = "completed"
        retry_safe = terminal_retry_safe
    elif busy and authorization_decision == "run":
        state = "running-authorized"
        retry_safe = False
    elif busy and (ready is not None or authorization_decision == "abort"):
        state = "running-pre-authorization"
        retry_safe = False
    elif busy:
        state = "starting"
        retry_safe = False
    elif authorization_decision == "run" and activation is None:
        # Both supervisor generations publish durable launch intent before
        # calling Popen.  A free lease with no activation is therefore safe
        # to resume as this same immutable operation, never as a new attempt.
        state = "authorized-unactivated"
        retry_safe = False
    elif authorization_decision == "run":
        state = "outcome-unknown"
        retry_safe = False
    elif ready is not None or output_exists or authorization_decision == "abort":
        state = "abandoned-pre-authorization"
        retry_safe = True
    else:
        state = "prepared"
        retry_safe = True
    return OperationObservation(
        operation_id=handle.operation_id,
        identity=handle.identity,
        request_sha256=handle.request_sha256,
        state=state,
        retry_safe=retry_safe,
        lease_busy=busy,
        request=request,
        ready=ready,
        authorization=authorization,
        activation=activation,
        result=result,
    )


def _load_observation_from_fd(
    operation: str,
    *,
    state_root: Path,
    operation_fd: int,
    recover: bool,
    allow_unpublished: bool = False,
) -> tuple[OperationHandle, OperationObservation] | None:
    try:
        decision_fd = _secure_open_at(
            operation_fd,
            "decision.lock",
            flags=os.O_RDWR,
            label="decision lock",
        )
    except OperationStateError as exc:
        entries = set(os.listdir(operation_fd))
        if entries not in (set(), {"decision.lock"}):
            raise OperationStateError(
                "incomplete operation lacks its first permanent decision lock"
            ) from exc
        if not recover:
            raise OperationStateError(
                "incomplete operation prepare requires explicit mutating recovery"
            ) from exc
        decision_fd = _create_private_at(
            operation_fd,
            "decision.lock",
            label="decision lock",
        )
    try:
        fcntl.flock(decision_fd, fcntl.LOCK_EX)
        _validate_regular_at(
            operation_fd,
            "decision.lock",
            label="decision lock",
            size_limit=0,
        )
        _validate_operation_entries_at(operation_fd, recover=recover)
        entries = set(os.listdir(operation_fd))
        if entries == {"decision.lock"}:
            if not recover:
                raise OperationStateError(
                    "incomplete operation prepare requires explicit mutating recovery"
                )
            lease_fd = _create_private_at(
                operation_fd,
                "lease.lock",
                label="lease lock",
            )
            os.close(lease_fd)
            entries.add("lease.lock")
        if allow_unpublished and entries == {"decision.lock", "lease.lock"}:
            _validate_regular_at(
                operation_fd,
                "lease.lock",
                label="lease lock",
                size_limit=0,
            )
            if _lease_busy_at(operation_fd):
                raise OperationStateError(
                    "unpublished operation unexpectedly owns the lifetime lease"
                )
            return None
        handle = _load_handle_from_fd(
            operation,
            state_root=state_root,
            operation_fd=operation_fd,
        )
        return handle, _observe_locked_from_fd(handle, operation_fd)
    finally:
        fcntl.flock(decision_fd, fcntl.LOCK_UN)
        os.close(decision_fd)


def observe_operation(
    operation: str,
    *,
    state_root: Path | None = None,
    recover: bool = False,
) -> OperationObservation:
    """Reconcile receipts with the lease, optionally repairing publish debris."""
    if not isinstance(recover, bool):
        raise ValueError("recover must be a boolean")
    operation = _validate_operation_id(operation)
    root = _state_root(state_root)
    with _open_operation_fd(operation, state_root=root, create=False) as operation_fd:
        loaded = _load_observation_from_fd(
            operation,
            state_root=root,
            operation_fd=operation_fd,
            recover=recover,
        )
        assert loaded is not None
        return loaded[1]


def read_output_since(
    operation: str,
    offset: int,
    *,
    state_root: Path | None = None,
    max_bytes: int = 64 * 1024,
) -> OperationOutputChunk:
    """Read a bounded append-only live chunk without emitting it anywhere."""
    if (
        not isinstance(offset, int)
        or isinstance(offset, bool)
        or not 0 <= offset <= MAX_OUTPUT_BYTES
    ):
        raise ValueError(f"offset must be between 0 and {MAX_OUTPUT_BYTES}")
    if (
        not isinstance(max_bytes, int)
        or isinstance(max_bytes, bool)
        or not 1 <= max_bytes <= MAX_OUTPUT_TAIL_BYTES
    ):
        raise ValueError(f"max_bytes must be between 1 and {MAX_OUTPUT_TAIL_BYTES}")
    operation = _validate_operation_id(operation)
    root = _state_root(state_root)
    with _open_operation_fd(operation, state_root=root, create=False) as operation_fd:
        loaded = _load_observation_from_fd(
            operation,
            state_root=root,
            operation_fd=operation_fd,
            recover=False,
        )
        assert loaded is not None
        before = _validate_regular_at(
            operation_fd,
            "output.log",
            label="operation output",
            size_limit=MAX_OUTPUT_BYTES,
        )
        if offset > before.st_size:
            raise OperationStateError("live output offset is beyond the append-only spool")
        fd = _secure_open_at(
            operation_fd,
            "output.log",
            flags=os.O_RDONLY,
            label="operation output",
        )
        try:
            data = os.pread(fd, max_bytes, offset)
            after = os.fstat(fd)
        finally:
            os.close(fd)
        if (
            after.st_size < before.st_size
            or after.st_size < offset + len(data)
            or after.st_size > MAX_OUTPUT_BYTES
        ):
            raise OperationStateError("live output violated append-only bounds")
        next_offset = offset + len(data)
        return OperationOutputChunk(
            data=data,
            offset=offset,
            next_offset=next_offset,
            stored_bytes=after.st_size,
            at_end=next_offset >= after.st_size,
            spool_full=after.st_size == MAX_OUTPUT_BYTES,
        )


def read_output_tail(
    operation: str,
    *,
    state_root: Path | None = None,
    max_bytes: int = 64 * 1024,
) -> OperationOutputTail:
    """Return a bounded tail only after streaming full digest verification."""
    if (
        not isinstance(max_bytes, int)
        or isinstance(max_bytes, bool)
        or not 1 <= max_bytes <= MAX_OUTPUT_TAIL_BYTES
    ):
        raise ValueError(f"max_bytes must be between 1 and {MAX_OUTPUT_TAIL_BYTES}")
    operation = _validate_operation_id(operation)
    root = _state_root(state_root)
    with _open_operation_fd(operation, state_root=root, create=False) as operation_fd:
        loaded = _load_observation_from_fd(
            operation,
            state_root=root,
            operation_fd=operation_fd,
            recover=False,
        )
        assert loaded is not None
        _, observed = loaded
        if observed.state != "completed" or observed.result is None:
            raise OperationStateError("operation output is not terminal and verified")
        fd = _secure_open_at(
            operation_fd,
            "output.log",
            flags=os.O_RDONLY,
            label="operation output",
        )
        digest = hashlib.sha256()
        total = 0
        tail = bytearray()
        try:
            while True:
                chunk = os.read(fd, 1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
                total += len(chunk)
                tail.extend(chunk)
                if len(tail) > max_bytes:
                    del tail[:-max_bytes]
        finally:
            os.close(fd)
        output_sha = digest.hexdigest()
        if (
            observed.result.get("output_sha256") != output_sha
            or observed.result.get("output_stored_bytes") != total
        ):
            raise OperationStateError("operation output changed during bounded read")
        raw = bytes(tail)
        observed_bytes = observed.result.get("output_observed_bytes")
        output_truncated = observed.result.get("output_truncated")
        assert isinstance(observed_bytes, int)
        assert isinstance(output_truncated, bool)
        return OperationOutputTail(
            text=raw.decode("utf-8", "replace"),
            truncated=output_truncated or total > len(raw),
            bytes_read=len(raw),
            stored_bytes=total,
            total_bytes=observed_bytes,
            output_sha256=output_sha,
        )


def wait_for_ready(
    operation: str,
    *,
    state_root: Path | None = None,
    timeout: float = DEFAULT_AUTHORIZATION_TIMEOUT_SECONDS,
    poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS,
) -> OperationObservation:
    """Wait until a live supervisor publishes its ready nonce."""
    if (
        type(timeout) not in {int, float}
        or not math.isfinite(timeout)
        or timeout < 0
        or type(poll_interval) not in {int, float}
        or not math.isfinite(poll_interval)
        or poll_interval <= 0
    ):
        raise ValueError("timeout must be non-negative and poll_interval positive")
    deadline = time.monotonic() + timeout
    while True:
        observed = observe_operation(
            operation,
            state_root=state_root,
            recover=True,
        )
        if observed.ready is not None and observed.lease_busy:
            return observed
        if observed.state in {
            "authorized-unactivated",
            "abandoned-pre-authorization",
            "outcome-unknown",
            "completed",
        }:
            raise OperationStateError(
                f"operation reached {observed.state} before a live ready receipt"
            )
        if time.monotonic() >= deadline:
            raise TimeoutError("timed out waiting for fleet operation supervisor")
        time.sleep(poll_interval)


def authorize_operation(
    operation: str,
    *,
    expected_report_digest_sha256: str,
    state_root: Path | None = None,
) -> dict[str, object]:
    """Commit one live ready supervisor after external report revalidation."""
    if not isinstance(expected_report_digest_sha256, str) or not _REPORT_DIGEST.fullmatch(
        expected_report_digest_sha256
    ):
        raise OperationStateError("expected report digest is not canonical SHA-256")
    operation = _validate_operation_id(operation)
    root = _state_root(state_root)
    with _open_operation_fd(operation, state_root=root, create=False) as operation_fd:
        decision_fd = _secure_open_at(
            operation_fd,
            "decision.lock",
            flags=os.O_RDWR,
            label="decision lock",
        )
        try:
            fcntl.flock(decision_fd, fcntl.LOCK_EX)
            _validate_operation_entries_at(operation_fd, recover=True)
            handle = _load_handle_from_fd(
                operation,
                state_root=root,
                operation_fd=operation_fd,
            )
            if handle.identity.report_digest_sha256 != expected_report_digest_sha256:
                raise OperationStateError("accepted report changed before operation authorization")
            existing = _read_optional_receipt_at(
                operation_fd,
                "authorization.json",
                label="authorization receipt",
            )
            ready = _read_optional_receipt_at(
                operation_fd,
                "ready.json",
                label="ready receipt",
            )
            if ready is None:
                raise OperationStateError("supervisor has not published a ready receipt")
            nonce = _validate_ready(ready, handle)
            if existing is not None:
                decision, _ = _validate_authorization(
                    existing,
                    handle,
                    ready_nonce=nonce,
                )
                if decision != "run":
                    raise OperationStateError("operation was permanently aborted")
                return existing
            if not _lease_busy_at(operation_fd):
                raise OperationStateError("ready supervisor no longer owns the lifetime lease")
            payload = _authorization_payload(handle, nonce=nonce, decision="run")
            return _write_receipt_at(operation_fd, "authorization.json", payload)
        finally:
            fcntl.flock(decision_fd, fcntl.LOCK_UN)
            os.close(decision_fd)


def _create_output_at(operation_fd: int) -> Any:
    try:
        fd = os.open(
            "output.log",
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
            0o600,
            dir_fd=operation_fd,
        )
    except FileExistsError as exc:
        _validate_regular_at(
            operation_fd,
            "output.log",
            label="existing operation output",
            size_limit=MAX_OUTPUT_BYTES,
        )
        raise OperationStateError("operation output already exists; use a new attempt") from exc
    try:
        if os.get_inheritable(fd):
            os.set_inheritable(fd, False)
        os.fsync(fd)
        os.fsync(operation_fd)
    except Exception:
        os.close(fd)
        raise
    return os.fdopen(fd, "wb", buffering=0)


def _ensure_empty_output_at(operation_fd: int) -> None:
    try:
        info = _validate_regular_at(
            operation_fd,
            "output.log",
            label="operation output",
            size_limit=0,
        )
    except OperationStateError:
        with _create_output_at(operation_fd):
            pass
        return
    if info.st_size != 0:
        raise OperationStateError("non-executed operation has non-empty output")


def _write_aborted_result_at(
    handle: OperationHandle,
    operation_fd: int,
    *,
    nonce: str,
    duration_seconds: float,
) -> dict[str, object]:
    existing = _read_optional_receipt_at(
        operation_fd,
        "result.json",
        label="result receipt",
    )
    if existing is not None:
        _validate_result(
            existing,
            handle,
            nonce=nonce,
            activation=None,
            authorization_decision="abort",
            operation_fd=operation_fd,
        )
        return existing
    if (
        _read_optional_receipt_at(
            operation_fd,
            "activation.json",
            label="activation receipt",
        )
        is not None
    ):
        raise OperationStateError("cannot abort after durable launch intent")
    _ensure_empty_output_at(operation_fd)
    output_sha, output_bytes = _output_digest_at(operation_fd)
    result = _result_payload(
        handle,
        nonce=nonce,
        status="aborted",
        executed=False,
        returncode=None,
        retry_safe=True,
        child=None,
        output_sha256=output_sha,
        output_stored_bytes=output_bytes,
        output_observed_bytes=output_bytes,
        output_truncated=False,
        duration_seconds=duration_seconds,
    )
    return _write_receipt_at(operation_fd, "result.json", result)


def _write_aborted_result_decided_at(
    handle: OperationHandle,
    operation_fd: int,
    *,
    nonce: str,
    duration_seconds: float,
) -> dict[str, object]:
    decision_fd = _secure_open_at(
        operation_fd,
        "decision.lock",
        flags=os.O_RDWR,
        label="decision lock",
    )
    try:
        fcntl.flock(decision_fd, fcntl.LOCK_EX)
        _validate_operation_entries_at(operation_fd, recover=True)
        return _write_aborted_result_at(
            handle,
            operation_fd,
            nonce=nonce,
            duration_seconds=duration_seconds,
        )
    finally:
        fcntl.flock(decision_fd, fcntl.LOCK_UN)
        os.close(decision_fd)


def abort_operation(
    operation: str,
    *,
    state_root: Path | None = None,
    expected_request_sha256: str,
) -> dict[str, object]:
    """Permanently cancel an obsolete operation without permitting launch."""
    operation = _validate_operation_id(operation)
    if not isinstance(expected_request_sha256, str) or not _REPORT_DIGEST.fullmatch(
        expected_request_sha256
    ):
        raise OperationStateError("expected request digest is not canonical SHA-256")
    root = _state_root(state_root)
    with _open_operation_fd(operation, state_root=root, create=False) as operation_fd:
        decision_fd = _secure_open_at(
            operation_fd,
            "decision.lock",
            flags=os.O_RDWR,
            label="decision lock",
        )
        try:
            fcntl.flock(decision_fd, fcntl.LOCK_EX)
            _validate_operation_entries_at(operation_fd, recover=True)
            handle = _load_handle_from_fd(
                operation,
                state_root=root,
                operation_fd=operation_fd,
            )
            if handle.request_sha256 != expected_request_sha256:
                raise OperationStateError("operation request changed before abort")
            ready = _read_optional_receipt_at(
                operation_fd,
                "ready.json",
                label="ready receipt",
            )
            ready_nonce = _validate_ready(ready, handle) if ready is not None else None
            existing = _read_optional_receipt_at(
                operation_fd,
                "authorization.json",
                label="authorization receipt",
            )
            if existing is not None:
                decision, nonce = _validate_authorization(
                    existing,
                    handle,
                    ready_nonce=ready_nonce,
                )
                if decision != "abort":
                    raise OperationStateError("operation already has run authorization")
                payload = existing
            else:
                nonce = ready_nonce or secrets.token_hex(32)
                payload = _write_receipt_at(
                    operation_fd,
                    "authorization.json",
                    _authorization_payload(handle, nonce=nonce, decision="abort"),
                )
            if not _lease_busy_at(operation_fd):
                _write_aborted_result_at(
                    handle,
                    operation_fd,
                    nonce=nonce,
                    duration_seconds=0.0,
                )
            return payload
        finally:
            fcntl.flock(decision_fd, fcntl.LOCK_UN)
            os.close(decision_fd)


def _wait_for_authorization(
    handle: OperationHandle,
    *,
    operation_fd: int,
    nonce: str,
    timeout: float,
    poll_interval: float,
) -> Mapping[str, object]:
    deadline = time.monotonic() + timeout
    while True:
        authorization = _read_optional_decided_receipt_at(
            operation_fd,
            "authorization.json",
            label="authorization receipt",
        )
        if authorization is not None:
            _validate_authorization(authorization, handle, ready_nonce=nonce)
            return authorization
        if time.monotonic() >= deadline:
            # Run, external abort, and timeout-abort are competing immutable
            # decisions.  The permanent lock and create-once receipt ensure
            # exactly one can win.
            decision_fd = _secure_open_at(
                operation_fd,
                "decision.lock",
                flags=os.O_RDWR,
                label="decision lock",
            )
            try:
                fcntl.flock(decision_fd, fcntl.LOCK_EX)
                _validate_operation_entries_at(operation_fd, recover=True)
                authorization = _read_optional_receipt_at(
                    operation_fd,
                    "authorization.json",
                    label="authorization receipt",
                )
                if authorization is not None:
                    _validate_authorization(authorization, handle, ready_nonce=nonce)
                    return authorization
                return _write_receipt_at(
                    operation_fd,
                    "authorization.json",
                    _authorization_payload(handle, nonce=nonce, decision="abort"),
                )
            finally:
                fcntl.flock(decision_fd, fcntl.LOCK_UN)
                os.close(decision_fd)
        time.sleep(poll_interval)


def _supervisor_exit_code(returncode: int) -> int:
    if returncode < 0:
        return 128 + min(abs(returncode), 127)
    return min(returncode, 255)


def _drain_child_output(
    child: subprocess.Popen[bytes],
    output: Any,
) -> tuple[int, int, bool]:
    """Drain without backpressure while storing at most ``MAX_OUTPUT_BYTES``."""
    pipe = child.stdout
    if pipe is None:
        raise OperationStateError("supervised child lacks a captured output pipe")
    observed = 0
    stored = 0
    try:
        while True:
            try:
                chunk = os.read(pipe.fileno(), 64 * 1024)
            except InterruptedError:
                continue
            if not chunk:
                break
            observed += len(chunk)
            remaining = MAX_OUTPUT_BYTES - stored
            if remaining > 0:
                kept = chunk[:remaining]
                output.write(kept)
                stored += len(kept)
    finally:
        pipe.close()
    return observed, stored, observed > stored


def _open_empty_output_at(operation_fd: int) -> Any:
    """Open the prepared spool for its sole append-only terminal writer."""
    expected = _validate_regular_at(
        operation_fd,
        "output.log",
        label="operation output",
        size_limit=0,
    )
    if expected.st_size != 0:
        raise OperationStateError("operation output is not empty before activation")
    fd = _secure_open_at(
        operation_fd,
        "output.log",
        flags=os.O_WRONLY | os.O_APPEND,
        label="operation output",
    )
    actual = os.fstat(fd)
    if actual.st_size != 0:
        os.close(fd)
        raise OperationStateError("operation output changed before activation")
    return os.fdopen(fd, "wb", buffering=0)


def _claim_recorder_lease_at(operation_fd: int, lease_fd: int) -> None:
    """Validate and retain the supervisor's inherited lifetime lock.

    ``pass_fds`` preserves the same open file description across fork/exec on
    the supported POSIX hosts.  Closing the supervisor's copy without
    ``LOCK_UN`` therefore transfers the already-held lock without a gap.
    Re-taking the lock here is idempotent for that shared description and
    fails closed if an unrelated descriptor races the real lease owner.
    """
    if not isinstance(lease_fd, int) or isinstance(lease_fd, bool) or lease_fd < 3:
        raise OperationSecurityError("recorder lease fd must be an inherited descriptor")
    expected = _validate_regular_at(
        operation_fd,
        "lease.lock",
        label="lease lock",
        size_limit=0,
    )
    try:
        actual = os.fstat(lease_fd)
    except OSError as exc:
        raise OperationSecurityError("recorder lease fd is not open") from exc
    if (
        (actual.st_dev, actual.st_ino) != (expected.st_dev, expected.st_ino)
        or not stat.S_ISREG(actual.st_mode)
        or stat.S_IMODE(actual.st_mode) != 0o600
        or actual.st_uid != os.geteuid()
        or actual.st_nlink != 1
    ):
        raise OperationSecurityError("recorder lease fd does not match the operation")
    try:
        fcntl.flock(lease_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        if exc.errno not in {errno.EACCES, errno.EAGAIN}:
            raise
        raise OperationBusyError("another process owns the lifetime lease") from exc
    if os.get_inheritable(lease_fd):
        os.set_inheritable(lease_fd, False)


def _execute_activated_action(
    handle: OperationHandle,
    *,
    operation_fd: int,
    nonce: str,
    output: Any,
    started: float,
    child_popen: ChildPopen,
    lifecycle_handoff: str | None = None,
) -> int:
    """Launch, drain, wait, and publish one already-authorized action."""
    command = [sys.executable, "-m", "vq", *handle.identity.argv]
    with output:
        lifecycle_fds = _lifecycle_handoff_fds(
            lifecycle_handoff,
            expected_resources=handle.identity.lifecycle_resources,
            expected_rollout_id=handle.identity.rollout_id,
            expected_rollout_lock_path=handle.identity.rollout_lock_path,
        )
        child_kwargs: dict[str, object] = {
            "stdin": subprocess.DEVNULL,
            "stdout": subprocess.PIPE,
            "stderr": subprocess.STDOUT,
            "close_fds": True,
            "env": _activated_child_environment(
                handle,
                nonce=nonce,
                lifecycle_handoff=lifecycle_handoff,
            ),
        }
        if lifecycle_fds and os.name == "posix":
            child_kwargs["pass_fds"] = lifecycle_fds
        child = child_popen(
            command,
            **child_kwargs,
        )
        child_identity = _process_identity(child.pid)
        observed_bytes, stored_bytes, output_truncated = _drain_child_output(
            child,
            output,
        )
        returncode = child.wait()
        output.flush()
        os.fsync(output.fileno())
    output_sha, output_bytes = _output_digest_at(operation_fd)
    if output_bytes != stored_bytes:
        raise OperationStateError("bounded output spool length changed unexpectedly")
    result = _result_payload(
        handle,
        nonce=nonce,
        status="success" if returncode == 0 else "failed",
        executed=True,
        returncode=returncode,
        retry_safe=False,
        child=child_identity,
        output_sha256=output_sha,
        output_stored_bytes=stored_bytes,
        output_observed_bytes=observed_bytes,
        output_truncated=output_truncated,
        duration_seconds=time.monotonic() - started,
    )
    _write_decided_receipt_at(operation_fd, "result.json", result)
    return _supervisor_exit_code(returncode)


def run_recorder(
    operation: str,
    *,
    lease_fd: int,
    state_root: Path | None = None,
    child_popen: ChildPopen = subprocess.Popen,
    lifecycle_handoff: str | None = None,
) -> int:
    """Execute one authorized action while owning the transferred lease.

    This process is started in a fresh session.  It is the sole publisher of
    durable launch intent and the terminal result, so the earlier detached
    supervisor may disappear without orphaning an activated action.
    """
    if not isinstance(lease_fd, int) or isinstance(lease_fd, bool) or lease_fd < 3:
        raise OperationSecurityError("recorder lease fd must be an inherited descriptor")
    operation = _validate_operation_id(operation)
    root = _state_root(state_root)
    try:
        with _open_operation_fd(operation, state_root=root, create=False) as operation_fd:
            _claim_recorder_lease_at(operation_fd, lease_fd)
            decision_fd = _secure_open_at(
                operation_fd,
                "decision.lock",
                flags=os.O_RDWR,
                label="decision lock",
            )
            output = None
            try:
                fcntl.flock(decision_fd, fcntl.LOCK_EX)
                _validate_operation_entries_at(operation_fd, recover=True)
                handle = _load_handle_from_fd(
                    operation,
                    state_root=root,
                    operation_fd=operation_fd,
                )
                # Validate the inherited controller fence before publishing
                # durable launch intent. A missing/closed descriptor is a
                # pre-activation failure and the exact authorized operation
                # must remain resumable, never become outcome-unknown without
                # having launched its mutation.
                _lifecycle_handoff_fds(
                    lifecycle_handoff,
                    expected_resources=handle.identity.lifecycle_resources,
                    expected_rollout_id=handle.identity.rollout_id,
                    expected_rollout_lock_path=handle.identity.rollout_lock_path,
                )
                ready = _read_optional_receipt_at(
                    operation_fd,
                    "ready.json",
                    label="ready receipt",
                )
                authorization = _read_optional_receipt_at(
                    operation_fd,
                    "authorization.json",
                    label="authorization receipt",
                )
                activation = _read_optional_receipt_at(
                    operation_fd,
                    "activation.json",
                    label="activation receipt",
                )
                result = _read_optional_receipt_at(
                    operation_fd,
                    "result.json",
                    label="result receipt",
                )
                if ready is None or authorization is None:
                    raise OperationStateError("recorder lacks a ready run authorization")
                nonce = _validate_ready(ready, handle)
                decision, authorization_nonce = _validate_authorization(
                    authorization,
                    handle,
                    ready_nonce=nonce,
                )
                if decision != "run" or authorization_nonce != nonce:
                    raise OperationStateError("recorder was not authorized to run")
                if result is not None:
                    raise OperationStateError("operation already has a terminal result")
                if activation is not None:
                    raise OperationStateError("operation already has durable launch intent")
                output = _open_empty_output_at(operation_fd)
                _write_receipt_at(
                    operation_fd,
                    "activation.json",
                    _activation_payload(handle, nonce=nonce),
                )
            except BaseException:
                if output is not None:
                    output.close()
                raise
            finally:
                fcntl.flock(decision_fd, fcntl.LOCK_UN)
                os.close(decision_fd)
            assert output is not None
            return _execute_activated_action(
                handle,
                operation_fd=operation_fd,
                nonce=nonce,
                output=output,
                started=time.monotonic(),
                child_popen=child_popen,
                lifecycle_handoff=lifecycle_handoff,
            )
    finally:
        # Never issue LOCK_UN here: the spawning supervisor can still hold a
        # duplicate of the same open file description until Popen returns.
        # Last-close semantics release the lock without creating a handoff gap.
        with contextlib.suppress(OSError):
            os.close(lease_fd)


def _launch_recorder(
    handle: OperationHandle,
    lease_fd: int,
    *,
    recorder_popen: ChildPopen,
    lifecycle_handoff: str | None = None,
) -> subprocess.Popen[bytes]:
    environment = _scrubbed_operation_environment()
    environment[paths.ENV_STATE_DIR] = str(handle.state_root)
    lifecycle_fds = _lifecycle_handoff_fds(
        lifecycle_handoff,
        expected_resources=handle.identity.lifecycle_resources,
        expected_rollout_id=handle.identity.rollout_id,
        expected_rollout_lock_path=handle.identity.rollout_lock_path,
    )
    if lifecycle_handoff is not None:
        environment[ENV_LIFECYCLE_HANDOFF] = lifecycle_handoff
    return recorder_popen(
        [
            sys.executable,
            "-m",
            "vq.fleet_operation",
            "--record",
            "--lease-fd",
            str(lease_fd),
            handle.operation_id,
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
        pass_fds=(lease_fd, *lifecycle_fds),
        env=environment,
    )


def run_supervisor(
    operation: str,
    *,
    state_root: Path | None = None,
    authorization_timeout: float = DEFAULT_AUTHORIZATION_TIMEOUT_SECONDS,
    poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS,
    child_popen: ChildPopen | None = None,
    recorder_popen: ChildPopen = subprocess.Popen,
    lifecycle_handoff: str | None = None,
) -> int:
    """Authorize once, then hand the lifetime lease to a detached recorder."""
    if (
        type(authorization_timeout) not in {int, float}
        or not math.isfinite(authorization_timeout)
        or type(poll_interval) not in {int, float}
        or not math.isfinite(poll_interval)
        or authorization_timeout < 0
        or poll_interval <= 0
    ):
        raise ValueError("authorization_timeout must be non-negative and poll_interval positive")
    operation = _validate_operation_id(operation)
    root = _state_root(state_root)
    with _open_operation_fd(operation, state_root=root, create=False) as operation_fd:
        request_decision_fd = _secure_open_at(
            operation_fd,
            "decision.lock",
            flags=os.O_RDWR,
            label="decision lock",
        )
        recorder_launch_entered = False
        try:
            fcntl.flock(request_decision_fd, fcntl.LOCK_EX)
            _validate_operation_entries_at(operation_fd, recover=True)
            handle = _load_handle_from_fd(
                operation,
                state_root=root,
                operation_fd=operation_fd,
            )
            # ``launch_supervisor`` validates this before the first exec, but
            # the detached entry point is also callable directly. Revalidate
            # before publishing readiness or activation so an incomplete
            # descriptor handoff cannot strand an unexecuted operation as
            # outcome-unknown.
            _lifecycle_handoff_fds(
                lifecycle_handoff,
                expected_resources=handle.identity.lifecycle_resources,
                expected_rollout_id=handle.identity.rollout_id,
                expected_rollout_lock_path=handle.identity.rollout_lock_path,
            )
        finally:
            fcntl.flock(request_decision_fd, fcntl.LOCK_UN)
            os.close(request_decision_fd)
        lease_fd = _secure_open_at(
            operation_fd,
            "lease.lock",
            flags=os.O_RDWR,
            label="lease lock",
        )
        try:
            try:
                fcntl.flock(lease_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                if exc.errno not in {errno.EACCES, errno.EAGAIN}:
                    raise
                raise OperationBusyError("another supervisor owns the lifetime lease") from exc

            output = None
            resuming_authorized = False
            started = time.monotonic()
            decision_fd = _secure_open_at(
                operation_fd,
                "decision.lock",
                flags=os.O_RDWR,
                label="decision lock",
            )
            try:
                fcntl.flock(decision_fd, fcntl.LOCK_EX)
                _validate_operation_entries_at(operation_fd, recover=True)
                existing_authorization = _read_optional_receipt_at(
                    operation_fd,
                    "authorization.json",
                    label="authorization receipt",
                )
                existing_result = _read_optional_receipt_at(
                    operation_fd,
                    "result.json",
                    label="result receipt",
                )
                existing_ready = _read_optional_receipt_at(
                    operation_fd,
                    "ready.json",
                    label="ready receipt",
                )
                existing_activation = _read_optional_receipt_at(
                    operation_fd,
                    "activation.json",
                    label="activation receipt",
                )
                if existing_result is not None:
                    raise OperationStateError("operation already has a terminal result")
                if existing_authorization is not None:
                    ready_nonce = (
                        _validate_ready(existing_ready, handle)
                        if existing_ready is not None
                        else None
                    )
                    decision, nonce = _validate_authorization(
                        existing_authorization,
                        handle,
                        ready_nonce=ready_nonce,
                    )
                    if decision == "abort" and existing_activation is None:
                        _write_aborted_result_at(
                            handle,
                            operation_fd,
                            nonce=nonce,
                            duration_seconds=0.0,
                        )
                        return SUPERVISOR_NOT_AUTHORIZED_EXIT_CODE
                    if decision != "run" or existing_activation is not None:
                        raise OperationStateError("operation already has a committed decision")
                    # Authorization is immutable, and every supported
                    # supervisor writes activation before mutating Popen.
                    # Resume this exact operation after a pre-activation
                    # supervisor loss; never mint another action attempt.
                    output = _open_empty_output_at(operation_fd)
                    resuming_authorized = True
                else:
                    if existing_ready is not None or existing_activation is not None:
                        raise OperationStateError(
                            "operation already has supervisor receipts; use a new attempt"
                        )

                    nonce = secrets.token_hex(32)
                    output = _create_output_at(operation_fd)
                    _write_receipt_at(
                        operation_fd,
                        "ready.json",
                        _ready_payload(handle, nonce=nonce),
                    )
            except BaseException:
                if output is not None:
                    output.close()
                raise
            finally:
                fcntl.flock(decision_fd, fcntl.LOCK_UN)
                os.close(decision_fd)
            assert output is not None
            authorization: Mapping[str, object]
            with output:
                if resuming_authorized:
                    assert existing_authorization is not None
                    authorization = existing_authorization
                else:
                    authorization = _wait_for_authorization(
                        handle,
                        operation_fd=operation_fd,
                        nonce=nonce,
                        timeout=authorization_timeout,
                        poll_interval=poll_interval,
                    )
                decision, authorization_nonce = _validate_authorization(
                    authorization,
                    handle,
                    ready_nonce=nonce,
                )
                if decision == "abort":
                    output.flush()
                    os.fsync(output.fileno())
                    _write_aborted_result_decided_at(
                        handle,
                        operation_fd,
                        nonce=authorization_nonce,
                        duration_seconds=time.monotonic() - started,
                    )
                    return SUPERVISOR_NOT_AUTHORIZED_EXIT_CODE

                if child_popen is not None:
                    # Dependency injection keeps focused unit tests local;
                    # production always uses the detached recorder path.
                    _write_decided_receipt_at(
                        operation_fd,
                        "activation.json",
                        _activation_payload(handle, nonce=nonce),
                    )
                    return _execute_activated_action(
                        handle,
                        operation_fd=operation_fd,
                        nonce=nonce,
                        output=output,
                        started=started,
                        child_popen=child_popen,
                        lifecycle_handoff=lifecycle_handoff,
                    )
                output.flush()
                os.fsync(output.fileno())
            # From this point onward Popen may have forked and inherited this
            # open file description even if it never returns to us.  Cleanup
            # must therefore use close-only semantics: last-close releases a
            # failed pre-fork launch, while a post-fork child retains the lock.
            recorder_launch_entered = True
            recorder = _launch_recorder(
                handle,
                lease_fd,
                recorder_popen=recorder_popen,
                lifecycle_handoff=lifecycle_handoff,
            )
            # Popen returns only after the recorder exec succeeds. Its
            # pass_fds copy now owns the same flock. Close without LOCK_UN:
            # an explicit unlock would release the shared lock in both
            # processes on Linux and macOS.
            with contextlib.suppress(OSError):
                os.close(lease_fd)
            lease_fd = -1
            return _supervisor_exit_code(recorder.wait())
        finally:
            if lease_fd >= 0:
                if not recorder_launch_entered:
                    with contextlib.suppress(OSError):
                        fcntl.flock(lease_fd, fcntl.LOCK_UN)
                with contextlib.suppress(OSError):
                    os.close(lease_fd)


def launch_supervisor(
    operation: str,
    *,
    state_root: Path | None = None,
    lifecycle_handoff: str | None = None,
) -> subprocess.Popen[bytes]:
    """Start the dependency-light supervisor in a detached interpreter."""
    handle = _load_handle(operation, state_root=state_root)
    environment = _scrubbed_operation_environment()
    environment[paths.ENV_STATE_DIR] = str(handle.state_root)
    lifecycle_fds = _lifecycle_handoff_fds(
        lifecycle_handoff,
        expected_resources=handle.identity.lifecycle_resources,
        expected_rollout_id=handle.identity.rollout_id,
        expected_rollout_lock_path=handle.identity.rollout_lock_path,
    )
    if lifecycle_handoff is not None:
        environment[ENV_LIFECYCLE_HANDOFF] = lifecycle_handoff
    return subprocess.Popen(
        [sys.executable, "-m", "vq.fleet_operation", handle.operation_id],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
        pass_fds=lifecycle_fds,
        env=environment,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--record", action="store_true")
    parser.add_argument("--lease-fd", type=int)
    parser.add_argument("operation_id")
    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        if args.record:
            if args.lease_fd is None:
                raise ValueError("recorder mode requires an inherited lease fd")
            return run_recorder(
                args.operation_id,
                lease_fd=args.lease_fd,
                lifecycle_handoff=os.environ.get(ENV_LIFECYCLE_HANDOFF),
            )
        if args.lease_fd is not None:
            raise ValueError("supervisor mode does not accept a lease fd")
        return run_supervisor(
            args.operation_id,
            lifecycle_handoff=os.environ.get(ENV_LIFECYCLE_HANDOFF),
        )
    except (OperationError, ValueError):
        return SUPERVISOR_INVALID_REQUEST_EXIT_CODE
    except Exception:
        # A detached internal supervisor has no user-facing output channel.
        # Durable receipts and the lease are the only reconciliation surface.
        return SUPERVISOR_INTERNAL_ERROR_EXIT_CODE


if __name__ == "__main__":
    raise SystemExit(main())
