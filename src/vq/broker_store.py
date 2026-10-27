"""Durable held-job store for the fleet broker (#64, B3).

The broker keeps accepted submissions outside its own execution queue: a held
job is a broker-owned record plus an immutable payload archive, never a
JobSpec in the broker's run queue. This module owns only that storage and its
state machine. It does not gather capacity, choose hosts, contact targets or
enable a broker role; B4 drives these transitions and B5 routes clients.

Durability discipline, from ``docs/design_broker.md``:

* One writer. :meth:`BrokerStore.open` takes an exclusive non-blocking lock
  for the lifetime of the store object; a second writer is refused.
* Every transition is a compare-and-swap on the record revision, so a caller
  holding a stale view cannot apply a transition.
* A record or payload is published by atomic rename followed by a strict
  directory ``fsync``. Any write or ``fsync`` failure raises, so the caller
  must not acknowledge or take the next side effect.
* ``transferring`` is written before any remote submission. From then on the
  target binding and the transfer fingerprint are frozen, the resource charge
  stays outstanding, and the only exits are a recorded acceptance or operator
  reconciliation. No transition returns an attempt that may have reached its
  target, because the submit path offers no cross-host rejection fence.
* Records are never deleted here. A placed or cancelled record is the
  tombstone that stops a retry from creating a second logical job.

Paths are validated no-follow, owner-only (``0700`` directories, ``0600``
files) and operated on relative to open directory descriptors.
"""

from __future__ import annotations

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
import tarfile
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from vq import dirfd, overview
from vq.spec import validate_job_id

RECORD_SCHEMA = "vq.broker-record.v1"
FINGERPRINT_SCHEMA = "vq.broker-transfer.v1"

STATE_HELD = "held"
STATE_RESERVED = "reserved"
STATE_TRANSFERRING = "transferring"
STATE_PLACED = "placed"
STATE_RETURNED = "returned"
STATE_CANCELLED = "cancelled"
STATES = frozenset(
    {
        STATE_HELD,
        STATE_RESERVED,
        STATE_TRANSFERRING,
        STATE_PLACED,
        STATE_RETURNED,
        STATE_CANCELLED,
    }
)

# ``b-`` plus 12 hex characters: a safe job id and a valid submission
# idempotency key (SPEC §4.8), and visibly not a target's 12-hex job id.
BROKER_JOB_ID_PATTERN = re.compile(r"b-[0-9a-f]{12}")

_RECORDS = "records"
_PAYLOADS = "payloads"
_STAGING = "staging"
_LOCK = ".lock"
_PASS_LOCK = ".pass.lock"
DEFAULT_LOCK_TIMEOUT_SECONDS = 30.0
_RECORD_MAX_BYTES = 1024 * 1024
_DIAGNOSTIC_MAX_CHARS = 2000
_DIAGNOSTICS_MAX = 50
_ATTEMPTS_MAX = 1000
_COPY_CHUNK = 1024 * 1024
# Non-blocking so a FIFO planted in place of a file fails the S_ISREG check
# instead of blocking the sole writer in open(); regular files ignore it.
_READ_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK


class BrokerStoreError(ValueError):
    """The broker store refused an operation; no state changed."""


class BrokerStoreBusy(BrokerStoreError):
    """Another writer holds the broker store."""


class BrokerRecordConflict(BrokerStoreError):
    """Stale revision, illegal transition or changed transfer intent."""


class BrokerIdempotencyConflict(BrokerStoreError):
    """The idempotency key already names a job with other content."""


class BrokerRecordCorrupt(BrokerStoreError):
    """A stored record or payload failed validation."""


def new_broker_job_id() -> str:
    return "b-" + secrets.token_hex(6)


_IDEMPOTENCY_KEY_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
_IDEMPOTENCY_ID_DOMAIN = "vq.broker.idempotency-key/1"


def broker_job_id_for_key(key: str) -> str:
    """The broker job id an idempotency key names (SPEC §4.8, #64).

    Derived, not stored: the key itself never reaches the record, and the
    same key always names the same id on this broker.
    """
    if not isinstance(key, str) or _IDEMPOTENCY_KEY_PATTERN.fullmatch(key) is None:
        raise BrokerStoreError(
            "idempotency key must be 1-128 characters, start with an "
            "alphanumeric character, and contain only alphanumerics plus "
            "'.', '_', ':', or '-'"
        )
    return "b-" + _sha256_hex(f"{_IDEMPOTENCY_ID_DOMAIN}\0{key}".encode())[:12]


def validate_broker_job_id(value: object) -> str:
    if not isinstance(value, str) or BROKER_JOB_ID_PATTERN.fullmatch(value) is None:
        raise BrokerStoreError(f"invalid broker job id {value!r}")
    return value


def default_broker_root() -> Path:
    from vq import paths

    return paths.state_root() / "broker"


def _finite_time(value: object, name: str) -> float:
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise BrokerStoreError(f"{name} must be a finite, non-negative time")
    return float(value)


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("ascii")


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _exact_keys(data: object, keys: frozenset[str], what: str) -> Mapping[str, Any]:
    if not isinstance(data, dict):
        raise BrokerRecordCorrupt(f"{what} must be an object")
    if set(data) != keys:
        missing = sorted(keys - set(data))
        extra = sorted(set(data) - keys)
        raise BrokerRecordCorrupt(
            f"{what} has unexpected fields (missing {missing}, extra {extra})"
        )
    return data


# --------------------------------------------------------------------------
# Submission intent


@dataclass(frozen=True)
class BrokerIntent:
    """The canonical submission a held job will make on its chosen target.

    A deliberate subset of the remote submit wire: target-specific inputs
    (scheduler target, branch interpreter, cross-host dependencies, arrays and
    chains) are not accepted. Memory is mandatory because placement never
    treats unknown memory as unlimited.
    """

    command: tuple[str, ...]
    cpus: int
    mem_mb: int
    wall_time_seconds: int | None = None
    priority: int = 0
    auto_resume: bool = False
    retry: int = 0
    job_name: str | None = None
    program: str | None = None
    expected_sha: str | None = None
    tags: tuple[str, ...] = ()
    clean_workdir_on_terminal: bool = False
    rerun_until_file_exists: str | None = None
    rerun_max: int = 10
    pool: str | None = None
    #: #159: ``(low, high)`` execution seconds on a speed-1.0 target, from a
    #: calibrated estimate. Only the completion-time policy reads it, to rank.
    runtime_seconds: tuple[float, float] | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.command, tuple) or not self.command or not all(
            isinstance(part, str) for part in self.command
        ):
            raise BrokerStoreError("broker intent command must be a non-empty tuple of strings")
        if not isinstance(self.tags, tuple) or not all(isinstance(t, str) for t in self.tags):
            raise BrokerStoreError("broker intent tags must be a tuple of strings")
        for name in ("auto_resume", "clean_workdir_on_terminal"):
            if type(getattr(self, name)) is not bool:
                raise BrokerStoreError(f"broker intent {name} must be a boolean")
        for name in ("priority", "retry", "rerun_max"):
            if type(getattr(self, name)) is not int:
                raise BrokerStoreError(f"broker intent {name} must be an integer")
        if self.wall_time_seconds is not None and type(self.wall_time_seconds) is not int:
            raise BrokerStoreError("broker intent wall_time_seconds must be an integer")
        for name in ("job_name", "program", "expected_sha", "rerun_until_file_exists", "pool"):
            value = getattr(self, name)
            if value is not None and not isinstance(value, str):
                raise BrokerStoreError(f"broker intent {name} must be a string")
        if self.pool is not None and not self.pool:
            raise BrokerStoreError("broker intent pool must not be empty")
        if self.runtime_seconds is not None:
            bounds = self.runtime_seconds
            if not (isinstance(bounds, tuple) and len(bounds) == 2 and all(
                    type(b) in (int, float) and math.isfinite(b) for b in bounds)
                    and 0 <= bounds[0] <= bounds[1]):
                raise BrokerStoreError(
                    "broker intent runtime_seconds must be finite seconds 0 <= low <= high")
        try:
            # The demand model B2 places with, and the exact field validation
            # the remote submit wire applies before any upload.
            overview.BrokerPlacementRequest(
                cpus=self.cpus, mem_mb=self.mem_mb, pool=self.pool,
                wall_time_seconds=self.wall_time_seconds,
            )
            from vq import submit

            submit._validate_command_head(self.command)
            submit._validate_remote_submit_fields(
                command=list(self.command),
                cpus=self.cpus,
                scheduler_tasks=None,
                mem_mb=self.mem_mb,
                wall_time_seconds=self.wall_time_seconds,
                priority=self.priority,
                auto_resume=self.auto_resume,
                retry=self.retry,
                job_name=self.job_name,
                branch=None,
                program=self.program,
                expected_sha=self.expected_sha,
                tags=list(self.tags),
                not_before=None,
                depends_on=None,
                depends_on_any=None,
                clean_workdir_on_terminal=self.clean_workdir_on_terminal,
                array=1,
                chain=1,
                rerun_until_file_exists=self.rerun_until_file_exists,
                rerun_max=self.rerun_max,
                refresh_before=None,
                scheduler_target=None,
            )
        except BrokerStoreError:
            raise
        except (TypeError, ValueError) as exc:
            raise BrokerStoreError(f"invalid broker intent: {exc}") from exc

    def placement_request(self) -> overview.BrokerPlacementRequest:
        return overview.BrokerPlacementRequest(
            cpus=self.cpus, mem_mb=self.mem_mb, pool=self.pool,
            wall_time_seconds=self.wall_time_seconds,
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "command": list(self.command),
            "cpus": self.cpus,
            "mem_mb": self.mem_mb,
            "wall_time_seconds": self.wall_time_seconds,
            "priority": self.priority,
            "auto_resume": self.auto_resume,
            "retry": self.retry,
            "job_name": self.job_name,
            "program": self.program,
            "expected_sha": self.expected_sha,
            "tags": list(self.tags),
            "clean_workdir_on_terminal": self.clean_workdir_on_terminal,
            "rerun_until_file_exists": self.rerun_until_file_exists,
            "rerun_max": self.rerun_max,
            "pool": self.pool,
            # Written only when set: a record without it, and its digest, are
            # exactly what a vq without this field writes and reads.
            **({"runtime_seconds": list(self.runtime_seconds)}
               if self.runtime_seconds is not None else {}),
        }

    @classmethod
    def from_json(cls, data: object) -> BrokerIntent:
        keys = _INTENT_KEYS if isinstance(data, dict) and "runtime_seconds" in data else (
            _INTENT_KEYS - {"runtime_seconds"})
        fields_ = _exact_keys(data, keys, "broker intent")
        values = dict(fields_)
        if "runtime_seconds" in values:
            if not isinstance(values["runtime_seconds"], list):
                raise BrokerRecordCorrupt("broker intent runtime_seconds must be a list")
            values["runtime_seconds"] = tuple(values["runtime_seconds"])
        for name in ("command", "tags"):
            if not isinstance(values[name], list):
                raise BrokerRecordCorrupt(f"broker intent {name} must be a list")
            values[name] = tuple(values[name])
        try:
            return cls(**values)
        except BrokerStoreError as exc:
            raise BrokerRecordCorrupt(str(exc)) from exc

    def digest(self) -> str:
        return _sha256_hex(_canonical_json(self.to_json()))


_INTENT_KEYS = frozenset(BrokerIntent.__dataclass_fields__)


# --------------------------------------------------------------------------
# Record model


@dataclass(frozen=True)
class BrokerCharge:
    cpus: int
    mem_mb: int
    job_slots: int = 1

    def to_json(self) -> dict[str, int]:
        return {"cpus": self.cpus, "mem_mb": self.mem_mb, "job_slots": self.job_slots}


@dataclass(frozen=True)
class BrokerAttempt:
    """One reservation of a target and, if started, its keyed transfer."""

    number: int
    phase: str
    target_host: str
    target_state_root: str
    reservation_id: str
    charge: BrokerCharge
    decided_at: float
    snapshot: dict[str, Any]
    ranking: list[list[Any]]
    blockers: dict[str, str]
    idempotency_key: str
    request_fingerprint: str | None = None
    transfer_started_at: float | None = None
    target_job_id: str | None = None
    accepted_at: float | None = None
    returned_at: float | None = None
    return_reason: str | None = None
    charge_released_at: float | None = None
    diagnostics: tuple[str, ...] = ()

    def to_json(self) -> dict[str, Any]:
        return {
            "number": self.number,
            "phase": self.phase,
            "target_host": self.target_host,
            "target_state_root": self.target_state_root,
            "reservation_id": self.reservation_id,
            "charge": self.charge.to_json(),
            "decided_at": self.decided_at,
            "snapshot": self.snapshot,
            "ranking": self.ranking,
            "blockers": self.blockers,
            "idempotency_key": self.idempotency_key,
            "request_fingerprint": self.request_fingerprint,
            "transfer_started_at": self.transfer_started_at,
            "target_job_id": self.target_job_id,
            "accepted_at": self.accepted_at,
            "returned_at": self.returned_at,
            "return_reason": self.return_reason,
            "charge_released_at": self.charge_released_at,
            "diagnostics": list(self.diagnostics),
        }


_ATTEMPT_KEYS = frozenset(BrokerAttempt.__dataclass_fields__)
_ATTEMPT_PHASES = frozenset({STATE_RESERVED, STATE_TRANSFERRING, STATE_PLACED, STATE_RETURNED})


@dataclass(frozen=True)
class BrokerCancellation:
    requested_at: float
    reason: str
    completed_at: float | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "requested_at": self.requested_at,
            "reason": self.reason,
            "completed_at": self.completed_at,
        }


@dataclass(frozen=True)
class BrokerRecord:
    broker_job_id: str
    revision: int
    state: str
    created_at: float
    updated_at: float
    intent: BrokerIntent
    payload_sha256: str
    payload_size: int
    attempts: tuple[BrokerAttempt, ...] = ()
    cancellation: BrokerCancellation | None = None
    held_blockers: tuple[tuple[str, str], ...] = ()

    @property
    def intent_digest(self) -> str:
        return self.intent.digest()

    @property
    def current_attempt(self) -> BrokerAttempt | None:
        return self.attempts[-1] if self.attempts else None

    def to_json(self) -> dict[str, Any]:
        return {
            "schema": RECORD_SCHEMA,
            "broker_job_id": self.broker_job_id,
            "revision": self.revision,
            "state": self.state,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "intent": self.intent.to_json(),
            "intent_digest": self.intent_digest,
            "payload": {"sha256": self.payload_sha256, "size": self.payload_size},
            "attempts": [attempt.to_json() for attempt in self.attempts],
            "cancellation": (
                self.cancellation.to_json() if self.cancellation is not None else None
            ),
            **({"held_blockers": dict(self.held_blockers)} if self.held_blockers else {}),
        }


_RECORD_KEYS = frozenset(
    {
        "schema", "broker_job_id", "revision", "state", "created_at", "updated_at",
        "intent", "intent_digest", "payload", "attempts", "cancellation",
    }
)


def _parse_time(value: object, name: str) -> float:
    try:
        return _finite_time(value, name)
    except BrokerStoreError as exc:
        raise BrokerRecordCorrupt(str(exc)) from exc


def _parse_opt_time(value: object, name: str) -> float | None:
    if value is None:
        return None
    try:
        return _finite_time(value, name)
    except BrokerStoreError as exc:
        raise BrokerRecordCorrupt(str(exc)) from exc


def _parse_str(value: object, name: str, *, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    if not isinstance(value, str) or not value:
        raise BrokerRecordCorrupt(f"{name} must be a non-empty string")
    return value


def _parse_positive_int(value: object, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise BrokerRecordCorrupt(f"{name} must be a positive integer")
    return value


def _parse_attempt(data: object, number: int, job_id: str) -> BrokerAttempt:
    raw = _exact_keys(data, _ATTEMPT_KEYS, f"attempt {number}")
    if raw["number"] != number:
        raise BrokerRecordCorrupt(f"attempt {number} is numbered {raw['number']!r}")
    if raw["phase"] not in _ATTEMPT_PHASES:
        raise BrokerRecordCorrupt(f"attempt {number} has unknown phase {raw['phase']!r}")
    if raw["idempotency_key"] != job_id:
        raise BrokerRecordCorrupt(f"attempt {number} idempotency key is not the broker job id")
    charge = _exact_keys(raw["charge"], frozenset({"cpus", "mem_mb", "job_slots"}), "charge")
    diagnostics = raw["diagnostics"]
    if not isinstance(diagnostics, list) or not all(isinstance(d, str) for d in diagnostics):
        raise BrokerRecordCorrupt(f"attempt {number} diagnostics must be strings")
    for name, kind in (("snapshot", dict), ("ranking", list), ("blockers", dict)):
        if not isinstance(raw[name], kind):
            raise BrokerRecordCorrupt(f"attempt {number} {name} has the wrong type")
    target_job_id = raw["target_job_id"]
    if target_job_id is not None:
        try:
            validate_job_id(target_job_id)
        except (TypeError, ValueError) as exc:
            raise BrokerRecordCorrupt(f"attempt {number}: {exc}") from exc
    fingerprint = raw["request_fingerprint"]
    if fingerprint is not None and (
        not isinstance(fingerprint, str) or re.fullmatch(r"[0-9a-f]{64}", fingerprint) is None
    ):
        raise BrokerRecordCorrupt(f"attempt {number} fingerprint is malformed")
    return BrokerAttempt(
        number=number,
        phase=raw["phase"],
        target_host=_parse_str(raw["target_host"], "target_host"),
        target_state_root=_parse_str(raw["target_state_root"], "target_state_root"),
        reservation_id=_parse_str(raw["reservation_id"], "reservation_id"),
        charge=BrokerCharge(
            cpus=_parse_positive_int(charge["cpus"], "charge cpus"),
            mem_mb=_parse_positive_int(charge["mem_mb"], "charge mem_mb"),
            job_slots=_parse_positive_int(charge["job_slots"], "charge job_slots"),
        ),
        decided_at=_parse_time(raw["decided_at"], "decided_at"),
        snapshot=raw["snapshot"],
        ranking=raw["ranking"],
        blockers=raw["blockers"],
        idempotency_key=raw["idempotency_key"],
        request_fingerprint=fingerprint,
        transfer_started_at=_parse_opt_time(raw["transfer_started_at"], "transfer_started_at"),
        target_job_id=target_job_id,
        accepted_at=_parse_opt_time(raw["accepted_at"], "accepted_at"),
        returned_at=_parse_opt_time(raw["returned_at"], "returned_at"),
        return_reason=_parse_str(raw["return_reason"], "return_reason", optional=True),
        charge_released_at=_parse_opt_time(raw["charge_released_at"], "charge_released_at"),
        diagnostics=tuple(diagnostics),
    )


FENCED_RETURN_PREFIX = "fenced on target: "
"""Return reason of a started transfer the target fenced (#64).

A started transfer may only be returned with this proof: the target held the
per-key lock, found no submission under the key, and durably recorded the key
as refused, so no copy of the request can ever be accepted there."""


def _check_consistency(record: BrokerRecord) -> None:
    """Refuse a record whose state contradicts its own attempt evidence."""
    if record.held_blockers and record.state not in (STATE_HELD, STATE_CANCELLED):
        raise BrokerRecordCorrupt("held blockers on a record that is no longer held")
    for attempt in record.attempts[:-1]:
        if attempt.phase != STATE_RETURNED or attempt.charge_released_at is None:
            raise BrokerRecordCorrupt(
                f"attempt {attempt.number} is superseded but was never returned"
            )
    attempt = record.current_attempt
    phase = attempt.phase if attempt is not None else None
    for a in record.attempts:
        started = a.transfer_started_at is not None or a.request_fingerprint is not None
        if a.phase in (STATE_TRANSFERRING, STATE_PLACED) and not (
            a.transfer_started_at is not None and a.request_fingerprint is not None
        ):
            raise BrokerRecordCorrupt(f"attempt {a.number} lacks its transfer intent")
        if a.phase == STATE_RETURNED and started and not (
            a.return_reason or ""
        ).startswith(FENCED_RETURN_PREFIX):
            # Only a target-side fence proves a started transfer cannot land.
            raise BrokerRecordCorrupt(f"attempt {a.number} returned after its transfer began")
        if (a.phase == STATE_PLACED) != (a.target_job_id is not None):
            raise BrokerRecordCorrupt(f"attempt {a.number} target job id contradicts its phase")
        if (a.phase == STATE_RETURNED) != (a.returned_at is not None):
            raise BrokerRecordCorrupt(f"attempt {a.number} return evidence contradicts its phase")
        if a.phase in (STATE_RESERVED, STATE_TRANSFERRING) and a.charge_released_at is not None:
            raise BrokerRecordCorrupt(f"attempt {a.number} released the charge of an open attempt")
    expected_phase = {
        STATE_HELD: (None, STATE_RETURNED),
        STATE_CANCELLED: (None, STATE_RETURNED),
        STATE_RESERVED: (STATE_RESERVED,),
        STATE_TRANSFERRING: (STATE_TRANSFERRING,),
        STATE_PLACED: (STATE_PLACED,),
        STATE_RETURNED: (STATE_RETURNED,),
    }[record.state]
    if phase not in expected_phase:
        raise BrokerRecordCorrupt(
            f"state {record.state!r} contradicts current attempt phase {phase!r}"
        )
    if record.state in (STATE_HELD, STATE_CANCELLED) and attempt is not None and (
        attempt.charge_released_at is None
    ):
        raise BrokerRecordCorrupt("a held record still carries an outstanding charge")
    cancellation = record.cancellation
    if record.state == STATE_CANCELLED and (
        cancellation is None or cancellation.completed_at is None
    ):
        raise BrokerRecordCorrupt("a cancelled record lacks completed cancellation evidence")
    if record.state != STATE_CANCELLED and cancellation is not None and (
        cancellation.completed_at is not None
    ):
        raise BrokerRecordCorrupt("completed cancellation on a record that is not cancelled")


def parse_record(data: object) -> BrokerRecord:
    keys = _RECORD_KEYS
    if isinstance(data, dict) and "held_blockers" in data:
        keys = keys | {"held_blockers"}
    raw = _exact_keys(data, keys, "broker record")
    blockers = raw.get("held_blockers", {})
    if not isinstance(blockers, dict) or not all(
        isinstance(host, str) and host and isinstance(reason, str) and reason
        for host, reason in blockers.items()
    ):
        raise BrokerRecordCorrupt("held blockers must map non-empty hosts to reasons")
    if raw["schema"] != RECORD_SCHEMA:
        raise BrokerRecordCorrupt(f"unsupported broker record schema {raw['schema']!r}")
    try:
        job_id = validate_broker_job_id(raw["broker_job_id"])
    except BrokerStoreError as exc:
        raise BrokerRecordCorrupt(str(exc)) from exc
    if raw["state"] not in STATES:
        raise BrokerRecordCorrupt(f"unknown broker state {raw['state']!r}")
    intent = BrokerIntent.from_json(raw["intent"])
    if raw["intent_digest"] != intent.digest():
        raise BrokerRecordCorrupt("broker intent digest does not match its intent")
    payload = _exact_keys(raw["payload"], frozenset({"sha256", "size"}), "payload")
    if not isinstance(payload["sha256"], str) or re.fullmatch(
        r"[0-9a-f]{64}", payload["sha256"]
    ) is None:
        raise BrokerRecordCorrupt("payload digest is malformed")
    if type(payload["size"]) is not int or payload["size"] < 0:
        raise BrokerRecordCorrupt("payload size is malformed")
    attempts_raw = raw["attempts"]
    if not isinstance(attempts_raw, list) or len(attempts_raw) > _ATTEMPTS_MAX:
        raise BrokerRecordCorrupt("attempts must be a bounded list")
    cancellation = None
    if raw["cancellation"] is not None:
        c = _exact_keys(
            raw["cancellation"], frozenset({"requested_at", "reason", "completed_at"}),
            "cancellation",
        )
        cancellation = BrokerCancellation(
            requested_at=_parse_time(c["requested_at"], "requested_at"),
            reason=_parse_str(c["reason"], "cancellation reason"),
            completed_at=_parse_opt_time(c["completed_at"], "completed_at"),
        )
    record = BrokerRecord(
        broker_job_id=job_id,
        revision=_parse_positive_int(raw["revision"], "revision"),
        state=raw["state"],
        created_at=_parse_time(raw["created_at"], "created_at"),
        updated_at=_parse_time(raw["updated_at"], "updated_at"),
        intent=intent,
        payload_sha256=payload["sha256"],
        payload_size=payload["size"],
        attempts=tuple(
            _parse_attempt(item, index + 1, job_id) for index, item in enumerate(attempts_raw)
        ),
        cancellation=cancellation,
        held_blockers=tuple(sorted(blockers.items())),
    )
    _check_consistency(record)
    return record


# --------------------------------------------------------------------------
# Transfer request


def transfer_fingerprint(record: BrokerRecord, attempt: BrokerAttempt) -> str:
    """Identity of one keyed submission: authority, key, intent and payload."""
    return _sha256_hex(
        _canonical_json(
            {
                "schema": FINGERPRINT_SCHEMA,
                "target_host": attempt.target_host,
                "target_state_root": attempt.target_state_root,
                "idempotency_key": attempt.idempotency_key,
                "intent_digest": record.intent_digest,
                "payload_sha256": record.payload_sha256,
                **({"authority": attempt.snapshot["authority"]}
                   if "authority" in attempt.snapshot else {}),
                **({"runtime_pins": attempt.snapshot["runtime_pins"]}
                   if "runtime_pins" in attempt.snapshot else {}),
            }
        )
    )


@dataclass(frozen=True)
class BrokerTransferRequest:
    """Everything B4 needs to replay one keyed submit to its fixed target.

    ``submit_kwargs`` feed :func:`vq.submit.submit_remote` together with the
    target's ``host`` and ``host_cfg``. B4 must first confirm that the host
    configuration still resolves to ``target_state_root``; a renamed alias or
    changed state root must not redirect a replay.
    """

    broker_job_id: str
    target_host: str
    target_state_root: str
    idempotency_key: str
    request_fingerprint: str
    payload_path: Path
    submit_kwargs: dict[str, Any] = field(default_factory=dict)
    authority: dict[str, str] | None = None


# --------------------------------------------------------------------------
# Filesystem helpers


def _open_owned_dir(name: str, *, dir_fd: int | None, create: bool) -> int:
    if create:
        with contextlib.suppress(FileExistsError):
            os.mkdir(name, 0o700, dir_fd=dir_fd)
    try:
        fd = os.open(
            name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=dir_fd
        )
    except OSError as exc:
        if exc.errno in (errno.ELOOP, errno.ENOTDIR):
            raise BrokerStoreError(f"broker path {name!r} is not a real directory") from exc
        raise BrokerStoreError(f"cannot open broker directory {name!r}: {exc}") from exc
    try:
        st = os.fstat(fd)
        if st.st_uid != os.geteuid():
            raise BrokerStoreError(f"broker directory {name!r} has the wrong owner")
        if st.st_mode & 0o077:
            raise BrokerStoreError(f"broker directory {name!r} has unsafe permissions")
    except BaseException:
        os.close(fd)
        raise
    return fd


def _fsync_dir(fd: int) -> None:
    # Strict: a lost directory entry would be a lost acknowledgement.
    os.fsync(fd)


def _write_file_at(dir_fd: int, name: str, data: bytes) -> None:
    fd = os.open(
        name,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
        0o600,
        dir_fd=dir_fd,
    )
    try:
        view = memoryview(data)
        while view:
            written = os.write(fd, view)
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)


def _read_regular_at(dir_fd: int, name: str, limit: int) -> bytes:
    fd = os.open(name, _READ_FLAGS, dir_fd=dir_fd)
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode) or st.st_uid != os.geteuid():
            raise BrokerRecordCorrupt(f"{name} is not an owned regular file")
        if st.st_size > limit:
            raise BrokerRecordCorrupt(f"{name} exceeds {limit} bytes")
        chunks = []
        remaining = limit + 1
        while chunk := os.read(fd, min(_COPY_CHUNK, remaining)):
            chunks.append(chunk)
            remaining -= len(chunk)
            if remaining <= 0:
                raise BrokerRecordCorrupt(f"{name} exceeds {limit} bytes")
        return b"".join(chunks)
    finally:
        os.close(fd)


def _unlink_quietly(dir_fd: int, name: str) -> None:
    with contextlib.suppress(OSError):
        os.unlink(name, dir_fd=dir_fd)


# --------------------------------------------------------------------------
# Store


def _open_lock_file(name: str, root_fd: int) -> int:
    fd = os.open(
        name, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=root_fd
    )
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode) or st.st_uid != os.geteuid() or st.st_mode & 0o077:
            raise BrokerStoreError(f"broker lock file {name} is not an owner-only regular file")
    except BaseException:
        os.close(fd)
        raise
    return fd


class BrokerStore:
    """One broker's held records and payloads.

    Several processes may hold a store open at once (a placement pass, a
    client submission, a cancellation). Every read-modify-write transition
    takes the store's writer lock only for its own duration and applies only
    if the record is still at the caller's revision, so a stale view fails
    instead of overwriting. Creating a record needs no lock: its name is
    published with an exclusive ``link()``. Placement passes additionally
    exclude each other with :meth:`pass_lock`.
    """

    def __init__(
        self, root: Path, root_fd: int, lock_fd: int, dirs: dict[str, int],
        *, lock_timeout: float = DEFAULT_LOCK_TIMEOUT_SECONDS,
    ) -> None:
        if not math.isfinite(lock_timeout) or lock_timeout < 0:
            raise BrokerStoreError("broker lock timeout must be finite and not negative")
        self.root = root
        self._root_fd = root_fd
        self._lock_fd = lock_fd
        self._dirs = dirs
        self._closed = False
        self._lock_timeout = lock_timeout
        self._thread_lock = threading.Lock()

    @contextlib.contextmanager
    def _writer_lock(self) -> Iterator[None]:
        """Serialize one transition across processes (and threads), bounded."""
        deadline = time.monotonic() + self._lock_timeout
        if not self._thread_lock.acquire(timeout=max(self._lock_timeout, 0.0)):
            raise BrokerStoreBusy("the broker store stayed busy; retry")
        try:
            while True:
                try:
                    fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise BrokerStoreBusy("the broker store stayed busy; retry") from None
                    time.sleep(0.02)
            try:
                yield
            finally:
                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
        finally:
            self._thread_lock.release()

    @contextlib.contextmanager
    def pass_lock(self) -> Iterator[None]:
        """Exclude a second placement pass; refuses at once rather than wait."""
        self._check_open()
        fd = _open_lock_file(_PASS_LOCK, self._root_fd)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise BrokerStoreBusy("another broker placement pass is running") from exc
            yield
        finally:
            os.close(fd)

    @classmethod
    @contextlib.contextmanager
    def open(
        cls, root: Path | None = None, *, lock_timeout: float = DEFAULT_LOCK_TIMEOUT_SECONDS,
    ) -> Iterator[BrokerStore]:
        root = Path(root) if root is not None else default_broker_root()
        if not root.is_absolute():
            raise BrokerStoreError("broker root must be an absolute path")
        parent_fd = os.open(
            str(root.parent), os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC
        )
        opened: list[int] = []
        try:
            root_fd = _open_owned_dir(root.name, dir_fd=parent_fd, create=True)
            opened.append(root_fd)
            lock_fd = _open_lock_file(_LOCK, root_fd)
            opened.append(lock_fd)
            dirs = {}
            for name in (_RECORDS, _PAYLOADS, _STAGING):
                dirs[name] = _open_owned_dir(name, dir_fd=root_fd, create=True)
                opened.append(dirs[name])
            _fsync_dir(root_fd)
        except BaseException:
            for fd in reversed(opened):
                os.close(fd)
            raise
        finally:
            os.close(parent_fd)
        store = cls(root, root_fd, lock_fd, dirs, lock_timeout=lock_timeout)
        try:
            yield store
        finally:
            store._close()

    def _close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for fd in self._dirs.values():
            os.close(fd)
        os.close(self._lock_fd)
        os.close(self._root_fd)

    def _check_open(self) -> None:
        if self._closed:
            raise BrokerStoreError("broker store is closed")

    # -- paths --------------------------------------------------------

    def payload_path(self, broker_job_id: str) -> Path:
        return self.root / _PAYLOADS / f"{validate_broker_job_id(broker_job_id)}.tar"

    # -- reading ------------------------------------------------------

    def get(self, broker_job_id: str) -> BrokerRecord:
        self._check_open()
        job_id = validate_broker_job_id(broker_job_id)
        try:
            data = _read_regular_at(self._dirs[_RECORDS], f"{job_id}.json", _RECORD_MAX_BYTES)
        except FileNotFoundError as exc:
            raise BrokerStoreError(f"no broker record {job_id}") from exc
        except OSError as exc:
            raise BrokerRecordCorrupt(f"cannot read broker record {job_id}: {exc}") from exc
        try:
            parsed = json.loads(data)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BrokerRecordCorrupt(f"broker record {job_id} is not JSON") from exc
        record = parse_record(parsed)
        if record.broker_job_id != job_id:
            raise BrokerRecordCorrupt(f"broker record {job_id} names {record.broker_job_id}")
        return record

    def scan(self) -> tuple[list[BrokerRecord], dict[str, str]]:
        """Every record, plus a refusal reason for each one that is unusable.

        Recovery must stop placement for a refused record rather than infer
        anything from it; the caller decides whether that blocks new work.
        """
        self._check_open()
        records: list[BrokerRecord] = []
        problems: dict[str, str] = {}
        for name in sorted(dirfd.listdir(self._dirs[_RECORDS])):
            if name.endswith(".tmp"):
                continue  # an unpublished write; never an accepted record
            stem = name[:-5] if name.endswith(".json") else name
            if not name.endswith(".json") or BROKER_JOB_ID_PATTERN.fullmatch(stem) is None:
                problems[name] = "unexpected entry in the broker record directory"
                continue
            try:
                records.append(self.get(stem))
            except BrokerStoreError as exc:
                problems[stem] = str(exc)
        return records, problems

    def outstanding_charges(self) -> dict[str, BrokerCharge]:
        """Per target host, the broker charges not yet released.

        Raises if any record is unusable: a charge that cannot be read must
        not be counted as free capacity.
        """
        records, problems = self.scan()
        if problems:
            raise BrokerRecordCorrupt(
                "cannot reconstruct broker charges; unusable records: "
                + ", ".join(sorted(problems))
            )
        totals: dict[str, BrokerCharge] = {}
        for record in records:
            for attempt in record.attempts:
                if attempt.charge_released_at is not None:
                    continue
                prior = totals.get(attempt.target_host, BrokerCharge(0, 0, 0))
                totals[attempt.target_host] = BrokerCharge(
                    prior.cpus + attempt.charge.cpus,
                    prior.mem_mb + attempt.charge.mem_mb,
                    prior.job_slots + attempt.charge.job_slots,
                )
        return totals

    def verify_payload(self, record: BrokerRecord) -> Path:
        """Return the payload path after proving its bytes match the record."""
        self._check_open()
        name = f"{record.broker_job_id}.tar"
        try:
            fd = os.open(
                name, _READ_FLAGS, dir_fd=self._dirs[_PAYLOADS]
            )
        except OSError as exc:
            raise BrokerRecordCorrupt(
                f"payload of {record.broker_job_id} is unavailable: {exc}"
            ) from exc
        try:
            st = os.fstat(fd)
            if not stat.S_ISREG(st.st_mode) or st.st_uid != os.geteuid():
                raise BrokerRecordCorrupt(
                    f"payload of {record.broker_job_id} is not a regular file"
                )
            digest = hashlib.sha256()
            size = 0
            while chunk := os.read(fd, _COPY_CHUNK):
                digest.update(chunk)
                size += len(chunk)
        finally:
            os.close(fd)
        if size != record.payload_size or digest.hexdigest() != record.payload_sha256:
            raise BrokerRecordCorrupt(
                f"payload of {record.broker_job_id} does not match its record"
            )
        return self.payload_path(record.broker_job_id)

    # -- writing ------------------------------------------------------

    def _publish(self, record: BrokerRecord, *, create: bool) -> BrokerRecord:
        _check_consistency(record)
        records_fd = self._dirs[_RECORDS]
        name = f"{record.broker_job_id}.json"
        tmp = f"{name}.{secrets.token_hex(4)}.tmp"
        data = _canonical_json(record.to_json())
        if len(data) > _RECORD_MAX_BYTES:
            raise BrokerStoreError(f"broker record {record.broker_job_id} would be too large")
        _write_file_at(records_fd, tmp, data)
        try:
            if create:
                # link() refuses an existing name, so an id collision can
                # never overwrite another job's record.
                os.link(tmp, name, src_dir_fd=records_fd, dst_dir_fd=records_fd)
                os.unlink(tmp, dir_fd=records_fd)
            else:
                os.replace(tmp, name, src_dir_fd=records_fd, dst_dir_fd=records_fd)
        except BaseException:
            _unlink_quietly(records_fd, tmp)
            raise
        try:
            _fsync_dir(records_fd)
        except BaseException:
            if create:
                # Unacknowledged and possibly not durable: withdraw it so the
                # caller's cleanup of the payload cannot orphan a record. An
                # update stays visible; its caller takes no next side effect,
                # and a later revision check refuses the stale view.
                _unlink_quietly(records_fd, name)
            raise
        return record

    def _update(
        self, broker_job_id: str, expected_revision: int, now: float,
        change: Callable[[BrokerRecord, float], BrokerRecord],
    ) -> BrokerRecord:
        self._check_open()
        now = _finite_time(now, "now")
        with self._writer_lock():
            current = self.get(broker_job_id)
            if type(expected_revision) is not int or current.revision != expected_revision:
                raise BrokerRecordConflict(
                    f"broker record {current.broker_job_id} is at revision "
                    f"{current.revision}, not {expected_revision!r}"
                )
            updated = change(current, now)
            return self._publish(
                replace(updated, revision=current.revision + 1, updated_at=now),
                create=False,
            )

    def accept(
        self,
        payload_source: Path,
        intent: BrokerIntent,
        *,
        now: float,
        idempotency_key: str | None = None,
    ) -> BrokerRecord:
        """Durably take a payload archive and its intent into the held store.

        Returns only after both the payload and the ``held`` record are
        published; the caller acknowledges the submission after that and not
        before. A partial or invalid upload leaves no accepted job.

        With ``idempotency_key`` the id is derived from the key, and the
        record check, payload link and record publish run under the writer
        lock. The same key with the same intent and payload bytes returns the
        existing record unchanged; anything else raises
        :class:`BrokerIdempotencyConflict`. A payload left without a record
        by a crash mid-accept is adopted only when its bytes are identical.
        """
        self._check_open()
        now = _finite_time(now, "now")
        if not isinstance(intent, BrokerIntent):
            raise BrokerStoreError("accept needs a BrokerIntent")
        keyed = idempotency_key is not None
        job_id = (
            broker_job_id_for_key(idempotency_key) if keyed else new_broker_job_id()
        )
        staging_fd = self._dirs[_STAGING]
        payloads_fd = self._dirs[_PAYLOADS]
        staged = f"{job_id}.tar.part"
        final = f"{job_id}.tar"
        digest = hashlib.sha256()
        size = 0
        try:
            src_fd = os.open(str(payload_source), _READ_FLAGS)
        except OSError as exc:
            raise BrokerStoreError(f"cannot open payload {payload_source}: {exc}") from exc
        try:
            if not stat.S_ISREG(os.fstat(src_fd).st_mode):
                raise BrokerStoreError("broker payload must be a regular archive file")
            dst_fd = os.open(
                staged,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                0o600,
                dir_fd=staging_fd,
            )
            try:
                while chunk := os.read(src_fd, _COPY_CHUNK):
                    digest.update(chunk)
                    size += len(chunk)
                    view = memoryview(chunk)
                    while view:
                        view = view[os.write(dst_fd, view):]
                os.fsync(dst_fd)
            finally:
                os.close(dst_fd)
        except BaseException:
            _unlink_quietly(staging_fd, staged)
            raise
        finally:
            os.close(src_fd)
        published_payload = False
        try:
            staged_path = self.root / _STAGING / staged
            if not tarfile.is_tarfile(staged_path):
                raise BrokerStoreError("broker payload is not a tar archive")
            from vq import submit

            try:
                submit.validate_staged_python_entrypoint(
                    command=list(intent.command), source_archive=staged_path
                )
            except (OSError, ValueError, tarfile.TarError) as exc:
                raise BrokerStoreError(f"broker payload refused: {exc}") from exc
            record = BrokerRecord(
                broker_job_id=job_id,
                revision=1,
                state=STATE_HELD,
                created_at=now,
                updated_at=now,
                intent=intent,
                payload_sha256=digest.hexdigest(),
                payload_size=size,
            )
            if keyed:
                with self._writer_lock():
                    existing = self._existing_record(job_id)
                    if existing is not None:
                        _unlink_quietly(staging_fd, staged)
                        if (
                            existing.intent == intent
                            and existing.payload_sha256 == record.payload_sha256
                        ):
                            return existing
                        raise BrokerIdempotencyConflict(
                            f"idempotency key already names broker job {job_id} "
                            "with a different command, resources or payload"
                        )
                    self._link_payload(staged, final, adopt_sha256=record.payload_sha256)
                    published_payload = True
                    return self._publish(record, create=True)
            self._link_payload(staged, final, adopt_sha256=None)
            published_payload = True
            # Under the writer lock: should the directory fsync fail, the new
            # record is withdrawn before any other process's transition (a
            # pass's reserve) can act on it.
            with self._writer_lock():
                return self._publish(record, create=True)
        except BaseException:
            # No record means no accepted job: withdraw the payload too.
            if published_payload:
                _unlink_quietly(payloads_fd, final)
            else:
                _unlink_quietly(staging_fd, staged)
            raise

    def _existing_record(self, job_id: str) -> BrokerRecord | None:
        try:
            return self.get(job_id)
        except BrokerRecordCorrupt:
            raise
        except BrokerStoreError:
            return None

    def _link_payload(self, staged: str, final: str, *, adopt_sha256: str | None) -> None:
        """Publish a staged payload; never replace an existing one.

        link() refuses an existing name, so an id collision can never replace
        (and then, on cleanup, delete) another job's payload. Only a keyed
        accept, holding the writer lock and having found no record, may
        adopt an existing payload, and only when its bytes are identical.
        """
        staging_fd = self._dirs[_STAGING]
        payloads_fd = self._dirs[_PAYLOADS]
        try:
            os.link(staged, final, src_dir_fd=staging_fd, dst_dir_fd=payloads_fd)
        except FileExistsError as exc:
            if adopt_sha256 is None or self._payload_sha256(final) != adopt_sha256:
                raise BrokerStoreError(
                    f"broker job id {final.removesuffix('.tar')} collides with an "
                    "existing payload"
                ) from exc
        os.unlink(staged, dir_fd=staging_fd)
        _fsync_dir(payloads_fd)
        _fsync_dir(staging_fd)

    def _payload_sha256(self, name: str) -> str | None:
        try:
            fd = os.open(name, _READ_FLAGS, dir_fd=self._dirs[_PAYLOADS])
        except OSError:
            return None
        try:
            st = os.fstat(fd)
            if not stat.S_ISREG(st.st_mode) or st.st_uid != os.geteuid():
                return None
            digest = hashlib.sha256()
            while chunk := os.read(fd, _COPY_CHUNK):
                digest.update(chunk)
            return digest.hexdigest()
        finally:
            os.close(fd)

    def record_held_blockers(
        self, broker_job_id: str, expected_revision: int, *,
        blockers: tuple[tuple[str, str], ...], now: float,
    ) -> BrokerRecord:
        """Save the latest held decision without creating an attempt or charge."""
        def change(record: BrokerRecord, now: float) -> BrokerRecord:
            if record.state != STATE_HELD or record.cancellation is not None:
                raise BrokerRecordConflict("cannot record blockers for a job no longer held")
            candidate = replace(record, held_blockers=blockers)
            # Use the same validation as a fresh process reading this record.
            parse_record(candidate.to_json())
            return candidate

        return self._update(broker_job_id, expected_revision, now, change)

    def reserve(
        self,
        broker_job_id: str,
        expected_revision: int,
        *,
        decision: overview.BrokerPlacementDecision,
        snapshot: overview.BrokerCapacitySnapshot,
        target_state_root: str,
        now: float,
    ) -> BrokerRecord:
        """``held`` → ``reserved``: record the chosen target and its charge."""
        if decision.host is None or snapshot.host != decision.host:
            raise BrokerStoreError("reserve needs the snapshot of the decision's chosen host")
        if not isinstance(target_state_root, str) or not target_state_root.startswith("/"):
            raise BrokerStoreError("target state root must be an absolute path")

        def change(record: BrokerRecord, now: float) -> BrokerRecord:
            if record.state != STATE_HELD:
                raise BrokerRecordConflict(f"cannot reserve a {record.state} broker job")
            if record.cancellation is not None:
                raise BrokerRecordConflict("cannot reserve a broker job with a cancellation")
            snapshot_json = {
                "host": snapshot.host,
                "observed_at": snapshot.observed_at,
                "free_cpus": snapshot.free_cpus,
                "free_mem_mb": snapshot.free_mem_mb,
                "free_jobs": snapshot.free_jobs,
                "pending_cpus": snapshot.pending_cpus,
                "idle_seconds": snapshot.idle_seconds,
                "pools": sorted(snapshot.pools),
                "max_wall_time_seconds": snapshot.max_wall_time_seconds,
            }
            if snapshot.authority is not None:
                snapshot_json["authority"] = snapshot.authority
            if snapshot.runtime_pins is not None:
                snapshot_json["runtime_pins"] = snapshot.runtime_pins
            attempt = BrokerAttempt(
                number=len(record.attempts) + 1,
                phase=STATE_RESERVED,
                target_host=snapshot.host,
                target_state_root=target_state_root,
                reservation_id=f"{record.broker_job_id}.{len(record.attempts) + 1}",
                charge=BrokerCharge(record.intent.cpus, record.intent.mem_mb, 1),
                decided_at=now,
                snapshot=snapshot_json,
                ranking=[list(rank) for rank in decision.ranking],
                blockers=dict(decision.blockers),
                idempotency_key=record.broker_job_id,
            )
            if len(record.attempts) >= _ATTEMPTS_MAX:
                raise BrokerStoreError("broker job has too many placement attempts")
            return replace(record, state=STATE_RESERVED, attempts=(*record.attempts, attempt),
                           held_blockers=())

        return self._update(broker_job_id, expected_revision, now, change)

    def _replace_attempt(self, record: BrokerRecord, attempt: BrokerAttempt) -> BrokerRecord:
        return replace(record, attempts=(*record.attempts[:-1], attempt))

    def release_reservation(
        self, broker_job_id: str, expected_revision: int, *, reason: str, now: float
    ) -> BrokerRecord:
        """``reserved`` → ``returned``: drop an attempt whose transfer never began."""
        reason = _parse_reason(reason)

        def change(record: BrokerRecord, now: float) -> BrokerRecord:
            if record.state != STATE_RESERVED:
                raise BrokerRecordConflict(
                    f"only an unstarted reservation can be released, not a {record.state} job"
                )
            attempt = replace(
                record.attempts[-1], phase=STATE_RETURNED, returned_at=now,
                return_reason=reason, charge_released_at=now,
            )
            return replace(self._replace_attempt(record, attempt), state=STATE_RETURNED)

        return self._update(broker_job_id, expected_revision, now, change)

    def fence_transfer(
        self, broker_job_id: str, expected_revision: int, *, proof: str, now: float
    ) -> BrokerRecord:
        """``transferring`` → ``returned`` on a target fence, charge released (#64).

        ``proof`` describes the target's fence answer; it is recorded as the
        attempt's return reason under :data:`FENCED_RETURN_PREFIX`. The caller
        has checked that the fence ran on the attempt's own target and state
        root for this record's idempotency key.
        """
        proof = _parse_reason(proof)

        def change(record: BrokerRecord, now: float) -> BrokerRecord:
            if record.state != STATE_TRANSFERRING:
                raise BrokerRecordConflict(
                    f"only a transfer in doubt can be fenced, not a {record.state} job"
                )
            attempt = replace(
                record.attempts[-1], phase=STATE_RETURNED, returned_at=now,
                return_reason=FENCED_RETURN_PREFIX + proof, charge_released_at=now,
            )
            return replace(self._replace_attempt(record, attempt), state=STATE_RETURNED)

        return self._update(broker_job_id, expected_revision, now, change)

    def resume_holding(
        self, broker_job_id: str, expected_revision: int, *, now: float
    ) -> BrokerRecord:
        """``returned`` → ``held``, or → ``cancelled`` if cancellation was requested."""

        def change(record: BrokerRecord, now: float) -> BrokerRecord:
            if record.state != STATE_RETURNED:
                raise BrokerRecordConflict(f"cannot resume holding a {record.state} broker job")
            if record.cancellation is not None:
                return replace(
                    record, state=STATE_CANCELLED,
                    cancellation=replace(record.cancellation, completed_at=now),
                )
            return replace(record, state=STATE_HELD)

        return self._update(broker_job_id, expected_revision, now, change)

    def begin_transfer(
        self, broker_job_id: str, expected_revision: int, *, now: float
    ) -> BrokerRecord:
        """``reserved`` → ``transferring``, written before any remote submit.

        Freezes the transfer fingerprint. From here the attempt is uncertain
        until an acceptance is recorded; it can no longer be released.
        """

        def change(record: BrokerRecord, now: float) -> BrokerRecord:
            if record.state != STATE_RESERVED:
                raise BrokerRecordConflict(f"cannot begin a transfer from a {record.state} job")
            if record.cancellation is not None:
                raise BrokerRecordConflict(
                    "cancellation was requested; release the reservation instead"
                )
            attempt = record.attempts[-1]
            attempt = replace(
                attempt, phase=STATE_TRANSFERRING, transfer_started_at=now,
                request_fingerprint=transfer_fingerprint(record, attempt),
            )
            return replace(self._replace_attempt(record, attempt), state=STATE_TRANSFERRING)

        return self._update(broker_job_id, expected_revision, now, change)

    def record_transfer_diagnostic(
        self, broker_job_id: str, expected_revision: int, *, message: str, now: float
    ) -> BrokerRecord:
        """Keep an uncertain transfer's evidence; the job stays ``transferring``."""
        message = _parse_reason(message)

        def change(record: BrokerRecord, now: float) -> BrokerRecord:
            if record.state != STATE_TRANSFERRING:
                raise BrokerRecordConflict(f"no transfer in progress for a {record.state} job")
            attempt = record.attempts[-1]
            diagnostics = (*attempt.diagnostics, message)[-_DIAGNOSTICS_MAX:]
            return self._replace_attempt(record, replace(attempt, diagnostics=diagnostics))

        return self._update(broker_job_id, expected_revision, now, change)

    def record_placed(
        self,
        broker_job_id: str,
        expected_revision: int,
        *,
        request_fingerprint: str,
        target_job_id: str,
        now: float,
    ) -> BrokerRecord:
        """``transferring`` → ``placed`` on a verified target acceptance.

        The charge stays outstanding until :meth:`settle_charge`, once a
        refreshed target snapshot demonstrably includes the accepted job.
        """
        try:
            target_job_id = validate_job_id(target_job_id)
        except (TypeError, ValueError) as exc:
            raise BrokerStoreError(str(exc)) from exc

        def change(record: BrokerRecord, now: float) -> BrokerRecord:
            if record.state != STATE_TRANSFERRING:
                raise BrokerRecordConflict(f"cannot record placement of a {record.state} job")
            attempt = record.attempts[-1]
            if request_fingerprint != attempt.request_fingerprint:
                raise BrokerRecordConflict("acceptance is for a different transfer request")
            attempt = replace(
                attempt, phase=STATE_PLACED, target_job_id=target_job_id, accepted_at=now
            )
            return replace(self._replace_attempt(record, attempt), state=STATE_PLACED)

        return self._update(broker_job_id, expected_revision, now, change)

    def settle_charge(
        self, broker_job_id: str, expected_revision: int, *, now: float
    ) -> BrokerRecord:
        """Release a placed job's charge once target state accounts for it."""

        def change(record: BrokerRecord, now: float) -> BrokerRecord:
            if record.state != STATE_PLACED:
                raise BrokerRecordConflict(f"cannot settle the charge of a {record.state} job")
            attempt = record.attempts[-1]
            if attempt.charge_released_at is not None:
                raise BrokerRecordConflict("the charge was already settled")
            return self._replace_attempt(record, replace(attempt, charge_released_at=now))

        return self._update(broker_job_id, expected_revision, now, change)

    def request_cancel(
        self, broker_job_id: str, expected_revision: int, *, reason: str, now: float
    ) -> BrokerRecord:
        """Record cancellation intent; complete it locally for a held job.

        A reserved job keeps its intent until the reservation is released; a
        transferring or placed job keeps it for reconciliation and a target
        kill (B4/B5). Nothing here reports a target job as killed.
        """
        reason = _parse_reason(reason)

        def change(record: BrokerRecord, now: float) -> BrokerRecord:
            if record.state == STATE_CANCELLED or record.cancellation is not None:
                raise BrokerRecordConflict("cancellation was already requested")
            cancellation = BrokerCancellation(requested_at=now, reason=reason)
            if record.state == STATE_HELD:
                return replace(
                    record, state=STATE_CANCELLED,
                    cancellation=replace(cancellation, completed_at=now),
                )
            return replace(record, cancellation=cancellation)

        return self._update(broker_job_id, expected_revision, now, change)

    def transfer_request(self, record: BrokerRecord) -> BrokerTransferRequest:
        """Build the keyed submit for a ``transferring`` record.

        Refuses when the stored fingerprint no longer matches the record's
        target, intent and payload, or when the payload bytes changed: a
        changed intent is a conflict, never a new submission.
        """
        current = self.get(record.broker_job_id)
        if current != record:
            raise BrokerRecordConflict("broker record changed since it was read")
        if current.state != STATE_TRANSFERRING:
            raise BrokerRecordConflict(f"no transfer to replay for a {current.state} job")
        attempt = current.attempts[-1]
        if attempt.request_fingerprint != transfer_fingerprint(current, attempt):
            raise BrokerRecordConflict("transfer intent changed after the transfer began")
        payload_path = self.verify_payload(current)
        intent = current.intent
        submit_kwargs: dict[str, Any] = {
            "archive": str(payload_path),
            "command": list(intent.command),
            "cpus": intent.cpus,
            "mem_mb": intent.mem_mb,
            "wall_time_seconds": intent.wall_time_seconds,
            "priority": intent.priority,
            "auto_resume": intent.auto_resume,
            "retry": intent.retry,
            "job_name": intent.job_name,
            "program": intent.program,
            "expected_sha": intent.expected_sha or (
                attempt.snapshot.get("runtime_pins", {}).get(intent.program)
                if intent.program else None
            ),
            "tags": list(intent.tags),
            "clean_workdir_on_terminal": intent.clean_workdir_on_terminal,
            "rerun_until_file_exists": intent.rerun_until_file_exists,
            "rerun_max": intent.rerun_max,
            "idempotency_key": attempt.idempotency_key,
        }
        return BrokerTransferRequest(
            broker_job_id=current.broker_job_id,
            target_host=attempt.target_host,
            target_state_root=attempt.target_state_root,
            idempotency_key=attempt.idempotency_key,
            request_fingerprint=attempt.request_fingerprint,
            payload_path=payload_path,
            submit_kwargs=submit_kwargs,
            authority=attempt.snapshot.get("authority"),
        )


def _parse_reason(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise BrokerStoreError("a reason must be a non-empty string")
    return value[:_DIAGNOSTIC_MAX_CHARS]
