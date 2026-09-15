"""Pure state transitions for crash-safe legacy failure reconciliation.

This module deliberately knows nothing about rollout execution, report
discovery, journal storage, or live drain control.  Callers authenticate those
inputs first, then apply the returned compare-and-swap effects in order.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import InitVar, dataclass, field, fields, is_dataclass
from datetime import datetime
from enum import StrEnum
from threading import RLock
from weakref import ReferenceType, ref

from vq.report_paths import same_report

FAILURE_UPDATE_INTENT_SCHEMA = "vq.fleet.legacy_failure_update_intent/2"
FAILURE_UPDATE_BACKLINK_SCHEMA = "vq.fleet.legacy_failure_update_ack/1"
FAILURE_UPDATE_ACTIONS_SCHEMA = "vq.fleet.legacy_failure_update_actions/2"

_SHA256 = re.compile(r"[0-9a-f]{64}")
_SHA1 = re.compile(r"[0-9a-f]{40}")
_NO_VALUE = object()
_PLANNER_EFFECT_SEALED = object()
_PLANNER_TRANSITION_BUILD_TOKEN = object()
_PLANNER_TRANSITION_SEALED = object()
_RECOVERY_INPUT_SEALED = object()


class _PlannerSealed:
    __slots__ = ("_planner_seal", "_planner_payload_sha256", "__weakref__")


_AuthorityEntry = tuple[ReferenceType[object], type[object], str]
_AUTHORITY_REGISTRY: dict[int, _AuthorityEntry] = {}
_AUTHORITY_REGISTRY_LOCK = RLock()


def _register_authority(value: object, digest: str) -> None:
    key = id(value)

    def remove_if_current(dead_ref: ReferenceType[object]) -> None:
        with _AUTHORITY_REGISTRY_LOCK:
            current = _AUTHORITY_REGISTRY.get(key)
            if current is not None and current[0] is dead_ref:
                del _AUTHORITY_REGISTRY[key]

    authority_ref = ref(value, remove_if_current)
    with _AUTHORITY_REGISTRY_LOCK:
        _AUTHORITY_REGISTRY[key] = (authority_ref, type(value), digest)


def _require_registered_authority(value: object, digest: str) -> None:
    if type(digest) is not str:
        raise TransitionError("registered authority digest must be an exact string")
    with _AUTHORITY_REGISTRY_LOCK:
        entry = _AUTHORITY_REGISTRY.get(id(value))
        if (
            entry is None
            or entry[0]() is not value
            or entry[1] is not type(value)
            or type(entry[2]) is not str
            or entry[2] != digest
        ):
            raise TransitionError("planner provenance is not registered for this exact object")


class _FrozenJSONMapping(tuple[tuple[str, object], ...], Mapping[str, object]):
    """Small recursively immutable mapping for authenticated JSON values."""

    __slots__ = ()

    def __new__(
        cls,
        items: tuple[tuple[str, object], ...],
    ) -> _FrozenJSONMapping:
        return tuple.__new__(cls, items)

    @property
    def _items(self) -> tuple[tuple[str, object], ...]:
        return tuple(tuple.__iter__(self))

    def __getitem__(self, key: str) -> object:
        for item_key, value in tuple.__iter__(self):
            if item_key == key:
                return value
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        return (key for key, _ in tuple.__iter__(self))

    def __len__(self) -> int:
        return tuple.__len__(self)

    def __contains__(self, key: object) -> bool:
        return any(item_key == key for item_key, _ in tuple.__iter__(self))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Mapping):
            return dict(self.items()) == dict(other.items())
        return False

    def __ne__(self, other: object) -> bool:
        return not self == other

    def __repr__(self) -> str:
        return f"_FrozenJSONMapping({dict(self.items())!r})"

    __hash__ = None


def _freeze_json(value: object) -> object:
    if isinstance(value, _FrozenJSONMapping):
        return value
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise TransitionError("JSON mapping keys must be strings")
        frozen = _FrozenJSONMapping(tuple((key, _freeze_json(item)) for key, item in value.items()))
        canonical_json_sha256(frozen)
        return frozen
    if isinstance(value, (list, tuple)):
        frozen_items = tuple(_freeze_json(item) for item in value)
        canonical_json_sha256(frozen_items)
        return frozen_items
    if value is None or type(value) in {str, int, float, bool}:
        canonical_json_sha256(value)
        return value
    raise TransitionError(f"value is not canonical-JSON encodable: {type(value).__name__}")


def thaw_json(value: object) -> object:
    """Return a fresh mutable JSON value for the adapter persistence boundary."""
    if isinstance(value, Mapping):
        return {key: thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [thaw_json(item) for item in value]
    if isinstance(value, list):
        return [thaw_json(item) for item in value]
    return value


def _freeze_mapping(value: Mapping[str, object], label: str) -> _FrozenJSONMapping:
    frozen = _freeze_json(value)
    if not isinstance(frozen, _FrozenJSONMapping):
        raise TransitionError(f"{label} must be a mapping")
    return frozen


class TransitionError(ValueError):
    """The supplied snapshots cannot form a safe pure transition."""


class SkipState(StrEnum):
    """Classification of an older host-local success skip."""

    ABSENT = "absent"
    CURRENT = "current"
    STALE = "stale"


class CurrentAckState(StrEnum):
    """Classification of the current journal's forward acknowledgement."""

    ABSENT = "absent"
    EXACT = "exact"
    CONFLICT = "conflict"


class TransitionPhase(StrEnum):
    """Next crash-safe persistence phase for one host group."""

    PREVALIDATED = "prevalidated"
    ACKED = "acked"
    BACKLINKING = "backlinking"
    CONSUMING = "consuming"
    COMPLETE = "complete"


class HoldProofKind(StrEnum):
    """Authenticated origin of a settled historical hold."""

    NONE = "none"
    OBSERVED_INACTIVE = "observed-inactive"
    CONDITIONAL_ABSENCE_CONFIRMED = "conditional-absence-confirmed"
    EXISTING_BACKLINK = "existing-backlink"


@dataclass(frozen=True, slots=True)
class ReportRef:
    """Stable identity of one accepted report and its rollout journal."""

    source_path: str
    digest_sha256: str
    rollout_id: str

    def __post_init__(self) -> None:
        _require_text(self.source_path, "report source_path")
        _require_sha256(self.digest_sha256, "report digest_sha256")
        _require_text(self.rollout_id, "report rollout_id")


@dataclass(frozen=True, slots=True)
class LatestReportRef:
    """Capability proving a report was discovered as the latest report."""

    ref: ReportRef
    discovery_token: str

    def __post_init__(self) -> None:
        if type(self.ref) is not ReportRef:
            raise TypeError("latest report ref must be an exact ReportRef")
        _require_text(self.discovery_token, "latest report discovery_token")


@dataclass(frozen=True, slots=True)
class ReceiptReportRef:
    """Opaque adapter-authenticated capability for one accepted report."""

    ref: ReportRef
    authentication_token: str

    def __post_init__(self) -> None:
        if type(self.ref) is not ReportRef:
            raise TypeError("receipt report ref must be an exact ReportRef")
        _require_text(self.authentication_token, "receipt report authentication_token")


@dataclass(frozen=True, slots=True)
class PlanActionRef:
    """Complete immutable identity of one action in a host plan."""

    action_id: str
    phase: str
    host: str
    program: str
    decision: str
    reason: str
    pin_name: str
    target_sha: str
    target_version: str | None
    target_tag: str | None
    argv: tuple[str, ...]
    before: Mapping[str, object]

    def __post_init__(self) -> None:
        for name in ("action_id", "host", "program", "reason", "pin_name"):
            _require_text(getattr(self, name), f"plan action {name}")
        if type(self.phase) is not str or self.phase not in {
            "driver",
            "helper",
            "scheduler-runtime",
            "local-runtime",
        }:
            raise TransitionError("plan action phase is invalid")
        if type(self.decision) is not str or self.decision not in {
            "update",
            "skip",
            "defer",
            "block",
        }:
            raise TransitionError("plan action decision is invalid")
        _require_sha1(self.target_sha, "plan action target_sha")
        if self.target_version is not None:
            _require_text(self.target_version, "plan action target_version")
        if self.target_tag is not None:
            _require_text(self.target_tag, "plan action target_tag")
        if not isinstance(self.argv, tuple) or not all(
            isinstance(item, str) and item for item in self.argv
        ):
            raise TransitionError("plan action argv must be a tuple of nonempty strings")
        if not isinstance(self.before, Mapping):
            raise TransitionError("plan action before must be a mapping")
        object.__setattr__(self, "before", _normalize_before(self.before))


@dataclass(frozen=True, slots=True)
class FailureAttemptRef:
    """One exact durable failure fence in a historical action row."""

    action_id: str
    operation_id: str
    consumed: bool = False
    retry_authorized: bool = False

    def __post_init__(self) -> None:
        _require_text(self.action_id, "failure attempt action_id")
        _require_sha256(self.operation_id, "failure attempt operation_id")
        if type(self.consumed) is not bool or type(self.retry_authorized) is not bool:
            raise TransitionError("failure attempt fence flags must be bool")
        if self.retry_authorized:
            raise TransitionError("legacy failure transition cannot authorize retry")


@dataclass(frozen=True, slots=True)
class HostFailureGroup:
    """All failed attempts from one historical rollout on one host."""

    report: ReportRef
    host: str
    reason: str
    attempts: tuple[FailureAttemptRef, ...]
    skip_states: tuple[tuple[str, SkipState], ...] = ()

    def __post_init__(self) -> None:
        if type(self.report) is not ReportRef:
            raise TypeError("failure group report must be an exact ReportRef")
        _require_text(self.host, "failure host")
        _require_text(self.reason, "failure reason")
        if not isinstance(self.attempts, tuple) or not self.attempts:
            raise TransitionError("failure group attempts must be nonempty")
        if any(not isinstance(item, FailureAttemptRef) for item in self.attempts):
            raise TransitionError("failure group attempts must be FailureAttemptRef records")
        action_ids = {item.action_id for item in self.attempts}
        operation_ids = [item.operation_id for item in self.attempts]
        if len(operation_ids) != len(set(operation_ids)):
            raise TransitionError("failure group operation IDs must be unique")
        normalized_states: list[tuple[str, SkipState]] = []
        for action_id, state in self.skip_states:
            if action_id not in action_ids or not isinstance(state, SkipState):
                raise TransitionError("failure group has invalid skip classification")
            normalized_states.append((action_id, state))
        if len(normalized_states) != len({item[0] for item in normalized_states}):
            raise TransitionError("failure group skip action IDs must be unique")
        if self.host == "" or any(item.action_id == "" for item in self.attempts):
            raise TransitionError("failure group identity must be nonempty")

    @property
    def failed_action_ids(self) -> tuple[str, ...]:
        return tuple(sorted({item.action_id for item in self.attempts}))

    @property
    def failed_operation_ids(self) -> tuple[str, ...]:
        return tuple(sorted({item.operation_id for item in self.attempts}))

    @property
    def pending_attempts(self) -> tuple[FailureAttemptRef, ...]:
        return tuple(item for item in self.attempts if not item.consumed)

    def skip_state(self, action_id: str) -> SkipState:
        return dict(self.skip_states).get(action_id, SkipState.ABSENT)


@dataclass(frozen=True, slots=True)
class JournalToken:
    """Whole-journal compare-and-swap identity."""

    rollout_id: str
    digest_sha256: str | None

    def __post_init__(self) -> None:
        _require_text(self.rollout_id, "journal token rollout_id")
        if self.digest_sha256 is not None:
            _require_sha256(self.digest_sha256, "journal token digest_sha256")


@dataclass(frozen=True, slots=True)
class MutationVector:
    """Preauthenticated adapter capability for every invocation-wide CAS token."""

    latest: LatestReportRef
    journals: tuple[JournalToken, ...]

    def __post_init__(self) -> None:
        _require_latest(self.latest)
        if not isinstance(self.journals, tuple) or any(
            type(item) is not JournalToken for item in self.journals
        ):
            raise TransitionError("mutation vector journals must be JournalToken records")
        rollout_ids = [item.rollout_id for item in self.journals]
        if rollout_ids != sorted(rollout_ids) or len(rollout_ids) != len(set(rollout_ids)):
            raise TransitionError("mutation vector journals must be sorted and unique")

    def token_for(self, rollout_id: str) -> JournalToken:
        for token in self.journals:
            if token.rollout_id == rollout_id:
                return token
        raise TransitionError(f"mutation vector lacks journal {rollout_id}")


@dataclass(frozen=True, slots=True)
class JournalHostSnapshot:
    """Host-local projection paired with a whole-journal CAS token."""

    report: ReportRef
    host: str
    token: JournalToken
    relevant_action_records: Mapping[str, object]
    hold: Mapping[str, object] | None = None
    retained_hold: Mapping[str, object] | None = None
    failed_reason: str | None = None
    complete: bool = False
    forward_intent: object | None = None

    def __post_init__(self) -> None:
        if type(self.report) is not ReportRef:
            raise TypeError("journal snapshot report must be an exact ReportRef")
        if type(self.token) is not JournalToken:
            raise TypeError("journal snapshot token must be an exact JournalToken")
        _require_text(self.host, "journal snapshot host")
        if self.token.rollout_id != self.report.rollout_id:
            raise TransitionError("journal snapshot token binds another rollout")
        if self.token.digest_sha256 is None:
            raise TransitionError("present journal snapshot requires a digest")
        if not isinstance(self.relevant_action_records, Mapping):
            raise TransitionError("journal action records must be a mapping")
        if self.hold is not None and not isinstance(self.hold, Mapping):
            raise TransitionError("journal hold must be a mapping or None")
        if self.retained_hold is not None and not isinstance(self.retained_hold, Mapping):
            raise TransitionError("journal retained hold must be a mapping or None")
        if self.failed_reason is not None:
            _require_text(self.failed_reason, "journal failed reason")
        if type(self.complete) is not bool:
            raise TransitionError("journal complete must be bool")
        object.__setattr__(
            self,
            "relevant_action_records",
            _freeze_mapping(self.relevant_action_records, "journal action records"),
        )
        if self.hold is not None:
            object.__setattr__(self, "hold", _freeze_mapping(self.hold, "journal hold"))
        if self.retained_hold is not None:
            object.__setattr__(
                self,
                "retained_hold",
                _freeze_mapping(self.retained_hold, "journal retained hold"),
            )
        if self.forward_intent is not None:
            if not isinstance(self.forward_intent, Mapping):
                raise TransitionError("journal forward intent must be a mapping or None")
            object.__setattr__(
                self,
                "forward_intent",
                _freeze_mapping(self.forward_intent, "journal forward intent"),
            )


@dataclass(frozen=True, slots=True)
class HoldProof:
    """Pure authenticated settlement result for one historical hold."""

    old_rollout_id: str
    kind: HoldProofKind
    settled_hold: Mapping[str, object] | None
    settled_hold_sha256: str

    def __post_init__(self) -> None:
        _require_text(self.old_rollout_id, "hold proof old_rollout_id")
        if not isinstance(self.kind, HoldProofKind):
            raise TransitionError("hold proof kind is invalid")
        _require_sha256(self.settled_hold_sha256, "hold proof settled_hold_sha256")
        if self.settled_hold is None:
            raise TransitionError("hold proof requires explicit settled hold evidence")
        frozen = _freeze_mapping(self.settled_hold, "settled hold proof")
        object.__setattr__(self, "settled_hold", frozen)
        if canonical_json_sha256(frozen) != self.settled_hold_sha256:
            raise TransitionError("settled hold does not match its proof digest")


@dataclass(frozen=True, slots=True)
class FailureMember:
    """Closed source record embedded in one forward intent."""

    historical_rollout_id: str
    historical_report_path: str
    historical_report_digest_sha256: str
    failed_action_ids: tuple[str, ...]
    failed_operation_ids: tuple[str, ...]
    failure_reason: str
    settled_hold_sha256: str

    def __post_init__(self) -> None:
        _require_text(self.historical_rollout_id, "failure member rollout_id")
        _require_text(self.historical_report_path, "failure member report path")
        _require_sha256(
            self.historical_report_digest_sha256,
            "failure member report digest",
        )
        _require_ids(self.failed_action_ids, "failure member action IDs", sha=False)
        _require_ids(self.failed_operation_ids, "failure member operation IDs", sha=True)
        _require_text(self.failure_reason, "failure member reason")
        _require_sha256(self.settled_hold_sha256, "failure member settled hold digest")

    def as_dict(self) -> dict[str, object]:
        return {
            "historical_rollout_id": self.historical_rollout_id,
            "historical_report_path": self.historical_report_path,
            "historical_report_digest_sha256": self.historical_report_digest_sha256,
            "failed_action_ids": list(self.failed_action_ids),
            "failed_operation_ids": list(self.failed_operation_ids),
            "failure_reason": self.failure_reason,
            "settled_hold_sha256": self.settled_hold_sha256,
        }


@dataclass(frozen=True, slots=True)
class FailureUpdateIntent:
    """Current-journal forward intent written before historical backlinks."""

    schema: str
    host: str
    current_report_path: str
    current_report_digest_sha256: str
    current_rollout_id: str
    current_action_ids: tuple[str, ...]
    current_plan_projection: Mapping[str, object]
    current_actions_sha256: str
    current_ack_sha256: str
    failed_host_reason: str
    sources: tuple[FailureMember, ...]
    recorded_at: str

    def __post_init__(self) -> None:
        if self.schema != FAILURE_UPDATE_INTENT_SCHEMA:
            raise TransitionError("failure update intent schema is invalid")
        _require_text(self.host, "failure update intent host")
        _require_text(self.current_report_path, "failure update intent report path")
        _require_sha256(self.current_report_digest_sha256, "intent report digest")
        _require_text(self.current_rollout_id, "failure update intent rollout_id")
        _require_ids(self.current_action_ids, "intent current action IDs", sha=False)
        projection = _validate_plan_projection(
            self.current_plan_projection,
            host=self.host,
        )
        object.__setattr__(self, "current_plan_projection", projection)
        _require_sha256(self.current_actions_sha256, "intent current actions digest")
        _require_sha256(self.current_ack_sha256, "intent current ack digest")
        _require_text(self.failed_host_reason, "intent failed host reason")
        if not self.sources or any(not isinstance(item, FailureMember) for item in self.sources):
            raise TransitionError("intent sources must be nonempty FailureMember records")
        source_ids = [item.historical_rollout_id for item in self.sources]
        if source_ids != sorted(source_ids) or len(source_ids) != len(set(source_ids)):
            raise TransitionError("intent sources must be sorted and unique by rollout")
        _require_aware_timestamp(self.recorded_at, "intent recorded_at")
        projection_rows = projection["actions"]
        assert isinstance(projection_rows, tuple)
        update_ids = tuple(
            row["action_id"]
            for row in projection_rows
            if isinstance(row, Mapping) and row["decision"] == "update"
        )
        if self.current_action_ids != update_ids:
            raise TransitionError("intent update IDs conflict with persisted projection")
        if canonical_json_sha256(projection) != self.current_actions_sha256:
            raise TransitionError("intent plan projection digest is inconsistent")
        if self.failed_host_reason != self.sources[-1].failure_reason:
            raise TransitionError("intent failed reason conflicts with sorted sources")
        rows = _rows_from_intent(self)
        if _current_ack_sha256(self.host, self.failed_host_reason, rows) != (
            self.current_ack_sha256
        ):
            raise TransitionError("intent current acknowledgement digest is inconsistent")

    def as_dict(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "host": self.host,
            "current_report_path": self.current_report_path,
            "current_report_digest_sha256": self.current_report_digest_sha256,
            "current_rollout_id": self.current_rollout_id,
            "current_action_ids": list(self.current_action_ids),
            "current_plan_projection": thaw_json(self.current_plan_projection),
            "current_actions_sha256": self.current_actions_sha256,
            "current_ack_sha256": self.current_ack_sha256,
            "failed_host_reason": self.failed_host_reason,
            "sources": [item.as_dict() for item in self.sources],
            "recorded_at": self.recorded_at,
        }


@dataclass(frozen=True, slots=True)
class FailureUpdateBacklink:
    """Closed 13-field historical backlink derived from one intent source."""

    schema: str
    host: str
    failed_action_ids: tuple[str, ...]
    failed_operation_ids: tuple[str, ...]
    historical_report_path: str
    historical_report_digest_sha256: str
    settled_hold_sha256: str
    current_report_path: str
    current_report_digest_sha256: str
    current_rollout_id: str
    current_action_ids: tuple[str, ...]
    current_actions_sha256: str
    recorded_at: str

    def __post_init__(self) -> None:
        if self.schema != FAILURE_UPDATE_BACKLINK_SCHEMA:
            raise TransitionError("failure update backlink schema is invalid")
        _require_text(self.host, "failure update backlink host")
        _require_ids(self.failed_action_ids, "backlink failed action IDs", sha=False)
        _require_ids(self.failed_operation_ids, "backlink failed operation IDs", sha=True)
        _require_text(self.historical_report_path, "backlink historical report path")
        _require_sha256(self.historical_report_digest_sha256, "backlink historical digest")
        _require_sha256(self.settled_hold_sha256, "backlink settled hold digest")
        _require_text(self.current_report_path, "backlink current report path")
        _require_sha256(self.current_report_digest_sha256, "backlink current digest")
        _require_text(self.current_rollout_id, "backlink current rollout_id")
        _require_ids(self.current_action_ids, "backlink current action IDs", sha=False)
        _require_sha256(self.current_actions_sha256, "backlink current actions digest")
        _require_aware_timestamp(self.recorded_at, "backlink recorded_at")

    def as_dict(self) -> dict[str, object]:
        return _backlink_dict(self)


def _backlink_dict(backlink: FailureUpdateBacklink) -> dict[str, object]:
    return {
        "schema": object.__getattribute__(backlink, "schema"),
        "host": object.__getattribute__(backlink, "host"),
        "failed_action_ids": list(object.__getattribute__(backlink, "failed_action_ids")),
        "failed_operation_ids": list(object.__getattribute__(backlink, "failed_operation_ids")),
        "historical_report_path": object.__getattribute__(backlink, "historical_report_path"),
        "historical_report_digest_sha256": object.__getattribute__(
            backlink, "historical_report_digest_sha256"
        ),
        "settled_hold_sha256": object.__getattribute__(backlink, "settled_hold_sha256"),
        "current_report_path": object.__getattribute__(backlink, "current_report_path"),
        "current_report_digest_sha256": object.__getattribute__(
            backlink, "current_report_digest_sha256"
        ),
        "current_rollout_id": object.__getattribute__(backlink, "current_rollout_id"),
        "current_action_ids": list(object.__getattribute__(backlink, "current_action_ids")),
        "current_actions_sha256": object.__getattribute__(backlink, "current_actions_sha256"),
        "recorded_at": object.__getattribute__(backlink, "recorded_at"),
    }


@dataclass(frozen=True, slots=True)
class ActionEdit:
    """Pure edit of one historical action record."""

    action_id: str
    delete_keys: tuple[str, ...] = ()
    set_items: tuple[tuple[str, object], ...] = ()

    def __post_init__(self) -> None:
        _require_text(self.action_id, "action edit action_id")
        if not isinstance(self.delete_keys, tuple) or not all(
            isinstance(item, str) and item for item in self.delete_keys
        ):
            raise TransitionError("action edit delete keys must be nonempty strings")
        if len(self.delete_keys) != len(set(self.delete_keys)):
            raise TransitionError("action edit delete keys must be unique")
        if not isinstance(self.set_items, tuple) or any(
            not isinstance(item, tuple)
            or len(item) != 2
            or not isinstance(item[0], str)
            or not item[0]
            for item in self.set_items
        ):
            raise TransitionError("action edit set items must be key/value tuples")
        set_keys = [item[0] for item in self.set_items]
        if len(set_keys) != len(set(set_keys)) or set(self.delete_keys) & set(set_keys):
            raise TransitionError("action edit keys must be disjoint and unique")
        object.__setattr__(
            self,
            "set_items",
            tuple((key, _freeze_json(value)) for key, value in self.set_items),
        )


@dataclass(frozen=True, slots=True)
class WriteCurrentAckAndIntent(_PlannerSealed):
    """Atomic current-journal acknowledgement and forward-intent write."""

    expected_vector: MutationVector
    report: ReportRef
    host: str
    intent: FailureUpdateIntent
    action_replacements: tuple[tuple[str, Mapping[str, object]], ...]
    failed_reason: str
    set_complete_false: bool = True

    def __post_init__(self) -> None:
        _require_effect_vector(
            self.expected_vector,
            self.report,
            self.host,
            allow_missing=True,
        )
        if not isinstance(self.intent, FailureUpdateIntent):
            raise TransitionError("current acknowledgement requires FailureUpdateIntent")
        if (
            self.intent.host != self.host
            or _intent_report(self.intent) != self.report
            or self.expected_vector.latest.ref != self.report
            or self.failed_reason != self.intent.failed_host_reason
        ):
            raise TransitionError("current acknowledgement effect identity is inconsistent")
        if self.set_complete_false is not True:
            raise TransitionError("current acknowledgement must set complete false")
        if not isinstance(self.action_replacements, tuple):
            raise TransitionError("current action replacements must be a tuple")
        frozen: list[tuple[str, Mapping[str, object]]] = []
        for action_id, row in self.action_replacements:
            _require_text(action_id, "current replacement action ID")
            if not isinstance(row, Mapping):
                raise TransitionError("current replacement row must be a mapping")
            frozen.append((action_id, _freeze_mapping(row, "current replacement row")))
        if len(frozen) != len({item[0] for item in frozen}):
            raise TransitionError("current replacement action IDs must be unique")
        expected_rows = _rows_from_intent(self.intent)
        if not frozen or dict(frozen) != expected_rows:
            raise TransitionError("current replacement rows conflict with intent")
        if _current_ack_sha256(self.host, self.failed_reason, expected_rows) != (
            self.intent.current_ack_sha256
        ):
            raise TransitionError("current acknowledgement effect hash is inconsistent")
        object.__setattr__(self, "action_replacements", tuple(frozen))


@dataclass(frozen=True, slots=True)
class WriteOldBacklink(_PlannerSealed):
    """Atomic historical hold settlement, backlink, and stale-skip removal."""

    expected_vector: MutationVector
    old_report: ReportRef
    host: str
    backlink: FailureUpdateBacklink
    settled_hold: Mapping[str, object]
    action_edits: tuple[ActionEdit, ...]
    delete_retained_hold: bool = True
    set_complete_false: bool = True

    def __post_init__(self) -> None:
        _require_effect_vector(self.expected_vector, self.old_report, self.host)
        if not isinstance(self.backlink, FailureUpdateBacklink):
            raise TransitionError("old backlink effect requires FailureUpdateBacklink")
        if (
            self.backlink.host != self.host
            or not same_report(self.backlink.historical_report_path, self.old_report.source_path)
            or self.backlink.historical_report_digest_sha256 != self.old_report.digest_sha256
        ):
            raise TransitionError("old backlink effect identity is inconsistent")
        if self.delete_retained_hold is not True or self.set_complete_false is not True:
            raise TransitionError("old backlink effect safety flags must remain true")
        if not isinstance(self.settled_hold, Mapping):
            raise TransitionError("old backlink effect requires settled hold content")
        settled = _freeze_mapping(self.settled_hold, "old backlink settled hold")
        if canonical_json_sha256(settled) != self.backlink.settled_hold_sha256:
            raise TransitionError("old backlink settled hold digest is inconsistent")
        object.__setattr__(self, "settled_hold", settled)
        if not self.action_edits or any(
            not isinstance(item, ActionEdit) for item in self.action_edits
        ):
            raise TransitionError("old backlink effect requires action edits")
        edit_ids = [item.action_id for item in self.action_edits]
        if len(edit_ids) != len(set(edit_ids)) or set(edit_ids) != set(
            self.backlink.failed_action_ids
        ):
            raise TransitionError("old backlink action edits do not bind failed actions")
        expected_receipt = _backlink_dict(self.backlink)
        for edit in self.action_edits:
            if edit.delete_keys not in {(), ("legacy_failure_skip",)} or edit.set_items != (
                ("legacy_failure_update_ack", _freeze_json(expected_receipt)),
            ):
                raise TransitionError("old backlink action edit payload is inconsistent")
        current_token = _vector_token(
            self.expected_vector,
            self.backlink.current_rollout_id,
        )
        if current_token.digest_sha256 is None:
            raise TransitionError("old backlink effect lacks current journal authority")


@dataclass(frozen=True, slots=True)
class ConsumeMember(_PlannerSealed):
    """Consume only the pending exact fences of one backlinked source."""

    expected_vector: MutationVector
    old_report: ReportRef
    host: str
    attempts: tuple[FailureAttemptRef, ...]
    consumed: bool = True
    retry_authorized: bool = False

    def __post_init__(self) -> None:
        _require_effect_vector(self.expected_vector, self.old_report, self.host)
        if (
            not isinstance(self.attempts, tuple)
            or not self.attempts
            or any(not isinstance(item, FailureAttemptRef) for item in self.attempts)
            or any(item.consumed for item in self.attempts)
        ):
            raise TransitionError("consume effect must contain pending attempts only")
        operation_ids = [item.operation_id for item in self.attempts]
        if len(operation_ids) != len(set(operation_ids)):
            raise TransitionError("consume effect operation IDs must be unique")
        if self.consumed is not True or self.retry_authorized is not False:
            raise TransitionError("consume effect cannot authorize deployment or retry")


@dataclass(frozen=True, slots=True)
class ClearForwardIntent(_PlannerSealed):
    """Remove a completed forward intent from its current journal."""

    expected_vector: MutationVector
    report: ReportRef
    host: str
    intent_sha256: str

    def __post_init__(self) -> None:
        _require_effect_vector(self.expected_vector, self.report, self.host)
        _require_sha256(self.intent_sha256, "clear intent digest")


@dataclass(frozen=True, slots=True)
class Reject(_PlannerSealed):
    """Fail-closed terminal effect containing no mutation authority."""

    host: str
    reason: str

    def __post_init__(self) -> None:
        _require_text(self.host, "reject host")
        _require_text(self.reason, "reject reason")


type RecoveryEffect = (
    WriteCurrentAckAndIntent | WriteOldBacklink | ConsumeMember | ClearForwardIntent | Reject
)

type _MutationEffect = (
    WriteCurrentAckAndIntent | WriteOldBacklink | ConsumeMember | ClearForwardIntent
)
type _PlannerEffect = _MutationEffect | Reject


def _authority_dataclass_types() -> tuple[type[object], ...]:
    return (
        ReportRef,
        LatestReportRef,
        ReceiptReportRef,
        PlanActionRef,
        FailureAttemptRef,
        HostFailureGroup,
        JournalToken,
        MutationVector,
        JournalHostSnapshot,
        HoldProof,
        FailureMember,
        FailureUpdateIntent,
        FailureUpdateBacklink,
        ActionEdit,
        WriteCurrentAckAndIntent,
        WriteOldBacklink,
        ConsumeMember,
        ClearForwardIntent,
        Reject,
        HostRecoveryInput,
        HostTransition,
    )


def _runtime_fingerprint(value: object) -> object:
    """Describe exact immutable runtime shape without calling payload methods."""
    if value is None:
        return ("none",)
    if type(value) is bool:
        return ("bool", value)
    if type(value) is int:
        return ("int", value)
    if type(value) is float:
        return ("float", value)
    if type(value) is str:
        return ("str", value)
    if type(value) in (SkipState, CurrentAckState, TransitionPhase, HoldProofKind):
        enum_value = value
        assert isinstance(enum_value, StrEnum)
        return ("enum", type(enum_value).__name__, enum_value.value)
    if type(value) is _FrozenJSONMapping:
        raw_items = tuple(tuple.__iter__(value))
        seen: set[str] = set()
        encoded: list[object] = []
        for item in raw_items:
            if type(item) is not tuple or len(item) != 2 or type(item[0]) is not str:
                raise TransitionError("frozen JSON mapping storage is malformed")
            key, nested = item
            if key in seen:
                raise TransitionError("frozen JSON mapping keys must be unique")
            seen.add(key)
            encoded.append((key, _runtime_fingerprint(nested)))
        return ("frozen-json-mapping", tuple(encoded))
    if type(value) is tuple:
        return ("tuple", tuple(_runtime_fingerprint(item) for item in value))
    if is_dataclass(value) and not isinstance(value, type):
        if type(value) not in _authority_dataclass_types() or hasattr(value, "__dict__"):
            raise TransitionError("authority payload has an unexpected dataclass type")
        return (
            "dataclass",
            type(value).__name__,
            tuple(
                (
                    item.name,
                    _runtime_fingerprint(object.__getattribute__(value, item.name)),
                )
                for item in fields(type(value))
            ),
        )
    raise TransitionError(
        f"authority payload has mutable or unsupported runtime type {type(value).__name__}"
    )


def _authority_digest(value: object) -> str:
    return canonical_json_sha256(_runtime_fingerprint(value))


def _validate_effect_runtime_shape(effect: _PlannerEffect) -> None:
    if type(effect) not in (
        WriteCurrentAckAndIntent,
        WriteOldBacklink,
        ConsumeMember,
        ClearForwardIntent,
        Reject,
    ):
        raise TransitionError("mutation effect has an unexpected exact type")
    _runtime_fingerprint(effect)
    values = {
        item.name: object.__getattribute__(effect, item.name) for item in fields(type(effect))
    }
    try:
        type(effect)(**values)
    except (TypeError, ValueError) as exc:
        raise TransitionError(
            f"mutation effect planner provenance runtime shape is invalid: {exc}"
        ) from exc


def _seal_planner_effect(effect: _PlannerEffect) -> None:
    object.__setattr__(effect, "_planner_seal", _PLANNER_EFFECT_SEALED)
    digest = _authority_digest(effect)
    object.__setattr__(
        effect,
        "_planner_payload_sha256",
        digest,
    )
    _register_authority(effect, digest)


def _require_planner_effect(effect: _PlannerEffect) -> None:
    try:
        seal = object.__getattribute__(effect, "_planner_seal")
        observed = object.__getattribute__(effect, "_planner_payload_sha256")
    except AttributeError as exc:
        raise TransitionError("mutation effect lacks private planner provenance") from exc
    if seal is not _PLANNER_EFFECT_SEALED:
        raise TransitionError("mutation effect lacks private planner provenance seal")
    _require_sha256(observed, "mutation effect planner provenance digest")
    _require_registered_authority(effect, observed)
    _validate_effect_runtime_shape(effect)
    expected = _authority_digest(effect)
    if observed != expected:
        raise TransitionError("mutation effect planner provenance payload digest is stale")


@dataclass(frozen=True, slots=True)
class HostRecoveryInput(_PlannerSealed):
    """Preauthenticated pure inputs for one host-group transition."""

    latest_report: LatestReportRef
    host: str
    plan_actions: tuple[PlanActionRef, ...]
    failures: tuple[HostFailureGroup, ...]
    old_snapshots: tuple[JournalHostSnapshot, ...]
    current_snapshot: JournalHostSnapshot | None
    hold_proofs: tuple[HoldProof, ...]
    mutation_vector: MutationVector
    recorded_at: str
    receipt_report: ReceiptReportRef | None = None

    def __post_init__(self) -> None:
        if type(self) is not HostRecoveryInput:
            raise TypeError("recovery input must be an exact HostRecoveryInput")
        _validate_recovery_input(self)
        digest = _authority_digest(self)
        object.__setattr__(self, "_planner_seal", _RECOVERY_INPUT_SEALED)
        object.__setattr__(self, "_planner_payload_sha256", digest)
        _register_authority(self, digest)


def _validate_recovery_input(recovery: HostRecoveryInput) -> None:
    _require_latest(recovery.latest_report)
    _require_text(recovery.host, "host recovery host")
    _require_aware_timestamp(recovery.recorded_at, "host recovery recorded_at")
    if recovery.current_snapshot is not None and type(recovery.current_snapshot) is not (
        JournalHostSnapshot
    ):
        raise TypeError("current snapshot must be an exact JournalHostSnapshot or None")
    if type(recovery.mutation_vector) is not MutationVector:
        raise TypeError("recovery mutation vector must be an exact MutationVector")
    if type(recovery.failures) is not tuple or any(
        type(item) is not HostFailureGroup for item in recovery.failures
    ):
        raise TransitionError("recovery failures must be HostFailureGroup records")
    if type(recovery.old_snapshots) is not tuple or any(
        type(item) is not JournalHostSnapshot for item in recovery.old_snapshots
    ):
        raise TransitionError("recovery old snapshots must be JournalHostSnapshot records")
    if type(recovery.hold_proofs) is not tuple or any(
        type(item) is not HoldProof for item in recovery.hold_proofs
    ):
        raise TransitionError("recovery hold proofs must be HoldProof records")
    if recovery.mutation_vector.latest != recovery.latest_report:
        raise TransitionError("recovery mutation vector has another latest report")
    if type(recovery.plan_actions) is not tuple or any(
        type(item) is not PlanActionRef for item in recovery.plan_actions
    ):
        raise TransitionError("recovery plan actions must be PlanActionRef records")
    if (
        recovery.receipt_report is not None
        and type(recovery.receipt_report) is not ReceiptReportRef
    ):
        raise TypeError("recovery receipt report must be an exact ReceiptReportRef")
    if recovery.current_snapshot is not None and recovery.current_snapshot.host != recovery.host:
        raise TransitionError("current snapshot belongs to another host")
    if any(item.host != recovery.host for item in recovery.failures):
        raise TransitionError("failure group belongs to another host")
    if any(item.host != recovery.host for item in recovery.old_snapshots):
        raise TransitionError("old snapshot belongs to another host")
    snapshot_ids = [item.report.rollout_id for item in recovery.old_snapshots]
    if len(snapshot_ids) != len(set(snapshot_ids)):
        raise TransitionError("old snapshots must be unique by rollout")
    proof_ids = [item.old_rollout_id for item in recovery.hold_proofs]
    if len(proof_ids) != len(set(proof_ids)):
        raise TransitionError("hold proofs must be unique by rollout")
    current_rollout_id = (
        recovery.latest_report.ref.rollout_id
        if recovery.current_snapshot is None
        else recovery.current_snapshot.report.rollout_id
    )
    if current_rollout_id in snapshot_ids:
        raise TransitionError("current and old journal rollout IDs collide")
    snapshots_by_id = {item.report.rollout_id: item for item in recovery.old_snapshots}
    groups_by_id = {item.report.rollout_id: item for item in recovery.failures}
    if set(snapshots_by_id) != set(groups_by_id):
        raise TransitionError("failure groups and old journal snapshots differ")
    for rollout_id, snapshot in snapshots_by_id.items():
        if snapshot.report != groups_by_id[rollout_id].report:
            raise TransitionError("old snapshot report differs from failure group report")
    required_tokens = {current_rollout_id, *snapshot_ids}
    vector_ids = {item.rollout_id for item in recovery.mutation_vector.journals}
    if not required_tokens.issubset(vector_ids):
        raise TransitionError("mutation vector lacks a required current or old token")
    if recovery.current_snapshot is None:
        if recovery.receipt_report is not None:
            raise TransitionError("absent current journal cannot carry a receipt report")
        current_token = _vector_token(recovery.mutation_vector, current_rollout_id)
        if current_token.digest_sha256 is not None:
            raise TransitionError("absent current journal requires a missing token")
    elif (
        _vector_token(recovery.mutation_vector, current_rollout_id)
        != recovery.current_snapshot.token
    ):
        raise TransitionError("current snapshot token differs from mutation vector")
    for snapshot in recovery.old_snapshots:
        if _vector_token(recovery.mutation_vector, snapshot.report.rollout_id) != snapshot.token:
            raise TransitionError("snapshot token differs from mutation vector")


def _require_recovery_input_authority(recovery: HostRecoveryInput) -> None:
    if type(recovery) is not HostRecoveryInput:
        raise TypeError("recovery input must be an exact HostRecoveryInput")
    try:
        seal = object.__getattribute__(recovery, "_planner_seal")
        observed = object.__getattribute__(recovery, "_planner_payload_sha256")
    except AttributeError as exc:
        raise TransitionError("recovery input lacks registered authority") from exc
    if seal is not _RECOVERY_INPUT_SEALED:
        raise TransitionError("recovery input lacks its exact authority seal")
    if type(observed) is not str:
        raise TransitionError("recovery input authority digest must be an exact string")
    _require_sha256(observed, "recovery input authority digest")
    _require_registered_authority(recovery, observed)
    expected = _authority_digest(recovery)
    if observed != expected:
        raise TransitionError("recovery input authority digest is stale")
    _validate_recovery_input(recovery)


@dataclass(frozen=True, slots=True)
class HostTransition(_PlannerSealed):
    """Ordered pure effects for exactly one crash-restart phase."""

    phase: TransitionPhase
    host: str
    effects: tuple[RecoveryEffect, ...] = field(default_factory=tuple)
    _planner_token: InitVar[object] = None

    def __post_init__(self, _planner_token: object) -> None:
        _require_text(self.host, "host transition host")
        if type(self.phase) is not TransitionPhase:
            raise TransitionError("transition phase must be an exact TransitionPhase")
        if type(self.effects) is not tuple:
            raise TransitionError("transition effects must be a tuple")
        if len(self.effects) > 1:
            raise TransitionError("transition may contain at most one effect")
        if any(type(item) is Reject for item in self.effects) and len(self.effects) != 1:
            raise TransitionError("reject must be the transition's only effect")
        for effect in self.effects:
            if type(effect) in (
                WriteCurrentAckAndIntent,
                WriteOldBacklink,
                ConsumeMember,
                ClearForwardIntent,
                Reject,
            ):
                _require_planner_effect(effect)
        allowed: dict[TransitionPhase, tuple[type[object], ...]] = {
            TransitionPhase.PREVALIDATED: (WriteCurrentAckAndIntent, Reject),
            TransitionPhase.ACKED: (WriteOldBacklink, Reject),
            TransitionPhase.BACKLINKING: (WriteOldBacklink, Reject),
            TransitionPhase.CONSUMING: (ConsumeMember, Reject),
            TransitionPhase.COMPLETE: (ClearForwardIntent, Reject),
        }
        if any(type(item) not in allowed[self.phase] for item in self.effects):
            raise TransitionError("transition phase contains an out-of-order effect")
        if any(item.host != self.host for item in self.effects):
            raise TransitionError("transition effect belongs to another host")
        if self.phase is not TransitionPhase.COMPLETE and not self.effects:
            raise TransitionError("non-complete transition must have an effect")
        if _planner_token is not _PLANNER_TRANSITION_BUILD_TOKEN:
            raise TransitionError("host transition lacks private planner provenance seal")
        object.__setattr__(self, "_planner_seal", _PLANNER_TRANSITION_SEALED)
        digest = _authority_digest(self)
        object.__setattr__(
            self,
            "_planner_payload_sha256",
            digest,
        )
        _register_authority(self, digest)

    def authorized_effects(self) -> tuple[RecoveryEffect, ...]:
        """Verify planner provenance at the adapter consumption boundary."""
        _require_transition_envelope(self)
        effects = object.__getattribute__(self, "effects")
        for effect in effects:
            if type(effect) in (
                WriteCurrentAckAndIntent,
                WriteOldBacklink,
                ConsumeMember,
                ClearForwardIntent,
                Reject,
            ):
                _require_planner_effect(effect)
        return effects


def _require_transition_envelope(transition: HostTransition) -> None:
    if type(transition) is not HostTransition:
        raise TransitionError("host transition has an unexpected exact type")
    try:
        seal = object.__getattribute__(transition, "_planner_seal")
        observed = object.__getattribute__(transition, "_planner_payload_sha256")
    except AttributeError as exc:
        raise TransitionError("host transition lacks private planner provenance") from exc
    if seal is not _PLANNER_TRANSITION_SEALED:
        raise TransitionError("host transition lacks private planner provenance seal")
    _require_sha256(observed, "host transition planner provenance digest")
    _require_registered_authority(transition, observed)
    if type(object.__getattribute__(transition, "effects")) is not tuple:
        raise TransitionError("host transition effects must remain an exact tuple")
    expected = _authority_digest(transition)
    if observed != expected:
        raise TransitionError("host transition planner provenance envelope digest is stale")


def _build_transition(
    phase: TransitionPhase,
    host: str,
    effects: tuple[RecoveryEffect, ...] = (),
) -> HostTransition:
    return HostTransition(
        phase,
        host,
        effects,
        _planner_token=_PLANNER_TRANSITION_BUILD_TOKEN,
    )


def canonical_json_sha256(value: object) -> str:
    """Hash canonical compact JSON with non-finite numbers rejected."""
    try:
        encoded = json.dumps(
            thaw_json(value),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise TransitionError(f"value is not canonical-JSON encodable: {exc}") from exc
    return hashlib.sha256(encoded).hexdigest()


def project_host_actions(
    latest_report: LatestReportRef,
    host: str,
    actions: Sequence[PlanActionRef],
) -> dict[str, object]:
    """Project every current host lane into the closed plan identity domain."""
    _require_latest(latest_report)
    return _project_host_actions(host, actions)


def _project_host_actions(
    host: str,
    actions: Sequence[PlanActionRef],
) -> dict[str, object]:
    _require_text(host, "projected host")
    if any(not isinstance(item, PlanActionRef) for item in actions):
        raise TransitionError("host plan must contain PlanActionRef records")
    selected = sorted(
        (item for item in actions if item.host == host),
        key=lambda item: item.action_id,
    )
    action_ids = [item.action_id for item in selected]
    if len(action_ids) != len(set(action_ids)):
        raise TransitionError("host plan contains duplicate action IDs")
    return {
        "schema": FAILURE_UPDATE_ACTIONS_SCHEMA,
        "host": host,
        "actions": [
            {
                "action_id": item.action_id,
                "phase": item.phase,
                "host": item.host,
                "program": item.program,
                "decision": item.decision,
                "reason": item.reason,
                "pin_name": item.pin_name,
                "target_sha": item.target_sha,
                "target_version": item.target_version,
                "target_tag": item.target_tag,
                "argv": list(item.argv),
                "before": thaw_json(item.before),
            }
            for item in selected
        ],
    }


def _normalize_before(
    raw: Mapping[str, object],
    *,
    require_required: bool = False,
) -> _FrozenJSONMapping:
    keys = [
        "configured",
        "current_sha",
        "current_version",
        "current_tag",
        "dirty",
        "last_ok",
        "acknowledged",
        "detail",
        "metrics",
    ]
    allowed = {*keys, "required"}
    if set(raw) - allowed or any(key not in raw for key in keys):
        raise TransitionError("plan action before has invalid fields")
    if require_required and set(raw) != allowed:
        raise TransitionError("projected action before fields are not canonical")
    if type(raw["configured"]) is not bool:
        raise TransitionError("plan action before configured must be bool")
    current_sha = raw["current_sha"]
    if current_sha is not None:
        _require_sha1(current_sha, "plan action before current_sha")
    for name in ("current_version", "current_tag"):
        value = raw[name]
        if value is not None and type(value) is not str:
            raise TransitionError(f"plan action before {name} must be str or None")
    dirty = raw["dirty"]
    if dirty is not None and type(dirty) is not bool:
        raise TransitionError("plan action before dirty must be bool or None")
    if type(raw["last_ok"]) is not bool or type(raw["acknowledged"]) is not bool:
        raise TransitionError("plan action before state flags must be bool")
    if type(raw["detail"]) is not str:
        raise TransitionError("plan action before detail must be str")
    metrics = raw["metrics"]
    if metrics is not None and not isinstance(metrics, Mapping):
        raise TransitionError("plan action before metrics must be a mapping or None")
    required = raw.get("required", False)
    if type(required) is not bool:
        raise TransitionError("plan action before required must be bool")
    normalized = {
        "configured": raw["configured"],
        "current_sha": current_sha,
        "current_version": raw["current_version"],
        "current_tag": raw["current_tag"],
        "dirty": dirty,
        "last_ok": raw["last_ok"],
        "acknowledged": raw["acknowledged"],
        "detail": raw["detail"],
        "metrics": metrics,
        "required": required,
    }
    return _freeze_mapping(normalized, "plan action before")


def _validate_plan_projection(
    raw: object,
    *,
    host: str,
) -> _FrozenJSONMapping:
    if not isinstance(raw, Mapping) or set(raw) != {"schema", "host", "actions"}:
        raise TransitionError("intent plan projection fields are not canonical")
    if raw["schema"] != FAILURE_UPDATE_ACTIONS_SCHEMA or raw["host"] != host:
        raise TransitionError("intent plan projection identity is invalid")
    raw_actions = raw["actions"]
    if not isinstance(raw_actions, (list, tuple)):
        raise TransitionError("intent plan projection actions must be a list")
    action_fields = [
        "action_id",
        "phase",
        "host",
        "program",
        "decision",
        "reason",
        "pin_name",
        "target_sha",
        "target_version",
        "target_tag",
        "argv",
        "before",
    ]
    normalized_rows: list[Mapping[str, object]] = []
    action_ids: list[str] = []
    for raw_row in raw_actions:
        if not isinstance(raw_row, Mapping) or set(raw_row) != set(action_fields):
            raise TransitionError("projected action fields are not canonical")
        for name in ("action_id", "host", "program", "reason", "pin_name"):
            _require_text(raw_row[name], f"projected action {name}")
        if raw_row["host"] != host:
            raise TransitionError("projected action belongs to another host")
        if type(raw_row["phase"]) is not str or raw_row["phase"] not in {
            "driver",
            "helper",
            "scheduler-runtime",
            "local-runtime",
        }:
            raise TransitionError("projected action phase is invalid")
        if type(raw_row["decision"]) is not str or raw_row["decision"] not in {
            "update",
            "skip",
            "defer",
            "block",
        }:
            raise TransitionError("projected action decision is invalid")
        _require_sha1(raw_row["target_sha"], "projected action target_sha")
        for name in ("target_version", "target_tag"):
            value = raw_row[name]
            if value is not None and type(value) is not str:
                raise TransitionError(f"projected action {name} must be str or None")
        argv = raw_row["argv"]
        if not isinstance(argv, (list, tuple)) or not all(
            type(item) is str and item for item in argv
        ):
            raise TransitionError("projected action argv is invalid")
        before = raw_row["before"]
        if not isinstance(before, Mapping):
            raise TransitionError("projected action before must be a mapping")
        normalized_rows.append(
            _freeze_mapping(
                {
                    "action_id": raw_row["action_id"],
                    "phase": raw_row["phase"],
                    "host": raw_row["host"],
                    "program": raw_row["program"],
                    "decision": raw_row["decision"],
                    "reason": raw_row["reason"],
                    "pin_name": raw_row["pin_name"],
                    "target_sha": raw_row["target_sha"],
                    "target_version": raw_row["target_version"],
                    "target_tag": raw_row["target_tag"],
                    "argv": tuple(argv),
                    "before": _normalize_before(before, require_required=True),
                },
                "projected action",
            )
        )
        assert isinstance(raw_row["action_id"], str)
        action_ids.append(raw_row["action_id"])
    if action_ids != sorted(action_ids) or len(action_ids) != len(set(action_ids)):
        raise TransitionError("projected action IDs must be sorted and unique")
    return _freeze_mapping(
        {
            "schema": FAILURE_UPDATE_ACTIONS_SCHEMA,
            "host": host,
            "actions": tuple(normalized_rows),
        },
        "intent plan projection",
    )


def current_actions_sha256(
    latest_report: LatestReportRef,
    host: str,
    actions: Sequence[PlanActionRef],
) -> str:
    """Hash the complete host plan, including non-update lane membership."""
    return canonical_json_sha256(project_host_actions(latest_report, host, actions))


def canonical_no_launch_rows(
    host: str,
    actions: Sequence[PlanActionRef],
    failed_reason: str,
) -> dict[str, dict[str, object]]:
    """Return executor-identical three-field rows for every host update."""
    _require_text(host, "no-launch host")
    _require_text(failed_reason, "no-launch failed reason")
    selected = sorted(
        (item for item in actions if item.host == host and item.decision == "update"),
        key=lambda item: item.action_id,
    )
    if not selected:
        raise TransitionError("failure acknowledgement requires a current update action")
    if len(selected) != len({item.action_id for item in selected}):
        raise TransitionError("host update plan contains duplicate action IDs")
    reason = f"skipped: an earlier lane on {host} failed ({failed_reason})"
    return {
        item.action_id: {"decision": "update", "reason": reason, "status": "not-run"}
        for item in selected
    }


def canonical_host_failure_reason(failures: Sequence[HostFailureGroup]) -> str:
    """Collapse source reasons deterministically; the final sorted source wins."""
    members = _canonical_groups(failures)
    if not members:
        raise TransitionError("failure reason requires at least one source")
    return members[-1].reason


def parse_failure_update_intent(raw: object) -> FailureUpdateIntent:
    """Strictly parse the closed forward-intent schema."""
    raw = thaw_json(raw)
    fields = {
        "schema",
        "host",
        "current_report_path",
        "current_report_digest_sha256",
        "current_rollout_id",
        "current_action_ids",
        "current_plan_projection",
        "current_actions_sha256",
        "current_ack_sha256",
        "failed_host_reason",
        "sources",
        "recorded_at",
    }
    value = _closed_mapping(raw, fields, "failure update intent")
    sources_raw = value["sources"]
    if not isinstance(sources_raw, list):
        raise TransitionError("failure update intent sources must be a list")
    sources = tuple(_parse_failure_member(item) for item in sources_raw)
    return FailureUpdateIntent(
        schema=_string(value["schema"], "intent schema"),
        host=_string(value["host"], "intent host"),
        current_report_path=_string(value["current_report_path"], "intent report path"),
        current_report_digest_sha256=_string(
            value["current_report_digest_sha256"], "intent report digest"
        ),
        current_rollout_id=_string(value["current_rollout_id"], "intent rollout_id"),
        current_action_ids=_string_tuple(value["current_action_ids"], "intent action IDs"),
        current_plan_projection=value["current_plan_projection"],
        current_actions_sha256=_string(value["current_actions_sha256"], "intent actions digest"),
        current_ack_sha256=_string(value["current_ack_sha256"], "intent ack digest"),
        failed_host_reason=_string(value["failed_host_reason"], "intent failed reason"),
        sources=sources,
        recorded_at=_string(value["recorded_at"], "intent recorded_at"),
    )


def parse_failure_update_backlink(raw: object) -> FailureUpdateBacklink:
    """Strictly parse the exact closed 13-field historical backlink."""
    raw = thaw_json(raw)
    fields = {
        "schema",
        "host",
        "failed_action_ids",
        "failed_operation_ids",
        "historical_report_path",
        "historical_report_digest_sha256",
        "settled_hold_sha256",
        "current_report_path",
        "current_report_digest_sha256",
        "current_rollout_id",
        "current_action_ids",
        "current_actions_sha256",
        "recorded_at",
    }
    value = _closed_mapping(raw, fields, "failure update backlink")
    return FailureUpdateBacklink(
        schema=_string(value["schema"], "backlink schema"),
        host=_string(value["host"], "backlink host"),
        failed_action_ids=_string_tuple(value["failed_action_ids"], "backlink action IDs"),
        failed_operation_ids=_string_tuple(value["failed_operation_ids"], "backlink operation IDs"),
        historical_report_path=_string(
            value["historical_report_path"], "backlink historical report path"
        ),
        historical_report_digest_sha256=_string(
            value["historical_report_digest_sha256"],
            "backlink historical report digest",
        ),
        settled_hold_sha256=_string(value["settled_hold_sha256"], "backlink settled hold digest"),
        current_report_path=_string(value["current_report_path"], "backlink current report path"),
        current_report_digest_sha256=_string(
            value["current_report_digest_sha256"], "backlink current report digest"
        ),
        current_rollout_id=_string(value["current_rollout_id"], "backlink rollout_id"),
        current_action_ids=_string_tuple(
            value["current_action_ids"], "backlink current action IDs"
        ),
        current_actions_sha256=_string(
            value["current_actions_sha256"], "backlink current actions digest"
        ),
        recorded_at=_string(value["recorded_at"], "backlink recorded_at"),
    )


def expand_failure_update_backlink(
    intent: FailureUpdateIntent,
    member: FailureMember,
    receipt_report: ReceiptReportRef,
) -> FailureUpdateBacklink:
    """Expand one source into the identical receipt copied to its old rows."""
    if type(receipt_report) is not ReceiptReportRef:
        raise TypeError("historical backlink expansion requires ReceiptReportRef")
    if receipt_report.ref != _intent_report(intent) or member not in intent.sources:
        raise TransitionError("backlink does not bind the exact persisted intent source")
    return FailureUpdateBacklink(
        schema=FAILURE_UPDATE_BACKLINK_SCHEMA,
        host=intent.host,
        failed_action_ids=member.failed_action_ids,
        failed_operation_ids=member.failed_operation_ids,
        historical_report_path=member.historical_report_path,
        historical_report_digest_sha256=member.historical_report_digest_sha256,
        settled_hold_sha256=member.settled_hold_sha256,
        current_report_path=intent.current_report_path,
        current_report_digest_sha256=intent.current_report_digest_sha256,
        current_rollout_id=intent.current_rollout_id,
        current_action_ids=intent.current_action_ids,
        current_actions_sha256=intent.current_actions_sha256,
        recorded_at=intent.recorded_at,
    )


def classify_current_ack(
    snapshot: JournalHostSnapshot,
    intent: FailureUpdateIntent,
) -> CurrentAckState:
    """Classify current acknowledgement material anchored by forward intent."""
    if snapshot.forward_intent is None:
        if (
            not snapshot.relevant_action_records
            and snapshot.failed_reason is None
            and snapshot.complete is False
        ):
            return CurrentAckState.ABSENT
        return CurrentAckState.CONFLICT
    try:
        observed = parse_failure_update_intent(snapshot.forward_intent)
    except TransitionError:
        return CurrentAckState.CONFLICT
    rows = _rows_from_intent(intent)
    exact_rows = bool(
        set(snapshot.relevant_action_records) == set(rows)
        and all(snapshot.relevant_action_records.get(key) == row for key, row in rows.items())
    )
    exact = bool(
        observed == intent
        and same_report(snapshot.report.source_path, intent.current_report_path)
        and snapshot.report.digest_sha256 == intent.current_report_digest_sha256
        and snapshot.report.rollout_id == intent.current_rollout_id
        and snapshot.failed_reason == intent.failed_host_reason
        and snapshot.complete is False
        and exact_rows
        and _current_ack_sha256(intent.host, intent.failed_host_reason, rows)
        == intent.current_ack_sha256
    )
    return CurrentAckState.EXACT if exact else CurrentAckState.CONFLICT


def classify_old_failure_row(
    group: HostFailureGroup,
    snapshot: JournalHostSnapshot,
    action_id: str,
    expected: FailureUpdateBacklink,
) -> CurrentAckState:
    """Classify one old row, rejecting malformed skip/backlink overlap."""
    raw_record = snapshot.relevant_action_records.get(action_id)
    if not isinstance(raw_record, Mapping) or raw_record.get("status") != "failed":
        raise TransitionError(f"old failure action {action_id} is missing or malformed")
    raw_backlink = raw_record.get("legacy_failure_update_ack", _NO_VALUE)
    raw_skip = raw_record.get("legacy_failure_skip", _NO_VALUE)
    skip_state = group.skip_state(action_id)
    if (raw_skip is _NO_VALUE) != (skip_state is SkipState.ABSENT):
        raise TransitionError(f"old failure action {action_id} has malformed skip state")
    if raw_backlink is _NO_VALUE:
        if skip_state is SkipState.CURRENT:
            raise TransitionError(f"old failure action {action_id} has a current skip")
        return CurrentAckState.ABSENT
    if raw_skip is not _NO_VALUE or skip_state is not SkipState.ABSENT:
        raise TransitionError(f"old failure action {action_id} overlaps skip and backlink")
    observed = parse_failure_update_backlink(raw_backlink)
    if observed != expected:
        raise TransitionError(f"old failure action {action_id} has a conflicting backlink")
    return CurrentAckState.EXACT


def _build_current_ack_effect(
    recovery: HostRecoveryInput,
    intent: FailureUpdateIntent,
) -> WriteCurrentAckAndIntent:
    """Build the single atomic current acknowledgement and intent effect."""
    rows = _rows_from_intent(intent)
    effect = WriteCurrentAckAndIntent(
        expected_vector=recovery.mutation_vector,
        report=(
            recovery.latest_report.ref
            if recovery.current_snapshot is None
            else recovery.current_snapshot.report
        ),
        host=recovery.host,
        intent=intent,
        action_replacements=tuple((key, value) for key, value in rows.items()),
        failed_reason=intent.failed_host_reason,
    )
    _seal_planner_effect(effect)
    return effect


def _build_old_backlink_effect(
    recovery: HostRecoveryInput,
    group: HostFailureGroup,
    snapshot: JournalHostSnapshot,
    proof: HoldProof,
    backlink: FailureUpdateBacklink,
) -> WriteOldBacklink:
    """Build one old-journal atomic backlink and hold-settlement effect."""
    if proof.settled_hold is None:
        raise TransitionError("old backlink write requires settled hold content")
    edits: list[ActionEdit] = []
    for action_id in group.failed_action_ids:
        state = classify_old_failure_row(group, snapshot, action_id, backlink)
        if state is CurrentAckState.EXACT:
            continue
        delete_keys = (
            ("legacy_failure_skip",) if group.skip_state(action_id) is SkipState.STALE else ()
        )
        edits.append(
            ActionEdit(
                action_id=action_id,
                delete_keys=delete_keys,
                set_items=(("legacy_failure_update_ack", backlink.as_dict()),),
            )
        )
    if not edits:
        raise TransitionError("old backlink effect has no absent rows")
    effect = WriteOldBacklink(
        expected_vector=recovery.mutation_vector,
        old_report=group.report,
        host=group.host,
        backlink=backlink,
        settled_hold=proof.settled_hold,
        action_edits=tuple(edits),
    )
    _seal_planner_effect(effect)
    return effect


def _build_consume_effect(
    recovery: HostRecoveryInput,
    group: HostFailureGroup,
    snapshot: JournalHostSnapshot,
) -> ConsumeMember:
    """Build a consume-only effect with retry authorization fixed false."""
    effect = ConsumeMember(
        expected_vector=recovery.mutation_vector,
        old_report=group.report,
        host=group.host,
        attempts=group.pending_attempts,
    )
    _seal_planner_effect(effect)
    return effect


def _build_clear_intent_effect(
    recovery: HostRecoveryInput,
    snapshot: JournalHostSnapshot,
    intent: FailureUpdateIntent,
) -> ClearForwardIntent:
    """Build the final compare-and-swap removal of a completed intent."""
    effect = ClearForwardIntent(
        expected_vector=recovery.mutation_vector,
        report=snapshot.report,
        host=snapshot.host,
        intent_sha256=canonical_json_sha256(intent.as_dict()),
    )
    _seal_planner_effect(effect)
    return effect


def advance_mutation_vector(
    vector: MutationVector,
    expected: JournalToken,
    replacement: JournalToken,
) -> MutationVector:
    """Advance one whole-journal token or reject an intervening mutation."""
    if expected.rollout_id != replacement.rollout_id:
        raise TransitionError("journal token replacement changes rollout identity")
    if vector.token_for(expected.rollout_id) != expected:
        raise TransitionError("whole journal changed before the planned mutation")
    journals = tuple(
        replacement if item.rollout_id == expected.rollout_id else item for item in vector.journals
    )
    return MutationVector(latest=vector.latest, journals=journals)


def plan_host_transition(recovery: HostRecoveryInput) -> HostTransition:
    """Dispatch one host group to its next crash-safe pure persistence phase."""
    _require_recovery_input_authority(recovery)
    try:
        groups = _canonical_groups(recovery.failures)
        snapshots = {item.report.rollout_id: item for item in recovery.old_snapshots}
        if set(snapshots) != {item.report.rollout_id for item in groups}:
            raise TransitionError("failure groups and old journal snapshots differ")
        if recovery.current_snapshot is None:
            return _plan_without_intent(recovery, groups, snapshots)
        raw_intent = recovery.current_snapshot.forward_intent
        if raw_intent is None:
            return _plan_without_intent(recovery, groups, snapshots)

        try:
            intent = parse_failure_update_intent(raw_intent)
        except TransitionError as exc:
            return _reject(recovery.host, f"forward intent is malformed: {exc}")
        receipt = _validate_existing_receipt(recovery, intent)
        if classify_current_ack(recovery.current_snapshot, intent) is not CurrentAckState.EXACT:
            return _reject(recovery.host, "forward intent conflicts with current acknowledgement")
        expected_members = _members_for_existing_intent(groups, intent)
        if expected_members != intent.sources:
            return _reject(recovery.host, "failure sources conflict with forward intent")

        saw_exact = False
        missing_backlinks: list[
            tuple[
                HostFailureGroup,
                JournalHostSnapshot,
                HoldProof,
                FailureUpdateBacklink,
            ]
        ] = []
        for group, member in zip(groups, intent.sources, strict=True):
            snapshot = snapshots[group.report.rollout_id]
            backlink = expand_failure_update_backlink(intent, member, receipt)
            states = [
                classify_old_failure_row(group, snapshot, action_id, backlink)
                for action_id in group.failed_action_ids
            ]
            _reject_extra_backlink_placement(snapshot, group)
            exact_count = sum(state is CurrentAckState.EXACT for state in states)
            if exact_count not in {0, len(states)}:
                raise TransitionError("historical backlink has incomplete row placement")
            if exact_count:
                saw_exact = True
                proof = _proof_for(recovery.hold_proofs, group.report.rollout_id)
                _validate_existing_backlink_hold(group, snapshot, proof, member)
                continue
            if any(item.consumed for item in group.attempts):
                raise TransitionError("failure was consumed before full backlink placement")
            proof = _proof_for(recovery.hold_proofs, group.report.rollout_id)
            _validate_fresh_hold(group, snapshot, proof)
            if proof.settled_hold_sha256 != member.settled_hold_sha256:
                raise TransitionError("settled hold proof conflicts with forward intent")
            missing_backlinks.append((group, snapshot, proof, backlink))
        if missing_backlinks:
            group, snapshot, proof, backlink = missing_backlinks[0]
            effect = _build_old_backlink_effect(recovery, group, snapshot, proof, backlink)
            phase = TransitionPhase.BACKLINKING if saw_exact else TransitionPhase.ACKED
            return _build_transition(phase, recovery.host, (effect,))

        for group in groups:
            if group.pending_attempts:
                return _build_transition(
                    TransitionPhase.CONSUMING,
                    recovery.host,
                    (_build_consume_effect(recovery, group, snapshots[group.report.rollout_id]),),
                )
        return _build_transition(
            TransitionPhase.COMPLETE,
            recovery.host,
            (_build_clear_intent_effect(recovery, recovery.current_snapshot, intent),),
        )
    except TransitionError as exc:
        return _reject(recovery.host, str(exc))


def _plan_without_intent(
    recovery: HostRecoveryInput,
    groups: tuple[HostFailureGroup, ...],
    snapshots: Mapping[str, JournalHostSnapshot],
) -> HostTransition:
    pending_groups: list[HostFailureGroup] = []
    consumed_groups: list[HostFailureGroup] = []
    for group in groups:
        pending_count = len(group.pending_attempts)
        if pending_count not in {0, len(group.attempts)}:
            raise TransitionError("mixed consumed and pending failures lack a forward intent")
        if pending_count:
            pending_groups.append(group)
        else:
            consumed_groups.append(group)
    if pending_groups and consumed_groups:
        raise TransitionError("mixed consumed and pending groups lack a forward intent")

    if recovery.current_snapshot is None:
        has_backlink = any(
            isinstance(record, Mapping) and "legacy_failure_update_ack" in record
            for snapshot in snapshots.values()
            for record in snapshot.relevant_action_records.values()
        )
        if consumed_groups or has_backlink:
            raise TransitionError(
                "absent current journal is valid only for a fresh unbacklinked create"
            )

    if consumed_groups:
        if recovery.current_snapshot is None:
            raise TransitionError("absent current journal cannot complete a consumed audit")
        receipt_contexts: set[tuple[object, ...]] = set()
        for group in consumed_groups:
            receipt_contexts.add(
                _validate_consumed_audit_group(
                    group,
                    snapshots[group.report.rollout_id],
                    _proof_for(recovery.hold_proofs, group.report.rollout_id),
                )
            )
        if len(receipt_contexts) != 1:
            raise TransitionError("consumed audit groups have divergent receipt context")
        context = next(iter(receipt_contexts))
        _validate_terminal_receipt_current(recovery, context)
        return _build_transition(TransitionPhase.COMPLETE, recovery.host)

    if (
        recovery.current_snapshot is not None
        and recovery.current_snapshot.report != recovery.latest_report.ref
    ):
        raise TransitionError("fresh acknowledgement must target the latest report")
    members: list[FailureMember] = []
    for group in pending_groups:
        snapshot = snapshots[group.report.rollout_id]
        _validate_absent_old_rows(group, snapshot)
        proof = _proof_for(recovery.hold_proofs, group.report.rollout_id)
        _validate_fresh_hold(group, snapshot, proof)
        members.append(_member_from_group(group, proof.settled_hold_sha256))

    if not members:
        if recovery.current_snapshot is None:
            raise TransitionError("absent current journal is valid only for a fresh create")
        return _build_transition(TransitionPhase.COMPLETE, recovery.host)
    intent = _mint_intent(recovery, tuple(members), tuple(pending_groups))
    current_state = (
        CurrentAckState.ABSENT
        if recovery.current_snapshot is None
        else _classify_fresh_current(recovery.current_snapshot, intent)
    )
    if current_state is CurrentAckState.CONFLICT:
        raise TransitionError("current acknowledgement state conflicts with fresh recovery")
    return _build_transition(
        TransitionPhase.PREVALIDATED,
        recovery.host,
        (
            _build_current_ack_effect(
                recovery,
                intent,
            ),
        ),
    )


def _mint_intent(
    recovery: HostRecoveryInput,
    members: tuple[FailureMember, ...],
    groups: tuple[HostFailureGroup, ...],
) -> FailureUpdateIntent:
    reason = canonical_host_failure_reason(groups)
    rows = canonical_no_launch_rows(recovery.host, recovery.plan_actions, reason)
    current_ids = tuple(rows)
    projection = project_host_actions(
        recovery.latest_report,
        recovery.host,
        recovery.plan_actions,
    )
    return FailureUpdateIntent(
        schema=FAILURE_UPDATE_INTENT_SCHEMA,
        host=recovery.host,
        current_report_path=recovery.latest_report.ref.source_path,
        current_report_digest_sha256=recovery.latest_report.ref.digest_sha256,
        current_rollout_id=recovery.latest_report.ref.rollout_id,
        current_action_ids=current_ids,
        current_plan_projection=projection,
        current_actions_sha256=canonical_json_sha256(projection),
        current_ack_sha256=_current_ack_sha256(recovery.host, reason, rows),
        failed_host_reason=reason,
        sources=members,
        recorded_at=recovery.recorded_at,
    )


def _members_for_existing_intent(
    groups: tuple[HostFailureGroup, ...],
    intent: FailureUpdateIntent,
) -> tuple[FailureMember, ...]:
    by_rollout = {item.historical_rollout_id: item for item in intent.sources}
    if set(by_rollout) != {item.report.rollout_id for item in groups}:
        raise TransitionError("forward intent source membership changed")
    return tuple(
        _member_from_group(
            group,
            by_rollout[group.report.rollout_id].settled_hold_sha256,
            include_consumed=True,
        )
        for group in groups
    )


def _validate_existing_receipt(
    recovery: HostRecoveryInput,
    intent: FailureUpdateIntent,
) -> ReceiptReportRef:
    receipt = recovery.receipt_report
    if type(receipt) is not ReceiptReportRef:
        raise TransitionError("existing forward intent requires authenticated receipt report")
    if recovery.current_snapshot is None:
        raise TransitionError("existing forward intent requires a current snapshot")
    if intent.host != recovery.host:
        raise TransitionError("forward intent belongs to another host")
    if receipt.ref != _intent_report(intent) or receipt.ref != recovery.current_snapshot.report:
        raise TransitionError("authenticated receipt does not bind the persisted report")
    reason = canonical_host_failure_reason(recovery.failures)
    if reason != intent.failed_host_reason:
        raise TransitionError("canonical failure reason conflicts with intent")
    rows = _rows_from_intent(intent)
    if _current_ack_sha256(intent.host, intent.failed_host_reason, rows) != (
        intent.current_ack_sha256
    ):
        raise TransitionError("current acknowledgement hash conflicts with intent")
    return receipt


def _classify_fresh_current(
    snapshot: JournalHostSnapshot,
    intent: FailureUpdateIntent,
) -> CurrentAckState:
    if snapshot.forward_intent is not None:
        return CurrentAckState.CONFLICT
    if (
        not snapshot.relevant_action_records
        and snapshot.failed_reason is None
        and snapshot.complete is False
    ):
        return CurrentAckState.ABSENT
    return CurrentAckState.CONFLICT


def _validate_absent_old_rows(
    group: HostFailureGroup,
    snapshot: JournalHostSnapshot,
) -> None:
    if snapshot.complete is not False:
        raise TransitionError("complete historical journal cannot be reopened")
    for action_id in group.failed_action_ids:
        raw_record = snapshot.relevant_action_records.get(action_id)
        if not isinstance(raw_record, Mapping) or raw_record.get("status") != "failed":
            raise TransitionError(f"old failure action {action_id} is missing or malformed")
        raw_skip = raw_record.get("legacy_failure_skip", _NO_VALUE)
        skip_state = group.skip_state(action_id)
        if (raw_skip is _NO_VALUE) != (skip_state is SkipState.ABSENT):
            raise TransitionError(f"old failure action {action_id} has malformed skip state")
        if skip_state is SkipState.CURRENT:
            raise TransitionError(f"old failure action {action_id} has a current skip")
        if "legacy_failure_update_ack" in raw_record:
            raise TransitionError("pending historical backlink exists without forward intent")
    _reject_extra_backlink_placement(snapshot, group)


def _validate_fresh_hold(
    group: HostFailureGroup,
    snapshot: JournalHostSnapshot,
    proof: HoldProof,
) -> None:
    if snapshot.complete is not False:
        raise TransitionError("complete historical journal cannot be reopened")
    status = snapshot.hold.get("status") if snapshot.hold is not None else None
    if status not in {"active", "cleanup-failed"}:
        raise TransitionError("released-only historical hold cannot mint an intent")
    allowed = {
        HoldProofKind.OBSERVED_INACTIVE: "observed-inactive",
        HoldProofKind.CONDITIONAL_ABSENCE_CONFIRMED: "conditional-absence-confirmed",
    }
    if proof.kind not in allowed:
        raise TransitionError("fresh intent requires a fresh authenticated hold proof")
    _validate_settled_hold_evidence(
        proof,
        group,
        required_outcome=allowed[proof.kind],
    )


def _validate_existing_backlink_hold(
    group: HostFailureGroup,
    snapshot: JournalHostSnapshot,
    proof: HoldProof,
    member: FailureMember,
) -> None:
    if snapshot.complete is not False:
        raise TransitionError("complete historical journal cannot be consumed")
    if proof.kind is not HoldProofKind.EXISTING_BACKLINK:
        raise TransitionError("exact backlink requires existing-backlink hold proof")
    if proof.settled_hold_sha256 != member.settled_hold_sha256:
        raise TransitionError("exact backlink hold proof digest conflicts with intent")
    if snapshot.retained_hold is not None:
        raise TransitionError("exact backlink cannot retain a historical hold")
    _validate_settled_hold_evidence(proof, group)
    if snapshot.hold is None:
        raise TransitionError("exact backlink requires the persisted released hold")
    if snapshot.hold.get("status") != "released":
        raise TransitionError("exact backlink historical hold is not released")
    if canonical_json_sha256(snapshot.hold) != member.settled_hold_sha256:
        raise TransitionError("exact backlink historical hold digest conflicts with intent")


def _validate_settled_hold_evidence(
    proof: HoldProof,
    group: HostFailureGroup,
    *,
    required_outcome: str | None = None,
) -> str:
    settled = proof.settled_hold
    if settled is None:
        raise TransitionError("hold proof lacks explicit settled hold evidence")
    evidence = settled.get("legacy_reconciliation")
    outcome = evidence.get("outcome") if isinstance(evidence, Mapping) else None
    if (
        settled.get("host") != group.host
        or settled.get("kind") != "full"
        or settled.get("status") != "released"
        or settled.get("owned") is not True
        or outcome not in {"observed-inactive", "conditional-absence-confirmed"}
    ):
        raise TransitionError("settled hold proof kind or content is not canonical")
    if required_outcome is not None and outcome != required_outcome:
        raise TransitionError("settled hold proof outcome conflicts with proof kind")
    assert isinstance(outcome, str)
    return outcome


def _validate_consumed_audit_group(
    group: HostFailureGroup,
    snapshot: JournalHostSnapshot,
    proof: HoldProof,
) -> tuple[object, ...]:
    observed: FailureUpdateBacklink | None = None
    for action_id in group.failed_action_ids:
        raw_record = snapshot.relevant_action_records.get(action_id)
        if not isinstance(raw_record, Mapping) or raw_record.get("status") != "failed":
            raise TransitionError(f"consumed failure action {action_id} is missing or malformed")
        raw = raw_record.get("legacy_failure_update_ack", _NO_VALUE)
        if raw is _NO_VALUE:
            raise TransitionError("consumed failure is missing its historical backlink")
        backlink = parse_failure_update_backlink(raw)
        if observed is None:
            observed = backlink
        elif observed != backlink:
            raise TransitionError("consumed failure backlinks conflict")
        if (
            "legacy_failure_skip" in raw_record
            or group.skip_state(action_id) is not SkipState.ABSENT
        ):
            raise TransitionError("consumed failure overlaps skip and backlink")
    assert observed is not None
    expected_member = _member_from_group(
        group,
        observed.settled_hold_sha256,
        include_consumed=True,
    )
    if (
        observed.host != group.host
        or observed.failed_action_ids != expected_member.failed_action_ids
        or observed.failed_operation_ids != expected_member.failed_operation_ids
        or not same_report(observed.historical_report_path, group.report.source_path)
        or observed.historical_report_digest_sha256 != group.report.digest_sha256
    ):
        raise TransitionError("consumed failure backlink does not bind the harvested failures")
    _reject_extra_backlink_placement(snapshot, group)
    _validate_existing_backlink_hold(group, snapshot, proof, expected_member)
    return (
        observed.schema,
        observed.host,
        observed.current_report_path,
        observed.current_report_digest_sha256,
        observed.current_rollout_id,
        observed.current_action_ids,
        observed.current_actions_sha256,
        observed.recorded_at,
    )


def _validate_terminal_receipt_current(
    recovery: HostRecoveryInput,
    context: tuple[object, ...],
) -> None:
    snapshot = recovery.current_snapshot
    receipt = recovery.receipt_report
    if snapshot is None or type(receipt) is not ReceiptReportRef:
        raise TransitionError("consumed audit requires an authenticated receipt current journal")
    (
        _schema,
        backlink_host,
        current_report_path,
        current_report_digest,
        current_rollout_id,
        current_action_ids,
        _locally_trusted_plan_hash,
        _locally_trusted_recorded_at,
    ) = context
    if backlink_host != recovery.host:
        raise TransitionError("receipt current host conflicts with historical backlinks")
    expected_report = ReportRef(
        _string(current_report_path, "receipt current report path"),
        _string(current_report_digest, "receipt current report digest"),
        _string(current_rollout_id, "receipt current rollout_id"),
    )
    if receipt.ref != expected_report or snapshot.report != expected_report:
        raise TransitionError("authenticated receipt current report conflicts with backlinks")
    if snapshot.forward_intent is not None:
        raise TransitionError("terminal receipt current journal still has a forward intent")
    if snapshot.complete is not False:
        raise TransitionError("terminal receipt current journal must remain complete false")
    failed_reason = canonical_host_failure_reason(recovery.failures)
    if snapshot.failed_reason != failed_reason:
        raise TransitionError("terminal receipt current failed reason is inconsistent")
    action_ids = current_action_ids
    if not isinstance(action_ids, tuple):
        raise TransitionError("terminal receipt current action IDs are malformed")
    reason = f"skipped: an earlier lane on {recovery.host} failed ({failed_reason})"
    expected_rows = {
        action_id: {"decision": "update", "reason": reason, "status": "not-run"}
        for action_id in action_ids
    }
    if set(snapshot.relevant_action_records) != set(expected_rows) or any(
        snapshot.relevant_action_records.get(action_id) != row
        for action_id, row in expected_rows.items()
    ):
        raise TransitionError("terminal receipt current acknowledgement rows are inconsistent")


def _reject_extra_backlink_placement(
    snapshot: JournalHostSnapshot,
    group: HostFailureGroup,
) -> None:
    expected = set(group.failed_action_ids)
    for action_id, raw_record in snapshot.relevant_action_records.items():
        if (
            action_id not in expected
            and isinstance(raw_record, Mapping)
            and "legacy_failure_update_ack" in raw_record
        ):
            raise TransitionError("historical backlink is placed on an unrelated action")


def _canonical_groups(
    failures: Sequence[HostFailureGroup],
) -> tuple[HostFailureGroup, ...]:
    unique: dict[tuple[str, str], HostFailureGroup] = {}
    for group in failures:
        key = (group.report.rollout_id, group.host)
        prior = unique.get(key)
        if prior is not None and prior != group:
            raise TransitionError("duplicate historical host source has conflicting content")
        unique[key] = group
    hosts = {item.host for item in unique.values()}
    if len(hosts) > 1:
        raise TransitionError("failure transition may cover only one host")
    return tuple(unique[key] for key in sorted(unique))


def _member_from_group(
    group: HostFailureGroup,
    settled_hold_sha256: str,
    *,
    include_consumed: bool = False,
) -> FailureMember:
    attempts = group.attempts if include_consumed else group.pending_attempts
    if not attempts:
        raise TransitionError("failure member requires pending attempts")
    return FailureMember(
        historical_rollout_id=group.report.rollout_id,
        historical_report_path=group.report.source_path,
        historical_report_digest_sha256=group.report.digest_sha256,
        failed_action_ids=tuple(sorted({item.action_id for item in attempts})),
        failed_operation_ids=tuple(sorted({item.operation_id for item in attempts})),
        failure_reason=group.reason,
        settled_hold_sha256=settled_hold_sha256,
    )


def _proof_for(proofs: Sequence[HoldProof], rollout_id: str) -> HoldProof:
    matches = [item for item in proofs if item.old_rollout_id == rollout_id]
    if len(matches) != 1:
        raise TransitionError(f"expected one settled hold proof for {rollout_id}")
    return matches[0]


def _rows_from_intent(intent: FailureUpdateIntent) -> dict[str, dict[str, object]]:
    reason = f"skipped: an earlier lane on {intent.host} failed ({intent.failed_host_reason})"
    return {
        action_id: {"decision": "update", "reason": reason, "status": "not-run"}
        for action_id in intent.current_action_ids
    }


def _current_ack_sha256(
    host: str,
    failed_reason: str,
    rows: Mapping[str, Mapping[str, object]],
) -> str:
    return canonical_json_sha256(
        {
            "complete": False,
            "failed_hosts": {host: failed_reason},
            "actions": dict(rows),
        }
    )


def _intent_report(intent: FailureUpdateIntent) -> ReportRef:
    return ReportRef(
        source_path=intent.current_report_path,
        digest_sha256=intent.current_report_digest_sha256,
        rollout_id=intent.current_rollout_id,
    )


def _vector_token(vector: MutationVector, rollout_id: str) -> JournalToken:
    matches = [
        item
        for item in object.__getattribute__(vector, "journals")
        if object.__getattribute__(item, "rollout_id") == rollout_id
    ]
    if len(matches) != 1:
        raise TransitionError(f"mutation vector has no unique token for {rollout_id}")
    return matches[0]


def _require_effect_vector(
    vector: MutationVector,
    report: ReportRef,
    host: str,
    *,
    allow_missing: bool = False,
) -> None:
    if type(vector) is not MutationVector:
        raise TypeError("mutation effect requires an exact MutationVector")
    if type(report) is not ReportRef:
        raise TypeError("mutation effect report must be an exact ReportRef")
    _require_text(host, "mutation effect host")
    token = _vector_token(vector, report.rollout_id)
    if token.digest_sha256 is None and not allow_missing:
        raise TransitionError("mutation effect cannot target an absent journal")


def _reject(host: str, reason: str) -> HostTransition:
    effect = Reject(host=host, reason=reason)
    _seal_planner_effect(effect)
    return _build_transition(
        TransitionPhase.PREVALIDATED,
        host,
        (effect,),
    )


def _parse_failure_member(raw: object) -> FailureMember:
    fields = {
        "historical_rollout_id",
        "historical_report_path",
        "historical_report_digest_sha256",
        "failed_action_ids",
        "failed_operation_ids",
        "failure_reason",
        "settled_hold_sha256",
    }
    value = _closed_mapping(raw, fields, "failure update source")
    return FailureMember(
        historical_rollout_id=_string(value["historical_rollout_id"], "source rollout_id"),
        historical_report_path=_string(value["historical_report_path"], "source report path"),
        historical_report_digest_sha256=_string(
            value["historical_report_digest_sha256"], "source report digest"
        ),
        failed_action_ids=_string_tuple(value["failed_action_ids"], "source action IDs"),
        failed_operation_ids=_string_tuple(value["failed_operation_ids"], "source operation IDs"),
        failure_reason=_string(value["failure_reason"], "source reason"),
        settled_hold_sha256=_string(value["settled_hold_sha256"], "source settled hold digest"),
    )


def _closed_mapping(raw: object, fields: set[str], label: str) -> Mapping[str, object]:
    if not isinstance(raw, Mapping) or set(raw) != fields:
        raise TransitionError(f"{label} must be a mapping with exact fields")
    if not all(isinstance(key, str) for key in raw):
        raise TransitionError(f"{label} keys must be strings")
    return raw


def _string(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise TransitionError(f"{label} must be a string")
    return value


def _string_tuple(value: object, label: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise TransitionError(f"{label} must be a list of strings")
    return tuple(value)


def _require_text(value: object, label: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise TransitionError(f"{label} must be a nonempty string")


def _require_sha256(value: object, label: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise TransitionError(f"{label} must be a lowercase SHA-256")


def _require_sha1(value: object, label: str) -> None:
    if not isinstance(value, str) or _SHA1.fullmatch(value) is None:
        raise TransitionError(f"{label} must be a lowercase 40-hex SHA")


def _require_ids(values: tuple[str, ...], label: str, *, sha: bool) -> None:
    if not isinstance(values, tuple) or not values or values != tuple(sorted(set(values))):
        raise TransitionError(f"{label} must be sorted, unique, and nonempty")
    for value in values:
        (_require_sha256 if sha else _require_text)(value, label)


def _require_aware_timestamp(value: object, label: str) -> None:
    _require_text(value, label)
    assert isinstance(value, str)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise TransitionError(f"{label} must be an ISO-8601 timestamp") from exc
    if parsed.utcoffset() is None:
        raise TransitionError(f"{label} must include a UTC offset")


def _require_latest(value: object) -> LatestReportRef:
    if type(value) is not LatestReportRef:
        raise TypeError("latest-report planning requires LatestReportRef")
    return value
