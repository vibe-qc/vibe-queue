"""Calibrate estimates on a training set, to be judged on a held-out one (#159).

A calibration is fitted on a ``vq.estimate-evaluation-set/1`` used only for
training and written as ``vq.estimate-calibration/1``. It is evaluated by
``vq estimate-evaluate HELD_OUT --calibration``, which refuses the training
set and any case of it. It never changes a receipt, an admission or a
placement.

Cases are grouped by everything that makes estimates comparable: estimator,
program, program version, method, algorithm, memory scope and hardware
class. Only decided evidence counts:

* an *exact* observed/predicted peak ratio, where the measurement compares
  with the estimate: a rank-scope estimate, or a single-process run;
* a *lower bound* on that ratio otherwise: a multi-process run that
  measurably exceeded a job- or node-scope estimate (the job held at least
  its largest process), or an OOM kill at a comparable limit at or above the
  estimate (the job needed more than the limit).

Not-refuted, censored, unobserved, excluded and no-prediction cases are
counted and never used. A group's multiplier is its worst ratio, never below
1, so a calibration never lowers a program's estimate; it is flagged when
that worst case is only a lower bound. A group with fewer decided cases than
``min_cases``, chosen before fitting, stays uncalibrated. The multiplier is a
worst-case ratio on the training cases, not a confidence bound.

Runtime is calibrated the same way in separate ``runtime_groups``, keyed
without the memory scope, against the upper bound of a runtime interval: a
completed, measured run gives the exact wall/upper ratio; a run stopped at its
time limit a lower bound of the larger of its wall time and limit over the
upper bound; another stopped run a lower bound only when it ran past that
bound. Only the upper bound is scaled. Applying a calibration to an estimate
also records its calibrated runtime upper bound, which only
``vq submit --time-from-estimate`` turns into a limit.

Each group also records, in ``feature_ranges``, the smallest and largest
value of every numeric feature its decided cases had (#201). A multiplier
says nothing about an input outside that range, so applying a calibration
leaves such an estimate uncalibrated, as it does one that lacks a feature the
group has a range for. A calibration fitted before ranges were recorded has
none and checks nothing.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Callable
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from vq.estimate_evaluation import (
    _CENSORED,
    EvaluationError,
    LoadedCase,
    _memory_verdict,
    _read,
    _runtime_verdict,
    load_set,
)

CALIBRATION_SCHEMA = "vq.estimate-calibration/1"
METHOD = "worst-observed-over-predicted-peak/1"
RUNTIME_METHOD = "worst-observed-over-predicted-runtime-upper/1"
DEFAULT_MIN_CASES = 5
MAX_CALIBRATION_BYTES = 16 * 1024 * 1024
KEY_FIELDS = ("estimator", "program", "program_version", "method", "algorithm",
              "memory_scope", "hardware_class")
RUNTIME_KEY_FIELDS = ("estimator", "program", "program_version", "method", "algorithm",
                      "hardware_class")
RangeFeature = Literal["nbf", "naux", "occupied", "virtual", "roots", "kpoints",
                       "mpi_ranks", "threads_per_rank"]
RANGE_FEATURES: tuple[RangeFeature, ...] = ("nbf", "naux", "occupied", "virtual", "roots",
                                            "kpoints", "mpi_ranks", "threads_per_rank")
LIMITATIONS = (
    "Runtime calibration scales only the upper bound of a runtime interval.",
    "A multiplier is the worst observed/predicted ratio on the training cases, not a "
    "confidence bound; a lower-bound multiplier may still be too small.",
    "A calibration describes only the estimator, program version, method, scope and "
    "hardware class it was fitted on; other cases stay uncalibrated.",
    "A multiplier applies only within the feature ranges its decided cases covered.",
)

Ratio = Annotated[float, Field(ge=1.0, allow_inf_nan=False)]
Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
DimensionCount = Annotated[int, Field(ge=0)]


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)


class _Evidence(_Model):
    cases: Annotated[int, Field(ge=0)]
    exact: Annotated[int, Field(ge=0)]
    lower_bounds: Annotated[int, Field(ge=0)]
    status: Literal["calibrated", "insufficient"]
    multiplier: Ratio | None
    multiplier_is_lower_bound: bool
    # The [smallest, largest] value of each numeric feature the group's decided
    # cases had (#201); absent from calibrations fitted before ranges existed.
    feature_ranges: dict[RangeFeature, tuple[DimensionCount, DimensionCount]] | None = None

    @field_validator("feature_ranges")
    @classmethod
    def valid_ranges(
        cls, value: dict[RangeFeature, tuple[int, int]] | None,
    ) -> dict[RangeFeature, tuple[int, int]] | None:
        for name, (lower, upper) in (value or {}).items():
            if lower > upper or (lower == 0 and name not in ("occupied", "virtual")):
                raise ValueError("invalid calibration dimension range")
        return value


class RuntimeGroup(_Evidence):
    """A runtime calibration group: its multiplier scales the interval's upper bound."""

    estimator: str
    program: str
    program_version: str | None
    method: str | None
    algorithm: str | None
    hardware_class: str

    def key(self) -> tuple[object, ...]:
        return tuple(getattr(self, name) for name in RUNTIME_KEY_FIELDS)


class Group(_Evidence):
    estimator: str
    program: str
    program_version: str | None
    method: str | None
    algorithm: str | None
    memory_scope: Literal["job", "node", "rank", "unspecified"]
    hardware_class: str

    def key(self) -> tuple[object, ...]:
        return tuple(getattr(self, name) for name in KEY_FIELDS)


class Calibration(_Model):
    schema_name: Literal["vq.estimate-calibration/1"]
    calibration_id: Digest
    method: Literal["worst-observed-over-predicted-peak/1"]
    min_cases: Annotated[int, Field(ge=1)]
    training_set_id: str
    training_set_sha256: Digest
    #: (estimate, usage) receipt digests of every training case, so a held-out
    #: evaluation can refuse a case it was fitted on.
    training_cases: list[tuple[Digest, Digest | None]] = Field(max_length=10_000)
    groups: list[Group]
    # Absent from calibrations fitted before runtime was calibrated.
    runtime_method: Literal["worst-observed-over-predicted-runtime-upper/1"] | None = None
    runtime_groups: list[RuntimeGroup] = Field(default_factory=list)
    limitations: list[str]


def group_key(item: LoadedCase) -> tuple[object, ...]:
    estimate = item.estimate
    return (estimate.estimator, estimate.program, estimate.program_version,
            estimate.features.method, estimate.features.algorithm, estimate.memory.scope,
            item.case.hardware_class)


def out_of_range(group: _Evidence, features: Any) -> str | None:
    """Why ``features`` lie outside ``group``'s observed ranges, or None if they do not."""
    for name, (low, high) in sorted((group.feature_ranges or {}).items()):
        value = getattr(features, name)
        if value is None:
            return (f"the estimate has no {name}, so it cannot be placed in the "
                    f"calibration's observed range {low}-{high}")
        if not low <= value <= high:
            return (f"its {name} {value} is outside the calibration's observed "
                    f"range {low}-{high}")
    return None


def runtime_group_key(item: LoadedCase) -> tuple[object, ...]:
    estimate = item.estimate
    return (estimate.estimator, estimate.program, estimate.program_version,
            estimate.features.method, estimate.features.algorithm, item.case.hardware_class)


def runtime_evidence(item: LoadedCase) -> tuple[str, float] | None:
    """What this case says about its runtime upper bound, like ``evidence``."""
    case, estimate, usage = item.case, item.estimate, item.usage
    verdict = _runtime_verdict(case, estimate, usage)
    if verdict not in {"covered", "below", "above"}:
        return None
    assert estimate.runtime_seconds is not None
    upper = estimate.runtime_seconds[1]
    if upper == 0:
        return None  # no ratio to a zero bound
    wall = usage["wall_seconds"] if usage else None
    if case.outcome == "time_exceeded":
        # It needed more than it got: at least its wall time and its limit.
        floor = max(x for x in (wall, case.wall_limit_seconds) if x is not None)
        return "lower_bound", floor / upper
    assert wall is not None
    return ("lower_bound" if case.outcome in _CENSORED else "exact"), wall / upper


def evidence(item: LoadedCase) -> tuple[str, float] | None:
    """``("exact", ratio)``, ``("lower_bound", ratio)`` or None: what this case says."""
    case, estimate, usage = item.case, item.estimate, item.usage
    predicted = estimate.memory.peak_bytes
    if predicted is None:
        return None
    verdict = _memory_verdict(case, estimate, usage)
    observed = usage["peak_rss_bytes"] if usage else None
    if verdict == "within" and observed is not None:
        return "exact", observed / predicted
    if verdict != "underpredicted":
        return None
    if observed is not None and observed > predicted:
        comparable = estimate.memory.scope == "rank" or (
            usage is not None and usage["process_count"] == 1)
        return ("exact" if comparable else "lower_bound"), observed / predicted
    # An OOM kill at a comparable limit at or above the estimate.
    assert case.memory_limit_bytes is not None
    return "lower_bound", case.memory_limit_bytes / predicted


def _digest(document: dict[str, Any]) -> str:
    body = {k: v for k, v in document.items() if k != "calibration_id"}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def _fit_groups(
    loaded: list[LoadedCase], key_of: Callable[[LoadedCase], tuple[object, ...]],
    evidence_of: Callable[[LoadedCase], tuple[str, float] | None],
    fields: tuple[str, ...], min_cases: int,
) -> list[dict[str, Any]]:
    groups: dict[tuple[object, ...], dict[str, Any]] = {}
    for item in loaded:
        group = groups.setdefault(key_of(item), {"cases": 0, "exact": [], "lower": [],
                                                 "ranges": {}})
        group["cases"] += 1
        found = evidence_of(item)
        if found is not None:
            group["exact" if found[0] == "exact" else "lower"].append(found[1])
            for name in RANGE_FEATURES:
                value = getattr(item.estimate.features, name)
                if value is not None:
                    low, high = group["ranges"].get(name, (value, value))
                    group["ranges"][name] = (min(low, value), max(high, value))
    records = []
    for key, group in sorted(groups.items(), key=lambda item: json.dumps(item[0])):
        decided = len(group["exact"]) + len(group["lower"])
        worst = max([*group["exact"], *group["lower"]], default=None)
        calibrated = decided >= min_cases and worst is not None
        records.append({
            **dict(zip(fields, key, strict=True)),
            "cases": group["cases"], "exact": len(group["exact"]),
            "lower_bounds": len(group["lower"]),
            "status": "calibrated" if calibrated else "insufficient",
            "multiplier": round(max(1.0, worst), 6) if calibrated and worst is not None
            else None,
            "multiplier_is_lower_bound": bool(
                calibrated and group["lower"] and max(group["lower"]) == worst
                and worst > 1.0),
            "feature_ranges": {name: list(bounds)
                               for name, bounds in sorted(group["ranges"].items())},
        })
    return records


def fit(set_file: Path, *, min_cases: int = DEFAULT_MIN_CASES) -> dict[str, Any]:
    """The ``vq.estimate-calibration/1`` document for the training set at ``set_file``."""
    if min_cases < 1:
        raise EvaluationError("min_cases must be at least 1")
    evaluation_set, raw, loaded = load_set(set_file)
    document: dict[str, Any] = {
        "schema_name": CALIBRATION_SCHEMA, "calibration_id": "", "method": METHOD,
        "min_cases": min_cases, "training_set_id": evaluation_set.set_id,
        "training_set_sha256": hashlib.sha256(raw).hexdigest(),
        # A kill may have left no usage receipt: its digest is None.
        "training_cases": sorted(([item.estimate_sha256, item.usage_sha256]
                                  for item in loaded),
                                 key=lambda pair: (pair[0], pair[1] or "")),
        "groups": _fit_groups(loaded, group_key, evidence, KEY_FIELDS, min_cases),
        "runtime_method": RUNTIME_METHOD,
        "runtime_groups": _fit_groups(loaded, runtime_group_key, runtime_evidence,
                                      RUNTIME_KEY_FIELDS, min_cases),
        "limitations": list(LIMITATIONS),
    }
    document["calibration_id"] = _digest(document)
    Calibration.model_validate_json(json.dumps(document))
    return document


def load(path: Path) -> Calibration:
    """A calibration document, validated and checked against its own identity."""
    raw = _read(path, MAX_CALIBRATION_BYTES)
    try:
        document = json.loads(raw)
        calibration = Calibration.model_validate_json(raw)
    except ValueError as exc:
        raise EvaluationError(f"{path}: not a {CALIBRATION_SCHEMA} document: {exc}") from None
    if _digest(document) != calibration.calibration_id:
        raise EvaluationError(f"{path}: its calibration_id does not match its content")
    if (calibration.runtime_method is None) != ("runtime_groups" not in document):
        raise EvaluationError(f"{path}: runtime_method and runtime_groups go together")
    for group in [*calibration.groups, *calibration.runtime_groups]:
        if (group.status == "calibrated") != (group.multiplier is not None):
            raise EvaluationError(f"{path}: a group's status and multiplier disagree")
    return calibration


def document_json(document: dict[str, Any]) -> str:
    return json.dumps(document, indent=2, sort_keys=True, allow_nan=False) + "\n"


# --- applying a calibration to an estimate (#159) -----------------------------

CALIBRATED_SCHEMA = "vq.calibrated-estimate/1"
Label = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,63}$")]


def receipt_digest(record: dict[str, Any]) -> str:
    """The digest a calibrated estimate names its raw receipt by."""
    return hashlib.sha256(json.dumps(record, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


class CalibratedEstimate(_Model):
    """A raw ``vq.resource-estimate/1`` receipt and what a calibration makes of it.

    The receipt itself is never changed; its calibrated peak sits beside it.
    """

    schema_name: Literal["vq.calibrated-estimate/1"]
    estimate: dict[str, Any]
    estimate_sha256: Digest
    calibration_id: Digest
    calibration_method: Literal["worst-observed-over-predicted-peak/1"]
    training_set_id: str
    hardware_class: Label
    status: Literal["calibrated", "uncalibrated"]
    reason: str | None
    multiplier: Ratio | None
    multiplier_is_lower_bound: bool
    calibrated_peak_bytes: Annotated[int, Field(ge=1)] | None
    limitations: list[str]
    # The runtime upper bound's calibration; absent from documents written
    # before runtime groups were applied.
    runtime_status: Literal["calibrated", "uncalibrated"] | None = None
    runtime_reason: str | None = None
    runtime_multiplier: Ratio | None = None
    runtime_multiplier_is_lower_bound: bool = False
    calibrated_runtime_upper_seconds: Annotated[
        float, Field(gt=0, allow_inf_nan=False)] | None = None


_RUNTIME_FIELDS = ("runtime_status", "runtime_reason", "runtime_multiplier",
                   "runtime_multiplier_is_lower_bound", "calibrated_runtime_upper_seconds")


def _apply_runtime(receipt: Any, calibration: Calibration,
                   hardware_class: str) -> dict[str, Any]:
    key = (receipt.estimator, receipt.program, receipt.program_version,
           receipt.features.method, receipt.features.algorithm, hardware_class)
    group = next((g for g in calibration.runtime_groups if g.key() == key), None)
    interval = receipt.runtime_seconds
    reason: str | None = None
    if interval is None:
        reason = "the estimate has no runtime interval"
    elif interval[1] == 0:
        reason = "the estimate's runtime upper bound is zero"
    elif group is None:
        reason = ("no runtime group of this calibration matches the estimate's estimator, "
                  "program version, method, algorithm and hardware class")
    elif group.multiplier is None:
        reason = (f"its runtime group has {group.exact + group.lower_bounds} decided "
                  f"case(s), fewer than the calibration's {calibration.min_cases}")
    else:
        reason = out_of_range(group, receipt.features)
    if reason is not None or group is None or group.multiplier is None or interval is None:
        return {"runtime_status": "uncalibrated", "runtime_reason": reason,
                "runtime_multiplier": None, "runtime_multiplier_is_lower_bound": False,
                "calibrated_runtime_upper_seconds": None}
    return {"runtime_status": "calibrated", "runtime_reason": None,
            "runtime_multiplier": group.multiplier,
            "runtime_multiplier_is_lower_bound": group.multiplier_is_lower_bound,
            "calibrated_runtime_upper_seconds": round(interval[1] * group.multiplier, 6)}


def apply(receipt: Any, calibration: Calibration, *, hardware_class: str) -> dict[str, Any]:
    """The ``vq.calibrated-estimate/1`` for ``receipt`` on ``hardware_class``."""
    from vq.resource_estimate import receipt_record

    record = receipt_record(receipt)
    key = (receipt.estimator, receipt.program, receipt.program_version,
           receipt.features.method, receipt.features.algorithm, receipt.memory.scope,
           hardware_class)
    group = next((g for g in calibration.groups if g.key() == key), None)
    peak = receipt.memory.peak_bytes
    reason: str | None = None
    if peak is None:
        reason = "the estimate has no memory figure"
    elif group is None:
        reason = ("no group of this calibration matches the estimate's estimator, program "
                  "version, method, algorithm, memory scope and hardware class")
    elif group.multiplier is None:
        reason = (f"its group has {group.exact + group.lower_bounds} decided case(s), fewer "
                  f"than the calibration's {calibration.min_cases}")
    else:
        reason = out_of_range(group, receipt.features)
    calibrated = reason is None and group is not None and group.multiplier is not None
    document = {
        "schema_name": CALIBRATED_SCHEMA, "estimate": record,
        "estimate_sha256": receipt_digest(record),
        "calibration_id": calibration.calibration_id,
        "calibration_method": calibration.method,
        "training_set_id": calibration.training_set_id, "hardware_class": hardware_class,
        "status": "calibrated" if calibrated else "uncalibrated", "reason": reason,
        "multiplier": group.multiplier if calibrated and group is not None else None,
        "multiplier_is_lower_bound": bool(calibrated and group is not None
                                          and group.multiplier_is_lower_bound),
        "calibrated_peak_bytes": (math.ceil(peak * group.multiplier)
                                  if calibrated and group is not None and peak is not None
                                  and group.multiplier is not None else None),
        "limitations": list(calibration.limitations),
        **_apply_runtime(receipt, calibration, hardware_class),
    }
    check_calibrated(document)
    return document


def check_calibrated(document: dict[str, Any]) -> tuple[Any, dict[str, Any]]:
    """The raw receipt and the calibration record of a ``vq.calibrated-estimate/1``.

    The inner receipt is validated as a receipt and must have the named digest;
    a calibrated status needs its multiplier and the exact calibrated peak.
    Raises EvaluationError otherwise.
    """
    from vq.resource_estimate import EstimateError, load_receipt

    try:
        calibrated = CalibratedEstimate.model_validate_json(
            json.dumps(document, allow_nan=False))
        receipt = load_receipt(json.dumps(calibrated.estimate))
    except (ValueError, EstimateError) as exc:
        raise EvaluationError(f"not a {CALIBRATED_SCHEMA} document: {exc}") from None
    if receipt_digest(calibrated.estimate) != calibrated.estimate_sha256:
        raise EvaluationError("its estimate does not match its estimate_sha256")
    peak = receipt.memory.peak_bytes
    if calibrated.status == "calibrated":
        if (calibrated.multiplier is None or peak is None or calibrated.reason is not None
                or calibrated.calibrated_peak_bytes != math.ceil(peak * calibrated.multiplier)):
            raise EvaluationError("its calibrated peak is not the estimate times its multiplier")
    elif (calibrated.multiplier is not None or calibrated.calibrated_peak_bytes is not None
          or calibrated.reason is None or calibrated.multiplier_is_lower_bound):
        raise EvaluationError("an uncalibrated estimate carries no multiplier and says why")
    _check_runtime(calibrated, receipt)
    exclude = {"estimate", *(_RUNTIME_FIELDS if calibrated.runtime_status is None else ())}
    record = calibrated.model_dump(mode="json", exclude=exclude)
    return receipt, record


def _check_runtime(calibrated: CalibratedEstimate, receipt: Any) -> None:
    multiplier = calibrated.runtime_multiplier
    upper = calibrated.calibrated_runtime_upper_seconds
    if calibrated.runtime_status is None:
        if (multiplier is not None or upper is not None or calibrated.runtime_reason
                is not None or calibrated.runtime_multiplier_is_lower_bound):
            raise EvaluationError("runtime calibration fields need a runtime_status")
    elif calibrated.runtime_status == "calibrated":
        interval = receipt.runtime_seconds
        if (multiplier is None or interval is None or calibrated.runtime_reason is not None
                or upper != round(interval[1] * multiplier, 6)):
            raise EvaluationError("its calibrated runtime upper bound is not the estimate's "
                                  "upper bound times its runtime multiplier")
    elif (multiplier is not None or upper is not None or calibrated.runtime_reason is None
          or calibrated.runtime_multiplier_is_lower_bound):
        raise EvaluationError("an uncalibrated runtime carries no multiplier and says why")
