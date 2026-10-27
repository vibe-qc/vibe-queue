"""Held-out evaluation of resource estimates against measured usage (#159).

An evaluation set pairs ``vq.resource-estimate/1`` receipts with the
queue's own ``resource-usage.json`` receipts and each job's outcome and
hardware class. The report says, per method and hardware class, how often
memory was underpredicted and how often a runtime interval covered the
measured wall time, with sample sizes and every exclusion.

What a receipt measures sets what can be concluded. Its peak RSS is the
largest single process (``ru_maxrss``), not a sum over concurrent
processes, so:

* measured above predicted is an underprediction, whatever the estimate's
  scope: no process, rank, node or job total can be below its largest
  process;
* measured at or below predicted confirms a ``rank``-scope estimate, and a
  job- or node-scope one only for a single-process run; otherwise it fails
  to refute it;
* an OOM kill, a time limit or a kill censors the measurement: the true
  need is at least what was seen. An OOM kill at a limit at or above the
  estimate refutes it only when the limit covers the same or a narrower
  scope: exceeding a job total cannot refute a per-rank estimate;
* a failed run is not a measurement and is excluded.

Where a verdict cannot be decided the frequency is reported as bounds, not
a point. There is no tunable threshold, so nothing is chosen after seeing
the data; the report binds the set's digest and every receipt's digest.

Every report also has ``route_groups``, keyed by method, algorithm and
hardware class (#201). An optional ``vq.estimate-thresholds/1`` document,
written before the set is seen, gives each route group an acceptance verdict
from its underprediction bounds; the report binds the document's digest.
"""

from __future__ import annotations

import hashlib
import json
import math
import statistics
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from vq.resource_estimate import ResourceEstimate
from vq.resource_receipt import RESOURCE_USAGE_SCHEMA as DIRECT_USAGE_SCHEMA

if TYPE_CHECKING:
    from vq.estimate_calibration import Calibration

SET_SCHEMA = "vq.estimate-evaluation-set/1"
REPORT_SCHEMA = "vq.estimate-evaluation/1"
#: Written by the scheduler job wrapper (``scheduler_dispatch``).
SCHEDULER_USAGE_SCHEMA = "vq.scheduler-resource-usage.v1"

MAX_SET_BYTES = 16 * 1024 * 1024
MAX_RECEIPT_BYTES = 1024 * 1024

Label = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,63}$")]
Outcome = Literal["completed", "failed", "oom_killed", "time_exceeded", "killed"]
_CENSORED = frozenset({"oom_killed", "time_exceeded", "killed"})

LIMITATIONS = (
    "Measured peak RSS is the largest single process (ru_maxrss), not a sum over "
    "concurrent processes; a job- or node-scope estimate is confirmed only for a "
    "single-process run.",
    "OOM, time-limit and kill outcomes are censored: the true need is at least what "
    "was measured.",
    "An OOM limit refutes an estimate only at the same or a narrower scope; "
    "unknown scopes remain undecided without a measured excess.",
    "Undecided cases widen the reported bounds; they are never counted as correct.",
    "Results describe only the estimators, programs, methods and hardware classes "
    "listed, on this set.",
)


class EvaluationError(ValueError):
    """The set or one of its receipts cannot be evaluated."""


def _relative(value: str) -> str:
    if (not value or len(value) > 1024 or value.startswith("/") or "\\" in value
            or "\0" in value or any(p in ("", ".", "..") for p in value.split("/"))):
        raise ValueError(f"path {value!r} must be relative to the set file, without '..'")
    return value


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)


class Case(_Model):
    case_id: Label
    #: The estimate receipt, relative to the set file.
    estimate: str
    #: The job's resource-usage.json; absent when the job left none (a kill).
    usage: str | None = None
    outcome: Outcome
    hardware_class: Label
    #: An enforced memory limit, which turns an OOM kill into scoped evidence.
    memory_limit_bytes: Annotated[int, Field(ge=1)] | None = None
    #: Existing sets describe a total-job limit unless they declare otherwise.
    memory_limit_scope: Literal["job", "node", "rank", "unspecified"] = "job"
    #: The job's wall-time limit, which bounds a time-limited run.
    wall_limit_seconds: Annotated[int, Field(ge=1)] | None = None

    check_estimate = field_validator("estimate")(_relative)

    @field_validator("usage")
    @classmethod
    def _usage(cls, value: str | None) -> str | None:
        return None if value is None else _relative(value)


THRESHOLDS_SCHEMA = "vq.estimate-thresholds/1"
MAX_THRESHOLDS_BYTES = 64 * 1024


class Thresholds(_Model):
    """Acceptance thresholds for route groups, fixed before a set is evaluated (#201)."""

    schema_name: Literal["vq.estimate-thresholds/1"]
    description: Annotated[str, Field(max_length=1024)] | None = None
    #: The largest acceptable memory underprediction frequency.
    max_underprediction: Annotated[float, Field(ge=0.0, le=1.0)]
    #: Cases a route group needs before it is judged at all.
    min_cases: Annotated[int, Field(ge=1)]
    #: Judge the raw estimates, or the calibrated ones (needs a calibration).
    basis: Literal["raw", "calibrated"] = "raw"


def load_thresholds(path: Path) -> tuple[Thresholds, str]:
    """The thresholds document at ``path`` and its SHA-256."""
    raw = _read(path, MAX_THRESHOLDS_BYTES)
    try:
        return Thresholds.model_validate_json(raw), hashlib.sha256(raw).hexdigest()
    except ValueError as exc:
        raise EvaluationError(f"{path}: not a {THRESHOLDS_SCHEMA} document: {exc}") from None


def _acceptance(summary: dict[str, Any], thresholds: Thresholds) -> dict[str, Any]:
    memory = summary["memory"]
    bounds = (memory["calibrated"]["underprediction"] if thresholds.basis == "calibrated"
              else memory["underprediction"])
    limit = thresholds.max_underprediction
    if bounds["n"] < thresholds.min_cases:
        verdict = "insufficient"
        reason = f"{bounds['n']} case(s), fewer than the thresholds' {thresholds.min_cases}"
    elif bounds["lower"] > limit:
        verdict, reason = "fail", f"underprediction is at least {bounds['lower']:.2%}"
    elif bounds["upper"] <= limit:
        verdict, reason = "pass", f"underprediction is at most {bounds['upper']:.2%}"
    else:
        verdict = "undecided"
        reason = (f"underprediction lies between {bounds['lower']:.2%} and "
                  f"{bounds['upper']:.2%}, across the limit")
    return {"verdict": verdict, "reason": reason, "underprediction": bounds}


class EvaluationSet(_Model):
    schema_name: Literal["vq.estimate-evaluation-set/1"]
    set_id: Label
    description: Annotated[str, Field(max_length=1024)] | None = None
    cases: list[Case] = Field(min_length=1, max_length=10_000)

    @model_validator(mode="after")
    def _unique(self) -> EvaluationSet:
        ids = [c.case_id for c in self.cases]
        if len(set(ids)) != len(ids):
            raise ValueError("each case_id may appear once")
        return self


def _read(path: Path, limit: int) -> bytes:
    if not path.is_file():
        raise EvaluationError(f"{path}: not a regular file")
    with path.open("rb") as handle:
        data = handle.read(limit + 1)
    if len(data) > limit:
        raise EvaluationError(f"{path}: larger than {limit} bytes")
    return data


def _usage(data: bytes, where: str) -> dict[str, Any]:
    """The fields of a resource-usage receipt this evaluation relies on."""
    try:
        payload = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvaluationError(f"{where}: not JSON: {exc}") from None
    if not isinstance(payload, dict) or payload.get("schema") not in {
            DIRECT_USAGE_SCHEMA, SCHEDULER_USAGE_SCHEMA}:
        raise EvaluationError(f"{where}: not a vq resource-usage receipt")
    if payload.get("status") != "ok":
        return {"ok": False, "peak_rss_bytes": None, "wall_seconds": None,
                "process_count": None}

    command_status = payload.get("command_status")
    if command_status not in ("succeeded", "failed"):
        raise EvaluationError(f"{where}: command_status must be succeeded or failed")
    exit_code = payload.get("command_exit_code")
    if isinstance(exit_code, bool) or not isinstance(exit_code, int):
        raise EvaluationError(f"{where}: command_exit_code must be an integer")
    if (command_status == "succeeded") != (exit_code == 0):
        raise EvaluationError(f"{where}: command_status and command_exit_code disagree")

    def number(name: str) -> float | None:
        value = payload.get(name)
        if value is None:
            return None
        message = f"{where}: {name} must be a finite non-negative number"
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise EvaluationError(message)
        try:
            normalized = float(value)
        except OverflowError:
            raise EvaluationError(message) from None
        if not math.isfinite(normalized) or normalized < 0:
            raise EvaluationError(message)
        return normalized

    rss_kb = number("peak_rss_kb")
    if rss_kb is not None and not math.isfinite(rss_kb * 1024):
        raise EvaluationError(f"{where}: peak_rss_kb is too large to convert to bytes")
    count = payload.get("process_count")
    if count is not None and (isinstance(count, bool) or not isinstance(count, int)
                              or count < 0):
        raise EvaluationError(f"{where}: process_count must be a non-negative integer")
    return {"ok": True, "command_status": command_status,
            "peak_rss_bytes": None if rss_kb is None else int(rss_kb * 1024),
            "wall_seconds": number("wall_seconds"), "process_count": count}


def _memory_verdict(case: Case, estimate: ResourceEstimate,
                    usage: dict[str, Any] | None) -> str:
    predicted = estimate.memory.peak_bytes
    if predicted is None:
        return "no_prediction"
    if case.outcome == "failed":
        return "excluded"
    if usage is not None and not usage["ok"]:
        return "excluded"
    observed = usage["peak_rss_bytes"] if usage else None
    if observed is not None and observed > predicted:
        return "underpredicted"
    # Exceeding a rank/node cap also refutes a wider estimate. Exceeding a
    # job cap cannot establish that any individual rank/node needs too much.
    comparable_limit = case.memory_limit_scope in {
        "job": ("job", "node", "rank"),
        "node": ("node", "rank"),
        "rank": ("rank",),
    }.get(estimate.memory.scope, ())
    if (case.outcome == "oom_killed" and case.memory_limit_bytes is not None
            and comparable_limit and predicted <= case.memory_limit_bytes):
        return "underpredicted"
    if case.outcome in _CENSORED:
        return "censored"
    if observed is None:
        return "unobserved"
    single = usage is not None and usage["process_count"] == 1
    return "within" if estimate.memory.scope == "rank" or single else "not_refuted"


def _runtime_verdict(case: Case, estimate: ResourceEstimate,
                     usage: dict[str, Any] | None) -> str:
    interval = estimate.runtime_seconds
    if interval is None:
        return "no_prediction"
    if case.outcome == "failed" or (usage is not None and not usage["ok"]):
        return "excluded"
    low, high = interval
    wall = usage["wall_seconds"] if usage else None
    if case.outcome == "time_exceeded":
        floor = max(x for x in (wall, case.wall_limit_seconds, 0.0) if x is not None)
        return "above" if floor > high else "censored"
    if case.outcome in _CENSORED:
        return "above" if wall is not None and wall > high else "censored"
    if wall is None:
        return "unobserved"
    if wall < low:
        return "below"
    return "above" if wall > high else "covered"


def _bounds(hits: int, open_cases: int, total: int) -> dict[str, object]:
    if total == 0:
        return {"lower": None, "upper": None, "n": 0}
    return {"lower": round(hits / total, 6), "upper": round((hits + open_cases) / total, 6),
            "n": total}


def _calibrated(item: LoadedCase, multiplier: float | None,
                runtime_multiplier: float | None) -> dict[str, Any]:
    """The case's verdicts for its estimate scaled by its groups' multipliers."""
    predicted = item.estimate.memory.peak_bytes
    interval = item.estimate.runtime_seconds
    row: dict[str, Any]
    if multiplier is None or predicted is None:
        row = {"calibration_multiplier": None, "calibrated_peak_bytes": None,
               "calibrated_memory": "uncalibrated"}
    else:
        scaled = math.ceil(predicted * multiplier)
        estimate = item.estimate.model_copy(update={
            "memory": item.estimate.memory.model_copy(update={"peak_bytes": scaled})})
        row = {"calibration_multiplier": multiplier, "calibrated_peak_bytes": scaled,
               "calibrated_memory": _memory_verdict(item.case, estimate, item.usage)}
    if runtime_multiplier is None or interval is None:
        return {**row, "runtime_calibration_multiplier": None,
                "calibrated_runtime_upper_seconds": None, "calibrated_runtime": "uncalibrated"}
    upper = round(interval[1] * runtime_multiplier, 6)
    estimate = item.estimate.model_copy(update={"runtime_seconds": (interval[0], upper)})
    return {**row, "runtime_calibration_multiplier": runtime_multiplier,
            "calibrated_runtime_upper_seconds": upper,
            "calibrated_runtime": _runtime_verdict(item.case, estimate, item.usage)}


def _summarise(rows: list[dict[str, Any]]) -> dict[str, object]:
    memory = Counter(r["memory"] for r in rows)
    runtime = Counter(r["runtime"] for r in rows)
    mem_total = sum(memory[k] for k in ("underpredicted", "within", "not_refuted",
                                        "censored", "unobserved"))
    run_total = sum(runtime[k] for k in ("covered", "above", "below", "censored",
                                         "unobserved"))
    ratios = sorted(r["observed_over_predicted"] for r in rows
                    if r["observed_over_predicted"] is not None)
    memory_summary: dict[str, object] = {}
    runtime_summary: dict[str, object] = {}
    if any("calibrated_runtime" in r for r in rows):
        calibrated_runtime = Counter(r["calibrated_runtime"] for r in rows)
        runtime_summary["calibrated"] = {
            "verdicts": dict(sorted(calibrated_runtime.items())),
            "coverage": _bounds(calibrated_runtime["covered"], calibrated_runtime["censored"]
                                + calibrated_runtime["unobserved"],
                                sum(calibrated_runtime[k] for k in (
                                    "covered", "above", "below", "censored", "unobserved"))),
            "uncalibrated": calibrated_runtime["uncalibrated"],
        }
    if any("calibrated_memory" in r for r in rows):
        # #159: the same bounds for the calibrated estimates; cases without a
        # calibrated group are counted apart, never as either kind.
        calibrated = Counter(r["calibrated_memory"] for r in rows)
        decided = sum(calibrated[k] for k in ("underpredicted", "within", "not_refuted",
                                              "censored", "unobserved"))
        memory_summary["calibrated"] = {
            "verdicts": dict(sorted(calibrated.items())),
            "underprediction": _bounds(calibrated["underpredicted"],
                                       calibrated["not_refuted"] + calibrated["censored"]
                                       + calibrated["unobserved"], decided),
            "uncalibrated": calibrated["uncalibrated"],
        }
    return {
        "cases": len(rows),
        "memory": {
            **memory_summary,
            "verdicts": dict(sorted(memory.items())),
            # Lower bound counts only decided underpredictions; the upper
            # adds every case that could still be one.
            "underprediction": _bounds(memory["underpredicted"],
                                       memory["not_refuted"] + memory["censored"]
                                       + memory["unobserved"], mem_total),
            "observed_over_predicted": None if not ratios else {
                "n": len(ratios), "median": round(statistics.median(ratios), 6),
                "max": round(ratios[-1], 6)},
        },
        "runtime": {
            **runtime_summary,
            "verdicts": dict(sorted(runtime.items())),
            "coverage": _bounds(runtime["covered"], runtime["censored"]
                                + runtime["unobserved"], run_total),
        },
    }


@dataclass(frozen=True)
class LoadedCase:
    """One case of a set with its validated receipts."""

    case: Case
    estimate: ResourceEstimate
    estimate_sha256: str
    usage: dict[str, Any] | None
    usage_sha256: str | None


def load_set(set_file: Path) -> tuple[EvaluationSet, bytes, list[LoadedCase]]:
    """The set at ``set_file``, its bytes, and its cases in id order."""
    raw = _read(set_file, MAX_SET_BYTES)
    try:
        evaluation_set = EvaluationSet.model_validate_json(raw)
    except ValueError as exc:
        raise EvaluationError(f"{set_file}: {exc}") from None
    base = set_file.parent
    loaded: list[LoadedCase] = []
    for case in sorted(evaluation_set.cases, key=lambda c: c.case_id):
        estimate_bytes = _read(base / case.estimate, MAX_RECEIPT_BYTES)
        try:
            estimate = ResourceEstimate.model_validate_json(estimate_bytes)
        except ValueError as exc:
            raise EvaluationError(f"case {case.case_id}: estimate receipt: {exc}") from None
        usage = None
        usage_digest = None
        if case.usage is not None:
            usage_bytes = _read(base / case.usage, MAX_RECEIPT_BYTES)
            usage = _usage(usage_bytes, f"case {case.case_id}: usage receipt")
            if (case.outcome == "completed" and usage["ok"]
                    and usage["command_status"] == "failed"):
                raise EvaluationError(f"case {case.case_id}: usage receipt: "
                                      "completed case records a failed command")
            usage_digest = hashlib.sha256(usage_bytes).hexdigest()
        loaded.append(LoadedCase(case, estimate, hashlib.sha256(estimate_bytes).hexdigest(),
                                 usage, usage_digest))
    return evaluation_set, raw, loaded


def evaluate(set_file: Path, calibration: Calibration | None = None,
             thresholds: tuple[Thresholds, str] | None = None) -> dict[str, object]:
    """The evaluation report for the set at ``set_file``.

    With ``calibration`` (#159), each case also gets the verdict for its
    estimate times its group's multiplier, ``calibrated_memory``, and for its
    runtime interval with the upper bound scaled, ``calibrated_runtime``; every
    summary gets the calibrated bounds of both. The set must be held out:
    the training set itself, or any of its cases, is refused.

    With ``thresholds`` (the document and its digest, #201), each route group
    gets an ``acceptance`` verdict; calibrated thresholds need a calibration.
    """
    from vq.estimate_calibration import (
        Group,
        RuntimeGroup,
        group_key,
        out_of_range,
        runtime_group_key,
    )

    if thresholds is not None and thresholds[0].basis == "calibrated" and calibration is None:
        raise EvaluationError("thresholds on calibrated estimates need --calibration")
    evaluation_set, raw, loaded = load_set(set_file)
    memory_groups: dict[tuple[object, ...], Group] = {}
    runtime_groups: dict[tuple[object, ...], RuntimeGroup] = {}
    if calibration is not None:
        if hashlib.sha256(raw).hexdigest() == calibration.training_set_sha256:
            raise EvaluationError(f"{set_file} is the set this calibration was fitted on; "
                                  "evaluate it on held-out cases")
        trained = {tuple(pair) for pair in calibration.training_cases}
        for item in loaded:
            if (item.estimate_sha256, item.usage_sha256) in trained:
                raise EvaluationError(f"case {item.case.case_id} is also a training case of "
                                      "this calibration; evaluate it on held-out cases")
        memory_groups = {group.key(): group for group in calibration.groups
                         if group.multiplier is not None}
        runtime_groups = {group.key(): group for group in calibration.runtime_groups
                          if group.multiplier is not None}
    rows: list[dict[str, Any]] = []
    for item in loaded:
        case, estimate, usage = item.case, item.estimate, item.usage
        usage_digest = item.usage_sha256
        predicted = estimate.memory.peak_bytes
        observed = usage["peak_rss_bytes"] if usage else None
        memory = _memory_verdict(case, estimate, usage)
        rows.append({
            "case_id": case.case_id,
            "method": estimate.features.method or "unknown",
            "algorithm": estimate.features.algorithm,
            "hardware_class": case.hardware_class,
            "outcome": case.outcome,
            "estimator": estimate.estimator,
            "calibration": estimate.calibration_id or estimate.calibration,
            "program_version": estimate.program_version,
            "memory_scope": estimate.memory.scope,
            "predicted_peak_bytes": predicted,
            "observed_peak_rss_bytes": observed,
            "observed_over_predicted": (
                round(observed / predicted, 6)
                if observed is not None and predicted and memory != "excluded" else None),
            "memory": memory,
            "runtime": _runtime_verdict(case, estimate, usage),
            "estimate_sha256": item.estimate_sha256,
            "usage_sha256": usage_digest,
        })
        if calibration is not None:
            # #201: a case outside a group's observed feature ranges reads
            # uncalibrated, and the row says why.
            memory_group = memory_groups.get(group_key(item))
            runtime_group = runtime_groups.get(runtime_group_key(item))
            memory_ood = out_of_range(memory_group, estimate.features) if memory_group else None
            runtime_ood = (out_of_range(runtime_group, estimate.features)
                           if runtime_group else None)
            rows[-1].update(_calibrated(
                item, None if memory_group is None or memory_ood else memory_group.multiplier,
                None if runtime_group is None or runtime_ood else runtime_group.multiplier))
            rows[-1].update({"calibration_out_of_range": memory_ood,
                             "runtime_calibration_out_of_range": runtime_ood})
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    routes: dict[tuple[str, str | None, str], list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault((row["method"], row["hardware_class"]), []).append(row)
        routes.setdefault((row["method"], row["algorithm"], row["hardware_class"]),
                          []).append(row)
    route_groups = []
    for (method, algorithm, hardware), members in sorted(routes.items(),
                                                         key=lambda i: json.dumps(i[0])):
        summary = _summarise(members)
        route_groups.append({
            "method": method, "algorithm": algorithm, "hardware_class": hardware, **summary,
            **({"acceptance": _acceptance(summary, thresholds[0])}
               if thresholds is not None else {})})
    return {
        "schema_name": REPORT_SCHEMA,
        "set_id": evaluation_set.set_id,
        "set_sha256": hashlib.sha256(raw).hexdigest(),
        "limitations": list(LIMITATIONS),
        "overall": _summarise(rows),
        "groups": [
            {"method": method, "hardware_class": hardware,
             "estimators": sorted({f"{r['estimator']} ({r['calibration']})"
                                   for r in members}),
             **_summarise(members)}
            for (method, hardware), members in sorted(groups.items())
        ],
        "route_groups": route_groups,
        "cases": rows,
        **({"thresholds": {"sha256": thresholds[1],
                           **thresholds[0].model_dump(mode="json", exclude={"schema_name"})}}
           if thresholds is not None else {}),
        **({"calibration": {
            "calibration_id": calibration.calibration_id, "method": calibration.method,
            "runtime_method": calibration.runtime_method,
            "min_cases": calibration.min_cases,
            "training_set_id": calibration.training_set_id,
            "training_set_sha256": calibration.training_set_sha256,
            "limitations": list(calibration.limitations),
        }} if calibration is not None else {}),
    }


Report = dict[str, Any]


def report_json(report: Report) -> str:
    return json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n"


def _share(bounds: dict[str, Any]) -> str:
    if bounds["n"] == 0:
        return "-"
    lower, upper = bounds["lower"], bounds["upper"]
    text = f"{lower:.0%}" if lower == upper else f"{lower:.0%}-{upper:.0%}"
    return f"{text} of {bounds['n']}"


def report_text(report: Report) -> str:
    lines = [f"evaluation set {report['set_id']} ({report['set_sha256'][:12]}): "
             f"{report['overall']['cases']} case(s)", ""]
    calibrated = "calibration" in report
    rows = [("METHOD", "HARDWARE", "CASES", "MEM UNDERPREDICTED", "OBS/PRED MAX",
             "RUNTIME COVERED", *(("CALIBRATED UNDER", "UNCALIBRATED", "CALIBRATED COVERED")
                                  if calibrated else ()))]
    for group in [*report["groups"], {**report["overall"], "method": "(all)",
                                      "hardware_class": ""}]:
        ratio = group["memory"]["observed_over_predicted"]
        extra: tuple[str, ...] = ()
        if calibrated:
            summary = group["memory"]["calibrated"]
            extra = (_share(summary["underprediction"]), str(summary["uncalibrated"]),
                     _share(group["runtime"]["calibrated"]["coverage"]))
        rows.append((group["method"], group["hardware_class"], str(group["cases"]),
                     _share(group["memory"]["underprediction"]),
                     "-" if ratio is None else f"{ratio['max']:.2f}",
                     _share(group["runtime"]["coverage"]), *extra))
    widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
    lines.extend("  ".join(r[i].ljust(widths[i]) for i in range(len(r))).rstrip()
                 for r in rows)
    excluded = [c["case_id"] for c in report["cases"] if c["memory"] == "excluded"]
    if excluded:
        lines += ["", f"Excluded (failed run or collector error): {', '.join(excluded)}"]
    lines += ["", "A range means some cases could not be decided; see limitations.", ""]
    lines += [f"- {text}" for text in report["limitations"]]
    if calibrated:
        meta = report["calibration"]
        lines += ["", f"Calibrated with {meta['calibration_id'][:12]}, fitted on training set "
                      f"{meta['training_set_id']} ({meta['training_set_sha256'][:12]}), "
                      f"method {meta['method']}, at least {meta['min_cases']} decided "
                      "case(s) per group:"]
        lines += [f"- {text}" for text in meta["limitations"]]
    if "thresholds" in report:
        meta = report["thresholds"]
        lines += ["", f"Acceptance by route, thresholds {meta['sha256'][:12]}: {meta['basis']} "
                      f"underprediction at most {meta['max_underprediction']:.2%} over at "
                      f"least {meta['min_cases']} case(s):"]
        lines += [f"- {g['method']} / {g['algorithm'] or '-'} on {g['hardware_class']}: "
                  f"{g['acceptance']['verdict'].upper()} ({g['acceptance']['reason']})"
                  for g in report["route_groups"]]
    return "\n".join(lines) + "\n"
