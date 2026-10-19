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
"""

from __future__ import annotations

import hashlib
import json
import math
import statistics
from collections import Counter
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from vq.resource_estimate import ResourceEstimate
from vq.resource_receipt import RESOURCE_USAGE_SCHEMA as DIRECT_USAGE_SCHEMA

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


def _summarise(rows: list[dict[str, Any]]) -> dict[str, object]:
    memory = Counter(r["memory"] for r in rows)
    runtime = Counter(r["runtime"] for r in rows)
    mem_total = sum(memory[k] for k in ("underpredicted", "within", "not_refuted",
                                        "censored", "unobserved"))
    run_total = sum(runtime[k] for k in ("covered", "above", "below", "censored",
                                         "unobserved"))
    ratios = sorted(r["observed_over_predicted"] for r in rows
                    if r["observed_over_predicted"] is not None)
    return {
        "cases": len(rows),
        "memory": {
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
            "verdicts": dict(sorted(runtime.items())),
            "coverage": _bounds(runtime["covered"], runtime["censored"]
                                + runtime["unobserved"], run_total),
        },
    }


def evaluate(set_file: Path) -> dict[str, object]:
    """The evaluation report for the set at ``set_file``."""
    raw = _read(set_file, MAX_SET_BYTES)
    try:
        evaluation_set = EvaluationSet.model_validate_json(raw)
    except ValueError as exc:
        raise EvaluationError(f"{set_file}: {exc}") from None
    base = set_file.parent
    rows: list[dict[str, Any]] = []
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
            "estimate_sha256": hashlib.sha256(estimate_bytes).hexdigest(),
            "usage_sha256": usage_digest,
        })
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault((row["method"], row["hardware_class"]), []).append(row)
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
        "cases": rows,
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
    rows = [("METHOD", "HARDWARE", "CASES", "MEM UNDERPREDICTED", "OBS/PRED MAX",
             "RUNTIME COVERED")]
    for group in [*report["groups"], {**report["overall"], "method": "(all)",
                                      "hardware_class": ""}]:
        ratio = group["memory"]["observed_over_predicted"]
        rows.append((group["method"], group["hardware_class"], str(group["cases"]),
                     _share(group["memory"]["underprediction"]),
                     "-" if ratio is None else f"{ratio['max']:.2f}",
                     _share(group["runtime"]["coverage"])))
    widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
    lines.extend("  ".join(r[i].ljust(widths[i]) for i in range(len(r))).rstrip()
                 for r in rows)
    excluded = [c["case_id"] for c in report["cases"] if c["memory"] == "excluded"]
    if excluded:
        lines += ["", f"Excluded (failed run or collector error): {', '.join(excluded)}"]
    lines += ["", "A range means some cases could not be decided; see limitations.", ""]
    lines += [f"- {text}" for text in report["limitations"]]
    return "\n".join(lines) + "\n"
