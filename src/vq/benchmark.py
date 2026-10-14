"""Generic-versus-optimized runtime benchmarks: plans and their analysis (#162).

A plan fixes what is compared: one baseline runtime and its alternatives,
identified by digest, over named workloads with declared correctness
checks, on named compute-node classes with one MPI/thread layout. It also
fixes how: warm-up blocks, at least five measured blocks, and a seed for
the order of runs within each block.

The analysis pairs each alternative's run with the baseline run of the
same workload, node class and block. A pair counts only when both runs
completed with the planned runtime and layout, ran on the same CPU model
where that is recorded, and agree on every declared check. Everything
else is excluded with its reason, and a missing run is reported as
missing. Per workload and node class it reports ``T_baseline /
T_variant`` of the steady-state time: geometric mean, a Student-t
interval on the mean log ratio, the sample count, a verdict that may be
``inconclusive``, and the break-even run count against the variant's
declared build and deployment cost.

Nothing here submits, measures or reads anything but the two files. The
same plan and results always produce the same report, byte for byte.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import random
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

PLAN_SCHEMA = "vq.benchmark-plan/1"
RESULTS_SCHEMA = "vq.benchmark-results/1"
REPORT_SCHEMA = "vq.benchmark-report/1"
ANALYSIS = "vq.benchmark-paired-log-t/1"

#: The issue's floor: never decide a variant on fewer measured repeats.
MIN_REPEATS = 5
MAX_PLANNED_RUNS = 20_000
MAX_FILE_BYTES = 64 * 1024 * 1024

Name = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,63}$")]
Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Text = Annotated[str, Field(min_length=1, max_length=256)]
NonNegative = Annotated[float, Field(ge=0)]

RunStatus = Literal[
    "completed", "failed", "unconverged", "time_exceeded", "oom_killed", "killed",
]
Verdict = Literal["faster", "slower", "equivalent", "inconclusive", "incomparable"]


class BenchmarkError(ValueError):
    """The plan or results cannot be analysed as they stand."""


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)


class Variant(_Model):
    """One runtime build, identified by digest, with what it cost to make."""

    name: Name
    runtime_digest: Digest
    build_flags: Annotated[str, Field(max_length=1024)] | None = None
    #: Loaded numerical libraries and their versions or digests.
    libraries: dict[Name, Text] = Field(default_factory=dict, max_length=64)
    build_cost_core_seconds: NonNegative = 0.0
    deployment_cost_core_seconds: NonNegative = 0.0
    storage_bytes: Annotated[int, Field(ge=0)] | None = None


class Check(_Model):
    """A result quantity both runs of a pair must agree on."""

    quantity: Name
    abs_tolerance: NonNegative
    rel_tolerance: NonNegative = 0.0


class Workload(_Model):
    name: Name
    input_digest: Digest
    method: Text | None = None
    algorithm: Text | None = None
    nbf: Annotated[int, Field(gt=0)] | None = None
    naux: Annotated[int, Field(gt=0)] | None = None
    checks: list[Check] = Field(min_length=1, max_length=32)

    @model_validator(mode="after")
    def _unique_quantities(self) -> Workload:
        quantities = [check.quantity for check in self.checks]
        if len(set(quantities)) != len(quantities):
            raise ValueError(f"workload {self.name}: each check quantity may appear once")
        return self


class Layout(_Model):
    mpi_ranks: Annotated[int, Field(ge=1, le=100_000)]
    threads_per_rank: Annotated[int, Field(ge=1, le=4096)]

    @property
    def cores(self) -> int:
        return self.mpi_ranks * self.threads_per_rank

    def label(self) -> str:
        return f"{self.mpi_ranks}x{self.threads_per_rank}"


class Plan(_Model):
    schema_name: Literal["vq.benchmark-plan/1"]
    plan_id: Name
    baseline: Name
    variants: list[Variant] = Field(min_length=2, max_length=16)
    workloads: list[Workload] = Field(min_length=1, max_length=256)
    node_classes: list[Name] = Field(min_length=1, max_length=32)
    layout: Layout
    warmup: Annotated[int, Field(ge=0, le=10)] = 1
    repeats: Annotated[int, Field(ge=MIN_REPEATS, le=100)]
    seed: Annotated[int, Field(ge=0, le=2**63 - 1)]
    confidence: Annotated[float, Field(gt=0.5, lt=1.0)] = 0.95
    #: Practical-significance band around 1: a ratio within ``1 ± margin``
    #: is no difference worth a separate artifact.
    margin: Annotated[float, Field(ge=0.0, lt=1.0)] = 0.02

    @model_validator(mode="after")
    def _consistent(self) -> Plan:
        for kind, names in (
            ("variant", [v.name for v in self.variants]),
            ("workload", [w.name for w in self.workloads]),
            ("node class", list(self.node_classes)),
            ("runtime digest", [v.runtime_digest for v in self.variants]),
        ):
            if len(set(names)) != len(names):
                raise ValueError(f"each {kind} may appear once")
        if self.baseline not in {v.name for v in self.variants}:
            raise ValueError(f"baseline {self.baseline!r} is not one of the variants")
        planned = (len(self.node_classes) * len(self.workloads) * len(self.variants)
                   * (self.warmup + self.repeats))
        if planned > MAX_PLANNED_RUNS:
            raise ValueError(f"the plan has {planned} runs; at most {MAX_PLANNED_RUNS}")
        return self

    def variant(self, name: str) -> Variant:
        return next(v for v in self.variants if v.name == name)

    @property
    def blocks(self) -> int:
        return self.warmup + self.repeats


class Hardware(_Model):
    cpu_model: Text | None = None
    mpi_ranks: Annotated[int, Field(ge=1)]
    threads_per_rank: Annotated[int, Field(ge=1)]


class Run(_Model):
    """One measured execution. Warm-up is derived from ``block``, not declared."""

    workload: Name
    variant: Name
    node_class: Name
    block: Annotated[int, Field(ge=0)]
    status: RunStatus
    runtime_digest: Digest
    hardware: Hardware
    #: Execution after startup; the time the comparison is made on.
    steady_seconds: Annotated[float, Field(gt=0)] | None = None
    startup_seconds: NonNegative | None = None
    cpu_seconds: NonNegative | None = None
    peak_rss_mb: NonNegative | None = None
    scf_iterations: Annotated[int, Field(ge=0)] | None = None
    values: dict[Name, float] = Field(default_factory=dict, max_length=32)

    @property
    def key(self) -> tuple[str, str, str, int]:
        return (self.workload, self.variant, self.node_class, self.block)


class Results(_Model):
    schema_name: Literal["vq.benchmark-results/1"]
    plan_sha256: Digest
    runs: list[Run] = Field(max_length=MAX_PLANNED_RUNS)


def _read_json(path: Path) -> object:
    try:
        with path.open("rb") as handle:
            data = handle.read(MAX_FILE_BYTES + 1)
    except OSError as exc:
        raise BenchmarkError(f"{path}: {exc.strerror or exc}") from None
    if len(data) > MAX_FILE_BYTES:
        raise BenchmarkError(f"{path}: larger than {MAX_FILE_BYTES} bytes")
    try:
        return json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BenchmarkError(f"{path}: not UTF-8 JSON: {exc}") from None


def load_plan(path: Path) -> Plan:
    return Plan.model_validate(_read_json(path))


def load_results(path: Path) -> Results:
    return Results.model_validate(_read_json(path))


def _canonical_digest(payload: object) -> str:
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def plan_digest(plan: Plan) -> str:
    """What a results file records as ``plan_sha256``; formatting and key order do not matter."""
    return _canonical_digest(plan.model_dump(mode="json"))


def results_digest(results: Results) -> str:
    """Independent of the order runs were written in."""
    runs = sorted((run.model_dump(mode="json") for run in results.runs),
                  key=lambda run: json.dumps(run, sort_keys=True))
    return _canonical_digest({**results.model_dump(mode="json"), "runs": runs})


def run_order(plan: Plan) -> list[dict[str, object]]:
    """Every planned run, in the order to execute them.

    Per node class, block by block; within a block every workload/variant
    run once, shuffled by the plan's seed so no variant always runs first
    or last. The same plan always yields the same order.
    """
    rng = random.Random(plan.seed)
    order: list[dict[str, object]] = []
    for node_class in plan.node_classes:
        for block in range(plan.blocks):
            cell = [(w, v) for w in plan.workloads for v in plan.variants]
            rng.shuffle(cell)
            for workload, variant in cell:
                order.append({
                    "sequence": len(order),
                    "node_class": node_class,
                    "block": block,
                    "warmup": block < plan.warmup,
                    "workload": workload.name,
                    "variant": variant.name,
                    "input_digest": workload.input_digest,
                    "runtime_digest": variant.runtime_digest,
                })
    return order


# --- Student t quantile -----------------------------------------------------
# The regularized incomplete beta function by Lentz's continued fraction,
# then the t distribution's CDF from it and its inverse by bisection. Exact
# to well below the precision the report prints, with no SciPy dependency.

def _beta_continued_fraction(a: float, b: float, x: float) -> float:
    tiny = 1e-300
    c, d = 1.0, 1.0 - (a + b) * x / (a + 1.0)
    d = 1.0 / (d if abs(d) > tiny else tiny)
    result = d
    for m in range(1, 500):
        m2 = 2 * m
        for numerator in (
            m * (b - m) * x / ((a + m2 - 1.0) * (a + m2)),
            -(a + m) * (a + b + m) * x / ((a + m2) * (a + m2 + 1.0)),
        ):
            d = 1.0 + numerator * d
            d = 1.0 / (d if abs(d) > tiny else tiny)
            c = 1.0 + numerator / c
            c = c if abs(c) > tiny else tiny
            result *= d * c
        if abs(d * c - 1.0) < 1e-15:
            break
    return result


def _regularized_beta(a: float, b: float, x: float) -> float:
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    front = math.exp(
        math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b)
        + a * math.log(x) + b * math.log1p(-x)
    )
    if x < (a + 1.0) / (a + b + 2.0):
        return front * _beta_continued_fraction(a, b, x) / a
    return 1.0 - front * _beta_continued_fraction(b, a, 1.0 - x) / b


def _t_cdf(t: float, df: int) -> float:
    tail = 0.5 * _regularized_beta(df / 2.0, 0.5, df / (df + t * t))
    return 1.0 - tail if t > 0 else tail


def t_quantile(p: float, df: int) -> float:
    """The ``p`` quantile of Student's t with ``df`` degrees of freedom."""
    if not 0.5 <= p < 1.0 or df < 1:
        raise ValueError("t_quantile needs 0.5 <= p < 1 and df >= 1")
    low, high = 0.0, 1.0
    while _t_cdf(high, df) < p:
        high *= 2.0
    for _ in range(200):
        mid = (low + high) / 2.0
        if _t_cdf(mid, df) < p:
            low = mid
        else:
            high = mid
    return (low + high) / 2.0


# --- Analysis ---------------------------------------------------------------

@dataclass
class _Pair:
    block: int
    baseline: Run
    variant: Run
    #: The two runs' steady_seconds, present once the pair is retained.
    baseline_seconds: float
    variant_seconds: float


def _round(value: float | None, digits: int = 6) -> float | None:
    return None if value is None else round(value, digits)


def _run_problem(plan: Plan, run: Run, variant: Variant) -> str | None:
    if run.status != "completed":
        return f"{variant.name} run {run.status}"
    if run.runtime_digest != variant.runtime_digest:
        return (f"{variant.name} ran runtime {run.runtime_digest[:12]}, "
                f"not the planned {variant.runtime_digest[:12]}")
    ranks, threads = run.hardware.mpi_ranks, run.hardware.threads_per_rank
    if (ranks, threads) != (plan.layout.mpi_ranks, plan.layout.threads_per_rank):
        return (f"{variant.name} ran a {ranks}x{threads} layout, "
                f"not the planned {plan.layout.label()}")
    if run.steady_seconds is None:
        return f"{variant.name} recorded no steady_seconds"
    return None


def _pair_problem(workload: Workload, baseline: Run, variant: Run) -> str | None:
    models = (baseline.hardware.cpu_model, variant.hardware.cpu_model)
    if None not in models and models[0] != models[1]:
        return f"the pair ran on different CPU models: {models[0]} and {models[1]}"
    for check in workload.checks:
        reference = baseline.values.get(check.quantity)
        value = variant.values.get(check.quantity)
        for name, recorded in ((baseline.variant, reference), (variant.variant, value)):
            if recorded is None:
                return f"{check.quantity} missing from the {name} run"
        assert reference is not None and value is not None
        difference = abs(value - reference)
        tolerance = check.abs_tolerance + check.rel_tolerance * abs(reference)
        if difference > tolerance:
            return (f"numerical mismatch on {check.quantity}: "
                    f"|difference| {difference:.3g} > tolerance {tolerance:.3g}")
    return None


def _summary(values: list[float | int | None]) -> dict[str, object] | None:
    present = [float(v) for v in values if v is not None]
    if not present:
        return None
    return {
        "n": len(present),
        "mean": _round(statistics.fmean(present)),
        "sd": _round(statistics.stdev(present)) if len(present) > 1 else None,
    }


def _break_even(
    plan: Plan, variant: Variant, pairs: list[_Pair], verdict: str,
    geomean: float | None, ci_low: float | None,
) -> dict[str, object]:
    """Runs needed before the variant's saving repays its cost.

    Cost and saving are both allocated core-seconds: the saving per run is
    the steady-time difference times the planned layout's cores.
    """
    cost = variant.build_cost_core_seconds + variant.deployment_cost_core_seconds
    result: dict[str, object] = {
        "cost_core_seconds": _round(cost),
        "storage_bytes": variant.storage_bytes,
        "saving_core_seconds_per_run": None,
        "runs": None,
        "conservative_saving_core_seconds_per_run": None,
        "conservative_runs": None,
        "reason": None,
    }
    if verdict != "faster" or geomean is None or ci_low is None:
        result["reason"] = "no demonstrated saving"
        return result
    cores = plan.layout.cores
    saving = statistics.fmean(p.baseline_seconds - p.variant_seconds for p in pairs) * cores
    mean_baseline = statistics.fmean(p.baseline_seconds for p in pairs)
    conservative = mean_baseline * (1.0 - 1.0 / ci_low) * cores
    result["saving_core_seconds_per_run"] = _round(saving)
    result["conservative_saving_core_seconds_per_run"] = _round(conservative)
    if saving <= 0:
        result["reason"] = "the mean paired saving is not positive"
        return result
    result["runs"] = math.ceil(cost / saving) if cost > 0 else 0
    result["conservative_runs"] = math.ceil(cost / conservative) if cost > 0 else 0
    return result


def _analyze_cell(
    plan: Plan, workload: Workload, variant: Variant, node_class: str,
    runs: dict[tuple[str, str, str, int], Run],
) -> tuple[dict[str, object], float | None]:
    baseline = plan.variant(plan.baseline)
    excluded: list[dict[str, object]] = []
    missing: list[dict[str, object]] = []
    warmup_runs = 0
    pairs: list[_Pair] = []
    for block in range(plan.blocks):
        base_run = runs.get((workload.name, baseline.name, node_class, block))
        var_run = runs.get((workload.name, variant.name, node_class, block))
        if block < plan.warmup:
            warmup_runs += (base_run is not None) + (var_run is not None)
            continue
        absent = [name for name, run in ((baseline.name, base_run), (variant.name, var_run))
                  if run is None]
        if absent:
            missing.append({"block": block, "variants": absent})
            continue
        assert base_run is not None and var_run is not None
        problem = (_run_problem(plan, base_run, baseline)
                   or _run_problem(plan, var_run, variant)
                   or _pair_problem(workload, base_run, var_run))
        if problem:
            excluded.append({"block": block, "reason": problem})
            continue
        assert base_run.steady_seconds is not None and var_run.steady_seconds is not None
        pairs.append(_Pair(block, base_run, var_run,
                           base_run.steady_seconds, var_run.steady_seconds))

    reasons: list[str] = []
    models = sorted({m for p in pairs
                     for m in (p.baseline.hardware.cpu_model, p.variant.hardware.cpu_model)
                     if m is not None})
    ratios = [p.baseline_seconds / p.variant_seconds for p in pairs]
    logs = [math.log(r) for r in ratios]
    n = len(pairs)
    geomean = math.exp(statistics.fmean(logs)) if logs else None
    ci_low = ci_high = None
    if n >= 2:
        half = (t_quantile(0.5 + plan.confidence / 2.0, n - 1)
                * statistics.stdev(logs) / math.sqrt(n))
        mean = statistics.fmean(logs)
        ci_low, ci_high = math.exp(mean - half), math.exp(mean + half)

    upper, lower = 1.0 + plan.margin, 1.0 / (1.0 + plan.margin)
    verdict: Verdict
    if len(models) > 1:
        verdict = "incomparable"
        reasons.append(f"node class {node_class} ran on several CPU models: {', '.join(models)}")
    elif n < plan.repeats or ci_low is None or ci_high is None:
        verdict = "inconclusive"
        reasons.append(f"{n} of {plan.repeats} planned pairs retained")
    elif ci_low > upper:
        verdict = "faster"
    elif ci_high < lower:
        verdict = "slower"
    elif ci_low >= lower and ci_high <= upper:
        verdict = "equivalent"
    else:
        verdict = "inconclusive"
        reasons.append(f"the interval crosses the ±{plan.margin:g} margin")

    def both(field: str) -> dict[str, object]:
        return {
            "baseline": _summary([getattr(p.baseline, field) for p in pairs]),
            "variant": _summary([getattr(p.variant, field) for p in pairs]),
        }

    cell: dict[str, object] = {
        "node_class": node_class,
        "workload": workload.name,
        "variant": variant.name,
        "pairs": n,
        "planned_pairs": plan.repeats,
        "verdict": verdict,
        "reasons": reasons,
        "ratio": None if geomean is None else {
            "geomean": _round(geomean),
            "ci_low": _round(ci_low),
            "ci_high": _round(ci_high),
            "median": _round(statistics.median(ratios)),
            "min": _round(min(ratios)),
            "max": _round(max(ratios)),
        },
        "steady_seconds": both("steady_seconds"),
        "startup_seconds": both("startup_seconds"),
        "cpu_seconds": both("cpu_seconds"),
        "peak_rss_mb": both("peak_rss_mb"),
        "scf_iterations": both("scf_iterations"),
        "cpu_models": models,
        "warmup_runs": warmup_runs,
        "excluded": excluded,
        "missing": missing,
        "break_even": _break_even(plan, variant, pairs, verdict, geomean, ci_low),
    }
    comparable = verdict != "incomparable" and n >= plan.repeats
    return cell, (geomean if comparable else None)


def analyze(plan: Plan, results: Results) -> dict[str, object]:
    """The report for ``results`` measured under ``plan``."""
    expected = plan_digest(plan)
    if results.plan_sha256 != expected:
        raise BenchmarkError(
            f"the results were recorded against plan {results.plan_sha256[:12]}, "
            f"not this plan ({expected[:12]}); a changed plan needs its own runs"
        )
    workloads = {w.name for w in plan.workloads}
    variants = {v.name for v in plan.variants}
    runs: dict[tuple[str, str, str, int], Run] = {}
    for run in results.runs:
        for kind, value, known in (("workload", run.workload, workloads),
                                   ("variant", run.variant, variants),
                                   ("node class", run.node_class, set(plan.node_classes))):
            if value not in known:
                raise BenchmarkError(f"a run names {kind} {value!r}, which the plan does not")
        if run.block >= plan.blocks:
            raise BenchmarkError(f"a run names block {run.block}; the plan has {plan.blocks}")
        if run.key in runs:
            raise BenchmarkError(
                "two runs recorded for workload {}, variant {}, node class {}, block {}"
                .format(*run.key)
            )
        runs[run.key] = run

    cells: list[dict[str, object]] = []
    aggregates: list[dict[str, object]] = []
    for node_class in plan.node_classes:
        for variant in plan.variants:
            if variant.name == plan.baseline:
                continue
            included: list[str] = []
            left_out: list[dict[str, object]] = []
            logs: list[float] = []
            for workload in plan.workloads:
                cell, geomean = _analyze_cell(plan, workload, variant, node_class, runs)
                cells.append(cell)
                if geomean is None:
                    left_out.append({"workload": workload.name, "verdict": cell["verdict"]})
                else:
                    included.append(workload.name)
                    logs.append(math.log(geomean))
            aggregates.append({
                "node_class": node_class,
                "variant": variant.name,
                "weighting": "equal per workload",
                "workloads": included,
                "left_out": left_out,
                "geomean_ratio": _round(math.exp(statistics.fmean(logs))) if logs else None,
            })

    return {
        "schema_name": REPORT_SCHEMA,
        "analysis": ANALYSIS,
        "plan_id": plan.plan_id,
        "plan_sha256": expected,
        "results_sha256": results_digest(results),
        "baseline": plan.baseline,
        "settings": {
            "ratio": ("T_baseline / T_variant of steady_seconds, paired by block; "
                      "above 1 means the variant is faster"),
            "interval": "Student t on the mean log ratio",
            "confidence": plan.confidence,
            "margin": plan.margin,
            "warmup": plan.warmup,
            "repeats": plan.repeats,
            "cost_unit": "allocated core-seconds",
        },
        "applies_to": {
            "layout": plan.layout.model_dump(mode="json"),
            "node_classes": list(plan.node_classes),
            "variants": [v.model_dump(mode="json") for v in plan.variants],
            "workloads": [
                w.model_dump(mode="json", exclude={"checks"}) for w in plan.workloads
            ],
        },
        "cells": cells,
        "aggregates": aggregates,
    }


#: A report as :func:`analyze` returns it; the renderers below read it.
Report = dict[str, Any]


def report_json(report: Report) -> str:
    return json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n"


CSV_COLUMNS = (
    "node_class", "workload", "variant", "pairs", "planned_pairs", "verdict",
    "ratio_geomean", "ratio_ci_low", "ratio_ci_high", "baseline_steady_mean",
    "variant_steady_mean", "excluded", "missing", "break_even_runs",
    "conservative_break_even_runs", "reasons",
)


def report_csv(report: Report) -> str:
    """One row per node class, workload and variant."""
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(CSV_COLUMNS)
    for cell in report["cells"]:
        ratio = cell["ratio"] or {}
        steady = cell["steady_seconds"]
        reasons = list(cell["reasons"]) + [e["reason"] for e in cell["excluded"]]
        writer.writerow([
            cell["node_class"], cell["workload"], cell["variant"], cell["pairs"],
            cell["planned_pairs"], cell["verdict"], ratio.get("geomean"),
            ratio.get("ci_low"), ratio.get("ci_high"),
            (steady["baseline"] or {}).get("mean"), (steady["variant"] or {}).get("mean"),
            len(cell["excluded"]), len(cell["missing"]),
            cell["break_even"]["runs"], cell["break_even"]["conservative_runs"],
            "; ".join(reasons),
        ])
    return out.getvalue()


def _fmt(value: object) -> str:
    return "-" if value is None else f"{value:.3f}" if isinstance(value, float) else str(value)


def report_text(report: Report) -> str:
    settings = report["settings"]
    lines = [
        f"benchmark {report['plan_id']}: baseline {report['baseline']}; "
        f"{settings['repeats']} measured pairs per cell after {settings['warmup']} warm-up; "
        f"{settings['confidence']:.0%} interval; margin ±{settings['margin']:g}",
        "ratio = T_baseline / T_variant (above 1: the variant is faster)",
        "",
    ]
    header = ("NODE CLASS", "WORKLOAD", "VARIANT", "PAIRS", "RATIO", "INTERVAL",
              "VERDICT", "BREAK-EVEN RUNS")
    rows = []
    notes: list[str] = []
    for cell in report["cells"]:
        ratio = cell["ratio"] or {}
        interval = (f"{_fmt(ratio.get('ci_low'))}-{_fmt(ratio.get('ci_high'))}"
                    if ratio.get("ci_low") is not None else "-")
        be = cell["break_even"]
        rows.append((cell["node_class"], cell["workload"], cell["variant"],
                     f"{cell['pairs']}/{cell['planned_pairs']}", _fmt(ratio.get("geomean")),
                     interval, cell["verdict"],
                     "-" if be["runs"] is None else f"{be['runs']} ({be['conservative_runs']})"))
        label = f"{cell['node_class']}/{cell['workload']}/{cell['variant']}"
        notes.extend(f"  {label}: {reason}" for reason in cell["reasons"])
        notes.extend(f"  {label} block {e['block']}: excluded, {e['reason']}"
                     for e in cell["excluded"])
        notes.extend(f"  {label} block {m['block']}: missing {', '.join(m['variants'])}"
                     for m in cell["missing"])
    widths = [max(len(str(r[i])) for r in [header, *rows]) for i in range(len(header))]
    lines.extend("  ".join(str(r[i]).ljust(widths[i]) for i in range(len(header))).rstrip()
                 for r in [header, *rows])
    if notes:
        lines += ["", "Notes:", *notes]
    lines.append("")
    for agg in report["aggregates"]:
        geo = _fmt(agg["geomean_ratio"])
        lines.append(
            f"aggregate {agg['node_class']}/{agg['variant']}: geometric mean {geo} over "
            f"{len(agg['workloads'])} workload(s), equal weight"
            + (f"; left out: {', '.join(x['workload'] for x in agg['left_out'])}"
               if agg["left_out"] else "")
        )
    lines.append("Break-even in parentheses uses the interval's conservative bound.")
    return "\n".join(lines) + "\n"
