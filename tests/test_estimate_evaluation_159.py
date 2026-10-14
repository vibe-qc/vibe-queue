"""#159: held-out evaluation of resource estimates against measured usage.

Estimate receipts come from the real `vq estimate`; one usage receipt comes
from the real `vq.resource_receipt` collector around a process that
allocates memory. The rest are synthetic so each verdict is pinned exactly.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import estimate_evaluation as ev
from vq.cli import main
from vq.resource_estimate import ResourceEstimate, estimate_vibeqc
from vq.resource_receipt import RESOURCE_USAGE_SCHEMA

GIB = 1024**3


def _estimate(tmp_path: Path, name: str, *, peak: int | None, scope: str = "job",
              method: str = "hf", runtime: tuple[float, float] | None = None) -> str:
    """A receipt from the real estimator, written next to the set file."""
    work = tmp_path / f"est-{name}"
    work.mkdir()
    manifest = work / "calc.system"
    memory = f"estimate_bytes = {peak}" if peak is not None else ""
    manifest.write_text('[vibeqc]\nversion = "example"\n[plan]\n'
                        f'method = "{method}"\n[outputs]\nstatus = "dry_run"\n'
                        f'[memory]\n{memory}\n')
    features = work / "features.json"
    features.write_text(json.dumps({"memory_scope": scope}))
    receipt = estimate_vibeqc(manifest, features_file=features)
    if runtime is not None:
        receipt = receipt.model_copy(update={"runtime_seconds": runtime})
    (tmp_path / f"{name}.estimate.json").write_text(receipt.model_dump_json(indent=2))
    return f"{name}.estimate.json"


def _usage(tmp_path: Path, name: str, *, rss: int | None = None, wall: float = 100.0,
           processes: int | None = None, status: str = "ok",
           schema: str = RESOURCE_USAGE_SCHEMA) -> str:
    payload = {"schema": schema, "status": status, "collector": "posix-wait4",
               "scope": "effective-command", "command_status": "succeeded",
               "command_exit_code": 0, "wall_seconds": wall,
               "peak_rss_kb": None if rss is None else rss // 1024,
               "process_count": processes}
    (tmp_path / f"{name}.usage.json").write_text(json.dumps(payload))
    return f"{name}.usage.json"


def _write_set(tmp_path: Path, cases: list[dict]) -> Path:
    path = tmp_path / "set.json"
    path.write_text(json.dumps({"schema_name": "vq.estimate-evaluation-set/1",
                                "set_id": "held-out-1", "cases": cases}))
    return path


def _case(case_id: str, estimate: str, usage: str | None, outcome: str = "completed",
          hardware: str = "class-a", **extra: object) -> dict:
    return {"case_id": case_id, "estimate": estimate, "usage": usage, "outcome": outcome,
            "hardware_class": hardware, **extra}


def _verdicts(report: dict, field: str = "memory") -> dict[str, str]:
    return {c["case_id"]: c[field] for c in report["cases"]}


def test_real_estimate_and_real_collector_end_to_end(tmp_path: Path) -> None:
    runner = CliRunner()
    manifest = tmp_path / "calc.system"
    manifest.write_text('[vibeqc]\nversion = "example"\n[plan]\nmethod = "hf"\n'
                        '[outputs]\nstatus = "dry_run"\n[memory]\nestimate_bytes = 1048576\n')
    produced = runner.invoke(main, ["estimate", str(manifest), "--json"])
    assert produced.exit_code == 0, produced.output
    (tmp_path / "tiny.estimate.json").write_text(produced.output)
    big = ResourceEstimate.model_validate_json(produced.output).model_copy(
        update={"memory": ResourceEstimate.model_validate_json(produced.output).memory
                .model_copy(update={"peak_bytes": 64 * GIB, "scope": "job"})})
    (tmp_path / "big.estimate.json").write_text(big.model_dump_json())

    receipt = tmp_path / "usage.json"
    subprocess.run(
        [sys.executable, "-m", "vq.resource_receipt", "--receipt", str(receipt),
         "--exit-marker", str(tmp_path / "exit-code"), "--",
         sys.executable, "-c", "x = bytearray(40 * 1024 * 1024)"],
        check=True, timeout=60,
    )
    measured = json.loads(receipt.read_text())
    assert measured["status"] == "ok" and measured["peak_rss_kb"] > 40 * 1024

    set_file = _write_set(tmp_path, [
        _case("tiny", "tiny.estimate.json", "usage.json"),
        _case("big", "big.estimate.json", "usage.json"),
    ])
    result = runner.invoke(main, ["estimate-evaluate", str(set_file), "--json"])
    assert result.exit_code == 0, result.output
    verdicts = _verdicts(json.loads(result.output))
    # A 1 MiB estimate under a 40 MiB allocation is a decided underprediction.
    assert verdicts["tiny"] == "underpredicted"
    # Below a 64 GiB job estimate: confirmed only if the run was one process.
    assert verdicts["big"] == ("within" if measured.get("process_count") == 1
                               else "not_refuted")


@pytest.mark.parametrize(("peak", "scope", "usage", "outcome", "extra", "verdict"), [
    (GIB, "job", {"rss": 2 * GIB}, "completed", {}, "underpredicted"),
    (GIB, "job", {"rss": GIB // 2, "processes": 1}, "completed", {}, "within"),
    (GIB, "job", {"rss": GIB // 2, "processes": 4}, "completed", {}, "not_refuted"),
    (GIB, "unspecified", {"rss": GIB // 2}, "completed", {}, "not_refuted"),
    (GIB, "rank", {"rss": GIB // 2}, "completed", {}, "within"),
    (GIB, "job", None, "oom_killed", {"memory_limit_bytes": 2 * GIB}, "underpredicted"),
    (GIB, "job", None, "oom_killed", {"memory_limit_bytes": GIB // 2}, "censored"),
    (GIB, "job", None, "oom_killed", {}, "censored"),
    (GIB, "job", {"rss": GIB // 2}, "time_exceeded", {}, "censored"),
    (GIB, "job", {"rss": 2 * GIB}, "killed", {}, "underpredicted"),
    (GIB, "job", {"rss": 2 * GIB}, "failed", {}, "excluded"),
    (GIB, "job", {"rss": GIB // 2, "status": "error"}, "completed", {}, "excluded"),
    (None, "job", {"rss": GIB}, "completed", {}, "no_prediction"),
    (GIB, "job", None, "completed", {}, "unobserved"),
])
def test_memory_verdicts(tmp_path: Path, peak: int | None, scope: str, usage: dict | None,
                         outcome: str, extra: dict, verdict: str) -> None:
    estimate = _estimate(tmp_path, "e", peak=peak, scope=scope)
    usage_file = _usage(tmp_path, "u", **usage) if usage is not None else None
    report = ev.evaluate(_write_set(tmp_path, [_case("c", estimate, usage_file, outcome,
                                                     **extra)]))
    assert _verdicts(report)["c"] == verdict


@pytest.mark.parametrize(("wall", "outcome", "extra", "verdict"), [
    (150.0, "completed", {}, "covered"),
    (250.0, "completed", {}, "above"),
    (50.0, "completed", {}, "below"),
    (120.0, "time_exceeded", {"wall_limit_seconds": 300}, "above"),
    (120.0, "time_exceeded", {"wall_limit_seconds": 150}, "censored"),
    (250.0, "killed", {}, "above"),
    (150.0, "failed", {}, "excluded"),
])
def test_runtime_verdicts(tmp_path: Path, wall: float, outcome: str, extra: dict,
                          verdict: str) -> None:
    estimate = _estimate(tmp_path, "e", peak=GIB, runtime=(100.0, 200.0))
    usage = _usage(tmp_path, "u", rss=GIB // 2, wall=wall)
    report = ev.evaluate(_write_set(tmp_path, [_case("c", estimate, usage, outcome, **extra)]))
    assert _verdicts(report, "runtime")["c"] == verdict


def test_bounds_groups_and_sample_sizes(tmp_path: Path) -> None:
    hf = _estimate(tmp_path, "hf", peak=GIB)
    mp2 = _estimate(tmp_path, "mp2", peak=GIB, method="mp2")
    over = _usage(tmp_path, "over", rss=2 * GIB)
    under_single = _usage(tmp_path, "single", rss=GIB // 2, processes=1)
    under_multi = _usage(tmp_path, "multi", rss=GIB // 2, processes=3)
    report = ev.evaluate(_write_set(tmp_path, [
        _case("a1", hf, over), _case("a2", hf, under_single), _case("a3", hf, under_multi),
        _case("a4", hf, None, "oom_killed"), _case("a5", hf, over, "failed"),
        _case("b1", mp2, over, hardware="class-b"),
    ]))
    group = next(g for g in report["groups"]
                 if (g["method"], g["hardware_class"]) == ("hf", "class-a"))
    # Decided: a1 under, a2 within. Undecided: a3 not refuted, a4 censored.
    # a5 failed and is not a measurement.
    assert group["cases"] == 5
    assert group["memory"]["underprediction"] == {"lower": 0.25, "upper": 0.75, "n": 4}
    assert group["memory"]["verdicts"]["excluded"] == 1
    assert group["memory"]["observed_over_predicted"] == {"n": 3, "median": 0.5, "max": 2.0}
    assert group["runtime"]["coverage"]["n"] == 0
    assert [(g["method"], g["hardware_class"]) for g in report["groups"]] == [
        ("hf", "class-a"), ("mp2", "class-b")]
    assert report["overall"]["memory"]["underprediction"] == {
        "lower": 0.4, "upper": 0.8, "n": 5}
    assert group["estimators"] == ["vq.vibeqc-manifest/1 (uncalibrated)"]
    text = ev.report_text(report)
    assert "25%-75% of 4" in text and "Excluded (failed run or collector error): a5" in text


def test_the_report_binds_its_data_and_is_reproducible(tmp_path: Path) -> None:
    estimate = _estimate(tmp_path, "e", peak=GIB)
    usage = _usage(tmp_path, "u", rss=GIB // 2)
    set_file = _write_set(tmp_path, [_case("c", estimate, usage)])
    first = ev.report_json(ev.evaluate(set_file))
    assert first == ev.report_json(ev.evaluate(set_file))
    report = json.loads(first)
    assert report["set_sha256"] == hashlib.sha256(set_file.read_bytes()).hexdigest()
    assert report["cases"][0]["usage_sha256"] == hashlib.sha256(
        (tmp_path / usage).read_bytes()).hexdigest()
    _usage(tmp_path, "u", rss=GIB // 4)
    assert json.loads(ev.report_json(ev.evaluate(set_file)))["cases"][0]["usage_sha256"] != (
        report["cases"][0]["usage_sha256"])


@pytest.mark.parametrize(("mutate", "message"), [
    (lambda c: c.update(estimate="../outside.json"), "must be relative to the set file"),
    (lambda c: c.update(usage="/abs/usage.json"), "must be relative to the set file"),
    (lambda c: c.update(outcome="exploded"), "outcome"),
    (lambda c: c.update(surprise=1), "Extra inputs are not permitted"),
])
def test_malformed_sets_are_refused(tmp_path: Path, mutate, message: str) -> None:
    case = _case("c", _estimate(tmp_path, "e", peak=GIB), _usage(tmp_path, "u", rss=GIB))
    mutate(case)
    with pytest.raises(ev.EvaluationError, match=message):
        ev.evaluate(_write_set(tmp_path, [case]))


def test_receipts_must_be_what_they_claim(tmp_path: Path) -> None:
    estimate = _estimate(tmp_path, "e", peak=GIB)
    other = _usage(tmp_path, "u", rss=GIB, schema="vq.something-else/1")
    with pytest.raises(ev.EvaluationError, match="not a vq resource-usage receipt"):
        ev.evaluate(_write_set(tmp_path, [_case("c", estimate, other)]))
    (tmp_path / "bad.estimate.json").write_text('{"schema_name": "nope"}')
    with pytest.raises(ev.EvaluationError, match="estimate receipt"):
        ev.evaluate(_write_set(tmp_path, [_case("c", "bad.estimate.json", None)]))
    with pytest.raises(ev.EvaluationError, match="not a regular file"):
        ev.evaluate(_write_set(tmp_path, [_case("c", "missing.json", None)]))
    duplicate = _write_set(tmp_path, [_case("c", estimate, None), _case("c", estimate, None)])
    with pytest.raises(ev.EvaluationError, match="each case_id may appear once"):
        ev.evaluate(duplicate)


def test_the_scheduler_receipt_schema_matches_its_writer() -> None:
    from vq.scheduler_dispatch import RESOURCE_USAGE_SCHEMA as scheduler_schema

    assert scheduler_schema == ev.SCHEDULER_USAGE_SCHEMA
