"""#201: held-out evaluation by route, judged against thresholds fixed beforehand.

A report's ``route_groups`` key cases by method, algorithm and hardware
class, so two routes of one method are judged apart. A
``vq.estimate-thresholds/1`` document gives each route group a verdict from
its underprediction bounds: pass, fail, undecided across the limit, or
insufficient cases. Estimate receipts come from the real adapter.
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vq import estimate_evaluation as ev
from vq.cli import main
from vq.resource_estimate import estimate_vibeqc

sys.path.insert(0, str(Path(__file__).parent))
from test_estimate_calibration_159 import _set  # noqa: E402
from test_estimate_evaluation_159 import _case, _usage  # noqa: E402

GIB = 1024**3


def _routed(directory: Path, name: str, *, algorithm: str, observed: int,
            scope: str = "rank", processes: int = 4) -> dict[str, Any]:
    """An hf case on class-a: within when ``observed`` is at most 2 GiB."""
    work = directory / f"est-{name}"
    work.mkdir(parents=True)
    manifest = work / "calc.system"
    manifest.write_text('[vibeqc]\nversion = "example"\n[plan]\nmethod = "hf"\n'
                        f'[outputs]\nstatus = "dry_run"\n[memory]\nestimate_bytes = {2 * GIB}\n')
    (work / "features.json").write_text(json.dumps({"memory_scope": scope,
                                                    "algorithm": algorithm}))
    receipt = estimate_vibeqc(manifest, features_file=work / "features.json")
    (directory / f"{name}.estimate.json").write_text(receipt.model_dump_json())
    return _case(name, f"{name}.estimate.json",
                 _usage(directory, name, rss=observed, processes=processes))


def _thresholds(directory: Path, **values: object) -> Path:
    path = directory / "thresholds.json"
    path.write_text(json.dumps({"schema_name": "vq.estimate-thresholds/1",
                                "max_underprediction": 0.25, "min_cases": 2, **values}))
    return path


def _routes(report: dict[str, Any]) -> dict[str | None, dict[str, Any]]:
    return {g["algorithm"]: g for g in report["route_groups"]}


@pytest.fixture
def held_out(tmp_path: Path) -> Path:
    d = tmp_path / "held"
    d.mkdir()
    cases = [
        # direct: both within. df: one of two underpredicted.
        _routed(d, "direct-1", algorithm="direct", observed=GIB),
        _routed(d, "direct-2", algorithm="direct", observed=GIB),
        _routed(d, "df-1", algorithm="df", observed=GIB),
        _routed(d, "df-2", algorithm="df", observed=3 * GIB),
        # dense: one undecided (job scope, many processes, below the estimate).
        _routed(d, "dense-1", algorithm="dense", observed=GIB),
        _routed(d, "dense-2", algorithm="dense", observed=GIB, scope="job"),
        # ri: a single case.
        _routed(d, "ri-1", algorithm="ri", observed=GIB),
    ]
    return _set(d, "held", cases)


def test_routes_of_one_method_are_judged_apart(held_out: Path) -> None:
    report = ev.evaluate(held_out)
    assert [(g["method"], g["hardware_class"], g["cases"]) for g in report["groups"]] == [
        ("hf", "class-a", 7)]
    routes = _routes(report)
    assert sorted(routes) == ["dense", "df", "direct", "ri"]
    assert routes["df"]["memory"]["underprediction"] == {"lower": 0.5, "upper": 0.5, "n": 2}
    assert routes["direct"]["memory"]["underprediction"] == {"lower": 0.0, "upper": 0.0,
                                                             "n": 2}
    assert "acceptance" not in routes["df"] and "thresholds" not in report


def test_thresholds_give_each_route_a_verdict(held_out: Path, tmp_path: Path) -> None:
    path = _thresholds(tmp_path)
    report = ev.evaluate(held_out, thresholds=ev.load_thresholds(path))
    verdicts = {name: g["acceptance"]["verdict"] for name, g in _routes(report).items()}
    assert verdicts == {"direct": "pass", "df": "fail", "dense": "undecided",
                        "ri": "insufficient"}
    assert _routes(report)["ri"]["acceptance"]["reason"] == (
        "1 case(s), fewer than the thresholds' 2")
    assert report["thresholds"] == {
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "description": None,
        "max_underprediction": 0.25, "min_cases": 2, "basis": "raw"}


def test_calibrated_thresholds_need_a_calibration(held_out: Path, tmp_path: Path) -> None:
    thresholds = ev.load_thresholds(_thresholds(tmp_path, basis="calibrated"))
    with pytest.raises(ev.EvaluationError, match="need --calibration"):
        ev.evaluate(held_out, thresholds=thresholds)


@pytest.mark.parametrize("change", [{"max_underprediction": 1.5}, {"min_cases": 0},
                                    {"basis": "other"}, {"extra": 1}])
def test_a_malformed_thresholds_document_is_refused(tmp_path: Path,
                                                    change: dict[str, Any]) -> None:
    with pytest.raises(ev.EvaluationError, match="vq.estimate-thresholds/1"):
        ev.load_thresholds(_thresholds(tmp_path, **change))


def test_the_cli_prints_the_route_verdicts(held_out: Path, tmp_path: Path) -> None:
    result = CliRunner().invoke(main, ["estimate-evaluate", str(held_out), "--thresholds",
                                       str(_thresholds(tmp_path))])
    assert result.exit_code == 0, result.output
    assert "- hf / df on class-a: FAIL (underprediction is at least 50.00%)" in result.output
    assert "- hf / direct on class-a: PASS (underprediction is at most 0.00%)" in result.output
