"""#162: benchmark plans, run order and the paired analysis.

Every timing here is synthetic with a known ratio, so each reported number
can be checked against an independent calculation.
"""
from __future__ import annotations

import copy
import json
import math
import random
import statistics
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from click.testing import CliRunner
from pydantic import ValidationError

from vq import benchmark as bm
from vq.cli import main

#: Student t, 97.5 % quantile, 4 degrees of freedom (published tables).
T_975_DF4 = 2.776445105


def _plan(**overrides: object) -> dict:
    plan: dict = {
        "schema_name": "vq.benchmark-plan/1",
        "plan_id": "isa-pilot",
        "baseline": "generic",
        "variants": [
            {"name": "generic", "runtime_digest": "a" * 64,
             "build_flags": "-O2 -march=x86-64-v2"},
            {"name": "tuned", "runtime_digest": "b" * 64,
             "build_flags": "-O2 -march=sapphirerapids",
             "libraries": {"openblas": "0.3.28"},
             "build_cost_core_seconds": 36000, "deployment_cost_core_seconds": 1800,
             "storage_bytes": 400_000_000},
        ],
        "workloads": [
            {"name": "h2o-ccsd", "input_digest": "c" * 64, "method": "CCSD",
             "algorithm": "conventional", "nbf": 58,
             "checks": [{"quantity": "total_energy", "abs_tolerance": 1e-8}]},
        ],
        "node_classes": ["class-a"],
        "layout": {"mpi_ranks": 1, "threads_per_rank": 8},
        "warmup": 1,
        "repeats": 5,
        "seed": 7,
    }
    plan.update(overrides)
    return plan


BASE = [130.0, 100.0, 102.0, 98.0, 101.0, 99.0]  # block 0 is the warm-up


def _results(plan: dict, ratios: dict[str, list[float]] | None = None) -> dict:
    """Runs for every planned key; the variant's time is BASE / ratio."""
    parsed = bm.Plan.model_validate(plan)
    ratios = ratios or {}
    runs = []
    for node_class in parsed.node_classes:
        for workload in parsed.workloads:
            for variant in parsed.variants:
                wanted = ratios.get(workload.name, [1.25] * parsed.blocks)
                for block in range(parsed.blocks):
                    base = BASE[block % len(BASE)]
                    seconds = base if variant.name == parsed.baseline else base / wanted[block]
                    runs.append({
                        "workload": workload.name, "variant": variant.name,
                        "node_class": node_class, "block": block, "status": "completed",
                        "runtime_digest": variant.runtime_digest,
                        "hardware": {"cpu_model": "Model X", "mpi_ranks": 1,
                                     "threads_per_rank": 8},
                        "steady_seconds": seconds, "startup_seconds": 2.0,
                        "cpu_seconds": seconds * 8, "peak_rss_mb": 1000.0,
                        "scf_iterations": 12, "values": {"total_energy": -76.0},
                    })
    return {"schema_name": "vq.benchmark-results/1",
            "plan_sha256": bm.plan_digest(parsed), "runs": runs}


def _run(results: dict, variant: str, block: int, workload: str = "h2o-ccsd") -> dict:
    return next(r for r in results["runs"]
                if (r["variant"], r["block"], r["workload"]) == (variant, block, workload))


def _analyze(plan: dict, results: dict) -> dict:
    return bm.analyze(bm.Plan.model_validate(plan), bm.Results.model_validate(results))


def _cell(report: dict, workload: str = "h2o-ccsd") -> dict:
    return next(c for c in report["cells"] if c["workload"] == workload)


class TestPlan:
    def test_run_order_is_seeded_complete_and_interleaved(self) -> None:
        plan = bm.Plan.model_validate(_plan(workloads=[
            *_plan()["workloads"],
            {"name": "benzene-dfhf", "input_digest": "d" * 64,
             "checks": [{"quantity": "total_energy", "abs_tolerance": 1e-8}]},
        ]))
        order = bm.run_order(plan)
        assert order == bm.run_order(plan)
        assert len(order) == 1 * 2 * 2 * 6
        keys = {(e["workload"], e["variant"], e["block"]) for e in order}
        assert len(keys) == len(order)
        # Blocks run in sequence; each block holds every workload/variant once.
        assert [e["block"] for e in order] == sorted(e["block"] for e in order)
        assert [e["warmup"] for e in order] == [e["block"] == 0 for e in order]
        within = [tuple((e["workload"], e["variant"]) for e in order if e["block"] == b)
                  for b in range(6)]
        assert len(set(within)) > 1, "the order within blocks is shuffled"
        other_seed = bm.Plan.model_validate({**plan.model_dump(mode="json"), "seed": 8})
        assert bm.run_order(other_seed) != order

    @pytest.mark.parametrize(("change", "message"), [
        ({"repeats": 4}, "greater than or equal to 5"),
        ({"baseline": "missing"}, "is not one of the variants"),
        ({"node_classes": ["a", "a"]}, "each node class may appear once"),
        ({"variants": [_plan()["variants"][0],
                       {**_plan()["variants"][1], "runtime_digest": "a" * 64}]},
         "each runtime digest may appear once"),
        ({"surprise": 1}, "Extra inputs are not permitted"),
        ({"node_classes": ["login-1.example.invalid/x"]}, "String should match pattern"),
    ])
    def test_invalid_plans_are_refused(self, change: dict, message: str) -> None:
        with pytest.raises(ValidationError, match=message):
            bm.Plan.model_validate(_plan(**change))

    def test_a_workload_needs_a_correctness_check(self) -> None:
        workload = {**_plan()["workloads"][0], "checks": []}
        with pytest.raises(ValidationError, match="at least 1 item"):
            bm.Plan.model_validate(_plan(workloads=[workload]))

    def test_the_digest_ignores_layout_but_not_content(self) -> None:
        plan = bm.Plan.model_validate(_plan())
        reordered = bm.Plan.model_validate(json.loads(json.dumps(_plan(), sort_keys=True)))
        assert bm.plan_digest(plan) == bm.plan_digest(reordered)
        assert bm.plan_digest(plan) != bm.plan_digest(bm.Plan.model_validate(_plan(seed=8)))


class TestStudentT:
    @pytest.mark.parametrize(("p", "df", "expected"), [
        (0.975, 1, 12.706204736), (0.975, 4, T_975_DF4), (0.995, 10, 3.169272673),
        (0.95, 30, 1.697260887), (0.975, 120, 1.979930405),
    ])
    def test_matches_published_quantiles(self, p: float, df: int, expected: float) -> None:
        assert bm.t_quantile(p, df) == pytest.approx(expected, abs=1e-8)


class TestAnalysis:
    def test_known_speedup_interval_and_break_even(self) -> None:
        ratios = [1.0, 1.25, 1.24, 1.26, 1.25, 1.25]
        plan = _plan()
        cell = _cell(_analyze(plan, _results(plan, {"h2o-ccsd": ratios})))

        logs = [math.log(r) for r in ratios[1:]]
        mean, half = statistics.fmean(logs), T_975_DF4 * statistics.stdev(logs) / math.sqrt(5)
        assert cell["pairs"] == 5 and cell["verdict"] == "faster"
        assert cell["ratio"]["geomean"] == pytest.approx(math.exp(mean), abs=1e-6)
        assert cell["ratio"]["ci_low"] == pytest.approx(math.exp(mean - half), abs=1e-6)
        assert cell["ratio"]["ci_high"] == pytest.approx(math.exp(mean + half), abs=1e-6)
        assert cell["warmup_runs"] == 2 and cell["excluded"] == [] and cell["missing"] == []

        base, variant = BASE[1:], [b / r for b, r in zip(BASE[1:], ratios[1:], strict=True)]
        saving = statistics.fmean(b - v for b, v in zip(base, variant, strict=True)) * 8
        conservative = statistics.fmean(base) * (1 - math.exp(-(mean - half))) * 8
        be = cell["break_even"]
        assert be["cost_core_seconds"] == 37800
        assert be["saving_core_seconds_per_run"] == pytest.approx(saving, abs=1e-5)
        assert be["runs"] == math.ceil(37800 / saving)
        assert be["conservative_runs"] == math.ceil(37800 / conservative)
        assert be["conservative_runs"] >= be["runs"]
        assert be["storage_bytes"] == 400_000_000
        # Cold startup and steady execution are reported apart.
        assert cell["startup_seconds"]["baseline"]["mean"] == 2.0
        assert cell["steady_seconds"]["baseline"]["mean"] == pytest.approx(100.0)

    @pytest.mark.parametrize(("ratios", "verdict"), [
        ([1, 0.80, 0.81, 0.79, 0.80, 0.80], "slower"),
        ([1, 1.001, 0.999, 1.0, 1.002, 0.998], "equivalent"),
        ([1, 1.5, 0.7, 1.3, 0.8, 1.2], "inconclusive"),
    ])
    def test_verdicts_follow_the_interval_and_margin(
        self, ratios: list[float], verdict: str,
    ) -> None:
        plan = _plan()
        cell = _cell(_analyze(plan, _results(plan, {"h2o-ccsd": ratios})))
        assert cell["verdict"] == verdict
        assert cell["break_even"]["runs"] is None
        assert cell["break_even"]["reason"] == "no demonstrated saving"
        if verdict == "inconclusive":
            assert cell["reasons"] == ["the interval crosses the ±0.02 margin"]

    def test_warmups_never_count(self) -> None:
        plan = _plan()
        steady = _results(plan, {"h2o-ccsd": [1.25] * 6})
        wild = copy.deepcopy(steady)
        _run(wild, "tuned", 0)["steady_seconds"] = 1.0
        _run(wild, "generic", 0)["status"] = "failed"
        cell = _cell(_analyze(plan, wild))
        assert cell["ratio"] == _cell(_analyze(plan, steady))["ratio"]
        assert cell["excluded"] == [] and cell["warmup_runs"] == 2

    def test_failed_and_missing_replicates_are_listed_and_leave_it_inconclusive(self) -> None:
        plan = _plan()
        results = _results(plan)
        _run(results, "generic", 2)["status"] = "failed"
        _run(results, "tuned", 4)["status"] = "unconverged"
        results["runs"].remove(_run(results, "tuned", 3))
        cell = _cell(_analyze(plan, results))
        assert cell["pairs"] == 2 and cell["verdict"] == "inconclusive"
        assert cell["reasons"] == ["2 of 5 planned pairs retained"]
        assert cell["excluded"] == [
            {"block": 2, "reason": "generic run failed"},
            {"block": 4, "reason": "tuned run unconverged"},
        ]
        assert cell["missing"] == [{"block": 3, "variants": ["tuned"]}]

    def test_numerical_mismatch_and_missing_values_exclude_the_pair(self) -> None:
        plan = _plan()
        results = _results(plan)
        _run(results, "tuned", 1)["values"]["total_energy"] = -76.000001
        del _run(results, "tuned", 2)["values"]["total_energy"]
        _run(results, "tuned", 3)["values"]["total_energy"] = -76.000000005  # within 1e-8
        cell = _cell(_analyze(plan, results))
        assert [e["block"] for e in cell["excluded"]] == [1, 2]
        assert cell["excluded"][0]["reason"].startswith("numerical mismatch on total_energy")
        assert cell["excluded"][1]["reason"] == "total_energy missing from the tuned run"
        assert cell["pairs"] == 3

    def test_other_runtime_layout_or_cpu_model_is_excluded(self) -> None:
        plan = _plan()
        results = _results(plan)
        _run(results, "tuned", 1)["runtime_digest"] = "e" * 64
        _run(results, "generic", 2)["hardware"]["threads_per_rank"] = 4
        _run(results, "tuned", 3)["hardware"]["cpu_model"] = "Model Y"
        _run(results, "tuned", 4)["steady_seconds"] = None
        reasons = [e["reason"] for e in _cell(_analyze(plan, results))["excluded"]]
        assert reasons == [
            f"tuned ran runtime {'e' * 12}, not the planned {'b' * 12}",
            "generic ran a 1x4 layout, not the planned 1x8",
            "the pair ran on different CPU models: Model X and Model Y",
            "tuned recorded no steady_seconds",
        ]

    def test_a_node_class_spanning_cpu_models_is_incomparable(self) -> None:
        plan = _plan(workloads=[
            *_plan()["workloads"],
            {"name": "benzene-dfhf", "input_digest": "d" * 64,
             "checks": [{"quantity": "total_energy", "abs_tolerance": 1e-8}]},
        ])
        results = _results(plan, {"benzene-dfhf": [1.1] * 6})
        for run in results["runs"]:
            if run["workload"] == "h2o-ccsd" and run["block"] >= 4:
                run["hardware"]["cpu_model"] = "Model Y"
        report = _analyze(plan, results)
        cell = _cell(report)
        assert cell["verdict"] == "incomparable"
        assert cell["reasons"] == ["node class class-a ran on several CPU models: Model X, Model Y"]
        (aggregate,) = report["aggregates"]
        assert aggregate["workloads"] == ["benzene-dfhf"]
        assert aggregate["left_out"] == [{"workload": "h2o-ccsd", "verdict": "incomparable"}]
        assert aggregate["geomean_ratio"] == pytest.approx(1.1, abs=1e-6)

    def test_aggregate_is_the_equal_weight_geometric_mean(self) -> None:
        plan = _plan(workloads=[
            *_plan()["workloads"],
            {"name": "benzene-dfhf", "input_digest": "d" * 64,
             "checks": [{"quantity": "total_energy", "abs_tolerance": 1e-8}]},
        ])
        report = _analyze(plan, _results(plan, {"h2o-ccsd": [1.25] * 6,
                                                "benzene-dfhf": [1.1] * 6}))
        (aggregate,) = report["aggregates"]
        assert aggregate["weighting"] == "equal per workload"
        assert aggregate["geomean_ratio"] == pytest.approx(math.sqrt(1.25 * 1.1), abs=1e-6)

    @pytest.mark.parametrize(("mutate", "message"), [
        (lambda r: r.update(plan_sha256="f" * 64), "recorded against plan ffffffffffff"),
        (lambda r: r["runs"].append(dict(r["runs"][0])), "two runs recorded for workload"),
        (lambda r: r["runs"][0].update(workload="other"), "names workload 'other'"),
        (lambda r: r["runs"][0].update(block=6), "names block 6; the plan has 6"),
    ])
    def test_results_that_do_not_belong_to_the_plan_are_refused(
        self, mutate, message: str,
    ) -> None:
        plan = _plan()
        results = _results(plan)
        mutate(results)
        with pytest.raises(bm.BenchmarkError, match=message):
            _analyze(plan, results)

    def test_reports_are_reproducible_and_independent_of_run_order(self) -> None:
        plan = _plan()
        results = _results(plan, {"h2o-ccsd": [1.0, 1.25, 1.2, 1.3, 1.22, 1.27]})
        shuffled = copy.deepcopy(results)
        random.Random(1).shuffle(shuffled["runs"])
        first = _analyze(plan, results)
        assert bm.report_json(first) == bm.report_json(_analyze(plan, results))
        assert bm.report_json(first) == bm.report_json(_analyze(plan, shuffled))
        assert bm.report_csv(first) == bm.report_csv(_analyze(plan, shuffled))
        assert bm.report_svg(first) == bm.report_svg(_analyze(plan, shuffled))
        assert bm.report_text(first) == bm.report_text(_analyze(plan, shuffled))


SVG = {"svg": "http://www.w3.org/2000/svg"}


def test_svg_plots_log_ratios_and_intervals_without_inventing_missing_estimates() -> None:
    names = ["h2o-ccsd", "slower", "no-data", "one-pair", "mixed-cpu", "rounded-to-zero"]
    plan = _plan(workloads=[{**_plan()["workloads"][0], "name": name} for name in names])
    results = _results(plan, {"h2o-ccsd": [1.0, 1.25, 1.24, 1.26, 1.25, 1.25],
                              "slower": [0.8] * 6, "rounded-to-zero": [1e-7] * 6})
    results["runs"] = [r for r in results["runs"] if r["workload"] != "no-data"]
    _run(results, "tuned", 2, "one-pair")["status"] = "failed"
    for block in (3, 4, 5):
        results["runs"].remove(_run(results, "tuned", block, "one-pair"))
    for run in results["runs"]:
        if run["workload"] == "mixed-cpu" and run["block"] >= 3:
            run["hardware"]["cpu_model"] = "Model Y"
    report = _analyze(plan, results)
    root = ET.fromstring(bm.report_svg(report))
    cells = {g.attrib["data-workload"]: g for g in root.findall("svg:g", SVG)}
    assert set(cells) == set(names)
    assert json.loads(root.find("svg:metadata", SVG).text) == report

    # Read the plotted tick positions, then independently locate the known ratios.
    reference = float(root.find("svg:line[@class='baseline']", SVG).attrib["x1"])
    half = next(float(t.attrib["x"]) for t in root.findall("svg:text", SVG) if t.text == "0.5")
    scale = (reference - half) / math.log(2)
    for name in ("h2o-ccsd", "slower"):
        cell = _cell(report, name)
        group = cells[name]
        circle = group.find("svg:circle", SVG)
        x = float(circle.attrib["cx"])
        assert x == pytest.approx(reference + math.log(cell["ratio"]["geomean"]) * scale,
                                  abs=0.002)
        interval = group.find("svg:line", SVG)
        assert float(interval.attrib["x1"]) <= x <= float(interval.attrib["x2"])
        assert "5/5" in list(group.itertext())
    assert float(cells["h2o-ccsd"].find("svg:circle", SVG).attrib["cx"]) > reference
    assert float(cells["slower"].find("svg:circle", SVG).attrib["cx"]) < reference

    one = cells["one-pair"]
    assert one.find("svg:circle", SVG).attrib["fill"] == "white"
    assert one.find("svg:line", SVG) is None
    assert "Interval unavailable" in list(one.itertext())
    assert "Excluded: 1; missing: 3" in list(one.itertext())
    assert "1/5" in list(one.itertext())
    for name in ("no-data", "mixed-cpu", "rounded-to-zero"):
        assert cells[name].find("svg:circle", SVG) is None
        assert cells[name].find("svg:line", SVG) is None
    assert "Unavailable" in list(cells["no-data"].itertext())
    assert "0/5" in list(cells["no-data"].itertext())
    assert "Not comparable" in list(cells["mixed-cpu"].itertext())


def test_svg_keeps_arbitrary_evidence_as_inert_xml_text() -> None:
    plan = _plan()
    results = _results(plan)
    payload = '</title><script>alert("x")</script>&\x00'
    _run(results, "tuned", 1)["hardware"]["cpu_model"] = payload
    report = _analyze(plan, results)
    root = ET.fromstring(bm.report_svg(report))
    assert json.loads(root.find("svg:metadata", SVG).text) == report
    assert root.find(".//svg:script", SVG) is None
    assert not any("href" in key for element in root.iter() for key in element.attrib)
    tooltip = json.loads(root.find("svg:g/svg:title", SVG).text)
    assert payload in tooltip["excluded"][0]["reason"]


def test_cli_plans_then_analyzes_end_to_end(tmp_path: Path) -> None:
    plan = _plan()
    plan_file = tmp_path / "plan.json"
    plan_file.write_text(json.dumps(plan), encoding="utf-8")
    runner = CliRunner()

    planned = runner.invoke(main, ["benchmark", "plan", str(plan_file), "--json"])
    assert planned.exit_code == 0, planned.output
    order = json.loads(planned.output)
    assert len(order["runs"]) == 12

    # Record results from the printed digest and order, as a runner would.
    results = _results(plan)
    assert results["plan_sha256"] == order["plan_sha256"]
    results_file = tmp_path / "results.json"
    results_file.write_text(json.dumps(results), encoding="utf-8")
    csv_file = tmp_path / "cells.csv"
    svg_file = tmp_path / "speedups.svg"

    analyzed = runner.invoke(main, ["benchmark", "analyze", str(plan_file), str(results_file),
                                    "--json", "--csv", str(csv_file), "--svg", str(svg_file)])
    assert analyzed.exit_code == 0, analyzed.output
    report = json.loads(analyzed.output)
    assert report["schema_name"] == "vq.benchmark-report/1"
    assert _cell(report)["verdict"] == "faster"
    rows = csv_file.read_text(encoding="utf-8").splitlines()
    assert rows[0].split(",") == list(bm.CSV_COLUMNS) and len(rows) == 2
    svg = ET.fromstring(svg_file.read_text(encoding="utf-8"))
    assert json.loads(svg.find("svg:metadata", SVG).text) == report

    text = runner.invoke(main, ["benchmark", "analyze", str(plan_file), str(results_file)])
    assert text.exit_code == 0
    assert "class-a" in text.output and "faster" in text.output

    results_file.write_text(json.dumps({**results, "plan_sha256": "f" * 64}), encoding="utf-8")
    refused = runner.invoke(main, ["benchmark", "analyze", str(plan_file), str(results_file)])
    assert refused.exit_code == 1
    assert "a changed plan needs its own runs" in refused.output


@pytest.mark.parametrize("alias", ["input", "symlink", "hardlink", "csv"])
def test_cli_refuses_export_aliases_before_writing(tmp_path: Path, alias: str) -> None:
    plan = _plan()
    files = [tmp_path / "plan.json", tmp_path / "results.json"]
    for path, content in zip(files, [plan, _results(plan)], strict=True):
        path.write_text(json.dumps(content), encoding="utf-8")
    original = [path.read_bytes() for path in files]
    svg = tmp_path / "speedups.svg"
    options = []
    if alias == "input":
        svg = files[0]
    elif alias == "symlink":
        svg.symlink_to(files[1])
    elif alias == "hardlink":
        svg.hardlink_to(files[0])
    else:
        options = ["--csv", str(svg)]
        svg.write_text("previous export", encoding="utf-8")
    invoked = CliRunner().invoke(main, ["benchmark", "analyze", *map(str, files),
                                       "--svg", str(svg), *options])
    assert invoked.exit_code == 1
    assert "report outputs must be distinct" in invoked.output
    assert [path.read_bytes() for path in files] == original
    if alias == "csv":
        assert svg.read_text(encoding="utf-8") == "previous export"
