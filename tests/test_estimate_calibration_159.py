"""#159: peak-memory calibration fitted on a training set, judged on a held-out one.

Estimate receipts come from the real ``vq estimate`` adapter; usage receipts
are synthetic so each piece of evidence is pinned exactly. Fitting uses only
decided evidence (exact ratios where the measurement compares, lower bounds
otherwise); the held-out evaluation refuses the training set and its cases.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vq import estimate_calibration as cal
from vq import estimate_evaluation as ev
from vq.cli import main

sys.path.insert(0, str(Path(__file__).parent))
from test_estimate_evaluation_159 import _case, _estimate, _usage  # noqa: E402

GIB = 1024**3


def _dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _set(directory: Path, set_id: str, cases: list[dict[str, Any]]) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "set.json"
    path.write_text(json.dumps({"schema_name": "vq.estimate-evaluation-set/1",
                                "set_id": set_id, "cases": cases}))
    return path


def _rank_case(directory: Path, name: str, *, peak: int, observed: int,
               method: str = "hf", hardware: str = "class-a") -> dict[str, Any]:
    """A rank-scope estimate and a measured peak: an exact ratio."""
    return _case(name, _estimate(directory, name, peak=peak, scope="rank", method=method),
                 _usage(directory, name, rss=observed, processes=4), hardware=hardware)


def _loaded(directory: Path, cases: list[dict[str, Any]]) -> list[ev.LoadedCase]:
    return ev.load_set(_set(directory, "s", cases))[2]


class TestEvidence:
    def test_what_each_kind_of_case_says(self, tmp_path: Path) -> None:
        d = tmp_path
        cases = [
            _rank_case(d, "rank-within", peak=4 * GIB, observed=2 * GIB),
            _case("single", _estimate(d, "single", peak=4 * GIB),
                  _usage(d, "single", rss=3 * GIB, processes=1)),
            _case("multi", _estimate(d, "multi", peak=4 * GIB),
                  _usage(d, "multi", rss=3 * GIB, processes=4)),
            _rank_case(d, "rank-over", peak=2 * GIB, observed=3 * GIB),
            _case("multi-over", _estimate(d, "multi-over", peak=2 * GIB),
                  _usage(d, "multi-over", rss=3 * GIB, processes=4)),
            _case("oom", _estimate(d, "oom", peak=2 * GIB), None, outcome="oom_killed",
                  memory_limit_bytes=5 * GIB, memory_limit_scope="job"),
            _case("timeout", _estimate(d, "timeout", peak=2 * GIB),
                  _usage(d, "timeout", rss=1 * GIB, processes=1), outcome="time_exceeded"),
            _case("failed", _estimate(d, "failed", peak=2 * GIB),
                  _usage(d, "failed", rss=1 * GIB, processes=1), outcome="failed"),
            _case("no-peak", _estimate(d, "no-peak", peak=None),
                  _usage(d, "no-peak", rss=1 * GIB, processes=1)),
            _case("unobserved", _estimate(d, "unobserved", peak=2 * GIB), None),
        ]
        said = {item.case.case_id: cal.evidence(item) for item in _loaded(d, cases)}
        assert said == {
            "rank-within": ("exact", 0.5), "single": ("exact", 0.75), "multi": None,
            "rank-over": ("exact", 1.5), "multi-over": ("lower_bound", 1.5),
            "oom": ("lower_bound", 2.5), "timeout": None, "failed": None,
            "no-peak": None, "unobserved": None}


class TestFit:
    def test_groups_thresholds_and_the_worst_ratio(self, tmp_path: Path) -> None:
        d = _dir(tmp_path / "train")
        cases = [_rank_case(d, f"hf-{i}", peak=4 * GIB, observed=int(4 * GIB * r))
                 for i, r in enumerate([0.5, 0.9, 1.25, 1.1, 0.8])]
        cases += [_rank_case(d, f"mp2-{i}", peak=4 * GIB, observed=3 * GIB, method="mp2")
                  for i in range(4)]
        cases += [_rank_case(d, f"gpu-{i}", peak=4 * GIB, observed=2 * GIB,
                             hardware="class-b") for i in range(5)]
        document = cal.fit(_set(d, "train-1", cases), min_cases=5)
        groups = {(g["method"], g["hardware_class"]): g for g in document["groups"]}
        assert groups[("hf", "class-a")]["status"] == "calibrated"
        assert groups[("hf", "class-a")]["multiplier"] == 1.25  # the worst of the five
        assert groups[("mp2", "class-a")] | {"status": "insufficient", "multiplier": None} \
            == groups[("mp2", "class-a")] and groups[("mp2", "class-a")]["exact"] == 4
        # Conservative estimates are never lowered by a calibration.
        assert groups[("hf", "class-b")]["multiplier"] == 1.0
        assert {g["program_version"] for g in document["groups"]} == {"example"}
        assert len(document["training_cases"]) == 14
        assert cal.load(_write(tmp_path / "c.json", document)).calibration_id == (
            document["calibration_id"])

    def test_a_worst_case_that_is_only_a_lower_bound_says_so(self, tmp_path: Path) -> None:
        d = _dir(tmp_path / "train")
        cases = [_rank_case(d, f"r{i}", peak=4 * GIB, observed=4 * GIB) for i in range(2)]
        cases.append(_case("oom", _estimate(d, "oom", peak=4 * GIB, scope="rank"), None,
                           outcome="oom_killed", memory_limit_bytes=6 * GIB,
                           memory_limit_scope="rank"))
        (group,) = cal.fit(_set(d, "t", cases), min_cases=3)["groups"]
        assert (group["multiplier"], group["multiplier_is_lower_bound"], group["exact"],
                group["lower_bounds"]) == (1.5, True, 2, 1)

    def test_an_exact_worst_case_above_a_bound_is_exact(self, tmp_path: Path) -> None:
        d = _dir(tmp_path / "train")
        cases = [_rank_case(d, "r", peak=4 * GIB, observed=6 * GIB),
                 _case("oom", _estimate(d, "oom", peak=4 * GIB, scope="rank"), None,
                       outcome="oom_killed", memory_limit_bytes=5 * GIB,
                       memory_limit_scope="rank")]
        (group,) = cal.fit(_set(d, "t", cases), min_cases=2)["groups"]
        assert (group["multiplier"], group["multiplier_is_lower_bound"]) == (1.5, False)

    def test_a_bound_of_one_is_no_lower_bound_multiplier(self, tmp_path: Path) -> None:
        d = _dir(tmp_path / "train")
        cases = [_rank_case(d, "r", peak=4 * GIB, observed=2 * GIB),
                 _case("oom", _estimate(d, "oom", peak=4 * GIB, scope="rank"), None,
                       outcome="oom_killed", memory_limit_bytes=4 * GIB,
                       memory_limit_scope="rank")]
        (group,) = cal.fit(_set(d, "t", cases), min_cases=2)["groups"]
        assert (group["multiplier"], group["multiplier_is_lower_bound"]) == (1.0, False)

    def test_undecided_cases_never_count(self, tmp_path: Path) -> None:
        d = _dir(tmp_path / "train")
        cases = [_case(f"m{i}", _estimate(d, f"m{i}", peak=4 * GIB),
                       _usage(d, f"m{i}", rss=GIB, processes=8)) for i in range(6)]
        (group,) = cal.fit(_set(d, "t", cases), min_cases=1)["groups"]
        assert (group["status"], group["cases"], group["exact"], group["lower_bounds"]) == (
            "insufficient", 6, 0, 0)

    def test_min_cases_must_be_positive(self, tmp_path: Path) -> None:
        d = _dir(tmp_path / "train")
        with pytest.raises(ev.EvaluationError, match="at least 1"):
            cal.fit(_set(d, "t", [_rank_case(d, "a", peak=GIB, observed=GIB)]), min_cases=0)


def _write(path: Path, document: dict[str, Any]) -> Path:
    path.write_text(json.dumps(document))
    return path


class TestLoad:
    def _document(self, tmp_path: Path) -> dict[str, Any]:
        d = _dir(tmp_path / "train")
        return cal.fit(_set(d, "t", [_rank_case(d, "a", peak=GIB, observed=2 * GIB)]),
                       min_cases=1)

    def test_a_changed_document_no_longer_matches_its_identity(self, tmp_path: Path) -> None:
        document = self._document(tmp_path)
        document["groups"][0]["multiplier"] = 1.0
        with pytest.raises(ev.EvaluationError, match="calibration_id does not match"):
            cal.load(_write(tmp_path / "c.json", document))

    def test_status_and_multiplier_must_agree(self, tmp_path: Path) -> None:
        document = self._document(tmp_path)
        document["groups"][0]["status"] = "insufficient"
        document["calibration_id"] = cal._digest(document)
        with pytest.raises(ev.EvaluationError, match="status and multiplier disagree"):
            cal.load(_write(tmp_path / "c.json", document))

    @pytest.mark.parametrize("change", [
        {"schema_name": "vq.estimate-calibration/2"},
        {"method": "best-guess/1"},
        {"min_cases": 0},
    ])
    def test_another_shape_is_refused(self, tmp_path: Path, change: dict[str, Any]) -> None:
        document = {**self._document(tmp_path), **change}
        document["calibration_id"] = cal._digest(document)
        with pytest.raises(ev.EvaluationError, match="not a vq.estimate-calibration/1"):
            cal.load(_write(tmp_path / "c.json", document))

    def test_a_multiplier_below_one_is_refused(self, tmp_path: Path) -> None:
        document = self._document(tmp_path)
        document["groups"][0]["multiplier"] = 0.5
        document["calibration_id"] = cal._digest(document)
        with pytest.raises(ev.EvaluationError, match="not a vq.estimate-calibration/1"):
            cal.load(_write(tmp_path / "c.json", document))


@pytest.fixture
def fitted(tmp_path: Path) -> tuple[Path, Path]:
    """A calibration (hf on class-a: x2) and the training set it was fitted on."""
    d = _dir(tmp_path / "train")
    cases = [_rank_case(d, f"t{i}", peak=4 * GIB, observed=8 * GIB) for i in range(3)]
    training = _set(d, "train-1", cases)
    return _write(tmp_path / "cal.json", cal.fit(training, min_cases=3)), training


class TestHeldOut:
    def _held_out(self, tmp_path: Path) -> Path:
        d = _dir(tmp_path / "held")
        return _set(d, "held-1", [
            _rank_case(d, "fits-only-calibrated", peak=4 * GIB, observed=6 * GIB),
            _rank_case(d, "too-big-even-so", peak=4 * GIB, observed=9 * GIB),
            _rank_case(d, "other-method", peak=4 * GIB, observed=6 * GIB, method="mp2"),
        ])

    def test_calibrated_verdicts_sit_beside_the_raw_ones(
        self, tmp_path: Path, fitted: tuple[Path, Path],
    ) -> None:
        calibration = cal.load(fitted[0])
        report = ev.evaluate(self._held_out(tmp_path), calibration)
        rows = {c["case_id"]: c for c in report["cases"]}
        assert {k: (r["memory"], r["calibrated_memory"], r["calibration_multiplier"])
                for k, r in rows.items()} == {
            "fits-only-calibrated": ("underpredicted", "within", 2.0),
            "too-big-even-so": ("underpredicted", "underpredicted", 2.0),
            "other-method": ("underpredicted", "uncalibrated", None)}
        assert rows["fits-only-calibrated"]["calibrated_peak_bytes"] == 8 * GIB
        overall = report["overall"]["memory"]
        assert overall["underprediction"] == {"lower": 1.0, "upper": 1.0, "n": 3}
        assert overall["calibrated"]["underprediction"] == {"lower": 0.5, "upper": 0.5, "n": 2}
        assert overall["calibrated"]["uncalibrated"] == 1
        assert report["calibration"]["calibration_id"] == calibration.calibration_id
        assert report["calibration"]["training_set_id"] == "train-1"

    def test_without_a_calibration_the_report_is_unchanged(self, tmp_path: Path) -> None:
        report = ev.evaluate(self._held_out(tmp_path))
        assert "calibration" not in report
        assert "calibrated" not in report["overall"]["memory"]
        assert all("calibrated_memory" not in row for row in report["cases"])

    def test_the_training_set_and_its_cases_are_refused(
        self, tmp_path: Path, fitted: tuple[Path, Path],
    ) -> None:
        calibration = cal.load(fitted[0])
        with pytest.raises(ev.EvaluationError, match="the set this calibration was fitted on"):
            ev.evaluate(fitted[1], calibration)
        # The same receipts under a new name and set are still training cases.
        training = json.loads(fitted[1].read_text())
        reused = _set(fitted[1].parent, "held-2",
                      [{**training["cases"][0], "case_id": "renamed"}])
        reused_file = fitted[1].parent / "held.json"
        reused.rename(reused_file)
        with pytest.raises(ev.EvaluationError, match="case renamed is also a training case"):
            ev.evaluate(reused_file, calibration)


def test_the_cli_fits_then_judges_on_held_out_cases(tmp_path: Path) -> None:
    runner = CliRunner()
    d = _dir(tmp_path / "train")
    training = _set(d, "train-1", [_rank_case(d, f"t{i}", peak=4 * GIB, observed=8 * GIB)
                                   for i in range(5)])
    output = tmp_path / "cal.json"
    fitted = runner.invoke(main, ["estimate-calibrate", str(training), "-o", str(output)])
    assert fitted.exit_code == 0, fitted.output
    assert "vibeqc example hf rank on class-a: 5 case(s), 5 exact, 0 lower-bound: x2" in (
        fitted.output)
    again = runner.invoke(main, ["estimate-calibrate", str(training), "-o", str(output)])
    assert again.exit_code != 0 and "is never overwritten" in again.output
    assert runner.invoke(main, ["estimate-calibrate", str(training), "-o",
                                str(tmp_path / "x.json"), "--min-cases", "0"]).exit_code == 2

    h = _dir(tmp_path / "held")
    held = _set(h, "held-1", [_rank_case(h, "a", peak=4 * GIB, observed=6 * GIB)])
    text = runner.invoke(main, ["estimate-evaluate", str(held), "--calibration", str(output)])
    assert text.exit_code == 0, text.output
    header = next(line for line in text.output.splitlines() if line.startswith("METHOD"))
    assert header.split() == ["METHOD", "HARDWARE", "CASES", "MEM", "UNDERPREDICTED",
                              "OBS/PRED", "MAX", "RUNTIME", "COVERED", "CALIBRATED",
                              "UNDER", "UNCALIBRATED", "CALIBRATED", "COVERED"]
    assert "fitted on training set train-1" in text.output
    as_json = runner.invoke(main, ["estimate-evaluate", str(held), "--calibration",
                                   str(output), "--json"])
    assert json.loads(as_json.output)["cases"][0]["calibrated_memory"] == "within"
    refused = runner.invoke(main, ["estimate-evaluate", str(training), "--calibration",
                                   str(output)])
    assert refused.exit_code != 0 and "held-out" in refused.output
