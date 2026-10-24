"""#159: runtime calibration, fitted on a training set and judged on a held-out one.

Beside its peak-memory groups, a calibration fits ``runtime_groups`` against
the upper bound of each estimate's runtime interval, keyed without the
memory scope. A completed, measured run is an exact wall/upper ratio; a run
stopped at its time limit, or stopped after it passed the bound, a lower
bound. ``vq estimate-evaluate --calibration`` then gives each held-out case a
``calibrated_runtime`` verdict for its interval with the upper bound scaled.
Estimate receipts come from the real ``vq estimate`` adapter.
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
from test_estimate_calibration_159 import _dir, _loaded, _set  # noqa: E402
from test_estimate_evaluation_159 import _case, _estimate, _usage  # noqa: E402

GIB = 1024**3


def _timed(directory: Path, name: str, *, wall: float | None, upper: float = 100.0,
           lower: float = 10.0, outcome: str = "completed", scope: str = "job",
           method: str = "hf", hardware: str = "class-a", **extra: object) -> dict[str, Any]:
    estimate = _estimate(directory, name, peak=4 * GIB, scope=scope, method=method,
                         runtime=(lower, upper))
    usage = None if wall is None else _usage(directory, name, rss=GIB, wall=wall,
                                             processes=1)
    return _case(name, estimate, usage, outcome=outcome, hardware=hardware, **extra)


def _write(path: Path, document: dict[str, Any]) -> Path:
    path.write_text(json.dumps(document))
    return path


class TestEvidence:
    def test_what_each_kind_of_run_says_about_the_upper_bound(self, tmp_path: Path) -> None:
        d = tmp_path
        cases = [
            _timed(d, "within", wall=80.0),
            _timed(d, "over", wall=150.0),
            _timed(d, "fast", wall=5.0),
            _timed(d, "time-limited", wall=120.0, outcome="time_exceeded",
                   wall_limit_seconds=200),
            _timed(d, "limit-only", wall=None, outcome="time_exceeded",
                   wall_limit_seconds=250),
            _timed(d, "limited-early", wall=50.0, outcome="time_exceeded",
                   wall_limit_seconds=60),
            _timed(d, "killed-late", wall=130.0, outcome="killed"),
            _timed(d, "killed-early", wall=50.0, outcome="killed"),
            _timed(d, "oom-late", wall=140.0, outcome="oom_killed"),
            _timed(d, "failed", wall=500.0, outcome="failed"),
            _timed(d, "unmeasured", wall=None),
            _timed(d, "zero-bound", wall=5.0, lower=0.0, upper=0.0),
            _case("no-interval", _estimate(d, "no-interval", peak=GIB),
                  _usage(d, "no-interval", rss=GIB, wall=50.0, processes=1)),
        ]
        said = {item.case.case_id: cal.runtime_evidence(item) for item in _loaded(d, cases)}
        assert said == {
            "within": ("exact", 0.8), "over": ("exact", 1.5), "fast": ("exact", 0.05),
            "time-limited": ("lower_bound", 2.0), "limit-only": ("lower_bound", 2.5),
            "limited-early": None, "killed-late": ("lower_bound", 1.3),
            "killed-early": None, "oom-late": ("lower_bound", 1.4), "failed": None,
            "unmeasured": None, "zero-bound": None, "no-interval": None}


class TestFit:
    def test_runtime_groups_ignore_the_memory_scope_and_take_the_worst_ratio(
        self, tmp_path: Path,
    ) -> None:
        d = _dir(tmp_path / "train")
        cases = [_timed(d, f"job-{i}", wall=w, scope="job")
                 for i, w in enumerate([50.0, 90.0, 120.0])]
        cases += [_timed(d, f"rank-{i}", wall=w, scope="rank")
                  for i, w in enumerate([70.0, 110.0])]
        cases += [_timed(d, f"mp2-{i}", wall=40.0, method="mp2") for i in range(4)]
        cases += [_timed(d, f"b-{i}", wall=30.0, hardware="class-b") for i in range(5)]
        document = cal.fit(_set(d, "train-1", cases), min_cases=5)
        assert document["runtime_method"] == "worst-observed-over-predicted-runtime-upper/1"
        groups = {(g["method"], g["hardware_class"]): g for g in document["runtime_groups"]}
        assert set(groups) == {("hf", "class-a"), ("mp2", "class-a"), ("hf", "class-b")}
        hf = groups[("hf", "class-a")]
        assert (hf["cases"], hf["exact"], hf["status"], hf["multiplier"],
                hf["multiplier_is_lower_bound"]) == (5, 5, "calibrated", 1.2, False)
        assert "memory_scope" not in hf
        assert (groups[("mp2", "class-a")]["status"], groups[("mp2", "class-a")]["multiplier"]
                ) == ("insufficient", None)
        assert groups[("hf", "class-b")]["multiplier"] == 1.0  # never lowered
        # Memory groups are fitted as before, by scope.
        assert {(g["method"], g["memory_scope"]) for g in document["groups"]} >= {
            ("hf", "job"), ("hf", "rank")}
        path = _write(tmp_path / "c.json", document)
        assert [g.key() for g in cal.load(path).runtime_groups] == [
            tuple(g[f] for f in cal.RUNTIME_KEY_FIELDS) for g in document["runtime_groups"]]

    def test_a_worst_case_that_is_only_a_lower_bound_says_so(self, tmp_path: Path) -> None:
        d = _dir(tmp_path / "train")
        cases = [_timed(d, "a", wall=90.0), _timed(d, "b", wall=110.0),
                 _timed(d, "limited", wall=100.0, outcome="time_exceeded",
                        wall_limit_seconds=300)]
        (group,) = cal.fit(_set(d, "t", cases), min_cases=3)["runtime_groups"]
        assert (group["multiplier"], group["multiplier_is_lower_bound"], group["exact"],
                group["lower_bounds"]) == (3.0, True, 2, 1)


class TestLoad:
    def _fitted(self, tmp_path: Path) -> dict[str, Any]:
        d = _dir(tmp_path / "train")
        return cal.fit(_set(d, "t", [_timed(d, "a", wall=120.0)]), min_cases=1)

    def test_a_calibration_from_before_runtime_groups_still_loads(
        self, tmp_path: Path,
    ) -> None:
        document = self._fitted(tmp_path)
        del document["runtime_method"], document["runtime_groups"]
        document["calibration_id"] = cal._digest(document)
        assert cal.load(_write(tmp_path / "old.json", document)).runtime_groups == []

    @pytest.mark.parametrize("damage", ["no-groups", "status"])
    def test_inconsistent_runtime_groups_are_refused(self, tmp_path: Path,
                                                     damage: str) -> None:
        document = self._fitted(tmp_path)
        if damage == "no-groups":
            del document["runtime_groups"]
        else:
            document["runtime_groups"][0]["multiplier"] = None
        document["calibration_id"] = cal._digest(document)
        with pytest.raises(ev.EvaluationError, match="go together|disagree"):
            cal.load(_write(tmp_path / "bad.json", document))


def test_held_out_cases_get_a_calibrated_runtime_verdict(tmp_path: Path) -> None:
    t = _dir(tmp_path / "train")
    training = _set(t, "train-1", [_timed(t, f"r{i}", wall=w)
                                   for i, w in enumerate([60.0, 150.0, 80.0])])
    runner = CliRunner()
    output = tmp_path / "calibration.json"
    fitted = runner.invoke(main, ["estimate-calibrate", str(training), "-o", str(output),
                                  "--min-cases", "3"])
    assert fitted.exit_code == 0, fitted.output
    assert "runtime upper bounds:" in fitted.output
    assert "fixture" not in fitted.output and "x1.5" in fitted.output

    h = _dir(tmp_path / "held")
    held = _set(h, "held-1", [
        _timed(h, "covered-now", wall=140.0),  # above 100, within 150
        _timed(h, "still-over", wall=200.0),
        _timed(h, "near-lower", wall=12.0),  # the lower bound (10) is not scaled
        _timed(h, "elsewhere", wall=140.0, hardware="class-b"),
        _timed(h, "stopped", wall=50.0, outcome="killed"),  # may or may not have fit
    ])
    report = json.loads(runner.invoke(main, ["estimate-evaluate", str(held),
                                             "--calibration", str(output), "--json"]).output)
    rows = {c["case_id"]: c for c in report["cases"]}
    assert {k: (r["runtime"], r["calibrated_runtime"]) for k, r in rows.items()} == {
        "covered-now": ("above", "covered"), "still-over": ("above", "above"),
        "near-lower": ("covered", "covered"), "elsewhere": ("above", "uncalibrated"),
        "stopped": ("censored", "censored")}
    assert (rows["covered-now"]["runtime_calibration_multiplier"],
            rows["covered-now"]["calibrated_runtime_upper_seconds"]) == (1.5, 150.0)
    assert rows["elsewhere"]["calibrated_runtime_upper_seconds"] is None
    summary = report["overall"]["runtime"]["calibrated"]
    assert summary["coverage"] == {"lower": 0.5, "upper": 0.75, "n": 4}
    assert summary["uncalibrated"] == 1
    assert report["calibration"]["runtime_method"] == cal.RUNTIME_METHOD

    text = runner.invoke(main, ["estimate-evaluate", str(held), "--calibration", str(output)])
    overall = next(line for line in text.output.splitlines() if line.startswith("(all)"))
    assert overall.split()[-3:] == ["50%-75%", "of", "4"]


def test_a_calibration_without_runtime_groups_leaves_runtime_uncalibrated(
    tmp_path: Path,
) -> None:
    t = _dir(tmp_path / "train")
    document = cal.fit(_set(t, "t", [_timed(t, "a", wall=120.0)]), min_cases=1)
    del document["runtime_method"], document["runtime_groups"]
    document["calibration_id"] = cal._digest(document)
    calibration = cal.load(_write(tmp_path / "old.json", document))
    h = _dir(tmp_path / "held")
    report: dict[str, Any] = ev.evaluate(_set(h, "h", [_timed(h, "b", wall=121.0)]),
                                         calibration)
    assert report["cases"][0]["calibrated_runtime"] == "uncalibrated"
    assert report["calibration"]["runtime_method"] is None
