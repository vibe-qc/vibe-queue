"""#201: a calibration applies only within the feature ranges it was fitted on.

Each group records the smallest and largest value of every numeric feature
its decided training cases had. Applying the calibration, or judging a
held-out case with it, leaves an estimate outside those ranges, or missing
such a feature, uncalibrated and says why. A calibration fitted before ranges
existed checks nothing. Estimate receipts come from the real adapter.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from vq import estimate_calibration as cal
from vq import estimate_evaluation as ev
from vq.resource_estimate import estimate_vibeqc

sys.path.insert(0, str(Path(__file__).parent))
from test_estimate_calibration_159 import _dir, _set, _write  # noqa: E402
from test_estimate_evaluation_159 import _case, _usage  # noqa: E402

GIB = 1024**3


def _receipt(directory: Path, name: str, **features: object) -> Any:
    work = directory / f"est-{name}"
    work.mkdir(parents=True)
    manifest = work / "calc.system"
    manifest.write_text('[vibeqc]\nversion = "example"\n[plan]\nmethod = "hf"\n'
                        f'[outputs]\nstatus = "dry_run"\n[memory]\nestimate_bytes = {GIB}\n')
    (work / "features.json").write_text(json.dumps({"memory_scope": "rank", **features}))
    return estimate_vibeqc(manifest, features_file=work / "features.json")


def _ranked(directory: Path, name: str, *, observed: int | None = 2 * GIB,
            **features: object) -> dict[str, Any]:
    """A rank-scope case with the given features: exact evidence when observed."""
    receipt = _receipt(directory, name, **features).model_copy(
        update={"runtime_seconds": (10.0, 100.0)})
    (directory / f"{name}.estimate.json").write_text(receipt.model_dump_json())
    usage = None if observed is None else _usage(directory, name, rss=observed, processes=4)
    return _case(name, f"{name}.estimate.json", usage)


def _training(directory: Path) -> Path:
    _dir(directory)
    cases = [_ranked(directory, "a", nbf=100, naux=400), _ranked(directory, "b", nbf=300),
             # No usage: undecided, so its nbf is not part of the range.
             _ranked(directory, "c", observed=None, nbf=5000)]
    return _set(directory, "train", cases)


def test_a_group_records_the_ranges_of_its_decided_cases(tmp_path: Path) -> None:
    (group,) = cal.fit(_training(tmp_path / "train"), min_cases=2)["groups"]
    assert group["status"] == "calibrated"
    assert group["feature_ranges"] == {"naux": [400, 400], "nbf": [100, 300]}
    (runtime,) = cal.fit(_training(tmp_path / "t2"), min_cases=2)["runtime_groups"]
    assert runtime["feature_ranges"] == {"naux": [400, 400], "nbf": [100, 300]}


def test_resolved_zero_virtual_space_survives_fit_and_load(tmp_path: Path) -> None:
    directory = _dir(tmp_path / "train")
    cases = [_ranked(directory, "zero", nbf=1, occupied=1, virtual=0)]
    document = cal.fit(_set(directory, "train", cases), min_cases=1)
    assert document["groups"][0]["feature_ranges"]["virtual"] == [0, 0]
    calibration = cal.load(_write(tmp_path / "cal.json", document))
    receipt = _receipt(tmp_path, "in", nbf=1, occupied=1, virtual=0)
    assert receipt.features.virtual == 0
    assert cal.apply(receipt, calibration, hardware_class="class-a")["status"] == "calibrated"


def test_only_an_estimate_inside_every_range_is_calibrated(tmp_path: Path) -> None:
    path = _write(tmp_path / "cal.json", cal.fit(_training(tmp_path / "train"), min_cases=2))
    calibration = cal.load(path)

    inside = cal.apply(_receipt(tmp_path, "in", nbf=200, naux=400), calibration,
                       hardware_class="class-a")
    assert inside["status"] == "calibrated" and inside["calibrated_peak_bytes"] == 2 * GIB

    outside = cal.apply(_receipt(tmp_path, "out", nbf=301, naux=400), calibration,
                        hardware_class="class-a")
    assert outside["status"] == "uncalibrated" and outside["multiplier"] is None
    assert outside["reason"] == "its nbf 301 is outside the calibration's observed range 100-300"

    missing = cal.apply(_receipt(tmp_path, "missing", nbf=200), calibration,
                        hardware_class="class-a")
    assert missing["status"] == "uncalibrated"
    assert missing["reason"] == ("the estimate has no naux, so it cannot be placed in the "
                                 "calibration's observed range 400-400")


def test_a_calibration_from_before_ranges_checks_nothing(tmp_path: Path) -> None:
    document = cal.fit(_training(tmp_path / "train"), min_cases=2)
    for group in [*document["groups"], *document["runtime_groups"]]:
        del group["feature_ranges"]
    document["calibration_id"] = cal._digest(document)
    calibration = cal.load(_write(tmp_path / "cal.json", document))
    applied = cal.apply(_receipt(tmp_path, "far", nbf=9000), calibration,
                        hardware_class="class-a")
    assert applied["status"] == "calibrated"


def test_a_held_out_case_outside_the_ranges_reads_uncalibrated(tmp_path: Path) -> None:
    calibration = cal.load(_write(tmp_path / "cal.json",
                                  cal.fit(_training(tmp_path / "train"), min_cases=2)))
    held = _dir(tmp_path / "held")
    cases = [_ranked(held, "in", nbf=150, naux=400), _ranked(held, "out", nbf=50, naux=400)]
    report = ev.evaluate(_set(held, "held", cases), calibration)
    rows = {row["case_id"]: row for row in report["cases"]}
    assert rows["in"]["calibrated_memory"] == "within"
    assert rows["in"]["calibration_out_of_range"] is None
    assert rows["out"]["calibrated_memory"] == "uncalibrated"
    assert rows["out"]["calibrated_runtime"] == "uncalibrated"
    reason = "its nbf 50 is outside the calibration's observed range 100-300"
    assert rows["out"]["calibration_out_of_range"] == reason
    assert rows["out"]["runtime_calibration_out_of_range"] == reason
