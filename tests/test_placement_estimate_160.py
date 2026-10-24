"""#160/#159: a placement request completed from a calibrated estimate.

`vq placement explain FLEET REQUEST --estimate CALIBRATED.json` fills the
request's unset `runtime_seconds` (the receipt's lower bound and the
calibrated upper bound) and `mem_mb_per_node` (the calibrated peak in MiB,
when that figure is per node). Explicit request values win. The explanation
says, per field, what was filled, kept or not filled and why. Calibrations
are fitted for real on the evaluation helpers' receipts.
"""
from __future__ import annotations

import itertools
import json
import sys
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vq import estimate_calibration as cal
from vq import placement_policy as pp
from vq.cli import main
from vq.resource_estimate import estimate_vibeqc

sys.path.insert(0, str(Path(__file__).parent))
from test_estimate_calibration_159 import _dir, _set  # noqa: E402
from test_estimate_evaluation_159 import _case, _estimate, _usage  # noqa: E402
from test_placement_policy_160 import NOW, _explain, _fleet, _request  # noqa: E402

GIB = 1024**3
_COUNT = itertools.count()


def _calibrated(tmp_path: Path, *, scope: str = "job", hardware: str = "class-a",
                runtime: tuple[float, float] | None = (60.0, 1000.1)) -> dict[str, Any]:
    """Memory and runtime both x1.5 for hf on class-a, at this memory scope."""
    tmp_path = tmp_path / f"estimate-{next(_COUNT)}"
    d = _dir(tmp_path / f"train-{scope}")
    cases = [_case(f"r{i}", _estimate(d, f"r{i}", peak=4 * GIB, scope=scope,
                                      runtime=(10.0, 100.0)),
                   _usage(d, f"r{i}", rss=6 * GIB if i == 0 else 4 * GIB,
                          wall=150.0 if i == 0 else 80.0, processes=1))
             for i in range(5)]
    calibration = cal.Calibration.model_validate_json(
        json.dumps(cal.fit(_set(d, "train-1", cases), min_cases=5)))
    work = tmp_path / f"est-{scope}"
    work.mkdir(exist_ok=True)
    manifest = work / "calc.system"
    manifest.write_text('[vibeqc]\nversion = "example"\n[plan]\nmethod = "hf"\n'
                        '[outputs]\nstatus = "dry_run"\n'
                        f"[memory]\nestimate_bytes = {3 * GIB + 1}\n")
    features = work / "features.json"
    features.write_text(json.dumps({"memory_scope": scope}))
    receipt = estimate_vibeqc(manifest, features_file=features).model_copy(
        update={"runtime_seconds": runtime})
    return cal.apply(receipt, calibration, hardware_class=hardware)


def _without(**fields: object) -> pp.Request:
    request = _request(**fields)
    for name in ("runtime_seconds", "mem_mb_per_node"):
        if name not in fields:
            request.pop(name)
    return pp.Request.model_validate(request)


MIB_4609 = 4609  # ceil((3 GiB + 1 byte) x 1.5, then to MiB)


class TestFilling:
    def test_unset_runtime_and_memory_are_filled(self, tmp_path: Path) -> None:
        document = _calibrated(tmp_path)
        request, report = pp.request_with_estimate(_without(), document)
        assert request.runtime_seconds == pp.Interval(low=60.0, high=1500.15)
        assert request.mem_mb_per_node == MIB_4609
        assert report == {
            "estimate_sha256": document["estimate_sha256"],
            "calibration_id": document["calibration_id"], "hardware_class": "class-a",
            "fields": {
                "mem_mb_per_node": {"source": "filled", "value": MIB_4609},
                "runtime_seconds": {"source": "filled",
                                    "value": {"low": 60.0, "high": 1500.15}}}}

    def test_explicit_request_values_win(self, tmp_path: Path) -> None:
        explicit = _without(runtime_seconds={"low": 1.0, "high": 2.0}, mem_mb_per_node=8)
        request, report = pp.request_with_estimate(explicit, _calibrated(tmp_path))
        assert request == explicit
        assert report["fields"] == {"mem_mb_per_node": {"source": "kept"},
                                    "runtime_seconds": {"source": "kept"}}

    @pytest.mark.parametrize(("scope", "nodes", "filled"), [
        ("node", 2, True), ("job", 1, True), ("job", 2, False), ("rank", 1, False)])
    def test_memory_is_filled_only_when_it_is_per_node(
        self, tmp_path: Path, scope: str, nodes: int, filled: bool,
    ) -> None:
        request, report = pp.request_with_estimate(
            _without(nodes=nodes), _calibrated(tmp_path, scope=scope))
        field = report["fields"]["mem_mb_per_node"]
        if filled:
            assert field["source"] == "filled" and request.mem_mb_per_node == MIB_4609
        else:
            assert request.mem_mb_per_node is None
            assert field == {"source": "not filled", "reason": (
                f"a {scope}-scope figure is not memory per node"
                + (f" of a {nodes}-node job" if scope == "job" else ""))}

    def test_uncalibrated_parts_say_why(self, tmp_path: Path) -> None:
        request, report = pp.request_with_estimate(
            _without(), _calibrated(tmp_path, hardware="class-b"))
        assert (request.runtime_seconds, request.mem_mb_per_node) == (None, None)
        assert report["fields"]["runtime_seconds"]["reason"].startswith(
            "no runtime group of this calibration matches")
        assert report["fields"]["mem_mb_per_node"]["reason"].startswith(
            "no group of this calibration matches")
        _request_, no_interval = pp.request_with_estimate(
            _without(), _calibrated(tmp_path, runtime=None))
        assert no_interval["fields"]["runtime_seconds"] == {
            "source": "not filled", "reason": "the estimate has no runtime interval"}

    def test_a_document_without_a_runtime_part_says_so(self, tmp_path: Path) -> None:
        document = _calibrated(tmp_path)
        for name in cal._RUNTIME_FIELDS:
            del document[name]
        _request_, report = pp.request_with_estimate(_without(), document)
        assert report["fields"]["runtime_seconds"]["reason"] == (
            "this calibrated estimate has no runtime part")

    def test_a_raw_or_altered_estimate_is_refused(self, tmp_path: Path) -> None:
        document = _calibrated(tmp_path)
        with pytest.raises(pp.PlacementError, match="needs a calibrated estimate"):
            pp.request_with_estimate(_without(), document["estimate"])
        with pytest.raises(pp.PlacementError, match="--estimate: "):
            pp.request_with_estimate(_without(), {**document, "multiplier": 9.0})


def test_without_an_estimate_explanations_are_unchanged() -> None:
    assert "estimate" not in _explain()


def test_the_cli_fills_explains_and_reports(tmp_path: Path) -> None:
    fleet_file, request_file = tmp_path / "fleet.json", tmp_path / "request.json"
    estimate_file = tmp_path / "calibrated.json"
    fleet_file.write_text(json.dumps(_fleet()))
    request = _request()
    for name in ("runtime_seconds", "mem_mb_per_node"):
        request.pop(name)
    request_file.write_text(json.dumps(request))
    estimate_file.write_text(json.dumps(_calibrated(tmp_path)))
    args = ["placement", "explain", str(fleet_file), str(request_file),
            "--now", NOW.isoformat(), "--estimate", str(estimate_file)]
    as_json = CliRunner().invoke(main, [*args, "--json"])
    assert as_json.exit_code == 0, as_json.output
    result = json.loads(as_json.output)
    assert result["request"]["mem_mb_per_node"] == MIB_4609
    assert result["request"]["runtime_seconds"] == {"low": 60.0, "high": 1500.15}
    assert result["estimate"]["fields"]["mem_mb_per_node"]["source"] == "filled"
    assert all(c["components"]["execution"].get("unknown") is not True
               for c in result["candidates"])
    text = CliRunner().invoke(main, args)
    assert text.exit_code == 0, text.output
    assert "mem_mb_per_node: filled, 4609 MB" in text.output
    assert "runtime_seconds: filled, 60-1500.15 s" in text.output
    raw = tmp_path / "raw.json"
    raw.write_text(json.dumps(_calibrated(tmp_path)["estimate"]))
    refused = CliRunner().invoke(main, [*args[:-1], str(raw)])
    assert refused.exit_code != 0 and "needs a calibrated estimate" in refused.output
