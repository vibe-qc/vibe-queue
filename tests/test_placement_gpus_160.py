"""#160: GPU requirements in read-only placement explanations.

A request may name `gpus_per_node`. A target records `node.gpus`: 0 means
known none, and an absent value is unknown and rejects. A daemon may also
record `free.gpus`, its unassigned devices now (#168), which decides whether
it "fits the free capacity now". A request without GPUs is planned exactly
as before. Uses the simulated fleet of test_placement_policy_160.py.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner
from pydantic import ValidationError

from vq import placement_policy as pp
from vq.cli import main

sys.path.insert(0, str(Path(__file__).parent))
from test_placement_policy_160 import (  # noqa: E402
    NOW,
    _accounts_for_every_target,
    _explain,
    _fleet,
    _ranked,
    _rejected,
    _request,
)


def _gpu_fleet() -> dict[str, Any]:
    """fast-busy has 4 GPUs a node, idle-slow 2 (1 free now), mem-rich none,
    avx-only unknown."""
    fleet = _fleet()
    by_name = {t["name"]: t for t in fleet["targets"]}
    by_name["fast-busy"]["node"]["gpus"] = 4
    by_name["idle-slow"]["node"]["gpus"] = 2
    by_name["idle-slow"]["free"]["gpus"] = 1
    by_name["mem-rich"]["node"]["gpus"] = 0
    return fleet


class TestGpuRequirement:
    def test_gpus_filter_before_ranking_and_keep_other_rejections(self) -> None:
        result = _explain(_gpu_fleet(), gpus_per_node=2)
        rejected = _rejected(result)
        assert "needs 2 GPUs per node; a node has 0" in rejected["mem-rich"]
        assert "GPUs per node unknown" in rejected["avx-only"]
        assert rejected["down"][0] == "unreachable when observed"
        assert "GPUs per node unknown" in rejected["down"]
        assert _ranked(result) == ["fast-busy", "idle-slow"]
        _accounts_for_every_target(result)
        assert result["request"]["gpus_per_node"] == 2

    def test_a_node_with_exactly_the_requested_gpus_qualifies(self) -> None:
        assert "fast-busy" in _ranked(_explain(_gpu_fleet(), gpus_per_node=4))
        assert _rejected(_explain(_gpu_fleet(), gpus_per_node=5))["fast-busy"] == [
            "needs 5 GPUs per node; a node has 4"]

    def test_a_daemon_fits_now_only_with_enough_free_gpus(self) -> None:
        one = _explain(_gpu_fleet(), gpus_per_node=1)
        idle = next(c for c in one["candidates"] if c["target"] == "idle-slow")
        assert idle["components"]["queue_wait"] == {
            "low": 0.0, "high": 0.0, "source": "fits the free capacity now"}
        assert one["selected"]["target"] == "idle-slow"
        two = _explain(_gpu_fleet(), gpus_per_node=2)
        idle = next(c for c in two["candidates"] if c["target"] == "idle-slow")
        assert idle["components"]["queue_wait"]["unknown"] is True
        assert two["selected"]["target"] == "fast-busy"  # complete predictions first

    def test_unknown_free_gpus_do_not_count_as_free(self) -> None:
        fleet = _gpu_fleet()
        del fleet["targets"][1]["free"]["gpus"]
        idle = next(c for c in _explain(fleet, gpus_per_node=1)["candidates"]
                    if c["target"] == "idle-slow")
        assert idle["components"]["queue_wait"]["unknown"] is True

    def test_a_request_without_gpus_is_planned_as_before(self) -> None:
        before = _explain()
        after = _explain(_gpu_fleet())
        assert before["request"]["gpus_per_node"] is None
        assert {k: v for k, v in after.items() if k != "request"} == {
            k: v for k, v in before.items() if k != "request"}

    @pytest.mark.parametrize("value", [0, -1, 1.5, "2"])
    def test_a_gpu_request_is_a_positive_integer(self, value: object) -> None:
        with pytest.raises(ValidationError):
            pp.Request.model_validate(_request(gpus_per_node=value))

    @pytest.mark.parametrize(("where", "value"), [("node", -1), ("free", -1), ("node", 1.5)])
    def test_recorded_gpus_are_non_negative_integers(self, where: str, value: object) -> None:
        fleet = _gpu_fleet()
        fleet["targets"][1][where]["gpus"] = value
        with pytest.raises(ValidationError):
            pp.Fleet.model_validate(fleet)


def test_cli_explains_a_gpu_hold_and_an_eligible_target(tmp_path: Path) -> None:
    fleet_file, request_file = tmp_path / "fleet.json", tmp_path / "request.json"
    fleet_file.write_text(json.dumps(_gpu_fleet()), encoding="utf-8")
    args = ["placement", "explain", str(fleet_file), str(request_file),
            "--now", NOW.isoformat()]
    runner = CliRunner()
    request_file.write_text(json.dumps(_request(gpus_per_node=8)), encoding="utf-8")
    held = runner.invoke(main, args)
    assert held.exit_code == 0, held.output
    assert "requested GPUs: 8 per node" in held.output and "HELD:" in held.output
    assert "fast-busy: needs 8 GPUs per node; a node has 4" in held.output
    request_file.write_text(json.dumps(_request(gpus_per_node=1)), encoding="utf-8")
    chosen = json.loads(runner.invoke(main, [*args, "--json"]).output)
    assert chosen["selected"]["target"] == "idle-slow"
