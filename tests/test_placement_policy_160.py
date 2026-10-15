"""#160: completion-time placement over a deterministic simulated fleet.

The fleet is the one the issue's first acceptance item describes: a
fast-but-busy cluster, an idle slower node, a memory-rich lane, an
unreachable and a stale target, and a lane that only has an incompatible
runtime variant. Every expected number below is worked out by hand.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from click.testing import CliRunner
from pydantic import ValidationError

from vq import placement_policy as pp
from vq.cli import main

NOW = datetime(2026, 10, 12, 12, 0, 0, tzinfo=UTC)
FRESH = (NOW - timedelta(seconds=60)).isoformat()
GENERIC = "a" * 64
AVX512 = "b" * 64
ALL_ISA = ["x86-64-v2", "x86-64-v3", "x86-64-v4"]


def _fleet() -> dict:
    return {"schema_name": "vq.placement-fleet/1", "targets": [
        {"name": "fast-busy", "kind": "scheduler", "observed_at": FRESH, "reachable": True,
         "arch": "x86_64", "isa_levels": ALL_ISA, "programs": {"vibe-qc": [GENERIC, AVX512]},
         "node": {"cpus": 128, "mem_mb": 512000}, "max_nodes": 16,
         "max_walltime_seconds": 172800,
         "queue_wait_seconds": {"low": 14400, "high": 28800, "source": "history"},
         "speed_factor": 2.0, "speed_source": "benchmark",
         "staging_mb_per_second": 200, "transfer_mb_per_second": 200},
        {"name": "idle-slow", "kind": "daemon", "observed_at": FRESH, "reachable": True,
         "arch": "x86_64", "isa_levels": ALL_ISA[:2], "programs": {"vibe-qc": [GENERIC]},
         "node": {"cpus": 32, "mem_mb": 128000}, "free": {"cpus": 32, "mem_mb": 120000},
         "speed_factor": 1.0, "speed_source": "declared",
         "staging_mb_per_second": 50, "transfer_mb_per_second": 50},
        {"name": "mem-rich", "kind": "scheduler", "observed_at": FRESH, "reachable": True,
         "arch": "x86_64", "isa_levels": ALL_ISA, "programs": {"vibe-qc": [GENERIC]},
         "node": {"cpus": 64, "mem_mb": 1000000}, "max_nodes": 2,
         "max_walltime_seconds": 172800,
         "queue_wait_seconds": {"low": 1800, "high": 3600, "source": "history"},
         "speed_factor": 1.2, "speed_source": "benchmark",
         "staging_mb_per_second": 100, "transfer_mb_per_second": 100},
        {"name": "avx-only", "kind": "scheduler", "observed_at": FRESH, "reachable": True,
         "arch": "x86_64", "isa_levels": ALL_ISA, "programs": {"vibe-qc": [AVX512]},
         "node": {"cpus": 64, "mem_mb": 256000}, "max_nodes": 4,
         "speed_factor": 3.0, "speed_source": "benchmark"},
        {"name": "down", "kind": "daemon", "observed_at": FRESH, "reachable": False,
         "programs": {"vibe-qc": [GENERIC]}, "node": {"cpus": 64, "mem_mb": 256000}},
        {"name": "stale", "kind": "daemon",
         "observed_at": (NOW - timedelta(hours=2)).isoformat(), "reachable": True,
         "programs": {"vibe-qc": [GENERIC]}, "node": {"cpus": 64, "mem_mb": 256000},
         "free": {"cpus": 64, "mem_mb": 256000}},
    ]}


def _request(**overrides: object) -> dict:
    request: dict = {
        "schema_name": "vq.placement-request/1", "program": "vibe-qc",
        "runtime_digest": GENERIC, "cpus": 16, "mem_mb_per_node": 64000,
        "runtime_seconds": {"low": 6000, "high": 8400},
        "input_mb": 100, "output_mb": 500,
    }
    request.update(overrides)
    return request


def _explain(fleet: dict | None = None, **request: object) -> dict:
    return pp.explain(pp.Fleet.model_validate(fleet or _fleet()),
                      pp.Request.model_validate(_request(**request)),
                      now=NOW, max_age_seconds=300)


def _ranked(result: dict) -> list[str]:
    return [c["target"] for c in result["candidates"]]


def _rejected(result: dict) -> dict[str, list[str]]:
    return {r["target"]: r["reasons"] for r in result["rejected"]}


def _accounts_for_every_target(result: dict) -> None:
    candidates, rejected = set(_ranked(result)), set(_rejected(result))
    assert candidates.isdisjoint(rejected)
    assert candidates | rejected == {t["name"] for t in _fleet()["targets"]}
    assert all(reasons for reasons in _rejected(result).values())


class TestSimulatedFleet:
    def test_a_short_job_goes_to_the_idle_slower_node(self) -> None:
        result = _explain()
        _accounts_for_every_target(result)
        # idle-slow: 100/50 + 0 + 6000..8400 + 500/50   = 6012..8412  (mid 7212)
        # mem-rich:  1 + 1800..3600 + 5000..7000 + 5     = 6806..10606 (mid 8706)
        # fast-busy: 0.5 + 14400..28800 + 3000..4200 + 2.5 = 17403..33003
        assert _ranked(result) == ["idle-slow", "mem-rich", "fast-busy"]
        assert result["selected"] == {
            "target": "idle-slow", "runtime_digest": GENERIC,
            "completion": {"low": 6012.0, "high": 8412.0, "complete": True},
        }
        idle = result["candidates"][0]["components"]
        assert idle["queue_wait"] == {"low": 0.0, "high": 0.0,
                                      "source": "fits the free capacity now"}
        assert idle["staging"]["source"] == "100 MB at 50 MB/s"
        assert result["candidates"][1]["completion"] == {
            "low": 6806.0, "high": 10606.0, "complete": True}
        assert result["candidates"][2]["components"]["execution"]["source"] == (
            "reference estimate / speed 2 (benchmark)")
        rejected = _rejected(result)
        assert rejected["down"] == ["unreachable when observed"]
        assert rejected["stale"] == ["observation is 7200 s old (limit 300 s)"]
        assert rejected["avx-only"] == [
            f"runtime {GENERIC[:12]} of vibe-qc is not available (has {AVX512[:12]})"]
        assert result["held"] is None

    def test_a_long_job_goes_to_the_fast_busy_cluster(self) -> None:
        result = _explain(runtime_seconds={"low": 144000, "high": 216000})
        # fast-busy: 0.5 + 14400..28800 + 72000..108000 + 2.5 -> mid 111603
        # idle-slow: 2 + 0 + 144000..216000 + 10                -> mid 180012
        # mem-rich: up to 216000 / 1.2 = 180000 s > its 172800 s walltime limit
        assert _ranked(result) == ["fast-busy", "idle-slow"]
        assert _rejected(result)["mem-rich"] == [
            "predicted execution up to 180000 s exceeds the 172800 s walltime limit"]

    def test_a_big_memory_job_only_fits_the_memory_rich_lane(self) -> None:
        result = _explain(cpus=32, mem_mb_per_node=700000)
        assert _ranked(result) == ["mem-rich"]
        rejected = _rejected(result)
        assert rejected["fast-busy"] == ["needs 700000 MB per node; a node has 512000"]
        assert rejected["idle-slow"] == ["needs 700000 MB per node; a node has 128000"]

    def test_an_explicit_runtime_variant_is_never_substituted(self) -> None:
        result = _explain(runtime_digest=AVX512, isa_level="x86-64-v4")
        # avx-only is faster, but its queue wait is unknown, so the complete
        # prediction ranks first and avx-only says what it is missing.
        assert _ranked(result) == ["fast-busy", "avx-only"]
        assert all(c["runtime_digest"] == AVX512 for c in result["candidates"])
        avx = result["candidates"][1]
        assert avx["unknown_components"] == ["staging", "queue_wait", "transfer"]
        assert avx["components"]["queue_wait"] == {
            "unknown": True, "reason": "no queue-wait model for this lane"}
        assert avx["completion"]["complete"] is False
        # The text never shows a partial sum as if it were a prediction.
        assert "0:33:20-0:46:40 + unknown" in pp.explanation_text(result)
        assert _rejected(result)["idle-slow"] == [
            f"runtime {AVX512[:12]} of vibe-qc is not available (has {GENERIC[:12]})",
            "ISA level x86-64-v4 is not supported",
        ]

    def test_without_a_pin_the_lanes_current_runtime_is_reported(self) -> None:
        result = _explain(runtime_digest=None)
        runtimes = {c["target"]: c["runtime_digest"] for c in result["candidates"]}
        assert runtimes["fast-busy"] == GENERIC and runtimes["avx-only"] == AVX512

    def test_no_eligible_target_holds_with_every_reason(self) -> None:
        result = _explain(program="orca")
        _accounts_for_every_target(result)
        assert result["selected"] is None and result["candidates"] == []
        assert result["held"] == "no eligible target; see rejected"
        assert all("program orca is not available" in reasons
                   for reasons in _rejected(result).values())
        assert _rejected(result)["down"] == [
            "unreachable when observed", "program orca is not available"]

    def test_a_busy_daemon_without_a_wait_model_is_not_assumed_free(self) -> None:
        fleet = _fleet()
        fleet["targets"][1]["free"] = {"cpus": 4, "mem_mb": 120000}
        result = _explain(fleet)
        idle = next(c for c in result["candidates"] if c["target"] == "idle-slow")
        assert idle["components"]["queue_wait"] == {
            "unknown": True,
            "reason": "does not fit the free capacity now and no wait model is recorded",
        }
        assert _ranked(result)[-1] == "idle-slow"

    def test_multi_node_jobs_respect_node_shape_and_limits(self) -> None:
        result = _explain(cpus=256, nodes=4, mem_mb_per_node=100000)
        assert _ranked(result) == ["fast-busy"]
        rejected = _rejected(result)
        assert rejected["idle-slow"] == ["needs 4 nodes; at most 1 per job",
                                         "needs 64 CPUs per node; a node has 32"]
        assert rejected["mem-rich"] == ["needs 4 nodes; at most 2 per job"]
        # Four nodes of 64 CPUs fit avx-only exactly; only its runtime is wrong.
        assert rejected["avx-only"] == [
            f"runtime {GENERIC[:12]} of vibe-qc is not available (has {AVX512[:12]})"]

    def test_unknown_capability_and_clock_skew_are_rejections(self) -> None:
        fleet = _fleet()
        fleet["targets"][1]["node"] = {"cpus": 32}
        fleet["targets"][2]["observed_at"] = (NOW + timedelta(minutes=10)).isoformat()
        rejected = _rejected(_explain(fleet))
        assert rejected["idle-slow"] == ["memory per node unknown"]
        assert rejected["mem-rich"] == ["observation is 600 s in the future: clock skew"]

    def test_accounts_and_architecture_are_hard_requirements(self) -> None:
        fleet = _fleet()
        fleet["targets"][0]["accounts"] = ["physics"]
        fleet["targets"][2]["arch"] = "aarch64"
        rejected = _rejected(_explain(fleet, account="chem", arch="x86_64"))
        assert rejected["fast-busy"] == ["account chem is not permitted"]
        assert rejected["mem-rich"] == ["architecture aarch64, not x86_64"]

    def test_requested_walltime_is_not_replaced_by_a_faster_estimate(self) -> None:
        fleet = _fleet()
        fleet["targets"][0]["max_walltime_seconds"] = 3600
        fleet["targets"][2]["max_walltime_seconds"] = 7200
        fleet["targets"][1]["walltime_unlimited"] = True
        result = _explain(fleet, wall_time_seconds=7200,
                          runtime_seconds={"low": 60, "high": 120})
        # The fast lane predicts 30..60 s execution, but cannot accept the
        # requested two-hour reservation. The equal-cap and unlimited targets fit.
        assert _ranked(result) == ["idle-slow", "mem-rich"]
        assert _rejected(result)["fast-busy"] == [
            "requested walltime 7200 s exceeds the 3600 s walltime limit"]
        assert _rejected(result)["avx-only"] == [
            f"runtime {GENERIC[:12]} of vibe-qc is not available (has {AVX512[:12]})",
            "walltime limit unknown for an explicit walltime request",
        ]
        assert result["request"]["wall_time_seconds"] == 7200
        assert "requested walltime: 2:00:00" in pp.explanation_text(result)

    def test_requested_walltime_is_checked_without_an_execution_estimate(self) -> None:
        result = _explain(wall_time_seconds=172801, runtime_seconds=None)
        assert result["selected"] is None
        assert _rejected(result)["fast-busy"] == [
            "requested walltime 172801 s exceeds the 172800 s walltime limit"]
        assert _rejected(result)["idle-slow"] == [
            "walltime limit unknown for an explicit walltime request"]

    def test_requested_walltime_does_not_include_queue_or_transfer_time(self) -> None:
        # Queueing for hours does not consume the target's execution walltime.
        fleet = _fleet()
        fleet["targets"][0]["max_walltime_seconds"] = 7200
        fleet["targets"][2]["max_walltime_seconds"] = 7200
        result = _explain(fleet, wall_time_seconds=7200)
        assert _ranked(result) == ["mem-rich", "fast-busy"]
        assert result["candidates"][1]["components"]["queue_wait"]["high"] == 28800
        assert all(c["completion"]["high"] > 7200 for c in result["candidates"])

    def test_walltime_rejections_keep_the_other_fit_failures(self) -> None:
        result = _explain(wall_time_seconds=172801, mem_mb_per_node=700000,
                          runtime_seconds={"low": 400000, "high": 500000})
        assert _rejected(result)["fast-busy"] == [
            "needs 700000 MB per node; a node has 512000",
            "requested walltime 172801 s exceeds the 172800 s walltime limit",
            "predicted execution up to 250000 s exceeds the 172800 s walltime limit",
        ]

    def test_without_a_runtime_estimate_execution_is_unknown_everywhere(self) -> None:
        result = _explain(runtime_seconds=None)
        assert all(c["unknown_components"] == ["execution"] for c in result["candidates"])
        # Ranked on what is known: idle-slow waits 0, mem-rich 1800..3600.
        assert _ranked(result)[:2] == ["idle-slow", "mem-rich"]

    def test_the_explanation_is_deterministic_and_order_independent(self) -> None:
        reversed_fleet = _fleet()
        reversed_fleet["targets"].reverse()
        first = pp.explanation_json(_explain())
        assert first == pp.explanation_json(_explain())
        assert first == pp.explanation_json(_explain(reversed_fleet))
        assert pp.explanation_text(_explain()) == pp.explanation_text(_explain(reversed_fleet))


class TestScratchRequirements:
    def test_scratch_filters_before_ranking_and_keeps_every_rejection(self) -> None:
        fleet = _fleet()
        fleet["targets"][0]["node"]["scratch_mb"] = 64000
        fleet["targets"][1]["node"]["scratch_mb"] = 0
        fleet["targets"][2]["node"]["scratch_mb"] = 256000
        result = _explain(fleet, scratch_mb_per_node=128000)
        _accounts_for_every_target(result)
        assert _ranked(result) == ["mem-rich"]
        rejected = _rejected(result)
        assert rejected["fast-busy"] == [
            "needs 128000 MB scratch per node; a node has 64000"]
        assert rejected["idle-slow"] == [
            "needs 128000 MB scratch per node; a node has 0"]
        assert rejected["down"] == ["unreachable when observed", "scratch per node unknown"]
        assert rejected["stale"] == [
            "observation is 7200 s old (limit 300 s)", "scratch per node unknown"]
        assert rejected["avx-only"] == [
            f"runtime {GENERIC[:12]} of vibe-qc is not available (has {AVX512[:12]})",
            "scratch per node unknown",
        ]

    def test_unknown_scratch_holds_even_with_free_now_evidence(self) -> None:
        fleet = _fleet()
        fleet["targets"][1]["free"]["scratch_mb"] = 64000
        result = _explain(fleet, scratch_mb_per_node=64000)
        assert result["selected"] is None and result["candidates"] == []
        assert result["held"] == "no eligible target; see rejected"
        assert all("scratch per node unknown" in reasons
                   for reasons in _rejected(result).values())

    def test_multiple_nodes_do_not_pool_scratch_capacity(self) -> None:
        fleet = _fleet()
        fleet["targets"][0]["node"]["scratch_mb"] = 40000
        fleet["targets"][2]["node"]["scratch_mb"] = 30000
        result = _explain(fleet, cpus=64, nodes=2, scratch_mb_per_node=40000)
        # Each fast-busy node fits exactly. Two mem-rich nodes offer more
        # scratch in total, but neither meets the per-node requirement.
        assert _ranked(result) == ["fast-busy"]
        assert _rejected(result)["mem-rich"] == [
            "needs 40000 MB scratch per node; a node has 30000"]

    @pytest.mark.parametrize("free_scratch", [None, 0, 39999, 40000])
    def test_daemon_starts_now_only_when_free_scratch_fits(self, free_scratch: int | None) -> None:
        fleet = _fleet()
        for target in fleet["targets"]:
            target["node"]["scratch_mb"] = 40000
        fleet["targets"][1]["free"]["scratch_mb"] = free_scratch
        result = _explain(fleet, scratch_mb_per_node=40000)
        idle = next(c for c in result["candidates"] if c["target"] == "idle-slow")
        if free_scratch == 40000:
            assert idle["components"]["queue_wait"] == {
                "low": 0.0, "high": 0.0, "source": "fits the free capacity now"}
            assert result["selected"]["target"] == "idle-slow"
        else:
            assert idle["components"]["queue_wait"] == {
                "unknown": True,
                "reason": "does not fit the free capacity now and no wait model is recorded",
            }
            assert result["selected"]["target"] == "mem-rich"

    def test_busy_scratch_uses_a_declared_wait_model(self) -> None:
        fleet = _fleet()
        host = fleet["targets"][1]
        fleet["targets"] = [host]
        host["node"]["scratch_mb"] = 40000
        host["free"]["scratch_mb"] = 0
        host["queue_wait_seconds"] = {"low": 30, "high": 60, "source": "declared"}
        result = _explain(fleet, scratch_mb_per_node=40000)
        assert result["candidates"][0]["components"]["queue_wait"] == {
            "low": 30.0, "high": 60.0, "source": "declared"}

    def test_omitting_scratch_preserves_existing_placement(self) -> None:
        fleet = _fleet()
        for target in fleet["targets"]:
            target["node"]["scratch_mb"] = 0
            if "free" in target:
                target["free"]["scratch_mb"] = 0
        assert _explain(fleet) == _explain()


class TestValidation:
    @pytest.mark.parametrize(("mutate", "message"), [
        (lambda f: f["targets"][1].update(max_nodes=2), "a daemon host is one node"),
        (lambda f: f["targets"][0].update(free={"cpus": 1}), "has no free-now capacity"),
        (lambda f: f["targets"][0].update(observed_at="2026-10-12T12:00:00"),
         "needs a zone offset"),
        (lambda f: f["targets"].append(dict(f["targets"][0])), "each target name may appear once"),
        (lambda f: f["targets"][0].update(
            queue_wait_seconds={"low": 10, "high": 5, "source": "history"}),
         "high must not be below its low"),
        (lambda f: f["targets"][0].update(speed_factor=0), "greater than 0"),
        (lambda f: f["targets"][0].update(cluster_ram_mb=1), "Extra inputs are not permitted"),
        (lambda f: f["targets"][0].update(walltime_unlimited=True),
         "walltime cannot be both limited and unlimited"),
    ])
    def test_malformed_fleets_are_refused(self, mutate, message: str) -> None:
        fleet = _fleet()
        mutate(fleet)
        with pytest.raises(ValidationError, match=message):
            pp.Fleet.model_validate(fleet)

    def test_a_request_needs_a_cpu_per_node(self) -> None:
        with pytest.raises(ValidationError, match="at least one per node"):
            pp.Request.model_validate(_request(cpus=2, nodes=4))

    @pytest.mark.parametrize("field", ["wall_time_seconds", "scratch_mb_per_node"])
    @pytest.mark.parametrize("value", [0, -1, 1.5, True])
    def test_resource_limits_are_positive_integers(self, field: str, value: object) -> None:
        with pytest.raises(ValidationError):
            pp.Request.model_validate(_request(**{field: value}))

    @pytest.mark.parametrize("field", ["node", "free"])
    def test_scratch_capacities_cannot_be_negative(self, field: str) -> None:
        fleet = _fleet()
        fleet["targets"][1][field]["scratch_mb"] = -1
        with pytest.raises(ValidationError):
            pp.Fleet.model_validate(fleet)

    def test_now_and_max_age_are_checked(self) -> None:
        fleet, request = pp.Fleet.model_validate(_fleet()), pp.Request.model_validate(_request())
        with pytest.raises(pp.PlacementError, match="zone offset"):
            pp.explain(fleet, request, now=NOW.replace(tzinfo=None), max_age_seconds=300)
        with pytest.raises(pp.PlacementError, match="finite and positive"):
            pp.explain(fleet, request, now=NOW, max_age_seconds=0)


def test_cli_explains_from_files(tmp_path: Path) -> None:
    fleet_file, request_file = tmp_path / "fleet.json", tmp_path / "request.json"
    fleet_file.write_text(json.dumps(_fleet()), encoding="utf-8")
    request_file.write_text(json.dumps(_request()), encoding="utf-8")
    runner = CliRunner()
    args = ["placement", "explain", str(fleet_file), str(request_file),
            "--now", NOW.isoformat()]

    as_json = runner.invoke(main, [*args, "--json"])
    assert as_json.exit_code == 0, as_json.output
    assert json.loads(as_json.output)["selected"]["target"] == "idle-slow"

    text = runner.invoke(main, args)
    assert text.exit_code == 0, text.output
    assert "selected idle-slow" in text.output
    assert "stale: observation is 7200 s old (limit 300 s)" in text.output

    # A longer max age readmits the stale target; it is idle and free too.
    relaxed = runner.invoke(main, [*args, "--json", "--max-age", "86400"])
    assert "stale" in [c["target"] for c in json.loads(relaxed.output)["candidates"]]

    naive = runner.invoke(main, [*args[:-1], "2026-10-12T12:00:00"])
    assert naive.exit_code == 2 and "zone offset" in naive.output

    request_file.write_text(json.dumps(_request(cpus=0)), encoding="utf-8")
    refused = runner.invoke(main, args)
    assert refused.exit_code == 1 and "greater than or equal to 1" in refused.output


def test_cli_preserves_requested_walltime_and_explains_a_hold(tmp_path: Path) -> None:
    fleet_file, request_file = tmp_path / "fleet.json", tmp_path / "request.json"
    fleet_file.write_text(json.dumps(_fleet()), encoding="utf-8")
    request_file.write_text(json.dumps(_request(wall_time_seconds=172801)), encoding="utf-8")
    args = ["placement", "explain", str(fleet_file), str(request_file),
            "--now", NOW.isoformat()]
    runner = CliRunner()
    as_json = runner.invoke(main, [*args, "--json"])
    assert as_json.exit_code == 0, as_json.output
    result = json.loads(as_json.output)
    assert result["request"]["wall_time_seconds"] == 172801
    assert result["selected"] is None and result["candidates"] == []
    assert "requested walltime 172801 s exceeds" in str(result["rejected"])

    text = runner.invoke(main, args)
    assert text.exit_code == 0, text.output
    assert "requested walltime: 48:00:01" in text.output
    assert "HELD: no eligible target" in text.output


def test_cli_preserves_scratch_and_explains_fit_or_hold(tmp_path: Path) -> None:
    fleet = _fleet()
    fleet["targets"][1]["node"]["scratch_mb"] = 40000
    fleet["targets"][1]["free"]["scratch_mb"] = 40000
    fleet_file, request_file = tmp_path / "fleet.json", tmp_path / "request.json"
    fleet_file.write_text(json.dumps(fleet), encoding="utf-8")
    request_file.write_text(json.dumps(_request(scratch_mb_per_node=40000)), encoding="utf-8")
    args = ["placement", "explain", str(fleet_file), str(request_file),
            "--now", NOW.isoformat()]
    runner = CliRunner()
    as_json = runner.invoke(main, [*args, "--json"])
    assert as_json.exit_code == 0, as_json.output
    result = json.loads(as_json.output)
    assert result["request"]["scratch_mb_per_node"] == 40000
    assert result["selected"]["target"] == "idle-slow"

    text = runner.invoke(main, args)
    assert text.exit_code == 0, text.output
    assert "requested scratch: 40000 MB per node" in text.output
    assert "selected idle-slow" in text.output

    request_file.write_text(json.dumps(_request(scratch_mb_per_node=40001)), encoding="utf-8")
    held = runner.invoke(main, [*args, "--json"])
    assert held.exit_code == 0, held.output
    result = json.loads(held.output)
    assert result["request"]["scratch_mb_per_node"] == 40001
    assert result["selected"] is None and result["candidates"] == []
    assert _rejected(result)["idle-slow"] == [
        "needs 40001 MB scratch per node; a node has 40000"]
