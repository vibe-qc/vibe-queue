"""#160: the opt-in completion-time broker policy re-ranks what B2 admits.

With `[broker] policy = "completion-time"` the broker still admits only the
hosts the capacity gates admit (open admission, ready queue, room now after
outstanding charges). It then orders them by the placement planner over
`[broker] placement_fleet` with this pass's evidence laid over it, and
refuses those the planner rejects. The default `capacity` policy is
unchanged. Uses the placer fixtures of the broker's own tests.
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from tests.test_broker_placement_b4a_64 import Target
from tests.test_broker_store_64 import _archive, _intent, _snapshot
from vq import config
from vq.broker_completion import completion_time_reranker
from vq.broker_placement import BrokerPlacer
from vq.broker_store import BrokerStore

GENERIC = "a" * 64


def _record(name: str, *, speed: float = 1.0, kind: str = "daemon",
            **extra: Any) -> dict[str, Any]:
    target: dict[str, Any] = {
        "name": name, "kind": kind, "observed_at": "2026-01-01T00:00:00+00:00",
        "reachable": False, "programs": {"vibe-qc": [GENERIC]},
        "node": {"cpus": 64, "mem_mb": 256000}, "walltime_unlimited": True,
        "speed_factor": speed, "speed_source": "benchmark", **extra}
    if kind == "scheduler":
        target.update(max_nodes=4, queue_wait_seconds={"low": 0, "high": 0,
                                                       "source": "declared"})
    return target


def _fleet_file(tmp_path: Path, *targets: dict[str, Any]) -> Path:
    path = tmp_path / "placement-fleet.json"
    path.write_text(json.dumps({"schema_name": "vq.placement-fleet/1",
                                "targets": list(targets)}))
    return path


@pytest.fixture
def world(tmp_path: Path) -> Any:
    payload = _archive(tmp_path / "payload.tar")
    target = Target()
    clock = iter(range(106, 10_000))
    fleet = _fleet_file(tmp_path, _record("alpha", speed=1.0), _record("beta", speed=2.0))
    with BrokerStore.open(tmp_path / "broker") as store:
        placer = BrokerPlacer(
            store, submit=target.submit, state_root_of=target.state_root_of,
            clock=lambda: float(next(clock)), rerank=completion_time_reranker(fleet),
        )
        yield SimpleNamespace(store=store, placer=placer, target=target, payload=payload,
                              fleet=fleet, tmp_path=tmp_path)


def _place(world: Any, snapshots: list[Any] | None = None, **intent: Any) -> Any:
    record = world.store.accept(world.payload, _intent(**intent), now=100.0)
    return world.placer.place(record.broker_job_id, snapshots or [
        _snapshot("alpha", free_cpus=32, free_mem_mb=128000), _snapshot("beta")],
        max_age_seconds=60.0)


class TestRanking:
    def test_the_faster_admitted_host_is_chosen_over_the_roomier(self, world: Any) -> None:
        # The capacity ranking prefers alpha, which has more free memory and CPUs.
        outcome = _place(world, program="vibe-qc")
        assert outcome.outcome == "placed" and outcome.host == "beta"
        assert world.target.calls[0].target_host == "beta"

    def test_a_plain_command_runs_anywhere_the_fleet_lists(self, world: Any) -> None:
        assert _place(world).host == "beta"  # intent.program is None

    def test_it_never_admits_a_host_the_capacity_gates_refuse(self, world: Any) -> None:
        outcome = _place(world, [_snapshot("alpha"),
                                 _snapshot("beta", admission_open=False)])
        assert outcome.host == "alpha"
        blockers = dict(world.store.get(outcome.broker_job_id).current_attempt.blockers)
        assert blockers == {"beta": "admission-blocked"}

    def test_charges_count_once_across_held_jobs(self, world: Any) -> None:
        snapshots = [_snapshot("alpha", free_cpus=32, free_mem_mb=128000),
                     _snapshot("beta", free_cpus=4)]
        assert _place(world, snapshots).host == "beta"
        # beta's four free CPUs are promised to the first job.
        second = _place(world, snapshots)
        assert second.host == "alpha"
        blockers = dict(world.store.get(second.broker_job_id).current_attempt.blockers)
        assert blockers == {"beta": "cpus-insufficient"}


class TestRefusals:
    def test_a_host_missing_from_the_fleet_or_rejected_is_a_blocker(
        self, world: Any,
    ) -> None:
        _fleet_file(world.tmp_path, _record("alpha"),
                    _record("beta", speed=2.0, node={"cpus": 2, "mem_mb": 256000}))
        outcome = _place(world, [_snapshot("alpha"), _snapshot("beta"),
                                 _snapshot("gamma")])
        assert outcome.host == "alpha"
        blockers = dict(world.store.get(outcome.broker_job_id).current_attempt.blockers)
        assert blockers == {
            "beta": "completion-time: needs 4 CPUs per node; a node has 2",
            "gamma": "completion-time: not a daemon target of the placement fleet"}

    def test_with_none_left_the_job_is_held_with_every_reason(self, world: Any) -> None:
        _fleet_file(world.tmp_path, _record("alpha", programs={}))
        outcome = _place(world, program="vibe-qc")
        assert outcome.outcome == "held" and world.target.calls == []
        assert dict(outcome.blockers) == {
            "alpha": "completion-time: program vibe-qc is not available",
            "beta": "completion-time: not a daemon target of the placement fleet"}
        stored = world.store.get(outcome.broker_job_id)
        assert stored.state == "held" and dict(stored.held_blockers) == dict(outcome.blockers)

    def test_an_unreadable_fleet_holds_rather_than_falling_back(self, world: Any) -> None:
        world.fleet.write_text("{not json")
        outcome = _place(world)
        assert outcome.outcome == "held" and world.target.calls == []
        assert all(reason.startswith("completion-time: placement fleet unusable: ")
                   for _host, reason in outcome.blockers)
        assert {host for host, _ in outcome.blockers} == {"alpha", "beta"}

    def test_a_scheduler_record_for_an_admitted_name_is_not_used(self, world: Any) -> None:
        _fleet_file(world.tmp_path, _record("alpha"), _record("beta", kind="scheduler"))
        outcome = _place(world)
        assert outcome.host == "alpha"

    def test_a_walltime_the_fleet_cannot_confirm_is_refused(self, world: Any) -> None:
        _fleet_file(world.tmp_path, _record("alpha"),
                    {**_record("beta", speed=2.0), "walltime_unlimited": False})
        outcome = _place(world, [_snapshot("alpha", max_wall_time_seconds=7200),
                                 _snapshot("beta", max_wall_time_seconds=7200)],
                         wall_time_seconds=3600)
        assert outcome.host == "alpha"


def test_without_the_policy_the_capacity_ranking_is_unchanged(tmp_path: Path) -> None:
    payload = _archive(tmp_path / "payload.tar")
    target = Target()
    with BrokerStore.open(tmp_path / "broker") as store:
        placer = BrokerPlacer(store, submit=target.submit,
                              state_root_of=target.state_root_of, clock=lambda: 106.0)
        record = store.accept(payload, _intent(), now=100.0)
        outcome = placer.place(record.broker_job_id, [
            _snapshot("alpha", free_cpus=32, free_mem_mb=128000), _snapshot("beta")],
            max_age_seconds=60.0)
    assert outcome.host == "alpha"


class TestConfig:
    def test_the_policy_needs_an_absolute_fleet(self) -> None:
        with pytest.raises(ValueError, match="needs an absolute placement_fleet"):
            config.BrokerConfig(policy="completion-time")
        with pytest.raises(ValueError, match="needs an absolute placement_fleet"):
            config.BrokerConfig(policy="completion-time", placement_fleet="fleet.json")
        assert config.BrokerConfig().policy == "capacity"
        assert config.BrokerConfig(policy="completion-time",
                                   placement_fleet="/etc/vq/fleet.json").placement_fleet

    def test_the_service_wires_the_policy_only_when_set(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from vq import broker_placement, broker_service

        seen: list[Any] = []
        real = broker_placement.BrokerPlacer

        def placer(*args: Any, **kwargs: Any) -> Any:
            seen.append(kwargs.get("rerank"))
            raise RuntimeError("stop after construction")

        monkeypatch.setattr(broker_placement, "BrokerPlacer", placer)
        for policy, expected in (("capacity", False), ("completion-time", True)):
            seen.clear()
            cfg = config.Config(
                hosts={"alpha": config.HostConfig(ssh="alpha")},
                broker=config.BrokerConfig(
                    enabled=True, targets=["alpha"], policy=policy,
                    placement_fleet=str(tmp_path / "fleet.json")))
            with pytest.raises(RuntimeError, match="stop after construction"):
                broker_service.run_pass(cfg, root=tmp_path / f"store-{policy}",
                                        collect=lambda *a, **k: {})
            assert (seen[0] is not None) is expected
        assert real is not placer
