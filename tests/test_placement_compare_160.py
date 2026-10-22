"""#160: `vq placement compare`, the broker's two orderings on recorded jobs.

Each of this host's recent finished local jobs is placed twice over capacity
snapshots built from a fleet file's daemon targets: by B2 as the broker
calls it (`capacity`), and by the broker's own completion-time re-ranker on
that decision. The report says where each would place it and how often they
agree. Nothing is placed and no host is contacted.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vq import config, paths
from vq import placement_policy as pp
from vq.cli import main
from vq.placement_compare import compare, snapshots_from_fleet
from vq.spec import JobSpec, JobState

NOW = datetime(2026, 10, 22, 12, 0, tzinfo=UTC)
FRESH = (NOW - timedelta(seconds=60)).isoformat()


def _daemon(name: str, *, speed: float, cpus: int, mem_mb: int, **extra: Any) -> dict[str, Any]:
    return {"name": name, "kind": "daemon", "observed_at": FRESH, "reachable": True,
            "programs": {"vibe-qc": ["a" * 64]}, "node": {"cpus": 64, "mem_mb": 256000},
            "free": {"cpus": cpus, "mem_mb": mem_mb}, "walltime_unlimited": True,
            "speed_factor": speed, "speed_source": "benchmark", **extra}


def _fleet(*extra: dict[str, Any]) -> pp.Fleet:
    return pp.Fleet.model_validate({"schema_name": "vq.placement-fleet/1", "targets": [
        _daemon("alpha", speed=1.0, cpus=32, mem_mb=128000),
        _daemon("beta", speed=2.0, cpus=8, mem_mb=16000), *extra]})


def _job(jobid: str, *, cpus: int = 4, mem_mb: int | None = 8000, minutes_ago: int = 10,
         state: JobState = JobState.COMPLETED, **extra: Any) -> JobSpec:
    return JobSpec(id=jobid, command=["python3", "run.py"], cwd="/w", cpus=cpus,
                   mem_mb=mem_mb, state=state,
                   submitted_at=(NOW - timedelta(minutes=minutes_ago)).isoformat(), **extra)


def _compare(specs: list[JobSpec], fleet: pp.Fleet | None = None, **kwargs: Any) -> Any:
    options: dict[str, Any] = {"now": NOW, "max_age_seconds": 300.0, "limit": 50}
    options.update(kwargs)
    return compare(fleet or _fleet(), specs, **options)


def _rows(report: dict[str, Any]) -> dict[str, tuple[str | None, str | None]]:
    return {row["job_id"]: (row["capacity"], row["completion_time"]) for row in report["jobs"]}


class TestCompare:
    def test_where_each_ordering_places_each_job(self) -> None:
        report = _compare([
            _job("small"),                       # both fit: roomier alpha vs faster beta
            _job("wide", cpus=16, mem_mb=8000),  # only alpha has 16 CPUs free
            _job("huge", cpus=64, mem_mb=8000),  # nobody has 64 free
        ])
        assert _rows(report) == {"small": ("alpha", "beta"), "wide": ("alpha", "alpha"),
                                 "huge": (None, None)}
        assert report["totals"] == {"compared": 3, "same": 2, "different": 1,
                                    "held_by_capacity": 1, "held_by_completion_time": 1,
                                    "skipped": {}}
        wide = next(row for row in report["jobs"] if row["job_id"] == "wide")
        assert wide["capacity_blockers"] == {"beta": "cpus-insufficient"}
        assert report["schema"] == "vq.placement-compare/1"

    def test_only_recent_finished_local_jobs_with_memory_count(self) -> None:
        report = _compare([
            _job("kept"), _job("running", state=JobState.RUNNING),
            _job("cluster", scheduler_target="cluster-a", minutes_ago=1),
            _job("no-memory", mem_mb=None), _job("old", minutes_ago=999),
        ], limit=2)
        assert [row["job_id"] for row in report["jobs"]] == ["kept"]
        assert report["totals"]["skipped"] == {"no declared mem_mb": 1}

    def test_a_planner_refusal_shows_as_a_completion_time_blocker(self) -> None:
        fleet = _fleet()
        fleet.targets[1].programs = {}
        (row,) = _compare([_job("named", program="vibe-qc")], fleet)["jobs"]
        assert (row["capacity"], row["completion_time"], row["same"]) == ("alpha", "alpha", True)
        assert row["completion_time_blockers"] == {
            "beta": "completion-time: program vibe-qc is not available"}

    def test_a_fleet_without_daemons_cannot_be_compared(self) -> None:
        only_lane = pp.Fleet.model_validate({"schema_name": "vq.placement-fleet/1", "targets": [
            {"name": "lane", "kind": "scheduler", "observed_at": FRESH, "reachable": True,
             "programs": {}, "node": {"cpus": 64}}]})
        with pytest.raises(pp.PlacementError, match="no daemon target"):
            _compare([_job("j")], only_lane)


class TestSnapshots:
    @pytest.mark.parametrize(("change", "reason"), [
        ({"reachable": False}, "unreachable"),
        ({"maintenance": True}, "admission-blocked"),
        ({"free": None}, "capacity-unknown"),
    ])
    def test_a_daemon_record_becomes_the_evidence_it_states(
        self, change: dict[str, Any], reason: str,
    ) -> None:
        fleet = _fleet(_daemon("gamma", speed=3.0, cpus=64, mem_mb=256000, **change))
        (row,) = _compare([_job("j")], fleet)["jobs"]
        assert row["capacity_blockers"] == {"gamma": reason}

    def test_a_walltime_limit_is_carried(self) -> None:
        fleet = _fleet(_daemon("short", speed=3.0, cpus=64, mem_mb=256000,
                               walltime_unlimited=False, max_walltime_seconds=600))
        (row,) = _compare([_job("j", wall_time_seconds=3600)], fleet)["jobs"]
        assert row["capacity_blockers"] == {"short": "wall-time-exceeded"}
        snapshot = next(s for s in snapshots_from_fleet(fleet) if s.host == "short")
        assert snapshot.max_wall_time_seconds == 600
        assert snapshot.observed_at == (NOW - timedelta(seconds=60)).timestamp()


def test_the_cli_compares_this_hosts_jobs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                          ) -> None:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    paths.queue_dir().mkdir(parents=True)
    _job("small").write(paths.queue_dir() / "small.json")
    _job("wide", cpus=16).write(paths.queue_dir() / "wide.json")
    fleet = tmp_path / "fleet.json"
    fleet.write_text(_fleet().model_dump_json())
    args = ["placement", "compare", str(fleet), "--now", NOW.isoformat()]
    text = CliRunner().invoke(main, args)
    assert text.exit_code == 0, text.output
    assert "small" in text.output and "differs" in text.output
    assert "2 compared: 1 same, 1 different" in text.output
    as_json = json.loads(CliRunner().invoke(main, [*args, "--json"]).output)
    assert _rows(as_json) == {"small": ("alpha", "beta"), "wide": ("alpha", "alpha")}
