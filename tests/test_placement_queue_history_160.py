"""#160: scheduler-lane queue waits recorded by the driver, used in fleet files.

The driver daemon records when qsub/sbatch accepted a job
(`scheduler_submitted_at`) and when its poll first saw the cluster run it
(`scheduler_started_at`). `vq placement fleet --queue-history NAME[=TARGET]`
turns this host's recorded waits on that target into the lane's
`queue_wait_seconds` (nearest-rank 10th-90th percentile, source history).
The daemon side runs the real scheduler path with the mock dispatcher.
"""
from __future__ import annotations

import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vq import paths
from vq import placement_policy as pp
from vq.cli import main
from vq.daemon import Daemon
from vq.scheduler_dispatch import QstatDetail, SchedulerPhase
from vq.spec import JobSpec, JobState

sys.path.insert(0, str(Path(__file__).parent))
from test_daemon_scheduler import (  # noqa: E402
    MockDispatcher,
    _inject,
    _submit_scheduler,
    daemon,  # noqa: F401
)
from test_placement_policy_160 import _fleet  # noqa: E402

NOW = datetime(2026, 10, 21, 12, 0, tzinfo=UTC)


class TestRecording:
    def test_acceptance_and_first_running_poll_are_stamped_once(
        self, daemon: Daemon,  # noqa: F811
    ) -> None:
        mock = MockDispatcher(submit_id="1.cluster", phase=SchedulerPhase.PENDING,
                              detail=QstatDetail(raw_state="Q"))
        _inject(daemon, mock)
        daemon._start_scheduler_job(_submit_scheduler(daemon, "waits"))
        accepted = JobSpec.read(daemon._spec_path("waits"))
        assert accepted.scheduler_submitted_at is not None
        assert accepted.scheduler_started_at is None
        daemon._reconcile_scheduler()
        assert JobSpec.read(daemon._spec_path("waits")).scheduler_started_at is None
        mock.phase = SchedulerPhase.RUNNING
        mock.detail = QstatDetail(raw_state="R", exec_host="node01/0")
        daemon._reconcile_scheduler()
        started = JobSpec.read(daemon._spec_path("waits"))
        assert started.scheduler_started_at is not None
        assert started.scheduler_started_at >= started.scheduler_submitted_at
        # A later poll keeps the first observation.
        daemon._scheduler_running["waits"].last_status_write = 0.0
        daemon._reconcile_scheduler()
        assert JobSpec.read(daemon._spec_path("waits")).scheduler_started_at == (
            started.scheduler_started_at)

    def test_a_retry_clears_the_previous_attempts_times(
        self, daemon: Daemon,  # noqa: F811
    ) -> None:
        spec = _submit_scheduler(daemon, "retried")
        spec.retry_max = 1
        spec.scheduler_submitted_at = spec.scheduler_started_at = NOW.isoformat()
        spec.state = JobState.RUNNING
        spec.write(daemon._spec_path("retried"))
        assert daemon._maybe_retry(spec, 1, jobid="retried",
                                   spec_path=daemon._spec_path("retried"))
        retried = JobSpec.read(daemon._spec_path("retried"))
        assert (retried.state, retried.scheduler_submitted_at,
                retried.scheduler_started_at) == (JobState.PENDING, None, None)


def _job(jobid: str, wait: float, *, started_ago: float = 3600.0,
         target: str = "fast-busy") -> JobSpec:
    started = NOW - timedelta(seconds=started_ago)
    return JobSpec(id=jobid, command=["true"], cwd="/w", cpus=1, scheduler_target=target,
                   scheduler_submitted_at=(started - timedelta(seconds=wait)).isoformat(),
                   scheduler_started_at=started.isoformat())


def _history(specs: list[JobSpec], **kwargs: Any) -> tuple[dict[str, Any] | None, int]:
    options: dict[str, Any] = {"now": NOW, "days": 14.0, "max_jobs": 50, "min_jobs": 5}
    options.update(kwargs)
    return pp.queue_wait_history(specs, "fast-busy", **options)


class TestInterval:
    def test_the_10th_to_90th_percentile_of_recorded_waits(self) -> None:
        specs = [_job(f"j{i}", float(wait)) for i, wait in enumerate(range(10, 110, 10))]
        assert _history(specs) == ({"low": 10.0, "high": 90.0, "source": "history"}, 10)
        twenty = [_job(f"t{i}", float(wait)) for i, wait in enumerate(range(1, 21))]
        assert _history(twenty)[0] == {"low": 2.0, "high": 18.0, "source": "history"}
        five = [_job(f"k{i}", float(wait)) for i, wait in enumerate((5, 1, 9, 3, 7))]
        assert _history(five) == ({"low": 1.0, "high": 9.0, "source": "history"}, 5)

    def test_only_recent_complete_waits_on_this_target_count(self) -> None:
        good = [_job(f"g{i}", 100.0) for i in range(5)]
        noise = [
            _job("old", 1.0, started_ago=15 * 86400),
            _job("future", 1.0, started_ago=-3600),
            _job("other", 1.0, target="mem-rich"),
            _job("backwards", -10.0),
            JobSpec(id="unstarted", command=["true"], cwd="/w", cpus=1,
                    scheduler_target="fast-busy",
                    scheduler_submitted_at=NOW.isoformat()),
            JobSpec(id="unparsable", command=["true"], cwd="/w", cpus=1,
                    scheduler_target="fast-busy", scheduler_submitted_at="later",
                    scheduler_started_at=NOW.isoformat()),
        ]
        assert _history(good + noise) == (
            {"low": 100.0, "high": 100.0, "source": "history"}, 5)

    def test_the_most_recent_jobs_count_and_too_few_give_nothing(self) -> None:
        recent = [_job(f"r{i}", 10.0, started_ago=100 + i) for i in range(5)]
        older = [_job(f"o{i}", 1000.0, started_ago=10000 + i) for i in range(5)]
        assert _history(recent + older, max_jobs=5)[0] == {
            "low": 10.0, "high": 10.0, "source": "history"}
        assert _history(recent[:4]) == (None, 4)


def test_the_cli_sets_a_lanes_queue_wait_from_this_hosts_jobs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vq import config

    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    paths.queue_dir().mkdir(parents=True)
    now = datetime.now(UTC)
    for i, wait in enumerate((600, 1200, 1800, 2400, 3000)):
        started = now - timedelta(hours=1, seconds=i)
        JobSpec(id=f"hist{i}", command=["true"], cwd=str(tmp_path), cpus=1,
                scheduler_target="cluster-a", state=JobState.COMPLETED,
                scheduler_submitted_at=(started - timedelta(seconds=wait)).isoformat(),
                scheduler_started_at=started.isoformat(),
                ).write(paths.queue_dir() / f"hist{i}.json")
    base = tmp_path / "base.json"
    base.write_text(json.dumps(_fleet()))
    runner = CliRunner()
    result = runner.invoke(main, ["placement", "fleet", str(base),
                                  "--queue-history", "fast-busy=cluster-a",
                                  "--queue-history", "mem-rich"])
    assert result.exit_code == 0, result.output
    fleet = json.loads(result.stdout)
    lanes = {t["name"]: t for t in fleet["targets"]}
    assert lanes["fast-busy"]["queue_wait_seconds"] == {
        "low": 600.0, "high": 3000.0, "source": "history"}
    assert lanes["mem-rich"]["queue_wait_seconds"] == _fleet()["targets"][2][
        "queue_wait_seconds"]
    assert "fast-busy: queue wait 600-3000 s from 5 job(s)" in result.stderr
    assert "mem-rich: kept the base queue wait; 0 recorded wait(s), fewer than 5" in (
        result.stderr)
    for args, message in [
        (["--queue-history", "idle-slow"], "not a scheduler lane of the fleet"),
        (["--queue-history", "nowhere"], "not a scheduler lane of the fleet"),
        (["--queue-history", "fast-busy", "--queue-history", "fast-busy=x"],
         "--queue-history fast-busy is given twice"),
    ]:
        refused = runner.invoke(main, ["placement", "fleet", str(base), *args])
        assert refused.exit_code != 0 and message in refused.output, refused.output
