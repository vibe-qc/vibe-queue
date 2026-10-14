"""#168: live usage of running Torque/PBS jobs, as the scheduler reports it.

The `qstat -f` detail the poll already fetches carries `resources_used.mem`
and `resources_used.cput`; the daemon keeps them on the job beside the
walltime, and `vq status` and the fleet console show them labelled as the
scheduler's own reading, with CPU as an average over the run so far. They
never enter the fresh-sample totals. Slurm reports neither.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from vq.scheduler_dialect import SlurmDialect, TorqueDialect, torque_memory_mb
from vq.spec import JobSpec, JobState
from vq.status import show_status
from vq.web import calculations
from vq.web import fleet as fleet_mod

QSTAT = """Job Id: 4242.pbs-server
    Job_Name = h2o.calc
    job_state = R
    exec_host = node07/0-7
    resources_used.cput = 01:30:00
    resources_used.mem = 5242880kb
    resources_used.vmem = 9000000kb
    resources_used.walltime = 00:30:00
    Resource_List.walltime = 08:00:00
"""


def test_the_torque_detail_carries_memory_and_cpu_time() -> None:
    detail = TorqueDialect().parse_qstat_detail(QSTAT)["4242.pbs-server"]
    assert (detail.mem_used, detail.cpu_used, detail.walltime_used) == (
        "5242880kb", "01:30:00", "00:30:00")


@pytest.mark.parametrize(("value", "mb"), [
    ("5242880kb", 5120.0), ("2gb", 2048.0), ("512mb", 512.0), ("1048576", 1.0),
    ("1048576b", 1.0), ("1tb", 1024.0**2), ("10w", None), ("lots", None), (None, None),
])
def test_torque_memory_values(value: str | None, mb: float | None) -> None:
    assert torque_memory_mb(value) == mb


def test_slurm_reports_neither() -> None:
    # sacct detail rows: JobID|State|ExitCode|Elapsed|Timelimit|NodeList
    (detail,) = SlurmDialect().parse_qstat_detail(
        "4243|RUNNING|0:0|00:10:00|01:00:00|node01").values()
    assert (detail.walltime_used, detail.exec_host) == ("00:10:00", "node01")
    assert (detail.mem_used, detail.cpu_used) == (None, None)


def _spec(tmp_path: Path, **fields: object) -> Path:
    queue = tmp_path / "queue"
    queue.mkdir()
    workspace = tmp_path / "ws"
    workspace.mkdir()
    (workspace / "stdout.log").write_text("")
    (workspace / "stderr.log").write_text("")
    JobSpec(id="abc", command=["run"], cwd=str(workspace), cpus=8, state=JobState.RUNNING,
            scheduler_target="pbs_a", scheduler_job_id="4242.pbs-server",
            scheduler_state="running", started_at="2026-10-14T10:00:00+00:00",
            **fields).write(queue / "abc.json")
    return queue


def test_status_shows_the_schedulers_reading(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import vq.status as status_mod

    monkeypatch.setattr(status_mod.rpc, "request_scheduler_status_refresh",
                        lambda *a, **k: None)
    queue = _spec(tmp_path, scheduler_walltime_used="00:30:00",
                  scheduler_mem_used="5242880kb", scheduler_cpu_used="01:30:00")
    text = show_status("localhost", "abc", queue_dir=queue)
    assert ("sched_use:    mem 5120 MB · cpu 01:30:00 (3.0 cores avg) (scheduler-reported)"
            in text)


def _row(**fields: object) -> dict[str, object]:
    row: dict[str, object] = {
        "id": "abc", "queue_host": "pbs_a", "queue_owner": "driver", "state": "running",
        "effective_state": "running", "scheduler_target": "pbs_a",
        "scheduler_state": "running", "cpus": 8, "mem_mb": 16000,
        "scheduler_walltime_used": "00:30:00",
        "last_heartbeat_at": (datetime.now(UTC) - timedelta(seconds=40)).isoformat(),
    }
    row.update(fields)
    return row


class TestConsole:
    def test_a_reported_job_shows_memory_and_average_cores(self) -> None:
        usage = fleet_mod._UsageObservation(collected_at=datetime.now(UTC).isoformat())
        row = _row(scheduler_mem_used="5242880kb", scheduler_cpu_used="01:30:00")
        row["resource_observation"] = fleet_mod._resource_observation(row, usage)
        observation = row["resource_observation"]
        assert isinstance(observation, dict)
        assert (observation["status"], observation["source"]) == ("scheduler_reported",
                                                                  "scheduler")
        assert (observation["cpu_percent"], observation["rss_mb"]) == (300.0, 5120.0)
        view = calculations.observation_view(row, now=datetime.now(UTC))
        assert (view["cpu_cores"], view["rss_mb"]) == (3.0, 5120.0)
        assert str(view["label"]).startswith("scheduler, 0:00:4")

    def test_without_a_reading_it_is_not_sampled(self) -> None:
        usage = fleet_mod._UsageObservation(collected_at=datetime.now(UTC).isoformat())
        assert fleet_mod._resource_observation(_row(), usage)["status"] == "scheduler"

    def test_reported_usage_stays_out_of_the_fresh_totals(self) -> None:
        usage = fleet_mod._UsageObservation(collected_at=datetime.now(UTC).isoformat())
        row = _row(scheduler_mem_used="5242880kb", scheduler_cpu_used="01:30:00")
        row["resource_observation"] = fleet_mod._resource_observation(row, usage)
        summary = calculations.calculation_summary([row], now=datetime.now(UTC))
        assert (summary["measured_jobs"], summary["measured_cpu_cores"]) == (0, 0.0)
        assert summary["partial"] is True
        (item,) = summary["unmeasured"]  # type: ignore[misc]
        assert (item["status"], item["label"]) == ("scheduler_reported", "scheduler-reported")
