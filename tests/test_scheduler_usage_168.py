"""#168: live usage of running Torque/PBS jobs, as the scheduler reports it.

The `qstat -f` detail the poll already fetches carries `resources_used.mem`
and `resources_used.cput`; the daemon keeps them on the job beside the
walltime, and `vq status` and the fleet console show them labelled as the
scheduler's own reading, with CPU as an average over the run so far. They
never enter the fresh-sample totals.

Slurm's squeue and `sacct -X` report neither for a running job, so the
detail poll adds one batched `sstat` for the jobs it sees running: the CPU
time of their running steps, and the peak RSS of their largest task, which is
shown as a largest-task peak and never as the job's memory. It is telemetry:
a failing or unreadable call leaves the values unknown and the poll intact.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tests.test_daemon_scheduler import (
    MockDispatcher,
    _inject,
    _submit_scheduler,
    daemon,  # noqa: F401 -- pytest fixture
)
from tests.test_scheduler_dispatch import FakeRunner, make_dispatcher, make_slurm_dispatcher
from vq.daemon import Daemon
from vq.scheduler_dialect import (
    DialectError,
    LiveUsage,
    QstatDetail,
    SchedulerPhase,
    SlurmDialect,
    TorqueDialect,
    slurm_cpu_seconds,
    slurm_memory_bytes,
    torque_memory_mb,
)
from vq.scheduler_dispatch import RemoteResult, SchedulerHandle
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


# --- Slurm: sstat ---------------------------------------------------------------

#: sstat --allsteps rows: JobID|MaxRSS|AveCPU|NTasks. 4243 runs a batch
#: script that launched a four-task srun step; its extern step is idle.
SSTAT = """4243.extern|1024K|00:00.010|1
4243.batch|1.50G|01:30:00|1
4243.0|512M|02:00:00|4
"""


@pytest.mark.parametrize(("value", "expected"), [
    ("1.50G", 1610612736), ("123456K", 123456 * 1024), ("512M", 512 * 1024**2),
    ("900", 900), ("1T", 1024**4), ("", None), ("12X", None), ("-5K", None),
])
def test_slurm_memory_values(value: str, expected: int | None) -> None:
    assert slurm_memory_bytes(value) == expected


@pytest.mark.parametrize(("value", "expected"), [
    ("01:23.456", 83.456), ("01:30:00", 5400.0), ("1-02:03:04", 93784.0),
    ("00:00:00", 0.0), ("", None), ("01:61:00", None), ("1-25:00:00", None), ("abc", None),
])
def test_slurm_cpu_times(value: str, expected: float | None) -> None:
    assert slurm_cpu_seconds(value) == expected


def test_sstat_keeps_the_largest_task_peak_and_sums_cpu_over_tasks_and_steps() -> None:
    usage = SlurmDialect().parse_usage(SSTAT)
    # Memory: the largest task's peak over the steps, never a sum. CPU:
    # 1.5 h + 4 tasks x 2 h + 0.01 s.
    assert usage == {"4243": LiveUsage(task_peak_rss="1572864kb", cpu_used="09:30:00")}


def test_a_step_without_accounting_makes_that_value_unknown() -> None:
    usage = SlurmDialect().parse_usage("4243.batch||01:00:00|1\n4243.0|2G||\n")
    assert usage == {"4243": LiveUsage(task_peak_rss=None, cpu_used=None)}


def test_malformed_sstat_output_is_refused() -> None:
    with pytest.raises(DialectError, match="malformed sstat row"):
        SlurmDialect().parse_usage("4243|1G|00:01:00|1\n")
    with pytest.raises(DialectError, match="malformed sstat row"):
        SlurmDialect().parse_usage("4243.batch|1G\n")


def _sacct(*rows: tuple[str, str]) -> str:
    return "".join(f"{job}|{state}|0:0|00:30:00|08:00:00|node01\n" for job, state in rows)


def test_the_poll_reads_usage_for_running_jobs_only() -> None:
    def respond(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv[0] == "sacct":
            return RemoteResult(0, _sacct(("4243", "RUNNING"), ("4244", "PENDING")), "")
        if argv[0] == "sstat":
            return RemoteResult(0, SSTAT, "")
        return None

    runner = FakeRunner(responder=respond)
    details = make_slurm_dispatcher(runner).poll_detail(
        [SchedulerHandle("4243", "/ws/a"), SchedulerHandle("4244", "/ws/b")])
    (sstat,) = [call.argv for call in runner.calls if call.argv[0] == "sstat"]
    assert sstat[-2:] == ["-j", "4243"]
    assert (details["4243"].task_peak_rss, details["4243"].cpu_used) == ("1572864kb", "09:30:00")
    assert details["4243"].cpu_usage_observed is True
    assert details["4243"].walltime_used == "00:30:00"
    assert (details["4244"].task_peak_rss, details["4244"].cpu_used) == (None, None)


@pytest.mark.parametrize("failure", ["raises", "garbage"])
def test_a_failing_usage_read_leaves_the_poll_intact(failure: str) -> None:
    def respond(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv[0] == "sacct":
            return RemoteResult(0, _sacct(("4243", "RUNNING")), "")
        if argv[0] == "sstat":
            if failure == "raises":
                raise OSError("ssh: connection reset")
            return RemoteResult(1, "sstat: error: no steps\n", "")
        return None

    details = make_slurm_dispatcher(FakeRunner(responder=respond)).poll_detail(
        [SchedulerHandle("4243", "/ws/a")])
    assert details["4243"].raw_state == "RUNNING"
    assert details["4243"].cpu_usage_observed is True
    assert details["4243"].walltime_used == "00:30:00"
    assert (details["4243"].task_peak_rss, details["4243"].cpu_used) == (None, None)


def test_torque_runs_no_usage_command() -> None:
    runner = FakeRunner(responder=lambda argv, stdin: RemoteResult(0, QSTAT, "")
                        if argv[0] == "qstat" else None)
    details = make_dispatcher(runner).poll_detail([SchedulerHandle("4242.pbs-server", "/ws")])
    assert details["4242.pbs-server"].mem_used == "5242880kb"
    assert [call.argv[0] for call in runner.calls] == ["qstat"]


def test_reconcile_stamps_and_keeps_the_task_peak(daemon: Daemon) -> None:  # noqa: F811
    detail = QstatDetail(raw_state="RUNNING", walltime_used="00:30:00",
                         task_peak_rss="1572864kb", cpu_used="09:30:00")
    mock = MockDispatcher(phase=SchedulerPhase.RUNNING, detail=detail)
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)
    daemon._reconcile_scheduler()
    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert (on_disk.scheduler_task_peak_rss, on_disk.scheduler_cpu_used) == (
        "1572864kb", "09:30:00")
    assert on_disk.scheduler_mem_used is None

    mock.detail = QstatDetail(raw_state="RUNNING", walltime_used="00:31:00")
    daemon._scheduler_running["j1"].last_status_write = 0.0
    daemon._reconcile_scheduler()
    assert JobSpec.read(daemon._spec_path("j1")).scheduler_task_peak_rss == "1572864kb"


def test_status_labels_the_slurm_peak_as_one_tasks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import vq.status as status_mod

    monkeypatch.setattr(status_mod.rpc, "request_scheduler_status_refresh",
                        lambda *a, **k: None)
    queue = _spec(tmp_path, scheduler_walltime_used="03:10:00",
                  scheduler_task_peak_rss="1572864kb", scheduler_cpu_used="09:30:00")
    text = show_status("localhost", "abc", queue_dir=queue)
    assert ("sched_use:    largest-task peak 1536 MB · cpu 09:30:00 (3.0 cores avg) "
            "(scheduler-reported)") in text
    assert "mem " not in text.split("sched_use:", 1)[1].splitlines()[0]


class TestSlurmConsole:
    def test_the_peak_is_a_largest_task_peak_not_current_memory(self) -> None:
        usage = fleet_mod._UsageObservation(collected_at=datetime.now(UTC).isoformat())
        started = (datetime.now(UTC) - timedelta(hours=3)).isoformat()
        row = _row(scheduler_walltime_used="03:10:00", scheduler_task_peak_rss="1572864kb",
                   scheduler_cpu_used="09:30:00", started_at=started)
        row["resource_observation"] = fleet_mod._resource_observation(row, usage)
        observation = row["resource_observation"]
        assert isinstance(observation, dict)
        assert observation["status"] == "scheduler_reported"
        assert (observation["rss_mb"], observation["rss_peak_mb"]) == (None, 1536.0)
        assert observation["rss_peak_scope"] == "largest task"
        view = calculations.observation_view(row, now=datetime.now(UTC))
        assert (view["cpu_cores"], view["rss_mb"], view["rss_peak_mb"]) == (3.0, None, 1536.0)
        assert view["rss_peak_scope"] == "largest task"
        summary = calculations.calculation_summary([row], now=datetime.now(UTC))
        assert summary["measured_jobs"] == 0 and summary["partial"] is True

    def test_a_peak_without_a_start_time_is_dropped(self) -> None:
        usage = fleet_mod._UsageObservation(collected_at=datetime.now(UTC).isoformat())
        row = _row(scheduler_task_peak_rss="1572864kb")
        observation = fleet_mod._resource_observation(row, usage)
        assert observation["status"] == "scheduler"
        assert observation["rss_peak_mb"] is None


def test_the_rendered_table_marks_a_task_peak(tmp_path: Path,
                                              monkeypatch: pytest.MonkeyPatch) -> None:
    from fastapi.testclient import TestClient

    from vq import auth, config, paths
    from vq.overview import HostOverview
    from vq.web import create_app

    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
    monkeypatch.setenv("VQ_WEB_FLEET", "1")
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    usage = fleet_mod._UsageObservation(collected_at=datetime.now(UTC).isoformat())
    row = _row(scheduler_task_peak_rss="1572864kb", scheduler_cpu_used="09:30:00",
               started_at=(datetime.now(UTC) - timedelta(hours=3)).isoformat())
    row["resource_observation"] = fleet_mod._resource_observation(row, usage)
    client = TestClient(create_app())
    client.app.state.fleet_cache.set_snapshot(fleet_mod.FleetSnapshot(
        gathered_at=datetime.now(UTC).isoformat(), duration_seconds=1.0,
        hosts=[fleet_mod.HostSnapshot(host="slurm_a", overview=HostOverview(host="slurm_a"),
                                      jobs=[row])]))
    html = client.get("/fleet/jobs/_table").text
    assert '1536 <span class="hint">task</span>' in html
    assert "peak RSS of the largest task in the running steps (Slurm sstat)" in html


def test_sstat_has_its_own_short_budget() -> None:
    from vq import scheduler_dispatch

    argv = SlurmDialect().usage_command(["4243"])
    assert scheduler_dispatch._scheduler_remote_timeout(argv) == (
        scheduler_dispatch.SCHEDULER_USAGE_TIMEOUT_SECONDS)
    assert scheduler_dispatch.SCHEDULER_USAGE_TIMEOUT_SECONDS < (
        scheduler_dispatch.SCHEDULER_ACCOUNTING_TIMEOUT_SECONDS)
    assert scheduler_dispatch._scheduler_remote_retries(argv) == 0


def test_average_cores_cover_a_slurm_job_past_a_day() -> None:
    # Slurm writes an elapsed time past a day as D-HH:MM:SS.
    usage = fleet_mod._UsageObservation(collected_at=datetime.now(UTC).isoformat())
    row = _row(scheduler_walltime_used="1-02:00:00", scheduler_cpu_used="52:00:00")
    observation = fleet_mod._resource_observation(row, usage)
    assert observation["cpu_percent"] == 200.0
    assert calculations.runtime_view(row, now=datetime.now(UTC))["seconds"] == 93600


def test_a_watchdog_peak_is_a_job_peak() -> None:
    now = datetime.now(UTC)
    usage = fleet_mod._UsageObservation(collected_at=now.isoformat(), by_job={"abc": {
        "cpu_percent": 100.0, "rss_mb": 200.0, "rss_peak_mb": 300.0,
        "rss_peak_since": (now - timedelta(minutes=5)).isoformat(), "sample_at": now.isoformat()}})
    row = _row(scheduler_target=None, scheduler_state=None)
    observation = fleet_mod._resource_observation(row, usage)
    assert (observation["rss_peak_mb"], observation["rss_peak_scope"]) == (300.0, "job")


@pytest.mark.parametrize("value", ["213503982334-14:25:51", "5124095576030:25:51"])
def test_reserved_slurm_cpu_is_unknown(value: str) -> None:
    assert slurm_cpu_seconds(value) is None
    reading = SlurmDialect().parse_usage(
        f"4243.extern|1024K|{value}|1\n4243.batch|2048K|00:34:56|1\n")
    assert reading["4243"].cpu_used is None
    assert reading["4243"].task_peak_rss == "2048kb"


def test_valid_long_cpu_duration_is_preserved() -> None:
    assert slurm_cpu_seconds("1000000-00:00:00") == 86400000000


def test_unavailable_live_cpu_clears_prior_measured_value(daemon: Daemon) -> None:  # noqa: F811
    mock = MockDispatcher(phase=SchedulerPhase.RUNNING, detail=QstatDetail(
        raw_state="RUNNING", walltime_used="00:30:00", cpu_used="01:30:00"))
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)
    daemon._reconcile_scheduler()
    assert JobSpec.read(daemon._spec_path("j1")).scheduler_cpu_used is not None
    mock.detail = QstatDetail(raw_state="RUNNING", walltime_used="00:31:00",
                             cpu_usage_observed=True)
    daemon._scheduler_running["j1"].last_status_write = 0.0
    daemon._reconcile_scheduler()
    assert JobSpec.read(daemon._spec_path("j1")).scheduler_cpu_used is None


@pytest.mark.parametrize("cpu", ["5124095576031:46:00", "213503982334-14:25:51"])
def test_historical_terminal_cpu_sentinel_read_and_raw_fleet(tmp_path, cpu):
    import json

    from vq.status import _format_scheduler_usage

    # The first string is an actual historical aggregate: a reserved .extern
    # counter plus valid .batch CPU. No live scheduler poll will repair it.
    raw = dict(id="historical", command=["run"], cwd="/tmp/work", cpus=4,
               state="completed", scheduler_target="cluster",
               scheduler_cpu_used=cpu, scheduler_walltime_used="00:10:00")
    path = tmp_path / "historical.json"
    path.write_text(json.dumps(raw))
    spec = JobSpec.read(path)
    assert spec.scheduler_cpu_used is None
    assert spec.model_dump()["scheduler_cpu_used"] is None
    assert _format_scheduler_usage(spec) is None
    assert fleet_mod._scheduler_reported(raw) is None
    row, = fleet_mod._normalize_rows([raw.copy()], "driver")
    assert row["scheduler_cpu_used"] is None
    # Read normalization preserves the original evidence bytes on disk.
    assert json.loads(path.read_text())["scheduler_cpu_used"] == cpu
    spec.scheduler_cpu_used = cpu
    assert spec.scheduler_cpu_used is None


@pytest.mark.parametrize("cpu", ["100000000:00:00", "52:00:00", "00:00:00", "00:01.250"])
def test_historical_valid_long_and_fractional_cpu_remains_measured(cpu):
    from vq.scheduler_dialect import slurm_cpu_seconds
    from vq.status import _format_scheduler_usage

    raw = dict(id="valid", command=["run"], cwd="/tmp/work", cpus=4,
               scheduler_cpu_used=cpu, scheduler_walltime_used="00:10:00")
    spec = JobSpec.model_validate(raw)
    assert spec.scheduler_cpu_used == cpu
    assert f"cpu {cpu}" in _format_scheduler_usage(spec)
    assert fleet_mod._scheduler_reported(raw)["cpu_percent"] == round(
        slurm_cpu_seconds(cpu) / 600 * 100, 1)


def test_valid_counter_just_below_reserved_bound_is_not_rounded_to_sentinel():
    from vq.scheduler_dialect import SLURM_CPU_RESERVED_SECONDS, format_walltime, slurm_cpu_seconds

    seconds = SLURM_CPU_RESERVED_SECONDS - 1
    cpu = format_walltime(seconds)
    assert slurm_cpu_seconds(cpu) == seconds
    assert JobSpec(id="bound", command=["run"], cwd="/tmp", cpus=1,
                   scheduler_cpu_used=cpu).scheduler_cpu_used == cpu
