"""#168: the fleet jobs table as a calculation overview.

Declared resources, measured usage and unknowns stay apart; measurements
are read once per driver; queue wait never poses as runtime; a missing
measurement never reads as zero.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, parse_qsl, urlsplit

import pytest
from fastapi.testclient import TestClient

from vq import auth, config, paths, transport
from vq.overview import HostOverview
from vq.spec import JobSpec, JobState
from vq.web import calculations, create_app, view_context
from vq.web import fleet as fleet_mod

NOW = datetime(2026, 10, 12, 12, 0, 0, tzinfo=UTC)


def _row(jobid: str, **fields: object) -> dict:
    row: dict[str, object] = {
        "id": jobid,
        "queue_host": "host_a",
        "queue_owner": "host_a",
        "state": "running",
        "effective_state": "running",
        "scheduler_target": None,
        "cpus": 8,
        "mem_mb": 16000,
        "submitted_at": "2026-10-12T10:00:00+00:00",
        "started_at": "2026-10-12T10:30:00+00:00",
        "command": ["python", "run.py"],
    }
    row.update(fields)
    return row


def _observed(cpu: float | None, rss: float | None, *, status: str = "observed",
              sample_at: str | None = "2026-10-12T11:59:50+00:00") -> dict:
    return {"status": status, "source": "watchdog", "cpu_percent": cpu,
            "rss_mb": rss, "sample_at": sample_at,
            "collected_at": "2026-10-12T11:59:55+00:00"}


class TestRuntime:
    def test_direct_job_runs_from_dispatch_minus_pauses_and_waits_until_dispatch(self) -> None:
        runtime = calculations.runtime_view(
            _row("direct01", paused_seconds_total=600.0, wall_time_seconds=7200), now=NOW,
        )
        assert runtime["seconds"] == 90 * 60 - 600
        assert runtime["source"] == "vq"
        assert runtime["wait_seconds"] == 30 * 60
        assert runtime["waiting"] is False
        assert (runtime["limit_seconds"], runtime["limit_source"]) == (7200, "declared")

    def test_handed_off_scheduler_job_still_queued_has_no_runtime(self) -> None:
        # vq stamps started_at when it hands the job to the scheduler. The
        # job then sat in the cluster queue: that is wait, not runtime.
        row = _row("sched01", scheduler_target="cluster_a", scheduler_state="queued",
                   effective_state="queued")
        runtime = calculations.runtime_view(row, now=NOW)
        assert runtime["seconds"] is None
        assert runtime["waiting"] is True
        assert runtime["wait_seconds"] == 2 * 3600
        assert calculations.activity(row) == "queued"

    def test_running_scheduler_job_uses_the_reported_walltime_only(self) -> None:
        row = _row("sched02", scheduler_target="cluster_a", scheduler_state="running",
                   scheduler_walltime_used="00:45:00", scheduler_walltime_limit="04:00:00",
                   wall_time_seconds=3600)
        runtime = calculations.runtime_view(row, now=NOW)
        assert (runtime["seconds"], runtime["source"]) == (45 * 60, "scheduler")
        assert (runtime["limit_seconds"], runtime["limit_source"]) == (4 * 3600, "scheduler")
        # The cluster queue wait is not recorded, so it is not guessed.
        assert runtime["wait_seconds"] is None

        unreported = dict(row, scheduler_walltime_used=None)
        assert calculations.runtime_view(unreported, now=NOW)["seconds"] is None

    def test_finished_jobs_report_their_final_duration(self) -> None:
        direct = _row("done01", state="completed", effective_state="completed",
                      finished_at="2026-10-12T11:00:00+00:00")
        runtime = calculations.runtime_view(direct, now=NOW)
        assert runtime["final"] is True and runtime["seconds"] == 30 * 60

        never_started = _row("killed01", state="killed", effective_state="killed",
                             started_at=None, finished_at="2026-10-12T11:00:00+00:00")
        assert calculations.runtime_view(never_started, now=NOW)["seconds"] is None

    def test_duration_format(self) -> None:
        assert calculations.format_duration(None) == "-"
        assert calculations.format_duration(45) == "0:00:45"
        assert calculations.format_duration(2 * 86400 + 3 * 3600 + 4 * 60 + 5) == "2d 3:04:05"


class TestObservation:
    def test_measured_usage_is_core_equivalents_and_rss_with_age(self) -> None:
        view = calculations.observation_view(
            _row("direct01", resource_observation=_observed(390.0, 5120.0)), now=NOW,
        )
        assert view["status"] == "observed"
        assert (view["cpu_cores"], view["rss_mb"]) == (3.9, 5120.0)
        assert view["age_seconds"] == 10.0
        assert view["label"] == "0:00:10 ago"

    def test_cached_sample_ages_out_of_fresh_totals_without_a_new_sweep(self) -> None:
        row = _row("cached01", resource_observation=_observed(400.0, 3000.0))
        assert calculations.observation_view(row, now=NOW)["status"] == "observed"
        assert calculations.calculation_summary([row], now=NOW)["measured_jobs"] == 1
        assert calculations.observation_view(
            row, now=NOW + timedelta(seconds=20),
        )["status"] == "observed"
        later = NOW + timedelta(seconds=21)
        view = calculations.observation_view(row, now=later)
        assert view["status"] == "stale"
        assert view["label"] == "stale, 0:00:31 ago"
        assert (view["cpu_cores"], view["rss_mb"]) == (4.0, 3000.0)
        summary = calculations.calculation_summary([row], now=later)
        assert summary["measured_jobs"] == 0
        assert summary["measured_cpu_cores"] == summary["measured_rss_mb"] == 0.0
        assert summary["partial"] is True
        assert summary["unmeasured"][0]["status"] == "stale"
        assert row["resource_observation"]["status"] == "observed"

    @pytest.mark.parametrize("sample_at", [None, "not-a-timestamp", "2026-10-12T12:00:01Z"])
    def test_unknown_sample_age_does_not_use_collection_time_as_freshness(self, sample_at):
        row = _row("undated01", resource_observation=_observed(
            400.0, 3000.0, sample_at=sample_at,
        ))
        view = calculations.observation_view(row, now=NOW)
        assert view["status"] == "age_unknown"
        assert view["label"] == "age unknown"
        assert view["age_seconds"] is None
        assert (view["cpu_cores"], view["rss_mb"]) == (4.0, 3000.0)
        summary = calculations.calculation_summary([row], now=NOW)
        assert summary["measured_jobs"] == 0
        assert summary["partial"] is True
        assert summary["unmeasured"][0]["status"] == "age_unknown"

    @pytest.mark.parametrize("status", [
        "scheduler", "no_sample", "collector_error", "not_collected", "not_running",
    ])
    def test_unmeasured_rows_carry_no_numbers(self, status: str) -> None:
        # Even with numbers present in the payload: only observed/stale
        # statuses may show them, and declared values never stand in.
        view = calculations.observation_view(
            _row("job01", resource_observation=_observed(800.0, 9000.0, status=status)),
            now=NOW,
        )
        assert view["status"] == status
        assert view["cpu_cores"] is None and view["rss_mb"] is None
        assert view["detail"] or status == "not_running"

    def test_missing_observation_is_unknown_not_zero(self) -> None:
        view = calculations.observation_view(_row("old01"), now=NOW)
        assert view["status"] == "not_collected"
        assert view["label"] == "unknown"
        assert view["cpu_cores"] is None


class TestSummary:
    def test_queued_reservations_are_not_running_utilization(self) -> None:
        rows = [
            _row("direct01", resource_observation=_observed(400.0, 2048.0)),
            _row("stale01", resource_observation=_observed(
                100.0, 1024.0, status="stale")),
            _row("sched01", scheduler_target="cluster_a", effective_state="queued",
                 cpus=64, mem_mb=256000),
            _row("sched02", scheduler_target="cluster_a", effective_state="running",
                 cpus=16, mem_mb=None,
                 resource_observation=_observed(None, None, status="scheduler")),
            _row("unpolled01", scheduler_target="cluster_a", effective_state="unpolled",
                 cpus=32),
            _row("done01", state="completed", effective_state="completed"),
        ]
        summary = calculations.calculation_summary(rows, now=NOW)
        assert summary["counts"] == {
            "running": 3, "queued": 1, "held": 0, "unconfirmed": 1, "finished": 1,
        }
        # Allocation: declared by running jobs only.
        assert summary["allocated_cpus"] == 8 + 8 + 16
        assert summary["allocated_mem_mb"] == 16000 + 16000
        assert summary["unlimited_mem_jobs"] == 1
        # Measurement: fresh samples only; never the declared limits.
        assert summary["measured_jobs"] == 1
        assert summary["measured_cpu_cores"] == 4.0
        assert summary["measured_rss_mb"] == 2048.0
        assert summary["partial"] is True
        assert {item["status"]: item["count"] for item in summary["unmeasured"]} == {
            "scheduler": 1, "stale": 1,
        }


def _cfg() -> config.Config:
    return config.Config(hosts={
        "localhost": config.HostConfig(ssh="localhost"),
        "driver": config.HostConfig(ssh="driver.example.invalid"),
        "idle": config.HostConfig(ssh="idle.example.invalid"),
        "gone": config.HostConfig(ssh="gone.example.invalid"),
        "cluster_a": config.HostConfig(
            ssh="login.example.invalid", scheduler="slurm", scheduler_dialect="slurm",
            scheduler_driver="driver", scratch_root="/tmp/scratch",
        ),
    })


def _spec(root: Path, jobid: str, **fields: object) -> JobSpec:
    workspace = root / jobid
    (workspace / "_vq").mkdir(parents=True, exist_ok=True)
    base: dict[str, object] = {
        "id": jobid, "command": ["true"], "cwd": str(workspace), "cpus": 4,
        "state": JobState.RUNNING, "started_at": "2026-10-12T10:00:00+00:00",
    }
    base.update(fields)
    return JobSpec(**base)


def test_snapshot_reads_samples_once_per_driver_and_keeps_authorities_apart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A mixed fleet: a local direct job with a sample, a remote driver
    running a direct job and a Slurm job under the same id as the local
    one, an idle host, and an unreachable host."""
    from vq import overview

    now_ts = datetime.now(UTC).isoformat()
    local = _spec(tmp_path, "samejob0001", mem_mb=8000)
    (Path(local.cwd) / "_vq" / "samples.jsonl").write_text(
        json.dumps({"ts": now_ts, "cpu_percent": 395.0, "rss_mb": 3000.0}) + "\n"
    )
    remote_direct = _spec(tmp_path, "samejob0001", cpus=2)
    remote_sched = _spec(tmp_path, "slurmjob001", scheduler_target="cluster_a",
                         scheduler_state="running", scheduler_exec_host="node[01-02]",
                         scheduler_walltime_used="00:10:00", cpus=32)
    idle_done = _spec(tmp_path, "idledone001", state=JobState.COMPLETED,
                      finished_at="2026-10-12T11:00:00+00:00")
    queue_rows = {
        "driver.example.invalid": [remote_direct, remote_sched],
        "idle.example.invalid": [idle_done],
    }
    reads: list[tuple[str, tuple[str, ...]]] = []

    def remote_vq(host_cfg, *args, **kwargs):
        reads.append((host_cfg.ssh, args))
        if host_cfg.ssh == "gone.example.invalid":
            raise transport.RemoteError("ssh: connect timed out")
        if args == ("queue", "localhost", "--json"):
            return SimpleNamespace(stdout=json.dumps(
                [fleet_mod._job_row(s, "localhost") for s in queue_rows[host_cfg.ssh]]))
        assert args == ("top", "localhost", "--json")
        # An older remote vq: no sample_at. Its rows for jobs that are not
        # running direct jobs are ignored.
        return SimpleNamespace(stdout=json.dumps([
            {"jobid": "samejob0001", "cpu_percent": 180.0, "rss_mb": 900.0,
             "sample_stale": False},
            {"jobid": "slurmjob001", "cpu_percent": 3200.0, "rss_mb": 1.0},
        ]))

    monkeypatch.setattr(fleet_mod, "list_jobs", lambda *a, **k: [local])
    monkeypatch.setattr(fleet_mod.transport, "run_remote_vq", remote_vq)
    monkeypatch.setattr(overview, "_collect_env_statuses", lambda cfg: [])
    monkeypatch.setattr(overview, "gather_overview_remote",
                        lambda host, *a, **k: HostOverview(host=host))

    snapshot = fleet_mod.gather_fleet_snapshot(_cfg())

    # One queue read per remote driver; one sample read only where a
    # direct job runs (not "idle"); nothing per row.
    assert sorted(reads) == sorted([
        ("driver.example.invalid", ("queue", "localhost", "--json")),
        ("driver.example.invalid", ("top", "localhost", "--json")),
        ("idle.example.invalid", ("queue", "localhost", "--json")),
        ("gone.example.invalid", ("queue", "localhost", "--json")),
    ])
    rows = {(row["queue_owner"], row["id"]): row for row in snapshot.jobs}
    assert set(rows) == {
        ("localhost", "samejob0001"), ("driver", "samejob0001"),
        ("driver", "slurmjob001"), ("idle", "idledone001"),
    }
    observed_now = datetime.now(UTC)
    local_obs = calculations.observation_view(rows["localhost", "samejob0001"], now=observed_now)
    remote_obs = calculations.observation_view(rows["driver", "samejob0001"], now=observed_now)
    assert (local_obs["status"], local_obs["cpu_cores"], local_obs["rss_mb"]) == (
        "observed", 3.95, 3000.0)
    assert (remote_obs["status"], remote_obs["cpu_cores"], remote_obs["sample_at"]) == (
        "age_unknown", 1.8, None)
    sched = rows["driver", "slurmjob001"]
    assert sched["queue_host"] == "cluster_a"
    assert sched["resource_observation"]["status"] == "scheduler"
    assert sched["resource_observation"]["cpu_percent"] is None
    assert calculations.location_view(sched) == {"nodes": "node[01-02]", "driver": "driver"}
    assert rows["idle", "idledone001"]["resource_observation"]["status"] == "final_absent"

    coverage = calculations.coverage_view(snapshot)
    assert [item["host"] for item in coverage["unlisted"]] == ["gone"]
    assert "connect timed out" in coverage["unlisted"][0]["reason"]
    # Every job is listed once; the Slurm job is not charged to its driver.
    summary = calculations.calculation_summary(snapshot.jobs, now=observed_now)
    assert summary["counts"]["running"] == 3
    assert summary["allocated_cpus"] == 4 + 2 + 32
    assert summary["measured_jobs"] == 1
    assert summary["measured_cpu_cores"] == 4.0  # The undated remote sample is excluded.


def test_failed_sample_read_is_a_collector_error_not_zero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vq import overview

    remote = _spec(tmp_path, "remotejob01")

    def remote_vq(host_cfg, *args, **kwargs):
        if args[0] == "top":
            raise transport.RemoteError("remote vq exited 1")
        return SimpleNamespace(stdout=json.dumps([fleet_mod._job_row(remote, "localhost")]))

    monkeypatch.setattr(fleet_mod, "list_jobs", lambda *a, **k: [])
    monkeypatch.setattr(fleet_mod.transport, "run_remote_vq", remote_vq)
    monkeypatch.setattr(overview, "_collect_env_statuses", lambda cfg: [])
    monkeypatch.setattr(overview, "gather_overview_remote",
                        lambda host, *a, **k: HostOverview(host=host))
    cfg = config.Config(hosts={
        "driver": config.HostConfig(ssh="driver.example.invalid"),
        "cluster_a": config.HostConfig(
            ssh="login.example.invalid", scheduler="slurm", scheduler_dialect="slurm",
            scheduler_driver="driver", scratch_root="/tmp/scratch",
        ),
    })

    snapshot = fleet_mod.gather_fleet_snapshot(cfg)

    host, alias = snapshot.hosts
    # Reported once, on the driver: the alias has nothing to sample.
    assert alias.host == "cluster_a" and alias.usage_error is None
    assert host.usage_error and "exited 1" in host.usage_error
    assert snapshot.to_json()["hosts"][0]["usage_error"] == host.usage_error
    (row,) = snapshot.jobs
    view = calculations.observation_view(row, now=NOW)
    assert (view["status"], view["cpu_cores"], view["rss_mb"]) == (
        "collector_error", None, None)
    assert calculations.coverage_view(snapshot)["usage_errors"] == [
        {"host": "driver", "reason": host.usage_error},
    ]


def test_default_order_puts_confirmed_running_jobs_first() -> None:
    rows = [
        _row("cluster_queued", scheduler_target="cluster_a", effective_state="queued",
             submitted_at="2026-10-12T11:59:00+00:00"),
        _row("done", state="completed", effective_state="completed"),
        _row("paused", state="suspended", effective_state="suspended"),
        _row("unpolled", scheduler_target="cluster_a", effective_state="unpolled"),
        _row("direct_pending", state="pending", effective_state="pending"),
        _row("direct_running"),
    ]
    snapshot = fleet_mod.FleetSnapshot(
        gathered_at=NOW.isoformat(), duration_seconds=1.0,
        hosts=[fleet_mod.HostSnapshot(host="host_a", overview=HostOverview(host="host_a"),
                                      jobs=rows)],
    )
    context = view_context.fleet_jobs_context(snapshot, {})
    assert [job["id"] for job in context["jobs"]] == [
        "direct_running", "unpolled", "paused", "cluster_queued", "direct_pending", "done",
    ]


def test_sort_and_program_filter_survive_the_table_refresh() -> None:
    rows = [
        _row("short01", program="vibe-qc", started_at="2026-10-12T11:50:00+00:00"),
        _row("long01", program="vibe-qc", started_at="2026-10-12T08:00:00+00:00"),
        _row("orca01", program="orca"),
        _row("queued01", program="vibe-qc", state="pending", effective_state="pending",
             started_at=None),
    ]
    snapshot = fleet_mod.FleetSnapshot(
        gathered_at=NOW.isoformat(), duration_seconds=1.0,
        hosts=[fleet_mod.HostSnapshot(host="host_a", overview=HostOverview(host="host_a"),
                                      jobs=rows)],
    )
    context = view_context.fleet_jobs_context(
        snapshot, {"program": "vibe-qc", "sort": "runtime"},
    )
    # Longest-running first; the queued job has no runtime and sorts last.
    assert [job["id"] for job in context["jobs"]] == ["long01", "short01", "queued01"]
    assert context["program_options"] == ["orca", "vibe-qc"]
    assert context["table_refresh_url"] == "/fleet/jobs/_table?program=vibe-qc&sort=runtime"
    assert context["summary"]["counts"]["running"] == 2


@pytest.mark.parametrize("field", ["project", "tag", "submitter", "q"])
def test_literal_all_labels_survive_refresh_and_sorting(field: str) -> None:
    query = view_context.fleet_query_state({field: "all"})
    serialized = view_context.fleet_query_string(query)
    assert parse_qs(serialized)[field] == ["all"]
    assert view_context.fleet_query_state(dict(parse_qsl(serialized))) == query
    href = view_context.fleet_sort_href(query, "runtime")
    assert parse_qs(urlsplit(href).query)[field] == ["all"]


def _project_snapshot() -> fleet_mod.FleetSnapshot:
    return fleet_mod.FleetSnapshot(
        gathered_at=NOW.isoformat(), duration_seconds=1.0,
        hosts=[
            fleet_mod.HostSnapshot(host="host_a", overview=HostOverview(host="host_a"), jobs=[
                _row("samejob001", submitter="alice", tags=["project.water", "shared"]),
                _row("multijob01", submitter="alice",
                     tags=["project.water", "project.kinetics"]),
                _row("otherjob01", submitter="alice", tags=["project.water-long"]),
                _row("untagged01", submitter="alice", tags=["water"]),
                _row("alljob0001", submitter="all", tags=["project.all", "all"]),
            ]),
            fleet_mod.HostSnapshot(host="host_b", overview=HostOverview(host="host_b"), jobs=[
                _row("samejob001", queue_host="host_b", queue_owner="host_b",
                     submitter="bob", tags=["project.water"]),
            ]),
            fleet_mod.HostSnapshot(
                host="gone", overview=HostOverview(host="gone"), jobs_error="unreachable",
                last_known_at=NOW.isoformat(), last_known=[
                    _row("carried001", queue_host="gone", submitter="alice", cpus=32,
                         tags=["project.water"]),
                    _row("carried002", queue_host="gone", submitter="carried-owner",
                         tags=["project.retained"]),
                ],
            ),
        ],
    )


def test_projects_and_owners_filter_live_and_last_known_jobs_and_totals() -> None:
    snapshot = _project_snapshot()
    context = view_context.fleet_jobs_context(snapshot, {"project": "water", "submitter": "alice"})
    assert {row["id"] for row in context["jobs"]} == {"samejob001", "multijob01"}
    assert context["filtered_count"] == 2 and context["total_count"] == 6
    assert context["summary"]["counts"]["running"] == 2
    assert context["summary"]["allocated_cpus"] == 16
    assert [row["id"] for group in context["last_known"]
            for row in group["jobs"]] == ["carried001"]
    assert context["project_options"] == ["all", "kinetics", "retained", "water", "water-long"]
    assert context["submitter_options"] == ["alice", "all", "bob", "carried-owner"]
    assert context["table_refresh_url"] == "/fleet/jobs/_table?project=water&submitter=alice"
    tagged = view_context.fleet_jobs_context(snapshot, {
        "project": "water", "submitter": "alice", "tag": "shared",
    })
    assert [row["id"] for row in tagged["jobs"]] == ["samejob001"]

    for tags in (None, "project.water", [None, 3, {}, "project.", "water", "project.water-long"]):
        row = _row("oldremote1", tags=tags)
        assert not view_context.fleet_row_matches(row, context["query"] | {"submitter": ""})


def test_project_selection_survives_an_empty_snapshot() -> None:
    context = view_context.fleet_jobs_context(None, {"project": "water"})
    assert context["project_options"] == ["water"]
    assert context["query"]["project"] == "water"
    assert context["table_refresh_url"] == "/fleet/jobs/_table?project=water"
    assert context["jobs"] == [] and context["summary"]["counts"]["running"] == 0


@pytest.fixture
def fleet_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
    monkeypatch.setenv("VQ_WEB_FLEET", "1")
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    return TestClient(create_app())


def test_project_owner_controls_filter_page_refresh_and_api_without_network_reads(
    fleet_client: TestClient, monkeypatch: pytest.MonkeyPatch,
) -> None:
    fleet_client.app.state.fleet_cache.set_snapshot(_project_snapshot())

    def no_network(*_args, **_kwargs):
        pytest.fail("filtering cached rows must not contact any execution host")

    monkeypatch.setattr(transport, "run_remote_vq", no_network)
    params = {"project": "water", "submitter": "alice"}
    page = fleet_client.get("/fleet/jobs", params=params)
    assert page.status_code == 200
    assert '<select name="project">' in page.text
    assert '<option value="water" selected>water</option>' in page.text
    assert 'list="fleet-submitters"' in page.text and "owner (submitter)" in page.text
    assert '<option value="carried-owner"></option>' in page.text
    assert 'project=water&amp;submitter=alice' in page.text
    fragment = fleet_client.get("/fleet/jobs/_table", params=params)
    assert fragment.status_code == 200
    assert "multijob01" in fragment.text and "carried001" in fragment.text
    assert "otherjob01" not in fragment.text and "carried002" not in fragment.text
    payload = fleet_client.get("/api/v1/fleet/jobs", params=params).json()
    assert payload["count"] == 2
    assert {(row["queue_owner"], row["id"]) for row in payload["jobs"]} == {
        ("host_a", "samejob001"), ("host_a", "multijob01"),
    }
    assert [row["id"] for group in payload["last_known"]
            for row in group["jobs"]] == ["carried001"]
    # "all" is a valid label as well as the host/state/program default.
    payload = fleet_client.get("/api/v1/fleet/jobs", params={
        "project": "all", "tag": "all", "submitter": "all",
    }).json()
    assert [row["id"] for row in payload["jobs"]] == ["alljob0001"]


def test_jobs_page_renders_measured_unknown_and_coverage(fleet_client: TestClient) -> None:
    now = datetime.now(UTC)
    rows = [
        _row("direct01", program="vibe-qc", program_runtime_pin={"program_version": "1.4.0"},
             started_at=now.isoformat(), submitted_at=now.isoformat(),
             resource_observation=_observed(400.0, 3000.0, sample_at=now.isoformat())),
        _row("sched01", queue_host="cluster_a", queue_owner="driver",
             scheduler_target="cluster_a", scheduler_state="running",
             scheduler_exec_host="node07", scheduler_walltime_used="01:02:03",
             resource_observation=_observed(None, None, status="scheduler")),
    ]
    snapshot = fleet_mod.FleetSnapshot(
        gathered_at=now.isoformat(), duration_seconds=1.0,
        hosts=[
            fleet_mod.HostSnapshot(host="host_a", overview=HostOverview(host="host_a"),
                                   jobs=rows),
            fleet_mod.HostSnapshot(host="gone", overview=HostOverview(host="gone"),
                                   jobs_error="RemoteError: ssh: connect timed out"),
        ],
    )
    fleet_client.app.state.fleet_cache.set_snapshot(snapshot)

    page = fleet_client.get("/fleet/jobs")
    assert page.status_code == 200
    html = page.text
    assert "gone was not listed in this snapshot" in html
    # Partial: the scheduler job runs but is not sampled.
    assert "cpu cores used" in html and "4.0*" in html
    assert ">4.0<" in html.replace(" ", "")  # the direct job's measured cores
    assert "1 not sampled (scheduler jobs are not sampled" in html
    assert "vibe-qc 1.4.0" in html
    assert "1:02:03" in html and "node07" in html and "via driver" in html
    assert "not sampled" in html
    assert "GPUs are not shown" in html

    fragment = fleet_client.get("/fleet/jobs/_table?program=vibe-qc")
    assert fragment.status_code == 200
    assert "direct01" in fragment.text and "sched01" not in fragment.text


def test_cached_samples_age_in_table_refresh_and_jobs_api(
    fleet_client: TestClient, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vq.web import fleet_routes

    instant = NOW

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return instant

    monkeypatch.setattr(view_context, "datetime", Clock)
    monkeypatch.setattr(fleet_routes, "datetime", Clock)

    def no_network(*_args, **_kwargs):
        pytest.fail("reading the cached table/API must not collect new resource samples")

    monkeypatch.setattr(transport, "run_remote_vq", no_network)
    rows = [
        _row("cached01", resource_observation=_observed(400.0, 3000.0)),
        _row("undated01", resource_observation=_observed(100.0, 1000.0, sample_at=None)),
    ]
    snapshot = fleet_mod.FleetSnapshot(
        gathered_at=NOW.isoformat(), duration_seconds=1.0,
        hosts=[fleet_mod.HostSnapshot(host="host_a", overview=HostOverview(host="host_a"),
                                      jobs=rows)],
    )
    fleet_client.app.state.fleet_cache.set_snapshot(snapshot)

    page = fleet_client.get("/fleet/jobs")
    assert page.status_code == 200
    assert "1 of 2 running" in page.text
    assert "age unknown" in page.text
    initial = fleet_client.get("/api/v1/fleet/jobs").json()["jobs"]
    assert initial[0]["resource_observation"]["status"] == "observed"
    assert initial[0]["resource_observation"]["age_seconds"] == 10.0
    assert initial[1]["resource_observation"]["status"] == "age_unknown"

    instant += timedelta(seconds=21)
    fragment = fleet_client.get("/fleet/jobs/_table")
    assert fragment.status_code == 200
    assert "0 of 2 running" in fragment.text
    assert "stale, 0:00:31 ago" in fragment.text
    assert "1 stale" in fragment.text
    updated = fleet_client.get("/api/v1/fleet/jobs").json()["jobs"]
    assert updated[0]["resource_observation"]["status"] == "stale"
    assert updated[0]["resource_observation"]["age_seconds"] == 31.0
    assert updated[0]["resource_observation"]["rss_mb"] == 3000.0
    assert updated[0]["resource_observation"]["sample_at"] == rows[0]["resource_observation"][
        "sample_at"]
    assert rows[0]["resource_observation"]["status"] == "observed"
    assert "age_seconds" not in rows[0]["resource_observation"]
