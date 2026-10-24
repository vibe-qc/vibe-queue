"""Final resource receipts reach the fleet table without per-job requests (#168)."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner
from fastapi.testclient import TestClient

from vq import cli, config, final_usage, paths
from vq.overview import HostOverview
from vq.spec import JobSpec, JobState
from vq.web import calculations, create_app
from vq.web import fleet as fleet_mod

NOW = datetime(2026, 10, 15, 12, tzinfo=UTC)


def _job(root: Path, jobid: str, *, state: JobState = JobState.COMPLETED) -> JobSpec:
    workspace = root / jobid
    (workspace / "_vq").mkdir(parents=True)
    return JobSpec(id=jobid, command=["true"], cwd=str(workspace), cpus=8, mem_mb=16000,
                   state=state, started_at="2026-10-15T11:00:00+00:00",
                   finished_at="2026-10-15T11:10:00+00:00" if state != JobState.RUNNING else None)


def _receipt(spec: JobSpec, *, index: int | None = None, wall: float = 120,
             cpu: float = 450, peak: float = 1536) -> None:
    name = "resource-usage.json" + (f".{index}" if index is not None else "")
    (Path(spec.cwd) / "_vq" / name).write_text(json.dumps({
        "schema": final_usage.DIRECT_SCHEMA, "status": "ok", "collector": "posix-wait4",
        "wall_seconds": wall, "active_cpu_seconds": cpu, "peak_rss_mb": peak,
        "command_exit_code": 0,
    }))


def _row(spec: JobSpec) -> dict:
    row = fleet_mod._job_row(spec, "localhost")
    row.update(queue_host="localhost", queue_owner="localhost")
    row["resource_observation"] = fleet_mod._resource_observation(
        row, fleet_mod._UsageObservation())
    return row


def test_queue_json_reads_final_receipts_only_on_the_owning_host(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = config.Config(hosts={"localhost": config.HostConfig(ssh="localhost")})
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    done, running = _job(tmp_path, "done"), _job(tmp_path, "running", state=JobState.RUNNING)
    _receipt(done)
    _receipt(running, cpu=9000)  # An old receipt must never become a live measurement.
    monkeypatch.setattr(cli, "list_jobs", lambda *a, **k: [done, running])
    result = CliRunner().invoke(cli.main, ["queue", "localhost", "--json"])
    assert result.exit_code == 0, result.output
    rows = {row["id"]: row for row in json.loads(result.output)}
    assert rows["done"]["resource_usage"]["average_cores"] == 3.75
    assert rows["running"]["resource_usage"] is None

    remote = dict(rows["done"], scheduler_target="lane", resource_usage={
        **rows["done"]["resource_usage"], "peak_rss_mb": 4096.0})
    cfg.hosts.update({
        "driver": config.HostConfig(ssh="driver.example.invalid"),
        "lane": config.HostConfig(ssh="login.example.invalid", scheduler="slurm",
                                  scheduler_dialect="slurm", scheduler_driver="driver",
                                  scratch_root="/tmp/scratch"),
    })
    calls = []

    def delegate(host, cfg, *args):
        calls.append((host, args))
        return json.dumps([remote])

    monkeypatch.setattr(cli, "_delegate_to_remote", delegate)
    monkeypatch.setattr(final_usage, "read_final_usage", lambda *a: pytest.fail(
        "scheduler projection must preserve the driver's receipt, never read its cwd here"))
    result = CliRunner().invoke(cli.main, ["queue", "lane", "--json"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)[0]["resource_usage"]["peak_rss_mb"] == 4096.0
    assert calls == [("driver", ("queue", "localhost", "--json"))]


def test_finished_rows_keep_receipt_scope_and_stay_out_of_live_totals(tmp_path: Path) -> None:
    done = _job(tmp_path, "done")
    _receipt(done)
    row = _row(done)
    view = calculations.calculation_view(row, now=NOW)
    assert view["runtime"]["seconds"] == 120.0  # Command time, not 600 s since dispatch.
    assert view["runtime"]["source"] == "receipt"
    obs = view["observation"]
    assert (obs["status"], obs["label"], obs["cpu_cores"]) == ("final", "final", 3.75)
    assert obs["rss_mb"] is None  # A peak is never current RSS.
    assert obs["rss_peak_mb"] == 1536.0
    assert obs["rss_peak_scope"] == "largest single process (ru_maxrss)"
    assert obs["rss_peak_since"] is None  # This is not a watchdog sample.
    summary = calculations.calculation_summary([row], now=NOW)
    assert summary["counts"]["finished"] == 1
    assert summary["allocated_cpus"] == summary["measured_cpu_cores"] == 0
    assert summary["measured_rss_mb"] == summary["measured_jobs"] == 0


def test_array_summary_reports_per_element_cpu_and_largest_peak(tmp_path: Path) -> None:
    done = _job(tmp_path, "array")
    _receipt(done, index=0, wall=100, cpu=300, peak=512)
    _receipt(done, index=1, wall=200, cpu=600, peak=1024)
    view = calculations.calculation_view(_row(done), now=NOW)
    assert view["runtime"]["seconds"] == 200.0
    obs = view["observation"]
    assert obs["label"] == "final, 2 elements"
    assert "average per element" in obs["detail"]
    assert obs["cpu_cores"] == 3.0
    assert obs["rss_peak_mb"] == 1024.0


def test_nonfinite_remote_final_values_are_not_displayed(tmp_path: Path) -> None:
    row = _row(_job(tmp_path, "remote"))
    row["resource_usage"] = {"status": "ok", "wall_seconds": float("inf"),
                             "average_cores": float("inf"), "peak_rss_mb": float("nan"),
                             "peak_rss_scope": "largest single process"}
    row["resource_observation"] = fleet_mod._resource_observation(
        row, fleet_mod._UsageObservation())
    view = calculations.calculation_view(row, now=NOW)
    assert view["runtime"]["source"] == "vq"
    assert view["runtime"]["seconds"] == 600.0
    assert view["observation"]["cpu_cores"] is None
    assert view["observation"]["rss_peak_mb"] is None


@pytest.mark.parametrize(("receipt", "label"), [
    (None, "no receipt"),
    ("partial JSON", "invalid receipt"),
    (json.dumps({"schema": final_usage.DIRECT_SCHEMA, "status": "error",
                 "collector": "posix-wait4", "error": "measurement failed"}), "collector error"),
])
def test_missing_or_bad_final_receipts_are_unknown_not_zero(
    tmp_path: Path, receipt: str | None, label: str,
) -> None:
    done = _job(tmp_path, "failed", state=JobState.FAILED)
    if receipt is not None:
        (Path(done.cwd) / "_vq/resource-usage.json").write_text(receipt)
    obs = calculations.observation_view(_row(done), now=NOW)
    assert obs["label"] == label
    assert obs["cpu_cores"] is obs["rss_mb"] is obs["rss_peak_mb"] is None


def test_batch_remote_listing_carries_final_usage_and_older_hosts_remain_unknown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    done = _job(tmp_path, "remote")
    _receipt(done)
    remote = _row(done)
    old = dict(remote, id="old")
    old.pop("resource_usage")
    reads = []

    def remote_vq(host, *args):
        reads.append(args)
        return SimpleNamespace(stdout=json.dumps([remote, old]))

    cfg = config.Config(hosts={"driver": config.HostConfig(ssh="driver.example.invalid")})
    monkeypatch.setattr(fleet_mod.transport, "run_remote_vq", remote_vq)
    queue = fleet_mod._observe_queue("driver", cfg, multi_user=False)
    assert queue.error is None and len(queue.rows) == 2
    rows, error = fleet_mod._gather_host_jobs(
        "driver", cfg, multi_user=False, scheduler_host_names=set(), observation=queue)
    assert error is None
    for row in rows:
        row["resource_observation"] = fleet_mod._resource_observation(
            row, fleet_mod._UsageObservation())
    assert calculations.observation_view(rows[0], now=NOW)["cpu_cores"] == 3.75
    assert calculations.observation_view(rows[1], now=NOW)["label"] == "unknown"
    assert reads == [("queue", "localhost", "--json")]


def test_jobs_table_and_api_show_final_usage_from_the_cached_listing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VQ_WEB_FLEET", "1")
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    done = _job(tmp_path, "done")
    _receipt(done)
    row = _row(done)
    snapshot = fleet_mod.FleetSnapshot(
        gathered_at=NOW.isoformat(), duration_seconds=1.0,
        hosts=[fleet_mod.HostSnapshot(host="localhost", overview=HostOverview(host="localhost"),
                                     jobs=[row])])
    client = TestClient(create_app())
    client.app.state.fleet_cache.set_snapshot(snapshot)
    monkeypatch.setattr(fleet_mod.transport, "run_remote_vq", lambda *a, **k: pytest.fail(
        "rendering cached final usage must not request individual job details"))
    html = client.get("/fleet/jobs").text
    assert "1536 <span class=\"hint\">process</span>" in html
    assert "3.8 <span class=\"hint\">avg</span>" in html
    assert "largest single process (ru_maxrss)" in html
    assert ">final</td>" in html and "0:02:00" in html
    fragment = client.get("/fleet/jobs/_table?sort=rss_peak")
    assert fragment.status_code == 200 and ">final</td>" in fragment.text
    payload = client.get("/api/v1/fleet/jobs").json()
    assert payload["jobs"][0]["resource_usage"]["peak_rss_mb"] == 1536.0
    assert payload["jobs"][0]["resource_observation"]["status"] == "final"
