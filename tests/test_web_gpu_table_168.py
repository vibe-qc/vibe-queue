"""#168: measured GPU use in the /fleet/jobs table, beside the allocation.

A running GPU job shows its newest sample; a finished one its summary, which
the owning host's queue listing carries as ``gpu_usage`` (no per-row request).
Both are device-level, and allocation is never shown as use.
"""
from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vq import cli, config, gpu_usage
from vq.spec import JobSpec, JobState
from vq.web import calculations
from vq.web import fleet as fleet_mod

NOW = datetime(2026, 10, 20, 12, tzinfo=UTC)
OBSERVED = int(NOW.timestamp()) - 45


def _device(index: int, util: float | None, used: float | None, *, job: bool = True
            ) -> dict[str, Any]:
    return {"index": index, "name": "NVIDIA Test GPU", "utilization_percent": util,
            "memory_used_mb": used, "memory_total_mb": 81920.0, "job_process": job,
            "job_memory_mb": 1000.0 if job else None}


def _live(*devices: dict[str, Any], observed: int = OBSERVED) -> dict[str, Any]:
    return {"schema": "vq.gpu-live/1", "observed_at": observed, "devices": list(devices)}


def _row(state: str, gpus: int | None, **fields: Any) -> dict[str, Any]:
    return {"id": f"j-{state}", "queue_host": "host_f", "state": state,
            "effective_state": state, "scheduler_target": "host_f", "cpus": 4,
            "gpus": gpus, "scheduler_running_confirmed": True, **fields}


SUMMARY = {"status": "ok", "collector": "nvidia-smi", "interval_seconds": 30, "samples": 20,
           "truncated": False, "errors": 0, "allocated": 2, "visible": 2,
           "coverage": "allocation", "devices": [
               {"index": 0, "name": "NVIDIA Test GPU", "memory_total_mb": 81920.0,
                "device_peak_memory_mb": 3072.0, "utilization_mean_percent": 80.0,
                "utilization_peak_percent": 100.0, "job_peak_memory_mb": 3000.0, "samples": 20},
               {"index": 1, "name": "NVIDIA Test GPU", "memory_total_mb": 81920.0,
                "device_peak_memory_mb": 5120.0, "utilization_mean_percent": 60.0,
                "utilization_peak_percent": 90.0, "job_peak_memory_mb": 5000.0, "samples": 20}]}


class TestView:
    def test_a_running_job_shows_one_moment(self) -> None:
        view = calculations.gpu_view(_row("running", 2, scheduler_gpu_live=_live(
            _device(0, 75.0, 2048.0), _device(1, None, 1024.0, job=False))), now=NOW)
        assert view is not None
        assert (view["status"], view["label"], view["utilization_percent"], view["memory_mb"],
                view["memory_total_mb"]) == ("live", "0:00:45 ago", 75.0, 3072.0, 163840.0)
        assert "device-level" in str(view["detail"]) and "(allocation)" in str(view["detail"])

    def test_an_unknown_memory_is_not_a_partial_sum(self) -> None:
        view = calculations.gpu_view(_row("running", 2, scheduler_gpu_live=_live(
            _device(0, 75.0, 2048.0), _device(1, 50.0, None))), now=NOW)
        assert view is not None and view["memory_mb"] is None and view["memory_total_mb"]
        no_total = _device(1, 50.0, 1024.0)
        no_total["memory_total_mb"] = None
        view = calculations.gpu_view(_row("running", 2, scheduler_gpu_live=_live(
            _device(0, 75.0, 2048.0), no_total)), now=NOW)
        assert view is not None and view["memory_mb"] == 3072.0
        assert view["memory_total_mb"] is None

    def test_an_old_sample_is_stale(self) -> None:
        old = _live(_device(0, 75.0, 2048.0), observed=OBSERVED - 3600)
        view = calculations.gpu_view(_row("running", 1, scheduler_gpu_live=old), now=NOW)
        assert view is not None and view["status"] == "stale"
        assert str(view["label"]).startswith("stale, ")
        edge = _live(_device(0, 75.0, 2048.0), observed=int(NOW.timestamp()) - 599)
        fresh = calculations.gpu_view(_row("running", 1, scheduler_gpu_live=edge), now=NOW)
        assert fresh is not None and fresh["status"] == "live"

    def test_nothing_attributable_says_why(self) -> None:
        view = calculations.gpu_view(_row("running", 1, scheduler_gpu_live=_live(
            _device(0, 75.0, 2048.0, job=False), _device(1, 10.0, 1.0, job=False))), now=NOW)
        assert view is not None and view["status"] == "unmeasured"
        assert "no process of this job is on one" in str(view["detail"])

    @pytest.mark.parametrize("row", [
        _row("running", None, scheduler_gpu_live=_live(_device(0, 75.0, 2048.0))),
        _row("running", 1),
        _row("pending", 1, scheduler_gpu_live=_live(_device(0, 75.0, 2048.0))),
        _row("completed", 2),  # an older owning host: no gpu_usage key
    ])
    def test_otherwise_there_is_no_figure(self, row: dict[str, Any]) -> None:
        assert calculations.gpu_view(row, now=NOW) is None

    def test_a_finished_job_shows_its_summary_peaks_never_added(self) -> None:
        view = calculations.gpu_view(_row("completed", 2, gpu_usage=SUMMARY), now=NOW)
        assert view is not None
        assert (view["status"], view["utilization_percent"], view["memory_mb"],
                view["memory_total_mb"]) == ("final", 70.0, 5120.0, None)
        unmeasured = calculations.gpu_view(_row("failed", 2, gpu_usage={
            "status": "unavailable", "reason": "nvidia-smi not found"}), now=NOW)
        assert unmeasured == {"status": "unmeasured", "label": "not measured",
                              "detail": "nvidia-smi not found"}
        invalid = calculations.gpu_view(_row("failed", 2, gpu_usage={
            "status": "invalid", "error": "line 3: malformed G row"}), now=NOW)
        assert invalid is not None and invalid["detail"] == "line 3: malformed G row"


def _gpu_job(root: Path, jobid: str, *, gpus: int | None, state: JobState) -> JobSpec:
    workspace = root / jobid
    (workspace / "_vq").mkdir(parents=True)
    (workspace / "_vq" / "gpu-samples.txt").write_text("\n".join([
        "V vq.gpu-samples/1 interval=30", "T 1",
        "G GPU-aaaa-1111, 0, 80, 3072, 81920, NVIDIA Test GPU",
        "P GPU-aaaa-1111, 7, 3000", "S 2"]) + "\n")
    return JobSpec(id=jobid, command=["true"], cwd=str(workspace), cpus=4, gpus=gpus,
                   state=state)


class TestRows:
    def test_the_owning_hosts_listing_carries_it_for_gpu_jobs_only(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cfg = config.Config(hosts={"localhost": config.HostConfig(ssh="localhost")})
        monkeypatch.setattr(config, "load_config", lambda: cfg)
        specs = [_gpu_job(tmp_path, "gpu-done", gpus=1, state=JobState.COMPLETED),
                 _gpu_job(tmp_path, "gpu-running", gpus=1, state=JobState.RUNNING),
                 _gpu_job(tmp_path, "cpu-done", gpus=None, state=JobState.COMPLETED),
                 _gpu_job(tmp_path, "gpu-unsummarised", gpus=1, state=JobState.COMPLETED)]
        gpu_usage.write_summary(Path(specs[0].cwd), 1)  # as the fetch worker does
        monkeypatch.setattr(cli, "list_jobs", lambda *a, **k: specs)
        # A listing reads only recorded summaries: parsing samples here would
        # cost every refresh a full read of every finished GPU job's samples.
        monkeypatch.setattr(gpu_usage, "_summarise_file", lambda *a: pytest.fail(
            "a listing parsed raw GPU samples"))
        result = CliRunner().invoke(cli.main, ["queue", "localhost", "--json"])
        assert result.exit_code == 0, result.output
        rows = {row["id"]: row for row in json.loads(result.output)}
        assert rows["gpu-done"]["gpu_usage"]["devices"][0]["job_peak_memory_mb"] == 3000.0
        assert rows["gpu-running"]["gpu_usage"] is None  # old samples are never live use
        assert "gpu_usage" not in rows["cpu-done"]
        assert rows["gpu-unsummarised"]["gpu_usage"] is None

        local = fleet_mod._job_row(specs[0], "localhost")
        assert local["gpu_usage"] == rows["gpu-done"]["gpu_usage"]
        assert fleet_mod._job_row(specs[3], "localhost")["gpu_usage"] is None
        assert "gpu_usage" not in fleet_mod._job_row(specs[2], "localhost")

    def test_a_scheduler_projection_keeps_the_drivers_reading(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cfg = config.Config(hosts={
            "driver": config.HostConfig(ssh="driver.example.invalid"),
            "lane": config.HostConfig(ssh="login.example.invalid", scheduler="slurm",
                                      scheduler_dialect="slurm", scheduler_driver="driver",
                                      scratch_root="/tmp/scratch")})
        monkeypatch.setattr(config, "load_config", lambda: cfg)
        remote = dict(_gpu_job(tmp_path, "gpu-done", gpus=2,
                               state=JobState.COMPLETED).model_dump(mode="json"),
                      scheduler_target="lane", gpu_usage=SUMMARY)
        monkeypatch.setattr(cli, "_delegate_to_remote", lambda *a: json.dumps([remote]))
        monkeypatch.setattr(gpu_usage, "read_gpu_usage", lambda *a: pytest.fail(
            "a projection must keep the driver's reading, never read its cwd here"))
        result = CliRunner().invoke(cli.main, ["queue", "lane", "--json"])
        assert result.exit_code == 0, result.output
        (row,) = json.loads(result.output)
        assert row["gpu_usage"] == SUMMARY


def test_the_table_shows_measured_use_beside_the_allocation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fastapi.testclient import TestClient

    from vq import auth, paths
    from vq.overview import HostOverview
    from vq.web import create_app

    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
    monkeypatch.setenv("VQ_WEB_FLEET", "1")
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    live = _live(_device(0, 75.0, 2048.0), _device(1, 25.0, 1024.0),
                 observed=int(datetime.now(UTC).timestamp()) - 30)
    rows = [_row("running", 2, id="live", scheduler_gpu_live=live),
            _row("completed", 2, id="done", gpu_usage=SUMMARY),
            _row("failed", 1, id="none", gpu_usage={"status": "unavailable",
                                                    "reason": "nvidia-smi not found"}),
            _row("running", None, id="cpu")]
    app = create_app()
    app.state.fleet_cache.set_snapshot(fleet_mod.FleetSnapshot(
        gathered_at=datetime.now(UTC).isoformat(), duration_seconds=1.0,
        hosts=[fleet_mod.HostSnapshot(host="host_f", overview=HostOverview(host="host_f"),
                                      jobs=rows)]))
    html = " ".join(TestClient(app).get("/fleet/jobs/_table").text.split())
    assert ">+2 gpu</span>" in html  # the allocation, unchanged
    # The page's own clock reads the age, so a second may tick over.
    assert re.search(r">50% · 3\.0/160 GiB · 0:00:3[01] ago</span>", html)
    assert ">70% · 5.0 GiB peak</span>" in html
    assert 'title="nvidia-smi not found">gpu not measured</span>' in html
    assert html.count('class="hint gpu-use"') == 3  # never for a CPU job
    assert "vq has no GPU collector" not in html
    assert "A GPU job's own row adds its measured use" in html
    # Use never enters the allocation tile.
    assert '<span class="tile-value">2</span><span class="tile-label">gpus allocated</span>' \
        in html
