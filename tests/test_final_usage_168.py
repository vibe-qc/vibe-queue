"""#168: a finished job's measured usage, from its terminal receipt.

`vq status` (text and JSON) and the fleet console's job drill-down show
the wall time, CPU time and peak RSS the job's collector measured: the
direct `posix-wait4` receipt or the scheduler's GNU time receipt, a
scheduler array's element receipts combined. A receipt that cannot be
trusted (a link, oversized, malformed, a negative or non-finite value) is
reported as invalid instead of shown, and a running job shows none.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from vq import final_usage
from vq.spec import JobSpec, JobState
from vq.status import show_status, show_status_json
from vq.web import view_context


def _scheduler_receipt(wall: float, cpu: float, peak_mb: float, rc: int = 0) -> dict[str, Any]:
    return {
        "schema": final_usage.SCHEDULER_SCHEMA, "status": "ok", "collector": "gnu-time",
        "scope": "effective-command", "command_status": "succeeded" if rc == 0 else "failed",
        "command_exit_code": rc, "wall_seconds": wall, "user_cpu_seconds": cpu,
        "system_cpu_seconds": 0.0, "active_cpu_seconds": cpu,
        "peak_rss_kb": int(peak_mb * 1024), "peak_rss_mb": peak_mb,
    }


def _vq_dir(workspace: Path) -> Path:
    directory = workspace / "_vq"
    directory.mkdir(parents=True, exist_ok=True)
    return directory


class TestReader:
    def test_a_real_direct_receipt_reads_as_measured(self, tmp_path: Path) -> None:
        directory = _vq_dir(tmp_path)
        script = "x = bytearray(48 * 1024 * 1024); sum(range(2_000_000))"
        subprocess.run(
            [sys.executable, "-m", "vq.resource_receipt",
             "--receipt", str(directory / "resource-usage.json"),
             "--exit-marker", str(directory / "exit-code"),
             "--", sys.executable, "-c", script],
            check=True, timeout=60,
        )
        usage = final_usage.read_final_usage(tmp_path)
        assert usage is not None
        assert (usage["status"], usage["collector"], usage["receipts"]) == (
            "ok", "posix-wait4", 1)
        assert usage["command_exit_code"] == 0
        assert usage["wall_seconds"] > 0 and usage["active_cpu_seconds"] > 0
        assert usage["peak_rss_mb"] >= 48  # the 48 MiB the command allocated
        assert usage["peak_rss_scope"] == "largest single process (ru_maxrss)"
        assert usage["average_cores"] is not None

    def test_a_collector_error_is_not_a_measurement(self, tmp_path: Path) -> None:
        (_vq_dir(tmp_path) / "resource-usage.json").write_text(json.dumps({
            "schema": "vq.direct-resource-usage.v1", "status": "error",
            "collector": "posix-wait4", "error": "fork failed", "wall_seconds": None,
        }))
        assert final_usage.read_final_usage(tmp_path) == {
            "status": "error", "collector": "posix-wait4", "error": "fork failed",
            "receipts": 1}

    def test_scheduler_array_elements_combine(self, tmp_path: Path) -> None:
        directory = _vq_dir(tmp_path)
        for index, (wall, cpu, peak) in enumerate([(100.0, 380.0, 900.0),
                                                   (120.0, 450.0, 1200.0),
                                                   (80.0, 300.0, 700.0)]):
            (directory / f"resource-usage.json.{index}").write_text(
                json.dumps(_scheduler_receipt(wall, cpu, peak)))
        (directory / "resource-usage.json.tmp.123").write_text("partial")
        usage = final_usage.read_final_usage(tmp_path)
        assert usage is not None
        assert (usage["collector"], usage["receipts"]) == ("gnu-time", 3)
        assert usage["wall_seconds"] == 120.0  # the largest element
        assert usage["active_cpu_seconds"] == 1130.0  # summed
        assert usage["peak_rss_mb"] == 1200.0  # the largest element
        assert usage["average_cores"] == round(1130.0 / 300.0, 3)
        assert usage["command_exit_code"] is None

    def test_no_receipt_is_none(self, tmp_path: Path) -> None:
        assert final_usage.read_final_usage(tmp_path) is None
        _vq_dir(tmp_path)
        assert final_usage.read_final_usage(tmp_path) is None

    @pytest.mark.parametrize(("content", "reason"), [
        ("not json", "is not JSON"),
        (json.dumps({"schema": "something-else", "status": "ok"}),
         "is not a vq resource-usage receipt"),
        (json.dumps({**_scheduler_receipt(10.0, 5.0, 100.0), "peak_rss_mb": -1}),
         "peak_rss_mb is not a non-negative number"),
        ('{"schema": "vq.scheduler-resource-usage.v1", "status": "ok", '
         '"collector": "gnu-time", "wall_seconds": NaN}', "wall_seconds is not a non-negative"),
        (json.dumps({**_scheduler_receipt(10.0, 5.0, 100.0), "collector": "made-up"}),
         "unknown collector"),
        ("x" * (final_usage.MAX_RECEIPT_BYTES + 1), "is larger than"),
    ])
    def test_untrustworthy_receipts_are_invalid(
        self, tmp_path: Path, content: str, reason: str,
    ) -> None:
        (_vq_dir(tmp_path) / "resource-usage.json").write_text(content)
        usage = final_usage.read_final_usage(tmp_path)
        assert usage is not None and usage["status"] == "invalid"
        assert reason in usage["error"]

    def test_a_linked_receipt_is_not_followed(self, tmp_path: Path) -> None:
        elsewhere = tmp_path / "elsewhere.json"
        elsewhere.write_text(json.dumps(_scheduler_receipt(10.0, 5.0, 100.0)))
        os.symlink(elsewhere, _vq_dir(tmp_path / "ws") / "resource-usage.json")
        usage = final_usage.read_final_usage(tmp_path / "ws")
        assert usage is not None and usage["status"] == "invalid"
        assert "cannot be opened" in usage["error"]


def _job(tmp_path: Path, state: JobState, receipt: dict[str, Any] | None) -> Path:
    queue = tmp_path / "queue"
    queue.mkdir(parents=True)
    workspace = tmp_path / "ws"
    workspace.mkdir()
    (workspace / "stdout.log").write_text("")
    (workspace / "stderr.log").write_text("")
    if receipt is not None:
        (_vq_dir(workspace) / "resource-usage.json").write_text(json.dumps(receipt))
    terminal = state in {JobState.COMPLETED, JobState.FAILED}
    JobSpec(id="abc", command=["run"], cwd=str(workspace), cpus=4, mem_mb=2048, state=state,
            started_at="2026-10-14T10:00:00+00:00",
            finished_at="2026-10-14T10:02:00+00:00" if terminal else None,
            exit_code=0 if terminal else None).write(queue / "abc.json")
    return queue


class TestStatus:
    def test_a_finished_job_reports_its_usage(self, tmp_path: Path) -> None:
        queue = _job(tmp_path, JobState.COMPLETED, _scheduler_receipt(3723.0, 14000.0, 1536.0))
        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))
        assert payload["resource_usage"]["status"] == "ok"
        assert payload["resource_usage"]["peak_rss_mb"] == 1536.0
        text = show_status("localhost", "abc", queue_dir=queue)
        assert ("usage:        wall 1:02:03 · cpu 3:53:20 (3.76 cores avg) · peak rss "
                "1536 MB (largest single process (GNU time maximum resident set))") in text

    def test_a_running_job_reports_none_even_beside_a_stale_receipt(self, tmp_path: Path) -> None:
        queue = _job(tmp_path, JobState.RUNNING, _scheduler_receipt(10.0, 5.0, 100.0))
        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))
        assert "resource_usage" in payload and payload["resource_usage"] is None
        assert "usage:" not in show_status("localhost", "abc", queue_dir=queue)

    def test_a_finished_job_without_a_receipt_reports_null(self, tmp_path: Path) -> None:
        queue = _job(tmp_path, JobState.FAILED, None)
        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))
        assert payload["resource_usage"] is None


class TestConsole:
    def _detail(self, state: str, **status: object) -> dict[str, object]:
        return {"status": {"state": state, "cpus": 4, "mem_mb": 2048, **status}}

    def test_an_older_host_or_a_running_job_shows_no_section(self) -> None:
        assert view_context.final_usage_view(self._detail("completed")) is None
        assert view_context.final_usage_view(
            self._detail("running", resource_usage=None)) is None
        assert view_context.final_usage_view(None) is None

    def test_views_for_each_receipt_state(self) -> None:
        absent = view_context.final_usage_view(self._detail("killed", resource_usage=None))
        assert absent == {"status": "absent"}
        broken = view_context.final_usage_view(self._detail(
            "failed", resource_usage={"status": "invalid", "error": "is not JSON"}))
        assert broken == {"status": "invalid", "error": "is not JSON"}
        ok = view_context.final_usage_view(self._detail("completed", resource_usage={
            "status": "ok", "collector": "posix-wait4", "receipts": 1, "wall_seconds": 3723.0,
            "active_cpu_seconds": 14000.0, "average_cores": 3.761, "peak_rss_mb": 1536.0,
            "peak_rss_scope": "largest single process (ru_maxrss)", "process_count": 3}))
        assert ok is not None
        assert (ok["wall"], ok["cpu"], ok["peak_share"]) == ("1:02:03", "3:53:20", 75.0)

    def test_the_drill_down_renders_the_section(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from fastapi.testclient import TestClient

        from vq import auth, config, paths
        from vq.web import create_app
        from vq.web import fleet as fleet_mod

        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
        monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
        monkeypatch.setenv("VQ_WEB_FLEET", "1")
        paths.queue_dir().mkdir(parents=True, exist_ok=True)
        status = JobSpec(id="abc", command=["run"], cwd=str(tmp_path), cpus=4, mem_mb=2048,
                         state=JobState.COMPLETED, exit_code=0).model_dump(mode="json")
        status["resource_usage"] = {
            "status": "ok", "collector": "gnu-time", "receipts": 3, "wall_seconds": 120.0,
            "active_cpu_seconds": 1130.0, "average_cores": 3.767, "peak_rss_mb": 1200.0,
            "peak_rss_scope": "largest single process (GNU time maximum resident set)",
            "process_count": None}
        detail = {"status": status, "events": [], "samples": [], "owner_host": "localhost"}
        monkeypatch.setattr(fleet_mod, "fetch_job_detail", lambda *a, **k: (detail, None))
        monkeypatch.setattr(fleet_mod, "fetch_job_preview", lambda *a, **k: (None, None))
        html = TestClient(create_app()).get("/fleet/jobs/abc?host=localhost").text
        assert "final resource usage" in html
        assert "3.767 cores on average of 4 declared" in html
        assert "1200 MB (58.6% of 2048 MB declared)" in html
        assert "over 3 array elements" in html
