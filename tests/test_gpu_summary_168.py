"""#168: a finished GPU job's summary, recorded once; GPU arrays.

The driver's fetch worker writes ``_vq/gpu-usage.json`` once it has fetched a
finished GPU job's workspace, so listings (run on every console refresh)
read a small validated record instead of parsing raw samples. A record whose
samples have since changed is ignored. A scheduler array's elements are each
summarised on their own and listed together. The decisive case runs the
daemon's real scheduler path on a local stand-in for the cluster.
"""
from __future__ import annotations

import json
import os
import sys
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from vq import cli, gpu_usage, paths
from vq import submit as submit_module
from vq.daemon import Daemon
from vq.gpu_usage import format_gpu_usage, read_gpu_usage, summarise, write_summary
from vq.scheduler_dialect import TorqueDialect
from vq.scheduler_dispatch import SchedulerDispatcher
from vq.spec import JobSpec, JobState
from vq.web import calculations

sys.path.insert(0, str(Path(__file__).parent))
from test_gpu_usage_168 import _fake_nvidia_smi  # noqa: E402
from test_runtime_binding_scheduler_161 import LocalCluster  # noqa: E402
from test_scheduler_dispatch import _install_fake_gnu_time  # noqa: E402

HEADER = "V vq.gpu-samples/1 interval=30"


def _rows(uuid: str, index: int, util: int, mem: int, job_mem: int | None = 900) -> list[str]:
    rows = [f"G {uuid}, {index}, {util}, {mem}, 81920, NVIDIA Test GPU"]
    if job_mem is not None:
        rows.append(f"P {uuid}, 7, {job_mem}")
    return rows


def _write(workspace: Path, name: str, *lines: str) -> Path:
    (workspace / "_vq").mkdir(parents=True, exist_ok=True)
    path = workspace / "_vq" / name
    path.write_text("\n".join(lines) + "\n")
    return path


def _single(tmp_path: Path) -> Path:
    workspace = tmp_path / "ws"
    _write(workspace, "gpu-samples.txt", HEADER, "T 1", *_rows("GPU-aaaa-1111", 0, 80, 3072),
           "T 31", *_rows("GPU-aaaa-1111", 0, 60, 2048), "S 40")
    return workspace


class TestRecordedSummary:
    def test_it_is_written_once_and_read_without_parsing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        workspace = _single(tmp_path)
        expected = summarise(workspace, 1)
        write_summary(workspace, 1)
        record = json.loads((workspace / "_vq" / "gpu-usage.json").read_text())
        assert record["schema"] == "vq.gpu-usage/1" and record["usage"] == expected
        monkeypatch.setattr(gpu_usage, "_summarise_file", lambda *a: pytest.fail("parsed"))
        assert read_gpu_usage(workspace, 1) == expected
        assert read_gpu_usage(workspace, 1, summary_only=True) == expected

    def test_changed_samples_make_it_stale(self, tmp_path: Path) -> None:
        workspace = _single(tmp_path)
        write_summary(workspace, 1)
        with (workspace / "_vq" / "gpu-samples.txt").open("a") as samples:
            samples.write("T 61\n" + "\n".join(_rows("GPU-aaaa-1111", 0, 100, 4096)) + "\n")
        assert read_gpu_usage(workspace, 1, summary_only=True) is None
        fresh = read_gpu_usage(workspace, 1)  # one job: parse the samples instead
        assert fresh is not None and fresh["devices"][0]["device_peak_memory_mb"] == 4096.0

    def test_a_new_element_file_makes_it_stale(self, tmp_path: Path) -> None:
        workspace = tmp_path / "array"
        _write(workspace, "gpu-samples.txt.0", HEADER, "T 1", *_rows("GPU-aaaa-1111", 0, 80, 1),
               "S 2")
        write_summary(workspace, 1)
        assert read_gpu_usage(workspace, 1, summary_only=True) is not None
        _write(workspace, "gpu-samples.txt.1", HEADER, "T 1", *_rows("GPU-bbbb-2222", 0, 9, 1),
               "S 2")
        assert read_gpu_usage(workspace, 1, summary_only=True) is None

    @pytest.mark.parametrize("mutate", [
        lambda record: {**record, "schema": "vq.gpu-usage/0"},
        lambda record: {**record, "usage": {**record["usage"], "status": "great"}},
        lambda record: {**record, "usage": {**record["usage"], "coverage": "everything"}},
        lambda record: {**record, "usage": {**record["usage"], "samples": -1}},
        lambda record: {**record, "usage": {**record["usage"], "devices": [
            {**record["usage"]["devices"][0], "utilization_mean_percent": float("nan")}]}},
        lambda record: {**record, "usage": {**record["usage"], "devices": [
            {**record["usage"]["devices"][0], "job_peak_memory_mb": float("inf")}]}},
        lambda record: {**record, "usage": {**record["usage"], "devices": [
            {**record["usage"]["devices"][0], "index": "0"}]}},
        lambda record: {**record, "usage": {**record["usage"], "devices": [
            {**record["usage"]["devices"][0], "name": "x" * 129}]}},
        lambda record: {**record, "usage": {**record["usage"], "devices": "all"}},
        lambda record: {**record, "usage": {"status": "unavailable"}},
        lambda record: {**record, "usage": {**record["usage"], "interval_seconds": "30"}},
        lambda record: {**record, "usage": {**record["usage"], "errors": None}},
        lambda record: {**record, "usage": {**record["usage"], "devices": [
            {**record["usage"]["devices"][0], "index": -1}]}},
        lambda record: {**record, "usage": {**record["usage"], "devices": [
            {**record["usage"]["devices"][0], "device_peak_memory_mb": -2048.0}]}},
        lambda record: [record],
    ])
    def test_a_record_of_another_shape_is_ignored(self, tmp_path: Path, mutate: Any) -> None:
        workspace = _single(tmp_path)
        write_summary(workspace, 1)
        summary = workspace / "_vq" / "gpu-usage.json"
        summary.write_text(json.dumps(mutate(json.loads(summary.read_text())),
                                      allow_nan=True))
        assert read_gpu_usage(workspace, 1, summary_only=True) is None
        assert read_gpu_usage(workspace, 1) == summarise(workspace, 1)

    def test_a_link_or_an_oversized_record_is_ignored(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        workspace = _single(tmp_path)
        write_summary(workspace, 1)
        summary = workspace / "_vq" / "gpu-usage.json"
        elsewhere = tmp_path / "elsewhere.json"
        summary.rename(elsewhere)
        summary.symlink_to(elsewhere)
        assert read_gpu_usage(workspace, 1, summary_only=True) is None
        summary.unlink()
        elsewhere.rename(summary)
        # A whole record padded past the cap: the part read would still parse.
        size = summary.stat().st_size
        with summary.open("a") as record:
            record.write(" " * 100)
        monkeypatch.setattr(gpu_usage, "MAX_SUMMARY_BYTES", size + 10)
        assert read_gpu_usage(workspace, 1, summary_only=True) is None

    def test_a_record_with_too_many_devices_is_ignored(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        workspace = _single(tmp_path)
        write_summary(workspace, 1)
        monkeypatch.setattr(gpu_usage, "MAX_DEVICES", 0)
        assert read_gpu_usage(workspace, 1, summary_only=True) is None

    def test_nothing_is_written_without_samples(self, tmp_path: Path) -> None:
        (tmp_path / "ws" / "_vq").mkdir(parents=True)
        write_summary(tmp_path / "ws", 1)
        assert not (tmp_path / "ws" / "_vq" / "gpu-usage.json").exists()


def _array(tmp_path: Path, *elements: list[str]) -> Path:
    workspace = tmp_path / "array"
    for index, rows in enumerate(elements):
        _write(workspace, f"gpu-samples.txt.{index}", HEADER, "T 1", *rows, "T 31", "S 40")
    return workspace


class TestArrays:
    def test_each_element_is_summarised_on_its_own_node(self, tmp_path: Path) -> None:
        workspace = _array(tmp_path, _rows("GPU-aaaa-1111", 0, 80, 3072),
                           _rows("GPU-bbbb-2222", 0, 40, 6144))
        usage = read_gpu_usage(workspace, 1)
        assert usage is not None and usage["status"] == "ok"
        assert (usage["coverage"], usage["visible"], usage["samples"]) == ("allocation", 2, 4)
        assert usage["elements"] == [{"index": 0, "status": "ok", "coverage": "allocation"},
                                     {"index": 1, "status": "ok", "coverage": "allocation"}]
        assert [(d["element"], d["index"], d["device_peak_memory_mb"])
                for d in usage["devices"]] == [(0, 0, 3072.0), (1, 0, 6144.0)]
        assert format_gpu_usage(usage) == (
            "2 array elements, 2 devices: utilisation mean 60%, largest device memory peak "
            "6144 MB of 81920 MB (element 1, GPU 0) (device-level, the allocated devices, "
            "4 samples every 30 s)")
        view = calculations.gpu_view({"state": "completed", "gpus": 1, "gpu_usage": usage},
                                     now=datetime.now(UTC))
        assert view is not None and (view["utilization_percent"], view["memory_mb"]) == (
            60.0, 6144.0)

    def test_elements_with_different_coverage_are_mixed(self, tmp_path: Path) -> None:
        workspace = _array(tmp_path, _rows("GPU-aaaa-1111", 0, 80, 3072),
                           [*_rows("GPU-bbbb-2222", 0, 40, 6144),
                            *_rows("GPU-cccc-3333", 1, 1, 1, job_mem=None)],
                           _rows("GPU-dddd-4444", 0, 1, 1, job_mem=None)
                           + _rows("GPU-eeee-5555", 1, 1, 1, job_mem=None))
        usage = read_gpu_usage(workspace, 1)
        assert usage is not None and usage["coverage"] == "mixed"
        assert [e.get("coverage", e.get("reason")) for e in usage["elements"]] == [
            "allocation", "attributed",
            "the node shows 2 GPUs for an allocation of 1, and no process of this job was "
            "seen on one"]
        assert [d["element"] for d in usage["devices"]] == [0, 1]
        assert "per element, which differs between elements" in str(format_gpu_usage(usage))

    def test_leftover_files_beside_the_elements_are_not_elements(self, tmp_path: Path) -> None:
        workspace = _array(tmp_path, _rows("GPU-aaaa-1111", 0, 80, 3072))
        _write(workspace, "gpu-samples.txt.apps", "GPU-aaaa-1111, 7, 900")
        usage = read_gpu_usage(workspace, 1)
        assert usage is not None and usage["status"] == "ok" and len(usage["elements"]) == 1

    def test_one_invalid_element_makes_the_whole_summary_invalid(self, tmp_path: Path) -> None:
        workspace = _array(tmp_path, _rows("GPU-aaaa-1111", 0, 80, 3072),
                           ["G GPU-bbbb-2222, 0, -5, 1, 1, bad"])
        usage = read_gpu_usage(workspace, 1)
        assert usage is not None and usage["status"] == "invalid"
        assert usage["error"].startswith("element 1: ")

    def test_no_measured_element_is_unavailable(self, tmp_path: Path) -> None:
        workspace = tmp_path / "array"
        for index in range(2):
            _write(workspace, f"gpu-samples.txt.{index}", HEADER, "U nvidia-smi not found")
        assert read_gpu_usage(workspace, 1) == {
            "status": "unavailable", "elements": 2,
            "reason": "element 0: nvidia-smi not found"}

    @pytest.mark.parametrize(("bound", "value", "message"), [
        ("MAX_ARRAY_ELEMENTS", 1, "a scheduler array of 2 elements is not summarised"),
        ("MAX_ARRAY_BYTES", 100, "larger than 100 bytes"),
    ])
    def test_a_large_array_is_not_summarised(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, bound: str, value: int,
        message: str,
    ) -> None:
        monkeypatch.setattr(gpu_usage, bound, value)
        workspace = _array(tmp_path, _rows("GPU-aaaa-1111", 0, 80, 3072),
                           _rows("GPU-bbbb-2222", 0, 40, 6144))
        usage = read_gpu_usage(workspace, 1)
        assert usage is not None and usage["status"] == "unavailable"
        assert message in usage["reason"]

    def test_the_drill_down_lists_each_elements_devices(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from fastapi.testclient import TestClient

        from vq import auth, config
        from vq.web import create_app
        from vq.web import fleet as fleet_mod

        usage = read_gpu_usage(_array(tmp_path, _rows("GPU-aaaa-1111", 0, 80, 3072),
                                      [*_rows("GPU-bbbb-2222", 0, 40, 6144),
                                       *_rows("GPU-cccc-3333", 1, 1, 1, job_mem=None)]), 1)
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
        monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
        monkeypatch.setenv("VQ_WEB_FLEET", "1")
        paths.queue_dir().mkdir(parents=True, exist_ok=True)
        status = JobSpec(id="abc", command=["run"], cwd=str(tmp_path), cpus=4, gpus=1,
                         state=JobState.COMPLETED, exit_code=0).model_dump(mode="json")
        status["gpu_usage"] = usage
        detail = {"status": status, "events": [], "samples": [], "owner_host": "localhost"}
        monkeypatch.setattr(fleet_mod, "fetch_job_detail", lambda *a, **k: (detail, None))
        monkeypatch.setattr(fleet_mod, "fetch_job_preview", lambda *a, **k: (None, None))
        html = " ".join(TestClient(create_app()).get("/fleet/jobs/abc?host=localhost").text.split())
        assert "<th>element</th><th>gpu</th>" in html
        assert "<td>1</td><td>0</td><td>NVIDIA Test GPU</td>" in html
        assert ("Each of the 2 array elements is summarised on its own node, and which "
                "devices are listed differs between them: element 0 allocation; element 1 "
                "attributed.") in html


@pytest.fixture
def daemon(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Daemon]:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    queue = Daemon(max_cpus=4, poll_interval=0.05, queue_dir=tmp_path / "queue",
                   jobs_dir=tmp_path / "jobs")
    queue.queue_dir.mkdir(parents=True, exist_ok=True)
    queue.jobs_dir.mkdir(parents=True, exist_ok=True)
    yield queue


@pytest.mark.parametrize("summary_fails", [False, True])
def test_end_to_end_the_fetch_worker_records_the_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, daemon: Daemon, summary_fails: bool,
) -> None:
    monkeypatch.setattr(gpu_usage, "SAMPLE_INTERVAL_SECONDS", 1)
    monkeypatch.setenv("PATH", f"{_fake_nvidia_smi(tmp_path)}:{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_OTHER_PID", str(os.getpid()))
    if summary_fails:
        def refuse(*_a: object) -> None:
            raise OSError("read-only workspace")
        monkeypatch.setattr(gpu_usage, "write_summary", refuse)
    cluster = LocalCluster()
    dispatcher = SchedulerDispatcher(
        TorqueDialect(), cluster, scratch_root=str(tmp_path / "scratch"),  # type: ignore[arg-type]
        gnu_time_command=str(_install_fake_gnu_time(tmp_path, monkeypatch)))
    daemon._scheduler_dispatchers["cluster"] = dispatcher
    payload = tmp_path / "payload"
    payload.mkdir()
    (payload / "README").write_text("calculation\n")
    jobid = submit_module.submit_local(
        host="localhost", directory=str(payload), scheduler_target="cluster", gpus=2,
        command=["sh", "-c", "echo $$ > gpu.pid; sleep 2.5"],
        queue_dir=daemon.queue_dir, jobs_dir=daemon.jobs_dir)
    monkeypatch.setenv("FAKE_GPU_PIDFILE",
                       str(Path(dispatcher.remote_workspace(jobid)) / "gpu.pid"))
    try:
        daemon._dispatch_pending()
        cluster.wait()
        deadline = time.monotonic() + 30
        while JobSpec.read(daemon._spec_path(jobid)).state != JobState.COMPLETED:
            assert time.monotonic() < deadline
            daemon._reconcile_scheduler()
            time.sleep(0.05)
    finally:
        cluster.stop()
    spec = JobSpec.read(daemon._spec_path(jobid))
    summary = Path(spec.cwd) / "_vq" / "gpu-usage.json"
    assert summary.exists() is not summary_fails  # a failed write never fails the job
    row = cli._queue_json_row(spec, "localhost", include_usage=True)
    if summary_fails:
        assert row["gpu_usage"] is None
    else:
        assert row["gpu_usage"]["devices"][0]["job_peak_memory_mb"] == 1234.0
