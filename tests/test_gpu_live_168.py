"""#168: a running scheduler GPU job's newest GPU sample, live.

The daemon's detail poll reads the end of each running GPU job's samples
file in one bounded remote command, takes the newest *complete* sample and
records it on the job. Only jobs that requested GPUs are read, so a target
without one makes no extra call. The decisive case runs the daemon's real
poll on a local stand-in for the cluster while the job is still running.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from vq import daemon as daemon_mod
from vq import gpu_usage, paths
from vq import submit as submit_module
from vq.daemon import Daemon
from vq.gpu_usage import format_gpu_live, latest_sample, live_view, parse_live_read
from vq.scheduler_dialect import QstatDetail, SchedulerPhase, TorqueDialect
from vq.scheduler_dispatch import RemoteResult, SchedulerDispatcher, SchedulerHandle
from vq.spec import JobSpec, JobState
from vq.status import show_status
from vq.web import view_context

sys.path.insert(0, str(Path(__file__).parent))
from test_daemon_scheduler import (  # noqa: E402
    MockDispatcher,
    _inject,
    _running_scheduler_spec,
    _submit_scheduler,
)
from test_gpu_usage_168 import _fake_nvidia_smi  # noqa: E402
from test_runtime_binding_scheduler_161 import LocalCluster  # noqa: E402
from test_scheduler_dispatch import _install_fake_gnu_time  # noqa: E402

HEADER = "V vq.gpu-samples/1 interval=30"
A = "G GPU-aaaa-1111, 0, 50, 4000, 81920, NVIDIA Test GPU"
B = "G GPU-bbbb-2222, 1, [N/A], 7000, 81920, NVIDIA Test GPU"


def _block(t: int, *rows: str) -> list[str]:
    return [f"T {t}", *rows]


class TestLatestSample:
    def test_the_sample_being_written_is_never_read(self) -> None:
        text = "\n".join([HEADER, *_block(100, A, "P GPU-aaaa-1111, 7, 1000"),
                          *_block(130, A.replace(", 50,", ", 90,"), B,
                                  "P GPU-aaaa-1111, 7, 1500", "P GPU-aaaa-1111, 8, 25"),
                          *_block(160, A.replace(", 50,", ", 10,"))])  # still being written
        sample = latest_sample(text)
        assert sample is not None and sample["observed_at"] == 130
        first, second = sample["devices"]
        assert (first["utilization_percent"], first["job_memory_mb"], first["job_process"]) == (
            90.0, 1525.0, True)
        assert (second["utilization_percent"], second["job_memory_mb"],
                second["job_process"]) == (None, None, False)

    @pytest.mark.parametrize("end", ["S 140", "X sample cap reached"])
    def test_a_stop_or_cap_line_completes_the_last_sample(self, end: str) -> None:
        sample = latest_sample("\n".join([HEADER, *_block(100, A), end]))
        assert sample is not None and sample["observed_at"] == 100

    def test_a_tail_cut_mid_line_starts_at_the_first_sample(self) -> None:
        text = "\n".join(["0, 81920, NVIDIA Test GPU", "P GPU-aaaa-1111, 7, 5",
                          *_block(100, A), *_block(130, A)])
        sample = latest_sample(text)
        assert sample is not None and sample["observed_at"] == 100

    @pytest.mark.parametrize("text", [
        "\n".join([HEADER, *_block(100, A)]),  # one sample, maybe unfinished
        "\n".join([HEADER, *_block(100, "G GPU-aaaa-1111, 0, -1, 1, 2, x"), "T 130"]),
        "\n".join([HEADER, *_block(100, "E query-gpu failed"), "T 130"]),
        "\n".join([HEADER, "T soon", A, "T 130"]),
        "",
    ])
    def test_otherwise_there_is_no_live_figure(self, text: str) -> None:
        assert latest_sample(text) is None

    def test_an_unknown_process_memory_is_unknown(self) -> None:
        sample = latest_sample("\n".join([HEADER, *_block(
            100, A, "P GPU-aaaa-1111, 7, 1000", "P GPU-aaaa-1111, 8, [N/A]"), "T 130"]))
        assert sample is not None and sample["devices"][0]["job_memory_mb"] is None

    def test_devices_with_one_index_keep_one_order(self) -> None:
        sample = latest_sample("\n".join([HEADER, *_block(
            100, B.replace(", 1, ", ", 0, "), A), "T 130"]))
        assert sample is not None
        assert [d["memory_used_mb"] for d in sample["devices"]] == [4000.0, 7000.0]


class TestRemoteRead:
    def test_the_command_reads_each_file_once_and_bounded(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(gpu_usage, "LIVE_TAIL_BYTES", 200)
        long = tmp_path / "long.txt"
        long.write_text("\n".join([HEADER, *(line for t in range(100, 100 + 30 * 200, 30)
                                             for line in _block(t, A)), ""]))
        assert long.stat().st_size > 10_000
        short = tmp_path / "short.txt"
        short.write_text("\n".join([HEADER, *_block(100, A), *_block(130, B), ""]))
        missing = tmp_path / "missing.txt"
        command = gpu_usage.live_read_command([str(long), str(short), str(missing)])
        done = subprocess.run(command, capture_output=True, text=True, check=True)
        assert len(done.stdout.encode()) < 3 * 200 + 1024  # each file's tail only
        found = parse_live_read(done.stdout)
        assert set(found) == {str(long), str(short)}
        assert found[str(long)]["observed_at"] == 6040  # 6070 may still be being written
        assert found[str(short)]["observed_at"] == 100


class TestView:
    def _live(self, *devices: dict[str, Any]) -> dict[str, Any]:
        return {"schema": "vq.gpu-live/1", "observed_at": 1_792_000_000,
                "devices": list(devices)}

    def _device(self, index: int, *, job: bool, util: float | None = 50.0) -> dict[str, Any]:
        return {"index": index, "name": "NVIDIA Test GPU", "utilization_percent": util,
                "memory_used_mb": 4000.0, "memory_total_mb": 81920.0, "job_process": job,
                "job_memory_mb": 1500.0 if job else None}

    def test_the_coverage_rules_of_the_finished_summary(self) -> None:
        live = self._live(self._device(0, job=True), self._device(1, job=False))
        allocation = live_view(live, 2)
        assert allocation is not None and allocation["coverage"] == "allocation"
        attributed = live_view(live, 1)
        assert attributed is not None and attributed["coverage"] == "attributed"
        assert [d["index"] for d in attributed["devices"]] == [0]
        partial = live_view(live, 4)
        assert partial is not None and partial["coverage"] == "partial"
        nobody = live_view(self._live(self._device(0, job=False), self._device(1, job=False)), 1)
        assert nobody is not None and nobody["status"] == "unavailable"
        assert live_view(None, 1) is None
        assert live_view({"schema": "other/1", "devices": [self._device(0, job=True)]}, 1) is None

    def test_the_status_line(self) -> None:
        live = self._live(self._device(0, job=True), self._device(1, job=False, util=None))
        assert format_gpu_live(live, 2) == (
            "GPU 0 NVIDIA Test GPU: utilisation 50%, device memory 4000 MB of 81920 MB, job "
            "processes 1500 MB; GPU 1 NVIDIA Test GPU: utilisation unknown, device memory "
            "4000 MB of 81920 MB (device-level, the allocated devices; sampled 2026-10-14 "
            "17:46:40 UTC, node clock)")
        assert format_gpu_live(self._live(self._device(0, job=False),
                                          self._device(1, job=False)), 1) == (
            "not measured: the node shows 2 GPUs for an allocation of 1, and no process of "
            "this job is on one (sampled 2026-10-14 17:46:40 UTC, node clock)")

    def test_status_and_the_console_show_it_while_the_job_runs(self, tmp_path: Path) -> None:
        live = self._live(self._device(0, job=True))
        queue = tmp_path / "queue"
        queue.mkdir()
        for state in (JobState.RUNNING, JobState.COMPLETED):
            JobSpec(id="abc", command=["run"], cwd=str(tmp_path), cpus=1, gpus=1, state=state,
                    scheduler_gpu_live=live).write(queue / "abc.json")
            shown = "gpu now:      GPU 0 NVIDIA Test GPU" in show_status(
                "localhost", "abc", queue_dir=queue)
            assert shown == (state == JobState.RUNNING)
        status = {"state": "running", "gpus": 1, "scheduler_gpu_live": live}
        view = view_context.gpu_live_view({"status": status})
        assert view is not None and view["observed"] == "2026-10-14 17:46:40 UTC"
        assert view_context.gpu_live_view({"status": {**status, "state": "completed"}}) is None
        assert view_context.gpu_live_view({"status": {**status, "gpus": None}}) is None
        assert view_context.gpu_live_view(None) is None

    def test_the_drill_down_renders_the_section(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from fastapi.testclient import TestClient

        from vq import auth, config
        from vq.web import create_app
        from vq.web import fleet as fleet_mod

        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
        monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
        monkeypatch.setenv("VQ_WEB_FLEET", "1")
        paths.queue_dir().mkdir(parents=True, exist_ok=True)
        status = JobSpec(id="abc", command=["run"], cwd=str(tmp_path), cpus=4, gpus=2,
                         state=JobState.RUNNING, scheduler_gpu_live=self._live(
                             self._device(0, job=True), self._device(1, job=False)),
                         ).model_dump(mode="json")
        detail = {"status": status, "events": [], "samples": [], "owner_host": "localhost"}
        monkeypatch.setattr(fleet_mod, "fetch_job_detail", lambda *a, **k: (detail, None))
        monkeypatch.setattr(fleet_mod, "fetch_job_preview", lambda *a, **k: (None, None))
        html = " ".join(TestClient(create_app()).get("/fleet/jobs/abc?host=localhost").text.split())
        assert "<h3>gpu now</h3>" in html
        assert "<td>50.0%</td> <td>4000 MB of 81920 MB</td> <td>1500 MB</td>" in html
        assert "<td>none seen</td>" in html
        assert "taken 2026-10-14 17:46:40 UTC by the node's clock" in html


class RecordingRunner:
    def __init__(self, fail: bool = False) -> None:
        self.calls: list[list[str]] = []
        self.fail = fail

    def run(self, argv: list[str], *, stdin_data: str | None = None,
            check: bool = False) -> RemoteResult:
        self.calls.append(list(argv))
        if self.fail:
            raise OSError("connection lost")
        done = subprocess.run(argv, capture_output=True, text=True, check=False)
        return RemoteResult(done.returncode, done.stdout, done.stderr)


class TestCollection:
    def _workspace(self, root: Path, name: str, *lines: str) -> str:
        workspace = root / name
        (workspace / "_vq").mkdir(parents=True)
        (workspace / "_vq" / "gpu-samples.txt").write_text("\n".join(lines) + "\n")
        return str(workspace)

    def test_the_dispatcher_reads_single_jobs_and_survives_a_failure(
        self, tmp_path: Path,
    ) -> None:
        runner = RecordingRunner()
        dispatcher = SchedulerDispatcher(TorqueDialect(), runner, scratch_root=str(tmp_path))  # type: ignore[arg-type]
        one = SchedulerHandle("1.c", self._workspace(tmp_path, "one", HEADER, *_block(1, A),
                                                     "T 2"))
        array = SchedulerHandle("2[].c", self._workspace(tmp_path, "two", HEADER, *_block(1, A),
                                                         "T 2"), array_size=2)
        live = dispatcher.read_live_gpu([one, array])
        assert set(live) == {"1.c"} and json.loads(live["1.c"])["observed_at"] == 1
        assert len(runner.calls) == 1
        broken = SchedulerDispatcher(TorqueDialect(), RecordingRunner(fail=True),  # type: ignore[arg-type]
                                     scratch_root=str(tmp_path))
        assert broken.read_live_gpu([one]) == {}

    def test_many_jobs_share_bounded_reads(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(gpu_usage, "LIVE_JOBS_PER_READ", 2)
        runner = RecordingRunner()
        dispatcher = SchedulerDispatcher(TorqueDialect(), runner, scratch_root=str(tmp_path))  # type: ignore[arg-type]
        handles = [SchedulerHandle(f"{i}.c", self._workspace(tmp_path, f"w{i}", HEADER,
                                                             *_block(i, A), "T 99"))
                   for i in range(1, 4)]
        assert set(dispatcher.read_live_gpu(handles)) == {"1.c", "2.c", "3.c"}
        # argv is sh -c SCRIPT NAME PATH...: two files, then the third.
        assert [len(call) - 4 for call in runner.calls] == [2, 1]

    def test_the_job_record_keeps_its_gpus_across_restart_and_recovery(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from vq import scheduler_dispatch
        from vq.scheduler_dispatch import SchedulerSubmitReceipt

        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        restarted = Daemon(max_cpus=4, poll_interval=0.05, queue_dir=tmp_path / "queue",
                           jobs_dir=tmp_path / "jobs")
        restarted.queue_dir.mkdir(parents=True, exist_ok=True)
        _inject(restarted, MockDispatcher())
        spec = _running_scheduler_spec(restarted, "j1")
        spec.gpus = 2
        spec.write(restarted._spec_path("j1"))
        assert restarted._reattach_scheduler_job(spec) is True
        assert restarted._scheduler_running["j1"].gpus == 2

        unknown = scheduler_dispatch.SchedulerSubmitOutcomeUnknown("observer lost")
        mock = MockDispatcher(submit_error=unknown)
        mock.receipt = SchedulerSubmitReceipt("accepted", "18109.host_f", 0)
        _inject(restarted, mock)
        recovered = _submit_scheduler(restarted, "j2")
        recovered.gpus = 1
        recovered.write(restarted._spec_path("j2"))
        restarted._dispatch_pending()
        assert JobSpec.read(restarted._spec_path("j2")).state == JobState.RUNNING
        assert restarted._scheduler_running["j2"].gpus == 1

    def test_only_running_gpu_jobs_are_read(self) -> None:
        reads: list[list[str]] = []

        class Dispatcher:
            def phase_from_detail(self, detail: QstatDetail) -> SchedulerPhase:
                return (SchedulerPhase.RUNNING if detail.raw_state == "R"
                        else SchedulerPhase.PENDING)

            def read_live_gpu(self, handles: list[SchedulerHandle]) -> dict[str, str]:
                reads.append([h.job_id for h in handles])
                return {h.job_id: '{"schema":"vq.gpu-live/1"}' for h in handles}

        def job(job_id: str, gpus: int | None) -> tuple[str, Any]:
            return job_id, daemon_mod._SchedulerJob(
                handle=SchedulerHandle(job_id, "/w"), dispatcher=None,  # type: ignore[arg-type]
                scheduler_target="c", cpus=1, mem_mb=None, gpus=gpus)

        details = {"gpu-running": QstatDetail(raw_state="R"),
                   "gpu-queued": QstatDetail(raw_state="Q"),
                   "cpu-running": QstatDetail(raw_state="R")}
        items = (job("gpu-running", 2), job("gpu-queued", 1), job("cpu-running", None))
        Daemon._merge_live_gpu(Dispatcher(), items, details)  # type: ignore[arg-type]
        assert reads == [["gpu-running"]]
        assert details["gpu-running"].gpu_live == '{"schema":"vq.gpu-live/1"}'
        assert details["cpu-running"].gpu_live is None
        reads.clear()
        Daemon._merge_live_gpu(Dispatcher(), (job("cpu-running", None),), details)  # type: ignore[arg-type]
        assert reads == []  # no GPU job: no read at all


@pytest.fixture
def daemon(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Daemon]:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    queue = Daemon(max_cpus=4, poll_interval=0.05, queue_dir=tmp_path / "queue",
                   jobs_dir=tmp_path / "jobs")
    queue.queue_dir.mkdir(parents=True, exist_ok=True)
    queue.jobs_dir.mkdir(parents=True, exist_ok=True)
    yield queue


class CountingCluster(LocalCluster):
    def __init__(self) -> None:
        super().__init__()
        self.live_reads = 0

    def run(self, argv: list[str], *, stdin_data: str | None = None,
            check: bool = False) -> RemoteResult:
        if argv[:2] == ["sh", "-c"] and "vq-gpu-live" in argv:
            self.live_reads += 1
        return super().run(argv, stdin_data=stdin_data, check=check)


def test_end_to_end_a_running_gpu_job_shows_its_gpus_now(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, daemon: Daemon,
) -> None:
    monkeypatch.setattr(gpu_usage, "SAMPLE_INTERVAL_SECONDS", 1)
    monkeypatch.setattr(daemon_mod, "SCHEDULER_STATUS_REFRESH_SECONDS", 0.0)
    monkeypatch.setenv("PATH", f"{_fake_nvidia_smi(tmp_path)}:{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_OTHER_PID", str(os.getpid()))
    cluster = CountingCluster()
    dispatcher = SchedulerDispatcher(
        TorqueDialect(), cluster, scratch_root=str(tmp_path / "scratch"),
        gnu_time_command=str(_install_fake_gnu_time(tmp_path, monkeypatch)))
    daemon._scheduler_dispatchers["cluster"] = dispatcher
    payload = tmp_path / "payload"
    payload.mkdir()
    (payload / "README").write_text("calculation\n")

    def submit(gpus: int | None, seconds: float) -> str:
        return submit_module.submit_local(
            host="localhost", directory=str(payload), scheduler_target="cluster", gpus=gpus,
            command=["sh", "-c", f"echo $$ > gpu.pid; sleep {seconds}"],
            queue_dir=daemon.queue_dir, jobs_dir=daemon.jobs_dir)

    try:
        plain = submit(None, 2)
        daemon._dispatch_pending()
        for _ in range(5):
            daemon._reconcile_scheduler()
            time.sleep(0.2)
        assert cluster.live_reads == 0  # a CPU job's poll reads no GPU samples
        cluster.wait()

        jobid = submit(2, 6)
        monkeypatch.setenv("FAKE_GPU_PIDFILE",
                           str(Path(dispatcher.remote_workspace(jobid)) / "gpu.pid"))
        daemon._dispatch_pending()
        deadline = time.monotonic() + 20
        while (spec := JobSpec.read(daemon._spec_path(jobid))).scheduler_gpu_live is None:
            assert time.monotonic() < deadline, "no live GPU sample while the job ran"
            assert spec.state == JobState.RUNNING
            daemon._reconcile_scheduler()
            time.sleep(0.2)
        shown = show_status("localhost", jobid, queue_dir=daemon.queue_dir)
        (line,) = [line for line in shown.splitlines() if line.startswith("gpu now:")]
        assert line.startswith(
            "gpu now:      GPU 0 NVIDIA Test GPU: utilisation 75%, device memory 2048 MB of "
            "81920 MB, job processes 1234 MB; GPU 1 NVIDIA Test GPU: utilisation unknown, "
            "device memory 100 MB of 81920 MB (device-level, the allocated devices; sampled ")
        assert JobSpec.read(daemon._spec_path(plain)).scheduler_gpu_live is None
    finally:
        cluster.stop()
