"""#168: a scheduler GPU job's measured GPU use, from nvidia-smi samples.

The job script of a job that requested ``--gpus`` samples ``nvidia-smi``
on its batch node while the command runs; the driver parses the raw samples
once the job has finished and decides what it can attribute. These checks
run the real rendered script with a stand-in ``nvidia-smi`` and GNU time.
The decisive case goes through the daemon's real scheduler path on a local
stand-in for the cluster, through to `vq status`.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import textwrap
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from vq import gpu_usage, paths
from vq import submit as submit_module
from vq.daemon import Daemon
from vq.gpu_usage import format_gpu_usage, read_gpu_usage
from vq.scheduler_dialect import TorqueDialect
from vq.scheduler_dispatch import SchedulerDispatcher
from vq.spec import JobSpec, JobState
from vq.status import show_status, show_status_json
from vq.web import view_context

sys.path.insert(0, str(Path(__file__).parent))
from test_runtime_binding_scheduler_161 import LocalCluster  # noqa: E402
from test_scheduler_dispatch import FakeRunner, _install_fake_gnu_time  # noqa: E402

DEVICES = ("GPU-aaaa-1111, 0, 75, 2048, 81920, NVIDIA Test GPU\n"
           "GPU-bbbb-2222, 1, [N/A], 100, 81920, NVIDIA Test GPU\n")


def _fake_nvidia_smi(tmp_path: Path) -> Path:
    """Two devices; the job's process (its pid in FAKE_GPU_PIDFILE) holds 1234 MB on
    the first, and an unrelated process (FAKE_OTHER_PID, outside the job) 999 MB on
    the second. FAKE_GPU_SLOW makes the process query take that many seconds."""
    bin_dir = tmp_path / "fake-bin"
    bin_dir.mkdir()
    fake = bin_dir / "nvidia-smi"
    fake.write_text(textwrap.dedent(f"""\
        #!/bin/bash
        case "$*" in
          *--query-gpu=*) printf '%s' '{DEVICES}' ;;
          *--query-compute-apps=*)
            sleep "${{FAKE_GPU_SLOW:-0}}"
            if [ -s "${{FAKE_GPU_PIDFILE:-/nonexistent}}" ]; then
              printf 'GPU-aaaa-1111, %s, 1234\\n' "$(cat "$FAKE_GPU_PIDFILE")"
            fi
            printf 'GPU-bbbb-2222, %s, 999\\n' "${{FAKE_OTHER_PID:-1}}" ;;
          *) exit 2 ;;
        esac
        """))
    fake.chmod(0o755)
    return bin_dir


@pytest.fixture
def fast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gpu_usage, "SAMPLE_INTERVAL_SECONDS", 1)


def _dispatcher(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, **kwargs: Any
                ) -> SchedulerDispatcher:
    return SchedulerDispatcher(
        TorqueDialect(), FakeRunner(), scratch_root=str(tmp_path / "scratch"),
        gnu_time_command=str(_install_fake_gnu_time(tmp_path, monkeypatch)), **kwargs)


def _run_script(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, command: list[str], *,
                with_smi: bool = True, gpus: int = 2, slow: float = 0,
                extra_bin: Path | None = None) -> tuple[subprocess.Popen[bytes], Path]:
    workspace = tmp_path / "ws"
    workspace.mkdir()
    script = _dispatcher(tmp_path, monkeypatch).build_job_script(
        job_id="j1", command=command, remote_workspace=str(workspace), cpus=1, gpus=gpus)
    path = (str(_fake_nvidia_smi(tmp_path)) + ":" if with_smi else "") + "/usr/bin:/bin"
    if extra_bin is not None:
        path = f"{extra_bin}:{path}"
    process = subprocess.Popen(
        ["/bin/bash", "-c", script], start_new_session=True,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        env={"PATH": path, "FAKE_GPU_PIDFILE": str(workspace / "gpu.pid"),
             "FAKE_OTHER_PID": str(os.getpid()), "FAKE_GPU_SLOW": str(slow)})
    return process, workspace


def _group_gone(pgid: int, within: float = 5) -> bool:
    deadline = time.monotonic() + within
    while time.monotonic() < deadline:
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            return True
        time.sleep(0.05)
    return False


JOB = ["sh", "-c", 'echo $$ > "$FAKE_GPU_PIDFILE"; sleep 2.5']


class TestScript:
    def test_a_job_without_gpus_keeps_its_script(self, tmp_path: Path,
                                                 monkeypatch: pytest.MonkeyPatch) -> None:
        script = _dispatcher(tmp_path, monkeypatch).build_job_script(
            job_id="j1", command=["run"], remote_workspace="/w", cpus=1)
        assert "__vq_gpu" not in script and "nvidia-smi" not in script

    @pytest.mark.parametrize("scratch", [None, "/tmp/node"])
    def test_sampling_brackets_the_command(self, tmp_path: Path,
                                           monkeypatch: pytest.MonkeyPatch,
                                           scratch: str | None) -> None:
        script = _dispatcher(tmp_path, monkeypatch, node_scratch_dir=scratch).build_job_script(
            job_id="j1", command=["run-it"], remote_workspace="/w", cpus=1, gpus=1)
        lines = script.splitlines()
        command = next(i for i, line in enumerate(lines) if "run-it" in line)
        assert lines[command - 1] == "__vq_gpu_start"
        assert lines[command + 1:command + 4] == ["__vq_rc=$?", "__vq_gpu_stop",
                                                  "__vq_recover_signal_rc"]
        assert "__vq_term() { __vq_rc=\"$1\"; __vq_gpu_stop; " in script

    def test_the_rendered_script_samples_the_node(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fast: None,
    ) -> None:
        process, workspace = _run_script(tmp_path, monkeypatch, JOB)
        assert process.wait(timeout=30) == 0
        assert _group_gone(process.pid), "the sampler outlived the job"
        lines = (workspace / "_vq" / "gpu-samples.txt").read_text().splitlines()
        pid = (workspace / "gpu.pid").read_text().strip()
        assert lines[0] == "V vq.gpu-samples/1 interval=1"
        assert lines[-1].startswith("S ")
        assert lines.count("G GPU-aaaa-1111, 0, 75, 2048, 81920, NVIDIA Test GPU") >= 2
        processes = [line for line in lines if line.startswith("P ")]
        # Only the job's own process: the test runner's is not under the script.
        assert processes and set(processes) == {f"P GPU-aaaa-1111, {pid}, 1234"}
        assert not list((workspace / "_vq").glob("gpu-samples.txt.*"))

        usage = read_gpu_usage(workspace, 2)
        assert usage is not None and usage["status"] == "ok"
        assert (usage["coverage"], usage["visible"], usage["interval_seconds"]) == (
            "allocation", 2, 1)
        first, second = usage["devices"]
        assert (first["index"], first["job_peak_memory_mb"], first["utilization_mean_percent"],
                first["device_peak_memory_mb"], first["memory_total_mb"]) == (
            0, 1234.0, 75.0, 2048.0, 81920.0)
        # Not supported is unknown, never zero; nobody of the job held memory there.
        assert (second["utilization_mean_percent"], second["job_peak_memory_mb"]) == (None, None)

    def test_without_nvidia_smi_the_job_runs_and_says_so(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fast: None,
    ) -> None:
        process, workspace = _run_script(tmp_path, monkeypatch, ["true"], with_smi=False)
        assert process.wait(timeout=30) == 0
        assert (workspace / "_vq" / "gpu-samples.txt").read_text() == (
            "V vq.gpu-samples/1 interval=1\nU nvidia-smi not found\n")
        assert read_gpu_usage(workspace, 2) == {
            "status": "unavailable", "reason": "nvidia-smi not found", "collector": "nvidia-smi",
            "interval_seconds": 1, "samples": 0, "truncated": False, "errors": 1,
            "allocated": 2}

    def test_a_killed_job_stops_sampling(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # A long interval: the sampler sits in its sleep, which must not outlive it.
        monkeypatch.setattr(gpu_usage, "SAMPLE_INTERVAL_SECONDS", 20)
        process, workspace = _run_script(tmp_path, monkeypatch, ["sleep", "30"])
        samples = workspace / "_vq" / "gpu-samples.txt"
        deadline = time.monotonic() + 10
        while "\nT " not in (samples.read_text() if samples.exists() else ""):
            assert time.monotonic() < deadline
            time.sleep(0.05)
        os.killpg(process.pid, signal.SIGTERM)  # how the scheduler ends a job
        assert process.wait(timeout=10) == 143
        assert _group_gone(process.pid), "the sampler outlived the job"
        assert samples.read_text().splitlines()[-1].startswith("S ")

    def test_stopping_leaves_no_sampler_behind(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # The sampler sleeps out a long interval when the command ends.
        monkeypatch.setattr(gpu_usage, "SAMPLE_INTERVAL_SECONDS", 20)
        process, _workspace = _run_script(tmp_path, monkeypatch, ["sleep", "0.5"])
        assert process.wait(timeout=30) == 0
        assert _group_gone(process.pid, within=1), "the sampler's sleep outlived the job"

    def test_the_job_waits_for_a_slow_sample_before_it_ends(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fast: None,
    ) -> None:
        process, workspace = _run_script(tmp_path, monkeypatch, ["sleep", "0.3"], slow=2)
        assert process.wait(timeout=30) == 0
        # Nothing of the job is left the moment its script has exited.
        assert _group_gone(process.pid, within=0.5), "the job ended mid-sample"
        assert (workspace / "_vq" / "gpu-samples.txt").read_text().splitlines()[-1].startswith(
            "S ")

    def test_each_query_is_bounded_where_the_node_has_timeout(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fast: None,
    ) -> None:
        shim = tmp_path / "shim-bin"
        shim.mkdir()
        (shim / "timeout").write_text(
            '#!/bin/bash\nprintf "%s\\n" "$1" >> "$(dirname "$FAKE_GPU_PIDFILE")/timeouts"\n'
            'shift\nexec "$@"\n')
        (shim / "timeout").chmod(0o755)
        process, workspace = _run_script(tmp_path, monkeypatch, JOB, extra_bin=shim)
        assert process.wait(timeout=30) == 0
        used = (workspace / "timeouts").read_text().split()
        assert used and set(used) == {"10"}
        usage = read_gpu_usage(workspace, 2)
        assert usage is not None and usage["status"] == "ok"

    def test_sampling_stops_at_its_cap(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fast: None,
    ) -> None:
        monkeypatch.setattr(gpu_usage, "MAX_SAMPLES_BYTES", 300)
        process, workspace = _run_script(tmp_path, monkeypatch, JOB)
        assert process.wait(timeout=30) == 0
        text = (workspace / "_vq" / "gpu-samples.txt").read_text()
        assert text.count("X sample cap reached") == 1
        usage = read_gpu_usage(workspace, 2)
        assert usage is not None and usage["truncated"] is True


def _samples(root: Path, *rows: str, name: str = "gpu-samples.txt") -> Path:
    workspace = root / "ws"
    (workspace / "_vq").mkdir(parents=True, exist_ok=True)
    (workspace / "_vq" / name).write_text("\n".join(rows) + "\n")
    return workspace


HEADER = "V vq.gpu-samples/1 interval=30"
A = "G GPU-aaaa-1111, 0, 50, 4000, 81920, NVIDIA Test GPU"
B = "G GPU-bbbb-2222, 1, 90, 7000, 81920, NVIDIA Test GPU"


class TestReader:
    def test_the_node_shows_the_allocation(self, tmp_path: Path) -> None:
        workspace = _samples(tmp_path, HEADER, "T 1", A.replace(", 50,", ", 40,"),
                             "P GPU-aaaa-1111, 42, 1000", "P GPU-aaaa-1111, 43, 500",
                             "T 31", A, "P GPU-aaaa-1111, 42, 1200", "S 40")
        usage = read_gpu_usage(workspace, 1)
        assert usage is not None and usage["coverage"] == "allocation"
        (device,) = usage["devices"]
        # Processes on one device add up within a sample; the peak is over samples.
        assert (device["job_peak_memory_mb"], device["utilization_mean_percent"],
                device["utilization_peak_percent"], device["samples"]) == (1500.0, 45.0, 50.0, 2)

    def test_more_devices_than_allocated_report_only_attributed_ones(self, tmp_path: Path) -> None:
        workspace = _samples(tmp_path, HEADER, "T 1", A, B, "P GPU-bbbb-2222, 42, 3000",
                             "P GPU-dddd-4444, 42, 10")  # a device the node did not list
        usage = read_gpu_usage(workspace, 1)
        assert usage is not None and usage["coverage"] == "attributed"
        assert [d["index"] for d in usage["devices"]] == [1]
        unattributed = read_gpu_usage(_samples(tmp_path / "x", HEADER, "T 1", A, B), 1)
        assert unattributed is not None and unattributed["status"] == "unavailable"
        assert unattributed["reason"] == (
            "the node shows 2 GPUs for an allocation of 1, and no process of this job was "
            "seen on one")

    def test_fewer_devices_than_allocated_are_partial(self, tmp_path: Path) -> None:
        usage = read_gpu_usage(_samples(tmp_path, HEADER, "T 1", A), 4)
        assert usage is not None and (usage["coverage"], usage["visible"]) == ("partial", 1)

    def test_an_unknown_process_memory_is_not_a_partial_sum(self, tmp_path: Path) -> None:
        workspace = _samples(tmp_path, HEADER, "T 1", A, "P GPU-aaaa-1111, 42, 1000",
                             "P GPU-aaaa-1111, 43, [N/A]",
                             "T 2", "G MIG-cccc-3333, 1, [Not Supported], 10, 20, MIG slice")
        usage = read_gpu_usage(workspace, 2)
        assert usage is not None
        assert [d["job_peak_memory_mb"] for d in usage["devices"]] == [None, None]
        assert usage["devices"][1]["utilization_mean_percent"] is None

    def test_devices_with_one_index_keep_one_order(self, tmp_path: Path) -> None:
        workspace = _samples(tmp_path, HEADER, "T 1", B.replace(", 1, ", ", 0, "), A)
        orders = []
        for _ in range(3):
            usage = read_gpu_usage(workspace, 2)
            assert usage is not None
            orders.append([d["device_peak_memory_mb"] for d in usage["devices"]])
        # GPU-aaaa sorts before GPU-bbbb however the set iterates.
        assert orders == [[4000.0, 7000.0]] * 3
        script = (
            "import sys; from pathlib import Path; from vq.gpu_usage import read_gpu_usage; "
            "print([d['device_peak_memory_mb'] for d in "
            "read_gpu_usage(Path(sys.argv[1]), 2)['devices']])")
        for seed in ("1", "2", "3"):
            out = subprocess.run([sys.executable, "-c", script, str(workspace)],
                                 env={**os.environ, "PYTHONHASHSEED": seed},
                                 capture_output=True, text=True, check=True).stdout
            assert out.strip() == "[4000.0, 7000.0]"

    def test_failed_queries_are_unavailable(self, tmp_path: Path) -> None:
        usage = read_gpu_usage(_samples(tmp_path, HEADER, "T 1", "E query-gpu failed"), 1)
        assert usage is not None and (usage["status"], usage["reason"]) == (
            "unavailable", "query-gpu failed")

    @pytest.mark.parametrize(("rows", "error"), [
        (["V something-else"], "not a vq.gpu-samples/1 file"),
        ([HEADER, "G GPU-aaaa-1111, 0, 50"], "unexpected row"),
        ([HEADER, "T 1", "G GPU-aaaa-1111, 0, 50"], "line 3: malformed G row"),
        ([HEADER, "T 1", "G GPU-aaaa-1111, x, 50, 1, 2, n"], "device index is not an integer"),
        ([HEADER, "T 1", "G GPU-aaaa-1111, 0, -5, 1, 2, n"], "not a finite non-negative"),
        ([HEADER, "T 1", "G GPU-aaaa-1111, 0, nan, 1, 2, n"], "not a finite non-negative"),
        ([HEADER, "T 1", "G GPU-aaaa-1111, 0, lots, 1, 2, n"], "utilization is not a number"),
        ([HEADER, "T 1", "P not-a-uuid, 1, 2"], "malformed P row"),
        ([HEADER, "Q what"], "unexpected row"),
    ])
    def test_untrustworthy_samples_are_invalid(self, tmp_path: Path, rows: list[str],
                                               error: str) -> None:
        usage = read_gpu_usage(_samples(tmp_path, *rows), 1)
        assert usage is not None and usage["status"] == "invalid"
        assert error in usage["error"]

    def test_too_many_devices_or_bytes_are_invalid(self, tmp_path: Path,
                                                   monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(gpu_usage, "MAX_DEVICES", 1)
        usage = read_gpu_usage(_samples(tmp_path, HEADER, "T 1", A, B), 2)
        assert usage is not None and "more than 1 devices" in usage["error"]
        monkeypatch.setattr(gpu_usage, "MAX_SAMPLES_BYTES", 10)
        big = _samples(tmp_path / "big", HEADER, "T 1", "E " + "x" * 70_000)
        usage = read_gpu_usage(big, 2)
        assert usage is not None and "larger than their cap" in usage["error"]

    def test_a_link_is_never_followed(self, tmp_path: Path) -> None:
        elsewhere = _samples(tmp_path / "elsewhere", HEADER, "T 1", A)
        workspace = tmp_path / "ws"
        (workspace / "_vq").mkdir(parents=True)
        (workspace / "_vq" / "gpu-samples.txt").symlink_to(
            elsewhere / "_vq" / "gpu-samples.txt")
        usage = read_gpu_usage(workspace, 1)
        assert usage is not None and usage["status"] == "invalid"

    def test_no_samples_or_only_array_elements(self, tmp_path: Path) -> None:
        assert read_gpu_usage(tmp_path / "nowhere", 1) is None
        (tmp_path / "empty" / "_vq").mkdir(parents=True)
        assert read_gpu_usage(tmp_path / "empty", 1) is None
        array = _samples(tmp_path, HEADER, name="gpu-samples.txt.0")
        assert read_gpu_usage(array, 1) == {
            "status": "unavailable", "elements": 1, "reason": "element 0: no sample was taken"}


OK = {"status": "ok", "collector": "nvidia-smi", "interval_seconds": 30, "samples": 12,
      "truncated": True, "errors": 0, "allocated": 1, "visible": 1, "coverage": "allocation",
      "devices": [{"index": 0, "name": "NVIDIA Test GPU", "memory_total_mb": 81920.0,
                   "device_peak_memory_mb": 4000.0, "utilization_mean_percent": 45.0,
                   "utilization_peak_percent": 50.0, "job_peak_memory_mb": 1500.0,
                   "samples": 12}]}


class TestSurfaces:
    def test_the_status_line(self) -> None:
        assert format_gpu_usage(OK) == (
            "GPU 0 NVIDIA Test GPU: job processes peak 1500 MB, device memory peak 4000 MB "
            "of 81920 MB, utilisation mean 45% peak 50% (device-level, the allocated devices, "
            "12 samples every 30 s, sampling stopped at its size cap)")
        assert format_gpu_usage({"status": "unavailable", "reason": "why"}) == (
            "not measured: why")
        assert format_gpu_usage({"status": "invalid", "error": "bad"}) == "not shown: bad"
        assert format_gpu_usage(None) is None

    def _queue(self, tmp_path: Path, state: JobState, gpus: int | None) -> Path:
        workspace = _samples(tmp_path, HEADER, "T 1", A, "P GPU-aaaa-1111, 42, 1500")
        queue = tmp_path / "queue"
        queue.mkdir()
        JobSpec(id="abc", command=["run"], cwd=str(workspace), cpus=1, gpus=gpus,
                state=state, exit_code=0 if state == JobState.COMPLETED else None,
                scheduler_target=None).write(queue / "abc.json")
        return queue

    def test_status_shows_a_finished_gpu_job_only(self, tmp_path: Path) -> None:
        queue = self._queue(tmp_path, JobState.COMPLETED, 1)
        assert "gpu use:      GPU 0 NVIDIA Test GPU: job processes peak 1500 MB" in (
            show_status("localhost", "abc", queue_dir=queue))
        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))
        assert payload["gpu_usage"]["devices"][0]["job_peak_memory_mb"] == 1500.0
        running = self._queue(tmp_path / "running", JobState.RUNNING, 1)
        assert json.loads(show_status_json("localhost", "abc", queue_dir=running))[
            "gpu_usage"] is None
        assert "gpu use:" not in show_status("localhost", "abc", queue_dir=running)
        plain = self._queue(tmp_path / "plain", JobState.COMPLETED, None)
        assert "gpu_usage" not in json.loads(show_status_json("localhost", "abc",
                                                              queue_dir=plain))

    def test_console_views(self) -> None:
        def detail(state: str, **status: object) -> dict[str, object]:
            return {"status": {"state": state, "gpus": 1, **status}}

        assert view_context.gpu_usage_view(detail("completed")) is None  # an older host
        assert view_context.gpu_usage_view(detail("running", gpu_usage=None)) is None
        assert view_context.gpu_usage_view(None) is None
        assert view_context.gpu_usage_view(detail("killed", gpu_usage=None)) == {
            "status": "absent", "allocated": 1}
        assert view_context.gpu_usage_view(detail("completed", gpu_usage=OK)) == OK

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
        status = JobSpec(id="abc", command=["run"], cwd=str(tmp_path), cpus=4, gpus=1,
                         state=JobState.COMPLETED, exit_code=0).model_dump(mode="json")
        status["gpu_usage"] = OK
        detail = {"status": status, "events": [], "samples": [], "owner_host": "localhost"}
        monkeypatch.setattr(fleet_mod, "fetch_job_detail", lambda *a, **k: (detail, None))
        monkeypatch.setattr(fleet_mod, "fetch_job_preview", lambda *a, **k: (None, None))
        html = " ".join(TestClient(create_app()).get("/fleet/jobs/abc?host=localhost").text.split())
        assert "<h3>gpu use</h3>" in html
        assert "<td>1500 MB</td> <td>4000 MB of 81920 MB</td> <td>45.0%</td> <td>50.0%</td>" in html
        assert "showed exactly the 1 allocated GPUs" in html
        assert "until sampling reached its size cap" in html


@pytest.fixture
def daemon(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Daemon]:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    queue = Daemon(max_cpus=4, poll_interval=0.05, queue_dir=tmp_path / "queue",
                   jobs_dir=tmp_path / "jobs")
    queue.queue_dir.mkdir(parents=True, exist_ok=True)
    queue.jobs_dir.mkdir(parents=True, exist_ok=True)
    yield queue


def test_end_to_end_a_gpu_jobs_use_reaches_vq_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, daemon: Daemon, fast: None,
) -> None:
    monkeypatch.setenv("PATH", f"{_fake_nvidia_smi(tmp_path)}:{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_OTHER_PID", str(os.getpid()))
    cluster = LocalCluster()
    daemon._scheduler_dispatchers["cluster"] = SchedulerDispatcher(
        TorqueDialect(), cluster, scratch_root=str(tmp_path / "scratch"),
        gnu_time_command=str(_install_fake_gnu_time(tmp_path, monkeypatch)))
    payload = tmp_path / "payload"
    payload.mkdir()
    (payload / "README").write_text("calculation\n")
    jobid = submit_module.submit_local(
        host="localhost", directory=str(payload), scheduler_target="cluster", gpus=2,
        command=["sh", "-c", 'echo $$ > gpu.pid; sleep 2.5'],
        queue_dir=daemon.queue_dir, jobs_dir=daemon.jobs_dir)
    try:
        spec = JobSpec.read(daemon._spec_path(jobid))
        remote = Path(daemon._scheduler_dispatchers["cluster"].remote_workspace(jobid))
        monkeypatch.setenv("FAKE_GPU_PIDFILE", str(remote / "gpu.pid"))
        daemon._dispatch_pending()
        cluster.wait()
        deadline = time.monotonic() + 30
        local = Path(spec.cwd) / "_vq" / "gpu-samples.txt"
        while (JobSpec.read(daemon._spec_path(jobid)).state != JobState.COMPLETED
               or not local.exists()):
            assert time.monotonic() < deadline
            daemon._reconcile_scheduler()
            time.sleep(0.05)
    finally:
        cluster.stop()
    shown = show_status("localhost", jobid, queue_dir=daemon.queue_dir)
    (line,) = [line for line in shown.splitlines() if line.startswith("gpu use:")]
    assert line.startswith("gpu use:      GPU 0 NVIDIA Test GPU: job processes peak 1234 MB, "
                           "device memory peak 2048 MB of 81920 MB, utilisation mean 75% "
                           "peak 75%; GPU 1 NVIDIA Test GPU: device memory peak 100 MB of "
                           "81920 MB, utilisation mean unknown peak unknown (device-level, "
                           "the allocated devices, ")
