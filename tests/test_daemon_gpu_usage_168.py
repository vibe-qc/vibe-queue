"""#168: what a daemon GPU job's own devices measurably did.

A local job the daemon gave devices to runs with the scheduler job's
`nvidia-smi` sampler beside it, querying only its devices (`-i`), outside the
resource collector. When it finishes, the daemon writes the same
`gpu-usage.json` summary as after a scheduler fetch, so `vq status` shows its
`gpu use:`. A real daemon loop runs real jobs against a stand-in `nvidia-smi`.
"""
from __future__ import annotations

import contextlib
import json
import os
import signal
import subprocess
import textwrap
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from vq import gpu_usage, resource_receipt
from vq.daemon import Daemon
from vq.gpu_usage import read_gpu_usage
from vq.spec import JobSpec, JobState
from vq.status import show_status

ROWS = {"0": "GPU-aaaa-1111, 0, 75, 2048, 81920, NVIDIA Test GPU",
        "1": "GPU-bbbb-2222, 1, 10, 100, 81920, NVIDIA Test GPU"}


def _fake_nvidia_smi(tmp_path: Path) -> Path:
    """Two devices; `-i` limits the rows. The job's pid (FAKE_GPU_PIDFILE) holds
    1234 MB on whichever device it was given. Every call is logged."""
    bin_dir = tmp_path / "fake-bin"
    bin_dir.mkdir()
    fake = bin_dir / "nvidia-smi"
    fake.write_text(textwrap.dedent(f"""\
        #!/bin/sh
        echo "$*" >> "$FAKE_SMI_LOG"
        ids=all
        if [ "$1" = -i ]; then ids=$2; fi
        case "$*" in
          *--query-gpu=*)
            case ",$ids," in *,0,*|,all,) echo '{ROWS["0"]}' ;; esac
            case ",$ids," in *,1,*|,all,) echo '{ROWS["1"]}' ;; esac ;;
          *--query-compute-apps=*)
            if [ -s "$FAKE_GPU_PIDFILE" ]; then
              uuid=GPU-aaaa-1111; [ "$ids" = 1 ] && uuid=GPU-bbbb-2222
              echo "$uuid, $(cat "$FAKE_GPU_PIDFILE"), 1234"
            fi ;;
          *) exit 2 ;;
        esac
        """))
    fake.chmod(0o755)
    return bin_dir


@pytest.fixture
def gpu_daemon(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Daemon]:
    monkeypatch.setattr(gpu_usage, "SAMPLE_INTERVAL_SECONDS", 1)
    monkeypatch.setenv("FAKE_SMI_LOG", str(tmp_path / "smi.log"))
    monkeypatch.setenv("FAKE_GPU_PIDFILE", str(tmp_path / "job.pid"))
    d = Daemon(max_cpus=4, poll_interval=0.05, queue_dir=tmp_path / "queue",
               jobs_dir=tmp_path / "jobs", gpu_devices=["0", "1"])
    d.queue_dir.mkdir(parents=True, exist_ok=True)
    d.jobs_dir.mkdir(parents=True, exist_ok=True)
    yield d
    for rj in d._running.values():
        # The whole group: the wrapper alone would leave its children running.
        with contextlib.suppress(ProcessLookupError):
            os.killpg(rj.popen.pid, signal.SIGKILL)
        rj.popen.wait(timeout=10)
        rj.close_logs()


def _with_smi(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PATH", f"{_fake_nvidia_smi(tmp_path)}{os.pathsep}{os.environ['PATH']}")


def _run(daemon: Daemon, jobid: str, command: list[str], gpus: int | None,
         timeout: float = 20) -> JobSpec:
    workspace = daemon.jobs_dir / jobid
    workspace.mkdir()
    JobSpec(id=jobid, command=command, cwd=str(workspace), cpus=1,
            gpus=gpus).write(daemon._spec_path(jobid))
    deadline = time.monotonic() + timeout
    while True:
        daemon.iterate()
        spec = JobSpec.read(daemon._spec_path(jobid))
        if spec.is_terminal and jobid not in daemon._running:
            return spec
        assert time.monotonic() < deadline, f"{jobid} stuck in {spec.state}"
        time.sleep(0.05)


# The job's own pid is what the stand-in reports on its device.
JOB = ["sh", "-c", 'echo $$ > "$FAKE_GPU_PIDFILE"; sleep 2.5']


def test_a_daemon_gpu_job_reports_its_own_devices(
    gpu_daemon: Daemon, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _with_smi(tmp_path, monkeypatch)
    spec = _run(gpu_daemon, "gpu1", JOB, 1)
    assert (spec.state, spec.gpu_devices) == (JobState.COMPLETED, ["0"])
    workspace = Path(spec.cwd)
    recorded = json.loads((workspace / "_vq" / gpu_usage.SUMMARY_BASENAME).read_text())
    usage = recorded["usage"]
    assert (usage["status"], usage["coverage"], usage["allocated"]) == ("ok", "allocation", 1)
    (device,) = usage["devices"]
    assert (device["index"], device["job_peak_memory_mb"], device["utilization_peak_percent"]
            ) == (0, 1234, 75)
    assert read_gpu_usage(workspace, 1, summary_only=True) == usage
    calls = (tmp_path / "smi.log").read_text().splitlines()
    assert calls and all(call.startswith("-i 0 ") for call in calls)
    assert "gpu use:" in show_status("localhost", "gpu1", queue_dir=gpu_daemon.queue_dir)
    # The command's receipt still describes the command, not the sampler.
    receipt = json.loads((workspace / "_vq" / resource_receipt.RESOURCE_USAGE_BASENAME)
                         .read_text())
    assert receipt["command_status"] == "succeeded"


def test_the_commands_exit_code_and_arguments_pass_through(
    gpu_daemon: Daemon, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _with_smi(tmp_path, monkeypatch)
    out = tmp_path / "args.txt"
    spec = _run(gpu_daemon, "gpu2", ["sh", "-c", f'printf "%s|" "$@" > {out}; exit 3', "sh",
                                     "a b", "'q'", "$HOME"], 2)
    assert (spec.state, spec.exit_code, spec.gpu_devices) == (JobState.FAILED, 3, ["0", "1"])
    assert out.read_text() == "a b|'q'|$HOME|"
    assert (tmp_path / "smi.log").read_text().splitlines()[0].startswith("-i 0,1 ")


def test_without_nvidia_smi_the_job_runs_and_says_so(
    gpu_daemon: Daemon, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PATH", "/usr/bin:/bin")
    spec = _run(gpu_daemon, "gpu3", ["true"], 1)
    assert spec.state == JobState.COMPLETED
    usage = read_gpu_usage(Path(spec.cwd), 1, summary_only=True)
    assert usage is not None
    assert (usage["status"], usage["reason"]) == ("unavailable", "nvidia-smi not found")


def test_a_job_without_gpus_is_not_wrapped(
    gpu_daemon: Daemon, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _with_smi(tmp_path, monkeypatch)
    spec = _run(gpu_daemon, "plain", ["true"], None)
    assert spec.state == JobState.COMPLETED
    assert not (Path(spec.cwd) / "_vq" / gpu_usage.SAMPLES_BASENAME).exists()
    assert not (tmp_path / "smi.log").exists()


def test_the_wrapper_keeps_any_argv_intact(tmp_path: Path) -> None:
    argv = gpu_usage.local_command(
        ["printf", "%s|", "a b", "--", "-i", "'x'", ""], samples_path=tmp_path / "s.txt",
        devices=["GPU-1", "GPU-2"])
    result = subprocess.run(argv, capture_output=True, text=True, check=False,
                            stdin=subprocess.DEVNULL, env={"PATH": "/usr/bin:/bin"})
    assert (result.returncode, result.stdout) == (0, "a b|--|-i|'x'||")
    assert (tmp_path / "s.txt").read_text().splitlines()[1] == "U nvidia-smi not found"


def test_a_multi_user_daemon_never_writes_into_the_users_workspace(
    gpu_daemon: Daemon, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    written: list[Path] = []
    monkeypatch.setattr(gpu_usage, "write_summary",
                        lambda workspace, allocated: written.append(workspace))
    spec = JobSpec(id="j", command=["true"], cwd=str(tmp_path), cpus=1, gpus=1,
                   gpu_devices=["0"])
    gpu_daemon._write_local_gpu_summary(spec)
    assert written == [tmp_path]
    gpu_daemon._multi_user = True
    gpu_daemon._write_local_gpu_summary(spec)
    gpu_daemon._multi_user = False
    for other in (spec.model_copy(update={"gpu_devices": None}),
                  spec.model_copy(update={"scheduler_target": "host_f"})):
        gpu_daemon._write_local_gpu_summary(other)
    assert written == [tmp_path]


SAMPLE = ("V vq.gpu-samples/1 interval=1\nT 1700000000\n"
          f"G {ROWS['0']}\nP GPU-aaaa-1111, 4242, 1234\nT 1700000001\n")


def _running(daemon: Daemon, jobid: str) -> JobSpec:
    """Start a long GPU job and return its RUNNING spec."""
    workspace = daemon.jobs_dir / jobid
    workspace.mkdir()
    JobSpec(id=jobid, command=["sleep", "30"], cwd=str(workspace), cpus=1,
            gpus=1).write(daemon._spec_path(jobid))
    daemon.iterate()
    spec = JobSpec.read(daemon._spec_path(jobid))
    assert spec.state == JobState.RUNNING
    return spec


class TestLive:
    def test_a_running_daemon_gpu_job_shows_its_newest_sample(
        self, gpu_daemon: Daemon, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _with_smi(tmp_path, monkeypatch)
        workspace = gpu_daemon.jobs_dir / "live"
        workspace.mkdir()
        JobSpec(id="live", command=["sh", "-c", 'echo $$ > "$FAKE_GPU_PIDFILE"; sleep 30'],
                cwd=str(workspace), cpus=1, gpus=1).write(gpu_daemon._spec_path("live"))
        # The sampler starts beside the job's shell, so its first samples can
        # predate the shell writing its pid: wait for one that sees the job.
        deadline = time.monotonic() + 20
        while True:
            live = JobSpec.read(gpu_daemon._spec_path("live")).scheduler_gpu_live
            if live is not None and any(d.get("job_process") for d in live["devices"]):
                break
            assert time.monotonic() < deadline, f"no live sample of the job recorded: {live}"
            gpu_daemon.iterate()
            time.sleep(0.1)
        (device,) = live["devices"]
        assert (live["schema"], device["index"], device["job_process"],
                device["job_memory_mb"], device["utilization_percent"]) == (
            "vq.gpu-live/1", 0, True, 1234, 75)
        assert "gpu now:" in show_status("localhost", "live", queue_dir=gpu_daemon.queue_dir)

    def test_the_file_is_read_only_when_it_changes_and_never_through_a_link(
        self, gpu_daemon: Daemon, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _running(gpu_daemon, "j")
        rj = gpu_daemon._running["j"]
        samples = tmp_path / "samples.txt"
        samples.write_text(SAMPLE)
        rj.gpu_samples = samples
        reads: list[str] = []
        real = gpu_usage.latest_sample
        def counted(text: str) -> dict[str, Any] | None:
            reads.append(text)
            return real(text)

        monkeypatch.setattr(gpu_usage, "latest_sample", counted)
        gpu_daemon._refresh_local_gpu_live()
        gpu_daemon._refresh_local_gpu_live()
        assert len(reads) == 1
        recorded = JobSpec.read(gpu_daemon._spec_path("j")).scheduler_gpu_live
        assert recorded is not None and recorded["observed_at"] == 1700000000
        with samples.open("a") as handle:
            handle.write(f"G {ROWS['0']}\nT 1700000002\n")
        gpu_daemon._refresh_local_gpu_live()
        assert len(reads) == 2
        newer = JobSpec.read(gpu_daemon._spec_path("j")).scheduler_gpu_live
        assert newer is not None and newer["observed_at"] == 1700000001
        link = tmp_path / "link.txt"
        link.symlink_to(samples)
        rj.gpu_samples, rj.gpu_samples_seen = link, None
        gpu_daemon._refresh_local_gpu_live()
        assert len(reads) == 2

    def test_a_job_no_longer_running_is_left_alone(
        self, gpu_daemon: Daemon, tmp_path: Path,
    ) -> None:
        spec = _running(gpu_daemon, "k")
        samples = tmp_path / "samples.txt"
        samples.write_text(SAMPLE)
        gpu_daemon._running["k"].gpu_samples = samples
        spec.state = JobState.SUSPENDED
        spec.write(gpu_daemon._spec_path("k"))
        gpu_daemon._refresh_local_gpu_live()
        assert JobSpec.read(gpu_daemon._spec_path("k")).scheduler_gpu_live is None

    def test_an_unchanged_sample_is_not_rewritten(
        self, gpu_daemon: Daemon, tmp_path: Path,
    ) -> None:
        _running(gpu_daemon, "m")
        rj = gpu_daemon._running["m"]
        samples = tmp_path / "samples.txt"
        samples.write_text(SAMPLE)
        rj.gpu_samples = samples
        gpu_daemon._refresh_local_gpu_live()
        before = gpu_daemon._spec_path("m").stat().st_mtime_ns
        stat = samples.stat()
        os.utime(samples, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))
        gpu_daemon._refresh_local_gpu_live()  # read again: the same newest sample
        assert rj.gpu_samples_seen == (stat.st_size, stat.st_mtime_ns + 1_000_000_000)
        assert gpu_daemon._spec_path("m").stat().st_mtime_ns == before


def test_only_the_end_of_a_long_samples_file_is_read(
    gpu_daemon: Daemon, tmp_path: Path,
) -> None:
    _running(gpu_daemon, "long")
    samples = tmp_path / "samples.txt"
    body = "".join(f"T {1700000000 + i}\nG {ROWS['0']}\n" for i in range(2000))
    samples.write_text("V vq.gpu-samples/1 interval=1\n" + body + "S 1700009999\n")
    assert samples.stat().st_size > 2 * gpu_usage.LIVE_TAIL_BYTES
    gpu_daemon._running["long"].gpu_samples = samples
    gpu_daemon._refresh_local_gpu_live()
    live = JobSpec.read(gpu_daemon._spec_path("long")).scheduler_gpu_live
    assert live is not None and live["observed_at"] == 1700001999


def test_the_sampler_never_outlives_its_wrapper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A wrapper killed on its own, so it never stops the sampler, still ends it."""
    monkeypatch.setattr(gpu_usage, "SAMPLE_INTERVAL_SECONDS", 1)
    samples = tmp_path / "s.txt"
    wrapper = subprocess.Popen(
        gpu_usage.local_command(["sleep", "60"], samples_path=samples, devices=["0"]),
        start_new_session=True, stdin=subprocess.DEVNULL,
        env={"PATH": f"{_fake_nvidia_smi(tmp_path)}:/usr/bin:/bin",
             "FAKE_SMI_LOG": str(tmp_path / "smi.log"),
             "FAKE_GPU_PIDFILE": str(tmp_path / "none")})
    try:
        deadline = time.monotonic() + 10
        while "\nT " not in (samples.read_text() if samples.exists() else ""):
            assert time.monotonic() < deadline, "the sampler never sampled"
            time.sleep(0.05)
        wrapper.kill()  # the wrapper only: its stop never runs
        wrapper.wait(timeout=10)
        sampler_gone = time.monotonic() + 10
        while subprocess.run(["pgrep", "-f", str(samples)], capture_output=True,
                             stdin=subprocess.DEVNULL).returncode == 0:
            assert time.monotonic() < sampler_gone, "the sampler outlived its wrapper"
            time.sleep(0.1)
    finally:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(wrapper.pid, signal.SIGKILL)


def test_a_job_reattached_after_a_restart_keeps_its_gpu_use(
    gpu_daemon: Daemon, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The first daemon starts the job; a second, as after a restart, re-attaches it."""
    _with_smi(tmp_path, monkeypatch)
    release = tmp_path / "go"
    workspace = gpu_daemon.jobs_dir / "kept"
    workspace.mkdir()
    JobSpec(id="kept", cwd=str(workspace), cpus=1, gpus=1, command=[
        # Bounded, so the job cannot outlive a pytest killed before cleanup.
        "sh", "-c", f'echo $$ > "$FAKE_GPU_PIDFILE"; i=0; '
        f'while [ ! -f {release} ] && [ "$i" -lt 1200 ]; do sleep 0.1; i=$((i+1)); done; '
        f'[ -f {release} ]',
    ]).write(gpu_daemon._spec_path("kept"))
    gpu_daemon.iterate()
    first = gpu_daemon._running["kept"]
    gpu_daemon._queue_lock_fd.close()  # the first daemon "stops"; its job runs on
    restarted = Daemon(max_cpus=4, poll_interval=0.05, queue_dir=gpu_daemon.queue_dir,
                       jobs_dir=gpu_daemon.jobs_dir, gpu_devices=["0", "1"])
    restarted._reattach_or_interrupt_at_startup()
    orphan = restarted._orphans["kept"]
    assert orphan.gpu_devices == ("0",)
    assert orphan.gpu_samples == workspace / "_vq" / gpu_usage.SAMPLES_BASENAME
    deadline = time.monotonic() + 20
    while JobSpec.read(restarted._spec_path("kept")).scheduler_gpu_live is None:
        assert time.monotonic() < deadline, "the re-attached job got no live sample"
        restarted.iterate()
        time.sleep(0.1)
    release.write_text("")
    first.popen.wait(timeout=20)  # its parent is the first daemon, which reaps it
    while (spec := JobSpec.read(restarted._spec_path("kept"))).state == JobState.RUNNING:
        assert time.monotonic() < deadline + 20, "the re-attached job never finished"
        restarted.iterate()
        time.sleep(0.1)
    assert spec.state == JobState.COMPLETED
    usage = read_gpu_usage(workspace, 1, summary_only=True)
    assert usage is not None and usage["status"] == "ok"
    assert usage["devices"][0]["job_peak_memory_mb"] == 1234
