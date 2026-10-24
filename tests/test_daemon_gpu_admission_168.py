"""#168: GPU admission and device assignment on daemon hosts.

A daemon started with `--gpu-devices` (or `[daemon] gpu_devices`) admits a
local `--gpus N` job only when N of its devices are free, gives it the
lowest free ones in CUDA_VISIBLE_DEVICES and VQ_GPU_DEVICES, and records
them on the job so a restarted daemon still counts them busy. `vq submit
--gpus` to a daemon host is accepted only when that host's running daemon
advertises enough devices. Device IDs are synthetic; real jobs run.
"""
from __future__ import annotations

import contextlib
import os
import signal
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vq import config, paths
from vq import daemon as daemon_module
from vq import submit as submit_module
from vq.cli import main
from vq.daemon import Daemon, _OrphanJob, _TerminalSurvivor, _vq_job_env
from vq.spec import JobSpec, JobState

sys.path.insert(0, str(Path(__file__).parent))
from test_daemon_config_caps import captured_daemon, cfg_dir  # noqa: E402,F401

DEVICES = ["0", "1", "2"]


@pytest.fixture
def gpu_daemon(tmp_path: Path) -> Iterator[Daemon]:
    d = Daemon(max_cpus=8, poll_interval=0.05, queue_dir=tmp_path / "queue",
               jobs_dir=tmp_path / "jobs", gpu_devices=DEVICES)
    d.queue_dir.mkdir(parents=True, exist_ok=True)
    d.jobs_dir.mkdir(parents=True, exist_ok=True)
    yield d
    for rj in d._running.values():
        # The whole group: the wrapper alone would leave its children running.
        with contextlib.suppress(ProcessLookupError):
            os.killpg(rj.popen.pid, signal.SIGKILL)
        rj.popen.wait(timeout=10)
        rj.close_logs()


def _job(daemon: Daemon, jobid: str, gpus: int | None, tmp_path: Path,
         **extra: Any) -> None:
    """A job that writes what devices it sees, then waits for its release file."""
    workspace = daemon.jobs_dir / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    seen, release = tmp_path / f"{jobid}.seen", tmp_path / f"{jobid}.go"
    script = (f'echo "${{CUDA_VISIBLE_DEVICES-unset}} ${{VQ_GPU_DEVICES-unset}}" > {seen}; '
              f"while [ ! -f {release} ]; do sleep 0.05; done")
    JobSpec(id=jobid, command=["sh", "-c", script], cwd=str(workspace), cpus=1, gpus=gpus,
            **extra).write(daemon._spec_path(jobid))


def _settle(daemon: Daemon, ticks: int = 3) -> None:
    for _ in range(ticks):
        daemon.iterate()
        time.sleep(0.02)


def _spec(daemon: Daemon, jobid: str) -> JobSpec:
    return JobSpec.read(daemon._spec_path(jobid))


def _seen(tmp_path: Path, jobid: str) -> str:
    path = tmp_path / f"{jobid}.seen"
    deadline = time.monotonic() + 10
    while not (path.exists() and path.read_text().strip()) and time.monotonic() < deadline:
        time.sleep(0.02)
    return path.read_text().strip()


def _finish(daemon: Daemon, jobid: str, tmp_path: Path) -> None:
    (tmp_path / f"{jobid}.go").write_text("")
    deadline = time.monotonic() + 10
    while _spec(daemon, jobid).state != JobState.COMPLETED:
        assert time.monotonic() < deadline, f"{jobid} did not finish"
        daemon.iterate()
        time.sleep(0.02)


class TestAdmission:
    def test_jobs_get_the_lowest_free_devices_and_wait_for_enough(
        self, gpu_daemon: Daemon, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("CUDA_VISIBLE_DEVICES", raising=False)
        _job(gpu_daemon, "a", 2, tmp_path)
        _settle(gpu_daemon, 1)
        _job(gpu_daemon, "b", 2, tmp_path)
        _job(gpu_daemon, "c", 1, tmp_path)
        _job(gpu_daemon, "plain", None, tmp_path)
        _settle(gpu_daemon)
        assert _spec(gpu_daemon, "a").gpu_devices == ["0", "1"]
        assert _seen(tmp_path, "a") == "0,1 0,1"
        assert _spec(gpu_daemon, "b").state == JobState.PENDING  # one device free
        assert _spec(gpu_daemon, "c").gpu_devices == ["2"]
        assert _seen(tmp_path, "c") == "2 2"
        plain = _spec(gpu_daemon, "plain")
        assert plain.state == JobState.RUNNING and plain.gpu_devices is None
        assert _seen(tmp_path, "plain") == "unset unset"
        snapshot = gpu_daemon._admission_snapshot
        assert snapshot is not None
        assert (snapshot["gpu_devices"], snapshot["used_gpus"]) == (DEVICES, 3)

        _finish(gpu_daemon, "a", tmp_path)
        _settle(gpu_daemon)
        assert _spec(gpu_daemon, "b").gpu_devices == ["0", "1"]
        for jobid in ("b", "c", "plain"):
            _finish(gpu_daemon, jobid, tmp_path)

    def test_jobs_admitted_in_one_pass_never_share_a_device(
        self, gpu_daemon: Daemon, tmp_path: Path, caplog: pytest.LogCaptureFixture,
    ) -> None:
        for jobid in ("x", "y", "z", "w"):
            _job(gpu_daemon, jobid, 1, tmp_path)
        with caplog.at_level("INFO"):
            gpu_daemon.iterate()
        assert [_spec(gpu_daemon, j).gpu_devices for j in ("x", "y", "z")] == [
            ["0"], ["1"], ["2"]]
        assert _spec(gpu_daemon, "w").state == JobState.PENDING
        # It waits without being set up and turned back at the claim.
        assert "changed its GPU request" not in caplog.text
        for jobid in ("x", "y", "z"):
            _finish(gpu_daemon, jobid, tmp_path)

    def test_a_request_over_the_daemons_devices_stays_pending(
        self, gpu_daemon: Daemon, tmp_path: Path,
    ) -> None:
        _job(gpu_daemon, "big", 4, tmp_path)
        _job(gpu_daemon, "small", 1, tmp_path)
        _settle(gpu_daemon)
        assert _spec(gpu_daemon, "big").state == JobState.PENDING
        assert _spec(gpu_daemon, "small").gpu_devices == ["0"]
        _finish(gpu_daemon, "small", tmp_path)

    def test_a_daemon_without_devices_admits_no_gpu_job(self, tmp_path: Path) -> None:
        d = Daemon(max_cpus=4, poll_interval=0.05, queue_dir=tmp_path / "queue",
                   jobs_dir=tmp_path / "jobs")
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)
        _job(d, "gpu", 1, tmp_path)
        _settle(d)
        assert _spec(d, "gpu").state == JobState.PENDING
        assert d._admission_snapshot is not None
        assert (d._admission_snapshot["gpu_devices"], d._admission_snapshot["used_gpus"]) == (
            None, 0)

    def test_a_forged_device_list_never_counts(self, gpu_daemon: Daemon, tmp_path: Path) -> None:
        _job(gpu_daemon, "forged-plain", None, tmp_path, gpu_devices=["0", "1", "2"])
        _job(gpu_daemon, "forged-gpu", 1, tmp_path, gpu_devices=["2"])
        _settle(gpu_daemon)
        assert _spec(gpu_daemon, "forged-plain").gpu_devices is None
        assert _spec(gpu_daemon, "forged-gpu").gpu_devices == ["0"]
        for jobid in ("forged-plain", "forged-gpu"):
            _finish(gpu_daemon, jobid, tmp_path)

    def test_the_claim_refuses_a_request_that_does_not_match_its_devices(
        self, gpu_daemon: Daemon, tmp_path: Path,
    ) -> None:
        _job(gpu_daemon, "changed", 2, tmp_path)
        assert gpu_daemon._start_job(_spec(gpu_daemon, "changed"), gpu_devices=("0",)) is False
        spec = _spec(gpu_daemon, "changed")
        assert (spec.state, spec.gpu_devices) == (JobState.PENDING, None)

    def test_reattached_and_surviving_jobs_keep_their_devices_busy(
        self, gpu_daemon: Daemon, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(daemon_module, "_pgroup_alive", lambda pgid: True)
        workspace = gpu_daemon.jobs_dir / "survivor"
        workspace.mkdir()
        JobSpec(id="before-restart", command=["true"], cwd=str(workspace), cpus=1, gpus=1,
                gpu_devices=["0"], state=JobState.RUNNING, pgid=999_003,
                ).write(gpu_daemon._spec_path("before-restart"))
        gpu_daemon._reattach_or_interrupt_at_startup()
        assert gpu_daemon._orphans["before-restart"].gpu_devices == ("0",)
        gpu_daemon._terminal_survivors["killed"] = _TerminalSurvivor(
            pgid=999_004, cpus=1, mem_mb=None, state=JobState.KILLED, workspace=workspace,
            deadline=time.monotonic() + 60, gpu_devices=("1",))
        assert gpu_daemon._busy_gpu_devices() == {"0", "1"}
        gpu_daemon._orphans["other"] = _OrphanJob(pgid=999_005, cpus=1, mem_mb=None)
        assert gpu_daemon._busy_gpu_devices() == {"0", "1"}


def test_the_job_environment_names_only_its_devices() -> None:
    gpu = JobSpec(id="j", command=["true"], cwd="/w", cpus=1, gpus=2, gpu_devices=["3", "5"])
    env = _vq_job_env(gpu, workdir="/w")
    assert (env["CUDA_VISIBLE_DEVICES"], env["VQ_GPU_DEVICES"], env["VQ_GPUS"]) == (
        "3,5", "3,5", "2")
    scheduler = JobSpec(id="s", command=["true"], cwd="/w", cpus=1, gpus=2,
                        scheduler_target="host_f")
    assert "CUDA_VISIBLE_DEVICES" not in _vq_job_env(scheduler, workdir="/w")


class TestDeviceLists:
    @pytest.mark.parametrize(("devices", "message"), [
        ([], "1 to 64 device IDs"),
        (["0", "0"], "must not repeat"),
        (["0,1"], "plain token"),
        (["-1"], "plain token"),
    ])
    def test_invalid_lists_are_refused(self, devices: list[str], message: str) -> None:
        with pytest.raises(ValueError, match=message):
            config.DaemonRunConfig(gpu_devices=devices)
        with pytest.raises(ValueError, match=message):
            Daemon(max_cpus=1, gpu_devices=devices)

    def test_a_uuid_style_id_is_accepted(self) -> None:
        assert config.DaemonRunConfig(gpu_devices=["GPU-1a2b3c4d", "MIG-0:1"]).gpu_devices == [
            "GPU-1a2b3c4d", "MIG-0:1"]

    def test_daemon_run_takes_the_flag_or_the_config(
        self, cfg_dir: Path, captured_daemon: dict[str, Any],  # noqa: F811
    ) -> None:
        result = CliRunner().invoke(main, ["daemon", "run", "--gpu-devices", "0, 1"])
        assert result.exit_code == 0, result.output
        assert captured_daemon["gpu_devices"] == ["0", "1"]
        (cfg_dir / "config.toml").write_text('[daemon]\ngpu_devices = ["2", "3"]\n')
        captured_daemon.clear()
        assert CliRunner().invoke(main, ["daemon", "run"]).exit_code == 0
        assert captured_daemon["gpu_devices"] == ["2", "3"]
        captured_daemon.clear()
        assert CliRunner().invoke(main, ["daemon", "run", "--gpu-devices", "7"]).exit_code == 0
        assert captured_daemon["gpu_devices"] == ["7"]  # the flag wins
        refused = CliRunner().invoke(main, ["daemon", "run", "--gpu-devices", "0,0"])
        assert refused.exit_code == 2 and "must not repeat" in refused.output

    def test_without_either_there_are_none(
        self, cfg_dir: Path, captured_daemon: dict[str, Any],  # noqa: F811
    ) -> None:
        assert CliRunner().invoke(main, ["daemon", "run"]).exit_code == 0
        assert captured_daemon["gpu_devices"] is None


class TestSubmit:
    @pytest.fixture(autouse=True)
    def _state(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))

    def _daemon_says(self, monkeypatch: pytest.MonkeyPatch, answer: Any) -> list[str]:
        from vq import rpc

        asked: list[str] = []

        def call(method: str, *_args: Any, **_kwargs: Any) -> Any:
            if method != "get_admission_snapshot":
                raise ConnectionError("no daemon for anything else")
            asked.append(method)
            if isinstance(answer, Exception):
                raise answer
            return answer

        monkeypatch.setattr(rpc, "call", call)
        return asked

    def _submit(self, tmp_path: Path, *args: str) -> Any:
        source = tmp_path / "run.py"
        source.write_text("pass\n")
        return CliRunner().invoke(main, ["submit", "localhost", *args, str(source)])

    def test_a_daemon_with_enough_devices_takes_it(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        asked = self._daemon_says(monkeypatch, {"gpu_devices": DEVICES, "used_gpus": 3})
        result = self._submit(tmp_path, "--gpus", "3")
        assert result.exit_code == 0, result.output
        spec = JobSpec.read(paths.spec_path(result.output.strip().splitlines()[-1]))
        assert (spec.gpus, spec.gpu_devices, spec.scheduler_target) == (3, None, None)
        assert asked == ["get_admission_snapshot"]  # busy devices only delay it

    @pytest.mark.parametrize(("answer", "message"), [
        ({"gpu_devices": DEVICES}, "this host's daemon has only 3 GPU device(s)"),
        ({"gpu_devices": None}, "start it with --gpu-devices"),
        ({"gpu_devices": []}, "start it with --gpu-devices"),
        ({"gpu_devices": "0,1,2,3"}, "start it with --gpu-devices"),
        ({"max_cpus": 4}, "start it with --gpu-devices"),  # an older daemon
        (None, "start it with --gpu-devices"),
        (ConnectionError("no daemon"), "start it with --gpu-devices"),
    ])
    def test_otherwise_it_is_refused_and_nothing_queued(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, answer: Any, message: str,
    ) -> None:
        self._daemon_says(monkeypatch, answer)
        result = self._submit(tmp_path, "--gpus", "4")
        assert result.exit_code == 2 and message in result.output
        assert not list(paths.queue_dir().glob("*.json"))

    def test_without_gpus_the_daemon_is_not_asked(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        asked = self._daemon_says(monkeypatch, None)
        assert self._submit(tmp_path).exit_code == 0
        assert asked == []

    def test_a_scheduler_target_is_not_asked(self, monkeypatch: pytest.MonkeyPatch,
                                             tmp_path: Path) -> None:
        asked = self._daemon_says(monkeypatch, None)
        source = tmp_path / "run.py"
        source.write_text("pass\n")
        submit_module.submit_local(host="localhost", input_file=str(source),
                                   python="/cluster/bin/python", gpus=2,
                                   scheduler_target="host_f")
        assert asked == []

    def test_auto_placement_onto_a_daemon_host_is_refused(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from vq import cli

        monkeypatch.setattr(cli, "_pick_auto_host", lambda *_a, **_k: "localhost")
        source = tmp_path / "run.py"
        source.write_text("pass\n")
        result = CliRunner().invoke(main, ["submit", "auto", "--gpus", "1", str(source)])
        assert result.exit_code == 2
        assert "--gpus cannot be auto-placed onto a daemon host" in result.output
