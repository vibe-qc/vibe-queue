"""#168: opt-in GPU device isolation for a multi-user daemon's jobs.

With `[daemon] gpu_isolation = true`, every job's system scope gets
`DevicePolicy=closed` and `DeviceAllow=` for the operator's allow list. A
GPU job also gets its own `/dev/nvidiaN` and the shared NVIDIA control
nodes, so a job can open only the GPUs it was given. systemd can only allow,
never deny, and a user manager cannot enforce device policy: the daemon
refuses the setting on a single-user daemon or with non-numeric devices.
Checked on the generated `systemd-run` arguments; no GPU or systemd is used.
"""
from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vq import cgroup, config, paths, throttle
from vq import daemon as daemon_module
from vq.cli import main
from vq.daemon import NVIDIA_CONTROL_DEVICES, Daemon
from vq.spec import JobSpec, JobState

UID = "1000"


class TestScope:
    def test_a_system_scope_closes_its_device_policy(self, monkeypatch: pytest.MonkeyPatch,
                                                     ) -> None:
        monkeypatch.setattr(cgroup, "_systemd_run_path", lambda: "/usr/bin/systemd-run")
        argv = cgroup.wrap_command(["run.sh"], unit_name="vq-job-x", run_as_uid=1000,
                                   run_as_gid=1000,
                                   device_allow=["/dev/nvidia1", "char-infiniband_verbs"])
        tail = argv[argv.index("--property=DevicePolicy=closed"):]
        assert tail == ["--property=DevicePolicy=closed",
                        "--property=DeviceAllow=/dev/nvidia1 rw",
                        "--property=DeviceAllow=char-infiniband_verbs rw", "--", "run.sh"]
        # An empty allow list still closes the policy: no device but the standard ones.
        closed = cgroup.wrap_command(["run.sh"], run_as_uid=1000, device_allow=[])
        assert "--property=DevicePolicy=closed" in closed
        assert not any(arg.startswith("--property=DeviceAllow") for arg in closed)
        plain = cgroup.wrap_command(["run.sh"], run_as_uid=1000)
        assert not any("Device" in arg for arg in plain)

    def test_a_user_scope_cannot_isolate(self) -> None:
        with pytest.raises(RuntimeError, match="needs a system-mode scope"):
            cgroup.wrap_command(["run.sh"], unit_name="vq-job-x", device_allow=[])


@pytest.fixture
def isolated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    """A multi-user daemon with isolation on; its scope arguments are recorded."""
    monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "multi-user"))
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "config.toml").write_text("[multi_user]\nenabled = true\n\n[quotas]\n")
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(config_dir))
    scopes: list[dict[str, Any]] = []

    def wrap(command: list[str], **kwargs: Any) -> list[str]:
        scopes.append(kwargs)
        return list(command)

    monkeypatch.setattr(cgroup, "systemd_run_on_path", lambda: True)
    monkeypatch.setattr(cgroup, "available", lambda: False)
    monkeypatch.setattr(cgroup, "scope_exists", lambda *_a, **_k: False)
    monkeypatch.setattr(cgroup, "stop_scope", lambda *_a, **_k: True)
    monkeypatch.setattr(cgroup, "wrap_command", wrap)
    monkeypatch.setattr(daemon_module, "_gid_for_uid", lambda _uid: 1)
    monkeypatch.setattr(daemon_module, "_chown_tree", lambda *_a: None)
    monkeypatch.setattr(throttle, "apply_persistent_throttle_if_set", lambda *_a, **_k: None)
    daemon = Daemon(max_cpus=8, max_jobs=8, max_mem_mb=100_000, poll_interval=0.05,
                    multi_user=True, queue_dir=tmp_path / "q", jobs_dir=tmp_path / "j",
                    gpu_devices=["0", "1"], gpu_isolation=True,
                    gpu_isolation_allow=["char-infiniband_verbs"])
    try:
        yield daemon, scopes
    finally:
        for rj in daemon._running.values():
            rj.popen.kill()
            rj.popen.wait(timeout=10)
        daemon._close_running_logs()
        daemon._queue_lock_fd.close()


def _job(job_id: str, gpus: int | None) -> None:
    workspace = paths.user_workspace_dir(UID, job_id)
    workspace.mkdir(parents=True, exist_ok=True)
    spec_path = paths.user_spec_path(UID, job_id)
    spec_path.parent.mkdir(parents=True, exist_ok=True)
    JobSpec(id=job_id, command=["true"], cwd=str(workspace), cpus=1, gpus=gpus,
            state=JobState.PENDING, submitter=UID).write(spec_path)


class TestDaemon:
    def test_each_job_opens_only_its_own_gpus(self, isolated: Any) -> None:
        daemon, scopes = isolated
        assert daemon._job_devices(("1",)) == [
            "char-infiniband_verbs", "/dev/nvidia1", *NVIDIA_CONTROL_DEVICES]
        assert daemon._job_devices(()) == ["char-infiniband_verbs"]  # no GPU at all
        _job("gpujob000001", 1)
        daemon._dispatch_pending()
        assert scopes, "the job was not started in a scope"
        assert scopes[-1]["device_allow"] == [
            "char-infiniband_verbs", "/dev/nvidia0", *NVIDIA_CONTROL_DEVICES]
        assert scopes[-1]["run_as_uid"] == int(UID)

    def test_without_isolation_the_scope_is_unrestricted(self, tmp_path: Path) -> None:
        daemon = Daemon(max_cpus=1, queue_dir=tmp_path / "q", jobs_dir=tmp_path / "j",
                        gpu_devices=["0"])
        try:
            assert daemon._job_devices(("0",)) is None
        finally:
            daemon._queue_lock_fd.close()

    @pytest.mark.parametrize(("kwargs", "message"), [
        ({"multi_user": False, "gpu_devices": ["0"]}, "needs a multi-user daemon"),
        ({"multi_user": True, "gpu_devices": ["GPU-1a2b"]}, "needs numeric gpu_devices"),
    ])
    def test_a_daemon_that_cannot_isolate_refuses_to_start(
        self, tmp_path: Path, kwargs: dict[str, Any], message: str,
    ) -> None:
        with pytest.raises(ValueError, match=message):
            Daemon(max_cpus=1, queue_dir=tmp_path / "q", jobs_dir=tmp_path / "j",
                   gpu_isolation=True, **kwargs)


class TestConfig:
    @pytest.mark.parametrize("entry", [
        "/dev/infiniband/uverbs0", "/dev/infiniband/*", "char-infiniband_verbs", "block-loop"])
    def test_device_entries_and_classes_are_accepted(self, entry: str) -> None:
        assert config.DaemonRunConfig(gpu_isolation_allow=[entry]).gpu_isolation_allow == [entry]

    @pytest.mark.parametrize("entry", [
        "infiniband", "/dev/../etc/shadow", "char-", "/dev/a b", "/dev/x rw", "pipe-x"])
    def test_other_entries_are_refused(self, entry: str) -> None:
        with pytest.raises(ValueError, match="gpu_isolation_allow entry"):
            config.DaemonRunConfig(gpu_isolation_allow=[entry])

    def test_daemon_run_reads_it_and_refuses_cleanly(self, tmp_path: Path,
                                                     monkeypatch: pytest.MonkeyPatch) -> None:
        from vq import cli

        captured: dict[str, Any] = {}

        class FakeDaemon:
            def __init__(self, **kwargs: Any) -> None:
                captured.update(kwargs)
                if kwargs["gpu_isolation"]:
                    raise ValueError("gpu_isolation needs a multi-user daemon")

            def run(self) -> None:
                return None

        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
        monkeypatch.setattr(cli, "Daemon", FakeDaemon)
        monkeypatch.setattr(cli, "setup_daemon_logging", lambda *_a, **_k: None)
        monkeypatch.setattr(cli, "write_pidfile", lambda **_k: None)
        monkeypatch.setattr(cli, "remove_pidfile", lambda **_k: None)
        monkeypatch.setattr(cli.os, "geteuid", lambda: 1000)
        (cfg_dir / "config.toml").write_text(
            '[daemon]\ngpu_devices = ["0"]\ngpu_isolation = true\n'
            'gpu_isolation_allow = ["char-infiniband_verbs"]\n')
        refused = CliRunner().invoke(main, ["daemon", "run"])
        assert refused.exit_code == 1 and "needs a multi-user daemon" in refused.output
        assert (captured["gpu_isolation"], captured["gpu_isolation_allow"]) == (
            True, ["char-infiniband_verbs"])
        (cfg_dir / "config.toml").write_text('[daemon]\ngpu_devices = ["0"]\n')
        assert CliRunner().invoke(main, ["daemon", "run"]).exit_code == 0
        assert captured["gpu_isolation"] is False
