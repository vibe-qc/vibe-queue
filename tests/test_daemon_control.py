"""Tests for daemon process lifecycle: pidfile + start/stop/status helpers.

The full start/stop spawn cycle is exercised in test_e2e.py using a real
subprocess; this file focuses on the pidfile primitives and the
DaemonAlreadyRunning / DaemonStartFailed branches in isolation.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from vq import daemon_control, fleet_rollout

# Literal property output from the fixed read-only systemctl probe, 2026-09-12.
# Unlike the older synthetic fixture, real not-found units omit ExecStart.
_NOT_FOUND_OUTPUTS = {
    "host_0": (
        "MainPID=0\nUser=\nId=vq-daemon-multi-user.service\n"
        "LoadState=not-found\nActiveState=inactive\nSubState=dead\n"
    ),
    "host_b-host_e": (
        "Id=vq-daemon-multi-user.service\nLoadState=not-found\n"
        "ActiveState=inactive\nSubState=dead\nMainPID=0\nUser=\n"
    ),
}


def _probe_unit_output(monkeypatch: pytest.MonkeyPatch, stdout: str):
    monkeypatch.setattr(
        daemon_control, "_trusted_systemctl_path", lambda: Path("/usr/bin/systemctl"),
    )
    monkeypatch.setattr(
        daemon_control.subprocess, "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args[0], 0, stdout=stdout, stderr="",
        ),
    )
    return daemon_control._systemd_multi_user_service_evidence()


def _root_lane(service, *, policy="absent", multi_user=False):
    return fleet_rollout._root_daemon_provenance_lane(
        {"host": {"checks": [{
            "name": "daemon_rpc", "ok": True, "multi_user": multi_user,
            "system_multi_user": {
                "source": "/etc/vq/config.toml", "status": policy,
                "enabled": policy == "enabled", "error": None,
            },
            "system_service": service,
        }]}},
        host="host", target_sha="a" * 40, target_version="0.26.2",
        target_tree_sha256="b" * 64,
    )


@pytest.mark.parametrize("host", _NOT_FOUND_OUTPUTS)
@pytest.mark.parametrize("policy", ["absent", "disabled"])
def test_live_not_found_without_execstart_is_not_applicable(
    monkeypatch: pytest.MonkeyPatch, host: str, policy: str,
) -> None:
    evidence = _probe_unit_output(monkeypatch, _NOT_FOUND_OUTPUTS[host])
    assert evidence["status"] == "ok"
    assert evidence["error"] is None
    assert evidence["id"] == "vq-daemon-multi-user.service"
    assert evidence["load_state"] == "not-found"
    assert evidence["main_pid"] == 0
    assert evidence["exec_start"] == ""
    assert evidence["executable"] is None
    assert evidence["argv"] == []
    lane = _root_lane(evidence, policy=policy)
    assert lane.applicable is False
    assert lane.decision == "skip"
    assert lane.evidence["comparison"] == {"status": "not-applicable", "errors": []}


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("LoadState=not-found", "LoadState=loaded"),
        ("LoadState=not-found", "LoadState=error"),
        ("ActiveState=inactive", "ActiveState=active"),
        ("ActiveState=inactive", "ActiveState=activating"),
        ("ActiveState=inactive", "ActiveState=deactivating"),
        ("SubState=dead", "SubState=running"),
        ("SubState=dead", "SubState=start"),
        ("MainPID=0", "MainPID=1"),
        ("MainPID=0", "MainPID=-1"),
        ("MainPID=0", "MainPID=invalid"),
        ("MainPID=0", "MainPID=00"),
        ("Id=vq-daemon-multi-user.service", "Id=other.service"),
        ("User=", "User=root"),
        ("User=\n", ""),
        ("SubState=dead\n", ""),
        ("MainPID=0\n", ""),
        ("MainPID=0", "MainPID=0\nMainPID=42"),
        ("LoadState=not-found", "LoadState=not-found\nLoadState=loaded"),
        ("User=\n", "User=\nExecStart\n"),
        ("User=\n", "User=\nmalformed output\n"),
    ],
)
def test_not_found_without_execstart_refuses_ambiguous_or_incomplete_identity(
    monkeypatch: pytest.MonkeyPatch, old: str, new: str,
) -> None:
    output = _NOT_FOUND_OUTPUTS["host_b-host_e"].replace(old, new)
    assert output != _NOT_FOUND_OUTPUTS["host_b-host_e"]
    evidence = _probe_unit_output(monkeypatch, output)
    assert evidence["status"] == "unknown"
    assert evidence["error"]
    assert _root_lane(evidence).decision == "defer"


@pytest.mark.parametrize(
    ("policy", "multi_user"), [("enabled", False), ("absent", True)],
)
def test_not_found_without_execstart_cannot_override_enabled_or_live_daemon(
    monkeypatch: pytest.MonkeyPatch, policy: str, multi_user: bool,
) -> None:
    evidence = _probe_unit_output(monkeypatch, _NOT_FOUND_OUTPUTS["host_b-host_e"])
    lane = _root_lane(evidence, policy=policy, multi_user=multi_user)
    assert lane.applicable is True
    assert lane.decision == "defer"


class TestPidfile:
    def test_write_and_read(self, tmp_path: Path) -> None:
        pf = tmp_path / "daemon.pid"
        daemon_control.write_pidfile(pf)
        assert pf.read_text().strip() == str(os.getpid())
        assert daemon_control.read_pidfile(pf) == os.getpid()

    def test_read_missing_returns_none(self, tmp_path: Path) -> None:
        assert daemon_control.read_pidfile(tmp_path / "missing.pid") is None

    def test_read_garbage_returns_none(self, tmp_path: Path) -> None:
        pf = tmp_path / "daemon.pid"
        pf.write_text("not a number")
        assert daemon_control.read_pidfile(pf) is None

    def test_remove_idempotent(self, tmp_path: Path) -> None:
        pf = tmp_path / "daemon.pid"
        daemon_control.remove_pidfile(pf)  # missing -> no error
        pf.write_text("1234")
        daemon_control.remove_pidfile(pf)
        assert not pf.exists()
        daemon_control.remove_pidfile(pf)  # already gone -> still no error


class TestIsDaemonRunning:
    def test_no_pidfile_means_not_running(self, tmp_path: Path) -> None:
        assert not daemon_control.is_daemon_running(tmp_path / "missing.pid")

    def test_self_pid_counts_as_running(self, tmp_path: Path) -> None:
        pf = tmp_path / "daemon.pid"
        pf.write_text(f"{os.getpid()}\n")
        assert daemon_control.is_daemon_running(pf)

    def test_dead_pid_cleaned_up(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pf = tmp_path / "daemon.pid"
        pf.write_text("99999\n")

        def fake_kill(pid: int, sig: int) -> None:
            raise ProcessLookupError(f"no such pid {pid}")

        monkeypatch.setattr("vq.daemon_control.os.kill", fake_kill)
        assert not daemon_control.is_daemon_running(pf)
        assert not pf.exists()  # cleaned up


class TestIsDaemonServing:
    def test_rpc_healthy_counts_even_without_pidfile(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("vq.rpc.ping", lambda **kw: {"version": "9.9.9"})

        assert daemon_control.is_daemon_serving(tmp_path / "missing.pid")

    def test_rpc_down_falls_back_to_pidfile(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("vq.rpc.ping", lambda **kw: None)
        pf = tmp_path / "daemon.pid"
        pf.write_text(f"{os.getpid()}\n")

        assert daemon_control.is_daemon_serving(pf)


class TestSystemMultiUserServiceEvidence:
    @pytest.mark.parametrize(
        ("platform", "expected_status"),
        [("darwin", "unsupported"), ("linux", "unknown")],
    )
    def test_missing_systemctl_distinguishes_unsupported_platforms(
        self,
        monkeypatch: pytest.MonkeyPatch,
        platform: str,
        expected_status: str,
    ) -> None:
        monkeypatch.setattr(daemon_control, "_trusted_systemctl_path", lambda: None)
        monkeypatch.setattr(daemon_control.sys, "platform", platform)

        evidence = daemon_control._systemd_multi_user_service_evidence()

        assert evidence["status"] == expected_status
        assert evidence["main_pid"] is None

    def test_systemctl_show_is_parsed_without_a_shell(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        seen: dict[str, object] = {}
        monkeypatch.setattr(
            daemon_control,
            "_trusted_systemctl_path",
            lambda: Path("/usr/bin/systemctl"),
        )

        def fake_run(argv: list[str], **kwargs: object):
            seen.update(argv=argv, kwargs=kwargs)
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=(
                    "Id=vq-daemon-multi-user.service\n"
                    "LoadState=loaded\n"
                    "MainPID=4242\n"
                    "User=root\n"
                    "ExecStart={ path=/opt/vq/venv/bin/vq ; "
                    "argv[]=/opt/vq/venv/bin/vq daemon run ; "
                    "ignore_errors=no ; }\n"
                    "ActiveState=active\n"
                    "SubState=running\n"
                ),
                stderr="",
            )

        monkeypatch.setattr(daemon_control.subprocess, "run", fake_run)

        evidence = daemon_control._systemd_multi_user_service_evidence()

        assert evidence == {
            "active_state": "active",
            "argv": ["/opt/vq/venv/bin/vq", "daemon", "run"],
            "error": None,
            "exec_start": (
                "{ path=/opt/vq/venv/bin/vq ; "
                "argv[]=/opt/vq/venv/bin/vq daemon run ; "
                "ignore_errors=no ; }"
            ),
            "executable": "/opt/vq/venv/bin/vq",
            "id": "vq-daemon-multi-user.service",
            "load_state": "loaded",
            "main_pid": 4242,
            "source": "/usr/bin/systemctl",
            "status": "ok",
            "sub_state": "running",
            "user": "root",
        }
        assert seen["argv"] == [
            "/usr/bin/systemctl",
            "show",
            "vq-daemon-multi-user.service",
            "--no-pager",
            "--property=Id",
            "--property=LoadState",
            "--property=ActiveState",
            "--property=SubState",
            "--property=MainPID",
            "--property=User",
            "--property=ExecStart",
        ]
        assert seen["kwargs"] == {
            "capture_output": True,
            "check": False,
            "text": True,
            "timeout": 10,
            "stdin": subprocess.DEVNULL,
        }

    def test_missing_unit_is_complete_inactive_evidence(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            daemon_control,
            "_trusted_systemctl_path",
            lambda: Path("/usr/bin/systemctl"),
        )
        monkeypatch.setattr(
            daemon_control.subprocess,
            "run",
            lambda *args, **kwargs: subprocess.CompletedProcess(
                args[0],
                0,
                stdout=(
                    "Id=vq-daemon-multi-user.service\n"
                    "LoadState=not-found\n"
                    "ActiveState=inactive\n"
                    "SubState=dead\n"
                    "MainPID=0\n"
                    "User=\n"
                    "ExecStart=\n"
                ),
                stderr="",
            ),
        )

        evidence = daemon_control._systemd_multi_user_service_evidence()

        assert evidence["status"] == "ok"
        assert evidence["load_state"] == "not-found"
        assert evidence["active_state"] == "inactive"
        assert evidence["main_pid"] == 0
        assert evidence["executable"] is None
        assert evidence["argv"] == []

    def test_activating_unit_without_a_pid_remains_unknown(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            daemon_control,
            "_trusted_systemctl_path",
            lambda: Path("/usr/bin/systemctl"),
        )
        monkeypatch.setattr(
            daemon_control.subprocess,
            "run",
            lambda *args, **kwargs: subprocess.CompletedProcess(
                args[0],
                0,
                stdout=(
                    "Id=vq-daemon-multi-user.service\n"
                    "LoadState=loaded\n"
                    "ActiveState=activating\n"
                    "SubState=start\n"
                    "MainPID=0\n"
                    "User=root\n"
                    "ExecStart=\n"
                ),
                stderr="",
            ),
        )

        evidence = daemon_control._systemd_multi_user_service_evidence()

        assert evidence["status"] == "unknown"
        assert "ExecStart" in str(evidence["error"])

    @pytest.mark.parametrize(
        ("returncode", "stdout", "stderr", "detail"),
        [
            (1, "", "access denied", "access denied"),
            (
                0,
                "Id=vq-daemon-multi-user.service\n"
                "LoadState=loaded\n"
                "ActiveState=active\n"
                "SubState=running\n"
                "MainPID=not-a-pid\n"
                "User=root\n"
                "ExecStart={ path=/opt/vq/venv/bin/vq ; "
                "argv[]=/opt/vq/venv/bin/vq daemon run ; }\n",
                "",
                "MainPID",
            ),
        ],
    )
    def test_unusable_systemctl_evidence_is_structured_and_fail_closed(
        self,
        monkeypatch: pytest.MonkeyPatch,
        returncode: int,
        stdout: str,
        stderr: str,
        detail: str,
    ) -> None:
        monkeypatch.setattr(
            daemon_control,
            "_trusted_systemctl_path",
            lambda: Path("/usr/bin/systemctl"),
        )
        monkeypatch.setattr(
            daemon_control.subprocess,
            "run",
            lambda *args, **kwargs: subprocess.CompletedProcess(
                args[0], returncode, stdout=stdout, stderr=stderr
            ),
        )

        evidence = daemon_control._systemd_multi_user_service_evidence()

        assert evidence["active_state"] is None
        assert evidence["main_pid"] is None
        assert evidence["executable"] is None
        assert evidence["status"] == "unknown"
        assert detail in str(evidence["error"])


class TestStartFailure:
    def test_start_when_already_running_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pf = tmp_path / "daemon.pid"
        pf.write_text(f"{os.getpid()}\n")
        with pytest.raises(daemon_control.DaemonAlreadyRunning):
            daemon_control.start_daemon(pidfile=pf, log_file=tmp_path / "log")

    def test_start_failure_when_subprocess_exits_immediately(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Stub Popen so the "child" exits with code 7 right away.
        class FakePopen:
            def __init__(self, *args: object, **kwargs: object) -> None:
                self.pid = 1234
                self._rc = 7
                self.returncode = 7

            def poll(self) -> int | None:
                return self._rc

        monkeypatch.setattr("vq.daemon_control.subprocess.Popen", FakePopen)
        with pytest.raises(daemon_control.DaemonStartFailed, match="exited with code 7"):
            daemon_control.start_daemon(
                pidfile=tmp_path / "daemon.pid",
                log_file=tmp_path / "log",
                spawn_timeout=1.0,
            )

    def test_start_timeout_when_no_pidfile_appears(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        class HangingPopen:
            def __init__(self, *args: object, **kwargs: object) -> None:
                self.pid = 1234
                self.returncode: int | None = None

            def poll(self) -> int | None:
                return None  # still running, but never writes a pidfile

        monkeypatch.setattr("vq.daemon_control.subprocess.Popen", HangingPopen)
        with pytest.raises(daemon_control.DaemonStartFailed, match="did not write pidfile"):
            daemon_control.start_daemon(
                pidfile=tmp_path / "daemon.pid",
                log_file=tmp_path / "log",
                spawn_timeout=0.2,
            )


class TestStopDaemon:
    def test_stop_when_not_running_returns_none(self, tmp_path: Path) -> None:
        result = daemon_control.stop_daemon(pidfile=tmp_path / "missing.pid")
        assert result is None

    def test_stop_signals_then_cleans_up_after_exit(
        self, tmp_path: Path
    ) -> None:
        # Spawn a real sleep so SIGTERM has a target.
        proc = subprocess.Popen(["sleep", "30"])
        try:
            pf = tmp_path / "daemon.pid"
            pf.write_text(f"{proc.pid}\n")
            # Race: stop_daemon checks is_daemon_running, sees alive, signals.
            # We then also have to remove the pidfile to simulate the daemon's
            # own shutdown. Do that after a small pause.
            import threading
            import time as _time

            def cleanup() -> None:
                _time.sleep(0.2)
                proc.wait()
                if pf.exists():
                    pf.unlink()

            threading.Thread(target=cleanup, daemon=True).start()
            pid = daemon_control.stop_daemon(pidfile=pf, timeout=5.0)
            assert pid == proc.pid
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
