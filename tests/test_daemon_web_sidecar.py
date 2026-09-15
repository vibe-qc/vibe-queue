"""Daemon-owned web dashboard sidecar tests."""
from __future__ import annotations

import subprocess
from typing import Any

import pytest
from click.testing import CliRunner

from vq import cli
from vq.cli import main


class FakeProc:
    def __init__(self, *, rc: int | None = None) -> None:
        self.rc = rc
        self.terminated = False
        self.killed = False
        self.wait_calls = 0

    def poll(self) -> int | None:
        return self.rc

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.killed = True
        self.rc = -9

    def wait(self, timeout: float | None = None) -> int:
        self.wait_calls += 1
        self.rc = 0
        return 0


def test_daemon_run_help_exposes_web_sidecar_options() -> None:
    result = CliRunner().invoke(main, ["daemon", "run", "--help"])

    assert result.exit_code == 0
    assert "--web" in result.output
    assert "--web-host" in result.output
    assert "--web-port" in result.output


def test_start_web_sidecar_uses_current_python_and_web_command(
    monkeypatch,
) -> None:
    calls: list[list[str]] = []
    proc = FakeProc()

    def fake_popen(argv: list[str], *args: Any, **kwargs: Any) -> FakeProc:
        calls.append(argv)
        return proc

    monkeypatch.setattr(cli.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(cli.time, "sleep", lambda _seconds: None)

    returned = cli._start_web_sidecar(
        host="127.0.0.1",
        port=8765,
        log_level="warning",
        acknowledge_public_bind=False,
    )

    assert returned is proc
    assert calls == [
        [
            cli.sys.executable,
            "-m",
            "vq.cli",
            "web",
            "run",
            "--host",
            "127.0.0.1",
            "--port",
            "8765",
            "--log-level",
            "warning",
        ]
    ]


def test_start_web_sidecar_forwards_public_bind_ack(monkeypatch) -> None:
    calls: list[list[str]] = []
    proc = FakeProc()

    def fake_popen(argv: list[str], *args: Any, **kwargs: Any) -> FakeProc:
        calls.append(argv)
        return proc

    monkeypatch.setattr(cli.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(cli.time, "sleep", lambda _seconds: None)

    cli._start_web_sidecar(
        host="0.0.0.0",
        port=9000,
        log_level="info",
        acknowledge_public_bind=True,
    )

    assert calls[0][-1] == "--i-understand-public-bind"


def test_start_web_sidecar_fails_if_child_exits_immediately(monkeypatch) -> None:
    monkeypatch.setattr(cli.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(cli.subprocess, "Popen", lambda *_a, **_kw: FakeProc(rc=2))

    with pytest.raises(cli.click.ClickException) as exc:
        cli._start_web_sidecar(
            host="127.0.0.1",
            port=8765,
            log_level="info",
            acknowledge_public_bind=False,
        )

    assert "web sidecar exited during startup" in str(exc.value)


def test_stop_web_sidecar_terminates_running_child() -> None:
    proc = FakeProc()

    cli._stop_web_sidecar(proc)

    assert proc.terminated is True
    assert proc.killed is False
    assert proc.wait_calls == 1


def test_stop_web_sidecar_kills_child_that_ignores_term() -> None:
    class StubbornProc(FakeProc):
        def wait(self, timeout: float | None = None) -> int:
            self.wait_calls += 1
            if self.wait_calls == 1:
                raise subprocess.TimeoutExpired(["vq"], timeout or 0)
            self.rc = -9
            return -9

    proc = StubbornProc()

    cli._stop_web_sidecar(proc)

    assert proc.terminated is True
    assert proc.killed is True
    assert proc.wait_calls == 2


def test_ensure_web_sidecar_keeps_running_child() -> None:
    proc = FakeProc()

    returned = cli._ensure_web_sidecar_running(
        proc,
        host="127.0.0.1",
        port=8768,
        log_level="info",
        acknowledge_public_bind=False,
    )

    assert returned is proc


def test_ensure_web_sidecar_restarts_exited_child(monkeypatch) -> None:
    old_proc = FakeProc(rc=1)
    new_proc = FakeProc()
    calls: list[dict[str, object]] = []

    def fake_start(**kwargs: object) -> FakeProc:
        calls.append(kwargs)
        return new_proc

    monkeypatch.setattr(cli, "_start_web_sidecar", fake_start)

    returned = cli._ensure_web_sidecar_running(
        old_proc,
        host="127.0.0.1",
        port=8768,
        log_level="warning",
        acknowledge_public_bind=False,
    )

    assert returned is new_proc
    assert calls == [
        {
            "host": "127.0.0.1",
            "port": 8768,
            "log_level": "warning",
            "acknowledge_public_bind": False,
        }
    ]
