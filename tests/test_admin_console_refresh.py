"""Console refresh after verified managed queue updates; no live services."""
from __future__ import annotations

import json
import subprocess

import pytest

from vq import admin, config
from vq.web import install


@pytest.fixture
def refresh(tmp_path, monkeypatch):
    old = config.VenvProgram(kind="venv", python=str(tmp_path / "old/bin/python"),
                             git_dir=str(tmp_path / "old"), branch="main")
    new = old.model_copy(update={"python": str(tmp_path / "new/bin/python")})
    marker = {"python": old.python, "manager": "systemd-user", "unit_name": "custom-web"}
    result = admin.UpdateResult(env="queue", git_dir=old.git_dir, branch="main",
                                update_script=None, git_pull_rc=0,
                                daemon_restart_attempted=True,
                                daemon_restart_succeeded=True,
                                daemon_health_verified=True)
    status = {"installed": True, "active": True, "drifted": False, "runtime_ok": True,
              "manager": "systemd-user", "unit_name": "custom-web"}
    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, json.dumps(status), "")

    monkeypatch.setattr(install, "read_install_marker", lambda: marker)
    monkeypatch.setattr(admin.subprocess, "run", run)
    monkeypatch.setattr(admin, "_run_log_command_output", lambda *args: None)
    return old, new, marker, result, status, calls


@pytest.mark.parametrize("manager", ["systemd-user", "launchd-user"])
@pytest.mark.parametrize("immutable", [True, False])
def test_refresh_uses_target_interpreter_and_preserves_service(refresh, manager, immutable):
    old, new, marker, result, status, calls = refresh
    if not immutable:
        new = old
    marker["manager"] = status["manager"] = manager
    admin._maybe_refresh_console(old, new, result)
    assert result.success
    assert calls == [
        [new.python, "-m", "vq", "web", "install", "--manager", manager,
         "--unit-name", "custom-web"],
        [new.python, "-m", "vq", "web", "status", "--json"],
    ]


@pytest.mark.parametrize("field,value", [
    ("git_pull_rc", 1), ("daemon_restart_succeeded", False),
    ("daemon_restart_succeeded", None), ("daemon_health_verified", False),
])
def test_failed_or_non_queue_update_does_not_refresh(refresh, field, value):
    old, new, marker, result, status, calls = refresh
    setattr(result, field, value)
    admin._maybe_refresh_console(old, new, result)
    assert not calls


def test_no_console_does_not_install_one(refresh, monkeypatch):
    old, new, marker, result, status, calls = refresh
    monkeypatch.setattr(install, "read_install_marker", lambda: None)
    admin._maybe_refresh_console(old, new, result)
    assert not calls
    assert result.success


def test_other_venv_with_same_python_binary_is_not_restarted(refresh, tmp_path):
    old, new, marker, result, status, calls = refresh
    binary = tmp_path / "system-python"
    binary.touch()
    for root in ("old", "other"):
        path = tmp_path / root / "bin/python"
        path.parent.mkdir(parents=True)
        path.symlink_to(binary)
    marker["python"] = str(tmp_path / "other/bin/python")
    admin._maybe_refresh_console(old, new, result)
    assert not calls
    assert result.success


@pytest.mark.parametrize("field,value", [
    ("active", False), ("active", None), ("drifted", True),
    ("runtime_ok", False), ("unit_name", "wrong-service"),
])
def test_bad_readback_fails_update_without_invalidating_daemon(refresh, field, value):
    old, new, marker, result, status, calls = refresh
    status[field] = value
    admin._maybe_refresh_console(old, new, result)
    assert not result.success
    assert result.daemon_health_verified
    assert "console refresh failed" in result.work_errors[0]


@pytest.mark.parametrize("failure", ["exit", "timeout", "invalid-json"])
def test_refresh_command_failure_is_visible(refresh, monkeypatch, failure):
    old, new, marker, result, status, calls = refresh

    def run(argv, **kwargs):
        if failure == "timeout":
            raise subprocess.TimeoutExpired(argv, 120)
        return subprocess.CompletedProcess(argv, 1 if failure == "exit" else 0,
                                           "invalid", "installer refused")

    monkeypatch.setattr(admin.subprocess, "run", run)
    admin._maybe_refresh_console(old, new, result)
    assert not result.success
    assert result.daemon_restart_succeeded
    assert "vq web install" in result.work_errors[0]


def test_system_console_requires_explicit_service_user(refresh):
    old, new, marker, result, status, calls = refresh
    marker["manager"] = "systemd-system"
    admin._maybe_refresh_console(old, new, result)
    assert not result.success
    assert not calls
