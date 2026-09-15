"""v0.6.28: cgroup scope helpers target the right systemd manager.

Multi-user job scopes are SYSTEM scopes (``systemd-run --scope
--uid``, no ``--user``) created by the root daemon; single-user job
scopes live in the per-user manager (``systemd-run --user --scope``).
The scope-management helpers — scope_exists / stop_scope /
scope_main_pid — must run ``systemctl`` against the matching manager:
``systemctl --user`` cannot see or stop a system scope.

These tests pin that each helper omits ``--user`` when
``multi_user=True`` and keeps it (unchanged single-user behaviour)
when ``multi_user=False``.
"""
from __future__ import annotations

import subprocess

import pytest

from vq import cgroup


def _capturing_run(
    captured: list[list[str]], *, returncode: int = 0, stdout: str = ""
):
    """A fake subprocess.run that records each argv it is handed."""

    def _run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess:
        captured.append(list(argv))
        return subprocess.CompletedProcess(
            argv, returncode, stdout=stdout, stderr=""
        )

    return _run


# ----------------------------------------------------------------------
# _systemctl_scope_argv
# ----------------------------------------------------------------------


class TestSystemctlScopeArgv:
    def test_single_user_keeps_user_flag(self) -> None:
        assert cgroup._systemctl_scope_argv("/b/systemctl", False) == [
            "/b/systemctl", "--user",
        ]

    def test_multi_user_omits_user_flag(self) -> None:
        assert cgroup._systemctl_scope_argv("/b/systemctl", True) == [
            "/b/systemctl",
        ]


# ----------------------------------------------------------------------
# scope_exists
# ----------------------------------------------------------------------


class TestScopeExistsManager:
    def test_single_user_queries_user_manager(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/b/systemctl")
        cap: list[list[str]] = []
        monkeypatch.setattr(
            cgroup.subprocess, "run", _capturing_run(cap, stdout="loaded")
        )
        cgroup.scope_exists("vq-job-abc.scope")
        assert "--user" in cap[0]

    def test_multi_user_queries_system_manager(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/b/systemctl")
        cap: list[list[str]] = []
        monkeypatch.setattr(
            cgroup.subprocess, "run", _capturing_run(cap, stdout="loaded")
        )
        cgroup.scope_exists("vq-job-abc.scope", multi_user=True)
        assert "--user" not in cap[0]
        assert cap[0][0] == "/b/systemctl"
        assert "show" in cap[0] and "vq-job-abc.scope" in cap[0]

    def test_multi_user_still_returns_true_for_loaded(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The mode change must not alter the True/False/None verdict
        logic — only which manager is queried."""
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/b/systemctl")
        monkeypatch.setattr(
            cgroup.subprocess, "run",
            _capturing_run([], stdout="loaded"),
        )
        assert cgroup.scope_exists("vq-job-abc", multi_user=True) is True


# ----------------------------------------------------------------------
# stop_scope
# ----------------------------------------------------------------------


class TestStopScopeManager:
    def test_single_user_stops_in_user_manager(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/b/systemctl")
        cap: list[list[str]] = []
        monkeypatch.setattr(cgroup.subprocess, "run", _capturing_run(cap))
        cgroup.stop_scope("vq-job-abc.scope")
        assert "--user" in cap[0] and "stop" in cap[0]

    def test_multi_user_stops_in_system_manager(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/b/systemctl")
        cap: list[list[str]] = []
        monkeypatch.setattr(cgroup.subprocess, "run", _capturing_run(cap))
        ok = cgroup.stop_scope("vq-job-abc.scope", multi_user=True)
        assert "--user" not in cap[0]
        assert "stop" in cap[0] and "vq-job-abc.scope" in cap[0]
        assert ok is True  # rc=0


# ----------------------------------------------------------------------
# scope_main_pid
# ----------------------------------------------------------------------


class TestScopeMainPidManager:
    def test_single_user_queries_user_manager(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/b/systemctl")
        cap: list[list[str]] = []
        monkeypatch.setattr(
            cgroup.subprocess, "run", _capturing_run(cap, stdout="4242")
        )
        assert cgroup.scope_main_pid("vq-job-abc.scope") == 4242
        assert "--user" in cap[0]

    def test_multi_user_queries_system_manager(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/b/systemctl")
        cap: list[list[str]] = []
        monkeypatch.setattr(
            cgroup.subprocess, "run", _capturing_run(cap, stdout="4242")
        )
        assert cgroup.scope_main_pid("vq-job-abc.scope", multi_user=True) == 4242
        assert "--user" not in cap[0]
        assert "MainPID" in cap[0]
