"""v0.6.37: `vq throttle` works in multi-user mode.

A multi-user job runs in a root-owned **system** scope, not a
``systemd-run --user`` scope. Before this fix `vq throttle` was
doubly broken on a multi-user host:

* `cgroup.set_cpu_weight` issued ``systemctl --user set-property``
  (wrong manager for a system scope) and was gated behind
  ``available()`` (which probes ``--user`` delegation) — so it
  silently no-op'd.
* `throttle_job` read the spec from the single-user
  ``paths.queue_dir()`` — in multi-user mode the spec lives under
  ``/var/lib/vq/users/<uid>/queue/``, so it failed with
  "no such job" before reaching the cgroup call.

v0.6.37 (model: root-only, any job): `set_cpu_weight` takes a
``multi_user`` flag → system-manager argv, no ``available()``
gate; `throttle_job` / `throttle_all` resolve specs from the
per-user dirs and require root.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from vq import cgroup, paths, throttle
from vq.spec import JobSpec, JobState
from vq.throttle import ThrottleError


# ---------------------------------------------------------------------
# cgroup.set_cpu_weight — manager selection + gate
# ---------------------------------------------------------------------
class TestSetCpuWeightManager:
    def _capture_argv(
        self, monkeypatch: pytest.MonkeyPatch, *, rc: int = 0
    ) -> list[list[str]]:
        calls: list[list[str]] = []

        def fake_run(argv, **kwargs):  # type: ignore[no-untyped-def]
            calls.append(argv)
            return subprocess.CompletedProcess(argv, rc, stdout="", stderr="")

        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/usr/bin/systemctl")
        monkeypatch.setattr(cgroup.subprocess, "run", fake_run)
        return calls

    def test_multi_user_uses_system_manager(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls = self._capture_argv(monkeypatch)
        # available() probes --user delegation; multi-user must NOT
        # consult it. Force it False to prove the gate is skipped.
        monkeypatch.setattr(cgroup, "available", lambda: False)
        ok = cgroup.set_cpu_weight("vq-job-x.scope", 40, multi_user=True)
        assert ok is True
        assert len(calls) == 1
        argv = calls[0]
        assert "--user" not in argv
        assert "set-property" in argv
        assert "CPUWeight=40" in argv

    def test_single_user_uses_user_manager(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls = self._capture_argv(monkeypatch)
        monkeypatch.setattr(cgroup, "available", lambda: True)
        ok = cgroup.set_cpu_weight("vq-job-x.scope", 40)
        assert ok is True
        assert "--user" in calls[0]

    def test_single_user_gated_by_available(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls = self._capture_argv(monkeypatch)
        monkeypatch.setattr(cgroup, "available", lambda: False)
        ok = cgroup.set_cpu_weight("vq-job-x.scope", 40)
        assert ok is False
        assert calls == []  # never reached subprocess


# ---------------------------------------------------------------------
# throttle._apply_throttle — multi-user path
# ---------------------------------------------------------------------
class TestApplyThrottleMultiUser:
    def test_multi_user_calls_set_cpu_weight_system(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        seen: list[tuple[str, int, bool]] = []

        def spy(scope, weight, *, multi_user=False):  # type: ignore[no-untyped-def]
            seen.append((scope, weight, multi_user))
            return True

        monkeypatch.setattr(cgroup, "set_cpu_weight", spy)
        path, detail = throttle._apply_throttle(
            "vq-job-x.scope", None, 30, multi_user=True
        )
        assert path == "cgroup"
        assert seen == [("vq-job-x.scope", 30, True)]

    def test_multi_user_failure_raises_no_renice_fallback(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            cgroup, "set_cpu_weight",
            lambda *a, **k: False,
        )
        # pgid is None — in single-user this would try renice; in
        # multi-user it must raise instead (no renice on a systemd
        # host running root-owned scopes).
        with pytest.raises(ThrottleError):
            throttle._apply_throttle("vq-job-x.scope", None, 30, multi_user=True)


# ---------------------------------------------------------------------
# throttle_job / throttle_all — root requirement + per-user resolution
# ---------------------------------------------------------------------
class TestThrottleRequiresRoot:
    def test_throttle_job_non_root_rejected(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(os, "geteuid", lambda: 1000)
        with pytest.raises(ThrottleError, match="root"):
            throttle.throttle_job("localhost", "job-1", 40, multi_user=True)

    def test_throttle_all_non_root_rejected(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(os, "geteuid", lambda: 1000)
        with pytest.raises(ThrottleError, match="root"):
            throttle.throttle_all("localhost", 40, multi_user=True)

    def test_single_user_throttle_not_root_gated(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Single-user throttle must NOT acquire a root requirement.
        monkeypatch.setattr(os, "geteuid", lambda: 1000)
        # Empty queue dir → clean "0 jobs" return, no ThrottleError.
        qd = tmp_path / "queue"
        qd.mkdir()
        msg = throttle.throttle_all("localhost", 40, queue_dir=qd)
        assert "0 job" in msg


class TestThrottleJobMultiUserResolvesPerUserSpec:
    def test_throttle_job_resolves_and_drives_system_scope(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "mu"))
        uid = os.getuid()
        # Provision the user's state dir + a RUNNING spec.
        paths.provision_user_state(uid, os.getgid())
        monkeypatch.setattr(os, "geteuid", lambda: 0)
        ws = paths.user_workspace_dir(uid, "job-1")
        ws.mkdir(parents=True, exist_ok=True)
        JobSpec(
            id="job-1",
            command=["true"],
            cwd=str(ws),
            cpus=1,
            state=JobState.RUNNING,
            pgid=4242,
            submitter=str(uid),
        ).write(paths.user_spec_path(uid, "job-1"))

        seen: list[tuple[str, int, bool]] = []

        def spy(scope, weight, *, multi_user=False):  # type: ignore[no-untyped-def]
            seen.append((scope, weight, multi_user))
            return True

        monkeypatch.setattr(cgroup, "set_cpu_weight", spy)
        msg = throttle.throttle_job("localhost", "job-1", 25, multi_user=True)
        assert "job-1" in msg
        # Resolved the per-user spec and drove its system scope.
        assert seen == [("vq-job-job-1.scope", 25, True)]

    def test_throttle_job_unknown_job_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "mu"))
        monkeypatch.setattr(os, "geteuid", lambda: 0)
        with pytest.raises(FileNotFoundError):
            throttle.throttle_job("localhost", "nope", 25, multi_user=True)


# ---------------------------------------------------------------------
# apply_persistent_throttle_if_set — daemon-side multi-user path
# ---------------------------------------------------------------------
class TestPersistentThrottleMultiUser:
    def test_persistent_apply_uses_system_scope(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        # A persistent throttle is active.
        throttle.write_throttle_state(throttle.ThrottleState(weight=15))

        seen: list[tuple[str, int, bool]] = []

        def spy(scope, weight, *, multi_user=False):  # type: ignore[no-untyped-def]
            seen.append((scope, weight, multi_user))
            return True

        monkeypatch.setattr(cgroup, "set_cpu_weight", spy)
        applied = throttle.apply_persistent_throttle_if_set(
            "job-1", pgid=4242, multi_user=True,
        )
        assert applied == 15
        assert seen == [("vq-job-job-1.scope", 15, True)]
