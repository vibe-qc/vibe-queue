"""Tests for the daemon main loop: dispatch, reconciliation, capacity, orphans."""
from __future__ import annotations

import contextlib
import json
import math
import os
import signal
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace

import pytest

from vq import config, resource_receipt
from vq.cli import _program_runtime_pin_for_submit
from vq.daemon import (
    EXIT_MARKER_RELPATH,
    Daemon,
    _build_wrapped_command,
    _OrphanJob,
    _pid_fingerprint_matches,
    _read_exit_marker,
    _read_pid_start_time,
    _read_vq_version_from_source,
)
from vq.spec import JobSpec, JobState, ProgramRuntimePin, utcnow_iso

_THREAD_CAP_ENV_VARS = (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "BLIS_NUM_THREADS",
)


class _FakePopen:
    pid = 999_999_999

    def kill(self) -> None:
        pass

    def wait(self, timeout: float | None = None) -> int:
        return 0


class _CapturingDispatcher:
    def __init__(self) -> None:
        self.env: dict[str, str] | None = None

    def launch(self, *, run_command, cwd, env, stdout_fh, stderr_fh):
        self.env = dict(env)
        return _FakePopen()


def _init_git_repo(path: Path) -> str:
    path.mkdir()
    subprocess.run(["git", "init"], cwd=path, check=True, capture_output=True)
    (path / "README.md").write_text("test repo\n", encoding="utf-8")
    subprocess.run(["git", "add", "README.md"], cwd=path, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=vq tests",
            "-c",
            "user.email=vq-tests@example.invalid",
            "commit",
            "-m",
            "seed",
        ],
        cwd=path,
        check=True,
        capture_output=True,
    )
    return subprocess.run(
        ["git", "rev-parse", "--short=12", "HEAD"],
        cwd=path,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _commit_git_file(path: Path, name: str) -> str:
    (path / name).write_text("changed\n", encoding="utf-8")
    subprocess.run(["git", "add", name], cwd=path, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=vq tests",
            "-c",
            "user.email=vq-tests@example.invalid",
            "commit",
            "-m",
            f"add {name}",
        ],
        cwd=path,
        check=True,
        capture_output=True,
    )
    return subprocess.run(
        ["git", "rev-parse", "--short=12", "HEAD"],
        cwd=path,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


@pytest.fixture
def daemon(tmp_path: Path) -> Iterator[Daemon]:
    d = Daemon(
        max_cpus=4,
        poll_interval=0.05,
        queue_dir=tmp_path / "queue",
        jobs_dir=tmp_path / "jobs",
    )
    d.queue_dir.mkdir(parents=True, exist_ok=True)
    d.jobs_dir.mkdir(parents=True, exist_ok=True)
    yield d
    for rj in d._running.values():
        try:
            rj.popen.kill()
            rj.popen.wait(timeout=1)
        except Exception:
            pass
        rj.close_logs()


def _submit(daemon: Daemon, jobid: str, command: list[str], cpus: int = 1) -> JobSpec:
    workspace = daemon.jobs_dir / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(id=jobid, command=command, cwd=str(workspace), cpus=cpus)
    spec.write(daemon._spec_path(jobid))
    return spec


def _wait_for_state(
    daemon: Daemon, jobid: str, state: JobState, timeout: float = 5.0
) -> JobSpec:
    deadline = time.monotonic() + timeout
    last: JobSpec | None = None
    while time.monotonic() < deadline:
        last = JobSpec.read(daemon._spec_path(jobid))
        if last.state == state:
            return last
        daemon.iterate()
        time.sleep(0.02)
    raise AssertionError(
        f"job {jobid} did not reach {state} in {timeout}s; "
        f"last state={last.state if last else 'unknown'}"
    )


def test_direct_process_count_uses_cgroup_peak_minus_collector(
    tmp_path: Path,
) -> None:
    (tmp_path / "pids.peak").write_text("7\n", encoding="utf-8")

    count, source, semantics = resource_receipt._peak_command_process_count(
        (tmp_path, 2)
    )

    assert count == 5
    assert source == "cgroup-v2-pids.peak-minus-collector-baseline"
    assert "threads count as tasks" in semantics


def test_scoped_resource_collector_refuses_payload_outside_named_scope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A promised job scope is enforcement, not optional telemetry.

    The host_d root-daemon incident placed payloads in the daemon service's
    256 MiB cgroup instead of the requested per-job scope.  The collector is
    the last trusted process before the user command, so it must fail closed
    when its own cgroup does not match the named ``vq-job-*`` scope.
    """
    receipt_path = tmp_path / resource_receipt.RESOURCE_USAGE_BASENAME
    marker_path = tmp_path / "exit-code"
    command_started = False

    monkeypatch.setattr(
        resource_receipt.cgroup,
        "cgroup_path_for_pid",
        lambda _pid: (
            "/sys/fs/cgroup/system.slice/"
            "vq-daemon-multi-user.service"
        ),
    )

    def fake_exec_and_wait(
        _command: list[str],
    ) -> tuple[int, float, SimpleNamespace]:
        nonlocal command_started
        command_started = True
        return 0, 0.01, SimpleNamespace(ru_utime=0.0, ru_stime=0.0, ru_maxrss=1)

    monkeypatch.setattr(resource_receipt, "_exec_and_wait", fake_exec_and_wait)

    rc = resource_receipt.main(
        [
            "--receipt",
            str(receipt_path),
            "--exit-marker",
            str(marker_path),
            "--cgroup-scope-name",
            "vq-job-host_d-regression",
            "--",
            "payload-must-not-run",
        ]
    )

    assert rc == resource_receipt.COLLECTOR_FAILURE_EXIT_CODE
    assert command_started is False
    assert marker_path.read_text(encoding="utf-8") == "125\n"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["status"] == "error"
    assert receipt["error"] == "cgroup_scope_mismatch"
    assert receipt["command_status"] == "not_run"


def test_scoped_resource_collector_runs_payload_inside_named_scope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt_path = tmp_path / resource_receipt.RESOURCE_USAGE_BASENAME
    marker_path = tmp_path / "exit-code"
    command_started = False

    monkeypatch.setattr(
        resource_receipt.cgroup,
        "cgroup_path_for_pid",
        lambda _pid: (
            "/sys/fs/cgroup/system.slice/"
            "vq-job-host_d-regression.scope"
        ),
    )

    def fake_exec_and_wait(
        _command: list[str],
    ) -> tuple[int, float, SimpleNamespace]:
        nonlocal command_started
        command_started = True
        return 0, 0.01, SimpleNamespace(ru_utime=0.0, ru_stime=0.0, ru_maxrss=1)

    monkeypatch.setattr(resource_receipt, "_exec_and_wait", fake_exec_and_wait)

    rc = resource_receipt.main(
        [
            "--receipt",
            str(receipt_path),
            "--exit-marker",
            str(marker_path),
            "--cgroup-scope-name",
            "vq-job-host_d-regression",
            "--",
            "payload-can-run",
        ]
    )

    assert rc == 0
    assert command_started is True
    assert marker_path.read_text(encoding="utf-8") == "0\n"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["status"] == "ok"
    assert receipt["command_status"] == "succeeded"


class TestDispatch:
    def test_pending_job_runs_to_completion(self, daemon: Daemon) -> None:
        _submit(daemon, "j1", ["true"])
        spec = _wait_for_state(daemon, "j1", JobState.COMPLETED)
        assert spec.exit_code == 0
        assert spec.pid is not None
        assert spec.started_at is not None
        assert spec.finished_at is not None

    def test_failing_job_marked_failed_with_exit_code(self, daemon: Daemon) -> None:
        _submit(daemon, "j1", ["false"])
        spec = _wait_for_state(daemon, "j1", JobState.FAILED)
        assert spec.exit_code == 1

    def test_sub_interval_job_writes_terminal_resource_receipt(
        self, daemon: Daemon
    ) -> None:
        daemon.watchdog.interval_seconds = float("inf")
        _submit(daemon, "quickreceipt", ["true"])

        spec = _wait_for_state(daemon, "quickreceipt", JobState.COMPLETED)
        workspace = Path(spec.cwd)
        assert not (workspace / "_vq" / "samples.jsonl").exists()

        receipt = json.loads(
            (workspace / "_vq" / resource_receipt.RESOURCE_USAGE_BASENAME).read_text()
        )
        assert receipt["schema"] == resource_receipt.RESOURCE_USAGE_SCHEMA
        assert receipt["status"] == "ok"
        assert receipt["collector"] == "posix-wait4"
        assert receipt["scope"] == "effective-command"
        assert receipt["command_status"] == "succeeded"
        assert receipt["command_exit_code"] == 0
        assert 0.0 <= receipt["wall_seconds"] < 5.0
        assert receipt["active_cpu_seconds"] >= 0.0
        assert receipt["peak_rss_kb"] > 0
        assert "process_count" in receipt
        assert receipt["metric_sources"]["wall_seconds"] == "clock-monotonic"
        assert receipt["metric_sources"]["cpu_seconds"] == "posix-wait4-rusage"
        assert receipt["metric_sources"]["peak_rss"] == "posix-wait4-ru_maxrss"
        assert "not a sum" in receipt["aggregation_semantics"]["peak_rss"]
        if receipt["process_count"] is None:
            assert receipt["metric_sources"]["process_count"] is None
            assert "unavailable" in receipt["aggregation_semantics"]["process_count"]
        else:
            assert receipt["process_count"] >= 1
            assert receipt["metric_sources"]["process_count"].startswith("cgroup-v2")
            assert "threads count as tasks" in (
                receipt["aggregation_semantics"]["process_count"]
            )

    def test_direct_resource_receipt_preserves_failed_outcome(
        self, daemon: Daemon
    ) -> None:
        _submit(daemon, "failedreceipt", ["sh", "-c", "exit 17"])

        spec = _wait_for_state(daemon, "failedreceipt", JobState.FAILED)
        workspace = Path(spec.cwd)
        receipt = json.loads(
            (workspace / "_vq" / resource_receipt.RESOURCE_USAGE_BASENAME).read_text()
        )

        assert spec.exit_code == 17
        assert (workspace / EXIT_MARKER_RELPATH).read_text() == "17\n"
        assert receipt["status"] == "ok"
        assert receipt["command_status"] == "failed"
        assert receipt["command_exit_code"] == 17

    def test_kill_during_dispatch_window_is_not_clobbered(self, daemon: Daemon) -> None:
        """STATE-1: a `vq kill` that lands after _dispatch_pending's
        pre-dispatch re-read but before _start_job writes RUNNING must be
        preserved, and the job must NOT be launched. We simulate the race by
        writing KILLED to disk and then dispatching the stale PENDING spec
        object _dispatch_pending would have captured before the kill.
        """
        pending = _submit(daemon, "race1", ["sleep", "30"])
        assert pending.state == JobState.PENDING

        # `vq kill` lands on disk during the dispatch-setup window.
        killed = JobSpec.read(daemon._spec_path("race1"))
        killed.state = JobState.KILLED
        killed.finished_at = utcnow_iso()
        killed.write(daemon._spec_path("race1"))

        started = daemon._start_job(pending)

        assert started is False, "must not launch a job that was killed mid-dispatch"
        assert "race1" not in daemon._running, "no process should be tracked"
        on_disk = JobSpec.read(daemon._spec_path("race1"))
        assert on_disk.state == JobState.KILLED, "KILLED must not be clobbered to RUNNING"
        assert on_disk.pid is None, "the job must never have been spawned"

    def test_command_not_found_marked_failed(self, daemon: Daemon) -> None:
        _submit(daemon, "j1", ["/nonexistent/binary/xyz"])
        spec = _wait_for_state(daemon, "j1", JobState.FAILED)
        # exit_code is -1 when Popen itself raises FileNotFoundError
        # (no cgroup wrap) and a non-zero exit (typically 1, sometimes
        # 127) when systemd-run wraps and the inner exec fails. Both
        # represent the same user-visible failure -- the command did not
        # run -- so we just require a non-success terminal state.
        assert spec.state == JobState.FAILED
        assert spec.exit_code is not None
        assert spec.exit_code != 0

    def test_local_dispatch_caps_scientific_thread_env_to_cpus(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Local jobs inherit thread-pool caps from their declared CPU budget."""
        for name in _THREAD_CAP_ENV_VARS:
            monkeypatch.delenv(name, raising=False)
        dispatcher = _CapturingDispatcher()
        daemon.dispatcher = dispatcher
        spec = _submit(daemon, "threads", ["true"], cpus=4)

        assert daemon._start_job(spec) is True

        assert dispatcher.env is not None
        for name in _THREAD_CAP_ENV_VARS:
            expected = "1" if name == "OPENBLAS_NUM_THREADS" else "4"
            assert dispatcher.env[name] == expected

    def test_local_dispatch_preserves_explicit_thread_env(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Operator-provided thread caps win over the queue default."""
        for name in _THREAD_CAP_ENV_VARS:
            monkeypatch.delenv(name, raising=False)
        monkeypatch.setenv("OMP_NUM_THREADS", "2")
        dispatcher = _CapturingDispatcher()
        daemon.dispatcher = dispatcher
        spec = _submit(daemon, "threads", ["true"], cpus=4)

        assert daemon._start_job(spec) is True

        assert dispatcher.env is not None
        assert dispatcher.env["OMP_NUM_THREADS"] == "2"
        assert dispatcher.env["OPENBLAS_NUM_THREADS"] == "1"

    def test_local_dispatch_preserves_explicit_openblas_threads(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Operators can still override the conservative OpenBLAS default."""
        for name in _THREAD_CAP_ENV_VARS:
            monkeypatch.delenv(name, raising=False)
        monkeypatch.setenv("OPENBLAS_NUM_THREADS", "2")
        dispatcher = _CapturingDispatcher()
        daemon.dispatcher = dispatcher
        spec = _submit(daemon, "threads", ["true"], cpus=4)

        assert daemon._start_job(spec) is True

        assert dispatcher.env is not None
        assert dispatcher.env["OMP_NUM_THREADS"] == "4"
        assert dispatcher.env["OPENBLAS_NUM_THREADS"] == "2"

    def test_local_dispatch_exports_vq_resource_env(self, daemon: Daemon) -> None:
        """Wrappers can size themselves from VQ_* resource metadata."""
        dispatcher = _CapturingDispatcher()
        daemon.dispatcher = dispatcher
        spec = _submit(daemon, "resources", ["true"], cpus=4)
        spec.mem_mb = 16_000
        spec.wall_time_seconds = 7200
        spec.write(daemon._spec_path(spec.id))

        assert daemon._start_job(spec) is True

        assert dispatcher.env is not None
        assert dispatcher.env["VQ_JOB_ID"] == "resources"
        assert dispatcher.env["VQ_CPUS"] == "4"
        assert dispatcher.env["VQ_MEM_MB"] == "16000"
        assert dispatcher.env["VQ_WALL_TIME_SECONDS"] == "7200"

    def test_local_dispatch_exports_program_env(self, daemon: Daemon) -> None:
        """Program identity reaches the local job environment unchanged."""
        dispatcher = _CapturingDispatcher()
        daemon.dispatcher = dispatcher
        spec = _submit(daemon, "programjob", ["true"], cpus=1)
        spec.program = "orca"
        spec.write(daemon._spec_path(spec.id))

        assert daemon._start_job(spec) is True

        assert dispatcher.env is not None
        assert dispatcher.env["VQ_PROGRAM"] == "orca"

    def test_local_dispatch_reread_preserves_program_metadata(
        self,
        daemon: Daemon,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The RUNNING claim must not write a stale in-memory spec."""
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir(exist_ok=True)
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
        git_dir = tmp_path / "repo"
        python = git_dir / ".venv" / "bin" / "python"
        (cfg_dir / "config.toml").write_text(
            "[programs.vibeqc-dev]\n"
            'kind = "venv"\n'
            f'python = "{python}"\n'
            f'git_dir = "{git_dir}"\n'
        )
        dispatcher = _CapturingDispatcher()
        daemon.dispatcher = dispatcher
        spec = _submit(daemon, "program-reread", ["true"], cpus=1)
        spec.program = "vibeqc-dev"
        spec.program_runtime_pin = ProgramRuntimePin()
        spec.write(daemon._spec_path(spec.id))
        stale = spec.model_copy(deep=True)
        stale.program = None
        stale.program_runtime_pin = None
        stale.workdir = None

        assert daemon._start_job(stale) is True

        assert dispatcher.env is not None
        assert dispatcher.env["VQ_PROGRAM"] == "vibeqc-dev"
        on_disk = JobSpec.read(daemon._spec_path(spec.id))
        assert on_disk.program == "vibeqc-dev"
        assert on_disk.program_runtime_pin is not None
        assert on_disk.workdir is not None

    def test_local_dispatch_exports_venv_program_paths(
        self,
        daemon: Daemon,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Directory payloads can call managed tools without host path guesses."""
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir(exist_ok=True)
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
        git_dir = tmp_path / "repo"
        submitted_sha = _init_git_repo(git_dir)
        python = git_dir / ".venv-vibeview" / "bin" / "python"
        (cfg_dir / "config.toml").write_text(
            "[programs.vibeview-dev]\n"
            'kind = "venv"\n'
            f'python = "{python}"\n'
            f'git_dir = "{git_dir}"\n'
            'branch = "main"\n'
        )
        dispatcher = _CapturingDispatcher()
        daemon.dispatcher = dispatcher
        spec = _submit(daemon, "vibeview", ["true"], cpus=1)
        spec.program = "vibeview-dev"
        spec.program_runtime_pin = ProgramRuntimePin(
            expected_git_sha=submitted_sha,
            enforce_git_sha=False,
        )
        spec.write(daemon._spec_path(spec.id))

        assert daemon._start_job(spec) is True

        assert dispatcher.env is not None
        assert dispatcher.env["VQ_PROGRAM"] == "vibeview-dev"
        assert dispatcher.env["VQ_PROGRAM_BIN"] == str(python.parent)
        assert dispatcher.env["VQ_PROGRAM_PYTHON"] == str(python)
        assert dispatcher.env["VQ_PROGRAM_GIT_DIR"] == str(git_dir)
        assert dispatcher.env["VQ_PROGRAM_BRANCH"] == "main"
        on_disk = JobSpec.read(daemon._spec_path(spec.id))
        assert on_disk.program_runtime_pin is not None
        resolved_sha = on_disk.program_runtime_pin.resolved_git_sha
        assert resolved_sha is not None
        assert resolved_sha.startswith(submitted_sha)
        assert dispatcher.env["VQ_PROGRAM_GIT_SHA"] == resolved_sha

    def test_unreadable_observational_sha_fails_before_launch(
        self,
        daemon: Daemon,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir(exist_ok=True)
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
        git_dir = tmp_path / "repo"
        python = git_dir / ".venv" / "bin" / "python"
        (cfg_dir / "config.toml").write_text(
            "[programs.vibeqc-dev]\n"
            'kind = "venv"\n'
            f'python = "{python}"\n'
            f'git_dir = "{git_dir}"\n'
        )
        monkeypatch.setattr(
            config.VenvProgram,
            "current_git_sha",
            lambda self, *, full=False: None,
        )
        dispatcher = _CapturingDispatcher()
        daemon.dispatcher = dispatcher
        spec = _submit(daemon, "unattributed", ["true"], cpus=1)
        spec.program = "vibeqc-dev"
        spec.program_runtime_pin = ProgramRuntimePin(
            expected_git_sha="a" * 40,
            enforce_git_sha=False,
        )
        spec.write(daemon._spec_path(spec.id))

        assert daemon._start_job(spec) is False

        on_disk = JobSpec.read(daemon._spec_path(spec.id))
        assert on_disk.state == JobState.FAILED
        assert on_disk.failure_reason is not None
        assert "resolved git SHA could not be read" in on_disk.failure_reason
        assert dispatcher.env is None

    def test_local_dispatch_exports_binary_program_path(
        self,
        daemon: Daemon,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A binary-kind program's validated executable reaches the payload
        as VQ_PROGRAM_EXE, so wrappers can exec it without host-conditional
        path logic (GitLab #126)."""
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir(exist_ok=True)
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
        orca = tmp_path / "bin" / "orca"
        orca.parent.mkdir(parents=True, exist_ok=True)
        orca.write_text("#!/bin/sh\nexit 0\n")
        orca.chmod(0o755)
        (cfg_dir / "config.toml").write_text(
            "[programs.orca]\n"
            'kind = "binary"\n'
            f'binary = "{orca}"\n'
        )
        dispatcher = _CapturingDispatcher()
        daemon.dispatcher = dispatcher
        spec = _submit(daemon, "binary-program", ["true"], cpus=1)
        spec.program = "orca"
        spec.write(daemon._spec_path(spec.id))

        assert daemon._start_job(spec) is True

        assert dispatcher.env is not None
        assert dispatcher.env["VQ_PROGRAM"] == "orca"
        assert dispatcher.env["VQ_PROGRAM_EXE"] == str(orca)

    def test_program_runtime_pin_rechecked_before_dispatch(
        self,
        daemon: Daemon,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A queued --program job must not launch after its venv pin drifts."""
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir(exist_ok=True)
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
        git_dir = tmp_path / "repo"
        submitted_sha = _init_git_repo(git_dir)
        drifted_sha = _commit_git_file(git_dir, "DRIFT.txt")
        (cfg_dir / "config.toml").write_text(
            "[programs.vibeqc-dev]\n"
            'kind = "venv"\n'
            f'python = "{sys.executable}"\n'
            f'git_dir = "{git_dir}"\n'
            f'expected_git_sha = "{submitted_sha}"\n'
        )
        dispatcher = _CapturingDispatcher()
        daemon.dispatcher = dispatcher
        spec = _submit(daemon, "pinjob", ["true"], cpus=1)
        spec.program = "vibeqc-dev"
        spec.write(daemon._spec_path(spec.id))

        assert daemon._start_job(spec) is False

        on_disk = JobSpec.read(daemon._spec_path(spec.id))
        assert on_disk.state == JobState.FAILED
        assert on_disk.exit_code == -1
        assert on_disk.failure_reason is not None
        assert "runtime pin mismatch before dispatch" in on_disk.failure_reason
        assert submitted_sha in on_disk.failure_reason
        assert drifted_sha in on_disk.failure_reason
        assert dispatcher.env is None

    def test_program_runtime_pin_snapshot_survives_config_update(
        self,
        daemon: Daemon,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Dispatch uses the submitted pin, not the config's new pin."""
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir(exist_ok=True)
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
        git_dir = tmp_path / "repo"
        submitted_sha = _init_git_repo(git_dir)
        (cfg_dir / "config.toml").write_text(
            "[programs.vibeqc-dev]\n"
            'kind = "venv"\n'
            f'python = "{sys.executable}"\n'
            f'git_dir = "{git_dir}"\n'
            f'expected_git_sha = "{submitted_sha}"\n'
        )
        drifted_sha = _commit_git_file(git_dir, "DRIFT.txt")
        (cfg_dir / "config.toml").write_text(
            "[programs.vibeqc-dev]\n"
            'kind = "venv"\n'
            f'python = "{sys.executable}"\n'
            f'git_dir = "{git_dir}"\n'
            f'expected_git_sha = "{drifted_sha}"\n'
        )
        dispatcher = _CapturingDispatcher()
        daemon.dispatcher = dispatcher
        spec = _submit(daemon, "pinjob2", ["true"], cpus=1)
        spec.program = "vibeqc-dev"
        spec.program_runtime_pin = ProgramRuntimePin(
            expected_git_sha=submitted_sha
        )
        spec.write(daemon._spec_path(spec.id))

        assert daemon._start_job(spec) is False

        on_disk = JobSpec.read(daemon._spec_path(spec.id))
        assert on_disk.state == JobState.FAILED
        assert on_disk.failure_reason is not None
        assert "runtime pin mismatch before dispatch" in on_disk.failure_reason
        assert submitted_sha in on_disk.failure_reason
        assert drifted_sha in on_disk.failure_reason
        assert dispatcher.env is None


class TestCrashFeedback:
    """v0.12.0: a non-COMPLETED terminal captures the stderr tail into
    spec.failure_tail so `vq status` shows the crash cause inline."""

    def test_failed_job_captures_stderr_tail(self, daemon: Daemon) -> None:
        _submit(
            daemon,
            "boom",
            ["bash", "-c", "echo 'ValueError: basis set xyz not found' >&2; exit 1"],
        )
        spec = _wait_for_state(daemon, "boom", JobState.FAILED)
        assert spec.exit_code == 1
        assert spec.failure_tail is not None
        assert "ValueError: basis set xyz not found" in spec.failure_tail

    def test_completed_job_has_no_tail(self, daemon: Daemon) -> None:
        # stderr noise on a SUCCESS must not be mistaken for a crash cause.
        _submit(daemon, "ok", ["bash", "-c", "echo noise >&2; exit 0"])
        spec = _wait_for_state(daemon, "ok", JobState.COMPLETED)
        assert spec.failure_tail is None


class TestCapacity:
    def test_oversized_job_skipped_so_smaller_one_runs(self, tmp_path: Path) -> None:
        d = Daemon(
            max_cpus=2, poll_interval=0.05, queue_dir=tmp_path / "q", jobs_dir=tmp_path / "j"
        )
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)
        _submit(d, "big", ["true"], cpus=4)
        time.sleep(0.01)
        _submit(d, "small", ["true"], cpus=1)
        _wait_for_state(d, "small", JobState.COMPLETED)
        assert JobSpec.read(d._spec_path("big")).state == JobState.PENDING

    def test_capacity_cap_limits_concurrent_runs(self, tmp_path: Path) -> None:
        d = Daemon(
            max_cpus=2, poll_interval=0.05, queue_dir=tmp_path / "q", jobs_dir=tmp_path / "j"
        )
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)
        try:
            for i in range(3):
                _submit(d, f"j{i}", ["sleep", "0.5"])
                time.sleep(0.01)
            d.iterate()
            states = [JobSpec.read(d._spec_path(f"j{i}")).state for i in range(3)]
            assert states.count(JobState.RUNNING) == 2
            assert states.count(JobState.PENDING) == 1
            for i in range(3):
                _wait_for_state(d, f"j{i}", JobState.COMPLETED, timeout=10.0)
        finally:
            for rj in d._running.values():
                rj.popen.kill()
                rj.popen.wait(timeout=1)
                rj.close_logs()


class TestMaxJobs:
    """The max_jobs cap is independent of max_cpus -- it limits the number
    of concurrent processes. This is the "one-job-at-a-time but each job
    may use the whole box" knob."""

    def test_max_jobs_one_serializes_dispatch_regardless_of_cpu_budget(
        self, tmp_path: Path
    ) -> None:
        # Plenty of CPU budget; jobs each declare 1 cpu. Without max_jobs
        # they'd all run concurrently. With max_jobs=1 only one runs.
        d = Daemon(
            max_cpus=32,
            max_jobs=1,
            poll_interval=0.05,
            queue_dir=tmp_path / "q",
            jobs_dir=tmp_path / "j",
        )
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)
        try:
            for i in range(3):
                _submit(d, f"j{i}", ["sleep", "0.4"], cpus=1)
                time.sleep(0.01)
            d.iterate()
            states = [JobSpec.read(d._spec_path(f"j{i}")).state for i in range(3)]
            assert states.count(JobState.RUNNING) == 1
            assert states.count(JobState.PENDING) == 2
            for i in range(3):
                _wait_for_state(d, f"j{i}", JobState.COMPLETED, timeout=10.0)
        finally:
            for rj in d._running.values():
                rj.popen.kill()
                rj.popen.wait(timeout=1)
                rj.close_logs()

    def test_max_jobs_one_dispatches_oversized_cpu_jobs(
        self, tmp_path: Path
    ) -> None:
        # max_cpus is the headroom-for-one-job, max_jobs=1 makes serial.
        # A job declaring cpus=32 must be dispatched -- it claims the
        # whole box, but max_jobs=1 means it runs alone. This is the
        # exact case max_cpus=1 broke (8-cpu jobs would never dispatch).
        d = Daemon(
            max_cpus=32,
            max_jobs=1,
            poll_interval=0.05,
            queue_dir=tmp_path / "q",
            jobs_dir=tmp_path / "j",
        )
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)
        _submit(d, "big-cpu", ["true"], cpus=32)
        _wait_for_state(d, "big-cpu", JobState.COMPLETED)

    def test_max_jobs_default_none_preserves_cpu_only_behavior(
        self, tmp_path: Path
    ) -> None:
        # No max_jobs means "no cap on job count, only cpu budget matters."
        # Two cpus=1 jobs in a max_cpus=4 daemon both run.
        d = Daemon(
            max_cpus=4,
            poll_interval=0.05,
            queue_dir=tmp_path / "q",
            jobs_dir=tmp_path / "j",
        )
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)
        try:
            _submit(d, "a", ["sleep", "0.5"], cpus=1)
            _submit(d, "b", ["sleep", "0.5"], cpus=1)
            d.iterate()
            running = [
                JobSpec.read(d._spec_path(j)).state == JobState.RUNNING
                for j in ("a", "b")
            ]
            assert all(running)
            for j in ("a", "b"):
                _wait_for_state(d, j, JobState.COMPLETED, timeout=10.0)
        finally:
            for rj in d._running.values():
                rj.popen.kill()
                rj.popen.wait(timeout=1)
                rj.close_logs()

    def test_max_jobs_two_caps_at_two(self, tmp_path: Path) -> None:
        d = Daemon(
            max_cpus=32,
            max_jobs=2,
            poll_interval=0.05,
            queue_dir=tmp_path / "q",
            jobs_dir=tmp_path / "j",
        )
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)
        try:
            for i in range(4):
                _submit(d, f"j{i}", ["sleep", "0.5"], cpus=1)
                time.sleep(0.01)
            d.iterate()
            states = [JobSpec.read(d._spec_path(f"j{i}")).state for i in range(4)]
            assert states.count(JobState.RUNNING) == 2
            assert states.count(JobState.PENDING) == 2
            for i in range(4):
                _wait_for_state(d, f"j{i}", JobState.COMPLETED, timeout=10.0)
        finally:
            for rj in d._running.values():
                rj.popen.kill()
                rj.popen.wait(timeout=1)
                rj.close_logs()

    def test_max_jobs_zero_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="max_jobs must be >= 1"):
            Daemon(max_jobs=0, queue_dir=tmp_path / "q", jobs_dir=tmp_path / "j")


class TestMemoryBudget:
    """v0.3 introduces a memory-budget gate alongside CPU + max_jobs.
    Both `--max-mem-mb` (daemon) and `mem_mb` (per-job spec) must be set
    for the gate to fire; undeclared jobs pass through (v0.3 leniency
    that v0.4 will tighten)."""

    def _submit_with_mem(
        self, daemon: Daemon, jobid: str, command: list[str], mem_mb: int | None = None
    ) -> JobSpec:
        workspace = daemon.jobs_dir / jobid
        workspace.mkdir(parents=True, exist_ok=True)
        spec = JobSpec(
            id=jobid, command=command, cwd=str(workspace), cpus=1, mem_mb=mem_mb
        )
        spec.write(daemon._spec_path(jobid))
        return spec

    def test_memory_budget_blocks_oversize_job(self, tmp_path: Path) -> None:
        # max 16 GB. First job uses 12 GB, second wants 8 GB -> wait.
        d = Daemon(
            max_cpus=32,
            max_mem_mb=16_000,
            poll_interval=0.05,
            queue_dir=tmp_path / "q",
            jobs_dir=tmp_path / "j",
        )
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)
        try:
            self._submit_with_mem(d, "big", ["sleep", "0.5"], mem_mb=12_000)
            time.sleep(0.01)
            self._submit_with_mem(d, "second", ["sleep", "0.5"], mem_mb=8_000)
            d.iterate()
            assert JobSpec.read(d._spec_path("big")).state == JobState.RUNNING
            assert JobSpec.read(d._spec_path("second")).state == JobState.PENDING
            for j in ("big", "second"):
                _wait_for_state(d, j, JobState.COMPLETED, timeout=10.0)
        finally:
            for rj in d._running.values():
                rj.popen.kill()
                rj.popen.wait(timeout=1)
                rj.close_logs()

    def test_undeclared_job_bypasses_dispatch_memory_gate(
        self, tmp_path: Path
    ) -> None:
        # If the spec doesn't declare mem_mb, the daemon's dispatch-time
        # memory gate doesn't apply (v0.3 leniency, retained in v0.4+).
        # We have to use a max_mem_mb that's also large enough that the
        # watchdog's host-percent ceiling (90% of host_total_mem_mb,
        # which defaults to max_mem_mb) doesn't immediately trigger on
        # the few MB of process startup RSS. 1 GB is comfortable.
        from vq.watchdog import Watchdog
        d = Daemon(
            max_cpus=32,
            max_mem_mb=1000,
            poll_interval=0.05,
            # Disable the watchdog's host-percent kill for this test so
            # we're isolating just the dispatch-gate behaviour. (Real
            # daemons keep it on; this fixture just doesn't care about it.)
            watchdog=Watchdog(host_total_mem_mb=None),
            queue_dir=tmp_path / "q",
            jobs_dir=tmp_path / "j",
        )
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)
        self._submit_with_mem(d, "no-decl", ["true"], mem_mb=None)
        _wait_for_state(d, "no-decl", JobState.COMPLETED)

    def test_pgid_recorded_on_dispatch(self, daemon: Daemon) -> None:
        # The pgid field gets populated at dispatch time. With
        # start_new_session=True, pgid == pid for the leader.
        _submit(daemon, "pgid-check", ["true"])
        spec = _wait_for_state(daemon, "pgid-check", JobState.COMPLETED)
        assert spec.pgid is not None
        assert spec.pgid == spec.pid

    def test_max_mem_mb_zero_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="max_mem_mb must be >= 1"):
            Daemon(
                max_mem_mb=0,
                queue_dir=tmp_path / "q",
                jobs_dir=tmp_path / "j",
            )


class TestWatchdogIntegration:
    """End-to-end: submit a job with wall_time_seconds = 1, run a long-
    sleeping subprocess, verify the watchdog terminates it and the spec
    lands in TIME_EXCEEDED. Exercises the full daemon -> watchdog ->
    SIGTERM -> reaper -> spec-write chain."""

    def test_watchdog_pass_supplies_cgroup_scope_name(
        self, tmp_path: Path
    ) -> None:
        from vq.watchdog import Verdict, WatchdogAction

        class CapturingWatchdog:
            kwargs: dict[str, object] | None = None

            def evaluate(self, **kwargs):  # type: ignore[no-untyped-def]
                self.kwargs = dict(kwargs)
                return Verdict(action=WatchdogAction.OK)

        workspace = tmp_path / "j" / "scope-name"
        workspace.mkdir(parents=True)
        d = Daemon(
            max_cpus=4,
            poll_interval=0.05,
            watchdog=CapturingWatchdog(),  # type: ignore[arg-type]
            queue_dir=tmp_path / "q",
            jobs_dir=tmp_path / "j",
        )
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)
        spec = JobSpec(
            id="scope-name",
            command=["true"],
            cwd=str(workspace),
            cpus=1,
            state=JobState.RUNNING,
            pid=123,
            pgid=456,
        )
        spec.write(d._spec_path("scope-name"))
        d._running["scope-name"] = SimpleNamespace(
            popen=SimpleNamespace(pid=123)
        )  # type: ignore[assignment]

        d._watchdog_pass()

        capture = d.watchdog.kwargs  # type: ignore[attr-defined]
        assert capture is not None
        assert capture["cgroup_unit_name"] == "vq-job-scope-name"
        assert capture["cgroup_multi_user"] is False

    def test_wall_time_terminates_runaway_job(self, tmp_path: Path) -> None:
        from vq.watchdog import Watchdog

        # interval_seconds=0 = sample on every iterate(); grace_seconds=0.5
        # so SIGKILL follows fast if SIGTERM is ignored. We use `sleep`
        # (which honours SIGTERM) so SIGKILL shouldn't be needed.
        d = Daemon(
            max_cpus=4,
            poll_interval=0.05,
            watchdog=Watchdog(interval_seconds=0.0, grace_seconds=0.5),
            queue_dir=tmp_path / "q",
            jobs_dir=tmp_path / "j",
        )
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)

        workspace = d.jobs_dir / "long"
        workspace.mkdir(parents=True)
        spec = JobSpec(
            id="long",
            command=["sleep", "30"],
            cwd=str(workspace),
            cpus=1,
            wall_time_seconds=1,
        )
        spec.write(d._spec_path("long"))

        try:
            # Drive the daemon until the spec hits TIME_EXCEEDED. Should
            # take ~1 second of wall time for the watchdog to fire +
            # ~0.5s grace, but we give plenty of headroom.
            deadline = time.monotonic() + 10.0
            while time.monotonic() < deadline:
                d.iterate()
                cur = JobSpec.read(d._spec_path("long"))
                if cur.state == JobState.TIME_EXCEEDED:
                    break
                time.sleep(0.05)

            assert (
                JobSpec.read(d._spec_path("long")).state == JobState.TIME_EXCEEDED
            )

            # Drive a bit more so the reaper picks up the dead child.
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline and "long" in d._running:
                d.iterate()
                time.sleep(0.05)
            assert "long" not in d._running
            final = JobSpec.read(d._spec_path("long"))
            # Watchdog terminal state must be preserved through reaping.
            assert final.state == JobState.TIME_EXCEEDED
            # Exit code populated (the SIGTERM/SIGKILL ended the sleep).
            assert final.exit_code is not None
        finally:
            for rj in d._running.values():
                rj.popen.kill()
                rj.popen.wait(timeout=2)
                rj.close_logs()

    @pytest.mark.skipif(
        not hasattr(signal, "SIGSTOP") or not hasattr(signal, "SIGCONT"),
        reason="requires POSIX job-control signals",
    )
    def test_wall_time_wakes_and_reaps_stopped_build_job(
        self, tmp_path: Path
    ) -> None:
        from vq.watchdog import Watchdog

        d = Daemon(
            max_cpus=4,
            poll_interval=0.05,
            watchdog=Watchdog(interval_seconds=0.0, grace_seconds=0.3),
            queue_dir=tmp_path / "q",
            jobs_dir=tmp_path / "j",
        )
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)

        workspace = d.jobs_dir / "stopped-build"
        workspace.mkdir(parents=True)
        spec = JobSpec(
            id="stopped-build",
            command=[
                sys.executable,
                "-c",
                (
                    "import os, signal, time; "
                    "os.kill(os.getpid(), signal.SIGSTOP); "
                    "time.sleep(30)"
                ),
            ],
            cwd=str(workspace),
            cpus=1,
            build_env="vibeqc-dev",
            wall_time_seconds=1,
        )
        spec.write(d._spec_path("stopped-build"))

        try:
            deadline = time.monotonic() + 10.0
            while time.monotonic() < deadline:
                d.iterate()
                cur = JobSpec.read(d._spec_path("stopped-build"))
                if cur.state == JobState.TIME_EXCEEDED:
                    break
                time.sleep(0.05)

            assert (
                JobSpec.read(d._spec_path("stopped-build")).state
                == JobState.TIME_EXCEEDED
            )

            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline and "stopped-build" in d._running:
                d.iterate()
                time.sleep(0.05)
            assert "stopped-build" not in d._running
            final = JobSpec.read(d._spec_path("stopped-build"))
            assert final.state == JobState.TIME_EXCEEDED
            assert final.exit_code is not None
        finally:
            for rj in d._running.values():
                if getattr(rj, "popen", None) is not None:
                    with contextlib.suppress(Exception):
                        os.killpg(os.getpgid(rj.popen.pid), signal.SIGCONT)
                    try:
                        rj.popen.kill()
                        rj.popen.wait(timeout=2)
                    except Exception:
                        pass
                rj.close_logs()


class TestOrphanReset:
    """v0.4 reshape (refined in v0.4.1): at startup, RUNNING specs with a
    still-alive pgid stay RUNNING (re-attached as orphans); RUNNING specs
    with a dead/missing pgid become ABORTED_BY_QUEUE (was INTERRUPTED in
    v0.4 -- renamed for clarity to the submitter). Non-RUNNING specs are
    always untouched."""

    def test_running_with_no_pgid_marked_aborted_by_queue(self, daemon: Daemon) -> None:
        """Pre-v0.3 spec on disk: pgid is None, so we can't probe liveness.
        Conservative behavior is INTERRUPTED."""
        ws = daemon.jobs_dir / "orphan"
        ws.mkdir(parents=True)
        spec = JobSpec(
            id="orphan",
            command=["sleep", "1"],
            cwd=str(ws),
            cpus=1,
            state=JobState.RUNNING,
            pid=999999,  # pid set, pgid not -- old-spec scenario
            started_at="2026-01-01T00:00:00+00:00",
        )
        spec.write(daemon._spec_path("orphan"))
        daemon._reattach_or_interrupt_at_startup()
        recovered = JobSpec.read(daemon._spec_path("orphan"))
        assert recovered.state == JobState.ABORTED_BY_QUEUE
        assert recovered.finished_at is not None
        # Should NOT be tracked as an orphan (no pgid to track).
        assert "orphan" not in daemon._orphans

    def test_running_with_dead_pgid_marked_aborted_by_queue(self, daemon: Daemon) -> None:
        ws = daemon.jobs_dir / "orphan"
        ws.mkdir(parents=True)
        # Pick a pgid we're confident is gone. 2147483646 is near INT_MAX
        # and we use it as our "definitely-not-running" sentinel elsewhere.
        spec = JobSpec(
            id="orphan",
            command=["sleep", "1"],
            cwd=str(ws),
            cpus=1,
            state=JobState.RUNNING,
            pid=2_147_483_645,
            pgid=2_147_483_646,
            started_at="2026-01-01T00:00:00+00:00",
        )
        spec.write(daemon._spec_path("orphan"))
        daemon._reattach_or_interrupt_at_startup()
        recovered = JobSpec.read(daemon._spec_path("orphan"))
        assert recovered.state == JobState.ABORTED_BY_QUEUE
        assert "orphan" not in daemon._orphans

    def test_running_with_alive_pgid_stays_running_and_tracked(
        self, daemon: Daemon
    ) -> None:
        """Spawn a real subprocess, capture its pgid, write a RUNNING spec
        with that pgid, and check that startup re-attaches it instead of
        moving it to INTERRUPTED."""
        import subprocess
        proc = subprocess.Popen(
            ["sleep", "30"], start_new_session=True
        )
        try:
            pgid = os.getpgid(proc.pid)
            ws = daemon.jobs_dir / "alive-orphan"
            ws.mkdir(parents=True)
            spec = JobSpec(
                id="alive-orphan",
                command=["sleep", "30"],
                cwd=str(ws),
                cpus=1,
                state=JobState.RUNNING,
                pid=proc.pid,
                pgid=pgid,
                started_at="2026-01-01T00:00:00+00:00",
            )
            spec.write(daemon._spec_path("alive-orphan"))
            daemon._reattach_or_interrupt_at_startup()
            recovered = JobSpec.read(daemon._spec_path("alive-orphan"))
            assert recovered.state == JobState.RUNNING
            assert "alive-orphan" in daemon._orphans
            assert daemon._orphans["alive-orphan"].pgid == pgid
        finally:
            proc.kill()
            proc.wait(timeout=2)

    def test_orphan_exit_marked_aborted_by_queue_on_next_iterate(
        self, daemon: Daemon
    ) -> None:
        """Re-attach an orphan, then kill its process and tick iterate().
        The next reconcile pass should notice and mark INTERRUPTED."""
        import subprocess
        proc = subprocess.Popen(["sleep", "30"], start_new_session=True)
        try:
            pgid = os.getpgid(proc.pid)
            ws = daemon.jobs_dir / "doomed"
            ws.mkdir(parents=True)
            spec = JobSpec(
                id="doomed",
                command=["sleep", "30"],
                cwd=str(ws),
                cpus=1,
                state=JobState.RUNNING,
                pid=proc.pid,
                pgid=pgid,
                started_at="2026-01-01T00:00:00+00:00",
            )
            spec.write(daemon._spec_path("doomed"))
            daemon._reattach_or_interrupt_at_startup()
            assert "doomed" in daemon._orphans

            # Kill the process and wait for it to be reaped (init does this).
            proc.kill()
            proc.wait(timeout=2)

            # Give the kernel a moment to clean up the pgid mapping.
            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline:
                daemon._reconcile_orphans()
                if "doomed" not in daemon._orphans:
                    break
                time.sleep(0.05)

            assert "doomed" not in daemon._orphans
            assert (
                JobSpec.read(daemon._spec_path("doomed")).state
                == JobState.ABORTED_BY_QUEUE
            )
        finally:
            try:
                proc.kill()
                proc.wait(timeout=2)
            except Exception:
                pass

    def test_non_running_specs_untouched_at_startup(self, daemon: Daemon) -> None:
        # Cover every non-RUNNING terminal state (including the legacy
        # INTERRUPTED bucket, which old specs on disk may still carry).
        for state in (
            JobState.PENDING,
            JobState.COMPLETED,
            JobState.FAILED,
            JobState.KILLED,
            JobState.INTERRUPTED,
            JobState.ABORTED_BY_QUEUE,
        ):
            jid = f"j_{state.value}"
            ws = daemon.jobs_dir / jid
            ws.mkdir(parents=True)
            JobSpec(
                id=jid,
                command=["true"],
                cwd=str(ws),
                cpus=1,
                state=state,
            ).write(daemon._spec_path(jid))
        daemon._reattach_or_interrupt_at_startup()
        for state in (
            JobState.PENDING,
            JobState.COMPLETED,
            JobState.FAILED,
            JobState.KILLED,
            JobState.INTERRUPTED,
            JobState.ABORTED_BY_QUEUE,
        ):
            jid = f"j_{state.value}"
            assert JobSpec.read(daemon._spec_path(jid)).state == state


class TestRobustness:
    def test_corrupt_spec_skipped_not_crashing(self, daemon: Daemon) -> None:
        (daemon.queue_dir / "broken.json").write_text("not valid json")
        daemon.iterate()  # must not raise

    def test_iterate_with_no_pending_or_running_is_noop(self, daemon: Daemon) -> None:
        daemon.iterate()
        assert daemon._running == {}


class TestStdoutCapture:
    def test_stdout_and_stderr_written_to_workspace(self, daemon: Daemon) -> None:
        _submit(daemon, "j1", ["sh", "-c", "echo hello && echo err >&2"])
        spec = _wait_for_state(daemon, "j1", JobState.COMPLETED)
        workspace = Path(spec.cwd)
        assert (workspace / "stdout.log").read_text() == "hello\n"
        assert (workspace / "stderr.log").read_text() == "err\n"


class TestConstructorValidation:
    def test_max_cpus_must_be_positive(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError):
            Daemon(max_cpus=0, queue_dir=tmp_path / "q", jobs_dir=tmp_path / "j")

    def test_poll_interval_must_be_positive(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError):
            Daemon(poll_interval=0, queue_dir=tmp_path / "q", jobs_dir=tmp_path / "j")

    @pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
    def test_poll_interval_must_be_finite_before_queue_lock(
        self,
        tmp_path: Path,
        value: float,
    ) -> None:
        queue_dir = tmp_path / "q"

        with pytest.raises(ValueError, match="poll_interval must be finite"):
            Daemon(
                poll_interval=value,
                queue_dir=queue_dir,
                jobs_dir=tmp_path / "j",
            )

        assert not queue_dir.exists()


class TestDefensiveAgainstExternalMutations:
    def test_external_kill_during_run_does_not_overwrite_state(self, daemon: Daemon) -> None:
        """If `vq kill` writes KILLED while the job is running, the daemon
        must leave that state alone when it sees the process exit."""
        _submit(daemon, "j1", ["sleep", "30"])
        daemon.iterate()
        spec = JobSpec.read(daemon._spec_path("j1"))
        assert spec.state == JobState.RUNNING
        pid = spec.pid
        assert pid is not None

        # Simulate `vq kill`: mark KILLED externally and signal the process.
        spec.state = JobState.KILLED
        spec.finished_at = utcnow_iso()
        spec.write(daemon._spec_path("j1"))
        os.kill(pid, signal.SIGTERM)

        # Drive the daemon until it reaps the dead child.
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and "j1" in daemon._running:
            daemon.iterate()
            time.sleep(0.05)
        assert "j1" not in daemon._running

        spec = JobSpec.read(daemon._spec_path("j1"))
        assert spec.state == JobState.KILLED  # not overwritten to FAILED
        assert spec.exit_code is not None  # but the rc was recorded for visibility

    def test_external_kill_while_pending_skips_dispatch(self, daemon: Daemon) -> None:
        """If a pending job is killed before the daemon dispatches, the daemon
        must not start it."""
        _submit(daemon, "j1", ["sleep", "30"])
        spec = JobSpec.read(daemon._spec_path("j1"))
        spec.state = JobState.KILLED
        spec.finished_at = utcnow_iso()
        spec.write(daemon._spec_path("j1"))

        daemon.iterate()
        assert "j1" not in daemon._running
        assert JobSpec.read(daemon._spec_path("j1")).state == JobState.KILLED


class TestExitMarkerHelpers:
    """v0.5.9 unit-level coverage of the marker file primitives."""

    def test_read_marker_missing_returns_none(self, tmp_path: Path) -> None:
        assert _read_exit_marker(tmp_path) is None

    def test_read_marker_empty_returns_none(self, tmp_path: Path) -> None:
        marker = tmp_path / EXIT_MARKER_RELPATH
        marker.parent.mkdir(parents=True)
        marker.write_text("")
        assert _read_exit_marker(tmp_path) is None

    def test_read_marker_garbage_returns_none(self, tmp_path: Path) -> None:
        """Defensive against a corrupt marker -- must not crash the daemon."""
        marker = tmp_path / EXIT_MARKER_RELPATH
        marker.parent.mkdir(parents=True)
        marker.write_text("not a number")
        assert _read_exit_marker(tmp_path) is None

    def test_read_marker_zero_returns_zero(self, tmp_path: Path) -> None:
        marker = tmp_path / EXIT_MARKER_RELPATH
        marker.parent.mkdir(parents=True)
        marker.write_text("0\n")
        assert _read_exit_marker(tmp_path) == 0

    def test_read_marker_signal_convention(self, tmp_path: Path) -> None:
        """Bash records signaled exits as 128+sig (e.g. SIGTERM=143).
        We pass it through verbatim; consumer interprets."""
        marker = tmp_path / EXIT_MARKER_RELPATH
        marker.parent.mkdir(parents=True)
        marker.write_text("143")
        assert _read_exit_marker(tmp_path) == 143

    def test_build_wrapped_command_shape(self, tmp_path: Path) -> None:
        marker = tmp_path / "m"
        wrapped = _build_wrapped_command(["python", "-c", "pass"], marker)
        assert wrapped[:3] == [sys.executable, "-m", "vq.resource_receipt"]
        assert wrapped[3:7] == [
            "--receipt",
            str(tmp_path / resource_receipt.RESOURCE_USAGE_BASENAME),
            "--exit-marker",
            str(marker),
        ]
        assert wrapped[7:] == ["--", "python", "-c", "pass"]

    def test_wrapped_rc_zero_writes_marker(self, tmp_path: Path) -> None:
        """End-to-end: actually run the wrapped command and read the marker."""
        import subprocess
        marker = tmp_path / "m"
        wrapped = _build_wrapped_command(["true"], marker)
        rc = subprocess.run(wrapped).returncode
        assert rc == 0
        assert marker.read_text().strip() == "0"

    def test_wrapped_rc_nonzero_writes_marker(self, tmp_path: Path) -> None:
        import subprocess
        marker = tmp_path / "m"
        wrapped = _build_wrapped_command(["sh", "-c", "exit 7"], marker)
        rc = subprocess.run(wrapped).returncode
        assert rc == 7
        assert marker.read_text().strip() == "7"

    def test_wrapped_path_with_spaces_in_marker(self, tmp_path: Path) -> None:
        """shlex.quote in the wrap must handle spaces / quotes safely."""
        import subprocess
        weird = tmp_path / "with space" / "marker file"
        weird.parent.mkdir(parents=True)
        wrapped = _build_wrapped_command(["true"], weird)
        rc = subprocess.run(wrapped).returncode
        assert rc == 0
        assert weird.read_text().strip() == "0"

    def test_wrapped_signaled_inner_records_128_plus_sig(
        self, tmp_path: Path
    ) -> None:
        """Inner cmd dies on signal; bash records 128+sig."""
        import subprocess
        marker = tmp_path / "m"
        # Self-SIGTERM: shell sends SIGTERM to its own pid, exits 143.
        wrapped = _build_wrapped_command(
            ["sh", "-c", "kill -TERM $$"], marker
        )
        completed = subprocess.run(wrapped)
        # Preserve the established shell-style 128+signal convention.
        recorded = int(marker.read_text().strip())
        assert recorded > 128, f"expected 128+sig, got {recorded}"
        assert completed.returncode == recorded


class TestExitMarkerIntegration:
    """v0.5.9 end-to-end: marker is created in the workspace as expected."""

    def test_completed_job_leaves_marker_with_zero(
        self, daemon: Daemon
    ) -> None:
        _submit(daemon, "ok", ["true"])
        spec = _wait_for_state(daemon, "ok", JobState.COMPLETED)
        marker = Path(spec.cwd) / EXIT_MARKER_RELPATH
        assert marker.exists()
        assert marker.read_text().strip() == "0"

    def test_failed_job_leaves_marker_with_rc(self, daemon: Daemon) -> None:
        _submit(daemon, "bad", ["sh", "-c", "exit 3"])
        spec = _wait_for_state(daemon, "bad", JobState.FAILED)
        assert spec.exit_code == 3
        marker = Path(spec.cwd) / EXIT_MARKER_RELPATH
        assert marker.read_text().strip() == "3"

    def test_stale_marker_unlinked_at_dispatch(self, daemon: Daemon) -> None:
        """A pre-existing marker (e.g. from a partially-recovered crash
        on a re-used workspace) must not poison the next dispatch.
        _start_job unlinks before wrapping."""
        workspace = daemon.jobs_dir / "stale"
        workspace.mkdir(parents=True)
        stale = workspace / EXIT_MARKER_RELPATH
        stale.parent.mkdir(parents=True)
        stale.write_text("99")  # would mark FAILED if read
        spec = JobSpec(
            id="stale", command=["true"], cwd=str(workspace), cpus=1
        )
        spec.write(daemon._spec_path("stale"))
        result = _wait_for_state(daemon, "stale", JobState.COMPLETED)
        assert result.exit_code == 0
        assert stale.read_text().strip() == "0"

    def test_stdout_flushed_before_marker(self, daemon: Daemon) -> None:
        """Marker write happens AFTER the inner command exits, so stdout
        must already be on disk by the time the marker appears. Asserting
        ordering directly is racy; instead we check the by-product: when
        the spec is in COMPLETED, both files are present."""
        _submit(daemon, "io", ["sh", "-c", "echo done; exit 0"])
        spec = _wait_for_state(daemon, "io", JobState.COMPLETED)
        ws = Path(spec.cwd)
        assert (ws / "stdout.log").read_text() == "done\n"
        assert (ws / EXIT_MARKER_RELPATH).read_text().strip() == "0"


class TestOrphanRecoveryViaMarker:
    """v0.5.9's central regression test: an orphan that exits cleanly
    while the daemon is down must come back as COMPLETED / FAILED, not
    ABORTED_BY_QUEUE. Pre-v0.5.9 this was always ABORTED_BY_QUEUE
    because init reaped the rc."""

    def test_reconcile_orphan_with_marker_rc_zero_marks_completed(
        self, daemon: Daemon
    ) -> None:
        """The classic post-restart-resume-completes flow, simulated:
        register an orphan, write its exit marker, kill its pgid, run
        a reconcile pass. Must land in COMPLETED."""
        import subprocess
        proc = subprocess.Popen(["sleep", "30"], start_new_session=True)
        try:
            pgid = os.getpgid(proc.pid)
            ws = daemon.jobs_dir / "post-restart-ok"
            ws.mkdir(parents=True)
            spec = JobSpec(
                id="post-restart-ok",
                command=["sleep", "30"],
                cwd=str(ws),
                cpus=1,
                state=JobState.RUNNING,
                pid=proc.pid,
                pgid=pgid,
                started_at="2026-01-01T00:00:00+00:00",
            )
            spec.write(daemon._spec_path("post-restart-ok"))

            # The orphan-like setup: spec is RUNNING with a real pgid;
            # no Popen handle in daemon._running. Register it as an
            # orphan the way _reattach_or_interrupt_at_startup would.
            daemon._orphans["post-restart-ok"] = _OrphanJob(
                pgid=pgid, cpus=1, mem_mb=None
            )
            daemon.watchdog.register("post-restart-ok")

            # Job "completes" -- write the marker the command wrapper would
            # write, then kill the pgid (init equivalent).
            marker = ws / EXIT_MARKER_RELPATH
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text("0")
            proc.kill()
            proc.wait(timeout=2)

            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline:
                daemon._reconcile_orphans()
                if "post-restart-ok" not in daemon._orphans:
                    break
                time.sleep(0.05)

            recovered = JobSpec.read(daemon._spec_path("post-restart-ok"))
            assert recovered.state == JobState.COMPLETED, (
                f"expected COMPLETED via marker; got {recovered.state}"
            )
            assert recovered.exit_code == 0
            assert recovered.finished_at is not None
        finally:
            try:
                proc.kill()
                proc.wait(timeout=2)
            except Exception:
                pass

    def test_reconcile_orphan_with_marker_rc_nonzero_marks_failed(
        self, daemon: Daemon
    ) -> None:
        """Same as above but inner command errored (rc=1)."""
        import subprocess
        proc = subprocess.Popen(["sleep", "30"], start_new_session=True)
        try:
            pgid = os.getpgid(proc.pid)
            ws = daemon.jobs_dir / "post-restart-err"
            ws.mkdir(parents=True)
            spec = JobSpec(
                id="post-restart-err", command=["sleep", "30"],
                cwd=str(ws), cpus=1,
                state=JobState.RUNNING, pid=proc.pid, pgid=pgid,
                started_at="2026-01-01T00:00:00+00:00",
            )
            spec.write(daemon._spec_path("post-restart-err"))
            daemon._orphans["post-restart-err"] = _OrphanJob(
                pgid=pgid, cpus=1, mem_mb=None
            )
            daemon.watchdog.register("post-restart-err")

            marker = ws / EXIT_MARKER_RELPATH
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text("1")
            proc.kill()
            proc.wait(timeout=2)

            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline:
                daemon._reconcile_orphans()
                if "post-restart-err" not in daemon._orphans:
                    break
                time.sleep(0.05)

            recovered = JobSpec.read(daemon._spec_path("post-restart-err"))
            assert recovered.state == JobState.FAILED
            assert recovered.exit_code == 1
        finally:
            try:
                proc.kill()
                proc.wait(timeout=2)
            except Exception:
                pass

    def test_reconcile_orphan_no_marker_falls_back_to_aborted(
        self, daemon: Daemon
    ) -> None:
        """Without a marker (e.g. SIGKILL of the wrapper, host crash,
        pre-v0.5.9 spec on disk) the v0.4 ABORTED_BY_QUEUE path
        is still taken."""
        import subprocess
        proc = subprocess.Popen(["sleep", "30"], start_new_session=True)
        try:
            pgid = os.getpgid(proc.pid)
            ws = daemon.jobs_dir / "no-marker"
            ws.mkdir(parents=True)
            spec = JobSpec(
                id="no-marker", command=["sleep", "30"],
                cwd=str(ws), cpus=1,
                state=JobState.RUNNING, pid=proc.pid, pgid=pgid,
                started_at="2026-01-01T00:00:00+00:00",
            )
            spec.write(daemon._spec_path("no-marker"))
            daemon._orphans["no-marker"] = _OrphanJob(
                pgid=pgid, cpus=1, mem_mb=None
            )
            daemon.watchdog.register("no-marker")

            # Deliberately do NOT write a marker.
            proc.kill()
            proc.wait(timeout=2)

            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline:
                daemon._reconcile_orphans()
                if "no-marker" not in daemon._orphans:
                    break
                time.sleep(0.05)

            recovered = JobSpec.read(daemon._spec_path("no-marker"))
            assert recovered.state == JobState.ABORTED_BY_QUEUE
        finally:
            try:
                proc.kill()
                proc.wait(timeout=2)
            except Exception:
                pass

    def test_startup_reattach_with_marker_marks_completed(
        self, daemon: Daemon
    ) -> None:
        """The daemon-restart entry point (_reattach_or_interrupt_at_startup)
        must also consult the marker. A spec in RUNNING state with a
        gone pgid + a written marker should land in COMPLETED, not
        ABORTED_BY_QUEUE. This is the exact failure mode of the bug
        we're fixing in v0.5.9."""
        ws = daemon.jobs_dir / "startup-recovered"
        ws.mkdir(parents=True)
        marker = ws / EXIT_MARKER_RELPATH
        marker.parent.mkdir(parents=True)
        marker.write_text("0")
        spec = JobSpec(
            id="startup-recovered",
            command=["sleep", "1"],
            cwd=str(ws),
            cpus=1,
            state=JobState.RUNNING,
            pid=2_147_483_645,  # definitely-gone pid (per existing convention)
            pgid=2_147_483_646,
            started_at="2026-01-01T00:00:00+00:00",
        )
        spec.write(daemon._spec_path("startup-recovered"))

        daemon._reattach_or_interrupt_at_startup()

        recovered = JobSpec.read(daemon._spec_path("startup-recovered"))
        assert recovered.state == JobState.COMPLETED
        assert recovered.exit_code == 0
        assert "startup-recovered" not in daemon._orphans

    def test_orphan_with_terminal_state_keeps_state_records_marker_rc(
        self, daemon: Daemon
    ) -> None:
        """If the watchdog (or `vq kill`) already wrote a terminal
        state, the marker rc is forensic info only -- the state must
        not flip back to COMPLETED/FAILED."""
        import subprocess
        proc = subprocess.Popen(["sleep", "30"], start_new_session=True)
        try:
            pgid = os.getpgid(proc.pid)
            ws = daemon.jobs_dir / "killed-then-finished"
            ws.mkdir(parents=True)
            spec = JobSpec(
                id="killed-then-finished",
                command=["sleep", "30"],
                cwd=str(ws), cpus=1,
                # Already KILLED before we reconcile.
                state=JobState.KILLED, pid=proc.pid, pgid=pgid,
                started_at="2026-01-01T00:00:00+00:00",
                finished_at=utcnow_iso(),
            )
            spec.write(daemon._spec_path("killed-then-finished"))
            daemon._orphans["killed-then-finished"] = _OrphanJob(
                pgid=pgid, cpus=1, mem_mb=None
            )
            daemon.watchdog.register("killed-then-finished")

            (ws / EXIT_MARKER_RELPATH).parent.mkdir(parents=True, exist_ok=True)
            (ws / EXIT_MARKER_RELPATH).write_text("143")  # SIGTERM
            proc.kill()
            proc.wait(timeout=2)

            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline:
                daemon._reconcile_orphans()
                if "killed-then-finished" not in daemon._orphans:
                    break
                time.sleep(0.05)

            recovered = JobSpec.read(daemon._spec_path("killed-then-finished"))
            # KILLED must win; marker rc only provides forensic exit_code.
            assert recovered.state == JobState.KILLED
            assert recovered.exit_code == 143
        finally:
            try:
                proc.kill()
                proc.wait(timeout=2)
            except Exception:
                pass

    def test_pause_restart_resume_complete_real_subprocess(
        self, daemon: Daemon
    ) -> None:
        """End-to-end-ish reproduction of the reported bug: a job that
        survives a daemon restart (re-attached as orphan) and then
        completes normally must be COMPLETED, not ABORTED_BY_QUEUE.

        We don't actually fork a fresh daemon process -- the test
        emulates the lifecycle by:
          1. Dispatching a 1s shell script via _start_job (real wrap).
          2. Forgetting the Popen handle (simulating the old daemon
             dying mid-job): move the entry from _running to _orphans.
          3. Letting the subprocess exit on its own (writes marker
             via the command wrapper).
          4. Running _reconcile_orphans until the orphan is processed.

        This is the highest-fidelity unit-test reproduction of the
        smoke-test failure on host_d without spinning a real systemd
        unit; it exercises the wrap + marker + reconcile path in one
        flow."""
        # 1s shell script -- short enough to avoid slowing CI but long
        # enough that the test can perform the orphan handoff before
        # the subprocess exits.
        _submit(daemon, "lifecycle", ["sh", "-c", "sleep 1; exit 0"])
        # Drive one iterate to dispatch the job.
        daemon.iterate()
        assert "lifecycle" in daemon._running, "expected dispatch on first tick"

        rj = daemon._running["lifecycle"]
        pgid = JobSpec.read(daemon._spec_path("lifecycle")).pgid
        assert pgid is not None

        # Step 2: simulate daemon restart -- drop the Popen, register
        # the still-alive pgid as an orphan. (We close logs to avoid
        # leaking fds; the wrapper holds its own fds for stdout.log.)
        rj.close_logs()
        del daemon._running["lifecycle"]
        daemon._orphans["lifecycle"] = _OrphanJob(
            pgid=pgid, cpus=1, mem_mb=None
        )
        # Watchdog gets re-registered on restart, mirror that.
        daemon.watchdog.register("lifecycle")

        # Step 3+4: wait for the subprocess to finish and the marker
        # to land, then reconcile until classified.
        #
        # The ``rj.popen.poll()`` call inside the loop simulates the
        # zombie-reaping that init / systemd-user does in production.
        # In a real ``systemctl --user restart vq-daemon`` flow, the
        # OLD daemon process exits during restart and the orphaned
        # command wrapper is reparented to the user manager (PID 1 of the
        # user systemd), which reaps the zombie automatically. The
        # NEW daemon then sees ``_pgroup_alive(pgid)`` return False
        # and reads the exit-code marker. In this same-process
        # simulation we are still the wrap's parent, so without an
        # explicit poll() / wait() the exited wrap lingers as a
        # defunct zombie. ``killpg(pgid, 0)`` returns success on a
        # zombie process group (the pgid stays in the kernel's
        # process table until the leader is reaped), so the
        # reconciler would otherwise believe the orphan is alive
        # forever and the test would time out before classifying.
        # Repro confirmed on host_d 2026-05-10 (cgroup-enabled
        # Linux): ``ps -o pid,pgid,stat -g <pgid>`` showed
        # "Zs <defunct>" with ``_pgroup_alive(pgid)`` still True.
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            rj.popen.poll()
            daemon._reconcile_orphans()
            if "lifecycle" not in daemon._orphans:
                break
            time.sleep(0.05)

        final = JobSpec.read(daemon._spec_path("lifecycle"))
        marker = Path(final.cwd) / EXIT_MARKER_RELPATH
        marker_txt = marker.read_text() if marker.exists() else "absent"
        assert final.state == JobState.COMPLETED, (
            f"orphan should recover via marker; got {final.state}, "
            f"marker={marker_txt}"
        )
        assert final.exit_code == 0


class TestNotificationsOnTerminalTransition:
    """v0.5.35: every terminal-state code path in the daemon calls
    ``notify.send_terminal_notification`` so an operator with a
    webhook configured gets one POST per terminal event."""

    def test_completed_job_triggers_notification(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from vq import notify
        calls: list[tuple[str, str | None]] = []
        monkeypatch.setattr(
            notify, "send_terminal_notification",
            lambda spec, url, **k: calls.append((spec.state.value, url)),
        )
        daemon.notify_webhook_url = "https://hook.example.com/x"
        _submit(daemon, "j1", ["true"])
        _wait_for_state(daemon, "j1", JobState.COMPLETED)
        # exactly one notification for the COMPLETED transition
        terminal_calls = [c for c in calls if c[0] == "completed"]
        assert len(terminal_calls) == 1
        assert terminal_calls[0][1] == "https://hook.example.com/x"

    def test_failed_job_triggers_notification(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from vq import notify
        calls: list[str] = []
        monkeypatch.setattr(
            notify, "send_terminal_notification",
            lambda spec, url, **k: calls.append(spec.state.value),
        )
        daemon.notify_webhook_url = "https://hook.example.com/x"
        _submit(daemon, "j1", ["false"])
        _wait_for_state(daemon, "j1", JobState.FAILED)
        assert "failed" in calls

    def test_no_notification_when_webhook_url_unset(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """notify.send_terminal_notification IS called (the daemon
        always calls through; the no-op is inside notify itself)
        but it must be called with url=None so it doesn't POST.
        This protects the "absent config → zero network activity"
        guarantee."""
        from vq import notify
        urls_seen: list[str | None] = []
        monkeypatch.setattr(
            notify, "send_terminal_notification",
            lambda spec, url, **k: urls_seen.append(url),
        )
        # daemon fixture has notify_webhook_url=None by default
        assert daemon.notify_webhook_url is None
        _submit(daemon, "j1", ["true"])
        _wait_for_state(daemon, "j1", JobState.COMPLETED)
        assert all(u is None for u in urls_seen), (
            f"daemon leaked a webhook URL: {urls_seen}"
        )

    def test_aborted_by_queue_triggers_notification(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """_mark_aborted_by_queue is the third terminal sink (orphan
        with no exit info). Test it directly by calling the method —
        building a real orphan-lost scenario is involved and covered
        elsewhere."""
        from vq import notify
        calls: list[str] = []
        monkeypatch.setattr(
            notify, "send_terminal_notification",
            lambda spec, url, **k: calls.append(spec.state.value),
        )
        daemon.notify_webhook_url = "https://hook.example.com/x"
        spec = _submit(daemon, "lost", ["sleep", "10"])
        spec.state = JobState.RUNNING
        spec.write(daemon._spec_path("lost"))
        daemon._mark_aborted_by_queue(spec, reason="test orphan loss")
        assert calls == ["aborted_by_queue"]

    def test_no_notification_for_retry_re_enqueue(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """v0.5.31 retry: a job that exits non-zero with retry budget
        left goes back to PENDING — that's NOT terminal, must NOT
        notify. Otherwise users with --retry 3 get 4 spammy webhooks
        for one job."""
        from vq import notify
        calls: list[str] = []
        monkeypatch.setattr(
            notify, "send_terminal_notification",
            lambda spec, url, **k: calls.append(spec.state.value),
        )
        daemon.notify_webhook_url = "https://hook.example.com/x"
        spec = _submit(daemon, "retry1", ["false"])
        spec.retry_max = 2
        spec.write(daemon._spec_path("retry1"))

        # First failure: retry kicks in, no notification yet.
        _wait_for_state(daemon, "retry1", JobState.PENDING, timeout=8.0)
        # The retry put it back into PENDING; we should have seen
        # zero terminal-state notifications so far.
        assert calls == [], (
            f"retry-rescheduled job leaked a notification: {calls}"
        )

    def test_daemon_default_has_no_webhook(self, tmp_path: Path) -> None:
        """The Daemon() default keeps notifications off. A user has
        to explicitly thread the URL through to enable them.
        Belt-and-braces guard against an accidental "POST to None"
        regression in the constructor."""
        d = Daemon(
            max_cpus=1,
            queue_dir=tmp_path / "queue",
            jobs_dir=tmp_path / "jobs",
        )
        assert d.notify_webhook_url is None


# ----------------------------------------------------------------------
# v0.5.45: daemon honors the admin-update-in-progress marker
# ----------------------------------------------------------------------


class TestAdminUpdateMarkerEnforcement:
    """v0.5.45: when the admin-update-in-progress marker is on disk,
    the daemon's _dispatch_pending must skip new dispatches. Running
    jobs, reconcile passes, and the watchdog continue normally.
    State transitions (marker appears / clears) emit one log line
    each rather than per-tick spam."""

    @pytest.fixture
    def isolated_daemon(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> Iterator[Daemon]:
        """Daemon fixture that also redirects VQ_STATE_DIR into
        tmp_path so the marker writes / reads stay isolated from
        the developer's real state root."""
        from vq import paths

        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        d = Daemon(
            max_cpus=4,
            poll_interval=0.05,
            queue_dir=tmp_path / "queue",
            jobs_dir=tmp_path / "jobs",
        )
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)
        yield d
        for rj in d._running.values():
            try:
                # Kill the group, not just the wrapper: the pause-intent test
                # SIGSTOPs it, and a command vq.resource_receipt had already
                # forked would otherwise outlive the session, stopped. The
                # wrapper may already be dead while that command holds the
                # group, so this cannot be gated on the wrapper; while a member
                # exists the id is still this job's. OSError, not only ESRCH:
                # a group mid-teardown answers EPERM on macOS (#27, #29).
                with contextlib.suppress(OSError):
                    os.killpg(rj.popen.pid, signal.SIGKILL)
                rj.popen.wait(timeout=1)
            except Exception:
                pass
            rj.close_logs()

    def test_no_marker_dispatches_normally(
        self, isolated_daemon: Daemon
    ) -> None:
        _submit(isolated_daemon, "j1", ["true"])
        spec = _wait_for_state(
            isolated_daemon, "j1", JobState.COMPLETED, timeout=5.0,
        )
        assert spec.exit_code == 0

    def test_marker_present_blocks_new_dispatch(
        self, isolated_daemon: Daemon
    ) -> None:
        """A pending job submitted with the marker on disk stays
        pending across several iterations — _start_job is never
        called."""
        from vq import admin

        admin.write_admin_update_marker(
            envs=["vibeqc-dev"], host="host_d",
        )
        _submit(isolated_daemon, "blocked", ["true"])
        # Several iterate() calls; none should dispatch.
        for _ in range(5):
            isolated_daemon.iterate()
        spec = JobSpec.read(isolated_daemon._spec_path("blocked"))
        assert spec.state == JobState.PENDING
        assert spec.started_at is None

    def test_marker_appearing_during_setup_blocks_final_dispatch_claim(
        self,
        isolated_daemon: Daemon,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The final locked PENDING -> RUNNING claim re-attests the marker.

        Admin admission can race the setup work after the pending sweep's
        earlier marker check.  Whichever side wins must be visible: inject the
        marker at the late workdir step and prove no child is launched.
        """
        from vq import admin, paths

        spec = _submit(isolated_daemon, "late-marker", ["true"])
        original_workdir_for = paths.workdir_for

        def marker_during_setup(jobid: str) -> Path:
            admin.write_admin_update_marker(
                envs=["vibeqc-dev"], host="host_d",
            )
            return original_workdir_for(jobid)

        monkeypatch.setattr(paths, "workdir_for", marker_during_setup)

        assert isolated_daemon._start_job(spec) is False
        current = JobSpec.read(isolated_daemon._spec_path(spec.id))
        assert current.state == JobState.PENDING
        assert current.started_at is None
        assert spec.id not in isolated_daemon._running

    def test_iterate_reconciles_durable_pause_intent(
        self, isolated_daemon: Daemon
    ) -> None:
        """A crashed pauser cannot leave RUNNING-but-stopped durable state."""
        _submit(isolated_daemon, "pause-intent", ["sleep", "30"])
        isolated_daemon.iterate()
        spec_path = isolated_daemon._spec_path("pause-intent")
        running = JobSpec.read(spec_path)
        assert running.state == JobState.RUNNING
        assert running.pgid is not None

        running.pause_intent_at = utcnow_iso()
        running.pause_intent_monotonic_at = time.monotonic()
        running.pause_intent_pgid = running.pgid
        running.pause_intent_by = "admin-update-deadbeef1234"
        running.write(spec_path)

        isolated_daemon.iterate()

        reconciled = JobSpec.read(spec_path)
        assert reconciled.state == JobState.SUSPENDED
        assert reconciled.paused_by == "admin-update-deadbeef1234"
        assert reconciled.pause_intent_at is None
        assert reconciled.pause_intent_pgid is None
        assert reconciled.pause_intent_by is None

    def test_marker_clearing_resumes_dispatch(
        self, isolated_daemon: Daemon
    ) -> None:
        """The marker → no-marker transition unblocks pending jobs
        within one tick."""
        from vq import admin

        admin.write_admin_update_marker(
            envs=["vibeqc-dev"], host="host_d",
        )
        _submit(isolated_daemon, "blocked", ["true"])
        # Confirm it stays blocked.
        for _ in range(3):
            isolated_daemon.iterate()
        spec = JobSpec.read(isolated_daemon._spec_path("blocked"))
        assert spec.state == JobState.PENDING
        # Operator runs `vq admin clear-update-marker`.
        admin.clear_admin_update_marker()
        # Next tick dispatches.
        completed = _wait_for_state(
            isolated_daemon, "blocked", JobState.COMPLETED, timeout=5.0,
        )
        assert completed.exit_code == 0

    def test_running_jobs_continue_through_marker_appearance(
        self, isolated_daemon: Daemon
    ) -> None:
        """A job already running when the marker is written keeps
        running through to completion. Only NEW dispatches are
        gated — the venv mutation can't reach a process whose
        modules are already mapped."""
        from vq import admin

        _submit(isolated_daemon, "in-flight", ["sleep", "0.5"])
        isolated_daemon.iterate()
        assert "in-flight" in isolated_daemon._running
        # Marker appears mid-flight.
        admin.write_admin_update_marker(
            envs=["vibeqc-dev"], host="host_d",
        )
        spec = _wait_for_state(
            isolated_daemon, "in-flight", JobState.COMPLETED, timeout=5.0,
        )
        assert spec.exit_code == 0

    def test_state_transition_logs_once_per_change(
        self, isolated_daemon: Daemon, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """One log line when the marker appears, one when it
        clears. Repeated ticks while the marker stays present do
        NOT spam the log."""
        from vq import admin

        caplog.set_level("INFO", logger="vq.daemon")
        # Tick once with no marker → no log.
        isolated_daemon._dispatch_pending()
        # Marker appears.
        admin.write_admin_update_marker(
            envs=["vibeqc-dev"], host="host_d",
        )
        for _ in range(5):
            isolated_daemon._dispatch_pending()
        detect_records = [
            r for r in caplog.records
            if "marker detected" in r.getMessage()
        ]
        assert len(detect_records) == 1, (
            f"expected exactly one 'detected' log entry across "
            f"5 ticks, got {len(detect_records)}"
        )
        # Marker clears.
        caplog.clear()
        admin.clear_admin_update_marker()
        for _ in range(3):
            isolated_daemon._dispatch_pending()
        clear_records = [
            r for r in caplog.records
            if "marker cleared" in r.getMessage()
        ]
        assert len(clear_records) == 1, (
            f"expected exactly one 'cleared' log entry across "
            f"3 ticks, got {len(clear_records)}"
        )

    def test_malformed_marker_still_blocks_with_unreadable_log(
        self, isolated_daemon: Daemon, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A marker file present but unparseable still blocks new
        dispatches (cheap stat decides). Log message includes
        the 'unreadable' hint so the operator can diagnose."""
        from vq import admin

        caplog.set_level("INFO", logger="vq.daemon")
        path = admin.admin_update_marker_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("garbage")
        _submit(isolated_daemon, "blocked", ["true"])
        for _ in range(3):
            isolated_daemon.iterate()
        spec = JobSpec.read(isolated_daemon._spec_path("blocked"))
        assert spec.state == JobState.PENDING
        unreadable = [
            r for r in caplog.records
            if "unreadable" in r.getMessage()
        ]
        assert len(unreadable) >= 1


# ----------------------------------------------------------------------
# v0.5.50: PID-fingerprint anti-recycle check at startup recovery
# ----------------------------------------------------------------------

LINUX = sys.platform.startswith("linux")


class TestPidFingerprint:
    """v0.5.50 (audit § 2d): _read_pid_start_time captures
    /proc/<pid>/stat field 22; _pid_fingerprint_matches cross-checks
    it on startup recovery so a recycled PID doesn't get silently
    re-attached as if it were the original orphan."""

    @pytest.mark.skipif(not LINUX, reason="/proc only on Linux")
    def test_read_pid_start_time_for_self_returns_int(self) -> None:
        import os
        st = _read_pid_start_time(os.getpid())
        assert st is not None
        assert isinstance(st, int)
        assert st > 0

    def test_read_pid_start_time_returns_none_for_nonexistent_pid(
        self,
    ) -> None:
        # PID 1 always exists (init); but a high-but-likely-unused
        # PID returns None on /proc.
        # Use a deliberately out-of-range PID to force the open() to
        # fail. On macOS the open() always fails (no /proc), giving
        # the same None result — uniform behavior.
        st = _read_pid_start_time(2**31 - 1)
        assert st is None

    def test_fingerprint_matches_returns_none_when_pid_start_time_absent(
        self, tmp_path: Path,
    ) -> None:
        """Pre-v0.5.50 specs have no pid_start_time → cannot do
        anti-recycle check → return None so the caller falls back
        to pgid-only liveness."""
        spec = JobSpec(
            id="legacy12345",
            command=["true"],
            cwd=str(tmp_path),
            cpus=1,
            state=JobState.RUNNING,
            pid=os.getpid(),
            pgid=os.getpid(),
            # pid_start_time deliberately not set → defaults to None
        )
        assert _pid_fingerprint_matches(spec) is None

    @pytest.mark.skipif(not LINUX, reason="/proc only on Linux")
    def test_fingerprint_matches_returns_true_for_live_pid(
        self, tmp_path: Path,
    ) -> None:
        """If we record our own pid + its current start time and
        immediately re-check, the fingerprint matches."""
        import os
        my_pid = os.getpid()
        my_start = _read_pid_start_time(my_pid)
        assert my_start is not None
        spec = JobSpec(
            id="livejob123",
            command=["true"],
            cwd=str(tmp_path),
            cpus=1,
            state=JobState.RUNNING,
            pid=my_pid,
            pgid=my_pid,
            pid_start_time=my_start,
        )
        assert _pid_fingerprint_matches(spec) is True

    @pytest.mark.skipif(not LINUX, reason="/proc only on Linux")
    def test_fingerprint_matches_returns_false_for_recycled_pid(
        self, tmp_path: Path,
    ) -> None:
        """If we record our pid but a WRONG start time, the
        fingerprint comparison returns False (the canonical
        'PID was recycled' signal)."""
        import os
        my_pid = os.getpid()
        my_start = _read_pid_start_time(my_pid)
        assert my_start is not None
        spec = JobSpec(
            id="recycled123",
            command=["true"],
            cwd=str(tmp_path),
            cpus=1,
            state=JobState.RUNNING,
            pid=my_pid,
            pgid=my_pid,
            pid_start_time=my_start + 1000000,  # deliberately wrong
        )
        assert _pid_fingerprint_matches(spec) is False


class TestStartupRecoveryPidRecycleDetection:
    """v0.5.50: _reattach_or_interrupt_at_startup MUST detect a
    recycled-PID orphan and mark ABORTED_BY_QUEUE with reason
    'pid_recycled' instead of silently re-attaching."""

    @pytest.mark.skipif(not LINUX, reason="/proc only on Linux")
    def test_recycled_pid_lands_in_aborted_by_queue(
        self, tmp_path: Path,
    ) -> None:
        d = Daemon(
            max_cpus=4,
            poll_interval=0.05,
            queue_dir=tmp_path / "queue",
            jobs_dir=tmp_path / "jobs",
        )
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)

        # Write a RUNNING spec whose recorded pid is alive (use the
        # test's own PID) but whose recorded pid_start_time is wrong
        # — simulating the kernel-reused-pid scenario.
        my_pid = os.getpid()
        my_start = _read_pid_start_time(my_pid)
        assert my_start is not None
        workspace = d.jobs_dir / "recycled-001"
        workspace.mkdir()
        spec = JobSpec(
            id="recycled-001",
            command=["true"],
            cwd=str(workspace),
            cpus=1,
            state=JobState.RUNNING,
            pid=my_pid,
            pgid=my_pid,  # pgid liveness check would pass
            pid_start_time=my_start + 999999,  # but fingerprint won't
            started_at=utcnow_iso(),
        )
        spec.write(d._spec_path("recycled-001"))

        d._reattach_or_interrupt_at_startup()

        recovered = JobSpec.read(d._spec_path("recycled-001"))
        assert recovered.state == JobState.ABORTED_BY_QUEUE
        # Not in the orphans dict — we refused to re-attach.
        assert "recycled-001" not in d._orphans


# ----------------------------------------------------------------------
# v0.5.50: cgroup availability re-probe at daemon startup
# ----------------------------------------------------------------------


class TestCgroupReprobeAtStartup:
    """v0.5.50 (audit § 2j): the lru_cache on cgroup.available()
    is cleared at daemon construction so a restart re-tests
    availability instead of inheriting a stale True from the
    previous daemon's view."""

    def test_construction_calls_reset_availability_cache(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from vq import cgroup
        calls: list[str] = []
        orig = cgroup.reset_availability_cache

        def _spy() -> None:
            calls.append("reset")
            orig()

        monkeypatch.setattr(
            cgroup, "reset_availability_cache", _spy,
        )
        Daemon(
            max_cpus=4,
            poll_interval=0.05,
            queue_dir=tmp_path / "q",
            jobs_dir=tmp_path / "j",
        )
        assert calls == ["reset"], (
            "daemon constructor must invoke "
            "cgroup.reset_availability_cache() before calling "
            "cgroup.available()"
        )


# ----------------------------------------------------------------------
# v0.5.51: marker-PID liveness check at daemon startup tick
# ----------------------------------------------------------------------


class TestMarkerPidLivenessCheck:
    """v0.5.51 → v0.11.0: when the daemon's per-tick marker check finds a
    marker on disk it probes whether the recorded PID is still alive. A
    dead PID means the writing `vq admin update` is gone — the marker is
    a stale corpse. Pre-v0.11.0 the daemon merely logged and kept pausing
    dispatch forever (the host_e/host_b 2026-06-18 wedge). Now it REAPS the
    corpse (deletes it, warns loudly) and lets dispatch proceed; a marker
    whose pid is alive + fresh is a live update and still pauses."""

    @pytest.fixture
    def daemon_with_state_dir(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> Iterator[Daemon]:
        from vq import config, paths
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
        (tmp_path / "cfg").mkdir()
        paths.queue_dir().mkdir(parents=True, exist_ok=True)
        paths.jobs_dir().mkdir(parents=True, exist_ok=True)
        d = Daemon(
            max_cpus=4,
            poll_interval=0.05,
            queue_dir=tmp_path / "queue",
            jobs_dir=tmp_path / "jobs",
        )
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)
        yield d

    def test_dead_pid_marker_is_reaped_not_paused(
        self,
        daemon_with_state_dir: Daemon,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        from vq import admin
        # Write a marker whose recorded pid is guaranteed dead.
        admin.acquire_admin_update_marker(
            envs=["vibeqc-dev"], host="host_d",
        )
        marker = admin.read_admin_update_marker()
        assert marker is not None
        # Mutate the pid on disk to a definitely-dead value.
        import json as _json
        from dataclasses import asdict as _asdict
        path = admin.admin_update_marker_path()
        data = _asdict(marker)
        data["pid"] = 2**31 - 1  # max signed int — never a real PID
        path.write_text(_json.dumps(data))

        caplog.set_level("INFO")
        present = daemon_with_state_dir._poll_admin_update_marker()
        # v0.11.0: corpse → does NOT pause dispatch, and is reaped.
        assert present is False
        assert admin.admin_update_marker_exists() is False
        warnings = [
            r for r in caplog.records
            if r.levelname == "WARNING"
            and "STALE" in r.getMessage()
            and "reaped" in r.getMessage()
        ]
        assert len(warnings) == 1, (
            f"expected exactly one STALE-reaped warning, "
            f"got: {[r.getMessage() for r in caplog.records]}"
        )

    def test_alive_pid_marker_pauses_and_is_kept(
        self,
        daemon_with_state_dir: Daemon,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        from vq import admin
        # Write the marker normally — the recorded pid is os.getpid(),
        # which is alive (the test process) and the timestamp is fresh.
        admin.acquire_admin_update_marker(
            envs=["vibeqc-dev"], host="host_d",
        )
        caplog.set_level("INFO")
        present = daemon_with_state_dir._poll_admin_update_marker()
        # A live update still holds dispatch and the marker is preserved.
        assert present is True
        assert admin.admin_update_marker_exists() is True
        # No reap warning; one active-lease detection line instead.
        reaps = [
            r for r in caplog.records
            if r.levelname == "WARNING" and "reaped" in r.getMessage()
        ]
        assert reaps == []
        infos = [
            r for r in caplog.records
            if r.levelname == "INFO"
            and "marker detected" in r.getMessage()
            and "active lease" in r.getMessage()
        ]
        assert len(infos) == 1


# ----------------------------------------------------------------------
# v0.5.51: cgroup scope-name collision detection in _start_job
# ----------------------------------------------------------------------


class TestScopeCollisionDetection:
    """v0.5.51 (audit § 2e): when a previous job's scope unit leaked
    past --collect cleanup, _start_job must detect it pre-flight and
    either stop-and-retry OR land the spec FAILED with a clear
    reason — instead of letting systemd-run fail cryptically with
    'Unit already exists' and loop on the same broken dispatch."""

    def test_collision_with_failed_stop_lands_spec_failed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from vq import cgroup

        # Force cgroup_enabled=True via direct attribute mutation
        # AFTER constructor.
        d = Daemon(
            max_cpus=4,
            poll_interval=0.05,
            queue_dir=tmp_path / "q",
            jobs_dir=tmp_path / "j",
        )
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)
        d.cgroup_enabled = True

        # Mock collision detection: ALWAYS True (scope leaked).
        # Mock stop: always False (cannot clean it up).
        monkeypatch.setattr(cgroup, "scope_exists", lambda u, **kw: True)
        monkeypatch.setattr(cgroup, "stop_scope", lambda u, **kw: False)
        # If we got past the collision check and into wrap_command +
        # Popen, the test would be wrong — assert it doesn't via
        # raising on wrap_command.
        def _should_not_call(*a, **kw):
            raise AssertionError(
                "_start_job should have bailed before wrap_command"
            )

        monkeypatch.setattr(cgroup, "wrap_command", _should_not_call)

        spec = _submit(d, "collidejob01", ["true"])
        result = d._start_job(spec)
        assert result is False
        recovered = JobSpec.read(d._spec_path("collidejob01"))
        assert recovered.state == JobState.FAILED
        assert recovered.exit_code == -1

    def test_multi_user_runs_collision_preflight_without_cgroup(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """MU-1: in multi-user mode systemd-run --scope is MANDATORY (the
        privilege drop), so a leaked scope can collide even with cgroup
        enforcement off. The collision pre-flight must therefore run whenever
        ``_multi_user`` — not only when ``cgroup_enabled``. Pre-fix the gate
        was ``cgroup_enabled`` alone, so this dispatch skipped the check and
        sailed into the (cryptic) systemd-run failure."""
        from vq import cgroup
        from vq import daemon as daemon_mod

        d = Daemon(
            max_cpus=4,
            poll_interval=0.05,
            queue_dir=tmp_path / "q",
            jobs_dir=tmp_path / "j",
        )
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)
        d.cgroup_enabled = False  # enforcement OFF...
        d._multi_user = True  # ...but multi-user (scope still created)

        # Keep the spec store single-user (the per-user path resolution isn't
        # what's under test) — pin _spec_path so _submit and _start_job agree.
        monkeypatch.setattr(d, "_spec_path", lambda jid: d.queue_dir / f"{jid}.json")
        # Get past the multi-user spec gate + give the privilege-drop block
        # the bits it needs, so that WITHOUT the fix _start_job would sail
        # past the (skipped) collision check into wrap_command.
        monkeypatch.setattr(d, "_validate_multi_user_spec", lambda spec: None)
        monkeypatch.setattr(daemon_mod, "_gid_for_uid", lambda uid: 1000)
        monkeypatch.setattr(daemon_mod, "_chown_tree", lambda *a, **k: None)
        # Collision present + unstoppable -> the pre-flight must land FAILED.
        monkeypatch.setattr(cgroup, "scope_exists", lambda u, **kw: True)
        monkeypatch.setattr(cgroup, "stop_scope", lambda u, **kw: False)

        def _boom(*a: object, **k: object) -> None:
            raise AssertionError(
                "MU-1: collision pre-flight was skipped in multi-user mode"
            )

        monkeypatch.setattr(cgroup, "wrap_command", _boom)

        spec = _submit(d, "mucollide0001", ["true"])
        d._job_uid[spec.id] = "1000"
        result = d._start_job(spec)
        assert result is False
        assert JobSpec.read(d._spec_path("mucollide0001")).state == JobState.FAILED

    def test_no_collision_proceeds_normally(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When scope_exists returns False (no collision), _start_job
        proceeds to wrap_command + Popen as usual — the pre-flight
        check is silent in the happy path."""

        d = Daemon(
            max_cpus=4,
            poll_interval=0.05,
            queue_dir=tmp_path / "q",
            jobs_dir=tmp_path / "j",
        )
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)
        # Default cgroup_enabled is whatever cgroup.available()
        # returns — on macOS that's False, so wrap_command is never
        # called. To assert the no-collision happy-path proceeds,
        # we don't need cgroup_enabled True; we just need to confirm
        # the spec lands RUNNING.
        spec = _submit(d, "nocollidejob1", ["sleep", "5"])
        try:
            ok = d._start_job(spec)
            assert ok is True
            recovered = JobSpec.read(d._spec_path("nocollidejob1"))
            assert recovered.state == JobState.RUNNING
        finally:
            # Clean up the spawned sleep.
            for rj in d._running.values():
                try:
                    rj.popen.kill()
                    rj.popen.wait(timeout=2)
                except Exception:
                    pass
                rj.close_logs()


# ----------------------------------------------------------------------
# v0.6.0: _start_job race fix (audit § 2b)
# Write spec.state=RUNNING with pid=None BEFORE Popen so a crash in
# the narrow Popen window leaves a recoverable spec.
# ----------------------------------------------------------------------


class TestStartJobRaceFix:
    """v0.6.0 (audit § 2b): _start_job now writes spec.state=RUNNING
    with pid=None/pgid=None BEFORE invoking subprocess.Popen, so a
    daemon crash mid-Popen leaves the spec in a recoverable state
    (RUNNING + pgid=None → ABORTED_BY_QUEUE on next startup) instead
    of leaving spec=PENDING + a live process, which would cause
    double-dispatch."""

    def test_spec_persisted_as_RUNNING_before_popen(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Snapshot the spec on disk AT the Popen call (via a
        mock) and assert it's already RUNNING with pid=None at that
        point. If the daemon crashed exactly here, recovery would
        see a recoverable spec."""
        d = Daemon(
            max_cpus=4,
            poll_interval=0.05,
            queue_dir=tmp_path / "q",
            jobs_dir=tmp_path / "j",
        )
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)
        spec = _submit(d, "racejob00001", ["true"])

        snapshot: dict[str, object] = {}

        def _spy_popen(*args, **kwargs):
            disk = JobSpec.read(d._spec_path("racejob00001"))
            snapshot["state"] = disk.state
            snapshot["pid"] = disk.pid
            snapshot["pgid"] = disk.pgid
            # Raise to short-circuit — we only care about the spec
            # state at the moment Popen would have been called.
            raise OSError("mock-fail-after-spec-write")

        monkeypatch.setattr(
            "vq.daemon.subprocess.Popen",
            _spy_popen,
        )

        ok = d._start_job(spec)
        # Popen raised; _start_job's except branch marks FAILED.
        assert ok is False
        # The snapshot taken AT Popen entry shows the spec was
        # already on-disk as RUNNING + pid=None.
        assert snapshot["state"] == JobState.RUNNING
        assert snapshot["pid"] is None
        assert snapshot["pgid"] is None


# ----------------------------------------------------------------------
# v0.6.0: cgroup-scope MainPID cross-check at startup recovery
# (audit § 4c.2)
# ----------------------------------------------------------------------


class TestCgroupScopeMainPidCrossCheck:
    """v0.6.0: when cgroup_enabled, the startup-recovery path queries
    systemd for the vq-job-<id>.scope's MainPID and cross-checks
    against spec.pid. Mismatch → ABORTED_BY_QUEUE with reason
    'cgroup_scope_mismatch' rather than silently re-attaching."""

    @pytest.mark.skipif(not LINUX, reason="/proc only on Linux")
    def test_scope_mismatch_marks_aborted(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from vq import cgroup
        d = Daemon(
            max_cpus=4,
            poll_interval=0.05,
            queue_dir=tmp_path / "queue",
            jobs_dir=tmp_path / "jobs",
        )
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)
        d.cgroup_enabled = True

        # Write a RUNNING spec with our test PID + matching pgid +
        # matching pid_start_time so the pgid liveness AND PID-
        # fingerprint checks BOTH pass — the cgroup-scope check is
        # the FINAL gate.
        my_pid = os.getpid()
        my_pgid = os.getpgid(my_pid)
        my_start = _read_pid_start_time(my_pid)
        assert my_start is not None
        workspace = d.jobs_dir / "scope-mismatch-1"
        workspace.mkdir()
        spec = JobSpec(
            id="scope-mismatch-1",
            command=["true"],
            cwd=str(workspace),
            cpus=1,
            state=JobState.RUNNING,
            pid=my_pid,
            pgid=my_pgid,
            pid_start_time=my_start,
            started_at=utcnow_iso(),
        )
        spec.write(d._spec_path("scope-mismatch-1"))

        # Mock scope_main_pid to return a different pid (= mismatch).
        monkeypatch.setattr(
            cgroup, "scope_main_pid",
            lambda unit, **kw: 99999,
        )

        d._reattach_or_interrupt_at_startup()

        recovered = JobSpec.read(d._spec_path("scope-mismatch-1"))
        assert recovered.state == JobState.ABORTED_BY_QUEUE
        assert "scope-mismatch-1" not in d._orphans

    @pytest.mark.skipif(not LINUX, reason="/proc only on Linux")
    def test_scope_main_pid_none_falls_back_to_pgid_only(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When scope_main_pid returns None (systemctl unreachable,
        scope already cleaned, macOS) the recovery falls back to
        the pgid+fingerprint verdict — re-attach as orphan."""
        from vq import cgroup
        d = Daemon(
            max_cpus=4,
            poll_interval=0.05,
            queue_dir=tmp_path / "queue",
            jobs_dir=tmp_path / "jobs",
        )
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)
        d.cgroup_enabled = True

        my_pid = os.getpid()
        my_pgid = os.getpgid(my_pid)
        my_start = _read_pid_start_time(my_pid)
        assert my_start is not None
        workspace = d.jobs_dir / "scope-none-1"
        workspace.mkdir()
        spec = JobSpec(
            id="scope-none-1",
            command=["true"],
            cwd=str(workspace),
            cpus=1,
            state=JobState.RUNNING,
            pid=my_pid,
            pgid=my_pgid,
            pid_start_time=my_start,
            started_at=utcnow_iso(),
        )
        spec.write(d._spec_path("scope-none-1"))

        monkeypatch.setattr(
            cgroup,
            "scope_main_pid",
            lambda unit, **kw: None,
        )

        d._reattach_or_interrupt_at_startup()

        recovered = JobSpec.read(d._spec_path("scope-none-1"))
        # Spec stays RUNNING (re-attached as orphan); cgroup check
        # was inconclusive.
        assert recovered.state == JobState.RUNNING
        assert "scope-none-1" in d._orphans


# ----------------------------------------------------------------------
# v0.6.2: daemon-side version-drift probe (audit § 1c)
# ----------------------------------------------------------------------


class TestVersionDriftProbe:
    """v0.6.2: `Daemon._maybe_check_version_drift` periodically
    re-reads vq.__version__ from on-disk source and warns when the
    daemon's import-time version disagrees. Audit § 1c — closes
    the silent-stale-code gap for operator-bypass scenarios."""

    def test_read_vq_version_from_source_for_live_install(self) -> None:
        """The helper reads vq/__init__.py and parses out
        __version__. On a healthy editable install it returns a
        non-empty string matching vq.__version__."""
        from vq import __version__ as live_version
        v = _read_vq_version_from_source()
        assert v == live_version

    def test_drift_detected_when_on_disk_differs(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        from vq import daemon as daemon_mod
        d = Daemon(
            max_cpus=4,
            poll_interval=0.05,
            queue_dir=tmp_path / "q",
            jobs_dir=tmp_path / "j",
        )
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)

        # Force on-disk to report a different version than the
        # running daemon's (the actual vq.__version__).
        monkeypatch.setattr(
            daemon_mod, "_read_vq_version_from_source",
            lambda: "0.99.99-test-drift",
        )

        caplog.set_level("INFO")
        d._maybe_check_version_drift()
        warnings = [
            r for r in caplog.records
            if r.levelname == "WARNING"
            and "version drift" in r.getMessage()
        ]
        assert len(warnings) == 1
        # Second call within the interval is a no-op (rate-limited).
        caplog.clear()
        d._maybe_check_version_drift()
        again = [
            r for r in caplog.records
            if "version drift" in r.getMessage()
        ]
        assert again == []

    def test_no_drift_no_warning(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        from vq import __version__ as live_version
        from vq import daemon as daemon_mod
        d = Daemon(
            max_cpus=4,
            poll_interval=0.05,
            queue_dir=tmp_path / "q",
            jobs_dir=tmp_path / "j",
        )
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)
        monkeypatch.setattr(
            daemon_mod, "_read_vq_version_from_source",
            lambda: live_version,
        )
        caplog.set_level("INFO")
        d._maybe_check_version_drift()
        warnings = [
            r for r in caplog.records
            if r.levelname == "WARNING"
        ]
        assert warnings == []

    def test_drift_cleared_logs_info(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """First check sees drift → WARNING. Subsequent check (after
        the interval) sees matching versions → INFO 'drift cleared'."""
        from vq import __version__ as live_version
        from vq import daemon as daemon_mod
        d = Daemon(
            max_cpus=4,
            poll_interval=0.05,
            queue_dir=tmp_path / "q",
            jobs_dir=tmp_path / "j",
        )
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)

        # First tick: drift present.
        monkeypatch.setattr(
            daemon_mod, "_read_vq_version_from_source",
            lambda: "0.99.99-test-drift",
        )
        caplog.set_level("INFO")
        d._maybe_check_version_drift()
        warn_msgs = [
            r for r in caplog.records
            if r.levelname == "WARNING"
            and "version drift" in r.getMessage()
        ]
        assert len(warn_msgs) == 1

        # Force the rate-limit window to elapse so the next call
        # actually re-checks.
        d._last_version_drift_check_monotonic = 0.0

        # Second tick: on-disk now matches → drift cleared.
        monkeypatch.setattr(
            daemon_mod, "_read_vq_version_from_source",
            lambda: live_version,
        )
        caplog.clear()
        d._maybe_check_version_drift()
        cleared_msgs = [
            r for r in caplog.records
            if r.levelname == "INFO"
            and "drift cleared" in r.getMessage()
        ]
        assert len(cleared_msgs) == 1

    def test_unreadable_source_is_silent_noop(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """When the helper returns None (source file gone /
        unreadable / no version line), the probe is a quiet
        no-op — best-effort, no spam."""
        from vq import daemon as daemon_mod
        d = Daemon(
            max_cpus=4,
            poll_interval=0.05,
            queue_dir=tmp_path / "q",
            jobs_dir=tmp_path / "j",
        )
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)
        monkeypatch.setattr(
            daemon_mod, "_read_vq_version_from_source",
            lambda: None,
        )
        caplog.set_level("INFO")
        d._maybe_check_version_drift()
        messages = [
            r for r in caplog.records
            if "drift" in r.getMessage()
        ]
        assert messages == []
class TestQueueDirectoryLock:
    """v0.5.51: only one daemon per queue directory."""

    def test_second_daemon_raises(self, tmp_path):
        """Two Daemons against the same queue_dir must not both succeed."""
        qd = tmp_path / "q"
        jd = tmp_path / "j"
        qd.mkdir()
        jd.mkdir()
        d1 = Daemon(queue_dir=qd, jobs_dir=jd)
        import pytest
        with pytest.raises(RuntimeError, match="Another vq daemon"):
            Daemon(queue_dir=qd, jobs_dir=jd)
        # d1 still works
        assert d1.max_cpus is not None

    def test_different_queue_dirs_independent(self, tmp_path):
        """Different queue directories should be independent."""
        qd1, qd2 = tmp_path / "q1", tmp_path / "q2"
        jd1, jd2 = tmp_path / "j1", tmp_path / "j2"
        for d in (qd1, qd2, jd1, jd2):
            d.mkdir()
        d1 = Daemon(queue_dir=qd1, jobs_dir=jd1)
        d2 = Daemon(queue_dir=qd2, jobs_dir=jd2)
        assert d1.max_cpus == d2.max_cpus


class TestAbortedByQueueWorkdirCleanup:
    """CLEAN-4: a job aborted by the queue must honour --clean-tmp, like every
    other terminal path does."""

    def test_clean_tmp_workdir_removed_on_queue_abort(self, tmp_path: Path) -> None:
        d = Daemon(
            max_cpus=4, poll_interval=0.05,
            queue_dir=tmp_path / "q", jobs_dir=tmp_path / "j",
        )
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)
        ws = d.jobs_dir / "abrtwd000001"
        ws.mkdir(parents=True)
        workdir = tmp_path / "workdirs" / "abrtwd000001"
        workdir.mkdir(parents=True)
        (workdir / "scratch.bin").write_text("x")

        spec = JobSpec(
            id="abrtwd000001",
            command=["sleep", "1"],
            cwd=str(ws),
            cpus=1,
            state=JobState.RUNNING,
            pid=999,
            pgid=4242,
            workdir=str(workdir),
            clean_workdir_on_terminal=True,
        )
        spec.write(d._spec_path("abrtwd000001"))

        d._mark_aborted_by_queue(
            spec,
            reason="test: pgid gone at startup",
            process_exit_confirmed=True,
        )

        recovered = JobSpec.read(d._spec_path("abrtwd000001"))
        assert recovered.state == JobState.ABORTED_BY_QUEUE
        assert not workdir.exists(), (
            "CLEAN-4: the --clean-tmp workdir must be removed on a queue-abort"
        )

    def test_workdir_kept_without_clean_tmp(self, tmp_path: Path) -> None:
        """Without --clean-tmp the workdir is preserved (the opt-in gate)."""
        d = Daemon(
            max_cpus=4, poll_interval=0.05,
            queue_dir=tmp_path / "q", jobs_dir=tmp_path / "j",
        )
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)
        ws = d.jobs_dir / "abrtwd000002"
        ws.mkdir(parents=True)
        workdir = tmp_path / "workdirs" / "abrtwd000002"
        workdir.mkdir(parents=True)

        spec = JobSpec(
            id="abrtwd000002",
            command=["sleep", "1"],
            cwd=str(ws),
            cpus=1,
            state=JobState.RUNNING,
            pid=999,
            pgid=4242,
            workdir=str(workdir),
            clean_workdir_on_terminal=False,
        )
        spec.write(d._spec_path("abrtwd000002"))

        d._mark_aborted_by_queue(spec, reason="test")

        assert workdir.exists(), "workdir must be kept when --clean-tmp not set"


class TestAnUnpinnedSubmitSurvivesARuntimeRoll:
    """2026-08-01: all 123 of the campaign's bundle jobs failed with "runtime
    pin mismatch before dispatch" because the release runtime rolled between
    their submit and their dispatch tick -- which is exactly what an unpinned
    submit exists to survive.

    Three different things landed in `expected_git_sha`: an explicit
    `--expected-sha`, a configured `expected_git_sha`, and (when neither was
    given) whatever the runtime happened to be at submit time. The first two
    are intent; the third is an observation, and enforcing an observation on a
    continuously-rolling fleet means any job queued across a roll dies.
    """

    def _program_config(self, cfg_dir: Path, git_dir: Path, sha: str | None) -> None:
        body = (
            "[programs.vibeqc-dev]\n"
            'kind = "venv"\n'
            f'python = "{sys.executable}"\n'
            f'git_dir = "{git_dir}"\n'
        )
        if sha is not None:
            body += f'expected_git_sha = "{sha}"\n'
        (cfg_dir / "config.toml").write_text(body)

    def test_an_observed_sha_does_not_block_dispatch_after_a_roll(
        self, daemon: Daemon, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir(exist_ok=True)
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
        git_dir = tmp_path / "repo"
        submitted_sha = _init_git_repo(git_dir)
        self._program_config(cfg_dir, git_dir, None)
        rolled_sha = _commit_git_file(git_dir, "ROLL.txt")
        assert rolled_sha != submitted_sha

        dispatcher = _CapturingDispatcher()
        daemon.dispatcher = dispatcher
        spec = _submit(daemon, "unpinned", ["true"], cpus=1)
        spec.program = "vibeqc-dev"
        # What an unpinned submit records: observed, not required.
        spec.program_runtime_pin = ProgramRuntimePin(
            expected_git_sha=submitted_sha, enforce_git_sha=False
        )
        spec.write(daemon._spec_path(spec.id))

        assert daemon._start_job(spec) is True

        on_disk = JobSpec.read(daemon._spec_path(spec.id))
        assert on_disk.state != JobState.FAILED

    def test_the_observed_sha_is_still_recorded_for_provenance(
        self, daemon: Daemon, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Not enforcing it must not mean discarding it."""
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir(exist_ok=True)
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
        git_dir = tmp_path / "repo"
        submitted_sha = _init_git_repo(git_dir)
        self._program_config(cfg_dir, git_dir, None)

        pin = _program_runtime_pin_for_submit(config.load_config(), "vibeqc-dev")

        assert pin is not None
        # Captured full; the fixture hands back the abbreviated form.
        assert pin.expected_git_sha is not None
        assert pin.expected_git_sha.startswith(submitted_sha)
        assert pin.enforce_git_sha is False

    def test_an_explicit_expected_sha_is_still_enforced(
        self, daemon: Daemon, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The operator asked for a specific commit; a roll must still fail."""
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir(exist_ok=True)
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
        git_dir = tmp_path / "repo"
        submitted_sha = _init_git_repo(git_dir)
        self._program_config(cfg_dir, git_dir, None)

        pin = _program_runtime_pin_for_submit(
            config.load_config(), "vibeqc-dev", expected_sha=submitted_sha
        )

        assert pin is not None
        assert pin.enforce_git_sha is True

        rolled_sha = _commit_git_file(git_dir, "ROLL.txt")
        dispatcher = _CapturingDispatcher()
        daemon.dispatcher = dispatcher
        spec = _submit(daemon, "pinned", ["true"], cpus=1)
        spec.program = "vibeqc-dev"
        spec.program_runtime_pin = pin
        spec.write(daemon._spec_path(spec.id))

        assert daemon._start_job(spec) is False
        on_disk = JobSpec.read(daemon._spec_path(spec.id))
        assert on_disk.failure_reason is not None
        assert rolled_sha in on_disk.failure_reason

    def test_a_configured_site_pin_is_still_enforced(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A site that pinned its program in config meant it."""
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir(exist_ok=True)
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
        git_dir = tmp_path / "repo"
        submitted_sha = _init_git_repo(git_dir)
        self._program_config(cfg_dir, git_dir, submitted_sha)

        pin = _program_runtime_pin_for_submit(config.load_config(), "vibeqc-dev")

        assert pin is not None
        assert pin.enforce_git_sha is True

    def test_an_old_spec_without_the_flag_stays_enforced(self) -> None:
        """Default True, so a spec written before this change is unchanged."""
        assert ProgramRuntimePin(expected_git_sha="abc").enforce_git_sha is True


@pytest.mark.parametrize("fault", ["missing", "permission", "cwd"])
def test_dispatch_os_error_survives_in_spec_event_and_stderr(
    daemon: Daemon, monkeypatch: pytest.MonkeyPatch, fault: str,
) -> None:
    from vq.dispatch import LocalDispatcher

    class BrokenLaunch(LocalDispatcher):
        def launch(self, **kwargs):
            # Exercise real Popen errors at the wrapper launch boundary.
            if fault == "cwd":
                kwargs["cwd"] = daemon.jobs_dir / "missing-cwd"
            elif fault == "permission":
                executable = daemon.jobs_dir / "not-executable"
                executable.write_text("#!/bin/sh\nexit 0\n")
                executable.chmod(0o600)
                kwargs["run_command"] = [str(executable)]
            else:
                kwargs["run_command"] = [str(daemon.jobs_dir / "missing-executable")]
            return super().launch(**kwargs)

    daemon.dispatcher = BrokenLaunch()
    spec = _submit(daemon, "execfail", ["true"])
    assert not daemon._start_job(spec)
    saved = JobSpec.read(daemon._spec_path(spec.id))
    assert saved.state == JobState.FAILED
    assert saved.exit_code == -1
    assert saved.failure_reason and "Errno" in saved.failure_reason
    expected = "Permission denied" if fault == "permission" else "No such file"
    assert expected in saved.failure_reason
    workspace = Path(spec.cwd)
    assert saved.failure_reason in (workspace / spec.stderr_path).read_text()
    assert saved.failure_reason in (workspace / "_vq" / "events.jsonl").read_text()
