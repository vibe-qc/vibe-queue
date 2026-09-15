"""Tests for v0.11.0 per-job ``--refresh ENV``: the JobSpec.refresh_before
field, the ``vq submit --refresh`` CLI flag, submit_remote argv passthrough,
and the daemon's ``_maybe_refresh_before_run`` drain + rebuild orchestration
(with ``admin.update_env`` mocked).

The feature: a job submitted with ``--refresh dev`` makes the daemon, WHEN
THAT JOB IS NEXT TO DISPATCH, drain the host (let running jobs finish), run
``admin.update_env("dev")`` (git pull + update_script), then dispatch the job
against the freshly-rebuilt venv. A failed rebuild fails the JOB (with a
failure_reason) rather than running it against a broken env. See
HANDOVER_VQ_REFRESH.md.
"""
from __future__ import annotations

import json
import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import admin, config, paths, submit, transport
from vq.cli import main
from vq.config import HostConfig
from vq.daemon import BUILD_JOB_PRIORITY, Daemon, _TerminalSurvivor
from vq.spec import JobSpec, JobState
from vq.submit import submit_local

# ----------------------------------------------------------------------
# JobSpec.refresh_before field
# ----------------------------------------------------------------------


class TestRefreshBeforeSpecField:
    def test_default_is_none(self) -> None:
        spec = JobSpec(id="a" * 12, command=["true"], cwd="/tmp", cpus=1)
        assert spec.refresh_before is None

    def test_accepts_env_name(self) -> None:
        spec = JobSpec(
            id="b" * 12, command=["true"], cwd="/tmp", cpus=1,
            refresh_before="vibeqc-dev",
        )
        assert spec.refresh_before == "vibeqc-dev"

    def test_roundtrips_through_disk(self, tmp_path: Path) -> None:
        spec = JobSpec(
            id="c" * 12, command=["true"], cwd="/tmp", cpus=1,
            refresh_before="vibeqc-dev",
        )
        path = tmp_path / "spec.json"
        spec.write(path)
        back = JobSpec.read(path)
        assert back.refresh_before == "vibeqc-dev"

    def test_none_roundtrips_through_disk(self, tmp_path: Path) -> None:
        spec = JobSpec(id="d" * 12, command=["true"], cwd="/tmp", cpus=1)
        path = tmp_path / "spec.json"
        spec.write(path)
        back = JobSpec.read(path)
        assert back.refresh_before is None

    def test_old_spec_without_refresh_before_reads_clean(
        self, tmp_path: Path
    ) -> None:
        """Additive field: a v2 spec JSON written before v0.11.0 (no
        'refresh_before' key) reads into the current model with
        refresh_before defaulting to None — no SPEC_VERSION bump."""
        old_json = {
            "spec_version": 2,
            "id": "e" * 12,
            "command": ["true"],
            "cwd": "/tmp/e",
            "cpus": 1,
            "state": "pending",
            "submitted_at": "2026-06-10T12:00:00+00:00",
            # NOTE: no "refresh_before" key — simulates a pre-v0.11.0 spec
        }
        path = tmp_path / "old.json"
        path.write_text(json.dumps(old_json))
        spec = JobSpec.read(path)
        assert spec.refresh_before is None
        assert spec.id == "e" * 12


# ----------------------------------------------------------------------
# submit_local writes refresh_before
# ----------------------------------------------------------------------


@pytest.fixture
def submit_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    return tmp_path


class TestSubmitLocalWritesRefreshBefore:
    def test_default_is_none(self, submit_state: Path) -> None:
        script = submit_state / "job.py"
        script.write_text("print('hi')\n")
        jobid = submit_local(host="localhost", input_file=str(script))
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.refresh_before is None

    def test_explicit_env(self, submit_state: Path) -> None:
        script = submit_state / "job.py"
        script.write_text("print('hi')\n")
        jobid = submit_local(
            host="localhost", input_file=str(script),
            refresh_before="vibeqc-dev",
        )
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.refresh_before == "vibeqc-dev"


# ----------------------------------------------------------------------
# CLI --refresh flag
# ----------------------------------------------------------------------


class TestRefreshCLI:
    def test_submit_refresh_flag_writes_spec(
        self, submit_state: Path
    ) -> None:
        (submit_state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        script = submit_state / "job.py"
        script.write_text("print('hi')\n")
        result = CliRunner().invoke(
            main, ["submit", str(script), "--refresh", "vibeqc-dev"]
        )
        assert result.exit_code == 0, result.output
        jobid = result.output.strip()
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.refresh_before == "vibeqc-dev"

    def test_submit_default_refresh_none(self, submit_state: Path) -> None:
        (submit_state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        script = submit_state / "job.py"
        script.write_text("print('hi')\n")
        result = CliRunner().invoke(main, ["submit", str(script)])
        assert result.exit_code == 0, result.output
        jobid = result.output.strip()
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.refresh_before is None

    def test_submit_help_mentions_refresh(self) -> None:
        result = CliRunner().invoke(main, ["submit", "--help"])
        assert result.exit_code == 0
        assert "--refresh" in result.output

    def test_refresh_rejected_with_array(self, submit_state: Path) -> None:
        """--refresh is single-job only in v1; combining with --array is
        a fast UsageError, not a silent drop."""
        (submit_state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        script = submit_state / "job.py"
        script.write_text("print('hi')\n")
        result = CliRunner().invoke(
            main,
            ["submit", str(script), "--refresh", "vibeqc-dev",
             "--array", "3"],
        )
        assert result.exit_code != 0
        assert "--refresh" in result.output
        assert "array" in result.output

    def test_refresh_rejected_with_chain(self, submit_state: Path) -> None:
        (submit_state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        script = submit_state / "job.py"
        script.write_text("print('hi')\n")
        result = CliRunner().invoke(
            main,
            ["submit", str(script), "--refresh", "vibeqc-dev",
             "--chain", "3"],
        )
        assert result.exit_code != 0
        assert "--refresh" in result.output


# ----------------------------------------------------------------------
# submit_remote argv passthrough (SSH primitives mocked)
# ----------------------------------------------------------------------


@pytest.fixture
def remote_calls(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, ...]]:
    """Record run_remote_vq argv; canned single-jobid response."""
    recorded: list[tuple[str, ...]] = []

    def fake_upload(host_cfg: HostConfig, local: Path, remote: str) -> None:
        return None

    def fake_run_remote_vq(
        host_cfg: HostConfig,
        *args: str,
        check: bool = True,
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        recorded.append(args)
        return subprocess.CompletedProcess(
            args=["ssh", host_cfg.ssh, host_cfg.remote_vq, *args],
            returncode=0,
            stdout="abc123def456\n",
            stderr="",
        )

    def fake_run_remote_shell(
        host_cfg: HostConfig,
        *args: str,
        check: bool = True,
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=list(args), returncode=0, stdout="", stderr="",
        )

    monkeypatch.setattr(transport, "upload_file", fake_upload)
    monkeypatch.setattr(transport, "run_remote_vq", fake_run_remote_vq)
    monkeypatch.setattr(transport, "run_remote_shell", fake_run_remote_shell)
    return recorded


@pytest.fixture
def host_cfg() -> HostConfig:
    return HostConfig(ssh="host_d", remote_vq="vq", remote_python="/remote/py3")


class TestRefreshRemotePassthrough:
    def test_refresh_forwarded_to_remote_argv(
        self, tmp_path: Path, host_cfg: HostConfig,
        remote_calls: list[tuple[str, ...]],
    ) -> None:
        """--refresh ENV becomes ``--refresh ENV`` on the remote vq argv
        so the remote spec carries refresh_before and the remote daemon
        does the drain + rebuild."""
        src = tmp_path / "input.py"
        src.write_text("print('remote hi')")
        jids = submit.submit_remote(
            host="host_d", host_cfg=host_cfg, input_file=str(src),
            refresh_before="vibeqc-dev",
        )
        assert jids == ["abc123def456"]
        (vq_args,) = remote_calls
        assert "--refresh" in vq_args
        idx = vq_args.index("--refresh")
        assert vq_args[idx + 1] == "vibeqc-dev"

    def test_no_refresh_omits_flag(
        self, tmp_path: Path, host_cfg: HostConfig,
        remote_calls: list[tuple[str, ...]],
    ) -> None:
        src = tmp_path / "input.py"
        src.write_text("")
        submit.submit_remote(
            host="host_d", host_cfg=host_cfg, input_file=str(src),
        )
        (vq_args,) = remote_calls
        assert "--refresh" not in vq_args


# ----------------------------------------------------------------------
# Daemon orchestration: _maybe_refresh_before_run
# ----------------------------------------------------------------------


def _ok_result() -> admin.UpdateResult:
    """A successful UpdateResult (clean git pull, no update_script)."""
    return admin.UpdateResult(
        env="vibeqc-dev", git_dir="/tmp/git", branch=None,
        update_script=None, git_pull_rc=0,
    )


def _failed_result() -> admin.UpdateResult:
    """A failed UpdateResult: git pull returned non-zero."""
    return admin.UpdateResult(
        env="vibeqc-dev", git_dir="/tmp/git", branch=None,
        update_script="update.sh", git_pull_rc=0,
        update_script_rc=1, update_script_output="build broke",
    )


def _drain_daemon_running(daemon: Daemon) -> None:
    """Teardown helper: kill + reap any processes the daemon started."""
    for rj in daemon._running.values():
        try:
            rj.popen.kill()
            rj.popen.wait(timeout=1)
        except Exception:
            pass
        rj.close_logs()


@pytest.fixture
def serial_daemon(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[Daemon]:
    """Daemon with max_jobs=1 + isolated queue/jobs dirs (so it never
    collides with a live daemon on the default state dir). max_jobs=1
    lets us observe which single job dispatches."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    d = Daemon(
        max_cpus=8,
        max_jobs=1,
        poll_interval=0.05,
        queue_dir=tmp_path / "queue",
        jobs_dir=tmp_path / "jobs",
    )
    d.queue_dir.mkdir(parents=True, exist_ok=True)
    d.jobs_dir.mkdir(parents=True, exist_ok=True)
    yield d
    _drain_daemon_running(d)


@pytest.fixture
def drain_daemon(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[Daemon]:
    """Daemon with max_jobs=2 + isolated dirs. The headroom matters for
    the drain-wait tests: with one job RUNNING and a refresh job queued,
    the budget gate (max_jobs) does NOT short-circuit dispatch, so the
    refresh hook is actually reached and we can assert it HOLDS dispatch
    purely on the drain condition (running jobs > 0) — independent of any
    budget pressure. (With max_jobs=1 the budget gate returns first; the
    outcome — refresh not started — is the same, but it wouldn't be
    *the drain gate* doing the holding.)"""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    d = Daemon(
        max_cpus=8,
        max_jobs=2,
        poll_interval=0.05,
        queue_dir=tmp_path / "queue",
        jobs_dir=tmp_path / "jobs",
    )
    d.queue_dir.mkdir(parents=True, exist_ok=True)
    d.jobs_dir.mkdir(parents=True, exist_ok=True)
    yield d
    _drain_daemon_running(d)


def _write_pending(
    daemon: Daemon,
    jobid: str,
    *,
    refresh_before: str | None = None,
    priority: int = 0,
    submitted_at: str = "2026-06-10T00:00:00+00:00",
    command: list[str] | None = None,
    build_env: str | None = None,
    scheduler_target: str | None = None,
    program: str | None = None,
    state: JobState = JobState.PENDING,
    depends_on: list[str] | None = None,
) -> JobSpec:
    workspace = daemon.jobs_dir / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(
        id=jobid,
        command=command or ["sleep", "30"],
        cwd=str(workspace),
        cpus=1,
        priority=priority,
        submitted_at=submitted_at,
        refresh_before=refresh_before,
        build_env=build_env,
        scheduler_target=scheduler_target,
        program=program,
        state=state,
        depends_on=depends_on or [],
    )
    spec.write(daemon._spec_path(jobid))
    return spec


def _state(daemon: Daemon, jobid: str) -> JobState:
    return JobSpec.read(daemon._spec_path(jobid)).state


class TestBuildAsJobOrchestration:
    """v0.12.0: _maybe_refresh_before_run no longer rebuilds inline. On a
    drained host it CREATES a build job and points the --refresh job at it
    via depends_on. The depends_on gate then holds the refresh job until the
    build COMPLETED, or cascade-fails it if the build FAILED."""

    def test_creates_build_job_and_attaches_dependency(
        self, serial_daemon: Daemon
    ) -> None:
        spec = _write_pending(
            serial_daemon, "jobrefresh01", refresh_before="vibeqc-dev"
        )
        held = serial_daemon._maybe_refresh_before_run(spec, [spec])
        assert held is True  # this tick is held
        builds = [
            s for s in serial_daemon._iter_specs()
            if s.build_env == "vibeqc-dev"
        ]
        assert len(builds) == 1
        build = builds[0]
        assert build.command[-2:] == ["build-env", "vibeqc-dev"]
        assert build.priority == BUILD_JOB_PRIORITY
        assert build.job_name == "build-vibeqc-dev"
        # v0.12.x fix 1: the build job carries a wall-time cap so the
        # watchdog reaps a wedged rebuild (and releases its CPUs) even if
        # the in-process stall guard is missed.
        assert build.wall_time_seconds is not None
        assert build.wall_time_seconds > 0
        refreshed = JobSpec.read(serial_daemon._spec_path("jobrefresh01"))
        assert refreshed.depends_on == [build.id]
        assert refreshed.refresh_before is None

    def test_dedup_two_refresh_jobs_share_one_build(
        self, serial_daemon: Daemon
    ) -> None:
        s1 = _write_pending(
            serial_daemon, "jobrefresh01", refresh_before="vibeqc-dev"
        )
        serial_daemon._maybe_refresh_before_run(s1, [s1])
        builds = [
            s for s in serial_daemon._iter_specs()
            if s.build_env == "vibeqc-dev"
        ]
        assert len(builds) == 1
        build_id = builds[0].id
        # second refresh job, SAME env, finds the in-flight build + attaches
        s2 = _write_pending(
            serial_daemon, "jobrefresh02", refresh_before="vibeqc-dev",
            submitted_at="2026-06-10T00:00:05+00:00",
        )
        all_specs = list(serial_daemon._iter_specs())
        serial_daemon._maybe_refresh_before_run(s2, all_specs)
        builds = [
            s for s in serial_daemon._iter_specs()
            if s.build_env == "vibeqc-dev"
        ]
        assert len(builds) == 1, "a burst of refresh jobs triggers ONE build"
        assert JobSpec.read(
            serial_daemon._spec_path("jobrefresh01")
        ).depends_on == [build_id]
        assert JobSpec.read(
            serial_daemon._spec_path("jobrefresh02")
        ).depends_on == [build_id]

    def test_create_failure_fails_the_refresh_job(
        self, serial_daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If creating the build job raises, FAIL the refresh job rather than
        loop forever as pending[0]."""
        def boom(*a, **k):  # type: ignore[no-untyped-def]
            raise OSError("disk full writing build spec")

        monkeypatch.setattr(serial_daemon, "_create_build_job", boom)
        spec = _write_pending(
            serial_daemon, "jobrefresh03", refresh_before="vibeqc-dev"
        )
        held = serial_daemon._maybe_refresh_before_run(spec, [spec])
        assert held is True
        failed = JobSpec.read(serial_daemon._spec_path("jobrefresh03"))
        assert failed.state == JobState.FAILED
        assert failed.failure_reason is not None
        assert "vibeqc-dev" in failed.failure_reason

    def test_failed_build_cascades_to_refresh_job(
        self, drain_daemon: Daemon
    ) -> None:
        """A FAILED build job cascade-fails its --refresh dependents via the
        existing depends_on gate, so the refresh job never runs against a
        half-built env."""
        _write_pending(
            drain_daemon, "jobbuildbad1", build_env="vibeqc-dev",
            state=JobState.FAILED, command=["false"],
        )
        _write_pending(
            drain_daemon, "jobrefresh04", depends_on=["jobbuildbad1"],
        )
        drain_daemon._dispatch_pending()
        assert _state(drain_daemon, "jobrefresh04") == JobState.FAILED


class TestBuildJobExclusivity:
    """v0.12.0: a build job runs exclusively for local/env-dependent work.

    A running build still holds local jobs and scheduler jobs that would use
    or depend on the mutating env, but unrelated scheduler-target jobs may
    dispatch because they consume remote scheduler resources and cannot race
    the local env rebuild."""

    def test_running_build_holds_local_dispatch(
        self, drain_daemon: Daemon
    ) -> None:
        # A build job recorded RUNNING. The gate reads spec state, not the
        # _running dict, so no real subprocess is needed to exercise it.
        _write_pending(
            drain_daemon, "jobbuildrun1", build_env="vibeqc-dev",
            state=JobState.RUNNING, command=["sleep", "30"],
        )
        _write_pending(drain_daemon, "jobnormal001", command=["sleep", "30"])
        drain_daemon._dispatch_pending()
        # despite max_jobs=2 headroom, the normal job is HELD by the gate.
        assert _state(drain_daemon, "jobnormal001") == JobState.PENDING
        assert len(drain_daemon._running) == 0

    def test_running_build_allows_unrelated_scheduler_dispatch(
        self, drain_daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_pending(
            drain_daemon, "jobbuildrun1", build_env="vibeqc-dev",
            state=JobState.RUNNING, command=["sleep", "30"],
        )
        _write_pending(
            drain_daemon, "jobscheduler1", scheduler_target="host_c",
            program="orca",
        )
        started: list[str] = []

        def fake_start_scheduler_job(spec: JobSpec) -> bool:
            started.append(spec.id)
            spec.state = JobState.RUNNING
            spec.write(drain_daemon._spec_path(spec.id))
            return True

        monkeypatch.setattr(
            drain_daemon, "_start_scheduler_job", fake_start_scheduler_job
        )

        drain_daemon._dispatch_pending()

        assert started == ["jobscheduler1"]
        assert _state(drain_daemon, "jobscheduler1") == JobState.RUNNING

    def test_running_build_holds_scheduler_job_using_same_env(
        self, drain_daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_pending(
            drain_daemon, "jobbuildrun1", build_env="vibeqc-dev",
            state=JobState.RUNNING, command=["sleep", "30"],
        )
        _write_pending(
            drain_daemon, "jobscheduler1", scheduler_target="host_c",
            program="vibeqc-dev",
        )
        started: list[str] = []

        def fake_start_scheduler_job(spec: JobSpec) -> bool:
            started.append(spec.id)
            return True

        monkeypatch.setattr(
            drain_daemon, "_start_scheduler_job", fake_start_scheduler_job
        )

        drain_daemon._dispatch_pending()

        assert started == []
        assert _state(drain_daemon, "jobscheduler1") == JobState.PENDING


class TestRefreshDrainWait:
    def test_running_job_holds_refresh(
        self, drain_daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """With a job already RUNNING (and budget headroom, so the budget
        gate doesn't short-circuit first), the refresh must NOT start:
        the daemon drains on the drain condition alone. admin.update_env
        stays untouched and the refresh job stays PENDING."""
        called = False

        def fake_update_env(*a, **k):  # type: ignore[no-untyped-def]
            nonlocal called
            called = True
            return _ok_result()

        monkeypatch.setattr(admin, "update_env", fake_update_env)

        # A plain long-running job dispatched first (no refresh).
        _write_pending(drain_daemon, "jobrunning0a", command=["sleep", "30"])
        drain_daemon._dispatch_pending()
        assert _state(drain_daemon, "jobrunning0a") == JobState.RUNNING
        assert len(drain_daemon._running) == 1

        # Now a refresh job is queued behind it. max_jobs=2 means the
        # budget gate would PERMIT a second start — so anything holding
        # dispatch here is the refresh drain gate, not budget pressure.
        _write_pending(
            drain_daemon, "jobrefresh0b", refresh_before="vibeqc-dev",
            submitted_at="2026-06-10T00:00:05+00:00",
        )

        # Dispatch tick: the host is NOT drained (one job running), so
        # the refresh must be held — update_env not called, drain
        # breadcrumb recorded, refresh job still PENDING (and NOT
        # dispatched despite the budget headroom).
        drain_daemon._dispatch_pending()
        assert called is False, "refresh started before host drained"
        assert drain_daemon._refresh_draining_for == "jobrefresh0b"
        assert _state(drain_daemon, "jobrefresh0b") == JobState.PENDING

    def test_killed_job_survivor_holds_refresh(self, drain_daemon: Daemon) -> None:
        """A killed job whose process group outlived its reaped wrapper is
        still running on the host until the kill grace ends. Its spec is
        already KILLED and it is in neither _running nor _orphans, so only
        its survivor record can keep the host from counting as drained.
        max_jobs=2 leaves budget headroom, so the drain gate is what holds."""
        drain_daemon._terminal_survivors["jobkilled0d"] = _TerminalSurvivor(
            pgid=900004,
            cpus=1,
            mem_mb=None,
            state=JobState.KILLED,
            workspace=drain_daemon.jobs_dir / "jobkilled0d",
            deadline=float("inf"),
        )
        _write_pending(drain_daemon, "jobrefresh0d", refresh_before="vibeqc-dev")

        drain_daemon._dispatch_pending()
        assert drain_daemon._refresh_draining_for == "jobrefresh0d"
        assert not [
            s for s in drain_daemon._iter_specs() if s.build_env == "vibeqc-dev"
        ], "build job created while a killed job's group was still on the host"
        assert _state(drain_daemon, "jobrefresh0d") == JobState.PENDING

        # Once the escalation ends the group, the host is drained.
        del drain_daemon._terminal_survivors["jobkilled0d"]
        drain_daemon._dispatch_pending()
        builds = [
            s for s in drain_daemon._iter_specs() if s.build_env == "vibeqc-dev"
        ]
        assert len(builds) == 1

    def test_drain_completes_then_build_job_created(
        self, drain_daemon: Daemon
    ) -> None:
        """Once the running job finishes (drain complete), the held refresh
        creates its build job and attaches to it on the next tick."""
        # Short-lived job so it terminates quickly + an empty-running state
        # after reconcile. Use 'true' (exits immediately).
        _write_pending(drain_daemon, "jobquick00a", command=["true"])
        drain_daemon._dispatch_pending()
        # Drive the job to completion: reconcile picks up the exit.
        rj = drain_daemon._running.get("jobquick00a")
        if rj is not None:
            rj.popen.wait(timeout=5)
        # A refresh job is queued.
        _write_pending(
            drain_daemon, "jobrefresh0c", refresh_before="vibeqc-dev",
            submitted_at="2026-06-10T00:00:05+00:00",
        )
        # Reconcile clears the finished job out of _running, then the refresh
        # hook sees a drained host and creates the build job.
        drain_daemon._reconcile_running()
        assert len(drain_daemon._running) == 0
        drain_daemon._dispatch_pending()
        builds = [
            s for s in drain_daemon._iter_specs()
            if s.build_env == "vibeqc-dev"
        ]
        assert len(builds) == 1
        refreshed = JobSpec.read(drain_daemon._spec_path("jobrefresh0c"))
        assert refreshed.depends_on == [builds[0].id]
        assert refreshed.refresh_before is None
