"""Regression tests for the v0.12.x build-env wedge fixes (2026-06-26
fleet incident, where the dev-HEAD auto-update timer wedged host_e/host_b/
host_a with stuck, duplicate, and ABI-skewing rebuilds).

Three fixes, one file:

1. ``_run_monitored_build`` reaps a wedged build's WHOLE process group
   (not just the bash wrapper) on a stall/wall timeout, so a stuck rebuild
   stops pinning CPUs and the job can go terminal (releasing its lock).
2. ``build_job.submit_build_env_job`` dedupes a second build for an env
   already building, and backs off after a failed build.
3. ``admin._do_update_work`` makes the {Python tree, native .so} pair
   atomic: a failed build (or a failed post-build import probe) rolls the
   checkout back, so the env is never importable-but-ABI-broken.
"""
from __future__ import annotations

import importlib.machinery
import json
import os
import shlex
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import admin, auto_update, build_job, capacity, config, output, paths, runtime_slots
from vq.cli import main
from vq.spec import JobSpec, JobState

# ======================================================================
# Fix 1 — a wedged build is reaped (process group + wall/stall caps)
# ======================================================================


def _assert_pid_dead(pid: int, *, timeout: float = 5.0) -> None:
    """Poll until ``pid`` no longer exists (signal 0 raises). Proves the
    grandchild was reaped, not orphaned."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return  # gone — reaped
        except PermissionError:  # pragma: no cover — exists, foreign owner
            break
        time.sleep(0.05)
    raise AssertionError(f"process {pid} still alive after {timeout}s (leaked)")


@pytest.mark.no_autopatch_build_runner
class TestBuildRunnerReaping:
    """The real :func:`admin._run_monitored_build` (opt out of the
    conftest stub that delegates to subprocess.run)."""

    def test_stall_reaps_whole_process_group(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Poll fast so the test reaps within ~1s.
        monkeypatch.setattr("vq.admin._BUILD_POLL_INTERVAL_SECONDS", 0.1)
        pidfile = tmp_path / "grandchild.pid"
        script = tmp_path / "wedge.sh"
        # Spawn a long-lived grandchild, record its pid, then go SILENT
        # (no stdout) — exactly the 2026-06-26 "empty stdout for hours"
        # wedge. The stall cap must reap the grandchild too.
        script.write_text(
            "#!/bin/bash\n"
            "sleep 120 &\n"
            f"echo $! > {pidfile}\n"
            "wait\n"
        )
        result = admin._run_monitored_build(
            ["bash", str(script)],
            cwd=str(tmp_path),
            env=dict(os.environ),
            wall_timeout=30.0,
            stall_timeout=1.0,
            heartbeat_interval=0.0,
            log_label="test-wedge",
            emit=lambda *_: None,
        )
        assert result.stalled is True
        assert result.timed_out is False
        grandchild = int(pidfile.read_text().strip())
        _assert_pid_dead(grandchild)

    def test_wall_cap_reaps_a_chatty_runaway(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("vq.admin._BUILD_POLL_INTERVAL_SECONDS", 0.1)
        # Emits constantly (so the STALL cap never fires) but never exits;
        # only the wall cap can stop it.
        script = tmp_path / "loud.sh"
        script.write_text(
            "#!/bin/bash\nwhile true; do echo tick; sleep 0.05; done\n"
        )
        result = admin._run_monitored_build(
            ["bash", str(script)],
            cwd=str(tmp_path),
            env=dict(os.environ),
            wall_timeout=1.0,
            stall_timeout=0.0,  # disabled — isolate the wall cap
            heartbeat_interval=0.0,
            log_label="test-loud",
            emit=lambda *_: None,
        )
        assert result.timed_out is True
        assert result.stalled is False
        assert "tick" in result.output

    def test_clean_build_is_not_reaped(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("vq.admin._BUILD_POLL_INTERVAL_SECONDS", 0.1)
        script = tmp_path / "ok.sh"
        script.write_text("#!/bin/bash\necho building done\nexit 0\n")
        result = admin._run_monitored_build(
            ["bash", str(script)],
            cwd=str(tmp_path),
            env=dict(os.environ),
            wall_timeout=30.0,
            stall_timeout=5.0,
            heartbeat_interval=0.0,
            log_label="test-ok",
            emit=lambda *_: None,
        )
        assert result.rc == 0
        assert result.timed_out is False
        assert result.stalled is False
        assert "building done" in result.output

    def test_build_heartbeat_refreshes_admin_marker(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        monkeypatch.setattr("vq.admin._BUILD_POLL_INTERVAL_SECONDS", 0.02)
        marker = admin.write_admin_update_marker(
            envs=["vibeqc-dev"], host="localhost",
        )
        script = tmp_path / "slow-ok.sh"
        script.write_text("#!/bin/bash\nsleep 0.25\necho done\n")
        result = admin._run_monitored_build(
            ["bash", str(script)],
            cwd=str(tmp_path),
            env=dict(os.environ),
            wall_timeout=5.0,
            stall_timeout=5.0,
            heartbeat_interval=0.05,
            log_label="test-build-heartbeat",
            emit=lambda *_: None,
        )
        assert result.rc == 0
        refreshed = admin.read_admin_update_marker()
        assert refreshed is not None
        assert refreshed.last_heartbeat_at >= marker.last_heartbeat_at
        assert refreshed.last_heartbeat_message is not None
        assert "test-build-heartbeat: still running" in (
            refreshed.last_heartbeat_message
        )

    @pytest.mark.parametrize("timeout", [False, True])
    def test_default_emitter_preserves_cli_json_and_run_log(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, timeout: bool,
    ) -> None:
        """Exercise the real supervisor under the CLI's JSON output policy."""
        state_dir = tmp_path
        (state_dir / "cfg").mkdir()
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(state_dir / "cfg"))
        monkeypatch.setattr(admin, "_BUILD_POLL_INTERVAL_SECONDS", 0.01)
        monkeypatch.setattr(output, "_terminal_enabled", False)
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[programs.vibeqc-dev]\nkind = "venv"\n'
            'python = "/fake/python"\ngit_dir = "/fake/repo"\n'
        )
        transcript = state_dir / "build.log"

        def supervised_update(env, cfg, **kwargs):
            run_log = output.RunLog(transcript)
            try:
                with output.channel(run_log=run_log):
                    run = admin._run_monitored_build(
                        [sys.executable, "-c", "import time; time.sleep(0.3)"],
                        cwd=str(state_dir), env=dict(os.environ),
                        wall_timeout=0.15 if timeout else 5,
                        stall_timeout=0, heartbeat_interval=0.04,
                        log_label="json-build",
                    )
            finally:
                run_log.close()
            assert run.timed_out is timeout
            return admin.UpdateResult(
                env=env, git_dir="/fake/repo", branch="main",
                update_script=None, git_pull_rc=0,
                work_errors=["build timeout"] if timeout else [],
            )

        monkeypatch.setattr(admin, "update_env", supervised_update)
        result = CliRunner().invoke(main, [
            "admin", "update", "vibeqc-dev", "localhost", "--json",
        ])
        assert result.exit_code == (1 if timeout else 0), result.output
        assert json.loads(result.stdout)["success"] is not timeout
        assert "json-build: still running" in result.stderr
        assert "json-build: still running" in transcript.read_text()
        if timeout:
            assert "wall-clock cap" in result.stderr
            assert "wall-clock cap" in transcript.read_text()

    @pytest.mark.parametrize("custom_sink", [False, True])
    def test_build_library_call_keeps_default_silent_and_explicit_sink(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str], custom_sink: bool,
    ) -> None:
        monkeypatch.setattr(admin, "_BUILD_POLL_INTERVAL_SECONDS", 0.01)
        monkeypatch.setattr(output, "_active", None)
        messages: list[str] = []
        run = admin._run_monitored_build(
            [sys.executable, "-c", "import time; time.sleep(0.2)"],
            cwd=str(tmp_path), env=dict(os.environ), wall_timeout=5,
            stall_timeout=0, heartbeat_interval=0.04,
            log_label="library-build", emit=messages.append if custom_sink else None,
        )
        assert run.rc == 0
        captured = capsys.readouterr()
        assert captured.out == captured.err == ""
        assert bool(messages) is custom_sink


class TestBuildRunnerWiring:
    def test_default_stall_cap_allows_long_libint_compile(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("VQ_BUILD_STALL_TIMEOUT", raising=False)
        assert admin.BUILD_STALL_TIMEOUT_SECONDS == 3600
        assert admin._build_stall_timeout() == 3600.0

    @pytest.mark.parametrize(
        ("name", "resolver", "expected"),
        [
            (
                "VQ_BUILD_STALL_TIMEOUT",
                admin._build_stall_timeout,
                admin.BUILD_STALL_TIMEOUT_SECONDS,
            ),
            (
                "VQ_BUILD_HEARTBEAT_INTERVAL",
                admin._build_heartbeat_interval,
                admin.BUILD_HEARTBEAT_INTERVAL_SECONDS,
            ),
        ],
    )
    @pytest.mark.parametrize("value", ["nan", "inf", "-inf"])
    def test_nonfinite_build_timer_override_falls_back(
        self,
        monkeypatch: pytest.MonkeyPatch,
        name: str,
        resolver,
        expected: float,
        value: str,
    ) -> None:
        monkeypatch.setenv(name, value)
        assert resolver() == float(expected)

    @pytest.mark.parametrize(
        ("name", "resolver"),
        [
            ("VQ_BUILD_STALL_TIMEOUT", admin._build_stall_timeout),
            ("VQ_BUILD_HEARTBEAT_INTERVAL", admin._build_heartbeat_interval),
        ],
    )
    def test_zero_still_disables_build_timer(
        self,
        monkeypatch: pytest.MonkeyPatch,
        name: str,
        resolver,
    ) -> None:
        monkeypatch.setenv(name, "0")
        assert resolver() == 0.0

    def test_update_script_timeout_reports_reap(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``_run_update_script`` maps a reaped build onto a loud
        work_error (not a silent hang)."""
        (tmp_path / "scripts").mkdir()
        script = tmp_path / "scripts" / "build.sh"
        script.write_text("#!/bin/bash\nexit 0\n")

        def _stalled(*a, **k):
            return admin._BuildRunResult(rc=None, output="", stalled=True)

        # opt out of the conftest delegating stub for this one call
        monkeypatch.setattr("vq.admin._run_monitored_build", _stalled)
        errs: list[str] = []
        rc, _out, _seconds = admin._run_update_script(
            tmp_path, "scripts/build.sh", work_errors=errs,
        )
        assert rc is None
        assert any("stalled" in e and "reaped" in e for e in errs)

    def test_build_job_wall_time_exceeds_update_cap(self) -> None:
        """The watchdog backstop cap sits ABOVE the update_script's own
        cap, so the loud in-process timeout fires first."""
        assert (
            build_job.default_build_wall_time()
            > admin._update_script_timeout()
        )


# ======================================================================
# Fix 2 — dedup + backoff on build-env submission
# ======================================================================


@pytest.fixture
def state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> config.Config:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "config.toml").write_text('default_host = "localhost"\n')
    return config.load_config()


def _qd_jd(tmp_path: Path) -> tuple[Path, Path]:
    qd = tmp_path / "queue"
    jd = tmp_path / "jobs"
    qd.mkdir()
    jd.mkdir()
    return qd, jd


def _register_update_program(
    state: config.Config,
    tmp_path: Path,
    name: str,
    *,
    policy: str,
) -> config.VenvProgram:
    """Give auto-update tests a real checkout without weakening production."""
    repo = tmp_path / f"{name}-repo"
    repo.mkdir()
    subprocess.run(
        ["git", "-C", str(repo), "init", "--quiet", "-b", "main"],
        check=True,
        capture_output=True,
        text=True,
    )
    program = config.VenvProgram(
        kind="venv",
        python=str(tmp_path / f"{name}-venv" / "bin" / "python"),
        git_dir=str(repo),
        branch="main",
        auto_update_policy=policy,
    )
    state.programs[name] = program
    return program


class TestBuildJobDedup:
    def test_first_submit_writes_capped_build_job(
        self, state: config.Config, tmp_path: Path
    ) -> None:
        qd, jd = _qd_jd(tmp_path)
        out = build_job.submit_build_env_job(
            "vibeqc-dev", state, host="localhost", queue_dir=qd, jobs_dir=jd,
        )
        assert out.action == "submitted"
        specs = list(qd.glob("*.json"))
        assert len(specs) == 1
        spec = JobSpec.read(specs[0])
        assert spec.build_env == "vibeqc-dev"
        assert spec.priority == build_job.BUILD_JOB_PRIORITY
        assert spec.wall_time_seconds and spec.wall_time_seconds > 0
        assert spec.command[-2:] == ["build-env", "vibeqc-dev"]

    def test_second_submit_for_same_env_is_deduped(
        self, state: config.Config, tmp_path: Path
    ) -> None:
        qd, jd = _qd_jd(tmp_path)
        first = build_job.submit_build_env_job(
            "vibeqc-dev", state, host="localhost", queue_dir=qd, jobs_dir=jd,
        )
        assert first.action == "submitted"
        second = build_job.submit_build_env_job(
            "vibeqc-dev", state, host="localhost", queue_dir=qd, jobs_dir=jd,
        )
        assert second.action == "deduped"
        assert second.jobid == first.jobid
        # No second job written.
        assert len(list(qd.glob("*.json"))) == 1

    def test_terminal_build_does_not_block_resubmission(
        self, state: config.Config, tmp_path: Path
    ) -> None:
        qd, jd = _qd_jd(tmp_path)
        first = build_job.submit_build_env_job(
            "vibeqc-dev", state, host="localhost", queue_dir=qd, jobs_dir=jd,
        )
        # Drive the first job to a terminal state — dedup is for live builds.
        p = qd / f"{first.jobid}.json"
        s = JobSpec.read(p)
        s.state = JobState.COMPLETED
        s.write(p)
        second = build_job.submit_build_env_job(
            "vibeqc-dev", state, host="localhost", queue_dir=qd, jobs_dir=jd,
        )
        assert second.action == "submitted"
        assert second.jobid != first.jobid

    def test_different_env_is_not_deduped(
        self, state: config.Config, tmp_path: Path
    ) -> None:
        qd, jd = _qd_jd(tmp_path)
        a = build_job.submit_build_env_job(
            "vibeqc-dev", state, host="localhost", queue_dir=qd, jobs_dir=jd,
        )
        b = build_job.submit_build_env_job(
            "vibeqc-release", state, host="localhost", queue_dir=qd, jobs_dir=jd,
        )
        assert a.action == "submitted"
        assert b.action == "submitted"
        assert len(list(qd.glob("*.json"))) == 2


class TestBuildJobBackoff:
    def test_recent_failure_backs_off_then_clears(
        self, state: config.Config, tmp_path: Path
    ) -> None:
        qd, jd = _qd_jd(tmp_path)
        build_job.record_build_failure("vibeqc-dev")
        out = build_job.submit_build_env_job(
            "vibeqc-dev", state, host="localhost", queue_dir=qd, jobs_dir=jd,
        )
        assert out.action == "backed_off"
        assert list(qd.glob("*.json")) == []  # nothing submitted
        # A success clears the backoff → next submit goes through.
        build_job.clear_build_backoff("vibeqc-dev")
        out2 = build_job.submit_build_env_job(
            "vibeqc-dev", state, host="localhost", queue_dir=qd, jobs_dir=jd,
        )
        assert out2.action == "submitted"

    def test_backoff_window_grows_and_expires(
        self, state: config.Config
    ) -> None:
        t0 = "2026-01-01T00:00:00+00:00"
        # First failure → 15 min (900s) window.
        assert build_job.record_build_failure("vibeqc-dev", now=t0) == 1
        rem, rec = build_job.build_backoff_remaining(
            "vibeqc-dev", now=datetime(2026, 1, 1, 0, 5, tzinfo=UTC),
        )
        assert rec is not None and rec["count"] == 1
        assert 590 < rem < 610  # 900 - 300 elapsed
        # Window elapsed → no backoff.
        rem_done, _ = build_job.build_backoff_remaining(
            "vibeqc-dev", now=datetime(2026, 1, 1, 0, 20, tzinfo=UTC),
        )
        assert rem_done == 0.0
        # Second consecutive failure → window doubles to 30 min (1800s).
        assert build_job.record_build_failure("vibeqc-dev", now=t0) == 2
        rem2, rec2 = build_job.build_backoff_remaining(
            "vibeqc-dev", now=datetime(2026, 1, 1, 0, 5, tzinfo=UTC),
        )
        assert rec2 is not None and rec2["count"] == 2
        assert 1490 < rem2 < 1510  # 1800 - 300

    def test_clear_resets_the_counter(self, state: config.Config) -> None:
        t0 = "2026-01-01T00:00:00+00:00"
        build_job.record_build_failure("vibeqc-dev", now=t0)
        build_job.record_build_failure("vibeqc-dev", now=t0)
        build_job.clear_build_backoff("vibeqc-dev")
        # Back to a clean slate: next failure is count 1 again.
        assert build_job.record_build_failure("vibeqc-dev", now=t0) == 1


class TestAutoUpdateRoutesThroughBuildJob:
    """Fix 2 wiring: branch-mode (dev-HEAD) drift submits a capped build
    JOB instead of running an inline rebuild in the timer process."""

    def test_branch_drift_submits_build_job_not_inline(
        self,
        state: config.Config,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _register_update_program(state, tmp_path, "vibeqc-dev", policy="branch")
        decision = auto_update.AutoUpdateDecision(
            env_name="vibeqc-dev",
            action="update",
            reason="branch drift",
            current_tag=None,
            target_tag=None,
            policy="branch",
            current_sha="a" * 40,
            target_sha="b" * 40,
        )
        monkeypatch.setattr(auto_update, "check_env_drift", lambda env, cfg: decision)
        monkeypatch.setattr(auto_update, "is_local_host", lambda h: True)

        captured: dict[str, object] = {}

        def _fake_submit(env, cfg, *, host, **k):
            captured["env"] = env
            return build_job.BuildSubmitOutcome("submitted", env, jobid="j" * 12)

        monkeypatch.setattr(
            auto_update.build_job, "submit_build_env_job", _fake_submit,
        )
        # update_env must NOT be called on the branch-mode path now.
        def _boom(*a, **k):  # pragma: no cover - asserts non-invocation
            raise AssertionError("inline update_env must not run for branch drift")

        monkeypatch.setattr(auto_update.admin, "update_env", _boom)

        outcome = auto_update.auto_update_env("vibeqc-dev", state, host="localhost")
        assert captured["env"] == "vibeqc-dev"
        assert outcome.update_result is None
        assert outcome.build_submit is not None
        assert outcome.build_submit.action == "submitted"

    def test_branch_drift_refuses_vq_self_target_before_submission(
        self,
        state: config.Config,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _register_update_program(state, tmp_path, "vibeqc-dev", policy="branch")
        decision = auto_update.AutoUpdateDecision(
            env_name="vibeqc-dev",
            action="update",
            reason="branch drift",
            current_tag=None,
            target_tag=None,
            policy="branch",
            current_sha="a" * 40,
            target_sha="b" * 40,
        )
        monkeypatch.setattr(auto_update, "check_env_drift", lambda env, cfg: decision)
        monkeypatch.setattr(auto_update, "is_local_host", lambda host: True)
        monkeypatch.setattr(
            auto_update.admin,
            "_detect_vq_self_update",
            lambda unused: admin._SelfUpdateProbe(
                is_self_update=True,
                daemon_running=True,
                service_manager="systemd",
                manager_available=True,
                diagnostic="test self-update",
            ),
        )
        submitted = False

        def submit(*args, **kwargs):
            nonlocal submitted
            submitted = True
            return build_job.BuildSubmitOutcome("submitted", "vibeqc-dev")

        monkeypatch.setattr(auto_update.build_job, "submit_build_env_job", submit)

        outcome = auto_update.auto_update_env(
            "vibeqc-dev", state, host="localhost",
        )

        assert submitted is False
        assert outcome.build_submit is None
        assert outcome.update_result is None
        assert outcome.decision.action == "error"
        assert "vq self-update --expected-sha" in outcome.decision.reason

    def test_tag_drift_still_runs_inline(
        self,
        state: config.Config,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _register_update_program(state, tmp_path, "vibeqc-release", policy="tag")
        decision = auto_update.AutoUpdateDecision(
            env_name="vibeqc-release",
            action="update",
            reason="tag drift",
            current_tag="v0.8.0",
            target_tag="v0.8.1",
            policy="tag",
        )
        monkeypatch.setattr(auto_update, "check_env_drift", lambda env, cfg: decision)

        seen: dict[str, object] = {}

        class _R:
            success = True
            work_errors: list[str] = []

        def _fake_update(env, cfg, *, host, expected_tag=None, **k):
            seen["env"] = env
            seen["expected_tag"] = expected_tag
            return _R()

        monkeypatch.setattr(auto_update.admin, "update_env", _fake_update)
        # build-job path must NOT fire for tag-mode.
        monkeypatch.setattr(
            auto_update.build_job, "submit_build_env_job",
            lambda *a, **k: (_ for _ in ()).throw(
                AssertionError("tag mode must not submit a build job")
            ),
        )
        outcome = auto_update.auto_update_env(
            "vibeqc-release", state, host="localhost",
        )
        assert seen == {"env": "vibeqc-release", "expected_tag": "v0.8.1"}
        assert outcome.build_submit is None
        assert outcome.update_result is not None

    def test_cli_branch_drift_end_to_end_submits_and_renders(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`vq admin auto-update <dev>` end-to-end: a branch-mode drift
        decision → a real queued build job → clean exit 0 + rendered
        text (catches formatter / wiring regressions)."""
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
        (tmp_path / "cfg").mkdir()
        paths.queue_dir().mkdir(parents=True, exist_ok=True)
        paths.jobs_dir().mkdir(parents=True, exist_ok=True)
        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / ".git").mkdir()
        (tmp_path / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n\n'
            "[programs.vibeqc-dev]\n"
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            f'git_dir = "{repo}"\n'
            'branch = "main"\n'
            'update_script = "scripts/update.sh --dev"\n'
            'auto_update_policy = "branch"\n'
            'import_check = "vibeqc"\n'
        )
        decision = auto_update.AutoUpdateDecision(
            env_name="vibeqc-dev", action="update", reason="branch drift",
            current_tag=None, target_tag=None, policy="branch",
            current_sha="a" * 40, target_sha="b" * 40,
        )
        monkeypatch.setattr(auto_update, "check_env_drift", lambda env, cfg: decision)

        result = CliRunner().invoke(main, ["admin", "auto-update", "vibeqc-dev"])
        assert result.exit_code == 0, result.output
        assert "build job submitted" in result.output
        # A real, capped build job landed in the queue.
        specs = list(paths.queue_dir().glob("*.json"))
        assert len(specs) == 1
        spec = JobSpec.read(specs[0])
        assert spec.build_env == "vibeqc-dev"
        assert spec.wall_time_seconds and spec.wall_time_seconds > 0


# ======================================================================
# Fix 3 — atomic build: a failed rebuild never leaves a skewed venv
# ======================================================================


def _git(cwd: Path, *args: str) -> None:
    subprocess.run(
        [
            "git",
            "-c", "user.name=t",
            "-c", "user.email=t@t",
            "-c", "commit.gpgsign=false",
            "-c", "init.defaultBranch=main",
            *args,
        ],
        cwd=str(cwd), check=True, capture_output=True, text=True,
    )


def _head(repo: Path) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        capture_output=True, text=True, check=True,
    ).stdout.strip()


def _local_git_config(repo: Path, key: str) -> str | None:
    proc = subprocess.run(
        ["git", "-C", str(repo), "config", "--local", "--get", key],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return None
    return proc.stdout.strip() or None


def _make_tracking_repos(
    tmp: Path,
    *,
    build_sh: str,
    initial_files: dict[str, str] | None = None,
    remote_files: dict[str, str] | None = None,
) -> tuple[Path, str, str]:
    """A local origin with two commits and a clone parked one commit
    behind, so ``git pull`` fast-forwards A -> B. ``build.sh`` (the
    update_script) carries ``build_sh`` and is identical in both commits,
    so it survives the pull and the rollback. Returns (work, sha_A, sha_B)."""
    remote = tmp / "remote.git"
    remote.mkdir()
    _git(remote, "init", "--bare", "-b", "main")

    work = tmp / "work"
    _git(tmp, "clone", str(remote), str(work))
    (work / "build.sh").write_text(build_sh)
    (work / "marker.txt").write_text("A\n")
    for relative, content in (initial_files or {}).items():
        target = work / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    _git(work, "add", "-A")
    _git(work, "commit", "-m", "A")
    _git(work, "push", "origin", "main")
    sha_a = _head(work)

    # Advance the remote one commit via a second clone.
    work2 = tmp / "work2"
    _git(tmp, "clone", str(remote), str(work2))
    changes = remote_files or {"marker.txt": "B\n"}
    for relative, content in changes.items():
        target = work2 / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    _git(work2, "add", "-A")
    _git(work2, "commit", "-m", "B")
    _git(work2, "push", "origin", "main")
    sha_b = _head(work2)

    assert sha_a != sha_b
    return work, sha_a, sha_b


def _prog(
    work: Path,
    *,
    update_script: str,
    import_check: str | None,
    import_symbols: list[str] | None = None,
    python: str | None = None,
):
    return config.VenvProgram(
        kind="venv",
        python=python or sys.executable,
        git_dir=str(work),
        branch="main",
        update_script=update_script,
        import_check=import_check,
        import_symbols=import_symbols or [],
    )


def _managed_python(work: Path) -> Path:
    python = work / ".venv" / "bin" / "python"
    python.parent.mkdir(parents=True, exist_ok=True)
    python.write_text(
        "#!/bin/sh\n"
        f"PYTHONPATH={shlex.quote(str(work / 'python'))} "
        f"exec {shlex.quote(sys.executable)} -S \"$@\"\n",
        encoding="utf-8",
    )
    python.chmod(0o755)
    return python


def _seed_so(work: Path, import_check: str, content: str) -> Path:
    sodir = work / "python" / import_check
    sodir.mkdir(parents=True, exist_ok=True)
    so = sodir / "_core.so"
    so.write_text(content)
    return so


class TestAtomicBuildRollback:
    def test_failed_build_rolls_back_tree_and_restores_so(
        self, tmp_path: Path
    ) -> None:
        # update_script corrupts the .so then FAILS (rc=1): the pre-fix
        # outcome was newer-Python (B) against the corrupted/old .so.
        build_sh = (
            "#!/bin/bash\n"
            "echo NEW > python/sys/_core.so\n"
            "exit 1\n"
        )
        work, sha_a, sha_b = _make_tracking_repos(tmp_path, build_sh=build_sh)
        so = _seed_so(work, "sys", "OLD")  # import_check=sys → importable

        result = admin._do_update_work(
            "vibeqc-dev", _prog(work, update_script="build.sh", import_check="sys"),
        )

        assert result.git_pull_rc == 0
        assert result.update_script_rc == 1
        assert result.rolled_back is True
        assert result.success is False
        # Tree reverted to the pre-update commit...
        assert _head(work) == sha_a
        assert (work / "marker.txt").read_text() == "A\n"
        # ...and the snapshotted .so restored, so {tree, .so} are consistent.
        assert so.read_text() == "OLD"
        assert any("rollback" in e.lower() for e in result.work_errors)

    def test_managed_vibeqc_failed_build_rolls_back_without_config_opt_in(
        self, tmp_path: Path
    ) -> None:
        runtime_core = tmp_path / "runtime" / "vibeqc" / "_vibeqc_core.so"
        runtime_core.parent.mkdir(parents=True)
        runtime_core.write_text("OLD", encoding="utf-8")
        package_code = (
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            f"_vibeqc_core.__file__ = {str(runtime_core)!r}\n"
        )
        build_sh = (
            "#!/bin/bash\n"
            f"echo NEW > {shlex.quote(str(runtime_core))}\n"
            "exit 1\n"
        )
        work, sha_a, _sha_b = _make_tracking_repos(
            tmp_path,
            build_sh=build_sh,
            initial_files={
                ".gitignore": ".venv/\n__pycache__/\n",
                "python/vibeqc/__init__.py": package_code,
                "cpp/src/bindings.cpp": "// managed source marker\n",
            },
        )
        bindings = work / "cpp" / "src" / "bindings.cpp"
        os.utime(bindings, ns=(1_000_000_000, 1_000_000_000))
        os.utime(runtime_core, ns=(2_000_000_000, 2_000_000_000))
        python = _managed_python(work)

        result = admin._do_update_work(
            "vibeqc-release",
            _prog(
                work,
                update_script="build.sh",
                import_check=None,
                python=str(python),
            ),
        )

        assert result.update_script_rc == 1
        assert result.rolled_back is True
        assert result.success is False
        assert _head(work) == sha_a
        assert runtime_core.read_text() == "OLD"
        assert "post-rollback import OK" in result.rollback_summary

    @staticmethod
    def _managed_vibeqc_linking_libint(
        tmp_path: Path, build_sh: str,
    ) -> tuple[Path, str, Path, Path, Path]:
        """A managed vibe-qc checkout whose package refuses to import unless
        the vendored ``third_party/libint/install`` it was built against is
        the one on disk -- the dependency a real ``_vibeqc_core`` has on
        ``libint2.so``. Returns (work, sha_A, python, runtime core, libint)."""
        runtime_core = tmp_path / "runtime" / "vibeqc" / "_vibeqc_core.so"
        runtime_core.parent.mkdir(parents=True)
        runtime_core.write_text("OLD", encoding="utf-8")
        package_code = (
            "import pathlib\n"
            "_lib = (pathlib.Path(__file__).resolve().parents[2]\n"
            "        / 'third_party' / 'libint' / 'install' / 'lib'\n"
            "        / 'libint2.so')\n"
            "if not _lib.is_file() or _lib.read_text() != 'OLD':\n"
            "    raise ImportError('libint2.so: cannot open shared object file')\n"
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            f"_vibeqc_core.__file__ = {str(runtime_core)!r}\n"
        )
        work, sha_a, _sha_b = _make_tracking_repos(
            tmp_path,
            build_sh=build_sh,
            initial_files={
                ".gitignore": ".venv/\n__pycache__/\nthird_party/*/install/\n",
                "python/vibeqc/__init__.py": package_code,
                "cpp/src/bindings.cpp": "// managed source marker\n",
            },
        )
        libint = work / "third_party" / "libint" / "install" / "lib" / "libint2.so"
        libint.parent.mkdir(parents=True)
        libint.write_text("OLD", encoding="utf-8")
        os.symlink("libint2.so", libint.parent / "libint2.so.2")
        bindings = work / "cpp" / "src" / "bindings.cpp"
        os.utime(bindings, ns=(1_000_000_000, 1_000_000_000))
        os.utime(runtime_core, ns=(2_000_000_000, 2_000_000_000))
        return work, sha_a, _managed_python(work), runtime_core, libint

    @pytest.mark.parametrize(
        "death",
        ["exit 1", "kill -KILL $$"],
        ids=["build-fails", "build-is-killed"],
    )
    def test_rollback_restores_the_vendored_libraries_the_core_links(
        self, tmp_path: Path, death: str,
    ) -> None:
        """#44: the build wipes ``third_party/*/install`` (as vibe-qc's
        update.sh does before a native-deps rebuild) and dies before the core
        is rebuilt. Restoring the core alone left host_c2, host_e and
        host_d with a lane that did not import."""
        build_sh = (
            "#!/bin/bash\n"
            "rm -rf third_party/libint/install\n"
            "mkdir -p third_party/libint/install/lib\n"
            "printf NEW > third_party/libint/install/lib/libint2.so\n"
            f"{death}\n"
        )
        work, sha_a, python, runtime_core, libint = (
            self._managed_vibeqc_linking_libint(tmp_path, build_sh)
        )

        result = admin._do_update_work(
            "vibeqc-release",
            _prog(
                work,
                update_script="build.sh",
                import_check=None,
                python=str(python),
            ),
        )

        assert result.update_script_rc != 0
        assert _head(work) == sha_a
        assert runtime_core.read_text() == "OLD"
        assert libint.read_text() == "OLD"
        assert os.readlink(libint.parent / "libint2.so.2") == "libint2.so"
        assert result.rolled_back is True, result.rollback_summary
        assert "restored 1 native runtime tree(s)" in result.rollback_summary
        assert "post-rollback import OK" in result.rollback_summary
        assert not list(libint.parents[2].glob(".vq-restore-*"))

    def test_rollback_leaves_an_untouched_vendored_tree_alone(
        self, tmp_path: Path,
    ) -> None:
        work, sha_a, python, _runtime_core, libint = (
            self._managed_vibeqc_linking_libint(
                tmp_path, "#!/bin/bash\nexit 1\n",
            )
        )
        inode = libint.stat().st_ino

        result = admin._do_update_work(
            "vibeqc-release",
            _prog(
                work,
                update_script="build.sh",
                import_check=None,
                python=str(python),
            ),
        )

        assert result.rolled_back is True, result.rollback_summary
        assert _head(work) == sha_a
        assert libint.stat().st_ino == inode
        assert "native runtime tree" not in result.rollback_summary

    def test_empty_post_rollback_probe_failure_marks_rollback_failed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        runtime_core = tmp_path / "runtime" / "vibeqc" / "_vibeqc_core.so"
        runtime_core.parent.mkdir(parents=True)
        runtime_core.write_text("OLD", encoding="utf-8")
        package_code = (
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            f"_vibeqc_core.__file__ = {str(runtime_core)!r}\n"
        )
        work, sha_a, _sha_b = _make_tracking_repos(
            tmp_path,
            build_sh="#!/bin/bash\nexit 1\n",
            initial_files={
                ".gitignore": ".venv/\n__pycache__/\n",
                "python/vibeqc/__init__.py": package_code,
                "cpp/src/bindings.cpp": "// managed source marker\n",
            },
        )
        bindings = work / "cpp" / "src" / "bindings.cpp"
        os.utime(bindings, ns=(1_000_000_000, 1_000_000_000))
        os.utime(runtime_core, ns=(2_000_000_000, 2_000_000_000))
        python = _managed_python(work)
        monkeypatch.setattr(
            admin,
            "_run_import_check",
            lambda *args, **kwargs: (1, ""),
        )

        result = admin._do_update_work(
            "vibeqc-release",
            _prog(
                work,
                update_script="build.sh",
                import_check=None,
                python=str(python),
            ),
        )

        assert result.update_script_rc == 1
        assert result.rolled_back is False
        assert result.success is False
        assert _head(work) == sha_a
        assert runtime_core.read_text() == "OLD"
        assert "post-rollback import STILL FAILING rc=1" in (
            result.rollback_summary
        )
        assert any("rollback FAILED" in error for error in result.work_errors)

    def test_rollback_removes_new_source_tree_core_candidate(
        self, tmp_path: Path
    ) -> None:
        runtime_core = tmp_path / "runtime" / "vibeqc" / "_vibeqc_core.so"
        runtime_core.parent.mkdir(parents=True)
        runtime_core.write_text("OLD", encoding="utf-8")
        package_core = Path("python/vibeqc/_vibeqc_core.so")
        package_code = (
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            f"_vibeqc_core.__file__ = {str(runtime_core)!r}\n"
        )
        work, sha_a, _sha_b = _make_tracking_repos(
            tmp_path,
            build_sh=(
                "#!/bin/bash\n"
                f"echo TRANSACTION > {package_core}\n"
                "exit 1\n"
            ),
            initial_files={
                ".gitignore": ".venv/\n__pycache__/\n",
                "python/vibeqc/__init__.py": package_code,
                "cpp/src/bindings.cpp": "// source A\n",
            },
        )
        python = _managed_python(work)
        source = work / "cpp" / "src" / "bindings.cpp"
        os.utime(source, ns=(1_000_000_000, 1_000_000_000))
        os.utime(runtime_core, ns=(2_000_000_000, 2_000_000_000))

        result = admin._do_update_work(
            "vibeqc-release",
            _prog(
                work,
                update_script="build.sh",
                import_check=None,
                python=str(python),
            ),
        )

        assert result.rolled_back is True
        assert _head(work) == sha_a
        assert not (work / package_core).exists()
        assert runtime_core.read_text(encoding="utf-8") == "OLD"
        assert "removed 1 transaction-created core" in result.rollback_summary

    def test_rollback_removes_higher_priority_site_packages_core(
        self, tmp_path: Path
    ) -> None:
        specific_suffix = next(
            (
                suffix
                for suffix in importlib.machinery.EXTENSION_SUFFIXES
                if suffix != ".so"
            ),
            None,
        )
        if specific_suffix is None:
            pytest.skip("platform exposes no ABI-specific extension suffix")
        runtime_root = tmp_path / "runtime" / "vibeqc"
        runtime_root.mkdir(parents=True)
        runtime_core = runtime_root / "_vibeqc_core.so"
        runtime_core.write_text("OLD", encoding="utf-8")
        higher_priority = runtime_root / f"_vibeqc_core{specific_suffix}"
        package_code = (
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            f"_vibeqc_core.__file__ = {str(runtime_core)!r}\n"
        )
        work, sha_a, _sha_b = _make_tracking_repos(
            tmp_path,
            build_sh=(
                "#!/bin/bash\n"
                f"echo TRANSACTION > {shlex.quote(str(higher_priority))}\n"
                "exit 1\n"
            ),
            initial_files={
                ".gitignore": ".venv/\n__pycache__/\n",
                "python/vibeqc/__init__.py": package_code,
                "cpp/src/bindings.cpp": "// source A\n",
            },
        )
        python = _managed_python(work)
        source = work / "cpp" / "src" / "bindings.cpp"
        os.utime(source, ns=(1_000_000_000, 1_000_000_000))
        os.utime(runtime_core, ns=(2_000_000_000, 2_000_000_000))

        result = admin._do_update_work(
            "vibeqc-release",
            _prog(
                work,
                update_script="build.sh",
                import_check=None,
                python=str(python),
            ),
        )

        assert result.rolled_back is True
        assert _head(work) == sha_a
        assert not higher_priority.exists()
        assert runtime_core.read_text(encoding="utf-8") == "OLD"

    def test_rollback_restores_all_preexisting_site_packages_candidates(
        self, tmp_path: Path
    ) -> None:
        specific_suffix = next(
            (
                suffix
                for suffix in importlib.machinery.EXTENSION_SUFFIXES
                if suffix != ".so"
            ),
            None,
        )
        if specific_suffix is None:
            pytest.skip("platform exposes no ABI-specific extension suffix")
        runtime_root = tmp_path / "runtime" / "vibeqc"
        runtime_root.mkdir(parents=True)
        serving_core = runtime_root / f"_vibeqc_core{specific_suffix}"
        fallback_core = runtime_root / "_vibeqc_core.so"
        serving_core.write_text("OLD-SERVING", encoding="utf-8")
        fallback_core.write_text("OLD-FALLBACK", encoding="utf-8")
        package_code = (
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            f"_vibeqc_core.__file__ = {str(serving_core)!r}\n"
        )
        work, sha_a, _sha_b = _make_tracking_repos(
            tmp_path,
            build_sh=(
                "#!/bin/bash\n"
                f"echo BROKEN > {shlex.quote(str(serving_core))}\n"
                f"echo BROKEN > {shlex.quote(str(fallback_core))}\n"
                "exit 1\n"
            ),
            initial_files={
                ".gitignore": ".venv/\n__pycache__/\n",
                "python/vibeqc/__init__.py": package_code,
                "cpp/src/bindings.cpp": "// source A\n",
            },
        )
        python = _managed_python(work)
        source = work / "cpp" / "src" / "bindings.cpp"
        os.utime(source, ns=(1_000_000_000, 1_000_000_000))
        os.utime(serving_core, ns=(2_000_000_000, 2_000_000_000))

        result = admin._do_update_work(
            "vibeqc-release",
            _prog(
                work,
                update_script="build.sh",
                import_check=None,
                python=str(python),
            ),
        )

        assert result.rolled_back is True
        assert _head(work) == sha_a
        assert serving_core.read_text(encoding="utf-8") == "OLD-SERVING"
        assert fallback_core.read_text(encoding="utf-8") == "OLD-FALLBACK"

    def test_rollback_restores_loader_symlink_identity(
        self, tmp_path: Path
    ) -> None:
        runtime_root = tmp_path / "runtime" / "vibeqc"
        runtime_root.mkdir(parents=True)
        target = runtime_root / "core-target.bin"
        target.write_text("OLD", encoding="utf-8")
        runtime_core = runtime_root / "_vibeqc_core.so"
        runtime_core.symlink_to(target.name)
        package_code = (
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            f"_vibeqc_core.__file__ = {str(runtime_core)!r}\n"
        )
        work, sha_a, _sha_b = _make_tracking_repos(
            tmp_path,
            build_sh=(
                "#!/bin/bash\n"
                f"rm {shlex.quote(str(runtime_core))}\n"
                f"echo TRANSACTION > {shlex.quote(str(runtime_core))}\n"
                "exit 1\n"
            ),
            initial_files={
                ".gitignore": ".venv/\n__pycache__/\n",
                "python/vibeqc/__init__.py": package_code,
                "cpp/src/bindings.cpp": "// source A\n",
            },
        )
        python = _managed_python(work)
        source = work / "cpp" / "src" / "bindings.cpp"
        os.utime(source, ns=(1_000_000_000, 1_000_000_000))
        os.utime(target, ns=(2_000_000_000, 2_000_000_000))

        result = admin._do_update_work(
            "vibeqc-release",
            _prog(
                work,
                update_script="build.sh",
                import_check=None,
                python=str(python),
            ),
        )

        assert result.rolled_back is True
        assert _head(work) == sha_a
        assert runtime_core.is_symlink()
        assert os.readlink(runtime_core) == target.name
        assert target.read_text(encoding="utf-8") == "OLD"

    def test_managed_vibeqc_contract_survives_marker_removal(
        self, tmp_path: Path
    ) -> None:
        runtime_core = tmp_path / "runtime" / "vibeqc" / "_vibeqc_core.so"
        runtime_core.parent.mkdir(parents=True)
        runtime_core.write_text("OLD", encoding="utf-8")
        package_code = (
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            f"_vibeqc_core.__file__ = {str(runtime_core)!r}\n"
        )
        work, sha_a, _sha_b = _make_tracking_repos(
            tmp_path,
            build_sh=(
                "#!/bin/bash\n"
                "rm python/vibeqc/__init__.py\n"
                "exit 1\n"
            ),
            initial_files={
                "python/vibeqc/__init__.py": package_code,
                "cpp/src/bindings.cpp": "// managed source marker\n",
            },
        )
        source = work / "cpp" / "src" / "bindings.cpp"
        os.utime(source, ns=(1_000_000_000, 1_000_000_000))
        os.utime(runtime_core, ns=(2_000_000_000, 2_000_000_000))
        python = _managed_python(work)
        program = config.VenvProgram(
            kind="venv",
            python=str(python),
            git_dir=str(work),
            branch="main",
            update_script="build.sh",
        )

        result = admin._do_update_work("vibeqc-release", program)

        assert result.update_script_rc == 1
        assert result.rolled_back is True
        assert result.success is False
        assert _head(work) == sha_a
        assert (work / "python" / "vibeqc" / "__init__.py").is_file()
        assert "post-rollback import OK" in result.rollback_summary

    def test_managed_vibeqc_rollback_restores_freshness_evidence(
        self, tmp_path: Path
    ) -> None:
        runtime_core = tmp_path / "runtime" / "vibeqc" / "_vibeqc_core.so"
        runtime_core.parent.mkdir(parents=True)
        runtime_core.write_text("OLD", encoding="utf-8")
        package_code = (
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            f"_vibeqc_core.__file__ = {str(runtime_core)!r}\n"
        )
        work, sha_a, _sha_b = _make_tracking_repos(
            tmp_path,
            build_sh="#!/bin/bash\nexit 1\n",
            initial_files={
                "python/vibeqc/__init__.py": package_code,
                "cpp/src/bindings.cpp": "// source A\n",
            },
            remote_files={"cpp/src/bindings.cpp": "// source B\n"},
        )
        source = work / "cpp" / "src" / "bindings.cpp"
        os.utime(source, ns=(1_000_000_000, 1_000_000_000))
        os.utime(runtime_core, ns=(2_000_000_000, 2_000_000_000))
        python = _managed_python(work)
        program = config.VenvProgram(
            kind="venv",
            python=str(python),
            git_dir=str(work),
            branch="main",
            update_script="build.sh",
        )

        result = admin._do_update_work("vibeqc-release", program)

        assert result.update_script_rc == 1
        assert result.rolled_back is True
        assert result.success is False
        assert _head(work) == sha_a
        assert source.read_text(encoding="utf-8") == "// source A\n"
        assert source.stat().st_mtime_ns == 1_000_000_000
        assert runtime_core.read_text(encoding="utf-8") == "OLD"
        assert "post-rollback import OK" in result.rollback_summary

    @pytest.mark.no_autopatch_branch_check
    @pytest.mark.parametrize("immutable", [False, True], ids=["atomic", "immutable"])
    def test_source_mtime_restore_failure_marks_rollback_failed(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        immutable: bool,
    ) -> None:
        runtime_core = tmp_path / "runtime" / "vibeqc" / "_vibeqc_core.so"
        runtime_core.parent.mkdir(parents=True)
        runtime_core.write_text("OLD", encoding="utf-8")
        package_code = (
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            f"_vibeqc_core.__file__ = {str(runtime_core)!r}\n"
        )
        work, sha_a, _sha_b = _make_tracking_repos(
            tmp_path,
            build_sh=(
                "#!/bin/bash\n"
                f"echo NEW > {shlex.quote(str(runtime_core))}\n"
                "exit 1\n"
            ),
            initial_files={
                ".gitignore": ".venv/\n__pycache__/\n",
                "python/vibeqc/__init__.py": package_code,
                "cpp/src/bindings.cpp": "// source A\n",
            },
        )
        python = _managed_python(work)
        source = work / "cpp" / "src" / "bindings.cpp"
        os.utime(source, ns=(1_000_000_000, 1_000_000_000))
        os.utime(runtime_core, ns=(2_000_000_000, 2_000_000_000))
        real_utime = os.utime

        def fail_source_utime(
            path: str | os.PathLike[str],
            *args: object,
            **kwargs: object,
        ) -> None:
            if Path(path).is_relative_to(work / "cpp"):
                raise OSError("synthetic mtime restore failure")
            real_utime(path, *args, **kwargs)

        monkeypatch.setattr(admin.os, "utime", fail_source_utime)

        result = admin._do_update_work(
            "vibeqc-release",
            _prog(
                work,
                update_script="build.sh",
                import_check=None,
                python=str(python),
            ),
            expected_sha=sha_a if immutable else None,
        )

        assert result.success is False
        assert result.rolled_back is False
        assert "could not restore native-source mtimes" in result.rollback_summary
        assert any("rollback FAILED" in error for error in result.work_errors)

    def test_changed_distribution_version_cannot_pass_rollback_identity(
        self, tmp_path: Path
    ) -> None:
        runtime_core = tmp_path / "runtime" / "vibeqc" / "_vibeqc_core.so"
        runtime_core.parent.mkdir(parents=True)
        runtime_core.write_text("OLD", encoding="utf-8")
        site_packages = tmp_path / "runtime" / "site-packages"
        dist_info = site_packages / "vibe_qc-1.0.dist-info"
        dist_info.mkdir(parents=True)
        metadata = dist_info / "METADATA"
        metadata.write_text(
            "Metadata-Version: 2.1\nName: vibe-qc\nVersion: 1.0\n",
            encoding="utf-8",
        )
        package_code = (
            "from importlib.metadata import version\n"
            "__version__ = version('vibe-qc')\n"
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            f"_vibeqc_core.__file__ = {str(runtime_core)!r}\n"
        )
        work, sha_a, _sha_b = _make_tracking_repos(
            tmp_path,
            build_sh=(
                "#!/bin/bash\n"
                f"echo NEW > {shlex.quote(str(runtime_core))}\n"
                "cat > " + shlex.quote(str(metadata)) + " <<'EOF'\n"
                "Metadata-Version: 2.1\nName: vibe-qc\nVersion: 2.0\n"
                "EOF\n"
                "exit 1\n"
            ),
            initial_files={
                ".gitignore": ".venv/\n__pycache__/\n",
                "python/vibeqc/__init__.py": package_code,
                "cpp/src/bindings.cpp": "// source A\n",
            },
        )
        python = _managed_python(work)
        python.write_text(
            "#!/bin/sh\n"
            "PYTHONPATH="
            f"{shlex.quote(str(work / 'python'))}:{shlex.quote(str(site_packages))} "
            f"exec {shlex.quote(sys.executable)} -S \"$@\"\n",
            encoding="utf-8",
        )
        python.chmod(0o755)
        source = work / "cpp" / "src" / "bindings.cpp"
        os.utime(source, ns=(1_000_000_000, 1_000_000_000))
        os.utime(runtime_core, ns=(2_000_000_000, 2_000_000_000))

        result = admin._do_update_work(
            "vibeqc-release",
            _prog(
                work,
                update_script="build.sh",
                import_check=None,
                python=str(python),
            ),
        )

        assert result.success is False
        assert result.rolled_back is False
        assert _head(work) == sha_a
        assert runtime_core.read_text(encoding="utf-8") == "OLD"
        assert "runtime version mismatch" in result.rollback_summary
        assert "Version: 2.0" in metadata.read_text(encoding="utf-8")
        assert any("rollback FAILED" in error for error in result.work_errors)

    @pytest.mark.no_autopatch_branch_check
    def test_late_immutable_failure_rechecks_restored_runtime_identity(
        self, tmp_path: Path
    ) -> None:
        runtime_core = tmp_path / "runtime" / "vibeqc" / "_vibeqc_core.so"
        runtime_core.parent.mkdir(parents=True)
        runtime_core.write_text("OLD", encoding="utf-8")
        site_packages = tmp_path / "runtime" / "site-packages"
        dist_info = site_packages / "vibe_qc-1.0.dist-info"
        dist_info.mkdir(parents=True)
        metadata = dist_info / "METADATA"
        metadata.write_text(
            "Metadata-Version: 2.1\nName: vibe-qc\nVersion: 1.0\n",
            encoding="utf-8",
        )
        package_code = (
            "from importlib.metadata import version\n"
            "__version__ = version('vibe-qc')\n"
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            f"_vibeqc_core.__file__ = {str(runtime_core)!r}\n"
        )
        work, sha_a, sha_b = _make_tracking_repos(
            tmp_path,
            build_sh=(
                "#!/bin/bash\n"
                f"echo NEW > {shlex.quote(str(runtime_core))}\n"
                "cat > " + shlex.quote(str(metadata)) + " <<'EOF'\n"
                "Metadata-Version: 2.1\nName: vibe-qc\nVersion: 2.0\n"
                "EOF\n"
                "exit 0\n"
            ),
            initial_files={
                ".gitignore": ".venv/\n__pycache__/\n",
                "python/vibeqc/__init__.py": package_code,
                "cpp/src/bindings.cpp": "// source A\n",
                "post.sh": "#!/bin/bash\nexit 1\n",
            },
        )
        python = _managed_python(work)
        python.write_text(
            "#!/bin/sh\n"
            "PYTHONPATH="
            f"{shlex.quote(str(work / 'python'))}:{shlex.quote(str(site_packages))} "
            f"exec {shlex.quote(sys.executable)} -S \"$@\"\n",
            encoding="utf-8",
        )
        python.chmod(0o755)
        source = work / "cpp" / "src" / "bindings.cpp"
        os.utime(source, ns=(1_000_000_000, 1_000_000_000))
        os.utime(runtime_core, ns=(2_000_000_000, 2_000_000_000))
        program = config.VenvProgram(
            kind="venv",
            python=str(python),
            git_dir=str(work),
            branch="main",
            update_script="build.sh",
            post_update_script="post.sh",
        )

        result = admin._do_update_work(
            "vibeqc-release",
            program,
            expected_sha=sha_b,
        )

        assert result.update_script_rc == 0
        assert result.import_check_rc == 0
        assert result.post_update_script_rc == 1
        assert result.success is False
        assert result.rolled_back is False
        assert _head(work) == sha_a
        assert runtime_core.read_text(encoding="utf-8") == "OLD"
        assert "runtime version mismatch" in result.rollback_summary
        assert "Version: 2.0" in metadata.read_text(encoding="utf-8")
        assert any("rollback FAILED" in error for error in result.work_errors)

    @pytest.mark.no_autopatch_branch_check
    def test_detached_managed_vibeqc_captures_serving_freshness_before_reattach(
        self, tmp_path: Path
    ) -> None:
        runtime_core = tmp_path / "runtime" / "vibeqc" / "_vibeqc_core.so"
        runtime_core.parent.mkdir(parents=True)
        runtime_core.write_text("OLD", encoding="utf-8")
        package_code = (
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            f"_vibeqc_core.__file__ = {str(runtime_core)!r}\n"
        )
        work, sha_a, sha_b = _make_tracking_repos(
            tmp_path,
            build_sh="#!/bin/bash\nexit 1\n",
            initial_files={
                ".gitignore": ".venv/\n__pycache__/\n",
                "python/vibeqc/__init__.py": package_code,
                "cpp/src/bindings.cpp": "// source A\n",
            },
            remote_files={"cpp/src/bindings.cpp": "// source B\n"},
        )
        python = _managed_python(work)

        # The managed env serves detached A while its local main branch points
        # at B. Reattachment rewrites the source tree before the build fails.
        _git(work, "fetch", "origin")
        _git(work, "checkout", "--detach", sha_a)
        _git(work, "branch", "-f", "main", sha_b)
        assert admin._capture_checkout_state(work) == (sha_a, None)
        assert admin._capture_local_branch_tip(work, "main")[:2] == (0, sha_b)
        source = work / "cpp" / "src" / "bindings.cpp"
        os.utime(source, ns=(1_000_000_000, 1_000_000_000))
        os.utime(runtime_core, ns=(2_000_000_000, 2_000_000_000))

        result = admin._do_update_work(
            "vibeqc-release",
            _prog(
                work,
                update_script="build.sh",
                import_check=None,
                python=str(python),
            ),
        )

        assert result.update_script_rc == 1
        assert result.rolled_back is True
        assert result.success is False
        assert result.pre_update_sha == sha_a
        assert admin._capture_checkout_state(work) == (sha_a, None)
        assert admin._capture_local_branch_tip(work, "main")[:2] == (0, sha_b)
        assert source.read_text(encoding="utf-8") == "// source A\n"
        assert source.stat().st_mtime_ns == 1_000_000_000
        assert runtime_core.read_text(encoding="utf-8") == "OLD"
        assert "post-rollback import OK" in result.rollback_summary

    @pytest.mark.no_autopatch_branch_check
    def test_managed_vibeqc_refuses_dirty_attached_checkout_before_mutation(
        self, tmp_path: Path
    ) -> None:
        runtime_core = tmp_path / "runtime" / "vibeqc" / "_vibeqc_core.so"
        runtime_core.parent.mkdir(parents=True)
        runtime_core.write_text("OLD", encoding="utf-8")
        package_code = (
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            f"_vibeqc_core.__file__ = {str(runtime_core)!r}\n"
        )
        work, sha_a, _sha_b = _make_tracking_repos(
            tmp_path,
            build_sh=(
                "#!/bin/bash\n"
                f"echo NEW > {shlex.quote(str(runtime_core))}\n"
                "exit 0\n"
            ),
            initial_files={
                ".gitignore": ".venv/\n__pycache__/\n",
                "python/vibeqc/__init__.py": package_code,
                "cpp/src/bindings.cpp": "// source A\n",
                "operator.txt": "committed\n",
            },
        )
        _managed_python(work)
        (work / "operator.txt").write_text("operator edit\n", encoding="utf-8")

        result = admin._do_update_work(
            "vibeqc-dev",
            _prog(
                work,
                update_script="build.sh",
                import_check=None,
                python=str(work / ".venv" / "bin" / "python"),
            ),
        )

        assert result.git_pull_rc is None
        assert result.update_script_rc is None
        assert result.rolled_back is False
        assert result.success is False
        assert _head(work) == sha_a
        assert (work / "marker.txt").read_text(encoding="utf-8") == "A\n"
        assert (work / "operator.txt").read_text() == "operator edit\n"
        assert runtime_core.read_text(encoding="utf-8") == "OLD"
        assert any("working tree is dirty" in error for error in result.work_errors)

    @pytest.mark.no_autopatch_branch_check
    def test_attached_managed_vibeqc_wrong_branch_gate_rolls_back_pull(
        self, tmp_path: Path
    ) -> None:
        runtime_core = tmp_path / "runtime" / "vibeqc" / "_vibeqc_core.so"
        runtime_core.parent.mkdir(parents=True)
        runtime_core.write_text("OLD", encoding="utf-8")
        package_code = (
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            f"_vibeqc_core.__file__ = {str(runtime_core)!r}\n"
        )
        work, sha_a, _sha_b = _make_tracking_repos(
            tmp_path,
            build_sh=(
                "#!/bin/bash\n"
                f"echo NEW > {shlex.quote(str(runtime_core))}\n"
                "exit 0\n"
            ),
            initial_files={
                ".gitignore": ".venv/\n__pycache__/\n",
                "python/vibeqc/__init__.py": package_code,
                "cpp/src/bindings.cpp": "// source A\n",
            },
            remote_files={"cpp/src/bindings.cpp": "// source B\n"},
        )
        python = _managed_python(work)
        _git(work, "checkout", "-b", "wrong")
        _git(work, "branch", "--set-upstream-to", "origin/main", "wrong")
        source = work / "cpp" / "src" / "bindings.cpp"
        os.utime(source, ns=(1_000_000_000, 1_000_000_000))
        os.utime(runtime_core, ns=(2_000_000_000, 2_000_000_000))

        result = admin._do_update_work(
            "vibeqc-release",
            _prog(
                work,
                update_script="build.sh",
                import_check=None,
                python=str(python),
            ),
        )

        assert result.git_pull_rc == 0
        assert result.actual_branch == "wrong"
        assert result.branch_matches is False
        assert result.update_script_rc is None
        assert result.rolled_back is True
        assert result.success is False
        assert admin._capture_checkout_state(work) == (sha_a, "wrong")
        assert source.read_text(encoding="utf-8") == "// source A\n"
        assert source.stat().st_mtime_ns == 1_000_000_000
        assert runtime_core.read_text(encoding="utf-8") == "OLD"
        assert "post-rollback import OK" in result.rollback_summary

    @pytest.mark.no_autopatch_branch_check
    def test_detached_managed_vibeqc_pull_failure_restores_exact_state(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        runtime_core = tmp_path / "runtime" / "vibeqc" / "_vibeqc_core.so"
        runtime_core.parent.mkdir(parents=True)
        runtime_core.write_text("OLD", encoding="utf-8")
        package_code = (
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            f"_vibeqc_core.__file__ = {str(runtime_core)!r}\n"
        )
        work, sha_a, sha_b = _make_tracking_repos(
            tmp_path,
            build_sh="#!/bin/bash\nexit 0\n",
            initial_files={
                ".gitignore": ".venv/\n__pycache__/\n",
                "python/vibeqc/__init__.py": package_code,
                "cpp/src/bindings.cpp": "// source A\n",
            },
            remote_files={"cpp/src/bindings.cpp": "// source B\n"},
        )
        python = _managed_python(work)
        _git(work, "fetch", "origin")
        _git(work, "checkout", "--detach", sha_a)
        _git(work, "branch", "-f", "main", sha_b)
        _git(work, "branch", "--unset-upstream", "main")
        source = work / "cpp" / "src" / "bindings.cpp"
        os.utime(source, ns=(1_000_000_000, 1_000_000_000))
        os.utime(runtime_core, ns=(2_000_000_000, 2_000_000_000))
        monkeypatch.setattr(
            admin,
            "_run_git_pull",
            lambda *args, **kwargs: (1, "network down\n"),
        )

        result = admin._do_update_work(
            "vibeqc-release",
            _prog(
                work,
                update_script="build.sh",
                import_check=None,
                python=str(python),
            ),
        )

        assert result.git_pull_rc == 1
        assert result.update_script_rc is None
        assert result.rolled_back is True
        assert result.success is False
        assert admin._capture_checkout_state(work) == (sha_a, None)
        assert admin._capture_local_branch_tip(work, "main")[:2] == (0, sha_b)
        assert _local_git_config(work, "branch.main.remote") is None
        assert _local_git_config(work, "branch.main.merge") is None
        assert source.stat().st_mtime_ns == 1_000_000_000
        assert runtime_core.read_text(encoding="utf-8") == "OLD"
        assert "post-rollback import OK" in result.rollback_summary

    @pytest.mark.no_autopatch_branch_check
    def test_failed_post_checkout_hook_rolls_back_observed_branch_switch(
        self, tmp_path: Path
    ) -> None:
        runtime_core = tmp_path / "runtime" / "vibeqc" / "_vibeqc_core.so"
        runtime_core.parent.mkdir(parents=True)
        runtime_core.write_text("OLD", encoding="utf-8")
        package_code = (
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            f"_vibeqc_core.__file__ = {str(runtime_core)!r}\n"
        )
        work, sha_a, _sha_b = _make_tracking_repos(
            tmp_path,
            build_sh="#!/bin/bash\nexit 0\n",
            initial_files={
                ".gitignore": ".venv/\n__pycache__/\n",
                "python/vibeqc/__init__.py": package_code,
                "cpp/src/bindings.cpp": "// source A\n",
            },
        )
        python = _managed_python(work)
        _git(work, "checkout", "--detach", sha_a)
        hook = work / ".git" / "hooks" / "post-checkout"
        hook.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
        hook.chmod(0o755)
        source = work / "cpp" / "src" / "bindings.cpp"
        os.utime(source, ns=(1_000_000_000, 1_000_000_000))
        os.utime(runtime_core, ns=(2_000_000_000, 2_000_000_000))

        result = admin._do_update_work(
            "vibeqc-release",
            _prog(
                work,
                update_script="build.sh",
                import_check=None,
                python=str(python),
            ),
        )

        assert result.git_pull_rc is None
        assert result.update_script_rc is None
        assert result.rolled_back is True
        assert result.success is False
        assert admin._capture_checkout_state(work) == (sha_a, None)
        assert admin._capture_local_branch_tip(work, "main")[:2] == (0, sha_a)
        assert runtime_core.read_text(encoding="utf-8") == "OLD"
        assert "post-rollback import OK" in result.rollback_summary
        assert any(
            "could not check out configured branch" in error
            for error in result.work_errors
        )

    @pytest.mark.no_autopatch_branch_check
    def test_attached_managed_vibeqc_partial_pull_failure_rolls_back(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        runtime_core = tmp_path / "runtime" / "vibeqc" / "_vibeqc_core.so"
        runtime_core.parent.mkdir(parents=True)
        runtime_core.write_text("OLD", encoding="utf-8")
        package_code = (
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            f"_vibeqc_core.__file__ = {str(runtime_core)!r}\n"
        )
        work, sha_a, sha_b = _make_tracking_repos(
            tmp_path,
            build_sh="#!/bin/bash\nexit 0\n",
            initial_files={
                ".gitignore": ".venv/\n__pycache__/\n",
                "python/vibeqc/__init__.py": package_code,
                "cpp/src/bindings.cpp": "// source A\n",
            },
            remote_files={"cpp/src/bindings.cpp": "// source B\n"},
        )
        python = _managed_python(work)
        _git(work, "fetch", "origin")
        source = work / "cpp" / "src" / "bindings.cpp"
        os.utime(source, ns=(1_000_000_000, 1_000_000_000))
        os.utime(runtime_core, ns=(2_000_000_000, 2_000_000_000))

        def partial_pull(*args, **kwargs):
            _git(work, "reset", "--hard", sha_b)
            return 1, "pull failed after moving the tree\n"

        monkeypatch.setattr(admin, "_run_git_pull", partial_pull)

        result = admin._do_update_work(
            "vibeqc-release",
            _prog(
                work,
                update_script="build.sh",
                import_check=None,
                python=str(python),
            ),
        )

        assert result.git_pull_rc == 1
        assert result.update_script_rc is None
        assert result.rolled_back is True
        assert result.success is False
        assert admin._capture_checkout_state(work) == (sha_a, "main")
        assert source.read_text(encoding="utf-8") == "// source A\n"
        assert source.stat().st_mtime_ns == 1_000_000_000
        assert runtime_core.read_text(encoding="utf-8") == "OLD"
        assert "post-rollback import OK" in result.rollback_summary

    @pytest.mark.no_autopatch_branch_check
    @pytest.mark.parametrize(
        "pull_rebase", [False, True], ids=["merge", "rebase"]
    )
    def test_detached_managed_vibeqc_pull_conflict_clears_unmerged_index(
        self, tmp_path: Path, pull_rebase: bool
    ) -> None:
        runtime_core = tmp_path / "runtime" / "vibeqc" / "_vibeqc_core.so"
        runtime_core.parent.mkdir(parents=True)
        runtime_core.write_text("OLD", encoding="utf-8")
        package_code = (
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            f"_vibeqc_core.__file__ = {str(runtime_core)!r}\n"
        )
        work, _sha_a, _sha_b = _make_tracking_repos(
            tmp_path,
            build_sh="#!/bin/bash\nexit 0\n",
            initial_files={
                ".gitignore": ".venv/\n__pycache__/\n",
                "python/vibeqc/__init__.py": package_code,
                "cpp/src/bindings.cpp": "// source A\n",
                "conflict.txt": "base\n",
            },
            remote_files={"conflict.txt": "remote\n"},
        )
        python = _managed_python(work)
        (work / "conflict.txt").write_text("local\n", encoding="utf-8")
        _git(work, "add", "conflict.txt")
        _git(work, "commit", "-m", "local divergent commit")
        sha_c = _head(work)
        _git(work, "config", "pull.rebase", str(pull_rebase).lower())
        _git(work, "checkout", "--detach", sha_c)
        source = work / "cpp" / "src" / "bindings.cpp"
        os.utime(source, ns=(1_000_000_000, 1_000_000_000))
        os.utime(runtime_core, ns=(2_000_000_000, 2_000_000_000))

        result = admin._do_update_work(
            "vibeqc-release",
            _prog(
                work,
                update_script="build.sh",
                import_check=None,
                python=str(python),
            ),
        )

        assert result.git_pull_rc != 0
        assert result.update_script_rc is None
        assert result.rolled_back is True
        assert result.success is False
        assert admin._capture_checkout_state(work) == (sha_c, None)
        assert admin._capture_local_branch_tip(work, "main")[:2] == (0, sha_c)
        assert (work / "conflict.txt").read_text(encoding="utf-8") == "local\n"
        assert subprocess.run(
            ["git", "-C", str(work), "status", "--porcelain"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout == ""
        assert subprocess.run(
            ["git", "-C", str(work), "ls-files", "-u"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout == ""
        for state_name in ("rebase-merge", "rebase-apply"):
            state_path_text = subprocess.run(
                ["git", "-C", str(work), "rev-parse", "--git-path", state_name],
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
            state_path = Path(state_path_text)
            if not state_path.is_absolute():
                state_path = work / state_path
            assert not state_path.exists()
        assert source.stat().st_mtime_ns == 1_000_000_000
        assert runtime_core.read_text(encoding="utf-8") == "OLD"
        assert "post-rollback import OK" in result.rollback_summary

    @pytest.mark.no_autopatch_branch_check
    def test_detached_managed_vibeqc_upstream_failure_rolls_back(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        runtime_core = tmp_path / "runtime" / "vibeqc" / "_vibeqc_core.so"
        runtime_core.parent.mkdir(parents=True)
        runtime_core.write_text("OLD", encoding="utf-8")
        package_code = (
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            f"_vibeqc_core.__file__ = {str(runtime_core)!r}\n"
        )
        work, sha_a, sha_b = _make_tracking_repos(
            tmp_path,
            build_sh=(
                "#!/bin/bash\n"
                f"echo NEW > {shlex.quote(str(runtime_core))}\n"
                "exit 0\n"
            ),
            initial_files={
                ".gitignore": ".venv/\n__pycache__/\n",
                "python/vibeqc/__init__.py": package_code,
                "cpp/src/bindings.cpp": "// source A\n",
            },
            remote_files={"cpp/src/bindings.cpp": "// source B\n"},
        )
        python = _managed_python(work)
        _git(work, "fetch", "origin")
        _git(work, "checkout", "--detach", sha_a)
        _git(work, "branch", "-f", "main", sha_b)
        _git(work, "branch", "--unset-upstream", "main")
        source = work / "cpp" / "src" / "bindings.cpp"
        os.utime(source, ns=(1_000_000_000, 1_000_000_000))
        os.utime(runtime_core, ns=(2_000_000_000, 2_000_000_000))
        monkeypatch.setattr(
            admin,
            "_run_git_set_upstream",
            lambda *args, **kwargs: (1, "upstream failed\n"),
        )

        result = admin._do_update_work(
            "vibeqc-release",
            _prog(
                work,
                update_script="build.sh",
                import_check=None,
                python=str(python),
            ),
        )

        assert result.git_pull_rc == 0
        assert result.update_script_rc == 0
        assert result.rolled_back is True
        assert result.success is False
        assert admin._capture_checkout_state(work) == (sha_a, None)
        assert admin._capture_local_branch_tip(work, "main")[:2] == (0, sha_b)
        assert _local_git_config(work, "branch.main.remote") is None
        assert _local_git_config(work, "branch.main.merge") is None
        assert source.stat().st_mtime_ns == 1_000_000_000
        assert runtime_core.read_text(encoding="utf-8") == "OLD"
        assert any("upstream" in error for error in result.work_errors)

    @pytest.mark.no_autopatch_branch_check
    def test_detached_managed_vibeqc_absent_branch_stays_absent_on_failure(
        self, tmp_path: Path
    ) -> None:
        runtime_core = tmp_path / "runtime" / "vibeqc" / "_vibeqc_core.so"
        runtime_core.parent.mkdir(parents=True)
        runtime_core.write_text("OLD", encoding="utf-8")
        package_code = (
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            f"_vibeqc_core.__file__ = {str(runtime_core)!r}\n"
        )
        work, sha_a, _sha_b = _make_tracking_repos(
            tmp_path,
            build_sh="#!/bin/bash\nexit 1\n",
            initial_files={
                ".gitignore": ".venv/\n__pycache__/\n",
                "python/vibeqc/__init__.py": package_code,
                "cpp/src/bindings.cpp": "// source A\n",
            },
        )
        python = _managed_python(work)
        _git(work, "checkout", "--detach", sha_a)
        _git(work, "branch", "-D", "main")
        source = work / "cpp" / "src" / "bindings.cpp"
        os.utime(source, ns=(1_000_000_000, 1_000_000_000))
        os.utime(runtime_core, ns=(2_000_000_000, 2_000_000_000))
        assert admin._capture_local_branch_tip(work, "main")[:2] == (1, None)
        assert _local_git_config(work, "branch.main.remote") is None
        assert _local_git_config(work, "branch.main.merge") is None

        result = admin._do_update_work(
            "vibeqc-release",
            _prog(
                work,
                update_script="build.sh",
                import_check=None,
                python=str(python),
            ),
        )

        assert result.update_script_rc == 1
        assert result.rolled_back is True
        assert result.success is False
        assert admin._capture_checkout_state(work) == (sha_a, None)
        assert admin._capture_local_branch_tip(work, "main")[:2] == (1, None)
        assert _local_git_config(work, "branch.main.remote") is None
        assert _local_git_config(work, "branch.main.merge") is None
        assert source.stat().st_mtime_ns == 1_000_000_000
        assert runtime_core.read_text(encoding="utf-8") == "OLD"

    @pytest.mark.no_autopatch_branch_check
    def test_detached_managed_vibeqc_absent_branch_commits_only_after_success(
        self, tmp_path: Path
    ) -> None:
        runtime_core = tmp_path / "runtime" / "vibeqc" / "_vibeqc_core.so"
        runtime_core.parent.mkdir(parents=True)
        runtime_core.write_text("OLD", encoding="utf-8")
        package_code = (
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            f"_vibeqc_core.__file__ = {str(runtime_core)!r}\n"
        )
        work, sha_a, sha_b = _make_tracking_repos(
            tmp_path,
            build_sh=(
                "#!/bin/bash\n"
                f"echo NEW > {shlex.quote(str(runtime_core))}\n"
                "exit 0\n"
            ),
            initial_files={
                ".gitignore": ".venv/\n__pycache__/\n",
                "python/vibeqc/__init__.py": package_code,
                "cpp/src/bindings.cpp": "// source A\n",
            },
        )
        python = _managed_python(work)
        _git(work, "checkout", "--detach", sha_a)
        _git(work, "branch", "-D", "main")
        source = work / "cpp" / "src" / "bindings.cpp"
        os.utime(source, ns=(1_000_000_000, 1_000_000_000))
        os.utime(runtime_core, ns=(2_000_000_000, 2_000_000_000))

        result = admin._do_update_work(
            "vibeqc-release",
            _prog(
                work,
                update_script="build.sh",
                import_check=None,
                python=str(python),
            ),
        )

        assert result.update_script_rc == 0
        assert result.import_check_rc == 0
        assert result.rolled_back is False
        assert result.success is True
        assert admin._capture_checkout_state(work) == (sha_b, "main")
        assert admin._capture_local_branch_tip(work, "main")[:2] == (0, sha_b)
        assert _local_git_config(work, "branch.main.remote") == "origin"
        assert (
            _local_git_config(work, "branch.main.merge")
            == "refs/heads/main"
        )
        assert runtime_core.read_text(encoding="utf-8") == "NEW\n"

    def test_broken_import_after_clean_build_rolls_back(
        self, tmp_path: Path
    ) -> None:
        # update_script SUCCEEDS (rc=0) but leaves an env that does not
        # import (import_check names a module that isn't installed) — the
        # host_b ABI-skew signature. The import gate must catch it and roll
        # back even though the build "succeeded".
        mod = "vq_zzz_nonexistent_module_xyz"
        build_sh = (
            "#!/bin/bash\n"
            f"echo NEW > python/{mod}/_core.so\n"
            "exit 0\n"
        )
        work, sha_a, _sha_b = _make_tracking_repos(tmp_path, build_sh=build_sh)
        so = _seed_so(work, mod, "OLD")

        result = admin._do_update_work(
            "vibeqc-dev", _prog(work, update_script="build.sh", import_check=mod),
        )

        assert result.update_script_rc == 0  # build itself "passed"
        assert result.import_check_rc is not None and result.import_check_rc != 0
        assert result.rolled_back is True
        assert result.success is False
        assert _head(work) == sha_a
        assert so.read_text() == "OLD"

    def test_managed_vibeqc_arms_import_rollback_without_config_opt_in(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Legacy vibe-qc entries omitted import_check.  A clean script rc must
        # not publish a newer Python tree when its compiled core cannot import.
        runtime_core = (
            tmp_path
            / "venv"
            / "lib"
            / "python-test"
            / "site-packages"
            / "vibeqc"
            / "_vibeqc_core.so"
        )
        runtime_core.parent.mkdir(parents=True)
        runtime_core.write_text("OLD", encoding="utf-8")
        build_sh = (
            "#!/bin/bash\n"
            f"echo NEW > {shlex.quote(str(runtime_core))}\n"
            "exit 0\n"
        )
        work, sha_a, _sha_b = _make_tracking_repos(tmp_path, build_sh=build_sh)
        package = work / "python" / "vibeqc"
        package.mkdir(parents=True)
        (package / "__init__.py").write_text("", encoding="utf-8")
        bindings = work / "cpp" / "src" / "bindings.cpp"
        bindings.parent.mkdir(parents=True)
        bindings.write_text("// managed source marker\n", encoding="utf-8")
        python = _managed_python(work)
        calls: list[tuple[str, str, list[str] | None]] = []

        def discover_runtime(
            python: str,
            module: str,
            **_kwargs: object,
        ) -> tuple[int, str, str | None, str | None, str | None]:
            assert python == str(work / ".venv" / "bin" / "python")
            assert module == "vibeqc"
            return (
                1,
                "broken import",
                None,
                str(work / "python" / "vibeqc" / "__init__.py"),
                str(runtime_core),
            )

        def failed_probe(
            python: str,
            module: str,
            *,
            symbols: list[str] | None = None,
            source_root: Path | None = None,
            **_kwargs: object,
        ) -> tuple[int, str]:
            assert source_root == work
            calls.append((python, module, symbols))
            return 1, (
                "ImportError: missing symbol "
                "MOLECULAR_XC_GRID_MAX_WORKERS from _vibeqc_core"
            )

        monkeypatch.setattr(
            config, "run_import_runtime_identity_probe", discover_runtime
        )
        monkeypatch.setattr(admin, "_run_import_check", failed_probe)
        result = admin._do_update_work(
            "vibeqc-dev",
            _prog(
                work,
                update_script="build.sh",
                import_check=None,
                python=str(python),
            ),
        )

        assert result.update_script_rc == 0
        assert result.import_check_rc == 1
        assert result.rolled_back is False
        assert result.success is False
        assert _head(work) == sha_a
        assert runtime_core.read_text() == "OLD"
        assert any("rollback FAILED" in error for error in result.work_errors)
        assert calls and all(module == "vibeqc" for _, module, _ in calls)

    def test_managed_vibeqc_refuses_mutation_without_recoverable_core(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        work, sha_a, _sha_b = _make_tracking_repos(
            tmp_path,
            build_sh="#!/bin/bash\nexit 0\n",
            initial_files={
                ".gitignore": ".venv/\n__pycache__/\n",
                "python/vibeqc/__init__.py": "",
                "cpp/src/bindings.cpp": "// managed source marker\n",
            },
        )
        python = _managed_python(work)
        monkeypatch.setattr(
            config,
            "run_import_runtime_identity_probe",
            lambda *args, **kwargs: (
                1,
                "import vibeqc: probe timed out after 15s",
                None,
                None,
                None,
            ),
        )

        result = admin._do_update_work(
            "vibeqc-release",
            _prog(
                work,
                update_script="build.sh",
                import_check=None,
                python=str(python),
            ),
        )

        assert result.git_pull_rc is None
        assert result.update_script_rc is None
        assert result.rolled_back is False
        assert result.success is False
        assert _head(work) == sha_a
        assert (work / "marker.txt").read_text(encoding="utf-8") == "A\n"
        assert any(
            "did not identify its vibeqc package source" in error
            for error in result.work_errors
        )

    def test_managed_vibeqc_refuses_pure_python_core_before_mutation(
        self, tmp_path: Path
    ) -> None:
        work, sha_a, _sha_b = _make_tracking_repos(
            tmp_path,
            build_sh="#!/bin/bash\nexit 0\n",
            initial_files={
                ".gitignore": ".venv/\n__pycache__/\n",
                "python/vibeqc/__init__.py": "from . import _vibeqc_core\n",
                "python/vibeqc/_vibeqc_core.py": "# shim\n",
                "cpp/src/bindings.cpp": "// managed source marker\n",
            },
        )
        python = _managed_python(work)

        result = admin._do_update_work(
            "vibeqc-release",
            _prog(
                work,
                update_script="build.sh",
                import_check=None,
                python=str(python),
            ),
        )

        assert result.git_pull_rc is None
        assert result.update_script_rc is None
        assert result.rolled_back is False
        assert _head(work) == sha_a
        assert any(
            "target-supported compiled-extension proof" in error
            for error in result.work_errors
        )

    def test_managed_vibeqc_refuses_interpreter_from_wrong_checkout(
        self, tmp_path: Path
    ) -> None:
        work, sha_a, _sha_b = _make_tracking_repos(
            tmp_path,
            build_sh="#!/bin/bash\nexit 0\n",
            initial_files={
                ".gitignore": ".venv/\n__pycache__/\n",
                "python/vibeqc/__init__.py": "",
                "cpp/src/bindings.cpp": "// managed source marker\n",
            },
        )
        other_package = tmp_path / "other" / "python" / "vibeqc"
        other_package.mkdir(parents=True)
        other_core = tmp_path / "other" / ".venv" / "_vibeqc_core.so"
        other_core.parent.mkdir(parents=True)
        other_core.write_text("OTHER", encoding="utf-8")
        (other_package / "__init__.py").write_text(
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            f"_vibeqc_core.__file__ = {str(other_core)!r}\n",
            encoding="utf-8",
        )
        python = _managed_python(work)
        python.write_text(
            "#!/bin/sh\n"
            f"PYTHONPATH={shlex.quote(str(other_package.parent))} "
            f"exec {shlex.quote(sys.executable)} -S \"$@\"\n",
            encoding="utf-8",
        )
        python.chmod(0o755)

        result = admin._do_update_work(
            "vibeqc-release",
            _prog(
                work,
                update_script="build.sh",
                import_check=None,
                python=str(python),
            ),
        )

        assert result.git_pull_rc is None
        assert result.update_script_rc is None
        assert result.rolled_back is False
        assert _head(work) == sha_a
        assert any(
            "wrong checkout" in error
            and str(other_package / "__init__.py") in error
            for error in result.work_errors
        )

    @pytest.mark.no_autopatch_branch_check
    @pytest.mark.parametrize(
        ("marker", "directory"),
        (
            ("MERGE_HEAD", False),
            ("CHERRY_PICK_HEAD", False),
            ("REVERT_HEAD", False),
            ("rebase-merge", True),
            ("rebase-apply", True),
            ("sequencer", True),
            ("BISECT_START", False),
            ("BISECT_LOG", False),
        ),
    )
    def test_managed_vibeqc_refuses_preexisting_git_operation(
        self, tmp_path: Path, marker: str, directory: bool
    ) -> None:
        work, sha_a, _sha_b = _make_tracking_repos(
            tmp_path,
            build_sh="#!/bin/bash\nexit 0\n",
            initial_files={
                ".gitignore": ".venv/\n__pycache__/\n",
                "python/vibeqc/__init__.py": "",
                "cpp/src/bindings.cpp": "// managed source marker\n",
            },
        )
        python = _managed_python(work)
        operation = work / ".git" / marker
        if directory:
            operation.mkdir()
        else:
            operation.write_text(f"{sha_a}\n", encoding="utf-8")

        result = admin._do_update_work(
            "vibeqc-release",
            _prog(
                work,
                update_script="build.sh",
                import_check=None,
                python=str(python),
            ),
        )

        assert result.git_pull_rc is None
        assert result.update_script_rc is None
        assert result.rolled_back is False
        assert _head(work) == sha_a
        assert operation.exists()
        assert any(
            "pre-existing Git operation" in error and marker in error
            for error in result.work_errors
        )

    def test_missing_import_symbol_after_clean_build_rolls_back(
        self, tmp_path: Path
    ) -> None:
        # A stale native extension can still import the package but miss a
        # newly exported API symbol (host_f CosxVariant-class drift). The symbol
        # gate must fail the update just like a module ImportError.
        build_sh = (
            "#!/bin/bash\n"
            "echo NEW > python/os/_core.so\n"
            "exit 0\n"
        )
        work, sha_a, _sha_b = _make_tracking_repos(tmp_path, build_sh=build_sh)
        so = _seed_so(work, "os", "OLD")

        result = admin._do_update_work(
            "vibeqc-dev",
            _prog(
                work,
                update_script="build.sh",
                import_check="os",
                import_symbols=["definitely_missing_symbol_xyz"],
            ),
        )

        assert result.update_script_rc == 0
        assert result.import_check_rc is not None and result.import_check_rc != 0
        assert "definitely_missing_symbol_xyz" in result.import_check_output
        assert result.rolled_back is True
        assert result.success is False
        assert _head(work) == sha_a
        assert so.read_text() == "OLD"

    def test_successful_build_keeps_new_tree(self, tmp_path: Path) -> None:
        # Clean build + importable env → no rollback, the new commit stays.
        build_sh = (
            "#!/bin/bash\n"
            "echo NEW > python/sys/_core.so\n"
            "exit 0\n"
        )
        work, _sha_a, sha_b = _make_tracking_repos(tmp_path, build_sh=build_sh)
        so = _seed_so(work, "sys", "OLD")

        result = admin._do_update_work(
            "vibeqc-dev", _prog(work, update_script="build.sh", import_check="sys"),
        )

        assert result.update_script_rc == 0
        assert result.import_check_rc == 0
        assert result.rolled_back is False
        assert result.success is True
        assert _head(work) == sha_b          # advanced + kept
        assert so.read_text() == "NEW\n"     # freshly built artifact kept

    @pytest.mark.no_autopatch_branch_check
    def test_same_sha_post_script_failure_restores_native_state(
        self, tmp_path: Path
    ) -> None:
        runtime_core = tmp_path / "runtime" / "vibeqc" / "_vibeqc_core.so"
        runtime_core.parent.mkdir(parents=True)
        runtime_core.write_text("OLD", encoding="utf-8")
        package_code = (
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            f"_vibeqc_core.__file__ = {str(runtime_core)!r}\n"
        )
        work, sha_a, _sha_b = _make_tracking_repos(
            tmp_path,
            build_sh="#!/bin/bash\nexit 0\n",
            initial_files={
                ".gitignore": ".venv/\n__pycache__/\n",
                "post.sh": (
                    "#!/bin/bash\n"
                    f"echo BROKEN > {shlex.quote(str(runtime_core))}\n"
                    "exit 1\n"
                ),
                "python/vibeqc/__init__.py": package_code,
                "cpp/src/bindings.cpp": "// source A\n",
            },
        )
        python = _managed_python(work)
        _git(work, "checkout", "--detach", sha_a)
        source = work / "cpp" / "src" / "bindings.cpp"
        os.utime(source, ns=(1_000_000_000, 1_000_000_000))
        os.utime(runtime_core, ns=(2_000_000_000, 2_000_000_000))
        program = config.VenvProgram(
            kind="venv",
            python=str(python),
            git_dir=str(work),
            branch="main",
            update_script="build.sh",
            post_update_script="post.sh",
        )

        result = admin._do_update_work(
            "vibeqc-release",
            program,
            expected_sha=sha_a,
        )

        assert result.update_script_rc == 0
        assert result.import_check_rc == 0
        assert result.post_update_script_rc == 1
        assert result.rolled_back is True
        assert result.success is False
        assert admin._capture_checkout_state(work) == (sha_a, None)
        assert runtime_core.read_text(encoding="utf-8") == "OLD"

    @pytest.mark.no_autopatch_branch_check
    def test_immutable_restore_failure_does_not_overwrite_new_checkout_core(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        work, sha_a, sha_b = _make_tracking_repos(
            tmp_path,
            build_sh="#!/bin/bash\nexit 0\n",
        )
        backup_dir = tmp_path / "native-backup"
        backup_dir.mkdir()
        backup_core = backup_dir / "_vibeqc_core.so"
        backup_core.write_text("OLD", encoding="utf-8")
        live_core = tmp_path / "runtime" / "_vibeqc_core.so"
        live_core.parent.mkdir()
        live_core.write_text("NEW", encoding="utf-8")
        snapshot = admin._NativeArtifactSnapshot(
            backup_dir,
            ((backup_core, live_core),),
            (),
            (),
        )
        _git(work, "fetch", "origin")
        _git(work, "reset", "--hard", sha_b)
        result = admin.UpdateResult(
            env="vibeqc-release",
            git_dir=str(work),
            branch="main",
            update_script="build.sh",
            expected_sha=sha_b,
            pre_update_sha=sha_a,
            pre_update_branch="main",
        )
        result.work_errors.append("synthetic immutable failure")
        monkeypatch.setattr(
            admin,
            "_restore_checkout_state",
            lambda *args, **kwargs: (1, "synthetic restore failure"),
        )

        admin._finalize_immutable_checkout(
            result,
            _prog(work, update_script="build.sh", import_check="sys"),
            work,
            snapshot,
        )

        assert result.rolled_back is False
        assert live_core.read_text(encoding="utf-8") == "NEW"
        assert any("rollback FAILED" in error for error in result.work_errors)

    def test_disarmed_without_import_check(self, tmp_path: Path) -> None:
        # No import_check → atomic machinery is off: a failed build does
        # NOT roll back (pre-v0.12.x behavior preserved for pure-git envs).
        build_sh = "#!/bin/bash\nexit 1\n"
        work, _sha_a, sha_b = _make_tracking_repos(tmp_path, build_sh=build_sh)
        result = admin._do_update_work(
            "pure-git-env",
            _prog(work, update_script="build.sh", import_check=None),
        )
        assert result.update_script_rc == 1
        assert result.rolled_back is False
        assert result.pre_update_sha is None
        assert _head(work) == sha_b  # NOT rolled back

    def test_explicit_vibeqc_probe_on_generic_checkout_keeps_legacy_atomicity(
        self, tmp_path: Path
    ) -> None:
        build_sh = (
            "#!/bin/bash\n"
            "echo NEW > python/vibeqc/_core.so\n"
            "exit 1\n"
        )
        work, sha_a, _sha_b = _make_tracking_repos(tmp_path, build_sh=build_sh)
        so = _seed_so(work, "vibeqc", "OLD")
        module_dir = tmp_path / "modules"
        module_dir.mkdir()
        (module_dir / "vibeqc.py").write_text("", encoding="utf-8")
        python = tmp_path / "external-venv" / "bin" / "python"
        python.parent.mkdir(parents=True)
        python.write_text(
            "#!/bin/sh\n"
            f"PYTHONPATH={shlex.quote(str(module_dir))} "
            f"exec {shlex.quote(sys.executable)} -S \"$@\"\n",
            encoding="utf-8",
        )
        python.chmod(0o755)

        result = admin._do_update_work(
            "generic-with-vibeqc-dependency",
            _prog(
                work,
                update_script="build.sh",
                import_check="vibeqc",
                python=str(python),
            ),
        )

        assert result.update_script_rc == 1
        assert result.rolled_back is True
        assert result.success is False
        assert _head(work) == sha_a
        assert so.read_text(encoding="utf-8") == "OLD"


# ======================================================================
# An auto-generated build job must be born admissible
# ======================================================================


class TestBuildJobCpuRequest:
    """``build_job_cpus`` never asks for more than the daemon will admit.

    Requesting ``os.cpu_count()`` outright made every generated build
    unadmittable on any host whose daemon caps CPUs below the physical core
    count: host_a had 16 cores against an 8-CPU cap, so ``--refresh`` and the
    auto-update timer were dead there (2026-07-25, jobs c621e42ce2a2 /
    68e6cd5297d0). The spec sat PENDING forever before the daemon learned to
    terminal-fail it, and fails immediately after -- unusable either way.
    """

    def test_clamps_to_the_advertised_effective_cap(
        self, state: config.Config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(build_job.os, "cpu_count", lambda: 16)
        capacity.write_daemon_capacity(
            max_cpus=8, max_jobs=None, max_mem_mb=51218,
        )

        assert build_job.build_job_cpus(state) == 8

    def test_advertised_cap_wins_over_the_config_section(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Only the advertised file reflects a CLI-only ``--max-cpus``."""
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
        (tmp_path / "cfg").mkdir()
        (tmp_path / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n\n[daemon]\nmax_cpus = 12\n'
        )
        cfg = config.load_config()
        monkeypatch.setattr(build_job.os, "cpu_count", lambda: 16)
        capacity.write_daemon_capacity(max_cpus=4, max_jobs=None, max_mem_mb=None)

        assert build_job.build_job_cpus(cfg) == 4

    def test_falls_back_to_the_daemon_config_section(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A daemon that never restarted since capacity advertising landed
        still has its durable ``[daemon]`` caps honoured."""
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
        (tmp_path / "cfg").mkdir()
        (tmp_path / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n\n[daemon]\nmax_cpus = 8\n'
        )
        cfg = config.load_config()
        monkeypatch.setattr(build_job.os, "cpu_count", lambda: 16)
        assert capacity.read_daemon_capacity() is None

        assert build_job.build_job_cpus(cfg) == 8

    def test_no_cap_anywhere_keeps_the_physical_count(
        self, state: config.Config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Behaviour-preserving: the daemon defaults max_cpus to the same
        physical count, so an uncapped host is unchanged."""
        monkeypatch.setattr(build_job.os, "cpu_count", lambda: 16)
        assert capacity.read_daemon_capacity() is None

        assert build_job.build_job_cpus(state) == 16

    def test_a_cap_above_the_machine_does_not_inflate_the_request(
        self, state: config.Config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(build_job.os, "cpu_count", lambda: 4)
        capacity.write_daemon_capacity(max_cpus=64, max_jobs=None, max_mem_mb=None)

        assert build_job.build_job_cpus(state) == 4

    def test_submitted_spec_is_admissible_under_the_cap(
        self, state: config.Config, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """End to end: the written spec is one the daemon can dispatch."""
        monkeypatch.setattr(build_job.os, "cpu_count", lambda: 16)
        capacity.write_daemon_capacity(
            max_cpus=8, max_jobs=None, max_mem_mb=51218,
        )
        qd, jd = _qd_jd(tmp_path)

        out = build_job.submit_build_env_job(
            "vibeqc-dev", state, host="localhost", queue_dir=qd, jobs_dir=jd,
        )

        assert out.action == "submitted"
        spec = JobSpec.read(next(iter(qd.glob("*.json"))))
        assert spec.build_env == "vibeqc-dev"
        assert spec.cpus == 8, "pre-fix this was os.cpu_count() = 16"


# ======================================================================
# Slot-enabled update: build alongside, flip only on success
# ======================================================================


def _ok_result(env: str, prog: object, **_kwargs: object) -> object:
    python = Path(prog.python)  # type: ignore[attr-defined]
    python.parent.mkdir(parents=True, exist_ok=True)
    python.symlink_to(sys.executable)
    res = admin.UpdateResult(
        env=env,
        git_dir=prog.git_dir,  # type: ignore[attr-defined]
        branch=None,
        update_script=None,
    )
    res.git_pull_rc = 0
    return res


class TestSlotEnabledUpdate:
    """A slot-enabled env builds a NEW slot and flips only after it passes.

    The live runtime is never written to, which is the whole point: an in-place
    update rewrites the files a live process imports from, and a job paused
    across one can serve some modules from `sys.modules` while importing others
    off the rewritten disk.
    """

    def _live_repo(self, tmp_path: Path) -> tuple[Path, str, str]:
        import subprocess

        repo = tmp_path / "live"
        repo.mkdir()

        def g(*a: str) -> str:
            return subprocess.run(
                ["git", "-C", str(repo), *a],
                capture_output=True, text=True, check=True,
            ).stdout.strip()

        g("init", "--quiet", "-b", "main")
        g("config", "user.email", "t@example.invalid")
        g("config", "user.name", "T")
        (repo / "VERSION").write_text("one")
        g("add", "-A")
        g("-c", "commit.gpgsign=false", "commit", "-m", "one", "--quiet")
        first = g("rev-parse", "HEAD")
        (repo / "VERSION").write_text("two")
        g("add", "-A")
        g("-c", "commit.gpgsign=false", "commit", "-m", "two", "--quiet")
        return repo, first, g("rev-parse", "HEAD")

    def _prog(self, repo: Path, root: Path) -> config.VenvProgram:
        return config.VenvProgram(
            kind="venv",
            python=str(repo / ".venv" / "bin" / "python"),
            git_dir=str(repo),
            runtime_slot_root=str(root),
        )

    def test_expected_sha_is_required(self, tmp_path: Path) -> None:
        """Slots are keyed by commit, so the target must be known up front."""
        repo, _first, _second = self._live_repo(tmp_path)
        root = tmp_path / "rt"

        result = admin._do_update_work(
            "vibeqc-release", self._prog(repo, root), expected_sha=None
        )

        assert result.success is False
        assert any("--expected-sha" in e for e in result.work_errors)
        assert not (root / "current").exists(), "nothing should have been published"

    def test_successful_build_activates_the_new_slot(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        repo, first, _second = self._live_repo(tmp_path)
        root = tmp_path / "rt"

        def fake_inner(env, prog, **kw):  # type: ignore[no-untyped-def]
            # The inner build runs against the SLOT, never the live checkout.
            assert str(root) in prog.git_dir
            assert prog.runtime_slot_root is None
            python = Path(prog.python)
            python.parent.mkdir(parents=True, exist_ok=True)
            python.symlink_to(sys.executable)
            res = admin.UpdateResult(
                env=env,
                git_dir=prog.git_dir,
                branch=None,
                update_script=None,
            )
            res.git_pull_rc = 0
            return res

        monkeypatch.setattr(admin, "_do_update_work", fake_inner)

        result = admin._do_slot_update_work(
            "vibeqc-release", self._prog(repo, root), expected_sha=first
        )

        assert result.success is True
        assert runtime_slots.resolve_current(root) == first

    @pytest.mark.no_autopatch_branch_check
    def test_fresh_structural_vibeqc_slot_bootstraps_before_activation(
        self, tmp_path: Path
    ) -> None:
        repo = tmp_path / "live-vibeqc"
        repo.mkdir()
        _git(repo, "init", "-b", "main")
        package = repo / "python" / "vibeqc"
        package.mkdir(parents=True)
        (package / "__init__.py").write_text(
            "from pathlib import Path\n"
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            "_vibeqc_core.__file__ = str(\n"
            "    Path(__file__).resolve().parents[2]\n"
            "    / '.venv' / 'lib' / 'vibeqc' / '_vibeqc_core.so'\n"
            ")\n",
            encoding="utf-8",
        )
        source = repo / "cpp" / "src" / "bindings.cpp"
        source.parent.mkdir(parents=True)
        source.write_text("// native source\n", encoding="utf-8")
        (repo / ".gitignore").write_text(".venv/\n__pycache__/\n")
        (repo / "build.sh").write_text(
            "#!/bin/bash\n"
            "set -eu\n"
            "mkdir -p .venv/bin .venv/lib/vibeqc\n"
            "echo CORE > .venv/lib/vibeqc/_vibeqc_core.so\n"
            "cat > .venv/bin/python <<'EOF'\n"
            "#!/bin/sh\n"
            '_vq_bin=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)\n'
            '_vq_source=$(dirname "$(dirname "$_vq_bin")")\n'
            f'PYTHONPATH="$_vq_source/python" exec {shlex.quote(sys.executable)} '
            '-S "$@"\n'
            "EOF\n"
            "chmod +x .venv/bin/python\n",
            encoding="utf-8",
        )
        _git(repo, "add", "-A")
        _git(repo, "commit", "-m", "structural vibeqc")
        sha = _head(repo)
        root = tmp_path / "rt"
        program = config.VenvProgram(
            kind="venv",
            python=str(repo / ".venv" / "bin" / "python"),
            git_dir=str(repo),
            branch="main",
            update_script="build.sh",
            runtime_slot_root=str(root),
        )

        result = admin._do_slot_update_work(
            "vibeqc-release",
            program,
            expected_sha=sha,
        )

        assert result.success is True, result.work_errors
        assert result.import_check_rc == 0
        assert runtime_slots.resolve_current(root) == sha
        assert runtime_slots.slot_python(root, sha).is_file()

        reused = admin._do_slot_update_work(
            "vibeqc-release",
            program,
            expected_sha=sha,
        )

        assert reused.success is True, reused.work_errors
        assert reused.update_script_rc == 0
        assert reused.import_check_rc == 0
        assert runtime_slots.resolve_current(root) == sha

    @pytest.mark.no_autopatch_branch_check
    def test_fresh_structural_slot_rejects_external_core(
        self, tmp_path: Path
    ) -> None:
        external_core = tmp_path / "external" / "vibeqc" / "_vibeqc_core.so"
        external_core.parent.mkdir(parents=True)
        external_core.write_text("EXTERNAL", encoding="utf-8")
        repo = tmp_path / "live-vibeqc"
        repo.mkdir()
        _git(repo, "init", "-b", "main")
        package = repo / "python" / "vibeqc"
        package.mkdir(parents=True)
        (package / "__init__.py").write_text(
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            f"_vibeqc_core.__file__ = {str(external_core)!r}\n",
            encoding="utf-8",
        )
        source = repo / "cpp" / "src" / "bindings.cpp"
        source.parent.mkdir(parents=True)
        source.write_text("// native source\n", encoding="utf-8")
        (repo / ".gitignore").write_text(".venv/\n__pycache__/\n")
        (repo / "build.sh").write_text(
            "#!/bin/bash\n"
            "set -eu\n"
            "mkdir -p .venv/bin\n"
            "cat > .venv/bin/python <<'EOF'\n"
            "#!/bin/sh\n"
            '_vq_bin=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)\n'
            '_vq_source=$(dirname "$(dirname "$_vq_bin")")\n'
            f'PYTHONPATH="$_vq_source/python" exec {shlex.quote(sys.executable)} '
            '-S "$@"\n'
            "EOF\n"
            "chmod +x .venv/bin/python\n",
            encoding="utf-8",
        )
        _git(repo, "add", "-A")
        _git(repo, "commit", "-m", "structural vibeqc")
        sha = _head(repo)
        future = time.time_ns() + 10_000_000_000
        os.utime(external_core, ns=(future, future))
        root = tmp_path / "rt"
        program = config.VenvProgram(
            kind="venv",
            python=str(repo / ".venv" / "bin" / "python"),
            git_dir=str(repo),
            branch="main",
            update_script="build.sh",
            runtime_slot_root=str(root),
        )

        result = admin._do_slot_update_work(
            "vibeqc-release",
            program,
            expected_sha=sha,
        )

        assert result.success is False
        assert result.import_check_rc == 1
        assert "outside the slot generation" in result.import_check_output
        assert runtime_slots.resolve_current(root) is None

    @pytest.mark.no_autopatch_branch_check
    @pytest.mark.parametrize(
        "core_relative",
        (
            "python/vibeqc/.venv/_vibeqc_core.so",
            ".venv/lib/.vq-immutable-runtime/_vibeqc_core.so",
        ),
        ids=["nested-venv", "immutable-marker-component"],
    )
    def test_fresh_structural_slot_rejects_core_in_excluded_source_path(
        self, tmp_path: Path, core_relative: str
    ) -> None:
        repo = tmp_path / "live-vibeqc"
        repo.mkdir()
        _git(repo, "init", "-b", "main")
        package = repo / "python" / "vibeqc"
        package.mkdir(parents=True)
        (package / "__init__.py").write_text(
            "from pathlib import Path\n"
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            "_vibeqc_core.__file__ = str(\n"
            "    Path(__file__).resolve().parents[2]\n"
            f"    / {core_relative!r}\n"
            ")\n",
            encoding="utf-8",
        )
        source = repo / "cpp" / "src" / "bindings.cpp"
        source.parent.mkdir(parents=True)
        source.write_text("// native source\n", encoding="utf-8")
        (repo / ".gitignore").write_text(".venv/\n__pycache__/\n")
        (repo / "build.sh").write_text(
            "#!/bin/bash\n"
            "set -eu\n"
            f"mkdir -p .venv/bin {shlex.quote(str(Path(core_relative).parent))}\n"
            f"echo CORE > {shlex.quote(core_relative)}\n"
            "cat > .venv/bin/python <<'EOF'\n"
            "#!/bin/sh\n"
            '_vq_bin=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)\n'
            '_vq_source=$(dirname "$(dirname "$_vq_bin")")\n'
            f'PYTHONPATH="$_vq_source/python" exec {shlex.quote(sys.executable)} '
            '-S "$@"\n'
            "EOF\n"
            "chmod +x .venv/bin/python\n",
            encoding="utf-8",
        )
        _git(repo, "add", "-A")
        _git(repo, "commit", "-m", "structural vibeqc")
        sha = _head(repo)
        root = tmp_path / "rt"
        program = config.VenvProgram(
            kind="venv",
            python=str(repo / ".venv" / "bin" / "python"),
            git_dir=str(repo),
            branch="main",
            update_script="build.sh",
            runtime_slot_root=str(root),
        )

        result = admin._do_slot_update_work(
            "vibeqc-release",
            program,
            expected_sha=sha,
        )

        assert result.success is False
        assert result.import_check_rc == 1
        assert "outside the slot's hashed content" in result.import_check_output
        assert runtime_slots.resolve_current(root) is None

    def test_verified_structural_slot_is_revalidated_before_reuse(
        self, tmp_path: Path
    ) -> None:
        repo = tmp_path / "live-vibeqc"
        repo.mkdir()
        _git(repo, "init", "-b", "main")
        package = repo / "python" / "vibeqc"
        package.mkdir(parents=True)
        (package / "__init__.py").write_text(
            "from pathlib import Path\n"
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            "_vibeqc_core.__file__ = str(\n"
            "    Path(__file__).resolve().parents[2]\n"
            "    / '.venv' / 'lib' / 'vibeqc' / '_vibeqc_core.so'\n"
            ")\n",
            encoding="utf-8",
        )
        bindings = repo / "cpp" / "src" / "bindings.cpp"
        bindings.parent.mkdir(parents=True)
        bindings.write_text("// native source\n", encoding="utf-8")
        (repo / ".gitignore").write_text(".venv/\n__pycache__/\n")
        _git(repo, "add", "-A")
        _git(repo, "commit", "-m", "structural vibeqc")
        sha = _head(repo)
        root = tmp_path / "rt"
        transaction = "2" * 32
        runtime_slots.begin_slot_build(
            root,
            sha,
            transaction_id=transaction,
            in_use=lambda: set(),
        )
        source = runtime_slots.materialize_source(repo, root, sha)
        _managed_python(source)
        core = source / ".venv" / "lib" / "vibeqc" / "_vibeqc_core.so"
        core.parent.mkdir(parents=True)
        core.write_text("STALE", encoding="utf-8")
        slot_bindings = source / "cpp" / "src" / "bindings.cpp"
        os.utime(core, ns=(1_000_000_000, 1_000_000_000))
        os.utime(slot_bindings, ns=(2_000_000_000, 2_000_000_000))
        runtime_slots.seal_slot_build(
            root,
            sha,
            transaction_id=transaction,
        )
        program = config.VenvProgram(
            kind="venv",
            python=str(repo / ".venv" / "bin" / "python"),
            git_dir=str(repo),
            branch="main",
            update_script="build.sh",
            runtime_slot_root=str(root),
        )

        result = admin._do_slot_update_work(
            "vibeqc-release",
            program,
            expected_sha=sha,
        )

        assert result.success is False
        assert result.import_check_rc == 1
        assert "stale compiled core" in result.import_check_output
        assert runtime_slots.resolve_current(root) is None
        assert any(
            "failed structural vibeqc runtime revalidation" in error
            for error in result.work_errors
        )

    def test_public_admin_update_publishes_a_verified_slot(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        repo, first, _second = self._live_repo(tmp_path)
        root = tmp_path / "rt"
        prog = self._prog(repo, root)
        cfg = config.Config(programs={"vibeqc-release": prog})
        ordinary_update = admin._do_update_work

        def route(env, candidate, **kwargs):  # type: ignore[no-untyped-def]
            if candidate.runtime_slot_root is None:
                return _ok_result(env, candidate)
            return ordinary_update(env, candidate, **kwargs)

        monkeypatch.setattr(admin, "_do_update_work", route)

        result = admin.update_env(
            "vibeqc-release",
            cfg,
            host="localhost",
            expected_sha=first,
        )

        assert result.success is True
        assert runtime_slots.resolve_current(root) == first
        assert json.loads(
            (
                runtime_slots.slot_path(root, first)
                / runtime_slots.SLOT_STATE_MARKER
            ).read_text()
        )["state"] == "verified"

    def test_a_failed_build_does_not_flip(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The flip is the last step, so a half-built slot is never published
        and the live runtime keeps serving."""
        repo, first, second = self._live_repo(tmp_path)
        root = tmp_path / "rt"
        # A slot is already live.
        transaction = "1" * 32
        runtime_slots.begin_slot_build(
            root,
            first,
            transaction_id=transaction,
            in_use=lambda: set(),
        )
        runtime_slots.materialize_source(repo, root, first)
        python = runtime_slots.slot_python(root, first)
        python.parent.mkdir(parents=True)
        python.symlink_to(sys.executable)
        runtime_slots.seal_slot_build(
            root,
            first,
            transaction_id=transaction,
        )
        runtime_slots.activate(root, first)

        def failing_inner(env, prog, **kw):  # type: ignore[no-untyped-def]
            res = admin.UpdateResult(
                env=env,
                git_dir=prog.git_dir,
                branch=None,
                update_script=None,
            )
            res.work_errors.append("build blew up")
            return res

        monkeypatch.setattr(admin, "_do_update_work", failing_inner)

        result = admin._do_slot_update_work(
            "vibeqc-release", self._prog(repo, root), expected_sha=second
        )

        assert result.success is False
        assert runtime_slots.resolve_current(root) == first, (
            "a failed build changed the live runtime"
        )
        assert any("live runtime is unchanged" in e for e in result.work_errors)

    def test_a_failed_build_is_cleaned_and_retried_only_when_unused(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        repo, first, _second = self._live_repo(tmp_path)
        root = tmp_path / "rt"

        def failing_inner(env, prog, **kw):  # type: ignore[no-untyped-def]
            res = admin.UpdateResult(
                env=env,
                git_dir=prog.git_dir,
                branch=None,
                update_script=None,
            )
            res.work_errors.append("first build failed")
            return res

        monkeypatch.setattr(admin, "_do_update_work", failing_inner)
        first_result = admin._do_slot_update_work(
            "vibeqc-release", self._prog(repo, root), expected_sha=first
        )
        failed_slot = runtime_slots.slot_path(root, first)
        sentinel = failed_slot / "failed-attempt"
        sentinel.write_text("old transaction\n")

        monkeypatch.setattr(admin, "_do_update_work", _ok_result)
        retry = admin._do_slot_update_work(
            "vibeqc-release", self._prog(repo, root), expected_sha=first
        )

        assert first_result.success is False
        assert retry.success is True
        assert not sentinel.exists()
        assert runtime_slots.resolve_current(root) == first

    @pytest.mark.parametrize("reuse", [False, True])
    def test_activation_retains_unreferenced_generations(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, reuse: bool,
    ) -> None:
        repo, first, _second = self._live_repo(tmp_path)
        root = tmp_path / "rt"
        monkeypatch.setattr(admin, "_do_update_work", _ok_result)
        if reuse:
            assert admin._do_slot_update_work(
                "vibeqc-release", self._prog(repo, root), expected_sha=first,
            ).success
        candidate = runtime_slots.create_slot(root, "d" * 40)
        sentinel = candidate / "retained-history"
        sentinel.write_bytes(b"must survive deployment\n")

        result = admin._do_slot_update_work(
            "vibeqc-release", self._prog(repo, root), expected_sha=first,
        )

        assert result.success
        assert runtime_slots.resolve_current(root) == first
        assert sentinel.read_bytes() == b"must survive deployment\n"

    def test_unreadable_spec_refuses_liveness_snapshot_without_deletion(
        self, tmp_path: Path
    ) -> None:
        root = tmp_path / "rt"
        candidate = runtime_slots.create_slot(root, "d" * 40)
        sentinel = candidate / "must-survive"
        sentinel.write_text("slot bytes\n")
        queue = paths.queue_dir()
        queue.mkdir(parents=True, exist_ok=True)
        (queue / "corrupt.json").write_text("not-json\n")

        with pytest.raises(runtime_slots.RuntimeSlotError, match="unreadable"):
            admin._runtime_slot_in_use_snapshot(str(root))

        assert sentinel.read_text() == "slot bytes\n"

    def test_the_live_checkout_is_never_written_to(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The property that makes this safe for a running job."""
        repo, first, _second = self._live_repo(tmp_path)
        root = tmp_path / "rt"
        before = (repo / "VERSION").read_text()

        monkeypatch.setattr(
            admin,
            "_do_update_work",
            lambda env, prog, **kw: _ok_result(env, prog),
        )

        admin._do_slot_update_work(
            "vibeqc-release", self._prog(repo, root), expected_sha=first
        )

        assert (repo / "VERSION").read_text() == before

    def test_a_program_without_a_slot_root_is_untouched(
        self, tmp_path: Path
    ) -> None:
        """Every host today. The slot path must be entirely opt-in."""
        repo, _first, _second = self._live_repo(tmp_path)
        prog = config.VenvProgram(
            kind="venv",
            python=str(repo / ".venv" / "bin" / "python"),
            git_dir=str(repo),
        )

        assert prog.runtime_slot_root is None
        # Reaches the ordinary path: no slot root, so no slot machinery.
        result = admin._do_update_work("vibeqc-dev", prog)
        assert result.git_dir == str(repo)


class TestBuildJobMemoryRequest:
    """A build job declares a figure that fits, rather than inheriting one
    that cannot.

    An undeclared job is charged the daemon's `default_job_mem_mb`. On host_a that
    EXCEEDS `max_mem_mb`, so every undeclared job there -- builds included -- was
    unadmittable forever: the memory host_f of the CPU defect fixed in 60c915752.

    Declaring rather than clamping is deliberate. Clamping CPUs costs a build
    wall time; clamping memory gets it OOM-killed halfway through, which is
    worse than not starting at all.
    """

    def test_fits_inside_the_advertised_cap(
        self, state: config.Config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        capacity.write_daemon_capacity(
            max_cpus=8, max_jobs=None, max_mem_mb=51218
        )

        mem = build_job.build_job_mem_mb(state)

        assert mem is not None
        assert mem < 51218, "the whole point is that it fits"
        assert mem == int(51218 * build_job.BUILD_MEM_FRACTION)

    def test_leaves_room_for_the_work_already_running(
        self, state: config.Config
    ) -> None:
        """A build is maintenance, not the point of the machine."""
        capacity.write_daemon_capacity(
            max_cpus=8, max_jobs=None, max_mem_mb=40000
        )

        assert build_job.build_job_mem_mb(state) <= 20000

    def test_falls_back_to_the_daemon_config_section(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
        (tmp_path / "cfg").mkdir()
        (tmp_path / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n\n[daemon]\nmax_mem_mb = 30000\n'
        )
        cfg = config.load_config()
        assert capacity.read_daemon_capacity() is None

        assert build_job.build_job_mem_mb(cfg) == 15000

    def test_an_uncapped_host_declares_nothing(
        self, state: config.Config
    ) -> None:
        """Nothing to fit inside, so behave exactly as before."""
        assert capacity.read_daemon_capacity() is None

        assert build_job.build_job_mem_mb(state) is None

    def test_the_submitted_spec_carries_the_figure(
        self, state: config.Config, tmp_path: Path
    ) -> None:
        capacity.write_daemon_capacity(
            max_cpus=8, max_jobs=None, max_mem_mb=51218
        )
        qd, jd = _qd_jd(tmp_path)

        out = build_job.submit_build_env_job(
            "vibeqc-dev", state, host="localhost", queue_dir=qd, jobs_dir=jd,
        )

        assert out.action == "submitted"
        spec = JobSpec.read(next(iter(qd.glob("*.json"))))
        assert spec.mem_mb is not None
        assert spec.mem_mb < 51218


@pytest.mark.no_autopatch_branch_check  # the reattach must really run
def test_rollback_restores_the_pinned_head_not_the_stale_branch(
    tmp_path: Path,
) -> None:
    """A failed build must restore what the env was SERVING.

    The rollback baseline used to be captured after
    `_reattach_clean_detached_checkout_for_update`, which runs
    `git checkout <branch>` and moves the working tree. A pinned fleet never
    advances the local branch ref -- every `--expected-sha` update checks out a
    detached HEAD -- so the baseline was wherever the branch was last left, not
    what was running.

    Localhost, 2026-08-01: a transient build failure rolled a live v0.15.106
    checkout back to v0.15.48, months old, while the untouched `.dist-info` kept
    reporting the newer version. Silent wrong-version execution.
    """
    build_sh = "#!/bin/bash\nexit 1\n"  # build always fails -> rollback
    work, sha_a, sha_b = _make_tracking_repos(tmp_path, build_sh=build_sh)

    # The env is SERVING sha_b, pinned as a detached HEAD, while the local
    # `main` ref still points at sha_a -- exactly the pinned-fleet shape.
    _git(work, "fetch", "origin")  # the clone needs sha_b locally first
    _git(work, "checkout", "--detach", sha_b)
    assert _head(work) == sha_b
    _git(work, "branch", "-f", "main", sha_a)
    assert admin._capture_checkout_state(work) == (sha_b, None)
    assert admin._capture_local_branch_tip(work, "main")[:2] == (0, sha_a)

    result = admin._do_update_work(
        "vibeqc-dev",
        _prog(work, update_script="build.sh", import_check="json"),
    )

    # The property under test is the baseline, not the build's rc (which
    # the conftest build-runner stub owns).
    assert result.pre_update_sha == sha_b, (
        "the rollback baseline must be the SERVING commit, not the stale "
        "branch ref the reattach moves to"
    )
    assert admin._capture_checkout_state(work) == (sha_b, None)
    assert admin._capture_local_branch_tip(work, "main")[:2] == (0, sha_a)


def test_rollback_baseline_is_unaffected_when_head_is_already_attached(
    tmp_path: Path,
) -> None:
    """The ordinary case: no detach, so the reattach is a no-op and the
    baseline is the same either way."""
    build_sh = "#!/bin/bash\nexit 1\n"
    work, sha_a, _sha_b = _make_tracking_repos(tmp_path, build_sh=build_sh)

    assert _head(work) == sha_a

    result = admin._do_update_work(
        "vibeqc-dev",
        _prog(work, update_script="build.sh", import_check="json"),
    )

    assert result.pre_update_sha == sha_a
