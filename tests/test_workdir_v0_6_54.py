"""v0.6.54: per-job workdir + auto-cleanup of stale workdirs.

The "don't write to the git repo on host_d/host_a" story. Chats that
need scratch space (basis-opt convergence runs, vqfetch downloads,
experimental example runs that would otherwise dirty
/home/USER/gitlab/vibeqc-dev/) get a daemon-created, per-job
workdir whose path is injected via ``$VQ_WORKDIR``. Operator reads
results back via `vq status` (path) + `ssh HOST cat ...` or a
future `vq fetch --workdir`.

Coverage shape:

* `TestSpecRoundtrip`         workdir + clean_workdir_on_terminal
                              fields default sensibly, roundtrip,
                              pre-v0.6.54 specs read clean.
* `TestPaths`                 workdir_root / workdir_for / user_*
                              live at the right layout.
* `TestDaemonDispatch`        _start_job creates workdir, sets
                              spec.workdir, injects VQ_WORKDIR into
                              child env. Composes with the v0.6.52
                              VQ_ARRAY_* env injection.
* `TestTerminalCleanup`       clean_workdir_on_terminal=True →
                              daemon rmtrees the workdir on
                              terminal transition; False → workdir
                              survives.
* `TestStaleSweep`            cleanup.run_auto_cleanup_pass sweeps
                              workdirs older than
                              workdir_max_age_seconds; younger
                              workdirs survive; disabled-by-default
                              (None) is a no-op.
* `TestStatusDisplay`         vq status renders the workdir line
                              with the cleanup-mode annotation;
                              hidden when workdir is None.
* `TestCLI`                   `vq submit --clean-tmp` sets the
                              spec field; status renders the line.
"""
from __future__ import annotations

import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from vq import cleanup, config, paths
from vq.cli import main
from vq.daemon import Daemon
from vq.spec import JobSpec, JobState
from vq.status import show_status

# ===========================================================================
# Spec roundtrip
# ===========================================================================


class TestSpecRoundtrip:
    def test_defaults(self) -> None:
        s = JobSpec(id="a", command=["true"], cwd=".", cpus=1)
        assert s.workdir is None
        assert s.clean_workdir_on_terminal is False

    def test_roundtrip(self, tmp_path: Path) -> None:
        s = JobSpec(
            id="a", command=["true"], cwd=".", cpus=1,
            workdir="/some/abs/path", clean_workdir_on_terminal=True,
        )
        p = tmp_path / "a.json"
        s.write(p)
        loaded = JobSpec.read(p)
        assert loaded.workdir == "/some/abs/path"
        assert loaded.clean_workdir_on_terminal is True

    def test_pre_v0_6_54_reads_clean(self, tmp_path: Path) -> None:
        p = tmp_path / "old.json"
        p.write_text(json.dumps({
            "id": "old1", "command": ["true"],
            "cwd": str(tmp_path), "cpus": 1,
        }))
        loaded = JobSpec.read(p)
        assert loaded.workdir is None
        assert loaded.clean_workdir_on_terminal is False


# ===========================================================================
# Paths
# ===========================================================================


class TestPaths:
    def test_single_user_workdir_layout(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        assert paths.workdir_root() == tmp_path / "state" / "workdirs"
        assert paths.workdir_for("abc123") == (
            tmp_path / "state" / "workdirs" / "abc123"
        )

    def test_multi_user_workdir_layout(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "mu"))
        assert paths.user_workdir_root(1000) == (
            tmp_path / "mu" / "users" / "1000" / "workdirs"
        )
        assert paths.user_workdir(1000, "abc123") == (
            tmp_path / "mu" / "users" / "1000" / "workdirs" / "abc123"
        )


# ===========================================================================
# Daemon dispatch
# ===========================================================================


def _make_daemon(tmp_path: Path) -> Daemon:
    d = Daemon(
        max_cpus=4,
        poll_interval=0.05,
        queue_dir=tmp_path / "queue",
        jobs_dir=tmp_path / "jobs",
    )
    d.queue_dir.mkdir(parents=True, exist_ok=True)
    d.jobs_dir.mkdir(parents=True, exist_ok=True)
    return d


def _write_spec(
    daemon: Daemon,
    jobid: str,
    *,
    clean_workdir_on_terminal: bool = False,
) -> JobSpec:
    ws = daemon.jobs_dir / jobid
    ws.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(
        id=jobid,
        command=["true"],
        cwd=str(ws),
        cpus=1,
        clean_workdir_on_terminal=clean_workdir_on_terminal,
    )
    spec.write(daemon._spec_path(jobid))
    return spec


class TestDaemonDispatch:
    def test_workdir_created_and_env_injected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        d = _make_daemon(tmp_path)
        spec = _write_spec(d, "job001")

        captured: dict[str, object] = {}

        class FakePopen:
            def __init__(self, *args, **kwargs):  # noqa: ANN
                captured["kwargs"] = kwargs
                self.pid = 12345

            def poll(self):
                return 0

        with patch("vq.daemon.subprocess.Popen", FakePopen), patch(
            "vq.daemon.os.getpgid", return_value=12345,
        ), patch(
            "vq.daemon._read_pid_start_time", return_value=None,
        ):
            d._start_job(spec)

        # spec.workdir now persists the absolute path.
        loaded = JobSpec.read(d._spec_path("job001"))
        assert loaded.workdir is not None
        wd = Path(loaded.workdir)
        assert wd.exists()
        assert wd.is_dir()
        # Lives under the single-user workdir root.
        assert wd.parent == paths.workdir_root()

        # Env injection.
        env = captured["kwargs"]["env"]
        assert env["VQ_WORKDIR"] == str(wd)


# ===========================================================================
# Opt-in terminal cleanup
# ===========================================================================


class TestTerminalCleanup:
    def test_clean_workdir_on_terminal_rmtrees(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        d = _make_daemon(tmp_path)
        wd = paths.workdir_root() / "job001"
        wd.mkdir(parents=True, exist_ok=True)
        (wd / "result.json").write_text('{"ok": true}')

        spec = _write_spec(d, "job001", clean_workdir_on_terminal=True)
        spec.workdir = str(wd)
        spec.write(d._spec_path("job001"))

        d._maybe_cleanup_workdir(spec)

        assert not wd.exists()

    def test_clean_workdir_off_leaves_workdir(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        d = _make_daemon(tmp_path)
        wd = paths.workdir_root() / "job002"
        wd.mkdir(parents=True, exist_ok=True)
        (wd / "result.json").write_text('{"ok": true}')

        spec = _write_spec(d, "job002", clean_workdir_on_terminal=False)
        spec.workdir = str(wd)

        d._maybe_cleanup_workdir(spec)

        assert wd.exists()
        assert (wd / "result.json").exists()

    def test_missing_workdir_is_silent(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Idempotent: calling cleanup when the workdir is already
        gone is a no-op (e.g. operator hand-removed it)."""
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        d = _make_daemon(tmp_path)
        spec = _write_spec(d, "job003", clean_workdir_on_terminal=True)
        spec.workdir = "/nonexistent/never/was"
        # Must not raise.
        d._maybe_cleanup_workdir(spec)


# ===========================================================================
# Stale-workdir auto-cleanup sweep
# ===========================================================================


class TestStaleSweep:
    def test_old_workdir_swept(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        # Two workdirs: one old (mtime 30d ago), one young (now).
        old = paths.workdir_root() / "oldjob"
        old.mkdir(parents=True, exist_ok=True)
        (old / "leftover.txt").write_text("data")
        old_ts = time.time() - 30 * 86400
        import os as _os
        _os.utime(old, (old_ts, old_ts))

        young = paths.workdir_root() / "youngjob"
        young.mkdir(parents=True, exist_ok=True)
        (young / "fresh.txt").write_text("data")

        policy = cleanup.AutoCleanupPolicy(
            enabled=True,
            workdir_max_age_seconds=14 * 86400,  # 14 days
        )
        # Pin the policy so run_auto_cleanup_pass reads it back from
        # the same place it persists to.
        cleanup.write_auto_cleanup_policy(policy)
        counts = cleanup.run_auto_cleanup_pass(
            policy, multi_user=False,
        )

        assert not old.exists(), "old workdir should be swept"
        assert young.exists(), "young workdir should survive"
        assert counts["workdirs_swept"] == 1
        assert counts["workdir_errors"] == 0

    def test_age_swept_workdir_stamps_breadcrumb_on_spec(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """CLEAN-3: sweeping a terminal job's workdir stamps workdir_swept_at
        on its spec, so a later `vq fetch --workdir` can say the workdir was
        age-swept (not removed by --clean-tmp)."""
        from vq.spec import JobSpec, JobState
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        paths.queue_dir().mkdir(parents=True, exist_ok=True)
        jobid = "ageswept0001"
        ws = paths.jobs_dir() / jobid
        ws.mkdir(parents=True, exist_ok=True)
        wd = paths.workdir_root() / jobid
        wd.mkdir(parents=True, exist_ok=True)
        (wd / "scratch.bin").write_text("x")
        # Terminal + finished long ago (older than the cutoff), NOT --clean-tmp.
        JobSpec(
            id=jobid, command=["true"], cwd=str(ws), cpus=1,
            state=JobState.COMPLETED, finished_at="2026-01-01T00:00:00+00:00",
            workdir=str(wd), clean_workdir_on_terminal=False,
        ).write(paths.spec_path(jobid))

        policy = cleanup.AutoCleanupPolicy(
            enabled=True, workdir_max_age_seconds=14 * 86400,
        )
        cleanup.write_auto_cleanup_policy(policy)
        counts = cleanup.run_auto_cleanup_pass(policy, multi_user=False)

        assert counts["workdirs_swept"] == 1
        assert not wd.exists()
        recovered = JobSpec.read(paths.spec_path(jobid))
        assert recovered.workdir_swept_at is not None  # CLEAN-3 breadcrumb

    def test_max_age_none_is_noop(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        old = paths.workdir_root() / "oldjob"
        old.mkdir(parents=True, exist_ok=True)
        import os as _os
        old_ts = time.time() - 365 * 86400
        _os.utime(old, (old_ts, old_ts))

        policy = cleanup.AutoCleanupPolicy(
            enabled=True,
            workdir_max_age_seconds=None,  # disabled
        )
        cleanup.write_auto_cleanup_policy(policy)
        counts = cleanup.run_auto_cleanup_pass(
            policy, multi_user=False,
        )

        # Workdir untouched.
        assert old.exists()
        assert counts["workdirs_swept"] == 0


class TestStaleSweepTerminalGate:
    """Regression for CLEAN-1: the sweep must gate on the owning job's
    state + finished_at (like the archive/delete passes), NOT on the
    workdir's directory mtime. A dir mtime stays frozen at dispatch for a
    long-running job that only appends to existing files / writes into a
    subdir, so the old mtime-only heuristic rmtree'd live scratch.
    """

    def _aged_workdir(self, jobid: str) -> Path:
        """A workdir whose top-level mtime is 30 days old — past the 14d
        cutoff the tests use."""
        import os as _os

        wd = paths.workdir_root() / jobid
        wd.mkdir(parents=True, exist_ok=True)
        (wd / "scratch.bin").write_text("partial results")
        old_ts = time.time() - 30 * 86400
        _os.utime(wd, (old_ts, old_ts))
        return wd

    def _spec(
        self, jobid: str, state: JobState, *, finished_at: str | None = None,
    ) -> None:
        """Write a spec to the location the sweep resolves
        (``paths.spec_path``), which is the env-derived state root the
        module-level sweep reads — not a daemon-overridden queue_dir."""
        ws = paths.workspace_dir(jobid)
        ws.mkdir(parents=True, exist_ok=True)
        spec = JobSpec(id=jobid, command=["true"], cwd=str(ws), cpus=1)
        spec.state = state
        if finished_at is not None:
            spec.finished_at = finished_at
        spec.write(paths.spec_path(jobid))

    def _run_sweep(self, *, multi_user: bool = False) -> dict[str, int]:
        policy = cleanup.AutoCleanupPolicy(
            enabled=True, workdir_max_age_seconds=14 * 86400,
        )
        cleanup.write_auto_cleanup_policy(policy)
        return cleanup.run_auto_cleanup_pass(policy, multi_user=multi_user)

    def test_running_jobs_old_mtime_workdir_survives(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        wd = self._aged_workdir("runjob")
        self._spec("runjob", JobState.RUNNING)  # non-terminal: live job

        counts = self._run_sweep()

        assert wd.exists(), "a RUNNING job's live workdir must never be swept"
        assert counts["workdirs_swept"] == 0

    def test_terminal_job_old_finished_at_is_swept(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        wd = self._aged_workdir("donejob")
        self._spec(
            "donejob", JobState.COMPLETED,
            finished_at=(datetime.now(UTC) - timedelta(days=30)).isoformat(),
        )

        counts = self._run_sweep()

        assert not wd.exists(), "a long-terminal job's workdir should be swept"
        assert counts["workdirs_swept"] == 1

    def test_terminal_job_recent_finished_at_survives(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Old DIR mtime but JUST finished — proves the gate is finished_at,
        # not the directory mtime.
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        wd = self._aged_workdir("freshdone")
        self._spec(
            "freshdone", JobState.COMPLETED,
            finished_at=datetime.now(UTC).isoformat(),
        )

        counts = self._run_sweep()

        assert wd.exists(), "gate is finished_at, not dir mtime"
        assert counts["workdirs_swept"] == 0


# ===========================================================================
# Status display
# ===========================================================================


class TestStatusDisplay:
    def _setup(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> Path:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        cfgdir = tmp_path / "cfg"
        cfgdir.mkdir()
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfgdir))
        (cfgdir / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        paths.queue_dir().mkdir(parents=True, exist_ok=True)
        paths.jobs_dir().mkdir(parents=True, exist_ok=True)
        return tmp_path

    def test_workdir_line_renders_when_set(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._setup(tmp_path, monkeypatch)
        ws = tmp_path / "ws"
        ws.mkdir()
        JobSpec(
            id="a", command=["true"], cwd=str(ws), cpus=1,
            workdir="/var/lib/vq/users/1000/workdirs/a",
            clean_workdir_on_terminal=False,
        ).write(paths.queue_dir() / "a.json")
        out = show_status("localhost", "a")
        assert "workdir:      /var/lib/vq/users/1000/workdirs/a" in out
        assert "lingers until cleanup-sweep" in out

    def test_workdir_clean_mode_annotation(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._setup(tmp_path, monkeypatch)
        ws = tmp_path / "ws2"
        ws.mkdir()
        JobSpec(
            id="b", command=["true"], cwd=str(ws), cpus=1,
            workdir="/tmp/wd",
            clean_workdir_on_terminal=True,
        ).write(paths.queue_dir() / "b.json")
        out = show_status("localhost", "b")
        assert "clean-on-terminal" in out

    def test_workdir_line_omitted_when_unset(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._setup(tmp_path, monkeypatch)
        ws = tmp_path / "ws3"
        ws.mkdir()
        JobSpec(
            id="c", command=["true"], cwd=str(ws), cpus=1,
        ).write(paths.queue_dir() / "c.json")
        out = show_status("localhost", "c")
        assert "workdir:" not in out


# ===========================================================================
# CLI
# ===========================================================================


class TestCLI:
    def test_clean_tmp_flag_sets_spec_field(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        cfgdir = tmp_path / "cfg"
        cfgdir.mkdir()
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfgdir))
        (cfgdir / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        paths.queue_dir().mkdir(parents=True, exist_ok=True)
        paths.jobs_dir().mkdir(parents=True, exist_ok=True)

        src = tmp_path / "x.py"
        src.write_text("print('hi')\n")

        result = CliRunner().invoke(
            main, ["submit", "--clean-tmp", str(src)],
        )
        assert result.exit_code == 0, result.output
        jobid = result.output.strip()
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.clean_workdir_on_terminal is True
