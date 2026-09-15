"""End-to-end: CLI submit -> daemon dispatches -> job completes -> queue lists it.

These tests run the daemon in-process (calling iterate() in a loop) rather than
spawning a daemon subprocess; that's reserved for step 7 (daemon control verbs).
"""
from __future__ import annotations

import sys
import tarfile
import time
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import paths
from vq.cli import main
from vq.daemon import Daemon
from vq.daemon_control import (
    DaemonAlreadyRunning,
    is_daemon_running,
    start_daemon,
)
from vq.spec import JobSpec, JobState


@pytest.fixture
def state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    return tmp_path


def _submit(args: list[str]) -> str:
    """Invoke the submit CLI and return the jobid printed on stdout."""
    result = CliRunner().invoke(main, ["submit", *args])
    assert result.exit_code == 0, result.output
    return result.output.strip()


def _drive_until_completed(daemon: Daemon, jobids: list[str], timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        daemon.iterate()
        states = [JobSpec.read(paths.spec_path(jid)).state for jid in jobids]
        if all(s in (JobState.COMPLETED, JobState.FAILED) for s in states):
            return
        time.sleep(0.05)
    raise AssertionError(f"jobs did not finish in {timeout}s: states={states}")


def _new_daemon() -> Daemon:
    return Daemon(max_cpus=2, poll_interval=0.05)


class TestSingleFileE2E:
    def test_submit_then_run_then_list(self, state: Path) -> None:
        src = state / "hello.py"
        src.write_text("print('hello from vq')")

        jobid = _submit(["localhost", str(src)])

        spec = JobSpec.read(paths.spec_path(jobid))
        assert spec.state == JobState.PENDING
        assert spec.command == [sys.executable, "hello.py"]

        d = _new_daemon()
        try:
            _drive_until_completed(d, [jobid])
        finally:
            d._close_running_logs()

        spec = JobSpec.read(paths.spec_path(jobid))
        assert spec.state == JobState.COMPLETED
        assert spec.exit_code == 0
        assert "hello from vq" in (Path(spec.cwd) / "stdout.log").read_text()

        list_result = CliRunner().invoke(main, ["queue", "localhost"])
        assert list_result.exit_code == 0
        assert jobid in list_result.output
        assert "completed" in list_result.output


class TestDirE2E:
    def test_dir_submit_runs_with_explicit_command(self, state: Path) -> None:
        src = state / "ws"
        src.mkdir()
        (src / "main.py").write_text("import data; print(data.MSG)")
        (src / "data.py").write_text("MSG = 'from data module'")

        jobid = _submit(["localhost", "-d", str(src), sys.executable, "main.py"])

        d = _new_daemon()
        try:
            _drive_until_completed(d, [jobid])
        finally:
            d._close_running_logs()

        spec = JobSpec.read(paths.spec_path(jobid))
        assert spec.state == JobState.COMPLETED
        assert "from data module" in (Path(spec.cwd) / "stdout.log").read_text()


class TestArchiveE2E:
    def test_tarball_submit_extracts_and_runs(self, state: Path) -> None:
        src = state / "src"
        src.mkdir()
        (src / "run.sh").write_text("#!/bin/sh\necho 'tarball job'\n")
        arc = state / "bundle.tar.gz"
        with tarfile.open(arc, "w:gz") as tf:
            tf.add(src / "run.sh", arcname="run.sh")

        jobid = _submit(["localhost", "-c", str(arc), "bash", "run.sh"])

        d = _new_daemon()
        try:
            _drive_until_completed(d, [jobid])
        finally:
            d._close_running_logs()

        spec = JobSpec.read(paths.spec_path(jobid))
        assert spec.state == JobState.COMPLETED
        assert "tarball job" in (Path(spec.cwd) / "stdout.log").read_text()


class TestMultipleJobs:
    def test_multiple_jobs_run_to_completion_in_fifo(self, state: Path) -> None:
        jobids: list[str] = []
        for i in range(4):
            src = state / f"job_{i}.py"
            src.write_text(f"print('job {i}')")
            jobids.append(_submit(["localhost", str(src)]))
            time.sleep(0.01)

        d = _new_daemon()
        try:
            _drive_until_completed(d, jobids)
        finally:
            d._close_running_logs()

        for jid in jobids:
            spec = JobSpec.read(paths.spec_path(jid))
            assert spec.state == JobState.COMPLETED
            assert spec.exit_code == 0


class TestFailureAndQueueDisplay:
    def test_failing_job_marked_failed_and_visible_in_queue(self, state: Path) -> None:
        src = state / "boom.py"
        src.write_text("import sys; sys.exit(7)")

        jobid = _submit(["localhost", str(src)])

        d = _new_daemon()
        try:
            _drive_until_completed(d, [jobid])
        finally:
            d._close_running_logs()

        spec = JobSpec.read(paths.spec_path(jobid))
        assert spec.state == JobState.FAILED
        assert spec.exit_code == 7

        result = CliRunner().invoke(main, ["queue", "localhost"])
        assert result.exit_code == 0
        assert jobid in result.output
        assert "failed" in result.output


class TestQueueOrdering:
    def test_running_appears_above_completed(self, state: Path) -> None:
        # Submit a fast job and let it complete
        fast = state / "fast.py"
        fast.write_text("print('done')")
        completed_id = _submit(["localhost", str(fast)])
        d = _new_daemon()
        try:
            _drive_until_completed(d, [completed_id])
            # Now submit a slow job; iterate once so it goes RUNNING but doesn't finish
            slow = state / "slow.py"
            slow.write_text("import time; time.sleep(2)")
            running_id = _submit(["localhost", str(slow)])
            d.iterate()
            spec = JobSpec.read(paths.spec_path(running_id))
            assert spec.state == JobState.RUNNING
            result = CliRunner().invoke(main, ["queue", "localhost"])
            assert result.exit_code == 0
            running_idx = result.output.index(running_id)
            completed_idx = result.output.index(completed_id)
            assert running_idx < completed_idx
        finally:
            for rj in d._running.values():
                rj.popen.kill()
                rj.popen.wait(timeout=2)
                rj.close_logs()


class TestStatusE2E:
    def test_status_shows_stdout_after_completion(self, state: Path) -> None:
        src = state / "hello.py"
        src.write_text("print('hello world')")
        jobid = _submit(["localhost", str(src)])

        d = _new_daemon()
        try:
            _drive_until_completed(d, [jobid])
        finally:
            d._close_running_logs()

        result = CliRunner().invoke(main, ["status", "localhost", jobid])
        assert result.exit_code == 0
        assert "completed" in result.output
        assert "exit_code:    0" in result.output
        assert "hello world" in result.output


class TestDaemonProcessE2E:
    """v0.6.0: `vq daemon start` was removed (only the systemd-user
    unit is supported in production), but the underlying
    ``daemon_control.start_daemon`` Python API is retained for
    end-to-end test coverage of the spawn / pidfile / stop cycle.
    These tests now exercise the API directly instead of through
    the removed CLI verb."""

    def test_full_lifecycle(self, state: Path) -> None:
        runner = CliRunner()
        try:
            result = start_daemon(max_cpus=2, poll_interval=0.1)
            assert result.pid > 0
            assert is_daemon_running()

            # Daemon should be reachable via the status verb
            status = runner.invoke(main, ["daemon", "status"])
            assert status.exit_code == 0
            assert "running" in status.output

            # Submit a job, wait for the real daemon (not in-process iterate) to run it
            src = state / "hello.py"
            src.write_text("print('from real daemon')")
            sub = runner.invoke(main, ["submit", "localhost", str(src)])
            assert sub.exit_code == 0, sub.output
            jobid = sub.output.strip()

            deadline = time.monotonic() + 15.0
            spec = JobSpec.read(paths.spec_path(jobid))
            while time.monotonic() < deadline:
                spec = JobSpec.read(paths.spec_path(jobid))
                if spec.state in (JobState.COMPLETED, JobState.FAILED):
                    break
                time.sleep(0.1)
            assert spec.state == JobState.COMPLETED, f"final state: {spec.state}"
            assert "from real daemon" in (Path(spec.cwd) / "stdout.log").read_text()

            stop = runner.invoke(main, ["daemon", "stop"])
            assert stop.exit_code == 0
            assert "daemon stopped" in stop.output
            assert not is_daemon_running()
        finally:
            # Belt-and-suspenders cleanup so we never leak a daemon between tests
            if is_daemon_running():
                runner.invoke(main, ["daemon", "stop"])

    def test_status_when_not_running(self, state: Path) -> None:
        result = CliRunner().invoke(main, ["daemon", "status"])
        assert result.exit_code == 0
        assert "not running" in result.output

    def test_stop_when_not_running(self, state: Path) -> None:
        result = CliRunner().invoke(main, ["daemon", "stop"])
        assert result.exit_code == 0
        assert "not running" in result.output

    def test_double_start_rejected(self, state: Path) -> None:
        runner = CliRunner()
        try:
            first = start_daemon(poll_interval=0.1)
            assert first.pid > 0
            with pytest.raises(DaemonAlreadyRunning):
                start_daemon(poll_interval=0.1)
        finally:
            if is_daemon_running():
                runner.invoke(main, ["daemon", "stop"])

    def test_cli_start_was_removed(self, state: Path) -> None:
        """v0.6.0: the CLI verb itself raises with the recovery
        recipe pointing at the systemd-user unit. Operators
        running scripts that called `vq daemon start` see the
        clear migration path."""
        result = CliRunner().invoke(
            main, ["daemon", "start", "--poll-interval", "0.1"],
        )
        assert result.exit_code != 0
        assert "removed in v0.6.0" in result.output
        assert "vq-daemon.service" in result.output
        assert "lifecycle.md" in result.output


class TestKillE2E:
    def test_kill_pending_then_daemon_skips_dispatch(self, state: Path) -> None:
        src = state / "long.py"
        src.write_text("import time; time.sleep(30)")
        jobid = _submit(["localhost", str(src)])

        kill_result = CliRunner().invoke(main, ["kill", "localhost", jobid])
        assert kill_result.exit_code == 0

        d = _new_daemon()
        d.iterate()  # would dispatch, but state is KILLED now
        assert jobid not in d._running

        spec = JobSpec.read(paths.spec_path(jobid))
        assert spec.state == JobState.KILLED

    def test_kill_running_keeps_killed_state(self, state: Path) -> None:
        src = state / "long.py"
        src.write_text("import time; time.sleep(30)")
        jobid = _submit(["localhost", str(src)])

        d = _new_daemon()
        try:
            d.iterate()  # dispatch
            assert JobSpec.read(paths.spec_path(jobid)).state == JobState.RUNNING

            kill_result = CliRunner().invoke(main, ["kill", "localhost", jobid])
            assert kill_result.exit_code == 0
            assert "SIGTERM" in kill_result.output or "not found" in kill_result.output

            # Drive daemon until it reaps the dead child
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline and jobid in d._running:
                d.iterate()
                time.sleep(0.05)
        finally:
            for rj in d._running.values():
                rj.popen.kill()
                rj.popen.wait(timeout=2)
                rj.close_logs()

        spec = JobSpec.read(paths.spec_path(jobid))
        assert spec.state == JobState.KILLED  # daemon did not overwrite


class TestDetachedDaemonE2E:
    """Spawn a real detached daemon subprocess (the path `vq daemon start`
    actually goes through) and verify it dispatches a job end-to-end.

    Also acts as a regression test for the duplicate-log-lines bug: when
    the spawned daemon's stderr is redirected into the same log_file the
    in-process FileHandler writes to, an unguarded StreamHandler would
    cause every record to land twice.
    """

    def test_detached_daemon_runs_job_with_clean_log(self, state: Path) -> None:
        from vq import daemon_control

        src = state / "hi.py"
        src.write_text("print('detached hi')")
        jobid = _submit(["localhost", str(src)])

        result = daemon_control.start_daemon(poll_interval=0.1, spawn_timeout=10.0)
        try:
            deadline = time.monotonic() + 15.0
            spec = JobSpec.read(paths.spec_path(jobid))
            while time.monotonic() < deadline:
                spec = JobSpec.read(paths.spec_path(jobid))
                if spec.state in (JobState.COMPLETED, JobState.FAILED):
                    break
                time.sleep(0.1)
            assert spec.state == JobState.COMPLETED, f"final state {spec.state}"
            assert spec.exit_code == 0
            assert "detached hi" in (Path(spec.cwd) / "stdout.log").read_text()
        finally:
            daemon_control.stop_daemon(timeout=5.0)

        log_lines = result.log_file.read_text().splitlines()
        # Regression: each daemon log message must appear exactly once.
        # The bug emitted every line twice because the spawned process'
        # stderr was redirected into log_file AND a StreamHandler also
        # routed records there.
        dispatch = [line for line in log_lines if "started job" in line and jobid in line]
        assert len(dispatch) == 1, (
            f"expected 1 dispatch line for {jobid}, got {len(dispatch)}; "
            f"full log:\n{result.log_file.read_text()}"
        )
        finish = [line for line in log_lines if "finished" in line and jobid in line]
        assert len(finish) == 1, (
            f"expected 1 finish line for {jobid}, got {len(finish)}; "
            f"full log:\n{result.log_file.read_text()}"
        )
