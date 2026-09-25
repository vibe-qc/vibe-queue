"""A due auto-cleanup runs even when the daemon is never idle (#90).

The in-process sweep still defers while jobs are active, so it cannot delay
child reaping. Instead the daemon starts one separate ``vq cleanup
--auto-run-due`` process and only polls it.
"""
from __future__ import annotations

import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import cleanup, config, daemon, paths
from vq.cli import main
from vq.spec import JobSpec, JobState


@pytest.fixture
def state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "config.toml").write_text(
        'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
    )
    paths.state_root().mkdir(parents=True, exist_ok=True)
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("vq.cgroup.available", lambda: False)
    return tmp_path


def _old_terminal_spec(jobid: str) -> None:
    workspace = paths.jobs_dir() / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    (workspace / "out.txt").write_text("x\n")
    JobSpec(
        id=jobid, command=["true"], cwd=str(workspace), cpus=1,
        state=JobState.COMPLETED, exit_code=0,
        finished_at=(datetime.now(UTC) - timedelta(days=100)).isoformat(),
    ).write(paths.spec_path(jobid))


def _due_policy() -> None:
    cleanup.write_auto_cleanup_policy(
        cleanup.AutoCleanupPolicy(delete_after_seconds=86400 * 30)
    )


class _Child:
    def __init__(self) -> None:
        self.returncode: int | None = None

    def poll(self) -> int | None:
        return self.returncode


def _busy_daemon(monkeypatch: pytest.MonkeyPatch) -> tuple[daemon.Daemon, list[_Child]]:
    d = daemon.Daemon(max_cpus=4, max_jobs=1, max_mem_mb=None)
    d._running = {"live-job": object()}  # type: ignore[assignment]
    spawned: list[_Child] = []

    def spawn() -> _Child:
        spawned.append(_Child())
        return spawned[-1]

    monkeypatch.setattr(d, "_spawn_auto_cleanup_child", spawn)
    in_process: list[bool] = []
    monkeypatch.setattr(
        "vq.cleanup.run_auto_cleanup_pass",
        lambda *a, **k: in_process.append(True),
    )
    d._in_process_calls = in_process  # type: ignore[attr-defined]
    return d, spawned


def test_busy_daemon_starts_one_out_of_process_sweep(state, monkeypatch) -> None:
    _due_policy()
    d, spawned = _busy_daemon(monkeypatch)

    d._maybe_auto_cleanup()
    d._maybe_auto_cleanup()

    assert len(spawned) == 1
    assert d._in_process_calls == []  # type: ignore[attr-defined]


def test_a_finished_child_is_not_restarted_before_the_gap(state, monkeypatch) -> None:
    _due_policy()
    d, spawned = _busy_daemon(monkeypatch)
    d._maybe_auto_cleanup()
    spawned[0].returncode = 1  # died before stamping: the policy is still due

    d._maybe_auto_cleanup()
    assert len(spawned) == 1

    monkeypatch.setattr(daemon, "_AUTO_CLEANUP_CHILD_GAP_SECONDS", 0.0)
    d._maybe_auto_cleanup()
    assert len(spawned) == 2


def test_nothing_starts_when_no_sweep_is_due(state, monkeypatch) -> None:
    cleanup.write_auto_cleanup_policy(cleanup.AutoCleanupPolicy(
        delete_after_seconds=86400 * 30, last_run_at=datetime.now(UTC).isoformat(),
    ))
    d, spawned = _busy_daemon(monkeypatch)

    d._maybe_auto_cleanup()

    assert spawned == []


def test_an_idle_daemon_does_not_sweep_over_a_running_child(state, monkeypatch) -> None:
    _due_policy()
    d, spawned = _busy_daemon(monkeypatch)
    d._maybe_auto_cleanup()
    d._running = {}  # type: ignore[assignment]

    d._maybe_auto_cleanup()
    assert d._in_process_calls == []  # type: ignore[attr-defined]

    spawned[0].returncode = 0
    d._maybe_auto_cleanup()
    assert d._in_process_calls == [True]  # type: ignore[attr-defined]


def test_a_held_sweep_lock_skips_without_stamping(state) -> None:
    _old_terminal_spec("lockedjob001")
    _due_policy()
    policy = cleanup.read_auto_cleanup_policy()
    assert policy is not None

    with cleanup._auto_cleanup_sweep_lock() as held:
        assert held
        counts = cleanup.run_auto_cleanup_pass(policy)

    assert counts == {"locked": 1}
    assert paths.spec_path("lockedjob001").exists()
    reread = cleanup.read_auto_cleanup_policy()
    assert reread is not None and reread.last_run_at is None


def test_cli_runs_only_a_due_sweep(state) -> None:
    _old_terminal_spec("clijob000001")
    _due_policy()

    first = CliRunner().invoke(main, ["cleanup", "localhost", "--auto-run-due"])
    second = CliRunner().invoke(main, ["cleanup", "localhost", "--auto-run-due"])

    assert first.exit_code == 0, first.output
    assert json.loads(first.output)["due"] is True
    assert not paths.spec_path("clijob000001").exists()
    assert json.loads(second.output) == {"counts": {}, "due": False}


def test_busy_daemon_child_really_sweeps(state) -> None:
    _old_terminal_spec("realchild001")
    _due_policy()
    d = daemon.Daemon(max_cpus=4, max_jobs=1, max_mem_mb=None)
    d._running = {"live-job": object()}  # type: ignore[assignment]

    d._maybe_auto_cleanup()
    child = d._auto_cleanup_child
    assert child is not None
    deadline = time.monotonic() + 120
    while child.poll() is None:
        assert time.monotonic() < deadline, "cleanup child did not finish"
        time.sleep(0.1)

    log = (paths.state_root() / "auto-cleanup-child.log").read_text()
    assert child.returncode == 0, log
    assert not paths.spec_path("realchild001").exists()
    d._maybe_auto_cleanup()
    assert d._auto_cleanup_child is None
