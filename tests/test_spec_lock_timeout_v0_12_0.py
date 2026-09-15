"""Tests for v0.12.0 spec_lock timeout: ``paths.spec_lock(timeout=...)`` and
the ``vq kill`` fail-fast path, so a wedged lock holder no longer hangs the
CLI forever (the host_e wedged-build incident).
"""
from __future__ import annotations

import fcntl
import os
import time
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from vq import config, paths
from vq.cli import main
from vq.spec import JobSpec, JobState


def _hold_lock(spec_path: Path) -> int:
    """Take the spec's flock on a fresh fd and return it (caller closes)."""
    lock_path = paths.spec_lock_path(spec_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o666)
    fcntl.flock(fd, fcntl.LOCK_EX)
    return fd


class TestSpecLockTimeout:
    def test_timeout_raises_on_held_lock(self, tmp_path: Path) -> None:
        sp = tmp_path / "held.json"
        fd = _hold_lock(sp)
        try:
            t0 = time.monotonic()
            with (
                pytest.raises(paths.SpecLockTimeout, match="could not acquire"),
                paths.spec_lock(sp, timeout=0.3),
            ):
                pass
            # Waited roughly the timeout: not forever, not instant.
            assert 0.2 <= time.monotonic() - t0 < 3.0
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def test_timeout_acquires_when_free(self, tmp_path: Path) -> None:
        sp = tmp_path / "free.json"
        entered = False
        with paths.spec_lock(sp, timeout=0.3):
            entered = True
        assert entered

    def test_default_none_still_blocks_and_acquires(self, tmp_path: Path) -> None:
        # The blocking default that the daemon's hot-path writers rely on.
        sp = tmp_path / "blk.json"
        with paths.spec_lock(sp):
            pass

    def test_is_a_timeout_error(self) -> None:
        assert issubclass(paths.SpecLockTimeout, TimeoutError)


@pytest.fixture
def cli_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "config.toml").write_text('default_host = "localhost"\n')
    return tmp_path


class TestKillFailsFast:
    def test_kill_surfaces_lock_timeout_not_hang(self, cli_state: Path) -> None:
        # A RUNNING spec whose lock is held by another fd: vq kill must fail
        # fast with the timeout message rather than block on flock forever.
        queue = paths.queue_dir()
        queue.mkdir(parents=True, exist_ok=True)
        ws = cli_state / "ws"
        ws.mkdir(parents=True, exist_ok=True)
        jobid = "deadbeef0001"
        spec = JobSpec(
            id=jobid,
            command=["sleep", "1"],
            cwd=str(ws),
            cpus=1,
            submitter="t",
            state=JobState.RUNNING,
            pid=999999,
        )
        spec_path = queue / f"{jobid}.json"
        spec.write(spec_path)
        fd = _hold_lock(spec_path)
        try:
            with patch("vq.kill._KILL_LOCK_TIMEOUT", 0.3):
                result = CliRunner().invoke(main, ["kill", "localhost", jobid])
            assert result.exit_code != 0, result.output
            assert "could not acquire the spec lock" in result.output
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
