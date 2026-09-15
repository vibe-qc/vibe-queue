"""Unit tests for the dispatcher seam (vq/dispatch.py).

Covers the platform-agnostic process-lifecycle surface of
:class:`LocalDispatcher` — launch, poll, terminate, kill, wait — plus the
:class:`DispatchError` contract. The daemon's integration with the dispatcher
(start / reconcile / watchdog) is exercised by the existing daemon test
suite; these tests pin the local-process mechanism in isolation. The separate
``SchedulerDispatcher`` contract has its own test module.
"""

from __future__ import annotations

import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import IO

import pytest

from vq.dispatch import DispatchError, LocalDispatcher


@pytest.fixture
def logs(tmp_path: Path) -> Iterator[tuple[IO[bytes], IO[bytes]]]:
    out = (tmp_path / "stdout.log").open("wb")
    err = (tmp_path / "stderr.log").open("wb")
    yield out, err
    out.close()
    err.close()


def _py(code: str) -> list[str]:
    return [sys.executable, "-c", code]


def test_launch_returns_handle_and_poll_reports_exit_code(
    tmp_path: Path, logs: tuple[IO[bytes], IO[bytes]]
) -> None:
    out, err = logs
    d = LocalDispatcher()
    handle = d.launch(
        run_command=_py("import sys; sys.exit(7)"),
        cwd=tmp_path,
        env={},
        stdout_fh=out,
        stderr_fh=err,
    )
    # Block for the exit, then poll must surface rc=7.
    assert d.wait(handle, timeout=10) is True
    assert d.poll(handle) == 7


def test_poll_is_none_while_running(tmp_path: Path, logs: tuple[IO[bytes], IO[bytes]]) -> None:
    out, err = logs
    d = LocalDispatcher()
    handle = d.launch(
        run_command=_py("import time; time.sleep(30)"),
        cwd=tmp_path,
        env={},
        stdout_fh=out,
        stderr_fh=err,
    )
    try:
        assert d.poll(handle) is None
    finally:
        d.kill(handle)
        assert d.wait(handle, timeout=10) is True


def test_terminate_stops_a_running_job(tmp_path: Path, logs: tuple[IO[bytes], IO[bytes]]) -> None:
    out, err = logs
    d = LocalDispatcher()
    handle = d.launch(
        run_command=_py("import time; time.sleep(30)"),
        cwd=tmp_path,
        env={},
        stdout_fh=out,
        stderr_fh=err,
    )
    d.terminate(handle)
    assert d.wait(handle, timeout=10) is True
    # POSIX: signalled processes report a non-None (negative) exit code.
    assert d.poll(handle) is not None


def test_kill_stops_a_running_job(tmp_path: Path, logs: tuple[IO[bytes], IO[bytes]]) -> None:
    out, err = logs
    d = LocalDispatcher()
    handle = d.launch(
        run_command=_py("import time; time.sleep(30)"),
        cwd=tmp_path,
        env={},
        stdout_fh=out,
        stderr_fh=err,
    )
    d.kill(handle)
    assert d.wait(handle, timeout=10) is True
    assert d.poll(handle) is not None


def test_wait_returns_false_on_timeout(tmp_path: Path, logs: tuple[IO[bytes], IO[bytes]]) -> None:
    out, err = logs
    d = LocalDispatcher()
    handle = d.launch(
        run_command=_py("import time; time.sleep(30)"),
        cwd=tmp_path,
        env={},
        stdout_fh=out,
        stderr_fh=err,
    )
    try:
        start = time.monotonic()
        assert d.wait(handle, timeout=0.2) is False
        # It really blocked for ~the timeout rather than returning instantly.
        assert time.monotonic() - start >= 0.15
    finally:
        d.kill(handle)
        assert d.wait(handle, timeout=10) is True


def test_terminate_and_kill_are_noops_on_a_finished_job(
    tmp_path: Path, logs: tuple[IO[bytes], IO[bytes]]
) -> None:
    out, err = logs
    d = LocalDispatcher()
    handle = d.launch(
        run_command=_py("import sys; sys.exit(0)"),
        cwd=tmp_path,
        env={},
        stdout_fh=out,
        stderr_fh=err,
    )
    assert d.wait(handle, timeout=10) is True
    # ProcessLookupError is swallowed — these must not raise on a dead job.
    d.terminate(handle)
    d.kill(handle)
    assert d.poll(handle) == 0


def test_launch_raises_dispatch_error_for_missing_executable(
    tmp_path: Path, logs: tuple[IO[bytes], IO[bytes]]
) -> None:
    out, err = logs
    d = LocalDispatcher()
    with pytest.raises(DispatchError):
        d.launch(
            run_command=["/nonexistent/vq-dispatch-test-binary"],
            cwd=tmp_path,
            env={},
            stdout_fh=out,
            stderr_fh=err,
        )


def test_launch_runs_in_given_cwd_and_env(
    tmp_path: Path, logs: tuple[IO[bytes], IO[bytes]]
) -> None:
    out, err = logs
    d = LocalDispatcher()
    marker = tmp_path / "from_child"
    handle = d.launch(
        run_command=_py(
            "import os, pathlib; pathlib.Path(os.environ['VQ_TEST_MARKER']).write_text(os.getcwd())"
        ),
        cwd=tmp_path,
        env={"VQ_TEST_MARKER": str(marker), "PATH": "/usr/bin:/bin"},
        stdout_fh=out,
        stderr_fh=err,
    )
    assert d.wait(handle, timeout=10) is True
    assert d.poll(handle) == 0
    # cwd was honoured (resolve both sides — macOS /tmp is a symlink to
    # /private/tmp, so the child's getcwd() is the realpath).
    assert Path(marker.read_text()).resolve() == tmp_path.resolve()
