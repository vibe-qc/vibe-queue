"""#67: daemon readiness does not wait for the startup reattachment pass.

``Daemon.run`` reconciles every spec on disk and reattaches every live
scheduler job -- one SSH probe per job -- before the daemon is otherwise
useful. On a busy driver that pass runs for minutes: a reported 175 live
scheduler jobs over an 18,013-spec queue took 9 min 51 s from ``daemon
starting`` to ``RPC server listening``. Until v0.26.10 the RPC socket was
published *after* the pass, so:

* every local ``vq`` call failed for the whole window, and
* ``vq admin update``'s post-restart readiness window -- at most
  ``DAEMON_HEALTH_TIMEOUT_MAX_SECONDS`` -- expired on a daemon that was
  healthy and merely busy, and a valid install was rolled back. No formula
  could rescue it: the ceiling is a hard cap, so a driver whose honest
  startup exceeds it could never be updated without the
  ``VQ_DAEMON_HEALTH_TIMEOUT`` override.

Readiness and provenance need nothing from the pass -- the source SHA and
tree digest are captured from the installed package. So the socket now opens
first, and these tests pin the consequences:

* the RPC server is started before the pass begins,
* a ping is answered *while* the pass is still in flight,
* the updater's own provenance probe passes in that same window, and
* a stop delivered during the pass still withdraws the socket before exit,
  which is the #53 guarantee restated for the new order: a reader that
  reaches the socket is talking to a daemon that came up and was not asked
  to leave.

The pass here is held open by an event rather than by a large queue: what is
under test is the ordering, not the wall-clock cost of a real driver's walk.
"""
from __future__ import annotations

import contextlib
import os
import shutil
import signal
import tempfile
import threading
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from vq import __version__, admin, paths, rpc
from vq.daemon import Daemon

# Bound every wait: a hung phase must fail the test, not hang the suite.
WAIT_SECONDS = 20.0


@pytest.fixture
def short_state(monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """A state root short enough for the ~104-byte AF_UNIX path limit.

    Same shape as ``tests/test_rpc.py`` and ``tests/test_daemon_ping.py``:
    these tests bind the real socket, so the sandbox's ordinary deep tmp
    path would fail the bind on macOS before anything under test ran.
    """
    tmpdir = Path(
        tempfile.mkdtemp(
            prefix="vqready-",
            dir=os.environ.get("VQ_TEST_SHORT_TMPDIR"),
        )
    )
    (tmpdir / "state").mkdir()
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmpdir / "state"))
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def daemon(tmp_path: Path, short_state: Path) -> Iterator[Daemon]:
    d = Daemon(
        max_cpus=4,
        poll_interval=0.05,
        queue_dir=tmp_path / "queue",
        jobs_dir=tmp_path / "jobs",
    )
    d.queue_dir.mkdir(parents=True, exist_ok=True)
    d.jobs_dir.mkdir(parents=True, exist_ok=True)
    yield d
    with contextlib.suppress(Exception):
        d._stop_rpc_server()


@pytest.fixture
def restored_signal_handlers() -> Iterator[None]:
    """``Daemon.run`` mutates process-global signal dispositions."""
    watched = [signal.SIGTERM, signal.SIGINT, signal.SIGHUP]
    saved = {sig: signal.getsignal(sig) for sig in watched}
    try:
        yield
    finally:
        for sig, handler in saved.items():
            if handler is not None:
                with contextlib.suppress(ValueError, OSError, TypeError):
                    signal.signal(sig, handler)


def _probe_during_the_startup_pass(
    daemon: Daemon,
    probe: Callable[[], Any],
) -> Any:
    """Run ``daemon.run()`` and call ``probe`` while its pass is in flight.

    ``run()`` stays on the main thread because it installs signal handlers,
    which ``signal.signal`` permits nowhere else; the probe is what moves to a
    worker. The pass is held open until the probe returns, then completes for
    real and asks the daemon to stop, so ``run()`` returns without entering
    the main loop.
    """
    pass_started = threading.Event()
    probe_done = threading.Event()
    outcome: dict[str, Any] = {}
    real_pass = daemon._reattach_or_interrupt_at_startup

    def _held_pass() -> None:
        pass_started.set()
        probe_done.wait(WAIT_SECONDS)
        real_pass()
        daemon._stop = True

    def _run_probe() -> None:
        try:
            if not pass_started.wait(WAIT_SECONDS):
                outcome["error"] = "the startup reattachment pass never began"
                return
            outcome["value"] = probe()
        except BaseException as exc:  # noqa: BLE001 - reported as a failure
            outcome["error"] = f"{type(exc).__name__}: {exc}"
        finally:
            probe_done.set()

    daemon._reattach_or_interrupt_at_startup = _held_pass  # type: ignore[method-assign]
    worker = threading.Thread(target=_run_probe, name="vq-test-probe", daemon=True)
    worker.start()
    try:
        daemon.run()
    finally:
        probe_done.set()
        worker.join(timeout=WAIT_SECONDS)
    assert "error" not in outcome, outcome["error"]
    assert not worker.is_alive(), "the probe thread outlived the daemon"
    return outcome["value"]


class TestRpcIsPublishedBeforeTheReattachmentPass:
    def test_the_rpc_server_starts_before_the_pass_begins(
        self,
        daemon: Daemon,
        restored_signal_handlers: None,
    ) -> None:
        """Ordering, stated directly and cheaply."""
        order: list[str] = []
        real_start = daemon._start_rpc_server
        real_pass = daemon._reattach_or_interrupt_at_startup

        def _note_start(advertised: Any) -> None:
            order.append("rpc")
            real_start(advertised)

        def _note_pass() -> None:
            order.append("reattachment pass")
            real_pass()
            daemon._stop = True

        daemon._start_rpc_server = _note_start  # type: ignore[method-assign]
        daemon._reattach_or_interrupt_at_startup = _note_pass  # type: ignore[method-assign]
        daemon.run()

        assert order == ["rpc", "reattachment pass"], (
            "the daemon published its RPC socket only after the startup "
            "reattachment pass; on a busy driver that is minutes of "
            "unanswered readiness"
        )

    def test_a_ping_is_answered_while_the_pass_is_still_running(
        self,
        daemon: Daemon,
        restored_signal_handlers: None,
    ) -> None:
        """The real socket, over a real connection, mid-pass."""
        envelope = _probe_during_the_startup_pass(daemon, rpc.ping)

        assert envelope is not None, (
            "no answer on the daemon socket while the startup reattachment "
            "pass was in flight; an updater polling for readiness here sees "
            "a daemon that never came up"
        )
        assert envelope["version"] == __version__
        assert envelope["multi_user"] is False

    def test_the_updater_provenance_probe_passes_during_the_pass(
        self,
        daemon: Daemon,
        monkeypatch: pytest.MonkeyPatch,
        restored_signal_handlers: None,
    ) -> None:
        """The lane that actually rolled a valid install back.

        ``_verify_restarted_daemon`` is what ``vq admin update`` runs after
        the restart. Its window is bounded here so the failing case reports
        in seconds instead of sitting out the full allowance.
        """
        expected = admin.running_source_sha(Path(rpc.__file__))
        if expected is None:
            pytest.skip("no source provenance available for this install")
        monkeypatch.setenv("VQ_DAEMON_HEALTH_TIMEOUT", "8")

        provenance = _probe_during_the_startup_pass(
            daemon,
            lambda: admin._verify_restarted_daemon(expected),
        )

        assert provenance.verified is True, provenance.detail
        assert provenance.actual_sha == expected.lower()


class TestAStopInsideThePassStillWithdrawsTheSocket:
    def test_the_socket_is_gone_once_run_returns(
        self,
        daemon: Daemon,
        restored_signal_handlers: None,
    ) -> None:
        """#53's guarantee, restated for the new order.

        The socket may now answer during the pass, so the daemon must take it
        down itself on the way out -- otherwise a stale socket file outlives
        the process and a reader mistakes it for a daemon that stayed up.
        """
        answered = _probe_during_the_startup_pass(daemon, rpc.ping)
        assert answered is not None

        # `_held_pass` set the stop flag, so run() left through the
        # stopped-during-startup branch rather than the main loop.
        assert daemon._stop is True
        assert not rpc.socket_path(multi_user=False).exists()
        assert rpc.ping() is None
