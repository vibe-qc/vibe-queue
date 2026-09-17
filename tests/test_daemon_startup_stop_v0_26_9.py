"""#53: a stop that arrives during the daemon's startup walk is graceful.

``Daemon.run`` reconciles every spec on disk
(:meth:`~vq.daemon.Daemon._reattach_or_interrupt_at_startup`) before its RPC
socket answers. On a driver-sized queue that walk takes minutes. Until
v0.26.9 the SIGTERM/SIGINT handlers were installed *after* it, so for that
whole window a stop hit the default disposition and killed the process
outright -- no shutdown, no log line. A self-update whose health window
expired mid-walk then rolled back by removing a daemon that could not stop
gracefully.

Two halves are pinned here:

* the handlers are in place *before* the walk starts, and
* the walk actually gives up promptly once the flag is set, because
  ``vq daemon stop`` waits 10 s and both service managers escalate to
  SIGKILL on their own timeouts. Noting the signal and then finishing a
  several-minute scan is still an ungraceful death.
"""
from __future__ import annotations

import contextlib
import signal
from collections.abc import Iterator
from pathlib import Path

import pytest

from vq.daemon import Daemon
from vq.spec import JobSpec, JobState


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
        with contextlib.suppress(Exception):
            rj.popen.kill()
            rj.popen.wait(timeout=1)
        rj.close_logs()


@pytest.fixture
def restored_signal_handlers() -> Iterator[None]:
    """``Daemon.run`` mutates process-global signal dispositions.

    Without this the pytest process would keep the daemon's handlers for the
    rest of the session, which breaks Ctrl-C and leaks into later tests.
    """
    watched = [signal.SIGTERM, signal.SIGINT, signal.SIGHUP]
    saved = {sig: signal.getsignal(sig) for sig in watched}
    try:
        yield
    finally:
        for sig, handler in saved.items():
            if handler is not None:
                with contextlib.suppress(ValueError, OSError, TypeError):
                    signal.signal(sig, handler)


def _write_running_spec(
    daemon: Daemon,
    jobid: str,
    *,
    recover_on_reboot: bool = False,
) -> JobSpec:
    """A RUNNING spec whose pgid is a value no real group will hold."""
    workspace = daemon.jobs_dir / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(
        id=jobid,
        command=["echo", "hi"],
        cwd=str(workspace),
        cpus=1,
        state=JobState.RUNNING,
        pgid=999_001,
        recover_on_reboot=recover_on_reboot,
        started_at="2026-01-01T00:00:00+00:00",
    )
    spec.write(daemon._spec_path(jobid))
    return spec


def _all_specs(daemon: Daemon) -> list[JobSpec]:
    return [JobSpec.read(p) for p in daemon.queue_dir.glob("*.json")]


class TestStopHandlerPrecedesTheStartupWalk:
    """The regression itself: SIGTERM must not be at SIG_DFL during the walk."""

    def test_handlers_are_installed_before_the_walk_runs(
        self,
        daemon: Daemon,
        restored_signal_handlers: None,
    ) -> None:
        seen: dict[str, object] = {}
        walk = daemon._reattach_or_interrupt_at_startup

        def _observe_dispositions() -> None:
            seen["sigterm"] = signal.getsignal(signal.SIGTERM)
            seen["sigint"] = signal.getsignal(signal.SIGINT)
            walk()
            # Leave run() promptly; the loop below is not what is under test.
            daemon._stop = True

        daemon._reattach_or_interrupt_at_startup = _observe_dispositions  # type: ignore[method-assign]
        daemon.run()

        # On the parent these are SIG_DFL: the process would have died here
        # with no shutdown and nothing in the log.
        assert seen["sigterm"] is not signal.SIG_DFL
        assert seen["sigint"] is not signal.SIG_DFL
        assert seen["sigterm"] == daemon._handle_signal
        assert seen["sigint"] == daemon._handle_signal

    def test_a_signal_delivered_during_the_walk_requests_a_stop(
        self,
        daemon: Daemon,
        restored_signal_handlers: None,
    ) -> None:
        """End to end in-process: the handler installed before the walk turns
        a real SIGTERM into a stop request rather than process death."""
        import os

        walk = daemon._reattach_or_interrupt_at_startup
        observed: dict[str, bool] = {}

        def _signal_self_mid_walk() -> None:
            # Guard before delivering: on code where the handler is still at
            # its default disposition this SIGTERM kills the test runner
            # outright -- which is precisely the defect, but it must be
            # reported as a failure, not as a dead pytest process.
            if signal.getsignal(signal.SIGTERM) is signal.SIG_DFL:
                observed["handler_installed"] = False
                daemon._stop = True
                return
            observed["handler_installed"] = True
            os.kill(os.getpid(), signal.SIGTERM)
            observed["survived_the_signal"] = True
            walk()

        daemon._reattach_or_interrupt_at_startup = _signal_self_mid_walk  # type: ignore[method-assign]
        daemon.run()

        assert observed["handler_installed"] is True, (
            "SIGTERM was still at its default disposition during the startup "
            "walk; a stop here kills the daemon outright"
        )
        assert observed["survived_the_signal"] is True
        assert daemon._stop is True

    def test_run_does_not_publish_rpc_when_stopped_during_startup(
        self,
        daemon: Daemon,
        monkeypatch: pytest.MonkeyPatch,
        restored_signal_handlers: None,
    ) -> None:
        """An updater polling for readiness must not get an answer from a
        daemon that is on its way out."""
        constructed: list[object] = []

        class _SpyRPCServer:
            def __init__(self, *args: object, **kwargs: object) -> None:
                constructed.append(self)

            def start(self) -> None:
                pass

            def stop(self) -> None:
                pass

        monkeypatch.setattr("vq.rpc.RPCServer", _SpyRPCServer)
        walk = daemon._reattach_or_interrupt_at_startup

        def _stop_during_walk() -> None:
            walk()
            daemon._stop = True

        daemon._reattach_or_interrupt_at_startup = _stop_during_walk  # type: ignore[method-assign]
        daemon.run()

        assert constructed == []


class TestStartupWalkHonoursTheStop:
    """The walk gives up at the next spec boundary, not minutes later."""

    def test_unvisited_specs_keep_their_entry_state(
        self,
        daemon: Daemon,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr("vq.daemon._pgroup_alive", lambda _pgid: False)
        first = _write_running_spec(daemon, "aaaaaaaaaaaa")
        second = _write_running_spec(daemon, "bbbbbbbbbbbb")

        def _iter_then_stop() -> Iterator[JobSpec]:
            yield first
            # The stop lands while the daemon is between two specs.
            daemon._stop = True
            yield second

        monkeypatch.setattr(daemon, "_iter_specs", _iter_then_stop)
        daemon._reattach_or_interrupt_at_startup()

        # The spec this pass reached is reconciled and persisted...
        assert (
            JobSpec.read(daemon._spec_path("aaaaaaaaaaaa")).state
            == JobState.ABORTED_BY_QUEUE
        )
        # ...and the one it never reached is left exactly as it was, for the
        # next daemon life to reconcile. On the parent the walk ran on and
        # aborted it too, minutes after the service manager asked it to stop.
        assert (
            JobSpec.read(daemon._spec_path("bbbbbbbbbbbb")).state
            == JobState.RUNNING
        )

    def test_the_partial_pass_still_auto_resumes_what_it_aborted(
        self,
        daemon: Daemon,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A sibling is a durable PENDING spec the next daemon dispatches.

        Dropping it would lose the resume for good: the parent is
        ABORTED_BY_QUEUE now, so no later startup sees it enter from RUNNING.
        """
        monkeypatch.setattr("vq.daemon._pgroup_alive", lambda _pgid: False)
        first = _write_running_spec(
            daemon, "aaaaaaaaaaaa", recover_on_reboot=True,
        )
        second = _write_running_spec(
            daemon, "bbbbbbbbbbbb", recover_on_reboot=True,
        )

        def _iter_then_stop() -> Iterator[JobSpec]:
            yield first
            daemon._stop = True
            yield second

        monkeypatch.setattr(daemon, "_iter_specs", _iter_then_stop)
        daemon._reattach_or_interrupt_at_startup()

        specs = _all_specs(daemon)
        assert [s.id for s in specs if s.parent_jobid == "aaaaaaaaaaaa"] != []
        # The unvisited job is untouched, so it gets no sibling either.
        assert [s.id for s in specs if s.parent_jobid == "bbbbbbbbbbbb"] == []

    def test_an_uninterrupted_walk_still_visits_every_spec(
        self,
        daemon: Daemon,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Control: nothing above changes the ordinary startup pass."""
        monkeypatch.setattr("vq.daemon._pgroup_alive", lambda _pgid: False)
        _write_running_spec(daemon, "aaaaaaaaaaaa")
        _write_running_spec(daemon, "bbbbbbbbbbbb")

        daemon._reattach_or_interrupt_at_startup()

        for jobid in ("aaaaaaaaaaaa", "bbbbbbbbbbbb"):
            assert (
                JobSpec.read(daemon._spec_path(jobid)).state
                == JobState.ABORTED_BY_QUEUE
            )
