"""A process group that is tearing down is gone, not another owner's (#27).

On macOS a group whose last member is exiting answers ``killpg`` with
``EPERM`` for a moment before ``ESRCH``. ``vq kill``, pause and resume read
that as "another user's group". The real-kernel tests hold a group in that
phase deterministically: its only remaining member is a zombie child of the
test process, which macOS keeps answering ``EPERM`` for until the test reaps
it. Where the kernel lets a zombie-only group be signalled (Linux), there is
no teardown ``EPERM`` to disambiguate and those tests skip; the fake-kernel
tests pin the mechanism everywhere.
"""
from __future__ import annotations

import contextlib
import errno
import logging
import os
import signal
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests import test_kill
from vq import events, process_group, watchdog
from vq.kill import kill_job
from vq.spec import JobSpec, JobState


class _FakeKernel:
    """``os.killpg`` answering from a script of outcomes, one per call."""

    def __init__(self, outcomes: list[object]) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[tuple[int, int]] = []

    def __call__(self, pgid: int, sig: int) -> None:
        self.calls.append((pgid, sig))
        outcome = self.outcomes.pop(0) if len(self.outcomes) > 1 else self.outcomes[0]
        if outcome == "EPERM":
            raise PermissionError(errno.EPERM, "Operation not permitted")
        if outcome == "ESRCH":
            raise ProcessLookupError(errno.ESRCH, "No such process")


class TestTheSettleMechanism:
    def test_a_group_that_settles_to_esrch_is_gone(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        kernel = _FakeKernel(["EPERM", "EPERM", "EPERM", "ESRCH"])
        monkeypatch.setattr(process_group.os, "killpg", kernel)

        with pytest.raises(ProcessLookupError):
            process_group.signal_process_group(4242, signal.SIGTERM, settle_seconds=5)

        assert kernel.calls[0] == (4242, signal.SIGTERM)
        assert all(sig == 0 for _pgid, sig in kernel.calls[1:])

    def test_a_persistent_eperm_is_believed_after_the_budget(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        kernel = _FakeKernel(["EPERM"])
        monkeypatch.setattr(process_group.os, "killpg", kernel)

        started = time.monotonic()
        with pytest.raises(PermissionError):
            process_group.signal_process_group(4242, signal.SIGSTOP, settle_seconds=0.1)
        elapsed = time.monotonic() - started

        assert 0.1 <= elapsed < 2.0
        assert len(kernel.calls) >= 3

    def test_a_group_that_answers_again_gets_the_signal_resent(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        kernel = _FakeKernel(["EPERM", "ok", "ok"])
        monkeypatch.setattr(process_group.os, "killpg", kernel)

        process_group.signal_process_group(4242, signal.SIGCONT, settle_seconds=5)

        assert kernel.calls == [
            (4242, signal.SIGCONT), (4242, 0), (4242, signal.SIGCONT),
        ]

    def test_an_ordinary_signal_is_one_call(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        kernel = _FakeKernel(["ok"])
        monkeypatch.setattr(process_group.os, "killpg", kernel)

        process_group.signal_process_group(4242, signal.SIGTERM)

        assert kernel.calls == [(4242, signal.SIGTERM)]


def _await_pgid(pid: int, pgid: int) -> None:
    for _ in range(2000):
        with contextlib.suppress(ProcessLookupError):
            if os.getpgid(pid) == pgid:
                return
        time.sleep(0.001)
    raise AssertionError(f"pid {pid} never joined process group {pgid}")


@contextlib.contextmanager
def _zombie_held_group() -> Iterator[tuple[int, int]]:
    """A killed group whose only remaining member is our unreaped zombie.

    Yields (pgid, zombie pid). The zombie is reaped on exit if the test did
    not reap it itself.
    """
    leader = os.fork()
    if leader == 0:  # pragma: no cover - child
        os.setpgid(0, 0)
        time.sleep(60)
        os._exit(0)
    _await_pgid(leader, leader)
    member = os.fork()
    if member == 0:  # pragma: no cover - child
        with contextlib.suppress(OSError):
            os.setpgid(0, leader)
        time.sleep(60)
        os._exit(0)
    with contextlib.suppress(OSError):
        os.setpgid(member, leader)
    _await_pgid(member, leader)
    os.killpg(leader, signal.SIGKILL)
    os.waitpid(leader, 0)
    os.waitid(os.P_PID, member, os.WEXITED | os.WNOWAIT)
    try:
        try:
            os.killpg(leader, 0)
        except PermissionError:
            pass
        else:
            pytest.skip(
                "this kernel lets a zombie-only group be signalled, so it has "
                "no teardown EPERM to disambiguate"
            )
        yield leader, member
    finally:
        with contextlib.suppress(ChildProcessError):
            os.waitpid(member, 0)


def _reap_after(pid: int, seconds: float) -> threading.Timer:
    timer = threading.Timer(seconds, lambda: os.waitpid(pid, 0))
    timer.start()
    return timer


class TestARealTearingDownGroup:
    def test_the_helper_reports_it_gone_once_it_settles(self) -> None:
        with _zombie_held_group() as (pgid, zombie):
            timer = _reap_after(zombie, 0.1)
            try:
                with pytest.raises(ProcessLookupError):
                    process_group.signal_process_group(
                        pgid, signal.SIGTERM, settle_seconds=10,
                    )
            finally:
                timer.join()

    def test_a_group_that_never_settles_stays_denied(self) -> None:
        """The conservative answer, and the one another owner's group gets."""
        with _zombie_held_group() as (pgid, _zombie), pytest.raises(PermissionError):
            process_group.signal_process_group(
                pgid, signal.SIGTERM, settle_seconds=0.2,
            )

    def test_vq_kill_does_not_call_an_exiting_job_permission_denied(
        self, tmp_path: Path,
    ) -> None:
        queue = tmp_path / "queue"
        workspace = tmp_path / "job"
        workspace.mkdir()
        with _zombie_held_group() as (pgid, zombie):
            test_kill._write(
                queue, "j1", cwd=str(workspace), state=JobState.RUNNING,
                pid=zombie, pgid=pgid,
            )
            timer = _reap_after(zombie, 0.1)
            try:
                message = kill_job("localhost", "j1", queue_dir=queue)
            finally:
                timer.join()

        assert "permission denied" not in message, message
        assert "already dead" in message
        assert JobSpec.read(queue / "j1.json").state == JobState.KILLED
        reasons = [
            event["reason"]
            for event in events.read_events(workspace)
            if event.get("kind") == "state_transition"
        ]
        assert "permission denied" not in reasons[-1]

    def test_the_watchdog_calls_it_gone_without_a_warning(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        with _zombie_held_group() as (pgid, zombie):
            timer = _reap_after(zombie, 0.1)
            try:
                with caplog.at_level(logging.WARNING, logger=watchdog.log.name):
                    landed = watchdog.killpg(pgid, signal.SIGKILL)
            finally:
                timer.join()

        assert landed is False
        assert "permission denied" not in caplog.text


@pytest.fixture
def state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    from vq import config, paths

    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


def _spec_for_group(
    jobid: str, *, pgid: int, pid: int, job_state: JobState,
) -> JobSpec:
    from vq import paths

    workspace = paths.jobs_dir() / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    extra: dict[str, object] = {}
    if job_state == JobState.SUSPENDED:
        extra = {
            "paused_at": "2026-09-13T12:00:00+00:00",
            "paused_monotonic_at": time.monotonic(),
        }
    spec = JobSpec(
        id=jobid,
        command=["sleep", "30"],
        cwd=str(workspace),
        cpus=1,
        state=job_state,
        pid=pid,
        pgid=pgid,
        started_at="2026-09-13T11:00:00+00:00",
        **extra,
    )
    spec.write(paths.spec_path(jobid))
    return spec


class TestPauseAndResumeOfATearingDownGroup:
    def test_pause_says_the_job_is_gone_not_another_user_s(
        self, state: Path,
    ) -> None:
        from vq.pause_resume import PauseError, pause_job

        with _zombie_held_group() as (pgid, zombie):
            spec = _spec_for_group(
                "pexit0001", pgid=pgid, pid=zombie, job_state=JobState.RUNNING,
            )
            timer = _reap_after(zombie, 0.1)
            try:
                with pytest.raises(PauseError) as caught:
                    pause_job("localhost", spec.id)
            finally:
                timer.join()

        assert "another user" not in str(caught.value), caught.value
        assert "is gone" in str(caught.value)

    def test_resume_says_the_job_is_gone_not_another_user_s(
        self, state: Path,
    ) -> None:
        from vq.pause_resume import PauseError, resume_job

        with _zombie_held_group() as (pgid, zombie):
            spec = _spec_for_group(
                "rexit0001", pgid=pgid, pid=zombie, job_state=JobState.SUSPENDED,
            )
            timer = _reap_after(zombie, 0.1)
            try:
                with pytest.raises(PauseError) as caught:
                    resume_job("localhost", spec.id)
            finally:
                timer.join()

        assert "another user" not in str(caught.value), caught.value
        assert "is gone" in str(caught.value)
