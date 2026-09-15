"""Terminal-but-alive process reaping (audit STATE-3 + STATE-2).

A job can reach a terminal state (via `vq kill` or the watchdog) while its
process is still alive — a SIGTERM-ignoring command, or a process group that
survives a daemon restart. vq must force-reap these so they don't pin a
cpu/mem slot (STATE-3) or leak untracked across a restart (STATE-2).

A job whose wrapper dies leaks from the other side. A paused command stays
SIGSTOPped with nothing left to continue or reap it; a running one keeps going
after its slot is released, beside its own retry.
"""
from __future__ import annotations

import contextlib
import fcntl
import os
import signal
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from vq import daemon as daemon_mod
from vq import kill as kill_mod
from vq import paths
from vq.daemon import Daemon, _RunningJob
from vq.spec import JobSpec, JobState, utcnow_iso


# A command that installs SIG_IGN for SIGTERM, touches a readiness file, then
# sleeps — i.e. it ignores the `vq kill` SIGTERM and only dies to SIGKILL. The
# readiness file lets the test wait until the handler is installed before
# sending SIGTERM (otherwise the signal can land during interpreter startup,
# before SIG_IGN is in place, and the default terminate action kills it).
def _sigterm_ignoring_cmd(ready: Path) -> list[str]:
    return [
        sys.executable,
        "-c",
        "import signal, sys, time; "
        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        "open(sys.argv[1], 'w').close(); "
        "time.sleep(60)",
        str(ready),
    ]


@pytest.fixture
def daemon(tmp_path: Path) -> Iterator[Daemon]:
    d = Daemon(
        max_cpus=8,
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


def _spawn_running_job(daemon: Daemon, jobid: str) -> subprocess.Popen[bytes]:
    """Spawn a real SIGTERM-ignoring process, register it as a RUNNING job in
    the daemon's _running table, and write a matching RUNNING spec."""
    ws = daemon.jobs_dir / jobid
    ws.mkdir(parents=True, exist_ok=True)
    ready = ws / "ready"
    proc = subprocess.Popen(
        _sigterm_ignoring_cmd(ready),
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    # Wait until the child has installed its SIGTERM-ignore handler. A
    # liveness guard, not a timing claim (#41).
    deadline = time.monotonic() + _LIVENESS_SECONDS
    while not ready.exists():
        if time.monotonic() > deadline or proc.poll() is not None:
            proc.kill()
            raise AssertionError("child process did not signal readiness")
        time.sleep(0.01)
    pgid = os.getpgid(proc.pid)
    spec = JobSpec(
        id=jobid,
        command=["sleep", "60"],
        cwd=str(ws),
        cpus=1,
        state=JobState.RUNNING,
        pid=proc.pid,
        pgid=pgid,
        started_at="2026-05-14T00:00:00+00:00",
    )
    spec.write(daemon._spec_path(jobid))
    daemon._running[jobid] = _RunningJob(
        popen=proc,  # type: ignore[arg-type]
        cpus=1,
        mem_mb=None,
        stdout_fh=(ws / "stdout.log").open("ab"),
        stderr_fh=(ws / "stderr.log").open("ab"),
    )
    return proc


class TestState3KillEscalation:
    def test_sigterm_ignoring_kill_is_escalated_to_sigkill(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A `vq kill`'d job whose process ignores SIGTERM must be SIGKILLed
        by the daemon once the grace elapses, then reaped from _running — so
        it stops pinning its slot. Pre-STATE-3 the daemon never escalated and
        the job sat in _running forever."""
        # Zero grace so the escalation fires on the second reconcile pass.
        monkeypatch.setattr(daemon_mod, "KILL_ESCALATION_GRACE_SECONDS", 0.0)
        jobid = "state3kill01"
        proc = _spawn_running_job(daemon, jobid)
        try:
            pgid = os.getpgid(proc.pid)
            # Simulate `vq kill`: SIGTERM (ignored) + terminal label on disk.
            os.killpg(pgid, signal.SIGTERM)
            spec = JobSpec.read(daemon._spec_path(jobid))
            spec.state = JobState.KILLED
            spec.finished_at = utcnow_iso()
            spec.write(daemon._spec_path(jobid))
            time.sleep(0.05)
            assert proc.poll() is None, "process should ignore SIGTERM"

            # Tick 1: daemon notices the terminal label, arms the deadline.
            daemon._reconcile_running()
            assert jobid in daemon._running
            assert proc.poll() is None, "no SIGKILL on the first (arming) tick"

            # Tick 2: grace (0s) elapsed -> SIGKILL.
            time.sleep(0.02)
            daemon._reconcile_running()
            # SIGKILL is uncatchable; the bound only catches a hang (#41).
            proc.wait(timeout=_LIVENESS_SECONDS)

            # Tick 3: popen.poll() now returns -> reap out of _running.
            daemon._reconcile_running()
            assert jobid not in daemon._running, "killed job must leave _running"
            final = JobSpec.read(daemon._spec_path(jobid))
            assert final.state == JobState.KILLED, "terminal label must survive"
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=2)

    def test_running_job_not_killed_is_left_alone(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A normal RUNNING job (no terminal label) must never be escalated —
        the deadline clock stays un-armed."""
        monkeypatch.setattr(daemon_mod, "KILL_ESCALATION_GRACE_SECONDS", 0.0)
        jobid = "state3live01"
        proc = _spawn_running_job(daemon, jobid)
        try:
            for _ in range(3):
                daemon._reconcile_running()
                time.sleep(0.02)
            assert jobid in daemon._running
            assert proc.poll() is None, "a non-killed RUNNING job must stay alive"
            assert daemon._running[jobid].term_deadline is None
        finally:
            proc.kill()
            proc.wait(timeout=2)


def _spawn_detached(stdout: int = subprocess.DEVNULL) -> subprocess.Popen[bytes]:
    """A plain long-lived process in its own session/pgid — stands in for a
    job's process group that outlived the previous daemon."""
    return subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"],
        start_new_session=True,
        stdout=stdout,
        stderr=subprocess.DEVNULL,
    )


class TestState2StartupReap:
    def test_terminal_survivor_is_sigkilled_at_startup(self, daemon: Daemon) -> None:
        """A spec that was terminal (KILLED) before the previous daemon died,
        whose process group is still alive at startup, must be SIGKILL-reaped
        — pre-STATE-2 the startup scan skipped terminal specs entirely and the
        process leaked untracked."""
        jobid = "state2surv01"
        ws = daemon.jobs_dir / jobid
        ws.mkdir(parents=True, exist_ok=True)
        proc = _spawn_detached()
        try:
            pgid = os.getpgid(proc.pid)
            spec = JobSpec(
                id=jobid,
                command=["sleep", "60"],
                cwd=str(ws),
                cpus=1,
                state=JobState.KILLED,
                pid=proc.pid,
                pgid=pgid,
                finished_at="2026-05-14T00:00:00+00:00",
            )
            spec.write(daemon._spec_path(jobid))
            assert proc.poll() is None

            daemon._reattach_or_interrupt_at_startup()
            proc.wait(timeout=_LIVENESS_SECONDS)  # SIGKILLed by the startup reap

            assert proc.poll() is not None, "leaked process group must be reaped"
            # The terminal spec is preserved; the job is not (re)tracked.
            assert JobSpec.read(daemon._spec_path(jobid)).state == JobState.KILLED
            assert jobid not in daemon._running
            assert jobid not in daemon._orphans
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=2)

    def test_recycled_pgid_is_not_reaped(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If the pid fingerprint says the pgid was recycled to an unrelated
        process during the downtime, the startup reap must NOT SIGKILL it —
        killing someone else's process is worse than a rare leak."""
        monkeypatch.setattr(daemon_mod, "_pid_fingerprint_matches", lambda _spec: False)
        jobid = "state2recyc1"
        ws = daemon.jobs_dir / jobid
        ws.mkdir(parents=True, exist_ok=True)
        proc = _spawn_detached()
        try:
            pgid = os.getpgid(proc.pid)
            spec = JobSpec(
                id=jobid,
                command=["sleep", "60"],
                cwd=str(ws),
                cpus=1,
                state=JobState.KILLED,
                pid=proc.pid,
                pgid=pgid,
                finished_at="2026-05-14T00:00:00+00:00",
            )
            spec.write(daemon._spec_path(jobid))

            daemon._reattach_or_interrupt_at_startup()
            time.sleep(0.2)
            assert proc.poll() is None, "a recycled pgid must NOT be reaped"
        finally:
            proc.kill()
            proc.wait(timeout=2)


class TestScopeReaping:
    """v0.15.x: every local job runs in a cgroup scope (even capless),
    and the daemon stops that scope on teardown so descendants that
    escaped the process group (setsid/PR_SET_PGID children that killpg
    cannot reach) die with the job instead of lingering as memory
    unowned by any job. ``_reap_scope`` is the helper, and the
    kill/finish paths call it."""

    def test_reaps_when_cgroups_enabled(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: list[tuple[str, bool]] = []

        def _fake_stop(name: str, *, multi_user: bool = False) -> bool:
            captured.append((name, multi_user))
            return True

        monkeypatch.setattr(daemon_mod.cgroup, "stop_scope", _fake_stop)
        daemon.cgroup_enabled = True
        daemon._reap_scope("abc123")
        assert captured == [("vq-job-abc123", False)]

    def test_noop_without_cgroups_or_multiuser(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        called: list[object] = []
        monkeypatch.setattr(
            daemon_mod.cgroup, "stop_scope",
            lambda *a, **kw: called.append(a) or True,
        )
        daemon.cgroup_enabled = False  # macOS / no-delegation host
        # The fixture daemon is single-user, so _multi_user is False.
        daemon._reap_scope("abc123")
        assert called == [], "no scope to reap without cgroups"

    def test_sigkill_escalation_reaps_the_scope(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The STATE-3 SIGKILL escalation of a SIGTERM-ignoring killed
        job must also reap the scope. We spy on _reap_scope: the call
        fires on the escalation tick, not the arming tick."""
        monkeypatch.setattr(daemon_mod, "KILL_ESCALATION_GRACE_SECONDS", 0.0)
        reaped: list[str] = []
        monkeypatch.setattr(
            daemon, "_reap_scope", lambda jobid: reaped.append(jobid)
        )
        jobid = "state3reap01"
        proc = _spawn_running_job(daemon, jobid)
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            spec = JobSpec.read(daemon._spec_path(jobid))
            spec.state = JobState.KILLED
            spec.finished_at = utcnow_iso()
            spec.write(daemon._spec_path(jobid))
            daemon._reconcile_running()  # tick 1: arm the deadline
            assert reaped == [], "no reap on the arming tick"
            time.sleep(0.02)
            daemon._reconcile_running()  # tick 2: SIGKILL + reap
            proc.wait(timeout=_LIVENESS_SECONDS)
            assert jobid in reaped, "scope reaped on the SIGKILL escalation"
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=2)

    def test_a_retry_reaps_the_attempts_scope(
        self, dispatching_daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A retry ends the attempt just as FAILED does, so it stops the scope
        too, instead of leaving escaped group members to run on through the
        backoff. Spied, since the fixture host has no cgroups."""
        daemon = dispatching_daemon
        reaped: list[str] = []
        monkeypatch.setattr(daemon, "_reap_scope", reaped.append)
        jobid = "retryscope1"
        workspace = daemon.jobs_dir / jobid
        workspace.mkdir(parents=True, exist_ok=True)
        JobSpec(
            id=jobid,
            command=[sys.executable, "-c", "raise SystemExit(3)"],
            cwd=str(workspace),
            cpus=1,
            retry_max=1,
        ).write(daemon._spec_path(jobid))

        final = _drive(daemon, jobid, lambda s: s.retry_count == 1)

        assert final.state == JobState.PENDING
        assert reaped == [jobid]


# Holds an exclusive flock for as long as it lives. The lock is the liveness
# probe: an exited process releases it at once, even while it waits as a zombie
# for its new parent to reap it (a zombie still answers `killpg(pgid, 0)`, and
# a container's PID 1 need not reap promptly), whereas a SIGSTOPped process
# keeps holding it.
_LOCK_HOLDING_COMMAND = (
    "import fcntl, pathlib, time; "
    "held = open('held', 'w'); fcntl.flock(held, fcntl.LOCK_EX); "
    "pathlib.Path('started').touch(); time.sleep(60)"
)
# The same, but deaf to the SIGTERM `vq kill` and the watchdog send: only
# SIGKILL ends it. ``started`` is touched after SIG_IGN is installed, so a
# test that waits for it cannot race the handler.
_SIGTERM_IGNORING_LOCK_COMMAND = (
    "import fcntl, pathlib, signal, time; "
    "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
    "held = open('held', 'w'); fcntl.flock(held, fcntl.LOCK_EX); "
    "pathlib.Path('started').touch(); time.sleep(60)"
)
# Bounds spin loops so a hang fails instead of blocking the suite; not a timing claim.
_LIVENESS_SECONDS = 30.0

# How far below the SIGKILL grace an ordinary kill must release its slot. The
# boundary test's claim is "did not wait out the grace", so its ceiling is the
# grace less this margin, not a fixed five seconds that a loaded host can miss
# without the claim being false (#41).
_RELEASE_MARGIN_SECONDS = 2.0

# Starts the lock holder in the background and exits 0 once it holds the lock,
# so the wrapper waits for a command that finished normally and exits cleanly
# with a live member left in its group.
_BACKGROUNDING_COMMAND = "\n".join(
    [
        "import pathlib, subprocess, sys, time",
        f"subprocess.Popen([sys.executable, '-c', {_LOCK_HOLDING_COMMAND!r}])",
        f"deadline = time.monotonic() + {_LIVENESS_SECONDS}",
        "while not pathlib.Path('started').exists() and time.monotonic() < deadline:",
        "    time.sleep(0.01)",
    ]
)


def _kill_group_best_effort(workspace: Path, pgid: int) -> None:
    """Tear down whatever is left of a job's process group.

    Each test ends its own group rather than leaving it to the fixture, but by
    then the daemon under test has usually SIGKILLed it already. On macOS
    ``killpg`` then answers EPERM, not ESRCH, until the last member is reaped:
    from the moment that member starts exiting, while it still holds its lock,
    and on while it waits unreaped for its new parent. That window is
    scheduling work, so it widens under load: on a saturated box EPERM escaped
    a cleanup that tolerated only ESRCH and failed the test it was meant to
    tidy up after.

    The command's lock decides whether to signal at all. Once it is free there
    is nothing left to end, and a group that has gone leaves its id free for
    an unrelated one, so nothing is sent. While it is held a member still
    exists, so the id is still this job's, and EPERM only says that member is
    already exiting.
    """
    if _command_exited(workspace, budget=0.0):
        return
    with contextlib.suppress(OSError):
        os.killpg(pgid, signal.SIGKILL)


@pytest.fixture
def dispatching_daemon(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[Daemon]:
    """A daemon that dispatches real wrapped jobs on a host without cgroups.

    The queue sits on the default state paths because the host-pressure pause
    resolves specs through ``paths``. Cgroups are pinned off so ``_reap_scope``
    stays a no-op on a Linux developer box too: that is the host class where a
    command outlived its wrapper.
    """
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setattr(daemon_mod.cgroup, "available", lambda: False)
    d = Daemon(
        max_cpus=4,
        poll_interval=0.05,
        queue_dir=paths.queue_dir(),
        jobs_dir=paths.jobs_dir(),
    )
    d.queue_dir.mkdir(parents=True, exist_ok=True)
    d.jobs_dir.mkdir(parents=True, exist_ok=True)
    yield d
    for survivor in d._terminal_survivors.values():
        # Belt and braces behind each test's own finally.
        _kill_group_best_effort(survivor.workspace, survivor.pgid)
    for jobid, rj in d._running.items():
        _kill_group_best_effort(d.jobs_dir / jobid, rj.popen.pid)
        # The leader is our child: until the wait below reaps it, its pid
        # cannot name anyone else.
        rj.popen.kill()
        with contextlib.suppress(subprocess.TimeoutExpired):
            rj.popen.wait(timeout=1)
        rj.close_logs()


def _dispatch_started(
    daemon: Daemon,
    jobid: str,
    *,
    retry_max: int = 0,
    command: str = _LOCK_HOLDING_COMMAND,
    cpus: int = 1,
) -> JobSpec:
    """Dispatch a lock-holding command; return its RUNNING spec once the lock
    is held, so the group has a member besides its leader."""
    workspace = daemon.jobs_dir / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    JobSpec(
        id=jobid,
        command=[sys.executable, "-c", command],
        cwd=str(workspace),
        cpus=cpus,
        retry_max=retry_max,
    ).write(daemon._spec_path(jobid))
    deadline = time.monotonic() + _LIVENESS_SECONDS
    while not (workspace / "started").exists():
        assert time.monotonic() < deadline, "the wrapper never started the command"
        daemon.iterate()
        time.sleep(0.02)
    spec = JobSpec.read(daemon._spec_path(jobid))
    assert spec.state == JobState.RUNNING
    assert spec.pid is not None and spec.pid == spec.pgid
    return spec


def _drive(daemon: Daemon, jobid: str, done: Callable[[JobSpec], bool]) -> JobSpec:
    deadline = time.monotonic() + _LIVENESS_SECONDS
    while True:
        daemon.iterate()
        spec = JobSpec.read(daemon._spec_path(jobid))
        if done(spec):
            return spec
        assert time.monotonic() < deadline, f"job {jobid} stuck in {spec.state.value}"
        time.sleep(0.02)


def _command_exited(workspace: Path, budget: float) -> bool:
    """True once the command has released its lock, i.e. has exited."""
    deadline = time.monotonic() + budget
    with (workspace / "held").open("a") as probe:
        while True:
            try:
                fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    return False
                time.sleep(0.01)
            else:
                return True


class TestPausedGroupReap:
    """A paused job's command must not outlive its wrapper, stopped for good.

    ``vq.resource_receipt`` leads the job's process group and forks the
    command into it, and a pause SIGSTOPs both. On 2026-09-11, SIGKILLing the
    wrapper alone (the pid ``vq status`` shows) left the job FAILED rc=-9 and
    the command stopped under init, where no resume, retry or restart sweep
    would reach it.
    """

    @pytest.mark.no_autopatch_host_pressure
    @pytest.mark.parametrize("retry_max", [0, 1], ids=["failed", "retried"])
    def test_host_pressure_paused_command_dies_with_its_wrapper(
        self,
        dispatching_daemon: Daemon,
        monkeypatch: pytest.MonkeyPatch,
        retry_max: int,
    ) -> None:
        """The reported sequence, down both exits: FAILED, and the retry that
        clears the pgid."""
        daemon = dispatching_daemon
        pressure = [10.0]
        monkeypatch.setattr(
            "vq.watchdog.read_host_memory_pressure_pct", lambda: pressure[0]
        )
        jobid = f"pausedwrap{retry_max}"
        running = _dispatch_started(daemon, jobid, retry_max=retry_max)
        assert running.pid is not None and running.pgid is not None
        try:
            pressure[0] = 90.0
            paused = _drive(daemon, jobid, lambda s: s.state == JobState.SUSPENDED)
            assert paused.paused_by == daemon_mod.HOST_PRESSURE_PAUSE_TAG

            os.kill(running.pid, signal.SIGKILL)  # the wrapper alone
            final = _drive(daemon, jobid, lambda _s: jobid not in daemon._running)

            if retry_max:
                assert final.state == JobState.PENDING
                assert final.pgid is None
            else:
                assert final.state == JobState.FAILED
                assert final.exit_code == -signal.SIGKILL
            assert _command_exited(daemon.jobs_dir / jobid, budget=_LIVENESS_SECONDS), (
                "the SIGSTOPped command outlived its wrapper"
            )
        finally:
            _kill_group_best_effort(daemon.jobs_dir / jobid, running.pgid)

    def test_an_unreconciled_pause_intent_counts_as_paused(
        self, dispatching_daemon: Daemon
    ) -> None:
        """``pause_job`` fsyncs its intent before SIGSTOP, so a pauser killed
        after the signal leaves a stopped group behind a RUNNING spec.
        ``iterate()`` normally completes that intent into SUSPENDED before it
        polls; poll alone here, as when reconciliation fails and leaves the
        intent in place."""
        daemon = dispatching_daemon
        jobid = "intentwrap01"
        running = _dispatch_started(daemon, jobid)
        assert running.pid is not None and running.pgid is not None
        try:
            spec_path = daemon._spec_path(jobid)
            running.pause_intent_at = utcnow_iso()
            running.pause_intent_monotonic_at = time.monotonic()
            running.pause_intent_pgid = running.pgid
            running.write(spec_path)
            os.killpg(running.pgid, signal.SIGSTOP)
            os.kill(running.pid, signal.SIGKILL)
            daemon._running[jobid].popen.wait(timeout=_LIVENESS_SECONDS)

            daemon._reconcile_running()

            assert jobid not in daemon._running
            assert JobSpec.read(spec_path).state == JobState.FAILED
            assert _command_exited(daemon.jobs_dir / jobid, budget=_LIVENESS_SECONDS), (
                "the SIGSTOPped command outlived its wrapper"
            )
        finally:
            _kill_group_best_effort(daemon.jobs_dir / jobid, running.pgid)



class TestRunningGroupReap:
    """A running job's command must not outlive a wrapper killed by a signal.

    The wrapper writes the exit marker and the resource receipt only after it
    has waited for the command, so once it is killed the command's outcome
    cannot be recorded. Until 2026-09-11 the daemon then released the job's
    slot, or re-enqueued it into the same workspace, while the command ran on
    under init on every host without a cgroup scope.
    """

    @pytest.mark.parametrize(
        ("sig", "retry_max"),
        [(signal.SIGKILL, 0), (signal.SIGKILL, 1), (signal.SIGTERM, 0)],
        ids=["sigkill-failed", "sigkill-retried", "sigterm-failed"],
    )
    def test_a_running_command_dies_with_its_signalled_wrapper(
        self, dispatching_daemon: Daemon, sig: signal.Signals, retry_max: int
    ) -> None:
        """The wrapper alone, as a ``kill`` of the pid ``vq status`` shows
        reaches it."""
        daemon = dispatching_daemon
        jobid = f"runwrap{int(sig)}r{retry_max}"
        running = _dispatch_started(daemon, jobid, retry_max=retry_max)
        assert running.pid is not None and running.pgid is not None
        try:
            os.kill(running.pid, sig)
            final = _drive(daemon, jobid, lambda _s: jobid not in daemon._running)

            if retry_max:
                assert final.state == JobState.PENDING
                assert final.pgid is None
            else:
                assert final.state == JobState.FAILED
                assert final.exit_code == -sig
            assert _command_exited(daemon.jobs_dir / jobid, budget=_LIVENESS_SECONDS), (
                "the command outlived its signalled wrapper"
            )
        finally:
            _kill_group_best_effort(daemon.jobs_dir / jobid, running.pgid)

    def test_a_normal_wrapper_exit_leaves_background_members_alone(
        self, dispatching_daemon: Daemon
    ) -> None:
        """The boundary of the reap above. A wrapper that exits normally has
        waited for its command and recorded the outcome, and on a host without
        a cgroup scope a job may mean a process it put in the background to
        outlive it."""
        daemon = dispatching_daemon
        jobid = "bgwrap01"
        running = _dispatch_started(daemon, jobid, command=_BACKGROUNDING_COMMAND)
        assert running.pgid is not None
        try:
            final = _drive(daemon, jobid, lambda _s: jobid not in daemon._running)

            assert final.state == JobState.COMPLETED
            assert not _command_exited(daemon.jobs_dir / jobid, budget=0.3)
        finally:
            _kill_group_best_effort(daemon.jobs_dir / jobid, running.pgid)


def _tracked(daemon: Daemon, jobid: str) -> bool:
    """True while the daemon still holds this job's resources for it."""
    return (
        jobid in daemon._running
        or jobid in daemon._orphans
        or jobid in daemon._terminal_survivors
    )


class TestTerminalSurvivorEscalation:
    """A killed command that ignores SIGTERM must still be SIGKILLed.

    ``vq kill`` and the watchdog SIGTERM the whole process group and owe it a
    grace before SIGKILL. The group is led by the ``vq.resource_receipt``
    wrapper, which installs no handlers and dies to that SIGTERM at once, so
    the daemon reaps it on its next pass and ``_escalate_if_killed`` -- which
    reads the deadline off the ``_RunningJob`` -- never fires. On 2026-09-12
    that left a SIGTERM-ignoring command running on a host without cgroups,
    tracked by nothing and charged to no one.
    """

    def test_kill_of_a_sigterm_ignoring_command_is_escalated(
        self, dispatching_daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The reported sequence, end to end."""
        daemon = dispatching_daemon
        monkeypatch.setattr(daemon_mod, "KILL_ESCALATION_GRACE_SECONDS", 0.0)
        jobid = "killdeaf0001"
        running = _dispatch_started(
            daemon, jobid, command=_SIGTERM_IGNORING_LOCK_COMMAND
        )
        assert running.pgid is not None
        workspace = daemon.jobs_dir / jobid
        try:
            kill_mod.kill_job("localhost", jobid, queue_dir=daemon.queue_dir)
            # The wrapper dies to the SIGTERM the kill sent; the command does
            # not. Reaping it here is exactly what the daemon's next poll does.
            daemon._running[jobid].popen.wait(timeout=_LIVENESS_SECONDS)
            assert not _command_exited(workspace, budget=0.3), (
                "the command under test must ignore SIGTERM"
            )

            daemon._reconcile_running()
            assert jobid not in daemon._running
            assert jobid in daemon._terminal_survivors, (
                "a killed job whose group outlived its wrapper must stay "
                "tracked, or nothing will ever escalate to SIGKILL"
            )

            daemon._reconcile_terminal_survivors()
            assert _command_exited(workspace, budget=_LIVENESS_SECONDS), (
                "the SIGTERM-ignoring command outlived its killed job"
            )
            assert not _tracked(daemon, jobid), "capacity must be released"
            assert JobSpec.read(daemon._spec_path(jobid)).state == JobState.KILLED
        finally:
            _kill_group_best_effort(daemon.jobs_dir / jobid, running.pgid)

    def test_the_kill_grace_is_not_cut_short(
        self, dispatching_daemon: Daemon
    ) -> None:
        """The point of tracking rather than SIGKILLing at the reap: a command
        shutting down cleanly still gets the whole grace its SIGTERM opened."""
        daemon = dispatching_daemon
        assert daemon_mod.KILL_ESCALATION_GRACE_SECONDS > 1.0
        jobid = "killgrace001"
        running = _dispatch_started(
            daemon, jobid, command=_SIGTERM_IGNORING_LOCK_COMMAND
        )
        assert running.pgid is not None
        workspace = daemon.jobs_dir / jobid
        try:
            kill_mod.kill_job("localhost", jobid, queue_dir=daemon.queue_dir)
            daemon._running[jobid].popen.wait(timeout=_LIVENESS_SECONDS)
            for _ in range(5):
                daemon.iterate()
                time.sleep(0.02)
            assert jobid in daemon._terminal_survivors
            assert not _command_exited(workspace, budget=0.3), (
                "the grace was cut short by an immediate SIGKILL"
            )
        finally:
            _kill_group_best_effort(daemon.jobs_dir / jobid, running.pgid)

    def test_a_surviving_group_keeps_its_slot_until_it_is_reaped(
        self, dispatching_daemon: Daemon
    ) -> None:
        """Its cpus stay charged while it runs, so the host is not
        oversubscribed by dispatching over a job that is still on it."""
        daemon = dispatching_daemon
        jobid = "killcap00001"
        running = _dispatch_started(
            daemon,
            jobid,
            command=_SIGTERM_IGNORING_LOCK_COMMAND,
            cpus=daemon.max_cpus,
        )
        assert running.pgid is not None
        try:
            kill_mod.kill_job("localhost", jobid, queue_dir=daemon.queue_dir)
            daemon._running[jobid].popen.wait(timeout=_LIVENESS_SECONDS)
            daemon._reconcile_running()
            assert jobid in daemon._terminal_survivors

            waiting = "killcap00002"
            waiting_ws = daemon.jobs_dir / waiting
            waiting_ws.mkdir(parents=True, exist_ok=True)
            JobSpec(
                id=waiting,
                command=[sys.executable, "-c", "pass"],
                cwd=str(waiting_ws),
                cpus=daemon.max_cpus,
            ).write(daemon._spec_path(waiting))

            daemon.iterate()
            assert waiting not in daemon._running, (
                "the host's cpus are still held by the killed job's group"
            )
            assert JobSpec.read(daemon._spec_path(waiting)).state == JobState.PENDING

            # Once the escalation ends the group, the slot is free again.
            daemon._terminal_survivors[jobid].deadline = time.monotonic() - 1.0
            daemon._reconcile_terminal_survivors()
            assert not _tracked(daemon, jobid)
            _drive(daemon, waiting, lambda s: s.state == JobState.COMPLETED)
        finally:
            _kill_group_best_effort(daemon.jobs_dir / jobid, running.pgid)

    def test_watchdog_kill_of_a_sigterm_ignoring_command_is_escalated(
        self, dispatching_daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The watchdog's own SIGTERM -> grace -> SIGKILL loses its grace state
        the moment ``_record_finish`` unregisters the job, so its escalation
        has to be carried by the same survivor record."""
        daemon = dispatching_daemon
        monkeypatch.setattr(daemon_mod, "KILL_ESCALATION_GRACE_SECONDS", 0.0)
        jobid = "wdogdeaf0001"
        running = _dispatch_started(
            daemon, jobid, command=_SIGTERM_IGNORING_LOCK_COMMAND
        )
        assert running.pgid is not None
        workspace = daemon.jobs_dir / jobid
        try:
            spec_path = daemon._spec_path(jobid)
            spec = JobSpec.read(spec_path)
            spec.wall_time_seconds = 1
            spec.write(spec_path)
            # Rewind the watchdog's own anchor rather than sleeping out the
            # smallest budget a spec can carry.
            daemon.watchdog._states[jobid].started_monotonic -= 100.0

            daemon._watchdog_pass()
            assert JobSpec.read(spec_path).state == JobState.TIME_EXCEEDED
            daemon._running[jobid].popen.wait(timeout=_LIVENESS_SECONDS)
            assert not _command_exited(workspace, budget=0.3)

            daemon._reconcile_running()
            assert jobid not in daemon.watchdog._states, (
                "the reap drops the watchdog state that tracked the grace"
            )
            assert jobid in daemon._terminal_survivors

            daemon._reconcile_terminal_survivors()
            assert _command_exited(workspace, budget=_LIVENESS_SECONDS), (
                "the SIGTERM-ignoring command outlived its watchdog kill"
            )
            assert not _tracked(daemon, jobid)
            assert JobSpec.read(spec_path).state == JobState.TIME_EXCEEDED
        finally:
            _kill_group_best_effort(daemon.jobs_dir / jobid, running.pgid)

    def test_an_ordinary_kill_releases_without_waiting_out_the_grace(
        self, dispatching_daemon: Daemon
    ) -> None:
        """The boundary: a command that honours SIGTERM must not have its
        slot held for the grace. Liveness releases the record, not the clock,
        so this finishes well inside the untouched default grace."""
        daemon = dispatching_daemon
        release_within = (
            daemon_mod.KILL_ESCALATION_GRACE_SECONDS - _RELEASE_MARGIN_SECONDS
        )
        assert release_within > 0
        jobid = "killclean001"
        running = _dispatch_started(daemon, jobid)
        assert running.pgid is not None
        try:
            kill_mod.kill_job("localhost", jobid, queue_dir=daemon.queue_dir)
            deadline = time.monotonic() + release_within
            while _tracked(daemon, jobid):
                assert time.monotonic() < deadline, (
                    "an ordinary kill held its slot into the SIGKILL grace"
                )
                daemon.iterate()
                time.sleep(0.02)
            assert _command_exited(daemon.jobs_dir / jobid, budget=_LIVENESS_SECONDS)
            assert JobSpec.read(daemon._spec_path(jobid)).state == JobState.KILLED
        finally:
            _kill_group_best_effort(daemon.jobs_dir / jobid, running.pgid)
