"""The admin-update dispatch hold is scoped to what the marker protects.

Regression for 2026-07-25: a managed host_f ``vibeqc-dev`` rebuild
(``envs=['scheduler-runtime:host_f:vibeqc-dev']``) parked a host_c SLURM smoke
(``d446f1a62143``) on an idle, healthy cluster, because the driver's daemon
paused ALL dispatch while ANY admin-update marker existed. The marker names
exactly what it protects; the daemon now holds only those targets:

  * ``scheduler:<host>`` / ``scheduler-runtime:<host>:<program>`` hold that
    scheduler host's handoffs only;
  * a plain env name (a venv update on the daemon's own host) holds local
    execution only;
  * anything unrecognised holds everything — fail-safe, never fail-open.
"""
from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests import test_terminal_reaping as terminal_reaping
from vq import admin, config, paths
from vq.admin import LOCAL_DISPATCH_SCOPE, AdminUpdateMarker
from vq.daemon import Daemon
from vq.spec import JobSpec, JobState


@pytest.fixture
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "config.toml").write_text('default_host = "localhost"\n')
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


def _reap_local_jobs(daemon: Daemon) -> None:
    """Kill each dispatched job's whole process group, not just its wrapper.

    ``vq.resource_receipt`` leads the job's session and forks the command into
    the same group, so ``popen.kill()`` alone orphans the command. When the
    host-pressure pause had already SIGSTOPped it, the command outlived the
    session, still stopped and reparented to init: two such orphans, one a
    bare ``sleep 5``, turned up on a dev machine on 2026-09-10. SIGKILL
    reaches a stopped process, so no SIGCONT is needed.

    A leader that already exited but is not yet reaped makes Darwin answer
    ``killpg`` with EPERM rather than ESRCH, so any OSError is tolerated; the
    wait below reaps it either way.
    """
    for rj in list(daemon._running.values()):
        # An unreaped leader keeps its pid, which is also the group id.
        if rj.popen.returncode is None:
            with contextlib.suppress(OSError):
                os.killpg(rj.popen.pid, signal.SIGKILL)
        with contextlib.suppress(subprocess.TimeoutExpired):
            rj.popen.wait(timeout=terminal_reaping._LIVENESS_SECONDS)
        rj.close_logs()


@pytest.fixture
def daemon(state_dir: Path) -> Iterator[Daemon]:
    d = Daemon(
        max_cpus=4,
        poll_interval=0.05,
        queue_dir=paths.queue_dir(),
        jobs_dir=paths.jobs_dir(),
    )
    yield d
    _reap_local_jobs(d)


def _submit(
    daemon: Daemon,
    jobid: str,
    command: list[str],
    *,
    scheduler_target: str | None = None,
) -> JobSpec:
    workspace = daemon.jobs_dir / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(
        id=jobid,
        command=command,
        cwd=str(workspace),
        cpus=1,
        scheduler_target=scheduler_target,
    )
    spec.write(daemon._spec_path(jobid))
    return spec


def _drive_until(daemon: Daemon, jobid: str, state: JobState, budget: float = 5.0) -> JobSpec:
    deadline = time.monotonic() + budget
    while time.monotonic() < deadline:
        daemon.iterate()
        spec = JobSpec.read(daemon._spec_path(jobid))
        if spec.state == state:
            return spec
        time.sleep(0.02)
    return JobSpec.read(daemon._spec_path(jobid))


# ----------------------------------------------------------------------
# The scope parser
# ----------------------------------------------------------------------


def _marker(envs: list[str]) -> AdminUpdateMarker:
    return AdminUpdateMarker(
        envs=envs, host="host_f", state="building",
        started_at="2026-07-25T00:00:00+00:00", pid=1, vq_version="0.14.0",
    )


def test_scheduler_runtime_scope_names_only_that_host() -> None:
    scope = admin.admin_update_marker_scope(
        _marker(["scheduler-runtime:host_f:vibeqc-dev"])
    )
    assert scope == frozenset({"host_f"})


def test_helper_scope_names_only_that_host() -> None:
    assert admin.admin_update_marker_scope(
        _marker(["scheduler:host_c"])
    ) == frozenset({"host_c"})


def test_plain_env_scope_holds_local_execution_only() -> None:
    assert admin.admin_update_marker_scope(
        _marker(["vibeqc-dev", "vibeqc-release"])
    ) == frozenset({LOCAL_DISPATCH_SCOPE})


def test_unrecognised_scoped_env_fails_safe_to_global() -> None:
    """A colon form written by a different vq must hold everything."""
    assert admin.admin_update_marker_scope(
        _marker(["scheduler-runtime:host_f:vibeqc-dev", "future:shape:x:y"])
    ) is None


def test_unreadable_marker_fails_safe_to_global() -> None:
    assert admin.admin_update_marker_scope(None) is None
    assert admin.admin_update_marker_scope(_marker([])) is None


# ----------------------------------------------------------------------
# The daemon dispatch gate
# ----------------------------------------------------------------------


class TestScopedDispatchHold:
    def test_local_job_dispatches_during_a_host_f_rebuild(
        self, daemon: Daemon
    ) -> None:
        """The 2026-07-25 over-block, local flavor: a scheduler-runtime
        marker for host_f must not park work on the driver itself."""
        admin.write_admin_update_marker(
            envs=["scheduler-runtime:host_f:vibeqc-dev"], host="host_f"
        )
        _submit(daemon, "j-local", ["true"])

        spec = _drive_until(daemon, "j-local", JobState.COMPLETED)

        assert spec.state == JobState.COMPLETED
        assert admin.admin_update_marker_exists() is True  # never reaped

    @pytest.mark.no_autopatch_host_pressure
    def test_local_job_is_paused_by_injected_host_pressure(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """#563 mechanism proof: the daemon reads the pressure probe through
        the module, so a pinned 90 % pauses the local job (SUSPENDED, tagged
        ``watchdog_host_pressure``). At the parent the default-argument
        binding ignored this pin and the job completed: that is the
        behavioural fail-first, and it is also why the gate could read the
        CI host's real memory."""
        from vq.daemon import HOST_PRESSURE_PAUSE_TAG

        monkeypatch.setattr(
            "vq.watchdog.read_host_memory_pressure_pct", lambda: 90.0
        )
        admin.write_admin_update_marker(
            envs=["scheduler-runtime:host_f:vibeqc-dev"], host="host_f"
        )
        _submit(daemon, "j-local-pressured", ["sleep", "5"])
        spec = _drive_until(daemon, "j-local-pressured", JobState.SUSPENDED)
        assert spec.state == JobState.SUSPENDED
        assert spec.paused_by == HOST_PRESSURE_PAUSE_TAG

    @pytest.mark.no_autopatch_host_pressure
    def test_teardown_reaps_a_paused_command_the_wrapper_already_forked(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The fixture teardown must not strand the job the test above pauses.

        That pause usually lands before ``vq.resource_receipt`` has forked, so
        killing the wrapper freed the whole job. When the fork won the race,
        the SIGSTOPped command survived the session, stopped for good. Force
        that ordering here: let the command start, then raise the pressure.

        The command's own flock is the liveness probe, not ``killpg(pgid, 0)``
        on its group. It asks about exactly the process this test started, and
        it has no timing in it: an exited command releases the lock even while
        it waits as a zombie for launchd, and a stopped one keeps holding it.
        The group probe raced that reap, since Darwin answers EPERM for a group
        whose last member is dying: 7 failures in 24 runs at load 135 on
        2026-09-13, both with and without other pytest sessions running
        beside it. The collision was with load, not with those sessions.
        """
        liveness = terminal_reaping._LIVENESS_SECONDS  # bounds hangs only
        pressure = [10.0]
        monkeypatch.setattr(
            "vq.watchdog.read_host_memory_pressure_pct", lambda: pressure[0]
        )
        command = terminal_reaping._LOCK_HOLDING_COMMAND
        _submit(daemon, "j-forked", [sys.executable, "-c", command])
        workspace = daemon.jobs_dir / "j-forked"
        running = _drive_until(daemon, "j-forked", JobState.RUNNING, liveness)
        assert running.state == JobState.RUNNING
        wrapper = daemon._running["j-forked"].popen
        deadline = time.monotonic() + liveness
        while not (workspace / "started").exists():
            assert time.monotonic() < deadline, "the wrapper never started the command"
            time.sleep(0.01)

        pressure[0] = 90.0
        spec = _drive_until(daemon, "j-forked", JobState.SUSPENDED, liveness)
        assert spec.state == JobState.SUSPENDED
        assert spec.pgid is not None

        _reap_local_jobs(daemon)

        if not terminal_reaping._command_exited(workspace, budget=liveness):
            # Never leak what this pins; signals only while the lock is held.
            terminal_reaping._kill_group_best_effort(workspace, spec.pgid)
            pytest.fail("the SIGSTOPped command outlived the fixture teardown")
        assert wrapper.returncode is not None, "the teardown never reaped the wrapper"

    def test_local_job_dispatch_is_immune_to_the_real_host(
        self, daemon: Daemon
    ) -> None:
        """#563: with the conftest pin in force (10 %), a local job completes
        regardless of the machine running the suite. Same route as the
        pressured test above with the feature off (L125)."""
        admin.write_admin_update_marker(
            envs=["scheduler-runtime:host_f:vibeqc-dev"], host="host_f"
        )
        _submit(daemon, "j-local-quiet", ["true"])
        spec = _drive_until(daemon, "j-local-quiet", JobState.COMPLETED)
        assert spec.state == JobState.COMPLETED
        assert spec.paused_by is None

    def test_other_cluster_dispatches_during_a_host_f_rebuild(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """THE host_c case: an independent cluster's handoff proceeds."""
        admin.write_admin_update_marker(
            envs=["scheduler-runtime:host_f:vibeqc-dev"], host="host_f"
        )
        started: list[str] = []

        def fake_start_scheduler_job(spec: JobSpec) -> bool:
            started.append(spec.id)
            return False  # "not dispatched" keeps daemon bookkeeping inert

        monkeypatch.setattr(
            daemon, "_start_scheduler_job", fake_start_scheduler_job
        )
        _submit(daemon, "j-host_c", ["run.py"], scheduler_target="host_c")

        daemon.iterate()

        assert started == ["j-host_c"], (
            "a host_c handoff must not wait on a host_f rebuild"
        )

    def test_the_named_cluster_is_still_held(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        admin.write_admin_update_marker(
            envs=["scheduler-runtime:host_f:vibeqc-dev"], host="host_f"
        )
        started: list[str] = []
        monkeypatch.setattr(
            daemon,
            "_start_scheduler_job",
            lambda spec: started.append(spec.id) or False,
        )
        _submit(daemon, "j-host_f", ["run.py"], scheduler_target="host_f")

        for _ in range(3):
            daemon.iterate()
            time.sleep(0.02)

        assert started == []
        assert JobSpec.read(daemon._spec_path("j-host_f")).state == JobState.PENDING

    def test_a_local_env_update_still_holds_local_dispatch(
        self, daemon: Daemon
    ) -> None:
        """The original protection stands: no dispatch into a venv being
        rebuilt on this host."""
        admin.write_admin_update_marker(envs=["vibeqc-dev"], host="localhost")
        _submit(daemon, "j-local", ["true"])

        for _ in range(3):
            daemon.iterate()
            time.sleep(0.02)

        assert (
            JobSpec.read(daemon._spec_path("j-local")).state == JobState.PENDING
        )

    def test_a_local_env_update_releases_scheduler_handoffs(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The v0.12.1 host_c lesson, marker flavor: a driver-local venv
        rebuild has no bearing on cluster handoffs."""
        admin.write_admin_update_marker(envs=["vibeqc-queue"], host="localhost")
        started: list[str] = []
        monkeypatch.setattr(
            daemon,
            "_start_scheduler_job",
            lambda spec: started.append(spec.id) or False,
        )
        _submit(daemon, "j-host_c", ["run.py"], scheduler_target="host_c")

        daemon.iterate()

        assert started == ["j-host_c"]
