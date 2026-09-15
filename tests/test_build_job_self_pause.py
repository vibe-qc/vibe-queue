"""The build-as-job self-pause wedge and impossible-spec admission.

2026-07-25 fleet incident: `vq submit --refresh vibeqc-dev` build jobs on
host_e (e4c32b654b03), host_b (ca78bea5db13) and localhost (8cd5ad2cd096) sat
"running" ~9 h past their wall clocks in process state T. The build job runs
`vq build-env` -> admin.update_env -> pause_all, which SIGSTOPped the build
job's OWN process group: the process froze inside killpg, before the
SUSPENDED write, the marker acquire, or any event — a self-inflicted,
unobservable wedge. Separately, host_a carried a refresh build requesting 16
CPUs against an 8-CPU daemon: eternally pending, reading as "queued" while
meaning "impossible".

Pinned here: pause_job refuses its own process group; pause_all/update_env
exclude the executing job; the daemon terminal-fails a spec whose request
can never fit its configured caps.
"""
from __future__ import annotations

import os
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from vq import config, paths, pause_resume
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


def _running_spec(jobid: str, *, pgid: int, cpus: int = 1) -> JobSpec:
    workspace = paths.jobs_dir() / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(
        id=jobid, command=["true"], cwd=str(workspace), cpus=cpus,
        state=JobState.RUNNING, pgid=pgid,
    )
    spec.write(paths.queue_dir() / f"{jobid}.json")
    return spec


def test_pause_job_refuses_its_own_process_group(state_dir: Path) -> None:
    """THE WEDGE, at its root. SIGSTOPping our own pgid freezes the pauser
    before it can record anything; it must refuse instead."""
    _running_spec("selfjob", pgid=os.getpgrp())

    with pytest.raises(pause_resume.PauseError, match="own process group"):
        pause_resume.pause_job("localhost", "selfjob")

    spec = JobSpec.read(paths.queue_dir() / "selfjob.json")
    assert spec.state == JobState.RUNNING, "a refused pause must change nothing"


def test_pause_all_excludes_the_executing_job(state_dir: Path) -> None:
    """update_env passes its own VQ_JOB_ID; pause_all must skip it and
    still pause everything else."""
    _running_spec("buildjob", pgid=os.getpgrp())

    summary = pause_resume.pause_all(
        "localhost", paused_by="admin-update-test",
        exclude_jobids={"buildjob"},
    )

    assert "paused 0 jobs" in summary
    spec = JobSpec.read(paths.queue_dir() / "buildjob.json")
    assert spec.state == JobState.RUNNING


def test_pause_all_without_exclusion_is_saved_by_the_pgid_guard(
    state_dir: Path,
) -> None:
    """Defense in depth: even a caller that forgets the exclusion cannot
    self-freeze — the guard converts the self-pause into a per-job error."""
    _running_spec("buildjob", pgid=os.getpgrp())

    summary = pause_resume.pause_all("localhost", paused_by="x")

    # The process reached this line at all — before the guard, killpg froze
    # the test process here. The job stays RUNNING.
    assert JobSpec.read(
        paths.queue_dir() / "buildjob.json"
    ).state == JobState.RUNNING
    assert "error" in summary or "paused 0" in summary


@pytest.fixture
def daemon(state_dir: Path) -> Iterator[Daemon]:
    d = Daemon(
        max_cpus=4,
        max_mem_mb=1000,
        poll_interval=0.05,
        queue_dir=paths.queue_dir(),
        jobs_dir=paths.jobs_dir(),
    )
    yield d
    for rj in list(d._running.values()):
        try:
            rj.popen.kill()
            rj.popen.wait(timeout=1)
        except Exception:
            pass
        rj.close_logs()


def _pending_spec(
    daemon: Daemon, jobid: str, *, cpus: int = 1, mem_mb: int | None = None,
    build_env: str | None = None,
) -> JobSpec:
    workspace = daemon.jobs_dir / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(
        id=jobid, command=["true"], cwd=str(workspace), cpus=cpus,
        mem_mb=mem_mb, build_env=build_env,
    )
    spec.write(daemon._spec_path(jobid))
    return spec


class TestImpossibleSpecAdmission:
    def test_cpu_request_beyond_caps_fails_fast(self, daemon: Daemon) -> None:
        """THE host_a CASE: a 16-CPU refresh BUILD against an 8-CPU daemon
        must fail with a named reason, not pend forever."""
        _pending_spec(daemon, "j-toobig", cpus=8, build_env="vibeqc-dev")

        daemon.iterate()

        spec = JobSpec.read(daemon._spec_path("j-toobig"))
        assert spec.state == JobState.FAILED
        assert "impossible resource request" in (spec.failure_reason or "")
        assert "--max-cpus 4" in (spec.failure_reason or "")

    def test_memory_request_beyond_caps_fails_fast(self, daemon: Daemon) -> None:
        _pending_spec(
            daemon, "j-toofat", cpus=1, mem_mb=2000, build_env="vibeqc-dev"
        )

        daemon.iterate()

        spec = JobSpec.read(daemon._spec_path("j-toofat"))
        assert spec.state == JobState.FAILED
        assert "--max-mem-mb 1000" in (spec.failure_reason or "")

    def test_operator_spec_over_caps_stays_pending(self, daemon: Daemon) -> None:
        """Deliberate scoping: only auto-generated build jobs fail fast.
        An operator spec over the caps stays pending — restarting the
        daemon with a higher --max-cpus is a supported path to run it."""
        _pending_spec(daemon, "j-operator", cpus=8)

        for _ in range(3):
            daemon.iterate()

        assert JobSpec.read(
            daemon._spec_path("j-operator")
        ).state == JobState.PENDING

    def test_request_at_exactly_the_cap_still_dispatches(
        self, daemon: Daemon
    ) -> None:
        """The guard is strictly 'can never fit', not 'is big'."""
        _pending_spec(daemon, "j-max", cpus=4, mem_mb=1000)

        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            daemon.iterate()
            if JobSpec.read(
                daemon._spec_path("j-max")
            ).state == JobState.COMPLETED:
                break
            time.sleep(0.02)

        assert JobSpec.read(
            daemon._spec_path("j-max")
        ).state == JobState.COMPLETED
