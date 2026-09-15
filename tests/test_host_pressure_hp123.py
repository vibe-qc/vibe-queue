"""HP-1/2/3 — host-pressure scheduling hardening (v0.8.24, audit § B).

* HP-1: jobs auto-paused for host memory pressure are stranded SUSPENDED
  across a daemon restart (the watchdog's in-memory resume record dies with
  the process). The startup re-seed re-arms it — for watchdog pauses only,
  never operator pauses.
* HP-2: the daemon must not dispatch NEW jobs while it has auto-paused for
  pressure (that re-loads the host it's relieving). Composes with drain.
* HP-3: reattached orphans must be auto-pause candidates too, not just
  in-process (_running) jobs.
"""
from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from unittest.mock import patch

import pytest

from vq.daemon import HOST_PRESSURE_PAUSE_TAG, Daemon, _OrphanJob
from vq.spec import JobSpec, JobState
from vq.watchdog import HostPressureAction, HostPressureVerdict, Watchdog


@pytest.fixture
def daemon(tmp_path: Path) -> Iterator[Daemon]:
    d = Daemon(
        max_cpus=8, poll_interval=0.05,
        queue_dir=tmp_path / "queue", jobs_dir=tmp_path / "jobs",
    )
    d.queue_dir.mkdir(parents=True, exist_ok=True)
    d.jobs_dir.mkdir(parents=True, exist_ok=True)
    yield d


def _write_spec(daemon: Daemon, jobid: str, *, state: JobState, **fields: object) -> None:
    ws = daemon.jobs_dir / jobid
    ws.mkdir(parents=True, exist_ok=True)
    JobSpec(
        id=jobid, command=["sleep", "1"], cwd=str(ws), cpus=1,
        state=state, **fields,
    ).write(daemon._spec_path(jobid))


# ---------------------------------------------------------------------------
# HP-1 — watchdog re-seed primitive
# ---------------------------------------------------------------------------


class TestWatchdogReseed:
    def test_reseed_re_arms_resume_for_exactly_those_jobs(self) -> None:
        wd = Watchdog(interval_seconds=60)
        assert wd.host_pressure_active is False
        wd.reseed_host_pressure_paused(["jobA", "jobB"])
        assert wd.host_pressure_active is True
        # pressure recedes -> RESUME targets exactly the re-seeded jobs
        verdict = wd.check_host_pressure([], _pressure_reader=lambda: 30.0)
        assert verdict.action == HostPressureAction.RESUME
        assert set(verdict.jobids) == {"jobA", "jobB"}
        assert wd.host_pressure_active is False  # cleared after resume

    def test_reseed_empty_is_noop(self) -> None:
        wd = Watchdog(interval_seconds=60)
        wd.reseed_host_pressure_paused([])
        assert wd.host_pressure_active is False


# ---------------------------------------------------------------------------
# HP-1 — daemon startup re-seed (operator pauses excluded)
# ---------------------------------------------------------------------------


class TestStartupReseed:
    def test_reseeds_watchdog_pauses_only(self, daemon: Daemon) -> None:
        _write_spec(daemon, "hpauto000001", state=JobState.SUSPENDED,
                    paused_by=HOST_PRESSURE_PAUSE_TAG)
        # operator pauses — bare `vq pause` (None) and a tagged operator pause
        _write_spec(daemon, "hpoper000001", state=JobState.SUSPENDED, paused_by=None)
        _write_spec(daemon, "hpoper000002", state=JobState.SUSPENDED,
                    paused_by="kids-gaming")
        # a RUNNING job must be ignored entirely
        _write_spec(daemon, "hprun0000001", state=JobState.RUNNING)

        daemon._reseed_host_pressure_pauses_at_startup()

        assert daemon.watchdog.host_pressure_active is True
        assert daemon.watchdog._host_pressure_paused_jobids == {"hpauto000001"}

    def test_no_tagged_jobs_leaves_watchdog_inactive(self, daemon: Daemon) -> None:
        _write_spec(daemon, "hpoper000001", state=JobState.SUSPENDED, paused_by=None)
        daemon._reseed_host_pressure_pauses_at_startup()
        assert daemon.watchdog.host_pressure_active is False

    def test_skipped_when_enforcement_off(self, daemon: Daemon) -> None:
        """If host-pressure enforcement is off, check_host_pressure never
        fires RESUME — re-seeding active=True would gate dispatch forever, so
        we must NOT re-seed (just warn)."""
        daemon.watchdog.enforce_host_pressure_pause = False
        _write_spec(daemon, "hpauto000001", state=JobState.SUSPENDED,
                    paused_by=HOST_PRESSURE_PAUSE_TAG)
        daemon._reseed_host_pressure_pauses_at_startup()
        assert daemon.watchdog.host_pressure_active is False


# ---------------------------------------------------------------------------
# HP-2 — dispatch gating
# ---------------------------------------------------------------------------


class TestDispatchGating:
    def test_pressure_active_gates_new_dispatch(self, daemon: Daemon) -> None:
        _write_spec(daemon, "hppend000001", state=JobState.PENDING)
        started: list[str] = []

        def fake_start(spec: JobSpec) -> bool:
            started.append(spec.id)
            return True

        # Pressure active -> no dispatch.
        daemon.watchdog._host_pressure_active = True
        with patch.object(daemon, "_start_job", side_effect=fake_start):
            daemon._dispatch_pending()
        assert started == [], "HP-2: must not dispatch while pressure-paused"

        # Pressure recedes -> dispatch proceeds.
        daemon.watchdog._host_pressure_active = False
        with patch.object(daemon, "_start_job", side_effect=fake_start):
            daemon._dispatch_pending()
        assert started == ["hppend000001"]


# ---------------------------------------------------------------------------
# HP-3 — orphans are auto-pause candidates
# ---------------------------------------------------------------------------


class TestOrphanCandidates:
    def test_reattached_orphan_is_a_pressure_candidate(self, daemon: Daemon) -> None:
        # A reattached orphan with a RUNNING spec — pre-HP-3 it was invisible
        # to the host-pressure pass (candidate list was _running only).
        _write_spec(daemon, "hporph000001", state=JobState.RUNNING, pgid=4242)
        daemon._orphans["hporph000001"] = _OrphanJob(pgid=4242, cpus=1, mem_mb=None)

        captured: list[list[str]] = []

        def fake_check(jobids: list[str], **_kw: object) -> HostPressureVerdict:
            captured.append(list(jobids))
            return HostPressureVerdict(
                action=HostPressureAction.NO_OP, jobids=[], pressure_pct=50.0,
            )

        with patch.object(daemon.watchdog, "check_host_pressure", side_effect=fake_check):
            daemon._host_pressure_pass()

        assert captured == [["hporph000001"]], (
            "HP-3: the reattached orphan must be a host-pressure candidate"
        )
