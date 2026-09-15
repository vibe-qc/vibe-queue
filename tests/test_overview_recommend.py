"""v0.7.18 *Kay's Object* — `vq overview --recommend` tests.

Pins:

1. Load fields (``running_cpus`` / ``pending_cpus``) populate from
   the spec list correctly — sum spec.cpus across the right state.
2. ``recommend_host`` ranking honors the filter (unreachable /
   drained / dead-daemon hosts excluded) and the tiebreak chain
   (workload asc → pending_cpus asc → -idle asc → host name).
3. ``recommend_host`` returns ``None`` when no host qualifies.
4. JSON round-trip carries the new fields through the
   gather_overview_remote path.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from vq import config, lifecycle
from vq.overview import (
    HostOverview,
    _count_specs,
    _overview_from_json,
    format_overview_json,
    format_overview_text,
    gather_scheduler_overview,
    recommend_host,
)
from vq.spec import JobSpec, JobState


def _spec(
    jobid: str,
    *,
    state: JobState,
    cpus: int = 1,
    finished_at: str | None = None,
) -> JobSpec:
    return JobSpec(
        id=jobid,
        command=["true"],
        cwd="/tmp",
        cpus=cpus,
        submitter="x@y",
        state=state,
        finished_at=finished_at,
    )


def _healthy_verdict() -> lifecycle.ContractVerdict:
    return lifecycle.ContractVerdict(
        ok=True,
        manager_pid=1,
        loginctl_state="active",
        loginctl_runtime_path="/run/user/1000",
        systemctl_user_reachable=True,
        vq_daemon_state="active",
        vq_daemon_main_pid=42,
        daemon_pidfile_pid=42,
        daemon_process_alive=True,
        memory_pressure_pct=None,
        findings=[],
    )


_UNCONFIRMED_SCHEDULER_PHASES = (
    "poll_failed",
    "finishing",
    "marker_probe_failed",
    "fetch_failed",
    "reattach_failed",
    "mystery_phase",
)


# ----------------------------------------------------------------------
# Load-field accounting
# ----------------------------------------------------------------------


class TestLoadFieldCounts:
    def test_running_cpus_sums_spec_cpus(self) -> None:
        specs = [
            _spec("a000", state=JobState.RUNNING, cpus=4),
            _spec("b000", state=JobState.RUNNING, cpus=8),
            _spec("c000", state=JobState.PENDING, cpus=2),
        ]
        _, _, _, running, pending = _count_specs(
            specs, recent_window=timedelta(hours=24),
        )
        assert running == 12
        assert pending == 2

    def test_terminal_specs_not_counted(self) -> None:
        """Only RUNNING / PENDING contribute. COMPLETED, FAILED, etc.
        are excluded (they don't hold capacity)."""
        specs = [
            _spec("a000", state=JobState.RUNNING, cpus=4),
            _spec("b000", state=JobState.COMPLETED, cpus=8,
                  finished_at="2026-05-28T10:00:00+00:00"),
            _spec("c000", state=JobState.FAILED, cpus=16,
                  finished_at="2026-05-28T10:00:00+00:00"),
        ]
        _, _, _, running, pending = _count_specs(
            specs, recent_window=timedelta(hours=24),
        )
        assert running == 4
        assert pending == 0

    def test_suspended_not_counted(self) -> None:
        """SUSPENDED jobs hold no CPU (the watchdog SIGSTOP'd them).
        The recommendation ranking shouldn't penalize a host whose
        only "load" is suspended jobs."""
        specs = [_spec("a000", state=JobState.SUSPENDED, cpus=16)]
        _, _, _, running, pending = _count_specs(
            specs, recent_window=timedelta(hours=24),
        )
        assert running == 0
        assert pending == 0

    def test_empty_spec_list(self) -> None:
        _, _, _, running, pending = _count_specs(
            [], recent_window=timedelta(hours=24),
        )
        assert running == 0
        assert pending == 0


# ----------------------------------------------------------------------
# recommend_host — filter + ranking
# ----------------------------------------------------------------------


class TestRecommendHost:
    def test_empty_fleet_returns_none(self) -> None:
        assert recommend_host([]) is None

    def test_unreachable_hosts_filtered(self) -> None:
        only = HostOverview(host="alpha", reachable=False, error="down")
        assert recommend_host([only]) is None

    def test_drained_hosts_filtered(self) -> None:
        from vq.drain import DrainState
        d = DrainState(enabled=True, reason="testing")
        only = HostOverview(
            host="alpha",
            reachable=True,
            daemon_health=_healthy_verdict(),
            drain_state=d,
        )
        assert recommend_host([only]) is None, (
            "a fully-drained host shouldn't be recommended for new "
            "submissions — it won't dispatch them"
        )

    def test_dead_daemon_filtered(self) -> None:
        verdict = lifecycle.ContractVerdict(
            ok=False, manager_pid=None,
            loginctl_state=None, loginctl_runtime_path=None,
            systemctl_user_reachable=False,
            vq_daemon_state=None, vq_daemon_main_pid=None,
            daemon_pidfile_pid=None,
            daemon_process_alive=False, memory_pressure_pct=None,
            findings=["FAIL: daemon not running"],
        )
        only = HostOverview(
            host="alpha", reachable=True, daemon_health=verdict,
        )
        assert recommend_host([only]) is None

    def test_least_loaded_wins(self) -> None:
        a = HostOverview(
            host="alpha", reachable=True,
            daemon_health=_healthy_verdict(),
            running_cpus=10, pending_cpus=4,  # workload 14
        )
        b = HostOverview(
            host="bravo", reachable=True,
            daemon_health=_healthy_verdict(),
            running_cpus=2, pending_cpus=0,  # workload 2
        )
        assert recommend_host([a, b]) == "bravo"

    def test_unconfirmed_scheduler_cpus_remain_committed(self) -> None:
        uncertain = HostOverview(
            host="alpha",
            reachable=True,
            daemon_health=_healthy_verdict(),
            max_cpus=16,
            pending_cpus=16,
            unconfirmed_scheduler_jobs=1,
            unconfirmed_scheduler_cpus=16,
        )
        confirmed = HostOverview(
            host="bravo",
            reachable=True,
            daemon_health=_healthy_verdict(),
            max_cpus=16,
            running_cpus=4,
        )

        assert recommend_host([uncertain, confirmed], job_cpus=1) == "bravo"

    def test_unconfirmed_scheduler_subset_is_not_double_charged(self) -> None:
        uncertain = HostOverview(
            host="alpha",
            reachable=True,
            daemon_health=_healthy_verdict(),
            max_cpus=16,
            pending_cpus=8,
            unconfirmed_scheduler_jobs=1,
            unconfirmed_scheduler_cpus=8,
        )
        confirmed = HostOverview(
            host="bravo",
            reachable=True,
            daemon_health=_healthy_verdict(),
            max_cpus=16,
            running_cpus=10,
        )

        assert recommend_host([uncertain, confirmed], job_cpus=7) == "alpha"

    def test_tiebreak_by_pending_cpus(self) -> None:
        """Same total workload but different pending balances: the
        host with the smaller pending queue wins (favours hosts with
        running-but-not-queued over hosts with queued-but-not-yet-
        running, since the latter is closer to capacity)."""
        a = HostOverview(
            host="alpha", reachable=True,
            daemon_health=_healthy_verdict(),
            running_cpus=2, pending_cpus=4,  # workload 6, pending 4
        )
        b = HostOverview(
            host="bravo", reachable=True,
            daemon_health=_healthy_verdict(),
            running_cpus=5, pending_cpus=1,  # workload 6, pending 1
        )
        assert recommend_host([a, b]) == "bravo"

    def test_tiebreak_by_idle_seconds(self) -> None:
        """Same workload + pending: prefer the host that's been idle
        longer (operator wants to put new work there)."""
        a = HostOverview(
            host="alpha", reachable=True,
            daemon_health=_healthy_verdict(),
            running_cpus=0, pending_cpus=0,
            idle_seconds=100,
        )
        b = HostOverview(
            host="bravo", reachable=True,
            daemon_health=_healthy_verdict(),
            running_cpus=0, pending_cpus=0,
            idle_seconds=10_000,
        )
        assert recommend_host([a, b]) == "bravo"

    def test_tiebreak_by_host_name(self) -> None:
        """Fully tied: alphabetical for determinism (tests can
        assert without flake)."""
        a = HostOverview(
            host="zulu", reachable=True,
            daemon_health=_healthy_verdict(),
            running_cpus=0, pending_cpus=0,
        )
        b = HostOverview(
            host="alpha", reachable=True,
            daemon_health=_healthy_verdict(),
            running_cpus=0, pending_cpus=0,
        )
        assert recommend_host([a, b]) == "alpha"

    def test_unhealthy_alphabetically_first_skipped(self) -> None:
        """Sanity: a dead-daemon host that sorts first alphabetically
        is still skipped — the filter runs BEFORE the ranking."""
        dead = HostOverview(
            host="alpha", reachable=True,
            daemon_health=lifecycle.ContractVerdict(
                ok=False, manager_pid=None,
                loginctl_state=None, loginctl_runtime_path=None,
                systemctl_user_reachable=False,
                vq_daemon_state=None, vq_daemon_main_pid=None,
                daemon_pidfile_pid=None,
            daemon_process_alive=False, memory_pressure_pct=None,
                findings=["FAIL"],
            ),
        )
        live = HostOverview(
            host="zulu", reachable=True,
            daemon_health=_healthy_verdict(),
        )
        assert recommend_host([dead, live]) == "zulu"


# ----------------------------------------------------------------------
# JSON round-trip carries the load fields
# ----------------------------------------------------------------------


class TestJsonRoundtripCarriesLoadFields:
    def test_payload_keeps_compatible_load_and_unconfirmed_subset(self) -> None:
        ov = HostOverview(
            host="alpha", reachable=True,
            running_cpus=12,
            pending_cpus=10,
            unconfirmed_scheduler_jobs=2,
            unconfirmed_scheduler_cpus=7,
        )
        payload = format_overview_json(ov)
        assert payload["running_cpus"] == 12
        assert payload["pending_cpus"] == 10
        assert payload["unconfirmed_scheduler_jobs"] == 2
        assert payload["unconfirmed_scheduler_cpus"] == 7

        rebuilt = _overview_from_json("alpha", payload)
        assert rebuilt.unconfirmed_scheduler_jobs == 2
        assert rebuilt.unconfirmed_scheduler_cpus == 7

    def test_legacy_payload_has_no_invented_unconfirmed_reservations(self) -> None:
        rebuilt = _overview_from_json(
            "legacy",
            {"queue_counts": {"pending": 1}, "pending_cpus": 8},
        )

        assert rebuilt.unconfirmed_scheduler_jobs == 0
        assert rebuilt.unconfirmed_scheduler_cpus == 0

    def test_remote_unconfirmed_reservation_clears_stale_idle_claim(self) -> None:
        rebuilt = _overview_from_json(
            "remote",
            {
                "queue_counts": {"pending": 1},
                "pending_cpus": 8,
                "unconfirmed_scheduler_jobs": 1,
                "unconfirmed_scheduler_cpus": 8,
                "idle_seconds": 300,
            },
        )

        assert rebuilt.idle_seconds is None
        assert "idle:" not in format_overview_text(rebuilt)

        payload = format_overview_json(
            HostOverview(
                host="inconsistent",
                queue_counts={"pending": 1},
                pending_cpus=8,
                unconfirmed_scheduler_jobs=1,
                unconfirmed_scheduler_cpus=8,
                idle_seconds=300,
            )
        )
        assert payload["idle_seconds"] is None


# ----------------------------------------------------------------------
# scheduler-host overview synthesis
# ----------------------------------------------------------------------


def _scheduler_cfg() -> config.Config:
    return config.Config(
        hosts={
            "host_f": config.HostConfig(
                ssh="host_f",
                scheduler="pbs",
                scheduler_dialect="torque",
                scheduler_driver="driver",
                scratch_root="/scratch/USER",
            ),
            "driver": config.HostConfig(ssh="driver"),
        }
    )


class TestGatherSchedulerOverview:
    def test_unconfirmed_phases_are_not_confirmed_running(
        self, monkeypatch
    ) -> None:
        cfg = _scheduler_cfg()
        now = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)
        driver_overview = HostOverview(
            host="driver",
            daemon_health=_healthy_verdict(),
        )

        running = _spec("running", state=JobState.RUNNING, cpus=2)
        running.scheduler_target = "host_f"
        running.scheduler_state = "running"
        pending_phase_specs = []
        for index, phase in enumerate(("queued", "held", None), start=3):
            label = phase or "unpolled"
            spec = _spec(label, state=JobState.RUNNING, cpus=index)
            spec.scheduler_target = "host_f"
            spec.scheduler_state = phase
            pending_phase_specs.append(spec)

        specs = [running, *pending_phase_specs]
        for index, phase in enumerate(_UNCONFIRMED_SCHEDULER_PHASES, start=6):
            spec = _spec(phase, state=JobState.RUNNING, cpus=index)
            spec.scheduler_target = "host_f"
            spec.scheduler_state = phase
            specs.append(spec)

        monkeypatch.setattr("vq.overview.is_local_host", lambda h: h == "driver")
        monkeypatch.setattr(
            "vq.overview.gather_overview_local",
            lambda *a, **kw: driver_overview,
        )
        monkeypatch.setattr("vq.overview.list_jobs", lambda *a, **kw: specs)

        overview = gather_scheduler_overview(
            "host_f", cfg.host("host_f"), cfg, now=now
        )

        assert overview.queue_counts == {
            "running": 1,
            "pending": (
                len(pending_phase_specs)
                + len(_UNCONFIRMED_SCHEDULER_PHASES)
            ),
        }
        assert overview.scheduler_queue_counts == {
            "running": 1,
            "queued": 1,
            "held": 1,
            "unpolled": 1,
            **{phase: 1 for phase in _UNCONFIRMED_SCHEDULER_PHASES},
        }
        assert overview.running_cpus == 2
        assert overview.pending_cpus == sum(range(3, 6)) + sum(range(6, 12))
        assert overview.unconfirmed_scheduler_jobs == len(
            _UNCONFIRMED_SCHEDULER_PHASES
        )
        assert overview.unconfirmed_scheduler_cpus == sum(range(6, 12))

        payload = format_overview_json(overview)
        assert payload["unconfirmed_scheduler_jobs"] == len(
            _UNCONFIRMED_SCHEDULER_PHASES
        )
        assert payload["unconfirmed_scheduler_cpus"] == sum(range(6, 12))
        text = format_overview_text(overview)
        assert (
            "unconfirmed scheduler reservations: 6 job(s), 51 CPU(s) "
            "(included in pending; capacity reserved)"
        ) in text

    def test_unconfirmed_reservation_prevents_idle_inference(
        self, monkeypatch
    ) -> None:
        cfg = _scheduler_cfg()
        now = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)
        driver_overview = HostOverview(
            host="driver",
            daemon_health=_healthy_verdict(),
        )
        uncertain_specs = []
        for phase in _UNCONFIRMED_SCHEDULER_PHASES:
            spec = _spec(phase, state=JobState.RUNNING, cpus=8)
            spec.scheduler_target = "host_f"
            spec.scheduler_state = phase
            uncertain_specs.append(spec)
        completed = _spec(
            "completed",
            state=JobState.COMPLETED,
            finished_at=(now - timedelta(hours=1)).isoformat(),
        )
        completed.scheduler_target = "host_f"

        monkeypatch.setattr("vq.overview.is_local_host", lambda h: h == "driver")
        monkeypatch.setattr(
            "vq.overview.gather_overview_local",
            lambda *a, **kw: driver_overview,
        )
        monkeypatch.setattr(
            "vq.overview.list_jobs",
            lambda *a, **kw: [*uncertain_specs, completed],
        )

        overview = gather_scheduler_overview(
            "host_f", cfg.host("host_f"), cfg, now=now
        )

        assert overview.queue_counts.get("running", 0) == 0
        assert overview.queue_counts["pending"] == len(uncertain_specs)
        assert overview.unconfirmed_scheduler_jobs == len(uncertain_specs)
        assert overview.idle_seconds is None

    def test_counts_only_specs_for_that_scheduler_target(self, monkeypatch) -> None:
        cfg = _scheduler_cfg()
        driver_health = _healthy_verdict()
        driver_overview = HostOverview(
            host="driver",
            vq_version="1.2.3",
            daemon_health=driver_health,
        )
        host_f_pending = _spec("tw-pend", state=JobState.PENDING, cpus=16)
        host_f_pending.scheduler_target = "host_f"
        host_f_running = _spec("tw-run", state=JobState.RUNNING, cpus=32)
        host_f_running.scheduler_target = "host_f"
        host_f_running.scheduler_state = "running"
        host_f_pbs_queued = _spec("tw-pbsq", state=JobState.RUNNING, cpus=8)
        host_f_pbs_queued.scheduler_target = "host_f"
        host_f_pbs_queued.scheduler_state = "queued"
        host_f_poll_failed = _spec("tw-poll", state=JobState.RUNNING, cpus=4)
        host_f_poll_failed.scheduler_target = "host_f"
        host_f_poll_failed.scheduler_state = "poll_failed"
        other_running = _spec("other", state=JobState.RUNNING, cpus=64)
        other_running.scheduler_target = "other-cluster"

        monkeypatch.setattr("vq.overview.is_local_host", lambda h: h == "driver")
        monkeypatch.setattr(
            "vq.overview.gather_overview_local",
            lambda *a, **kw: driver_overview,
        )
        monkeypatch.setattr(
            "vq.overview.list_jobs",
            lambda *a, **kw: [
                host_f_pending,
                host_f_running,
                host_f_pbs_queued,
                host_f_poll_failed,
                other_running,
            ],
        )

        overview = gather_scheduler_overview("host_f", cfg.host("host_f"), cfg)

        assert overview.host == "host_f"
        # A scheduler host no longer borrows the driver's version ("1.2.3"
        # here). It is daemonless and runs its own helper vq, whose skew from
        # the driver was a whole incident; showing the driver's number hid it.
        assert overview.vq_version is None
        assert overview.daemon_health is driver_health
        assert overview.queue_counts == {"pending": 3, "running": 1}
        assert overview.scheduler_queue_counts == {
            "running": 1,
            "queued": 1,
            "poll_failed": 1,
        }
        assert overview.pending_cpus == 28
        assert overview.running_cpus == 32
        assert overview.unconfirmed_scheduler_jobs == 1
        assert overview.unconfirmed_scheduler_cpus == 4
        # Placement remains fail-closed: telemetry loss does not free the
        # poll_failed job's reserved CPUs. Overview exposes the unconfirmed
        # subset without adding its CPUs a second time.
        assert overview.running_cpus + overview.pending_cpus == 60
        text = format_overview_text(overview)
        assert "scheduler queue:" in text
        assert "queued                1" in text

    def test_remote_driver_queue_json_feeds_target_counts(self, monkeypatch) -> None:
        cfg = _scheduler_cfg()
        driver_overview = HostOverview(
            host="driver",
            vq_version="1.2.3",
            daemon_health=_healthy_verdict(),
        )
        host_f = _spec("tw", state=JobState.PENDING, cpus=8)
        host_f.scheduler_target = "host_f"
        uncertain = _spec("tw-uncertain", state=JobState.RUNNING, cpus=4)
        uncertain.scheduler_target = "host_f"
        uncertain.scheduler_state = "poll_failed"
        local = _spec("local", state=JobState.PENDING, cpus=99)
        payload = json.dumps(
            [
                host_f.model_dump(mode="json"),
                uncertain.model_dump(mode="json"),
                local.model_dump(mode="json"),
            ]
        )
        calls: list[tuple[str, tuple[str, ...]]] = []

        def fake_run_remote_vq(host_cfg, *argv):
            calls.append((host_cfg.ssh, argv))
            return SimpleNamespace(stdout=payload)

        monkeypatch.setattr("vq.overview.is_local_host", lambda h: False)
        monkeypatch.setattr(
            "vq.overview.gather_overview_remote",
            lambda *a, **kw: driver_overview,
        )
        monkeypatch.setattr("vq.overview.transport.run_remote_vq", fake_run_remote_vq)

        overview = gather_scheduler_overview("host_f", cfg.host("host_f"), cfg)

        assert calls == [("driver", ("queue", "localhost", "--json"))]
        assert overview.queue_counts == {"pending": 2}
        assert overview.pending_cpus == 12
        assert overview.unconfirmed_scheduler_jobs == 1
        assert overview.unconfirmed_scheduler_cpus == 4

    def test_unreachable_remote_driver_marks_scheduler_unreachable(self, monkeypatch) -> None:
        cfg = _scheduler_cfg()
        monkeypatch.setattr("vq.overview.is_local_host", lambda h: False)
        monkeypatch.setattr(
            "vq.overview.gather_overview_remote",
            lambda *a, **kw: HostOverview(
                host="driver",
                reachable=False,
                error="ssh failed",
            ),
        )

        overview = gather_scheduler_overview("host_f", cfg.host("host_f"), cfg)

        assert overview.reachable is False
        assert "driver" in (overview.error or "")
        assert "ssh failed" in (overview.error or "")
