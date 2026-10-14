"""#168: execution readiness on the Fleet view.

The focused acceptance from the issue, on a bounded synthetic snapshot:
a replacement build or a failed update leaves the current runtime
eligible; a genuine admission failure names its program, target and
reason; an unreachable target is unknown and makes the totals partial;
a driver and its scheduler lane count one execution once.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from vq import admin, auth, config, paths
from vq.overview import AdminMarkerSnapshot, HostOverview
from vq.spec import JobSpec, JobState
from vq.web import create_app, readiness, view_context
from vq.web import fleet as fleet_mod

NOW = datetime(2026, 10, 14, 12, 0, 0, tzinfo=UTC)
OK = SimpleNamespace(ok=True, findings=[])
DEGRADED = SimpleNamespace(ok=False, findings=["FAIL vq-daemon.service is inactive"])


def _env(name: str, version: str | None = "1.2.0", error: str | None = None) -> admin.EnvStatus:
    return admin.EnvStatus(
        name=name, git_dir="/synthetic", branch="main", current_sha=None,
        current_describe=None, is_dirty=False, last_record=None, error=error,
        current_version=version,
    )


def _lease(
    envs: list[str], *, host: str, status: str, managed: bool = False,
    stale_reason: str | None = None,
) -> AdminMarkerSnapshot:
    marker = admin.AdminUpdateMarker(
        envs=envs, host=host, started_at=NOW.isoformat(), pid=4242, vq_version="0.26.35",
        managed_transaction={"kind": "synthetic"} if managed else None,
    )
    return AdminMarkerSnapshot(
        marker=marker, status=status, stale_reason=stale_reason,
        summary=f"synthetic {status} lease for {', '.join(envs)}",
        action="Run `vq admin recover-update`." if status != "running" else "Wait.",
    )


def _daemon(
    key: str, *, envs: list[admin.EnvStatus] | None = None,
    leases: list[AdminMarkerSnapshot] | None = None, health: object = OK,
    reported: str | None = None, jobs: list[dict] | None = None, **fields: object,
) -> fleet_mod.HostSnapshot:
    ov = HostOverview(
        host=key, vq_version="0.26.35", reported_hostname=reported or key,
        daemon_health=health,  # type: ignore[arg-type]
        envs=envs or [], admin_markers=leases or [],
    )
    for name, value in fields.items():
        setattr(ov, name, value)
    return fleet_mod.HostSnapshot(host=key, overview=ov, jobs=jobs or [])


def _lane(
    key: str, driver: fleet_mod.HostSnapshot, *, cluster: str | None = None,
    jobs: list[dict] | None = None,
) -> fleet_mod.HostSnapshot:
    ov = HostOverview(host=key, is_scheduler_host=True,
                      daemon_health=driver.overview.daemon_health)
    ov.reachable = driver.overview.reachable
    ov.drain_state = driver.overview.drain_state
    return fleet_mod.HostSnapshot(
        host=key, overview=ov, jobs=jobs or [], driver=driver.host,
        driver_leases=list(driver.overview.admin_markers), cluster=cluster or key,
    )


def _snapshot(*hosts: fleet_mod.HostSnapshot) -> fleet_mod.FleetSnapshot:
    return fleet_mod.FleetSnapshot(
        gathered_at=NOW.isoformat(), duration_seconds=1.0, hosts=list(hosts),
    )


def _row(jobid: str, target: str, owner: str | None = None, **fields: object) -> dict:
    row: dict[str, object] = {
        "id": jobid, "queue_host": target, "queue_owner": owner or target,
        "state": "running", "effective_state": "running", "scheduler_target": None,
        "cpus": 4, "program": "vibeqc", "exit_code": None, "failure_reason": None,
        "submitted_at": "2026-10-14T10:00:00+00:00",
        "started_at": "2026-10-14T10:05:00+00:00",
    }
    row.update(fields)
    return row


def _program(result: dict, name: str) -> dict:
    return next(p for p in result["programs"] if p["program"] == name)


def _targets(places: list[dict]) -> list[str]:
    return [place["target"] for place in places]


class TestReplacementBuilds:
    def test_a_building_replacement_leaves_the_current_runtime_eligible(self) -> None:
        # Healthy A with B building: on a daemon host (a runtime slot build)
        # and on a scheduler lane (a scheduler-runtime build on its driver).
        host_a = _daemon("host_a", envs=[_env("vibeqc", "1.2.0")],
                         leases=[_lease(["runtime-slot:vibeqc"], host="host_a", status="running")])
        drv = _daemon("drv", leases=[_lease(["scheduler-runtime:pbs_a:vibeqc"],
                                            host="pbs_a", status="running")])
        result = readiness.fleet_readiness(_snapshot(host_a, drv, _lane("pbs_a", drv)))
        assert result is not None

        assert result["dispatch"] == {"open": 3, "held": 0, "unknown": 0}
        vibeqc = _program(result, "vibeqc")
        assert vibeqc["eligible"] == [
            {"target": "host_a", "runtime": "1.2.0", "reason": None},
            {"target": "pbs_a", "runtime": None, "reason": None},
        ]
        assert [(u["target"], u["state"], u["holds_dispatch"]) for u in vibeqc["updates"]] == [
            ("host_a", "building", False), ("pbs_a", "building", False),
        ]
        assert vibeqc["held"] == vibeqc["refused"] == vibeqc["unknown"] == []

    def test_a_failed_replacement_leaves_the_current_runtime_eligible(self) -> None:
        # Failed B with healthy A: the update needs action, A still runs jobs.
        host_a = _daemon("host_a", envs=[_env("vibeqc", "1.2.0")], leases=[_lease(
            ["runtime-slot:vibeqc"], host="host_a", status="failed",
            stale_reason="build step exited 2",
        )])
        result = readiness.fleet_readiness(_snapshot(host_a))
        assert result is not None

        vibeqc = _program(result, "vibeqc")
        assert _targets(vibeqc["eligible"]) == ["host_a"]
        (update,) = vibeqc["updates"]
        assert (update["state"], update["holds_dispatch"]) == ("update failed", False)
        assert update["action"] == "Run `vq admin recover-update`."
        assert result["by_target"]["host_a"]["dispatch"] == "open"

    def test_an_update_that_holds_dispatch_is_held_not_failed(self) -> None:
        # The contrast: a managed venv update on the host itself holds local
        # dispatch until it is recovered, so its program is held there, while
        # the same host's other program is held too (the hold is per target).
        lease = _lease(["vibeqc"], host="host_a", status="stale", managed=True,
                       stale_reason="updater pid is gone")
        host_a = _daemon("host_a", envs=[_env("vibeqc"), _env("orca", "6.0")], leases=[lease])
        result = readiness.fleet_readiness(_snapshot(host_a))
        assert result is not None

        assert result["by_target"]["host_a"]["dispatch"] == "held"
        vibeqc = _program(result, "vibeqc")
        assert _targets(vibeqc["held"]) == ["host_a"]
        assert "admin update holds dispatch (vibeqc)" in vibeqc["held"][0]["reason"]
        assert vibeqc["updates"][0]["state"] == "needs recovery"
        assert _targets(_program(result, "orca")["held"]) == ["host_a"]

    def test_a_drivers_local_update_does_not_hold_its_lanes(self) -> None:
        drv = _daemon("drv", envs=[_env("vibeqc")],
                      leases=[_lease(["vibeqc"], host="drv", status="running")])
        result = readiness.fleet_readiness(_snapshot(drv, _lane("pbs_a", drv)))
        assert result is not None
        assert result["by_target"]["drv"]["dispatch"] == "held"
        assert result["by_target"]["pbs_a"]["dispatch"] == "open"

    def test_mixed_vq_versions_alone_change_nothing(self) -> None:
        old = _daemon("host_a", envs=[_env("vibeqc")])
        new = _daemon("host_b", envs=[_env("vibeqc")])
        old.overview.vq_version = "0.26.20"
        result = readiness.fleet_readiness(_snapshot(old, new))
        assert result is not None
        assert result["dispatch"] == {"open": 2, "held": 0, "unknown": 0}
        assert _targets(_program(result, "vibeqc")["eligible"]) == ["host_a", "host_b"]


class TestAdmissionFailures:
    def test_a_dispatch_refusal_names_program_target_and_reason(self) -> None:
        reason = "program runtime pin for orca is not an immutable runtime"
        rows = [
            _row("refused01", "host_b", program="orca", state="failed", started_at=None,
                 exit_code=-1, failure_reason=reason,
                 finished_at="2026-10-14T11:00:00+00:00"),
            # Neither of these is the queue refusing a job.
            _row("cascade01", "host_b", program="orca", state="failed", started_at=None,
                 failure_reason="predecessor abc failed (state=failed)",
                 finished_at="2026-10-14T11:01:00+00:00"),
            _row("crashed01", "host_b", program="cp2k", state="failed", exit_code=1,
                 failure_reason="exit code 1", finished_at="2026-10-14T11:02:00+00:00"),
        ]
        host_b = _daemon("host_b", envs=[_env("orca", "6.0")], jobs=rows)
        result = readiness.fleet_readiness(_snapshot(host_b))
        assert result is not None

        assert result["admission_failures"] == [{
            "target": "host_b", "program": "orca", "job": "refused01",
            "kind": "dispatch refused", "reason": reason,
            "at": "2026-10-14T11:00:00+00:00",
        }]
        orca = _program(result, "orca")
        assert orca["eligible"] == []
        assert orca["refused"] == [{"target": "host_b", "runtime": "6.0", "reason": reason}]
        # The target itself is open: one program was refused, not the host.
        assert result["by_target"]["host_b"]["dispatch"] == "open"

    def test_a_later_admitted_job_retires_the_refusal(self) -> None:
        rows = [
            _row("refused01", "host_b", program="orca", state="failed", started_at=None,
                 exit_code=-1, failure_reason="runtime missing",
                 finished_at="2026-10-14T09:00:00+00:00"),
            _row("ran01", "host_b", program="orca", started_at="2026-10-14T10:00:00+00:00"),
        ]
        result = readiness.fleet_readiness(_snapshot(
            _daemon("host_b", envs=[_env("orca", "6.0")], jobs=rows),
        ))
        assert result is not None
        assert result["admission_failures"] == []
        assert _targets(_program(result, "orca")["eligible"]) == ["host_b"]

    def test_over_capacity_and_scheduler_rejection_are_admission_failures(self) -> None:
        drv = _daemon("drv")
        rows = [
            _row("toobig01", "drv", program="vibeqc", state="pending",
                 effective_state="pending", started_at=None, pending_over_capacity=True,
                 configured_capacity_overages=[
                     {"resource": "cpus", "requested": 128, "limit": 64, "uses_default": False},
                 ]),
            _row("rejected01", "pbs_a", "drv", program="cp2k", state="failed",
                 scheduler_target="pbs_a", exit_code=1,
                 failure_reason="qsub: Job exceeds queue resource limits",
                 terminal_diagnosis={"category": "scheduler_submit_failed",
                                     "summary": "The scheduler rejected the job."},
                 finished_at="2026-10-14T11:30:00+00:00"),
        ]
        drv.jobs = rows[:1]
        lane = _lane("pbs_a", drv, jobs=rows[1:])
        result = readiness.fleet_readiness(_snapshot(drv, lane))
        assert result is not None
        failures = {(f["target"], f["program"]): f for f in result["admission_failures"]}
        assert failures["drv", "vibeqc"]["kind"] == "over capacity"
        assert "cpus 128 > limit 64" in failures["drv", "vibeqc"]["reason"]
        assert failures["pbs_a", "cp2k"]["kind"] == "scheduler rejected"
        assert "exceeds queue resource limits" in failures["pbs_a", "cp2k"]["reason"]


class TestUnknownAndPartial:
    def test_an_unreachable_target_is_unknown_and_partial_never_idle(self) -> None:
        gone = fleet_mod.HostSnapshot(
            host="gone", overview=HostOverview(host="gone", reachable=False,
                                               error="RemoteError: ssh: connect timed out"),
            jobs_error="RemoteError: ssh: connect timed out",
        )
        result = readiness.fleet_readiness(_snapshot(_daemon("host_a"), gone))
        assert result is not None

        target = result["by_target"]["gone"]
        assert (target["daemon"], target["dispatch"]) == ("unknown", "unknown")
        assert result["dispatch"] == {"open": 1, "held": 0, "unknown": 1}
        assert result["daemons"] == {"ok": 1, "degraded": 0, "unknown": 1}
        assert result["partial"] == [
            {"target": "gone", "reason": "not observed: RemoteError: ssh: connect timed out"},
        ]
        # It may be a second key for a machine that did answer.
        assert result["resources"]["unidentified"] == 1

    def test_unestablished_daemon_health_is_unknown_not_open(self) -> None:
        result = readiness.fleet_readiness(_snapshot(
            _daemon("degraded", envs=[_env("vibeqc")], health=DEGRADED),
            _daemon("silent", envs=[_env("vibeqc")], health=None),
        ))
        assert result is not None
        assert result["dispatch"] == {"open": 0, "held": 0, "unknown": 2}
        assert result["by_target"]["degraded"]["reasons"] == [
            "daemon degraded: FAIL vq-daemon.service is inactive",
        ]
        assert result["by_target"]["silent"]["reasons"] == ["daemon health not reported"]
        assert _targets(_program(result, "vibeqc")["unknown"]) == ["degraded", "silent"]

    def test_an_operator_stop_is_held(self) -> None:
        down = fleet_mod.HostSnapshot(
            host="down", overview=HostOverview(host="down", reachable=False,
                                               admin_down="down since 2026-10-14: maintenance"),
        )
        result = readiness.fleet_readiness(_snapshot(down))
        assert result is not None
        assert result["by_target"]["down"]["dispatch"] == "held"
        assert result["partial"] == [
            {"target": "down", "reason": "administratively down, not probed"},
        ]


class TestCountingOnce:
    def test_driver_and_lanes_count_one_execution_and_one_daemon(self) -> None:
        drv = _daemon("drv", health=DEGRADED)
        job = _row("pbs00001", "pbs_a", "drv", scheduler_target="pbs_a", cpus=16)
        pbs_a = _lane("pbs_a", drv, cluster="pbs_a", jobs=[job])
        pbs_gpu = _lane("pbs_gpu", drv, cluster="pbs_a")
        result = readiness.fleet_readiness(_snapshot(drv, pbs_a, pbs_gpu))
        assert result is not None

        assert result["executions"]["counts"]["running"] == 1
        assert result["executions"]["allocated_cpus"] == 16
        assert result["targets"] == {"configured": 3, "daemon_hosts": 1, "scheduler_lanes": 2}
        # Two lanes logging in to one cluster are one resource; the driver
        # machine is another. Its degraded daemon is counted once, not three times.
        assert result["resources"] == {
            "known": 2, "machines": 1, "clusters": 1, "unidentified": 0,
        }
        assert result["daemons"] == {"ok": 0, "degraded": 1, "unknown": 0}

    def test_a_duplicate_enrolment_folds_into_one_machine(self) -> None:
        job = _row("dup00001", "coordinator", cpus=8)
        a = _daemon("coordinator", reported="ws1", jobs=[job])
        b = _daemon("localhost", reported="ws1",
                    jobs=[dict(job, queue_host="localhost", queue_owner="localhost")])
        result = readiness.fleet_readiness(_snapshot(a, b))
        assert result is not None
        assert result["executions"]["counts"]["running"] == 1
        assert result["executions"]["allocated_cpus"] == 8
        assert result["executions"]["folded_rows"] == 1
        assert result["resources"]["machines"] == 1
        assert result["daemons"] == {"ok": 1, "degraded": 0, "unknown": 0}

    def test_queued_reservations_are_not_running(self) -> None:
        drv = _daemon("drv")
        queued = _row("pbsq0001", "pbs_a", "drv", scheduler_target="pbs_a",
                      scheduler_state="queued", effective_state="queued", cpus=32)
        result = readiness.fleet_readiness(_snapshot(drv, _lane("pbs_a", drv, jobs=[queued])))
        assert result is not None
        assert result["executions"]["counts"] == {
            "running": 0, "queued": 1, "held": 0, "unconfirmed": 0,
        }
        assert result["executions"]["allocated_cpus"] == 0


def _spec(root: Path, jobid: str, **fields: object) -> JobSpec:
    workspace = root / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    base: dict[str, object] = {
        "id": jobid, "command": ["true"], "cwd": str(workspace), "cpus": 4,
        "state": JobState.RUNNING, "started_at": "2026-10-14T10:00:00+00:00",
    }
    base.update(fields)
    return JobSpec(**base)


def test_a_sweep_keeps_a_lanes_background_build_visible(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End to end through gather_fleet_snapshot: the lane's projected
    overview drops a build lease that does not hold it, the readiness still
    sees it on the driver, two lanes on one login are one cluster, and no
    probe is added for readiness."""
    from vq import overview

    sched = _spec(tmp_path, "pbsjob0001", scheduler_target="pbs_a",
                  scheduler_state="running", cpus=16)
    build = _lease(["scheduler-runtime:pbs_a:vibeqc"], host="pbs_a", status="running")
    reads: list[tuple[str, tuple[str, ...]]] = []

    def remote_vq(host_cfg, *args, **kwargs):
        reads.append((host_cfg.ssh, args))
        assert args == ("queue", "localhost", "--json")
        return SimpleNamespace(stdout=json.dumps([fleet_mod._job_row(sched, "localhost")]))

    monkeypatch.setattr(fleet_mod.transport, "run_remote_vq", remote_vq)
    monkeypatch.setattr(overview, "gather_overview_remote", lambda host, *a, **k: HostOverview(
        host=host, daemon_health=OK, admin_markers=[build],  # type: ignore[arg-type]
    ))
    cfg = config.Config(hosts={
        "driver": config.HostConfig(ssh="driver.example.invalid"),
        **{
            lane: config.HostConfig(
                ssh="login.example.invalid", scheduler="pbs", scheduler_dialect="torque",
                scheduler_driver="driver",
                scratch_root="/tmp/scratch",
            )
            for lane in ("pbs_a", "pbs_gpu")
        },
    })

    snapshot = fleet_mod.gather_fleet_snapshot(cfg)

    assert reads == [("driver.example.invalid", ("queue", "localhost", "--json"))]
    lane = next(h for h in snapshot.hosts if h.host == "pbs_a")
    assert lane.overview.admin_markers == []
    assert (lane.driver, lane.cluster) == ("driver", "pbs_a")
    assert next(h for h in snapshot.hosts if h.host == "pbs_gpu").cluster == "pbs_a"
    # The ssh destination is grouped on, never carried.
    assert "login.example.invalid" not in repr(snapshot.hosts)

    result = readiness.fleet_readiness(snapshot)
    assert result is not None
    vibeqc = _program(result, "vibeqc")
    assert _targets(vibeqc["eligible"]) == ["pbs_a"]
    assert [(u["target"], u["state"]) for u in vibeqc["updates"]] == [("pbs_a", "building")]
    assert result["resources"]["clusters"] == 1
    assert result["executions"]["counts"]["running"] == 1


@pytest.fixture
def fleet_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
    monkeypatch.setenv("VQ_WEB_FLEET", "1")
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    return TestClient(create_app())


def test_fleet_grid_renders_readiness(fleet_client: TestClient) -> None:
    host_a = _daemon("host_a", envs=[_env("vibeqc", "1.2.0")], leases=[_lease(
        ["runtime-slot:vibeqc"], host="host_a", status="failed", stale_reason="build failed",
    )], jobs=[_row("refused01", "host_a", program="orca", state="failed", started_at=None,
                   exit_code=-1, failure_reason="orca runtime is not installed",
                   finished_at="2026-10-14T11:00:00+00:00")])
    drv = _daemon("drv")
    gone = fleet_mod.HostSnapshot(
        host="gone", overview=HostOverview(host="gone", reachable=False, error="ssh: timeout"),
    )
    snapshot = _snapshot(host_a, drv, _lane("pbs_a", drv), gone)
    snapshot.gathered_at = datetime.now(UTC).isoformat()
    fleet_client.app.state.fleet_cache.set_snapshot(snapshot)

    html = fleet_client.get("/fleet/_grid").text
    assert "Execution readiness" in html
    assert "configured targets" in html and "execution resources" in html
    assert "Partial: 1 target not observed." in html and "gone: not observed: ssh: timeout" in html
    assert "host_a (1.2.0)" in html
    assert "<strong>update failed</strong> (current runtime unaffected)" in html
    assert "refused on host_a: orca runtime is not installed" in html
    assert 'href="/fleet/jobs/refused01?host=host_a"' in html
    assert "driver daemon ok" in html and "dispatch open" in html
    # The lane's health is its driver's: one daemon, not two.
    context = view_context.fleet_grid_context(snapshot)
    assert context["readiness"]["daemons"] == {"ok": 2, "degraded": 0, "unknown": 1}
