"""A finished rollout can abandon a host it deferred and never started (#96)."""
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

from tests.test_fleet_rollout import _plan_bound_hold, _supported_drain_snapshot
from vq import fleet_rollout

ROLLOUT = "v0.17.8-6d94d574ebc7"


@pytest.fixture
def journal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    hold = _plan_bound_hold(ROLLOUT, host="host_f")
    fleet_rollout.save_run(fleet_rollout.RolloutRun(
        rollout_id=ROLLOUT,
        report_digest_sha256="d" * 64,
        report_source_path="releases/v0.17.8.json",
        actions={
            "helper:host_f": {"status": "not-run"},
            "scheduler-runtime:host_f:vibeqc-release": {"status": "not-run"},
            "helper:host_c": {"status": "success"},
        },
        holds={"host_f": hold},
    ))
    return hold


class Control:
    def __init__(self, snapshot: dict[str, Any]) -> None:
        self.snapshot = snapshot
        self.calls: list[list[str]] = []

    def __call__(self, argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        del kwargs
        self.calls.append(list(argv))
        if "--read-only-snapshot" in argv:
            return subprocess.CompletedProcess(argv, 0, stdout=json.dumps(self.snapshot),
                                               stderr="")
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")


def _own_lease(hold: dict[str, Any]) -> dict[str, Any]:
    return _supported_drain_snapshot(host="host_f", reason=hold["reason"],
                                     lease_owner=hold["lease_owner"])


@pytest.mark.parametrize("still_leased", [True, False])
def test_a_never_started_host_is_abandoned_and_its_hold_released(journal, still_leased) -> None:
    control = Control(_own_lease(journal) if still_leased
                      else _supported_drain_snapshot(host="host_f"))

    result = fleet_rollout.abandon_deferred_host(
        rollout_id_value=ROLLOUT, host="host_f", control_runner=control)

    assert result.actions == ("helper:host_f", "scheduler-runtime:host_f:vibeqc-release")
    run = fleet_rollout.load_run(ROLLOUT)
    assert run is not None
    hold = run.holds["host_f"]
    assert hold["status"] == "released" and hold["abandoned_at"]
    assert all(run.actions[a]["abandoned_at"] for a in result.actions)
    assert run.actions["helper:host_c"] == {"status": "success"}
    assert not any(
        h.get("owned") is True and h.get("status") in {"active", "cleanup-failed"}
        for r in fleet_rollout._all_rollout_runs() for h in r.holds.values()
    )
    assert any("--release" in call for call in control.calls)

    replay = fleet_rollout.abandon_deferred_host(
        rollout_id_value=ROLLOUT, host="host_f",
        control_runner=lambda *a, **k: pytest.fail("a replay must not touch the host"))
    assert replay.replayed is True


def test_a_started_child_cannot_be_abandoned(journal) -> None:
    run = fleet_rollout.load_run(ROLLOUT)
    assert run is not None
    run.actions["helper:host_f"] = {"status": "failed"}
    fleet_rollout.save_run(run)

    with pytest.raises(fleet_rollout.FleetRolloutError, match="already started"):
        fleet_rollout.abandon_deferred_host(
            rollout_id_value=ROLLOUT, host="host_f", control_runner=Control({}))


@pytest.mark.parametrize("claim", ["other-owner", "legacy"])
def test_another_claim_on_the_host_refuses(journal, claim) -> None:
    snapshot = (
        _supported_drain_snapshot(host="host_f", lease_owner="fleet-rollout:someone-else")
        if claim == "other-owner"
        else _supported_drain_snapshot(host="host_f", reason="old", legacy=True)
    )
    control = Control(snapshot)

    with pytest.raises(fleet_rollout.FleetRolloutError, match="another owner|legacy"):
        fleet_rollout.abandon_deferred_host(
            rollout_id_value=ROLLOUT, host="host_f", control_runner=control)
    run = fleet_rollout.load_run(ROLLOUT)
    assert run is not None and run.holds["host_f"]["status"] == "active"
    assert not any("--release" in call for call in control.calls)


def test_cli_requires_resume_and_reports(journal, monkeypatch) -> None:
    from click.testing import CliRunner

    from vq.cli import main

    control = Control(_own_lease(journal))
    real = fleet_rollout.abandon_deferred_host
    monkeypatch.setattr(fleet_rollout, "abandon_deferred_host",
                        lambda **kw: real(**kw, control_runner=control))

    refused = CliRunner().invoke(main, ["admin", "rollout-latest", "--abandon-host", "host_f"])
    assert refused.exit_code == 2
    done = CliRunner().invoke(main, ["admin", "rollout-latest", "--resume", ROLLOUT,
                                     "--abandon-host", "host_f", "--json"])
    assert done.exit_code == 0, done.output
    assert json.loads(done.output)["status"] == "abandoned"


# --- #99: a finished rollout releases the holds of hosts it never started ---

def _finish(control) -> fleet_rollout.RolloutRun:
    run = fleet_rollout.load_run(ROLLOUT)
    assert run is not None
    return fleet_rollout.finalize_run(run, complete=True, control_runner=control)


def test_99_finalizing_releases_a_deferred_hosts_own_hold(journal) -> None:
    control = Control(_own_lease(journal))

    run = _finish(control)

    hold = run.holds["host_f"]
    assert hold["status"] == "released" and hold["abandoned_at"]
    assert any("--release" in call for call in control.calls)
    assert not any(
        h.get("owned") is True and h.get("status") in {"active", "cleanup-failed"}
        for h in run.holds.values()
    )


def test_99_a_foreign_claim_keeps_the_hold_and_says_why(journal) -> None:
    control = Control(_supported_drain_snapshot(
        host="host_f", lease_owner="operator"))

    run = _finish(control)

    hold = run.holds["host_f"]
    assert hold["status"] == "active"
    assert "another owner" in hold["auto_release_refused"]
    assert run.complete is False
    assert not any("--release" in call for call in control.calls)


def test_99_a_host_with_an_attempted_action_is_left_alone(journal) -> None:
    run = fleet_rollout.load_run(ROLLOUT)
    assert run is not None
    run.actions["helper:host_f"] = {"status": "failed"}
    fleet_rollout.save_run(run)
    control = Control(_own_lease(journal))

    finished = _finish(control)

    assert finished.holds["host_f"]["status"] == "active"
    assert "auto_release_refused" not in finished.holds["host_f"]
    assert control.calls == []
