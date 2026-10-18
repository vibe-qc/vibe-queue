"""#182: `rollout-latest` does not drain a local host for its serial step.

A local host's `vq admin update` holds that host's dispatch itself, for
exactly an in-place or serving-daemon transaction, and not while it builds an
immutable replacement beside the current generation (#167). The rollout used
to add an outer full drain over the host's whole step, so a host idled while
its replacement built, and one whose step had ended stayed drained while
later hosts updated. Now the rollout takes none, and a local hold an earlier
invocation journaled is released before any host runs, unless its host's
operation is unsettled.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from vq import fleet_release, fleet_rollout

sys.path.insert(0, str(Path(__file__).parent))
from test_fleet_rollout import DEV, _report  # noqa: E402

ROLLOUT = "v0.15.60-two-host-182"
SET_AT = "2026-10-14T21:01:11+00:00"


def _action(host: str, program: str, decision: str = "update") -> fleet_rollout.RolloutAction:
    return fleet_rollout.RolloutAction(
        id=f"local-runtime:{host}:{program}", phase="local-runtime", host=host,
        program=program, pin_name="dev", target_sha=DEV, target_version="0.15.61.dev0",
        target_tag=None, argv=["admin", "update", program, host], decision=decision,
        reason="target is newer" if decision == "update" else "already at target",
        before={},
    )


class Fleet:
    """Each host's daemon drain, as `vq drain ... HOST` sees and changes it."""

    def __init__(self, drained: set[str]) -> None:
        self.drained = set(drained)
        self.mutations: list[list[str]] = []
        self.seen_while_updating: list[tuple[str, frozenset[str]]] = []

    def control(self, argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        args = argv[3:]
        host = args[-1]
        if "--status" in args:
            active = host in self.drained
            payload = {"active": active, "is_full_drain": active, "scheduler_hosts": [],
                       "state": ({"reason": fleet_rollout._hold_reason(ROLLOUT, host),
                                  "set_at": SET_AT} if active else None)}
            return subprocess.CompletedProcess(argv, 0, stdout=json.dumps(payload), stderr="")
        self.mutations.append(args)
        if "--release-full" in args:
            self.drained.discard(host)
        else:
            self.drained.add(host)
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    def update(self, argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        # argv ends with PROGRAM HOST: record which hosts are drained right now.
        self.seen_while_updating.append((argv[-1], frozenset(self.drained)))
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")


def _plan(*actions: fleet_rollout.RolloutAction) -> fleet_rollout.RolloutPlan:
    return fleet_rollout.RolloutPlan(driver="localhost",
                                     report=fleet_release.report_summary(_report()),
                                     topology={}, actions=list(actions))


def _journal_hold(plan: fleet_rollout.RolloutPlan, host: str) -> None:
    """A full hold an earlier invocation of this rollout took and never released."""
    fleet_rollout.save_run(fleet_rollout.RolloutRun(
        rollout_id=ROLLOUT, report_digest_sha256=str(plan.report["digest_sha256"]),
        report_source_path=str(plan.report["source_path"]),
        holds={host: {"host": host, "kind": "full", "owned": True, "status": "active",
                      "reason": fleet_rollout._hold_reason(ROLLOUT, host),
                      "preexisting": False, "duration_seconds": 6 * 3600,
                      "control_host": host, "set_at": SET_AT}},
    ))


@pytest.fixture(autouse=True)
def _state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)


def test_neither_host_is_drained_while_either_updates() -> None:
    plan = _plan(_action("host_a", "vibeqc-queue"), _action("host_a", "vibeqc-dev"),
                 _action("host_b", "vibeqc-queue"), _action("host_b", "vibeqc-dev"))
    fleet = Fleet(drained=set())
    run = fleet_rollout.execute_plan(plan, rollout_id=ROLLOUT, runner=fleet.update,
                                     control_runner=fleet.control)
    assert [host for host, _ in fleet.seen_while_updating] == ["host_a"] * 2 + ["host_b"] * 2
    assert all(drained == frozenset() for _, drained in fleet.seen_while_updating)
    assert fleet.mutations == [] and run.holds == {}
    assert {record["status"] for record in run.actions.values()} == {"success"}


def test_a_finished_hosts_journaled_drain_is_released_before_the_next_host_updates() -> None:
    # host_a's step ended under an earlier invocation that left its drain;
    # host_b is still to update.
    plan = _plan(_action("host_a", "vibeqc-queue", "skip"), _action("host_a", "vibeqc-dev", "skip"),
                 _action("host_b", "vibeqc-queue"), _action("host_b", "vibeqc-dev"))
    _journal_hold(plan, "host_a")
    fleet = Fleet(drained={"host_a"})
    run = fleet_rollout.execute_plan(plan, rollout_id=ROLLOUT, runner=fleet.update,
                                     control_runner=fleet.control)
    # host_a dispatches again while host_b is still updating.
    assert fleet.seen_while_updating == [("host_b", frozenset())] * 2
    assert [args[0:2] for args in fleet.mutations] == [["drain", "--release-full"]]
    assert fleet.mutations[0][-1] == "host_a"
    assert run.holds["host_a"]["status"] == "released"


@pytest.mark.parametrize("unsettled", [False, True])
def test_a_journaled_drain_stays_while_its_hosts_operation_is_unsettled(
    monkeypatch: pytest.MonkeyPatch, unsettled: bool,
) -> None:
    # The durable path (no runner): an operation that may still own the
    # mutation keeps the exact hold, as it always did; a settled one does not.
    plan = _plan(_action("host_a", "vibeqc-queue", "skip"))
    _journal_hold(plan, "host_a")
    monkeypatch.setattr(fleet_rollout, "_host_has_unsettled_operation",
                        lambda run, actions: unsettled)
    fleet = Fleet(drained={"host_a"})
    run = fleet_rollout.execute_plan(plan, rollout_id=ROLLOUT, control_runner=fleet.control)
    assert ("host_a" in fleet.drained) is unsettled
    assert run.holds["host_a"]["status"] == ("active" if unsettled else "released")
