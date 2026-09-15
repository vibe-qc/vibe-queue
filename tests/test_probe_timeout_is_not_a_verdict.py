"""A probe that ran out of time is not a probe that answered "no".

On 2026-09-10 five of six supersede refusals on host_f read "lacks strictly
healthy exact-target evidence" while nothing was wrong. The
`scheduler_remote_vq` doctor check makes three remote vq calls inside one
10 s budget; host_f's login node needs 1.6-2.5 s each, so `source-sha`
intermittently ran out, the helper's live SHA went missing from that sweep,
and the lane read as not converged. One success in four attempts.

Two separate defects, and the second is the one that matters:

* the rollout sweep could not pass `--check-timeout` at all, so every fleet
  got the 10 s default forever;
* a timed-out probe was indistinguishable from a negative verdict, all the
  way up to a gate that refuses permanently on it.
"""
from __future__ import annotations

import subprocess

import pytest
from pydantic import ValidationError

from vq import admin, config, doctor, fleet_rollout


class TestATimeoutIsMarkedAsOne:
    """The doctor payload has to carry the distinction before anyone can use it."""

    def _timeout_error(self) -> Exception:
        cause = subprocess.TimeoutExpired(cmd=["ssh"], timeout=2.37)
        exc = RuntimeError("remote vq timed out after 2.37s on host_f-login")
        exc.__cause__ = cause
        return exc

    def test_a_transport_timeout_is_flagged(self) -> None:
        marked = doctor._timeout_metadata(self._timeout_error(), subprobe="source_sha")

        assert marked == {"timed_out": True, "subprobe": "source_sha"}

    def test_an_ordinary_failure_is_not(self) -> None:
        """A host that answered "no" must not be excused as a timeout: that
        would turn a real verdict into an endless retry."""
        marked = doctor._timeout_metadata(
            RuntimeError("permission denied"), subprobe="source_sha",
        )

        assert marked == {}

    def test_the_flag_adds_nothing_to_a_plain_failure(self) -> None:
        """Empty rather than `timed_out: False`, so an existing consumer of
        this payload sees exactly what it always saw."""
        assert "timed_out" not in doctor._timeout_metadata(
            OSError("boom"), subprobe="x",
        )


class TestTheLaneCarriesUnavailability:
    """`last_ok` has two values; "we did not find out" is not one of them."""

    RECORDED = "a" * 40

    def _lane(self, check: dict) -> fleet_rollout.LaneState:
        return fleet_rollout._helper_lane_state(
            {
                "host_f": {
                    "helper": {
                        "last_success": True,
                        "actual_sha": self.RECORDED,
                    }
                }
            },
            {"host_f": {"ok": False, "checks": [check]}},
            host="host_f",
            configured=True,
        )

    def test_a_timed_out_probe_marks_the_lane_unavailable(self) -> None:
        lane = self._lane({
            "name": "scheduler_remote_vq",
            "ok": False,
            "message": "SOURCE-SHA check failed: remote vq timed out after 2.37s",
            "timed_out": True,
        })

        assert lane.last_ok is False, "still not converged: the safe reading"
        assert lane.probe_unavailable is True
        assert "timed out" in lane.detail

    def test_a_probe_that_answered_is_not_unavailable(self) -> None:
        """The distinction has to cut both ways or it is worthless: a live
        answer, even a disagreeing one, is evidence and must not be retried
        forever as though it were missing."""
        lane = self._lane({
            "name": "scheduler_remote_vq",
            "ok": True,
            "message": "helper SOURCE-SHA b" + "b" * 39,
            "source_sha": "b" * 40,
        })

        assert lane.probe_unavailable is False
        assert lane.current_sha == "b" * 40

    def test_unavailability_reaches_the_action_payload(self) -> None:
        """`_before` is `asdict`, so the gate can see it. Pinned because a
        gate reading a field the lane forgot to carry is the whole bug."""
        lane = fleet_rollout.LaneState(
            configured=True,
            current_sha=None,
            current_version=None,
            current_tag=None,
            dirty=False,
            last_ok=False,
            acknowledged=False,
            detail="live helper provenance probe timed out",
            probe_unavailable=True,
        )

        assert fleet_rollout._before(lane)["probe_unavailable"] is True


class TestTheGateRetriesInsteadOfRefusing:
    def test_the_error_classifies_as_retryable(self) -> None:
        exc = fleet_rollout.FleetProbeUnavailable("probe did not answer")

        assert exc.outcome == "locked"
        assert exc.outcome in admin.ADMIN_OUTCOMES
        assert admin.ADMIN_OUTCOME_EXIT_CODES[exc.outcome] == 75

    def test_it_stays_catchable_as_a_rollout_error(self) -> None:
        """Every existing `except FleetRolloutError` must keep working."""
        assert issubclass(
            fleet_rollout.FleetProbeUnavailable, fleet_rollout.FleetRolloutError
        )

    def test_it_is_not_the_permanent_refusal(self) -> None:
        """`precondition-failed` means stop; this one must never map there,
        because a bounded retry is the correct response to it."""
        assert (
            fleet_rollout.FleetProbeUnavailable.outcome
            != admin.OUTCOME_PRECONDITION_FAILED
        )


class TestTheSweepCanCarryABudget:
    def test_it_passes_the_configured_budget(self) -> None:
        seen: list[list[str]] = []

        def runner(argv, **kwargs):  # type: ignore[no-untyped-def]
            seen.append(list(argv))
            return subprocess.CompletedProcess(argv, 0, "{}", "")

        fleet_rollout.collect_snapshots(
            runner=runner, check_timeout_seconds=45.0,
        )

        doctor_argv = next(a for a in seen if "doctor" in a)
        assert "--check-timeout" in doctor_argv
        assert doctor_argv[doctor_argv.index("--check-timeout") + 1] == "45"

    def test_an_unconfigured_sweep_is_unchanged(self) -> None:
        """A fleet that never hit this must see the exact argv it always did."""
        seen: list[list[str]] = []

        def runner(argv, **kwargs):  # type: ignore[no-untyped-def]
            seen.append(list(argv))
            return subprocess.CompletedProcess(argv, 0, "{}", "")

        fleet_rollout.collect_snapshots(runner=runner)

        doctor_argv = next(a for a in seen if "doctor" in a)
        assert doctor_argv[-3:] == ["doctor", "--all", "--json"]
        assert "--check-timeout" not in doctor_argv

    def test_the_budget_must_be_positive(self) -> None:
        with pytest.raises(ValidationError):
            config.Config.model_validate({"fleet": {"check_timeout_seconds": 0}})

    def test_it_is_absent_by_default(self) -> None:
        assert config.Config().fleet.check_timeout_seconds is None
