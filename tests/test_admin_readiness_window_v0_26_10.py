"""#67: the post-restart readiness window must fit a busy driver.

``vq admin update`` on the fleet driver rolled back a valid install. The
driver was supervising 175 live scheduler jobs and held 18,013 queue
records; its daemon needed 9 min 51 s to publish its RPC socket -- about
5 min 20 s reattaching scheduler jobs one at a time, one connection to the
scheduler host each, and about 4 min 30 s walking ``queue/*.json``, all of
it before RPC answers.

Two things were wrong with the window that was supposed to cover that:

* it counted queued specs and nothing else, so the reattachment phase --
  the more expensive of the two on that host -- was invisible to it; and
* its ceiling was below the driver's real startup time, so no allowance
  formula could reach it and only ``VQ_DAEMON_HEALTH_TIMEOUT`` could.

Both are pinned here, together with the rule that replaces "the window
expired, therefore the restart failed": a daemon process that exists and
has never answered RPC is a daemon that is still starting, and one that is
not there at all fails immediately instead of being waited out.

Every wait in this module runs on a fake clock, so the module's runtime is
independent of the windows it asserts.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from vq import admin

EXPECTED_SHA = "a" * 40
STALE_SHA = "b" * 40

# The #67 driver, as measured. Kept as data rather than as files on disk:
# the assertions below are about the shipped allowance, not about this
# machine's ability to create 21k files.
DRIVER_SPECS = 21_600
DRIVER_SCHEDULER_JOBS = 175
DRIVER_COLD_START_SECONDS = 591.0  # 9 min 51 s, from the daemon log


def _write_spec(
    queue: Path,
    name: str,
    *,
    state: str,
    scheduler_target: str | None = None,
) -> None:
    queue.mkdir(parents=True, exist_ok=True)
    record: dict[str, object] = {"id": name, "state": state}
    if scheduler_target is not None:
        record["scheduler_target"] = scheduler_target
    (queue / f"{name}.json").write_text(json.dumps(record))


@pytest.fixture
def queue(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """An isolated single-user state dir with no operator override set."""
    from vq import paths

    state = tmp_path / "state"
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(state))
    monkeypatch.delenv("VQ_DAEMON_HEALTH_TIMEOUT", raising=False)
    return state / "queue"


class TestLiveSchedulerJobsEnterTheAllowance:
    """Gap 1: reattachment cost was not modelled at all."""

    def test_live_scheduler_jobs_add_their_own_allowance(
        self, queue: Path,
    ) -> None:
        for i in range(3):
            _write_spec(
                queue, f"sched{i:03d}", state="running",
                scheduler_target="cluster-a",
            )
        for i in range(5):
            _write_spec(queue, f"local{i:03d}", state="running")

        expected = (
            admin.DAEMON_HEALTH_TIMEOUT_SECONDS
            + 8 * admin.DAEMON_HEALTH_SECONDS_PER_SPEC
            + 3 * admin.DAEMON_HEALTH_SECONDS_PER_SCHEDULER_JOB
        )
        assert admin._daemon_health_timeout() == pytest.approx(expected)

    def test_only_the_states_that_cost_a_round_trip_are_counted(
        self, queue: Path,
    ) -> None:
        """Mirrors the daemon's startup pass.

        RUNNING and SUSPENDED specs are reattached and an unconfirmed
        submit is reconciled against the remote, so each costs a round
        trip. A PENDING scheduler job has no remote allocation to find and
        a terminal one is not visited at all -- counting either would
        inflate the window on a host that is merely backlogged.
        """
        _write_spec(
            queue, "aaaa", state="running", scheduler_target="cluster-a",
        )
        _write_spec(
            queue, "bbbb", state="suspended", scheduler_target="cluster-a",
        )
        _write_spec(
            queue, "cccc", state="submitting", scheduler_target="cluster-a",
        )
        _write_spec(
            queue,
            "dddd",
            state="submit_outcome_unknown",
            scheduler_target="cluster-a",
        )
        _write_spec(
            queue, "eeee", state="pending", scheduler_target="cluster-a",
        )
        _write_spec(
            queue, "ffff", state="completed", scheduler_target="cluster-a",
        )
        _write_spec(
            queue, "gggg", state="failed", scheduler_target="cluster-a",
        )

        assert admin._count_live_scheduler_jobs(7).allowance_jobs == 4

    def test_a_corrupt_or_unreadable_spec_does_not_break_the_count(
        self, queue: Path,
    ) -> None:
        """Sizing a timeout must never raise into the update lane."""
        _write_spec(
            queue, "good", state="running", scheduler_target="cluster-a",
        )
        queue.joinpath("truncated.json").write_text("{not json")
        queue.joinpath("list.json").write_text("[]")
        queue.joinpath("empty.json").write_text("")

        assert admin._count_live_scheduler_jobs(4).allowance_jobs == 1
        assert admin._daemon_health_timeout() == pytest.approx(
            admin.DAEMON_HEALTH_TIMEOUT_SECONDS
            + 4 * admin.DAEMON_HEALTH_SECONDS_PER_SPEC
            + 1 * admin.DAEMON_HEALTH_SECONDS_PER_SCHEDULER_JOB
        )

    def test_an_exhausted_walk_reports_actual_population(
        self, queue: Path,
    ) -> None:
        """An exhausted inventory is complete even if the old census was larger."""
        for i in range(10):
            _write_spec(
                queue, f"sched{i:03d}", state="running",
                scheduler_target="cluster-a",
            )
        for i in range(10):
            _write_spec(queue, f"local{i:03d}", state="running")

        assert admin._count_live_scheduler_jobs(200).allowance_jobs == 10
        assert admin._count_live_scheduler_jobs(25).allowance_jobs == 10

    def test_an_empty_queue_costs_nothing(self, queue: Path) -> None:
        assert admin._count_live_scheduler_jobs(0).allowance_jobs == 0
        assert admin._daemon_health_timeout() == pytest.approx(
            admin.DAEMON_HEALTH_TIMEOUT_SECONDS
        )


class TestTheCeilingFitsTheDriverThatFailed:
    """Gap 2: the ceiling was below the real startup time it had to cover."""

    def test_the_shipped_allowance_covers_the_measured_driver(self) -> None:
        modelled = (
            admin.DAEMON_HEALTH_TIMEOUT_SECONDS
            + DRIVER_SPECS * admin.DAEMON_HEALTH_SECONDS_PER_SPEC
            + DRIVER_SCHEDULER_JOBS
            * admin.DAEMON_HEALTH_SECONDS_PER_SCHEDULER_JOB
        )
        window = min(
            modelled, float(admin.DAEMON_HEALTH_TIMEOUT_MAX_SECONDS),
        )
        # The ceiling must not clip the cost the model just computed --
        # that clipping is what made the driver unupdatable.
        assert window == pytest.approx(modelled)
        # And the result has to clear the startup that was measured.
        assert window >= DRIVER_COLD_START_SECONDS

    def test_the_scheduler_allowance_covers_the_measured_probe_rate(
        self,
    ) -> None:
        """175 jobs took about 5 min 20 s: roughly 1.8 s each, with the
        allowance carrying headroom over that rather than assuming the
        measured rate is the worst case."""
        measured_per_job = 320.0 / DRIVER_SCHEDULER_JOBS
        assert measured_per_job < admin.DAEMON_HEALTH_SECONDS_PER_SCHEDULER_JOB

    def test_the_window_is_still_bounded(
        self, queue: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A pathological state dir still cannot stretch the poll freely."""
        _write_spec(
            queue, "aaaa", state="running", scheduler_target="cluster-a",
        )
        monkeypatch.setattr(
            admin, "DAEMON_HEALTH_SECONDS_PER_SCHEDULER_JOB", 10_000.0,
        )
        assert (
            admin._daemon_health_timeout()
            == admin.DAEMON_HEALTH_TIMEOUT_MAX_SECONDS
        )


@pytest.fixture
def fake_clock(monkeypatch: pytest.MonkeyPatch) -> dict[str, float]:
    """Monotonic time the poll advances by sleeping, never by waiting.

    Replaces only ``admin``'s view of the clock, so nothing else in the
    process -- pytest's own timing included -- sees a stopped one.
    """
    clock = {"now": 0.0}
    monkeypatch.setattr(
        admin,
        "time",
        SimpleNamespace(
            **{
                name: getattr(time, name)
                for name in dir(time)
                if not name.startswith("_")
            },
        ),
    )
    monkeypatch.setattr(admin.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(
        admin.time,
        "sleep",
        lambda s: clock.__setitem__("now", clock["now"] + s),
    )
    return clock


class TestAStartingDaemonIsNotAFailedOne:
    """The verdict on expiry now depends on whether the daemon exists."""

    def test_a_running_daemon_keeps_the_poll_alive_past_the_window(
        self, monkeypatch: pytest.MonkeyPatch, fake_clock: dict[str, float],
    ) -> None:
        """#67 itself: RPC silent for longer than the allowance, daemon
        process present throughout, socket up at 591 s. Rolling that back
        cost the driver about 20 minutes of downtime."""
        from vq import rpc

        monkeypatch.setattr(admin, "_daemon_health_timeout", lambda: 510.0)

        def scanning_then_healthy() -> dict[str, object] | None:
            if fake_clock["now"] < DRIVER_COLD_START_SECONDS:
                return None
            return {"version": "0.26.9", "source_sha": EXPECTED_SHA}

        monkeypatch.setattr(rpc, "ping_user_daemon", scanning_then_healthy)

        provenance = admin._verify_restarted_daemon(
            EXPECTED_SHA, daemon_alive=lambda: True,
        )

        assert provenance.verified is True
        assert provenance.actual_sha == EXPECTED_SHA

    def test_no_daemon_process_fails_at_the_window_and_says_why(
        self, monkeypatch: pytest.MonkeyPatch, fake_clock: dict[str, float],
    ) -> None:
        """The other half: a start that really did fail must not be given
        the long allowance, and the operator must not be sent to the
        timeout knob for a problem the knob cannot fix."""
        from vq import rpc

        monkeypatch.setattr(admin, "_daemon_health_timeout", lambda: 120.0)
        monkeypatch.setattr(rpc, "ping_user_daemon", lambda: None)

        provenance = admin._verify_restarted_daemon(
            EXPECTED_SHA, daemon_alive=lambda: False,
        )

        assert provenance.verified is False
        assert "no running daemon process" in provenance.detail
        assert "will not help" in provenance.detail
        # Stopped at the window, not at the alive ceiling.
        assert fake_clock["now"] < 200.0

    def test_the_extension_is_bounded_and_proportional(
        self, monkeypatch: pytest.MonkeyPatch, fake_clock: dict[str, float],
    ) -> None:
        """A wedged-but-running daemon still ends the poll, at a multiple
        of its own allowance rather than at a flat hour."""
        from vq import rpc

        monkeypatch.setattr(admin, "_daemon_health_timeout", lambda: 120.0)
        monkeypatch.setattr(rpc, "ping_user_daemon", lambda: None)

        provenance = admin._verify_restarted_daemon(
            EXPECTED_SHA, daemon_alive=lambda: True,
        )

        limit = 120.0 * admin.DAEMON_HEALTH_ALIVE_EXTENSION_FACTOR
        assert limit < float(admin.DAEMON_HEALTH_TIMEOUT_ALIVE_MAX_SECONDS)
        assert provenance.verified is False
        assert fake_clock["now"] == pytest.approx(limit, abs=1.0)

    def test_a_short_window_is_never_stretched_into_a_long_one(
        self, monkeypatch: pytest.MonkeyPatch, fake_clock: dict[str, float],
    ) -> None:
        """A host with nothing to scan gets no startup grace.

        The extension exists because a big state dir makes the allowance a
        guess. Where the allowance is a fraction of a second there is
        nothing to be wrong about, and holding the update open anyway --
        on a flat grace, or up to a flat ceiling -- would turn every fast
        failure into a long one.
        """
        from vq import rpc

        monkeypatch.setattr(admin, "_daemon_health_timeout", lambda: 0.01)
        monkeypatch.setattr(rpc, "ping_user_daemon", lambda: None)

        provenance = admin._verify_restarted_daemon(
            EXPECTED_SHA, daemon_alive=lambda: True,
        )

        assert provenance.verified is False
        assert fake_clock["now"] < 1.0

    def test_the_absolute_ceiling_still_caps_a_huge_window(
        self, monkeypatch: pytest.MonkeyPatch, fake_clock: dict[str, float],
    ) -> None:
        from vq import rpc

        window = float(admin.DAEMON_HEALTH_TIMEOUT_MAX_SECONDS)
        monkeypatch.setattr(admin, "_daemon_health_timeout", lambda: window)
        monkeypatch.setattr(rpc, "ping_user_daemon", lambda: None)
        assert (
            window * admin.DAEMON_HEALTH_ALIVE_EXTENSION_FACTOR
            > admin.DAEMON_HEALTH_TIMEOUT_ALIVE_MAX_SECONDS
        )

        admin._verify_restarted_daemon(
            EXPECTED_SHA, daemon_alive=lambda: True,
        )

        assert fake_clock["now"] == pytest.approx(
            float(admin.DAEMON_HEALTH_TIMEOUT_ALIVE_MAX_SECONDS), abs=1.0,
        )

    def test_liveness_is_rechecked_so_a_death_ends_the_poll_promptly(
        self, monkeypatch: pytest.MonkeyPatch, fake_clock: dict[str, float],
    ) -> None:
        """The probe runs again at every step, not once."""
        from vq import rpc

        monkeypatch.setattr(admin, "_daemon_health_timeout", lambda: 120.0)
        monkeypatch.setattr(rpc, "ping_user_daemon", lambda: None)
        calls = {"n": 0}

        def dies_after_two_checks() -> bool:
            calls["n"] += 1
            return calls["n"] <= 2

        provenance = admin._verify_restarted_daemon(
            EXPECTED_SHA, daemon_alive=dies_after_two_checks,
        )

        step = (
            120.0 * (admin.DAEMON_HEALTH_ALIVE_EXTENSION_FACTOR - 1.0)
            / admin.DAEMON_HEALTH_ALIVE_LIVENESS_CHECKS
        )
        assert provenance.verified is False
        assert calls["n"] == 3
        assert "no running daemon process" in provenance.detail
        # Two steps past the window, and nowhere near the ceiling.
        assert fake_clock["now"] < 120.0 + 3 * step

    def test_a_broken_probe_is_not_evidence_the_daemon_died(
        self, monkeypatch: pytest.MonkeyPatch, fake_clock: dict[str, float],
    ) -> None:
        """A momentarily unreachable service manager must not roll back a
        valid install -- "cannot tell" is not "gone"."""
        from vq import rpc

        monkeypatch.setattr(admin, "_daemon_health_timeout", lambda: 120.0)

        def raises() -> bool:
            raise OSError("service manager unreachable")

        def scanning_then_healthy() -> dict[str, object] | None:
            if fake_clock["now"] < 300.0:
                return None
            return {"version": "0.26.9", "source_sha": EXPECTED_SHA}

        monkeypatch.setattr(rpc, "ping_user_daemon", scanning_then_healthy)

        provenance = admin._verify_restarted_daemon(
            EXPECTED_SHA, daemon_alive=raises,
        )

        assert provenance.verified is True

    def test_a_daemon_that_answered_once_is_never_extended(
        self, monkeypatch: pytest.MonkeyPatch, fake_clock: dict[str, float],
    ) -> None:
        """Once the socket is up, the startup allowance no longer applies.

        A wrong source SHA is an answer, and waiting an hour cannot change
        what that process reports. The verdict must stay the mismatch, at
        the scaled window, even though the process is alive.
        """
        from vq import rpc

        monkeypatch.setattr(admin, "_daemon_health_timeout", lambda: 120.0)
        monkeypatch.setattr(
            rpc,
            "ping_user_daemon",
            lambda: {"version": "0.26.9", "source_sha": STALE_SHA},
        )

        provenance = admin._verify_restarted_daemon(
            EXPECTED_SHA, daemon_alive=lambda: True,
        )

        assert provenance.verified is False
        assert STALE_SHA in provenance.detail
        assert fake_clock["now"] < 200.0

    def test_without_a_probe_the_window_is_exactly_what_it_always_was(
        self, monkeypatch: pytest.MonkeyPatch, fake_clock: dict[str, float],
    ) -> None:
        """Control: callers that supply no liveness probe are unchanged."""
        from vq import rpc

        monkeypatch.setattr(admin, "_daemon_health_timeout", lambda: 156.0)
        monkeypatch.setattr(rpc, "ping_user_daemon", lambda: None)

        provenance = admin._verify_restarted_daemon(EXPECTED_SHA)

        assert provenance.verified is False
        assert "within 156s" in provenance.detail
        assert "VQ_DAEMON_HEALTH_TIMEOUT" in provenance.detail
        assert fake_clock["now"] < 160.0


class TestTheUpdateLanesCarryTheProbe:
    """The fix is only real if the lanes that roll back actually pass it."""

    def test_the_service_probe_retains_the_managers_unknown_state(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            admin, "_query_daemon_service_state",
            lambda manager: SimpleNamespace(running=None),
        )
        assert admin._service_daemon_alive_probe(
            admin._DaemonServiceManager.SYSTEMD,
        )() is None

    @pytest.mark.parametrize(
        "lane",
        [
            "_complete_managed_daemon_update",
            "_recover_managed_daemon_after_exception",
            "_maybe_restart_daemon",
            "_verify_preupdate_daemon_identity",
        ],
    )
    def test_every_verifying_lane_builds_a_probe(self, lane: str) -> None:
        """Each lane that verifies a daemon must have liveness to offer.

        The defect was that these call sites had no evidence at all beyond
        "RPC is silent", so a bare call is what regressing looks like.
        Read off the compiled function's global references rather than its
        source text, so reformatting -- or a file edited under a running
        suite -- cannot turn this into a false verdict.
        """
        names = getattr(admin, lane).__code__.co_names
        assert "_service_daemon_alive_probe" in names


class TestSizingTheWindowNeverRaises:
    """The scan's stated contract, found missing by the #67 merge review.

    ``_count_live_scheduler_jobs`` says of itself that "sizing a timeout must
    never raise into the update lane", but it skipped an unreadable spec only
    for ``(OSError, ValueError, AttributeError, TypeError)``. ``json.loads``
    raises ``RecursionError`` on a deeply nested record, which is none of
    those, so the exception left the scan, left
    :func:`~vq.admin._daemon_health_timeout`, and aborted whichever lane had
    asked for a window -- ``_recover_managed_daemon_after_exception``
    included, which is the lane that runs once an update has already failed.
    """

    def test_a_deeply_nested_spec_does_not_escape_the_scan(
        self, queue: Path,
    ) -> None:
        queue.mkdir(parents=True, exist_ok=True)
        # Deeper than the interpreter's recursion limit by a wide margin, so
        # this is a property of the parser rather than of this machine.
        (queue / "aaaa.json").write_text(
            '{"a":' * 200_000 + "1" + "}" * 200_000
        )

        assert admin._count_live_scheduler_jobs(1).allowance_jobs == 0

    def test_the_window_is_still_returned_for_such_a_queue(
        self, queue: Path,
    ) -> None:
        """The public entry point the update lanes actually call."""
        queue.mkdir(parents=True, exist_ok=True)
        (queue / "aaaa.json").write_text(
            '{"a":' * 200_000 + "1" + "}" * 200_000
        )
        _write_spec(
            queue, "bbbb", state="running", scheduler_target="cluster-a",
        )

        # The unparsable spec is not counted; the readable one is, and the
        # window is a number rather than a traceback.
        assert admin._daemon_health_timeout() == pytest.approx(
            admin.DAEMON_HEALTH_TIMEOUT_SECONDS
            + 2 * admin.DAEMON_HEALTH_SECONDS_PER_SPEC
            + 1 * admin.DAEMON_HEALTH_SECONDS_PER_SCHEDULER_JOB
        )

    def test_an_unlistable_queue_dir_retains_unread_records(
        self, queue: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The outer guard, for the same reason: degrade, never raise."""
        class _Exploding:
            def iterdir(self) -> object:
                raise RecursionError("the directory walk itself")

        monkeypatch.setattr(admin.paths, "queue_dir", _Exploding)

        assert admin._count_live_scheduler_jobs(10).allowance_jobs == 10
