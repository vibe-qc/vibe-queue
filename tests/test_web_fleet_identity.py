"""Host identity, version labelling, and snapshot freshness on /fleet.

These pin the three things the 2026-08-05 fleet audit found the console
could not tell an operator:

* which *machine* a card is about, as opposed to which config key;
* whose version a version number belongs to, and what to show when there
  is genuinely none;
* whether what is on screen is current.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from vq.overview import HostOverview
from vq.web import fleet as fleet_mod
from vq.web import view_context


def _snapshot(
    *hosts: fleet_mod.HostSnapshot, gathered_at: str | None = None
) -> fleet_mod.FleetSnapshot:
    return fleet_mod.FleetSnapshot(
        gathered_at=gathered_at or datetime.now(UTC).isoformat(),
        duration_seconds=1.0,
        hosts=list(hosts),
    )


def _host(
    key: str,
    *,
    reported: str | None = None,
    version: str | None = "0.24.0",
    helper: str | None = None,
    jobs: int = 0,
    reachable: bool = True,
) -> fleet_mod.HostSnapshot:
    return fleet_mod.HostSnapshot(
        host=key,
        overview=HostOverview(
            host=key,
            reachable=reachable,
            vq_version=version,
            helper_source_sha=helper,
            reported_hostname=reported,
        ),
        jobs=[{"id": f"{key}{i}"} for i in range(jobs)],
    )


class TestVerifiedIdentity:
    def test_card_carries_the_machines_own_name(self) -> None:
        card = view_context.fleet_host_card(_host("localhost", reported="host_0"))
        assert card["reported_hostname"] == "host_0"

    def test_mismatch_is_flagged_when_key_and_machine_disagree(self) -> None:
        """The `localhost` case: a machine-relative key that means a
        different machine depending on which host reads the config."""
        card = view_context.fleet_host_card(_host("localhost", reported="host_0"))
        assert card["identity_mismatch"] is True

    def test_matching_name_is_not_flagged(self) -> None:
        card = view_context.fleet_host_card(_host("host_a", reported="host_a"))
        assert card["identity_mismatch"] is False

    def test_absent_hostname_is_not_a_mismatch(self) -> None:
        """A host running an older vq reports no name. Unknown identity
        must not render as a contradiction."""
        card = view_context.fleet_host_card(_host("host_a", reported=None))
        assert card["identity_mismatch"] is False
        assert card["reported_hostname"] is None


class TestDuplicateEnrolment:
    def test_two_keys_one_machine_are_reported(self) -> None:
        """Both keys are swept and both render, so every job on that
        machine is counted twice in the jobs table and its totals."""
        snapshot = _snapshot(
            _host("host_0", reported="host_0"),
            _host("localhost", reported="host_0"),
            _host("host_a", reported="host_a"),
        )
        assert snapshot.duplicate_enrolments == [
            ("host_0", ["host_0", "localhost"])
        ]

    def test_the_double_count_is_real(self) -> None:
        """Guards the claim the banner makes, so it cannot become a lie."""
        snapshot = _snapshot(
            _host("host_0", reported="host_0", jobs=3),
            _host("localhost", reported="host_0", jobs=3),
        )
        assert len(snapshot.jobs) == 6

    def test_distinct_machines_are_not_reported(self) -> None:
        snapshot = _snapshot(
            _host("host_a", reported="host_a"), _host("host_b", reported="host_b")
        )
        assert snapshot.duplicate_enrolments == []

    def test_scheduler_aliases_do_not_trip_it(self) -> None:
        """host_f / host_f-amd / host_f-big deliberately share one SSH target.

        They are distinct queues on one cluster, not a duplicated
        machine. Being daemonless they report no hostname, so they must
        never be flagged -- a false positive here would train operators
        to ignore the banner.
        """
        snapshot = _snapshot(
            _host("host_f", reported=None, version=None, helper="c1f56871"),
            _host("host_f-amd", reported=None, version=None, helper="c1f56871"),
            _host("host_f-big", reported=None, version=None, helper="c1f56871"),
        )
        assert snapshot.duplicate_enrolments == []

    def test_it_reaches_the_grid_context(self) -> None:
        snapshot = _snapshot(
            _host("host_0", reported="host_0"),
            _host("localhost", reported="host_0"),
        )
        context = view_context.fleet_grid_context(snapshot)
        assert context["duplicate_enrolments"] == [
            ("host_0", ["host_0", "localhost"])
        ]


class TestVersionLabel:
    def test_a_real_version_is_shown(self) -> None:
        assert view_context.host_version_label(
            HostOverview(host="host_a", vq_version="0.24.0")
        ) == "vq 0.24.0"

    def test_a_daemonless_scheduler_host_shows_its_helper_sha(self) -> None:
        """It has no vq of its own, but it does run a helper whose skew
        from the driver has caused an incident -- and which the CLI
        already prints. The web console printed a blank."""
        label = view_context.host_version_label(
            HostOverview(
                host="host_f",
                vq_version=None,
                helper_source_sha="c1f568717f438b62d42daba8dcbefe2f568ce10d",
            )
        )
        assert label == "vq helper c1f568717"

    def test_nothing_known_says_so_explicitly(self) -> None:
        """Silence is indistinguishable from agreement. The chip used to
        render only `{% if card.vq_version %}`, so a host with nothing to
        report showed nothing at all."""
        assert view_context.host_version_label(
            HostOverview(host="x", vq_version=None)
        ) == "vq version unknown"

    def test_a_real_version_wins_over_a_helper_sha(self) -> None:
        assert view_context.host_version_label(
            HostOverview(host="x", vq_version="0.24.0", helper_source_sha="abc123def")
        ) == "vq 0.24.0"


class TestSnapshotFreshness:
    def test_a_fresh_snapshot_is_not_stale(self) -> None:
        context = view_context.fleet_grid_context(
            _snapshot(_host("host_a")), interval_seconds=30
        )
        assert context["snapshot_stale"] is False

    def test_an_old_snapshot_is_stale(self) -> None:
        """A dead sweep thread keeps serving its last good snapshot while
        the page goes on polling every 10 s, so it still animates. Age is
        the only signal that it has stopped."""
        old = (datetime.now(UTC) - timedelta(minutes=10)).isoformat()
        context = view_context.fleet_grid_context(
            _snapshot(_host("host_a"), gathered_at=old), interval_seconds=30
        )
        assert context["snapshot_stale"] is True
        assert context["snapshot_age_seconds"] >= 600

    def test_the_threshold_follows_the_configured_interval(self) -> None:
        """A site sweeping every 5 minutes must not see a permanent
        stale banner."""
        age = (datetime.now(UTC) - timedelta(minutes=4)).isoformat()
        snapshot = _snapshot(_host("host_a"), gathered_at=age)
        fast = view_context.fleet_grid_context(snapshot, interval_seconds=30)
        slow = view_context.fleet_grid_context(snapshot, interval_seconds=300)
        assert fast["snapshot_stale"] is True
        assert slow["snapshot_stale"] is False

    def test_no_snapshot_is_not_stale(self) -> None:
        """"We have not swept yet" and "the sweep died" are different
        states and must not render the same alarm."""
        context = view_context.fleet_grid_context(None)
        assert context["snapshot_stale"] is False
        assert context["snapshot_age_seconds"] is None

    def test_an_unparseable_timestamp_is_not_stale(self) -> None:
        context = view_context.fleet_grid_context(
            _snapshot(_host("host_a"), gathered_at="not-a-time")
        )
        assert context["snapshot_age_seconds"] is None
        assert context["snapshot_stale"] is False


class TestFleetSummary:
    def test_counts_roll_up_across_hosts(self) -> None:
        a = _host("host_a")
        a.overview.queue_counts = {"running": 2, "pending": 5}
        a.overview.running_cpus = 8
        b = _host("host_b")
        b.overview.queue_counts = {"running": 3, "pending": 1}
        b.overview.running_cpus = 4
        summary = view_context.fleet_grid_context(_snapshot(a, b))["summary"]
        assert summary["hosts"] == 2
        assert summary["running"] == 5
        assert summary["pending"] == 6
        assert summary["running_cpus"] == 12

    def test_one_version_across_the_fleet_is_converged(self) -> None:
        summary = view_context.fleet_grid_context(
            _snapshot(_host("host_a"), _host("host_b"))
        )["summary"]
        assert summary["vq_versions"] == ["0.24.0"]
        assert summary["converged"] is True

    def test_mixed_versions_are_not_converged(self) -> None:
        """The number an operator running a rolling update watches."""
        summary = view_context.fleet_grid_context(
            _snapshot(_host("host_a"), _host("host_b", version="0.16.0"))
        )["summary"]
        assert summary["vq_versions"] == ["0.16.0", "0.24.0"]
        assert summary["converged"] is False

    def test_hosts_with_no_version_do_not_break_convergence(self) -> None:
        """Scheduler hosts report no vq version. Counting `None` as a
        distinct version would report every fleet with a cluster in it as
        permanently unconverged."""
        summary = view_context.fleet_grid_context(
            _snapshot(_host("host_a"), _host("host_f", version=None, helper="abc123def"))
        )["summary"]
        assert summary["vq_versions"] == ["0.24.0"]
        assert summary["converged"] is True

    def test_unreachable_hosts_are_counted(self) -> None:
        summary = view_context.fleet_grid_context(
            _snapshot(_host("host_a"), _host("host_e", reachable=False, version=None))
        )["summary"]
        assert summary["unreachable"] == 1


class TestSelfReportedFlag:
    def test_local_gather_is_marked_self_reported(self) -> None:
        """Distinguishes "host_0 probed host_0 over SSH" from "the
        console read its own constants and labelled them host_0"."""
        overview = HostOverview(host="x")
        overview.self_reported = True
        card = view_context.fleet_host_card(
            fleet_mod.HostSnapshot(host="x", overview=overview)
        )
        assert card["self_reported"] is True

    def test_remote_gather_is_not(self) -> None:
        card = view_context.fleet_host_card(_host("host_a"))
        assert card["self_reported"] is False


class TestOverviewJsonRoundTrip:
    def test_reported_hostname_survives_the_wire(self) -> None:
        """The seam where verified identity used to be dropped:
        _overview_from_json rebuilt HostOverview(host=host) and threw the
        payload's own identity away."""
        from vq import overview as overview_mod

        source = HostOverview(host="host_a", reachable=True, vq_version="0.24.0")
        source.reported_hostname = "host_a-01"
        payload = overview_mod.format_overview_json(source)
        assert payload["reported_hostname"] == "host_a-01"

        restored = overview_mod._overview_from_json("host_a", payload)
        assert restored.reported_hostname == "host_a-01"

    def test_an_older_host_reporting_nothing_round_trips_as_none(self) -> None:
        """No fleet-wide upgrade gate: a host on an older vq simply omits
        the key and renders no subtitle."""
        from vq import overview as overview_mod

        restored = overview_mod._overview_from_json("host_a", {"vq_version": "0.20.0"})
        assert restored.reported_hostname is None

    @pytest.mark.parametrize("bogus", [123, [], {}, True])
    def test_a_non_string_hostname_is_rejected(self, bogus: object) -> None:
        from vq import overview as overview_mod

        restored = overview_mod._overview_from_json(
            "host_a", {"reported_hostname": bogus}
        )
        assert restored.reported_hostname is None


class TestSchedulerHostLabel:
    """A daemonless host with no helper record says which it is.

    "This host has no vq of its own and nobody has recorded its helper
    yet" is a different and far more actionable statement than "we could
    not find out". Both used to render as the same bare "unknown".
    """

    def test_scheduler_host_without_a_helper_record(self) -> None:
        overview = HostOverview(host="host_f", vq_version=None)
        overview.is_scheduler_host = True
        assert (
            view_context.host_version_label(overview)
            == "daemonless · helper not recorded"
        )

    def test_a_recorded_helper_still_wins(self) -> None:
        overview = HostOverview(
            host="host_f", vq_version=None, helper_source_sha="c1f568717f43"
        )
        overview.is_scheduler_host = True
        assert view_context.host_version_label(overview) == "vq helper c1f568717"

    def test_a_non_scheduler_host_still_says_unknown(self) -> None:
        """A daemon host that reported nothing is a different problem and
        must not be excused as 'daemonless'."""
        assert (
            view_context.host_version_label(HostOverview(host="host_a"))
            == "vq version unknown"
        )

    def test_the_flag_reaches_the_card(self) -> None:
        overview = HostOverview(host="host_f", vq_version=None)
        overview.is_scheduler_host = True
        card = view_context.fleet_host_card(
            fleet_mod.HostSnapshot(host="host_f", overview=overview)
        )
        assert card["version_label"] == "daemonless · helper not recorded"


def _job(
    jobid: str,
    *,
    state: str,
    host: str = "host_a",
    submitted: str = "2026-08-01T00:00:00+00:00",
    cpus: int = 1,
    mem: int = 1024,
    cwd: str = "",
    name: str = "",
) -> dict:
    return {
        "id": jobid, "state": state, "queue_host": host, "submitted_at": submitted,
        "cpus": cpus, "mem_mb": mem, "cwd": cwd, "job_name": name, "command": ["python", "x.py"],
    }


def _jobs_snapshot(*rows: dict) -> fleet_mod.FleetSnapshot:
    return fleet_mod.FleetSnapshot(
        gathered_at=datetime.now(UTC).isoformat(),
        duration_seconds=1.0,
        hosts=[
            fleet_mod.HostSnapshot(
                host="host_a", overview=HostOverview(host="host_a"), jobs=list(rows)
            )
        ],
    )


class TestRunningJobsSortFirst:
    """A running job must be visible without scrolling.

    Sorting by submission time alone buries it: a job running for two
    days was submitted two days ago, so it sinks below everything that
    has completed since. On a busy host the only running job sat
    hundreds of rows down.
    """

    def test_running_beats_a_newer_completed_job(self) -> None:
        snapshot = _jobs_snapshot(
            _job("done", state="completed", submitted="2026-08-06T12:00:00+00:00"),
            _job("live", state="running", submitted="2026-08-01T00:00:00+00:00"),
        )
        ctx = view_context.fleet_jobs_context(snapshot, {})
        assert [j["id"] for j in ctx["jobs"]] == ["live", "done"]

    def test_active_states_rank_running_then_suspended_then_pending(self) -> None:
        snapshot = _jobs_snapshot(
            _job("c", state="completed"),
            _job("p", state="pending"),
            _job("r", state="running"),
            _job("s", state="suspended"),
        )
        ctx = view_context.fleet_jobs_context(snapshot, {})
        assert [j["id"] for j in ctx["jobs"]] == ["r", "s", "p", "c"]

    def test_within_a_rank_newest_first(self) -> None:
        snapshot = _jobs_snapshot(
            _job("old", state="running", submitted="2026-08-01T00:00:00+00:00"),
            _job("new", state="running", submitted="2026-08-06T00:00:00+00:00"),
        )
        ctx = view_context.fleet_jobs_context(snapshot, {})
        assert [j["id"] for j in ctx["jobs"]] == ["new", "old"]

    def test_terminal_states_share_one_rank(self) -> None:
        """failed/killed/completed are equally finished; recency decides."""
        snapshot = _jobs_snapshot(
            _job("f", state="failed", submitted="2026-08-02T00:00:00+00:00"),
            _job("k", state="killed", submitted="2026-08-03T00:00:00+00:00"),
            _job("c", state="completed", submitted="2026-08-01T00:00:00+00:00"),
        )
        ctx = view_context.fleet_jobs_context(snapshot, {})
        assert [j["id"] for j in ctx["jobs"]] == ["k", "f", "c"]


class TestSortableColumns:
    @pytest.mark.parametrize(
        "field", sorted(view_context.FLEET_SORT_FIELDS)
    )
    def test_every_column_sorts_without_error(self, field: str) -> None:
        """Every rendered column is clickable, so every one must work."""
        snapshot = _jobs_snapshot(
            _job("b", state="running", cpus=2, mem=2048, cwd="/b", name="beta"),
            _job("a", state="completed", cpus=1, mem=1024, cwd="/a", name="alpha"),
        )
        ctx = view_context.fleet_jobs_context(snapshot, {"sort": field})
        assert len(ctx["jobs"]) == 2

    def test_numeric_columns_sort_numerically(self) -> None:
        """String ordering would put 9 above 10."""
        snapshot = _jobs_snapshot(
            _job("small", state="completed", cpus=9),
            _job("big", state="completed", cpus=10),
        )
        ctx = view_context.fleet_jobs_context(snapshot, {"sort": "cpus", "dir": "desc"})
        assert [j["id"] for j in ctx["jobs"]] == ["big", "small"]

    def test_direction_flips(self) -> None:
        snapshot = _jobs_snapshot(
            _job("a", state="completed", cpus=1), _job("b", state="completed", cpus=2)
        )
        desc = view_context.fleet_jobs_context(snapshot, {"sort": "cpus", "dir": "desc"})
        asc = view_context.fleet_jobs_context(snapshot, {"sort": "cpus", "dir": "asc"})
        assert [j["id"] for j in desc["jobs"]] == ["b", "a"]
        assert [j["id"] for j in asc["jobs"]] == ["a", "b"]

    def test_an_explicit_sort_overrides_the_running_first_default(self) -> None:
        snapshot = _jobs_snapshot(
            _job("live", state="running", cpus=1),
            _job("done", state="completed", cpus=8),
        )
        ctx = view_context.fleet_jobs_context(snapshot, {"sort": "cpus", "dir": "desc"})
        assert [j["id"] for j in ctx["jobs"]] == ["done", "live"]

    def test_an_unknown_sort_falls_back_to_the_default(self) -> None:
        snapshot = _jobs_snapshot(
            _job("done", state="completed", submitted="2026-08-06T12:00:00+00:00"),
            _job("live", state="running"),
        )
        ctx = view_context.fleet_jobs_context(snapshot, {"sort": "'; DROP TABLE"})
        assert ctx["query"]["sort"] == view_context.DEFAULT_FLEET_SORT
        assert [j["id"] for j in ctx["jobs"]] == ["live", "done"]

    def test_sort_href_flips_only_the_active_column(self) -> None:
        query = view_context.fleet_query_state({"sort": "cpus", "dir": "desc"})
        assert "dir=asc" in view_context.fleet_sort_href(query, "cpus")
        # A different column starts descending: newest/biggest/most-CPUs
        # is what an operator reaches for first.
        assert "dir=asc" not in view_context.fleet_sort_href(query, "mem")

    def test_sort_label_marks_only_the_active_column(self) -> None:
        query = view_context.fleet_query_state({"sort": "cpus", "dir": "desc"})
        assert view_context.fleet_sort_label(query, "cpus").strip() == "v"
        assert view_context.fleet_sort_label(query, "mem") == ""

    def test_filters_survive_a_sort_click(self) -> None:
        query = view_context.fleet_query_state({"host": "host_a", "state": "running"})
        href = view_context.fleet_sort_href(query, "cpus")
        assert "host=host_a" in href and "state=running" in href


class TestWorkdirColumn:
    def test_cwd_reaches_the_row_view(self) -> None:
        row = _job("a", state="running", cwd="/home/USER/vq/work/abc123")
        assert view_context.fleet_row_view(row)["cwd"] == "/home/USER/vq/work/abc123"

    def test_a_missing_cwd_renders_as_empty_not_none(self) -> None:
        """The template length-checks it; None would raise."""
        assert view_context.fleet_row_view(_job("a", state="running"))["cwd"] == ""


def test_completed_slow_sweep_keeps_observation_age_without_false_missed_sweep():
    now = datetime.now(UTC)
    snapshot = _snapshot(gathered_at=(now - timedelta(seconds=300)).isoformat())
    snapshot.duration_seconds = 300
    context = view_context.fleet_grid_context(snapshot, now=now, interval_seconds=30)
    assert context["snapshot_age_seconds"] == 300
    assert context["snapshot_stale"] is False
    later = view_context.fleet_grid_context(
        snapshot, now=now + timedelta(seconds=100), interval_seconds=30,
    )
    assert later["snapshot_stale"] is True
