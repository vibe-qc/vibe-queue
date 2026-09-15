"""Direct contracts for the web layer's pure view projections."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

from vq.overview import HostOverview
from vq.spec import JobSpec, JobState
from vq.web import fleet as fleet_mod
from vq.web import view_context


def _spec(jobid: str, **overrides: object) -> JobSpec:
    fields: dict[str, object] = {
        "id": jobid,
        "command": ["python", f"{jobid}.py"],
        "cwd": f"/tmp/{jobid}",
        "cpus": 1,
    }
    fields.update(overrides)
    return JobSpec(**fields)


def test_queue_query_defaults_and_invalid_values() -> None:
    expected = {
        "sort": "submitted",
        "dir": "desc",
        "state": "all",
        "host": "all",
        "q": "",
        "triage": False,
        "limit": "200",
    }

    assert view_context.queue_query_state({}) == expected
    assert view_context.queue_query_state(
        {"sort": "bogus", "dir": "sideways", "limit": "many"}
    ) == expected
    assert view_context.queue_query_string(expected) == ""
    assert view_context.queue_table_href(expected) == "/queue/_table"


def test_queue_query_normalization_and_links() -> None:
    query = view_context.queue_query_state(
        {
            "sort": "id",
            "dir": "asc",
            "state": " FAILED ",
            "host": " host_a ",
            "q": " water ",
            "triage": "yes",
            "limit": "50",
        }
    )

    assert query == {
        "sort": "id",
        "dir": "asc",
        "state": "failed",
        "host": "host_a",
        "q": "water",
        "triage": True,
        "limit": "50",
    }
    assert view_context.queue_query_string(query) == (
        "state=failed&host=host_a&q=water&sort=id&dir=asc&triage=1&limit=50"
    )
    assert view_context.queue_table_href(query) == (
        "/queue/_table?"
        "state=failed&host=host_a&q=water&sort=id&dir=asc&triage=1&limit=50"
    )
    assert view_context.queue_sort_href(query, "id") == (
        "/queue?state=failed&host=host_a&q=water&sort=id&triage=1&limit=50"
    )
    assert view_context.queue_sort_label(query, "id") == " ^"
    assert view_context.queue_sort_label(query, "state") == ""


def test_parse_time_for_sort_contract() -> None:
    floor = datetime.min.replace(tzinfo=UTC)

    assert view_context.parse_time_for_sort(None) == floor
    assert view_context.parse_time_for_sort("not-a-time") == floor
    assert view_context.parse_time_for_sort("2026-08-02T10:00:00") == datetime(
        2026, 8, 2, 10, tzinfo=UTC
    )
    assert view_context.parse_time_for_sort(
        "2026-08-02T06:00:00-04:00"
    ) == datetime(2026, 8, 2, 10, tzinfo=UTC)


def test_filter_and_sort_specs_contract() -> None:
    old_completed = _spec(
        "old",
        state=JobState.COMPLETED,
        submitted_at="2026-08-01T10:00:00+00:00",
    )
    failed = _spec(
        "failed",
        state=JobState.FAILED,
        scheduler_target="host_a",
        submitter="alice",
        job_name="water-run",
        failure_reason="command exited 2",
        submitted_at="2026-08-02T10:00:00+00:00",
    )
    running = _spec(
        "running",
        state=JobState.RUNNING,
        scheduler_target="host_a",
        submitted_at="2026-08-03T10:00:00+00:00",
    )
    specs = [old_completed, failed, running]

    default_query = view_context.queue_query_state({})
    assert [
        spec.id
        for spec in view_context.filter_and_sort_specs(specs, default_query)
    ] == ["running", "failed", "old"]
    assert [spec.id for spec in specs] == ["old", "failed", "running"]

    filtered_query = view_context.queue_query_state(
        {
            "state": "failed",
            "host": "host_a",
            "q": "water",
            "triage": "1",
        }
    )
    assert [
        spec.id
        for spec in view_context.filter_and_sort_specs(specs, filtered_query)
    ] == ["failed"]


def test_cockpit_summary_contract() -> None:
    specs = [
        _spec("pending", state=JobState.PENDING, mem_mb=1000),
        _spec("running", state=JobState.RUNNING, cpus=4, mem_mb=2000),
        _spec(
            "suspended",
            state=JobState.SUSPENDED,
            cpus=2,
            scheduler_target="host_a",
        ),
        _spec(
            "submit-unknown",
            state=JobState.SUBMIT_OUTCOME_UNKNOWN,
            cpus=3,
            scheduler_target="host_f",
        ),
        _spec(
            "failed",
            state=JobState.FAILED,
            scheduler_target="host_f",
            failure_reason="command exited 2",
        ),
    ]

    assert view_context.cockpit_summary(specs) == {
        "total": 5,
        "active": 3,
        "pending": 1,
        "terminal_attention": 1,
        "active_cpus": 9,
        "declared_mem_mb": 3000,
        "scheduler_hosts": ["host_a", "host_f"],
        "state_counts": {
            "pending": 1,
            "running": 1,
            "suspended": 1,
            "submit_outcome_unknown": 1,
            "failed": 1,
        },
    }


def test_safe_fleet_next_and_idle_format_contract() -> None:
    assert view_context.safe_fleet_next("/fleet/jobs?q=water") == (
        "/fleet/jobs?q=water"
    )
    for unsafe in (None, "", "fleet", "//evil.example/path"):
        assert view_context.safe_fleet_next(unsafe) == "/fleet"

    assert [
        view_context.format_idle(value)
        for value in (None, 59, 60, 3599, 3600, 86399, 86400, 90000)
    ] == [None, "59s", "1m", "59m", "1h00m", "23h59m", "1d0h", "1d1h"]


def test_fleet_host_card_contract() -> None:
    host_snapshot = fleet_mod.HostSnapshot(
        host="host_a",
        overview=HostOverview(
            host="host_a",
            reachable=True,
            admin_down="maintenance",
            error="stale error",
            vq_version="0.15.1",
            daemon_health=SimpleNamespace(ok=True),
            queue_counts={"running": 2, "pending": 3},
            scheduler_queue_counts={"queued": 4},
            running_cpus=8,
            max_cpus=16,
            mem_available_mb=12000,
            mem_total_mb=32000,
            drain_state=SimpleNamespace(),
            throttle_state=SimpleNamespace(),
            admin_marker_summary="update active",
            admin_marker_stale_reason="heartbeat stale",
            idle_seconds=3725,
        ),
        jobs=[{"id": "a"}, {"id": "b"}],
        jobs_error="listing partial",
    )

    assert view_context.fleet_host_card(host_snapshot) == {
        "host": "host_a",
        "reachable": True,
        "admin_down": "maintenance",
        "error": "stale error",
        "jobs_error": "listing partial",
        "vq_version": "0.15.1",
        "daemon_ok": True,
        "queue_counts": {"running": 2, "pending": 3},
        "scheduler_queue_counts": {"queued": 4},
        "running": 2,
        "pending": 3,
        "running_cpus": 8,
        "max_cpus": 16,
        "mem_available_mb": 12000,
        "mem_total_mb": 32000,
        "draining": True,
        "throttled": True,
        "admin_marker_summary": "update active",
        "admin_marker_stale_reason": "heartbeat stale",
        "idle": "1h02m",
        "job_count": 2,
        # v0.25.0. The card used to drop `envs` and `helper_source_sha`,
        # which made the HTML dashboard strictly less informative than
        # `vq overview` and unable to answer "which program version is
        # deployed on this host?" at all.
        "envs": [],
        "helper_source_sha": None,
        "recent_terminal_counts": {},
        # Never an empty version chip: silence was indistinguishable from
        # agreement, and a daemonless scheduler host rendered blank even
        # though the CLI prints its helper SHA.
        "version_label": "vq 0.15.1",
        # Verified identity -- what the machine calls itself, as opposed
        # to the config key an operator typed.
        "reported_hostname": None,
        "identity_mismatch": False,
        "self_reported": False,
    }


def _fleet_row(jobid: str, **overrides: object) -> dict:
    row = {
        "id": jobid,
        "queue_host": "host_a",
        "state": "running",
        "scheduler_state": "queued",
        "cpus": 4,
        "mem_mb": 8000,
        "submitted_at": "2026-08-02T10:00:00+00:00",
        "submitter": "alice",
        "job_name": "water-run",
        "tags": ["chemistry"],
        "command": ["python", "water.py"],
        "failure_reason": None,
        "terminal_diagnosis": {"summary": "needs attention"},
    }
    row.update(overrides)
    return row


def test_fleet_query_row_filter_and_projection_contract() -> None:
    query = view_context.fleet_query_state(
        {
            "host": " host_a ",
            "state": " RUNNING ",
            "tag": " chemistry ",
            "submitter": " alice ",
            "q": " WATER ",
            "limit": "50",
        }
    )
    row = _fleet_row("job-a")

    assert query == {
        "host": "host_a",
        "state": "running",
        "tag": "chemistry",
        "submitter": "alice",
        "q": "WATER",
        "limit": "50",
        "sort": "status",
        "dir": "desc",
    }
    # The default sort and direction stay OUT of the query string, so a
    # shared link carries only what the operator actually chose.
    assert view_context.fleet_query_string(query) == (
        "host=host_a&state=running&tag=chemistry&submitter=alice&q=WATER&limit=50"
    )
    assert view_context.fleet_row_matches(row, query) is True
    assert view_context.fleet_row_matches(
        {**row, "queue_host": "host_f"}, query
    ) is False
    assert view_context.fleet_row_view(row) == {
        "id": "job-a",
        "host": "host_a",
        "state": "running",
        "sched": "queued",
        "cpus": 4,
        "mem_mb": 8000,
        "submitted_at": "2026-08-02T10:00:00+00:00",
        "submitter": "alice",
        "name": "water-run",
        "tags": ["chemistry"],
        # v0.25.x: two jobs running the same command in different
        # checkouts are otherwise indistinguishable in this table, which
        # is the normal case on a fleet running one script against
        # several trees.
        "cwd": "",
        "command": "python water.py",
        "diagnosis": "needs attention",
    }


def test_fleet_snapshot_context_contract() -> None:
    newer = _fleet_row(
        "newer",
        submitted_at="2026-08-03T10:00:00+00:00",
    )
    older = _fleet_row(
        "older",
        state="failed",
        submitted_at="2026-08-01T10:00:00+00:00",
    )
    host_snapshot = fleet_mod.HostSnapshot(
        host="host_a",
        overview=HostOverview(host="host_a"),
        jobs=[older, newer],
    )
    snapshot = fleet_mod.FleetSnapshot(
        gathered_at="2026-08-03T11:00:00+00:00",
        duration_seconds=1.0,
        hosts=[host_snapshot],
    )

    context = view_context.fleet_jobs_context(snapshot, {"limit": "50"})

    assert context["snapshot"] is snapshot
    assert context["query"] == {
        "host": "all",
        "state": "all",
        "tag": "",
        "submitter": "",
        "q": "",
        "limit": "50",
        # v0.25.x: the table sorts. "status" is not a column -- it ranks
        # active work above finished work, so a job running for two days
        # is not buried under everything that completed since.
        "sort": "status",
        "dir": "desc",
    }
    assert [row["id"] for row in context["jobs"]] == ["newer", "older"]
    assert context["host_options"] == ["host_a"]
    assert context["state_options"] == ["failed", "running"]
    assert context["row_limit_options"] == view_context.ROW_LIMITS
    assert context["shown_count"] == 2
    assert context["filtered_count"] == 2
    assert context["total_count"] == 2
    assert context["table_refresh_url"] == "/fleet/jobs/_table?limit=50"

    grid = view_context.fleet_grid_context(snapshot)
    assert grid["snapshot"] is snapshot
    assert grid["cards"] == [view_context.fleet_host_card(host_snapshot)]
    assert grid["duplicate_enrolments"] == []
    assert grid["summary"]["hosts"] == 1

    empty = view_context.fleet_grid_context(None)
    assert empty["snapshot"] is None
    assert empty["cards"] == []
    assert empty["summary"]["hosts"] == 0
    # No snapshot means no age to report -- and, critically, NOT stale:
    # "we have not swept yet" and "the sweep died" are different states
    # and must not render the same alarm.
    assert empty["snapshot_age_seconds"] is None
    assert empty["snapshot_stale"] is False


def test_doctor_context_contract() -> None:
    state = {
        "results": [
            {"host": "host_a", "ok": True},
            {"host": "host_f", "ok": False},
            {"host": "host_e"},
        ],
        "gathered_at": "2026-08-03T12:00:00+00:00",
        "refreshing": True,
    }

    assert view_context.doctor_context(state) == {
        "results": state["results"],
        "gathered_at": "2026-08-03T12:00:00+00:00",
        "refreshing": True,
        "ok_count": 1,
        "bad_count": 2,
    }
