"""#168: all cached matching jobs remain reachable beyond the row limit."""
from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from html import unescape
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit

import pytest
from fastapi.testclient import TestClient

from vq import auth, config, paths, transport
from vq.overview import HostOverview
from vq.web import create_app, view_context
from vq.web import fleet as fleet_mod

FILTERS = {
    "host": "host_a", "state": "failed", "program": "simulator",
    "project": "water & ice", "tag": "history & samples", "submitter": "alice",
    "q": "calculation", "limit": "50", "sort": "id", "dir": "asc",
}


def _snapshot(count: int = 121, *, state: str = "failed") -> fleet_mod.FleetSnapshot:
    now = datetime(2026, 10, 12, 12, tzinfo=UTC)
    rows = [{
        "id": f"history{i:03}", "queue_host": "host_a", "queue_owner": "host_a",
        "state": state, "effective_state": state, "program": "simulator",
        "tags": ["project.water & ice", "history & samples"], "submitter": "alice",
        "command": ["python", "calculation.py"], "cpus": 2, "mem_mb": 1000,
        "submitted_at": (now + timedelta(minutes=i)).isoformat(),
    } for i in range(count)]
    # Newer jobs from another owner must not consume space on a filtered page.
    excluded = [dict(row, id=f"excluded{i:03}", submitter="bob")
                for i, row in enumerate(rows[:20])]
    return fleet_mod.FleetSnapshot(
        gathered_at=now.isoformat(), duration_seconds=1.0,
        hosts=[fleet_mod.HostSnapshot(
            host="host_a", overview=HostOverview(host="host_a"), jobs=rows + excluded,
        )],
    )


@pytest.mark.parametrize("raw", ["", "0", "-2", "many", "1.5", "9" * 5000],
                         ids=["empty", "zero", "negative", "text", "fraction", "oversized"])
def test_invalid_pages_fall_back_to_first_page(raw: str) -> None:
    query = view_context.fleet_query_state({"page": raw})
    assert query["page"] == "1"
    assert view_context.fleet_query_string(query) == ""


@pytest.mark.parametrize("state", ["running", "failed"])
def test_pagination_follows_filter_and_sort_without_limiting_totals(state: str) -> None:
    snapshot = _snapshot(state=state)
    context = view_context.fleet_jobs_context(snapshot, FILTERS | {"state": state, "page": "2"})
    assert [row["id"] for row in context["jobs"]] == [f"history{i:03}" for i in range(50, 100)]
    assert (context["page_number"], context["page_count"]) == (2, 3)
    assert (context["first_row"], context["last_row"], context["shown_count"]) == (51, 100, 50)
    assert (context["filtered_count"], context["total_count"]) == (121, 141)
    assert context["summary"]["counts"]["running" if state == "running" else "finished"] == 121
    assert context["summary"]["allocated_cpus"] == (242 if state == "running" else 0)
    assert context["summary"]["allocated_mem_mb"] == (121000 if state == "running" else 0)
    # Sorting uses a separate list and leaves the shared snapshot untouched.
    assert snapshot.jobs[0]["id"] == "history000"
    sort_query = dict(parse_qsl(urlsplit(context["sort_href"]("id")).query))
    assert "page" not in sort_query and "dir" not in sort_query
    assert sort_query == {key: value for key, value in FILTERS.items()
                          if key != "dir"} | {"state": state}


@pytest.mark.parametrize("count,page,first,last", [
    (0, 1, 0, 0), (50, 1, 1, 50), (51, 2, 51, 51), (100, 2, 51, 100),
])
def test_missing_pages_clamp_to_last_available_page(count: int, page: int,
                                                   first: int, last: int) -> None:
    context = view_context.fleet_jobs_context(_snapshot(count), FILTERS | {"page": "999"})
    assert context["page_number"] == context["page_count"] == page
    assert (context["first_row"], context["last_row"]) == (first, last)
    assert context["next_page_href"] is None
    refresh = dict(parse_qsl(urlsplit(context["table_refresh_url"]).query))
    assert refresh.get("page", "1") == str(page)

    warming = view_context.fleet_jobs_context(None, {"page": "999"})
    assert warming["page_number"] == warming["page_count"] == 1
    assert warming["previous_page_href"] is warming["next_page_href"] is None
    assert warming["table_refresh_url"] == "/fleet/jobs/_table"


@pytest.fixture
def fleet_client(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> TestClient:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
    monkeypatch.setenv("VQ_WEB_FLEET", "1")
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    return TestClient(create_app())


def test_page_links_and_background_refresh_use_cached_rows_only(
    fleet_client: TestClient, monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = fleet_client
    client.app.state.fleet_cache.set_snapshot(_snapshot())

    def no_network(*_args, **_kwargs):
        pytest.fail("paging cached jobs must not contact an execution host")

    monkeypatch.setattr(transport, "run_remote_vq", no_network)
    url = "/fleet/jobs?" + view_context.fleet_query_string(FILTERS)
    seen: list[str] = []
    for page, count in [(1, 50), (2, 50), (3, 21)]:
        response = client.get(url)
        assert response.status_code == 200
        assert f"Page {page} of 3" in response.text
        assert "rows per page" in response.text
        ids = re.findall(r"/fleet/jobs/(history\d{3})\?host=", response.text)
        assert len(ids) == count
        seen.extend(ids)
        assert "excluded" not in response.text
        assert (f"Showing {(page - 1) * 50 + 1}–{len(seen)} of 121 matching jobs (141 total)."
                in response.text)

        refresh_url = unescape(re.search(r'hx-get="([^"]+)"', response.text).group(1))
        fragment = client.get(refresh_url)
        assert fragment.status_code == 200
        assert f"Page {page} of 3" in fragment.text
        assert re.findall(r"/fleet/jobs/(history\d{3})\?host=", fragment.text) == ids
        assert 'name="page"' not in response.text  # Applying filters starts at page 1.
        if page > 1:
            previous = unescape(re.search(r'rel="prev" href="([^"]+)"',
                                         response.text).group(1))
            assert dict(parse_qsl(urlsplit(previous).query)) == (
                FILTERS | ({"page": str(page - 1)} if page > 2 else {})
            )
        if page < 3:
            url = unescape(re.search(r'rel="next" href="([^"]+)"', response.text).group(1))
            assert dict(parse_qsl(urlsplit(url).query)) == FILTERS | {"page": str(page + 1)}
        else:
            assert 'rel="next"' not in response.text
    assert seen == [f"history{i:03}" for i in range(121)]
    # Pagination keeps the existing API contract: all filtered cached rows.
    payload = client.get("/api/v1/fleet/jobs", params=FILTERS | {"page": "2"}).json()
    assert payload["count"] == len(payload["jobs"]) == 121

    client.app.state.fleet_cache.set_snapshot(_snapshot(1))
    refreshed = client.get(refresh_url)
    assert refreshed.status_code == 200
    assert "Showing 1–1 of 1 matching jobs" in refreshed.text
    assert 'rel="prev"' not in refreshed.text and 'rel="next"' not in refreshed.text
    assert 'page=' not in unescape(re.search(r'hx-get="([^"]+)"', refreshed.text).group(1))


@pytest.mark.parametrize("bounds,expected", [
    ({"submitted_from": "2026-10-12"}, {"start", "last", "after", "offset", "naive"}),
    ({"submitted_to": "2026-10-12"}, {"before", "start", "last", "offset", "naive"}),
    ({"submitted_from": "2026-10-12", "submitted_to": "2026-10-12"},
     {"start", "last", "offset", "naive"}),
    ({}, {"before", "start", "last", "after", "offset", "naive", "missing", "bad", "number"}),
])
def test_submission_date_bounds_are_inclusive_in_utc(bounds: dict, expected: set[str]) -> None:
    query = view_context.fleet_query_state(bounds)
    timestamps = {
        "before": "2026-10-12T01:59:59.999999+02:00",
        "start": "2026-10-12T02:00:00+02:00",
        "last": "2026-10-12T23:59:59.999999Z",
        "after": "2026-10-12T20:00:00-04:00",
        "offset": "2026-10-13T01:59:59.999999+02:00",
        "naive": "2026-10-12T12:00:00",
        "missing": None, "bad": "not-a-time", "number": 42,
    }
    assert {jobid for jobid, timestamp in timestamps.items()
            if view_context.fleet_row_matches({"submitted_at": timestamp}, query)} == expected


def test_history_range_filters_page_refresh_last_known_and_api_without_network(
    fleet_client: TestClient, monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _snapshot()
    rows = snapshot.hosts[0].jobs
    # The first 20 matching rows lie outside the selected day, as do the
    # other owner's rows. Date and owner filters must run before pagination.
    for row in rows[:20]:
        row["submitted_at"] = "2026-10-11T23:59:59Z"
    snapshot.hosts.append(fleet_mod.HostSnapshot(
        host="unlisted", overview=HostOverview(host="unlisted"), jobs_error="not reachable",
        last_known_at="2026-10-13T10:00:00Z",
        last_known=[dict(rows[index], id=jobid, queue_host="unlisted", queue_owner="unlisted")
                    for index, jobid in ((0, "carried-old"), (20, "carried-in-range"))],
    ))
    fleet_client.app.state.fleet_cache.set_snapshot(snapshot)

    def no_network(*_args, **_kwargs):
        pytest.fail("history queries must use the cached snapshot only")

    monkeypatch.setattr(transport, "run_remote_vq", no_network)
    filters = {key: value for key, value in FILTERS.items() if key != "host"} | {
        "submitted_from": "2026-10-12", "submitted_to": "2026-10-12",
    }
    params = filters | {"page": "2"}
    page = fleet_client.get("/fleet/jobs", params=params)
    assert page.status_code == 200
    for field in ("submitted_from", "submitted_to"):
        assert f'type="date" name="{field}" value="2026-10-12"' in page.text
    assert "Showing 51–100 of 101 matching jobs (141 total)." in page.text
    assert "carried-in-range" in page.text and "carried-old" not in page.text
    ids = re.findall(r"/fleet/jobs/(history\d{3})\?host=", page.text)
    assert ids == [f"history{i:03}" for i in range(70, 120)]
    refresh_url = unescape(re.search(r'hx-get="([^"]+)"', page.text).group(1))
    assert dict(parse_qsl(urlsplit(refresh_url).query)) == params
    fragment = fleet_client.get(refresh_url)
    assert fragment.status_code == 200
    assert re.findall(r"/fleet/jobs/(history\d{3})\?host=", fragment.text) == ids
    next_url = unescape(re.search(r'rel="next" href="([^"]+)"', fragment.text).group(1))
    assert dict(parse_qsl(urlsplit(next_url).query)) == filters | {"page": "3"}
    next_page = fleet_client.get(next_url)
    assert "Showing 101–101 of 101 matching jobs" in next_page.text
    assert 'history120?host=' in next_page.text
    payload = fleet_client.get("/api/v1/fleet/jobs", params=params).json()
    assert payload["count"] == len(payload["jobs"]) == 101
    assert [job["id"] for group in payload["last_known"] for job in group["jobs"]] == [
        "carried-in-range",
    ]
    context = view_context.fleet_jobs_context(snapshot, params)
    assert context["summary"]["counts"]["finished"] == 101
    sort_query = dict(parse_qsl(urlsplit(context["sort_href"]("submitted")).query))
    assert sort_query["submitted_from"] == sort_query["submitted_to"] == "2026-10-12"
    assert "page" not in sort_query


@pytest.mark.parametrize("params,message", [
    ({"submitted_from": "2026-02-29"}, "submitted_from must be a calendar date"),
    ({"submitted_to": "2026-13-01"}, "submitted_to must be a calendar date"),
    ({"submitted_from": "20261012"}, "submitted_from must be a calendar date"),
    ({"submitted_to": "2026-10-12T00:00:00Z"}, "submitted_to must be a calendar date"),
    ({"submitted_from": "2026-10-13", "submitted_to": "2026-10-12"},
     "submitted_to must be on or after submitted_from"),
])
def test_bad_history_ranges_are_rejected_even_before_the_first_snapshot(
    fleet_client: TestClient, params: dict, message: str,
) -> None:
    with pytest.raises(view_context.FleetQueryError, match=message):
        view_context.fleet_query_state(params)
    for url in ("/fleet/jobs", "/fleet/jobs/_table", "/api/v1/fleet/jobs"):
        response = fleet_client.get(url, params=params)
        assert response.status_code == 400
        assert response.json()["detail"].startswith(message)
