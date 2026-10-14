"""#168: an unlisted host keeps its last-known jobs, visibly not current.

When a host's queue cannot be listed, the console shows what its last
successful listing held, as of that listing, apart from every live count
and total. The rows are carried in the console's memory, keep their
original observation time across further failures, are dropped when the
host lists again or once they are too old, and cost no probe.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from vq import auth, config, paths
from vq.overview import HostOverview
from vq.web import calculations, create_app, readiness, view_context
from vq.web import fleet as fleet_mod

T0 = datetime(2026, 10, 14, 10, 0, 0, tzinfo=UTC)
LOST = "RemoteError: ssh: connect timed out"


def _row(jobid: str, host: str = "host_b", **fields: object) -> dict:
    row: dict[str, object] = {
        "id": jobid, "queue_host": host, "queue_owner": host, "state": "running",
        "effective_state": "running", "scheduler_target": None, "cpus": 8,
        "mem_mb": 16000, "command": ["python", "run.py"],
        "submitted_at": (T0 - timedelta(hours=2)).isoformat(),
        "started_at": (T0 - timedelta(hours=1)).isoformat(),
    }
    row.update(fields)
    return row


def _host(key: str, *, rows: list[dict] | None = None, error: str | None = None,
          admin_down: str | None = None) -> fleet_mod.HostSnapshot:
    overview = HostOverview(host=key, reachable=error is None and admin_down is None,
                            error=error, admin_down=admin_down)
    return fleet_mod.HostSnapshot(host=key, overview=overview, jobs=rows or [],
                                  jobs_error=error)


def _snapshot(at: datetime, *hosts: fleet_mod.HostSnapshot) -> fleet_mod.FleetSnapshot:
    return fleet_mod.FleetSnapshot(gathered_at=at.isoformat(), duration_seconds=1.0,
                                   hosts=list(hosts))


def _host_of(snapshot: fleet_mod.FleetSnapshot, key: str) -> fleet_mod.HostSnapshot:
    return next(h for h in snapshot.hosts if h.host == key)


class TestCarry:
    def test_an_unlisted_host_carries_its_last_listing_apart_from_live_rows(self) -> None:
        live_a = [_row("a0001", "host_a")]
        before = _snapshot(T0, _host("host_a", rows=live_a),
                           _host("host_b", rows=[_row("b0001"), _row("b0002")]))
        now = _snapshot(T0 + timedelta(minutes=5), _host("host_a", rows=live_a),
                        _host("host_b", error=LOST))
        carried = fleet_mod.carry_last_known(before, now)

        b = _host_of(carried, "host_b")
        assert [row["id"] for row in b.last_known] == ["b0001", "b0002"]
        assert b.last_known_at == T0.isoformat()
        assert b.jobs == []
        # No live consumer sees them: counts, totals and readiness are
        # computed from live rows only.
        assert [row["id"] for row in carried.jobs] == ["a0001"]
        summary = calculations.calculation_summary(carried.jobs, now=T0)
        assert (summary["counts"]["running"], summary["allocated_cpus"]) == (1, 8)
        result = readiness.fleet_readiness(carried)
        assert result is not None and result["executions"]["counts"]["running"] == 1

    def test_a_second_failure_keeps_the_original_observation_time(self) -> None:
        first = _snapshot(T0, _host("host_b", rows=[_row("b0001")]))
        second = fleet_mod.carry_last_known(
            first, _snapshot(T0 + timedelta(minutes=5), _host("host_b", error=LOST)))
        third = fleet_mod.carry_last_known(
            second, _snapshot(T0 + timedelta(minutes=10), _host("host_b", error=LOST)))
        b = _host_of(third, "host_b")
        assert ([row["id"] for row in b.last_known], b.last_known_at) == (
            ["b0001"], T0.isoformat())

    def test_listing_again_replaces_the_last_known_rows(self) -> None:
        lost = fleet_mod.carry_last_known(
            _snapshot(T0, _host("host_b", rows=[_row("b0001")])),
            _snapshot(T0 + timedelta(minutes=5), _host("host_b", error=LOST)))
        back = fleet_mod.carry_last_known(
            lost, _snapshot(T0 + timedelta(minutes=10), _host("host_b", rows=[])))
        b = _host_of(back, "host_b")
        assert (b.last_known, b.last_known_at, b.jobs) == ([], None, [])

    def test_rows_older_than_the_limit_are_dropped(self) -> None:
        old = _snapshot(T0, _host("host_b", rows=[_row("b0001")]))
        kept = fleet_mod.carry_last_known(
            old, _snapshot(T0 + fleet_mod.LAST_KNOWN_MAX_AGE, _host("host_b", error=LOST)))
        assert _host_of(kept, "host_b").last_known
        dropped = fleet_mod.carry_last_known(
            kept, _snapshot(T0 + fleet_mod.LAST_KNOWN_MAX_AGE + timedelta(seconds=1),
                            _host("host_b", error=LOST)))
        assert _host_of(dropped, "host_b").last_known == []

    def test_a_partial_listing_is_not_replaced_by_old_rows(self) -> None:
        # Some rows were invalid, the rest listed: the host is listed.
        partial = _host("host_b", rows=[_row("b0002")])
        partial.jobs_error = "2 invalid queue rows"
        carried = fleet_mod.carry_last_known(
            _snapshot(T0, _host("host_b", rows=[_row("b0001")])),
            _snapshot(T0 + timedelta(minutes=5), partial))
        assert _host_of(carried, "host_b").last_known == []

    def test_an_administratively_down_host_carries_too(self) -> None:
        carried = fleet_mod.carry_last_known(
            _snapshot(T0, _host("host_b", rows=[_row("b0001")])),
            _snapshot(T0 + timedelta(minutes=5),
                      _host("host_b", admin_down="down since 2026-10-14: maintenance")))
        assert [row["id"] for row in _host_of(carried, "host_b").last_known] == ["b0001"]

    def test_nothing_is_invented_for_a_host_never_listed(self) -> None:
        carried = fleet_mod.carry_last_known(
            _snapshot(T0, _host("host_a", rows=[_row("a0001", "host_a")])),
            _snapshot(T0 + timedelta(minutes=5), _host("host_b", error=LOST)))
        assert _host_of(carried, "host_b").last_known == []
        assert fleet_mod.carry_last_known(None, carried) is carried


def test_the_cache_carries_rows_between_its_own_sweeps(monkeypatch: pytest.MonkeyPatch) -> None:
    sweeps = iter([
        _snapshot(T0, _host("host_b", rows=[_row("b0001")])),
        _snapshot(T0 + timedelta(minutes=1), _host("host_b", error=LOST)),
    ])
    monkeypatch.setattr(fleet_mod, "gather_fleet_snapshot", lambda: next(sweeps))
    cache = fleet_mod.FleetCache(interval_seconds=30)
    cache.refresh()
    snapshot = cache.refresh()
    assert snapshot is not None
    b = _host_of(snapshot, "host_b")
    assert ([row["id"] for row in b.last_known], b.last_known_at) == (["b0001"], T0.isoformat())
    assert snapshot.to_json()["hosts"][0]["last_known_count"] == 1  # type: ignore[index]


def test_last_known_rows_render_as_of_their_listing() -> None:
    observed = datetime.now(UTC) - timedelta(hours=3)
    snapshot = _snapshot(
        datetime.now(UTC),
        _host("host_a", rows=[_row("a0001", "host_a", program="orca")]),
        _host("host_b", error=LOST),
    )
    b = _host_of(snapshot, "host_b")
    b.last_known = [
        _row("b0001", program="vibe-qc", started_at=(observed - timedelta(hours=1)).isoformat()),
        _row("b0002", program="orca", state="completed", effective_state="completed",
             finished_at=(observed - timedelta(minutes=30)).isoformat(),
             started_at=(observed - timedelta(hours=1)).isoformat()),
    ]
    b.last_known_at = observed.isoformat()

    context = view_context.fleet_jobs_context(snapshot, {})
    (group,) = context["last_known"]
    assert group["host"] == "host_b" and group["reason"] == LOST
    running = next(job for job in group["jobs"] if job["id"] == "b0001")
    # One hour run as of the listing, not four hours by the wall clock.
    assert running["calc"]["runtime"]["seconds"] == 3600
    assert context["last_known_count"] == 2
    # Live totals and filters: the carried jobs are filterable but uncounted.
    assert context["summary"]["counts"]["running"] == 1
    assert "host_b" in context["host_options"]
    orca_only = view_context.fleet_jobs_context(snapshot, {"program": "orca"})
    assert [job["id"] for g in orca_only["last_known"] for job in g["jobs"]] == ["b0002"]


@pytest.fixture
def fleet_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
    monkeypatch.setenv("VQ_WEB_FLEET", "1")
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    return TestClient(create_app())


def test_page_and_api_keep_last_known_apart(fleet_client: TestClient) -> None:
    now = datetime.now(UTC)
    observed = now - timedelta(minutes=7)
    snapshot = _snapshot(now, _host("host_a", rows=[_row("a0001", "host_a")]),
                         _host("host_b", error=LOST))
    b = _host_of(snapshot, "host_b")
    b.last_known, b.last_known_at = [_row("b0001", cpus=32)], observed.isoformat()
    fleet_client.app.state.fleet_cache.set_snapshot(snapshot)

    html = fleet_client.get("/fleet/jobs/_table").text
    assert "host_b was not listed in this snapshot." in html
    assert "the\n  1 it listed at" in html and 'href="#last-known"' in html
    assert "Last known: 1 job on hosts not listed now" in html
    assert 'class="last-known-row"' in html and "b0001" in html
    assert "0:07:" in html  # listed about seven minutes ago
    # The 32 carried cpus are not allocated in the live totals.
    assert '<span class="tile-value">8</span><span class="tile-label">cpus allocated' in html

    payload = fleet_client.get("/api/v1/fleet/jobs").json()
    assert (payload["count"], [row["id"] for row in payload["jobs"]]) == (1, ["a0001"])
    assert payload["last_known"] == [{
        "host": "host_b", "observed_at": observed.isoformat(), "reason": LOST,
        "jobs": [b.last_known[0]],
    }]
    assert fleet_client.get("/api/v1/fleet/jobs?host=host_a").json()["last_known"] == []
