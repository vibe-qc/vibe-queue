"""#168: fleet trend plots and a per-host load view on /fleet/jobs.

The console keeps a bounded, in-memory history of each sweep's totals and
draws them as server-side SVG sparklines; a partial sweep is marked, and a
missing value breaks the line instead of reading as zero. The per-host
table puts each host's running work against its own budget and never shows
an unlisted host or an unsampled scheduler lane as idle.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from vq.overview import HostOverview
from vq.web import calculations
from vq.web import fleet as fleet_mod

NOW = datetime(2026, 10, 14, 12, 0, 0, tzinfo=UTC)


def _row(jobid: str, host: str = "host_a", *, cpus: int = 4, rss: float | None = 2048.0,
         cpu_percent: float = 350.0, **fields: object) -> dict:
    row: dict[str, object] = {
        "id": jobid, "queue_host": host, "queue_owner": host, "state": "running",
        "effective_state": "running", "scheduler_target": None, "cpus": cpus, "mem_mb": 8192,
        "submitted_at": (NOW - timedelta(hours=1)).isoformat(),
        "started_at": (NOW - timedelta(minutes=30)).isoformat(), "command": ["run"],
        "resource_observation": {
            "status": "observed" if rss is not None else "no_sample", "source": "watchdog",
            "cpu_percent": cpu_percent if rss is not None else None, "rss_mb": rss,
            "sample_at": (NOW - timedelta(seconds=5)).isoformat(),
            "collected_at": NOW.isoformat(),
        },
    }
    row.update(fields)
    return row


def _host(key: str, rows: list[dict], *, error: str | None = None, max_cpus: int | None = 32,
          lane: bool = False) -> fleet_mod.HostSnapshot:
    overview = HostOverview(host=key, reachable=error is None, error=error, max_cpus=max_cpus,
                            mem_total_mb=65536, mem_available_mb=40000)
    return fleet_mod.HostSnapshot(host=key, overview=overview, jobs=rows, jobs_error=error,
                                  cluster=key if lane else None)


def _snapshot(*hosts: fleet_mod.HostSnapshot, at: datetime = NOW) -> fleet_mod.FleetSnapshot:
    return fleet_mod.FleetSnapshot(gathered_at=at.isoformat(), duration_seconds=1.0,
                                   hosts=list(hosts))


class TestTrendPoint:
    def test_a_fully_measured_sweep(self) -> None:
        point = calculations.trend_point(_snapshot(_host("host_a", [_row("a1"), _row("a2")])))
        assert point == {"at": NOW.isoformat(), "running": 2, "allocated_cpus": 8,
                         "measured_cores": 7.0, "measured_rss_mb": 4096.0, "partial": False}

    @pytest.mark.parametrize("gap", ["an unmeasured job", "an unlisted host"])
    def test_a_sweep_that_misses_something_is_partial(self, gap: str) -> None:
        hosts = [_host("host_a", [_row("a1")])]
        if gap == "an unmeasured job":
            hosts[0].jobs.append(_row("a2", rss=None))
        else:
            hosts.append(_host("gone", [], error="ssh: connect timed out"))
        assert calculations.trend_point(_snapshot(*hosts))["partial"] is True


class TestCache:
    def test_each_sweep_is_recorded_and_the_history_is_bounded(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(fleet_mod, "TREND_POINTS", 3)
        sweeps = iter(_snapshot(_host("host_a", [_row(f"j{i}") for i in range(n)]),
                                at=NOW + timedelta(seconds=30 * n)) for n in range(5))
        monkeypatch.setattr(fleet_mod, "gather_fleet_snapshot", lambda: next(sweeps))
        cache = fleet_mod.FleetCache(interval_seconds=30)
        for _ in range(5):
            cache.refresh()
        assert [point["running"] for point in cache.trend()] == [2, 3, 4]


class TestSparkline:
    def test_a_missing_value_breaks_the_line(self) -> None:
        svg = str(calculations.sparkline_svg([("trend-used", [1.0, 2.0, None, 3.0, 4.0])],
                                             title="t"))
        assert svg.count("<polyline") == 2

    def test_a_lone_point_is_a_dot_and_no_data_draws_nothing(self) -> None:
        lone = str(calculations.sparkline_svg([("trend-used", [None, 5.0, None])], title="t"))
        assert "<polyline" not in lone and lone.count("<circle") == 1
        empty = str(calculations.sparkline_svg([("trend-used", [None, None])], title="t"))
        assert "<polyline" not in empty and "<circle" not in empty

    def test_partial_points_are_marked_and_zero_sits_at_the_bottom(self) -> None:
        svg = str(calculations.sparkline_svg([("trend-used", [0.0, 0.0])],
                                             marks=[False, True], title="t", height=40))
        assert 'points="2.0,38.0 198.0,38.0"' in svg
        assert svg.count('class="trend-partial"') == 1

    def test_the_title_is_escaped(self) -> None:
        svg = str(calculations.sparkline_svg([("trend-used", [1.0])], title="<b>x</b>"))
        assert "<b>" not in svg and "&lt;b&gt;" in svg


def test_trend_view_reports_latest_values() -> None:
    points = [
        {"at": NOW.isoformat(), "running": 1, "allocated_cpus": 4, "measured_cores": 3.5,
         "measured_rss_mb": 1024.0, "partial": False},
        {"at": (NOW + timedelta(seconds=30)).isoformat(), "running": 2, "allocated_cpus": 8,
         "measured_cores": None, "measured_rss_mb": None, "partial": True},
    ]
    view = calculations.trend_view(points)
    assert view is not None and calculations.trend_view([]) is None
    running, cpu, rss = view["charts"]  # type: ignore[misc]
    assert (running["latest"], cpu["latest"], cpu["latest_allocated"]) == (2.0, None, 8.0)
    assert rss["latest"] is None and view["latest_partial"] is True
    assert view["points"] == 2 and view["since"] == NOW.isoformat()


class TestHostLoad:
    def test_each_host_against_its_budget_and_never_idle_when_unknown(self) -> None:
        snapshot = _snapshot(
            _host("host_a", [_row("a1", cpus=8), _row("a2", rss=None)]),
            _host("cluster_a", [_row("c1", host="cluster_a", scheduler_target="cluster_a",
                                     rss=None)], max_cpus=None, lane=True),
            _host("gone", [], error="ssh: connect timed out"),
        )
        rows = {row["host"]: row for row in calculations.host_load_view(snapshot, now=NOW)}
        a = rows["host_a"]
        assert (a["running"], a["allocated_cpus"], a["max_cpus"]) == (2, 12, 32)
        assert (a["measured_cores"], a["measured_jobs"], a["partial"]) == (3.5, 1, True)
        assert (rows["cluster_a"]["lane"], rows["cluster_a"]["measured_cores"]) == (True, None)
        assert rows["gone"]["listed"] is False
        assert "connect timed out" in str(rows["gone"]["reason"])
        only = calculations.host_load_view(snapshot, now=NOW, host="gone")
        assert [row["host"] for row in only] == ["gone"]


def test_the_jobs_page_renders_trends_and_host_load(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fastapi.testclient import TestClient

    from vq import auth, config, paths
    from vq.web import create_app

    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
    monkeypatch.setenv("VQ_WEB_FLEET", "1")
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    client = TestClient(create_app())
    cache = client.app.state.fleet_cache
    now = datetime.now(UTC)
    for minutes in (2, 1, 0):
        at = now - timedelta(minutes=minutes)
        rows = [_row("a1")]
        rows[0]["resource_observation"]["sample_at"] = (at - timedelta(seconds=3)).isoformat()
        cache.record_trend(_snapshot(_host("host_a", rows), _host("gone", [], error="ssh down"),
                                     at=at))
    cache.set_snapshot(_snapshot(_host("host_a", [_row("a1")]),
                                 _host("gone", [], error="ssh down"), at=now))

    html = client.get("/fleet/jobs/_table").text
    assert html.count('<svg class="sparkline"') == 3
    assert 'class="trend-partial"' in html  # the unlisted host made every sweep partial
    assert "Trends cover the last 3 sweeps of this console" in html
    assert "not listed in this snapshot: unknown, not idle" in html
    assert "cpus allocated / budget" in html and "4 / 32" in html
