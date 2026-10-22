"""#168: the observed peak of a running job's memory.

The watchdog keeps the highest RSS its samples have shown for each job and
writes it, with the time tracking began, into every sample. A daemon
restart resumes it from the same attempt's last sample and never from an
earlier attempt's. `vq top --json` carries it to the fleet console, whose
jobs table shows it as a sampled peak, never summed and never zero when
unknown.
"""
from __future__ import annotations

import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from vq import cgroup, top, watchdog
from vq.overview import HostOverview
from vq.spec import JobSpec, JobState
from vq.watchdog import Watchdog
from vq.web import calculations, view_context
from vq.web import fleet as fleet_mod

JOB = "peakjob00001"


@pytest.fixture(autouse=True)
def _no_ambient_cgroup(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cgroup, "cgroup_path_for_pid", lambda pid: None)
    monkeypatch.setattr(cgroup, "cgroup_path_for_scope", lambda unit, **kwargs: None)


def _spec(tmp_path: Path, started: datetime) -> JobSpec:
    (tmp_path / "ws").mkdir(exist_ok=True)
    return JobSpec(id=JOB, command=["true"], cwd=str(tmp_path / "ws"), cpus=1,
                   state=JobState.RUNNING, started_at=started.isoformat())


def _samples(tmp_path: Path) -> list[dict]:
    lines = (tmp_path / "ws/_vq/samples.jsonl").read_text().splitlines()
    return [json.loads(line) for line in lines]


def _sample(wd: Watchdog, spec: JobSpec, monkeypatch: pytest.MonkeyPatch, rss: int | None,
            *, pgid: int = 4242) -> None:
    monkeypatch.setattr(watchdog, "read_rss_mb_pgid", lambda _pgid: rss)
    monkeypatch.setattr(watchdog, "read_cputime_seconds_pgid", lambda _pgid: 1.0)
    wd.evaluate(JOB, pid=pgid, pgid=pgid, spec=spec)


class TestWatchdog:
    def test_the_peak_only_rises_and_records_when_it_began(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        spec = _spec(tmp_path, datetime.now(UTC) - timedelta(minutes=1))
        wd = Watchdog(interval_seconds=0.0)
        for rss in (100, 300, None, 200):
            _sample(wd, spec, monkeypatch, rss)
        samples = _samples(tmp_path)
        assert [s["rss_mb"] for s in samples] == [100, 300, None, 200]
        assert [s["rss_peak_mb"] for s in samples] == [100.0, 300.0, 300.0, 300.0]
        assert {s["rss_peak_since"] for s in samples} == {samples[0]["ts"]}

    def test_a_restarted_daemon_resumes_the_same_attempts_peak(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        spec = _spec(tmp_path, datetime.now(UTC) - timedelta(minutes=1))
        first = Watchdog(interval_seconds=0.0)
        _sample(first, spec, monkeypatch, 900)
        since = _samples(tmp_path)[0]["rss_peak_since"]

        restarted = Watchdog(interval_seconds=0.0)
        _sample(restarted, spec, monkeypatch, 400)
        last = _samples(tmp_path)[-1]
        assert (last["rss_mb"], last["rss_peak_mb"], last["rss_peak_since"]) == (400, 900.0, since)

    @pytest.mark.parametrize("change", ["another process group", "a later attempt"])
    def test_an_earlier_attempts_peak_is_not_inherited(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str,
    ) -> None:
        spec = _spec(tmp_path, datetime.now(UTC) - timedelta(minutes=1))
        _sample(Watchdog(interval_seconds=0.0), spec, monkeypatch, 900)
        pgid = 4242
        if change == "another process group":
            pgid = 5151
        else:  # a retry reuses the workspace; it started after that sample
            spec = _spec(tmp_path, datetime.now(UTC) + timedelta(seconds=1))
        _sample(Watchdog(interval_seconds=0.0), spec, monkeypatch, 400, pgid=pgid)
        last = _samples(tmp_path)[-1]
        assert last["rss_peak_mb"] == 400.0
        assert last["rss_peak_since"] == last["ts"]

    def test_the_last_sample_is_read_from_the_tail_only(self, tmp_path: Path) -> None:
        vq_dir = tmp_path / "_vq"
        vq_dir.mkdir()
        filler = json.dumps({"pad": "x" * 1000}) + "\n"
        (vq_dir / "samples.jsonl").write_text(
            filler * 200 + json.dumps({"rss_peak_mb": 7}) + "\n")
        assert watchdog.last_sample(tmp_path) == {"rss_peak_mb": 7}
        (vq_dir / "samples.jsonl").write_text("")
        assert watchdog.last_sample(tmp_path) is None


def test_vq_top_json_carries_the_peak(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    spec = _spec(tmp_path, now - timedelta(minutes=5))
    (tmp_path / "ws/_vq").mkdir(exist_ok=True)
    (tmp_path / "ws/_vq/samples.jsonl").write_text(json.dumps({
        "ts": now.isoformat(), "rss_mb": 120, "cpu_percent": 90.0,
        "rss_peak_mb": 512, "rss_peak_since": (now - timedelta(minutes=4)).isoformat(),
    }) + "\n")
    (row,) = top.gather_top_rows([spec], now=now)
    payload = top.top_row_payload(row)
    assert (payload["rss_mb"], payload["rss_peak_mb"]) == (120.0, 512.0)
    assert payload["rss_peak_since"] == (now - timedelta(minutes=4)).isoformat()
    assert "PEAK" in top.format_top_table([row]) and "512M" in top.format_top_table([row])


def _row(jobid: str, peak: float | None, since: str | None, rss: float = 100.0) -> dict:
    now = datetime.now(UTC)
    return {
        "id": jobid, "queue_host": "host_a", "queue_owner": "host_a", "state": "running",
        "effective_state": "running", "scheduler_target": None, "cpus": 2, "mem_mb": 4096,
        "submitted_at": (now - timedelta(hours=1)).isoformat(),
        "started_at": (now - timedelta(minutes=30)).isoformat(), "command": ["run"],
        "resource_observation": {
            "status": "observed", "source": "watchdog", "cpu_percent": 150.0,
            "rss_mb": rss, "rss_peak_mb": peak, "rss_peak_since": since,
            "sample_at": (now - timedelta(seconds=3)).isoformat(),
            "collected_at": now.isoformat(),
        },
    }


class TestConsole:
    def test_the_sweep_keeps_peak_and_its_start_together(self) -> None:
        since = "2026-10-14T10:00:00+00:00"
        usage = fleet_mod._UsageObservation(
            by_job={"a": {"jobid": "a", "cpu_percent": 1.0, "rss_mb": 50.0,
                          "rss_peak_mb": 80.0, "rss_peak_since": since},
                    "b": {"jobid": "b", "cpu_percent": 1.0, "rss_mb": 50.0,
                          "rss_peak_mb": 80.0}},
            collected_at=datetime.now(UTC).isoformat())
        seen = {jobid: fleet_mod._resource_observation(
            {"id": jobid, "state": "running", "scheduler_target": None}, usage)
            for jobid in ("a", "b")}
        assert (seen["a"]["rss_peak_mb"], seen["a"]["rss_peak_since"]) == (80.0, since)
        # A peak without the time its tracking began is not shown.
        assert (seen["b"]["rss_peak_mb"], seen["b"]["rss_peak_since"]) == (None, None)

    def test_an_older_host_without_a_peak_reads_unknown_not_zero(self) -> None:
        view = calculations.observation_view(_row("old", None, None), now=datetime.now(UTC))
        assert view["rss_peak_mb"] is None and view["rss_mb"] == 100.0

    def test_the_table_shows_and_sorts_the_peak_but_never_sums_it(self) -> None:
        since = (datetime.now(UTC) - timedelta(minutes=20)).isoformat()
        rows = [_row("small", 300.0, since), _row("big", 9000.0, since),
                _row("unknown", None, None)]
        snapshot = fleet_mod.FleetSnapshot(
            gathered_at=datetime.now(UTC).isoformat(), duration_seconds=1.0,
            hosts=[fleet_mod.HostSnapshot(host="host_a", overview=HostOverview(host="host_a"),
                                          jobs=rows)])
        context = view_context.fleet_jobs_context(snapshot, {"sort": "rss_peak"})
        assert [job["id"] for job in context["jobs"]] == ["big", "small", "unknown"]
        assert context["jobs"][0]["calc"]["observation"]["rss_peak_mb"] == 9000.0
        assert "rss_peak" not in json.dumps(context["summary"])
        assert context["summary"]["measured_rss_mb"] == 300.0

    def test_the_rendered_table_has_the_column(self, tmp_path: Path,
                                               monkeypatch: pytest.MonkeyPatch) -> None:
        from fastapi.testclient import TestClient

        from vq import auth, config, paths
        from vq.web import create_app

        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
        monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
        monkeypatch.setenv("VQ_WEB_FLEET", "1")
        paths.queue_dir().mkdir(parents=True, exist_ok=True)
        client = TestClient(create_app())
        since = (datetime.now(UTC) - timedelta(minutes=20)).isoformat()
        client.app.state.fleet_cache.set_snapshot(fleet_mod.FleetSnapshot(
            gathered_at=datetime.now(UTC).isoformat(), duration_seconds=1.0,
            hosts=[fleet_mod.HostSnapshot(
                host="host_a", overview=HostOverview(host="host_a"),
                jobs=[_row("big", 9000.0, since), _row("unknown", None, None)])]))
        html = client.get("/fleet/jobs/_table").text
        assert ">peak MB" in html and "sampled peak since" in html
        assert ">9000<" in html.replace(" ", "")


def test_the_peak_survives_a_real_wait(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Sampling at the watchdog's own interval: a later, lower reading after
    the interval elapsed keeps the earlier peak."""
    spec = _spec(tmp_path, datetime.now(UTC) - timedelta(minutes=1))
    wd = Watchdog(interval_seconds=0.05)
    _sample(wd, spec, monkeypatch, 700)
    time.sleep(0.06)
    _sample(wd, spec, monkeypatch, 10)
    assert [s["rss_peak_mb"] for s in _samples(tmp_path)] == [700.0, 700.0]
