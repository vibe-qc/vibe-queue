"""Tests for the fleet-mode web surface (vq.web fleet routes + vq.web.fleet).

Fleet mode (docs/fleet_dashboard_design.md M0) is gated by VQ_WEB_FLEET:
off = the app is byte-identical to the single-host dashboard; on = the
/fleet pages + /api/v1/fleet JSON exist, served from a cached
FleetSnapshot. Tests inject snapshots directly into the cache — the
background SSH poller only starts under the lifespan hook, which
TestClient never triggers here.
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from vq import auth, capacity, config, paths
from vq.overview import HostOverview
from vq.spec import JobSpec, JobState
from vq.web import create_app
from vq.web import fleet as fleet_mod


@pytest.fixture
def web_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
    monkeypatch.delenv("VQ_WEB_FLEET", raising=False)
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


@pytest.fixture
def fleet_client(
    web_state: Path, monkeypatch: pytest.MonkeyPatch
) -> TestClient:
    monkeypatch.setenv("VQ_WEB_FLEET", "1")
    return TestClient(create_app())


def _write_spec(jobid: str, **overrides) -> JobSpec:
    workspace = paths.jobs_dir() / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    base = {
        "id": jobid,
        "command": ["python", "run.py"],
        "cwd": str(workspace),
        "cpus": 1,
    }
    base.update(overrides)
    spec = JobSpec(**base)
    spec.write(paths.spec_path(jobid))
    return spec


def _row(jobid: str, host: str, **overrides) -> dict:
    row = {
        "id": jobid,
        "state": "running",
        "queue_host": host,
        "scheduler_state": None,
        "cpus": 4,
        "mem_mb": 8000,
        "submitted_at": "2026-07-25T10:00:00+00:00",
        "submitter": "test_user",
        "job_name": None,
        "tags": [],
        "command": ["python", "run.py"],
        "failure_reason": None,
        "terminal_diagnosis": None,
        "queue_handle": {"job_id": jobid, "host": host},
    }
    row.update(overrides)
    return row


def _snapshot(*hosts: fleet_mod.HostSnapshot) -> fleet_mod.FleetSnapshot:
    return fleet_mod.FleetSnapshot(
        gathered_at="2026-07-25T12:00:00+00:00",
        duration_seconds=1.5,
        hosts=list(hosts),
    )


class TestFleetGating:
    def test_fleet_routes_absent_by_default(self, web_state: Path) -> None:
        client = TestClient(create_app())
        assert client.get("/fleet").status_code == 404
        assert client.get("/fleet/jobs").status_code == 404
        assert client.get("/api/v1/fleet").status_code == 404
        # And no fleet nav in the single-host pages.
        assert "fleet-nav" not in client.get("/queue").text

    def test_single_host_routes_survive_fleet_mode(
        self, fleet_client: TestClient
    ) -> None:
        assert fleet_client.get("/queue").status_code == 200
        assert fleet_client.get("/health/live").status_code == 200


class TestFleetPages:
    def test_warming_up_before_first_snapshot(
        self, fleet_client: TestClient
    ) -> None:
        r = fleet_client.get("/fleet")
        assert r.status_code == 200
        assert "Gathering the first fleet snapshot" in r.text
        api = fleet_client.get("/api/v1/fleet").json()
        assert api["warming_up"] is True
        assert api["hosts"] == []

    def test_host_grid_renders_cards(self, fleet_client: TestClient) -> None:
        cache = fleet_client.app.state.fleet_cache
        ok_host = fleet_mod.HostSnapshot(
            host="host_a",
            overview=HostOverview(
                host="host_a",
                vq_version="0.15.57",
                queue_counts={"running": 2, "pending": 1},
                running_cpus=8,
                max_cpus=16,
            ),
            jobs=[_row("marsjob00001", "host_a")],
        )
        bad_host = fleet_mod.HostSnapshot(
            host="host_d",
            overview=HostOverview(
                host="host_d", reachable=False, admin_down="wedged"
            ),
        )
        err_host = fleet_mod.HostSnapshot(
            host="host_e",
            overview=HostOverview(
                host="host_e", reachable=False, error="ssh: timeout"
            ),
        )
        cache.set_snapshot(_snapshot(ok_host, bad_host, err_host))

        r = fleet_client.get("/fleet")
        assert r.status_code == 200
        assert "host_a" in r.text
        assert "0.15.57" in r.text
        assert "admin down" in r.text
        assert "unreachable" in r.text
        assert "ssh: timeout" in r.text

    def test_fleet_api_shape(self, fleet_client: TestClient) -> None:
        cache = fleet_client.app.state.fleet_cache
        cache.set_snapshot(
            _snapshot(
                fleet_mod.HostSnapshot(
                    host="host_a",
                    overview=HostOverview(host="host_a", vq_version="0.15.57"),
                    jobs=[_row("marsjob00001", "host_a")],
                )
            )
        )
        payload = fleet_client.get("/api/v1/fleet").json()
        assert payload["gathered_at"] == "2026-07-25T12:00:00+00:00"
        assert payload["hosts"][0]["host"] == "host_a"
        assert payload["hosts"][0]["job_count"] == 1
        assert payload["hosts"][0]["overview"]["vq_version"] == "0.15.57"


class TestFleetJobsTable:
    @pytest.fixture
    def seeded(self, fleet_client: TestClient) -> TestClient:
        cache = fleet_client.app.state.fleet_cache
        cache.set_snapshot(
            _snapshot(
                fleet_mod.HostSnapshot(
                    host="host_a",
                    overview=HostOverview(host="host_a"),
                    jobs=[
                        _row("marsrun00001", "host_a", state="running"),
                        _row(
                            "marsfail0001",
                            "host_a",
                            state="failed",
                            submitter="alice",
                            tags=["pr-request"],
                        ),
                    ],
                ),
                fleet_mod.HostSnapshot(
                    host="host_f",
                    overview=HostOverview(host="host_f"),
                    jobs=[
                        _row(
                            "twinpend0001",
                            "host_f",
                            state="pending",
                            scheduler_state="queued",
                            job_name="rp207-probe",
                        )
                    ],
                ),
            )
        )
        return fleet_client

    def test_all_jobs_render(self, seeded: TestClient) -> None:
        r = seeded.get("/fleet/jobs")
        assert r.status_code == 200
        for jid in ("marsrun00001", "marsfail0001", "twinpend0001"):
            assert jid in r.text

    def test_filter_by_host(self, seeded: TestClient) -> None:
        r = seeded.get("/fleet/jobs", params={"host": "host_f"})
        assert "twinpend0001" in r.text
        assert "marsrun00001" not in r.text

    def test_filter_by_state(self, seeded: TestClient) -> None:
        r = seeded.get("/fleet/jobs", params={"state": "failed"})
        assert "marsfail0001" in r.text
        assert "marsrun00001" not in r.text

    def test_filter_by_tag_and_submitter(self, seeded: TestClient) -> None:
        r = seeded.get("/fleet/jobs", params={"tag": "pr-request"})
        assert "marsfail0001" in r.text
        assert "twinpend0001" not in r.text
        r = seeded.get("/fleet/jobs", params={"submitter": "alice"})
        assert "marsfail0001" in r.text
        assert "marsrun00001" not in r.text

    def test_free_text_search(self, seeded: TestClient) -> None:
        r = seeded.get("/fleet/jobs", params={"q": "rp207"})
        assert "twinpend0001" in r.text
        assert "marsrun00001" not in r.text

    def test_jobs_api_filters(self, seeded: TestClient) -> None:
        payload = seeded.get(
            "/api/v1/fleet/jobs", params={"host": "host_a"}
        ).json()
        assert payload["count"] == 2
        ids = {row["id"] for row in payload["jobs"]}
        assert ids == {"marsrun00001", "marsfail0001"}


class TestGatherLocal:
    def test_local_snapshot_lists_local_jobs(self, web_state: Path) -> None:
        _write_spec("localrun0001", state=JobState.RUNNING, cpus=2)
        _write_spec("localdone001", state=JobState.COMPLETED)
        snapshot = fleet_mod.gather_fleet_snapshot()
        assert [h.host for h in snapshot.hosts] == ["localhost"]
        local = snapshot.hosts[0]
        assert local.jobs_error is None
        ids = {row["id"] for row in local.jobs}
        assert ids == {"localrun0001", "localdone001"}
        # Rows are CLI-queue-row shaped with the display host injected.
        for row in local.jobs:
            assert row["queue_host"] == "localhost"
            assert row["queue_handle"]["host"] == "localhost"
        assert local.overview.queue_counts.get("running") == 1

    def test_local_rows_match_remote_capacity_classification(
        self, web_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_spec("localpend001", state=JobState.PENDING, cpus=9)
        monkeypatch.setattr(
            capacity,
            "read_daemon_capacity",
            lambda **_kwargs: capacity.DaemonCapacity.model_validate(
                {
                    "max_cpus": 8,
                    "max_mem_mb": 4_000,
                    "written_at": "2026-08-20T12:00:00+00:00",
                }
            ),
        )

        jobs, err = fleet_mod._gather_host_jobs(
            "localhost",
            SimpleNamespace(hosts={}, host=lambda name: None),
            multi_user=False,
            scheduler_host_names=set(),
        )

        assert err is None
        assert jobs[0]["pending_over_capacity"] is True
        assert jobs[0]["configured_capacity_overages"] == [
            {
                "resource": "cpus",
                "requested": 9,
                "limit": 8,
                "uses_default": False,
            }
        ]

    def test_local_scheduler_row_matches_queue_state_projection(
        self, web_state: Path
    ) -> None:
        _write_spec(
            "pollfailed01",
            state=JobState.RUNNING,
            scheduler_target="host_c",
            scheduler_state="poll_failed",
        )

        row = fleet_mod._job_row(
            JobSpec.read(paths.spec_path("pollfailed01")),
            "host_c",
        )

        assert row["state"] == "running"
        assert row["effective_state"] == "poll_failed"
        assert row["scheduler_running_confirmed"] is False

    def test_unsafe_scheduler_target_is_diagnostic_not_actionable(
        self, web_state: Path
    ) -> None:
        raw_target = "host_f\nFORGED host\x1b[31m" + ("x" * 150)
        spec = _write_spec(
            "unsafetarget",
            state=JobState.RUNNING,
            scheduler_target=raw_target,
            scheduler_state="running",
        )

        row = fleet_mod._job_row(spec, "driver")

        assert row["scheduler_target"] == raw_target
        assert row["effective_state"] == "scheduler_unknown"
        assert row["scheduler_running_confirmed"] is False
        assert row["queue_handle"]["host"] == "driver"

    def test_malformed_empty_target_falls_back_to_queried_host(self) -> None:
        rows = [
            {
                "id": "emptytarget",
                "command": ["true"],
                "cwd": "/tmp/emptytarget",
                "cpus": 1,
                "state": "running",
                "scheduler_target": "",
                "scheduler_state": ["running"],
                "queue_handle": {
                    "job_id": "different-job",
                    "host": "untrusted-host",
                },
            }
        ]

        normalized = fleet_mod._normalize_rows(rows, "driver")

        assert normalized[0]["queue_host"] == "driver"
        assert normalized[0]["queue_handle"]["host"] == "driver"
        assert normalized[0]["queue_handle"]["job_id"] == "emptytarget"
        assert normalized[0]["effective_state"] == "scheduler_unknown"
        assert normalized[0]["scheduler_running_confirmed"] is False

    def test_malformed_unsafe_id_has_no_actionable_handle(self) -> None:
        rows = [
            {
                "id": "../../escape",
                "state": "running",
                "scheduler_target": "host_f",
                "scheduler_state": ["running"],
                "queue_handle": {
                    "job_id": "different-job",
                    "host": "untrusted-host",
                },
            }
        ]

        normalized = fleet_mod._normalize_rows(rows, "driver")

        assert normalized[0]["queue_host"] == "driver"
        assert normalized[0]["queue_handle"] is None

    def test_local_listing_excludes_scheduler_owned_specs(
        self, web_state: Path
    ) -> None:
        """A driver's own listing must not double-count specs owned by a
        configured scheduler host (they render under that host)."""
        _write_spec("localown0001", state=JobState.RUNNING)
        _write_spec(
            "twinowned001", state=JobState.PENDING, scheduler_target="host_f"
        )
        jobs, err = fleet_mod._gather_host_jobs(
            "localhost",
            SimpleNamespace(hosts={}, host=lambda name: None),
            multi_user=False,
            scheduler_host_names={"host_f"},
        )
        assert err is None
        assert {row["id"] for row in jobs} == {"localown0001"}

    def test_scheduler_host_reads_driver_specs(self, web_state: Path) -> None:
        _write_spec("localown0001", state=JobState.RUNNING)
        host_f_spec = _write_spec(
            "twinowned001", state=JobState.PENDING, scheduler_target="host_f"
        )
        host_f_cfg = config.HostConfig(
            ssh="host_f-login",
            scheduler="pbs",
            scheduler_dialect="torque",
            scheduler_driver="localhost",
            scratch_root="/tmp/scratch",
        )
        cfg = SimpleNamespace(
            hosts={"host_f": host_f_cfg}, host=lambda name: host_f_cfg
        )
        jobs, err = fleet_mod._gather_host_jobs(
            "host_f", cfg, multi_user=False, scheduler_host_names={"host_f"}
        )
        assert err is None
        assert [row["id"] for row in jobs] == [host_f_spec.id]
        assert jobs[0]["queue_host"] == "host_f"

    def test_scheduler_host_corrects_old_remote_driver_state_projection(
        self, web_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        host_f_cfg = config.HostConfig(
            ssh="host_f-login",
            scheduler="pbs",
            scheduler_dialect="torque",
            scheduler_driver="driver",
            scratch_root="/tmp/scratch",
        )
        driver_cfg = config.HostConfig(ssh="driver-login")
        cfg = SimpleNamespace(
            hosts={"host_f": host_f_cfg, "driver": driver_cfg},
            host=lambda name: {"host_f": host_f_cfg, "driver": driver_cfg}[name],
        )
        spec = JobSpec(
            id="pollfailed01",
            command=["true"],
            cwd="/tmp/pollfailed01",
            cpus=1,
            state=JobState.RUNNING,
            scheduler_target="host_f",
            scheduler_state="poll_failed",
        )
        old_row = spec.model_dump(mode="json")
        old_row["effective_state"] = "running"
        invalid_row = dict(old_row)
        invalid_row["id"] = "invalidrow01"
        invalid_row["scheduler_state"] = ["running"]
        invalid_row["scheduler_running_confirmed"] = True
        monkeypatch.setattr(
            fleet_mod.transport,
            "run_remote_vq",
            lambda *args: SimpleNamespace(
                stdout=json.dumps([old_row, invalid_row])
            ),
        )

        jobs, err = fleet_mod._gather_host_jobs(
            "host_f", cfg, multi_user=False, scheduler_host_names={"host_f"}
        )

        assert err == "1 invalid queue rows"
        rows = {row["id"]: row for row in jobs}
        assert rows["pollfailed01"]["effective_state"] == "poll_failed"
        assert rows["pollfailed01"]["scheduler_running_confirmed"] is False
        assert rows["invalidrow01"]["effective_state"] == "scheduler_unknown"
        assert rows["invalidrow01"]["scheduler_running_confirmed"] is False

    def test_snapshot_json_roundtrip(self, web_state: Path) -> None:
        import json

        _write_spec("localrun0001", state=JobState.RUNNING)
        snapshot = fleet_mod.gather_fleet_snapshot()
        payload = snapshot.to_json()
        # Must be JSON-serializable end to end (dataclasses flattened).
        text = json.dumps(payload)
        assert "localhost" in text


class TestFleetJobDetail:
    def test_resolve_owner_host(self) -> None:
        host_f_cfg = config.HostConfig(
            ssh="host_f-login",
            scheduler="pbs",
            scheduler_dialect="torque",
            scheduler_driver="host_0",
            scratch_root="/tmp/scratch",
        )
        cfg = SimpleNamespace(hosts={"host_f": host_f_cfg})
        assert fleet_mod.resolve_owner_host(cfg, "host_f") == "host_0"
        assert fleet_mod.resolve_owner_host(cfg, "host_a") == "host_a"

    def test_local_detail_payload(self, web_state: Path) -> None:
        spec = _write_spec(
            "detaildone01",
            state=JobState.COMPLETED,
            exit_code=0,
            job_name="detail-demo",
        )
        workspace = Path(spec.cwd)
        (workspace / spec.stdout_path).parent.mkdir(parents=True, exist_ok=True)
        (workspace / spec.stdout_path).write_text("SCF converged\n")
        payload, err = fleet_mod.fetch_job_detail(
            config.load_config(), "localhost", "detaildone01"
        )
        assert err is None
        assert payload["status"]["id"] == "detaildone01"
        assert "SCF converged" in payload["status"]["stdout"]
        assert payload["owner_host"] == "localhost"
        assert isinstance(payload["events"], list)

    def test_missing_job_returns_error(self, web_state: Path) -> None:
        payload, err = fleet_mod.fetch_job_detail(
            config.load_config(), "localhost", "missingjob01"
        )
        assert payload is None
        assert "missingjob01" in err

    def test_detail_page_renders(self, fleet_client: TestClient) -> None:
        spec = _write_spec(
            "detailrun001", state=JobState.RUNNING, job_name="detail-page"
        )
        workspace = Path(spec.cwd)
        (workspace / spec.stdout_path).write_text("iteration 7\n")
        r = fleet_client.get("/fleet/jobs/detailrun001", params={"host": "localhost"})
        assert r.status_code == 200
        assert "detailrun001" in r.text
        assert "iteration 7" in r.text
        assert "detail-page" in r.text

    def test_detail_page_falls_back_to_snapshot_host(
        self, fleet_client: TestClient
    ) -> None:
        _write_spec("detailsnap01", state=JobState.PENDING)
        cache = fleet_client.app.state.fleet_cache
        cache.set_snapshot(
            _snapshot(
                fleet_mod.HostSnapshot(
                    host="localhost",
                    overview=HostOverview(host="localhost"),
                    jobs=[_row("detailsnap01", "localhost", state="pending")],
                )
            )
        )
        r = fleet_client.get("/fleet/jobs/detailsnap01")
        assert r.status_code == 200
        assert "detailsnap01" in r.text

    def test_detail_page_404_without_host_hint(
        self, fleet_client: TestClient
    ) -> None:
        r = fleet_client.get("/fleet/jobs/nowhere00001")
        assert r.status_code == 404

    def test_detail_page_renders_error_for_missing_job(
        self, fleet_client: TestClient
    ) -> None:
        r = fleet_client.get(
            "/fleet/jobs/missingjob01", params={"host": "localhost"}
        )
        assert r.status_code == 200
        assert "Could not read job" in r.text


class TestJobSamples:
    def test_parse_sample_lines_skips_garbage(self) -> None:
        text = (
            '{"ts": "2026-07-25T10:00:00+00:00", "cpu_percent": 750.0, "rss_mb": 9000}\n'
            "not json\n"
            '{"ts": "2026-07-25T10:00:05+00:00", "cpu_percent": 760.0, "rss_mb": 9100}\n'
        )
        records = fleet_mod._parse_sample_lines(text)
        assert len(records) == 2
        assert records[-1]["rss_mb"] == 9100

    def test_detail_includes_local_samples(self, web_state: Path) -> None:
        spec = _write_spec("samplerun001", state=JobState.RUNNING, mem_mb=16000)
        vq_dir = Path(spec.cwd) / "_vq"
        vq_dir.mkdir(parents=True, exist_ok=True)
        (vq_dir / "samples.jsonl").write_text(
            '{"ts": "2026-07-25T10:00:00+00:00", "cpu_percent": 390.0, "rss_mb": 8123}\n'
            '{"ts": "2026-07-25T10:00:05+00:00", "cpu_percent": 401.0, "rss_mb": 8200}\n'
        )
        payload, err = fleet_mod.fetch_job_detail(
            config.load_config(), "localhost", "samplerun001"
        )
        assert err is None
        assert len(payload["samples"]) == 2
        assert payload["samples"][-1]["cpu_percent"] == 401.0

    def test_detail_page_renders_samples_table(
        self, fleet_client: TestClient
    ) -> None:
        spec = _write_spec("samplepage01", state=JobState.RUNNING, mem_mb=16000)
        vq_dir = Path(spec.cwd) / "_vq"
        vq_dir.mkdir(parents=True, exist_ok=True)
        (vq_dir / "samples.jsonl").write_text(
            '{"ts": "2026-07-25T10:00:05+00:00", "cpu_percent": 401.0, "rss_mb": 8200}\n'
        )
        r = fleet_client.get(
            "/fleet/jobs/samplepage01", params={"host": "localhost"}
        )
        assert r.status_code == 200
        assert "resource samples" in r.text
        assert "8200" in r.text

    def test_missing_samples_yield_empty_list(self, web_state: Path) -> None:
        _write_spec("nosamples001", state=JobState.PENDING)
        payload, err = fleet_mod.fetch_job_detail(
            config.load_config(), "localhost", "nosamples001"
        )
        assert err is None
        assert payload["samples"] == []


class TestDoctorBoard:
    def test_empty_board_renders(self, fleet_client: TestClient) -> None:
        r = fleet_client.get("/fleet/doctor")
        assert r.status_code == 200
        assert "No doctor sweep yet" in r.text

    def test_board_renders_injected_results(
        self, fleet_client: TestClient
    ) -> None:
        fleet_client.app.state.doctor_cache.set_results(
            [
                {
                    "host": "host_a",
                    "ok": True,
                    "scheduler": "local",
                    "driver": None,
                    "checks": [
                        {"name": "config", "ok": True, "message": "configured"}
                    ],
                },
                {
                    "host": "host_f",
                    "ok": False,
                    "scheduler": "pbs",
                    "driver": "host_0",
                    "checks": [
                        {
                            "name": "scheduler_liveness",
                            "ok": False,
                            "message": "queue enabled but not started",
                        }
                    ],
                },
            ]
        )
        r = fleet_client.get("/fleet/doctor")
        assert r.status_code == 200
        assert "host_a" in r.text
        assert "pbs via host_0" in r.text
        assert "queue enabled but not started" in r.text
        api = fleet_client.get("/api/v1/fleet/doctor").json()
        assert api["results"][1]["host"] == "host_f"

    def test_refresh_endpoint_debounces(
        self, fleet_client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            fleet_mod, "gather_doctor_results", lambda timeout=10.0: []
        )
        first = fleet_client.post("/fleet/doctor/_refresh")
        assert first.status_code == 200
        assert first.text == "doctor sweep started"
        cache = fleet_client.app.state.doctor_cache
        for _ in range(100):
            if not cache.state()["refreshing"]:
                break
            import time

            time.sleep(0.02)
        second = fleet_client.post("/fleet/doctor/_refresh")
        assert second.text == "sweep already fresh"

    def test_fleet_refresh_endpoint_debounces(
        self, fleet_client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            fleet_mod, "gather_fleet_snapshot", lambda cfg=None, **kw: _snapshot()
        )
        first = fleet_client.post("/fleet/_refresh")
        assert first.status_code == 200
        assert first.text == "sweep started"
        cache = fleet_client.app.state.fleet_cache
        for _ in range(100):
            with cache._lock:
                busy = cache._manual_refreshing
            if not busy:
                break
            import time

            time.sleep(0.02)
        second = fleet_client.post("/fleet/_refresh")
        assert second.text == "sweep already fresh"


class TestFleetCache:
    def test_snapshot_roundtrip_and_default_interval(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("VQ_WEB_FLEET_INTERVAL", raising=False)
        cache = fleet_mod.FleetCache()
        assert cache.interval_seconds == fleet_mod.DEFAULT_INTERVAL_SECONDS
        assert cache.snapshot() is None
        snap = _snapshot()
        cache.set_snapshot(snap)
        assert cache.snapshot() is snap

    def test_interval_env_override_and_floor(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("VQ_WEB_FLEET_INTERVAL", "120")
        assert fleet_mod.fleet_interval_seconds() == 120
        monkeypatch.setenv("VQ_WEB_FLEET_INTERVAL", "1")
        assert (
            fleet_mod.fleet_interval_seconds()
            == fleet_mod.DEFAULT_INTERVAL_SECONDS
        )

    def test_fleet_enabled_parsing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("VQ_WEB_FLEET", raising=False)
        assert fleet_mod.fleet_enabled() is False
        monkeypatch.setenv("VQ_WEB_FLEET", "1")
        assert fleet_mod.fleet_enabled() is True
        monkeypatch.setenv("VQ_WEB_FLEET", "0")
        assert fleet_mod.fleet_enabled() is False


def _shared_driver_config(driver: str = 'localhost') -> config.Config:
    return config.Config(hosts={
        'localhost': config.HostConfig(ssh='localhost'),
        **({driver: config.HostConfig(ssh='driver.example.invalid')}
           if driver != 'localhost' else {}),
        'alpha': config.HostConfig(ssh='login.example.invalid', scheduler='pbs',
                                  scheduler_dialect='torque', scheduler_driver=driver,
                                  scratch_root='/tmp/scratch'),
        'beta': config.HostConfig(ssh='login.example.invalid', scheduler='pbs',
                                 scheduler_dialect='torque', scheduler_driver=driver,
                                 scratch_root='/tmp/scratch'),
    })


def test_fleet_reuses_large_local_driver_history_and_projects_physical_ownership(
    web_state, monkeypatch,
):
    from vq import overview
    from vq.web import view_context

    specs = [JobSpec(id=f'history{i:05d}', command=['true'], cwd=str(web_state),
                     cpus=1, state=JobState.COMPLETED,
                     finished_at='2020-01-01T00:00:00+00:00')
             for i in range(2000)]
    cases = [
        ('localrun0001', JobState.RUNNING, None, None, 2),
        ('localpend001', JobState.PENDING, None, None, 4),
        ('alpharun001', JobState.RUNNING, 'alpha', 'running', 8),
        ('alphapend01', JobState.PENDING, 'alpha', None, 4),
        ('betaqueued1', JobState.RUNNING, 'beta', 'queued', 16),
        ('betaunknown', JobState.RUNNING, 'beta', 'poll_failed', 32),
        ('unknownhost', JobState.RUNNING, 'removed-target', 'running', 64),
    ]
    specs.extend(JobSpec(id=jobid, command=['true'], cwd=str(web_state), state=state,
                         scheduler_target=target, scheduler_state=phase, cpus=cpus)
                 for jobid, state, target, phase, cpus in cases)
    original_states = [(s.state, s.scheduler_state) for s in specs]
    reads = []
    conversions = []
    env_reads = []
    monkeypatch.setattr(fleet_mod, 'list_jobs', lambda *a, **k: reads.append(a[0]) or specs)
    monkeypatch.setattr(overview, 'list_jobs',
                        lambda *a, **k: pytest.fail('overview reread the shared history'))
    monkeypatch.setattr(overview, '_collect_env_statuses', lambda cfg: env_reads.append(1) or [])
    original_row = fleet_mod._job_row

    def row(spec, *args, **kwargs):
        conversions.append(spec.id)
        return original_row(spec, *args, **kwargs)

    monkeypatch.setattr(fleet_mod, '_job_row', row)
    monkeypatch.setattr(JobSpec, 'model_validate',
                        lambda *a, **k: pytest.fail('local models were revalidated'))
    snapshot = fleet_mod.gather_fleet_snapshot(_shared_driver_config())
    hosts = {h.host: h for h in snapshot.hosts}
    assert reads == ['localhost'] and env_reads == [1]
    assert len(conversions) == len(specs)
    assert hosts['localhost'].overview.queue_counts['running'] == 1
    assert hosts['localhost'].overview.running_cpus == 2
    assert hosts['localhost'].overview.pending_cpus == 4
    assert hosts['alpha'].overview.queue_counts['running'] == 1
    assert hosts['alpha'].overview.pending_cpus == 4
    assert hosts['beta'].overview.queue_counts.get('running', 0) == 0
    assert hosts['beta'].overview.queue_counts['pending'] == 2
    assert hosts['beta'].overview.pending_cpus == 48
    assert hosts['beta'].overview.unconfirmed_scheduler_jobs == 1
    summary = view_context.fleet_grid_context(snapshot)['summary']
    assert summary['running'] == 2 and summary['running_cpus'] == 10
    assert summary['pending'] == 4
    assert len(snapshot.jobs) == len(specs)
    assert [(s.state, s.scheduler_state) for s in specs] == original_states
    assert all(h.jobs_error is None for h in snapshot.hosts)


def test_fleet_reads_remote_driver_once_without_collapsing_distinct_job_ids(web_state, monkeypatch):
    from vq import overview

    local = JobSpec(id='samejob0001', command=['true'], cwd=str(web_state),
                    state=JobState.RUNNING, cpus=2)
    remote = [local.model_copy(update={'cpus': 3}),
              local.model_copy(update={'id': 'alpha000001', 'scheduler_target': 'alpha',
                                       'scheduler_state': 'running', 'cpus': 8}),
              local.model_copy(update={'id': 'beta0000001', 'scheduler_target': 'beta',
                                       'scheduler_state': 'queued', 'cpus': 16})]
    reads = []
    overview_reads = []
    validations = []
    original_validate = JobSpec.model_validate
    payload = json.dumps([fleet_mod._job_row(s, 'localhost') for s in remote])
    monkeypatch.setattr(fleet_mod, 'list_jobs', lambda *a, **k: [local])

    def remote_vq(host, *args, **kwargs):
        reads.append(args)
        assert args == ('queue', 'localhost', '--json')
        return SimpleNamespace(stdout=payload)

    def validate(value, *args, **kwargs):
        validations.append(value['id'])
        return original_validate(value, *args, **kwargs)

    monkeypatch.setattr(JobSpec, 'model_validate', validate)
    monkeypatch.setattr(fleet_mod.transport, 'run_remote_vq', remote_vq)
    monkeypatch.setattr(overview, 'gather_overview_remote',
                        lambda host, *a, **k: overview_reads.append(host) or HostOverview(
                            host=host, queue_counts={'running': 999}, running_cpus=999))
    snapshot = fleet_mod.gather_fleet_snapshot(_shared_driver_config('driver'))
    hosts = {h.host: h for h in snapshot.hosts}
    assert len(reads) == 1 and overview_reads == ['driver']
    assert len(validations) == len(remote)
    assert hosts['driver'].overview.running_cpus == 3
    assert hosts['alpha'].overview.running_cpus == 8
    assert hosts['beta'].overview.running_cpus == 0
    handles = [row['queue_handle']['host'] for row in snapshot.jobs if row['id'] == local.id]
    assert sorted(handles) == ['driver', 'localhost']


@pytest.mark.parametrize('manual', [False, True])
def test_slow_refresh_is_visible_and_automatic_manual_sweeps_do_not_overlap(
    fleet_client, monkeypatch, manual,
):
    import threading
    from datetime import UTC, datetime, timedelta

    cache = fleet_client.app.state.fleet_cache
    previous = fleet_mod.FleetSnapshot(
        gathered_at=(datetime.now(UTC) - timedelta(seconds=600)).isoformat(),
        duration_seconds=120,
    )
    cache.set_snapshot(previous)
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    original_refresh = cache._run_refresh

    def refresh():
        try:
            return original_refresh()
        finally:
            finished.set()

    monkeypatch.setattr(cache, '_run_refresh', refresh)
    calls = []
    clock = [1000.0]
    monkeypatch.setattr(fleet_mod, 'monotonic', lambda: clock[0])

    def gather():
        calls.append(1)
        entered.set()
        assert release.wait(10), 'test did not release the slow sweep'
        return previous

    monkeypatch.setattr(fleet_mod, 'gather_fleet_snapshot', gather)
    thread = None
    try:
        if manual:
            assert cache.refresh_async()
        else:
            thread = threading.Thread(target=cache.refresh)
            thread.start()
        assert entered.wait(10)
        clock[0] += 180
        assert not cache.refresh_async()
        assert cache.refresh() is previous
        response = fleet_client.get('/fleet/_grid')
        assert 'Refresh in progress for 3m' in response.text
        assert 'hung' not in response.text and 'last fleet refresh failed' not in response.text
        state = fleet_client.get('/api/v1/fleet').json()
        assert state['gathered_at'] == previous.gathered_at
        assert state['refresh']['refreshing'] is True
        assert state['refresh']['elapsed_seconds'] == 180
        assert calls == [1]
    finally:
        release.set()
        assert finished.wait(10)
        if thread is not None:
            thread.join(10)
            assert not thread.is_alive()


def test_failed_refresh_keeps_source_age_and_reports_failure(fleet_client, monkeypatch):
    cache = fleet_client.app.state.fleet_cache
    previous = _snapshot()
    cache.set_snapshot(previous)

    def gather():
        raise ValueError('injected observation failure')

    monkeypatch.setattr(fleet_mod, 'gather_fleet_snapshot', gather)
    with pytest.raises(ValueError, match='injected observation'):
        cache.refresh()
    assert cache.snapshot() is previous
    state = cache.refresh_state()
    assert state['refreshing'] is False
    assert state['error'] == 'injected observation failure'
    assert state['finished_at'] is not None
    response = fleet_client.get('/fleet/_grid')
    assert 'The last fleet refresh failed' in response.text
    assert 'injected observation failure' in response.text
