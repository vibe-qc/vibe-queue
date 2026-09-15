"""Tests for the read-only web UI (FastAPI app at vq.web)."""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from vq import auth, config, ownership, paths
from vq.spec import JobSpec, JobState
from vq.web import create_app


@pytest.fixture
def web_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Per-test isolated state dir, fresh app instance.

    Also isolates the auth-token config dir: tests that exercise the
    "no token configured" path would otherwise see the real
    `~/.config/vq/web-token` on hosts that run the live web service
    (e.g. host_d) and get 401 where they expected 503.
    """
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


@pytest.fixture
def client(web_state: Path) -> TestClient:
    return TestClient(create_app())


def _write_spec(jobid: str, **overrides) -> JobSpec:
    """Helper: create a workspace + spec on disk under the current
    state dir, return the spec."""
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


class TestRoot:
    def test_root_redirects_to_queue(self, client: TestClient) -> None:
        r = client.get("/", follow_redirects=False)
        assert r.status_code == 302
        assert r.headers["location"] == "/queue"


class TestQueue:
    def test_empty_queue_renders(self, client: TestClient) -> None:
        r = client.get("/queue")
        assert r.status_code == 200
        assert "text/html" in r.headers["content-type"]
        assert "No jobs." in r.text

    def test_queue_lists_submitted_jobs(self, client: TestClient) -> None:
        _write_spec("aaaaaaaaaaaa", state=JobState.PENDING, cpus=4)
        _write_spec("bbbbbbbbbbbb", state=JobState.RUNNING, cpus=8, mem_mb=16000)
        _write_spec("cccccccccccc", state=JobState.COMPLETED)
        r = client.get("/queue")
        assert r.status_code == 200
        for jid in ("aaaaaaaaaaaa", "bbbbbbbbbbbb", "cccccccccccc"):
            assert jid in r.text
        assert "16000" in r.text
        # State CSS classes wired up so the table renders coloured badges
        assert 'state-pending' in r.text
        assert 'state-running' in r.text
        assert 'state-completed' in r.text

    def test_queue_renders_cockpit_summary(self, client: TestClient) -> None:
        _write_spec("summaryrun01", state=JobState.RUNNING, cpus=8, mem_mb=32000)
        _write_spec(
            "summaryfail1",
            state=JobState.FAILED,
            exit_code=137,
            scheduler_target="host_c",
            failure_reason="Killed: 9",
        )

        r = client.get("/queue")

        assert r.status_code == 200
        assert "active CPUs" in r.text
        assert "declared memory" in r.text
        assert "needs triage" in r.text
        assert "scheduler hosts: host_c" in r.text
        assert "Command died from SIGKILL" in r.text

    def test_queue_defaults_to_newest_jobs_first(self, client: TestClient) -> None:
        _write_spec(
            "olderjob0001",
            state=JobState.COMPLETED,
            submitted_at="2026-07-14T10:00:00+00:00",
        )
        _write_spec(
            "newerjob0001",
            state=JobState.RUNNING,
            submitted_at="2026-07-16T10:00:00+00:00",
        )

        r = client.get("/queue")

        assert r.status_code == 200
        assert r.text.index("newerjob0001") < r.text.index("olderjob0001")
        assert 'value="submitted" selected' in r.text
        assert 'value="desc" selected' in r.text

    def test_queue_filters_by_state_host_and_search(
        self, client: TestClient
    ) -> None:
        _write_spec(
            "keepjob00001",
            state=JobState.FAILED,
            scheduler_target="host_c",
            command=["python", "target.py"],
        )
        _write_spec(
            "dropjob00001",
            state=JobState.RUNNING,
            scheduler_target="host_f",
            command=["python", "other.py"],
        )

        r = client.get("/queue?state=failed&host=host_c&q=target")

        assert r.status_code == 200
        assert "keepjob00001" in r.text
        assert "dropjob00001" not in r.text
        assert "1 / 1 shown (2 total)" in r.text

    def test_queue_defaults_to_bounded_row_count(self, client: TestClient) -> None:
        for idx in range(205):
            _write_spec(
                f"limitjob{idx:04d}",
                state=JobState.COMPLETED,
                submitted_at=f"2026-07-14T10:{idx // 60:02d}:{idx % 60:02d}+00:00",
            )

        r = client.get("/queue")

        assert r.status_code == 200
        assert "200 / 205 shown" in r.text
        assert "limitjob0204" in r.text
        assert "limitjob0000" not in r.text

    def test_queue_limit_query_can_show_more_rows(self, client: TestClient) -> None:
        for idx in range(205):
            _write_spec(
                f"morejob{idx:04d}",
                state=JobState.COMPLETED,
                submitted_at=f"2026-07-14T10:{idx // 60:02d}:{idx % 60:02d}+00:00",
            )

        r = client.get("/queue?limit=500")

        assert r.status_code == 200
        assert "205 / 205 shown" in r.text
        assert "morejob0204" in r.text
        assert "morejob0000" in r.text

    def test_queue_sort_headers_change_order(self, client: TestClient) -> None:
        _write_spec("aaaacpus0001", state=JobState.RUNNING, cpus=1)
        _write_spec("zzzzcpus0008", state=JobState.RUNNING, cpus=8)

        r = client.get("/queue?sort=cpus&dir=asc")

        assert r.status_code == 200
        assert r.text.index("aaaacpus0001") < r.text.index("zzzzcpus0008")
        assert "/queue?sort=cpus" in r.text

    def test_queue_table_fragment_does_not_include_chrome(
        self, client: TestClient
    ) -> None:
        """The htmx polling endpoint returns just the table div; no
        <html>/<head>/<header>. Otherwise hx-swap=outerHTML would
        cascade and re-render the page header on every poll."""
        _write_spec("abc123def456", state=JobState.RUNNING)
        r = client.get("/queue/_table")
        assert r.status_code == 200
        assert "abc123def456" in r.text
        assert "<html" not in r.text
        assert "<head" not in r.text
        assert "<header" not in r.text

    def test_queue_api_returns_cockpit_job_payloads(
        self, client: TestClient
    ) -> None:
        _write_spec(
            "apiqueue0001",
            state=JobState.RUNNING,
            scheduler_target="host_c",
            submitted_at="2026-07-15T10:00:00+00:00",
        )

        r = client.get("/api/v1/queue")

        assert r.status_code == 200
        payload = r.json()
        assert payload["host"] == "localhost"
        assert payload["summary"] == {
            "total": 1,
            "active": 1,
            "pending": 0,
            "terminal_attention": 0,
            "active_cpus": 1,
            "declared_mem_mb": 0,
            "scheduler_hosts": ["host_c"],
            "state_counts": {"running": 1},
        }
        assert len(payload["jobs"]) == 1
        row = payload["jobs"][0]
        assert row["id"] == "apiqueue0001"
        assert row["queue_handle"] == {
            "job_id": "apiqueue0001",
            "host": "host_c",
            "submitted_at": "2026-07-15T10:00:00+00:00",
        }
        assert row["terminal_diagnosis"] is None

    def test_queue_clear_failed_endpoint_deletes_only_old_failed_jobs(
        self, client: TestClient, web_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(web_state / "cfg"))
        token = auth.generate_token()
        auth.write_token(token)
        old_failed = _write_spec(
            "oldfailed001",
            state=JobState.FAILED,
            submitted_at="2000-01-01T10:00:00+00:00",
            finished_at="2000-01-01T11:00:00+00:00",
        )
        recent_failed = _write_spec(
            "newfailed001",
            state=JobState.FAILED,
            submitted_at="2999-01-01T10:00:00+00:00",
            finished_at="2999-01-01T11:00:00+00:00",
        )
        completed = _write_spec(
            "completed001",
            state=JobState.COMPLETED,
            submitted_at="2000-01-01T10:00:00+00:00",
            finished_at="2000-01-01T11:00:00+00:00",
        )

        r = client.post(
            "/api/v1/queue/clear-failed?older_than=7d",
            headers={"Authorization": f"Bearer {token}"},
        )

        assert r.status_code == 200
        assert "cleared 1 old failed job" in r.text
        assert not paths.spec_path(old_failed.id).exists()
        assert not Path(old_failed.cwd).exists()
        assert paths.spec_path(recent_failed.id).exists()
        assert paths.spec_path(completed.id).exists()


class TestJobDetail:
    def test_unknown_jobid_404s(self, client: TestClient) -> None:
        r = client.get("/jobs/deadbeef")
        assert r.status_code == 404

    def test_job_detail_renders_spec_fields(self, client: TestClient) -> None:
        spec = _write_spec(
            "fedcba987654",
            state=JobState.RUNNING,
            cpus=8,
            mem_mb=16000,
            wall_time_seconds=7200,
            submitter="test_user@host_d",
            pid=12345,
            pgid=12345,
        )
        r = client.get(f"/jobs/{spec.id}")
        assert r.status_code == 200
        for needle in (
            "fedcba987654",
            "8",            # cpus
            "16000",        # mem_mb
            "7200",         # wall_time_seconds
            "test_user@host_d",
            "12345",        # pid / pgid
            "python run.py",
        ):
            assert needle in r.text, f"missing {needle!r}"

    def test_job_detail_renders_monitor_fields(self, client: TestClient) -> None:
        spec = _write_spec(
            "monitorjob01",
            state=JobState.RUNNING,
            scheduler_target="host_c",
            scheduler_state="queued",
            workdir="/scratch/vq/workdirs/monitorjob01",
        )

        r = client.get(f"/jobs/{spec.id}")

        assert r.status_code == 200
        assert "queue_handle" in r.text
        assert "host_c:monitorjob01" in r.text
        assert "scheduler" in r.text
        assert "host_c / queued" in r.text
        assert "/scratch/vq/workdirs/monitorjob01" in r.text
        assert "/scratch/vq/workdirs/monitorjob01/checkpoint.qvf" in r.text

    def test_job_detail_decodes_a_signal_exit(self, client: TestClient) -> None:
        # v0.12.0: a 128+sig exit (137 = SIGKILL) renders the decoded signal
        # via the describe_exit Jinja filter, matching vq status, not a bare 137.
        spec = _write_spec(
            "aaaabbbbcccc",
            state=JobState.OOM_KILLED,
            exit_code=137,
        )
        r = client.get(f"/jobs/{spec.id}")
        assert r.status_code == 200
        assert "SIGKILL" in r.text
        assert "137" in r.text

    def test_job_detail_includes_stdout_stderr_when_present(
        self, client: TestClient
    ) -> None:
        spec = _write_spec("logsexamples1", state=JobState.COMPLETED)
        (Path(spec.cwd) / spec.stdout_path).write_text("hello from stdout\n")
        (Path(spec.cwd) / spec.stderr_path).write_text("warn: deprecated\n")
        r = client.get(f"/jobs/{spec.id}")
        assert "hello from stdout" in r.text
        assert "warn: deprecated" in r.text

    def test_job_log_fragment(self, client: TestClient) -> None:
        spec = _write_spec("logfragment0", state=JobState.RUNNING)
        (Path(spec.cwd) / spec.stdout_path).write_text("live output line 1\n")
        r = client.get(f"/jobs/{spec.id}/_log")
        assert r.status_code == 200
        assert "live output line 1" in r.text
        # Fragment, not full page
        assert "<html" not in r.text

    def test_job_detail_renders_event_log(self, client: TestClient) -> None:
        from vq import events
        spec = _write_spec("eventslogged0", state=JobState.COMPLETED)
        ws = Path(spec.cwd)
        events.append_event(ws, events.EventKind.SUBMITTED, spec.id, cpus=1)
        events.append_event(
            ws, events.EventKind.STATE_TRANSITION, spec.id,
            **{"from": "pending", "to": "running"},
        )
        r = client.get(f"/jobs/{spec.id}")
        assert "submitted" in r.text
        assert "state_transition" in r.text
        # No events recorded yet branch
        assert "No events recorded yet" not in r.text

    def test_job_detail_api_returns_terminal_diagnosis(
        self, client: TestClient
    ) -> None:
        spec = _write_spec(
            "apijob000001",
            state=JobState.FAILED,
            exit_code=137,
            failure_reason="Killed: 9",
            submitted_at="2026-07-15T10:00:00+00:00",
        )

        r = client.get(f"/api/v1/jobs/{spec.id}")

        assert r.status_code == 200
        payload = r.json()
        assert payload["id"] == "apijob000001"
        assert payload["queue_handle"] == {
            "job_id": "apijob000001",
            "host": "localhost",
            "submitted_at": "2026-07-15T10:00:00+00:00",
        }
        assert payload["terminal_diagnosis"]["category"] == "sigkill"
        assert (
            payload["terminal_diagnosis"]["action_hint"]
            == "increase_memory_or_check_external_kill"
        )

    def test_job_detail_api_unknown_jobid_404s(self, client: TestClient) -> None:
        r = client.get("/api/v1/jobs/deadbeef")
        assert r.status_code == 404


class TestHealth:
    def test_live_always_200(self, client: TestClient) -> None:
        r = client.get("/health/live")
        assert r.status_code == 200
        assert r.text == "ok"

    def test_ready_503_when_daemon_down(self, client: TestClient) -> None:
        # No daemon running in tests
        r = client.get("/health/ready")
        assert r.status_code == 503
        assert "daemon not running" in r.text

    def test_ready_200_when_daemon_running(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Mock is_daemon_running to True
        monkeypatch.setattr(
            "vq.web.is_daemon_running", lambda *a, **kw: True
        )
        r = client.get("/health/ready")
        assert r.status_code == 200
        assert r.text == "ok"

    def test_ready_uses_single_user_daemon_scope(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        scopes: list[bool] = []

        def daemon_running(*, multi_user: bool = False) -> bool:
            scopes.append(multi_user)
            return True

        monkeypatch.setattr("vq.web.is_daemon_running", daemon_running)

        r = client.get("/health/ready")

        assert r.status_code == 200
        assert scopes == [False]

    def test_ready_missing_queue_does_not_expose_path(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        queue_root = paths.queue_dir()
        queue_root.rmdir()
        monkeypatch.setattr(
            "vq.web.is_daemon_running", lambda *a, **kw: True
        )

        r = client.get("/health/ready")

        assert r.status_code == 503
        assert r.json() == {"detail": "queue dir missing"}
        assert str(queue_root) not in r.text


class TestStatic:
    def test_static_css_served(self, client: TestClient) -> None:
        r = client.get("/static/style.css")
        assert r.status_code == 200
        assert "text/css" in r.headers["content-type"]
        assert ".badge-ok" in r.text  # sanity


class TestWriteActionsAuth:
    """v0.5.1 write endpoints require a bearer token. Read endpoints
    must remain unauthenticated (covered by the read tests above)."""

    @pytest.fixture
    def client_with_token(
        self, web_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> tuple[TestClient, str]:
        from vq import auth, config
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(web_state / "cfg"))
        monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
        token = auth.generate_token()
        auth.write_token(token)
        return TestClient(create_app()), token

    def test_write_endpoint_returns_503_when_no_token_configured(
        self, client: TestClient, web_state: Path
    ) -> None:
        # client fixture doesn't set up a token file
        r = client.post("/api/v1/jobs/abc123def456/kill")
        assert r.status_code == 503
        assert "auth not configured" in r.json()["detail"]

    def test_write_endpoint_returns_401_without_bearer_header(
        self, client_with_token: tuple[TestClient, str]
    ) -> None:
        client, _ = client_with_token
        r = client.post("/api/v1/jobs/abc123def456/kill")
        assert r.status_code == 401

    def test_write_endpoint_returns_401_with_wrong_token(
        self, client_with_token: tuple[TestClient, str]
    ) -> None:
        client, _ = client_with_token
        r = client.post(
            "/api/v1/jobs/abc123def456/kill",
            headers={"Authorization": "Bearer wrong-token"},
        )
        assert r.status_code == 401

    def test_kill_with_correct_token_404s_unknown_jobid(
        self, client_with_token: tuple[TestClient, str]
    ) -> None:
        client, token = client_with_token
        r = client.post(
            "/api/v1/jobs/nonexistent1/kill",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert r.status_code == 404
        assert "no such job" in r.json()["detail"]

    def test_kill_pending_job_via_api(
        self, client_with_token: tuple[TestClient, str], web_state: Path
    ) -> None:
        client, token = client_with_token
        spec = _write_spec("readytodie01", state=JobState.PENDING)
        r = client.post(
            f"/api/v1/jobs/{spec.id}/kill",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert r.status_code == 200, r.text
        assert "killed pending" in r.text
        assert JobSpec.read(paths.spec_path(spec.id)).state == JobState.KILLED

    def test_kill_api_records_reason(
        self, client_with_token: tuple[TestClient, str], web_state: Path
    ) -> None:
        client, token = client_with_token
        spec = _write_spec("readytodie02", state=JobState.PENDING)
        r = client.post(
            f"/api/v1/jobs/{spec.id}/kill",
            params={"reason": "obsolete runtime; resubmit after update"},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert r.status_code == 200, r.text
        killed = JobSpec.read(paths.spec_path(spec.id))
        assert killed.failure_reason == (
            "killed by vq/operator request: "
            "obsolete runtime; resubmit after update"
        )

    def test_kill_api_can_resubmit_after_update(
        self, client_with_token: tuple[TestClient, str], web_state: Path
    ) -> None:
        client, token = client_with_token
        spec = _write_spec("readytodie03", state=JobState.PENDING)
        r = client.post(
            f"/api/v1/jobs/{spec.id}/kill",
            params={
                "reason": "obsolete runtime; resubmit after update",
                "resubmit": "true",
            },
            headers={"Authorization": f"Bearer {token}"},
        )
        assert r.status_code == 200, r.text
        assert f"resubmitted {spec.id} -> " in r.text
        new_id = r.text.strip().split(" -> ")[-1]
        killed = JobSpec.read(paths.spec_path(spec.id))
        restarted = JobSpec.read(paths.spec_path(new_id))
        assert killed.state == JobState.KILLED
        assert restarted.state == JobState.PENDING
        assert restarted.parent_jobid == spec.id

    def test_pause_unknown_jobid_404s(
        self, client_with_token: tuple[TestClient, str]
    ) -> None:
        client, token = client_with_token
        r = client.post(
            "/api/v1/jobs/nopejobid000/pause",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert r.status_code == 404

    def test_pause_pending_job_409s(
        self, client_with_token: tuple[TestClient, str], web_state: Path
    ) -> None:
        client, token = client_with_token
        spec = _write_spec("pendingjob01", state=JobState.PENDING)
        r = client.post(
            f"/api/v1/jobs/{spec.id}/pause",
            headers={"Authorization": f"Bearer {token}"},
        )
        # PauseError -> 409 (state precludes the action)
        assert r.status_code == 409
        assert "only RUNNING jobs" in r.json()["detail"]

    def test_resume_when_not_suspended_409s(
        self, client_with_token: tuple[TestClient, str], web_state: Path
    ) -> None:
        client, token = client_with_token
        spec = _write_spec("notsusp00001", state=JobState.PENDING)
        r = client.post(
            f"/api/v1/jobs/{spec.id}/resume",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert r.status_code == 409
        assert "only SUSPENDED jobs" in r.json()["detail"]

    @pytest.mark.parametrize(
        ("route", "service_name", "error", "expected_status"),
        (
            (
                "/api/v1/jobs/policyjob001/pause",
                "pause_job",
                ownership.OwnershipError("foreign pause denied"),
                403,
            ),
            (
                "/api/v1/jobs/policyjob001/resume",
                "resume_job",
                config.ConfigError("resume policy unavailable"),
                503,
            ),
            (
                "/api/v1/queue/pause",
                "pause_all",
                config.ConfigError("pause policy unavailable"),
                503,
            ),
            (
                "/api/v1/queue/resume",
                "resume_all",
                ownership.OwnershipError("foreign queue denied"),
                403,
            ),
        ),
    )
    def test_pause_resume_policy_errors_are_translated(
        self,
        client_with_token: tuple[TestClient, str],
        monkeypatch: pytest.MonkeyPatch,
        route: str,
        service_name: str,
        error: Exception,
        expected_status: int,
    ) -> None:
        _, token = client_with_token

        def fail_policy(*args, **kwargs):  # type: ignore[no-untyped-def]
            raise error

        monkeypatch.setattr(f"vq.web.{service_name}", fail_policy)
        client = TestClient(create_app(), raise_server_exceptions=False)

        response = client.post(
            route,
            headers={"Authorization": f"Bearer {token}"},
        )

        assert response.status_code == expected_status
        assert response.json() == {"detail": str(error)}

    def test_read_endpoints_remain_unauthenticated(
        self, client_with_token: tuple[TestClient, str]
    ) -> None:
        """Setting up a token must NOT lock the read pages."""
        client, _ = client_with_token
        for path in ("/queue", "/health/live"):
            r = client.get(path)
            assert r.status_code == 200, f"{path} got {r.status_code}"


class TestQueueWideActions:
    """v0.5.2 added /api/v1/queue/{pause,resume} for whole-queue
    operations. Same auth rules as the per-job endpoints."""

    @pytest.fixture
    def client_with_token(
        self, web_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> tuple[TestClient, str]:
        from vq import auth, config
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(web_state / "cfg"))
        monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
        token = auth.generate_token()
        auth.write_token(token)
        return TestClient(create_app()), token

    def test_queue_pause_requires_auth(self, client: TestClient) -> None:
        r = client.post("/api/v1/queue/pause")
        # No token configured at all -> 503
        assert r.status_code == 503

    def test_queue_pause_returns_summary(
        self, client_with_token: tuple[TestClient, str], web_state: Path
    ) -> None:
        client, token = client_with_token
        # Empty queue: summary shows 0 paused, no exception.
        r = client.post(
            "/api/v1/queue/pause",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert r.status_code == 200
        assert "paused 0 jobs" in r.text

    def test_queue_resume_returns_summary(
        self, client_with_token: tuple[TestClient, str]
    ) -> None:
        client, token = client_with_token
        r = client.post(
            "/api/v1/queue/resume",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert r.status_code == 200
        assert "resumed 0 jobs" in r.text

    def test_queue_pause_skips_pending_and_completed(
        self, client_with_token: tuple[TestClient, str], web_state: Path
    ) -> None:
        client, token = client_with_token
        _write_spec("pendingq0001", state=JobState.PENDING)
        _write_spec("completedq01", state=JobState.COMPLETED)
        r = client.post(
            "/api/v1/queue/pause",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert r.status_code == 200
        # 0 paused (no RUNNING jobs to act on), 2 not RUNNING.
        assert "paused 0 jobs" in r.text
        assert "not RUNNING" in r.text
