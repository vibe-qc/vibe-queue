"""Tests for fleet-console authentication (M2): local accounts, signed
sessions, route gating, write actions, and the audit trail."""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from vq import auth, config, paths
from vq.spec import JobSpec, JobState
from vq.web import authn, create_app, fleet_audit


@pytest.fixture
def web_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
    monkeypatch.setenv("VQ_WEB_FLEET", "1")
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


@pytest.fixture
def client(web_state: Path) -> TestClient:
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


def _login(client: TestClient, user: str, password: str) -> None:
    r = client.post(
        "/fleet/login",
        data={"user": user, "password": password, "next": "/fleet"},
        follow_redirects=False,
    )
    assert r.status_code == 303, r.text


class TestAuthnUnit:
    def test_password_roundtrip(self, web_state: Path) -> None:
        authn.add_user("example_user", "correct horse", "admin")
        assert authn.verify_password("example_user", "correct horse") == "admin"
        assert authn.verify_password("example_user", "wrong") is None
        assert authn.verify_password("nobody", "correct horse") is None

    def test_duplicate_user_needs_force(self, web_state: Path) -> None:
        authn.add_user("example_user", "pw1", "viewer")
        with pytest.raises(FileExistsError):
            authn.add_user("example_user", "pw2", "viewer")
        authn.add_user("example_user", "pw2", "operator", force=True)
        assert authn.verify_password("example_user", "pw2") == "operator"
        assert authn.verify_password("example_user", "pw1") is None

    def test_remove_user(self, web_state: Path) -> None:
        authn.add_user("example_user", "pw", "viewer")
        authn.remove_user("example_user")
        assert authn.load_users() == {}
        with pytest.raises(FileNotFoundError):
            authn.remove_user("example_user")

    def test_role_order(self) -> None:
        assert authn.role_at_least("admin", "viewer")
        assert authn.role_at_least("operator", "operator")
        assert not authn.role_at_least("viewer", "operator")
        assert not authn.role_at_least(None, "viewer")
        assert not authn.role_at_least("bogus", "viewer")

    def test_session_roundtrip(self, web_state: Path) -> None:
        authn.add_user("example_user", "pw", "operator")
        token = authn.issue_session("example_user", "operator")
        ident = authn.verify_session(token)
        assert ident == {"user": "example_user", "role": "operator"}

    def test_session_tamper_and_expiry(self, web_state: Path) -> None:
        authn.add_user("example_user", "pw", "operator")
        token = authn.issue_session("example_user", "operator")
        tampered_char = "0" if token[-1] != "0" else "1"
        assert authn.verify_session(token[:-1] + tampered_char) is None
        assert authn.verify_session("garbage") is None
        assert authn.verify_session(None) is None
        expired = authn.issue_session("example_user", "operator", ttl_seconds=-5)
        assert authn.verify_session(expired) is None

    def test_users_file_mode_is_0600(self, web_state: Path) -> None:
        path = authn.add_user("example_user", "pw", "viewer")
        assert (path.stat().st_mode & 0o777) == 0o600


class TestOpenMode:
    def test_pages_open_without_accounts(self, client: TestClient) -> None:
        assert client.get("/fleet").status_code == 200
        assert client.get("/fleet/jobs").status_code == 200
        assert client.get("/api/v1/fleet").status_code == 200

    def test_write_actions_disabled(self, client: TestClient) -> None:
        _write_spec("openpend0001", state=JobState.PENDING)
        r = client.post(
            "/fleet/jobs/openpend0001/kill",
            data={"host": "localhost"},
            follow_redirects=False,
        )
        assert r.status_code == 503

    def test_login_page_explains_open_mode(self, client: TestClient) -> None:
        r = client.get("/fleet/login")
        assert r.status_code == 200
        assert "No accounts are configured" in r.text


class TestGating:
    def test_pages_redirect_to_login(self, client: TestClient) -> None:
        authn.add_user("example_user", "pw", "viewer")
        r = client.get("/fleet", follow_redirects=False)
        assert r.status_code == 302
        assert r.headers["location"].startswith("/fleet/login")

    def test_fragments_and_api_401(self, client: TestClient) -> None:
        authn.add_user("example_user", "pw", "viewer")
        assert client.get("/fleet/_grid").status_code == 401
        assert client.get("/api/v1/fleet").status_code == 401

    def test_login_flow(self, client: TestClient) -> None:
        authn.add_user("example_user", "pw", "viewer")
        bad = client.post(
            "/fleet/login",
            data={"user": "example_user", "password": "wrong", "next": "/fleet"},
            follow_redirects=False,
        )
        assert bad.status_code == 401
        _login(client, "example_user", "pw")
        page = client.get("/fleet")
        assert page.status_code == 200
        assert "example_user (viewer)" in page.text
        assert client.get("/api/v1/fleet").status_code == 200

    def test_logout_clears_session(self, client: TestClient) -> None:
        authn.add_user("example_user", "pw", "viewer")
        _login(client, "example_user", "pw")
        assert client.get("/fleet").status_code == 200
        client.post("/fleet/logout", follow_redirects=False)
        assert client.get("/fleet", follow_redirects=False).status_code == 302

    def test_bearer_token_still_serves_api(self, client: TestClient) -> None:
        authn.add_user("example_user", "pw", "viewer")
        token = auth.generate_token()
        auth.write_token(token)
        r = client.get(
            "/api/v1/fleet", headers={"Authorization": f"Bearer {token}"}
        )
        assert r.status_code == 200

    def test_open_redirect_is_neutralized(self, client: TestClient) -> None:
        authn.add_user("example_user", "pw", "viewer")
        r = client.post(
            "/fleet/login",
            data={
                "user": "example_user",
                "password": "pw",
                "next": "//evil.example.com/",
            },
            follow_redirects=False,
        )
        assert r.status_code == 303
        assert r.headers["location"] == "/fleet"

    def test_single_host_dashboard_is_open_until_an_account_exists(
        self, client: TestClient
    ) -> None:
        """No accounts = documented open "tunnel posture", unchanged."""
        assert client.get("/queue").status_code == 200
        assert client.get("/api/v1/queue").status_code == 200

    def test_single_host_dashboard_is_gated_once_auth_is_on(
        self, client: TestClient
    ) -> None:
        """v0.25.0 reverses the M2 split.

        M2 gated only the fleet surface, leaving /queue, /jobs/<id> (with
        its stdout/stderr tails) and /api/v1/* answering anybody who
        could reach the same port -- while /fleet next door asked for a
        password. On a console bound to a private overlay that is every
        peer on the overlay, uncredentialed. Once accounts exist, the
        whole port is behind the login.
        """
        authn.add_user("example_user", "pw", "viewer")
        assert client.get("/queue").status_code == 401
        assert client.get("/api/v1/queue").status_code == 401

        _login(client, "example_user", "pw")
        assert client.get("/queue").status_code == 200
        assert client.get("/api/v1/queue").status_code == 200

    def test_gated_browser_navigation_redirects_to_login(
        self, client: TestClient
    ) -> None:
        """A browser gets the login form; a script gets a status code.

        htmx 2.x does not swap non-2xx responses, so answering a fetch
        with a redirect would paint a login page into a table.
        """
        authn.add_user("example_user", "pw", "viewer")
        r = client.get(
            "/queue",
            headers={"accept": "text/html"},
            follow_redirects=False,
        )
        assert r.status_code == 302
        assert r.headers["location"].startswith("/fleet/login?next=")

    def test_health_endpoints_stay_open_for_supervisors(
        self, client: TestClient
    ) -> None:
        """Liveness/readiness carry no job data and are polled by things
        that have no session -- systemd, a uptime checker, a load
        balancer. Gating them would report a healthy console as down."""
        authn.add_user("example_user", "pw", "viewer")
        assert client.get("/health/live").status_code == 200
        # /health/ready answers 503 with no daemon running, which is the
        # readiness verdict working -- what matters here is that it is
        # not a 401, i.e. the guard let it through to be answered.
        assert client.get("/health/ready").status_code != 401

    def test_static_assets_stay_open_so_the_login_page_renders(
        self, client: TestClient
    ) -> None:
        """The login form needs its own stylesheet and htmx."""
        authn.add_user("example_user", "pw", "viewer")
        assert client.get("/static/style.css").status_code == 200
        assert client.get("/static/htmx.min.js").status_code == 200


class TestWriteActions:
    def test_operator_kill_local_job(self, client: TestClient) -> None:
        authn.add_user("op", "pw", "operator")
        _login(client, "op", "pw")
        spec = _write_spec("killme000001", state=JobState.PENDING)
        r = client.post(
            f"/fleet/jobs/{spec.id}/kill",
            data={"host": "localhost", "reason": "console test"},
            follow_redirects=False,
        )
        assert r.status_code == 303
        assert JobSpec.read(paths.spec_path(spec.id)).state == JobState.KILLED
        records = fleet_audit.read_audit()
        kill_records = [x for x in records if x["action"] == "kill"]
        assert kill_records and kill_records[0]["outcome"].startswith("ok")
        assert kill_records[0]["user"] == "op"

    def test_viewer_cannot_act(self, client: TestClient) -> None:
        authn.add_user("view", "pw", "viewer")
        _login(client, "view", "pw")
        spec = _write_spec("noperm000001", state=JobState.PENDING)
        r = client.post(
            f"/fleet/jobs/{spec.id}/kill",
            data={"host": "localhost"},
            follow_redirects=False,
        )
        assert r.status_code == 403
        assert JobSpec.read(paths.spec_path(spec.id)).state == JobState.PENDING
        records = fleet_audit.read_audit()
        assert any(x["outcome"].startswith("forbidden") for x in records)

    def test_action_error_is_audited_not_500(self, client: TestClient) -> None:
        authn.add_user("op", "pw", "operator")
        _login(client, "op", "pw")
        r = client.post(
            "/fleet/jobs/missingjob01/kill",
            data={"host": "localhost"},
            follow_redirects=False,
        )
        assert r.status_code == 303
        assert "error" in r.headers["location"]


class TestAuditSurface:
    def test_admin_sees_audit_page(self, client: TestClient) -> None:
        authn.add_user("boss", "pw", "admin")
        _login(client, "boss", "pw")
        r = client.get("/fleet/audit")
        assert r.status_code == 200
        assert "Fleet audit trail" in r.text
        assert "login" in r.text

    def test_operator_denied_audit_page(self, client: TestClient) -> None:
        authn.add_user("op", "pw", "operator")
        _login(client, "op", "pw")
        assert client.get("/fleet/audit").status_code == 403

    def test_audit_api_with_bearer(self, client: TestClient) -> None:
        authn.add_user("boss", "pw", "admin")
        token = auth.generate_token()
        auth.write_token(token)
        r = client.get(
            "/api/v1/fleet/audit",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert r.status_code == 200
        assert "records" in r.json()
