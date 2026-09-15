"""Registration and adapter contracts for the staged web split.

These tests intentionally exercise the pre-extraction app factory.  They pin
the FastAPI registration surface, template context boundaries, authentication
gates, cookies, and audit records so route-registration helpers can move
without silently changing the web protocol.
"""

from __future__ import annotations

import inspect
from pathlib import Path

import pytest
from fastapi.responses import HTMLResponse
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from vq import auth, config, paths
from vq import web as web_mod
from vq.overview import HostOverview
from vq.spec import JobSpec, JobState
from vq.web import authn, fleet_audit

RouteContract = tuple[
    tuple[str, ...],
    str,
    str,
    bool,
    str,
    int,
    bool,
]


@pytest.fixture
def web_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "multi-user"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "config"))
    monkeypatch.setattr(
        config,
        "SYSTEM_CONFIG_PATH",
        tmp_path / "missing-system-config.toml",
    )
    monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
    monkeypatch.delenv("VQ_WEB_FLEET", raising=False)
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


def _write_spec(jobid: str) -> JobSpec:
    workspace = paths.jobs_dir() / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(
        id=jobid,
        command=["python", "contract.py"],
        cwd=str(workspace),
        cpus=2,
        state=JobState.RUNNING,
    )
    spec.write(paths.spec_path(jobid))
    return spec


def _response_class_name(route: APIRoute) -> str:
    response_class = route.response_class
    if hasattr(response_class, "__name__"):
        return response_class.__name__
    return f"default:{response_class.value.__name__}"


def _route_contract(app) -> list[RouteContract]:
    return [
        (
            tuple(sorted(route.methods)),
            route.path,
            route.name,
            route.include_in_schema,
            _response_class_name(route),
            len(route.dependencies),
            inspect.iscoroutinefunction(route.endpoint),
        )
        for route in app.routes
        if isinstance(route, APIRoute)
    ]


_SINGLE_HOST_ROUTES: list[RouteContract] = [
    (("GET",), "/", "root", False, "default:JSONResponse", 0, False),
    (("GET",), "/queue", "queue_page", True, "HTMLResponse", 0, False),
    (
        ("GET",),
        "/queue/_table",
        "queue_table_fragment",
        False,
        "HTMLResponse",
        0,
        False,
    ),
    (
        ("GET",),
        "/jobs/{jobid}",
        "job_detail",
        True,
        "HTMLResponse",
        0,
        False,
    ),
    (
        ("GET",),
        "/jobs/{jobid}/_log",
        "job_log_fragment",
        False,
        "HTMLResponse",
        0,
        False,
    ),
    (
        ("GET",),
        "/health/live",
        "health_live",
        True,
        "PlainTextResponse",
        0,
        False,
    ),
    (
        ("GET",),
        "/health/ready",
        "health_ready",
        True,
        "PlainTextResponse",
        0,
        False,
    ),
    (
        ("GET",),
        "/api/v1/queue",
        "api_queue",
        True,
        "default:JSONResponse",
        0,
        False,
    ),
    (
        ("GET",),
        "/api/v1/jobs/{jobid}",
        "api_job_detail",
        True,
        "default:JSONResponse",
        0,
        False,
    ),
    (
        ("POST",),
        "/api/v1/jobs/{jobid}/kill",
        "api_kill",
        True,
        "PlainTextResponse",
        1,
        False,
    ),
    (
        ("POST",),
        "/api/v1/jobs/{jobid}/pause",
        "api_pause",
        True,
        "PlainTextResponse",
        1,
        False,
    ),
    (
        ("POST",),
        "/api/v1/jobs/{jobid}/resume",
        "api_resume",
        True,
        "PlainTextResponse",
        1,
        False,
    ),
    (
        ("POST",),
        "/api/v1/queue/pause",
        "api_queue_pause",
        True,
        "PlainTextResponse",
        1,
        False,
    ),
    (
        ("POST",),
        "/api/v1/queue/resume",
        "api_queue_resume",
        True,
        "PlainTextResponse",
        1,
        False,
    ),
    (
        ("POST",),
        "/api/v1/queue/clear-failed",
        "api_queue_clear_failed",
        True,
        "PlainTextResponse",
        1,
        False,
    ),
]

_FLEET_ROUTES: list[RouteContract] = [
    (("GET",), "/fleet", "fleet_page", True, "HTMLResponse", 0, False),
    (
        ("GET",),
        "/fleet/_grid",
        "fleet_grid_fragment",
        False,
        "HTMLResponse",
        0,
        False,
    ),
    (
        ("GET",),
        "/fleet/jobs",
        "fleet_jobs_page",
        True,
        "HTMLResponse",
        0,
        False,
    ),
    (
        ("GET",),
        "/fleet/jobs/_table",
        "fleet_jobs_table_fragment",
        False,
        "HTMLResponse",
        0,
        False,
    ),
    (
        ("GET",),
        "/fleet/jobs/{jobid}",
        "fleet_job_detail",
        True,
        "HTMLResponse",
        0,
        False,
    ),
    (
        ("POST",),
        "/fleet/_refresh",
        "fleet_refresh_now",
        False,
        "PlainTextResponse",
        0,
        False,
    ),
    (
        ("GET",),
        "/fleet/doctor",
        "fleet_doctor_page",
        True,
        "HTMLResponse",
        0,
        False,
    ),
    (
        ("GET",),
        "/fleet/doctor/_board",
        "fleet_doctor_board",
        False,
        "HTMLResponse",
        0,
        False,
    ),
    (
        ("POST",),
        "/fleet/doctor/_refresh",
        "fleet_doctor_refresh",
        False,
        "PlainTextResponse",
        0,
        False,
    ),
    (
        ("GET",),
        "/api/v1/fleet",
        "api_fleet",
        True,
        "default:JSONResponse",
        0,
        False,
    ),
    (
        ("GET",),
        "/api/v1/fleet/doctor",
        "api_fleet_doctor",
        True,
        "default:JSONResponse",
        0,
        False,
    ),
    (
        ("GET",),
        "/fleet/login",
        "fleet_login_page",
        True,
        "HTMLResponse",
        0,
        False,
    ),
    (
        ("POST",),
        "/fleet/login",
        "fleet_login_submit",
        True,
        "default:JSONResponse",
        0,
        True,
    ),
    (
        ("POST",),
        "/fleet/logout",
        "fleet_logout",
        True,
        "default:JSONResponse",
        0,
        False,
    ),
    (
        ("POST",),
        "/fleet/jobs/{jobid}/kill",
        "fleet_job_kill",
        False,
        "default:JSONResponse",
        0,
        True,
    ),
    (
        ("POST",),
        "/fleet/jobs/{jobid}/pause",
        "fleet_job_pause",
        False,
        "default:JSONResponse",
        0,
        True,
    ),
    (
        ("POST",),
        "/fleet/jobs/{jobid}/resume",
        "fleet_job_resume",
        False,
        "default:JSONResponse",
        0,
        True,
    ),
    (
        ("GET",),
        "/fleet/audit",
        "fleet_audit_page",
        True,
        "HTMLResponse",
        0,
        False,
    ),
    (
        ("GET",),
        "/api/v1/fleet/audit",
        "api_fleet_audit",
        True,
        "default:JSONResponse",
        0,
        False,
    ),
    (
        ("GET",),
        "/api/v1/fleet/jobs",
        "api_fleet_jobs",
        True,
        "default:JSONResponse",
        0,
        False,
    ),
]


def test_single_host_route_registration_contract(web_state: Path) -> None:
    app = web_mod.create_app()

    assert [
        (type(route).__name__, route.path, route.name)
        for route in app.routes[:5]
    ] == [
        ("Route", "/openapi.json", "openapi"),
        ("Route", "/docs", "swagger_ui_html"),
        ("Route", "/docs/oauth2-redirect", "swagger_ui_redirect"),
        ("Route", "/redoc", "redoc_html"),
        ("Mount", "/static", "static"),
    ]
    assert _route_contract(app) == _SINGLE_HOST_ROUTES
    assert not hasattr(app.state, "fleet_cache")
    assert not hasattr(app.state, "doctor_cache")


def test_single_host_route_description_contract(web_state: Path) -> None:
    app = web_mod.create_app()

    assert [
        (route.path, route.description)
        for route in app.routes
        if isinstance(route, APIRoute) and route.description
    ] == [
        (
            "/queue/_table",
            "HTML fragment that htmx polls every few seconds to refresh\n"
            "just the table without reloading the whole page.",
        ),
        ("/health/live", "Web service is up. Always 200 when reachable."),
        (
            "/health/ready",
            "Daemon pidfile has a live PID + queue root exists. 503 otherwise.",
        ),
        (
            "/api/v1/jobs/{jobid}/kill",
            "Cancel a job. SIGTERM if running / suspended; mark KILLED if\n"
            "pending. Returns a human-readable confirmation string.",
        ),
    ]


def test_single_host_lifespan_never_constructs_fleet_state(
    web_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_cache():
        raise AssertionError("fleet cache constructed while fleet mode is off")

    monkeypatch.setattr(web_mod.fleet_mod, "FleetCache", unexpected_cache)
    monkeypatch.setattr(web_mod.fleet_mod, "DoctorCache", unexpected_cache)

    with TestClient(web_mod.create_app()):
        pass


def test_fleet_registration_is_an_ordered_additive_suffix(
    web_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VQ_WEB_FLEET", "1")

    app = web_mod.create_app()

    assert _route_contract(app) == [
        *_SINGLE_HOST_ROUTES,
        *_FLEET_ROUTES,
    ]
    assert isinstance(app.state.fleet_cache, web_mod.fleet_mod.FleetCache)
    assert isinstance(app.state.doctor_cache, web_mod.fleet_mod.DoctorCache)


def test_fleet_route_description_contract(
    web_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VQ_WEB_FLEET", "1")

    app = web_mod.create_app()

    assert [
        (route.path, route.description)
        for route in app.routes
        if (
            isinstance(route, APIRoute)
            and route.path.startswith("/fleet")
            and route.description
        )
    ] == [
        (
            "/fleet/jobs/{jobid}",
            "M1 drill-down: spec + tails + events fetched live from the\n"
            "owning host (driver-aware). The owning queue host comes from\n"
            "the ?host= parameter (the table links carry it) with the\n"
            "cached snapshot as fallback for hand-typed URLs.",
        ),
        (
            "/fleet/_refresh",
            "Debounced operator-triggered sweep. The grid's htmx poll\n"
            "picks up the new snapshot when the sweep lands.",
        ),
    ]


def test_fleet_lifespan_starts_only_the_fleet_cache(
    web_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    class FakeFleetCache:
        # v0.25.0: create_app() passes the resolved sweep interval rather
        # than letting the cache re-derive it, so the app and its poller
        # cannot disagree about how often the fleet is swept.
        def __init__(self, interval_seconds: int | None = None) -> None:
            events.append(f"fleet-init:{interval_seconds}")

        def start(self) -> None:
            events.append("fleet-start")

        def stop(self) -> None:
            events.append("fleet-stop")

    class FakeDoctorCache:
        pass

    monkeypatch.setenv("VQ_WEB_FLEET", "1")
    monkeypatch.setattr(web_mod.fleet_mod, "FleetCache", FakeFleetCache)
    monkeypatch.setattr(web_mod.fleet_mod, "DoctorCache", FakeDoctorCache)
    app = web_mod.create_app()

    with TestClient(app):
        assert events == ["fleet-init:30", "fleet-start"]

    assert events == ["fleet-init:30", "fleet-start", "fleet-stop"]


_SINGLE_TEMPLATE_CONTEXTS = {
    "/queue": (
        "queue.html",
        {
            "daemon_pid",
            "daemon_running",
            "filtered_count",
            "host_options",
            "host_pressure_active",
            "host_pressure_paused_jobs",
            "host_pressure_pct",
            "jobs",
            "query",
            "row_limit_options",
            "shown_count",
            "sort_href",
            "sort_label",
            "specs",
            "state_options",
            "summary",
            "table_refresh_url",
            "total_count",
        },
    ),
    "/queue/_table": (
        "queue_table.html",
        {
            "filtered_count",
            "host_options",
            "host_pressure_active",
            "host_pressure_paused_jobs",
            "host_pressure_pct",
            "jobs",
            "query",
            "row_limit_options",
            "shown_count",
            "sort_href",
            "sort_label",
            "specs",
            "state_options",
            "table_refresh_url",
            "total_count",
        },
    ),
    "/jobs/contractjob/_log": (
        "job_log_fragment.html",
        {"spec", "stderr_text", "stdout_text"},
    ),
    "/jobs/contractjob": (
        "job_detail.html",
        {
            "daemon_pid",
            "daemon_running",
            "events",
            "job",
            "spec",
            "stderr_text",
            "stdout_text",
        },
    ),
}


_FLEET_TEMPLATE_CONTEXTS = {
    # v0.25.0 added the fleet-level roll-up and the snapshot-freshness
    # keys. Freshness in particular is load-bearing: the sweep thread
    # swallows its exceptions and keeps serving the last good snapshot,
    # so without these the page cannot tell a dead poller from a healthy
    # one -- it just goes on animating.
    "/fleet": (
        "fleet.html",
        {
            "cards",
            "daemon_pid",
            "daemon_running",
            "duplicate_enrolments",
            "identity",
            "snapshot",
            "snapshot_age",
            "snapshot_age_seconds",
            "snapshot_stale",
            "refreshing",
            "refresh_elapsed",
            "refresh_error",
            "interval_seconds",
            "stale_after_seconds",
            "summary",
        },
    ),
    "/fleet/_grid": (
        "fleet_grid.html",
        {
            "cards",
            "duplicate_enrolments",
            "snapshot",
            "snapshot_age",
            "snapshot_age_seconds",
            "snapshot_stale",
            "refreshing",
            "refresh_elapsed",
            "refresh_error",
            "interval_seconds",
            "stale_after_seconds",
            "summary",
        },
    ),
    # v0.25.x added sortable columns: every rendered column is clickable,
    # since a header that looks like the others but does not respond
    # reads as a broken control.
    "/fleet/jobs": (
        "fleet_jobs.html",
        {
            "daemon_pid",
            "daemon_running",
            "filtered_count",
            "host_options",
            "identity",
            "jobs",
            "query",
            "row_limit_options",
            "shown_count",
            "snapshot",
            "sort_href",
            "sort_label",
            "state_options",
            "table_refresh_url",
            "total_count",
        },
    ),
    "/fleet/jobs/_table": (
        "fleet_jobs_table.html",
        {
            "filtered_count",
            "host_options",
            "jobs",
            "query",
            "row_limit_options",
            "shown_count",
            "sort_href",
            "sort_label",
            "snapshot",
            "state_options",
            "table_refresh_url",
            "total_count",
        },
    ),
    "/fleet/jobs/contractjob?host=localhost": (
        "fleet_job_detail.html",
        {
            "can_act",
            "daemon_pid",
            "daemon_running",
            "detail",
            "error",
            "flash",
            "identity",
            "jobid",
            "queue_host",
        },
    ),
    "/fleet/doctor": (
        "fleet_doctor.html",
        {
            "bad_count",
            "daemon_pid",
            "daemon_running",
            "gathered_at",
            "identity",
            "ok_count",
            "refreshing",
            "results",
        },
    ),
    "/fleet/doctor/_board": (
        "fleet_doctor_board.html",
        {"bad_count", "gathered_at", "ok_count", "refreshing", "results"},
    ),
    "/fleet/login": (
        "fleet_login.html",
        {
            "auth_configured",
            "daemon_pid",
            "daemon_running",
            "error",
            "next",
        },
    ),
    "/fleet/audit": (
        "fleet_audit.html",
        {"daemon_pid", "daemon_running", "identity", "records"},
    ),
}


def _capture_template_contexts(
    monkeypatch: pytest.MonkeyPatch,
) -> list[tuple[str, set[str], int]]:
    calls: list[tuple[str, set[str], int]] = []

    def capture(
        request,
        name: str,
        context: dict[str, object],
        *,
        status_code: int = 200,
        **_kwargs,
    ) -> HTMLResponse:
        calls.append((name, set(context), status_code))
        return HTMLResponse(name, status_code=status_code)

    monkeypatch.setattr(web_mod.TEMPLATES, "TemplateResponse", capture)
    return calls


def test_single_host_template_context_contract(
    web_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_spec("contractjob")
    calls = _capture_template_contexts(monkeypatch)
    client = TestClient(web_mod.create_app())

    for path, expected in _SINGLE_TEMPLATE_CONTEXTS.items():
        calls.clear()
        response = client.get(path)
        assert response.status_code == 200
        assert calls == [(expected[0], expected[1], 200)]


def test_fleet_template_context_contract(
    web_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VQ_WEB_FLEET", "1")
    monkeypatch.setattr(
        web_mod.fleet_mod,
        "fetch_job_detail",
        lambda *_args, **_kwargs: ({"id": "contractjob"}, None),
    )
    calls = _capture_template_contexts(monkeypatch)
    client = TestClient(web_mod.create_app())

    for path, expected in _FLEET_TEMPLATE_CONTEXTS.items():
        calls.clear()
        response = client.get(path)
        assert response.status_code == 200
        assert calls == [(expected[0], expected[1], 200)]


def test_core_and_warming_fleet_response_contract(
    web_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    single = TestClient(web_mod.create_app())
    root = single.get("/", follow_redirects=False)
    assert (root.status_code, root.headers["location"]) == (302, "/queue")
    live = single.get("/health/live")
    assert (
        live.status_code,
        live.text,
        live.headers["content-type"],
    ) == (200, "ok", "text/plain; charset=utf-8")
    missing = single.get("/api/v1/jobs/missing")
    assert (missing.status_code, missing.json()) == (
        404,
        {"detail": "no such job: missing"},
    )

    monkeypatch.setenv("VQ_WEB_FLEET", "1")
    fleet = TestClient(web_mod.create_app())
    assert fleet.get("/api/v1/fleet").json() == {
        "gathered_at": None,
        "warming_up": True,
        "hosts": [],
        "refresh": {
            "refreshing": False, "started_at": None, "elapsed_seconds": None,
            "finished_at": None, "error": None,
        },
    }
    assert fleet.get("/api/v1/fleet/doctor").json() == {
        "gathered_at": None,
        "refreshing": False,
        "results": [],
    }
    assert fleet.get("/api/v1/fleet/jobs").json() == {
        "gathered_at": None,
        "warming_up": True,
        "count": 0,
        "jobs": [],
    }


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/jobs/contractjob/kill",
        "/api/v1/jobs/contractjob/pause",
        "/api/v1/jobs/contractjob/resume",
        "/api/v1/queue/pause",
        "/api/v1/queue/resume",
        "/api/v1/queue/clear-failed",
    ],
)
def test_single_host_write_auth_dependency_contract(
    web_state: Path,
    path: str,
) -> None:
    response = TestClient(web_mod.create_app()).post(path)

    assert (response.status_code, response.json()) == (
        503,
        {
            "detail": (
                "web auth not configured: no token file at "
                f"{auth.web_token_path()}. Run `vq web init-token`."
            )
        },
    )


def test_fleet_jobs_api_preserves_ignored_row_limit(
    web_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VQ_WEB_FLEET", "1")
    client = TestClient(web_mod.create_app())
    rows = [
        {
            "id": f"job-{index:02d}",
            "queue_host": "host_a",
            "state": "running",
        }
        for index in range(51)
    ]
    client.app.state.fleet_cache.set_snapshot(
        web_mod.fleet_mod.FleetSnapshot(
            gathered_at="2026-08-02T12:00:00+00:00",
            duration_seconds=1.0,
            hosts=[
                web_mod.fleet_mod.HostSnapshot(
                    host="host_a",
                    overview=HostOverview(host="host_a"),
                    jobs=rows,
                )
            ],
        )
    )

    payload = client.get("/api/v1/fleet/jobs?limit=50").json()

    assert payload["count"] == 51
    assert payload["jobs"] == rows


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("get", "/fleet/_grid"),
        ("get", "/fleet/jobs/_table"),
        ("post", "/fleet/_refresh"),
        ("get", "/fleet/doctor/_board"),
        ("post", "/fleet/doctor/_refresh"),
        ("get", "/api/v1/fleet"),
        ("get", "/api/v1/fleet/doctor"),
        ("get", "/api/v1/fleet/jobs"),
        ("get", "/api/v1/fleet/audit"),
    ],
)
def test_fleet_fragment_and_api_auth_gate_contract(
    web_state: Path,
    monkeypatch: pytest.MonkeyPatch,
    method: str,
    path: str,
) -> None:
    monkeypatch.setenv("VQ_WEB_FLEET", "1")
    authn.add_user("viewer", "pw", "viewer")
    client = TestClient(web_mod.create_app())

    response = getattr(client, method)(path)

    assert (response.status_code, response.json()) == (
        401,
        {"detail": "login or bearer token required"},
    )


@pytest.mark.parametrize(
    "path",
    [
        "/fleet",
        "/fleet/jobs?host=host_a&q=water",
        "/fleet/jobs/contractjob?host=host_a",
        "/fleet/doctor",
        "/fleet/audit",
    ],
)
def test_fleet_page_auth_redirect_contract(
    web_state: Path,
    monkeypatch: pytest.MonkeyPatch,
    path: str,
) -> None:
    monkeypatch.setenv("VQ_WEB_FLEET", "1")
    authn.add_user("viewer", "pw", "viewer")
    client = TestClient(web_mod.create_app())

    response = client.get(path, follow_redirects=False)

    target = path.replace("/", "%2F").replace("?", "%3F").replace("=", "%3D")
    target = target.replace("&", "%26")
    assert (response.status_code, response.headers["location"]) == (
        302,
        f"/fleet/login?next={target}",
    )


def _without_timestamp(record: dict[str, object]) -> dict[str, object]:
    return {key: value for key, value in record.items() if key != "ts"}


def test_fleet_login_cookie_and_audit_contract(
    web_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VQ_WEB_FLEET", "1")
    authn.add_user("alice", "pw", "operator")
    client = TestClient(web_mod.create_app())

    denied = client.post(
        "/fleet/login",
        data={"user": "alice", "password": "wrong", "next": "/fleet/jobs"},
        follow_redirects=False,
    )
    accepted = client.post(
        "/fleet/login",
        data={"user": "alice", "password": "pw", "next": "/fleet/jobs"},
        follow_redirects=False,
    )

    assert (denied.status_code, "invalid user or password" in denied.text) == (
        401,
        True,
    )
    assert (accepted.status_code, accepted.headers["location"]) == (
        303,
        "/fleet/jobs",
    )
    cookie = accepted.headers["set-cookie"]
    assert cookie.startswith(f"{authn.SESSION_COOKIE}=")
    assert "; HttpOnly" in cookie
    assert f"; Max-Age={authn.SESSION_TTL_SECONDS}" in cookie
    assert "; Path=/" in cookie
    assert "; SameSite=lax" in cookie

    records = [_without_timestamp(record) for record in fleet_audit.read_audit()]
    assert records[:2] == [
        {
            "user": "alice",
            "role": "operator",
            "action": "login",
            "jobid": None,
            "host": None,
            "outcome": "ok",
        },
        {
            "user": "alice",
            "role": None,
            "action": "login",
            "jobid": None,
            "host": None,
            "outcome": "denied",
        },
    ]

    logout = client.post("/fleet/logout", follow_redirects=False)
    assert (logout.status_code, logout.headers["location"]) == (
        303,
        "/fleet/login",
    )
    cleared = logout.headers["set-cookie"]
    assert cleared.startswith(f'{authn.SESSION_COOKIE}=""')
    assert "; Max-Age=0" in cleared
    assert "; Path=/" in cleared
    assert "; SameSite=lax" in cleared


def test_fleet_viewer_forbidden_action_audit_contract(
    web_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VQ_WEB_FLEET", "1")
    authn.add_user("reader", "pw", "viewer")
    client = TestClient(web_mod.create_app())
    login = client.post(
        "/fleet/login",
        data={"user": "reader", "password": "pw", "next": "/fleet"},
        follow_redirects=False,
    )
    assert login.status_code == 303

    response = client.post(
        "/fleet/jobs/contractjob/kill",
        data={"host": "localhost"},
        follow_redirects=False,
    )

    assert (response.status_code, response.json()) == (
        403,
        {"detail": "operator role required"},
    )
    assert _without_timestamp(fleet_audit.read_audit()[0]) == {
        "user": "reader",
        "role": "viewer",
        "action": "kill",
        "jobid": "contractjob",
        "host": None,
        "outcome": "forbidden: viewer role",
    }


@pytest.mark.parametrize(
    ("action", "adapter_name", "message", "expected_kwargs"),
    [
        (
            "kill",
            "kill_job",
            "killed contract job",
            {
                "multi_user": False,
                "reason": "contract",
                "via": "fleet-web",
            },
        ),
        (
            "pause",
            "pause_job",
            "paused contract job",
            {"multi_user": False},
        ),
        (
            "resume",
            "resume_job",
            "resumed contract job",
            {"multi_user": False},
        ),
    ],
)
def test_fleet_operator_action_redirect_and_audit_contract(
    web_state: Path,
    monkeypatch: pytest.MonkeyPatch,
    action: str,
    adapter_name: str,
    message: str,
    expected_kwargs: dict[str, object],
) -> None:
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def adapter(*args, **kwargs) -> str:
        calls.append((args, kwargs))
        return message

    monkeypatch.setenv("VQ_WEB_FLEET", "1")
    authn.add_user("operator", "pw", "operator")
    monkeypatch.setattr(
        web_mod.fleet_mod,
        "resolve_owner_host",
        lambda _cfg, _queue_host: "localhost",
    )
    monkeypatch.setattr(
        web_mod,
        adapter_name,
        adapter,
    )
    client = TestClient(web_mod.create_app())
    login = client.post(
        "/fleet/login",
        data={"user": "operator", "password": "pw", "next": "/fleet"},
        follow_redirects=False,
    )
    assert login.status_code == 303

    response = client.post(
        f"/fleet/jobs/contractjob/{action}",
        data={"host": "localhost", "reason": "contract"},
        follow_redirects=False,
    )

    assert calls == [(("localhost", "contractjob"), expected_kwargs)]
    assert (response.status_code, response.headers["location"]) == (
        303,
        "/fleet/jobs/contractjob?host=localhost&msg=ok%3A%20"
        f"{message.replace(' ', '%20')}",
    )
    assert _without_timestamp(fleet_audit.read_audit()[0]) == {
        "user": "operator",
        "role": "operator",
        "action": action,
        "jobid": "contractjob",
        "host": "localhost",
        "outcome": f"ok: {message}",
    }
