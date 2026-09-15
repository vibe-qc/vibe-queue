"""Fleet-only FastAPI route registration for the vq web app."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import parse_qs, quote

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import (
    HTMLResponse,
    PlainTextResponse,
    RedirectResponse,
    Response,
)
from fastapi.templating import Jinja2Templates
from starlette.concurrency import run_in_threadpool

from vq import auth, config
from vq.web import authn, fleet_audit
from vq.web import fleet as fleet_mod
from vq.web import view_context as web_view


@dataclass(frozen=True)
class FleetRouteServices:
    """Effectful adapters kept late-bound by the owning app factory."""

    templates: Jinja2Templates
    is_daemon_running: Callable[[], bool]
    read_pidfile: Callable[[], int | None]
    is_local_host: Callable[[str], bool]
    safe_next: Callable[[str | None], str]
    row_matches: Callable[[dict, dict[str, str]], bool]
    kill_job: Callable[..., str]
    pause_job: Callable[..., str]
    resume_job: Callable[..., str]


def register_fleet_routes(
    app: FastAPI,
    *,
    multi_user: bool,
    cache: fleet_mod.FleetCache,
    doctor_cache: fleet_mod.DoctorCache,
    services: FleetRouteServices,
) -> None:
    """Register the established fleet suffix over factory-owned caches."""

    open_identity: dict[str, object] = {
        "user": None,
        "role": "viewer",
        "open_mode": True,
    }

    def _identity(request: Request) -> dict[str, str] | None:
        return authn.verify_session(request.cookies.get(authn.SESSION_COOKIE))

    def _page_gate(request: Request) -> dict[str, object] | RedirectResponse:
        """Identity for an HTML page; a redirect to the login form
        when auth is on and the session is missing/expired."""
        if not authn.auth_enabled():
            return dict(open_identity)
        ident = _identity(request)
        if ident is None:
            target = request.url.path
            if request.url.query:
                target = f"{target}?{request.url.query}"
            return RedirectResponse(
                f"/fleet/login?next={quote(target, safe='')}",
                status_code=302,
            )
        return dict(ident)

    def _api_gate(request: Request) -> dict[str, object]:
        """Identity for fragments + JSON APIs: session cookie, or
        the existing bearer token as an agent-friendly equivalent.
        401 instead of a redirect — htmx swaps and scripts should
        fail loudly, not render a login page into a table."""
        if not authn.auth_enabled():
            return dict(open_identity)
        ident = _identity(request)
        if ident is not None:
            return dict(ident)
        authz = request.headers.get("authorization", "")
        if authz.lower().startswith("bearer "):
            expected = auth.load_token()
            if expected is not None and auth.constant_time_eq(
                authz[7:], expected
            ):
                return {"user": "bearer-token", "role": "admin"}
        raise HTTPException(
            status_code=401,
            detail="login or bearer token required",
        )

    async def _form_fields(request: Request) -> dict[str, str]:
        """Parse an application/x-www-form-urlencoded body without
        the python-multipart dependency."""
        chunks = bytearray()
        async for chunk in request.stream():
            if len(chunks) + len(chunk) > 16384:
                raise HTTPException(status_code=413, detail="form too large")
            chunks.extend(chunk)
        body = chunks.decode("utf-8", errors="replace")
        return {key: value[0] for key, value in parse_qs(body).items() if value}

    def _fleet_query(request: Request) -> dict[str, str]:
        return web_view.fleet_query_state(request.query_params)

    def _fleet_jobs_context(request: Request) -> dict[str, object]:
        return web_view.fleet_jobs_context(
            cache.snapshot(),
            request.query_params,
        )

    def _fleet_grid_context() -> dict[str, object]:
        # The cache's own interval, not a constant: the staleness
        # threshold has to track how often this console actually sweeps,
        # or a site that configured a 5-minute interval would see a
        # permanent "stale" banner.
        return web_view.fleet_grid_context(
            cache.snapshot(), interval_seconds=cache.interval_seconds,
            refresh_state=cache.refresh_state()
        )

    def _daemon_badge_context() -> dict[str, object]:
        return {
            "daemon_running": services.is_daemon_running(),
            "daemon_pid": services.read_pidfile(),
        }

    @app.get("/fleet", response_class=HTMLResponse)
    def fleet_page(request: Request) -> Response:
        ident = _page_gate(request)
        if isinstance(ident, RedirectResponse):
            return ident
        context = _fleet_grid_context()
        context["identity"] = ident
        context.update(_daemon_badge_context())
        return services.templates.TemplateResponse(
            request,
            "fleet.html",
            context,
        )

    @app.get(
        "/fleet/_grid",
        response_class=HTMLResponse,
        include_in_schema=False,
    )
    def fleet_grid_fragment(request: Request) -> HTMLResponse:
        _api_gate(request)
        return services.templates.TemplateResponse(
            request,
            "fleet_grid.html",
            _fleet_grid_context(),
        )

    @app.get("/fleet/jobs", response_class=HTMLResponse)
    def fleet_jobs_page(request: Request) -> Response:
        ident = _page_gate(request)
        if isinstance(ident, RedirectResponse):
            return ident
        context = _fleet_jobs_context(request)
        context["identity"] = ident
        context.update(_daemon_badge_context())
        return services.templates.TemplateResponse(
            request,
            "fleet_jobs.html",
            context,
        )

    @app.get(
        "/fleet/jobs/_table",
        response_class=HTMLResponse,
        include_in_schema=False,
    )
    def fleet_jobs_table_fragment(request: Request) -> HTMLResponse:
        _api_gate(request)
        return services.templates.TemplateResponse(
            request,
            "fleet_jobs_table.html",
            _fleet_jobs_context(request),
        )

    @app.get("/fleet/jobs/{jobid}", response_class=HTMLResponse)
    def fleet_job_detail(request: Request, jobid: str) -> Response:
        """M1 drill-down: spec + tails + events fetched live from the
        owning host (driver-aware). The owning queue host comes from
        the ?host= parameter (the table links carry it) with the
        cached snapshot as fallback for hand-typed URLs."""
        ident = _page_gate(request)
        if isinstance(ident, RedirectResponse):
            return ident
        snapshot = cache.snapshot()
        queue_host = (request.query_params.get("host") or "").strip()
        if not queue_host and snapshot is not None:
            for row in snapshot.jobs:
                if row.get("id") == jobid:
                    queue_host = str(row.get("queue_host") or "")
                    break
        if not queue_host:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"job {jobid!r} not in the fleet snapshot; pass "
                    "?host=HOST explicitly"
                ),
            )
        payload, error = fleet_mod.fetch_job_detail(
            config.load_config(),
            queue_host,
            jobid,
            multi_user=multi_user,
        )
        context: dict[str, object] = {
            "jobid": jobid,
            "queue_host": queue_host,
            "detail": payload,
            "error": error,
            "identity": ident,
            "can_act": (
                authn.auth_enabled()
                and authn.role_at_least(str(ident.get("role")), "operator")
            ),
            "flash": (request.query_params.get("msg") or "").strip() or None,
        }
        context.update(_daemon_badge_context())
        return services.templates.TemplateResponse(
            request,
            "fleet_job_detail.html",
            context,
        )

    @app.post(
        "/fleet/_refresh",
        response_class=PlainTextResponse,
        include_in_schema=False,
    )
    def fleet_refresh_now(request: Request) -> str:
        """Debounced operator-triggered sweep. The grid's htmx poll
        picks up the new snapshot when the sweep lands."""
        _api_gate(request)
        started = cache.refresh_async()
        return "sweep started" if started else "sweep already fresh"

    def _doctor_context() -> dict[str, object]:
        return web_view.doctor_context(doctor_cache.state())

    @app.get("/fleet/doctor", response_class=HTMLResponse)
    def fleet_doctor_page(request: Request) -> Response:
        ident = _page_gate(request)
        if isinstance(ident, RedirectResponse):
            return ident
        context = _doctor_context()
        context["identity"] = ident
        context.update(_daemon_badge_context())
        return services.templates.TemplateResponse(
            request,
            "fleet_doctor.html",
            context,
        )

    @app.get(
        "/fleet/doctor/_board",
        response_class=HTMLResponse,
        include_in_schema=False,
    )
    def fleet_doctor_board(request: Request) -> HTMLResponse:
        _api_gate(request)
        return services.templates.TemplateResponse(
            request,
            "fleet_doctor_board.html",
            _doctor_context(),
        )

    @app.post(
        "/fleet/doctor/_refresh",
        response_class=PlainTextResponse,
        include_in_schema=False,
    )
    def fleet_doctor_refresh(request: Request) -> str:
        _api_gate(request)
        started = doctor_cache.refresh_async()
        return "doctor sweep started" if started else "sweep already fresh"

    @app.get("/api/v1/fleet")
    def api_fleet(request: Request) -> dict[str, object]:
        _api_gate(request)
        snapshot = cache.snapshot()
        if snapshot is None:
            return {"gathered_at": None, "warming_up": True, "hosts": [],
                    "refresh": cache.refresh_state()}
        return {**snapshot.to_json(), "refresh": cache.refresh_state()}

    @app.get("/api/v1/fleet/doctor")
    def api_fleet_doctor(request: Request) -> dict[str, object]:
        _api_gate(request)
        state = doctor_cache.state()
        return {
            "gathered_at": state["gathered_at"],
            "refreshing": state["refreshing"],
            "results": state["results"] or [],
        }

    @app.get("/fleet/login", response_class=HTMLResponse)
    def fleet_login_page(request: Request) -> HTMLResponse:
        context: dict[str, object] = {
            "next": services.safe_next(request.query_params.get("next")),
            "error": None,
            "auth_configured": authn.auth_enabled(),
        }
        context.update(_daemon_badge_context())
        return services.templates.TemplateResponse(
            request,
            "fleet_login.html",
            context,
        )

    @app.post("/fleet/login")
    async def fleet_login_submit(request: Request) -> Response:
        form = await _form_fields(request)
        user = form.get("user", "").strip()
        password = form.get("password", "")
        next_path = services.safe_next(form.get("next"))
        if (len(user) > authn.MAX_LOGIN_USER_LENGTH
                or len(password) > authn.MAX_LOGIN_PASSWORD_LENGTH):
            raise HTTPException(status_code=400, detail="login fields too long")
        peer = request.client.host if request.client else "unknown"
        try:
            authenticated = await run_in_threadpool(authn.authenticate_login, user, password, peer)
        except authn.LoginRateLimited as exc:
            raise HTTPException(status_code=429, detail=str(exc),
                                headers={"Retry-After": str(exc.retry_after)}) from None
        except (OSError, ValueError, sqlite3.Error):
            raise HTTPException(status_code=503, detail="login state unavailable") from None
        if authenticated is None:
            fleet_audit.append_audit(
                user=user or None,
                role=None,
                action="login",
                jobid=None,
                host=None,
                outcome="denied",
            )
            context: dict[str, object] = {
                "next": next_path,
                "error": "invalid user or password",
                "auth_configured": authn.auth_enabled(),
            }
            context.update(_daemon_badge_context())
            return services.templates.TemplateResponse(
                request,
                "fleet_login.html",
                context,
                status_code=401,
            )
        role, token = authenticated
        fleet_audit.append_audit(
            user=user,
            role=role,
            action="login",
            jobid=None,
            host=None,
            outcome="ok",
        )
        response = RedirectResponse(next_path, status_code=303)
        response.set_cookie(
            authn.SESSION_COOKIE,
            token,
            max_age=authn.SESSION_TTL_SECONDS,
            httponly=True,
            samesite="lax",
            secure=request.url.scheme == "https",
        )
        return response

    @app.post("/fleet/logout")
    def fleet_logout(request: Request) -> RedirectResponse:
        try:
            authn.revoke_session(request.cookies.get(authn.SESSION_COOKIE))
        except (OSError, sqlite3.Error):
            raise HTTPException(status_code=503, detail="session revocation unavailable") from None
        response = RedirectResponse("/fleet/login", status_code=303)
        response.delete_cookie(authn.SESSION_COOKIE, httponly=True,
                               secure=request.url.scheme == "https", samesite="lax")
        return response

    def _run_job_action(
        ident: dict[str, object],
        action: str,
        queue_host: str,
        jobid: str,
        reason: str | None,
    ) -> str:
        """Execute kill/pause/resume on the owning host and return a
        one-line outcome. Never raises — the outcome string carries
        the error, and every attempt lands in the audit trail."""
        cfg = config.load_config()
        owner = fleet_mod.resolve_owner_host(cfg, queue_host)
        try:
            if owner is None:
                raise ValueError(
                    f"scheduler host {queue_host!r} has no scheduler_driver"
                )
            if services.is_local_host(owner):
                if action == "kill":
                    message = services.kill_job(
                        "localhost",
                        jobid,
                        multi_user=multi_user,
                        reason=reason,
                        via="fleet-web",
                    )
                elif action == "pause":
                    message = services.pause_job(
                        "localhost",
                        jobid,
                        multi_user=multi_user,
                    )
                else:
                    message = services.resume_job(
                        "localhost",
                        jobid,
                        multi_user=multi_user,
                    )
            else:
                from vq import transport  # noqa: PLC0415 — remote leg only

                argv: list[str] = [action]
                if action == "kill" and reason:
                    argv.extend(["--reason", reason])
                argv.extend(["localhost", jobid])
                message = transport.run_remote_vq(
                    cfg.host(owner),
                    *argv,
                ).stdout.strip()
            outcome = f"ok: {message.splitlines()[0] if message else action}"
        except Exception as exc:
            outcome = f"error: {exc}"
        fleet_audit.append_audit(
            user=(
                str(ident.get("user"))
                if ident.get("user") is not None
                else None
            ),
            role=str(ident.get("role")),
            action=action,
            jobid=jobid,
            host=queue_host,
            outcome=outcome,
        )
        return outcome

    async def _job_action_route(
        request: Request,
        jobid: str,
        action: str,
    ) -> RedirectResponse:
        if not authn.auth_enabled():
            raise HTTPException(
                status_code=503,
                detail=(
                    "fleet write actions need authentication: create an "
                    "account with `vq web user add USER --role operator`"
                ),
            )
        ident = _identity(request)
        if ident is None:
            raise HTTPException(status_code=401, detail="login required")
        if not authn.role_at_least(str(ident.get("role")), "operator"):
            fleet_audit.append_audit(
                user=str(ident.get("user")),
                role=str(ident.get("role")),
                action=action,
                jobid=jobid,
                host=None,
                outcome="forbidden: viewer role",
            )
            raise HTTPException(
                status_code=403,
                detail="operator role required",
            )
        form = await _form_fields(request)
        queue_host = (form.get("host") or "").strip()
        if not queue_host:
            raise HTTPException(status_code=400, detail="missing host field")
        reason = (form.get("reason") or "").strip() or None
        outcome = _run_job_action(ident, action, queue_host, jobid, reason)
        return RedirectResponse(
            f"/fleet/jobs/{jobid}?host={quote(queue_host, safe='')}"
            f"&msg={quote(outcome, safe='')}",
            status_code=303,
        )

    @app.post("/fleet/jobs/{jobid}/kill", include_in_schema=False)
    async def fleet_job_kill(
        request: Request,
        jobid: str,
    ) -> RedirectResponse:
        return await _job_action_route(request, jobid, "kill")

    @app.post("/fleet/jobs/{jobid}/pause", include_in_schema=False)
    async def fleet_job_pause(
        request: Request,
        jobid: str,
    ) -> RedirectResponse:
        return await _job_action_route(request, jobid, "pause")

    @app.post("/fleet/jobs/{jobid}/resume", include_in_schema=False)
    async def fleet_job_resume(
        request: Request,
        jobid: str,
    ) -> RedirectResponse:
        return await _job_action_route(request, jobid, "resume")

    @app.get("/fleet/audit", response_class=HTMLResponse)
    def fleet_audit_page(request: Request) -> Response:
        ident = _page_gate(request)
        if isinstance(ident, RedirectResponse):
            return ident
        if authn.auth_enabled() and not authn.role_at_least(
            str(ident.get("role")),
            "admin",
        ):
            raise HTTPException(status_code=403, detail="admin role required")
        context: dict[str, object] = {
            "records": fleet_audit.read_audit(limit=200),
            "identity": ident,
        }
        context.update(_daemon_badge_context())
        return services.templates.TemplateResponse(
            request,
            "fleet_audit.html",
            context,
        )

    @app.get("/api/v1/fleet/audit")
    def api_fleet_audit(request: Request) -> dict[str, object]:
        ident = _api_gate(request)
        if authn.auth_enabled() and not authn.role_at_least(
            str(ident.get("role")),
            "admin",
        ):
            raise HTTPException(status_code=403, detail="admin role required")
        return {"records": fleet_audit.read_audit(limit=500)}

    @app.get("/api/v1/fleet/jobs")
    def api_fleet_jobs(request: Request) -> dict[str, object]:
        _api_gate(request)
        snapshot = cache.snapshot()
        query = _fleet_query(request)
        rows = snapshot.jobs if snapshot is not None else []
        filtered = [
            row for row in rows if services.row_matches(row, query)
        ]
        return {
            "gathered_at": snapshot.gathered_at if snapshot else None,
            "warming_up": snapshot is None,
            "count": len(filtered),
            "jobs": filtered,
        }
