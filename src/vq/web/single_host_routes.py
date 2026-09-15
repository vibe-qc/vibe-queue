"""Single-host FastAPI route registration for the vq web app."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.templating import Jinja2Templates

from vq import auth, ownership, paths
from vq.cleanup import parse_age
from vq.config import ConfigError
from vq.listing import queue_handle_for_spec
from vq.pause_resume import PauseError
from vq.spec import JobSpec, JobState
from vq.status import monitoring_payload_for_spec, terminal_diagnosis_for_spec
from vq.web import fleet_audit
from vq.web import view_context as web_view

FAILED_CLEAR_STATES = {
    JobState.FAILED,
    JobState.KILLED,
    JobState.OOM_KILLED,
    JobState.STARVED,
    JobState.TIME_EXCEEDED,
    JobState.ABORTED_BY_QUEUE,
    JobState.INTERRUPTED,
}


@dataclass(frozen=True)
class SingleHostRouteServices:
    """Effectful adapters kept late-bound by the owning app factory."""

    templates: Jinja2Templates
    list_jobs: Callable[..., list[JobSpec]]
    is_daemon_running: Callable[[], bool]
    read_pidfile: Callable[[], int | None]
    read_events: Callable[[Path], list[dict[str, object]]]
    kill_job: Callable[..., str]
    resubmit_local: Callable[..., str]
    pause_job: Callable[..., str]
    resume_job: Callable[..., str]
    pause_all: Callable[..., str]
    resume_all: Callable[..., str]
    delete_job: Callable[..., None]
    tail: Callable[[Path, int], str]


def register_single_host_routes(
    app: FastAPI,
    *,
    multi_user: bool,
    services: SingleHostRouteServices,
) -> None:
    """Register the established single-host pages and JSON/write API."""

    def _resolve_job_spec_path(jobid: str) -> Path:
        """Per-user-aware spec resolution for the job-detail routes."""
        if multi_user:
            return paths.resolve_spec_path(jobid, multi_user=True)
        spec_path = paths.spec_path(jobid)
        if not spec_path.exists():
            raise FileNotFoundError(f"no such job: {jobid}")
        return spec_path

    def _job_payload(spec: JobSpec) -> dict[str, object]:
        payload = spec.model_dump(mode="json")
        payload["queue_handle"] = queue_handle_for_spec(spec, "localhost")
        payload.update(monitoring_payload_for_spec(spec))
        payload["terminal_diagnosis"] = terminal_diagnosis_for_spec(spec)
        return payload

    def _job_view(spec: JobSpec) -> dict[str, object]:
        diagnosis = terminal_diagnosis_for_spec(spec)
        monitoring = monitoring_payload_for_spec(spec)
        return {
            "spec": spec,
            "queue_host": spec.scheduler_target or "localhost",
            "scheduler_phase": spec.scheduler_state or "unpolled",
            "monitoring": monitoring,
            "diagnosis": diagnosis,
            "diagnosis_summary": (
                diagnosis.get("summary") if isinstance(diagnosis, dict) else None
            ),
        }

    def _query_state(request: Request) -> dict[str, str | bool]:
        return web_view.queue_query_state(request.query_params)

    def _view_context(
        specs: list[JobSpec],
        request: Request,
        *,
        include_summary: bool,
    ) -> dict[str, object]:
        query_state = _query_state(request)
        filtered = web_view.filter_and_sort_specs(specs, query_state)
        row_limit = int(str(query_state["limit"]))
        visible = filtered[:row_limit]
        hosts = sorted({spec.scheduler_target or "localhost" for spec in specs})
        states = sorted({spec.state.value for spec in specs})
        context: dict[str, object] = {
            "specs": visible,
            "jobs": [_job_view(spec) for spec in visible],
            "query": query_state,
            "sort_href": lambda field: web_view.queue_sort_href(
                query_state, field
            ),
            "sort_label": lambda field: web_view.queue_sort_label(
                query_state, field
            ),
            "table_refresh_url": web_view.queue_table_href(query_state),
            "state_options": states,
            "host_options": hosts,
            "row_limit_options": web_view.ROW_LIMITS,
            "shown_count": len(visible),
            "filtered_count": len(filtered),
            "total_count": len(specs),
        }
        if include_summary:
            context["summary"] = web_view.cockpit_summary(specs)
        return context

    @app.get("/", include_in_schema=False)
    def root() -> RedirectResponse:
        return RedirectResponse("/queue", status_code=302)

    def _host_pressure_context() -> dict:
        """Pressure-banner context shared by the page and table fragment."""
        from vq.watchdog import read_host_memory_pressure_pct

        try:
            pressure = read_host_memory_pressure_pct()
        except Exception:  # pragma: no cover - defensive
            pressure = None
        pressure_paused = 0
        try:
            for spec in services.list_jobs("localhost", multi_user=multi_user):
                if spec.paused_by == "watchdog_host_pressure":
                    pressure_paused += 1
        except Exception:  # pragma: no cover
            pass
        return {
            "host_pressure_pct": pressure,
            "host_pressure_active": pressure_paused > 0,
            "host_pressure_paused_jobs": pressure_paused,
        }

    @app.get("/queue", response_class=HTMLResponse)
    def queue_page(request: Request) -> HTMLResponse:
        specs = services.list_jobs("localhost", multi_user=multi_user)
        context = _view_context(specs, request, include_summary=True)
        context.update(
            {
                "daemon_running": services.is_daemon_running(),
                "daemon_pid": services.read_pidfile(),
            }
        )
        context.update(_host_pressure_context())
        return services.templates.TemplateResponse(
            request,
            "queue.html",
            context,
        )

    @app.get(
        "/queue/_table",
        response_class=HTMLResponse,
        include_in_schema=False,
    )
    def queue_table_fragment(request: Request) -> HTMLResponse:
        """HTML fragment that htmx polls every few seconds to refresh
        just the table without reloading the whole page."""
        specs = services.list_jobs("localhost", multi_user=multi_user)
        context = _view_context(specs, request, include_summary=False)
        context.update(_host_pressure_context())
        return services.templates.TemplateResponse(
            request,
            "queue_table.html",
            context,
        )

    @app.get("/jobs/{jobid}", response_class=HTMLResponse)
    def job_detail(request: Request, jobid: str) -> HTMLResponse:
        try:
            spec_path = _resolve_job_spec_path(jobid)
        except FileNotFoundError:
            raise HTTPException(
                status_code=404,
                detail=f"no such job: {jobid}",
            ) from None
        spec = JobSpec.read(spec_path)
        workspace = Path(spec.cwd)
        stdout_text = services.tail(workspace / spec.stdout_path, 200)
        stderr_text = services.tail(workspace / spec.stderr_path, 200)
        events = services.read_events(workspace)
        return services.templates.TemplateResponse(
            request,
            "job_detail.html",
            {
                "spec": spec,
                "job": _job_view(spec),
                "stdout_text": stdout_text,
                "stderr_text": stderr_text,
                "events": events,
                "daemon_running": services.is_daemon_running(),
                "daemon_pid": services.read_pidfile(),
            },
        )

    @app.get(
        "/jobs/{jobid}/_log",
        response_class=HTMLResponse,
        include_in_schema=False,
    )
    def job_log_fragment(request: Request, jobid: str) -> HTMLResponse:
        try:
            spec_path = _resolve_job_spec_path(jobid)
        except FileNotFoundError:
            raise HTTPException(
                status_code=404,
                detail=f"no such job: {jobid}",
            ) from None
        spec = JobSpec.read(spec_path)
        workspace = Path(spec.cwd)
        return services.templates.TemplateResponse(
            request,
            "job_log_fragment.html",
            {
                "spec": spec,
                "stdout_text": services.tail(
                    workspace / spec.stdout_path,
                    200,
                ),
                "stderr_text": services.tail(
                    workspace / spec.stderr_path,
                    200,
                ),
            },
        )

    @app.get("/health/live", response_class=PlainTextResponse)
    def health_live() -> str:
        """Web service is up. Always 200 when reachable."""
        return "ok"

    @app.get("/health/ready", response_class=PlainTextResponse)
    def health_ready() -> str:
        """Daemon pidfile has a live PID + queue root exists. 503 otherwise."""
        if not services.is_daemon_running():
            raise HTTPException(status_code=503, detail="daemon not running")
        queue_root = paths.users_root() if multi_user else paths.queue_dir()
        if not queue_root.exists():
            raise HTTPException(
                status_code=503,
                detail="queue dir missing",
            )
        return "ok"

    @app.get("/api/v1/queue")
    def api_queue() -> dict[str, object]:
        specs = services.list_jobs("localhost", multi_user=multi_user)
        return {
            "host": "localhost",
            "summary": web_view.cockpit_summary(specs),
            "jobs": [_job_payload(spec) for spec in specs],
        }

    @app.get("/api/v1/jobs/{jobid}")
    def api_job_detail(jobid: str) -> dict[str, object]:
        try:
            spec_path = _resolve_job_spec_path(jobid)
        except FileNotFoundError:
            raise HTTPException(
                status_code=404,
                detail=f"no such job: {jobid}",
            ) from None
        return _job_payload(JobSpec.read(spec_path))

    bearer_scheme = HTTPBearer(auto_error=False)

    def require_token(
        request: Request,
        creds: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),  # noqa: B008
    ) -> Iterator[str]:
        """Require the token and retain start/outcome receipts for every write.

        Admission refuses if the initial audit cannot be recorded. An audit
        failure after execution is logged without misreporting a completed
        action as safe to retry. No token, query string or error text is saved.
        """
        expected = auth.load_token()
        if expected is None:
            raise HTTPException(
                status_code=503,
                detail=(
                    "web auth not configured: no token file at "
                    f"{auth.web_token_path()}. Run `vq web init-token`."
                ),
            )
        if creds is None or not auth.constant_time_eq(creds.credentials, expected):
            raise HTTPException(
                status_code=401,
                detail="missing or invalid bearer token",
            )
        action = request.url.path.rsplit("/", 1)[-1]
        if request.url.path.startswith("/api/v1/queue/"):
            action = "queue-" + action
        elif action == "kill" and request.query_params.get("resubmit", "").lower() in {
            "true", "1", "on", "yes",
        }:
            action = "kill-resubmit"
        request_id = uuid.uuid4().hex

        def record(outcome: str) -> None:
            fleet_audit.append_audit(
                user="bearer-token", role="admin", action=action,
                jobid=request.path_params.get("jobid"), host="localhost",
                outcome=outcome, request_id=request_id,
            )

        try:
            record("started")
        except OSError:
            raise HTTPException(status_code=503, detail="write audit unavailable") from None
        outcome = "error"
        try:
            yield creds.credentials
            outcome = "ok"
        except HTTPException as exc:
            outcome = f"http-{exc.status_code}"
            raise
        finally:
            try:
                record(outcome)
            except OSError:
                logging.getLogger(__name__).exception(
                    "write outcome audit failed for request %s (%s)", request_id, outcome,
                )

    def _pause_resume_policy_error(
        exc: ownership.OwnershipError | ConfigError,
    ) -> HTTPException:
        """Map locked ownership/config failures onto stable HTTP outcomes."""
        status_code = (
            403 if isinstance(exc, ownership.OwnershipError) else 503
        )
        return HTTPException(status_code=status_code, detail=str(exc))

    @app.post(
        "/api/v1/jobs/{jobid}/kill",
        response_class=PlainTextResponse,
        dependencies=[Depends(require_token)],
    )
    def api_kill(
        jobid: str,
        reason: str | None = None,
        resubmit: bool = False,
    ) -> str:
        """Cancel a job. SIGTERM if running / suspended; mark KILLED if
        pending. Returns a human-readable confirmation string."""
        try:
            message = services.kill_job(
                "localhost",
                jobid,
                multi_user=multi_user,
                reason=reason,
                via="api",
            )
            if resubmit:
                new_id = services.resubmit_local(jobid, multi_user=multi_user)
                message = f"{message}\nresubmitted {jobid} -> {new_id}"
            return message
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from None
        except (FileExistsError, ValueError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from None

    @app.post(
        "/api/v1/jobs/{jobid}/pause",
        response_class=PlainTextResponse,
        dependencies=[Depends(require_token)],
    )
    def api_pause(jobid: str) -> str:
        try:
            return services.pause_job("localhost", jobid, multi_user=multi_user)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from None
        except (ownership.OwnershipError, ConfigError) as exc:
            raise _pause_resume_policy_error(exc) from None
        except PauseError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from None

    @app.post(
        "/api/v1/jobs/{jobid}/resume",
        response_class=PlainTextResponse,
        dependencies=[Depends(require_token)],
    )
    def api_resume(jobid: str) -> str:
        try:
            return services.resume_job("localhost", jobid, multi_user=multi_user)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from None
        except (ownership.OwnershipError, ConfigError) as exc:
            raise _pause_resume_policy_error(exc) from None
        except PauseError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from None

    @app.post(
        "/api/v1/queue/pause",
        response_class=PlainTextResponse,
        dependencies=[Depends(require_token)],
    )
    def api_queue_pause() -> str:
        try:
            return services.pause_all("localhost", multi_user=multi_user)
        except (ownership.OwnershipError, ConfigError) as exc:
            raise _pause_resume_policy_error(exc) from None

    @app.post(
        "/api/v1/queue/resume",
        response_class=PlainTextResponse,
        dependencies=[Depends(require_token)],
    )
    def api_queue_resume() -> str:
        try:
            return services.resume_all("localhost", multi_user=multi_user)
        except (ownership.OwnershipError, ConfigError) as exc:
            raise _pause_resume_policy_error(exc) from None

    @app.post(
        "/api/v1/queue/clear-failed",
        response_class=PlainTextResponse,
        dependencies=[Depends(require_token)],
    )
    def api_queue_clear_failed(older_than: str = "7d") -> str:
        try:
            age = parse_age(older_than)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from None
        if age < timedelta(0):
            raise HTTPException(
                status_code=400,
                detail="older_than must be a non-negative duration",
            )
        now = datetime.now(UTC)
        removed = 0
        skipped = 0
        specs = services.list_jobs("localhost", multi_user=multi_user)
        for spec in specs:
            if spec.state not in FAILED_CLEAR_STATES:
                skipped += 1
                continue
            finished = web_view.parse_time_for_sort(
                spec.finished_at or spec.submitted_at
            )
            if finished > now - age:
                skipped += 1
                continue
            try:
                spec_path = _resolve_job_spec_path(spec.id)
                services.delete_job(spec, queue_dir=spec_path.parent)
                removed += 1
            except (OSError, ValueError):
                skipped += 1
        return f"cleared {removed} old failed job(s); skipped {skipped}"
