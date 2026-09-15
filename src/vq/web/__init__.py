"""Read-only web UI for vq.

A small FastAPI app that reads the same on-disk state the daemon writes:
* ``<state_root>/queue/*.json`` -- JobSpecs
* ``<state_root>/jobs/<id>/_vq/events.jsonl`` -- v0.4 event log
* ``<state_root>/jobs/<id>/{stdout,stderr}.log`` -- captured output

Read-only by design (v0.5.0). Write actions (POST kill / resubmit) and
the JSON HTTP API at ``/api/v1/`` arrive in v0.5.1+. The dashboard
gracefully degrades if the daemon is down -- you can still browse
historical job state.

Run interactively:
    vq web run

Or as a systemd-user service (recommended for production):
    cp vibe-queue/contrib/vq-web.service ~/.config/systemd/user/
    systemctl --user daemon-reload
    systemctl --user enable --now vq-web
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

from vq import __version__, config
from vq import auth as auth
from vq.cleanup import delete_job
from vq.daemon_control import is_daemon_running, read_pidfile
from vq.events import read_events
from vq.host import is_local_host
from vq.kill import kill_job
from vq.listing import list_jobs
from vq.pause_resume import pause_all, pause_job, resume_all, resume_job
from vq.resubmit import resubmit_local
from vq.spec import describe_exit_code
from vq.web import authn as authn
from vq.web import console_status
from vq.web import fleet as fleet_mod
from vq.web import fleet_audit as fleet_audit
from vq.web import settings as web_settings
from vq.web import view_context as web_view

if TYPE_CHECKING:  # pragma: no cover — typing only, never imported at runtime
    from fastapi import FastAPI

# FastAPI is NOT imported at module level, and neither are the two route
# modules that import it (`fleet_routes`, `single_host_routes`).
#
# Importing this package must not require the ``[web]`` extra. `vq web
# status` and `vq web config` are diagnostic verbs whose whole job is to
# answer "is a console installed here, and is it current?" — and the
# answer is "no console here" on every fleet host except the coordinator,
# which is exactly where the extra is absent. Before this, both verbs
# raised a bare `ModuleNotFoundError: No module named 'fastapi'` on those
# hosts, so the drift detection built to prevent the 2026-08-05 incident
# was unusable on 13 of 14 hosts, and a fleet-wide convergence probe
# would have reported a hard failure everywhere it was working correctly.
#
# `vq.web.install` and `vq.web.settings` need only stdlib plus vq itself;
# it was solely this file's import block that dragged FastAPI in.
WEB_DIR = Path(__file__).parent
SORT_FIELDS = web_view.SORT_FIELDS
ROW_LIMITS = web_view.ROW_LIMITS
DEFAULT_ROW_LIMIT = web_view.DEFAULT_ROW_LIMIT
_cockpit_summary = web_view.cockpit_summary
_filter_and_sort_specs = web_view.filter_and_sort_specs
_parse_time_for_sort = web_view.parse_time_for_sort
_sort_href = web_view.queue_sort_href
_sort_label = web_view.queue_sort_label
_table_href = web_view.queue_table_href
_safe_next = web_view.safe_fleet_next
_row_matches = web_view.fleet_row_matches

#: Cached so every caller shares one instance. A test monkeypatches
#: ``web.TEMPLATES.TemplateResponse`` and then asks ``create_app()`` to
#: render; handing out a fresh Jinja2Templates per access would patch an
#: object nobody uses.
_TEMPLATES: Any = None


def _templates() -> Any:
    """The Jinja environment, built on first use.

    Deferred for the reason given at the top of this module: constructing
    it needs the ``[web]`` extra, and importing this package must not.
    """
    global _TEMPLATES
    if _TEMPLATES is None:
        from fastapi.templating import Jinja2Templates  # noqa: PLC0415

        _TEMPLATES = Jinja2Templates(directory=str(WEB_DIR / "templates"))
    return _TEMPLATES


def __getattr__(name: str) -> Any:
    """Lazy module attributes (PEP 562).

    ``TEMPLATES``, ``FAILED_CLEAR_STATES`` and ``app`` were module-level
    values whose construction imports FastAPI. They stay reachable under
    the same names — ``uvicorn vq.web:app`` is a documented entry point
    and a test patches ``web.TEMPLATES`` — but they are now built on
    first access, so merely importing ``vq.web`` costs nothing and needs
    no extra.
    """
    if name == "TEMPLATES":
        return _templates()
    if name == "FAILED_CLEAR_STATES":
        from vq.web import single_host_routes  # noqa: PLC0415

        return single_host_routes.FAILED_CLEAR_STATES
    if name == "app":
        return create_app()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def create_app(
    settings: web_settings.WebSettings | None = None,
) -> FastAPI:
    """Build the FastAPI app. Factored as a function so tests can build
    fresh apps with overridden state-dir env vars.

    ``settings`` is the resolved console configuration (see
    :mod:`vq.web.settings`). Omit it and it is resolved from the
    environment plus the ``[web]`` config section, which is what the
    module-level ``app`` and every existing caller get. Passing one
    explicitly lets a caller — the CLI verb, a test — state the
    configuration outright instead of round-tripping it through
    ``os.environ``.
    """
    from fastapi import FastAPI  # noqa: PLC0415 — optional [web] extra
    from fastapi.staticfiles import StaticFiles  # noqa: PLC0415

    from vq.web import fleet_routes as fleet_web  # noqa: PLC0415
    from vq.web import single_host_routes as single_host  # noqa: PLC0415

    TEMPLATES = _templates()

    if settings is None:
        settings = web_settings.resolve_settings()

    # Fleet mode (docs/fleet_dashboard_design.md) is an app-construction
    # decision: the /fleet routes and the background SSH-sweep poller
    # exist only when it is on. Off = byte-identical single-host
    # dashboard. The poller is started by the lifespan hook when the
    # server actually runs (a TestClient used without a context manager
    # never starts it).
    fleet_mode = settings.fleet

    @asynccontextmanager
    async def _lifespan(app: FastAPI):
        cache = getattr(app.state, "fleet_cache", None)
        if cache is not None:
            cache.start()
        yield
        if cache is not None:
            cache.stop()

    app = FastAPI(
        title=settings.title,
        description="Read-only dashboard for the vq job queue",
        version=__version__,
        lifespan=_lifespan,
        # Serve OpenAPI docs at /docs even though the read-only UI has
        # no documented endpoints to call from a script -- useful when
        # v0.5.1 adds the /api/v1/ surface.
    )
    app.mount(
        "/static",
        StaticFiles(directory=str(WEB_DIR / "static")),
        name="static",
    )

    # Make ``daemon_running`` available to every template via a context
    # processor pattern. Jinja2Templates exposes globals through
    # `env.globals` but those are evaluated once; we want a per-request
    # call. Use a small helper that templates call via a Jinja global.
    TEMPLATES.env.globals["vq_version"] = __version__
    # The version chip needs to say *whose* version it is. Before v0.25.0
    # `vq_version` was rendered bare, in the same style as four unrelated
    # facts, and read as a property of the fleet — which is how a console
    # 1081 commits stale looked like a fleet running old vq. `console`
    # carries the interpreter path and start time so the tooltip can name
    # the tree an operator has to go fix.
    TEMPLATES.env.globals["console"] = console_status.console_identity()
    TEMPLATES.env.globals["site_title"] = settings.title
    # Reassigned on every create_app() call so test apps built with a
    # different fleet setting don't inherit a stale nav state.
    TEMPLATES.env.globals["fleet_mode"] = fleet_mode
    # v0.12.0: decode a signaled exit (137 -> SIGKILL) in the job detail
    # view, matching `vq status` and the CLI surfaces.
    TEMPLATES.env.filters["describe_exit"] = describe_exit_code

    # v0.6.41: detect multi-user mode once at app construction. The
    # dashboard then reads job state from the per-user dirs under
    # /var/lib/vq/users/<uid>/ instead of the single-user queue dir
    # — without this the queue page is empty on a multi-user host.
    multi_user = config.system_multi_user_enabled()
    try:
        if config.load_config().multi_user.enabled:
            multi_user = True
    except Exception:  # pragma: no cover — defensive
        pass

    # A callable, not a value: the verdict must be able to change while
    # the process runs (somebody restarts the daemon under it), and it is
    # TTL-cached so a page render never costs more than one probe a
    # minute. Bound here rather than beside the other globals because it
    # needs ``multi_user``, which is resolved just above.
    TEMPLATES.env.globals["console_staleness"] = (
        lambda: console_status.cached_staleness(multi_user=multi_user)
    )

    single_host.register_single_host_routes(
        app,
        multi_user=multi_user,
        services=single_host.SingleHostRouteServices(
            templates=TEMPLATES,
            list_jobs=lambda *args, **kwargs: list_jobs(*args, **kwargs),
            is_daemon_running=lambda: is_daemon_running(multi_user=multi_user),
            read_pidfile=lambda: read_pidfile(),
            read_events=lambda workspace: read_events(workspace),
            kill_job=lambda *args, **kwargs: kill_job(*args, **kwargs),
            resubmit_local=lambda *args, **kwargs: resubmit_local(
                *args, **kwargs
            ),
            pause_job=lambda *args, **kwargs: pause_job(*args, **kwargs),
            resume_job=lambda *args, **kwargs: resume_job(*args, **kwargs),
            pause_all=lambda *args, **kwargs: pause_all(*args, **kwargs),
            resume_all=lambda *args, **kwargs: resume_all(*args, **kwargs),
            delete_job=lambda *args, **kwargs: delete_job(*args, **kwargs),
            tail=lambda path, n: _tail(path, n),
        ),
    )

    # Fleet routes remain an ordered suffix and their caches exist only
    # when fleet mode was enabled at app construction.
    app.state.web_settings = settings

    if fleet_mode:
        # Close the gap where /fleet asked for a password while /queue,
        # /jobs/<id> and /api/v1/* on the same port answered anybody.
        # No-op until an account exists (open "tunnel posture").
        authn.install_single_host_guard(app)

        cache = fleet_mod.FleetCache(
            interval_seconds=settings.fleet_interval_seconds
        )
        app.state.fleet_cache = cache
        doctor_cache = fleet_mod.DoctorCache()
        app.state.doctor_cache = doctor_cache
        fleet_web.register_fleet_routes(
            app,
            multi_user=multi_user,
            cache=cache,
            doctor_cache=doctor_cache,
            services=fleet_web.FleetRouteServices(
                templates=TEMPLATES,
                is_daemon_running=lambda: is_daemon_running(),
                read_pidfile=lambda: read_pidfile(),
                is_local_host=lambda host: is_local_host(host),
                safe_next=lambda raw: _safe_next(raw),
                row_matches=lambda row, query: _row_matches(row, query),
                kill_job=lambda *args, **kwargs: kill_job(*args, **kwargs),
                pause_job=lambda *args, **kwargs: pause_job(*args, **kwargs),
                resume_job=lambda *args, **kwargs: resume_job(*args, **kwargs),
            ),
        )

    return app


def _tail(path: Path, n: int) -> str:
    """Last ``n`` lines of a text file; "" if the file doesn't exist
    yet (the dispatched job hasn't started writing). Decode errors are
    replaced rather than raised -- we never want a broken log file to
    take the dashboard down."""
    if not path.exists():
        return ""
    text = path.read_text(errors="replace")
    lines = text.splitlines()
    return "\n".join(lines[-n:])


# `app` is served lazily by __getattr__ above, so `uvicorn vq.web:app`
# still works while merely importing this package does not build an app
# (and so does not need the [web] extra).
#
# Building it eagerly here was also a second, quieter bug: the instance
# was constructed at import time, which froze whatever settings and
# environment happened to be in effect at first import. `vq web run` has
# always ignored it and called create_app() itself for exactly that
# reason. Tests should keep using create_app() to get a fresh instance.
