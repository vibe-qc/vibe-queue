"""Resolved runtime settings for the vq web console.

One module answers "how is this console configured?", for every consumer:
the CLI verb that starts it, the app factory that builds it, the fleet
poller that sweeps on it, the templates that render it, and the installer
that writes its unit file.

**Precedence, highest first: CLI flag > environment variable > the
``[web]`` config section > built-in default.** Each layer is optional and
each is allowed to fill in only what the layer above left unset, so a
site can pin whatever it wants at whatever level suits it and leave the
rest alone.

Why this exists
---------------

Before v0.25.0 the console's configuration lived in exactly two places:
``argv``, and two environment variables (``VQ_WEB_FLEET``,
``VQ_WEB_FLEET_INTERVAL``) that appeared in no config file and no
``--help`` output. A deployed console's entire configuration was
therefore one ``ExecStart`` line, unvalidated and unversioned. The
2026-08-05 fleet audit found the predictable result on the reference
fleet: a unit pinned to a hand-staged checkout 1081 commits behind the vq
that owned it, invisible to every convergence check the fleet had.

Nothing here reads the network or touches disk beyond the config file the
caller already loaded, so it is cheap enough to resolve per request and
safe to call from a test.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, replace

from vq import config

#: Built-in defaults. Loopback bind because the single-host read surface
#: is unauthenticated; 8765 is the port ``vq web run`` has defaulted to
#: since v0.5 and is kept verbatim so this layer changes nobody's
#: behaviour; single-host mode because an SSH fan-out must be opt-in.
DEFAULT_BIND = "127.0.0.1"
DEFAULT_PORT = 8765
DEFAULT_FLEET = False
DEFAULT_FLEET_INTERVAL_SECONDS = 30
DEFAULT_LOG_LEVEL = "info"
DEFAULT_TITLE = "vq"

#: Below this, a fleet sweep would overlap its own SSH fan-out. Values
#: under the floor are rejected by the config model and ignored (with the
#: default substituted) when they arrive through the environment, which
#: has no validation layer of its own.
MIN_FLEET_INTERVAL_SECONDS = 5

_TRUE = frozenset({"1", "true", "yes", "on"})
_FALSE = frozenset({"0", "false", "no", "off"})

#: Environment overrides, documented so ``vq web config`` can list them.
ENV_FLEET = "VQ_WEB_FLEET"
ENV_FLEET_INTERVAL = "VQ_WEB_FLEET_INTERVAL"
ENV_BIND = "VQ_WEB_BIND"
ENV_PORT = "VQ_WEB_PORT"
ENV_LOG_LEVEL = "VQ_WEB_LOG_LEVEL"
ENV_TITLE = "VQ_WEB_TITLE"
ENV_PUBLIC_BIND_ACK = "VQ_WEB_PUBLIC_BIND_ACK"


@dataclass(frozen=True)
class WebSettings:
    """A fully resolved console configuration.

    Every field is concrete — no ``None``, no "look it up later". A
    consumer that holds one of these needs no further lookups and cannot
    disagree with another consumer about what the console is doing.
    """

    bind: str = DEFAULT_BIND
    port: int = DEFAULT_PORT
    fleet: bool = DEFAULT_FLEET
    fleet_interval_seconds: int = DEFAULT_FLEET_INTERVAL_SECONDS
    log_level: str = DEFAULT_LOG_LEVEL
    title: str = DEFAULT_TITLE
    public_bind_ack: bool = False

    @property
    def is_loopback_bind(self) -> bool:
        """True when :attr:`bind` is a loopback address.

        Accepts the literal ``localhost`` plus anything in 127.0.0.0/8 or
        ``::1``. A hostname that is not ``localhost`` counts as
        non-loopback: vq will not resolve it to decide whether to warn,
        because a name that resolves to a loopback address today may not
        tomorrow.
        """
        import ipaddress  # noqa: PLC0415 — stdlib, cold path only

        if self.bind == "localhost":
            return True
        try:
            return ipaddress.ip_address(self.bind).is_loopback
        except ValueError:
            return False

    @property
    def needs_public_bind_warning(self) -> bool:
        """True when this console should warn loudly at startup."""
        return not self.is_loopback_bind and not self.public_bind_ack

    def url(self) -> str:
        """The URL an operator would type to reach this console.

        A wildcard bind is not a reachable address, so it renders as
        loopback — which is where the operator on the box will find it.
        """
        host = self.bind
        if host in {"0.0.0.0", "::", ""}:  # noqa: S104 — display only
            host = "127.0.0.1"
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        return f"http://{host}:{self.port}/"


def _env_bool(name: str) -> bool | None:
    """Tri-state read of a boolean environment variable.

    ``None`` means "not set, defer to the next layer" — distinct from an
    explicit ``0``, which means "off, and stop looking".
    """
    raw = os.environ.get(name)
    if raw is None:
        return None
    lowered = raw.strip().lower()
    if lowered in _TRUE:
        return True
    if lowered in _FALSE:
        return False
    return None


def _env_int(name: str, *, minimum: int | None = None) -> int | None:
    raw = os.environ.get(name)
    if raw is None:
        return None
    try:
        value = int(raw.strip())
    except ValueError:
        return None
    if minimum is not None and value < minimum:
        return None
    return value


def _env_str(name: str) -> str | None:
    raw = os.environ.get(name)
    if raw is None:
        return None
    stripped = raw.strip()
    return stripped or None


def _field_is_valid(field: str, value: object) -> bool:
    """Whether ``value`` would be accepted for ``field`` in ``[web]``.

    Validation is delegated to :class:`~vq.config.WebConsoleConfig` rather
    than reimplemented, so all four layers agree on what is legal. They
    did not always: before this, ``[web] port = 70000`` was rejected at
    config load while ``VQ_WEB_PORT=70000`` sailed through, and
    ``log_level = "chatty"`` was rejected in the file but reached
    ``uvicorn.run()`` from the environment and crashed the console at
    startup. A rule enforced by one of four input layers is not a rule.
    """
    try:
        config.WebConsoleConfig.model_validate({field: value})
    except Exception:
        return False
    return True


def _resolve_field(
    field: str, candidates: list[tuple[str, object | None]]
) -> tuple[object, str]:
    """First candidate that is both present and valid, with its source.

    ``candidates`` is ordered highest-precedence first and must end with
    the built-in default. A candidate that is present but invalid is
    skipped *and does not claim the field* — which is what makes
    :func:`describe_sources` able to say "default" for a port whose
    environment variable was garbage, instead of blaming the variable for
    a value it did not supply.
    """
    for source, value in candidates:
        if value is None:
            continue
        if not _field_is_valid(field, value):
            continue
        return value, source
    # Unreachable in practice: the last candidate is always a built-in
    # default, and the built-in defaults are valid by construction.
    raise ValueError(f"no valid candidate for web setting {field!r}")


def resolve_settings(
    cfg: config.Config | None = None,
    *,
    bind: str | None = None,
    port: int | None = None,
    fleet: bool | None = None,
    fleet_interval_seconds: int | None = None,
    log_level: str | None = None,
    title: str | None = None,
    public_bind_ack: bool | None = None,
) -> WebSettings:
    """Compose CLI arguments, the environment, and ``[web]`` into one
    :class:`WebSettings`.

    Pass ``None`` for any CLI argument the operator did not give — that is
    what lets the lower layers show through. A caller that passes a
    concrete value is stating the operator asked for it explicitly, so it
    wins outright.

    ``cfg`` is loaded on demand when omitted. A config that fails to load
    is not fatal here: the console must still be startable on a host whose
    config file is broken, since one of the things an operator reaches for
    when the config is broken is the console.
    """
    resolved, _ = _resolve_all(
        cfg,
        bind=bind,
        port=port,
        fleet=fleet,
        fleet_interval_seconds=fleet_interval_seconds,
        log_level=log_level,
        title=title,
        public_bind_ack=public_bind_ack,
    )
    return resolved


def _candidate_table(
    cfg: config.Config, cli: dict[str, object | None]
) -> dict[str, list[tuple[str, object | None]]]:
    """Per-field candidate lists, highest precedence first.

    This table *is* the precedence rule, written once. Both
    :func:`resolve_settings` and :func:`describe_sources` walk it, which
    is what makes them incapable of disagreeing about where a value came
    from.
    """
    section = cfg.web
    return {
        "bind": [
            ("flag", cli.get("bind")),
            (f"env {ENV_BIND}", _env_str(ENV_BIND)),
            ("config [web]", section.bind),
            ("default", DEFAULT_BIND),
        ],
        "port": [
            ("flag", cli.get("port")),
            (f"env {ENV_PORT}", _env_int(ENV_PORT)),
            ("config [web]", section.port),
            ("default", DEFAULT_PORT),
        ],
        "fleet": [
            ("flag", cli.get("fleet")),
            (f"env {ENV_FLEET}", _env_bool(ENV_FLEET)),
            ("config [web]", section.fleet),
            ("default", DEFAULT_FLEET),
        ],
        "fleet_interval_seconds": [
            ("flag", cli.get("fleet_interval_seconds")),
            (f"env {ENV_FLEET_INTERVAL}", _env_int(ENV_FLEET_INTERVAL)),
            ("config [web]", section.fleet_interval_seconds),
            ("default", DEFAULT_FLEET_INTERVAL_SECONDS),
        ],
        "log_level": [
            ("flag", cli.get("log_level")),
            (f"env {ENV_LOG_LEVEL}", _env_str(ENV_LOG_LEVEL)),
            ("config [web]", section.log_level),
            ("default", DEFAULT_LOG_LEVEL),
        ],
        "title": [
            ("flag", cli.get("title")),
            (f"env {ENV_TITLE}", _env_str(ENV_TITLE)),
            ("config [web]", section.title),
            ("default", DEFAULT_TITLE),
        ],
        "public_bind_ack": [
            ("flag", cli.get("public_bind_ack")),
            (f"env {ENV_PUBLIC_BIND_ACK}", _env_bool(ENV_PUBLIC_BIND_ACK)),
            ("config [web]", section.public_bind_ack),
            ("default", False),
        ],
    }


def _resolve_all(
    cfg: config.Config | None, **cli: object | None
) -> tuple[WebSettings, dict[str, str]]:
    """Resolve every field once, returning the settings and the sources."""
    if cfg is None:
        try:
            cfg = config.load_config()
        except Exception:  # pragma: no cover — defensive; see docstring
            cfg = config.Config()

    values: dict[str, object] = {}
    sources: dict[str, str] = {}
    for field, candidates in _candidate_table(cfg, cli).items():
        value, source = _resolve_field(field, candidates)
        values[field] = value
        sources[field] = source

    return (
        WebSettings(
            bind=str(values["bind"]),
            port=int(values["port"]),  # type: ignore[arg-type]
            fleet=bool(values["fleet"]),
            fleet_interval_seconds=int(values["fleet_interval_seconds"]),  # type: ignore[arg-type]
            log_level=str(values["log_level"]).lower(),
            title=str(values["title"]),
            public_bind_ack=bool(values["public_bind_ack"]),
        ),
        sources,
    )


def config_load_failure() -> str | None:
    """Why the config file could not be loaded, or None when it loads.

    The resolvers above fall back to built-in defaults when it does not, on
    purpose: the console must still start on a host whose config is broken.
    What they must not do is fall back silently. An operator whose ``[web]``
    sets ``fleet = true`` or a non-default ``bind`` would otherwise get a
    console that ignores both, and ``vq web config`` confirming the defaults as
    "the resolved configuration" (#40). A ``ConfigError`` message names keys
    and reasons, not values (#38).
    """
    try:
        config.load_config()
    except config.ConfigError as exc:
        return str(exc)
    except Exception as exc:  # noqa: BLE001 - mirrors the resolver's fallback
        return f"{type(exc).__name__}: {exc}"
    return None


def describe_sources(cfg: config.Config | None = None) -> list[dict[str, object]]:
    """Per-field provenance, for ``vq web config``.

    Answers the question an operator actually has when the console does
    something they did not expect — *which* layer set this? Returns one
    row per setting with its resolved value and the layer that supplied
    it. CLI flags are not represented: this describes the persistent
    configuration, and a flag is by definition not persistent.

    The source is taken from the same resolution pass that produces the
    value, so the two cannot disagree. An earlier version inferred the
    source separately, by asking only whether an environment variable was
    *set* -- which meant a typo'd ``VQ_WEB_PORT=notanint`` reported the
    built-in default under the label ``env VQ_WEB_PORT``, blaming the
    variable for a value it had not supplied. That is precisely the case
    this command exists to diagnose.
    """
    resolved, sources = _resolve_all(cfg)
    return [
        {"name": name, "value": getattr(resolved, name), "source": sources[name]}
        for name in (
            "bind",
            "port",
            "fleet",
            "fleet_interval_seconds",
            "log_level",
            "title",
            "public_bind_ack",
        )
    ]


def with_overrides(base: WebSettings, **overrides: object) -> WebSettings:
    """A copy of ``base`` with the non-None overrides applied. Used by
    callers that resolve once and then narrow (tests, the installer)."""
    concrete = {k: v for k, v in overrides.items() if v is not None}
    return replace(base, **concrete)  # type: ignore[arg-type]
