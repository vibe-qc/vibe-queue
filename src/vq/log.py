"""Daemon + client logging configuration."""
from __future__ import annotations

import logging
import logging.handlers
import os
import sys
from pathlib import Path

ENV_LOG_LEVEL = "VQ_LOG_LEVEL"
"""v0.6.16: client log level override. Accepted values match
:py:mod:`logging` standard names (DEBUG, INFO, WARNING, ERROR,
CRITICAL). Case-insensitive. Invalid values fall back to INFO."""

ENV_LOG_DISABLED = "VQ_LOG_DISABLED"
"""v0.6.16: set to ``1`` to skip client-log file setup entirely.
Useful in test environments where the log file would pollute
``$VQ_STATE_DIR``, and as the emergency escape hatch if the file
write itself becomes a failure mode (read-only fs, etc.).
``setup_cli_logging`` returns None in that case; existing
``logging.getLogger()`` callers still work — their output just
goes nowhere unless something else attached a handler."""

CLI_LOG_FILE_BASENAME = "client.log"
"""Name of the file the client log lands in, under ``state_root()``.
Co-located with ``daemon.log`` so an operator inspecting a single
host has both views in one directory. Different from
``daemon.log`` so daemon writes (which can be voluminous) don't
fight CLI writes for the same file handle."""

CLI_LOG_MAX_BYTES = 10 * 1024 * 1024
"""Per-file size cap before rotation. 10 MB keeps a long history
of CLI invocations (each typically a few hundred bytes) without
unbounded growth on a host where ``vq`` is invoked tens of
thousands of times a day."""

CLI_LOG_BACKUP_COUNT = 3
"""Number of rotated backups (``client.log.1``, ``.2``, ``.3``).
Total disk footprint: 4 × 10 MB = 40 MB. Cheap on every modern
host; enough history to forensic-debug a crash that hit hours
ago."""


DAEMON_LOG_MAX_BYTES = 20 * 1024 * 1024
"""Per-file size cap for ``daemon.log`` before rotation.

Was unbounded: a plain ``FileHandler``, no rotation, and nothing in the cleanup
sweep pruning it, so on a long-lived fleet host the daemon log grew forever.
20 MB is double the CLI log's cap because the daemon writes continuously (one
dispatch narrative per job) where the CLI writes one line per invocation."""

DAEMON_LOG_BACKUP_COUNT = 3
"""Rotated backups kept (``daemon.log.1`` … ``.3``); 80 MB total."""


def setup_daemon_logging(log_file: Path, *, level: int | None = None) -> None:
    """Configure root logger to write to ``log_file``, plus a console handler
    when stderr is a terminal.

    ``level`` defaults to :data:`ENV_LOG_LEVEL` (then INFO). Before this the
    daemon's level was hardcoded: ``VQ_LOG_LEVEL=DEBUG vq daemon run`` changed
    nothing, because the env var was read only on the CLI path. Debugging a
    dispatch problem meant editing source on the host.

    The stderr handler is intentionally skipped when stderr is not a TTY
    because in the two non-interactive modes the daemon runs in --

      * `vq daemon start` (detached): the parent redirects the child's
        stdout+stderr into ``log_file`` itself; a StreamHandler would then
        cause every line to land in the file twice.
      * systemd (``vq daemon run`` under a unit): journald captures stdout
        directly via ``StandardOutput=``; a Python-level StreamHandler is
        redundant.

    Interactive ``vq daemon run`` from a shell still gets live console output.
    """
    log_file.parent.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    # Drop any handlers a previous call (or pytest) installed.
    for h in list(root.handlers):
        root.removeHandler(h)
    root.setLevel(level if level is not None else _resolve_cli_log_level())
    fmt = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    file_h: logging.Handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=DAEMON_LOG_MAX_BYTES,
        backupCount=DAEMON_LOG_BACKUP_COUNT,
    )
    file_h.setFormatter(fmt)
    root.addHandler(file_h)
    if sys.stderr.isatty():
        err_h = logging.StreamHandler()
        err_h.setFormatter(fmt)
        root.addHandler(err_h)


def _resolve_cli_log_level() -> int:
    """Read VQ_LOG_LEVEL env var; default INFO on invalid / unset."""
    raw = os.environ.get(ENV_LOG_LEVEL, "").strip().upper()
    if not raw:
        return logging.INFO
    # logging.getLevelName accepts INFO/DEBUG/etc and returns the int;
    # invalid names return the literal string "Level <name>", which is
    # not an int, hence the isinstance check.
    level = logging.getLevelName(raw)
    if isinstance(level, int):
        return level
    return logging.INFO


def setup_cli_logging(log_file: Path) -> logging.handlers.RotatingFileHandler | None:
    """v0.6.16: configure client-side logging for ``vq`` CLI invocations.

    Writes to ``log_file`` (typically ``<state_root>/client.log``)
    with size-based rotation so the file never grows unbounded:

      * ``CLI_LOG_MAX_BYTES`` (10 MB) per file.
      * ``CLI_LOG_BACKUP_COUNT`` (3) rotated backups
        (``client.log.1`` / ``.2`` / ``.3``).
      * Total disk footprint: 40 MB worst-case.

    Level is read from ``$VQ_LOG_LEVEL`` env (default INFO).
    Returns the installed handler so callers (tests, the CLI
    entry point) can attach extra context filters or tear it
    down. Returns ``None`` if ``$VQ_LOG_DISABLED=1`` or if the
    file open fails (read-only filesystem, permission denied,
    parent dir un-createable) — the CLI keeps working in that
    case; logging is best-effort.

    Why a separate file from ``daemon.log``: avoids two handles
    fighting over the same file when a host runs both ``vq
    daemon`` and ``vq`` client commands simultaneously
    (laptop-only hosts won't have daemon.log; daemon-only hosts
    won't have client.log; mixed hosts have both, side by side).

    Idempotent: removes any prior CLI-log handler from the root
    logger before installing the new one so repeated CLI
    invocations in the same Python interpreter (testing, the
    web stack import path) don't pile up handlers.
    """
    if os.environ.get(ENV_LOG_DISABLED, "") == "1":
        return None
    level = _resolve_cli_log_level()
    root = logging.getLogger()
    # Drop any prior RotatingFileHandler we installed (idempotency).
    for h in list(root.handlers):
        if getattr(h, "_vq_cli_log_marker", False):
            root.removeHandler(h)
            h.close()
    try:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=CLI_LOG_MAX_BYTES,
            backupCount=CLI_LOG_BACKUP_COUNT,
            encoding="utf-8",
        )
    except OSError:
        # Best-effort: a CLI invocation must not die because the
        # log file is unwriteable. The operator who hits this case
        # probably has bigger problems than missing logs (read-only
        # state dir would break dispatch anyway).
        return None
    handler._vq_cli_log_marker = True  # type: ignore[attr-defined]
    handler.setFormatter(
        logging.Formatter(
            # Add a `pid` field so concurrent invocations (parallel
            # shell pipelines) are disambiguable in the rotated log.
            "%(asctime)s [pid=%(process)d] %(levelname)s %(name)s: %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S%z",
        )
    )
    # Honor the root level if it's tighter than ours; otherwise lift
    # the root to ours so our handler isn't gated by an INFO root.
    if root.level == logging.NOTSET or root.level > level:
        root.setLevel(level)
    handler.setLevel(level)
    root.addHandler(handler)
    return handler
