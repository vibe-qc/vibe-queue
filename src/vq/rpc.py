"""v0.8.0 *Dahl's Simula* — Unix-socket RPC between vq CLI and daemon.

The shipping motivation is the v0.7.12 audit's "admin-status split"
footgun: ``vq admin status`` from the operator reads
``~/.local/share/vq/admin-status.json``, but ``vq admin auto-update``
running from the daemon's cron writes ``/var/lib/vq/admin-status.json``.
Two parallel views of "this env's last update", and the operator can't
tell them apart without manual ``VQ_STATE_DIR=`` gymnastics.

v0.8.0 routes admin-status access through this RPC so there's ONE
canonical file — the daemon-owned one — regardless of who's asking.

v0.8.1 *Karp's Reduction* extends the same pattern to drain.json and
throttle.json — the other two daemon-state files affected by the
v0.7.12 XDG split. drain.json was the worst offender (no multi-user
path mapping at all pre-v0.8.1; ``vq drain --all`` from an admin user
in multi-user mode wrote ``~/.local/share/vq/drain.json`` while the
daemon read ``/var/lib/vq/drain.json``). throttle.json had a v0.6.37
multi-user path mapping but still required root for the file write —
RPC routing now lets any admin-group caller set persistent throttle
without needing to ``sudo``. Both files are now daemon-mediated via
the same methods + auth pattern as admin-status.

## Protocol

Unix domain socket. Default path:

* single-user → ``<state_root>/daemon.sock``
* multi-user → ``<multi_user_root>/daemon.sock`` (typically
  ``/var/lib/vq/daemon.sock``)

Line-delimited JSON. Each request is one JSON object on one line;
each response is one JSON object on one line.

Request shape:

    {"method": "get_admin_status", "args": {}}
    {"method": "set_admin_status", "args": {"env": "..", "record": {...}, "token": "..."}}

Response shape (success):

    {"ok": true, "result": <method-specific>}

Response shape (error):

    {"ok": false, "error": "human-readable message"}

The connection closes after one request/response pair. Simple
to reason about, no protocol state to track. Clients open + send
+ read + close; daemon accepts + reads + writes + closes.

## Methods

* ``get_admin_status()`` — returns ``dict[str, AdminUpdateRecord-dict]``.
  Open to any caller. The data isn't sensitive (env update outcomes).

* ``set_admin_status(env, record, token=None)`` — write a single env's
  record. In multi-user mode requires a valid admin token (the same one
  ``vq admin update`` uses). In single-user mode the socket file is
  0600 so the caller's process identity gates access; no token needed.

* ``get_drain_state()`` *(v0.8.1)* — returns the current drain state as
  a dict (the ``DrainState.model_dump()`` shape) or ``None`` if drain
  isn't active. Open to any caller — drain state is operator-visible,
  not sensitive.

* ``set_drain_state(state, token=None)`` *(v0.8.1)* — write a drain
  state dict. ``state=None`` clears drain (same as
  ``vq drain --release``). Multi-user requires the admin token.

* ``set_owned_full_drain_release(expected_reason, expected_set_at,
  token=None)`` — atomically release a full drain only while both recorded
  ownership fields still match. Multi-user requires the admin token.

* ``get_scheduler_drain_leases()`` — returns the independently owned
  scheduler-target holds from the mixed-version-safe lease store.

* ``set_scheduler_drain_lease(...)`` — atomically acquires or releases one
  owner-scoped scheduler-target hold. New clients capability-probe this method
  and never fall back to the legacy whole-object drain write.

* ``get_throttle_state()`` *(v0.8.1)* — returns the current persistent
  throttle state as a dict or ``None``. Open to any caller.

* ``get_daemon_capacity()`` — returns the daemon's in-memory CPU, job, and
  memory ceilings. Open to any caller; these are operator-visible scheduling
  facts, not secrets.

* ``set_throttle_state(state, token=None)`` *(v0.8.1)* — write a
  throttle state dict. ``state=None`` clears persistent throttle.
  Multi-user requires the admin token.

* ``ping()`` — health check; returns the daemon version and the exact source
  SHA captured when the daemon process started. Used by clients to prove both
  liveness and source provenance without doing real work.

* ``get_process_identity()`` — open, capability-probed read of PID, effective
  uid, Python executable, and argv. Verbose fleet diagnostics bind it to the
  system service manager; the stable ``ping`` result remains unchanged.

* ``get_methods()`` *(v0.8.4)* — returns
  ``{methods: [...], version: ..., multi_user: ...}``. Open to any
  caller. Forward-compat probe: a newer client asks the daemon
  what's available before calling. The methods list is sorted so
  monitoring-script diffs are stable across daemon restarts.

## Permissions

* single-user: socket file is 0600 (owner-only). The Unix kernel
  enforces; only the daemon-owning user can connect.
* multi-user: socket file is 0660 with group = ``admin_group`` from
  the multi-user config. Any user in the admin group can connect.
  Per-method auth (token check) gates writes.

## Fallback

Clients use :func:`try_rpc_or_fallback`. If the RPC call fails
(socket missing, daemon down, timeout, error response), the caller
falls back to direct file access — in single-user mode this is the
correct file; in multi-user mode this gives the user's local file
(which is the wrong file vs. the daemon's, but better than
nothing). Multi-user fallback is logged at WARNING level so the
operator knows they're seeing a stale view.

## Audit trail (v0.8.6)

Every ``set_*`` call appends one JSON-Lines entry to the audit
log at ``<state_root>/rpc-audit.jsonl`` (or
``<multi_user_root>/rpc-audit.jsonl``). Schema:
``{ts, method, uid, ok, args_summary, error?}``. Reads (``ping``,
``get_methods``, ``get_*``) are NOT logged — they're high-volume
and not sensitive. The peer uid is captured via ``SO_PEERCRED``
on Linux; ``null`` on platforms without it (macOS dev boxes).
See :mod:`vq.audit` for the public read API.

## Threading model

The server parses requests and runs ordinary handlers on one accept-loop
thread, preserving serialized mutations. Scheduler-refresh waits alone run
in at most four daemon threads: waiting for the main loop must not block
health checks or admin requests. Excess refresh readers receive an explicit
busy error, never a fabricated fresh observation.
"""

from __future__ import annotations

import contextlib
import json
import logging
import math
import os
import socket
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path
from typing import Any

from vq import __version__, paths
from vq.spec import validate_job_id

log = logging.getLogger(__name__)

# Connect / read timeout for clients. Short — the daemon's
# responses are local + cheap; a hang means something's wrong.
DEFAULT_CLIENT_TIMEOUT_SECONDS = 5.0
MAX_JSON_DEPTH = 64
SCHEDULER_STATUS_REFRESH_RPC_METHOD = "get_scheduler_status_refresh"
SCHEDULER_STATUS_REFRESH_MAX_SECONDS = 30.0

# Server-side accept-loop poll interval. Short enough that a
# stop request is acted on quickly; long enough that the kernel
# isn't burning CPU on no-op loops.
SERVER_POLL_INTERVAL_SECONDS = 0.5


def _capture_source_value(
    reader: Callable[[], str | None] | None,
) -> str | None:
    """Capture one optional provenance value without blocking RPC startup."""
    if reader is None:
        return None
    try:
        return reader()
    except Exception:  # noqa: BLE001 - health remains useful without provenance
        return None


def socket_path(*, multi_user: bool = False) -> Path:
    """Resolve the RPC socket path. Mirrors :func:`paths.daemon_pidfile`
    in mode awareness: single-user puts the socket under the user's
    XDG state root; multi-user puts it under the system root so any
    admin-group caller can find it without guessing the daemon's
    home dir.
    """
    if multi_user:
        return paths.multi_user_root() / "daemon.sock"
    return paths.state_root() / "daemon.sock"


def user_socket_path() -> Path:
    """Resolve the socket of the *per-user* daemon, specifically.

    ``socket_path(multi_user=False)`` expresses an intent that
    ``$VQ_STATE_DIR`` can silently defeat: admins on a multi-user host are
    documented to run admin verbs as ``VQ_STATE_DIR=/var/lib/vq vq admin ...``
    (``docs/state_file_audit.md``), and the multi-user unit itself carries that
    value, so the "single-user" path resolves onto the *root system* daemon's
    socket. A caller that has just restarted a user daemon and wants to hear
    back from that daemon gets the wrong process, confidently.

    So: honour the override normally — tests and genuine relocated single-user
    state roots depend on it — but when it lands exactly on the multi-user
    socket, fall back to the XDG default. That path is provably not a per-user
    daemon, and the user daemon's unit never sets ``$VQ_STATE_DIR``.
    """
    single = socket_path(multi_user=False)
    if single == socket_path(multi_user=True):
        return paths.xdg_state_root() / "daemon.sock"
    return single


# ----------------------------------------------------------------------
# Protocol helpers
# ----------------------------------------------------------------------


class RPCError(RuntimeError):
    """RPC-level failure. Raised by the client when the server
    returns ``{"ok": false}`` or the connection breaks mid-protocol.
    Distinct from connection failure (which surfaces as
    ``ConnectionError`` from the socket layer)."""


def _encode_request(method: str, args: dict[str, Any]) -> bytes:
    return (json.dumps({"method": method, "args": args}) + "\n").encode("utf-8")


def _encode_response(ok: bool, *, result: Any = None, error: str | None = None) -> bytes:
    payload: dict[str, Any] = {"ok": ok}
    if ok:
        payload["result"] = result
    else:
        payload["error"] = error or "unknown error"
    return (json.dumps(payload, default=str) + "\n").encode("utf-8")


def _read_line(conn: socket.socket, *, timeout: float) -> bytes:
    """Read one newline-terminated line from ``conn`` with the given
    timeout. Defends against the case where the peer closes mid-line
    by returning what was received so far (caller surfaces as a
    protocol error)."""
    conn.settimeout(timeout)
    buf = bytearray()
    while True:
        chunk = conn.recv(4096)
        if not chunk:
            break
        buf.extend(chunk)
        if b"\n" in chunk:
            # Trim at the first newline; ignore anything after (one
            # request per connection).
            nl = buf.index(b"\n")
            return bytes(buf[:nl])
    return bytes(buf)


def _validate_json_depth(value: object) -> None:
    """Reject pathological request/response nesting before dispatch."""
    pending: list[tuple[object, int]] = [(value, 0)]
    while pending:
        item, depth = pending.pop()
        if depth > MAX_JSON_DEPTH:
            raise ValueError(
                f"JSON nesting exceeds {MAX_JSON_DEPTH} levels"
            )
        if isinstance(item, dict):
            pending.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            pending.extend((child, depth + 1) for child in item)


# ----------------------------------------------------------------------
# Client
# ----------------------------------------------------------------------


def call(
    method: str,
    args: dict[str, Any] | None = None,
    *,
    multi_user: bool = False,
    timeout: float = DEFAULT_CLIENT_TIMEOUT_SECONDS,
    socket_override: Path | None = None,
) -> Any:
    """Make one RPC call. Returns the ``result`` field of a successful
    response; raises ``RPCError`` on a failure response and
    ``ConnectionError`` (or subclasses) on socket-level failure.

    Args:
        method: the method name (e.g. ``"get_admin_status"``).
        args: kwargs dict for the method. ``None`` is treated as ``{}``.
        multi_user: which socket path to use (mirrors
            :func:`socket_path`).
        timeout: connect + read timeout in seconds.
        socket_override: connect to this socket instead of the one
            ``multi_user`` selects. For callers that must reach one specific
            daemon (see :func:`user_socket_path`) rather than whichever daemon
            this process's environment points at.

    Connection is closed by this function regardless of outcome.
    """
    if args is None:
        args = {}
    elif not isinstance(args, dict):
        raise TypeError("RPC args must be a dict or None")
    sock_path = (socket_override or socket_path(multi_user=multi_user)).expanduser()
    paths.require_test_path_within_sandbox(sock_path, "vq RPC socket")
    try:
        socket_exists = sock_path.exists()
    except OSError as e:
        raise ConnectionError(
            f"RPC socket probe failed at {sock_path}: {e}"
        ) from e
    if not socket_exists:
        raise ConnectionError(
            f"RPC socket not found at {sock_path} (daemon down?)"
        )
    conn: socket.socket | None = None
    try:
        try:
            conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            conn.settimeout(timeout)
        except (OSError, ValueError) as e:
            raise ConnectionError(
                f"RPC {method!r} socket setup failed at {sock_path}: {e}"
            ) from e
        try:
            conn.connect(str(sock_path))
        except OSError as e:
            raise ConnectionError(
                f"RPC connect to {sock_path} failed: {e}"
            ) from e
        try:
            conn.sendall(_encode_request(method, args))
            line = _read_line(conn, timeout=timeout)
        except OSError as e:
            raise ConnectionError(
                f"RPC {method!r} transport failed at {sock_path}: {e}"
            ) from e
    finally:
        if conn is not None:
            with contextlib.suppress(Exception):
                conn.close()
    if not line:
        raise RPCError(
            f"RPC {method!r}: empty response (daemon closed mid-protocol)"
        )
    try:
        payload = json.loads(line)
        _validate_json_depth(payload)
    except (ValueError, RecursionError) as e:
        raise RPCError(
            f"RPC {method!r}: malformed response: {e}; got {line!r}"
        ) from e
    if (
        not isinstance(payload, dict)
        or "ok" not in payload
        or type(payload["ok"]) is not bool
    ):
        raise RPCError(
            f"RPC {method!r}: bad response shape: {payload!r}"
        )
    if payload["ok"]:
        return payload.get("result")
    raise RPCError(
        f"RPC {method!r} failed: {payload.get('error', 'no error message')}"
    )


def ping(*, multi_user: bool = False) -> dict[str, Any] | None:
    """Probe the daemon RPC. Returns the daemon's response dict on
    success; returns ``None`` on any connection failure (so callers
    can do ``if ping() is None: fallback``)."""
    try:
        return call("ping", multi_user=multi_user)
    except (ConnectionError, RPCError):
        return None


def ping_user_daemon() -> dict[str, Any] | None:
    """Probe the *per-user* daemon, whatever ``$VQ_STATE_DIR`` says.

    Same contract as :func:`ping`, but pinned to :func:`user_socket_path` so
    the answer comes from the daemon a ``systemctl --user`` / launchd-user
    restart actually touched. Used by post-restart provenance verification,
    which is meaningless if a different daemon answers.
    """
    try:
        return call("ping", socket_override=user_socket_path())
    except (ConnectionError, RPCError):
        return None


def _validate_scheduler_status_refresh_result(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise RPCError("scheduler status refresh response must be an object")
    if value.get("schema") != "vq.scheduler.status_refresh/1":
        raise RPCError("scheduler status refresh response has an unsupported schema")
    completed = value.get("completed")
    observed_at = value.get("observed_at")
    reason = value.get("reason")
    if type(completed) is not bool:
        raise RPCError("scheduler status refresh completed must be a boolean")
    if completed:
        if not isinstance(observed_at, str) or not observed_at:
            raise RPCError(
                "completed scheduler status refresh requires observed_at"
            )
        if reason is not None:
            raise RPCError(
                "completed scheduler status refresh must not claim a failure reason"
            )
    elif observed_at is not None:
        raise RPCError(
            "incomplete scheduler status refresh must not claim observed_at"
        )
    elif reason is not None and (not isinstance(reason, str) or not reason):
        raise RPCError(
            "scheduler status refresh reason must be a non-empty string or null"
        )
    return {
        "schema": "vq.scheduler.status_refresh/1",
        "completed": completed,
        "observed_at": observed_at,
        "reason": reason,
    }


def request_scheduler_status_refresh(
    jobid: str,
    *,
    multi_user: bool = False,
    timeout_seconds: float = DEFAULT_CLIENT_TIMEOUT_SECONDS,
) -> dict[str, object] | None:
    """Request one fresh daemon-owned scheduler observation.

    ``None`` means the daemon is down or predates this additive RPC.  Once a
    daemon advertises the method, transport/protocol failures are surfaced:
    callers must not render a stale state as though the refresh succeeded.
    """
    jobid = validate_job_id(jobid)
    if (
        isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, (int, float))
        or not math.isfinite(float(timeout_seconds))
        or float(timeout_seconds) <= 0
        or float(timeout_seconds) > SCHEDULER_STATUS_REFRESH_MAX_SECONDS
    ):
        raise ValueError(
            "timeout_seconds must be finite, greater than zero, and at most "
            f"{SCHEDULER_STATUS_REFRESH_MAX_SECONDS:g}"
        )
    timeout = float(timeout_seconds)
    deadline = time.monotonic() + timeout
    try:
        methods = call("get_methods", multi_user=multi_user, timeout=timeout)
    except (ConnectionError, RPCError):
        return None
    if not isinstance(methods, dict) or not isinstance(methods.get("methods"), list):
        raise RPCError("get_methods returned an invalid method inventory")
    if SCHEDULER_STATUS_REFRESH_RPC_METHOD not in methods["methods"]:
        return None
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise RPCError("scheduler status refresh deadline elapsed before request")
    response_reserve = min(0.1, remaining / 4)
    server_timeout = remaining - response_reserve
    result = call(
        SCHEDULER_STATUS_REFRESH_RPC_METHOD,
        {"jobid": jobid, "timeout_seconds": server_timeout},
        multi_user=multi_user,
        timeout=remaining,
    )
    return _validate_scheduler_status_refresh_result(result)


def try_rpc_or_fallback(
    method: str,
    args: dict[str, Any] | None = None,
    *,
    multi_user: bool = False,
    fallback: Callable[[], Any],
    fallback_warns_in_multi_user: bool = True,
) -> Any:
    """Try the RPC; on any failure run ``fallback()`` and return its
    result. In multi-user mode, log a WARNING when falling back
    because the fallback path reads/writes the user's local
    ``admin-status.json`` rather than the daemon's canonical one —
    operator wants to know they're seeing a stale view.

    Use this from CLI verbs that want the daemon's authoritative
    view when possible but degrade gracefully if the daemon is
    down (admin operations should still work during a daemon
    restart).
    """
    try:
        return call(method, args, multi_user=multi_user)
    except (ConnectionError, RPCError) as e:
        if multi_user and fallback_warns_in_multi_user:
            log.warning(
                "RPC %s failed (%s); falling back to direct file "
                "access — this may read a stale view in multi-user "
                "mode (the daemon's canonical admin-status.json "
                "lives at %s but this client will read the user's "
                "local copy). Restart the daemon to restore RPC.",
                method, e, socket_path(multi_user=True),
            )
        return fallback()


# ----------------------------------------------------------------------
# Server
# ----------------------------------------------------------------------


class RPCServer:
    """Accept-loop wrapper around a Unix domain socket. Methods are
    registered via the ``register`` decorator at module-init time;
    the server thread dispatches incoming requests to the registered
    handlers.

    Daemon bootstrap supplies source-identity readers; their values are
    captured once during construction.  Isolated protocol/test servers may
    omit them and advertise unknown (``None``) provenance.

    Usage from the daemon:

        rpc = RPCServer(multi_user=self._multi_user)
        register_get_admin_status_method(rpc, read_admin_status)
        register_set_admin_status_method(rpc, replace_admin_status_record)
        rpc.start()
        try:
            ...  # main daemon work
        finally:
            rpc.stop()
    """

    def __init__(
        self,
        *,
        multi_user: bool = False,
        admin_group_gid: int | None = None,
        source_sha_reader: Callable[[], str | None] | None = None,
        source_tree_sha256_reader: Callable[[], str | None] | None = None,
    ) -> None:
        self.multi_user = multi_user
        self.admin_group_gid = admin_group_gid
        self._socket: socket.socket | None = None
        self._socket_path: Path = socket_path(multi_user=multi_user)
        self._methods: dict[str, Callable[..., Any]] = {}
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._refresh_connections: set[socket.socket] = set()
        self._refresh_lock = threading.Lock()
        # Capture source identity exactly once.  A self-update can move the
        # files on disk while this process still executes old imported code;
        # live re-reads would falsely report that the daemon restarted.
        self._source_sha = _capture_source_value(source_sha_reader)
        self._source_tree_sha256 = _capture_source_value(
            source_tree_sha256_reader,
        )
        # Built-in ping always registered.
        self.register("ping")(self._handle_ping)
        # v0.8.4: built-in method introspection so clients can probe
        # what's available before calling unknown methods.
        self.register("get_methods")(self._handle_get_methods)
        self.register("get_process_identity")(
            self._handle_get_process_identity
        )

    def register(self, name: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Decorator to register a method. Handler signature:
        ``handler(**args) -> result``. The args dict from the
        request is splatted in as kwargs; the return value
        becomes the response ``result``."""
        def _deco(fn: Callable[..., Any]) -> Callable[..., Any]:
            self._methods[name] = fn
            return fn
        return _deco

    def _handle_ping(self) -> dict[str, Any]:
        return {
            "version": __version__,
            "multi_user": self.multi_user,
            "source_sha": self._source_sha,
            "source_tree_sha256": self._source_tree_sha256,
        }

    def _handle_get_methods(self) -> dict[str, Any]:
        """v0.8.4 *Brooks's Mythical*: return the registered method
        names + daemon version so clients can probe what's supported
        before calling. Closes the version-skew problem: a v0.8.5
        client talking to a v0.8.3 daemon can ask "do you know about
        the method I'm about to call?" instead of falling back from
        an opaque RPCError.

        Returns ``{methods: [...], version: <vq>, multi_user: bool}``.
        Methods list is sorted for stable monitoring-script diffs.
        """
        return {
            "methods": sorted(self._methods.keys()),
            "version": __version__,
            "multi_user": self.multi_user,
            "source_sha": self._source_sha,
            "source_tree_sha256": self._source_tree_sha256,
        }

    def _handle_get_process_identity(self) -> dict[str, Any]:
        """Return one responder-bound identity snapshot without credentials."""
        return {
            "pid": os.getpid(),
            "euid": os.geteuid(),
            "python_executable": sys.executable,
            "argv": list(sys.argv),
            # Repeat the startup-captured provenance in this same response.
            # A daemon can restart between the ordinary ping and this call;
            # consumers must not pair an old ping's SHA with a new PID.
            "version": __version__,
            "multi_user": self.multi_user,
            "source_sha": self._source_sha,
            "source_tree_sha256": self._source_tree_sha256,
            "socket_path": str(self._socket_path),
        }

    def start(self) -> None:
        """Open the socket, set permissions, start the accept thread.
        Idempotent — calling twice is a no-op."""
        if self._thread is not None:
            return
        # Stale socket file cleanup. If the daemon crashed and left
        # the socket behind, bind() would fail. Best-effort unlink.
        try:
            if self._socket_path.exists():
                self._socket_path.unlink()
        except OSError as e:
            log.warning(
                "RPC: failed to unlink stale socket %s: %s",
                self._socket_path, e,
            )
        self._socket_path.parent.mkdir(parents=True, exist_ok=True)
        self._socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._socket.bind(str(self._socket_path))
        # Permissions: 0600 single-user, 0660 multi-user. The
        # multi-user case also chowns to the admin group so admins
        # can connect without being the daemon-owning user.
        if self.multi_user:
            mode = 0o660
            if self.admin_group_gid is not None:
                try:
                    os.chown(self._socket_path, os.geteuid(),
                             self.admin_group_gid)
                except OSError as e:
                    log.warning(
                        "RPC: failed to chown socket %s to gid=%d: "
                        "%s — admin-group clients may be unable to "
                        "connect.",
                        self._socket_path, self.admin_group_gid, e,
                    )
        else:
            mode = 0o600
        try:
            os.chmod(self._socket_path, mode)
        except OSError as e:
            log.warning(
                "RPC: failed to chmod socket %s to %o: %s",
                self._socket_path, mode, e,
            )
        self._socket.listen(8)
        self._socket.settimeout(SERVER_POLL_INTERVAL_SECONDS)
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._accept_loop,
            name="vq-rpc-server",
            daemon=True,
        )
        self._thread.start()
        log.info(
            "RPC server listening on %s (mode=%o, methods=%d)",
            self._socket_path, mode, len(self._methods),
        )

    def stop(self) -> None:
        """Signal the accept loop, close the socket, unlink the
        socket file. Idempotent."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None
        # The daemon wakes refresh callbacks before stopping RPC. Close their
        # transports even if an isolated callback has not returned yet, so a
        # late response cannot outlive this server's socket endpoint.
        with self._refresh_lock:
            for conn in self._refresh_connections:
                with contextlib.suppress(OSError):
                    conn.shutdown(socket.SHUT_RDWR)
                conn.close()
        if self._socket is not None:
            with contextlib.suppress(Exception):
                self._socket.close()
            self._socket = None
        try:
            if self._socket_path.exists():
                self._socket_path.unlink()
        except OSError:
            pass

    def _accept_loop(self) -> None:
        """Parse inline; transfer only scheduler waits to bounded workers."""
        assert self._socket is not None
        while not self._stop_event.is_set():
            try:
                conn, _ = self._socket.accept()
            except TimeoutError:
                continue
            except OSError as e:
                # If the socket was closed mid-accept, exit cleanly.
                if self._stop_event.is_set():
                    return
                log.warning("RPC accept failed: %s", e)
                continue
            deferred = False
            try:
                deferred = bool(self._handle_one(conn))
            except Exception as e:  # noqa: BLE001 — last-ditch
                log.warning(
                    "RPC handler raised %s: %s",
                    type(e).__name__, e,
                )
            finally:
                if not deferred:
                    with contextlib.suppress(Exception):
                        conn.close()

    def _handle_one(self, conn: socket.socket) -> bool | None:
        line = _read_line(conn, timeout=DEFAULT_CLIENT_TIMEOUT_SECONDS)
        if not line:
            conn.sendall(_encode_response(
                ok=False, error="empty request",
            ))
            return
        try:
            req = json.loads(line)
            _validate_json_depth(req)
        except (ValueError, RecursionError) as e:
            conn.sendall(_encode_response(
                ok=False, error=f"malformed JSON: {e}",
            ))
            return
        if not isinstance(req, dict):
            conn.sendall(_encode_response(
                ok=False, error="request must be a JSON object",
            ))
            return
        method = req.get("method")
        args = req.get("args", {})
        if not isinstance(method, str) or not method:
            conn.sendall(_encode_response(
                ok=False, error="missing or empty 'method'",
            ))
            return
        from vq import audit as _audit
        should_audit = method.startswith("set_")
        peer_uid: int | None = None
        if should_audit:
            peer_uid = _audit.peer_uid(conn)
        if not isinstance(args, dict):
            if should_audit:
                _audit.append_audit_line(
                    method=method,
                    uid=peer_uid,
                    ok=False,
                    args_summary=f"invalid args type={type(args).__name__}",
                    error="'args' must be an object",
                    multi_user=self.multi_user,
                )
            conn.sendall(_encode_response(
                ok=False, error="'args' must be an object",
            ))
            return
        handler = self._methods.get(method)
        if handler is None:
            conn.sendall(_encode_response(
                ok=False,
                error=f"unknown method: {method!r} "
                      f"(known: {sorted(self._methods.keys())})",
            ))
            return
        if method == SCHEDULER_STATUS_REFRESH_RPC_METHOD:
            return self._defer_scheduler_refresh(conn, method, handler, args)
        self._respond(conn, method, handler, args, peer_uid)
        return None

    def _defer_scheduler_refresh(
        self,
        conn: socket.socket,
        method: str,
        handler: Callable[..., Any],
        args: dict[str, Any],
    ) -> bool:
        """Transfer one read-only wait; never queue an unbounded backlog."""
        def wait_and_respond() -> None:
            try:
                self._respond(conn, method, handler, args, None)
            except OSError:
                log.debug("scheduler refresh client disconnected")
            except Exception:
                log.exception("scheduler refresh RPC failed")
            finally:
                conn.close()
                with self._refresh_lock:
                    self._refresh_connections.discard(conn)

        with self._refresh_lock:
            if self._stop_event.is_set() or len(self._refresh_connections) >= 4:
                conn.sendall(_encode_response(
                    ok=False, error="scheduler refresh busy or server stopping",
                ))
                return False
            self._refresh_connections.add(conn)
            try:
                threading.Thread(
                    target=wait_and_respond,
                    name="vq-rpc-scheduler-refresh",
                    daemon=True,
                ).start()
            except Exception:
                self._refresh_connections.discard(conn)
                raise
        return True

    def _respond(
        self,
        conn: socket.socket,
        method: str,
        handler: Callable[..., Any],
        args: dict[str, Any],
        peer_uid: int | None,
    ) -> None:
        from vq import audit as _audit

        should_audit = method.startswith("set_")
        # v0.8.6 *Codd's Audit*: capture the peer uid + summary
        # before dispatching. Only mutating methods (set_*) get
        # audited; reads are high-volume + not sensitive. Audit
        # lines land regardless of handler outcome (the failure
        # is itself forensically interesting).
        args_summary: str = ""
        if should_audit:
            args_summary = _audit.summarise_args(method, args)
        try:
            result = handler(**args)
        except TypeError as e:
            # Bad args (e.g. missing kwarg) — surface as a 4xx-ish
            # error to the client.
            if should_audit:
                _audit.append_audit_line(
                    method=method, uid=peer_uid, ok=False,
                    args_summary=args_summary,
                    error=f"bad args: {e}",
                    multi_user=self.multi_user,
                )
            conn.sendall(_encode_response(
                ok=False, error=f"bad args for {method}: {e}",
            ))
            return
        except Exception as e:  # noqa: BLE001 — handler may raise
            if should_audit:
                _audit.append_audit_line(
                    method=method, uid=peer_uid, ok=False,
                    args_summary=args_summary,
                    error=f"{type(e).__name__}: {e}",
                    multi_user=self.multi_user,
                )
            conn.sendall(_encode_response(
                ok=False,
                error=f"handler {method} raised {type(e).__name__}: {e}",
            ))
            return
        if should_audit:
            _audit.append_audit_line(
                method=method, uid=peer_uid, ok=True,
                args_summary=args_summary,
                multi_user=self.multi_user,
            )
        conn.sendall(_encode_response(ok=True, result=result))


# ----------------------------------------------------------------------
# Admin-status methods — the canonical v0.8.0 use case
# ----------------------------------------------------------------------


def register_get_admin_status_method(
    rpc: RPCServer,
    read_status: Callable[[], dict[str, Any]],
) -> None:
    """Register the canonical admin-status view through one callback.

    The injected reader owns the daemon's direct state lookup.  The transport
    retains dataclass serialization and the open, read-only wire contract.
    """

    @rpc.register("get_admin_status")
    def _get_admin_status() -> dict[str, dict[str, Any]]:
        records = read_status()
        # Serialize dataclasses to plain dicts so JSON encoding
        # doesn't trip on the dataclass type.
        return {name: asdict(rec) for name, rec in records.items()}


def register_set_admin_status_method(
    rpc: RPCServer,
    replace_record: Callable[[str, dict[str, Any]], None],
) -> None:
    """Register the admin-status mutation through one neutral callback.

    The RPC adapter retains authorization, auditing, and the response envelope.
    The injected callback owns mixed-version schema filtering, dataclass
    construction, and direct persistence in the daemon's canonical state tree.
    """

    @rpc.register("set_admin_status")
    def _set_admin_status(
        env: str,
        record: dict[str, Any],
        token: str | None = None,
    ) -> dict[str, Any]:
        # Multi-user write gate: token required, matched against the
        # admin-token file the same way `vq admin update` checks.
        if rpc.multi_user:
            from vq import auth as _auth
            if not _auth.verify_admin_token(token or ""):
                raise PermissionError(
                    "set_admin_status: admin token required in "
                    "multi-user mode. Pass via the 'token' arg."
                )
        replace_record(env, record)
        return {"env": env, "ok": True}


def register_capacity_methods(rpc: RPCServer, advertised: Any) -> None:
    """Expose one daemon-start capacity snapshot over RPC.

    The snapshot comes from daemon construction, not a file re-read.  A healthy
    daemon therefore remains authoritative even if its best-effort fallback
    file could not be written.
    """
    from vq.capacity import DaemonCapacity

    snapshot = DaemonCapacity.model_validate(advertised)

    @rpc.register("get_daemon_capacity")
    def _get_daemon_capacity() -> dict[str, Any]:
        return snapshot.model_dump(mode="json")


def register_get_scheduler_status_refresh_method(
    rpc: RPCServer,
    refresh: Callable[[str, float], dict[str, object]],
) -> None:
    """Register the bounded main-loop scheduler-refresh handoff."""

    @rpc.register(SCHEDULER_STATUS_REFRESH_RPC_METHOD)
    def _get_scheduler_status_refresh(
        jobid: str,
        timeout_seconds: float,
    ) -> dict[str, object]:
        jobid = validate_job_id(jobid)
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(float(timeout_seconds))
            or float(timeout_seconds) <= 0
            or float(timeout_seconds) > SCHEDULER_STATUS_REFRESH_MAX_SECONDS
        ):
            raise ValueError(
                "timeout_seconds must be finite, greater than zero, and at most "
                f"{SCHEDULER_STATUS_REFRESH_MAX_SECONDS:g}"
            )
        return _validate_scheduler_status_refresh_result(
            refresh(jobid, float(timeout_seconds))
        )


# ----------------------------------------------------------------------
# Drain + throttle methods — the v0.8.1 *Karp's Reduction* use case
# ----------------------------------------------------------------------


def register_get_drain_state_method(
    rpc: RPCServer,
    read_state: Callable[[], Any],
) -> None:
    """Register the read-only drain view through one neutral callback.

    The daemon owns which state tree this callback reads.  Keeping that choice
    outside the transport prevents the RPC layer from rediscovering a possibly
    changed multi-user mode after daemon startup.
    """

    @rpc.register("get_drain_state")
    def _get_drain_state() -> dict[str, Any] | None:
        state = read_state()
        if state is None:
            return None
        return state.model_dump()


def register_get_drain_read_only_snapshot_method(
    rpc: RPCServer,
    read_snapshot: Callable[[], dict[str, object]],
) -> None:
    """Register one atomic legacy/lease snapshot with responder provenance."""

    @rpc.register("get_drain_read_only_snapshot")
    def _get_drain_read_only_snapshot() -> dict[str, Any]:
        snapshot = read_snapshot()
        if not isinstance(snapshot, dict):
            raise ValueError("read-only drain snapshot must be an object")
        legacy_error = snapshot.get("legacy_error")
        leases_error = snapshot.get("scheduler_leases_error")
        if legacy_error not in {None, "unreadable"}:
            raise ValueError("invalid legacy snapshot coverage")
        if leases_error not in {None, "unreadable"}:
            raise ValueError("invalid scheduler-lease snapshot coverage")
        raw_leases = snapshot.get("scheduler_leases")
        if not isinstance(raw_leases, list):
            raise ValueError("scheduler_leases must be a list")
        return {
            "schema": "vq.drain.read_only_snapshot/1",
            "observed_at": snapshot.get("observed_at"),
            "provenance": {
                "method": "get_drain_read_only_snapshot",
                "version": __version__,
                "source_sha": rpc._source_sha,  # noqa: SLF001
                "source_tree_sha256": rpc._source_tree_sha256,  # noqa: SLF001
                "multi_user": rpc.multi_user,
            },
            "coverage": {
                "legacy_state": legacy_error is None,
                "scheduler_leases": leases_error is None,
            },
            "legacy_state": (
                snapshot.get("legacy_state") if legacy_error is None else None
            ),
            "scheduler_lease_store": (
                {
                    "schema_version": 1,
                    "leases": raw_leases,
                }
                if leases_error is None
                else None
            ),
        }


def register_get_scheduler_drain_leases_method(
    rpc: RPCServer,
    read_leases: Callable[[], list[Any]],
    *,
    schema_version: int,
) -> None:
    """Register the read-only scheduler-drain lease view.

    The injected reader owns the state root and returns domain models.  This
    transport adapter preserves the versioned JSON envelope consumed by
    mixed-version clients without importing the drain domain.
    """

    @rpc.register("get_scheduler_drain_leases")
    def _get_scheduler_drain_leases() -> dict[str, Any]:
        return {
            "schema_version": schema_version,
            "leases": [
                lease.model_dump(mode="json") for lease in read_leases()
            ],
        }


def register_legacy_scheduler_drain_release_method(
    rpc: RPCServer,
    release_host: Callable[[str, str | None, str | None], bool],
) -> None:
    """Register the atomic legacy scheduler-lane release mutation.

    The RPC adapter retains input validation, authorization, auditing, and the
    response envelope.  The injected callback performs one direct, locked
    domain transaction against the state root selected at daemon startup.
    """

    @rpc.register("set_legacy_scheduler_drain_release")
    def _release_legacy_scheduler_drain(
        host: str,
        expected_reason: str | None = None,
        expected_set_at: str | None = None,
        token: str | None = None,
    ) -> dict[str, Any]:
        if not isinstance(host, str) or not host.strip():
            raise ValueError("host must be a non-empty string")
        if rpc.multi_user:
            from vq import auth as _auth

            if not _auth.verify_admin_token(token or ""):
                raise PermissionError(
                    "set_legacy_scheduler_drain_release: admin token required in "
                    "multi-user mode. Pass via the 'token' arg."
                )
        changed = release_host(
            host.strip(),
            expected_reason,
            expected_set_at,
        )
        return {"changed": changed, "ok": True}


def register_owned_full_drain_release_method(
    rpc: RPCServer,
    release_owned: Callable[[str, str], bool],
) -> None:
    """Register one exact compare-and-release full-drain mutation.

    The injected callback is pinned to the daemon's startup-selected state
    tree and performs the comparison and mutation under one domain lock.  The
    transport validates both identity fields and authorizes the caller before
    invoking it; exposing a dedicated ``set_*`` method also gives the attempt
    the ordinary mutation audit trail.
    """

    @rpc.register("set_owned_full_drain_release")
    def _release_owned_full_drain(
        expected_reason: str,
        expected_set_at: str,
        token: str | None = None,
    ) -> dict[str, Any]:
        if (
            not isinstance(expected_reason, str)
            or not expected_reason.strip()
        ):
            raise ValueError("expected_reason must be a non-empty string")
        if (
            not isinstance(expected_set_at, str)
            or not expected_set_at.strip()
        ):
            raise ValueError("expected_set_at must be a non-empty string")
        if rpc.multi_user:
            from vq import auth as _auth

            if not _auth.verify_admin_token(token or ""):
                raise PermissionError(
                    "set_owned_full_drain_release: admin token required in "
                    "multi-user mode. Pass via the 'token' arg."
                )
        changed = release_owned(expected_reason, expected_set_at)
        return {"changed": changed, "ok": True}


def register_set_drain_state_method(
    rpc: RPCServer,
    *,
    clear_state: Callable[[], bool],
    replace_state: Callable[[dict[str, Any]], bool],
) -> None:
    """Register legacy drain-state replace and clear mutations.

    The RPC adapter owns authorization, branch selection, auditing, and both
    response envelopes.  The injected callbacks own direct state operations
    against the daemon's startup-selected tree.
    """

    @rpc.register("set_drain_state")
    def _set_drain_state(
        state: dict[str, Any] | None = None,
        token: str | None = None,
    ) -> dict[str, Any]:
        if rpc.multi_user:
            from vq import auth as _auth

            if not _auth.verify_admin_token(token or ""):
                raise PermissionError(
                    "set_drain_state: admin token required in "
                    "multi-user mode. Pass via the 'token' arg."
                )
        if state is None:
            return {"cleared": clear_state(), "ok": True}
        return {"enabled": replace_state(state), "ok": True}


def register_set_scheduler_drain_lease_method(
    rpc: RPCServer,
    mutate_leases: Callable[
        [dict[str, Any] | None, str | None, str | None, str | None, bool],
        tuple[list[Any], Any | None, bool],
    ],
    *,
    schema_version: int,
) -> None:
    """Register the atomic scheduler-drain lease mutation.

    The RPC adapter retains structural request validation, authorization,
    auditing, and the versioned response envelope.  The injected callback owns
    strict lease-model validation and one locked domain transaction against the
    state root selected when the daemon started.
    """
    supported_schema_version = schema_version

    @rpc.register("set_scheduler_drain_lease")
    def _set_scheduler_drain_lease(
        schema_version: int,
        lease: dict[str, Any] | None = None,
        release_id: str | None = None,
        release_host: str | None = None,
        release_owner: str | None = None,
        release_all: bool = False,
        token: str | None = None,
    ) -> dict[str, Any]:
        if (
            type(schema_version) is not int
            or schema_version != supported_schema_version
        ):
            raise ValueError(
                f"unsupported scheduler drain lease schema {schema_version}"
            )
        if type(release_all) is not bool:
            raise ValueError("release_all must be a boolean")
        for name, value in (
            ("release_id", release_id),
            ("release_host", release_host),
            ("release_owner", release_owner),
        ):
            if value is not None and (
                not isinstance(value, str) or not value.strip()
            ):
                raise ValueError(f"{name} must be a non-empty string")
        if lease is not None and not isinstance(lease, dict):
            raise ValueError("lease must be an object")
        if rpc.multi_user:
            from vq import auth as _auth

            if not _auth.verify_admin_token(token or ""):
                raise PermissionError(
                    "set_scheduler_drain_lease: admin token required in "
                    "multi-user mode. Pass via the 'token' arg."
                )
        leases, acquired, changed = mutate_leases(
            lease,
            release_id,
            release_host,
            release_owner,
            release_all,
        )
        return {
            "ok": True,
            "schema_version": supported_schema_version,
            "changed": changed,
            "lease": (
                acquired.model_dump(mode="json")
                if acquired is not None
                else None
            ),
            "leases": [item.model_dump(mode="json") for item in leases],
        }


def register_reload_methods(rpc: RPCServer, daemon: Any) -> None:
    """Wire ``set_config_reload`` into ``rpc``.

    The daemon caches config-derived state — most consequentially one
    ``SchedulerDispatcher`` per scheduler host, which snapshots that host's
    ``scheduler_program_hooks``. Before this existed there was no way to make a
    running daemon notice a config fix: ``vq daemon start`` is removed and
    nothing handled SIGHUP, so a full restart was the only path. A config fix
    could therefore land on disk and be ignored for the rest of the daemon's
    life while it dispatched a whole backlog with superseded settings.

    Named ``set_*`` so :meth:`RPCServer._handle_one` audits it — a reload
    changes how subsequent jobs are dispatched, which belongs in the audit log.

    The handler only *requests* the reload; the main loop applies it on its next
    iteration. So no config parsing happens on the RPC thread, and a reload can
    never interleave with an in-progress dispatch.
    """

    @rpc.register("set_config_reload")
    def _set_config_reload(token: str | None = None) -> dict[str, Any]:
        if rpc.multi_user:
            from vq import auth as _auth
            if not _auth.verify_admin_token(token or ""):
                raise PermissionError(
                    "set_config_reload: admin token required in "
                    "multi-user mode. Pass via the 'token' arg."
                )
        daemon.request_config_reload()
        return {"ok": True, "queued": True}


def register_get_throttle_state_method(
    rpc: RPCServer,
    read_state: Callable[[], Any],
) -> None:
    """Register the persistent-throttle view through one neutral callback.

    The injected reader retains the domain's direct-read behavior, including
    automatic expiry, while the transport owns only the wire serialization.
    """

    @rpc.register("get_throttle_state")
    def _get_throttle_state() -> dict[str, Any] | None:
        state = read_state()
        if state is None:
            return None
        return state.model_dump()


def register_set_throttle_state_method(
    rpc: RPCServer,
    *,
    clear_state: Callable[[], bool],
    replace_state: Callable[[dict[str, Any]], int],
) -> None:
    """Register persistent-throttle clear and replace mutations.

    The RPC adapter retains authorization, auditing, branch selection, and the
    response envelopes.  The injected callbacks own direct state operations.
    """

    @rpc.register("set_throttle_state")
    def _set_throttle_state(
        state: dict[str, Any] | None = None,
        token: str | None = None,
    ) -> dict[str, Any]:
        if rpc.multi_user:
            from vq import auth as _auth
            if not _auth.verify_admin_token(token or ""):
                raise PermissionError(
                    "set_throttle_state: admin token required in "
                    "multi-user mode. Pass via the 'token' arg."
                )
        if state is None:
            return {"cleared": clear_state(), "ok": True}
        return {"weight": replace_state(state), "ok": True}
