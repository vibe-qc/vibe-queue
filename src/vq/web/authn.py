"""Local-account authentication for the fleet console (M2).

Design (``docs/fleet_dashboard_design.md`` § 5, Phase B): cookie
sessions in front of the fleet surface, roles ``viewer`` < ``operator``
< ``admin``. This module is the provider-independent core — the signed
session cookie, the on-disk local account store, and the role order.
The OIDC provider (GitLab) plugs into the same session layer once an
OAuth application is registered; until then the local store is both the
only provider and the designed break-glass account.

Deliberately stdlib-only: ``hashlib.scrypt`` for password hashes,
HMAC-SHA256 for the cookie signature, SQLite for revocation and login
admission shared across workers. No new dependency enters the
``[web]`` extra.

Files (mode 0600, refused when wider — same posture as the bearer
token in :mod:`vq.auth`):

* ``<config-dir>/web-users.json`` — ``{user: {"hash": ..., "role": ...}}``
* ``<config-dir>/web-session-secret`` — random 32-byte hex, auto-created
* ``<config-dir>/web-auth/sessions.sqlite3`` — session hashes, account
  fingerprints and bounded login counters, inside a private 0700 directory

With **no users configured, authentication is off**: the fleet pages
stay open exactly like the single-host dashboard (Phase A tunnel
posture), and the write actions answer 503 until an account exists.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import os
import secrets
import sqlite3
import stat
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from vq import config, paths

#: Role order, weakest first. ``role_at_least(have, need)`` compares by
#: index, so adding a role means inserting it at the right rank here.
ROLES = ("viewer", "operator", "admin")

SESSION_COOKIE = "vq_fleet_session"

#: Session lifetime. Half a workday: long enough to not re-login over
#: lunch, short enough that a forgotten browser tab expires same-day.
SESSION_TTL_SECONDS = 12 * 3600

LOGIN_WINDOW_SECONDS = 60
LOGIN_PEER_LIMIT = 20
LOGIN_ACCOUNT_LIMIT = 10
LOGIN_GLOBAL_LIMIT = 200
MAX_SESSIONS_PER_USER = 32
MAX_LOGIN_USER_LENGTH = 128
MAX_LOGIN_PASSWORD_LENGTH = 1024


class LoginRateLimited(Exception):
    def __init__(self, retry_after: int) -> None:
        self.retry_after = retry_after
        super().__init__("too many login attempts; try again later")


@contextmanager
def _auth_db() -> Iterator[sqlite3.Connection]:
    """Private, process-shared session and login-admission state.

    SQLite serializes writers across web workers. The directory prevents
    untrusted replacement of the database and its journal sidecars.
    """
    directory = _local_account_config_dir() / "web-auth"
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = directory.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != 0o700):
        raise PermissionError("web-auth must be an owned private directory (0700)")
    path = directory / "sessions.sqlite3"
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1):
            raise PermissionError("session database must be an owned private regular file (0600)")
    finally:
        os.close(fd)
    db = sqlite3.connect(path, timeout=2)
    try:
        db.execute("PRAGMA trusted_schema=OFF")
        db.execute("CREATE TABLE IF NOT EXISTS sessions "
                   "(token_hash TEXT PRIMARY KEY, user TEXT, account_hash TEXT, expires REAL)")
        db.execute("CREATE TABLE IF NOT EXISTS login_limits "
                   "(key TEXT PRIMARY KEY, attempts INTEGER, expires REAL)")
        with db:
            yield db
    finally:
        db.close()


def _account_hash(user: str, role: str) -> str:
    entry = load_users().get(user)
    if not isinstance(entry, dict) or entry.get("role") != role:
        raise ValueError("session account is missing or its role changed")
    return hashlib.sha256(json.dumps(entry, sort_keys=True).encode()).hexdigest()


def reserve_login_attempt(user: str, peer: str) -> None:
    """Count admission before password work, including concurrent attempts.

    Fixed windows expire from their first attempt; rejected requests do not
    extend them. Keys are hashed so the database contains no peer addresses.
    The global ceiling bounds both password work and cardinality from random
    attacker-supplied names. No client-supplied forwarded header is consulted.
    """
    now = time.time()
    buckets = [("global", LOGIN_GLOBAL_LIMIT),
               ("peer:" + peer, LOGIN_PEER_LIMIT),
               ("user:" + user, LOGIN_ACCOUNT_LIMIT)]
    with _auth_db() as db:
        db.execute("BEGIN IMMEDIATE")
        db.execute("DELETE FROM login_limits WHERE expires <= ?", (now,))
        keys = []
        retry_after = 0
        for name, limit in buckets:
            key = hashlib.sha256(name.encode()).hexdigest()
            keys.append(key)
            row = db.execute("SELECT attempts, expires FROM login_limits WHERE key = ?",
                             (key,)).fetchone()
            if row is not None and row[0] >= limit:
                retry_after = max(retry_after, math.ceil(row[1] - now))
        if retry_after:
            raise LoginRateLimited(retry_after)
        for key in keys:
            db.execute("INSERT INTO login_limits VALUES (?, 1, ?) "
                       "ON CONFLICT(key) DO UPDATE SET attempts = attempts + 1",
                       (key, now + LOGIN_WINDOW_SECONDS))


def authenticate_login(user: str, password: str, peer: str) -> tuple[str, str] | None:
    """Blocking authentication work; the async route runs this in a worker."""
    reserve_login_attempt(user, peer)
    account = load_users().get(user)
    expected = hashlib.sha256(json.dumps(account, sort_keys=True).encode()).hexdigest()
    role = verify_password(user, password)
    if role is None:
        return None
    return role, issue_session(user, role, expected_account_hash=expected)

_SCRYPT_N = 2**14
_SCRYPT_R = 8
_SCRYPT_P = 1


def users_path() -> Path:
    """``web-users.json`` under the legacy local-account config root."""
    return _local_account_config_dir() / "web-users.json"


def session_secret_path() -> Path:
    return _local_account_config_dir() / "web-session-secret"


def _local_account_config_dir() -> Path:
    """Preserve auth-file precedence while sharing the pytest boundary.

    Local-account files historically ignore ``XDG_CONFIG_HOME`` unless
    ``VQ_CONFIG_DIR`` is explicit. Moving an existing deployment's users file
    would make authentication appear disabled, so #527 deliberately keeps
    that production contract while refusing its implicit HOME path in tests.
    """
    if os.environ.get(config.ENV_CONFIG_DIR):
        return config.config_dir()
    paths.require_explicit_test_path(
        config.ENV_CONFIG_DIR,
        "per-user vq config root",
    )
    return Path.home() / ".config" / "vq"


def _refuse_wide_mode(path: Path) -> None:
    mode = stat.S_IMODE(path.stat().st_mode)
    if mode & 0o077:
        raise PermissionError(
            f"{path} is mode {oct(mode)}; refusing to use it. "
            f"Fix with: chmod 600 {path}"
        )


def load_users() -> dict[str, dict[str, str]]:
    """The account store; ``{}`` when no file exists. A malformed file
    raises — silently treating a broken store as 'auth off' would fail
    open."""
    path = users_path()
    if not path.exists():
        return {}
    _refuse_wide_mode(path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected a JSON object")
    return payload


def auth_enabled() -> bool:
    """Auth is on as soon as at least one account exists."""
    try:
        return bool(load_users())
    except (OSError, ValueError, PermissionError):
        # A broken/unreadable store fails CLOSED for the pages that
        # ask (they will demand a login nobody can complete, which an
        # operator notices immediately) rather than silently open.
        return True


def role_at_least(have: str | None, need: str) -> bool:
    if have not in ROLES or need not in ROLES:
        return False
    return ROLES.index(have) >= ROLES.index(need)


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    dk = hashlib.scrypt(
        password.encode("utf-8"),
        salt=salt,
        n=_SCRYPT_N,
        r=_SCRYPT_R,
        p=_SCRYPT_P,
    )
    return (
        f"scrypt${_SCRYPT_N}${_SCRYPT_R}${_SCRYPT_P}"
        f"${salt.hex()}${dk.hex()}"
    )


def _verify_hash(password: str, stored: str) -> bool:
    try:
        scheme, n, r, p, salt_hex, dk_hex = stored.split("$")
        if scheme != "scrypt":
            return False
        dk = hashlib.scrypt(
            password.encode("utf-8"),
            salt=bytes.fromhex(salt_hex),
            n=int(n),
            r=int(r),
            p=int(p),
        )
        return hmac.compare_digest(dk.hex(), dk_hex)
    except (ValueError, TypeError):
        return False


def _write_users(users: dict[str, dict[str, str]]) -> Path:
    path = users_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(users, indent=2, sort_keys=True), encoding="utf-8")
    tmp.chmod(0o600)
    tmp.replace(path)
    return path


def add_user(user: str, password: str, role: str, *, force: bool = False) -> Path:
    if role not in ROLES:
        raise ValueError(f"unknown role {role!r}; valid: {', '.join(ROLES)}")
    if not user or any(c.isspace() for c in user):
        raise ValueError("user name must be non-empty without whitespace")
    if len(user) > MAX_LOGIN_USER_LENGTH or len(password) > MAX_LOGIN_PASSWORD_LENGTH:
        raise ValueError("user name is limited to 128 characters and password to 1024 characters")
    users = load_users()
    if user in users and not force:
        raise FileExistsError(
            f"user {user!r} already exists; pass --force to replace"
        )
    users[user] = {"hash": hash_password(password), "role": role}
    return _write_users(users)


def remove_user(user: str) -> Path:
    users = load_users()
    if user not in users:
        raise FileNotFoundError(f"no such user: {user}")
    del users[user]
    return _write_users(users)


def verify_password(user: str, password: str) -> str | None:
    """Return the user's role on success, None on any failure."""
    try:
        users = load_users()
    except (OSError, ValueError, PermissionError):
        return None
    entry = users.get(user)
    if not isinstance(entry, dict):
        # Burn comparable time so a probe can't distinguish "no such
        # user" from "wrong password" by latency.
        _verify_hash(password, hash_password("x"))
        return None
    if not _verify_hash(password, str(entry.get("hash", ""))):
        return None
    role = str(entry.get("role", ""))
    return role if role in ROLES else None


def session_secret() -> bytes:
    """The cookie-signing secret, auto-created on first use (0600)."""
    path = session_secret_path()
    if path.exists():
        _refuse_wide_mode(path)
        return bytes.fromhex(path.read_text(encoding="utf-8").strip())
    secret = secrets.token_bytes(32)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Publish complete bytes without replacing another worker's first key.
    # A reader must never observe a partially written signing secret.
    fd, temporary = tempfile.mkstemp(prefix=".web-secret-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(secret.hex() + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            _refuse_wide_mode(path)
            return bytes.fromhex(path.read_text(encoding="utf-8").strip())
        return secret
    finally:
        os.unlink(temporary)


def issue_session(
    user: str, role: str, *, ttl_seconds: int = SESSION_TTL_SECONDS,
    expected_account_hash: str | None = None,
) -> str:
    account_hash = _account_hash(user, role)
    if expected_account_hash is not None and account_hash != expected_account_hash:
        raise ValueError("account changed during login; authenticate again")
    payload = {
        "user": user,
        "role": role,
        "exp": int(time.time()) + ttl_seconds,
        "nonce": secrets.token_hex(8),
    }
    body = base64.urlsafe_b64encode(
        json.dumps(payload, sort_keys=True).encode("utf-8")
    ).decode("ascii")
    sig = hmac.new(session_secret(), body.encode("ascii"), hashlib.sha256)
    token = f"{body}.{sig.hexdigest()}"
    with _auth_db() as db:
        db.execute("BEGIN IMMEDIATE")
        db.execute("DELETE FROM sessions WHERE expires <= ?", (time.time(),))
        db.execute("INSERT INTO sessions VALUES (?, ?, ?, ?)",
                   (hashlib.sha256(token.encode()).hexdigest(), user,
                    account_hash, payload["exp"]))
        db.execute("DELETE FROM sessions WHERE token_hash IN "
                   "(SELECT token_hash FROM sessions WHERE user = ? "
                   "ORDER BY expires DESC, rowid DESC LIMIT -1 OFFSET ?)",
                   (user, MAX_SESSIONS_PER_USER))
    return token


def verify_session(token: str | None) -> dict[str, str] | None:
    try:
        return _verify_session(token)
    except (OSError, ValueError, TypeError, sqlite3.Error):
        # Unreadable, missing or corrupt authority never grants access.
        return None


def _verify_session(token: str | None) -> dict[str, str] | None:
    """Decode + verify a session cookie. None on any defect (bad
    signature, expiry, malformed payload, unknown role)."""
    if not token or len(token) > 4096 or "." not in token:
        return None
    body, _, sig = token.rpartition(".")
    expected = hmac.new(
        session_secret(), body.encode("ascii", errors="replace"), hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(sig, expected):
        return None
    try:
        payload = json.loads(base64.urlsafe_b64decode(body.encode("ascii")))
    except (ValueError, TypeError):
        return None
    if not isinstance(payload, dict):
        return None
    if type(payload.get("exp")) is not int or payload["exp"] <= time.time():
        return None
    user = payload.get("user")
    role = payload.get("role")
    if not isinstance(user, str) or role not in ROLES:
        return None
    with _auth_db() as db:
        row = db.execute("SELECT account_hash, expires FROM sessions WHERE token_hash = ?",
                         (hashlib.sha256(token.encode()).hexdigest(),)).fetchone()
    if row is None or row[1] <= time.time() or row[0] != _account_hash(user, role):
        return None
    return {"user": user, "role": role}


def revoke_session(token: str | None) -> None:
    """Revoke just this browser's session, durably, even after a restart."""
    if token:
        with _auth_db() as db:
            db.execute("DELETE FROM sessions WHERE token_hash = ?",
                       (hashlib.sha256(token.encode()).hexdigest(),))


#: Path prefixes the single-host guard never challenges.
#:
#: * ``/static`` — the stylesheet and htmx itself. Gating these would
#:   make the login page render unstyled and without its own scripts.
#: * ``/fleet`` — the fleet surface gates itself, per-route, and owns the
#:   login form. Double-gating here would redirect the login page to
#:   itself.
#: * ``/health`` — liveness and readiness. These exist for supervisors
#:   and uptime checks, which have no session and should not need one;
#:   they expose no job data.
GUARD_EXEMPT_PREFIXES = ("/static", "/fleet", "/health")


def install_single_host_guard(app: object) -> None:
    """Require a session for the single-host surface when accounts exist.

    The fleet console's own pages have required a login since M2, but the
    single-host pages registered on the *same port* never did: ``/queue``,
    ``/jobs/<id>`` (including stdout/stderr tails), ``/api/v1/queue``,
    ``/api/v1/jobs/<id>``, ``/docs`` and ``/openapi.json`` answered
    anybody who could reach the port. On a console bound to a private
    overlay that meant every peer on the overlay, with no credential,
    while ``/fleet`` next door asked for a password. The 2026-08-05 audit
    called this out; this closes it.

    Two deliberate limits:

    * **Only when auth is enabled.** With no accounts configured the
      console is in its documented open "tunnel posture" and nothing
      changes.
    * **Only in fleet mode.** The login form lives on the fleet surface.
      Guarding a plain single-host sidecar, which registers no login
      route, would lock the operator out of their own dashboard with no
      way back in.

    Implemented as middleware rather than a per-route dependency on
    purpose: a dependency has to be remembered on every new route, and
    the surface this is protecting is exactly the one where it was
    forgotten fifteen times.
    """
    from urllib.parse import quote  # noqa: PLC0415

    from fastapi import Request  # noqa: PLC0415 — optional web extra
    from fastapi.responses import JSONResponse, RedirectResponse  # noqa: PLC0415

    @app.middleware("http")  # type: ignore[attr-defined]
    async def _single_host_guard(request: Request, call_next):  # type: ignore[no-untyped-def]
        path = request.url.path
        if path.startswith(GUARD_EXEMPT_PREFIXES) or not auth_enabled():
            return await call_next(request)

        if verify_session(request.cookies.get(SESSION_COOKIE)) is not None:
            return await call_next(request)

        # A bearer token is the scripted-reader equivalent of a session.
        # It already gates the write endpoints, so honouring it for reads
        # keeps `curl -H "Authorization: Bearer ..."` working for the
        # people who were reading /api/v1/queue before this guard existed.
        authz = request.headers.get("authorization", "")
        if authz.lower().startswith("bearer "):
            from vq import auth as _auth  # noqa: PLC0415

            expected = _auth.load_token()
            if expected is not None and _auth.constant_time_eq(
                authz[7:], expected
            ):
                return await call_next(request)

        # An HTML navigation gets sent to the login form; anything else
        # gets a status code, because a browser fetch or a script must
        # fail loudly rather than parse a login page as data.
        accept = request.headers.get("accept", "")
        if "text/html" in accept and request.method == "GET":
            target = path
            if request.url.query:
                target = f"{target}?{request.url.query}"
            return RedirectResponse(
                f"/fleet/login?next={quote(target, safe='')}",
                status_code=302,
            )
        return JSONResponse(
            {"detail": "login or bearer token required"}, status_code=401
        )
