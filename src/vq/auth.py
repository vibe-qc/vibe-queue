"""Shared bearer-token auth for web mutations and admin operations.

The token first shipped in v0.5.1 and remains one shared administrative
secret. Multi-user mode reuses it for privileged CLI and daemon RPC writes;
it is not a per-user identity. The token lives in a 0600-mode file at the
path resolved by :func:`web_token_path`.

Workflow:
* ``vq web init-token`` writes a fresh random token if none exists.
  Idempotent: existing tokens are left alone unless ``--force`` is
  passed.
* The web service reads the token when checking a request; if no token file exists
  the write endpoints return 503 ("token not configured"). Read-only
  endpoints on a plain single-host sidecar work without a token.
* Clients send ``Authorization: Bearer <token>``. The auth dependency
  checks the header against a constant-time-equal of the token file
  content; mismatch -> 401, missing header -> 401.

Why a file rather than env var:
* Survives systemd-user restarts without manual re-export.
* Permissions are explicit (0600). vq refuses to read modes wider than
  that, mirroring how SSH agent / GPG behave.

Authentication boundaries:
* Fleet-console username/password accounts and signed sessions live in
  :mod:`vq.web.authn`; they do not replace or identify the shared bearer.
* Per-user bearer tokens, OIDC, and PAM authentication are not implemented.
"""

from __future__ import annotations

import contextlib
import hmac
import logging
import os
import secrets
import stat
from pathlib import Path

from vq import config, paths

log = logging.getLogger(__name__)

ENV_WEB_TOKEN_FILE = "VQ_WEB_TOKEN_FILE"
"""Override the token-file path. Useful for tests."""


def web_token_path() -> Path:
    """Resolve the token-file location.

    Precedence:

    1. ``$VQ_WEB_TOKEN_FILE`` — explicit override.
    2. ``$VQ_CONFIG_DIR/web-token`` — when the config dir is set
       explicitly (the daemon's systemd unit sets it; an operator
       may too).
    3. ``/etc/vq/web-token`` — v0.6.x: on a multi-user host (a
       system-wide ``/etc/vq/config.toml`` with ``[multi_user]
       enabled = true``) the token belongs next to the system
       config, where the root daemon reads it. Without this,
       ``sudo vq web init-token`` run *without* ``VQ_CONFIG_DIR``
       writes a dead token to ``/root/.config/vq/web-token`` —
       a real footgun hit while rotating the compute-a token.
    4. ``~/.config/vq/web-token`` — single-user default.
    """
    env = os.environ.get(ENV_WEB_TOKEN_FILE)
    if env:
        paths.require_test_path_within_sandbox(env, "vq web token file")
        return Path(env).expanduser()
    # An explicit VQ_CONFIG_DIR wins — honor what the caller chose.
    if os.environ.get(config.ENV_CONFIG_DIR):
        return config.config_dir() / "web-token"
    # Do not even inspect the host's system config from an unsandboxed pytest
    # process. The conftest/gate always supplies an explicit config root.
    paths.require_explicit_test_path(
        config.ENV_CONFIG_DIR,
        "per-user vq config root",
    )
    # No explicit config dir: on a multi-user host, follow the
    # system config location so the CLI and the root daemon agree.
    if config.system_multi_user_enabled():
        token_path = config.SYSTEM_CONFIG_PATH.parent / "web-token"
        paths.require_test_path_within_sandbox(token_path, "vq web token file")
        return token_path
    return config.config_dir() / "web-token"


def generate_token() -> str:
    """Cryptographically random 256-bit token, urlsafe-base64 encoded."""
    return secrets.token_urlsafe(32)


def load_token() -> str | None:
    """Read the token from disk, or None if no file (web write actions
    will return 503 in that case).

    Refuses to read files with mode wider than 0600 -- matches SSH /
    GPG behaviour. Caller can fix with ``chmod 600 <path>`` and retry.
    """
    path = web_token_path()
    if not path.exists():
        return None
    try:
        st = path.stat()
    except OSError as e:
        log.warning("cannot stat web-token file %s: %s", path, e)
        return None
    # On Linux/macOS, st_mode's low bits encode permissions. Refuse if
    # group or other have any access.
    mode = st.st_mode & 0o777
    if mode & 0o077:
        log.error(
            "web-token file %s has too-permissive mode %o; refusing to read. "
            "fix with: chmod 600 %s",
            path,
            mode,
            path,
        )
        return None
    try:
        token = path.read_text().strip()
    except OSError as e:
        log.warning("cannot read web-token file %s: %s", path, e)
        return None
    if not token:
        return None
    return token


def write_token(token: str, *, force: bool = False) -> Path:
    """Persist ``token`` to the canonical path with mode 0600.

    Refuses to overwrite an existing file unless ``force=True`` (so an
    accidental ``vq web init-token`` doesn't invalidate every running
    integration silently). Returns the path written.
    """
    path = web_token_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    if os.path.lexists(path) and not force:
        raise FileExistsError(
            f"refusing to overwrite existing token at {path}; pass --force to rotate"
        )
    payload = token.encode("utf-8") + b"\n"
    if not token or any(character in token for character in "\x00\r\n"):
        raise ValueError("web token must be one non-empty line")
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(16)}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(temporary, flags, 0o600)
    try:
        os.fchmod(fd, 0o600)
        remaining = memoryview(payload)
        while remaining:
            written = os.write(fd, remaining)
            if written <= 0:
                raise OSError("short write while persisting web token")
            remaining = remaining[written:]
        info = os.fstat(fd)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise OSError("unsafe web-token temporary file")
        os.fsync(fd)
    finally:
        os.close(fd)
    parent_fd = os.open(
        path.parent,
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        # Make the fully fsynced temporary entry replayable before its atomic
        # publication. A host crash after replace/link must recover either the
        # old token or this complete temporary inode, never neither.
        os.fsync(parent_fd)
    finally:
        os.close(parent_fd)
    try:
        if force:
            os.replace(temporary, path)
        else:
            # link(2) is an atomic no-replace publish on the same filesystem.
            os.link(temporary, path, follow_symlinks=False)
            temporary.unlink()
        parent_fd = os.open(
            path.parent,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
    finally:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()
    return path


def constant_time_eq(a: str, b: str) -> bool:
    """Wrap ``hmac.compare_digest`` to keep the call site readable.
    Constant-time comparison prevents timing side channels on the token."""
    return hmac.compare_digest(a.encode("utf-8"), b.encode("utf-8"))


def verify_admin_token(token: str) -> bool:
    """v0.6.44: verify a bearer token against the stored web-token.

    Used by admin verbs (``vq admin update``, etc.) when multi-user
    mode is active. The token is passed via ``--token`` or
    ``$VQ_TOKEN``.

    Returns True only when a token file exists **and** the supplied
    token matches it. Returns False when:

    * no token file is configured (run ``vq web init-token``); OR
    * the supplied token does not match the stored one.

    **Security note (pre-v0.6.44 bug).** This used to return True
    when no token file existed, justified as a "single-user compat"
    path. But the only caller — ``vq admin update`` (cli.py) —
    already gates the call on ``cfg.multi_user.enabled``, so the
    compat branch never fired in single-user mode. In multi-user
    mode it instead silently bypassed the admin gate on any host
    that lacked a token file (e.g. before ``vq web init-token``
    had run, or after a misconfigured wipe), letting anyone with
    shell access run ``vq admin update``. Treating a missing token
    file as "auth not configured = no auth" matches what the web
    API has always done (FastAPI ``require_token`` raises 503 in
    that case).
    """
    stored = load_token()
    if stored is None:
        return False
    return constant_time_eq(token, stored)


def resolve_token(
    cli_token: str | None = None,
    *,
    token_stdin: bool = False,
    token_file: str | None = None,
) -> str | None:
    """Resolve the admin bearer token from CLI input, stdin, file, or env.

    Precedence (highest first):

    1. ``cli_token`` — value passed via ``--token TOKEN``. Discouraged:
       the token appears in shell history + ``ps -ef`` argv. Callers
       should print an audit warning when this path fires (see the
       ``warn_argv_token_exposure`` helper).
    2. ``token_stdin=True`` — read one line from ``sys.stdin``,
       trailing newline stripped. The remote-dispatch path uses this
       to forward the token over SSH without putting it on the
       remote process's argv.
    3. ``token_file`` — read the token from a 0600-mode file. Same
       perm-check semantics as :func:`load_token` (refuses wider
       modes, returns the stripped contents).
    4. ``$VQ_TOKEN`` env var — recommended for interactive use.
    5. ``None`` if none of the above is set.

    The argv path (option 1) stays available for backwards compat
    and quick one-off use, but the audit recommendation (security
    review #3) is to migrate scripted callers to stdin / file / env.
    """
    if cli_token:
        return cli_token
    if token_stdin:
        import sys  # noqa: PLC0415

        line = sys.stdin.readline()
        # rstrip newline only; leave embedded whitespace alone in
        # case a future token format ever uses it.
        return line.rstrip("\n").rstrip("\r") or None
    if token_file:
        path = Path(token_file)
        if not path.exists():
            return None
        try:
            st = path.stat()
        except OSError as e:
            log.warning("cannot stat --token-file %s: %s", path, e)
            return None
        mode = st.st_mode & 0o777
        if mode & 0o077:
            log.error(
                "--token-file %s has too-permissive mode %o; refusing to read. "
                "fix with: chmod 600 %s",
                path,
                mode,
                path,
            )
            return None
        try:
            token = path.read_text().strip()
        except OSError as e:
            log.warning("cannot read --token-file %s: %s", path, e)
            return None
        return token or None
    return os.environ.get("VQ_TOKEN")


def warn_argv_token_exposure() -> None:
    """Print a stderr warning that ``--token TOKEN`` leaks via argv.

    Audit recommendation (security review #3): nudge callers to the
    safer input channels. Emitted by ``vq admin update`` whenever
    ``--token`` is passed on the command line. Silenced if
    ``$VQ_SUPPRESS_TOKEN_ARGV_WARNING=1`` is set (for scripted callers
    that have weighed the trade-off and prefer the simpler flag).
    """
    if os.environ.get("VQ_SUPPRESS_TOKEN_ARGV_WARNING") == "1":
        return
    import sys  # noqa: PLC0415

    print(
        "⚠️  --token TOKEN exposes the bearer token in shell history\n"
        "    and process listings (`ps -ef`). Prefer one of:\n"
        "      $VQ_TOKEN env var       (recommended for interactive use)\n"
        "      --token-stdin           (pipe the token in)\n"
        "      --token-file PATH       (read from a 0600 file)\n"
        "    Suppress this warning with VQ_SUPPRESS_TOKEN_ARGV_WARNING=1.",
        file=sys.stderr,
    )


def redact_token_args(argv: list[str]) -> list[str]:
    """Replace sensitive token/key values in ``argv`` with ``"<redacted>"``.

    Used by transport-layer debug logging so the SSH command line
    that ships the token to the remote isn't preserved verbatim in
    logs / journal entries / debug captures.

    Recognises ``--token VALUE`` and submit ``--idempotency-key VALUE``
    pairs (plus their equals forms). ``--token-stdin`` carries no value and
    is left alone. ``$VQ_TOKEN`` lives in the environment and never enters
    argv at all.
    """
    out: list[str] = []
    skip = False
    for arg in argv:
        if skip:
            out.append("<redacted>")
            skip = False
            continue
        if arg.startswith(("--token=", "--idempotency-key=")):
            option = arg.partition("=")[0]
            out.append(f"{option}=<redacted>")
            continue
        out.append(arg)
        if arg in {"--token", "--idempotency-key"}:
            skip = True
    return out


def sensitive_arg_values(argv: list[str]) -> tuple[str, ...]:
    """Return raw credential/idempotency values carried by known argv flags."""
    values: list[str] = []
    index = 0
    while index < len(argv):
        argument = argv[index]
        if argument in {"--token", "--idempotency-key"}:
            if index + 1 < len(argv):
                values.append(argv[index + 1])
            index += 2
            continue
        for option in ("--token=", "--idempotency-key="):
            if argument.startswith(option):
                values.append(argument.removeprefix(option))
                break
        index += 1
    return tuple(value for value in values if value)


def redact_sensitive_text(text: str, values: tuple[str, ...]) -> str:
    """Redact exact known-sensitive argv values echoed by remote stderr."""
    for value in sorted(set(values), key=len, reverse=True):
        text = text.replace(value, "<redacted>")
    return text
