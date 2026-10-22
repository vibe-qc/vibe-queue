"""Gateway API keys (#163): issued once, stored only as a keyed digest.

A key reads ``vqk_<key id>_<secret>``: a public 16-hex id and a 256-bit
url-safe secret. The store keeps ``HMAC-SHA256(pepper, secret)`` beside the
principal, project, scopes, expiry and revocation, never the secret. The
pepper is a private random file next to the store, so a copy of the store
alone cannot test guesses. Every request re-reads the store: revocation
takes effect on the next request, with no cache.
"""
from __future__ import annotations

import contextlib
import fcntl
import hashlib
import hmac
import json
import os
import re
import secrets
import stat
import tempfile
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from vq import paths
from vq.config import GATEWAY_SCOPES

STORE_SCHEMA = "vq.gateway-keys/1"
KEY_RE = re.compile(r"vqk_([0-9a-f]{16})_([A-Za-z0-9_-]{43})")
PRINCIPAL_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._@-]{0,127}")
_MAX_STORE_BYTES = 4 * 1024 * 1024
_LAST_USED_GRANULARITY = timedelta(minutes=1)


class GatewayKeyError(ValueError):
    """A key operation the store refuses."""


class GatewayAuthError(PermissionError):
    """A presented key does not authenticate. The message never echoes it."""


@dataclass(frozen=True)
class GatewayKey:
    key_id: str
    principal: str
    project: str
    scopes: tuple[str, ...]
    created_at: str
    expires_at: str | None
    revoked_at: str | None
    last_used_at: str | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "key_id": self.key_id, "principal": self.principal, "project": self.project,
            "scopes": list(self.scopes), "created_at": self.created_at,
            "expires_at": self.expires_at, "revoked_at": self.revoked_at,
            "last_used_at": self.last_used_at,
        }


def gateway_root() -> Path:
    return paths.state_root() / "gateway"


def _now() -> datetime:
    return datetime.now(UTC)


def _iso(moment: datetime) -> str:
    return moment.isoformat(timespec="seconds")


def _private_dir(root: Path) -> Path:
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = root.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
            or info.st_mode & 0o077):
        raise GatewayKeyError(f"gateway state directory {root} must be private to its owner")
    return root


def _read_private(path: Path, limit: int) -> bytes | None:
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError:
        return None
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or info.st_mode & 0o077 or info.st_size > limit):
            raise GatewayKeyError(f"gateway file {path.name} is not a private regular file")
        return stream.read(limit + 1)


def _write_private(path: Path, data: bytes) -> None:
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        try:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


class KeyStore:
    def __init__(self, root: Path | None = None) -> None:
        self.root = root or gateway_root()

    @contextlib.contextmanager
    def _locked(self) -> Iterator[None]:
        root = _private_dir(self.root)
        fd = os.open(root / "keys.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            os.close(fd)

    def _pepper(self, *, create: bool) -> bytes:
        path = self.root / "pepper"
        data = _read_private(path, 64)
        if data is None:
            if not create:
                raise GatewayAuthError("gateway has no keys")
            data = secrets.token_bytes(32)
            _write_private(path, data)
        if len(data) != 32:
            raise GatewayKeyError("gateway pepper is malformed")
        return data

    def _load(self) -> dict[str, Any]:
        raw = _read_private(self.root / "keys.json", _MAX_STORE_BYTES)
        if raw is None:
            return {"schema": STORE_SCHEMA, "keys": {}}
        data = json.loads(raw)
        if not isinstance(data, dict) or data.get("schema") != STORE_SCHEMA or not isinstance(
            data.get("keys"), dict
        ):
            raise GatewayKeyError("gateway key store is malformed")
        return data

    def _save(self, data: dict[str, Any]) -> None:
        _write_private(self.root / "keys.json",
                       (json.dumps(data, sort_keys=True, indent=2) + "\n").encode())

    @staticmethod
    def _record(key_id: str, raw: dict[str, Any]) -> GatewayKey:
        return GatewayKey(
            key_id=key_id, principal=raw["principal"], project=raw["project"],
            scopes=tuple(raw["scopes"]), created_at=raw["created_at"],
            expires_at=raw.get("expires_at"), revoked_at=raw.get("revoked_at"),
            last_used_at=raw.get("last_used_at"),
        )

    def create(self, *, principal: str, project: str, scopes: Sequence[str],
               expires_days: int | None, projects: Sequence[str]) -> tuple[GatewayKey, str]:
        """Issue one key; the plaintext is returned once and never stored."""
        if not PRINCIPAL_RE.fullmatch(principal):
            raise GatewayKeyError("principal must be 1-128 letters, digits or ._@-")
        if project not in projects:
            raise GatewayKeyError(f"project {project!r} is not configured in [gateway.projects]")
        chosen = tuple(dict.fromkeys(scopes))
        if not chosen or any(scope not in GATEWAY_SCOPES for scope in chosen):
            raise GatewayKeyError(f"scopes must be a non-empty subset of {list(GATEWAY_SCOPES)}")
        if expires_days is not None and not 1 <= expires_days <= 3650:
            raise GatewayKeyError("expiry must be 1 to 3650 days")
        with self._locked():
            pepper = self._pepper(create=True)
            data = self._load()
            key_id = secrets.token_hex(8)
            while key_id in data["keys"]:  # pragma: no cover - 64-bit collision
                key_id = secrets.token_hex(8)
            secret = secrets.token_urlsafe(32)
            now = _now()
            data["keys"][key_id] = {
                "principal": principal, "project": project, "scopes": list(chosen),
                "digest": hmac.new(pepper, secret.encode(), hashlib.sha256).hexdigest(),
                "created_at": _iso(now),
                "expires_at": _iso(now + timedelta(days=expires_days)) if expires_days else None,
                "revoked_at": None, "last_used_at": None,
            }
            self._save(data)
            return self._record(key_id, data["keys"][key_id]), f"vqk_{key_id}_{secret}"

    def list(self) -> list[GatewayKey]:
        with self._locked():
            data = self._load()
        return [self._record(key_id, raw) for key_id, raw in sorted(data["keys"].items())]

    def revoke(self, key_id: str) -> GatewayKey:
        with self._locked():
            data = self._load()
            raw = data["keys"].get(key_id)
            if raw is None:
                raise GatewayKeyError(f"no gateway key {key_id!r}")
            if raw.get("revoked_at") is None:
                raw["revoked_at"] = _iso(_now())
                self._save(data)
            return self._record(key_id, raw)

    def rotate(self, key_id: str, *, projects: Sequence[str]) -> tuple[GatewayKey, str]:
        """Revoke ``key_id`` and issue a new key with the same grant and expiry."""
        old = next((key for key in self.list() if key.key_id == key_id), None)
        if old is None:
            raise GatewayKeyError(f"no gateway key {key_id!r}")
        if old.revoked_at is not None:
            raise GatewayKeyError(f"gateway key {key_id!r} is already revoked")
        days = None
        if old.expires_at is not None:
            remaining = datetime.fromisoformat(old.expires_at) - _now()
            days = max(1, remaining.days)
        new = self.create(principal=old.principal, project=old.project, scopes=old.scopes,
                          expires_days=days, projects=projects)
        self.revoke(key_id)
        return new

    def authenticate(self, presented: str, *, projects: Sequence[str]) -> GatewayKey:
        """The live key for a presented credential, or GatewayAuthError."""
        match = KEY_RE.fullmatch(presented or "")
        if match is None:
            raise GatewayAuthError("invalid gateway API key")
        key_id, secret = match.groups()
        with self._locked():
            pepper = self._pepper(create=False)
            data = self._load()
            raw = data["keys"].get(key_id)
            expected = raw.get("digest", "") if raw else hashlib.sha256().hexdigest()
            actual = hmac.new(pepper, secret.encode(), hashlib.sha256).hexdigest()
            if not hmac.compare_digest(actual, expected) or raw is None:
                raise GatewayAuthError("invalid gateway API key")
            now = _now()
            if raw.get("revoked_at") is not None:
                raise GatewayAuthError("gateway API key is revoked")
            if raw.get("expires_at") and datetime.fromisoformat(raw["expires_at"]) <= now:
                raise GatewayAuthError("gateway API key has expired")
            if raw["project"] not in projects:
                raise GatewayAuthError("gateway API key's project is no longer configured")
            last = raw.get("last_used_at")
            if last is None or now - datetime.fromisoformat(last) >= _LAST_USED_GRANULARITY:
                raw["last_used_at"] = _iso(now)
                self._save(data)
            return self._record(key_id, raw)
