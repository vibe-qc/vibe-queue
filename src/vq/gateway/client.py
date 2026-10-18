"""A minimal gateway client (#163 slice 5): stdlib HTTPS, resumable, retrying.

The API key lives in an owner-only credentials file, never on the command
line. Every request is safe to repeat:
- an upload chunk resumes at the byte count the gateway reports;
- a submission carries its idempotency key;
- finalize and cancel are idempotent.
So transport failures and dropped responses are retried without creating a
second job.
"""
from __future__ import annotations

import hashlib
import ipaddress
import json
import os
import ssl
import stat
import tarfile
import tempfile
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

ENV_CREDENTIALS = "VQ_GATEWAY_CREDENTIALS"
CHUNK_BYTES = 8 * 1024 * 1024
RETRIES = 5
RETRY_STATUSES = frozenset({502, 503, 504})

Send = Callable[[str, str, dict[str, str], bytes | None], tuple[int, dict[str, str], bytes]]


class ClientError(RuntimeError):
    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class Credentials:
    url: str
    key: str
    ca_file: str | None = None


def default_credentials_path() -> Path:
    if env := os.environ.get(ENV_CREDENTIALS):
        return Path(env).expanduser()
    base = os.environ.get("XDG_CONFIG_HOME", "~/.config")
    return Path(base).expanduser() / "vq" / "gateway-credentials.toml"


def load_credentials(path: Path | None = None) -> Credentials:
    """Read ``url``, ``key`` and optional ``ca_file`` from a private TOML file."""
    path = path or default_credentials_path()
    try:
        info = path.lstat()
    except FileNotFoundError:
        raise ClientError(f"no gateway credentials at {path}") from None
    if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
        raise ClientError(f"gateway credentials {path} must be a regular file readable "
                          "only by you (chmod 600)")
    data = tomllib.loads(path.read_text())
    url, key = data.get("url"), data.get("key")
    if not isinstance(url, str) or not isinstance(key, str):
        raise ClientError(f"gateway credentials {path} need url and key")
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in ("https", "http") or not parsed.hostname:
        raise ClientError("gateway url must be https://HOST[:PORT]")
    if parsed.scheme == "http" and not _loopback(parsed.hostname):
        raise ClientError("refusing to send a gateway key over plain http to a remote host")
    ca_file = data.get("ca_file")
    return Credentials(url=url.rstrip("/"), key=key,
                       ca_file=ca_file if isinstance(ca_file, str) else None)


def _loopback(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def urllib_send(credentials: Credentials) -> Send:
    context = ssl.create_default_context(cafile=credentials.ca_file)

    def send(method: str, url: str, headers: dict[str, str],
             body: bytes | None) -> tuple[int, dict[str, str], bytes]:
        request = urllib.request.Request(url, data=body, method=method, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=300, context=context) as response:
                return response.status, dict(response.headers), response.read()
        except urllib.error.HTTPError as exc:
            return exc.code, dict(exc.headers or {}), exc.read()

    return send


class GatewayClient:
    def __init__(self, credentials: Credentials, send: Send | None = None,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        self.credentials = credentials
        self._send = send or urllib_send(credentials)
        self._sleep = sleep

    def _request(self, method: str, path: str, *, payload: Any = None,
                 body: bytes | None = None, headers: dict[str, str] | None = None,
                 retry: bool = True) -> tuple[int, dict[str, str], bytes]:
        all_headers = {"Authorization": f"Bearer {self.credentials.key}", **(headers or {})}
        if payload is not None:
            body = json.dumps(payload).encode()
            all_headers["Content-Type"] = "application/json"
        attempts = RETRIES if retry else 1
        for attempt in range(attempts):
            try:
                status, response_headers, data = self._send(
                    method, self.credentials.url + path, all_headers, body)
            except (OSError, urllib.error.URLError):
                if attempt + 1 == attempts:
                    raise ClientError(f"cannot reach the gateway for {method} {path}") from None
                self._sleep(min(30.0, 2.0 ** attempt))
                continue
            if status in RETRY_STATUSES and attempt + 1 < attempts:
                self._sleep(min(30.0, 2.0 ** attempt))
                continue
            return status, response_headers, data
        raise AssertionError("unreachable")  # pragma: no cover

    def _json(self, method: str, path: str, *, ok: tuple[int, ...] = (200,), **kw: Any
              ) -> dict[str, Any]:
        status, _headers, data = self._request(method, path, **kw)
        try:
            decoded = json.loads(data or b"{}")
        except ValueError:
            decoded = {}
        if status not in ok:
            raise ClientError(decoded.get("error") or f"gateway answered {status}", status)
        return decoded

    def upload(self, archive: Path) -> str:
        """Upload one tar archive; resumes after any interruption. Returns its id."""
        size = archive.stat().st_size
        digest = hashlib.sha256()
        with archive.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1 << 20), b""):
                digest.update(chunk)
        created = self._json("POST", "/v1/uploads", ok=(201,), retry=False,
                             payload={"size": size, "sha256": digest.hexdigest()})
        upload_id = created["upload_id"]
        received = 0
        with archive.open("rb") as stream:
            while received < size:
                stream.seek(received)
                chunk = stream.read(CHUNK_BYTES)
                end = received + len(chunk) - 1
                try:
                    received = self._json(
                        "PUT", f"/v1/uploads/{upload_id}", body=chunk,
                        headers={"Content-Range": f"bytes {received}-{end}/{size}"},
                    )["received"]
                except ClientError as exc:
                    if exc.status != 409:
                        raise
                    # A dropped response after the chunk landed: resume where
                    # the gateway says it is.
                    received = self._json("GET", f"/v1/uploads/{upload_id}")["received"]
        self._json("POST", f"/v1/uploads/{upload_id}/finalize")
        return upload_id

    def submit(self, request: dict[str, Any]) -> dict[str, Any]:
        """Submit; a repeated identical request returns the original job."""
        return self._json("POST", "/v1/jobs", ok=(200, 202), payload=request)

    def job(self, job_id: str) -> dict[str, Any]:
        return self._json("GET", f"/v1/jobs/{urllib.parse.quote(job_id)}")

    def jobs(self) -> list[dict[str, Any]]:
        found: list[dict[str, Any]] = []
        after: str | None = None
        while True:
            query = "?limit=100" + (f"&after={urllib.parse.quote(after)}" if after else "")
            page = self._json("GET", f"/v1/jobs{query}")
            found.extend(page["jobs"])
            after = page.get("next")
            if not after:
                return found

    def logs(self, job_id: str, *, stream: str, offset: int) -> dict[str, Any]:
        query = urllib.parse.urlencode({"stream": stream, "offset": offset})
        return self._json("GET", f"/v1/jobs/{urllib.parse.quote(job_id)}/logs?{query}")

    def cancel(self, job_id: str) -> dict[str, Any]:
        return self._json("POST", f"/v1/jobs/{urllib.parse.quote(job_id)}/cancel")

    def results(self, job_id: str) -> dict[str, Any]:
        return self._json("GET", f"/v1/jobs/{urllib.parse.quote(job_id)}/results")

    def fetch(self, job_id: str, destination: Path, *, force: bool = False) -> list[str]:
        """Download every manifest file into ``destination``, verifying digests."""
        manifest = self.results(job_id)
        if not manifest.get("ready"):
            raise ClientError(f"results are not ready ({manifest.get('state')})")
        destination.mkdir(parents=True, exist_ok=True)
        root = destination.resolve()
        written = []
        for entry in manifest["files"]:
            parts = PurePosixPath(entry["path"]).parts
            target = root.joinpath(*parts).resolve()
            if root not in target.parents or ".." in parts:
                raise ClientError(f"refusing manifest path {entry['path']!r}")
            if target.exists() and not force:
                raise ClientError(f"{target} exists; pass --force to overwrite")
            path = "/".join(urllib.parse.quote(p) for p in parts)
            status, _headers, data = self._request(
                "GET", f"/v1/jobs/{urllib.parse.quote(job_id)}/results/{path}")
            if status != 200:
                raise ClientError(f"download of {entry['path']} failed ({status})", status)
            if hashlib.sha256(data).hexdigest() != entry["sha256"]:
                raise ClientError(f"{entry['path']} does not match its manifest digest")
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
            written.append(entry["path"])
        return written


def pack(path: Path) -> tuple[Path, bool]:
    """A tar archive for ``path``: the file itself if it is a .tar, else a temp."""
    if path.is_file() and path.suffix == ".tar":
        return path, False
    if not path.is_dir():
        raise ClientError(f"{path} is neither a directory nor a .tar archive")
    fd, name = tempfile.mkstemp(prefix="vq-remote-", suffix=".tar")
    os.close(fd)
    archive = Path(name)
    def plain_only(info: tarfile.TarInfo) -> tarfile.TarInfo:
        if not (info.isfile() or info.isdir()):
            raise ClientError(f"{info.name} is not a plain file or directory; the "
                              "gateway accepts neither links nor special files")
        return info

    try:
        with tarfile.open(archive, "w") as out:
            for child in sorted(path.iterdir()):
                out.add(child, arcname=child.name, filter=plain_only)
    except BaseException:
        archive.unlink(missing_ok=True)
        raise
    return archive, True
