"""Gateway uploads (#163 slice 2): resumable, digest-checked, validated tars.

A client declares the size and SHA-256 of one tar archive, sends it in
ordered chunks (resuming at the byte count the gateway reports), then
finalizes it. Finalizing verifies the digest and the archive: regular files
and directories only, no absolute or ``..`` paths, bounded member count and
expanded size. A final upload is immutable and owned by the principal and
project that created it. Uploads count against that principal's storage
quota.
"""
from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import re
import secrets
import shutil
import tarfile
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from vq.gateway.keys import GatewayKey, _iso, _now, _private_dir

UPLOAD_ID_RE = re.compile(r"u-[0-9a-f]{16}")
SHA256_RE = re.compile(r"[0-9a-f]{64}")
MAX_MEMBERS = 10_000
CHUNK_LIMIT = 64 * 1024 * 1024
STATE_OPEN = "open"
STATE_FINAL = "final"


class UploadError(ValueError):
    """A request the upload store refuses; the HTTP status rides along."""

    def __init__(self, message: str, status: int = 400) -> None:
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class Upload:
    upload_id: str
    principal: str
    project: str
    size: int
    sha256: str
    received: int
    state: str
    created_at: str

    def as_dict(self) -> dict[str, Any]:
        return {"upload_id": self.upload_id, "size": self.size, "sha256": self.sha256,
                "received": self.received, "state": self.state,
                "created_at": self.created_at}


class UploadStore:
    def __init__(self, root: Path) -> None:
        self.root = root / "uploads"

    @contextlib.contextmanager
    def _locked(self) -> Iterator[None]:
        _private_dir(self.root.parent)
        root = _private_dir(self.root)
        fd = os.open(root / ".lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            os.close(fd)

    def _dir(self, upload_id: str) -> Path:
        if not UPLOAD_ID_RE.fullmatch(upload_id):
            raise UploadError("no such upload", 404)
        return self.root / upload_id

    def _meta(self, upload_id: str) -> dict[str, Any]:
        path = self._dir(upload_id) / "meta.json"
        try:
            return json.loads(path.read_text())
        except FileNotFoundError:
            raise UploadError("no such upload", 404) from None

    @staticmethod
    def _write_meta(directory: Path, meta: dict[str, Any]) -> None:
        temporary = directory / "meta.json.tmp"
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "w") as stream:
            stream.write(json.dumps(meta, sort_keys=True))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, directory / "meta.json")

    @staticmethod
    def _upload(meta: dict[str, Any]) -> Upload:
        return Upload(**{key: meta[key] for key in Upload.__dataclass_fields__})

    def owned(self, upload_id: str, key: GatewayKey) -> tuple[Upload, Path]:
        """The caller's own upload; another principal's id reads as missing."""
        meta = self._meta(upload_id)
        if (meta["principal"], meta["project"]) != (key.principal, key.project):
            raise UploadError("no such upload", 404)
        return self._upload(meta), self._dir(upload_id)

    def usage_bytes(self, principal: str, project: str) -> int:
        total = 0
        if not self.root.exists():
            return 0
        for child in self.root.iterdir():
            if not UPLOAD_ID_RE.fullmatch(child.name):
                continue
            try:
                meta = json.loads((child / "meta.json").read_text())
            except (OSError, ValueError):
                continue
            if (meta.get("principal"), meta.get("project")) == (principal, project):
                total += int(meta.get("size", 0))
        return total

    def create(self, key: GatewayKey, *, size: int, sha256: str, quota_bytes: int) -> Upload:
        if type(size) is not int or size < 1:
            raise UploadError("size must be a positive byte count")
        if not isinstance(sha256, str) or not SHA256_RE.fullmatch(sha256):
            raise UploadError("sha256 must be 64 lowercase hex digits")
        with self._locked():
            if self.usage_bytes(key.principal, key.project) + size > quota_bytes:
                raise UploadError("this upload would exceed the project storage quota", 413)
            upload_id = f"u-{secrets.token_hex(8)}"
            directory = self.root / upload_id
            directory.mkdir(mode=0o700)
            os.close(os.open(directory / "data.part",
                             os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600))
            meta = {"upload_id": upload_id, "principal": key.principal,
                    "project": key.project, "key_id": key.key_id, "size": size,
                    "sha256": sha256, "received": 0, "state": STATE_OPEN,
                    "created_at": _iso(_now())}
            self._write_meta(directory, meta)
            return self._upload(meta)

    def append(self, upload_id: str, key: GatewayKey, *, start: int, data: bytes) -> Upload:
        """Write the next chunk; it must start exactly at the received count."""
        with self._locked():
            upload, directory = self.owned(upload_id, key)
            if upload.state != STATE_OPEN:
                raise UploadError("this upload is already finalized", 409)
            if start != upload.received:
                raise UploadError(f"resume at byte {upload.received}", 409)
            if len(data) > CHUNK_LIMIT or upload.received + len(data) > upload.size:
                raise UploadError("chunk exceeds the declared upload size", 413)
            fd = os.open(directory / "data.part", os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW)
            with os.fdopen(fd, "ab") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            meta = self._meta(upload_id)
            meta["received"] = upload.received + len(data)
            self._write_meta(directory, meta)
            return self._upload(meta)

    def finalize(self, upload_id: str, key: GatewayKey, *, expanded_limit: int) -> Upload:
        with self._locked():
            upload, directory = self.owned(upload_id, key)
            if upload.state == STATE_FINAL:
                return upload
            if upload.received != upload.size:
                raise UploadError(f"incomplete upload: {upload.received} of {upload.size} bytes",
                                  409)
            part = directory / "data.part"
            digest = hashlib.sha256()
            with part.open("rb") as stream:
                for chunk in iter(lambda: stream.read(1 << 20), b""):
                    digest.update(chunk)
            if digest.hexdigest() != upload.sha256:
                raise UploadError("upload bytes do not match the declared sha256", 422)
            validate_archive(part, expanded_limit=expanded_limit)
            os.replace(part, directory / "payload.tar")
            meta = self._meta(upload_id)
            meta["state"] = STATE_FINAL
            self._write_meta(directory, meta)
            return self._upload(meta)

    def payload(self, upload_id: str, key: GatewayKey) -> Path:
        upload, directory = self.owned(upload_id, key)
        if upload.state != STATE_FINAL:
            raise UploadError("finalize the upload before submitting it", 409)
        return directory / "payload.tar"

    def delete(self, upload_id: str, key: GatewayKey) -> None:
        with self._locked():
            _upload, directory = self.owned(upload_id, key)
            shutil.rmtree(directory)


def validate_archive(path: Path, *, expanded_limit: int) -> None:
    """Refuse anything but a bounded tree of plain files and directories."""
    try:
        with tarfile.open(path, "r:") as archive:
            members = 0
            expanded = 0
            for member in archive:
                members += 1
                if members > MAX_MEMBERS:
                    raise UploadError(f"archive has more than {MAX_MEMBERS} members", 422)
                name = PurePosixPath(member.name)
                if name.is_absolute() or ".." in name.parts or not member.name.strip("./"):
                    if member.isdir() and member.name in (".", "./"):
                        continue
                    raise UploadError(f"unsafe archive path {member.name!r}", 422)
                if not (member.isfile() or member.isdir()):
                    raise UploadError(
                        f"archive member {member.name!r} is not a plain file or directory", 422
                    )
                expanded += member.size
                if expanded > expanded_limit:
                    raise UploadError("archive expands beyond the project storage quota", 422)
    except tarfile.TarError as exc:
        raise UploadError(f"not a valid uncompressed tar archive: {exc}", 422) from None


def prune_uploads(store: UploadStore, retention_days: dict[str, int], *, dry_run: bool
                  ) -> list[str]:
    """Remove uploads older than their project's retention days."""
    from datetime import datetime, timedelta

    removed = []
    if not store.root.exists():
        return removed
    now = _now()
    for child in sorted(store.root.iterdir()):
        if not UPLOAD_ID_RE.fullmatch(child.name):
            continue
        try:
            meta = json.loads((child / "meta.json").read_text())
        except (OSError, ValueError):
            continue
        days = retention_days.get(meta.get("project"))
        if days is None:
            continue
        if now - datetime.fromisoformat(meta["created_at"]) >= timedelta(days=days):
            removed.append(child.name)
            if not dry_run:
                shutil.rmtree(child, ignore_errors=True)
    return removed
