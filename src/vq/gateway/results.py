"""Gateway results (#163 slice 4): collect a finished job's outputs once.

When a placed job's target reports a terminal state, the gateway streams the
target's ``tar-workspace`` into its own result store, keeps only regular
files under safe relative paths, and writes a manifest of each file's size
and SHA-256. Downloads are served from that store by exact manifest path.
Collected bytes count against the principal's storage quota and expire
after the project's retention.
"""
from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import shutil
import tarfile
from collections.abc import Iterator
from pathlib import Path, PurePosixPath
from typing import Any

from vq import broker_relay, broker_store, transport
from vq.gateway.keys import _iso, _now, _private_dir
from vq.spec import TERMINAL_STATES

MANIFEST_SCHEMA = "vq.gateway-results/1"
_TERMINAL = frozenset(state.value for state in TERMINAL_STATES)


class ResultError(ValueError):
    def __init__(self, message: str, status: int = 400) -> None:
        super().__init__(message)
        self.status = status


class ResultStore:
    def __init__(self, root: Path) -> None:
        self.root = root / "results"

    def _dir(self, broker_job_id: str) -> Path:
        if not broker_store.BROKER_JOB_ID_PATTERN.fullmatch(broker_job_id):
            raise ResultError("no such job", 404)
        return self.root / broker_job_id

    @contextlib.contextmanager
    def _locked(self, broker_job_id: str) -> Iterator[Path]:
        _private_dir(self.root.parent)
        _private_dir(self.root)
        directory = self._dir(broker_job_id)
        directory.mkdir(mode=0o700, exist_ok=True)
        fd = os.open(directory / ".lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield directory
        finally:
            os.close(fd)

    def manifest(self, broker_job_id: str) -> dict[str, Any] | None:
        try:
            return json.loads((self._dir(broker_job_id) / "manifest.json").read_text())
        except FileNotFoundError:
            return None

    def usage_bytes(self, job_ids: list[str]) -> int:
        total = 0
        for job_id in job_ids:
            manifest = self.manifest(job_id)
            if manifest is not None:
                total += sum(item["size"] for item in manifest["files"])
        return total

    def file(self, broker_job_id: str, path: str) -> tuple[Path, dict[str, Any]]:
        manifest = self.manifest(broker_job_id)
        if manifest is None:
            raise ResultError("results are not collected yet", 409)
        entry = next((item for item in manifest["files"] if item["path"] == path), None)
        if entry is None:
            raise ResultError("no such result file", 404)
        return self._dir(broker_job_id) / "files" / path, entry

    def collect(self, cfg: Any, record: broker_store.BrokerRecord, *,
                byte_budget: int) -> dict[str, Any]:
        """Collect once: target terminal, stream, extract plain files, manifest."""
        existing = self.manifest(record.broker_job_id)
        if existing is not None:
            return existing
        target = broker_relay.target_of(cfg, record)
        if target is None:
            raise ResultError(f"the job is {record.state}; it has no results yet", 409)
        host_cfg, target_job_id = target
        status = transport.run_remote_vq(
            host_cfg, "status", "localhost", target_job_id, "--json", timeout=60,
            owned_process_group=True, max_stdout_bytes=16 * 1024 * 1024,
            max_stderr_bytes=64 * 1024,
        )
        state = json.loads(status.stdout or "{}").get("state")
        if state not in _TERMINAL:
            raise ResultError(f"the job has not finished ({state})", 409)
        with self._locked(record.broker_job_id) as directory:
            existing = self.manifest(record.broker_job_id)
            if existing is not None:
                return existing
            part = directory / "workspace.tar.part"
            files = directory / "files"
            shutil.rmtree(files, ignore_errors=True)
            try:
                with part.open("wb") as sink, transport.stream_remote_vq(
                    host_cfg, "tar-workspace", target_job_id
                ) as stream:
                    received = 0
                    while chunk := stream.stdout.read(1 << 20):
                        received += len(chunk)
                        if received > byte_budget:
                            raise ResultError(
                                "the job's outputs exceed the remaining storage quota", 413
                            )
                        sink.write(chunk)
                entries, skipped = _extract(part, files, byte_budget=byte_budget)
            finally:
                part.unlink(missing_ok=True)
            manifest = {
                "schema": MANIFEST_SCHEMA, "job_id": record.broker_job_id,
                "collected_at": _iso(_now()), "target_state": state,
                "files": entries, "skipped": skipped,
            }
            temporary = directory / "manifest.json.tmp"
            temporary.write_text(json.dumps(manifest, sort_keys=True))
            os.chmod(temporary, 0o600)
            os.replace(temporary, directory / "manifest.json")
            return manifest

    def prune(self, job_ids_by_retention: dict[str, int], *, dry_run: bool) -> list[str]:
        """Remove collected results older than their project's retention days."""
        removed = []
        now = _now()
        for job_id, days in sorted(job_ids_by_retention.items()):
            manifest = self.manifest(job_id)
            if manifest is None:
                continue
            from datetime import datetime, timedelta

            if now - datetime.fromisoformat(manifest["collected_at"]) >= timedelta(days=days):
                removed.append(job_id)
                if not dry_run:
                    shutil.rmtree(self._dir(job_id), ignore_errors=True)
        return removed


def _extract(archive_path: Path, destination: Path, *, byte_budget: int
             ) -> tuple[list[dict[str, Any]], list[str]]:
    """Plain files under safe relative paths; everything else is skipped by name."""
    entries: list[dict[str, Any]] = []
    skipped: list[str] = []
    expanded = 0
    destination.mkdir(mode=0o700)
    try:
        with tarfile.open(archive_path, "r:") as archive:
            for member in archive:
                name = PurePosixPath(member.name)
                parts = [part for part in name.parts if part not in ("", ".")]
                if member.isdir():
                    continue
                if (not member.isfile() or name.is_absolute() or ".." in name.parts
                        or not parts):
                    skipped.append(member.name)
                    continue
                expanded += member.size
                if expanded > byte_budget:
                    raise ResultError(
                        "the job's outputs exceed the remaining storage quota", 413
                    )
                relative = "/".join(parts)
                target = destination.joinpath(*parts)
                target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                source = archive.extractfile(member)
                if source is None:  # pragma: no cover - isfile() guarantees a stream
                    skipped.append(member.name)
                    continue
                digest = hashlib.sha256()
                fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                             0o600)
                with os.fdopen(fd, "wb") as sink:
                    while chunk := source.read(1 << 20):
                        digest.update(chunk)
                        sink.write(chunk)
                entries.append({"path": relative, "size": member.size,
                                "sha256": digest.hexdigest()})
    except tarfile.TarError as exc:
        raise ResultError(f"the job's workspace stream is not a valid tar: {exc}", 502) from None
    except FileExistsError:
        raise ResultError("the job's workspace stream repeats a path", 502) from None
    return entries, skipped
