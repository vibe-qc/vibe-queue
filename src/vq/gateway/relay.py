"""Relay mode (#164): the gateway holds admitted jobs for an outbound connector.

In relay mode the gateway runs where clients can reach it and holds no broker,
SSH keys or fleet inventory. A connector on the internal broker host enrolls
once, then makes only outbound requests: it claims work items, drives its
local broker with the same scoped idempotency key a local-mode gateway uses,
and acknowledges each outcome.

Every claim raises the item's ``epoch``; an acknowledgement carrying an older
epoch is refused. A lease coordinates delivery only: an expired lease lets the
connector claim the item again, and its replay of the keyed broker accept
returns the job it already admitted. Nothing is ever launched elsewhere.

One connector authority serves a project. Replacing it is an operator action:
revoke, then enroll a new one.
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
from collections.abc import Iterator, Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from vq.gateway.keys import _iso, _now, _private_dir, _read_private, _write_private

CONNECTOR_KEY_RE = re.compile(r"vqc_([0-9a-f]{16})_([A-Za-z0-9_-]{43})")
ENROLL_TOKEN_RE = re.compile(r"vqe_([0-9a-f]{16})_([A-Za-z0-9_-]{43})")
GW_JOB_RE = re.compile(r"g-[0-9a-f]{12}")
ENROLL_TTL = timedelta(minutes=10)
ROTATION_OVERLAP = timedelta(minutes=10)
LEASE = timedelta(seconds=120)
FRESH = timedelta(seconds=90)
TRANSFER_QUEUED = "queued"
TRANSFER_CLAIMED = "claimed"
TRANSFER_ACCEPTED = "accepted"
TRANSFER_REFUSED = "refused"
TRANSFER_UNCERTAIN = "uncertain"
TRANSFER_CANCELLED = "cancelled"
"""Cancelled at the gateway before any connector claimed it: never delivered."""
_CLAIMABLE = frozenset({TRANSFER_QUEUED, TRANSFER_CLAIMED, TRANSFER_UNCERTAIN})
LOG_STREAMS = ("stdout", "stderr")
LOGS_WANTED_FOR = timedelta(minutes=10)
LOG_BUFFER_CAP = 16 * 1024 * 1024
"""Per job and stream: the gateway buffers at most this much pushed output."""
COLLECT_REQUESTED = "requested"
COLLECT_DONE = "done"
COLLECT_NOT_FINISHED = "not-finished"
COLLECT_FAILED = "failed"
CANCEL_REQUESTED = "requested"
CANCEL_STOPPED = "stopped"
CANCEL_NOT_STOPPED = "not-stopped"


class RelayError(ValueError):
    def __init__(self, message: str, status: int = 400) -> None:
        super().__init__(message)
        self.status = status


def gateway_job_id(scoped_key: str) -> str:
    """The relay job id for a scoped idempotency key: replays map to one job."""
    return "g-" + hashlib.sha256(f"relay\0{scoped_key}".encode()).hexdigest()[:12]


def _digest(pepper: bytes, secret: str) -> str:
    return hmac.new(pepper, secret.encode(), hashlib.sha256).hexdigest()


class ConnectorStore:
    """Connector credentials and one-use enrollment tokens, HMAC-stored."""

    def __init__(self, root: Path) -> None:
        self.root = root / "connectors"

    @contextlib.contextmanager
    def _locked(self) -> Iterator[dict[str, Any]]:
        _private_dir(self.root.parent)
        root = _private_dir(self.root)
        fd = os.open(root / ".lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            raw = _read_private(root / "connectors.json", 1 << 20)
            data = json.loads(raw) if raw else {"connectors": {}, "enrollments": {}}
            before = json.dumps(data, sort_keys=True)
            yield data
            if json.dumps(data, sort_keys=True) != before:
                _write_private(root / "connectors.json",
                               json.dumps(data, sort_keys=True, indent=2).encode())
        finally:
            os.close(fd)

    def _pepper(self) -> bytes:
        path = self.root / "pepper"
        data = _read_private(path, 64)
        if data is None:
            data = secrets.token_bytes(32)
            _write_private(path, data)
        return data

    @staticmethod
    def _active_for(data: dict[str, Any], project: str) -> str | None:
        for connector_id, record in data["connectors"].items():
            if record["project"] == project and record.get("revoked_at") is None:
                return connector_id
        return None

    def create_enrollment(self, project: str, *, projects: Sequence[str]) -> str:
        if project not in projects:
            raise RelayError(f"project {project!r} is not configured in [gateway.projects]")
        with self._locked() as data:
            pepper = self._pepper()
            if self._active_for(data, project):
                raise RelayError(f"project {project!r} already has a connector; revoke it "
                                 "before enrolling a replacement", 409)
            token_id, secret = secrets.token_hex(8), secrets.token_urlsafe(32)
            data["enrollments"][token_id] = {
                "project": project, "digest": _digest(pepper, secret),
                "expires_at": _iso(_now() + ENROLL_TTL), "used_at": None,
            }
            return f"vqe_{token_id}_{secret}"

    def enroll(self, token: str) -> tuple[str, str, str]:
        """Exchange a one-use token for (connector id, project, connector key)."""
        match = ENROLL_TOKEN_RE.fullmatch(token or "")
        if match is None:
            raise RelayError("invalid enrollment token", 401)
        token_id, secret = match.groups()
        with self._locked() as data:
            pepper = self._pepper()
            record = data["enrollments"].get(token_id)
            if record is None or not hmac.compare_digest(
                record["digest"], _digest(pepper, secret)
            ):
                raise RelayError("invalid enrollment token", 401)
            if record["used_at"] is not None:
                raise RelayError("this enrollment token was already used", 401)
            if datetime.fromisoformat(record["expires_at"]) <= _now():
                raise RelayError("this enrollment token has expired", 401)
            project = record["project"]
            if self._active_for(data, project):
                raise RelayError(f"project {project!r} already has a connector", 409)
            record["used_at"] = _iso(_now())
            connector_id, key_secret = secrets.token_hex(8), secrets.token_urlsafe(32)
            data["connectors"][connector_id] = {
                "project": project, "digest": _digest(pepper, key_secret),
                "previous_digest": None, "previous_expires_at": None,
                "created_at": _iso(_now()), "revoked_at": None, "last_seen_at": None,
            }
            return connector_id, project, f"vqc_{connector_id}_{key_secret}"

    def authenticate(self, key: str) -> tuple[str, str]:
        """(connector id, project) for a live connector key."""
        match = CONNECTOR_KEY_RE.fullmatch(key or "")
        if match is None:
            raise RelayError("invalid connector key", 401)
        connector_id, secret = match.groups()
        with self._locked() as data:
            pepper = self._pepper()
            record = data["connectors"].get(connector_id)
            presented = _digest(pepper, secret)
            if record is None:
                raise RelayError("invalid connector key", 401)
            current = hmac.compare_digest(record["digest"], presented)
            previous = (
                record.get("previous_digest") is not None
                and hmac.compare_digest(record["previous_digest"], presented)
                and datetime.fromisoformat(record["previous_expires_at"]) > _now()
            )
            if not (current or previous):
                raise RelayError("invalid connector key", 401)
            if record.get("revoked_at") is not None:
                raise RelayError("this connector is revoked", 401)
            record["last_seen_at"] = _iso(_now())
            return connector_id, record["project"]

    def rotate(self, connector_id: str) -> str:
        """A new key; the old one keeps working for a short overlap."""
        with self._locked() as data:
            pepper = self._pepper()
            record = data["connectors"].get(connector_id)
            if record is None or record.get("revoked_at") is not None:
                raise RelayError("no such active connector", 404)
            secret = secrets.token_urlsafe(32)
            record["previous_digest"] = record["digest"]
            record["previous_expires_at"] = _iso(_now() + ROTATION_OVERLAP)
            record["digest"] = _digest(pepper, secret)
            return f"vqc_{connector_id}_{secret}"

    def revoke(self, connector_id: str) -> dict[str, Any]:
        with self._locked() as data:
            record = data["connectors"].get(connector_id)
            if record is None:
                raise RelayError("no such connector", 404)
            if record.get("revoked_at") is None:
                record["revoked_at"] = _iso(_now())
            return {"connector_id": connector_id, **_public(record)}

    def list(self) -> list[dict[str, Any]]:
        with self._locked() as data:
            return [{"connector_id": cid, **_public(r)}
                    for cid, r in sorted(data["connectors"].items())]

    def last_seen(self, project: str) -> str | None:
        with self._locked() as data:
            connector_id = self._active_for(data, project)
            return data["connectors"][connector_id]["last_seen_at"] if connector_id else None


def _public(record: dict[str, Any]) -> dict[str, Any]:
    return {key: record.get(key) for key in ("project", "created_at", "revoked_at",
                                             "last_seen_at")}


class RelayStore:
    """Admitted relay jobs, their claim state and the connector's last report."""

    def __init__(self, root: Path) -> None:
        self.root = root / "relay"
        self._withheld: dict[str, int] = {}

    def withheld(self, project: str) -> int:
        """Items the connector's last claim left here for lack of spool."""
        return self._withheld.get(project, 0)

    @contextlib.contextmanager
    def _locked(self) -> Iterator[Path]:
        _private_dir(self.root.parent)
        root = _private_dir(self.root)
        fd = os.open(root / ".lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield root
        finally:
            os.close(fd)

    def _path(self, gw_job_id: str) -> Path:
        if not GW_JOB_RE.fullmatch(gw_job_id):
            raise RelayError("no such job", 404)
        return self.root / f"{gw_job_id}.json"

    def read(self, gw_job_id: str) -> dict[str, Any] | None:
        try:
            return json.loads(self._path(gw_job_id).read_text())
        except FileNotFoundError:
            return None

    def _write(self, record: dict[str, Any]) -> None:
        path = self._path(record["job_id"])
        temporary = path.with_suffix(".tmp")
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "w") as stream:
            stream.write(json.dumps(record, sort_keys=True))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)

    def _all(self) -> list[dict[str, Any]]:
        if not self.root.exists():
            return []
        records = []
        for child in sorted(self.root.glob("g-*.json")):
            with contextlib.suppress(OSError, ValueError):
                records.append(json.loads(child.read_text()))
        return records

    def admit(self, *, principal: str, project: str, key_id: str, scoped_key: str,
              request: dict[str, Any], upload: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        """Record one job durably; the same request replays, a changed one conflicts."""
        job_id = gateway_job_id(scoped_key)
        request_digest = hashlib.sha256(json.dumps(
            {"request": request, "sha256": upload["sha256"]}, sort_keys=True).encode()
        ).hexdigest()
        with self._locked():
            existing = self.read(job_id)
            if existing is not None:
                if existing["request_digest"] != request_digest or (
                    existing["principal"], existing["project"]
                ) != (principal, project):
                    raise RelayError(
                        "this idempotency_key already names a different submission", 409
                    )
                return existing, True
            now = _iso(_now())
            record = {
                "job_id": job_id, "principal": principal, "project": project,
                "key_id": key_id, "scoped_key": scoped_key, "request": request,
                "request_digest": request_digest, "upload_id": upload["upload_id"],
                "upload_sha256": upload["sha256"], "upload_size": upload["size"],
                "transfer": TRANSFER_QUEUED, "epoch": 0, "lease_until": None,
                "broker_job_id": None, "refused_reason": None, "view": None,
                "view_at": None, "created_at": now, "updated_at": now,
                "cancel": None,
            }
            self._write(record)
            return record, False

    def owned(self, gw_job_id: str, principal: str, project: str) -> dict[str, Any]:
        record = self.read(gw_job_id)
        if record is None or (record["principal"], record["project"]) != (principal, project):
            raise RelayError("no such job", 404)
        return record

    def owned_records(self, principal: str, project: str) -> list[dict[str, Any]]:
        return [r for r in self._all() if (r["principal"], r["project"]) == (principal, project)]

    def claim(self, project: str, *, max_items: int, spool_free: int | None = None
              ) -> list[dict[str, Any]]:
        """Lease up to ``max_items`` items of ``project``; each claim bumps the epoch.

        With ``spool_free`` (the connector's free spool bytes), a submit or
        collect item that would not fit stays here: :meth:`withheld` counts it
        so clients see the backpressure.
        """
        items = []
        self._withheld[project] = 0
        with self._locked():
            now = _now()
            for record in self._all():
                if len(items) >= max_items:
                    break
                if record["project"] != project or record["transfer"] not in _CLAIMABLE:
                    continue
                lease = record["lease_until"]
                if record["transfer"] != TRANSFER_QUEUED and lease is not None and (
                    datetime.fromisoformat(lease) > now
                ):
                    continue
                if record.get("cancel") and record["transfer"] == TRANSFER_QUEUED:
                    continue  # cancelled before delivery; request_cancel settled it
                if spool_free is not None and record["upload_size"] > spool_free:
                    self._withheld[project] += 1
                    continue
                if spool_free is not None:
                    spool_free -= record["upload_size"]
                record["epoch"] += 1
                record["transfer"] = TRANSFER_CLAIMED
                record["lease_until"] = _iso(now + LEASE)
                record["updated_at"] = _iso(now)
                self._write(record)
                items.append({
                    "item_id": f"submit:{record['job_id']}", "kind": "submit",
                    "epoch": record["epoch"], "job_id": record["job_id"],
                    "scoped_key": record["scoped_key"], "request": record["request"],
                    "upload_id": record["upload_id"], "sha256": record["upload_sha256"],
                    "size": record["upload_size"],
                })
            for record in self._all():
                if len(items) >= max_items:
                    break
                cancel = record.get("cancel")
                if (record["project"] != project or not cancel
                        or cancel["state"] != CANCEL_REQUESTED
                        or record["transfer"] != TRANSFER_ACCEPTED):
                    continue
                lease = cancel.get("lease_until")
                if lease is not None and datetime.fromisoformat(lease) > now:
                    continue
                cancel["epoch"] = cancel.get("epoch", 0) + 1
                cancel["lease_until"] = _iso(now + LEASE)
                record["updated_at"] = _iso(now)
                self._write(record)
                items.append({
                    "item_id": f"cancel:{record['job_id']}", "kind": "cancel",
                    "epoch": cancel["epoch"], "job_id": record["job_id"],
                    "broker_job_id": record["broker_job_id"],
                    "reason": cancel["reason"],
                })
            for record in self._all():
                if len(items) >= max_items:
                    break
                collect = record.get("collect")
                if (record["project"] != project or not collect
                        or collect["state"] != COLLECT_REQUESTED
                        or record["transfer"] != TRANSFER_ACCEPTED):
                    continue
                lease = collect.get("lease_until")
                if lease is not None and datetime.fromisoformat(lease) > now:
                    continue
                if spool_free is not None and collect["byte_budget"] > spool_free:
                    self._withheld[project] += 1
                    continue
                if spool_free is not None:
                    spool_free -= collect["byte_budget"]
                collect["epoch"] = collect.get("epoch", 0) + 1
                collect["lease_until"] = _iso(now + LEASE)
                self._write(record)
                items.append({
                    "item_id": f"collect:{record['job_id']}", "kind": "collect",
                    "epoch": collect["epoch"], "job_id": record["job_id"],
                    "broker_job_id": record["broker_job_id"],
                    "byte_budget": collect["byte_budget"],
                })
        return items

    # -- results: collected by the connector, pushed file by file -------------
    def request_results(self, gw_job_id: str, principal: str, project: str, *,
                        byte_budget: int) -> dict[str, Any]:
        """Ask the connector to collect; a settled not-finished or failed asks again."""
        with self._locked():
            record = self.owned(gw_job_id, principal, project)
            collect = record.get("collect")
            if record["transfer"] == TRANSFER_ACCEPTED and (
                collect is None or collect["state"] in (COLLECT_NOT_FINISHED, COLLECT_FAILED)
            ):
                record["collect"] = {"state": COLLECT_REQUESTED, "epoch":
                                     (collect or {}).get("epoch", 0), "lease_until": None,
                                     "requested_at": _iso(_now()), "byte_budget": byte_budget,
                                     "detail": None}
                self._write(record)
            return record

    def _incoming(self, results_root: Path, gw_job_id: str) -> Path:
        directory = results_root / gw_job_id / "incoming"
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        return directory

    def push_result(self, project: str, gw_job_id: str, path: str, *, start: int,
                    data: bytes, results_root: Path) -> int:
        """Append one chunk of one result file at exactly its received size."""
        parts = _safe_parts(path)
        with self._locked():
            record = self.read(gw_job_id) if GW_JOB_RE.fullmatch(gw_job_id) else None
            collect = (record or {}).get("collect")
            if (record is None or record["project"] != project or not collect
                    or collect["state"] != COLLECT_REQUESTED):
                raise RelayError("no collection is pending for this job", 404)
            incoming = self._incoming(results_root, gw_job_id)
            total = sum(p.stat().st_size for p in incoming.rglob("*") if p.is_file())
            if total + len(data) > collect["byte_budget"]:
                raise RelayError("these results exceed the remaining storage quota", 413)
            target = incoming.joinpath(*parts)
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            size = target.stat().st_size if target.exists() else 0
            if start != size:
                raise RelayError(f"resume at byte {size}", 409)
            fd = os.open(target, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "ab") as handle:
                handle.write(data)
            return size + len(data)

    def _ack_collect(self, record: dict[str, Any], *, epoch: int, outcome: str,
                     manifest: Any, reason: str | None, results_root: Path) -> dict[str, Any]:
        import shutil

        collect = record.get("collect")
        if not collect:
            raise RelayError("unknown work item", 404)
        if epoch != collect.get("epoch"):
            raise RelayError("stale claim epoch", 409)
        if collect["state"] == COLLECT_DONE:
            return record  # a repeated acknowledgement
        if collect["state"] != COLLECT_REQUESTED:
            raise RelayError("this collection was already settled", 409)
        directory = results_root / record["job_id"]
        incoming = directory / "incoming"
        if outcome == "collected":
            if not isinstance(manifest, dict) or not isinstance(manifest.get("files"), list):
                raise RelayError("a collected outcome needs its manifest")
            expected = {}
            for entry in manifest["files"]:
                if (not isinstance(entry, dict) or not isinstance(entry.get("path"), str)
                        or type(entry.get("size")) is not int
                        or not isinstance(entry.get("sha256"), str)):
                    raise RelayError("malformed manifest entry")
                expected["/".join(_safe_parts(entry["path"]))] = entry
            present = {"/".join(p.relative_to(incoming).parts): p
                       for p in (incoming.rglob("*") if incoming.exists() else [])
                       if p.is_file()}
            if set(present) != set(expected):
                raise RelayError("pushed files do not match the manifest", 409)
            for name, entry in expected.items():
                data = present[name].read_bytes()
                digest = hashlib.sha256(data).hexdigest()
                if len(data) != entry["size"] or digest != entry["sha256"]:
                    raise RelayError(f"{name} does not match its manifest digest", 409)
            files = directory / "files"
            shutil.rmtree(files, ignore_errors=True)
            if incoming.exists():
                os.replace(incoming, files)
            else:
                files.mkdir(mode=0o700)
            final = {"schema": "vq.gateway-results/1", "job_id": record["job_id"],
                     "collected_at": _iso(_now()),
                     "target_state": manifest.get("target_state"),
                     "files": list(expected.values()),
                     "skipped": [str(s)[:500] for s in manifest.get("skipped") or []][:1000]}
            temporary = directory / "manifest.json.tmp"
            fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "w") as handle:
                handle.write(json.dumps(final, sort_keys=True))
            os.replace(temporary, directory / "manifest.json")
            collect.update(state=COLLECT_DONE, lease_until=None)
        elif outcome in (COLLECT_NOT_FINISHED, COLLECT_FAILED):
            shutil.rmtree(incoming, ignore_errors=True)
            collect.update(state=outcome, lease_until=None, detail=str(reason or "")[:300])
        else:
            raise RelayError("a collect outcome is collected, not-finished or failed")
        record["updated_at"] = _iso(_now())
        self._write(record)
        return record

    def request_cancel(self, gw_job_id: str, principal: str, project: str,
                       *, reason: str) -> dict[str, Any]:
        """Record a client's cancel durably; repeatable.

        A job no connector has claimed yet is cancelled here and never
        delivered. Otherwise the request waits for the connector, which
        acknowledges whether the job is stopped. Requested is not cancelled.
        """
        with self._locked():
            record = self.owned(gw_job_id, principal, project)
            if record["transfer"] in (TRANSFER_REFUSED, TRANSFER_CANCELLED):
                return record
            if record.get("cancel") is None:
                now = _iso(_now())
                record["cancel"] = {"state": CANCEL_REQUESTED, "requested_at": now,
                                    "reason": reason[:300], "epoch": 0, "lease_until": None,
                                    "settled_at": None}
                if record["transfer"] == TRANSFER_QUEUED:
                    record["transfer"] = TRANSFER_CANCELLED
                    record["cancel"].update(state=CANCEL_STOPPED, settled_at=now)
                record["updated_at"] = now
                self._write(record)
            return record

    def ack(self, project: str, item_id: str, *, epoch: int, outcome: str,
            broker_job_id: str | None = None, reason: str | None = None,
            manifest: Any = None, results_root: Path | None = None) -> dict[str, Any]:
        kind, _, job_id = item_id.partition(":")
        if kind not in ("submit", "cancel", "collect"):
            raise RelayError("unknown work item", 404)
        with self._locked():
            record = self.read(job_id) if GW_JOB_RE.fullmatch(job_id) else None
            if record is None or record["project"] != project:
                raise RelayError("unknown work item", 404)
            if kind == "cancel":
                return self._ack_cancel(record, epoch=epoch, outcome=outcome)
            if kind == "collect":
                if results_root is None:  # pragma: no cover - the app always passes it
                    raise RelayError("results are not configured", 500)
                return self._ack_collect(record, epoch=epoch, outcome=outcome,
                                         manifest=manifest, reason=reason,
                                         results_root=results_root)
            if epoch != record["epoch"]:
                raise RelayError("stale claim epoch", 409)
            if record["transfer"] == TRANSFER_ACCEPTED:
                if outcome == TRANSFER_ACCEPTED and broker_job_id == record["broker_job_id"]:
                    return record  # a repeated acknowledgement
                raise RelayError("this item was already accepted", 409)
            if outcome == TRANSFER_ACCEPTED:
                if not isinstance(broker_job_id, str) or not broker_job_id:
                    raise RelayError("an accepted item needs its broker job id")
                record.update(transfer=TRANSFER_ACCEPTED, broker_job_id=broker_job_id,
                              lease_until=None)
            elif outcome == TRANSFER_REFUSED:
                record.update(transfer=TRANSFER_REFUSED, refused_reason=str(reason or "")[:500],
                              lease_until=None)
                cancel = record.get("cancel")
                if cancel and cancel["state"] == CANCEL_REQUESTED:
                    # Refused jobs never ran: a pending cancel is settled.
                    cancel.update(state=CANCEL_STOPPED, settled_at=_iso(_now()))
            elif outcome == TRANSFER_UNCERTAIN:
                record.update(transfer=TRANSFER_UNCERTAIN)
            else:
                raise RelayError("outcome must be accepted, refused or uncertain")
            record["updated_at"] = _iso(_now())
            self._write(record)
            return record

    def _ack_cancel(self, record: dict[str, Any], *, epoch: int, outcome: str
                    ) -> dict[str, Any]:
        cancel = record.get("cancel")
        if not cancel:
            raise RelayError("unknown work item", 404)
        if epoch != cancel.get("epoch"):
            raise RelayError("stale claim epoch", 409)
        if outcome not in (CANCEL_STOPPED, CANCEL_NOT_STOPPED):
            raise RelayError("a cancel outcome is stopped or not-stopped")
        if cancel["state"] != CANCEL_REQUESTED:
            if cancel["state"] == outcome:
                return record  # a repeated acknowledgement
            raise RelayError("this cancellation was already settled", 409)
        now = _iso(_now())
        cancel.update(state=outcome, settled_at=now, lease_until=None)
        record["updated_at"] = now
        self._write(record)
        return record

    # -- logs: pulled on demand, pushed by the connector by offset ------------
    def _log_path(self, gw_job_id: str, stream: str) -> Path:
        if stream not in LOG_STREAMS:
            raise RelayError("stream must be stdout or stderr")
        directory = _private_dir(_private_dir(self.root) / "logs")
        self._path(gw_job_id)  # validates the id
        return directory / f"{gw_job_id}.{stream}"

    def want_logs(self, gw_job_id: str, principal: str, project: str) -> dict[str, Any]:
        """Ask the connector to push this job's output for a while."""
        with self._locked():
            record = self.owned(gw_job_id, principal, project)
            record["logs_wanted_until"] = _iso(_now() + LOGS_WANTED_FOR)
            self._write(record)
            return record

    def read_logs(self, gw_job_id: str, stream: str, *, offset: int, limit: int
                  ) -> tuple[bytes, int, str | None]:
        """(window, buffered size, time of the last push)."""
        path = self._log_path(gw_job_id, stream)
        try:
            size = path.stat().st_size
            with path.open("rb") as handle:
                handle.seek(offset)
                data = handle.read(limit)
            pushed = _iso(datetime.fromtimestamp(path.stat().st_mtime, _now().tzinfo))
        except FileNotFoundError:
            return b"", 0, None
        return data, size, pushed

    def logs_wanted(self, project: str) -> list[dict[str, Any]]:
        now = _now()
        wanted = []
        for record in self._all():
            until = record.get("logs_wanted_until")
            if (record["project"] != project or record["transfer"] != TRANSFER_ACCEPTED
                    or until is None or datetime.fromisoformat(until) <= now):
                continue
            offsets = {}
            for stream in LOG_STREAMS:
                path = self._log_path(record["job_id"], stream)
                offsets[stream] = path.stat().st_size if path.exists() else 0
            wanted.append({"job_id": record["job_id"], "broker_job_id": record["broker_job_id"],
                           "offsets": offsets})
        return wanted

    def append_logs(self, project: str, gw_job_id: str, stream: str, *, start: int,
                    data: bytes) -> int:
        """Append pushed output at exactly the buffered size; returns the new size."""
        with self._locked():
            record = self.read(gw_job_id) if GW_JOB_RE.fullmatch(gw_job_id) else None
            if (record is None or record["project"] != project
                    or record["transfer"] != TRANSFER_ACCEPTED):
                raise RelayError("no such admitted job", 404)
            path = self._log_path(gw_job_id, stream)
            size = path.stat().st_size if path.exists() else 0
            if start != size:
                raise RelayError(f"resume at byte {size}", 409)
            if size + len(data) > LOG_BUFFER_CAP:
                raise RelayError("the gateway's buffer for this job's output is full", 413)
            fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "ab") as handle:
                handle.write(data)
            return size + len(data)

    def report(self, project: str, views: dict[str, dict[str, Any]]) -> int:
        """Store the connector's client views for its own accepted jobs."""
        stored = 0
        with self._locked():
            now = _iso(_now())
            for job_id, view in views.items():
                record = self.read(job_id) if GW_JOB_RE.fullmatch(job_id) else None
                if (record is None or record["project"] != project
                        or record["transfer"] != TRANSFER_ACCEPTED or not isinstance(view, dict)):
                    continue
                record["view"] = {k: view.get(k) for k in (
                    "state", "placed", "waiting_reasons", "attempts", "cancellation",
                    "updated_at")}
                record["view_at"] = now
                self._write(record)
                stored += 1
        return stored


def _safe_parts(path: str) -> tuple[str, ...]:
    from pathlib import PurePosixPath

    pure = PurePosixPath(path)
    parts = tuple(part for part in pure.parts if part not in ("", "."))
    if not parts or pure.is_absolute() or ".." in pure.parts or len(path) > 4096:
        raise RelayError(f"unsafe result path {path!r}")
    return parts


def client_view(record: dict[str, Any], *, connector_last_seen: str | None,
                withheld: int = 0) -> dict[str, Any]:
    """What an external client sees of a relay job."""
    view = record.get("view") or {}
    request = record["request"]
    fresh = connector_last_seen is not None and (
        _now() - datetime.fromisoformat(connector_last_seen) <= FRESH
    )
    return {
        "job_id": record["job_id"], "transfer": record["transfer"],
        "state": (view.get("state") or record["transfer"]),
        "placed": bool(view.get("placed")),
        "waiting_reasons": view.get("waiting_reasons") or [],
        "refused_reason": record.get("refused_reason"),
        "program": request.get("program"), "job_name": request.get("job_name"),
        "cpus": request.get("cpus"), "mem_mb": request.get("mem_mb"),
        "created_at": record["created_at"], "updated_at": record["updated_at"],
        "status_as_of": record.get("view_at"), "connector_fresh": fresh,
        "connector_backpressure": withheld > 0,
        "collection": None if not record.get("collect") else {
            "state": record["collect"]["state"], "detail": record["collect"].get("detail"),
        },
        "cancellation": None if not record.get("cancel") else {
            "state": record["cancel"]["state"],
            "requested_at": record["cancel"]["requested_at"],
            "settled_at": record["cancel"]["settled_at"],
        },
    }
