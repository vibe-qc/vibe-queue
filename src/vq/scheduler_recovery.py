"""Audited operator evidence for an existing uncertain scheduler attempt.

Imports are authority assertions by the authenticated operator, not observations
made by vq. Only an exact positive Torque accounting record is implemented. No
negative observation can release a reservation or authorize another submission.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from vq import driver_identity, paths
from vq.config import Config
from vq.scheduler_dialect import sanitize_job_name
from vq.spec import JobSpec, utcnow_iso, validate_job_id

MAX_EVIDENCE_BYTES = 128 * 1024
ATTESTATION = "I verified this record with the authoritative scheduler operator."
_BINDING_FIELDS = (
    "id", "cwd", "command", "cpus", "mem_mb", "scheduler_tasks", "array_index",
    "submitted_at", "started_at", "scheduler_target", "scheduler_remote_workspace",
    "program", "program_runtime_pin", "submitter", "retry_count", "wall_time_seconds",
    "array_group_id", "array_total", "stdout_path", "stderr_path",
)


def digest(value: object) -> str:
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode()).hexdigest()


def _timestamp(value: object) -> float:
    if not isinstance(value, str):
        raise ValueError("evidence timestamps must be timezone-qualified ISO dates")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("evidence timestamps require a timezone")
    return parsed.timestamp()


def binding(spec: JobSpec, cfg: Config, *, owner_uid: str | None) -> dict[str, Any]:
    if not spec.scheduler_target or not spec.scheduler_remote_workspace or not spec.started_at:
        raise ValueError("original scheduler target, workspace and attempt start are required")
    host = cfg.hosts.get(spec.scheduler_target)
    if host is None or host.scheduler_dialect != "torque":
        raise ValueError("authoritative import currently supports Torque positive records only")
    identity_path = paths.state_root() / driver_identity.DRIVER_ID_BASENAME
    identity = identity_path.read_text().strip()
    if not driver_identity.is_driver_id(identity):
        raise ValueError("original driver identity is unavailable")
    data = spec.model_dump(mode="json")
    return {
        "schema": "vq.scheduler-recovery-binding/1",
        "driver_id": identity,
        "owner_uid": owner_uid if owner_uid is not None else str(os.geteuid()),
        "scheduler_endpoint": host.ssh,
        "scheduler_dialect": host.scheduler_dialect,
        "intent": {key: data.get(key) for key in _BINDING_FIELDS},
    }


def _torque_record(text: str) -> tuple[str, dict[str, str]]:
    """Strict one-record qstat -f parser; ambiguous/duplicate fields fail closed."""
    job_id: str | None = None
    fields: dict[str, str] = {}
    key: str | None = None
    for line in text.splitlines():
        if not line.strip():
            continue
        if line.startswith("Job Id: "):
            if job_id is not None:
                raise ValueError("evidence must contain exactly one scheduler record")
            job_id = line.removeprefix("Job Id: ").strip()
            key = None
        elif job_id is not None and " = " in line and line.startswith("    "):
            key, value = line.strip().split(" = ", 1)
            if key in fields:
                raise ValueError("duplicate accounting field")
            fields[key] = value
        elif line.startswith("\t") and key is not None:
            fields[key] += line.strip()
        else:
            raise ValueError("unsupported or malformed Torque accounting record")
    if job_id is None or not re.fullmatch(r"[0-9]+\.[A-Za-z0-9._-]+", job_id):
        raise ValueError("exact non-array scheduler ID including server is required")
    return job_id, fields


def plan(original: dict[str, Any], evidence: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(evidence, dict) or len(json.dumps(evidence).encode()) > MAX_EVIDENCE_BYTES:
        raise ValueError("evidence must be a bounded JSON object")
    if evidence.get("decision") != "accepted":
        raise ValueError(
            "negative resolution unavailable: complete authoritative accounting coverage "
            "and a durable no-in-flight submission fence are required; absence is not rejection"
        )
    required = {
        "schema", "decision", "binding_digest", "format", "record", "record_sha256",
        "source", "captured_at", "account", "server", "attestation", "scheduler_utc_offset",
    }
    if set(evidence) != required or evidence["schema"] != "vq.scheduler-recovery-evidence/1":
        raise ValueError("unsupported scheduler evidence schema or fields")
    if evidence["binding_digest"] != digest(original):
        raise ValueError("evidence is stale or belongs to another original job/runtime/attempt")
    if evidence["format"] != "torque-qstat-f":
        raise ValueError("unsupported authoritative scheduler record format")
    if evidence["attestation"] != ATTESTATION:
        raise ValueError("explicit authoritative scheduler operator attestation is required")
    text = evidence["record"]
    if not isinstance(text, str) or not text or "\x00" in text:
        raise ValueError("missing or invalid raw scheduler evidence")
    if hashlib.sha256(text.encode()).hexdigest() != evidence["record_sha256"]:
        raise ValueError("raw scheduler evidence digest mismatch")
    for key in ("source", "account", "server"):
        value = evidence[key]
        if not isinstance(value, str) or not value.strip() or len(value) > 1024:
            raise ValueError(f"evidence {key} is required and must be bounded")
        if any(ord(c) < 32 for c in value):
            raise ValueError(f"invalid evidence {key}")
    scheduler_id, fields = _torque_record(text)
    intent = original["intent"]
    if scheduler_id.split(".", 1)[1] != evidence["server"]:
        raise ValueError("scheduler server conflicts with the accounting ID")
    if fields.get("Job_Owner", "").split("@", 1)[0] != evidence["account"]:
        raise ValueError("scheduler account conflicts with the authoritative record")
    if fields.get("Job_Name") != sanitize_job_name(intent["id"]):
        raise ValueError("scheduler job name does not identify the original queue job")
    workspace = intent["scheduler_remote_workspace"]
    for key, basename in (("Output_Path", ".pbs-spool.out"), ("Error_Path", ".pbs-spool.err")):
        value = fields.get(key, "")
        # Torque supplies host:absolute-path. Never accept a suffix-only match.
        if ":" not in value or value.split(":", 1)[1] != f"{workspace}/{basename}":
            raise ValueError("scheduler spool paths do not identify the original workspace")
    offset = evidence["scheduler_utc_offset"]
    if not isinstance(offset, str) or not re.fullmatch(r"[+-](?:0[0-9]|1[0-4]):[0-5][0-9]", offset):
        raise ValueError("scheduler UTC offset at admission is required")
    minutes = int(offset[1:3]) * 60 + int(offset[4:])
    if minutes > 14 * 60:
        raise ValueError("invalid scheduler UTC offset")
    zone = timezone(timedelta(minutes=minutes if offset[0] == "+" else -minutes))
    try:
        raw_qtime = fields["qtime"]
        # Torque qstat -f normally formats server-local ctime. Preserve that
        # raw record and require the operator to supply its historical offset.
        queued = (int(raw_qtime) if raw_qtime.isdecimal() else
                  datetime.strptime(raw_qtime, "%a %b %d %H:%M:%S %Y")
                  .replace(tzinfo=zone).timestamp())
    except (KeyError, ValueError) as exc:
        raise ValueError("authoritative qtime and its server UTC offset are required") from exc
    started = _timestamp(intent["started_at"])
    captured = _timestamp(evidence["captured_at"])
    # qtime has whole-second precision. No arbitrary wider clock-skew allowance.
    if queued < int(started) or queued > captured or captured > _timestamp(utcnow_iso()) + 1:
        raise ValueError("scheduler admission time is outside the original attempt evidence window")
    if fields.get("job_state") not in {"Q", "W", "H", "T", "R", "E", "S", "C"}:
        raise ValueError("unsupported scheduler accounting state")
    result = {
        "schema": "vq.scheduler-recovery-plan/1", "binding": original,
        "evidence": evidence, "scheduler_job_id": scheduler_id,
    }
    return {**result, "plan_digest": digest(result)}


def _record_path(job_id: str, *, owner_uid: str | None, multi_user: bool,
                 retry_count: int = 0) -> Path:
    uid = owner_uid if owner_uid is not None else str(os.geteuid())
    if not uid.isascii() or not uid.isdecimal() or str(int(uid)) != uid:
        raise ValueError("canonical owner uid required")
    root = paths.multi_user_root() if multi_user else paths.state_root()
    if isinstance(retry_count, bool) or not isinstance(retry_count, int) or retry_count < 0:
        raise ValueError("invalid original retry count")
    return root / "scheduler-recovery" / f"{uid}.{validate_job_id(job_id)}.{retry_count}.json"


def read_record(job_id: str, *, owner_uid: str | None, multi_user: bool,
                retry_count: int = 0) -> dict | None:
    path = _record_path(job_id, owner_uid=owner_uid, multi_user=multi_user,
                        retry_count=retry_count)
    try:
        directory = path.parent.lstat()
    except FileNotFoundError:
        return None
    if (not stat.S_ISDIR(directory.st_mode) or directory.st_uid != os.geteuid()
            or directory.st_mode & 0o077):
        raise ValueError("unsafe daemon-owned reconciliation directory")
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError:
        return None
    with os.fdopen(fd, "rb") as stream:
        metadata = os.fstat(stream.fileno())
        if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid()
                or metadata.st_mode & 0o077 or metadata.st_size > MAX_EVIDENCE_BYTES * 2):
            raise ValueError("unsafe daemon-owned reconciliation record")
        result = json.loads(stream.read(MAX_EVIDENCE_BYTES * 2 + 1))
    if not isinstance(result, dict):
        raise ValueError("invalid reconciliation record")
    return result


def persist(plan_data: dict, *, owner_uid: str | None, multi_user: bool,
            operator_uid: int | None) -> dict:
    """Caller holds the original spec lock; a repeated identical apply is a read."""
    job_id = plan_data["binding"]["intent"]["id"]
    retry_count = plan_data["binding"]["intent"]["retry_count"]
    identity = dict(owner_uid=owner_uid, multi_user=multi_user, retry_count=retry_count)
    existing = read_record(job_id, **identity)
    if existing is not None:
        if existing.get("plan_digest") != plan_data["plan_digest"]:
            raise ValueError("conflicting reconciliation already exists; preserve both sources")
        return existing
    path = _record_path(job_id, **identity)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    # Recheck the directory after creation; do not follow a replaced parent.
    read_record(job_id, **identity)
    result = {**plan_data, "applied_at": utcnow_iso(), "operator_uid": operator_uid,
              "operator_auth": "admin-token" if multi_user else "owner-socket"}
    serialized = json.dumps(result, sort_keys=True) + "\n"
    if len(serialized.encode()) > MAX_EVIDENCE_BYTES * 2:
        raise ValueError("original intent plus evidence exceeds private journal size limit")
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        try:
            stream.write(serialized)
            stream.flush()
            os.fsync(stream.fileno())
            os.replace(temporary, path)
            directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            temporary.unlink(missing_ok=True)
    return result


def accepted_id(spec: JobSpec, cfg: Config, *, owner_uid: str | None,
                multi_user: bool) -> str | None:
    record = read_record(spec.id, owner_uid=owner_uid, multi_user=multi_user,
                         retry_count=spec.retry_count)
    if record is None:
        return None
    current = plan(binding(spec, cfg, owner_uid=owner_uid), record["evidence"])
    if current["plan_digest"] != record.get("plan_digest"):
        raise ValueError("stored reconciliation conflicts with original job identity")
    return current["scheduler_job_id"]
