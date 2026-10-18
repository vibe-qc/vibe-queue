"""Gateway submissions (#163 slice 2): policy, broker intake and ownership.

The external request is mapped onto a :class:`~vq.broker_store.BrokerIntent`
by server-side project policy; a client never supplies a raw JobSpec, uid,
path, target or hook. The broker's keyed intake gives durable, idempotent
admission. The client's idempotency key is namespaced by project and
principal before it reaches the broker, so the same content returns the
original job and changed content conflicts. A per-job ownership record lets
only the submitting principal in its project read or control the job.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any

from vq import broker_service, broker_store, config
from vq.gateway.keys import GatewayKey, _iso, _now, _private_dir
from vq.gateway.uploads import UploadError, UploadStore

CLIENT_KEY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
EXPECTED_SHA_RE = re.compile(r"[0-9a-f]{40}")
WAITING_STATES = frozenset({
    broker_store.STATE_HELD, broker_store.STATE_RESERVED, broker_store.STATE_TRANSFERRING,
})
OWNER_SCHEMA = "vq.gateway-job/1"
MAX_LIST = 100


class GatewayJobError(ValueError):
    def __init__(self, message: str, status: int = 400) -> None:
        super().__init__(message)
        self.status = status


def broker_key(key: GatewayKey, client_key: str) -> str:
    """The broker idempotency key for one client key, scoped to its owner."""
    material = f"{key.project}\0{key.principal}\0{client_key}".encode()
    return "gw-" + hashlib.sha256(material).hexdigest()[:40]


class JobIndex:
    """Ownership records: broker job id -> principal, project, key, upload."""

    def __init__(self, root: Path) -> None:
        self.root = root / "jobs"

    def _path(self, broker_job_id: str) -> Path:
        if not broker_store.BROKER_JOB_ID_PATTERN.fullmatch(broker_job_id):
            raise GatewayJobError("no such job", 404)
        return self.root / f"{broker_job_id}.json"

    def read(self, broker_job_id: str) -> dict[str, Any] | None:
        try:
            return json.loads(self._path(broker_job_id).read_text())
        except FileNotFoundError:
            return None

    def record(self, broker_job_id: str, owner: dict[str, Any]) -> dict[str, Any]:
        _private_dir(self.root.parent)
        _private_dir(self.root)
        existing = self.read(broker_job_id)
        if existing is not None:
            if (existing["principal"], existing["project"]) != (
                owner["principal"], owner["project"]
            ):
                raise GatewayJobError("job ownership conflict", 409)
            return existing
        path = self._path(broker_job_id)
        temporary = path.with_suffix(".tmp")
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "w") as stream:
            stream.write(json.dumps(owner, sort_keys=True))
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)  # first writer wins
        except FileExistsError:
            pass
        finally:
            temporary.unlink(missing_ok=True)
        return self.read(broker_job_id) or owner

    def owned_ids(self, key: GatewayKey) -> list[str]:
        if not self.root.exists():
            return []
        found = []
        for child in sorted(self.root.glob("b-*.json")):
            try:
                owner = json.loads(child.read_text())
            except (OSError, ValueError):
                continue
            if (owner.get("principal"), owner.get("project")) == (key.principal, key.project):
                found.append(child.stem)
        return found

    def owned(self, broker_job_id: str, key: GatewayKey) -> dict[str, Any]:
        owner = self.read(broker_job_id)
        if owner is None or (owner["principal"], owner["project"]) != (
            key.principal, key.project
        ):
            raise GatewayJobError("no such job", 404)  # never reveal another's job
        return owner


def client_view(record: broker_store.BrokerRecord) -> dict[str, Any]:
    """What an external client may see: no command, payload, host or diagnostic."""
    attempt = record.current_attempt
    reasons = record.held_blockers or (attempt.blockers if attempt else {})
    return {
        "job_id": record.broker_job_id,
        "state": record.state,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
        "program": record.intent.program,
        "job_name": record.intent.job_name,
        "cpus": record.intent.cpus,
        "mem_mb": record.intent.mem_mb,
        "attempts": len(record.attempts),
        "placed": record.state == broker_store.STATE_PLACED,
        "waiting_reasons": sorted({broker_service._console_blocker(r)
                                   for r in reasons.values()}),
        "cancellation": None if record.cancellation is None else {
            "requested_at": record.cancellation.requested_at,
            "completed": record.cancellation.completed_at is not None,
        },
    }


def _intent(request: dict[str, Any], project: config.GatewayProjectConfig
            ) -> broker_store.BrokerIntent:
    allowed = {"upload_id", "command", "program", "expected_sha", "cpus", "mem_mb",
               "wall_time_seconds", "job_name", "tags", "idempotency_key"}
    extra = set(request) - allowed
    if extra:
        raise GatewayJobError(f"unsupported fields: {', '.join(sorted(extra))}")
    command = request.get("command")
    if (not isinstance(command, list) or not command or len(command) > 64
            or not all(isinstance(p, str) and 0 < len(p) <= 4096 for p in command)):
        raise GatewayJobError("command must be a list of 1-64 non-empty strings")
    program = request.get("program")
    if program not in project.programs:
        raise GatewayJobError(f"program must be one of {project.programs}", 403)
    limits = (("cpus", project.max_cpus), ("mem_mb", project.max_mem_mb),
              ("wall_time_seconds", project.max_wall_time_seconds))
    for name, ceiling in limits:
        value = request.get(name)
        if type(value) is not int or not 1 <= value <= ceiling:
            raise GatewayJobError(f"{name} must be an integer from 1 to {ceiling}", 403
                                  if type(value) is int and value > ceiling else 400)
    expected_sha = request.get("expected_sha")
    if expected_sha is not None and (not isinstance(expected_sha, str)
                                     or not EXPECTED_SHA_RE.fullmatch(expected_sha)):
        raise GatewayJobError("expected_sha must be a full 40-hex commit")
    tags = request.get("tags") or []
    if not isinstance(tags, list) or len(tags) > 16 or not all(
        isinstance(t, str) for t in tags
    ):
        raise GatewayJobError("tags must be a list of at most 16 strings")
    job_name = request.get("job_name")
    try:
        return broker_store.BrokerIntent(
            command=tuple(command), cpus=request["cpus"], mem_mb=request["mem_mb"],
            wall_time_seconds=request["wall_time_seconds"], program=program,
            expected_sha=expected_sha, job_name=job_name, tags=tuple(tags),
        )
    except (broker_store.BrokerStoreError, ValueError) as exc:
        raise GatewayJobError(str(exc)) from None


def submit(cfg: config.Config, key: GatewayKey, request: dict[str, Any], *,
           uploads: UploadStore, index: JobIndex, broker_root: Path | None,
           results: Any = None) -> tuple[dict[str, Any], bool]:
    """Admit one job; returns (client view, replayed)."""
    project = cfg.gateway.projects[key.project]
    client_key = request.get("idempotency_key")
    if not isinstance(client_key, str) or not CLIENT_KEY_RE.fullmatch(client_key):
        raise GatewayJobError("idempotency_key is required: 1-128 of A-Z a-z 0-9 . _ : -")
    intent = _intent(request, project)
    try:
        payload = uploads.payload(str(request.get("upload_id", "")), key)
    except UploadError as exc:
        raise GatewayJobError(str(exc), exc.status) from None
    scoped = broker_key(key, client_key)
    replay_id = broker_store.broker_job_id_for_key(scoped)
    replayed = index.read(replay_id) is not None
    if not replayed:
        waiting = 0
        with broker_store.BrokerStore.open(broker_root) as store:
            for job_id in index.owned_ids(key):
                try:
                    state = store.get(job_id).state
                except broker_store.BrokerStoreError:
                    continue
                # A placed job counts until its results are collected (slice 4).
                if state in WAITING_STATES or (
                    state == broker_store.STATE_PLACED and results is not None
                    and results.manifest(job_id) is None
                ):
                    waiting += 1
        if waiting >= project.max_active_jobs:
            raise GatewayJobError(
                f"{waiting} jobs are already active for this principal in {key.project}",
                429,
            )
    try:
        from vq import submit as submit_module

        submit_module.validate_staged_python_entrypoint(
            command=list(intent.command), source_archive=payload, interpreter_explicit=False,
        )
        record = broker_service.accept(cfg, payload, intent, root=broker_root,
                                       idempotency_key=scoped)
    except broker_store.BrokerIdempotencyConflict:
        raise GatewayJobError(
            "this idempotency_key already names a different submission", 409
        ) from None
    except broker_store.BrokerStoreBusy as exc:
        raise GatewayJobError(str(exc), 503) from None
    except (broker_store.BrokerStoreError, ValueError, OSError) as exc:
        raise GatewayJobError(str(exc), 422) from None
    index.record(record.broker_job_id, {
        "schema": OWNER_SCHEMA, "principal": key.principal, "project": key.project,
        "key_id": key.key_id, "upload_id": request["upload_id"],
        "idempotency_key": client_key, "submitted_at": _iso(_now()),
    })
    return client_view(record), replayed


def show(key: GatewayKey, broker_job_id: str, *, index: JobIndex,
         broker_root: Path | None) -> dict[str, Any]:
    index.owned(broker_job_id, key)
    with broker_store.BrokerStore.open(broker_root) as store:
        return client_view(store.get(broker_job_id))


def listing(key: GatewayKey, *, index: JobIndex, broker_root: Path | None,
            after: str | None, limit: int) -> dict[str, Any]:
    ids = index.owned_ids(key)
    if after is not None:
        ids = [job_id for job_id in ids if job_id > after]
    page = ids[: max(1, min(limit, MAX_LIST))]
    jobs = []
    with broker_store.BrokerStore.open(broker_root) as store:
        for job_id in page:
            try:
                jobs.append(client_view(store.get(job_id)))
            except broker_store.BrokerStoreError:
                continue
    more = len(ids) > len(page)
    return {"jobs": jobs, "next": page[-1] if more and page else None}


LOG_STREAMS = ("stdout", "stderr")
MAX_LOG_WINDOW = 1024 * 1024


def logs(cfg: config.Config, key: GatewayKey, broker_job_id: str, *, index: JobIndex,
         broker_root: Path | None, stream: str, offset: int, limit: int) -> dict[str, Any]:
    """One byte window of a placed job's stdout or stderr, read on its target.

    The target returns the whole stream (bounded by the relay's 64 MiB cap);
    the client pages through it with ``offset``. A job not placed yet has no
    output, which is not an error.
    """
    from vq import broker_relay, transport

    if stream not in LOG_STREAMS:
        raise GatewayJobError("stream must be stdout or stderr")
    if offset < 0 or not 1 <= limit <= MAX_LOG_WINDOW:
        raise GatewayJobError(f"offset must be >= 0 and limit 1 to {MAX_LOG_WINDOW}")
    index.owned(broker_job_id, key)
    with broker_store.BrokerStore.open(broker_root) as store:
        record = store.get(broker_job_id)
    if record.state != broker_store.STATE_PLACED:
        return {"job_id": broker_job_id, "state": record.state, "placed": False,
                "stream": stream, "offset": offset, "data": "", "next_offset": offset}
    try:
        text = broker_relay.proxy_text(cfg, record, "logs", "localhost", "{target}",
                                       f"--{stream}", "-n", "0")
    except (transport.RemoteError, broker_store.BrokerStoreError) as exc:
        raise GatewayJobError(f"the job's output is unavailable right now: "
                              f"{type(exc).__name__}", 502) from None
    raw = text.encode()
    window = raw[offset:offset + limit]
    return {"job_id": broker_job_id, "state": record.state, "placed": True,
            "stream": stream, "offset": offset,
            "data": window.decode(errors="replace"),
            "next_offset": offset + len(window), "size": len(raw)}


def cancel(cfg: config.Config, key: GatewayKey, broker_job_id: str, *, index: JobIndex,
           broker_root: Path | None) -> dict[str, Any]:
    """Cancel one owned job: at the broker, and on its target once placed.

    Repeatable. A transfer in doubt cannot be stopped from here; the answer
    says so, and the broker's operator action applies.
    """
    from vq import broker_relay, transport

    index.owned(broker_job_id, key)
    reason = f"cancelled through the gateway by {key.principal} ({key.key_id})"
    try:
        _message, record, stopped = broker_relay._kill(
            cfg, broker_job_id, reason=reason, root=broker_root,
        )
    except transport.RemoteError as exc:
        raise GatewayJobError(f"the cancellation could not reach the job's host: "
                              f"{type(exc).__name__}", 502) from None
    except broker_store.BrokerStoreBusy as exc:
        raise GatewayJobError(str(exc), 503) from None
    except broker_store.BrokerStoreError:
        raise GatewayJobError("the cancellation did not complete; retry it", 502) from None
    return {**client_view(record), "stopped": stopped}


def results_view(cfg: config.Config, key: GatewayKey, broker_job_id: str, *,
                 index: JobIndex, broker_root: Path | None, results: Any,
                 byte_budget: int) -> dict[str, Any]:
    """The job's result manifest, collecting it first once the job has finished."""
    from vq import transport
    from vq.gateway.results import ResultError

    index.owned(broker_job_id, key)
    manifest = results.manifest(broker_job_id)
    if manifest is None:
        with broker_store.BrokerStore.open(broker_root) as store:
            record = store.get(broker_job_id)
        try:
            manifest = results.collect(cfg, record, byte_budget=byte_budget)
        except ResultError as exc:
            if exc.status == 409:
                return {"job_id": broker_job_id, "ready": False, "state": record.state,
                        "detail": str(exc)}
            raise GatewayJobError(str(exc), exc.status) from None
        except (transport.RemoteError, ValueError, OSError) as exc:
            raise GatewayJobError(f"the job's results are unavailable right now: "
                                  f"{type(exc).__name__}", 502) from None
    return {"ready": True, **{k: v for k, v in manifest.items() if k != "schema"}}
