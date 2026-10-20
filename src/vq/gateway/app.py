"""The gateway ASGI app (#163). Separate from the console; never mounted on it.

Slice 1 added identity (key authentication, scope checks, audit); slice 2
resumable uploads, submission through the broker, and job status/list;
slice 3 paged logs and cancellation; slice 4 result collection and downloads.
A request presents ``Authorization: Bearer vqk_...``; the shared admin
bearer is never a gateway credential, and keys in URLs or query strings are
not read.
"""
from __future__ import annotations

import json
import os
import re
import time
from collections.abc import Callable
from pathlib import Path, PurePosixPath
from typing import Any

from fastapi import Body, Depends, FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, JSONResponse

from vq import config
from vq.gateway import jobs as gateway_jobs
from vq.gateway import relay
from vq.gateway.jobs import JobIndex
from vq.gateway.keys import GatewayAuthError, GatewayKey, KeyStore, gateway_root
from vq.gateway.results import ResultError, ResultStore
from vq.gateway.uploads import CHUNK_LIMIT, UploadError, UploadStore

AUDIT_BASENAME = "audit.jsonl"
LOG_PUSH_SECONDS = 5.0
"""How soon a claim answers when only log pushes are wanted (#164)."""


def _audit(root: Path, entry: dict[str, Any]) -> None:
    """Append one line: who, what, outcome. Never a secret, header or payload."""
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(root / AUDIT_BASENAME, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW,
                 0o600)
    with os.fdopen(fd, "a") as stream:
        stream.write(json.dumps(entry, sort_keys=True) + "\n")


def create_gateway_app(
    cfg_loader: Callable[[], config.Config] = config.load_config,
    *,
    root: Path | None = None,
    broker_root: Path | None = None,
) -> FastAPI:
    state_root = root or gateway_root()
    store = KeyStore(state_root)
    uploads = UploadStore(state_root)
    index = JobIndex(state_root)
    results = ResultStore(state_root)
    relay_store = relay.RelayStore(state_root)
    connectors = relay.ConnectorStore(state_root)

    def relay_mode() -> bool:
        return cfg_loader().gateway.mode == "relay"

    def relay_only_local(feature: str) -> None:
        if relay_mode():
            raise HTTPException(501, f"{feature} through a relay connector arrives in a "
                                     "later release")
    app = FastAPI(title="vq submission gateway", version="1", docs_url=None, redoc_url=None)

    @app.middleware("http")
    async def audit_trail(request: Request, call_next):  # type: ignore[no-untyped-def]
        started = time.time()
        try:
            response = await call_next(request)
        except Exception:
            _audit(state_root, _entry(request, started, 500))
            raise
        _audit(state_root, _entry(request, started, response.status_code))
        return response

    def authenticated(request: Request) -> GatewayKey:
        header = request.headers.get("authorization", "")
        scheme, _, presented = header.partition(" ")
        if scheme.lower() != "bearer" or not presented:
            raise HTTPException(401, "a gateway API key is required",
                                headers={"WWW-Authenticate": "Bearer"})
        cfg = cfg_loader()
        if not cfg.gateway.enabled:
            raise HTTPException(503, "the gateway is disabled")
        try:
            key = store.authenticate(presented.strip(), projects=list(cfg.gateway.projects))
        except GatewayAuthError as exc:
            raise HTTPException(401, str(exc), headers={"WWW-Authenticate": "Bearer"}) from None
        request.state.gateway_key = key
        return key

    app.state.authenticated = authenticated

    @app.get("/v1/healthz")
    def healthz() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/v1/whoami")
    def whoami(key: GatewayKey = Depends(authenticated)) -> dict[str, Any]:  # noqa: B008
        return {"key_id": key.key_id, "principal": key.principal, "project": key.project,
                "scopes": list(key.scopes), "expires_at": key.expires_at}

    def scoped(scope: str) -> Callable[..., GatewayKey]:
        def dependency(key: GatewayKey = Depends(authenticated)) -> GatewayKey:  # noqa: B008
            if scope not in key.scopes:
                raise HTTPException(403, f"this key lacks the {scope!r} scope")
            return key

        return dependency

    can_submit = scoped("submit")
    can_read = scoped("read")

    def quota_bytes(key: GatewayKey) -> int:
        return cfg_loader().gateway.projects[key.project].storage_quota_mb * 1024 * 1024

    def result_bytes(key: GatewayKey) -> int:
        relay_ids = [r["job_id"] for r in relay_store.owned_records(key.principal, key.project)]
        return results.usage_bytes(index.owned_ids(key) + relay_ids)

    @app.post("/v1/uploads", status_code=201)
    def create_upload(body: dict[str, Any] = Body(...),  # noqa: B008
                      key: GatewayKey = Depends(can_submit)) -> dict[str, Any]:  # noqa: B008
        try:
            return uploads.create(key, size=body.get("size"), sha256=body.get("sha256"),
                                  quota_bytes=quota_bytes(key) - result_bytes(key)).as_dict()
        except UploadError as exc:
            raise HTTPException(exc.status, str(exc)) from None

    @app.put("/v1/uploads/{upload_id}")
    async def put_chunk(upload_id: str, request: Request,
                        key: GatewayKey = Depends(can_submit)) -> dict[str, Any]:  # noqa: B008
        start = _content_range_start(request.headers.get("content-range"))
        declared = request.headers.get("content-length")
        if declared is not None and declared.isdecimal() and int(declared) > CHUNK_LIMIT:
            raise HTTPException(413, "chunk exceeds the per-request limit")
        data = bytearray()
        async for part in request.stream():
            data.extend(part)
            if len(data) > CHUNK_LIMIT:
                raise HTTPException(413, "chunk exceeds the per-request limit")
        try:
            return uploads.append(upload_id, key, start=start, data=bytes(data)).as_dict()
        except UploadError as exc:
            raise HTTPException(exc.status, str(exc)) from None

    @app.get("/v1/uploads/{upload_id}")
    def get_upload(upload_id: str,
                   key: GatewayKey = Depends(can_submit)) -> dict[str, Any]:  # noqa: B008
        try:
            return uploads.owned(upload_id, key)[0].as_dict()
        except UploadError as exc:
            raise HTTPException(exc.status, str(exc)) from None

    @app.post("/v1/uploads/{upload_id}/finalize")
    def finalize_upload(upload_id: str,
                        key: GatewayKey = Depends(can_submit)) -> dict[str, Any]:  # noqa: B008
        try:
            return uploads.finalize(upload_id, key,
                                    expanded_limit=quota_bytes(key)).as_dict()
        except UploadError as exc:
            raise HTTPException(exc.status, str(exc)) from None

    @app.delete("/v1/uploads/{upload_id}", status_code=204)
    def delete_upload(upload_id: str,
                      key: GatewayKey = Depends(can_submit)) -> None:  # noqa: B008
        try:
            uploads.delete(upload_id, key)
        except UploadError as exc:
            raise HTTPException(exc.status, str(exc)) from None

    @app.post("/v1/jobs", status_code=202)
    def submit_job(response: Response, body: dict[str, Any] = Body(...),  # noqa: B008
                   key: GatewayKey = Depends(can_submit)) -> dict[str, Any]:  # noqa: B008
        if relay_mode():
            return relay_submit(response, body, key)
        try:
            view, replayed = gateway_jobs.submit(cfg_loader(), key, body, uploads=uploads,
                                                 index=index, broker_root=broker_root,
                                                 results=results)
        except gateway_jobs.GatewayJobError as exc:
            raise HTTPException(exc.status, str(exc)) from None
        if replayed:
            response.status_code = 200
        return {**view, "replayed": replayed}

    def relay_submit(response: Response, body: dict[str, Any],
                     key: GatewayKey) -> dict[str, Any]:
        cfg = cfg_loader()
        project = cfg.gateway.projects[key.project]
        try:
            intent, scoped, _payload = gateway_jobs.prepare(cfg, key, body, uploads)
            upload = uploads.owned(str(body["upload_id"]), key)[0]
        except gateway_jobs.GatewayJobError as exc:
            raise HTTPException(exc.status, str(exc)) from None
        except UploadError as exc:
            raise HTTPException(exc.status, str(exc)) from None
        if relay_store.read(relay.gateway_job_id(scoped)) is None:
            active = sum(
                1 for r in relay_store.owned_records(key.principal, key.project)
                if r["transfer"] not in (relay.TRANSFER_REFUSED, relay.TRANSFER_CANCELLED)
                and (r.get("view") or {}).get("state") != "cancelled"
                and results.manifest(r["job_id"]) is None
            )
            if active >= project.max_active_jobs:
                raise HTTPException(429, f"{active} jobs are already active for this "
                                         f"principal in {key.project}")
        try:
            record, replayed = relay_store.admit(
                principal=key.principal, project=key.project, key_id=key.key_id,
                scoped_key=scoped, request=gateway_jobs.intent_request(intent),
                upload=upload.as_dict(),
            )
        except relay.RelayError as exc:
            raise HTTPException(exc.status, str(exc)) from None
        if replayed:
            response.status_code = 200
        return {**relay.client_view(record,
                                    connector_last_seen=connectors.last_seen(key.project),
                                    withheld=relay_store.withheld(key.project)),
                "replayed": replayed}

    @app.get("/v1/jobs")
    def list_jobs(after: str | None = None, limit: int = 50,
                  key: GatewayKey = Depends(can_read)) -> dict[str, Any]:  # noqa: B008
        if relay_mode():
            seen = connectors.last_seen(key.project)
            records = [r for r in relay_store.owned_records(key.principal, key.project)
                       if after is None or r["job_id"] > after]
            page = records[: max(1, min(limit, gateway_jobs.MAX_LIST))]
            held = relay_store.withheld(key.project)
            return {"jobs": [relay.client_view(r, connector_last_seen=seen, withheld=held)
                             for r in page],
                    "next": page[-1]["job_id"] if len(records) > len(page) and page else None}
        return gateway_jobs.listing(key, index=index, broker_root=broker_root,
                                    after=after, limit=limit)

    @app.get("/v1/jobs/{job_id}")
    def get_job(job_id: str,
                key: GatewayKey = Depends(can_read)) -> dict[str, Any]:  # noqa: B008
        if relay_mode():
            try:
                record = relay_store.owned(job_id, key.principal, key.project)
            except relay.RelayError as exc:
                raise HTTPException(exc.status, str(exc)) from None
            return relay.client_view(record,
                                     connector_last_seen=connectors.last_seen(key.project),
                                     withheld=relay_store.withheld(key.project))
        try:
            return gateway_jobs.show(key, job_id, index=index, broker_root=broker_root)
        except gateway_jobs.GatewayJobError as exc:
            raise HTTPException(exc.status, str(exc)) from None

    can_cancel = scoped("cancel")

    @app.get("/v1/jobs/{job_id}/logs")
    def job_logs(job_id: str, stream: str = "stdout", offset: int = 0,
                 limit: int = 256 * 1024,
                 key: GatewayKey = Depends(can_read)) -> dict[str, Any]:  # noqa: B008
        if relay_mode():
            if not 1 <= limit <= gateway_jobs.MAX_LOG_WINDOW or offset < 0:
                raise HTTPException(400, "offset must be >= 0 and limit 1 to "
                                         f"{gateway_jobs.MAX_LOG_WINDOW}")
            try:
                record = relay_store.want_logs(job_id, key.principal, key.project)
                data, size, pushed = relay_store.read_logs(job_id, stream, offset=offset,
                                                           limit=limit)
            except relay.RelayError as exc:
                raise HTTPException(exc.status, str(exc)) from None
            view = relay.client_view(record,
                                     connector_last_seen=connectors.last_seen(key.project),
                                     withheld=relay_store.withheld(key.project))
            return {"job_id": job_id, "state": view["state"], "placed": view["placed"],
                    "stream": stream, "offset": offset, "data": data.decode(errors="replace"),
                    "next_offset": offset + len(data), "size": size, "as_of": pushed,
                    "connector_fresh": view["connector_fresh"]}
        try:
            return gateway_jobs.logs(cfg_loader(), key, job_id, index=index,
                                     broker_root=broker_root, stream=stream,
                                     offset=offset, limit=limit)
        except gateway_jobs.GatewayJobError as exc:
            raise HTTPException(exc.status, str(exc)) from None

    @app.post("/v1/jobs/{job_id}/cancel")
    def cancel_job(job_id: str,
                   key: GatewayKey = Depends(can_cancel)) -> dict[str, Any]:  # noqa: B008
        if relay_mode():
            try:
                record = relay_store.request_cancel(
                    job_id, key.principal, key.project,
                    reason=f"cancelled through the gateway by {key.principal} "
                           f"({key.key_id})")
            except relay.RelayError as exc:
                raise HTTPException(exc.status, str(exc)) from None
            view = relay.client_view(record,
                                     connector_last_seen=connectors.last_seen(key.project),
                                     withheld=relay_store.withheld(key.project))
            cancel = view["cancellation"]
            return {**view, "stopped": bool(cancel and cancel["state"] == "stopped")}
        try:
            return gateway_jobs.cancel(cfg_loader(), key, job_id, index=index,
                                       broker_root=broker_root)
        except gateway_jobs.GatewayJobError as exc:
            raise HTTPException(exc.status, str(exc)) from None

    can_download = scoped("download")

    @app.get("/v1/jobs/{job_id}/results")
    def job_results(job_id: str,
                    key: GatewayKey = Depends(can_download)) -> dict[str, Any]:  # noqa: B008
        budget = quota_bytes(key) - result_bytes(key) - uploads.usage_bytes(
            key.principal, key.project)
        if relay_mode():
            try:
                record = relay_store.owned(job_id, key.principal, key.project)
            except relay.RelayError as exc:
                raise HTTPException(exc.status, str(exc)) from None
            manifest = results.manifest(job_id)
            if manifest is not None:
                return {"ready": True, **{k: v for k, v in manifest.items() if k != "schema"}}
            record = relay_store.request_results(job_id, key.principal, key.project,
                                                 byte_budget=max(0, budget))
            view = relay.client_view(record,
                                     connector_last_seen=connectors.last_seen(key.project),
                                     withheld=relay_store.withheld(key.project))
            return {"job_id": job_id, "ready": False, "state": view["state"],
                    "transfer": view["transfer"], "collection": view["collection"],
                    "connector_fresh": view["connector_fresh"]}
        try:
            return gateway_jobs.results_view(cfg_loader(), key, job_id, index=index,
                                             broker_root=broker_root, results=results,
                                             byte_budget=max(0, budget))
        except gateway_jobs.GatewayJobError as exc:
            raise HTTPException(exc.status, str(exc)) from None

    @app.get("/v1/jobs/{job_id}/results/{path:path}")
    def job_result_file(job_id: str, path: str,
                        key: GatewayKey = Depends(can_download)) -> FileResponse:  # noqa: B008
        try:
            if relay_mode():
                relay_store.owned(job_id, key.principal, key.project)
            else:
                index.owned(job_id, key)
            location, entry = results.file(job_id, path)
        except (gateway_jobs.GatewayJobError, ResultError, relay.RelayError) as exc:
            raise HTTPException(exc.status, str(exc)) from None
        return FileResponse(location, media_type="application/octet-stream",
                            filename=PurePosixPath(path).name,
                            headers={"X-Content-SHA256": entry["sha256"]})

    # --- the connector side (#164) ---------------------------------------
    def connector_auth(request: Request) -> tuple[str, str]:
        if not cfg_loader().gateway.enabled or not relay_mode():
            raise HTTPException(404, "this gateway is not in relay mode")
        scheme, _, presented = request.headers.get("authorization", "").partition(" ")
        if scheme.lower() != "bearer" or not presented:
            raise HTTPException(401, "a connector key is required",
                                headers={"WWW-Authenticate": "Bearer"})
        try:
            connector_id, project = connectors.authenticate(presented.strip())
        except relay.RelayError as exc:
            raise HTTPException(exc.status, str(exc)) from None
        if project not in cfg_loader().gateway.projects:
            raise HTTPException(401, "this connector's project is no longer configured")
        request.state.connector_id = connector_id
        return connector_id, project

    @app.post("/v1/connector/enroll")
    def connector_enroll(body: dict[str, Any] = Body(...)) -> dict[str, Any]:  # noqa: B008
        if not cfg_loader().gateway.enabled or not relay_mode():
            raise HTTPException(404, "this gateway is not in relay mode")
        try:
            connector_id, project, key = connectors.enroll(str(body.get("token", "")))
        except relay.RelayError as exc:
            raise HTTPException(exc.status, str(exc)) from None
        return {"connector_id": connector_id, "project": project, "connector_key": key}

    @app.post("/v1/connector/claim")
    def connector_claim(body: dict[str, Any] = Body(default={}),  # noqa: B008
                        who: tuple[str, str] = Depends(connector_auth)  # noqa: B008
                        ) -> dict[str, Any]:
        wait = body.get("wait_seconds", 0)
        wait = min(30.0, max(0.0, float(wait))) if isinstance(wait, int | float) else 0.0
        max_items = body.get("max_items", 10)
        max_items = max(1, min(100, max_items)) if isinstance(max_items, int) else 10
        spool_free = body.get("spool_free_bytes")
        if spool_free is not None and (type(spool_free) is not int or spool_free < 0):
            raise HTTPException(400, "spool_free_bytes must be a non-negative integer")
        started = time.monotonic()
        deadline = started + wait
        while True:
            items = relay_store.claim(who[1], max_items=max_items, spool_free=spool_free)
            wanted = relay_store.logs_wanted(who[1])
            now = time.monotonic()
            # Work returns at once; wanted logs return within LOG_PUSH_SECONDS so
            # a follower sees output without the connector spinning.
            if items or now >= deadline or (wanted and now - started >= LOG_PUSH_SECONDS):
                return {"items": items, "logs_wanted": wanted}
            time.sleep(0.25)

    @app.put("/v1/connector/jobs/{job_id}/logs/{stream}")
    async def connector_push_logs(job_id: str, stream: str, request: Request,
                                  who: tuple[str, str] = Depends(connector_auth)  # noqa: B008
                                  ) -> dict[str, Any]:
        start = _content_range_start(request.headers.get("content-range"))
        data = bytearray()
        async for part in request.stream():
            data.extend(part)
            if len(data) > CHUNK_LIMIT:
                raise HTTPException(413, "chunk exceeds the per-request limit")
        try:
            size = relay_store.append_logs(who[1], job_id, stream, start=start,
                                           data=bytes(data))
        except relay.RelayError as exc:
            raise HTTPException(exc.status, str(exc)) from None
        return {"size": size}

    @app.get("/v1/connector/uploads/{upload_id}")
    def connector_upload(upload_id: str,
                         who: tuple[str, str] = Depends(connector_auth)  # noqa: B008
                         ) -> FileResponse:
        if not any(r["upload_id"] == upload_id and r["transfer"] == relay.TRANSFER_CLAIMED
                   for r in relay_store._all() if r["project"] == who[1]):
            raise HTTPException(404, "no such upload for a claimed item")
        try:
            payload = uploads.root / upload_id / "payload.tar"
            if not uploads._meta(upload_id)["state"] == "final" or not payload.is_file():
                raise UploadError("no such upload", 404)
        except UploadError as exc:
            raise HTTPException(exc.status, str(exc)) from None
        return FileResponse(payload, media_type="application/x-tar")

    @app.post("/v1/connector/items/{item_id}/ack")
    def connector_ack(item_id: str, body: dict[str, Any] = Body(...),  # noqa: B008
                      who: tuple[str, str] = Depends(connector_auth)  # noqa: B008
                      ) -> dict[str, Any]:
        epoch = body.get("epoch")
        if type(epoch) is not int:
            raise HTTPException(400, "epoch must be an integer")
        try:
            record = relay_store.ack(who[1], item_id, epoch=epoch,
                                     outcome=str(body.get("outcome", "")),
                                     broker_job_id=body.get("broker_job_id"),
                                     reason=body.get("reason"),
                                     manifest=body.get("manifest"),
                                     results_root=results.root)
        except relay.RelayError as exc:
            raise HTTPException(exc.status, str(exc)) from None
        return {"job_id": record["job_id"], "transfer": record["transfer"]}

    @app.put("/v1/connector/jobs/{job_id}/results/{path:path}")
    async def connector_push_result(job_id: str, path: str, request: Request,
                                    who: tuple[str, str] = Depends(connector_auth)  # noqa: B008
                                    ) -> dict[str, Any]:
        start = _content_range_start(request.headers.get("content-range"))
        data = bytearray()
        async for part in request.stream():
            data.extend(part)
            if len(data) > CHUNK_LIMIT:
                raise HTTPException(413, "chunk exceeds the per-request limit")
        try:
            size = relay_store.push_result(who[1], job_id, path, start=start,
                                           data=bytes(data), results_root=results.root)
        except relay.RelayError as exc:
            raise HTTPException(exc.status, str(exc)) from None
        return {"size": size}

    @app.post("/v1/connector/report")
    def connector_report(body: dict[str, Any] = Body(default={}),  # noqa: B008
                         who: tuple[str, str] = Depends(connector_auth)  # noqa: B008
                         ) -> dict[str, Any]:
        views = body.get("views") or {}
        if not isinstance(views, dict):
            raise HTTPException(400, "views must be an object")
        return {"stored": relay_store.report(who[1], views)}

    @app.post("/v1/connector/rotate")
    def connector_rotate(who: tuple[str, str] = Depends(connector_auth)  # noqa: B008
                         ) -> dict[str, Any]:
        try:
            return {"connector_key": connectors.rotate(who[0])}
        except relay.RelayError as exc:
            raise HTTPException(exc.status, str(exc)) from None

    @app.exception_handler(HTTPException)
    async def http_error(request: Request, exc: HTTPException) -> JSONResponse:
        return JSONResponse({"error": exc.detail}, status_code=exc.status_code,
                            headers=exc.headers)

    return app


def require_scope(app: FastAPI, scope: str) -> Callable[..., GatewayKey]:
    """A dependency for later routes: authenticated and granted ``scope``."""
    authenticated = app.state.authenticated

    def dependency(key: GatewayKey = Depends(authenticated)) -> GatewayKey:  # noqa: B008
        if scope not in key.scopes:
            raise HTTPException(403, f"this key lacks the {scope!r} scope")
        return key

    return dependency


def _content_range_start(header: str | None) -> int:
    """The start offset of ``bytes S-E/T``; a chunk must say where it goes."""
    match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", header or "")
    if match is None:
        raise HTTPException(400, "a Content-Range: bytes START-END/TOTAL header is required")
    start, end, _total = (int(value) for value in match.groups())
    if end < start:
        raise HTTPException(400, "invalid Content-Range")
    return start


def _entry(request: Request, started: float, status: int) -> dict[str, Any]:
    key: GatewayKey | None = getattr(request.state, "gateway_key", None)
    return {
        "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(started)),
        "connector_id": getattr(request.state, "connector_id", None),
        "key_id": key.key_id if key else None,
        "principal": key.principal if key else None,
        "project": key.project if key else None,
        "method": request.method, "path": request.url.path, "status": status,
    }
