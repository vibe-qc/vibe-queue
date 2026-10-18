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
from vq.gateway.jobs import JobIndex
from vq.gateway.keys import GatewayAuthError, GatewayKey, KeyStore, gateway_root
from vq.gateway.results import ResultError, ResultStore
from vq.gateway.uploads import CHUNK_LIMIT, UploadError, UploadStore

AUDIT_BASENAME = "audit.jsonl"


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
        return results.usage_bytes(index.owned_ids(key))

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
        try:
            view, replayed = gateway_jobs.submit(cfg_loader(), key, body, uploads=uploads,
                                                 index=index, broker_root=broker_root,
                                                 results=results)
        except gateway_jobs.GatewayJobError as exc:
            raise HTTPException(exc.status, str(exc)) from None
        if replayed:
            response.status_code = 200
        return {**view, "replayed": replayed}

    @app.get("/v1/jobs")
    def list_jobs(after: str | None = None, limit: int = 50,
                  key: GatewayKey = Depends(can_read)) -> dict[str, Any]:  # noqa: B008
        return gateway_jobs.listing(key, index=index, broker_root=broker_root,
                                    after=after, limit=limit)

    @app.get("/v1/jobs/{job_id}")
    def get_job(job_id: str,
                key: GatewayKey = Depends(can_read)) -> dict[str, Any]:  # noqa: B008
        try:
            return gateway_jobs.show(key, job_id, index=index, broker_root=broker_root)
        except gateway_jobs.GatewayJobError as exc:
            raise HTTPException(exc.status, str(exc)) from None

    can_cancel = scoped("cancel")

    @app.get("/v1/jobs/{job_id}/logs")
    def job_logs(job_id: str, stream: str = "stdout", offset: int = 0,
                 limit: int = 256 * 1024,
                 key: GatewayKey = Depends(can_read)) -> dict[str, Any]:  # noqa: B008
        try:
            return gateway_jobs.logs(cfg_loader(), key, job_id, index=index,
                                     broker_root=broker_root, stream=stream,
                                     offset=offset, limit=limit)
        except gateway_jobs.GatewayJobError as exc:
            raise HTTPException(exc.status, str(exc)) from None

    @app.post("/v1/jobs/{job_id}/cancel")
    def cancel_job(job_id: str,
                   key: GatewayKey = Depends(can_cancel)) -> dict[str, Any]:  # noqa: B008
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
            index.owned(job_id, key)
            location, entry = results.file(job_id, path)
        except (gateway_jobs.GatewayJobError, ResultError) as exc:
            raise HTTPException(exc.status, str(exc)) from None
        return FileResponse(location, media_type="application/octet-stream",
                            filename=PurePosixPath(path).name,
                            headers={"X-Content-SHA256": entry["sha256"]})

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
        "key_id": key.key_id if key else None,
        "principal": key.principal if key else None,
        "project": key.project if key else None,
        "method": request.method, "path": request.url.path, "status": status,
    }
