"""FastAPI 入口：提交迁移清单 / 查询当前版本。"""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ValidationError

from .checkpoints import (
    CheckpointAliasMismatch,
    CheckpointCorrupt,
    CheckpointNotFound,
    CheckpointStore,
)
from .config import MAX_REQUEST_BYTES, load_settings
from .engine import (
    DatabaseBusy,
    DatabaseRegistry,
    HistoryMismatch,
    MigrationError,
    ScriptFailed,
    VersionConflict,
    apply_manifest,
    status,
)
from .manifest import MigrationManifest

settings = load_settings()
registry = DatabaseRegistry()
checkpoints = CheckpointStore(settings.checkpoint_dir)

app = FastAPI(title="SQLite Migration Backend", version="1.0.0")


@app.middleware("http")
async def limit_body(request: Request, call_next):
    declared = request.headers.get("content-length")
    if declared and int(declared) > MAX_REQUEST_BYTES:
        return JSONResponse(
            status_code=413,
            content={"detail": f"request body too large (> {MAX_REQUEST_BYTES} bytes)"},
        )
    response = await call_next(request)
    return response


@app.exception_handler(MigrationError)
async def _migration_error_handler(_: Request, exc: MigrationError) -> JSONResponse:
    if isinstance(exc, VersionConflict):
        status_code = 409
        code = "version_conflict"
    elif isinstance(exc, HistoryMismatch):
        status_code = 422
        code = "history_mismatch"
    elif isinstance(exc, DatabaseBusy):
        status_code = 503
        code = "database_busy"
    elif isinstance(exc, ScriptFailed):
        status_code = 422
        code = "migration_failed"
    elif isinstance(exc, CheckpointNotFound):
        status_code = 404
        code = "checkpoint_not_found"
    elif isinstance(exc, CheckpointAliasMismatch):
        status_code = 409
        code = "checkpoint_alias_mismatch"
    elif isinstance(exc, CheckpointCorrupt):
        status_code = 422
        code = "checkpoint_corrupt"
    else:
        status_code = 400
        code = "migration_error"
    payload = {"detail": str(exc), "code": code}
    if isinstance(exc, ScriptFailed):
        payload["failed_version"] = exc.version
        payload["reason"] = exc.reason
    return JSONResponse(status_code=status_code, content=payload)


def _resolve(alias: str):
    db_path = settings.aliases.get(alias)
    if db_path is None:
        return JSONResponse(
            status_code=404, content={"detail": f"unknown alias: {alias}", "code": "unknown_alias"}
        )
    return db_path


@app.get("/health")
async def health() -> dict:
    return {"ok": True, "aliases": sorted(settings.aliases)}


@app.get("/databases/{alias}/version")
async def get_version(alias: str):
    resolved = _resolve(alias)
    if isinstance(resolved, JSONResponse):
        return resolved
    return status(resolved)


@app.post("/databases/{alias}/migrate")
async def migrate(alias: str, request: Request):
    resolved = _resolve(alias)
    if isinstance(resolved, JSONResponse):
        return resolved
    raw = await request.body()
    if len(raw) > MAX_REQUEST_BYTES:
        return JSONResponse(
            status_code=413,
            content={"detail": f"request body too large (> {MAX_REQUEST_BYTES} bytes)"},
        )
    try:
        manifest = MigrationManifest.model_validate_json(raw)
    except ValidationError as exc:
        return JSONResponse(
            status_code=422,
            content={"detail": jsonable_encoder(exc.errors()), "code": "invalid_manifest"},
        )
    result = apply_manifest(resolved, manifest, registry.lock_for(alias))
    return {
        "alias": alias,
        "before_version": result.before_version,
        "after_version": result.after_version,
        "applied_versions": result.applied,
        "already_applied": not result.applied,
    }


def _meta_json(meta) -> dict:
    return {
        "id": meta.id,
        "alias": meta.alias,
        "version": meta.version,
        "created_at": meta.created_at,
        "sha256": meta.sha256,
        "size_bytes": meta.size_bytes,
    }


@app.post("/databases/{alias}/checkpoints", status_code=201)
async def create_checkpoint(alias: str):
    resolved = _resolve(alias)
    if isinstance(resolved, JSONResponse):
        return resolved
    meta = checkpoints.create(alias, resolved, registry.lock_for(alias))
    return _meta_json(meta)


@app.get("/databases/{alias}/checkpoints")
async def list_checkpoints(alias: str):
    resolved = _resolve(alias)
    if isinstance(resolved, JSONResponse):
        return resolved
    return {
        "alias": alias,
        "checkpoints": [_meta_json(m) for m in checkpoints.list(alias)],
    }


class RestoreRequest(BaseModel):
    checkpoint_id: str = Field(min_length=1)
    expected_version: int = Field(ge=0)


@app.post("/databases/{alias}/restore")
async def restore_checkpoint(alias: str, request: Request):
    resolved = _resolve(alias)
    if isinstance(resolved, JSONResponse):
        return resolved
    raw = await request.body()
    try:
        payload = RestoreRequest.model_validate_json(raw)
    except ValidationError as exc:
        return JSONResponse(
            status_code=422,
            content={"detail": jsonable_encoder(exc.errors()), "code": "invalid_restore_request"},
        )
    meta = checkpoints.restore(
        alias,
        resolved,
        payload.checkpoint_id,
        payload.expected_version,
        registry.lock_for(alias),
    )
    return {"alias": alias, "restored": _meta_json(meta)}
