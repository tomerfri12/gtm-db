from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status

from gtmdb.client import GtmDB
from gtmdb.scope import Scope
from gtmdb.server.deps import get_db, require_admin_scope
from gtmdb.server.schemas.admin import (
    KeyCreateBody,
    KeyCreatedResponse,
    KeyInfoResponse,
)

router = APIRouter(prefix="/admin", tags=["admin"])


@router.post("/keys", response_model=KeyCreatedResponse)
async def admin_create_key(
    body: KeyCreateBody,
    db: Annotated[GtmDB, Depends(get_db)],
    admin_scope: Annotated[Scope, Depends(require_admin_scope)],
) -> KeyCreatedResponse:
    tenant = body.tenant_id or admin_scope.tenant_id
    try:
        result = await db.api_keys.create(
            owner_id=body.owner_id,
            owner_type=body.owner_type,
            tenant_id=str(tenant),
            preset_names=body.preset_names,
            label=body.label,
            expires_in_days=body.expires_in_days,
            created_by="api",
        )
    except PermissionError as e:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail=str(e)) from e
    except KeyError as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
    return KeyCreatedResponse(
        raw_key=result.raw_key,
        key_id=result.key_id,
        owner_id=result.owner_id,
        label=result.label,
        expires_at=result.expires_at,
    )


@router.get("/keys", response_model=list[KeyInfoResponse])
async def admin_list_keys(
    db: Annotated[GtmDB, Depends(get_db)],
    admin_scope: Annotated[Scope, Depends(require_admin_scope)],
) -> list[KeyInfoResponse]:
    try:
        keys = await db.api_keys.list_keys(admin_scope.tenant_id)
    except PermissionError as e:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail=str(e)) from e
    return [
        KeyInfoResponse(
            key_id=k.key_id,
            owner_id=k.owner_id,
            owner_type=k.owner_type,
            label=k.label,
            is_active=k.is_active,
            expires_at=k.expires_at,
            created_at=k.created_at,
            last_used_at=k.last_used_at,
        )
        for k in keys
    ]


@router.delete("/keys/{key_id}")
async def admin_revoke_key(
    db: Annotated[GtmDB, Depends(get_db)],
    _: Annotated[Scope, Depends(require_admin_scope)],
    key_id: str,
) -> dict[str, bool]:
    try:
        ok = await db.api_keys.revoke(key_id)
    except PermissionError as e:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail=str(e)) from e
    if not ok:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Key not found")
    return {"revoked": True}


@router.post("/keys/{key_id}/rotate", response_model=KeyCreatedResponse)
async def admin_rotate_key(
    key_id: str,
    db: Annotated[GtmDB, Depends(get_db)],
    _: Annotated[Scope, Depends(require_admin_scope)],
    body: dict[str, Any] = Body(default_factory=dict),
) -> KeyCreatedResponse:
    expires_in_days = body.get("expires_in_days")
    try:
        result = await db.api_keys.rotate(
            key_id, expires_in_days=expires_in_days
        )
    except PermissionError as e:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=str(e)) from e
    return KeyCreatedResponse(
        raw_key=result.raw_key,
        key_id=result.key_id,
        owner_id=result.owner_id,
        label=result.label,
        expires_at=result.expires_at,
    )


def _parse_activity_ts(raw: str | None):
    from datetime import datetime

    if raw is None or not str(raw).strip():
        return None
    s = str(raw).strip().replace("Z", "+00:00")
    return datetime.fromisoformat(s)


@router.get("/activity-log")
async def admin_activity_log(
    db: Annotated[GtmDB, Depends(get_db)],
    _: Annotated[Scope, Depends(require_admin_scope)],
    tenant_id: str | None = None,
    owner_id: str | None = None,
    key_id: str | None = None,
    action: str | None = None,
    entity_type: str | None = None,
    from_ts: Annotated[str | None, Query(alias="from")] = None,
    to_ts: Annotated[str | None, Query(alias="to")] = None,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[dict[str, Any]]:
    if not db._settings.key_store_url:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Activity log requires GTMDB_KEY_STORE_URL (Postgres)",
        )
    ks = db._get_key_store()
    rows = await ks.list_activity_log(
        tenant_id=tenant_id,
        owner_id=owner_id,
        key_id=key_id,
        action=action,
        entity_type=entity_type,
        from_ts=_parse_activity_ts(from_ts),
        to_ts=_parse_activity_ts(to_ts),
        limit=limit,
        offset=offset,
    )
    out = []
    for r in rows:
        d = dict(r)
        ts = d.get("timestamp")
        if ts is not None and hasattr(ts, "isoformat"):
            d["timestamp"] = ts.isoformat()
        out.append(d)
    return out
