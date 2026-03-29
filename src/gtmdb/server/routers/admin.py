from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends, HTTPException, status

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
