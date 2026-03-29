"""FastAPI dependencies: DB singleton, API key → Scope, admin guard."""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from gtmdb.api_keys import set_request_scope
from gtmdb.client import GtmDB
from gtmdb.config import GtmdbSettings
from gtmdb.presets import create_token_from_presets
from gtmdb.scope import Scope

security = HTTPBearer(auto_error=False)


def get_settings(request: Request) -> GtmdbSettings:
    return request.app.state.gtmdb_settings


def get_db(request: Request) -> GtmDB:
    return request.app.state.db


async def get_scope(
    request: Request,
    creds: Annotated[HTTPAuthorizationCredentials | None, Depends(security)],
) -> Scope:
    if creds is None or (creds.scheme or "").lower() != "bearer":
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid Authorization header (expected Bearer token)",
        )
    raw = (creds.credentials or "").strip()
    if not raw:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            detail="Empty Bearer token",
        )

    db: GtmDB = request.app.state.db
    cfg: GtmdbSettings = request.app.state.gtmdb_settings

    if cfg.admin_key and raw == cfg.admin_key:
        token = create_token_from_presets(
            tenant_id=cfg.default_tenant_id,
            owner_id="admin",
            owner_type="admin",
            preset_names=["full_access"],
        )
        scope = Scope(token)
    else:
        try:
            scope = await db.api_keys.resolve(raw)
        except ValueError as e:
            raise HTTPException(
                status.HTTP_401_UNAUTHORIZED,
                detail=str(e) or "Invalid API key",
            ) from e
        except Exception as e:
            raise HTTPException(
                status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate API key",
            ) from e

    set_request_scope(scope)
    return scope


async def require_admin_scope(
    scope: Annotated[Scope, Depends(get_scope)],
) -> Scope:
    if scope.owner_type != "admin":
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            detail="Admin API key required for this operation",
        )
    return scope
