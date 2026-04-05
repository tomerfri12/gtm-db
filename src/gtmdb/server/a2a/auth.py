"""Resolve Bearer tokens to Scope for A2A (aligned with REST ``get_scope``)."""

from __future__ import annotations

from starlette.requests import Request

from gtmdb.api_keys import set_request_scope
from gtmdb.client import GtmDB
from gtmdb.config import GtmdbSettings
from gtmdb.presets import create_token_from_presets
from gtmdb.scope import Scope


class BearerAuthFailed(Exception):
    """Raised when A2A JSON-RPC should not proceed (maps to 401)."""

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


async def resolve_bearer_to_scope(request: Request) -> Scope:
    """Validate ``Authorization: Bearer`` and attach scope to request state.

    Raises
    ------
    BearerAuthFailed
        Missing/invalid token.
    """
    auth = (request.headers.get("authorization") or "").strip()
    if not auth.lower().startswith("bearer "):
        raise BearerAuthFailed(
            "Missing or invalid Authorization header (expected Bearer token)"
        )
    raw = auth[7:].strip()
    if not raw:
        raise BearerAuthFailed("Empty Bearer token")

    db: GtmDB = request.app.state.db
    cfg: GtmdbSettings = request.app.state.gtmdb_settings

    if cfg.admin_key and raw == cfg.admin_key:
        token = create_token_from_presets(
            tenant_id=cfg.default_tenant_id,
            owner_id="admin",
            owner_type="admin",
            preset_names=["full_access"],
            key_id="admin",
        )
        scope = Scope(token)
    else:
        try:
            scope = await db.api_keys.resolve(raw)
        except ValueError as e:
            raise BearerAuthFailed(str(e) or "Invalid API key") from e
        except Exception as e:
            raise BearerAuthFailed("Could not validate API key") from e

    set_request_scope(scope)
    request.state.gtmdb_scope = scope
    return scope
