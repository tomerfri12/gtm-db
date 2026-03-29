from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, status

from gtmdb.api_keys import canonical_owner_type
from gtmdb.client import GtmDB
from gtmdb.scope import Scope
from gtmdb.server.deps import get_db, get_scope

router = APIRouter(prefix="/activity-log", tags=["activity"])


def _parse_activity_ts(raw: str | None):
    from datetime import datetime

    if raw is None or not str(raw).strip():
        return None
    s = str(raw).strip().replace("Z", "+00:00")
    return datetime.fromisoformat(s)


@router.get("")
async def activity_log(
    db: Annotated[GtmDB, Depends(get_db)],
    scope: Annotated[Scope, Depends(get_scope)],
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

    if scope.owner_type == "admin":
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
    else:
        rows = await ks.list_activity_log(
            tenant_id=scope.tenant_id,
            owner_id=scope.owner_id,
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
        ot = d.get("owner_type")
        if ot is not None:
            d["owner_type"] = canonical_owner_type(str(ot))
        # Legacy rows: explore used entity_type "entity"
        if d.get("action") == "explore" and d.get("entity_type") == "entity":
            d["entity_type"] = "explore"
        out.append(d)
    return out
