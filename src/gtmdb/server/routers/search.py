from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, status

from gtmdb.client import GtmDB
from gtmdb.scope import Scope
from gtmdb.server.deps import get_db, get_scope

router = APIRouter(tags=["search"])


@router.get("/search")
async def search_entities(
    db: Annotated[GtmDB, Depends(get_db)],
    scope: Annotated[Scope, Depends(get_scope)],
    q: str = Query(..., min_length=1),
    limit: int = Query(default=25, ge=1, le=100),
) -> list[dict[str, Any]]:
    try:
        hits = await db.search(scope, q, limit=limit)
    except PermissionError as e:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail=str(e)) from e
    out: list[dict[str, Any]] = []
    for h in hits:
        node = h["node"]
        props = dict(node.properties)
        name = (
            props.get("name")
            or props.get("email")
            or (
                (props.get("first_name") or "")
                + " "
                + (props.get("last_name") or "")
            ).strip()
            or props.get("company_name")
        )
        out.append(
            {
                "type": node.label,
                "id": node.id,
                "name": name,
            }
        )
    return out
