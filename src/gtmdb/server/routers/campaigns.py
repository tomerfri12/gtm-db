from __future__ import annotations

from typing import Annotated, Any

from fastapi import Body, Depends, HTTPException, status

from gtmdb.client import GtmDB
from gtmdb.scope import Scope
from gtmdb.server.deps import get_db, get_scope
from gtmdb.server.routers._crud import build_crud_router

router = build_crud_router("campaigns", db_attr="campaigns")


@router.post("/{campaign_id}/add-lead")
async def campaign_add_lead(
    campaign_id: str,
    db: Annotated[GtmDB, Depends(get_db)],
    scope: Annotated[Scope, Depends(get_scope)],
    body: dict[str, Any] = Body(...),
) -> dict[str, str]:
    lid = (body.get("lead_id") or "").strip()
    reasoning = (body.get("reasoning") or "").strip()
    if not lid or not reasoning:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail="lead_id and reasoning are required",
        )
    try:
        await db.campaigns.add_lead(
            scope, campaign_id, lid, reasoning=reasoning
        )
    except PermissionError as e:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail=str(e)) from e
    return {"status": "linked"}
