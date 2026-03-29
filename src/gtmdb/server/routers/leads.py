from __future__ import annotations

from typing import Annotated, Any

from fastapi import Body, Depends, HTTPException, status

from gtmdb.client import GtmDB
from gtmdb.scope import Scope
from gtmdb.server.deps import get_db, get_scope
from gtmdb.server.routers._crud import build_crud_router
from gtmdb.server.util import entity_as_dict

router = build_crud_router("leads", db_attr="leads")


@router.post("/{lead_id}/scores")
async def add_lead_score(
    lead_id: str,
    db: Annotated[GtmDB, Depends(get_db)],
    scope: Annotated[Scope, Depends(get_scope)],
    body: dict[str, Any] = Body(...),
) -> dict[str, Any]:
    data = dict(body)
    actor_id = (data.pop("actor_id", None) or scope.owner_id or "").strip()
    if not actor_id:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail="actor_id required",
        )
    has_score_reasoning = data.pop("has_score_reasoning", None)
    if not (has_score_reasoning and str(has_score_reasoning).strip()):
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail="has_score_reasoning is required",
        )
    reasoning = data.pop("reasoning", None)
    try:
        score = await db.leads.add_score(
            scope,
            lead_id,
            actor_id=actor_id,
            has_score_reasoning=str(has_score_reasoning).strip(),
            reasoning=reasoning,
            **data,
        )
    except PermissionError as e:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
    return entity_as_dict(score)


@router.post("/{lead_id}/link-campaign")
async def link_lead_campaign(
    lead_id: str,
    db: Annotated[GtmDB, Depends(get_db)],
    scope: Annotated[Scope, Depends(get_scope)],
    body: dict[str, Any] = Body(...),
) -> dict[str, str]:
    cid = (body.get("campaign_id") or "").strip()
    reasoning = (body.get("reasoning") or "").strip()
    if not cid or not reasoning:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail="campaign_id and reasoning are required",
        )
    try:
        await db.leads.link_campaign(
            scope, lead_id, cid, reasoning=reasoning
        )
    except PermissionError as e:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail=str(e)) from e
    return {"status": "linked"}
