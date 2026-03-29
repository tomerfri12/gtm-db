from __future__ import annotations

from typing import Annotated, Any

from fastapi import Body, Depends, HTTPException, status

from gtmdb.client import GtmDB
from gtmdb.scope import Scope
from gtmdb.server.deps import get_db, get_scope
from gtmdb.server.routers._crud import build_crud_router
from gtmdb.server.util import entity_as_dict

router = build_crud_router("email-campaigns", db_attr="email_campaigns")


@router.post("/with-artifacts")
async def create_email_campaign_with_artifacts(
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
    emails = data.pop("emails", None)
    if not emails or not isinstance(emails, list):
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail="emails must be a non-empty list",
        )
    lead_ids = data.pop("lead_ids", None)
    if lead_ids is not None and not isinstance(lead_ids, list):
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail="lead_ids must be a list when provided",
        )
    try:
        out = await db.email_campaigns.create_with_artifacts(
            scope,
            actor_id=actor_id,
            emails=emails,
            lead_ids=lead_ids,
            has_email_reasoning=data.pop("has_email_reasoning", None),
            sourced_from_reasoning=data.pop("sourced_from_reasoning", None),
            **data,
        )
    except PermissionError as e:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
    return {
        "campaign": entity_as_dict(out["campaign"]),
        "email_ids": out["email_ids"],
        "linked_lead_count": out["linked_lead_count"],
    }
