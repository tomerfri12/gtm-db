from __future__ import annotations

from typing import Annotated, Any

from fastapi import Body, Depends, HTTPException, status

from gtmdb.client import GtmDB
from gtmdb.scope import Scope
from gtmdb.server.deps import get_db, get_scope
from gtmdb.server.routers._crud import build_crud_router

router = build_crud_router("deals", db_attr="deals")


@router.post("/{deal_id}/assign-account")
async def assign_deal_account(
    deal_id: str,
    db: Annotated[GtmDB, Depends(get_db)],
    scope: Annotated[Scope, Depends(get_scope)],
    body: dict[str, Any] = Body(...),
) -> dict[str, str]:
    aid = (body.get("account_id") or "").strip()
    reasoning = (body.get("reasoning") or "").strip()
    if not aid or not reasoning:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail="account_id and reasoning are required",
        )
    try:
        await db.deals.assign_to_account(
            scope, deal_id, aid, reasoning=reasoning
        )
    except PermissionError as e:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail=str(e)) from e
    return {"status": "assigned"}


@router.post("/{deal_id}/add-contact")
async def deal_add_contact(
    deal_id: str,
    db: Annotated[GtmDB, Depends(get_db)],
    scope: Annotated[Scope, Depends(get_scope)],
    body: dict[str, Any] = Body(...),
) -> dict[str, str]:
    cid = (body.get("contact_id") or "").strip()
    reasoning = (body.get("reasoning") or "").strip()
    if not cid or not reasoning:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail="contact_id and reasoning are required",
        )
    try:
        await db.deals.add_contact(
            scope, deal_id, cid, reasoning=reasoning
        )
    except PermissionError as e:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail=str(e)) from e
    return {"status": "linked"}
