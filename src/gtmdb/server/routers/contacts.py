from __future__ import annotations

from typing import Annotated, Any

from fastapi import Body, Depends, HTTPException, status

from gtmdb.client import GtmDB
from gtmdb.scope import Scope
from gtmdb.server.deps import get_db, get_scope
from gtmdb.server.routers._crud import build_crud_router

router = build_crud_router("contacts", db_attr="contacts")


@router.post("/{contact_id}/assign-account")
async def assign_contact_account(
    contact_id: str,
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
        await db.contacts.assign_to_account(
            scope, contact_id, aid, reasoning=reasoning
        )
    except PermissionError as e:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail=str(e)) from e
    return {"status": "assigned"}
