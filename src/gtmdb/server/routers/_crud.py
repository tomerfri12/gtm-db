"""Generic CRUD routes for a typed EntityAPI."""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, status

from gtmdb.api._base import EntityAPI
from gtmdb.client import GtmDB
from gtmdb.scope import Scope
from gtmdb.server.deps import get_db, get_scope
from gtmdb.server.util import entity_as_dict


def build_crud_router(
    segment: str,
    *,
    db_attr: str,
) -> APIRouter:
    """``segment`` is URL segment (``leads``); ``db_attr`` is ``GtmDB`` property name."""
    router = APIRouter(prefix=f"/{segment}", tags=[segment])

    def api_for(db: GtmDB) -> EntityAPI[Any]:
        return getattr(db, db_attr)

    @router.post("")
    async def create_entity(
        db: Annotated[GtmDB, Depends(get_db)],
        scope: Annotated[Scope, Depends(get_scope)],
        body: dict[str, Any] = Body(default_factory=dict),
    ) -> dict[str, Any]:
        api = api_for(db)
        data = dict(body)
        actor_id = (data.pop("actor_id", None) or scope.owner_id or "").strip()
        if not actor_id:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                detail="actor_id required (or use a key with owner_id)",
            )
        reasoning = data.pop("reasoning", None)
        clean = {k: v for k, v in data.items() if k in api._domain_fields}
        try:
            ent = await api.create(
                scope,
                actor_id=actor_id,
                reasoning=reasoning,
                **clean,
            )
        except PermissionError as e:
            raise HTTPException(status.HTTP_403_FORBIDDEN, detail=str(e)) from e
        except ValueError as e:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
        return entity_as_dict(ent)

    @router.get("/{entity_id}")
    async def get_entity(
        entity_id: str,
        db: Annotated[GtmDB, Depends(get_db)],
        scope: Annotated[Scope, Depends(get_scope)],
    ) -> dict[str, Any]:
        api = api_for(db)
        try:
            ent = await api.get(scope, entity_id)
        except PermissionError as e:
            raise HTTPException(status.HTTP_403_FORBIDDEN, detail=str(e)) from e
        if ent is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Not found")
        return entity_as_dict(ent)

    @router.get("")
    async def list_entities(
        request: Request,
        db: Annotated[GtmDB, Depends(get_db)],
        scope: Annotated[Scope, Depends(get_scope)],
        limit: int = Query(default=50, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ) -> list[dict[str, Any]]:
        api = api_for(db)
        qp = dict(request.query_params)
        qp.pop("limit", None)
        qp.pop("offset", None)
        filters = {k: v for k, v in qp.items() if k in api._domain_fields}
        try:
            rows = await api.list(scope, limit=limit, offset=offset, **filters)
        except PermissionError as e:
            raise HTTPException(status.HTTP_403_FORBIDDEN, detail=str(e)) from e
        return [entity_as_dict(r) for r in rows]

    @router.patch("/{entity_id}")
    async def patch_entity(
        entity_id: str,
        db: Annotated[GtmDB, Depends(get_db)],
        scope: Annotated[Scope, Depends(get_scope)],
        body: dict[str, Any] = Body(default_factory=dict),
    ) -> dict[str, Any]:
        api = api_for(db)
        data = dict(body)
        actor_id = (data.pop("actor_id", None) or scope.owner_id or "").strip()
        if not actor_id:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                detail="actor_id required (or use a key with owner_id)",
            )
        reasoning = data.pop("reasoning", None)
        clean = {k: v for k, v in data.items() if k in api._domain_fields}
        try:
            ent = await api.update(
                scope,
                entity_id,
                actor_id=actor_id,
                reasoning=reasoning,
                **clean,
            )
        except PermissionError as e:
            raise HTTPException(status.HTTP_403_FORBIDDEN, detail=str(e)) from e
        if ent is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Not found")
        return entity_as_dict(ent)

    @router.delete("/{entity_id}")
    async def delete_entity(
        entity_id: str,
        db: Annotated[GtmDB, Depends(get_db)],
        scope: Annotated[Scope, Depends(get_scope)],
    ) -> dict[str, bool]:
        api = api_for(db)
        try:
            ok = await api.delete(scope, entity_id)
        except PermissionError as e:
            raise HTTPException(status.HTTP_403_FORBIDDEN, detail=str(e)) from e
        if not ok:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Not found")
        return {"deleted": True}

    return router
