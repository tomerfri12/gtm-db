from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from gtmdb.client import GtmDB
from gtmdb.scope import Scope
from gtmdb.server.deps import get_db, get_scope

router = APIRouter(prefix="/entities", tags=["explore"])


@router.get("/{entity_id}/explore")
async def explore_entity(
    entity_id: str,
    request: Request,
    db: Annotated[GtmDB, Depends(get_db)],
    scope: Annotated[Scope, Depends(get_scope)],
    depth: int | None = Query(default=None),
    mode: str = Query(default="compact", pattern="^(compact|full)$"),
) -> dict:
    srv = request.app.state.server_settings
    d = depth if depth is not None else srv.explore_default_depth
    d = max(1, min(d, srv.explore_max_depth, 5))
    try:
        return await db.explore_subgraph(
            scope,
            entity_id,
            max_depth=d,
            nodes_per_type_cap=srv.explore_nodes_per_type,
            mode=mode,
        )
    except PermissionError as e:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail=str(e)) from e
