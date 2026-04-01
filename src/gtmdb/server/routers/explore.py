from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from neo4j.exceptions import DriverError, Neo4jError

from gtmdb.client import GtmDB
from gtmdb.scope import Scope
from gtmdb.server.deps import get_db, get_scope
from gtmdb.server.explore_errors import (
    explore_failure_detail,
    is_likely_neo4j_timeout,
)

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
            read_transaction_timeout_s=srv.explore_transaction_timeout_s,
        )
    except PermissionError as e:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail=str(e)) from e
    except (Neo4jError, DriverError) as e:
        if is_likely_neo4j_timeout(e):
            raise HTTPException(
                status.HTTP_504_GATEWAY_TIMEOUT,
                detail=explore_failure_detail(
                    error="explore_timeout",
                    message="Explore subgraph query exceeded the time limit or was terminated.",
                ),
            ) from e
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=explore_failure_detail(
                error="explore_neo4j_error",
                message=str(e) or "Neo4j error while running explore.",
            ),
        ) from e
