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
from gtmdb.server.explore_labels import (
    normalize_labels_for_cypher,
    parse_explore_label_csv,
)

router = APIRouter(prefix="/entities", tags=["explore"])

_EXPLORE_LABEL_CONFLICT_DETAIL: dict[str, object] = {
    "error": "explore_label_filter_conflict",
    "message": "Use only one of include_labels or exclude_labels (comma-separated).",
    "suggestions": [
        "Example: include_labels=Visitor,Lead to traverse only through those node types.",
        "Example: exclude_labels=Channel,Product to skip heavy types.",
        "The start node is always included; filters apply to neighbors added by BFS.",
    ],
}


@router.get("/{entity_id}/explore")
async def explore_entity(
    entity_id: str,
    request: Request,
    db: Annotated[GtmDB, Depends(get_db)],
    scope: Annotated[Scope, Depends(get_scope)],
    depth: int | None = Query(default=None),
    mode: str = Query(default="compact", pattern="^(compact|full)$"),
    include_labels: str | None = Query(
        default=None,
        description="Comma-separated labels: only step into neighbors with any of these labels (case-insensitive).",
    ),
    exclude_labels: str | None = Query(
        default=None,
        description="Comma-separated labels: do not add neighbors that have any of these labels.",
    ),
) -> dict:
    srv = request.app.state.server_settings
    d = depth if depth is not None else srv.explore_default_depth
    d = max(1, min(d, srv.explore_max_depth, 5))
    inc_raw = parse_explore_label_csv(include_labels)
    exc_raw = parse_explore_label_csv(exclude_labels)
    if inc_raw and exc_raw:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail=_EXPLORE_LABEL_CONFLICT_DETAIL,
        )
    inc_lo = normalize_labels_for_cypher(inc_raw) if inc_raw else None
    exc_lo = normalize_labels_for_cypher(exc_raw) if exc_raw else None
    if inc_lo is not None and len(inc_lo) == 0:
        inc_lo = None
    if exc_lo is not None and len(exc_lo) == 0:
        exc_lo = None
    filter_meta: dict[str, object] | None = None
    if inc_raw:
        filter_meta = {"mode": "include", "labels": list(inc_raw)}
    elif exc_raw:
        filter_meta = {"mode": "exclude", "labels": list(exc_raw)}
    try:
        return await db.explore_subgraph(
            scope,
            entity_id,
            max_depth=d,
            nodes_per_type_cap=srv.explore_nodes_per_type,
            mode=mode,
            read_transaction_timeout_s=srv.explore_transaction_timeout_s,
            max_discovered_nodes=srv.explore_max_discovered_nodes,
            traverse_include_labels_lower=inc_lo,
            traverse_exclude_labels_lower=exc_lo,
            traverse_filter_meta=filter_meta,
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
