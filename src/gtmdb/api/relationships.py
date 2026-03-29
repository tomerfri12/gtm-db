"""RelationshipsAPI -- generic CRUD for edges between CRM nodes."""

from __future__ import annotations

from typing import Any

from gtmdb.api._common import require_non_empty_str
from gtmdb.api.models import Relationship
from gtmdb.graph.adapter import GraphAdapter
from gtmdb.scope import Scope
from gtmdb.types import EdgeData


class RelationshipsAPI:
    """Generic relationship (edge) CRUD.

    Use this for arbitrary edge types. For common CRM relationships,
    prefer typed helpers (e.g. ``contacts.assign_to_account``,
    ``deals.assign_to_account``, ``leads.link_campaign``) when available.
    For a **lead → account** ``WORKS_AT`` link, use :meth:`create` here
    (only contacts have ``assign_to_account`` on the Contact API).
    """

    def __init__(self, graph: GraphAdapter) -> None:
        self._graph = graph

    async def create(
        self,
        scope: Scope,
        from_id: str,
        rel_type: str,
        to_id: str,
        *,
        reasoning: str,
        **props: Any,
    ) -> Relationship:
        """Create a relationship between two tenant-scoped nodes."""
        rs = require_non_empty_str(reasoning, "reasoning")
        pdict = dict(props)
        edge = EdgeData(rel_type, from_id, to_id, pdict, reasoning=rs)
        await self._graph.create_edge(scope, edge)
        merged = dict(pdict)
        merged["reasoning"] = rs
        return Relationship(
            type=rel_type,
            from_id=from_id,
            to_id=to_id,
            properties=merged,
        )

    async def list(
        self,
        scope: Scope,
        node_id: str,
        *,
        rel_type: str | None = None,
        direction: str = "both",
        limit: int = 100,
    ) -> list[Relationship]:
        """List relationships for a node.

        ``direction`` can be ``"out"``, ``"in"``, or ``"both"``
        (default). Results always report the true from/to direction
        regardless of the match direction.
        """
        if direction == "out":
            pattern = "(a)-[r]->(b)"
        elif direction == "in":
            pattern = "(a)<-[r]-(b)"
        else:
            pattern = "(a)-[r]-(b)"

        rel_filter = "AND type(r) = $rel_type " if rel_type else ""
        lim = max(1, min(int(limit), 500))

        query = (
            f"MATCH {pattern} "
            f"WHERE a.id = $node_id AND a.tenant_id = $tenant_id "
            f"AND b.tenant_id = $tenant_id "
            f"{rel_filter}"
            f"RETURN startNode(r).id AS from_id, endNode(r).id AS to_id, "
            f"type(r) AS rel_type, properties(r) AS rel_props "
            f"LIMIT {lim}"
        )

        params: dict[str, Any] = {"node_id": node_id}
        if rel_type:
            params["rel_type"] = rel_type

        records = await self._graph.execute(scope, query, params)
        results: list[Relationship] = []
        for rec in records:
            results.append(Relationship(
                type=rec["rel_type"],
                from_id=str(rec["from_id"]),
                to_id=str(rec["to_id"]),
                properties=dict(rec["rel_props"]) if rec["rel_props"] else {},
            ))
        return results

    async def delete(
        self,
        scope: Scope,
        from_id: str,
        rel_type: str,
        to_id: str,
    ) -> bool:
        """Delete a specific relationship. Returns ``True`` if it existed."""
        if not scope.can_write(rel_type):
            raise PermissionError(
                f"Token {scope.owner_id} cannot write edge {rel_type}"
            )

        query = (
            "MATCH (a {id: $from_id, tenant_id: $tenant_id})"
            "-[r]->"
            "(b {id: $to_id, tenant_id: $tenant_id}) "
            "WHERE type(r) = $rel_type "
            "DELETE r "
            "RETURN true AS deleted"
        )
        records = await self._graph.execute(
            scope,
            query,
            {"from_id": from_id, "to_id": to_id, "rel_type": rel_type},
        )
        return len(records) > 0
