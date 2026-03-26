"""GraphAdapter -- internal Neo4j adapter.

Manages the async driver lifecycle, enforces tenant isolation and
scope-based permission checks on every operation, and delegates to
the Cypher helpers in ``mutations`` and ``traversal``. Read paths apply
field masking and redaction (hint/hide) per ``Scope``.
"""

from __future__ import annotations

import uuid
from typing import Any

import neo4j

from crmdb.config import CrmdbSettings
from crmdb.graph import schema as _schema
from crmdb.graph.mutations import cypher_create_edge, cypher_create_node
from crmdb.graph import traversal as tr
from crmdb.scope import Scope
from crmdb.types import EdgeData, NodeData


class GraphAdapter:
    """Async Neo4j adapter with tenant isolation and scope checks."""

    def __init__(self, settings: CrmdbSettings) -> None:
        self._driver = neo4j.AsyncGraphDatabase.driver(
            settings.neo4j_uri,
            auth=(settings.neo4j_user, settings.neo4j_password),
        )

    async def close(self) -> None:
        await self._driver.close()

    async def verify_connectivity(self) -> None:
        await self._driver.verify_connectivity()

    async def bootstrap_schema(self) -> None:
        async with self._driver.session() as session:
            await _schema.bootstrap(session)

    def _finalize_read_node(
        self,
        scope: Scope,
        primary_label: str,
        raw_props: dict,
        *,
        edge_type: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Apply scope to a raw node row. Returns None to omit (hide redaction)."""
        nid = str(raw_props.get("id", ""))
        tenant = str(raw_props.get("tenant_id", scope.tenant_id))
        node_for_check = NodeData(primary_label, nid, tenant, dict(raw_props))

        if not scope.can_read(primary_label):
            redacted = scope.apply_redaction(primary_label, node_for_check)
            if redacted is None:
                return None
            out: dict[str, Any] = {"node": redacted}
            if edge_type is not None:
                out["edge_type"] = edge_type
            if extra:
                out.update(extra)
            return out

        if not scope.can_read(primary_label, {"id": nid}):
            redacted = scope.apply_redaction(primary_label, node_for_check)
            if redacted is None:
                return None
            out = {"node": redacted}
            if edge_type is not None:
                out["edge_type"] = edge_type
            if extra:
                out.update(extra)
            return out

        props = dict(raw_props)
        props.pop("id", None)
        props.pop("tenant_id", None)
        masked = scope.mask_fields(primary_label, props)
        out = {
            "node": NodeData(
                label=primary_label,
                id=nid,
                tenant_id=tenant,
                properties=masked,
            ),
        }
        if edge_type is not None:
            out["edge_type"] = edge_type
        if extra:
            out.update(extra)
        return out

    async def create_node(self, scope: Scope, node: NodeData) -> NodeData:
        if not scope.can_write(node.label):
            raise PermissionError(
                f"Token {scope.owner_id} cannot write {node.label}"
            )
        props = {
            "id": node.id or str(uuid.uuid4()),
            "tenant_id": scope.tenant_id,
            **node.properties,
        }
        if node.label != "Actor":
            props["created_by_actor_id"] = scope.owner_id
        async with self._driver.session() as session:
            result = await session.execute_write(
                cypher_create_node, node.label, props
            )
        return NodeData(
            label=node.label,
            id=result["id"],
            tenant_id=result["tenant_id"],
            properties={
                k: v
                for k, v in result.items()
                if k not in ("id", "tenant_id")
            },
        )

    async def create_edge(self, scope: Scope, edge: EdgeData) -> EdgeData:
        if not scope.can_write(edge.type):
            raise PermissionError(
                f"Token {scope.owner_id} cannot write edge {edge.type}"
            )
        async with self._driver.session() as session:
            rel_type = await session.execute_write(
                cypher_create_edge,
                edge.type,
                edge.from_id,
                edge.to_id,
                scope.tenant_id,
                edge.properties,
            )
        if rel_type is None:
            raise ValueError(
                f"Could not create edge {edge.type}: one or both nodes not found "
                f"(from={edge.from_id}, to={edge.to_id})"
            )
        return edge

    async def get_node(
        self, scope: Scope, label: str, node_id: str
    ) -> NodeData | None:
        if not scope.can_read(label):
            return None
        async with self._driver.session() as session:
            result = await session.execute_read(
                tr.cypher_get_node, label, node_id, scope.tenant_id
            )
        if result is None:
            return None
        props = dict(result["properties"])
        resolved_label = result["labels"][0] if result["labels"] else label
        finalized = self._finalize_read_node(scope, resolved_label, props)
        if finalized is None:
            return None
        return finalized["node"]

    async def get_neighbors(
        self,
        scope: Scope,
        node_id: str,
        edge_types: list[str] | None = None,
        direction: str = "both",
    ) -> list[dict]:
        async with self._driver.session() as session:
            records = await session.execute_read(
                tr.cypher_get_neighbors,
                node_id,
                scope.tenant_id,
                edge_types,
                direction,
            )
        results: list[dict] = []
        for rec in records:
            labels = rec.get("labels", [])
            primary_label = labels[0] if labels else "Unknown"
            raw_props = dict(rec["properties"])
            item = self._finalize_read_node(
                scope,
                primary_label,
                raw_props,
                edge_type=rec["edge_type"],
            )
            if item is not None:
                results.append(item)
        return results

    async def entity_360(
        self,
        scope: Scope,
        anchor_label: str,
        anchor_id: str,
        max_depth: int = 2,
    ) -> dict[str, Any]:
        """Center node (masked) plus connected nodes up to ``max_depth`` hops."""
        center = await self.get_node(scope, anchor_label, anchor_id)
        if center is None:
            return {"center": None, "connected": []}

        async with self._driver.session() as session:
            rows = await session.execute_read(
                tr.cypher_entity_360,
                anchor_id,
                scope.tenant_id,
                max_depth,
            )

        connected: list[dict] = []
        for row in rows:
            labels = row["labels"]
            primary_label = labels[0] if labels else "Unknown"
            raw_props = dict(row["properties"])
            item = self._finalize_read_node(
                scope,
                primary_label,
                raw_props,
                edge_type=row["edge_type"],
                extra={"depth": row["depth"]},
            )
            if item is not None:
                connected.append(item)

        return {"center": center, "connected": connected}

    async def timeline(
        self,
        scope: Scope,
        entity_id: str,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Activity nodes linked to an entity, newest first (masked)."""
        async with self._driver.session() as session:
            rows = await session.execute_read(
                tr.cypher_timeline,
                entity_id,
                scope.tenant_id,
                limit,
            )

        out: list[dict[str, Any]] = []
        for row in rows:
            labels = row["labels"]
            primary_label = labels[0] if labels else "Unknown"
            raw_props = dict(row["properties"])
            item = self._finalize_read_node(
                scope,
                primary_label,
                raw_props,
                edge_type=row["edge_type"],
                extra={"sort_key": row["sort_key"]},
            )
            if item is not None:
                out.append(item)
        return out

    async def pipeline(
        self,
        scope: Scope,
        stage: str | None = None,
        limit: int = 100,
    ) -> list[NodeData]:
        """Deals in scope, optionally filtered by stage."""
        if not scope.can_read("Deal"):
            return []

        async with self._driver.session() as session:
            rows = await session.execute_read(
                tr.cypher_pipeline,
                scope.tenant_id,
                stage,
                limit,
            )

        deals: list[NodeData] = []
        for row in rows:
            labels = row["labels"]
            primary_label = labels[0] if labels else "Deal"
            raw_props = dict(row["properties"])
            item = self._finalize_read_node(scope, primary_label, raw_props)
            if item is not None and "node" in item:
                deals.append(item["node"])
        return deals

    async def campaign_attribution(
        self,
        scope: Scope,
        deal_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Pairs of Campaign + Deal connected by INFLUENCED (both masked)."""
        if not scope.can_read("Campaign") and not scope.can_read("Deal"):
            return []

        async with self._driver.session() as session:
            rows = await session.execute_read(
                tr.cypher_campaign_attribution,
                scope.tenant_id,
                deal_id,
                limit,
            )

        pairs: list[dict[str, Any]] = []
        for row in rows:
            clabels = row["campaign_labels"]
            c_label = clabels[0] if clabels else "Campaign"
            c_props = dict(row["campaign_properties"])
            dlabels = row["deal_labels"]
            d_label = dlabels[0] if dlabels else "Deal"
            d_props = dict(row["deal_properties"])

            camp = self._finalize_read_node(scope, c_label, c_props)
            deal = self._finalize_read_node(scope, d_label, d_props)
            if camp is None and deal is None:
                continue
            pairs.append(
                {
                    "campaign": camp["node"] if camp else None,
                    "deal": deal["node"] if deal else None,
                }
            )
        return pairs

    async def path_finding(
        self,
        scope: Scope,
        from_id: str,
        to_id: str,
        max_hops: int = 15,
    ) -> dict[str, Any] | None:
        """Shortest path; each node is masked per scope."""
        async with self._driver.session() as session:
            raw = await session.execute_read(
                tr.cypher_path_finding,
                from_id,
                to_id,
                scope.tenant_id,
                max_hops,
            )

        if raw is None:
            return None

        nodes_out: list[NodeData | None] = []
        for n in raw["nodes"]:
            labels = n["labels"]
            pl = labels[0] if labels else "Unknown"
            props = dict(n["properties"])
            item = self._finalize_read_node(scope, pl, props)
            nodes_out.append(item["node"] if item else None)

        return {
            "nodes": nodes_out,
            "relationships": raw["relationships"],
        }

    async def search(
        self,
        scope: Scope,
        query: str,
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        """Full-text search; results masked per scope."""
        async with self._driver.session() as session:
            rows = await session.execute_read(
                tr.cypher_fulltext_search,
                query,
                scope.tenant_id,
                limit,
            )

        hits: list[dict[str, Any]] = []
        for row in rows:
            labels = row["labels"]
            primary_label = labels[0] if labels else "Unknown"
            raw_props = dict(row["properties"])
            item = self._finalize_read_node(
                scope,
                primary_label,
                raw_props,
                extra={"score": row["score"]},
            )
            if item is not None:
                hits.append(item)
        return hits

    async def execute(
        self, scope: Scope, query: str, params: dict | None = None
    ) -> list[dict]:
        """Run raw Cypher with tenant_id injected into params."""
        p = dict(params or {})
        p["tenant_id"] = scope.tenant_id
        async with self._driver.session() as session:
            result = await session.run(query, p)
            return [dict(record) async for record in result]
