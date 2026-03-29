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

from gtmdb.config import GtmdbSettings
from gtmdb.graph import schema as _schema
from gtmdb.graph.mutations import cypher_create_edge, cypher_create_node
from gtmdb.graph import traversal as tr
from gtmdb.scope import Scope
from gtmdb.types import EdgeData, NodeData


def _effective_neo4j_uri(uri: str, *, force_direct_bolt: bool) -> str:
    """Map cluster routing URIs to direct Bolt (same host), e.g. for Aura from Railway."""
    u = uri.strip()
    if not force_direct_bolt:
        return u
    for neo4j_prefix, bolt_prefix in (
        ("neo4j+s://", "bolt+s://"),
        ("neo4j+ssc://", "bolt+ssc://"),
        ("neo4j://", "bolt://"),
    ):
        if u.startswith(neo4j_prefix):
            return bolt_prefix + u.removeprefix(neo4j_prefix)
    return u


class GraphAdapter:
    """Async Neo4j adapter with tenant isolation and scope checks."""

    def __init__(self, settings: GtmdbSettings) -> None:
        uri = _effective_neo4j_uri(
            settings.neo4j_uri,
            force_direct_bolt=settings.neo4j_force_direct_bolt,
        )
        drv_kw: dict[str, object] = {}
        if settings.neo4j_connection_timeout is not None:
            drv_kw["connection_timeout"] = settings.neo4j_connection_timeout
        if settings.neo4j_connection_acquisition_timeout is not None:
            drv_kw["connection_acquisition_timeout"] = (
                settings.neo4j_connection_acquisition_timeout
            )
        self._driver = neo4j.AsyncGraphDatabase.driver(
            uri,
            auth=(settings.neo4j_user, settings.neo4j_password),
            **drv_kw,
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
            cid = (props.get("created_by_actor_id") or "").strip()
            if not cid:
                raise ValueError(
                    "created_by_actor_id is required for non-Actor nodes "
                    "(set on NodeData.properties before create_node)"
                )
            props["created_by_actor_id"] = cid
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
        merged_props = dict(edge.properties)
        if edge.reasoning is not None:
            rs = edge.reasoning.strip()
            if rs:
                merged_props["reasoning"] = rs
        async with self._driver.session() as session:
            rel_type = await session.execute_write(
                cypher_create_edge,
                edge.type,
                edge.from_id,
                edge.to_id,
                scope.tenant_id,
                merged_props,
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
            rel_props = dict(rec.get("rel_props") or {})
            extra = {"edge_properties": rel_props} if rel_props else {}
            item = self._finalize_read_node(
                scope,
                primary_label,
                raw_props,
                edge_type=rec["edge_type"],
                extra=extra or None,
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
            rel_props = dict(row.get("rel_props") or {})
            extra: dict[str, Any] = {"depth": row["depth"]}
            if rel_props:
                extra["edge_properties"] = rel_props
            item = self._finalize_read_node(
                scope,
                primary_label,
                raw_props,
                edge_type=row["edge_type"],
                extra=extra,
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

    async def explore_subgraph(
        self,
        scope: Scope,
        center_id: str,
        max_depth: int = 1,
        *,
        nodes_per_type_cap: int = 10,
        mode: str = "compact",
    ) -> dict[str, Any]:
        """BFS subgraph with per-label node cap (for API ``/explore``).

        ``mode="compact"`` (default) returns node IDs grouped by label.
        ``mode="full"`` returns full node properties (heavier).
        """
        d = max(1, min(int(max_depth), 5))
        cap = max(1, min(int(nodes_per_type_cap), 50))

        async with self._driver.session() as session:
            bundle = await session.execute_read(
                tr.cypher_explore_subgraph_bundle,
                center_id,
                scope.tenant_id,
                d,
            )
        node_rows: list[dict[str, Any]] = bundle["node_rows"]
        if not node_rows:
            return {"nodes": {}, "edges": [], "truncated": {}}

        label_by_id: dict[str, str] = {
            nr["id"]: (nr["labels"][0] if nr["labels"] else "Unknown")
            for nr in node_rows
        }
        all_ids: set[str] = {nr["id"] for nr in node_rows}

        edges_raw: list[dict[str, Any]] = []
        for row in bundle["edges"]:
            rp = dict(row["rel_props"])
            edge_obj: dict[str, Any] = {
                "from": row["from_id"],
                "to": row["to_id"],
                "type": row["rel_type"],
            }
            edge_obj.update(rp)
            edges_raw.append(edge_obj)

        props_by_id: dict[str, dict[str, Any]] = {
            nr["id"]: dict(nr["props"]) for nr in node_rows
        }

        # Totals per label
        total_by_label: dict[str, int] = {}
        ids_by_label: dict[str, list[str]] = {}
        for nid in all_ids:
            lb = label_by_id.get(nid, "Unknown")
            total_by_label[lb] = total_by_label.get(lb, 0) + 1
            ids_by_label.setdefault(lb, []).append(nid)

        included_ids: set[str] = set()
        truncated: dict[str, dict[str, int]] = {}
        for lb, ids in ids_by_label.items():
            ids_sorted = sorted(ids)
            total = len(ids_sorted)
            if total > cap:
                truncated[lb] = {"returned": cap, "total": total}
                for i in range(cap):
                    included_ids.add(ids_sorted[i])
            else:
                for x in ids_sorted:
                    included_ids.add(x)

        if mode == "full":
            nodes_out: dict[str, Any] = {}
            for nid in sorted(included_ids):
                raw_props = props_by_id.get(nid)
                if raw_props is None:
                    continue
                pl = label_by_id.get(nid, "Unknown")
                item = self._finalize_read_node(scope, pl, raw_props)
                if item is None:
                    continue
                node = item["node"]
                payload = {"type": pl, **node.properties}
                payload["id"] = nid
                nodes_out[nid] = payload

            edges_out = [
                e
                for e in edges_raw
                if e["from"] in nodes_out and e["to"] in nodes_out
            ]
            return {
                "nodes": nodes_out,
                "edges": edges_out,
                "truncated": truncated,
            }

        # compact mode: IDs grouped by label, lightweight edges
        compact_nodes: dict[str, list[str]] = {}
        for nid in sorted(included_ids):
            lb = label_by_id.get(nid, "Unknown")
            compact_nodes.setdefault(lb, []).append(nid)

        compact_edges = [
            {"from": e["from"], "to": e["to"], "type": e["type"]}
            for e in edges_raw
            if e["from"] in included_ids and e["to"] in included_ids
        ]
        return {
            "nodes": compact_nodes,
            "edges": compact_edges,
            "truncated": truncated,
        }

    async def execute(
        self, scope: Scope, query: str, params: dict | None = None
    ) -> list[dict]:
        """Run raw Cypher with tenant_id injected into params."""
        p = dict(params or {})
        p["tenant_id"] = scope.tenant_id
        async with self._driver.session() as session:
            result = await session.run(query, p)
            return [dict(record) async for record in result]
