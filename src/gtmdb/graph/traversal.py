"""Cypher helpers for reading nodes, traversals, and graph search."""

from __future__ import annotations

from typing import Any

from neo4j import AsyncManagedTransaction


async def cypher_get_node(
    tx: AsyncManagedTransaction, label: str, node_id: str, tenant_id: str
) -> dict | None:
    """Fetch a single node by label + id within a tenant."""
    query = (
        f"MATCH (n:{label} {{id: $id, tenant_id: $tid}}) "
        "RETURN properties(n) AS props, labels(n) AS labels"
    )
    result = await tx.run(query, id=node_id, tid=tenant_id)
    record = await result.single()
    if not record:
        return None
    return {"properties": dict(record["props"]), "labels": list(record["labels"])}


async def cypher_get_neighbors(
    tx: AsyncManagedTransaction,
    node_id: str,
    tenant_id: str,
    edge_types: list[str] | None = None,
    direction: str = "both",
) -> list[dict]:
    """Get all nodes connected to a given node within the same tenant."""
    rel_filter = ""
    if edge_types:
        types_str = "|".join(edge_types)
        rel_filter = f":{types_str}"

    if direction == "out":
        pattern = f"(a)-[r{rel_filter}]->(b)"
    elif direction == "in":
        pattern = f"(a)<-[r{rel_filter}]-(b)"
    else:
        pattern = f"(a)-[r{rel_filter}]-(b)"

    query = (
        f"MATCH {pattern} "
        "WHERE a.id = $id AND a.tenant_id = $tid AND b.tenant_id = $tid "
        "RETURN properties(b) AS props, labels(b) AS labels, type(r) AS rel_type, "
        "properties(r) AS rel_props"
    )
    result = await tx.run(query, id=node_id, tid=tenant_id)
    records = [record async for record in result]
    return [
        {
            "properties": dict(r["props"]),
            "labels": list(r["labels"]),
            "edge_type": r["rel_type"],
            "rel_props": dict(r["rel_props"]) if r.get("rel_props") else {},
        }
        for r in records
    ]


async def cypher_entity_360(
    tx: AsyncManagedTransaction,
    anchor_id: str,
    tenant_id: str,
    max_depth: int = 2,
) -> list[dict]:
    """Nodes reachable from anchor within ``max_depth`` hops (same tenant)."""
    d = max(1, min(int(max_depth), 5))
    query = (
        f"MATCH path = (a {{id: $id, tenant_id: $tid}})-[*1..{d}]-(n) "
        "WHERE n.tenant_id = $tid "
        "AND all(x IN nodes(path) WHERE x.tenant_id = $tid) "
        "AND n <> a "
        "WITH n, path, relationships(path) AS rels "
        "RETURN DISTINCT properties(n) AS props, labels(n) AS labels, "
        "length(path) AS depth, type(rels[-1]) AS rel_type, "
        "properties(rels[-1]) AS rel_props "
        "LIMIT 300"
    )
    result = await tx.run(query, id=anchor_id, tid=tenant_id)
    rows = [record async for record in result]
    return [
        {
            "properties": dict(r["props"]),
            "labels": list(r["labels"]),
            "depth": int(r["depth"]),
            "edge_type": r["rel_type"],
            "rel_props": dict(r["rel_props"]) if r.get("rel_props") else {},
        }
        for r in rows
    ]


async def cypher_timeline(
    tx: AsyncManagedTransaction,
    entity_id: str,
    tenant_id: str,
    limit: int = 50,
) -> list[dict]:
    """Activity-like nodes linked to an entity, newest first."""
    lim = max(1, min(int(limit), 200))
    query = (
        "MATCH (e {id: $eid, tenant_id: $tid})"
        "-[r:HAS_COMMUNICATION_EVENT]-(act) "
        "WHERE act.tenant_id = $tid "
        "WITH act, type(r) AS rel_type, "
        "coalesce(act.occurred_at, act.created_at, act.timestamp, act.posted_at) AS sort_key "
        f"RETURN properties(act) AS props, labels(act) AS labels, rel_type, sort_key "
        f"ORDER BY sort_key DESC "
        f"LIMIT {lim}"
    )
    result = await tx.run(query, eid=entity_id, tid=tenant_id)
    rows = [record async for record in result]
    return [
        {
            "properties": dict(r["props"]),
            "labels": list(r["labels"]),
            "edge_type": r["rel_type"],
            "sort_key": r["sort_key"],
        }
        for r in rows
    ]


async def cypher_pipeline(
    tx: AsyncManagedTransaction,
    tenant_id: str,
    stage: str | None = None,
    limit: int = 100,
) -> list[dict]:
    """Deals in the tenant, optionally filtered by ``stage``."""
    lim = max(1, min(int(limit), 500))
    if stage:
        query = (
            "MATCH (d:Deal {tenant_id: $tid, stage: $stage}) "
            "RETURN properties(d) AS props, labels(d) AS labels "
            f"ORDER BY coalesce(d.amount, 0) DESC LIMIT {lim}"
        )
        result = await tx.run(query, tid=tenant_id, stage=stage)
    else:
        query = (
            "MATCH (d:Deal {tenant_id: $tid}) "
            "RETURN properties(d) AS props, labels(d) AS labels "
            f"ORDER BY coalesce(d.amount, 0) DESC LIMIT {lim}"
        )
        result = await tx.run(query, tid=tenant_id)
    rows = [record async for record in result]
    return [
        {"properties": dict(r["props"]), "labels": list(r["labels"])}
        for r in rows
    ]


async def cypher_campaign_attribution(
    tx: AsyncManagedTransaction,
    tenant_id: str,
    deal_id: str | None = None,
    limit: int = 100,
) -> list[dict]:
    """Campaigns that influenced deals (``INFLUENCED``), optional deal filter."""
    lim = max(1, min(int(limit), 300))
    if deal_id:
        query = (
            "MATCH (c:Campaign {tenant_id: $tid})-[:INFLUENCED]->"
            "(d:Deal {tenant_id: $tid, id: $deal_id}) "
            "RETURN properties(c) AS cprops, labels(c) AS clabels, "
            "properties(d) AS dprops, labels(d) AS dlabels "
            f"LIMIT {lim}"
        )
        result = await tx.run(query, tid=tenant_id, deal_id=deal_id)
    else:
        query = (
            "MATCH (c:Campaign {tenant_id: $tid})-[:INFLUENCED]->"
            "(d:Deal {tenant_id: $tid}) "
            "RETURN properties(c) AS cprops, labels(c) AS clabels, "
            "properties(d) AS dprops, labels(d) AS dlabels "
            f"LIMIT {lim}"
        )
        result = await tx.run(query, tid=tenant_id)
    rows = [record async for record in result]
    return [
        {
            "campaign_properties": dict(r["cprops"]),
            "campaign_labels": list(r["clabels"]),
            "deal_properties": dict(r["dprops"]),
            "deal_labels": list(r["dlabels"]),
        }
        for r in rows
    ]


async def cypher_path_finding(
    tx: AsyncManagedTransaction,
    from_id: str,
    to_id: str,
    tenant_id: str,
    max_hops: int = 15,
) -> dict | None:
    """Shortest undirected path between two nodes (same tenant)."""
    mh = max(1, min(int(max_hops), 25))
    query = (
        "MATCH (a {id: $from_id, tenant_id: $tid}), (b {id: $to_id, tenant_id: $tid}) "
        f"MATCH p = shortestPath((a)-[*..{mh}]-(b)) "
        "WHERE all(x IN nodes(p) WHERE x.tenant_id = $tid) "
        "RETURN p"
    )
    result = await tx.run(query, from_id=from_id, to_id=to_id, tid=tenant_id)
    record = await result.single()
    if not record:
        return None
    path = record["p"]
    nodes_out = []
    rels_out = []
    rel_list = list(path.relationships)
    for i, node in enumerate(path.nodes):
        nodes_out.append(
            {
                "properties": dict(node.items()),
                "labels": list(node.labels),
            }
        )
        if i < len(rel_list):
            rel = rel_list[i]
            rels_out.append(
                {"type": rel.type, "properties": dict(rel.items())}
            )
    return {"nodes": nodes_out, "relationships": rels_out}


async def cypher_fulltext_search(
    tx: AsyncManagedTransaction,
    search_query: str,
    tenant_id: str,
    limit: int = 25,
) -> list[dict]:
    """Full-text search on ``entity_search`` index, tenant-scoped."""
    lim = max(1, min(int(limit), 100))
    query = (
        "CALL db.index.fulltext.queryNodes('entity_search', $q) "
        "YIELD node, score "
        "WHERE node.tenant_id = $tid "
        f"RETURN properties(node) AS props, labels(node) AS labels, score "
        f"ORDER BY score DESC LIMIT {lim}"
    )
    result = await tx.run(query, q=search_query, tid=tenant_id)
    rows = [record async for record in result]
    return [
        {
            "properties": dict(r["props"]),
            "labels": list(r["labels"]),
            "score": float(r["score"]),
        }
        for r in rows
    ]


async def cypher_find_node_by_id(
    tx: AsyncManagedTransaction, node_id: str, tenant_id: str
) -> dict | None:
    """Any node with id + tenant."""
    query = (
        "MATCH (n {id: $id, tenant_id: $tid}) "
        "RETURN properties(n) AS props, labels(n) AS labels LIMIT 1"
    )
    result = await tx.run(query, id=node_id, tid=tenant_id)
    record = await result.single()
    if not record:
        return None
    return {
        "properties": dict(record["props"]),
        "labels": list(record["labels"]),
    }


async def cypher_incident_edges(
    tx: AsyncManagedTransaction, node_id: str, tenant_id: str
) -> list[dict]:
    """Directed edges touching ``node_id`` (both directions), same tenant."""
    query = (
        "MATCH (a {id: $id, tenant_id: $tid})-[r]-(b {tenant_id: $tid}) "
        "RETURN startNode(r).id AS from_id, endNode(r).id AS to_id, "
        "type(r) AS rel_type, properties(r) AS rel_props"
    )
    result = await tx.run(query, id=node_id, tid=tenant_id)
    rows = [record async for record in result]
    return [
        {
            "from_id": str(r["from_id"]),
            "to_id": str(r["to_id"]),
            "rel_type": str(r["rel_type"]),
            "rel_props": dict(r["rel_props"] or {}),
        }
        for r in rows
    ]


def _map_row(m: Any) -> dict[str, Any]:
    """Normalize a Neo4j map / driver mapping to a plain dict."""
    if m is None:
        return {}
    if isinstance(m, dict):
        return dict(m)
    return dict(m)


async def cypher_explore_subgraph_bundle(
    tx: AsyncManagedTransaction, center_id: str, tenant_id: str, max_depth: int
) -> dict[str, Any]:
    """Nodes within ``max_depth`` hops + edges (BFS, no variable-length paths).

    Nodes: shortest undirected hop distance from center in ``[0, max_depth]``.
    Implemented as frontier expansion to avoid path explosion from
    ``-[*0..d]-`` on hub nodes.

    Edges: same as before: directed rows with both endpoints in the node set
    and at least one endpoint with ``dist < max_depth``.
    """
    d = max(1, min(int(max_depth), 5))
    q0 = (
        "MATCH (center {id: $id, tenant_id: $tid}) "
        "RETURN center.id AS id, labels(center) AS labels, "
        "properties(center) AS props"
    )
    r0 = await tx.run(q0, id=center_id, tid=tenant_id)
    rec0 = await r0.single()
    if not rec0:
        return {"node_rows": [], "edges": []}

    dist_by_id: dict[str, int] = {center_id: 0}
    labels_by_id: dict[str, list[str]] = {}
    props_by_id: dict[str, dict[str, Any]] = {}

    def _ingest_row(
        rid: Any, labels: Any, props: Any, dist: int,
    ) -> None:
        sid = str(rid)
        dist_by_id[sid] = dist
        lb = labels
        if lb is not None and not isinstance(lb, list):
            lb = list(lb)
        labels_by_id[sid] = list(lb or [])
        props_by_id[sid] = dict(props or {})

    _ingest_row(rec0["id"], rec0["labels"], rec0["props"], 0)

    frontier: list[str] = [center_id]
    q_expand = (
        "UNWIND $frontier AS eid "
        "MATCH (a {id: eid, tenant_id: $tid})-[r]-(b {tenant_id: $tid}) "
        "WHERE NOT b.id IN $seen "
        "RETURN DISTINCT b.id AS id, labels(b) AS labels, properties(b) AS props"
    )

    for hop in range(1, d + 1):
        if not frontier:
            break
        seen = list(dist_by_id.keys())
        result = await tx.run(
            q_expand, frontier=frontier, seen=seen, tid=tenant_id,
        )
        next_frontier: list[str] = []
        async for row in result:
            bid = str(row["id"])
            if bid in dist_by_id:
                continue
            _ingest_row(row["id"], row["labels"], row["props"], hop)
            next_frontier.append(bid)
        frontier = next_frontier

    node_rows: list[dict[str, Any]] = []
    for nid, dist in dist_by_id.items():
        node_rows.append(
            {
                "id": nid,
                "labels": labels_by_id[nid],
                "props": props_by_id[nid],
                "dist": dist,
            }
        )

    # Expand only from nodes with dist < d (same as BFS frontiers). O(total degree)
    # instead of O(n^2) pair UNWIND — critical for hub nodes.
    allowed_ids = [nr["id"] for nr in node_rows]
    expandable_ids = [nr["id"] for nr in node_rows if nr["dist"] < d]
    q2 = (
        "UNWIND $expandable AS eid "
        "MATCH (a {id: eid, tenant_id: $tid})-[r]-(b {tenant_id: $tid}) "
        "WHERE b.id IN $allowed_ids "
        "WITH startNode(r).id AS fid, endNode(r).id AS to_nid, type(r) AS rtype, "
        "properties(r) AS rprops "
        "RETURN collect(DISTINCT {from_id: fid, to_id: to_nid, rel_type: rtype, "
        "rel_props: rprops}) AS edges"
    )
    result2 = await tx.run(
        q2, expandable=expandable_ids, allowed_ids=allowed_ids, tid=tenant_id
    )
    rec2 = await result2.single()
    edges_out: list[dict[str, Any]] = []
    if rec2 and rec2.get("edges"):
        for e in rec2["edges"]:
            er = _map_row(e)
            edges_out.append(
                {
                    "from_id": str(er["from_id"]),
                    "to_id": str(er["to_id"]),
                    "rel_type": str(er["rel_type"]),
                    "rel_props": dict(er.get("rel_props") or {}),
                }
            )

    return {"node_rows": node_rows, "edges": edges_out}
