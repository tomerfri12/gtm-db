"""Cypher helpers for creating nodes and edges."""

from __future__ import annotations

from neo4j import AsyncManagedTransaction


async def cypher_create_node(
    tx: AsyncManagedTransaction, label: str, props: dict
) -> dict:
    """Create a single node and return its properties."""
    query = f"CREATE (n:{label} $props) RETURN properties(n) AS props"
    result = await tx.run(query, props=props)
    record = await result.single()
    return dict(record["props"]) if record else {}


async def cypher_create_edge(
    tx: AsyncManagedTransaction,
    edge_type: str,
    from_id: str,
    to_id: str,
    tenant_id: str,
    props: dict | None = None,
) -> str | None:
    """Create an edge between two tenant-scoped nodes. Returns the edge type on success.

    Nodes are matched by ``id`` + ``tenant_id`` without label filtering
    since node ids are globally unique.
    """
    rel_props = props or {}
    query = (
        "MATCH (a {id: $from_id, tenant_id: $tid}) "
        "MATCH (b {id: $to_id, tenant_id: $tid}) "
        f"CREATE (a)-[r:{edge_type}]->(b) "
        "SET r += $props "
        "RETURN type(r) AS rel_type"
    )
    result = await tx.run(
        query,
        from_id=from_id,
        to_id=to_id,
        tid=tenant_id,
        props=rel_props,
    )
    record = await result.single()
    return record["rel_type"] if record else None
