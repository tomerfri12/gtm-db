"""OlapSyncLayer — fires ClickHouse events after every Neo4j write.

This is the glue between the graph write path and the OLAP store.
``GraphAdapter`` holds an optional reference to ``OlapSyncLayer`` and calls
``on_node_created`` / ``on_edge_created`` after each successful write.

Design
------
* **Log-and-continue**: any ClickHouse failure is caught, logged, and
  silently dropped.  Neo4j remains the source of truth; ClickHouse can
  always be repopulated via ``gtmdb materialize``.
* **Awaited, not fire-and-forget**: keeps the code simple and makes tests
  deterministic.  The 2-5 ms overhead per write is acceptable.
* **Edge enrichment via static label map**: avoids an extra Neo4j round-trip
  to resolve node labels on edge creation.  The map encodes the known
  graph schema.

Only edges listed in ``EDGE_EVENT_DEFAULTS`` emit OLAP events (attribution
and lifecycle edges).  Audit edges (``CREATED_BY``, ``UPDATED_BY``) are
deliberately skipped.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from gtmdb.graph.adapter import GraphAdapter
    from gtmdb.scope import Scope
    from gtmdb.types import EdgeData, NodeData

from gtmdb.olap.client import ClickHouseClient
from gtmdb.olap.enrichment import enrich_edge, enrich_node
from gtmdb.olap.events import EDGE_EVENT_DEFAULTS

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Edge schema map (edge_type → from_label, to_label)
# Avoids an extra Neo4j query to resolve labels on edge creation.
# ---------------------------------------------------------------------------

_EDGE_LABELS: dict[str, tuple[str, str]] = {
    "SOURCED_FROM":           ("Lead",              "Campaign"),
    "CONVERTED_TO":           ("Lead",              "Contact"),
    "WORKS_AT":               ("Contact",           "Account"),
    "INFLUENCED":             ("Campaign",          "Deal"),
    "HAS_SUBSCRIPTION_EVENT": ("ProductAccount",    "SubscriptionEvent"),
    "SIGNED_UP_AS":           ("Lead",              "ProductAccount"),
    "FOR_PRODUCT":            ("SubscriptionEvent", "Product"),
    "TOUCHED":                ("Visitor",           "Campaign"),
}

# Labels whose nodes we skip emitting events for (they're infrastructure,
# not GTM domain events).
_SKIP_LABELS = frozenset({"Actor", "Score"})


class OlapSyncLayer:
    """Receives post-write callbacks and persists enriched events to ClickHouse."""

    def __init__(self, ch: ClickHouseClient) -> None:
        self._ch = ch

    async def on_node_created(
        self,
        graph: GraphAdapter,
        scope: Scope,
        node: NodeData,
    ) -> None:
        """Enrich and emit an event for a newly created node."""
        if node.label in _SKIP_LABELS:
            return
        try:
            event = await enrich_node(
                graph,
                scope,
                node_id=node.id,
                label=node.label,
                actor_id=str(node.properties.get("created_by_actor_id", "")),
            )
            await self._ch.insert_events([event.to_row()])
            log.debug("OLAP sync: emitted %s for %s/%s", event.event_type, node.label, node.id)
        except Exception:
            log.warning(
                "OLAP sync failed for node %s/%s — event dropped",
                node.label, node.id, exc_info=True,
            )

    async def on_edge_created(
        self,
        graph: GraphAdapter,
        scope: Scope,
        edge: EdgeData,
    ) -> None:
        """Enrich and emit an event for a newly created relationship."""
        if edge.type not in EDGE_EVENT_DEFAULTS:
            return

        labels = _EDGE_LABELS.get(edge.type)
        if labels is None:
            return

        from_label, to_label = labels
        try:
            event = await enrich_edge(
                graph,
                scope,
                from_id=edge.from_id,
                from_label=from_label,
                to_id=edge.to_id,
                to_label=to_label,
                edge_type=edge.type,
            )
            await self._ch.insert_events([event.to_row()])
            log.debug(
                "OLAP sync: emitted %s for edge %s (%s→%s)",
                event.event_type, edge.type, edge.from_id, edge.to_id,
            )
        except Exception:
            log.warning(
                "OLAP sync failed for edge %s (%s→%s) — event dropped",
                edge.type, edge.from_id, edge.to_id, exc_info=True,
            )

    async def sync_node(
        self,
        graph: GraphAdapter,
        scope: Scope,
        node_id: str,
        label: str,
        *,
        event_type: str | None = None,
        actor_id: str = "",
    ) -> None:
        """Manually enrich and emit a node event.

        Used by batch import paths (e.g. SubscriptionEvent bulk creation)
        that bypass ``create_node`` and write directly via Cypher.
        """
        if label in _SKIP_LABELS:
            return
        try:
            event = await enrich_node(
                graph,
                scope,
                node_id=node_id,
                label=label,
                event_type=event_type,
                actor_id=actor_id,
            )
            await self._ch.insert_events([event.to_row()])
        except Exception:
            log.warning(
                "OLAP sync failed for %s/%s — event dropped",
                label, node_id, exc_info=True,
            )
