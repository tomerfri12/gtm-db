"""SubscriptionEventsAPI -- typed CRUD for SubscriptionEvent nodes."""

from __future__ import annotations

import math
import uuid
from collections.abc import Callable
from typing import Any

from gtmdb.api._base import EntityAPI
from gtmdb.api._common import optional_reasoning, require_non_empty_str
from gtmdb.api.actors import ActorsAPI
from gtmdb.api.models import SubscriptionEvent
from gtmdb.scope import Scope


class SubscriptionEventsAPI(EntityAPI[SubscriptionEvent]):
    _label = "SubscriptionEvent"
    _entity_cls = SubscriptionEvent

    _CYPHER_BATCH_CREATE_CORE = """
UNWIND $rows AS row
CREATE (e:SubscriptionEvent {
  tenant_id: $tenant_id,
  id: randomUUID(),
  created_at: $now,
  updated_at: $now,
  created_by_actor_id: $actor_id,
  event_type: row.event_type,
  occurred_at: row.occurred_at
})
SET e += coalesce(row.extra_props, {})
WITH e, row
MATCH (a:Actor {tenant_id: $tenant_id, id: $actor_id})
MERGE (a)-[cr:CREATED_BY]->(e)
ON CREATE SET cr.reasoning = coalesce(row.created_reasoning, '')
WITH e, row
MATCH (pa:ProductAccount {tenant_id: $tenant_id, id: row.pa_id})
CREATE (pa)-[h:HAS_SUBSCRIPTION_EVENT]->(e)
SET h.reasoning = coalesce(row.pa_link_reasoning, '')
RETURN row.import_key AS import_key, e.id AS event_id
"""

    _CYPHER_BATCH_FOR_PRODUCT = """
UNWIND $rows AS row
MATCH (e:SubscriptionEvent {tenant_id: $tenant_id, id: row.event_id})
MATCH (prod:Product {tenant_id: $tenant_id, id: row.product_id})
MERGE (e)-[fp:FOR_PRODUCT]->(prod)
ON CREATE SET fp.reasoning = coalesce(row.prod_link_reasoning, '')
"""

    async def create_import_batch(
        self,
        scope: Scope,
        *,
        actor_id: str,
        rows: list[dict[str, Any]],
        batch_size: int = 500,
        after_chunk: Callable[[int, int], None] | None = None,
    ) -> None:
        """Bulk-create subscription events + ``CREATED_BY``, ``HAS_SUBSCRIPTION_EVENT``, optional ``FOR_PRODUCT``.

        Each input row: ``pa_id``, ``event_type``, ``occurred_at`` (required),
        optional ``product_id``, ``extra_props`` (map of domain fields),
        ``created_reasoning``, ``pa_link_reasoning``, ``prod_link_reasoning``.
        """
        if not rows:
            return
        if not scope.can_write(self._label):
            raise PermissionError(
                f"Token {scope.owner_id} cannot write {self._label}"
            )

        aid = require_non_empty_str(actor_id, "actor_id")
        await ActorsAPI(self._graph).ensure(scope, aid)

        normalized: list[dict[str, Any]] = []
        for raw in rows:
            pa_id = (raw.get("pa_id") or "").strip()
            et = (raw.get("event_type") or "").strip()
            oc = raw.get("occurred_at")
            if not pa_id or not et or oc is None or not str(oc).strip():
                continue
            extra = dict(raw.get("extra_props") or {})
            cleaned: dict[str, Any] = {}
            for k, v in extra.items():
                if v is None:
                    continue
                if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                    continue
                cleaned[k] = v

            pid = raw.get("product_id")
            ps = (str(pid).strip() if pid is not None else "") or None

            cr = optional_reasoning(raw.get("created_reasoning"))
            normalized.append(
                {
                    "import_key": str(uuid.uuid4()),
                    "pa_id": pa_id,
                    "event_type": et,
                    "occurred_at": str(oc).strip(),
                    "extra_props": cleaned,
                    "created_reasoning": cr or "",
                    "pa_link_reasoning": (raw.get("pa_link_reasoning") or "")
                    .strip()
                    or "Lifecycle event for this product account",
                    "prod_link_reasoning": (raw.get("prod_link_reasoning") or "")
                    .strip()
                    or "Event applies to this product line",
                    "_product_id": ps,
                }
            )

        if not normalized:
            return

        now = self._now_iso()
        bs = max(1, batch_size)

        n_core = (len(normalized) + bs - 1) // bs
        total_chunks = n_core
        chunk_i = 0

        def _step() -> None:
            nonlocal chunk_i
            if after_chunk is not None and total_chunks > 0:
                chunk_i += 1
                after_chunk(chunk_i, total_chunks)

        cypher_rows: list[dict[str, Any]] = []
        for r in normalized:
            cypher_rows.append(
                {
                    "import_key": r["import_key"],
                    "pa_id": r["pa_id"],
                    "event_type": r["event_type"],
                    "occurred_at": r["occurred_at"],
                    "extra_props": r["extra_props"],
                    "created_reasoning": r["created_reasoning"],
                    "pa_link_reasoning": r["pa_link_reasoning"],
                    "prod_link_reasoning": r["prod_link_reasoning"],
                }
            )

        for i in range(0, len(cypher_rows), bs):
            chunk = cypher_rows[i : i + bs]
            recs = await self._graph.execute(
                scope,
                self._CYPHER_BATCH_CREATE_CORE,
                {"rows": chunk, "now": now, "actor_id": aid},
            )
            key_to_eid = {
                str(r["import_key"]): str(r["event_id"])
                for r in recs
                if r.get("import_key") and r.get("event_id")
            }
            fp_chunk: list[dict[str, Any]] = []
            for src, _ in zip(
                normalized[i : i + bs],
                chunk,
                strict=True,
            ):
                pid = src.get("_product_id")
                if not pid:
                    continue
                eid = key_to_eid.get(src["import_key"])
                if not eid:
                    continue
                fp_chunk.append(
                    {
                        "event_id": eid,
                        "product_id": pid,
                        "prod_link_reasoning": src["prod_link_reasoning"],
                    }
                )
            if fp_chunk:
                await self._graph.execute(
                    scope,
                    self._CYPHER_BATCH_FOR_PRODUCT,
                    {"rows": fp_chunk},
                )

            # OLAP sync: enrich each created SubscriptionEvent and emit to ClickHouse
            for event_id in key_to_eid.values():
                await self._graph.sync_node_to_olap(
                    scope, event_id, "SubscriptionEvent", actor_id=aid,
                )

            _step()
