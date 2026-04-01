"""ProductAccountsAPI -- typed CRUD for ProductAccount nodes."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from gtmdb.api._base import EntityAPI
from gtmdb.api._common import require_non_empty_str
from gtmdb.api.actors import ActorsAPI
from gtmdb.api.models import ProductAccount
from gtmdb.scope import Scope


class ProductAccountsAPI(EntityAPI[ProductAccount]):
    _label = "ProductAccount"
    _entity_cls = ProductAccount

    _CYPHER_MERGE_NODES = """
UNWIND $rows AS row
MERGE (pa:ProductAccount {tenant_id: $tenant_id, external_id: row.external_id})
ON CREATE SET
    pa.id = randomUUID(),
    pa.created_at = $now,
    pa.created_by_actor_id = $actor_id,
    pa.updated_at = $now
SET
    pa.name = row.name,
    pa.status = row.status,
    pa.updated_at = $now
WITH pa, row
MATCH (a:Actor {tenant_id: $tenant_id, id: $actor_id})
MERGE (a)-[r:CREATED_BY]->(pa)
ON CREATE SET r.reasoning = coalesce(row.reasoning, '')
RETURN pa.external_id AS external_id, pa.id AS id
"""

    _CYPHER_MERGE_FOR_PRODUCT = """
UNWIND $rows AS row
MATCH (pa:ProductAccount {tenant_id: $tenant_id, external_id: row.external_id})
MATCH (prod:Product {tenant_id: $tenant_id, id: row.product_id})
MERGE (pa)-[r:FOR_PRODUCT]->(prod)
ON CREATE SET r.reasoning = coalesce(row.product_reasoning, '')
"""

    async def merge_import_batch(
        self,
        scope: Scope,
        *,
        actor_id: str,
        rows: list[dict[str, Any]],
        batch_size: int = 500,
        after_chunk: Callable[[int, int], None] | None = None,
    ) -> dict[str, str]:
        """Idempotent bulk upsert for CSV-style imports.

        MERGE on ``(tenant_id, external_id)``, refresh ``name``/``status``,
        ensure ``CREATED_BY`` and optional ``FOR_PRODUCT`` per row.

        Each row dict: ``external_id`` (required), ``name``, ``status``,
        ``reasoning`` (``CREATED_BY``), optional ``product_id`` and
        ``product_reasoning`` for ``FOR_PRODUCT``.

        Returns map ``external_id -> node id``.
        """
        if not rows:
            return {}
        if not scope.can_write(self._label):
            raise PermissionError(
                f"Token {scope.owner_id} cannot write {self._label}"
            )

        aid = require_non_empty_str(actor_id, "actor_id")
        await ActorsAPI(self._graph).ensure(scope, aid)

        normalized: list[dict[str, Any]] = []
        for raw in rows:
            eid = (raw.get("external_id") or "").strip()
            if not eid:
                continue
            name = raw.get("name")
            status = raw.get("status")
            reasoning = raw.get("reasoning")
            if reasoning is not None:
                reasoning = str(reasoning).strip() or None
            entry: dict[str, Any] = {
                "external_id": eid,
                "name": name if name is None else str(name),
                "status": status if status is None else str(status),
                "reasoning": reasoning,
            }
            pid = raw.get("product_id")
            if pid is not None and str(pid).strip():
                entry["product_id"] = str(pid).strip()
                pr = raw.get("product_reasoning")
                entry["product_reasoning"] = (
                    str(pr).strip() if pr is not None else ""
                )
            normalized.append(entry)

        if not normalized:
            return {}

        now = self._now_iso()
        out: dict[str, str] = {}
        bs = max(1, batch_size)

        product_rows = [
            {
                "external_id": r["external_id"],
                "product_id": r["product_id"],
                "product_reasoning": r.get("product_reasoning", ""),
            }
            for r in normalized
            if r.get("product_id")
        ]
        n1 = (len(normalized) + bs - 1) // bs
        n2 = (len(product_rows) + bs - 1) // bs
        total_chunks = n1 + n2
        chunk_i = 0

        def _step() -> None:
            nonlocal chunk_i
            if after_chunk is not None and total_chunks > 0:
                chunk_i += 1
                after_chunk(chunk_i, total_chunks)

        for i in range(0, len(normalized), bs):
            chunk = normalized[i : i + bs]
            records = await self._graph.execute(
                scope,
                self._CYPHER_MERGE_NODES,
                {"rows": chunk, "now": now, "actor_id": aid},
            )
            for rec in records:
                eid = rec.get("external_id")
                nid = rec.get("id")
                if eid is not None and nid is not None:
                    out[str(eid)] = str(nid)
            _step()

        for i in range(0, len(product_rows), bs):
            chunk = product_rows[i : i + bs]
            await self._graph.execute(
                scope,
                self._CYPHER_MERGE_FOR_PRODUCT,
                {"rows": chunk},
            )
            _step()

        return out

    async def merge_for_product_edges_only(
        self,
        scope: Scope,
        *,
        rows: list[dict[str, Any]],
        batch_size: int = 500,
        after_chunk: Callable[[int, int], None] | None = None,
    ) -> None:
        """MERGE ``(ProductAccount)-[:FOR_PRODUCT]->(Product)`` in batches.

        Each row: ``external_id``, ``product_id``, optional ``product_reasoning``.
        Idempotent with existing edges from :meth:`merge_import_batch`.
        """
        if not rows:
            return
        if not scope.can_write(self._label):
            raise PermissionError(
                f"Token {scope.owner_id} cannot write {self._label}"
            )

        normalized: list[dict[str, Any]] = []
        for raw in rows:
            eid = (raw.get("external_id") or "").strip()
            pid = (raw.get("product_id") or "").strip()
            if not eid or not pid:
                continue
            pr = raw.get("product_reasoning")
            normalized.append(
                {
                    "external_id": eid,
                    "product_id": pid,
                    "product_reasoning": str(pr).strip() if pr is not None else "",
                }
            )
        if not normalized:
            return

        bs = max(1, batch_size)
        total = (len(normalized) + bs - 1) // bs
        for i in range(0, len(normalized), bs):
            chunk = normalized[i : i + bs]
            await self._graph.execute(
                scope,
                self._CYPHER_MERGE_FOR_PRODUCT,
                {"rows": chunk},
            )
            if after_chunk is not None and total > 0:
                after_chunk(i // bs + 1, total)
