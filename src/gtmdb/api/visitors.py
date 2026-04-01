"""VisitorsAPI -- typed CRUD for Visitor nodes."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from gtmdb.api._base import EntityAPI
from gtmdb.api._common import require_non_empty_str
from gtmdb.api.actors import ActorsAPI
from gtmdb.api.models import Visitor
from gtmdb.scope import Scope


class VisitorsAPI(EntityAPI[Visitor]):
    _label = "Visitor"
    _entity_cls = Visitor

    _ALLOWED_PHASE7_RELS = frozenset({"SIGNED_UP_AS", "SIGNED_UP_FOR", "TOUCHED"})

    _CYPHER_MERGE_VISITORS = """
UNWIND $rows AS row
MERGE (v:Visitor {tenant_id: $tenant_id, visitor_id: row.visitor_id})
ON CREATE SET
    v.id = randomUUID(),
    v.created_at = $now,
    v.updated_at = $now,
    v.created_by_actor_id = $actor_id,
    v.source_channel = row.source_channel,
    v.first_seen_at = row.first_seen_at
SET v.updated_at = $now
WITH v, row
MATCH (a:Actor {tenant_id: $tenant_id, id: $actor_id})
MERGE (a)-[cr:CREATED_BY]->(v)
ON CREATE SET cr.reasoning = coalesce(row.created_reasoning, '')
RETURN row.visitor_id AS visitor_id, v.id AS id
"""

    async def import_phase7_batch(
        self,
        scope: Scope,
        *,
        actor_id: str,
        visitor_specs: list[dict[str, Any]],
        edges_signed_as: list[dict[str, Any]],
        edges_signed_for: list[dict[str, Any]],
        edges_touched: list[dict[str, Any]],
        batch_size: int = 500,
        after_chunk: Callable[[int, int], None] | None = None,
    ) -> dict[str, str]:
        """CSV phase 7: MERGE Visitors, then MERGE ``SIGNED_UP_AS`` / ``SIGNED_UP_FOR`` / ``TOUCHED``.

        ``visitor_specs``: ``visitor_id`` (external), optional ``source_channel``,
        ``first_seen_at``, optional ``created_reasoning``.

        Edge rows use ``vid_ext`` (same string as ``visitor_id``) plus target ids:
        ``pa_id``, ``product_id``, ``camp_id`` respectively, and ``reasoning``.

        Returns map external ``visitor_id`` -> graph node ``id``.
        """
        if not scope.can_write(self._label):
            raise PermissionError(
                f"Token {scope.owner_id} cannot write {self._label}"
            )

        aid = require_non_empty_str(actor_id, "actor_id")
        await ActorsAPI(self._graph).ensure(scope, aid)

        bs = max(1, batch_size)
        now = self._now_iso()

        def _nchunks(n: int) -> int:
            return (n + bs - 1) // bs if n else 0

        vrows: list[dict[str, Any]] = []
        for s in visitor_specs:
            ext = (s.get("visitor_id") or "").strip()
            if not ext:
                continue
            sc = s.get("source_channel")
            fs = s.get("first_seen_at")
            cr = s.get("created_reasoning")
            sc_out: str | None = None if sc is None else (str(sc).strip() or None)
            fs_out: str | None
            if fs is None:
                fs_out = None
            else:
                fs_out = str(fs).strip() or None
            vrows.append(
                {
                    "visitor_id": ext,
                    "source_channel": sc_out,
                    "first_seen_at": fs_out,
                    "created_reasoning": (str(cr).strip() if cr is not None else "")
                    or "",
                }
            )

        extra_steps = (
            (1 if edges_signed_as else 0)
            + (1 if edges_signed_for else 0)
            + (1 if edges_touched else 0)
        )
        total_chunks = _nchunks(len(vrows)) + extra_steps
        chunk_i = 0

        def _step() -> None:
            nonlocal chunk_i
            if after_chunk is not None and total_chunks > 0:
                chunk_i += 1
                after_chunk(chunk_i, total_chunks)

        vid_to_nid: dict[str, str] = {}

        for i in range(0, len(vrows), bs):
            chunk = vrows[i : i + bs]
            recs = await self._graph.execute(
                scope,
                self._CYPHER_MERGE_VISITORS,
                {"rows": chunk, "now": now, "actor_id": aid},
            )
            for r in recs:
                vk = r.get("visitor_id")
                nid = r.get("id")
                if vk is not None and nid is not None:
                    vid_to_nid[str(vk)] = str(nid)
            _step()

        await self._merge_phase7_rels_batched(
            scope,
            "SIGNED_UP_AS",
            edges_signed_as,
            "pa_id",
            vid_to_nid,
            bs,
            _step,
        )
        await self._merge_phase7_rels_batched(
            scope,
            "SIGNED_UP_FOR",
            edges_signed_for,
            "product_id",
            vid_to_nid,
            bs,
            _step,
        )
        await self._merge_phase7_rels_batched(
            scope,
            "TOUCHED",
            edges_touched,
            "camp_id",
            vid_to_nid,
            bs,
            _step,
        )

        return vid_to_nid

    async def _merge_phase7_rels_batched(
        self,
        scope: Scope,
        rel_type: str,
        raw_rows: list[dict[str, Any]],
        target_key: str,
        vid_to_nid: dict[str, str],
        bs: int,
        step: Callable[[], None] | None,
    ) -> None:
        if rel_type not in self._ALLOWED_PHASE7_RELS:
            raise ValueError(f"unsupported relationship type: {rel_type}")

        resolved: list[dict[str, Any]] = []
        for r in raw_rows:
            ext = (r.get("vid_ext") or "").strip()
            tid = (r.get(target_key) or "").strip()
            if not ext or not tid:
                continue
            v_id = vid_to_nid.get(ext)
            if not v_id:
                continue
            rs = (r.get("reasoning") or "").strip()
            resolved.append(
                {
                    "v_id": v_id,
                    "target_id": tid,
                    "reasoning": rs,
                }
            )

        if not resolved:
            return

        query = (
            "UNWIND $rows AS row "
            "MATCH (v:Visitor {tenant_id: $tenant_id, id: row.v_id}) "
            "MATCH (t {tenant_id: $tenant_id, id: row.target_id}) "
            f"MERGE (v)-[r:{rel_type}]->(t) "
            "ON CREATE SET r.reasoning = coalesce(row.reasoning, '')"
        )

        for i in range(0, len(resolved), bs):
            chunk = resolved[i : i + bs]
            await self._graph.execute(scope, query, {"rows": chunk})
        if resolved and step is not None:
            step()
