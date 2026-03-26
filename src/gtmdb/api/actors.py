"""ActorsAPI — create / upsert Actor nodes (AI agents, humans)."""

from __future__ import annotations

from datetime import datetime, timezone

from gtmdb.api.models import ActorSpec
from gtmdb.graph.adapter import GraphAdapter
from gtmdb.scope import Scope


class ActorsAPI:
    """Upsert :Actor nodes keyed by ``(tenant_id, id)``."""

    _label = "Actor"

    def __init__(self, graph: GraphAdapter) -> None:
        self._graph = graph

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    async def create(self, scope: Scope, specs: list[ActorSpec]) -> None:
        """Ensure Actor nodes exist; refresh properties on repeat calls (MERGE semantics)."""
        if not specs:
            return
        if not scope.can_write(self._label):
            raise PermissionError(
                f"Token {scope.owner_id} cannot write {self._label}"
            )

        now = self._now_iso()
        rows: list[dict] = []
        for s in specs:
            aid = (s.id or "").strip()
            if not aid:
                continue
            kind = (s.kind or "ai").strip() or "ai"
            rows.append(
                {
                    "id": aid,
                    "kind": kind,
                    "display_name": s.display_name,
                    "role_key": (s.role_key or aid).strip(),
                    "created_at": s.created_at or now,
                    "updated_at": now,
                }
            )

        if not rows:
            return

        query = """
UNWIND $rows AS row
MERGE (a:Actor {id: row.id, tenant_id: $tenant_id})
ON CREATE SET a.created_at = row.created_at
SET a.kind = row.kind,
    a.display_name = row.display_name,
    a.role_key = row.role_key,
    a.updated_at = row.updated_at
"""
        await self._graph.execute(scope, query, {"rows": rows})
