"""ScoresAPI -- Score nodes linked to Leads via HAS_SCORE."""

from __future__ import annotations

from typing import Any

from gtmdb.api._base import EntityAPI
from gtmdb.api.models import Score
from gtmdb.scope import Scope
from gtmdb.types import EdgeData


class ScoresAPI(EntityAPI[Score]):
    _label = "Score"
    _entity_cls = Score

    async def create(self, scope: Scope, **kwargs: Any) -> Score:
        lead_id = kwargs.get("lead_id")
        lid = (str(lead_id).strip() if lead_id is not None else "")
        if not lid:
            raise ValueError("lead_id is required to create a Score")
        kwargs["lead_id"] = lid
        lead_node = await self._graph.get_node(scope, "Lead", lid)
        if lead_node is None:
            raise ValueError(f"Lead {lid} not found")
        if kwargs.get("scored_by") in (None, ""):
            kwargs["scored_by"] = scope.owner_id
        result = await super().create(scope, **kwargs)
        await self._graph.create_edge(
            scope,
            EdgeData("HAS_SCORE", result.id, lid),
        )
        return result

    async def list_for_lead(
        self, scope: Scope, lead_id: str, *, limit: int = 20
    ) -> list[Score]:
        """Return Score nodes that point at this Lead via HAS_SCORE, newest first."""
        if not scope.can_read(self._label):
            return []
        lid = (lead_id or "").strip()
        if not lid:
            return []
        lim = max(1, min(int(limit), 100))
        query = (
            "MATCH (s:Score {tenant_id: $tenant_id})-[:HAS_SCORE]->"
            "(l:Lead {id: $lead_id, tenant_id: $tenant_id}) "
            "RETURN properties(s) AS props "
            f"ORDER BY s.created_at DESC LIMIT {lim}"
        )
        records = await self._graph.execute(scope, query, {"lead_id": lid})
        out: list[Score] = []
        for rec in records:
            entity = self._from_raw_props(dict(rec["props"]), scope)
            if entity is not None:
                out.append(entity)
        return out
