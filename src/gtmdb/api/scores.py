"""ScoresAPI -- Score nodes linked to Leads via HAS_SCORE."""

from __future__ import annotations

from typing import Any

from gtmdb.api._base import EntityAPI
from gtmdb.api._common import optional_reasoning, require_non_empty_str
from gtmdb.api.models import Score
from gtmdb.scope import Scope
from gtmdb.types import EdgeData


class ScoresAPI(EntityAPI[Score]):
    _label = "Score"
    _entity_cls = Score

    async def create(self, scope: Scope, **kwargs: Any) -> Score:
        raise TypeError(
            "Scores are created via LeadsAPI.add_score(...), not ScoresAPI.create"
        )

    async def _create_for_lead(
        self,
        scope: Scope,
        *,
        lead_id: str,
        actor_id: str,
        has_score_reasoning: str,
        creation_reasoning: str | None = None,
        **kwargs: Any,
    ) -> Score:
        """Internal: create Score + HAS_SCORE after lead validation."""
        lid = require_non_empty_str(lead_id, "lead_id")
        aid = require_non_empty_str(actor_id, "actor_id")
        rs_link = require_non_empty_str(has_score_reasoning, "has_score_reasoning")

        lead_node = await self._graph.get_node(scope, "Lead", lid)
        if lead_node is None:
            raise ValueError(f"Lead {lid} not found")

        kwargs["lead_id"] = lid
        if kwargs.get("scored_by") in (None, ""):
            kwargs["scored_by"] = aid

        st = str(kwargs.get("score_type") or "bant")
        total = int(kwargs.get("total", 0))
        kwargs["name"] = f"{st}:{total}"

        result = await EntityAPI.create(
            self,
            scope,
            actor_id=aid,
            reasoning=optional_reasoning(creation_reasoning),
            **kwargs,
        )
        await self._graph.create_edge(
            scope,
            EdgeData("HAS_SCORE", result.id, lid, reasoning=rs_link),
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
