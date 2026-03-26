"""LeadsAPI -- typed CRUD + relationship helpers for Lead nodes."""

from __future__ import annotations

from typing import Any

from gtmdb.api._base import EntityAPI
from gtmdb.api.models import Lead, Score
from gtmdb.api.scores import ScoresAPI
from gtmdb.scope import Scope
from gtmdb.types import EdgeData


def _composed_lead_name(
    first_name: str | None,
    last_name: str | None,
) -> str | None:
    parts = [(first_name or "").strip(), (last_name or "").strip()]
    parts = [p for p in parts if p]
    return " ".join(parts) if parts else None


class LeadsAPI(EntityAPI[Lead]):
    _label = "Lead"
    _entity_cls = Lead

    async def create(self, scope: Scope, **kwargs: Any) -> Lead:
        kwargs["name"] = _composed_lead_name(
            kwargs.get("first_name"),
            kwargs.get("last_name"),
        )
        return await super().create(scope, **kwargs)

    async def update(self, scope: Scope, entity_id: str, **kwargs: Any) -> Lead | None:
        if "first_name" in kwargs or "last_name" in kwargs:
            current = await self.get(scope, entity_id)
            if current is None:
                return None
            fn = (
                kwargs["first_name"]
                if "first_name" in kwargs
                else current.first_name
            )
            ln = (
                kwargs["last_name"]
                if "last_name" in kwargs
                else current.last_name
            )
            kwargs = {**kwargs, "name": _composed_lead_name(fn, ln)}
        return await super().update(scope, entity_id, **kwargs)

    async def assign_to_account(
        self,
        scope: Scope,
        lead_id: str,
        account_id: str,
        *,
        reasoning: str | None = None,
    ) -> None:
        """Create a WORKS_AT edge from this lead to an account."""
        await self._graph.create_edge(
            scope, EdgeData("WORKS_AT", lead_id, account_id, reasoning=reasoning),
        )

    async def link_campaign(
        self,
        scope: Scope,
        lead_id: str,
        campaign_id: str,
        *,
        reasoning: str | None = None,
    ) -> None:
        """Create a SOURCED_FROM edge from this lead to a campaign (MQL source)."""
        await self._graph.create_edge(
            scope, EdgeData("SOURCED_FROM", lead_id, campaign_id, reasoning=reasoning),
        )

    async def scores_for(
        self, scope: Scope, lead_id: str, *, limit: int = 20
    ) -> list[Score]:
        """BANT / qualification scores linked to this lead (newest first)."""
        scores_api = ScoresAPI(self._graph)
        return await scores_api.list_for_lead(scope, lead_id, limit=limit)

    async def for_account(
        self, scope: Scope, account_id: str, *, limit: int = 50,
    ) -> list[Lead]:
        """List leads linked to an account via WORKS_AT."""
        if not scope.can_read(self._label):
            return []
        lim = max(1, min(int(limit), 500))
        query = (
            "MATCH (n:Lead {tenant_id: $tenant_id})-[:WORKS_AT]->"
            "(:Account {id: $account_id, tenant_id: $tenant_id}) "
            "RETURN properties(n) AS props "
            f"ORDER BY n.created_at DESC LIMIT {lim}"
        )
        records = await self._graph.execute(
            scope, query, {"account_id": account_id},
        )
        results: list[Lead] = []
        for rec in records:
            entity = self._from_raw_props(dict(rec["props"]), scope)
            if entity is not None:
                results.append(entity)
        return results
