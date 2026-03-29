"""LeadsAPI -- typed CRUD + relationship helpers for Lead nodes."""

from __future__ import annotations

from typing import Any

from gtmdb.api._base import EntityAPI
from gtmdb.api._common import display_name_for_person, require_non_empty_str
from gtmdb.api.models import Lead, Score
from gtmdb.api.scores import ScoresAPI
from gtmdb.scope import Scope
from gtmdb.types import EdgeData


class LeadsAPI(EntityAPI[Lead]):
    _label = "Lead"
    _entity_cls = Lead

    async def create(self, scope: Scope, **kwargs: Any) -> Lead:
        fn, ln = kwargs.get("first_name"), kwargs.get("last_name")
        kwargs["name"] = display_name_for_person(
            fn, ln,
            company_name=kwargs.get("company_name"),
            email=kwargs.get("email"),
            fallback="Lead",
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
            kwargs = {
                **kwargs,
                "name": display_name_for_person(
                    fn, ln,
                    company_name=(
                        kwargs["company_name"]
                        if "company_name" in kwargs
                        else current.company_name
                    ),
                    email=(
                        kwargs["email"] if "email" in kwargs else current.email
                    ),
                    fallback="Lead",
                ),
            }
        return await super().update(scope, entity_id, **kwargs)

    async def link_campaign(
        self,
        scope: Scope,
        lead_id: str,
        campaign_id: str,
        *,
        reasoning: str,
    ) -> None:
        """Create a SOURCED_FROM edge from this lead to a campaign (MQL source)."""
        rs = require_non_empty_str(reasoning, "reasoning")
        await self._graph.create_edge(
            scope, EdgeData("SOURCED_FROM", lead_id, campaign_id, reasoning=rs),
        )

    async def add_score(
        self,
        scope: Scope,
        lead_id: str,
        *,
        actor_id: str,
        has_score_reasoning: str,
        reasoning: str | None = None,
        **score_fields: Any,
    ) -> Score:
        """Create a Score node and ``HAS_SCORE`` to this lead."""
        scores_api = ScoresAPI(self._graph)
        return await scores_api._create_for_lead(
            scope,
            lead_id=lead_id,
            actor_id=actor_id,
            has_score_reasoning=has_score_reasoning,
            creation_reasoning=reasoning,
            **score_fields,
        )

    async def scores_for(
        self, scope: Scope, lead_id: str, *, limit: int = 20
    ) -> list[Score]:
        """BANT / qualification scores linked to this lead (newest first)."""
        scores_api = ScoresAPI(self._graph)
        return await scores_api.list_for_lead(scope, lead_id, limit=limit)
