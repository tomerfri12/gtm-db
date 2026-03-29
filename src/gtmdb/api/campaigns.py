"""CampaignsAPI -- typed CRUD + relationship helpers for Campaign nodes."""

from __future__ import annotations

from gtmdb.api._base import EntityAPI
from gtmdb.api._common import require_non_empty_str
from gtmdb.api.models import Campaign
from gtmdb.scope import Scope
from gtmdb.types import EdgeData


class CampaignsAPI(EntityAPI[Campaign]):
    _label = "Campaign"
    _entity_cls = Campaign

    async def add_lead(
        self,
        scope: Scope,
        campaign_id: str,
        lead_id: str,
        *,
        reasoning: str,
    ) -> None:
        """Record that a lead was sourced from this campaign (``SOURCED_FROM`` lead → campaign)."""
        rs = require_non_empty_str(reasoning, "reasoning")
        await self._graph.create_edge(
            scope, EdgeData("SOURCED_FROM", lead_id, campaign_id, reasoning=rs),
        )
