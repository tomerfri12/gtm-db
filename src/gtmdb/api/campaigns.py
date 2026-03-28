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

    async def link_deal(
        self,
        scope: Scope,
        campaign_id: str,
        deal_id: str,
        *,
        reasoning: str,
    ) -> None:
        """Create an INFLUENCED edge from this campaign to a deal."""
        rs = require_non_empty_str(reasoning, "reasoning")
        await self._graph.create_edge(
            scope, EdgeData("INFLUENCED", campaign_id, deal_id, reasoning=rs),
        )

    async def influenced_deals(
        self, scope: Scope, campaign_id: str, *, limit: int = 50,
    ) -> list[dict]:
        """List deals influenced by this campaign.

        Returns dicts with ``deal`` key containing a Deal-shaped properties
        dict (scope-masked).
        """
        lim = max(1, min(int(limit), 500))
        query = (
            "MATCH (:Campaign {id: $campaign_id, tenant_id: $tenant_id})"
            "-[:INFLUENCED]->(d:Deal {tenant_id: $tenant_id}) "
            "RETURN properties(d) AS props "
            f"ORDER BY coalesce(d.amount, 0) DESC LIMIT {lim}"
        )
        records = await self._graph.execute(
            scope, query, {"campaign_id": campaign_id},
        )
        from gtmdb.api.deals import DealsAPI

        deal_api = DealsAPI(self._graph)
        results: list[dict] = []
        for rec in records:
            deal = deal_api._from_raw_props(dict(rec["props"]), scope)
            if deal is not None:
                results.append({"deal": deal})
        return results
