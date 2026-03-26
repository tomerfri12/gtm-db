"""DealsAPI -- typed CRUD + relationship helpers for Deal nodes."""

from __future__ import annotations

from gtmdb.api._base import EntityAPI
from gtmdb.api.models import Deal
from gtmdb.scope import Scope
from gtmdb.types import EdgeData


class DealsAPI(EntityAPI[Deal]):
    _label = "Deal"
    _entity_cls = Deal

    async def assign_to_account(
        self, scope: Scope, deal_id: str, account_id: str,
    ) -> None:
        """Create a BELONGS_TO edge from this deal to an account."""
        await self._graph.create_edge(
            scope, EdgeData("BELONGS_TO", deal_id, account_id),
        )

    async def add_contact(
        self, scope: Scope, deal_id: str, contact_id: str,
    ) -> None:
        """Create a HAS_CONTACT edge from this deal to a contact."""
        await self._graph.create_edge(
            scope, EdgeData("HAS_CONTACT", deal_id, contact_id),
        )

    async def add_campaign(
        self, scope: Scope, deal_id: str, campaign_id: str,
    ) -> None:
        """Link a campaign to this deal via INFLUENCED (campaign -> deal)."""
        await self._graph.create_edge(
            scope, EdgeData("INFLUENCED", campaign_id, deal_id),
        )

    async def for_account(
        self, scope: Scope, account_id: str, *, limit: int = 50,
    ) -> list[Deal]:
        """List deals linked to an account via BELONGS_TO."""
        if not scope.can_read(self._label):
            return []
        lim = max(1, min(int(limit), 500))
        query = (
            "MATCH (n:Deal {tenant_id: $tenant_id})-[:BELONGS_TO]->"
            "(:Account {id: $account_id, tenant_id: $tenant_id}) "
            "RETURN properties(n) AS props "
            f"ORDER BY coalesce(n.amount, 0) DESC LIMIT {lim}"
        )
        records = await self._graph.execute(
            scope, query, {"account_id": account_id},
        )
        results: list[Deal] = []
        for rec in records:
            entity = self._from_raw_props(dict(rec["props"]), scope)
            if entity is not None:
                results.append(entity)
        return results
