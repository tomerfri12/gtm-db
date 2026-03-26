"""ContactsAPI -- typed CRUD + relationship helpers for Contact nodes."""

from __future__ import annotations

from gtmdb.api._base import EntityAPI
from gtmdb.api.models import Contact
from gtmdb.scope import Scope
from gtmdb.types import EdgeData


class ContactsAPI(EntityAPI[Contact]):
    _label = "Contact"
    _entity_cls = Contact

    async def assign_to_account(
        self, scope: Scope, contact_id: str, account_id: str,
    ) -> None:
        """Create a WORKS_AT edge from this contact to an account."""
        await self._graph.create_edge(
            scope, EdgeData("WORKS_AT", contact_id, account_id),
        )

    async def for_account(
        self, scope: Scope, account_id: str, *, limit: int = 50,
    ) -> list[Contact]:
        """List contacts linked to an account via WORKS_AT."""
        if not scope.can_read(self._label):
            return []
        lim = max(1, min(int(limit), 500))
        query = (
            "MATCH (n:Contact {tenant_id: $tenant_id})-[:WORKS_AT]->"
            "(:Account {id: $account_id, tenant_id: $tenant_id}) "
            "RETURN properties(n) AS props "
            f"ORDER BY n.created_at DESC LIMIT {lim}"
        )
        records = await self._graph.execute(
            scope, query, {"account_id": account_id},
        )
        results: list[Contact] = []
        for rec in records:
            entity = self._from_raw_props(dict(rec["props"]), scope)
            if entity is not None:
                results.append(entity)
        return results
