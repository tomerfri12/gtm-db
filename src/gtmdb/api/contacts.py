"""ContactsAPI -- typed CRUD + relationship helpers for Contact nodes."""

from __future__ import annotations

from typing import Any

from gtmdb.api._base import EntityAPI
from gtmdb.api._common import display_name_for_person, require_non_empty_str
from gtmdb.api.models import Contact
from gtmdb.scope import Scope
from gtmdb.types import EdgeData


class ContactsAPI(EntityAPI[Contact]):
    _label = "Contact"
    _entity_cls = Contact

    async def create(self, scope: Scope, **kwargs: Any) -> Contact:
        kwargs["name"] = display_name_for_person(
            kwargs.get("first_name"),
            kwargs.get("last_name"),
            company_name=kwargs.get("company_name"),
            email=kwargs.get("email"),
            fallback="Contact",
        )
        return await super().create(scope, **kwargs)

    async def update(self, scope: Scope, entity_id: str, **kwargs: Any) -> Contact | None:
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
                    fn,
                    ln,
                    company_name=(
                        kwargs["company_name"]
                        if "company_name" in kwargs
                        else current.company_name
                    ),
                    email=(
                        kwargs["email"] if "email" in kwargs else current.email
                    ),
                    fallback="Contact",
                ),
            }
        return await super().update(scope, entity_id, **kwargs)

    async def assign_to_account(
        self,
        scope: Scope,
        contact_id: str,
        account_id: str,
        *,
        reasoning: str,
    ) -> None:
        """Create a WORKS_AT edge from this contact to an account."""
        rs = require_non_empty_str(reasoning, "reasoning")
        await self._graph.create_edge(
            scope, EdgeData("WORKS_AT", contact_id, account_id, reasoning=rs),
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
