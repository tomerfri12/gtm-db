"""EmailsAPI -- typed CRUD for Email nodes."""

from __future__ import annotations

from typing import Any

from gtmdb.api._base import EntityAPI
from gtmdb.api.models import Email
from gtmdb.scope import Scope


class EmailsAPI(EntityAPI[Email]):
    _label = "Email"
    _entity_cls = Email

    async def create(self, scope: Scope, **kwargs: Any) -> Email:
        subj = (kwargs.get("subject") or "").strip()
        seq = kwargs.get("sequence_number")
        try:
            n = int(seq) if seq is not None else 1
        except (TypeError, ValueError):
            n = 1
        kwargs["name"] = subj or f"Email step {n}"
        return await super().create(scope, **kwargs)
