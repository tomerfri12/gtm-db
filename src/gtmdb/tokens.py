from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime


def _utcnow() -> datetime:
    return datetime.utcnow()


@dataclass
class AccessToken:
    """In-memory credential for :class:`gtmdb.scope.Scope` (policy evaluation).

    Host apps (e.g. CRM2) may persist tokens in their own store; GtmDB does not
    accept a SQL engine or manage Postgres tables for consumers.
    """

    tenant_id: uuid.UUID
    owner_id: str
    owner_type: str
    label: str = ""
    policies: str = "[]"
    redact_mode: str = "hint"
    key_id: str | None = None
    is_active: bool = True
    expires_at: datetime | None = None
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    created_at: datetime = field(default_factory=_utcnow)
    updated_at: datetime = field(default_factory=_utcnow)

    def __repr__(self) -> str:
        return (
            f"<AccessToken id={self.id} owner={self.owner_type}:{self.owner_id} "
            f"tenant={self.tenant_id}>"
        )
