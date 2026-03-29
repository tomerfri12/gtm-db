"""API key management: create, resolve, revoke, rotate, list.

``ApiKeysManager`` sits on top of :class:`~gtmdb.key_store.KeyStore` and
adds key generation, SHA-256 hashing, policy composition, and expiry
verification.  The ``resolve`` method is the hot path used by agents at
connect time; admin methods (create / revoke / rotate / list) are
typically called from the CLI.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import secrets
import uuid
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from typing import Any

from gtmdb.api.models import ApiKeyInfo, ApiKeyResult
from gtmdb.key_store import KeyStore
from gtmdb.presets import PRESETS
from gtmdb.scope import Scope
from gtmdb.tokens import AccessToken

# Per-request scope for HTTP servers (asyncio task–local). CLI may use ``bind_scope`` only.
_request_scope: ContextVar[Scope | None] = ContextVar("gtmdb_request_scope", default=None)


def _generate_key() -> tuple[str, str]:
    """Return ``(raw_key, key_id)``."""
    key_id = secrets.token_urlsafe(6)
    secret = secrets.token_urlsafe(32)
    raw_key = f"gtmdb_{key_id}_{secret}"
    return raw_key, key_id


def _hash_key(raw_key: str) -> str:
    return hashlib.sha256(raw_key.encode()).hexdigest()


def _parse_key(raw_key: str) -> str:
    """Extract ``key_id`` from a raw key string. Raises on bad format."""
    parts = raw_key.split("_", 2)
    if len(parts) != 3 or parts[0] != "gtmdb" or not parts[1] or not parts[2]:
        raise ValueError("Invalid API key format (expected gtmdb_<key_id>_<secret>)")
    return parts[1]


class ApiKeysManager:
    """Business logic for GtmDB API keys (backed by Postgres).

    Admin methods (create / revoke / rotate / list) require a bound scope
    with ``owner_type == "admin"``.  Call :meth:`bind_scope` after connect
    to enable them.  :meth:`resolve` is always available (used at connect time).
    """

    def __init__(self, store: KeyStore) -> None:
        self._store = store
        self._scope: Scope | None = None

    def bind_scope(self, scope: Scope) -> None:
        """Attach the caller's scope so admin guards can be evaluated."""
        self._scope = scope

    def _require_admin(self) -> None:
        scope = _request_scope.get() or self._scope
        if scope is None or scope.owner_type != "admin":
            raise PermissionError(
                "Only admin keys can manage API keys. "
                "Connect with GTMDB_ADMIN_KEY to perform this operation."
            )

    async def create(
        self,
        *,
        owner_id: str,
        owner_type: str = "agent",
        tenant_id: str,
        preset_names: list[str] | None = None,
        extra_policies: list[dict[str, Any]] | None = None,
        label: str = "",
        expires_in_days: int | None = None,
        created_by: str | None = None,
    ) -> ApiKeyResult:
        """Generate a new API key. Returns the raw key (shown once)."""
        self._require_admin()
        raw_key, key_id = _generate_key()
        key_hash = _hash_key(raw_key)

        policies: list[dict[str, Any]] = []
        chosen = ["full_access"] if preset_names is None else list(preset_names)
        for name in chosen:
            if name not in PRESETS:
                raise KeyError(f"Unknown preset: {name!r}")
            policies.extend(PRESETS[name])
        if extra_policies:
            policies.extend(extra_policies)

        expires_at: datetime | None = None
        if expires_in_days is not None:
            expires_at = datetime.now(timezone.utc) + timedelta(days=expires_in_days)

        tid = uuid.UUID(tenant_id)
        row = {
            "id": uuid.uuid4(),
            "key_id": key_id,
            "key_hash": key_hash,
            "tenant_id": tid,
            "owner_id": owner_id,
            "owner_type": owner_type,
            "label": label,
            "policies": json.dumps(policies),
            "is_active": True,
            "expires_at": expires_at,
            "created_at": datetime.now(timezone.utc),
            "created_by": created_by,
        }
        await self._store.insert(row)

        return ApiKeyResult(
            raw_key=raw_key,
            key_id=key_id,
            owner_id=owner_id,
            label=label,
            expires_at=expires_at.isoformat() if expires_at else None,
        )

    async def resolve(self, raw_key: str) -> Scope:
        """Verify a raw API key and return its ``Scope``.

        Hits Postgres on every call (no cache). Raises ``ValueError`` on
        invalid, revoked, or expired keys.
        """
        key_id = _parse_key(raw_key)
        row = await self._store.get_by_key_id(key_id)
        if row is None:
            raise ValueError("Invalid API key")

        if _hash_key(raw_key) != row["key_hash"]:
            raise ValueError("Invalid API key")

        if not row["is_active"]:
            raise ValueError("API key has been revoked")

        if row["expires_at"] is not None:
            exp = row["expires_at"]
            if not exp.tzinfo:
                exp = exp.replace(tzinfo=timezone.utc)
            if exp < datetime.now(timezone.utc):
                raise ValueError("API key has expired")

        token = AccessToken(
            tenant_id=row["tenant_id"],
            owner_id=row["owner_id"],
            owner_type=row["owner_type"],
            label=row.get("label", ""),
            policies=row["policies"],
            redact_mode="hint",
            is_active=True,
            expires_at=row["expires_at"],
        )

        asyncio.ensure_future(self._touch(key_id))
        return Scope(token)

    async def _touch(self, key_id: str) -> None:
        try:
            await self._store.update_last_used(key_id)
        except Exception:
            pass

    async def revoke(self, key_id: str) -> bool:
        """Deactivate a key. Returns ``True`` if the key existed."""
        self._require_admin()
        return await self._store.deactivate(key_id)

    async def rotate(
        self,
        key_id: str,
        *,
        expires_in_days: int | None = None,
    ) -> ApiKeyResult:
        """Create a replacement key with the same metadata, then revoke the old one."""
        self._require_admin()
        old = await self._store.get_by_key_id(key_id)
        if old is None:
            raise ValueError(f"Key {key_id!r} not found")

        result = await self.create(
            owner_id=old["owner_id"],
            owner_type=old["owner_type"],
            tenant_id=str(old["tenant_id"]),
            preset_names=[],
            extra_policies=json.loads(old["policies"]),
            label=old.get("label", ""),
            expires_in_days=expires_in_days,
            created_by=old.get("created_by"),
        )
        await self._store.deactivate(key_id)
        return result

    async def list_keys(self, tenant_id: str) -> list[ApiKeyInfo]:
        self._require_admin()
        rows = await self._store.list_keys(tenant_id)
        return [
            ApiKeyInfo(
                key_id=r["key_id"],
                owner_id=r["owner_id"],
                owner_type=r["owner_type"],
                label=r.get("label", ""),
                is_active=r["is_active"],
                expires_at=r["expires_at"].isoformat() if r.get("expires_at") else None,
                created_at=r["created_at"].isoformat() if r.get("created_at") else None,
                last_used_at=r["last_used_at"].isoformat() if r.get("last_used_at") else None,
            )
            for r in rows
        ]


def set_request_scope(scope: Scope | None) -> None:
    """Bind *scope* for the current asyncio task (HTTP request)."""
    _request_scope.set(scope)
