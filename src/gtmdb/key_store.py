"""Postgres persistence layer for GtmDB API keys.

Uses SQLAlchemy async core (not ORM) with asyncpg. The table is
auto-created via ``init_db()`` (idempotent ``CREATE TABLE IF NOT EXISTS``).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

metadata = sa.MetaData()

api_keys_table = sa.Table(
    "gtmdb_api_keys",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True, default=uuid.uuid4),
    sa.Column("key_id", sa.String(16), nullable=False, unique=True, index=True),
    sa.Column("key_hash", sa.String(64), nullable=False),
    sa.Column("tenant_id", sa.Uuid, nullable=False, index=True),
    sa.Column("owner_id", sa.String(255), nullable=False),
    sa.Column("owner_type", sa.String(50), nullable=False, server_default="actor"),
    sa.Column("label", sa.String(255), server_default=""),
    sa.Column("policies", sa.Text, nullable=False, server_default="[]"),
    sa.Column("is_active", sa.Boolean, nullable=False, server_default=sa.text("true")),
    sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.Column("created_by", sa.String(255), nullable=True),
    sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
)

def _asyncpg_dsn(dsn: str) -> str:
    """Railway and others often supply ``postgresql://``; async engine needs asyncpg."""
    dsn = dsn.strip()
    if dsn.startswith("postgres://"):
        return "postgresql+asyncpg://" + dsn.removeprefix("postgres://")
    if dsn.startswith("postgresql://") and not dsn.startswith("postgresql+"):
        return "postgresql+asyncpg://" + dsn.removeprefix("postgresql://")
    return dsn


_LIST_COLUMNS = [
    api_keys_table.c.key_id,
    api_keys_table.c.owner_id,
    api_keys_table.c.owner_type,
    api_keys_table.c.label,
    api_keys_table.c.tenant_id,
    api_keys_table.c.is_active,
    api_keys_table.c.expires_at,
    api_keys_table.c.created_at,
    api_keys_table.c.created_by,
    api_keys_table.c.last_used_at,
]

activity_log_table = sa.Table(
    "gtmdb_activity_log",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True, default=uuid.uuid4),
    sa.Column(
        "timestamp",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.Column("tenant_id", sa.Uuid, nullable=True, index=True),
    sa.Column("owner_type", sa.String(50), nullable=True),
    sa.Column("owner_id", sa.String(255), nullable=True, index=True),
    sa.Column("key_id", sa.String(32), nullable=False, index=True),
    sa.Column("method", sa.String(16), nullable=False),
    sa.Column("path", sa.Text, nullable=False),
    sa.Column("status_code", sa.Integer, nullable=False),
    sa.Column("action", sa.String(32), nullable=True),
    sa.Column("entity_type", sa.String(64), nullable=True),
    sa.Column("entity_id", sa.String(64), nullable=True),
    sa.Column("reasoning", sa.Text, nullable=True),
    sa.Column("duration_ms", sa.Integer, nullable=False),
    sa.Column("error_detail", sa.Text, nullable=True),
    sa.Column("ip_address", sa.String(64), nullable=True),
)


class KeyStore:
    """Low-level async Postgres accessor for the ``gtmdb_api_keys`` table."""

    def __init__(self, dsn: str) -> None:
        self._engine: AsyncEngine = create_async_engine(_asyncpg_dsn(dsn))

    async def init_db(self) -> None:
        async with self._engine.begin() as conn:
            await conn.run_sync(metadata.create_all)

    async def close(self) -> None:
        await self._engine.dispose()

    async def insert(self, row: dict[str, Any]) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(api_keys_table.insert().values(**row))

    async def get_by_key_id(self, key_id: str) -> dict[str, Any] | None:
        async with self._engine.connect() as conn:
            result = await conn.execute(
                api_keys_table.select().where(api_keys_table.c.key_id == key_id)
            )
            row = result.mappings().first()
            return dict(row) if row else None

    async def deactivate(self, key_id: str) -> bool:
        async with self._engine.begin() as conn:
            result = await conn.execute(
                api_keys_table.update()
                .where(api_keys_table.c.key_id == key_id)
                .values(is_active=False)
            )
            return result.rowcount > 0

    async def update_last_used(self, key_id: str) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(
                api_keys_table.update()
                .where(api_keys_table.c.key_id == key_id)
                .values(last_used_at=datetime.now(timezone.utc))
            )

    async def list_keys(
        self, tenant_id: str, *, active_only: bool = True
    ) -> list[dict[str, Any]]:
        stmt = sa.select(*_LIST_COLUMNS).where(
            api_keys_table.c.tenant_id == uuid.UUID(tenant_id)
        )
        if active_only:
            stmt = stmt.where(api_keys_table.c.is_active.is_(True))
        stmt = stmt.order_by(api_keys_table.c.created_at.desc())

        async with self._engine.connect() as conn:
            result = await conn.execute(stmt)
            return [dict(r) for r in result.mappings().all()]

    async def insert_activity_log(self, row: dict[str, Any]) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(activity_log_table.insert().values(**row))

    async def list_activity_log(
        self,
        *,
        tenant_id: str | None = None,
        owner_id: str | None = None,
        key_id: str | None = None,
        action: str | None = None,
        entity_type: str | None = None,
        from_ts: datetime | None = None,
        to_ts: datetime | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        stmt = sa.select(activity_log_table).order_by(
            activity_log_table.c.timestamp.desc()
        )
        if tenant_id is not None:
            stmt = stmt.where(
                activity_log_table.c.tenant_id == uuid.UUID(tenant_id)
            )
        if owner_id is not None:
            stmt = stmt.where(activity_log_table.c.owner_id == owner_id)
        if key_id is not None:
            stmt = stmt.where(activity_log_table.c.key_id == key_id)
        if action is not None:
            stmt = stmt.where(activity_log_table.c.action == action)
        if entity_type is not None:
            stmt = stmt.where(activity_log_table.c.entity_type == entity_type)
        if from_ts is not None:
            stmt = stmt.where(activity_log_table.c.timestamp >= from_ts)
        if to_ts is not None:
            stmt = stmt.where(activity_log_table.c.timestamp <= to_ts)
        lim = max(1, min(int(limit), 500))
        off = max(0, int(offset))
        stmt = stmt.limit(lim).offset(off)

        async with self._engine.connect() as conn:
            result = await conn.execute(stmt)
            rows = []
            for r in result.mappings().all():
                d = dict(r)
                if d.get("tenant_id") is not None:
                    d["tenant_id"] = str(d["tenant_id"])
                if d.get("id") is not None:
                    d["id"] = str(d["id"])
                rows.append(d)
            return rows
