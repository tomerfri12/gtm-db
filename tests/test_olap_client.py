"""Integration tests for the ClickHouse OLAP client (Phase 1a).

Requires a running ClickHouse instance.  In CI the docker-compose service
is expected to be up (see docker-compose.yml).  Tests are skipped when
the server is unreachable rather than failing hard.

Run with:
    pytest tests/test_olap_client.py -v
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest

from gtmdb.config import GtmdbSettings
from gtmdb.olap.client import ClickHouseClient
from gtmdb.olap.schema import EVENTS_COLUMNS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _make_client() -> ClickHouseClient:
    settings = GtmdbSettings()
    client = await ClickHouseClient.create(settings)
    reachable = await client.ping()
    if not reachable:
        await client.close()
        pytest.skip("ClickHouse not reachable — skipping OLAP tests")
    await client.bootstrap()
    return client


# ---------------------------------------------------------------------------
# Tests  (asyncio_mode = "auto" in pyproject.toml picks these up automatically)
# ---------------------------------------------------------------------------

async def test_ping() -> None:
    client = await _make_client()
    try:
        assert await client.ping() is True
    finally:
        await client.close()


async def test_bootstrap_idempotent() -> None:
    """Calling bootstrap twice must not raise."""
    client = await _make_client()
    try:
        await client.bootstrap()
    finally:
        await client.close()


async def test_events_table_exists() -> None:
    client = await _make_client()
    try:
        rows = await client.query(
            "SELECT name FROM system.tables "
            "WHERE database = {db:String} AND name = 'events'",
            {"db": client._database},
        )
        assert rows, "events table was not created"
        assert rows[0]["name"] == "events"
    finally:
        await client.close()


async def test_column_count() -> None:
    client = await _make_client()
    try:
        rows = await client.query(
            "SELECT count() AS cnt FROM system.columns "
            "WHERE database = {db:String} AND table = 'events'",
            {"db": client._database},
        )
        col_count = int(rows[0]["cnt"])
        assert col_count == len(EVENTS_COLUMNS), (
            f"Column count mismatch: table has {col_count}, "
            f"schema lists {len(EVENTS_COLUMNS)}"
        )
    finally:
        await client.close()


async def test_insert_and_query() -> None:
    client = await _make_client()
    try:
        tenant = "test-tenant-olap"
        event_id = str(uuid.uuid4())

        row = {
            "event_id": event_id,
            "tenant_id": tenant,
            "event_type": "lead.created",
            "event_category": "lifecycle",
            "occurred_at": datetime.now(timezone.utc),
            "source_node_id": "lead-001",
            "source_label": "Lead",
            "lead_id": "lead-001",
            "lead_status": "new",
            "lead_source": "organic",
            "lead_company": "Acme Corp",
            "lead_domain": "acme.com",
            "lead_score": 42.5,
            "lead_is_signup": 1,
            "campaign_id": "camp-001",
            "campaign_name": "Spring Launch",
            "campaign_channel": "SEM",
            "campaign_category": "paid",
            "channel_id": "ch-001",
            "channel_name": "Google Ads",
            "channel_type": "paid_search",
        }

        n = await client.insert_events([row])
        assert n == 1

        result = await client.query(
            "SELECT event_id, tenant_id, lead_status, campaign_name FROM events "
            "WHERE event_id = {eid:String}",
            {"eid": event_id},
        )
        assert len(result) == 1
        r = result[0]
        assert r["event_id"] == event_id
        assert r["tenant_id"] == tenant
        assert r["lead_status"] == "new"
        assert r["campaign_name"] == "Spring Launch"
    finally:
        await client.close()


async def test_insert_batch() -> None:
    client = await _make_client()
    try:
        tenant = f"test-tenant-batch-{uuid.uuid4().hex[:8]}"
        rows = [
            {
                "event_id": str(uuid.uuid4()),
                "tenant_id": tenant,
                "event_type": "subscription.created",
                "event_category": "subscription",
                "occurred_at": datetime.now(timezone.utc),
                "sub_arr": 1200.0,
                "sub_plan_tier": "pro",
                "sub_plan_period": "annual",
            }
            for _ in range(5)
        ]
        n = await client.insert_events(rows)
        assert n == 5

        result = await client.query(
            "SELECT count() AS cnt FROM events WHERE tenant_id = {tid:String}",
            {"tid": tenant},
        )
        assert int(result[0]["cnt"]) == 5
    finally:
        await client.close()


async def test_insert_empty() -> None:
    client = await _make_client()
    try:
        n = await client.insert_events([])
        assert n == 0
    finally:
        await client.close()


async def test_query_one() -> None:
    client = await _make_client()
    try:
        result = await client.query_one("SELECT 1 AS val")
        assert result is not None
        assert int(result["val"]) == 1
    finally:
        await client.close()
