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
from gtmdb.olap.schema import EVENTS_COLUMNS
from gtmdb.olap.store import OlapStore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _make_client() -> OlapStore:
    settings = GtmdbSettings()
    store = await OlapStore.create(settings)
    reachable = await store.ping()
    if not reachable:
        await store.close()
        pytest.skip("OLAP store not reachable — skipping OLAP tests")
    await store.bootstrap()
    return store


# ---------------------------------------------------------------------------
# Tests  (asyncio_mode = "auto" in pyproject.toml picks these up automatically)
# ---------------------------------------------------------------------------

async def test_ping() -> None:
    store = await _make_client()
    try:
        assert await store.ping() is True
    finally:
        await store.close()


async def test_bootstrap_idempotent() -> None:
    """Calling bootstrap twice must not raise."""
    store = await _make_client()
    try:
        await store.bootstrap()
    finally:
        await store.close()


async def test_events_table_exists() -> None:
    store = await _make_client()
    try:
        rows = await store.query(
            "SELECT name FROM system.tables "
            "WHERE database = {db:String} AND name = 'events'",
            {"db": store._impl._database},
        )
        assert rows, "events table was not created"
        assert rows[0]["name"] == "events"
    finally:
        await store.close()


async def test_column_count() -> None:
    store = await _make_client()
    try:
        rows = await store.query(
            "SELECT count() AS cnt FROM system.columns "
            "WHERE database = {db:String} AND table = 'events'",
            {"db": store._impl._database},
        )
        col_count = int(rows[0]["cnt"])
        assert col_count == len(EVENTS_COLUMNS), (
            f"Column count mismatch: table has {col_count}, "
            f"schema lists {len(EVENTS_COLUMNS)}"
        )
    finally:
        await store.close()


async def test_insert_and_query() -> None:
    store = await _make_client()
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

        n = await store.insert_events([row])
        assert n == 1

        result = await store.query(
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
        await store.close()


async def test_insert_batch() -> None:
    store = await _make_client()
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
        n = await store.insert_events(rows)
        assert n == 5

        result = await store.query(
            "SELECT count() AS cnt FROM events WHERE tenant_id = {tid:String}",
            {"tid": tenant},
        )
        assert int(result[0]["cnt"]) == 5
    finally:
        await store.close()


async def test_insert_empty() -> None:
    store = await _make_client()
    try:
        n = await store.insert_events([])
        assert n == 0
    finally:
        await store.close()


async def test_query_one() -> None:
    store = await _make_client()
    try:
        result = await store.query_one("SELECT 1 AS val")
        assert result is not None
        assert int(result["val"]) == 1
    finally:
        await store.close()
