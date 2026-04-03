"""Tests for GtmEvent model and enrichment logic (Phase 1b).

Unit tests run always. Integration tests require both Neo4j and ClickHouse
(opt-in via env var) and exercise the full enrich_node + insert path.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone

import pytest

from gtmdb.olap.events import (
    CATEGORY_LIFECYCLE,
    CATEGORY_PIPELINE,
    CATEGORY_SUBSCRIPTION,
    EDGE_EVENT_DEFAULTS,
    NODE_EVENT_DEFAULTS,
    GtmEvent,
)
from gtmdb.olap.enrichment import _apply_campaign, _apply_lead, _apply_account


# ---------------------------------------------------------------------------
# Unit tests — no external dependencies
# ---------------------------------------------------------------------------

def test_gtm_event_defaults() -> None:
    e = GtmEvent(tenant_id="t1", event_type="lead.created", event_category="lifecycle")
    assert e.lead_id == ""
    assert e.campaign_id == ""
    assert e.lead_score == 0.0
    assert e.lead_is_signup == 0
    assert e.account_employees == 0
    assert e.sub_arr == 0.0
    assert e.extra == {}


def test_gtm_event_to_row_has_all_columns() -> None:
    from gtmdb.olap.schema import EVENTS_COLUMNS
    e = GtmEvent(tenant_id="t1", event_type="lead.created", event_category="lifecycle")
    row = e.to_row()
    missing = [c for c in EVENTS_COLUMNS if c not in row]
    assert not missing, f"Missing columns in to_row(): {missing}"


def test_gtm_event_to_row_extra_serialized() -> None:
    e = GtmEvent(
        tenant_id="t1",
        event_type="lead.created",
        event_category="lifecycle",
        extra={"foo": "bar", "num": 42},
    )
    row = e.to_row()
    assert isinstance(row["extra"], str)
    parsed = json.loads(row["extra"])
    assert parsed == {"foo": "bar", "num": 42}


def test_gtm_event_to_row_empty_extra() -> None:
    e = GtmEvent(tenant_id="t1", event_type="lead.created", event_category="lifecycle")
    row = e.to_row()
    assert row["extra"] == "{}"


def test_gtm_event_explicit_fields() -> None:
    e = GtmEvent(
        tenant_id="tenant-abc",
        event_type="deal.created",
        event_category="pipeline",
        source_node_id="deal-001",
        source_label="Deal",
        deal_id="deal-001",
        deal_name="Big Deal",
        deal_stage="proposal",
        deal_amount=50000.0,
        deal_probability=0.4,
        campaign_id="camp-001",
        campaign_name="Q1 Outbound",
        campaign_channel="outbound",
        account_id="acc-001",
        account_name="Acme",
    )
    row = e.to_row()
    assert row["deal_amount"] == 50000.0
    assert row["campaign_name"] == "Q1 Outbound"
    assert row["account_name"] == "Acme"


def test_node_event_defaults_coverage() -> None:
    expected_labels = {"Lead", "Contact", "Account", "Deal", "Campaign", "Channel",
                       "SubscriptionEvent", "ProductAccount", "Product", "Visitor", "Content"}
    assert expected_labels.issubset(NODE_EVENT_DEFAULTS.keys())


def test_edge_event_defaults_coverage() -> None:
    expected_edges = {"SOURCED_FROM", "CONVERTED_TO", "INFLUENCED", "TOUCHED"}
    assert expected_edges.issubset(EDGE_EVENT_DEFAULTS.keys())


def test_apply_lead_mapper() -> None:
    e = GtmEvent(tenant_id="t", event_type="x", event_category="y")
    _apply_lead(e, {
        "id": "lead-999",
        "status": "qualified",
        "source": "organic",
        "company_name": "Pied Piper",
        "domain": "piedpiper.com",
        "score": 77.5,
        "is_signup": True,
        "signup_date": "2026-01-15",
    })
    assert e.lead_id == "lead-999"
    assert e.lead_status == "qualified"
    assert e.lead_score == 77.5
    assert e.lead_is_signup == 1
    assert e.lead_domain == "piedpiper.com"


def test_apply_lead_mapper_none() -> None:
    e = GtmEvent(tenant_id="t", event_type="x", event_category="y")
    _apply_lead(e, None)
    assert e.lead_id == ""


def test_apply_campaign_mapper() -> None:
    e = GtmEvent(tenant_id="t", event_type="x", event_category="y")
    _apply_campaign(e, {
        "id": "camp-123",
        "name": "Spring Launch",
        "channel": "paid_search",
        "campaign_category": "demand_gen",
        "status": "active",
        "budget": 12000.0,
    })
    assert e.campaign_id == "camp-123"
    assert e.campaign_channel == "paid_search"
    assert e.campaign_budget == 12000.0


def test_apply_account_mapper_employee_count() -> None:
    e = GtmEvent(tenant_id="t", event_type="x", event_category="y")
    _apply_account(e, {"id": "acc-1", "name": "Corp", "employee_count": 500, "annual_revenue": 2e6})
    assert e.account_employees == 500
    assert e.account_arr == 2_000_000.0


# ---------------------------------------------------------------------------
# Integration tests — require Neo4j + ClickHouse
# ---------------------------------------------------------------------------

pytestmark_integration = pytest.mark.skipif(
    not os.environ.get("GTMDB_RUN_NEO4J_IT"),
    reason="Set GTMDB_RUN_NEO4J_IT=1 to run Neo4j+ClickHouse integration tests",
)


@pytestmark_integration
async def test_enrich_lead_node_roundtrip() -> None:
    """Create a Lead in Neo4j, enrich it, insert into ClickHouse, query back."""
    from gtmdb import GtmDB, create_token_from_presets
    from gtmdb.api.models import ActorSpec
    from gtmdb.config import GtmdbSettings
    from gtmdb.olap.client import ClickHouseClient
    from gtmdb.olap.enrichment import enrich_node
    from gtmdb.scope import Scope

    settings = GtmdbSettings()
    tid = uuid.uuid4()
    owner = "enrich-test-owner"
    token = create_token_from_presets(tid, owner, "system", ["full_access"])
    scope = Scope(token)

    db = GtmDB()
    await db.connect()

    await db.actors.create(scope, [ActorSpec(id=owner, kind="service", display_name="Test")])
    campaign = await db.campaigns.create(scope, actor_id=owner, name="Enrichment Test Camp", channel="SEM")
    lead = await db.leads.create(scope, actor_id=owner, company_name="Enrich Corp", first_name="Alice")
    await db.leads.link_campaign(scope, lead.id, campaign.id, reasoning="test enrichment")

    event = await enrich_node(
        db._graph,
        scope,
        node_id=lead.id,
        label="Lead",
    )

    assert event.lead_id == lead.id
    assert event.campaign_id == campaign.id
    assert event.campaign_name == "Enrichment Test Camp"
    assert event.tenant_id == str(tid)

    async with await ClickHouseClient.create(settings) as ch:
        await ch.bootstrap()
        n = await ch.insert_events([event.to_row()])
        assert n == 1

        rows = await ch.query(
            "SELECT lead_id, campaign_name FROM events WHERE event_id = {eid:String}",
            {"eid": event.event_id},
        )
        assert len(rows) == 1
        assert rows[0]["lead_id"] == lead.id
        assert rows[0]["campaign_name"] == "Enrichment Test Camp"

    await db.close()
