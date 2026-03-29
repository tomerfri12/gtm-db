"""Lead–campaign SOURCED_FROM edge (live Neo4j, opt-in)."""

from __future__ import annotations

import os
import uuid

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("GtmDB_RUN_NEO4J_IT"),
    reason="Set GtmDB_RUN_NEO4J_IT=1 with Neo4j running (gtmdb/docker-compose.yml)",
)


@pytest.mark.asyncio
async def test_link_lead_to_campaign_creates_sourced_from_edge() -> None:
    from gtmdb import GtmDB, Scope, create_token_from_presets
    from gtmdb.api.models import ActorSpec

    db = GtmDB()
    await db.connect()

    tid = uuid.uuid4()
    owner = "lead_campaign_it_owner"
    token = create_token_from_presets(
        tid, owner, "system", ["full_access"], label="lead-campaign-it"
    )
    scope = Scope(token)

    await db.actors.create(
        scope,
        [
            ActorSpec(
                id=owner,
                kind="service",
                display_name="IT Owner",
                role_key="system",
            )
        ],
    )

    campaign = await db.campaigns.create(
        scope, actor_id=owner, name="Q1 Webinar", channel="webinar"
    )
    lead = await db.leads.create(
        scope, actor_id=owner, company_name="Acme", first_name="Jane"
    )

    await db.leads.link_campaign(
        scope,
        lead.id,
        campaign.id,
        reasoning="Lead registered for webinar program",
    )

    recs = await db.execute_cypher(
        scope,
        (
            "MATCH (l:Lead {tenant_id: $tenant_id, id: $lid})"
            "-[r:SOURCED_FROM]->(c:Campaign {tenant_id: $tenant_id, id: $cid}) "
            "RETURN type(r) AS t"
        ),
        {"lid": lead.id, "cid": campaign.id},
    )
    assert len(recs) == 1
    assert recs[0]["t"] == "SOURCED_FROM"

    await db.close()
