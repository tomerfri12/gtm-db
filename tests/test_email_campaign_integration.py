"""EmailCampaign create_with_artifacts + Email CRUD (live Neo4j, opt-in)."""

from __future__ import annotations

import os
import uuid

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("CRMDB_RUN_NEO4J_IT"),
    reason="Set CRMDB_RUN_NEO4J_IT=1 with Neo4j running (crmdb/docker-compose.yml)",
)


@pytest.mark.asyncio
async def test_email_campaign_create_with_artifacts_creates_graph() -> None:
    from crmdb import CrmDB, Scope, create_token_from_presets
    from crmdb.api.models import ActorSpec

    db = CrmDB()
    await db.connect()

    tid = uuid.uuid4()
    owner = "email_campaign_it_owner"
    token = create_token_from_presets(
        tid, owner, "system", ["full_access"], label="email-campaign-it"
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

    lead = await db.leads.create(scope, company_name="Acme", first_name="Jane")

    out = await db.email_campaigns.create_with_artifacts(
        scope,
        name="Q1 Nurture",
        from_name="Marketing",
        from_email="mkt@example.com",
        emails=[
            {"subject": "Hello", "body": "First touch"},
            {"subject": "Follow up", "body": "Second touch"},
        ],
        lead_ids=[lead.id],
    )
    ec = out["campaign"]
    assert ec.name == "Q1 Nurture"
    assert ec.channel == "email"
    assert len(out["email_ids"]) == 2
    assert out["linked_lead_count"] == 1

    recs = await db.execute_cypher(
        scope,
        (
            "MATCH (ec:EmailCampaign {tenant_id: $tenant_id, id: $ecid})"
            "-[:HAS_EMAIL]->(e:Email {tenant_id: $tenant_id}) "
            "RETURN e.subject AS sub ORDER BY e.sequence_number ASC"
        ),
        {"ecid": ec.id},
    )
    assert [r["sub"] for r in recs] == ["Hello", "Follow up"]

    src = await db.execute_cypher(
        scope,
        (
            "MATCH (l:Lead {tenant_id: $tenant_id, id: $lid})"
            "-[:SOURCED_FROM]->(ec:EmailCampaign {tenant_id: $tenant_id, id: $ecid}) "
            "RETURN count(*) AS c"
        ),
        {"lid": lead.id, "ecid": ec.id},
    )
    assert src[0]["c"] == 1

    await db.close()


@pytest.mark.asyncio
async def test_emails_crud_smoke() -> None:
    from crmdb import CrmDB, Scope, create_token_from_presets
    from crmdb.api.models import ActorSpec

    db = CrmDB()
    await db.connect()

    tid = uuid.uuid4()
    owner = "emails_crud_owner"
    token = create_token_from_presets(
        tid, owner, "system", ["full_access"], label="emails-crud-it"
    )
    scope = Scope(token)

    await db.actors.create(
        scope,
        [
            ActorSpec(
                id=owner,
                kind="service",
                display_name="Owner",
                role_key="system",
            )
        ],
    )

    em = await db.emails.create(
        scope,
        subject="Standalone",
        body="Body text",
        state="draft",
    )
    got = await db.emails.get(scope, em.id)
    assert got is not None
    assert got.subject == "Standalone"
    assert got.body == "Body text"

    await db.close()
