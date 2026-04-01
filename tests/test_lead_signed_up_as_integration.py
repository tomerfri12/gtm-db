"""Lead —SIGNED_UP_AS→ ProductAccount (live Neo4j, opt-in)."""

from __future__ import annotations

import os
import uuid

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("GtmDB_RUN_NEO4J_IT"),
    reason="Set GtmDB_RUN_NEO4J_IT=1 with Neo4j running (gtmdb/docker-compose.yml)",
)


@pytest.mark.asyncio
async def test_lead_sign_up_as_product_account_creates_edge() -> None:
    from gtmdb import GtmDB, Scope, create_token_from_presets
    from gtmdb.api.models import ActorSpec

    db = GtmDB()
    await db.connect()

    tid = uuid.uuid4()
    owner = "lead_signup_pa_it_owner"
    token = create_token_from_presets(
        tid, owner, "system", ["full_access"], label="lead-signup-pa-it"
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

    lead = await db.leads.create(
        scope, actor_id=owner, company_name="Acme", first_name="Jane"
    )
    pa = await db.product_accounts.create(
        scope,
        actor_id=owner,
        external_id=f"pulse-{uuid.uuid4().hex[:12]}",
        name="Workspace",
    )

    await db.leads.sign_up_as(
        scope,
        lead.id,
        pa.id,
        reasoning="Product trial signup mapped to product workspace",
    )

    recs = await db.execute_cypher(
        scope,
        (
            "MATCH (l:Lead {tenant_id: $tenant_id, id: $lid})"
            "-[r:SIGNED_UP_AS]->(pa:ProductAccount {tenant_id: $tenant_id, id: $paid}) "
            "RETURN type(r) AS t"
        ),
        {"lid": lead.id, "paid": pa.id},
    )
    assert len(recs) == 1
    assert recs[0]["t"] == "SIGNED_UP_AS"

    await db.close()
