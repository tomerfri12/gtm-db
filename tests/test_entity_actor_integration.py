"""Entity–actor stamping: properties + CREATED_BY / UPDATED_BY edges (live Neo4j, opt-in)."""

from __future__ import annotations

import os
import uuid

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("CRMDB_RUN_NEO4J_IT"),
    reason="Set CRMDB_RUN_NEO4J_IT=1 with Neo4j running (crmdb/docker-compose.yml)",
)


@pytest.mark.asyncio
async def test_lead_create_stamps_property_and_created_by_edge() -> None:
    from crmdb import CrmDB, Scope, create_token_from_presets
    from crmdb.api.models import ActorSpec

    db = CrmDB()
    await db.connect()

    tid = uuid.uuid4()
    owner = "entity_actor_it_owner"
    token = create_token_from_presets(
        tid, owner, "system", ["full_access"], label="entity-actor-it"
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
    assert lead.created_by_actor_id == owner
    assert lead.updated_by_actor_id is None

    recs = await db.execute_cypher(
        scope,
        (
            "MATCH (a:Actor {tenant_id: $tenant_id, id: $aid})"
            "-[r:CREATED_BY]->(l:Lead {tenant_id: $tenant_id, id: $lid}) "
            "RETURN type(r) AS t, properties(l) AS lp"
        ),
        {"aid": owner, "lid": lead.id},
    )
    assert len(recs) == 1
    assert recs[0]["t"] == "CREATED_BY"
    assert recs[0]["lp"]["created_by_actor_id"] == owner

    await db.close()


@pytest.mark.asyncio
async def test_lead_update_stamps_property_and_two_updated_by_edges() -> None:
    from crmdb import CrmDB, Scope, create_token_from_presets
    from crmdb.api.models import ActorSpec

    db = CrmDB()
    await db.connect()

    tid = uuid.uuid4()
    owner = "entity_actor_it_updater"
    token = create_token_from_presets(
        tid, owner, "system", ["full_access"], label="entity-actor-update-it"
    )
    scope = Scope(token)

    await db.actors.create(
        scope,
        [
            ActorSpec(
                id=owner,
                kind="service",
                display_name="Updater",
                role_key="system",
            )
        ],
    )

    lead = await db.leads.create(scope, company_name="Beta", first_name="Bob")
    await db.leads.update(scope, lead.id, status="qualified")
    await db.leads.update(scope, lead.id, status="won")

    updated = await db.leads.get(scope, lead.id)
    assert updated is not None
    assert updated.updated_by_actor_id == owner

    recs = await db.execute_cypher(
        scope,
        (
            "MATCH (a:Actor {tenant_id: $tenant_id, id: $aid})"
            "-[r:UPDATED_BY]->(l:Lead {tenant_id: $tenant_id, id: $lid}) "
            "RETURN r.at AS at ORDER BY r.at ASC"
        ),
        {"aid": owner, "lid": lead.id},
    )
    assert len(recs) == 2
    assert all(rec["at"] for rec in recs)

    await db.close()
