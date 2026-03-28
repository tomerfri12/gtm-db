"""Edge ``reasoning`` on relationships (optional Neo4j integration)."""

from __future__ import annotations

import os
import uuid

import pytest

from gtmdb import GtmDB, GtmdbSettings
from gtmdb.api.models import ActorSpec
from gtmdb.presets import create_token_from_presets
from gtmdb.scope import Scope
from gtmdb.types import EdgeData

pytestmark = pytest.mark.skipif(
    not os.environ.get("GtmDB_RUN_NEO4J_IT"),
    reason="Set GtmDB_RUN_NEO4J_IT=1 with Neo4j running (gtmdb/docker-compose.yml)",
)


@pytest.mark.asyncio
async def test_reasoning_stored_and_visible_in_relationship_list() -> None:
    db = GtmDB()
    await db.connect()
    tid = uuid.UUID(GtmdbSettings().default_tenant_id)
    owner = "edge_reasoning_it_owner"
    scope = Scope(
        create_token_from_presets(
            tid, owner, "system", ["full_access"], label="edge-reasoning-it"
        )
    )
    await db.actors.create(
        scope,
        [
            ActorSpec(
                id=owner,
                kind="service",
                display_name="Reasoning IT",
                role_key="system",
            )
        ],
    )

    acc = await db.accounts.create(
        scope, actor_id=owner, name="Reasoning Test Account"
    )
    lead = await db.leads.create(
        scope,
        actor_id=owner,
        email="reason-test@example.com",
        company_name="Co",
    )
    await db.leads.assign_to_account(
        scope, lead.id, acc.id, reasoning="Matched by domain enrichment"
    )

    rels = await db.relationships.list(scope, lead.id, rel_type="WORKS_AT", direction="out")
    await db.close()

    assert rels, "expected WORKS_AT from lead"
    r = rels[0]
    assert r.properties.get("reasoning") == "Matched by domain enrichment"


@pytest.mark.asyncio
async def test_create_edge_reasoning_on_edge_data() -> None:
    db = GtmDB()
    await db.connect()
    tid = uuid.UUID(GtmdbSettings().default_tenant_id)
    owner = "edge_reasoning_it_owner2"
    scope = Scope(
        create_token_from_presets(
            tid, owner, "system", ["full_access"], label="edge-reasoning-it2"
        )
    )
    await db.actors.create(
        scope,
        [
            ActorSpec(
                id=owner,
                kind="service",
                display_name="Reasoning IT2",
                role_key="system",
            )
        ],
    )

    a = await db.accounts.create(scope, actor_id=owner, name="R2 A")
    b = await db.accounts.create(scope, actor_id=owner, name="R2 B")
    await db.create_edge(
        scope,
        EdgeData("TAGGED", a.id, b.id, reasoning="manual link for test"),
    )
    rels = await db.relationships.list(scope, a.id, rel_type="TAGGED", direction="out")
    await db.close()

    assert rels and rels[0].properties.get("reasoning") == "manual link for test"
