"""ActorsAPI integration tests (live Neo4j, opt-in)."""

from __future__ import annotations

import os
import uuid

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("CRMDB_RUN_NEO4J_IT"),
    reason="Set CRMDB_RUN_NEO4J_IT=1 with Neo4j running (crmdb/docker-compose.yml)",
)


@pytest.mark.asyncio
async def test_actors_create_idempotent_and_updates_display_name() -> None:
    from crmdb import CrmDB, Scope, create_token_from_presets
    from crmdb.api.models import ActorSpec

    db = CrmDB()
    await db.connect()

    tid = uuid.uuid4()
    token = create_token_from_presets(
        tid, "it", "system", ["full_access"], label="actors-it"
    )
    scope = Scope(token)

    specs = [
        ActorSpec(
            id="test_actor_role",
            kind="ai",
            display_name="First Name",
            role_key="test_actor_role",
        )
    ]

    await db.actors.create(scope, specs)
    await db.actors.create(scope, specs)

    recs = await db.execute_cypher(
        scope,
        "MATCH (a:Actor {tenant_id: $tenant_id, id: $aid}) RETURN properties(a) AS props",
        {"aid": "test_actor_role"},
    )
    assert len(recs) == 1
    props = recs[0]["props"]
    assert props["id"] == "test_actor_role"
    assert props["kind"] == "ai"
    assert props["display_name"] == "First Name"
    assert props["role_key"] == "test_actor_role"
    assert "created_at" in props

    specs2 = [
        ActorSpec(
            id="test_actor_role",
            kind="ai",
            display_name="Updated Name",
            role_key="test_actor_role",
        )
    ]
    await db.actors.create(scope, specs2)

    recs2 = await db.execute_cypher(
        scope,
        "MATCH (a:Actor {tenant_id: $tenant_id, id: $aid}) RETURN properties(a) AS props",
        {"aid": "test_actor_role"},
    )
    assert recs2[0]["props"]["display_name"] == "Updated Name"
    assert recs2[0]["props"]["created_at"] == props["created_at"]

    count = await db.execute_cypher(
        scope,
        "MATCH (a:Actor {tenant_id: $tenant_id, id: $aid}) RETURN count(a) AS c",
        {"aid": "test_actor_role"},
    )
    assert count[0]["c"] == 1

    await db.close()
