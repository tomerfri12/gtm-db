"""Integration tests against a live Neo4j (opt-in).

Set ``GtmDB_RUN_NEO4J_IT=1`` and start ``docker compose`` in ``gtmdb/``.
"""

from __future__ import annotations

import os
import uuid

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("GtmDB_RUN_NEO4J_IT"),
    reason="Set GtmDB_RUN_NEO4J_IT=1 with Neo4j running (gtmdb/docker-compose.yml)",
)


@pytest.mark.asyncio
async def test_seed_and_phase3_traversals() -> None:
    from gtmdb import GtmDB, Scope, create_token_from_presets
    from gtmdb.seed import seed_sample_graph

    db = GtmDB()
    await db.connect()

    tid = uuid.uuid4()
    suffix = str(tid)[:8]
    token = create_token_from_presets(
        tid, "it", "system", ["full_access"], label="integration"
    )
    scope = Scope(token)

    meta = await seed_sample_graph(db, scope, id_suffix=suffix)
    ids = meta["ids"]
    assert ids["deal"] == f"seed-deal-{suffix}"
    assert ids["lead"] == f"seed-lead-{suffix}"

    e360 = await db.entity_360(scope, "Deal", ids["deal"], max_depth=2)
    assert e360["center"] is not None
    assert e360["center"].properties.get("name") == "Enterprise Plan"
    assert len(e360["connected"]) >= 1

    tl = await db.timeline(scope, ids["deal"], limit=10)
    assert any(
        n["node"].label == "Note" or "note" in n["node"].label.lower()
        for n in tl
    )

    pipe = await db.pipeline(scope, stage="negotiation", limit=50)
    assert any(d.id == ids["deal"] for d in pipe)

    attr = await db.campaign_attribution(scope, deal_id=ids["deal"], limit=10)
    assert len(attr) >= 1
    assert attr[0]["campaign"] is not None
    assert attr[0]["deal"] is not None

    path = await db.path_finding(scope, ids["contact"], ids["deal"], max_hops=15)
    assert path is not None
    assert len(path["nodes"]) >= 2

    path_campaign = await db.path_finding(
        scope, ids["contact"], ids["campaign"], max_hops=15
    )
    assert path_campaign is not None
    assert len(path_campaign["nodes"]) >= 2

    hits = await db.search(scope, "Acme", limit=5)
    assert len(hits) >= 1

    await db.close()


@pytest.mark.asyncio
async def test_pipeline_denies_without_deal_read() -> None:
    import json

    from gtmdb import GtmDB, Scope
    from gtmdb.tokens import AccessToken

    db = GtmDB()
    await db.connect()

    token = AccessToken(
        tenant_id=uuid.uuid4(),
        owner_id="m",
        owner_type="user",
        policies=json.dumps(
            [
                {
                    "effect": "allow",
                    "actions": ["read"],
                    "resources": ["contact", "account"],
                    "conditions": {},
                }
            ]
        ),
    )
    scope = Scope(token)
    assert await db.pipeline(scope, limit=5) == []
    await db.close()
