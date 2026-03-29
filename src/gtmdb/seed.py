"""Bootstrap sample graph data for demos, tests, and CRM2 integration.

Requires a ``Scope`` with sufficient write permissions (e.g. ``full_access`` preset).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from gtmdb.api.models import ActorSpec
from gtmdb.types import EdgeData, NodeData

if TYPE_CHECKING:
    from gtmdb.client import GtmDB
    from gtmdb.scope import Scope

_SEED_ACTOR_ID = "seed-bootstrap"


async def seed_sample_graph(
    db: "GtmDB",
    scope: "Scope",
    *,
    id_suffix: str = "1",
) -> dict[str, Any]:
    """Create a small CRM-shaped graph: Org, Account, Lead, Contact, Deal, Campaign, Note.

    ``id_suffix`` makes node ids unique across runs (default ``"1"`` for demos).

    Returns a dict of stable string keys to created node ids for assertions / tools.
    """
    tid = scope.tenant_id
    ids: dict[str, str] = {}
    s = id_suffix

    await db.actors.create(
        scope,
        [
            ActorSpec(
                id=_SEED_ACTOR_ID,
                kind="ai",
                display_name="Seed bootstrap",
                role_key="seed-bootstrap",
            )
        ],
    )

    org = await db.create_node(
        scope,
        NodeData(
            "Org",
            "",
            tid,
            {
                "name": "Demo Org",
                "domain": "demo.example",
                "industry": "Software",
            },
        ),
        actor_id=_SEED_ACTOR_ID,
        reasoning="Seed demo organization",
    )
    ids["org"] = org.id

    acc = await db.create_node(
        scope,
        NodeData(
            "Account",
            f"seed-account-{s}",
            tid,
            {"name": "Acme Industries", "domain": "acme.example"},
        ),
        actor_id=_SEED_ACTOR_ID,
        reasoning="Seed demo account",
    )
    ids["account"] = acc.id

    contact = await db.create_node(
        scope,
        NodeData(
            "Contact",
            f"seed-contact-{s}",
            tid,
            {
                "first_name": "Jane",
                "last_name": "Doe",
                "name": "Jane Doe",
                "email": f"jane-{s}@acme.example",
                "company_name": "Acme Industries",
            },
        ),
        actor_id=_SEED_ACTOR_ID,
        reasoning="Seed demo contact",
    )
    ids["contact"] = contact.id

    lead = await db.create_node(
        scope,
        NodeData(
            "Lead",
            f"seed-lead-{s}",
            tid,
            {
                "first_name": "Jane",
                "last_name": "Doe",
                "name": "Jane Doe",
                "email": f"jane-lead-{s}@acme.example",
                "company_name": "Acme Industries",
                "status": "qualified",
            },
        ),
        actor_id=_SEED_ACTOR_ID,
        reasoning="Seed demo lead",
    )
    ids["lead"] = lead.id

    deal = await db.create_node(
        scope,
        NodeData(
            "Deal",
            f"seed-deal-{s}",
            tid,
            {
                "name": "Enterprise Plan",
                "amount": 120000,
                "stage": "negotiation",
                "probability": 0.7,
            },
        ),
        actor_id=_SEED_ACTOR_ID,
        reasoning="Seed demo deal",
    )
    ids["deal"] = deal.id

    campaign = await db.create_node(
        scope,
        NodeData(
            "Campaign",
            f"seed-campaign-{s}",
            tid,
            {
                "name": "Q1 Outbound",
                "status": "active",
                "channel": "email",
            },
        ),
        actor_id=_SEED_ACTOR_ID,
        reasoning="Seed demo campaign",
    )
    ids["campaign"] = campaign.id

    note = await db.create_node(
        scope,
        NodeData(
            "Note",
            f"seed-note-{s}",
            tid,
            {
                "title": "Discovery call",
                "body": "Discussed security requirements.",
                "created_at": "2025-01-15T10:00:00Z",
            },
        ),
        actor_id=_SEED_ACTOR_ID,
        reasoning="Seed demo note",
    )
    ids["note"] = note.id

    await db.create_edge(
        scope,
        EdgeData(
            "WORKS_AT",
            contact.id,
            acc.id,
            reasoning="Seed: contact employed at account",
        ),
    )
    await db.create_edge(
        scope,
        EdgeData(
            "WORKS_AT",
            lead.id,
            acc.id,
            reasoning="Seed: lead works at account",
        ),
    )
    await db.create_edge(
        scope,
        EdgeData(
            "CONVERTED_TO",
            lead.id,
            contact.id,
            reasoning="Seed: lead converted to contact",
        ),
    )
    await db.create_edge(
        scope,
        EdgeData(
            "SOURCED_FROM",
            lead.id,
            campaign.id,
            reasoning="Seed: lead attributed to campaign",
        ),
    )
    await db.create_edge(
        scope,
        EdgeData(
            "BELONGS_TO",
            deal.id,
            acc.id,
            reasoning="Seed: deal on account",
        ),
    )
    await db.create_edge(
        scope,
        EdgeData(
            "HAS_CONTACT",
            deal.id,
            contact.id,
            reasoning="Seed: contact on deal",
        ),
    )
    await db.create_edge(
        scope,
        EdgeData(
            "INFLUENCED",
            campaign.id,
            deal.id,
            reasoning="Seed: campaign influenced deal",
        ),
    )
    await db.create_edge(
        scope,
        EdgeData(
            "HAS_COMMUNICATION_EVENT",
            note.id,
            deal.id,
            reasoning="Seed: note on deal timeline",
        ),
    )

    return {"tenant_id": tid, "ids": ids}
