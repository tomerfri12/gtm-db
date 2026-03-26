"""Bootstrap sample graph data for demos, tests, and CRM2 integration.

Requires a ``Scope`` with sufficient write permissions (e.g. ``full_access`` preset).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from gtmdb.types import EdgeData, NodeData

if TYPE_CHECKING:
    from gtmdb.client import GtmDB
    from gtmdb.scope import Scope


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
                "email": f"jane-{s}@acme.example",
                "company_name": "Acme Industries",
            },
        ),
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
                "email": f"jane-lead-{s}@acme.example",
                "company_name": "Acme Industries",
                "status": "qualified",
            },
        ),
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
    )
    ids["note"] = note.id

    await db.create_edge(scope, EdgeData("WORKS_AT", contact.id, acc.id))
    await db.create_edge(scope, EdgeData("WORKS_AT", lead.id, acc.id))
    await db.create_edge(scope, EdgeData("CONVERTED_TO", lead.id, contact.id))
    await db.create_edge(scope, EdgeData("SOURCED_FROM", lead.id, campaign.id))
    await db.create_edge(scope, EdgeData("BELONGS_TO", deal.id, acc.id))
    await db.create_edge(scope, EdgeData("HAS_CONTACT", deal.id, contact.id))
    await db.create_edge(scope, EdgeData("INFLUENCED", campaign.id, deal.id))
    await db.create_edge(scope, EdgeData("HAS_COMMUNICATION_EVENT", note.id, deal.id))

    return {"tenant_id": tid, "ids": ids}
