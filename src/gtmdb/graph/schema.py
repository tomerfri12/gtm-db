"""Neo4j constraints, indexes, and fulltext indexes.

All statements are idempotent (IF NOT EXISTS). Called once on startup
via ``GraphAdapter.bootstrap_schema()``.
"""

from __future__ import annotations

from neo4j import AsyncSession

NODE_LABELS = [
    "Org",
    "Account",
    "Contact",
    "Lead",
    "Deal",
    "Campaign",
    "EmailCampaign",
    "Ticket",
    "Insight",
    "Agent",
    "Tag",
    "Stage",
    "ICP",
    "Score",
    "Call",
    "Email",
    "Meeting",
    "Note",
    "Content",
    "AgentInteraction",
    "Actor",
]

# Actor uses composite uniqueness (tenant_id, id) so the same role id can exist per tenant.
_LABELS_ID_UNIQUE = [lb for lb in NODE_LABELS if lb != "Actor"]

CONSTRAINTS = [
    f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.id IS UNIQUE"
    for label in _LABELS_ID_UNIQUE
]

ACTOR_CONSTRAINTS = [
    "CREATE CONSTRAINT actor_tenant_id_id IF NOT EXISTS "
    "FOR (n:Actor) REQUIRE (n.tenant_id, n.id) IS UNIQUE",
]

INDEXES = [
    f"CREATE INDEX IF NOT EXISTS FOR (n:{label}) ON (n.tenant_id)"
    for label in NODE_LABELS
]

LOOKUP_INDEXES = [
    "CREATE INDEX IF NOT EXISTS FOR (n:Contact) ON (n.email)",
    "CREATE INDEX IF NOT EXISTS FOR (n:Account) ON (n.domain)",
    "CREATE INDEX IF NOT EXISTS FOR (n:Deal) ON (n.stage)",
    "CREATE INDEX IF NOT EXISTS FOR (n:Lead) ON (n.status)",
    "CREATE INDEX IF NOT EXISTS FOR (n:Campaign) ON (n.status)",
    "CREATE INDEX IF NOT EXISTS FOR (n:EmailCampaign) ON (n.status)",
    "CREATE INDEX IF NOT EXISTS FOR (n:AgentInteraction) ON (n.interaction_type)",
]

FULLTEXT_INDEXES = [
    (
        "CREATE FULLTEXT INDEX entity_search IF NOT EXISTS "
        "FOR (n:Account|Contact|Lead|Deal|Campaign) "
        "ON EACH [n.name, n.first_name, n.last_name, n.email, n.company_name]"
    ),
]


async def bootstrap(session: AsyncSession) -> None:
    """Run all schema statements. Safe to call repeatedly."""
    stmts = (
        CONSTRAINTS
        + ACTOR_CONSTRAINTS
        + INDEXES
        + LOOKUP_INDEXES
        + FULLTEXT_INDEXES
    )
    for stmt in stmts:
        await session.run(stmt)
