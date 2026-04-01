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
    "Channel",
    "Product",
    "ProductAccount",
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
    "Visitor",
    "SubscriptionEvent",
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

# Import MERGE uses (tenant_id, external_id). If duplicates already exist
# (e.g. repeated CREATE before MERGE), drop or merge extras before bootstrap.
PRODUCT_ACCOUNT_CONSTRAINTS = [
    "CREATE CONSTRAINT productaccount_tenant_external IF NOT EXISTS "
    "FOR (n:ProductAccount) REQUIRE (n.tenant_id, n.external_id) IS UNIQUE",
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
    "CREATE INDEX IF NOT EXISTS FOR (n:Channel) ON (n.name)",
    "CREATE INDEX IF NOT EXISTS FOR (n:Product) ON (n.name)",
    "CREATE INDEX IF NOT EXISTS FOR (n:ProductAccount) ON (n.external_id)",
    "CREATE INDEX IF NOT EXISTS FOR (n:Content) ON (n.url)",
    "CREATE INDEX IF NOT EXISTS FOR (n:Visitor) ON (n.visitor_id)",
    "CREATE INDEX IF NOT EXISTS FOR (n:SubscriptionEvent) ON (n.event_type)",
    "CREATE INDEX IF NOT EXISTS FOR (n:AgentInteraction) ON (n.interaction_type)",
]

FULLTEXT_INDEXES = [
    (
        "CREATE FULLTEXT INDEX entity_search IF NOT EXISTS "
        "FOR (n:Account|Contact|Lead|Deal|Campaign|Channel|Product|ProductAccount|Content|Visitor|SubscriptionEvent) "
        "ON EACH [n.name, n.first_name, n.last_name, n.email, n.company_name, "
        "n.visitor_id, n.source_channel, n.product_name, n.external_id]"
    ),
]


async def bootstrap(session: AsyncSession) -> None:
    """Run all schema statements. Safe to call repeatedly."""
    stmts = (
        CONSTRAINTS
        + ACTOR_CONSTRAINTS
        + PRODUCT_ACCOUNT_CONSTRAINTS
        + INDEXES
        + LOOKUP_INDEXES
        + FULLTEXT_INDEXES
    )
    for stmt in stmts:
        await session.run(stmt)
