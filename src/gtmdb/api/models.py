"""Typed entity dataclasses for the GtmDB CRUD API.

Each entity has explicit fields rather than a generic properties dict.
System-managed fields (id, tenant_id, created_at, updated_at,
created_by_actor_id, updated_by_actor_id) are set automatically by the
API / graph layer — callers provide only domain fields.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Entity:
    """Base for all CRM entity types."""

    id: str = ""
    tenant_id: str = ""
    created_at: str | None = None
    updated_at: str | None = None
    created_by_actor_id: str | None = None
    updated_by_actor_id: str | None = None


# -- Core CRM entities -------------------------------------------------------


@dataclass
class Lead(Entity):
    first_name: str | None = None
    last_name: str | None = None
    name: str | None = None
    email: str | None = None
    phone: str | None = None
    title: str | None = None
    company_name: str | None = None
    domain: str | None = None
    status: str = "new"
    source: str | None = None
    score: float | None = None
    linkedin_url: str | None = None
    snippet: str | None = None
    outreach_email: str | None = None


@dataclass
class Score(Entity):
    """Qualification score (e.g. BANT) linked to a Lead via HAS_SCORE."""

    name: str | None = None
    lead_id: str | None = None
    score_type: str = "bant"
    total: int = 0
    budget: int | None = None
    authority: int | None = None
    need: int | None = None
    timeline: int | None = None
    reasoning: str | None = None
    status: str | None = None
    scored_by: str | None = None


@dataclass
class Account(Entity):
    name: str | None = None
    domain: str | None = None
    industry: str | None = None
    employee_count: int | None = None
    annual_revenue: float | None = None
    website: str | None = None
    type: str | None = None


@dataclass
class Contact(Entity):
    name: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    email: str | None = None
    phone: str | None = None
    title: str | None = None
    company_name: str | None = None
    department: str | None = None
    linkedin_url: str | None = None


@dataclass
class Deal(Entity):
    name: str | None = None
    amount: float | None = None
    stage: str | None = None
    probability: float | None = None
    close_date: str | None = None
    description: str | None = None
    owner_id: str | None = None


@dataclass
class Campaign(Entity):
    name: str | None = None
    status: str | None = None
    channel: str | None = None
    budget: float | None = None
    start_date: str | None = None
    end_date: str | None = None
    description: str | None = None


@dataclass
class EmailCampaign(Campaign):
    """Marketing email program; graph label ``EmailCampaign``."""

    from_name: str | None = None
    from_email: str | None = None
    reply_to: str | None = None


# -- Marketing entities (Channel, Product, Content) --------------------------


@dataclass
class Channel(Entity):
    """Acquisition channel grouping campaigns (e.g. SEM, YouTube, Direct Sales)."""

    name: str | None = None
    channel_type: str | None = None
    status: str | None = None
    description: str | None = None
    budget: float | None = None


@dataclass
class Product(Entity):
    """Product or service being sold (e.g. CRM, Work Management)."""

    name: str | None = None
    sku: str | None = None
    product_type: str | None = None
    status: str | None = None
    description: str | None = None
    price: float | None = None


@dataclass
class Content(Entity):
    """Marketing asset — landing page, blog post, whitepaper, case study, etc."""

    name: str | None = None
    url: str | None = None
    content_type: str | None = None
    status: str | None = None
    description: str | None = None


# -- Communication events (Email, Call, Meeting, …) --------------------------


@dataclass
class Email(Entity):
    """Single email artifact (sequence step, draft, or sent record)."""

    name: str | None = None
    subject: str | None = None
    body: str | None = None
    from_name: str | None = None
    from_email: str | None = None
    reply_to: str | None = None
    state: str = "draft"
    sequence_number: int | None = None
    send_at: str | None = None


# -- Actors (parties that perform actions: AI agents, humans) -----------------


@dataclass
class ActorSpec:
    """Input for :meth:`gtmdb.api.actors.ActorsAPI.create` (batch upsert)."""

    id: str
    kind: str
    display_name: str | None = None
    role_key: str | None = None
    created_at: str | None = None


@dataclass
class Actor(Entity):
    """Graph node label ``Actor`` — aligned with token ``owner_id`` when kind is ``ai``."""

    kind: str = "ai"
    display_name: str | None = None
    role_key: str | None = None


# -- Relationship ------------------------------------------------------------


@dataclass
class Relationship:
    """A typed relationship (edge) between two nodes."""

    type: str
    from_id: str
    to_id: str
    properties: dict = field(default_factory=dict)


# -- API keys ----------------------------------------------------------------


@dataclass
class ApiKeyResult:
    """Returned on key create / rotate. ``raw_key`` is shown once."""

    raw_key: str
    key_id: str
    owner_id: str
    label: str = ""
    expires_at: str | None = None


@dataclass
class ApiKeyInfo:
    """Returned on key list. Never contains secrets."""

    key_id: str
    owner_id: str
    owner_type: str = "actor"
    label: str = ""
    is_active: bool = True
    expires_at: str | None = None
    created_at: str | None = None
    last_used_at: str | None = None
