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
    """external_id is the source lead id (e.g. Salesforce LEAD_ID) for MERGE imports."""
    external_id: str | None = None
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
    lead_date: str | None = None
    is_signup: bool | None = None
    is_contact_sales: bool | None = None
    signup_date: str | None = None
    contact_sales_date: str | None = None


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
    campaign_category: str | None = None
    marketing_source: str | None = None
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
class ProductAccount(Entity):
    """Customer workspace / tenant in one product—distinct from the company Account."""

    external_id: str | None = None
    name: str | None = None
    status: str | None = None
    region: str | None = None
    country: str | None = None
    industry: str | None = None
    company_size_group: str | None = None
    company_size_num: str | None = None
    is_paying: str | None = None
    install_date: str | None = None
    first_product_install_date: str | None = None
    is_first_product: str | None = None
    first_account_channel: str | None = None
    is_cross_sell: str | None = None
    first_plan_tier: str | None = None
    first_plan_period: str | None = None
    first_product_arr: str | None = None
    first_product_arr_date: str | None = None
    first_account_arr_date: str | None = None
    first_churn_date: str | None = None
    days_to_pay: str | None = None
    survey_sector: str | None = None
    survey_sub_sector: str | None = None
    dep0_predicted_arr: str | None = None
    extra_predicted: str | None = None


@dataclass
class Content(Entity):
    """Marketing asset — landing page, blog post, whitepaper, case study, etc."""

    name: str | None = None
    url: str | None = None
    content_type: str | None = None
    status: str | None = None
    description: str | None = None


@dataclass
class Visitor(Entity):
    """Anonymous or identified site visitor; link to campaigns via TOUCHED."""

    visitor_id: str | None = None
    source_channel: str | None = None
    first_seen_at: str | None = None
    visitor_row_type: str | None = None
    device: str | None = None
    first_user_platform_language: str | None = None
    user_goal: str | None = None
    signup_flow: str | None = None
    raw_signup_flow: str | None = None
    signup_use_case: str | None = None
    signup_cluster: str | None = None
    product_intent: str | None = None
    seniority: str | None = None
    department: str | None = None
    job_role: str | None = None
    team_size: str | None = None


@dataclass
class SubscriptionEvent(Entity):
    """Signup, purchase, churn, or other subscription lifecycle event."""

    import_key: str | None = None
    event_type: str | None = None
    occurred_at: str | None = None
    plan_tier: str | None = None
    plan_period: str | None = None
    arr: float | None = None
    days_from_signup: int | None = None
    product_name: str | None = None


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
