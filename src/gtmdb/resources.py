"""Canonical GTM resource definitions for permissions and query guarding.

Single source of truth for Neo4j labels, ClickHouse column prefixes, and
explicit column lists used by :class:`~gtmdb.guard.QueryGuard` and prompts.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ResourceSchema:
    """One logical GTM entity: graph label + OLAP columns."""

    name: str
    col_prefix: str
    columns: tuple[str, ...]


RESOURCES: tuple[ResourceSchema, ...] = (
    ResourceSchema(
        "Campaign",
        "campaign",
        (
            "campaign_id",
            "campaign_name",
            "campaign_channel",
            "campaign_category",
            "campaign_status",
            "campaign_budget",
        ),
    ),
    ResourceSchema(
        "Channel",
        "channel",
        ("channel_id", "channel_name", "channel_type"),
    ),
    ResourceSchema(
        "Lead",
        "lead",
        (
            "lead_id",
            "lead_status",
            "lead_source",
            "lead_company",
            "lead_domain",
            "lead_score",
            "lead_is_signup",
            "lead_signup_date",
        ),
    ),
    ResourceSchema(
        "Contact",
        "contact",
        (
            "contact_id",
            "contact_name",
            "contact_title",
            "contact_dept",
            "contact_email",
        ),
    ),
    ResourceSchema(
        "Account",
        "account",
        (
            "account_id",
            "account_name",
            "account_domain",
            "account_industry",
            "account_type",
            "account_employees",
            "account_arr",
        ),
    ),
    ResourceSchema(
        "Deal",
        "deal",
        (
            "deal_id",
            "deal_name",
            "deal_stage",
            "deal_amount",
            "deal_probability",
            "deal_owner_id",
            "deal_close_date",
        ),
    ),
    ResourceSchema(
        "SubscriptionEvent",
        "sub",
        (
            "sub_event_type",
            "sub_plan_tier",
            "sub_plan_period",
            "sub_arr",
            "sub_days_from_signup",
            "sub_product_name",
        ),
    ),
    ResourceSchema(
        "Product",
        "product",
        ("product_id", "product_name", "product_type"),
    ),
    ResourceSchema(
        "ProductAccount",
        "product_account",
        (
            "product_account_id",
            "product_account_name",
            "product_account_region",
            "product_account_country",
            "product_account_industry",
            "product_account_size_group",
            "product_account_is_paying",
        ),
    ),
    ResourceSchema(
        "Visitor",
        "visitor",
        (
            "visitor_id",
            "visitor_channel",
            "visitor_signup_flow",
            "visitor_signup_cluster",
            "visitor_dept",
            "visitor_seniority",
            "visitor_product_intent",
            "visitor_team_size",
        ),
    ),
    ResourceSchema(
        "Content",
        "content",
        ("content_id", "content_name", "content_type", "content_url"),
    ),
)

RESOURCE_BY_NAME: dict[str, ResourceSchema] = {r.name: r for r in RESOURCES}

COLUMN_TO_RESOURCE: dict[str, ResourceSchema] = {
    col.lower(): r for r in RESOURCES for col in r.columns
}
