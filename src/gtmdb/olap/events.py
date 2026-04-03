"""GtmEvent — the canonical OLAP event row.

One ``GtmEvent`` maps 1-to-1 to one row in the ClickHouse ``events`` table.
All dimension columns default to safe empty values so callers only need to
set what they know — the rest stays blank and doesn't pollute aggregations.

Usage::

    from gtmdb.olap.events import GtmEvent
    from gtmdb.olap.enrichment import enrich_node

    event = await enrich_node(graph, scope, node_id="lead-123", label="Lead")
    await ch.insert_events([event.to_row()])
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _uid() -> str:
    return str(uuid.uuid4())


class GtmEvent(BaseModel):
    """Full OLAP event row — mirrors the ClickHouse ``events`` table exactly."""

    model_config = {"populate_by_name": True}

    # ------------------------------------------------------------------ #
    #  Primary identity                                                    #
    # ------------------------------------------------------------------ #
    event_id:       str      = Field(default_factory=_uid)
    tenant_id:      str      = ""
    event_type:     str      = ""   # e.g. "lead.created", "deal.stage_changed"
    event_category: str      = ""   # "lifecycle" | "pipeline" | "subscription" | "attribution"
    occurred_at:    datetime = Field(default_factory=_now)
    source_node_id: str      = ""
    source_label:   str      = ""
    related_node_id: str     = ""
    related_label:  str      = ""
    relation:       str      = ""   # edge type that triggered this event, if any
    actor_id:       str      = ""

    # ------------------------------------------------------------------ #
    #  Lead dimensions                                                     #
    # ------------------------------------------------------------------ #
    lead_id:         str   = ""
    lead_status:     str   = ""
    lead_source:     str   = ""
    lead_company:    str   = ""
    lead_domain:     str   = ""
    lead_score:      float = 0.0
    lead_is_signup:  int   = 0    # UInt8 — 0 or 1
    lead_signup_date: str  = ""

    # ------------------------------------------------------------------ #
    #  Contact dimensions                                                  #
    # ------------------------------------------------------------------ #
    contact_id:    str = ""
    contact_name:  str = ""
    contact_title: str = ""
    contact_dept:  str = ""
    contact_email: str = ""

    # ------------------------------------------------------------------ #
    #  Account dimensions                                                  #
    # ------------------------------------------------------------------ #
    account_id:        str   = ""
    account_name:      str   = ""
    account_domain:    str   = ""
    account_industry:  str   = ""
    account_type:      str   = ""
    account_employees: int   = 0
    account_arr:       float = 0.0

    # ------------------------------------------------------------------ #
    #  Campaign dimensions (first-touch when multi-touch exists)          #
    # ------------------------------------------------------------------ #
    campaign_id:       str   = ""
    campaign_name:     str   = ""
    campaign_channel:  str   = ""
    campaign_category: str   = ""
    campaign_status:   str   = ""
    campaign_budget:   float = 0.0

    # ------------------------------------------------------------------ #
    #  Channel dimensions                                                  #
    # ------------------------------------------------------------------ #
    channel_id:   str = ""
    channel_name: str = ""
    channel_type: str = ""

    # ------------------------------------------------------------------ #
    #  Deal dimensions                                                     #
    # ------------------------------------------------------------------ #
    deal_id:          str   = ""
    deal_name:        str   = ""
    deal_stage:       str   = ""
    deal_amount:      float = 0.0
    deal_probability: float = 0.0
    deal_owner_id:    str   = ""
    deal_close_date:  str   = ""

    # ------------------------------------------------------------------ #
    #  Subscription payload                                               #
    # ------------------------------------------------------------------ #
    sub_event_type:       str   = ""
    sub_plan_tier:        str   = ""
    sub_plan_period:      str   = ""
    sub_arr:              float = 0.0
    sub_days_from_signup: int   = 0
    sub_product_name:     str   = ""

    # ------------------------------------------------------------------ #
    #  Product & ProductAccount dimensions                                #
    # ------------------------------------------------------------------ #
    product_id:   str = ""
    product_name: str = ""
    product_type: str = ""

    product_account_id:         str = ""
    product_account_name:       str = ""
    product_account_region:     str = ""
    product_account_country:    str = ""
    product_account_industry:   str = ""
    product_account_size_group: str = ""
    product_account_is_paying:  int = 0  # UInt8

    # ------------------------------------------------------------------ #
    #  Visitor dimensions                                                  #
    # ------------------------------------------------------------------ #
    visitor_id:             str = ""
    visitor_channel:        str = ""
    visitor_signup_flow:    str = ""
    visitor_signup_cluster: str = ""
    visitor_dept:           str = ""
    visitor_seniority:      str = ""
    visitor_product_intent: str = ""
    visitor_team_size:      str = ""

    # ------------------------------------------------------------------ #
    #  Content dimensions                                                  #
    # ------------------------------------------------------------------ #
    content_id:   str = ""
    content_name: str = ""
    content_type: str = ""
    content_url:  str = ""

    # ------------------------------------------------------------------ #
    #  Overflow                                                            #
    # ------------------------------------------------------------------ #
    extra: dict[str, Any] = Field(default_factory=dict)

    # ------------------------------------------------------------------ #
    #  Serialisation                                                       #
    # ------------------------------------------------------------------ #

    def to_row(self) -> dict[str, Any]:
        """Return a flat dict suitable for ``ClickHouseClient.insert_events``."""
        d = self.model_dump()
        # Serialise the overflow dict to JSON string (CH column is String)
        d["extra"] = json.dumps(d["extra"], default=str) if d["extra"] else "{}"
        return d


# ---------------------------------------------------------------------------
# Category constants — kept here so enrichment.py and sync hooks stay DRY
# ---------------------------------------------------------------------------

CATEGORY_LIFECYCLE     = "lifecycle"
CATEGORY_PIPELINE      = "pipeline"
CATEGORY_SUBSCRIPTION  = "subscription"
CATEGORY_ATTRIBUTION   = "attribution"
CATEGORY_RELATIONSHIP  = "relationship"

# Map Neo4j label → default event_type / event_category when a node is created
NODE_EVENT_DEFAULTS: dict[str, tuple[str, str]] = {
    "Lead":              ("lead.created",              CATEGORY_LIFECYCLE),
    "Contact":           ("contact.created",           CATEGORY_LIFECYCLE),
    "Account":           ("account.created",           CATEGORY_LIFECYCLE),
    "Deal":              ("deal.created",              CATEGORY_PIPELINE),
    "Campaign":          ("campaign.created",          CATEGORY_LIFECYCLE),
    "Channel":           ("channel.created",           CATEGORY_LIFECYCLE),
    "SubscriptionEvent": ("subscription.created",      CATEGORY_SUBSCRIPTION),
    "ProductAccount":    ("product_account.created",   CATEGORY_LIFECYCLE),
    "Product":           ("product.created",           CATEGORY_LIFECYCLE),
    "Visitor":           ("visitor.created",           CATEGORY_ATTRIBUTION),
    "Content":           ("content.created",           CATEGORY_LIFECYCLE),
}

# Map Neo4j edge type → event_type / event_category when a relationship fires
EDGE_EVENT_DEFAULTS: dict[str, tuple[str, str]] = {
    "SOURCED_FROM":             ("lead.sourced_from_campaign",    CATEGORY_ATTRIBUTION),
    "CONVERTED_TO":             ("lead.converted_to_contact",     CATEGORY_LIFECYCLE),
    "WORKS_AT":                 ("contact.linked_to_account",     CATEGORY_LIFECYCLE),
    "INFLUENCED":               ("deal.influenced_by_campaign",   CATEGORY_ATTRIBUTION),
    "HAS_SUBSCRIPTION_EVENT":   ("subscription.linked",           CATEGORY_SUBSCRIPTION),
    "SIGNED_UP_AS":             ("lead.signed_up",                CATEGORY_LIFECYCLE),
    "FOR_PRODUCT":              ("subscription.for_product",      CATEGORY_SUBSCRIPTION),
    "TOUCHED":                  ("visitor.touched_campaign",      CATEGORY_ATTRIBUTION),
}
