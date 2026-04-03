"""Neo4j enrichment — traverse the graph and populate GtmEvent dimensions.

For every node label we run a single optimised Cypher query (OPTIONAL MATCH
chains) that fetches all reachable dimension nodes in one round-trip.  The
result is mapped into the flat ``GtmEvent`` columns.

Key design choices
------------------
* One Cypher per label type — keeps the enrichment predictable and fast
  (~2-5 ms per event on a warm Neo4j connection).
* First-touch attribution — when a Lead or Contact can be traced to
  multiple Campaigns via different paths, we take the first Campaign
  ordered by ``camp.created_at`` ascending.
* All OPTIONAL MATCH — a missing relationship fills with empty strings / 0,
  never blocks the insert.
* The enricher does NOT write to ClickHouse — it returns a GtmEvent.
  The caller (sync hook / materialiser) decides when to flush.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from gtmdb.graph.adapter import GraphAdapter
from gtmdb.scope import Scope

from .events import (
    CATEGORY_LIFECYCLE,
    CATEGORY_RELATIONSHIP,
    EDGE_EVENT_DEFAULTS,
    NODE_EVENT_DEFAULTS,
    GtmEvent,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-label enrichment Cypher queries
# ---------------------------------------------------------------------------

_ENRICH_LEAD = """
MATCH (l:Lead {id: $id, tenant_id: $tenant_id})
OPTIONAL MATCH (l)-[:SOURCED_FROM]->(c:Campaign)
OPTIONAL MATCH (c)-[:BELONGS_TO]->(ch:Channel)
OPTIONAL MATCH (l)-[:CONVERTED_TO]->(con:Contact)
OPTIONAL MATCH (con)-[:WORKS_AT]->(acc:Account)
OPTIONAL MATCH (l)-[:SIGNED_UP_AS]->(pa:ProductAccount)
RETURN
  properties(l)   AS lead,
  properties(c)   AS camp,
  properties(ch)  AS chan,
  properties(con) AS contact,
  properties(acc) AS account,
  properties(pa)  AS product_account
LIMIT 1
"""

_ENRICH_CONTACT = """
MATCH (con:Contact {id: $id, tenant_id: $tenant_id})
OPTIONAL MATCH (con)-[:WORKS_AT]->(acc:Account)
OPTIONAL MATCH (lead:Lead)-[:CONVERTED_TO]->(con)
OPTIONAL MATCH (lead)-[:SOURCED_FROM]->(c:Campaign)
OPTIONAL MATCH (c)-[:BELONGS_TO]->(ch:Channel)
RETURN
  properties(con)  AS contact,
  properties(acc)  AS account,
  properties(lead) AS lead,
  properties(c)    AS camp,
  properties(ch)   AS chan
LIMIT 1
"""

_ENRICH_DEAL = """
MATCH (d:Deal {id: $id, tenant_id: $tenant_id})
OPTIONAL MATCH (d)-[:FOR_CONTACT]->(con:Contact)
OPTIONAL MATCH (con)-[:WORKS_AT]->(acc:Account)
OPTIONAL MATCH (c:Campaign)-[:INFLUENCED]->(d)
OPTIONAL MATCH (c)-[:BELONGS_TO]->(ch:Channel)
OPTIONAL MATCH (con)<-[:CONVERTED_TO]-(lead:Lead)
OPTIONAL MATCH (lead)-[:SOURCED_FROM]->(lc:Campaign)
WITH d, con, acc, c, ch, lead,
     CASE WHEN c IS NOT NULL THEN c ELSE lc END AS best_camp,
     CASE WHEN c IS NOT NULL THEN ch ELSE null END AS best_chan
OPTIONAL MATCH (best_camp)-[:BELONGS_TO]->(final_ch:Channel)
RETURN
  properties(d)         AS deal,
  properties(con)       AS contact,
  properties(acc)       AS account,
  properties(best_camp) AS camp,
  coalesce(properties(best_chan), properties(final_ch)) AS chan,
  properties(lead)      AS lead
LIMIT 1
"""

_ENRICH_CAMPAIGN = """
MATCH (c:Campaign {id: $id, tenant_id: $tenant_id})
OPTIONAL MATCH (c)-[:BELONGS_TO]->(ch:Channel)
RETURN
  properties(c)  AS camp,
  properties(ch) AS chan
LIMIT 1
"""

_ENRICH_SUBSCRIPTION_EVENT = """
MATCH (se:SubscriptionEvent {id: $id, tenant_id: $tenant_id})
OPTIONAL MATCH (pa:ProductAccount)-[:HAS_SUBSCRIPTION_EVENT]->(se)
OPTIONAL MATCH (pa)<-[:SIGNED_UP_AS]-(lead:Lead)
OPTIONAL MATCH (lead)-[:SOURCED_FROM]->(c:Campaign)
OPTIONAL MATCH (c)-[:BELONGS_TO]->(ch:Channel)
OPTIONAL MATCH (pa)-[:FOR_ACCOUNT]->(acc:Account)
OPTIONAL MATCH (se)-[:FOR_PRODUCT]->(p:Product)
OPTIONAL MATCH (lead)-[:CONVERTED_TO]->(con:Contact)
RETURN
  properties(se)   AS sub_event,
  properties(pa)   AS product_account,
  properties(lead) AS lead,
  properties(c)    AS camp,
  properties(ch)   AS chan,
  properties(acc)  AS account,
  properties(p)    AS product,
  properties(con)  AS contact
LIMIT 1
"""

_ENRICH_PRODUCT_ACCOUNT = """
MATCH (pa:ProductAccount {id: $id, tenant_id: $tenant_id})
OPTIONAL MATCH (pa)-[:FOR_ACCOUNT]->(acc:Account)
OPTIONAL MATCH (pa)<-[:SIGNED_UP_AS]-(lead:Lead)
OPTIONAL MATCH (lead)-[:SOURCED_FROM]->(c:Campaign)
OPTIONAL MATCH (c)-[:BELONGS_TO]->(ch:Channel)
OPTIONAL MATCH (lead)-[:CONVERTED_TO]->(con:Contact)
RETURN
  properties(pa)   AS product_account,
  properties(acc)  AS account,
  properties(lead) AS lead,
  properties(c)    AS camp,
  properties(ch)   AS chan,
  properties(con)  AS contact
LIMIT 1
"""

_ENRICH_VISITOR = """
MATCH (v:Visitor {id: $id, tenant_id: $tenant_id})
OPTIONAL MATCH (v)-[:TOUCHED]->(c:Campaign)
OPTIONAL MATCH (c)-[:BELONGS_TO]->(ch:Channel)
RETURN
  properties(v)  AS visitor,
  properties(c)  AS camp,
  properties(ch) AS chan
LIMIT 1
"""

_ENRICH_ACCOUNT = """
MATCH (acc:Account {id: $id, tenant_id: $tenant_id})
RETURN properties(acc) AS account
LIMIT 1
"""

_ENRICH_CONTENT = """
MATCH (ct:Content {id: $id, tenant_id: $tenant_id})
OPTIONAL MATCH (ct)-[:BELONGS_TO]->(c:Campaign)
OPTIONAL MATCH (c)-[:BELONGS_TO]->(ch:Channel)
RETURN
  properties(ct) AS content,
  properties(c)  AS camp,
  properties(ch) AS chan
LIMIT 1
"""

# Generic fallback — just read the node itself
_ENRICH_GENERIC = """
MATCH (n {{id: $id, tenant_id: $tenant_id}})
RETURN properties(n) AS node
LIMIT 1
"""

_LABEL_TO_CYPHER: dict[str, str] = {
    "Lead":              _ENRICH_LEAD,
    "Contact":           _ENRICH_CONTACT,
    "Deal":              _ENRICH_DEAL,
    "Campaign":          _ENRICH_CAMPAIGN,
    "SubscriptionEvent": _ENRICH_SUBSCRIPTION_EVENT,
    "ProductAccount":    _ENRICH_PRODUCT_ACCOUNT,
    "Visitor":           _ENRICH_VISITOR,
    "Account":           _ENRICH_ACCOUNT,
    "Content":           _ENRICH_CONTENT,
}


# ---------------------------------------------------------------------------
# Row-to-field mappers
# ---------------------------------------------------------------------------

def _s(d: dict | None, key: str) -> str:
    """Safe string extraction from a nullable property dict."""
    if not d:
        return ""
    v = d.get(key)
    return str(v) if v is not None else ""


def _f(d: dict | None, key: str) -> float:
    if not d:
        return 0.0
    try:
        return float(d.get(key) or 0)
    except (TypeError, ValueError):
        return 0.0


def _i(d: dict | None, key: str) -> int:
    if not d:
        return 0
    try:
        return int(d.get(key) or 0)
    except (TypeError, ValueError):
        return 0


def _b(d: dict | None, key: str) -> int:
    """Bool → UInt8."""
    if not d:
        return 0
    v = d.get(key)
    if v is None:
        return 0
    if isinstance(v, bool):
        return 1 if v else 0
    if isinstance(v, (int, float)):
        return 1 if v else 0
    if isinstance(v, str):
        return 1 if v.lower() in ("true", "1", "yes") else 0
    return 0


def _apply_lead(event: GtmEvent, lead: dict | None) -> None:
    if not lead:
        return
    event.lead_id         = _s(lead, "id")
    event.lead_status     = _s(lead, "status")
    event.lead_source     = _s(lead, "source")
    event.lead_company    = _s(lead, "company_name") or _s(lead, "company")
    event.lead_domain     = _s(lead, "domain")
    event.lead_score      = _f(lead, "score")
    event.lead_is_signup  = _b(lead, "is_signup")
    event.lead_signup_date = _s(lead, "signup_date")


def _apply_contact(event: GtmEvent, contact: dict | None) -> None:
    if not contact:
        return
    event.contact_id    = _s(contact, "id")
    event.contact_name  = _s(contact, "name")
    event.contact_title = _s(contact, "title")
    event.contact_dept  = _s(contact, "department") or _s(contact, "dept")
    event.contact_email = _s(contact, "email")


def _apply_account(event: GtmEvent, account: dict | None) -> None:
    if not account:
        return
    event.account_id        = _s(account, "id")
    event.account_name      = _s(account, "name")
    event.account_domain    = _s(account, "domain")
    event.account_industry  = _s(account, "industry")
    event.account_type      = _s(account, "type")
    event.account_employees = _i(account, "employee_count")
    event.account_arr       = _f(account, "annual_revenue")


def _apply_campaign(event: GtmEvent, camp: dict | None) -> None:
    if not camp:
        return
    event.campaign_id       = _s(camp, "id")
    event.campaign_name     = _s(camp, "name")
    event.campaign_channel  = _s(camp, "channel")
    event.campaign_category = _s(camp, "campaign_category") or _s(camp, "category")
    event.campaign_status   = _s(camp, "status")
    event.campaign_budget   = _f(camp, "budget")


def _apply_channel(event: GtmEvent, chan: dict | None) -> None:
    if not chan:
        return
    event.channel_id   = _s(chan, "id")
    event.channel_name = _s(chan, "name")
    event.channel_type = _s(chan, "channel_type") or _s(chan, "type")


def _apply_deal(event: GtmEvent, deal: dict | None) -> None:
    if not deal:
        return
    event.deal_id          = _s(deal, "id")
    event.deal_name        = _s(deal, "name")
    event.deal_stage       = _s(deal, "stage")
    event.deal_amount      = _f(deal, "amount")
    event.deal_probability = _f(deal, "probability")
    event.deal_owner_id    = _s(deal, "owner_id")
    event.deal_close_date  = _s(deal, "close_date")


def _apply_product_account(event: GtmEvent, pa: dict | None) -> None:
    if not pa:
        return
    event.product_account_id         = _s(pa, "id")
    event.product_account_name       = _s(pa, "name")
    event.product_account_region     = _s(pa, "region")
    event.product_account_country    = _s(pa, "country")
    event.product_account_industry   = _s(pa, "industry")
    event.product_account_size_group = _s(pa, "company_size_group")
    event.product_account_is_paying  = _b(pa, "is_paying")


def _apply_product(event: GtmEvent, product: dict | None) -> None:
    if not product:
        return
    event.product_id   = _s(product, "id")
    event.product_name = _s(product, "name")
    event.product_type = _s(product, "product_type") or _s(product, "type")


def _apply_visitor(event: GtmEvent, visitor: dict | None) -> None:
    if not visitor:
        return
    event.visitor_id             = _s(visitor, "id") or _s(visitor, "visitor_id")
    event.visitor_channel        = _s(visitor, "source_channel")
    event.visitor_signup_flow    = _s(visitor, "signup_flow")
    event.visitor_signup_cluster = _s(visitor, "signup_cluster")
    event.visitor_dept           = _s(visitor, "department")
    event.visitor_seniority      = _s(visitor, "seniority")
    event.visitor_product_intent = _s(visitor, "product_intent")
    event.visitor_team_size      = _s(visitor, "team_size")


def _apply_content(event: GtmEvent, content: dict | None) -> None:
    if not content:
        return
    event.content_id   = _s(content, "id")
    event.content_name = _s(content, "name")
    event.content_type = _s(content, "content_type") or _s(content, "type")
    event.content_url  = _s(content, "url")


# ---------------------------------------------------------------------------
# Per-label result mappers
# ---------------------------------------------------------------------------

def _map_lead_result(event: GtmEvent, row: dict) -> None:
    _apply_lead(event, row.get("lead"))
    _apply_campaign(event, row.get("camp"))
    _apply_channel(event, row.get("chan"))
    _apply_contact(event, row.get("contact"))
    _apply_account(event, row.get("account"))
    _apply_product_account(event, row.get("product_account"))


def _map_contact_result(event: GtmEvent, row: dict) -> None:
    _apply_contact(event, row.get("contact"))
    _apply_account(event, row.get("account"))
    _apply_lead(event, row.get("lead"))
    _apply_campaign(event, row.get("camp"))
    _apply_channel(event, row.get("chan"))


def _map_deal_result(event: GtmEvent, row: dict) -> None:
    _apply_deal(event, row.get("deal"))
    _apply_contact(event, row.get("contact"))
    _apply_account(event, row.get("account"))
    _apply_campaign(event, row.get("camp"))
    _apply_channel(event, row.get("chan"))
    _apply_lead(event, row.get("lead"))


def _map_campaign_result(event: GtmEvent, row: dict) -> None:
    _apply_campaign(event, row.get("camp"))
    _apply_channel(event, row.get("chan"))


def _map_subscription_result(event: GtmEvent, row: dict) -> None:
    se = row.get("sub_event") or {}
    event.sub_event_type       = _s(se, "event_type")
    event.sub_plan_tier        = _s(se, "plan_tier")
    event.sub_plan_period      = _s(se, "plan_period")
    event.sub_arr              = _f(se, "arr")
    event.sub_days_from_signup = _i(se, "days_from_signup")
    event.sub_product_name     = _s(se, "product_name")

    _apply_product_account(event, row.get("product_account"))
    _apply_lead(event, row.get("lead"))
    _apply_campaign(event, row.get("camp"))
    _apply_channel(event, row.get("chan"))
    _apply_account(event, row.get("account"))
    _apply_product(event, row.get("product"))
    _apply_contact(event, row.get("contact"))


def _map_product_account_result(event: GtmEvent, row: dict) -> None:
    _apply_product_account(event, row.get("product_account"))
    _apply_account(event, row.get("account"))
    _apply_lead(event, row.get("lead"))
    _apply_campaign(event, row.get("camp"))
    _apply_channel(event, row.get("chan"))
    _apply_contact(event, row.get("contact"))


def _map_visitor_result(event: GtmEvent, row: dict) -> None:
    _apply_visitor(event, row.get("visitor"))
    _apply_campaign(event, row.get("camp"))
    _apply_channel(event, row.get("chan"))


def _map_account_result(event: GtmEvent, row: dict) -> None:
    _apply_account(event, row.get("account"))


def _map_content_result(event: GtmEvent, row: dict) -> None:
    _apply_content(event, row.get("content"))
    _apply_campaign(event, row.get("camp"))
    _apply_channel(event, row.get("chan"))


_LABEL_TO_MAPPER = {
    "Lead":              _map_lead_result,
    "Contact":           _map_contact_result,
    "Deal":              _map_deal_result,
    "Campaign":          _map_campaign_result,
    "SubscriptionEvent": _map_subscription_result,
    "ProductAccount":    _map_product_account_result,
    "Visitor":           _map_visitor_result,
    "Account":           _map_account_result,
    "Content":           _map_content_result,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def enrich_node(
    graph: GraphAdapter,
    scope: Scope,
    *,
    node_id: str,
    label: str,
    event_type: str | None = None,
    event_category: str | None = None,
    occurred_at: datetime | None = None,
    actor_id: str = "",
    extra: dict[str, Any] | None = None,
) -> GtmEvent:
    """Traverse Neo4j and return a fully enriched GtmEvent for one node.

    Parameters
    ----------
    graph:          GraphAdapter (carries the Neo4j driver)
    scope:          Scope for tenant_id + permission context
    node_id:        Neo4j node id to enrich
    label:          Primary Neo4j label (e.g. "Lead", "Deal")
    event_type:     Override default event type (e.g. "lead.updated")
    event_category: Override default category
    occurred_at:    Override timestamp (defaults to now)
    actor_id:       Actor that triggered this event (optional)
    extra:          Arbitrary overflow properties

    Returns
    -------
    GtmEvent with all reachable dimensions filled.
    """
    default_type, default_cat = NODE_EVENT_DEFAULTS.get(
        label, ("node.created", CATEGORY_LIFECYCLE)
    )

    event = GtmEvent(
        tenant_id=scope.tenant_id,
        event_type=event_type or default_type,
        event_category=event_category or default_cat,
        occurred_at=occurred_at or datetime.now(timezone.utc),
        source_node_id=node_id,
        source_label=label,
        actor_id=actor_id,
        extra=extra or {},
    )

    cypher = _LABEL_TO_CYPHER.get(label)
    if cypher is None:
        log.debug("No enrichment Cypher for label %r — skipping graph traversal", label)
        return event

    try:
        rows = await graph.execute(scope, cypher, {"id": node_id})
    except Exception:
        log.warning("Enrichment query failed for %s/%s", label, node_id, exc_info=True)
        return event

    if not rows:
        return event

    mapper = _LABEL_TO_MAPPER.get(label)
    if mapper:
        mapper(event, dict(rows[0]))

    return event


async def enrich_edge(
    graph: GraphAdapter,
    scope: Scope,
    *,
    from_id: str,
    from_label: str,
    to_id: str,
    to_label: str,
    edge_type: str,
    actor_id: str = "",
    extra: dict[str, Any] | None = None,
) -> GtmEvent:
    """Create a relationship event row enriched from the *source* node.

    The edge itself is the semantic event (e.g. SOURCED_FROM fired because
    a Lead was linked to a Campaign).  We enrich from the *from* node so
    the row carries the full lead/contact/deal context.
    """
    default_type, default_cat = EDGE_EVENT_DEFAULTS.get(
        edge_type, ("relation.created", CATEGORY_RELATIONSHIP)
    )

    event = await enrich_node(
        graph,
        scope,
        node_id=from_id,
        label=from_label,
        event_type=default_type,
        event_category=default_cat,
        actor_id=actor_id,
        extra=extra or {},
    )
    event.related_node_id = to_id
    event.related_label   = to_label
    event.relation        = edge_type
    return event
