"""Materializer — bulk-migrate the existing Neo4j graph to the OLAP store.

Strategy
--------
Rather than calling ``enrich_node()`` per node (which makes one Neo4j round-
trip per node), the materializer uses a **batched lookup** approach:

1. Run ~12 batch Cypher queries (one per relationship type) to load ALL
   relationships for the tenant into Python dicts.
2. Read nodes by label in paginated chunks (default 2 000).
3. Enrich each node in O(1) from the in-memory lookup dicts.
4. Bulk-insert the resulting events into the OLAP store.

For 500K nodes this typically takes 2-5 minutes, vs hours for per-node
enrichment.

Usage (programmatic)::

    mat = Materializer(graph, store, scope)
    stats = await mat.run(batch_size=2000)
    print(stats)

Usage (CLI)::

    gtmdb materialize                      # all labels, default tenant
    gtmdb materialize --label Lead         # single label
    gtmdb materialize --dry-run            # count only, no inserts
    gtmdb materialize --batch-size 5000    # larger chunks
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from gtmdb.graph.adapter import GraphAdapter
from gtmdb.olap.events import (
    NODE_EVENT_DEFAULTS,
    GtmEvent,
)
from gtmdb.olap.store import OlapStore
from gtmdb.scope import Scope

log = logging.getLogger(__name__)

# Labels materialized in this order (dependencies first)
ALL_LABELS: list[str] = [
    "Channel",
    "Campaign",
    "Account",
    "Product",
    "Lead",
    "Contact",
    "Deal",
    "Visitor",
    "Content",
    "ProductAccount",
    "SubscriptionEvent",
]


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

@dataclass
class MaterializeStats:
    labels_processed: list[str] = field(default_factory=list)
    nodes_read: int = 0
    events_emitted: int = 0
    elapsed_s: float = 0.0
    dry_run: bool = False

    def __str__(self) -> str:
        mode = "[DRY RUN] " if self.dry_run else ""
        return (
            f"{mode}Materialized {self.events_emitted} events "
            f"from {self.nodes_read} nodes "
            f"across {len(self.labels_processed)} labels "
            f"in {self.elapsed_s:.1f}s"
        )


# ---------------------------------------------------------------------------
# Lookup table builders
# ---------------------------------------------------------------------------

async def _batch(graph: GraphAdapter, scope: Scope, cypher: str) -> list[dict]:
    return await graph.execute(scope, cypher)


async def _build_lookups(graph: GraphAdapter, scope: Scope) -> dict[str, dict[str, dict]]:
    """Fire all batch relationship queries and return indexed lookup dicts."""
    log.info("Building enrichment lookup tables from Neo4j…")

    lookups: dict[str, dict[str, dict]] = {}

    # Lead → Campaign (via SOURCED_FROM) + Channel (via BELONGS_TO)
    rows = await _batch(graph, scope, """
        MATCH (l:Lead {tenant_id: $tenant_id})-[:SOURCED_FROM]->(c:Campaign)
        OPTIONAL MATCH (c)-[:BELONGS_TO]->(ch:Channel)
        RETURN l.id AS lead_id,
               c.id AS campaign_id, c.name AS campaign_name,
               c.channel AS campaign_channel,
               c.campaign_category AS campaign_category,
               c.status AS campaign_status, c.budget AS campaign_budget,
               ch.id AS channel_id, ch.name AS channel_name,
               ch.channel_type AS channel_type
    """)
    lookups["lead_to_campaign"] = {r["lead_id"]: r for r in rows if r.get("lead_id")}

    # Contact → Account (via WORKS_AT)
    rows = await _batch(graph, scope, """
        MATCH (con:Contact {tenant_id: $tenant_id})-[:WORKS_AT]->(acc:Account)
        RETURN con.id AS contact_id,
               acc.id AS account_id, acc.name AS account_name,
               acc.domain AS account_domain, acc.industry AS account_industry,
               acc.type AS account_type, acc.employee_count AS account_employees,
               acc.annual_revenue AS account_arr
    """)
    lookups["contact_to_account"] = {r["contact_id"]: r for r in rows if r.get("contact_id")}

    # Lead → Contact (via CONVERTED_TO) — keyed by lead_id
    rows = await _batch(graph, scope, """
        MATCH (l:Lead {tenant_id: $tenant_id})-[:CONVERTED_TO]->(con:Contact)
        RETURN l.id AS lead_id,
               con.id AS contact_id, con.name AS contact_name,
               con.title AS contact_title, con.department AS contact_dept,
               con.email AS contact_email
    """)
    lookups["lead_to_contact"] = {r["lead_id"]: r for r in rows if r.get("lead_id")}
    # Also index by contact_id for reverse lookups
    lookups["contact_to_lead"] = {r["contact_id"]: r for r in rows if r.get("contact_id")}

    # Deal → Contact (via FOR_CONTACT)
    rows = await _batch(graph, scope, """
        MATCH (d:Deal {tenant_id: $tenant_id})-[:FOR_CONTACT]->(con:Contact)
        RETURN d.id AS deal_id,
               con.id AS contact_id, con.name AS contact_name,
               con.title AS contact_title, con.department AS contact_dept,
               con.email AS contact_email
    """)
    lookups["deal_to_contact"] = {r["deal_id"]: r for r in rows if r.get("deal_id")}

    # Campaign → Deal (via INFLUENCED) — keyed by deal_id (first-touch)
    rows = await _batch(graph, scope, """
        MATCH (c:Campaign {tenant_id: $tenant_id})-[:INFLUENCED]->(d:Deal)
        OPTIONAL MATCH (c)-[:BELONGS_TO]->(ch:Channel)
        RETURN d.id AS deal_id,
               c.id AS campaign_id, c.name AS campaign_name,
               c.channel AS campaign_channel,
               c.campaign_category AS campaign_category,
               c.status AS campaign_status, c.budget AS campaign_budget,
               ch.id AS channel_id, ch.name AS channel_name,
               ch.channel_type AS channel_type
        ORDER BY c.created_at ASC
    """)
    # First-touch: keep only the first campaign per deal
    deal_to_campaign: dict[str, dict] = {}
    for r in rows:
        if r.get("deal_id") and r["deal_id"] not in deal_to_campaign:
            deal_to_campaign[r["deal_id"]] = r
    lookups["deal_to_campaign"] = deal_to_campaign

    # ProductAccount → Account (via FOR_ACCOUNT)
    rows = await _batch(graph, scope, """
        MATCH (pa:ProductAccount {tenant_id: $tenant_id})-[:FOR_ACCOUNT]->(acc:Account)
        RETURN pa.id AS pa_id,
               acc.id AS account_id, acc.name AS account_name,
               acc.domain AS account_domain, acc.industry AS account_industry,
               acc.type AS account_type, acc.employee_count AS account_employees,
               acc.annual_revenue AS account_arr
    """)
    lookups["pa_to_account"] = {r["pa_id"]: r for r in rows if r.get("pa_id")}

    # Lead → ProductAccount (via SIGNED_UP_AS) — keyed by pa_id (first lead)
    rows = await _batch(graph, scope, """
        MATCH (l:Lead {tenant_id: $tenant_id})-[:SIGNED_UP_AS]->(pa:ProductAccount)
        RETURN pa.id AS pa_id,
               l.id AS lead_id, l.status AS lead_status,
               l.source AS lead_source, l.company_name AS lead_company,
               l.domain AS lead_domain, l.score AS lead_score,
               l.is_signup AS lead_is_signup
    """)
    pa_to_lead: dict[str, dict] = {}
    for r in rows:
        if r.get("pa_id") and r["pa_id"] not in pa_to_lead:
            pa_to_lead[r["pa_id"]] = r
    lookups["pa_to_lead"] = pa_to_lead

    # ProductAccount → lead → Campaign (via SIGNED_UP_AS + SOURCED_FROM) — keyed by pa_id
    # Fallback: only used when a Lead (not Visitor) signed up as this PA
    rows = await _batch(graph, scope, """
        MATCH (l:Lead {tenant_id: $tenant_id})-[:SIGNED_UP_AS]->(pa:ProductAccount)
        MATCH (l)-[:SOURCED_FROM]->(c:Campaign)
        OPTIONAL MATCH (c)-[:BELONGS_TO]->(ch:Channel)
        RETURN pa.id AS pa_id,
               c.id AS campaign_id, c.name AS campaign_name,
               c.channel AS campaign_channel,
               c.campaign_category AS campaign_category,
               c.status AS campaign_status, c.budget AS campaign_budget,
               ch.id AS channel_id, ch.name AS channel_name,
               ch.channel_type AS channel_type
        ORDER BY c.created_at ASC
    """)
    pa_to_campaign: dict[str, dict] = {}
    for r in rows:
        if r.get("pa_id") and r["pa_id"] not in pa_to_campaign:
            pa_to_campaign[r["pa_id"]] = r
    lookups["pa_to_campaign"] = pa_to_campaign

    # Visitor → ProductAccount (via SIGNED_UP_AS) — keyed by pa_id
    # Primary path in real data: Visitor signs up, not Lead
    rows = await _batch(graph, scope, """
        MATCH (v:Visitor {tenant_id: $tenant_id})-[:SIGNED_UP_AS]->(pa:ProductAccount)
        RETURN pa.id AS pa_id,
               v.visitor_id AS visitor_id,
               v.source_channel AS visitor_channel,
               v.signup_flow AS visitor_signup_flow,
               v.signup_cluster AS visitor_signup_cluster,
               v.seniority AS visitor_seniority,
               v.product_intent AS visitor_product_intent,
               v.team_size AS visitor_team_size
    """)
    pa_to_visitor: dict[str, dict] = {}
    for r in rows:
        if r.get("pa_id") and r["pa_id"] not in pa_to_visitor:
            pa_to_visitor[r["pa_id"]] = r
    lookups["pa_to_visitor"] = pa_to_visitor

    # Visitor → ProductAccount + Campaign (SIGNED_UP_AS + TOUCHED) — keyed by pa_id
    # Primary campaign attribution for PAs signed up by Visitors
    rows = await _batch(graph, scope, """
        MATCH (v:Visitor {tenant_id: $tenant_id})-[:SIGNED_UP_AS]->(pa:ProductAccount)
        MATCH (v)-[:TOUCHED]->(c:Campaign)
        OPTIONAL MATCH (c)-[:BELONGS_TO]->(ch:Channel)
        RETURN pa.id AS pa_id,
               c.id AS campaign_id, c.name AS campaign_name,
               c.channel AS campaign_channel,
               c.campaign_category AS campaign_category,
               c.status AS campaign_status, c.budget AS campaign_budget,
               ch.id AS channel_id, ch.name AS channel_name,
               ch.channel_type AS channel_type
        ORDER BY c.created_at ASC
    """)
    pa_to_visitor_campaign: dict[str, dict] = {}
    for r in rows:
        if r.get("pa_id") and r["pa_id"] not in pa_to_visitor_campaign:
            pa_to_visitor_campaign[r["pa_id"]] = r
    lookups["pa_to_visitor_campaign"] = pa_to_visitor_campaign

    # SubscriptionEvent → ProductAccount (via HAS_SUBSCRIPTION_EVENT, reversed)
    rows = await _batch(graph, scope, """
        MATCH (pa:ProductAccount {tenant_id: $tenant_id})-[:HAS_SUBSCRIPTION_EVENT]->(se:SubscriptionEvent)
        RETURN se.id AS se_id,
               pa.id AS pa_id, pa.name AS pa_name,
               pa.region AS pa_region, pa.country AS pa_country,
               pa.industry AS pa_industry,
               pa.company_size_group AS pa_size_group,
               pa.is_paying AS pa_is_paying
    """)
    lookups["se_to_pa"] = {r["se_id"]: r for r in rows if r.get("se_id")}

    # SubscriptionEvent → Product (via FOR_PRODUCT)
    rows = await _batch(graph, scope, """
        MATCH (se:SubscriptionEvent {tenant_id: $tenant_id})-[:FOR_PRODUCT]->(p:Product)
        RETURN se.id AS se_id,
               p.id AS product_id, p.name AS product_name,
               p.product_type AS product_type
    """)
    lookups["se_to_product"] = {r["se_id"]: r for r in rows if r.get("se_id")}

    # Visitor → Campaign (via TOUCHED) — first-touch
    rows = await _batch(graph, scope, """
        MATCH (v:Visitor {tenant_id: $tenant_id})-[:TOUCHED]->(c:Campaign)
        OPTIONAL MATCH (c)-[:BELONGS_TO]->(ch:Channel)
        RETURN v.id AS visitor_id,
               c.id AS campaign_id, c.name AS campaign_name,
               c.channel AS campaign_channel,
               c.campaign_category AS campaign_category,
               c.status AS campaign_status, c.budget AS campaign_budget,
               ch.id AS channel_id, ch.name AS channel_name,
               ch.channel_type AS channel_type
        ORDER BY c.created_at ASC
    """)
    visitor_to_campaign: dict[str, dict] = {}
    for r in rows:
        if r.get("visitor_id") and r["visitor_id"] not in visitor_to_campaign:
            visitor_to_campaign[r["visitor_id"]] = r
    lookups["visitor_to_campaign"] = visitor_to_campaign

    # Campaign → Channel (via BELONGS_TO)
    rows = await _batch(graph, scope, """
        MATCH (c:Campaign {tenant_id: $tenant_id})-[:BELONGS_TO]->(ch:Channel)
        RETURN c.id AS campaign_id,
               ch.id AS channel_id, ch.name AS channel_name,
               ch.channel_type AS channel_type
    """)
    lookups["campaign_to_channel"] = {r["campaign_id"]: r for r in rows if r.get("campaign_id")}

    sizes = {k: len(v) for k, v in lookups.items()}
    log.info("Lookup tables built: %s", sizes)
    return lookups


# ---------------------------------------------------------------------------
# Node → GtmEvent converters (use lookup dicts, no Neo4j calls)
# ---------------------------------------------------------------------------

def _s(d: dict | None, key: str, fallback: str = "") -> str:
    if not d:
        return fallback
    v = d.get(key)
    return str(v) if v is not None else fallback


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


def _parse_dt(val: Any) -> datetime:
    if val is None:
        return datetime.now(timezone.utc)
    if isinstance(val, datetime):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(str(val))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return datetime.now(timezone.utc)


def _apply_campaign_from_lookup(event: GtmEvent, lk: dict | None) -> None:
    if not lk:
        return
    event.campaign_id = _s(lk, "campaign_id")
    event.campaign_name = _s(lk, "campaign_name")
    event.campaign_channel = _s(lk, "campaign_channel")
    event.campaign_category = _s(lk, "campaign_category")
    event.campaign_status = _s(lk, "campaign_status")
    event.campaign_budget = _f(lk, "campaign_budget")
    event.channel_id = _s(lk, "channel_id")
    event.channel_name = _s(lk, "channel_name")
    event.channel_type = _s(lk, "channel_type")


def _apply_account_from_lookup(event: GtmEvent, lk: dict | None) -> None:
    if not lk:
        return
    event.account_id = _s(lk, "account_id")
    event.account_name = _s(lk, "account_name")
    event.account_domain = _s(lk, "account_domain")
    event.account_industry = _s(lk, "account_industry")
    event.account_type = _s(lk, "account_type")
    event.account_employees = _i(lk, "account_employees")
    event.account_arr = _f(lk, "account_arr")


def _apply_contact_from_lookup(event: GtmEvent, lk: dict | None) -> None:
    if not lk:
        return
    event.contact_id = _s(lk, "contact_id")
    event.contact_name = _s(lk, "contact_name")
    event.contact_title = _s(lk, "contact_title")
    event.contact_dept = _s(lk, "contact_dept")
    event.contact_email = _s(lk, "contact_email")


def _node_to_event(
    props: dict,
    label: str,
    tenant_id: str,
    lookups: dict[str, dict[str, dict]],
) -> GtmEvent:
    """Convert a raw Neo4j property dict to a GtmEvent using lookup dicts."""
    node_id = _s(props, "id")
    event_type, event_category = NODE_EVENT_DEFAULTS.get(
        label, ("node.created", "lifecycle")
    )

    event = GtmEvent(
        tenant_id=tenant_id,
        event_type=event_type,
        event_category=event_category,
        occurred_at=_parse_dt(props.get("created_at")),
        source_node_id=node_id,
        source_label=label,
        actor_id=_s(props, "created_by_actor_id"),
    )

    if label == "Lead":
        event.lead_id = node_id
        event.lead_status = _s(props, "status")
        event.lead_source = _s(props, "source")
        event.lead_company = _s(props, "company_name")
        event.lead_domain = _s(props, "domain")
        event.lead_score = _f(props, "score")
        event.lead_is_signup = _b(props, "is_signup")
        event.lead_signup_date = _s(props, "signup_date")
        _apply_campaign_from_lookup(event, lookups["lead_to_campaign"].get(node_id))
        _apply_contact_from_lookup(event, lookups["lead_to_contact"].get(node_id))

    elif label == "Contact":
        event.contact_id = node_id
        event.contact_name = _s(props, "name")
        event.contact_title = _s(props, "title")
        event.contact_dept = _s(props, "department")
        event.contact_email = _s(props, "email")
        _apply_account_from_lookup(event, lookups["contact_to_account"].get(node_id))
        # Backfill lead dims if this contact was converted from a lead
        lead_lk = lookups["contact_to_lead"].get(node_id)
        if lead_lk:
            event.lead_id = _s(lead_lk, "lead_id")
            _apply_campaign_from_lookup(
                event, lookups["lead_to_campaign"].get(event.lead_id)
            )

    elif label == "Account":
        event.account_id = node_id
        event.account_name = _s(props, "name")
        event.account_domain = _s(props, "domain")
        event.account_industry = _s(props, "industry")
        event.account_type = _s(props, "type")
        event.account_employees = _i(props, "employee_count")
        event.account_arr = _f(props, "annual_revenue")

    elif label == "Deal":
        event.deal_id = node_id
        event.deal_name = _s(props, "name")
        event.deal_stage = _s(props, "stage")
        event.deal_amount = _f(props, "amount")
        event.deal_probability = _f(props, "probability")
        event.deal_owner_id = _s(props, "owner_id")
        event.deal_close_date = _s(props, "close_date")
        _apply_contact_from_lookup(event, lookups["deal_to_contact"].get(node_id))
        _apply_campaign_from_lookup(event, lookups["deal_to_campaign"].get(node_id))
        # Account from contact
        if event.contact_id:
            _apply_account_from_lookup(
                event, lookups["contact_to_account"].get(event.contact_id)
            )

    elif label == "Campaign":
        event.campaign_id = node_id
        event.campaign_name = _s(props, "name")
        event.campaign_channel = _s(props, "channel")
        event.campaign_category = _s(props, "campaign_category")
        event.campaign_status = _s(props, "status")
        event.campaign_budget = _f(props, "budget")
        ch_lk = lookups["campaign_to_channel"].get(node_id)
        if ch_lk:
            event.channel_id = _s(ch_lk, "channel_id")
            event.channel_name = _s(ch_lk, "channel_name")
            event.channel_type = _s(ch_lk, "channel_type")

    elif label == "Channel":
        event.channel_id = node_id
        event.channel_name = _s(props, "name")
        event.channel_type = _s(props, "channel_type")

    elif label == "Product":
        event.product_id = node_id
        event.product_name = _s(props, "name")
        event.product_type = _s(props, "product_type")

    elif label == "ProductAccount":
        event.product_account_id = node_id
        event.product_account_name = _s(props, "name")
        event.product_account_region = _s(props, "region")
        event.product_account_country = _s(props, "country")
        event.product_account_industry = _s(props, "industry")
        event.product_account_size_group = _s(props, "company_size_group")
        event.product_account_is_paying = _b(props, "is_paying")
        _apply_account_from_lookup(event, lookups["pa_to_account"].get(node_id))
        # Primary path: Visitor signed up → carry visitor dims
        vis_lk = lookups["pa_to_visitor"].get(node_id)
        if vis_lk:
            event.visitor_id = _s(vis_lk, "visitor_id")
            event.visitor_channel = _s(vis_lk, "visitor_channel")
            event.visitor_signup_flow = _s(vis_lk, "visitor_signup_flow")
            event.visitor_signup_cluster = _s(vis_lk, "visitor_signup_cluster")
            event.visitor_seniority = _s(vis_lk, "visitor_seniority")
            event.visitor_product_intent = _s(vis_lk, "visitor_product_intent")
            event.visitor_team_size = _s(vis_lk, "visitor_team_size")
        else:
            # Fallback: Lead signed up
            lead_lk = lookups["pa_to_lead"].get(node_id)
            if lead_lk:
                event.lead_id = _s(lead_lk, "lead_id")
                event.lead_status = _s(lead_lk, "lead_status")
                event.lead_source = _s(lead_lk, "lead_source")
                event.lead_company = _s(lead_lk, "lead_company")
                event.lead_domain = _s(lead_lk, "lead_domain")
        # Campaign: visitor-based first, lead-based fallback
        _apply_campaign_from_lookup(
            event,
            lookups["pa_to_visitor_campaign"].get(node_id)
            or lookups["pa_to_campaign"].get(node_id),
        )

    elif label == "SubscriptionEvent":
        event.sub_event_type = _s(props, "event_type")
        event.sub_plan_tier = _s(props, "plan_tier")
        event.sub_plan_period = _s(props, "plan_period")
        event.sub_arr = _f(props, "arr")
        event.sub_days_from_signup = _i(props, "days_from_signup")
        event.sub_product_name = _s(props, "product_name")
        event.occurred_at = _parse_dt(props.get("occurred_at") or props.get("created_at"))
        pa_lk = lookups["se_to_pa"].get(node_id)
        if pa_lk:
            event.product_account_id = _s(pa_lk, "pa_id")
            event.product_account_name = _s(pa_lk, "pa_name")
            event.product_account_region = _s(pa_lk, "pa_region")
            event.product_account_country = _s(pa_lk, "pa_country")
            event.product_account_industry = _s(pa_lk, "pa_industry")
            event.product_account_size_group = _s(pa_lk, "pa_size_group")
            event.product_account_is_paying = _b(pa_lk, "pa_is_paying")
            pa_id = event.product_account_id
            # Chain: SE → PA → Visitor (primary) or Lead (fallback)
            vis_lk = lookups["pa_to_visitor"].get(pa_id)
            if vis_lk:
                event.visitor_id = _s(vis_lk, "visitor_id")
                event.visitor_channel = _s(vis_lk, "visitor_channel")
                event.visitor_signup_flow = _s(vis_lk, "visitor_signup_flow")
                event.visitor_signup_cluster = _s(vis_lk, "visitor_signup_cluster")
                event.visitor_seniority = _s(vis_lk, "visitor_seniority")
                event.visitor_product_intent = _s(vis_lk, "visitor_product_intent")
                event.visitor_team_size = _s(vis_lk, "visitor_team_size")
            else:
                lead_lk = lookups["pa_to_lead"].get(pa_id)
                if lead_lk:
                    event.lead_id = _s(lead_lk, "lead_id")
            # Campaign: visitor-based first, lead-based fallback
            _apply_campaign_from_lookup(
                event,
                lookups["pa_to_visitor_campaign"].get(pa_id)
                or lookups["pa_to_campaign"].get(pa_id),
            )
            _apply_account_from_lookup(event, lookups["pa_to_account"].get(pa_id))
        prod_lk = lookups["se_to_product"].get(node_id)
        if prod_lk:
            event.product_id = _s(prod_lk, "product_id")
            event.product_name = _s(prod_lk, "product_name")
            event.product_type = _s(prod_lk, "product_type")

    elif label == "Visitor":
        event.visitor_id = _s(props, "visitor_id") or node_id
        event.visitor_channel = _s(props, "source_channel")
        event.visitor_signup_flow = _s(props, "signup_flow")
        event.visitor_signup_cluster = _s(props, "signup_cluster")
        event.visitor_dept = _s(props, "department")
        event.visitor_seniority = _s(props, "seniority")
        event.visitor_product_intent = _s(props, "product_intent")
        event.visitor_team_size = _s(props, "team_size")
        _apply_campaign_from_lookup(event, lookups["visitor_to_campaign"].get(node_id))

    elif label == "Content":
        event.content_id = node_id
        event.content_name = _s(props, "name")
        event.content_type = _s(props, "content_type")
        event.content_url = _s(props, "url")

    return event


# ---------------------------------------------------------------------------
# Materializer
# ---------------------------------------------------------------------------

class Materializer:
    """Bulk-migrates the Neo4j graph to the OLAP store for one tenant."""

    def __init__(
        self,
        graph: GraphAdapter,
        store: OlapStore,
        scope: Scope,
        *,
        progress_cb: Callable[[str, int, int], None] | None = None,
    ) -> None:
        self._graph = graph
        self._store = store
        self._scope = scope
        self._progress = progress_cb  # (label, done, total) callback

    async def run(
        self,
        labels: list[str] | None = None,
        batch_size: int = 2000,
        dry_run: bool = False,
    ) -> MaterializeStats:
        """Run the full materialization and return stats."""
        t0 = time.monotonic()
        stats = MaterializeStats(dry_run=dry_run)

        target_labels = labels or ALL_LABELS
        lookups = await _build_lookups(self._graph, self._scope)

        for label in target_labels:
            n_read, n_emitted = await self._materialize_label(
                label, lookups, batch_size, dry_run
            )
            stats.labels_processed.append(label)
            stats.nodes_read += n_read
            stats.events_emitted += n_emitted

        stats.elapsed_s = time.monotonic() - t0
        return stats

    async def _materialize_label(
        self,
        label: str,
        lookups: dict[str, dict[str, dict]],
        batch_size: int,
        dry_run: bool,
    ) -> tuple[int, int]:
        """Process one label. Returns (nodes_read, events_emitted)."""
        total = await self._count_nodes(label)
        if total == 0:
            log.info("Label %s: 0 nodes — skipping", label)
            return 0, 0

        log.info("Label %s: %d nodes to materialize (batch_size=%d)", label, total, batch_size)
        n_read = 0
        n_emitted = 0
        skip = 0

        while True:
            props_list = await self._read_nodes(label, skip, batch_size)
            if not props_list:
                break

            events = [
                _node_to_event(p, label, self._scope.tenant_id, lookups)
                for p in props_list
            ]

            if not dry_run:
                await self._store.insert_events([e.to_row() for e in events])

            n_read += len(props_list)
            n_emitted += len(events)
            skip += len(props_list)

            if self._progress:
                self._progress(label, n_read, total)

            log.debug("Label %s: %d / %d", label, n_read, total)

            if len(props_list) < batch_size:
                break

        log.info("Label %s: done — %d events emitted", label, n_emitted)
        return n_read, n_emitted

    async def _count_nodes(self, label: str) -> int:
        rows = await self._graph.execute(
            self._scope,
            f"MATCH (n:{label} {{tenant_id: $tenant_id}}) RETURN count(n) AS cnt",
        )
        return int(rows[0]["cnt"]) if rows else 0

    async def _read_nodes(self, label: str, skip: int, limit: int) -> list[dict]:
        rows = await self._graph.execute(
            self._scope,
            f"MATCH (n:{label} {{tenant_id: $tenant_id}}) "
            f"RETURN properties(n) AS props "
            f"ORDER BY n.id SKIP {skip} LIMIT {limit}",
        )
        return [dict(r["props"]) for r in rows]
