"""Materialize all nodes connected to a specific campaign into ClickHouse.

Usage:
    python scripts/materialize_campaign.py <campaign_id> [--limit N]

Finds all Visitors, Leads, Contacts, Deals, ProductAccounts, and
SubscriptionEvents reachable from the campaign, enriches them, and
inserts into ClickHouse events table.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from dotenv import load_dotenv
load_dotenv()

import clickhouse_connect
from neo4j import AsyncGraphDatabase

NEO4J_URI = os.environ["GTMDB_NEO4J_URI"]
NEO4J_USER = os.environ.get("GTMDB_NEO4J_USER", "neo4j")
NEO4J_PW = os.environ["GTMDB_NEO4J_PASSWORD"]
TENANT = os.environ.get("GTMDB_DEFAULT_TENANT_ID", "00000000-0000-4000-8000-000000000001")

CH_HOST = os.environ["GTMDB_CLICKHOUSE_HOST"]
CH_PORT = int(os.environ.get("GTMDB_CLICKHOUSE_PORT", "8443"))
CH_USER = os.environ.get("GTMDB_CLICKHOUSE_USER", "default")
CH_PW = os.environ["GTMDB_CLICKHOUSE_PASSWORD"]
CH_DB = os.environ.get("GTMDB_CLICKHOUSE_DATABASE", "default")

now_iso = datetime.now(timezone.utc).isoformat()


def _s(d, k, fb=""):
    v = d.get(k)
    return str(v) if v is not None else fb

def _f(d, k):
    try: return float(d.get(k) or 0)
    except: return 0.0

def _i(d, k):
    try: return int(d.get(k) or 0)
    except: return 0

def _b(d, k):
    v = d.get(k)
    if isinstance(v, bool): return 1 if v else 0
    try: return 1 if int(v) else 0
    except: return 0

def _dt(v):
    if v is None: return now_iso
    if isinstance(v, datetime): return v.isoformat()
    try:
        dt = datetime.fromisoformat(str(v))
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except: return now_iso


async def fetch_campaign(session, campaign_id: str) -> dict:
    r = await session.run("""
        MATCH (c:Campaign {id: $cid, tenant_id: $t})
        OPTIONAL MATCH (c)-[:BELONGS_TO]->(ch:Channel)
        RETURN c.id AS id, c.name AS name, c.status AS status,
               c.channel AS channel, c.campaign_category AS category,
               c.budget AS budget, c.created_at AS created_at,
               ch.id AS channel_id, ch.name AS channel_name,
               ch.channel_type AS channel_type
    """, cid=campaign_id, t=TENANT)
    rec = await r.single()
    if not rec:
        raise ValueError(f"Campaign {campaign_id} not found in tenant {TENANT}")
    return dict(rec)


async def fetch_visitors(session, campaign_id: str, limit: int) -> list[dict]:
    r = await session.run("""
        MATCH (v:Visitor {tenant_id: $t})-[:TOUCHED]->(c:Campaign {id: $cid})
        RETURN v.id AS id, v.visitor_id AS visitor_id,
               v.source_channel AS source_channel,
               v.signup_flow AS signup_flow,
               v.signup_cluster AS signup_cluster,
               v.department AS department,
               v.seniority AS seniority,
               v.product_intent AS product_intent,
               v.team_size AS team_size,
               v.created_at AS created_at
        LIMIT $lim
    """, cid=campaign_id, t=TENANT, lim=limit)
    return [dict(rec) async for rec in r]


async def fetch_leads(session, campaign_id: str, limit: int) -> list[dict]:
    r = await session.run("""
        MATCH (l:Lead {tenant_id: $t})-[:SOURCED_FROM]->(c:Campaign {id: $cid})
        OPTIONAL MATCH (l)-[:CONVERTED_TO]->(con:Contact)
        OPTIONAL MATCH (con)-[:WORKS_AT]->(acc:Account)
        RETURN l.id AS id, l.status AS status, l.source AS source,
               l.score AS score, l.domain AS domain,
               l.company_name AS company_name,
               l.is_signup AS is_signup, l.signup_date AS signup_date,
               l.created_at AS created_at,
               con.id AS contact_id, con.name AS contact_name,
               con.title AS contact_title, con.department AS contact_dept,
               con.email AS contact_email,
               acc.id AS account_id, acc.name AS account_name,
               acc.domain AS account_domain, acc.industry AS account_industry,
               acc.type AS account_type, acc.employee_count AS account_employees,
               acc.annual_revenue AS account_arr
        LIMIT $lim
    """, cid=campaign_id, t=TENANT, lim=limit)
    return [dict(rec) async for rec in r]


async def fetch_subscription_events(session, campaign_id: str, limit: int) -> list[dict]:
    """SE ← PA ← Visitor ← Campaign (via Visitor TOUCHED + SIGNED_UP_AS)"""
    r = await session.run("""
        MATCH (v:Visitor {tenant_id: $t})-[:TOUCHED]->(c:Campaign {id: $cid})
        MATCH (v)-[:SIGNED_UP_AS]->(pa:ProductAccount)
        MATCH (pa)-[:HAS_SUBSCRIPTION_EVENT]->(se:SubscriptionEvent)
        OPTIONAL MATCH (se)-[:FOR_PRODUCT]->(p:Product)
        OPTIONAL MATCH (pa)-[:FOR_ACCOUNT]->(acc:Account)
        RETURN se.id AS id, se.event_type AS event_type,
               se.plan_tier AS plan_tier, se.plan_period AS plan_period,
               se.arr AS arr, se.days_from_signup AS days_from_signup,
               se.occurred_at AS occurred_at, se.created_at AS created_at,
               pa.id AS pa_id, pa.name AS pa_name,
               pa.region AS pa_region, pa.country AS pa_country,
               pa.industry AS pa_industry,
               pa.company_size_group AS pa_size_group,
               pa.is_paying AS pa_is_paying,
               p.id AS product_id, p.name AS product_name,
               p.product_type AS product_type,
               v.visitor_id AS visitor_id,
               v.source_channel AS visitor_channel,
               v.signup_flow AS visitor_signup_flow,
               v.signup_cluster AS visitor_signup_cluster,
               v.seniority AS visitor_seniority,
               acc.id AS account_id, acc.name AS account_name,
               acc.domain AS account_domain, acc.industry AS account_industry,
               acc.type AS account_type,
               acc.employee_count AS account_employees,
               acc.annual_revenue AS account_arr
        LIMIT $lim
    """, cid=campaign_id, t=TENANT, lim=limit)
    return [dict(rec) async for rec in r]


async def fetch_product_accounts(session, campaign_id: str, limit: int) -> list[dict]:
    r = await session.run("""
        MATCH (v:Visitor {tenant_id: $t})-[:TOUCHED]->(c:Campaign {id: $cid})
        MATCH (v)-[:SIGNED_UP_AS]->(pa:ProductAccount)
        OPTIONAL MATCH (pa)-[:FOR_ACCOUNT]->(acc:Account)
        OPTIONAL MATCH (pa)-[:HAS_SUBSCRIPTION_EVENT]->(se:SubscriptionEvent)
            -[:FOR_PRODUCT]->(p:Product)
        RETURN DISTINCT
               pa.id AS id, pa.name AS pa_name,
               pa.region AS pa_region, pa.country AS pa_country,
               pa.industry AS pa_industry,
               pa.company_size_group AS pa_size_group,
               pa.is_paying AS pa_is_paying, pa.created_at AS created_at,
               v.visitor_id AS visitor_id,
               v.source_channel AS visitor_channel,
               v.signup_flow AS visitor_signup_flow,
               v.signup_cluster AS visitor_signup_cluster,
               v.seniority AS visitor_seniority,
               acc.id AS account_id, acc.name AS account_name,
               acc.domain AS account_domain, acc.industry AS account_industry,
               acc.type AS account_type,
               acc.employee_count AS account_employees,
               acc.annual_revenue AS account_arr,
               p.id AS product_id, p.name AS product_name
        LIMIT $lim
    """, cid=campaign_id, t=TENANT, lim=limit)
    return [dict(rec) async for rec in r]


def make_base_row(campaign: dict, event_type: str, event_category: str,
                  source_label: str, source_node_id: str, occurred_at: str) -> dict:
    return {
        "tenant_id": TENANT,
        "event_type": event_type,
        "event_category": event_category,
        "occurred_at": occurred_at,
        "source_node_id": source_node_id,
        "source_label": source_label,
        "actor_id": "",
        # Campaign dims
        "campaign_id": _s(campaign, "id"),
        "campaign_name": _s(campaign, "name"),
        "campaign_channel": _s(campaign, "channel"),
        "campaign_category": _s(campaign, "category"),
        "campaign_status": _s(campaign, "status"),
        "campaign_budget": _f(campaign, "budget"),
        "channel_id": _s(campaign, "channel_id"),
        "channel_name": _s(campaign, "channel_name"),
        "channel_type": _s(campaign, "channel_type"),
        # rest empty
        "lead_id": "", "lead_status": "", "lead_source": "",
        "lead_company": "", "lead_domain": "", "lead_score": 0.0,
        "lead_is_signup": 0, "lead_signup_date": "",
        "contact_id": "", "contact_name": "", "contact_title": "",
        "contact_dept": "", "contact_email": "",
        "account_id": "", "account_name": "", "account_domain": "",
        "account_industry": "", "account_type": "",
        "account_employees": 0, "account_arr": 0.0,
        "deal_id": "", "deal_name": "", "deal_stage": "",
        "deal_amount": 0.0, "deal_probability": 0.0,
        "deal_owner_id": "", "deal_close_date": "",
        "product_id": "", "product_name": "", "product_type": "",
        "product_account_id": "", "product_account_name": "",
        "product_account_region": "", "product_account_country": "",
        "product_account_industry": "", "product_account_size_group": "",
        "product_account_is_paying": 0,
        "sub_event_type": "", "sub_plan_tier": "", "sub_plan_period": "",
        "sub_arr": 0.0, "sub_days_from_signup": 0, "sub_product_name": "",
        "visitor_id": "", "visitor_channel": "", "visitor_signup_flow": "",
        "visitor_signup_cluster": "", "visitor_dept": "",
        "visitor_seniority": "", "visitor_product_intent": "",
        "visitor_team_size": "",
        "content_id": "", "content_name": "", "content_type": "", "content_url": "",
        "extra": "{}",
    }


def visitor_to_row(v: dict, campaign: dict) -> dict:
    row = make_base_row(campaign, "visitor.touched", "acquisition",
                        "Visitor", _s(v, "id"), _dt(v.get("created_at")))
    row["visitor_id"] = _s(v, "visitor_id") or _s(v, "id")
    row["visitor_channel"] = _s(v, "source_channel")
    row["visitor_signup_flow"] = _s(v, "signup_flow")
    row["visitor_signup_cluster"] = _s(v, "signup_cluster")
    row["visitor_dept"] = _s(v, "department")
    row["visitor_seniority"] = _s(v, "seniority")
    row["visitor_product_intent"] = _s(v, "product_intent")
    row["visitor_team_size"] = _s(v, "team_size")
    return row


def lead_to_row(l: dict, campaign: dict) -> dict:
    row = make_base_row(campaign, "lead.created", "lifecycle",
                        "Lead", _s(l, "id"), _dt(l.get("created_at")))
    row["lead_id"] = _s(l, "id")
    row["lead_status"] = _s(l, "status")
    row["lead_source"] = _s(l, "source")
    row["lead_score"] = _f(l, "score")
    row["lead_company"] = _s(l, "company_name")
    row["lead_domain"] = _s(l, "domain")
    row["lead_is_signup"] = _b(l, "is_signup")
    row["lead_signup_date"] = _s(l, "signup_date")
    row["contact_id"] = _s(l, "contact_id")
    row["contact_name"] = _s(l, "contact_name")
    row["contact_title"] = _s(l, "contact_title")
    row["contact_dept"] = _s(l, "contact_dept")
    row["contact_email"] = _s(l, "contact_email")
    row["account_id"] = _s(l, "account_id")
    row["account_name"] = _s(l, "account_name")
    row["account_domain"] = _s(l, "account_domain")
    row["account_industry"] = _s(l, "account_industry")
    row["account_type"] = _s(l, "account_type")
    row["account_employees"] = _i(l, "account_employees")
    row["account_arr"] = _f(l, "account_arr")
    return row


def se_to_row(se: dict, campaign: dict) -> dict:
    row = make_base_row(campaign, "subscription.event", "revenue",
                        "SubscriptionEvent", _s(se, "id"),
                        _dt(se.get("occurred_at") or se.get("created_at")))
    row["sub_event_type"] = _s(se, "event_type")
    row["sub_plan_tier"] = _s(se, "plan_tier")
    row["sub_plan_period"] = _s(se, "plan_period")
    row["sub_arr"] = _f(se, "arr")
    row["sub_days_from_signup"] = _i(se, "days_from_signup")
    row["product_id"] = _s(se, "product_id")
    row["product_name"] = _s(se, "product_name")
    row["product_type"] = _s(se, "product_type")
    row["product_account_id"] = _s(se, "pa_id")
    row["product_account_name"] = _s(se, "pa_name")
    row["product_account_region"] = _s(se, "pa_region")
    row["product_account_country"] = _s(se, "pa_country")
    row["product_account_industry"] = _s(se, "pa_industry")
    row["product_account_size_group"] = _s(se, "pa_size_group")
    row["product_account_is_paying"] = _b(se, "pa_is_paying")
    # Visitor is the signup entity — not a Lead
    row["visitor_id"] = _s(se, "visitor_id")
    row["visitor_channel"] = _s(se, "visitor_channel")
    row["visitor_signup_flow"] = _s(se, "visitor_signup_flow")
    row["visitor_signup_cluster"] = _s(se, "visitor_signup_cluster")
    row["visitor_seniority"] = _s(se, "visitor_seniority")
    row["account_id"] = _s(se, "account_id")
    row["account_name"] = _s(se, "account_name")
    row["account_domain"] = _s(se, "account_domain")
    row["account_industry"] = _s(se, "account_industry")
    row["account_type"] = _s(se, "account_type")
    row["account_employees"] = _i(se, "account_employees")
    row["account_arr"] = _f(se, "account_arr")
    return row


def pa_to_row(pa: dict, campaign: dict) -> dict:
    row = make_base_row(campaign, "product_account.created", "lifecycle",
                        "ProductAccount", _s(pa, "id"),
                        _dt(pa.get("created_at")))
    row["product_account_id"] = _s(pa, "id")
    row["product_account_name"] = _s(pa, "pa_name")
    row["product_account_region"] = _s(pa, "pa_region")
    row["product_account_country"] = _s(pa, "pa_country")
    row["product_account_industry"] = _s(pa, "pa_industry")
    row["product_account_size_group"] = _s(pa, "pa_size_group")
    row["product_account_is_paying"] = _b(pa, "pa_is_paying")
    # Visitor is the signup entity — not a Lead
    row["visitor_id"] = _s(pa, "visitor_id")
    row["visitor_channel"] = _s(pa, "visitor_channel")
    row["visitor_signup_flow"] = _s(pa, "visitor_signup_flow")
    row["visitor_signup_cluster"] = _s(pa, "visitor_signup_cluster")
    row["visitor_seniority"] = _s(pa, "visitor_seniority")
    row["account_id"] = _s(pa, "account_id")
    row["account_name"] = _s(pa, "account_name")
    row["account_domain"] = _s(pa, "account_domain")
    row["account_industry"] = _s(pa, "account_industry")
    row["account_type"] = _s(pa, "account_type")
    row["account_employees"] = _i(pa, "account_employees")
    row["account_arr"] = _f(pa, "account_arr")
    row["product_id"] = _s(pa, "product_id")
    row["product_name"] = _s(pa, "product_name")
    return row


COLUMNS = [
    "tenant_id","event_type","event_category","occurred_at","source_node_id",
    "source_label","actor_id",
    "campaign_id","campaign_name","campaign_channel","campaign_category",
    "campaign_status","campaign_budget","channel_id","channel_name","channel_type",
    "lead_id","lead_status","lead_source","lead_company","lead_domain",
    "lead_score","lead_is_signup","lead_signup_date",
    "contact_id","contact_name","contact_title","contact_dept","contact_email",
    "account_id","account_name","account_domain","account_industry","account_type",
    "account_employees","account_arr",
    "deal_id","deal_name","deal_stage","deal_amount","deal_probability",
    "deal_owner_id","deal_close_date",
    "product_id","product_name","product_type",
    "product_account_id","product_account_name","product_account_region",
    "product_account_country","product_account_industry","product_account_size_group",
    "product_account_is_paying",
    "sub_event_type","sub_plan_tier","sub_plan_period","sub_arr",
    "sub_days_from_signup","sub_product_name",
    "visitor_id","visitor_channel","visitor_signup_flow","visitor_signup_cluster",
    "visitor_dept","visitor_seniority","visitor_product_intent","visitor_team_size",
    "content_id","content_name","content_type","content_url","extra",
]


def insert_rows(ch_client, rows: list[dict], label: str):
    if not rows:
        print(f"  {label}: 0 rows — skipping")
        return
    data = [[row[col] for col in COLUMNS] for row in rows]
    ch_client.insert("events", data, column_names=COLUMNS)
    print(f"  {label:22s} → {len(rows):>5} rows inserted")


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("campaign_id", help="Campaign UUID to materialize")
    parser.add_argument("--limit", type=int, default=1000,
                        help="Max rows per node type (default 1000)")
    args = parser.parse_args()

    driver = AsyncGraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PW))
    ch = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, secure=True,
        user=CH_USER, password=CH_PW, database=CH_DB,
    )

    print(f"\nCampaign: {args.campaign_id}")
    print(f"Tenant:   {TENANT}")
    print(f"Limit:    {args.limit} rows per type\n")

    async with driver.session() as s:
        campaign = await fetch_campaign(s, args.campaign_id)
        print(f"Campaign name:    {campaign['name']!r}")
        print(f"Channel:          {campaign['channel']} / {campaign['channel_name']}")
        print(f"Status:           {campaign['status']}")
        print()

        print("Fetching from Neo4j…")
        visitors = await fetch_visitors(s, args.campaign_id, args.limit)
        leads = await fetch_leads(s, args.campaign_id, args.limit)
        sub_events = await fetch_subscription_events(s, args.campaign_id, args.limit)
        product_accounts = await fetch_product_accounts(s, args.campaign_id, args.limit)

    print(f"  Visitors:         {len(visitors)}")
    print(f"  Leads:            {len(leads)}")
    print(f"  ProductAccounts:  {len(product_accounts)}")
    print(f"  SubscriptionEvts: {len(sub_events)}")
    total = len(visitors) + len(leads) + len(product_accounts) + len(sub_events)
    print(f"  Total:            {total}\n")

    print("Inserting into ClickHouse Cloud…")
    insert_rows(ch, [visitor_to_row(v, campaign) for v in visitors], "Visitor")
    insert_rows(ch, [lead_to_row(l, campaign) for l in leads], "Lead")
    insert_rows(ch, [pa_to_row(p, campaign) for p in product_accounts], "ProductAccount")
    insert_rows(ch, [se_to_row(se, campaign) for se in sub_events], "SubscriptionEvent")

    # Verify
    r = ch.query(
        "SELECT source_label, count() AS c FROM events "
        "WHERE campaign_id = %(cid)s GROUP BY source_label ORDER BY c DESC",
        parameters={"cid": args.campaign_id}
    )
    print(f"\n── ClickHouse events for campaign {args.campaign_id} ──")
    for row in r.result_rows:
        print(f"  {row[0]:22s} {row[1]:>6}")

    ch.close()
    await driver.close()


if __name__ == "__main__":
    asyncio.run(main())
