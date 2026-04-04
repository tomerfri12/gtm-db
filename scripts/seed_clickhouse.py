"""Seed ClickHouse events table with a small but realistic GTM dataset.

Run with:
    .venv/bin/python scripts/seed_clickhouse.py

The dataset models a SaaS company's GTM motion:
  - 3 Channels (Paid Search, Content, Direct Sales)
  - 4 Campaigns spread across those channels
  - 12 Leads (sourced from campaigns), some converted to Contacts
  - 6 Accounts (companies)
  - 5 Deals (various stages)
  - 8 Subscription events (signups, upgrades, churns)
  - 10 Visitor touch events
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from gtmdb.config import GtmdbSettings
from gtmdb.olap.store import OlapStore

# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

TENANT = "demo-tenant-01"

def dt(days_ago: float, hour: int = 9) -> datetime:
    """Return a UTC datetime that many days in the past."""
    base = datetime.now(timezone.utc).replace(hour=hour, minute=0, second=0, microsecond=0)
    return base - timedelta(days=days_ago)


def uid() -> str:
    return str(uuid.uuid4())


# Channels
CHANNELS = {
    "ch-sem":     {"channel_id": "ch-sem",     "channel_name": "Google Ads",      "channel_type": "paid_search"},
    "ch-content": {"channel_id": "ch-content",  "channel_name": "Content/SEO",     "channel_type": "organic"},
    "ch-outbound":{"channel_id": "ch-outbound", "channel_name": "Direct Outbound", "channel_type": "outbound"},
}

# Campaigns
CAMPAIGNS = {
    "camp-spring": {
        "campaign_id": "camp-spring",
        "campaign_name": "Spring Launch 2026",
        "campaign_channel": "paid_search",
        "campaign_category": "demand_gen",
        "campaign_status": "active",
        "campaign_budget": 15000.0,
        **CHANNELS["ch-sem"],
    },
    "camp-ebook": {
        "campaign_id": "camp-ebook",
        "campaign_name": "GTM Playbook eBook",
        "campaign_channel": "organic",
        "campaign_category": "content",
        "campaign_status": "active",
        "campaign_budget": 3000.0,
        **CHANNELS["ch-content"],
    },
    "camp-enterprise": {
        "campaign_id": "camp-enterprise",
        "campaign_name": "Enterprise Outreach Q1",
        "campaign_channel": "outbound",
        "campaign_category": "outbound",
        "campaign_status": "active",
        "campaign_budget": 8000.0,
        **CHANNELS["ch-outbound"],
    },
    "camp-retarget": {
        "campaign_id": "camp-retarget",
        "campaign_name": "Retargeting Wave",
        "campaign_channel": "paid_search",
        "campaign_category": "retargeting",
        "campaign_status": "paused",
        "campaign_budget": 5000.0,
        **CHANNELS["ch-sem"],
    },
}

# Accounts
ACCOUNTS = {
    "acc-acme":    {"account_id": "acc-acme",    "account_name": "Acme Corp",       "account_domain": "acme.com",       "account_industry": "SaaS",        "account_type": "customer",  "account_employees": 250,  "account_arr": 48000.0},
    "acc-globex":  {"account_id": "acc-globex",  "account_name": "Globex Inc",      "account_domain": "globex.io",      "account_industry": "Fintech",     "account_type": "prospect",  "account_employees": 80,   "account_arr": 0.0},
    "acc-initech": {"account_id": "acc-initech", "account_name": "Initech Systems", "account_domain": "initech.com",    "account_industry": "Enterprise",  "account_type": "customer",  "account_employees": 1200, "account_arr": 120000.0},
    "acc-hooli":   {"account_id": "acc-hooli",   "account_name": "Hooli",           "account_domain": "hooli.xyz",      "account_industry": "Consumer",    "account_type": "churned",   "account_employees": 40,   "account_arr": 0.0},
    "acc-pied":    {"account_id": "acc-pied",    "account_name": "Pied Piper",      "account_domain": "piedpiper.com",  "account_industry": "SaaS",        "account_type": "prospect",  "account_employees": 15,   "account_arr": 0.0},
    "acc-dunder":  {"account_id": "acc-dunder",  "account_name": "Dunder Mifflin",  "account_domain": "dundermifflin.com", "account_industry": "Other",   "account_type": "customer",  "account_employees": 300,  "account_arr": 24000.0},
}

# Leads (lead_id -> dict of all their attributes + which campaign sourced them)
LEADS = [
    {"lead_id": "lead-001", "lead_status": "converted", "lead_source": "paid_search",  "lead_company": "Acme Corp",       "lead_domain": "acme.com",          "lead_score": 85.0, "lead_is_signup": 1, "camp": "camp-spring",     "contact_id": "con-001", "acc": "acc-acme"},
    {"lead_id": "lead-002", "lead_status": "converted", "lead_source": "organic",       "lead_company": "Globex Inc",      "lead_domain": "globex.io",         "lead_score": 72.0, "lead_is_signup": 1, "camp": "camp-ebook",      "contact_id": "con-002", "acc": "acc-globex"},
    {"lead_id": "lead-003", "lead_status": "converted", "lead_source": "outbound",      "lead_company": "Initech Systems", "lead_domain": "initech.com",       "lead_score": 91.0, "lead_is_signup": 0, "camp": "camp-enterprise", "contact_id": "con-003", "acc": "acc-initech"},
    {"lead_id": "lead-004", "lead_status": "new",       "lead_source": "paid_search",  "lead_company": "Hooli",           "lead_domain": "hooli.xyz",         "lead_score": 45.0, "lead_is_signup": 1, "camp": "camp-spring",     "contact_id": None,      "acc": "acc-hooli"},
    {"lead_id": "lead-005", "lead_status": "qualified", "lead_source": "organic",       "lead_company": "Pied Piper",      "lead_domain": "piedpiper.com",     "lead_score": 67.0, "lead_is_signup": 1, "camp": "camp-ebook",      "contact_id": "con-005", "acc": "acc-pied"},
    {"lead_id": "lead-006", "lead_status": "converted", "lead_source": "outbound",      "lead_company": "Dunder Mifflin",  "lead_domain": "dundermifflin.com", "lead_score": 78.0, "lead_is_signup": 0, "camp": "camp-enterprise", "contact_id": "con-006", "acc": "acc-dunder"},
    {"lead_id": "lead-007", "lead_status": "new",       "lead_source": "paid_search",  "lead_company": "Acme Corp",       "lead_domain": "acme.com",          "lead_score": 33.0, "lead_is_signup": 0, "camp": "camp-retarget",   "contact_id": None,      "acc": "acc-acme"},
    {"lead_id": "lead-008", "lead_status": "disqualified","lead_source": "organic",     "lead_company": "Unknown",         "lead_domain": "random.io",         "lead_score": 12.0, "lead_is_signup": 1, "camp": "camp-ebook",      "contact_id": None,      "acc": None},
    {"lead_id": "lead-009", "lead_status": "qualified", "lead_source": "outbound",      "lead_company": "Initech Systems", "lead_domain": "initech.com",       "lead_score": 88.0, "lead_is_signup": 0, "camp": "camp-enterprise", "contact_id": "con-009", "acc": "acc-initech"},
    {"lead_id": "lead-010", "lead_status": "new",       "lead_source": "paid_search",  "lead_company": "Pied Piper",      "lead_domain": "piedpiper.com",     "lead_score": 55.0, "lead_is_signup": 1, "camp": "camp-spring",     "contact_id": None,      "acc": "acc-pied"},
    {"lead_id": "lead-011", "lead_status": "converted", "lead_source": "organic",       "lead_company": "Dunder Mifflin",  "lead_domain": "dundermifflin.com", "lead_score": 80.0, "lead_is_signup": 0, "camp": "camp-ebook",      "contact_id": "con-011", "acc": "acc-dunder"},
    {"lead_id": "lead-012", "lead_status": "qualified", "lead_source": "outbound",      "lead_company": "Acme Corp",       "lead_domain": "acme.com",          "lead_score": 74.0, "lead_is_signup": 0, "camp": "camp-enterprise", "contact_id": "con-012", "acc": "acc-acme"},
]

# Deals
DEALS = [
    {"deal_id": "deal-001", "deal_name": "Acme Pro Upgrade",      "deal_stage": "closed_won",  "deal_amount": 48000.0, "deal_probability": 1.0,  "contact_id": "con-001", "acc": "acc-acme",    "camp": "camp-spring"},
    {"deal_id": "deal-002", "deal_name": "Initech Enterprise",    "deal_stage": "closed_won",  "deal_amount": 120000.0,"deal_probability": 1.0,  "contact_id": "con-003", "acc": "acc-initech", "camp": "camp-enterprise"},
    {"deal_id": "deal-003", "deal_name": "Dunder Mifflin Team",   "deal_stage": "negotiation", "deal_amount": 24000.0, "deal_probability": 0.75, "contact_id": "con-006", "acc": "acc-dunder",  "camp": "camp-enterprise"},
    {"deal_id": "deal-004", "deal_name": "Globex Starter",        "deal_stage": "proposal",    "deal_amount": 12000.0, "deal_probability": 0.4,  "contact_id": "con-002", "acc": "acc-globex",  "camp": "camp-ebook"},
    {"deal_id": "deal-005", "deal_name": "Pied Piper Growth",     "deal_stage": "discovery",   "deal_amount": 8400.0,  "deal_probability": 0.2,  "contact_id": "con-005", "acc": "acc-pied",    "camp": "camp-ebook"},
]

# Subscription events
SUB_EVENTS = [
    {"lead_id": "lead-001", "contact_id": "con-001", "acc": "acc-acme",    "camp": "camp-spring",     "sub_event_type": "signup",  "sub_plan_tier": "free",  "sub_plan_period": "monthly", "sub_arr": 0.0,      "sub_days_from_signup": 0,   "days_ago": 180, "sub_product_name": "CRM"},
    {"lead_id": "lead-001", "contact_id": "con-001", "acc": "acc-acme",    "camp": "camp-spring",     "sub_event_type": "upgrade", "sub_plan_tier": "pro",   "sub_plan_period": "annual",  "sub_arr": 48000.0,  "sub_days_from_signup": 14,  "days_ago": 166, "sub_product_name": "CRM"},
    {"lead_id": "lead-002", "contact_id": "con-002", "acc": "acc-globex",  "camp": "camp-ebook",      "sub_event_type": "signup",  "sub_plan_tier": "free",  "sub_plan_period": "monthly", "sub_arr": 0.0,      "sub_days_from_signup": 0,   "days_ago": 120, "sub_product_name": "CRM"},
    {"lead_id": "lead-003", "contact_id": "con-003", "acc": "acc-initech", "camp": "camp-enterprise", "sub_event_type": "signup",  "sub_plan_tier": "pro",   "sub_plan_period": "annual",  "sub_arr": 120000.0, "sub_days_from_signup": 0,   "days_ago": 90,  "sub_product_name": "Enterprise Suite"},
    {"lead_id": "lead-004", "contact_id": None,      "acc": "acc-hooli",   "camp": "camp-spring",     "sub_event_type": "signup",  "sub_plan_tier": "free",  "sub_plan_period": "monthly", "sub_arr": 0.0,      "sub_days_from_signup": 0,   "days_ago": 60,  "sub_product_name": "CRM"},
    {"lead_id": "lead-004", "contact_id": None,      "acc": "acc-hooli",   "camp": "camp-spring",     "sub_event_type": "churn",   "sub_plan_tier": "free",  "sub_plan_period": "monthly", "sub_arr": 0.0,      "sub_days_from_signup": 30,  "days_ago": 30,  "sub_product_name": "CRM"},
    {"lead_id": "lead-006", "contact_id": "con-006", "acc": "acc-dunder",  "camp": "camp-enterprise", "sub_event_type": "signup",  "sub_plan_tier": "team",  "sub_plan_period": "annual",  "sub_arr": 24000.0,  "sub_days_from_signup": 0,   "days_ago": 45,  "sub_product_name": "CRM"},
    {"lead_id": "lead-011", "contact_id": "con-011", "acc": "acc-dunder",  "camp": "camp-ebook",      "sub_event_type": "upgrade", "sub_plan_tier": "pro",   "sub_plan_period": "annual",  "sub_arr": 36000.0,  "sub_days_from_signup": 21,  "days_ago": 10,  "sub_product_name": "CRM"},
]

# Visitor touch events
VISITOR_EVENTS = [
    {"visitor_id": "vis-001", "visitor_channel": "paid_search", "visitor_signup_flow": "trial",     "visitor_signup_cluster": "smb",        "visitor_dept": "marketing", "visitor_seniority": "manager",   "visitor_product_intent": "crm",         "visitor_team_size": "11-50",   "camp": "camp-spring",     "days_ago": 200},
    {"visitor_id": "vis-002", "visitor_channel": "organic",     "visitor_signup_flow": "content",    "visitor_signup_cluster": "enterprise", "visitor_dept": "sales",     "visitor_seniority": "vp",        "visitor_product_intent": "pipeline",    "visitor_team_size": "201-500", "camp": "camp-ebook",      "days_ago": 150},
    {"visitor_id": "vis-003", "visitor_channel": "paid_search", "visitor_signup_flow": "trial",      "visitor_signup_cluster": "smb",        "visitor_dept": "ops",       "visitor_seniority": "director",  "visitor_product_intent": "crm",         "visitor_team_size": "51-200",  "camp": "camp-spring",     "days_ago": 130},
    {"visitor_id": "vis-004", "visitor_channel": "outbound",    "visitor_signup_flow": "demo",       "visitor_signup_cluster": "enterprise", "visitor_dept": "sales",     "visitor_seniority": "c_level",   "visitor_product_intent": "enterprise",  "visitor_team_size": "1001+",   "camp": "camp-enterprise", "days_ago": 100},
    {"visitor_id": "vis-005", "visitor_channel": "organic",     "visitor_signup_flow": "content",    "visitor_signup_cluster": "startup",    "visitor_dept": "engineering","visitor_seniority": "ic",        "visitor_product_intent": "workflow",    "visitor_team_size": "1-10",    "camp": "camp-ebook",      "days_ago": 80},
    {"visitor_id": "vis-006", "visitor_channel": "paid_search", "visitor_signup_flow": "retargeting","visitor_signup_cluster": "smb",        "visitor_dept": "marketing", "visitor_seniority": "manager",   "visitor_product_intent": "crm",         "visitor_team_size": "11-50",   "camp": "camp-retarget",   "days_ago": 65},
    {"visitor_id": "vis-007", "visitor_channel": "outbound",    "visitor_signup_flow": "demo",       "visitor_signup_cluster": "mid_market", "visitor_dept": "finance",   "visitor_seniority": "director",  "visitor_product_intent": "reporting",   "visitor_team_size": "51-200",  "camp": "camp-enterprise", "days_ago": 50},
    {"visitor_id": "vis-008", "visitor_channel": "organic",     "visitor_signup_flow": "trial",      "visitor_signup_cluster": "smb",        "visitor_dept": "sales",     "visitor_seniority": "ic",        "visitor_product_intent": "pipeline",    "visitor_team_size": "11-50",   "camp": "camp-ebook",      "days_ago": 30},
    {"visitor_id": "vis-009", "visitor_channel": "paid_search", "visitor_signup_flow": "trial",      "visitor_signup_cluster": "startup",    "visitor_dept": "product",   "visitor_seniority": "manager",   "visitor_product_intent": "crm",         "visitor_team_size": "1-10",    "camp": "camp-spring",     "days_ago": 20},
    {"visitor_id": "vis-010", "visitor_channel": "outbound",    "visitor_signup_flow": "demo",       "visitor_signup_cluster": "enterprise", "visitor_dept": "sales",     "visitor_seniority": "vp",        "visitor_product_intent": "enterprise",  "visitor_team_size": "501-1000","camp": "camp-enterprise", "days_ago": 10},
]


# ---------------------------------------------------------------------------
# Event builders
# ---------------------------------------------------------------------------

def _base(event_type: str, event_category: str, occurred_at: datetime, source_id: str, source_label: str) -> dict:
    return {
        "event_id": uid(),
        "tenant_id": TENANT,
        "event_type": event_type,
        "event_category": event_category,
        "occurred_at": occurred_at,
        "source_node_id": source_id,
        "source_label": source_label,
    }


def lead_events() -> list[dict]:
    rows = []
    for i, lead in enumerate(LEADS):
        camp = CAMPAIGNS[lead["camp"]]
        acc  = ACCOUNTS[lead["acc"]] if lead["acc"] else {}
        base = _base("lead.created", "lifecycle", dt(180 - i * 10), lead["lead_id"], "Lead")
        rows.append({
            **base,
            "lead_id":     lead["lead_id"],
            "lead_status": lead["lead_status"],
            "lead_source": lead["lead_source"],
            "lead_company":lead["lead_company"],
            "lead_domain": lead["lead_domain"],
            "lead_score":  lead["lead_score"],
            "lead_is_signup": lead["lead_is_signup"],
            **camp,
            **acc,
        })
    return rows


def deal_events() -> list[dict]:
    rows = []
    for i, deal in enumerate(DEALS):
        camp = CAMPAIGNS[deal["camp"]]
        acc  = ACCOUNTS[deal["acc"]]
        base = _base("deal.created", "pipeline", dt(90 - i * 10), deal["deal_id"], "Deal")
        rows.append({
            **base,
            "contact_id":      deal["contact_id"],
            "deal_id":         deal["deal_id"],
            "deal_name":       deal["deal_name"],
            "deal_stage":      deal["deal_stage"],
            "deal_amount":     deal["deal_amount"],
            "deal_probability":deal["deal_probability"],
            **camp,
            **acc,
        })
        if deal["deal_stage"] in ("closed_won", "negotiation"):
            stage_base = _base("deal.stage_changed", "pipeline", dt(85 - i * 10), deal["deal_id"], "Deal")
            rows.append({
                **stage_base,
                "deal_id":    deal["deal_id"],
                "deal_name":  deal["deal_name"],
                "deal_stage": deal["deal_stage"],
                "deal_amount":deal["deal_amount"],
                **camp,
                **acc,
            })
    return rows


def subscription_events() -> list[dict]:
    rows = []
    for s in SUB_EVENTS:
        camp = CAMPAIGNS[s["camp"]]
        acc  = ACCOUNTS[s["acc"]]
        base = _base(f"subscription.{s['sub_event_type']}", "subscription", dt(s["days_ago"]), s["lead_id"], "Lead")
        rows.append({
            **base,
            "lead_id":             s["lead_id"],
            "contact_id":          s.get("contact_id") or "",
            "sub_event_type":      s["sub_event_type"],
            "sub_plan_tier":       s["sub_plan_tier"],
            "sub_plan_period":     s["sub_plan_period"],
            "sub_arr":             s["sub_arr"],
            "sub_days_from_signup":s["sub_days_from_signup"],
            "sub_product_name":    s["sub_product_name"],
            **camp,
            **acc,
        })
    return rows


def visitor_events() -> list[dict]:
    rows = []
    for v in VISITOR_EVENTS:
        camp = CAMPAIGNS[v["camp"]]
        base = _base("visitor.touched", "attribution", dt(v["days_ago"]), v["visitor_id"], "Visitor")
        rows.append({
            **base,
            "visitor_id":             v["visitor_id"],
            "visitor_channel":        v["visitor_channel"],
            "visitor_signup_flow":    v["visitor_signup_flow"],
            "visitor_signup_cluster": v["visitor_signup_cluster"],
            "visitor_dept":           v["visitor_dept"],
            "visitor_seniority":      v["visitor_seniority"],
            "visitor_product_intent": v["visitor_product_intent"],
            "visitor_team_size":      v["visitor_team_size"],
            **camp,
        })
    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    settings = GtmdbSettings()
    async with await OlapStore.create(settings) as ch:
        await ch.bootstrap()

        all_rows = (
            lead_events()
            + deal_events()
            + subscription_events()
            + visitor_events()
        )

        n = await ch.insert_events(all_rows)
        print(f"Inserted {n} rows into ClickHouse events table")

        summary = await ch.query(
            """
            SELECT
                event_category,
                event_type,
                count() AS n
            FROM events
            WHERE tenant_id = {tid:String}
            GROUP BY event_category, event_type
            ORDER BY event_category, event_type
            """,
            {"tid": TENANT},
        )
        print(f"\n{'Category':<16} {'Event type':<28} {'Count':>5}")
        print("-" * 52)
        for r in summary:
            print(f"{r['event_category']:<16} {r['event_type']:<28} {r['n']:>5}")

        arr_summary = await ch.query(
            """
            SELECT
                campaign_name,
                campaign_channel,
                count() AS leads,
                round(sum(sub_arr), 0) AS total_arr
            FROM events
            WHERE tenant_id = {tid:String}
              AND event_category = 'subscription'
              AND sub_event_type = 'signup'
            GROUP BY campaign_name, campaign_channel
            ORDER BY total_arr DESC
            """,
            {"tid": TENANT},
        )
        print(f"\n{'Campaign':<30} {'Channel':<14} {'Leads':>6} {'ARR ($)':>10}")
        print("-" * 64)
        for r in arr_summary:
            print(f"{r['campaign_name']:<30} {r['campaign_channel']:<14} {r['leads']:>6} {r['total_arr']:>10,.0f}")


if __name__ == "__main__":
    asyncio.run(main())
