"""Seed ~80 realistic GTM nodes under tenant 'mat-test' in Neo4j Aura.

Run:  python scripts/seed_mat_test.py
Clean:  python scripts/seed_mat_test.py --clean
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import uuid
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from dotenv import load_dotenv
load_dotenv()

from neo4j import AsyncGraphDatabase

TENANT = "mat-test"
URI  = os.environ["GTMDB_NEO4J_URI"]
USER = os.environ.get("GTMDB_NEO4J_USER", "neo4j")
PW   = os.environ["GTMDB_NEO4J_PASSWORD"]

now = datetime.now(timezone.utc).isoformat()


def uid(prefix: str) -> str:
    return f"{TENANT}-{prefix}-{uuid.uuid4().hex[:8]}"


# ── fixed IDs so relationships wire up cleanly ──────────────────────────────

CH_EMAIL   = f"{TENANT}-ch-email"
CH_PAID    = f"{TENANT}-ch-paid"
CH_CONTENT = f"{TENANT}-ch-content"

CAMP_Q1    = f"{TENANT}-camp-q1"
CAMP_PAID  = f"{TENANT}-camp-paid"
CAMP_BLOG  = f"{TENANT}-camp-blog"

ACC_ACME   = f"{TENANT}-acc-acme"
ACC_NOVA   = f"{TENANT}-acc-nova"
ACC_ZEON   = f"{TENANT}-acc-zeon"

PROD_CORE  = f"{TENANT}-prod-core"
PROD_PRO   = f"{TENANT}-prod-pro"

LEADS = [
    {"id": f"{TENANT}-lead-{i}", "campaign": CAMP_Q1,   "account": ACC_ACME,
     "status": "qualified", "source": "email",  "score": 80 + i, "domain": "acme.io"}
    for i in range(1, 6)
] + [
    {"id": f"{TENANT}-lead-{i}", "campaign": CAMP_PAID, "account": ACC_NOVA,
     "status": "new",       "source": "paid",   "score": 60 + i, "domain": "nova.io"}
    for i in range(6, 11)
] + [
    {"id": f"{TENANT}-lead-{i}", "campaign": CAMP_BLOG, "account": ACC_ZEON,
     "status": "qualified", "source": "organic","score": 70 + i, "domain": "zeon.io"}
    for i in range(11, 16)
]

CONTACTS = [
    {"id": f"{TENANT}-con-{i}", "account": ACC_ACME,
     "name": f"Alice {i}", "title": "CTO",    "dept": "Engineering", "email": f"alice{i}@acme.io",
     "lead_id": f"{TENANT}-lead-{i}"}
    for i in range(1, 4)
] + [
    {"id": f"{TENANT}-con-{i}", "account": ACC_NOVA,
     "name": f"Bob {i}",   "title": "VP Sales", "dept": "Sales", "email": f"bob{i}@nova.io",
     "lead_id": f"{TENANT}-lead-{i}"}
    for i in range(6, 9)
]

DEALS = [
    {"id": f"{TENANT}-deal-1", "contact": f"{TENANT}-con-1", "campaign": CAMP_Q1,
     "name": "Acme Enterprise", "stage": "closed_won", "amount": 120000, "prob": 1.0},
    {"id": f"{TENANT}-deal-2", "contact": f"{TENANT}-con-2", "campaign": CAMP_Q1,
     "name": "Acme Pro",       "stage": "negotiation", "amount": 45000,  "prob": 0.7},
    {"id": f"{TENANT}-deal-3", "contact": f"{TENANT}-con-6", "campaign": CAMP_PAID,
     "name": "Nova Starter",   "stage": "proposal",    "amount": 18000,  "prob": 0.4},
]

PA_LIST = [
    {"id": f"{TENANT}-pa-1", "account": ACC_ACME, "lead": f"{TENANT}-lead-1",
     "name": "Acme PA Core", "region": "US", "country": "USA",
     "industry": "Tech", "size": "mid", "is_paying": True},
    {"id": f"{TENANT}-pa-2", "account": ACC_NOVA, "lead": f"{TENANT}-lead-6",
     "name": "Nova PA Pro",  "region": "EU", "country": "Germany",
     "industry": "Finance", "size": "large", "is_paying": True},
    {"id": f"{TENANT}-pa-3", "account": ACC_ZEON, "lead": f"{TENANT}-lead-11",
     "name": "Zeon PA Free", "region": "APAC", "country": "Singapore",
     "industry": "Retail", "size": "small", "is_paying": False},
]

SE_LIST = [
    {"id": f"{TENANT}-se-1", "pa": f"{TENANT}-pa-1", "product": PROD_CORE,
     "etype": "subscription.renewed", "tier": "enterprise", "period": "annual",
     "arr": 120000, "days": 365},
    {"id": f"{TENANT}-se-2", "pa": f"{TENANT}-pa-1", "product": PROD_PRO,
     "etype": "subscription.upgraded", "tier": "pro",       "period": "monthly",
     "arr": 24000,  "days": 90},
    {"id": f"{TENANT}-se-3", "pa": f"{TENANT}-pa-2", "product": PROD_PRO,
     "etype": "subscription.started", "tier": "pro",        "period": "annual",
     "arr": 48000,  "days": 0},
    {"id": f"{TENANT}-se-4", "pa": f"{TENANT}-pa-3", "product": PROD_CORE,
     "etype": "trial.started",        "tier": "free",       "period": "trial",
     "arr": 0,      "days": 0},
    {"id": f"{TENANT}-se-5", "pa": f"{TENANT}-pa-2", "product": PROD_CORE,
     "etype": "subscription.churned", "tier": "pro",        "period": "annual",
     "arr": -48000, "days": 400},
]

VISITORS = [
    {"id": f"{TENANT}-vis-{i}", "campaign": CAMP_BLOG,
     "channel": "organic", "flow": "blog-signup",
     "dept": "Engineering", "seniority": "senior"}
    for i in range(1, 6)
]

CONTENT = [
    {"id": f"{TENANT}-con-content-1", "campaign": CAMP_BLOG,
     "name": "How GTM works", "ctype": "blog_post", "url": "https://blog.example.com/gtm"},
    {"id": f"{TENANT}-con-content-2", "campaign": CAMP_Q1,
     "name": "Q1 Email Series", "ctype": "email_sequence", "url": ""},
]


async def seed(tx):
    t = TENANT

    # Channels
    await tx.run("""
        MERGE (n:Channel {id: $id, tenant_id: $t})
        SET n += {name: $name, channel_type: $ct, created_at: $now}
    """, id=CH_EMAIL,   t=t, name="Email",         ct="email",   now=now)
    await tx.run("""
        MERGE (n:Channel {id: $id, tenant_id: $t})
        SET n += {name: $name, channel_type: $ct, created_at: $now}
    """, id=CH_PAID,    t=t, name="Paid Search",   ct="paid",    now=now)
    await tx.run("""
        MERGE (n:Channel {id: $id, tenant_id: $t})
        SET n += {name: $name, channel_type: $ct, created_at: $now}
    """, id=CH_CONTENT, t=t, name="Content/SEO",   ct="organic", now=now)

    # Campaigns
    for cid, cname, ch, budget in [
        (CAMP_Q1,   "Q1 Outbound",    CH_EMAIL,   25000),
        (CAMP_PAID, "Paid Ads Q2",    CH_PAID,    40000),
        (CAMP_BLOG, "Blog & SEO",     CH_CONTENT,  8000),
    ]:
        await tx.run("""
            MERGE (c:Campaign {id: $id, tenant_id: $t})
            SET c += {name: $name, status: 'active', channel: $ch_type,
                      campaign_category: 'demand_gen', budget: $budget, created_at: $now}
            WITH c
            MATCH (ch:Channel {id: $ch_id, tenant_id: $t})
            MERGE (c)-[:BELONGS_TO]->(ch)
        """, id=cid, t=t, name=cname, ch_type=ch.split("-")[-1],
             ch_id=ch, budget=budget, now=now)

    # Accounts
    for aid, aname, domain, ind, emp, arr in [
        (ACC_ACME, "Acme Industries", "acme.io",  "Tech",    500,  500000),
        (ACC_NOVA, "Nova Corp",       "nova.io",  "Finance", 1200, 2000000),
        (ACC_ZEON, "Zeon Retail",     "zeon.io",  "Retail",  80,   50000),
    ]:
        await tx.run("""
            MERGE (n:Account {id: $id, tenant_id: $t})
            SET n += {name: $name, domain: $domain, industry: $ind,
                      type: 'prospect', employee_count: $emp,
                      annual_revenue: $arr, created_at: $now}
        """, id=aid, t=t, name=aname, domain=domain, ind=ind, emp=emp, arr=arr, now=now)

    # Products
    for pid, pname, ptype in [
        (PROD_CORE, "GTM Core", "saas"),
        (PROD_PRO,  "GTM Pro",  "saas"),
    ]:
        await tx.run("""
            MERGE (n:Product {id: $id, tenant_id: $t})
            SET n += {name: $name, product_type: $ptype, created_at: $now}
        """, id=pid, t=t, name=pname, ptype=ptype, now=now)

    # Leads
    for lead in LEADS:
        await tx.run("""
            MERGE (n:Lead {id: $id, tenant_id: $t})
            SET n += {status: $status, source: $source, score: $score,
                      domain: $domain, company_name: $domain,
                      is_signup: true, created_at: $now}
            WITH n
            MATCH (c:Campaign {id: $camp, tenant_id: $t})
            MERGE (n)-[:SOURCED_FROM]->(c)
        """, id=lead["id"], t=t, status=lead["status"], source=lead["source"],
             score=lead["score"], domain=lead["domain"], camp=lead["campaign"], now=now)

    # Contacts + WORKS_AT + CONVERTED_TO
    for con in CONTACTS:
        await tx.run("""
            MERGE (n:Contact {id: $id, tenant_id: $t})
            SET n += {name: $name, title: $title, department: $dept,
                      email: $email, created_at: $now}
            WITH n
            MATCH (a:Account {id: $acc, tenant_id: $t})
            MERGE (n)-[:WORKS_AT]->(a)
            WITH n
            MATCH (l:Lead {id: $lead, tenant_id: $t})
            MERGE (l)-[:CONVERTED_TO]->(n)
        """, id=con["id"], t=t, name=con["name"], title=con["title"],
             dept=con["dept"], email=con["email"],
             acc=con["account"], lead=con["lead_id"], now=now)

    # Deals
    for deal in DEALS:
        await tx.run("""
            MERGE (n:Deal {id: $id, tenant_id: $t})
            SET n += {name: $name, stage: $stage, amount: $amount,
                      probability: $prob, created_at: $now}
            WITH n
            MATCH (con:Contact {id: $con, tenant_id: $t})
            MERGE (n)-[:FOR_CONTACT]->(con)
            WITH n
            MATCH (c:Campaign {id: $camp, tenant_id: $t})
            MERGE (c)-[:INFLUENCED]->(n)
        """, id=deal["id"], t=t, name=deal["name"], stage=deal["stage"],
             amount=deal["amount"], prob=deal["prob"],
             con=deal["contact"], camp=deal["campaign"], now=now)

    # ProductAccounts
    for pa in PA_LIST:
        await tx.run("""
            MERGE (n:ProductAccount {id: $id, tenant_id: $t})
            SET n += {name: $name, region: $region, country: $country,
                      industry: $industry, company_size_group: $size,
                      is_paying: $paying, created_at: $now}
            WITH n
            MATCH (a:Account {id: $acc, tenant_id: $t})
            MERGE (n)-[:FOR_ACCOUNT]->(a)
            WITH n
            MATCH (l:Lead {id: $lead, tenant_id: $t})
            MERGE (l)-[:SIGNED_UP_AS]->(n)
        """, id=pa["id"], t=t, name=pa["name"], region=pa["region"],
             country=pa["country"], industry=pa["industry"],
             size=pa["size"], paying=pa["is_paying"],
             acc=pa["account"], lead=pa["lead"], now=now)

    # SubscriptionEvents
    for se in SE_LIST:
        await tx.run("""
            MERGE (n:SubscriptionEvent {id: $id, tenant_id: $t})
            SET n += {event_type: $etype, plan_tier: $tier, plan_period: $period,
                      arr: $arr, days_from_signup: $days,
                      occurred_at: $now, created_at: $now}
            WITH n
            MATCH (pa:ProductAccount {id: $pa, tenant_id: $t})
            MERGE (pa)-[:HAS_SUBSCRIPTION_EVENT]->(n)
            WITH n
            MATCH (p:Product {id: $prod, tenant_id: $t})
            MERGE (n)-[:FOR_PRODUCT]->(p)
        """, id=se["id"], t=t, etype=se["etype"], tier=se["tier"],
             period=se["period"], arr=se["arr"], days=se["days"],
             pa=se["pa"], prod=se["product"], now=now)

    # Visitors
    for vis in VISITORS:
        await tx.run("""
            MERGE (n:Visitor {id: $id, tenant_id: $t})
            SET n += {source_channel: $ch, signup_flow: $flow,
                      department: $dept, seniority: $sen, created_at: $now}
            WITH n
            MATCH (c:Campaign {id: $camp, tenant_id: $t})
            MERGE (n)-[:TOUCHED]->(c)
        """, id=vis["id"], t=t, ch=vis["channel"], flow=vis["flow"],
             dept=vis["dept"], sen=vis["seniority"], camp=vis["campaign"], now=now)

    # Content
    for ct in CONTENT:
        await tx.run("""
            MERGE (n:Content {id: $id, tenant_id: $t})
            SET n += {name: $name, content_type: $ctype, url: $url, created_at: $now}
            WITH n
            MATCH (c:Campaign {id: $camp, tenant_id: $t})
            MERGE (n)-[:BELONGS_TO]->(c)
        """, id=ct["id"], t=t, name=ct["name"], ctype=ct["ctype"],
             url=ct["url"], camp=ct["campaign"], now=now)


async def clean(tx):
    await tx.run(
        "MATCH (n {tenant_id: $t}) DETACH DELETE n", t=TENANT
    )
    print(f"Deleted all nodes with tenant_id='{TENANT}'")


async def count_nodes(session) -> int:
    r = await session.run(
        "MATCH (n {tenant_id: $t}) RETURN count(n) AS cnt", t=TENANT
    )
    return (await r.single())["cnt"]


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--clean", action="store_true", help="Delete all mat-test nodes")
    args = parser.parse_args()

    driver = AsyncGraphDatabase.driver(URI, auth=(USER, PW))
    async with driver.session() as session:
        if args.clean:
            await session.execute_write(clean)
        else:
            before = await count_nodes(session)
            print(f"Seeding mat-test tenant into Neo4j Aura…  (existing: {before} nodes)")
            await session.execute_write(seed)
            after = await count_nodes(session)
            print(f"Done. {after - before} new nodes created  ({after} total for tenant '{TENANT}')")
            node_types = [
                "Channel", "Campaign", "Account", "Product",
                "Lead", "Contact", "Deal",
                "ProductAccount", "SubscriptionEvent", "Visitor", "Content",
            ]
            for label in node_types:
                r = await session.run(
                    f"MATCH (n:{label} {{tenant_id: $t}}) RETURN count(n) AS c", t=TENANT
                )
                c = (await r.single())["c"]
                if c:
                    print(f"  {label:20s} {c}")
    await driver.close()


if __name__ == "__main__":
    asyncio.run(main())
