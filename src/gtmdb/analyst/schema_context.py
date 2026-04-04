"""Builds the schema context string injected into the analyst agent's system prompt.

Describes both data sources so the LLM knows:
- Which questions go to ClickHouse (aggregations, metrics, trends)
- Which questions go to Neo4j (traversal, relationships, paths)
- The exact column / label names to use
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# ClickHouse events table schema (compact, prompt-friendly)
# ---------------------------------------------------------------------------

CLICKHOUSE_SCHEMA = """
## ClickHouse — `events` table
Single denormalized table. Every row = one GTM event with its full graph context
pre-joined. Ideal for aggregations, trend analysis, and metrics.

**Always filter by `tenant_id = '<tenant>'` in every query.**

### Key columns
| Group | Columns |
|---|---|
| Identity | event_id, event_type, event_category, occurred_at, source_label, source_node_id |
| Campaign | campaign_id, campaign_name, campaign_channel, campaign_category, campaign_status, campaign_budget |
| Channel | channel_id, channel_name, channel_type |
| Lead | lead_id, lead_status, lead_source, lead_domain, lead_score, lead_is_signup |
| Contact | contact_id, contact_name, contact_title, contact_dept, contact_email |
| Account | account_id, account_name, account_domain, account_industry, account_type, account_employees, account_arr |
| Deal | deal_id, deal_name, deal_stage, deal_amount, deal_probability, deal_close_date |
| Subscription | sub_event_type, sub_plan_tier, sub_plan_period, sub_arr, sub_days_from_signup |
| Product | product_id, product_name, product_type |
| ProductAccount | product_account_id, product_account_name, product_account_region, product_account_country, product_account_industry, product_account_size_group, product_account_is_paying |
| Visitor | visitor_id, visitor_channel, visitor_signup_flow, visitor_signup_cluster, visitor_dept, visitor_seniority, visitor_product_intent, visitor_team_size |
| Content | content_id, content_name, content_type, content_url |

### Common event_type values
- visitor.touched, visitor.created
- lead.created, lead.updated
- subscription.event (sub_event_type: signup, purchase, churn, upgrade, renewal)
- product_account.created
- deal.created, deal.updated

### Example queries
```sql
-- Campaign conversion funnel
SELECT campaign_name,
       countIf(source_label='Visitor') AS visitors,
       countIf(source_label='ProductAccount') AS signups,
       countIf(sub_event_type='purchase') AS purchases,
       round(sumIf(sub_arr, sub_arr > 0), 0) AS arr
FROM events
WHERE tenant_id = '<tenant>' AND campaign_id != ''
GROUP BY campaign_name ORDER BY arr DESC;

-- ARR by channel
SELECT campaign_channel, sum(sub_arr) AS arr
FROM events
WHERE tenant_id = '<tenant>' AND source_label = 'SubscriptionEvent' AND sub_arr > 0
GROUP BY campaign_channel ORDER BY arr DESC;
```
""".strip()


# ---------------------------------------------------------------------------
# Neo4j graph schema (compact, prompt-friendly)
# ---------------------------------------------------------------------------

NEO4J_SCHEMA = """
## Neo4j — property graph
Ideal for path queries, relationship traversals, and finding connections.

**Always filter by `tenant_id: $tenant_id` on every node pattern.**

### Node labels & key properties
| Label | Key properties |
|---|---|
| Campaign | id, name, channel, status, budget, campaign_category |
| Channel | id, name, channel_type |
| Lead | id, status, source, score, domain, company_name, is_signup |
| Contact | id, name, title, department, email |
| Account | id, name, domain, industry, type, employee_count, annual_revenue |
| Deal | id, name, stage, amount, probability, close_date |
| Product | id, name, product_type |
| ProductAccount | id, name, region, country, industry, company_size_group, is_paying, status |
| SubscriptionEvent | id, event_type, plan_tier, plan_period, arr, days_from_signup, occurred_at |
| Visitor | id, visitor_id, source_channel, signup_flow, signup_cluster, seniority, product_intent |
| Content | id, name, content_type, url |

### Relationships
```
(Visitor)-[:TOUCHED]->(Campaign)
(Visitor)-[:SIGNED_UP_AS]->(ProductAccount)
(ProductAccount)-[:HAS_SUBSCRIPTION_EVENT]->(SubscriptionEvent)
(SubscriptionEvent)-[:FOR_PRODUCT]->(Product)
(ProductAccount)-[:FOR_ACCOUNT]->(Account)
(Lead)-[:SOURCED_FROM]->(Campaign)
(Lead)-[:CONVERTED_TO]->(Contact)
(Contact)-[:WORKS_AT]->(Account)
(Deal)-[:FOR_CONTACT]->(Contact)
(Campaign)-[:INFLUENCED]->(Deal)
(Campaign)-[:BELONGS_TO]->(Channel)
(Content)-[:BELONGS_TO]->(Campaign)
```

### Example queries
```cypher
// Which visitors from a campaign became paying customers?
MATCH (v:Visitor {tenant_id: $tenant_id})-[:TOUCHED]->(c:Campaign {id: $campaign_id})
MATCH (v)-[:SIGNED_UP_AS]->(pa:ProductAccount {is_paying: '1'})
RETURN v.visitor_id, pa.name, pa.country

// Campaign influence chain
MATCH p=(c:Campaign {tenant_id: $tenant_id})-[:INFLUENCED]->(d:Deal)-[:FOR_CONTACT]->(con:Contact)
RETURN c.name, d.name, d.amount, con.name
```
""".strip()


# ---------------------------------------------------------------------------
# Decision guide for the planner
# ---------------------------------------------------------------------------

ROUTING_GUIDE = """
## When to use which data source

Use **ClickHouse** for:
- Counting, summing, averaging across many rows (metrics, KPIs)
- Trend analysis over time (GROUP BY toYYYYMM(occurred_at))
- Campaign performance comparison, funnel analysis, ARR attribution
- Any question containing: "how many", "total", "average", "trend", "top N", "conversion rate"

Use **Neo4j** for:
- Finding specific paths or connections ("who introduced X to Y?")
- Neighborhood queries ("all contacts at accounts that churned")
- Shortest path, reachability, influence tracing
- Any question containing: "connected to", "path from", "influenced by", "relationships"

Use **both** (fuse results) for:
- "Which campaigns have the best LTV?" → ClickHouse for ARR sums, Neo4j to enrich with full campaign graph
- "Show me the content that converted the most visitors" → ClickHouse for counts, Neo4j for content details
""".strip()


def build_system_prompt(tenant_id: str) -> str:
    """Full system prompt injected into the analyst agent."""
    return f"""You are the gtmDB Analyst — an expert data analyst agent with access to a
Go-To-Market (GTM) database. You answer questions by generating and executing
database queries across two data sources, then synthesizing the results into
a clear, structured answer.

## Your tenant
tenant_id = "{tenant_id}"
Always scope every query to this tenant.

{CLICKHOUSE_SCHEMA}

{NEO4J_SCHEMA}

{ROUTING_GUIDE}

## Rules
1. Generate ONE query at a time. Execute it. Use the result to decide the next step.
2. If a query fails, analyze the error and retry with a corrected query (max 2 retries).
3. Always return a structured final answer with: summary, key numbers, and the raw data.
4. Never hallucinate data — only report what the queries actually returned.
5. If results are empty, say so clearly and suggest why.
6. Do not ask clarifying questions — make a reasonable assumption and state it.
"""
