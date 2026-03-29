---
name: gtmdb
description: GtmDB — the world's best graph-native GTM data layer. Neo4j-backed accounts, leads, contacts, deals, campaigns, email programs, scores, relationships. Async Python SDK + REST API. Every write needs actor_id. Tiered edge reasoning. Scores only via leads.add_score.
metadata: {"openclaw": {"requires": {"bins": ["python3"]}, "homepage": "https://github.com/tomerfri12/gtm-db"}}
---

# GtmDB

GtmDB is the **graph-native CRM/GTM persistence engine**. It stores accounts, leads, contacts, deals, campaigns, email programs, scores, and the relationships between them in **Neo4j** — with full tenant isolation, field-level permissions, and audit trails.

Use this skill when the user needs to **read, write, search, or analyze CRM/GTM data** through the GtmDB Python SDK or REST API.

---

## Getting your API key

**You need an API key before you can do anything.** GtmDB authenticates every request.

1. **Ask the human** for a GtmDB API key, or
2. **Read it from an environment variable** (commonly `GTMDB_API_KEY` or `GTMDB_ADMIN_KEY`).

```python
import os
api_key = os.environ.get("GTMDB_API_KEY") or os.environ.get("GTMDB_ADMIN_KEY")
if not api_key:
    raise RuntimeError("No GtmDB API key found. Ask the user or set GTMDB_API_KEY.")
```

There are two key types:

| Type | Source | Permissions | Can manage keys? |
|------|--------|-------------|-----------------|
| **Admin** | `GTMDB_ADMIN_KEY` env var | Full access to everything | Yes |
| **Agent** | Provisioned via admin (`gtmdb_<key_id>_<secret>`) | Policy-scoped per key | No |

Use whichever you have. Both work the same way for data operations.

---

## Two ways to talk to GtmDB

### Option A: REST API (recommended for most agents)

The hosted REST API is at:

```
https://gtm-db-production.up.railway.app
```

Every `/v1/*` request requires:

```
Authorization: Bearer <api_key>
```

### Option B: Python SDK (for scripts or deeper integrations)

```bash
pip install "git+https://github.com/tomerfri12/gtm-db.git"
```

```python
from gtmdb import connect_gtmdb

db, scope = await connect_gtmdb(api_key=api_key)
# ... use db.leads, db.accounts, etc.
await db.close()
```

---

## REST API reference

Base URL: `https://gtm-db-production.up.railway.app`

### Public endpoints (no auth needed)

| Method | Path | Response |
|--------|------|----------|
| GET | `/health` | `{"status": "ok"}` |
| GET | `/docs` | OpenAPI interactive UI |
| GET | `/v1/schema` | Full schema: node types with fields, relationship catalog |

### CRUD endpoints (all entities)

Every entity type has the same five endpoints. Replace `{entity}` with the entity name from the table below.

| Entity | Path prefix | Domain fields |
|--------|-------------|---------------|
| **Accounts** | `/v1/accounts` | `name`, `domain`, `industry`, `employee_count`, `annual_revenue`, `website`, `type` |
| **Leads** | `/v1/leads` | `first_name`, `last_name`, `email`, `phone`, `title`, `company_name`, `domain`, `status`, `source`, `score`, `linkedin_url`, `snippet`, `outreach_email` |
| **Contacts** | `/v1/contacts` | `first_name`, `last_name`, `email`, `phone`, `title`, `company_name`, `department`, `linkedin_url` |
| **Deals** | `/v1/deals` | `name`, `amount`, `stage`, `probability`, `close_date`, `description`, `owner_id` |
| **Campaigns** | `/v1/campaigns` | `name`, `status`, `channel`, `budget`, `start_date`, `end_date`, `description` |
| **Email campaigns** | `/v1/email-campaigns` | `name`, `status`, `channel`, `budget`, `start_date`, `end_date`, `description`, `from_name`, `from_email`, `reply_to` |
| **Emails** | `/v1/emails` | `name`, `subject`, `body`, `from_name`, `from_email`, `reply_to`, `state`, `sequence_number`, `send_at` |

**The five CRUD operations:**

#### Create

```
POST /v1/{entity}
Content-Type: application/json
Authorization: Bearer <key>

{
  "actor_id": "my-agent",
  "name": "Acme Corp",
  "domain": "acme.com",
  "reasoning": "Optional audit note on why this was created"
}
```

Response: the created entity (only non-null fields).

#### Get one

```
GET /v1/{entity}/{id}
Authorization: Bearer <key>
```

#### List (with filtering)

```
GET /v1/{entity}?limit=50&offset=0&status=active&channel=email
Authorization: Bearer <key>
```

- `limit`: 1–500 (default 50)
- `offset`: 0+ (default 0)
- Any **domain field** as a query param filters by equality

Response: array of entity dicts.

#### Update

```
PATCH /v1/{entity}/{id}
Authorization: Bearer <key>

{
  "actor_id": "my-agent",
  "stage": "negotiation",
  "reasoning": "Customer agreed to terms"
}
```

#### Delete

```
DELETE /v1/{entity}/{id}
Authorization: Bearer <key>
```

Response: `{"deleted": true}`

### Relationship and link endpoints

These create typed graph edges between entities. **`reasoning` is always required** — explain *why* this link exists.

#### Link lead to campaign

```
POST /v1/leads/{lead_id}/link-campaign
Authorization: Bearer <key>

{"campaign_id": "...", "reasoning": "Registered via webinar landing page"}
```

#### Add lead to campaign (from campaign side)

```
POST /v1/campaigns/{campaign_id}/add-lead
Authorization: Bearer <key>

{"lead_id": "...", "reasoning": "Matched ICP from intent data"}
```

#### Assign contact to account

```
POST /v1/contacts/{contact_id}/assign-account
Authorization: Bearer <key>

{"account_id": "...", "reasoning": "Primary stakeholder identified in QBR"}
```

#### Assign deal to account

```
POST /v1/deals/{deal_id}/assign-account
Authorization: Bearer <key>

{"account_id": "...", "reasoning": "Parent account for this opportunity"}
```

#### Add contact to deal

```
POST /v1/deals/{deal_id}/add-contact
Authorization: Bearer <key>

{"contact_id": "...", "reasoning": "Economic buyer confirmed in discovery"}
```

### Scoring leads

Scores are **only** created through leads. Never try to POST to a `/scores` endpoint.

```
POST /v1/leads/{lead_id}/scores
Authorization: Bearer <key>

{
  "actor_id": "scoring-agent",
  "has_score_reasoning": "BANT fit based on discovery call notes",
  "total": 72,
  "score_type": "bant",
  "budget": 8,
  "authority": 9,
  "need": 7,
  "timeline": 6,
  "reasoning": "Strong budget signal from Q1 planning mention",
  "status": "qualified",
  "scored_by": "scoring-agent"
}
```

- **`has_score_reasoning`** is **required** (non-empty) — this goes on the HAS_SCORE edge
- **`score_type`** defaults to `"bant"`, **`total`** defaults to 0
- Optional BANT sub-scores: `budget`, `authority`, `need`, `timeline`
- `reasoning` is for the Score node itself (qualification narrative)

### Email campaigns with artifacts (composite create)

Create an email campaign with all its email steps and lead attributions in one call:

```
POST /v1/email-campaigns/with-artifacts
Authorization: Bearer <key>

{
  "actor_id": "nurture-agent",
  "name": "Q1 Nurture Sequence",
  "status": "draft",
  "from_name": "Jane at Acme",
  "from_email": "jane@acme.com",
  "emails": [
    {"subject": "Hi {{name}}, quick question", "body": "...", "sequence_number": 1},
    {"subject": "Following up", "body": "...", "sequence_number": 2},
    {"subject": "Last chance", "body": "...", "sequence_number": 3}
  ],
  "lead_ids": ["lead-uuid-1", "lead-uuid-2"],
  "sourced_from_reasoning": "Enrolled from inbound demo requests"
}
```

Response: `{"campaign": {...}, "email_ids": [...], "linked_lead_count": 2}`

- If `lead_ids` is non-empty, `sourced_from_reasoning` is **required**
- `has_email_reasoning` is optional (stored on EmailCampaign→Email edges)

### Search

```
GET /v1/search?q=acme&limit=25
Authorization: Bearer <key>
```

Response: `[{"type": "Account", "id": "...", "name": "Acme Corp"}, ...]`

- `q` is required (min 1 char)
- `limit`: 1–100 (default 25)
- Searches across all entity types by name/properties

### Explore (subgraph traversal)

```
GET /v1/entities/{entity_id}/explore?depth=2
Authorization: Bearer <key>
```

Returns a subgraph of connected nodes and edges around the given entity.

**`mode` parameter** (default: `compact`):

- **`?mode=compact`** (default) — node IDs grouped by type, lightweight edges. Fast and cheap:

```json
{
  "nodes": {"Account": ["id-1"], "Lead": ["id-2", "id-3"], "Campaign": ["id-4"]},
  "edges": [
    {"from": "id-2", "to": "id-4", "type": "SOURCED_FROM"},
    {"from": "id-2", "to": "id-1", "type": "WORKS_AT"}
  ],
  "truncated": {}
}
```

- **`?mode=full`** — full node properties on every node (heavier, use when you need field values):

```json
{
  "nodes": {
    "id-1": {"type": "Account", "id": "id-1", "name": "Acme Corp", "domain": "acme.com"},
    "id-2": {"type": "Lead", "id": "id-2", "name": "Jane Doe", "email": "jane@acme.com"}
  },
  "edges": [{"from": "id-2", "to": "id-1", "type": "WORKS_AT", "reasoning": "..."}],
  "truncated": {}
}
```

**Best practice:** Start with compact to understand the graph shape, then fetch specific nodes with `GET /v1/{entity}/{id}` or re-explore with `mode=full` if you need all properties at once.

### Admin: API key management

These require an **admin** API key.

```
POST   /v1/admin/keys              — Create an agent key
GET    /v1/admin/keys              — List active keys
DELETE /v1/admin/keys/{key_id}     — Revoke a key
POST   /v1/admin/keys/{key_id}/rotate — Rotate (revoke old, create new)
```

Create key body:

```json
{
  "owner_id": "sdr-agent",
  "owner_type": "agent",
  "preset_names": ["write_all"],
  "label": "SDR Bot production key",
  "expires_in_days": 90
}
```

Presets: `full_access`, `read_all`, `write_all`, `no_raw_content`. Combine as needed.

---

## Python SDK reference

### Connect

```python
from gtmdb import connect_gtmdb
import os

api_key = os.environ["GTMDB_API_KEY"]
db, scope = await connect_gtmdb(api_key=api_key)
```

Always close when done:

```python
await db.close()
```

### Accounts

```python
acc = await db.accounts.create(scope, actor_id="my-agent",
    name="Acme Corp", domain="acme.com", industry="SaaS",
    employee_count=500, annual_revenue=50000000)

acc = await db.accounts.get(scope, acc.id)
rows = await db.accounts.list(scope, limit=50, industry="SaaS")
acc = await db.accounts.update(scope, acc.id, actor_id="my-agent", type="enterprise")
await db.accounts.delete(scope, acc.id)
```

### Leads

```python
lead = await db.leads.create(scope, actor_id="my-agent",
    first_name="Jane", last_name="Doe", email="jane@acme.com",
    company_name="Acme", title="VP Engineering", source="webinar")

await db.leads.update(scope, lead.id, actor_id="my-agent", status="qualified")

# Link to campaign (reasoning required)
await db.leads.link_campaign(scope, lead.id, campaign.id,
    reasoning="Registered via webinar landing page")

# Score the lead (always use add_score, never db.scores.create)
await db.leads.add_score(scope, lead.id,
    actor_id="scoring-agent",
    has_score_reasoning="BANT fit from discovery call",
    total=72, score_type="bant",
    budget=8, authority=9, need=7, timeline=6)

scores = await db.leads.scores_for(scope, lead.id)
```

**Lead statuses:** `"new"` (default), `"contacted"`, `"qualified"`, `"unqualified"`, etc. — free-form string, but be consistent.

### Contacts

```python
contact = await db.contacts.create(scope, actor_id="my-agent",
    first_name="Sam", last_name="Lee", email="sam@acme.com",
    title="CTO", department="Engineering")

# Assign to account (reasoning required)
await db.contacts.assign_to_account(scope, contact.id, acc.id,
    reasoning="Primary technical stakeholder")

# List contacts for an account
contacts = await db.contacts.for_account(scope, acc.id, limit=50)
```

### Deals

```python
deal = await db.deals.create(scope, actor_id="my-agent",
    name="Acme expansion", amount=150000, stage="proposal",
    probability=0.7, close_date="2026-06-30")

await db.deals.assign_to_account(scope, deal.id, acc.id,
    reasoning="Parent account for this opportunity")

await db.deals.add_contact(scope, deal.id, contact.id,
    reasoning="Economic buyer from procurement")

deals = await db.deals.for_account(scope, acc.id)
```

### Campaigns

```python
camp = await db.campaigns.create(scope, actor_id="my-agent",
    name="Q1 Outbound", channel="email", status="active",
    budget=10000, start_date="2026-01-01", end_date="2026-03-31")

await db.campaigns.add_lead(scope, camp.id, lead.id,
    reasoning="Matched ICP from intent data")
```

### Email campaigns (with full email sequence)

```python
result = await db.email_campaigns.create_with_artifacts(scope,
    actor_id="my-agent",
    name="Nurture A",
    status="draft",
    from_name="Jane",
    from_email="jane@acme.com",
    emails=[
        {"subject": "Hi {{name}}", "body": "Intro email...", "sequence_number": 1},
        {"subject": "Quick follow-up", "body": "Follow-up...", "sequence_number": 2},
    ],
    lead_ids=[lead.id],
    sourced_from_reasoning="Enrolled from inbound demo request")

campaign = result["campaign"]
email_ids = result["email_ids"]
```

### Relationships (generic edges)

For edges **not covered by typed helpers** above (e.g. Lead→Account WORKS_AT, Campaign→Deal INFLUENCED):

```python
await db.relationships.create(scope, lead.id, "WORKS_AT", acc.id,
    reasoning="Domain match and title confirms employment")

rels = await db.relationships.list(scope, acc.id,
    rel_type="WORKS_AT", direction="in", limit=100)

await db.relationships.delete(scope, from_id=lead.id,
    rel_type="WORKS_AT", to_id=acc.id)
```

**Always prefer typed helpers** (`contacts.assign_to_account`, `leads.link_campaign`, etc.) when they exist. Use `relationships.create` only for edge types without a dedicated method.

### Actors

Actors represent humans or AI agents in the audit trail. They're auto-created when you pass `actor_id` to create/update, but you can also manage them explicitly:

```python
await db.actors.ensure(scope, "my-agent", kind="ai", display_name="SDR Bot")

from gtmdb.api.models import ActorSpec
await db.actors.create(scope, [
    ActorSpec(id="human-42", kind="human", display_name="Pat Chen"),
])
```

### Advanced: traversals and graph queries

```python
subgraph = await db.entity_360(scope, entity_id, depth=2)
neighbors = await db.get_neighbors(scope, entity_id)
results = await db.search(scope, query="acme", limit=25)
pipeline = await db.pipeline(scope, stage="proposal")
attribution = await db.campaign_attribution(scope, campaign_id)
raw = await db.execute_cypher(scope, "MATCH (n:Lead) RETURN n LIMIT 5")
```

---

## Graph data model

```
Account ←[WORKS_AT]— Contact
Account ←[BELONGS_TO]— Deal
Deal —[HAS_CONTACT]→ Contact
Lead —[SOURCED_FROM]→ Campaign / EmailCampaign
Lead —[CONVERTED_TO]→ Contact
Lead ←[HAS_SCORE]— Score
Campaign —[INFLUENCED]→ Deal
EmailCampaign —[HAS_EMAIL]→ Email
Actor —[CREATED_BY]→ * (any entity)
Actor —[UPDATED_BY]→ * (any entity)
```

**Key relationships:**
- **WORKS_AT**: Contact/Lead → Account (person works at company)
- **SOURCED_FROM**: Lead → Campaign (lead attribution)
- **BELONGS_TO**: Deal → Account (deal is for this company)
- **HAS_CONTACT**: Deal → Contact (stakeholder on deal)
- **HAS_SCORE**: Score → Lead (lead scoring)
- **CONVERTED_TO**: Lead → Contact (lead became a customer contact)
- **INFLUENCED**: Campaign → Deal (campaign influenced the deal)
- **HAS_EMAIL**: EmailCampaign → Email (sequence steps)
- **CREATED_BY / UPDATED_BY**: Actor → any (audit trail)

---

## Critical rules

1. **`actor_id` is required on every create and update.** It identifies who/what made the change. Use your agent name (e.g. `"sdr-bot"`, `"enrichment-agent"`).

2. **`reasoning` is required on every relationship/link.** Always explain *why* two entities are connected. This is non-negotiable — the API will reject empty reasoning on link methods.

3. **Never create scores directly.** Always use `db.leads.add_score()` or `POST /v1/leads/{id}/scores`. `db.scores.create()` raises `TypeError`.

4. **`name` is auto-derived** for Leads, Contacts, Emails, and Scores. Don't set it manually — it's computed from other fields (first+last name, subject line, score_type:total, etc.).

5. **Lead `status` defaults to `"new"`.** Update it as the lead progresses: `"contacted"`, `"qualified"`, `"unqualified"`, etc.

6. **Email `state` defaults to `"draft"`.** Update when sent.

7. **Tenant isolation is automatic.** Every query is scoped to the tenant in your API key. You cannot see or modify data from other tenants.

8. **Use typed link methods over generic `relationships.create`** when a dedicated method exists (e.g. `contacts.assign_to_account` instead of manually creating a WORKS_AT edge).

9. **Response format:** Entity dicts only include non-null fields. System fields (`id`, `tenant_id`, `created_at`, `updated_at`, `created_by_actor_id`) are included automatically.

10. **Filtering on list:** Pass any domain field as a query parameter for equality filtering. Only known domain fields are used; unknown params are ignored.

---

## Common workflows

### Inbound lead processing

```python
db, scope = await connect_gtmdb(api_key=api_key)

# 1. Create the lead
lead = await db.leads.create(scope, actor_id="inbound-processor",
    first_name="Jane", last_name="Doe", email="jane@acme.com",
    company_name="Acme Corp", title="VP Engineering",
    source="demo_request", status="new")

# 2. Find or create the account
accounts = await db.accounts.list(scope, domain="acme.com")
if accounts:
    acc = accounts[0]
else:
    acc = await db.accounts.create(scope, actor_id="inbound-processor",
        name="Acme Corp", domain="acme.com")

# 3. Link lead to account
await db.relationships.create(scope, lead.id, "WORKS_AT", acc.id,
    reasoning="Email domain matches account domain")

# 4. Attribute to campaign
await db.leads.link_campaign(scope, lead.id, campaign_id,
    reasoning="Demo request form submission on landing page")

# 5. Score the lead
await db.leads.add_score(scope, lead.id, actor_id="inbound-processor",
    has_score_reasoning="Inbound demo request from VP at target account",
    total=85, score_type="bant",
    budget=8, authority=9, need=9, timeline=7)

# 6. Qualify
await db.leads.update(scope, lead.id, actor_id="inbound-processor",
    status="qualified",
    reasoning="High BANT score, target ICP, active buying signal")

await db.close()
```

### REST equivalent of the same workflow

```bash
# Create lead
curl -X POST https://gtm-db-production.up.railway.app/v1/leads \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"actor_id":"inbound-processor","first_name":"Jane","last_name":"Doe",
       "email":"jane@acme.com","company_name":"Acme Corp","title":"VP Engineering",
       "source":"demo_request"}'

# List accounts by domain
curl "https://gtm-db-production.up.railway.app/v1/accounts?domain=acme.com" \
  -H "Authorization: Bearer $API_KEY"

# Link lead to campaign
curl -X POST https://gtm-db-production.up.railway.app/v1/leads/{lead_id}/link-campaign \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"campaign_id":"...","reasoning":"Demo request form submission"}'

# Score the lead
curl -X POST https://gtm-db-production.up.railway.app/v1/leads/{lead_id}/scores \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"actor_id":"inbound-processor","has_score_reasoning":"High BANT from VP",
       "total":85,"score_type":"bant"}'
```

### Building a deal pipeline

```python
# Create deal
deal = await db.deals.create(scope, actor_id="ae-agent",
    name="Acme enterprise expansion", amount=250000,
    stage="discovery", probability=0.3)

# Attach to account
await db.deals.assign_to_account(scope, deal.id, acc.id,
    reasoning="Expansion deal for existing customer")

# Add stakeholders
await db.deals.add_contact(scope, deal.id, cto.id,
    reasoning="Technical decision maker")
await db.deals.add_contact(scope, deal.id, cfo.id,
    reasoning="Budget holder, signs off on purchases over 100k")

# Progress the deal
await db.deals.update(scope, deal.id, actor_id="ae-agent",
    stage="proposal", probability=0.6,
    reasoning="Sent proposal after positive technical review")
```

### Launching an email campaign

```python
result = await db.email_campaigns.create_with_artifacts(scope,
    actor_id="nurture-agent",
    name="Post-webinar nurture",
    status="active",
    from_name="Alex from Acme",
    from_email="alex@acme.com",
    reply_to="alex@acme.com",
    emails=[
        {"subject": "Great meeting you at the webinar",
         "body": "Hi {{name}},\n\nThanks for attending...",
         "sequence_number": 1},
        {"subject": "The ROI calculator I mentioned",
         "body": "Hi {{name}},\n\nHere's the link...",
         "sequence_number": 2,
         "send_at": "2026-04-05T09:00:00Z"},
        {"subject": "Quick question about your timeline",
         "body": "Hi {{name}},\n\nWanted to check...",
         "sequence_number": 3,
         "send_at": "2026-04-10T09:00:00Z"},
    ],
    lead_ids=[lead1.id, lead2.id, lead3.id],
    sourced_from_reasoning="Attended Q1 product webinar")
```

---

## Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `GTMDB_NEO4J_URI` | Yes | Neo4j connection URI |
| `GTMDB_NEO4J_USER` | Yes | Neo4j username |
| `GTMDB_NEO4J_PASSWORD` | Yes | Neo4j password |
| `GTMDB_ADMIN_KEY` | For admin ops | Admin API key |
| `GTMDB_DEFAULT_TENANT_ID` | No | UUID tenant (default provided) |
| `GTMDB_KEY_STORE_URL` | For agent keys | Postgres DSN (`postgresql+asyncpg://...`) |
| `GTMDB_NEO4J_FORCE_DIRECT_BOLT` | No | Set `true` if routing times out (PaaS) |
| `GTMDB_NEO4J_CONNECTION_TIMEOUT` | No | Driver TCP timeout (seconds) |
| `GTMDB_NEO4J_CONNECTION_ACQUISITION_TIMEOUT` | No | Pool acquisition timeout (seconds) |

---

## Error handling

| HTTP code | Meaning |
|-----------|---------|
| 400 | Missing `actor_id`, empty `reasoning` where required, invalid field values |
| 401 | Missing or invalid API key |
| 403 | Insufficient permissions (scope doesn't allow this operation) |
| 404 | Entity or key not found |

In the Python SDK, these map to `ValueError` (400), `PermissionError` (403), and `None` returns (404 on get).

---

## Permission presets (for admin key creation)

| Preset | Description |
|--------|-------------|
| `full_access` | Read and write all entity types |
| `read_all` | Read-only access to all entity types |
| `write_all` | Write access to all entity types |
| `no_raw_content` | Deny read on sensitive fields (email body, etc.) |

Combine with commas: `["write_all", "no_raw_content"]`
