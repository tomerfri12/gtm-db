---
name: gtmdb
description: GtmDB — GTM system of record as a graph (accounts, product accounts, products, leads, contacts, deals, campaigns, visitors, subscription events, links). REST or code (curl, httpx, Python SDK) with scoped API keys; prefer compact explore + targeted GET over mode=full. Reasoning on writes.
metadata: {"openclaw": {"requires": {"bins": ["python3"]}, "homepage": "https://github.com/tomerfri12/gtm-db"}}
---

# GtmDB — agent skill

Use this skill when the user wants **go-to-market (GTM) truth**: people, companies, pipeline, programs, touches, and **how they connect**—not a one-off export. You interact through an **HTTP API** (REST) with a **Bearer token**, or through **code** that performs the same requests. You do **not** need implementation details of how the server stores data internally.

---

## 1. What this system is (and why it helps you)

### GTM and **one source of truth**

**GTM (go-to-market)** is everything your org does to **find, win, and grow customers**: marketing programs, inbound and outbound motion, sales pipeline, and (in mature setups) customer-success context on the same accounts. In most companies that reality is **fragmented**—spreadsheets, partial CRM exports, and tool-specific APIs that don’t agree.

**GtmDB is meant to be the system of record for that world:** **one authoritative store** where marketing, sales, and ops-aligned workflows read and write the **same** accounts, leads, contacts, deals, campaigns, and the **relationships** between them (attribution, ownership, influence, enrollment—not just unrelated tables). When a human or an agent asks “what happened with this account?” or “where did this lead come from?”, the answer should come from **this layer**, not from three conflicting copies.

That matches the product intent in the official docs: **one system of record for GTM—and the agents that use it**—with typed reads/writes and traversals over the **commercial graph** (the full entity model in **`GET /v1/schema`**) so **MCP servers, tools, and automations** can share **one** integration surface instead of re-wrapping a different partial API per system.

**Security is part of the same story:** every call runs under a **scoped** permission model (tenant isolation, what you may read/write, sometimes field masking). Different keys can represent a human rep, an internal agent, or a partner—so **what you see is what was deliberately allowed**, which is how autonomous GTM stays governable.

### The graph shape (how you should think)

**GtmDB** exposes that system of record as a **graph**:

- **Nodes** are entities: accounts (companies), product accounts (workspace in a product), products, leads, contacts, deals, campaigns, visitors, subscription lifecycle events, email programs, content, etc.
- **Relationships** carry meaning the business cares about: e.g. a lead **sourced from** a campaign, a contact **works at** an account, a deal **belongs to** an account, stakeholders **on** a deal.

### Why that helps **you** (the agent)

- **Multi-hop questions** (e.g. contact → deals → account → campaigns) have a natural home instead of the human stitching spreadsheets.
- **Scoped keys** reduce blast radius: you operate inside explicit read/write rules.
- **Reasoning on writes** (see §10) makes autonomous changes **auditable**—aligned with how a real system of record is governed.

### Run GtmDB **as code** (same API, executable)

You are not limited to describing curl in chat. **Execute** the same operations from a terminal or script the human approves:

**Shell (`curl`)**

```bash
export GTMDB_BASE="https://your-instance.example"   # human provides
export GTMDB_API_KEY="…"                            # human provides

curl -sS -H "Authorization: Bearer $GTMDB_API_KEY" \
  "$GTMDB_BASE/v1/schema" | head -c 2000
```

```bash
curl -sS -H "Authorization: Bearer $GTMDB_API_KEY" \
  "$GTMDB_BASE/v1/search?q=acme&limit=5"
```

**Python (`httpx` or `requests`)** — same headers and URLs as §3:

```python
import os, httpx

base = os.environ["GTMDB_BASE"].rstrip("/")
key = os.environ["GTMDB_API_KEY"]

def gtm(method: str, path: str, **kw):
    r = httpx.request(
        method,
        f"{base}{path}",
        headers={"Authorization": f"Bearer {key}"},
        timeout=60.0,
        **kw,
    )
    r.raise_for_status()
    return r.json() if r.content else None

rows = gtm("GET", "/v1/search", params={"q": "acme", "limit": 10})
```

**Python SDK (async, in-process)** — mirrors the REST rules (`actor_id`, link `reasoning`, scores only via leads):

```python
import os
from gtmdb import connect_gtmdb

api_key = os.environ["GTMDB_API_KEY"]
db, scope = await connect_gtmdb(api_key=api_key)
lead = await db.leads.get(scope, "…uuid…")
await db.close()
```

Use whichever style the human’s environment supports; the **contract** (paths, body fields, permissions) is the same.

---

## 2. API key: ask the human, keep it, respect limits

**Before any call**, you need:

1. **Base URL** of the GtmDB instance (e.g. the human’s deployed server or their dev URL).
2. **API key** string the human gives you.

**Ask the human** for both if missing. Suggest they store the key where you can reuse it next time (e.g. env var `GTMDB_API_KEY`, a password manager note they paste in, or their agent secrets—whatever *they* use).

**How keys relate to what you can do**

- Keys are tied to a **tenant** and a **permission profile**. That profile defines what you may **read**, **write**, and which **fields** you see—the human who gave you the key is responsible for it matching your task.
- **You only have the permissions on *your* key.** If something fails with **403**, you may be blocked for that operation or field—not necessarily because the data doesn’t exist.

**If you truly have no key**

- You cannot call authenticated routes. Say clearly that the human must provide a GtmDB API key (and base URL) before you can query or update data.

**Optional check** (no auth):

```http
GET {BASE_URL}/health
```

---

## 3. API

Every authenticated request:

```http
Authorization: Bearer <your_api_key>
```

Below, `{BASE}` means the server root (no trailing slash), e.g. `https://example.com`.

### Discovery (still “data” for you)

| Method | Path | Purpose |
|--------|------|--------|
| GET | `{BASE}/v1/schema` | Node types, fields, relationship names—use when you’re unsure what exists. |
| GET | `{BASE}/v1/search?q=...&limit=...` | Find entities by text across types (`limit` 1–100, default 25). |

### CRUD — same pattern for every entity collection

Replace `{entities}` with the path segment (plural, kebab-case if needed):

| Collection path | Typical role |
|-----------------|---------------|
| `/v1/accounts` | Companies / orgs |
| `/v1/leads` | Prospects / inbound records |
| `/v1/contacts` | People (often post-sale or qualified) |
| `/v1/deals` | Opportunities |
| `/v1/campaigns` | Marketing / outbound campaigns |
| `/v1/email-campaigns` | Sequences with multiple emails |
| `/v1/emails` | Single email artifacts / steps |
| `/v1/channels` | Channel grouping (e.g. SEM, paid social) |
| `/v1/products` | Product lines / SKUs |
| `/v1/product-accounts` | Customer workspace in one product; upstream id in `external_id` (not the company id) |
| `/v1/content` | Landing pages, assets |
| `/v1/visitors` | Site visitors (anonymous or identified); upstream id in `visitor_id` |
| `/v1/subscription-events` | Signup, purchase, churn, etc. (timestamps and plan fields) |

**Operations**

```http
POST   {BASE}/v1/{entities}           → create (JSON body)
GET    {BASE}/v1/{entities}/{id}     → one record
GET    {BASE}/v1/{entities}?limit=50&offset=0&field=value  → list + equality filters on domain fields
PATCH  {BASE}/v1/{entities}/{id}      → partial update (JSON body)
DELETE {BASE}/v1/{entities}/{id}     → delete
```

- `limit` usually 1–500, `offset` ≥ 0.
- Query params that match **domain field names** filter by **exact match**. Unknown params are ignored.

**Create / update bodies** must include **`actor_id`** (who is acting—use a stable id for yourself, e.g. `cursor-agent` or the name the human prefers). Optional **`reasoning`** on creates/updates is strongly recommended (see §10).

### Typed links (relationships)

These create graph edges. **`reasoning` is required** on the link payloads (non-empty string explaining *why*).

| Action | Method + path (conceptually) |
|--------|------------------------------|
| Lead → campaign | `POST /v1/leads/{lead_id}/link-campaign` body: `campaign_id`, `reasoning` |
| Lead → Account or ProductAccount (signup) | `POST /v1/leads/{lead_id}/sign-up-as` body: exactly one of `account_id` or `product_account_id`, plus `reasoning` (`SIGNED_UP_AS`) |
| Campaign → lead | `POST /v1/campaigns/{campaign_id}/add-lead` body: `lead_id`, `reasoning` |
| Contact → account | `POST /v1/contacts/{contact_id}/assign-account` body: `account_id`, `reasoning` |
| Deal → account | `POST /v1/deals/{deal_id}/assign-account` body: `account_id`, `reasoning` |
| Deal → contact | `POST /v1/deals/{deal_id}/add-contact` body: `contact_id`, `reasoning` |

**Visitor, subscription events, and attribution edges**

There are no dedicated REST “link” routes for **TOUCHED**, **HAS_SUBSCRIPTION_EVENT**, **BELONGS_TO** (product account → company), or **FOR_PRODUCT** (product account → product line) yet—except **Lead** **`SIGNED_UP_AS`** via **`POST /v1/leads/{id}/sign-up-as`**. For **Visitor** **`SIGNED_UP_AS`** and other edges, use **`db.relationships.create(..., reasoning="…")`** with the relationship types listed in §5, subject to your key’s policies.

**Scores (only via leads)**

- Do **not** invent a generic “scores” CRUD. Create scores with:

```http
POST {BASE}/v1/leads/{lead_id}/scores
```

Body includes **`has_score_reasoning`** (required, non-empty), plus fields like `total`, `score_type`, optional BANT components, `actor_id`, `reasoning`, etc., as in your schema.

**Email campaign + steps + leads in one shot**

```http
POST {BASE}/v1/email-campaigns/with-artifacts
```

If `lead_ids` is non-empty, **`sourced_from_reasoning`** is required.

### Neighbourhood of one node (explore)

```http
GET {BASE}/v1/entities/{entity_id}/explore?depth=1|2&mode=compact|full
```

- **`depth`**: how many relationship hops from the start node (server clamps to a max, often 1–5).
- **`mode=compact`** (default): ids grouped by type + lightweight edges—best for **shape** and planning.
- **`mode=full`**: returns full properties for nodes in the subgraph—**heavy**; see §7. Usually you should stay on **compact** and then **`GET /v1/{entity}/{id}`** for each neighbor you care about instead of using **`full`**.

Responses include `truncated` when caps apply—read it and compensate (narrow query, fetch specific ids, or another hop).

---

## 4. Entities — what they mean (plain language)

| Entity | Plain meaning |
|--------|----------------|
| **Account** | A company or organization you sell to. |
| **ProductAccount** | The customer’s **account inside one product** (e.g. their id in your CRM app)—different from the company **Account**. Use **`external_id`** for that product-system id; link **`BELONGS_TO`** → company **Account** and **`FOR_PRODUCT`** → **Product**. |
| **Lead** | A prospect record (often early stage): form fill, list import, trial signup. Can link to a **ProductAccount** or company **Account** via **`SIGNED_UP_AS`** (product workspace / signup identity), or **`WORKS_AT`** for firmographic company association. |
| **Contact** | A person record—often tied to an account when they’re a known stakeholder. |
| **Deal** | A revenue opportunity (amount, stage, dates, etc.). |
| **Campaign** | A marketing initiative; leads can be **sourced from** campaigns. |
| **Email campaign** | A multi-step email program; may include many **emails** and enrolled **leads**. |
| **Email** | One message template/step (subject, body, sequence, send rules). |
| **Channel** | A grouping dimension for campaigns (e.g. paid search vs organic). |
| **Product** | Something sold; leads/deals can relate to products per your model. |
| **Content** | An asset (landing page, doc) campaigns may point to. |
| **Visitor** | A visitor record (often pre-account): external `visitor_id`, first touch channel/time. Links to **campaigns** via **TOUCHED** and optionally to a **ProductAccount** or company **Account** via **SIGNED_UP_AS** when you model signup attribution. |
| **SubscriptionEvent** | A point-in-time subscription milestone: `event_type` (e.g. signup, purchase, churn), `occurred_at`, plan tier/period, optional ARR. Typically hangs off a **ProductAccount** or company **Account** via **HAS_SUBSCRIPTION_EVENT** and may link to a **product** via **FOR_PRODUCT**. |

**Scores** are not a separate “free” entity type in your head: they attach to **leads** via the dedicated scores endpoint.

---

## 5. Graph structure — relationships (mental model)

Think in **nodes** and **labeled relationships**. Names below match what you’ll see in **`/v1/schema`** and **`explore`** edges.

**Common paths**

- **Lead —SOURCED_FROM→ Campaign** (or email campaign path, depending on data)
- **Lead —WORKS_AT→ Account** or **ProductAccount** (person at company; optional product-workspace reading)
- **Lead —SIGNED_UP_AS→ Account** or **ProductAccount** (signup / product workspace identity—same pattern as **Visitor**)
- **Contact —WORKS_AT→ Account**
- **Deal —BELONGS_TO→ Account**
- **ProductAccount —BELONGS_TO→ Account** (workspace belongs to the company)
- **ProductAccount —FOR_PRODUCT→ Product** (which product line this workspace is)
- **Deal —HAS_CONTACT→ Contact**
- **Lead —CONVERTED_TO→ Contact** (lifecycle)
- **Lead ←HAS_SCORE— Score** (score attaches to lead)
- **Campaign —INFLUENCED→ Deal** (when modeled)
- **Channel —HAS_CAMPAIGN→ Campaign**
- **Campaign —HAS_CONTENT→ Content**
- **EmailCampaign —HAS_EMAIL→ Email**
- **Visitor —TOUCHED→ Campaign** (touch / ad context; edge properties may hold ad group, landing page, timestamps)
- **Visitor —SIGNED_UP_AS→ Account** or **ProductAccount** (signed-up customer record)
- **Account** or **ProductAccount —HAS_SUBSCRIPTION_EVENT→ SubscriptionEvent**
- **SubscriptionEvent —FOR_PRODUCT→ Product** (same type as **Deal —FOR_PRODUCT→ Product** and **ProductAccount —FOR_PRODUCT→ Product**)

**Audit-style links** (who changed what) may appear as actor-linked metadata depending on mode and server; **`actor_id` on your writes** is what you control.

When in doubt, **`GET /v1/schema`** and **`explore?mode=compact`** are the source of truth for *this* deployment.

---

## 6. Implementation detail

You don’t need to know **how** the service persists the graph. Treat it as **a governed HTTP API** over a **GTM graph**.

---

## 7. Best practices — explore vs entity APIs, depth, compact vs full

### When to use **`GET /v1/{entities}/{id}`**

- You **already have the id** (from search, from a previous response, or the user pasted it).
- You need **full fields** for **one** record.
- You’re about to **update** or **delete** that record.

### When to use **`GET /v1/{entities}?...`**

- You want a **flat list** filtered by a field (e.g. all deals in stage `proposal`, accounts with `domain=acme.com`).
- You do **not** need relationship context yet.

### When to use **`/v1/search`**

- The human gave a **name, email fragment, or keyword** and you don’t know the **type** or **id**.
- You want **quick candidates**; then **GET by id** or **explore** from the winner.

### When to use **`explore` with `depth=1`**

- You have one id and need **immediate neighbours only** (e.g. “this deal’s account” or “this lead’s campaigns” if one hop).
- Use **`mode=compact` first** to see **types + topology** cheaply.

### When to use **`explore` with `depth=2`**

- The question needs **two hops** in one round trip (e.g. lead → campaign → channel, or contact → deal → account) **and** you want the API to walk it for you.
- If you would otherwise chain **many** GETs just to “walk the graph,” prefer **`depth=2`** (within server limits).

### When to use **`mode=compact` vs `mode=full`**

| Mode | Use it when |
|------|-------------|
| **compact** | Mapping **what’s connected**, collecting **ids** and **edge types**, minimizing payload. **Default choice.** |
| **`full`** | Rarely. Only when you truly need **full properties for many related nodes in one response** and pulling each record separately is unreasonable. |

**Prefer targeted entity reads over `mode=full`**

After **`explore?...&mode=compact`**, you already have **neighbor ids** grouped by type (e.g. `Campaign`, `Account`). In most cases you should **not** switch to `mode=full`. Instead, **fetch only what you need** with the normal CRUD route:

```http
GET {BASE}/v1/campaigns/{id}
GET {BASE}/v1/accounts/{id}
```

That keeps payloads small, respects **field-level permissions** more predictably, and avoids dumping unrelated fields for every node in the subgraph. Use **`mode=full`** only when the human explicitly wants a **single bulk snapshot** or when the number of follow-up GETs would be excessive **and** compact truly lacks the ids you need (it shouldn’t, if explore succeeded).

**Practical recipe**

1. **Search** or **list** to get a candidate **id**.  
2. **`explore?depth=1&mode=compact`** to see what’s attached and **collect related ids**.  
3. If the answer needs **one more hop**, **`depth=2`**—still **`mode=compact`**.  
4. For **fields** on specific neighbors, call **`GET /v1/{entity}/{id}`** per entity you care about (parallelize in code if appropriate).  
5. Reserve **`mode=full`** for exceptional **whole-neighborhood** property dumps.  
6. If **`truncated`** is non-empty, narrow the question, reduce depth, or **GET** the critical ids explicitly.

### When multiple separate queries are still OK

- You need **two unrelated** subgraphs (two different leads)—run **two** explores or GETs.
- **Permissions** might hide part of the graph; smaller targeted calls make gaps easier to explain to the human.

---

## 8. Question → answer patterns (examples)

Use these as **playbooks**. Replace `{BASE}` and ids with real values.

### One hop — use **GET** or **explore depth=1**

**Q:** “What’s this deal’s amount and stage?”  
**A:** `GET /v1/deals/{deal_id}` — single resource.

**Q:** “Which account is this deal for?”  
**A:** `GET /v1/deals/{deal_id}` if `account_id` is denormalized on the deal **or** `GET .../explore?depth=1&mode=compact` and look for **BELONGS_TO** to an **Account**.

**Q:** “What campaigns is this lead tied to?”  
**A:** `explore` from `lead_id`, `depth=1`, `mode=compact`; find **SOURCED_FROM** (or equivalent) edges to **Campaign** / **EmailCampaign**.

### Two hops — prefer **`explore depth=2`** (or two steps if clearer)

**Q:** “For this **contact**, which **campaigns** influenced their **account’s** deals?”  
**A:** Often: `explore` from **contact** `depth=2` `mode=compact` to see **Contact → Deal / Account → Campaign** patterns, then **GET** specific campaign ids for names/budgets. If the graph paths aren’t obvious, split: contact → deals → campaigns.

**Q:** “Lead → which campaign → which channel?”  
**A:** `explore?depth=2&mode=compact` from **lead_id**; trace **SOURCED_FROM** then **HAS_CAMPAIGN** / channel edges per schema.

**Q:** “Show me everything one step out, then I’ll ask for step two.”  
**A:** `depth=1` first (compact); then either **`depth=2`** from the same root **or** second **explore** from a **new** id you discovered.

### Finding ids first

**Q:** “Find anything mentioning ‘Acme’.”  
**A:** `GET /v1/search?q=Acme&limit=25` → pick types/ids → **GET** or **explore**.

**Q:** “Deals stuck in negotiation over $100k.”  
**A:** If the API supports filtering those fields: `GET /v1/deals?stage=negotiation` then filter **amount** client-side **or** use search if text-only.

### Writes — always **`actor_id`** + link **reasoning**

**Q:** “Attach this lead to campaign X.”  
**A:** `POST /v1/leads/{lead_id}/link-campaign` with `campaign_id`, **`reasoning`**, and ensure **`actor_id`** on any related create/update the human asked for.

**Q:** “Create a lead from this form payload.”  
**A:** `POST /v1/leads` with fields + **`actor_id`** + optional **`reasoning`** on the create.

### Multi-step reasoning (you, the agent)

**Q:** “Summarize this lead’s journey.”  
**A:**  
1) `GET /v1/leads/{id}` for core fields.  
2) `explore?depth=2&mode=compact` for graph shape.  
3) `GET` a few **campaign** / **account** ids for names.  
4) **Narrate in plain English** (see §11)—don’t dump raw JSON unless asked.

**Q:** “We need two hops but explore truncated.”  
**A:** Tell the human **truncation** happened; rerun with **smaller depth**, **targeted search**, or **explicit GET** for the missing ids listed in `truncated`.

---

## 9. Error handling

| HTTP | What it usually means | What you should do |
|------|------------------------|-------------------|
| **400** | Bad body (missing **`actor_id`** where required, empty **`reasoning`** on links, invalid field) | Fix the payload; quote `detail` from the response to the human if useful. |
| **401** | Missing/invalid API key | Ask the human to verify the key. |
| **403** | **Not allowed** for this key (read/write/field masked) | Explain **you aren’t permitted**, not “data doesn’t exist.” Suggest the human **request a key or permission change** from whoever manages GtmDB access in their organization. |
| **404** | Unknown id or route | Id may be wrong, deleted, or outside tenant. Confirm id and tenant. |

**Network / 5xx** — retry with backoff once or twice; if it persists, stop and tell the human the service is unhealthy.

---

## 10. Reasoning on writes — treat it as mandatory discipline

On **creates** and **updates**, include **`reasoning`** whenever the API allows it and whenever it helps a human understand **why** the change happened.

On **relationship / link** endpoints, **`reasoning` is required**—empty explanations should fail.

On **scores**, **`has_score_reasoning`** (and related fields) carry the same obligation: explain **why** that score exists.

This is how **autonomous** actions stay **reviewable**.

---

## 11. Talk to humans in human language

You will work with **JSON**, **ids**, and **relationship types** internally. **Do not** paste huge raw responses unless the user asks.

**Default**

- Short **summary** (bullets or a tight paragraph).
- Mention **key entities by name** (company, person, campaign) once you’ve fetched those fields.
- If something was **blocked by permissions**, say so plainly.
- If you **guessed** an id from search, say it was the **best match** and what you’d verify next.

---

## Quick reference — minimal HTTP examples

```http
GET {BASE}/v1/schema
Authorization: Bearer $GTMDB_API_KEY
```

```http
GET {BASE}/v1/search?q=acme&limit=10
Authorization: Bearer $GTMDB_API_KEY
```

```http
GET {BASE}/v1/entities/{id}/explore?depth=2&mode=compact
Authorization: Bearer $GTMDB_API_KEY
```

```http
POST {BASE}/v1/leads
Authorization: Bearer $GTMDB_API_KEY
Content-Type: application/json

{
  "actor_id": "my-agent",
  "first_name": "Jane",
  "last_name": "Doe",
  "email": "jane@example.com",
  "company_name": "Example Co",
  "reasoning": "Inbound trial signup from pricing page"
}
```

```http
POST {BASE}/v1/leads/{lead_id}/link-campaign
Authorization: Bearer $GTMDB_API_KEY
Content-Type: application/json

{
  "campaign_id": "...",
  "reasoning": "UTM on form matched Q1 outbound campaign"
}
```


