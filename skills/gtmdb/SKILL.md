---
name: gtmdb
description: GtmDB graph CRM/GTM — Neo4j-backed leads, accounts, deals, campaigns; async Python; every write needs actor_id; tiered edge reasoning; scores only via leads.add_score.
metadata: {"openclaw": {"requires": {"bins": ["python3"]}, "homepage": "https://github.com/tomerfri12/gtm-db"}}
---

# GtmDB

Use this skill when the user needs to **read or write CRM/GTM graph data** (accounts, leads, contacts, deals, campaigns, email programs, relationships) backed by **Neo4j** through the **`gtmdb`** Python package.

## Install

```bash
python3 -m pip install -U pip
python3 -m pip install "git+https://github.com/tomerfri12/gtm-db.git"
```

Monorepo consumers: append `#subdirectory=crmdb` to the Git URL.

Verify: `python3 -c "import gtmdb; print(gtmdb.__file__)"`

## Neo4j

GtmDB needs a reachable Neo4j. Defaults come from `GtmdbSettings` (override with env):

- `GTMDB_NEO4J_URI`, `GTMDB_NEO4J_USER`, `GTMDB_NEO4J_PASSWORD`
- `GTMDB_DEFAULT_TENANT_ID` (UUID string for tenant scoping)

Local dev: use `docker-compose` in the `gtm-db` / `crmdb` repo if you self-host Neo4j.

## Connect and scope

Always: **`connect_gtmdb()`** → build **`Scope`** from **`create_token_from_presets`** → pass **`scope`** into every call.

```python
import asyncio
import uuid
from gtmdb import connect_gtmdb, GtmdbSettings
from gtmdb.presets import create_token_from_presets
from gtmdb.scope import Scope

async def main():
    db = await connect_gtmdb()
    tid = uuid.UUID(GtmdbSettings().default_tenant_id)
    token = create_token_from_presets(tid, "my-agent", "agent", ["full_access"])
    scope = Scope(token)
    # ... use db with scope ...
    await db.close()

asyncio.run(main())
```

Use **`["read_all"]`** instead of **`["full_access"]`** for read-only tokens.

## Actor (`actor_id`)

Every **`EntityAPI.create`** and **`update`** requires keyword **`actor_id`** (non-empty string). The client **MERGE**s an **`Actor`** node and sets **`created_by_actor_id`** / **`updated_by_actor_id`**. It creates **`(Actor)-[:CREATED_BY|:UPDATED_BY]->(entity)`** edges. Optional **`reasoning`** on create/update is stored on those audit relationships when non-empty.

**`GtmDB.create_node(..., *, actor_id, reasoning=None)`** is the low-level path; same rules except label **`Actor`** skips `CREATED_BY`.

## Entity accessors on `db`

| API | Notes |
|-----|--------|
| `db.leads` | CRUD + `assign_to_account`, `link_campaign`, `add_score`, `scores_for`, `for_account` |
| `db.accounts`, `db.contacts`, `db.deals`, `db.campaigns` | CRUD + typed link helpers on deals/contacts/leads/campaigns |
| `db.emails`, `db.email_campaigns` | CRUD; batch: `email_campaigns.create_with_artifacts` |
| `db.actors` | `create` / MERGE batch (`ActorSpec`) |
| `db.relationships` | Generic edge CRUD |
| `db.scores` | **Read** (`list_for_lead`); **do not** call `ScoresAPI.create` (raises `TypeError`) |

Typical calls:

```python
lead = await db.leads.create(scope, actor_id="my-agent", company_name="Acme", first_name="Jane")
await db.leads.update(scope, lead.id, actor_id="my-agent", status="qualified")
acc = await db.accounts.get(scope, account_id)
```

## Edge `reasoning` (tiered)

**Non-empty `reasoning` required** for:

- `db.relationships.create(..., reasoning="...")`
- `db.leads.assign_to_account`, `db.leads.link_campaign`
- `db.contacts.assign_to_account`
- `db.deals.assign_to_account`, `add_contact`, `add_campaign`
- `db.campaigns.link_deal`
- `db.leads.add_score(..., has_score_reasoning="...")` (separate from Score node domain field `reasoning`)
- `db.email_campaigns.create_with_artifacts`: if **`lead_ids`** is non-empty, **`sourced_from_reasoning`** is required

**Optional** for: `CREATED_BY` / `UPDATED_BY` on entity create/update; `HAS_EMAIL` (`has_email_reasoning`); raw `EdgeData` / `db.create_edge`.

Reads: `relationships.list` → `Relationship.properties`; `get_neighbors` / `entity_360` → `edge_properties`.

## Scores (lead-only)

Create scores only via:

```python
await db.leads.add_score(
    scope,
    lead_id,
    actor_id="my-agent",
    has_score_reasoning="Why this score applies to the lead",
    reasoning=None,  # optional: CREATED_BY edge on Score node
    total=72,
    score_type="bant",
    # optional domain fields: budget, authority, need, timeline, reasoning (qualification notes), status, scored_by
)
```

List: `await db.leads.scores_for(scope, lead_id)` or `db.scores.list_for_lead(scope, lead_id)`.

## Node `name` (display)

- **Lead / Contact**: composed first+last; fallback company_name, email, then default label.
- **Score**: `{score_type}:{total}` (set by API).
- **Email**: subject, else `Email step {sequence_number}`.
- **Account / Deal / Campaign / EmailCampaign**: caller-supplied **`name`**.

## Traversals and search

`db.entity_360`, `get_neighbors`, `timeline`, `path_finding`, `search`, `pipeline`, `campaign_attribution`, `execute_cypher` — all take **`scope`**; tenant isolation is enforced.

## Types

Import **`EdgeData`**, **`NodeData`** from **`gtmdb.types`** for low-level **`create_edge`** / **`create_node`**.

## Canonical API docs

In the package repo: **`docs/api-reference.html`** (browser) and source under **`src/gtmdb/`**. When the API changes, refresh this **`SKILL.md`** and republish to ClawHub if you distribute the skill there.

## ClawHub

After edits: `clawhub publish ./skills/gtmdb --slug gtmdb --name "GtmDB" --version <semver> --tags latest` (from repo root). Others install with `openclaw skills install gtmdb` or `clawhub install gtmdb` per [ClawHub](https://docs.openclaw.ai/tools/clawhub).
