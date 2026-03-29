# GtmDB

Graph-native CRM / GTM data layer on **Neo4j**, with **Postgres-backed API keys**, policy scopes, and a **FastAPI** REST server.

## Quick start

1. Copy `.env.example` to `.env` and set **Neo4j** credentials, optional **`GTMDB_KEY_STORE_URL`**, and **`GTMDB_ADMIN_KEY`**.
2. Install: `pip install -e ".[dev]"` (use a virtualenv).
3. Bootstrap schema: `python -m gtmdb init` (admin key required).
4. Run API: `python -m gtmdb serve` — defaults to port **8100**; on Railway use **`PORT`**.

- **OpenAPI / Swagger:** `http://localhost:8100/docs`
- **Health:** `GET /health`
- **Client HTML reference:** [docs/api-reference.html](docs/api-reference.html)

## Auth

- `Authorization: Bearer <key>` on all `/v1/*` routes.
- Admin key = `GTMDB_ADMIN_KEY` (full access + `/v1/admin/keys`).
- Agent keys = stored in Postgres when `GTMDB_KEY_STORE_URL` is set.

## Deploy (Railway)

1. Create a **Railway** project, connect this GitHub repo.
2. Add **Postgres**; set `GTMDB_KEY_STORE_URL` to the `postgresql+asyncpg://…` URL Railway provides.
3. Set `GTMDB_NEO4J_*`, `GTMDB_ADMIN_KEY`, and point Neo4j to your **Aura** (or other) instance.
4. Railway injects **`PORT`** — the CLI `serve` command reads it automatically.

## Python library

```python
from gtmdb import connect_gtmdb

db, scope = await connect_gtmdb(api_key="…")
await db.leads.create(scope, actor_id="agent-1", company_name="Acme", status="new")
await db.close()
```

## License

Proprietary / as per repository owner.
