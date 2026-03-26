# GtmDB

Managed graph service for CRM data. **The graph backend is owned by this package**; host apps use the `gtmdb` Python client (`connect_gtmdb`, entity APIs) and never pass in SQL engines or app databases.

Access tokens for `gtmdb.scope.Scope` are plain in-memory objects; if a host app persists them, that lives in the host’s own store (e.g. CRM2’s Postgres).

## Local graph store

From this directory:

```bash
docker compose up -d
```

Default ports: **7474** (HTTP/Browser), **7687** (Bolt). Auth matches `NEO4J_AUTH` in `docker-compose.yml` (`neo4j` / `crmdb_password`).

## Bootstrap schema (operators / new environments)

Run once against a **new** graph instance before clients rely on indexes/constraints. With a `.env` in the **current working directory** (repo root) containing `GTMDB_*` from `.env.example`:

```bash
python -m gtmdb init
python -m gtmdb init --seed   # optional demo nodes
```

Or, after `pip install -e ./crmdb`:

```bash
gtmdb init --seed
```

## Connect later (Neo4j Browser / console)

- **Browser:** open `http://localhost:7474`, sign in with `neo4j` and your password (`crmdb_password` with the bundled compose).
- **cypher-shell** (from the Neo4j container):

  ```bash
  docker compose exec neo4j cypher-shell -u neo4j -p crmdb_password
  ```

  Quick check after `init --seed`:

  ```cypher
  MATCH (n) RETURN labels(n)[0] AS label, count(*) AS n LIMIT 25;
  ```

## Host application (CRM2)

CRM2 calls `connect_gtmdb()` only (no Postgres engine, no GtmDB “init” from the app). Set `GTMDB_*` so the client reaches an already-running graph endpoint.
