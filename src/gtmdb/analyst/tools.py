"""Agent tools: execute_sql, execute_cypher, get_schema.

No permission guards yet — those come in a later phase.
Each tool is a plain async function; the LangGraph agent calls them via
LangChain's @tool decorator.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from langchain.tools import tool

log = logging.getLogger(__name__)

# These are injected by AnalystRunner before the agent starts
_graph_adapter = None
_olap_store = None
_tenant_id: str = ""
_schema_text: str = ""


def configure(*, graph_adapter, olap_store, tenant_id: str, schema_text: str) -> None:
    """Wire live adapters into tools. Called once by AnalystRunner."""
    global _graph_adapter, _olap_store, _tenant_id, _schema_text
    _graph_adapter = graph_adapter
    _olap_store = olap_store
    _tenant_id = tenant_id
    _schema_text = schema_text



@tool
async def execute_sql(query: str) -> str:
    """Execute a SQL query against the ClickHouse events table.

    The query must be a SELECT statement only.
    tenant_id scoping must be included in the WHERE clause.
    Returns JSON-encoded list of result rows (max 200 rows shown).
    """
    if _olap_store is None:
        return json.dumps({"error": "OLAP store not connected"})

    query = query.strip().rstrip(";")
    log.info("[analyst] execute_sql: %s", query[:200])

    try:
        # OlapStore.query returns list[dict] directly
        rows = await _olap_store.query(query)
        total = len(rows)
        out: dict[str, Any] = {"rows": rows[:200], "row_count": total}
        if total > 200:
            out["truncated"] = True
            out["note"] = f"Showing 200 of {total} rows"
        return json.dumps(out, default=str)
    except Exception as e:
        log.warning("[analyst] execute_sql error: %s", e)
        return json.dumps({"error": str(e), "query": query})


@tool
async def execute_cypher(query: str) -> str:
    """Execute a read-only Cypher query against the Neo4j graph.

    The query must be a MATCH/RETURN statement only — no writes.
    Always use {tenant_id: $tenant_id} in node patterns.
    Returns JSON-encoded list of result records (max 200 rows shown).
    """
    if _graph_adapter is None:
        return json.dumps({"error": "Graph adapter not connected"})

    query = query.strip().rstrip(";")
    log.info("[analyst] execute_cypher: %s", query[:200])

    # Basic write guard (permissive — full guard comes in Phase 2)
    upper = query.upper().lstrip()
    for forbidden in ("CREATE ", "MERGE ", "DELETE ", "SET ", "REMOVE ", "DROP "):
        if forbidden in upper:
            return json.dumps({
                "error": f"Write operations are not allowed. Detected: {forbidden.strip()}",
                "query": query,
            })

    try:
        class _Scope:
            tenant_id = _tenant_id

        rows = await _graph_adapter.execute(_Scope(), query)
        truncated = len(rows) > 200
        out: dict[str, Any] = {"rows": rows[:200], "row_count": len(rows)}
        if truncated:
            out["truncated"] = True
            out["note"] = f"Showing 200 of {len(rows)} rows"
        return json.dumps(out, default=str)
    except Exception as e:
        log.warning("[analyst] execute_cypher error: %s", e)
        return json.dumps({"error": str(e), "query": query})


@tool
def get_schema() -> str:
    """Return the full schema description for both data sources.

    Call this first if you need to know available columns, labels, or
    relationship types before writing a query.
    """
    return _schema_text
