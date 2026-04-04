"""Agent tools: execute_sql, execute_cypher, get_schema.

Permission checks run inside :class:`~gtmdb.graph.adapter.GraphAdapter` and
:class:`~gtmdb.olap.store.OlapStore` when a scope is provided.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from langchain.tools import tool

log = logging.getLogger(__name__)


class _TenantOnlyScope:
    """Minimal scope for analyst runs without a token (tenant isolation only)."""

    __slots__ = ("tenant_id",)

    def __init__(self, tenant_id: str) -> None:
        self.tenant_id = tenant_id

    @property
    def policies(self) -> list:
        return []


# Injected by AnalystRunner before the agent starts
_graph_adapter = None
_olap_store = None
_tenant_id: str = ""
_schema_text: str = ""
_scope: object | None = None


def configure(
    *,
    graph_adapter,
    olap_store,
    tenant_id: str,
    schema_text: str,
    scope=None,
) -> None:
    """Wire live adapters into tools. Called once by AnalystRunner."""
    global _graph_adapter, _olap_store, _tenant_id, _schema_text, _scope
    _graph_adapter = graph_adapter
    _olap_store = olap_store
    _tenant_id = tenant_id
    _schema_text = schema_text
    _scope = scope


def _effective_scope() -> object:
    if _scope is not None:
        return _scope
    return _TenantOnlyScope(_tenant_id)


@tool
def think(plan: str) -> str:
    """Record your reasoning and query plan.

    Call this tool EXACTLY ONCE at the start — before your first query — to
    write out your complete plan:
    - What the user is really asking
    - Which data sources you will use and why
    - The ordered list of queries you intend to run
    - What IDs or values will flow from one query to the next

    Call it a SECOND time when ANY of these specific conditions occur:
    - A query returns 0 rows
    - A query returns fewer results than expected
    - Results reveal the next step in your plan won't work as written
    - You discover an ID or value that requires changing your next query

    Do NOT call think before every query — only at the start, and when one
    of the above conditions is triggered by a result.

    This tool has no side effects — it just records your reasoning.
    """
    log.info("[analyst] think: %s", plan[:200])
    return f"Plan recorded: {plan}"


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
        rows = await _olap_store.query(query, scope=_effective_scope())
        total = len(rows)
        out: dict[str, Any] = {"rows": rows[:200], "row_count": total}
        if total > 200:
            out["truncated"] = True
            out["note"] = f"Showing 200 of {total} rows"
        return json.dumps(out, default=str)
    except PermissionError as e:
        log.warning("[analyst][guard] SQL blocked: %s", e)
        return json.dumps({"error": str(e)})
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

    try:
        rows = await _graph_adapter.execute(_effective_scope(), query)
        truncated = len(rows) > 200
        out: dict[str, Any] = {"rows": rows[:200], "row_count": len(rows)}
        if truncated:
            out["truncated"] = True
            out["note"] = f"Showing 200 of {len(rows)} rows"
        return json.dumps(out, default=str)
    except PermissionError as e:
        log.warning("[analyst][guard] Cypher blocked: %s", e)
        return json.dumps({"error": str(e)})
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
