"""Layer 2 query guard — hard enforcement before Neo4j / OLAP execution.

Parses SQL with sqlglot and Cypher with regex to detect references to denied
columns or graph labels. Used by :class:`~gtmdb.graph.adapter.GraphAdapter`
and :class:`~gtmdb.olap.store.OlapStore` so all callers get consistent checks.

Usage::

    from gtmdb.guard import QueryGuard

    guard = QueryGuard(scope)
    error = guard.check_sql(query)    # None = ok, str = rejection message
    error = guard.check_cypher(query)
"""

from __future__ import annotations

import re

import sqlglot
import sqlglot.expressions as exp

from gtmdb.resources import COLUMN_TO_RESOURCE, RESOURCE_BY_NAME, RESOURCES

# Cypher write keywords that should never appear
_CYPHER_WRITE_KEYWORDS = re.compile(
    r"\b(CREATE|MERGE|SET|DELETE|DETACH\s+DELETE|REMOVE|DROP|CALL\s+db\.)\b",
    re.IGNORECASE,
)


class QueryGuard:
    """Stateless guard bound to a specific scope.

    Parameters
    ----------
    scope:
        A :class:`~gtmdb.scope.Scope` instance (or any object with a
        ``policies`` attribute containing a list of policy dicts).
    """

    def __init__(self, scope: object) -> None:
        self._denied_resources: set[str] = set()
        self._denied_columns: set[str] = set()   # lower-cased ClickHouse col names
        self._denied_labels: set[str] = set()    # Neo4j label names (original case)

        policies: list[dict] = getattr(scope, "policies", [])
        for policy in policies:
            if policy.get("effect") != "deny":
                continue
            if "read" not in policy.get("actions", []):
                continue
            for r in policy.get("resources", []):
                if "." in r:
                    node, field = r.lower().split(".", 1)
                    self._denied_columns.add(f"{node}_{field}")
                else:
                    self._denied_resources.add(r)
                    self._denied_labels.add(r)
                    schema = RESOURCE_BY_NAME.get(r)
                    if schema:
                        for col in schema.columns:
                            self._denied_columns.add(col.lower())

    # ------------------------------------------------------------------
    # SQL guard
    # ------------------------------------------------------------------

    def check_sql(self, query: str) -> str | None:
        """Return an error string if the SQL touches denied columns, else None."""
        if not self._denied_columns:
            return None

        try:
            parsed = sqlglot.parse_one(query, dialect="clickhouse")
        except Exception:
            return self._text_scan_sql(query)

        violations: set[str] = set()
        for node in parsed.walk():
            if isinstance(node, exp.Column):
                col_name = node.name.lower()
                if col_name in self._denied_columns:
                    rs = COLUMN_TO_RESOURCE.get(col_name)
                    violations.add(rs.name if rs else col_name)
            elif isinstance(node, exp.Star):
                pass

        if violations:
            return self._rejection_message(sorted(violations), "SQL")
        return None

    def _text_scan_sql(self, query: str) -> str | None:
        """Fallback: scan raw SQL text for denied column names."""
        query_lower = query.lower()
        violations: set[str] = set()
        for col in self._denied_columns:
            if re.search(rf"\b{re.escape(col)}\b", query_lower):
                rs = COLUMN_TO_RESOURCE.get(col)
                violations.add(rs.name if rs else col)
        if violations:
            return self._rejection_message(sorted(violations), "SQL")
        return None

    # ------------------------------------------------------------------
    # Cypher guard
    # ------------------------------------------------------------------

    def check_cypher(self, query: str) -> str | None:
        """Return an error if the Cypher touches denied labels or is a write."""
        if _CYPHER_WRITE_KEYWORDS.search(query):
            match = _CYPHER_WRITE_KEYWORDS.search(query)
            keyword = match.group(0) if match else "write operation"
            return f"Write operations are not allowed in Cypher. Detected: {keyword}"

        if not self._denied_labels:
            return None

        violations: set[str] = set()
        for label_match in re.finditer(r"\([\w\s]*:(\w+)", query):
            label = label_match.group(1)
            if label in self._denied_labels:
                violations.add(label)

        if violations:
            return self._rejection_message(sorted(violations), "Cypher")
        return None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _rejection_message(self, resources: list[str], query_type: str) -> str:
        allowed = sorted(
            rs.name for rs in RESOURCES if rs.name not in self._denied_resources
        )
        return (
            f"PERMISSION DENIED: Your {query_type} query references "
            f"{', '.join(resources)}, which you do not have access to. "
            f"Your allowed resources are: {', '.join(allowed)}. "
            f"Please tell the user you cannot access this data."
        )
