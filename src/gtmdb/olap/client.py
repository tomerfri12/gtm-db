"""ClickHouse async client wrapper.

Wraps ``clickhouse-connect`` to provide a small, typed API used by the rest
of gtmDB (sync hooks, materializer, query agent).  All public methods are
async so they compose naturally with FastAPI / LangGraph async runtimes.

Usage::

    from gtmdb.olap.client import ClickHouseClient

    ch = await ClickHouseClient.create(settings)
    await ch.bootstrap()                     # idempotent: CREATE TABLE IF NOT EXISTS
    await ch.insert_events([row_dict, ...])  # bulk insert
    rows = await ch.query("SELECT ...")      # returns list[dict]
    await ch.close()
"""

from __future__ import annotations

import json
import logging
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient

from gtmdb.config import GtmdbSettings

from .schema import EVENTS_COLUMNS, EVENTS_TABLE_DDL

log = logging.getLogger(__name__)


class ClickHouseClient:
    """Thin async façade over ``clickhouse-connect`` AsyncClient."""

    def __init__(self, inner: AsyncClient, database: str) -> None:
        self._client = inner
        self._database = database

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    async def create(cls, settings: GtmdbSettings) -> "ClickHouseClient":
        """Connect and return a ready client (does NOT bootstrap schema)."""
        inner: AsyncClient = await clickhouse_connect.get_async_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
        )
        return cls(inner, settings.clickhouse_database)

    # ------------------------------------------------------------------
    # Schema bootstrap
    # ------------------------------------------------------------------

    async def bootstrap(self) -> None:
        """Create the ``events`` table if it does not exist (idempotent)."""
        ddl = EVENTS_TABLE_DDL.format(database=self._database)
        await self._client.command(ddl)
        log.info("ClickHouse bootstrap complete (database=%s)", self._database)

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    async def insert_events(self, rows: list[dict[str, Any]]) -> int:
        """Insert a batch of event rows (dicts keyed by column name).

        Missing keys are filled with column defaults so callers only need to
        set the fields they care about.  Returns the number of rows inserted.
        """
        if not rows:
            return 0

        # Build column-oriented data (list of lists) aligned to EVENTS_COLUMNS
        data: list[list[Any]] = [[] for _ in EVENTS_COLUMNS]

        for row in rows:
            for i, col in enumerate(EVENTS_COLUMNS):
                val = row.get(col)
                if val is None:
                    val = _col_default(col)
                # Serialize dicts/lists to JSON string for the 'extra' column
                if isinstance(val, (dict, list)):
                    val = json.dumps(val, default=str)
                data[i].append(val)

        await self._client.insert(
            "events",
            data,
            column_names=EVENTS_COLUMNS,
            column_oriented=True,
            database=self._database,
        )
        log.debug("Inserted %d events into ClickHouse", len(rows))
        return len(rows)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    async def query(
        self,
        sql: str,
        parameters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a SELECT and return results as a list of dicts."""
        result = await self._client.query(sql, parameters=parameters or {})
        columns = result.column_names
        return [dict(zip(columns, row)) for row in result.result_rows]

    async def query_one(
        self,
        sql: str,
        parameters: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Return the first row or ``None``."""
        rows = await self.query(sql, parameters)
        return rows[0] if rows else None

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    async def ping(self) -> bool:
        """Return True if ClickHouse is reachable."""
        try:
            await self._client.ping()
            return True
        except Exception:
            return False

    async def close(self) -> None:
        """Close the underlying connection."""
        await self._client.close()

    # Make the client usable as an async context manager
    async def __aenter__(self) -> "ClickHouseClient":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_STRING_DEFAULT = ""
_FLOAT_DEFAULT = 0.0
_INT_DEFAULT = 0

def _col_default(col: str) -> Any:
    """Return the Python-side default for a column that was not provided."""
    # Numeric columns
    _float_cols = {
        "lead_score", "account_arr", "campaign_budget",
        "deal_amount", "deal_probability", "sub_arr",
    }
    _int_cols = {
        "account_employees", "sub_days_from_signup",
        "lead_is_signup", "product_account_is_paying",
    }
    if col == "extra":
        return "{}"
    if col in _float_cols:
        return 0.0
    if col in _int_cols:
        return 0
    return ""
