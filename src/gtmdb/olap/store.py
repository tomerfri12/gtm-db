"""OlapStore — technology-agnostic OLAP store interface.

``GtmDB`` and all higher-level code interact with this class, never with
``ClickHouseClient`` directly.  The implementation detail (ClickHouse) lives
inside this module; swapping it for another columnar store only requires
changes here.

This mirrors the pattern of ``GraphAdapter`` for Neo4j:

    GraphAdapter  ←→  OlapStore
    Neo4j              ClickHouse (current implementation)
"""

from __future__ import annotations

from typing import Any

from gtmdb.config import GtmdbSettings

from .client import ClickHouseClient


class OlapStore:
    """Async OLAP store façade used throughout gtmDB.

    All public methods are async and mirror those of ``ClickHouseClient``.
    Callers never import or reference ``ClickHouseClient`` directly.
    """

    def __init__(self, _impl: ClickHouseClient) -> None:
        self._impl = _impl

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    async def create(cls, settings: GtmdbSettings) -> "OlapStore":
        """Connect to the configured OLAP backend and return a ready store."""
        impl = await ClickHouseClient.create(settings)
        return cls(impl)

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    async def bootstrap(self) -> None:
        """Create the ``events`` table if it does not exist (idempotent)."""
        await self._impl.bootstrap()

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    async def insert_events(self, rows: list[dict[str, Any]]) -> int:
        """Insert a batch of event rows. Returns the number of rows inserted."""
        return await self._impl.insert_events(rows)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    async def query(
        self,
        sql: str,
        parameters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a SELECT and return results as a list of dicts."""
        return await self._impl.query(sql, parameters)

    async def query_one(
        self,
        sql: str,
        parameters: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Return the first row or ``None``."""
        return await self._impl.query_one(sql, parameters)

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    async def ping(self) -> bool:
        """Return True if the OLAP backend is reachable."""
        return await self._impl.ping()

    async def close(self) -> None:
        """Close the underlying connection."""
        await self._impl.close()

    async def __aenter__(self) -> "OlapStore":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()
