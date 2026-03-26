"""Connect a client to a **running** GtmDB graph service.

Operators provision and scale the graph store; consumers only need a reachable
endpoint in :class:`~gtmdb.config.GtmdbSettings` and call :func:`connect_gtmdb`.
"""

from __future__ import annotations

from gtmdb.client import GtmDB
from gtmdb.config import GtmdbSettings


async def connect_gtmdb(settings: GtmdbSettings | None = None) -> GtmDB:
    """Return a connected ``GtmDB`` (verify connectivity; ensure schema on the service)."""
    db = GtmDB(settings)
    try:
        await db.connect()
    except Exception:
        try:
            await db.close()
        except Exception:
            pass
        raise
    return db
