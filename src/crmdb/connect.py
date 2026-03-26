"""Connect a client to a **running** CRMDB graph service.

Operators provision and scale the graph store; consumers only need a reachable
endpoint in :class:`~crmdb.config.CrmdbSettings` and call :func:`connect_crmdb`.
"""

from __future__ import annotations

from crmdb.client import CrmDB
from crmdb.config import CrmdbSettings


async def connect_crmdb(settings: CrmdbSettings | None = None) -> CrmDB:
    """Return a connected ``CrmDB`` (verify connectivity; ensure schema on the service)."""
    db = CrmDB(settings)
    try:
        await db.connect()
    except Exception:
        try:
            await db.close()
        except Exception:
            pass
        raise
    return db
