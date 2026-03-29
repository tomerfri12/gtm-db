"""Connect a client to a **running** GtmDB graph service.

Both admins and agents call :func:`connect_gtmdb` with an ``api_key``.

* If the key matches ``GtmdbSettings.admin_key`` the caller gets a
  ``full_access`` :class:`~gtmdb.scope.Scope` with ``owner_type="admin"``
  (no Postgres round-trip).
* Otherwise the key is resolved from the Postgres key store and the scope
  is determined by the policies stored with that key.
"""

from __future__ import annotations

from gtmdb.client import GtmDB
from gtmdb.config import GtmdbSettings
from gtmdb.presets import create_token_from_presets
from gtmdb.scope import Scope


async def connect_gtmdb(
    settings: GtmdbSettings | None = None,
    *,
    api_key: str,
) -> tuple[GtmDB, Scope]:
    """Connect to GtmDB and resolve *api_key* into a :class:`~gtmdb.scope.Scope`.

    Raises :class:`ValueError` if *api_key* is empty, if no matching admin key
    or Postgres record is found, or if the key is expired / revoked.
    """
    raw = (api_key or "").strip()
    if not raw:
        raise ValueError("api_key is required (non-empty string)")

    cfg = settings or GtmdbSettings()
    db = GtmDB(cfg)
    try:
        await db.connect()
    except Exception:
        try:
            await db.close()
        except Exception:
            pass
        raise

    # Admin key — checked locally, no Postgres needed.
    if cfg.admin_key and raw == cfg.admin_key:
        token = create_token_from_presets(
            tenant_id=cfg.default_tenant_id,
            owner_id="admin",
            owner_type="admin",
            preset_names=["full_access"],
        )
        scope = Scope(token)
        db.api_keys.bind_scope(scope)
        return db, scope

    # Agent key — resolve from Postgres key store.
    try:
        scope = await db.api_keys.resolve(raw)
    except Exception:
        try:
            await db.close()
        except Exception:
            pass
        raise

    db.api_keys.bind_scope(scope)
    return db, scope
