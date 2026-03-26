"""GtmDB settings.

Default Neo4j connection targets a shared **dev** Neo4j Aura instance so
``connect_gtmdb()`` works out of the box after ``pip install``. Override any
value with ``GTMDB_NEO4J_URI``, ``GTMDB_NEO4J_USER``, ``GTMDB_NEO4J_PASSWORD``,
or ``GTMDB_DEFAULT_TENANT_ID`` (or pass kwargs to ``GtmdbSettings(...)``).
"""

from pydantic_settings import BaseSettings

# Shared dev Aura — committed for convenience. Do not use for production data.
_DEV_NEO4J_URI = "neo4j+s://a74a5a7e.databases.neo4j.io"
_DEV_NEO4J_USER = "neo4j"
_DEV_NEO4J_PASSWORD = "YKm2DImeObl87ZwHYiSPiWQGs4MHbMVkDng-X2TIz3o"
_DEV_DEFAULT_TENANT_ID = "00000000-0000-4000-8000-000000000001"


class GtmdbSettings(BaseSettings):
    neo4j_uri: str = _DEV_NEO4J_URI
    neo4j_user: str = _DEV_NEO4J_USER
    neo4j_password: str = _DEV_NEO4J_PASSWORD
    # Used by ``python -m gtmdb init --seed`` (align with host app tenant if needed).
    default_tenant_id: str = _DEV_DEFAULT_TENANT_ID

    model_config = {"env_prefix": "GTMDB_", "env_file": ".env", "extra": "ignore"}
