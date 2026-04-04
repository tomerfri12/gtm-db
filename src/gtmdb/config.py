"""GtmDB settings.

All connection secrets must be supplied via environment variables or a ``.env``
file (see ``.env.example``). There are **no** hardcoded credentials in the package.
"""

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings


class GtmdbSettings(BaseSettings):
    neo4j_uri: str = Field(default="", description="Neo4j bolt URI")
    neo4j_user: str = Field(default="", description="Neo4j user")
    neo4j_password: str = Field(default="", description="Neo4j password")
    # Aura ``neo4j+s://`` routing can time out from some PaaS networks; set true or use ``bolt+s://`` URI.
    neo4j_force_direct_bolt: bool = Field(default=False)
    neo4j_connection_timeout: float | None = Field(
        default=None,
        description="Driver TCP connect timeout (seconds); omit for Neo4j default",
    )
    neo4j_connection_acquisition_timeout: float | None = Field(
        default=None,
        description="Max seconds to wait for a connection from the pool",
    )
    # Used by ``python -m gtmdb init --seed`` (align with host app tenant if needed).
    default_tenant_id: str = Field(
        default="00000000-0000-4000-8000-000000000001",
    )
    # Postgres DSN for the API key store (e.g. "postgresql+asyncpg://user:pass@host/db").
    # Required for agent key resolution. When None/empty, only the admin key works.
    key_store_url: str | None = None
    # Shared admin key (``GTMDB_ADMIN_KEY``). Checked locally in ``connect_gtmdb``.
    admin_key: str | None = None

    # ClickHouse (OLAP analytics store)
    clickhouse_host: str = Field(default="localhost", description="ClickHouse server host")
    clickhouse_port: int = Field(default=8123, description="ClickHouse HTTP port (8443 for Cloud/TLS)")
    clickhouse_secure: bool = Field(default=False, description="Use TLS — set true for ClickHouse Cloud")
    clickhouse_user: str = Field(default="dev", description="ClickHouse user")
    clickhouse_password: str = Field(default="", description="ClickHouse password")
    clickhouse_database: str = Field(default="gtmdb", description="ClickHouse database name")

    # LLM (planner agent)
    openai_api_key: str = Field(default="", description="OpenAI API key for planner agent")
    planner_model: str = Field(default="gpt-4o-mini", description="Fast model for simple/single-engine queries")
    planner_model_complex: str = Field(default="gpt-4o", description="Capable model for multi-engine queries")

    model_config = {"env_prefix": "GTMDB_", "env_file": ".env", "extra": "ignore"}

    @field_validator("key_store_url", mode="before")
    @classmethod
    def _empty_key_store(cls, v: object) -> object:
        if v == "":
            return None
        return v

    @field_validator("neo4j_connection_timeout", "neo4j_connection_acquisition_timeout", mode="before")
    @classmethod
    def _empty_float_opt(cls, v: object) -> object:
        if v == "" or v is None:
            return None
        return v
