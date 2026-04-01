"""Process-level server settings (host/port, explore limits)."""

from pydantic import Field
from pydantic_settings import BaseSettings


class ServerSettings(BaseSettings):
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8100)
    explore_default_depth: int = Field(default=1, ge=1, le=5)
    explore_max_depth: int = Field(default=3, ge=1, le=5)
    explore_nodes_per_type: int = Field(default=10, ge=1, le=50)
    # Hard cap on distinct nodes collected during explore BFS (before per-type cap).
    # Prevents hub nodes (e.g. Actor, Campaign) from timing out proxies.
    explore_max_discovered_nodes: int = Field(default=500, ge=20, le=50_000)
    # Neo4j read transaction timeout for /explore only. 0 = no driver cap (Neo4j server default).
    explore_transaction_timeout_s: float = Field(
        default=55.0,
        ge=0,
        description="Managed read tx timeout for explore; 0 disables client-side cap",
    )

    model_config = {"env_prefix": "GTMDB_SERVER_", "env_file": ".env", "extra": "ignore"}
