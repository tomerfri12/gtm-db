"""Process-level server settings (host/port, explore limits)."""

from pydantic import Field
from pydantic_settings import BaseSettings


class ServerSettings(BaseSettings):
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8100)
    explore_default_depth: int = Field(default=1, ge=1, le=5)
    explore_max_depth: int = Field(default=3, ge=1, le=5)
    explore_nodes_per_type: int = Field(default=10, ge=1, le=50)

    model_config = {"env_prefix": "GTMDB_SERVER_", "env_file": ".env", "extra": "ignore"}
