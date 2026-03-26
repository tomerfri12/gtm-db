from pydantic_settings import BaseSettings


class CrmdbSettings(BaseSettings):
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "crmdb_password"
    # Used by ``python -m crmdb init --seed`` (align with host app tenant if needed).
    default_tenant_id: str = "00000000-0000-4000-8000-000000000001"

    model_config = {"env_prefix": "CRMDB_", "env_file": ".env", "extra": "ignore"}
