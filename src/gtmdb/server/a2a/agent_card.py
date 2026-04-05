"""A2A v0.3 Agent Card for the gtmDB Analyst."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from a2a.types import (
    AgentCapabilities,
    AgentCard,
    AgentSkill,
    HTTPAuthSecurityScheme,
)

from gtmdb.server.a2a.constants import A2A_RPC_PATH


def _package_version() -> str:
    try:
        return version("gtmdb")
    except PackageNotFoundError:
        return "0.0.0"


def build_agent_card(*, public_base_url: str) -> AgentCard:
    """Build the public Agent Card (``GET /.well-known/agent-card.json``)."""
    base = public_base_url.rstrip("/")
    rpc_url = f"{base}{A2A_RPC_PATH}"

    return AgentCard(
        name="gtmDB Analyst",
        description=(
            "Natural-language analytics over your tenant's GTM graph and OLAP store. "
            "Effective access is determined by the Bearer API key's policies (same as REST /v1), "
            "not by skill id."
        ),
        version=_package_version(),
        protocol_version="0.3.0",
        url=rpc_url,
        preferred_transport="JSONRPC",
        capabilities=AgentCapabilities(streaming=True),
        default_input_modes=["text/plain"],
        default_output_modes=["text/plain"],
        security_schemes={
            "bearerAuth": HTTPAuthSecurityScheme(
                scheme="bearer",
                bearer_format="API key",
                description="Same Bearer token as REST /v1 (admin key or Postgres-backed agent key).",
            ),
        },
        security=[{"bearerAuth": []}],
        skills=[
            AgentSkill(
                id="gtmdb.analyst.query",
                name="Analyst query",
                description="Ask questions in plain language; the agent plans SQL/Cypher and returns answers.",
                tags=["analytics", "gtm", "crm", "sql", "cypher"],
                examples=[
                    "Which campaigns drove the most paid conversions last quarter?",
                    "What is pipeline by stage for account X?",
                ],
            ),
        ],
    )
