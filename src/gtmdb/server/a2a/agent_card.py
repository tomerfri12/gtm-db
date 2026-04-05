"""A2A v0.3 Agent Card for the gtmDB Analyst."""

from __future__ import annotations

import contextvars
from importlib.metadata import PackageNotFoundError, version

from a2a.types import (
    AgentCapabilities,
    AgentCard,
    AgentSkill,
    HTTPAuthSecurityScheme,
)
from a2a.utils.constants import (
    AGENT_CARD_WELL_KNOWN_PATH,
    PREV_AGENT_CARD_WELL_KNOWN_PATH,
)
from starlette.requests import Request

from gtmdb.config import GtmdbSettings
from gtmdb.server.a2a.constants import A2A_RPC_PATH

_CARD_INFER_BASE: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "gtmdb_a2a_card_infer_base",
    default=None,
)

_AGENT_CARD_PATHS = frozenset(
    {AGENT_CARD_WELL_KNOWN_PATH, PREV_AGENT_CARD_WELL_KNOWN_PATH}
)


def _package_version() -> str:
    try:
        return version("gtmdb")
    except PackageNotFoundError:
        return "0.0.0"


def infer_public_base_from_request(request: Request) -> str | None:
    """Best-effort public origin for the Agent Card ``url`` (Railway, nginx, etc.)."""
    host = (
        request.headers.get("x-forwarded-host") or request.headers.get("host") or ""
    ).strip()
    if not host:
        return None
    proto = (
        request.headers.get("x-forwarded-proto") or request.url.scheme or "https"
    ).strip().lower()
    if proto not in ("http", "https"):
        proto = "https"
    return f"{proto}://{host}".rstrip("/")


def push_inferred_card_public_base(request: Request) -> contextvars.Token | None:
    """If this is an agent-card GET, stash inferred base for :func:`apply_card_url_modifier`."""
    if request.method != "GET" or request.url.path not in _AGENT_CARD_PATHS:
        return None
    base = infer_public_base_from_request(request)
    if not base:
        return None
    return _CARD_INFER_BASE.set(base)


def pop_inferred_card_public_base(token: contextvars.Token | None) -> None:
    if token is not None:
        _CARD_INFER_BASE.reset(token)


def with_rpc_url_for_base(card: AgentCard, public_base: str) -> AgentCard:
    base = public_base.strip().rstrip("/")
    if not base:
        return card
    return card.model_copy(update={"url": f"{base}{A2A_RPC_PATH}"})


async def apply_card_url_modifier(card: AgentCard) -> AgentCard:
    """Prefer ``GTMDB_PUBLIC_URL``, else proxy-inferred base, else static card."""
    explicit = (GtmdbSettings().public_url or "").strip().rstrip("/")
    if explicit:
        return with_rpc_url_for_base(card, explicit)
    inferred = _CARD_INFER_BASE.get()
    if inferred:
        return with_rpc_url_for_base(card, inferred)
    return card


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
