"""Structured errors for ``GET /v1/entities/{id}/explore`` (agent-friendly)."""

from __future__ import annotations

EXPLORE_FAILURE_SUGGESTIONS: list[str] = [
    "Retry with depth=1 and mode=compact.",
    "Fetch the center with GET /v1/{entity}/{id} (infer entity type from search or a prior response).",
    "Use list endpoints with filters instead of a large explore subgraph.",
    "If the center is a hub (e.g. Actor, Campaign), explore is heavier; prefer targeted hops or lower depth.",
]


def explore_failure_detail(*, error: str, message: str) -> dict[str, object]:
    return {
        "error": error,
        "message": message,
        "suggestions": list(EXPLORE_FAILURE_SUGGESTIONS),
    }


def is_likely_neo4j_timeout(exc: BaseException) -> bool:
    code = (getattr(exc, "code", None) or "") + ""
    msg = str(exc).lower()
    if "timeout" in msg or "timed out" in msg:
        return True
    cl = type(exc).__name__.lower()
    if "timeout" in cl:
        return True
    c = code.lower()
    return "terminated" in c or "executiontimeout" in c
