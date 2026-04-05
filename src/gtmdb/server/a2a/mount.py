"""Register A2A routes and auth middleware on the main FastAPI app."""

from __future__ import annotations

import os

from a2a.server.apps.jsonrpc.fastapi_app import A2AFastAPIApplication
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryTaskStore
from fastapi import FastAPI

from gtmdb.config import GtmdbSettings
from gtmdb.server.a2a.agent_card import apply_card_url_modifier, build_agent_card
from gtmdb.server.a2a.constants import A2A_RPC_PATH
from gtmdb.server.a2a.context import GtmDBCallContextBuilder
from gtmdb.server.a2a.executor import GtmDBAnalystExecutor
from gtmdb.server.a2a.middleware import (
    A2AAuthMiddleware,
    AgentCardPublicBaseMiddleware,
)
from gtmdb.server.config import ServerSettings


def _public_base_url(app: FastAPI) -> str:
    cfg = getattr(app.state, "gtmdb_settings", None)
    if cfg is None:
        cfg = GtmdbSettings()
    server = getattr(app.state, "server_settings", None)
    if server is None:
        server = ServerSettings()
    base = (cfg.public_url or "").strip().rstrip("/")
    if base:
        return base
    port = int(os.environ.get("PORT", str(server.port)))
    return f"http://127.0.0.1:{port}"


def install_a2a(app: FastAPI) -> None:
    """Mount well-known Agent Card + JSON-RPC at :data:`A2A_RPC_PATH`."""
    card = build_agent_card(public_base_url=_public_base_url(app))
    task_store = InMemoryTaskStore()
    executor = GtmDBAnalystExecutor(app)
    handler = DefaultRequestHandler(
        agent_executor=executor,
        task_store=task_store,
    )
    a2a = A2AFastAPIApplication(
        agent_card=card,
        http_handler=handler,
        context_builder=GtmDBCallContextBuilder(),
        card_modifier=apply_card_url_modifier,
    )
    a2a.add_routes_to_app(app, rpc_url=A2A_RPC_PATH)
    # Last added is outermost on the request: infer public URL for the card, then Bearer check for JSON-RPC.
    app.add_middleware(A2AAuthMiddleware, rpc_path=A2A_RPC_PATH)
    app.add_middleware(AgentCardPublicBaseMiddleware)
