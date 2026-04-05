"""A2A JSON-RPC call context: copy resolved Scope into ``ServerCallContext.state``."""

from __future__ import annotations

from a2a.server.apps.jsonrpc.jsonrpc_app import DefaultCallContextBuilder
from a2a.server.context import ServerCallContext
from starlette.requests import Request


class GtmDBCallContextBuilder(DefaultCallContextBuilder):
    """Extends default builder so the executor can read ``gtmdb_scope``."""

    def build(self, request: Request) -> ServerCallContext:
        ctx = super().build(request)
        scope = getattr(request.state, "gtmdb_scope", None)
        if scope is not None:
            ctx.state["gtmdb_scope"] = scope
        return ctx
