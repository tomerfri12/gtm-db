"""Require Bearer auth for A2A JSON-RPC POST (same keys as ``/v1``)."""

from __future__ import annotations

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from gtmdb.server.a2a.auth import BearerAuthFailed, resolve_bearer_to_scope
from gtmdb.server.a2a.constants import A2A_RPC_PATH


class A2AAuthMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, rpc_path: str = A2A_RPC_PATH) -> None:
        super().__init__(app)
        self._rpc_path = rpc_path

    async def dispatch(self, request: Request, call_next):  # type: ignore[no-untyped-def]
        if request.method == "POST" and request.url.path == self._rpc_path:
            try:
                await resolve_bearer_to_scope(request)
            except BearerAuthFailed as e:
                return JSONResponse({"detail": e.detail}, status_code=401)
        return await call_next(request)
