"""A2A HTTP paths mounted on the main FastAPI app."""

from __future__ import annotations

# JSON-RPC (message/send, message/stream, tasks/*) — same Bearer semantics as /v1.
A2A_RPC_PATH = "/v1/a2a"
