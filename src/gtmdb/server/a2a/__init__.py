"""A2A Protocol v0.3 endpoints (Agent Card + JSON-RPC Analyst)."""

from __future__ import annotations

from gtmdb.server.a2a.constants import A2A_RPC_PATH
from gtmdb.server.a2a.mount import install_a2a

__all__ = ["A2A_RPC_PATH", "install_a2a"]
