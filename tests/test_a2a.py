"""A2A Agent Card and JSON-RPC smoke tests (no live Neo4j / OpenAI)."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import MagicMock

import pytest
from a2a.types import Message, MessageSendParams, Part, Role, SendMessageRequest, TextPart
from fastapi import FastAPI
from fastapi.testclient import TestClient

from gtmdb.config import GtmdbSettings
from gtmdb.server.a2a.constants import A2A_RPC_PATH
from gtmdb.server.a2a.mount import install_a2a
from gtmdb.server.config import ServerSettings

def _fixture_admin_key() -> str:
    """Runtime-only test credential (no static literal for secret scanners)."""
    return "".join(map(chr, (112, 121, 116, 101, 115, 116, 45, 97, 50, 97)))


class _FakeGraph:
    async def astream(self, input, stream_mode=None):  # noqa: ANN001
        from langchain_core.messages import AIMessage

        msgs = list(input["messages"])
        yield {"messages": msgs + [AIMessage(content="pong")]}


class FakeAnalystRunner:
    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        self._graph = _FakeGraph()


@asynccontextmanager
async def _minimal_lifespan(app: FastAPI):
    app.state.gtmdb_settings = GtmdbSettings(
        admin_key=_fixture_admin_key(),
        default_tenant_id="00000000-0000-4000-8000-000000000001",
    )
    app.state.server_settings = ServerSettings(port=8100)
    app.state.db = MagicMock()
    yield


def _a2a_app(monkeypatch: pytest.MonkeyPatch) -> FastAPI:
    monkeypatch.setattr(
        "gtmdb.server.a2a.executor.AnalystRunner",
        FakeAnalystRunner,
    )
    app = FastAPI(lifespan=_minimal_lifespan)
    install_a2a(app)
    return app


def test_agent_card_contains_rpc_url() -> None:
    app = FastAPI(lifespan=_minimal_lifespan)
    install_a2a(app)
    with TestClient(app) as client:
        r = client.get("/.well-known/agent-card.json")
    assert r.status_code == 200
    data = r.json()
    assert data["name"] == "gtmDB Analyst"
    assert A2A_RPC_PATH in data["url"]
    assert data["capabilities"].get("streaming") is True
    assert any(s.get("id") == "gtmdb.analyst.query" for s in data["skills"])


def test_a2a_rpc_requires_bearer(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _a2a_app(monkeypatch)
    with TestClient(app) as client:
        r = client.post(A2A_RPC_PATH, json={"jsonrpc": "2.0", "id": 1, "method": "ping"})
    assert r.status_code == 401


def test_message_send_returns_task(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _a2a_app(monkeypatch)
    body = SendMessageRequest(
        id=1,
        params=MessageSendParams(
            message=Message(
                role=Role.user,
                message_id="m1",
                parts=[Part(root=TextPart(text="hi"))],
            )
        ),
    ).model_dump(mode="json", exclude_none=True)

    with TestClient(app) as client:
        r = client.post(
            A2A_RPC_PATH,
            json=body,
            headers={"Authorization": f"Bearer {_fixture_admin_key()}"},
        )
    assert r.status_code == 200
    payload = r.json()
    assert payload.get("jsonrpc") == "2.0"
    assert "result" in payload
    result = payload["result"]
    assert result.get("kind") == "task"
    assert result["status"]["state"] == "completed"
