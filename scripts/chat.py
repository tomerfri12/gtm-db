"""Local dev chat server for the Analyst agent.

Serves a single-page chat UI with streaming plan display.

Usage
-----
    cd /Users/tomerfriedman/Development/gtmdb
    .venv/bin/python scripts/chat.py

Then open http://localhost:7433 in your browser.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

import uvicorn  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from fastapi.responses import HTMLResponse, StreamingResponse  # noqa: E402
from langchain_core.messages import AIMessage, ToolMessage  # noqa: E402
from pydantic import BaseModel  # noqa: E402

from gtmdb.analyst import AnalystRunner  # noqa: E402
from gtmdb.config import GtmdbSettings  # noqa: E402
from gtmdb.connect import connect_gtmdb  # noqa: E402
from gtmdb.scope import Scope  # noqa: E402
from gtmdb.tokens import AccessToken  # noqa: E402

logging.basicConfig(level=logging.WARNING)

from contextlib import asynccontextmanager  # noqa: E402

_runner: AnalystRunner | None = None
_scope: Scope | None = None


@asynccontextmanager
async def lifespan(app):
    global _runner, _scope
    settings = GtmdbSettings()
    db, _ = await connect_gtmdb(settings=settings, api_key=settings.admin_key)

    # --- Dev scope: full access ---
    token = AccessToken(
        tenant_id=settings.default_tenant_id,
        owner_id="dev-admin",
        owner_type="agent",
        label="Full access (dev)",
        policies=json.dumps([
            {"effect": "allow", "actions": ["read"], "resources": ["*"]},
        ]),
        redact_mode="hint",
    )
    _scope = Scope(token)
    _runner = AnalystRunner(db, scope=_scope)

    print("\n✓ Analyst ready (Full access) — open http://localhost:7433\n")
    yield
    await db.close()


app = FastAPI(title="gtmDB Analyst Chat", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Permissions endpoint
# ---------------------------------------------------------------------------

@app.get("/permissions")
async def permissions():
    """Return the current scope's allowed/denied resources as JSON."""
    if _scope is None:
        return {"allowed": [], "denied": [], "label": "No scope"}

    policies = getattr(_scope, "policies", [])
    allowed, denied = [], []
    for p in policies:
        effect = p.get("effect", "")
        resources = [r for r in p.get("resources", []) if "." not in r]
        if effect == "allow":
            allowed.extend(resources)
        elif effect == "deny":
            denied.extend(resources)

    label = getattr(getattr(_scope, "_token", None), "label", "Unknown scope")
    return {"allowed": sorted(allowed), "denied": sorted(denied), "label": label}


# ---------------------------------------------------------------------------
# SSE streaming endpoint
# ---------------------------------------------------------------------------

class AskRequest(BaseModel):
    question: str


@app.post("/ask")
async def ask(req: AskRequest) -> StreamingResponse:
    """Stream structured analyst events as Server-Sent Events.

    Event types emitted:
      {"type": "step",   "step": N, "tool": "execute_sql"|"execute_cypher", "lang": "sql"|"cypher", "query": "..."}
      {"type": "result", "step": N, "text": "..."}
      {"type": "answer", "text": "..."}   -- final answer chunk (may arrive multiple times)
      {"type": "error",  "text": "..."}
      {"type": "done"}
    """

    async def generate():
        assert _runner is not None
        step = 0
        from langchain_core.messages import HumanMessage
        messages = [HumanMessage(content=req.question)]
        try:
            tools_used = False  # tracks whether any tool has been called yet
            async for event in _runner._graph.astream(
                {"messages": messages}, stream_mode="values"
            ):
                last = event["messages"][-1]

                if isinstance(last, AIMessage):
                    if last.tool_calls:
                        tools_used = True
                        tool_names = {tc["name"] for tc in last.tool_calls}
                        # Models often put the plan in `content` and call sql/cypher without
                        # `think`. Previously we only emitted `content` when there were no
                        # tool_calls, so the UI never showed a plan box.
                        prose = str(last.content or "").strip()
                        if prose and "think" not in tool_names:
                            payload = json.dumps({"type": "plan", "text": prose})
                            yield f"data: {payload}\n\n"
                            await asyncio.sleep(0)

                        for tc in last.tool_calls:
                            tool_name = tc["name"]
                            args = tc.get("args") or {}
                            if not isinstance(args, dict):
                                args = {}

                            if tool_name == "think":
                                plan_text = args.get("plan") or args.get("input") or ""
                                payload = json.dumps({
                                    "type": "plan",
                                    "text": str(plan_text),
                                })
                                yield f"data: {payload}\n\n"
                                await asyncio.sleep(0)
                                continue

                            step += 1
                            query = args.get("query", "")
                            lang = (
                                "sql" if tool_name == "execute_sql"
                                else "cypher" if tool_name == "execute_cypher"
                                else "text"
                            )
                            payload = json.dumps({
                                "type": "step",
                                "step": step,
                                "tool": tool_name,
                                "lang": lang,
                                "query": query,
                            })
                            yield f"data: {payload}\n\n"
                            await asyncio.sleep(0)

                    elif last.content:
                        # Before any tool call → planning text
                        # After tool calls → final answer
                        msg_type = "answer" if tools_used else "plan"
                        payload = json.dumps({
                            "type": msg_type,
                            "text": str(last.content),
                        })
                        yield f"data: {payload}\n\n"
                        await asyncio.sleep(0)

                elif isinstance(last, ToolMessage):
                    # Skip think tool results — plan is already shown
                    if last.name == "think":
                        continue
                    preview = last.content[:600]
                    payload = json.dumps({
                        "type": "result",
                        "step": step,
                        "text": preview,
                        "truncated": len(last.content) > 600,
                    })
                    yield f"data: {payload}\n\n"
                    await asyncio.sleep(0)

        except Exception as exc:
            payload = json.dumps({"type": "error", "text": str(exc)})
            yield f"data: {payload}\n\n"
        finally:
            yield f"data: {json.dumps({'type': 'done'})}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# ---------------------------------------------------------------------------
# Serve the UI
# ---------------------------------------------------------------------------

UI_PATH = Path(__file__).parent / "chat.html"


@app.get("/", response_class=HTMLResponse)
async def ui():
    return HTMLResponse(UI_PATH.read_text())


if __name__ == "__main__":
    uvicorn.run("chat:app", host="0.0.0.0", port=7433, reload=False, app_dir=str(Path(__file__).parent))
