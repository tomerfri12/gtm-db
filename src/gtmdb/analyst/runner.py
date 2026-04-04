"""AnalystRunner — the public interface for the Analyst agent.

Usage
-----
::

    from gtmdb.analyst.runner import AnalystRunner

    runner = AnalystRunner(db, tenant_id="...")
    result = await runner.ask("Which campaigns drive the most paid conversions?")
    print(result.answer)

    # Streaming (yields intermediate steps as they happen)
    async for chunk in runner.stream("What is the ARR by channel?"):
        print(chunk)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, AsyncIterator

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from gtmdb.analyst import tools as _tools
from gtmdb.analyst.planner import build_analyst_graph
from gtmdb.analyst.schema_context import build_system_prompt
from gtmdb.config import GtmdbSettings

log = logging.getLogger(__name__)


@dataclass
class AnalystResult:
    """Structured output from a completed analyst run."""
    question: str
    answer: str
    tool_calls: list[dict[str, Any]] = field(default_factory=list)
    raw_messages: list[Any] = field(default_factory=list)

    def __str__(self) -> str:
        return self.answer


class AnalystRunner:
    """Thin async wrapper that wires the graph, tools, and adapters together.

    Parameters
    ----------
    db:
        A connected :class:`~gtmdb.client.GtmDB` instance.
    tenant_id:
        Tenant to scope all queries to. Defaults to ``db``'s configured tenant.
    model:
        OpenAI model name override (default: ``GtmdbSettings.planner_model``).
    """

    def __init__(
        self,
        db,
        *,
        tenant_id: str | None = None,
        model: str | None = None,
    ) -> None:
        self._db = db
        settings = GtmdbSettings()

        self._tenant_id = tenant_id or settings.default_tenant_id
        self._model = model or settings.planner_model
        self._api_key = settings.openai_api_key

        if not self._api_key:
            raise ValueError(
                "GTMDB_OPENAI_API_KEY is not set. "
                "Add it to .env to use the Analyst agent."
            )

        # Build schema context once
        system_prompt = build_system_prompt(self._tenant_id)

        # Wire tools to live adapters
        _tools.configure(
            graph_adapter=db._graph,
            olap_store=db._olap_store,
            tenant_id=self._tenant_id,
            schema_text=system_prompt,
        )

        # Compile the LangGraph agent
        self._graph = build_analyst_graph(
            system_prompt=system_prompt,
            model=self._model,
            openai_api_key=self._api_key,
        )

        log.info(
            "[analyst] AnalystRunner ready  tenant=%s  model=%s",
            self._tenant_id, self._model,
        )

    async def ask(self, question: str) -> AnalystResult:
        """Run a question to completion and return a structured result."""
        log.info("[analyst] ask: %s", question)

        messages = [HumanMessage(content=question)]
        final_state = await self._graph.ainvoke({"messages": messages})

        all_messages = final_state["messages"]
        tool_calls = _extract_tool_calls(all_messages)
        answer = _extract_final_answer(all_messages)

        return AnalystResult(
            question=question,
            answer=answer,
            tool_calls=tool_calls,
            raw_messages=all_messages,
        )

    async def stream(self, question: str) -> AsyncIterator[str]:
        """Stream intermediate steps and the final answer as text chunks."""
        log.info("[analyst] stream: %s", question)

        messages = [HumanMessage(content=question)]

        async for event in self._graph.astream(
            {"messages": messages},
            stream_mode="values",
        ):
            last = event["messages"][-1]

            if isinstance(last, AIMessage):
                if last.content:
                    yield last.content
                elif last.tool_calls:
                    for tc in last.tool_calls:
                        yield f"\n[calling {tc['name']}]\n"

            elif isinstance(last, ToolMessage):
                yield f"[{last.name} result]: {last.content[:300]}{'...' if len(last.content) > 300 else ''}\n"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_final_answer(messages: list) -> str:
    """Return the last AI message that is not a tool call."""
    for msg in reversed(messages):
        if isinstance(msg, AIMessage) and msg.content and not msg.tool_calls:
            return str(msg.content)
    return "(no answer produced)"


def _extract_tool_calls(messages: list) -> list[dict[str, Any]]:
    calls = []
    for msg in messages:
        if isinstance(msg, AIMessage) and msg.tool_calls:
            for tc in msg.tool_calls:
                calls.append({"tool": tc["name"], "args": tc["args"]})
        elif isinstance(msg, ToolMessage):
            if calls:
                calls[-1]["result_preview"] = msg.content[:200]
    return calls
