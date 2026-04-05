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
    scope:
        Optional :class:`~gtmdb.scope.Scope` instance. When provided, the
        caller's permission set is injected into the agent's system prompt
        (Layer 1 awareness) and made available to the query guard (Layer 2).
    trace_metadata:
        Extra key/value pairs attached to LangSmith runs (e.g. ``{"source": "a2a"}``).
    """

    def __init__(
        self,
        db,
        *,
        tenant_id: str | None = None,
        model: str | None = None,
        scope=None,
        trace_metadata: dict[str, Any] | None = None,
    ) -> None:
        self._db = db
        settings = GtmdbSettings()

        self._tenant_id = tenant_id or settings.default_tenant_id
        self._model = model or settings.planner_model
        self._api_key = settings.openai_api_key
        self._scope = scope
        self._trace_metadata = dict(trace_metadata) if trace_metadata else {}

        if not self._api_key:
            raise ValueError(
                "GTMDB_OPENAI_API_KEY is not set. "
                "Add it to .env to use the Analyst agent."
            )

        # Build schema context — includes permission summary when scope is provided
        system_prompt = build_system_prompt(self._tenant_id, scope=scope)

        # Wire tools to live adapters (scope passed for Layer 2 guard)
        _tools.configure(
            graph_adapter=db._graph,
            olap_store=db._olap_store,
            tenant_id=self._tenant_id,
            schema_text=system_prompt,
            scope=scope,
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

    def _run_config(self) -> dict[str, Any]:
        """LangGraph / LangSmith RunnableConfig (metadata + tags)."""
        meta: dict[str, Any] = {"tenant_id": self._tenant_id}
        if self._scope is not None:
            kid = getattr(self._scope, "key_id", None)
            if kid:
                meta["key_id"] = kid
            meta["owner_type"] = getattr(self._scope, "owner_type", "")
        meta.update(self._trace_metadata)
        return {"tags": ["gtmdb-analyst"], "metadata": meta}

    async def ask(self, question: str) -> AnalystResult:
        """Run a question to completion and return a structured result."""
        log.info("[analyst] ask: %s", question)

        messages = [HumanMessage(content=question)]
        final_state = await self._graph.ainvoke(
            {"messages": messages},
            config=self._run_config(),
        )

        all_messages = final_state["messages"]
        tool_calls = _extract_tool_calls(all_messages)
        answer = _extract_final_answer(all_messages)

        return AnalystResult(
            question=question,
            answer=answer,
            tool_calls=tool_calls,
            raw_messages=all_messages,
        )

    async def stream(self, question: str, *, verbose: bool = False) -> AsyncIterator[str]:
        """Stream intermediate steps and the final answer as text chunks.

        Parameters
        ----------
        verbose:
            When True, emit each tool call with the full query and a preview
            of the result so the caller can see the agent's plan step-by-step.
        """
        log.info("[analyst] stream: %s", question)

        messages = [HumanMessage(content=question)]
        step = 0

        async for event in self._graph.astream(
            {"messages": messages},
            stream_mode="values",
            config=self._run_config(),
        ):
            last = event["messages"][-1]

            if isinstance(last, AIMessage):
                if last.tool_calls and verbose:
                    step += 1
                    for tc in last.tool_calls:
                        tool_name = tc["name"]
                        args = tc.get("args", {})
                        query = args.get("query", "")
                        lang = "sql" if tool_name == "execute_sql" else "cypher"
                        header = f"\n{'─'*60}\nStep {step} → {tool_name}"
                        if query:
                            header += f"\n\n```{lang}\n{query.strip()}\n```"
                        yield header + "\n"
                elif last.tool_calls and not verbose:
                    for tc in last.tool_calls:
                        yield f"\n[{tc['name']}]\n"
                elif last.content:
                    yield last.content

            elif isinstance(last, ToolMessage):
                if verbose:
                    preview = last.content[:500]
                    ellipsis = "…" if len(last.content) > 500 else ""
                    yield f"\nResult preview:\n{preview}{ellipsis}\n"
                else:
                    yield f"[result]: {last.content[:200]}{'…' if len(last.content) > 200 else ''}\n"


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
