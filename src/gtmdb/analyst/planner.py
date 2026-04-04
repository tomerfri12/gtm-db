"""LangGraph Analyst agent — state graph definition.

Graph shape
-----------

    [start] ──► [agent]  ◄─────────────────────┐
                  │                             │
          has tool calls?                       │
                  │                             │
           yes ──►[tools] ──► back to agent ───┘
                  │
           no (final answer)
                  │
               [END]

The agent node calls the LLM (gpt-4o-mini by default).
The tools node dispatches to execute_sql / execute_cypher / get_schema.
LangGraph handles the loop automatically via `create_react_agent`.
"""

from __future__ import annotations

import logging

from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent

from gtmdb.analyst.tools import execute_cypher, execute_sql, get_schema

log = logging.getLogger(__name__)

TOOLS = [execute_sql, execute_cypher, get_schema]


def build_analyst_graph(
    *,
    system_prompt: str,
    model: str = "gpt-4o-mini",
    openai_api_key: str,
    temperature: float = 0.0,
):
    """Build and return the compiled LangGraph ReAct agent.

    Uses `create_react_agent` which gives us:
    - Automatic tool-call loop (agent → tools → agent → ... → END)
    - Built-in message history
    - Streaming support via `.astream()`
    """
    llm = ChatOpenAI(
        model=model,
        temperature=temperature,
        api_key=openai_api_key,
        streaming=True,
    )

    graph = create_react_agent(
        llm,
        tools=TOOLS,
        prompt=system_prompt,
    )

    log.info("[analyst] Graph compiled (model=%s, tools=%s)", model, [t.name for t in TOOLS])
    return graph
