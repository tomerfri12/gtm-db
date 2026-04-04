"""LangGraph Analyst agent — built with the LangChain 1.x create_agent API.

https://docs.langchain.com/oss/python/langchain/agents
"""

from __future__ import annotations

import logging

from langchain.agents import create_agent
from langchain_openai import ChatOpenAI

from gtmdb.analyst.tools import execute_cypher, execute_sql, get_schema, think

log = logging.getLogger(__name__)

TOOLS = [think, execute_sql, execute_cypher, get_schema]


def build_analyst_graph(
    *,
    system_prompt: str,
    model: str = "gpt-4o-mini",
    openai_api_key: str,
    temperature: float = 0.0,
):
    """Build and return the compiled analyst agent."""
    llm = ChatOpenAI(
        model=model,
        temperature=temperature,
        api_key=openai_api_key,
        streaming=True,
    )

    agent = create_agent(
        llm,
        tools=TOOLS,
        system_prompt=system_prompt,
        name="analyst",
    )

    log.info(
        "[analyst] Agent compiled (model=%s, tools=%s)",
        model,
        [t.name for t in TOOLS],
    )
    return agent
