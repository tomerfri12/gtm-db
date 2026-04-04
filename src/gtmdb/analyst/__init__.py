"""gtmdb.analyst — LangGraph-powered Analyst agent.

The Analyst takes natural language questions and answers them by generating
and executing text2sql (ClickHouse) and text2cypher (Neo4j) queries,
fusing the results into a structured final answer.

Quick start::

    from gtmdb.analyst.runner import AnalystRunner

    runner = AnalystRunner(db, tenant_id="...")
    result = await runner.ask("Which campaigns drive the most ARR?")
    print(result.answer)
"""

from .runner import AnalystResult, AnalystRunner

__all__ = ["AnalystRunner", "AnalystResult"]
