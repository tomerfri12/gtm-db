"""LangSmith / LangChain tracing — maps gtmDB settings to process env.

LangChain reads ``LANGCHAIN_TRACING_V2``, ``LANGCHAIN_API_KEY``, and
``LANGCHAIN_PROJECT`` at run time. Set ``GTMDB_LANGSMITH_API_KEY`` (and
optionally ``GTMDB_LANGSMITH_PROJECT``) to enable tracing for the Analyst.
"""

from __future__ import annotations

import os

from gtmdb.config import GtmdbSettings

_DEFAULT_PROJECT = "gtmdb-analyst"


def configure_langsmith_env(cfg: GtmdbSettings) -> None:
    """If ``GTMDB_LANGSMITH_API_KEY`` is set, enable LangSmith tracing for LangChain."""
    key = (cfg.langsmith_api_key or "").strip()
    if not key:
        return
    os.environ["LANGCHAIN_TRACING_V2"] = "true"
    os.environ["LANGCHAIN_API_KEY"] = key
    project = (cfg.langsmith_project or _DEFAULT_PROJECT).strip() or _DEFAULT_PROJECT
    os.environ["LANGCHAIN_PROJECT"] = project
