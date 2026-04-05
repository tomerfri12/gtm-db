"""LangSmith env wiring."""

from __future__ import annotations

import os

import pytest

from gtmdb.config import GtmdbSettings
from gtmdb.tracing import configure_langsmith_env


def test_configure_langsmith_env_sets_langchain_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LANGCHAIN_TRACING_V2", raising=False)
    monkeypatch.delenv("LANGCHAIN_API_KEY", raising=False)
    monkeypatch.delenv("LANGCHAIN_PROJECT", raising=False)
    cfg = GtmdbSettings(
        langsmith_api_key="ls-test-key",
        langsmith_project="my-test-project",
    )
    configure_langsmith_env(cfg)
    assert os.environ.get("LANGCHAIN_TRACING_V2") == "true"
    assert os.environ.get("LANGCHAIN_API_KEY") == "ls-test-key"
    assert os.environ.get("LANGCHAIN_PROJECT") == "my-test-project"


def test_configure_langsmith_env_noop_without_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LANGCHAIN_TRACING_V2", raising=False)
    configure_langsmith_env(GtmdbSettings(langsmith_api_key=None))
    assert os.environ.get("LANGCHAIN_TRACING_V2") is None
