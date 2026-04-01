"""Unit tests for explore error helpers."""

from __future__ import annotations

from gtmdb.server.explore_errors import explore_failure_detail, is_likely_neo4j_timeout


def test_explore_failure_detail_shape() -> None:
    d = explore_failure_detail(error="explore_timeout", message="x")
    assert d["error"] == "explore_timeout"
    assert d["message"] == "x"
    assert isinstance(d["suggestions"], list)
    assert len(d["suggestions"]) >= 2


def test_is_likely_neo4j_timeout_message() -> None:
    assert is_likely_neo4j_timeout(RuntimeError("Query timed out after 30s"))


def test_is_likely_neo4j_timeout_code() -> None:
    class _E(Exception):
        code = "Neo.TransientError.Transaction.Terminated"

    assert is_likely_neo4j_timeout(_E("x"))
