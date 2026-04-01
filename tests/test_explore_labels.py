"""Explore label filter parsing."""

from __future__ import annotations

from gtmdb.server.explore_labels import (
    normalize_labels_for_cypher,
    parse_explore_label_csv,
)


def test_parse_explore_label_csv_basic() -> None:
    assert parse_explore_label_csv("Visitor, Lead") == ["Visitor", "Lead"]
    assert parse_explore_label_csv(None) == []
    assert parse_explore_label_csv("  ") == []


def test_normalize_labels_for_cypher() -> None:
    assert normalize_labels_for_cypher(["Visitor", "Lead"]) == ["visitor", "lead"]
