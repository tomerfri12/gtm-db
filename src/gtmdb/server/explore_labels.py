"""Parse and validate explore label filters (traversal scope)."""

from __future__ import annotations

_EXPLORE_LABEL_FILTER_MAX = 40


def parse_explore_label_csv(raw: str | None) -> list[str]:
    """Split comma-separated labels; strip; drop empties; cap count."""
    if not raw or not str(raw).strip():
        return []
    parts = [p.strip() for p in str(raw).split(",")]
    out = [p for p in parts if p]
    return out[:_EXPLORE_LABEL_FILTER_MAX]


def normalize_labels_for_cypher(labels: list[str]) -> list[str]:
    """Lowercase for case-insensitive match against ``labels(b)`` in Cypher."""
    return [x.lower() for x in labels if x.strip()]
