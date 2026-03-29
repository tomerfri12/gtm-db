"""Serialize dataclass entities to JSON-friendly dicts."""

from __future__ import annotations

import dataclasses
from typing import Any


def entity_as_dict(obj: Any) -> dict[str, Any]:
    if obj is None:
        return {}
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return {
            k: v
            for k, v in dataclasses.asdict(obj).items()
            if v is not None
        }
    if isinstance(obj, dict):
        return {k: v for k, v in obj.items() if v is not None}
    raise TypeError(f"Cannot serialize {type(obj)!r}")
