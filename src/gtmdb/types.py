from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class NodeData:
    label: str
    id: str
    tenant_id: str
    properties: dict = field(default_factory=dict)


@dataclass
class EdgeData:
    type: str
    from_id: str
    to_id: str
    properties: dict = field(default_factory=dict)
    reasoning: str | None = None
