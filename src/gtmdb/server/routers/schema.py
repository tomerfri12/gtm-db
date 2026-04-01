"""Graph schema introspection for agents."""

from __future__ import annotations

import dataclasses
from typing import Any

from fastapi import APIRouter

from gtmdb.api.models import (
    Account,
    Campaign,
    Channel,
    Contact,
    Content,
    Deal,
    Email,
    EmailCampaign,
    Lead,
    Product,
    ProductAccount,
    Score,
    SubscriptionEvent,
    Visitor,
)

router = APIRouter(tags=["schema"])

_SYSTEM = frozenset(
    {
        "id",
        "tenant_id",
        "created_at",
        "updated_at",
        "created_by_actor_id",
        "updated_by_actor_id",
    }
)

_ENTITY_CLASSES: list[tuple[str, type]] = [
    ("Lead", Lead),
    ("Score", Score),
    ("Account", Account),
    ("Contact", Contact),
    ("Deal", Deal),
    ("Campaign", Campaign),
    ("EmailCampaign", EmailCampaign),
    ("Email", Email),
    ("Channel", Channel),
    ("Product", Product),
    ("ProductAccount", ProductAccount),
    ("Content", Content),
    ("Visitor", Visitor),
    ("SubscriptionEvent", SubscriptionEvent),
]

_RELATIONSHIPS: list[dict[str, Any]] = [
    {
        "type": "WORKS_AT",
        "from": ["Contact", "Lead"],
        "to": ["Account", "ProductAccount"],
    },
    {"type": "SOURCED_FROM", "from": ["Lead"], "to": ["Campaign", "EmailCampaign"]},
    {"type": "BELONGS_TO", "from": ["ProductAccount"], "to": ["Account"]},
    {"type": "BELONGS_TO", "from": ["Deal"], "to": ["Account", "ProductAccount"]},
    {"type": "INFLUENCED", "from": ["Campaign"], "to": ["Deal"]},
    {"type": "HAS_CONTACT", "from": ["Deal"], "to": ["Contact"]},
    {"type": "CONVERTED_TO", "from": ["Lead", "Visitor"], "to": ["Contact"]},
    {"type": "HAS_SCORE", "from": ["Lead"], "to": ["Score"]},
    {"type": "HAS_EMAIL", "from": ["EmailCampaign"], "to": ["Email"]},
    {"type": "HAS_CAMPAIGN", "from": ["Channel"], "to": ["Campaign"]},
    {"type": "HAS_CONTENT", "from": ["Campaign"], "to": ["Content"]},
    {"type": "SIGNED_UP_FOR", "from": ["Lead", "Visitor"], "to": ["Product"]},
    {"type": "FOR_PRODUCT", "from": ["Deal", "SubscriptionEvent", "ProductAccount"], "to": ["Product"]},
    {"type": "TOUCHED", "from": ["Visitor"], "to": ["Campaign"]},
    {"type": "LANDED_ON", "from": ["Visitor"], "to": ["Content"]},
    {"type": "SIGNED_UP_AS", "from": ["Visitor", "Lead"], "to": ["Account", "ProductAccount"]},
    {
        "type": "HAS_SUBSCRIPTION_EVENT",
        "from": ["Account", "ProductAccount", "Visitor", "Lead"],
        "to": ["SubscriptionEvent"],
    },
    {"type": "CREATED_BY", "from": ["Actor"], "to": ["*"]},
    {"type": "UPDATED_BY", "from": ["Actor"], "to": ["*"]},
]


@router.get("/schema")
def get_schema() -> dict[str, Any]:
    node_types: dict[str, Any] = {}
    for label, cls in _ENTITY_CLASSES:
        fields = [
            f.name
            for f in dataclasses.fields(cls)
            if f.name not in _SYSTEM
        ]
        node_types[label] = {"fields": fields}
    return {
        "node_types": node_types,
        "relationships": _RELATIONSHIPS,
    }
