"""SubscriptionEventsAPI -- typed CRUD for SubscriptionEvent nodes."""

from __future__ import annotations

from gtmdb.api._base import EntityAPI
from gtmdb.api.models import SubscriptionEvent


class SubscriptionEventsAPI(EntityAPI[SubscriptionEvent]):
    _label = "SubscriptionEvent"
    _entity_cls = SubscriptionEvent
