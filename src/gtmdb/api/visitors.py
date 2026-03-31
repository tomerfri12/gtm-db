"""VisitorsAPI -- typed CRUD for Visitor nodes."""

from __future__ import annotations

from gtmdb.api._base import EntityAPI
from gtmdb.api.models import Visitor


class VisitorsAPI(EntityAPI[Visitor]):
    _label = "Visitor"
    _entity_cls = Visitor
