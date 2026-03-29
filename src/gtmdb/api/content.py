"""ContentAPI -- typed CRUD for Content nodes."""

from __future__ import annotations

from gtmdb.api._base import EntityAPI
from gtmdb.api.models import Content


class ContentAPI(EntityAPI[Content]):
    _label = "Content"
    _entity_cls = Content
