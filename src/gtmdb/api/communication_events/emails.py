"""EmailsAPI -- typed CRUD for Email nodes."""

from __future__ import annotations

from gtmdb.api._base import EntityAPI
from gtmdb.api.models import Email


class EmailsAPI(EntityAPI[Email]):
    _label = "Email"
    _entity_cls = Email
