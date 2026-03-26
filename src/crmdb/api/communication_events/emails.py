"""EmailsAPI -- typed CRUD for Email nodes."""

from __future__ import annotations

from crmdb.api._base import EntityAPI
from crmdb.api.models import Email


class EmailsAPI(EntityAPI[Email]):
    _label = "Email"
    _entity_cls = Email
