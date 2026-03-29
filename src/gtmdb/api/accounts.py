"""AccountsAPI -- typed CRUD for Account nodes."""

from __future__ import annotations

from gtmdb.api._base import EntityAPI
from gtmdb.api.models import Account


class AccountsAPI(EntityAPI[Account]):
    _label = "Account"
    _entity_cls = Account
