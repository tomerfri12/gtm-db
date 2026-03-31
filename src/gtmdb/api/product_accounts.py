"""ProductAccountsAPI -- typed CRUD for ProductAccount nodes."""

from __future__ import annotations

from gtmdb.api._base import EntityAPI
from gtmdb.api.models import ProductAccount


class ProductAccountsAPI(EntityAPI[ProductAccount]):
    _label = "ProductAccount"
    _entity_cls = ProductAccount
