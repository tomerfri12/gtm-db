"""ProductsAPI -- typed CRUD for Product nodes."""

from __future__ import annotations

from gtmdb.api._base import EntityAPI
from gtmdb.api.models import Product


class ProductsAPI(EntityAPI[Product]):
    _label = "Product"
    _entity_cls = Product
