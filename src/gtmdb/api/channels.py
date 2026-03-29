"""ChannelsAPI -- typed CRUD for Channel nodes."""

from __future__ import annotations

from gtmdb.api._base import EntityAPI
from gtmdb.api.models import Channel


class ChannelsAPI(EntityAPI[Channel]):
    _label = "Channel"
    _entity_cls = Channel
