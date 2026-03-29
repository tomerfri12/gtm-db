"""Pydantic models for REST bodies and responses."""

from gtmdb.server.schemas.admin import (
    KeyCreateBody,
    KeyCreatedResponse,
    KeyInfoResponse,
)
from gtmdb.server.schemas.common import ErrorResponse

__all__ = [
    "ErrorResponse",
    "KeyCreateBody",
    "KeyCreatedResponse",
    "KeyInfoResponse",
]
