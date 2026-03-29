"""FastAPI HTTP server for GtmDB (``python -m gtmdb serve``)."""

from gtmdb.server.app import create_app

__all__ = ["create_app"]
