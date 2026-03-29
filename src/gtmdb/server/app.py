"""FastAPI application factory."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import APIRouter, FastAPI
from fastapi.responses import JSONResponse

from gtmdb.client import GtmDB
from gtmdb.config import GtmdbSettings
from gtmdb.server.config import ServerSettings
from gtmdb.server.routers import (
    accounts,
    admin,
    campaigns,
    contacts,
    deals,
    email_campaigns,
    emails,
    explore,
    health,
    leads,
    schema,
    search,
)


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    gtmdb_settings = GtmdbSettings()
    server_settings = ServerSettings()
    db = GtmDB(gtmdb_settings)
    await db.connect()
    app.state.gtmdb_settings = gtmdb_settings
    app.state.server_settings = server_settings
    app.state.db = db
    try:
        yield
    finally:
        await db.close()


def create_app() -> FastAPI:
    app = FastAPI(
        title="GtmDB API",
        version="1.0.0",
        description="Graph-native GTM data layer — REST + API key auth.",
        lifespan=_lifespan,
    )

    @app.exception_handler(PermissionError)
    async def _permission_error_handler(_request, exc: PermissionError):
        return JSONResponse(
            status_code=403,
            content={"detail": str(exc)},
        )

    app.include_router(health.router)

    v1 = APIRouter(prefix="/v1")
    v1.include_router(search.router)
    v1.include_router(schema.router)
    v1.include_router(explore.router)
    v1.include_router(admin.router)
    v1.include_router(accounts.router)
    v1.include_router(leads.router)
    v1.include_router(contacts.router)
    v1.include_router(deals.router)
    v1.include_router(campaigns.router)
    v1.include_router(emails.router)
    v1.include_router(email_campaigns.router)
    app.include_router(v1)

    return app


app = create_app()
