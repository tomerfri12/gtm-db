from gtmdb.server.routers._crud import build_crud_router

router = build_crud_router("subscription-events", db_attr="subscription_events")
