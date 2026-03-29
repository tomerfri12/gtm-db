from gtmdb.server.routers._crud import build_crud_router

router = build_crud_router("emails", db_attr="emails")
