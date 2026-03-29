from gtmdb.server.routers._crud import build_crud_router

router = build_crud_router("products", db_attr="products")
