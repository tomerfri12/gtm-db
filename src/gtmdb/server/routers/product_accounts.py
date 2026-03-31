from gtmdb.server.routers._crud import build_crud_router

router = build_crud_router("product-accounts", db_attr="product_accounts")
