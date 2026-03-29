"""CLI: ``python -m gtmdb init [--seed]`` and ``python -m gtmdb keys ...``.

All commands authenticate via the admin key (``GTMDB_ADMIN_KEY`` env var or
``--api-key`` flag).
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys


def _resolve_admin_key(cli_key: str | None) -> str | None:
    """Return the admin key from the CLI flag or env, or None."""
    return (cli_key or os.environ.get("GTMDB_ADMIN_KEY") or "").strip() or None


async def _connect(api_key: str | None):
    """Shared connect helper. Returns ``(db, scope)`` or exits on failure."""
    from gtmdb.connect import connect_gtmdb

    if not api_key:
        print(
            "Admin key required: set GTMDB_ADMIN_KEY or pass --api-key.",
            file=sys.stderr,
        )
        raise SystemExit(1)
    return await connect_gtmdb(api_key=api_key)


async def _run_init(*, seed: bool, api_key: str | None) -> int:
    from gtmdb.seed import seed_sample_graph

    try:
        db, scope = await _connect(api_key)
    except SystemExit:
        raise
    except Exception as e:
        print(f"GtmDB connect failed: {e}", file=sys.stderr)
        return 1

    if seed:
        try:
            await seed_sample_graph(db, scope, id_suffix="gtmdb")
        except Exception as e:
            print(f"Seed failed: {e}", file=sys.stderr)
            await db.close()
            return 1
        print("Sample graph seeded.")

    await db.close()
    print("GtmDB graph store ready (constraints and indexes applied).")
    return 0


async def _run_keys(args: argparse.Namespace, *, api_key: str | None) -> int:
    try:
        db, scope = await _connect(api_key)
    except SystemExit:
        raise
    except Exception as e:
        print(f"GtmDB connect failed: {e}", file=sys.stderr)
        return 1

    from gtmdb.config import GtmdbSettings
    tenant_id = GtmdbSettings().default_tenant_id
    mgr = db.api_keys

    try:
        if args.keys_cmd == "create":
            presets = [p.strip() for p in args.presets.split(",") if p.strip()]
            result = await mgr.create(
                owner_id=args.owner_id,
                owner_type=args.owner_type,
                tenant_id=tenant_id,
                preset_names=presets,
                label=args.label or "",
                expires_in_days=args.expires_days,
                created_by="cli",
            )
            print(f"Key created. Store this — it will NOT be shown again:\n")
            print(f"  API Key:    {result.raw_key}")
            print(f"  Key ID:     {result.key_id}")
            print(f"  Owner:      {result.owner_id}")
            print(f"  Expires:    {result.expires_at or 'never'}")

        elif args.keys_cmd == "list":
            keys = await mgr.list_keys(tenant_id)
            if not keys:
                print("No active keys.")
            else:
                for k in keys:
                    exp = k.expires_at or "never"
                    used = k.last_used_at or "never"
                    print(
                        f"  {k.key_id}  owner={k.owner_id}  type={k.owner_type}  "
                        f"label={k.label!r}  expires={exp}  last_used={used}"
                    )

        elif args.keys_cmd == "revoke":
            ok = await mgr.revoke(args.key_id)
            if ok:
                print(f"Key {args.key_id} revoked.")
            else:
                print(f"Key {args.key_id} not found.", file=sys.stderr)
                return 1

        elif args.keys_cmd == "rotate":
            result = await mgr.rotate(
                args.key_id,
                expires_in_days=args.expires_days,
            )
            print(f"Old key {args.key_id} revoked. New key:\n")
            print(f"  API Key:    {result.raw_key}")
            print(f"  Key ID:     {result.key_id}")
            print(f"  Expires:    {result.expires_at or 'never'}")

    finally:
        await db.close()

    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m gtmdb",
        description="GtmDB tooling. Requires GTMDB_ADMIN_KEY (env or --api-key).",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="Admin key (overrides GTMDB_ADMIN_KEY env var).",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # --- init ---
    init_p = sub.add_parser("init", help="Connect and bootstrap graph schema.")
    init_p.add_argument(
        "--seed",
        action="store_true",
        help="Load demo graph data for default_tenant_id.",
    )

    # --- keys ---
    keys_p = sub.add_parser("keys", help="Manage API keys (Postgres key store).")
    keys_sub = keys_p.add_subparsers(dest="keys_cmd", required=True)

    kc = keys_sub.add_parser("create", help="Create a new API key.")
    kc.add_argument("--owner-id", required=True, help="Owner identity (e.g. agent id).")
    kc.add_argument("--owner-type", default="agent", help="Owner type (default: agent).")
    kc.add_argument("--presets", default="full_access", help="Comma-separated preset names.")
    kc.add_argument("--label", default="", help="Human-readable label.")
    kc.add_argument("--expires-days", type=int, default=None, help="Days until expiry.")

    keys_sub.add_parser("list", help="List active API keys.")

    kr = keys_sub.add_parser("revoke", help="Revoke an API key.")
    kr.add_argument("key_id", help="The key_id prefix to revoke.")

    krot = keys_sub.add_parser("rotate", help="Rotate an API key (create new, revoke old).")
    krot.add_argument("key_id", help="The key_id prefix to rotate.")
    krot.add_argument("--expires-days", type=int, default=None, help="Days until new key expires.")

    # --- serve ---
    serve_p = sub.add_parser("serve", help="Run the REST API server (FastAPI + uvicorn).")
    serve_p.add_argument(
        "--host",
        default=None,
        help="Bind host (default: GTMDB_SERVER_HOST or 0.0.0.0).",
    )
    serve_p.add_argument(
        "--port",
        type=int,
        default=None,
        help="Port (default: PORT env or GTMDB_SERVER_PORT or 8100).",
    )

    args = parser.parse_args()
    admin_key = _resolve_admin_key(getattr(args, "api_key", None))

    if args.cmd == "init":
        raise SystemExit(asyncio.run(_run_init(seed=args.seed, api_key=admin_key)))
    elif args.cmd == "keys":
        raise SystemExit(asyncio.run(_run_keys(args, api_key=admin_key)))
    elif args.cmd == "serve":
        import uvicorn

        from gtmdb.server.config import ServerSettings

        ss = ServerSettings()
        host = args.host or ss.host
        port = args.port or int(os.environ.get("PORT", str(ss.port)))
        uvicorn.run(
            "gtmdb.server.app:app",
            host=host,
            port=port,
            proxy_headers=True,
            forwarded_allow_ips="*",
        )
    else:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
