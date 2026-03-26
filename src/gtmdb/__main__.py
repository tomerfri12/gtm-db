"""CLI: ``python -m gtmdb init [--seed]`` — bootstrap the GtmDB graph store."""

from __future__ import annotations

import argparse
import asyncio
import sys
import uuid


async def _run_init(*, seed: bool) -> int:
    from gtmdb.config import GtmdbSettings
    from gtmdb.connect import connect_gtmdb

    try:
        db = await connect_gtmdb()
    except Exception as e:
        print(f"GtmDB graph init failed: {e}", file=sys.stderr)
        return 1

    if seed:
        from gtmdb.presets import create_token_from_presets
        from gtmdb.scope import Scope
        from gtmdb.seed import seed_sample_graph

        s = GtmdbSettings()
        tenant = uuid.UUID(s.default_tenant_id)
        token = create_token_from_presets(
            tenant,
            "cli_seed",
            "agent",
            ["full_access"],
            label="token:gtmdb-cli",
        )
        scope = Scope(token)
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


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m gtmdb",
        description="GtmDB tooling (graph store lives in this package only).",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    init_p = sub.add_parser("init", help="Connect and bootstrap graph schema.")
    init_p.add_argument(
        "--seed",
        action="store_true",
        help="Load demo graph data for default_tenant_id.",
    )
    args = parser.parse_args()
    if args.cmd == "init":
        raise SystemExit(asyncio.run(_run_init(seed=args.seed)))
    raise SystemExit(2)


if __name__ == "__main__":
    main()
