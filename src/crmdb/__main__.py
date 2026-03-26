"""CLI: ``python -m crmdb init [--seed]`` — bootstrap the CRMDB graph store."""

from __future__ import annotations

import argparse
import asyncio
import sys
import uuid


async def _run_init(*, seed: bool) -> int:
    from crmdb.config import CrmdbSettings
    from crmdb.connect import connect_crmdb

    try:
        db = await connect_crmdb()
    except Exception as e:
        print(f"CRMDB graph init failed: {e}", file=sys.stderr)
        return 1

    if seed:
        from crmdb.presets import create_token_from_presets
        from crmdb.scope import Scope
        from crmdb.seed import seed_sample_graph

        s = CrmdbSettings()
        tenant = uuid.UUID(s.default_tenant_id)
        token = create_token_from_presets(
            tenant,
            "cli_seed",
            "agent",
            ["full_access"],
            label="token:crmdb-cli",
        )
        scope = Scope(token)
        try:
            await seed_sample_graph(db, scope, id_suffix="crmdb")
        except Exception as e:
            print(f"Seed failed: {e}", file=sys.stderr)
            await db.close()
            return 1
        print("Sample graph seeded.")

    await db.close()
    print("CRMDB graph store ready (constraints and indexes applied).")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m crmdb",
        description="CRMDB tooling (graph store lives in this package only).",
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
