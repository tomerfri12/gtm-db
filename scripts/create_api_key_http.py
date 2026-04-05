#!/usr/bin/env python3
"""Create an agent API key via REST (requires admin Bearer + Postgres key store on server).

Usage
-----
  export GTMDB_ADMIN_KEY='your-admin-key'
  export GTMDB_API_BASE='https://your-host'   # no trailing slash
  python scripts/create_api_key_http.py

  # Optional: owner, label, tenant override
  python scripts/create_api_key_http.py --owner-id openclaw --label "external agent"

The server must have ``GTMDB_KEY_STORE_URL`` set; otherwise create returns 500.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--base-url", default=os.environ.get("GTMDB_API_BASE", "").rstrip("/"))
    p.add_argument("--owner-id", default="api-client")
    p.add_argument("--owner-type", default="agent")
    p.add_argument("--label", default="")
    p.add_argument(
        "--presets",
        default="full_access",
        help="Comma-separated preset names (default: full_access)",
    )
    args = p.parse_args()

    admin = (os.environ.get("GTMDB_ADMIN_KEY") or "").strip()
    if not admin:
        print("Set GTMDB_ADMIN_KEY in the environment.", file=sys.stderr)
        return 1
    if not args.base_url:
        print("Set GTMDB_API_BASE or pass --base-url (e.g. https://gtm-db-production.up.railway.app).", file=sys.stderr)
        return 1

    presets = [x.strip() for x in args.presets.split(",") if x.strip()]
    body = json.dumps(
        {
            "owner_id": args.owner_id,
            "owner_type": args.owner_type,
            "preset_names": presets,
            "label": args.label,
        }
    ).encode()

    url = f"{args.base_url}/v1/admin/keys"
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {admin}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        detail = e.read().decode() or str(e)
        print(f"HTTP {e.code}: {detail}", file=sys.stderr)
        return 1
    except OSError as e:
        print(f"Request failed: {e}", file=sys.stderr)
        return 1

    print("Store raw_key securely — it is not shown again.\n")
    print(f"  raw_key:    {data.get('raw_key')}")
    print(f"  key_id:     {data.get('key_id')}")
    print(f"  owner_id:   {data.get('owner_id')}")
    print(f"  label:      {data.get('label')!r}")
    print(f"  expires_at: {data.get('expires_at')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
