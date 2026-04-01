#!/usr/bin/env python3
"""Load GTM CSV exports into GtmDB via the Python SDK.

Live writes require ``--execute`` (use ``--dry-run`` first). Progress lines show % per phase.

Creates **ProductAccount** nodes from ``pulse_account_id`` only (no separate ``Account`` / company nodes).
**Visitor** from ``master_visitor_id``; **contacts** and **deals** link to **ProductAccount**.

This script connects **directly to Neo4j** with the same settings as the API server
(``GTMDB_NEO4J_*``, ``GTMDB_ADMIN_KEY`` or agent key + ``GTMDB_KEY_STORE_URL``). It is
**not** an HTTP client: generic edges (``HAS_CAMPAIGN``, ``SIGNED_UP_FOR``, …) are not
exposed on REST yet.

Usage (from repo root, with ``pip install -e .`` and env configured)::

    uv run python scripts/import_csv.py --csv export.csv --mapping monday_crm_campaigns --dry-run
    uv run python scripts/import_csv.py --csv export.csv --mapping monday_crm_campaigns --execute
    uv run python scripts/import_csv.py --csv export.csv --mapping monday_crm_campaigns --execute --only-phase 5
    uv run python scripts/import_csv.py --csv export.csv --mapping monday_crm_campaigns --execute --only-phase 7

``--only-phase 5`` runs only the batched ProductAccount MERGE (same CSV filters). Phase 2
Product nodes must already exist in the graph (names must match Phase 2 rules).

``--only-phase 6`` runs only phases 6–8 (subscription events, visitors, paying contacts/deals).
Requires ProductAccounts from phase 5, Products, and Campaigns from a prior full import (1–4).

``--only-phase 7`` runs only phase 7 (batched visitors + links). Same graph prerequisites as 6,
but does not create subscription events or paying contacts/deals.

Environment:

- ``GTMDB_API_KEY`` or ``GTMDB_ADMIN_KEY`` — required non-empty API key for :func:`connect_gtmdb`.
- Standard ``GTMDB_NEO4J_URI``, user, password, optional key store — same as ``gtmdb serve``.

Source IDs from the CSV are used only for in-memory deduplication; persisted nodes get server UUIDs.

**ProductAccount:** Phase 5 uses MERGE on ``(tenant_id, external_id)`` (pulse id). If you previously
ran imports that CREATE-d duplicate ProductAccounts for the same pulse, remove or merge the extras
before ``connect`` / schema bootstrap — the composite uniqueness constraint on
``(tenant_id, external_id)`` will otherwise fail to apply.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import os
import sys
from pathlib import Path
from typing import Any

# Repo root + src on path when run as ``python scripts/import_csv.py``
_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
for p in (_ROOT, _SRC):
    if p.is_dir() and str(p) not in sys.path:
        sys.path.insert(0, str(p))


def _load_mapping(module_name: str) -> Any:
    """Load ``scripts.mappings.<name>`` or a dotted path under ``mappings``."""
    base = "scripts.mappings"
    if module_name.startswith("scripts.mappings."):
        full = module_name
    elif "." in module_name:
        full = module_name
    else:
        full = f"{base}.{module_name}"
    return importlib.import_module(full)


def _int_or_none(raw: str | None) -> int | None:
    if not raw:
        return None
    try:
        return int(float(raw))
    except ValueError:
        return None


def _content_title_from_url(url: str) -> str:
    slug = url.rstrip("/").split("/")[-1] or url
    return slug.replace("-", " ").replace("_", " ").title() or "Landing page"


def _product_stored_name_from_csv(csv_product_name: str) -> str:
    """Canonical Product.name for a CSV product string (must match Phase 2 create)."""
    name = csv_product_name.strip()
    return name.upper() if len(name) <= 8 else name.title()


_PHASE_TOTAL = 8


def _log_phase_progress(
    phase_num: int,
    label: str,
    current: int,
    total: int,
) -> None:
    if total <= 0:
        return
    throttle = max(1, total // 25) if total > 25 else 1
    if current not in (1, total) and current % throttle != 0:
        return
    pct = 100.0 * current / total
    print(
        f"[import phase {phase_num}/{_PHASE_TOTAL}] {label}  {current}/{total}  ({pct:.1f}%)",
        flush=True,
    )


def _dry_run_subscription_estimates(
    filtered: list[dict[str, str | None]], mapping_mod: Any
) -> tuple[int, int, int, int]:
    if hasattr(mapping_mod, "rows_by_pulse"):
        by_p = mapping_mod.rows_by_pulse(filtered)
        signup = len(by_p)
        purchase = sum(
            1 for rows in by_p.values() if any(mapping_mod.is_paying_row(r) for r in rows)
        )
        churn = sum(
            1
            for rows in by_p.values()
            if any((r.get("first_churn_date") or "").strip() for r in rows)
        )
        return signup, purchase, churn, signup + purchase + churn
    pulses = {r.get("pulse_account_id") for r in filtered if r.get("pulse_account_id")}
    paying = sum(
        1
        for p in pulses
        if any(
            mapping_mod.is_paying_row(r)
            for r in filtered
            if r.get("pulse_account_id") == p
        )
    )
    churn = sum(
        1
        for p in pulses
        if any(
            (r.get("first_churn_date") or "").strip()
            for r in filtered
            if r.get("pulse_account_id") == p
        )
    )
    s = len(pulses)
    return s, paying, churn, s + paying + churn


def _collect_phase6_subscription_event_rows(
    mapping_mod: Any,
    filtered: list[dict[str, str | None]],
    product_account_by_pulse: dict[str, str],
    product_by_name: dict[str, str],
) -> list[dict[str, Any]]:
    """Build rows for :meth:`SubscriptionEventsAPI.create_import_batch`."""
    pulse_keys = sorted(
        {r.get("pulse_account_id") for r in filtered if r.get("pulse_account_id")}
    )
    if hasattr(mapping_mod, "rows_by_pulse"):
        by_pulse = mapping_mod.rows_by_pulse(filtered)
    else:
        by_pulse = {
            p: [r for r in filtered if r.get("pulse_account_id") == p] for p in pulse_keys
        }
    pulse_list = sorted(by_pulse.keys())
    out: list[dict[str, Any]] = []
    pa_reason = "Lifecycle event for this product account"
    prod_reason = "Event applies to this product line"
    for pulse in pulse_list:
        rows_p = by_pulse[pulse]
        pa_id = product_account_by_pulse[pulse]
        prod_name = rows_p[0].get("product")
        prod_id = product_by_name.get(prod_name) if prod_name else None

        installs = [r.get("install_date") for r in rows_p if r.get("install_date")]
        if installs:
            oc = min(installs)
            if oc and str(oc).strip():
                extra_sn: dict[str, Any] = {}
                if prod_name:
                    extra_sn["product_name"] = prod_name
                out.append(
                    {
                        "pa_id": pa_id,
                        "event_type": "signup",
                        "occurred_at": str(oc).strip(),
                        "product_id": prod_id,
                        "extra_props": extra_sn,
                        "created_reasoning": "CSV import: signup from install_date",
                        "pa_link_reasoning": pa_reason,
                        "prod_link_reasoning": prod_reason,
                    }
                )

        pay_rows = [r for r in rows_p if mapping_mod.is_paying_row(r)]
        if pay_rows:
            pr = pay_rows[0]
            occurred = pr.get("first_product_arr_date") or pr.get("install_date")
            if occurred and str(occurred).strip():
                extra_pu: dict[str, Any] = {}
                if prod_name:
                    extra_pu["product_name"] = prod_name
                pt = pr.get("first_plan_tier")
                if pt:
                    extra_pu["plan_tier"] = pt
                pp = pr.get("first_plan_period")
                if pp:
                    extra_pu["plan_period"] = pp
                amt = mapping_mod.parse_amount(pr)
                if amt is not None:
                    extra_pu["arr"] = amt
                dsu = _int_or_none(pr.get("days_to_pay"))
                if dsu is not None:
                    extra_pu["days_from_signup"] = dsu
                out.append(
                    {
                        "pa_id": pa_id,
                        "event_type": "purchase",
                        "occurred_at": str(occurred).strip(),
                        "product_id": prod_id,
                        "extra_props": extra_pu,
                        "created_reasoning": "CSV import: paying account",
                        "pa_link_reasoning": pa_reason,
                        "prod_link_reasoning": prod_reason,
                    }
                )

        churn_row = next(
            (r for r in rows_p if (r.get("first_churn_date") or "").strip()), None
        )
        if churn_row:
            cd = churn_row.get("first_churn_date")
            if cd and str(cd).strip():
                extra_ch: dict[str, Any] = {}
                if prod_name:
                    extra_ch["product_name"] = prod_name
                out.append(
                    {
                        "pa_id": pa_id,
                        "event_type": "churn",
                        "occurred_at": str(cd).strip(),
                        "product_id": prod_id,
                        "extra_props": extra_ch,
                        "created_reasoning": "CSV import: churn date from export",
                        "pa_link_reasoning": pa_reason,
                        "prod_link_reasoning": prod_reason,
                    }
                )
    return out


def _aggregate_phase7_visitor_specs(
    visitor_rows: list[dict[str, str | None]],
) -> list[dict[str, Any]]:
    """One spec per distinct ``master_visitor_id`` (earliest install_date, first channel)."""
    by_vid: dict[str, dict[str, Any]] = {}
    for row in visitor_rows:
        vid = (row.get("master_visitor_id") or "").strip()
        if not vid:
            continue
        if vid not in by_vid:
            by_vid[vid] = {"dates": [], "channels": []}
        inst = row.get("install_date")
        if inst and str(inst).strip():
            by_vid[vid]["dates"].append(str(inst).strip())
        ch = row.get("grouped_channel")
        if ch and str(ch).strip():
            by_vid[vid]["channels"].append(str(ch).strip())
    out: list[dict[str, Any]] = []
    for vid, d in by_vid.items():
        dates: list[str] = d["dates"]
        chans: list[str] = d["channels"]
        fs = min(dates) if dates else None
        sc = chans[0] if chans else None
        out.append(
            {
                "visitor_id": vid,
                "source_channel": sc,
                "first_seen_at": fs,
                "created_reasoning": "CSV import: Visitor from master_visitor_id",
            }
        )
    return sorted(out, key=lambda x: x["visitor_id"])


def _collect_phase7_edges(
    visitor_rows: list[dict[str, str | None]],
    mapping_mod: Any,
    product_account_by_pulse: dict[str, str],
    product_by_name: dict[str, str],
    campaign_by_key: dict[tuple[str, str], str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    seen_sp: set[tuple[str, str]] = set()
    seen_pr: set[tuple[str, str]] = set()
    seen_tc: set[tuple[str, str]] = set()
    eas: list[dict[str, Any]] = []
    efor: list[dict[str, Any]] = []
    etouch: list[dict[str, Any]] = []
    for row in visitor_rows:
        vid = (row.get("master_visitor_id") or "").strip()
        pulse = row.get("pulse_account_id")
        if not vid or not pulse:
            continue
        pa_uuid = product_account_by_pulse.get(pulse)
        if not pa_uuid:
            continue
        k = (vid, pa_uuid)
        if k not in seen_sp:
            seen_sp.add(k)
            eas.append(
                {
                    "vid_ext": vid,
                    "pa_id": pa_uuid,
                    "reasoning": "Visitor attributed to this ProductAccount (pulse)",
                }
            )
        prod = row.get("product")
        if prod and prod in product_by_name:
            pid = product_by_name[prod]
            kp = (vid, pid)
            if kp not in seen_pr:
                seen_pr.add(kp)
                efor.append(
                    {
                        "vid_ext": vid,
                        "product_id": pid,
                        "reasoning": "Visitor signed up for this product",
                    }
                )
        ck = mapping_mod.campaign_dedupe_key(row)
        if ck and ck in campaign_by_key:
            cid = campaign_by_key[ck]
            kt = (vid, cid)
            if kt not in seen_tc:
                seen_tc.add(kt)
                etouch.append(
                    {
                        "vid_ext": vid,
                        "camp_id": cid,
                        "reasoning": "Attribution from marketing export",
                    }
                )
    return eas, efor, etouch


async def _resolve_product_ids_from_graph(
    db: Any,
    scope: Any,
    filtered: list[dict[str, str | None]],
) -> dict[str, str]:
    """Map CSV ``product`` cell -> Product node id (from an earlier Phase 2 import)."""
    prod_names = sorted({r.get("product") for r in filtered if r.get("product")})
    out: dict[str, str] = {}
    for raw in prod_names:
        key = str(raw).strip()
        stored = _product_stored_name_from_csv(key)
        ents = await db.products.list(scope, name=stored, limit=5)
        if len(ents) >= 1:
            if len(ents) > 1:
                print(
                    f"[import] WARNING: {len(ents)} Product nodes for name={stored!r} — "
                    f"using newest (id={ents[0].id}) for CSV product={key!r}",
                    flush=True,
                )
            out[key] = ents[0].id
        else:
            print(
                f"[import] WARNING: no Product in graph for CSV product={key!r} "
                f"(expected node name={stored!r}); FOR_PRODUCT for those rows skipped",
                flush=True,
            )
    return out


def _campaign_rows_from_filtered(
    filtered: list[dict[str, str | None]], mapping_mod: Any
) -> dict[tuple[str, str], dict[str, str | None]]:
    campaign_rows: dict[tuple[str, str], dict[str, str | None]] = {}
    for row in filtered:
        key = mapping_mod.campaign_dedupe_key(row)
        if key and key not in campaign_rows:
            campaign_rows[key] = row
    return campaign_rows


async def _resolve_campaign_by_key_from_graph(
    db: Any,
    scope: Any,
    mapping_mod: Any,
    filtered: list[dict[str, str | None]],
) -> dict[tuple[str, str], str]:
    """Match Phase 4 campaigns by ``(channel, display name)``."""
    campaign_rows = _campaign_rows_from_filtered(filtered, mapping_mod)
    out: dict[tuple[str, str], str] = {}
    for key, sample in campaign_rows.items():
        display = mapping_mod.campaign_display_name(sample)
        if not display:
            continue
        _cat, ch_name = key
        ents = await db.campaigns.list(scope, channel=ch_name, name=display, limit=10)
        if len(ents) >= 1:
            if len(ents) > 1:
                print(
                    f"[import] WARNING: {len(ents)} Campaign nodes for "
                    f"channel={ch_name!r} name={display!r} — using {ents[0].id}",
                    flush=True,
                )
            out[key] = ents[0].id
        else:
            print(
                f"[import] WARNING: no Campaign in graph for channel={ch_name!r} "
                f"name={display!r} — TOUCHED/INFLUENCED skipped for this key",
                flush=True,
            )
    return out


def _build_pa_import_rows(
    filtered: list[dict[str, str | None]],
    mapping_mod: Any,
    product_by_name: dict[str, str],
) -> tuple[list[str], list[dict[str, Any]]]:
    pulse_keys = sorted(
        {r.get("pulse_account_id") for r in filtered if r.get("pulse_account_id")}
    )
    pa_rows: list[dict[str, Any]] = []
    for pulse in pulse_keys:
        sample = next(r for r in filtered if r.get("pulse_account_id") == pulse)
        paying = mapping_mod.is_paying_row(sample)
        row: dict[str, Any] = {
            "external_id": pulse,
            "name": pulse,
            "status": "paying" if paying else "free",
            "reasoning": (
                "CSV import: ProductAccount (external_id = pulse_account_id from export)"
            ),
        }
        prod = sample.get("product")
        if prod and prod in product_by_name:
            row["product_id"] = product_by_name[prod]
            row["product_reasoning"] = "Product workspace is for this product line"
        pa_rows.append(row)
    return pulse_keys, pa_rows


def _for_product_edge_rows(pa_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "external_id": r["external_id"],
            "product_id": r["product_id"],
            "product_reasoning": r.get("product_reasoning", ""),
        }
        for r in pa_rows
        if r.get("product_id")
    ]


async def _resolve_product_account_ids_bulk(
    db: Any,
    scope: Any,
    pulse_keys: list[str],
    *,
    batch_size: int = 500,
) -> dict[str, str]:
    """Map ``pulse_account_id`` (external_id) -> ProductAccount node id."""
    out: dict[str, str] = {}
    missing: list[str] = []
    bs = max(1, batch_size)
    for i in range(0, len(pulse_keys), bs):
        chunk = pulse_keys[i : i + bs]
        recs = await db.execute_cypher(
            scope,
            """
            UNWIND $eids AS eid
            OPTIONAL MATCH (pa:ProductAccount {tenant_id: $tenant_id, external_id: eid})
            RETURN eid AS external_id, pa.id AS id
            """,
            {"eids": chunk},
        )
        for r in recs:
            eid = r.get("external_id")
            nid = r.get("id")
            if eid is not None and nid is not None:
                out[str(eid)] = str(nid)
            elif eid is not None:
                missing.append(str(eid))
    if missing:
        raise ValueError(
            f"{len(missing)} pulse ids have no ProductAccount in graph "
            f"(run phase 5 first). Examples: {missing[:5]!r}"
        )
    return out


async def _phase5_merge_product_accounts(
    db: Any,
    scope: Any,
    mapping_mod: Any,
    actor_id: str,
    filtered: list[dict[str, str | None]],
    product_by_name: dict[str, str],
) -> dict[str, str]:
    _pulse_keys, pa_rows = _build_pa_import_rows(filtered, mapping_mod, product_by_name)

    def _pa_batch_progress(cur: int, tot: int) -> None:
        _log_phase_progress(5, "product accounts (batches)", cur, tot)

    result = await db.product_accounts.merge_import_batch(
        scope,
        actor_id=actor_id,
        rows=pa_rows,
        batch_size=500,
        after_chunk=_pa_batch_progress,
    )
    fp_rows = _for_product_edge_rows(pa_rows)
    if fp_rows:

        def _fp_progress(cur: int, tot: int) -> None:
            _log_phase_progress(5, "FOR_PRODUCT (ensure)", cur, tot)

        await db.product_accounts.merge_for_product_edges_only(
            scope,
            rows=fp_rows,
            batch_size=500,
            after_chunk=_fp_progress,
        )
    return result


async def _run_phase7_visitors_batched(
    db: Any,
    scope: Any,
    mapping_mod: Any,
    actor_id: str,
    filtered: list[dict[str, str | None]],
    product_account_by_pulse: dict[str, str],
    product_by_name: dict[str, str],
    campaign_by_key: dict[tuple[str, str], str],
) -> dict[str, str]:
    """Phase 7 only: batched Visitor MERGE + ``SIGNED_UP_AS`` / ``SIGNED_UP_FOR`` / ``TOUCHED``."""
    visitor_rows = [
        r
        for r in filtered
        if r.get("pulse_account_id")
        and r.get("master_visitor_id")
        and r.get("pulse_account_id") in product_account_by_pulse
    ]
    visitor_id_to_node: dict[str, str] = {}
    if visitor_rows:
        vspecs = _aggregate_phase7_visitor_specs(visitor_rows)
        eas, efor, etouch = _collect_phase7_edges(
            visitor_rows,
            mapping_mod,
            product_account_by_pulse,
            product_by_name,
            campaign_by_key,
        )

        def _ph7_batch_progress(cur: int, tot: int) -> None:
            _log_phase_progress(7, "visitors + links (batched)", cur, tot)

        visitor_id_to_node = await db.visitors.import_phase7_batch(
            scope,
            actor_id=actor_id,
            visitor_specs=vspecs,
            edges_signed_as=eas,
            edges_signed_for=efor,
            edges_touched=etouch,
            batch_size=500,
            after_chunk=_ph7_batch_progress,
        )
    else:
        _log_phase_progress(7, "visitors + links (batched)", 1, 1)
    return visitor_id_to_node


async def _run_phases_6_7_8(
    db: Any,
    scope: Any,
    mapping_mod: Any,
    actor_id: str,
    filtered: list[dict[str, str | None]],
    product_account_by_pulse: dict[str, str],
    product_by_name: dict[str, str],
    campaign_by_key: dict[tuple[str, str], str],
) -> None:
    pulse_keys = sorted(
        {r.get("pulse_account_id") for r in filtered if r.get("pulse_account_id")}
    )
    # --- Phase 6: SubscriptionEvent (per pulse) ---
    by_pulse: dict[str, list[dict[str, str | None]]]
    if hasattr(mapping_mod, "rows_by_pulse"):
        by_pulse = mapping_mod.rows_by_pulse(filtered)
    else:
        by_pulse = {
            p: [r for r in filtered if r.get("pulse_account_id") == p] for p in pulse_keys
        }
    pulse_list = sorted(by_pulse.keys())
    sub_rows = _collect_phase6_subscription_event_rows(
        mapping_mod, filtered, product_account_by_pulse, product_by_name
    )
    if sub_rows:

        def _ph6_batch_progress(cur: int, tot: int) -> None:
            _log_phase_progress(6, "subscription events (batched)", cur, tot)

        await db.subscription_events.create_import_batch(
            scope,
            actor_id=actor_id,
            rows=sub_rows,
            batch_size=500,
            after_chunk=_ph6_batch_progress,
        )
    elif pulse_list:
        _log_phase_progress(6, "subscription events (batched)", 1, 1)

    visitor_id_to_node = await _run_phase7_visitors_batched(
        db,
        scope,
        mapping_mod,
        actor_id,
        filtered,
        product_account_by_pulse,
        product_by_name,
        campaign_by_key,
    )

    # --- Phase 8: paying -> contact + deal ---
    pay_eligible = [
        r
        for r in filtered
        if mapping_mod.is_paying_row(r)
        and r.get("pulse_account_id") in product_account_by_pulse
        and r.get("master_visitor_id")
        and visitor_id_to_node.get(r.get("master_visitor_id") or "")
    ]
    for j, row in enumerate(pay_eligible, start=1):
        pulse = row.get("pulse_account_id")
        visitor = row.get("master_visitor_id")
        assert pulse and visitor
        visitor_nid = visitor_id_to_node[visitor]
        pa_id = product_account_by_pulse[pulse]

        contact = await db.contacts.create(
            scope,
            actor_id=actor_id,
            title=mapping_mod.title_from_row(row),
            company_name=row.get("industry"),
            department=row.get("department"),
            reasoning="CSV import: converted contact",
        )
        await db.contacts.assign_to_account(
            scope,
            contact.id,
            pa_id,
            reasoning="Contact works at same ProductAccount (pulse) as visitor",
        )
        await db.relationships.create(
            scope,
            visitor_nid,
            "CONVERTED_TO",
            contact.id,
            reasoning="Visitor converted to paying contact",
        )

        amount = mapping_mod.parse_amount(row)
        close_d = row.get("first_product_arr_date")
        tier = row.get("first_plan_tier") or ""
        period = row.get("first_plan_period") or ""
        desc = " / ".join(x for x in (tier, period) if x) or None
        deal = await db.deals.create(
            scope,
            actor_id=actor_id,
            name="Product subscription",
            amount=amount,
            stage="closed_won",
            close_date=close_d,
            description=desc,
            reasoning="CSV import: first product ARR",
        )
        await db.deals.assign_to_account(
            scope, deal.id, pa_id, reasoning="Deal belongs to this ProductAccount (pulse)"
        )
        prod = row.get("product")
        if prod and prod in product_by_name:
            await db.relationships.create(
                scope,
                deal.id,
                "FOR_PRODUCT",
                product_by_name[prod],
                reasoning="Subscription is for this product",
            )
        await db.deals.add_contact(
            scope,
            deal.id,
            contact.id,
            reasoning="Primary buyer on subscription deal",
        )
        ck = mapping_mod.campaign_dedupe_key(row)
        if ck and ck in campaign_by_key:
            await db.relationships.create(
                scope,
                campaign_by_key[ck],
                "INFLUENCED",
                deal.id,
                reasoning="Campaign attributed to this conversion",
            )
        _log_phase_progress(8, "paying -> contact + deal", j, len(pay_eligible))


async def run_import(
    *,
    csv_path: str,
    mapping_mod: Any,
    actor_id: str,
    dry_run: bool,
    campaign_category: str | None,
    grouped_channel: str | None,
    api_key: str,
    only_phase: int | None = None,
) -> None:
    rows = mapping_mod.read_csv_rows(csv_path, delimiter=",", has_header=False)
    filtered: list[dict[str, str | None]] = []
    for row in rows:
        if not mapping_mod.row_passes_cli_filter(
            row,
            campaign_category=campaign_category,
            grouped_channel=grouped_channel,
        ):
            continue
        filtered.append(row)

    print(f"Rows read: {len(rows)}, after filter: {len(filtered)}")
    if dry_run:
        chans = set()
        camps = set()
        prods = set()
        contents = set()
        pulses = set()
        paying = 0
        for row in filtered:
            k = mapping_mod.campaign_dedupe_key(row)
            if k:
                camps.add(k)
            if row.get("grouped_channel"):
                chans.add(row["grouped_channel"])
            if row.get("product"):
                prods.add(row["product"])
            if row.get("marketing_landing_page"):
                contents.add(row["marketing_landing_page"])
            if row.get("pulse_account_id"):
                pulses.add(row["pulse_account_id"])
            if mapping_mod.is_paying_row(row):
                paying += 1
        su, pur, ch, ev_total = _dry_run_subscription_estimates(filtered, mapping_mod)
        leadish = sum(
            1 for r in filtered if r.get("master_visitor_id") and r.get("pulse_account_id")
        )
        print(
            f"[dry-run] channels={len(chans)} products={len(prods)} "
            f"content={len(contents)} campaigns={len(camps)} "
            f"product_accounts={len(pulses)} (no Account nodes) "
            f"paying_rows={paying}"
        )
        print(
            f"[dry-run] subscription_events: signup≈{su} purchase≈{pur} churn≈{ch} "
            f"(total nodes≈{ev_total})"
        )
        print(f"[dry-run] visitor_row_candidates≈{leadish}")
        if only_phase == 5:
            print(
                f"[dry-run] --only-phase 5: would MERGE {len(pulses)} ProductAccount nodes "
                f"(products in CSV={len(prods)}; FOR_PRODUCT needs matching Product nodes in graph).",
                flush=True,
            )
        if only_phase == 6:
            print(
                f"[dry-run] --only-phase 6: would run phases 6–8 only "
                f"(subscription events per pulse, ~{leadish} visitor rows, paying_rows={paying}). "
                f"Requires ProductAccounts (phase 5), Products, Campaigns in graph.",
                flush=True,
            )
        if only_phase == 7:
            print(
                f"[dry-run] --only-phase 7: would run phase 7 only "
                f"(batched visitors + links, ~{leadish} visitor row lines). "
                f"Requires ProductAccounts, Products, Campaigns in graph.",
                flush=True,
            )
        print("[dry-run] No DB writes. Live: add --execute + API key.")
        return

    from gtmdb import connect_gtmdb

    print("Connecting to GtmDB / Neo4j …", flush=True)
    db, scope = await connect_gtmdb(api_key=api_key)
    try:
        if only_phase == 6:
            print(
                "[import] --only-phase 6: running phases 6–8 only "
                "(subscription events, visitors, paying contacts/deals).",
                flush=True,
            )
            product_by_name = await _resolve_product_ids_from_graph(db, scope, filtered)
            pulse_keys, pa_rows = _build_pa_import_rows(
                filtered, mapping_mod, product_by_name
            )
            fp_rows = _for_product_edge_rows(pa_rows)
            if fp_rows:

                def _fp_only_progress(cur: int, tot: int) -> None:
                    _log_phase_progress(6, "FOR_PRODUCT (ensure)", cur, tot)

                await db.product_accounts.merge_for_product_edges_only(
                    scope,
                    rows=fp_rows,
                    batch_size=500,
                    after_chunk=_fp_only_progress,
                )
            product_account_by_pulse = await _resolve_product_account_ids_bulk(
                db, scope, pulse_keys
            )
            campaign_by_key = await _resolve_campaign_by_key_from_graph(
                db, scope, mapping_mod, filtered
            )
            await _run_phases_6_7_8(
                db,
                scope,
                mapping_mod,
                actor_id,
                filtered,
                product_account_by_pulse,
                product_by_name,
                campaign_by_key,
            )
            n_pulses = len(pulse_keys)
            print(
                f"[import] Phases 6–8 only finished OK "
                f"(rows={len(filtered)} product_accounts={n_pulses}).",
                flush=True,
            )
            return

        if only_phase == 7:
            print(
                "[import] --only-phase 7: running phase 7 only "
                "(batched visitors + links; skips phases 6 and 8).",
                flush=True,
            )
            product_by_name = await _resolve_product_ids_from_graph(db, scope, filtered)
            pulse_keys, pa_rows = _build_pa_import_rows(
                filtered, mapping_mod, product_by_name
            )
            fp_rows = _for_product_edge_rows(pa_rows)
            if fp_rows:

                def _fp7_prep(cur: int, tot: int) -> None:
                    _log_phase_progress(7, "FOR_PRODUCT (ensure)", cur, tot)

                await db.product_accounts.merge_for_product_edges_only(
                    scope,
                    rows=fp_rows,
                    batch_size=500,
                    after_chunk=_fp7_prep,
                )
            product_account_by_pulse = await _resolve_product_account_ids_bulk(
                db, scope, pulse_keys
            )
            campaign_by_key = await _resolve_campaign_by_key_from_graph(
                db, scope, mapping_mod, filtered
            )
            await _run_phase7_visitors_batched(
                db,
                scope,
                mapping_mod,
                actor_id,
                filtered,
                product_account_by_pulse,
                product_by_name,
                campaign_by_key,
            )
            n_pulses = len(pulse_keys)
            print(
                f"[import] Phase 7 only finished OK "
                f"(rows={len(filtered)} product_accounts={n_pulses}).",
                flush=True,
            )
            return

        if only_phase == 5:
            print(
                "[import] --only-phase 5: running ProductAccount MERGE only "
                "(skips phases 1–4 and 6–8).",
                flush=True,
            )
            product_by_name = await _resolve_product_ids_from_graph(db, scope, filtered)
            n_pulses = len(
                {r.get("pulse_account_id") for r in filtered if r.get("pulse_account_id")}
            )
            await _phase5_merge_product_accounts(
                db, scope, mapping_mod, actor_id, filtered, product_by_name
            )
            print(
                f"[import] Phase 5 only finished OK — merged/up to {n_pulses} product_accounts.",
                flush=True,
            )
            return

        # --- Phase 1: channels ---
        ch_names = sorted({r.get("grouped_channel") for r in filtered if r.get("grouped_channel")})
        channel_by_name: dict[str, str] = {}
        for i, name in enumerate(ch_names, start=1):
            ent = await db.channels.create(
                scope,
                actor_id=actor_id,
                name=name,
                channel_type=mapping_mod.channel_type(name),
                status="active",
                reasoning="CSV import: acquisition channel",
            )
            channel_by_name[name] = ent.id
            _log_phase_progress(1, "channels", i, len(ch_names))

        # --- Phase 2: products ---
        prod_names = sorted({r.get("product") for r in filtered if r.get("product")})
        product_by_name: dict[str, str] = {}
        for i, name in enumerate(prod_names, start=1):
            ent = await db.products.create(
                scope,
                actor_id=actor_id,
                name=_product_stored_name_from_csv(name),
                product_type="core",
                status="active",
                reasoning="CSV import: product line",
            )
            product_by_name[name] = ent.id
            _log_phase_progress(2, "products", i, len(prod_names))

        # --- Phase 3: content (landing pages) ---
        urls = sorted(
            {r.get("marketing_landing_page") for r in filtered if r.get("marketing_landing_page")}
        )
        content_by_url: dict[str, str] = {}
        for i, url in enumerate(urls, start=1):
            ent = await db.content.create(
                scope,
                actor_id=actor_id,
                name=_content_title_from_url(url),
                url=url,
                content_type="landing_page",
                status="published",
                reasoning="CSV import: marketing landing page",
            )
            content_by_url[url] = ent.id
            _log_phase_progress(3, "content (landing pages)", i, len(urls))

        # --- Phase 4: campaigns (dedupe by category + channel, clean name) ---
        campaign_rows: dict[tuple[str, str], dict[str, str | None]] = {}
        for row in filtered:
            key = mapping_mod.campaign_dedupe_key(row)
            if key and key not in campaign_rows:
                campaign_rows[key] = row

        campaign_items = list(campaign_rows.items())
        campaign_by_key: dict[tuple[str, str], str] = {}
        for i, (key, sample) in enumerate(campaign_items, start=1):
            display = mapping_mod.campaign_display_name(sample)
            if not display:
                continue
            cat, ch_name = key
            desc_parts = [cat]
            if sample.get("marketing_source"):
                desc_parts.append(sample["marketing_source"])
            ent = await db.campaigns.create(
                scope,
                actor_id=actor_id,
                name=display,
                status="active",
                channel=ch_name,
                description=" | ".join(desc_parts),
                reasoning="CSV import: deduped campaign (category + channel)",
            )
            campaign_by_key[key] = ent.id
            ch_id = channel_by_name.get(ch_name)
            if ch_id:
                await db.relationships.create(
                    scope,
                    ch_id,
                    "HAS_CAMPAIGN",
                    ent.id,
                    reasoning="Channel groups this campaign",
                )
            lp = sample.get("marketing_landing_page")
            if lp and lp in content_by_url:
                await db.relationships.create(
                    scope,
                    ent.id,
                    "HAS_CONTENT",
                    content_by_url[lp],
                    reasoning="Campaign drives traffic to this landing page",
                )
            _log_phase_progress(4, "campaigns", i, len(campaign_items))

        # --- Phase 5: ProductAccount (pulse = external_id; MERGE + batched UNWIND) ---
        pulse_keys = sorted({r.get("pulse_account_id") for r in filtered if r.get("pulse_account_id")})
        product_account_by_pulse = await _phase5_merge_product_accounts(
            db, scope, mapping_mod, actor_id, filtered, product_by_name
        )

        # --- Phases 6–8: subscriptions, visitors, paying contacts/deals ---
        await _run_phases_6_7_8(
            db,
            scope,
            mapping_mod,
            actor_id,
            filtered,
            product_account_by_pulse,
            product_by_name,
            campaign_by_key,
        )

        print(
            f"[import] finished OK — phases 1–{_PHASE_TOTAL} "
            f"(rows={len(filtered)} product_accounts={len(pulse_keys)}).",
            flush=True,
        )
    finally:
        await db.close()


def main() -> None:
    try:
        from dotenv import load_dotenv

        load_dotenv(_ROOT / ".env")
    except ImportError:
        pass

    parser = argparse.ArgumentParser(description="Import GTM CSV into GtmDB (SDK).")
    parser.add_argument("--csv", required=True, help="Path to CSV file")
    parser.add_argument(
        "--mapping",
        default="monday_crm_campaigns",
        help="Mapping module under scripts.mappings (default: monday_crm_campaigns)",
    )
    parser.add_argument(
        "--actor-id",
        default="csv-importer",
        help="actor_id for creates and updates",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse CSV and print counts only; no database writes",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Required for live import (writes Neo4j)",
    )
    parser.add_argument(
        "--only-phase",
        type=int,
        default=None,
        metavar="N",
        help="5 = ProductAccount only; 6 = phases 6–8; 7 = visitors only (needs PA + Products + Campaigns)",
    )
    parser.add_argument(
        "--campaign-category",
        default=None,
        help="If set, only import rows with this campaign_category value",
    )
    parser.add_argument(
        "--grouped-channel",
        default=None,
        help="If set, only import rows with this grouped_channel value",
    )
    args = parser.parse_args()

    if not args.dry_run and not args.execute:
        print(
            "Refusing live import: use --dry-run first, then --execute.",
            file=sys.stderr,
        )
        sys.exit(2)

    api_key = (
        (os.environ.get("GTMDB_API_KEY") or "").strip()
        or (os.environ.get("GTMDB_ADMIN_KEY") or "").strip()
    )
    if not api_key and not args.dry_run:
        try:
            from gtmdb.config import GtmdbSettings

            api_key = (GtmdbSettings().admin_key or "").strip()
        except Exception:
            api_key = ""
    if not args.dry_run and not api_key:
        print(
            "Set GTMDB_API_KEY or GTMDB_ADMIN_KEY (env or .env in repo root) for live import.",
            file=sys.stderr,
        )
        sys.exit(1)

    if args.only_phase is not None and args.only_phase not in (5, 6, 7):
        print(
            "error: --only-phase: supported values are 5, 6, or 7.",
            file=sys.stderr,
        )
        sys.exit(2)

    mapping_mod = _load_mapping(args.mapping)
    asyncio.run(
        run_import(
            csv_path=args.csv,
            mapping_mod=mapping_mod,
            actor_id=args.actor_id,
            dry_run=args.dry_run,
            campaign_category=args.campaign_category,
            grouped_channel=args.grouped_channel,
            api_key=api_key,
            only_phase=args.only_phase,
        )
    )


if __name__ == "__main__":
    main()
