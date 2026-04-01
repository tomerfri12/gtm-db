#!/usr/bin/env python3
"""Unified funnel import: Monday CRM CSV + visitor/campaign export + leads export.

Idempotent batch MERGE into Neo4j (same connection pattern as ``import_csv.py``).
Phases 1–8 write funnel data; phase 9 MERGEs ``SubscriptionEvent`` from Monday
(signup / purchase / churn) linked to ``ProductAccount``, ``Visitor``, and ``Lead``;
phase 10 runs verification Cypher.

Usage::

    uv run python scripts/import_funnel.py \\
      --monday monday.csv --visitors q1.csv --leads q2.csv --dry-run
    uv run python scripts/import_funnel.py \\
      --monday monday.csv --visitors q1.csv --leads q2.csv --execute

Resume a single phase::

    uv run python scripts/import_funnel.py ... --execute --only-phase 5
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
for p in (_ROOT, _SRC):
    if p.is_dir() and str(p) not in sys.path:
        sys.path.insert(0, str(p))

_PHASE_TOTAL = 10


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _log_phase_progress(phase_num: int, label: str, current: int, total: int) -> None:
    if total <= 0:
        return
    throttle = max(1, total // 25) if total > 25 else 1
    if current not in (1, total) and current % throttle != 0:
        return
    pct = 100.0 * current / total
    print(
        f"[funnel phase {phase_num}/{_PHASE_TOTAL}] {label}  {current}/{total}  ({pct:.1f}%)",
        flush=True,
    )


def _chunks(xs: list[Any], n: int) -> list[list[Any]]:
    return [xs[i : i + n] for i in range(0, len(xs), n)]


def _product_stored_name(csv_product_name: str) -> str:
    name = csv_product_name.strip()
    return name.upper() if len(name) <= 8 else name.title()


def _normalize_landing_url(raw: str | None) -> str:
    if not raw:
        return ""
    s = raw.strip()
    if not s:
        return ""
    if s.startswith("http://") or s.startswith("https://"):
        return s
    return "https://" + s.lstrip("/")


def _channel_type(channel_name: str) -> str:
    n = channel_name.upper()
    if n in (
        "SEM",
        "YOUTUBE",
        "FACEBOOK",
        "LINKEDIN",
        "MB_PUSH",
        "MB_PULL",
        "DSPS_DISPLAY",
        "DSPS_VIDEO",
    ):
        return "paid"
    if n in ("ORGANIC_SEO", "BRANDED_SEARCH"):
        return "organic"
    if n.startswith("MB_") or n == "MB":
        return "marketplace"
    return "other"


def _s(v: str | None) -> str | None:
    if v is None:
        return None
    t = v.strip()
    return t if t else None


def _int_or_none(raw: str | None) -> int | None:
    if not raw:
        return None
    try:
        return int(float(raw))
    except ValueError:
        return None


def _bit(v: str | None) -> bool | None:
    if v is None or (isinstance(v, str) and not v.strip()):
        return None
    return str(v).strip() == "1"


def _content_title_from_url(url: str) -> str:
    slug = url.rstrip("/").split("/")[-1] or url
    return slug.replace("-", " ").replace("_", " ").title() or "Landing page"


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------


def _load_monday(path: str) -> list[dict[str, str | None]]:
    from scripts.mappings.monday_crm_campaigns import read_csv_rows

    return read_csv_rows(path, delimiter=",", has_header=False)


def _load_q1(path: str) -> list[dict[str, str]]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _load_q2(path: str) -> list[dict[str, str]]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ---------------------------------------------------------------------------
# Phase builders (pure)
# ---------------------------------------------------------------------------


def _build_channel_product_rows(
    monday: list[dict[str, str | None]], q1: list[dict[str, str]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    chans: dict[str, str] = {}
    for r in monday:
        n = _s(r.get("grouped_channel"))
        if n:
            chans[n] = _channel_type(n)
    for r in q1:
        n = _s(r.get("GROUPED_CHANNEL"))
        if n:
            chans[n] = _channel_type(n)
    channel_rows = [{"name": k, "channel_type": v} for k, v in sorted(chans.items())]

    products: set[str] = set()
    for r in monday:
        p = _s(r.get("product"))
        if p:
            products.add(_product_stored_name(p))
    product_rows = [{"name": n, "status": "active"} for n in sorted(products)]
    return channel_rows, product_rows


def _build_content_rows(
    monday: list[dict[str, str | None]], q1: list[dict[str, str]]
) -> list[dict[str, Any]]:
    urls: dict[str, str] = {}
    for r in monday:
        u = _normalize_landing_url(r.get("marketing_landing_page") or "")
        if u:
            urls[u] = _content_title_from_url(u)
    for r in q1:
        u = _normalize_landing_url(r.get("LANDING_PAGE") or "")
        if u:
            urls[u] = _content_title_from_url(u)
    return [
        {"url": u, "name": urls[u], "content_type": "landing_page", "status": "active"}
        for u in sorted(urls.keys())
    ]


def _build_campaign_rows(
    monday: list[dict[str, str | None]], q1: list[dict[str, str]]
) -> list[dict[str, Any]]:
    """One row per distinct campaign name (full execution string)."""
    by_name: dict[str, dict[str, Any]] = {}
    for r in monday:
        name = _s(r.get("marketing_campaign"))
        if not name:
            continue
        entry = by_name.setdefault(
            name,
            {
                "name": name,
                "campaign_category": None,
                "channel_name": None,
                "landing_url": None,
                "marketing_source": None,
            },
        )
        entry["campaign_category"] = entry["campaign_category"] or _s(
            r.get("campaign_category")
        )
        entry["channel_name"] = entry["channel_name"] or _s(r.get("grouped_channel"))
        lu = _normalize_landing_url(r.get("marketing_landing_page") or "")
        if lu:
            entry["landing_url"] = entry["landing_url"] or lu
        entry["marketing_source"] = entry["marketing_source"] or _s(
            r.get("marketing_source")
        )
    for r in q1:
        name = _s(r.get("CAMPAIGN"))
        if not name:
            continue
        entry = by_name.setdefault(
            name,
            {
                "name": name,
                "campaign_category": None,
                "channel_name": None,
                "landing_url": None,
                "marketing_source": None,
            },
        )
        entry["campaign_category"] = entry["campaign_category"] or _s(
            r.get("CAMPAIGN_CATEGORY")
        )
        entry["channel_name"] = entry["channel_name"] or _s(r.get("GROUPED_CHANNEL"))
        lu = _normalize_landing_url(r.get("LANDING_PAGE") or "")
        if lu:
            entry["landing_url"] = entry["landing_url"] or lu
    return list(by_name.values())


def _product_account_props_from_monday(r: dict[str, str | None]) -> dict[str, Any]:
    from scripts.mappings.monday_crm_campaigns import account_name_from_row

    paying = r.get("is_paying") == "1"
    props: dict[str, Any] = {
        "name": account_name_from_row(r),
        "status": "paying" if paying else "free",
        "region": _s(r.get("region")),
        "country": _s(r.get("country")),
        "industry": _s(r.get("industry")),
        "company_size_group": _s(r.get("company_size_group")),
        "company_size_num": _s(r.get("company_size_num")),
        "is_paying": r.get("is_paying"),
        "install_date": _s(r.get("install_date")),
        "first_product_install_date": _s(r.get("first_product_install_date")),
        "is_first_product": _s(r.get("is_first_product")),
        "first_account_channel": _s(r.get("first_account_channel")),
        "is_cross_sell": _s(r.get("is_cross_sell")),
        "first_plan_tier": _s(r.get("first_plan_tier")),
        "first_plan_period": _s(r.get("first_plan_period")),
        "first_product_arr": _s(r.get("first_product_arr")),
        "first_product_arr_date": _s(r.get("first_product_arr_date")),
        "first_account_arr_date": _s(r.get("first_account_arr_date")),
        "first_churn_date": _s(r.get("first_churn_date")),
        "days_to_pay": _s(r.get("days_to_pay")),
        "survey_sector": _s(r.get("survey_sector")),
        "survey_sub_sector": _s(r.get("survey_sub_sector")),
        "dep0_predicted_arr": _s(r.get("dep0_predicted_arr")),
        "extra_predicted": _s(r.get("extra_predicted")),
    }
    return {k: v for k, v in props.items() if v is not None}


def _build_pulse_rows(
    monday: list[dict[str, str | None]],
    q1: list[dict[str, str]],
    q2: list[dict[str, str]],
    product_by_stored_name: dict[str, str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """ProductAccount upsert rows + FOR_PRODUCT edge rows."""
    by_pulse: dict[str, dict[str, Any]] = {}
    for r in monday:
        pid = _s(r.get("pulse_account_id"))
        if not pid:
            continue
        props = _product_account_props_from_monday(r)
        props["external_id"] = pid
        prod_raw = _s(r.get("product"))
        props["product_name"] = _product_stored_name(prod_raw) if prod_raw else None
        by_pulse[pid] = props
    for row in q1:
        pid = _s(row.get("PULSE_ACCOUNT_ID"))
        if not pid:
            continue
        if pid not in by_pulse:
            by_pulse[pid] = {
                "external_id": pid,
                "name": f"Workspace {pid}",
                "status": "unknown",
            }
    for row in q2:
        pid = _s(row.get("PULSE_ACCOUNT_ID"))
        if not pid:
            continue
        if pid not in by_pulse:
            by_pulse[pid] = {
                "external_id": pid,
                "name": f"Workspace {pid}",
                "status": "unknown",
            }

    pa_rows: list[dict[str, Any]] = []
    fp_rows: list[dict[str, Any]] = []
    for pid, props in sorted(by_pulse.items()):
        pname = props.pop("product_name", None)
        pa_props = {k: v for k, v in props.items() if k != "external_id" and v is not None}
        pa_rows.append({"external_id": pid, "pa_props": pa_props})
        if pname and pname in product_by_stored_name:
            fp_rows.append(
                {
                    "external_id": pid,
                    "product_id": product_by_stored_name[pname],
                    "product_reasoning": "FOR_PRODUCT from Monday product column",
                }
            )
    return pa_rows, fp_rows


def _build_visitor_rows(
    monday: list[dict[str, str | None]], q1: list[dict[str, str]]
) -> list[dict[str, Any]]:
    by_vid: dict[str, dict[str, Any]] = {}
    for row in q1:
        vid = _s(row.get("VISITOR_ID"))
        if not vid:
            continue
        by_vid[vid] = {
            "visitor_id": vid,
            "source_channel": _s(row.get("GROUPED_CHANNEL")),
            "first_seen_at": _s(row.get("CREATED_AT")),
            "visitor_row_type": _s(row.get("TYPE")),
        }
    for r in monday:
        vid = _s(r.get("master_visitor_id"))
        if not vid:
            continue
        m = {
            "device": _s(r.get("device")),
            "first_user_platform_language": _s(r.get("first_user_platform_language")),
            "user_goal": _s(r.get("user_goal")),
            "signup_flow": _s(r.get("signup_flow")),
            "raw_signup_flow": _s(r.get("raw_signup_flow")),
            "signup_use_case": _s(r.get("signup_use_case")),
            "signup_cluster": _s(r.get("signup_cluster")),
            "product_intent": _s(r.get("product_intent")),
            "seniority": _s(r.get("seniority")),
            "department": _s(r.get("department")),
            "job_role": _s(r.get("job_role")),
            "team_size": _s(r.get("team_size")),
        }
        if vid not in by_vid:
            by_vid[vid] = {
                "visitor_id": vid,
                "source_channel": _s(r.get("grouped_channel")),
                "first_seen_at": _s(r.get("install_date")),
                "visitor_row_type": None,
            }
        base = by_vid[vid]
        for k, v in m.items():
            if v is not None:
                base[k] = v
        if base.get("source_channel") is None:
            sc = _s(r.get("grouped_channel"))
            if sc:
                base["source_channel"] = sc
        if base.get("first_seen_at") is None:
            fs = _s(r.get("install_date"))
            if fs:
                base["first_seen_at"] = fs
    out: list[dict[str, Any]] = []
    for spec in by_vid.values():
        row = {k: v for k, v in spec.items() if v is not None}
        row.setdefault("visitor_id", "")
        out.append(row)
    return out


def _build_touched_landed_rows(
    q1: list[dict[str, str]],
    monday: list[dict[str, str | None]],
    q1_vids: set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    touched: list[dict[str, Any]] = []
    landed: list[dict[str, Any]] = []
    for row in q1:
        vid = _s(row.get("VISITOR_ID"))
        camp = _s(row.get("CAMPAIGN"))
        if not vid or not camp:
            continue
        touched.append(
            {
                "visitor_id": vid,
                "campaign_name": camp,
                "ad_group": _s(row.get("AD_GROUP")),
                "touched_at": _s(row.get("CREATED_AT")),
            }
        )
        lu = _normalize_landing_url(row.get("LANDING_PAGE") or "")
        if lu:
            landed.append({"visitor_id": vid, "landing_url": lu})
    for r in monday:
        vid = _s(r.get("master_visitor_id"))
        mc = _s(r.get("marketing_campaign"))
        if not vid or not mc:
            continue
        if vid in q1_vids:
            continue
        touched.append(
            {
                "visitor_id": vid,
                "campaign_name": mc,
                "ad_group": _s(r.get("marketing_ad_group")),
                "touched_at": _s(r.get("install_date")),
            }
        )
        lu = _normalize_landing_url(r.get("marketing_landing_page") or "")
        if lu:
            landed.append({"visitor_id": vid, "landing_url": lu})
    return touched, landed


def _build_signed_up_rows(
    q1: list[dict[str, str]], monday: list[dict[str, str | None]]
) -> list[dict[str, Any]]:
    """Dedupe (visitor_id, pulse); prefer Q1 attribution flags."""
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for row in q1:
        if _s(row.get("TYPE")) != "attribution":
            continue
        vid = _s(row.get("VISITOR_ID"))
        pulse = _s(row.get("PULSE_ACCOUNT_ID"))
        if not vid or not pulse:
            continue
        by_key[(vid, pulse)] = {
            "visitor_id": vid,
            "pulse_id": pulse,
            "reasoning": "Q1 attribution row",
            "is_initial_setup_24h": _bit(row.get("IS_INITIAL_SETUP_24H")),
            "is_first_week_retention": _bit(row.get("IS_FIRST_WEEK_RETENTION")),
            "is_second_plus_week_retention": _bit(row.get("IS_SECOND_PLUS_WEEK_RETENTION")),
            "is_pay": _bit(row.get("IS_PAY")),
            "is_pay_21d": _bit(row.get("IS_PAY_21D")),
            "is_pay_28d": _bit(row.get("IS_PAY_28D")),
        }
    for r in monday:
        vid = _s(r.get("master_visitor_id"))
        pulse = _s(r.get("pulse_account_id"))
        if not vid or not pulse:
            continue
        k = (vid, pulse)
        if k in by_key:
            continue
        by_key[k] = {
            "visitor_id": vid,
            "pulse_id": pulse,
            "reasoning": "Monday CRM row",
            "is_initial_setup_24h": None,
            "is_first_week_retention": None,
            "is_second_plus_week_retention": None,
            "is_pay": None,
            "is_pay_21d": None,
            "is_pay_28d": None,
        }
    return list(by_key.values())


def _aggregate_leads(q2: list[dict[str, str]]) -> list[dict[str, Any]]:
    by_lid: dict[str, dict[str, Any]] = {}
    for row in q2:
        lid = _s(row.get("LEAD_ID"))
        if not lid:
            continue
        pulse = _s(row.get("PULSE_ACCOUNT_ID"))
        et = (_s(row.get("ENTITY_TYPE")) or "").strip()
        ld = _s(row.get("LEAD_DATE"))
        cur = by_lid.setdefault(
            lid,
            {
                "lead_id": lid,
                "pulse_id": pulse,
                "dates": [],
                "is_signup": False,
                "is_contact_sales": False,
                "signup_date": None,
                "contact_sales_date": None,
            },
        )
        if pulse and not cur.get("pulse_id"):
            cur["pulse_id"] = pulse
        if ld:
            cur["dates"].append(ld)
        if et == "Signup":
            cur["is_signup"] = True
            sd = cur["signup_date"]
            if sd is None or (ld and ld < sd):
                cur["signup_date"] = ld
        if et == "Contact Sales":
            cur["is_contact_sales"] = True
            cd = cur["contact_sales_date"]
            if cd is None or (ld and ld < cd):
                cur["contact_sales_date"] = ld
    out: list[dict[str, Any]] = []
    for cur in by_lid.values():
        dates = [d for d in cur["dates"] if d]
        cur["lead_date"] = min(dates) if dates else None
        del cur["dates"]
        out.append(cur)
    return out


def _sub_event_row(
    *,
    pulse: str,
    visitor_id: str,
    event_type: str,
    occurred_at: str,
    prod_gid: str | None,
    product_name: str,
    plan_tier: str | None,
    plan_period: str | None,
    arr: float | None,
    days_from_signup: int | None,
    created_reasoning: str,
    pa_reason: str,
    prod_reason: str,
) -> dict[str, Any]:
    import_key = f"{pulse}|{event_type}|{occurred_at}"
    return {
        "import_key": import_key,
        "pulse_external_id": pulse,
        "visitor_id": visitor_id,
        "event_type": event_type,
        "occurred_at": occurred_at,
        "product_graph_id": prod_gid or "",
        "product_name": product_name or None,
        "plan_tier": plan_tier,
        "plan_period": plan_period,
        "arr": arr,
        "days_from_signup": days_from_signup,
        "created_reasoning": created_reasoning,
        "pa_link_reasoning": pa_reason,
        "prod_link_reasoning": prod_reason,
    }


def _build_subscription_event_rows(
    monday: list[dict[str, str | None]],
    product_by_name: dict[str, str],
) -> list[dict[str, Any]]:
    """Monday export: signup (install_date), purchase (paying), churn — mirrors import_csv phase 6."""
    from scripts.mappings.monday_crm_campaigns import (
        is_paying_row,
        parse_amount,
        rows_by_pulse,
    )

    by_pulse = rows_by_pulse(monday)
    out: list[dict[str, Any]] = []
    pa_reason = "Lifecycle event for this product account"
    prod_reason = "Event applies to this product line"
    for pulse in sorted(by_pulse.keys()):
        rows_p = by_pulse[pulse]
        prod_raw = _s(rows_p[0].get("product"))
        prod_stored = _product_stored_name(prod_raw) if prod_raw else None
        prod_gid = product_by_name.get(prod_stored) if prod_stored else None
        visitor_id = ""
        for r in rows_p:
            mv = _s(r.get("master_visitor_id"))
            if mv:
                visitor_id = mv
                break

        installs = [r.get("install_date") for r in rows_p if r.get("install_date")]
        if installs:
            oc = min(installs)
            ost = str(oc).strip()
            if ost:
                out.append(
                    _sub_event_row(
                        pulse=pulse,
                        visitor_id=visitor_id,
                        event_type="signup",
                        occurred_at=ost,
                        prod_gid=prod_gid,
                        product_name=prod_raw or "",
                        plan_tier=None,
                        plan_period=None,
                        arr=None,
                        days_from_signup=None,
                        created_reasoning="Monday export: signup from install_date",
                        pa_reason=pa_reason,
                        prod_reason=prod_reason,
                    )
                )

        pay_rows = [r for r in rows_p if is_paying_row(r)]
        if pay_rows:
            pr = pay_rows[0]
            occurred = pr.get("first_product_arr_date") or pr.get("install_date")
            if occurred and str(occurred).strip():
                ost = str(occurred).strip()
                out.append(
                    _sub_event_row(
                        pulse=pulse,
                        visitor_id=visitor_id,
                        event_type="purchase",
                        occurred_at=ost,
                        prod_gid=prod_gid,
                        product_name=prod_raw or "",
                        plan_tier=_s(pr.get("first_plan_tier")),
                        plan_period=_s(pr.get("first_plan_period")),
                        arr=parse_amount(pr),
                        days_from_signup=_int_or_none(pr.get("days_to_pay")),
                        created_reasoning="Monday export: paying account",
                        pa_reason=pa_reason,
                        prod_reason=prod_reason,
                    )
                )

        churn_row = next(
            (r for r in rows_p if (r.get("first_churn_date") or "").strip()),
            None,
        )
        if churn_row:
            cd = churn_row.get("first_churn_date")
            if cd and str(cd).strip():
                ost = str(cd).strip()
                out.append(
                    _sub_event_row(
                        pulse=pulse,
                        visitor_id=visitor_id,
                        event_type="churn",
                        occurred_at=ost,
                        prod_gid=prod_gid,
                        product_name=prod_raw or "",
                        plan_tier=None,
                        plan_period=None,
                        arr=None,
                        days_from_signup=None,
                        created_reasoning="Monday export: churn date",
                        pa_reason=pa_reason,
                        prod_reason=prod_reason,
                    )
                )
    return out


# ---------------------------------------------------------------------------
# Cypher batches
# ---------------------------------------------------------------------------

_CYPHER_MERGE_CHANNELS = """
UNWIND $rows AS row
MERGE (ch:Channel {tenant_id: $tenant_id, name: row.name})
ON CREATE SET
  ch.id = randomUUID(),
  ch.created_at = $now,
  ch.updated_at = $now,
  ch.created_by_actor_id = $actor_id,
  ch.channel_type = row.channel_type,
  ch.status = 'active'
SET ch.channel_type = coalesce(row.channel_type, ch.channel_type),
    ch.updated_at = $now
WITH ch
MATCH (a:Actor {tenant_id: $tenant_id, id: $actor_id})
MERGE (a)-[cr:CREATED_BY]->(ch)
ON CREATE SET cr.reasoning = 'funnel import: Channel'
RETURN count(ch) AS c
"""

_CYPHER_MERGE_PRODUCTS = """
UNWIND $rows AS row
MERGE (pr:Product {tenant_id: $tenant_id, name: row.name})
ON CREATE SET
  pr.id = randomUUID(),
  pr.created_at = $now,
  pr.updated_at = $now,
  pr.created_by_actor_id = $actor_id,
  pr.status = coalesce(row.status, 'active')
SET pr.updated_at = $now
WITH pr
MATCH (a:Actor {tenant_id: $tenant_id, id: $actor_id})
MERGE (a)-[cr:CREATED_BY]->(pr)
ON CREATE SET cr.reasoning = 'funnel import: Product'
RETURN count(pr) AS c
"""

_CYPHER_MERGE_CONTENT = """
UNWIND $rows AS row
MERGE (co:Content {tenant_id: $tenant_id, url: row.url})
ON CREATE SET
  co.id = randomUUID(),
  co.created_at = $now,
  co.updated_at = $now,
  co.created_by_actor_id = $actor_id,
  co.name = row.name,
  co.content_type = row.content_type,
  co.status = coalesce(row.status, 'active')
SET co.name = coalesce(row.name, co.name),
    co.content_type = coalesce(row.content_type, co.content_type),
    co.updated_at = $now
WITH co
MATCH (a:Actor {tenant_id: $tenant_id, id: $actor_id})
MERGE (a)-[cr:CREATED_BY]->(co)
ON CREATE SET cr.reasoning = 'funnel import: Content'
RETURN count(co) AS c
"""

_CYPHER_MERGE_CAMPAIGNS = """
UNWIND $rows AS row
MERGE (c:Campaign {tenant_id: $tenant_id, name: row.name})
ON CREATE SET
  c.id = randomUUID(),
  c.created_at = $now,
  c.updated_at = $now,
  c.created_by_actor_id = $actor_id,
  c.status = 'active'
SET c.campaign_category = coalesce(row.campaign_category, c.campaign_category),
    c.marketing_source = coalesce(row.marketing_source, c.marketing_source),
    c.channel = coalesce(row.channel_name, c.channel),
    c.updated_at = $now
WITH c, row
MATCH (a:Actor {tenant_id: $tenant_id, id: $actor_id})
MERGE (a)-[cr:CREATED_BY]->(c)
ON CREATE SET cr.reasoning = 'funnel import: Campaign'
WITH c, row
FOREACH (_ IN CASE WHEN row.channel_name IS NULL OR row.channel_name = '' THEN [] ELSE [1] END |
  MERGE (ch:Channel {tenant_id: $tenant_id, name: row.channel_name})
  MERGE (ch)-[hc:HAS_CAMPAIGN]->(c)
  ON CREATE SET hc.reasoning = ''
)
WITH c, row
FOREACH (_ IN CASE WHEN row.landing_url IS NULL OR row.landing_url = '' THEN [] ELSE [1] END |
  MERGE (co:Content {tenant_id: $tenant_id, url: row.landing_url})
  MERGE (c)-[hx:HAS_CONTENT]->(co)
  ON CREATE SET hx.reasoning = ''
)
RETURN count(c) AS c
"""

_CYPHER_MERGE_PRODUCT_ACCOUNTS = """
UNWIND $rows AS row
MERGE (pa:ProductAccount {tenant_id: $tenant_id, external_id: row.external_id})
ON CREATE SET
  pa.id = randomUUID(),
  pa.created_at = $now,
  pa.updated_at = $now,
  pa.created_by_actor_id = $actor_id
SET pa += row.pa_props
SET pa.updated_at = $now
WITH pa
MATCH (a:Actor {tenant_id: $tenant_id, id: $actor_id})
MERGE (a)-[cr:CREATED_BY]->(pa)
ON CREATE SET cr.reasoning = 'funnel import: ProductAccount'
RETURN count(pa) AS c
"""

_CYPHER_MERGE_FOR_PRODUCT = """
UNWIND $rows AS row
MATCH (pa:ProductAccount {tenant_id: $tenant_id, external_id: row.external_id})
MATCH (pr:Product {tenant_id: $tenant_id, id: row.product_id})
MERGE (pa)-[r:FOR_PRODUCT]->(pr)
ON CREATE SET r.reasoning = coalesce(row.product_reasoning, '')
RETURN count(r) AS c
"""

_CYPHER_MERGE_VISITORS = """
UNWIND $rows AS row
MERGE (v:Visitor {tenant_id: $tenant_id, visitor_id: row.visitor_id})
ON CREATE SET
  v.id = randomUUID(),
  v.created_at = $now,
  v.updated_at = $now,
  v.created_by_actor_id = $actor_id
SET v.source_channel = coalesce(row.source_channel, v.source_channel),
    v.first_seen_at = coalesce(row.first_seen_at, v.first_seen_at),
    v.visitor_row_type = coalesce(row.visitor_row_type, v.visitor_row_type),
    v.device = coalesce(row.device, v.device),
    v.first_user_platform_language = coalesce(row.first_user_platform_language, v.first_user_platform_language),
    v.user_goal = coalesce(row.user_goal, v.user_goal),
    v.signup_flow = coalesce(row.signup_flow, v.signup_flow),
    v.raw_signup_flow = coalesce(row.raw_signup_flow, v.raw_signup_flow),
    v.signup_use_case = coalesce(row.signup_use_case, v.signup_use_case),
    v.signup_cluster = coalesce(row.signup_cluster, v.signup_cluster),
    v.product_intent = coalesce(row.product_intent, v.product_intent),
    v.seniority = coalesce(row.seniority, v.seniority),
    v.department = coalesce(row.department, v.department),
    v.job_role = coalesce(row.job_role, v.job_role),
    v.team_size = coalesce(row.team_size, v.team_size),
    v.updated_at = $now
WITH v
MATCH (a:Actor {tenant_id: $tenant_id, id: $actor_id})
MERGE (a)-[cr:CREATED_BY]->(v)
ON CREATE SET cr.reasoning = 'funnel import: Visitor'
RETURN count(v) AS c
"""

_CYPHER_MERGE_TOUCHED = """
UNWIND $rows AS row
MATCH (v:Visitor {tenant_id: $tenant_id, visitor_id: row.visitor_id})
MATCH (c:Campaign {tenant_id: $tenant_id, name: row.campaign_name})
MERGE (v)-[t:TOUCHED]->(c)
ON CREATE SET t.reasoning = 'funnel touchpoint'
SET t.ad_group = coalesce(row.ad_group, t.ad_group),
    t.touched_at = coalesce(row.touched_at, t.touched_at)
RETURN count(t) AS c
"""

_CYPHER_MERGE_LANDED_ON = """
UNWIND $rows AS row
MATCH (v:Visitor {tenant_id: $tenant_id, visitor_id: row.visitor_id})
MATCH (co:Content {tenant_id: $tenant_id, url: row.landing_url})
MERGE (v)-[l:LANDED_ON]->(co)
ON CREATE SET l.reasoning = 'funnel landing page'
RETURN count(l) AS c
"""

_CYPHER_MERGE_SIGNED_UP_AS = """
UNWIND $rows AS row
MATCH (v:Visitor {tenant_id: $tenant_id, visitor_id: row.visitor_id})
MATCH (pa:ProductAccount {tenant_id: $tenant_id, external_id: row.pulse_id})
MERGE (v)-[s:SIGNED_UP_AS]->(pa)
ON CREATE SET s.reasoning = coalesce(row.reasoning, '')
SET s.is_initial_setup_24h = coalesce(row.is_initial_setup_24h, s.is_initial_setup_24h),
    s.is_first_week_retention = coalesce(row.is_first_week_retention, s.is_first_week_retention),
    s.is_second_plus_week_retention = coalesce(row.is_second_plus_week_retention, s.is_second_plus_week_retention),
    s.is_pay = coalesce(row.is_pay, s.is_pay),
    s.is_pay_21d = coalesce(row.is_pay_21d, s.is_pay_21d),
    s.is_pay_28d = coalesce(row.is_pay_28d, s.is_pay_28d)
RETURN count(s) AS c
"""

_CYPHER_MERGE_LEADS = """
UNWIND $rows AS row
MERGE (l:Lead {tenant_id: $tenant_id, external_id: row.lead_id})
ON CREATE SET
  l.id = randomUUID(),
  l.created_at = $now,
  l.updated_at = $now,
  l.created_by_actor_id = $actor_id,
  l.name = row.name,
  l.status = 'new'
SET l.lead_date = coalesce(row.lead_date, l.lead_date),
    l.is_signup = coalesce(row.is_signup, l.is_signup),
    l.is_contact_sales = coalesce(row.is_contact_sales, l.is_contact_sales),
    l.signup_date = coalesce(row.signup_date, l.signup_date),
    l.contact_sales_date = coalesce(row.contact_sales_date, l.contact_sales_date),
    l.updated_at = $now
WITH l
MATCH (a:Actor {tenant_id: $tenant_id, id: $actor_id})
MERGE (a)-[cr:CREATED_BY]->(l)
ON CREATE SET cr.reasoning = 'funnel import: Lead'
RETURN count(l) AS c
"""

_CYPHER_MERGE_LEAD_WORKS_AT = """
UNWIND $rows AS row
MATCH (l:Lead {tenant_id: $tenant_id, external_id: row.lead_id})
MATCH (pa:ProductAccount {tenant_id: $tenant_id, external_id: row.pulse_id})
MERGE (l)-[w:WORKS_AT]->(pa)
ON CREATE SET w.reasoning = coalesce(row.reasoning, 'Lead linked to workspace')
RETURN count(w) AS c
"""

_CYPHER_SUB_EVENT_CORE = """
UNWIND $rows AS row
MERGE (e:SubscriptionEvent {tenant_id: $tenant_id, import_key: row.import_key})
ON CREATE SET
  e.id = randomUUID(),
  e.created_at = $now,
  e.updated_at = $now,
  e.created_by_actor_id = $actor_id
SET
  e.event_type = row.event_type,
  e.occurred_at = row.occurred_at,
  e.plan_tier = coalesce(row.plan_tier, e.plan_tier),
  e.plan_period = coalesce(row.plan_period, e.plan_period),
  e.arr = coalesce(row.arr, e.arr),
  e.days_from_signup = coalesce(row.days_from_signup, e.days_from_signup),
  e.product_name = coalesce(row.product_name, e.product_name),
  e.updated_at = $now
WITH e, row
MATCH (a:Actor {tenant_id: $tenant_id, id: $actor_id})
MERGE (a)-[cr:CREATED_BY]->(e)
ON CREATE SET cr.reasoning = coalesce(row.created_reasoning, '')
WITH e, row
MATCH (pa:ProductAccount {tenant_id: $tenant_id, external_id: row.pulse_external_id})
MERGE (pa)-[h:HAS_SUBSCRIPTION_EVENT]->(e)
ON CREATE SET h.reasoning = coalesce(row.pa_link_reasoning, '')
RETURN count(e) AS c
"""

_CYPHER_SUB_EVENT_FOR_PRODUCT = """
UNWIND $rows AS row
MATCH (e:SubscriptionEvent {tenant_id: $tenant_id, import_key: row.import_key})
MATCH (pr:Product {tenant_id: $tenant_id, id: row.product_graph_id})
MERGE (e)-[fp:FOR_PRODUCT]->(pr)
ON CREATE SET fp.reasoning = coalesce(row.prod_link_reasoning, '')
RETURN count(fp) AS c
"""

_CYPHER_SUB_EVENT_VISITOR = """
UNWIND $rows AS row
MATCH (e:SubscriptionEvent {tenant_id: $tenant_id, import_key: row.import_key})
MATCH (v:Visitor {tenant_id: $tenant_id, visitor_id: row.visitor_id})
MERGE (v)-[h:HAS_SUBSCRIPTION_EVENT]->(e)
ON CREATE SET h.reasoning = 'Visitor from Monday row for this workspace'
RETURN count(h) AS c
"""

_CYPHER_SUB_EVENT_LEADS = """
UNWIND $rows AS row
MATCH (e:SubscriptionEvent {tenant_id: $tenant_id, import_key: row.import_key})
MATCH (pa:ProductAccount {tenant_id: $tenant_id, external_id: row.pulse_external_id})
MATCH (l:Lead {tenant_id: $tenant_id})-[:WORKS_AT]->(pa)
MERGE (l)-[h:HAS_SUBSCRIPTION_EVENT]->(e)
ON CREATE SET h.reasoning = 'Lead on workspace for this lifecycle event'
RETURN count(h) AS c
"""


async def _batched_execute(
    db: Any,
    scope: Any,
    cypher: str,
    rows: list[dict[str, Any]],
    *,
    batch_size: int,
    actor_id: str,
    phase: int,
    label: str,
) -> None:
    if not rows:
        return
    bs = max(1, batch_size)
    chunks = _chunks(rows, bs)
    total = len(chunks)
    now = _now_iso()
    for i, chunk in enumerate(chunks, start=1):
        await db.execute_cypher(
            scope,
            cypher,
            {"rows": chunk, "now": now, "actor_id": actor_id},
        )
        _log_phase_progress(phase, label, i, total)


async def _resolve_products(db: Any, scope: Any) -> dict[str, str]:
    recs = await db.execute_cypher(
        scope,
        "MATCH (p:Product {tenant_id: $tenant_id}) RETURN p.id AS id, p.name AS name",
        {},
    )
    return {str(r["name"]): str(r["id"]) for r in recs if r.get("id") and r.get("name")}


async def _verify(db: Any, scope: Any) -> None:
    print("\n=== Funnel verification (tenant) ===", flush=True)
    labels = [
        "Channel",
        "Product",
        "Content",
        "Campaign",
        "Visitor",
        "ProductAccount",
        "Lead",
        "SubscriptionEvent",
    ]
    for lb in labels:
        recs = await db.execute_cypher(
            scope,
            f"MATCH (n:{lb} {{tenant_id: $tenant_id}}) RETURN count(n) AS c",
            {},
        )
        c = recs[0].get("c", 0) if recs else 0
        print(f"  {lb}: {c}", flush=True)
    rels = [
        "HAS_CAMPAIGN",
        "HAS_CONTENT",
        "TOUCHED",
        "LANDED_ON",
        "SIGNED_UP_AS",
        "FOR_PRODUCT",
        "WORKS_AT",
        "HAS_SUBSCRIPTION_EVENT",
    ]
    for rt in rels:
        recs = await db.execute_cypher(
            scope,
            f"""
            MATCH (a {{tenant_id: $tenant_id}})-[r:{rt}]->(b {{tenant_id: $tenant_id}})
            RETURN count(r) AS c
            """,
            {},
        )
        c = recs[0].get("c", 0) if recs else 0
        print(f"  {rt}: {c}", flush=True)

    q_no_touch = """
    MATCH (v:Visitor {tenant_id: $tenant_id})
    WHERE NOT (v)-[:TOUCHED]->()
    RETURN count(v) AS c
    """
    recs = await db.execute_cypher(scope, q_no_touch, {})
    c = recs[0].get("c", 0) if recs else 0
    print(f"\n  Anomaly: Visitors with no TOUCHED edge: {c}", flush=True)
    print(
        "    (expected: Monday visitors without marketing_campaign / Q1-only gaps)",
        flush=True,
    )

    q_pa_no_v = """
    MATCH (pa:ProductAccount {tenant_id: $tenant_id})
    WHERE NOT ()-[:SIGNED_UP_AS]->(pa)
    RETURN count(pa) AS c
    """
    recs = await db.execute_cypher(scope, q_pa_no_v, {})
    c = recs[0].get("c", 0) if recs else 0
    print(f"  Anomaly: ProductAccounts with no incoming SIGNED_UP_AS: {c}", flush=True)

    q_lead_orphan = """
    MATCH (l:Lead {tenant_id: $tenant_id})
    WHERE NOT (l)-[:WORKS_AT]->()
    RETURN count(l) AS c
    """
    recs = await db.execute_cypher(scope, q_lead_orphan, {})
    c = recs[0].get("c", 0) if recs else 0
    print(f"  Anomaly: Leads with no WORKS_AT (missing ProductAccount): {c}", flush=True)


async def run_funnel_import(
    *,
    monday_path: str,
    visitors_path: str,
    leads_path: str,
    actor_id: str,
    dry_run: bool,
    execute: bool,
    only_phase: int | None,
    batch_size: int,
    api_key: str,
) -> None:
    from gtmdb import connect_gtmdb

    print("Loading CSVs …", flush=True)
    monday = _load_monday(monday_path)
    q1 = _load_q1(visitors_path)
    q2 = _load_q2(leads_path)
    q1_vids = {_s(r.get("VISITOR_ID")) for r in q1 if _s(r.get("VISITOR_ID"))}

    ch_rows, pr_rows = _build_channel_product_rows(monday, q1)
    co_rows = _build_content_rows(monday, q1)
    camp_rows = _build_campaign_rows(monday, q1)
    visitor_rows = _build_visitor_rows(monday, q1)
    touched_rows, landed_rows = _build_touched_landed_rows(q1, monday, q1_vids)
    signed_rows = _build_signed_up_rows(q1, monday)
    lead_agg = _aggregate_leads(q2)
    lead_merge_rows = [
        {
            "lead_id": x["lead_id"],
            "name": f"Lead {x['lead_id'][:18]}",
            "lead_date": x.get("lead_date"),
            "is_signup": x.get("is_signup"),
            "is_contact_sales": x.get("is_contact_sales"),
            "signup_date": x.get("signup_date"),
            "contact_sales_date": x.get("contact_sales_date"),
        }
        for x in lead_agg
    ]
    lead_work_rows = [
        {
            "lead_id": x["lead_id"],
            "pulse_id": x["pulse_id"],
            "reasoning": "Q2 export",
        }
        for x in lead_agg
        if x.get("pulse_id")
    ]

    pa_est, fp_est = _build_pulse_rows(monday, q1, q2, {})
    sub_rows_est = _build_subscription_event_rows(monday, {})
    print(
        f"  Monday rows={len(monday)} Q1={len(q1)} Q2={len(q2)}\n"
        f"  Phase1 channels={len(ch_rows)} products={len(pr_rows)}\n"
        f"  Phase2 content={len(co_rows)}\n"
        f"  Phase3 campaigns={len(camp_rows)}\n"
        f"  Phase4 product_accounts={len(pa_est)} for_product_edges_if_product_loaded={len(fp_est)}\n"
        f"  Phase5 visitors={len(visitor_rows)}\n"
        f"  Phase6 touched={len(touched_rows)} landed_on={len(landed_rows)}\n"
        f"  Phase7 signed_up_as={len(signed_rows)}\n"
        f"  Phase8 leads={len(lead_merge_rows)} works_at rows={len(lead_work_rows)}\n"
        f"  Phase9 subscription_events={len(sub_rows_est)} (from Monday; MERGE idempotent)",
        flush=True,
    )

    if dry_run:
        print("Dry run only — no database writes.", flush=True)
        return

    if not execute:
        print("Refusing: pass --execute for writes.", flush=True)
        return

    db, scope = await connect_gtmdb(api_key=api_key)
    try:
        await db.actors.ensure(scope, actor_id)

        phases = list(range(1, _PHASE_TOTAL + 1))
        if only_phase is not None:
            if only_phase < 1 or only_phase > _PHASE_TOTAL:
                raise SystemExit(f"--only-phase must be 1..{_PHASE_TOTAL}")
            phases = [only_phase]

        product_by_name: dict[str, str] = {}
        if any(p in phases for p in (4, 9)) and 1 not in phases:
            product_by_name = await _resolve_products(db, scope)

        if 1 in phases:
            await _batched_execute(
                db,
                scope,
                _CYPHER_MERGE_CHANNELS,
                ch_rows,
                batch_size=max(batch_size, len(ch_rows) or 1),
                actor_id=actor_id,
                phase=1,
                label="channels",
            )
            await _batched_execute(
                db,
                scope,
                _CYPHER_MERGE_PRODUCTS,
                pr_rows,
                batch_size=max(batch_size, len(pr_rows) or 1),
                actor_id=actor_id,
                phase=1,
                label="products",
            )
            print("[funnel] Phase 1 complete: channels + products.", flush=True)
            product_by_name = await _resolve_products(db, scope)

        if 2 in phases:
            await _batched_execute(
                db,
                scope,
                _CYPHER_MERGE_CONTENT,
                co_rows,
                batch_size=batch_size,
                actor_id=actor_id,
                phase=2,
                label="content (landing pages)",
            )
            print("[funnel] Phase 2 complete: Content.", flush=True)

        if 3 in phases:
            await _batched_execute(
                db,
                scope,
                _CYPHER_MERGE_CAMPAIGNS,
                camp_rows,
                batch_size=batch_size,
                actor_id=actor_id,
                phase=3,
                label="campaigns + HAS_CAMPAIGN + HAS_CONTENT",
            )
            print("[funnel] Phase 3 complete: Campaigns.", flush=True)

        if 4 in phases:
            if not product_by_name:
                product_by_name = await _resolve_products(db, scope)
            pa_rows, fp_rows = _build_pulse_rows(monday, q1, q2, product_by_name)
            await _batched_execute(
                db,
                scope,
                _CYPHER_MERGE_PRODUCT_ACCOUNTS,
                pa_rows,
                batch_size=batch_size,
                actor_id=actor_id,
                phase=4,
                label="product accounts",
            )
            await _batched_execute(
                db,
                scope,
                _CYPHER_MERGE_FOR_PRODUCT,
                fp_rows,
                batch_size=batch_size,
                actor_id=actor_id,
                phase=4,
                label="FOR_PRODUCT edges",
            )
            print("[funnel] Phase 4 complete: ProductAccounts.", flush=True)

        if 5 in phases:
            await _batched_execute(
                db,
                scope,
                _CYPHER_MERGE_VISITORS,
                visitor_rows,
                batch_size=batch_size,
                actor_id=actor_id,
                phase=5,
                label="visitors",
            )
            print("[funnel] Phase 5 complete: Visitors.", flush=True)

        if 6 in phases:
            await _batched_execute(
                db,
                scope,
                _CYPHER_MERGE_TOUCHED,
                touched_rows,
                batch_size=batch_size,
                actor_id=actor_id,
                phase=6,
                label="TOUCHED edges",
            )
            await _batched_execute(
                db,
                scope,
                _CYPHER_MERGE_LANDED_ON,
                landed_rows,
                batch_size=batch_size,
                actor_id=actor_id,
                phase=6,
                label="LANDED_ON edges",
            )
            print("[funnel] Phase 6 complete: TOUCHED + LANDED_ON.", flush=True)

        if 7 in phases:
            await _batched_execute(
                db,
                scope,
                _CYPHER_MERGE_SIGNED_UP_AS,
                signed_rows,
                batch_size=batch_size,
                actor_id=actor_id,
                phase=7,
                label="SIGNED_UP_AS edges",
            )
            print("[funnel] Phase 7 complete: SIGNED_UP_AS.", flush=True)

        if 8 in phases:
            await _batched_execute(
                db,
                scope,
                _CYPHER_MERGE_LEADS,
                lead_merge_rows,
                batch_size=batch_size,
                actor_id=actor_id,
                phase=8,
                label="Lead nodes",
            )
            await _batched_execute(
                db,
                scope,
                _CYPHER_MERGE_LEAD_WORKS_AT,
                lead_work_rows,
                batch_size=batch_size,
                actor_id=actor_id,
                phase=8,
                label="WORKS_AT (Lead→ProductAccount)",
            )
            print("[funnel] Phase 8 complete: Leads.", flush=True)

        if 9 in phases:
            if not product_by_name:
                product_by_name = await _resolve_products(db, scope)
            sub_rows = _build_subscription_event_rows(monday, product_by_name)
            fp_sub = [
                {
                    "import_key": r["import_key"],
                    "product_graph_id": r["product_graph_id"],
                    "prod_link_reasoning": r["prod_link_reasoning"],
                }
                for r in sub_rows
                if (r.get("product_graph_id") or "").strip()
            ]
            v_sub = [r for r in sub_rows if (r.get("visitor_id") or "").strip()]
            await _batched_execute(
                db,
                scope,
                _CYPHER_SUB_EVENT_CORE,
                sub_rows,
                batch_size=batch_size,
                actor_id=actor_id,
                phase=9,
                label="subscription events (core + ProductAccount)",
            )
            await _batched_execute(
                db,
                scope,
                _CYPHER_SUB_EVENT_FOR_PRODUCT,
                fp_sub,
                batch_size=batch_size,
                actor_id=actor_id,
                phase=9,
                label="subscription events FOR_PRODUCT",
            )
            await _batched_execute(
                db,
                scope,
                _CYPHER_SUB_EVENT_VISITOR,
                v_sub,
                batch_size=batch_size,
                actor_id=actor_id,
                phase=9,
                label="subscription events Visitor→Event",
            )
            await _batched_execute(
                db,
                scope,
                _CYPHER_SUB_EVENT_LEADS,
                sub_rows,
                batch_size=batch_size,
                actor_id=actor_id,
                phase=9,
                label="subscription events Lead→Event",
            )
            print("[funnel] Phase 9 complete: SubscriptionEvents.", flush=True)

        if 10 in phases:
            await _verify(db, scope)
            print("[funnel] Phase 10 complete: verification.", flush=True)

        print("\n[funnel] All requested phases finished.", flush=True)
    finally:
        await db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Unified funnel CSV import (3 sheets).")
    parser.add_argument("--monday", required=True, help="Monday CRM export (headerless 47-col)")
    parser.add_argument("--visitors", required=True, help="Query_1 visitor/campaign CSV (headered)")
    parser.add_argument("--leads", required=True, help="Query_2 leads CSV (headered)")
    parser.add_argument("--actor-id", default="funnel-importer", help="Actor id for provenance")
    parser.add_argument("--batch-size", type=int, default=500, help="Rows per UNWIND batch")
    parser.add_argument("--dry-run", action="store_true", help="Parse + counts only")
    parser.add_argument("--execute", action="store_true", help="Write to Neo4j")
    parser.add_argument(
        "--only-phase",
        type=int,
        default=None,
        metavar="N",
        help=f"Run only phase N (1..{_PHASE_TOTAL}). Requires prior phases already loaded.",
    )
    args = parser.parse_args()

    if not args.dry_run and not args.execute:
        print("Use --dry-run or --execute.", file=sys.stderr)
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
    if not api_key and not args.dry_run:
        print("Set GTMDB_API_KEY or GTMDB_ADMIN_KEY for live import.", file=sys.stderr)
        sys.exit(1)

    asyncio.run(
        run_funnel_import(
            monday_path=args.monday,
            visitors_path=args.visitors,
            leads_path=args.leads,
            actor_id=args.actor_id,
            dry_run=args.dry_run,
            execute=args.execute,
            only_phase=args.only_phase,
            batch_size=args.batch_size,
            api_key=api_key,
        )
    )


if __name__ == "__main__":
    main()
