"""Monday.com-style CRM campaign export (headerless CSV, 47 columns).

Column indices are 0-based (``row[0]`` == first SQL column ``master_visitor_id``).
"""

from __future__ import annotations

import csv
from typing import Any

# 47 columns total (indices 0..46)
COL: dict[str, int] = {
    "master_visitor_id": 0,
    "pulse_account_id": 1,
    "product": 2,
    "is_first_product": 3,
    "first_product_install_date": 4,
    "install_date": 5,
    "user_goal": 6,
    "company_size_num": 7,
    "company_size_group": 8,
    "industry": 9,
    "signup_flow": 10,
    "raw_signup_flow": 11,
    "product_intent": 12,
    "survey_sector": 13,
    "survey_sub_sector": 14,
    "signup_use_case": 15,
    "first_plan_tier": 16,
    "first_plan_period": 17,
    "first_account_channel": 18,
    "first_product_arr_date": 19,
    "first_product_arr": 20,
    "first_churn_date": 21,
    "signup_cluster": 22,
    "first_account_arr_date": 23,
    "first_account_touch_date": 24,
    "is_cross_sell": 25,
    "installed_user": 26,
    "seniority": 27,
    "team_size": 28,
    "region": 29,
    "country": 30,
    "job_role": 31,
    "department": 32,
    "first_user_platform_language": 33,
    "device": 34,
    "marketing_campaign": 35,
    "grouped_channel": 36,
    "marketing_ad_group": 37,
    "marketing_landing_page": 38,
    "campaign_category": 39,
    "marketing_source": 40,
    "dep0_predicted_arr": 41,
    "extra_predicted": 42,
    "is_paying": 43,
    "days_to_pay": 44,
    "extra_46": 45,
    "extra_47": 46,
}

CATEGORY_LABELS: dict[str, str] = {
    "comp_crm_hubspot": "CRM vs HubSpot",
    "comp_crm_salesforce": "CRM vs Salesforce",
    "comp_crm": "CRM Competitive",
    "crm_main": "CRM Main",
    "crm_sales": "CRM Sales",
    "crm_industries": "CRM Industries",
    "crm_free": "CRM Free Tier",
    "crm": "CRM Core",
    "brand": "Brand Awareness",
    "brand_crm": "Brand CRM",
    "cvr_crm_msc": "CRM Conversion",
    "cvr_crm_msc_uitest": "CRM Conversion (test)",
    "lead_management": "Lead Management",
    "account_management": "Account Management",
    "contact_management": "Contact Management",
    "project": "Project Management",
    "project_management": "Project Management",
    "management": "Management",
    "main": "Main Awareness",
    "main2": "Main Awareness 2",
    "subreddits": "Reddit Communities",
    "to_do_list": "To-do List",
    "team_tasks_and_projects": "Team Tasks & Projects",
    "sales_pipeline": "Sales Pipeline",
    "grants_management": "Grants Management",
    "client_projects": "Client Projects",
}


def normalize_cell(raw: str | None) -> str | None:
    if raw is None:
        return None
    s = raw.strip()
    if not s or s.lower() == "null":
        return None
    return s


def parse_row(fields: list[str]) -> dict[str, str | None]:
    out: dict[str, str | None] = {}
    for key, idx in COL.items():
        if idx >= len(fields):
            out[key] = None
        else:
            out[key] = normalize_cell(fields[idx])
    return out


def campaign_dedupe_key(row: dict[str, str | None]) -> tuple[str, str] | None:
    cat = row.get("campaign_category")
    ch = row.get("grouped_channel")
    if not cat or not ch:
        return None
    return (cat, ch)


def campaign_display_name(row: dict[str, str | None]) -> str | None:
    key = campaign_dedupe_key(row)
    if not key:
        return None
    cat, ch = key
    label = CATEGORY_LABELS.get(cat, cat.replace("_", " ").title())
    return f"{label} - {ch}"


def channel_type(channel_name: str) -> str:
    n = channel_name.upper()
    if n in ("SEM", "YOUTUBE", "FACEBOOK", "MB_PUSH", "DSPS_DISPLAY"):
        return "paid"
    if n in ("ORGANIC_SEO", "BRANDED_SEARCH"):
        return "organic"
    if n.startswith("MB_"):
        return "marketplace"
    return "other"


def lead_status_from_intent(intent: str | None) -> str:
    if not intent:
        return "new"
    if "high" in intent.lower():
        return "qualified"
    return "new"


def title_from_row(row: dict[str, str | None]) -> str | None:
    parts = [row.get("seniority"), row.get("job_role")]
    parts = [p for p in parts if p]
    return " - ".join(parts) if parts else None


def snippet_from_row(row: dict[str, str | None]) -> str:
    """Non-PII context (no visitor / account / user ids)."""
    pairs = [
        ("region", row.get("region")),
        ("country", row.get("country")),
        ("segment", row.get("company_size_group")),
        ("team_size", row.get("team_size")),
        ("use_case", row.get("signup_use_case")),
        ("intent", row.get("product_intent")),
        ("device", row.get("device")),
        ("lang", row.get("first_user_platform_language")),
        ("sector", row.get("survey_sector")),
        ("sub_sector", row.get("survey_sub_sector")),
        ("install", row.get("install_date")),
    ]
    if row.get("days_to_pay"):
        pairs.append(("days_to_pay", row.get("days_to_pay")))
    return " | ".join(f"{k}={v}" for k, v in pairs if v)


def bant_from_row(row: dict[str, str | None]) -> dict[str, Any]:
    seg = (row.get("company_size_group") or "").upper()
    budget = {"ENT": 9, "MM": 7, "SMB": 5, "S": 3}.get(seg, 3)

    sen = (row.get("seniority") or "").lower()
    authority = {
        "executive": 9,
        "director": 8,
        "manager": 6,
        "entry": 3,
        "unknown": 4,
    }.get(sen, 4)

    intent = (row.get("product_intent") or "").lower()
    if "high" in intent:
        need = 9
    elif "low" in intent:
        need = 4
    else:
        need = 5

    goal = (row.get("user_goal") or "").lower()
    timeline = 7 if goal == "work" else 3
    if row.get("first_product_arr_date"):
        timeline = min(9, timeline + 2)

    total = budget + authority + need + timeline
    return {
        "budget": budget,
        "authority": authority,
        "need": need,
        "timeline": timeline,
        "total": total,
    }


def is_paying_row(row: dict[str, str | None]) -> bool:
    return row.get("is_paying") == "1"

def account_name_from_row(row: dict[str, str | None]) -> str:
    """Human-readable company placeholder (no legal name in export): geo + industry + pulse."""
    pulse = row.get("pulse_account_id") or "unknown"
    country = row.get("country") or ""
    region = row.get("region") or ""
    ind = row.get("industry") or "Unknown industry"
    geo = ", ".join(x for x in (region, country) if x) or "Unknown geo"
    return f"{geo} · {ind} · {pulse}"


def rows_by_pulse(
    rows: list[dict[str, str | None]],
) -> dict[str, list[dict[str, str | None]]]:
    from collections import defaultdict

    out: dict[str, list[dict[str, str | None]]] = defaultdict(list)
    for r in rows:
        p = r.get("pulse_account_id")
        if p:
            out[p].append(r)
    return dict(out)



def parse_amount(row: dict[str, str | None]) -> float | None:
    raw = row.get("first_product_arr")
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def row_passes_cli_filter(
    row: dict[str, str | None],
    *,
    campaign_category: str | None,
    grouped_channel: str | None,
) -> bool:
    if campaign_category and row.get("campaign_category") != campaign_category:
        return False
    if grouped_channel and row.get("grouped_channel") != grouped_channel:
        return False
    return True


def read_csv_rows(path: str, *, delimiter: str = ",", has_header: bool = False) -> list[dict[str, str | None]]:
    rows: list[dict[str, str | None]] = []
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f, delimiter=delimiter)
        first_data = True
        for fields in reader:
            if not fields or all(not normalize_cell(x) for x in fields):
                continue
            if has_header and first_data:
                first_data = False
                continue
            first_data = False
            rows.append(parse_row(fields))
    return rows
