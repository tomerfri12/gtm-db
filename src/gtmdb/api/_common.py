"""Shared validation helpers for the public API."""

from __future__ import annotations

from typing import Any


def require_non_empty_str(value: Any, field: str) -> str:
    s = ("" if value is None else str(value)).strip()
    if not s:
        raise ValueError(f"{field} is required and must be non-empty")
    return s


def optional_reasoning(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def composed_person_name(
    first_name: str | None,
    last_name: str | None,
) -> str | None:
    parts = [(first_name or "").strip(), (last_name or "").strip()]
    parts = [p for p in parts if p]
    return " ".join(parts) if parts else None


def display_name_for_person(
    first_name: str | None,
    last_name: str | None,
    *,
    company_name: str | None = None,
    email: str | None = None,
    fallback: str = "Unknown",
) -> str:
    name = composed_person_name(first_name, last_name)
    if name:
        return name
    for candidate in (company_name, email):
        c = (candidate or "").strip()
        if c:
            return c
    return fallback
