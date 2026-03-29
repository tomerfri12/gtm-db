"""Composable permission presets and token factory."""

from __future__ import annotations

import json
import uuid
from typing import Any

from gtmdb.tokens import AccessToken

# Generic library-level presets (JSON-serializable policy dicts)
PRESETS: dict[str, list[dict[str, Any]]] = {
    "full_access": [
        {
            "effect": "allow",
            "actions": ["read", "write"],
            "resources": ["*"],
            "conditions": {},
        }
    ],
    "read_all": [
        {
            "effect": "allow",
            "actions": ["read"],
            "resources": ["*"],
            "conditions": {},
        }
    ],
    "write_all": [
        {
            "effect": "allow",
            "actions": ["write"],
            "resources": ["*"],
            "conditions": {},
        }
    ],
    "no_raw_content": [
        {
            "effect": "deny",
            "actions": ["read"],
            "resources": [
                "email.body",
                "call.transcript",
                "call.recording_s3_key",
                "meeting.recording_s3_key",
            ],
            "conditions": {},
        }
    ],
}


def create_token_from_presets(
    tenant_id: uuid.UUID | str,
    owner_id: str,
    owner_type: str,
    preset_names: list[str],
    *,
    extra_policies: list[dict[str, Any]] | None = None,
    custom_presets: dict[str, list[dict[str, Any]]] | None = None,
    label: str = "",
    key_id: str | None = None,
    redact_mode: str = "hint",
    is_active: bool = True,
) -> AccessToken:
    """Merge named presets (and optional custom presets) into one ``AccessToken``."""
    all_presets = {**PRESETS, **(custom_presets or {})}
    policies: list[dict[str, Any]] = []
    for name in preset_names:
        if name not in all_presets:
            raise KeyError(f"Unknown preset: {name!r}")
        policies.extend(all_presets[name])
    if extra_policies:
        policies.extend(extra_policies)

    tid = uuid.UUID(str(tenant_id)) if not isinstance(tenant_id, uuid.UUID) else tenant_id

    return AccessToken(
        tenant_id=tid,
        owner_id=owner_id,
        owner_type=owner_type,
        label=label,
        policies=json.dumps(policies),
        key_id=key_id,
        redact_mode=redact_mode,
        is_active=is_active,
    )
