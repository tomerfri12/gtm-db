"""Phase 2: field masking, redaction, ids condition, presets."""

from __future__ import annotations

import json
import uuid

import pytest

from gtmdb.presets import PRESETS, create_token_from_presets
from gtmdb.scope import Scope
from gtmdb.tokens import AccessToken
from gtmdb.types import NodeData


def _token(policies: list[dict], redact_mode: str = "hint") -> AccessToken:
    return AccessToken(
        tenant_id=uuid.uuid4(),
        owner_id="test",
        owner_type="user",
        policies=json.dumps(policies),
        redact_mode=redact_mode,
    )


def test_mask_fields_denylist_strips_denied_keys() -> None:
    token = _token(
        [
            {
                "effect": "allow",
                "actions": ["read"],
                "resources": ["*"],
                "conditions": {},
            },
            {
                "effect": "deny",
                "actions": ["read"],
                "resources": ["deal.amount", "deal.probability"],
                "conditions": {},
            },
        ]
    )
    scope = Scope(token)
    out = scope.mask_fields(
        "Deal", {"name": "Big", "amount": 99, "probability": 0.5, "stage": "x"}
    )
    assert out == {"name": "Big", "stage": "x"}


def test_mask_fields_allowlist_keeps_only_allowed() -> None:
    token = _token(
        [
            {
                "effect": "allow",
                "actions": ["read"],
                "resources": ["deal.name", "deal.stage"],
                "conditions": {},
            },
        ]
    )
    scope = Scope(token)
    out = scope.mask_fields(
        "Deal", {"name": "Big", "amount": 99, "stage": "open"}
    )
    assert out == {"name": "Big", "stage": "open"}


def test_apply_redaction_hide_returns_none() -> None:
    token = _token(
        [{"effect": "allow", "actions": ["read"], "resources": ["*"], "conditions": {}}],
        redact_mode="hide",
    )
    scope = Scope(token)
    node = NodeData("Campaign", "c1", str(uuid.uuid4()), {"name": "Q1"})
    assert scope.apply_redaction("Campaign", node) is None


def test_apply_redaction_hint_returns_stub() -> None:
    token = _token(
        [{"effect": "allow", "actions": ["read"], "resources": ["*"], "conditions": {}}],
        redact_mode="hint",
    )
    scope = Scope(token)
    tid = str(uuid.uuid4())
    node = NodeData("Campaign", "c1", tid, {"name": "Q1"})
    stub = scope.apply_redaction("Campaign", node)
    assert stub is not None
    assert stub.id == "c1"
    assert stub.properties == {"_redacted": True}


def test_ids_condition_instance_scoping() -> None:
    token = _token(
        [
            {
                "effect": "allow",
                "actions": ["read"],
                "resources": ["deal"],
                "conditions": {"ids": ["deal-1", "deal-3"]},
            },
        ]
    )
    scope = Scope(token)
    assert scope.can_read("Deal")
    assert scope.can_read("Deal", {"id": "deal-1"})
    assert not scope.can_read("Deal", {"id": "deal-2"})


def test_deny_overrides_allow_same_resource() -> None:
    token = _token(
        [
            {
                "effect": "allow",
                "actions": ["read"],
                "resources": ["*"],
                "conditions": {},
            },
            {
                "effect": "deny",
                "actions": ["read"],
                "resources": ["deal"],
                "conditions": {},
            },
        ]
    )
    scope = Scope(token)
    assert not scope.can_read("Deal")
    assert scope.can_read("Contact")


def test_create_token_from_presets_merges() -> None:
    tid = uuid.uuid4()
    tok = create_token_from_presets(
        tid,
        "cro",
        "agent",
        ["read_all", "no_raw_content"],
        label="CRO token",
    )
    policies = json.loads(tok.policies)
    assert len(policies) == 2
    assert tok.tenant_id == tid
    assert tok.owner_id == "cro"
    assert "read_all" in PRESETS and "no_raw_content" in PRESETS


def test_create_token_from_presets_custom() -> None:
    tid = uuid.uuid4()
    tok = create_token_from_presets(
        tid,
        "x",
        "api_key",
        preset_names=["read_all", "extra"],
        custom_presets={
            "extra": [
                {
                    "effect": "allow",
                    "actions": ["write"],
                    "resources": ["note"],
                    "conditions": {},
                }
            ]
        },
    )
    policies = json.loads(tok.policies)
    assert any(p.get("resources") == ["note"] for p in policies)


def test_create_token_from_presets_unknown_raises() -> None:
    with pytest.raises(KeyError, match="nope"):
        create_token_from_presets(
            uuid.uuid4(), "a", "user", ["nope"]
        )
