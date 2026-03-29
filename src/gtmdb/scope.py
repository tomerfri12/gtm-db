from __future__ import annotations

import json
from typing import TYPE_CHECKING

from gtmdb.types import NodeData

if TYPE_CHECKING:
    from gtmdb.tokens import AccessToken


class Scope:
    """Universal, engine-agnostic policy evaluator.

    Supports type- and field-level read/write checks, ``ids`` conditions
    for instance scoping, field masking (denylist / allowlist), and
    redaction modes for unreadable nodes (hint vs hide).
    """

    def __init__(self, token: AccessToken) -> None:
        self._token = token
        self._policies: list[dict] = json.loads(token.policies)

    @property
    def tenant_id(self) -> str:
        return str(self._token.tenant_id)

    @property
    def owner_id(self) -> str:
        return self._token.owner_id

    @property
    def owner_type(self) -> str:
        return self._token.owner_type

    @property
    def key_id(self) -> str | None:
        return self._token.key_id

    @property
    def redact_mode(self) -> str:
        return self._token.redact_mode

    @property
    def policies(self) -> list[dict]:
        return self._policies

    def can_read(self, resource: str, instance: dict | None = None) -> bool:
        """Return True if the token allows reading this resource (and instance if given)."""
        allows, denies = self._matching_policies("read", resource, instance)
        if self._has_full_deny(denies, resource, instance):
            return False
        return len(allows) > 0

    def can_write(self, resource: str, instance: dict | None = None) -> bool:
        """Return True if the token allows writing this resource (and instance if given)."""
        allows, denies = self._matching_policies("write", resource, instance)
        if self._has_full_deny(denies, resource, instance):
            return False
        return len(allows) > 0

    def mask_fields(self, resource: str, fields: dict) -> dict:
        """Apply field-level read policy: denylist (strip denied keys) or allowlist.

        Denylist mode applies when there is a type-level read allow (``*`` or exact
        resource). Field keys listed in deny policies are removed.

        Allowlist mode applies when there are only field-level read allows
        (e.g. ``deal.name``): only those property keys are kept.
        """
        resource_lower = resource.lower()
        denied_fields: set[str] = set()
        allowed_fields: set[str] = set()
        has_type_level_allow = False

        for policy in self._policies:
            if "read" not in policy.get("actions", []):
                continue
            effect = policy.get("effect")
            for r in policy.get("resources", []):
                r_lower = r.lower()
                if r_lower == "*":
                    if effect == "allow":
                        has_type_level_allow = True
                elif r_lower == resource_lower:
                    if effect == "allow":
                        has_type_level_allow = True
                elif r_lower.startswith(f"{resource_lower}."):
                    field_name = r_lower[len(resource_lower) + 1 :]
                    if effect == "deny":
                        denied_fields.add(field_name.lower())
                    elif effect == "allow":
                        allowed_fields.add(field_name.lower())

        if has_type_level_allow:
            return {
                k: v for k, v in fields.items() if k.lower() not in denied_fields
            }
        if allowed_fields:
            return {k: v for k, v in fields.items() if k.lower() in allowed_fields}
        return dict(fields)

    def apply_redaction(self, label: str, node: NodeData) -> NodeData | None:
        """When a node is not readable, return a stub (hint) or None (hide)."""
        if self.redact_mode == "hide":
            return None
        return NodeData(
            label=label,
            id=node.id,
            tenant_id=node.tenant_id,
            properties={"_redacted": True},
        )

    @staticmethod
    def _policy_matches_resource(resources: list[str], resource_lower: str) -> bool:
        for r in resources:
            r = r.lower()
            if r == "*" or r == resource_lower:
                return True
            if r.startswith(f"{resource_lower}."):
                return True
        return False

    def _matching_policies(
        self,
        action: str,
        resource: str,
        instance: dict | None = None,
    ) -> tuple[list[dict], list[dict]]:
        """Find allow/deny policies matching action + resource (+ optional instance)."""
        resource_lower = resource.lower()
        allows: list[dict] = []
        denies: list[dict] = []

        for policy in self._policies:
            if action not in policy.get("actions", []):
                continue
            resources = [r for r in policy.get("resources", [])]
            resources_lower = [r.lower() for r in resources]
            if not self._policy_matches_resource(resources_lower, resource_lower):
                continue

            conditions = policy.get("conditions") or {}
            ids_cond = conditions.get("ids")
            if ids_cond is not None and instance is not None:
                inst_id = instance.get("id")
                if inst_id not in ids_cond:
                    continue

            target = allows if policy.get("effect") == "allow" else denies
            target.append(policy)

        return allows, denies

    def _has_full_deny(
        self,
        denies: list[dict],
        resource: str,
        instance: dict | None = None,
    ) -> bool:
        """True if a deny blocks the whole resource (not field-only)."""
        resource_lower = resource.lower()
        for policy in denies:
            conditions = policy.get("conditions") or {}
            ids_cond = conditions.get("ids")
            if ids_cond is not None:
                if instance is None:
                    continue
                if instance.get("id") not in ids_cond:
                    continue

            for r in policy.get("resources", []):
                r = r.lower()
                if r == "*":
                    return True
                if r == resource_lower:
                    return True
        return False
