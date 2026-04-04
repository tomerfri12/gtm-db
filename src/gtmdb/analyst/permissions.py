"""Layer 1: render scope policies into analyst system-prompt text."""

from __future__ import annotations

from gtmdb.resources import RESOURCE_BY_NAME


def format_permissions(scope: object) -> str:
    """Render a Scope's policies as a human-readable permission summary.

    Includes the exact ClickHouse column names for denied resources so the
    agent knows precisely which columns to avoid.
    """
    policies: list[dict] = getattr(scope, "policies", [])
    if not policies:
        return ""

    allowed_resources: set[str] = set()
    denied_resources: set[str] = set()
    denied_fields: set[str] = set()

    for policy in policies:
        actions = policy.get("actions", [])
        effect = policy.get("effect", "")
        resources: list[str] = policy.get("resources", [])

        for r in resources:
            r_lower = r.lower()
            if r_lower == "*":
                if effect == "allow":
                    allowed_resources.add("*")
                else:
                    denied_resources.add("*")
            elif "." in r_lower:
                node, field = r_lower.split(".", 1)
                if effect == "deny" and "read" in actions:
                    denied_fields.add(f"{node}.{field}")
            else:
                if effect == "allow":
                    allowed_resources.add(r)
                else:
                    denied_resources.add(r)

    if not allowed_resources and not denied_resources and not denied_fields:
        return ""

    lines: list[str] = ["## Your permissions — HARD RULES, not suggestions"]
    lines.append(
        "These restrictions are enforced. Violating them will cause your query "
        "to be rejected. If a user asks about a denied resource, tell them you "
        "do not have access to that data — do not attempt to query it.\n"
    )

    if "*" in allowed_resources:
        lines.append("You have read access to ALL resources.")
    elif allowed_resources:
        lines.append(f"Allowed resources: {', '.join(sorted(allowed_resources))}.")

    if denied_resources - {"*"}:
        denied_sorted = sorted(denied_resources - {"*"})
        lines.append("\nDENIED resources — do NOT query these in SQL or Cypher:")
        for resource in denied_sorted:
            schema = RESOURCE_BY_NAME.get(resource)
            cols = list(schema.columns) if schema else []
            col_str = f" (columns: {', '.join(cols)})" if cols else ""
            lines.append(f"  - {resource}{col_str}")

    if denied_fields:
        lines.append(f"\nDenied fields: {', '.join(sorted(denied_fields))}.")

    lines.append(
        "\nIf the user asks about a denied resource, respond: "
        "'I don't have access to [resource] data in this context.'"
    )

    return "\n".join(lines)
