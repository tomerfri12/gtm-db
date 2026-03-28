"""EmailCampaignAPI -- EmailCampaign CRUD + batch create with Email artifacts."""

from __future__ import annotations

from typing import Any

from gtmdb.api._base import EntityAPI
from gtmdb.api._common import optional_reasoning, require_non_empty_str
from gtmdb.api.communication_events.emails import EmailsAPI
from gtmdb.api.models import EmailCampaign
from gtmdb.scope import Scope
from gtmdb.types import EdgeData


class EmailCampaignAPI(EntityAPI[EmailCampaign]):
    _label = "EmailCampaign"
    _entity_cls = EmailCampaign

    async def create_with_artifacts(
        self,
        scope: Scope,
        *,
        actor_id: str,
        emails: list[dict[str, Any]],
        lead_ids: list[str] | None = None,
        has_email_reasoning: str | None = None,
        sourced_from_reasoning: str | None = None,
        **campaign_kwargs: Any,
    ) -> dict[str, Any]:
        """Persist an EmailCampaign plus Email artifact nodes and edges (does not send mail).

        Creates ``HAS_EMAIL`` (EmailCampaign → Email) and optional ``SOURCED_FROM``
        (Lead → EmailCampaign). ``emails`` lists partial Email field dicts per step
        (subject, body, from_name, …).
        """
        if not emails:
            raise ValueError("emails must be a non-empty list")

        aid = require_non_empty_str(actor_id, "actor_id")
        campaign_kwargs = dict(campaign_kwargs)
        campaign_kwargs["channel"] = "email"
        campaign_kwargs["actor_id"] = aid

        ec = await self.create(scope, **campaign_kwargs)

        emails_api = EmailsAPI(self._graph)
        email_domain = emails_api._domain_fields
        email_ids: list[str] = []
        email_edge_reason = optional_reasoning(has_email_reasoning)

        lead_list = [(x or "").strip() for x in (lead_ids or []) if (x or "").strip()]
        if lead_list:
            sf = require_non_empty_str(
                sourced_from_reasoning, "sourced_from_reasoning"
            )
        else:
            sf = None

        for i, spec in enumerate(emails):
            row = {k: v for k, v in spec.items() if k in email_domain and v is not None}
            row.setdefault("sequence_number", i + 1)
            row["actor_id"] = aid
            email = await emails_api.create(scope, **row)
            email_ids.append(email.id)
            await self._graph.create_edge(
                scope,
                EdgeData(
                    "HAS_EMAIL", ec.id, email.id, reasoning=email_edge_reason
                ),
            )

        linked = 0
        for lid in lead_list:
            await self._graph.create_edge(
                scope,
                EdgeData("SOURCED_FROM", lid, ec.id, reasoning=sf),
            )
            linked += 1

        return {
            "campaign": ec,
            "email_ids": email_ids,
            "linked_lead_count": linked,
        }
