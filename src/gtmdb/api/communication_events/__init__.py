"""Typed APIs for communication events (Email, EmailCampaign, …)."""

from gtmdb.api.communication_events.email_campaigns import EmailCampaignAPI
from gtmdb.api.communication_events.emails import EmailsAPI

__all__ = ["EmailCampaignAPI", "EmailsAPI"]
