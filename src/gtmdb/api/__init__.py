"""GtmDB typed entity CRUD API.

Usage::

    from gtmdb import connect_gtmdb

    db, scope = await connect_gtmdb(api_key="gtmdb_...")

    lead = await db.leads.create(
        scope, actor_id="agent-1", first_name="Jane", company_name="Acme"
    )
    lead = await db.leads.get(scope, lead.id)
    leads = await db.leads.list(scope, status="new", limit=50)
    await db.leads.update(scope, lead.id, actor_id="agent-1", status="qualified")
    await db.leads.delete(scope, lead.id)

    await db.relationships.create(
        scope, lead.id, "WORKS_AT", account.id, reasoning="Enrichment match"
    )
"""

from gtmdb.api.accounts import AccountsAPI
from gtmdb.api.actors import ActorsAPI
from gtmdb.api.campaigns import CampaignsAPI
from gtmdb.api.communication_events import EmailCampaignAPI, EmailsAPI
from gtmdb.api.contacts import ContactsAPI
from gtmdb.api.deals import DealsAPI
from gtmdb.api.leads import LeadsAPI
from gtmdb.api.visitors import VisitorsAPI
from gtmdb.api.models import (
    Account,
    Actor,
    ActorSpec,
    Campaign,
    Contact,
    Deal,
    Email,
    EmailCampaign,
    Entity,
    Lead,
    Relationship,
    Score,
    SubscriptionEvent,
    Visitor,
)
from gtmdb.api.scores import ScoresAPI
from gtmdb.api.subscription_events import SubscriptionEventsAPI
from gtmdb.api.relationships import RelationshipsAPI

__all__ = [
    "Account",
    "AccountsAPI",
    "Actor",
    "ActorSpec",
    "ActorsAPI",
    "Campaign",
    "CampaignsAPI",
    "Contact",
    "ContactsAPI",
    "Deal",
    "DealsAPI",
    "Email",
    "EmailCampaign",
    "EmailCampaignAPI",
    "EmailsAPI",
    "Entity",
    "Lead",
    "LeadsAPI",
    "Relationship",
    "RelationshipsAPI",
    "Score",
    "ScoresAPI",
    "SubscriptionEvent",
    "SubscriptionEventsAPI",
    "Visitor",
    "VisitorsAPI",
]
