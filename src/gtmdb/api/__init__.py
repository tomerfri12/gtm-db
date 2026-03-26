"""GtmDB typed entity CRUD API.

Usage via GtmDB client::

    db = GtmDB()
    await db.connect()

    lead = await db.leads.create(scope, first_name="Jane", company_name="Acme")
    lead = await db.leads.get(scope, lead.id)
    leads = await db.leads.list(scope, status="new", limit=50)
    await db.leads.update(scope, lead.id, status="qualified")
    await db.leads.delete(scope, lead.id)

    await db.relationships.create(scope, lead.id, "WORKS_AT", account.id)
"""

from gtmdb.api.accounts import AccountsAPI
from gtmdb.api.actors import ActorsAPI
from gtmdb.api.campaigns import CampaignsAPI
from gtmdb.api.communication_events import EmailCampaignAPI, EmailsAPI
from gtmdb.api.contacts import ContactsAPI
from gtmdb.api.deals import DealsAPI
from gtmdb.api.leads import LeadsAPI
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
)
from gtmdb.api.scores import ScoresAPI
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
]
