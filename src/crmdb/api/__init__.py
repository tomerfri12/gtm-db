"""CRMDB typed entity CRUD API.

Usage via CrmDB client::

    db = CrmDB()
    await db.connect()

    lead = await db.leads.create(scope, first_name="Jane", company_name="Acme")
    lead = await db.leads.get(scope, lead.id)
    leads = await db.leads.list(scope, status="new", limit=50)
    await db.leads.update(scope, lead.id, status="qualified")
    await db.leads.delete(scope, lead.id)

    await db.relationships.create(scope, lead.id, "WORKS_AT", account.id)
"""

from crmdb.api.accounts import AccountsAPI
from crmdb.api.actors import ActorsAPI
from crmdb.api.campaigns import CampaignsAPI
from crmdb.api.communication_events import EmailCampaignAPI, EmailsAPI
from crmdb.api.contacts import ContactsAPI
from crmdb.api.deals import DealsAPI
from crmdb.api.leads import LeadsAPI
from crmdb.api.models import (
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
from crmdb.api.scores import ScoresAPI
from crmdb.api.relationships import RelationshipsAPI

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
