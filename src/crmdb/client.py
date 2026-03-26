"""CrmDB -- the top-level client that consumers interact with.

Manages internal graph and auxiliary stores and exposes a typed API.
Consumers use :func:`crmdb.connect.connect_crmdb` and never import drivers
or backend-specific adapters.
"""

from __future__ import annotations

from typing import Any

from crmdb.api.accounts import AccountsAPI
from crmdb.api.actors import ActorsAPI
from crmdb.api.campaigns import CampaignsAPI
from crmdb.api.communication_events import EmailCampaignAPI, EmailsAPI
from crmdb.api.contacts import ContactsAPI
from crmdb.api.deals import DealsAPI
from crmdb.api.leads import LeadsAPI
from crmdb.api.scores import ScoresAPI
from crmdb.api.relationships import RelationshipsAPI
from crmdb.config import CrmdbSettings
from crmdb.graph.adapter import GraphAdapter
from crmdb.scope import Scope
from crmdb.types import EdgeData, NodeData


class CrmDB:
    """High-level async client for the CRMDB managed service."""

    def __init__(self, settings: CrmdbSettings | None = None) -> None:
        self._settings = settings or CrmdbSettings()
        self._graph = GraphAdapter(self._settings)

        self._leads: LeadsAPI | None = None
        self._scores: ScoresAPI | None = None
        self._accounts: AccountsAPI | None = None
        self._contacts: ContactsAPI | None = None
        self._deals: DealsAPI | None = None
        self._campaigns: CampaignsAPI | None = None
        self._emails: EmailsAPI | None = None
        self._email_campaigns: EmailCampaignAPI | None = None
        self._relationships: RelationshipsAPI | None = None
        self._actors: ActorsAPI | None = None

    async def connect(self) -> None:
        """Verify connectivity and bootstrap schemas. Call once on startup."""
        await self._graph.verify_connectivity()
        await self._graph.bootstrap_schema()

    async def close(self) -> None:
        """Shut down all connections. Call on application teardown."""
        await self._graph.close()

    # ------------------------------------------------------------------
    # Typed entity APIs
    # ------------------------------------------------------------------

    @property
    def leads(self) -> LeadsAPI:
        if self._leads is None:
            self._leads = LeadsAPI(self._graph)
        return self._leads

    @property
    def scores(self) -> ScoresAPI:
        if self._scores is None:
            self._scores = ScoresAPI(self._graph)
        return self._scores

    @property
    def accounts(self) -> AccountsAPI:
        if self._accounts is None:
            self._accounts = AccountsAPI(self._graph)
        return self._accounts

    @property
    def contacts(self) -> ContactsAPI:
        if self._contacts is None:
            self._contacts = ContactsAPI(self._graph)
        return self._contacts

    @property
    def deals(self) -> DealsAPI:
        if self._deals is None:
            self._deals = DealsAPI(self._graph)
        return self._deals

    @property
    def campaigns(self) -> CampaignsAPI:
        if self._campaigns is None:
            self._campaigns = CampaignsAPI(self._graph)
        return self._campaigns

    @property
    def emails(self) -> EmailsAPI:
        if self._emails is None:
            self._emails = EmailsAPI(self._graph)
        return self._emails

    @property
    def email_campaigns(self) -> EmailCampaignAPI:
        if self._email_campaigns is None:
            self._email_campaigns = EmailCampaignAPI(self._graph)
        return self._email_campaigns

    @property
    def relationships(self) -> RelationshipsAPI:
        if self._relationships is None:
            self._relationships = RelationshipsAPI(self._graph)
        return self._relationships

    @property
    def actors(self) -> ActorsAPI:
        if self._actors is None:
            self._actors = ActorsAPI(self._graph)
        return self._actors

    # ------------------------------------------------------------------
    # Graph operations (low-level)
    # ------------------------------------------------------------------

    async def create_node(self, scope: Scope, node: NodeData) -> NodeData:
        return await self._graph.create_node(scope, node)

    async def create_edge(self, scope: Scope, edge: EdgeData) -> EdgeData:
        return await self._graph.create_edge(scope, edge)

    async def get_node(
        self, scope: Scope, label: str, node_id: str
    ) -> NodeData | None:
        return await self._graph.get_node(scope, label, node_id)

    async def get_neighbors(
        self,
        scope: Scope,
        node_id: str,
        edge_types: list[str] | None = None,
        direction: str = "both",
    ) -> list[dict]:
        return await self._graph.get_neighbors(
            scope, node_id, edge_types, direction
        )

    async def execute_cypher(
        self, scope: Scope, query: str, params: dict | None = None
    ) -> list[dict]:
        """Run raw Cypher. Tenant isolation is automatically enforced."""
        return await self._graph.execute(scope, query, params)

    # ------------------------------------------------------------------
    # Rich traversals (Phase 3)
    # ------------------------------------------------------------------

    async def entity_360(
        self,
        scope: Scope,
        anchor_label: str,
        anchor_id: str,
        max_depth: int = 2,
    ) -> dict[str, Any]:
        """Center node plus neighbors up to ``max_depth`` hops (masked)."""
        return await self._graph.entity_360(
            scope, anchor_label, anchor_id, max_depth
        )

    async def timeline(
        self, scope: Scope, entity_id: str, limit: int = 50
    ) -> list[dict[str, Any]]:
        """Activity nodes around an entity, newest first (masked)."""
        return await self._graph.timeline(scope, entity_id, limit)

    async def pipeline(
        self, scope: Scope, stage: str | None = None, limit: int = 100
    ) -> list[NodeData]:
        """Deals in the tenant, optional ``stage`` filter (masked)."""
        return await self._graph.pipeline(scope, stage, limit)

    async def campaign_attribution(
        self,
        scope: Scope,
        deal_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Campaign–deal pairs linked by ``INFLUENCED`` (masked)."""
        return await self._graph.campaign_attribution(scope, deal_id, limit)

    async def path_finding(
        self,
        scope: Scope,
        from_id: str,
        to_id: str,
        max_hops: int = 15,
    ) -> dict[str, Any] | None:
        """Shortest path between two node ids (masked nodes)."""
        return await self._graph.path_finding(scope, from_id, to_id, max_hops)

    async def search(
        self, scope: Scope, query: str, limit: int = 25
    ) -> list[dict[str, Any]]:
        """Full-text search on indexed entity fields (masked)."""
        return await self._graph.search(scope, query, limit)
