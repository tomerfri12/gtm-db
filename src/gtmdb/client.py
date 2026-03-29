"""GtmDB -- the top-level client that consumers interact with.

Manages internal graph and auxiliary stores and exposes a typed API.
Consumers use :func:`gtmdb.connect.connect_gtmdb` and never import drivers
or backend-specific adapters.
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from gtmdb.api_keys import ApiKeysManager
    from gtmdb.key_store import KeyStore

from gtmdb.api.accounts import AccountsAPI
from gtmdb.api.actors import ActorsAPI
from gtmdb.api.campaigns import CampaignsAPI
from gtmdb.api.channels import ChannelsAPI
from gtmdb.api.communication_events import EmailCampaignAPI, EmailsAPI
from gtmdb.api.contacts import ContactsAPI
from gtmdb.api.content import ContentAPI
from gtmdb.api.deals import DealsAPI
from gtmdb.api.leads import LeadsAPI
from gtmdb.api.products import ProductsAPI
from gtmdb.api.scores import ScoresAPI
from gtmdb.api.relationships import RelationshipsAPI
from gtmdb.api._common import optional_reasoning, require_non_empty_str
from gtmdb.config import GtmdbSettings
from gtmdb.graph.adapter import GraphAdapter
from gtmdb.scope import Scope
from gtmdb.types import EdgeData, NodeData


class GtmDB:
    """High-level async client for the GtmDB managed service."""

    def __init__(self, settings: GtmdbSettings | None = None) -> None:
        self._settings = settings or GtmdbSettings()
        self._graph = GraphAdapter(self._settings)

        self._leads: LeadsAPI | None = None
        self._scores: ScoresAPI | None = None
        self._accounts: AccountsAPI | None = None
        self._contacts: ContactsAPI | None = None
        self._deals: DealsAPI | None = None
        self._campaigns: CampaignsAPI | None = None
        self._channels: ChannelsAPI | None = None
        self._products: ProductsAPI | None = None
        self._content: ContentAPI | None = None
        self._emails: EmailsAPI | None = None
        self._email_campaigns: EmailCampaignAPI | None = None
        self._relationships: RelationshipsAPI | None = None
        self._actors: ActorsAPI | None = None
        self._api_keys: ApiKeysManager | None = None
        self._key_store: KeyStore | None = None

    async def connect(self) -> None:
        """Verify connectivity and bootstrap schemas. Call once on startup."""
        uri = (self._settings.neo4j_uri or "").strip()
        if not uri:
            raise ValueError(
                "Neo4j is not configured. Set GTMDB_NEO4J_URI (and user/password) "
                "in the environment or .env file."
            )
        await self._graph.verify_connectivity()
        await self._graph.bootstrap_schema()
        if self._settings.key_store_url:
            ks = self._get_key_store()
            await ks.init_db()

    async def close(self) -> None:
        """Shut down all connections. Call on application teardown."""
        await self._graph.close()
        if self._key_store is not None:
            await self._key_store.close()
            self._key_store = None
            self._api_keys = None

    def _get_key_store(self) -> KeyStore:
        if self._key_store is None:
            dsn = self._settings.key_store_url
            if not dsn:
                raise RuntimeError(
                    "Key store not configured. Set GTMDB_KEY_STORE_URL to a "
                    "Postgres DSN (e.g. postgresql+asyncpg://user:pass@host/db)."
                )
            from gtmdb.key_store import KeyStore
            self._key_store = KeyStore(dsn)
        return self._key_store

    @property
    def api_keys(self) -> ApiKeysManager:
        if self._api_keys is None:
            from gtmdb.api_keys import ApiKeysManager
            self._api_keys = ApiKeysManager(self._get_key_store())
        return self._api_keys

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
    def channels(self) -> ChannelsAPI:
        if self._channels is None:
            self._channels = ChannelsAPI(self._graph)
        return self._channels

    @property
    def products(self) -> ProductsAPI:
        if self._products is None:
            self._products = ProductsAPI(self._graph)
        return self._products

    @property
    def content(self) -> ContentAPI:
        if self._content is None:
            self._content = ContentAPI(self._graph)
        return self._content

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

    async def create_node(
        self,
        scope: Scope,
        node: NodeData,
        *,
        actor_id: str,
        reasoning: str | None = None,
    ) -> NodeData:
        """Create a node with tenant id, ensure ``Actor``, set ``created_by_actor_id``, ``CREATED_BY`` edge.

        For label ``Actor``, skips ``created_by_actor_id`` / ``CREATED_BY`` (use :meth:`actors.create` for MERGE).
        """
        aid = require_non_empty_str(actor_id, "actor_id")
        r = optional_reasoning(reasoning)
        await self.actors.ensure(scope, aid)
        props = dict(node.properties)
        if node.label == "Actor":
            return await self._graph.create_node(
                scope,
                NodeData(node.label, node.id, node.tenant_id, props),
            )
        props["created_by_actor_id"] = aid
        result = await self._graph.create_node(
            scope,
            NodeData(node.label, node.id, node.tenant_id, props),
        )
        await self._graph.create_edge(
            scope,
            EdgeData("CREATED_BY", aid, result.id, reasoning=r),
        )
        return result

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

    async def explore_subgraph(
        self,
        scope: Scope,
        center_id: str,
        *,
        max_depth: int = 1,
        nodes_per_type_cap: int = 10,
        mode: str = "compact",
    ) -> dict[str, Any]:
        """Return nodes, edges, and truncation info for ``center_id`` within ``max_depth`` hops.

        ``mode="compact"`` (default) returns node IDs grouped by label.
        ``mode="full"`` returns full node properties.
        """
        return await self._graph.explore_subgraph(
            scope,
            center_id,
            max_depth=max_depth,
            nodes_per_type_cap=nodes_per_type_cap,
            mode=mode,
        )
