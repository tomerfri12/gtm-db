"""Generic EntityAPI[T] base class with CRUD operations backed by Neo4j."""

from __future__ import annotations

import dataclasses
from datetime import datetime, timezone
from typing import Any, Generic, TypeVar

from gtmdb.api.models import Entity
from gtmdb.graph.adapter import GraphAdapter
from gtmdb.scope import Scope
from gtmdb.types import EdgeData, NodeData

T = TypeVar("T", bound=Entity)

_SYSTEM_FIELDS = frozenset(
    {
        "id",
        "tenant_id",
        "created_by_actor_id",
        "updated_by_actor_id",
    }
)


class EntityAPI(Generic[T]):
    """Typed CRUD API for a single CRM entity type.

    Subclasses set ``_label`` (Neo4j node label) and ``_entity_cls``
    (the typed dataclass). All graph operations go through
    ``GraphAdapter`` with full scope enforcement.
    """

    _label: str
    _entity_cls: type[T]

    def __init__(self, graph: GraphAdapter) -> None:
        self._graph = graph
        self._valid_fields = {
            f.name for f in dataclasses.fields(self._entity_cls)
        }
        self._domain_fields = self._valid_fields - _SYSTEM_FIELDS

    # -- Internal helpers ----------------------------------------------------

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _to_node_data(self, props: dict[str, Any]) -> NodeData:
        return NodeData(label=self._label, id="", tenant_id="", properties=props)

    def _from_node_data(self, node: NodeData) -> T:
        kwargs: dict[str, Any] = {"id": node.id, "tenant_id": node.tenant_id}
        for k, v in node.properties.items():
            if k in self._valid_fields:
                kwargs[k] = v
        return self._entity_cls(**kwargs)

    def _from_raw_props(self, props: dict[str, Any], scope: Scope) -> T | None:
        """Convert a raw Cypher properties dict to a typed entity, applying scope masking."""
        nid = str(props.get("id", ""))
        tid = str(props.get("tenant_id", scope.tenant_id))

        if not scope.can_read(self._label, {"id": nid}):
            return None

        domain_props = {
            k: v for k, v in props.items() if k not in ("id", "tenant_id")
        }
        masked = scope.mask_fields(self._label, domain_props)
        node = NodeData(self._label, nid, tid, masked)
        return self._from_node_data(node)

    # -- CRUD ----------------------------------------------------------------

    async def create(self, scope: Scope, **kwargs: Any) -> T:
        """Create a new entity. Pass domain fields as keyword arguments.

        ``id`` and ``tenant_id`` are set automatically. ``created_at``
        and ``updated_at`` default to now if not provided.
        """
        now = self._now_iso()
        kwargs.setdefault("created_at", now)
        kwargs.setdefault("updated_at", now)

        props = {
            k: v for k, v in kwargs.items()
            if k in self._domain_fields and v is not None
        }

        result = await self._graph.create_node(scope, self._to_node_data(props))
        await self._graph.create_edge(
            scope,
            EdgeData("CREATED_BY", scope.owner_id, result.id),
        )
        return self._from_node_data(result)

    async def get(self, scope: Scope, entity_id: str) -> T | None:
        """Get an entity by id. Returns ``None`` if not found or not readable."""
        node = await self._graph.get_node(scope, self._label, entity_id)
        if node is None:
            return None
        return self._from_node_data(node)

    async def list(
        self,
        scope: Scope,
        *,
        limit: int = 50,
        offset: int = 0,
        **filters: Any,
    ) -> list[T]:
        """List entities with optional property-equality filters.

        Filters are AND-ed together. Only keys matching valid entity
        fields are used; unknown keys are silently ignored.
        """
        if not scope.can_read(self._label):
            return []

        params: dict[str, Any] = {}
        where_parts: list[str] = []
        for key, value in filters.items():
            if key not in self._domain_fields:
                continue
            pname = f"f_{key}"
            where_parts.append(f"n.{key} = ${pname}")
            params[pname] = value

        where_clause = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
        lim = max(1, min(int(limit), 500))
        off = max(0, int(offset))

        query = (
            f"MATCH (n:{self._label} {{tenant_id: $tenant_id}}) "
            f"{where_clause} "
            f"RETURN properties(n) AS props "
            f"ORDER BY n.created_at DESC "
            f"SKIP {off} LIMIT {lim}"
        )

        records = await self._graph.execute(scope, query, params)
        results: list[T] = []
        for rec in records:
            entity = self._from_raw_props(dict(rec["props"]), scope)
            if entity is not None:
                results.append(entity)
        return results

    async def update(self, scope: Scope, entity_id: str, **kwargs: Any) -> T | None:
        """Partially update an entity (PATCH semantics).

        Only the provided keyword arguments are changed; other fields
        remain untouched. ``updated_at`` is set automatically.
        Returns ``None`` if the entity does not exist.
        """
        if not scope.can_write(self._label):
            raise PermissionError(
                f"Token {scope.owner_id} cannot write {self._label}"
            )

        updates = {k: v for k, v in kwargs.items() if k in self._domain_fields}
        if not updates:
            return await self.get(scope, entity_id)

        now = self._now_iso()
        updates["updated_at"] = now
        updates["updated_by_actor_id"] = scope.owner_id

        set_parts: list[str] = []
        params: dict[str, Any] = {"id": entity_id}
        for key, value in updates.items():
            if value is None:
                set_parts.append(f"n.{key} = null")
            else:
                pname = f"s_{key}"
                set_parts.append(f"n.{key} = ${pname}")
                params[pname] = value

        query = (
            f"MATCH (n:{self._label} {{id: $id, tenant_id: $tenant_id}}) "
            f"SET {', '.join(set_parts)} "
            f"RETURN properties(n) AS props"
        )

        records = await self._graph.execute(scope, query, params)
        if not records:
            return None
        await self._graph.create_edge(
            scope,
            EdgeData("UPDATED_BY", scope.owner_id, entity_id, {"at": now}),
        )
        return self._from_raw_props(dict(records[0]["props"]), scope)

    async def delete(self, scope: Scope, entity_id: str) -> bool:
        """Delete an entity and all its relationships.

        Returns ``True`` if the entity existed and was removed.
        """
        if not scope.can_write(self._label):
            raise PermissionError(
                f"Token {scope.owner_id} cannot write {self._label}"
            )

        query = (
            f"MATCH (n:{self._label} {{id: $id, tenant_id: $tenant_id}}) "
            f"DETACH DELETE n "
            f"RETURN true AS deleted"
        )
        records = await self._graph.execute(scope, query, {"id": entity_id})
        return len(records) > 0
