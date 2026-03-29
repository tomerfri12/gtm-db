"""Request activity logging (Postgres)."""

from __future__ import annotations

import asyncio
import json
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from gtmdb.api_keys import get_request_scope, key_id_from_raw_for_log, set_request_scope

_MAX_BODY = 65536
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.I,
)


def _normalize_owner_type(ot: str | None) -> str | None:
    if ot is None:
        return None
    if ot == "agent":
        return "actor"
    return ot


def _infer_route_meta(method: str, path: str) -> tuple[str | None, str | None, str | None]:
    """Return (action, entity_type, entity_id) best-effort from path."""
    path = path.split("?", 1)[0]
    parts = [p for p in path.split("/") if p]
    if not parts:
        return None, None, None
    if parts[0] != "v1":
        if parts[0] == "health":
            return "health", "health", None
        return None, None, None

    rest = parts[1:]
    if not rest:
        return None, None, None

    seg0 = rest[0]
    if seg0 == "search":
        return "search", "search", None
    if seg0 == "schema":
        return "schema", "schema", None
    if seg0 == "admin":
        sub = rest[1] if len(rest) > 1 else None
        if sub == "keys":
            if len(rest) >= 4 and rest[3] == "rotate":
                return "rotate_key", "admin", rest[2]
            if len(rest) >= 3:
                return "admin_key_op", "admin", rest[2]
            amap = {"GET": "list_keys", "POST": "create_key"}
            return amap.get(method, "admin"), "admin", None
        return "admin", "admin", None
    if seg0 == "activity-log":
        return "list_activity", "activity", None
    if seg0 == "entities" and len(rest) >= 3:
        eid = rest[1]
        if rest[2] == "explore":
            return "explore", "entity", eid if _UUID_RE.match(eid) else None
        return None, "entity", eid if _UUID_RE.match(eid) else None

    resource = seg0
    if len(rest) == 1:
        amap = {"GET": "list", "POST": "create"}
        return amap.get(method), resource, None
    tail = rest[1]
    if _UUID_RE.match(tail):
        amap = {"GET": "read", "PATCH": "update", "DELETE": "delete"}
        return amap.get(method), resource, tail
    return None, resource, None


def _extract_reasoning(
    body: bytes,
    content_type: str | None,
    query_params: Any,
    headers: Any,
) -> str | None:
    r = headers.get("x-gtmdb-reason") or headers.get("X-Gtmdb-Reason")
    if r and str(r).strip():
        return str(r).strip()[:8000]
    q = query_params.get("reasoning")
    if q and str(q).strip():
        return str(q).strip()[:8000]
    if not body or not (content_type or "").lower().startswith("application/json"):
        return None
    try:
        obj = json.loads(body.decode("utf-8", errors="replace"))
    except (json.JSONDecodeError, UnicodeError):
        return None
    if isinstance(obj, dict):
        val = obj.get("reasoning")
        if val is not None and str(val).strip():
            return str(val).strip()[:8000]
    return None


def _bearer_raw(request: Request) -> str | None:
    h = request.headers.get("authorization") or request.headers.get("Authorization")
    if not h:
        return None
    parts = h.split(None, 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    return parts[1].strip() or None


def _error_detail(response: Response) -> str | None:
    if response.status_code < 400:
        return None
    body = getattr(response, "body", None)
    if body is None:
        return None
    if not isinstance(body, (bytes, bytearray)):
        return None
    if len(body) > 4096:
        body = bytes(body[:4096])
    try:
        text = body.decode("utf-8", errors="replace")
    except Exception:
        return None
    try:
        obj = json.loads(text)
        if isinstance(obj, dict) and "detail" in obj:
            d = obj["detail"]
            if isinstance(d, str):
                return d[:2000]
            return str(d)[:2000]
    except json.JSONDecodeError:
        pass
    return text[:2000]


class ActivityLogMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Any) -> Response:
        started = time.perf_counter()
        method = request.method.upper()
        path = request.url.path
        ip = request.client.host if request.client else None

        body = b""
        if method in ("POST", "PATCH", "PUT"):
            body = await request.body()
            if len(body) > _MAX_BODY:
                body = body[:_MAX_BODY]

        ct = request.headers.get("content-type")

        async def receive() -> dict[str, Any]:
            return {"type": "http.request", "body": body, "more_body": False}

        wrapped = Request(request.scope, receive)

        response = await call_next(wrapped)

        duration_ms = int((time.perf_counter() - started) * 1000)
        action, entity_type, entity_id = _infer_route_meta(method, path)
        reasoning = _extract_reasoning(
            body, ct, wrapped.query_params, wrapped.headers
        )
        err = _error_detail(response)

        scope = get_request_scope()
        raw = _bearer_raw(wrapped)
        if scope is not None:
            tid = scope.tenant_id
            oid = scope.owner_id
            otype = _normalize_owner_type(scope.owner_type)
            kid = scope.key_id or key_id_from_raw_for_log(raw or "")
        else:
            tid = None
            oid = None
            otype = None
            kid = key_id_from_raw_for_log(raw or "")

        row = {
            "id": uuid.uuid4(),
            "timestamp": datetime.now(timezone.utc),
            "tenant_id": uuid.UUID(tid) if tid else None,
            "owner_type": otype,
            "owner_id": oid,
            "key_id": kid,
            "method": method,
            "path": path[:2048],
            "status_code": response.status_code,
            "action": action,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "reasoning": reasoning,
            "duration_ms": duration_ms,
            "error_detail": err,
            "ip_address": ip,
        }

        settings = getattr(request.app.state, "gtmdb_settings", None)
        db = getattr(request.app.state, "db", None)
        if (
            settings
            and getattr(settings, "key_store_url", None)
            and db is not None
        ):

            async def _persist() -> None:
                try:
                    ks = db._get_key_store()
                    await ks.insert_activity_log(row)
                except Exception:
                    pass

            asyncio.create_task(_persist())

        set_request_scope(None)
        return response
