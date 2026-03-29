"""Unit tests for the API key system (key format, hashing, resolve logic)."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from gtmdb.api_keys import ApiKeysManager, _generate_key, _hash_key, _parse_key
from gtmdb.presets import create_token_from_presets
from gtmdb.scope import Scope


def _mgr_with_admin(store: AsyncMock) -> ApiKeysManager:
    mgr = ApiKeysManager(store)
    tok = create_token_from_presets(
        "00000000-0000-4000-8000-000000000001",
        "admin",
        "admin",
        ["full_access"],
        key_id="admin",
    )
    mgr.bind_scope(Scope(tok))
    return mgr



class TestKeyGeneration:
    def test_format(self):
        raw, key_id = _generate_key()
        assert raw.startswith("gtmdb_")
        parts = raw.split("_", 2)
        assert len(parts) == 3
        assert parts[0] == "gtmdb"
        assert parts[1] == key_id
        assert len(parts[2]) > 0

    def test_uniqueness(self):
        keys = {_generate_key()[0] for _ in range(100)}
        assert len(keys) == 100

    def test_hash(self):
        raw, _ = _generate_key()
        h = _hash_key(raw)
        assert h == hashlib.sha256(raw.encode()).hexdigest()
        assert len(h) == 64


class TestParseKey:
    def test_valid(self):
        kid = _parse_key("gtmdb_abc123_secretsecretsecretsecret")
        assert kid == "abc123"

    def test_missing_prefix(self):
        with pytest.raises(ValueError, match="Invalid API key format"):
            _parse_key("badprefix_abc123_secret")

    def test_missing_secret(self):
        with pytest.raises(ValueError, match="Invalid API key format"):
            _parse_key("gtmdb_abc123_")

    def test_no_underscores(self):
        with pytest.raises(ValueError, match="Invalid API key format"):
            _parse_key("gtmdb")

    def test_only_one_underscore(self):
        with pytest.raises(ValueError, match="Invalid API key format"):
            _parse_key("gtmdb_nosecret")


class TestResolve:
    @pytest.fixture
    def store(self):
        return AsyncMock()

    @pytest.fixture
    def mgr(self, store):
        return ApiKeysManager(store)

    @pytest.mark.asyncio
    async def test_valid_key(self, mgr, store):
        raw_key = "gtmdb_testid_secretsecretsecretsecret"
        key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
        store.get_by_key_id.return_value = {
            "key_id": "testid",
            "key_hash": key_hash,
            "tenant_id": uuid.UUID("00000000-0000-4000-8000-000000000001"),
            "owner_id": "sdr-agent",
            "owner_type": "agent",
            "label": "test",
            "policies": json.dumps([{"effect": "allow", "actions": ["read", "write"], "resources": ["*"], "conditions": {}}]),
            "is_active": True,
            "expires_at": None,
        }

        scope = await mgr.resolve(raw_key)
        assert scope.owner_id == "sdr-agent"
        assert scope.tenant_id == "00000000-0000-4000-8000-000000000001"
        assert scope.can_read("Lead")
        assert scope.can_write("Lead")

    @pytest.mark.asyncio
    async def test_wrong_hash(self, mgr, store):
        store.get_by_key_id.return_value = {
            "key_id": "testid",
            "key_hash": "wrong_hash",
            "tenant_id": uuid.UUID("00000000-0000-4000-8000-000000000001"),
            "owner_id": "x",
            "owner_type": "agent",
            "label": "",
            "policies": "[]",
            "is_active": True,
            "expires_at": None,
        }

        with pytest.raises(ValueError, match="Invalid API key"):
            await mgr.resolve("gtmdb_testid_secretsecretsecretsecret")

    @pytest.mark.asyncio
    async def test_not_found(self, mgr, store):
        store.get_by_key_id.return_value = None
        with pytest.raises(ValueError, match="Invalid API key"):
            await mgr.resolve("gtmdb_nope_secretsecretsecretsecret")

    @pytest.mark.asyncio
    async def test_revoked(self, mgr, store):
        raw_key = "gtmdb_testid_secretsecretsecretsecret"
        store.get_by_key_id.return_value = {
            "key_id": "testid",
            "key_hash": hashlib.sha256(raw_key.encode()).hexdigest(),
            "tenant_id": uuid.UUID("00000000-0000-4000-8000-000000000001"),
            "owner_id": "x",
            "owner_type": "agent",
            "label": "",
            "policies": "[]",
            "is_active": False,
            "expires_at": None,
        }
        with pytest.raises(ValueError, match="revoked"):
            await mgr.resolve(raw_key)

    @pytest.mark.asyncio
    async def test_expired(self, mgr, store):
        raw_key = "gtmdb_testid_secretsecretsecretsecret"
        store.get_by_key_id.return_value = {
            "key_id": "testid",
            "key_hash": hashlib.sha256(raw_key.encode()).hexdigest(),
            "tenant_id": uuid.UUID("00000000-0000-4000-8000-000000000001"),
            "owner_id": "x",
            "owner_type": "agent",
            "label": "",
            "policies": "[]",
            "is_active": True,
            "expires_at": datetime.now(timezone.utc) - timedelta(hours=1),
        }
        with pytest.raises(ValueError, match="expired"):
            await mgr.resolve(raw_key)

    @pytest.mark.asyncio
    async def test_write_only_scope(self, mgr, store):
        raw_key = "gtmdb_testid_secretsecretsecretsecret"
        store.get_by_key_id.return_value = {
            "key_id": "testid",
            "key_hash": hashlib.sha256(raw_key.encode()).hexdigest(),
            "tenant_id": uuid.UUID("00000000-0000-4000-8000-000000000001"),
            "owner_id": "writer",
            "owner_type": "agent",
            "label": "",
            "policies": json.dumps([{"effect": "allow", "actions": ["write"], "resources": ["*"], "conditions": {}}]),
            "is_active": True,
            "expires_at": None,
        }
        scope = await mgr.resolve(raw_key)
        assert scope.can_write("Lead")
        assert not scope.can_read("Lead")


class TestCreate:
    @pytest.mark.asyncio
    async def test_create_returns_raw_key(self):
        store = AsyncMock()
        mgr = _mgr_with_admin(store)
        result = await mgr.create(
            owner_id="test-agent",
            tenant_id="00000000-0000-4000-8000-000000000001",
        )
        assert result.raw_key.startswith("gtmdb_")
        assert result.owner_id == "test-agent"
        assert result.key_id
        store.insert.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_with_expiry(self):
        store = AsyncMock()
        mgr = _mgr_with_admin(store)
        result = await mgr.create(
            owner_id="test-agent",
            tenant_id="00000000-0000-4000-8000-000000000001",
            expires_in_days=30,
        )
        assert result.expires_at is not None

    @pytest.mark.asyncio
    async def test_unknown_preset_raises(self):
        store = AsyncMock()
        mgr = _mgr_with_admin(store)
        with pytest.raises(KeyError, match="nope"):
            await mgr.create(
                owner_id="x",
                tenant_id="00000000-0000-4000-8000-000000000001",
                preset_names=["nope"],
            )


class TestRevoke:
    @pytest.mark.asyncio
    async def test_revoke(self):
        store = AsyncMock()
        store.deactivate.return_value = True
        mgr = _mgr_with_admin(store)
        assert await mgr.revoke("someid") is True
        store.deactivate.assert_called_once_with("someid")


class TestRotate:
    @pytest.mark.asyncio
    async def test_rotate_creates_and_revokes(self):
        store = AsyncMock()
        store.get_by_key_id.return_value = {
            "key_id": "oldkey",
            "key_hash": "x",
            "tenant_id": uuid.UUID("00000000-0000-4000-8000-000000000001"),
            "owner_id": "agent",
            "owner_type": "agent",
            "label": "my key",
            "policies": json.dumps([{"effect": "allow", "actions": ["read"], "resources": ["*"], "conditions": {}}]),
            "is_active": True,
            "expires_at": None,
            "created_by": "admin",
        }
        mgr = _mgr_with_admin(store)
        result = await mgr.rotate("oldkey", expires_in_days=60)
        assert result.raw_key.startswith("gtmdb_")
        assert result.owner_id == "agent"
        store.deactivate.assert_called_once_with("oldkey")
