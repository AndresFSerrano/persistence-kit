import asyncio
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError

from persistence_kit.cache import (
    CacheBackend,
    CacheSettings,
    InMemoryTTLCache,
    NamespacedCache,
    get_cache,
    reset_cache,
)
from persistence_kit.cache.dynamodb import DynamoCache


class FakeDynamoTable:
    def __init__(self) -> None:
        self.items: dict[str, dict] = {}

    def get_item(self, Key: dict) -> dict:
        item = self.items.get(Key["pk"])
        return {"Item": dict(item)} if item is not None else {}

    def put_item(self, Item: dict, **kwargs) -> None:
        condition = kwargs.get("ConditionExpression")
        if condition:
            existing = self.items.get(Item["pk"])
            now = kwargs["ExpressionAttributeValues"][":now"]
            already_expired = (
                existing is not None
                and existing.get("expiresAt") is not None
                and existing["expiresAt"] <= now
            )
            if existing is not None and not already_expired:
                raise ClientError(
                    {"Error": {"Code": "ConditionalCheckFailedException", "Message": "exists"}},
                    "PutItem",
                )
        self.items[Item["pk"]] = dict(Item)

    def delete_item(self, Key: dict) -> None:
        self.items.pop(Key["pk"], None)

    def scan(self, **kwargs) -> dict:
        items = list(self.items.values())
        if kwargs.get("FilterExpression"):
            prefix = kwargs["ExpressionAttributeValues"][":prefix"]
            items = [item for item in items if item["pk"].startswith(prefix)]
        return {"Items": items}


@pytest.mark.asyncio
async def test_memory_cache_set_get():
    cache = InMemoryTTLCache()
    await cache.set("k", {"a": 1})
    assert await cache.get("k") == {"a": 1}


@pytest.mark.asyncio
async def test_memory_cache_missing_key_returns_none():
    cache = InMemoryTTLCache()
    assert await cache.get("nope") is None


@pytest.mark.asyncio
async def test_memory_cache_expires():
    cache = InMemoryTTLCache()
    await cache.set("k", 1, ttl_seconds=0.05)
    assert await cache.get("k") == 1
    await asyncio.sleep(0.08)
    assert await cache.get("k") is None


@pytest.mark.asyncio
async def test_memory_cache_delete():
    cache = InMemoryTTLCache()
    await cache.set("k", 1)
    await cache.delete("k")
    assert await cache.get("k") is None


def test_get_cache_defaults_to_memory(monkeypatch):
    monkeypatch.delenv("CACHE_BACKEND", raising=False)
    reset_cache()
    assert isinstance(get_cache("x"), InMemoryTTLCache)
    reset_cache()


def test_cache_backend_reads_env(monkeypatch):
    monkeypatch.setenv("CACHE_BACKEND", "mongo")
    assert CacheSettings().cache_backend is CacheBackend.MONGO
    monkeypatch.setenv("CACHE_BACKEND", "memory")
    assert CacheSettings().cache_backend is CacheBackend.MEMORY


def test_no_namespace_returns_backend_directly(monkeypatch):
    monkeypatch.setenv("CACHE_BACKEND", "memory")
    monkeypatch.setenv("CACHE_NAMESPACE", "")
    reset_cache()
    assert isinstance(get_cache("x"), InMemoryTTLCache)
    reset_cache()


@pytest.mark.asyncio
async def test_namespace_isolates_keys_on_shared_backend(monkeypatch):
    monkeypatch.setenv("CACHE_BACKEND", "memory")
    reset_cache()

    monkeypatch.setenv("CACHE_NAMESPACE", "app_a")
    await get_cache("shared").set("k", 1)

    monkeypatch.setenv("CACHE_NAMESPACE", "app_b")
    assert await get_cache("shared").get("k") is None

    monkeypatch.setenv("CACHE_NAMESPACE", "app_a")
    assert await get_cache("shared").get("k") == 1
    reset_cache()


@pytest.mark.asyncio
async def test_memory_clear_by_prefix():
    cache = InMemoryTTLCache()
    await cache.set("foo|a", 1)
    await cache.set("foo|b", 2)
    await cache.set("bar|c", 3)

    deleted = await cache.clear("foo")

    assert deleted == 2
    assert await cache.get("foo|a") is None
    assert await cache.get("bar|c") == 3


@pytest.mark.asyncio
async def test_memory_clear_all():
    cache = InMemoryTTLCache()
    await cache.set("a", 1)
    await cache.set("b", 2)

    assert await cache.clear() == 2
    assert await cache.get("a") is None


@pytest.mark.asyncio
async def test_clear_by_namespace_and_service(monkeypatch):
    monkeypatch.setenv("CACHE_BACKEND", "memory")
    reset_cache()

    monkeypatch.setenv("CACHE_NAMESPACE", "app_a")
    sm = get_cache("shared")
    await sm.set("foo|x", 1)
    await sm.set("bar|y", 2)

    monkeypatch.setenv("CACHE_NAMESPACE", "app_b")
    await get_cache("shared").set("foo|x", 99)

    monkeypatch.setenv("CACHE_NAMESPACE", "app_a")
    deleted = await get_cache("shared").clear("foo")

    assert deleted == 1
    assert await get_cache("shared").get("foo|x") is None
    assert await get_cache("shared").get("bar|y") == 2

    monkeypatch.setenv("CACHE_NAMESPACE", "app_b")
    assert await get_cache("shared").get("foo|x") == 99
    reset_cache()


@pytest.mark.asyncio
async def test_dynamo_cache_set_get():
    cache = DynamoCache(FakeDynamoTable())
    await cache.set("k", {"a": 1})
    assert await cache.get("k") == {"a": 1}


@pytest.mark.asyncio
async def test_dynamo_cache_missing_key_returns_none():
    cache = DynamoCache(FakeDynamoTable())
    assert await cache.get("nope") is None


@pytest.mark.asyncio
async def test_dynamo_cache_expires():
    cache = DynamoCache(FakeDynamoTable())
    await cache.set("k", 1, ttl_seconds=0.05)
    assert await cache.get("k") == 1
    await asyncio.sleep(0.08)
    assert await cache.get("k") is None


@pytest.mark.asyncio
async def test_dynamo_cache_set_if_absent_only_writes_once():
    cache = DynamoCache(FakeDynamoTable())
    assert await cache.set_if_absent("k", 1) is True
    assert await cache.set_if_absent("k", 2) is False
    assert await cache.get("k") == 1


@pytest.mark.asyncio
async def test_dynamo_cache_set_if_absent_writes_again_after_the_ttl():
    cache = DynamoCache(FakeDynamoTable())
    assert await cache.set_if_absent("k", 1, ttl_seconds=0.05) is True
    await asyncio.sleep(0.08)
    assert await cache.set_if_absent("k", 2) is True
    assert await cache.get("k") == 2


@pytest.mark.asyncio
async def test_dynamo_cache_delete():
    cache = DynamoCache(FakeDynamoTable())
    await cache.set("k", 1)
    await cache.delete("k")
    assert await cache.get("k") is None


@pytest.mark.asyncio
async def test_dynamo_cache_clear_by_prefix():
    cache = DynamoCache(FakeDynamoTable())
    await cache.set("foo|a", 1)
    await cache.set("foo|b", 2)
    await cache.set("bar|c", 3)

    deleted = await cache.clear("foo")

    assert deleted == 2
    assert await cache.get("foo|a") is None
    assert await cache.get("bar|c") == 3


@pytest.mark.asyncio
async def test_dynamo_cache_clear_all():
    cache = DynamoCache(FakeDynamoTable())
    await cache.set("a", 1)
    await cache.set("b", 2)

    assert await cache.clear() == 2
    assert await cache.get("a") is None


@pytest.mark.asyncio
async def test_dynamo_cache_roundtrips_floats_through_decimal():
    cache = DynamoCache(FakeDynamoTable())
    await cache.set("k", {"stored_at": 123.456, "count": 3})
    assert await cache.get("k") == {"stored_at": 123.456, "count": 3}


def test_cache_backend_reads_dynamodb_from_env(monkeypatch):
    monkeypatch.setenv("CACHE_BACKEND", "dynamodb")
    assert CacheSettings().cache_backend is CacheBackend.DYNAMODB


def test_get_cache_dynamodb_backend_is_namespaced_by_name(monkeypatch):
    monkeypatch.setenv("CACHE_BACKEND", "dynamodb")
    reset_cache()

    fake_table = FakeDynamoTable()
    with patch("boto3.resource") as resource:
        resource.return_value.Table.return_value = fake_table
        cache = get_cache("cognito_identity")

    assert isinstance(cache, NamespacedCache)
    reset_cache()


@pytest.mark.asyncio
async def test_get_cache_dynamodb_backend_isolates_by_name_on_the_shared_table(monkeypatch):
    monkeypatch.setenv("CACHE_BACKEND", "dynamodb")
    monkeypatch.setenv("CACHE_NAMESPACE", "")
    reset_cache()

    fake_table = FakeDynamoTable()
    with patch("boto3.resource") as resource:
        resource.return_value.Table.return_value = fake_table
        await get_cache("cognito_identity").set("k", 1)
        assert await get_cache("restclient").get("k") is None
        assert await get_cache("cognito_identity").get("k") == 1

    reset_cache()
