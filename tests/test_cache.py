import asyncio

import pytest

from persistence_kit.cache import (
    CacheBackend,
    CacheSettings,
    InMemoryTTLCache,
    get_cache,
    reset_cache,
)


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
