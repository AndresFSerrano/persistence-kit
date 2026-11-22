import asyncio
import os
import uuid
from contextlib import closing
from datetime import datetime, timedelta, timezone

import pytest
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient

from persistence_kit.cache.mongo import MongoCache

MONGO_DSN = os.environ.get("PERSISTENCE_KIT_TEST_MONGO_DSN", "")

pytestmark = pytest.mark.skipif(
    not MONGO_DSN,
    reason="Set PERSISTENCE_KIT_TEST_MONGO_DSN to run the cache against a real Mongo",
)


@pytest.fixture
def database_name():
    name = f"pk_cache_test_{uuid.uuid4().hex[:12]}"
    yield name
    with closing(MongoClient(MONGO_DSN)) as client:
        client.drop_database(name)


def _client() -> AsyncIOMotorClient:
    return AsyncIOMotorClient(MONGO_DSN, uuidRepresentation="standard")


def _expiry(doc: dict) -> datetime:
    return doc["expiresAt"].replace(tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_a_new_key_starts_at_one_with_its_ttl(database_name):
    with closing(_client()) as client:
        collection = client[database_name]["cache"]
        before = datetime.now(timezone.utc)

        assert await MongoCache(collection).increment("hits", ttl_seconds=60) == 1

        doc = await collection.find_one({"_id": "hits"})
        assert doc["value"] == 1
        assert timedelta(seconds=59) <= _expiry(doc) - before <= timedelta(seconds=61)


@pytest.mark.asyncio
async def test_a_live_key_adds_and_keeps_its_expiry(database_name):
    with closing(_client()) as client:
        collection = client[database_name]["cache"]
        cache = MongoCache(collection)
        await cache.increment("hits", ttl_seconds=60)
        first_expiry = (await collection.find_one({"_id": "hits"}))["expiresAt"]

        assert await cache.increment("hits", ttl_seconds=600) == 2
        assert await cache.increment("hits", ttl_seconds=600) == 3
        assert (await collection.find_one({"_id": "hits"}))["expiresAt"] == first_expiry
        assert await cache.get("hits") == 3


@pytest.mark.asyncio
async def test_an_expired_document_the_ttl_monitor_did_not_sweep_starts_over(database_name):
    with closing(_client()) as client:
        collection = client[database_name]["cache"]
        stale = datetime.now(timezone.utc) - timedelta(seconds=5)
        await collection.insert_one({"_id": "hits", "value": 9, "expiresAt": stale})
        cache = MongoCache(collection)

        assert await cache.increment("hits", ttl_seconds=60) == 1

        doc = await collection.find_one({"_id": "hits"})
        assert _expiry(doc) > datetime.now(timezone.utc)
        assert await cache.increment("hits", ttl_seconds=60) == 2


@pytest.mark.asyncio
async def test_without_ttl_the_key_never_gets_an_expiry(database_name):
    with closing(_client()) as client:
        collection = client[database_name]["cache"]
        cache = MongoCache(collection)

        assert await cache.increment("hits") == 1
        assert await cache.increment("hits") == 2
        assert "expiresAt" not in await collection.find_one({"_id": "hits"})


@pytest.mark.asyncio
async def test_a_key_without_expiry_keeps_it_that_way(database_name):
    with closing(_client()) as client:
        collection = client[database_name]["cache"]
        cache = MongoCache(collection)
        await cache.set("hits", 4)

        assert await cache.increment("hits", ttl_seconds=60) == 5
        assert "expiresAt" not in await collection.find_one({"_id": "hits"})


@pytest.mark.asyncio
async def test_concurrent_increments_on_a_new_key_lose_nothing(database_name):
    with closing(_client()) as first, closing(_client()) as second:
        replicas = [
            MongoCache(first[database_name]["cache"]),
            MongoCache(second[database_name]["cache"]),
        ]

        results = await asyncio.gather(
            *(replicas[index % 2].increment("hits", ttl_seconds=60) for index in range(100))
        )

        assert sorted(results) == list(range(1, 101))
        assert await replicas[0].get("hits") == 100


@pytest.mark.asyncio
async def test_concurrent_increments_on_an_expired_key_restart_it_once(database_name):
    with closing(_client()) as client:
        collection = client[database_name]["cache"]
        stale = datetime.now(timezone.utc) - timedelta(seconds=5)
        await collection.insert_one({"_id": "hits", "value": 9, "expiresAt": stale})
        cache = MongoCache(collection)

        results = await asyncio.gather(*(cache.increment("hits", ttl_seconds=60) for _ in range(50)))

        assert sorted(results) == list(range(1, 51))


@pytest.mark.asyncio
async def test_two_replicas_share_the_rate_limit(database_name):
    pytest.importorskip("fastapi")
    from persistence_kit.api import CacheRateLimiter

    with closing(_client()) as first, closing(_client()) as second:
        replicas = [
            CacheRateLimiter(MongoCache(first[database_name]["cache_rate-limit"])),
            CacheRateLimiter(MongoCache(second[database_name]["cache_rate-limit"])),
        ]

        results = [
            await replica.hit(
                scope="auth-login", client_id="203.0.113.7", limit=3, window_seconds=300
            )
            for replica in replicas * 2
        ]

        assert results[:3] == [None, None, None]
        assert results[3] >= 1
