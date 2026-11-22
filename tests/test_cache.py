import asyncio
import re
import time
from decimal import Decimal
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError
from pymongo.errors import DuplicateKeyError

from persistence_kit.cache import (
    CacheBackend,
    CacheSettings,
    InMemoryTTLCache,
    NamespacedCache,
    get_cache,
    reset_cache,
)
from persistence_kit.cache.dynamodb import INCREMENT_ATTEMPTS as DYNAMO_INCREMENT_ATTEMPTS
from persistence_kit.cache.dynamodb import DynamoCache
from persistence_kit.cache.mongo import INCREMENT_ATTEMPTS as MONGO_INCREMENT_ATTEMPTS
from persistence_kit.cache.mongo import MongoCache


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

    def update_item(self, Key: dict, **kwargs) -> dict:
        names = kwargs["ExpressionAttributeNames"]
        values = kwargs["ExpressionAttributeValues"]
        existing = self.items.get(Key["pk"])
        item = dict(existing) if existing is not None else {}
        if not _condition_holds(kwargs["ConditionExpression"], item, names, values):
            raise ClientError(
                {"Error": {"Code": "ConditionalCheckFailedException", "Message": "condition"}},
                "UpdateItem",
            )
        item["pk"] = Key["pk"]
        updated = {}
        for name, value in re.findall(r"ADD (#\w+) (:\w+)", kwargs["UpdateExpression"]):
            attribute = names[name]
            item[attribute] = item.get(attribute, 0) + values[value]
            updated[attribute] = item[attribute]
        for name, kept, value in re.findall(
            r"SET (#\w+) = if_not_exists\((#\w+), (:\w+)\)", kwargs["UpdateExpression"]
        ):
            attribute = names[name]
            item[attribute] = item.get(names[kept], values[value])
            updated[attribute] = item[attribute]
        self.items[Key["pk"]] = item
        return {"Attributes": updated}

    def delete_item(self, Key: dict) -> None:
        self.items.pop(Key["pk"], None)

    def scan(self, **kwargs) -> dict:
        items = list(self.items.values())
        if kwargs.get("FilterExpression"):
            prefix = kwargs["ExpressionAttributeValues"][":prefix"]
            items = [item for item in items if item["pk"].startswith(prefix)]
        return {"Items": items}


def _condition_holds(condition: str, item: dict, names: dict, values: dict) -> bool:
    return any(
        all(_term_holds(term, item, names, values) for term in alternative.split(" AND "))
        for alternative in condition.split(" OR ")
    )


def _term_holds(term: str, item: dict, names: dict, values: dict) -> bool:
    existence = re.fullmatch(r"attribute_(not_)?exists\((#\w+)\)", term)
    if existence:
        return (names[existence.group(2)] in item) != bool(existence.group(1))
    name, operator, value = re.fullmatch(r"(#\w+) (>|<=) (:\w+)", term).groups()
    if names[name] not in item:
        return False
    if operator == ">":
        return item[names[name]] > values[value]
    return item[names[name]] <= values[value]


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
async def test_memory_cache_set_if_absent_only_writes_once():
    cache = InMemoryTTLCache()

    assert await cache.set_if_absent("k", 1) is True
    assert await cache.set_if_absent("k", 2) is False
    assert await cache.get("k") == 1


@pytest.mark.asyncio
async def test_memory_cache_set_if_absent_writes_again_after_the_ttl():
    cache = InMemoryTTLCache()

    assert await cache.set_if_absent("k", 1, ttl_seconds=0.05) is True
    await asyncio.sleep(0.08)

    assert await cache.set_if_absent("k", 2) is True
    assert await cache.get("k") == 2


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
async def test_namespace_isolates_set_if_absent(monkeypatch):
    monkeypatch.setenv("CACHE_BACKEND", "memory")
    reset_cache()

    monkeypatch.setenv("CACHE_NAMESPACE", "app_a")
    assert await get_cache("shared").set_if_absent("k", 1) is True

    monkeypatch.setenv("CACHE_NAMESPACE", "app_b")
    assert await get_cache("shared").set_if_absent("k", 2) is True

    monkeypatch.setenv("CACHE_NAMESPACE", "app_a")
    assert await get_cache("shared").set_if_absent("k", 3) is False
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


@pytest.mark.asyncio
async def test_memory_increment_starts_a_new_key_at_one():
    cache = InMemoryTTLCache()

    assert await cache.increment("hits", ttl_seconds=60) == 1
    assert await cache.get("hits") == 1


@pytest.mark.asyncio
async def test_memory_increment_adds_to_a_live_key():
    cache = InMemoryTTLCache()
    await cache.set("hits", 4)

    assert await cache.increment("hits") == 5
    assert await cache.increment("hits") == 6
    assert await cache.get("hits") == 6


@pytest.mark.asyncio
async def test_memory_increment_starts_over_once_the_key_expires():
    cache = InMemoryTTLCache()
    assert await cache.increment("hits", ttl_seconds=0.05) == 1
    assert await cache.increment("hits", ttl_seconds=0.05) == 2
    await asyncio.sleep(0.08)

    assert await cache.increment("hits", ttl_seconds=0.05) == 1


@pytest.mark.asyncio
async def test_memory_increment_keeps_the_first_expiry():
    cache = InMemoryTTLCache()
    await cache.increment("hits", ttl_seconds=0.05)
    await cache.increment("hits", ttl_seconds=60)
    await asyncio.sleep(0.08)

    assert await cache.get("hits") is None


@pytest.mark.asyncio
async def test_memory_increment_does_not_lose_concurrent_hits():
    cache = InMemoryTTLCache()

    results = await asyncio.gather(*(cache.increment("hits", ttl_seconds=60) for _ in range(50)))

    assert sorted(results) == list(range(1, 51))
    assert await cache.get("hits") == 50


@pytest.mark.asyncio
async def test_namespaced_increment_prefixes_the_key():
    inner = InMemoryTTLCache()
    app_a = NamespacedCache(inner, "app_a")
    app_b = NamespacedCache(inner, "app_b")

    assert await app_a.increment("hits", ttl_seconds=60) == 1
    assert await app_a.increment("hits", ttl_seconds=60) == 2
    assert await app_b.increment("hits", ttl_seconds=60) == 1
    assert await inner.get("app_a:hits") == 2


@pytest.mark.asyncio
async def test_namespace_isolates_increment(monkeypatch):
    monkeypatch.setenv("CACHE_BACKEND", "memory")
    reset_cache()

    monkeypatch.setenv("CACHE_NAMESPACE", "app_a")
    await get_cache("shared").increment("k", ttl_seconds=60)
    await get_cache("shared").increment("k", ttl_seconds=60)

    monkeypatch.setenv("CACHE_NAMESPACE", "app_b")
    assert await get_cache("shared").increment("k", ttl_seconds=60) == 1
    reset_cache()


class DuplicateKeyCollection:
    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.calls = 0

    async def create_index(self, *args, **kwargs) -> str:
        return "expiresAt_1"

    async def find_one_and_update(self, query: dict, update: list, **kwargs) -> dict:
        self.calls += 1
        if self.calls <= self.failures:
            raise DuplicateKeyError("E11000 duplicate key error")
        return {"_id": query["_id"], "value": 2}


@pytest.mark.asyncio
async def test_mongo_increment_retries_the_upsert_that_lost_the_race():
    collection = DuplicateKeyCollection(failures=1)

    assert await MongoCache(collection).increment("hits", ttl_seconds=60) == 2
    assert collection.calls == 2


@pytest.mark.asyncio
async def test_mongo_increment_gives_up_after_a_bounded_number_of_races():
    collection = DuplicateKeyCollection(failures=MONGO_INCREMENT_ATTEMPTS)

    with pytest.raises(DuplicateKeyError):
        await MongoCache(collection).increment("hits", ttl_seconds=60)
    assert collection.calls == MONGO_INCREMENT_ATTEMPTS


@pytest.mark.asyncio
async def test_dynamo_increment_creates_the_row_with_its_ttl():
    table = FakeDynamoTable()
    cache = DynamoCache(table)
    before = time.time()

    assert await cache.increment("hits", ttl_seconds=60) == 1

    row = table.items["hits"]
    assert row["value"] == 1
    assert 59 <= float(row["expiresAt"]) - before <= 61
    assert await cache.get("hits") == 1


@pytest.mark.asyncio
async def test_dynamo_increment_adds_and_keeps_the_expiry():
    table = FakeDynamoTable()
    cache = DynamoCache(table)
    await cache.increment("hits", ttl_seconds=60)
    first_expiry = table.items["hits"]["expiresAt"]

    assert await cache.increment("hits", ttl_seconds=600) == 2
    assert await cache.increment("hits", ttl_seconds=600) == 3
    assert table.items["hits"]["expiresAt"] == first_expiry
    assert await cache.get("hits") == 3


@pytest.mark.asyncio
async def test_dynamo_increment_without_ttl_never_expires():
    table = FakeDynamoTable()
    cache = DynamoCache(table)

    assert await cache.increment("hits") == 1
    assert await cache.increment("hits") == 2
    assert "expiresAt" not in table.items["hits"]


@pytest.mark.asyncio
async def test_dynamo_increment_keeps_a_key_without_expiry_without_one():
    table = FakeDynamoTable()
    cache = DynamoCache(table)
    await cache.set("hits", 4)

    assert await cache.increment("hits", ttl_seconds=60) == 5
    assert "expiresAt" not in table.items["hits"]


@pytest.mark.asyncio
async def test_dynamo_increment_replaces_an_expired_row_the_ttl_did_not_sweep():
    table = FakeDynamoTable()
    table.items["hits"] = {"pk": "hits", "value": 9, "expiresAt": Decimal(str(time.time() - 5))}
    cache = DynamoCache(table)

    assert await cache.increment("hits", ttl_seconds=60) == 1
    assert table.items["hits"]["expiresAt"] > time.time()
    assert await cache.increment("hits", ttl_seconds=60) == 2


@pytest.mark.asyncio
async def test_dynamo_increment_replaces_an_expired_row_without_ttl():
    table = FakeDynamoTable()
    table.items["hits"] = {"pk": "hits", "value": 9, "expiresAt": Decimal(str(time.time() - 5))}
    cache = DynamoCache(table)

    assert await cache.increment("hits") == 1
    assert "expiresAt" not in table.items["hits"]


class RacedDynamoTable(FakeDynamoTable):
    def put_item(self, Item: dict, **kwargs) -> None:
        if kwargs.get("ConditionExpression") and not getattr(self, "raced", False):
            self.raced = True
            self.items[Item["pk"]] = {
                "pk": Item["pk"],
                "value": 1,
                "expiresAt": Decimal(str(time.time() + 60)),
            }
        super().put_item(Item, **kwargs)


@pytest.mark.asyncio
async def test_dynamo_increment_adds_on_top_of_the_request_that_won_the_race():
    table = RacedDynamoTable()
    table.items["hits"] = {"pk": "hits", "value": 9, "expiresAt": Decimal(str(time.time() - 5))}
    cache = DynamoCache(table)

    assert await cache.increment("hits", ttl_seconds=60) == 2
    assert table.items["hits"]["value"] == 2


class AlwaysLosingDynamoTable(FakeDynamoTable):
    def update_item(self, Key: dict, **kwargs) -> dict:
        raise ClientError(
            {"Error": {"Code": "ConditionalCheckFailedException", "Message": "condition"}},
            "UpdateItem",
        )

    def put_item(self, Item: dict, **kwargs) -> None:
        raise ClientError(
            {"Error": {"Code": "ConditionalCheckFailedException", "Message": "exists"}},
            "PutItem",
        )


@pytest.mark.asyncio
async def test_dynamo_increment_gives_up_after_a_bounded_number_of_races():
    with pytest.raises(RuntimeError, match=f"after {DYNAMO_INCREMENT_ATTEMPTS} attempts"):
        await DynamoCache(AlwaysLosingDynamoTable()).increment("hits", ttl_seconds=60)


class BrokenUpdateDynamoTable(FakeDynamoTable):
    def update_item(self, Key: dict, **kwargs) -> dict:
        raise ClientError({"Error": {"Code": "ValidationException", "Message": "bad"}}, "UpdateItem")


@pytest.mark.asyncio
async def test_dynamo_increment_lets_other_update_errors_through():
    with pytest.raises(ClientError, match="ValidationException"):
        await DynamoCache(BrokenUpdateDynamoTable()).increment("hits", ttl_seconds=60)


class BrokenPutDynamoTable(FakeDynamoTable):
    def put_item(self, Item: dict, **kwargs) -> None:
        raise ClientError({"Error": {"Code": "ValidationException", "Message": "bad"}}, "PutItem")


@pytest.mark.asyncio
async def test_dynamo_increment_lets_other_put_errors_through():
    table = BrokenPutDynamoTable()
    table.items["hits"] = {"pk": "hits", "value": 9, "expiresAt": Decimal(str(time.time() - 5))}

    with pytest.raises(ClientError, match="ValidationException"):
        await DynamoCache(table).increment("hits", ttl_seconds=60)


@pytest.mark.asyncio
async def test_get_cache_dynamodb_backend_increments_under_its_name(monkeypatch):
    monkeypatch.setenv("CACHE_BACKEND", "dynamodb")
    monkeypatch.setenv("CACHE_NAMESPACE", "")
    reset_cache()

    fake_table = FakeDynamoTable()
    with patch("boto3.resource") as resource:
        resource.return_value.Table.return_value = fake_table
        assert await get_cache("rate-limit").increment("k", ttl_seconds=60) == 1
        assert await get_cache("rate-limit").increment("k", ttl_seconds=60) == 2

    assert fake_table.items["rate-limit:k"]["value"] == 2
    reset_cache()
