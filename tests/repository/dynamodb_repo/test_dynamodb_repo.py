from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from persistence_kit.repository.dynamodb_repo.dynamodb_mapper import DynamoMapper
from persistence_kit.repository.dynamodb_repo.dynamodb_repo import (
    DynamoRepository,
    _build_filter,
    _serialize_value,
)


@dataclass
class Entity:
    id: str
    name: str
    value: int


class FakeTable:
    def __init__(self):
        self.items: list[dict] = []
        self.item_count = 0

    def put_item(self, Item: dict, **kwargs):
        condition = kwargs.get("ConditionExpression")
        if condition is not None:
            for item in self.items:
                if item.get("id") == Item.get("id"):
                    raise Exception("ConditionalCheckFailedException")
        for i, item in enumerate(self.items):
            if item.get("id") == Item.get("id"):
                self.items[i] = dict(Item)
                self.item_count = len(self.items)
                return
        self.items.append(dict(Item))
        self.item_count = len(self.items)

    def get_item(self, Key: dict) -> dict:
        for item in self.items:
            if all(item.get(k) == v for k, v in Key.items()):
                return {"Item": dict(item)}
        return {}

    def delete_item(self, Key: dict):
        self.items = [
            item for item in self.items
            if not all(item.get(k) == v for k, v in Key.items())
        ]
        self.item_count = len(self.items)

    def scan(self, **kwargs) -> dict:
        items = list(self.items)
        return {"Items": items}


@pytest.fixture
def mapper():
    return DynamoMapper(Entity, "test_table", unique_fields={"by_name": "name"})


@pytest.fixture
def repo(mapper):
    fake_table = FakeTable()
    r = DynamoRepository.__new__(DynamoRepository)
    r._mapper = mapper
    r._table = fake_table
    r._dynamodb = MagicMock()
    return r


@pytest.mark.asyncio
async def test_add_and_get(repo):
    e = Entity(id="1", name="alice", value=10)
    await repo.add(e)
    result = await repo.get("1")
    assert result is not None
    assert result.id == "1"
    assert result.name == "alice"
    assert result.value == 10


@pytest.mark.asyncio
async def test_get_not_found(repo):
    result = await repo.get("nonexistent")
    assert result is None


@pytest.mark.asyncio
async def test_update(repo):
    e = Entity(id="1", name="alice", value=10)
    await repo.add(e)
    updated = Entity(id="1", name="alice_updated", value=20)
    await repo.update(updated)
    result = await repo.get("1")
    assert result.name == "alice_updated"
    assert result.value == 20


@pytest.mark.asyncio
async def test_delete(repo):
    e = Entity(id="1", name="alice", value=10)
    await repo.add(e)
    await repo.delete("1")
    result = await repo.get("1")
    assert result is None


@pytest.mark.asyncio
async def test_list(repo):
    await repo.add(Entity(id="1", name="alice", value=10))
    await repo.add(Entity(id="2", name="bob", value=20))
    result = await repo.list(offset=0, limit=10)
    assert len(result) == 2
    ids = {e.id for e in result}
    assert ids == {"1", "2"}


@pytest.mark.asyncio
async def test_list_with_sort(repo):
    await repo.add(Entity(id="1", name="bob", value=20))
    await repo.add(Entity(id="2", name="alice", value=10))
    result = await repo.list(offset=0, limit=10, sort_by="name")
    assert result[0].name == "alice"
    assert result[1].name == "bob"


@pytest.mark.asyncio
async def test_list_with_sort_desc(repo):
    await repo.add(Entity(id="1", name="alice", value=10))
    await repo.add(Entity(id="2", name="bob", value=20))
    result = await repo.list(offset=0, limit=10, sort_by="name", sort_desc=True)
    assert result[0].name == "bob"


@pytest.mark.asyncio
async def test_list_with_offset(repo):
    await repo.add(Entity(id="1", name="alice", value=10))
    await repo.add(Entity(id="2", name="bob", value=20))
    result = await repo.list(offset=1, limit=10, sort_by="name")
    assert len(result) == 1
    assert result[0].name == "bob"


@pytest.mark.asyncio
async def test_list_invalid_sort_raises(repo):
    with pytest.raises(ValueError):
        await repo.list(offset=0, limit=10, sort_by="nonexistent")


@pytest.mark.asyncio
async def test_count(repo):
    await repo.add(Entity(id="1", name="alice", value=10))
    await repo.add(Entity(id="2", name="bob", value=20))
    assert await repo.count() == 2


@pytest.mark.asyncio
async def test_distinct_values(repo):
    await repo.add(Entity(id="1", name="alice", value=10))
    await repo.add(Entity(id="2", name="bob", value=20))
    await repo.add(Entity(id="3", name="alice", value=30))
    result = await repo.distinct_values("name")
    assert set(result) == {"alice", "bob"}


@pytest.mark.asyncio
async def test_distinct_values_invalid_field_raises(repo):
    with pytest.raises(ValueError):
        await repo.distinct_values("nonexistent")


def test_build_filter_empty_criteria(mapper):
    result = _build_filter(mapper, {})
    assert result is None


def test_build_filter_empty_list_returns_false(mapper):
    result = _build_filter(mapper, {"id": []})
    assert result is False


def test_build_filter_empty_in_range_returns_false(mapper):
    result = _build_filter(mapper, {"id": {"in": []}})
    assert result is False


def test_serialize_value_preserves_strings():
    assert _serialize_value("hello") == "hello"


def test_serialize_value_preserves_ints():
    assert _serialize_value(42) == 42


class PaginatedFakeTable:
    def __init__(self, pages: list[list[dict]]):
        self.pages = pages
        self.scan_calls = 0

    def scan(self, **kwargs) -> dict:
        self.scan_calls += 1
        start_key = kwargs.get("ExclusiveStartKey")
        index = start_key["page"] if start_key is not None else 0
        response: dict[str, Any] = {"Items": list(self.pages[index])}
        if index + 1 < len(self.pages):
            response["LastEvaluatedKey"] = {"page": index + 1}
        return response


def _paginated_repo(mapper, pages: list[list[dict]]):
    table = PaginatedFakeTable(pages)
    repo = DynamoRepository.__new__(DynamoRepository)
    repo._mapper = mapper
    repo._table = table
    repo._dynamodb = MagicMock()
    return repo, table


PAGES = [
    [{"id": "1", "name": "first", "value": 1}],
    [{"id": "2", "name": "second", "value": 2}],
    [{"id": "3", "name": "third", "value": 3}],
]


@pytest.mark.asyncio
async def test_list_by_fields_stops_scanning_once_the_limit_is_covered(mapper):
    repo, table = _paginated_repo(mapper, PAGES)

    results = await repo.list_by_fields({"name": "first"}, offset=0, limit=1)

    assert table.scan_calls == 1
    assert len(results) == 1


@pytest.mark.asyncio
async def test_list_by_fields_scans_every_page_when_sorting(mapper):
    repo, table = _paginated_repo(mapper, PAGES)

    await repo.list_by_fields({"name": "first"}, offset=0, limit=1, sort_by="name")

    assert table.scan_calls == len(PAGES)


@pytest.mark.asyncio
async def test_count_by_fields_scans_every_page(mapper):
    repo, table = _paginated_repo(mapper, PAGES)

    total = await repo.count_by_fields({"name": "first"})

    assert table.scan_calls == len(PAGES)
    assert total == len(PAGES)
