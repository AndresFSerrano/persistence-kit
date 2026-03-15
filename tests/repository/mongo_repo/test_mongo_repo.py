from dataclasses import dataclass
from typing import Any, Dict, Hashable, Mapping, Optional, Sequence

import pytest

from persistence_kit.repository.mongo_repo.mongo_repo import (
    MongoRepository,
    _find_by_fields,
    _normalize_field,
)


@dataclass
class Entity:
    id: int
    name: str


class FakeMapperForFind:
    def __init__(self):
        self._attrs = {"id", "name", "group", "_id"}

    def id_field(self) -> str:
        return "_id"

    def from_document(self, doc: Mapping[str, Any]) -> Any:
        return dict(doc)

    def has_attr(self, name: str) -> bool:
        return name in self._attrs

    def attr_to_storage(self, name: str) -> str:
        if name == "id":
            return "_id"
        return name


class FakeCursor:
    def __init__(self, docs: Sequence[Mapping[str, Any]]):
        self.docs = list(docs)
        self._skip = 0
        self._limit: Optional[int] = None

    def skip(self, n: int) -> "FakeCursor":
        self._skip = n
        return self

    def limit(self, n: int) -> "FakeCursor":
        self._limit = n
        return self

    def sort(self, field: str, direction: int) -> "FakeCursor":
        return self

    async def to_list(self, length: int) -> Sequence[Mapping[str, Any]]:
        docs = self.docs
        if self._skip:
            docs = docs[self._skip :]

        limit = self._limit if self._limit is not None and self._limit > 0 else length
        if limit and limit > 0:
            docs = docs[:limit]

        if length == 0 and (self._limit is None or self._limit == 0):
            return list(docs)

        return list(docs)


class FakeCollectionForFind:
    def __init__(self, docs: Sequence[Mapping[str, Any]]):
        self.docs = list(docs)
        self.last_find: Dict[str, Any] = {}

    def _matches(self, doc: Mapping[str, Any], query: Mapping[str, Any]) -> bool:
        for k, v in query.items():
            if isinstance(v, dict) and "$eq" in v:
                if v["$eq"] is None:
                    if doc.get(k) is not None:
                        return False
                else:
                    if doc.get(k) != v["$eq"]:
                        return False
            else:
                if doc.get(k) != v:
                    return False
        return True

    def find(self, query: Mapping[str, Any], skip: int = 0, limit: int = 0):
        self.last_find = {"query": dict(query), "skip": skip, "limit": limit}
        filtered = [d for d in self.docs if self._matches(d, query)]
        return FakeCursor(filtered)


class FakeCollection:
    def __init__(self):
        self.docs: list[dict] = []
        self.indexes: list[dict] = []
        self.last_find: Dict[str, Any] = {}
        self.last_replace: Optional[dict] = None
        self.last_delete_filter: Optional[dict] = None

    async def create_index(self, keys, name: str, unique: bool):
        self.indexes.append({"keys": keys, "name": name, "unique": unique})

    def _matches_filter(self, doc: Mapping[str, Any], flt: Mapping[str, Any]) -> bool:
        for k, v in flt.items():
            if doc.get(k) != v:
                return False
        return True

    def find(self, flt: Mapping[str, Any], skip: int = 0, limit: int = 0):
        self.last_find = {"filter": dict(flt), "skip": skip, "limit": limit}
        if flt:
            filtered = [d for d in self.docs if self._matches_filter(d, flt)]
        else:
            filtered = list(self.docs)
        return FakeCursor(filtered)

    async def find_one(self, flt: Mapping[str, Any]) -> Optional[dict]:
        for d in self.docs:
            if self._matches_filter(d, flt):
                return dict(d)
        return None

    async def insert_one(self, doc: Mapping[str, Any]):
        self.docs.append(dict(doc))

    async def replace_one(self, flt: Mapping[str, Any], doc: Mapping[str, Any], upsert: bool = False):
        self.last_replace = {"filter": dict(flt), "doc": dict(doc), "upsert": upsert}
        for i, d in enumerate(self.docs):
            if self._matches_filter(d, flt):
                self.docs[i] = dict(doc)
                return
        if upsert:
            self.docs.append(dict(doc))

    async def delete_one(self, flt: Mapping[str, Any]):
        self.last_delete_filter = dict(flt)
        for i, d in enumerate(self.docs):
            if self._matches_filter(d, flt):
                self.docs.pop(i)
                return

    async def count_documents(self, flt: Mapping[str, Any]) -> int:
        if not flt:
            return len(self.docs)
        return sum(1 for d in self.docs if self._matches_filter(d, flt))

    async def distinct(self, field: str, flt: Mapping[str, Any]) -> list[Any]:
        values: list[Any] = []
        for doc in self.docs:
            if flt and not self._matches_filter(doc, flt):
                continue
            value = doc.get(field)
            if value is None or value in values:
                continue
            values.append(value)
        return values


class FakeDb:
    def __init__(self, collection: FakeCollection):
        self._collection = collection

    def __getitem__(self, name: str) -> FakeCollection:
        return self._collection


class FakeMapper:
    def __init__(self):
        self._collection_name = "entities"
        self._id_field = "_id"
        self._unique = {"by_name": "name"}
        self._attrs = {"id", "name", "_id"}

    def collection(self) -> str:
        return self._collection_name

    def id_of(self, entity: Entity) -> int:
        return entity.id

    def to_document(self, entity: Entity) -> dict:
        return {"name": entity.name}

    def from_document(self, doc: Mapping[str, Any]) -> Entity:
        return Entity(id=doc["_id"], name=doc["name"])

    def id_field(self) -> str:
        return self._id_field

    def unique_fields(self) -> dict[str, str]:
        return dict(self._unique)

    def has_attr(self, name: str) -> bool:
        return name in self._attrs

    def attr_to_storage(self, name: str) -> str:
        if name == "id":
            return "_id"
        return name


def test_normalize_field_maps_id_to_id_field():
    mapper = FakeMapper()
    assert _normalize_field(mapper, "id") == "_id"
    assert _normalize_field(mapper, "name") == "name"


@pytest.mark.asyncio
async def test_find_by_fields_empty_criteria_returns_empty():
    col = FakeCollectionForFind([])
    mapper = FakeMapperForFind()
    result = await _find_by_fields(col, mapper, {}, offset=0, limit=10)
    assert result == []


@pytest.mark.asyncio
async def test_find_by_fields_builds_query_and_filters_with_none_and_limit():
    docs = [
        {"_id": 1, "name": "a", "group": "g1"},
        {"_id": 2, "name": "b", "group": None},
        {"_id": 3, "name": "b", "group": None},
    ]
    col = FakeCollectionForFind(docs)
    mapper = FakeMapperForFind()

    result = await _find_by_fields(col, mapper, {"group": None}, offset=0, limit=None)
    assert len(result) == 2
    assert col.last_find["query"] == {"group": {"$eq": None}}
    assert {r["_id"] for r in result} == {2, 3}

    result2 = await _find_by_fields(col, mapper, {"group": None}, offset=1, limit=1)
    assert len(result2) == 1
    assert result2[0]["_id"] in {2, 3}


@pytest.mark.asyncio
async def test_find_by_fields_normalizes_id_field():
    docs = [
        {"_id": 1, "name": "a"},
        {"_id": 2, "name": "b"},
    ]
    col = FakeCollectionForFind(docs)
    mapper = FakeMapperForFind()

    result = await _find_by_fields(col, mapper, {"id": 2}, offset=0, limit=10)
    assert len(result) == 1
    assert result[0]["_id"] == 2
    assert col.last_find["query"] == {"_id": 2}


@pytest.mark.asyncio
async def test_find_by_fields_range_ops_build_query():
    col = FakeCollectionForFind([])
    mapper = FakeMapperForFind()

    result = await _find_by_fields(
        col,
        mapper,
        {"id": {"between": [1, 5], "gt": 0, "in": [1, 2, 3]}},
        offset=0,
        limit=10,
    )
    assert result == []
    query = col.last_find["query"]["_id"]
    assert query["$gte"] == 1
    assert query["$lte"] == 5
    assert query["$gt"] == 0
    assert query["$in"] == [1, 2, 3]


@pytest.mark.asyncio
async def test_find_by_fields_builds_text_and_logical_query():
    col = FakeCollectionForFind([])
    mapper = FakeMapperForFind()

    await _find_by_fields(
        col,
        mapper,
        {
            "or": [
                {"name": {"icontains": "juan"}},
                {"group": {"contains": "mate"}},
            ]
        },
        offset=0,
        limit=10,
    )

    assert col.last_find["query"]["$or"][0]["name"]["$regex"] == "juan"
    assert col.last_find["query"]["$or"][0]["name"]["$options"] == "i"
    assert col.last_find["query"]["$or"][1]["group"]["$regex"] == "mate"


@pytest.mark.asyncio
async def test_find_by_fields_unsupported_operator_raises():
    col = FakeCollectionForFind([])
    mapper = FakeMapperForFind()

    with pytest.raises(ValueError):
        await _find_by_fields(col, mapper, {"id": {"bad": 1}}, offset=0, limit=10)


@pytest.mark.asyncio
async def test_find_by_fields_invalid_sort_by_raises():
    col = FakeCollectionForFind([])
    mapper = FakeMapperForFind()
    with pytest.raises(ValueError):
        await _find_by_fields(col, mapper, {"id": 1}, offset=0, limit=10, sort_by="bad")


@pytest.mark.asyncio
async def test_find_by_fields_empty_multi_and_empty_in_range_return_empty():
    col = FakeCollectionForFind([])
    mapper = FakeMapperForFind()
    result = await _find_by_fields(col, mapper, {"id": []}, offset=0, limit=10)
    assert result == []
    result2 = await _find_by_fields(col, mapper, {"id": {"in": []}}, offset=0, limit=10)
    assert result2 == []


@pytest.mark.asyncio
async def test_mongo_repository_crud_and_indexes():
    collection = FakeCollection()
    db = FakeDb(collection)
    mapper = FakeMapper()
    repo = MongoRepository[Entity, int](db, mapper)

    e1 = Entity(id=1, name="a")
    e2 = Entity(id=2, name="b")

    assert collection.indexes == []

    await repo.add(e1)
    await repo.add(e2)

    assert len(collection.indexes) == 1
    idx = collection.indexes[0]
    assert idx["name"] == "uniq_by_name"
    assert idx["unique"] is True
    assert idx["keys"] == [("name", 1)]

    g1 = await repo.get(1)
    g2 = await repo.get(2)
    assert g1.id == 1 and g1.name == "a"
    assert g2.id == 2 and g2.name == "b"

    lst = await repo.list(offset=0, limit=10)
    assert [e.id for e in lst] == [1, 2]

    e1_updated = Entity(id=1, name="a2")
    await repo.update(e1_updated)
    g1b = await repo.get(1)
    assert g1b.name == "a2"

    by_name = await repo.get_by_index("by_name", "a2")
    assert by_name.id == 1
    assert await repo.get_by_index("unknown", "x") is None

    await repo.delete(2)
    lst2 = await repo.list(offset=0, limit=10)
    assert [e.id for e in lst2] == [1]

    res = await repo.list_by_fields({"id": 1}, offset=0, limit=10)
    assert len(res) == 1
    assert res[0].id == 1

    await repo.list(offset=0, limit=10)
    await repo.get(1)
    await repo.list_by_fields({"id": 1}, offset=0, limit=10)
    assert len(collection.indexes) == 1


@pytest.mark.asyncio
async def test_mongo_repository_list_invalid_sort_by_raises():
    collection = FakeCollection()
    db = FakeDb(collection)
    mapper = FakeMapper()
    repo = MongoRepository[Entity, int](db, mapper)
    with pytest.raises(ValueError):
        await repo.list(offset=0, limit=10, sort_by="bad")


@pytest.mark.asyncio
async def test_mongo_repository_count_and_count_by_fields():
    collection = FakeCollection()
    collection.docs = [
        {"_id": 1, "name": "a"},
        {"_id": 2, "name": "b"},
        {"_id": 3, "name": "a"},
    ]
    db = FakeDb(collection)
    mapper = FakeMapper()
    repo = MongoRepository[Entity, int](db, mapper)
    repo._inited = True

    total = await repo.count()
    filtered = await repo.count_by_fields({"name": "a"})

    assert total == 3
    assert filtered == 2


@pytest.mark.asyncio
async def test_mongo_repository_distinct_values_returns_unique_non_null_values():
    collection = FakeCollection()
    collection.docs = [
        {"_id": 1, "name": "a"},
        {"_id": 2, "name": "b"},
        {"_id": 3, "name": "a"},
        {"_id": 4, "name": None},
    ]
    db = FakeDb(collection)
    mapper = FakeMapper()
    repo = MongoRepository[Entity, int](db, mapper)
    repo._inited = True

    result = await repo.distinct_values("name")

    assert result == ["a", "b"]


@pytest.mark.asyncio
async def test_mongo_repository_distinct_values_applies_criteria():
    collection = FakeCollection()
    collection.docs = [
        {"_id": 1, "name": "a"},
        {"_id": 2, "name": "b"},
        {"_id": 3, "name": "a"},
    ]
    db = FakeDb(collection)
    mapper = FakeMapper()
    repo = MongoRepository[Entity, int](db, mapper)
    repo._inited = True

    result = await repo.distinct_values("id", {"name": "a"})

    assert result == [1, 3]
