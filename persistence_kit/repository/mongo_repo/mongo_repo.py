from __future__ import annotations

from typing import Optional, Sequence, TypeVar, Generic, Hashable, Mapping, Protocol, Any
try:
    from typing import override
except ImportError:
    from typing_extensions import override

import asyncio
from motor.motor_asyncio import AsyncIOMotorDatabase, AsyncIOMotorCollection
from persistence_kit.abstract_repository import Repository
from persistence_kit.repository.filter_ops import (
    is_multi_value,
    is_range_dict,
    iter_range_ops,
)

T = TypeVar("T")
TId = TypeVar("TId", bound=Hashable)

class EntityMapper(Protocol[T, TId]):
    def collection(self) -> str: ...
    def id_of(self, entity: T) -> TId: ...
    def to_document(self, entity: T) -> dict: ...
    def from_document(self, doc: Mapping) -> T: ...
    def id_field(self) -> str: ...
    def unique_fields(self) -> dict[str, str]: ...
    def has_attr(self, name: str) -> bool: ...
    def attr_to_storage(self, name: str) -> str: ...

def _normalize_field(mapper: EntityMapper[T, TId], field: str) -> str:
    return mapper.attr_to_storage(field)

def _range_to_mongo(value: Mapping[str, Any]) -> Mapping[str, Any]:
    query: dict[str, Any] = {}
    for op, v in iter_range_ops(value):
        if op == "between":
            query["$gte"] = v[0]
            query["$lte"] = v[1]
        elif op == "gte":
            query["$gte"] = v
        elif op == "gt":
            query["$gt"] = v
        elif op == "lte":
            query["$lte"] = v
        elif op == "lt":
            query["$lt"] = v
        elif op == "in":
            query["$in"] = v
        elif op == "eq":
            query["$eq"] = v
        elif op == "ne":
            query["$ne"] = v
    return query

async def _find_by_fields(
    col: AsyncIOMotorCollection,
    mapper: EntityMapper[T, TId],
    criteria: Mapping[str, Hashable | list[Hashable] | Mapping[str, Any]],
    *,
    offset: int = 0,
    limit: Optional[int] = 50,
    sort_by: str | None = None,
    sort_desc: bool = False,
) -> Sequence[T]:
    if not criteria:
        return []
    for v in criteria.values():
        if is_multi_value(v) and not v:
            return []
        if is_range_dict(v) and v.get("in") == []:
            return []

    query: dict[str, Any] = {}
    for k, v in criteria.items():
        f = _normalize_field(mapper, k)
        if v is None:
            query[f] = {"$eq": None}
        elif is_multi_value(v):
            query[f] = {"$in": list(v)}
        elif is_range_dict(v):
            mongo_range = _range_to_mongo(v)
            if not mongo_range:
                return []
            query[f] = mongo_range
        else:
            query[f] = v

    cursor = col.find(query)

    if sort_by is not None:
        if not mapper.has_attr(sort_by):
            raise ValueError(f"Invalid sort attribute: {sort_by}")
        field = _normalize_field(mapper, sort_by)
        direction = -1 if sort_desc else 1
        cursor = cursor.sort(field, direction)

    if offset:
        cursor = cursor.skip(offset)
    if limit is not None:
        cursor = cursor.limit(limit)

    docs = await cursor.to_list(length=0 if limit is None else limit)
    return [mapper.from_document(d) for d in docs]

class MongoRepository(Repository[T, TId], Generic[T, TId]):
    def __init__(self, db: AsyncIOMotorDatabase, mapper: EntityMapper[T, TId]) -> None:
        self._db = db
        self._mapper = mapper
        self._col: AsyncIOMotorCollection = db[mapper.collection()]
        self._inited = False
        self._init_lock = asyncio.Lock()

    async def init_indexes(self) -> None:
        for name, field in self._mapper.unique_fields().items():
            await self._col.create_index([(field, 1)], name=f"uniq_{name}", unique=True)

    async def _ensure_indexes(self) -> None:
        if self._inited:
            return
        async with self._init_lock:
            if not self._inited:
                await self.init_indexes()
                self._inited = True

    @override
    async def add(self, entity: T) -> None:
        await self._ensure_indexes()
        eid = self._mapper.id_of(entity)
        doc = self._mapper.to_document(entity)
        doc[self._mapper.id_field()] = eid
        await self._col.insert_one(doc)

    @override
    async def get(self, entity_id: TId) -> Optional[T]:
        await self._ensure_indexes()
        doc = await self._col.find_one({self._mapper.id_field(): entity_id})
        return self._mapper.from_document(doc) if doc else None

    @override
    async def list(
        self,
        *,
        offset: int = 0,
        limit: int = 50,
        sort_by: str | None = None,
        sort_desc: bool = False,
    ) -> Sequence[T]:
        await self._ensure_indexes()

        cursor = self._col.find({})

        if sort_by is not None:
            if not self._mapper.has_attr(sort_by):
                raise ValueError(f"Invalid sort attribute: {sort_by}")
            field = _normalize_field(self._mapper, sort_by)
            direction = -1 if sort_desc else 1
            cursor = cursor.sort(field, direction)

        cursor = cursor.skip(offset).limit(limit)
        docs = await cursor.to_list(length=limit)
        return [self._mapper.from_document(d) for d in docs]

    @override
    async def update(self, entity: T) -> None:
        await self._ensure_indexes()
        eid = self._mapper.id_of(entity)
        doc = self._mapper.to_document(entity)
        doc[self._mapper.id_field()] = eid
        await self._col.replace_one({self._mapper.id_field(): eid}, doc, upsert=False)

    @override
    async def delete(self, entity_id: TId) -> None:
        await self._ensure_indexes()
        await self._col.delete_one({self._mapper.id_field(): entity_id})

    @override
    async def get_by_index(self, index: str, value: Hashable) -> Optional[T]:
        await self._ensure_indexes()
        field = self._mapper.unique_fields().get(index)
        if not field:
            return None
        doc = await self._col.find_one({field: value})
        return self._mapper.from_document(doc) if doc else None

    @override
    async def list_by_fields(
        self,
        criteria: Mapping[str, Hashable | list[Hashable] | Mapping[str, Any]],
        *,
        offset: int = 0,
        limit: Optional[int] = 50,
        sort_by: str | None = None,
        sort_desc: bool = False,
    ) -> Sequence[T]:
        await self._ensure_indexes()
        return await _find_by_fields(
            self._col,
            self._mapper,
            criteria,
            offset=offset,
            limit=limit,
            sort_by=sort_by,
            sort_desc=sort_desc,
        )
