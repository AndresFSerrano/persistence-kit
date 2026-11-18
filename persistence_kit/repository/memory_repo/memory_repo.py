from __future__ import annotations

import copy
from functools import partial
from typing import Optional, Sequence, TypeVar, Generic, Callable, Hashable, Mapping, Any
try:
    from typing import override
except ImportError:
    from typing_extensions import override

from persistence_kit.contracts.repository import Repository
from persistence_kit.repository.filter_ops import (
    is_multi_value,
    is_range_dict,
    match_criteria,
)
from persistence_kit.repository.transaction_context import RowLocks, current_transaction, require_transaction

T = TypeVar("T")
TId = TypeVar("TId", bound=Hashable)

def _get_field_value(entity: Any, field: str) -> Any:
    if isinstance(entity, dict):
        return entity.get(field)
    return getattr(entity, field, None)


def _detached(entity: T) -> T:
    return copy.deepcopy(entity) if current_transaction() is not None else entity


class MemoryRepository(Repository[T, TId], Generic[T, TId]):
    def __init__(
        self,
        id_getter: Callable[[T], TId],
        unique_indexes: dict[str, Callable[[T], Hashable]] | None = None,
    ) -> None:
        self._id_getter = id_getter
        self._items: dict[TId, T] = {}
        self._indexes: dict[str, dict[Hashable, TId]] = {k: {} for k in (unique_indexes or {})}
        self._extractors: dict[str, Callable[[T], Hashable]] = dict(unique_indexes or {})
        self._row_locks = RowLocks()

    async def _claim(self, entity_id: TId) -> None:
        tx = current_transaction()
        if tx is not None:
            await tx.lock_row(self._row_locks, entity_id, partial(self._snapshot, entity_id))

    def _snapshot(self, entity_id: TId) -> Callable[[], None]:
        return partial(self._restore, entity_id, copy.deepcopy(self._items.get(entity_id)))

    def _restore(self, entity_id: TId, saved: T | None) -> None:
        self._items.pop(entity_id, None)
        for idx in self._indexes.values():
            for value in [value for value, owner in idx.items() if owner == entity_id]:
                del idx[value]
        if saved is None:
            return
        self._items[entity_id] = saved
        for name, ext in self._extractors.items():
            self._indexes[name][ext(saved)] = entity_id

    @override
    async def add(self, entity: T) -> None:
        eid = self._id_getter(entity)
        await self._claim(eid)
        self._items[eid] = entity
        for name, ext in self._extractors.items():
            self._indexes[name][ext(entity)] = eid

    @override
    async def get(self, entity_id: TId, *, for_update: bool = False) -> Optional[T]:
        if for_update:
            require_transaction()
            await self._claim(entity_id)
        return _detached(self._items.get(entity_id))

    @override
    async def list(
        self,
        *,
        offset: int = 0,
        limit: int = 50,
        sort_by: str | None = None,
        sort_desc: bool = False,
    ) -> Sequence[T]:
        vals = list(self._items.values())
        if sort_by is not None:
            vals.sort(
                key=lambda e: _get_field_value(e, sort_by),
                reverse=sort_desc,
            )
        return [_detached(entity) for entity in vals[offset: offset + limit]]

    @override
    async def update(self, entity: T) -> None:
        eid = self._id_getter(entity)
        await self._claim(eid)
        self._items[eid] = entity
        for name, ext in self._extractors.items():
            self._indexes[name][ext(entity)] = eid

    @override
    async def delete(self, entity_id: TId) -> None:
        await self._claim(entity_id)
        ent = self._items.pop(entity_id, None)
        if not ent:
            return
        for name, ext in self._extractors.items():
            val = ext(ent)
            idx = self._indexes[name]
            if idx.get(val) == entity_id:
                idx.pop(val, None)

    @override
    async def get_by_index(self, index: str, value: Hashable) -> Optional[T]:
        idx = self._indexes.get(index)
        if not idx:
            return None
        eid = idx.get(value)
        return _detached(self._items.get(eid)) if eid else None

    @override
    async def count(self) -> int:
        return len(self._items)

    @override
    async def count_by_fields(
        self,
        criteria: Mapping[str, Hashable | list[Hashable] | Mapping[str, Any]],
    ) -> int:
        if not criteria:
            return len(self._items)
        rows = await self.list_by_fields(criteria, offset=0, limit=None)
        return len(rows)

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
        if not criteria:
            return []
        for v in criteria.values():
            if is_multi_value(v) and not v:
                return []
            if is_range_dict(v) and v.get("in") == []:
                return []
        matched: list[T] = []
        for ent in self._items.values():
            if match_criteria(criteria, lambda field: _get_field_value(ent, field)):
                matched.append(ent)
        if sort_by is not None:
            matched.sort(
                key=lambda e: _get_field_value(e, sort_by),
                reverse=sort_desc,
            )
        if offset:
            matched = matched[offset:]
        if limit is not None:
            matched = matched[:limit]
        return [_detached(entity) for entity in matched]

    @override
    async def distinct_values(
        self,
        field: str,
        criteria: Mapping[str, Hashable | list[Hashable] | Mapping[str, Any]] | None = None,
    ) -> Sequence[Any]:
        rows: Sequence[T]
        if criteria:
            rows = await self.list_by_fields(criteria, offset=0, limit=None)
        else:
            rows = await self.list(offset=0, limit=len(self._items) or 1)
        seen: list[Any] = []
        for row in rows:
            value = _get_field_value(row, field)
            if value is None or value in seen:
                continue
            seen.append(value)
        return seen
