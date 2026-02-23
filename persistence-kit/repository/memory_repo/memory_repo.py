from __future__ import annotations

from typing import Optional, Sequence, TypeVar, Generic, Callable, Hashable, Mapping, Any
try:
    from typing import override
except ImportError:
    from typing_extensions import override

from persistence_kit.abstract_repository import Repository
from persistence_kit.repository.filter_ops import (
    is_multi_value,
    is_range_dict,
    iter_range_ops,
)

T = TypeVar("T")
TId = TypeVar("TId", bound=Hashable)

def _get_field_value(entity: Any, field: str) -> Any:
    if isinstance(entity, dict):
        return entity.get(field)
    return getattr(entity, field, None)

def _match_value(val: Any, cond: Any) -> bool:
    if cond is None:
        return val is None
    if is_multi_value(cond):
        return val in cond
    if is_range_dict(cond):
        ops = list(iter_range_ops(cond))
        if not ops:
            return False
        for op, v in ops:
            if op == "between":
                lo, hi = v
                if val is None or val < lo or val > hi:
                    return False
            elif op == "gte":
                if val is None or val < v:
                    return False
            elif op == "gt":
                if val is None or val <= v:
                    return False
            elif op == "lte":
                if val is None or val > v:
                    return False
            elif op == "lt":
                if val is None or val >= v:
                    return False
            elif op == "in":
                if val not in v:
                    return False
            elif op == "eq":
                if val != v:
                    return False
            elif op == "ne":
                if val == v:
                    return False
        return True
    return val == cond

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

    @override
    async def add(self, entity: T) -> None:
        eid = self._id_getter(entity)
        self._items[eid] = entity
        for name, ext in self._extractors.items():
            self._indexes[name][ext(entity)] = eid

    @override
    async def get(self, entity_id: TId) -> Optional[T]:
        return self._items.get(entity_id)

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
        return vals[offset: offset + limit]

    @override
    async def update(self, entity: T) -> None:
        eid = self._id_getter(entity)
        self._items[eid] = entity
        for name, ext in self._extractors.items():
            self._indexes[name][ext(entity)] = eid

    @override
    async def delete(self, entity_id: TId) -> None:
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
        return self._items.get(eid) if eid else None

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
            ok = True
            for k, v in criteria.items():
                val = _get_field_value(ent, k)
                if not _match_value(val, v):
                    ok = False
                    break
            if ok:
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
        return matched
