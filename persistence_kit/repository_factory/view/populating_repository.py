from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any, Callable, Generic, Hashable, Iterable, Mapping, Optional, Sequence, TypeVar

from persistence_kit.contracts.repository import Repository
from persistence_kit.contracts.view_repository import ViewRepository
from persistence_kit.repository.filter_ops import (
    is_logical_key,
    iter_criteria_groups,
    match_criteria,
)
from persistence_kit.repository_factory.registry.entity_registry import get_entity_config

T = TypeVar("T")
TId = TypeVar("TId", bound=Hashable)


def _to_dict(x: Any) -> dict:
    if is_dataclass(x):
        return asdict(x)
    if isinstance(x, dict):
        return dict(x)
    return {}


def _normalize_sort_value(val: Any) -> Any:
    return "" if val is None else val


def _field_from_entity(entity: Any, field: str, base: dict | None = None) -> Any:
    if hasattr(entity, field):
        return getattr(entity, field)
    payload = base if base is not None else _to_dict(entity)
    return payload.get(field)


def _get_nested_value(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if not isinstance(current, Mapping):
            return None
        current = current.get(part)
        if current is None:
            return None
    return current


def _criteria_has_nested_fields(criteria: Mapping[str, Any]) -> bool:
    for field, expected in criteria.items():
        if is_logical_key(field):
            for group in iter_criteria_groups(expected):
                if _criteria_has_nested_fields(group):
                    return True
            continue
        if "." in field:
            return True
    return False


def _collect_support_includes(criteria: Mapping[str, Any]) -> set[str]:
    required: set[str] = set()
    for field, expected in criteria.items():
        if is_logical_key(field):
            for group in iter_criteria_groups(expected):
                required.update(_collect_support_includes(group))
            continue
        if "." not in field:
            continue
        relation_path, _, _ = field.rpartition(".")
        if relation_path:
            required.add(relation_path)
    return required


def _merge_includes(include: Iterable[str], extra: Iterable[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for value in list(include) + list(extra):
        if value in seen:
            continue
        seen.add(value)
        merged.append(value)
    return merged


class PopulatingRepository(ViewRepository[T, TId], Repository[T, TId], Generic[T, TId]):
    def __init__(
        self,
        entity_key: str,
        inner: Repository[T, TId],
        resolve_repo: Callable[[str], Repository[Any, Any]],
    ) -> None:
        self._key = entity_key
        self._inner = inner
        self._resolve = resolve_repo

    async def add(self, entity: T) -> None:
        await self._inner.add(entity)

    async def get(self, entity_id: TId) -> Optional[T]:
        return await self._inner.get(entity_id)

    async def list(
        self,
        *,
        offset: int = 0,
        limit: int = 50,
        sort_by: str | None = None,
        sort_desc: bool = False,
    ) -> Sequence[T]:
        return await self._inner.list(
            offset=offset,
            limit=limit,
            sort_by=sort_by,
            sort_desc=sort_desc,
        )

    async def update(self, entity: T) -> None:
        await self._inner.update(entity)

    async def delete(self, entity_id: TId) -> None:
        await self._inner.delete(entity_id)

    async def get_by_index(self, index: str, value: Hashable) -> Optional[T]:
        return await self._inner.get_by_index(index, value)

    async def count(self) -> int:
        return await self._inner.count()

    async def count_by_fields(
        self,
        criteria: Mapping[str, Hashable | list[Hashable] | Mapping[str, Any]],
    ) -> int:
        if criteria and _criteria_has_nested_fields(criteria):
            support_includes = _collect_support_includes(criteria)
            items = await self._list_all_entities()
            lookup_cache: dict[tuple[str, str, Hashable], Any] = {}
            count = 0
            for item in items:
                row = await self._populate(item, support_includes, lookup_cache)
                if self._row_matches_criteria(row, criteria):
                    count += 1
            return count
        return await self._inner.count_by_fields(criteria)

    async def distinct_values(
        self,
        field: str,
        criteria: Mapping[str, Hashable | list[Hashable] | Mapping[str, Any]] | None = None,
    ) -> Sequence[Any]:
        return await self._inner.distinct_values(field, criteria)

    async def _list_all_entities(self) -> list[T]:
        all_items: list[T] = []
        cursor = 0
        page_size = 1000
        while True:
            batch = await self._inner.list(offset=cursor, limit=page_size)
            if not batch:
                break
            all_items.extend(batch)
            if len(batch) < page_size:
                break
            cursor += page_size
        return all_items

    def _sort_populated_rows(
        self,
        rows: list[tuple[T, dict]],
        sort_by: str | None,
        *,
        sort_desc: bool,
    ) -> list[tuple[T, dict]]:
        if sort_by is None:
            return rows
        return sorted(
            rows,
            key=lambda pair: _normalize_sort_value(_get_nested_value(pair[1], sort_by)),
            reverse=sort_desc,
        )

    def _row_matches_criteria(
        self,
        row: Mapping[str, Any],
        criteria: Mapping[str, Hashable | list[Hashable] | Mapping[str, Any]],
    ) -> bool:
        return match_criteria(criteria, lambda field: _get_nested_value(row, field))

    async def get_with(self, entity_id: TId, include: Iterable[str]) -> Optional[dict]:
        ent = await self.get(entity_id)
        if not ent:
            return None
        return await self._populate(ent, include)

    async def get_by_index_with(
        self,
        index: str,
        value: Hashable,
        include: Iterable[str],
    ) -> Optional[dict]:
        ent = await self.get_by_index(index, value)
        if not ent:
            return None
        return await self._populate(ent, include)

    async def list_with(
        self,
        *,
        offset: int = 0,
        limit: int = 50,
        include: Iterable[str],
        sort_by: str | None = None,
        sort_desc: bool = False,
    ) -> list[dict]:
        lookup_cache: dict[tuple[str, str, Hashable], Any] = {}
        if sort_by and "." in sort_by:
            all_items = await self._list_all_entities()
            ordered = await self._sort_entities_by_nested(
                all_items,
                sort_by,
                sort_desc=sort_desc,
                lookup_cache=lookup_cache,
            )
            if offset:
                ordered = ordered[offset:]
            if limit is not None:
                ordered = ordered[:limit]
            return [await self._populate(it, include, lookup_cache) for it in ordered]

        items = await self._inner.list(
            offset=offset,
            limit=limit,
            sort_by=sort_by,
            sort_desc=sort_desc,
        )
        return [await self._populate(it, include, lookup_cache) for it in items]

    async def list_by_fields(
        self,
        criteria: Mapping[str, Hashable | list[Hashable] | Mapping[str, Any]],
        *,
        offset: int = 0,
        limit: Optional[int] = 50,
        include: Iterable[str],
        sort_by: str | None = None,
        sort_desc: bool = False,
    ) -> list[dict]:
        lookup_cache: dict[tuple[str, str, Hashable], Any] = {}
        requested_includes = list(include)
        if criteria and _criteria_has_nested_fields(criteria):
            support_includes = _merge_includes(requested_includes, _collect_support_includes(criteria))
            if sort_by and "." in sort_by:
                relation_path, _, _ = sort_by.rpartition(".")
                support_includes = _merge_includes(support_includes, [relation_path])
            items = await self._list_all_entities()
            hydrated: list[tuple[T, dict]] = []
            for item in items:
                support_row = await self._populate(item, support_includes, lookup_cache)
                if self._row_matches_criteria(support_row, criteria):
                    hydrated.append((item, support_row))
            hydrated = self._sort_populated_rows(
                hydrated,
                sort_by,
                sort_desc=sort_desc,
            )
            if offset:
                hydrated = hydrated[offset:]
            if limit is not None:
                hydrated = hydrated[:limit]
            if support_includes == requested_includes:
                return [row for _, row in hydrated]
            return [await self._populate(item, requested_includes, lookup_cache) for item, _ in hydrated]
        if sort_by and "." in sort_by:
            items = await self._inner.list_by_fields(
                criteria,
                offset=0,
                limit=None,
                sort_by=None,
                sort_desc=False,
            )
            ordered = await self._sort_entities_by_nested(
                list(items),
                sort_by,
                sort_desc=sort_desc,
                lookup_cache=lookup_cache,
            )
            if offset:
                ordered = ordered[offset:]
            if limit is not None:
                ordered = ordered[:limit]
            return [await self._populate(it, include, lookup_cache) for it in ordered]

        items = await self._inner.list_by_fields(
            criteria,
            offset=offset,
            limit=limit,
            sort_by=sort_by,
            sort_desc=sort_desc,
        )
        return [await self._populate(it, include, lookup_cache) for it in items]

    async def _resolve_relation(
        self,
        target: str,
        by: str,
        value: Hashable,
        lookup_cache: dict[tuple[str, str, Hashable], Any],
    ) -> Any:
        cache_key = (target, by, value)
        if cache_key in lookup_cache:
            return lookup_cache[cache_key]
        repo = self._resolve(target)
        ref = await (repo.get(value) if by == "id" else repo.get_by_index(by, value))
        lookup_cache[cache_key] = ref
        return ref

    async def _value_for_nested_sort(
        self,
        item: T,
        sort_by: str,
        lookup_cache: dict[tuple[str, str, Hashable], Any],
    ) -> Any:
        parts = sort_by.split(".")
        current_entity: Any = item
        current_key = self._key
        for idx, part in enumerate(parts):
            cfg = get_entity_config(current_key)
            rels = cfg.get("relations") or {}
            rel = rels.get(part)
            if not rel:
                return _field_from_entity(current_entity, part, _to_dict(current_entity))
            if bool(rel.get("many", False)):
                return None
            local_field = rel["local_field"]
            target = rel["target"]
            by = rel.get("by", "id")
            value = _field_from_entity(current_entity, local_field, _to_dict(current_entity))
            if value is None or not isinstance(value, Hashable):
                return None
            current_entity = await self._resolve_relation(target, by, value, lookup_cache)
            if current_entity is None:
                return None
            current_key = target
            if idx == len(parts) - 1:
                return _to_dict(current_entity)
        return None

    async def _sort_entities_by_nested(
        self,
        items: list[T],
        sort_by: str,
        *,
        sort_desc: bool,
        lookup_cache: dict[tuple[str, str, Hashable], Any],
    ) -> list[T]:
        decorated: list[tuple[Any, T]] = []
        for it in items:
            raw = await self._value_for_nested_sort(it, sort_by, lookup_cache)
            decorated.append((_normalize_sort_value(raw), it))
        decorated.sort(key=lambda pair: pair[0], reverse=sort_desc)
        return [it for _, it in decorated]

    async def _populate(
        self,
        item: T,
        include: Iterable[str],
        lookup_cache: Optional[dict[tuple[str, str, Hashable], Any]] = None,
    ) -> dict:
        cache = lookup_cache if lookup_cache is not None else {}
        cfg = get_entity_config(self._key)
        rels = cfg.get("relations") or {}
        base = _to_dict(item)

        roots: set[str] = set()
        nested: dict[str, list[str]] = {}
        for inc in include:
            if "." in inc:
                root, rest = inc.split(".", 1)
                roots.add(root)
                nested.setdefault(root, []).append(rest)
            else:
                roots.add(inc)

        for field in roots:
            relation = rels.get(field)
            if not relation:
                continue

            target = relation["target"]
            by = relation.get("by", "id")
            many = bool(relation.get("many", False))
            child_includes = nested.get(field, [])

            if relation.get("through"):
                source_field = relation.get("source_field", "id")
                target_field = relation.get("target_field", "id")
                source_by = relation.get("source_by", "id")
                target_by = relation.get("target_by", "id")
                source_value = getattr(item, "id", None) if source_by == "id" else base.get(source_by)
                if source_value is None:
                    base[field] = [] if many else None
                    continue
                link_repo = self._resolve(relation["through"])
                links = await link_repo.find_all_by_index(source_field, [source_value])  # type: ignore[attr-defined]
                target_values = [
                    candidate
                    for candidate in (_to_dict(link).get(target_field) for link in links)
                    if isinstance(candidate, Hashable)
                ]
                target_repo = self._resolve(target)
                if many:
                    acc: list[dict] = []
                    for candidate in target_values:
                        ref = await (
                            target_repo.get(candidate)
                            if target_by == "id"
                            else target_repo.get_by_index(target_by, candidate)
                        )
                        if ref is None:
                            continue
                        if child_includes:
                            nested_repo = PopulatingRepository(target, target_repo, self._resolve)
                            acc.append(await nested_repo._populate(ref, child_includes, cache))
                        else:
                            acc.append(_to_dict(ref))
                    base[field] = acc
                else:
                    candidate = target_values[0] if target_values else None
                    if candidate is None:
                        base[field] = None
                    else:
                        ref = await (
                            target_repo.get(candidate)
                            if target_by == "id"
                            else target_repo.get_by_index(target_by, candidate)
                        )
                        if ref is None:
                            base[field] = None
                        elif child_includes:
                            nested_repo = PopulatingRepository(target, target_repo, self._resolve)
                            base[field] = await nested_repo._populate(ref, child_includes, cache)
                        else:
                            base[field] = _to_dict(ref)
                continue

            local_field = relation["local_field"]
            reverse_target_field = relation.get("target_field")
            value = getattr(item, local_field, None) if hasattr(item, local_field) else base.get(local_field)
            if value is None:
                base[field] = [] if many else None
                continue

            if many and reverse_target_field:
                target_repo = self._resolve(target)
                refs = await target_repo.list_by_fields({reverse_target_field: value}, limit=None)
                acc: list[dict] = []
                for ref in refs:
                    if child_includes:
                        nested_repo = PopulatingRepository(target, target_repo, self._resolve)
                        acc.append(await nested_repo._populate(ref, child_includes, cache))
                    else:
                        acc.append(_to_dict(ref))
                base[field] = acc
                continue

            if many:
                seq = list(value) if isinstance(value, (list, tuple, set)) else [value]
                acc: list[dict] = []
                for candidate in seq:
                    if not isinstance(candidate, Hashable):
                        continue
                    ref = await self._resolve_relation(target, by, candidate, cache)
                    if ref is None:
                        continue
                    if child_includes:
                        nested_repo = PopulatingRepository(target, self._resolve(target), self._resolve)
                        acc.append(await nested_repo._populate(ref, child_includes, cache))
                    else:
                        acc.append(_to_dict(ref))
                base[field] = acc
                continue

            if not isinstance(value, Hashable):
                base[field] = None
                continue
            ref = await self._resolve_relation(target, by, value, cache)
            if ref is None:
                base[field] = None
            elif child_includes:
                nested_repo = PopulatingRepository(target, self._resolve(target), self._resolve)
                base[field] = await nested_repo._populate(ref, child_includes, cache)
            else:
                base[field] = _to_dict(ref)

        return base
