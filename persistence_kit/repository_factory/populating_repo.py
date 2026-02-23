from __future__ import annotations
from dataclasses import asdict, is_dataclass
from typing import Any, Iterable, Optional, Sequence, TypeVar, Generic, Hashable, Callable, Mapping

from persistence_kit.abstract_repository import Repository
from persistence_kit.abstract_view_repo import ViewRepository
from .entity_registry import get_entity_config

T = TypeVar("T")
TId = TypeVar("TId", bound=Hashable)

def _to_dict(x: Any) -> dict:
    if is_dataclass(x):
        return asdict(x)
    if isinstance(x, dict):
        return dict(x)
    return {}

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

    async def list(self, *, offset: int = 0, limit: int = 50) -> Sequence[T]:
        return await self._inner.list(offset=offset, limit=limit)

    async def update(self, entity: T) -> None:
        await self._inner.update(entity)

    async def delete(self, entity_id: TId) -> None:
        await self._inner.delete(entity_id)

    async def get_by_index(self, index: str, value: Hashable) -> Optional[T]:
        return await self._inner.get_by_index(index, value)

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
        items = await self._inner.list(
            offset=offset,
            limit=limit,
            sort_by=sort_by,
            sort_desc=sort_desc,
        )
        out: list[dict] = []
        for it in items:
            out.append(await self._populate(it, include))
        return out

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
        items = await self._inner.list_by_fields(
            criteria,
            offset=offset,
            limit=limit,
            sort_by=sort_by,
            sort_desc=sort_desc,
        )
        out: list[dict] = []
        for it in items:
            out.append(await self._populate(it, include))
        return out

    async def _populate(self, item: T, include: Iterable[str]) -> dict:
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

        for f in roots:
            r = rels.get(f)
            if not r:
                continue

            lf = r["local_field"]
            target = r["target"]
            by = r.get("by", "id")
            many = bool(r.get("many", False))
            val = getattr(item, lf, None) if hasattr(item, lf) else base.get(lf)
            repo = self._resolve(target)
            child_includes = nested.get(f, [])

            if val is None:
                base[f] = [] if many else None
                continue

            if many:
                seq = list(val) if isinstance(val, (list, tuple, set)) else [val]
                acc: list[dict] = []
                for v in seq:
                    ref = await (repo.get(v) if by == "id" else repo.get_by_index(by, v))
                    if ref is None:
                        continue
                    if child_includes:
                        nested_repo = PopulatingRepository(target, repo, self._resolve)
                        acc.append(await nested_repo._populate(ref, child_includes))
                    else:
                        acc.append(_to_dict(ref))
                base[f] = acc
            else:
                ref = await (repo.get(val) if by == "id" else repo.get_by_index(by, val))
                if ref is None:
                    base[f] = None
                elif child_includes:
                    nested_repo = PopulatingRepository(target, repo, self._resolve)
                    base[f] = await nested_repo._populate(ref, child_includes)
                else:
                    base[f] = _to_dict(ref)

        return base
