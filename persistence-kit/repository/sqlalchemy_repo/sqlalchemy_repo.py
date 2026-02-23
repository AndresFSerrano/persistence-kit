from __future__ import annotations

from typing import Any, Optional, Sequence, TypeVar, Generic, Hashable, Mapping, Protocol
try:
    from typing import override
except ImportError:
    from typing_extensions import override

from sqlalchemy import Table, select, insert, update as sql_update, delete as sql_delete, Index, and_
from sqlalchemy.ext.asyncio import AsyncEngine

from persistence_kit.abstract_repository import Repository
from persistence_kit.repository.filter_ops import (
    is_multi_value,
    is_range_dict,
    iter_range_ops,
)
from persistence_kit.repository.sqlalchemy_repo.schema_evolve import (
    ensure_missing_columns,
    ensure_foreign_keys,
)
from persistence_kit.repository_factory.entity_registry import build_fk_map_from_registry

T = TypeVar("T")
TId = TypeVar("TId", bound=Hashable)


class SqlAlchemyEntityMapper(Protocol[T, TId]):
    def table(self) -> Table: ...
    def id_of(self, entity: T) -> TId: ...
    def to_row(self, entity: T) -> Mapping[str, Any]: ...
    def from_row(self, row: Mapping[str, Any]) -> T: ...
    def id_column(self) -> str: ...
    def unique_columns(self) -> dict[str, str]: ...
    def entity_type(self) -> type[T]: ...
    def has_attr(self, name: str) -> bool: ...
    def attr_to_storage(self, name: str) -> str: ...


async def _select_by_fields(
    engine: AsyncEngine,
    mapper: SqlAlchemyEntityMapper[T, TId],
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

    table = mapper.table()
    clauses = []

    for field, value in criteria.items():
        if not mapper.has_attr(field):
            raise ValueError(
                f"Field '{field}' is not a valid attribute for {mapper.entity_type().__name__}"
            )
        col_name = mapper.attr_to_storage(field)
        try:
            col = table.c[col_name]
        except KeyError as e:
            raise ValueError(f"Column '{col_name}' does not exist on table '{table.name}'") from e
        if value is None:
            clauses.append(col.is_(None))
        elif is_multi_value(value):
            clauses.append(col.in_(list(value)))
        elif is_range_dict(value):
            ops = list(iter_range_ops(value))
            if not ops:
                return []
            for op, v in ops:
                if op == "between":
                    clauses.append(col.between(v[0], v[1]))
                elif op == "gte":
                    clauses.append(col >= v)
                elif op == "gt":
                    clauses.append(col > v)
                elif op == "lte":
                    clauses.append(col <= v)
                elif op == "lt":
                    clauses.append(col < v)
                elif op == "in":
                    clauses.append(col.in_(v))
                elif op == "eq":
                    clauses.append(col == v)
                elif op == "ne":
                    clauses.append(col != v)
        else:
            clauses.append(col == value)

    stmt = select(table).where(and_(*clauses))

    if sort_by is not None:
        if not mapper.has_attr(sort_by):
            raise ValueError(f"Invalid sort attribute: {sort_by}")
        sort_col_name = mapper.attr_to_storage(sort_by)
    else:
        sort_col_name = mapper.id_column()

    sort_col = table.c[sort_col_name]
    stmt = stmt.order_by(sort_col.desc() if sort_desc else sort_col.asc()).offset(offset)

    if limit is not None:
        stmt = stmt.limit(limit)

    async with engine.begin() as conn:
        res = await conn.execute(stmt)
        rows = res.mappings().all()

    return [mapper.from_row(dict(r)) for r in rows]


class SqlAlchemyRepository(Repository[T, TId], Generic[T, TId]):
    def __init__(self, engine: AsyncEngine, mapper: SqlAlchemyEntityMapper[T, TId], entity_key: str) -> None:
        self._engine = engine
        self._mapper = mapper
        self._entity_key = entity_key
        self._inited = False

    async def init_indexes(self) -> None:
        table = self._mapper.table()
        uniques = list(self._mapper.unique_columns().items())
        fk_map = build_fk_map_from_registry(self._entity_key)

        def _create_table_and_indexes(sync_conn):
            table.metadata.create_all(sync_conn, tables=[table], checkfirst=True)
            ensure_missing_columns(sync_conn, table, self._mapper.entity_type())
            for name, col in uniques:
                idx_name = f"uniq_{name}_{table.name}"
                idx = Index(idx_name, table.c[col], unique=True)
                idx.create(bind=sync_conn, checkfirst=True)
            ensure_foreign_keys(sync_conn, table, fk_map)

        async with self._engine.begin() as conn:
            await conn.run_sync(_create_table_and_indexes)
        self._inited = True

    async def _ensure_indexes(self) -> None:
        if not self._inited:
            await self.init_indexes()

    @override
    async def add(self, entity: T) -> None:
        await self._ensure_indexes()
        table = self._mapper.table()
        row = dict(self._mapper.to_row(entity))
        row[self._mapper.id_column()] = self._mapper.id_of(entity)
        async with self._engine.begin() as conn:
            await conn.execute(insert(table).values(row))

    @override
    async def get(self, entity_id: TId) -> Optional[T]:
        await self._ensure_indexes()
        table = self._mapper.table()
        id_col = table.c[self._mapper.id_column()]
        stmt = select(table).where(id_col == entity_id).limit(1)
        async with self._engine.begin() as conn:
            res = await conn.execute(stmt)
            row = res.mappings().first()
        return self._mapper.from_row(dict(row)) if row else None

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
        table = self._mapper.table()

        if sort_by is not None:
            if not self._mapper.has_attr(sort_by):
                raise ValueError(f"Invalid sort attribute: {sort_by}")
            col_name = self._mapper.attr_to_storage(sort_by)
        else:
            col_name = self._mapper.id_column()

        col = table.c[col_name]
        stmt = (
            select(table)
            .order_by(col.desc() if sort_desc else col.asc())
            .offset(offset)
            .limit(limit)
        )

        async with self._engine.begin() as conn:
            res = await conn.execute(stmt)
            rows = res.mappings().all()

        return [self._mapper.from_row(dict(r)) for r in rows]

    @override
    async def update(self, entity: T) -> None:
        await self._ensure_indexes()
        table = self._mapper.table()
        eid = self._mapper.id_of(entity)
        row = dict(self._mapper.to_row(entity))
        row.pop(self._mapper.id_column(), None)
        if not row:
            return
        id_col = table.c[self._mapper.id_column()]
        stmt = sql_update(table).where(id_col == eid).values(**row)
        async with self._engine.begin() as conn:
            await conn.execute(stmt)

    @override
    async def delete(self, entity_id: TId) -> None:
        await self._ensure_indexes()
        table = self._mapper.table()
        id_col = table.c[self._mapper.id_column()]
        stmt = sql_delete(table).where(id_col == entity_id)
        async with self._engine.begin() as conn:
            await conn.execute(stmt)

    @override
    async def get_by_index(self, index: str, value: Hashable) -> Optional[T]:
        await self._ensure_indexes()
        col_name = self._mapper.unique_columns().get(index)
        if not col_name:
            return None
        table = self._mapper.table()
        col = table.c[col_name]
        stmt = select(table).where(col == value).limit(1)
        async with self._engine.begin() as conn:
            res = await conn.execute(stmt)
            row = res.mappings().first()
        return self._mapper.from_row(dict(row)) if row else None

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
        return await _select_by_fields(
            self._engine,
            self._mapper,
            criteria,
            offset=offset,
            limit=limit,
            sort_by=sort_by,
            sort_desc=sort_desc,
        )
