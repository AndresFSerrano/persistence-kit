from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table
from sqlalchemy.sql import Delete, Insert, Select, Update

from persistence_kit.repository.sqlalchemy_repo.sqlalchemy_repo import SqlAlchemyRepository, _select_by_fields


@dataclass
class Item:
    id: int
    name: str
    category: Optional[str]


class ItemMapper:
    def __init__(self, table: Table):
        self._table = table
        self._attrs = {"id", "name", "category"}

    def table(self) -> Table:
        return self._table

    def id_of(self, entity: Item) -> int:
        return entity.id

    def to_row(self, entity: Item) -> Mapping[str, Any]:
        return {"id": entity.id, "name": entity.name, "category": entity.category}

    def from_row(self, row: Mapping[str, Any]) -> Item:
        return Item(**row)

    def id_column(self) -> str:
        return "id"

    def unique_columns(self) -> dict[str, str]:
        return {"by_name": "name"}

    def entity_type(self):
        return Item

    def has_attr(self, name: str) -> bool:
        return name in self._attrs

    def attr_to_storage(self, name: str) -> str:
        return name


class IdOnlyMapper(ItemMapper):
    def __init__(self, table: Table):
        self._table = table
        self._attrs = {"id"}

    def to_row(self, entity: Item) -> Mapping[str, Any]:
        return {"id": entity.id}

    def unique_columns(self) -> dict[str, str]:
        return {}


class FakeResult:
    def __init__(self, rows: Sequence[Mapping[str, Any]] | None = None, scalar_value: Any = None):
        self._rows = list(rows or [])
        self._scalar_value = scalar_value

    class _Mappings:
        def __init__(self, rows):
            self._rows = list(rows)

        def all(self):
            return list(self._rows)

        def first(self):
            return self._rows[0] if self._rows else None

    class _Scalars:
        def __init__(self, values):
            self._values = values

        def all(self):
            return list(self._values or [])

    def mappings(self):
        return FakeResult._Mappings(self._rows)

    def scalars(self):
        return FakeResult._Scalars(self._scalar_value)

    def scalar_one(self):
        if self._scalar_value is not None:
            return self._scalar_value
        if not self._rows:
            return 0
        return next(iter(self._rows[0].values()))


class FakeConn:
    def __init__(self, rows_sequence=None, scalar_sequence=None):
        self.rows_sequence = list(rows_sequence or [])
        self.scalar_sequence = list(scalar_sequence or [])
        self.statements: list[Any] = []

    async def execute(self, stmt):
        self.statements.append(stmt)
        rows = self.rows_sequence.pop(0) if isinstance(stmt, Select) and self.rows_sequence else []
        scalar = self.scalar_sequence.pop(0) if self.scalar_sequence else None
        return FakeResult(rows, scalar)

    async def run_sync(self, fn, *args, **kwargs):
        fn(self, *args, **kwargs)


class FakeBeginCtx:
    def __init__(self, conn: FakeConn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeEngine:
    def __init__(self, rows_sequence=None, scalar_sequence=None):
        self.conn = FakeConn(rows_sequence, scalar_sequence)

    def begin(self):
        return FakeBeginCtx(self.conn)


def build_table():
    return Table(
        "items",
        MetaData(),
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("category", String, nullable=True),
    )


@pytest.mark.asyncio
async def test_select_by_fields_core_behaviour():
    engine = FakeEngine([[{"id": 1, "name": "a", "category": None}]])
    result = await _select_by_fields(
        engine,
        ItemMapper(build_table()),
        {"name": "a", "category": None},
        offset=2,
        limit=5,
    )
    assert result[0].id == 1
    stmt = engine.conn.statements[0]
    assert isinstance(stmt, Select)
    assert stmt._limit == 5
    assert stmt._offset == 2


@pytest.mark.asyncio
async def test_select_by_fields_validation():
    with pytest.raises(ValueError):
        await _select_by_fields(FakeEngine(), ItemMapper(build_table()), {"missing": 1})
    with pytest.raises(ValueError):
        await _select_by_fields(FakeEngine(), ItemMapper(build_table()), {"name": {"bad": "x"}})
    with pytest.raises(ValueError):
        await _select_by_fields(FakeEngine(), ItemMapper(build_table()), {"name": "a"}, sort_by="bad")


@pytest.mark.asyncio
async def test_select_by_fields_supports_text_ops_and_logical_groups():
    engine = FakeEngine([[{"id": 1, "name": "Calculo", "category": "matematicas"}]])
    result = await _select_by_fields(
        engine,
        ItemMapper(build_table()),
        {
            "or": [
                {"name": {"icontains": "calc"}},
                {"category": {"startswith": "mate"}},
            ]
        },
        offset=0,
        limit=10,
    )

    assert result[0].name == "Calculo"
    stmt = engine.conn.statements[0]
    assert isinstance(stmt, Select)
    assert stmt._where_criteria


@pytest.mark.asyncio
async def test_init_indexes_calls_helpers(monkeypatch):
    import persistence_kit.repository.sqlalchemy_repo.sqlalchemy_repo as mod

    table = build_table()
    mapper = ItemMapper(table)
    engine = FakeEngine()
    created_indexes = []
    fk_calls = []

    monkeypatch.setattr(mod, "ensure_missing_columns", lambda *args: None)
    monkeypatch.setattr(mod, "ensure_foreign_keys", lambda *args: fk_calls.append(args))
    monkeypatch.setattr(mod, "build_fk_map_from_registry", lambda key: {"category_id": ("categories", "id")})
    monkeypatch.setattr(table.metadata, "create_all", lambda bind, tables=None, checkfirst=True: None)

    class FakeIndex:
        def __init__(self, name, column, unique=False):
            self.name = name
            self.unique = unique

        def create(self, bind=None, checkfirst=True):
            created_indexes.append((self.name, self.unique))

    monkeypatch.setattr(mod, "Index", FakeIndex)

    repo = SqlAlchemyRepository[Item, int](engine, mapper, "item")
    await repo.init_indexes()
    assert fk_calls
    assert created_indexes == [("uniq_by_name_items", True)]


@pytest.mark.asyncio
async def test_crud_methods_and_get_by_index():
    engine = FakeEngine(
        rows_sequence=[
            [{"id": 1, "name": "a", "category": "c1"}],
            [{"id": 1, "name": "a", "category": "c1"}, {"id": 2, "name": "b", "category": None}],
            [{"id": 2, "name": "x", "category": None}],
        ]
    )
    repo = SqlAlchemyRepository[Item, int](engine, ItemMapper(build_table()), "item")
    repo._inited = True

    await repo.add(Item(id=1, name="a", category="c1"))
    assert isinstance(engine.conn.statements[0], Insert)
    assert (await repo.get(1)).id == 1
    assert [item.id for item in await repo.list(offset=0, limit=10)] == [1, 2]
    await repo.update(Item(id=1, name="a2", category="c2"))
    assert isinstance(engine.conn.statements[3], Update)
    await repo.delete(1)
    assert isinstance(engine.conn.statements[4], Delete)
    assert (await repo.get_by_index("by_name", "x")).id == 2
    assert await repo.get_by_index("unknown", "x") is None


@pytest.mark.asyncio
async def test_list_by_fields_count_and_distinct(monkeypatch):
    import persistence_kit.repository.sqlalchemy_repo.sqlalchemy_repo as mod

    repo = SqlAlchemyRepository[Item, int](FakeEngine(scalar_sequence=[7, 3, ["a", "b", None], [1, 3]]), ItemMapper(build_table()), "item")
    repo._inited = True
    called = {}

    async def fake_select_by_fields(engine_arg, mapper_arg, criteria_arg, **kwargs):
        called["criteria"] = dict(criteria_arg)
        return [Item(id=99, name="z", category=None)]

    monkeypatch.setattr(mod, "_select_by_fields", fake_select_by_fields)
    result = await repo.list_by_fields({"name": "z"}, offset=3, limit=7)
    assert result[0].id == 99
    assert called["criteria"] == {"name": "z"}
    assert await repo.count() == 7
    assert await repo.count_by_fields({"name": "a"}) == 3
    assert await repo.distinct_values("name") == ["a", "b"]
    assert await repo.distinct_values("id", {"name": "a"}) == [1, 3]


@pytest.mark.asyncio
async def test_ensure_indexes_only_once_and_update_no_fields(monkeypatch):
    repo = SqlAlchemyRepository[Item, int](FakeEngine(), ItemMapper(build_table()), "item")
    calls = []

    async def fake_init_indexes(self):
        calls.append(1)
        self._inited = True

    monkeypatch.setattr(type(repo), "init_indexes", fake_init_indexes)
    await repo._ensure_indexes()
    await repo._ensure_indexes()
    assert calls == [1]

    repo = SqlAlchemyRepository[Item, int](FakeEngine(), IdOnlyMapper(build_table()), "item")
    repo._inited = True
    await repo.update(Item(id=1, name="a", category=None))
    assert repo._engine.conn.statements == []
