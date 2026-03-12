from dataclasses import dataclass
from typing import Any, Callable, Dict
from uuid import UUID, uuid4

import pytest

from persistence_kit import Database
from persistence_kit.repository_factory.factory.repository_factory import (
    _ensure_ready,
    _init_registry,
    _mongo_db,
    _normalize_database,
    _repo_cached,
    _to_memory_unique,
    get_repo,
    get_repo_view,
    provide_repo,
    provide_view_repo,
)


@pytest.fixture(autouse=True)
def clear_caches():
    _mongo_db.cache_clear()
    _repo_cached.cache_clear()
    _init_registry.cache_clear()


@dataclass
class DummyEntity:
    id: UUID
    name: str


def test_to_memory_unique_none():
    assert _to_memory_unique(None) == {}


def test_to_memory_unique_attr_and_callable():
    def get_name(entity: DummyEntity) -> str:
        return entity.name

    mapper = _to_memory_unique({"by_id": "id", "by_name": get_name})
    entity = DummyEntity(id=uuid4(), name="test")
    assert mapper["by_id"](entity) == entity.id
    assert mapper["by_name"](entity) == "test"


def test_normalize_database_passthrough_enum():
    assert _normalize_database(Database.MONGO, Database.MEMORY) is Database.MONGO


def test_normalize_database_string_known():
    assert _normalize_database(Database.POSTGRES.value, Database.MEMORY) is Database.POSTGRES


def test_normalize_database_string_unknown_returns_default():
    default = Database.MEMORY
    assert _normalize_database("unknown-db", default) is default


def test_mongo_db_is_cached(monkeypatch):
    created: list[Dict[str, Any]] = []

    class FakeDb:
        def __init__(self, name: str):
            self.name = name

    class FakeClient:
        def __init__(self, dsn: str, uuidRepresentation: str):
            created.append({"dsn": dsn, "uuidRepresentation": uuidRepresentation})
            self._dbs: Dict[str, FakeDb] = {}

        def __getitem__(self, item: str) -> FakeDb:
            if item not in self._dbs:
                self._dbs[item] = FakeDb(item)
            return self._dbs[item]

    import persistence_kit.repository_factory.factory.repository_factory as rf

    monkeypatch.setattr(rf, "AsyncIOMotorClient", FakeClient)

    db1 = _mongo_db("dsn1", "db1")
    db2 = _mongo_db("dsn1", "db1")
    db3 = _mongo_db("dsn1", "db2")

    assert db1 is db2
    assert db1 is not db3
    assert len(created) in {1, 2}


def test_repo_cached_memory(monkeypatch):
    import persistence_kit.repository_factory.factory.repository_factory as rf

    class FakeSettings:
        repo_database = Database.MEMORY
        mongo_dsn = "dsn"
        mongo_db = "db"

    captured: Dict[str, Any] = {}

    class FakeMemoryRepo:
        def __class_getitem__(cls, item):
            return cls

        def __init__(self, id_getter: Callable[[Any], Any], unique_indexes: Dict[str, Callable[[Any], Any]]):
            captured["id_getter"] = id_getter
            captured["unique_indexes"] = unique_indexes

    monkeypatch.setattr(rf, "RepoSettings", FakeSettings)
    monkeypatch.setattr(rf, "MemoryRepository", FakeMemoryRepo)
    monkeypatch.setattr(rf, "get_entity_config", lambda _: {"entity": DummyEntity, "unique": {"by_id": "id"}})

    repo = _repo_cached("dummy", Database.MEMORY)
    entity = DummyEntity(id=uuid4(), name="x")
    assert isinstance(repo, FakeMemoryRepo)
    assert captured["id_getter"](entity) == entity.id


def test_repo_cached_mongo(monkeypatch):
    import persistence_kit.repository_factory.factory.repository_factory as rf

    class FakeSettings:
        repo_database = Database.MEMORY
        mongo_dsn = "mongodb://localhost:27017"
        mongo_db = "testdb"

    class FakeDb:
        pass

    class FakeMapper:
        def __init__(self, entity: Any, collection: str, unique_fields: Dict[str, str]):
            self.entity = entity
            self.collection = collection
            self.unique_fields = unique_fields

    captured: Dict[str, Any] = {}

    class FakeMongoRepo:
        def __class_getitem__(cls, item):
            return cls

        def __init__(self, db: Any, mapper: Any):
            captured["db"] = db
            captured["mapper"] = mapper

    monkeypatch.setattr(rf, "RepoSettings", FakeSettings)
    monkeypatch.setattr(rf, "_mongo_db", lambda dsn, dbname: FakeDb())
    monkeypatch.setattr(rf, "DataclassMapper", FakeMapper)
    monkeypatch.setattr(rf, "MongoRepository", FakeMongoRepo)
    monkeypatch.setattr(
        rf,
        "get_entity_config",
        lambda _: {"entity": DummyEntity, "collection": "dummy_collection", "unique": {"by_id": "id"}},
    )

    repo = _repo_cached("dummy", Database.MONGO)
    assert isinstance(repo, FakeMongoRepo)
    assert captured["mapper"].collection == "dummy_collection"


def test_repo_cached_postgres(monkeypatch):
    import persistence_kit.repository_factory.factory.repository_factory as rf

    class FakeSettings:
        repo_database = Database.POSTGRES
        mongo_dsn = "dsn"
        mongo_db = "db"

    class FakeEngine:
        pass

    class FakeTable:
        def __init__(self, name: str):
            self.name = name

    class FakeSqlMapper:
        def __init__(self, entity: Any, table: Any, id_column: str, unique_cols: Dict[str, str]):
            self.entity = entity
            self.table = table
            self.id_column = id_column
            self.unique_cols = unique_cols

    captured: Dict[str, Any] = {}

    class FakeSqlRepo:
        def __class_getitem__(cls, item):
            return cls

        def __init__(self, engine: Any, mapper: Any, entity_key: str):
            captured["engine"] = engine
            captured["mapper"] = mapper
            captured["entity_key"] = entity_key

    monkeypatch.setattr(rf, "RepoSettings", FakeSettings)
    monkeypatch.setattr(rf, "get_engine", lambda: FakeEngine())
    monkeypatch.setattr(rf, "build_table_from_dataclass", lambda entity, table_name: FakeTable(table_name))
    monkeypatch.setattr(rf, "SqlDataclassMapper", FakeSqlMapper)
    monkeypatch.setattr(rf, "SqlAlchemyRepository", FakeSqlRepo)
    monkeypatch.setattr(
        rf,
        "get_entity_config",
        lambda _: {"entity": DummyEntity, "collection": "dummy_table", "unique": {"by_id": "id"}},
    )

    repo = _repo_cached("dummy", Database.POSTGRES)
    assert isinstance(repo, FakeSqlRepo)
    assert captured["mapper"].table.name == "dummy_table"
    assert captured["entity_key"] == "dummy"


def test_get_repo_uses_entity_database_override(monkeypatch):
    import persistence_kit.repository_factory.factory.repository_factory as rf

    class FakeSettings:
        repo_database = Database.MEMORY
        mongo_dsn = "dsn"
        mongo_db = "db"

    captured: Dict[str, Any] = {}
    sentinel_repo = object()

    monkeypatch.setattr(rf, "RepoSettings", FakeSettings)
    monkeypatch.setattr(
        rf,
        "get_entity_config",
        lambda _: {"entity": DummyEntity, "collection": "dummy", "database": Database.MONGO.value},
    )
    monkeypatch.setattr(
        rf,
        "_repo_cached",
        lambda entity_key, resolved: captured.update({"entity_key": entity_key, "resolved": resolved}) or sentinel_repo,
    )

    repo = get_repo("dummy")
    assert repo is sentinel_repo
    assert captured["resolved"] is Database.MONGO


def test_get_repo_view_wraps_with_populating_repo(monkeypatch):
    import persistence_kit.repository_factory.factory.repository_factory as rf

    class FakeBaseRepo:
        pass

    captured: Dict[str, Any] = {}

    class FakePopulatingRepo:
        def __init__(self, entity_key: str, base: Any, resolve_repo: Callable[[str], Any]):
            captured["entity_key"] = entity_key
            captured["base"] = base
            captured["resolve_repo"] = resolve_repo

    monkeypatch.setattr(rf, "PopulatingRepository", FakePopulatingRepo)
    monkeypatch.setattr(rf, "get_repo", lambda _: FakeBaseRepo())

    repo_view = get_repo_view("dummy")
    assert isinstance(repo_view, FakePopulatingRepo)
    assert captured["entity_key"] == "dummy"


@pytest.mark.asyncio
async def test_ensure_ready_calls_init_indexes_for_mongo(monkeypatch):
    import persistence_kit.repository_factory.factory.repository_factory as rf

    class FakeMongoRepo:
        def __init__(self):
            self.called = False

        async def init_indexes(self):
            self.called = True

    monkeypatch.setattr(rf, "MongoRepository", FakeMongoRepo)
    monkeypatch.setattr(rf, "SqlAlchemyRepository", type("X", (), {}))

    repo = FakeMongoRepo()
    result = await _ensure_ready(repo)
    assert result is repo
    assert repo.called is True


@pytest.mark.asyncio
async def test_ensure_ready_does_not_call_for_other_repo(monkeypatch):
    import persistence_kit.repository_factory.factory.repository_factory as rf

    monkeypatch.setattr(rf, "MongoRepository", type("FakeMongoRepo", (), {}))
    monkeypatch.setattr(rf, "SqlAlchemyRepository", type("X", (), {}))

    class OtherRepo:
        pass

    repo = OtherRepo()
    assert await _ensure_ready(repo) is repo


@pytest.mark.asyncio
async def test_provide_repo_returns_repo_after_ensure_ready(monkeypatch):
    import persistence_kit.repository_factory.factory.repository_factory as rf

    base_repo = object()
    ensured_repo = object()
    captured: Dict[str, Any] = {}

    monkeypatch.setattr(rf, "get_repo", lambda key: captured.update({"entity_key": key}) or base_repo)

    async def fake_ensure_ready(repo: Any) -> Any:
        captured["ensured_arg"] = repo
        return ensured_repo

    monkeypatch.setattr(rf, "_ensure_ready", fake_ensure_ready)

    dep = provide_repo("dummy")
    repo = await dep()
    assert repo is ensured_repo
    assert captured["ensured_arg"] is base_repo


@pytest.mark.asyncio
async def test_provide_view_repo_wraps_with_populating_repo(monkeypatch):
    import persistence_kit.repository_factory.factory.repository_factory as rf

    ensured_repo = object()
    captured: Dict[str, Any] = {}

    async def fake_dep() -> Any:
        return ensured_repo

    monkeypatch.setattr(rf, "provide_repo", lambda key: captured.update({"entity_key": key}) or fake_dep)

    class FakePopulatingRepo:
        def __init__(self, entity_key: str, base: Any, resolve_repo: Callable[[str], Any]):
            captured["pop_entity_key"] = entity_key
            captured["base"] = base

    monkeypatch.setattr(rf, "PopulatingRepository", FakePopulatingRepo)

    dep = provide_view_repo("dummy")
    repo_view = await dep()
    assert isinstance(repo_view, FakePopulatingRepo)
    assert captured["base"] is ensured_repo
