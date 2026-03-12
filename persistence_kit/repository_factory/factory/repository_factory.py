from functools import lru_cache
from operator import attrgetter
from typing import Any, Callable, TypeVar, cast
from uuid import UUID

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from persistence_kit.contracts.repository import Repository
from persistence_kit.contracts.view_repository import ViewRepository
from persistence_kit.settings.constants import Database
from persistence_kit.settings.repo_settings import RepoSettings
from persistence_kit.repository.memory_repo.memory_repo import MemoryRepository
from persistence_kit.repository.mongo_repo.mongo_mapper import DataclassMapper
from persistence_kit.repository.mongo_repo.mongo_repo import MongoRepository
from persistence_kit.repository.sqlalchemy_repo.sqlalchemy_dataclass_mapper import SqlDataclassMapper
from persistence_kit.repository.sqlalchemy_repo.sqlalchemy_engine import get_engine
from persistence_kit.repository.sqlalchemy_repo.sqlalchemy_repo import SqlAlchemyRepository
from persistence_kit.repository.sqlalchemy_repo.table_factory import build_table_from_dataclass
from persistence_kit.repository_factory.registry.entity_registry import get_entity_config
from persistence_kit.repository_factory.view.populating_repository import PopulatingRepository

T = TypeVar("T")

_registry_initializer: Callable[[], None] | None = None


def set_registry_initializer(fn: Callable[[], None]) -> None:
    global _registry_initializer
    _registry_initializer = fn


@lru_cache
def _init_registry() -> None:
    if _registry_initializer is not None:
        _registry_initializer()


def _to_memory_unique(unique: dict | None) -> dict[str, Callable[[T], Any]]:
    out: dict[str, Callable[[T], Any]] = {}
    for key, value in (unique or {}).items():
        out[key] = value if callable(value) else attrgetter(value)
    return out


@lru_cache
def _mongo_db(dsn: str, dbname: str) -> AsyncIOMotorDatabase:
    return AsyncIOMotorClient(dsn, uuidRepresentation="standard")[dbname]


def _normalize_database(value: Any, default_database: Database) -> Database:
    if isinstance(value, Database):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        return Database(normalized) if normalized in {db.value for db in Database} else default_database
    return default_database


@lru_cache
def _repo_cached(entity_key: str, resolved: Database) -> Repository[Any, UUID]:
    _init_registry()
    settings = RepoSettings()
    config = get_entity_config(entity_key)
    entity_type = cast(type, config["entity"])

    if resolved is Database.MEMORY:
        return MemoryRepository[Any, UUID](
            id_getter=lambda e: e.id,
            unique_indexes=_to_memory_unique(config.get("unique")),
        )

    if resolved is Database.MONGO:
        db = _mongo_db(settings.mongo_dsn, settings.mongo_db)
        mapper = DataclassMapper(
            entity_type,
            config["collection"],
            unique_fields={
                key: (value if isinstance(value, str) else key)
                for key, value in (config.get("unique") or {}).items()
            },
        )
        return MongoRepository[Any, UUID](db, mapper)

    if resolved is Database.POSTGRES:
        engine = get_engine()
        table_name = config["collection"]
        table = build_table_from_dataclass(entity_type, table_name)
        mapper = SqlDataclassMapper(
            entity_type,
            table,
            id_column="id",
            unique_cols={
                key: (value if isinstance(value, str) else key)
                for key, value in (config.get("unique") or {}).items()
            },
        )
        return SqlAlchemyRepository[Any, UUID](engine, mapper, entity_key)

    raise ValueError(f"Database {resolved} no soportado: {entity_key}")


def get_repo(entity_key: str) -> Repository[Any, UUID]:
    _init_registry()
    settings = RepoSettings()
    config = get_entity_config(entity_key)
    backend = _normalize_database(config.get("database"), settings.repo_database)
    return _repo_cached(entity_key, backend)


def get_repo_view(entity_key: str) -> ViewRepository[Any, UUID]:
    base = get_repo(entity_key)
    return PopulatingRepository(entity_key, base, resolve_repo=lambda key: get_repo(key))


async def _ensure_ready(repo: Repository[Any, UUID]) -> Repository[Any, UUID]:
    if isinstance(repo, (MongoRepository, SqlAlchemyRepository)):
        await repo.init_indexes()
    return repo


def provide_repo(entity_key: str) -> Callable[[], Repository[Any, UUID]]:
    async def _dep() -> Repository[Any, UUID]:
        repo = get_repo(entity_key)
        return await _ensure_ready(repo)

    return _dep


def provide_view_repo(entity_key: str) -> Callable[[], ViewRepository[Any, UUID]]:
    async def _dep() -> ViewRepository[Any, UUID]:
        base = await provide_repo(entity_key)()
        return PopulatingRepository(entity_key, base, resolve_repo=lambda key: get_repo(key))

    return _dep
