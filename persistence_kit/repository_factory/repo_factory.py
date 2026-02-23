from functools import lru_cache
from typing import Callable, Any, Type, TypeVar, cast
from uuid import UUID
from operator import attrgetter
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from persistence_kit.config import RepoSettings, Database
from persistence_kit.abstract_repository import Repository
from persistence_kit.repository.memory_repo.memory_repo import MemoryRepository
from persistence_kit.repository.mongo_repo.mongo_repo import MongoRepository
from persistence_kit.repository.mongo_repo.mongo_mapper import DataclassMapper
from .entity_registry import get_entity_config
from .populating_repo import PopulatingRepository
from ..repository.sqlalchemy_repo.sqlalchemy_dataclass_mapper import SqlDataclassMapper
from ..repository.sqlalchemy_repo.sqlalchemy_engine import get_engine
from ..repository.sqlalchemy_repo.sqlalchemy_repo import SqlAlchemyRepository
from ..repository.sqlalchemy_repo.table_factory import build_table_from_dataclass
from persistence_kit.abstract_view_repo import ViewRepository

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
    for k, v in (unique or {}).items():
        out[k] = v if callable(v) else attrgetter(v)
    return out

@lru_cache
def _mongo_db(dsn: str, dbname: str) -> AsyncIOMotorDatabase:
    return AsyncIOMotorClient(dsn, uuidRepresentation="standard")[dbname]

def _normalize_database(value: Any, default_database: Database) -> Database:
    if isinstance(value, Database): return value
    if isinstance(value, str):
        v = value.strip().lower()
        return Database(v) if v in {b.value for b in Database} else default_database
    return default_database

@lru_cache
def _repo_cached(entity_key: str, resolved: Database) -> Repository[Any, UUID]:
    _init_registry()
    s = RepoSettings()
    conf = get_entity_config(entity_key)
    et = cast(type, conf["entity"])

    if resolved is Database.MEMORY:
        return MemoryRepository[Any, UUID](
            id_getter=lambda e: e.id,
            unique_indexes=_to_memory_unique(conf.get("unique"))
        )

    if resolved is Database.MONGO:
        db = _mongo_db(s.mongo_dsn, s.mongo_db)
        mapper = DataclassMapper(
            et,
            conf["collection"],
            unique_fields={k: (v if isinstance(v, str) else k) for k, v in (conf.get("unique") or {}).items()}
        )
        return MongoRepository[Any, UUID](db, mapper)

    if resolved is Database.POSTGRES:
        engine = get_engine()
        table_name = conf["collection"]
        table = build_table_from_dataclass(et, table_name)
        mapper = SqlDataclassMapper(
            et,
            table,
            id_column="id",
            unique_cols={k: (v if isinstance(v, str) else k) for k, v in (conf.get("unique") or {}).items()},
        )
        return SqlAlchemyRepository[Any, UUID](engine, mapper, entity_key)

    raise ValueError(f"Database {resolved} no soportado: {entity_key}")

def get_repo(entity_key: str) -> Repository[Any, UUID]:
    _init_registry()
    s = RepoSettings()
    conf = get_entity_config(entity_key)
    be = _normalize_database(conf.get("database"), s.repo_database)
    return _repo_cached(entity_key, be)

def get_repo_view(entity_key: str) -> ViewRepository[Any, UUID]:
    base = get_repo(entity_key)
    return PopulatingRepository(entity_key, base, resolve_repo=lambda k: get_repo(k))

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
        return PopulatingRepository(entity_key, base, resolve_repo=lambda k: get_repo(k))
    return _dep
