import re
from functools import lru_cache
from importlib.util import find_spec

from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from persistence_kit.settings.constants import Database
from persistence_kit.settings.repo_settings import RepoSettings


def _translate(value, source, target):
    if value is None:
        return None
    return value.translate(str.maketrans(source, target))


def _regexp_replace(value, pattern, replacement, flags):
    if value is None:
        return None
    return re.sub(pattern, replacement, value, count=0 if "g" in (flags or "") else 1)


def _register_postgres_functions(dbapi_connection, _connection_record) -> None:
    dbapi_connection.create_function("translate", 3, _translate, deterministic=True)
    dbapi_connection.create_function("regexp_replace", 4, _regexp_replace, deterministic=True)


def _sqlite_engine(settings: RepoSettings) -> AsyncEngine:
    if not settings.sqlite_path:
        raise ValueError("REPO_DATABASE=sqlite needs SQLITE_PATH, the file that holds the database")
    if find_spec("aiosqlite") is None:
        raise ModuleNotFoundError(
            "The sqlite backend needs aiosqlite. Install it with: pip install 'persistence-kit[sqlite]'"
        )
    engine = create_async_engine(f"sqlite+aiosqlite:///{settings.sqlite_path}", future=True)
    event.listen(engine.sync_engine, "connect", _register_postgres_functions)
    return engine


@lru_cache
def get_engine(database: Database = Database.POSTGRES) -> AsyncEngine:
    s = RepoSettings()
    if database is Database.SQLITE:
        return _sqlite_engine(s)
    url = s.postgres_dsn
    if not url:
        url = f"postgresql+asyncpg://{s.postgres_user}:{s.postgres_password}@{s.postgres_host}:{s.postgres_port}/{s.postgres_db}"
    kwargs: dict = {"future": True}
    if getattr(s, "postgres_ssl", False):
        kwargs["connect_args"] = {"ssl": "require"}
    return create_async_engine(url, **kwargs)
