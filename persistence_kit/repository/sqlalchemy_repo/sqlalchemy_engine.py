from functools import lru_cache
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from persistence_kit.settings.repo_settings import RepoSettings

@lru_cache
def get_engine() -> AsyncEngine:
    s = RepoSettings()
    url = s.postgres_dsn
    if not url:
        url = f"postgresql+asyncpg://{s.postgres_user}:{s.postgres_password}@{s.postgres_host}:{s.postgres_port}/{s.postgres_db}"
    kwargs: dict = {"future": True}
    if s.postgres_ssl:
        kwargs["connect_args"] = {"ssl": "require"}
    return create_async_engine(url, **kwargs)
