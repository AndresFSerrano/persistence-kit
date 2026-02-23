from functools import lru_cache
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from persistence_kit.config import RepoSettings

@lru_cache
def get_engine() -> AsyncEngine:
    s = RepoSettings()
    url = getattr(s, "postgres_dsn", None)
    if not url:
        url = f"postgresql+asyncpg://{s.postgres_user}:{s.postgres_password}@{s.postgres_host}:{s.postgres_port}/{s.postgres_db}"
    return create_async_engine(url, future=True)
