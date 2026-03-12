import logging
from collections.abc import Awaitable, Callable

from sqlalchemy import text

from persistence_kit.repository.sqlalchemy_repo.sqlalchemy_engine import get_engine
from persistence_kit.settings.repo_settings import RepoSettings


def is_duplicate_startup_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "pg_type_typname_nsp_index" in message
        or "duplicate key value violates unique constraint" in message
        or "already exists" in message
    )


async def run_startup_bootstrap(
    settings: RepoSettings,
    logger: logging.Logger,
    run_bootstrap: Callable[[], Awaitable[None]],
) -> None:
    try:
        if settings.repo_database.value == "postgres":
            engine = get_engine()
            async with engine.connect() as conn:
                await conn.execute(text("SELECT pg_advisory_lock(84848484)"))
                try:
                    await run_bootstrap()
                finally:
                    await conn.execute(text("SELECT pg_advisory_unlock(84848484)"))
        else:
            await run_bootstrap()
    except Exception as exc:
        if is_duplicate_startup_error(exc):
            logger.warning("Startup config/seed race detected and ignored: %s", exc)
            return
        raise
