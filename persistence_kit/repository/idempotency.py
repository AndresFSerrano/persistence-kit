from __future__ import annotations

import zlib
from collections.abc import Callable
from datetime import datetime, timezone
from functools import partial

from sqlalchemy import Column, DateTime, MetaData, String, Table, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine

from persistence_kit.repository.errors import RepeatedActionError, TransactionNotSupportedError
from persistence_kit.repository.sqlalchemy_repo.sqlalchemy_engine import get_engine
from persistence_kit.repository.transaction_context import RowLocks, Transaction
from persistence_kit.settings.constants import Database
from persistence_kit.settings.repo_settings import RepoSettings

KEYS_TABLE = Table(
    "idempotency_keys",
    MetaData(),
    Column("key", String, primary_key=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
)
CREATE_TABLE_LOCK = zlib.crc32(b"persistence_kit.idempotency_keys")


class MemoryKeys:
    def __init__(self) -> None:
        self._done: set[str] = set()
        self._locks = RowLocks()

    async def claim(self, tx: Transaction, key: str) -> None:
        await tx.lock_row(self._locks, key, partial(self._snapshot, key))
        if key in self._done:
            raise RepeatedActionError(key)
        self._done.add(key)

    def _snapshot(self, key: str) -> Callable[[], None]:
        return partial(self._restore, key, key in self._done)

    def _restore(self, key: str, was_done: bool) -> None:
        if not was_done:
            self._done.discard(key)


class SqlKeys:
    def __init__(self) -> None:
        self._ready: set[AsyncEngine] = set()

    async def claim(self, tx: Transaction, key: str) -> None:
        engine = get_engine()
        await self._create_table(engine)
        conn = await tx.connection(engine)
        stmt = (
            insert(KEYS_TABLE)
            .values(key=key, created_at=datetime.now(timezone.utc))
            .on_conflict_do_nothing(index_elements=[KEYS_TABLE.c.key])
        )
        result = await conn.execute(stmt)
        if result.rowcount == 0:
            raise RepeatedActionError(key)

    async def _create_table(self, engine: AsyncEngine) -> None:
        if engine in self._ready:
            return
        async with engine.begin() as conn:
            await conn.execute(select(func.pg_advisory_xact_lock(CREATE_TABLE_LOCK)))
            await conn.run_sync(KEYS_TABLE.metadata.create_all, tables=[KEYS_TABLE], checkfirst=True)
        self._ready.add(engine)


_memory_keys = MemoryKeys()
_sql_keys = SqlKeys()


async def claim_key(tx: Transaction, key: str) -> None:
    database = RepoSettings().repo_database
    if database is Database.MEMORY:
        await _memory_keys.claim(tx, key)
        return
    if database is Database.POSTGRES:
        await _sql_keys.claim(tx, key)
        return
    raise TransactionNotSupportedError(
        f"Idempotency keys live in the default database, and {database.value} cannot keep them. "
        "Set REPO_DATABASE to postgres or memory to use 'transaction(idempotency_key=...)'."
    )
