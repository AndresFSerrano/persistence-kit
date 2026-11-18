from __future__ import annotations

import asyncio
from collections.abc import Callable, Hashable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING

from persistence_kit.repository.errors import TransactionNotSupportedError, TransactionRequiredError

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine


class RowLocks:
    def __init__(self) -> None:
        self._held: dict[Hashable, tuple[Transaction, asyncio.Event]] = {}

    async def acquire(self, key: Hashable, owner: Transaction) -> bool:
        while True:
            held = self._held.get(key)
            if held is None:
                self._held[key] = (owner, asyncio.Event())
                return True
            if held[0] is owner:
                return False
            await held[1].wait()

    def release(self, key: Hashable, owner: Transaction) -> None:
        held = self._held.get(key)
        if held is not None and held[0] is owner:
            del self._held[key]
            held[1].set()


class Transaction:
    def __init__(self) -> None:
        self._connections: dict[AsyncEngine, AsyncConnection] = {}
        self._undo: list[Callable[[], None]] = []
        self._locks: list[tuple[RowLocks, Hashable]] = []

    async def connection(self, engine: AsyncEngine) -> AsyncConnection:
        conn = self._connections.get(engine)
        if conn is None:
            conn = await engine.connect()
            self._connections[engine] = conn
            await conn.begin()
        return conn

    async def lock_row(
        self,
        locks: RowLocks,
        key: Hashable,
        snapshot: Callable[[], Callable[[], None]],
    ) -> None:
        if await locks.acquire(key, self):
            self._locks.append((locks, key))
            self._undo.append(snapshot())

    async def commit(self) -> None:
        for conn in self._connections.values():
            await conn.commit()

    async def rollback(self) -> None:
        for restore in reversed(self._undo):
            restore()
        for conn in self._connections.values():
            await conn.rollback()

    async def close(self) -> None:
        for locks, key in self._locks:
            locks.release(key, self)
        for conn in self._connections.values():
            await conn.close()


_current: ContextVar[Transaction | None] = ContextVar("persistence_kit_transaction", default=None)


def current_transaction() -> Transaction | None:
    return _current.get()


@contextmanager
def bound(tx: Transaction) -> Iterator[None]:
    token = _current.set(tx)
    try:
        yield
    finally:
        _current.reset(token)


def require_transaction() -> Transaction:
    tx = _current.get()
    if tx is None:
        raise TransactionRequiredError(
            "for_update=True needs an open transaction: call it inside 'async with transaction():'."
        )
    return tx


def reject_transaction(backend: str) -> None:
    if _current.get() is not None:
        raise TransactionNotSupportedError(
            f"{backend} repositories cannot write inside a transaction. "
            "Keep the entities written in 'async with transaction():' on Postgres or memory."
        )
