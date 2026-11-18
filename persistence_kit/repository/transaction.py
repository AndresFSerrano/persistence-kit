from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from persistence_kit.repository.idempotency import claim_key
from persistence_kit.repository.transaction_context import Transaction, bound, current_transaction


@asynccontextmanager
async def transaction(*, idempotency_key: str | None = None) -> AsyncIterator[None]:
    outer = current_transaction()
    if outer is not None:
        if idempotency_key is not None:
            await claim_key(outer, idempotency_key)
        yield
        return
    tx = Transaction()
    try:
        with bound(tx):
            if idempotency_key is not None:
                await claim_key(tx, idempotency_key)
            yield
        await tx.commit()
    except BaseException:
        await tx.rollback()
        raise
    finally:
        await tx.close()
