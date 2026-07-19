from __future__ import annotations

from typing import Any

from persistence_kit.cache.contracts import Cache


class NamespacedCache:
    """Prefija todas las claves con ``namespace:`` para aislar aplicaciones que
    comparten un mismo cache.

    Util cuando varias apps (p. ej. store_manager y siga) usan la misma tabla
    DynamoDB o la misma coleccion Mongo: cada app pone su ``CACHE_NAMESPACE`` y
    sus claves no colisionan.
    """

    def __init__(self, inner: Cache, namespace: str) -> None:
        self._inner = inner
        self._prefix = f"{namespace}:"

    async def get(self, key: str) -> Any | None:
        return await self._inner.get(self._prefix + key)

    async def set(self, key: str, value: Any, ttl_seconds: float | None = None) -> None:
        await self._inner.set(self._prefix + key, value, ttl_seconds)

    async def delete(self, key: str) -> None:
        await self._inner.delete(self._prefix + key)

    async def clear(self, prefix: str = "") -> int:
        return await self._inner.clear(self._prefix + prefix)
