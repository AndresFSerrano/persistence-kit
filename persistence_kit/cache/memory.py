from __future__ import annotations

import threading
import time
from typing import Any


class InMemoryTTLCache:
    """Cache en memoria del proceso con expiracion perezosa por TTL.

    Es por-proceso: cada instancia/tarea tiene la suya y se pierde al reiniciar.
    ``increment`` va bajo un ``threading.Lock`` porque la instancia la comparte
    todo el proceso (``get_cache`` la guarda), sin importar el event loop.
    """

    def __init__(self) -> None:
        self._store: dict[str, tuple[Any, float | None]] = {}
        self._lock = threading.Lock()

    async def get(self, key: str) -> Any | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        value, expires_at = entry
        if expires_at is not None and time.monotonic() >= expires_at:
            self._store.pop(key, None)
            return None
        return value

    async def set(self, key: str, value: Any, ttl_seconds: float | None = None) -> None:
        expires_at = time.monotonic() + ttl_seconds if ttl_seconds else None
        self._store[key] = (value, expires_at)

    async def set_if_absent(self, key: str, value: Any, ttl_seconds: float | None = None) -> bool:
        if await self.get(key) is not None:
            return False
        await self.set(key, value, ttl_seconds)
        return True

    async def increment(self, key: str, ttl_seconds: float | None = None) -> int:
        with self._lock:
            now = time.monotonic()
            entry = self._store.get(key)
            if entry is None or (entry[1] is not None and now >= entry[1]):
                self._store[key] = (1, now + ttl_seconds if ttl_seconds else None)
                return 1
            value, expires_at = entry
            self._store[key] = (value + 1, expires_at)
            return value + 1

    async def delete(self, key: str) -> None:
        self._store.pop(key, None)

    async def clear(self, prefix: str = "") -> int:
        keys = [key for key in self._store if key.startswith(prefix)]
        for key in keys:
            self._store.pop(key, None)
        return len(keys)
