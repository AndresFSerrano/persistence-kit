from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Cache(Protocol):
    """Almacen clave-valor con expiracion opcional (TTL).

    Los backends (memoria, Mongo, Dynamo) son intercambiables; el valor debe ser
    serializable a JSON para los backends persistentes.
    """

    async def get(self, key: str) -> Any | None: ...

    async def set(self, key: str, value: Any, ttl_seconds: float | None = None) -> None: ...

    async def delete(self, key: str) -> None: ...

    async def clear(self, prefix: str = "") -> int:
        """Borra todas las claves que empiezan con ``prefix`` (o todo si vacio).
        Devuelve cuantas borro."""
        ...
