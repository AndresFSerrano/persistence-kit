from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any

from motor.motor_asyncio import AsyncIOMotorCollection


class MongoCache:
    """Cache respaldado por una coleccion Mongo con indice TTL nativo sobre
    ``expiresAt`` (el servidor borra los documentos expirados solo).

    El monitor TTL de Mongo corre cada ~60s, por eso ``get`` valida la expiracion
    ademas del indice, para no devolver un documento recien vencido.
    """

    def __init__(self, collection: AsyncIOMotorCollection) -> None:
        self._col = collection
        self._index_ready = False

    async def _ensure_index(self) -> None:
        if not self._index_ready:
            await self._col.create_index("expiresAt", expireAfterSeconds=0)
            self._index_ready = True

    async def get(self, key: str) -> Any | None:
        doc = await self._col.find_one({"_id": key})
        if doc is None:
            return None
        expires_at = doc.get("expiresAt")
        if expires_at is not None:
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
            if expires_at <= datetime.now(timezone.utc):
                return None
        return doc.get("value")

    async def set(self, key: str, value: Any, ttl_seconds: float | None = None) -> None:
        await self._ensure_index()
        doc: dict[str, Any] = {"_id": key, "value": value}
        if ttl_seconds:
            doc["expiresAt"] = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
        await self._col.replace_one({"_id": key}, doc, upsert=True)

    async def delete(self, key: str) -> None:
        await self._col.delete_one({"_id": key})

    async def clear(self, prefix: str = "") -> int:
        if prefix:
            query = {"_id": {"$regex": f"^{re.escape(prefix)}"}}
        else:
            query = {}
        result = await self._col.delete_many(query)
        return result.deleted_count
