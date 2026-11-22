from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any

from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

INCREMENT_ATTEMPTS = 3


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

    async def set_if_absent(self, key: str, value: Any, ttl_seconds: float | None = None) -> bool:
        await self._ensure_index()
        now = datetime.now(timezone.utc)
        await self._col.delete_one({"_id": key, "expiresAt": {"$lte": now}})
        doc: dict[str, Any] = {"value": value}
        if ttl_seconds:
            doc["expiresAt"] = now + timedelta(seconds=ttl_seconds)
        try:
            result = await self._col.update_one({"_id": key}, {"$setOnInsert": doc}, upsert=True)
        except DuplicateKeyError:
            return False
        return result.upserted_id is not None

    async def increment(self, key: str, ttl_seconds: float | None = None) -> int:
        """Suma en un solo ``find_one_and_update`` con pipeline (Mongo 4.2+).

        El pipeline decide en el servidor si el documento sigue vigente: si no
        existe o su ``expiresAt`` ya paso (el monitor TTL aun no lo borra),
        arranca en 1 con vencimiento nuevo; si esta vigente, suma y conserva el
        suyo. Dos upserts simultaneos de una clave nueva pueden chocar con
        ``DuplicateKeyError``; el perdedor reintenta y suma sobre el ganador.
        """
        await self._ensure_index()
        for _ in range(INCREMENT_ATTEMPTS - 1):
            try:
                return await self._increment_once(key, ttl_seconds)
            except DuplicateKeyError:
                continue
        return await self._increment_once(key, ttl_seconds)

    async def _increment_once(self, key: str, ttl_seconds: float | None) -> int:
        doc = await self._col.find_one_and_update(
            {"_id": key},
            _increment_pipeline(datetime.now(timezone.utc), ttl_seconds),
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        return doc["value"]

    async def delete(self, key: str) -> None:
        await self._col.delete_one({"_id": key})

    async def clear(self, prefix: str = "") -> int:
        if prefix:
            query = {"_id": {"$regex": f"^{re.escape(prefix)}"}}
        else:
            query = {}
        result = await self._col.delete_many(query)
        return result.deleted_count


def _increment_pipeline(now: datetime, ttl_seconds: float | None) -> list[dict[str, Any]]:
    starts_over = {
        "$or": [
            {"$eq": [{"$type": "$value"}, "missing"]},
            {
                "$and": [
                    {"$ne": [{"$ifNull": ["$expiresAt", None]}, None]},
                    {"$lte": ["$expiresAt", now]},
                ]
            },
        ]
    }
    fresh_expiry: Any = now + timedelta(seconds=ttl_seconds) if ttl_seconds else "$$REMOVE"
    return [
        {
            "$set": {
                "value": {"$cond": [starts_over, 1, {"$add": ["$value", 1]}]},
                "expiresAt": {"$cond": [starts_over, fresh_expiry, "$expiresAt"]},
            }
        }
    ]
