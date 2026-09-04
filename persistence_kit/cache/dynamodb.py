from __future__ import annotations

import asyncio
import time
from typing import Any

from persistence_kit.repository.dynamodb_repo.dynamodb_mapper import (
    _deserialize_value,
    _serialize_value,
)

PARTITION_KEY = "pk"
TTL_ATTRIBUTE = "expiresAt"


class DynamoCache:
    """Cache respaldado por una tabla DynamoDB de item unico (pk=key) con TTL
    nativo sobre el atributo ``expiresAt`` (epoch, segundos). La tabla y su
    TTL los declara la infra (``timeToLiveAttribute="expiresAt"``); este
    cliente solo lee y escribe items, igual que ``MongoCache`` pero sin poder
    crear el TTL en caliente (DynamoDB lo fija a nivel de tabla).

    El monitor TTL de DynamoDB puede tardar hasta 48h en limpiar filas
    vencidas, por eso ``get`` valida la expiracion ademas del TTL nativo.
    """

    def __init__(self, table: Any) -> None:
        self._table = table

    async def get(self, key: str) -> Any | None:
        response = await asyncio.to_thread(self._table.get_item, Key={PARTITION_KEY: key})
        item = response.get("Item")
        if item is None:
            return None
        expires_at = item.get(TTL_ATTRIBUTE)
        if expires_at is not None and float(expires_at) <= time.time():
            return None
        return _deserialize_value(item.get("value"))

    async def set(self, key: str, value: Any, ttl_seconds: float | None = None) -> None:
        item = self._build_item(key, value, ttl_seconds)
        await asyncio.to_thread(self._table.put_item, Item=item)

    async def set_if_absent(self, key: str, value: Any, ttl_seconds: float | None = None) -> bool:
        from botocore.exceptions import ClientError

        item = self._build_item(key, value, ttl_seconds)
        now = _serialize_value(time.time())

        def _put() -> bool:
            try:
                self._table.put_item(
                    Item=item,
                    ConditionExpression="attribute_not_exists(#pk) OR #exp <= :now",
                    ExpressionAttributeNames={"#pk": PARTITION_KEY, "#exp": TTL_ATTRIBUTE},
                    ExpressionAttributeValues={":now": now},
                )
                return True
            except ClientError as exc:
                if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                    return False
                raise

        return await asyncio.to_thread(_put)

    async def delete(self, key: str) -> None:
        await asyncio.to_thread(self._table.delete_item, Key={PARTITION_KEY: key})

    async def clear(self, prefix: str = "") -> int:
        return await asyncio.to_thread(self._clear_sync, prefix)

    @staticmethod
    def _build_item(key: str, value: Any, ttl_seconds: float | None) -> dict[str, Any]:
        item: dict[str, Any] = {PARTITION_KEY: key, "value": _serialize_value(value)}
        if ttl_seconds:
            item[TTL_ATTRIBUTE] = _serialize_value(time.time() + ttl_seconds)
        return item

    def _clear_sync(self, prefix: str) -> int:
        scan_kwargs: dict[str, Any] = {"ProjectionExpression": "#pk"}
        expr_names = {"#pk": PARTITION_KEY}
        if prefix:
            scan_kwargs["FilterExpression"] = "begins_with(#pk, :prefix)"
            scan_kwargs["ExpressionAttributeValues"] = {":prefix": prefix}
        scan_kwargs["ExpressionAttributeNames"] = expr_names

        deleted = 0
        while True:
            response = self._table.scan(**scan_kwargs)
            for item in response.get("Items", []):
                self._table.delete_item(Key={PARTITION_KEY: item[PARTITION_KEY]})
                deleted += 1
            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                break
            scan_kwargs["ExclusiveStartKey"] = last_key
        return deleted
