from dataclasses import asdict, is_dataclass, fields
from typing import Any, Mapping, Type, TypeVar
from uuid import UUID
from datetime import datetime, date
from decimal import Decimal
from enum import Enum

T = TypeVar("T")


def _serialize_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, dict):
        return {k: _serialize_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize_value(v) for v in value]
    return value


def _deserialize_value(value: Any, target_type: type | None = None) -> Any:
    if value is None:
        return None
    if isinstance(value, Decimal):
        if value == int(value):
            return int(value)
        return float(value)
    if isinstance(value, dict) and target_type is None:
        return {k: _deserialize_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_deserialize_value(v) for v in value]
    return value


class DynamoMapper:
    def __init__(
        self,
        entity_type: Type[T],
        table_name: str,
        unique_fields: dict[str, str] | None = None,
    ) -> None:
        if not is_dataclass(entity_type):
            raise TypeError("Entity type must be a dataclass")
        self._entity_type = entity_type
        self._table_name = table_name
        self._unique_fields = dict(unique_fields or {})
        self._field_names = {f.name for f in fields(entity_type)}

    def table_name(self) -> str:
        return self._table_name

    def id_of(self, entity: T) -> Any:
        return getattr(entity, "id")

    def to_item(self, entity: T) -> dict[str, Any]:
        d = asdict(entity)
        result: dict[str, Any] = {}
        for k, v in d.items():
            if k not in self._field_names:
                continue
            serialized = _serialize_value(v)
            if serialized is not None:
                result[k] = serialized
        return result

    def from_item(self, item: Mapping[str, Any]) -> T:
        data: dict[str, Any] = {}
        for f in fields(self._entity_type):
            if f.name in item:
                val = item[f.name]
                if f.type in ("UUID", "uuid.UUID") or (
                    isinstance(f.type, str) and "UUID" in f.type
                ):
                    data[f.name] = UUID(val) if isinstance(val, str) else val
                elif f.type in ("datetime", "datetime.datetime") or (
                    isinstance(f.type, str) and "datetime" in f.type.lower()
                    and "date" != f.type.lower()
                ):
                    data[f.name] = (
                        datetime.fromisoformat(val)
                        if isinstance(val, str)
                        else _deserialize_value(val)
                    )
                elif f.type in ("date", "datetime.date"):
                    data[f.name] = (
                        date.fromisoformat(val)
                        if isinstance(val, str)
                        else _deserialize_value(val)
                    )
                else:
                    data[f.name] = _deserialize_value(val)
        return self._entity_type(**data)

    def unique_fields(self) -> dict[str, str]:
        return self._unique_fields

    def has_attr(self, name: str) -> bool:
        return name in self._field_names

    def attr_to_storage(self, name: str) -> str:
        return name
