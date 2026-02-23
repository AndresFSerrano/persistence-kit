from dataclasses import asdict, is_dataclass, fields
from typing import Any, Mapping, Type, TypeVar, Generic
from enum import Enum
import datetime

from sqlalchemy import Table

T = TypeVar("T")


class SqlDataclassMapper(Generic[T]):
    def __init__(
        self,
        entity_type: Type[T],
        table: Table,
        id_column: str = "id",
        unique_cols: dict[str, str] | None = None,
    ) -> None:
        if not is_dataclass(entity_type):
            raise TypeError("Entity type must be a dataclass")
        self._entity_type = entity_type
        self._table = table
        self._id_column = id_column
        self._unique_cols = dict(unique_cols or {})
        self._field_names = {f.name for f in fields(entity_type)}

    def table(self) -> Table:
        return self._table

    def id_of(self, entity: T):
        return getattr(entity, "id")

    def to_row(self, entity: T) -> Mapping[str, Any]:
        data = asdict(entity)
        for k, v in data.items():
            if isinstance(v, Enum):
                data[k] = v.value
            elif isinstance(v, datetime.date):
                data[k] = v.isoformat()
        return data

    def from_row(self, row: Mapping[str, Any]) -> T:
        data = dict(row)
        if "id" not in data and self._id_column in data:
            data["id"] = data[self._id_column]
        data = {k: v for k, v in data.items() if k in self._field_names}
        return self._entity_type(**data)

    def id_column(self) -> str:
        return self._id_column

    def unique_columns(self) -> dict[str, str]:
        return self._unique_cols

    def entity_type(self) -> type[T]:
        return self._entity_type

    def has_attr(self, name: str) -> bool:
        return name == "id" or name in self._field_names

    def attr_to_storage(self, name: str) -> str:
        if name == "id":
            return self._id_column
        return name
