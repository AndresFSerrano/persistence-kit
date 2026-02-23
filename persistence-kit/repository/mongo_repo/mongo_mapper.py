from dataclasses import asdict, is_dataclass, fields
from typing import Any, Mapping, Type, TypeVar

T = TypeVar("T")

class DataclassMapper:
    def __init__(
        self,
        entity_type: Type[T],
        collection_name: str,
        id_field: str = "_id",
        unique_fields: dict[str, str] | None = None,
    ) -> None:
        if not is_dataclass(entity_type):
            raise TypeError("Entity type must be a dataclass")
        self._entity_type = entity_type
        self._collection_name = collection_name
        self._id_field = id_field
        self._unique_fields = dict(unique_fields or {})
        self._field_names = {f.name for f in fields(entity_type)}

    def collection(self) -> str:
        return self._collection_name

    def id_of(self, entity: T):
        return getattr(entity, "id")

    def to_document(self, entity: T) -> dict:
        d = asdict(entity)
        d.pop("id", None)
        return {k: v for k, v in d.items() if k in self._field_names}

    def from_document(self, doc: Mapping[str, Any]) -> T:
        data = dict(doc)
        data["id"] = data.pop(self._id_field)
        data = {k: v for k, v in data.items() if k in self._field_names}
        return self._entity_type(**data)

    def id_field(self) -> str:
        return self._id_field

    def unique_fields(self) -> dict[str, str]:
        return self._unique_fields

    def has_attr(self, name: str) -> bool:
        return name == "id" or name in self._field_names

    def attr_to_storage(self, name: str) -> str:
        if name == "id":
            return self._id_field
        return name
