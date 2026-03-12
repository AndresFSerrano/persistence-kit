from dataclasses import fields as dc_fields, is_dataclass
from typing import Any, Mapping, Type, TypeVar
from uuid import uuid4

T = TypeVar("T")


def dataclass_field_names(cls: Type[T]) -> set[str]:
    if not is_dataclass(cls):
        raise TypeError("cls must be a dataclass")
    return {f.name for f in dc_fields(cls)}


async def upsert_entity(
    repo,
    entity_cls: Type[T],
    unique_key: str,
    payload: Mapping[str, Any],
    **extra,
) -> T:
    valid_fields = dataclass_field_names(entity_cls)
    data = {k: v for k, v in payload.items() if k in valid_fields}
    data.update({k: v for k, v in extra.items() if k in valid_fields})
    existing = await repo.get_by_index(unique_key, payload[unique_key])
    if existing:
        changed = False
        for k, v in data.items():
            current = object.__getattribute__(existing, k)
            if current != v:
                object.__setattr__(existing, k, v)
                changed = True
        if changed:
            await repo.update(existing)
        return existing

    entity = entity_cls(id=uuid4(), **data)
    await repo.add(entity)
    return entity
