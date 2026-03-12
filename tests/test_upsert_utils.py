from dataclasses import dataclass
from typing import Any, Mapping
from uuid import UUID, uuid4

import pytest

from persistence_kit.utils.upsert import dataclass_field_names, upsert_entity


@dataclass
class DummyEntity:
    id: UUID
    code: str
    name: str
    extra: int | None = None


class FakeRepo:
    def __init__(self) -> None:
        self.added: list[DummyEntity] = []
        self.updated: list[DummyEntity] = []
        self.stored: DummyEntity | None = None

    async def get_by_index(self, index: str, value: Any) -> DummyEntity | None:
        if self.stored is None:
            return None
        if getattr(self.stored, index) == value:
            return self.stored
        return None

    async def add(self, entity: DummyEntity) -> None:
        self.added.append(entity)
        self.stored = entity

    async def update(self, entity: DummyEntity) -> None:
        self.updated.append(entity)
        self.stored = entity


def test_dataclass_field_names_ok():
    names = dataclass_field_names(DummyEntity)
    assert names == {"id", "code", "name", "extra"}


def test_dataclass_field_names_non_dataclass_raises():
    class NotDataclass:
        pass

    with pytest.raises(TypeError):
        dataclass_field_names(NotDataclass)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_upsert_entity_creates_when_not_existing():
    repo = FakeRepo()
    payload: Mapping[str, Any] = {
        "code": "C1",
        "name": "Foo",
        "ignored": "x",
    }

    entity = await upsert_entity(repo, DummyEntity, "code", payload, extra=5)

    assert isinstance(entity, DummyEntity)
    assert entity.code == "C1"
    assert entity.name == "Foo"
    assert entity.extra == 5
    assert isinstance(entity.id, UUID)
    assert repo.added == [entity]
    assert repo.updated == []


@pytest.mark.asyncio
async def test_upsert_entity_updates_when_existing_changed():
    repo = FakeRepo()
    existing = DummyEntity(id=uuid4(), code="C1", name="Old", extra=None)
    repo.stored = existing

    payload: Mapping[str, Any] = {
        "code": "C1",
        "name": "New",
    }

    result = await upsert_entity(repo, DummyEntity, "code", payload)

    assert result is existing
    assert existing.name == "New"
    assert repo.updated == [existing]
    assert repo.added == []


@pytest.mark.asyncio
async def test_upsert_entity_does_not_update_when_data_unchanged():
    repo = FakeRepo()
    existing = DummyEntity(id=uuid4(), code="C1", name="Same", extra=7)
    repo.stored = existing

    payload: Mapping[str, Any] = {
        "code": "C1",
        "name": "Same",
        "extra": 7,
    }

    result = await upsert_entity(repo, DummyEntity, "code", payload)

    assert result is existing
    assert repo.updated == []
    assert repo.added == []
