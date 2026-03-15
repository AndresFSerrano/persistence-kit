from dataclasses import dataclass
from typing import Optional

import pytest

from persistence_kit.repository.memory_repo.memory_repo import (
    MemoryRepository,
    _get_field_value,
)


@dataclass
class Entity:
    id: int
    name: str
    group: Optional[str]
    score: Optional[int] = None


def test_get_field_value_dict():
    e = {"id": 1, "name": "x"}
    assert _get_field_value(e, "id") == 1
    assert _get_field_value(e, "missing") is None


def test_get_field_value_object():
    e = Entity(id=1, name="x", group=None)
    assert _get_field_value(e, "name") == "x"
    assert _get_field_value(e, "missing") is None


@pytest.mark.asyncio
async def test_add_and_get_and_list_and_indexes_on_object():
    repo = MemoryRepository[Entity, int](
        id_getter=lambda e: e.id,
        unique_indexes={"by_name": lambda e: e.name},
    )

    e1 = Entity(id=1, name="a", group="g1")
    e2 = Entity(id=2, name="b", group="g1")

    await repo.add(e1)
    await repo.add(e2)

    r1 = await repo.get(1)
    r2 = await repo.get(2)
    assert r1 == e1
    assert r2 == e2

    all_items = await repo.list()
    assert all_items == [e1, e2]

    by_name = await repo.get_by_index("by_name", "a")
    assert by_name == e1

    none_index = await repo.get_by_index("unknown", "a")
    assert none_index is None


@pytest.mark.asyncio
async def test_update_reindexes_and_list_pagination():
    repo = MemoryRepository[Entity, int](
        id_getter=lambda e: e.id,
        unique_indexes={"by_name": lambda e: e.name},
    )

    e1 = Entity(id=1, name="a", group="g1")
    e2 = Entity(id=2, name="b", group="g2")
    e3 = Entity(id=3, name="c", group="g3")

    await repo.add(e1)
    await repo.add(e2)
    await repo.add(e3)

    page1 = await repo.list(offset=0, limit=2)
    page2 = await repo.list(offset=2, limit=2)
    assert len(page1) == 2
    assert len(page2) == 1

    e2_updated = Entity(id=2, name="b2", group="g2")
    await repo.update(e2_updated)

    by_old = await repo.get_by_index("by_name", "b")
    by_new = await repo.get_by_index("by_name", "b2")
    assert by_new == e2_updated
    assert by_old == e2_updated


@pytest.mark.asyncio
async def test_delete_removes_from_items_and_indexes():
    repo = MemoryRepository[Entity, int](
        id_getter=lambda e: e.id,
        unique_indexes={"by_name": lambda e: e.name},
    )

    e1 = Entity(id=1, name="a", group=None)
    await repo.add(e1)

    assert await repo.get(1) == e1
    assert await repo.get_by_index("by_name", "a") == e1

    await repo.delete(1)

    assert await repo.get(1) is None
    assert await repo.get_by_index("by_name", "a") is None


@pytest.mark.asyncio
async def test_delete_non_existing_is_noop():
    repo = MemoryRepository[Entity, int](
        id_getter=lambda e: e.id,
        unique_indexes={"by_name": lambda e: e.name},
    )

    e1 = Entity(id=1, name="a", group=None)
    await repo.add(e1)

    await repo.delete(999)

    assert await repo.get(1) == e1


@pytest.mark.asyncio
async def test_list_by_fields_empty_criteria_returns_empty():
    repo = MemoryRepository[Entity, int](id_getter=lambda e: e.id)

    e1 = Entity(id=1, name="a", group="g1")
    await repo.add(e1)

    result = await repo.list_by_fields({})
    assert result == []


@pytest.mark.asyncio
async def test_list_by_fields_matches_object_fields():
    repo = MemoryRepository[Entity, int](id_getter=lambda e: e.id)

    e1 = Entity(id=1, name="a", group="g1")
    e2 = Entity(id=2, name="b", group="g1")
    e3 = Entity(id=3, name="c", group=None)
    await repo.add(e1)
    await repo.add(e2)
    await repo.add(e3)

    res_group_g1 = await repo.list_by_fields({"group": "g1"})
    assert res_group_g1 == [e1, e2]

    res_group_none = await repo.list_by_fields({"group": None})
    assert res_group_none == [e3]


@pytest.mark.asyncio
async def test_list_by_fields_dict_entities_and_pagination():
    repo = MemoryRepository[dict, int](
        id_getter=lambda e: e["id"],
        unique_indexes={"by_code": lambda e: e["code"]},
    )

    d1 = {"id": 1, "code": "x", "kind": "k1"}
    d2 = {"id": 2, "code": "y", "kind": "k1"}
    d3 = {"id": 3, "code": "z", "kind": "k2"}

    await repo.add(d1)
    await repo.add(d2)
    await repo.add(d3)

    res_kind_k1 = await repo.list_by_fields({"kind": "k1"})
    assert res_kind_k1 == [d1, d2]

    res_paginated = await repo.list_by_fields({"kind": "k1"}, offset=1, limit=1)
    assert res_paginated == [d2]

    res_unlimited = await repo.list_by_fields({"kind": "k1"}, offset=0, limit=None)
    assert res_unlimited == [d1, d2]


@pytest.mark.asyncio
async def test_list_by_fields_range_ops_and_unsupported_op():
    repo = MemoryRepository[Entity, int](id_getter=lambda e: e.id)

    e1 = Entity(id=1, name="a", group=None, score=5)
    e2 = Entity(id=2, name="b", group=None, score=10)
    e3 = Entity(id=3, name="c", group=None, score=15)
    await repo.add(e1)
    await repo.add(e2)
    await repo.add(e3)

    res_between = await repo.list_by_fields({"score": {"between": [6, 14]}})
    assert res_between == [e2]

    res_gte = await repo.list_by_fields({"score": {"gte": 10}})
    assert res_gte == [e2, e3]

    res_in = await repo.list_by_fields({"score": {"in": [5, 15]}})
    assert res_in == [e1, e3]

    with pytest.raises(ValueError):
        await repo.list_by_fields({"score": {"bad": 1}})


@pytest.mark.asyncio
async def test_list_by_fields_empty_multi_and_empty_in_range_returns_empty():
    repo = MemoryRepository[Entity, int](id_getter=lambda e: e.id)

    e1 = Entity(id=1, name="a", group=None, score=5)
    await repo.add(e1)

    res_empty_multi = await repo.list_by_fields({"id": []})
    assert res_empty_multi == []

    res_empty_in = await repo.list_by_fields({"score": {"in": []}})
    assert res_empty_in == []


@pytest.mark.asyncio
async def test_list_by_fields_more_range_ops_and_sorting():
    repo = MemoryRepository[Entity, int](id_getter=lambda e: e.id)

    e1 = Entity(id=1, name="a", group=None, score=5)
    e2 = Entity(id=2, name="b", group=None, score=10)
    e3 = Entity(id=3, name="c", group=None, score=15)
    await repo.add(e1)
    await repo.add(e2)
    await repo.add(e3)

    res_gt = await repo.list_by_fields({"score": {"gt": 5}})
    assert res_gt == [e2, e3]

    res_lt = await repo.list_by_fields({"score": {"lt": 15}})
    assert res_lt == [e1, e2]

    res_lte = await repo.list_by_fields({"score": {"lte": 10}})
    assert res_lte == [e1, e2]

    res_eq = await repo.list_by_fields({"score": {"eq": 10}})
    assert res_eq == [e2]

    res_ne = await repo.list_by_fields({"score": {"ne": 10}})
    assert res_ne == [e1, e3]

    res_sorted = await repo.list(sort_by="name", sort_desc=True)
    assert res_sorted == [e3, e2, e1]

    res_sorted_fields = await repo.list_by_fields({"group": None}, sort_by="id", sort_desc=True)
    assert res_sorted_fields == [e3, e2, e1]


@pytest.mark.asyncio
async def test_list_by_fields_supports_text_ops_and_logical_groups():
    repo = MemoryRepository[Entity, int](id_getter=lambda e: e.id)

    e1 = Entity(id=1, name="Calculo Diferencial", group="matematicas")
    e2 = Entity(id=2, name="Fisica I", group="ciencias")
    e3 = Entity(id=3, name="Profesor Juan Perez", group="docentes")
    await repo.add(e1)
    await repo.add(e2)
    await repo.add(e3)

    by_text = await repo.list_by_fields({"name": {"icontains": "calculo"}})
    assert by_text == [e1]

    by_or = await repo.list_by_fields(
        {
            "or": [
                {"name": {"icontains": "fisica"}},
                {"group": {"icontains": "docen"}},
            ]
        },
        sort_by="id",
    )
    assert by_or == [e2, e3]


@pytest.mark.asyncio
async def test_count_and_distinct_values():
    repo = MemoryRepository[Entity, int](id_getter=lambda e: e.id)

    await repo.add(Entity(id=1, name="a", group="g1"))
    await repo.add(Entity(id=2, name="b", group="g1"))
    await repo.add(Entity(id=3, name="a", group="g3"))

    assert await repo.count() == 3
    assert await repo.count_by_fields({"name": "a"}) == 2
    assert await repo.distinct_values("group") == ["g1", "g3"]
    assert await repo.distinct_values("group", {"name": "a"}) == ["g1", "g3"]
