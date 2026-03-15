from dataclasses import dataclass
from typing import Any, Dict, Hashable, Mapping, Optional, Sequence

import pytest

from persistence_kit.repository_factory.view.populating_repository import (
    PopulatingRepository,
    _to_dict,
)


@dataclass
class Child:
    id: int
    code: str
    name: str
    parent_id: Optional[int] = None


@dataclass
class Parent:
    id: int
    child_id: Optional[int]
    child_ids: list[int]


class FakeInnerRepo:
    def __init__(self, entity: Any, entities: Optional[Sequence[Any]] = None):
        self.entity = entity
        self.entities = list(entities) if entities is not None else [entity]
        self.called: Dict[str, Any] = {}
        self.list_calls: list[Dict[str, Any]] = []
        self.list_by_fields_calls: list[Dict[str, Any]] = []

    async def add(self, entity: Any) -> None:
        self.called["add"] = entity

    async def get(self, entity_id: Hashable) -> Any:
        self.called["get"] = entity_id
        return self.entity

    async def list(self, *, offset=0, limit=50, sort_by=None, sort_desc=False) -> Sequence[Any]:
        payload = {"offset": offset, "limit": limit, "sort_by": sort_by, "sort_desc": sort_desc}
        self.called["list"] = payload
        self.list_calls.append(payload)
        return self.entities

    async def update(self, entity: Any) -> None:
        self.called["update"] = entity

    async def delete(self, entity_id: Hashable) -> None:
        self.called["delete"] = entity_id

    async def get_by_index(self, index: str, value: Hashable) -> Any:
        self.called["get_by_index"] = {"index": index, "value": value}
        return self.entity

    async def count(self) -> int:
        self.called["count"] = True
        return len(self.entities)

    async def count_by_fields(self, criteria: Mapping[str, Hashable]) -> int:
        self.called["count_by_fields"] = {"criteria": dict(criteria)}
        return 1

    async def list_by_fields(self, criteria: Mapping[str, Hashable], *, offset=0, limit=50, sort_by=None, sort_desc=False):
        payload = {
            "criteria": dict(criteria),
            "offset": offset,
            "limit": limit,
            "sort_by": sort_by,
            "sort_desc": sort_desc,
        }
        self.called["list_by_fields"] = payload
        self.list_by_fields_calls.append(payload)
        return self.entities


class FakeChildRepo:
    def __init__(self, by_id: Dict[int, Child], by_code: Dict[str, Child]):
        self.by_id = by_id
        self.by_code = by_code
        self.called: Dict[str, Any] = {}

    async def get(self, entity_id: Hashable) -> Any:
        self.called.setdefault("get", []).append(entity_id)
        return self.by_id.get(entity_id)

    async def get_by_index(self, index: str, value: Hashable) -> Any:
        self.called.setdefault("get_by_index", []).append({"index": index, "value": value})
        if index == "code":
            return self.by_code.get(value)
        return None

    async def list_by_fields(self, criteria: Mapping[str, Any], *, offset=0, limit=50, sort_by=None, sort_desc=False):
        self.called.setdefault("list_by_fields", []).append(
            {"criteria": dict(criteria), "offset": offset, "limit": limit, "sort_by": sort_by, "sort_desc": sort_desc}
        )
        parent_id = criteria.get("parent_id")
        rows = [child for child in self.by_id.values() if child.parent_id == parent_id]
        rows = rows[offset:]
        if limit is not None:
            rows = rows[:limit]
        return rows


@dataclass
class Info:
    code: str
    text: str


class FakeInfoRepo:
    def __init__(self, by_code: Dict[str, Info]):
        self.by_code = by_code
        self.called: Dict[str, Any] = {}

    async def get(self, entity_id: Hashable) -> Any:
        self.called.setdefault("get", []).append(entity_id)
        return None

    async def get_by_index(self, index: str, value: Hashable) -> Any:
        self.called.setdefault("get_by_index", []).append({"index": index, "value": value})
        if index == "code":
            return self.by_code.get(value)
        return None


def test_to_dict_dataclass():
    data = _to_dict(Child(id=1, code="C1", name="n"))
    assert data["id"] == 1
    assert data["code"] == "C1"


def test_to_dict_dict():
    original = {"a": 1, "b": 2}
    data = _to_dict(original)
    assert data == original
    assert data is not original


def test_to_dict_other_returns_empty():
    assert _to_dict(123) == {}


@pytest.mark.asyncio
async def test_get_with_and_nested_population(monkeypatch):
    import persistence_kit.repository_factory.view.populating_repository as pr

    inner = FakeInnerRepo(Parent(id=10, child_id=1, child_ids=[]))
    child_repo = FakeChildRepo(by_id={1: Child(id=1, code="C1", name="child1")}, by_code={})
    info_repo = FakeInfoRepo(by_code={"C1": Info(code="C1", text="extra")})

    def fake_get_entity_config(key: str):
        if key == "parent":
            return {"relations": {"child": {"local_field": "child_id", "target": "child", "by": "id", "many": False}}}
        if key == "child":
            return {"relations": {"info": {"local_field": "code", "target": "info", "by": "code", "many": False}}}
        return {"relations": {}}

    monkeypatch.setattr(pr, "get_entity_config", fake_get_entity_config)
    repo = PopulatingRepository("parent", inner, lambda name: {"child": child_repo, "info": info_repo}[name])

    result = await repo.get_with(10, include=["child.info"])
    assert result["child"]["id"] == 1
    assert result["child"]["info"]["text"] == "extra"


@pytest.mark.asyncio
async def test_list_with_nested_sort_applies_after_population(monkeypatch):
    import persistence_kit.repository_factory.view.populating_repository as pr

    parents = [
        Parent(id=10, child_id=1, child_ids=[]),
        Parent(id=20, child_id=2, child_ids=[]),
        Parent(id=30, child_id=3, child_ids=[]),
    ]
    child_repo = FakeChildRepo(
        by_id={
            1: Child(id=1, code="c1", name="Charlie"),
            2: Child(id=2, code="c2", name="Bravo"),
            3: Child(id=3, code="c3", name="Alpha"),
        },
        by_code={},
    )
    inner = FakeInnerRepo(parents[0], entities=parents)
    monkeypatch.setattr(
        pr,
        "get_entity_config",
        lambda _: {"relations": {"child": {"local_field": "child_id", "target": "child", "by": "id", "many": False}}},
    )

    repo = PopulatingRepository("parent", inner, lambda _: child_repo)
    result = await repo.list_with(offset=1, limit=1, include=["child"], sort_by="child.name", sort_desc=False)
    assert result[0]["child"]["name"] == "Bravo"
    assert inner.list_calls[0]["sort_by"] is None


@pytest.mark.asyncio
async def test_list_by_fields_nested_sort_and_reverse_relation(monkeypatch):
    import persistence_kit.repository_factory.view.populating_repository as pr

    parent = Parent(id=10, child_id=None, child_ids=[])
    child_repo = FakeChildRepo(
        by_id={
            1: Child(id=1, code="c1", name="Charlie", parent_id=10),
            2: Child(id=2, code="c2", name="Bravo", parent_id=10),
        },
        by_code={},
    )
    inner = FakeInnerRepo(parent, entities=[parent])

    monkeypatch.setattr(
        pr,
        "get_entity_config",
        lambda _: {
            "relations": {
                "children": {
                    "local_field": "id",
                    "target": "child",
                    "target_field": "parent_id",
                    "many": True,
                }
            }
        },
    )

    repo = PopulatingRepository("parent", inner, lambda _: child_repo)
    result = await repo.get_with(10, include=["children"])
    assert {row["id"] for row in result["children"]} == {1, 2}


@pytest.mark.asyncio
async def test_count_and_count_by_fields_delegate_to_inner():
    parent = Parent(id=10, child_id=1, child_ids=[])
    inner = FakeInnerRepo(parent, entities=[parent, parent])
    repo = PopulatingRepository("parent", inner, lambda _: None)
    assert await repo.count() == 2
    assert await repo.count_by_fields({"id": 10}) == 1


@pytest.mark.asyncio
async def test_list_by_fields_supports_nested_text_search_and_pagination(monkeypatch):
    import persistence_kit.repository_factory.view.populating_repository as pr

    parents = [
        Parent(id=10, child_id=1, child_ids=[]),
        Parent(id=20, child_id=2, child_ids=[]),
        Parent(id=30, child_id=3, child_ids=[]),
    ]
    child_repo = FakeChildRepo(
        by_id={
            1: Child(id=1, code="MAT101", name="Calculo"),
            2: Child(id=2, code="FIS101", name="Fisica"),
            3: Child(id=3, code="PROF01", name="Juan Perez"),
        },
        by_code={},
    )
    inner = FakeInnerRepo(parents[0], entities=parents)
    monkeypatch.setattr(
        pr,
        "get_entity_config",
        lambda _: {"relations": {"child": {"local_field": "child_id", "target": "child", "by": "id", "many": False}}},
    )

    repo = PopulatingRepository("parent", inner, lambda _: child_repo)
    result = await repo.list_by_fields(
        {
            "or": [
                {"child.name": {"icontains": "fis"}},
                {"child.name": {"icontains": "juan"}},
            ]
        },
        offset=0,
        limit=1,
        include=["child"],
        sort_by="child.name",
        sort_desc=False,
    )

    assert len(result) == 1
    assert result[0]["child"]["name"] == "Fisica"


@pytest.mark.asyncio
async def test_count_by_fields_supports_nested_criteria(monkeypatch):
    import persistence_kit.repository_factory.view.populating_repository as pr

    parents = [
        Parent(id=10, child_id=1, child_ids=[]),
        Parent(id=20, child_id=2, child_ids=[]),
    ]
    child_repo = FakeChildRepo(
        by_id={
            1: Child(id=1, code="MAT101", name="Calculo"),
            2: Child(id=2, code="MAT102", name="Matematicas Discretas"),
        },
        by_code={},
    )
    inner = FakeInnerRepo(parents[0], entities=parents)
    monkeypatch.setattr(
        pr,
        "get_entity_config",
        lambda _: {"relations": {"child": {"local_field": "child_id", "target": "child", "by": "id", "many": False}}},
    )

    repo = PopulatingRepository("parent", inner, lambda _: child_repo)
    total = await repo.count_by_fields({"child.name": {"icontains": "mat"}})

    assert total == 1
