from dataclasses import dataclass
from typing import Any, Mapping

import pytest

from persistence_kit.repository.mongo_repo.mongo_mapper import DataclassMapper


@dataclass
class Entity:
    id: str
    name: str
    value: int


def test_init_requires_dataclass_type():
    class NotDataclass:
        id: str

    with pytest.raises(TypeError):
        DataclassMapper(NotDataclass, "col")


def test_collection_and_unique_fields_and_id_field_defaults():
    mapper = DataclassMapper(Entity, "entities", unique_fields={"u": "name"})
    assert mapper.collection() == "entities"
    assert mapper.id_field() == "_id"
    assert mapper.unique_fields() == {"u": "name"}


def test_collection_and_id_field_custom():
    mapper = DataclassMapper(Entity, "entities_custom", id_field="entity_id")
    assert mapper.collection() == "entities_custom"
    assert mapper.id_field() == "entity_id"


def test_id_of_entity():
    mapper = DataclassMapper(Entity, "entities")
    e = Entity(id="abc", name="n", value=10)
    assert mapper.id_of(e) == "abc"


def test_to_document_excludes_id_and_unknown_fields():
    mapper = DataclassMapper(Entity, "entities")
    e = Entity(id="abc", name="n", value=10)
    doc = mapper.to_document(e)
    assert "id" not in doc
    assert doc == {"name": "n", "value": 10}


def test_from_document_uses_default_id_field_and_filters_extra_fields():
    mapper = DataclassMapper(Entity, "entities")
    doc: Mapping[str, Any] = {
        "_id": "abc",
        "name": "n",
        "value": 10,
        "extra": "ignored",
    }
    e = mapper.from_document(doc)
    assert isinstance(e, Entity)
    assert e.id == "abc"
    assert e.name == "n"
    assert e.value == 10


def test_from_document_with_custom_id_field():
    mapper = DataclassMapper(Entity, "entities", id_field="entity_id")
    doc: Mapping[str, Any] = {
        "entity_id": "xyz",
        "name": "m",
        "value": 42,
    }
    e = mapper.from_document(doc)
    assert e.id == "xyz"
    assert e.name == "m"
    assert e.value == 42
