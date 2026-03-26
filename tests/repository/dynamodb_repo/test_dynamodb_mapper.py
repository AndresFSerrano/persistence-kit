from dataclasses import dataclass
from datetime import datetime, date
from decimal import Decimal
from enum import Enum
from uuid import UUID, uuid4

import pytest

from persistence_kit.repository.dynamodb_repo.dynamodb_mapper import (
    DynamoMapper,
    _serialize_value,
    _deserialize_value,
)


@dataclass
class SimpleEntity:
    id: str
    name: str
    value: int


class Color(str, Enum):
    RED = "red"
    BLUE = "blue"


@dataclass
class ComplexEntity:
    id: UUID
    label: str
    score: float
    color: Color
    created_at: datetime
    tags: list


def test_init_requires_dataclass():
    class NotDataclass:
        id: str

    with pytest.raises(TypeError):
        DynamoMapper(NotDataclass, "table")


def test_table_name_and_unique_fields():
    mapper = DynamoMapper(SimpleEntity, "my_table", unique_fields={"u": "name"})
    assert mapper.table_name() == "my_table"
    assert mapper.unique_fields() == {"u": "name"}


def test_id_of():
    mapper = DynamoMapper(SimpleEntity, "t")
    e = SimpleEntity(id="abc", name="n", value=10)
    assert mapper.id_of(e) == "abc"


def test_has_attr():
    mapper = DynamoMapper(SimpleEntity, "t")
    assert mapper.has_attr("name") is True
    assert mapper.has_attr("id") is True
    assert mapper.has_attr("unknown") is False


def test_attr_to_storage():
    mapper = DynamoMapper(SimpleEntity, "t")
    assert mapper.attr_to_storage("name") == "name"
    assert mapper.attr_to_storage("id") == "id"


def test_to_item_simple():
    mapper = DynamoMapper(SimpleEntity, "t")
    e = SimpleEntity(id="abc", name="n", value=10)
    item = mapper.to_item(e)
    assert item == {"id": "abc", "name": "n", "value": 10}


def test_to_item_skips_none_values():
    @dataclass
    class WithOptional:
        id: str
        name: str
        extra: str | None = None

    mapper = DynamoMapper(WithOptional, "t")
    e = WithOptional(id="1", name="a", extra=None)
    item = mapper.to_item(e)
    assert "extra" not in item


def test_from_item_simple():
    mapper = DynamoMapper(SimpleEntity, "t")
    item = {"id": "abc", "name": "n", "value": 10}
    e = mapper.from_item(item)
    assert isinstance(e, SimpleEntity)
    assert e.id == "abc"
    assert e.name == "n"
    assert e.value == 10


def test_from_item_converts_decimals():
    mapper = DynamoMapper(SimpleEntity, "t")
    item = {"id": "abc", "name": "n", "value": Decimal("10")}
    e = mapper.from_item(item)
    assert e.value == 10
    assert isinstance(e.value, int)


def test_serialize_value_uuid():
    uid = uuid4()
    assert _serialize_value(uid) == str(uid)


def test_serialize_value_enum():
    assert _serialize_value(Color.RED) == "red"


def test_serialize_value_float_to_decimal():
    result = _serialize_value(3.14)
    assert isinstance(result, Decimal)
    assert result == Decimal("3.14")


def test_serialize_value_datetime():
    dt = datetime(2024, 1, 15, 10, 30, 0)
    assert _serialize_value(dt) == "2024-01-15T10:30:00"


def test_serialize_value_date():
    d = date(2024, 1, 15)
    assert _serialize_value(d) == "2024-01-15"


def test_serialize_value_none():
    assert _serialize_value(None) is None


def test_serialize_value_nested_dict():
    result = _serialize_value({"a": 1.5, "b": None})
    assert result == {"a": Decimal("1.5"), "b": None}


def test_serialize_value_list():
    uid = uuid4()
    result = _serialize_value([uid, 1.5])
    assert result == [str(uid), Decimal("1.5")]


def test_deserialize_value_decimal_int():
    assert _deserialize_value(Decimal("10")) == 10
    assert isinstance(_deserialize_value(Decimal("10")), int)


def test_deserialize_value_decimal_float():
    result = _deserialize_value(Decimal("3.14"))
    assert result == 3.14
    assert isinstance(result, float)


def test_deserialize_value_none():
    assert _deserialize_value(None) is None


def test_roundtrip_complex_entity():
    mapper = DynamoMapper(ComplexEntity, "complex")
    uid = uuid4()
    dt = datetime(2024, 6, 15, 12, 0, 0)
    entity = ComplexEntity(
        id=uid,
        label="test",
        score=9.5,
        color=Color.BLUE,
        created_at=dt,
        tags=["a", "b"],
    )
    item = mapper.to_item(entity)
    assert item["id"] == str(uid)
    assert item["score"] == Decimal("9.5")
    assert item["color"] == "blue"
    assert item["created_at"] == "2024-06-15T12:00:00"
    assert item["tags"] == ["a", "b"]
