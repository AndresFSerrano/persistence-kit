import datetime
from dataclasses import dataclass
from enum import Enum

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table

from persistence_kit.repository.sqlalchemy_repo.sqlalchemy_dataclass_mapper import SqlDataclassMapper


class Status(Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"


@dataclass
class Entity:
    id: int
    name: str
    status: Status
    created: datetime.date


def test_init_requires_dataclass_type():
    class NotDataclass:
        id: int

    with pytest.raises(TypeError):
        SqlDataclassMapper(NotDataclass, Table("t", MetaData()))


def test_table_id_column_unique_columns_entity_type():
    table = Table("entities", MetaData(), Column("id", Integer, primary_key=True), Column("name", String))
    mapper = SqlDataclassMapper(Entity, table, id_column="id", unique_cols={"by_name": "name"})
    assert mapper.table() is table
    assert mapper.id_column() == "id"
    assert mapper.unique_columns() == {"by_name": "name"}
    assert mapper.entity_type() is Entity


def test_id_of_entity():
    mapper = SqlDataclassMapper(Entity, Table("entities", MetaData()))
    assert mapper.id_of(Entity(id=10, name="x", status=Status.ACTIVE, created=datetime.date(2024, 1, 2))) == 10


def test_to_row_converts_enum_and_date():
    mapper = SqlDataclassMapper(Entity, Table("entities", MetaData()))
    row = mapper.to_row(Entity(id=1, name="n", status=Status.INACTIVE, created=datetime.date(2024, 1, 2)))
    assert row["status"] == Status.INACTIVE.value
    assert row["created"] == "2024-01-02"


def test_from_row_uses_default_id_column_and_filters_extra_fields():
    mapper = SqlDataclassMapper(Entity, Table("entities", MetaData()))
    entity = mapper.from_row(
        {"id": 5, "name": "x", "status": Status.ACTIVE, "created": datetime.date(2024, 1, 2), "extra": "ignored"}
    )
    assert entity.id == 5
    assert entity.status == Status.ACTIVE


def test_from_row_with_custom_id_column():
    mapper = SqlDataclassMapper(Entity, Table("entities", MetaData()), id_column="pk")
    entity = mapper.from_row(
        {"pk": 7, "name": "y", "status": Status.INACTIVE, "created": datetime.date(2024, 3, 4), "other": 123}
    )
    assert entity.id == 7
    assert entity.status == Status.INACTIVE
