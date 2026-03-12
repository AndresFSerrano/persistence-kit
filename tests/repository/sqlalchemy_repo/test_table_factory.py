from dataclasses import dataclass
from typing import List, Optional, Union
import datetime
import types
import uuid

import pytest
from sqlalchemy import Boolean, DateTime, Float, Integer, MetaData, String, Table
from sqlalchemy.dialects.postgresql import ARRAY, UUID as PGUUID

from persistence_kit.repository.sqlalchemy_repo.table_factory import (
    _is_list_of,
    _sa_type,
    _unwrap_optional,
    build_table_from_dataclass,
)


def test_unwrap_optional_union_typing():
    base, is_opt = _unwrap_optional(Union[int, None])
    assert base is int
    assert is_opt is True


def test_unwrap_optional_pep604():
    if not hasattr(types, "UnionType"):
        pytest.skip("PEP 604 union not supported")
    base, is_opt = _unwrap_optional(int | None)
    assert base is int
    assert is_opt is True


def test_unwrap_optional_non_optional():
    base, is_opt = _unwrap_optional(str)
    assert base is str
    assert is_opt is False


def test_is_list_of_variants():
    assert _is_list_of(List[uuid.UUID], uuid.UUID) is True
    assert _is_list_of(List[int], uuid.UUID) is False
    assert _is_list_of(int, uuid.UUID) is False


def test_sa_type_mappings():
    assert isinstance(_sa_type(uuid.UUID), PGUUID)
    assert _sa_type(str) is String
    assert _sa_type(bool) is Boolean
    assert _sa_type(int) is Integer
    assert _sa_type(float) is Float
    assert _sa_type(datetime.datetime) is DateTime
    assert _sa_type(bytes) is String


@dataclass
class SampleEntity:
    id: uuid.UUID
    name: str
    active: bool
    count: Optional[int]
    created_at: datetime.datetime
    tags: List[uuid.UUID]


def test_build_table_from_dataclass_creates_columns_with_types_and_nullability():
    table = build_table_from_dataclass(SampleEntity, "sample_entities", MetaData())
    assert isinstance(table, Table)
    cols = {column.name: column for column in table.columns}
    assert isinstance(cols["id"].type, PGUUID)
    assert cols["id"].primary_key is True
    assert isinstance(cols["name"].type, String)
    assert cols["name"].nullable is False
    assert isinstance(cols["count"].type, Integer)
    assert cols["count"].nullable is True
    assert isinstance(cols["tags"].type, ARRAY)
    assert isinstance(cols["tags"].type.item_type, PGUUID)


def test_build_table_from_dataclass_reuses_existing_table():
    meta = MetaData()
    assert build_table_from_dataclass(SampleEntity, "sample_entities", meta) is build_table_from_dataclass(
        SampleEntity, "sample_entities", meta
    )


def test_build_table_from_dataclass_non_dataclass_raises():
    class NotDataclass:
        id: int

    with pytest.raises(TypeError):
        build_table_from_dataclass(NotDataclass, "bad", MetaData())
