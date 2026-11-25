from dataclasses import is_dataclass, fields as dc_fields
from typing import Any, get_origin, get_args, Union
import types
from enum import Enum
from sqlalchemy import JSON, Table, Column, String, Boolean, Integer, Float, DateTime, MetaData
from sqlalchemy.types import TypeDecorator
from sqlalchemy.dialects.postgresql import UUID as PGUUID, ARRAY
import uuid
import datetime

metadata = MetaData()


class UuidListAsJson(TypeDecorator):
    impl = JSON
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return [str(item) for item in value]

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return [uuid.UUID(item) for item in value]


def _uuid_list_type():
    return ARRAY(PGUUID(as_uuid=True)).with_variant(UuidListAsJson(), "sqlite")

def _unwrap_optional(tp: Any) -> tuple[Any, bool]:
    origin = get_origin(tp)
    # Soporta typing.Union y PEP 604 (X | Y)
    if origin in (Union, getattr(types, "UnionType", None)):
        args = [a for a in get_args(tp) if a is not type(None)]
        return (args[0] if args else Any), True
    return tp, False

def _is_list_of(tp: Any, inner: Any) -> bool:
    origin = get_origin(tp)
    if origin in (list, tuple):
        args = get_args(tp)
        return len(args) == 1 and args[0] is inner
    return False

def _sa_type(ft: Any):
    if ft is uuid.UUID:
        return PGUUID(as_uuid=True)
    if ft is str:
        return String
    if ft is bool:
        return Boolean
    if ft is int:
        return Integer
    if ft is float:
        return Float
    if ft is datetime.datetime:
        return DateTime
    return String

def build_table_from_dataclass(entity_type: type, table_name: str, meta: MetaData | None = None) -> Table:
    if not is_dataclass(entity_type):
        raise TypeError("entity_type must be dataclass")
    m = meta or metadata
    if table_name in m.tables:
        return m.tables[table_name]

    cols = []
    for f in dc_fields(entity_type):
        ft, is_optional = _unwrap_optional(f.type)

        if _is_list_of(ft, uuid.UUID):
            cols.append(Column(f.name, _uuid_list_type(), nullable=True))
            continue

        coltype = _sa_type(ft)
        if f.name == "id":
            cols.append(Column(f.name, coltype, primary_key=True))
        else:
            cols.append(Column(f.name, coltype, nullable=is_optional))

    return Table(table_name, m, *cols)
