from __future__ import annotations

from typing import Optional, Sequence, TypeVar, Generic, Hashable, Mapping, Any
try:
    from typing import override
except ImportError:
    from typing_extensions import override

from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key, Attr, ConditionBase
from persistence_kit.contracts.repository import Repository
from persistence_kit.repository.dynamodb_repo.dynamodb_mapper import DynamoMapper, _serialize_value
from persistence_kit.repository.filter_ops import (
    is_logical_key,
    iter_criteria_groups,
    is_multi_value,
    is_range_dict,
    iter_range_ops,
)

T = TypeVar("T")
TId = TypeVar("TId", bound=Hashable)


def _range_to_condition(attr: Attr, value: Mapping[str, Any]) -> ConditionBase | None:
    conditions: list[ConditionBase] = []
    for op, v in iter_range_ops(value):
        v = _serialize_value(v)
        if op == "between":
            conditions.append(attr.between(_serialize_value(v[0]), _serialize_value(v[1])))
        elif op == "gte":
            conditions.append(attr.gte(v))
        elif op == "gt":
            conditions.append(attr.gt(v))
        elif op == "lte":
            conditions.append(attr.lte(v))
        elif op == "lt":
            conditions.append(attr.lt(v))
        elif op == "in":
            conditions.append(attr.is_in([_serialize_value(i) for i in v]))
        elif op == "eq":
            conditions.append(attr.eq(v))
        elif op == "ne":
            conditions.append(attr.ne(v))
        elif op == "contains":
            conditions.append(attr.contains(str(v)))
        elif op == "icontains":
            conditions.append(attr.contains(str(v).lower()) | attr.contains(str(v).upper()) | attr.contains(str(v)))
        elif op == "startswith" or op == "istartswith":
            conditions.append(attr.begins_with(str(v)))
        elif op == "endswith" or op == "iendswith":
            conditions.append(attr.contains(str(v)))
    if not conditions:
        return None
    result = conditions[0]
    for c in conditions[1:]:
        result = result & c
    return result


def _build_filter(
    mapper: DynamoMapper,
    criteria: Mapping[str, Hashable | list[Hashable] | Mapping[str, Any]],
) -> ConditionBase | None:
    if not criteria:
        return None
    for v in criteria.values():
        if is_multi_value(v) and not v:
            return False
        if is_range_dict(v) and v.get("in") == []:
            return False

    conditions: list[ConditionBase] = []
    for k, v in criteria.items():
        if is_logical_key(k):
            group_conditions: list[ConditionBase] = []
            for group in iter_criteria_groups(v):
                rendered = _build_filter(mapper, group)
                if rendered is False:
                    continue
                if rendered is not None:
                    group_conditions.append(rendered)
            if not group_conditions:
                return False
            combined = group_conditions[0]
            for gc in group_conditions[1:]:
                if k == "or":
                    combined = combined | gc
                else:
                    combined = combined & gc
            conditions.append(combined)
            continue

        field = mapper.attr_to_storage(k)
        attr = Attr(field)
        if v is None:
            conditions.append(attr.not_exists() | attr.eq(None))
        elif is_multi_value(v):
            conditions.append(attr.is_in([_serialize_value(i) for i in v]))
        elif is_range_dict(v):
            range_cond = _range_to_condition(attr, v)
            if range_cond is None:
                return False
            conditions.append(range_cond)
        else:
            conditions.append(attr.eq(_serialize_value(v)))

    if not conditions:
        return None
    result = conditions[0]
    for c in conditions[1:]:
        result = result & c
    return result


class DynamoRepository(Repository[T, TId], Generic[T, TId]):
    def __init__(
        self,
        table_name: str,
        mapper: DynamoMapper,
        region: str = "us-east-1",
    ) -> None:
        self._mapper = mapper
        self._dynamodb = boto3.resource("dynamodb", region_name=region)
        self._table = self._dynamodb.Table(table_name)

    @override
    async def add(self, entity: T) -> None:
        item = self._mapper.to_item(entity)
        self._table.put_item(
            Item=item,
            ConditionExpression=Attr("id").not_exists(),
        )

    @override
    async def get(self, entity_id: TId) -> Optional[T]:
        response = self._table.get_item(Key={"id": _serialize_value(entity_id)})
        item = response.get("Item")
        return self._mapper.from_item(item) if item else None

    @override
    async def list(
        self,
        *,
        offset: int = 0,
        limit: int = 50,
        sort_by: str | None = None,
        sort_desc: bool = False,
    ) -> Sequence[T]:
        items = self._scan_all()
        entities = [self._mapper.from_item(i) for i in items]
        if sort_by is not None:
            if not self._mapper.has_attr(sort_by):
                raise ValueError(f"Invalid sort attribute: {sort_by}")
            entities.sort(key=lambda e: getattr(e, sort_by, None) or "", reverse=sort_desc)
        return entities[offset : offset + limit]

    @override
    async def update(self, entity: T) -> None:
        item = self._mapper.to_item(entity)
        self._table.put_item(Item=item)

    @override
    async def delete(self, entity_id: TId) -> None:
        self._table.delete_item(Key={"id": _serialize_value(entity_id)})

    @override
    async def get_by_index(self, index: str, value: Hashable) -> Optional[T]:
        field = self._mapper.unique_fields().get(index)
        if not field:
            return None
        response = self._table.scan(
            FilterExpression=Attr(field).eq(_serialize_value(value)),
            Limit=1,
        )
        items = response.get("Items", [])
        return self._mapper.from_item(items[0]) if items else None

    @override
    async def count(self) -> int:
        return self._table.item_count

    @override
    async def count_by_fields(
        self,
        criteria: Mapping[str, Hashable | list[Hashable] | Mapping[str, Any]],
    ) -> int:
        if not criteria:
            return await self.count()
        filter_expr = _build_filter(self._mapper, criteria)
        if filter_expr is False:
            return 0
        items = self._scan_with_filter(filter_expr)
        return len(items)

    @override
    async def list_by_fields(
        self,
        criteria: Mapping[str, Hashable | list[Hashable] | Mapping[str, Any]],
        *,
        offset: int = 0,
        limit: Optional[int] = 50,
        sort_by: str | None = None,
        sort_desc: bool = False,
    ) -> Sequence[T]:
        if not criteria:
            return []
        filter_expr = _build_filter(self._mapper, criteria)
        if filter_expr is False:
            return []
        items = self._scan_with_filter(filter_expr)
        entities = [self._mapper.from_item(i) for i in items]
        if sort_by is not None:
            if not self._mapper.has_attr(sort_by):
                raise ValueError(f"Invalid sort attribute: {sort_by}")
            entities.sort(key=lambda e: getattr(e, sort_by, None) or "", reverse=sort_desc)
        end = offset + limit if limit is not None else None
        return entities[offset:end]

    @override
    async def distinct_values(
        self,
        field: str,
        criteria: Mapping[str, Hashable | list[Hashable] | Mapping[str, Any]] | None = None,
    ) -> Sequence[Any]:
        if not self._mapper.has_attr(field):
            raise ValueError(f"Invalid distinct attribute: {field}")
        storage_field = self._mapper.attr_to_storage(field)
        if criteria:
            filter_expr = _build_filter(self._mapper, criteria)
            if filter_expr is False:
                return []
            items = self._scan_with_filter(filter_expr)
        else:
            items = self._scan_all()
        seen: set = set()
        result: list[Any] = []
        for item in items:
            val = item.get(storage_field)
            if val is not None and val not in seen:
                seen.add(val)
                result.append(val)
        return result

    def _scan_all(self) -> list[dict]:
        items: list[dict] = []
        response = self._table.scan()
        items.extend(response.get("Items", []))
        while "LastEvaluatedKey" in response:
            response = self._table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            items.extend(response.get("Items", []))
        return items

    def _scan_with_filter(self, filter_expr: ConditionBase | None) -> list[dict]:
        items: list[dict] = []
        kwargs: dict[str, Any] = {}
        if filter_expr is not None:
            kwargs["FilterExpression"] = filter_expr
        response = self._table.scan(**kwargs)
        items.extend(response.get("Items", []))
        while "LastEvaluatedKey" in response:
            kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
            response = self._table.scan(**kwargs)
            items.extend(response.get("Items", []))
        return items
