from __future__ import annotations

from typing import Any, Iterable, Mapping


_SIMPLE_OPS = {"gte", "gt", "lte", "lt", "eq", "ne"}


def is_multi_value(value: Any) -> bool:
    return isinstance(value, list)


def is_range_dict(value: Any) -> bool:
    return isinstance(value, dict)


def iter_range_ops(value: Mapping[str, Any]) -> Iterable[tuple[str, Any]]:
    if not value:
        return []
    items = list(value.items())
    for op, v in items:
        if op == "between":
            if not isinstance(v, list) or len(v) != 2:
                raise ValueError("between expects a list with exactly two values")
        elif op == "in":
            if not isinstance(v, list):
                raise ValueError("in expects a list")
        elif op in _SIMPLE_OPS:
            continue
        else:
            raise ValueError(f"Unsupported operator: {op}")
    return items
