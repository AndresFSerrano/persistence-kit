from __future__ import annotations

import re
import unicodedata
from typing import Any, Callable, Iterable, Mapping


_SIMPLE_OPS = {"gte", "gt", "lte", "lt", "eq", "ne"}
_STRING_OPS = {"contains", "icontains", "startswith", "istartswith", "endswith", "iendswith"}
_LOGICAL_KEYS = {"or", "and"}
_NON_ALNUM_RE = re.compile(r"[^0-9a-z]+")
_WHITESPACE_RE = re.compile(r"\s+")


def is_multi_value(value: Any) -> bool:
    return isinstance(value, list)


def is_range_dict(value: Any) -> bool:
    return isinstance(value, dict)


def is_logical_key(value: Any) -> bool:
    return value in _LOGICAL_KEYS


def iter_criteria_groups(value: Any) -> Iterable[Mapping[str, Any]]:
    if not isinstance(value, list) or not value:
        raise ValueError("Logical operators expect a non-empty list of criteria")
    for item in value:
        if not isinstance(item, Mapping):
            raise ValueError("Logical operators expect criteria mappings")
    return value


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
        elif op in _STRING_OPS:
            continue
        else:
            raise ValueError(f"Unsupported operator: {op}")
    return items


def normalize_search_text(value: Any) -> str:
    if value is None:
        return ""
    normalized = unicodedata.normalize("NFKD", str(value))
    without_marks = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    lowered = without_marks.lower()
    collapsed = _NON_ALNUM_RE.sub(" ", lowered)
    return _WHITESPACE_RE.sub(" ", collapsed).strip()


def tokenize_search_text(value: Any) -> list[str]:
    normalized = normalize_search_text(value)
    if not normalized:
        return []
    return normalized.split()


def _matches_string_op(current: Any, op: str, expected: Any) -> bool:
    if current is None:
        return False
    current_text = str(current)
    expected_text = str(expected)
    if op == "contains":
        return expected_text in current_text
    if op == "icontains":
        expected_tokens = tokenize_search_text(expected)
        if not expected_tokens:
            return False
        normalized_current = normalize_search_text(current)
        return all(token in normalized_current for token in expected_tokens)
    if op == "startswith":
        return current_text.startswith(expected_text)
    if op == "istartswith":
        normalized_expected = normalize_search_text(expected)
        if not normalized_expected:
            return False
        return normalize_search_text(current).startswith(normalized_expected)
    if op == "endswith":
        return current_text.endswith(expected_text)
    if op == "iendswith":
        normalized_expected = normalize_search_text(expected)
        if not normalized_expected:
            return False
        return normalize_search_text(current).endswith(normalized_expected)
    raise ValueError(f"Unsupported operator: {op}")


def match_value(current: Any, expected: Any) -> bool:
    if expected is None:
        return current is None
    if is_multi_value(expected):
        return current in expected
    if is_range_dict(expected):
        ops = list(iter_range_ops(expected))
        if not ops:
            return False
        for op, value in ops:
            if op == "between":
                lo, hi = value
                if current is None or current < lo or current > hi:
                    return False
            elif op == "gte":
                if current is None or current < value:
                    return False
            elif op == "gt":
                if current is None or current <= value:
                    return False
            elif op == "lte":
                if current is None or current > value:
                    return False
            elif op == "lt":
                if current is None or current >= value:
                    return False
            elif op == "in":
                if current not in value:
                    return False
            elif op == "eq":
                if current != value:
                    return False
            elif op == "ne":
                if current == value:
                    return False
            else:
                if not _matches_string_op(current, op, value):
                    return False
        return True
    return current == expected


def match_criteria(
    criteria: Mapping[str, Any],
    resolve_value: Callable[[str], Any],
) -> bool:
    if not criteria:
        return False
    for field, expected in criteria.items():
        if is_logical_key(field):
            groups = list(iter_criteria_groups(expected))
            if field == "or":
                if not any(match_criteria(group, resolve_value) for group in groups):
                    return False
            else:
                if not all(match_criteria(group, resolve_value) for group in groups):
                    return False
            continue
        if not match_value(resolve_value(field), expected):
            return False
    return True
