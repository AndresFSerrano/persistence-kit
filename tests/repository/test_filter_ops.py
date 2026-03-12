import pytest

from persistence_kit.repository.filter_ops import iter_range_ops


def test_iter_range_ops_empty_returns_empty():
    assert list(iter_range_ops({})) == []


def test_iter_range_ops_valid_ops():
    ops = list(iter_range_ops({"gte": 1, "lt": 5, "in": [1, 2]}))
    assert ("gte", 1) in ops
    assert ("lt", 5) in ops
    assert ("in", [1, 2]) in ops


def test_iter_range_ops_between_requires_two_values():
    with pytest.raises(ValueError):
        list(iter_range_ops({"between": [1]}))


def test_iter_range_ops_in_requires_list():
    with pytest.raises(ValueError):
        list(iter_range_ops({"in": "nope"}))


def test_iter_range_ops_unsupported_operator():
    with pytest.raises(ValueError):
        list(iter_range_ops({"bad": 1}))
