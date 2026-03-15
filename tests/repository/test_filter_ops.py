import pytest

from persistence_kit.repository.filter_ops import iter_criteria_groups, iter_range_ops, match_criteria, match_value


def test_iter_range_ops_empty_returns_empty():
    assert list(iter_range_ops({})) == []


def test_iter_range_ops_valid_ops():
    ops = list(iter_range_ops({"gte": 1, "lt": 5, "in": [1, 2], "icontains": "ab"}))
    assert ("gte", 1) in ops
    assert ("lt", 5) in ops
    assert ("in", [1, 2]) in ops
    assert ("icontains", "ab") in ops


def test_iter_range_ops_between_requires_two_values():
    with pytest.raises(ValueError):
        list(iter_range_ops({"between": [1]}))


def test_iter_range_ops_in_requires_list():
    with pytest.raises(ValueError):
        list(iter_range_ops({"in": "nope"}))


def test_iter_range_ops_unsupported_operator():
    with pytest.raises(ValueError):
        list(iter_range_ops({"bad": 1}))


def test_iter_criteria_groups_requires_non_empty_mapping_list():
    with pytest.raises(ValueError):
        list(iter_criteria_groups([]))
    with pytest.raises(ValueError):
        list(iter_criteria_groups([1]))


def test_match_value_supports_text_operators():
    assert match_value("Introduccion a la Programacion", {"icontains": "programacion"})
    assert match_value("Juan Perez", {"contains": "Perez"})
    assert match_value("Juan Perez", {"istartswith": "juan"})
    assert match_value("Juan Perez", {"iendswith": "PEREZ"})


def test_match_criteria_supports_logical_groups():
    payload = {"course.name": "Matematicas", "professor.name": "Ana Perez"}
    assert match_criteria(
        {
            "or": [
                {"course.name": {"icontains": "mate"}},
                {"professor.name": {"icontains": "juan"}},
            ]
        },
        lambda field: payload.get(field),
    )
    assert not match_criteria(
        {"and": [{"course.name": {"icontains": "mate"}}, {"professor.name": {"icontains": "juan"}}]},
        lambda field: payload.get(field),
    )
