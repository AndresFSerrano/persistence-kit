from persistence_kit.settings.parsers import split_csv_list


def test_split_csv_list_none():
    assert split_csv_list(None) is None


def test_split_csv_list_string():
    assert split_csv_list("a, b ,c") == ["a", "b", "c"]


def test_split_csv_list_sequence_passthrough():
    value = ["a", "b"]
    assert split_csv_list(value) is value
