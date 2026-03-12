from typing import Sequence


def split_csv_list(value: str | Sequence[str] | None) -> list[str] | Sequence[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return [item.strip() for item in value.split(",")]
    return value
