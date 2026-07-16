import json
import re
from typing import Any, Mapping, Sequence


def split_csv_list(value: str | Sequence[str] | None) -> list[str] | Sequence[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return [item.strip() for item in value.split(",")]
    return value


def parse_str_map(value: Any) -> dict[str, str]:
    if value is None or value == "":
        return {}
    if isinstance(value, Mapping):
        return {str(k): str(v) for k, v in value.items()}
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("{"):
            return {str(k): str(v) for k, v in json.loads(text).items()}
        result: dict[str, str] = {}
        for pair in re.split(r"[,;]", text):
            key, sep, val = pair.partition("=")
            if sep and key.strip():
                result[key.strip()] = val.strip()
        return result
    return dict(value)
