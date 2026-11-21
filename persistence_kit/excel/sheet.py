from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime

CellValue = str | int | float | bool | date | datetime | None


@dataclass(frozen=True, slots=True)
class ExcelSheet:
    name: str
    headers: Sequence[str]
    rows: Sequence[Sequence[CellValue]]
