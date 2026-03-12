from typing import Tuple

from fastapi import Query
from pydantic import BaseModel, Field


class ApiError(BaseModel):
    detail: str = Field(..., examples=["Not found"])


def pagination_params(
    offset: int = Query(0, ge=0, description="Número de elementos a omitir (paginación)."),
    limit: int = Query(50, ge=1, le=200, description="Cantidad máxima de elementos a devolver."),
) -> Tuple[int, int]:
    return offset, limit
