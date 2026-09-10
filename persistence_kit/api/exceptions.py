from typing import Any, Optional

from fastapi import HTTPException


class BaseAPIException(HTTPException):
    def __init__(
        self,
        status_code: int,
        detail: Any = None,
        headers: Optional[dict] = None,
    ) -> None:
        super().__init__(status_code=status_code, detail=detail, headers=headers)


class NotFoundException(BaseAPIException):
    def __init__(self, detail: str = "Recurso no encontrado"):
        super().__init__(status_code=404, detail=detail)


class ValidationException(BaseAPIException):
    def __init__(self, detail: str = "Datos inválidos"):
        super().__init__(status_code=400, detail=detail)


class BusinessRuleException(BaseAPIException):
    def __init__(self, detail: str = "Violación de regla de negocio"):
        super().__init__(status_code=422, detail=detail)


class DatabaseException(BaseAPIException):
    def __init__(self, detail: str = "Error en la base de datos"):
        super().__init__(status_code=500, detail=detail)
