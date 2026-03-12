import logging
from functools import wraps
from typing import Any, Callable

from persistence_kit.api.exceptions import (
    BusinessRuleException,
    DatabaseException,
    NotFoundException,
    ValidationException,
)

logger = logging.getLogger(__name__)


def handle_service_errors(func: Callable) -> Callable:
    @wraps(func)
    async def wrapper(*args, **kwargs) -> Any:
        try:
            return await func(*args, **kwargs)
        except NotFoundException as exc:
            logger.warning("Recurso no encontrado: %s", exc.detail)
            raise exc
        except ValidationException as exc:
            logger.warning("Error de validación: %s", exc.detail)
            raise exc
        except BusinessRuleException as exc:
            logger.warning("Regla de negocio: %s", exc.detail)
            raise exc
        except DatabaseException as exc:
            logger.error("Error de base de datos: %s", exc.detail, exc_info=True)
            raise exc
        except LookupError as exc:
            logger.warning("Recurso no encontrado: %s", exc)
            raise NotFoundException(detail=str(exc))
        except ValueError as exc:
            logger.warning("Error de validación: %s", exc)
            raise ValidationException(detail=str(exc))
        except Exception as exc:
            logger.error("Error inesperado: %s", exc, exc_info=True)
            raise DatabaseException(detail="Error interno del servidor")

    return wrapper


def handle_repository_errors(func: Callable) -> Callable:
    @wraps(func)
    async def wrapper(*args, **kwargs) -> Any:
        try:
            return await func(*args, **kwargs)
        except Exception as exc:
            logger.error("Error en repositorio: %s", exc, exc_info=True)
            raise DatabaseException(detail=f"Error de base de datos: {exc}")

    return wrapper
