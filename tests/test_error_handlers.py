import pytest

from persistence_kit.api.error_handlers import (
    handle_repository_errors,
    handle_service_errors,
)
from persistence_kit.api.exceptions import (
    BusinessRuleException,
    DatabaseException,
    NotFoundException,
    ValidationException,
)


@pytest.mark.asyncio
async def test_service_handler_preserves_not_found(caplog):
    @handle_service_errors
    async def fn():
        raise NotFoundException("missing")

    with pytest.raises(NotFoundException) as exc:
        await fn()
    assert exc.value.detail == "missing"
    assert "Recurso no encontrado" in caplog.text


@pytest.mark.asyncio
async def test_service_handler_preserves_validation(caplog):
    @handle_service_errors
    async def fn():
        raise ValidationException("bad")

    with pytest.raises(ValidationException) as exc:
        await fn()
    assert exc.value.detail == "bad"
    assert "Error de validación" in caplog.text


@pytest.mark.asyncio
async def test_service_handler_preserves_business_rule(caplog):
    @handle_service_errors
    async def fn():
        raise BusinessRuleException("rule")

    with pytest.raises(BusinessRuleException) as exc:
        await fn()
    assert exc.value.detail == "rule"
    assert "Regla de negocio" in caplog.text


@pytest.mark.asyncio
async def test_service_handler_preserves_database(caplog):
    @handle_service_errors
    async def fn():
        raise DatabaseException("db")

    with pytest.raises(DatabaseException) as exc:
        await fn()
    assert exc.value.detail == "db"
    assert "Error de base de datos" in caplog.text


@pytest.mark.asyncio
async def test_service_handler_transforms_lookup_error(caplog):
    @handle_service_errors
    async def fn():
        raise LookupError("missing-resource")

    with pytest.raises(NotFoundException) as exc:
        await fn()
    assert exc.value.detail == "missing-resource"
    assert "Recurso no encontrado" in caplog.text


@pytest.mark.asyncio
async def test_service_handler_transforms_value_error(caplog):
    @handle_service_errors
    async def fn():
        raise ValueError("invalid")

    with pytest.raises(ValidationException) as exc:
        await fn()
    assert exc.value.detail == "invalid"
    assert "Error de validación" in caplog.text


@pytest.mark.asyncio
async def test_service_handler_transforms_unexpected_exception(caplog):
    @handle_service_errors
    async def fn():
        raise RuntimeError("boom")

    with pytest.raises(DatabaseException) as exc:
        await fn()
    assert exc.value.detail == "Error interno del servidor"
    assert "Error inesperado" in caplog.text


@pytest.mark.asyncio
async def test_repository_handler_wraps_any_exception(caplog):
    @handle_repository_errors
    async def fn():
        raise RuntimeError("crash")

    with pytest.raises(DatabaseException) as exc:
        await fn()
    assert "Error en repositorio" in caplog.text
    assert exc.value.detail == "Error de base de datos: crash"
