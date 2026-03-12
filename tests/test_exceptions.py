from fastapi import HTTPException

from persistence_kit.api.exceptions import (
    BaseAPIException,
    BusinessRuleException,
    DatabaseException,
    NotFoundException,
    ValidationException,
)


def test_base_api_exception_inherits_http_exception():
    exc = BaseAPIException(status_code=418, detail="I'm a teapot")
    assert isinstance(exc, HTTPException)
    assert exc.status_code == 418
    assert exc.detail == "I'm a teapot"


def test_not_found_exception_defaults():
    exc = NotFoundException()
    assert exc.status_code == 404
    assert exc.detail == "Recurso no encontrado"


def test_not_found_exception_custom_detail():
    exc = NotFoundException("No existe")
    assert exc.status_code == 404
    assert exc.detail == "No existe"


def test_validation_exception_defaults():
    exc = ValidationException()
    assert exc.status_code == 400
    assert exc.detail == "Datos inválidos"


def test_validation_exception_custom_detail():
    exc = ValidationException("Faltan campos")
    assert exc.status_code == 400
    assert exc.detail == "Faltan campos"


def test_business_rule_exception_defaults():
    exc = BusinessRuleException()
    assert exc.status_code == 422
    assert exc.detail == "Violación de regla de negocio"


def test_business_rule_exception_custom_detail():
    exc = BusinessRuleException("Regla violada")
    assert exc.status_code == 422
    assert exc.detail == "Regla violada"


def test_database_exception_defaults():
    exc = DatabaseException()
    assert exc.status_code == 500
    assert exc.detail == "Error en la base de datos"


def test_database_exception_custom_detail():
    exc = DatabaseException("Falló conexión")
    assert exc.status_code == 500
    assert exc.detail == "Falló conexión"
