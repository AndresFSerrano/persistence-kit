from enum import Enum

import pytest

from persistence_kit.security.registration import unique_roles, validate_allowed_email_domain
from persistence_kit import ValidationException


class Role(str, Enum):
    ADMIN_GENERAL = "admin_general"
    PROFESSOR_INSUMOS = "professor_insumos"


def test_validate_allowed_email_domain_accepts_udea():
    validate_allowed_email_domain("profesor@udea.edu.co", "udea.edu.co")


def test_validate_allowed_email_domain_rejects_non_udea():
    with pytest.raises(ValidationException) as err:
        validate_allowed_email_domain("profesor@gmail.com", "udea.edu.co")

    assert "@udea.edu.co" in str(err.value)


def test_unique_roles_removes_duplicates_and_preserves_order():
    result = unique_roles(
        [
            Role.ADMIN_GENERAL,
            Role.ADMIN_GENERAL,
            Role.PROFESSOR_INSUMOS,
            Role.ADMIN_GENERAL,
        ]
    )
    assert result == (Role.ADMIN_GENERAL, Role.PROFESSOR_INSUMOS)


def test_validate_allowed_email_domain_rejects_missing_domain_separator():
    with pytest.raises(ValidationException):
        validate_allowed_email_domain("profesor-udea.edu.co", "udea.edu.co")


def test_unique_roles_normalizes_blanks_and_uppercase():
    result = unique_roles([" ADMIN_GENERAL ", "", "  ", "Professor_Insumos", "admin_general"])

    assert result == ("admin_general", "professor_insumos")
