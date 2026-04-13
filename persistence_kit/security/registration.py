from dataclasses import dataclass

from persistence_kit.api.exceptions import ValidationException


@dataclass(frozen=True)
class RegistrationResult:
    username: str
    user_confirmed: bool
    message: str


@dataclass(frozen=True)
class RoleAssignmentResult:
    username: str
    roles: tuple[str, ...]
    message: str


@dataclass(frozen=True)
class UserStatusUpdateResult:
    username: str
    enabled: bool
    message: str


@dataclass(frozen=True)
class LoginResult:
    access_token: str
    id_token: str | None
    refresh_token: str | None
    expires_in: int | None
    token_type: str | None


@dataclass(frozen=True)
class RefreshTokensResult:
    access_token: str
    id_token: str | None
    refresh_token: str | None
    expires_in: int | None
    token_type: str | None


@dataclass(frozen=True)
class LogoutResult:
    message: str


@dataclass(frozen=True)
class PasswordResetCodeRequestResult:
    message: str


@dataclass(frozen=True)
class PasswordResetConfirmResult:
    message: str


@dataclass(frozen=True)
class PasswordResetResult:
    username: str
    message: str


@dataclass(frozen=True)
class RegisteredUserResult:
    username: str
    email: str | None
    given_name: str | None
    family_name: str | None
    created_by: str | None
    enabled: bool
    roles: tuple[str, ...]


@dataclass(frozen=True)
class RegisteredUsersPageResult:
    page: int
    page_size: int
    total: int
    users: tuple[RegisteredUserResult, ...]


def validate_allowed_email_domain(email: str, allowed_domain: str) -> None:
    parts = email.split("@", maxsplit=1)
    if len(parts) != 2 or parts[1].lower() != allowed_domain.lower():
        raise ValidationException(f"Solo se permite registro con correos @{allowed_domain}.")


def unique_roles(roles: list[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    deduped: list[str] = []
    for role in roles:
        normalized = role.strip().lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return tuple(deduped)
