from typing import Any, Protocol

from persistence_kit.security.registration import (
    LoginResult,
    LogoutResult,
    PasswordResetCodeRequestResult,
    PasswordResetConfirmResult,
    PasswordResetResult,
    RegisteredUserResult,
    RegisteredUsersPageResult,
    RefreshTokensResult,
    RegistrationResult,
    RoleAssignmentResult,
    UserStatusUpdateResult,
)
class IdentityProvider(Protocol):
    async def sign_up_user(
        self,
        *,
        email: str,
        password: str,
        given_name: str | None = None,
        family_name: str | None = None,
        created_by: str | None = None,
    ) -> RegistrationResult: ...

    async def assign_user_roles(
        self,
        *,
        username: str,
        roles: tuple[str, ...],
    ) -> RoleAssignmentResult: ...

    async def ensure_roles_exist(
        self,
        *,
        role_codes: tuple[str, ...],
    ) -> None: ...

    async def delete_roles(
        self,
        *,
        role_codes: tuple[str, ...],
    ) -> None: ...

    async def authenticate_user(
        self,
        *,
        email: str,
        password: str,
    ) -> LoginResult: ...

    async def set_user_enabled(
        self,
        *,
        username: str,
        enabled: bool,
    ) -> UserStatusUpdateResult: ...

    async def delete_user(
        self,
        *,
        username: str,
    ) -> None: ...

    async def reset_user_password(
        self,
        *,
        username: str,
    ) -> PasswordResetResult: ...

    async def refresh_tokens(
        self,
        *,
        refresh_token: str,
        login: str | None = None,
    ) -> RefreshTokensResult: ...

    async def logout(
        self,
        *,
        refresh_token: str | None = None,
        access_token: str | None = None,
        login: str | None = None,
    ) -> LogoutResult: ...

    async def request_password_reset_code(
        self,
        *,
        login: str,
    ) -> PasswordResetCodeRequestResult: ...

    async def confirm_password_reset(
        self,
        *,
        username: str,
        confirmation_code: str,
        new_password: str,
    ) -> PasswordResetConfirmResult: ...

    async def resolve_email_from_access_token(
        self,
        *,
        access_token: str,
    ) -> str | None: ...

    async def resolve_username_from_access_token(
        self,
        *,
        access_token: str,
    ) -> str | None: ...

    async def list_users(
        self,
        *,
        page: int,
        page_size: int,
        roles: tuple[str, ...] = (),
    ) -> RegisteredUsersPageResult: ...


class TokenVerifier(Protocol):
    def verify(self, token: str) -> dict[str, Any]: ...
