from persistence_kit.authenticated_user import AuthenticatedUser
from persistence_kit.security.factory import (
    MEMORY_JWT_ISSUER,
    MEMORY_JWT_SECRET,
    MEMORY_JWT_TTL_SECONDS,
    get_identity_provider,
    get_token_verifier,
)
from persistence_kit.security.ports import IdentityProvider, TokenVerifier
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
    unique_roles,
    validate_allowed_email_domain,
)

_LAZY = {
    "CognitoIdentityProvider": (
        "persistence_kit.security.providers.cognito_identity_provider",
        "CognitoIdentityProvider",
    ),
    "MemorySecurityProvider": (
        "persistence_kit.security.providers.memory_security_provider",
        "MemorySecurityProvider",
    ),
    "CognitoJwtVerifier": (
        "persistence_kit.security.token_verifiers.cognito_jwt_verifier",
        "CognitoJwtVerifier",
    ),
    "MemoryJwtVerifier": (
        "persistence_kit.security.token_verifiers.memory_jwt_verifier",
        "MemoryJwtVerifier",
    ),
}

__all__ = [
    "AuthenticatedUser",
    "IdentityProvider",
    "TokenVerifier",
    "CognitoIdentityProvider",
    "MemorySecurityProvider",
    "CognitoJwtVerifier",
    "MemoryJwtVerifier",
    "MEMORY_JWT_SECRET",
    "MEMORY_JWT_ISSUER",
    "MEMORY_JWT_TTL_SECONDS",
    "get_identity_provider",
    "get_token_verifier",
    "RegistrationResult",
    "RoleAssignmentResult",
    "UserStatusUpdateResult",
    "LoginResult",
    "RefreshTokensResult",
    "LogoutResult",
    "PasswordResetCodeRequestResult",
    "PasswordResetConfirmResult",
    "PasswordResetResult",
    "RegisteredUserResult",
    "RegisteredUsersPageResult",
    "validate_allowed_email_domain",
    "unique_roles",
]


def __getattr__(name: str):
    if name in _LAZY:
        from importlib import import_module

        module_name, attr = _LAZY[name]
        value = getattr(import_module(module_name), attr)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
