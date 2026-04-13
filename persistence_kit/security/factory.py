from collections.abc import Iterable
from functools import lru_cache

from fastapi import HTTPException, status

from persistence_kit.security.ports import IdentityProvider, TokenVerifier
from persistence_kit.settings import (
    DEFAULT_MEMORY_JWT_ISSUER,
    DEFAULT_MEMORY_JWT_SECRET,
    DEFAULT_MEMORY_JWT_TTL_SECONDS,
    AuthProvider,
    PersistenceKitSettings,
)

MEMORY_JWT_SECRET = DEFAULT_MEMORY_JWT_SECRET
MEMORY_JWT_ISSUER = DEFAULT_MEMORY_JWT_ISSUER
MEMORY_JWT_TTL_SECONDS = DEFAULT_MEMORY_JWT_TTL_SECONDS


def _normalized_seed_roles(seed_role_codes: Iterable[str]) -> tuple[str, ...]:
    return tuple(
        dict.fromkeys(role.strip().lower() for role in seed_role_codes if role.strip())
    )


@lru_cache
def _memory_security_provider(
    secret: str,
    issuer: str,
    ttl_seconds: int,
    seed_user_password: str,
    seed_role_codes: tuple[str, ...] = (),
    seed_user_domain: str = "example.com",
    seed_created_by: str | None = None,
) -> IdentityProvider:
    from persistence_kit.security.providers.memory_security_provider import MemorySecurityProvider

    return MemorySecurityProvider(
        jwt_secret=secret,
        jwt_issuer=issuer,
        token_ttl_seconds=ttl_seconds,
        seed_role_users=True,
        seed_user_password=seed_user_password,
        seed_role_codes=seed_role_codes,
        seed_user_domain=seed_user_domain,
        seed_created_by=seed_created_by,
    )


@lru_cache
def _memory_token_verifier(
    secret: str,
    issuer: str,
) -> TokenVerifier:
    from persistence_kit.security.token_verifiers.memory_jwt_verifier import MemoryJwtVerifier

    return MemoryJwtVerifier(secret=secret, issuer=issuer)


@lru_cache
def _identity_provider_cached(
    provider: str,
    cognito_region: str,
    cognito_user_pool_client_id: str | None,
    cognito_user_pool_client_secret: str | None,
    cognito_user_pool_id: str | None,
    memory_jwt_secret: str,
    memory_jwt_issuer: str,
    memory_jwt_ttl_seconds: int,
    memory_seed_user_password: str,
    memory_seed_role_codes: tuple[str, ...] = (),
    memory_seed_user_domain: str = "example.com",
    memory_seed_created_by: str | None = None,
) -> IdentityProvider:
    if provider == AuthProvider.MEMORY.value:
        return _memory_security_provider(
            memory_jwt_secret,
            memory_jwt_issuer,
            memory_jwt_ttl_seconds,
            memory_seed_user_password,
            memory_seed_role_codes,
            memory_seed_user_domain,
            memory_seed_created_by,
        )
    if provider == AuthProvider.COGNITO.value:
        from persistence_kit.security.providers.cognito_identity_provider import CognitoIdentityProvider

        return CognitoIdentityProvider(
            region=cognito_region,
            user_pool_client_id=cognito_user_pool_client_id,
            user_pool_client_secret=cognito_user_pool_client_secret,
            user_pool_id=cognito_user_pool_id,
            auto_verify_email=True,
        )
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Proveedor de identidad no soportado: '{provider}'.",
    )


def get_identity_provider(settings: PersistenceKitSettings) -> IdentityProvider:
    return _identity_provider_cached(
        settings.auth_provider.value,
        settings.aws_region,
        settings.cognito_app_client_id,
        settings.cognito_app_client_secret,
        settings.cognito_user_pool_id,
        settings.memory_jwt_secret,
        settings.memory_jwt_issuer,
        settings.memory_jwt_ttl_seconds,
        settings.memory_seed_user_password,
        _normalized_seed_roles(settings.memory_seed_role_codes),
        settings.memory_seed_user_domain,
        settings.memory_seed_created_by,
    )


@lru_cache
def _token_verifier_cached(
    provider: str,
    cognito_region: str,
    cognito_user_pool_id: str,
    cognito_client_id: str | None,
    memory_jwt_secret: str,
    memory_jwt_issuer: str,
) -> TokenVerifier:
    if provider == AuthProvider.MEMORY.value:
        return _memory_token_verifier(memory_jwt_secret, memory_jwt_issuer)
    if provider == AuthProvider.COGNITO.value:
        if not cognito_user_pool_id:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Falta configuracion COGNITO_USER_POOL_ID.",
            )
        from persistence_kit.security.token_verifiers.cognito_jwt_verifier import CognitoJwtVerifier

        return CognitoJwtVerifier(
            region=cognito_region,
            user_pool_id=cognito_user_pool_id,
            client_id=cognito_client_id,
        )
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Proveedor de autenticacion no soportado: '{provider}'.",
    )


def get_token_verifier(settings: PersistenceKitSettings) -> TokenVerifier:
    return _token_verifier_cached(
        settings.auth_provider.value,
        settings.aws_region,
        settings.cognito_user_pool_id or "",
        settings.cognito_app_client_id,
        settings.memory_jwt_secret,
        settings.memory_jwt_issuer,
    )
