import pytest
from fastapi import HTTPException

import persistence_kit.security.factory as mod
from persistence_kit.security.token_verifiers.memory_jwt_verifier import MemoryJwtVerifier
from persistence_kit.settings import AuthProvider, PersistenceKitSettings


def test_get_identity_provider_returns_cached_instance(monkeypatch):
    import persistence_kit.security.providers.cognito_identity_provider as cognito_mod

    captured: list[dict] = []

    class FakeProvider:
        def __init__(self, **kwargs):
            captured.append(kwargs)

    mod._identity_provider_cached.cache_clear()
    monkeypatch.setattr(cognito_mod, "CognitoIdentityProvider", FakeProvider)
    settings = PersistenceKitSettings(
        auth_provider=AuthProvider.COGNITO,
        aws_region="us-east-1",
        cognito_user_pool_id="pool-1",
        cognito_app_client_id="client-1",
        cognito_app_client_secret="secret-1",
    )

    first = mod.get_identity_provider(settings)
    second = mod.get_identity_provider(settings)

    assert first is second
    assert captured == [
        {
            "region": "us-east-1",
            "user_pool_client_id": "client-1",
            "user_pool_client_secret": "secret-1",
            "user_pool_id": "pool-1",
            "auto_verify_email": True,
        }
    ]


def test_get_identity_provider_uses_configured_memory_seed_roles():
    mod._identity_provider_cached.cache_clear()
    mod._memory_security_provider.cache_clear()
    settings = PersistenceKitSettings(
        auth_provider=AuthProvider.MEMORY,
        memory_seed_role_codes=("admin", "operator"),
        memory_seed_user_domain="example.org",
        memory_seed_created_by="system.seed",
    )

    provider = mod.get_identity_provider(settings)

    assert provider is mod._memory_security_provider(
        settings.memory_jwt_secret,
        settings.memory_jwt_issuer,
        settings.memory_jwt_ttl_seconds,
        settings.memory_seed_user_password,
        ("admin", "operator"),
        "example.org",
        "system.seed",
    )


def test_get_token_verifier_uses_memory_by_default():
    mod._token_verifier_cached.cache_clear()
    mod._memory_token_verifier.cache_clear()
    settings = PersistenceKitSettings(auth_provider=AuthProvider.MEMORY)

    verifier = mod.get_token_verifier(settings)

    assert isinstance(verifier, MemoryJwtVerifier)
    assert verifier is mod._memory_token_verifier(
        settings.memory_jwt_secret,
        settings.memory_jwt_issuer,
    )


def test_get_token_verifier_requires_pool_id():
    settings = PersistenceKitSettings(
        auth_provider=AuthProvider.COGNITO,
        auth_enabled=True,
        cognito_user_pool_id=None,
    )

    with pytest.raises(HTTPException) as err:
        mod.get_token_verifier(settings)

    assert err.value.status_code == 500
    assert "COGNITO_USER_POOL_ID" in err.value.detail


def test_identity_provider_cached_rejects_unsupported_provider():
    mod._identity_provider_cached.cache_clear()

    with pytest.raises(HTTPException) as err:
        mod._identity_provider_cached(
            "unsupported",
            "us-east-1",
            None,
            None,
            None,
            mod.MEMORY_JWT_SECRET,
            mod.MEMORY_JWT_ISSUER,
            mod.MEMORY_JWT_TTL_SECONDS,
            "seed-password",
        )

    assert err.value.status_code == 500
    assert "no soportado" in err.value.detail.lower()


def test_token_verifier_cached_rejects_unsupported_provider():
    mod._token_verifier_cached.cache_clear()

    with pytest.raises(HTTPException) as err:
        mod._token_verifier_cached(
            "unsupported",
            "us-east-1",
            "pool-1",
            "client-1",
            mod.MEMORY_JWT_SECRET,
            mod.MEMORY_JWT_ISSUER,
        )

    assert err.value.status_code == 500
    assert "no soportado" in err.value.detail.lower()
