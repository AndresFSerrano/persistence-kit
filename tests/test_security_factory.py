import pytest
from fastapi import HTTPException

import persistence_kit.security.factory as mod
from persistence_kit.security.token_verifiers.memory_jwt_verifier import MemoryJwtVerifier
from persistence_kit.settings import (
    AuthProvider,
    PersistenceKitSettings,
    EncryptedType,
)


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
    assert len(captured) == 1
    kwargs = captured[0]
    cache = kwargs.pop("cache")
    from persistence_kit.cache.contracts import Cache

    assert isinstance(cache, Cache)
    assert kwargs == {
        "region": "us-east-1",
        "user_pool_client_id": "client-1",
        "user_pool_client_secret": "secret-1",
        "user_pool_id": "pool-1",
        "auto_verify_email": True,
        "list_users_cache_ttl_seconds": settings.cognito_list_users_cache_ttl_seconds,
        "list_users_cache_swr_seconds": settings.cognito_list_users_cache_swr_seconds,
        "list_users_concurrency": settings.cognito_list_users_concurrency,
    }


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
            5.0,
            25.0,
            20,
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


def test_get_key_provider_uses_kms_when_the_provider_says_so(monkeypatch):
    import persistence_kit.security.providers.encryption_provider as encryption_mod

    captured: list[str] = []

    class FakeKmsProvider:
        def __init__(self, key_id: str):
            captured.append(key_id)

    mod._key_provider_cached.cache_clear()
    monkeypatch.setattr(encryption_mod, "KmsKeyProvider", FakeKmsProvider)
    settings = PersistenceKitSettings(
        encrypted_type=EncryptedType.KMS, kms_key_id="key-1"
    )

    provider = mod.get_key_provider(settings)

    assert isinstance(provider, FakeKmsProvider)
    assert captured == ["key-1"]


def test_get_key_provider_uses_memory_by_default():
    from persistence_kit.security.providers.encryption_provider import MemoryKeyProvider

    mod._key_provider_cached.cache_clear()
    settings = PersistenceKitSettings()

    provider = mod.get_key_provider(settings)

    assert isinstance(provider, MemoryKeyProvider)


def test_get_key_provider_uses_local_when_the_type_says_so():
    from persistence_kit.security.providers.encryption_provider import LocalKeyProvider

    mod._key_provider_cached.cache_clear()
    settings = PersistenceKitSettings(
        encrypted_type=EncryptedType.LOCAL, encrypted_private_key="a-key-in-base64"
    )

    provider = mod.get_key_provider(settings)

    assert isinstance(provider, LocalKeyProvider)


def test_get_key_provider_rejects_local_without_a_private_key():
    mod._key_provider_cached.cache_clear()
    settings = PersistenceKitSettings(encrypted_type=EncryptedType.LOCAL)

    with pytest.raises(RuntimeError, match="ENCRYPTED_PRIVATE_KEY"):
        mod.get_key_provider(settings)


def test_get_key_provider_rejects_kms_without_a_key_id():
    mod._key_provider_cached.cache_clear()
    settings = PersistenceKitSettings(encrypted_type=EncryptedType.KMS)

    with pytest.raises(RuntimeError, match="KMS_KEY_ID"):
        mod.get_key_provider(settings)


def test_get_key_provider_ignores_a_kms_key_id_when_the_provider_is_local():
    from persistence_kit.security.providers.encryption_provider import LocalKeyProvider

    mod._key_provider_cached.cache_clear()
    settings = PersistenceKitSettings(
        encrypted_type=EncryptedType.LOCAL,
        encrypted_private_key="a-key-in-base64",
        kms_key_id="key-1",
    )

    assert isinstance(mod.get_key_provider(settings), LocalKeyProvider)


def test_get_key_provider_returns_cached_instance():
    mod._key_provider_cached.cache_clear()
    settings = PersistenceKitSettings(
        encrypted_type=EncryptedType.LOCAL, encrypted_private_key="a-key-in-base64"
    )

    assert mod.get_key_provider(settings) is mod.get_key_provider(settings)
