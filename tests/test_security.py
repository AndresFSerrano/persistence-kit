import sys
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from persistence_kit import ValidationException
from persistence_kit.security import (
    CognitoIdentityProvider,
    CognitoJwtVerifier,
    MemoryJwtVerifier,
    MemorySecurityProvider,
    unique_roles,
    validate_allowed_email_domain,
)
import persistence_kit.security.providers.cognito_identity_provider as cognito_mod


def test_security_helpers_validate_email_domain_and_roles():
    validate_allowed_email_domain("User@Example.org", "example.org")

    with pytest.raises(ValidationException):
        validate_allowed_email_domain("user@other.org", "example.org")

    assert unique_roles(["Admin", "admin", " user "]) == ("admin", "user")


@pytest.mark.asyncio
async def test_memory_security_provider_authenticates_and_verifies_tokens():
    provider = MemorySecurityProvider(
        jwt_secret="test-secret-with-32-characters-minimum",
        jwt_issuer="memory-sandbox",
        token_ttl_seconds=3600,
    )
    verifier = MemoryJwtVerifier(
        secret="test-secret-with-32-characters-minimum",
        issuer="memory-sandbox",
    )

    created = await provider.sign_up_user(email="Admin@Example.org", password="Password123!")
    await provider.assign_user_roles(username=created.username, roles=("admin",))
    await provider.set_user_enabled(username=created.username, enabled=True)

    login = await provider.authenticate_user(email="admin@example.org", password="Password123!")
    payload = verifier.verify(login.access_token)

    assert payload["sub"] == "admin"
    assert payload["roles"] == ["admin"]


@pytest.mark.asyncio
async def test_memory_security_provider_seeds_configured_role_users():
    provider = MemorySecurityProvider(
        jwt_secret="test-secret-with-32-characters-minimum",
        jwt_issuer="memory-sandbox",
        seed_role_users=True,
        seed_user_password="Temporal123!",
        seed_role_codes=("admin", "operator"),
        seed_user_domain="example.org",
        seed_created_by="system.seed",
    )

    page = await provider.list_users(page=1, page_size=10)

    assert page.total == 2
    assert {user.username for user in page.users} == {"admin.seed", "operator.seed"}
    assert {user.email for user in page.users} == {
        "admin.seed@example.org",
        "operator.seed@example.org",
    }
    assert {user.created_by for user in page.users} == {"system.seed"}


def test_cognito_identity_provider_fails_when_boto3_is_missing(monkeypatch):
    monkeypatch.setattr(cognito_mod, "boto3", None)

    with pytest.raises(HTTPException) as exc_info:
        CognitoIdentityProvider(region="us-east-1")

    assert exc_info.value.status_code == 500


@pytest.mark.asyncio
async def test_cognito_identity_provider_signs_up_user_with_fake_client(monkeypatch):
    captured = {}

    class FakeClient:
        def admin_create_user(self, **kwargs):
            captured["create"] = kwargs
            return {}

        def admin_set_user_password(self, **kwargs):
            captured["password"] = kwargs
            return {}

        def admin_disable_user(self, **kwargs):
            captured["disable"] = kwargs
            return {}

    fake_client = FakeClient()
    monkeypatch.setattr(
        cognito_mod,
        "boto3",
        SimpleNamespace(client=lambda service_name, region_name: fake_client),
    )
    provider = CognitoIdentityProvider(region="us-east-1", user_pool_id="pool-1")

    result = await provider.sign_up_user(email="User@Example.org", password="Password123!")

    assert result.username == "user"
    assert captured["create"]["UserPoolId"] == "pool-1"
    assert captured["create"]["Username"] == "user"
    assert captured["password"]["Password"] == "Password123!"
    assert captured["disable"]["Username"] == "user"


def test_cognito_jwt_verifier_returns_payload_with_fake_jwt(monkeypatch):
    captured = {}

    class FakeSigningKey:
        key = "public-key"

    class FakePyJWKClient:
        def __init__(self, url):
            captured["url"] = url

        def get_signing_key_from_jwt(self, token):
            captured["token"] = token
            return FakeSigningKey()

    def fake_decode(token, key, algorithms, issuer, audience, options):
        captured["decode"] = {
            "token": token,
            "key": key,
            "algorithms": algorithms,
            "issuer": issuer,
            "audience": audience,
            "options": options,
        }
        return {"sub": "user", "token_use": "access", "client_id": "client-1"}

    monkeypatch.setitem(
        sys.modules,
        "jwt",
        SimpleNamespace(PyJWKClient=FakePyJWKClient, decode=fake_decode),
    )

    verifier = CognitoJwtVerifier(
        region="us-east-1",
        user_pool_id="pool-1",
        client_id="client-1",
    )

    assert verifier.verify("token")["sub"] == "user"
    assert captured["url"].endswith("/pool-1/.well-known/jwks.json")
    assert captured["decode"]["options"] == {"verify_aud": False}
