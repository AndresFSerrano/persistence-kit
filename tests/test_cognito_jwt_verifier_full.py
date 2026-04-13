import sys
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from persistence_kit.security.token_verifiers.cognito_jwt_verifier import CognitoJwtVerifier


def _install_fake_jwt(monkeypatch, *, payload=None, decode_exc=None, signing_exc=None):
    captured = {}

    class FakeSigningKey:
        key = "public-key"

    class FakePyJWKClient:
        def __init__(self, url):
            captured["jwks_url"] = url

        def get_signing_key_from_jwt(self, token):
            captured["token"] = token
            if signing_exc:
                raise signing_exc
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
        if decode_exc:
            raise decode_exc
        return payload

    fake_jwt_module = SimpleNamespace(
        PyJWKClient=FakePyJWKClient,
        decode=fake_decode,
    )
    monkeypatch.setitem(sys.modules, "jwt", fake_jwt_module)
    return captured


def test_verify_returns_payload_when_token_is_valid(monkeypatch):
    captured = _install_fake_jwt(
        monkeypatch,
        payload={"sub": "123", "token_use": "access", "client_id": "client-1"},
    )

    verifier = CognitoJwtVerifier(
        region="us-east-1",
        user_pool_id="pool-1",
        client_id="client-1",
    )
    result = verifier.verify("token-abc")

    assert result["sub"] == "123"
    assert captured["jwks_url"].endswith("/pool-1/.well-known/jwks.json")
    assert captured["decode"]["audience"] is None
    assert captured["decode"]["options"] == {"verify_aud": False}


def test_verify_disables_audience_validation_without_client_id(monkeypatch):
    captured = _install_fake_jwt(
        monkeypatch,
        payload={"sub": "123", "token_use": "id"},
    )

    verifier = CognitoJwtVerifier(region="us-east-1", user_pool_id="pool-1")
    verifier.verify("token-abc")

    assert captured["decode"]["audience"] is None
    assert captured["decode"]["options"] == {"verify_aud": False}


def test_verify_rejects_access_token_with_wrong_client_id(monkeypatch):
    _install_fake_jwt(
        monkeypatch,
        payload={"sub": "123", "token_use": "access", "client_id": "other-client"},
    )
    verifier = CognitoJwtVerifier(
        region="us-east-1",
        user_pool_id="pool-1",
        client_id="client-1",
    )

    with pytest.raises(HTTPException) as exc_info:
        verifier.verify("token-abc")

    assert exc_info.value.status_code == 401
    assert "client_id inválido" in exc_info.value.detail


def test_verify_rejects_id_token_with_wrong_aud(monkeypatch):
    _install_fake_jwt(
        monkeypatch,
        payload={"sub": "123", "token_use": "id", "aud": "other-client"},
    )
    verifier = CognitoJwtVerifier(
        region="us-east-1",
        user_pool_id="pool-1",
        client_id="client-1",
    )

    with pytest.raises(HTTPException) as exc_info:
        verifier.verify("token-abc")

    assert exc_info.value.status_code == 401
    assert "aud inválido" in exc_info.value.detail


def test_verify_rejects_invalid_token_use(monkeypatch):
    _install_fake_jwt(
        monkeypatch,
        payload={"sub": "123", "token_use": "refresh"},
    )
    verifier = CognitoJwtVerifier(region="us-east-1", user_pool_id="pool-1")

    with pytest.raises(HTTPException) as exc_info:
        verifier.verify("token-abc")

    assert exc_info.value.status_code == 401
    assert "token_use inválido" in exc_info.value.detail


def test_verify_raises_unauthorized_for_decode_errors(monkeypatch):
    _install_fake_jwt(
        monkeypatch,
        decode_exc=ValueError("invalid signature"),
    )
    verifier = CognitoJwtVerifier(region="us-east-1", user_pool_id="pool-1")

    with pytest.raises(HTTPException) as exc_info:
        verifier.verify("token-abc")

    assert exc_info.value.status_code == 401
    assert "Token inválido o expirado" in exc_info.value.detail
