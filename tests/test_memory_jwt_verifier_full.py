import jwt
import pytest
from fastapi import HTTPException

from persistence_kit.security.token_verifiers.memory_jwt_verifier import MemoryJwtVerifier


def _build_verifier() -> MemoryJwtVerifier:
    return MemoryJwtVerifier(
        secret="test-secret-with-32-characters-minimum",
        issuer="memory-sandbox",
    )


def test_verify_rejects_invalid_token_use():
    verifier = _build_verifier()
    token = jwt.encode(
        {
            "sub": "user-1",
            "iss": "memory-sandbox",
            "token_use": "refresh",
            "iat": 1,
            "exp": 9999999999,
        },
        "test-secret-with-32-characters-minimum",
        algorithm="HS256",
    )

    with pytest.raises(HTTPException) as err:
        verifier.verify(token)

    assert err.value.status_code == 401
    assert "token_use" in err.value.detail


def test_verify_re_raises_http_exception():
    verifier = _build_verifier()
    token = jwt.encode(
        {
            "sub": "user-1",
            "iss": "memory-sandbox",
            "token_use": "refresh",
            "iat": 1,
            "exp": 9999999999,
        },
        "test-secret-with-32-characters-minimum",
        algorithm="HS256",
    )

    with pytest.raises(HTTPException) as err:
        verifier.verify(token)

    assert err.value.status_code == 401
