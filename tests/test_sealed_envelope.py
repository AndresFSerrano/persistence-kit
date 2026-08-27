import base64
import time
import json
from typing import cast

import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from persistence_kit.api.exceptions import SealedPayloadError
from persistence_kit.security.sealed.envelope import (
    HYBRID_VERSION, MAX_ENVELOPE_AGE_SECONDS, NONCE_BYTES, VERSION, open_hybrid,
    open_sealed, seal
)
from persistence_kit.security.ports import KeyProvider


class FakeKeyProvider:
    def __init__(self, data_key: bytes) -> None:
        self.data_key = data_key
        self.wrapped = None

    async def unwrap_key(self, wrapped: bytes) -> bytes:
        self.wrapped = wrapped
        return self.data_key


@pytest.fixture
def key():
    return AESGCM.generate_key(256)


@pytest.fixture
def payload():
    return {"user" : "jua.giraldo", "password": "demo1234" }


@pytest.fixture
def envelope(key, payload):
    return seal(json.dumps(payload).encode(), key)


@pytest.fixture
def hybrid_envelope(envelope):
    return {
        **envelope,
        "v": HYBRID_VERSION,
        "key": base64.b64encode(b"llave-envuelta").decode()
    }


@pytest.fixture
def provider(key) -> KeyProvider:
    return cast(KeyProvider, FakeKeyProvider(key))


def test_seal_return_four_keys(envelope):

    assert set(envelope) == {"v", "nonce", "ciphertext", "ts"}
    assert envelope["v"] == VERSION
    assert len(base64.b64decode(envelope["nonce"])) == NONCE_BYTES
    assert abs(envelope["ts"] - time.time()) < 5


def test_seal_round_trip(key, payload, envelope):

    return_envelope = open_sealed(envelope, key)

    assert json.loads(return_envelope) == payload


def test_seal_does_not_repeat_envelopes(key, payload): 
    envelope_one = seal(
        json.dumps(payload).encode(),
        key
    )
    envelope_two = seal(
        json.dumps(payload).encode(),
        key
    )

    assert envelope_one["nonce"] != envelope_two["nonce"]
    assert envelope_one["ciphertext"] != envelope_two["ciphertext"]


def test_open_seal_reject_non_dict(key):
    with pytest.raises(SealedPayloadError, match = "Sobre mal formado"):
        open_sealed("no soy un dict", key)


@pytest.mark.parametrize(
        "field, message", 
        [("nonce", "Sobre mal formado"),
         ("v","Versión de sobre no soportada"),
         ("ciphertext", "Sobre mal formado"),
         ("ts", "Sobre mal formado")
        ]
)
def test_open_sealed_rejects_missing_field(envelope, key, field, message):
    del envelope[field]
    with pytest.raises(SealedPayloadError, match = message):
        open_sealed(envelope, key)


@pytest.mark.parametrize(
    "field, value",
    [("nonce", "no-es-base64!!"), ("ciphertext", "###"), ("ts", "no-soy-un-número")]
)
def test_open_sealed_rejects_unreadable_field(envelope, key, field, value):
    envelope[field] = value
    with pytest.raises(SealedPayloadError, match = "Sobre mal formado"):
        open_sealed(envelope, key)


def test_open_sealed_long_invalid_nonce(envelope, key):
    envelope["nonce"] = base64.b64encode(b"12345678").decode()
    with pytest.raises(SealedPayloadError, match = "Sobre mal formado"):
        open_sealed(envelope, key)

@pytest.mark.parametrize("offset", [-MAX_ENVELOPE_AGE_SECONDS -1, MAX_ENVELOPE_AGE_SECONDS +1])
def test_open_sealed_expired(envelope, key, offset):
    envelope["ts"] = int(time.time()) + offset
    with pytest.raises(SealedPayloadError, match = "El sobre esta vencido"):
        open_sealed(envelope, key)


def test_seal_and_open_with_different_key(envelope):
    with pytest.raises(SealedPayloadError, match = "El contenido fue alterado o la llave no corresponde"):
        open_sealed(envelope, AESGCM.generate_key(256))


def test_open_sealed_rejects_manipulated_envope(envelope, key):
    raw = bytearray(base64.b64decode(envelope["ciphertext"]))
    raw[0] ^= 1
    envelope["ciphertext"] = base64.b64encode(bytes(raw)).decode()
    with pytest.raises(SealedPayloadError, match = "El contenido fue alterado o la llave no corresponde"):
        open_sealed(envelope, key)


@pytest.mark.asyncio
async def test_open_hybrid_returns_plaintext_and_key(hybrid_envelope, key, payload, provider):

    plaintext, data_key = await open_hybrid(hybrid_envelope, provider)

    assert data_key == key
    assert json.loads(plaintext) == payload
    assert provider.wrapped == b"llave-envuelta"


@pytest.mark.asyncio
async def test_open_hybrid_bad_version(envelope, provider):
    with pytest.raises(SealedPayloadError, match = "Versión de sobre no soportada"):
        await open_hybrid(envelope, provider)
    assert provider.wrapped is None


@pytest.mark.asyncio
async def test_open_hybrid_reject_invalid_field(hybrid_envelope, provider):
    hybrid_envelope["key"] = "no-es-base64!!"
    with pytest.raises(SealedPayloadError, match = "No se pudo abrir la llave del sobre"):
        await open_hybrid(hybrid_envelope, provider)
    assert provider.wrapped is None