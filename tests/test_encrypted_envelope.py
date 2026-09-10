import base64
import time
import json
from typing import cast

import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from persistence_kit.security.encrypted.errors import EncryptedPayloadError
from persistence_kit.security.encrypted.envelope import (
    HYBRID_VERSION, MAX_ENVELOPE_AGE_SECONDS, NONCE_BYTES, VERSION, decrypt_hybrid,
    decrypt, encrypt
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
    return encrypt(json.dumps(payload).encode(), key)


@pytest.fixture
def hybrid_envelope(envelope):
    return {
        **envelope,
        "v": HYBRID_VERSION,
        "key": base64.b64encode(b"wrapped-key").decode()
    }


@pytest.fixture
def provider(key) -> KeyProvider:
    return cast(KeyProvider, FakeKeyProvider(key))


def test_encrypt_returns_the_four_envelope_fields(envelope):

    assert set(envelope) == {"v", "nonce", "ciphertext", "ts"}
    assert envelope["v"] == VERSION
    assert len(base64.b64decode(envelope["nonce"])) == NONCE_BYTES
    assert abs(envelope["ts"] - time.time()) < 5


def test_encrypt_and_decrypt_round_trip(key, payload, envelope):

    return_envelope = decrypt(envelope, key)

    assert json.loads(return_envelope) == payload


def test_encrypt_never_repeats_a_nonce(key, payload): 
    envelope_one = encrypt(
        json.dumps(payload).encode(),
        key
    )
    envelope_two = encrypt(
        json.dumps(payload).encode(),
        key
    )

    assert envelope_one["nonce"] != envelope_two["nonce"]
    assert envelope_one["ciphertext"] != envelope_two["ciphertext"]


def test_decrypt_rejects_something_that_is_not_a_dict(key):
    with pytest.raises(EncryptedPayloadError, match = "Sobre mal formado"):
        decrypt("not a dict", key)


def test_decrypt_rejects_a_hybrid_envelope(hybrid_envelope, key):
    with pytest.raises(EncryptedPayloadError, match = "Versión de sobre no soportada"):
        decrypt(hybrid_envelope, key)


@pytest.mark.parametrize(
        "field, message", 
        [("nonce", "Sobre mal formado"),
         ("v","Versión de sobre no soportada"),
         ("ciphertext", "Sobre mal formado"),
         ("ts", "Sobre mal formado")
        ]
)
def test_decrypt_rejects_a_missing_field(envelope, key, field, message):
    del envelope[field]
    with pytest.raises(EncryptedPayloadError, match = message):
        decrypt(envelope, key)


@pytest.mark.parametrize(
    "field, value",
    [("nonce", "not-base64!!"), ("ciphertext", "###"), ("ts", "not-a-number")]
)
def test_decrypt_rejects_an_unreadable_field(envelope, key, field, value):
    envelope[field] = value
    with pytest.raises(EncryptedPayloadError, match = "Sobre mal formado"):
        decrypt(envelope, key)


def test_decrypt_rejects_a_nonce_of_the_wrong_length(envelope, key):
    envelope["nonce"] = base64.b64encode(b"12345678").decode()
    with pytest.raises(EncryptedPayloadError, match = "Sobre mal formado"):
        decrypt(envelope, key)

@pytest.mark.parametrize("offset", [-MAX_ENVELOPE_AGE_SECONDS -1, MAX_ENVELOPE_AGE_SECONDS +1])
def test_decrypt_rejects_an_expired_envelope(envelope, key, offset):
    envelope["ts"] = int(time.time()) + offset
    with pytest.raises(EncryptedPayloadError, match = "El sobre esta vencido"):
        decrypt(envelope, key)


def test_decrypt_rejects_a_key_that_does_not_match(envelope):
    with pytest.raises(EncryptedPayloadError, match = "El contenido fue alterado o la llave no corresponde"):
        decrypt(envelope, AESGCM.generate_key(256))


def test_decrypt_rejects_a_tampered_ciphertext(envelope, key):
    raw = bytearray(base64.b64decode(envelope["ciphertext"]))
    raw[0] ^= 1
    envelope["ciphertext"] = base64.b64encode(bytes(raw)).decode()
    with pytest.raises(EncryptedPayloadError, match = "El contenido fue alterado o la llave no corresponde"):
        decrypt(envelope, key)


@pytest.mark.asyncio
async def test_decrypt_hybrid_returns_the_plaintext_and_the_key(hybrid_envelope, key, payload, provider):

    plaintext, data_key = await decrypt_hybrid(hybrid_envelope, provider)

    assert data_key == key
    assert json.loads(plaintext) == payload
    assert provider.wrapped == b"wrapped-key"


@pytest.mark.asyncio
async def test_decrypt_hybrid_rejects_a_symmetric_envelope(envelope, provider):
    with pytest.raises(EncryptedPayloadError, match = "Versión de sobre no soportada"):
        await decrypt_hybrid(envelope, provider)
    assert provider.wrapped is None


@pytest.mark.asyncio
async def test_decrypt_hybrid_rejects_an_unreadable_key(hybrid_envelope, provider):
    hybrid_envelope["key"] = "not-base64!!"
    with pytest.raises(EncryptedPayloadError, match = "No se pudo abrir la llave del sobre"):
        await decrypt_hybrid(hybrid_envelope, provider)
    assert provider.wrapped is None


@pytest.mark.asyncio
async def test_decrypt_hybrid_does_not_touch_the_envelope(hybrid_envelope, provider):
    original = dict(hybrid_envelope)

    await decrypt_hybrid(hybrid_envelope, provider)

    assert hybrid_envelope == original
