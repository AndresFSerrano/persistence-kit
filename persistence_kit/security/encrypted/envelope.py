import base64
import os
import time
from typing import TYPE_CHECKING

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from persistence_kit.security.encrypted.errors import EncryptedPayloadError

if TYPE_CHECKING:
    from persistence_kit.security.ports import KeyProvider

NONCE_BYTES = 12

VERSION = 1
HYBRID_VERSION = 2
MAX_ENVELOPE_AGE_SECONDS = 60

_MALFORMED = "Sobre mal formado"


def _encode(raw: bytes) -> str:
    return base64.b64encode(raw).decode("ascii")


def _decode(envelope: dict, field: str) -> bytes:
    return base64.b64decode(envelope[field], validate=True)

def _open(envelope: dict, key: bytes) -> bytes:
    try:
        nonce = _decode(envelope, "nonce")
        ciphertext = _decode(envelope, "ciphertext")
        timestamp = int(envelope["ts"])
    except (KeyError, TypeError, ValueError) as exc:
        raise EncryptedPayloadError(_MALFORMED) from exc

    if len(nonce) != NONCE_BYTES:
        raise EncryptedPayloadError(_MALFORMED)

    if abs(timestamp - time.time()) > MAX_ENVELOPE_AGE_SECONDS:
        raise EncryptedPayloadError("El sobre esta vencido")

    try:
        return AESGCM(key).decrypt(nonce, ciphertext, str(timestamp).encode())
    except (InvalidTag, ValueError) as exc:
        raise EncryptedPayloadError("El contenido fue alterado o la llave no corresponde") from exc


def encrypt(plaintext: bytes, key: bytes) -> dict:
    nonce = os.urandom(NONCE_BYTES)
    timestamp = int(time.time())
    ciphertext = AESGCM(key).encrypt(nonce, plaintext, str(timestamp).encode())

    return {"v": VERSION, "nonce": _encode(nonce), "ciphertext": _encode(ciphertext), "ts": timestamp }


def decrypt(envelope: dict, key: bytes) -> bytes:
    if not isinstance(envelope, dict):
        raise EncryptedPayloadError(_MALFORMED)

    if envelope.get("v") != VERSION:
        raise EncryptedPayloadError("Versión de sobre no soportada")

    return _open(envelope, key)

async def decrypt_hybrid(envelope: dict, provider: "KeyProvider") -> tuple[bytes, bytes]:
    if not isinstance(envelope, dict):
        raise EncryptedPayloadError(_MALFORMED)

    if envelope.get("v") != HYBRID_VERSION:
        raise EncryptedPayloadError("Versión de sobre no soportada")

    try:
        data_key = await provider.unwrap_key(_decode(envelope, "key"))
    except (KeyError, TypeError, ValueError) as exc:
        raise EncryptedPayloadError("No se pudo abrir la llave del sobre") from exc

    return _open(envelope, data_key), data_key
