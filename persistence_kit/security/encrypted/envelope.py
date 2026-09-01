import base64
import os
import time
from typing import TYPE_CHECKING

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from persistence_kit.api.exceptions import SealedPayloadError

if TYPE_CHECKING:
    from persistence_kit.security.ports import KeyProvider

NONCE_BYTES = 12

VERSION = 1
HYBRID_VERSION = 3
MAX_ENVELOPE_AGE_SECONDS = 60

_MALFORMED = "Sobre mal formado"


def _encode(raw: bytes) -> str:
    return base64.b64encode(raw).decode("ascii")


def _decode(envelope: dict, field: str) -> bytes:
    return base64.b64decode(envelope[field], validate=True)


def seal(plaintext: bytes, key: bytes) -> dict:
    nonce = os.urandom(NONCE_BYTES)
    timestamp = int(time.time())
    ciphertext = AESGCM(key).encrypt(nonce, plaintext, str(timestamp).encode())
    return {"v": VERSION, "nonce": _encode(nonce), "ciphertext": _encode(ciphertext), "ts": timestamp }


def open_sealed(envelope: dict, key: bytes) -> bytes:
    if not isinstance(envelope, dict):
        raise SealedPayloadError(_MALFORMED)
    if envelope.get("v") != VERSION:
        raise SealedPayloadError("Versión de sobre no soportada")

    try:
        nonce = _decode(envelope, "nonce")
        ciphertext = _decode(envelope, "ciphertext")
        timestamp = int(envelope["ts"])
    except (KeyError, TypeError, ValueError) as exc:
        raise SealedPayloadError(_MALFORMED) from exc

    if len(nonce) != NONCE_BYTES:
        raise SealedPayloadError(_MALFORMED)

    if abs(timestamp - time.time()) > MAX_ENVELOPE_AGE_SECONDS:
        raise SealedPayloadError("El sobre esta vencido")

    try:
        return AESGCM(key).decrypt(nonce, ciphertext, str(timestamp).encode())
    except (InvalidTag, ValueError) as exc:
        raise SealedPayloadError("El contenido fue alterado o la llave no corresponde") from exc

async def open_hybrid(envelope: dict, provider: "KeyProvider") -> tuple[bytes, bytes]:
    if not isinstance(envelope, dict):
        raise SealedPayloadError(_MALFORMED)
    if envelope.get("v") != HYBRID_VERSION:
        raise SealedPayloadError("Versión de sobre no soportada")

    try:
        data_key = await provider.unwrap_key(_decode(envelope, "key"))
    except (KeyError, TypeError, ValueError) as exc:
        raise SealedPayloadError("No se pudo abrir la llave del sobre") from exc

    return open_sealed({**envelope, "v": VERSION}, data_key), data_key
