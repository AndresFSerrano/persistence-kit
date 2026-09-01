import base64
import json
from collections.abc import Callable
from typing import Annotated

from fastapi import Header, Request, Response, status
from fastapi.exceptions import HTTPException, RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from fastapi.encoders import jsonable_encoder

from persistence_kit.cache import CacheBackend, CacheSettings, get_cache
from persistence_kit.settings import PersistenceKitSettings
from persistence_kit.security.encrypted.errors import EncryptedPayloadError
from persistence_kit.security.encrypted.envelope import (
    HYBRID_VERSION,
    MAX_ENVELOPE_AGE_SECONDS,
    decrypt_hybrid,
    decrypt,
    encrypt,
)

ENCRYPTED_FLAG = "x-encrypted"
ENCRYPTED_FIELDS = "x-encrypted-fields"
ENCRYPTED_RESPONSE_FIELDS = "x-encrypted-response-fields"
KEY_HEADER = "x-encrypted-key"

KEY_STATE = "encrypted_key"
RESPONSE_FIELDS_STATE = "encrypted_response_fields"

_NOT_JSON = "El cuerpo debe ser un sobre cifrado en JSON."

_ENVELOPE_FIELDS = frozenset({"v", "key", "nonce", "ciphertext", "ts"})

_ENVELOPE_SCHEMA = {
    "type": "object",
    "required": ["v", "key", "nonce", "ciphertext", "ts"],
    "properties": {
        "v": {
            "type": "integer",
            "example": HYBRID_VERSION,
            "description": "Versión del sobre.",
        },
        "key": {
            "type": "string",
            "description": (
                "La llave AES de esta petición, envuelta con la llave pública del "
                "servidor (RSA-OAEP), en base64. La pública se obtiene en "
                "GET /auth/public-key."
            ),
        },
        "nonce": {
            "type": "string",
            "description": "Número irrepetible del cifrado, en base64.",
            "example": "WHLLIjLGwuGUTwb+",
        },
        "ciphertext": {
            "type": "string",
            "description": "El body original cifrado con AES-256-GCM, en base64.",
        },
        "ts": {
            "type": "integer",
            "example": 1786649445,
            "description": (
                f"Momento en que se cifró, en segundos desde 1970. El servidor "
                f"rechaza los sobres con más de {MAX_ENVELOPE_AGE_SECONDS} segundos."
            ),
        },
    },
}

def encrypted(
        with_body: bool = True,
        fields: list[str] | None = None,
        response_fields: list[str] | None = None
) -> dict:
    marca: dict[str, object] = {ENCRYPTED_FLAG: True}
    if fields:
        marca[ENCRYPTED_FIELDS] = fields
    if response_fields:
        marca[ENCRYPTED_RESPONSE_FIELDS] = response_fields
    if with_body and not fields:
        marca["requestBody"] = {
            "required": True,
            "content": {"application/json": {"schema": _ENVELOPE_SCHEMA}},
        }
    return marca


async def declare_key_header(
    x_encrypted_key: Annotated[
        str | None,
        Header(
            description=(
                "Llave AES de 32 bytes envuelta con la pública de "
                "`GET /auth/public-key`"
            ),
        ),
    ] = None,
) -> None:
    """Solo existe para que Swagger muestre el campo de la cabecera.

    La lee `_key_from_header` desde la petición cruda, antes de que corran las
    dependencias; esta no valida ni devuelve nada.
    """

def _require_the_middleware(app) -> None:
    from persistence_kit.api.encrypted_middleware import EncryptedResponseMiddleware

    if not any(mw.cls is EncryptedResponseMiddleware for mw in app.user_middleware):
        raise RuntimeError(
            "Falta app.add_middleware(EncryptedResponseMiddleware): sin el la "
            "respuesta de una ruta cifrada sale en claro."
        )

def _with_body(request: Request, body: bytes) -> Request:

    async def receive() -> dict:
        return {"type": "http.request", "body": body, "more_body": False}

    scope = dict(request.scope)
    scope["headers"] = [
        (name, value) for name, value in request.scope["headers"] if name != b"content-length"
    ] + [(b"content-length", str(len(body)).encode())]
    return Request(scope, receive)

def _provider_for(settings):
    from persistence_kit.security.factory import get_key_provider

    return get_key_provider(settings)

def _require_a_shared_cache(settings: PersistenceKitSettings) -> None:
    if settings.is_local_stage:
        return
    if CacheSettings().cache_backend is CacheBackend.MEMORY:
        raise RuntimeError(
            "Una ruta cifrada necesita un CACHE_BACKEND compartido: con 'memory' "
            "cada proceso lleva su propia lista de sobres usados y el mismo sobre "
            "pasa una vez por worker."
        )

async def _reject_if_replayed(nonce: str) -> None:
    cache = get_cache("encrypted")
    if not await cache.set_if_absent(nonce, True, ttl_seconds=MAX_ENVELOPE_AGE_SECONDS):
        raise EncryptedPayloadError("El sobre ya fue usado")

def _locate_all(payload: dict, path: str) -> list[tuple[dict, str]]:
    key, _, rest = path.partition(".")
    if key.endswith("[]"):
        items = payload.get(key[:-2])
        if not isinstance(items, list):
            return []
        return [
            pair
            for item in items
            if isinstance(item, dict)
            for pair in _locate_all(item, rest)
        ]
    if not rest:
        return [(payload, key)]
    child = payload.get(key)
    if not isinstance(child, dict):
        return []
    return _locate_all(child, rest)

def _open_fields(payload: dict, fields: list[str], data_key: bytes, settings: PersistenceKitSettings) -> tuple[dict, list[str]]:
    if not isinstance(payload, dict):
        raise EncryptedPayloadError("El cuerpo debe ser un objeto JSON para cifrar campos.")
    if _ENVELOPE_FIELDS <= payload.keys():
        raise EncryptedPayloadError("Esta ruta cifra campos sueltos, no el cuerpo entero.")
    nonces: list[str] = []
    for field in fields:
        for container, key in _locate_all(payload, field):
            if key not in container:
                continue
            valor = container[key]
            if isinstance(valor, dict) and "ciphertext" in valor:
                container[key] = json.loads(decrypt(valor, data_key))
                nonces.append(valor["nonce"])
            elif not settings.is_local_stage:
                raise EncryptedPayloadError(f"El campo {field} debe estar cifrado")
    return payload, nonces

def _encrypt_fields(payload: dict | list, fields: list[str], data_key: bytes) -> dict | list:
    if isinstance(payload, list):
        return [_encrypt_fields(item, fields, data_key) for item in payload]
    if not isinstance(payload, dict):
        raise EncryptedPayloadError("La respuesta debe ser un objeto o una lista de objetos")
    for field in fields:
        for container, key in _locate_all(payload, field):
            if key in container:
                container[key] = encrypt(json.dumps(container[key]).encode(), data_key)
    return payload


def _encrypted_response(payload, respuesta: Response) -> JSONResponse:
    nueva = JSONResponse(payload, status_code=respuesta.status_code)
    nueva.raw_headers += [
        (name, value)
        for name, value in respuesta.raw_headers
        if name not in (b"content-length", b"content-type")
    ]
    return nueva


async def _open_body(body: bytes, settings: PersistenceKitSettings) -> tuple[bytes | None, bytes | None]:
    payload = json.loads(body)
    if isinstance(payload, dict) and _ENVELOPE_FIELDS <= payload.keys():
        plain, data_key = await decrypt_hybrid(payload, _provider_for(settings))
        await _reject_if_replayed(payload["nonce"])
        return plain, data_key
    if settings.is_local_stage:
        return None, None
    raise EncryptedPayloadError("El cuerpo debe venir en un sobre cifrado.")


async def _key_from_header(request: Request, settings) -> bytes | None:
    raw = request.headers.get(KEY_HEADER)
    if not raw:
        if settings.is_local_stage:
            return None
        raise EncryptedPayloadError(f"Falta la cabecera {KEY_HEADER}.")
    try:
        wrapped = base64.b64decode(raw, validate=True)
        return await _provider_for(settings).unwrap_key(wrapped)
    except (TypeError, ValueError, RuntimeError) as exc:
        raise EncryptedPayloadError("No se pudo abrir la llave de la cabecera.") from exc


def build_encrypted_route(settings_dep: Callable) -> type[APIRoute]:

    class EncryptedRoute(APIRoute):

        def get_route_handler(self):
            original = super().get_route_handler()
            marca = self.openapi_extra or {}
            if not marca.get(ENCRYPTED_FLAG):
                return original
            _require_a_shared_cache(settings_dep())
            fields = marca.get(ENCRYPTED_FIELDS)
            response_fields = marca.get(ENCRYPTED_RESPONSE_FIELDS)

            async def encrypted_handler(request: Request) -> Response:
                settings = settings_dep()
                _require_the_middleware(request.app)
                state = request.state
                body = await request.body()
                try:
                    if fields:
                        data_key = await _key_from_header(request, settings)
                        if data_key is not None:
                            payload, nonces = _open_fields(json.loads(body), fields, data_key, settings)
                            for nonce in nonces:
                                await _reject_if_replayed(nonce)
                            request = _with_body(request, json.dumps(payload).encode())

                    elif body:
                        plain, data_key = await _open_body(body, settings)
                        if plain is not None:
                            request = _with_body(request, plain)

                    else:
                        data_key = await _key_from_header(request, settings)

                except json.JSONDecodeError as exc:
                    raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=_NOT_JSON) from exc
                except EncryptedPayloadError as exc:
                    raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

                if data_key:
                    setattr(state, KEY_STATE, data_key)
                    setattr(state, RESPONSE_FIELDS_STATE, response_fields)

                respuesta = await original(request)

                if data_key and not hasattr(respuesta, "body"):
                    raise RuntimeError(
                        f"Una ruta cifrada no puede devolver {type(respuesta).__name__}: "
                        "el contenido en streaming no se puede cifrar."
                    )

                return respuesta

            return encrypted_handler

    return EncryptedRoute

