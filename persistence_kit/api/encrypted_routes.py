import base64
import json
from collections.abc import Callable
from typing import Annotated, get_args

from fastapi import Header, Request, Response, status
from fastapi.exceptions import HTTPException
from fastapi.routing import APIRoute

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
    mark: dict[str, object] = {ENCRYPTED_FLAG: True}
    if fields:
        mark[ENCRYPTED_FIELDS] = fields
    if response_fields:
        mark[ENCRYPTED_RESPONSE_FIELDS] = response_fields
    if with_body and not fields:
        mark["requestBody"] = {
            "required": True,
            "content": {"application/json": {"schema": _ENVELOPE_SCHEMA}},
        }
    return mark


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
    """Declares the key header so Swagger shows the field.

    ``_key_from_header`` reads it from the raw request, before the dependencies
    run; this one neither validates nor returns anything.
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

def _prepare_key_provider(settings: PersistenceKitSettings) -> None:
    _provider_for(settings)

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
    if not await cache.set_if_absent(nonce, True, ttl_seconds=2 * MAX_ENVELOPE_AGE_SECONDS):
        raise EncryptedPayloadError("El sobre ya fue usado")

def _validate_path(path: str) -> None:
    key, dot, rest = path.partition(".")
    if not key:
        raise RuntimeError(f"El campo '{path}' tiene un segmento vacio.")
    if key == "[]":
        raise RuntimeError(f"El campo '{path}' tiene una lista sin nombre.")
    if "[]" in key and not key.endswith("[]"):
        raise RuntimeError(f"El campo '{path}' pone [] en medio de un nombre.")
    if key.endswith("[]") and not rest:
        raise RuntimeError(f"El campo '{path}' apunta a una lista, no a un campo de sus objetos.")
    if dot and not rest:
        raise RuntimeError(f"El campo '{path}' tiene un segmento vacio.")
    if rest:
        _validate_path(rest)

def _model_of(annotation):
    if hasattr(annotation, "model_fields"):
        return annotation
    for arg in get_args(annotation):
        found = _model_of(arg)
        if found is not None:
            return found
    return None

def _validate_against_model(path: str, model: type) -> None:
    key, _, rest = path.partition(".")
    name = key.removesuffix("[]")
    fields = getattr(model, "model_fields", None)
    if fields is None:
        return
    if name not in fields:
        raise RuntimeError(f"El campo '{name}' no existe en {model.__name__}.")
    if rest:
        _validate_against_model(rest, _model_of(fields[name].annotation))


def locate_all(payload: dict, path: str) -> list[tuple[dict, str]]:
    key, _, rest = path.partition(".")
    if key.endswith("[]"):
        items = payload.get(key[:-2])
        if not isinstance(items, list):
            return []
        return [
            pair
            for item in items
            if isinstance(item, dict)
            for pair in locate_all(item, rest)
        ]
    if not rest:
        return [(payload, key)]
    child = payload.get(key)
    if not isinstance(child, dict):
        return []
    return locate_all(child, rest)

def _open_fields(payload: dict, fields: list[str], data_key: bytes, settings: PersistenceKitSettings) -> tuple[dict, list[str]]:
    if not isinstance(payload, dict):
        raise EncryptedPayloadError("El cuerpo debe ser un objeto JSON para cifrar campos.")
    if _ENVELOPE_FIELDS <= payload.keys():
        raise EncryptedPayloadError("Esta ruta cifra campos sueltos, no el cuerpo entero.")
    nonces: list[str] = []
    for field in fields:
        for container, key in locate_all(payload, field):
            if key not in container:
                continue
            value = container[key]
            if isinstance(value, dict) and "ciphertext" in value:
                container[key] = json.loads(decrypt(value, data_key))
                nonces.append(value["nonce"])
            elif not settings.is_local_stage:
                raise EncryptedPayloadError(f"El campo {field} debe estar cifrado")
    return payload, nonces


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
    except (TypeError, ValueError) as exc:
        raise EncryptedPayloadError("No se pudo abrir la llave de la cabecera.") from exc
    return await _provider_for(settings).unwrap_key(wrapped)

def _validate_fields(route: APIRoute, fields, response_fields) -> None:
    body_model = None
    if route.body_field is not None:
        body_model = _model_of(route.body_field.field_info.annotation)

    for path in fields or ():
        _validate_path(path)
        if body_model is not None:
            _validate_against_model(path, body_model)

    for path in response_fields or ():
        _validate_path(path)


def build_encrypted_route(settings_dep: Callable) -> type[APIRoute]:

    class EncryptedRoute(APIRoute):

        def get_route_handler(self):
            original = super().get_route_handler()
            mark = self.openapi_extra or {}
            if not mark.get(ENCRYPTED_FLAG):
                return original
            settings = settings_dep()
            _require_a_shared_cache(settings)
            _prepare_key_provider(settings)
            fields = mark.get(ENCRYPTED_FIELDS)
            response_fields = mark.get(ENCRYPTED_RESPONSE_FIELDS)
            _validate_fields(self, fields, response_fields)

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

                response = await original(request)

                if data_key and not hasattr(response, "body"):
                    raise RuntimeError(
                        f"Una ruta cifrada no puede devolver {type(response).__name__}: "
                        "el contenido en streaming no se puede cifrar."
                    )

                return response

            return encrypted_handler

    return EncryptedRoute

