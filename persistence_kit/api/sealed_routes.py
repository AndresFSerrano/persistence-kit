import base64
import json
from collections.abc import Callable
from typing import Annotated

from fastapi import Header, Request, Response, status
from fastapi.exceptions import HTTPException, RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from fastapi.encoders import jsonable_encoder

from persistence_kit.cache import get_cache
from persistence_kit.settings import PersistenceKitSettings
from persistence_kit.api.exceptions import SealedPayloadError
from persistence_kit.security.sealed.envelope import (
    HYBRID_VERSION,
    MAX_ENVELOPE_AGE_SECONDS,
    open_hybrid,
    open_sealed,
    seal,
)

SEALED_FLAG = "x-sealed"
SEALED_FIELDS = "x-sealed-fields"
SEALED_RESPONSE_FIELDS = "x-sealed-response-fields"
KEY_HEADER = "x-sealed-key"

_NOT_JSON = "El cuerpo debe ser un sobre cifrado en JSON."

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
                f"Momento en que se selló, en segundos desde 1970. El servidor "
                f"rechaza los sobres con más de {MAX_ENVELOPE_AGE_SECONDS} segundos."
            ),
        },
    },
}

def sealed(
        with_body: bool = True,
        fields: list[str] | None = None,
        response_fields: list[str] | None = None
) -> dict:
    marca: dict[str, object] = {SEALED_FLAG: True}
    if fields:
        marca[SEALED_FIELDS] = fields
    if response_fields:
        marca[SEALED_RESPONSE_FIELDS] = response_fields
    if with_body and not fields:
        marca["requestBody"] = {
            "required": True,
            "content": {"application/json": {"schema": _ENVELOPE_SCHEMA}},
        }
    return marca


async def declare_key_header(
    x_sealed_key: Annotated[
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

def _with_body(request: Request, body: bytes) -> Request:

    async def receive() -> dict:
        return {"type": "http.request", "body": body, "more_body": False}

    scope = dict(request.scope)
    scope["headers"] = [
        (name, value) for name, value in request.scope["headers"] if name != b"content-length"
    ] + [(b"content-length", str(len(body)).encode())]
    return Request(scope, receive)

def _provider_for(settings):
    from persistence_kit.security.factory import key_provider

    return key_provider(settings)

async def _reject_if_replayed(nonce: str) -> None:
    cache = get_cache("sealed")
    if await cache.get(nonce) is not None:
        raise SealedPayloadError("El sobre ya fue usado")
    await cache.set(nonce, True, ttl_seconds=MAX_ENVELOPE_AGE_SECONDS)

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

def _open_fields(payload: dict, fields: list[str], data_key: bytes, settings: PersistenceKitSettings) -> dict:
    if not isinstance(payload, dict):
        raise SealedPayloadError("El cuerpo debe ser un objeto JSON para cifrar campos.")
    for field in fields:
        pairs = _locate_all(payload, field)
        if not pairs:
            raise SealedPayloadError(f"El campo '{field}' esta declarado pero no figura en el sobre")
        for container, key in pairs:
            if key not in container:
                raise SealedPayloadError(
                    f"El campo '{field}' esta declarado pero no figura en el sobre"
                )
            valor = container[key]
            if isinstance(valor, dict) and "ciphertext" in valor:
                container[key] = json.loads(open_sealed(valor, data_key))
            elif not settings.is_local_stage:
                raise SealedPayloadError(f"El campo {field} debe estar cifrado")
    return payload

def _seal_fields(payload: dict | list, fields: list[str], data_key: bytes) -> dict | list:
    if isinstance(payload, list):
        return [_seal_fields(item, fields, data_key) for item in payload]
    if not isinstance(payload, dict):
        raise SealedPayloadError("La respuesta debe ser un objeto o una lista de objetos")
    for field in fields:
        for container, key in _locate_all(payload, field):
            if key in container:
                container[key] = seal(json.dumps(container[key]).encode(), data_key)
    return payload


async def _open_body(body: bytes, settings: PersistenceKitSettings) -> tuple[bytes | None, bytes | None]:
    payload = json.loads(body)
    if isinstance(payload, dict) and "v" in payload:
        plain, data_key = await open_hybrid(payload, _provider_for(settings))
        await _reject_if_replayed(payload["nonce"])
        return plain, data_key
    if settings.is_local_stage:
        return None, None
    raise SealedPayloadError("El cuerpo debe venir en un sobre cifrado.")


async def _key_from_header(request: Request, settings) -> bytes | None:
    raw = request.headers.get(KEY_HEADER)
    if not raw:
        if settings.is_local_stage:
            return None
        raise SealedPayloadError(f"Falta la cabecera {KEY_HEADER}.")
    try:
        wrapped = base64.b64decode(raw, validate=True)
    except (TypeError, ValueError) as exc:
        raise SealedPayloadError("No se pudo abrir la llave de la cabecera.") from exc
    return await _provider_for(settings).unwrap_key(wrapped)


def build_sealed_route(settings_dep: Callable) -> type[APIRoute]:

    class SealedRoute(APIRoute):

        def get_route_handler(self):
            original = super().get_route_handler()
            marca = self.openapi_extra or {}
            if not marca.get(SEALED_FLAG):
                return original
            fields = marca.get(SEALED_FIELDS)
            response_fields = marca.get(SEALED_RESPONSE_FIELDS)

            async def sealed_handler(request: Request) -> Response:
                settings = settings_dep()
                body = await request.body()
                try:
                    if fields:
                        data_key = await _key_from_header(request, settings)
                        if data_key is not None:
                            payload = _open_fields(json.loads(body), fields, data_key, settings)
                            request = _with_body(request, json.dumps(payload).encode())

                    elif body:
                        plain, data_key = await _open_body(body, settings)
                        if plain is not None:
                            request = _with_body(request, plain)

                    else:
                        data_key = await _key_from_header(request, settings)

                except json.JSONDecodeError as exc:
                    raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=_NOT_JSON) from exc
                except SealedPayloadError as exc:
                    raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

                try:
                    respuesta = await original(request)
                except RequestValidationError as exc:
                    respuesta = JSONResponse(
                        {"detail": jsonable_encoder(exc.errors())},
                        status_code = 422
                    )
                except HTTPException as exc:
                    respuesta = JSONResponse(
                        {"detail": exc.detail},
                        status_code = exc.status_code,
                        headers = exc.headers
                    )

                if data_key and getattr(respuesta, "body", None):
                    if response_fields:
                        payload = _seal_fields(
                            json.loads(bytes(respuesta.body)), response_fields, data_key
                        )
                        return JSONResponse(
                            payload,
                            status_code = respuesta.status_code
                        )
                    return JSONResponse(
                        seal(bytes(respuesta.body), data_key),
                        status_code=respuesta.status_code,
                    )
                return respuesta

            return sealed_handler

    return SealedRoute

