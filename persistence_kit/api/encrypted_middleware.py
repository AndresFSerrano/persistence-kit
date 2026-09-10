import json
from collections.abc import Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from fastapi.responses import JSONResponse

from persistence_kit.api.encrypted_routes import (
    KEY_STATE,
    RESPONSE_FIELDS_STATE,
    locate_all,
)
from persistence_kit.security.encrypted.envelope import encrypt
from persistence_kit.security.encrypted.errors import EncryptedPayloadError


def _encrypt_fields(payload: dict | list, fields: list[str], data_key: bytes) -> dict | list:
    if isinstance(payload, list):
        return [_encrypt_fields(item, fields, data_key) for item in payload]
    if not isinstance(payload, dict):
        raise EncryptedPayloadError("La respuesta debe ser un objeto o una lista de objetos")
    for field in fields:
        for container, key in locate_all(payload, field):
            if key in container:
                container[key] = encrypt(json.dumps(container[key]).encode(), data_key)
    return payload


def _encrypted_response(payload, source: Response) -> JSONResponse:
    response = JSONResponse(payload, status_code=source.status_code)
    response.raw_headers += [
        (name, value)
        for name, value in source.raw_headers
        if name not in (b"content-length", b"content-type")
    ]
    return response


class EncryptedResponseMiddleware(BaseHTTPMiddleware):
    """Encrypts the response of routes marked with ``encrypted()``.

    Runs outside the ``ExceptionMiddleware``, so it also covers the responses
    built by the app's own exception handlers. The route only leaves the key on
    ``request.state``.
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        response = await call_next(request)
        data_key = getattr(request.state, KEY_STATE, None)
        if data_key is None:
            return response

        body = b"".join([chunk async for chunk in response.body_iterator])
        if not body:
            empty = Response(status_code=response.status_code)
            empty.raw_headers = list(response.raw_headers)
            return empty

        response_fields = getattr(request.state, RESPONSE_FIELDS_STATE, None)
        if response_fields:
            payload = _encrypt_fields(json.loads(body), response_fields, data_key)
        else:
            payload = encrypt(body, data_key)
        return _encrypted_response(payload, response)
