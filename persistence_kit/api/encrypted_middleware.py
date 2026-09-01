import json
from collections.abc import Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from persistence_kit.api.encrypted_routes import (
    KEY_STATE,
    RESPONSE_FIELDS_STATE,
    _encrypt_fields,
    _encrypted_response,
)
from persistence_kit.security.encrypted.envelope import encrypt


class EncryptedResponseMiddleware(BaseHTTPMiddleware):
    """Cifra la respuesta de las rutas marcadas con ``encrypted()``.

    Corre por fuera del ``ExceptionMiddleware``, asi que ve la respuesta ya
    construida por los ``exception_handler`` de la app y las cifra igual que las
    del handler. La ruta solo deja la llave en ``request.state``.
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        respuesta = await call_next(request)
        data_key = getattr(request.state, KEY_STATE, None)
        if data_key is None:
            return respuesta

        cuerpo = b"".join([chunk async for chunk in respuesta.body_iterator])
        if not cuerpo:
            return Response(
                status_code=respuesta.status_code,
                headers=dict(respuesta.headers),
            )

        response_fields = getattr(request.state, RESPONSE_FIELDS_STATE, None)
        if response_fields:
            payload = _encrypt_fields(json.loads(cuerpo), response_fields, data_key)
        else:
            payload = encrypt(cuerpo, data_key)
        return _encrypted_response(payload, respuesta)
