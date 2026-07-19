from __future__ import annotations

import json as _json
from typing import Any, Callable, Mapping

from persistence_kit.restclient.auth.base import NoAuth
from persistence_kit.restclient.contracts import (
    Authenticator,
    EndpointResolver,
    RestRequest,
    RestResponse,
)
from persistence_kit.restclient.errors import RestHTTPError
from persistence_kit.restclient.mapping import ModelMappingMixin
from persistence_kit.restclient.payload import prepare_request

Handler = Callable[[RestRequest], RestResponse]


def _encode(json: Any | None, content: bytes | None) -> bytes:
    if content is not None:
        return content
    if json is not None:
        return _json.dumps(json).encode("utf-8")
    return b""


class MemoryRestClient(ModelMappingMixin):
    """In-memory test double implementing the RestClient protocol.

    Register canned responses (or handlers) per ``(method, service)`` and assert
    on ``calls`` to verify how authenticators and callers shape each request.
    """

    def __init__(
        self,
        *,
        authenticator: Authenticator | None = None,
        resolver: EndpointResolver | None = None,
        default_headers: Mapping[str, str] | None = None,
        raise_for_status: bool = True,
    ) -> None:
        self._auth = authenticator or NoAuth()
        self._resolver = resolver
        self._default_headers = dict(default_headers or {})
        self._raise = raise_for_status
        self._stubs: dict[tuple[str, str], RestResponse | Handler] = {}
        self.calls: list[RestRequest] = []

    def stub(
        self,
        method: str,
        service: str,
        *,
        status_code: int = 200,
        json: Any | None = None,
        content: bytes | None = None,
        headers: Mapping[str, str] | None = None,
        handler: Handler | None = None,
    ) -> "MemoryRestClient":
        key = (method.upper(), service)
        if handler is not None:
            self._stubs[key] = handler
        else:
            self._stubs[key] = RestResponse(
                status_code=status_code,
                headers=dict(headers or {}),
                content=_encode(json, content),
                url=service,
            )
        return self

    async def request(
        self,
        method: str,
        service: str,
        *,
        params: Mapping[str, Any] | None = None,
        json: Any | None = None,
        xml: Any | None = None,
        content: bytes | None = None,
        content_type: str | None = None,
        soap_action: str | None = None,
        headers: Mapping[str, str] | None = None,
        cache_ttl: float | None = None,
    ) -> RestResponse:
        url = await self._resolver.resolve(service) if self._resolver else service
        request = prepare_request(
            method,
            url,
            default_headers=self._default_headers,
            params=params,
            json=json,
            xml=xml,
            content=content,
            content_type=content_type,
            soap_action=soap_action,
            headers=headers,
        )
        request = await self._auth.apply(request)
        self.calls.append(request)

        stub = self._stubs.get((method.upper(), service))
        if stub is None:
            response = RestResponse(
                status_code=404,
                headers={},
                content=b"",
                url=url,
            )
        elif callable(stub):
            response = stub(request)
        else:
            response = stub

        if self._raise and not response.is_success:
            raise RestHTTPError(response.status_code, response=response)
        return response

    async def get(self, service: str, **kwargs: Any) -> RestResponse:
        return await self.request("GET", service, **kwargs)

    async def post(self, service: str, **kwargs: Any) -> RestResponse:
        return await self.request("POST", service, **kwargs)

    async def put(self, service: str, **kwargs: Any) -> RestResponse:
        return await self.request("PUT", service, **kwargs)

    async def delete(self, service: str, **kwargs: Any) -> RestResponse:
        return await self.request("DELETE", service, **kwargs)

    async def aclose(self) -> None:
        return None
