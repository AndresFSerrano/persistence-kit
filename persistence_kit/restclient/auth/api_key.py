from __future__ import annotations

from typing import Literal

from persistence_kit.restclient.contracts import RestRequest, RestResponse

ApiKeyLocation = Literal["header", "query"]


class ApiKeyAuth:
    """Injects an API key either as a request header or a query parameter."""

    def __init__(
        self,
        name: str,
        value: str,
        *,
        location: ApiKeyLocation = "header",
    ) -> None:
        if location not in ("header", "query"):
            raise ValueError("location debe ser 'header' o 'query'.")
        self._name = name
        self._value = value
        self._location = location

    async def apply(self, request: RestRequest) -> RestRequest:
        if self._location == "header":
            request.headers[self._name] = self._value
        else:
            request.params[self._name] = self._value
        return request

    async def on_unauthorized(self, response: RestResponse) -> bool:
        return False
