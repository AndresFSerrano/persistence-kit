from __future__ import annotations

from persistence_kit.restclient.contracts import RestRequest, RestResponse


class BearerAuth:
    """Injects a static bearer (or custom scheme) token in a header."""

    def __init__(
        self,
        token: str,
        *,
        scheme: str = "Bearer",
        header: str = "Authorization",
    ) -> None:
        self._token = token
        self._scheme = scheme
        self._header = header

    async def apply(self, request: RestRequest) -> RestRequest:
        prefix = f"{self._scheme} " if self._scheme else ""
        request.headers[self._header] = f"{prefix}{self._token}"
        return request

    async def on_unauthorized(self, response: RestResponse) -> bool:
        return False
