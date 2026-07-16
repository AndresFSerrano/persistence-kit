from __future__ import annotations

from persistence_kit.restclient.contracts import RestRequest, RestResponse


class NoAuth:
    """Authenticator for public APIs that require no credentials."""

    async def apply(self, request: RestRequest) -> RestRequest:
        return request

    async def on_unauthorized(self, response: RestResponse) -> bool:
        return False
