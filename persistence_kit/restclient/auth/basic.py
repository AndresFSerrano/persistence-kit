from __future__ import annotations

import base64

from persistence_kit.restclient.contracts import RestRequest, RestResponse


class BasicAuth:
    """HTTP Basic authentication."""

    def __init__(self, username: str, password: str) -> None:
        raw = f"{username}:{password}".encode("utf-8")
        self._value = "Basic " + base64.b64encode(raw).decode("ascii")

    async def apply(self, request: RestRequest) -> RestRequest:
        request.headers["Authorization"] = self._value
        return request

    async def on_unauthorized(self, response: RestResponse) -> bool:
        return False
