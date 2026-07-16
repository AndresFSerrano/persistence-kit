from __future__ import annotations

import time
from typing import Sequence

from persistence_kit.restclient.contracts import RestRequest, RestResponse
from persistence_kit.restclient.errors import RestAuthError


class OAuth2ClientCredentials:
    """OAuth2 client-credentials grant with token caching and refresh.

    The token is fetched lazily on the first request, cached until shortly
    before expiry, and refreshed automatically after a 401 response.
    """

    def __init__(
        self,
        *,
        token_url: str,
        client_id: str,
        client_secret: str,
        scopes: Sequence[str] = (),
        audience: str | None = None,
        header: str = "Authorization",
        scheme: str = "Bearer",
        leeway_seconds: float = 30.0,
        timeout_seconds: float = 10.0,
        verify_tls: bool = True,
    ) -> None:
        self._token_url = token_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._scopes = tuple(scopes)
        self._audience = audience
        self._header = header
        self._scheme = scheme
        self._leeway = leeway_seconds
        self._timeout = timeout_seconds
        self._verify_tls = verify_tls
        self._token: str | None = None
        self._expires_at = 0.0

    async def apply(self, request: RestRequest) -> RestRequest:
        token = await self._ensure_token()
        request.headers[self._header] = f"{self._scheme} {token}"
        return request

    async def on_unauthorized(self, response: RestResponse) -> bool:
        self._token = None
        self._expires_at = 0.0
        return True

    async def _ensure_token(self) -> str:
        if self._token and time.monotonic() < self._expires_at - self._leeway:
            return self._token
        await self._fetch_token()
        assert self._token is not None
        return self._token

    async def _fetch_token(self) -> None:
        import httpx

        data: dict[str, str] = {
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
        }
        if self._scopes:
            data["scope"] = " ".join(self._scopes)
        if self._audience:
            data["audience"] = self._audience

        try:
            async with httpx.AsyncClient(
                timeout=self._timeout, verify=self._verify_tls
            ) as client:
                response = await client.post(self._token_url, data=data)
        except httpx.HTTPError as exc:
            raise RestAuthError(
                f"No se pudo contactar el token endpoint '{self._token_url}': {exc}"
            ) from exc

        if response.status_code != 200:
            raise RestAuthError(
                f"El token endpoint respondio {response.status_code} al solicitar el token."
            )

        payload = response.json()
        token = payload.get("access_token")
        if not token:
            raise RestAuthError("La respuesta del token endpoint no incluye 'access_token'.")

        self._token = token
        expires_in = float(payload.get("expires_in", 3600))
        self._expires_at = time.monotonic() + expires_in
