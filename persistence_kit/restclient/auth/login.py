from __future__ import annotations

import time
from typing import Any, Mapping

from persistence_kit.restclient.contracts import RestRequest, RestResponse
from persistence_kit.restclient.errors import RestAuthError


def _dig(data: Any, path: str) -> Any:
    current = data
    for part in path.split("."):
        if isinstance(current, dict):
            current = current.get(part)
        else:
            current = getattr(current, part, None)
    return current


class LoginTokenAuth:
    """Logs in with username/password at a login endpoint and sends the returned
    token as a bearer. The token is cached until shortly before expiry and
    re-fetched automatically after a 401.

    ``send_as`` picks the request body ("json" or "form"). ``token_field`` and
    ``expires_field`` are dotted paths into the login response.
    """

    def __init__(
        self,
        *,
        login_url: str,
        username: str,
        password: str,
        username_field: str = "username",
        password_field: str = "password",
        send_as: str = "json",
        token_field: str = "access_token",
        expires_field: str | None = "expires_in",
        default_ttl_seconds: float = 3600.0,
        extra_fields: Mapping[str, Any] | None = None,
        header: str = "Authorization",
        scheme: str = "Bearer",
        leeway_seconds: float = 30.0,
        timeout_seconds: float = 10.0,
        verify_tls: bool = True,
    ) -> None:
        if send_as not in ("json", "form"):
            raise ValueError("send_as debe ser 'json' o 'form'.")
        self._login_url = login_url
        self._username = username
        self._password = password
        self._username_field = username_field
        self._password_field = password_field
        self._send_as = send_as
        self._token_field = token_field
        self._expires_field = expires_field
        self._default_ttl = default_ttl_seconds
        self._extra_fields = dict(extra_fields or {})
        self._header = header
        self._scheme = scheme
        self._leeway = leeway_seconds
        self._timeout = timeout_seconds
        self._verify_tls = verify_tls
        self._token: str | None = None
        self._expires_at = 0.0

    async def apply(self, request: RestRequest) -> RestRequest:
        token = await self._ensure_token()
        prefix = f"{self._scheme} " if self._scheme else ""
        request.headers[self._header] = f"{prefix}{token}"
        return request

    async def on_unauthorized(self, response: RestResponse) -> bool:
        self._token = None
        self._expires_at = 0.0
        return True

    async def _ensure_token(self) -> str:
        if self._token and time.monotonic() < self._expires_at - self._leeway:
            return self._token
        await self._login()
        assert self._token is not None
        return self._token

    async def _login(self) -> None:
        import httpx

        payload: dict[str, Any] = {
            self._username_field: self._username,
            self._password_field: self._password,
        }
        payload.update(self._extra_fields)

        try:
            async with httpx.AsyncClient(
                timeout=self._timeout, verify=self._verify_tls
            ) as client:
                if self._send_as == "json":
                    response = await client.post(self._login_url, json=payload)
                else:
                    response = await client.post(self._login_url, data=payload)
        except httpx.HTTPError as exc:
            raise RestAuthError(
                f"No se pudo contactar el login endpoint '{self._login_url}': {exc}"
            ) from exc

        if response.status_code >= 400:
            raise RestAuthError(
                f"El login endpoint respondio {response.status_code} al autenticar."
            )

        data = response.json()
        token = _dig(data, self._token_field)
        if not token:
            raise RestAuthError(
                f"La respuesta del login no incluye el campo '{self._token_field}'."
            )

        self._token = str(token)
        expires = _dig(data, self._expires_field) if self._expires_field else None
        self._expires_at = time.monotonic() + (
            float(expires) if expires else self._default_ttl
        )
