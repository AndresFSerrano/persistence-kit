from __future__ import annotations

from typing import Any, Mapping

import httpx

from persistence_kit.resilience import CircuitBreaker
from persistence_kit.restclient.auth.base import NoAuth
from persistence_kit.restclient.config import ServiceConfig
from persistence_kit.restclient.contracts import (
    Authenticator,
    EndpointResolver,
    RestRequest,
    RestResponse,
)
from persistence_kit.restclient.errors import (
    RestCircuitOpenError,
    RestHTTPError,
    RestTimeoutError,
    RestTransportError,
)
from persistence_kit.restclient.mapping import ModelMappingMixin
from persistence_kit.restclient.payload import prepare_request
from persistence_kit.restclient.resolver import StaticEndpointResolver
from persistence_kit.restclient.retry import RetryPolicy


class HttpxRestClient(ModelMappingMixin):
    """Async REST transport backed by httpx.

    The endpoint resolver, authenticator and retry policy are pluggable, so the
    same client serves public, API-key, bearer or OAuth2 protected services.
    """

    def __init__(
        self,
        *,
        config: ServiceConfig | None = None,
        resolver: EndpointResolver | None = None,
        authenticator: Authenticator | None = None,
        retry_policy: RetryPolicy | None = None,
        circuit_breaker: CircuitBreaker | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._config = config or ServiceConfig()
        self._resolver = resolver or StaticEndpointResolver()
        self._auth = authenticator or NoAuth()
        self._retry = retry_policy or RetryPolicy(self._config.max_retries)
        self._circuit = circuit_breaker
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            timeout=self._config.timeout_seconds,
            verify=self._config.verify_tls,
            headers=self._base_headers(),
        )

    def _base_headers(self) -> dict[str, str]:
        headers = dict(self._config.default_headers)
        if self._config.user_agent:
            headers.setdefault("User-Agent", self._config.user_agent)
        return headers

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
    ) -> RestResponse:
        url = await self._resolver.resolve(service)
        request = prepare_request(
            method,
            url,
            default_headers=self._base_headers(),
            params=params,
            json=json,
            xml=xml,
            content=content,
            content_type=content_type,
            soap_action=soap_action,
            headers=headers,
        )
        request = await self._auth.apply(request)

        attempt = 0
        refreshed = False
        while True:
            if self._circuit is not None and not self._circuit.allow():
                raise RestCircuitOpenError(
                    f"Circuito abierto para '{request.url}'; solicitud corto-circuitada."
                )

            try:
                response = await self._send(request)
            except (RestTimeoutError, RestTransportError) as exc:
                if self._circuit is not None:
                    self._circuit.record_failure()
                if self._retry.should_retry_exception(exc, attempt):
                    await self._retry.sleep(attempt)
                    attempt += 1
                    continue
                raise

            if self._circuit is not None:
                if response.status_code >= 500:
                    self._circuit.record_failure()
                else:
                    self._circuit.record_success()

            if (
                response.status_code == 401
                and not refreshed
                and await self._auth.on_unauthorized(response)
            ):
                request = await self._auth.apply(request)
                refreshed = True
                continue

            if self._retry.should_retry(response.status_code, attempt):
                await self._retry.sleep(attempt)
                attempt += 1
                continue
            break

        if self._config.raise_for_status and not response.is_success:
            raise RestHTTPError(response.status_code, response=response)
        return response

    async def _send(self, request: RestRequest) -> RestResponse:
        try:
            raw = await self._client.request(
                request.method,
                request.url,
                params=request.params or None,
                headers=request.headers or None,
                json=request.json_body,
                content=request.content,
            )
        except httpx.TimeoutException as exc:
            raise RestTimeoutError(f"Timeout al consumir '{request.url}': {exc}") from exc
        except httpx.TransportError as exc:
            raise RestTransportError(
                f"Error de transporte al consumir '{request.url}': {exc}"
            ) from exc

        return RestResponse(
            status_code=raw.status_code,
            headers=dict(raw.headers),
            content=raw.content,
            url=str(raw.request.url),
        )

    async def get(self, service: str, **kwargs: Any) -> RestResponse:
        return await self.request("GET", service, **kwargs)

    async def post(self, service: str, **kwargs: Any) -> RestResponse:
        return await self.request("POST", service, **kwargs)

    async def put(self, service: str, **kwargs: Any) -> RestResponse:
        return await self.request("PUT", service, **kwargs)

    async def delete(self, service: str, **kwargs: Any) -> RestResponse:
        return await self.request("DELETE", service, **kwargs)

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> "HttpxRestClient":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()
