from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from persistence_kit.resilience import CircuitBreaker
from persistence_kit.restclient.auth.base import NoAuth
from persistence_kit.restclient.config import ServiceConfig
from persistence_kit.restclient.contracts import (
    Authenticator,
    EndpointResolver,
    RestClient,
)
from persistence_kit.restclient.errors import RestConfigError
from persistence_kit.restclient.resolver import StaticEndpointResolver
from persistence_kit.restclient.retry import RetryPolicy


@dataclass(slots=True)
class RegisteredService:
    name: str
    config: ServiceConfig
    resolver: EndpointResolver
    authenticator: Authenticator
    retry_policy: RetryPolicy | None = None
    circuit_breaker: CircuitBreaker | None = None


class RestClientRegistry:
    """Declares external APIs once by name and resolves cached clients on demand.

    Public APIs, API-key protected APIs and OAuth2 services coexist here; each is
    registered with its own resolver and authenticator.

    When built with ``settings``, each service's base URL is taken from
    ``settings.rest_service_urls[name]`` if present, so a URL can be overridden by
    environment variable (e.g. in CI/CD when an API moves) without code changes;
    the ``base_url`` passed to :meth:`register` is the fallback default.
    """

    def __init__(self, settings: Any = None) -> None:
        self._settings = settings
        self._services: dict[str, RegisteredService] = {}
        self._clients: dict[str, RestClient] = {}

    def _base_url_for(self, name: str, base_url: str) -> str:
        if self._settings is None:
            return base_url
        service_urls = getattr(self._settings, "rest_service_urls", None) or {}
        return service_urls.get(name, base_url)

    def register(
        self,
        name: str,
        *,
        base_url: str = "",
        authenticator: Authenticator | None = None,
        resolver: EndpointResolver | None = None,
        config: ServiceConfig | None = None,
        retry_policy: RetryPolicy | None = None,
        circuit_breaker: CircuitBreaker | None = None,
        replace: bool = False,
    ) -> "RestClientRegistry":
        if name in self._services and not replace:
            raise RestConfigError(f"El servicio '{name}' ya esta registrado.")
        self._services[name] = RegisteredService(
            name=name,
            config=config or ServiceConfig(),
            resolver=resolver or StaticEndpointResolver(self._base_url_for(name, base_url)),
            authenticator=authenticator or NoAuth(),
            retry_policy=retry_policy,
            circuit_breaker=circuit_breaker,
        )
        self._clients.pop(name, None)
        return self

    def get(self, name: str) -> RestClient:
        cached = self._clients.get(name)
        if cached is not None:
            return cached
        service = self._services.get(name)
        if service is None:
            raise RestConfigError(f"El servicio '{name}' no esta registrado.")

        try:
            from persistence_kit.restclient.client import HttpxRestClient
        except ModuleNotFoundError as exc:
            raise RestConfigError(
                "El transporte HTTP requiere la capability opcional 'restclient'. "
                "Instala con `persistence-kit[restclient]`."
            ) from exc

        client = HttpxRestClient(
            config=service.config,
            resolver=service.resolver,
            authenticator=service.authenticator,
            retry_policy=service.retry_policy,
            circuit_breaker=service.circuit_breaker,
        )
        self._clients[name] = client
        return client

    def names(self) -> tuple[str, ...]:
        return tuple(self._services)

    def clear(self) -> None:
        self._services.clear()
        self._clients.clear()

    async def aclose_all(self) -> None:
        for client in self._clients.values():
            await client.aclose()
        self._clients.clear()
