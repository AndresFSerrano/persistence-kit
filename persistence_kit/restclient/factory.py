from __future__ import annotations

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


def build_rest_client(
    *,
    base_url: str = "",
    authenticator: Authenticator | None = None,
    resolver: EndpointResolver | None = None,
    config: ServiceConfig | None = None,
    retry_policy: RetryPolicy | None = None,
    circuit_breaker: CircuitBreaker | None = None,
    settings: Any | None = None,
) -> RestClient:
    if config is None:
        config = (
            ServiceConfig.from_settings(settings)
            if settings is not None
            else ServiceConfig()
        )
    resolver = resolver or StaticEndpointResolver(base_url)
    authenticator = authenticator or NoAuth()

    try:
        from persistence_kit.restclient.client import HttpxRestClient
    except ModuleNotFoundError as exc:
        raise RestConfigError(
            "El transporte HTTP requiere la capability opcional 'restclient'. "
            "Instala con `persistence-kit[restclient]`."
        ) from exc

    return HttpxRestClient(
        config=config,
        resolver=resolver,
        authenticator=authenticator,
        retry_policy=retry_policy,
        circuit_breaker=circuit_breaker,
    )
