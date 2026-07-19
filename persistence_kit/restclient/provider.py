from __future__ import annotations

from functools import lru_cache
from typing import Callable

from persistence_kit.resilience import CircuitBreaker
from persistence_kit.restclient.config import ServiceConfig
from persistence_kit.restclient.contracts import (
    Authenticator,
    EndpointResolver,
    RestClient,
)
from persistence_kit.restclient.registry import RestClientRegistry
from persistence_kit.restclient.retry import RetryPolicy

_default_registry_instance: RestClientRegistry | None = None
_registry_initializer: Callable[[], None] | None = None


def default_rest_registry() -> RestClientRegistry:
    global _default_registry_instance
    if _default_registry_instance is None:
        from persistence_kit.settings import PersistenceKitSettings

        _default_registry_instance = RestClientRegistry(settings=PersistenceKitSettings())
    return _default_registry_instance


def set_rest_registry_initializer(fn: Callable[[], None] | None) -> None:
    global _registry_initializer
    _registry_initializer = fn


@lru_cache(maxsize=1)
def _init_registry() -> None:
    if _registry_initializer is not None:
        _registry_initializer()


def register_rest_service(
    name: str,
    *,
    base_url: str = "",
    authenticator: Authenticator | None = None,
    resolver: EndpointResolver | None = None,
    config: ServiceConfig | None = None,
    retry_policy: RetryPolicy | None = None,
    circuit_breaker: CircuitBreaker | None = None,
    on_cache_change: Callable[[str], None] | None = None,
    replace: bool = False,
) -> None:
    default_rest_registry().register(
        name,
        base_url=base_url,
        authenticator=authenticator,
        resolver=resolver,
        config=config,
        retry_policy=retry_policy,
        circuit_breaker=circuit_breaker,
        on_cache_change=on_cache_change,
        replace=replace,
    )


def get_rest_client(name: str) -> RestClient:
    _init_registry()
    return default_rest_registry().get(name)


def provide_rest_client(name: str) -> Callable[[], RestClient]:
    def _dep() -> RestClient:
        return get_rest_client(name)

    return _dep


def reset_rest_registry() -> None:
    global _default_registry_instance
    _init_registry.cache_clear()
    _default_registry_instance = None
