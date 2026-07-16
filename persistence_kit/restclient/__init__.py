from persistence_kit.restclient.auth import (
    ApiKeyAuth,
    ApiKeyLocation,
    BasicAuth,
    BearerAuth,
    LoginTokenAuth,
    NoAuth,
    OAuth2ClientCredentials,
)
from persistence_kit.resilience import CircuitBreaker, CircuitState
from persistence_kit.restclient.config import ServiceConfig
from persistence_kit.restclient.contracts import (
    Authenticator,
    EndpointResolver,
    RestClient,
    RestRequest,
    RestResponse,
)
from persistence_kit.restclient.errors import (
    RestAuthError,
    RestCircuitOpenError,
    RestClientError,
    RestConfigError,
    RestHTTPError,
    RestTimeoutError,
    RestTransportError,
)
from persistence_kit.restclient.aggregate import (
    Expansion,
    expand,
    expand_chain,
    expand_recursive,
)
from persistence_kit.restclient.factory import build_rest_client
from persistence_kit.restclient.mapping import (
    ModelMappingMixin,
    decode,
    encode,
)
from persistence_kit.restclient.memory import MemoryRestClient
from persistence_kit.restclient.populate import (
    RestRelation,
    RestRelationRegistry,
    populate,
)
from persistence_kit.restclient.payload import serialize_xml
from persistence_kit.restclient.registry import RegisteredService, RestClientRegistry
from persistence_kit.restclient.resolver import (
    DirectoryEndpointResolver,
    StaticEndpointResolver,
)
from persistence_kit.restclient.retry import RetryPolicy

__all__ = [
    "RestClient",
    "RestRequest",
    "RestResponse",
    "Authenticator",
    "EndpointResolver",
    "ServiceConfig",
    "StaticEndpointResolver",
    "DirectoryEndpointResolver",
    "serialize_xml",
    "RetryPolicy",
    "CircuitBreaker",
    "CircuitState",
    "decode",
    "encode",
    "ModelMappingMixin",
    "expand",
    "expand_chain",
    "expand_recursive",
    "Expansion",
    "populate",
    "RestRelation",
    "RestRelationRegistry",
    "NoAuth",
    "ApiKeyAuth",
    "ApiKeyLocation",
    "BearerAuth",
    "BasicAuth",
    "OAuth2ClientCredentials",
    "LoginTokenAuth",
    "MemoryRestClient",
    "RestClientRegistry",
    "RegisteredService",
    "build_rest_client",
    "RestClientError",
    "RestConfigError",
    "RestAuthError",
    "RestTimeoutError",
    "RestTransportError",
    "RestCircuitOpenError",
    "RestHTTPError",
    "HttpxRestClient",
]


def __getattr__(name: str):
    if name == "HttpxRestClient":
        from persistence_kit.restclient.client import HttpxRestClient

        globals()[name] = HttpxRestClient
        return HttpxRestClient
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
