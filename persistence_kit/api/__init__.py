from .common import ApiError, pagination_params
from .error_handlers import handle_repository_errors, handle_service_errors
from .exceptions import (
    BaseAPIException,
    BusinessRuleException,
    DatabaseException,
    NotFoundException,
    SealedPayloadError,
    ValidationException,
)
from .rate_limit import InMemoryRateLimiter, build_auth_rate_limit_dependency
from .route_loader import build_api_router

_LAZY = {
    "KEY_HEADER": ("persistence_kit.api.sealed_routes", "KEY_HEADER"),
    "SEALED_FLAG": ("persistence_kit.api.sealed_routes", "SEALED_FLAG"),
    "build_sealed_route": ("persistence_kit.api.sealed_routes", "build_sealed_route"),
    "declare_key_header": ("persistence_kit.api.sealed_routes", "declare_key_header"),
    "sealed": ("persistence_kit.api.sealed_routes", "sealed"),
}

__all__ = [
    "ApiError",
    "pagination_params",
    "BaseAPIException",
    "NotFoundException",
    "ValidationException",
    "BusinessRuleException",
    "DatabaseException",
    "SealedPayloadError",
    "handle_service_errors",
    "handle_repository_errors",
    "build_api_router",
    "InMemoryRateLimiter",
    "build_auth_rate_limit_dependency",
    "build_sealed_route",
    "sealed",
    "declare_key_header",
    "SEALED_FLAG",
    "KEY_HEADER",
]


def __getattr__(name: str):
    if name in _LAZY:
        from importlib import import_module

        module_name, attr = _LAZY[name]
        value = getattr(import_module(module_name), attr)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
