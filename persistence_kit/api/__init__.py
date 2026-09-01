from .common import ApiError, pagination_params
from .error_handlers import handle_repository_errors, handle_service_errors
from .exceptions import (
    BaseAPIException,
    BusinessRuleException,
    DatabaseException,
    NotFoundException,
    ValidationException,
)
from .rate_limit import InMemoryRateLimiter, build_auth_rate_limit_dependency
from .route_loader import build_api_router

_LAZY = {
    "KEY_HEADER": ("persistence_kit.api.encrypted_routes", "KEY_HEADER"),
    "ENCRYPTED_FLAG": ("persistence_kit.api.encrypted_routes", "ENCRYPTED_FLAG"),
    "build_encrypted_route": ("persistence_kit.api.encrypted_routes", "build_encrypted_route"),
    "declare_key_header": ("persistence_kit.api.encrypted_routes", "declare_key_header"),
    "encrypted": ("persistence_kit.api.encrypted_routes", "encrypted"),
    "EncryptedResponseMiddleware": (
        "persistence_kit.api.encrypted_middleware",
        "EncryptedResponseMiddleware",
    ),
}

__all__ = [
    "ApiError",
    "pagination_params",
    "BaseAPIException",
    "NotFoundException",
    "ValidationException",
    "BusinessRuleException",
    "DatabaseException",
    "handle_service_errors",
    "handle_repository_errors",
    "build_api_router",
    "InMemoryRateLimiter",
    "build_auth_rate_limit_dependency",
    "build_encrypted_route",
    "EncryptedResponseMiddleware",
    "encrypted",
    "declare_key_header",
    "ENCRYPTED_FLAG",
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
