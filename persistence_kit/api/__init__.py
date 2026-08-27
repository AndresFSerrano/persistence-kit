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
from .sealed_routes import (
    KEY_HEADER,
    SEALED_FLAG,
    build_sealed_route,
    declare_key_header,
    sealed,
)

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
