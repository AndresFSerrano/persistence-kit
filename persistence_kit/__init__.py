from importlib import import_module

from .bootstrap.configuration import ConfigRegistry, configuration, set_config_package
from .bootstrap.seeders import Seeder, SeederProvider
from .bootstrap.startup import is_duplicate_startup_error, run_startup_bootstrap
from .contracts.repository import Repository
from .contracts.view_repository import ViewRepository
from .settings import (
    DEFAULT_MEMORY_JWT_ISSUER,
    DEFAULT_MEMORY_JWT_SECRET,
    DEFAULT_MEMORY_JWT_TTL_SECONDS,
    LOCAL_DEFAULT_JOB_SERVICE_API_KEY,
    AuthProvider,
    DeploymentStage,
    ExportStorageProvider,
    MediaStorageProvider,
    PersistenceKitSettings,
)
from .settings.constants import Database
from .settings.parsers import split_csv_list
from .settings.repo_settings import RepoSettings
from .storage import (
    EXPORT_STORAGE_BUILDERS,
    ExportStorage,
    ExportStorageBuilder,
    ObjectStorage,
    StorageConfigError,
    StorageError,
    StoragePathError,
    StoragePresignError,
    StorageUploadError,
    get_export_storage,
)
from .authenticated_user import AuthenticatedUser
from .resilience import CircuitBreaker, CircuitOpenError, CircuitState
from .utils.upsert import dataclass_field_names, upsert_entity

_OPTIONAL_EXPORTS = {
    "ApiError": ("persistence_kit.api.common", "ApiError", "api"),
    "pagination_params": ("persistence_kit.api.common", "pagination_params", "api"),
    "BaseAPIException": ("persistence_kit.api.exceptions", "BaseAPIException", "api"),
    "NotFoundException": ("persistence_kit.api.exceptions", "NotFoundException", "api"),
    "ValidationException": ("persistence_kit.api.exceptions", "ValidationException", "api"),
    "BusinessRuleException": (
        "persistence_kit.api.exceptions",
        "BusinessRuleException",
        "api",
    ),
    "DatabaseException": ("persistence_kit.api.exceptions", "DatabaseException", "api"),
    "handle_service_errors": (
        "persistence_kit.api.error_handlers",
        "handle_service_errors",
        "api",
    ),
    "handle_repository_errors": (
        "persistence_kit.api.error_handlers",
        "handle_repository_errors",
        "api",
    ),
    "build_api_router": ("persistence_kit.api.route_loader", "build_api_router", "api"),
    "InMemoryRateLimiter": ("persistence_kit.api.rate_limit", "InMemoryRateLimiter", "api"),
    "build_auth_rate_limit_dependency": (
        "persistence_kit.api.rate_limit",
        "build_auth_rate_limit_dependency",
        "api",
    ),
    "IdentityProvider": ("persistence_kit.security.ports", "IdentityProvider", "security"),
    "TokenVerifier": ("persistence_kit.security.ports", "TokenVerifier", "security"),
    "CognitoIdentityProvider": (
        "persistence_kit.security.providers",
        "CognitoIdentityProvider",
        "security-cognito",
    ),
    "MemorySecurityProvider": (
        "persistence_kit.security.providers",
        "MemorySecurityProvider",
        "testing",
    ),
    "CognitoJwtVerifier": (
        "persistence_kit.security.token_verifiers",
        "CognitoJwtVerifier",
        "security-cognito",
    ),
    "MemoryJwtVerifier": (
        "persistence_kit.security.token_verifiers",
        "MemoryJwtVerifier",
        "testing",
    ),
    "get_identity_provider": (
        "persistence_kit.security.factory",
        "get_identity_provider",
        "security",
    ),
    "get_token_verifier": (
        "persistence_kit.security.factory",
        "get_token_verifier",
        "security",
    ),
    "RegistrationResult": (
        "persistence_kit.security.registration",
        "RegistrationResult",
        "security",
    ),
    "RoleAssignmentResult": (
        "persistence_kit.security.registration",
        "RoleAssignmentResult",
        "security",
    ),
    "UserStatusUpdateResult": (
        "persistence_kit.security.registration",
        "UserStatusUpdateResult",
        "security",
    ),
    "LoginResult": ("persistence_kit.security.registration", "LoginResult", "security"),
    "RefreshTokensResult": (
        "persistence_kit.security.registration",
        "RefreshTokensResult",
        "security",
    ),
    "LogoutResult": ("persistence_kit.security.registration", "LogoutResult", "security"),
    "PasswordResetCodeRequestResult": (
        "persistence_kit.security.registration",
        "PasswordResetCodeRequestResult",
        "security",
    ),
    "PasswordResetConfirmResult": (
        "persistence_kit.security.registration",
        "PasswordResetConfirmResult",
        "security",
    ),
    "PasswordResetResult": (
        "persistence_kit.security.registration",
        "PasswordResetResult",
        "security",
    ),
    "RegisteredUserResult": (
        "persistence_kit.security.registration",
        "RegisteredUserResult",
        "security",
    ),
    "RegisteredUsersPageResult": (
        "persistence_kit.security.registration",
        "RegisteredUsersPageResult",
        "security",
    ),
    "validate_allowed_email_domain": (
        "persistence_kit.security.registration",
        "validate_allowed_email_domain",
        "security",
    ),
    "unique_roles": ("persistence_kit.security.registration", "unique_roles", "security"),
    "LocalObjectStorage": ("persistence_kit.storage.local", "LocalObjectStorage", "testing"),
    "LocalExportStorageProvider": (
        "persistence_kit.storage.local",
        "LocalExportStorageProvider",
        "testing",
    ),
    "S3ObjectStorage": ("persistence_kit.storage.s3", "S3ObjectStorage", "storage-s3"),
    "S3ExportStorageProvider": (
        "persistence_kit.storage.s3",
        "S3ExportStorageProvider",
        "storage-s3",
    ),
    "build_local_export_storage_router": (
        "persistence_kit.storage.routes",
        "build_local_export_storage_router",
        "storage-routes",
    ),
    "serve_local_export": (
        "persistence_kit.storage.routes",
        "serve_local_export",
        "storage-routes",
    ),
    "guess_export_media_type": (
        "persistence_kit.storage.routes",
        "guess_export_media_type",
        "storage-routes",
    ),
    "build_rest_client": (
        "persistence_kit.restclient.factory",
        "build_rest_client",
        "restclient",
    ),
    "RestClientRegistry": (
        "persistence_kit.restclient.registry",
        "RestClientRegistry",
        "restclient",
    ),
    "HttpxRestClient": (
        "persistence_kit.restclient.client",
        "HttpxRestClient",
        "restclient",
    ),
    "MemoryRestClient": (
        "persistence_kit.restclient.memory",
        "MemoryRestClient",
        "restclient",
    ),
    "ServiceConfig": (
        "persistence_kit.restclient.config",
        "ServiceConfig",
        "restclient",
    ),
    "RetryPolicy": (
        "persistence_kit.restclient.retry",
        "RetryPolicy",
        "restclient",
    ),
    "StaticEndpointResolver": (
        "persistence_kit.restclient.resolver",
        "StaticEndpointResolver",
        "restclient",
    ),
    "DirectoryEndpointResolver": (
        "persistence_kit.restclient.resolver",
        "DirectoryEndpointResolver",
        "restclient",
    ),
    "decode": ("persistence_kit.restclient.mapping", "decode", "restclient"),
    "encode": ("persistence_kit.restclient.mapping", "encode", "restclient"),
    "NoAuth": ("persistence_kit.restclient.auth", "NoAuth", "restclient"),
    "ApiKeyAuth": ("persistence_kit.restclient.auth", "ApiKeyAuth", "restclient"),
    "BearerAuth": ("persistence_kit.restclient.auth", "BearerAuth", "restclient"),
    "BasicAuth": ("persistence_kit.restclient.auth", "BasicAuth", "restclient"),
    "OAuth2ClientCredentials": (
        "persistence_kit.restclient.auth",
        "OAuth2ClientCredentials",
        "restclient",
    ),
    "LoginTokenAuth": (
        "persistence_kit.restclient.auth",
        "LoginTokenAuth",
        "restclient",
    ),
    "RestClient": ("persistence_kit.restclient.contracts", "RestClient", "restclient"),
    "Authenticator": (
        "persistence_kit.restclient.contracts",
        "Authenticator",
        "restclient",
    ),
    "EndpointResolver": (
        "persistence_kit.restclient.contracts",
        "EndpointResolver",
        "restclient",
    ),
    "RestClientError": (
        "persistence_kit.restclient.errors",
        "RestClientError",
        "restclient",
    ),
    "RestHTTPError": (
        "persistence_kit.restclient.errors",
        "RestHTTPError",
        "restclient",
    ),
}

__all__ = [
    "Repository",
    "ViewRepository",
    "split_csv_list",
    "ConfigRegistry",
    "configuration",
    "Database",
    "RepoSettings",
    "AuthProvider",
    "ExportStorageProvider",
    "MediaStorageProvider",
    "DeploymentStage",
    "LOCAL_DEFAULT_JOB_SERVICE_API_KEY",
    "DEFAULT_MEMORY_JWT_SECRET",
    "DEFAULT_MEMORY_JWT_ISSUER",
    "DEFAULT_MEMORY_JWT_TTL_SECONDS",
    "PersistenceKitSettings",
    "Seeder",
    "SeederProvider",
    "set_config_package",
    "is_duplicate_startup_error",
    "run_startup_bootstrap",
    "AuthenticatedUser",
    "CircuitBreaker",
    "CircuitState",
    "CircuitOpenError",
    "dataclass_field_names",
    "upsert_entity",
    "ObjectStorage",
    "StorageError",
    "StorageConfigError",
    "StoragePathError",
    "StorageUploadError",
    "StoragePresignError",
    "ExportStorage",
    "ExportStorageBuilder",
    "EXPORT_STORAGE_BUILDERS",
    "get_export_storage",
    *_OPTIONAL_EXPORTS,
]


def __getattr__(name: str):
    if name not in _OPTIONAL_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module_name, attr_name, extra_name = _OPTIONAL_EXPORTS[name]
    try:
        module = import_module(module_name)
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            f"{name} requiere la capability opcional '{extra_name}'. "
            f"Instala con `persistence-kit[{extra_name}]`."
        ) from exc
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
