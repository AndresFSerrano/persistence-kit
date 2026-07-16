from enum import Enum

from pydantic import field_validator, model_validator
from pydantic_settings import SettingsConfigDict

from persistence_kit.settings.repo_settings import RepoSettings
from persistence_kit.settings.parsers import parse_str_map, split_csv_list


class AuthProvider(str, Enum):
    MEMORY = "memory"
    COGNITO = "cognito"


class ExportStorageProvider(str, Enum):
    LOCAL = "local"
    S3 = "s3"


class MediaStorageProvider(str, Enum):
    LOCAL = "local"
    S3 = "s3"


class DeploymentStage(str, Enum):
    LOCAL = "local"
    DEV = "dev"
    PRODUCTION = "production"


LOCAL_DEFAULT_JOB_SERVICE_API_KEY = "local-job-service-dev-key-32-bytes"
DEFAULT_MEMORY_JWT_SECRET = "memory-local-secret-not-for-production-12345"
DEFAULT_MEMORY_JWT_ISSUER = "memory-sandbox"
DEFAULT_MEMORY_JWT_TTL_SECONDS = 3600


class PersistenceKitSettings(RepoSettings):
    stage: DeploymentStage = DeploymentStage.LOCAL

    # Auth
    auth_enabled: bool = False
    auth_provider: AuthProvider = AuthProvider.MEMORY
    memory_jwt_secret: str = DEFAULT_MEMORY_JWT_SECRET
    memory_jwt_issuer: str = DEFAULT_MEMORY_JWT_ISSUER
    memory_jwt_ttl_seconds: int = DEFAULT_MEMORY_JWT_TTL_SECONDS
    memory_seed_user_password: str = "Temporal123!"
    memory_seed_role_codes: tuple[str, ...] = ()
    memory_seed_user_domain: str = "example.com"
    memory_seed_created_by: str | None = None
    auth_rate_limit_enabled: bool | None = None
    auth_rate_limit_window_seconds: int = 300
    auth_login_rate_limit: int = 10
    auth_refresh_rate_limit: int = 30
    auth_password_rate_limit: int = 5

    # Export Storage
    export_storage_provider: ExportStorageProvider = ExportStorageProvider.LOCAL
    local_export_storage_dir: str = ".local"
    local_export_url_secret: str = "local-export-dev-secret-key-32-bytes"

    # Media Storage (user-uploaded assets like product images, avatars, etc.)
    media_storage_provider: MediaStorageProvider = MediaStorageProvider.LOCAL
    local_media_storage_dir: str = ".local/media"
    aws_s3_media_bucket: str | None = None

    # Cognito
    cognito_user_pool_id: str | None = None
    cognito_app_client_id: str | None = None
    cognito_app_client_secret: str | None = None
    cognito_allowed_email_domain: str = "udea.edu.co"

    # Service
    service_name: str = "api"
    service_version: str = "0.1.0"

    # Observability
    observability_enabled: bool = True
    log_level: str = "INFO"
    metrics_enabled: bool = True
    metrics_endpoint: str = "/metrics"
    otlp_traces_endpoint: str = "http://tempo:4318/v1/traces"

    # Job Service
    public_base_url: str | None = None
    healthcheck_path: str = "/api/v1/health"
    job_service_enabled: bool = False
    job_service_url: str | None = None
    job_service_api_key: str = LOCAL_DEFAULT_JOB_SERVICE_API_KEY
    inngest_dashboard_url: str | None = None

    # AWS
    aws_region: str = "us-east-1"
    aws_s3_export_bucket: str | None = None

    # REST client (outbound)
    rest_default_timeout_seconds: float = 10.0
    rest_default_max_retries: int = 2
    rest_default_verify_tls: bool = True
    rest_default_user_agent: str | None = None
    rest_service_urls: dict[str, str] = {}

    model_config = SettingsConfigDict(extra="ignore")

    @property
    def is_local_stage(self) -> bool:
        return self.stage == DeploymentStage.LOCAL

    @property
    def docs_enabled(self) -> bool:
        return self.is_local_stage

    @property
    def seed_quick_access_enabled(self) -> bool:
        return (
            self.is_local_stage
            and self.auth_enabled
            and self.auth_provider == AuthProvider.MEMORY
        )

    @model_validator(mode="after")
    def resolve_rate_limit_default(self) -> "PersistenceKitSettings":
        if self.auth_rate_limit_enabled is None:
            self.auth_rate_limit_enabled = not self.is_local_stage
        return self

    @field_validator("memory_seed_role_codes", mode="before")
    def split_memory_seed_role_codes(cls, value):
        if value is None:
            return ()
        if isinstance(value, str):
            return tuple(split_csv_list(value) or ())
        return value

    @field_validator("rest_service_urls", mode="before")
    def parse_rest_service_urls(cls, value):
        return parse_str_map(value)
