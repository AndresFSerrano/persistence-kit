from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

DEFAULT_TIMEOUT_SECONDS = 10.0
DEFAULT_MAX_RETRIES = 2


class ServiceConfig(BaseModel):
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    max_retries: int = DEFAULT_MAX_RETRIES
    verify_tls: bool = True
    raise_for_status: bool = True
    default_headers: dict[str, str] = Field(default_factory=dict)
    user_agent: str | None = None

    @classmethod
    def from_settings(cls, settings: Any, **overrides: Any) -> "ServiceConfig":
        base: dict[str, Any] = {
            "timeout_seconds": getattr(
                settings, "rest_default_timeout_seconds", DEFAULT_TIMEOUT_SECONDS
            ),
            "max_retries": getattr(
                settings, "rest_default_max_retries", DEFAULT_MAX_RETRIES
            ),
            "verify_tls": getattr(settings, "rest_default_verify_tls", True),
            "user_agent": getattr(settings, "rest_default_user_agent", None),
        }
        base.update(overrides)
        return cls(**base)
