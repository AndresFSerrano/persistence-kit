from enum import Enum

from pydantic_settings import BaseSettings, SettingsConfigDict


class MailBackend(str, Enum):
    MEMORY = "memory"
    SMTP = "smtp"


class MailSettings(BaseSettings):
    """Outbound mail. `memory` keeps messages in process; `smtp` delivers through a relay."""

    mail_backend: MailBackend = MailBackend.MEMORY
    mail_from_address: str | None = None
    mail_from_name: str | None = None
    mail_smtp_host: str | None = None
    mail_smtp_port: int = 587
    mail_smtp_starttls: bool = True
    mail_smtp_username: str | None = None
    mail_smtp_password: str | None = None
    mail_smtp_timeout_seconds: float = 30.0

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
