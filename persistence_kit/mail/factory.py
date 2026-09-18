from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING

from persistence_kit.mail.contracts import MailSender
from persistence_kit.mail.errors import MailConfigError
from persistence_kit.mail.message import MailAddress
from persistence_kit.settings.mail_settings import MailBackend, MailSettings

if TYPE_CHECKING:
    from persistence_kit.mail.mailer import Mailer


def _configured_sender(settings: MailSettings) -> MailAddress | None:
    if not settings.mail_from_address:
        return None
    return MailAddress(email=settings.mail_from_address, name=settings.mail_from_name)


@lru_cache
def _memory_sender_cached(default_sender: MailAddress | None) -> MailSender:
    from persistence_kit.mail.memory import MemoryMailSender

    if default_sender is None:
        return MemoryMailSender()
    return MemoryMailSender(default_sender=default_sender)


@lru_cache
def _smtp_sender_cached(
    host: str,
    port: int,
    starttls: bool,
    username: str | None,
    password: str | None,
    timeout_seconds: float,
    default_sender: MailAddress,
) -> MailSender:
    from persistence_kit.mail.smtp import SmtpMailSender

    return SmtpMailSender(
        host=host,
        port=port,
        starttls=starttls,
        username=username,
        password=password,
        timeout_seconds=timeout_seconds,
        default_sender=default_sender,
    )


def get_mail_sender(settings: MailSettings | None = None) -> MailSender:
    """Returns the sender chosen by `MAIL_BACKEND`; `smtp` requires a host and a from address."""
    settings = settings or MailSettings()
    default_sender = _configured_sender(settings)

    if settings.mail_backend == MailBackend.MEMORY:
        return _memory_sender_cached(default_sender)

    if settings.mail_backend == MailBackend.SMTP:
        if not settings.mail_smtp_host:
            raise MailConfigError("MAIL_SMTP_HOST is required when MAIL_BACKEND is smtp.")
        if default_sender is None:
            raise MailConfigError("MAIL_FROM_ADDRESS is required when MAIL_BACKEND is smtp.")
        return _smtp_sender_cached(
            settings.mail_smtp_host,
            settings.mail_smtp_port,
            settings.mail_smtp_starttls,
            settings.mail_smtp_username,
            settings.mail_smtp_password,
            settings.mail_smtp_timeout_seconds,
            default_sender,
        )

    backend = getattr(settings.mail_backend, "value", settings.mail_backend)
    raise MailConfigError(f"Unsupported mail backend: '{backend}'.")


def build_mailer(templates_dir: str | Path, settings: MailSettings | None = None) -> Mailer:
    """A `Mailer` over the configured sender and the application's template directory."""
    from persistence_kit.mail.mailer import Mailer
    from persistence_kit.mail.templates import MailTemplates

    return Mailer(sender=get_mail_sender(settings), templates=MailTemplates(templates_dir))


def reset_mail_sender() -> None:
    _memory_sender_cached.cache_clear()
    _smtp_sender_cached.cache_clear()
