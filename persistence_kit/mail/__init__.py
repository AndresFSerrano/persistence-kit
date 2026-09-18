from importlib import import_module

from persistence_kit.mail.contracts import MailSender
from persistence_kit.mail.errors import (
    MailConfigError,
    MailDeliveryError,
    MailError,
    MailMessageError,
    MailRecipientsRefusedError,
    MailTemplateError,
)
from persistence_kit.mail.factory import build_mailer, get_mail_sender, reset_mail_sender
from persistence_kit.mail.message import MailAddress, MailAttachment, MailMessage
from persistence_kit.settings.mail_settings import MailBackend, MailSettings

_LAZY_EXPORTS = {
    "MemoryMailSender": ("persistence_kit.mail.memory", "MemoryMailSender"),
    "SmtpMailSender": ("persistence_kit.mail.smtp", "SmtpMailSender"),
    "MailTemplates": ("persistence_kit.mail.templates", "MailTemplates"),
    "RenderedMail": ("persistence_kit.mail.templates", "RenderedMail"),
    "Mailer": ("persistence_kit.mail.mailer", "Mailer"),
}

__all__ = [
    "MailSender",
    "MailAddress",
    "MailAttachment",
    "MailMessage",
    "MailError",
    "MailConfigError",
    "MailMessageError",
    "MailTemplateError",
    "MailDeliveryError",
    "MailRecipientsRefusedError",
    "MailBackend",
    "MailSettings",
    "get_mail_sender",
    "build_mailer",
    "reset_mail_sender",
    *_LAZY_EXPORTS,
]


def __getattr__(name: str):
    if name not in _LAZY_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = _LAZY_EXPORTS[name]
    try:
        module = import_module(module_name)
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            f"{name} requires the optional capability 'mail'. "
            "Install it with: pip install 'persistence-kit[mail]'"
        ) from exc
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
