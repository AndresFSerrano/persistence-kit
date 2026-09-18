from __future__ import annotations

from email.message import EmailMessage

from persistence_kit.mail.message import MailAddress, MailMessage
from persistence_kit.mail.mime import build_mime_message

DEFAULT_MEMORY_SENDER = MailAddress(email="no-reply@localhost.localdomain")


class MemoryMailSender:
    """Keeps every message in process instead of delivering it. Used by tests and local runs.

    It still builds the MIME message, so a message the relay would reject fails here too.
    """

    def __init__(self, *, default_sender: MailAddress = DEFAULT_MEMORY_SENDER) -> None:
        self._default_sender = default_sender
        self.sent: list[MailMessage] = []
        self.mime: list[EmailMessage] = []

    async def send(self, message: MailMessage) -> None:
        self.mime.append(build_mime_message(message, default_sender=self._default_sender))
        self.sent.append(message)

    def clear(self) -> None:
        self.sent.clear()
        self.mime.clear()
