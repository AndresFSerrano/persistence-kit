from __future__ import annotations

import asyncio
import smtplib
import ssl
from email.message import EmailMessage

from persistence_kit.mail.errors import MailDeliveryError, MailRecipientsRefusedError
from persistence_kit.mail.message import MailAddress, MailMessage
from persistence_kit.mail.mime import build_mime_message


class SmtpMailSender:
    """Delivers through an SMTP relay, upgrading with STARTTLS and logging in only when given a user.

    `smtplib` blocks, so each delivery runs in a worker thread and never stalls the event loop.
    """

    def __init__(
        self,
        *,
        host: str,
        default_sender: MailAddress,
        port: int = 587,
        starttls: bool = True,
        username: str | None = None,
        password: str | None = None,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._host = host
        self._port = port
        self._default_sender = default_sender
        self._starttls = starttls
        self._username = username
        self._password = password
        self._timeout_seconds = timeout_seconds

    async def send(self, message: MailMessage) -> None:
        mime = build_mime_message(message, default_sender=self._default_sender)
        await asyncio.to_thread(self._deliver, mime, message.envelope_recipients)

    def _deliver(self, mime: EmailMessage, recipients: tuple[str, ...]) -> None:
        try:
            with smtplib.SMTP(self._host, self._port, timeout=self._timeout_seconds) as client:
                client.ehlo()
                if self._starttls:
                    client.starttls(context=ssl.create_default_context())
                    client.ehlo()
                if self._username:
                    client.login(self._username, self._password or "")
                refused = client.send_message(mime, to_addrs=list(recipients))
        except (smtplib.SMTPException, OSError) as exc:
            raise MailDeliveryError(
                f"Could not deliver through {self._host}:{self._port}: {exc}"
            ) from exc
        if refused:
            raise MailRecipientsRefusedError(refused)
