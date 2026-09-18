from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from typing import Any

from persistence_kit.mail.contracts import MailSender
from persistence_kit.mail.message import MailAddress, MailAttachment, MailMessage, Recipients
from persistence_kit.mail.templates import MailTemplates

logger = logging.getLogger(__name__)


class Mailer:
    """Composes messages from the application's templates and sends them.

    `compose` raises on a broken template, so it shows up in tests. `dispatch` delivers in the
    background: a relay failure is logged and never reaches the request that triggered it.
    """

    def __init__(self, *, sender: MailSender, templates: MailTemplates) -> None:
        self._sender = sender
        self._templates = templates
        self._pending: set[asyncio.Task[None]] = set()

    def compose(
        self,
        template: str,
        *,
        to: Recipients = (),
        cc: Recipients = (),
        bcc: Recipients = (),
        reply_to: Recipients = (),
        context: Mapping[str, Any] | None = None,
        attachments: tuple[MailAttachment, ...] = (),
        sender: str | MailAddress | None = None,
    ) -> MailMessage:
        rendered = self._templates.render(template, context)
        return MailMessage(
            subject=rendered.subject,
            html=rendered.html,
            text=rendered.text,
            to=to,
            cc=cc,
            bcc=bcc,
            reply_to=reply_to,
            attachments=(*rendered.inline_images, *attachments),
            sender=sender,
        )

    async def send(self, message: MailMessage) -> None:
        await self._sender.send(message)

    def dispatch(self, message: MailMessage) -> asyncio.Task[None]:
        task = asyncio.create_task(self._send_logging_failures(message))
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)
        return task

    async def wait_for_pending(self) -> None:
        """Waits for every dispatched message, for tests and for a clean shutdown."""
        if self._pending:
            await asyncio.gather(*self._pending, return_exceptions=True)

    async def _send_logging_failures(self, message: MailMessage) -> None:
        try:
            await self._sender.send(message)
        except Exception:
            logger.exception(
                "mail.dispatch_failed subject=%r recipients=%d",
                message.subject,
                len(message.envelope_recipients),
            )
