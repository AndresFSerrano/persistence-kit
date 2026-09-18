from __future__ import annotations

from typing import Protocol

from persistence_kit.mail.message import MailMessage


class MailSender(Protocol):
    async def send(self, message: MailMessage) -> None: ...
