from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from email.utils import formataddr

from persistence_kit.mail.errors import MailMessageError

_LINE_BREAKS = ("\r", "\n")
CONTENT_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._@-]*")


def _reject_line_breaks(value: str, what: str) -> None:
    for line_break in _LINE_BREAKS:
        if line_break in value:
            raise MailMessageError(f"{what} cannot contain line breaks: {value!r}")


@dataclass(frozen=True, kw_only=True)
class MailAddress:
    """An address with an optional display name."""

    email: str
    name: str | None = None

    def __post_init__(self) -> None:
        email = self.email.strip()
        if not email or "@" not in email:
            raise MailMessageError(f"Not a mail address: {self.email!r}")
        _reject_line_breaks(email, "An address")
        if self.name is not None:
            _reject_line_breaks(self.name, "A display name")
        object.__setattr__(self, "email", email)

    def formatted(self) -> str:
        return formataddr((self.name or "", self.email))


Recipients = str | MailAddress | Iterable[str | MailAddress]


def to_address(value: str | MailAddress) -> MailAddress:
    if isinstance(value, MailAddress):
        return value
    return MailAddress(email=value)


def to_addresses(value: Recipients | None) -> tuple[MailAddress, ...]:
    if value is None:
        return ()
    if isinstance(value, (str, MailAddress)):
        return (to_address(value),)
    addresses: list[MailAddress] = []
    for item in value:
        addresses.append(to_address(item))
    return tuple(addresses)


@dataclass(frozen=True, kw_only=True)
class MailAttachment:
    """A file sent with the message. With `content_id` it goes inline, for `<img src="cid:...">`."""

    filename: str
    content: bytes
    content_type: str = "application/octet-stream"
    content_id: str | None = None

    def __post_init__(self) -> None:
        _reject_line_breaks(self.filename, "An attachment name")
        if self.content_type.count("/") != 1:
            raise MailMessageError(f"Not a content type: {self.content_type!r}")
        if self.content_id is not None and not CONTENT_ID_PATTERN.fullmatch(self.content_id):
            raise MailMessageError(f"Not a content id: {self.content_id!r}")

    @property
    def inline(self) -> bool:
        return self.content_id is not None


@dataclass(frozen=True, kw_only=True)
class MailMessage:
    """One message. `to`, `cc`, `bcc` and `reply_to` take a string, an address or a list of them.

    Blind copies travel in the envelope only; they never appear in the headers.
    """

    subject: str
    to: Recipients = ()
    cc: Recipients = ()
    bcc: Recipients = ()
    reply_to: Recipients = ()
    html: str | None = None
    text: str | None = None
    attachments: tuple[MailAttachment, ...] = field(default_factory=tuple)
    sender: str | MailAddress | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "to", to_addresses(self.to))
        object.__setattr__(self, "cc", to_addresses(self.cc))
        object.__setattr__(self, "bcc", to_addresses(self.bcc))
        object.__setattr__(self, "reply_to", to_addresses(self.reply_to))
        object.__setattr__(self, "attachments", tuple(self.attachments))
        if self.sender is not None:
            object.__setattr__(self, "sender", to_address(self.sender))

        _reject_line_breaks(self.subject, "The subject")
        if not self.to and not self.cc and not self.bcc:
            raise MailMessageError("A message needs at least one recipient in to, cc or bcc.")
        if not self.html and not self.text:
            raise MailMessageError("A message needs an html or a text body.")
        if not self.html and any(attachment.inline for attachment in self.attachments):
            raise MailMessageError("Inline attachments need an html body.")

    @property
    def envelope_recipients(self) -> tuple[str, ...]:
        """Every address the relay has to deliver to, blind copies included, without repeats."""
        seen: dict[str, str] = {}
        for address in (*self.to, *self.cc, *self.bcc):
            seen.setdefault(address.email.lower(), address.email)
        return tuple(seen.values())
