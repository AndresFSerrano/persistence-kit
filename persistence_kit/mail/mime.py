from __future__ import annotations

from email.message import EmailMessage
from email.utils import formatdate, make_msgid

from persistence_kit.mail.message import MailAddress, MailMessage


def _join(addresses: tuple[MailAddress, ...]) -> str:
    formatted: list[str] = []
    for address in addresses:
        formatted.append(address.formatted())
    return ", ".join(formatted)


def build_mime_message(message: MailMessage, *, default_sender: MailAddress) -> EmailMessage:
    """Turns a message into MIME. Bcc is left out of the headers on purpose."""
    sender = message.sender or default_sender
    mime = EmailMessage()
    mime["From"] = sender.formatted()
    if message.to:
        mime["To"] = _join(message.to)
    if message.cc:
        mime["Cc"] = _join(message.cc)
    if message.reply_to:
        mime["Reply-To"] = _join(message.reply_to)
    mime["Subject"] = message.subject
    mime["Date"] = formatdate(localtime=True)
    mime["Message-ID"] = make_msgid(domain=sender.email.rsplit("@", 1)[1])

    html_part = mime
    if message.text and message.html:
        mime.set_content(message.text)
        mime.add_alternative(message.html, subtype="html")
        html_part = mime.get_payload()[1]
    elif message.html:
        mime.set_content(message.html, subtype="html")
    else:
        mime.set_content(message.text or "")

    for attachment in message.attachments:
        if not attachment.inline:
            continue
        maintype, subtype = attachment.content_type.split("/")
        html_part.add_related(
            attachment.content,
            maintype=maintype,
            subtype=subtype,
            cid=f"<{attachment.content_id}>",
            disposition="inline",
            filename=attachment.filename,
        )

    for attachment in message.attachments:
        if attachment.inline:
            continue
        maintype, subtype = attachment.content_type.split("/")
        mime.add_attachment(
            attachment.content,
            maintype=maintype,
            subtype=subtype,
            filename=attachment.filename,
        )
    return mime
