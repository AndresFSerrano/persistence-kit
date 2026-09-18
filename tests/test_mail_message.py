import email
from email import policy

import pytest

from persistence_kit.mail import MailAddress, MailAttachment, MailMessage, MailMessageError
from persistence_kit.mail.mime import build_mime_message

SENDER = MailAddress(email="prestamos@example.edu", name="Préstamos FCEN")


def test_recipients_accept_a_string_an_address_or_a_list():
    message = MailMessage(
        subject="Hola",
        text="body",
        to="ana@example.edu",
        cc=MailAddress(email="luis@example.edu", name="Luis"),
        bcc=["audit@example.edu", MailAddress(email="ops@example.edu")],
    )

    assert message.to == (MailAddress(email="ana@example.edu"),)
    assert message.cc == (MailAddress(email="luis@example.edu", name="Luis"),)
    assert [address.email for address in message.bcc] == ["audit@example.edu", "ops@example.edu"]


def test_a_message_needs_a_recipient():
    with pytest.raises(MailMessageError, match="at least one recipient"):
        MailMessage(subject="Hola", text="body")


def test_a_message_needs_a_body():
    with pytest.raises(MailMessageError, match="html or a text body"):
        MailMessage(subject="Hola", to="ana@example.edu")


def test_a_blind_copy_alone_is_enough_to_send():
    message = MailMessage(subject="Hola", text="body", bcc="audit@example.edu")

    assert message.envelope_recipients == ("audit@example.edu",)


def _subject_with_line_break():
    return MailMessage(subject="Hola\r\nBcc: victim@example.edu", text="x", to="a@example.edu")


def _address_with_line_break():
    return MailAddress(email="a@example.edu\nBcc: victim@example.edu")


def _display_name_with_line_break():
    return MailAddress(email="a@example.edu", name="Ana\r\nX-Injected: 1")


def _attachment_name_with_line_break():
    return MailAttachment(filename="report.csv\r\nX: 1", content=b"")


@pytest.mark.parametrize(
    "build",
    [
        _subject_with_line_break,
        _address_with_line_break,
        _display_name_with_line_break,
        _attachment_name_with_line_break,
    ],
)
def test_header_values_cannot_carry_line_breaks(build):
    with pytest.raises(MailMessageError, match="line breaks"):
        build()


def test_an_address_needs_an_at_sign():
    with pytest.raises(MailMessageError, match="Not a mail address"):
        MailAddress(email="not-an-address")


def test_an_attachment_needs_a_full_content_type():
    with pytest.raises(MailMessageError, match="Not a content type"):
        MailAttachment(filename="a.pdf", content=b"", content_type="pdf")


def test_envelope_includes_blind_copies_once_and_keeps_the_first_spelling():
    message = MailMessage(
        subject="Hola",
        text="body",
        to=["Ana@Example.edu", "luis@example.edu"],
        cc="ana@example.edu",
        bcc=["audit@example.edu", "LUIS@example.edu"],
    )

    assert message.envelope_recipients == (
        "Ana@Example.edu",
        "luis@example.edu",
        "audit@example.edu",
    )


def test_mime_headers_carry_to_cc_and_reply_to_but_never_bcc():
    message = MailMessage(
        subject="Cambio de roles",
        text="body",
        to=MailAddress(email="ana@example.edu", name="Ana"),
        cc="luis@example.edu",
        bcc="audit@example.edu",
        reply_to="soporte@example.edu",
    )

    mime = build_mime_message(message, default_sender=SENDER)

    assert mime["From"] == "Préstamos FCEN <prestamos@example.edu>"
    assert mime["To"] == "Ana <ana@example.edu>"
    assert mime["Cc"] == "luis@example.edu"
    assert mime["Reply-To"] == "soporte@example.edu"
    assert mime["Bcc"] is None
    assert b"audit@example.edu" not in mime.as_bytes()
    assert mime["Message-ID"].endswith("@example.edu>")


def test_a_message_can_override_the_default_sender():
    message = MailMessage(subject="Hola", text="x", to="a@example.edu", sender="otro@example.edu")

    mime = build_mime_message(message, default_sender=SENDER)

    assert mime["From"] == "otro@example.edu"


def test_text_and_html_travel_as_alternatives():
    message = MailMessage(subject="Hola", text="plain body", html="<p>html body</p>", to="a@example.edu")

    mime = build_mime_message(message, default_sender=SENDER)

    assert mime.get_content_type() == "multipart/alternative"
    assert mime.get_body(preferencelist=("plain",)).get_content().strip() == "plain body"
    assert mime.get_body(preferencelist=("html",)).get_content().strip() == "<p>html body</p>"


def test_html_alone_is_sent_as_html():
    message = MailMessage(subject="Hola", html="<p>solo html</p>", to="a@example.edu")

    mime = build_mime_message(message, default_sender=SENDER)

    assert mime.get_content_type() == "text/html"


def test_attachments_keep_their_name_type_and_bytes():
    message = MailMessage(
        subject="Respaldo",
        text="adjunto",
        to="a@example.edu",
        attachments=(MailAttachment(filename="respaldo.csv", content=b"a,b\n1,2\n", content_type="text/csv"),),
    )

    mime = build_mime_message(message, default_sender=SENDER)
    parsed = email.message_from_bytes(mime.as_bytes(), policy=policy.default)
    attachments = list(parsed.iter_attachments())

    assert len(attachments) == 1
    assert attachments[0].get_filename() == "respaldo.csv"
    assert attachments[0].get_content_type() == "text/csv"
    assert attachments[0].get_content().replace("\r\n", "\n") == "a,b\n1,2\n"


LOGO = MailAttachment(filename="key.png", content=b"\x89PNG-bytes", content_type="image/png", content_id="key.png")
REPORT = MailAttachment(filename="respaldo.csv", content=b"a,b\n", content_type="text/csv")


def _structure(part, depth=0):
    lines = [f"{'  ' * depth}{part.get_content_type()}"]
    if part.is_multipart():
        for child in part.iter_parts():
            lines.extend(_structure(child, depth + 1))
    return lines


def test_an_inline_image_is_related_to_the_html_next_to_the_text_and_the_attachments():
    message = MailMessage(
        subject="Hola",
        text="plain",
        html='<img src="cid:key.png">',
        to="a@example.edu",
        attachments=(LOGO, REPORT),
    )

    mime = build_mime_message(message, default_sender=SENDER)
    parsed = email.message_from_bytes(mime.as_bytes(), policy=policy.default)
    image = next(part for part in parsed.walk() if part.get_content_type() == "image/png")

    assert _structure(parsed) == [
        "multipart/mixed",
        "  multipart/alternative",
        "    text/plain",
        "    multipart/related",
        "      text/html",
        "      image/png",
        "  text/csv",
    ]
    assert image["Content-ID"] == "<key.png>"
    assert image.get_content_disposition() == "inline"
    assert image.get_content() == b"\x89PNG-bytes"
    assert [part.get_filename() for part in parsed.iter_attachments()] == ["respaldo.csv"]


def test_an_inline_image_with_html_alone_makes_the_message_related():
    message = MailMessage(subject="Hola", html='<img src="cid:key.png">', to="a@example.edu", attachments=(LOGO,))

    mime = build_mime_message(message, default_sender=SENDER)

    assert _structure(mime) == ["multipart/related", "  text/html", "  image/png"]


def test_an_inline_image_needs_an_html_body():
    with pytest.raises(MailMessageError, match="html body"):
        MailMessage(subject="Hola", text="plain", to="a@example.edu", attachments=(LOGO,))


@pytest.mark.parametrize("content_id", ["", "../key.png", "key png", "<key.png>", ".hidden"])
def test_a_content_id_is_a_plain_token(content_id):
    with pytest.raises(MailMessageError, match="content id"):
        MailAttachment(filename="key.png", content=b"x", content_type="image/png", content_id=content_id)


def test_non_ascii_subject_and_body_survive_the_round_trip():
    message = MailMessage(subject="Préstamo de llave: aula 1-347", text="Hola, José", to="a@example.edu")

    mime = build_mime_message(message, default_sender=SENDER)
    parsed = email.message_from_bytes(mime.as_bytes(), policy=policy.default)

    assert parsed["Subject"] == "Préstamo de llave: aula 1-347"
    assert parsed.get_content().strip() == "Hola, José"
