import logging

import pytest

from persistence_kit.mail import MailTemplateError, MemoryMailSender
from persistence_kit.mail.mailer import Mailer
from persistence_kit.mail.message import MailAttachment, MailMessage
from persistence_kit.mail.templates import MailTemplates


@pytest.fixture
def templates_dir(tmp_path):
    (tmp_path / "layout.html").write_text(
        "<html><body>{% block content %}{% endblock %}<footer>FCEN</footer></body></html>",
        encoding="utf-8",
    )
    (tmp_path / "user_enabled.subject.txt").write_text(
        "Tu usuario {{ username }}\n  fue habilitado & listo\n", encoding="utf-8"
    )
    (tmp_path / "user_enabled.html").write_text(
        '{% extends "layout.html" %}{% block content %}<p>Hola {{ name }}</p>{% endblock %}',
        encoding="utf-8",
    )
    (tmp_path / "user_enabled.txt").write_text("Hola {{ name }}", encoding="utf-8")
    (tmp_path / "html_only.subject.txt").write_text("Solo html", encoding="utf-8")
    (tmp_path / "html_only.html").write_text("<p>{{ body }}</p>", encoding="utf-8")
    (tmp_path / "no_subject.html").write_text("<p>x</p>", encoding="utf-8")
    (tmp_path / "no_body.subject.txt").write_text("Sin cuerpo", encoding="utf-8")
    return tmp_path


def test_renders_subject_html_and_text_from_the_app_directory(templates_dir):
    rendered = MailTemplates(templates_dir).render(
        "user_enabled", {"username": "ana@example.edu", "name": "<Ana & Luis>"}
    )

    assert rendered.subject == "Tu usuario ana@example.edu fue habilitado & listo"
    assert rendered.html == (
        "<html><body><p>Hola &lt;Ana &amp; Luis&gt;</p><footer>FCEN</footer></body></html>"
    )
    assert rendered.text == "Hola <Ana & Luis>"


def test_a_template_can_have_only_html(templates_dir):
    rendered = MailTemplates(templates_dir).render("html_only", {"body": "hola"})

    assert rendered.html == "<p>hola</p>"
    assert rendered.text is None


def test_a_template_needs_a_subject(templates_dir):
    with pytest.raises(MailTemplateError, match="no_subject.subject.txt"):
        MailTemplates(templates_dir).render("no_subject")


def test_a_template_needs_a_body(templates_dir):
    with pytest.raises(MailTemplateError, match="needs no_body.html or no_body.txt"):
        MailTemplates(templates_dir).render("no_body")


def test_a_missing_variable_fails_instead_of_rendering_blank(templates_dir):
    with pytest.raises(MailTemplateError, match="user_enabled.subject.txt"):
        MailTemplates(templates_dir).render("user_enabled", {"name": "Ana"})


def test_a_template_outside_the_directory_is_not_found(templates_dir):
    with pytest.raises(MailTemplateError, match="has no"):
        MailTemplates(templates_dir).render("../user_enabled")


def test_the_directory_has_to_exist(tmp_path):
    with pytest.raises(MailTemplateError, match="does not exist"):
        MailTemplates(tmp_path / "missing")


@pytest.mark.asyncio
async def test_mailer_composes_from_a_template_and_sends_with_cc_and_bcc(templates_dir):
    sender = MemoryMailSender()
    mailer = Mailer(sender=sender, templates=MailTemplates(templates_dir))

    message = mailer.compose(
        "user_enabled",
        to="ana@example.edu",
        cc=["luis@example.edu"],
        bcc="audit@example.edu",
        context={"username": "ana@example.edu", "name": "Ana"},
    )
    await mailer.send(message)

    [sent] = sender.sent
    assert sent.subject == "Tu usuario ana@example.edu fue habilitado & listo"
    assert sent.envelope_recipients == ("ana@example.edu", "luis@example.edu", "audit@example.edu")
    assert sender.mime[0]["Bcc"] is None


@pytest.fixture
def branded_dir(templates_dir):
    (templates_dir / "images").mkdir()
    (templates_dir / "images" / "key.png").write_bytes(b"\x89PNG-key")
    (templates_dir / "images" / "notes.txt").write_text("x", encoding="utf-8")
    (templates_dir / "branded.subject.txt").write_text("Con logo", encoding="utf-8")
    (templates_dir / "branded.html").write_text(
        '<img src="cid:key.png" alt=""><p>{{ body }}</p><img src=\'cid:key.png\'>', encoding="utf-8"
    )
    (templates_dir / "branded.txt").write_text("{{ body }}", encoding="utf-8")
    return templates_dir


def test_a_cid_image_is_embedded_once_from_the_images_folder(branded_dir):
    rendered = MailTemplates(branded_dir).render("branded", {"body": "hola"})

    [image] = rendered.inline_images
    assert image.filename == "key.png"
    assert image.content_id == "key.png"
    assert image.content_type == "image/png"
    assert image.content == b"\x89PNG-key"


def test_a_template_without_cid_images_embeds_nothing(templates_dir):
    rendered = MailTemplates(templates_dir).render("user_enabled", {"username": "a", "name": "b"})

    assert rendered.inline_images == ()


@pytest.mark.parametrize(
    ("source", "error"),
    [
        ("cid:missing.png", "images/missing.png does not exist"),
        ("cid:notes.txt", "not an image"),
        ("cid:../key.png", "not a plain file name"),
    ],
)
def test_a_cid_image_has_to_be_an_image_file_in_the_images_folder(branded_dir, source, error):
    (branded_dir / "branded.html").write_text(f'<img src="{source}">', encoding="utf-8")

    with pytest.raises(MailTemplateError, match=error):
        MailTemplates(branded_dir).render("branded", {"body": "hola"})


def test_compose_attaches_the_cid_images_before_the_callers_attachments(branded_dir):
    mailer = Mailer(sender=MemoryMailSender(), templates=MailTemplates(branded_dir))
    report = MailAttachment(filename="r.csv", content=b"a", content_type="text/csv")

    message = mailer.compose("branded", to="ana@example.edu", context={"body": "hola"}, attachments=(report,))

    assert [attachment.filename for attachment in message.attachments] == ["key.png", "r.csv"]
    assert message.attachments[0].inline


def test_compose_raises_on_a_broken_template_so_tests_catch_it(templates_dir):
    mailer = Mailer(sender=MemoryMailSender(), templates=MailTemplates(templates_dir))

    with pytest.raises(MailTemplateError):
        mailer.compose("user_enabled", to="ana@example.edu", context={})


@pytest.mark.asyncio
async def test_dispatch_delivers_in_the_background(templates_dir):
    sender = MemoryMailSender()
    mailer = Mailer(sender=sender, templates=MailTemplates(templates_dir))

    mailer.dispatch(mailer.compose("html_only", to="ana@example.edu", context={"body": "hola"}))
    await mailer.wait_for_pending()

    assert [message.subject for message in sender.sent] == ["Solo html"]


class _BrokenRelay:
    async def send(self, message: MailMessage) -> None:
        raise ConnectionError("relay down")


@pytest.mark.asyncio
async def test_dispatch_logs_a_relay_failure_without_raising(templates_dir, caplog):
    mailer = Mailer(sender=_BrokenRelay(), templates=MailTemplates(templates_dir))
    message = mailer.compose("html_only", to="ana@example.edu", context={"body": "hola"})

    with caplog.at_level(logging.ERROR, logger="persistence_kit.mail.mailer"):
        task = mailer.dispatch(message)
        await mailer.wait_for_pending()

    assert task.exception() is None
    assert "mail.dispatch_failed" in caplog.text
    assert "relay down" in caplog.text
