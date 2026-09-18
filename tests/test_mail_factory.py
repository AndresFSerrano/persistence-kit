import subprocess
import sys

import pytest

from persistence_kit.mail import (
    MailAddress,
    MailBackend,
    MailConfigError,
    MailSettings,
    MemoryMailSender,
    SmtpMailSender,
    build_mailer,
    get_mail_sender,
    reset_mail_sender,
)
from persistence_kit.mail.mailer import Mailer


@pytest.fixture(autouse=True)
def fresh_senders():
    reset_mail_sender()
    yield
    reset_mail_sender()


def test_memory_is_the_default_backend_and_is_cached():
    first = get_mail_sender(MailSettings())
    second = get_mail_sender(MailSettings())

    assert isinstance(first, MemoryMailSender)
    assert first is second


def test_smtp_needs_a_host():
    settings = MailSettings(mail_backend=MailBackend.SMTP, mail_from_address="a@example.edu")

    with pytest.raises(MailConfigError, match="MAIL_SMTP_HOST"):
        get_mail_sender(settings)


def test_smtp_needs_a_from_address():
    settings = MailSettings(mail_backend=MailBackend.SMTP, mail_smtp_host="smtp.example.com")

    with pytest.raises(MailConfigError, match="MAIL_FROM_ADDRESS"):
        get_mail_sender(settings)


def test_environment_variables_configure_the_smtp_sender(monkeypatch):
    monkeypatch.setenv("MAIL_BACKEND", "smtp")
    monkeypatch.setenv("MAIL_SMTP_HOST", "smtp-relay.example.edu")
    monkeypatch.setenv("MAIL_SMTP_PORT", "587")
    monkeypatch.setenv("MAIL_FROM_ADDRESS", "prestamos@example.edu")
    monkeypatch.setenv("MAIL_FROM_NAME", "Préstamos FCEN")

    sender = get_mail_sender()

    assert isinstance(sender, SmtpMailSender)
    assert sender._host == "smtp-relay.example.edu"
    assert sender._port == 587
    assert sender._starttls is True
    assert sender._username is None
    assert sender._default_sender == MailAddress(email="prestamos@example.edu", name="Préstamos FCEN")


def test_build_mailer_joins_the_configured_sender_and_the_app_templates(tmp_path):
    mailer = build_mailer(tmp_path, MailSettings())

    assert isinstance(mailer, Mailer)


def test_reset_drops_the_cached_sender():
    first = get_mail_sender(MailSettings())
    reset_mail_sender()

    assert get_mail_sender(MailSettings()) is not first


def test_importing_the_mail_package_does_not_load_jinja2():
    code = "import sys; import persistence_kit.mail; print('jinja2' in sys.modules)"

    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        capture_output=True,
        text=True,
    )

    assert result.stdout.strip() == "False"
