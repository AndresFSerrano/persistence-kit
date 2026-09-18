import email
import socket
import socketserver
import threading
from email import policy

import pytest

from persistence_kit.mail import (
    MailAddress,
    MailDeliveryError,
    MailMessage,
    MailRecipientsRefusedError,
)
from persistence_kit.mail import smtp as smtp_module
from persistence_kit.mail.smtp import SmtpMailSender

SENDER = MailAddress(email="prestamos@example.edu", name="Préstamos FCEN")


class _RecordingSmtpHandler(socketserver.StreamRequestHandler):
    """Speaks just enough SMTP to record what a client really puts on the wire."""

    def _reply(self, line: str) -> None:
        self.wfile.write(f"{line}\r\n".encode())

    def handle(self) -> None:
        self._reply("220 test.local ESMTP")
        mail_from = None
        recipients: list[str] = []
        while True:
            raw = self.rfile.readline()
            if not raw:
                return
            command = raw.decode().strip()
            verb = command.upper()
            if verb.startswith(("EHLO", "HELO")):
                self._reply("250-test.local")
                self._reply("250 8BITMIME")
            elif verb.startswith("MAIL FROM:"):
                mail_from = command[len("MAIL FROM:"):].split()[0].strip("<>")
                recipients = []
                self._reply("250 2.1.0 OK")
            elif verb.startswith("RCPT TO:"):
                address = command[len("RCPT TO:"):].split()[0].strip("<>")
                if address in self.server.refused:
                    self._reply("550 5.1.1 No such user")
                else:
                    recipients.append(address)
                    self._reply("250 2.1.5 OK")
            elif verb == "DATA":
                self._reply("354 Go ahead")
                lines: list[bytes] = []
                while True:
                    line = self.rfile.readline()
                    if line in (b".\r\n", b"."):
                        break
                    lines.append(line[1:] if line.startswith(b"..") else line)
                self.server.deliveries.append((mail_from, recipients, b"".join(lines)))
                self._reply("250 2.0.0 OK queued")
            elif verb == "RSET":
                self._reply("250 2.0.0 OK")
            elif verb == "QUIT":
                self._reply("221 2.0.0 Bye")
                return
            else:
                self._reply("502 5.5.2 Not implemented")


class _RecordingSmtpServer(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), _RecordingSmtpHandler)
        self.deliveries: list[tuple[str, list[str], bytes]] = []
        self.refused: set[str] = set()


@pytest.fixture
def smtp_server():
    server = _RecordingSmtpServer()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    server.shutdown()
    server.server_close()


def _plain_sender(server: _RecordingSmtpServer) -> SmtpMailSender:
    return SmtpMailSender(
        host="127.0.0.1",
        port=server.server_address[1],
        starttls=False,
        default_sender=SENDER,
        timeout_seconds=5,
    )


@pytest.mark.asyncio
async def test_delivers_to_to_cc_and_bcc_but_only_names_to_and_cc_in_the_headers(smtp_server):
    message = MailMessage(
        subject="Tu usuario fue habilitado",
        text="Ya puedes ingresar, José.",
        html="<p>Ya puedes ingresar, José.</p>",
        to=MailAddress(email="ana@example.edu", name="Ana"),
        cc="luis@example.edu",
        bcc="audit@example.edu",
    )

    await _plain_sender(smtp_server).send(message)

    [(mail_from, recipients, data)] = smtp_server.deliveries
    parsed = email.message_from_bytes(data, policy=policy.default)
    assert mail_from == "prestamos@example.edu"
    assert recipients == ["ana@example.edu", "luis@example.edu", "audit@example.edu"]
    assert parsed["To"] == "Ana <ana@example.edu>"
    assert parsed["Cc"] == "luis@example.edu"
    assert parsed["Bcc"] is None
    assert b"audit@example.edu" not in data
    assert parsed["Subject"] == "Tu usuario fue habilitado"
    assert parsed.get_body(preferencelist=("plain",)).get_content().strip() == "Ya puedes ingresar, José."


@pytest.mark.asyncio
async def test_a_partial_refusal_is_reported_after_delivering_to_the_rest(smtp_server):
    smtp_server.refused.add("gone@example.edu")
    message = MailMessage(subject="Hola", text="x", to=["ana@example.edu", "gone@example.edu"])

    with pytest.raises(MailRecipientsRefusedError) as raised:
        await _plain_sender(smtp_server).send(message)

    assert list(raised.value.refused) == ["gone@example.edu"]
    [(_, recipients, _)] = smtp_server.deliveries
    assert recipients == ["ana@example.edu"]


@pytest.mark.asyncio
async def test_a_full_refusal_is_a_delivery_error(smtp_server):
    smtp_server.refused.add("gone@example.edu")
    message = MailMessage(subject="Hola", text="x", to="gone@example.edu")

    with pytest.raises(MailDeliveryError, match="127.0.0.1"):
        await _plain_sender(smtp_server).send(message)

    assert smtp_server.deliveries == []


def _closed_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@pytest.mark.asyncio
async def test_an_unreachable_relay_is_a_delivery_error():
    sender = SmtpMailSender(
        host="127.0.0.1",
        port=_closed_port(),
        starttls=False,
        default_sender=SENDER,
        timeout_seconds=2,
    )

    with pytest.raises(MailDeliveryError, match="Could not deliver"):
        await sender.send(MailMessage(subject="Hola", text="x", to="a@example.edu"))


class _ScriptedSmtp:
    """Stands in for `smtplib.SMTP` to check the order of the handshake."""

    calls: list[str] = []

    def __init__(self, host, port, timeout):
        self.calls.append(f"connect {host}:{port} timeout={timeout}")

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.calls.append("quit")

    def ehlo(self):
        self.calls.append("ehlo")

    def starttls(self, context):
        self.calls.append("starttls")

    def login(self, username, password):
        self.calls.append(f"login {username}:{password}")

    def send_message(self, mime, to_addrs):
        self.calls.append(f"send {','.join(to_addrs)}")
        return {}


@pytest.fixture
def scripted_smtp(monkeypatch):
    _ScriptedSmtp.calls = []
    monkeypatch.setattr(smtp_module.smtplib, "SMTP", _ScriptedSmtp)
    return _ScriptedSmtp


@pytest.mark.asyncio
async def test_upgrades_with_starttls_before_sending_and_skips_login_without_a_user(scripted_smtp):
    sender = SmtpMailSender(host="smtp-relay.example.com", default_sender=SENDER)

    await sender.send(MailMessage(subject="Hola", text="x", to="a@example.edu"))

    assert scripted_smtp.calls == [
        "connect smtp-relay.example.com:587 timeout=30.0",
        "ehlo",
        "starttls",
        "ehlo",
        "send a@example.edu",
        "quit",
    ]


@pytest.mark.asyncio
async def test_logs_in_after_starttls_when_given_a_user(scripted_smtp):
    sender = SmtpMailSender(
        host="smtp-relay.example.com",
        default_sender=SENDER,
        username="prestamos@example.edu",
        password="app-password",
    )

    await sender.send(MailMessage(subject="Hola", text="x", to="a@example.edu"))

    assert scripted_smtp.calls[2:5] == ["starttls", "ehlo", "login prestamos@example.edu:app-password"]
