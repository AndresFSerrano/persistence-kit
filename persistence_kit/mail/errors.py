class MailError(Exception):
    """Base exception for the mail capability."""


class MailConfigError(MailError):
    """Raised when the mail backend is not configured correctly."""


class MailMessageError(MailError, ValueError):
    """Raised when a message cannot be built: no recipients, no body or unsafe header values."""


class MailTemplateError(MailError):
    """Raised when a mail template is missing or fails to render."""


class MailDeliveryError(MailError):
    """Raised when the relay cannot be reached or rejects the message."""


class MailRecipientsRefusedError(MailDeliveryError):
    """Raised when the relay accepted the message for some recipients and refused the rest."""

    def __init__(self, refused: dict[str, tuple[int, bytes]]) -> None:
        self.refused = refused
        super().__init__(f"The relay refused {len(refused)} recipient(s): {', '.join(sorted(refused))}")
