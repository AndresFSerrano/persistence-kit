from __future__ import annotations

from persistence_kit.resilience import CircuitOpenError


class RestClientError(Exception):
    """Base exception for the REST client module."""


class RestConfigError(RestClientError):
    """Raised when a REST client or service is not configured correctly."""


class RestAuthError(RestClientError):
    """Raised when authentication cannot be applied or a token cannot be obtained."""


class RestTimeoutError(RestClientError):
    """Raised when a request exceeds its configured timeout."""


class RestTransportError(RestClientError):
    """Raised when the request fails at the transport layer (connection, DNS, TLS)."""


class RestCircuitOpenError(RestClientError, CircuitOpenError):
    """Raised when a request is short-circuited because the circuit breaker is open."""


class RestHTTPError(RestClientError):
    """Raised when a response carries a non-success status and raising is enabled."""

    def __init__(
        self,
        status_code: int,
        message: str | None = None,
        response: object | None = None,
    ) -> None:
        self.status_code = status_code
        self.response = response
        super().__init__(message or f"La respuesta HTTP devolvio el estado {status_code}.")
