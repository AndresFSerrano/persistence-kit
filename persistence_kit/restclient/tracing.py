from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator
from urllib.parse import urlsplit

try:
    from opentelemetry import trace
    from opentelemetry.trace import SpanKind, Status, StatusCode
except ImportError:
    trace = None

TRACER_NAME = "persistence_kit.restclient"


class TracedCall:
    def __init__(self, span: Any = None) -> None:
        self._span = span

    def record(self, status_code: int, attempts: int = 0) -> None:
        if self._span is None:
            return
        self._span.set_attribute("http.response.status_code", status_code)
        if attempts:
            self._span.set_attribute("http.request.resend_count", attempts)
        if status_code >= 400:
            self._span.set_status(Status(StatusCode.ERROR))


@contextmanager
def traced_call(service_name: str, method: str, url: str) -> Iterator[TracedCall]:
    if trace is None:
        yield TracedCall()
        return
    parts = urlsplit(url)
    path = parts.path or "/"
    verb = method.upper()
    tracer = trace.get_tracer(TRACER_NAME)
    with tracer.start_as_current_span(
        f"{verb} {path}",
        kind=SpanKind.CLIENT,
        record_exception=False,
        set_status_on_exception=False,
    ) as span:
        span.set_attribute("http.request.method", verb)
        span.set_attribute("url.path", path)
        if parts.hostname:
            span.set_attribute("server.address", parts.hostname)
        if service_name:
            span.set_attribute("peer.service", service_name)
        try:
            yield TracedCall(span)
        except Exception as error:
            status_code = getattr(error, "status_code", None)
            if isinstance(status_code, int):
                span.set_attribute("http.response.status_code", status_code)
            span.set_attribute("error.type", type(error).__name__)
            span.set_status(Status(StatusCode.ERROR))
            raise
