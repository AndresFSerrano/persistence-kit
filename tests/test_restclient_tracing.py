import pytest

httpx = pytest.importorskip("httpx")
pytest.importorskip("opentelemetry.sdk")

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, StatusCode

from persistence_kit.restclient import (
    MemoryRestClient,
    RestClientRegistry,
    RetryPolicy,
    ServiceConfig,
    StaticEndpointResolver,
)
from persistence_kit.restclient import tracing
from persistence_kit.restclient.errors import RestHTTPError, RestTransportError


@pytest.fixture
def spans(monkeypatch):
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(tracing.trace, "get_tracer", provider.get_tracer)
    yield exporter
    provider.shutdown()


def _client(handler, name="sipe", retries=0):
    from persistence_kit.restclient import HttpxRestClient

    return HttpxRestClient(
        config=ServiceConfig(max_retries=retries),
        resolver=StaticEndpointResolver("https://api.test/ws"),
        retry_policy=RetryPolicy(max_retries=retries, backoff_base=0.0, jitter=False),
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        name=name,
    )


def _values(span):
    return " ".join([span.name, *(str(value) for value in span.attributes.values())])


def _ok(request):
    return httpx.Response(200, json={"ok": True})


@pytest.mark.asyncio
async def test_each_call_is_a_client_span_of_its_service(spans):
    client = _client(_ok)

    await client.get("/consultaEmpleados", params={"usuario": "juan", "clave": "secreta"})

    [span] = spans.get_finished_spans()
    assert span.name == "GET /ws/consultaEmpleados"
    assert span.kind == SpanKind.CLIENT
    assert dict(span.attributes) == {
        "http.request.method": "GET",
        "url.path": "/ws/consultaEmpleados",
        "server.address": "api.test",
        "peer.service": "sipe",
        "http.response.status_code": 200,
    }
    assert "secreta" not in _values(span) and "juan" not in _values(span)
    await client.aclose()


@pytest.mark.asyncio
async def test_an_error_status_marks_the_span_without_keeping_the_exception(spans):
    def failing(request):
        return httpx.Response(500)

    client = _client(failing)

    with pytest.raises(RestHTTPError):
        await client.get("/validarUsuario", params={"clave": "secreta"})

    [span] = spans.get_finished_spans()
    assert span.status.status_code == StatusCode.ERROR
    assert span.attributes["http.response.status_code"] == 500
    assert span.attributes["error.type"] == "RestHTTPError"
    assert span.events == ()
    await client.aclose()


@pytest.mark.asyncio
async def test_a_transport_failure_keeps_the_url_out_of_the_span(spans):
    def unreachable(request):
        raise httpx.ConnectError(f"no route to {request.url}")

    client = _client(unreachable)

    with pytest.raises(RestTransportError):
        await client.get("/validarUsuario", params={"clave": "secreta"})

    [span] = spans.get_finished_spans()
    assert span.attributes["error.type"] == "RestTransportError"
    assert span.status.status_code == StatusCode.ERROR
    assert span.events == ()
    assert "secreta" not in _values(span)
    await client.aclose()


@pytest.mark.asyncio
async def test_retries_are_counted_on_the_same_span(spans):
    answers = iter([httpx.Response(503), httpx.Response(200, json={})])

    def flaky(request):
        return next(answers)

    client = _client(flaky, retries=2)

    await client.get("/consultaPersona")

    [span] = spans.get_finished_spans()
    assert span.attributes["http.request.resend_count"] == 1
    assert span.attributes["http.response.status_code"] == 200
    await client.aclose()


@pytest.mark.asyncio
async def test_calls_nest_under_the_current_span(spans):
    client = _client(_ok)
    tracer = tracing.trace.get_tracer("app")

    with tracer.start_as_current_span("POST /auth/login") as parent:
        await client.get("/consultaEmpleados")

    child = next(span for span in spans.get_finished_spans() if span.attributes.get("peer.service") == "sipe")
    assert child.parent.span_id == parent.get_span_context().span_id
    assert child.context.trace_id == parent.get_span_context().trace_id
    await client.aclose()


@pytest.mark.asyncio
async def test_the_registry_names_its_clients(spans):
    registry = RestClientRegistry()
    registry.register("mares", base_url="https://api.test")
    stub = MemoryRestClient()
    stub.stub("GET", "/validarUsuario", json={})
    named = MemoryRestClient(name="propio")
    registry.set_client("mua", stub)
    registry.set_client("otro", named)

    await registry.get("mua").get("/validarUsuario", params={"clave": "secreta"})

    [span] = spans.get_finished_spans()
    assert span.name == "GET /validarUsuario"
    assert span.attributes["peer.service"] == "mua"
    assert "secreta" not in _values(span)
    assert registry.get("mares").name == "mares"
    assert registry.get("otro").name == "propio"
    await registry.aclose_all()


@pytest.mark.asyncio
async def test_without_opentelemetry_the_call_still_goes_through(monkeypatch):
    monkeypatch.setattr(tracing, "trace", None)
    client = _client(_ok)

    response = await client.get("/consultaEmpleados")

    assert response.json() == {"ok": True}
    await client.aclose()
