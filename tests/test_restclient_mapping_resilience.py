import xml.etree.ElementTree as ET

import pytest
from pydantic import BaseModel

from persistence_kit.resilience import (
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
)
from persistence_kit.restclient import (
    DirectoryEndpointResolver,
    LoginTokenAuth,
    MemoryRestClient,
    RetryPolicy,
    ServiceConfig,
    StaticEndpointResolver,
    decode,
    encode,
)
from persistence_kit.restclient.contracts import RestRequest, RestResponse
from persistence_kit.restclient.errors import (
    RestCircuitOpenError,
    RestConfigError,
    RestTimeoutError,
    RestTransportError,
)


class Pais(BaseModel):
    pais: int
    nombre: str


@pytest.mark.asyncio
async def test_headers_merge_default_request_and_auth():
    from persistence_kit.restclient import BearerAuth

    client = MemoryRestClient(
        authenticator=BearerAuth("tok"),
        default_headers={"X-App": "store"},
        raise_for_status=False,
    )

    await client.get("/x", headers={"X-Trace": "42"})

    sent = client.calls[0].headers
    assert sent["X-App"] == "store"
    assert sent["X-Trace"] == "42"
    assert sent["Authorization"] == "Bearer tok"


@pytest.mark.asyncio
async def test_send_xml_sets_body_and_content_type():
    client = MemoryRestClient(raise_for_status=False)

    await client.post("/x", xml="<a/>")

    assert client.calls[0].content == b"<a/>"
    assert client.calls[0].headers["Content-Type"].startswith("application/xml")


@pytest.mark.asyncio
async def test_send_soap_action_sets_header_and_text_xml():
    client = MemoryRestClient(raise_for_status=False)

    await client.post("/x", xml="<env/>", soap_action="urn:foo")

    assert client.calls[0].headers["SOAPAction"] == '"urn:foo"'
    assert client.calls[0].headers["Content-Type"].startswith("text/xml")


def test_response_body_detects_content_type():
    json_resp = RestResponse(200, {"Content-Type": "application/json"}, b'{"a": 1}', "u")
    assert json_resp.body() == {"a": 1}
    assert json_resp.content_type == "application/json"

    xml_resp = RestResponse(200, {"Content-Type": "text/xml"}, b"<a>1</a>", "u")
    assert isinstance(xml_resp.body(), ET.Element)

    sniff_list = RestResponse(200, {}, b"[1, 2]", "u")
    assert sniff_list.body() == [1, 2]

    sniff_xml = RestResponse(200, {}, b"<a/>", "u")
    assert isinstance(sniff_xml.body(), ET.Element)

    plain = RestResponse(200, {"Content-Type": "text/plain"}, b"hola", "u")
    assert plain.body() == "hola"


@pytest.mark.asyncio
async def test_directory_resolver_resolves_and_caches():
    seen = []

    async def fetch(url):
        seen.append(url)
        return RestResponse(200, {}, b"https://real/svc  ", url)

    resolver = DirectoryEndpointResolver(
        "https://dir/list?serviceid={service}",
        fetch=fetch,
        cache_ttl_seconds=1000,
    )

    assert await resolver.resolve("svc") == "https://real/svc"
    assert await resolver.resolve("svc") == "https://real/svc"
    assert seen == ["https://dir/list?serviceid=svc"]

    resolver.invalidate("svc")
    await resolver.resolve("svc")
    assert len(seen) == 2


def test_directory_resolver_requires_placeholder():
    with pytest.raises(RestConfigError):
        DirectoryEndpointResolver("https://dir/no-placeholder")


@pytest.mark.asyncio
async def test_directory_resolver_errors_on_bad_directory():
    async def fetch_500(url):
        return RestResponse(500, {}, b"", url)

    async def fetch_empty(url):
        return RestResponse(200, {}, b"   ", url)

    with pytest.raises(RestConfigError):
        await DirectoryEndpointResolver("{service}?", fetch=fetch_500).resolve("x")
    with pytest.raises(RestConfigError):
        await DirectoryEndpointResolver("{service}?", fetch=fetch_empty).resolve("x")


def test_decode_list_single_and_select():
    resp = RestResponse(
        200,
        {"Content-Type": "application/json"},
        b'{"data": [{"pais": 1, "nombre": "Colombia"}]}',
        "u",
    )
    paises = decode(resp, list[Pais], select="data")
    assert paises == [Pais(pais=1, nombre="Colombia")]

    one = decode({"pais": 2, "nombre": "Peru"}, Pais)
    assert one == Pais(pais=2, nombre="Peru")

    called = decode([1, 2, 3], lambda data: sum(data))
    assert called == 6


def test_encode_model_list_and_plain():
    assert encode(Pais(pais=1, nombre="X")) == {"pais": 1, "nombre": "X"}
    assert encode([Pais(pais=1, nombre="X")]) == [{"pais": 1, "nombre": "X"}]
    assert encode({"raw": True}) == {"raw": True}


@pytest.mark.asyncio
async def test_get_as_maps_response_to_dtos():
    client = MemoryRestClient()
    client.stub("GET", "/paises", json=[{"pais": 1, "nombre": "Colombia"}])

    paises = await client.get_as("/paises", list[Pais])

    assert paises == [Pais(pais=1, nombre="Colombia")]


@pytest.mark.asyncio
async def test_post_as_encodes_dto_and_maps_response():
    client = MemoryRestClient()
    client.stub("POST", "/paises", json={"pais": 9, "nombre": "Creado"})

    created = await client.post_as("/paises", Pais, dto=Pais(pais=9, nombre="Creado"))

    assert created == Pais(pais=9, nombre="Creado")
    assert client.calls[0].json_body == {"pais": 9, "nombre": "Creado"}


def test_retry_policy_should_retry_exception():
    policy = RetryPolicy(max_retries=2)
    assert policy.should_retry_exception(RestTimeoutError("t"), 0) is True
    assert policy.should_retry_exception(RestTransportError("x"), 1) is True
    assert policy.should_retry_exception(RestTimeoutError("t"), 2) is False
    assert policy.should_retry_exception(ValueError("v"), 0) is False

    no_timeout = RetryPolicy(max_retries=2, retry_on_timeout=False)
    assert no_timeout.should_retry_exception(RestTimeoutError("t"), 0) is False


def test_circuit_breaker_opens_and_recovers():
    breaker = CircuitBreaker(failure_threshold=2, recovery_timeout=1000)
    breaker.record_failure()
    assert breaker.state is CircuitState.CLOSED
    breaker.record_failure()
    assert breaker.state is CircuitState.OPEN
    assert breaker.allow() is False

    with pytest.raises(CircuitOpenError):
        breaker.guard()


def test_circuit_breaker_half_open_closes_on_success():
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=0.0)
    breaker.record_failure()
    assert breaker.state is CircuitState.OPEN

    assert breaker.allow() is True
    assert breaker.state is CircuitState.HALF_OPEN
    breaker.record_success()
    assert breaker.state is CircuitState.CLOSED


@pytest.mark.asyncio
async def test_directory_resolver_url_field():
    async def fetch(url):
        return RestResponse(
            200,
            {"Content-Type": "application/json"},
            b'{"data": {"url": "https://real/svc"}}',
            url,
        )

    resolver = DirectoryEndpointResolver(
        "https://dir?serviceid={service}", url_field="data.url", fetch=fetch
    )
    assert await resolver.resolve("svc") == "https://real/svc"


@pytest.mark.asyncio
async def test_directory_resolver_default_picks_common_json_field():
    async def fetch(url):
        return RestResponse(
            200,
            {"Content-Type": "application/json"},
            b'{"endpoint": "https://real/svc"}',
            url,
        )

    resolver = DirectoryEndpointResolver("https://dir?serviceid={service}", fetch=fetch)
    assert await resolver.resolve("svc") == "https://real/svc"


@pytest.mark.asyncio
async def test_login_token_auth_caches_and_refreshes():
    auth = LoginTokenAuth(
        login_url="https://idp.test/login",
        username="user",
        password="pass",
    )
    logins = {"n": 0}

    async def fake_login():
        logins["n"] += 1
        auth._token = f"tok{logins['n']}"
        auth._expires_at = 10**9

    auth._login = fake_login

    req = await auth.apply(RestRequest(method="GET", url="https://api.test"))
    assert req.headers["Authorization"] == "Bearer tok1"

    await auth.apply(RestRequest(method="GET", url="https://api.test"))
    assert logins["n"] == 1

    retry = await auth.on_unauthorized(
        RestResponse(status_code=401, headers={}, content=b"", url="https://api.test")
    )
    assert retry is True

    req3 = await auth.apply(RestRequest(method="GET", url="https://api.test"))
    assert req3.headers["Authorization"] == "Bearer tok2"
    assert logins["n"] == 2


httpx = pytest.importorskip("httpx")


def _mock_client(handler):
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


@pytest.mark.asyncio
async def test_httpx_retries_on_timeout_then_succeeds():
    from persistence_kit.restclient import HttpxRestClient

    state = {"n": 0}

    def handler(request):
        state["n"] += 1
        if state["n"] == 1:
            raise httpx.ConnectTimeout("boom", request=request)
        return httpx.Response(200, json={"ok": True})

    client = HttpxRestClient(
        resolver=StaticEndpointResolver("https://api.test"),
        retry_policy=RetryPolicy(max_retries=2, backoff_base=0.0, jitter=False),
        client=_mock_client(handler),
    )

    response = await client.get("/flaky")
    assert response.status_code == 200
    assert state["n"] == 2
    await client.aclose()


@pytest.mark.asyncio
async def test_httpx_circuit_opens_and_short_circuits():
    from persistence_kit.restclient import HttpxRestClient

    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        return httpx.Response(500)

    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=1000)
    client = HttpxRestClient(
        config=ServiceConfig(raise_for_status=False),
        resolver=StaticEndpointResolver("https://api.test"),
        retry_policy=RetryPolicy(max_retries=0),
        circuit_breaker=breaker,
        client=_mock_client(handler),
    )

    first = await client.get("/x")
    assert first.status_code == 500
    assert breaker.state is CircuitState.OPEN

    with pytest.raises(RestCircuitOpenError) as err:
        await client.get("/x")
    assert isinstance(err.value, CircuitOpenError)
    assert calls["n"] == 1
    await client.aclose()
