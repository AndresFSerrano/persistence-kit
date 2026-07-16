import subprocess
import sys

import pytest

from persistence_kit.restclient import (
    ApiKeyAuth,
    BasicAuth,
    BearerAuth,
    MemoryRestClient,
    NoAuth,
    OAuth2ClientCredentials,
    RestClientRegistry,
    RetryPolicy,
    ServiceConfig,
    StaticEndpointResolver,
    build_rest_client,
)
from persistence_kit.restclient.contracts import RestRequest, RestResponse
from persistence_kit.restclient.errors import RestConfigError, RestHTTPError


@pytest.mark.asyncio
async def test_memory_client_returns_stubbed_json():
    client = MemoryRestClient()
    client.stub("GET", "/paises", json={"total": 2})

    response = await client.get("/paises")

    assert response.status_code == 200
    assert response.json() == {"total": 2}
    assert client.calls[0].method == "GET"


@pytest.mark.asyncio
async def test_api_key_auth_in_header():
    client = MemoryRestClient(authenticator=ApiKeyAuth("X-Api-Key", "secret"))
    client.stub("GET", "/data", json={})

    await client.get("/data")

    assert client.calls[0].headers["X-Api-Key"] == "secret"


@pytest.mark.asyncio
async def test_api_key_auth_in_query():
    client = MemoryRestClient(
        authenticator=ApiKeyAuth("api_key", "secret", location="query")
    )
    client.stub("GET", "/data", json={})

    await client.get("/data")

    assert client.calls[0].params["api_key"] == "secret"


@pytest.mark.asyncio
async def test_bearer_auth_header():
    client = MemoryRestClient(authenticator=BearerAuth("abc123"))
    client.stub("GET", "/me", json={})

    await client.get("/me")

    assert client.calls[0].headers["Authorization"] == "Bearer abc123"


@pytest.mark.asyncio
async def test_basic_auth_header():
    client = MemoryRestClient(authenticator=BasicAuth("user", "pass"))
    client.stub("GET", "/me", json={})

    await client.get("/me")

    assert client.calls[0].headers["Authorization"] == "Basic dXNlcjpwYXNz"


@pytest.mark.asyncio
async def test_memory_client_raises_on_error_status():
    client = MemoryRestClient()
    client.stub("GET", "/missing", status_code=404)

    with pytest.raises(RestHTTPError) as err:
        await client.get("/missing")

    assert err.value.status_code == 404


def test_api_key_auth_rejects_invalid_location():
    with pytest.raises(ValueError):
        ApiKeyAuth("k", "v", location="cookie")


@pytest.mark.asyncio
async def test_static_resolver_relative_absolute_and_error():
    resolver = StaticEndpointResolver("https://api.test/v1")
    assert await resolver.resolve("paises") == "https://api.test/v1/paises"
    assert await resolver.resolve("https://other.test/x") == "https://other.test/x"

    with pytest.raises(RestConfigError):
        await StaticEndpointResolver().resolve("paises")


def test_retry_policy_should_retry():
    policy = RetryPolicy(max_retries=2)
    assert policy.should_retry(503, attempt=0) is True
    assert policy.should_retry(503, attempt=2) is False
    assert policy.should_retry(404, attempt=0) is False


def test_service_config_from_settings():
    class Settings:
        rest_default_timeout_seconds = 3.5
        rest_default_max_retries = 5
        rest_default_verify_tls = False
        rest_default_user_agent = "kit/1"

    config = ServiceConfig.from_settings(Settings(), raise_for_status=False)

    assert config.timeout_seconds == 3.5
    assert config.max_retries == 5
    assert config.verify_tls is False
    assert config.user_agent == "kit/1"
    assert config.raise_for_status is False


@pytest.mark.asyncio
async def test_oauth2_caches_token_and_refreshes_on_unauthorized():
    auth = OAuth2ClientCredentials(
        token_url="https://idp.test/token",
        client_id="id",
        client_secret="secret",
    )
    fetch_count = 0

    async def fake_fetch():
        nonlocal fetch_count
        fetch_count += 1
        auth._token = f"tok{fetch_count}"
        auth._expires_at = 10**9

    auth._fetch_token = fake_fetch

    req = await auth.apply(RestRequest(method="GET", url="https://api.test"))
    assert req.headers["Authorization"] == "Bearer tok1"

    await auth.apply(RestRequest(method="GET", url="https://api.test"))
    assert fetch_count == 1

    retry = await auth.on_unauthorized(
        RestResponse(status_code=401, headers={}, content=b"", url="https://api.test")
    )
    assert retry is True

    req3 = await auth.apply(RestRequest(method="GET", url="https://api.test"))
    assert req3.headers["Authorization"] == "Bearer tok2"
    assert fetch_count == 2


def test_registry_rejects_duplicate_and_unknown():
    registry = RestClientRegistry()
    registry.register("example", base_url="https://api.test")

    with pytest.raises(RestConfigError):
        registry.register("example", base_url="https://other.test")

    with pytest.raises(RestConfigError):
        registry.get("unknown")

    assert registry.names() == ("example",)


def test_parse_str_map_accepts_json_and_csv():
    from persistence_kit.settings.parsers import parse_str_map

    assert parse_str_map('{"a": "x", "b": "y"}') == {"a": "x", "b": "y"}
    assert parse_str_map("a=x, b=y") == {"a": "x", "b": "y"}
    assert parse_str_map("a=x;b=y") == {"a": "x", "b": "y"}
    assert parse_str_map({"a": 1}) == {"a": "1"}
    assert parse_str_map("") == {}


def test_rest_service_urls_from_env(monkeypatch):
    from persistence_kit.settings import PersistenceKitSettings

    monkeypatch.setenv("REST_SERVICE_URLS", '{"geo": "https://env-url"}')
    settings = PersistenceKitSettings()

    assert settings.rest_service_urls == {"geo": "https://env-url"}


def test_registry_url_from_settings_overrides_code_default():
    from persistence_kit.settings import PersistenceKitSettings

    settings = PersistenceKitSettings(rest_service_urls={"geo": "https://env-url"})
    registry = RestClientRegistry(settings=settings)
    registry.register("geo", base_url="https://code-default")
    registry.register("other", base_url="https://code-default2")

    assert registry._services["geo"].resolver.base_url == "https://env-url"
    assert registry._services["other"].resolver.base_url == "https://code-default2"


def test_root_import_does_not_eagerly_load_httpx():
    code = (
        "import sys; import persistence_kit; "
        "print('httpx' in sys.modules)"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.stdout.strip() == "False"


httpx = pytest.importorskip("httpx")


def _mock_client(handler):
    transport = httpx.MockTransport(handler)
    return httpx.AsyncClient(transport=transport)


@pytest.mark.asyncio
async def test_httpx_client_applies_auth_and_parses_json():
    from persistence_kit.restclient import HttpxRestClient

    seen = {}

    def handler(request):
        seen["auth"] = request.headers.get("X-Api-Key")
        return httpx.Response(200, json={"ok": True})

    client = HttpxRestClient(
        resolver=StaticEndpointResolver("https://api.test"),
        authenticator=ApiKeyAuth("X-Api-Key", "secret"),
        client=_mock_client(handler),
    )

    response = await client.get("/paises")

    assert response.json() == {"ok": True}
    assert seen["auth"] == "secret"
    await client.aclose()


@pytest.mark.asyncio
async def test_httpx_client_retries_then_succeeds():
    from persistence_kit.restclient import HttpxRestClient

    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(503)
        return httpx.Response(200, json={"ok": True})

    client = HttpxRestClient(
        config=ServiceConfig(max_retries=2),
        resolver=StaticEndpointResolver("https://api.test"),
        retry_policy=RetryPolicy(max_retries=2, backoff_base=0.0, jitter=False),
        client=_mock_client(handler),
    )

    response = await client.get("/flaky")

    assert response.status_code == 200
    assert calls["n"] == 2
    await client.aclose()


@pytest.mark.asyncio
async def test_httpx_client_raises_for_status():
    from persistence_kit.restclient import HttpxRestClient

    def handler(request):
        return httpx.Response(404)

    client = HttpxRestClient(
        resolver=StaticEndpointResolver("https://api.test"),
        client=_mock_client(handler),
    )

    with pytest.raises(RestHTTPError):
        await client.get("/missing")
    await client.aclose()


@pytest.mark.asyncio
async def test_build_rest_client_and_registry_return_httpx_client():
    from persistence_kit.restclient import HttpxRestClient

    client = build_rest_client(base_url="https://api.test", authenticator=NoAuth())
    assert isinstance(client, HttpxRestClient)
    await client.aclose()

    registry = RestClientRegistry()
    registry.register("example", base_url="https://api.test")
    assert isinstance(registry.get("example"), HttpxRestClient)
    assert registry.get("example") is registry.get("example")
    await registry.aclose_all()
