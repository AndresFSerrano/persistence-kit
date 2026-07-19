import asyncio

import pytest

from persistence_kit.cache import InMemoryTTLCache
from persistence_kit.restclient import (
    CachingRestClient,
    MemoryRestClient,
    ServiceConfig,
    get_rest_client,
    register_rest_service,
    reset_rest_registry,
    set_rest_registry_initializer,
)
from persistence_kit.restclient.contracts import RestResponse


def _make(inner: MemoryRestClient, **kwargs) -> CachingRestClient:
    kwargs.setdefault("default_ttl_seconds", 60)
    kwargs.setdefault("name", "svc")
    return CachingRestClient(inner, InMemoryTTLCache(), **kwargs)


@pytest.mark.asyncio
async def test_get_served_from_cache_after_first_call():
    inner = MemoryRestClient()
    inner.stub("GET", "/items", json={"n": 1})
    client = _make(inner)

    first = await client.request("GET", "/items")
    second = await client.request("GET", "/items")

    assert first.json() == {"n": 1}
    assert second.json() == {"n": 1}
    assert len(inner.calls) == 1


@pytest.mark.asyncio
async def test_different_params_are_separate_entries():
    inner = MemoryRestClient()
    inner.stub("GET", "/x", json={"ok": True})
    client = _make(inner)

    await client.request("GET", "/x", params={"a": 1})
    await client.request("GET", "/x", params={"a": 2})

    assert len(inner.calls) == 2


@pytest.mark.asyncio
async def test_post_is_not_cached():
    inner = MemoryRestClient()
    inner.stub("POST", "/x", json={"ok": True})
    client = _make(inner)

    await client.request("POST", "/x", json={"body": 1})
    await client.request("POST", "/x", json={"body": 1})

    assert len(inner.calls) == 2


@pytest.mark.asyncio
async def test_cache_ttl_zero_bypasses_cache():
    inner = MemoryRestClient()
    inner.stub("GET", "/x", json={"ok": True})
    client = _make(inner)

    await client.request("GET", "/x")
    await client.request("GET", "/x", cache_ttl=0)

    assert len(inner.calls) == 2


@pytest.mark.asyncio
async def test_per_call_cache_ttl_enables_cache():
    inner = MemoryRestClient()
    inner.stub("GET", "/x", json={"ok": True})
    client = CachingRestClient(inner, InMemoryTTLCache(), name="svc")

    await client.request("GET", "/x")
    await client.request("GET", "/x")
    assert len(inner.calls) == 2

    await client.request("GET", "/x", cache_ttl=60)
    await client.request("GET", "/x", cache_ttl=60)
    assert len(inner.calls) == 3


@pytest.mark.asyncio
async def test_stale_while_revalidate_detects_change_and_fires_on_change():
    counter = {"n": 0}
    changed: list[str] = []

    def handler(_request):
        counter["n"] += 1
        body = b'{"v": 1}' if counter["n"] == 1 else b'{"v": 2}'
        return RestResponse(
            status_code=200,
            headers={"Content-Type": "application/json"},
            content=body,
            url="/x",
        )

    inner = MemoryRestClient()
    inner.stub("GET", "/x", handler=handler)
    client = CachingRestClient(
        inner,
        InMemoryTTLCache(),
        default_ttl_seconds=0.01,
        default_swr_seconds=60,
        name="svc",
        on_change=lambda key: changed.append(key),
    )

    first = await client.request("GET", "/x")
    assert first.json() == {"v": 1}

    await asyncio.sleep(0.02)
    stale = await client.request("GET", "/x")
    assert stale.json() == {"v": 1}

    await asyncio.sleep(0.05)
    assert changed == ["svc|GET|/x|{}"]

    refreshed = await client.request("GET", "/x")
    assert refreshed.json() == {"v": 2}


@pytest.mark.asyncio
async def test_registry_wraps_when_cache_ttl_set():
    reset_rest_registry()
    set_rest_registry_initializer(None)
    register_rest_service(
        "cached-svc",
        base_url="https://x.test",
        config=ServiceConfig(cache_ttl_seconds=60),
    )

    assert isinstance(get_rest_client("cached-svc"), CachingRestClient)
    reset_rest_registry()


@pytest.mark.asyncio
async def test_registry_wraps_when_cacheable_flag_set():
    reset_rest_registry()
    set_rest_registry_initializer(None)
    register_rest_service(
        "percall-svc",
        base_url="https://x.test",
        config=ServiceConfig(cacheable=True),
    )

    assert isinstance(get_rest_client("percall-svc"), CachingRestClient)
    reset_rest_registry()


@pytest.mark.asyncio
async def test_registry_plain_client_without_caching():
    reset_rest_registry()
    set_rest_registry_initializer(None)
    register_rest_service("plain-svc", base_url="https://x.test")

    assert not isinstance(get_rest_client("plain-svc"), CachingRestClient)
    reset_rest_registry()
