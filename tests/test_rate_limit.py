import pytest

pytest.importorskip("fastapi")
httpx = pytest.importorskip("httpx")

from fastapi import Depends, FastAPI

from persistence_kit.api import (
    CacheRateLimiter,
    InMemoryRateLimiter,
    build_auth_rate_limit_dependency,
    build_rate_limiter,
)
from persistence_kit.cache import InMemoryTTLCache, get_cache, reset_cache
from persistence_kit.settings import DeploymentStage, PersistenceKitSettings

WINDOW = 60
WINDOW_START = 100 * WINDOW


class FakeClock:
    def __init__(self, now: float = WINDOW_START) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


class RecordingCache(InMemoryTTLCache):
    def __init__(self) -> None:
        super().__init__()
        self.ttls: list[float | None] = []

    async def increment(self, key: str, ttl_seconds: float | None = None) -> int:
        self.ttls.append(ttl_seconds)
        return await super().increment(key, ttl_seconds)


@pytest.fixture(autouse=True)
def a_fresh_memory_cache(monkeypatch):
    monkeypatch.setenv("CACHE_BACKEND", "memory")
    monkeypatch.setenv("CACHE_NAMESPACE", "")
    reset_cache()
    yield
    reset_cache()


async def _hits(limiter, times, *, limit=3, scope="auth-login", client_id="203.0.113.7"):
    return [
        await limiter.hit(scope=scope, client_id=client_id, limit=limit, window_seconds=WINDOW)
        for _ in range(times)
    ]


def _app(trusted_proxy_hops):
    settings = PersistenceKitSettings(
        stage=DeploymentStage.DEV,
        auth_login_rate_limit=3,
        trusted_proxy_hops=trusted_proxy_hops,
    )

    def current_settings():
        return settings

    limit = build_auth_rate_limit_dependency(
        scope="auth-login",
        limit_setting_name="auth_login_rate_limit",
        settings_dep=current_settings,
    )
    app = FastAPI()

    @app.post("/login", dependencies=[Depends(limit)])
    async def login():
        return {"ok": True}

    return app


async def _statuses(app, forwarded_for):
    transport = httpx.ASGITransport(app=app, client=("10.0.0.9", 4321))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        return [(await client.post("/login", headers=headers)).status_code for headers in forwarded_for]


@pytest.mark.asyncio
async def test_a_forged_first_hop_does_not_open_a_new_bucket():
    spoofed = [{"X-Forwarded-For": f"198.51.100.{index}, 203.0.113.7"} for index in range(5)]

    assert await _statuses(_app(1), spoofed) == [200, 200, 200, 429, 429]


@pytest.mark.asyncio
async def test_each_real_client_keeps_its_own_bucket():
    clients = [{"X-Forwarded-For": f"203.0.113.{index}"} for index in range(5)]

    assert await _statuses(_app(1), clients) == [200, 200, 200, 200, 200]


@pytest.mark.asyncio
async def test_two_trusted_proxies_count_the_visitor():
    behind_cdn = [{"X-Forwarded-For": f"198.51.100.{index}, 203.0.113.7, 130.176.0.{index}"} for index in range(5)]

    assert await _statuses(_app(2), behind_cdn) == [200, 200, 200, 429, 429]


@pytest.mark.asyncio
async def test_without_trusted_proxies_the_header_is_ignored():
    rotating = [{"X-Forwarded-For": f"203.0.113.{index}"} for index in range(5)]

    assert await _statuses(_app(0), rotating) == [200, 200, 200, 429, 429]


@pytest.mark.asyncio
async def test_a_shorter_chain_than_expected_falls_back_to_the_peer():
    short = [{"X-Forwarded-For": f"203.0.113.{index}"} for index in range(5)]

    assert await _statuses(_app(2), short) == [200, 200, 200, 429, 429]


def test_trusted_proxy_hops_defaults_to_one_and_rejects_negatives():
    assert PersistenceKitSettings().trusted_proxy_hops == 1
    with pytest.raises(ValueError):
        PersistenceKitSettings(trusted_proxy_hops=-1)


@pytest.mark.asyncio
async def test_the_cache_limiter_lets_attempts_through_up_to_the_limit():
    limiter = CacheRateLimiter(InMemoryTTLCache(), clock=FakeClock())

    results = await _hits(limiter, 4)

    assert results[:3] == [None, None, None]
    assert results[3] >= 1


@pytest.mark.asyncio
@pytest.mark.parametrize(("offset", "passes"), [(-1, False), (0, True)])
async def test_retry_after_is_the_first_second_the_next_attempt_passes(offset, passes):
    clock = FakeClock()
    limiter = CacheRateLimiter(InMemoryTTLCache(), clock=clock)
    *_, retry_after = await _hits(limiter, 4)

    clock.now += retry_after + offset

    assert retry_after == 90
    assert ((await _hits(limiter, 1))[0] is None) is passes


@pytest.mark.asyncio
async def test_the_previous_window_still_weighs_on_the_current_one():
    clock = FakeClock()
    limiter = CacheRateLimiter(InMemoryTTLCache(), clock=clock)
    assert await _hits(limiter, 10, limit=10) == [None] * 10

    clock.now = WINDOW_START + WINDOW + WINDOW / 2
    results = await _hits(limiter, 6, limit=10)

    assert results[:5] == [None] * 5
    assert results[5] == 12
    clock.now += results[5]
    assert await _hits(limiter, 1, limit=10) == [None]


@pytest.mark.asyncio
async def test_the_previous_window_fades_out_as_the_current_one_advances():
    clock = FakeClock()
    limiter = CacheRateLimiter(InMemoryTTLCache(), clock=clock)
    await _hits(limiter, 10, limit=10)

    clock.now = WINDOW_START + 2 * WINDOW
    assert await _hits(limiter, 10, limit=10) == [None] * 10


@pytest.mark.asyncio
async def test_a_rejected_attempt_also_counts():
    cache = InMemoryTTLCache()
    limiter = CacheRateLimiter(cache, clock=FakeClock())

    first_rejection = (await _hits(limiter, 3, limit=2))[-1]
    last_rejection = (await _hits(limiter, 3, limit=2))[-1]

    assert await cache.get(f"auth-login:203.0.113.7:{WINDOW_START // WINDOW}") == 6
    assert last_rejection > first_rejection


@pytest.mark.asyncio
async def test_each_client_and_scope_keeps_its_own_count():
    limiter = CacheRateLimiter(InMemoryTTLCache(), clock=FakeClock())

    assert await _hits(limiter, 1, limit=1, scope="auth-login", client_id="a") == [None]
    assert await _hits(limiter, 1, limit=1, scope="auth-login", client_id="b") == [None]
    assert await _hits(limiter, 1, limit=1, scope="auth-refresh", client_id="a") == [None]
    assert (await _hits(limiter, 1, limit=1, scope="auth-login", client_id="a"))[0] >= 1


@pytest.mark.asyncio
async def test_two_limiters_on_the_same_cache_share_the_count():
    cache = InMemoryTTLCache()
    clock = FakeClock()
    first = CacheRateLimiter(cache, clock=clock)
    second = CacheRateLimiter(cache, clock=clock)

    assert await _hits(first, 2) == [None, None]
    assert await _hits(second, 1) == [None]
    assert (await _hits(first, 1))[0] >= 1
    assert (await _hits(second, 1))[0] >= 1


@pytest.mark.asyncio
async def test_each_counter_lives_two_windows():
    cache = RecordingCache()
    limiter = CacheRateLimiter(cache, clock=FakeClock())

    await _hits(limiter, 2)

    assert cache.ttls == [2 * WINDOW, 2 * WINDOW]


@pytest.mark.asyncio
@pytest.mark.parametrize(("limit", "window_seconds"), [(0, WINDOW), (3, 0)])
async def test_a_disabled_limit_never_rejects(limit, window_seconds):
    cache = RecordingCache()
    limiter = CacheRateLimiter(cache, clock=FakeClock())

    for _ in range(5):
        assert (
            await limiter.hit(
                scope="auth-login", client_id="a", limit=limit, window_seconds=window_seconds
            )
            is None
        )
    assert cache.ttls == []


@pytest.mark.asyncio
async def test_the_in_memory_limiter_still_counts_on_its_own():
    limiter = InMemoryRateLimiter()

    results = await _hits(limiter, 4)

    assert results[:3] == [None, None, None]
    assert results[3] >= 1


@pytest.mark.asyncio
async def test_build_rate_limiter_counts_in_the_rate_limit_cache():
    first = build_rate_limiter()
    second = build_rate_limiter()

    assert isinstance(first, CacheRateLimiter)
    assert await _hits(first, 2) == [None, None]
    assert await _hits(second, 1) == [None]
    assert (await _hits(second, 1))[0] >= 1
    assert await get_cache("rate-limit").clear("auth-login:203.0.113.7:") >= 1


@pytest.mark.asyncio
async def test_an_app_without_its_own_limiter_gets_the_cache_one():
    app = _app(1)

    await _statuses(app, [{"X-Forwarded-For": "203.0.113.7"}])

    assert isinstance(app.state.auth_rate_limiter, CacheRateLimiter)


@pytest.mark.asyncio
async def test_two_replicas_without_their_own_limiter_share_the_count():
    replicas = [_app(1), _app(1)]
    visitor = {"X-Forwarded-For": "203.0.113.7"}

    statuses = [(await _statuses(replica, [visitor]))[0] for replica in replicas * 2]

    assert statuses == [200, 200, 200, 429]


@pytest.mark.asyncio
async def test_an_app_that_sets_the_in_memory_limiter_keeps_it():
    app = _app(1)
    limiter = InMemoryRateLimiter()
    app.state.auth_rate_limiter = limiter
    visitor = [{"X-Forwarded-For": "203.0.113.7"}] * 5

    assert await _statuses(app, visitor) == [200, 200, 200, 429, 429]
    assert app.state.auth_rate_limiter is limiter
    assert await get_cache("rate-limit").clear() == 0
