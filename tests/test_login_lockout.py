import asyncio

import pytest

pytest.importorskip("fastapi")

from persistence_kit import (
    Lockout,
    LockoutScope,
    LoginLockout,
    build_login_lockout,
)
from persistence_kit.api.login_lockout import LOGIN_LOCKOUT_CACHE_NAME
from persistence_kit.cache import InMemoryTTLCache, get_cache, reset_cache
from persistence_kit.settings import DeploymentStage, PersistenceKitSettings

LOCK_SECONDS = 900
OFFICE = "203.0.113.7"
CAMPUS = "198.51.100.40"
USER = "juan.giraldo"


class FakeClock:
    def __init__(self, now: float = 1_000_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


@pytest.fixture(autouse=True)
def a_fresh_memory_cache(monkeypatch):
    monkeypatch.setenv("CACHE_BACKEND", "memory")
    monkeypatch.setenv("CACHE_NAMESPACE", "")
    reset_cache()
    yield
    reset_cache()


def _lockout(cache=None, *, clock=None, lock_seconds=LOCK_SECONDS, **caps):
    limits = {"pair_attempts": 5, "user_attempts": 20, "client_attempts": 20} | caps
    return LoginLockout(
        cache or InMemoryTTLCache(),
        lock_seconds=lock_seconds,
        clock=clock or FakeClock(),
        **limits,
    )


async def _fail(lockout, times, *, username=USER, client_id=OFFICE):
    return [await lockout.fail(username=username, client_id=client_id) for _ in range(times)]


@pytest.mark.asyncio
async def test_five_failures_of_a_user_from_one_address_block_that_pair():
    lockout = _lockout()

    started = await _fail(lockout, 5)

    assert started == [None, None, None, None, Lockout(LockoutScope.PAIR, LOCK_SECONDS)]
    assert await lockout.blocked(username=USER, client_id=OFFICE) == Lockout(
        LockoutScope.PAIR, LOCK_SECONDS
    )


@pytest.mark.asyncio
async def test_four_failures_do_not_block():
    lockout = _lockout()

    await _fail(lockout, 4)

    assert await lockout.blocked(username=USER, client_id=OFFICE) is None


@pytest.mark.asyncio
async def test_a_blocked_pair_leaves_the_user_free_from_another_address():
    lockout = _lockout()
    await _fail(lockout, 5)

    assert await lockout.blocked(username=USER, client_id=CAMPUS) is None
    assert await lockout.blocked(username="ana.castano", client_id=OFFICE) is None


@pytest.mark.asyncio
async def test_twenty_failures_of_a_user_from_many_addresses_block_the_user_everywhere():
    lockout = _lockout()

    for number in range(20):
        await lockout.fail(username=USER, client_id=f"192.0.2.{number}")

    locked = await lockout.blocked(username=USER, client_id=CAMPUS)
    assert locked == Lockout(LockoutScope.USER, LOCK_SECONDS)
    assert await lockout.blocked(username="ana.castano", client_id=CAMPUS) is None


@pytest.mark.asyncio
async def test_twenty_failures_from_one_address_with_many_users_block_the_address():
    lockout = _lockout()

    for number in range(20):
        await lockout.fail(username=f"persona.{number}", client_id=OFFICE)

    locked = await lockout.blocked(username="ana.castano", client_id=OFFICE)
    assert locked == Lockout(LockoutScope.CLIENT, LOCK_SECONDS)
    assert await lockout.blocked(username="ana.castano", client_id=CAMPUS) is None


@pytest.mark.asyncio
async def test_the_block_tells_how_long_is_left_and_ends_after_the_lock():
    clock = FakeClock()
    lockout = _lockout(clock=clock)
    await _fail(lockout, 5)

    clock.now += LOCK_SECONDS - 60
    left = await lockout.blocked(username=USER, client_id=OFFICE)
    clock.now += 60

    assert left == Lockout(LockoutScope.PAIR, 60)
    assert await lockout.blocked(username=USER, client_id=OFFICE) is None


@pytest.mark.asyncio
async def test_after_the_lock_the_count_starts_again():
    lockout = LoginLockout(
        InMemoryTTLCache(), pair_attempts=2, user_attempts=0, client_attempts=0, lock_seconds=1
    )
    await _fail(lockout, 2)
    assert await lockout.blocked(username=USER, client_id=OFFICE) is not None

    await asyncio.sleep(1.1)

    assert await lockout.blocked(username=USER, client_id=OFFICE) is None
    assert await _fail(lockout, 1) == [None]
    assert await lockout.blocked(username=USER, client_id=OFFICE) is None


@pytest.mark.asyncio
async def test_the_longest_block_wins_when_several_apply():
    clock = FakeClock()
    lockout = _lockout(clock=clock)
    for number in range(20):
        await lockout.fail(username=f"persona.{number}", client_id=OFFICE)
    clock.now += 300
    await _fail(lockout, 5, username="ana.castano")

    locked = await lockout.blocked(username="ana.castano", client_id=OFFICE)
    other = await lockout.blocked(username="luis.mesa", client_id=OFFICE)

    assert locked == Lockout(LockoutScope.PAIR, LOCK_SECONDS)
    assert other == Lockout(LockoutScope.CLIENT, LOCK_SECONDS - 300)


@pytest.mark.asyncio
async def test_the_username_counts_the_same_in_any_case_and_with_spaces():
    lockout = _lockout()

    for username in ("Juan.Giraldo", " juan.giraldo", "JUAN.GIRALDO ", "juan.giraldo", "Juan.giraldo"):
        await lockout.fail(username=username, client_id=OFFICE)

    assert await lockout.blocked(username="juan.GIRALDO", client_id=OFFICE) is not None


@pytest.mark.asyncio
async def test_a_good_login_clears_the_pair_but_not_the_user_count():
    lockout = _lockout(user_attempts=8)
    await _fail(lockout, 4)

    await lockout.succeed(username=USER, client_id=OFFICE)
    await _fail(lockout, 3)

    assert await lockout.blocked(username=USER, client_id=OFFICE) is None
    await _fail(lockout, 1)
    assert await lockout.blocked(username=USER, client_id=OFFICE) == Lockout(
        LockoutScope.USER, LOCK_SECONDS
    )


@pytest.mark.asyncio
async def test_a_failure_without_username_counts_for_the_address_alone():
    cache = InMemoryTTLCache()
    lockout = _lockout(cache, client_attempts=3)

    await _fail(lockout, 3, username=None)

    assert await lockout.blocked(username=None, client_id=OFFICE) == Lockout(
        LockoutScope.CLIENT, LOCK_SECONDS
    )
    assert len([key for key in cache._store if key.startswith("count:")]) == 1


@pytest.mark.asyncio
async def test_a_cap_of_zero_turns_that_scope_off():
    lockout = _lockout(pair_attempts=0)

    await _fail(lockout, 10)

    assert await lockout.blocked(username=USER, client_id=OFFICE) is None


@pytest.mark.asyncio
async def test_a_disabled_lockout_never_counts_nor_blocks():
    cache = InMemoryTTLCache()
    lockout = LoginLockout(
        cache,
        pair_attempts=1,
        user_attempts=1,
        client_attempts=1,
        lock_seconds=LOCK_SECONDS,
        enabled=False,
    )

    assert await _fail(lockout, 3) == [None, None, None]
    assert await lockout.blocked(username=USER, client_id=OFFICE) is None
    assert cache._store == {}


@pytest.mark.asyncio
async def test_the_cache_holds_no_username_nor_address():
    cache = InMemoryTTLCache()
    lockout = _lockout(cache)

    await _fail(lockout, 5)

    keys = " ".join(cache._store)
    assert USER not in keys
    assert OFFICE not in keys
    assert {key.split(":")[1] for key in cache._store} == {"pair", "user", "client"}


@pytest.mark.asyncio
async def test_simultaneous_failures_start_the_block_once():
    lockout = _lockout()
    await _fail(lockout, 4)

    started = await asyncio.gather(
        *(lockout.fail(username=USER, client_id=OFFICE) for _ in range(10))
    )

    assert [lock for lock in started if lock is not None] == [Lockout(LockoutScope.PAIR, LOCK_SECONDS)]


def test_build_login_lockout_reads_the_settings_and_the_shared_cache():
    settings = PersistenceKitSettings(
        stage=DeploymentStage.DEV,
        auth_lockout_pair_attempts=3,
        auth_lockout_user_attempts=7,
        auth_lockout_client_attempts=9,
        auth_lockout_seconds=60,
    )

    lockout = build_login_lockout(settings)

    assert lockout._cache is get_cache(LOGIN_LOCKOUT_CACHE_NAME)
    assert lockout._limits == {
        LockoutScope.PAIR: 3,
        LockoutScope.USER: 7,
        LockoutScope.CLIENT: 9,
    }
    assert (lockout._lock_seconds, lockout._enabled) == (60, True)


def test_build_login_lockout_follows_the_auth_rate_limit_switch():
    local = PersistenceKitSettings(stage=DeploymentStage.LOCAL)
    turned_off = PersistenceKitSettings(stage=DeploymentStage.DEV, auth_rate_limit_enabled=False)

    assert build_login_lockout(local)._enabled is False
    assert build_login_lockout(turned_off)._enabled is False


def test_the_default_caps_are_five_twenty_twenty_and_fifteen_minutes():
    settings = PersistenceKitSettings()

    assert (
        settings.auth_lockout_pair_attempts,
        settings.auth_lockout_user_attempts,
        settings.auth_lockout_client_attempts,
        settings.auth_lockout_seconds,
    ) == (5, 20, 20, 900)
