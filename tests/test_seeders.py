from typing import Optional

import pytest

from persistence_kit.bootstrap.seeders import SeederProvider


def _reset_provider():
    SeederProvider._instance = None  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_singleton_instance():
    _reset_provider()
    assert SeederProvider() is SeederProvider()


@pytest.mark.asyncio
async def test_register_single_seeder_object():
    _reset_provider()
    provider = SeederProvider()
    called = []

    class FakeSeeder:
        async def run(self) -> None:
            called.append("run")

    provider.register(FakeSeeder())
    await provider.run_all()
    assert called == ["run"]


@pytest.mark.asyncio
async def test_register_single_callable_seeder():
    _reset_provider()
    provider = SeederProvider()
    called = []

    async def seeder_fn() -> None:
        called.append("fn")

    provider.register(seeder_fn)
    await provider.run_all()
    assert called == ["fn"]


@pytest.mark.asyncio
async def test_register_iterable_of_seeders_mixed():
    _reset_provider()
    provider = SeederProvider()
    called = []

    class SeederA:
        async def run(self) -> None:
            called.append("A")

    class SeederB:
        async def run(self) -> None:
            called.append("B")

    async def seeder_fn() -> None:
        called.append("FN")

    provider.register([SeederA(), SeederB(), seeder_fn])
    await provider.run_all()
    assert called == ["A", "B", "FN"]


@pytest.mark.asyncio
async def test_run_all_only_once():
    _reset_provider()
    provider = SeederProvider()
    called = []

    async def seeder_fn() -> None:
        called.append(1)

    provider.register(seeder_fn)
    await provider.run_all()
    await provider.run_all()
    assert called == [1]
