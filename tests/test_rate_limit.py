import pytest

pytest.importorskip("fastapi")
httpx = pytest.importorskip("httpx")

from fastapi import Depends, FastAPI

from persistence_kit.api import build_auth_rate_limit_dependency
from persistence_kit.settings import DeploymentStage, PersistenceKitSettings


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
