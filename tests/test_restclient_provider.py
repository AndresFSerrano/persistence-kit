import pytest

from persistence_kit.restclient import (
    default_rest_registry,
    get_rest_client,
    provide_rest_client,
    register_rest_service,
    reset_rest_registry,
    set_rest_registry_initializer,
)
from persistence_kit.restclient.client import HttpxRestClient


@pytest.fixture(autouse=True)
def _clean_registry():
    reset_rest_registry()
    set_rest_registry_initializer(None)
    yield
    reset_rest_registry()
    set_rest_registry_initializer(None)


def test_provide_rest_client_returns_client_from_default_registry():
    set_rest_registry_initializer(
        lambda: register_rest_service("geo", base_url="https://api.test")
    )

    client = provide_rest_client("geo")()

    assert isinstance(client, HttpxRestClient)


def test_env_map_overrides_registered_base_url(monkeypatch):
    monkeypatch.setenv("REST_SERVICE_URLS", '{"geo": "https://mirror.test"}')
    reset_rest_registry()

    register_rest_service("geo", base_url="https://api.test")

    assert default_rest_registry()._services["geo"].resolver.base_url == "https://mirror.test"


def test_initializer_runs_only_once():
    calls = {"n": 0}

    def init():
        calls["n"] += 1
        register_rest_service("geo", base_url="https://api.test")

    set_rest_registry_initializer(init)

    get_rest_client("geo")
    get_rest_client("geo")

    assert calls["n"] == 1
