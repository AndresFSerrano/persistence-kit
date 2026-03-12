import types

import pytest
from fastapi import APIRouter

import persistence_kit.api.route_loader as loader


def test_iter_route_modules_filters_hidden_and_import_errors(monkeypatch):
    fake_pkgs = [
        types.SimpleNamespace(name="app.infrastructure.routes.alpha"),
        types.SimpleNamespace(name="app.infrastructure.routes._hidden"),
        types.SimpleNamespace(name="app.infrastructure.routes.bad"),
    ]

    def fake_walk_packages(path, prefix):
        return fake_pkgs

    imported = {}

    def fake_import_module(name):
        if name.endswith(".bad"):
            raise RuntimeError("boom")
        module = types.ModuleType(name)
        imported[name] = module
        return module

    monkeypatch.setattr(loader.pkgutil, "walk_packages", fake_walk_packages)
    monkeypatch.setattr(loader.importlib, "import_module", fake_import_module)
    warnings = []
    monkeypatch.setattr(
        loader.logger,
        "warning",
        lambda msg, name, exc: warnings.append((msg, name, str(exc))),
    )

    modules = list(loader._iter_route_modules("app.infrastructure.routes", ["ignored"]))

    assert len(modules) == 1
    assert "app.infrastructure.routes.alpha" in imported
    assert "app.infrastructure.routes._hidden" not in imported
    assert "app.infrastructure.routes.bad" not in imported
    assert len(warnings) == 1
    assert warnings[0][1] == "app.infrastructure.routes.bad"


@pytest.mark.asyncio
async def test_build_api_router_includes_only_valid_routers(monkeypatch):
    alpha = types.ModuleType("app.infrastructure.routes.alpha")
    beta = types.ModuleType("app.infrastructure.routes.beta")

    alpha.router = APIRouter()
    beta.router = "not_a_router"

    @alpha.router.get("/ping")
    async def ping():
        return {"ok": True}

    monkeypatch.setattr(
        loader.importlib,
        "import_module",
        lambda name: types.SimpleNamespace(__path__=["ignored"])
        if name == "app.infrastructure.routes"
        else None,
    )
    monkeypatch.setattr(
        loader,
        "_iter_route_modules",
        lambda package_name, package_path: [alpha, beta],
    )

    api = loader.build_api_router("app.infrastructure.routes", prefix="/api")

    assert isinstance(api, APIRouter)
    paths = {route.path for route in api.routes}
    assert "/api/ping" in paths


@pytest.mark.asyncio
async def test_build_api_router_excludes_modules_by_name(monkeypatch):
    auth_mod = types.ModuleType("app.infrastructure.routes.auth_router")
    security_admin_mod = types.ModuleType("app.infrastructure.routes.security_admin_router")
    core_mod = types.ModuleType("app.infrastructure.routes.room_router")
    auth_mod.router = APIRouter()
    security_admin_mod.router = APIRouter()
    core_mod.router = APIRouter()

    @auth_mod.router.get("/auth/ping")
    async def auth_ping():
        return {"auth": True}

    @core_mod.router.get("/rooms/ping")
    async def room_ping():
        return {"room": True}

    @security_admin_mod.router.get("/security-admin/ping")
    async def security_admin_ping():
        return {"security_admin": True}

    monkeypatch.setattr(
        loader.importlib,
        "import_module",
        lambda name: types.SimpleNamespace(__path__=["ignored"])
        if name == "app.infrastructure.routes"
        else None,
    )
    monkeypatch.setattr(
        loader,
        "_iter_route_modules",
        lambda package_name, package_path: [auth_mod, security_admin_mod, core_mod],
    )

    api = loader.build_api_router(
        "app.infrastructure.routes",
        prefix="/api",
        excluded_modules=["auth_router", "security_admin_router"],
    )
    paths = {route.path for route in api.routes}

    assert "/api/rooms/ping" in paths
    assert "/api/auth/ping" not in paths
    assert "/api/security-admin/ping" not in paths
