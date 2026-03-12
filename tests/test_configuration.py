import importlib
import types

cfg_mod = importlib.import_module("persistence_kit.bootstrap.configuration")
from persistence_kit.bootstrap.configuration import ConfigRegistry, configuration


def setup_function(_):
    ConfigRegistry.reset()
    ConfigRegistry.configure_package("app.core")


def test_register_functions():
    called = []

    @configuration
    def fn1():
        called.append("fn1")

    @configuration
    def fn2():
        called.append("fn2")

    assert len(ConfigRegistry._funcs) == 2
    assert set(ConfigRegistry._seen) == {id(fn1), id(fn2)}


def test_register_avoids_duplicates():
    @configuration
    def fn():
        return None

    configuration(fn)
    assert len(ConfigRegistry._funcs) == 1
    assert len(ConfigRegistry._seen) == 1


def test_run_all_executes_in_order(monkeypatch):
    called = []

    @configuration
    def a():
        called.append("a")

    @configuration
    def b():
        called.append("b")

    class FakePkg:
        __name__ = "app.core"
        __path__ = ["fakepath"]

    monkeypatch.setattr(importlib, "import_module", lambda name: FakePkg())
    monkeypatch.setattr("pkgutil.iter_modules", lambda _: [])

    ConfigRegistry.run_all()

    assert called == ["a", "b"]
    assert ConfigRegistry._initialized is True


def test_run_all_only_once(monkeypatch):
    count = {"x": 0}

    @configuration
    def fn():
        count["x"] += 1

    class FakePkg:
        __path__ = []
        __name__ = "app.core"

    monkeypatch.setattr(importlib, "import_module", lambda name: FakePkg())
    monkeypatch.setattr(
        "pkgutil.iter_modules",
        lambda x: [(None, "x_config", None), (None, "y_config", None), (None, "ignore", None)],
    )

    ConfigRegistry.run_all()
    ConfigRegistry.run_all()

    assert count["x"] == 1


def test_scan_imports_matching_modules(monkeypatch):
    imported = []

    class FakePkg:
        __name__ = "app.core"
        __path__ = ["fakepath"]

    def fake_import(name):
        imported.append(name)
        return FakePkg()

    monkeypatch.setattr(importlib, "import_module", fake_import)
    monkeypatch.setattr(
        "pkgutil.iter_modules",
        lambda path: [(None, "x_config", None), (None, "y_config", None), (None, "other", None)],
    )

    ConfigRegistry._scan()

    assert "app.core.x_config" in imported
    assert "app.core.y_config" in imported
    assert "app.core.other" not in imported
    assert ConfigRegistry._scanned is True


def test_scan_imports_only_config_modules(monkeypatch):
    imported = []

    class FakePkg:
        __name__ = "app.core"
        __path__ = ["fakepath"]

    def fake_import_module(name):
        imported.append(name)
        if name == ConfigRegistry._pkg_name:
            return FakePkg()
        return types.SimpleNamespace()

    def fake_iter_modules(path):
        return [(None, "x_config", None), (None, "y_config", None), (None, "other", None)]

    monkeypatch.setattr(cfg_mod.importlib, "import_module", fake_import_module)
    monkeypatch.setattr(cfg_mod.pkgutil, "iter_modules", fake_iter_modules)

    ConfigRegistry._scan()

    assert ConfigRegistry._scanned is True
    assert ConfigRegistry._pkg_name in imported
    assert "app.core.x_config" in imported
    assert "app.core.y_config" in imported
    assert "app.core.other" not in imported


def test_scan_early_return_when_already_scanned(monkeypatch):
    calls = []

    def fake_import_module(name):
        calls.append(name)
        return types.SimpleNamespace(__name__=name, __path__=["p"])

    def fake_iter_modules(path):
        return [(None, "x_config", None)]

    monkeypatch.setattr(cfg_mod.importlib, "import_module", fake_import_module)
    monkeypatch.setattr(cfg_mod.pkgutil, "iter_modules", fake_iter_modules)

    ConfigRegistry._scan()
    assert calls

    calls.clear()
    ConfigRegistry._scan()

    assert calls == []


def test_reset_clears_registry():
    @configuration
    def a():
        return None

    assert len(ConfigRegistry._funcs) == 1
    ConfigRegistry.reset()
    assert ConfigRegistry._funcs == []
    assert ConfigRegistry._seen == set()
    assert ConfigRegistry._initialized is False
    assert ConfigRegistry._scanned is False
