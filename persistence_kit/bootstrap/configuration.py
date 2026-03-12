import importlib
import pkgutil
from typing import Callable


class ConfigRegistry:
    _funcs: list[Callable[[], None]] = []
    _seen: set[int] = set()
    _initialized = False
    _scanned = False
    _pkg_name = "app.core"

    @classmethod
    def configure_package(cls, pkg_name: str) -> None:
        cls._pkg_name = pkg_name
        cls._scanned = False

    @classmethod
    def register(cls, fn: Callable[[], None]) -> Callable[[], None]:
        fid = id(fn)
        if fid not in cls._seen:
            cls._funcs.append(fn)
            cls._seen.add(fid)
        return fn

    @classmethod
    def _scan(cls) -> None:
        if cls._scanned:
            return
        pkg = importlib.import_module(cls._pkg_name)
        names = sorted(n for _, n, _ in pkgutil.iter_modules(pkg.__path__) if n.endswith("_config"))
        for name in names:
            importlib.import_module(f"{pkg.__name__}.{name}")
        cls._scanned = True

    @classmethod
    def run_all(cls) -> None:
        if cls._initialized:
            return
        cls._scan()
        for fn in cls._funcs:
            fn()
        cls._initialized = True

    @classmethod
    def reset(cls) -> None:
        cls._funcs.clear()
        cls._seen.clear()
        cls._initialized = False
        cls._scanned = False


def configuration(fn: Callable[[], None]) -> Callable[[], None]:
    return ConfigRegistry.register(fn)


def set_config_package(pkg_name: str) -> None:
    ConfigRegistry.configure_package(pkg_name)
