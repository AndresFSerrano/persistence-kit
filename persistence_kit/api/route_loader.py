import importlib
import logging
import pkgutil
from types import ModuleType
from typing import Iterable, Sequence

from fastapi import APIRouter

logger = logging.getLogger(__name__)


def _iter_route_modules(package_name: str, package_path: Sequence[str]) -> Iterable[ModuleType]:
    for mod in pkgutil.walk_packages(package_path, prefix=package_name + "."):
        name = mod.name
        leaf = name.rsplit(".", 1)[-1]
        if leaf.startswith("_"):
            continue
        try:
            module = importlib.import_module(name)
        except Exception as exc:
            logger.warning("No se pudo importar el módulo de rutas '%s': %s", name, exc)
            continue
        yield module


def build_api_router(
    routes_package: str,
    *,
    prefix: str = "/api",
    excluded_modules: Sequence[str] | None = None,
) -> APIRouter:
    package = importlib.import_module(routes_package)
    package_path = getattr(package, "__path__", None)
    if package_path is None:
        raise ValueError(f"El paquete de rutas '{routes_package}' no expone __path__.")

    excluded = set(excluded_modules or ())
    api = APIRouter(prefix=prefix)
    for module in _iter_route_modules(routes_package, package_path):
        full_name = module.__name__
        leaf_name = full_name.rsplit(".", 1)[-1]
        if full_name in excluded or leaf_name in excluded:
            continue
        router = getattr(module, "router", None)
        if isinstance(router, APIRouter):
            api.include_router(router)
    return api
