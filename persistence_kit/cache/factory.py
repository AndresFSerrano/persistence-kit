from __future__ import annotations

from functools import lru_cache

from persistence_kit.cache.contracts import Cache
from persistence_kit.settings.cache_settings import CacheBackend, CacheSettings


@lru_cache
def _mongo_collection(dsn: str, dbname: str, name: str):
    from motor.motor_asyncio import AsyncIOMotorClient

    return AsyncIOMotorClient(dsn, uuidRepresentation="standard")[dbname][name]


@lru_cache
def _dynamodb_cache_table(prefix: str, region: str):
    import boto3

    table_name = f"{prefix}cache" if prefix else "cache"
    return boto3.resource("dynamodb", region_name=region).Table(table_name)


@lru_cache
def _cache_cached(name: str, backend: CacheBackend) -> Cache:
    if backend is CacheBackend.MEMORY:
        from persistence_kit.cache.memory import InMemoryTTLCache

        return InMemoryTTLCache()

    if backend is CacheBackend.MONGO:
        from persistence_kit.cache.mongo import MongoCache
        from persistence_kit.settings.repo_settings import RepoSettings

        repo = RepoSettings()
        collection = _mongo_collection(repo.mongo_dsn, repo.mongo_db, f"cache_{name}")
        return MongoCache(collection)

    if backend is CacheBackend.DYNAMODB:
        from persistence_kit.cache.dynamodb import DynamoCache
        from persistence_kit.cache.namespaced import NamespacedCache
        from persistence_kit.settings.repo_settings import RepoSettings

        cache_settings = CacheSettings()
        region = RepoSettings().dynamodb_region
        table = _dynamodb_cache_table(cache_settings.cache_dynamodb_table_prefix, region)
        return NamespacedCache(DynamoCache(table), name)

    raise ValueError(f"Cache backend no soportado: {backend}")


def get_cache(name: str = "default") -> Cache:
    """Devuelve el cache para ``name``, con el backend elegido por el setting
    ``CACHE_BACKEND`` (gemelo de ``get_repo`` con ``REPO_DATABASE``).

    Si ``CACHE_NAMESPACE`` esta seteado, todas las claves se prefijan con el, para
    aislar apps que comparten un mismo cache (p. ej. una tabla DynamoDB comun).
    """
    settings = CacheSettings()
    cache = _cache_cached(name, settings.cache_backend)
    if settings.cache_namespace:
        from persistence_kit.cache.namespaced import NamespacedCache

        return NamespacedCache(cache, settings.cache_namespace)
    return cache


def reset_cache() -> None:
    _cache_cached.cache_clear()
    _mongo_collection.cache_clear()
    _dynamodb_cache_table.cache_clear()
