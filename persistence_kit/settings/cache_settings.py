from enum import Enum

from pydantic_settings import BaseSettings, SettingsConfigDict


class CacheBackend(str, Enum):
    MEMORY = "memory"
    MONGO = "mongo"
    DYNAMODB = "dynamodb"


class CacheSettings(BaseSettings):
    cache_backend: CacheBackend = CacheBackend.MEMORY
    cache_namespace: str = ""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
