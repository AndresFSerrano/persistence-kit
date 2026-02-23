from enum import Enum

from pydantic_settings import BaseSettings, SettingsConfigDict


class Database(str, Enum):
    MEMORY = "memory"
    MONGO = "mongo"
    POSTGRES = "postgres"


class RepoSettings(BaseSettings):
    repo_database: Database = Database.MEMORY

    mongo_dsn: str | None = None
    mongo_db: str | None = None

    postgres_user: str | None = None
    postgres_password: str | None = None
    postgres_host: str | None = None
    postgres_port: int | None = 5432
    postgres_db: str | None = None

    # Keep the library environment-agnostic. Host applications decide
    # whether settings come from .env, process env vars, secret stores, etc.
    model_config = SettingsConfigDict(extra="ignore")
