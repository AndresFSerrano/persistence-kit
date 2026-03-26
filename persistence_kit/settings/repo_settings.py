from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from persistence_kit.settings.constants import Database
from persistence_kit.settings.parsers import split_csv_list


class RepoSettings(BaseSettings):
    repo_database: Database = Database.MEMORY
    cors_origins: list[str] | None = None

    mongo_dsn: str | None = None
    mongo_db: str | None = None

    postgres_dsn: str | None = None
    postgres_user: str | None = None
    postgres_password: str | None = None
    postgres_host: str | None = None
    postgres_port: int | None = 5432
    postgres_db: str | None = None
    postgres_ssl: bool = False

    dynamodb_table_prefix: str = ""
    dynamodb_region: str = "us-east-1"

    @field_validator("cors_origins", mode="before")
    def split_origins(cls, value):
        return split_csv_list(value)

    model_config = SettingsConfigDict(extra="ignore")
