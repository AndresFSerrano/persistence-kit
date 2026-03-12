from sqlalchemy.ext.asyncio import AsyncEngine

from persistence_kit.repository.sqlalchemy_repo.sqlalchemy_engine import get_engine


class FakeSettingsWithDSN:
    postgres_dsn = "postgresql+asyncpg://user:pass@host/db"


class FakeSettingsFull:
    postgres_dsn = None
    postgres_user = "u"
    postgres_password = "p"
    postgres_host = "h"
    postgres_port = 5432
    postgres_db = "d"


def test_engine_uses_direct_dsn(monkeypatch):
    import persistence_kit.repository.sqlalchemy_repo.sqlalchemy_engine as mod

    monkeypatch.setattr(mod, "RepoSettings", lambda: FakeSettingsWithDSN())
    get_engine.cache_clear()
    engine = get_engine()
    assert isinstance(engine, AsyncEngine)
    assert engine.url.username == "user"
    assert engine.url.host == "host"
    assert engine.url.database == "db"


def test_engine_builds_url_from_components(monkeypatch):
    import persistence_kit.repository.sqlalchemy_repo.sqlalchemy_engine as mod

    monkeypatch.setattr(mod, "RepoSettings", lambda: FakeSettingsFull())
    get_engine.cache_clear()
    engine = get_engine()
    assert isinstance(engine, AsyncEngine)
    assert engine.url.username == "u"
    assert engine.url.host == "h"
    assert engine.url.database == "d"
    assert engine.url.port == 5432


def test_engine_is_cached(monkeypatch):
    import persistence_kit.repository.sqlalchemy_repo.sqlalchemy_engine as mod

    monkeypatch.setattr(mod, "RepoSettings", lambda: FakeSettingsWithDSN())
    get_engine.cache_clear()
    assert get_engine() is get_engine()
