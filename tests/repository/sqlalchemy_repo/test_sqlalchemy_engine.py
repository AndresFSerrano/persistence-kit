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


class FakeSqliteSettings:
    sqlite_path = "data/app.db"


class FakeSqliteSettingsWithoutPath:
    sqlite_path = None


def test_sqlite_engine_uses_the_configured_file(monkeypatch):
    import persistence_kit.repository.sqlalchemy_repo.sqlalchemy_engine as mod
    from persistence_kit import Database

    monkeypatch.setattr(mod, "RepoSettings", FakeSqliteSettings)
    get_engine.cache_clear()
    engine = get_engine(Database.SQLITE)
    assert engine.url.drivername == "sqlite+aiosqlite"
    assert engine.url.database == "data/app.db"
    assert get_engine(Database.SQLITE) is engine
    get_engine.cache_clear()


def test_sqlite_engine_requires_a_path(monkeypatch):
    import pytest
    import persistence_kit.repository.sqlalchemy_repo.sqlalchemy_engine as mod
    from persistence_kit import Database

    monkeypatch.setattr(mod, "RepoSettings", FakeSqliteSettingsWithoutPath)
    get_engine.cache_clear()
    with pytest.raises(ValueError, match="SQLITE_PATH"):
        get_engine(Database.SQLITE)


def test_sqlite_engine_names_the_missing_extra(monkeypatch):
    import pytest
    import persistence_kit.repository.sqlalchemy_repo.sqlalchemy_engine as mod
    from persistence_kit import Database

    monkeypatch.setattr(mod, "RepoSettings", FakeSqliteSettings)
    monkeypatch.setattr(mod, "find_spec", FakeMissingSpec.find)
    get_engine.cache_clear()
    with pytest.raises(ModuleNotFoundError, match=r"persistence-kit\[sqlite\]"):
        get_engine(Database.SQLITE)


class FakeMissingSpec:
    @staticmethod
    def find(_name):
        return None


def test_sqlite_helpers_mirror_postgres_functions():
    from persistence_kit.repository.sqlalchemy_repo.sqlalchemy_engine import _regexp_replace, _translate

    assert _translate("Ñandú", "Ññú", "Nnu") == "Nandu"
    assert _translate(None, "a", "b") is None
    assert _regexp_replace("a--b--c", "-+", " ", "g") == "a b c"
    assert _regexp_replace("a--b--c", "-+", " ", "") == "a b--c"
    assert _regexp_replace(None, "-", " ", "g") is None
