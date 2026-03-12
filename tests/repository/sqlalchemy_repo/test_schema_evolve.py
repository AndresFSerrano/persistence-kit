from dataclasses import dataclass

from persistence_kit.repository.sqlalchemy_repo.schema_evolve import (
    _constraint_exists,
    _safe_identifier,
    ensure_foreign_keys,
    ensure_missing_columns,
)


class FakeResult:
    def __init__(self, value):
        self._value = value

    def scalar(self):
        return self._value


class FakeSyncConn:
    def __init__(self):
        self.executed = []
        self._fake_inspector = None

    def execute(self, statement, params=None):
        sql = getattr(statement, "text", str(statement))
        self.executed.append((sql, params))
        return FakeResult(None)


class FakeInspector:
    def __init__(self, cols):
        self.cols = cols

    def get_columns(self, table_name):
        return [{"name": column} for column in self.cols]


@dataclass
class Entity:
    id: int
    name: str
    age: int | None


def test_ensure_missing_columns_adds_columns(monkeypatch):
    import persistence_kit.repository.sqlalchemy_repo.schema_evolve as mod

    conn = FakeSyncConn()
    conn._fake_inspector = FakeInspector({"id"})

    monkeypatch.setattr(mod, "inspect", lambda connection: connection._fake_inspector)

    class FakeType:
        def __call__(self):
            return self

        def compile(self, dialect):
            return "INTEGER"

    monkeypatch.setattr(mod, "_unwrap_optional", lambda tp: (int, True))
    monkeypatch.setattr(mod, "_sa_type", lambda tp: FakeType)

    class TableLike:
        name = "entities"

    ensure_missing_columns(conn, TableLike, Entity)
    sqls = " ".join(sql for sql, _ in conn.executed)
    assert 'ADD COLUMN "name" INTEGER NULL' in sqls
    assert 'ADD COLUMN "age" INTEGER NULL' in sqls


def test_constraint_exists_true():
    from sqlalchemy import text as sa_text

    class Conn:
        def __init__(self):
            self.last = None

        def execute(self, statement, params=None):
            self.last = (statement, params)
            return FakeResult(1)

    conn = Conn()
    assert _constraint_exists(conn, "x", "fk1") is True
    stmt, params = conn.last
    assert isinstance(stmt, type(sa_text("")))
    assert params == {"t": "x", "c": "fk1"}


def test_constraint_exists_false():
    class Conn:
        def execute(self, statement, params=None):
            return FakeResult(None)

    assert _constraint_exists(Conn(), "x", "fk_none") is False


def test_ensure_foreign_keys_creates_fk():
    import persistence_kit.repository.sqlalchemy_repo.schema_evolve as mod

    class Conn(FakeSyncConn):
        pass

    conn = Conn()
    original = mod._constraint_exists
    mod._constraint_exists = lambda c, t, n: False

    class TableLike:
        name = "parent"

    try:
        ensure_foreign_keys(conn, TableLike, {"child_id": ("child_table", "id")})
    finally:
        mod._constraint_exists = original

    sqls = [sql for sql, _ in conn.executed]
    assert 'CREATE INDEX IF NOT EXISTS "idx_parent_child_id"' in sqls[0]
    assert 'ADD CONSTRAINT "fk_parent_child_id__child_table_id"' in sqls[1]


def test_safe_identifier_short_and_long():
    short = "fk_parent_child_id__child_table_id"
    assert _safe_identifier(short) == short

    long_name = "fk_security_feature_resources_resource_id__security_resources_id"
    safe = _safe_identifier(long_name)
    assert len(safe) <= 63
    assert safe != long_name
