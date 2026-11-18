import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from uuid import uuid4

import pytest
from sqlalchemy import MetaData
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import Insert, Select, Update

from persistence_kit.repository import (
    MemoryRepository,
    RepeatedActionError,
    SqlAlchemyRepository,
    SqlDataclassMapper,
    TransactionNotSupportedError,
    TransactionRequiredError,
    build_table_from_dataclass,
    idempotency,
    transaction,
)
from persistence_kit.repository.dynamodb_repo.dynamodb_repo import DynamoRepository
from persistence_kit.repository.mongo_repo.mongo_repo import MongoRepository
from persistence_kit.repository_factory.view.populating_repository import PopulatingRepository


@dataclass
class Counter:
    id: str
    value: int


@dataclass
class Resolution:
    id: int
    request: str


class Boom(Exception):
    pass


def counter_id(counter: Counter) -> str:
    return counter.id


def resolution_id(resolution: Resolution) -> int:
    return resolution.id


def resolution_request(resolution: Resolution) -> str:
    return resolution.request


async def memory_counters(value: int = 0) -> MemoryRepository[Counter, str]:
    counters = MemoryRepository[Counter, str](id_getter=counter_id)
    await counters.add(Counter(id="resolution", value=value))
    return counters


def memory_resolutions() -> MemoryRepository[Resolution, int]:
    return MemoryRepository[Resolution, int](
        id_getter=resolution_id,
        unique_indexes={"by_request": resolution_request},
    )


async def take_next_number(counters, resolutions, request: str) -> int:
    async with transaction():
        counter = await counters.get("resolution", for_update=True)
        await asyncio.sleep(0)
        counter.value += 1
        await counters.update(counter)
        await resolutions.add(Resolution(id=counter.value, request=request))
        return counter.value


@pytest.mark.asyncio
async def test_approvals_at_the_same_time_take_consecutive_numbers():
    counters = await memory_counters()
    resolutions = memory_resolutions()

    numbers = await asyncio.gather(
        *(take_next_number(counters, resolutions, f"request-{n}") for n in range(10))
    )

    assert sorted(numbers) == list(range(1, 11))
    assert (await counters.get("resolution")).value == 10
    assert await resolutions.count() == 10


@pytest.mark.asyncio
async def test_committed_work_stays():
    counters = await memory_counters(value=1)

    async with transaction():
        counter = await counters.get("resolution", for_update=True)
        counter.value = 2
        await counters.update(counter)

    assert (await counters.get("resolution")).value == 2


@pytest.mark.asyncio
async def test_a_failure_undoes_every_write_of_the_transaction():
    counters = await memory_counters(value=12)
    resolutions = memory_resolutions()
    await resolutions.add(Resolution(id=12, request="old"))

    with pytest.raises(Boom):
        async with transaction():
            counter = await counters.get("resolution", for_update=True)
            counter.value += 1
            await counters.update(counter)
            await resolutions.add(Resolution(id=counter.value, request="new"))
            await resolutions.delete(12)
            raise Boom()

    assert (await counters.get("resolution")).value == 12
    assert await resolutions.get(13) is None
    assert (await resolutions.get(12)).request == "old"
    assert await resolutions.get_by_index("by_request", "new") is None
    assert (await resolutions.get_by_index("by_request", "old")).id == 12


@pytest.mark.asyncio
async def test_a_failed_approval_hands_its_number_to_the_next_one():
    counters = await memory_counters()
    resolutions = memory_resolutions()

    with pytest.raises(Boom):
        async with transaction():
            counter = await counters.get("resolution", for_update=True)
            counter.value += 1
            await counters.update(counter)
            raise Boom()

    next_number = await asyncio.wait_for(take_next_number(counters, resolutions, "request"), timeout=1)
    assert next_number == 1


@pytest.mark.asyncio
async def test_an_update_after_a_plain_read_is_undone_too():
    counters = await memory_counters(value=5)

    with pytest.raises(Boom):
        async with transaction():
            counter = await counters.get("resolution")
            counter.value = 6
            await counters.update(counter)
            raise Boom()

    assert (await counters.get("resolution")).value == 5


@pytest.mark.asyncio
async def test_an_entity_read_in_a_transaction_changes_only_through_update():
    counters = await memory_counters(value=5)

    async with transaction():
        counter = await counters.get("resolution")
        counter.value = 99

    assert (await counters.get("resolution")).value == 5


@pytest.mark.asyncio
async def test_outside_a_transaction_memory_keeps_returning_the_stored_entity():
    counters = await memory_counters(value=1)

    assert await counters.get("resolution") is await counters.get("resolution")


@pytest.mark.asyncio
async def test_a_nested_transaction_joins_the_outer_one():
    counters = await memory_counters(value=1)

    with pytest.raises(Boom):
        async with transaction():
            async with transaction():
                counter = await counters.get("resolution", for_update=True)
                counter.value = 2
                await counters.update(counter)
            raise Boom()

    assert (await counters.get("resolution")).value == 1


@pytest.mark.asyncio
async def test_locking_a_row_needs_a_transaction():
    counters = await memory_counters()

    with pytest.raises(TransactionRequiredError):
        await counters.get("resolution", for_update=True)
    with pytest.raises(TransactionRequiredError):
        await sql_repo(FakeEngine(), Counter, "counters").get("resolution", for_update=True)


class FakeResult:
    def __init__(self, rows, rowcount=1):
        self._rows = rows
        self.rowcount = rowcount

    def mappings(self):
        return self

    def first(self):
        return self._rows[0] if self._rows else None


class FakeConnection:
    def __init__(self, rows):
        self.rows = list(rows)
        self.statements = []
        self.calls = []
        self.keys = set()

    async def begin(self):
        self.calls.append("begin")

    async def execute(self, stmt):
        self.statements.append(stmt)
        if isinstance(stmt, Select) and self.rows:
            return FakeResult(self.rows.pop(0))
        if isinstance(stmt, Insert) and stmt.table.name == "idempotency_keys":
            key = stmt.compile(dialect=postgresql.dialect()).params["key"]
            if key in self.keys:
                return FakeResult([], rowcount=0)
            self.keys.add(key)
        return FakeResult([])

    async def commit(self):
        self.calls.append("commit")

    async def rollback(self):
        self.calls.append("rollback")

    async def close(self):
        self.calls.append("close")


class FakeEngine:
    def __init__(self, rows=()):
        self.connection = FakeConnection(rows)
        self.connects = 0

    async def connect(self):
        self.connects += 1
        return self.connection

    def begin(self):
        raise AssertionError("work inside a transaction must not open its own connection")


def sql_repo(engine, entity, table_name):
    table = build_table_from_dataclass(entity, table_name, MetaData())
    repo = SqlAlchemyRepository(engine, SqlDataclassMapper(entity, table), table_name)
    repo._inited = True
    return repo


@pytest.mark.asyncio
async def test_sql_repositories_share_one_connection_and_commit_once():
    engine = FakeEngine(rows=[[{"id": "resolution", "value": 4}]])
    counters = sql_repo(engine, Counter, "counters")
    resolutions = sql_repo(engine, Resolution, "resolutions")

    async with transaction():
        counter = await counters.get("resolution", for_update=True)
        counter.value += 1
        await counters.update(counter)
        await resolutions.add(Resolution(id=counter.value, request="request"))

    assert engine.connects == 1
    assert engine.connection.calls == ["begin", "commit", "close"]
    lock, update, insert = engine.connection.statements
    assert "FOR UPDATE" in str(lock.compile(dialect=postgresql.dialect()))
    assert isinstance(update, Update)
    assert isinstance(insert, Insert)
    assert insert.compile().params["id"] == 5


@pytest.mark.asyncio
async def test_a_failure_rolls_the_sql_work_back():
    engine = FakeEngine()
    counters = sql_repo(engine, Counter, "counters")

    with pytest.raises(Boom):
        async with transaction():
            await counters.update(Counter(id="resolution", value=1))
            raise Boom()

    assert engine.connection.calls == ["begin", "rollback", "close"]


@pytest.mark.asyncio
@pytest.mark.parametrize("repository_type", [MongoRepository, DynamoRepository])
async def test_mongo_and_dynamo_refuse_to_write_inside_a_transaction(repository_type):
    repo = repository_type.__new__(repository_type)

    async with transaction():
        with pytest.raises(TransactionNotSupportedError):
            await repo.add(Counter(id="resolution", value=1))
        with pytest.raises(TransactionNotSupportedError):
            await repo.update(Counter(id="resolution", value=1))
        with pytest.raises(TransactionNotSupportedError):
            await repo.delete("resolution")


@pytest.mark.asyncio
@pytest.mark.parametrize("repository_type", [MongoRepository, DynamoRepository])
async def test_mongo_and_dynamo_cannot_lock_a_row(repository_type):
    repo = repository_type.__new__(repository_type)

    async with transaction():
        with pytest.raises(TransactionNotSupportedError):
            await repo.get("resolution", for_update=True)


class RecordingRepository:
    def __init__(self):
        self.calls = []

    async def get(self, entity_id, **kwargs):
        self.calls.append((entity_id, kwargs))


def no_related_repository(entity_key):
    raise AssertionError(entity_key)


@pytest.mark.asyncio
async def test_the_view_repository_passes_the_lock_on_only_when_asked():
    inner = RecordingRepository()
    view = PopulatingRepository("counter", inner, resolve_repo=no_related_repository)

    async with transaction():
        await view.get("a", for_update=True)
    await view.get("b")

    assert inner.calls == [("a", {"for_update": True}), ("b", {})]


def action_key() -> str:
    return f"approve-request:{uuid4()}"


async def approve(resolutions, key: str, request: str, fail: bool = False) -> None:
    async with transaction(idempotency_key=key):
        await asyncio.sleep(0)
        await resolutions.add(Resolution(id=len(await resolutions.list()) + 1, request=request))
        if fail:
            raise Boom()


@pytest.fixture
def memory_keys(monkeypatch):
    monkeypatch.setenv("REPO_DATABASE", "memory")


@pytest.mark.asyncio
async def test_an_action_marked_with_a_key_runs_only_once(memory_keys):
    resolutions = memory_resolutions()
    key = action_key()

    await approve(resolutions, key, "first")
    with pytest.raises(RepeatedActionError) as repeated:
        await approve(resolutions, key, "second")

    assert repeated.value.key == key
    assert [r.request for r in await resolutions.list()] == ["first"]


@pytest.mark.asyncio
async def test_a_failed_action_leaves_its_key_free(memory_keys):
    resolutions = memory_resolutions()
    key = action_key()

    with pytest.raises(Boom):
        await approve(resolutions, key, "fails", fail=True)
    await approve(resolutions, key, "retry")

    assert [r.request for r in await resolutions.list()] == ["retry"]


@pytest.mark.asyncio
async def test_the_same_action_at_the_same_time_runs_only_once(memory_keys):
    resolutions = memory_resolutions()
    key = action_key()

    results = await asyncio.gather(
        approve(resolutions, key, "first"),
        approve(resolutions, key, "double click"),
        return_exceptions=True,
    )

    assert results[0] is None
    assert isinstance(results[1], RepeatedActionError)
    assert [r.request for r in await resolutions.list()] == ["first"]


@pytest.mark.asyncio
async def test_the_waiting_action_runs_when_the_first_one_fails(memory_keys):
    resolutions = memory_resolutions()
    key = action_key()

    results = await asyncio.gather(
        approve(resolutions, key, "fails", fail=True),
        approve(resolutions, key, "waits"),
        return_exceptions=True,
    )

    assert isinstance(results[0], Boom)
    assert results[1] is None
    assert [r.request for r in await resolutions.list()] == ["waits"]


@pytest.mark.asyncio
async def test_different_keys_do_not_wait_for_each_other(memory_keys):
    resolutions = memory_resolutions()

    await asyncio.gather(
        approve(resolutions, action_key(), "first"),
        approve(resolutions, action_key(), "second"),
    )

    assert sorted(r.request for r in await resolutions.list()) == ["first", "second"]


@pytest.mark.asyncio
async def test_a_nested_key_belongs_to_the_outer_transaction(memory_keys):
    resolutions = memory_resolutions()
    key = action_key()

    with pytest.raises(Boom):
        async with transaction():
            await approve(resolutions, key, "inner")
            raise Boom()
    await approve(resolutions, key, "again")

    assert [r.request for r in await resolutions.list()] == ["again"]


class DdlConnection:
    def __init__(self, engine):
        self._engine = engine

    async def execute(self, stmt):
        self._engine.ddl_statements.append(stmt)

    async def run_sync(self, fn, *args, **kwargs):
        self._engine.tables_created += 1


class KeyEngine(FakeEngine):
    def __init__(self):
        super().__init__()
        self.tables_created = 0
        self.ddl_statements = []

    @asynccontextmanager
    async def begin(self):
        yield DdlConnection(self)


@pytest.mark.asyncio
async def test_postgres_keeps_the_key_in_the_same_transaction(monkeypatch):
    engine = KeyEngine()

    def current_engine():
        return engine

    monkeypatch.setenv("REPO_DATABASE", "postgres")
    monkeypatch.setattr(idempotency, "get_engine", current_engine)
    resolutions = sql_repo(engine, Resolution, "resolutions")
    key = action_key()

    async with transaction(idempotency_key=key):
        await resolutions.add(Resolution(id=1, request="first"))
    with pytest.raises(RepeatedActionError):
        async with transaction(idempotency_key=key):
            await resolutions.add(Resolution(id=2, request="second"))

    claim, add, repeated = engine.connection.statements
    assert "ON CONFLICT (key) DO NOTHING" in str(claim.compile(dialect=postgresql.dialect()))
    assert add.compile().params["id"] == 1
    assert repeated.table.name == "idempotency_keys"
    assert engine.connects == 2
    assert engine.tables_created == 1
    (create_lock,) = engine.ddl_statements
    assert "pg_advisory_xact_lock" in str(create_lock.compile(dialect=postgresql.dialect()))
    assert engine.connection.calls == ["begin", "commit", "close", "begin", "rollback", "close"]


@pytest.mark.asyncio
@pytest.mark.parametrize("database", ["mongo", "dynamodb"])
async def test_mongo_and_dynamo_cannot_keep_idempotency_keys(monkeypatch, database):
    monkeypatch.setenv("REPO_DATABASE", database)

    with pytest.raises(TransactionNotSupportedError):
        async with transaction(idempotency_key=action_key()):
            pass
