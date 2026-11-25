from dataclasses import dataclass
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from persistence_kit import Database
from persistence_kit.repository.sqlalchemy_repo.sqlalchemy_engine import get_engine
from persistence_kit.repository.sqlalchemy_repo.sqlalchemy_repo import SqlAlchemyRepository
from persistence_kit.repository.transaction import transaction
from persistence_kit.repository_factory.factory.repository_factory import (
    _init_registry,
    _repo_cached,
    get_repo,
)
from persistence_kit.repository_factory.registry.entity_registry import ENTITY_CONFIG


@dataclass
class SqliteTeam:
    id: UUID
    name: str


@dataclass
class SqlitePlayer:
    id: UUID
    name: str
    team_id: UUID
    mentors: list[UUID] | None = None
    nickname: str | None = None


@dataclass
class SqliteLegacy:
    id: UUID
    name: str
    email: str


ENTITIES = {
    "sqlite_team": {
        "entity": SqliteTeam,
        "collection": "kit_teams",
        "unique": {"by_name": "name"},
    },
    "sqlite_player": {
        "entity": SqlitePlayer,
        "collection": "kit_players",
        "unique": {"by_name": "name"},
        "indexes": {"by_team": ["team_id"]},
        "relations": {"team": {"local_field": "team_id", "target": "sqlite_team"}},
    },
    "sqlite_legacy": {"entity": SqliteLegacy, "collection": "kit_legacy"},
}


@pytest_asyncio.fixture
async def sqlite_db(tmp_path, monkeypatch):
    monkeypatch.setenv("REPO_DATABASE", "sqlite")
    monkeypatch.setenv("SQLITE_PATH", str(tmp_path / "kit.db"))
    suffix = uuid4().hex[:8]
    for key, config in ENTITIES.items():
        monkeypatch.setitem(ENTITY_CONFIG, key, {**config, "collection": f"{config['collection']}_{suffix}"})
    get_engine.cache_clear()
    _repo_cached.cache_clear()
    _init_registry.cache_clear()
    engine = get_engine(Database.SQLITE)
    yield engine
    await engine.dispose()
    get_engine.cache_clear()
    _repo_cached.cache_clear()


async def _object_names(engine, kind: str) -> set[str]:
    async with engine.connect() as conn:
        rows = await conn.execute(text("SELECT name FROM sqlite_master WHERE type = :kind"), {"kind": kind})
        return {row[0] for row in rows}


@pytest.mark.asyncio
async def test_get_repo_uses_a_sqlite_engine(sqlite_db):
    repo = get_repo("sqlite_team")

    assert isinstance(repo, SqlAlchemyRepository)
    assert sqlite_db.url.drivername == "sqlite+aiosqlite"


@pytest.mark.asyncio
async def test_crud_round_trip_keeps_uuid_lists(sqlite_db):
    teams = get_repo("sqlite_team")
    players = get_repo("sqlite_player")
    team = SqliteTeam(id=uuid4(), name="Owls")
    mentors = [uuid4(), uuid4()]
    player = SqlitePlayer(id=uuid4(), name="Ana", team_id=team.id, mentors=mentors)

    await teams.add(team)
    await players.add(player)
    stored = await players.get(player.id)

    assert stored == player
    assert stored.mentors == mentors

    stored.nickname = "Lightning"
    await players.update(stored)
    assert (await players.get_by_index("by_name", "Ana")).nickname == "Lightning"

    await players.delete(player.id)
    assert await players.get(player.id) is None


@pytest.mark.asyncio
async def test_unique_index_rejects_a_duplicate(sqlite_db):
    teams = get_repo("sqlite_team")
    await teams.add(SqliteTeam(id=uuid4(), name="Owls"))

    with pytest.raises(IntegrityError):
        await teams.add(SqliteTeam(id=uuid4(), name="Owls"))


@pytest.mark.asyncio
async def test_icontains_ignores_accents_case_and_punctuation(sqlite_db):
    teams = get_repo("sqlite_team")
    await teams.add(SqliteTeam(id=uuid4(), name="Club Atlético Ñandú"))
    await teams.add(SqliteTeam(id=uuid4(), name="Herons"))

    found = await teams.list_by_fields({"name": {"icontains": "atletico nandu"}})
    starts = await teams.list_by_fields({"name": {"istartswith": "CLUB-ATLE"}})

    assert [team.name for team in found] == ["Club Atlético Ñandú"]
    assert [team.name for team in starts] == ["Club Atlético Ñandú"]


@pytest.mark.asyncio
async def test_relations_get_an_index_and_composite_indexes_are_created(sqlite_db):
    await get_repo("sqlite_team").count()
    await get_repo("sqlite_player").count()

    indexes = await _object_names(sqlite_db, "index")

    players_table = ENTITY_CONFIG["sqlite_player"]["collection"]
    assert f"idx_{players_table}_team_id" in indexes
    assert f"ix_by_team_{players_table}" in indexes
    assert f"uniq_by_name_{players_table}" in indexes


@pytest.mark.asyncio
async def test_missing_required_column_is_added_to_an_existing_table(sqlite_db):
    async with sqlite_db.begin() as conn:
        legacy_table = ENTITY_CONFIG["sqlite_legacy"]["collection"]
        await conn.execute(text(f'CREATE TABLE "{legacy_table}" (id CHAR(32) PRIMARY KEY, name VARCHAR NOT NULL)'))
    legacy = get_repo("sqlite_legacy")
    row = SqliteLegacy(id=uuid4(), name="Old", email="old@example.test")

    await legacy.add(row)

    assert await legacy.get(row.id) == row


@pytest.mark.asyncio
async def test_transaction_rolls_back_and_locks_rows_without_failing(sqlite_db):
    teams = get_repo("sqlite_team")
    team = SqliteTeam(id=uuid4(), name="Owls")
    await teams.add(team)

    with pytest.raises(RuntimeError):
        async with transaction():
            locked = await teams.get(team.id, for_update=True)
            locked.name = "Renamed"
            await teams.update(locked)
            await teams.add(SqliteTeam(id=uuid4(), name="Ghost"))
            raise RuntimeError("abort")

    assert (await teams.get(team.id)).name == "Owls"
    assert await teams.count() == 1
