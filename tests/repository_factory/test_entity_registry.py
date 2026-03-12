import pytest

from persistence_kit import Database
from persistence_kit.repository_factory.registry.entity_registry import (
    ENTITY_CONFIG,
    build_fk_map_from_registry,
    get_entity_config,
    get_target_table_name,
    register_entity,
)


class DummyEntity:
    pass


@pytest.fixture(autouse=True)
def clean_entity_config():
    ENTITY_CONFIG.clear()
    yield
    ENTITY_CONFIG.clear()


def test_register_and_get_entity_config():
    config = {
        "entity": DummyEntity,
        "collection": "dummy_collection",
        "unique": {"id": "id"},
        "database": Database.MEMORY,
        "relations": {},
    }

    register_entity("dummy", config)
    result = get_entity_config("dummy")

    assert result is config
    assert result["entity"] is DummyEntity
    assert result["collection"] == "dummy_collection"
    assert result["unique"] == {"id": "id"}
    assert result["database"] is Database.MEMORY


def test_get_entity_config_raises_keyerror_for_unknown_key():
    with pytest.raises(KeyError):
        get_entity_config("unknown")


def test_get_target_table_name_returns_collection():
    config = {
        "entity": DummyEntity,
        "collection": "dummy_table",
        "unique": {},
        "database": Database.MEMORY,
        "relations": {},
    }
    register_entity("dummy", config)

    table_name = get_target_table_name("dummy")
    assert table_name == "dummy_table"


def test_build_fk_map_from_registry_with_explicit_by():
    register_entity(
        "target",
        {
            "entity": DummyEntity,
            "collection": "target_table",
            "unique": {},
            "database": Database.MEMORY,
            "relations": {},
        },
    )

    register_entity(
        "source",
        {
            "entity": DummyEntity,
            "collection": "source_table",
            "unique": {},
            "database": Database.MEMORY,
            "relations": {
                "rel": {
                    "local_field": "target_id",
                    "target": "target",
                    "by": "uuid",
                    "many": False,
                }
            },
        },
    )

    fk_map = build_fk_map_from_registry("source")

    assert fk_map == {
        "target_id": ("target_table", "uuid"),
    }


def test_build_fk_map_from_registry_uses_default_id():
    register_entity(
        "room",
        {
            "entity": DummyEntity,
            "collection": "rooms",
            "unique": {},
            "database": Database.MEMORY,
            "relations": {},
        },
    )

    register_entity(
        "key",
        {
            "entity": DummyEntity,
            "collection": "keys",
            "unique": {},
            "database": Database.MEMORY,
            "relations": {
                "room_rel": {
                    "local_field": "room_id",
                    "target": "room",
                    "many": False,
                }
            },
        },
    )

    fk_map = build_fk_map_from_registry("key")

    assert fk_map == {
        "room_id": ("rooms", "id"),
    }


def test_build_fk_map_from_registry_without_relations():
    register_entity(
        "empty",
        {
            "entity": DummyEntity,
            "collection": "nothing",
            "unique": {},
            "database": Database.MEMORY,
            "relations": {},
        },
    )

    fk_map = build_fk_map_from_registry("empty")

    assert fk_map == {}


def test_build_fk_map_from_registry_ignores_reverse_relations_and_through():
    register_entity(
        "room",
        {
            "entity": DummyEntity,
            "collection": "rooms",
            "unique": {},
            "database": Database.MEMORY,
            "relations": {
                "keys": {
                    "local_field": "id",
                    "target": "key",
                    "target_field": "room_id",
                    "many": True,
                },
                "users": {
                    "target": "user",
                    "through": "room_user",
                    "many": True,
                },
            },
        },
    )
    register_entity(
        "key",
        {
            "entity": DummyEntity,
            "collection": "keys",
            "unique": {},
            "database": Database.MEMORY,
            "relations": {},
        },
    )

    fk_map = build_fk_map_from_registry("room")

    assert fk_map == {}
