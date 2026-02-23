from typing import Callable, Any, TypedDict, Type
from persistence_kit.config import Database

class Relation(TypedDict, total=False):
    local_field: str
    target: str
    by: str
    many: bool

class EntityConfig(TypedDict, total=False):
    entity: Type[Any]
    collection: str
    unique: dict[str, str | Callable]
    database: Database
    relations: dict[str, Relation]

ENTITY_CONFIG: dict[str, EntityConfig] = {}

def register_entity(key: str, config: EntityConfig) -> None:
    ENTITY_CONFIG[key] = config

def get_entity_config(key: str) -> EntityConfig:
    return ENTITY_CONFIG[key]

def get_target_table_name(target_key: str) -> str:
    cfg = get_entity_config(target_key)
    return cfg["collection"]

def build_fk_map_from_registry(entity_key: str) -> dict[str, tuple[str, str]]:
    fk_map: dict[str, tuple[str, str]] = {}
    cfg = get_entity_config(entity_key)
    relations = cfg.get("relations", {}) or {}
    for _, r in relations.items():
        local = r["local_field"]
        target_table = get_target_table_name(r["target"])
        target_col = r.get("by", "id")
        fk_map[local] = (target_table, target_col)
    return fk_map
