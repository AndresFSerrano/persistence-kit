from .registry.entity_registry import EntityConfig, Relation, register_entity

__all__ = [
    "EntityConfig",
    "Relation",
    "get_repo",
    "get_repo_view",
    "provide_repo",
    "provide_view_repo",
    "register_entity",
    "set_registry_initializer",
]


def __getattr__(name: str):
    if name in {
        "get_repo",
        "get_repo_view",
        "provide_repo",
        "provide_view_repo",
        "set_registry_initializer",
    }:
        from .factory import repository_factory

        return getattr(repository_factory, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
