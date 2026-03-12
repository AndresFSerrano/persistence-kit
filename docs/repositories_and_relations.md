# Repositories And Entity Relations

## Purpose

This document describes the reusable repository stack provided by `persistence_kit`:

- repository contracts
- concrete repositories by backend
- entity registry and relations
- repository factory by backend
- populated view repository
- recommended host application integration

## Current Internal Layout

- `persistence_kit/contracts/`
- `persistence_kit/repository/`
- `persistence_kit/repository_factory/registry/`
- `persistence_kit/repository_factory/factory/`
- `persistence_kit/repository_factory/view/`

The relevant modules are:

- `persistence_kit/contracts/repository.py`
- `persistence_kit/contracts/view_repository.py`
- `persistence_kit/repository_factory/registry/entity_registry.py`
- `persistence_kit/repository_factory/factory/repository_factory.py`
- `persistence_kit/repository_factory/view/populating_repository.py`

## Base Repository (`Repository`)

File: `persistence_kit/contracts/repository.py`

Available methods:

- `add(entity)`
- `get(entity_id)`
- `list(offset=0, limit=50, sort_by=None, sort_desc=False)`
- `update(entity)`
- `delete(entity_id)`
- `get_by_index(index, value)`
- `count()`
- `count_by_fields(criteria)`
- `list_by_fields(criteria, offset=0, limit=50|None, sort_by=None, sort_desc=False)`
- `distinct_values(field_name, criteria=None)`

Implementations:

- `MemoryRepository` in `persistence_kit/repository/memory_repo/`
- `MongoRepository` in `persistence_kit/repository/mongo_repo/`
- `SqlAlchemyRepository` in `persistence_kit/repository/sqlalchemy_repo/`

## View Repository (`ViewRepository`)

File: `persistence_kit/contracts/view_repository.py`

Available methods:

- `get_with(entity_id, include)`
- `get_by_index_with(index, value, include)`
- `count()`
- `count_by_fields(criteria)`
- `list_with(offset=0, limit=50, include=(), sort_by=None, sort_desc=False)`
- `list_by_fields(criteria, offset=0, limit=50|None, include=(), sort_by=None, sort_desc=False)`

Response shape:

- returns `dict` / `list[dict]` with populated relations according to `include`

## `PopulatingRepository`

File: `persistence_kit/repository_factory/view/populating_repository.py`

`PopulatingRepository` wraps a base `Repository` and:

- resolves relations through `entity_registry`
- supports nested includes like `"room.academic_unit"`
- supports nested sorting like `sort_by="room.block_number"`
- supports reverse relations through `target_field`
- supports join-like relations through `through`
- delegates `count`, `count_by_fields` and `distinct_values` to the inner repository

Important notes:

- `include` explicitly defines which relations are populated
- `many=True` returns a populated list
- lookup results are cached within the same query to reduce repeated reads

## Entity Registry And Relations

Files:

- `persistence_kit/repository_factory/registry/entity_registry.py`
- host application bootstrap such as `register_defaults.py`

Each registered entity can define:

- `entity`: domain dataclass
- `collection`: logical collection or table name
- `unique`: logical unique indexes
- `database` (optional): backend override per entity
- `relations` (optional): relation map

Relation fields:

- `local_field`: local field containing the reference
- `target`: target entity key in the registry
- `by`: target field to resolve by, defaults to `id`
- `many`: `True` for one-to-many or many-valued lookup
- `target_field`: reverse field on the target side, or join field pointing to the target when `through` is used
- `through`: join entity key for many-to-many style resolution
- `source_field`: field in the join entity pointing to the source
- `source_by`: lookup field for the source side when `through` is used
- `target_by`: lookup field for the target side when `through` is used

## Repository Factory

File: `persistence_kit/repository_factory/factory/repository_factory.py`

Main functions:

- `set_registry_initializer(fn)`
- `get_repo(entity_key)`
- `get_repo_view(entity_key)`
- `provide_repo(entity_key)` for FastAPI DI
- `provide_view_repo(entity_key)` for FastAPI DI

Behaviour:

- initializes the registry once through the configured initializer
- selects backend using `RepoSettings.repo_database`
- respects entity-level `database` override
- initializes indexes for Mongo and SQL repositories through the DI provider

## Recommended Host App Boundary

Inside a consumer app such as `api_store_manager_v1`, the reusable part stays in `persistence_kit`.

The host app should keep only:

- entity declarations
- local `register_defaults`
- local seeders and their registration
- application/use-case logic

The host app should not reimplement:

- repository contracts
- memory/mongo/postgres repositories
- seed runner infrastructure
- config registry infrastructure
- populated repository logic

## Recommended Imports

Preferred imports for host applications:

```python
from persistence_kit import Repository, ViewRepository, Database
from persistence_kit.repository_factory import (
    register_entity,
    get_repo,
    get_repo_view,
    provide_repo,
    provide_view_repo,
    set_registry_initializer,
)
```

Only use internal paths when you are extending or testing internals of the library itself.

## Filter Operators

File: `persistence_kit/repository/filter_ops.py`

Supported criteria operators:

- `between`
- `in`
- `gte`, `gt`, `lte`, `lt`, `eq`, `ne`

Also supported:

- scalar value: equality
- list value: multi-value filter

## Examples

Simple field query:

```python
rows = await repo.list_by_fields(
    {"academic_unit_id": unit_id, "year": 2026},
    sort_by="year",
    sort_desc=True,
    offset=0,
    limit=100,
)
```

Query with populated relations:

```python
rows = await repo_view.list_by_fields(
    {"calendar_id": calendar_id},
    include=["course", "professor", "room.academic_unit"],
    sort_by="room.block_number",
    sort_desc=False,
    offset=0,
    limit=100,
)
```

## Recommendations

- use `Repository` when you do not need relations
- use `ViewRepository` only when you need populated data
- keep `include` minimal
- prefer paginated and filtered queries over post-filtering in memory
