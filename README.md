# persistence-kit

Reusable persistence toolkit with async repository implementations for:

- `memory`
- `mongo` (Motor)
- `postgres` (SQLAlchemy async + asyncpg)

Documentation:

- `docs/repositories_and_relations.md`

Author: Andres Felipe Serrano Barrios

## Package Structure

`persistence_kit` is organized by responsibility:

- `contracts/`: repository interfaces
- `settings/`: shared settings, parsers, and persistence constants
- `api/`: reusable API exceptions, handlers, and route loading helpers
- `bootstrap/`: startup helpers, configuration registry, and seed orchestration
- `utils/`: transversal helpers such as upsert utilities
- `repository/`: concrete repository implementations by backend
- `repository_factory/`: entity registry, repository creation, and populated view repository

Recommended rule:

- import from `persistence_kit` when the public facade is enough
- import from the internal folders only when you need an implementation-specific module

## Installation

```bash
pip install persistence-kit
```

## Quick Start

```python
from persistence_kit import Database
from persistence_kit.repository_factory import get_repo, register_entity

# register entities during application startup
register_entity(
    "user",
    {
        "entity": User,
        "collection": "users",
        "database": Database.MEMORY,
        "unique": {"email": "email"},
    },
)

repo = get_repo("user")
```

## Public API

Preferred public imports:

```python
from persistence_kit import (
    Repository,
    ViewRepository,
    RepoSettings,
    Database,
    ConfigRegistry,
    configuration,
    SeederProvider,
    build_api_router,
    handle_service_errors,
    handle_repository_errors,
    NotFoundException,
    ValidationException,
    BusinessRuleException,
    DatabaseException,
)
from persistence_kit.repository_factory import (
    register_entity,
    get_repo,
    get_repo_view,
    provide_repo,
    provide_view_repo,
    set_registry_initializer,
)
```

Use internal paths only for implementation details, for example:

- `persistence_kit.repository.sqlalchemy_repo.sqlalchemy_repo`
- `persistence_kit.repository_factory.factory.repository_factory`
- `persistence_kit.repository_factory.registry.entity_registry`
- `persistence_kit.repository_factory.view.populating_repository`

## Typical Host Application Flow

1. Define your entities as dataclasses.
2. Register them in a local bootstrap such as `register_defaults`.
3. Call `set_registry_initializer(register_defaults)` during application startup.
4. Resolve repositories through `get_repo(...)`, `get_repo_view(...)`, or FastAPI providers.
5. Use `ConfigRegistry` and `SeederProvider` only as shared bootstrap infrastructure. The concrete registrations remain in the host app.

## Supported Environment Variables

- `REPO_DATABASE=memory|mongo|postgres`
- `MONGO_DSN`
- `MONGO_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_DB`

## Local Development

Create the local environment and run tests from the library root:

```bash
poetry lock
poetry install --with dev
poetry run pytest -q
```

Current validation baseline:

- `persistence_kit`: `138 passed`

## Publish to PyPI (Manual)

```bash
python -m pip install --upgrade build twine
python -m build
python -m twine check dist/*
python -m twine upload dist/*
```

## Automated Publish via GitHub Actions

This repository includes a workflow at `.github/workflows/publish-pypi.yml`.

It publishes to PyPI when:

- a GitHub Release is published
- the release tag points to the current `main` HEAD

Prerequisite:

- Configure PyPI Trusted Publishing for this repository and workflow file.

## Preview Releases Without PRs

Use `.github/workflows/publish-preview.yml` to publish directly from GitHub Actions
without merging a PR.

How it works:

- Trigger `Publish Preview Package` manually from the Actions tab.
- Enter a `version` (for example `0.1.1.dev1` or `0.1.2.dev1`).
- Choose target repository: `testpypi` (recommended) or `pypi`.
- The workflow patches `pyproject.toml` version only inside the CI run, builds, and publishes.
- No commit and no PR are required for this preview publish flow.

Important:

- Prefer `*.devN` versions for preview builds.
- PyPI/TestPyPI do not allow re-uploading the same file version.

## Local Test Releases (No PR Required)

If you want to test changes from your machine in external projects without opening a PR,
publish a prerelease from local code to TestPyPI.

### 1. Create a TestPyPI token

- Create an API token in TestPyPI.
- Export it as environment variable:

```bash
export TWINE_PASSWORD="pypi-***"
```

### 2. Publish from local code

```bash
bash ./scripts/publish-local.sh 0.1.1.dev1 testpypi
```

You can publish another local iteration with a new version:

```bash
bash ./scripts/publish-local.sh 0.1.1.dev2 testpypi
```

### 3. Install from external projects

```bash
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple persistence-kit==0.1.1.dev1
```

## Recommended Release Strategy

- Local experimental testing: publish `0.x.y.devN` to TestPyPI from local machine.
- Official release: publish `0.x.y` to PyPI through GitHub Release (`publish-pypi.yml`).
