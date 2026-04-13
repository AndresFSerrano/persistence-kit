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
- `storage/`: reusable object storage contracts and local/S3 adapters
- `security/`: reusable identity provider contracts, Cognito/memory adapters, and JWT verifiers
- `repository/`: concrete repository implementations by backend
- `repository_factory/`: entity registry, repository creation, and populated view repository

Recommended rule:

- import from `persistence_kit` when the public facade is enough
- import from the internal folders only when you need an implementation-specific module

## Installation

```bash
pip install persistence-kit
```

The base install keeps optional capabilities out of the dependency graph. Enable
only what the host project uses:

```bash
pip install "persistence-kit[api]"
pip install "persistence-kit[security]"
pip install "persistence-kit[security-cognito]"
pip install "persistence-kit[storage-s3]"
pip install "persistence-kit[storage-routes]"
pip install "persistence-kit[dynamodb]"
```

Available capabilities:

- `api`: FastAPI exceptions, pagination helpers, route loading, and error handlers.
- `security`: memory identity provider and JWT verifier.
- `security-cognito`: security plus Cognito and AWS/JWKS dependencies.
- `storage-s3`: S3 object storage adapter.
- `storage-routes`: FastAPI local export download router.
- `dynamodb`: DynamoDB repository backend.
- `all`: every optional capability.

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
    PersistenceKitSettings,
    AuthProvider,
    ExportStorageProvider,
    Database,
    ConfigRegistry,
    configuration,
    SeederProvider,
    ObjectStorage,
    LocalObjectStorage,
    S3ObjectStorage,
    get_export_storage,
    build_local_export_storage_router,
    IdentityProvider,
    MemorySecurityProvider,
    CognitoIdentityProvider,
    MemoryJwtVerifier,
    CognitoJwtVerifier,
    get_identity_provider,
    get_token_verifier,
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

## Object Storage

`persistence_kit.storage` provides driven adapters for storing generated files or
binary objects outside the domain layer:

```python
from persistence_kit.storage import LocalObjectStorage

storage = LocalObjectStorage(
    base_dir=".local",
    public_base_url="http://localhost:8000",
    signing_secret="dev-secret",
)

key = await storage.upload("exports/report.csv", b"id,name\n1,Ada\n", "text/csv")
url = await storage.generate_presigned_url(key)
```

Available adapters:

- `LocalObjectStorage`: stores files under a configured local directory and signs download URLs.
- `S3ObjectStorage`: uploads objects to S3 and returns AWS presigned URLs.

Backward-compatible aliases are exported for applications that previously used
`LocalExportStorageProvider` and `S3ExportStorageProvider`.

`get_export_storage(settings)` builds and caches the configured adapter from a
`PersistenceKitSettings` instance or a subclass inherited by the host app.

For FastAPI applications, `build_local_export_storage_router(...)` exposes a
reusable local download route. The host app passes its settings provider,
optional-current-user dependency, and product authorization callback.

Install `persistence-kit[storage-s3]` before using `S3ObjectStorage` and
`persistence-kit[storage-routes]` before using the FastAPI export route.

## Security

`persistence_kit.security` provides reusable driven adapters for application
authentication flows:

```python
from persistence_kit.security import MemorySecurityProvider, MemoryJwtVerifier

identity = MemorySecurityProvider(
    jwt_secret="dev-secret-with-enough-length",
    jwt_issuer="local-sandbox",
    seed_role_users=True,
    seed_role_codes=("admin", "operator"),
    seed_user_domain="example.org",
)
verifier = MemoryJwtVerifier(secret="dev-secret-with-enough-length", issuer="local-sandbox")
```

Available pieces:

- `IdentityProvider` and `TokenVerifier`: application-facing protocols.
- `MemorySecurityProvider`: local identity provider for tests/sandbox environments.
- `CognitoIdentityProvider`: AWS Cognito identity provider adapter.
- `MemoryJwtVerifier` and `CognitoJwtVerifier`: JWT token verifiers.
- Registration/login/password-reset result dataclasses and helper functions such as `unique_roles`.

Host applications should keep domain-specific roles, authorization policies,
scope rules, and route permission matrices outside the kit.

Install `persistence-kit[security]` for the memory provider/JWT verifier and
`persistence-kit[security-cognito]` for Cognito support.

`PersistenceKitSettings` centralizes common auth, storage, observability, AWS,
and job-service settings. Host applications can inherit from it and override
only product-specific defaults:

```python
from persistence_kit import Database, PersistenceKitSettings


class Settings(PersistenceKitSettings):
    service_name: str = "my-api"
    key_status_history_database: Database = Database.MONGO
    memory_seed_role_codes: tuple[str, ...] = ("admin", "operator")
```

`persistence_kit.security.factory` can build the identity provider and token
verifier from that inherited settings object. `persistence_kit.storage.factory`
does the same for export storage, and `persistence_kit.storage.routes` exposes
the reusable FastAPI local export route.

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
poetry install --with dev --all-extras
poetry run pytest -q
```

Current validation baseline:

- `persistence_kit`: `335 passed`

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
