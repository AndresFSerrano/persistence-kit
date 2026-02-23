# persistence-kit

Reusable persistence toolkit with async repository implementations for:

- `memory`
- `mongo` (Motor)
- `postgres` (SQLAlchemy async + asyncpg)

Author: Andres Felipe Serrano Barrios

## Installation

```bash
pip install persistence-kit
```

## Quick Start

```python
from persistence_kit.repository_factory import register_entity, get_repo
from persistence_kit.config import Database

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

## Supported Environment Variables

- `REPO_DATABASE=memory|mongo|postgres`
- `MONGO_DSN`
- `MONGO_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_DB`

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
