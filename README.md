# persistence-kit

The plumbing every one of our services rewrites: repositories, settings, object
storage, authentication, an HTTP client and a cache. You describe your entities
once and the kit gives you an async repository for them, backed by memory,
MongoDB, PostgreSQL or DynamoDB, without your domain code knowing which one.

Nothing here knows about your business. Roles, permissions, and domain rules stay
in your application.

- Python 3.11+ · async everywhere · type hints shipped (`py.typed`)
- Optional dependencies stay out of the graph until you ask for them

## Five minutes

```bash
pip install persistence-kit
```

```python
from dataclasses import dataclass, field
from uuid import UUID, uuid4

from persistence_kit import Database
from persistence_kit.repository_factory import get_repo, register_entity


@dataclass
class User:
    email: str
    name: str
    id: UUID = field(default_factory=uuid4)


register_entity("user", {
    "entity": User,
    "collection": "users",
    "database": Database.MEMORY,
    "unique": {"email": "email"},
})

repo = get_repo("user")
await repo.add(User(email="ada@example.org", name="Ada"))
ada = await repo.get_by_index("email", "ada@example.org")
```

The repository protocol is small and the same for every backend: `add`, `get`,
`update`, `delete`, `get_by_index`, `list`, `list_by_fields`, `count` and
`count_by_fields`.

Switch `database` to `Database.POSTGRES` and the same code talks to PostgreSQL.
That is the whole point of the kit.

## The one idea: the entity registry

Everything else hangs off a single dictionary. You register each entity once,
usually in a `register_defaults()` function in your app, and from then on you ask
for repositories by key.

```python
from persistence_kit.repository_factory import set_registry_initializer

set_registry_initializer(register_defaults)   # called during startup
```

What goes in a registration:

| Key | Meaning |
| --- | --- |
| `entity` | The dataclass this key maps to |
| `collection` | Table or collection name in the backing store |
| `database` | `Database.MEMORY`, `MONGO`, `POSTGRES` or `DYNAMODB`. Defaults to `REPO_DATABASE` |
| `unique` | Indexed lookups, `{"index_name": "field"}`, used by `get_by_index` |
| `relations` | How this entity joins to others, including many-to-many through a pivot |

Relations are what `get_repo_view(...)` reads to return an entity with its
related rows already populated, so you do not write joins by hand. The shapes and
the traps are in [`docs/repositories_and_relations.md`](docs/repositories_and_relations.md).

Four ways to reach a repository, all equivalent:

```python
get_repo("user")                    # plain repository
get_repo_view("user")               # repository that populates relations
provide_repo("user")                # FastAPI dependency
provide_view_repo("user")           # FastAPI dependency, populated
```

## Installation and extras

The base install stays light. Each capability that needs a heavy dependency
lives behind an extra, so a service that only talks to Mongo never installs
`boto3` or `fastapi`.

```bash
pip install "persistence-kit[api,security,restclient]"
```

| Extra | Gives you | Pulls in |
| --- | --- | --- |
| `api` | FastAPI exceptions, pagination, route loading, error handlers | fastapi, python-multipart |
| `security` | Memory identity provider and JWT verifier | fastapi, pyjwt, python-multipart |
| `security-cognito` | The above plus AWS Cognito and JWKS | + boto3 |
| `storage-s3` | S3 object storage adapter | boto3 |
| `storage-routes` | FastAPI route to serve local exports | fastapi, python-multipart |
| `dynamodb` | DynamoDB repository backend | boto3 |
| `restclient` | The REST client and its cache | httpx |
| `encrypted` | Encrypted request and response payloads | fastapi, cryptography |
| `testing` | Everything the test suite needs | fastapi, pyjwt, httpx, python-multipart, cryptography |
| `all` | Every optional capability | all of the above |

Importing `persistence_kit` never loads an optional dependency by itself. Ask for
something you did not install and you get a message telling you which extra to
add, not an obscure `ImportError`.

## What is in the box

| Module | What it does |
| --- | --- |
| `contracts/` | The protocols: `Repository`, `ViewRepository`, `ObjectStorage`, `IdentityProvider` |
| `repository/` | One implementation per backend: memory, Mongo, SQLAlchemy, DynamoDB |
| `repository_factory/` | The entity registry, the factory, and the view repository that populates relations |
| `settings/` | `RepoSettings` and `PersistenceKitSettings`, plus shared enums and parsers |
| `storage/` | Object storage: local directory or S3, with presigned URLs |
| `security/` | Identity providers and JWT verifiers, memory or Cognito, and the encrypted envelope format |
| `restclient/` | HTTP client with pluggable auth, endpoint resolution, retries and DTO decoding |
| `cache/` | Key-value cache with TTL: memory, Mongo or DynamoDB |
| `resilience/` | Circuit breaker, shared by anything that calls out |
| `api/` | Reusable FastAPI pieces: error handlers, pagination, rate limiting, encrypted routes |
| `bootstrap/` | Startup helpers, configuration registry, seed orchestration |
| `utils/` | Small transversal helpers such as upserts |

Import from `persistence_kit` when the public facade is enough. Reach into a
subpackage only when you need something implementation-specific. The root
exports about a hundred names, so let your editor complete them rather than
keeping a list here.

## Object storage

Writing generated files without your domain layer knowing where they land.

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

`LocalObjectStorage` writes to a directory and signs its own download URLs.
`S3ObjectStorage` uploads to S3 and returns AWS presigned URLs. Pick one with
`get_export_storage(settings)`, which reads your settings and caches the adapter.

FastAPI apps can mount `build_local_export_storage_router(...)` to serve local
files, passing their settings provider, an optional current-user dependency, and
an authorization callback.

Needs `[storage-s3]` for S3 and `[storage-routes]` for the route.

## Security

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

Two protocols, `IdentityProvider` and `TokenVerifier`, with a memory pair for
tests and local work and a Cognito pair for real deployments. Registration,
login, and password-reset results come back as dataclasses, not raw dicts.
`get_identity_provider(settings)` and `get_token_verifier(settings)` build the
right pair from your settings.

Your roles, authorization policies and route permission matrices stay in your
application. The kit only answers "who is this".

Needs `[security]`, or `[security-cognito]` for Cognito.

## Encrypted payloads

For data that should not be readable between the browser and your handler, not
even to a proxy that terminates TLS.

```python
from fastapi import APIRouter, FastAPI

from persistence_kit import (
    EncryptedResponseMiddleware,
    build_encrypted_route,
    encrypted,
)

router = APIRouter(route_class=build_encrypted_route(get_settings))


@router.post("/login", openapi_extra=encrypted(fields=["password"], response_fields=["token"]))
async def login(payload: LoginRequest) -> LoginResponse:
    ...


app = FastAPI()
app.add_middleware(EncryptedResponseMiddleware)
app.include_router(router)
```

The client generates one AES-256-GCM key per request, wraps it with the server's
public key, and sends it inside the envelope. The route unwraps it and opens the
payload before your handler runs; the middleware encrypts whatever comes back with
the same key, including the errors your own `exception_handler` builds. Your
handler never sees an envelope, and both pieces are required.

Two modes. `encrypted()` encrypts the whole body and the whole response.
`encrypted(fields=[...])` encrypts named fields only, so the rest of the payload
stays readable; paths may be nested and may cross lists, as in `items[].price`.
Those paths are validated when the route is mounted — against your Pydantic body
when there is one — so a typo fails at startup rather than quietly leaving the
field unencrypted.

By default the private key is generated in memory at startup, so encrypted routes
work with no configuration. Set `ENCRYPTED_TYPE=local` to load a fixed base64 PEM
from `ENCRYPTED_PRIVATE_KEY`, or `ENCRYPTED_TYPE=production` to keep it inside AWS
KMS. Clients fetch the matching public key from your own endpoint, built on
`public_key_der_b64(provider)`.

Envelopes carry a timestamp, expire after sixty seconds, and each nonce is
accepted once, so a captured request cannot be replayed. Anything malformed,
expired, replayed or tampered with comes back as a 400.

In `local` stage a plain body is allowed through, so you can call your API from
Swagger while developing.

Needs `[encrypted]`.

## REST client and cache

For calling somebody else's HTTP API without writing the same client again.
Register a service once, then ask for its client:

```python
from persistence_kit.restclient import get_rest_client, register_rest_service, ServiceConfig

register_rest_service(
    "billing",
    base_url="https://billing.example.org/api",
    config=ServiceConfig(
        default_headers={"X-Api-Key": settings.billing_key},
        timeout_seconds=10,
        cacheable=True,
        cache_ttl_seconds=300,
    ),
)

client = get_rest_client("billing")
```

It brings pluggable authentication (`NoAuth`, `ApiKeyAuth`, `BasicAuth`,
`BearerAuth`, `OAuth2ClientCredentials`, `LoginTokenAuth`), endpoint resolution
that can read a service directory, a retry policy, DTO decoding through
`decode`, and optional response caching. `MemoryRestClient` stands in for the
real one in tests.

Base URLs can be overridden per environment with `REST_SERVICE_URLS`, a JSON map
of service name to URL, without touching the registration.

The cache is a plain key-value store with TTL, chosen by `CACHE_BACKEND` the
same way repositories are chosen by `REPO_DATABASE`. Set `CACHE_NAMESPACE` when
several apps share one DynamoDB table or Mongo collection so their keys do not
collide.

Both are covered in depth in [`docs/restclient_and_cache.md`](docs/restclient_and_cache.md).

Needs `[restclient]`.

## Resilience

`CircuitBreaker` is domain-agnostic and shared by anything that calls out: after
enough consecutive failures it opens, fails fast with `CircuitOpenError` instead
of hammering a service that is already down, and closes again after a cooldown.
`RetryPolicy`, which lives with the REST client, handles the retry side.

## Settings

`PersistenceKitSettings` gathers what most services need: auth, storage,
observability, AWS and job-service settings. Inherit from it and override only
what is yours.

```python
from persistence_kit import Database, PersistenceKitSettings


class Settings(PersistenceKitSettings):
    service_name: str = "my-api"
    key_status_history_database: Database = Database.MONGO
    memory_seed_role_codes: tuple[str, ...] = ("admin", "operator")
```

The factories read that object: `get_identity_provider`, `get_token_verifier`,
`get_export_storage`, `get_cache`.

### Environment variables

| Variable | Meaning |
| --- | --- |
| `REPO_DATABASE` | `memory`, `mongo`, `postgres` or `dynamodb`. Default backend for every entity |
| `CACHE_BACKEND` | `memory`, `mongo` or `dynamodb`. Defaults to `memory` |
| `CACHE_NAMESPACE` | Key prefix, so apps sharing a cache store do not collide |
| `REST_SERVICE_URLS` | JSON map `{"service": "base_url"}` overriding registered REST base URLs |
| `MONGO_DSN`, `MONGO_DB` | Mongo connection |
| `POSTGRES_DSN` | Full DSN, if you prefer it to the pieces below |
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB` | Postgres connection |
| `POSTGRES_SSL` | Set when the server requires TLS |
| `DYNAMODB_REGION`, `DYNAMODB_TABLE_PREFIX` | DynamoDB connection |
| `ENCRYPTED_PRIVATE_KEY` | Base64 PEM of the RSA private key that unwraps encrypted payloads |
| `ENCRYPTED_TYPE` | `memory` (default, key generated at startup), `local`, or `production` for AWS KMS |
| `KMS_KEY_ID` | The KMS key that unwraps the AES key, required by `production` |

## Wiring it into an application

1. Define your entities as dataclasses.
2. Register them in a `register_defaults()` in your app.
3. Call `set_registry_initializer(register_defaults)` at startup.
4. Resolve repositories with `get_repo(...)`, `get_repo_view(...)` or the FastAPI providers.
5. Use `ConfigRegistry` and `SeederProvider` as bootstrap infrastructure only. What gets registered and seeded stays in your app.

## Trying a change against an app on your machine

This is the everyday loop, and it needs no release, no PR and no TestPyPI. It
assumes the kit and the app sit next to each other:

```
aux programación udea/
├── persistence_kit/          <- this repository
└── api_store_manager_v1/     <- the app that consumes it
```

Install the kit into the app's environment in editable mode, from the app:

```bash
cd api_store_manager_v1
.venv/Scripts/pip install -e ../persistence_kit      # Linux/macOS: .venv/bin/pip
```

From that moment the app imports your working copy. Edit the kit, rerun the app
or its tests, and the change is already there. No reinstall between edits.

To go back to the published version:

```bash
.venv/Scripts/pip uninstall persistence-kit
poetry install
```

Two things that will confuse you if nobody warns you:

- **The reported version lies.** In editable mode `importlib.metadata.version("persistence-kit")`
  keeps returning whatever the old `dist-info` said, even though the code running
  is your source. Trust `persistence_kit.__file__`, not the version string.
- **The app pins an exact version.** `api_store_manager_v1` asks for a pinned
  `persistence-kit` with its extras, so `poetry install` will happily undo your
  editable install. Redo it after any dependency change.

Run the kit's own tests with the app's interpreter, which already has every extra
installed:

```bash
"../api_store_manager_v1/.venv/Scripts/python.exe" -m pytest -q
```

Only when the change has to be tried from a machine that is not yours does it
make sense to publish a preview, which is the next section.

## Adding something to the kit

Every capability here is built the same way. Follow the shape and your module
will look like it was always part of the kit.

**1. Start with the contract.** Declare what the thing does before writing how.
The repository pair uses `ABC`; everything else uses `Protocol`, so an
implementation only has to match the shape, not inherit from anything.

```python
from typing import Protocol


class Mailer(Protocol):
    async def send(self, *, to: str, subject: str, body: str) -> None: ...
```

Everything is `async` and arguments are keyword-only. Both rules exist so callers
read clearly at the call site and so adding a parameter never breaks anyone.

**2. Write two implementations, not one.** A memory one and a real one. The
memory one is not a toy: it is what tests and local development run against, and
having it forces the contract to stay honest. `InMemoryTTLCache` next to the
Mongo cache, `MemorySecurityProvider` next to Cognito, `MemoryRestClient` next to
the httpx one.

**3. Choose between them with a factory driven by settings.** Never with an `if`
scattered around the app. The factory reads an enum from settings, caches with
`lru_cache`, and imports the provider lazily inside the builder so an unused
backend never loads its dependency.

```python
@lru_cache
def get_mailer(settings):
    if settings.mailer_backend is MailerBackend.SES:
        from persistence_kit.mailer.ses import SesMailer   # imported only if used
        return SesMailer(region=settings.aws_region)
    return InMemoryMailer()
```

**4. Give the package its own error hierarchy.** One base exception and specific
subclasses, the way `storage/errors.py` and `restclient/errors.py` do it. Callers
should be able to catch the whole family or one precise case.

**5. Export it lazily.** If it needs an optional dependency, add it to
`_OPTIONAL_EXPORTS` in the root `__init__.py` and declare its extra in
`pyproject.toml`. Someone who imports `persistence_kit` without that extra must
get a message naming the extra, not an `ImportError`. `tests/test_capabilities.py`
enforces this and will fail if you skip it.

**6. Add tests next to the feature.** Async tests carry `@pytest.mark.asyncio`.

**Getting it merged.** `main` is protected, so it goes through a pull request.
Branch off `main`, keep the change to one capability, run the suite, and open the
PR. Anything domain-specific, roles, business rules, product settings, belongs in
the application that consumes the kit, not here.

## Development

From the kit's root, with its own environment:

```bash
poetry lock
poetry install --with dev --all-extras
poetry run pytest -q
```

Current baseline: **515 tests passing** (version 3.10.0). Async tests use
`pytest-asyncio` in strict mode, so each one carries `@pytest.mark.asyncio`.

`tests/test_capabilities.py` guards the lazy-import promise: it fails if merely
importing `persistence_kit` drags in fastapi, pyjwt or boto3.

## Releasing

The normal path, and the only one that produces an official version:

1. Merge to `main`. The branch is protected, so it goes through a pull request.
2. Bump `version` in `pyproject.toml`.
3. Publish a GitHub Release with tag `vX.Y.Z` pointing at `main` HEAD.
4. `.github/workflows/publish-pypi.yml` checks the tag matches `main`, builds, and
   publishes to PyPI through Trusted Publishing. No tokens stored anywhere.

For previews that others need to install before there is a release, use a
`X.Y.Z.devN` version:

- From GitHub: run the `Publish Preview Package` workflow, enter the version, and
  pick `testpypi`. It patches the version inside the CI run only, so no commit.
- From your machine: `bash ./scripts/publish-local.sh 3.9.2.dev1 testpypi`, with a
  TestPyPI token exported as `TWINE_PASSWORD`.

Then, in the consuming project:

```bash
pip install --index-url https://test.pypi.org/simple/ \
            --extra-index-url https://pypi.org/simple persistence-kit==3.9.2.dev1
```

Neither PyPI nor TestPyPI lets you re-upload a version, so bump the `devN` on
every iteration.

## Further reading

- [`docs/repositories_and_relations.md`](docs/repositories_and_relations.md): relations, pivots, and how the view repository populates them.
- [`docs/restclient_and_cache.md`](docs/restclient_and_cache.md): the REST client, its auth strategies, and the cache.
- [`docs/encrypted_payloads.md`](docs/encrypted_payloads.md): the envelope format, the key providers, and what the client has to do.

Author: Andres Felipe Serrano Barrios · [github.com/AndresFSerrano/persistence-kit](https://github.com/AndresFSerrano/persistence-kit)
