# REST client and cache

The `restclient` module consumes external HTTP APIs the same way `repository`
consumes databases: you declare each service once by name, and resolve a client
on demand. The `cache` module is a generic key-value store with TTL, selected by
an environment variable exactly like `REPO_DATABASE`, and the REST client can use
it to cache responses transparently.

## REST client

### Declare services and resolve clients

Register each external API once (usually in a host `register_rest_defaults`) and
wire the initializer at startup, mirroring the repository factory:

```python
from persistence_kit.restclient import (
    ServiceConfig,
    register_rest_service,
    set_rest_registry_initializer,
)

def register_rest_defaults() -> None:
    register_rest_service(
        "countriesnow",
        base_url="https://countriesnow.space/api/v0.1",
    )

# at startup:
set_rest_registry_initializer(register_rest_defaults)
```

Resolve a client anywhere by name:

```python
from persistence_kit.restclient import get_rest_client, provide_rest_client

client = get_rest_client("countriesnow")                       # plain resolution
# FastAPI dependency (twin of provide_repo):
Dep = Annotated[RestClient, Depends(provide_rest_client("countriesnow"))]
```

Base URLs can be overridden per environment without code changes via
`REST_SERVICE_URLS` (a JSON map `{"service": "base_url"}`); the `base_url` passed
to `register_rest_service` is the fallback default.

### The `RestClient` interface

```python
async def request(
    method, service, *,
    params=None, json=None, xml=None, content=None,
    content_type=None, soap_action=None, headers=None,
    cache_ttl=None,
) -> RestResponse
```

`RestResponse` decodes by `Content-Type`: `.json()`, `.xml()`, `.text`, or
`.body()` (auto). The client is content-agnostic (JSON / XML / SOAP).

### `ServiceConfig`

Per-service configuration passed as `config=` to `register_rest_service`:

| Field | Purpose |
| --- | --- |
| `default_headers` | headers sent on every request (e.g. auth tokens) |
| `timeout_seconds` | request timeout |
| `max_retries` | retry attempts on 5xx/429/timeouts |
| `verify_tls` | TLS verification |
| `raise_for_status` | raise `RestHTTPError` on non-2xx |
| `cacheable` | enable the cache layer for per-call caching (no default TTL) |
| `cache_ttl_seconds` | default cache TTL (enables caching for the service) |
| `cache_stale_while_revalidate_seconds` | extra window to serve stale and revalidate in the background |

### Endpoint resolution, auth, resilience, mapping

- **Resolvers**: `StaticEndpointResolver` (fixed base URL) and
  `DirectoryEndpointResolver` (resolves `{service}` from a remote directory with a
  TTL cache and injectable `fetch`).
- **Auth** (pluggable strategies): `NoAuth`, `ApiKeyAuth` (header or query),
  `BearerAuth`, `BasicAuth`, `OAuth2ClientCredentials`, `LoginTokenAuth`
  (login -> token with cache and refresh on 401).
- **Resilience**: `RetryPolicy` (backoff + jitter, retries 5xx/429 and
  timeouts/transport) and `CircuitBreaker` (from `persistence_kit.resilience`).
- **Mapping** (`ModelMappingMixin`): `decode(response, list[Dto], select="data")`
  maps a response into DTOs, plus `get_as` / `post_as` / `request_as` sugar.
  `select` extracts a nested field (e.g. `"data"` or `"object"`) before mapping.

## Cache

A generic key-value cache with optional TTL. The backend is chosen by the
`CACHE_BACKEND` environment variable, exactly like repositories choose theirs with
`REPO_DATABASE`.

### Backends and selection

| `CACHE_BACKEND` | Backend | Expiry | Notes |
| --- | --- | --- | --- |
| `memory` (default) | `InMemoryTTLCache` | lazy on read | per-process, lost on restart |
| `mongo` | `MongoCache` | native TTL index on `expiresAt` | reuses `MONGO_DSN` / `MONGO_DB` |
| `dynamodb` | `DynamoCache` | native TTL attribute | planned (not yet implemented) |

```python
from persistence_kit.cache import get_cache

cache = get_cache("my-namespace")          # backend from CACHE_BACKEND
await cache.set("k", {"a": 1}, ttl_seconds=3600)
value = await cache.get("k")               # None if missing/expired
await cache.delete("k")
```

The `Cache` Protocol is `get` / `set` / `delete` / `clear`; values must be
JSON-serializable for the persistent backends.

### Sharing one cache across apps

When several applications share a single cache store (for example a common
DynamoDB table across `store-manager` and `siga`), set `CACHE_NAMESPACE` per app.
`get_cache` then wraps the backend in a `NamespacedCache` that prefixes every key
with `"<namespace>:"`, so identical keys from different apps do not collide.

```env
# store-manager .env
CACHE_NAMESPACE=store_manager
# siga .env
CACHE_NAMESPACE=siga
```

With no `CACHE_NAMESPACE` the backend is returned unwrapped (single-app default).

### Invalidation

`Cache.clear(prefix="")` deletes every key that starts with `prefix` (all keys
when empty) and returns how many were removed. Combined with the namespace this
gives targeted invalidation without touching other apps:

```python
cache = get_cache("restclient")   # namespaced by CACHE_NAMESPACE
await cache.clear()               # wipe this app's whole cache
await cache.clear("orders")       # wipe only the "orders" service entries
```

### Transparent response caching in the REST client

Caching is coupled to the REST client as a decorator (`CachingRestClient`) that
wraps the transport when a service opts in. The use case never changes: it keeps
calling `client.request("GET", ...)` and cache hits skip the network entirely.

Enable per service (recommended for reference catalogs that rarely change):

```python
register_rest_service(
    "moises",
    base_url="https://asone.udea.edu.co/wsMoises/servicios",
    config=ServiceConfig(
        default_headers={"OAuth_Token": token, "Tipo_Conexion": "Desarrollo"},
        cache_ttl_seconds=86400,   # 1 day
    ),
)
```

Or control it per call with the `cache_ttl` argument (no headers involved):

```python
await client.request("GET", "/consultaEmpleados", params={"cedula": ced}, cache_ttl=300)  # opt-in, 5 min
await client.request("GET", "/consultarPaises", cache_ttl=0)   # bypass this call
await client.request("GET", "/consultarPaises")                # None -> use the service default
```

`cache_ttl`: `None` uses the service default, `0` skips the cache (bypass), and a
value `> 0` caches with that TTL. Only idempotent methods (`GET`) without a body
are cached. Entries are keyed by `service|method|path|params`, so a parameterized
call (e.g. `?cedula=X`) gets one entry per parameter set.

### Change detection (optional)

Upstreams that do not send `ETag` / `Last-Modified` can still be watched via a
content hash. Within the `stale-while-revalidate` window the cached value is
served immediately and a background revalidation re-fetches, compares the
`sha256`, and on a difference updates the entry, logs the change, and calls
`on_cache_change(key)` if provided. No jobs, non-blocking.

```python
def on_moises_change(key: str) -> None:
    logger.warning("catalog changed: %s", key)   # invalidate, recompute, notify...

register_rest_service(
    "moises",
    base_url=...,
    config=ServiceConfig(
        cache_ttl_seconds=3600,                      # fresh window
        cache_stale_while_revalidate_seconds=82800,  # serve-stale + revalidate window
    ),
    on_cache_change=on_moises_change,                # optional; the kit logs changes anyway
)
```

### What to cache

- **Reference catalogs** (rarely change, not authorization): long TTL (hours/days).
- **Per-user / authorization data** (roles, positions, permissions): short TTL
  (minutes) or no caching, so a revoked permission is not served stale.
