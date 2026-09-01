# Encrypted payloads

The `encrypted` capability encrypts request and response bodies end to end, between
the browser and your route handler. TLS protects the wire; this protects the
payload from everything that terminates TLS on the way — load balancers, API
gateways, proxies, and the access logs of all three.

The kit provides the envelope format, the providers that unwrap the key, and an
`APIRoute` subclass that applies both. Which fields deserve encryption is your
application's decision, not the kit's.

## The envelope

Two versions travel, and they differ in one field.

**Version 2, the hybrid envelope.** What the client sends as a request body:

```json
{
  "v": 2,
  "key": "base64 of the AES key wrapped with the server's RSA public key",
  "nonce": "base64, 12 bytes",
  "ciphertext": "base64 of the body encrypted with AES-256-GCM",
  "ts": 1787000123
}
```

**Version 1, the symmetric envelope.** The same without `key`, used once both
sides already share the AES key: responses, and individual encrypted fields.

Why hybrid at all: RSA cannot encrypt a body. With a 2048-bit key and OAEP-SHA256
the limit is 190 bytes, and it is slow. AES has no such limit and is fast, but
both sides need the same key, and a browser has no way to agree on one in
advance. So RSA wraps the 32-byte AES key, and AES carries the payload.

`ts` is the number of seconds since 1970 at the moment of sealing. It is also
passed to AES-GCM as additional authenticated data, so changing it invalidates
the envelope.

## Wiring a route

```python
from fastapi import APIRouter, FastAPI

from persistence_kit import (
    EncryptedResponseMiddleware,
    build_encrypted_route,
    encrypted,
)

router = APIRouter(route_class=build_encrypted_route(get_settings))

app = FastAPI()
app.add_middleware(EncryptedResponseMiddleware)
```

`build_encrypted_route(settings_dep)` returns an `APIRoute` subclass. `settings_dep`
is called with no arguments and must return your `PersistenceKitSettings`.

Both pieces are required. The route opens the request and leaves the AES key on
`request.state`; the middleware encrypts whatever comes back. The middleware sits
outside the `ExceptionMiddleware`, so error responses built by your own
`exception_handler` are encrypted too, whatever the exception was. An encrypted
route whose app has no such middleware raises on the first request rather than
answering in the clear.

Routes inside that router are untouched unless they carry the mark:

```python
@router.post("/orders", openapi_extra=encrypted())
async def create_order(payload: OrderRequest) -> OrderResponse:
    ...
```

`encrypted(with_body=True, fields=None, response_fields=None)` returns the dict the
route class reads later.

- `encrypted()` — the whole body arrives as an envelope and the whole response goes
  back as one. It also replaces the request schema in Swagger, so the documented
  body is the envelope rather than your model.
- `encrypted(fields=["password"])` — the body is ordinary JSON and only those fields
  are envelopes. The documented schema stays your model.
- `encrypted(response_fields=["token"])` — the same on the way out.
- `encrypted(with_body=False)` — for a `GET`: nothing to open, but the response is
  still encrypted with the key from the header.

Field paths may be nested and may cross lists:

```python
encrypted(fields=["client.email", "items[].price"])
```

`fields` says how a field must travel, not whether it has to be there: a declared
field that never arrives is skipped, so partial updates keep working. Which fields
are required is your handler's model to decide, and it answers with a `422` naming
them. Outside the `local` stage, a declared field that arrives unencrypted is an
error.

## Where the AES key comes from

**Whole-body mode.** Inside the envelope, in `key`.

**Field mode.** In the `x-encrypted-key` header, wrapped the same way, because the
body itself is not an envelope. Add `declare_key_header` as a dependency if you
want Swagger to show the field.

Either way the server unwraps it with a `KeyProvider`, and the same key is used
to encrypt the response — the client can open the answer with the key it already
generated.

## Key providers

`KeyProvider` is a protocol with `unwrap_key(wrapped) -> bytes` and
`public_key() -> RSAPublicKey`. `get_key_provider(settings)` picks one and caches it:

| `ENCRYPTED_TYPE` | Provider | Where the private key lives |
| --- | --- | --- |
| `memory` (default) | `MemoryKeyProvider` | Generated at startup, gone on restart |
| `local` | `LocalKeyProvider` | In `ENCRYPTED_PRIVATE_KEY`, as a base64 PEM |
| `production` | `KmsKeyProvider` | Inside AWS KMS, named by `KMS_KEY_ID`; the service never sees it |

`ENCRYPTED_TYPE` alone decides, like `AUTH_PROVIDER` and `CACHE_BACKEND` do for
their own modules; the other two settings only carry the data. Asking for
`production` without a `KMS_KEY_ID`, or for `local` without an
`ENCRYPTED_PRIVATE_KEY`, fails with a 500 that names the missing setting.

`memory` generates an RSA-2048 pair when the process starts, so the kit runs with
no configuration at all — the twin of `AUTH_PROVIDER=memory` and
`CACHE_BACKEND=memory`. Its public key changes on every restart, so clients that
cached it can no longer encrypt: use it for local work and for the tests of
whoever consumes the kit, never for a deployment.

The KMS provider needs `[storage-s3]` or any other extra that brings `boto3`.

Generating a local pair:

```bash
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out encrypted.pem
base64 -w0 encrypted.pem   # this is ENCRYPTED_PRIVATE_KEY
```

## What the client has to do

1. Fetch the public key once. Expose it from your own route using
   `await public_key_der_b64(get_key_provider(settings))`, which returns base64 of
   the DER `SubjectPublicKeyInfo` — what `crypto.subtle.importKey` expects under
   `spki`.
2. Generate a fresh AES-256-GCM key **per request**. Never reuse one, and never
   reuse a nonce with the same key: with AES-GCM that leaks the plaintext.
3. Encrypt the serialized body with a random 12-byte nonce, passing the timestamp
   as additional data.
4. Wrap the AES key with RSA-OAEP-SHA256.
5. Send the envelope, or the plain body plus the `x-encrypted-key` header in field
   mode.
6. Open the response with the same AES key.

The server rejects envelopes older than sixty seconds, and remembers each nonce
for that long, so a captured request cannot be replayed.

Those nonces live in the cache, so outside the local stage an encrypted route needs a
`CACHE_BACKEND` shared by every worker. With `memory` each process keeps its own
list and the same envelope passes once per worker, so mounting an encrypted route
under that setting raises at startup.

## Errors

Everything the client can get wrong comes back as `400`, never `500`:

| Cause | Detail |
| --- | --- |
| Body is not JSON | `El cuerpo debe ser un sobre cifrado en JSON.` |
| Missing or unreadable field | `Sobre mal formado.` |
| Version the function does not handle | `Versión de sobre no soportada` |
| Older than sixty seconds | `El sobre esta vencido` |
| Nonce already seen | `El sobre ya fue usado` |
| Wrong key or altered ciphertext | `El contenido fue alterado o la llave no corresponde.` |
| Whole envelope sent to a field-mode route | `Esta ruta cifra campos sueltos, no el cuerpo entero.` |
| Declared field in plaintext, outside `local` | `El campo x debe estar cifrado` |
| Key header missing, outside `local` | `Falta la cabecera x-encrypted-key.` |
| Key header that the provider cannot unwrap | `No se pudo abrir la llave de la cabecera.` |

Internally these are `EncryptedPayloadError`; the route translates them at the HTTP
edge. Errors raised by your own handler are encrypted like any other response, so a
`404` reaches the client encrypted when the request was.

## Local development

In `local` stage the route accepts a plain body and plain fields, so you can call
your API from Swagger without a client that encrypts. Anywhere else, the same
request is rejected.
