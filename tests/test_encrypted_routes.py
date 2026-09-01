import base64
import json

import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from fastapi import APIRouter, FastAPI, Response
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.testclient import TestClient
from pydantic import BaseModel

from persistence_kit.api.encrypted_middleware import EncryptedResponseMiddleware
from persistence_kit.cache import InMemoryTTLCache
from persistence_kit.security.encrypted.errors import EncryptedPayloadError
from persistence_kit.api.encrypted_routes import (
    KEY_HEADER,
    ENCRYPTED_FIELDS,
    ENCRYPTED_FLAG,
    ENCRYPTED_RESPONSE_FIELDS,
    _locate_all,
    _open_fields,
    _encrypt_fields,
    build_encrypted_route,
    encrypted,
)
from persistence_kit.security.encrypted.envelope import HYBRID_VERSION, decrypt, encrypt
from persistence_kit.settings import DeploymentStage, PersistenceKitSettings


class DomainError(Exception):
    pass


class LoginRequest(BaseModel):
    user: str
    password: str


class FakeKeyProvider:
    def __init__(self, data_key: bytes) -> None:
        self.data_key = data_key
        self.wrapped = None

    async def unwrap_key(self, wrapped: bytes) -> bytes:
        self.wrapped = wrapped
        return self.data_key


class BrokenKeyProvider:
    def __init__(self, error: Exception) -> None:
        self.error = error

    async def unwrap_key(self, wrapped: bytes) -> bytes:
        raise self.error


@pytest.fixture
def data_key():
    return AESGCM.generate_key(256)


@pytest.fixture
def settings():
    def make(stage: DeploymentStage = DeploymentStage.PRODUCTION):
        return PersistenceKitSettings(stage=stage)
    return make


@pytest.fixture
def build_client(monkeypatch, data_key, settings):
    import persistence_kit.api.encrypted_routes as routes_mod
    import persistence_kit.security.factory as factory_mod

    monkeypatch.setattr(factory_mod, "get_key_provider", lambda _settings: FakeKeyProvider(data_key))
    monkeypatch.setenv("CACHE_BACKEND", "mongo")
    shared_cache = InMemoryTTLCache()
    monkeypatch.setattr(routes_mod, "get_cache", lambda _name: shared_cache)

    def make(stage: DeploymentStage = DeploymentStage.PRODUCTION):
        router = APIRouter(route_class = build_encrypted_route(lambda: settings(stage)))

        @router.post("/echo", openapi_extra=encrypted())
        async def echo(payload: dict):
            return payload

        @router.post("/login", openapi_extra=encrypted(fields=["password"], response_fields=["token"]))
        async def login(payload: dict):
            return {"user": payload["user"], "token": payload["password"]}

        @router.post("/typed", openapi_extra=encrypted())
        async def typed(payload: LoginRequest):
            return {"user": payload.user}

        @router.post("/plain")
        async def plain(payload: dict):
            return payload

        @router.post("/headers", openapi_extra=encrypted())
        async def with_headers(payload: dict, response: Response):
            response.set_cookie("refresh_token", "abc")
            response.headers["x-request-id"] = "42"
            return payload

        @router.post("/boom", openapi_extra=encrypted())
        async def boom(payload: dict):
            raise DomainError("saldo insuficiente")

        @router.post("/stream", openapi_extra=encrypted())
        async def stream(payload: dict):
            async def filas():
                yield b"una fila\n"

            return StreamingResponse(filas(), media_type="text/plain")

        app = FastAPI()

        @app.exception_handler(DomainError)
        async def handle_domain(request, exc):
            return JSONResponse({"error": str(exc)}, status_code=409)

        app.add_middleware(EncryptedResponseMiddleware)
        app.include_router(router)
        return TestClient(app)

    return make


@pytest.fixture
def hybrid_envelope(data_key):
    def make(payload: dict) -> dict:
        return {
            **encrypt(json.dumps(payload).encode(), data_key),
            "v": HYBRID_VERSION,
            "key": base64.b64encode(b"wrapped").decode()
        }
    return make


def test_a_encrypted_route_refuses_to_mount_with_a_memory_cache(monkeypatch, settings):
    monkeypatch.setenv("CACHE_BACKEND", "memory")
    router = APIRouter(route_class = build_encrypted_route(lambda: settings()))

    with pytest.raises(RuntimeError, match="CACHE_BACKEND compartido"):

        @router.post("/echo", openapi_extra=encrypted())
        async def echo(payload: dict):
            return payload


def test_a_encrypted_route_mounts_with_a_memory_cache_in_local(monkeypatch, settings):
    monkeypatch.setenv("CACHE_BACKEND", "memory")
    router = APIRouter(route_class = build_encrypted_route(lambda: settings(DeploymentStage.LOCAL)))

    @router.post("/echo", openapi_extra=encrypted())
    async def echo(payload: dict):
        return payload

    assert len(router.routes) == 1


def test_a_encrypted_route_needs_the_middleware(build_client, hybrid_envelope, settings):
    router = APIRouter(route_class = build_encrypted_route(lambda: settings()))

    @router.post("/echo", openapi_extra=encrypted())
    async def echo(payload: dict):
        return payload

    app = FastAPI()
    app.include_router(router)

    with pytest.raises(RuntimeError, match="EncryptedResponseMiddleware"):
        TestClient(app).post("/echo", json=hybrid_envelope({"user": "ana"}))


def test_encrypted_replaces_the_body_schema():
    marca = encrypted()

    assert marca[ENCRYPTED_FLAG] is True
    assert set(marca) == {ENCRYPTED_FLAG, "requestBody"}

    schema = marca["requestBody"]["content"]["application/json"]["schema"]
    assert schema["required"] == ["v", "key", "nonce", "ciphertext", "ts"]


def test_encrypted_with_fields_keeps_the_original_body():
    marca = encrypted(fields = ["password"], response_fields = ["token"])

    assert set(marca) == {ENCRYPTED_FIELDS, ENCRYPTED_FLAG, ENCRYPTED_RESPONSE_FIELDS}
    assert marca[ENCRYPTED_FIELDS] == ["password"]
    assert marca[ENCRYPTED_RESPONSE_FIELDS] == ["token"]


def test_encrypted_without_body_only_marks_the_route():
    marca = encrypted(with_body = False)

    assert marca == {ENCRYPTED_FLAG: True}


@pytest.mark.parametrize(
    "payload, path",
    [
        ({}, "cliente.correo"),
        ({"cliente": "ana"}, "cliente.correo"),
        ({"items": "no soy lista"}, "items[].precio"),
    ],
    ids=["sin_hijo", "hijo_no_es_dict", "no_es_lista"],
)
def test_locate_all_returns_empty_when_the_path_leads_nowhere(payload, path):
    assert _locate_all(payload, path) == []


def test_locate_all_finds_a_top_level_field():
    payload = {"password": "x"}

    assert _locate_all(payload, "password") == [(payload, "password")]


def test_locate_all_returns_the_pair_even_when_the_field_is_missing():
    payload = {}

    assert _locate_all(payload, "password") == [(payload, "password")]


def test_locate_all_walks_into_nested_objects():
    payload = {"cliente": {"correo": "ana@x.com"}}

    pairs = _locate_all(payload, "cliente.correo")

    assert len(pairs) == 1
    container, key = pairs[0]
    assert container is payload["cliente"]
    assert key == "correo"

    container[key] = "cifrado"
    assert payload["cliente"]["correo"] == "cifrado"


def test_locate_all_walks_lists():
    payload = {"items": [{"precio": 10}, {"precio": 20}]}

    pairs = _locate_all(payload, "items[].precio")

    assert len(pairs) == 2
    assert pairs[0][0] is payload["items"][0]
    assert pairs[1][0] is payload["items"][1]
    assert [key for _, key in pairs] == ["precio", "precio"]


def test_locate_all_skips_list_items_that_are_not_objects():
    payload = {"items": [{"precio": 10}, "basura"]}

    pairs = _locate_all(payload, "items[].precio")

    assert len(pairs) == 1
    assert pairs[0][0] is payload["items"][0]


def test_open_fields_replaces_the_envelope_with_its_content(data_key, settings):
    envelope = encrypt(json.dumps("demo1234").encode(), data_key)
    payload = {"user": "ana", "password": envelope}

    opened, nonces = _open_fields(payload, ["password"], data_key, settings())

    assert opened == {"user": "ana", "password": "demo1234"}
    assert opened is payload
    assert nonces == [envelope["nonce"]]


def test_open_fields_lets_a_declared_field_that_is_missing_through(data_key, settings):
    payload = {"user": "ana"}

    opened, nonces = _open_fields(payload, ["password"], data_key, settings())

    assert opened == {"user": "ana"}
    assert nonces == []


def test_open_fields_lets_a_missing_nested_field_through(data_key, settings):
    payload = {"user": "ana"}

    opened, nonces = _open_fields(payload, ["cliente.correo"], data_key, settings())

    assert opened == {"user": "ana"}
    assert nonces == []


def test_open_fields_rejects_a_plain_field_outside_local(data_key, settings):
    payload = {"password": "demo1234"}

    with pytest.raises(EncryptedPayloadError, match="debe estar cifrado"):
        _open_fields(payload, ["password"], data_key, settings())


def test_open_fields_lets_a_plain_field_through_in_local(data_key, settings):
    payload = {"password": "demo1234"}

    opened, nonces = _open_fields(payload, ["password"], data_key, settings(DeploymentStage.LOCAL))

    assert opened == {"password": "demo1234"}
    assert nonces == []


def test_seal_fields_seals_each_object_of_a_list(data_key):
    payload = [{"user": "ana", "token": "abc"}, {"user": "juan", "token": "xyz"}]

    encrypted_payload = _encrypt_fields(payload, ["token"], data_key)

    assert [item["user"] for item in encrypted_payload] == ["ana", "juan"]
    assert [json.loads(decrypt(item["token"], data_key)) for item in encrypted_payload] == [
        "abc",
        "xyz",
    ]


def test_seal_fields_rejects_a_response_that_is_not_an_object(data_key):
    with pytest.raises(EncryptedPayloadError, match="debe ser un objeto o una lista de objetos"):
        _encrypt_fields("no soy un objeto", ["token"], data_key)


def test_encrypted_route_opens_the_request_body(build_client, hybrid_envelope, data_key):
    client = build_client()
    response = client.post("/echo", json=hybrid_envelope({"user": "ana"}))

    assert response.status_code == 200
    assert json.loads(decrypt(response.json(), data_key)) == {"user": "ana"}


def test_encrypted_route_seals_the_response(build_client, hybrid_envelope):
    client = build_client()
    response = client.post("/echo", json=hybrid_envelope({"user": "ana"}))

    assert set(response.json()) == {"v", "nonce", "ciphertext", "ts"}
    assert b"ana" not in response.content


def test_encrypted_route_rejects_a_replayed_envelope(build_client, hybrid_envelope):
    client = build_client()
    envelope = hybrid_envelope({"user": "ana"})

    assert client.post("/echo", json=envelope).status_code == 200

    replayed = client.post("/echo", json=envelope)

    assert replayed.status_code == 400
    assert "ya fue usado" in replayed.json()["detail"]


def test_a_fields_route_rejects_a_replayed_field(build_client, data_key):
    client = build_client()
    payload = {"user": "ana", "password": encrypt(json.dumps("demo1234").encode(), data_key)}
    headers = {KEY_HEADER: base64.b64encode(b"wrapped").decode()}

    assert client.post("/login", json=payload, headers=headers).status_code == 200

    replayed = client.post("/login", json=payload, headers=headers)

    assert replayed.status_code == 400
    assert "ya fue usado" in replayed.json()["detail"]


def test_encrypted_route_rejects_a_body_that_is_not_json(build_client):
    client = build_client()
    response = client.post("/echo", content=b"no soy json")

    assert response.status_code == 400
    assert "sobre cifrado en JSON" in response.json()["detail"]


def test_encrypted_route_opens_the_declared_fields(build_client, data_key):
    client = build_client()
    payload = {"user": "ana", "password": encrypt(json.dumps("demo1234").encode(), data_key)}

    response = client.post(
        "/login",
        json=payload,
        headers={KEY_HEADER: base64.b64encode(b"wrapped").decode()},
    )

    assert response.status_code == 200
    assert response.json()["user"] == "ana"
    assert json.loads(decrypt(response.json()["token"], data_key)) == "demo1234"


def test_a_whole_envelope_on_a_fields_route_is_rejected(build_client, hybrid_envelope, data_key):
    client = build_client()
    envelope = hybrid_envelope({"user": "ana", "password": "demo1234"})

    response = client.post(
        "/login",
        json=envelope,
        headers={KEY_HEADER: base64.b64encode(b"wrapped").decode()},
    )

    assert response.status_code == 400
    assert "no el cuerpo entero" in response.json()["detail"]


def test_a_encrypted_field_inside_a_encrypted_body_is_not_opened(build_client, hybrid_envelope, data_key):
    client = build_client()
    inner = encrypt(json.dumps("demo1234").encode(), data_key)

    response = client.post("/typed", json=hybrid_envelope({"user": "ana", "password": inner}))

    assert response.status_code == 422

    detail = json.loads(decrypt(response.json(), data_key))["detail"]
    assert detail[0]["loc"] == ["body", "password"]
    assert set(detail[0]["input"]) == {"v", "nonce", "ciphertext", "ts"}


@pytest.mark.parametrize(
    "error",
    [ValueError("Decryption failed"), RuntimeError("El KMS no pudo desenvolver la llave")],
    ids=["local", "kms"],
)
def test_a_key_the_provider_cannot_unwrap_is_a_400(build_client, monkeypatch, error):
    import persistence_kit.security.factory as factory_mod

    client = build_client()
    monkeypatch.setattr(factory_mod, "get_key_provider", lambda _settings: BrokenKeyProvider(error))

    response = client.post(
        "/login",
        json={"password": "demo1234"},
        headers={KEY_HEADER: base64.b64encode(b"basura").decode()},
    )

    assert response.status_code == 400
    assert "No se pudo abrir la llave de la cabecera" in response.json()["detail"]


def test_encrypted_route_keeps_the_headers_and_cookies_of_the_handler(
    build_client, hybrid_envelope, data_key
):
    client = build_client()

    response = client.post("/headers", json=hybrid_envelope({"user": "ana"}))

    assert response.status_code == 200
    assert response.headers["x-request-id"] == "42"
    assert response.cookies["refresh_token"] == "abc"
    assert response.headers["content-length"] == str(len(response.content))
    assert json.loads(decrypt(response.json(), data_key)) == {"user": "ana"}


def test_an_app_exception_handler_runs_and_its_response_is_encrypted(
    build_client, hybrid_envelope, data_key
):
    client = build_client()

    response = client.post("/boom", json=hybrid_envelope({"user": "ana"}))

    assert response.status_code == 409
    assert b"saldo" not in response.content
    assert json.loads(decrypt(response.json(), data_key)) == {"error": "saldo insuficiente"}


def test_a_streaming_response_on_a_encrypted_route_fails_loudly(build_client, hybrid_envelope):
    client = build_client()

    with pytest.raises(RuntimeError, match="no puede devolver StreamingResponse"):
        client.post("/stream", json=hybrid_envelope({"user": "ana"}))


def test_a_plain_body_with_a_v_field_passes_in_local(build_client):
    local_client = build_client(DeploymentStage.LOCAL)

    response = local_client.post("/echo", json={"v": "1.2.0", "nombre": "app"})

    assert response.status_code == 200
    assert response.json() == {"v": "1.2.0", "nombre": "app"}


def test_a_plain_body_with_a_v_field_is_rejected_outside_local(build_client):
    client = build_client()
    response = client.post("/echo", json={"v": "1.2.0", "nombre": "app"})

    assert response.status_code == 400
    assert "debe venir en un sobre cifrado" in response.json()["detail"]


def test_a_route_without_the_mark_is_left_alone(build_client):
    client = build_client()
    response = client.post("/plain", json={"user": "ana"})

    assert response.status_code == 200
    assert response.json() == {"user": "ana"}