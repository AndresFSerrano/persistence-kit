import base64
import json

import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient

from persistence_kit.api.exceptions import SealedPayloadError
from persistence_kit.api.sealed_routes import (
    KEY_HEADER,
    SEALED_FIELDS,
    SEALED_FLAG,
    SEALED_RESPONSE_FIELDS,
    _locate_all,
    _open_fields,
    _seal_fields,
    build_sealed_route,
    sealed,
)
from persistence_kit.security.sealed.envelope import HYBRID_VERSION, open_sealed, seal
from persistence_kit.settings import DeploymentStage, PersistenceKitSettings


class FakeKeyProvider:
    def __init__(self, data_key: bytes) -> None:
        self.data_key = data_key
        self.wrapped = None

    async def unwrap_key(self, wrapped: bytes) -> bytes:
        self.wrapped = wrapped
        return self.data_key


@pytest.fixture
def data_key():
    return AESGCM.generate_key(256)


@pytest.fixture
def settings():
    def make(stage: DeploymentStage = DeploymentStage.PRODUCTION):
        return PersistenceKitSettings(stage=stage)
    return make


@pytest.fixture
def client(monkeypatch, data_key, settings):
    import persistence_kit.security.factory as factory_mod

    monkeypatch.setattr(factory_mod, "key_provider", lambda _settings: FakeKeyProvider(data_key))
    router = APIRouter(route_class = build_sealed_route(settings))

    @router.post("/echo", openapi_extra=sealed())
    async def echo(payload: dict):
        return payload

    @router.post("/login", openapi_extra=sealed(fields=["password"], response_fields=["token"]))
    async def login(payload: dict):
        return {"user": payload["user"], "token": payload["password"]}

    @router.post("/plain")
    async def plain(payload: dict):
        return payload

    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


@pytest.fixture
def hybrid_envelope(data_key):
    def make(payload: dict) -> dict:
        return {
            **seal(json.dumps(payload).encode(), data_key),
            "v": HYBRID_VERSION,
            "key": base64.b64encode(b"wrapped").decode()
        }
    return make


def test_sealed_replaces_the_body_schema():
    marca = sealed()

    assert marca[SEALED_FLAG] is True
    assert set(marca) == {SEALED_FLAG, "requestBody"}

    schema = marca["requestBody"]["content"]["application/json"]["schema"]
    assert schema["required"] == ["v", "key", "nonce", "ciphertext", "ts"]


def test_sealed_with_fields_keeps_the_original_body():
    marca = sealed(fields = ["password"], response_fields = ["token"])

    assert set(marca) == {SEALED_FIELDS, SEALED_FLAG, SEALED_RESPONSE_FIELDS}
    assert marca[SEALED_FIELDS] == ["password"]
    assert marca[SEALED_RESPONSE_FIELDS] == ["token"]


def test_sealed_without_body_only_marks_the_route():
    marca = sealed(with_body = False)

    assert marca == {SEALED_FLAG: True}


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
    payload = {"user": "ana", "password": seal(json.dumps("demo1234").encode(), data_key)}

    opened = _open_fields(payload, ["password"], data_key, settings())

    assert opened == {"user": "ana", "password": "demo1234"}
    assert opened is payload


def test_open_fields_rejects_a_declared_field_that_is_missing(data_key, settings):
    payload = {"user": "ana"}

    with pytest.raises(SealedPayloadError, match="esta declarado pero no figura en el sobre"):
        _open_fields(payload, ["password"], data_key, settings())


def test_open_fields_rejects_a_plain_field_outside_local(data_key, settings):
    payload = {"password": "demo1234"}

    with pytest.raises(SealedPayloadError, match="debe estar cifrado"):
        _open_fields(payload, ["password"], data_key, settings())


def test_open_fields_lets_a_plain_field_through_in_local(data_key, settings):
    payload = {"password": "demo1234"}

    opened = _open_fields(payload, ["password"], data_key, settings(DeploymentStage.LOCAL))

    assert opened == {"password": "demo1234"}


def test_seal_fields_seals_each_object_of_a_list(data_key):
    payload = [{"user": "ana", "token": "abc"}, {"user": "juan", "token": "xyz"}]

    sealed_payload = _seal_fields(payload, ["token"], data_key)

    assert [item["user"] for item in sealed_payload] == ["ana", "juan"]
    assert [json.loads(open_sealed(item["token"], data_key)) for item in sealed_payload] == [
        "abc",
        "xyz",
    ]


def test_seal_fields_rejects_a_response_that_is_not_an_object(data_key):
    with pytest.raises(SealedPayloadError, match="debe ser un objeto o una lista de objetos"):
        _seal_fields("no soy un objeto", ["token"], data_key)


def test_sealed_route_opens_the_request_body(client, hybrid_envelope, data_key):
    response = client.post("/echo", json=hybrid_envelope({"user": "ana"}))

    assert response.status_code == 200
    assert json.loads(open_sealed(response.json(), data_key)) == {"user": "ana"}


def test_sealed_route_seals_the_response(client, hybrid_envelope):
    response = client.post("/echo", json=hybrid_envelope({"user": "ana"}))

    assert set(response.json()) == {"v", "nonce", "ciphertext", "ts"}
    assert b"ana" not in response.content


def test_sealed_route_rejects_a_replayed_envelope(client, hybrid_envelope):
    envelope = hybrid_envelope({"user": "ana"})

    assert client.post("/echo", json=envelope).status_code == 200

    replayed = client.post("/echo", json=envelope)

    assert replayed.status_code == 400
    assert "ya fue usado" in replayed.json()["detail"]


def test_sealed_route_rejects_a_body_that_is_not_json(client):
    response = client.post("/echo", content=b"no soy json")

    assert response.status_code == 400
    assert "sobre cifrado en JSON" in response.json()["detail"]


def test_sealed_route_opens_the_declared_fields(client, data_key):
    payload = {"user": "ana", "password": seal(json.dumps("demo1234").encode(), data_key)}

    response = client.post(
        "/login",
        json=payload,
        headers={KEY_HEADER: base64.b64encode(b"wrapped").decode()},
    )

    assert response.status_code == 200
    assert response.json()["user"] == "ana"
    assert json.loads(open_sealed(response.json()["token"], data_key)) == "demo1234"


def test_a_route_without_the_mark_is_left_alone(client):
    response = client.post("/plain", json={"user": "ana"})

    assert response.status_code == 200
    assert response.json() == {"user": "ana"}