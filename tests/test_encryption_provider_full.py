import os
import sys
import base64
import pytest
from types import SimpleNamespace

from botocore.exceptions import ClientError
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa, ed25519
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from persistence_kit.security.ports import public_key_der_b64
from persistence_kit.security.providers.encryption_provider import (
    LocalKeyProvider,
    MemoryKeyProvider,
)
import persistence_kit.security.providers.encryption_provider as mod

OAEP_PADDING = padding.OAEP(
    mgf=padding.MGF1(hashes.SHA256()),
    algorithm=hashes.SHA256(),
    label=None,
)

class FakeKmsClient:
    def __init__(self) -> None:
        self.decrypt_args = None
        self.get_public_key_der = None

    def decrypt(self, **kwargs) -> dict:
        self.decrypt_args = kwargs
        return {"Plaintext": b"the-key"}

    def get_public_key(self, **kwargs) -> dict:
        self.get_public_key_args = kwargs
        return {"PublicKey": self.get_public_key_der}


@pytest.fixture
def kms_client(monkeypatch):
    client = FakeKmsClient()
    monkeypatch.setitem(
        sys.modules, "boto3", SimpleNamespace(client = lambda service_name: client)
    )
    return client


@pytest.fixture
def boom():
    def make(operation_name: str):
        def raise_client_error(**kwargs):
            raise ClientError(
                {"Error": {"Code": "AccessDeniedException", "Message": "without permissions"}},
                operation_name,
            )

        return raise_client_error

    return make


@pytest.fixture(scope="module")
def rsa_key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture
def provider(rsa_key):
    pem = rsa_key.private_bytes(
        encoding = serialization.Encoding.PEM,
        format = serialization.PrivateFormat.PKCS8,
        encryption_algorithm = serialization.NoEncryption()
    )
    return LocalKeyProvider(base64.b64encode(pem).decode())


@pytest.fixture
def key():
    return AESGCM.generate_key(256)


@pytest.mark.asyncio
async def test_unwrap_key_recovers_the_data_key(rsa_key, provider, key):
    wrapped = rsa_key.public_key().encrypt(key, OAEP_PADDING)

    assert await provider.unwrap_key(wrapped) == key


def test_private_key_is_loaded_once(provider):

    assert provider._key is None
    first = provider._private_key()
    assert provider._private_key() is first


def test_rejects_a_non_rsa_key():
    key = ed25519.Ed25519PrivateKey.generate()
    pem = key.private_bytes(
        encoding = serialization.Encoding.PEM,
        format = serialization.PrivateFormat.PKCS8,
        encryption_algorithm = serialization.NoEncryption()
    )
    provider = LocalKeyProvider(base64.b64encode(pem).decode())
    with pytest.raises(RuntimeError, match = "no es una llave RSA"):
        provider._private_key()


@pytest.mark.asyncio
async def test_public_key_der_b64_returns_the_public_key(rsa_key, provider):
    der_b64 = await public_key_der_b64(provider)

    loaded = serialization.load_der_public_key(base64.b64decode(der_b64))

    assert isinstance(loaded, rsa.RSAPublicKey)
    assert loaded.public_numbers() == rsa_key.public_key().public_numbers()


@pytest.mark.asyncio
async def  test_unwrap_key_fails_with_garbage(provider):
    with pytest.raises(ValueError):
        await provider.unwrap_key(os.urandom(256))


@pytest.mark.asyncio
async def test_memory_provider_unwraps_what_its_public_key_wrapped(key):
    provider = MemoryKeyProvider()
    public_key = await provider.public_key()

    wrapped = public_key.encrypt(key, OAEP_PADDING)

    assert await provider.unwrap_key(wrapped) == key


@pytest.mark.asyncio
async def test_memory_provider_generates_a_key_per_instance(key):
    wrapped = (await MemoryKeyProvider().public_key()).encrypt(key, OAEP_PADDING)

    with pytest.raises(ValueError):
        await MemoryKeyProvider().unwrap_key(wrapped)


def test_requires_boto3(monkeypatch):
    monkeypatch.setitem(sys.modules, "boto3", None)
    with pytest.raises(RuntimeError, match = "Falta la dependencia boto3 para desenvolver llaves con KMS"):
        mod.KmsKeyProvider("key-1")


@pytest.mark.asyncio
async def test_unwrap_key_calls_kms_with_the_right_arguments(kms_client):
    provider = mod.KmsKeyProvider("key-1")
    data_key = await provider.unwrap_key(b"encrypted")

    assert data_key == b"the-key"
    assert kms_client.decrypt_args == {
        "KeyId": "key-1",
        "CiphertextBlob": b"encrypted",
        "EncryptionAlgorithm": "RSAES_OAEP_SHA_256",
    }


@pytest.mark.asyncio
async def test_unwrap_key_translates_client_error(kms_client, boom):
    kms_client.decrypt = boom("Decrypt")
    provider = mod.KmsKeyProvider("key-1")

    with pytest.raises(RuntimeError, match = "El KMS no pudo desenvolver la llave") as err:
        await provider.unwrap_key(b"encrypted")

    assert isinstance(err.value.__cause__, ClientError)


@pytest.mark.asyncio
async def test_public_key_loads_the_der_from_kms(kms_client, rsa_key):
    kms_client.get_public_key_der = rsa_key.public_key().public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    provider = mod.KmsKeyProvider("key-1")

    loaded = await provider.public_key()

    assert loaded.public_numbers() == rsa_key.public_key().public_numbers()
    assert kms_client.get_public_key_args == {"KeyId": "key-1"}


@pytest.mark.asyncio
async def test_public_key_translates_client_error(kms_client, boom):
    kms_client.get_public_key = boom("GetPublicKey")
    provider = mod.KmsKeyProvider("key-1")

    with pytest.raises(RuntimeError, match = "El KMS no pudo entregar la llave pública") as err:
        await provider.public_key()

    assert isinstance(err.value.__cause__, ClientError)


@pytest.mark.asyncio
async def test_public_key_rejects_a_non_rsa_key(kms_client):
    kms_client.get_public_key_der = ed25519.Ed25519PrivateKey.generate().public_key().public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    provider = mod.KmsKeyProvider("key-1")

    with pytest.raises(RuntimeError, match = "La llave del KMS no es RSA"):
        await provider.public_key()