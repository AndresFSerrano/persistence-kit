import base64

from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey, RSAPublicKey
from fastapi.concurrency import run_in_threadpool

from persistence_kit.security.ports import KeyProvider

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    boto3 = None
    ClientError = None

_OAEP = padding.OAEP(
    mgf=padding.MGF1(hashes.SHA256()),
    algorithm=hashes.SHA256(),
    label=None,
)


class LocalKeyProvider(KeyProvider):

    def __init__(self, private_key_b64: str) -> None:
        self._private_key_b64 = private_key_b64
        self._key: RSAPrivateKey | None = None

    def _private_key(self) -> RSAPrivateKey:
        if self._key is None:
            pem = base64.b64decode(self._private_key_b64)
            key = serialization.load_pem_private_key(pem, password=None)
            if not isinstance(key, RSAPrivateKey):
                raise RuntimeError("SEALED_PRIVATE_KEY no es una llave RSA.")
            self._key = key
        return self._key

    async def unwrap_key(self, wrapped: bytes) -> bytes:
        return self._private_key().decrypt(wrapped, _OAEP)

    async def _public_key_obj(self) -> RSAPublicKey:
        return self._private_key().public_key()


class KmsKeyProvider(KeyProvider):

    def __init__(self, key_id: str) -> None:
        if boto3 is None:
            raise RuntimeError("Falta la dependencia boto3 para desenvolver llaves con KMS")
        self._kms = boto3.client("kms")
        self._key_id = key_id

    async def unwrap_key(self, wrapped: bytes) -> bytes:
        def _decrypt():
            return self._kms.decrypt(
                KeyId=self._key_id,
                CiphertextBlob=wrapped,
                EncryptionAlgorithm="RSAES_OAEP_SHA_256",
            )

        try:
            response = await run_in_threadpool(_decrypt)
        except ClientError as exc:
            raise RuntimeError("El KMS no pudo desenvolver la llave") from exc
        return response["Plaintext"]

    async def _public_key_obj(self) -> RSAPublicKey:
        def _get_public_key():
            return self._kms.get_public_key(KeyId=self._key_id)["PublicKey"]

        try:
            der = await run_in_threadpool(_get_public_key)
        except ClientError as exc:
            raise RuntimeError("El KMS no pudo entregar la llave pública") from exc
        key = serialization.load_der_public_key(der)
        if not isinstance(key, RSAPublicKey):
            raise RuntimeError("La llave del KMS no es RSA")
        return key