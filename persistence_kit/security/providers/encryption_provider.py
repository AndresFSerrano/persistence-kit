import base64

from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey, RSAPublicKey
from starlette.concurrency import run_in_threadpool

from persistence_kit.security.encrypted.errors import EncryptedPayloadError

_OAEP = padding.OAEP(
    mgf=padding.MGF1(hashes.SHA256()),
    algorithm=hashes.SHA256(),
    label=None,
)
_CLIENT_CODES = frozenset({"InvalidCiphertextException", "IncorrectKeyException"})


class MemoryKeyProvider:

    def __init__(self) -> None:
        self._key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    async def unwrap_key(self, wrapped: bytes) -> bytes:
        try:
            return await run_in_threadpool(self._key.decrypt, wrapped, _OAEP)
        except ValueError as exc:
            raise EncryptedPayloadError("No se pudo abrir la llave del sobre") from exc

    async def public_key(self) -> RSAPublicKey:
        return self._key.public_key()


class LocalKeyProvider:

    def __init__(self, private_key_b64: str) -> None:
        self._private_key_b64 = private_key_b64
        self._key: RSAPrivateKey | None = None

    def _private_key(self) -> RSAPrivateKey:
        if self._key is None:
            try:
                pem = base64.b64decode(self._private_key_b64)
                key = serialization.load_pem_private_key(pem, password=None)
            except ValueError as exc:
                raise RuntimeError("ENCRYPTED_PRIVATE_KEY no es una llave RSA.") from exc
            if not isinstance(key, RSAPrivateKey):
                raise RuntimeError("ENCRYPTED_PRIVATE_KEY no es una llave RSA.")
            self._key = key
        return self._key

    async def unwrap_key(self, wrapped: bytes) -> bytes:
        rsa_key = self._private_key()
        try:
            return await run_in_threadpool(rsa_key.decrypt, wrapped, _OAEP)
        except ValueError as exc:
            raise EncryptedPayloadError("No se pudo abrir la llave del sobre") from exc

    async def public_key(self) -> RSAPublicKey:
        return self._private_key().public_key()


class KmsKeyProvider:

    def __init__(self, key_id: str) -> None:
        try:
            import boto3
            from botocore.exceptions import ClientError
        except ImportError as exc:
            raise RuntimeError(
                "Falta la dependencia boto3 para desenvolver llaves con KMS"
            ) from exc
        self._kms = boto3.client("kms")
        self._key_id = key_id
        self._client_error = ClientError

    async def unwrap_key(self, wrapped: bytes) -> bytes:
        def _decrypt():
            return self._kms.decrypt(
                KeyId=self._key_id,
                CiphertextBlob=wrapped,
                EncryptionAlgorithm="RSAES_OAEP_SHA_256",
            )

        try:
            response = await run_in_threadpool(_decrypt)
        except self._client_error as exc:
            if exc.response["Error"]["Code"] in _CLIENT_CODES:
                raise EncryptedPayloadError("No se pudo abrir la llave del sobre") from exc
            raise RuntimeError("El KMS no pudo desenvolver la llave") from exc
        return response["Plaintext"]

    async def public_key(self) -> RSAPublicKey:
        def _get_public_key():
            return self._kms.get_public_key(KeyId=self._key_id)["PublicKey"]

        try:
            der = await run_in_threadpool(_get_public_key)
        except self._client_error as exc:
            raise RuntimeError("El KMS no pudo entregar la llave pública") from exc
        key = serialization.load_der_public_key(der)
        if not isinstance(key, RSAPublicKey):
            raise RuntimeError("La llave del KMS no es RSA")
        return key