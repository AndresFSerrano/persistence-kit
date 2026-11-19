from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from persistence_kit.storage.contracts import DEFAULT_CONTENT_TYPE
from persistence_kit.storage.errors import (
    StorageConfigError,
    StorageError,
    StorageNotFoundError,
    StoragePresignError,
    StorageUploadError,
)

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:  # pragma: no cover - handled at runtime.
    boto3 = None
    ClientError = Exception

logger = logging.getLogger(__name__)

_MISSING_CODES = frozenset({"NoSuchKey", "404"})


def _is_missing(exc: Exception) -> bool:
    response = getattr(exc, "response", None) or {}
    return response.get("Error", {}).get("Code") in _MISSING_CODES


class S3ObjectStorage:
    def __init__(self, *, bucket: str, region: str, prefix: str = "") -> None:
        if boto3 is None:
            raise StorageConfigError("Falta dependencia boto3 para integrar con S3.")
        self._bucket = bucket
        self._region = region
        normalized_prefix = prefix.strip("/")
        self._prefix = f"{normalized_prefix}/" if normalized_prefix else ""
        self._client = boto3.client("s3", region_name=region)

    def _object_key(self, key: str) -> str:
        return f"{self._prefix}{key}"

    async def upload(
        self,
        key: str,
        data: bytes,
        content_type: str = DEFAULT_CONTENT_TYPE,
    ) -> str:
        object_key = self._object_key(key)

        def _upload() -> None:
            self._client.put_object(
                Bucket=self._bucket,
                Key=object_key,
                Body=data,
                ContentType=content_type,
            )

        logger.info("S3 upload iniciado bucket=%s key=%s", self._bucket, object_key)
        try:
            await asyncio.to_thread(_upload)
        except ClientError as exc:
            logger.exception("S3 upload fallido bucket=%s key=%s", self._bucket, object_key)
            raise StorageUploadError(f"No fue posible subir el archivo a S3: {exc}") from exc

        logger.info("S3 upload exitoso bucket=%s key=%s", self._bucket, object_key)
        return key

    async def upload_file(
        self,
        key: str,
        file_path: Path,
        content_type: str = DEFAULT_CONTENT_TYPE,
    ) -> str:
        object_key = self._object_key(key)

        def _upload_file() -> None:
            with file_path.open("rb") as file_obj:
                self._client.upload_fileobj(
                    Fileobj=file_obj,
                    Bucket=self._bucket,
                    Key=object_key,
                    ExtraArgs={"ContentType": content_type},
                )

        logger.info(
            "S3 file upload iniciado bucket=%s key=%s path=%s", self._bucket, object_key, file_path
        )
        try:
            await asyncio.to_thread(_upload_file)
        except ClientError as exc:
            logger.exception("S3 file upload fallido bucket=%s key=%s", self._bucket, object_key)
            raise StorageUploadError(f"No fue posible subir el archivo a S3: {exc}") from exc

        logger.info("S3 file upload exitoso bucket=%s key=%s", self._bucket, object_key)
        return key

    async def read(self, key: str) -> bytes:
        object_key = self._object_key(key)

        def _read() -> bytes:
            response = self._client.get_object(Bucket=self._bucket, Key=object_key)
            return response["Body"].read()

        try:
            return await asyncio.to_thread(_read)
        except ClientError as exc:
            if _is_missing(exc):
                raise StorageNotFoundError(f"No existe el archivo '{key}'.") from exc
            logger.exception("S3 read fallido bucket=%s key=%s", self._bucket, object_key)
            raise StorageError(f"No fue posible leer el objeto en S3: {exc}") from exc

    async def content_type_of(self, key: str) -> str:
        object_key = self._object_key(key)

        def _head() -> str:
            response = self._client.head_object(Bucket=self._bucket, Key=object_key)
            return response.get("ContentType") or DEFAULT_CONTENT_TYPE

        try:
            return await asyncio.to_thread(_head)
        except ClientError as exc:
            if _is_missing(exc):
                raise StorageNotFoundError(f"No existe el archivo '{key}'.") from exc
            logger.exception("S3 head fallido bucket=%s key=%s", self._bucket, object_key)
            raise StorageError(f"No fue posible consultar el objeto en S3: {exc}") from exc

    async def generate_presigned_url(self, key: str, expires_in: int = 3600) -> str:
        object_key = self._object_key(key)

        def _presign() -> str:
            return self._client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self._bucket, "Key": object_key},
                ExpiresIn=expires_in,
            )

        try:
            return await asyncio.to_thread(_presign)
        except ClientError as exc:
            logger.exception("S3 presign fallido bucket=%s key=%s", self._bucket, object_key)
            raise StoragePresignError(f"No fue posible generar la URL prefirmada: {exc}") from exc

    async def delete(self, key: str) -> None:
        object_key = self._object_key(key)

        def _delete() -> None:
            self._client.delete_object(Bucket=self._bucket, Key=object_key)

        try:
            await asyncio.to_thread(_delete)
        except ClientError as exc:
            logger.exception("S3 delete fallido bucket=%s key=%s", self._bucket, object_key)
            raise StorageError(f"No fue posible borrar el objeto en S3: {exc}") from exc

    async def list_keys(self, prefix: str = "") -> list[str]:
        normalized_prefix = prefix.lstrip("/")
        if normalized_prefix and not normalized_prefix.endswith("/"):
            normalized_prefix = normalized_prefix + "/"
        search_prefix = self._object_key(normalized_prefix)

        def _list() -> list[str]:
            keys: list[str] = []
            paginator = self._client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self._bucket, Prefix=search_prefix):
                for obj in page.get("Contents", []) or []:
                    keys.append(obj["Key"].removeprefix(self._prefix))
            return keys

        try:
            return await asyncio.to_thread(_list)
        except ClientError as exc:
            logger.exception("S3 list fallido bucket=%s prefix=%s", self._bucket, search_prefix)
            raise StorageError(f"No fue posible listar objetos en S3: {exc}") from exc


S3ExportStorageProvider = S3ObjectStorage
