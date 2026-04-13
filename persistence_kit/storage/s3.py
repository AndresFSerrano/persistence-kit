from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from persistence_kit.storage.errors import (
    StorageConfigError,
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


class S3ObjectStorage:
    def __init__(self, *, bucket: str, region: str) -> None:
        if boto3 is None:
            raise StorageConfigError("Falta dependencia boto3 para integrar con S3.")
        self._bucket = bucket
        self._region = region
        self._client = boto3.client("s3", region_name=region)

    async def upload(
        self,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> str:
        def _upload() -> None:
            self._client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=data,
                ContentType=content_type,
            )

        logger.info("S3 upload iniciado bucket=%s key=%s", self._bucket, key)
        try:
            await asyncio.to_thread(_upload)
        except ClientError as exc:
            logger.exception("S3 upload fallido bucket=%s key=%s", self._bucket, key)
            raise StorageUploadError(f"No fue posible subir el archivo a S3: {exc}") from exc

        logger.info("S3 upload exitoso bucket=%s key=%s", self._bucket, key)
        return key

    async def upload_file(
        self,
        key: str,
        file_path: Path,
        content_type: str = "application/octet-stream",
    ) -> str:
        def _upload_file() -> None:
            with file_path.open("rb") as file_obj:
                self._client.upload_fileobj(
                    Fileobj=file_obj,
                    Bucket=self._bucket,
                    Key=key,
                    ExtraArgs={"ContentType": content_type},
                )

        logger.info("S3 file upload iniciado bucket=%s key=%s path=%s", self._bucket, key, file_path)
        try:
            await asyncio.to_thread(_upload_file)
        except ClientError as exc:
            logger.exception("S3 file upload fallido bucket=%s key=%s", self._bucket, key)
            raise StorageUploadError(f"No fue posible subir el archivo a S3: {exc}") from exc

        logger.info("S3 file upload exitoso bucket=%s key=%s", self._bucket, key)
        return key

    async def generate_presigned_url(self, key: str, expires_in: int = 3600) -> str:
        def _presign() -> str:
            return self._client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self._bucket, "Key": key},
                ExpiresIn=expires_in,
            )

        try:
            return await asyncio.to_thread(_presign)
        except ClientError as exc:
            logger.exception("S3 presign fallido bucket=%s key=%s", self._bucket, key)
            raise StoragePresignError(f"No fue posible generar la URL prefirmada: {exc}") from exc


S3ExportStorageProvider = S3ObjectStorage
