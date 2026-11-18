from __future__ import annotations

from pathlib import Path
from typing import Iterable

from persistence_kit.storage.contracts import ObjectStorage, UploadedFile

SignaturePart = tuple[int, bytes]
Signature = tuple[SignaturePart, ...]

CONTENT_SIGNATURES: dict[str, tuple[Signature, ...]] = {
    "application/pdf": (((0, b"%PDF-"),),),
    "image/png": (((0, b"\x89PNG\r\n\x1a\n"),),),
    "image/jpeg": (((0, b"\xff\xd8\xff"),),),
    "image/gif": (((0, b"GIF87a"),), ((0, b"GIF89a"),)),
    "image/webp": (((0, b"RIFF"), (8, b"WEBP")),),
}


def _matches_signature(data: bytes, signature: Signature) -> bool:
    return all(data.startswith(magic, offset) for offset, magic in signature)


def _has_expected_signature(data: bytes, content_type: str) -> bool:
    signatures = CONTENT_SIGNATURES.get(content_type)
    if signatures is None:
        return True
    return any(_matches_signature(data, signature) for signature in signatures)


class MediaStorage:
    CONTENT_TYPE_TO_EXTENSION: dict[str, str] = {
        "image/jpeg": "jpg",
        "image/png": "png",
        "image/webp": "webp",
        "image/gif": "gif",
        "application/pdf": "pdf",
    }
    DEFAULT_ALLOWED_CONTENT_TYPES: tuple[str, ...] = (
        "image/jpeg",
        "image/png",
        "image/webp",
    )
    DEFAULT_MAX_BYTES: int = 5 * 1024 * 1024

    def __init__(
        self,
        *,
        storage: ObjectStorage,
        public_base_url: str | None,
        url_prefix: str,
        allowed_content_types: Iterable[str] | None = None,
        max_bytes: int | None = None,
        verify_signature: bool = False,
    ) -> None:
        self._storage = storage
        self._public_base_url = (public_base_url or "").rstrip("/")
        self._url_prefix = "/" + url_prefix.strip("/")
        self._allowed_content_types = tuple(
            allowed_content_types
            if allowed_content_types is not None
            else self.DEFAULT_ALLOWED_CONTENT_TYPES
        )
        self._max_bytes = max_bytes if max_bytes is not None else self.DEFAULT_MAX_BYTES
        self._verify_signature = verify_signature

    @property
    def url_prefix(self) -> str:
        return self._url_prefix

    @property
    def object_storage(self) -> ObjectStorage:
        return self._storage

    @property
    def allowed_content_types(self) -> tuple[str, ...]:
        return self._allowed_content_types

    @property
    def max_bytes(self) -> int:
        return self._max_bytes

    @property
    def verify_signature(self) -> bool:
        return self._verify_signature

    def validate_upload(self, data: bytes, content_type: str) -> None:
        from persistence_kit.api.exceptions import ValidationException

        if not data:
            raise ValidationException("El archivo está vacío.")
        if content_type not in self._allowed_content_types:
            allowed = ", ".join(self._allowed_content_types)
            raise ValidationException(
                f"Tipo de archivo no permitido. Permitidos: {allowed}."
            )
        if len(data) > self._max_bytes:
            max_mb = self._max_bytes // (1024 * 1024)
            raise ValidationException(
                f"El archivo excede el tamaño máximo permitido de {max_mb}MB."
            )
        if self._verify_signature and not _has_expected_signature(data, content_type):
            raise ValidationException(
                f"El contenido del archivo no corresponde a su tipo ({content_type})."
            )

    async def read_upload(self, upload: UploadedFile) -> bytes:
        data = await upload.read(self._max_bytes + 1)
        self.validate_upload(data, upload.content_type or "")
        return data

    async def upload(
        self,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> str:
        return await self._storage.upload(key, data, content_type=content_type)

    async def upload_file(
        self,
        key: str,
        file_path: Path,
        content_type: str = "application/octet-stream",
    ) -> str:
        return await self._storage.upload_file(key, file_path, content_type=content_type)

    async def delete(self, key: str) -> None:
        await self._storage.delete(key)

    async def generate_presigned_url(self, key: str, expires_in: int = 3600) -> str:
        return await self._storage.generate_presigned_url(key, expires_in=expires_in)

    def build_url(self, key: str | None) -> str | None:
        if not key:
            return None
        return f"{self._public_base_url}{self._url_prefix}/{key}"

    async def remove_other_files_in_scope(self, scope: str, keep_key: str) -> None:
        existing_keys = await self._storage.list_keys(prefix=scope)
        for existing in existing_keys:
            if existing != keep_key:
                await self._storage.delete(existing)

    async def remove_scope_dir(self, scope: str) -> None:
        existing_keys = await self._storage.list_keys(prefix=scope)
        for existing in existing_keys:
            await self._storage.delete(existing)
