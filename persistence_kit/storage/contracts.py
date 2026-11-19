from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

DEFAULT_CONTENT_TYPE = "application/octet-stream"


class ObjectStorage(Protocol):
    async def upload(
        self,
        key: str,
        data: bytes,
        content_type: str = DEFAULT_CONTENT_TYPE,
    ) -> str: ...

    async def upload_file(
        self,
        key: str,
        file_path: Path,
        content_type: str = DEFAULT_CONTENT_TYPE,
    ) -> str: ...

    async def generate_presigned_url(
        self,
        key: str,
        expires_in: int = 3600,
    ) -> str: ...

    async def read(self, key: str) -> bytes: ...

    async def content_type_of(self, key: str) -> str: ...

    async def delete(self, key: str) -> None: ...

    async def list_keys(self, prefix: str = "") -> list[str]: ...


class UploadedFile(Protocol):
    @property
    def content_type(self) -> str | None: ...

    async def read(self, size: int = -1) -> bytes: ...


@dataclass(frozen=True, kw_only=True)
class ValidatedUpload:
    data: bytes
    content_type: str
