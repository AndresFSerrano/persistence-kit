from __future__ import annotations

from pathlib import Path
from typing import Protocol


class ObjectStorage(Protocol):
    async def upload(
        self,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> str: ...

    async def upload_file(
        self,
        key: str,
        file_path: Path,
        content_type: str = "application/octet-stream",
    ) -> str: ...

    async def generate_presigned_url(
        self,
        key: str,
        expires_in: int = 3600,
    ) -> str: ...

    async def delete(self, key: str) -> None: ...

    async def list_keys(self, prefix: str = "") -> list[str]: ...
