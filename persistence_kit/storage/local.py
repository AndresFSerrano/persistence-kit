from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import mimetypes
import shutil
from datetime import datetime, timedelta, timezone
from functools import cache
from pathlib import Path
from urllib.parse import quote

from persistence_kit.storage.contracts import DEFAULT_CONTENT_TYPE
from persistence_kit.storage.errors import StorageNotFoundError, StoragePathError

_CONTENT_TYPES_DIR = ".content-types"
_EXTRA_CONTENT_TYPES = (
    ("image/webp", ".webp"),
    ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", ".xlsx"),
    ("application/vnd.openxmlformats-officedocument.wordprocessingml.document", ".docx"),
)


def _b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64decode(data: str) -> bytes:
    padded = data + ("=" * (-len(data) % 4))
    return base64.urlsafe_b64decode(padded.encode("ascii"))


@cache
def _content_type_guesser() -> mimetypes.MimeTypes:
    guesser = mimetypes.MimeTypes()
    for content_type, extension in _EXTRA_CONTENT_TYPES:
        guesser.add_type(content_type, extension)
    return guesser


def _guess_content_type(file_name: str) -> str:
    guessed, _ = _content_type_guesser().guess_type(file_name)
    return guessed or DEFAULT_CONTENT_TYPE


def _is_reserved(relative: Path) -> bool:
    return bool(relative.parts) and relative.parts[0].casefold() == _CONTENT_TYPES_DIR


class LocalObjectStorage:
    def __init__(
        self,
        *,
        base_dir: str,
        public_base_url: str,
        signing_secret: str,
        download_route_path: str = "/api/v1/exports/local",
    ) -> None:
        self._base_dir = Path(base_dir).resolve()
        self._public_base_url = public_base_url.rstrip("/")
        self._signing_secret = signing_secret
        self._download_route_path = "/" + download_route_path.strip("/")
        self._base_dir.mkdir(parents=True, exist_ok=True)
        self._content_types_dir = self._base_dir / _CONTENT_TYPES_DIR

    def _resolve(self, key: str) -> Path:
        destination = (self._base_dir / Path(key)).resolve()
        if self._base_dir not in destination.parents and destination != self._base_dir:
            raise StoragePathError(f"Ruta de exportacion invalida: '{key}'.")
        if _is_reserved(destination.relative_to(self._base_dir)):
            raise StoragePathError(f"Ruta reservada para metadatos: '{key}'.")
        return destination

    def _existing_file(self, key: str) -> Path:
        source = self._resolve(key)
        if not source.is_file():
            raise StorageNotFoundError(f"No existe el archivo '{key}'.")
        return source

    def _content_type_path(self, destination: Path) -> Path:
        return self._content_types_dir / destination.relative_to(self._base_dir)

    def get_file_path(self, key: str) -> Path:
        destination = self._resolve(key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        return destination

    async def upload(
        self,
        key: str,
        data: bytes,
        content_type: str = DEFAULT_CONTENT_TYPE,
    ) -> str:
        destination = self.get_file_path(key)
        await asyncio.to_thread(destination.write_bytes, data)
        await self._save_content_type(destination, content_type)
        return key

    async def upload_file(
        self,
        key: str,
        file_path: Path,
        content_type: str = DEFAULT_CONTENT_TYPE,
    ) -> str:
        destination = self.get_file_path(key)
        await asyncio.to_thread(shutil.copyfile, file_path, destination)
        await self._save_content_type(destination, content_type)
        return key

    async def _save_content_type(self, destination: Path, content_type: str) -> None:
        content_type_path = self._content_type_path(destination)

        def _save() -> None:
            content_type_path.parent.mkdir(parents=True, exist_ok=True)
            content_type_path.write_text(content_type or DEFAULT_CONTENT_TYPE, encoding="utf-8")

        await asyncio.to_thread(_save)

    async def read(self, key: str) -> bytes:
        source = self._existing_file(key)
        return await asyncio.to_thread(source.read_bytes)

    async def content_type_of(self, key: str) -> str:
        source = self._existing_file(key)
        content_type_path = self._content_type_path(source)

        def _content_type() -> str:
            if content_type_path.is_file():
                stored = content_type_path.read_text(encoding="utf-8").strip()
                if stored:
                    return stored
            return _guess_content_type(source.name)

        return await asyncio.to_thread(_content_type)

    async def generate_presigned_url(self, key: str, expires_in: int = 3600) -> str:
        encoded_key = quote(key, safe="/")
        token = self.generate_download_token(key, expires_in=expires_in)
        encoded_token = quote(token, safe="")
        return (
            f"{self._public_base_url}{self._download_route_path}/"
            f"{encoded_key}?token={encoded_token}"
        )

    def generate_download_token(self, key: str, expires_in: int = 3600) -> str:
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
        payload = {
            "key": key,
            "exp": int(expires_at.timestamp()),
        }
        payload_raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        payload_token = _b64encode(payload_raw)
        signature = hmac.new(
            self._signing_secret.encode("utf-8"),
            payload_token.encode("ascii"),
            hashlib.sha256,
        ).digest()
        return f"{payload_token}.{_b64encode(signature)}"

    async def delete(self, key: str) -> None:
        destination = self.get_file_path(key)
        content_type_path = self._content_type_path(destination)

        def _delete() -> None:
            self._remove_with_empty_parents(destination)
            self._remove_with_empty_parents(content_type_path)

        await asyncio.to_thread(_delete)

    def _remove_with_empty_parents(self, path: Path) -> None:
        if path.is_file():
            path.unlink()
        current = path.parent
        while current != self._base_dir and current.is_relative_to(self._base_dir):
            try:
                if not current.is_dir() or any(current.iterdir()):
                    break
                current.rmdir()
            except OSError:
                break
            current = current.parent

    async def list_keys(self, prefix: str = "") -> list[str]:
        normalized_prefix = prefix.strip("/").rstrip("/")
        root = self._base_dir / normalized_prefix if normalized_prefix else self._base_dir

        def _list() -> list[str]:
            if not root.is_dir():
                return []
            collected: list[str] = []
            for path in root.rglob("*"):
                if path.is_file():
                    rel = path.relative_to(self._base_dir)
                    if not _is_reserved(rel):
                        collected.append(rel.as_posix())
            return collected

        return await asyncio.to_thread(_list)

    def validate_download_token(self, key: str, token: str) -> bool:
        try:
            payload_token, signature_token = token.split(".", maxsplit=1)
            expected_signature = hmac.new(
                self._signing_secret.encode("utf-8"),
                payload_token.encode("ascii"),
                hashlib.sha256,
            ).digest()
            actual_signature = _b64decode(signature_token)
            if not hmac.compare_digest(actual_signature, expected_signature):
                return False
            payload = json.loads(_b64decode(payload_token).decode("utf-8"))
            expires_at = int(payload["exp"])
        except Exception:
            return False
        if payload.get("key") != key:
            return False
        return datetime.now(timezone.utc).timestamp() < expires_at


LocalExportStorageProvider = LocalObjectStorage
