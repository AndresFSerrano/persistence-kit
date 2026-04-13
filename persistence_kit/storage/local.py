from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote

from persistence_kit.storage.errors import StoragePathError


def _b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64decode(data: str) -> bytes:
    padded = data + ("=" * (-len(data) % 4))
    return base64.urlsafe_b64decode(padded.encode("ascii"))


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

    def get_file_path(self, key: str) -> Path:
        normalized = Path(key)
        destination = (self._base_dir / normalized).resolve()
        if self._base_dir not in destination.parents and destination != self._base_dir:
            raise StoragePathError(f"Ruta de exportacion invalida: '{key}'.")
        destination.parent.mkdir(parents=True, exist_ok=True)
        return destination

    async def upload(
        self,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> str:
        _ = content_type
        destination = self.get_file_path(key)
        await asyncio.to_thread(destination.write_bytes, data)
        return key

    async def upload_file(
        self,
        key: str,
        file_path: Path,
        content_type: str = "application/octet-stream",
    ) -> str:
        _ = content_type
        destination = self.get_file_path(key)
        await asyncio.to_thread(shutil.copyfile, file_path, destination)
        return key

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
