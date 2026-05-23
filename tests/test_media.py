from pathlib import Path
from types import SimpleNamespace

import pytest

from persistence_kit.api.exceptions import ValidationException
from persistence_kit.settings import MediaStorageProvider, PersistenceKitSettings
from persistence_kit.storage import (
    LocalObjectStorage,
    MediaStorage,
    StorageConfigError,
    build_media_storage,
)
import persistence_kit.storage.factory as factory_mod
import persistence_kit.storage.s3 as s3_mod


class FakeClientError(Exception):
    pass


class FakeS3Client:
    def __init__(self) -> None:
        self.delete_calls: list[dict] = []
        self.paginate_calls: list[dict] = []
        self.list_pages: list[list[dict]] = []

    def delete_object(self, **kwargs) -> None:
        self.delete_calls.append(kwargs)

    def get_paginator(self, name: str):
        client = self

        class _Paginator:
            def paginate(self, **kwargs):
                client.paginate_calls.append({"name": name, **kwargs})
                for page in client.list_pages or [[]]:
                    yield {"Contents": page}

        return _Paginator()


@pytest.fixture(autouse=True)
def reset_factory_caches():
    factory_mod._local_media_object_storage_cached.cache_clear()
    factory_mod._s3_media_object_storage_cached.cache_clear()
    yield
    factory_mod._local_media_object_storage_cached.cache_clear()
    factory_mod._s3_media_object_storage_cached.cache_clear()


def _local_storage(tmp_path: Path) -> LocalObjectStorage:
    return LocalObjectStorage(
        base_dir=str(tmp_path),
        public_base_url="",
        signing_secret="unused",
    )


def _make_media(tmp_path: Path, **overrides) -> MediaStorage:
    return MediaStorage(
        storage=_local_storage(tmp_path),
        public_base_url=overrides.pop("public_base_url", "http://localhost:8000"),
        url_prefix=overrides.pop("url_prefix", "/media/products"),
        **overrides,
    )


def test_validate_upload_rejects_empty_data(tmp_path: Path):
    media = _make_media(tmp_path)

    with pytest.raises(ValidationException):
        media.validate_upload(b"", "image/png")


def test_validate_upload_rejects_disallowed_content_type(tmp_path: Path):
    media = _make_media(tmp_path)

    with pytest.raises(ValidationException) as exc:
        media.validate_upload(b"x", "application/pdf")

    assert "image/png" in str(exc.value)


def test_validate_upload_rejects_oversized_payload(tmp_path: Path):
    media = _make_media(tmp_path, max_bytes=10)

    with pytest.raises(ValidationException) as exc:
        media.validate_upload(b"x" * 11, "image/png")

    assert "máximo" in str(exc.value).lower() or "maximo" in str(exc.value).lower()


def test_validate_upload_accepts_allowed_input(tmp_path: Path):
    media = _make_media(tmp_path)

    media.validate_upload(b"valid bytes", "image/png")


def test_custom_allowed_content_types_extend_defaults(tmp_path: Path):
    media = _make_media(tmp_path, allowed_content_types=("application/pdf",))

    media.validate_upload(b"%PDF-1.4", "application/pdf")
    with pytest.raises(ValidationException):
        media.validate_upload(b"x", "image/png")


def test_build_url_returns_none_for_missing_key(tmp_path: Path):
    media = _make_media(tmp_path)

    assert media.build_url(None) is None
    assert media.build_url("") is None


def test_build_url_combines_base_prefix_and_key(tmp_path: Path):
    media = _make_media(
        tmp_path,
        public_base_url="http://localhost:8000",
        url_prefix="/media/products",
    )

    assert (
        media.build_url("abc/main.png")
        == "http://localhost:8000/media/products/abc/main.png"
    )


def test_build_url_strips_trailing_slash_from_base_and_normalizes_prefix(tmp_path: Path):
    media = _make_media(
        tmp_path,
        public_base_url="http://localhost:8000/",
        url_prefix="media/products/",
    )

    assert (
        media.build_url("abc/main.png")
        == "http://localhost:8000/media/products/abc/main.png"
    )


def test_build_url_works_without_public_base_url(tmp_path: Path):
    media = _make_media(tmp_path, public_base_url=None)

    assert media.build_url("abc/main.png") == "/media/products/abc/main.png"


@pytest.mark.asyncio
async def test_remove_other_files_in_scope_deletes_only_stale(tmp_path: Path):
    media = _make_media(tmp_path)
    await media.upload("abc/main.jpg", b"old", "image/jpeg")
    await media.upload("abc/main.png", b"new", "image/png")
    await media.upload("other/main.png", b"keep", "image/png")

    await media.remove_other_files_in_scope("abc", "abc/main.png")

    assert not (tmp_path / "abc" / "main.jpg").exists()
    assert (tmp_path / "abc" / "main.png").read_bytes() == b"new"
    assert (tmp_path / "other" / "main.png").read_bytes() == b"keep"


@pytest.mark.asyncio
async def test_remove_scope_dir_removes_all_keys_in_scope(tmp_path: Path):
    media = _make_media(tmp_path)
    await media.upload("abc/main.png", b"x", "image/png")
    await media.upload("abc/nested/extra.png", b"y", "image/png")
    await media.upload("other/main.png", b"z", "image/png")

    await media.remove_scope_dir("abc")

    assert not (tmp_path / "abc").exists()
    assert (tmp_path / "other" / "main.png").read_bytes() == b"z"


@pytest.mark.asyncio
async def test_remove_scope_dir_is_safe_when_scope_missing(tmp_path: Path):
    media = _make_media(tmp_path)

    await media.remove_scope_dir("never-existed")


def _settings(**overrides) -> PersistenceKitSettings:
    base = {
        "stage": "local",
        "auth_enabled": False,
        "export_storage_provider": "local",
        "media_storage_provider": "local",
        "public_base_url": "http://localhost:8000",
        "aws_s3_media_bucket": None,
    }
    base.update(overrides)
    return PersistenceKitSettings(**base)


def test_build_media_storage_local_writes_under_namespaced_dir(tmp_path: Path):
    settings = _settings(local_media_storage_dir=str(tmp_path))

    media = build_media_storage(
        settings, namespace="product_images", url_prefix="/media/products"
    )

    # The underlying LocalObjectStorage roots itself at <local_media_storage_dir>/<namespace>
    assert (tmp_path / "product_images").is_dir()
    assert media.url_prefix == "/media/products"


def test_build_media_storage_rejects_empty_namespace(tmp_path: Path):
    settings = _settings(local_media_storage_dir=str(tmp_path))

    with pytest.raises(StorageConfigError):
        build_media_storage(settings, namespace="  ", url_prefix="/media/products")


def test_build_media_storage_s3_requires_bucket(monkeypatch):
    monkeypatch.setattr(s3_mod, "boto3", SimpleNamespace(client=lambda *a, **kw: object()))
    settings = _settings(media_storage_provider="s3", aws_s3_media_bucket=None)

    with pytest.raises(StorageConfigError) as exc:
        build_media_storage(
            settings, namespace="product_images", url_prefix="/media/products"
        )

    assert "AWS_S3_MEDIA_BUCKET" in str(exc.value)


def test_build_media_storage_s3_uses_bucket(monkeypatch):
    fake_client = FakeS3Client()
    monkeypatch.setattr(s3_mod, "ClientError", FakeClientError)
    monkeypatch.setattr(
        s3_mod,
        "boto3",
        SimpleNamespace(client=lambda service_name, region_name: fake_client),
    )
    settings = _settings(
        media_storage_provider="s3",
        aws_s3_media_bucket="my-media",
        aws_region="us-east-1",
    )

    media = build_media_storage(
        settings, namespace="product_images", url_prefix="/media/products"
    )

    assert media.url_prefix == "/media/products"
    # Ensure the S3 backend was actually wired (not Local).
    assert fake_client.delete_calls == []
