from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import UploadFile
from starlette.datastructures import Headers

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
        self.put_calls: list[dict] = []
        self.delete_calls: list[dict] = []
        self.paginate_calls: list[dict] = []
        self.list_pages: list[list[dict]] = []

    def put_object(self, **kwargs) -> None:
        self.put_calls.append(kwargs)

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


def test_media_exposes_its_limits(tmp_path: Path):
    media = _make_media(tmp_path, allowed_content_types=("image/png",), max_bytes=10)

    assert media.allowed_content_types == ("image/png",)
    assert media.max_bytes == 10
    assert media.verify_signature is False


SIGNED_TYPES = ("application/pdf", "image/png", "image/jpeg", "image/gif", "image/webp")

VALID_CONTENT = (
    ("application/pdf", b"%PDF-1.7\n%\xe2\xe3\xcf\xd3\n"),
    ("image/png", b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"),
    ("image/jpeg", b"\xff\xd8\xff\xe0\x00\x10JFIF\x00"),
    ("image/gif", b"GIF87a\x01\x00\x01\x00"),
    ("image/gif", b"GIF89a\x01\x00\x01\x00"),
    ("image/webp", b"RIFF\x24\x00\x00\x00WEBPVP8 "),
)

INVALID_CONTENT = (
    ("application/pdf", b"just some text"),
    ("application/pdf", b"%PDF"),
    ("image/png", b"\x89PNG\r\n\x1a"),
    ("image/png", b"%PDF-1.7\n"),
    ("image/jpeg", b"\xff\xd8\x00\xe0"),
    ("image/gif", b"GIF88a\x01\x00"),
    ("image/webp", b"RIFF\x24\x00\x00\x00WAVEfmt "),
    ("image/webp", b"RIFX\x24\x00\x00\x00WEBPVP8 "),
)


def _verifying_media(tmp_path: Path, **overrides) -> MediaStorage:
    return _make_media(
        tmp_path,
        allowed_content_types=overrides.pop(
            "allowed_content_types", SIGNED_TYPES + ("text/csv",)
        ),
        verify_signature=True,
        **overrides,
    )


@pytest.mark.parametrize(("content_type", "data"), VALID_CONTENT)
def test_validate_upload_accepts_matching_signature(
    tmp_path: Path, content_type: str, data: bytes
):
    media = _verifying_media(tmp_path)

    media.validate_upload(data, content_type)


@pytest.mark.parametrize(("content_type", "data"), INVALID_CONTENT)
def test_validate_upload_rejects_mismatched_signature(
    tmp_path: Path, content_type: str, data: bytes
):
    media = _verifying_media(tmp_path)

    with pytest.raises(ValidationException) as exc:
        media.validate_upload(data, content_type)

    assert exc.value.detail == (
        f"El contenido del archivo no corresponde a su tipo ({content_type})."
    )


def test_validate_upload_skips_signature_for_unknown_types(tmp_path: Path):
    media = _verifying_media(tmp_path)

    media.validate_upload(b"id,name\n1,Ada\n", "text/csv")


@pytest.mark.parametrize(("content_type", "data"), INVALID_CONTENT)
def test_validate_upload_ignores_signature_when_verify_is_off(
    tmp_path: Path, content_type: str, data: bytes
):
    media = _make_media(tmp_path, allowed_content_types=SIGNED_TYPES)

    media.validate_upload(data, content_type)


def test_validate_upload_checks_size_before_signature(tmp_path: Path):
    media = _verifying_media(tmp_path, max_bytes=4)

    with pytest.raises(ValidationException) as exc:
        media.validate_upload(b"not a png", "image/png")

    assert "máximo" in exc.value.detail


class FakeUpload:
    def __init__(self, content: bytes, content_type: str | None) -> None:
        self.buffer = BytesIO(content)
        self.content_type = content_type
        self.read_sizes: list[int] = []

    async def read(self, size: int = -1) -> bytes:
        self.read_sizes.append(size)
        return self.buffer.read(size)


PNG_BYTES = b"\x89PNG\r\n\x1a\n" + b"\x00" * 12


@pytest.mark.asyncio
async def test_read_upload_returns_bytes_within_limit(tmp_path: Path):
    media = _verifying_media(tmp_path, max_bytes=64)
    upload = FakeUpload(PNG_BYTES, "image/png")

    data = await media.read_upload(upload)

    assert data == PNG_BYTES
    assert upload.read_sizes == [65]


@pytest.mark.asyncio
async def test_read_upload_accepts_file_of_exactly_max_bytes(tmp_path: Path):
    media = _verifying_media(tmp_path, max_bytes=len(PNG_BYTES))

    data = await media.read_upload(FakeUpload(PNG_BYTES, "image/png"))

    assert data == PNG_BYTES


@pytest.mark.asyncio
async def test_read_upload_rejects_oversize_reading_one_byte_past_the_limit(
    tmp_path: Path,
):
    media = _verifying_media(tmp_path, max_bytes=10)
    upload = FakeUpload(PNG_BYTES + b"\x00" * 1000, "image/png")

    with pytest.raises(ValidationException) as exc:
        await media.read_upload(upload)

    assert "máximo" in exc.value.detail
    assert upload.read_sizes == [11]
    assert upload.buffer.tell() == 11


@pytest.mark.asyncio
async def test_read_upload_rejects_empty_file(tmp_path: Path):
    media = _verifying_media(tmp_path)

    with pytest.raises(ValidationException) as exc:
        await media.read_upload(FakeUpload(b"", "image/png"))

    assert exc.value.detail == "El archivo está vacío."


@pytest.mark.asyncio
@pytest.mark.parametrize("content_type", ["text/plain", None])
async def test_read_upload_rejects_disallowed_or_missing_type(
    tmp_path: Path, content_type: str | None
):
    media = _verifying_media(tmp_path)

    with pytest.raises(ValidationException) as exc:
        await media.read_upload(FakeUpload(PNG_BYTES, content_type))

    assert exc.value.detail.startswith("Tipo de archivo no permitido.")


@pytest.mark.asyncio
async def test_read_upload_rejects_wrong_signature(tmp_path: Path):
    media = _verifying_media(tmp_path)

    with pytest.raises(ValidationException) as exc:
        await media.read_upload(FakeUpload(b"%PDF-1.7\n", "image/png"))

    assert exc.value.detail == (
        "El contenido del archivo no corresponde a su tipo (image/png)."
    )


@pytest.mark.asyncio
async def test_read_upload_accepts_a_fastapi_upload_file(tmp_path: Path):
    media = _verifying_media(tmp_path)
    upload = UploadFile(
        file=BytesIO(PNG_BYTES),
        filename="main.png",
        headers=Headers({"content-type": "image/png"}),
    )

    assert await media.read_upload(upload) == PNG_BYTES


@pytest.mark.asyncio
async def test_upload_file_and_delete_go_through_the_backend(tmp_path: Path):
    source = tmp_path / "source.png"
    source.write_bytes(PNG_BYTES)
    media = _make_media(tmp_path / "media")

    await media.upload_file("abc/main.png", source, "image/png")
    assert (tmp_path / "media" / "abc" / "main.png").read_bytes() == PNG_BYTES

    await media.delete("abc/main.png")
    assert not (tmp_path / "media" / "abc" / "main.png").exists()


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


def test_build_media_storage_leaves_signature_check_off_by_default(tmp_path: Path):
    settings = _settings(local_media_storage_dir=str(tmp_path))

    media = build_media_storage(
        settings, namespace="product_images", url_prefix="/media/products"
    )

    assert media.verify_signature is False
    media.validate_upload(b"valid bytes", "image/png")


def test_build_media_storage_passes_verify_signature(tmp_path: Path):
    settings = _settings(local_media_storage_dir=str(tmp_path))

    media = build_media_storage(
        settings,
        namespace="product_images",
        url_prefix="/media/products",
        verify_signature=True,
    )

    assert media.verify_signature is True
    with pytest.raises(ValidationException):
        media.validate_upload(b"valid bytes", "image/png")


def test_build_media_storage_rejects_unknown_provider(tmp_path: Path):
    settings = _settings(local_media_storage_dir=str(tmp_path)).model_copy(
        update={"media_storage_provider": "ftp"}
    )

    with pytest.raises(StorageConfigError) as exc:
        build_media_storage(settings, namespace="product_images", url_prefix="/media/products")

    assert "'ftp'" in str(exc.value)


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


@pytest.fixture
def s3_client(monkeypatch) -> FakeS3Client:
    client = FakeS3Client()

    def _build_client(*_args, **_kwargs) -> FakeS3Client:
        return client

    monkeypatch.setattr(s3_mod, "ClientError", FakeClientError)
    monkeypatch.setattr(s3_mod, "boto3", SimpleNamespace(client=_build_client))
    return client


def _s3_settings() -> PersistenceKitSettings:
    return _settings(
        media_storage_provider="s3",
        aws_s3_media_bucket="my-media",
        aws_region="us-east-1",
    )


@pytest.mark.asyncio
async def test_build_media_storage_s3_keeps_each_namespace_in_its_folder(s3_client: FakeS3Client):
    settings = _s3_settings()
    avatars = build_media_storage(settings, namespace="avatars", url_prefix="/media/avatars")
    products = build_media_storage(settings, namespace="product_images", url_prefix="/media/products")

    await avatars.upload("u1/main.png", b"avatar", "image/png")
    await products.upload("u1/main.png", b"product", "image/png")

    assert [call["Key"] for call in s3_client.put_calls] == [
        "avatars/u1/main.png",
        "product_images/u1/main.png",
    ]


@pytest.mark.asyncio
async def test_remove_other_files_in_scope_on_s3_keeps_the_current_file(s3_client: FakeS3Client):
    s3_client.list_pages = [
        [{"Key": "avatars/u1/old.png"}, {"Key": "avatars/u1/new.png"}],
    ]
    media = build_media_storage(_s3_settings(), namespace="avatars", url_prefix="/media/avatars")

    await media.remove_other_files_in_scope("u1", "u1/new.png")

    assert s3_client.delete_calls == [{"Bucket": "my-media", "Key": "avatars/u1/old.png"}]
