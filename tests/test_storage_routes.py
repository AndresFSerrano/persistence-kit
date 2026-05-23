from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from fastapi.responses import FileResponse, RedirectResponse

from persistence_kit.settings import PersistenceKitSettings
from persistence_kit.storage import (
    MediaStorage,
    build_local_export_storage_router,
    build_media_storage,
    get_export_storage,
    guess_export_media_type,
    serve_local_export,
    serve_media_object,
)
import persistence_kit.storage.factory as factory_mod
import persistence_kit.storage.s3 as s3_mod


@pytest.mark.asyncio
async def test_serve_local_export_serves_file_with_valid_token(tmp_path):
    settings = PersistenceKitSettings(
        local_export_storage_dir=str(tmp_path),
        public_base_url="http://localhost:8000",
    )
    storage = get_export_storage(settings)
    key = "exports/test/report.csv"
    await storage.upload(key, b"header\nvalue\n", "text/csv")
    token = (await storage.generate_presigned_url(key)).split("token=", maxsplit=1)[1]

    response = await serve_local_export(
        settings_provider=lambda: settings,
        key=key,
        token=token,
    )

    assert response.status_code == 200
    assert response.media_type == "text/csv"
    assert Path(response.path).read_bytes() == b"header\nvalue\n"


@pytest.mark.asyncio
async def test_serve_local_export_requires_auth_or_valid_token(tmp_path):
    settings = PersistenceKitSettings(local_export_storage_dir=str(tmp_path))

    with pytest.raises(HTTPException) as err:
        await serve_local_export(
            settings_provider=lambda: settings,
            key="exports/test/report.csv",
        )

    assert err.value.status_code == 401


@pytest.mark.asyncio
async def test_serve_local_export_uses_authorizer_for_authenticated_user(tmp_path):
    settings = PersistenceKitSettings(local_export_storage_dir=str(tmp_path))
    storage = get_export_storage(settings)
    key = "exports/test/report.csv"
    await storage.upload(key, b"header\nvalue\n", "text/csv")
    calls = []
    user = object()

    def authorize_download(received_key, received_user):
        calls.append((received_key, received_user))

    response = await serve_local_export(
        settings_provider=lambda: settings,
        key=key,
        current_user=user,
        authorize_download=authorize_download,
    )

    assert response.status_code == 200
    assert calls == [(key, user)]


@pytest.mark.asyncio
async def test_serve_local_export_returns_404_for_missing_file(tmp_path):
    settings = PersistenceKitSettings(local_export_storage_dir=str(tmp_path))
    storage = get_export_storage(settings)
    key = "exports/test/missing.csv"
    token = (await storage.generate_presigned_url(key)).split("token=", maxsplit=1)[1]

    with pytest.raises(HTTPException) as err:
        await serve_local_export(
            settings_provider=lambda: settings,
            key=key,
            token=token,
        )

    assert err.value.status_code == 404


def test_guess_export_media_type():
    assert (
        guess_export_media_type(Path("file.xlsx"))
        == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    assert guess_export_media_type(Path("file.csv")) == "text/csv"
    assert guess_export_media_type(Path("file.bin")) == "application/octet-stream"


def test_build_local_export_storage_router_registers_local_route():
    router = build_local_export_storage_router(
        settings_provider=lambda: PersistenceKitSettings(),
    )

    assert any(route.path == "/exports/local/{key:path}" for route in router.routes)


@pytest.fixture
def reset_media_factory_caches():
    factory_mod._local_media_object_storage_cached.cache_clear()
    factory_mod._s3_media_object_storage_cached.cache_clear()
    yield
    factory_mod._local_media_object_storage_cached.cache_clear()
    factory_mod._s3_media_object_storage_cached.cache_clear()


@pytest.mark.asyncio
async def test_serve_media_object_returns_file_response_for_local_backend(
    tmp_path: Path, reset_media_factory_caches
):
    settings = PersistenceKitSettings(
        local_media_storage_dir=str(tmp_path),
        media_storage_provider="local",
    )
    storage = build_media_storage(
        settings, namespace="product_images", url_prefix="/media/product_images"
    )
    await storage.upload("abc/main.png", b"png-bytes", "image/png")

    response = await serve_media_object(storage, "abc/main.png", media_type="image/png")

    assert isinstance(response, FileResponse)
    assert response.media_type == "image/png"


@pytest.mark.asyncio
async def test_serve_media_object_uses_default_media_type_when_unspecified(
    tmp_path: Path, reset_media_factory_caches
):
    settings = PersistenceKitSettings(
        local_media_storage_dir=str(tmp_path),
        media_storage_provider="local",
    )
    storage = build_media_storage(
        settings, namespace="product_images", url_prefix="/media/product_images"
    )
    await storage.upload("abc/main.png", b"x", "image/png")

    response = await serve_media_object(storage, "abc/main.png")

    assert isinstance(response, FileResponse)
    assert response.media_type == "application/octet-stream"


@pytest.mark.asyncio
async def test_serve_media_object_raises_404_when_local_file_missing(
    tmp_path: Path, reset_media_factory_caches
):
    settings = PersistenceKitSettings(
        local_media_storage_dir=str(tmp_path),
        media_storage_provider="local",
    )
    storage = build_media_storage(
        settings, namespace="product_images", url_prefix="/media/product_images"
    )

    with pytest.raises(HTTPException) as exc:
        await serve_media_object(storage, "abc/missing.png")

    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_serve_media_object_redirects_for_non_local_backend(monkeypatch, reset_media_factory_caches):
    class FakeS3Client:
        def __init__(self):
            self.presign_calls = []

        def generate_presigned_url(self, operation_name, *, Params, ExpiresIn):
            self.presign_calls.append({"operation_name": operation_name, "Params": Params, "ExpiresIn": ExpiresIn})
            return "https://example.com/signed?token=abc"

    fake_client = FakeS3Client()
    monkeypatch.setattr(s3_mod, "boto3", SimpleNamespace(client=lambda service_name, region_name: fake_client))

    settings = PersistenceKitSettings(
        media_storage_provider="s3",
        aws_s3_media_bucket="my-media",
        aws_region="us-east-1",
    )
    storage = build_media_storage(
        settings, namespace="product_images", url_prefix="/media/product_images"
    )

    response = await serve_media_object(storage, "abc/main.png")

    assert isinstance(response, RedirectResponse)
    assert response.headers["location"] == "https://example.com/signed?token=abc"
