from pathlib import Path

import pytest
from fastapi import HTTPException

from persistence_kit.settings import PersistenceKitSettings
from persistence_kit.storage import (
    build_local_export_storage_router,
    get_export_storage,
    guess_export_media_type,
    serve_local_export,
)


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
