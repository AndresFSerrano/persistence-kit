from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from inspect import isawaitable
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from starlette import status

from persistence_kit.settings import PersistenceKitSettings
from persistence_kit.storage.errors import StorageConfigError
from persistence_kit.storage.factory import get_export_storage
from persistence_kit.storage.local import LocalExportStorageProvider

SettingsProvider = Callable[[], PersistenceKitSettings]
CurrentUserDependency = Callable[..., Any]
ExportDownloadAuthorizer = Callable[[str, Any], None | Awaitable[None]]


def build_local_export_storage_router(
    *,
    settings_provider: SettingsProvider,
    optional_current_user_dependency: CurrentUserDependency | None = None,
    authorize_download: ExportDownloadAuthorizer | None = None,
    prefix: str = "/exports",
    tags: Sequence[str] | None = None,
    route_path: str = "/local/{key:path}",
    summary: str = "Descargar archivo de exportacion local",
) -> APIRouter:
    router = APIRouter(prefix=prefix, tags=list(tags or ("Export Storage",)))
    current_user_dependency = optional_current_user_dependency or _anonymous_current_user

    @router.get(
        route_path,
        status_code=status.HTTP_200_OK,
        summary=summary,
    )
    async def download_local_export_ep(
        key: str,
        token: str | None = None,
        current_user: Any | None = Depends(current_user_dependency),
    ):
        return await serve_local_export(
            settings_provider=settings_provider,
            key=key,
            token=token,
            current_user=current_user,
            authorize_download=authorize_download,
        )

    return router


async def serve_local_export(
    *,
    settings_provider: SettingsProvider,
    key: str,
    token: str | None = None,
    current_user: Any | None = None,
    authorize_download: ExportDownloadAuthorizer | None = None,
) -> FileResponse:
    storage = _get_local_export_storage(settings_provider)
    if token and storage.validate_download_token(key, token):
        pass
    else:
        if current_user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="No tiene permisos para acceder a este recurso.",
            )
        if authorize_download is not None:
            authorization_result = authorize_download(key, current_user)
            if isawaitable(authorization_result):
                await authorization_result

    file_path = storage.get_file_path(key)
    if not file_path.is_file():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Archivo de exportacion no encontrado.",
        )

    return FileResponse(
        path=file_path,
        filename=file_path.name,
        media_type=guess_export_media_type(file_path),
    )


def guess_export_media_type(file_path: Path) -> str:
    if file_path.suffix.lower() == ".xlsx":
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    if file_path.suffix.lower() == ".csv":
        return "text/csv"
    return "application/octet-stream"


def _get_local_export_storage(
    settings_provider: SettingsProvider,
) -> LocalExportStorageProvider:
    try:
        storage = get_export_storage(settings_provider())
    except StorageConfigError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc
    if not isinstance(storage, LocalExportStorageProvider):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Storage local no disponible.",
        )
    return storage


def _anonymous_current_user() -> None:
    return None
