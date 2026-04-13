from persistence_kit.storage.contracts import ObjectStorage
from persistence_kit.storage.errors import (
    StorageConfigError,
    StorageError,
    StoragePathError,
    StoragePresignError,
    StorageUploadError,
)
from persistence_kit.storage.factory import (
    EXPORT_STORAGE_BUILDERS,
    ExportStorage,
    ExportStorageBuilder,
    get_export_storage,
)

_LOCAL_EXPORTS = {"LocalObjectStorage", "LocalExportStorageProvider"}
_ROUTE_EXPORTS = {
    "SettingsProvider",
    "CurrentUserDependency",
    "ExportDownloadAuthorizer",
    "build_local_export_storage_router",
    "serve_local_export",
    "guess_export_media_type",
}
_S3_EXPORTS = {"S3ObjectStorage", "S3ExportStorageProvider"}

__all__ = [
    "ObjectStorage",
    "StorageError",
    "StorageConfigError",
    "StoragePathError",
    "StorageUploadError",
    "StoragePresignError",
    "LocalObjectStorage",
    "LocalExportStorageProvider",
    "S3ObjectStorage",
    "S3ExportStorageProvider",
    "ExportStorage",
    "ExportStorageBuilder",
    "EXPORT_STORAGE_BUILDERS",
    "get_export_storage",
    "SettingsProvider",
    "CurrentUserDependency",
    "ExportDownloadAuthorizer",
    "build_local_export_storage_router",
    "serve_local_export",
    "guess_export_media_type",
]


def __getattr__(name: str):
    if name in _LOCAL_EXPORTS:
        from persistence_kit.storage import local

        value = getattr(local, name)
        globals()[name] = value
        return value
    if name in _ROUTE_EXPORTS:
        from persistence_kit.storage import routes

        value = getattr(routes, name)
        globals()[name] = value
        return value
    if name in _S3_EXPORTS:
        from persistence_kit.storage import s3

        value = getattr(s3, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
