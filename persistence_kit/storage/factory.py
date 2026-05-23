from functools import lru_cache
from typing import Callable

from persistence_kit.settings import (
    ExportStorageProvider,
    MediaStorageProvider,
    PersistenceKitSettings,
)
from persistence_kit.storage.contracts import ObjectStorage
from persistence_kit.storage.errors import StorageConfigError

ExportStorage = ObjectStorage
ExportStorageBuilder = Callable[[PersistenceKitSettings], ExportStorage]


@lru_cache
def _local_export_storage_cached(
    base_dir: str,
    public_base_url: str,
    signing_secret: str,
) -> ExportStorage:
    from persistence_kit.storage.local import LocalExportStorageProvider

    return LocalExportStorageProvider(
        base_dir=base_dir,
        public_base_url=public_base_url,
        signing_secret=signing_secret,
    )


@lru_cache
def _s3_export_storage_cached(
    bucket: str,
    region: str,
) -> ExportStorage:
    from persistence_kit.storage.s3 import S3ExportStorageProvider

    return S3ExportStorageProvider(bucket=bucket, region=region)


def _build_local_export_storage(settings: PersistenceKitSettings) -> ExportStorage:
    public_base_url = (settings.public_base_url or "http://localhost:8000").rstrip("/")
    return _local_export_storage_cached(
        settings.local_export_storage_dir,
        public_base_url,
        settings.local_export_url_secret,
    )


def _build_s3_export_storage(settings: PersistenceKitSettings) -> ExportStorage:
    if not settings.aws_s3_export_bucket:
        raise StorageConfigError(
            "Falta configuracion AWS_S3_EXPORT_BUCKET para el storage de exportaciones."
        )
    return _s3_export_storage_cached(settings.aws_s3_export_bucket, settings.aws_region)


EXPORT_STORAGE_BUILDERS: dict[ExportStorageProvider, ExportStorageBuilder] = {
    ExportStorageProvider.LOCAL: _build_local_export_storage,
    ExportStorageProvider.S3: _build_s3_export_storage,
}


def get_export_storage(settings: PersistenceKitSettings) -> ExportStorage:
    builder = EXPORT_STORAGE_BUILDERS.get(settings.export_storage_provider)
    if builder is None:
        provider = getattr(
            settings.export_storage_provider,
            "value",
            settings.export_storage_provider,
        )
        raise StorageConfigError(
            f"Proveedor de storage de exportaciones no soportado: '{provider}'."
        )
    return builder(settings)


@lru_cache
def _local_media_object_storage_cached(base_dir: str) -> ObjectStorage:
    from persistence_kit.storage.local import LocalObjectStorage

    return LocalObjectStorage(
        base_dir=base_dir,
        public_base_url="",
        signing_secret="not-used-for-public-media",
    )


@lru_cache
def _s3_media_object_storage_cached(bucket: str, region: str) -> ObjectStorage:
    from persistence_kit.storage.s3 import S3ObjectStorage

    return S3ObjectStorage(bucket=bucket, region=region)


def _build_media_object_storage(
    settings: PersistenceKitSettings,
    namespace: str,
) -> ObjectStorage:
    namespace = namespace.strip().strip("/").strip()
    if not namespace:
        raise StorageConfigError("El namespace de MediaStorage no puede estar vacío.")
    if settings.media_storage_provider == MediaStorageProvider.LOCAL:
        return _local_media_object_storage_cached(
            f"{settings.local_media_storage_dir.rstrip('/')}/{namespace}"
        )
    if settings.media_storage_provider == MediaStorageProvider.S3:
        if not settings.aws_s3_media_bucket:
            raise StorageConfigError(
                "Falta configuracion AWS_S3_MEDIA_BUCKET para el storage de media."
            )
        return _s3_media_object_storage_cached(
            settings.aws_s3_media_bucket, settings.aws_region
        )
    provider = getattr(
        settings.media_storage_provider, "value", settings.media_storage_provider
    )
    raise StorageConfigError(
        f"Proveedor de storage de media no soportado: '{provider}'."
    )


def build_media_storage(
    settings: PersistenceKitSettings,
    *,
    namespace: str,
    url_prefix: str,
    allowed_content_types=None,
    max_bytes: int | None = None,
):
    from persistence_kit.storage.media import MediaStorage

    storage = _build_media_object_storage(settings, namespace)
    return MediaStorage(
        storage=storage,
        public_base_url=settings.public_base_url,
        url_prefix=url_prefix,
        allowed_content_types=allowed_content_types,
        max_bytes=max_bytes,
    )
