import pytest

import persistence_kit.storage.factory as mod
from persistence_kit.settings import ExportStorageProvider, PersistenceKitSettings
from persistence_kit.storage import LocalExportStorageProvider, StorageConfigError


def test_get_export_storage_uses_local_by_default(tmp_path):
    mod._local_export_storage_cached.cache_clear()
    settings = PersistenceKitSettings(local_export_storage_dir=str(tmp_path))
    public_base_url = (settings.public_base_url or "http://localhost:8000").rstrip("/")

    storage = mod.get_export_storage(settings)

    assert isinstance(storage, LocalExportStorageProvider)
    assert storage is mod._local_export_storage_cached(
        str(tmp_path),
        public_base_url,
        settings.local_export_url_secret,
    )


def test_get_export_storage_returns_cached_local_instance(tmp_path):
    mod._local_export_storage_cached.cache_clear()
    settings = PersistenceKitSettings(
        export_storage_provider=ExportStorageProvider.LOCAL,
        local_export_storage_dir=str(tmp_path),
    )

    first = mod.get_export_storage(settings)
    second = mod.get_export_storage(settings)

    assert first is second


def test_get_export_storage_requires_bucket_for_s3():
    mod._s3_export_storage_cached.cache_clear()
    settings = PersistenceKitSettings(
        export_storage_provider=ExportStorageProvider.S3,
        aws_s3_export_bucket=None,
    )

    with pytest.raises(StorageConfigError) as err:
        mod.get_export_storage(settings)

    assert "AWS_S3_EXPORT_BUCKET" in str(err.value)


def test_get_export_storage_raises_for_unregistered_provider(tmp_path, monkeypatch):
    settings = PersistenceKitSettings(
        export_storage_provider=ExportStorageProvider.LOCAL,
        local_export_storage_dir=str(tmp_path),
    )
    monkeypatch.setattr(mod, "EXPORT_STORAGE_BUILDERS", {})

    with pytest.raises(StorageConfigError) as err:
        mod.get_export_storage(settings)

    assert "no soportado" in str(err.value)
