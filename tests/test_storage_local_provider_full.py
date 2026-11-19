from pathlib import Path

import pytest

from persistence_kit.storage import LocalExportStorageProvider, StorageNotFoundError


@pytest.mark.asyncio
async def test_upload_persists_bytes_and_returns_key(tmp_path: Path):
    provider = LocalExportStorageProvider(
        base_dir=str(tmp_path),
        public_base_url="http://localhost:8000",
        signing_secret="test-secret-key-with-32-bytes-min",
    )

    key = await provider.upload("exports/report.csv", b"col1,col2\n1,2\n")

    assert key == "exports/report.csv"
    assert (tmp_path / "exports" / "report.csv").read_bytes() == b"col1,col2\n1,2\n"


@pytest.mark.asyncio
async def test_upload_file_copies_file_and_generates_file_uri(tmp_path: Path):
    provider = LocalExportStorageProvider(
        base_dir=str(tmp_path),
        public_base_url="http://localhost:8000",
        signing_secret="test-secret-key-with-32-bytes-min",
    )
    source_file = tmp_path / "source.xlsx"
    source_file.write_bytes(b"fake-xlsx-content")

    key = await provider.upload_file("exports/key-status-history/report.xlsx", source_file)
    uri = await provider.generate_presigned_url(key)

    destination = tmp_path / "exports" / "key-status-history" / "report.xlsx"
    assert destination.read_bytes() == b"fake-xlsx-content"
    assert uri.startswith("http://localhost:8000/api/v1/exports/local/exports/key-status-history/report.xlsx?token=")


def test_resolve_path_rejects_parent_traversal(tmp_path: Path):
    provider = LocalExportStorageProvider(
        base_dir=str(tmp_path),
        public_base_url="http://localhost:8000",
        signing_secret="test-secret-key-with-32-bytes-min",
    )

    with pytest.raises(ValueError) as err:
        provider.get_file_path("../escape.txt")

    assert "Ruta de exportacion invalida" in str(err.value)


@pytest.mark.asyncio
async def test_generate_presigned_url_token_is_valid_for_key(tmp_path: Path):
    provider = LocalExportStorageProvider(
        base_dir=str(tmp_path),
        public_base_url="http://localhost:8000",
        signing_secret="test-secret-key-with-32-bytes-min",
    )

    uri = await provider.generate_presigned_url("exports/test/report.xlsx")
    token = uri.split("token=")[1]

    assert provider.validate_download_token("exports/test/report.xlsx", token) is True


def _local(tmp_path: Path) -> LocalExportStorageProvider:
    return LocalExportStorageProvider(
        base_dir=str(tmp_path),
        public_base_url="http://localhost:8000",
        signing_secret="test-secret-key-with-32-bytes-min",
    )


@pytest.mark.asyncio
async def test_read_returns_the_stored_bytes(tmp_path: Path):
    provider = _local(tmp_path)
    await provider.upload("templates/v1.docx", b"word-bytes")

    assert await provider.read("templates/v1.docx") == b"word-bytes"


@pytest.mark.asyncio
async def test_read_of_a_missing_key_raises_not_found_without_creating_folders(tmp_path: Path):
    provider = _local(tmp_path)

    with pytest.raises(StorageNotFoundError) as err:
        await provider.read("templates/missing.docx")

    assert "templates/missing.docx" in str(err.value)
    assert not (tmp_path / "templates").exists()


@pytest.mark.asyncio
async def test_read_of_a_folder_raises_not_found(tmp_path: Path):
    provider = _local(tmp_path)
    await provider.upload("templates/v1.docx", b"word-bytes")

    with pytest.raises(StorageNotFoundError):
        await provider.read("templates")


@pytest.mark.asyncio
async def test_read_rejects_parent_traversal(tmp_path: Path):
    (tmp_path.parent / "outside.txt").write_bytes(b"secret")
    provider = _local(tmp_path)

    with pytest.raises(ValueError) as err:
        await provider.read("../outside.txt")

    assert "Ruta de exportacion invalida" in str(err.value)
