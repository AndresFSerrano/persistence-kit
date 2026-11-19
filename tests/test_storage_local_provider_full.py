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


XLSX_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
DOCX_TYPE = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"


@pytest.mark.asyncio
async def test_upload_keeps_the_content_type_next_to_the_object_tree(tmp_path: Path):
    provider = _local(tmp_path)

    await provider.upload("exports/report", b"%PDF-1.7", "application/pdf")

    assert await provider.content_type_of("exports/report") == "application/pdf"
    stored = tmp_path / ".content-types" / "exports" / "report"
    assert stored.read_text(encoding="utf-8") == "application/pdf"
    assert sorted(path.name for path in (tmp_path / "exports").iterdir()) == ["report"]


@pytest.mark.asyncio
async def test_upload_file_keeps_the_content_type(tmp_path: Path):
    provider = _local(tmp_path)
    source = tmp_path / "source.bin"
    source.write_bytes(b"fake-xlsx-content")

    await provider.upload_file("exports/report.bin", source, XLSX_TYPE)

    assert await provider.content_type_of("exports/report.bin") == XLSX_TYPE


@pytest.mark.asyncio
async def test_upload_without_content_type_keeps_the_default(tmp_path: Path):
    provider = _local(tmp_path)

    await provider.upload("exports/report.csv", b"a,b\n")

    assert await provider.content_type_of("exports/report.csv") == "application/octet-stream"


@pytest.mark.asyncio
@pytest.mark.parametrize("content_type", [None, ""])
async def test_upload_with_an_empty_content_type_keeps_the_default(
    tmp_path: Path, content_type: str | None
):
    provider = _local(tmp_path)

    await provider.upload("avatars/u1.png", b"png", content_type)

    assert await provider.content_type_of("avatars/u1.png") == "application/octet-stream"


@pytest.mark.asyncio
async def test_upload_again_replaces_the_content_type(tmp_path: Path):
    provider = _local(tmp_path)
    await provider.upload("avatars/u1", b"png", "image/png")

    await provider.upload("avatars/u1", b"GIF89a", "image/gif")

    assert await provider.content_type_of("avatars/u1") == "image/gif"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("key", "expected"),
    [
        ("legacy/photo.png", "image/png"),
        ("legacy/photo.JPG", "image/jpeg"),
        ("legacy/photo.webp", "image/webp"),
        ("legacy/report.pdf", "application/pdf"),
        ("legacy/report.csv", "text/csv"),
        ("legacy/report.xlsx", XLSX_TYPE),
        ("legacy/template.docx", DOCX_TYPE),
        ("legacy/blob.unknownext", "application/octet-stream"),
        ("legacy/notes", "application/octet-stream"),
    ],
)
async def test_content_type_of_an_object_saved_before_types_were_kept_is_guessed(
    tmp_path: Path, key: str, expected: str
):
    provider = _local(tmp_path)
    provider.get_file_path(key).write_bytes(b"legacy")

    assert await provider.content_type_of(key) == expected


@pytest.mark.asyncio
async def test_content_type_of_ignores_an_empty_stored_type(tmp_path: Path):
    provider = _local(tmp_path)
    await provider.upload("avatars/u1.png", b"png", "image/png")
    (tmp_path / ".content-types" / "avatars" / "u1.png").write_text(" \n", encoding="utf-8")

    assert await provider.content_type_of("avatars/u1.png") == "image/png"


@pytest.mark.asyncio
async def test_content_type_of_a_missing_key_raises_not_found_without_creating_folders(
    tmp_path: Path,
):
    provider = _local(tmp_path)

    with pytest.raises(StorageNotFoundError) as err:
        await provider.content_type_of("templates/missing.docx")

    assert "templates/missing.docx" in str(err.value)
    assert not (tmp_path / "templates").exists()
    assert not (tmp_path / ".content-types").exists()


@pytest.mark.asyncio
async def test_delete_removes_the_content_type_and_its_empty_folders(tmp_path: Path):
    provider = _local(tmp_path)
    await provider.upload("scope-a/nested/file.txt", b"a", "text/plain")
    await provider.upload("scope-b/file.txt", b"b", "text/plain")

    await provider.delete("scope-a/nested/file.txt")

    assert not (tmp_path / "scope-a").exists()
    assert not (tmp_path / ".content-types" / "scope-a").exists()
    assert await provider.content_type_of("scope-b/file.txt") == "text/plain"


@pytest.mark.asyncio
async def test_delete_of_the_last_object_leaves_no_metadata_folder(tmp_path: Path):
    provider = _local(tmp_path)
    await provider.upload("scope-a/file.txt", b"a", "text/plain")

    await provider.delete("scope-a/file.txt")

    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_delete_of_an_object_without_stored_type_still_works(tmp_path: Path):
    provider = _local(tmp_path)
    provider.get_file_path("legacy/photo.png").write_bytes(b"legacy")

    await provider.delete("legacy/photo.png")

    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_list_keys_never_returns_stored_types(tmp_path: Path):
    provider = _local(tmp_path)
    await provider.upload("scope-a/one.txt", b"1", "text/plain")
    await provider.upload("scope-a/nested/two.txt", b"2", "text/plain")
    await provider.upload("scope-b/three.txt", b"3", "text/plain")

    everything = await provider.list_keys()
    scope_a = await provider.list_keys(prefix="scope-a")
    metadata = await provider.list_keys(prefix=".content-types")

    assert sorted(everything) == [
        "scope-a/nested/two.txt",
        "scope-a/one.txt",
        "scope-b/three.txt",
    ]
    assert sorted(scope_a) == ["scope-a/nested/two.txt", "scope-a/one.txt"]
    assert metadata == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "key",
    [
        ".content-types/scope/file.txt",
        ".Content-Types/scope/file.txt",
        "scope/../.content-types/file.txt",
        ".content-types",
    ],
)
async def test_keys_inside_the_content_types_folder_are_rejected(tmp_path: Path, key: str):
    provider = _local(tmp_path)

    with pytest.raises(ValueError) as upload_err:
        await provider.upload(key, b"x", "text/plain")
    with pytest.raises(ValueError) as read_err:
        await provider.read(key)
    with pytest.raises(ValueError) as type_err:
        await provider.content_type_of(key)
    with pytest.raises(ValueError) as path_err:
        provider.get_file_path(key)

    errors = [upload_err, read_err, type_err, path_err]
    assert all("Ruta reservada para metadatos" in str(err.value) for err in errors)
    assert list(tmp_path.iterdir()) == []
