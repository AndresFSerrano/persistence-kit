from pathlib import Path
from types import SimpleNamespace

import pytest

from persistence_kit.storage import (
    LocalObjectStorage,
    S3ObjectStorage,
    StorageConfigError,
    StoragePresignError,
    StorageUploadError,
)
import persistence_kit.storage.s3 as s3_mod


class FakeClientError(Exception):
    pass


class FakeS3Client:
    def __init__(self) -> None:
        self.put_calls: list[dict] = []
        self.upload_fileobj_calls: list[dict] = []
        self.presign_calls: list[dict] = []
        self.put_error: Exception | None = None
        self.upload_fileobj_error: Exception | None = None
        self.presign_error: Exception | None = None

    def put_object(self, **kwargs) -> None:
        self.put_calls.append(kwargs)
        if self.put_error is not None:
            raise self.put_error

    def upload_fileobj(self, **kwargs) -> None:
        body = kwargs["Fileobj"].read()
        self.upload_fileobj_calls.append(
            {
                "Bucket": kwargs["Bucket"],
                "Key": kwargs["Key"],
                "ExtraArgs": kwargs["ExtraArgs"],
                "Body": body,
            }
        )
        if self.upload_fileobj_error is not None:
            raise self.upload_fileobj_error

    def generate_presigned_url(self, operation_name: str, *, Params: dict, ExpiresIn: int) -> str:
        self.presign_calls.append(
            {
                "operation_name": operation_name,
                "Params": Params,
                "ExpiresIn": ExpiresIn,
            }
        )
        if self.presign_error is not None:
            raise self.presign_error
        return "https://example.com/presigned"


@pytest.fixture
def fake_s3_client(monkeypatch) -> FakeS3Client:
    client = FakeS3Client()
    monkeypatch.setattr(s3_mod, "ClientError", FakeClientError)
    monkeypatch.setattr(
        s3_mod,
        "boto3",
        SimpleNamespace(client=lambda service_name, region_name: client),
    )
    return client


@pytest.mark.asyncio
async def test_local_storage_uploads_bytes_and_signs_download_url(tmp_path: Path):
    storage = LocalObjectStorage(
        base_dir=str(tmp_path),
        public_base_url="http://localhost:8000",
        signing_secret="test-secret",
    )

    key = await storage.upload("exports/report.csv", b"id,name\n1,Ada\n", "text/csv")
    url = await storage.generate_presigned_url(key)
    token = url.split("token=", maxsplit=1)[1]

    assert key == "exports/report.csv"
    assert (tmp_path / "exports" / "report.csv").read_bytes() == b"id,name\n1,Ada\n"
    assert url.startswith("http://localhost:8000/api/v1/exports/local/exports/report.csv?token=")
    assert storage.validate_download_token(key, token) is True
    assert storage.validate_download_token("exports/other.csv", token) is False


def test_local_storage_rejects_parent_traversal(tmp_path: Path):
    storage = LocalObjectStorage(
        base_dir=str(tmp_path),
        public_base_url="http://localhost:8000",
        signing_secret="test-secret",
    )

    with pytest.raises(ValueError) as exc:
        storage.get_file_path("../escape.txt")

    assert "Ruta de exportacion invalida" in str(exc.value)


def test_s3_storage_requires_boto3(monkeypatch):
    monkeypatch.setattr(s3_mod, "boto3", None)

    with pytest.raises(StorageConfigError):
        S3ObjectStorage(bucket="exports", region="us-east-1")


@pytest.mark.asyncio
async def test_s3_storage_uploads_bytes(fake_s3_client: FakeS3Client):
    storage = S3ObjectStorage(bucket="exports", region="us-east-1")

    key = await storage.upload("reports/file.csv", b"a,b\n1,2\n", "text/csv")

    assert key == "reports/file.csv"
    assert fake_s3_client.put_calls == [
        {
            "Bucket": "exports",
            "Key": "reports/file.csv",
            "Body": b"a,b\n1,2\n",
            "ContentType": "text/csv",
        }
    ]


@pytest.mark.asyncio
async def test_s3_storage_upload_errors_are_domain_errors(fake_s3_client: FakeS3Client):
    fake_s3_client.put_error = FakeClientError("boom")
    storage = S3ObjectStorage(bucket="exports", region="us-east-1")

    with pytest.raises(StorageUploadError):
        await storage.upload("reports/file.csv", b"data")


@pytest.mark.asyncio
async def test_s3_storage_uploads_file(tmp_path: Path, fake_s3_client: FakeS3Client):
    source = tmp_path / "report.xlsx"
    source.write_bytes(b"fake-xlsx")
    storage = S3ObjectStorage(bucket="exports", region="us-east-1")

    key = await storage.upload_file("reports/file.xlsx", source, "application/vnd.test")

    assert key == "reports/file.xlsx"
    assert fake_s3_client.upload_fileobj_calls == [
        {
            "Bucket": "exports",
            "Key": "reports/file.xlsx",
            "ExtraArgs": {"ContentType": "application/vnd.test"},
            "Body": b"fake-xlsx",
        }
    ]


@pytest.mark.asyncio
async def test_s3_storage_presigns_url(fake_s3_client: FakeS3Client):
    storage = S3ObjectStorage(bucket="exports", region="us-east-1")

    url = await storage.generate_presigned_url("reports/file.xlsx", expires_in=120)

    assert url == "https://example.com/presigned"
    assert fake_s3_client.presign_calls == [
        {
            "operation_name": "get_object",
            "Params": {"Bucket": "exports", "Key": "reports/file.xlsx"},
            "ExpiresIn": 120,
        }
    ]


@pytest.mark.asyncio
async def test_s3_storage_presign_errors_are_domain_errors(fake_s3_client: FakeS3Client):
    fake_s3_client.presign_error = FakeClientError("boom")
    storage = S3ObjectStorage(bucket="exports", region="us-east-1")

    with pytest.raises(StoragePresignError):
        await storage.generate_presigned_url("reports/file.xlsx")
