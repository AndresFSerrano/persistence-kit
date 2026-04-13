from pathlib import Path
from types import SimpleNamespace

import pytest

import persistence_kit.storage.s3 as mod
from persistence_kit.storage import (
    S3ExportStorageProvider,
    StorageConfigError,
    StoragePresignError,
    StorageUploadError,
)


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
        file_obj = kwargs["Fileobj"]
        body = file_obj.read()
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
    monkeypatch.setattr(mod, "ClientError", FakeClientError)
    monkeypatch.setattr(
        mod,
        "boto3",
        SimpleNamespace(client=lambda service_name, region_name: client),
    )
    return client


def test_init_requires_boto3_dependency(monkeypatch):
    monkeypatch.setattr(mod, "boto3", None)

    with pytest.raises(StorageConfigError) as err:
        S3ExportStorageProvider(bucket="exports", region="us-east-1")

    assert "boto3" in str(err.value)


@pytest.mark.asyncio
async def test_upload_puts_object_to_s3(fake_s3_client: FakeS3Client):
    provider = S3ExportStorageProvider(bucket="exports", region="us-east-1")

    key = await provider.upload("reports/file.csv", b"a,b\n1,2\n", "text/csv")

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
async def test_upload_raises_http_exception_when_s3_fails(fake_s3_client: FakeS3Client):
    fake_s3_client.put_error = FakeClientError("boom")
    provider = S3ExportStorageProvider(bucket="exports", region="us-east-1")

    with pytest.raises(StorageUploadError) as err:
        await provider.upload("reports/file.csv", b"data")

    assert "No fue posible subir el archivo a S3" in str(err.value)


@pytest.mark.asyncio
async def test_upload_file_streams_file_to_s3(tmp_path: Path, fake_s3_client: FakeS3Client):
    source = tmp_path / "report.xlsx"
    source.write_bytes(b"fake-xlsx")
    provider = S3ExportStorageProvider(bucket="exports", region="us-east-1")

    key = await provider.upload_file("reports/file.xlsx", source, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    assert key == "reports/file.xlsx"
    assert fake_s3_client.upload_fileobj_calls == [
        {
            "Bucket": "exports",
            "Key": "reports/file.xlsx",
            "ExtraArgs": {
                "ContentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            },
            "Body": b"fake-xlsx",
        }
    ]


@pytest.mark.asyncio
async def test_upload_file_raises_http_exception_when_s3_fails(tmp_path: Path, fake_s3_client: FakeS3Client):
    source = tmp_path / "report.xlsx"
    source.write_bytes(b"fake-xlsx")
    fake_s3_client.upload_fileobj_error = FakeClientError("boom")
    provider = S3ExportStorageProvider(bucket="exports", region="us-east-1")

    with pytest.raises(StorageUploadError) as err:
        await provider.upload_file("reports/file.xlsx", source)

    assert "No fue posible subir el archivo a S3" in str(err.value)


@pytest.mark.asyncio
async def test_generate_presigned_url_uses_s3_client(fake_s3_client: FakeS3Client):
    provider = S3ExportStorageProvider(bucket="exports", region="us-east-1")

    url = await provider.generate_presigned_url("reports/file.xlsx", expires_in=120)

    assert url == "https://example.com/presigned"
    assert fake_s3_client.presign_calls == [
        {
            "operation_name": "get_object",
            "Params": {"Bucket": "exports", "Key": "reports/file.xlsx"},
            "ExpiresIn": 120,
        }
    ]


@pytest.mark.asyncio
async def test_generate_presigned_url_raises_http_exception_when_s3_fails(fake_s3_client: FakeS3Client):
    fake_s3_client.presign_error = FakeClientError("boom")
    provider = S3ExportStorageProvider(bucket="exports", region="us-east-1")

    with pytest.raises(StoragePresignError) as err:
        await provider.generate_presigned_url("reports/file.xlsx")

    assert "No fue posible generar la URL prefirmada" in str(err.value)
