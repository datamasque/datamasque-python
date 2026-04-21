"""Tests for `FileClient` (upload, list, get-by-name, delete)."""

import uuid
from datetime import datetime, timezone
from io import BytesIO, StringIO
from pathlib import Path
from unittest.mock import mock_open, patch

import pytest
import requests_mock

from datamasque.client import DataMasqueClient
from datamasque.client.models.files import OracleWalletFile, SeedFile, SnowflakeKeyFile, SslZipFile
from tests.helpers import fake, parse_multipart_form


@pytest.mark.parametrize(
    "source_factory",
    [
        pytest.param(lambda: BytesIO(b"this is my file content"), id="BytesIO"),
        pytest.param(lambda: b"this is my file content", id="bytes"),
        pytest.param(lambda: StringIO("this is my file content"), id="StringIO"),
        pytest.param(lambda: "file.txt", id="str-path"),
        pytest.param(lambda: Path("file.txt"), id="Path"),
    ],
)
@pytest.mark.parametrize(
    "file_type",
    [SeedFile, OracleWalletFile, SslZipFile, SnowflakeKeyFile],
)
def test_upload_file(client, file_type, source_factory):
    source = source_factory()
    name_of_file = fake.word()
    with patch(
        "datamasque.client.base.open",
        mock_open(read_data=b"this is my file content"),
    ) as m_open:
        mock_return_id = str(uuid.uuid4())
        mock_return_created_date = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_return_modified_date = datetime(2024, 2, 1, 12, 0, 0, tzinfo=timezone.utc)
        with requests_mock.Mocker() as m_request:
            m_request.post(
                f"http://test-server/{file_type.get_url()}",
                status_code=201,
                json={
                    "id": mock_return_id,
                    "name": name_of_file,
                    "created_date": mock_return_created_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    "modified_date": mock_return_modified_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                },
            )
            file = client.upload_file(file_type, name_of_file, source)

        if isinstance(source, (str, Path)):
            m_open.assert_called_once_with(source, "rb")
        else:
            m_open.assert_not_called()

        assert "this is my file content" in m_request.request_history[0].text
        assert isinstance(file, file_type)
        assert file.name == name_of_file
        assert file.id == mock_return_id
        assert file.created_date == mock_return_created_date
        assert file.modified_date == mock_return_modified_date


@pytest.mark.parametrize(
    "file_type",
    [SeedFile, OracleWalletFile, SslZipFile, SnowflakeKeyFile],
)
def test_get_files_by_type(client, file_type):
    id_1 = str(uuid.uuid4())
    id_2 = str(uuid.uuid4())
    mock_return_created_date_1 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    mock_return_modified_date_1 = datetime(2024, 2, 1, 12, 0, 0, tzinfo=timezone.utc)
    mock_return_created_date_2 = datetime(2024, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
    mock_return_modified_date_2 = datetime(2024, 4, 1, 12, 0, 0, tzinfo=timezone.utc)

    with requests_mock.Mocker() as m_request:
        m_request.get(
            f"http://test-server/{file_type.get_url()}",
            status_code=201,
            json=[
                {
                    "id": id_1,
                    "name": "file1",
                    "created_date": mock_return_created_date_1.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    "modified_date": mock_return_modified_date_1.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                },
                {
                    "id": id_2,
                    "name": "file2",
                    "created_date": mock_return_created_date_2.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    "modified_date": mock_return_modified_date_2.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                },
            ],
        )
        results = client.list_files_of_type(file_type)

    assert len(results) == 2
    assert isinstance(results[0], file_type)
    assert results[0].id == id_1
    assert results[0].name == "file1"
    assert results[0].created_date == mock_return_created_date_1
    assert results[0].modified_date == mock_return_modified_date_1
    assert isinstance(results[1], file_type)
    assert results[1].id == id_2
    assert results[1].name == "file2"
    assert results[1].created_date == mock_return_created_date_2
    assert results[1].modified_date == mock_return_modified_date_2


@pytest.mark.parametrize(
    "file_type",
    [SeedFile, OracleWalletFile, SslZipFile, SnowflakeKeyFile],
)
def test_get_files_by_type_and_name(client, file_type):
    id_1 = str(uuid.uuid4())
    id_2 = str(uuid.uuid4())
    mock_return_created_date_1 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    mock_return_modified_date_1 = datetime(2024, 2, 1, 12, 0, 0, tzinfo=timezone.utc)
    mock_return_created_date_2 = datetime(2024, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
    mock_return_modified_date_2 = datetime(2024, 4, 1, 12, 0, 0, tzinfo=timezone.utc)

    with requests_mock.Mocker() as m_request:
        m_request.get(
            f"http://test-server/{file_type.get_url()}",
            status_code=201,
            json=[
                {
                    "id": id_1,
                    "name": "file1",
                    "created_date": mock_return_created_date_1.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    "modified_date": mock_return_modified_date_1.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                },
                {
                    "id": id_2,
                    "name": "file2",
                    "created_date": mock_return_created_date_2.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    "modified_date": mock_return_modified_date_2.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                },
            ],
        )
        result = client.get_file_of_type_by_name(file_type, "file2")

    assert isinstance(result, file_type)
    assert result.id == id_2


@pytest.mark.parametrize(
    "file_exists",
    [True, False],
)
@pytest.mark.parametrize(
    "file_type",
    [SeedFile, OracleWalletFile, SslZipFile, SnowflakeKeyFile],
)
def test_delete_file_if_exists(client, file_type, file_exists):
    file_id = str(uuid.uuid4())
    file_name = fake.word()
    file_to_delete = file_type(
        id=file_id,
        name=file_name,
        created_date=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        modified_date=datetime(2024, 2, 1, 12, 0, 0, tzinfo=timezone.utc),
    )
    with requests_mock.Mocker() as m_request:
        m_request.delete(
            f"http://test-server/{file_type.get_url()}{file_id}/",
            status_code=204 if file_exists else 404,
        )
        client.delete_file_if_exists(file_to_delete)  # shouldn't raise an error


def test_delete_file_if_exists_raises_when_id_not_set(client):
    """`delete_file_if_exists` requires a file object that has been persisted on the server."""
    unpersisted_file = SeedFile(
        id=None,  # type: ignore[arg-type]
        name="never_uploaded",
        created_date=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        modified_date=None,
    )

    with pytest.raises(ValueError, match="File has not yet been created"):
        client.delete_file_if_exists(unpersisted_file)


@pytest.mark.parametrize("file_type", [SeedFile, OracleWalletFile, SslZipFile, SnowflakeKeyFile])
def test_upload_file_if_not_exists_skips_when_same_name_exists(client, file_type):
    """Returns `None` and does not POST when a file of this type already has the same name."""
    with patch("datamasque.client.base.open", mock_open(read_data=b"content")):
        with requests_mock.Mocker() as m_request:
            m_request.get(
                f"http://test-server/{file_type.get_url()}",
                status_code=200,
                json=[
                    {
                        "id": str(uuid.uuid4()),
                        "name": "already_here.csv",
                        "created_date": "2024-01-01T00:00:00.000000Z",
                        "modified_date": "2024-01-01T00:00:00.000000Z",
                    },
                ],
            )
            result = client.upload_file_if_not_exists(file_type, "already_here.csv")

            assert result is None
            # Only the list-by-type GET should have fired — no POST.
            assert m_request.call_count == 1
            assert m_request.request_history[0].method == "GET"


@pytest.mark.parametrize("file_type", [SeedFile, OracleWalletFile, SslZipFile, SnowflakeKeyFile])
def test_upload_file_if_not_exists_uploads_when_missing(client, file_type):
    """Uploads and returns the new file object when no existing file of the same name is present."""
    new_id = str(uuid.uuid4())
    with patch("datamasque.client.base.open", mock_open(read_data=b"new content")) as m_open:
        with requests_mock.Mocker() as m_request:
            m_request.get(
                f"http://test-server/{file_type.get_url()}",
                status_code=200,
                json=[],
            )
            m_request.post(
                f"http://test-server/{file_type.get_url()}",
                status_code=201,
                json={
                    "id": new_id,
                    "name": "new_file.csv",
                    "created_date": "2024-01-01T00:00:00.000000Z",
                    "modified_date": "2024-01-01T00:00:00.000000Z",
                },
            )
            result = client.upload_file_if_not_exists(file_type, Path("new_file.csv"))

            assert isinstance(result, file_type)
            assert result.id == new_id
            m_open.assert_called_once_with(Path("new_file.csv"), "rb")
            # List first, then upload.
            assert [r.method for r in m_request.request_history] == ["GET", "POST"]


def test_upload_file_retries_on_401(config):
    """File content must be resent on retry after a 401 re-auth, not just an empty body."""
    with patch.object(DataMasqueClient, "authenticate"):
        client = DataMasqueClient(config)

        file_content = BytesIO(b"seed content")
        file_content.seek(0)

        with requests_mock.Mocker() as m_request:
            m_request.post(
                f"http://test-server/{SeedFile.get_url()}",
                [
                    {"status_code": 401},
                    {
                        "status_code": 201,
                        "json": {
                            "id": str(uuid.uuid4()),
                            "name": "seed.csv",
                            "created_date": "2024-01-01T00:00:00.000000Z",
                            "modified_date": "2024-01-01T00:00:00.000000Z",
                        },
                    },
                ],
            )
            client.upload_file(SeedFile, "seed.csv", file_content)

            assert m_request.call_count == 2
            first_form = parse_multipart_form(m_request.request_history[0])
            second_form = parse_multipart_form(m_request.request_history[1])
            assert first_form["seed_file"]["content"] == b"seed content"
            assert second_form["seed_file"]["content"] == b"seed content"
