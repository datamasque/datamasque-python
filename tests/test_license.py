"""Tests for `LicenseClient`."""

import uuid
from io import StringIO
from pathlib import Path
from unittest.mock import mock_open, patch

import requests_mock

from datamasque.client.models.license import LicenseInfo


def test_upload_license_file(client):
    with patch("datamasque.client.base.open", mock_open(read_data=b"license content")) as m_open:
        with requests_mock.Mocker() as m_request:
            m_request.post("http://test-server/api/license-upload/", status_code=200)
            client.upload_license_file(Path("path/to/test_license_file"))

    m_open.assert_called_once_with(Path("path/to/test_license_file"), "rb")
    assert "Content-Type: application/octet-stream" in m_request.request_history[0].text
    assert "license content" in m_request.request_history[0].text


def test_upload_license_file_stringio(client):
    with requests_mock.Mocker() as m_request:
        m_request.post("http://test-server/api/license-upload/", status_code=200)
        client.upload_license_file(StringIO("license content"))

    assert "Content-Type: application/octet-stream" in m_request.request_history[0].text
    assert "license content" in m_request.request_history[0].text


def test_get_current_license_info(client):
    license_data = {
        "uuid": str(uuid.uuid4()),
        "name": "Test License",
        "type": "enterprise",
        "is_expired": False,
        "uploadable": True,
    }
    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/license/", json=license_data, status_code=200)
        result = client.get_current_license_info()

    assert isinstance(result, LicenseInfo)
    assert result.uuid == license_data["uuid"]
    assert result.name == "Test License"
