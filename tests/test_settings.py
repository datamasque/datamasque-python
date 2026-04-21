"""Tests for `SettingsClient` (admin install bootstrap, application logs, locality)."""

import pytest
import requests_mock

from datamasque.client import DataMasqueClient
from datamasque.client.exceptions import DataMasqueApiError, DataMasqueUserError
from datamasque.client.models.dm_instance import DataMasqueInstanceConfig
from tests.helpers import fake


def test_admin_install_basic(client):
    with requests_mock.Mocker() as m:
        mock_email = fake.email()
        m.post(
            "http://test-server/api/users/admin-install/",
            json={"id": 1, "email": mock_email, "username": "admin"},
            status_code=201,
        )
        client.admin_install(email=mock_email)

        request_data = m.last_request.json()
        assert request_data["email"] == mock_email
        assert request_data["username"] == "admin"
        assert request_data["password"] == "test_password"
        assert request_data["re_password"] == "test_password"
        assert request_data["allowed_hosts"] == [
            "localhost",
            "127.0.0.1",
            "test-server",
        ]


def test_admin_install_overrides(client):
    with requests_mock.Mocker() as m:
        mock_email = fake.email()
        mock_username = fake.user_name()
        mock_password = fake.password()
        mock_hostname = fake.hostname()
        m.post(
            "http://test-server/api/users/admin-install/",
            json={"id": 1, "email": mock_email, "username": mock_username},
            status_code=201,
        )
        client.admin_install(
            email=mock_email,
            username=mock_username,
            password=mock_password,
            allowed_hosts=[mock_hostname],
        )

        request_data = m.last_request.json()
        assert request_data["email"] == mock_email
        assert request_data["username"] == mock_username
        assert request_data["password"] == mock_password
        assert request_data["re_password"] == mock_password
        assert request_data["allowed_hosts"] == [mock_hostname]


def test_admin_install_fail(client):
    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/users/admin-install/", status_code=400)
        with pytest.raises(DataMasqueApiError):
            client.admin_install(
                email=fake.email(),
                username=fake.user_name(),
                password=fake.password(),
                allowed_hosts=[fake.hostname()],
            )


def test_admin_install_requires_password_when_client_uses_token_source():
    """`admin_install` cannot fall back to `self.password` when the client was constructed with `token_source`."""
    config = DataMasqueInstanceConfig(
        base_url="http://test-server",
        username="admin",
        token_source=lambda: "token",
    )
    client = DataMasqueClient(config)

    with pytest.raises(DataMasqueUserError, match="`admin_install` requires a `password` argument"):
        client.admin_install(email=fake.email())


def test_retrieve_application_logs_writes_streamed_response_to_file(client, tmp_path):
    """Verifies the full streamed response body ends up in the output file, across multiple `iter_content` chunks."""
    # Content spans multiple 4096-byte chunks so the write-loop actually runs more than once.
    content = b"a" * 4096 + b"b" * 4096 + b"c" * 1000
    output = tmp_path / "logs.tar.gz"
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/logs/download/",
            content=content,
            status_code=200,
        )
        client.retrieve_application_logs(output)

    assert m.last_request.qs == {"log_service": ["application"]}
    assert output.read_bytes() == content


def test_set_locality_sends_patch(client):
    with requests_mock.Mocker() as m:
        m.patch("http://test-server/api/settings/", status_code=200)
        client.set_locality("en_GB")

    assert m.last_request.json() == {"locality": "en_GB"}
