"""Tests for remote provider support in the DataMasque client."""

import json
from datetime import datetime, timezone
from types import MappingProxyType

import pytest
import requests_mock

from datamasque.client import (
    CertificateFile,
    CertificateVerification,
    DataMasqueClient,
    FileId,
    RemoteProvider,
    RemoteProviderAuthType,
    RemoteProviderId,
    RemoteProviderItemValue,
    RemoteProviderMethod,
    RemoteProviderParameter,
    RemoteProviderTestResult,
    TokenBodyFormat,
)
from datamasque.client.exceptions import DataMasqueApiError

REMOTE_PROVIDERS_URL = "http://test-server/api/remote-providers/"
VALUE_PATHS = [["body", "values", "*"]]
TOKENS_RESPONSE_PATH = ["tokens", "*"]
PROVIDER_ID_1 = "aaaaaaaa-1111-2222-3333-444444444444"
PROVIDER_ID_2 = "bbbbbbbb-1111-2222-3333-444444444444"
CERTIFICATE_ID = "cccccccc-1111-2222-3333-444444444444"
CLIENT_SECRET = "the-client-secret"
UPLOADED_AT = datetime(2026, 11, 1, tzinfo=timezone.utc)


@pytest.fixture
def tokens_provider_json() -> dict[str, object]:
    """A tokenisation service provider, as the server returns it."""
    return {
        "id": PROVIDER_ID_1,
        "name": "tokenisation_service",
        "base_url": "https://tokens.example.com",
        "certificate_verification": "public_cas",
        "certificate_file": None,
        "timeout_seconds": 30,
        "auth_type": "oauth2_client_credentials",
        "token_url": "/api/v1/auth/token",
        "token_body_format": "form",
        "client_id": "datamasque",
        "token_field_path": ["access_token"],
        "header_name": "",
        "has_secret": True,
        "method": "POST",
        "path": "/api/v1/protect",
        "headers": {},
        "query": {},
        "body": {},
        "value_paths": VALUE_PATHS,
        "item_values": [],
        "parameters": [{"name": "crypt_id", "required": True, "paths": [["body", "cryptId"]]}],
        "response_value_path": ["protectedValues", "*"],
        "max_values_per_request": 0,
        "max_request_bytes": 0,
        "max_concurrent_requests": 4,
        "created_date": "2026-11-23T12:00:00Z",
        "modified_date": "2026-11-23T12:30:00Z",
    }


@pytest.fixture
def plain_http_provider_json() -> dict[str, object]:
    """A provider with an `http` base URL and no authentication, as the server returns it."""
    return {
        "id": PROVIDER_ID_2,
        "name": "plain_http",
        "base_url": "http://tokenizer.internal:8080",
        "certificate_verification": None,
        "certificate_file": None,
        "timeout_seconds": 10,
        "auth_type": "none",
        "token_url": "",
        "token_body_format": "",
        "client_id": "",
        "token_field_path": None,
        "header_name": "",
        "has_secret": False,
        "method": "PUT",
        "path": "/tokenize",
        "headers": {"Accept": "application/json"},
        "query": {},
        "body": {"format": "text"},
        "value_paths": VALUE_PATHS,
        "item_values": [],
        "parameters": [],
        "response_value_path": TOKENS_RESPONSE_PATH,
        "max_values_per_request": 1000,
        "max_request_bytes": 1048576,
        "max_concurrent_requests": 4,
        "created_date": "2026-11-23T12:00:00Z",
        "modified_date": "2026-11-23T12:00:00Z",
    }


@pytest.fixture
def tokens_provider() -> RemoteProvider:
    """A tokenisation service provider, as an admin enters it."""
    return RemoteProvider(
        name="tokenisation_service",
        base_url="https://tokens.example.com",
        certificate_verification=CertificateVerification.public_cas,
        auth_type=RemoteProviderAuthType.oauth2_client_credentials,
        token_url="/api/v1/auth/token",
        token_body_format=TokenBodyFormat.form,
        client_id="datamasque",
        secret=CLIENT_SECRET,
        path="/api/v1/protect",
        value_paths=VALUE_PATHS,
        parameters=[RemoteProviderParameter(name="crypt_id", required=True, paths=[["body", "cryptId"]])],
        response_value_path=["protectedValues", "*"],
        max_values_per_request=0,
        max_request_bytes=0,
    )


def test_defaults_match_the_servers_defaults() -> None:
    provider = RemoteProvider(
        name="minimal",
        base_url="https://tokenizer.example.com",
        path="/tokenize",
        value_paths=VALUE_PATHS,
        response_value_path=TOKENS_RESPONSE_PATH,
    )

    assert provider.certificate_verification is CertificateVerification.uploaded_root_ca
    assert provider.auth_type is RemoteProviderAuthType.none
    assert provider.method is RemoteProviderMethod.POST
    assert provider.timeout_seconds == 30
    assert provider.max_values_per_request == 1000
    assert provider.max_request_bytes == 1048576
    assert provider.max_concurrent_requests == 4
    assert provider.headers == {}
    assert provider.query == {}
    assert provider.body == {}
    assert provider.item_values == []
    assert provider.parameters == []
    assert provider.has_secret is False


def test_secret_is_not_in_the_repr(tokens_provider: RemoteProvider) -> None:
    assert CLIENT_SECRET not in repr(tokens_provider)


def test_certificate_file_accepts_a_certificate_file() -> None:
    certificate = CertificateFile(id=FileId(CERTIFICATE_ID), name="internal-ca.pem", created_date=UPLOADED_AT)

    provider = RemoteProvider(
        name="internal",
        base_url="https://tokenizer.internal",
        certificate_file=certificate,
        path="/tokenize",
        value_paths=VALUE_PATHS,
        response_value_path=TOKENS_RESPONSE_PATH,
    )

    assert provider.certificate_file == FileId(CERTIFICATE_ID)


def test_certificate_file_without_an_id_is_refused() -> None:
    with pytest.raises(ValueError, match="id is None"):
        RemoteProvider(
            name="internal",
            base_url="https://tokenizer.internal",
            certificate_file=CertificateFile(name="internal-ca.pem", created_date=UPLOADED_AT),
            path="/tokenize",
            value_paths=VALUE_PATHS,
            response_value_path=TOKENS_RESPONSE_PATH,
        )


def test_list_remote_providers(
    client: DataMasqueClient,
    tokens_provider_json: dict[str, object],
    plain_http_provider_json: dict[str, object],
) -> None:
    with requests_mock.Mocker() as m:
        m.get(REMOTE_PROVIDERS_URL, json=[tokens_provider_json, plain_http_provider_json])

        providers = client.list_remote_providers()

    assert [provider.name for provider in providers] == ["tokenisation_service", "plain_http"]
    tokens, plain = providers
    assert tokens.id == RemoteProviderId(PROVIDER_ID_1)
    assert tokens.auth_type is RemoteProviderAuthType.oauth2_client_credentials
    assert tokens.token_body_format is TokenBodyFormat.form
    assert tokens.parameters == [RemoteProviderParameter(name="crypt_id", required=True, paths=[["body", "cryptId"]])]
    assert tokens.has_secret is True
    assert tokens.secret is None
    assert tokens.created_date == datetime(2026, 11, 23, 12, 0, tzinfo=timezone.utc)
    assert plain.certificate_verification is None
    assert plain.token_body_format is None
    assert plain.method is RemoteProviderMethod.PUT
    assert plain.has_secret is False


def test_get_remote_provider(client: DataMasqueClient, tokens_provider_json: dict[str, object]) -> None:
    with requests_mock.Mocker() as m:
        m.get(f"{REMOTE_PROVIDERS_URL}{PROVIDER_ID_1}/", json=tokens_provider_json)

        provider = client.get_remote_provider(RemoteProviderId(PROVIDER_ID_1))

    assert provider.name == "tokenisation_service"
    assert provider.max_concurrent_requests == 4


def test_get_remote_provider_by_name(
    client: DataMasqueClient,
    tokens_provider_json: dict[str, object],
    plain_http_provider_json: dict[str, object],
) -> None:
    with requests_mock.Mocker() as m:
        m.get(REMOTE_PROVIDERS_URL, json=[tokens_provider_json, plain_http_provider_json])

        found = client.get_remote_provider_by_name("plain_http")
        missing = client.get_remote_provider_by_name("no_such_provider")

    assert found is not None
    assert found.id == RemoteProviderId(PROVIDER_ID_2)
    assert missing is None


def test_create_remote_provider_sends_the_secret_and_returns_the_saved_provider(
    client: DataMasqueClient, tokens_provider: RemoteProvider, tokens_provider_json: dict[str, object]
) -> None:
    with requests_mock.Mocker() as m:
        m.post(REMOTE_PROVIDERS_URL, json=tokens_provider_json, status_code=201)

        created = client.create_remote_provider(tokens_provider)

        sent = m.last_request.json()

    assert sent["secret"] == CLIENT_SECRET
    assert sent["name"] == "tokenisation_service"
    assert sent["certificate_verification"] == "public_cas"
    assert sent["auth_type"] == "oauth2_client_credentials"
    assert sent["token_body_format"] == "form"
    assert sent["parameters"] == [{"name": "crypt_id", "required": True, "paths": [["body", "cryptId"]]}]
    assert sent["value_paths"] == VALUE_PATHS
    for read_only in ("id", "has_secret", "created_date", "modified_date"):
        assert read_only not in sent

    assert created.id == RemoteProviderId(PROVIDER_ID_1)
    assert created.has_secret is True
    assert created.secret is None


def test_create_remote_provider_sends_a_blank_token_body_format_for_no_oauth2(
    client: DataMasqueClient, plain_http_provider_json: dict[str, object]
) -> None:
    provider = RemoteProvider(
        name="plain_http",
        base_url="http://tokenizer.internal:8080",
        certificate_verification=None,
        path="/tokenize",
        value_paths=VALUE_PATHS,
        response_value_path=TOKENS_RESPONSE_PATH,
    )

    with requests_mock.Mocker() as m:
        m.post(REMOTE_PROVIDERS_URL, json=plain_http_provider_json, status_code=201)

        client.create_remote_provider(provider)

        sent = m.last_request.json()

    assert sent["token_body_format"] == ""
    assert sent["certificate_verification"] is None
    assert "secret" not in sent


def test_create_remote_provider_round_trips_fixed_query_pairs_and_item_values(
    client: DataMasqueClient, plain_http_provider_json: dict[str, object]
) -> None:
    query = {"tenant": "claims"}
    item_values_json = [
        {"path": ["body", "items", "*", "type"], "value": "text"},
        {"path": ["body", "items", "*", "options"], "value": {"keep_case": True}},
    ]
    provider = RemoteProvider(
        name="plain_http",
        base_url="http://tokenizer.internal:8080",
        certificate_verification=None,
        path="/tokenize",
        query=query,
        value_paths=[["body", "items", "*", "data"]],
        item_values=[
            RemoteProviderItemValue(path=["body", "items", "*", "type"], value="text"),
            RemoteProviderItemValue(path=["body", "items", "*", "options"], value={"keep_case": True}),
        ],
        response_value_path=TOKENS_RESPONSE_PATH,
    )
    saved_json = {
        **plain_http_provider_json,
        "query": query,
        "value_paths": [["body", "items", "*", "data"]],
        "item_values": item_values_json,
    }

    with requests_mock.Mocker() as m:
        m.post(REMOTE_PROVIDERS_URL, json=saved_json, status_code=201)

        created = client.create_remote_provider(provider)

        sent = m.last_request.json()

    assert sent["query"] == query
    assert sent["item_values"] == item_values_json
    assert created.query == query
    assert created.item_values == provider.item_values


def test_create_remote_provider_raises_the_servers_field_errors(
    client: DataMasqueClient, tokens_provider: RemoteProvider
) -> None:
    errors = {"value_paths": ["A value path has exactly one `*`, where each value of a request goes."]}
    with requests_mock.Mocker() as m:
        m.post(REMOTE_PROVIDERS_URL, json=errors, status_code=400)

        with pytest.raises(DataMasqueApiError) as raised:
            client.create_remote_provider(tokens_provider)

    assert raised.value.response is not None
    assert raised.value.response.json() == errors


def test_update_remote_provider_without_a_secret_keeps_the_saved_one(
    client: DataMasqueClient, tokens_provider_json: dict[str, object]
) -> None:
    with requests_mock.Mocker() as m:
        m.get(f"{REMOTE_PROVIDERS_URL}{PROVIDER_ID_1}/", json=tokens_provider_json)
        m.put(
            f"{REMOTE_PROVIDERS_URL}{PROVIDER_ID_1}/",
            json={**tokens_provider_json, "timeout_seconds": 60, "modified_date": "2026-11-23T13:00:00Z"},
        )

        provider = client.get_remote_provider(RemoteProviderId(PROVIDER_ID_1))
        provider.timeout_seconds = 60
        updated = client.update_remote_provider(provider)

        sent = m.last_request.json()

    assert m.last_request.method == "PUT"
    assert sent["timeout_seconds"] == 60
    assert "secret" not in sent
    assert updated.timeout_seconds == 60
    assert updated.modified_date == datetime(2026, 11, 23, 13, 0, tzinfo=timezone.utc)


def test_update_remote_provider_sends_a_new_secret(
    client: DataMasqueClient, tokens_provider: RemoteProvider, tokens_provider_json: dict[str, object]
) -> None:
    tokens_provider.id = RemoteProviderId(PROVIDER_ID_1)
    with requests_mock.Mocker() as m:
        m.put(f"{REMOTE_PROVIDERS_URL}{PROVIDER_ID_1}/", json=tokens_provider_json)

        client.update_remote_provider(tokens_provider)

        sent = m.last_request.json()

    assert sent["secret"] == CLIENT_SECRET


def test_update_remote_provider_needs_an_id(client: DataMasqueClient, tokens_provider: RemoteProvider) -> None:
    with pytest.raises(ValueError, match="has not been created yet"):
        client.update_remote_provider(tokens_provider)


def test_create_or_update_remote_provider_creates_a_new_one(
    client: DataMasqueClient, tokens_provider: RemoteProvider, tokens_provider_json: dict[str, object]
) -> None:
    with requests_mock.Mocker() as m:
        m.get(REMOTE_PROVIDERS_URL, json=[])
        m.post(REMOTE_PROVIDERS_URL, json=tokens_provider_json, status_code=201)

        saved = client.create_or_update_remote_provider(tokens_provider)

    assert m.last_request.method == "POST"
    assert saved.id == RemoteProviderId(PROVIDER_ID_1)


def test_create_or_update_remote_provider_updates_the_one_with_the_same_name(
    client: DataMasqueClient, tokens_provider: RemoteProvider, tokens_provider_json: dict[str, object]
) -> None:
    with requests_mock.Mocker() as m:
        m.get(REMOTE_PROVIDERS_URL, json=[tokens_provider_json])
        m.put(f"{REMOTE_PROVIDERS_URL}{PROVIDER_ID_1}/", json=tokens_provider_json)

        saved = client.create_or_update_remote_provider(tokens_provider)

        sent = m.last_request.json()

    assert m.last_request.method == "PUT"
    assert sent["secret"] == CLIENT_SECRET
    assert saved.id == RemoteProviderId(PROVIDER_ID_1)
    assert tokens_provider.id == RemoteProviderId(PROVIDER_ID_1)


def test_delete_remote_provider_by_id_if_exists(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.delete(f"{REMOTE_PROVIDERS_URL}{PROVIDER_ID_1}/", status_code=204)
        m.delete(f"{REMOTE_PROVIDERS_URL}{PROVIDER_ID_2}/", status_code=404)

        client.delete_remote_provider_by_id_if_exists(RemoteProviderId(PROVIDER_ID_1))
        client.delete_remote_provider_by_id_if_exists(RemoteProviderId(PROVIDER_ID_2))

        assert [request.method for request in m.request_history] == ["DELETE", "DELETE"]


def test_delete_remote_provider_by_name_if_exists(
    client: DataMasqueClient, tokens_provider_json: dict[str, object]
) -> None:
    with requests_mock.Mocker() as m:
        m.get(REMOTE_PROVIDERS_URL, json=[tokens_provider_json])
        m.delete(f"{REMOTE_PROVIDERS_URL}{PROVIDER_ID_1}/", status_code=204)

        client.delete_remote_provider_by_name_if_exists("tokenisation_service")
        client.delete_remote_provider_by_name_if_exists("no_such_provider")

        deletes = [request for request in m.request_history if request.method == "DELETE"]

    assert [request.url for request in deletes] == [f"{REMOTE_PROVIDERS_URL}{PROVIDER_ID_1}/"]


def test_test_remote_provider_returns_the_output(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.post(f"{REMOTE_PROVIDERS_URL}{PROVIDER_ID_1}/test/", json={"output": "SSN123-45-6789", "error": None})

        result = client.test_remote_provider(
            RemoteProviderId(PROVIDER_ID_1), "123-45-6789", MappingProxyType({"crypt_id": "SSN"})
        )

        sent = m.last_request.json()

    assert sent == {"value": "123-45-6789", "params": {"crypt_id": "SSN"}}
    assert result == RemoteProviderTestResult(output="SSN123-45-6789", error=None)


def test_test_remote_provider_returns_the_error(client: DataMasqueClient) -> None:
    error = "The provider answered with HTTP status 503."
    with requests_mock.Mocker() as m:
        m.post(f"{REMOTE_PROVIDERS_URL}{PROVIDER_ID_2}/test/", json={"output": None, "error": error})

        result = client.test_remote_provider(RemoteProviderId(PROVIDER_ID_2), "123-45-6789")

        sent = m.last_request.json()

    assert sent == {"value": "123-45-6789", "params": {}}
    assert result.output is None
    assert result.error == error


def test_a_refused_test_does_not_log_the_value_or_the_params(
    client: DataMasqueClient, caplog: pytest.LogCaptureFixture
) -> None:
    with requests_mock.Mocker() as m:
        m.post(f"{REMOTE_PROVIDERS_URL}{PROVIDER_ID_1}/test/", json={"params": ["Unknown parameter."]}, status_code=400)

        with pytest.raises(DataMasqueApiError):
            client.test_remote_provider(
                RemoteProviderId(PROVIDER_ID_1), "123-45-6789", MappingProxyType({"crypt_id": "SSN-CRYPT"})
            )

    logged = caplog.text + json.dumps([record.args for record in caplog.records], default=str)
    assert "123-45-6789" not in logged
    assert "SSN-CRYPT" not in logged


def test_a_refused_create_does_not_log_the_secret(
    client: DataMasqueClient, tokens_provider: RemoteProvider, caplog: pytest.LogCaptureFixture
) -> None:
    with requests_mock.Mocker() as m:
        m.post(REMOTE_PROVIDERS_URL, json={"secret": ["Enter the secret."]}, status_code=400)

        with pytest.raises(DataMasqueApiError):
            client.create_remote_provider(tokens_provider)

    assert CLIENT_SECRET not in caplog.text
    assert CLIENT_SECRET not in json.dumps([record.args for record in caplog.records], default=str)
