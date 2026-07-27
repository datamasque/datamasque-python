"""Tests for discovery config library support in the DataMasque client."""

import logging
from datetime import datetime
from typing import Any, Optional

import pytest
import requests
import requests_mock
from pydantic import ValidationError

from datamasque.client import (
    DataMasqueClient,
    DiscoveryConfigLibrary,
    DiscoveryConfigLibraryId,
)
from datamasque.client.exceptions import DataMasqueApiError, DataMasqueArgumentError
from datamasque.client.models.status import ValidationStatus

LIBRARY_ID_1 = "aaaaaaaa-1111-2222-3333-444444444444"
LIBRARY_ID_2 = "bbbbbbbb-1111-2222-3333-444444444444"


@pytest.fixture
def sample_library_list_response() -> list[dict[str, Any]]:
    """List response: a bare (unpaginated) array of entries without `config_yaml`."""
    return [
        {
            "id": LIBRARY_ID_1,
            "name": "my_library",
            "namespace": "org",
            "is_valid": "valid",
            "validation_error": None,
            "usage_count": 0,
            "created": "2025-01-01T12:00:00Z",
            "modified": "2025-01-02T12:00:00Z",
        },
        {
            "id": LIBRARY_ID_2,
            "name": "another_library",
            "namespace": "",
            "is_valid": "invalid",
            "validation_error": "bad yaml",
            "usage_count": 2,
            "created": "2025-02-01T12:00:00Z",
            "modified": "2025-02-02T12:00:00Z",
        },
    ]


@pytest.fixture
def sample_library_detail_response() -> dict[str, Any]:
    """Detail response (with config_yaml)."""
    return {
        "id": LIBRARY_ID_1,
        "name": "my_library",
        "namespace": "org",
        "config_yaml": "labels: []\nmetadata_rules: []\nidd_rules: []\n",
        "is_valid": "valid",
        "validation_error": None,
        "usage_count": 0,
        "created": "2025-01-01T12:00:00Z",
        "modified": "2025-01-02T12:00:00Z",
    }


@pytest.fixture
def config_library() -> DiscoveryConfigLibrary:
    return DiscoveryConfigLibrary(
        name="test_library",
        namespace="test_ns",
        yaml="labels: []\nmetadata_rules: []\nidd_rules: []\n",
    )


def test_library_is_untyped() -> None:
    """A library carries no `config_type`; the same library serves both config types."""
    library = DiscoveryConfigLibrary(name="test_library", yaml="labels: []\n")
    assert "config_type" not in library.model_dump(by_alias=True)


def test_list_discovery_config_libraries(
    client: DataMasqueClient, sample_library_list_response: list[dict[str, Any]]
) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/discovery/config-libraries/",
            json=sample_library_list_response,
            status_code=200,
        )
        libraries = client.list_discovery_config_libraries()

    assert len(libraries) == 2
    assert libraries[0].id == DiscoveryConfigLibraryId(LIBRARY_ID_1)
    assert libraries[0].name == "my_library"
    assert libraries[0].namespace == "org"
    assert libraries[0].yaml is None
    assert libraries[0].is_valid is ValidationStatus.valid
    assert libraries[0].usage_count == 0
    assert libraries[1].id == DiscoveryConfigLibraryId(LIBRARY_ID_2)
    assert libraries[1].name == "another_library"
    assert libraries[1].is_valid is ValidationStatus.invalid
    assert libraries[1].validation_error == "bad yaml"
    assert libraries[1].usage_count == 2


def test_list_discovery_config_libraries_empty(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/discovery/config-libraries/",
            json=[],
            status_code=200,
        )
        libraries = client.list_discovery_config_libraries()

    assert libraries == []


def test_get_discovery_config_library(client: DataMasqueClient, sample_library_detail_response: dict[str, Any]) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            f"http://test-server/api/discovery/config-libraries/{LIBRARY_ID_1}/",
            json=sample_library_detail_response,
            status_code=200,
        )
        library = client.get_discovery_config_library(DiscoveryConfigLibraryId(LIBRARY_ID_1))

    assert library.id == DiscoveryConfigLibraryId(LIBRARY_ID_1)
    assert library.name == "my_library"
    assert library.namespace == "org"
    assert library.yaml == "labels: []\nmetadata_rules: []\nidd_rules: []\n"
    assert library.is_valid is ValidationStatus.valid
    assert library.usage_count == 0


def test_get_discovery_config_library_by_name_found(
    client: DataMasqueClient,
    sample_library_list_response: list[dict[str, Any]],
    sample_library_detail_response: dict[str, Any],
) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/discovery/config-libraries/",
            json=[sample_library_list_response[0]],
            status_code=200,
        )
        m.get(
            f"http://test-server/api/discovery/config-libraries/{LIBRARY_ID_1}/",
            json=sample_library_detail_response,
            status_code=200,
        )
        library = client.get_discovery_config_library_by_name("my_library", "org")

    assert library is not None
    assert library.name == "my_library"
    assert library.yaml == "labels: []\nmetadata_rules: []\nidd_rules: []\n"
    assert "name_exact=my_library" in m.request_history[0].url


def test_get_discovery_config_library_by_name_matches_namespace_client_side(
    client: DataMasqueClient, sample_library_detail_response: dict[str, Any]
) -> None:
    """The server's `namespace_exact` filter ignores empty strings, so namespace matching happens client-side."""
    same_name_two_namespaces = [
        {
            "id": LIBRARY_ID_1,
            "name": "my_library",
            "namespace": "org",
            "is_valid": "valid",
            "created": "2025-01-01T12:00:00Z",
            "modified": "2025-01-02T12:00:00Z",
        },
        {
            "id": LIBRARY_ID_2,
            "name": "my_library",
            "namespace": "",
            "is_valid": "valid",
            "created": "2025-01-01T12:00:00Z",
            "modified": "2025-01-02T12:00:00Z",
        },
    ]

    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/discovery/config-libraries/",
            json=same_name_two_namespaces,
            status_code=200,
        )
        m.get(
            f"http://test-server/api/discovery/config-libraries/{LIBRARY_ID_1}/",
            json=sample_library_detail_response,
            status_code=200,
        )
        library = client.get_discovery_config_library_by_name("my_library", "org")

    assert library is not None
    assert library.id == DiscoveryConfigLibraryId(LIBRARY_ID_1)


def test_get_discovery_config_library_by_name_default_namespace_does_not_match_other_namespaces(
    client: DataMasqueClient, sample_library_list_response: list[dict[str, Any]]
) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/discovery/config-libraries/",
            json=[sample_library_list_response[0]],
            status_code=200,
        )
        library = client.get_discovery_config_library_by_name("my_library")

    assert library is None
    assert m.call_count == 1


def test_get_discovery_config_library_by_name_not_found(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/discovery/config-libraries/",
            json=[],
            status_code=200,
        )
        library = client.get_discovery_config_library_by_name("nonexistent")

    assert library is None


def test_get_discovery_config_library_by_name_raises_when_server_omits_id(client: DataMasqueClient) -> None:
    """If the server returns a list entry without `id`, the by-name lookup surfaces a typed API error."""
    list_response_without_id = [
        {
            "name": "my_library",
            "namespace": "org",
            "is_valid": "valid",
            "created": "2025-01-01T12:00:00Z",
            "modified": "2025-01-02T12:00:00Z",
        },
    ]

    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/discovery/config-libraries/",
            json=list_response_without_id,
            status_code=200,
        )
        with pytest.raises(DataMasqueApiError, match="without an `id`"):
            client.get_discovery_config_library_by_name("my_library", "org")


def test_create_discovery_config_library(client: DataMasqueClient, config_library: DiscoveryConfigLibrary) -> None:
    create_response = {
        "id": LIBRARY_ID_1,
        "name": "test_library",
        "namespace": "test_ns",
        "config_yaml": "labels: []\nmetadata_rules: []\nidd_rules: []\n",
        "is_valid": "valid",
        "validation_error": None,
        "usage_count": 0,
        "created": "2025-06-01T10:00:00Z",
        "modified": "2025-06-01T10:00:00Z",
    }

    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/discovery/config-libraries/",
            json=create_response,
            status_code=201,
        )
        result = client.create_discovery_config_library(config_library)

    assert result is config_library
    assert result.id == DiscoveryConfigLibraryId(LIBRARY_ID_1)
    assert result.is_valid is ValidationStatus.valid
    assert result.usage_count == 0
    assert result.created == datetime.fromisoformat("2025-06-01T10:00:00+00:00")
    assert result.modified == datetime.fromisoformat("2025-06-01T10:00:00+00:00")

    request_body = m.last_request.json()
    assert request_body == {
        "name": "test_library",
        "namespace": "test_ns",
        "config_yaml": "labels: []\nmetadata_rules: []\nidd_rules: []\n",
    }


def test_create_discovery_config_library_reports_validation_error(
    client: DataMasqueClient, config_library: DiscoveryConfigLibrary
) -> None:
    create_response = {
        "id": LIBRARY_ID_1,
        "name": "test_library",
        "namespace": "test_ns",
        "config_yaml": "labels: []\nmetadata_rules: []\nidd_rules: []\n",
        "is_valid": "invalid",
        "validation_error": "metadata_rules is not a list",
        "created": "2025-06-01T10:00:00Z",
        "modified": "2025-06-01T10:00:00Z",
    }

    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/discovery/config-libraries/",
            json=create_response,
            status_code=201,
        )
        result = client.create_discovery_config_library(config_library)

    assert result.is_valid is ValidationStatus.invalid
    assert result.validation_error == "metadata_rules is not a list"


def test_update_discovery_config_library(client: DataMasqueClient, config_library: DiscoveryConfigLibrary) -> None:
    config_library.id = DiscoveryConfigLibraryId(LIBRARY_ID_1)
    config_library.validation_error = "stale error from a previous update"

    update_response = {
        "id": LIBRARY_ID_1,
        "name": "test_library",
        "namespace": "test_ns",
        "config_yaml": "labels: []\nmetadata_rules: []\nidd_rules: []\n",
        "is_valid": "valid",
        "validation_error": None,
        "usage_count": 3,
        "created": "2025-06-01T10:00:00Z",
        "modified": "2025-06-02T10:00:00Z",
    }

    with requests_mock.Mocker() as m:
        m.put(
            f"http://test-server/api/discovery/config-libraries/{LIBRARY_ID_1}/",
            json=update_response,
            status_code=200,
        )
        result = client.update_discovery_config_library(config_library)

    assert result is config_library
    assert result.is_valid is ValidationStatus.valid
    assert result.validation_error is None
    assert result.usage_count == 3
    assert result.modified == datetime.fromisoformat("2025-06-02T10:00:00+00:00")

    request_body = m.last_request.json()
    assert request_body == {
        "name": "test_library",
        "namespace": "test_ns",
        "config_yaml": "labels: []\nmetadata_rules: []\nidd_rules: []\n",
    }


def test_update_discovery_config_library_no_id_raises(
    client: DataMasqueClient, config_library: DiscoveryConfigLibrary
) -> None:
    with pytest.raises(DataMasqueArgumentError, match="id is None"):
        client.update_discovery_config_library(config_library)


def test_update_discovery_config_library_without_yaml_raises(
    client: DataMasqueClient, config_library: DiscoveryConfigLibrary
) -> None:
    config_library.id = DiscoveryConfigLibraryId(LIBRARY_ID_1)
    config_library.yaml = None

    with pytest.raises(DataMasqueArgumentError, match="without YAML content"):
        client.update_discovery_config_library(config_library)


def test_create_or_update_discovery_config_library_create(
    client: DataMasqueClient, config_library: DiscoveryConfigLibrary
) -> None:
    create_response = {
        "id": LIBRARY_ID_1,
        "name": "test_library",
        "namespace": "test_ns",
        "config_yaml": "labels: []\nmetadata_rules: []\nidd_rules: []\n",
        "is_valid": "valid",
        "validation_error": None,
        "created": "2025-06-01T10:00:00Z",
        "modified": "2025-06-01T10:00:00Z",
    }

    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/discovery/config-libraries/",
            json=[],
            status_code=200,
        )
        m.post(
            "http://test-server/api/discovery/config-libraries/",
            json=create_response,
            status_code=201,
        )
        result = client.create_or_update_discovery_config_library(config_library)

    assert result.id == DiscoveryConfigLibraryId(LIBRARY_ID_1)
    assert m.request_history[0].method == "GET"
    assert "name_exact=test_library" in m.request_history[0].url
    assert m.request_history[1].method == "POST"


def test_create_or_update_discovery_config_library_update(
    client: DataMasqueClient, config_library: DiscoveryConfigLibrary
) -> None:
    list_response = [
        {
            "id": LIBRARY_ID_1,
            "name": "test_library",
            "namespace": "test_ns",
            "is_valid": "valid",
            "created": "2025-06-01T10:00:00Z",
            "modified": "2025-06-01T10:00:00Z",
        },
    ]
    update_response = {
        "id": LIBRARY_ID_1,
        "name": "test_library",
        "namespace": "test_ns",
        "config_yaml": "labels: []\nmetadata_rules: []\nidd_rules: []\n",
        "is_valid": "valid",
        "validation_error": None,
        "created": "2025-06-01T10:00:00Z",
        "modified": "2025-06-02T10:00:00Z",
    }

    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/discovery/config-libraries/",
            json=list_response,
            status_code=200,
        )
        m.put(
            f"http://test-server/api/discovery/config-libraries/{LIBRARY_ID_1}/",
            json=update_response,
            status_code=200,
        )
        result = client.create_or_update_discovery_config_library(config_library)

    assert result.id == DiscoveryConfigLibraryId(LIBRARY_ID_1)
    assert m.call_count == 2
    assert m.request_history[0].method == "GET"
    assert m.request_history[1].method == "PUT"


def test_delete_discovery_config_library_by_id(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.delete(
            f"http://test-server/api/discovery/config-libraries/{LIBRARY_ID_1}/",
            status_code=204,
        )
        client.delete_discovery_config_library_by_id_if_exists(DiscoveryConfigLibraryId(LIBRARY_ID_1))

    assert m.call_count == 1


def test_delete_discovery_config_library_by_id_not_found(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.delete(
            f"http://test-server/api/discovery/config-libraries/{LIBRARY_ID_1}/",
            status_code=404,
        )
        client.delete_discovery_config_library_by_id_if_exists(DiscoveryConfigLibraryId(LIBRARY_ID_1))


def test_delete_discovery_config_library_by_id_force(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.delete(
            f"http://test-server/api/discovery/config-libraries/{LIBRARY_ID_1}/",
            status_code=204,
        )
        client.delete_discovery_config_library_by_id_if_exists(DiscoveryConfigLibraryId(LIBRARY_ID_1), force=True)

    assert "force=true" in m.last_request.url


def test_delete_discovery_config_library_by_name(
    client: DataMasqueClient, sample_library_list_response: list[dict[str, Any]]
) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/discovery/config-libraries/",
            json=sample_library_list_response,
            status_code=200,
        )
        m.delete(
            f"http://test-server/api/discovery/config-libraries/{LIBRARY_ID_1}/",
            status_code=204,
        )
        client.delete_discovery_config_library_by_name_if_exists("my_library", "org")

    assert m.request_history[0].method == "GET"
    assert "name_exact=my_library" in m.request_history[0].url
    assert m.request_history[1].method == "DELETE"


def test_delete_discovery_config_library_by_name_not_found(
    client: DataMasqueClient, sample_library_list_response: list[dict[str, Any]]
) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/discovery/config-libraries/",
            json=sample_library_list_response,
            status_code=200,
        )
        client.delete_discovery_config_library_by_name_if_exists("nonexistent")

    assert m.call_count == 1
    assert m.request_history[0].method == "GET"


@pytest.mark.parametrize(
    ("is_valid", "validation_error"),
    [("valid", None), ("invalid", 'duplicate label "email"')],
)
def test_validate_discovery_config_library_round_trips_outcome(
    client: DataMasqueClient,
    config_library: DiscoveryConfigLibrary,
    is_valid: str,
    validation_error: Optional[str],
) -> None:
    create_response = {
        "id": LIBRARY_ID_1,
        "name": "dm_python_validate_abc",
        "namespace": "test_ns",
        "is_valid": is_valid,
        "validation_error": validation_error,
        "usage_count": 0,
    }

    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/discovery/config-libraries/", json=create_response, status_code=201)
        delete = m.delete(f"http://test-server/api/discovery/config-libraries/{LIBRARY_ID_1}/", status_code=204)
        result = client.validate_discovery_config_library(config_library)

    assert result is config_library
    assert result.is_valid is ValidationStatus(is_valid)
    assert result.validation_error == validation_error
    assert result.name == "test_library"
    assert result.namespace == "test_ns"
    assert delete.called_once

    request_body = m.request_history[0].json()
    assert request_body["name"].startswith("dm_python_validate_")
    assert request_body["namespace"] == "test_ns"
    assert "config_yaml" in request_body


def test_validate_discovery_config_library_without_yaml_raises(
    client: DataMasqueClient, sample_library_list_response: list[dict[str, Any]]
) -> None:

    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/discovery/config-libraries/", json=sample_library_list_response, status_code=200)
        library = client.list_discovery_config_libraries()[0]
        assert library.yaml is None

        with pytest.raises(DataMasqueArgumentError, match="without YAML content"):
            client.validate_discovery_config_library(library)

        assert not any(request.method == "POST" for request in m.request_history)


def test_create_discovery_config_library_with_empty_yaml_raises(client: DataMasqueClient) -> None:
    library = DiscoveryConfigLibrary(name="test_library", yaml="")

    with requests_mock.Mocker() as m:
        with pytest.raises(DataMasqueArgumentError, match="without YAML content"):
            client.create_discovery_config_library(library)

        assert not any(request.method == "POST" for request in m.request_history)


def test_validate_discovery_config_library_with_empty_yaml_raises(client: DataMasqueClient) -> None:
    library = DiscoveryConfigLibrary(name="test_library", yaml="")

    with requests_mock.Mocker() as m:
        with pytest.raises(DataMasqueArgumentError, match="without YAML content"):
            client.validate_discovery_config_library(library)

        assert not any(request.method == "POST" for request in m.request_history)


def test_validate_discovery_config_library_deletes_temp_library_when_response_is_rejected(
    client: DataMasqueClient, config_library: DiscoveryConfigLibrary
) -> None:
    rejected_response = {
        "id": LIBRARY_ID_1,
        "name": "dm_python_validate_abc",
        "namespace": "",
        "is_valid": "not_a_real_status",
        "validation_error": None,
        "usage_count": 0,
    }

    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/discovery/config-libraries/", json=rejected_response, status_code=201)
        delete = m.delete(f"http://test-server/api/discovery/config-libraries/{LIBRARY_ID_1}/", status_code=204)

        with pytest.raises(ValidationError):
            client.validate_discovery_config_library(config_library)

    assert delete.called_once


def test_validate_discovery_config_library_returns_outcome_when_cleanup_cannot_reach_server(
    client: DataMasqueClient, config_library: DiscoveryConfigLibrary, caplog: pytest.LogCaptureFixture
) -> None:
    create_response = {
        "id": LIBRARY_ID_1,
        "name": "dm_python_validate_abc",
        "namespace": "",
        "is_valid": "valid",
        "validation_error": None,
        "usage_count": 0,
    }

    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/discovery/config-libraries/", json=create_response, status_code=201)
        m.delete(
            f"http://test-server/api/discovery/config-libraries/{LIBRARY_ID_1}/",
            exc=requests.exceptions.ConnectionError("connection refused"),
        )
        with caplog.at_level(logging.WARNING, logger="datamasque.client.base"):
            result = client.validate_discovery_config_library(config_library)

    assert result.is_valid is ValidationStatus.valid
    assert any("Failed to clean up temporary validation library" in r.getMessage() for r in caplog.records)
