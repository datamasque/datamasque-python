"""Tests for discovery config library support in the DataMasque client."""

from datetime import datetime
from typing import Any

import pytest
import requests_mock
from pydantic import ValidationError

from datamasque.client import (
    DataMasqueClient,
    DiscoveryConfigLibrary,
    DiscoveryConfigLibraryId,
    DiscoveryConfigType,
)
from datamasque.client.exceptions import DataMasqueApiError, DataMasqueException
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
            "config_type": "database",
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
            "config_type": "file",
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
        "config_type": "database",
        "config_yaml": "labels: []\nmetadata_rules: []\nidd_rules: []\n",
        "is_valid": "valid",
        "validation_error": None,
        "usage_count": 0,
        "created": "2025-01-01T12:00:00Z",
        "modified": "2025-01-02T12:00:00Z",
    }


@pytest.fixture
def same_name_both_config_types() -> list[dict[str, Any]]:
    """Two libraries sharing (name, namespace) but with different config types, as the server allows."""
    return [
        {
            "id": LIBRARY_ID_1,
            "name": "my_library",
            "namespace": "",
            "config_type": "database",
            "is_valid": "valid",
            "created": "2025-01-01T12:00:00Z",
            "modified": "2025-01-02T12:00:00Z",
        },
        {
            "id": LIBRARY_ID_2,
            "name": "my_library",
            "namespace": "",
            "config_type": "file",
            "is_valid": "valid",
            "created": "2025-01-01T12:00:00Z",
            "modified": "2025-01-02T12:00:00Z",
        },
    ]


@pytest.fixture
def config_library() -> DiscoveryConfigLibrary:
    return DiscoveryConfigLibrary(
        name="test_library",
        namespace="test_ns",
        config_type=DiscoveryConfigType.database,
        yaml="labels: []\nmetadata_rules: []\nidd_rules: []\n",
    )


def test_config_type_is_required_on_the_model() -> None:
    with pytest.raises(ValidationError, match="config_type"):
        DiscoveryConfigLibrary(name="test_library", yaml="labels: []\n")


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
    assert libraries[0].config_type is DiscoveryConfigType.database
    assert libraries[0].yaml is None
    assert libraries[0].is_valid is ValidationStatus.valid
    assert libraries[1].id == DiscoveryConfigLibraryId(LIBRARY_ID_2)
    assert libraries[1].name == "another_library"
    assert libraries[1].config_type is DiscoveryConfigType.file
    assert libraries[1].is_valid is ValidationStatus.invalid
    assert libraries[1].validation_error == "bad yaml"


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
    assert library.config_type is DiscoveryConfigType.database
    assert library.yaml == "labels: []\nmetadata_rules: []\nidd_rules: []\n"
    assert library.is_valid is ValidationStatus.valid


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
            "config_type": "database",
            "is_valid": "valid",
            "created": "2025-01-01T12:00:00Z",
            "modified": "2025-01-02T12:00:00Z",
        },
        {
            "id": LIBRARY_ID_2,
            "name": "my_library",
            "namespace": "",
            "config_type": "database",
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


def test_get_discovery_config_library_by_name_ambiguous_config_type_raises(
    client: DataMasqueClient, same_name_both_config_types: list[dict[str, Any]]
) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/discovery/config-libraries/",
            json=same_name_both_config_types,
            status_code=200,
        )
        with pytest.raises(DataMasqueException, match="config_type"):
            client.get_discovery_config_library_by_name("my_library")


def test_get_discovery_config_library_by_name_config_type_disambiguates(
    client: DataMasqueClient, same_name_both_config_types: list[dict[str, Any]]
) -> None:
    detail_response = {
        "id": LIBRARY_ID_2,
        "name": "my_library",
        "namespace": "",
        "config_type": "file",
        "config_yaml": "labels: []\n",
        "is_valid": "valid",
        "created": "2025-01-01T12:00:00Z",
        "modified": "2025-01-02T12:00:00Z",
    }

    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/discovery/config-libraries/",
            json=same_name_both_config_types,
            status_code=200,
        )
        m.get(
            f"http://test-server/api/discovery/config-libraries/{LIBRARY_ID_2}/",
            json=detail_response,
            status_code=200,
        )
        library = client.get_discovery_config_library_by_name("my_library", config_type=DiscoveryConfigType.file)

    assert library is not None
    assert library.id == DiscoveryConfigLibraryId(LIBRARY_ID_2)
    assert library.config_type is DiscoveryConfigType.file


def test_get_discovery_config_library_by_name_raises_when_server_omits_id(client: DataMasqueClient) -> None:
    """If the server returns a list entry without `id`, the by-name lookup surfaces a typed API error."""
    list_response_without_id = [
        {
            "name": "my_library",
            "namespace": "org",
            "config_type": "database",
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
        "config_type": "database",
        "config_yaml": "labels: []\nmetadata_rules: []\nidd_rules: []\n",
        "is_valid": "valid",
        "validation_error": None,
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
    assert result.created == datetime.fromisoformat("2025-06-01T10:00:00+00:00")
    assert result.modified == datetime.fromisoformat("2025-06-01T10:00:00+00:00")

    request_body = m.last_request.json()
    assert request_body["name"] == "test_library"
    assert request_body["namespace"] == "test_ns"
    assert request_body["config_type"] == "database"
    assert request_body["config_yaml"] == "labels: []\nmetadata_rules: []\nidd_rules: []\n"


def test_create_discovery_config_library_reports_validation_error(
    client: DataMasqueClient, config_library: DiscoveryConfigLibrary
) -> None:
    create_response = {
        "id": LIBRARY_ID_1,
        "name": "test_library",
        "namespace": "test_ns",
        "config_type": "database",
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
        "config_type": "database",
        "config_yaml": "labels: []\nmetadata_rules: []\nidd_rules: []\n",
        "is_valid": "valid",
        "validation_error": None,
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
    assert result.modified == datetime.fromisoformat("2025-06-02T10:00:00+00:00")

    request_body = m.last_request.json()
    assert request_body["name"] == "test_library"
    assert request_body["config_yaml"] == "labels: []\nmetadata_rules: []\nidd_rules: []\n"


def test_update_discovery_config_library_no_id_raises(
    client: DataMasqueClient, config_library: DiscoveryConfigLibrary
) -> None:
    with pytest.raises(ValueError, match="id is None"):
        client.update_discovery_config_library(config_library)


def test_update_discovery_config_library_without_yaml_raises(
    client: DataMasqueClient, config_library: DiscoveryConfigLibrary
) -> None:
    config_library.id = DiscoveryConfigLibraryId(LIBRARY_ID_1)
    config_library.yaml = None

    with pytest.raises(ValueError, match="yaml is None"):
        client.update_discovery_config_library(config_library)


def test_create_or_update_discovery_config_library_create(
    client: DataMasqueClient, config_library: DiscoveryConfigLibrary
) -> None:
    create_response = {
        "id": LIBRARY_ID_1,
        "name": "test_library",
        "namespace": "test_ns",
        "config_type": "database",
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
            "config_type": "database",
            "is_valid": "valid",
            "created": "2025-06-01T10:00:00Z",
            "modified": "2025-06-01T10:00:00Z",
        },
    ]
    update_response = {
        "id": LIBRARY_ID_1,
        "name": "test_library",
        "namespace": "test_ns",
        "config_type": "database",
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


def test_create_or_update_discovery_config_library_matches_config_type(
    client: DataMasqueClient, same_name_both_config_types: list[dict[str, Any]]
) -> None:
    file_library = DiscoveryConfigLibrary(
        name="my_library",
        config_type=DiscoveryConfigType.file,
        yaml="labels: []\n",
    )
    update_response = {
        "id": LIBRARY_ID_2,
        "name": "my_library",
        "namespace": "",
        "config_type": "file",
        "config_yaml": "labels: []\n",
        "is_valid": "valid",
        "validation_error": None,
        "created": "2025-01-01T12:00:00Z",
        "modified": "2025-01-03T12:00:00Z",
    }

    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/discovery/config-libraries/",
            json=same_name_both_config_types,
            status_code=200,
        )
        m.put(
            f"http://test-server/api/discovery/config-libraries/{LIBRARY_ID_2}/",
            json=update_response,
            status_code=200,
        )
        result = client.create_or_update_discovery_config_library(file_library)

    assert result.id == DiscoveryConfigLibraryId(LIBRARY_ID_2)


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


def test_delete_discovery_config_library_by_name_deletes_both_config_types(
    client: DataMasqueClient, same_name_both_config_types: list[dict[str, Any]]
) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/discovery/config-libraries/",
            json=same_name_both_config_types,
            status_code=200,
        )
        m.delete(
            f"http://test-server/api/discovery/config-libraries/{LIBRARY_ID_1}/",
            status_code=204,
        )
        m.delete(
            f"http://test-server/api/discovery/config-libraries/{LIBRARY_ID_2}/",
            status_code=204,
        )
        client.delete_discovery_config_library_by_name_if_exists("my_library")

    assert [request.method for request in m.request_history] == ["GET", "DELETE", "DELETE"]


def test_delete_discovery_config_library_by_name_config_type_deletes_only_that_type(
    client: DataMasqueClient, same_name_both_config_types: list[dict[str, Any]]
) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/discovery/config-libraries/",
            json=same_name_both_config_types,
            status_code=200,
        )
        m.delete(
            f"http://test-server/api/discovery/config-libraries/{LIBRARY_ID_2}/",
            status_code=204,
        )
        client.delete_discovery_config_library_by_name_if_exists("my_library", config_type=DiscoveryConfigType.file)

    assert m.call_count == 2
    assert m.request_history[1].method == "DELETE"
    assert LIBRARY_ID_2 in m.request_history[1].url


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
