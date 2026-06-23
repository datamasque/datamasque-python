"""Tests for discovery-config support in the DataMasque client."""

from datetime import datetime
from typing import Any

import pytest
import requests_mock

from datamasque.client import DataMasqueClient
from datamasque.client.exceptions import DataMasqueApiError, DataMasqueException
from datamasque.client.models.discovery_config import (
    DiscoveryConfig,
    DiscoveryConfigId,
    DiscoveryConfigType,
    unwrap_discovery_config_id,
)
from datamasque.client.models.status import ValidationStatus

CONFIG_ID_1 = "aaaaaaaa-1111-2222-3333-444444444444"
CONFIG_ID_2 = "bbbbbbbb-1111-2222-3333-444444444444"


@pytest.fixture
def sample_config_list_response() -> dict[str, Any]:
    return {
        "count": 2,
        "next": None,
        "previous": None,
        "results": [
            {
                "id": CONFIG_ID_1,
                "name": "my_config",
                "config_type": "database",
                "archived": False,
                "created": "2025-01-01T12:00:00Z",
                "modified": "2025-01-02T12:00:00Z",
            },
            {
                "id": CONFIG_ID_2,
                "name": "another_config",
                "config_type": "database",
                "archived": False,
                "created": "2025-02-01T12:00:00Z",
                "modified": "2025-02-02T12:00:00Z",
            },
        ],
    }


@pytest.fixture
def sample_config_detail_response() -> dict[str, Any]:
    return {
        "id": CONFIG_ID_1,
        "name": "my_config",
        "config_yaml": "labels: []\nmetadata_rules: []\nidd_rules: []\n",
        "config_type": "database",
        "archived": False,
        "created": "2025-01-01T12:00:00Z",
        "modified": "2025-01-02T12:00:00Z",
    }


@pytest.fixture
def discovery_config() -> DiscoveryConfig:
    return DiscoveryConfig(
        name="test_config",
        yaml="labels: []\nmetadata_rules: []\nidd_rules: []\n",
        config_type="database",
    )


def test_list_discovery_configs(client: DataMasqueClient, sample_config_list_response: dict[str, Any]) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/discovery/configs/",
            json=sample_config_list_response,
            status_code=200,
        )
        configs = client.list_discovery_configs()

    assert len(configs) == 2
    assert configs[0].id == DiscoveryConfigId(CONFIG_ID_1)
    assert configs[0].name == "my_config"
    assert configs[0].yaml is None
    assert configs[1].name == "another_config"


def test_list_discovery_configs_pagination(client: DataMasqueClient) -> None:
    page1 = {
        "count": 3,
        "next": "http://test-server/api/discovery/configs/?limit=2&offset=2",
        "previous": None,
        "results": [
            {
                "id": CONFIG_ID_1,
                "name": "c1",
                "config_type": "database",
                "archived": False,
                "created": "2025-01-01T12:00:00Z",
                "modified": "2025-01-01T12:00:00Z",
            },
            {
                "id": CONFIG_ID_2,
                "name": "c2",
                "config_type": "database",
                "archived": False,
                "created": "2025-01-01T12:00:00Z",
                "modified": "2025-01-01T12:00:00Z",
            },
        ],
    }
    page2 = {
        "count": 3,
        "next": None,
        "previous": "http://test-server/api/discovery/configs/?limit=2",
        "results": [
            {
                "id": "cccccccc-1111-2222-3333-444444444444",
                "name": "c3",
                "config_type": "database",
                "archived": False,
                "created": "2025-01-01T12:00:00Z",
                "modified": "2025-01-01T12:00:00Z",
            },
        ],
    }

    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/discovery/configs/",
            [{"json": page1, "status_code": 200}, {"json": page2, "status_code": 200}],
        )
        configs = client.list_discovery_configs()

    assert [c.name for c in configs] == ["c1", "c2", "c3"]


def test_list_discovery_configs_empty(client: DataMasqueClient) -> None:
    empty_response = {"count": 0, "next": None, "previous": None, "results": []}

    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/discovery/configs/", json=empty_response, status_code=200)
        configs = client.list_discovery_configs()

    assert configs == []


def test_get_discovery_config(client: DataMasqueClient, sample_config_detail_response: dict[str, Any]) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            f"http://test-server/api/discovery/configs/{CONFIG_ID_1}/",
            json=sample_config_detail_response,
            status_code=200,
        )
        config = client.get_discovery_config(DiscoveryConfigId(CONFIG_ID_1))

    assert config.id == DiscoveryConfigId(CONFIG_ID_1)
    assert config.name == "my_config"
    assert config.yaml == "labels: []\nmetadata_rules: []\nidd_rules: []\n"


def test_get_discovery_config_by_name_found(
    client: DataMasqueClient, sample_config_detail_response: dict[str, Any]
) -> None:
    list_response = {
        "count": 1,
        "next": None,
        "previous": None,
        "results": [
            {
                "id": CONFIG_ID_1,
                "name": "my_config",
                "config_type": "database",
                "archived": False,
                "created": "2025-01-01T12:00:00Z",
                "modified": "2025-01-02T12:00:00Z",
            },
        ],
    }

    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/discovery/configs/",
            json=list_response,
            status_code=200,
        )
        m.get(
            f"http://test-server/api/discovery/configs/{CONFIG_ID_1}/",
            json=sample_config_detail_response,
            status_code=200,
        )
        config = client.get_discovery_config_by_name("my_config", DiscoveryConfigType.database)

    assert config is not None
    assert config.name == "my_config"
    assert "name_exact=my_config" in m.request_history[0].url
    assert "config_type=database" in m.request_history[0].url


def test_get_discovery_config_by_name_not_found(client: DataMasqueClient) -> None:
    empty_response = {"count": 0, "next": None, "previous": None, "results": []}

    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/discovery/configs/",
            json=empty_response,
            status_code=200,
        )
        config = client.get_discovery_config_by_name("nonexistent", DiscoveryConfigType.database)

    assert config is None


def test_get_discovery_config_by_name_raises_when_server_omits_id(client: DataMasqueClient) -> None:
    list_response_without_id = {
        "count": 1,
        "next": None,
        "previous": None,
        "results": [
            {
                "name": "my_config",
                "config_type": "database",
                "archived": False,
                "created": "2025-01-01T12:00:00Z",
                "modified": "2025-01-02T12:00:00Z",
            },
        ],
    }

    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/discovery/configs/",
            json=list_response_without_id,
            status_code=200,
        )
        with pytest.raises(DataMasqueApiError, match="without an `id`"):
            client.get_discovery_config_by_name("my_config", DiscoveryConfigType.database)


def test_create_discovery_config(client: DataMasqueClient, discovery_config: DiscoveryConfig) -> None:
    create_response = {
        "id": CONFIG_ID_1,
        "name": "test_config",
        "config_yaml": "labels: []\nmetadata_rules: []\nidd_rules: []\n",
        "config_type": "database",
        "archived": False,
        "created": "2025-06-01T10:00:00Z",
        "modified": "2025-06-01T10:00:00Z",
    }

    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/discovery/configs/",
            json=create_response,
            status_code=201,
        )
        result = client.create_discovery_config(discovery_config)

    assert result is discovery_config
    assert result.id == DiscoveryConfigId(CONFIG_ID_1)
    assert result.created == datetime.fromisoformat("2025-06-01T10:00:00+00:00")
    assert result.modified == datetime.fromisoformat("2025-06-01T10:00:00+00:00")

    request_body = m.last_request.json()
    assert request_body["name"] == "test_config"
    assert request_body["config_yaml"] == "labels: []\nmetadata_rules: []\nidd_rules: []\n"
    assert request_body["config_type"] == "database"


def test_update_discovery_config(client: DataMasqueClient, discovery_config: DiscoveryConfig) -> None:
    discovery_config.id = DiscoveryConfigId(CONFIG_ID_1)

    update_response = {
        "id": CONFIG_ID_1,
        "name": "test_config",
        "config_yaml": "labels: []\nmetadata_rules: []\nidd_rules: []\n",
        "config_type": "database",
        "archived": False,
        "created": "2025-06-01T10:00:00Z",
        "modified": "2025-06-02T10:00:00Z",
    }

    with requests_mock.Mocker() as m:
        m.put(
            f"http://test-server/api/discovery/configs/{CONFIG_ID_1}/",
            json=update_response,
            status_code=200,
        )
        result = client.update_discovery_config(discovery_config)

    assert result is discovery_config
    assert result.modified == datetime.fromisoformat("2025-06-02T10:00:00+00:00")

    request_body = m.last_request.json()
    assert request_body["name"] == "test_config"
    assert request_body["config_yaml"] == "labels: []\nmetadata_rules: []\nidd_rules: []\n"


def test_update_discovery_config_no_id_raises(client: DataMasqueClient, discovery_config: DiscoveryConfig) -> None:
    with pytest.raises(ValueError, match="id is None"):
        client.update_discovery_config(discovery_config)


def test_create_or_update_discovery_config_create(client: DataMasqueClient, discovery_config: DiscoveryConfig) -> None:
    empty_list = {"count": 0, "next": None, "previous": None, "results": []}
    create_response = {
        "id": CONFIG_ID_1,
        "name": "test_config",
        "config_yaml": "labels: []\nmetadata_rules: []\nidd_rules: []\n",
        "config_type": "database",
        "archived": False,
        "created": "2025-06-01T10:00:00Z",
        "modified": "2025-06-01T10:00:00Z",
    }

    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/discovery/configs/", json=empty_list, status_code=200)
        m.post("http://test-server/api/discovery/configs/", json=create_response, status_code=201)
        result = client.create_or_update_discovery_config(discovery_config)

    assert result.id == DiscoveryConfigId(CONFIG_ID_1)
    assert m.request_history[0].method == "GET"
    assert m.request_history[1].method == "POST"


def test_create_or_update_discovery_config_update(client: DataMasqueClient, discovery_config: DiscoveryConfig) -> None:
    list_response = {
        "count": 1,
        "next": None,
        "previous": None,
        "results": [
            {
                "id": CONFIG_ID_1,
                "name": "test_config",
                "config_type": "database",
                "archived": False,
                "created": "2025-06-01T10:00:00Z",
                "modified": "2025-06-01T10:00:00Z",
            },
        ],
    }
    detail_response = {
        "id": CONFIG_ID_1,
        "name": "test_config",
        "config_yaml": "labels: []",
        "config_type": "database",
        "archived": False,
        "created": "2025-06-01T10:00:00Z",
        "modified": "2025-06-01T10:00:00Z",
    }
    update_response = {
        "id": CONFIG_ID_1,
        "name": "test_config",
        "config_yaml": "labels: []\nmetadata_rules: []\nidd_rules: []\n",
        "config_type": "database",
        "archived": False,
        "created": "2025-06-01T10:00:00Z",
        "modified": "2025-06-02T10:00:00Z",
    }

    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/discovery/configs/", json=list_response, status_code=200)
        m.get(
            f"http://test-server/api/discovery/configs/{CONFIG_ID_1}/",
            json=detail_response,
            status_code=200,
        )
        m.put(
            f"http://test-server/api/discovery/configs/{CONFIG_ID_1}/",
            json=update_response,
            status_code=200,
        )
        result = client.create_or_update_discovery_config(discovery_config)

    assert result.id == DiscoveryConfigId(CONFIG_ID_1)
    assert m.request_history[0].method == "GET"
    assert m.request_history[1].method == "GET"
    assert m.request_history[2].method == "PUT"


def test_delete_discovery_config_by_id(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.delete(f"http://test-server/api/discovery/configs/{CONFIG_ID_1}/", status_code=204)
        client.delete_discovery_config_by_id_if_exists(DiscoveryConfigId(CONFIG_ID_1))

    assert m.call_count == 1


def test_delete_discovery_config_by_id_not_found(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.delete(f"http://test-server/api/discovery/configs/{CONFIG_ID_1}/", status_code=404)
        client.delete_discovery_config_by_id_if_exists(DiscoveryConfigId(CONFIG_ID_1))


def test_delete_discovery_config_by_name(client: DataMasqueClient, sample_config_list_response: dict[str, Any]) -> None:
    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/discovery/configs/", json=sample_config_list_response, status_code=200)
        m.delete(f"http://test-server/api/discovery/configs/{CONFIG_ID_1}/", status_code=204)
        client.delete_discovery_config_by_name_if_exists("my_config", DiscoveryConfigType.database)

    assert m.request_history[0].method == "GET"
    assert m.request_history[1].method == "DELETE"


def test_delete_discovery_config_by_name_not_found(
    client: DataMasqueClient, sample_config_list_response: dict[str, Any]
) -> None:
    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/discovery/configs/", json=sample_config_list_response, status_code=200)
        client.delete_discovery_config_by_name_if_exists("nonexistent", DiscoveryConfigType.database)

    assert m.call_count == 1
    assert m.request_history[0].method == "GET"


def test_delete_discovery_config_by_name_raises_when_server_omits_id(client: DataMasqueClient) -> None:
    list_response_without_id = {
        "count": 1,
        "next": None,
        "previous": None,
        "results": [
            {
                "name": "my_config",
                "config_type": "database",
                "archived": False,
                "created": "2025-01-01T12:00:00Z",
                "modified": "2025-01-02T12:00:00Z",
            },
        ],
    }

    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/discovery/configs/",
            json=list_response_without_id,
            status_code=200,
        )
        with pytest.raises(DataMasqueException, match="without an `id`"):
            client.delete_discovery_config_by_name_if_exists("my_config", DiscoveryConfigType.database)


def test_delete_discovery_config_by_name_only_deletes_matching_type(client: DataMasqueClient) -> None:
    shared_name_response = {
        "count": 2,
        "next": None,
        "previous": None,
        "results": [
            {"id": CONFIG_ID_1, "name": "shared", "config_type": "database", "archived": False},
            {"id": CONFIG_ID_2, "name": "shared", "config_type": "file", "archived": False},
        ],
    }

    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/discovery/configs/", json=shared_name_response, status_code=200)
        m.delete(f"http://test-server/api/discovery/configs/{CONFIG_ID_2}/", status_code=204)
        client.delete_discovery_config_by_name_if_exists("shared", DiscoveryConfigType.file)

    assert m.request_history[-1].method == "DELETE"
    assert CONFIG_ID_2 in m.request_history[-1].url
    assert CONFIG_ID_1 not in m.request_history[-1].url


def test_get_default_discovery_config_yaml(client: DataMasqueClient) -> None:
    yaml_body = "labels: []\nmetadata_rules: []\nidd_rules: []\n"

    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/discovery/configs/defaults/",
            text=yaml_body,
            status_code=200,
            headers={"Content-Type": "application/x-yaml"},
        )
        result = client.get_default_discovery_config_yaml()

    assert result == yaml_body


def test_discovery_config_parses_validation_fields() -> None:
    """`is_valid` and `validation_error` round-trip from API responses."""
    config = DiscoveryConfig.model_validate(
        {
            "id": CONFIG_ID_1,
            "name": "my_config",
            "config_type": "database",
            "config_yaml": "labels: []",
            "is_valid": "invalid",
            "validation_error": "bad shape on line 3",
            "created": "2025-01-01T12:00:00Z",
            "modified": "2025-01-02T12:00:00Z",
        }
    )

    assert config.is_valid is ValidationStatus.invalid
    assert config.validation_error == "bad shape on line 3"


def test_discovery_config_validation_fields_optional() -> None:
    """Older / lighter API responses without validation fields still parse."""
    config = DiscoveryConfig.model_validate(
        {
            "id": CONFIG_ID_1,
            "name": "my_config",
            "config_type": "database",
            "created": "2025-01-01T12:00:00Z",
            "modified": "2025-01-02T12:00:00Z",
        }
    )

    assert config.is_valid is None
    assert config.validation_error is None


def test_unwrap_discovery_config_id_passes_through_strings() -> None:
    assert unwrap_discovery_config_id(CONFIG_ID_1) == CONFIG_ID_1
    assert unwrap_discovery_config_id(None) is None


def test_unwrap_discovery_config_id_extracts_id_from_model() -> None:
    config = DiscoveryConfig(name="x", config_type="database", id=DiscoveryConfigId(CONFIG_ID_1))
    assert unwrap_discovery_config_id(config) == CONFIG_ID_1


def test_unwrap_discovery_config_id_raises_without_id() -> None:
    config = DiscoveryConfig(name="x", config_type="database")
    with pytest.raises(ValueError, match="id is None"):
        unwrap_discovery_config_id(config)
