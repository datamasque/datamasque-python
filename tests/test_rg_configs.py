"""Tests for ruleset-generation (RG) config support in the DataMasque client."""

from datetime import datetime
from typing import Any

import pytest
import requests_mock

from datamasque.client import DataMasqueClient
from datamasque.client.exceptions import DataMasqueApiError
from datamasque.client.models.rg_config import (
    RGConfig,
    RGConfigId,
    unwrap_rg_config_id,
)
from datamasque.client.models.status import ValidationErrorType, ValidationStatus

CONFIG_ID_1 = "aaaaaaaa-1111-2222-3333-444444444444"
CONFIG_ID_2 = "bbbbbbbb-1111-2222-3333-444444444444"

CONFIG_YAML = "labels:\n  full_name:\n    preset_mask: first_name_mask\n"


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
                "archived": False,
                "created": "2025-01-01T12:00:00Z",
                "modified": "2025-01-02T12:00:00Z",
            },
            {
                "id": CONFIG_ID_2,
                "name": "another_config",
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
        "config_yaml": CONFIG_YAML,
        "archived": False,
        "created": "2025-01-01T12:00:00Z",
        "modified": "2025-01-02T12:00:00Z",
    }


@pytest.fixture
def rg_config() -> RGConfig:
    return RGConfig(name="test_config", yaml=CONFIG_YAML)


def test_list_rg_configs(client: DataMasqueClient, sample_config_list_response: dict[str, Any]) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/ruleset-generation-configs/",
            json=sample_config_list_response,
            status_code=200,
        )
        configs = client.list_rg_configs()

    assert len(configs) == 2
    assert configs[0].id == RGConfigId(CONFIG_ID_1)
    assert configs[0].name == "my_config"
    assert configs[0].yaml is None
    assert configs[1].name == "another_config"


def test_list_rg_configs_pagination(client: DataMasqueClient) -> None:
    page1 = {
        "count": 3,
        "next": "http://test-server/api/ruleset-generation-configs/?limit=2&offset=2",
        "previous": None,
        "results": [
            {
                "id": CONFIG_ID_1,
                "name": "c1",
                "archived": False,
                "created": "2025-01-01T12:00:00Z",
                "modified": "2025-01-01T12:00:00Z",
            },
            {
                "id": CONFIG_ID_2,
                "name": "c2",
                "archived": False,
                "created": "2025-01-01T12:00:00Z",
                "modified": "2025-01-01T12:00:00Z",
            },
        ],
    }
    page2 = {
        "count": 3,
        "next": None,
        "previous": "http://test-server/api/ruleset-generation-configs/?limit=2",
        "results": [
            {
                "id": "cccccccc-1111-2222-3333-444444444444",
                "name": "c3",
                "archived": False,
                "created": "2025-01-01T12:00:00Z",
                "modified": "2025-01-01T12:00:00Z",
            },
        ],
    }

    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/ruleset-generation-configs/",
            [{"json": page1, "status_code": 200}, {"json": page2, "status_code": 200}],
        )
        configs = client.list_rg_configs()

    assert [c.name for c in configs] == ["c1", "c2", "c3"]


def test_list_rg_configs_empty(client: DataMasqueClient) -> None:
    empty_response = {"count": 0, "next": None, "previous": None, "results": []}

    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/ruleset-generation-configs/", json=empty_response, status_code=200)
        configs = client.list_rg_configs()

    assert configs == []


def test_get_rg_config(client: DataMasqueClient, sample_config_detail_response: dict[str, Any]) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            f"http://test-server/api/ruleset-generation-configs/{CONFIG_ID_1}/",
            json=sample_config_detail_response,
            status_code=200,
        )
        config = client.get_rg_config(RGConfigId(CONFIG_ID_1))

    assert config.id == RGConfigId(CONFIG_ID_1)
    assert config.name == "my_config"
    assert config.yaml == CONFIG_YAML


def test_get_rg_config_by_name_found(client: DataMasqueClient, sample_config_detail_response: dict[str, Any]) -> None:
    list_response = {
        "count": 1,
        "next": None,
        "previous": None,
        "results": [
            {
                "id": CONFIG_ID_1,
                "name": "my_config",
                "archived": False,
                "created": "2025-01-01T12:00:00Z",
                "modified": "2025-01-02T12:00:00Z",
            },
        ],
    }

    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/ruleset-generation-configs/",
            json=list_response,
            status_code=200,
        )
        m.get(
            f"http://test-server/api/ruleset-generation-configs/{CONFIG_ID_1}/",
            json=sample_config_detail_response,
            status_code=200,
        )
        config = client.get_rg_config_by_name("my_config")

    assert config is not None
    assert config.name == "my_config"
    assert "name_exact=my_config" in m.request_history[0].url


def test_get_rg_config_by_name_not_found(client: DataMasqueClient) -> None:
    empty_response = {"count": 0, "next": None, "previous": None, "results": []}

    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/ruleset-generation-configs/",
            json=empty_response,
            status_code=200,
        )
        config = client.get_rg_config_by_name("nonexistent")

    assert config is None


def test_get_rg_config_by_name_raises_when_server_omits_id(client: DataMasqueClient) -> None:
    list_response_without_id = {
        "count": 1,
        "next": None,
        "previous": None,
        "results": [
            {
                "name": "my_config",
                "archived": False,
                "created": "2025-01-01T12:00:00Z",
                "modified": "2025-01-02T12:00:00Z",
            },
        ],
    }

    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/ruleset-generation-configs/",
            json=list_response_without_id,
            status_code=200,
        )
        with pytest.raises(DataMasqueApiError, match="without an `id`"):
            client.get_rg_config_by_name("my_config")


def test_create_rg_config(client: DataMasqueClient, rg_config: RGConfig) -> None:
    create_response = {
        "id": CONFIG_ID_1,
        "name": "test_config",
        "config_yaml": CONFIG_YAML,
        "is_valid": "valid",
        "archived": False,
        "created": "2025-06-01T10:00:00Z",
        "modified": "2025-06-01T10:00:00Z",
    }

    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/ruleset-generation-configs/",
            json=create_response,
            status_code=201,
        )
        result = client.create_rg_config(rg_config)

    assert result is rg_config
    assert result.id == RGConfigId(CONFIG_ID_1)
    assert result.is_valid is ValidationStatus.valid
    assert result.created == datetime.fromisoformat("2025-06-01T10:00:00+00:00")
    assert result.modified == datetime.fromisoformat("2025-06-01T10:00:00+00:00")

    request_body = m.last_request.json()
    assert request_body["name"] == "test_config"
    assert request_body["config_yaml"] == CONFIG_YAML
    # Server-managed read-only fields must not be echoed back in the request body.
    assert "id" not in request_body
    assert "is_valid" not in request_body
    assert "created" not in request_body
    assert "modified" not in request_body


def test_create_and_update_rg_config_populate_validation_errors(client: DataMasqueClient, rg_config: RGConfig) -> None:
    """The server's `validation_errors` list is surfaced on the returned config and never echoed back."""

    invalid_response = {
        "id": CONFIG_ID_1,
        "name": "test_config",
        "config_yaml": CONFIG_YAML,
        "is_valid": "invalid",
        "validation_errors": [
            {
                "message": "Missing required field: labels",
                "validation_error_type": "ruleset",
                "line_number": 5,
                "column_number": 3,
            }
        ],
        "archived": False,
        "created": "2025-06-01T10:00:00Z",
        "modified": "2025-06-01T10:00:00Z",
    }
    valid_response = {**invalid_response, "is_valid": "valid", "validation_errors": []}

    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/ruleset-generation-configs/", json=invalid_response, status_code=201)
        m.put(
            f"http://test-server/api/ruleset-generation-configs/{CONFIG_ID_1}/",
            json=valid_response,
            status_code=200,
        )

        created = client.create_rg_config(rg_config)
        assert created.is_valid is ValidationStatus.invalid
        assert len(created.validation_errors) == 1
        error = created.validation_errors[0]
        assert error.message == "Missing required field: labels"
        assert error.validation_error_type is ValidationErrorType.ruleset
        assert error.line_number == 5
        assert error.column_number == 3

        # Re-submitting the same object (read-only fields now populated) clears the errors on success.
        updated = client.update_rg_config(rg_config)
        assert updated.is_valid is ValidationStatus.valid
        assert updated.validation_errors == []

    for request in m.request_history:
        assert "validation_errors" not in request.json()


def test_update_rg_config(client: DataMasqueClient, rg_config: RGConfig) -> None:
    rg_config.id = RGConfigId(CONFIG_ID_1)

    update_response = {
        "id": CONFIG_ID_1,
        "name": "test_config",
        "config_yaml": CONFIG_YAML,
        "archived": False,
        "created": "2025-06-01T10:00:00Z",
        "modified": "2025-06-02T10:00:00Z",
    }

    with requests_mock.Mocker() as m:
        m.put(
            f"http://test-server/api/ruleset-generation-configs/{CONFIG_ID_1}/",
            json=update_response,
            status_code=200,
        )
        result = client.update_rg_config(rg_config)

    assert result is rg_config
    assert result.modified == datetime.fromisoformat("2025-06-02T10:00:00+00:00")

    request_body = m.last_request.json()
    assert request_body["name"] == "test_config"
    assert request_body["config_yaml"] == CONFIG_YAML
    # The id identifies the config in the URL, not the request body.
    assert "id" not in request_body


def test_update_rg_config_no_id_raises(client: DataMasqueClient, rg_config: RGConfig) -> None:
    with pytest.raises(ValueError, match="id is None"):
        client.update_rg_config(rg_config)


def test_update_rg_config_no_yaml_raises(client: DataMasqueClient) -> None:
    """A config from a list response carries no YAML, so a PUT would silently blank the server's copy."""

    listed = RGConfig(name="test_config")
    listed.id = RGConfigId(CONFIG_ID_1)

    with pytest.raises(ValueError, match="yaml is None"):
        client.update_rg_config(listed)


def test_create_or_update_rg_config_create(client: DataMasqueClient, rg_config: RGConfig) -> None:
    empty_list = {"count": 0, "next": None, "previous": None, "results": []}
    create_response = {
        "id": CONFIG_ID_1,
        "name": "test_config",
        "config_yaml": CONFIG_YAML,
        "archived": False,
        "created": "2025-06-01T10:00:00Z",
        "modified": "2025-06-01T10:00:00Z",
    }

    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/ruleset-generation-configs/", json=empty_list, status_code=200)
        m.post("http://test-server/api/ruleset-generation-configs/", json=create_response, status_code=201)
        result = client.create_or_update_rg_config(rg_config)

    assert result.id == RGConfigId(CONFIG_ID_1)
    assert m.request_history[0].method == "GET"
    assert m.request_history[1].method == "POST"


def test_create_or_update_rg_config_update(client: DataMasqueClient, rg_config: RGConfig) -> None:
    list_response = {
        "count": 1,
        "next": None,
        "previous": None,
        "results": [
            {
                "id": CONFIG_ID_1,
                "name": "test_config",
                "archived": False,
                "created": "2025-06-01T10:00:00Z",
                "modified": "2025-06-01T10:00:00Z",
            },
        ],
    }
    update_response = {
        "id": CONFIG_ID_1,
        "name": "test_config",
        "config_yaml": CONFIG_YAML,
        "archived": False,
        "created": "2025-06-01T10:00:00Z",
        "modified": "2025-06-02T10:00:00Z",
    }

    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/ruleset-generation-configs/", json=list_response, status_code=200)
        m.put(
            f"http://test-server/api/ruleset-generation-configs/{CONFIG_ID_1}/",
            json=update_response,
            status_code=200,
        )
        result = client.create_or_update_rg_config(rg_config)

    assert result.id == RGConfigId(CONFIG_ID_1)
    # The upsert resolves the id from the list response alone — no detail GET before the PUT.
    assert [r.method for r in m.request_history] == ["GET", "PUT"]


def test_delete_rg_config_by_id(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.delete(f"http://test-server/api/ruleset-generation-configs/{CONFIG_ID_1}/", status_code=204)
        client.delete_rg_config_by_id_if_exists(RGConfigId(CONFIG_ID_1))

    assert m.call_count == 1


def test_delete_rg_config_by_id_not_found(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.delete(f"http://test-server/api/ruleset-generation-configs/{CONFIG_ID_1}/", status_code=404)
        client.delete_rg_config_by_id_if_exists(RGConfigId(CONFIG_ID_1))


def test_delete_rg_config_by_name(client: DataMasqueClient) -> None:
    list_response = {
        "count": 1,
        "next": None,
        "previous": None,
        "results": [
            {
                "id": CONFIG_ID_1,
                "name": "my_config",
                "archived": False,
                "created": "2025-01-01T12:00:00Z",
                "modified": "2025-01-02T12:00:00Z",
            },
        ],
    }

    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/ruleset-generation-configs/", json=list_response, status_code=200)
        m.delete(f"http://test-server/api/ruleset-generation-configs/{CONFIG_ID_1}/", status_code=204)
        client.delete_rg_config_by_name_if_exists("my_config")

    assert [r.method for r in m.request_history] == ["GET", "DELETE"]
    assert "name_exact=my_config" in m.request_history[0].url


def test_delete_rg_config_by_name_not_found(client: DataMasqueClient) -> None:
    empty_response = {"count": 0, "next": None, "previous": None, "results": []}

    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/ruleset-generation-configs/", json=empty_response, status_code=200)
        client.delete_rg_config_by_name_if_exists("nonexistent")

    assert m.call_count == 1
    assert m.request_history[0].method == "GET"


def test_get_default_rg_config_yaml(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/ruleset-generation-configs/defaults/",
            text=CONFIG_YAML,
            status_code=200,
            headers={"Content-Type": "application/x-yaml"},
        )
        result = client.get_default_rg_config_yaml()

    assert result == CONFIG_YAML


def test_rg_config_parses_validation_fields() -> None:
    """`is_valid` and `validation_errors` round-trip from API responses."""
    config = RGConfig.model_validate(
        {
            "id": CONFIG_ID_1,
            "name": "my_config",
            "config_yaml": "labels: []",
            "is_valid": "invalid",
            "validation_errors": [{"message": "bad shape on line 3", "line_number": 3, "column_number": 1}],
            "created": "2025-01-01T12:00:00Z",
            "modified": "2025-01-02T12:00:00Z",
        }
    )

    assert config.is_valid is ValidationStatus.invalid
    assert config.validation_errors[0].message == "bad shape on line 3"


def test_rg_config_validation_fields_optional() -> None:
    """Older / lighter API responses without validation fields still parse."""
    config = RGConfig.model_validate(
        {
            "id": CONFIG_ID_1,
            "name": "my_config",
            "created": "2025-01-01T12:00:00Z",
            "modified": "2025-01-02T12:00:00Z",
        }
    )

    assert config.is_valid is None
    assert config.validation_errors == []


def test_unwrap_rg_config_id_passes_through_strings() -> None:
    assert unwrap_rg_config_id(CONFIG_ID_1) == CONFIG_ID_1
    assert unwrap_rg_config_id(None) is None


def test_unwrap_rg_config_id_extracts_id_from_model() -> None:
    config = RGConfig(name="x", id=RGConfigId(CONFIG_ID_1))
    assert unwrap_rg_config_id(config) == CONFIG_ID_1


def test_unwrap_rg_config_id_raises_without_id() -> None:
    config = RGConfig(name="x")
    with pytest.raises(ValueError, match="id is None"):
        unwrap_rg_config_id(config)
