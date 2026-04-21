"""Tests for ruleset library support in the DataMasque client."""

from datetime import datetime
from typing import Any

import pytest
import requests_mock

from datamasque.client import DataMasqueClient
from datamasque.client.exceptions import DataMasqueApiError
from datamasque.client.models.ruleset import RulesetType
from datamasque.client.models.ruleset_library import (
    RulesetLibrary,
    RulesetLibraryId,
    ValidationStatus,
)

LIBRARY_ID_1 = "aaaaaaaa-1111-2222-3333-444444444444"
LIBRARY_ID_2 = "bbbbbbbb-1111-2222-3333-444444444444"


@pytest.fixture
def sample_library_list_response():
    """Paginated list response (without config_yaml)."""
    return {
        "count": 2,
        "next": None,
        "previous": None,
        "results": [
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
                "name": "another_library",
                "namespace": "",
                "is_valid": "invalid",
                "created": "2025-02-01T12:00:00Z",
                "modified": "2025-02-02T12:00:00Z",
            },
        ],
    }


@pytest.fixture
def sample_library_detail_response():
    """Detail response (with config_yaml)."""
    return {
        "id": LIBRARY_ID_1,
        "name": "my_library",
        "namespace": "org",
        "config_yaml": "version: '1.0'\nfunctions:\n  - name: my_func",
        "is_valid": "valid",
        "created": "2025-01-01T12:00:00Z",
        "modified": "2025-01-02T12:00:00Z",
    }


@pytest.fixture
def ruleset_library():
    return RulesetLibrary(
        name="test_library",
        namespace="test_ns",
        yaml="version: '1.0'\nfunctions: []",
    )


def test_ruleset_library_model_dump() -> None:
    lib = RulesetLibrary(name="lib1", namespace="ns", yaml="content: true")
    assert lib.model_dump(exclude_none=True, by_alias=True, mode="json") == {
        "name": "lib1",
        "namespace": "ns",
        "config_yaml": "content: true",
    }


def test_ruleset_library_model_dump_no_yaml() -> None:
    lib = RulesetLibrary(name="lib1", namespace="ns")
    api_dict = lib.model_dump(exclude_none=True, by_alias=True, mode="json")
    assert "config_yaml" not in api_dict
    assert api_dict == {"name": "lib1", "namespace": "ns"}


def test_ruleset_library_model_validate() -> None:
    response = {
        "id": LIBRARY_ID_1,
        "name": "my_library",
        "namespace": "org",
        "config_yaml": "version: '1.0'",
        "is_valid": "valid",
        "created": "2025-01-01T12:00:00Z",
        "modified": "2025-01-02T12:00:00Z",
    }
    lib = RulesetLibrary.model_validate(response)
    assert lib.id == RulesetLibraryId(LIBRARY_ID_1)
    assert lib.name == "my_library"
    assert lib.namespace == "org"
    assert lib.yaml == "version: '1.0'"
    assert lib.is_valid is ValidationStatus.valid
    assert lib.created == datetime.fromisoformat("2025-01-01T12:00:00+00:00")
    assert lib.modified == datetime.fromisoformat("2025-01-02T12:00:00+00:00")


def test_ruleset_library_model_validate_list(
    sample_library_list_response: dict[str, Any],
) -> None:
    """List responses omit config_yaml, so yaml should be None."""
    result = sample_library_list_response["results"][0]
    lib = RulesetLibrary.model_validate(result)
    assert lib.yaml is None
    assert lib.is_valid is ValidationStatus.valid


def test_ruleset_library_equality() -> None:
    """Pydantic structural equality compares all fields."""
    lib1 = RulesetLibrary(name="lib", namespace="ns", yaml="content")
    lib2 = RulesetLibrary(name="lib", namespace="ns", yaml="content")
    lib3 = RulesetLibrary(name="lib", namespace="other", yaml="content")
    assert lib1 == lib2
    assert lib1 != lib3


def test_list_ruleset_libraries(client: DataMasqueClient, sample_library_list_response: dict[str, Any]) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/ruleset-libraries/",
            json=sample_library_list_response,
            status_code=200,
        )
        libraries = client.list_ruleset_libraries()

    assert len(libraries) == 2
    assert libraries[0].id == RulesetLibraryId(LIBRARY_ID_1)
    assert libraries[0].name == "my_library"
    assert libraries[0].namespace == "org"
    assert libraries[0].yaml is None
    assert libraries[0].is_valid is ValidationStatus.valid
    assert libraries[1].id == RulesetLibraryId(LIBRARY_ID_2)
    assert libraries[1].name == "another_library"
    assert libraries[1].is_valid is ValidationStatus.invalid


def test_list_ruleset_libraries_pagination(client: DataMasqueClient) -> None:
    page1 = {
        "count": 3,
        "next": "http://test-server/api/ruleset-libraries/?limit=2&offset=2",
        "previous": None,
        "results": [
            {
                "id": LIBRARY_ID_1,
                "name": "lib1",
                "namespace": "",
                "is_valid": "valid",
                "created": "2025-01-01T12:00:00Z",
                "modified": "2025-01-01T12:00:00Z",
            },
            {
                "id": LIBRARY_ID_2,
                "name": "lib2",
                "namespace": "",
                "is_valid": "valid",
                "created": "2025-01-01T12:00:00Z",
                "modified": "2025-01-01T12:00:00Z",
            },
        ],
    }
    page2 = {
        "count": 3,
        "next": None,
        "previous": "http://test-server/api/ruleset-libraries/?limit=2",
        "results": [
            {
                "id": "cccccccc-1111-2222-3333-444444444444",
                "name": "lib3",
                "namespace": "",
                "is_valid": "unknown",
                "created": "2025-01-01T12:00:00Z",
                "modified": "2025-01-01T12:00:00Z",
            },
        ],
    }

    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/ruleset-libraries/",
            [{"json": page1, "status_code": 200}, {"json": page2, "status_code": 200}],
        )
        libraries = client.list_ruleset_libraries()

    assert len(libraries) == 3
    assert libraries[0].name == "lib1"
    assert libraries[1].name == "lib2"
    assert libraries[2].name == "lib3"


def test_list_ruleset_libraries_empty(client: DataMasqueClient) -> None:
    empty_response = {"count": 0, "next": None, "previous": None, "results": []}

    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/ruleset-libraries/",
            json=empty_response,
            status_code=200,
        )
        libraries = client.list_ruleset_libraries()

    assert libraries == []


def test_get_ruleset_library(client: DataMasqueClient, sample_library_detail_response: dict[str, Any]) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            f"http://test-server/api/ruleset-libraries/{LIBRARY_ID_1}/",
            json=sample_library_detail_response,
            status_code=200,
        )
        library = client.get_ruleset_library(RulesetLibraryId(LIBRARY_ID_1))

    assert library.id == RulesetLibraryId(LIBRARY_ID_1)
    assert library.name == "my_library"
    assert library.namespace == "org"
    assert library.yaml == "version: '1.0'\nfunctions:\n  - name: my_func"
    assert library.is_valid is ValidationStatus.valid


def test_get_ruleset_library_by_name_found(
    client: DataMasqueClient, sample_library_detail_response: dict[str, Any]
) -> None:
    list_response = {
        "count": 1,
        "next": None,
        "previous": None,
        "results": [
            {
                "id": LIBRARY_ID_1,
                "name": "my_library",
                "namespace": "org",
                "is_valid": "valid",
                "created": "2025-01-01T12:00:00Z",
                "modified": "2025-01-02T12:00:00Z",
            },
        ],
    }

    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/ruleset-libraries/",
            json=list_response,
            status_code=200,
        )
        m.get(
            f"http://test-server/api/ruleset-libraries/{LIBRARY_ID_1}/",
            json=sample_library_detail_response,
            status_code=200,
        )
        library = client.get_ruleset_library_by_name("my_library", "org")

    assert library is not None
    assert library.name == "my_library"
    assert library.yaml == "version: '1.0'\nfunctions:\n  - name: my_func"
    assert "name_exact=my_library" in m.request_history[0].url
    assert "namespace_exact=org" in m.request_history[0].url


def test_get_ruleset_library_by_name_raises_when_server_omits_id(client: DataMasqueClient) -> None:
    """If the server returns a list entry without `id`, `get_ruleset_library_by_name` surfaces a typed API error."""
    list_response_without_id = {
        "count": 1,
        "next": None,
        "previous": None,
        "results": [
            {
                "name": "my_library",
                "namespace": "org",
                "is_valid": "valid",
                "created": "2025-01-01T12:00:00Z",
                "modified": "2025-01-02T12:00:00Z",
            },
        ],
    }

    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/ruleset-libraries/",
            json=list_response_without_id,
            status_code=200,
        )
        with pytest.raises(DataMasqueApiError, match="without an `id`"):
            client.get_ruleset_library_by_name("my_library", "org")


def test_get_ruleset_library_by_name_not_found(client: DataMasqueClient) -> None:
    empty_response = {"count": 0, "next": None, "previous": None, "results": []}

    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/ruleset-libraries/",
            json=empty_response,
            status_code=200,
        )
        library = client.get_ruleset_library_by_name("nonexistent")

    assert library is None


def test_create_ruleset_library(client: DataMasqueClient, ruleset_library: RulesetLibrary) -> None:
    create_response = {
        "id": LIBRARY_ID_1,
        "name": "test_library",
        "namespace": "test_ns",
        "config_yaml": "version: '1.0'\nfunctions: []",
        "is_valid": "unknown",
        "created": "2025-06-01T10:00:00Z",
        "modified": "2025-06-01T10:00:00Z",
    }

    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/ruleset-libraries/",
            json=create_response,
            status_code=201,
        )
        result = client.create_ruleset_library(ruleset_library)

    assert result is ruleset_library
    assert result.id == RulesetLibraryId(LIBRARY_ID_1)
    assert result.is_valid is ValidationStatus.unknown
    assert result.created == datetime.fromisoformat("2025-06-01T10:00:00+00:00")
    assert result.modified == datetime.fromisoformat("2025-06-01T10:00:00+00:00")

    request_body = m.last_request.json()
    assert request_body["name"] == "test_library"
    assert request_body["namespace"] == "test_ns"
    assert request_body["config_yaml"] == "version: '1.0'\nfunctions: []"


def test_update_ruleset_library(client: DataMasqueClient, ruleset_library: RulesetLibrary) -> None:
    ruleset_library.id = RulesetLibraryId(LIBRARY_ID_1)

    update_response = {
        "id": LIBRARY_ID_1,
        "name": "test_library",
        "namespace": "test_ns",
        "config_yaml": "version: '1.0'\nfunctions: []",
        "is_valid": "unknown",
        "created": "2025-06-01T10:00:00Z",
        "modified": "2025-06-02T10:00:00Z",
    }

    with requests_mock.Mocker() as m:
        m.put(
            f"http://test-server/api/ruleset-libraries/{LIBRARY_ID_1}/",
            json=update_response,
            status_code=200,
        )
        result = client.update_ruleset_library(ruleset_library)

    assert result is ruleset_library
    assert result.is_valid is ValidationStatus.unknown
    assert result.modified == datetime.fromisoformat("2025-06-02T10:00:00+00:00")

    request_body = m.last_request.json()
    assert request_body["name"] == "test_library"
    assert request_body["config_yaml"] == "version: '1.0'\nfunctions: []"


def test_update_ruleset_library_no_id_raises(client: DataMasqueClient, ruleset_library: RulesetLibrary) -> None:
    with pytest.raises(ValueError, match="id is None"):
        client.update_ruleset_library(ruleset_library)


def test_create_or_update_ruleset_library_create(
    client: DataMasqueClient,
    ruleset_library: RulesetLibrary,
    sample_library_detail_response: dict[str, Any],
) -> None:
    empty_list = {"count": 0, "next": None, "previous": None, "results": []}
    create_response = {
        "id": LIBRARY_ID_1,
        "name": "test_library",
        "namespace": "test_ns",
        "config_yaml": "version: '1.0'\nfunctions: []",
        "is_valid": "unknown",
        "created": "2025-06-01T10:00:00Z",
        "modified": "2025-06-01T10:00:00Z",
    }

    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/ruleset-libraries/",
            json=empty_list,
            status_code=200,
        )
        m.post(
            "http://test-server/api/ruleset-libraries/",
            json=create_response,
            status_code=201,
        )
        result = client.create_or_update_ruleset_library(ruleset_library)

    assert result.id == RulesetLibraryId(LIBRARY_ID_1)
    # Should have called GET (list for name lookup) then POST (create)
    assert m.request_history[0].method == "GET"
    assert m.request_history[1].method == "POST"


def test_create_or_update_ruleset_library_update(
    client: DataMasqueClient,
    ruleset_library: RulesetLibrary,
    sample_library_detail_response: dict[str, Any],
) -> None:
    list_response = {
        "count": 1,
        "next": None,
        "previous": None,
        "results": [
            {
                "id": LIBRARY_ID_1,
                "name": "test_library",
                "namespace": "test_ns",
                "is_valid": "valid",
                "created": "2025-06-01T10:00:00Z",
                "modified": "2025-06-01T10:00:00Z",
            },
        ],
    }
    detail_response = {
        "id": LIBRARY_ID_1,
        "name": "test_library",
        "namespace": "test_ns",
        "config_yaml": "version: '1.0'\nfunctions: []",
        "is_valid": "valid",
        "created": "2025-06-01T10:00:00Z",
        "modified": "2025-06-01T10:00:00Z",
    }
    update_response = {
        "id": LIBRARY_ID_1,
        "name": "test_library",
        "namespace": "test_ns",
        "config_yaml": "version: '1.0'\nfunctions: []",
        "is_valid": "unknown",
        "created": "2025-06-01T10:00:00Z",
        "modified": "2025-06-02T10:00:00Z",
    }

    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/ruleset-libraries/",
            json=list_response,
            status_code=200,
        )
        m.get(
            f"http://test-server/api/ruleset-libraries/{LIBRARY_ID_1}/",
            json=detail_response,
            status_code=200,
        )
        m.put(
            f"http://test-server/api/ruleset-libraries/{LIBRARY_ID_1}/",
            json=update_response,
            status_code=200,
        )
        result = client.create_or_update_ruleset_library(ruleset_library)

    assert result.id == RulesetLibraryId(LIBRARY_ID_1)
    # Should have called GET (list), GET (detail), then PUT (update)
    assert m.request_history[0].method == "GET"
    assert m.request_history[1].method == "GET"
    assert m.request_history[2].method == "PUT"


def test_delete_ruleset_library_by_id(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.delete(
            f"http://test-server/api/ruleset-libraries/{LIBRARY_ID_1}/",
            status_code=204,
        )
        client.delete_ruleset_library_by_id_if_exists(RulesetLibraryId(LIBRARY_ID_1))

    assert m.call_count == 1


def test_delete_ruleset_library_by_id_not_found(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.delete(
            f"http://test-server/api/ruleset-libraries/{LIBRARY_ID_1}/",
            status_code=404,
        )
        # Should not raise
        client.delete_ruleset_library_by_id_if_exists(RulesetLibraryId(LIBRARY_ID_1))


def test_delete_ruleset_library_by_id_force(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.delete(
            f"http://test-server/api/ruleset-libraries/{LIBRARY_ID_1}/",
            status_code=204,
        )
        client.delete_ruleset_library_by_id_if_exists(RulesetLibraryId(LIBRARY_ID_1), force=True)

    assert "force=true" in m.last_request.url


def test_delete_ruleset_library_by_name(client: DataMasqueClient, sample_library_list_response: dict[str, Any]) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/ruleset-libraries/",
            json=sample_library_list_response,
            status_code=200,
        )
        m.delete(
            f"http://test-server/api/ruleset-libraries/{LIBRARY_ID_1}/",
            status_code=204,
        )
        client.delete_ruleset_library_by_name_if_exists("my_library", "org")

    assert m.request_history[0].method == "GET"
    assert m.request_history[1].method == "DELETE"


def test_delete_ruleset_library_by_name_not_found(
    client: DataMasqueClient, sample_library_list_response: dict[str, Any]
) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/ruleset-libraries/",
            json=sample_library_list_response,
            status_code=200,
        )
        client.delete_ruleset_library_by_name_if_exists("nonexistent")

    # Only the list call should have been made, no DELETE
    assert m.call_count == 1
    assert m.request_history[0].method == "GET"


def test_validate_ruleset_library(client: DataMasqueClient) -> None:
    validate_response = {
        "id": LIBRARY_ID_1,
        "name": "my_library",
        "namespace": "org",
        "config_yaml": "version: '1.0'",
        "is_valid": "unknown",
        "created": "2025-01-01T12:00:00Z",
        "modified": "2025-06-03T10:00:00Z",
    }

    with requests_mock.Mocker() as m:
        m.patch(
            f"http://test-server/api/ruleset-libraries/{LIBRARY_ID_1}/",
            json=validate_response,
            status_code=200,
        )
        result = client.validate_ruleset_library(RulesetLibraryId(LIBRARY_ID_1))

    assert result.id == RulesetLibraryId(LIBRARY_ID_1)
    assert result.is_valid is ValidationStatus.unknown
    assert m.last_request.json() == {}


def test_list_rulesets_using_library(client: DataMasqueClient) -> None:
    rulesets_response = {
        "count": 2,
        "next": None,
        "previous": None,
        "results": [
            {
                "id": "eeeeeeee-1111-2222-3333-444444444444",
                "name": "ruleset_a",
                "mask_type": "database",
                "is_valid": "valid",
                "created": "2025-01-01T12:00:00Z",
                "modified": "2025-01-02T12:00:00Z",
            },
            {
                "id": "ffffffff-1111-2222-3333-444444444444",
                "name": "ruleset_b",
                "mask_type": "file",
                "is_valid": "unknown",
                "created": "2025-02-01T12:00:00Z",
                "modified": "2025-02-02T12:00:00Z",
            },
        ],
    }

    with requests_mock.Mocker() as m:
        m.get(
            f"http://test-server/api/ruleset-libraries/{LIBRARY_ID_1}/rulesets/",
            json=rulesets_response,
            status_code=200,
        )
        rulesets = client.list_rulesets_using_library(RulesetLibraryId(LIBRARY_ID_1))

    assert len(rulesets) == 2
    assert rulesets[0].name == "ruleset_a"
    assert rulesets[0].id == "eeeeeeee-1111-2222-3333-444444444444"
    assert rulesets[0].ruleset_type is RulesetType.database
    assert rulesets[0].yaml == ""
    assert rulesets[0].is_valid is ValidationStatus.valid
    assert rulesets[1].name == "ruleset_b"
    assert rulesets[1].ruleset_type is RulesetType.file
    assert rulesets[1].is_valid is ValidationStatus.unknown


def test_list_rulesets_using_library_empty(client: DataMasqueClient) -> None:
    empty_response = {"count": 0, "next": None, "previous": None, "results": []}

    with requests_mock.Mocker() as m:
        m.get(
            f"http://test-server/api/ruleset-libraries/{LIBRARY_ID_1}/rulesets/",
            json=empty_response,
            status_code=200,
        )
        rulesets = client.list_rulesets_using_library(RulesetLibraryId(LIBRARY_ID_1))

    assert rulesets == []


def test_list_rulesets_using_library_pagination(client: DataMasqueClient) -> None:
    page1 = {
        "count": 3,
        "next": "http://test-server/api/ruleset-libraries/{}/rulesets/?limit=2&offset=2".format(LIBRARY_ID_1),
        "previous": None,
        "results": [
            {
                "id": "aaa",
                "name": "r1",
                "mask_type": "database",
                "is_valid": "valid",
                "created": "2025-01-01T12:00:00Z",
                "modified": "2025-01-01T12:00:00Z",
            },
            {
                "id": "bbb",
                "name": "r2",
                "mask_type": "database",
                "is_valid": "valid",
                "created": "2025-01-01T12:00:00Z",
                "modified": "2025-01-01T12:00:00Z",
            },
        ],
    }
    page2 = {
        "count": 3,
        "next": None,
        "previous": None,
        "results": [
            {
                "id": "ccc",
                "name": "r3",
                "mask_type": "file",
                "is_valid": "unknown",
                "created": "2025-01-01T12:00:00Z",
                "modified": "2025-01-01T12:00:00Z",
            },
        ],
    }

    with requests_mock.Mocker() as m:
        m.get(
            f"http://test-server/api/ruleset-libraries/{LIBRARY_ID_1}/rulesets/",
            [{"json": page1, "status_code": 200}, {"json": page2, "status_code": 200}],
        )
        rulesets = client.list_rulesets_using_library(RulesetLibraryId(LIBRARY_ID_1))

    assert len(rulesets) == 3
    assert rulesets[0].name == "r1"
    assert rulesets[1].name == "r2"
    assert rulesets[2].name == "r3"
