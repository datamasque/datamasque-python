"""Tests for Git push, pull and file listing with the DataMasque client."""

from collections.abc import Callable
from datetime import datetime, timezone

import pytest
import requests_mock
from pydantic import ValidationError

from datamasque.client import (
    DataMasqueApiError,
    DataMasqueClient,
    DiscoveryConfig,
    DiscoveryConfigLibrary,
    DiscoveryConfigType,
    GitFile,
    Ruleset,
    RulesetLibrary,
    RulesetType,
)
from datamasque.client.models.dm_instance import DataMasqueInstanceConfig

BASE_URL = "http://test-server"
COMMIT_SHA = "0123456789abcdef0123456789abcdef01234567"
COMMIT = {"commit": COMMIT_SHA, "date": "2026-10-01T10:00:00+00:00", "message": "Add customers"}

LIST_METHODS: list[tuple[Callable[[DataMasqueClient], list[GitFile]], str, str]] = [
    (lambda client: client.list_ruleset_git_files(), "/api/ruleset-git/files/", "rulesets/Banking/customers.yml"),
    (lambda client: client.list_ruleset_library_git_files(), "/api/library-git/files/", "Banking/emails.yml"),
    (
        lambda client: client.list_discovery_config_git_files(),
        "/api/discovery-config-git/files/",
        "Banking/customers.yml",
    ),
    (
        lambda client: client.list_discovery_config_library_git_files(),
        "/api/discovery-config-library-git/files/",
        "Banking/emails.yml",
    ),
]


@pytest.mark.parametrize(("list_files", "endpoint", "project_file"), LIST_METHODS)
def test_list_git_files_returns_each_file_with_its_commits(
    client: DataMasqueClient,
    list_files: Callable[[DataMasqueClient], list[GitFile]],
    endpoint: str,
    project_file: str,
) -> None:
    with requests_mock.Mocker(case_sensitive=True) as m:
        m.get(f"{BASE_URL}{endpoint}", json={project_file: [COMMIT], "legacy.yml": []})

        files = list_files(client)

    assert m.last_request.qs == {}
    assert [file.path for file in files] == [project_file, "legacy.yml"]
    assert files[0].commits[0].commit_sha == COMMIT_SHA
    assert files[0].commits[0].date == datetime(2026, 10, 1, 10, tzinfo=timezone.utc)
    assert files[0].commits[0].message == "Add customers"
    assert files[1].commits == []


def test_list_git_files_sends_the_branch(client: DataMasqueClient) -> None:
    with requests_mock.Mocker(case_sensitive=True) as m:
        m.get(f"{BASE_URL}/api/ruleset-git/files/", json={})

        assert client.list_ruleset_git_files(branch="Feature/X") == []

    assert m.last_request.qs == {"branch": ["Feature/X"]}


@pytest.mark.parametrize(
    ("path", "folder", "name", "extension"),
    [
        ("rulesets/Banking/Customers.yml", "rulesets/Banking", "Customers", ".yml"),
        ("Customers.yaml", "", "Customers", ".yaml"),
        ("rulesets/Banking/v1.customers.yml", "rulesets/Banking", "v1.customers", ".yml"),
    ],
)
def test_pull_ruleset_from_git_splits_the_listed_path(
    client: DataMasqueClient, path: str, folder: str, name: str, extension: str
) -> None:
    with requests_mock.Mocker(case_sensitive=True) as m:
        m.get(f"{BASE_URL}/api/ruleset-git/", json={"config_yaml": "version: '1.0'"})

        content = client.pull_ruleset_from_git(path, COMMIT_SHA, branch="main")

    assert content == "version: '1.0'"
    assert m.last_request.qs == {
        "ruleset_name": [name],
        "extension": [extension],
        "folder": [folder],
        "commit_id": [COMMIT_SHA],
        "branch": ["main"],
    }


@pytest.mark.parametrize(
    ("pull", "endpoint", "name_field", "folder_param"),
    [
        (DataMasqueClient.pull_ruleset_library_from_git, "/api/library-git/", "library_name", "namespace"),
        (DataMasqueClient.pull_discovery_config_from_git, "/api/discovery-config-git/", "config_name", "folder"),
        (
            DataMasqueClient.pull_discovery_config_library_from_git,
            "/api/discovery-config-library-git/",
            "library_name",
            "namespace",
        ),
    ],
)
def test_pull_from_git_sends_each_type_s_fields(
    client: DataMasqueClient,
    pull: Callable[[DataMasqueClient, str, str], str],
    endpoint: str,
    name_field: str,
    folder_param: str,
) -> None:
    with requests_mock.Mocker(case_sensitive=True) as m:
        m.get(f"{BASE_URL}{endpoint}", json={"config_yaml": "libraries: []"})

        assert pull(client, "Bank/AU/Emails.yml", COMMIT_SHA) == "libraries: []"

    assert m.last_request.qs == {
        name_field: ["Emails"],
        "extension": [".yml"],
        folder_param: ["Bank/AU"],
        "commit_id": [COMMIT_SHA],
    }


def test_push_ruleset_to_git_sends_the_ruleset(client: DataMasqueClient) -> None:
    ruleset = Ruleset(name="customers", project="7-banking", yaml="version: '1.0'", ruleset_type=RulesetType.file)
    ruleset.id = "a-ruleset-id"
    with requests_mock.Mocker(case_sensitive=True) as m:
        m.post(f"{BASE_URL}/api/ruleset-git/", status_code=204)

        client.push_ruleset_to_git(ruleset, "Update customers", branch="main", extension=".yaml")

    assert m.last_request.json() == {
        "ruleset_name": "customers",
        "mask_type": "file",
        "id": "a-ruleset-id",
        "project": "7-banking",
        "ruleset_content": "version: '1.0'",
        "commit_message": "Update customers",
        "extension": ".yaml",
        "branch": "main",
    }


def test_push_and_push_path_leave_a_saved_resource_s_unset_project_to_the_server() -> None:
    client = DataMasqueClient(
        DataMasqueInstanceConfig(base_url=BASE_URL, username="u", password="p", default_project="1-default")
    )
    library = RulesetLibrary(name="emails", yaml="libraries: []")
    library.id = "a-library-id"
    with requests_mock.Mocker(case_sensitive=True) as m:
        m.post(f"{BASE_URL}/api/library-git/", status_code=204)
        m.get(f"{BASE_URL}/api/library-git/push-path/", json={"path": "masking/libraries/Banking/emails"})

        client.push_ruleset_library_to_git(library, "Add emails")
        assert client.get_ruleset_library_git_push_path(library) == "masking/libraries/Banking/emails"

    push, push_path = m.request_history
    assert push_path.qs == {"library_name": ["emails"], "id": ["a-library-id"]}
    assert push.json() == {
        "library_name": "emails",
        "id": "a-library-id",
        "library_content": "libraries: []",
        "commit_message": "Add emails",
        "extension": ".yml",
    }


def test_push_and_push_path_send_the_default_project_for_an_unsaved_resource_with_no_project() -> None:
    client = DataMasqueClient(
        DataMasqueInstanceConfig(base_url=BASE_URL, username="u", password="p", default_project="1-default")
    )
    library = DiscoveryConfigLibrary(name="emails", yaml="x: 1")
    with requests_mock.Mocker(case_sensitive=True) as m:
        m.post(f"{BASE_URL}/api/discovery-config-library-git/", status_code=204)
        m.get(
            f"{BASE_URL}/api/discovery-config-library-git/push-path/",
            json={"path": "masking/discovery-config-libraries/Default/emails"},
        )

        client.push_discovery_config_library_to_git(library, "Add emails")
        client.get_discovery_config_library_git_push_path(library)

    push, push_path = m.request_history
    assert push_path.qs == {"library_name": ["emails"], "project": ["1-default"]}
    assert push.json() == {
        "library_name": "emails",
        "project": "1-default",
        "library_content": "x: 1",
        "commit_message": "Add emails",
        "extension": ".yml",
    }


def test_push_and_push_path_send_no_project_when_neither_the_resource_nor_the_client_sets_one(
    client: DataMasqueClient,
) -> None:
    ruleset = Ruleset(name="customers", yaml="version: '1.0'")
    with requests_mock.Mocker(case_sensitive=True) as m:
        m.post(f"{BASE_URL}/api/ruleset-git/", status_code=204)
        m.get(f"{BASE_URL}/api/ruleset-git/push-path/", json={"path": "masking/rulesets/Banking/customers"})

        client.push_ruleset_to_git(ruleset, "Add customers")
        client.get_ruleset_git_push_path(ruleset)

    push, push_path = m.request_history
    assert "project" not in push.json()
    assert "project" not in push_path.qs


def test_push_discovery_config_to_git_sends_its_type(client: DataMasqueClient) -> None:
    config = DiscoveryConfig(name="customers", project="7-banking", yaml="x: 1", config_type=DiscoveryConfigType.file)
    with requests_mock.Mocker(case_sensitive=True) as m:
        m.post(f"{BASE_URL}/api/discovery-config-git/", status_code=204)

        client.push_discovery_config_to_git(config, "Add customers")

    assert m.last_request.json() == {
        "config_name": "customers",
        "config_type": "file",
        "project": "7-banking",
        "config_content": "x: 1",
        "commit_message": "Add customers",
        "extension": ".yml",
    }


@pytest.mark.parametrize(
    "push",
    [
        lambda client: client.push_ruleset_to_git(Ruleset(name="customers", project="7-banking"), "Add customers"),
        lambda client: client.push_ruleset_library_to_git(RulesetLibrary(name="emails", project="7-banking"), "Add"),
        lambda client: client.push_discovery_config_to_git(
            DiscoveryConfig(name="customers", project="7-banking", config_type=DiscoveryConfigType.database), "Add"
        ),
        lambda client: client.push_discovery_config_library_to_git(
            DiscoveryConfigLibrary(name="emails", project="7-banking", yaml=""), "Add"
        ),
    ],
)
def test_push_without_yaml_raises(client: DataMasqueClient, push: Callable[[DataMasqueClient], None]) -> None:
    with requests_mock.Mocker(case_sensitive=True) as m:
        with pytest.raises(ValueError, match="yaml is empty or unset"):
            push(client)

    assert m.request_history == []


def test_push_raises_on_a_path_conflict(client: DataMasqueClient) -> None:
    with requests_mock.Mocker(case_sensitive=True) as m:
        m.post(f"{BASE_URL}/api/ruleset-git/", status_code=409, json={"detail": "Cannot push"})

        with pytest.raises(DataMasqueApiError) as error:
            client.push_ruleset_to_git(
                Ruleset(name="customers", project="7-banking", yaml="version: '1.0'"), "Add customers"
            )

    assert error.value.response.status_code == 409


@pytest.mark.parametrize(
    ("get_push_path", "endpoint", "expected_params"),
    [
        (
            lambda client: client.get_ruleset_git_push_path(Ruleset(name="customers", project="7-banking")),
            "/api/ruleset-git/push-path/",
            {"ruleset_name": ["customers"], "mask_type": ["database"], "project": ["7-banking"]},
        ),
        (
            lambda client: client.get_ruleset_library_git_push_path(RulesetLibrary(name="emails", project="7-banking")),
            "/api/library-git/push-path/",
            {"library_name": ["emails"], "project": ["7-banking"]},
        ),
        (
            lambda client: client.get_discovery_config_git_push_path(
                DiscoveryConfig(name="customers", project="7-banking", config_type=DiscoveryConfigType.database)
            ),
            "/api/discovery-config-git/push-path/",
            {"config_name": ["customers"], "config_type": ["database"], "project": ["7-banking"]},
        ),
        (
            lambda client: client.get_discovery_config_library_git_push_path(
                DiscoveryConfigLibrary(name="emails", project="7-banking")
            ),
            "/api/discovery-config-library-git/push-path/",
            {"library_name": ["emails"], "project": ["7-banking"]},
        ),
    ],
)
def test_get_git_push_path_returns_the_server_s_path(
    client: DataMasqueClient,
    get_push_path: Callable[[DataMasqueClient], str],
    endpoint: str,
    expected_params: dict[str, list[str]],
) -> None:
    with requests_mock.Mocker(case_sensitive=True) as m:
        m.get(f"{BASE_URL}{endpoint}", json={"path": "masking/rulesets/Banking/customers"})

        assert get_push_path(client) == "masking/rulesets/Banking/customers"

    assert m.last_request.qs == expected_params


@pytest.mark.parametrize(
    ("call", "endpoint", "response"),
    [
        (
            lambda client: client.pull_ruleset_from_git("rulesets/Banking/customers.yml", COMMIT_SHA),
            "/api/ruleset-git/",
            {"config_yaml": None},
        ),
        (
            lambda client: client.get_ruleset_git_push_path(Ruleset(name="customers", project="7-banking")),
            "/api/ruleset-git/push-path/",
            {},
        ),
    ],
)
def test_a_malformed_git_response_raises(
    client: DataMasqueClient, call: Callable[[DataMasqueClient], str], endpoint: str, response: dict[str, object]
) -> None:
    with requests_mock.Mocker(case_sensitive=True) as m:
        m.get(f"{BASE_URL}{endpoint}", json=response)

        with pytest.raises(ValidationError):
            call(client)
