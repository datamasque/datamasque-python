"""Tests for the client's `default_project`, which a create uses when the resource names no project."""

from collections.abc import Callable
from typing import Any, Optional

import pytest
import requests_mock

from datamasque.client import DataMasqueClient
from datamasque.client.models.connection import ConnectionId
from datamasque.client.models.discovery_config import DiscoveryConfig
from datamasque.client.models.discovery_config_library import DiscoveryConfigLibrary
from datamasque.client.models.dm_instance import DataMasqueInstanceConfig
from datamasque.client.models.rg_config import RGConfig
from datamasque.client.models.ruleset import Ruleset, RulesetType
from datamasque.client.models.ruleset_library import RulesetLibrary
from datamasque.client.models.table_reference import TableReference
from tests.helpers import active_projects_page, database_connection_config

BASE_URL = "http://test-server"


def make_client(default_project: Optional[str] = None) -> DataMasqueClient:
    return DataMasqueClient(
        DataMasqueInstanceConfig(base_url=BASE_URL, username="u", password="p", default_project=default_project)
    )


def create_ruleset(client: DataMasqueClient, project: Optional[str], m: requests_mock.Mocker) -> Any:
    m.post(f"{BASE_URL}/api/rulesets/?upsert=true", json={"id": "r-1", "name": "r", "project": "2-acme"})
    ruleset = Ruleset(name="r", yaml="version: '1.0'", ruleset_type=RulesetType.database, project=project)
    return client.create_or_update_ruleset(ruleset)


def create_connection(client: DataMasqueClient, project: Optional[str], m: requests_mock.Mocker) -> Any:
    m.get(f"{BASE_URL}/api/connections/", json=[])
    m.post(f"{BASE_URL}/api/connections/", json={"id": "c-1", "project": "2-acme"})
    connection = database_connection_config()
    connection.project = project
    return client.create_or_update_connection(connection)


def create_discovery_config(client: DataMasqueClient, project: Optional[str], m: requests_mock.Mocker) -> Any:
    m.post(
        f"{BASE_URL}/api/discovery/configs/",
        json={"id": "dc-1", "name": "dc", "config_type": "database", "project": "2-acme"},
    )
    config = DiscoveryConfig(name="dc", yaml="labels: []\n", config_type="database", project=project)
    return client.create_discovery_config(config)


def create_rg_config(client: DataMasqueClient, project: Optional[str], m: requests_mock.Mocker) -> Any:
    m.post(f"{BASE_URL}/api/ruleset-generation-configs/", json={"id": "rg-1", "name": "rg", "project": "2-acme"})
    return client.create_rg_config(RGConfig(name="rg", yaml="x: 1\n", project=project))


def create_table_reference(client: DataMasqueClient, project: Optional[str], m: requests_mock.Mocker) -> Any:
    m.post(
        f"{BASE_URL}/api/table-references/",
        json={"id": "tr-1", "name": "tr", "connection": "c-1", "source": "people.csv", "project": "2-acme"},
    )
    table_reference = TableReference(name="tr", connection=ConnectionId("c-1"), source="people.csv", project=project)
    return client.create_table_reference(table_reference)


def create_ruleset_library(client: DataMasqueClient, project: Optional[str], m: requests_mock.Mocker) -> Any:
    m.post(f"{BASE_URL}/api/ruleset-libraries/", json={"id": "rl-1", "name": "rl", "project": "2-acme"})
    return client.create_ruleset_library(RulesetLibrary(name="rl", yaml="version: '1.0'\n", project=project))


def create_discovery_config_library(client: DataMasqueClient, project: Optional[str], m: requests_mock.Mocker) -> Any:
    m.post(f"{BASE_URL}/api/discovery/config-libraries/", json={"id": "dl-1", "name": "dl", "project": "2-acme"})
    return client.create_discovery_config_library(
        DiscoveryConfigLibrary(name="dl", yaml="labels: []\n", project=project)
    )


Create = Callable[[DataMasqueClient, Optional[str], requests_mock.Mocker], Any]

CREATES = [
    pytest.param(create_ruleset, id="ruleset"),
    pytest.param(create_connection, id="connection"),
    pytest.param(create_discovery_config, id="discovery_config"),
    pytest.param(create_rg_config, id="rg_config"),
    pytest.param(create_table_reference, id="table_reference"),
    pytest.param(create_ruleset_library, id="ruleset_library"),
    pytest.param(create_discovery_config_library, id="discovery_config_library"),
]


@pytest.mark.parametrize("create", CREATES)
def test_a_create_sends_the_resources_project_over_the_default(create: Create) -> None:
    with requests_mock.Mocker() as m:
        create(make_client(default_project="1-default"), "2-acme", m)

        assert m.last_request.json()["project"] == "2-acme"


@pytest.mark.parametrize("create", CREATES)
def test_a_create_sends_the_default_project_when_the_resource_names_none(create: Create) -> None:
    with requests_mock.Mocker() as m:
        create(make_client(default_project="1-default"), None, m)

        assert m.last_request.json()["project"] == "1-default"
        assert all(request.path != "/api/projects/" for request in m.request_history)


@pytest.mark.parametrize("create", CREATES)
def test_a_create_with_no_default_project_sends_no_project(create: Create) -> None:
    with requests_mock.Mocker() as m:
        m.get(f"{BASE_URL}/api/projects/", json=active_projects_page(1))
        create(make_client(), None, m)

        assert "project" not in m.last_request.json()


def test_a_connection_update_sends_no_project() -> None:
    """An update leaves the connection in its own project, whatever the default."""
    client = make_client(default_project="1-default")
    connection = database_connection_config()
    connection.project = None
    with requests_mock.Mocker() as m:
        m.get(f"{BASE_URL}/api/connections/", json=[{**connection.model_dump(mode="json"), "id": "c-1"}])
        m.put(f"{BASE_URL}/api/connections/c-1/", json={"id": "c-1", "project": "2-acme"})

        client.create_or_update_connection(connection)

        assert m.last_request.method == "PUT"
        assert "project" not in m.last_request.json()


def test_an_upsert_looks_for_the_name_in_the_default_project() -> None:
    with requests_mock.Mocker() as m:
        create_connection(make_client(default_project="1-default"), None, m)

        lookup = m.request_history[0]
        assert (lookup.method, lookup.path, lookup.qs) == ("GET", "/api/connections/", {"project": ["1-default"]})
