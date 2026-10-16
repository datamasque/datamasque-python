"""Tests for the `project` a create or upload sends."""

from collections.abc import Callable
from typing import Any, Optional

import pytest
import requests_mock

from datamasque.client import DataMasqueClient
from datamasque.client.models.connection import ConnectionId
from datamasque.client.models.discovery_config import DiscoveryConfig
from datamasque.client.models.dm_instance import DataMasqueInstanceConfig
from datamasque.client.models.files import SeedFile
from datamasque.client.models.rg_config import RGConfig
from datamasque.client.models.ruleset import Ruleset, RulesetType
from datamasque.client.models.table_reference import TableReference
from tests.helpers import database_connection_config

BASE_URL = "http://test-server"
SEED_RESPONSE = {
    "id": "s-1",
    "name": "seed.csv",
    "created_date": "2026-01-01T00:00:00Z",
    "modified_date": "2026-01-01T00:00:00Z",
}


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


Create = Callable[[DataMasqueClient, Optional[str], requests_mock.Mocker], Any]

CREATES = [
    pytest.param(create_ruleset, id="ruleset"),
    pytest.param(create_connection, id="connection"),
    pytest.param(create_discovery_config, id="discovery_config"),
    pytest.param(create_rg_config, id="rg_config"),
    pytest.param(create_table_reference, id="table_reference"),
]


@pytest.mark.parametrize("create", CREATES)
def test_create_sends_the_models_project_over_the_default(create: Create) -> None:
    with requests_mock.Mocker() as m:
        create(make_client(default_project="1-default"), "2-acme", m)

        assert m.last_request.json()["project"] == "2-acme"


@pytest.mark.parametrize("create", CREATES)
def test_create_sends_the_default_project_when_the_model_has_none(create: Create) -> None:
    with requests_mock.Mocker() as m:
        create(make_client(default_project="1-default"), None, m)

        assert m.last_request.json()["project"] == "1-default"


@pytest.mark.parametrize("create", CREATES)
def test_create_sends_no_project_when_neither_is_set(create: Create) -> None:
    with requests_mock.Mocker() as m:
        create(make_client(), None, m)

        assert "project" not in m.last_request.json()


@pytest.mark.parametrize("create", CREATES)
def test_create_sets_the_project_the_server_returns(create: Create) -> None:
    with requests_mock.Mocker() as m:
        created = create(make_client(), None, m)

    assert created.project == "2-acme"


def test_connection_update_sends_no_project() -> None:
    client = make_client(default_project="1-default")
    connection = database_connection_config()
    with requests_mock.Mocker() as m:
        m.get(f"{BASE_URL}/api/connections/", json=[{**connection.model_dump(mode="json"), "id": "c-1"}])
        m.put(f"{BASE_URL}/api/connections/c-1/", json={"id": "c-1", "project": "2-acme"})

        client.create_or_update_connection(connection)

        assert m.last_request.method == "PUT"
        assert "project" not in m.last_request.json()


@pytest.mark.parametrize(
    ("default_project", "project", "expected"),
    [
        pytest.param(None, "2-acme", b'name="project"\r\n\r\n2-acme', id="explicit"),
        pytest.param("1-default", None, b'name="project"\r\n\r\n1-default', id="default"),
    ],
)
def test_upload_file_sends_the_project_form_field(
    default_project: Optional[str], project: Optional[str], expected: bytes
) -> None:
    client = make_client(default_project=default_project)
    with requests_mock.Mocker() as m:
        m.post(f"{BASE_URL}/{SeedFile.get_url()}", status_code=201, json=SEED_RESPONSE)

        client.upload_file(SeedFile, "seed.csv", b"value\n", project=project)

        assert expected in m.last_request.body


def test_upload_file_sends_no_project_when_neither_is_set() -> None:
    with requests_mock.Mocker() as m:
        m.post(f"{BASE_URL}/{SeedFile.get_url()}", status_code=201, json=SEED_RESPONSE)

        make_client().upload_file(SeedFile, "seed.csv", b"value\n")

        assert b'name="project"' not in m.last_request.body
