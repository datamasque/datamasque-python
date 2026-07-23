"""Tests for table reference support in the DataMasque client."""

from datetime import datetime
from typing import Any

import pytest
import requests_mock
from pydantic import ValidationError

from datamasque.client import (
    ConnectionId,
    DataMasqueClient,
    TableReference,
    TableReferenceFormat,
    TableReferenceId,
    TableReferenceOptions,
)
from datamasque.client.exceptions import DataMasqueException
from tests.helpers import file_connection_config

TABLE_REFERENCE_ID_1 = "aaaaaaaa-1111-2222-3333-444444444444"
TABLE_REFERENCE_ID_2 = "bbbbbbbb-1111-2222-3333-444444444444"
CONNECTION_ID_1 = "cccccccc-1111-2222-3333-444444444444"
CONNECTION_ID_2 = "dddddddd-1111-2222-3333-444444444444"

DEFAULT_OPTIONS_JSON = {
    "format": "csv",
    "delimiter": ",",
    "encoding": "utf-8",
    "quotechar": '"',
    "null_string": None,
}


@pytest.fixture
def sample_table_reference_list_response() -> list[dict[str, Any]]:
    """List response: a bare (unpaginated) array of full entries."""
    return [
        {
            "id": TABLE_REFERENCE_ID_1,
            "name": "customer_identities",
            "connection": CONNECTION_ID_1,
            "source": "identities/customers.csv",
            "options": DEFAULT_OPTIONS_JSON,
            "created": "2025-01-01T12:00:00Z",
            "modified": "2025-01-02T12:00:00Z",
        },
        {
            "id": TABLE_REFERENCE_ID_2,
            "name": "account_identities",
            "connection": CONNECTION_ID_2,
            "source": "OPS.ACCOUNTS",
            "options": {
                "format": "parquet",
                "delimiter": ";",
                "encoding": "latin-1",
                "quotechar": "'",
                "null_string": "NULL",
            },
            "created": "2025-02-01T12:00:00Z",
            "modified": "2025-02-02T12:00:00Z",
        },
    ]


@pytest.fixture
def table_reference() -> TableReference:
    return TableReference(
        name="customer_identities",
        connection=ConnectionId(CONNECTION_ID_1),
        source="identities/customers.csv",
    )


def test_name_connection_and_source_are_required_on_the_model() -> None:
    with pytest.raises(ValidationError, match="source"):
        TableReference(name="customer_identities", connection=ConnectionId(CONNECTION_ID_1))


def test_options_default_to_the_servers_defaults() -> None:
    options = TableReferenceOptions()

    assert options.format is TableReferenceFormat.csv
    assert options.delimiter == ","
    assert options.encoding == "utf-8"
    assert options.quotechar == '"'
    assert options.null_string is None


def test_connection_accepts_a_connection_config() -> None:
    connection = file_connection_config()
    connection.id = ConnectionId(CONNECTION_ID_1)

    reference = TableReference(name="customer_identities", connection=connection, source="customers.csv")

    assert reference.connection == ConnectionId(CONNECTION_ID_1)
    assert reference.connection_id == ConnectionId(CONNECTION_ID_1)


def test_connection_config_without_an_id_is_rejected() -> None:
    with pytest.raises(ValidationError, match="id is None"):
        TableReference(name="customer_identities", connection=file_connection_config(), source="customers.csv")


def test_list_table_references(
    client: DataMasqueClient, sample_table_reference_list_response: list[dict[str, Any]]
) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/table-references/",
            json=sample_table_reference_list_response,
            status_code=200,
        )
        references = client.list_table_references()

    assert len(references) == 2
    assert references[0].id == TableReferenceId(TABLE_REFERENCE_ID_1)
    assert references[0].name == "customer_identities"
    assert references[0].connection == ConnectionId(CONNECTION_ID_1)
    assert references[0].source == "identities/customers.csv"
    assert references[0].options is not None
    assert references[0].options.format is TableReferenceFormat.csv
    assert references[0].created == datetime.fromisoformat("2025-01-01T12:00:00+00:00")
    assert references[1].id == TableReferenceId(TABLE_REFERENCE_ID_2)
    assert references[1].source == "OPS.ACCOUNTS"
    assert references[1].options is not None
    assert references[1].options.format is TableReferenceFormat.parquet
    assert references[1].options.delimiter == ";"
    assert references[1].options.encoding == "latin-1"
    assert references[1].options.quotechar == "'"
    assert references[1].options.null_string == "NULL"


def test_list_table_references_empty(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/table-references/",
            json=[],
            status_code=200,
        )
        references = client.list_table_references()

    assert references == []


def test_get_table_reference(
    client: DataMasqueClient, sample_table_reference_list_response: list[dict[str, Any]]
) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            f"http://test-server/api/table-references/{TABLE_REFERENCE_ID_1}/",
            json=sample_table_reference_list_response[0],
            status_code=200,
        )
        reference = client.get_table_reference(TableReferenceId(TABLE_REFERENCE_ID_1))

    assert reference.id == TableReferenceId(TABLE_REFERENCE_ID_1)
    assert reference.name == "customer_identities"
    assert reference.source == "identities/customers.csv"


def test_get_table_reference_by_name_found(
    client: DataMasqueClient, sample_table_reference_list_response: list[dict[str, Any]]
) -> None:
    """The list response carries every field, so a by-name lookup needs no follow-up detail request."""
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/table-references/",
            json=sample_table_reference_list_response,
            status_code=200,
        )
        reference = client.get_table_reference_by_name("account_identities")

    assert reference is not None
    assert reference.id == TableReferenceId(TABLE_REFERENCE_ID_2)
    assert m.call_count == 1


def test_get_table_reference_by_name_not_found(
    client: DataMasqueClient, sample_table_reference_list_response: list[dict[str, Any]]
) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/table-references/",
            json=sample_table_reference_list_response,
            status_code=200,
        )
        reference = client.get_table_reference_by_name("nonexistent")

    assert reference is None


def test_create_table_reference(client: DataMasqueClient, table_reference: TableReference) -> None:
    create_response = {
        "id": TABLE_REFERENCE_ID_1,
        "name": "customer_identities",
        "connection": CONNECTION_ID_1,
        "source": "identities/customers.csv",
        "options": DEFAULT_OPTIONS_JSON,
        "created": "2025-06-01T10:00:00Z",
        "modified": "2025-06-01T10:00:00Z",
    }

    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/table-references/",
            json=create_response,
            status_code=201,
        )
        result = client.create_table_reference(table_reference)

    assert result is table_reference
    assert result.id == TableReferenceId(TABLE_REFERENCE_ID_1)
    assert result.created == datetime.fromisoformat("2025-06-01T10:00:00+00:00")
    assert result.modified == datetime.fromisoformat("2025-06-01T10:00:00+00:00")
    # The server fills in the options it stored, even though none were sent.
    assert result.options is not None
    assert result.options.format is TableReferenceFormat.csv

    request_body = m.last_request.json()
    assert request_body["name"] == "customer_identities"
    assert request_body["connection"] == CONNECTION_ID_1
    assert request_body["source"] == "identities/customers.csv"
    assert "options" not in request_body
    read_only_fields = {"id", "created", "modified"}
    assert not read_only_fields & request_body.keys()


def test_create_table_reference_sends_options(client: DataMasqueClient) -> None:
    reference = TableReference(
        name="customer_identities",
        connection=ConnectionId(CONNECTION_ID_1),
        source="identities/customers.csv",
        options=TableReferenceOptions(format=TableReferenceFormat.parquet, delimiter=";", null_string="NULL"),
    )
    create_response = {
        "id": TABLE_REFERENCE_ID_1,
        "name": "customer_identities",
        "connection": CONNECTION_ID_1,
        "source": "identities/customers.csv",
        "options": {
            "format": "parquet",
            "delimiter": ";",
            "encoding": "utf-8",
            "quotechar": '"',
            "null_string": "NULL",
        },
        "created": "2025-06-01T10:00:00Z",
        "modified": "2025-06-01T10:00:00Z",
    }

    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/table-references/",
            json=create_response,
            status_code=201,
        )
        client.create_table_reference(reference)

    options = m.last_request.json()["options"]
    assert options["format"] == "parquet"
    assert options["delimiter"] == ";"
    assert options["null_string"] == "NULL"


def test_update_table_reference(client: DataMasqueClient, table_reference: TableReference) -> None:
    table_reference.id = TableReferenceId(TABLE_REFERENCE_ID_1)
    table_reference.source = "identities/customers_v2.csv"

    update_response = {
        "id": TABLE_REFERENCE_ID_1,
        "name": "customer_identities",
        "connection": CONNECTION_ID_1,
        "source": "identities/customers_v2.csv",
        "options": DEFAULT_OPTIONS_JSON,
        "created": "2025-06-01T10:00:00Z",
        "modified": "2025-06-02T10:00:00Z",
    }

    with requests_mock.Mocker() as m:
        m.put(
            f"http://test-server/api/table-references/{TABLE_REFERENCE_ID_1}/",
            json=update_response,
            status_code=200,
        )
        result = client.update_table_reference(table_reference)

    assert result is table_reference
    assert result.modified == datetime.fromisoformat("2025-06-02T10:00:00+00:00")

    request_body = m.last_request.json()
    assert request_body["source"] == "identities/customers_v2.csv"
    read_only_fields = {"id", "created", "modified"}
    assert not read_only_fields & request_body.keys()


def test_update_table_reference_without_options_sends_none(
    client: DataMasqueClient, table_reference: TableReference
) -> None:
    """Options are omitted entirely when unset, so the server keeps the ones it has stored."""
    table_reference.id = TableReferenceId(TABLE_REFERENCE_ID_1)

    update_response = {
        "id": TABLE_REFERENCE_ID_1,
        "name": "customer_identities",
        "connection": CONNECTION_ID_1,
        "source": "identities/customers.csv",
        "options": {**DEFAULT_OPTIONS_JSON, "delimiter": ";"},
        "created": "2025-06-01T10:00:00Z",
        "modified": "2025-06-02T10:00:00Z",
    }

    with requests_mock.Mocker() as m:
        m.put(
            f"http://test-server/api/table-references/{TABLE_REFERENCE_ID_1}/",
            json=update_response,
            status_code=200,
        )
        result = client.update_table_reference(table_reference)

    assert "options" not in m.last_request.json()
    # The server's stored options come back on the updated model.
    assert result.options is not None
    assert result.options.delimiter == ";"


def test_update_table_reference_no_id_raises(client: DataMasqueClient, table_reference: TableReference) -> None:
    with pytest.raises(ValueError, match="id is None"):
        client.update_table_reference(table_reference)


def test_create_or_update_table_reference_create(client: DataMasqueClient, table_reference: TableReference) -> None:
    create_response = {
        "id": TABLE_REFERENCE_ID_1,
        "name": "customer_identities",
        "connection": CONNECTION_ID_1,
        "source": "identities/customers.csv",
        "options": DEFAULT_OPTIONS_JSON,
        "created": "2025-06-01T10:00:00Z",
        "modified": "2025-06-01T10:00:00Z",
    }

    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/table-references/",
            json=[],
            status_code=200,
        )
        m.post(
            "http://test-server/api/table-references/",
            json=create_response,
            status_code=201,
        )
        result = client.create_or_update_table_reference(table_reference)

    assert result.id == TableReferenceId(TABLE_REFERENCE_ID_1)
    assert m.request_history[0].method == "GET"
    assert m.request_history[1].method == "POST"


def test_create_or_update_table_reference_update(
    client: DataMasqueClient,
    table_reference: TableReference,
    sample_table_reference_list_response: list[dict[str, Any]],
) -> None:
    update_response = {
        "id": TABLE_REFERENCE_ID_1,
        "name": "customer_identities",
        "connection": CONNECTION_ID_1,
        "source": "identities/customers.csv",
        "options": DEFAULT_OPTIONS_JSON,
        "created": "2025-01-01T12:00:00Z",
        "modified": "2025-06-02T10:00:00Z",
    }

    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/table-references/",
            json=sample_table_reference_list_response,
            status_code=200,
        )
        m.put(
            f"http://test-server/api/table-references/{TABLE_REFERENCE_ID_1}/",
            json=update_response,
            status_code=200,
        )
        result = client.create_or_update_table_reference(table_reference)

    assert result.id == TableReferenceId(TABLE_REFERENCE_ID_1)
    assert m.call_count == 2
    assert m.request_history[0].method == "GET"
    assert m.request_history[1].method == "PUT"


def test_create_or_update_table_reference_raises_when_server_omits_id(
    client: DataMasqueClient, table_reference: TableReference
) -> None:
    list_response_without_id = [
        {
            "name": "customer_identities",
            "connection": CONNECTION_ID_1,
            "source": "identities/customers.csv",
            "options": DEFAULT_OPTIONS_JSON,
            "created": "2025-01-01T12:00:00Z",
            "modified": "2025-01-02T12:00:00Z",
        },
    ]

    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/table-references/",
            json=list_response_without_id,
            status_code=200,
        )
        with pytest.raises(DataMasqueException, match="without an `id`"):
            client.create_or_update_table_reference(table_reference)


def test_delete_table_reference_by_id(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.delete(
            f"http://test-server/api/table-references/{TABLE_REFERENCE_ID_1}/",
            status_code=204,
        )
        client.delete_table_reference_by_id_if_exists(TableReferenceId(TABLE_REFERENCE_ID_1))

    assert m.call_count == 1


def test_delete_table_reference_by_id_not_found(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.delete(
            f"http://test-server/api/table-references/{TABLE_REFERENCE_ID_1}/",
            status_code=404,
        )
        client.delete_table_reference_by_id_if_exists(TableReferenceId(TABLE_REFERENCE_ID_1))


def test_delete_table_reference_by_name(
    client: DataMasqueClient, sample_table_reference_list_response: list[dict[str, Any]]
) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/table-references/",
            json=sample_table_reference_list_response,
            status_code=200,
        )
        m.delete(
            f"http://test-server/api/table-references/{TABLE_REFERENCE_ID_2}/",
            status_code=204,
        )
        client.delete_table_reference_by_name_if_exists("account_identities")

    assert m.call_count == 2
    assert m.request_history[0].method == "GET"
    assert m.request_history[1].method == "DELETE"
    assert TABLE_REFERENCE_ID_2 in m.request_history[1].url


def test_delete_table_reference_by_name_not_found(
    client: DataMasqueClient, sample_table_reference_list_response: list[dict[str, Any]]
) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/table-references/",
            json=sample_table_reference_list_response,
            status_code=200,
        )
        client.delete_table_reference_by_name_if_exists("nonexistent")

    assert m.call_count == 1
    assert m.request_history[0].method == "GET"
