"""Tests for `DiscoveryClient` (schema discovery, ruleset generation, db-discovery report)."""

import zipfile
from io import BytesIO, StringIO
from unittest.mock import patch

import pytest
import requests_mock
from pydantic import ValidationError

from datamasque.client import (
    DataMasqueClient,
    DiscoveryConfig,
    DiscoveryConfigId,
    FileDataDiscoveryFromConfigRequest,
    FileDataDiscoveryOptions,
    FileDataDiscoveryRequest,
    FileDiscoveryResult,
    FileFilter,
    FileFilterMatchAgainst,
    FileRulesetGenerationRequest,
    InDataDiscoveryConfig,
    RulesetGenerationRequest,
    RunId,
    SchemaDiscoveryFromConfigRequest,
    SchemaDiscoveryPage,
    SchemaDiscoveryRequest,
    SchemaDiscoveryResult,
)
from datamasque.client.exceptions import (
    AsyncRulesetGenerationInProgressError,
    DataMasqueApiError,
    DataMasqueException,
    DiscoveryConfigNotFoundError,
    FailedToStartError,
    InvalidDiscoveryConfigError,
)
from datamasque.client.models.connection import ConnectionId, DatabaseConnectionConfig, DatabaseType
from datamasque.client.models.data_selection import SelectedColumns, SelectedFileData, UserSelection
from datamasque.client.models.status import AsyncRulesetGenerationTaskStatus
from tests.helpers import parse_multipart_form

DISCOVERY_CONFIG_ID = "aaaaaaaa-1111-2222-3333-444444444444"


def test_generate_ruleset(client):
    req = RulesetGenerationRequest(connection="conn-1", selected_columns={"public": {"users": ["email"]}})
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/generate-ruleset/v2/",
            content=b'version: "1.0"',
            status_code=201,
        )
        assert client.generate_ruleset(req) == 'version: "1.0"'


def test_generate_file_ruleset(client):
    req = FileRulesetGenerationRequest(
        connection="conn-1",
        selected_data=[UserSelection(locators=[["a"]], files=["f1.csv"])],
    )
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/generate-file-ruleset/",
            content=b'version: "1.0"',
            status_code=201,
        )
        assert client.generate_file_ruleset(req) == 'version: "1.0"'


def test_user_selection_accepts_mixed_locator_shapes():
    """Tabular columns use bare strings; JSON paths use list[str | int]. Both should round-trip through `model_dump`."""
    selection = UserSelection(
        files=["tabular.csv", "nested.json"],
        locators=[
            "email",
            "phone",
            ["employees", "*", "firstName"],
            ["items", 0, "sku"],
        ],
    )
    assert selection.model_dump(mode="json") == {
        "files": ["tabular.csv", "nested.json"],
        "locators": [
            "email",
            "phone",
            ["employees", "*", "firstName"],
            ["items", 0, "sku"],
        ],
    }


def test_get_db_discovery_result_report(client):
    run_id = RunId(1)
    include_selection_column = True
    with requests_mock.Mocker() as m:
        url = f"http://test-server/api/runs/{run_id}/db-discovery-results/report/"
        m.get(url, text="db discovery report", status_code=200)
        result = client.get_db_discovery_result_report(run_id, include_selection_column)
        assert result == "db discovery report"

    # Test without selection column
    include_selection_column = False
    with requests_mock.Mocker() as m:
        url = f"http://test-server/api/runs/{run_id}/db-discovery-results/report/?include_selection_column=false"
        m.get(url, text="db discovery report without selection column", status_code=200)
        result = client.get_db_discovery_result_report(run_id, include_selection_column)
        assert result == "db discovery report without selection column"


def test_get_db_discovery_result_report_returns_zip_bytes_when_split(client):
    run_id = RunId(1)
    zip_bytes = b"PK\x03\x04 split report zip bytes"
    with requests_mock.Mocker() as m:
        url = f"http://test-server/api/runs/{run_id}/db-discovery-results/report/"
        m.get(url, content=zip_bytes, headers={"X-DM-Download-Format": "zip"}, status_code=200)
        result = client.get_db_discovery_result_report(run_id)
        assert result == zip_bytes
        assert isinstance(result, bytes)


def test_poll_async_ruleset_generation(client):
    connection_id = ConnectionId("1")
    with requests_mock.Mocker() as m:
        # Test running status
        m.get(
            f"http://test-server/api/async-generate-ruleset/{connection_id}/",
            json={"status": "running"},
            status_code=200,
        )
        status = client.get_async_ruleset_generation_task_status(connection_id)
        assert status is AsyncRulesetGenerationTaskStatus.running

        # Test finished status
        m.get(
            f"http://test-server/api/async-generate-ruleset/{connection_id}/",
            json={"status": "finished"},
            status_code=200,
        )
        status = client.get_async_ruleset_generation_task_status(connection_id)
        assert status is AsyncRulesetGenerationTaskStatus.finished

        # Test failed status
        m.get(
            f"http://test-server/api/async-generate-ruleset/{connection_id}/",
            json={"status": "failed"},
            status_code=200,
        )
        status = client.get_async_ruleset_generation_task_status(connection_id)
        assert status is AsyncRulesetGenerationTaskStatus.failed


def test_get_generated_rulesets_success(client):
    connection_id = ConnectionId("1")
    yaml_content_1 = b"""
    version: "1.0"
    tasks:
    - type: mask_table
      table: table1
      key: id
      rules:
      - column: col1
        masks:
        - type: do_nothing
    """
    yaml_content_2 = b"""
    version: "1.0"
    tasks:
    - type: mask_table
      table: table2
      key: id
      rules:
      - column: col2
        masks:
        - type: do_nothing
    """

    with requests_mock.Mocker() as m:
        m.get(
            f"http://test-server/api/async-generate-ruleset/{connection_id}/",
            json={"status": "finished"},
            status_code=200,
        )

        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zip_file:
            zip_file.writestr("ruleset1.yml", yaml_content_1.decode("utf-8"))
            zip_file.writestr("ruleset2.yaml", yaml_content_2.decode("utf-8"))
        zip_buffer.seek(0)

        m.get(
            f"http://test-server/api/async-generate-ruleset/{connection_id}/download-rulesets/",
            content=zip_buffer.getvalue(),
            headers={"Content-Disposition": 'attachment; filename="rulesets.zip"'},
            status_code=200,
        )

        rulesets = client.get_generated_rulesets(connection_id)

        assert len(rulesets) == 2
        assert rulesets[0].name == "ruleset1"
        assert rulesets[0].yaml == yaml_content_1.decode("utf-8")
        assert rulesets[1].name == "ruleset2"
        assert rulesets[1].yaml == yaml_content_2.decode("utf-8")


def test_get_generated_rulesets_empty_archive_raises(client):
    """A finished task whose download archive contains no ruleset files raises a clear error."""
    connection_id = ConnectionId("1")

    with requests_mock.Mocker() as m:
        m.get(
            f"http://test-server/api/async-generate-ruleset/{connection_id}/",
            json={"status": "finished"},
            status_code=200,
        )

        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zip_file:
            zip_file.writestr("README.txt", "no rulesets here")
        zip_buffer.seek(0)

        m.get(
            f"http://test-server/api/async-generate-ruleset/{connection_id}/download-rulesets/",
            content=zip_buffer.getvalue(),
            headers={"Content-Disposition": 'attachment; filename="rulesets.zip"'},
            status_code=200,
        )

        with pytest.raises(DataMasqueException, match="contained no rulesets"):
            client.get_generated_rulesets(connection_id)


def test_get_generated_rulesets_from_selection_success(client):
    """Non-CSV async RG: server 303s to the task-status endpoint, whose JSON body carries `generated_ruleset`."""
    connection_id = ConnectionId("1")
    generated_yaml = 'version: "1.0"\ntasks:\n- type: mask_table\n  table: users\n  key: id\n  rules: []\n'

    with requests_mock.Mocker() as m:
        # Status check and the redirect target are the same URL; both resolve to the same JSON.
        m.get(
            f"http://test-server/api/async-generate-ruleset/{connection_id}/",
            json={"status": "finished", "generated_ruleset": generated_yaml},
            status_code=200,
        )
        m.get(
            f"http://test-server/api/async-generate-ruleset/{connection_id}/download-rulesets/",
            status_code=303,
            headers={"Location": f"http://test-server/api/async-generate-ruleset/{connection_id}/"},
        )

        rulesets = client.get_generated_rulesets(connection_id)

    assert len(rulesets) == 1
    assert rulesets[0].yaml == generated_yaml
    # The server doesn't return a name — callers set one before create_or_update_ruleset.
    assert rulesets[0].name == "generated_ruleset"


def test_get_generated_rulesets_from_selection_empty_ruleset_raises(client):
    """A finished task with no `generated_ruleset` in the JSON body raises a clear error."""
    connection_id = ConnectionId("1")
    with requests_mock.Mocker() as m:
        m.get(
            f"http://test-server/api/async-generate-ruleset/{connection_id}/",
            json={"status": "finished", "generated_ruleset": None},
            status_code=200,
        )
        m.get(
            f"http://test-server/api/async-generate-ruleset/{connection_id}/download-rulesets/",
            status_code=303,
            headers={"Location": f"http://test-server/api/async-generate-ruleset/{connection_id}/"},
        )

        with pytest.raises(DataMasqueException, match="no ruleset was returned"):
            client.get_generated_rulesets(connection_id)


def test_get_generated_rulesets_failed(client):
    connection_id = ConnectionId("1")

    with requests_mock.Mocker() as m:
        m.get(
            f"http://test-server/api/async-generate-ruleset/{connection_id}/",
            json={"status": "failed"},
            status_code=200,
        )

        with pytest.raises(DataMasqueException, match="Ruleset generation failed for connection"):
            client.get_generated_rulesets(connection_id)


def test_get_generated_rulesets_in_progress(client):
    connection_id = ConnectionId("1")

    with requests_mock.Mocker() as m:
        m.get(
            f"http://test-server/api/async-generate-ruleset/{connection_id}/",
            json={"status": "running"},
            status_code=200,
        )

        with pytest.raises(
            AsyncRulesetGenerationInProgressError,
            match="Ruleset generation in progress or not ready",
        ):
            client.get_generated_rulesets(connection_id)


def test_get_generated_rulesets_download_fail(client):
    connection_id = ConnectionId("1")

    with requests_mock.Mocker() as m:
        m.get(
            f"http://test-server/api/async-generate-ruleset/{connection_id}/",
            json={"status": "finished"},
            status_code=200,
        )

        m.get(
            f"http://test-server/api/async-generate-ruleset/{connection_id}/download-rulesets/",
            status_code=500,
        )

        with pytest.raises(DataMasqueApiError):
            client.get_generated_rulesets(connection_id)


def test_start_async_ruleset_generation_success_columns(client):
    """Test when `selected_data` is of type `SelectedColumns`."""
    connection_id = ConnectionId("1")
    selected_columns = SelectedColumns(columns={"public": {"users": ["col1", "col2"]}})

    with requests_mock.Mocker() as m:
        m.post(f"http://test-server/api/async-generate-ruleset/{connection_id}/", status_code=201)
        client.start_async_ruleset_generation(connection_id, selected_columns)

        assert m.called
        request_data = m.last_request.json()
        assert "connection" not in request_data  # connection id belongs in the URL, not the body
        assert request_data["selected_columns"] == {"public": {"users": ["col1", "col2"]}}
        assert "hash_columns" not in request_data


def test_start_async_ruleset_generation_success_columns_with_hash(client):
    """Test when `selected_data` includes hash_columns with new table-level structure."""
    connection_id = ConnectionId("1")
    selected_columns = SelectedColumns(
        columns={"schema1": {"table1": ["col1", "col2"]}},
        hash_columns={
            "schema1": {
                "table1": {
                    "table": ["default_hash"],
                    "columns": {"col1": ["hashCol1"], "col2": None},
                }
            }
        },
    )

    with requests_mock.Mocker() as m:
        m.post(f"http://test-server/api/async-generate-ruleset/{connection_id}/", status_code=201)
        client.start_async_ruleset_generation(connection_id, selected_columns)

        assert m.called
        request_data = m.last_request.json()
        assert "connection" not in request_data
        assert request_data["selected_columns"] == {"schema1": {"table1": ["col1", "col2"]}}
        assert request_data["hash_columns"] == {
            "schema1": {
                "table1": {
                    "table": ["default_hash"],
                    "columns": {"col1": ["hashCol1"], "col2": None},
                }
            }
        }


def test_start_async_ruleset_generation_success_file(client):
    """Test when `selected_data` is of type `SelectedFileData`."""
    connection_id = ConnectionId("1")
    selected_file_data = SelectedFileData(
        user_selections=[
            {"locators": [["locator1"]], "files": ["file1"]},
            {"locators": [["locator2"]], "files": ["file2"]},
        ]
    )

    with requests_mock.Mocker() as m:
        m.post(f"http://test-server/api/async-generate-ruleset/{connection_id}/", status_code=201)
        client.start_async_ruleset_generation(connection_id, selected_file_data)

        assert m.called
        request_data = m.last_request.json()
        assert "connection" not in request_data
        assert request_data["selected_data"] == [
            {"locators": [["locator1"]], "files": ["file1"]},
            {"locators": [["locator2"]], "files": ["file2"]},
        ]


def test_start_async_ruleset_generation_no_selected_data(client):
    """Test that the function raises an error if `selected_data` is not provided."""
    connection_id = ConnectionId("1")

    with pytest.raises(ValueError, match="`selected_data` is a required argument"):
        client.start_async_ruleset_generation(connection_id, None)


def test_start_async_ruleset_generation_invalid_selected_data_type(client):
    """Test that the function raises an error if selected_data is of an invalid type."""
    connection_id = ConnectionId("1")
    invalid_selected_data = {"invalid": "data"}

    with pytest.raises(TypeError, match="expected `SelectedColumns` or `SelectedFileData`"):
        client.start_async_ruleset_generation(connection_id, invalid_selected_data)


def test_start_async_ruleset_generation_invalid_file_data(client):
    """Test that the function raises an error if `SelectedFileData` has empty locators or files."""
    connection_id = ConnectionId("1")
    # Pydantic accepts the construction (empty lists are valid `list[...]` values),
    # but the client validates that locators and files are non-empty before sending.
    invalid_file_data = SelectedFileData(
        user_selections=[
            UserSelection(locators=[["locator1"]], files=[]),  # Empty files
        ]
    )

    with pytest.raises(
        ValueError,
        match="Each `UserSelection` in `SelectedFileData.user_selections` must have",
    ):
        client.start_async_ruleset_generation(connection_id, invalid_file_data)


def test_start_async_ruleset_generation_request_failure(client):
    """Test that the function raises an error if the API request fails."""
    connection_id = ConnectionId("1")
    selected_columns = SelectedColumns(columns={"public": {"users": ["col1", "col2"]}})

    with requests_mock.Mocker() as m:
        m.post(f"http://test-server/api/async-generate-ruleset/{connection_id}/", status_code=500)

        with pytest.raises(DataMasqueApiError, match="failed with status 500"):
            client.start_async_ruleset_generation(connection_id, selected_columns)


@pytest.mark.parametrize(
    "csv_content",
    [
        "schema,table,column,selected\npublic,users,email,true",
        b"schema,table,column,selected\npublic,users,email,true",
        StringIO("schema,table,column,selected\npublic,users,email,true"),
        BytesIO(b"schema,table,column,selected\npublic,users,email,true"),
    ],
    ids=["str", "bytes", "StringIO", "BytesIO"],
)
def test_start_async_ruleset_generation_from_csv_success(client, csv_content):
    """Test successful async ruleset generation from CSV with various input types."""
    connection_id = ConnectionId("1")

    with requests_mock.Mocker() as m:
        m.post(
            f"http://test-server/api/async-generate-ruleset/{connection_id}/from-csv/",
            status_code=201,
        )
        client.start_async_ruleset_generation_from_csv(connection_id, csv_content)

        assert m.called
        form_data = parse_multipart_form(m.last_request)
        assert "csv_or_zip_file" in form_data
        assert form_data["csv_or_zip_file"]["filename"] == "ruleset.csv"
        assert form_data["csv_or_zip_file"]["content_type"] == "text/csv"
        assert form_data["csv_or_zip_file"]["content"] == b"schema,table,column,selected\npublic,users,email,true"


def test_start_async_ruleset_generation_from_csv_uploads_zip_as_zip(client):
    """A split report (zip bytes) is uploaded with a .zip filename and zip content-type."""
    connection_id = ConnectionId("1")
    zip_content = b"PK\x03\x04 zipped discovery report"

    with requests_mock.Mocker() as m:
        m.post(
            f"http://test-server/api/async-generate-ruleset/{connection_id}/from-csv/",
            status_code=201,
        )
        client.start_async_ruleset_generation_from_csv(connection_id, zip_content)

        form_data = parse_multipart_form(m.last_request)
        assert form_data["csv_or_zip_file"]["filename"] == "ruleset.zip"
        assert form_data["csv_or_zip_file"]["content_type"] == "application/zip"
        assert form_data["csv_or_zip_file"]["content"] == zip_content


def test_start_async_ruleset_generation_from_csv_with_target_size(client):
    """Test async ruleset generation from CSV with target_size_bytes parameter."""
    connection_id = ConnectionId("1")
    csv_content = "schema,table,column,selected\npublic,users,email,true"
    target_size = 1024000

    with requests_mock.Mocker() as m:
        m.post(
            f"http://test-server/api/async-generate-ruleset/{connection_id}/from-csv/",
            status_code=201,
        )
        client.start_async_ruleset_generation_from_csv(connection_id, csv_content, target_size_bytes=target_size)

        assert m.called
        form_data = parse_multipart_form(m.last_request)
        assert form_data["target_size_bytes"] == str(target_size)


def test_start_async_ruleset_generation_from_csv_failure(client):
    """Test that the function raises an error if the API request fails."""
    connection_id = ConnectionId("1")
    csv_content = "schema,table,column,selected\npublic,users,email,true"

    with requests_mock.Mocker() as m:
        m.post(
            f"http://test-server/api/async-generate-ruleset/{connection_id}/from-csv/",
            status_code=500,
        )

        with pytest.raises(DataMasqueApiError, match="failed with status 500"):
            client.start_async_ruleset_generation_from_csv(connection_id, csv_content)


def test_start_async_ruleset_generation_from_csv_retries_on_401(config):
    """Test that file content is correctly sent on retry after 401."""
    connection_id = ConnectionId("1")

    with patch.object(DataMasqueClient, "authenticate"):
        client = DataMasqueClient(config)
        csv_content = "schema,table,column,selected\npublic,users,email,true"

        with requests_mock.Mocker() as m:
            m.post(
                f"http://test-server/api/async-generate-ruleset/{connection_id}/from-csv/",
                [
                    {"status_code": 401},
                    {"status_code": 201},
                ],
            )
            client.start_async_ruleset_generation_from_csv(connection_id, csv_content)

            assert m.call_count == 2
            first_form = parse_multipart_form(m.request_history[0])
            second_form = parse_multipart_form(m.request_history[1])
            expected_content = b"schema,table,column,selected\npublic,users,email,true"
            assert first_form["csv_or_zip_file"]["content"] == expected_content
            assert second_form["csv_or_zip_file"]["content"] == expected_content


def test_schema_discovery_request_model_dump_minimal():
    """A request with only `connection` set dumps with empty lists and all `disable_*` flags off."""
    req = SchemaDiscoveryRequest(connection="conn-1")
    assert req.model_dump(exclude_none=True, mode="json") == {
        "connection": "conn-1",
        "custom_keywords": [],
        "ignored_keywords": [],
        "schemas": [],
        "disable_built_in_keywords": False,
        "disable_global_custom_keywords": False,
        "disable_global_ignored_keywords": False,
    }


def test_schema_discovery_request_model_dump_includes_set_fields():
    req = SchemaDiscoveryRequest(
        connection="conn-1",
        custom_keywords=["foo"],
        ignored_keywords=["bar"],
        schemas=["public"],
        in_data_discovery={"enabled": True, "row_sample_size": 100},
    )
    assert req.model_dump(exclude_none=True, mode="json") == {
        "connection": "conn-1",
        "custom_keywords": ["foo"],
        "ignored_keywords": ["bar"],
        "schemas": ["public"],
        "in_data_discovery": {"enabled": True, "row_sample_size": 100},
        "disable_built_in_keywords": False,
        "disable_global_custom_keywords": False,
        "disable_global_ignored_keywords": False,
    }


def test_discovery_requests_accept_connection_config_objects():
    """All three discovery request models accept a full `ConnectionConfig` and extract its `id`."""
    connection = DatabaseConnectionConfig(
        id=ConnectionId("conn-uuid"),
        name="prod_db",
        db_type=DatabaseType.postgres,
        host="db.example.com",
        port=5432,
        database="app",
        user="u",
    )

    schema_req = SchemaDiscoveryRequest(connection=connection)
    ruleset_req = RulesetGenerationRequest(connection=connection, selected_columns={"public": {"users": ["email"]}})
    file_req = FileRulesetGenerationRequest(connection=connection, selected_data=[])

    for req in (schema_req, ruleset_req, file_req):
        assert req.model_dump(exclude_none=True, mode="json")["connection"] == "conn-uuid"


def test_start_schema_discovery_run_accepts_typed_request(client):
    req = SchemaDiscoveryRequest(connection="conn-1", schemas=["public", "private"])

    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/schema-discovery/",
            json={"id": 7},
            status_code=201,
        )
        run_id = client.start_schema_discovery_run(req)

    assert run_id == 7
    assert m.last_request.json() == {
        "connection": "conn-1",
        "custom_keywords": [],
        "ignored_keywords": [],
        "schemas": ["public", "private"],
        "disable_built_in_keywords": False,
        "disable_global_custom_keywords": False,
        "disable_global_ignored_keywords": False,
    }


def test_ruleset_generation_request_round_trip(client):
    req = RulesetGenerationRequest(
        connection="conn-1",
        selected_columns={"public": {"users": ["email"]}},
        hash_columns={"public": {"users": {"table": ["id"]}}},
    )

    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/generate-ruleset/v2/",
            content=b"version: '1.0'",
            status_code=201,
        )
        yaml = client.generate_ruleset(req)

    assert yaml == "version: '1.0'"
    assert m.last_request.json() == {
        "connection": "conn-1",
        "selected_columns": {"public": {"users": ["email"]}},
        "hash_columns": {"public": {"users": {"table": ["id"]}}},
    }


def test_ruleset_generation_request_omits_optional_hash_columns(client):
    req = RulesetGenerationRequest(
        connection="conn-1",
        selected_columns={"public": {"users": ["email"]}},
    )

    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/generate-ruleset/v2/",
            content=b"yaml",
            status_code=201,
        )
        client.generate_ruleset(req)

    assert "hash_columns" not in m.last_request.json()


def test_file_ruleset_generation_request_round_trip(client):
    req = FileRulesetGenerationRequest(
        connection="conn-1",
        selected_data=[{"locators": [["a"]], "files": ["f1.csv"]}],
    )

    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/generate-file-ruleset/",
            content=b"yaml",
            status_code=201,
        )
        yaml = client.generate_file_ruleset(req)

    assert yaml == "yaml"
    assert m.last_request.json() == {
        "connection": "conn-1",
        "selected_data": [{"locators": [["a"]], "files": ["f1.csv"]}],
    }


def _schema_discovery_row(row_id: int, column_name: str, table_name: str = "users") -> dict:
    return {
        "id": row_id,
        "column": column_name,
        "table": table_name,
        "schema_name": "public",
        "data": {
            "data_type": "text",
            "foreign_keys": [],
            "discovery_matches": [],
            "constraint_columns": [],
            "unique_index_names": [],
            "referencing_foreign_keys": [],
            "constraint": "",
        },
    }


def test_list_schema_discovery_results_follows_pagination(client):
    run_id = RunId(42)
    page1 = {
        "count": 3,
        "next": "http://test-server/api/schema-discovery/v2/42/?limit=2&offset=2",
        "previous": None,
        "results": [_schema_discovery_row(1, "email"), _schema_discovery_row(2, "name")],
    }
    page2 = {
        "count": 3,
        "next": None,
        "previous": "http://test-server/api/schema-discovery/v2/42/?limit=2",
        "results": [_schema_discovery_row(3, "phone")],
    }

    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/schema-discovery/v2/42/",
            [{"json": page1, "status_code": 200}, {"json": page2, "status_code": 200}],
        )
        results = client.list_schema_discovery_results(run_id)

    assert len(results) == 3
    assert all(isinstance(r, SchemaDiscoveryResult) for r in results)
    assert [r.column for r in results] == ["email", "name", "phone"]


def test_iter_schema_discovery_results_is_lazy(client):
    """`iter_*` returns an iterator that only makes HTTP calls as pages are consumed."""
    run_id = RunId(99)
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/schema-discovery/v2/99/",
            json={
                "count": 1,
                "next": None,
                "previous": None,
                "results": [_schema_discovery_row(1, "email")],
            },
            status_code=200,
        )
        iterator = client.iter_schema_discovery_results(run_id)
        # No HTTP call yet — iterator is lazy.
        assert m.call_count == 0

        first = next(iterator)
        assert first.column == "email"
        assert m.call_count == 1


def test_get_schema_discovery_page_returns_page_with_table_metadata(client):
    run_id = RunId(7)
    response_json = {
        "count": 1,
        "next": None,
        "previous": None,
        "results": [_schema_discovery_row(1, "email")],
        "table_metadata": {
            "public": {
                "users": {
                    "primary_keys": [{"columns": ["id"]}],
                    "unique_keys": [{"columns": ["email"]}],
                    "foreign_keys": [],
                },
            },
        },
    }
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/schema-discovery/v2/7/",
            json=response_json,
            status_code=200,
        )
        page = client.get_schema_discovery_page(run_id, limit=10, offset=20)

    assert isinstance(page, SchemaDiscoveryPage)
    assert [r.column for r in page.results] == ["email"]
    assert page.table_metadata["public"]["users"].primary_keys[0].columns == ["id"]
    assert m.last_request.qs == {"limit": ["10"], "offset": ["20"]}


def test_start_schema_discovery_run_raises_on_non_201(client):
    """A non-201 response (e.g. validation failure) raises `FailedToStartError`."""
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/schema-discovery/",
            json={"detail": "connection not found"},
            status_code=400,
        )
        with pytest.raises(FailedToStartError, match="Schema discovery run failed to start"):
            client.start_schema_discovery_run(SchemaDiscoveryRequest(connection="nope"))


def test_schema_discovery_request_rejects_discovery_config():
    """The v1 schema-discovery request rejects `discovery_config` and points the user at the from-config method."""
    with pytest.raises(ValidationError, match="start_schema_discovery_run_from_config"):
        SchemaDiscoveryRequest(connection="conn-1", discovery_config=DiscoveryConfigId(DISCOVERY_CONFIG_ID))


def test_file_data_discovery_request_rejects_discovery_config():
    """The v1 file-data-discovery request rejects `discovery_config` and points the user at the from-config method."""
    with pytest.raises(ValidationError, match="start_file_data_discovery_run_from_config"):
        FileDataDiscoveryRequest(connection="conn-1", discovery_config=DiscoveryConfigId(DISCOVERY_CONFIG_ID))


def test_start_file_data_discovery_run_minimal(client):
    """A minimal FDD request — only `connection` set — round-trips through the server."""
    req = FileDataDiscoveryRequest(connection="conn-1")
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/run-file-data-discovery/",
            json={"id": 42},
            status_code=201,
        )
        run_id = client.start_file_data_discovery_run(req)

    assert run_id == 42
    body = m.last_request.json()
    assert body["connection"] == "conn-1"
    assert "discovery_config" not in body


def test_start_file_data_discovery_run_full(client):
    """All legacy FDD request fields populate the wire payload and pass through unwrap helpers."""
    req = FileDataDiscoveryRequest(
        connection="conn-1",
        options=FileDataDiscoveryOptions(diagnostic_logging=True),
        custom_keywords=["foo"],
        ignored_keywords=["bar"],
        disable_built_in_keywords=True,
        disable_global_custom_keywords=True,
        disable_global_ignored_keywords=False,
        in_data_discovery={"enabled": True, "row_sample_size": 50},
        recurse=True,
        include=[{"glob": "*.csv"}],
        skip=[{"regex": r".*/tmp/.*", "match_against": "path"}],
        encoding="utf-8",
        workers=4,
    )
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/run-file-data-discovery/",
            json={"id": 99},
            status_code=201,
        )
        assert client.start_file_data_discovery_run(req) == 99

    body = m.last_request.json()
    assert body == {
        "connection": "conn-1",
        "options": {"diagnostic_logging": True},
        "custom_keywords": ["foo"],
        "ignored_keywords": ["bar"],
        "disable_built_in_keywords": True,
        "disable_global_custom_keywords": True,
        "disable_global_ignored_keywords": False,
        "in_data_discovery": {"enabled": True, "row_sample_size": 50},
        "recurse": True,
        "include": [{"glob": "*.csv"}],
        "skip": [{"regex": r".*/tmp/.*", "match_against": "path"}],
        "encoding": "utf-8",
        "workers": 4,
    }


def test_file_filter_requires_exactly_one_of_glob_or_regex():
    """A `FileFilter` with neither, or both, of `glob`/`regex` is rejected."""
    FileFilter(glob="*.csv")
    FileFilter(regex=r".*\.csv")
    FileFilter(glob="*.csv", match_against=FileFilterMatchAgainst.filename)

    with pytest.raises(ValidationError, match="exactly one of `glob` or `regex`"):
        FileFilter()

    with pytest.raises(ValidationError, match="exactly one of `glob` or `regex`"):
        FileFilter(glob="*.csv", regex=r".*\.csv")


def test_file_filter_rejects_empty_pattern():
    """An empty `glob`/`regex` is rejected rather than silently treated as unset."""
    with pytest.raises(ValidationError):
        FileFilter(glob="")

    with pytest.raises(ValidationError):
        FileFilter(regex="", glob="*.csv")


def test_file_discovery_result_parses_server_response():
    """A file-discovery-results record parses; match label/categories stay optional for non-sensitive matches."""
    result = FileDiscoveryResult.model_validate(
        {
            "id": 7,
            "connection": {"id": "conn-1", "name": "my files"},
            "file_type": "csv",
            "files": [{"path": "data/people.csv", "file_type": "csv", "delimiter": ",", "encoding": "utf-8"}],
            "results": [
                {
                    "locator": "email",
                    "data_types": ["string"],
                    "matches": [
                        {"flagged_by": "MDD", "description": "Email", "label": "email", "categories": ["PII"]},
                        {"flagged_by": "MDD", "description": "Not sensitive"},
                    ],
                }
            ],
        }
    )

    assert result.id == 7
    assert result.connection.name == "my files"
    assert result.files[0].path == "data/people.csv"
    non_sensitive_match = result.results[0].matches[1]
    assert non_sensitive_match.label is None
    assert non_sensitive_match.categories is None


def test_file_data_discovery_ignore_rules_serialize():
    """`in_data_discovery.ignore_rules` round-trips into the wire payload."""
    req = FileDataDiscoveryRequest(
        connection="conn-1",
        in_data_discovery=InDataDiscoveryConfig(
            enabled=True,
            custom_rules=[{"name": "cc", "pattern": r"^1234"}],
            non_sensitive_rules=[{"pattern": r"^5555"}],
            ignore_rules=[{"pattern": r"^4321"}],
        ),
    )
    dumped = req.model_dump(exclude_none=True, mode="json")
    assert dumped["in_data_discovery"]["ignore_rules"] == [{"pattern": r"^4321"}]


def test_start_file_data_discovery_run_raises_on_non_201(client):
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/run-file-data-discovery/",
            json={"detail": "connection not found"},
            status_code=400,
        )
        with pytest.raises(FailedToStartError, match="File data discovery run failed to start"):
            client.start_file_data_discovery_run(FileDataDiscoveryRequest(connection="nope"))


def test_schema_discovery_from_config_request_accepts_discovery_config_id():
    """A `DiscoveryConfigId` string passes through unchanged in the v2 request body."""
    req = SchemaDiscoveryFromConfigRequest(connection="conn-1", discovery_config=DiscoveryConfigId(DISCOVERY_CONFIG_ID))
    assert req.model_dump(exclude_none=True, mode="json") == {
        "connection": "conn-1",
        "discovery_config": DISCOVERY_CONFIG_ID,
    }


def test_schema_discovery_from_config_request_unwraps_discovery_config_model():
    """Passing a full `DiscoveryConfig` object substitutes its `id` for the wire payload."""
    config = DiscoveryConfig(name="my_cfg", config_type="database", id=DiscoveryConfigId(DISCOVERY_CONFIG_ID))
    req = SchemaDiscoveryFromConfigRequest(connection="conn-1", discovery_config=config)
    assert req.model_dump(exclude_none=True, mode="json")["discovery_config"] == DISCOVERY_CONFIG_ID


def test_schema_discovery_from_config_request_rejects_unsaved_discovery_config():
    """A `DiscoveryConfig` without an `id` cannot be used yet — raises immediately."""
    config = DiscoveryConfig(name="my_cfg", config_type="database")
    with pytest.raises(ValueError, match="id is None"):
        SchemaDiscoveryFromConfigRequest(connection="conn-1", discovery_config=config)


def test_schema_discovery_from_config_request_rejects_legacy_fields():
    """The saved-config request rejects legacy detection options — they live in the config."""
    with pytest.raises(ValidationError):
        SchemaDiscoveryFromConfigRequest(
            connection="conn-1",
            discovery_config=DiscoveryConfigId(DISCOVERY_CONFIG_ID),
            custom_keywords=["foo"],
        )


def test_schema_discovery_from_config_request_accepts_schemas():
    """`schemas` scopes the saved-config schema run to specific schemas and is forwarded as-is."""
    req = SchemaDiscoveryFromConfigRequest(
        connection="conn-1",
        discovery_config=DiscoveryConfigId(DISCOVERY_CONFIG_ID),
        schemas=["public", "sales"],
    )
    assert req.model_dump(exclude_none=True, mode="json") == {
        "connection": "conn-1",
        "discovery_config": DISCOVERY_CONFIG_ID,
        "schemas": ["public", "sales"],
    }


@pytest.mark.parametrize(
    "extra_field",
    [
        {"custom_keywords": ["foo"]},
        {"in_data_discovery": {"enabled": True}},
        {"recurse": True},
        {"include": [{"glob": "*.csv"}]},
        {"skip": [{"glob": "*.tmp"}]},
        {"encoding": "utf-8"},
        {"workers": 4},
    ],
)
def test_file_data_discovery_from_config_request_rejects_non_config_fields(extra_field):
    """Detection options and file-handling params are rejected — they live in the config, not the request."""
    with pytest.raises(ValidationError):
        FileDataDiscoveryFromConfigRequest(
            connection="conn-1",
            discovery_config=DiscoveryConfigId(DISCOVERY_CONFIG_ID),
            **extra_field,
        )


def test_schema_discovery_from_config_request_requires_discovery_config():
    """`discovery_config` must be set explicitly; omitting it is a validation error (it may be None, but not absent)."""
    with pytest.raises(ValidationError, match="discovery_config"):
        SchemaDiscoveryFromConfigRequest(connection="conn-1")


def test_schema_discovery_from_config_request_accepts_none_discovery_config():
    """An explicit None is accepted and means the server uses its built-in defaults."""
    req = SchemaDiscoveryFromConfigRequest(connection="conn-1", discovery_config=None)
    assert req.discovery_config is None


def test_file_data_discovery_from_config_request_requires_discovery_config():
    """`discovery_config` must be set explicitly; omitting it is a validation error (it may be None, but not absent)."""
    with pytest.raises(ValidationError, match="discovery_config"):
        FileDataDiscoveryFromConfigRequest(connection="conn-1")


def test_file_data_discovery_from_config_request_accepts_none_discovery_config():
    """An explicit None is accepted and means the server uses its built-in defaults."""
    req = FileDataDiscoveryFromConfigRequest(connection="conn-1", discovery_config=None)
    assert req.discovery_config is None


def test_start_schema_discovery_run_from_config_sends_discovery_config(client):
    """`start_schema_discovery_run_from_config` posts the discovery_config id to the saved-config endpoint."""
    req = SchemaDiscoveryFromConfigRequest(connection="conn-1", discovery_config=DiscoveryConfigId(DISCOVERY_CONFIG_ID))
    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/schema-discovery/v2/", json={"id": 11}, status_code=201)
        assert client.start_schema_discovery_run_from_config(req) == 11

    assert m.last_request.json() == {"connection": "conn-1", "discovery_config": DISCOVERY_CONFIG_ID}


def test_start_schema_discovery_run_from_config_sends_schemas(client):
    """`start_schema_discovery_run_from_config` forwards a `schemas` scope to the saved-config endpoint."""
    req = SchemaDiscoveryFromConfigRequest(
        connection="conn-1", discovery_config=DiscoveryConfigId(DISCOVERY_CONFIG_ID), schemas=["public"]
    )
    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/schema-discovery/v2/", json={"id": 12}, status_code=201)
        assert client.start_schema_discovery_run_from_config(req) == 12

    assert m.last_request.json() == {
        "connection": "conn-1",
        "discovery_config": DISCOVERY_CONFIG_ID,
        "schemas": ["public"],
    }


def test_start_file_data_discovery_run_from_config_sends_discovery_config(client):
    """`start_file_data_discovery_run_from_config` posts only the connection and discovery_config id."""
    config = DiscoveryConfig(name="my_cfg", config_type="file", id=DiscoveryConfigId(DISCOVERY_CONFIG_ID))
    req = FileDataDiscoveryFromConfigRequest(connection="conn-1", discovery_config=config)
    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/run-file-data-discovery/v2/", json={"id": 99}, status_code=201)
        assert client.start_file_data_discovery_run_from_config(req) == 99

    assert m.last_request.json() == {"connection": "conn-1", "discovery_config": DISCOVERY_CONFIG_ID}


def test_start_file_data_discovery_run_from_config_sends_options(client):
    """`options` (diagnostic_logging) is posted alongside the config on the file from-config trigger."""
    req = FileDataDiscoveryFromConfigRequest(
        connection="conn-1",
        discovery_config=DiscoveryConfigId(DISCOVERY_CONFIG_ID),
        options=FileDataDiscoveryOptions(diagnostic_logging=True),
    )
    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/run-file-data-discovery/v2/", json={"id": 77}, status_code=201)
        assert client.start_file_data_discovery_run_from_config(req) == 77

    assert m.last_request.json() == {
        "connection": "conn-1",
        "discovery_config": DISCOVERY_CONFIG_ID,
        "options": {"diagnostic_logging": True},
    }


def test_start_schema_discovery_run_from_config_none_sends_null_discovery_config(client):
    """With `discovery_config=None` the client posts an explicit null so the server applies its defaults."""
    req = SchemaDiscoveryFromConfigRequest(connection="conn-1", discovery_config=None)
    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/schema-discovery/v2/", json={"id": 13}, status_code=201)
        assert client.start_schema_discovery_run_from_config(req) == 13

    assert m.last_request.json() == {"connection": "conn-1", "discovery_config": None}


def test_start_file_data_discovery_run_from_config_none_sends_null_discovery_config(client):
    """With `discovery_config=None` the file-data trigger posts an explicit null so the server applies its defaults."""
    req = FileDataDiscoveryFromConfigRequest(connection="conn-1", discovery_config=None)
    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/run-file-data-discovery/v2/", json={"id": 21}, status_code=201)
        assert client.start_file_data_discovery_run_from_config(req) == 21

    assert m.last_request.json() == {"connection": "conn-1", "discovery_config": None}


def test_start_schema_discovery_run_from_config_raises_invalid_discovery_config_when_not_valid(client):
    """A 400 with a `discovery_config` validation message raises the specific subclass."""
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/schema-discovery/v2/",
            json={
                "discovery_config": ['Discovery config "my_cfg" cannot be used: validation status is `invalid`.'],
            },
            status_code=400,
        )
        with pytest.raises(
            InvalidDiscoveryConfigError,
            match=r'Schema discovery run failed to start due to discovery config error: .*"my_cfg".*invalid',
        ):
            client.start_schema_discovery_run_from_config(
                SchemaDiscoveryFromConfigRequest(
                    connection="conn-1", discovery_config=DiscoveryConfigId(DISCOVERY_CONFIG_ID)
                ),
            )


def test_start_schema_discovery_run_from_config_raises_not_found_when_missing(client):
    """
    A 400 for a config that cannot be found raises `DiscoveryConfigNotFoundError`.

    This is a bad reference rather than an unusable-but-present config,
    so it must not be conflated with `InvalidDiscoveryConfigError`.
    The not-found subclass still inherits `FailedToStartError` for callers that catch the base.
    """
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/schema-discovery/v2/",
            json={"discovery_config": [f'Invalid pk "{DISCOVERY_CONFIG_ID}" - object does not exist.']},
            status_code=400,
        )
        with pytest.raises(DiscoveryConfigNotFoundError, match="object does not exist") as exc_info:
            client.start_schema_discovery_run_from_config(
                SchemaDiscoveryFromConfigRequest(
                    connection="conn-1", discovery_config=DiscoveryConfigId(DISCOVERY_CONFIG_ID)
                ),
            )

    assert isinstance(exc_info.value, FailedToStartError)


def test_start_file_data_discovery_run_from_config_raises_not_found_when_missing(client):
    """A not-found saved config on the file-data trigger also raises `DiscoveryConfigNotFoundError`."""
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/run-file-data-discovery/v2/",
            json={"discovery_config": [f'Invalid pk "{DISCOVERY_CONFIG_ID}" - object does not exist.']},
            status_code=400,
        )
        with pytest.raises(DiscoveryConfigNotFoundError, match="object does not exist"):
            client.start_file_data_discovery_run_from_config(
                FileDataDiscoveryFromConfigRequest(
                    connection="conn-1", discovery_config=DiscoveryConfigId(DISCOVERY_CONFIG_ID)
                ),
            )


def test_start_schema_discovery_run_from_config_raises_invalid_discovery_config_on_broken_yaml(client):
    """
    A 400 carrying trigger-time re-validation of broken saved YAML is classified.

    The server reports these under `discovery_config` as `{"message", "line_number", "column_number"}` dicts,
    not strings.
    """
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/schema-discovery/v2/",
            json={
                "discovery_config": [
                    {"message": "Unknown mask 'no_such_mask'.", "line_number": 4, "column_number": 7},
                ],
            },
            status_code=400,
        )
        with pytest.raises(
            InvalidDiscoveryConfigError,
            match=r"Schema discovery run failed to start due to discovery config error: Unknown mask 'no_such_mask'\.",
        ):
            client.start_schema_discovery_run_from_config(
                SchemaDiscoveryFromConfigRequest(
                    connection="conn-1", discovery_config=DiscoveryConfigId(DISCOVERY_CONFIG_ID)
                ),
            )


def test_start_file_data_discovery_run_from_config_raises_invalid_discovery_config(client):
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/run-file-data-discovery/v2/",
            json={
                "discovery_config": ['Discovery config "my_cfg" cannot be used: validation status is `in_progress`.'],
            },
            status_code=400,
        )
        with pytest.raises(
            InvalidDiscoveryConfigError,
            match=r"File data discovery run failed to start due to discovery config error: .*in_progress",
        ):
            client.start_file_data_discovery_run_from_config(
                FileDataDiscoveryFromConfigRequest(
                    connection="conn-1",
                    discovery_config=DiscoveryConfigId(DISCOVERY_CONFIG_ID),
                ),
            )


def test_start_schema_discovery_run_from_config_non_discovery_config_400_still_raises_generic_error(client):
    """Other 400s (e.g. unknown connection) keep raising the base FailedToStartError, not the config subclass."""
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/schema-discovery/v2/",
            json={"connection": ['Invalid pk "nope" - object does not exist.']},
            status_code=400,
        )
        with pytest.raises(FailedToStartError) as exc_info:
            client.start_schema_discovery_run_from_config(
                SchemaDiscoveryFromConfigRequest(
                    connection="nope", discovery_config=DiscoveryConfigId(DISCOVERY_CONFIG_ID)
                ),
            )
        assert not isinstance(exc_info.value, InvalidDiscoveryConfigError)


def _zip_bytes(*members: tuple[str, str]) -> bytes:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w") as zip_file:
        for name, content in members:
            zip_file.writestr(name, content)

    return buffer.getvalue()


def test_get_discovery_run_config_snapshot_yaml_returns_yaml(client):
    """The single zipped snapshot member is unzipped and returned as a YAML string."""
    snapshot = "# Discovery configuration: my_cfg.\nlabels: []\nmetadata_rules: []\n"
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/discovery/runs/7/config-snapshot/",
            content=_zip_bytes(("config-7.yaml", snapshot)),
            headers={"Content-Disposition": 'attachment; filename="config-7.yaml.zip"'},
            status_code=200,
        )
        result = client.get_discovery_run_config_snapshot_yaml(RunId(7))

    assert result == snapshot
    assert "timezone" not in (m.last_request.qs or {})


def test_get_discovery_run_config_snapshot_yaml_forwards_timezone(client):
    """An explicit `timezone` is forwarded as a query param for the provenance-header timestamp."""
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/discovery/runs/7/config-snapshot/",
            content=_zip_bytes(("config-7.yaml", "labels: []\n")),
            status_code=200,
        )
        client.get_discovery_run_config_snapshot_yaml(RunId(7), timezone="+12:00")

    assert m.last_request.qs["timezone"] == ["+12:00"]


def test_get_discovery_run_config_snapshot_yaml_empty_archive_raises(client):
    """An archive with no members raises a clear error rather than an IndexError."""
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/discovery/runs/7/config-snapshot/",
            content=_zip_bytes(),
            status_code=200,
        )
        with pytest.raises(DataMasqueException, match="contained no files"):
            client.get_discovery_run_config_snapshot_yaml(RunId(7))
