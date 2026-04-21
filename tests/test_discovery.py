"""Tests for `DiscoveryClient` (schema discovery, ruleset generation, db-discovery report)."""

import zipfile
from io import BytesIO, StringIO
from unittest.mock import patch

import pytest
import requests_mock

from datamasque.client import (
    DataMasqueClient,
    FileRulesetGenerationRequest,
    RulesetGenerationRequest,
    RunId,
    SchemaDiscoveryPage,
    SchemaDiscoveryRequest,
    SchemaDiscoveryResult,
)
from datamasque.client.exceptions import (
    AsyncRulesetGenerationInProgressError,
    DataMasqueApiError,
    DataMasqueException,
    FailedToStartError,
)
from datamasque.client.models.connection import ConnectionId, DatabaseConnectionConfig, DatabaseType
from datamasque.client.models.data_selection import SelectedColumns, SelectedFileData, UserSelection
from datamasque.client.models.status import AsyncRulesetGenerationTaskStatus
from tests.helpers import parse_multipart_form


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
