"""Tests for `RunClient` (start, status, log, run-report endpoints)."""

import pytest
import requests_mock

from datamasque.client.exceptions import (
    DataMasqueApiError,
    FailedToStartError,
    InvalidLibraryError,
    InvalidRulesetError,
    RunNotCancellableError,
)
from datamasque.client.models.connection import ConnectionId, DatabaseConnectionConfig, DatabaseType
from datamasque.client.models.ruleset import Ruleset, RulesetId, RulesetType
from datamasque.client.models.runs import (
    MaskingRunOptions,
    MaskingRunRequest,
    RunConnectionRef,
    RunId,
    RunInfo,
    UnfinishedRun,
)
from datamasque.client.models.status import MaskingRunStatus
from tests.helpers import fake


def test_get_run_log(client):
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/runs/1/log/",
            json={"log": "test_log"},
            status_code=200,
        )
        assert client.get_run_log(RunId(1)) == '{"log": "test_log"}'


def test_get_sdd_report(client):
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/runs/1/sdd-report/",
            json={"report": "test_report"},
            status_code=200,
        )
        assert client.get_sdd_report(RunId(1)) == '{"report": "test_report"}'


def test_get_file_data_discovery_report(client):
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/runs/1/file-discovery-results/",
            json=[{"id": 1, "file_type": "csv", "files": [], "results": []}],
            status_code=200,
        )
        results = client.get_file_data_discovery_report(RunId(1))
        assert len(results) == 1
        assert results[0].id == 1


def test_unfinished_run_str_with_destination():
    """`UnfinishedRun.__str__` includes both source and destination connection names when both are set."""
    run = UnfinishedRun(
        id=42,
        source_connection=RunConnectionRef(name="source_db"),
        destination_connection=RunConnectionRef(name="destination_db"),
        ruleset_name="my_ruleset",
        status=MaskingRunStatus.running,
    )

    assert str(run) == '"source_db", "destination_db": Run ID 42 in status `running`, ruleset "my_ruleset"'


def test_unfinished_run_str_without_destination():
    """`UnfinishedRun.__str__` omits the destination when it is `None`."""
    run = UnfinishedRun(
        id=42,
        source_connection=RunConnectionRef(name="source_db"),
        ruleset_name="my_ruleset",
        status=MaskingRunStatus.queued,
    )

    assert str(run) == '"source_db": Run ID 42 in status `queued`, ruleset "my_ruleset"'


def test_get_unfinished_runs(client):
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/runs/?connection_ruleset_name=&ruleset_name=&run_status=queued&limit=1&offset=0",
            json={
                "results": [
                    {
                        "source_connection_name": "queued_src",
                        "destination_connection_name": "queued_dst",
                        "id": 1,
                        "ruleset_name": "ruleset_1",
                        "status": "queued",
                    }
                ]
            },
            status_code=200,
        )
        m.get(
            "http://test-server/api/runs/?connection_ruleset_name=&ruleset_name=&run_status=running&limit=1&offset=0",
            json={
                "results": [
                    {
                        "source_connection_name": "running_src",
                        "destination_connection_name": "running_dst",
                        "id": 2,
                        "ruleset_name": "ruleset_2",
                        "status": "running",
                    }
                ]
            },
            status_code=200,
        )
        m.get(
            "http://test-server/api/runs/?connection_ruleset_name=&ruleset_name=&run_status=validating&limit=1&offset=0",
            json={
                "results": [
                    {
                        "source_connection_name": "validating_src",
                        "destination_connection_name": "validating_dst",
                        "id": 3,
                        "ruleset_name": "ruleset_3",
                        "status": "validating",
                    }
                ]
            },
            status_code=200,
        )
        m.get(
            "http://test-server/api/runs/?connection_ruleset_name=&ruleset_name=&run_status=cancelling&limit=1&offset=0",
            json={
                "results": [
                    {
                        "source_connection_name": "cancelling_src",
                        "destination_connection_name": "",
                        "id": 4,
                        "ruleset_name": "ruleset_4",
                        "status": "cancelling",
                    }
                ]
            },
            status_code=200,
        )

        ur = client.get_unfinished_runs()
        # 3 statuses have both source and destination keys, cancelling has only source (empty destination)
        assert len(ur) == 7
        for run in ur.values():
            assert isinstance(run, UnfinishedRun)


def test_start_masking_run(client):
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/runs/",
            json={"id": "1", "name": fake.word()},
            status_code=201,
        )
        assert client.start_masking_run(MaskingRunRequest(connection="1", ruleset="rs-1", name=fake.word())) == "1"


def test_start_masking_run_invalid_ruleset(client):
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/runs/",
            json={"ruleset": ["Cannot start run on invalid ruleset."]},
            status_code=400,
        )
        with pytest.raises(InvalidRulesetError):
            client.start_masking_run(MaskingRunRequest(connection="1", ruleset="rs-1", name=fake.word()))


def test_start_masking_run_invalid_library(client):
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/runs/",
            json={"ruleset": ['Cannot start run. Library "foo" is invalid.']},
            status_code=400,
        )
        with pytest.raises(InvalidLibraryError, match=r'Run failed to start due to invalid library named "foo"'):
            client.start_masking_run(MaskingRunRequest(connection="1", ruleset="rs-1", name=fake.word()))


def test_start_masking_run_invalid_library_without_named_match(client):
    """Server says the library is invalid but the error string doesn't quote a library name."""
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/runs/",
            json={"ruleset": ["Cannot start run because a library referenced from the ruleset is invalid."]},
            status_code=400,
        )
        with pytest.raises(InvalidLibraryError, match=r"Run failed to start due to invalid library\."):
            client.start_masking_run(MaskingRunRequest(connection="1", ruleset="rs-1", name=fake.word()))


def test_get_run_report_returns_response_text(client):
    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/runs/7/run-report/", text="the,report,csv\n1,2,3", status_code=200)
        report = client.get_run_report(RunId(7))

    assert report == "the,report,csv\n1,2,3"


def test_start_masking_run_invalid_library_with_quotes_in_name(client):
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/runs/",
            json={"ruleset": ['Cannot start run. Library "library with "quotes and spaces" in its name" is invalid.']},
            status_code=400,
        )
        with pytest.raises(
            InvalidLibraryError,
            match=r'Run failed to start due to invalid library named "library with "quotes and spaces" in its name"',
        ):
            client.start_masking_run(MaskingRunRequest(connection="1", ruleset="rs-1", name=fake.word()))


def test_start_masking_run_unparseable_ruleset_error(client):
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/runs/",
            json={"ruleset": []},
            status_code=400,
        )
        with pytest.raises(FailedToStartError):
            client.start_masking_run(MaskingRunRequest(connection="1", ruleset="rs-1", name=fake.word()))


def test_start_masking_run_fail(client):
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/runs/",
            json={"error": fake.sentence()},
            status_code=400,
        )
        with pytest.raises(FailedToStartError):
            client.start_masking_run(MaskingRunRequest(connection="1", ruleset="rs-1", name=fake.word()))


def test_start_masking_run_failure_surfaces_server_body(client):
    """On a non-201 response the raised `FailedToStartError` carries the `Response` and names the status + body."""
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/runs/",
            json={"options": ["This field is required."]},
            status_code=400,
        )
        with pytest.raises(FailedToStartError) as excinfo:
            client.start_masking_run(MaskingRunRequest(connection="1", ruleset="rs-1", name="my-run"))

    assert excinfo.value.response.status_code == 400
    assert excinfo.value.response.json() == {"options": ["This field is required."]}
    # The message surfaces the status and body so users can diagnose without re-inspecting the response.
    assert "status 400" in str(excinfo.value)
    assert "This field is required." in str(excinfo.value)


def test_get_run_info(client):
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/runs/1/",
            json={
                "id": 1,
                "name": "r1",
                "status": "finished",
                "mask_type": "database",
                "source_connection_name": "conn1",
                "ruleset_name": "rs1",
            },
            status_code=200,
        )
        result = client.get_run_info(1)
        assert isinstance(result, RunInfo)
        assert result.id == 1
        assert result.name == "r1"


def test_masking_run_request_model_dump_minimal():
    """A minimal request dumps with an empty `options` object (the server rejects missing `options`)."""
    req = MaskingRunRequest(connection="conn-1", ruleset="rs-1")
    assert req.model_dump(exclude_none=True, mode="json") == {
        "connection": "conn-1",
        "ruleset": "rs-1",
        "mask_type": "database",
        "options": {},
    }


def test_masking_run_request_requires_ruleset():
    """Omitting `ruleset` raises a validation error — `start_masking_run` only supports runs with a stored ruleset."""
    with pytest.raises(ValueError, match="ruleset"):
        MaskingRunRequest(connection="conn-1")  # type: ignore[call-arg]


def test_masking_run_request_accepts_connection_config_and_ruleset_objects():
    """Callers may pass full `ConnectionConfig` / `Ruleset` objects; their IDs are extracted at construction."""
    connection = DatabaseConnectionConfig(
        id=ConnectionId("conn-uuid"),
        name="prod_db",
        db_type=DatabaseType.postgres,
        host="db.example.com",
        port=5432,
        database="app",
        user="masker",
    )
    dest = DatabaseConnectionConfig(
        id=ConnectionId("dest-uuid"),
        name="staging_db",
        db_type=DatabaseType.postgres,
        host="staging.example.com",
        port=5432,
        database="app",
        user="masker",
    )
    ruleset = Ruleset(
        id=RulesetId("rs-uuid"),
        name="my_ruleset",
        yaml="version: '1.0'",
        ruleset_type=RulesetType.database,
    )

    req = MaskingRunRequest(connection=connection, destination_connection=dest, ruleset=ruleset)

    dumped = req.model_dump(exclude_none=True, mode="json")
    assert dumped["connection"] == "conn-uuid"
    assert dumped["destination_connection"] == "dest-uuid"
    assert dumped["ruleset"] == "rs-uuid"


def test_masking_run_request_rejects_unpersisted_connection():
    """A `ConnectionConfig` without an `id` means the caller hasn't created it yet — raise at construction."""
    connection = DatabaseConnectionConfig(
        name="not_yet_created",
        db_type=DatabaseType.postgres,
        host="localhost",
        port=5432,
        database="db",
        user="u",
    )
    with pytest.raises(ValueError, match="has not been created yet"):
        MaskingRunRequest(connection=connection, ruleset="rs-1")


def test_masking_run_request_rejects_unpersisted_ruleset():
    """Same check on the ruleset side."""
    ruleset = Ruleset(name="fresh_ruleset", yaml="version: '1.0'", ruleset_type=RulesetType.database)
    with pytest.raises(ValueError, match="has not been created yet"):
        MaskingRunRequest(connection="conn-1", ruleset=ruleset)


def test_run_info_collapses_flat_connection_fields():
    """`RunInfo.model_validate` folds the server's flat `source_connection*` pair into a nested `RunConnectionRef`."""
    run = RunInfo.model_validate(
        {
            "id": 1,
            "status": "finished",
            "mask_type": "database",
            "source_connection": "src-uuid",
            "source_connection_name": "prod",
            "destination_connection": "dst-uuid",
            "destination_connection_name": "staging",
            "ruleset_name": "rs",
        }
    )
    assert isinstance(run.source_connection, RunConnectionRef)
    assert run.source_connection.id == "src-uuid"
    assert run.source_connection.name == "prod"
    assert run.destination_connection is not None
    assert run.destination_connection.id == "dst-uuid"
    assert run.destination_connection.name == "staging"


def test_run_info_treats_empty_destination_name_as_absent():
    """The server returns an empty string for `destination_connection_name` when there is no destination."""
    run = RunInfo.model_validate(
        {
            "id": 1,
            "status": "finished",
            "mask_type": "database",
            "source_connection_name": "prod",
            "destination_connection_name": "",
            "ruleset_name": "rs",
        }
    )
    assert run.destination_connection is None


def test_masking_run_request_model_dump_includes_set_fields():
    req = MaskingRunRequest(
        connection="conn-1",
        ruleset="rs-1",
        destination_connection="conn-2",
        mask_type="file",
        options=MaskingRunOptions(batch_size=100, dry_run=True),
        name="my-run",
    )
    assert req.model_dump(exclude_none=True, mode="json") == {
        "connection": "conn-1",
        "ruleset": "rs-1",
        "destination_connection": "conn-2",
        "mask_type": "file",
        "options": {"batch_size": 100, "dry_run": True},
        "name": "my-run",
    }


def test_start_masking_run_accepts_typed_request(client):
    """A `MaskingRunRequest` is converted to its dict body before being sent."""
    req = MaskingRunRequest(connection="conn-1", ruleset="rs-1")

    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/runs/",
            json={"id": "42", "name": "the-run"},
            status_code=201,
        )
        run_id = client.start_masking_run(req)

    assert run_id == "42"
    sent_body = m.last_request.json()
    assert sent_body == {"connection": "conn-1", "mask_type": "database", "ruleset": "rs-1", "options": {}}


def test_cancel_run_returns_updated_run_info(client):
    """A successful cancel returns the run record with `cancelling` status."""
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/runs/42/cancel/",
            json={
                "id": 42,
                "status": "cancelling",
                "name": "the-run",
                "mask_type": "database",
                "source_connection_name": "conn1",
                "ruleset_name": "rs1",
            },
            status_code=200,
        )
        result = client.cancel_run(RunId(42))

    assert isinstance(result, RunInfo)
    assert result.id == 42
    assert result.status is MaskingRunStatus.cancelling
    assert m.last_request.method == "POST"
    # No body is sent — `cancel_run` is a pure command.
    assert m.last_request.body is None


def test_cancel_run_raises_run_not_cancellable_on_400(client):
    """A 400 means the run is in a state that cannot transition to cancelling."""
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/runs/42/cancel/",
            json={"detail": "Run is already finished"},
            status_code=400,
        )
        with pytest.raises(RunNotCancellableError, match="Run 42 cannot be cancelled"):
            client.cancel_run(RunId(42))


def test_cancel_run_raises_api_error_on_500(client):
    """Non-400 errors propagate as the generic `DataMasqueApiError`."""
    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/runs/42/cancel/", status_code=500)
        with pytest.raises(DataMasqueApiError):
            client.cancel_run(RunId(42))
