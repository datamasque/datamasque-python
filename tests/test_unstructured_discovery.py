"""Tests for reading an unstructured discovery run's results."""

from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any, Optional

import pytest
import requests_mock

from datamasque.client import DataMasqueClient
from datamasque.client.exceptions import DataMasqueApiError
from datamasque.client.models.connection import ConnectionId
from datamasque.client.models.runs import RunId
from datamasque.client.models.unstructured_discovery import (
    UnstructuredDiscoveryDirectoryOrdering,
    UnstructuredDiscoveryFileOutcome,
    UnstructuredDiscoveryFileStatus,
    UnstructuredDiscoverySampling,
)

RESULTS_URL = "http://test-server/api/runs/41/unstructured-discovery/"
REPORT_URL = "http://test-server/api/runs/41/unstructured-discovery/report/"
CONNECTION_ID = "aaaaaaaa-1111-2222-3333-444444444444"

ClientCall = Callable[[DataMasqueClient], object]


def _envelope(results: list[dict], **overrides: Any) -> dict:
    return {
        "run_id": 41,
        "connection": {"id": CONNECTION_ID, "name": "invoices"},
        "finished_at": "2026-09-04T02:11:07Z",
        "sampling": "sampled",
        "files_per_directory": 3,
        "count": len(results),
        "next": None,
        "previous": None,
        "results": results,
        **overrides,
    }


def _directory(directory: str) -> dict:
    return {
        "directory": directory,
        "files_found": 340,
        "files_read": 20,
        "files_with_findings": 14,
        "size_bytes": 14399,
        "entity_counts": {"CREDIT_CARD": 3, "EMAIL": 62},
        "skipped": [{"skip_reason": "Not selected by the per-directory sample", "file_extension": "pdf", "count": 318}],
    }


def _file(
    path: str,
    *,
    status: str = "read",
    skip_reason: str = "",
    character_count: Optional[int] = 5120,
    findings: Optional[list[dict]] = None,
) -> dict:
    findings = findings if findings is not None else []
    if status != "read":
        outcome = "failed"
    elif findings:
        outcome = "findings"
    else:
        outcome = "no_text" if character_count == 0 else "nothing_found"
    return {
        "path": path,
        "file_extension": "pdf",
        "size_bytes": 204817,
        "character_count": character_count,
        "status": status,
        "skip_reason": skip_reason,
        "outcome": outcome,
        "findings": findings,
    }


@pytest.mark.parametrize(
    "last_run_field, expected",
    [
        ({"last_unstructured_discovery_run_id": 19}, RunId(19)),
        ({"last_unstructured_discovery_run_id": None}, None),
    ],
)
def test_get_last_unstructured_discovery_run_id_reads_the_connection(
    client: DataMasqueClient, existing_connection_json: dict, last_run_field: dict, expected: Optional[RunId]
) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            f"http://test-server/api/connections/{CONNECTION_ID}/",
            json=existing_connection_json | {"id": CONNECTION_ID} | last_run_field,
        )

        assert client.get_last_unstructured_discovery_run_id(ConnectionId(CONNECTION_ID)) == expected


def test_get_last_unstructured_discovery_run_id_raises_on_a_server_without_the_field(
    client: DataMasqueClient, existing_connection_json: dict
) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            f"http://test-server/api/connections/{CONNECTION_ID}/",
            json=existing_connection_json | {"id": CONNECTION_ID},
        )

        with pytest.raises(KeyError) as exc_info:
            client.get_last_unstructured_discovery_run_id(ConnectionId(CONNECTION_ID))

    assert exc_info.value.args == ("last_unstructured_discovery_run_id",)


def test_get_unstructured_discovery_directory_page_parses_the_run_and_its_directories(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.get(RESULTS_URL, json=_envelope([_directory("finance/invoices")]))
        page = client.get_unstructured_discovery_directory_page(RunId(41), limit=10, offset=20)

    assert m.last_request.qs == {"ordering": ["directory"], "limit": ["10"], "offset": ["20"]}
    assert page.run_id == 41
    assert page.connection.id == CONNECTION_ID
    assert page.connection.name == "invoices"
    assert page.finished_at == datetime(2026, 9, 4, 2, 11, 7, tzinfo=timezone.utc)
    assert page.sampling is UnstructuredDiscoverySampling.sampled
    assert page.files_per_directory == 3
    assert page.count == 1

    directory = page.results[0]
    assert directory.directory == "finance/invoices"
    assert directory.files_found == 340
    assert directory.files_read == 20
    assert directory.files_with_findings == 14
    assert directory.size_bytes == 14399
    assert directory.entity_counts == {"CREDIT_CARD": 3, "EMAIL": 62}
    assert directory.skipped[0].skip_reason == "Not selected by the per-directory sample"
    assert directory.skipped[0].file_extension == "pdf"
    assert directory.skipped[0].count == 318


def test_get_unstructured_discovery_directory_page_asks_for_the_first_page_by_default(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.get(RESULTS_URL, json=_envelope([], sampling="all", files_per_directory=None, finished_at=None))
        page = client.get_unstructured_discovery_directory_page(RunId(41))

    assert m.last_request.qs == {"ordering": ["directory"], "limit": ["50"], "offset": ["0"]}
    assert page.results == []
    assert page.sampling is UnstructuredDiscoverySampling.all
    assert page.files_per_directory is None
    assert page.finished_at is None


@pytest.mark.parametrize(
    "call, expected_limit",
    [
        (lambda client, **filters: client.get_unstructured_discovery_directory_page(RunId(41), **filters), "50"),
        (lambda client, **filters: list(client.iter_unstructured_discovery_directories(RunId(41), **filters)), "1000"),
    ],
    ids=["page", "iterator"],
)
def test_unstructured_discovery_directories_send_the_search_and_label_filters(
    client: DataMasqueClient, call: Callable[..., object], expected_limit: str
) -> None:
    with requests_mock.Mocker(case_sensitive=True) as m:
        m.get(RESULTS_URL, json=_envelope([_directory("Finance/Invoices")]))
        call(client, search="Invoices", labels=["EMAIL", "CREDIT_CARD"], categories=["PII", "Financial"])

    assert m.last_request.qs == {
        "search": ["Invoices"],
        "labels": ["EMAIL,CREDIT_CARD"],
        "categories": ["PII,Financial"],
        "ordering": ["directory"],
        "limit": [expected_limit],
        "offset": ["0"],
    }


@pytest.mark.parametrize(
    "ordering, descending, expected_ordering",
    [
        (UnstructuredDiscoveryDirectoryOrdering.directory, True, "-directory"),
        (UnstructuredDiscoveryDirectoryOrdering.files_found, False, "files_found"),
        (UnstructuredDiscoveryDirectoryOrdering.files_read, True, "-files_read"),
        (UnstructuredDiscoveryDirectoryOrdering.files_with_findings, True, "-files_with_findings"),
        (UnstructuredDiscoveryDirectoryOrdering.size_bytes, False, "size_bytes"),
        (UnstructuredDiscoveryDirectoryOrdering.occurrences, True, "-occurrences"),
    ],
)
@pytest.mark.parametrize(
    "call",
    [
        lambda client, **ordering: client.get_unstructured_discovery_directory_page(RunId(41), **ordering),
        lambda client, **ordering: list(client.iter_unstructured_discovery_directories(RunId(41), **ordering)),
    ],
    ids=["page", "iterator"],
)
def test_unstructured_discovery_directories_send_the_ordering(
    client: DataMasqueClient,
    call: Callable[..., object],
    ordering: UnstructuredDiscoveryDirectoryOrdering,
    descending: bool,
    expected_ordering: str,
) -> None:
    with requests_mock.Mocker(case_sensitive=True) as m:
        m.get(RESULTS_URL, json=_envelope([_directory("Finance/Invoices")]))
        call(client, ordering=ordering, descending=descending)

    assert m.last_request.qs["ordering"] == [expected_ordering]


def test_get_unstructured_discovery_file_page_parses_the_files_and_their_findings(client: DataMasqueClient) -> None:
    finding = {"label": "EMAIL", "description": "Email address", "occurrences": 4, "categories": ["PII"]}
    rows = [
        _file("Finance/Invoices/0142.pdf", findings=[finding]),
        _file(
            "Finance/Invoices/0143.pdf",
            status="failed",
            skip_reason="An error occurred while running data discovery",
            character_count=None,
        ),
    ]
    with requests_mock.Mocker(case_sensitive=True) as m:
        m.get(RESULTS_URL, json=_envelope(rows))
        page = client.get_unstructured_discovery_file_page(
            RunId(41), "Finance/Invoices", status=UnstructuredDiscoveryFileStatus.read, limit=1000, offset=5
        )

    assert m.last_request.qs == {
        "directory": ["Finance/Invoices"],
        "status": ["read"],
        "limit": ["1000"],
        "offset": ["5"],
    }
    read_file, failed_file = page.results
    assert read_file.path == "Finance/Invoices/0142.pdf"
    assert read_file.file_extension == "pdf"
    assert read_file.size_bytes == 204817
    assert read_file.character_count == 5120
    assert read_file.status is UnstructuredDiscoveryFileStatus.read
    assert read_file.outcome is UnstructuredDiscoveryFileOutcome.findings
    assert read_file.skip_reason == ""
    assert read_file.findings[0].label == "EMAIL"
    assert read_file.findings[0].description == "Email address"
    assert read_file.findings[0].occurrences == 4
    assert read_file.findings[0].categories == ["PII"]
    assert failed_file.status is UnstructuredDiscoveryFileStatus.failed
    assert failed_file.outcome is UnstructuredDiscoveryFileOutcome.failed
    assert failed_file.skip_reason == "An error occurred while running data discovery"
    assert failed_file.character_count is None
    assert failed_file.findings == []
    assert not read_file.model_extra
    assert not failed_file.model_extra


def test_get_unstructured_discovery_file_page_sends_a_blank_directory_for_the_root(client: DataMasqueClient) -> None:
    with requests_mock.Mocker(case_sensitive=True) as m:
        m.get(RESULTS_URL, json=_envelope([_file("notes.pdf")]))
        page = client.get_unstructured_discovery_file_page(RunId(41), "")

    assert m.last_request.qs == {"directory": [""], "limit": ["50"], "offset": ["0"]}
    assert page.results[0].path == "notes.pdf"


def test_iter_unstructured_discovery_directories_follows_the_next_page(client: DataMasqueClient) -> None:
    next_url = f"{RESULTS_URL}?limit=1000&offset=1000"
    with requests_mock.Mocker() as m:
        m.get(
            RESULTS_URL, [{"json": _envelope([_directory("a")], next=next_url)}, {"json": _envelope([_directory("b")])}]
        )
        directories = list(client.iter_unstructured_discovery_directories(RunId(41)))

    assert [directory.directory for directory in directories] == ["a", "b"]
    assert m.request_history[0].qs == {"ordering": ["directory"], "limit": ["1000"], "offset": ["0"]}
    assert m.request_history[1].url == next_url


@pytest.mark.parametrize(
    "status, expected_status_query",
    [(None, {}), (UnstructuredDiscoveryFileStatus.failed, {"status": ["failed"]})],
)
def test_iter_unstructured_discovery_files_asks_for_one_directory(
    client: DataMasqueClient,
    status: Optional[UnstructuredDiscoveryFileStatus],
    expected_status_query: dict[str, list[str]],
) -> None:
    with requests_mock.Mocker(case_sensitive=True) as m:
        m.get(RESULTS_URL, json=_envelope([_file("claims/1.pdf"), _file("claims/2.pdf")]))
        files = list(client.iter_unstructured_discovery_files(RunId(41), "claims", status=status))

    assert [file.path for file in files] == ["claims/1.pdf", "claims/2.pdf"]
    assert m.last_request.qs == {
        "directory": ["claims"],
        "limit": ["1000"],
        "offset": ["0"],
        **expected_status_query,
    }


@pytest.mark.parametrize(
    "call, expected_limit",
    [
        (lambda client, **filters: client.get_unstructured_discovery_file_page(RunId(41), "claims", **filters), "50"),
        (
            lambda client, **filters: list(client.iter_unstructured_discovery_files(RunId(41), "claims", **filters)),
            "1000",
        ),
    ],
    ids=["page", "iterator"],
)
def test_unstructured_discovery_files_send_the_search_label_and_outcome_filters(
    client: DataMasqueClient, call: Callable[..., object], expected_limit: str
) -> None:
    with requests_mock.Mocker(case_sensitive=True) as m:
        m.get(RESULTS_URL, json=_envelope([_file("claims/Claim-1.pdf")]))
        call(
            client,
            search="Claim",
            labels=["EMAIL", "CREDIT_CARD"],
            categories=["PII", "Financial"],
            outcomes=[UnstructuredDiscoveryFileOutcome.no_text, UnstructuredDiscoveryFileOutcome.failed],
        )

    assert m.last_request.qs == {
        "directory": ["claims"],
        "search": ["Claim"],
        "labels": ["EMAIL,CREDIT_CARD"],
        "categories": ["PII,Financial"],
        "outcome": ["no_text,failed"],
        "limit": [expected_limit],
        "offset": ["0"],
    }


def test_get_unstructured_discovery_summary_parses_the_totals_and_each_label(client: DataMasqueClient) -> None:
    totals = {
        "run_id": 41,
        "files_found": 340,
        "files_read": 20,
        "files_failed": 2,
        "files_skipped": 318,
        "files_without_text": 3,
        "files_with_findings": 14,
        "bytes_opened": 8123344,
        "files_over_run_limit": 7,
        "labels": [
            {
                "label": "EMAIL",
                "description": "Email address",
                "categories": ["PII", "Contact"],
                "occurrences": 62,
                "file_count": 11,
            }
        ],
    }
    with requests_mock.Mocker() as m:
        m.get(f"{RESULTS_URL}summary/", json=totals)
        summary = client.get_unstructured_discovery_summary(RunId(41))

    assert summary.run_id == 41
    assert summary.files_found == 340
    assert summary.files_read == 20
    assert summary.files_failed == 2
    assert summary.files_skipped == 318
    assert summary.files_without_text == 3
    assert summary.files_with_findings == 14
    assert summary.bytes_opened == 8123344
    assert summary.files_over_run_limit == 7
    label_total = summary.labels[0]
    assert label_total.label == "EMAIL"
    assert label_total.description == "Email address"
    assert label_total.categories == ["PII", "Contact"]
    assert label_total.occurrences == 62
    assert label_total.file_count == 11
    assert not summary.model_extra
    assert not label_total.model_extra


def test_get_unstructured_report_returns_the_csv_text(client: DataMasqueClient) -> None:
    csv_text = (
        "\ufeffpath,directory,file_name,label,description,occurrences,categories,character_count\r\n"
        "réunions/0142.pdf,réunions,0142.pdf,EMAIL,Email address,4,PII;Contact,5120\r\n"
        "réunions/0143.pdf,réunions,0143.pdf,,,0,,96\r\n"
    )
    with requests_mock.Mocker() as m:
        m.get(
            REPORT_URL,
            content=csv_text.encode("utf-8"),
            headers={"Content-Type": "text/csv; charset=utf-8"},
        )

        assert client.get_unstructured_report(RunId(41)) == csv_text


def test_get_unstructured_report_returns_zip_bytes_when_split(client: DataMasqueClient) -> None:
    zip_bytes = b"PK\x03\x04 split report zip bytes"
    with requests_mock.Mocker() as m:
        m.get(REPORT_URL, content=zip_bytes, headers={"Content-Type": "application/zip"})

        assert client.get_unstructured_report(RunId(41)) == zip_bytes


@pytest.mark.parametrize(
    "url, call",
    [
        (RESULTS_URL, lambda client: client.get_unstructured_discovery_directory_page(RunId(41))),
        (RESULTS_URL, lambda client: client.get_unstructured_discovery_file_page(RunId(41), "")),
        (f"{RESULTS_URL}summary/", lambda client: client.get_unstructured_discovery_summary(RunId(41))),
        (REPORT_URL, lambda client: client.get_unstructured_report(RunId(41))),
    ],
)
def test_reading_a_run_that_is_not_unstructured_discovery_raises(
    client: DataMasqueClient, url: str, call: ClientCall
) -> None:
    with requests_mock.Mocker() as m:
        m.get(url, json={"detail": "No Run matches the given query."}, status_code=404)
        with pytest.raises(DataMasqueApiError) as exc_info:
            call(client)

    assert exc_info.value.response.status_code == 404
