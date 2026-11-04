"""Tests for reading an unstructured discovery run's results."""

from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any, Optional

import pytest
import requests_mock

from datamasque.client import DataMasqueClient
from datamasque.client.exceptions import DataMasqueApiError
from datamasque.client.models.runs import RunId
from datamasque.client.models.unstructured_discovery import (
    UnstructuredDiscoveryFileStatus,
    UnstructuredDiscoverySampling,
)

RESULTS_URL = "http://test-server/api/runs/41/unstructured-discovery/"
REPORT_URL = "http://test-server/api/runs/41/unstructured-report/"
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


def _file(path: str, *, status: str = "read", skip_reason: str = "", findings: Optional[list[dict]] = None) -> dict:
    return {
        "path": path,
        "file_extension": "pdf",
        "size_bytes": 204817,
        "status": status,
        "skip_reason": skip_reason,
        "findings": findings if findings is not None else [],
    }


def test_get_unstructured_discovery_directory_page_parses_the_run_and_its_directories(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.get(RESULTS_URL, json=_envelope([_directory("finance/invoices")]))
        page = client.get_unstructured_discovery_directory_page(RunId(41), limit=10, offset=20)

    assert m.last_request.qs == {"limit": ["10"], "offset": ["20"]}
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

    assert m.last_request.qs == {"limit": ["50"], "offset": ["0"]}
    assert page.results == []
    assert page.sampling is UnstructuredDiscoverySampling.all
    assert page.files_per_directory is None
    assert page.finished_at is None


def test_get_unstructured_discovery_file_page_parses_the_files_and_their_findings(client: DataMasqueClient) -> None:
    finding = {"label": "EMAIL", "description": "Email address", "occurrences": 4, "categories": ["PII"]}
    rows = [
        _file("Finance/Invoices/0142.pdf", findings=[finding]),
        _file(
            "Finance/Invoices/0143.pdf", status="unread", skip_reason="An error occurred while running data discovery"
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
    read_file, unread_file = page.results
    assert read_file.path == "Finance/Invoices/0142.pdf"
    assert read_file.file_extension == "pdf"
    assert read_file.size_bytes == 204817
    assert read_file.status is UnstructuredDiscoveryFileStatus.read
    assert read_file.skip_reason == ""
    assert read_file.findings[0].label == "EMAIL"
    assert read_file.findings[0].description == "Email address"
    assert read_file.findings[0].occurrences == 4
    assert read_file.findings[0].categories == ["PII"]
    assert unread_file.status is UnstructuredDiscoveryFileStatus.unread
    assert unread_file.skip_reason == "An error occurred while running data discovery"
    assert unread_file.findings == []


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
    assert m.request_history[0].qs == {"limit": ["1000"], "offset": ["0"]}
    assert m.request_history[1].url == next_url


@pytest.mark.parametrize(
    "status, expected_status_query",
    [(None, {}), (UnstructuredDiscoveryFileStatus.unread, {"status": ["unread"]})],
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


def test_get_unstructured_discovery_summary(client: DataMasqueClient) -> None:
    totals = {"run_id": 41, "files_found": 340, "files_read": 20, "files_with_findings": 14, "bytes_opened": 8123344}
    with requests_mock.Mocker() as m:
        m.get(f"{RESULTS_URL}summary/", json=totals)
        summary = client.get_unstructured_discovery_summary(RunId(41))

    assert summary.run_id == 41
    assert summary.files_found == 340
    assert summary.files_read == 20
    assert summary.files_with_findings == 14
    assert summary.bytes_opened == 8123344
    assert not summary.model_extra


def test_get_unstructured_report_returns_the_csv_text(client: DataMasqueClient) -> None:
    csv_text = (
        "\ufeffpath,directory,file_name,label,description,occurrences,categories\r\n"
        "réunions/0142.pdf,réunions,0142.pdf,EMAIL,Email address,4,PII;Contact\r\n"
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
