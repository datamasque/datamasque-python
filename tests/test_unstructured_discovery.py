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
    UnstructuredDiscoverySampleOrder,
)

RESULTS_URL = "http://test-server/api/runs/41/unstructured-discovery/"
DIRECTORIES_URL = f"{RESULTS_URL}directories/"
FILES_URL = f"{RESULTS_URL}files/"
REPORT_URL = f"{RESULTS_URL}report/"
RUNS_URL = "http://test-server/api/runs/"
CONNECTION_ID = "aaaaaaaa-1111-2222-3333-444444444444"

ClientCall = Callable[[DataMasqueClient], object]


def _page(results: list[dict], **overrides: Any) -> dict:
    return {"count": len(results), "next": None, "previous": None, "results": results, **overrides}


def _directory(directory: str) -> dict:
    return {
        "directory": directory,
        "files_found": 340,
        "files_read": 20,
        "files_with_findings": 14,
        "size_bytes": 14399,
        "entity_counts": {"CREDIT_CARD": 3, "EMAIL": 62},
        "skipped": [
            {
                "skip_reason": "over_sample_limit",
                "skip_reason_text": "Not selected by the per-directory sample",
                "file_extension": "pdf",
                "count": 318,
            }
        ],
    }


def _file(
    path: str,
    *,
    status: str = "read",
    skip_reason: str = "",
    skip_reason_text: str = "",
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
        "skip_reason_text": skip_reason_text,
        "outcome": outcome,
        "findings": findings,
    }


@pytest.mark.parametrize(
    "runs, expected",
    [([{"id": 19, "status": "finished"}], RunId(19)), ([], None)],
    ids=["finished_run", "no_finished_run"],
)
def test_get_last_unstructured_discovery_run_id_asks_the_run_list(
    client: DataMasqueClient, runs: list[dict], expected: Optional[RunId]
) -> None:
    with requests_mock.Mocker() as m:
        m.get(RUNS_URL, json=_page(runs))

        assert client.get_last_unstructured_discovery_run_id(ConnectionId(CONNECTION_ID)) == expected

    assert m.last_request.qs == {
        "discovery_kind": ["unstructured"],
        "source_connection": [CONNECTION_ID],
        "dry_run": ["false"],
        "run_status": ["finished", "finished_with_warnings"],
        "ordering": ["-id"],
        "limit": ["1"],
    }


def test_get_unstructured_discovery_directory_page_parses_the_directories(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.get(DIRECTORIES_URL, json=_page([_directory("finance/invoices")]))
        page = client.get_unstructured_discovery_directory_page(RunId(41), limit=10, offset=20)

    assert m.last_request.qs == {"ordering": ["directory"], "limit": ["10"], "offset": ["20"]}
    assert page.count == 1
    assert not page.model_extra

    directory = page.results[0]
    assert directory.directory == "finance/invoices"
    assert directory.files_found == 340
    assert directory.files_read == 20
    assert directory.files_with_findings == 14
    assert directory.size_bytes == 14399
    assert directory.entity_counts == {"CREDIT_CARD": 3, "EMAIL": 62}
    assert directory.skipped[0].skip_reason == "over_sample_limit"
    assert directory.skipped[0].skip_reason_text == "Not selected by the per-directory sample"
    assert directory.skipped[0].file_extension == "pdf"
    assert directory.skipped[0].count == 318
    assert not directory.skipped[0].model_extra


def test_get_unstructured_discovery_directory_page_asks_for_the_first_page_by_default(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.get(DIRECTORIES_URL, json=_page([]))
        page = client.get_unstructured_discovery_directory_page(RunId(41))

    assert m.last_request.qs == {"ordering": ["directory"], "limit": ["50"], "offset": ["0"]}
    assert page.results == []


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
        m.get(DIRECTORIES_URL, json=_page([_directory("Finance/Invoices")]))
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
        m.get(DIRECTORIES_URL, json=_page([_directory("Finance/Invoices")]))
        call(client, ordering=ordering, descending=descending)

    assert m.last_request.qs["ordering"] == [expected_ordering]


def test_get_unstructured_discovery_file_page_parses_the_files_and_their_findings(client: DataMasqueClient) -> None:
    finding = {"label": "EMAIL", "occurrences": 4}
    rows = [
        _file("Finance/Invoices/0142.pdf", findings=[finding]),
        _file(
            "Finance/Invoices/0143.pdf",
            status="failed",
            skip_reason="unscannable_file",
            skip_reason_text="File is encrypted, damaged or too large to unpack",
            character_count=None,
        ),
    ]
    with requests_mock.Mocker(case_sensitive=True) as m:
        m.get(FILES_URL, json=_page(rows))
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
    assert read_file.skip_reason_text == ""
    assert read_file.findings[0].label == "EMAIL"
    assert read_file.findings[0].occurrences == 4
    assert not read_file.findings[0].model_extra
    assert failed_file.status is UnstructuredDiscoveryFileStatus.failed
    assert failed_file.outcome is UnstructuredDiscoveryFileOutcome.failed
    assert failed_file.skip_reason == "unscannable_file"
    assert failed_file.skip_reason_text == "File is encrypted, damaged or too large to unpack"
    assert failed_file.character_count is None
    assert failed_file.findings == []
    assert not read_file.model_extra
    assert not failed_file.model_extra


def test_get_unstructured_discovery_file_page_sends_a_blank_directory_for_the_root(client: DataMasqueClient) -> None:
    with requests_mock.Mocker(case_sensitive=True) as m:
        m.get(FILES_URL, json=_page([_file("notes.pdf")]))
        page = client.get_unstructured_discovery_file_page(RunId(41), "")

    assert m.last_request.qs == {"directory": [""], "limit": ["50"], "offset": ["0"]}
    assert page.results[0].path == "notes.pdf"


def test_iter_unstructured_discovery_directories_follows_the_next_page(client: DataMasqueClient) -> None:
    next_url = f"{DIRECTORIES_URL}?limit=1000&offset=1000"
    with requests_mock.Mocker() as m:
        m.get(DIRECTORIES_URL, [{"json": _page([_directory("a")], next=next_url)}, {"json": _page([_directory("b")])}])
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
        m.get(FILES_URL, json=_page([_file("claims/1.pdf"), _file("claims/2.pdf")]))
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
        m.get(FILES_URL, json=_page([_file("claims/Claim-1.pdf")]))
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
        "outcomes": ["no_text,failed"],
        "limit": [expected_limit],
        "offset": ["0"],
    }


def _summary(**overrides: Any) -> dict:
    return {
        "run_id": 41,
        "connection": {"id": CONNECTION_ID, "name": "invoices"},
        "status": "finished",
        "finished_at": "2026-09-04T02:11:07Z",
        "settings": {
            "files_per_directory": 20,
            "sample_order": "hash",
            "files_per_run": 100000,
            "max_bytes_per_file": 10000000,
        },
        "ai_engine": {"model_name": "dm-ai-ner-small-1.0.0-20260601", "engine_version": "1.26.11"},
        "report_rows": 15,
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
            },
            {"label": "IBAN", "description": "Bank account", "categories": ["PCI"], "occurrences": 0, "file_count": 0},
        ],
        **overrides,
    }


def test_get_unstructured_discovery_summary_parses_the_run_its_totals_and_each_label(
    client: DataMasqueClient,
) -> None:
    with requests_mock.Mocker() as m:
        m.get(f"{RESULTS_URL}summary/", json=_summary())
        summary = client.get_unstructured_discovery_summary(RunId(41))

    assert summary.run_id == 41
    assert summary.connection.id == CONNECTION_ID
    assert summary.connection.name == "invoices"
    assert summary.status == "finished"
    assert summary.finished_at == datetime(2026, 9, 4, 2, 11, 7, tzinfo=timezone.utc)
    assert summary.settings is not None
    assert summary.settings.files_per_directory == 20
    assert summary.settings.sample_order is UnstructuredDiscoverySampleOrder.hash
    assert summary.settings.files_per_run == 100000
    assert summary.settings.max_bytes_per_file == 10000000
    assert summary.ai_engine is not None
    assert summary.ai_engine.model_name == "dm-ai-ner-small-1.0.0-20260601"
    assert summary.ai_engine.engine_version == "1.26.11"
    assert summary.report_rows == 15
    assert summary.files_found == 340
    assert summary.files_read == 20
    assert summary.files_failed == 2
    assert summary.files_skipped == 318
    assert summary.files_without_text == 3
    assert summary.files_with_findings == 14
    assert summary.bytes_opened == 8123344
    assert summary.files_over_run_limit == 7
    label_total, never_found = summary.labels
    assert label_total.label == "EMAIL"
    assert label_total.description == "Email address"
    assert label_total.categories == ["PII", "Contact"]
    assert label_total.occurrences == 62
    assert label_total.file_count == 11
    assert never_found.occurrences == 0
    assert not summary.model_extra
    assert not summary.settings.model_extra
    assert not summary.ai_engine.model_extra
    assert not label_total.model_extra


def test_get_unstructured_discovery_summary_reads_a_run_without_settings_or_ai_engine(
    client: DataMasqueClient,
) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            f"{RESULTS_URL}summary/",
            json=_summary(
                status="failed",
                finished_at=None,
                settings=None,
                ai_engine=None,
                report_rows=0,
                labels=[],
            ),
        )
        summary = client.get_unstructured_discovery_summary(RunId(41))

    assert summary.finished_at is None
    assert summary.settings is None
    assert summary.ai_engine is None


def test_get_unstructured_discovery_summary_reads_all_and_an_engine_without_a_version(
    client: DataMasqueClient,
) -> None:
    settings = {"files_per_directory": None, "sample_order": "name_asc", "files_per_run": None, "max_bytes_per_file": 5}
    with requests_mock.Mocker() as m:
        m.get(
            f"{RESULTS_URL}summary/",
            json=_summary(settings=settings, ai_engine={"model_name": "m", "engine_version": None}),
        )
        summary = client.get_unstructured_discovery_summary(RunId(41))

    assert summary.settings is not None
    assert summary.settings.files_per_directory is None
    assert summary.settings.files_per_run is None
    assert summary.settings.sample_order is UnstructuredDiscoverySampleOrder.name_asc
    assert summary.ai_engine is not None
    assert summary.ai_engine.engine_version is None


def test_get_unstructured_discovery_summary_reads_an_engine_without_a_model_name(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.get(f"{RESULTS_URL}summary/", json=_summary(ai_engine={"model_name": None, "engine_version": "1.26.11"}))
        summary = client.get_unstructured_discovery_summary(RunId(41))

    assert summary.ai_engine is not None
    assert summary.ai_engine.model_name is None
    assert summary.ai_engine.engine_version == "1.26.11"


def test_get_unstructured_discovery_report_returns_the_csv_text(client: DataMasqueClient) -> None:
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

        assert client.get_unstructured_discovery_report(RunId(41)) == csv_text


def test_get_unstructured_discovery_report_returns_zip_bytes_when_split(client: DataMasqueClient) -> None:
    zip_bytes = b"PK\x03\x04 split report zip bytes"
    with requests_mock.Mocker() as m:
        m.get(REPORT_URL, content=zip_bytes, headers={"Content-Type": "application/zip"})

        assert client.get_unstructured_discovery_report(RunId(41)) == zip_bytes


@pytest.mark.parametrize(
    "url, call",
    [
        (DIRECTORIES_URL, lambda client: client.get_unstructured_discovery_directory_page(RunId(41))),
        (FILES_URL, lambda client: client.get_unstructured_discovery_file_page(RunId(41), "")),
        (f"{RESULTS_URL}summary/", lambda client: client.get_unstructured_discovery_summary(RunId(41))),
        (REPORT_URL, lambda client: client.get_unstructured_discovery_report(RunId(41))),
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
