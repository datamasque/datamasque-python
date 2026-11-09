from typing import Iterator, Optional, Union

from datamasque.client.base import BaseClient
from datamasque.client.models.connection import ConnectionId
from datamasque.client.models.runs import RunId
from datamasque.client.models.unstructured_discovery import (
    UnstructuredDiscoveryDirectory,
    UnstructuredDiscoveryDirectoryOrdering,
    UnstructuredDiscoveryFile,
    UnstructuredDiscoveryFileOutcome,
    UnstructuredDiscoveryFileStatus,
    UnstructuredDiscoveryPage,
    UnstructuredDiscoverySummary,
)

SERVER_MAX_PAGE_SIZE = 1000
LIST_SEPARATOR = ","
DESCENDING_PREFIX = "-"


class UnstructuredDiscoveryClient(BaseClient):
    """
    Unstructured discovery result API methods. Mixed into `DataMasqueClient`.

    Each method that takes a run ID raises `DataMasqueApiError` with a 404 response
    when the run is not an unstructured discovery run.
    """

    @staticmethod
    def _build_label_filter_params(
        search: Optional[str], labels: Optional[list[str]], categories: Optional[list[str]]
    ) -> dict[str, str]:
        params = {}
        if search is not None:
            params["search"] = search
        if labels is not None:
            params["labels"] = LIST_SEPARATOR.join(labels)
        if categories is not None:
            params["categories"] = LIST_SEPARATOR.join(categories)

        return params

    @staticmethod
    def _build_unstructured_directory_page_params(
        search: Optional[str],
        labels: Optional[list[str]],
        categories: Optional[list[str]],
        ordering: UnstructuredDiscoveryDirectoryOrdering,
        is_descending: bool,
    ) -> dict[str, str]:
        params = UnstructuredDiscoveryClient._build_label_filter_params(search, labels, categories)
        params["ordering"] = f"{DESCENDING_PREFIX}{ordering.value}" if is_descending else ordering.value
        return params

    @staticmethod
    def _build_unstructured_file_page_params(
        directory: str,
        status: Optional[UnstructuredDiscoveryFileStatus],
        search: Optional[str],
        labels: Optional[list[str]],
        categories: Optional[list[str]],
        outcomes: Optional[list[UnstructuredDiscoveryFileOutcome]],
    ) -> dict[str, str]:
        params = {"directory": directory}
        if status is not None:
            params["status"] = status.value
        params |= UnstructuredDiscoveryClient._build_label_filter_params(search, labels, categories)
        if outcomes is not None:
            params["outcome"] = LIST_SEPARATOR.join(outcome.value for outcome in outcomes)

        return params

    def get_last_unstructured_discovery_run_id(self, connection_id: ConnectionId) -> Optional[RunId]:
        """
        Returns the ID of a connection's newest finished unstructured discovery run, with or without warnings.

        Returns `None` when no unstructured discovery run on the connection has finished.
        """

        response = self.make_request("GET", f"/api/connections/{connection_id}/")
        run_id = response.json()["last_unstructured_discovery_run_id"]
        return RunId(run_id) if run_id is not None else None

    def get_unstructured_discovery_directory_page(
        self,
        run_id: RunId,
        *,
        search: Optional[str] = None,
        labels: Optional[list[str]] = None,
        categories: Optional[list[str]] = None,
        ordering: UnstructuredDiscoveryDirectoryOrdering = UnstructuredDiscoveryDirectoryOrdering.directory,
        descending: bool = False,
        limit: int = 50,
        offset: int = 0,
    ) -> UnstructuredDiscoveryPage[UnstructuredDiscoveryDirectory]:
        """
        Returns one page of the directories an unstructured discovery run found on the connection.

        `search` keeps the directories whose path contains it, ignoring case,
        and the directories holding a file the run tried to read whose name contains it.
        `labels` keeps the directories holding any of those labels,
        and `categories` the directories holding a label that carries any of those categories;
        given both, a label must match both.
        Files with no text to search and files the run could not read hold no label,
        so they do not keep a directory.
        With `labels` or `categories`, a file matches `search` only if it holds a matching label,
        or the run read it and found no text to search in it;
        a file the run could not read never matches.
        Each directory's `entity_counts` still counts every label it holds.
        The directories come in `ordering`, descending when `descending` is set,
        and directories that tie are sorted by path, ascending.
        """

        params = self._build_unstructured_directory_page_params(search, labels, categories, ordering, descending) | {
            "limit": limit,
            "offset": offset,
        }
        response = self.make_request("GET", f"/api/runs/{run_id}/unstructured-discovery/", params=params)
        return UnstructuredDiscoveryPage[UnstructuredDiscoveryDirectory].model_validate(response.json())

    # `iter_unstructured_discovery_directories` and `iter_unstructured_discovery_files` have no `list_` counterparts,
    # because a run's directory and file counts are unbounded.
    def iter_unstructured_discovery_directories(
        self,
        run_id: RunId,
        *,
        search: Optional[str] = None,
        labels: Optional[list[str]] = None,
        categories: Optional[list[str]] = None,
        ordering: UnstructuredDiscoveryDirectoryOrdering = UnstructuredDiscoveryDirectoryOrdering.directory,
        descending: bool = False,
    ) -> Iterator[UnstructuredDiscoveryDirectory]:
        """
        Lazily iterate every directory an unstructured discovery run found on the connection.

        `search`, `labels`, `categories`, `ordering` and `descending`
        are as for `get_unstructured_discovery_directory_page`.
        """

        return self._iter_paginated(
            f"/api/runs/{run_id}/unstructured-discovery/",
            model=UnstructuredDiscoveryDirectory,
            params=self._build_unstructured_directory_page_params(search, labels, categories, ordering, descending),
            page_size=SERVER_MAX_PAGE_SIZE,
        )

    def get_unstructured_discovery_file_page(
        self,
        run_id: RunId,
        directory: str,
        *,
        status: Optional[UnstructuredDiscoveryFileStatus] = None,
        search: Optional[str] = None,
        labels: Optional[list[str]] = None,
        categories: Optional[list[str]] = None,
        outcomes: Optional[list[UnstructuredDiscoveryFileOutcome]] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> UnstructuredDiscoveryPage[UnstructuredDiscoveryFile]:
        """
        Returns one page of the files an unstructured discovery run tried to read in one directory.

        `directory` is a directory's path exactly as the directory pages give it;
        an empty string names the connection root.
        `status` keeps only the files with that status,
        and `outcomes` only the files with any of those outcomes.
        `search` keeps the files whose name contains it, ignoring case,
        or every file when the directory's path contains it.
        `labels` keeps the files holding any of those labels,
        and `categories` the files holding a label that carries any of those categories;
        given both, a label must match both.
        Either one also keeps the files the run read and found no text to search in, which may hold any label,
        and drops the files the run could not read.
        The files with findings come first,
        then the files with no text to search, those with nothing found and those the run could not read,
        each group sorted by name.
        With `labels` or `categories`, a file still lists every finding.
        Files the run skipped have no row;
        the directory's `skipped` counts them.
        Files a running or cancelled run has not reached yet are in neither.
        """

        params = self._build_unstructured_file_page_params(directory, status, search, labels, categories, outcomes) | {
            "limit": limit,
            "offset": offset,
        }
        response = self.make_request("GET", f"/api/runs/{run_id}/unstructured-discovery/", params=params)
        return UnstructuredDiscoveryPage[UnstructuredDiscoveryFile].model_validate(response.json())

    def iter_unstructured_discovery_files(
        self,
        run_id: RunId,
        directory: str,
        *,
        status: Optional[UnstructuredDiscoveryFileStatus] = None,
        search: Optional[str] = None,
        labels: Optional[list[str]] = None,
        categories: Optional[list[str]] = None,
        outcomes: Optional[list[UnstructuredDiscoveryFileOutcome]] = None,
    ) -> Iterator[UnstructuredDiscoveryFile]:
        """
        Lazily iterate every file an unstructured discovery run tried to read in one directory.

        `directory`, `status`, `search`, `labels`, `categories` and `outcomes`
        are as for `get_unstructured_discovery_file_page`, and so is the order of the files.
        """

        return self._iter_paginated(
            f"/api/runs/{run_id}/unstructured-discovery/",
            model=UnstructuredDiscoveryFile,
            params=self._build_unstructured_file_page_params(directory, status, search, labels, categories, outcomes),
            page_size=SERVER_MAX_PAGE_SIZE,
        )

    def get_unstructured_discovery_summary(self, run_id: RunId) -> UnstructuredDiscoverySummary:
        """
        Returns an unstructured discovery run's totals, and how many times it found each label.

        They are counted from the run's stored results on each request,
        so they change while results are still being stored.
        """

        response = self.make_request("GET", f"/api/runs/{run_id}/unstructured-discovery/summary/")
        return UnstructuredDiscoverySummary.model_validate(response.json())

    def get_unstructured_report(self, run_id: RunId) -> Union[str, bytes]:
        """
        Returns an unstructured discovery run's findings, one row for each label found in each file.

        A file the run read and found nothing in has one row,
        with a blank label, description and categories, and 0 occurrences.
        A file the run could not read has no row.
        Every row carries its file's `character_count`, blank where the run reported none.
        Returns CSV text (`str`) that starts with a byte-order mark,
        or a zip of numbered CSV parts as `bytes` when the server splits a report of more than 1,000,000 rows.
        The first row of each CSV names the columns.
        A label's categories share one cell, joined with `;`.
        """

        response = self.make_request("GET", f"/api/runs/{run_id}/unstructured-discovery/report/")
        if response.headers.get("Content-Type", "").startswith("application/zip"):
            return response.content
        return response.text
