from typing import Iterator, Optional, Union

from datamasque.client.base import BaseClient
from datamasque.client.models.connection import ConnectionId
from datamasque.client.models.runs import RunId
from datamasque.client.models.unstructured_discovery import (
    UnstructuredDiscoveryDirectory,
    UnstructuredDiscoveryFile,
    UnstructuredDiscoveryFileStatus,
    UnstructuredDiscoveryPage,
    UnstructuredDiscoverySummary,
)

SERVER_MAX_PAGE_SIZE = 1000


class UnstructuredDiscoveryClient(BaseClient):
    """
    Unstructured discovery result API methods. Mixed into `DataMasqueClient`.

    Each method that takes a run ID raises `DataMasqueApiError` with a 404 response
    when the run is not an unstructured discovery run.
    """

    @staticmethod
    def _build_unstructured_file_page_params(
        directory: str, status: Optional[UnstructuredDiscoveryFileStatus]
    ) -> dict[str, str]:
        params = {"directory": directory}
        if status is not None:
            params["status"] = status.value

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
        self, run_id: RunId, *, limit: int = 50, offset: int = 0
    ) -> UnstructuredDiscoveryPage[UnstructuredDiscoveryDirectory]:
        """Returns one page of the directories an unstructured discovery run found on the connection, ordered by path."""

        response = self.make_request(
            "GET",
            f"/api/runs/{run_id}/unstructured-discovery/",
            params={"limit": limit, "offset": offset},
        )
        return UnstructuredDiscoveryPage[UnstructuredDiscoveryDirectory].model_validate(response.json())

    # `iter_unstructured_discovery_directories` and `iter_unstructured_discovery_files` have no `list_` counterparts,
    # because a run's directory and file counts are unbounded.
    def iter_unstructured_discovery_directories(self, run_id: RunId) -> Iterator[UnstructuredDiscoveryDirectory]:
        """Lazily iterate every directory an unstructured discovery run found on the connection, ordered by path."""

        return self._iter_paginated(
            f"/api/runs/{run_id}/unstructured-discovery/",
            model=UnstructuredDiscoveryDirectory,
            page_size=SERVER_MAX_PAGE_SIZE,
        )

    def get_unstructured_discovery_file_page(
        self,
        run_id: RunId,
        directory: str,
        *,
        status: Optional[UnstructuredDiscoveryFileStatus] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> UnstructuredDiscoveryPage[UnstructuredDiscoveryFile]:
        """
        Returns one page of the files an unstructured discovery run tried to read in one directory, ordered by name.

        `directory` is a directory's path exactly as the directory pages give it;
        an empty string names the connection root.
        `status` keeps only the files with that status.
        Files the run skipped have no row;
        the directory's `skipped` counts them.
        Files a running or cancelled run has not reached yet are in neither.
        """

        params = self._build_unstructured_file_page_params(directory, status) | {"limit": limit, "offset": offset}
        response = self.make_request("GET", f"/api/runs/{run_id}/unstructured-discovery/", params=params)
        return UnstructuredDiscoveryPage[UnstructuredDiscoveryFile].model_validate(response.json())

    def iter_unstructured_discovery_files(
        self,
        run_id: RunId,
        directory: str,
        *,
        status: Optional[UnstructuredDiscoveryFileStatus] = None,
    ) -> Iterator[UnstructuredDiscoveryFile]:
        """
        Lazily iterate every file an unstructured discovery run tried to read in one directory, ordered by name.

        `directory` and `status` are as for `get_unstructured_discovery_file_page`.
        """

        return self._iter_paginated(
            f"/api/runs/{run_id}/unstructured-discovery/",
            model=UnstructuredDiscoveryFile,
            params=self._build_unstructured_file_page_params(directory, status),
            page_size=SERVER_MAX_PAGE_SIZE,
        )

    def get_unstructured_discovery_summary(self, run_id: RunId) -> UnstructuredDiscoverySummary:
        """
        Returns an unstructured discovery run's totals.

        They are counted from the run's stored results on each request,
        so they change while results are still being stored.
        """

        response = self.make_request("GET", f"/api/runs/{run_id}/unstructured-discovery/summary/")
        return UnstructuredDiscoverySummary.model_validate(response.json())

    def get_unstructured_report(self, run_id: RunId) -> Union[str, bytes]:
        """
        Returns an unstructured discovery run's findings, one row for each label found in each file.

        Returns CSV text (`str`) that starts with a byte-order mark,
        or a zip of numbered CSV parts as `bytes` when the server splits a report of more than 1,000,000 rows.
        The first row of each CSV names the columns.
        A label's categories share one cell, joined with `;`.
        """

        response = self.make_request("GET", f"/api/runs/{run_id}/unstructured-report/")
        if response.headers.get("Content-Type", "").startswith("application/zip"):
            return response.content
        return response.text
