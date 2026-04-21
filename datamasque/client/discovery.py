import logging
import zipfile
from io import BufferedIOBase, BytesIO, TextIOBase
from pathlib import Path
from typing import Iterator, Optional, Union

from datamasque.client.base import BaseClient, UploadFile
from datamasque.client.exceptions import (
    AsyncRulesetGenerationInProgressError,
    DataMasqueException,
    FailedToStartError,
)
from datamasque.client.models.connection import ConnectionId
from datamasque.client.models.data_selection import (
    SelectedColumns,
    SelectedData,
    SelectedFileData,
)
from datamasque.client.models.discovery import (
    FileDiscoveryResult,
    FileRulesetGenerationRequest,
    RulesetGenerationRequest,
    SchemaDiscoveryPage,
    SchemaDiscoveryRequest,
    SchemaDiscoveryResult,
)
from datamasque.client.models.ruleset import Ruleset
from datamasque.client.models.runs import RunId
from datamasque.client.models.status import AsyncRulesetGenerationTaskStatus

logger = logging.getLogger(__name__)


class DiscoveryClient(BaseClient):
    """Schema-discovery and ruleset-generation API methods. Mixed into `DataMasqueClient`."""

    def start_async_ruleset_generation(self, connection_id: ConnectionId, selected_data: SelectedData) -> None:
        """
        Starts async ruleset generation using the most recent discovery results on the given connection.

        If the connection is a database connection, `selected_data` should be of type `SelectedColumns`.
        If the connection is a file connection, `selected_data` should be of type `SelectedFileData`.

        Generation runs asynchronously on the server.
        Poll `get_async_ruleset_generation_task_status` until it returns
        `AsyncRulesetGenerationTaskStatus.finished`,
        then call `get_generated_rulesets` to retrieve the resulting `Ruleset`.
        """

        if not selected_data:
            raise ValueError("`selected_data` is a required argument to `start_async_ruleset_generation`.")

        data: dict = {}
        if isinstance(selected_data, SelectedColumns):
            data["selected_columns"] = selected_data.columns
            if selected_data.hash_columns is not None:
                data["hash_columns"] = {
                    schema: {table: cfg.model_dump(exclude_none=True) for table, cfg in tables.items()}
                    for schema, tables in selected_data.hash_columns.items()
                }
        elif isinstance(selected_data, SelectedFileData):
            for user_selection in selected_data.user_selections:
                if not (user_selection.locators and user_selection.files):
                    raise ValueError(
                        "Each `UserSelection` in `SelectedFileData.user_selections` "
                        "must have a non-null list of `locators` and `files` to be selected for."
                    )
            data["selected_data"] = [s.model_dump() for s in selected_data.user_selections]
        else:
            raise TypeError(
                f"The argument `selected_data` to `start_async_ruleset_generation` was of an invalid type, "
                f"expected `SelectedColumns` or `SelectedFileData`, got {type(selected_data)}."
            )

        self.make_request(method="POST", path=f"/api/async-generate-ruleset/{connection_id}/", data=data)

    def start_async_ruleset_generation_from_csv(
        self,
        connection_id: ConnectionId,
        csv_content: Union[str, bytes, TextIOBase, BufferedIOBase],
        target_size_bytes: Optional[int] = None,
    ) -> None:
        """
        Generate ruleset(s) from the schema discovery CSV file obtained from `get_db_discovery_result_report()`.

        `target_size_bytes` is an optional integer specifying the approximate size in bytes of each generated ruleset.

        `csv_content` can be:
        - A string (e.g. from `get_db_discovery_result_report()`)
        - Bytes
        - A text file handle (e.g. `open(path)`)
        - A binary file handle (e.g. `open(path, 'rb')`)

        Generation runs asynchronously on the server.
        Poll `get_async_ruleset_generation_task_status` until it returns
        `AsyncRulesetGenerationTaskStatus.finished`,
        then call `get_generated_rulesets` to retrieve the resulting `Ruleset` objects.
        """

        content: BufferedIOBase
        if isinstance(csv_content, str):
            content = BytesIO(csv_content.encode())
        elif isinstance(csv_content, bytes):
            content = BytesIO(csv_content)
        elif isinstance(csv_content, TextIOBase):
            content = BytesIO(csv_content.read().encode())
        else:
            content = csv_content

        files = [
            UploadFile(
                field_name="csv_or_zip_file",
                filename="ruleset.csv",
                content=content,
                content_type="text/csv",
            ),
        ]
        self.make_request(
            method="POST",
            path=f"/api/async-generate-ruleset/{connection_id}/from-csv/",
            data={"target_size_bytes": target_size_bytes} if target_size_bytes is not None else None,
            files=files,
        )

    def get_async_ruleset_generation_task_status(self, connection_id: ConnectionId) -> AsyncRulesetGenerationTaskStatus:
        """Queries the status of an async ruleset generation task."""

        response = self.make_request(method="GET", path=f"/api/async-generate-ruleset/{connection_id}/")
        response_data = response.json()
        status = response_data.get("status")
        if not status:
            raise DataMasqueException("Attempted to get an async ruleset generation task status but none was given.")

        return AsyncRulesetGenerationTaskStatus(status)

    def get_generated_rulesets(self, connection_id: ConnectionId) -> list[Ruleset]:
        """
        Return the `Ruleset` objects produced by a previously-started async ruleset generation.

        Use for all three async-RG flows:

        - Database masking from a schema-discovery CSV (`start_async_ruleset_generation_from_csv`) -
            returns one or more rulesets
        - Database masking from a column selection (`start_async_ruleset_generation` with `SelectedColumns`) -
            returns a list containing one ruleset
        - File masking from a file/locator selection (`start_async_ruleset_generation` with `SelectedFileData`) -
            returns a list containing one ruleset

        Raises `AsyncRulesetGenerationInProgressError` if the task hasn't finished yet,
        and `DataMasqueException` if it failed.

        Note that the ruleset(s) have autogenerated names, which you may want to customize before uploading.
        """

        status = self.get_async_ruleset_generation_task_status(connection_id)
        if status is AsyncRulesetGenerationTaskStatus.failed:
            logger.error("Ruleset generation failed for connection: %s", connection_id)
            raise DataMasqueException(f"Ruleset generation failed for connection: {connection_id}")

        if status is not AsyncRulesetGenerationTaskStatus.finished:
            logger.error(
                "Ruleset generation is still in progress for connection: %s. Status: `%s`",
                connection_id,
                status.value,
            )
            raise AsyncRulesetGenerationInProgressError(
                f"Ruleset generation in progress or not ready. Current status: `{status.value}`."
            )

        # The download-rulesets endpoint returns a ZIP attachment for the CSV flow,
        # or issues a 303 redirect back to the task-status endpoint for the column / file flows
        # (which carries the generated ruleset inline as `generated_ruleset`).
        # `requests` follows the 303 transparently, so we distinguish by the presence of
        # a `Content-Disposition: attachment` header, which Django's `FileResponse` sets on the ZIP response.
        response = self.make_request(
            method="GET",
            path=f"/api/async-generate-ruleset/{connection_id}/download-rulesets/",
        )

        if "attachment" in response.headers.get("Content-Disposition", "").lower():
            rulesets = []
            with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
                for file_info in zip_file.infolist():
                    if file_info.filename.endswith((".yml", ".yaml")):
                        with zip_file.open(file_info) as file:
                            yaml_content = file.read().decode("utf-8")
                            rulesets.append(Ruleset(name=Path(file_info.filename).stem, yaml=yaml_content))
            return rulesets

        generated = response.json().get("generated_ruleset")
        if not generated:
            raise DataMasqueException(
                f"Ruleset generation for connection {connection_id} reported `finished` "
                f"but no ruleset was returned on the task-status record."
            )

        return [Ruleset(name="generated_ruleset", yaml=generated)]

    def start_schema_discovery_run(self, discovery_config: SchemaDiscoveryRequest) -> RunId:
        """
        Starts a schema discovery run with the given configuration.

        Args:
            discovery_config: A `SchemaDiscoveryRequest` with connection ID and optional settings.

        Returns:
            RunId: The ID of the started discovery run

        Raises:
            FailedToStartError: If run fails to start
        """

        data = discovery_config.model_dump(exclude_none=True, mode="json")
        response = self.make_request(
            "POST",
            "/api/schema-discovery/",
            data=data,
            require_status_check=False,
        )
        run_data = response.json()

        if response.status_code == 201:
            logger.info("Schema discovery run %s started successfully", run_data["id"])
            return RunId(run_data["id"])

        logger.error("Schema discovery run failed to start: %s", run_data)
        raise FailedToStartError(
            f"Schema discovery run failed to start "
            f"(server responded with status {response.status_code}: {response.text}).",
            response=response,
        )

    def iter_schema_discovery_results(self, run_id: RunId) -> Iterator[SchemaDiscoveryResult]:
        """Lazily iterate all schema discovery results for a run via the paginated v2 endpoint."""

        return self._iter_paginated(
            f"/api/schema-discovery/v2/{run_id}/",
            model=SchemaDiscoveryResult,
        )

    def list_schema_discovery_results(self, run_id: RunId) -> list[SchemaDiscoveryResult]:
        """Returns all schema discovery results for a run."""

        return list(self.iter_schema_discovery_results(run_id))

    def get_schema_discovery_page(self, run_id: RunId, *, limit: int = 50, offset: int = 0) -> SchemaDiscoveryPage:
        """
        Returns a single page of schema discovery results including `table_metadata`.

        Use this when you need the table-constraint metadata alongside the results.
        """

        response = self.make_request(
            "GET",
            f"/api/schema-discovery/v2/{run_id}/",
            params={"limit": limit, "offset": offset},
        )
        return SchemaDiscoveryPage.model_validate(response.json())

    def generate_ruleset(self, generation_request: RulesetGenerationRequest) -> str:
        """
        Generates database-masking ruleset YAML from the most recent discovery run on the given connection.

        `generation_request` is a `RulesetGenerationRequest`.
        """

        data = generation_request.model_dump(exclude_none=True, mode="json")
        response = self.make_request("POST", "/api/generate-ruleset/v2/", data=data)
        return response.content.decode("utf-8")

    def generate_file_ruleset(self, generation_request: FileRulesetGenerationRequest) -> str:
        """
        Generates file-masking ruleset YAML from the most recent file-data-discovery run on the given connection.

        `generation_request` is a `FileRulesetGenerationRequest`.
        """

        data = generation_request.model_dump(exclude_none=True, mode="json")
        response = self.make_request("POST", "/api/generate-file-ruleset/", data=data)
        return response.content.decode("utf-8")

    def get_file_data_discovery_report(self, run_id: RunId) -> list[FileDiscoveryResult]:
        """Returns the file-data-discovery results for the specified run."""

        response = self.make_request("GET", f"api/runs/{run_id}/file-discovery-results/")
        return [FileDiscoveryResult.model_validate(d) for d in response.json()]
