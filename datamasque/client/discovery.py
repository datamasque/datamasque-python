import logging
import zipfile
from io import BufferedIOBase, BytesIO, TextIOBase
from pathlib import Path
from typing import Iterator, Optional, Union

from requests import Response

from datamasque.client.base import BaseClient, UploadFile
from datamasque.client.exceptions import (
    AsyncRulesetGenerationInProgressError,
    DataMasqueException,
    DiscoveryConfigNotFoundError,
    FailedToStartError,
    InvalidDiscoveryConfigError,
)
from datamasque.client.models.connection import ConnectionId
from datamasque.client.models.data_selection import (
    SelectedColumns,
    SelectedData,
    SelectedFileData,
)
from datamasque.client.models.discovery import (
    FileDataDiscoveryFromConfigRequest,
    FileDataDiscoveryRequest,
    FileDiscoveryResult,
    FileRulesetGenerationRequest,
    RulesetGenerationRequest,
    SchemaDiscoveryFromConfigRequest,
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

        If the content is a zip (for example a split report from `get_db_discovery_result_report()`),
        it is detected by its magic bytes and uploaded as a zip;
        otherwise it is uploaded as CSV.

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

        is_zip = False
        if content.seekable():
            is_zip = content.read(4) == b"PK\x03\x04"
            content.seek(0)
        filename = "ruleset.zip" if is_zip else "ruleset.csv"
        content_type = "application/zip" if is_zip else "text/csv"

        files = [
            UploadFile(
                field_name="csv_or_zip_file",
                filename=filename,
                content=content,
                content_type=content_type,
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
        and `DataMasqueException` if it failed or was cancelled.

        Note that the ruleset(s) have autogenerated names, which you may want to customize before uploading.
        """

        status = self.get_async_ruleset_generation_task_status(connection_id)
        if status is AsyncRulesetGenerationTaskStatus.failed:
            logger.error("Ruleset generation failed for connection: %s", connection_id)
            raise DataMasqueException(f"Ruleset generation failed for connection: {connection_id}")

        if status is AsyncRulesetGenerationTaskStatus.cancelled:
            logger.error("Ruleset generation was cancelled for connection: %s", connection_id)
            raise DataMasqueException(f"Ruleset generation was cancelled for connection: {connection_id}")

        if not status.is_finished:
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

            if not rulesets:
                raise DataMasqueException(
                    f"Ruleset generation for connection {connection_id} reported `finished` "
                    f"but the downloaded archive contained no rulesets."
                )

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

    def start_file_data_discovery_run(self, request: FileDataDiscoveryRequest) -> RunId:
        """
        Starts a file data discovery run with the given configuration.

        Args:
            request: A `FileDataDiscoveryRequest` with connection and optional settings.

        Returns:
            RunId: The ID of the started discovery run

        Raises:
            FailedToStartError: If run fails to start
        """

        data = request.model_dump(exclude_none=True, mode="json")
        response = self.make_request(
            "POST",
            "/api/run-file-data-discovery/",
            data=data,
            require_status_check=False,
        )
        run_data = response.json()

        if response.status_code == 201:
            logger.info("File data discovery run %s started successfully", run_data["id"])
            return RunId(run_data["id"])

        logger.error("File data discovery run failed to start: %s", run_data)
        raise FailedToStartError(
            f"File data discovery run failed to start "
            f"(server responded with status {response.status_code}: {response.text}).",
            response=response,
        )

    def start_schema_discovery_run_from_config(self, request: SchemaDiscoveryFromConfigRequest) -> RunId:
        """
        Starts a schema discovery run from a saved discovery config.

        Args:
            request: A `SchemaDiscoveryFromConfigRequest` with the `connection` and a required `discovery_config`
                (a saved config, or `None` for the server's defaults).

        Returns:
            RunId: The ID of the started discovery run

        Raises:
            DiscoveryConfigNotFoundError: the referenced discovery config cannot be found
                (it does not exist or is the wrong type for the run).
            InvalidDiscoveryConfigError: the config is present but not in a `valid` validation state,
                or its YAML is rejected when the run starts.
            FailedToStartError: the run failed to start for any other reason.
        """

        return self._start_discovery_run_from_config(request, "/api/schema-discovery/v2/", "Schema discovery")

    def start_file_data_discovery_run_from_config(self, request: FileDataDiscoveryFromConfigRequest) -> RunId:
        """
        Starts a file data discovery run from a saved discovery config.

        Args:
            request: A `FileDataDiscoveryFromConfigRequest` with the `connection`,
                a required `discovery_config` (a saved config, or `None` for the server's defaults),
                and optional run `options`.

        Returns:
            RunId: The ID of the started discovery run

        Raises:
            DiscoveryConfigNotFoundError: the referenced discovery config cannot be found
                (it does not exist or is the wrong type for the run).
            InvalidDiscoveryConfigError: the config is present but not in a `valid` validation state,
                or its YAML is rejected when the run starts.
            FailedToStartError: the run failed to start for any other reason.
        """

        return self._start_discovery_run_from_config(request, "/api/run-file-data-discovery/v2/", "File data discovery")

    def _start_discovery_run_from_config(
        self,
        request: Union[SchemaDiscoveryFromConfigRequest, FileDataDiscoveryFromConfigRequest],
        path: str,
        run_kind: str,
    ) -> RunId:
        """Post a saved-config discovery request and return its run id, classifying config errors on failure."""

        data = request.model_dump(exclude_none=True, mode="json")
        # The server requires `discovery_config` to be present; a null selects its built-in defaults,
        # so send it explicitly rather than letting `exclude_none` drop a None.
        data.setdefault("discovery_config", None)
        response = self.make_request("POST", path, data=data, require_status_check=False)
        run_data = response.json() if response.content else {}

        if response.status_code == 201:
            logger.info("%s run %s started successfully", run_kind, run_data["id"])
            return RunId(run_data["id"])

        logger.error("%s run failed to start: %s", run_kind, run_data)
        self._maybe_raise_discovery_config_error(run_data, response, run_kind)
        raise FailedToStartError(
            f"{run_kind} run failed to start (server responded with status {response.status_code}: {response.text}).",
            response=response,
        )

    # Server key for a 400 that means the discovery config itself is unusable:
    # a missing or wrong-type config, or one not in a `valid` validation state (string messages),
    # or re-validation of broken saved-config YAML when the run starts
    # (a `{"message", "line_number", "column_number"}` dict).
    DISCOVERY_CONFIG_ERROR_FIELD = "discovery_config"

    # The phrase the server uses when the config id cannot be resolved (a missing or wrong-type config).
    MISSING_DISCOVERY_CONFIG_SIGNATURE = "object does not exist"

    @classmethod
    def _maybe_raise_discovery_config_error(cls, run_data: object, response: Response, run_kind: str) -> None:
        """Raise a discovery-config error if the server's 400 body cites the discovery config."""
        if not isinstance(run_data, dict):
            return

        if not (errors := run_data.get(cls.DISCOVERY_CONFIG_ERROR_FIELD)):
            return

        detail = cls._format_discovery_config_error(errors)
        if cls.MISSING_DISCOVERY_CONFIG_SIGNATURE in detail:
            raise DiscoveryConfigNotFoundError(
                f"{run_kind} run failed to start: the referenced discovery config could not be found: {detail}",
                response=response,
            )

        raise InvalidDiscoveryConfigError(
            f"{run_kind} run failed to start due to discovery config error: {detail}",
            response=response,
        )

    @staticmethod
    def _format_discovery_config_error(errors: object) -> str:
        """Render the first server error, handling both string and `{message, ...}` dict items."""
        first = errors[0] if isinstance(errors, list) and errors else errors
        if isinstance(first, dict) and "message" in first:
            return str(first["message"])

        return str(first)

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

    def get_discovery_run_config_snapshot_yaml(self, run_id: RunId, *, timezone: Optional[str] = None) -> str:
        """
        Returns the discovery-config YAML that was effective at the start of the given discovery run.

        The YAML is prefixed with a commented provenance header naming the saved config
        (or the built-in defaults) the run used, and whether it has since been modified or deleted.
        `timezone`, a `±HH:MM` UTC offset, sets the timezone of the header timestamp; the server defaults to UTC.
        """

        params = {"timezone": timezone} if timezone is not None else None
        response = self.make_request("GET", f"/api/discovery/runs/{run_id}/config-snapshot/", params=params)
        with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
            names = zip_file.namelist()
            if not names:
                raise DataMasqueException(f"Discovery run {run_id} config snapshot archive contained no files.")

            with zip_file.open(names[0]) as snapshot_file:
                return snapshot_file.read().decode("utf-8")
