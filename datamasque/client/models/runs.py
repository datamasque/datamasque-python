"""Typed request and response shapes for run-related API endpoints."""

import enum
from datetime import datetime
from typing import Any, NewType, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from datamasque.client.models.connection import ConnectionConfig, ConnectionId, unwrap_connection_id
from datamasque.client.models.ruleset import Ruleset, RulesetId, unwrap_ruleset_id
from datamasque.client.models.status import MaskingRunStatus

RunId = NewType("RunId", int)


class MaskType(enum.Enum):
    """Type of a masking run."""

    database = "database"  # Also used for schema discovery.
    file = "file"
    file_data_discovery = "file_data_discovery"


class MaskingRunOptions(BaseModel):
    """
    Optional run-time overrides for `MaskingRunRequest.options`.

    All fields optional; server applies defaults when omitted.
    `run_secret`,
    if supplied,
    must be 16–256 characters and is used as the per-run encryption key;
    the server auto-generates one when omitted.
    """

    model_config = ConfigDict(extra="forbid")

    batch_size: Optional[int] = None
    dry_run: Optional[bool] = None
    continue_on_failure: Optional[bool] = None
    max_rows: Optional[int] = None
    diagnostic_logging: Optional[bool] = None
    run_secret: Optional[str] = Field(default=None, min_length=16, max_length=256)
    disable_instance_secret: Optional[bool] = None


class MaskingRunRequest(BaseModel):
    """
    Request body for `POST /api/runs/`.

    `connection`, `destination_connection`, and `ruleset` accept either the server-assigned ID
    or the corresponding object returned by an earlier client call (e.g. a `ConnectionConfig`
    or `Ruleset`); the object's `id` is extracted at construction time.
    """

    model_config = ConfigDict(extra="forbid")

    connection: Union[ConnectionId, ConnectionConfig]
    ruleset: Union[RulesetId, Ruleset]
    mask_type: MaskType = MaskType.database
    destination_connection: Optional[Union[ConnectionId, ConnectionConfig]] = None
    options: MaskingRunOptions = Field(default_factory=MaskingRunOptions)
    name: Optional[str] = None

    @field_validator("connection", "destination_connection", mode="before")
    @classmethod
    def _unwrap_connection(cls, value: Any) -> Any:
        return unwrap_connection_id(value)

    @field_validator("ruleset", mode="before")
    @classmethod
    def _unwrap_ruleset(cls, value: Any) -> Any:
        return unwrap_ruleset_id(value)


class RunConnectionRef(BaseModel):
    """A reference to a connection used in a run — just the ID and display name."""

    model_config = ConfigDict(extra="allow")

    id: Optional[ConnectionId] = None
    name: str


def _collapse_flat_connection_fields(data: Any) -> Any:
    """
    Collapse flat `*_connection` + `*_connection_name` pairs into nested `RunConnectionRef`s.

    The admin server sends connections as two parallel fields
    (`source_connection` holding the ID and `source_connection_name` holding the display name);
    the client surfaces them as a single nested object.
    Leaves the input alone if the fields are already in nested form
    (i.e. the caller constructed the model directly).
    """

    if not isinstance(data, dict):
        return data

    data = dict(data)

    if "source_connection_name" in data and not isinstance(data.get("source_connection"), dict):
        data["source_connection"] = {
            "id": data.pop("source_connection", None),
            "name": data.pop("source_connection_name"),
        }

    dest_name = data.get("destination_connection_name")
    if dest_name and not isinstance(data.get("destination_connection"), dict):
        data["destination_connection"] = {
            "id": data.pop("destination_connection", None),
            "name": data.pop("destination_connection_name"),
        }
    elif "destination_connection_name" in data:
        # Empty string or None — let the Optional default apply.
        data.pop("destination_connection_name", None)
        data.pop("destination_connection", None)

    return data


class RunInfo(BaseModel):
    """Full record for a masking run."""

    model_config = ConfigDict(extra="allow")

    id: int
    status: MaskingRunStatus
    mask_type: MaskType
    source_connection: RunConnectionRef
    ruleset_name: str
    name: Optional[str] = None
    destination_connection: Optional[RunConnectionRef] = None
    ruleset: Optional[RulesetId] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    options: Optional[dict[str, Any]] = None

    @model_validator(mode="before")
    @classmethod
    def _collapse_connection_fields(cls, data: Any) -> Any:
        return _collapse_flat_connection_fields(data)


class UnfinishedRun(BaseModel):
    """Represents a masking run that is queued, running, validating, or cancelling."""

    model_config = ConfigDict(extra="allow")

    id: int
    source_connection: RunConnectionRef
    ruleset_name: str
    status: MaskingRunStatus
    destination_connection: Optional[RunConnectionRef] = None

    @model_validator(mode="before")
    @classmethod
    def _collapse_connection_fields(cls, data: Any) -> Any:
        return _collapse_flat_connection_fields(data)

    def __str__(self) -> str:
        if self.destination_connection is not None:
            connection_part = f'"{self.source_connection.name}", "{self.destination_connection.name}"'
        else:
            connection_part = f'"{self.source_connection.name}"'

        return f'{connection_part}: Run ID {self.id} in status `{self.status.value}`, ruleset "{self.ruleset_name}"'
