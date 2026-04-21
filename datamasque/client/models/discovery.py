"""Typed request and response shapes for schema-discovery and ruleset-generation endpoints."""

from typing import Any, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

from datamasque.client.models.connection import ConnectionConfig, ConnectionId, unwrap_connection_id
from datamasque.client.models.data_selection import HashColumnsTableConfig, Locator, UserSelection
from datamasque.client.models.pagination import Page


class InDataDiscoveryRule(BaseModel):
    """A single rule for in-data discovery."""

    model_config = ConfigDict(extra="forbid")

    name: Optional[str] = None
    pattern: str


class InDataDiscoveryConfig(BaseModel):
    """In-data discovery configuration nested under `SchemaDiscoveryRequest.in_data_discovery`."""

    model_config = ConfigDict(extra="forbid")

    enabled: Optional[bool] = None
    row_sample_size: Optional[int] = None
    custom_rules: Optional[list[InDataDiscoveryRule]] = None
    non_sensitive_rules: Optional[list[InDataDiscoveryRule]] = None
    force: Optional[bool] = None


class SchemaDiscoveryRequest(BaseModel):
    """
    Request body for `POST /api/schema-discovery/`.

    `connection` accepts either a `ConnectionId` or a full `ConnectionConfig` returned by an earlier client call.
    Every other field uses the server's default value when omitted.
    """

    model_config = ConfigDict(extra="forbid")

    connection: Union[ConnectionId, ConnectionConfig]
    custom_keywords: list[str] = Field(default_factory=list)
    ignored_keywords: list[str] = Field(default_factory=list)
    schemas: list[str] = Field(default_factory=list)
    in_data_discovery: Optional[InDataDiscoveryConfig] = None
    disable_built_in_keywords: bool = False
    disable_global_custom_keywords: bool = False
    disable_global_ignored_keywords: bool = False

    @field_validator("connection", mode="before")
    @classmethod
    def _unwrap_connection(cls, value: Any) -> Any:
        return unwrap_connection_id(value)


class RulesetGenerationRequest(BaseModel):
    """
    Request body for `POST /api/generate-ruleset/v2/`.

    `connection` accepts either a `ConnectionId` or a full `ConnectionConfig` returned by an earlier client call.
    `selected_columns` is the same nested `schema -> table -> [column, ...]` mapping
    used by `SelectedColumns.columns`,
    and `hash_columns` follows the `HashColumnsTableConfig` shape.
    """

    model_config = ConfigDict(extra="forbid")

    connection: Union[ConnectionId, ConnectionConfig]
    selected_columns: dict[str, dict[str, list[str]]]
    hash_columns: Optional[dict[str, dict[str, HashColumnsTableConfig]]] = None

    @field_validator("connection", mode="before")
    @classmethod
    def _unwrap_connection(cls, value: Any) -> Any:
        return unwrap_connection_id(value)


class FileRulesetGenerationRequest(BaseModel):
    """
    Request body for `POST /api/generate-file-ruleset/`.

    `connection` accepts either a `ConnectionId` or a full `ConnectionConfig` returned by an earlier client call.
    """

    model_config = ConfigDict(extra="forbid")

    connection: Union[ConnectionId, ConnectionConfig]
    selected_data: list[UserSelection]

    @field_validator("connection", mode="before")
    @classmethod
    def _unwrap_connection(cls, value: Any) -> Any:
        return unwrap_connection_id(value)


class DiscoveryMatch(BaseModel):
    """A single match found by schema or file discovery."""

    model_config = ConfigDict(extra="allow")

    label: str
    categories: list[str]
    flagged_by: str
    description: str
    hit_ratio: Optional[int] = None  # None for metadata matches, percentage 0-100 for IDD matches.


class ForeignKeyRef(BaseModel):
    """A foreign key declared on a column, pointing to another column it references."""

    model_config = ConfigDict(extra="allow")

    name: str
    referenced_column: str  # Dotted path: "schema.table.column".


class ReferencingForeignKey(BaseModel):
    """A foreign key declared on another column that points *at* this column."""

    model_config = ConfigDict(extra="allow")

    name: str
    referencing_column: str  # Dotted path: "schema.table.column".


class SchemaDiscoveryColumn(BaseModel):
    """Column-level data in a schema discovery result."""

    model_config = ConfigDict(extra="allow")

    data_type: Optional[str] = None
    max_length: Optional[int] = None
    foreign_keys: list[ForeignKeyRef]
    discovery_matches: list[DiscoveryMatch]
    numeric_precision: Optional[int] = None
    numeric_scale: Optional[int] = None
    constraint_columns: list[str]
    pk_constraint_name: Optional[str] = None
    uk_constraint_name: Optional[str] = None
    unique_index_names: list[str]
    referencing_foreign_keys: list[ReferencingForeignKey]
    constraint: str  # Primary or Unique, or empty string if column does not participate in a PK/UK


class SchemaDiscoveryResult(BaseModel):
    """A single row in the v2 schema discovery results."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: int
    column: str
    table: str
    schema_name: Optional[str] = Field(default=None, alias="schema")  # "schema" is a reserved word in Pydantic
    data: SchemaDiscoveryColumn


class ConstraintColumns(BaseModel):
    """A constraint's column list in table metadata."""

    model_config = ConfigDict(extra="allow")

    columns: list[str]


class TableConstraints(BaseModel):
    """Constraint metadata for a single table."""

    model_config = ConfigDict(extra="allow")

    primary_keys: Optional[list[ConstraintColumns]] = None
    unique_keys: Optional[list[ConstraintColumns]] = None
    foreign_keys: Optional[list[ConstraintColumns]] = None


class SchemaDiscoveryPage(Page[SchemaDiscoveryResult]):
    """
    Admin-server envelope for `GET /api/schema-discovery/v2/{run_id}/`.

    Extends the standard `Page` with `table_metadata`.
    """

    table_metadata: Optional[dict[str, dict[str, TableConstraints]]] = None


class FileDiscoveryMatch(BaseModel):
    """A single match in a file discovery locator."""

    model_config = ConfigDict(extra="allow")

    categories: Optional[list[str]] = None
    flagged_by: Optional[str] = None
    description: Optional[str] = None
    label: Optional[str] = None
    hit_ratio: Optional[int] = None


class FileDiscoveryLocatorResult(BaseModel):
    """A locator (column/path) within a discovered file."""

    model_config = ConfigDict(extra="allow")

    locator: Optional[Locator] = None
    matches: Optional[list[FileDiscoveryMatch]] = None
    data_types: Optional[list[str]] = None


class FileDiscoveryFile(BaseModel):
    """A file entry in a file discovery result."""

    model_config = ConfigDict(extra="allow")

    path: Optional[str] = None
    file_type: Optional[str] = None
    delimiter: Optional[str] = None
    encoding: Optional[str] = None


class FileDiscoveryResult(BaseModel):
    """A single record from `GET /api/runs/{run_id}/file-discovery-results/`."""

    model_config = ConfigDict(extra="allow")

    id: Optional[int] = None
    connection: Optional[Any] = None
    file_type: Optional[str] = None
    files: Optional[list[FileDiscoveryFile]] = None
    results: Optional[list[FileDiscoveryLocatorResult]] = None
