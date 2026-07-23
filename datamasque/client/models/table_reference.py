"""Table reference models for the DataMasque API."""

import enum
from datetime import datetime
from typing import Any, NewType, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

from datamasque.client.models.connection import ConnectionConfig, ConnectionId, unwrap_connection_id

TableReferenceId = NewType("TableReferenceId", str)


class TableReferenceFormat(enum.Enum):
    """
    File format of the data a table reference points at.

    Only meaningful for a table reference on a file connection,
    and an explicit choice: the source path's suffix carries no meaning.
    """

    csv = "csv"
    parquet = "parquet"


class TableReferenceOptions(BaseModel):
    """
    Format and CSV parsing options for a table reference.

    These apply to file connections only —
    they are ignored for a table reference on a database connection,
    and the CSV options are ignored when the `format` is `parquet`.

    There is deliberately no header toggle:
    a CSV identity map's columns must be addressable by name,
    so the first row is always read as the header.

    Every option has a default, and the server fills in any option omitted from a request,
    so the values here are the same defaults the server applies.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    format: TableReferenceFormat = TableReferenceFormat.csv
    delimiter: str = ","
    encoding: str = "utf-8"
    quotechar: str = '"'
    null_string: Optional[str] = None
    """The string in the source data to read as a null value, or `None` to read no value as null."""


class TableReference(BaseModel):
    """
    Represents a named, persisted reference to a table of identity data held outside DataMasque.

    The data lives in the referenced connection —
    a CSV or Parquet file in a file connection, or a table in a database connection —
    so a ruleset can reference the identity map once, by name,
    instead of re-declaring the source each time.

    A table reference stores no credentials of its own; it borrows the referenced connection's.

    `source` is interpreted according to that connection's own type:

    * file connection — a path to the file within the connection's fileset.
      The format comes from `options.format`, never from the path's suffix.
    * database connection — a dotted, schema-qualified ``schema.table`` reference.

    Names are unique across live table references (a deleted reference frees its name for reuse),
    and the referenced connection must not be archived.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    name: str
    connection: Union[ConnectionId, ConnectionConfig]
    """The connection the data lives in. Accepts a `ConnectionId` or a `ConnectionConfig` (its `id` is extracted)."""
    source: str
    options: Optional[TableReferenceOptions] = None
    """Format and CSV parsing options; when `None`, the server applies its defaults on create."""

    # Server-populated read-only fields, excluded from request bodies.
    id: Optional[TableReferenceId] = Field(default=None, exclude=True)
    created: Optional[datetime] = Field(default=None, exclude=True)
    modified: Optional[datetime] = Field(default=None, exclude=True)

    @field_validator("connection", mode="before")
    @classmethod
    def _unwrap_connection(cls, value: Any) -> Any:
        return unwrap_connection_id(value)

    @property
    def connection_id(self) -> ConnectionId:
        """
        The ID of the referenced connection.

        `connection` accepts a whole `ConnectionConfig` for convenience but always holds an ID
        once validated, so this narrows the declared type back down for callers reading it.
        """

        return ConnectionId(unwrap_connection_id(self.connection))
