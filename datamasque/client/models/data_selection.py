"""Models related to data selection in endpoints such as /api/async-generate-ruleset."""

from typing import Optional, Union

from pydantic import BaseModel, ConfigDict

JsonPath = list[Union[str, int]]
"""
A path into a JSON/structured document,
e.g. `["employees", 0, "firstName"]` or `["users", "*", "email"]`.
String elements are object keys (or the `*` wildcard), and integer elements are list indices.
"""

Locator = Union[str, JsonPath]
"""
A locator identifying a masked value within a file.
- Tabular files (CSV, parquet, fixed-width) use a bare string column name, e.g. `"email"`.
- Structured files (JSON) use a :data:`JsonPath`, e.g. `["employees", "*", "email"]`.
"""


class UserSelection(BaseModel):
    """Information about selected files and locators for file masking ruleset generation."""

    model_config = ConfigDict(extra="forbid")

    files: list[str]
    locators: list[Locator]


class HashColumnsTableConfig(BaseModel):
    """
    Configuration for `hash_columns` at the table level.

    `table` contains table-level hash column defaults applied to all selected columns.
    `columns` contains per-column overrides (`None` or `[]` disables hashing for that column).
    """

    model_config = ConfigDict(extra="forbid")

    table: Optional[list[str]] = None
    columns: Optional[dict[str, Optional[list[str]]]] = None


class SelectedColumns(BaseModel):
    """Selected columns and hash columns for database masking ruleset generation."""

    model_config = ConfigDict(extra="forbid")

    columns: dict[str, dict[str, list[str]]]
    hash_columns: Optional[dict[str, dict[str, HashColumnsTableConfig]]] = None


class SelectedFileData(BaseModel):
    """Selected files and locators for file masking ruleset generation."""

    model_config = ConfigDict(extra="forbid")

    user_selections: list[UserSelection]


SelectedData = Union[SelectedColumns, SelectedFileData]
