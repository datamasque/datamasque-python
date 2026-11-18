from datetime import datetime
from typing import NewType, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from datamasque.client.models.project import drop_namespace
from datamasque.client.models.status import ValidationStatus

DiscoveryConfigLibraryId = NewType("DiscoveryConfigLibraryId", str)


class DiscoveryConfigLibrary(BaseModel):
    """
    Represents a named, persisted YAML discovery config library.

    A library is untyped: the same library may be imported by both database and
    file discovery configs, and its name is unique within its project.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    name: str
    project: Optional[str] = None
    """
    The project it belongs to: the `<id>-<slug>` reference a GET returns, such as `Project.reference`,
    or the project's ID as a string.
    Left as `None`, a create goes into the client's `default_project`, else the server's default project.
    """
    yaml: Optional[str] = Field(default=None, alias="config_yaml")
    # Server-populated read-only fields, excluded from request bodies.
    id: Optional[DiscoveryConfigLibraryId] = Field(default=None, exclude=True)
    is_valid: Optional[ValidationStatus] = Field(default=None, exclude=True)
    """Validation status; libraries are validated synchronously on create/update."""
    validation_error: Optional[str] = Field(default=None, exclude=True)
    """Human-readable validation error, or `None` when valid."""
    usage_count: Optional[int] = Field(default=None, exclude=True)
    """Number of active discovery configs that import this library."""
    created: Optional[datetime] = Field(default=None, exclude=True)
    modified: Optional[datetime] = Field(default=None, exclude=True)

    _drop_namespace = model_validator(mode="before")(drop_namespace)
