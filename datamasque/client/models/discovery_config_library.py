from datetime import datetime
from typing import NewType, Optional

from pydantic import BaseModel, ConfigDict, Field

from datamasque.client.models.status import ValidationStatus

DiscoveryConfigLibraryId = NewType("DiscoveryConfigLibraryId", str)


class DiscoveryConfigLibrary(BaseModel):
    """
    Represents a named, namespaced, persisted YAML discovery config library.

    A library is untyped: the same library may be imported by both database and
    file discovery configs, and its name is unique within a namespace.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    name: str
    namespace: str = ""
    yaml: Optional[str] = Field(default=None, alias="config_yaml")
    # Server-populated read-only fields, excluded from request bodies.
    id: Optional[DiscoveryConfigLibraryId] = Field(default=None, exclude=True)
    is_valid: Optional[ValidationStatus] = Field(default=None, exclude=True)
    """Validation status; libraries are validated synchronously on create/update."""
    validation_error: Optional[str] = Field(default=None, exclude=True)
    """Human-readable validation error, or `None` when valid."""
    created: Optional[datetime] = Field(default=None, exclude=True)
    modified: Optional[datetime] = Field(default=None, exclude=True)
