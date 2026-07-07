from datetime import datetime
from typing import NewType, Optional

from pydantic import BaseModel, ConfigDict, Field

from datamasque.client.models.discovery_config import DiscoveryConfigType
from datamasque.client.models.status import ValidationStatus

DiscoveryConfigLibraryId = NewType("DiscoveryConfigLibraryId", str)


class DiscoveryConfigLibrary(BaseModel):
    """
    Represents a named, namespaced, persisted YAML discovery config library.

    Library names are unique per config type within a namespace,
    so a database and a file library may share a name.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    name: str
    config_type: DiscoveryConfigType
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
