from datetime import datetime
from typing import NewType, Optional

from pydantic import BaseModel, ConfigDict, Field

from datamasque.client.models.status import ValidationStatus

DiscoveryConfigLibraryId = NewType("DiscoveryConfigLibraryId", str)


class DiscoveryConfigLibrary(BaseModel):
    """Represents a named, namespaced, persisted YAML discovery config library."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    name: str
    namespace: str = ""
    yaml: Optional[str] = Field(default=None, alias="config_yaml")
    id: Optional[DiscoveryConfigLibraryId] = None
    # Server-managed validation surface, populated by the DataMasque server.
    # `is_valid` may be `in_progress` immediately after creating a large library,
    # transitioning to `valid` or `invalid` once the server finishes validating.
    is_valid: Optional[ValidationStatus] = None
    validation_error: Optional[str] = None
    created: Optional[datetime] = None
    modified: Optional[datetime] = None
