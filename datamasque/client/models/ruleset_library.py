from datetime import datetime
from typing import NewType, Optional

from pydantic import Field

from datamasque.client.models.git import GitTrackedEntity
from datamasque.client.models.status import ValidationStatus

RulesetLibraryId = NewType("RulesetLibraryId", str)


class RulesetLibrary(GitTrackedEntity):
    """Represents a ruleset library."""

    name: str
    namespace: str = ""
    yaml: Optional[str] = Field(default=None, alias="config_yaml")

    # Server-populated read-only fields, excluded from request bodies.
    id: Optional[RulesetLibraryId] = Field(default=None, exclude=True)
    is_valid: Optional[ValidationStatus] = Field(default=None, exclude=True)
    validation_error: Optional[str] = Field(default=None, exclude=True)
    """Human-readable validation error, or `None` when valid."""
    created: Optional[datetime] = Field(default=None, exclude=True)
    modified: Optional[datetime] = Field(default=None, exclude=True)
