from datetime import datetime
from typing import NewType, Optional

from pydantic import Field

from datamasque.client.models.git import GitTrackedEntity
from datamasque.client.models.status import ValidationErrorDetails, ValidationStatus

RulesetLibraryId = NewType("RulesetLibraryId", str)


class RulesetLibrary(GitTrackedEntity):
    """Represents a ruleset library."""

    name: str
    namespace: str = ""
    yaml: Optional[str] = Field(default=None, alias="config_yaml")

    # Server-populated read-only fields, excluded from request bodies.
    id: Optional[RulesetLibraryId] = Field(default=None, exclude=True)
    is_valid: Optional[ValidationStatus] = Field(default=None, exclude=True)
    validation_errors: list[ValidationErrorDetails] = Field(default_factory=list, exclude=True)
    """Validation errors surfaced by the server; empty when valid."""
    unlicensed_feature_warnings: Optional[list[str]] = Field(default=None, exclude=True)
    """
    One sentence per feature the library uses that the server's license does not allow.
    Empty when the license allows every feature, and `None` until the library has passed validation.
    """
    created: Optional[datetime] = Field(default=None, exclude=True)
    modified: Optional[datetime] = Field(default=None, exclude=True)
