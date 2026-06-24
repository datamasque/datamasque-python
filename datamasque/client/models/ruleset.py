import enum
from typing import Any, NewType, Optional

from pydantic import Field

from datamasque.client.models.git import GitTrackedEntity
from datamasque.client.models.status import ValidationErrorType, ValidationStatus

RulesetId = NewType("RulesetId", str)


def unwrap_ruleset_id(value: Any) -> Any:
    """
    Coerce a `Ruleset` to its `id`; pass other values through unchanged.

    Used by request-model validators that accept either a `RulesetId`
    or a full `Ruleset` for user convenience.
    Raises `ValueError` if the ruleset has no `id`
    (i.e. the caller hasn't yet created it on the server).
    """

    if isinstance(value, Ruleset):
        if value.id is None:
            raise ValueError("Ruleset has not been created yet (id is None)")
        return value.id

    return value


class RulesetType(enum.Enum):
    """Ruleset type (database masking or file masking)."""

    file = "file"
    database = "database"


class Ruleset(GitTrackedEntity):
    """Represents a ruleset."""

    name: str
    yaml: str = Field(default="", alias="config_yaml")
    ruleset_type: RulesetType = Field(default=RulesetType.database, alias="mask_type")

    # Server-populated read-only fields, excluded from request bodies.
    id: Optional[RulesetId] = Field(default=None, exclude=True)
    is_valid: Optional[ValidationStatus] = Field(default=None, exclude=True)
    validation_error: Optional[str] = Field(default=None, exclude=True)
    """Human-readable validation error, or `None` when valid."""
    validation_error_type: Optional[ValidationErrorType] = Field(default=None, exclude=True)
    """Category of the validation failure, or `None` when valid."""
