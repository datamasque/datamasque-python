import enum
from datetime import datetime
from typing import Any, NewType, Optional

from pydantic import AliasChoices, AliasPath, BaseModel, ConfigDict, Field

from datamasque.client.models.status import ValidationErrorDetails, ValidationStatus

DiscoveryConfigId = NewType("DiscoveryConfigId", str)


class DiscoveryConfigType(enum.Enum):
    """Which discovery config variant a config targets: database (qualified columns) or file (locators)."""

    database = "database"
    file = "file"


class DiscoveryKind(enum.Enum):
    """What a file discovery config looks for: sensitive locators, or its labels in the text of documents."""

    structured = "structured"
    unstructured = "unstructured"


class DiscoveryConfigKind(enum.Enum):
    """
    What a saved discovery config looks for, as the server reads it from the config's YAML.

    A database config is always `structured`.
    A file config is the kind its YAML declares, or `structured` if it declares none;
    `unknown` when its YAML does not parse, is not a mapping, or names a kind that does not exist.
    """

    structured = "structured"
    unstructured = "unstructured"
    unknown = "unknown"


def unwrap_discovery_config_id(value: Any) -> Any:
    """
    Coerce a `DiscoveryConfig` to its `id`; pass other values through unchanged.

    Used by request-model validators that accept either a `DiscoveryConfigId`
    or a full `DiscoveryConfig` for user convenience.
    Raises `ValueError` if the config has no `id`
    (i.e. the caller hasn't yet created it on the server).
    """

    if isinstance(value, DiscoveryConfig):
        if value.id is None:
            raise ValueError("Discovery config has not been created yet (id is None)")
        return value.id

    return value


class DiscoveryConfig(BaseModel):
    """Represents a named, persisted YAML discovery configuration."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    name: str
    yaml: Optional[str] = Field(default=None, alias="config_yaml")
    config_type: DiscoveryConfigType

    # Server-populated read-only fields, excluded from request bodies.
    id: Optional[DiscoveryConfigId] = Field(default=None, exclude=True)
    project: Optional[str] = Field(default=None, exclude=True)
    """Its project's `<id>-<slug>` reference, e.g. `1-default`; `None` until it is created on the server."""
    discovery_kind: Optional[DiscoveryConfigKind] = Field(default=None, exclude=True)
    """
    What the config looks for, as `DiscoveryConfigKind` describes.
    `None` until it is created on the server, or when a response leaves it out.
    """
    is_valid: Optional[ValidationStatus] = Field(default=None, exclude=True)
    """Validation status; may be `in_progress` briefly after creating a large config."""
    validation_error: Optional[str] = Field(default=None, exclude=True)
    """Human-readable validation error, or `None` when valid."""
    # Deliberately not `validation_errors`:
    # this is a different shape to `Ruleset.validation_errors`,
    # and would sit one character from `validation_error` above.
    validation_error_details: list[ValidationErrorDetails] = Field(
        default_factory=list,
        exclude=True,
        validation_alias=AliasChoices(AliasPath("errors", "config_yaml"), "validation_error_details"),
    )
    """Structured, positional validation errors."""
    created: Optional[datetime] = Field(default=None, exclude=True)
    modified: Optional[datetime] = Field(default=None, exclude=True)
