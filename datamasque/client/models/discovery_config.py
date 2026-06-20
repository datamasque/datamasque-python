import enum
from datetime import datetime
from typing import Any, NewType, Optional

from pydantic import BaseModel, ConfigDict, Field

from datamasque.client.models.status import ValidationStatus

DiscoveryConfigId = NewType("DiscoveryConfigId", str)


class DiscoveryConfigType(enum.Enum):
    """Which discovery config variant a config targets: database (qualified columns) or file (locators)."""

    database = "database"
    file = "file"


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
    is_valid: Optional[ValidationStatus] = Field(default=None, exclude=True)
    """Validation status; may be `in_progress` briefly after creating a large config."""
    validation_error: Optional[str] = Field(default=None, exclude=True)
    """Human-readable validation error, or `None` when valid."""
    created: Optional[datetime] = Field(default=None, exclude=True)
    modified: Optional[datetime] = Field(default=None, exclude=True)
