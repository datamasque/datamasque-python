from datetime import datetime
from typing import Any, NewType, Optional

from pydantic import BaseModel, ConfigDict, Field

from datamasque.client.models.status import ValidationStatus

DiscoveryConfigId = NewType("DiscoveryConfigId", str)


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
    id: Optional[DiscoveryConfigId] = None
    # Server-managed validation surface, populated by the DataMasque server.
    # `is_valid` may be `in_progress` immediately after creating a large config,
    # transitioning to `valid` or `invalid` once the server finishes validating.
    is_valid: Optional[ValidationStatus] = None
    validation_error: Optional[str] = None
    created: Optional[datetime] = None
    modified: Optional[datetime] = None
