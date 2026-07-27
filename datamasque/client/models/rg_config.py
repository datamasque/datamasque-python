from datetime import datetime
from typing import Any, NewType, Optional

from pydantic import BaseModel, ConfigDict, Field

from datamasque.client.models.status import ValidationErrorDetails, ValidationStatus

RGConfigId = NewType("RGConfigId", str)


def unwrap_rg_config_id(value: Any) -> Any:
    """
    Coerce an `RGConfig` to its `id`; pass other values through unchanged.

    Used by request-model validators and the generate methods
    that accept either an `RGConfigId` or a full `RGConfig` for user convenience.
    Raises `ValueError` if the config has no `id`
    (i.e. the caller hasn't yet created it on the server).
    """

    if isinstance(value, RGConfig):
        if value.id is None:
            raise ValueError("RG config has not been created yet (id is None)")
        return value.id

    return value


class RGConfig(BaseModel):
    """
    Represents a named, persisted YAML ruleset-generation (RG) configuration.

    An RG config maps discovered labels to masks;
    ruleset generation applies it to a discovery run's results.
    Unlike discovery configs, RG configs are untyped —
    the same config serves both database and file ruleset generation.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    name: str
    yaml: Optional[str] = Field(default=None, alias="config_yaml")

    # Server-populated read-only fields, excluded from request bodies.
    id: Optional[RGConfigId] = Field(default=None, exclude=True)
    is_valid: Optional[ValidationStatus] = Field(default=None, exclude=True)
    validation_errors: list[ValidationErrorDetails] = Field(default_factory=list, exclude=True)
    created: Optional[datetime] = Field(default=None, exclude=True)
    modified: Optional[datetime] = Field(default=None, exclude=True)
